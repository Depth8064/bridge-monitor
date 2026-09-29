package storage

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
	"github.com/Depth8064/bridge-monitor/internal/model"

	_ "modernc.org/sqlite"
)

// Tier names double as table suffixes and watermark keys.
type tier struct {
	name string
	size int64 // bucket size in ms
}

var tiers = []tier{
	{"1m", int64(time.Minute / time.Millisecond)},
	{"1h", int64(time.Hour / time.Millisecond)},
	{"1d", int64(24 * time.Hour / time.Millisecond)},
}

const schema = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS targets (
	id   INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	role TEXT NOT NULL,
	type TEXT NOT NULL,
	host TEXT NOT NULL
);
-- rtt NULL = lost. ts is unix ms and shared by every sample of a probe round.
CREATE TABLE IF NOT EXISTS samples (
	ts        INTEGER NOT NULL,
	target_id INTEGER NOT NULL,
	rtt       REAL,
	err       TEXT,
	PRIMARY KEY (ts, target_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS outages (
	id       INTEGER PRIMARY KEY,
	target   TEXT NOT NULL,
	role     TEXT NOT NULL,
	start_ts INTEGER NOT NULL,
	end_ts   INTEGER NOT NULL,
	lost     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS outages_end ON outages(end_ts);
CREATE TABLE IF NOT EXISTS rollup_carry (
	target_id INTEGER PRIMARY KEY,
	prev      REAL NOT NULL,
	run       INTEGER NOT NULL,
	last      INTEGER NOT NULL
);
`

const tierSchema = `
CREATE TABLE IF NOT EXISTS agg_%[1]s (
	bucket    INTEGER NOT NULL,
	target_id INTEGER NOT NULL,
	sent      INTEGER NOT NULL,
	lost      INTEGER NOT NULL,
	rtt_sum   REAL NOT NULL,
	rtt_min   REAL NOT NULL,
	rtt_max   REAL NOT NULL,
	jit_sum   REAL NOT NULL,
	jit_n     INTEGER NOT NULL,
	spikes    INTEGER NOT NULL,
	bursts    INTEGER NOT NULL,
	max_burst INTEGER NOT NULL,
	hist      BLOB,
	PRIMARY KEY (bucket, target_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS verdict_%[1]s (
	bucket       INTEGER PRIMARY KEY,
	rounds       INTEGER NOT NULL,
	local_n      INTEGER NOT NULL,
	remote_n     INTEGER NOT NULL,
	inet_n       INTEGER NOT NULL,
	local_fail   INTEGER NOT NULL,
	remote_fail  INTEGER NOT NULL,
	inet_fail    INTEGER NOT NULL,
	far_fail     INTEGER NOT NULL,
	bridge_fault INTEGER NOT NULL,
	inet_bridge  INTEGER NOT NULL,
	upstream     INTEGER NOT NULL
);
`

type DB struct {
	db  *sql.DB
	cfg *config.Config

	mu    sync.RWMutex
	ids   map[string]int64
	roles map[int64]string

	insSample *sql.Stmt
}

func Open(path string, cfg *config.Config) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(10000)&_txlock=immediate"
	sdb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	d := &DB{db: sdb, cfg: cfg, ids: map[string]int64{}, roles: map[int64]string{}}
	if err := d.init(); err != nil {
		sdb.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) init() error {
	stmts := schema
	for _, t := range tiers {
		stmts += fmt.Sprintf(tierSchema, t.name)
	}
	if _, err := d.db.Exec(stmts); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	for _, t := range d.cfg.Targets {
		if _, err := d.ensureTarget(d.db, t.Name, t.Role, t.Type, t.Host, true); err != nil {
			return err
		}
	}
	var err error
	d.insSample, err = d.db.Prepare(`INSERT OR IGNORE INTO samples (ts, target_id, rtt, err) VALUES (?, ?, ?, ?)`)
	return err
}

type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ensureTarget returns the id for name, creating it if needed. Config targets overwrite
// role/type/host; imported history only fills in unknown names.
func (d *DB) ensureTarget(q execQuerier, name, role, typ, host string, update bool) (int64, error) {
	ctx := context.Background()
	stmt := `INSERT INTO targets (name, role, type, host) VALUES (?, ?, ?, ?) ON CONFLICT(name) DO NOTHING`
	if update {
		stmt = `INSERT INTO targets (name, role, type, host) VALUES (?, ?, ?, ?)
			ON CONFLICT(name) DO UPDATE SET role = excluded.role, type = excluded.type, host = excluded.host`
	}
	if _, err := q.ExecContext(ctx, stmt, name, role, typ, host); err != nil {
		return 0, fmt.Errorf("target %q: %w", name, err)
	}
	var id int64
	var r string
	if err := q.QueryRowContext(ctx, `SELECT id, role FROM targets WHERE name = ?`, name).Scan(&id, &r); err != nil {
		return 0, err
	}
	d.mu.Lock()
	d.ids[name], d.roles[id] = id, r
	d.mu.Unlock()
	return id, nil
}

func (d *DB) targetID(name string) (int64, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	id, ok := d.ids[name]
	return id, ok
}

func (d *DB) roleMap() map[int64]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	m := make(map[int64]string, len(d.roles))
	for k, v := range d.roles {
		m[k] = v
	}
	return m
}

func (d *DB) Close() error {
	if d.insSample != nil {
		d.insSample.Close()
	}
	return d.db.Close()
}

func (d *DB) WriteRound(t time.Time, samples []model.Sample) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt := tx.Stmt(d.insSample)
	ts := t.UnixMilli()
	for _, s := range samples {
		id, ok := d.targetID(s.Target)
		if !ok {
			continue
		}
		var rtt, errText any
		if s.OK {
			rtt = float64(s.RTT.Microseconds()) / 1000
		} else if s.Err != "" {
			errText = s.Err
		}
		if _, err := stmt.Exec(ts, id, rtt, errText); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) WriteOutage(o model.Outage) error {
	_, err := d.db.Exec(`INSERT INTO outages (target, role, start_ts, end_ts, lost) VALUES (?, ?, ?, ?, ?)`,
		o.Target, o.Role, o.Start.UnixMilli(), o.End.UnixMilli(), o.Lost)
	return err
}

// Replay feeds stored rounds at or after since into fn, oldest first.
func (d *DB) Replay(ctx context.Context, since time.Time, fn func(time.Time, []model.Sample)) (int, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT s.ts, t.name, s.rtt, s.err FROM samples s JOIN targets t ON t.id = s.target_id
		WHERE s.ts >= ? ORDER BY s.ts`, since.UnixMilli())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var (
		cur   int64 = -1
		batch []model.Sample
		n     int
	)
	flush := func() {
		if len(batch) > 0 {
			fn(time.UnixMilli(cur), batch)
			n++
		}
		batch = nil
	}
	for rows.Next() {
		var ts int64
		var name string
		var rtt sql.NullFloat64
		var errText sql.NullString
		if err := rows.Scan(&ts, &name, &rtt, &errText); err != nil {
			return n, err
		}
		if ts != cur {
			flush()
			cur = ts
		}
		s := model.Sample{Target: name, OK: rtt.Valid, Err: errText.String}
		if rtt.Valid {
			s.RTT = time.Duration(rtt.Float64 * float64(time.Millisecond))
		}
		batch = append(batch, s)
	}
	flush()
	return n, rows.Err()
}

func (d *DB) outages(ctx context.Context, from int64, limit int) ([]model.Outage, error) {
	q := `SELECT target, role, start_ts, end_ts, lost FROM outages WHERE end_ts >= ? ORDER BY end_ts DESC`
	args := []any{from}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := d.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []model.Outage
	for rows.Next() {
		var target, role string
		var start, end int64
		var lost int
		if err := rows.Scan(&target, &role, &start, &end, &lost); err != nil {
			return nil, err
		}
		res = append(res, model.NewOutage(target, role, time.UnixMilli(start), time.UnixMilli(end), lost, false))
	}
	return res, rows.Err()
}

// ExportSamples streams raw samples at or after from as CSV.
func (d *DB) ExportSamples(ctx context.Context, w io.Writer, from time.Time) error {
	rows, err := d.db.QueryContext(ctx, `
		SELECT s.ts, t.name, t.role, t.type, t.host, s.rtt, s.err FROM samples s JOIN targets t ON t.id = s.target_id
		WHERE s.ts >= ? ORDER BY s.ts, t.name`, from.UnixMilli())
	if err != nil {
		return err
	}
	defer rows.Close()
	cw := csv.NewWriter(w)
	cw.Write([]string{"time", "target", "role", "type", "host", "ok", "rtt_ms", "error"})
	for rows.Next() {
		var ts int64
		var name, role, typ, host string
		var rtt sql.NullFloat64
		var errText sql.NullString
		if err := rows.Scan(&ts, &name, &role, &typ, &host, &rtt, &errText); err != nil {
			return err
		}
		ok, r := "0", ""
		if rtt.Valid {
			ok, r = "1", strconv.FormatFloat(rtt.Float64, 'f', 3, 64)
		}
		cw.Write([]string{time.UnixMilli(ts).Format(timeLayout), name, role, typ, host, ok, r, errText.String})
	}
	cw.Flush()
	return errors.Join(rows.Err(), cw.Error())
}

func (d *DB) meta(ctx context.Context, q execQuerier, key string) (string, error) {
	var v string
	err := q.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (d *DB) setMeta(ctx context.Context, q execQuerier, key, value string) error {
	_, err := q.ExecContext(ctx, `INSERT INTO meta (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// watermark: every source row before it has been rolled into the tier.
func (d *DB) watermark(ctx context.Context, q execQuerier, tierName string) (int64, error) {
	v, err := d.meta(ctx, q, "wm_"+tierName)
	if err != nil || v == "" {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func (d *DB) setWatermark(ctx context.Context, q execQuerier, tierName string, v int64) error {
	return d.setMeta(ctx, q, "wm_"+tierName, strconv.FormatInt(v, 10))
}

// Earliest returns the oldest timestamp held in any table, or zero when empty.
func (d *DB) Earliest(ctx context.Context) (time.Time, error) {
	q := `SELECT MIN(v) FROM (SELECT MIN(ts) AS v FROM samples`
	for _, t := range tiers {
		q += fmt.Sprintf(` UNION ALL SELECT MIN(bucket) FROM agg_%s`, t.name)
	}
	q += `)`
	var v sql.NullInt64
	if err := d.db.QueryRowContext(ctx, q).Scan(&v); err != nil || !v.Valid {
		return time.Time{}, err
	}
	return time.UnixMilli(v.Int64), nil
}
