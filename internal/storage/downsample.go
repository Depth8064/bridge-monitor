package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
)

// Downsampler rolls raw samples into 1m → 1h → 1d tiers and prunes each tier by its retention.
// Every step runs in a transaction with its watermark, so it is safe to interrupt and re-run.
type Downsampler struct {
	db  *DB
	cfg *config.Config
}

func NewDownsampler(db *DB, cfg *config.Config) *Downsampler {
	return &Downsampler{db: db, cfg: cfg}
}

func (s *Downsampler) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var lastPrune time.Time
	for {
		now := time.Now()
		if err := s.Rollup(ctx, now); err != nil && ctx.Err() == nil {
			log.Printf("downsample: %v", err)
		}
		if now.Sub(lastPrune) >= time.Hour {
			if err := s.Prune(ctx, now); err != nil && ctx.Err() == nil {
				log.Printf("prune: %v", err)
			}
			lastPrune = now
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Downsampler) gapMs() int64 {
	return max(10*s.cfg.Interval.Duration, 30*time.Second).Milliseconds()
}

func (s *Downsampler) Rollup(ctx context.Context, now time.Time) error {
	if err := s.rollupRaw(ctx, now); err != nil {
		return fmt.Errorf("raw->%s: %w", tiers[0].name, err)
	}
	for i := 1; i < len(tiers); i++ {
		if err := s.rollupTier(ctx, tiers[i-1], tiers[i]); err != nil {
			return fmt.Errorf("%s->%s: %w", tiers[i-1].name, tiers[i].name, err)
		}
	}
	return nil
}

func (s *Downsampler) rollupRaw(ctx context.Context, now time.Time) error {
	t := tiers[0]
	// Rounds commit up to one timeout late; don't close a minute that could still receive samples.
	cutoff := floorTo(now.Add(-s.cfg.Timeout.Duration-5*time.Second).UnixMilli(), t.size)
	wm, err := s.db.watermark(ctx, s.db.db, t.name)
	if err != nil {
		return err
	}
	if wm == 0 {
		var first sql.NullInt64
		if err := s.db.db.QueryRowContext(ctx, `SELECT MIN(ts) FROM samples`).Scan(&first); err != nil || !first.Valid {
			return err
		}
		wm = floorTo(first.Int64, t.size)
	}
	chunk := (6 * time.Hour).Milliseconds()
	for wm < cutoff {
		end := min(wm+chunk, cutoff)
		if err := s.rawChunk(ctx, wm, end); err != nil {
			return err
		}
		wm = end
	}
	return nil
}

func (s *Downsampler) rawChunk(ctx context.Context, from, to int64) error {
	t := tiers[0]
	tx, err := s.db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	carries, err := loadCarry(ctx, tx)
	if err != nil {
		return err
	}
	acc := newRawAcc(t.size, s.gapMs(), s.cfg.SpikeMs, s.cfg.InternetIsRemote(), s.db.roleMap(), carries)
	if err := scanRaw(ctx, tx, from, to, acc); err != nil {
		return err
	}
	if err := writeAggs(ctx, tx, t.name, acc.aggs); err != nil {
		return err
	}
	if err := writeVerdicts(ctx, tx, t.name, acc.vaggs); err != nil {
		return err
	}
	if err := saveCarry(ctx, tx, acc.carry); err != nil {
		return err
	}
	if err := s.db.setWatermark(ctx, tx, t.name, to); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Downsampler) rollupTier(ctx context.Context, src, dst tier) error {
	srcWm, err := s.db.watermark(ctx, s.db.db, src.name)
	if err != nil {
		return err
	}
	cutoff := floorTo(srcWm, dst.size)
	wm, err := s.db.watermark(ctx, s.db.db, dst.name)
	if err != nil {
		return err
	}
	if wm == 0 {
		var first sql.NullInt64
		if err := s.db.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT MIN(bucket) FROM agg_%s`, src.name)).Scan(&first); err != nil || !first.Valid {
			return err
		}
		wm = floorTo(first.Int64, dst.size)
	}
	chunk := 48 * dst.size
	for wm < cutoff {
		end := min(wm+chunk, cutoff)
		if err := s.tierChunk(ctx, src, dst, wm, end); err != nil {
			return err
		}
		wm = end
	}
	return nil
}

func (s *Downsampler) tierChunk(ctx context.Context, src, dst tier, from, to int64) error {
	tx, err := s.db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	aggs := map[aggKey]*Agg{}
	vaggs := map[int64]*VAgg{}
	err = queryTier(ctx, tx, src, from, to,
		func(bucket, tid int64, a *Agg) {
			k := aggKey{floorTo(bucket, dst.size), tid}
			if cur := aggs[k]; cur != nil {
				cur.Merge(a)
			} else {
				aggs[k] = a
			}
		},
		func(bucket int64, v *VAgg) {
			b := floorTo(bucket, dst.size)
			if cur := vaggs[b]; cur != nil {
				cur.Merge(v)
			} else {
				vaggs[b] = v
			}
		})
	if err != nil {
		return err
	}
	if err := writeAggs(ctx, tx, dst.name, aggs); err != nil {
		return err
	}
	if err := writeVerdicts(ctx, tx, dst.name, vaggs); err != nil {
		return err
	}
	if err := s.db.setWatermark(ctx, tx, dst.name, to); err != nil {
		return err
	}
	return tx.Commit()
}

// Prune deletes data older than each tier's retention, but never data that the next tier
// hasn't absorbed yet.
func (s *Downsampler) Prune(ctx context.Context, now time.Time) error {
	st := s.cfg.Storage
	nowMs := now.UnixMilli()
	wm0, err := s.db.watermark(ctx, s.db.db, tiers[0].name)
	if err != nil {
		return err
	}
	if cut := min(nowMs-st.Raw.Milliseconds(), wm0); cut > 0 {
		if err := s.deleteBefore(ctx, "samples", "ts", cut, (6 * time.Hour).Milliseconds()); err != nil {
			return err
		}
	}
	rets := []time.Duration{st.Minute.Duration, st.Hour.Duration, st.Day.Duration}
	for i, t := range tiers {
		if rets[i] == 0 {
			continue
		}
		cut := nowMs - rets[i].Milliseconds()
		if i+1 < len(tiers) {
			next, err := s.db.watermark(ctx, s.db.db, tiers[i+1].name)
			if err != nil {
				return err
			}
			cut = min(cut, next)
		}
		if cut <= 0 {
			continue
		}
		for _, table := range []string{"agg_" + t.name, "verdict_" + t.name} {
			if err := s.deleteBefore(ctx, table, "bucket", cut, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteBefore removes rows with col < cut, in steps so a large backlog doesn't hold the write lock.
func (s *Downsampler) deleteBefore(ctx context.Context, table, col string, cut, step int64) error {
	if step <= 0 {
		_, err := s.db.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s < ?`, table, col), cut)
		return err
	}
	var first sql.NullInt64
	if err := s.db.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT MIN(%s) FROM %s`, col, table)).Scan(&first); err != nil || !first.Valid {
		return err
	}
	for end := first.Int64 + step; ; end += step {
		end = min(end, cut)
		if _, err := s.db.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s < ?`, table, col), end); err != nil {
			return err
		}
		if end >= cut {
			return nil
		}
	}
}

// ---- shared row helpers ----

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func scanRaw(ctx context.Context, q querier, from, to int64, acc *rawAcc) error {
	rows, err := q.QueryContext(ctx, `SELECT ts, target_id, rtt FROM samples WHERE ts >= ? AND ts < ? ORDER BY ts, target_id`, from, to)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var ts, tid int64
		var rtt sql.NullFloat64
		if err := rows.Scan(&ts, &tid, &rtt); err != nil {
			return err
		}
		v := -1.0
		if rtt.Valid {
			v = rtt.Float64
		}
		acc.add(ts, tid, v)
	}
	acc.finish()
	return rows.Err()
}

func queryTier(ctx context.Context, q querier, t tier, from, to int64, fn func(bucket, tid int64, a *Agg), vfn func(bucket int64, v *VAgg)) error {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`
		SELECT bucket, target_id, sent, lost, rtt_sum, rtt_min, rtt_max, jit_sum, jit_n, spikes, bursts, max_burst, hist
		FROM agg_%s WHERE bucket >= ? AND bucket < ?`, t.name), from, to)
	if err != nil {
		return err
	}
	for rows.Next() {
		var bucket, tid int64
		var blob []byte
		a := newAgg()
		if err := rows.Scan(&bucket, &tid, &a.Sent, &a.Lost, &a.Sum, &a.Min, &a.Max, &a.JitSum, &a.JitN,
			&a.Spikes, &a.Bursts, &a.MaxBurst, &blob); err != nil {
			rows.Close()
			return err
		}
		if err := DecodeHist(blob, a.Hist); err != nil {
			rows.Close()
			return err
		}
		fn(bucket, tid, a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	rows, err = q.QueryContext(ctx, fmt.Sprintf(`
		SELECT bucket, rounds, local_n, remote_n, inet_n, local_fail, remote_fail, inet_fail,
			far_fail, bridge_fault, inet_bridge, upstream
		FROM verdict_%s WHERE bucket >= ? AND bucket < ?`, t.name), from, to)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var bucket int64
		v := &VAgg{}
		if err := rows.Scan(&bucket, &v.Rounds, &v.LocalN, &v.RemoteN, &v.InetN, &v.LocalFail, &v.RemoteFail,
			&v.InetFail, &v.FarFail, &v.BridgeFault, &v.InetBridge, &v.Upstream); err != nil {
			return err
		}
		vfn(bucket, v)
	}
	return rows.Err()
}

func writeAggs(ctx context.Context, q querier, tierName string, aggs map[aggKey]*Agg) error {
	stmt := fmt.Sprintf(`INSERT OR REPLACE INTO agg_%s
		(bucket, target_id, sent, lost, rtt_sum, rtt_min, rtt_max, jit_sum, jit_n, spikes, bursts, max_burst, hist)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, tierName)
	for k, a := range aggs {
		if _, err := q.ExecContext(ctx, stmt, k.bucket, k.tid, a.Sent, a.Lost, a.Sum, a.Min, a.Max,
			a.JitSum, a.JitN, a.Spikes, a.Bursts, a.MaxBurst, a.Hist.Encode()); err != nil {
			return err
		}
	}
	return nil
}

func writeVerdicts(ctx context.Context, q querier, tierName string, vaggs map[int64]*VAgg) error {
	stmt := fmt.Sprintf(`INSERT OR REPLACE INTO verdict_%s
		(bucket, rounds, local_n, remote_n, inet_n, local_fail, remote_fail, inet_fail, far_fail, bridge_fault, inet_bridge, upstream)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, tierName)
	for b, v := range vaggs {
		if _, err := q.ExecContext(ctx, stmt, b, v.Rounds, v.LocalN, v.RemoteN, v.InetN, v.LocalFail, v.RemoteFail,
			v.InetFail, v.FarFail, v.BridgeFault, v.InetBridge, v.Upstream); err != nil {
			return err
		}
	}
	return nil
}

func loadCarry(ctx context.Context, q querier) (map[int64]*carry, error) {
	rows, err := q.QueryContext(ctx, `SELECT target_id, prev, run, last FROM rollup_carry`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[int64]*carry{}
	for rows.Next() {
		var tid int64
		c := &carry{}
		if err := rows.Scan(&tid, &c.prev, &c.run, &c.last); err != nil {
			return nil, err
		}
		m[tid] = c
	}
	return m, rows.Err()
}

func saveCarry(ctx context.Context, q querier, m map[int64]*carry) error {
	for tid, c := range m {
		if _, err := q.ExecContext(ctx, `INSERT OR REPLACE INTO rollup_carry (target_id, prev, run, last) VALUES (?, ?, ?, ?)`,
			tid, c.prev, c.run, c.last); err != nil {
			return err
		}
	}
	return nil
}
