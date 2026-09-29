package storage

import (
	"bufio"
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const timeLayout = "2006-01-02T15:04:05.000Z07:00"

// ImportCSV loads samples-*.csv and outages.csv written by earlier versions. It only runs
// once, into an empty database, so rollup watermarks never need to move backwards.
func (d *DB) ImportCSV(ctx context.Context, dir string) (int, error) {
	done, err := d.meta(ctx, d.db, "csv_imported")
	if err != nil || done != "" {
		return 0, err
	}
	var existing int
	if err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM samples LIMIT 1)`).Scan(&existing); err != nil {
		return 0, err
	}
	total := 0
	if existing == 0 {
		files, _ := filepath.Glob(filepath.Join(dir, "samples-*.csv"))
		sort.Strings(files)
		for _, f := range files {
			n, err := d.importSamples(ctx, f)
			total += n
			if err != nil {
				return total, err
			}
		}
		if err := d.importOutages(ctx, filepath.Join(dir, "outages.csv")); err != nil {
			return total, err
		}
	}
	return total, d.setMeta(ctx, d.db, "csv_imported", time.Now().Format(time.RFC3339))
}

func readCSV(path string, fn func(rec []string) error) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			continue // partial line from a crash
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

func (d *DB) importSamples(ctx context.Context, path string) (int, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt := tx.Stmt(d.insSample)
	n := 0
	err = readCSV(path, func(rec []string) error {
		// time,target,role,type,host,ok,rtt_ms,error
		if len(rec) < 8 || rec[0] == "time" {
			return nil
		}
		t, err := time.Parse(timeLayout, rec[0])
		if err != nil {
			return nil
		}
		id, err := d.ensureTarget(tx, rec[1], rec[2], rec[3], rec[4], false)
		if err != nil {
			return err
		}
		var rtt, errText any
		if rec[5] == "1" {
			v, err := strconv.ParseFloat(rec[6], 64)
			if err != nil {
				return nil
			}
			rtt = v
		} else if rec[7] != "" {
			errText = rec[7]
		}
		if _, err := stmt.ExecContext(ctx, t.UnixMilli(), id, rtt, errText); err != nil {
			return err
		}
		n++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

func (d *DB) importOutages(ctx context.Context, path string) error {
	return readCSV(path, func(rec []string) error {
		// target,role,start,end,duration_s,lost_probes
		if len(rec) < 6 || rec[0] == "target" {
			return nil
		}
		start, err1 := time.Parse(timeLayout, rec[2])
		end, err2 := time.Parse(timeLayout, rec[3])
		lost, err3 := strconv.Atoi(rec[5])
		if err1 != nil || err2 != nil || err3 != nil {
			return nil
		}
		_, err := d.db.ExecContext(ctx, `INSERT INTO outages (target, role, start_ts, end_ts, lost) VALUES (?, ?, ?, ?, ?)`,
			rec[0], rec[1], start.UnixMilli(), end.UnixMilli(), lost)
		return err
	})
}
