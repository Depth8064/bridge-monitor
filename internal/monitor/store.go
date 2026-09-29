package monitor

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
)

const (
	timeLayout = "2006-01-02T15:04:05.000Z07:00"
	dayLayout  = "2006-01-02"
)

var (
	sampleHeader = []string{"time", "target", "role", "type", "host", "ok", "rtt_ms", "error"}
	outageHeader = []string{"target", "role", "start", "end", "duration_s", "lost_probes"}
)

// Store appends every probe to daily CSV files and closed outages to outages.csv.
type Store struct {
	dir     string
	targets map[string]config.Target

	mu  sync.Mutex
	day string
	f   *os.File
	w   *csv.Writer
	of  *os.File
	ow  *csv.Writer
}

func OpenStore(dir string, targets []config.Target) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, targets: map[string]config.Target{}}
	for _, t := range targets {
		s.targets[t.Name] = t
	}
	var err error
	s.of, s.ow, err = openCSV(filepath.Join(dir, "outages.csv"), outageHeader)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func openCSV(path string, header []string) (*os.File, *csv.Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	w := csv.NewWriter(f)
	if st.Size() == 0 {
		w.Write(header)
		w.Flush()
	}
	return f, w, w.Error()
}

func (s *Store) WriteRound(t time.Time, samples []Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if day := t.Format(dayLayout); day != s.day {
		if s.f != nil {
			s.f.Close()
		}
		f, w, err := openCSV(filepath.Join(s.dir, "samples-"+day+".csv"), sampleHeader)
		if err != nil {
			s.f, s.w, s.day = nil, nil, ""
			return err
		}
		s.f, s.w, s.day = f, w, day
	}
	ts := t.Format(timeLayout)
	for _, smp := range samples {
		tg := s.targets[smp.Target]
		ok, rtt := "0", ""
		if smp.OK {
			ok = "1"
			rtt = strconv.FormatFloat(float64(smp.RTT.Microseconds())/1000, 'f', 3, 64)
		}
		s.w.Write([]string{ts, smp.Target, tg.Role, tg.Type, tg.Host, ok, rtt, smp.Err})
	}
	s.w.Flush()
	return s.w.Error()
}

func (s *Store) WriteOutage(o Outage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ow.Write([]string{
		o.Target, o.Role, o.Start.Format(timeLayout), o.End.Format(timeLayout),
		strconv.FormatFloat(o.Duration, 'f', 3, 64), strconv.Itoa(o.Lost),
	})
	s.ow.Flush()
	return s.ow.Error()
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	if s.f != nil {
		errs = append(errs, s.f.Close())
	}
	errs = append(errs, s.of.Close())
	return errors.Join(errs...)
}

// Replay feeds previously recorded rounds newer than since into fn, oldest first.
func Replay(dir string, since time.Time, fn func(time.Time, []Sample)) (int, error) {
	files, err := filepath.Glob(filepath.Join(dir, "samples-*.csv"))
	if err != nil {
		return 0, err
	}
	sort.Strings(files)
	sinceDay := since.Format(dayLayout)
	total := 0
	for _, f := range files {
		day := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "samples-"), ".csv")
		if day < sinceDay {
			continue
		}
		n, err := replayFile(f, since, fn)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func replayFile(path string, since time.Time, fn func(time.Time, []Sample)) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1

	var (
		curTS string
		cur   time.Time
		bad   bool
		batch []Sample
		n     int
	)
	flush := func() {
		if len(batch) > 0 && !bad && !cur.Before(since) {
			fn(cur, batch)
			n++
		}
		batch = nil
	}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) < len(sampleHeader) || rec[0] == "time" {
			continue
		}
		if rec[0] != curTS {
			flush()
			curTS = rec[0]
			cur, err = time.Parse(timeLayout, rec[0])
			bad = err != nil
		}
		smp := Sample{Target: rec[1], OK: rec[5] == "1", Err: rec[7]}
		if smp.OK {
			ms, _ := strconv.ParseFloat(rec[6], 64)
			smp.RTT = time.Duration(ms * float64(time.Millisecond))
		}
		batch = append(batch, smp)
	}
	flush()
	return n, nil
}
