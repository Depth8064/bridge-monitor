package monitor

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
	"github.com/Depth8064/bridge-monitor/internal/probe"
)

type Monitor struct {
	cfg     *config.Config
	eng     *Engine
	store   *Store
	probers []probe.Prober
}

func New(cfg *config.Config, eng *Engine, store *Store) *Monitor {
	m := &Monitor{cfg: cfg, eng: eng, store: store}
	for _, t := range cfg.Targets {
		m.probers = append(m.probers, probe.New(t, cfg.Timeout.Duration, cfg.ICMPPrivileged))
	}
	return m
}

// Run fires a probe round every interval until ctx is cancelled. Rounds may
// overlap when timeout > interval, so results are committed strictly in order.
func (m *Monitor) Run(ctx context.Context) {
	type result struct {
		seq     uint64
		t       time.Time
		samples []Sample
	}
	results := make(chan result, 16)
	done := make(chan struct{})
	go func() {
		defer close(done)
		pending := map[uint64]result{}
		var next uint64
		for r := range results {
			pending[r.seq] = r
			for {
				p, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				next++
				m.commit(p.t, p.samples)
			}
		}
	}()

	// In-flight probes are bounded by the timeout; don't let shutdown turn them into false losses.
	probeCtx := context.WithoutCancel(ctx)
	var wg sync.WaitGroup
	var seq uint64
	fire := func(t time.Time) {
		s := seq
		seq++
		t = t.Truncate(time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- result{s, t, m.probeAll(probeCtx)}
		}()
	}

	ticker := time.NewTicker(m.cfg.Interval.Duration)
	defer ticker.Stop()
	fire(time.Now())
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			close(results)
			<-done
			return
		case t := <-ticker.C:
			fire(t)
		}
	}
}

func (m *Monitor) probeAll(ctx context.Context) []Sample {
	out := make([]Sample, len(m.probers))
	var wg sync.WaitGroup
	for i, p := range m.probers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := p.Probe(ctx)
			out[i] = Sample{Target: m.cfg.Targets[i].Name, OK: r.OK, RTT: r.RTT, Err: r.Err}
		}()
	}
	wg.Wait()
	return out
}

func (m *Monitor) commit(t time.Time, samples []Sample) {
	if err := m.store.WriteRound(t, samples); err != nil {
		log.Printf("write samples: %v", err)
	}
	m.eng.Record(t, samples)
}
