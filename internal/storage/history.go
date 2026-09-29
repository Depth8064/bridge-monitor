package storage

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
	"github.com/Depth8064/bridge-monitor/internal/model"
	"github.com/Depth8064/bridge-monitor/internal/monitor"
)

// History answers dashboard queries for windows longer than the engine keeps in memory,
// stitching the coarsest suitable tier with finer tiers and the raw tail.
type History struct {
	db  *DB
	eng *monitor.Engine
	cfg *config.Config
	ttl time.Duration

	mu    sync.Mutex
	cache map[string]memo
}

type memo struct {
	at time.Time
	v  any
}

func NewHistory(db *DB, eng *monitor.Engine, cfg *config.Config) *History {
	return &History{db: db, eng: eng, cfg: cfg, ttl: 15 * time.Second, cache: map[string]memo{}}
}

// Handles reports whether window (0 = everything) is beyond the engine's in-memory range.
func (h *History) Handles(window time.Duration) bool {
	return window <= 0 || window > h.cfg.Retention.Duration
}

func (h *History) memoize(key string, build func() (any, error)) (any, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m, ok := h.cache[key]; ok && time.Since(m.at) < h.ttl {
		return m.v, nil
	}
	v, err := build()
	if err != nil {
		return nil, err
	}
	for k, m := range h.cache {
		if time.Since(m.at) >= h.ttl {
			delete(h.cache, k)
		}
	}
	h.cache[key] = memo{time.Now(), v}
	return v, nil
}

type span struct {
	from, to int64 // ms
	tier     int
	earliest time.Time
}

func (h *History) span(ctx context.Context, window time.Duration) (span, error) {
	now := time.Now().UnixMilli()
	earliest, err := h.db.Earliest(ctx)
	if err != nil {
		return span{}, err
	}
	from := now - window.Milliseconds()
	if window <= 0 {
		from = now - time.Minute.Milliseconds()
		if !earliest.IsZero() {
			from = min(from, earliest.UnixMilli())
		}
	}
	return span{from: from, to: now, tier: h.pickTier(now - from), earliest: earliest}, nil
}

// pickTier returns the finest tier that still holds the whole window at a sane row count.
func (h *History) pickTier(spanMs int64) int {
	st := h.cfg.Storage
	rets := []time.Duration{st.Minute.Duration, st.Hour.Duration, st.Day.Duration}
	for i, t := range tiers {
		if (rets[i] == 0 || rets[i].Milliseconds() >= spanMs) && spanMs/t.size <= 20000 {
			return i
		}
	}
	return len(tiers) - 1
}

// collect visits every aggregate in [from, to): tier k where rolled up, finer tiers after its
// watermark, and raw samples after the finest watermark.
func (h *History) collect(ctx context.Context, from, to int64, k int, fn func(bucket, tid int64, a *Agg), vfn func(bucket int64, v *VAgg)) error {
	wms := make([]int64, len(tiers))
	for i, t := range tiers {
		wm, err := h.db.watermark(ctx, h.db.db, t.name)
		if err != nil {
			return err
		}
		wms[i] = wm
	}
	lo := floorTo(from, tiers[k].size)
	for j := k; j >= 0; j-- {
		if hi := min(wms[j], to); hi > lo {
			if err := queryTier(ctx, h.db.db, tiers[j], lo, hi, fn, vfn); err != nil {
				return err
			}
		}
		lo = max(lo, wms[j])
	}
	if lo >= to {
		return nil
	}
	gap := max(10*h.cfg.Interval.Duration, 30*time.Second).Milliseconds()
	acc := newRawAcc(tiers[0].size, gap, h.cfg.SpikeMs, h.cfg.InternetIsRemote(), h.db.roleMap(), nil)
	if err := scanRaw(ctx, h.db.db, lo, to, acc); err != nil {
		return err
	}
	for k, a := range acc.aggs {
		fn(k.bucket, k.tid, a)
	}
	for b, v := range acc.vaggs {
		vfn(b, v)
	}
	return nil
}

func (h *History) names() map[int64]string {
	h.db.mu.RLock()
	defer h.db.mu.RUnlock()
	m := make(map[int64]string, len(h.db.ids))
	for name, id := range h.db.ids {
		m[id] = name
	}
	return m
}

// ---- state ----

type stateData struct {
	sp      span
	aggs    map[string]*Agg
	v       VAgg
	outages []model.Outage
}

func (h *History) State(ctx context.Context, window time.Duration) (monitor.State, error) {
	st := h.eng.State(window)
	d, err := h.memoize(fmt.Sprintf("state:%d", window), func() (any, error) { return h.buildState(ctx, window) })
	if err != nil {
		return st, err
	}
	sd := d.(*stateData)
	from := sd.sp.from
	st.WindowS = float64(time.Now().UnixMilli()-from) / 1000
	if !sd.sp.earliest.IsZero() {
		st.DataSince = sd.sp.earliest
	}

	var ongoing []model.Outage
	for _, o := range st.Outages {
		if o.Ongoing {
			ongoing = append(ongoing, o)
		}
	}
	all := append(slices.Clone(ongoing), sd.outages...)
	byTarget := map[string][]model.Outage{}
	for _, o := range all {
		byTarget[o.Target] = append(byTarget[o.Target], o)
	}

	st.Targets = slices.Clone(st.Targets)
	for i := range st.Targets {
		st.Targets[i].Window = h.stats(sd.aggs[st.Targets[i].Name], byTarget[st.Targets[i].Name], from)
	}
	st.Verdict = h.verdict(&sd.v, byTarget[model.BridgeTarget], from)
	st.Outages = all[:min(len(all), 200)]
	return st, nil
}

func (h *History) buildState(ctx context.Context, window time.Duration) (*stateData, error) {
	sp, err := h.span(ctx, window)
	if err != nil {
		return nil, err
	}
	names := h.names()
	sd := &stateData{sp: sp, aggs: map[string]*Agg{}}
	err = h.collect(ctx, sp.from, sp.to, sp.tier,
		func(_, tid int64, a *Agg) {
			name := names[tid]
			if cur := sd.aggs[name]; cur != nil {
				cur.Merge(a)
			} else {
				sd.aggs[name] = a
			}
		},
		func(_ int64, v *VAgg) { sd.v.Merge(v) })
	if err != nil {
		return nil, err
	}
	sd.outages, err = h.db.outages(ctx, sp.from, 0)
	return sd, err
}

func pct(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

// outageTotals clips outages to the window and returns count, downtime and longest in seconds.
func outageTotals(outs []model.Outage, from int64) (n int, total, longest float64) {
	for _, o := range outs {
		start := max(o.Start.UnixMilli(), from)
		d := float64(o.End.UnixMilli()-start) / 1000
		if d < 0 {
			continue
		}
		n++
		total += d
		longest = max(longest, d)
	}
	return
}

func (h *History) stats(a *Agg, outs []model.Outage, from int64) monitor.Stats {
	var s monitor.Stats
	if a != nil {
		s.Sent, s.Lost = int(a.Sent), int(a.Lost)
		s.LossPct = pct(a.Lost, a.Sent)
		if r := a.Recv(); r > 0 {
			q := func(p float64) float64 { return math.Min(math.Max(a.Hist.Quantile(p), a.Min), a.Max) }
			s.RTTAvg = a.Sum / float64(r)
			s.RTTMin, s.RTTMax = a.Min, a.Max
			s.RTTP50, s.RTTP95, s.RTTP99 = q(.50), q(.95), q(.99)
		}
		if a.JitN > 0 {
			s.Jitter = a.JitSum / float64(a.JitN)
		}
		s.Spikes, s.LossBursts, s.MaxBurst = int(a.Spikes), int(a.Bursts), int(a.MaxBurst)
		s.Covered = float64(a.Sent) * h.cfg.Interval.Seconds()
	}
	s.Outages, s.Downtime, s.LongestOutage = outageTotals(outs, from)
	if s.Covered > 0 {
		s.Availability = max(0, 100*(1-s.Downtime/s.Covered))
	}
	if s.Outages > 0 {
		s.AvgOutage = s.Downtime / float64(s.Outages)
		s.MTBO = s.Covered / float64(s.Outages)
	}
	return s
}

func (h *History) verdict(v *VAgg, bridgeOuts []model.Outage, from int64) monitor.Verdict {
	out := monitor.Verdict{
		Rounds:          int(v.Rounds),
		HasLocal:        v.LocalN > 0,
		HasRemote:       v.RemoteN > 0,
		HasInternet:     v.InetN > 0,
		LocalFail:       int(v.LocalFail),
		RemoteFail:      int(v.RemoteFail),
		InternetFail:    int(v.InetFail),
		LocalFailPct:    pct(v.LocalFail, v.Rounds),
		RemoteFailPct:   pct(v.RemoteFail, v.Rounds),
		InternetFailPct: pct(v.InetFail, v.Rounds),
		FarFail:         int(v.FarFail),
		BridgeFault:     int(v.BridgeFault),
		BridgeFaultPct:  pct(v.BridgeFault, v.Rounds),
		AttributionPct:  pct(v.BridgeFault, v.FarFail),
		InetBridge:      int(v.InetBridge),
		InetBridgePct:   pct(v.InetBridge, v.InetFail),
		Upstream:        int(v.Upstream),
		UpstreamPct:     pct(v.Upstream, v.Rounds),
		Covered:         float64(v.Rounds) * h.cfg.Interval.Seconds(),
	}
	out.BridgeOutages, out.BridgeDowntime, out.BridgeLongest = outageTotals(bridgeOuts, from)
	if out.BridgeOutages > 0 {
		out.BridgeAvg = out.BridgeDowntime / float64(out.BridgeOutages)
		out.BridgeMTBO = out.Covered / float64(out.BridgeOutages)
	}
	return out
}

// ---- series ----

func (h *History) Series(ctx context.Context, window time.Duration, buckets int) (monitor.Series, error) {
	v, err := h.memoize(fmt.Sprintf("series:%d:%d", window, buckets), func() (any, error) {
		return h.buildSeries(ctx, window, buckets)
	})
	if err != nil {
		return monitor.Series{}, err
	}
	return v.(monitor.Series), nil
}

func f64(v float64) *float64 {
	v = math.Round(v*1000) / 1000
	return &v
}

func (h *History) buildSeries(ctx context.Context, window time.Duration, buckets int) (monitor.Series, error) {
	sp, err := h.span(ctx, window)
	if err != nil {
		return monitor.Series{}, err
	}
	size := tiers[sp.tier].size
	step := max((sp.to-sp.from+int64(buckets)-1)/int64(buckets), size)
	step = (step + size - 1) / size * size
	start := floorTo(sp.from, step)
	n := int((sp.to - start + step - 1) / step)

	names := h.names()
	idx := map[string]int{}
	hasRemote, hasInet := false, false
	for i, t := range h.cfg.Targets {
		idx[t.Name] = i
		hasRemote = hasRemote || t.Role == config.RoleRemote
		hasInet = hasInet || t.Role == config.RoleInternet
	}
	type acc struct{ sent, lost, sum, max []float64 }
	accs := make([]acc, len(h.cfg.Targets))
	for i := range accs {
		accs[i] = acc{make([]float64, n), make([]float64, n), make([]float64, n), make([]float64, n)}
	}
	rounds, faults := make([]int64, n), make([]int64, n)

	err = h.collect(ctx, sp.from, sp.to, sp.tier,
		func(bucket, tid int64, a *Agg) {
			i, ok := idx[names[tid]]
			b := int((bucket - start) / step)
			if !ok || b < 0 || b >= n {
				return
			}
			c := accs[i]
			c.sent[b] += float64(a.Sent)
			c.lost[b] += float64(a.Lost)
			c.sum[b] += a.Sum
			if a.Recv() > 0 {
				c.max[b] = math.Max(c.max[b], a.Max)
			}
		},
		func(bucket int64, v *VAgg) {
			if b := int((bucket - start) / step); b >= 0 && b < n {
				rounds[b] += v.Rounds
				faults[b] += v.BridgeFault
			}
		})
	if err != nil {
		return monitor.Series{}, err
	}

	out := monitor.Series{Start: start, Step: step, Buckets: n}
	for i, t := range h.cfg.Targets {
		c := accs[i]
		st := monitor.SeriesTarget{Name: t.Name, Avg: make([]*float64, n), Max: make([]*float64, n), Loss: make([]*float64, n)}
		for b := range n {
			if c.sent[b] == 0 {
				continue
			}
			st.Loss[b] = f64(100 * c.lost[b] / c.sent[b])
			if recv := c.sent[b] - c.lost[b]; recv > 0 {
				st.Avg[b] = f64(c.sum[b] / recv)
				st.Max[b] = f64(c.max[b])
			}
		}
		out.Targets = append(out.Targets, st)
	}
	if hasRemote || (h.cfg.InternetIsRemote() && hasInet) {
		out.Bridge = make([]*float64, n)
		for b := range n {
			if rounds[b] > 0 {
				out.Bridge[b] = f64(pct(faults[b], rounds[b]))
			}
		}
	}
	return out, nil
}

// Outages returns ongoing outages from the engine followed by stored ones, newest first.
func (h *History) Outages(ctx context.Context, window time.Duration) ([]model.Outage, error) {
	sp, err := h.span(ctx, window)
	if err != nil {
		return nil, err
	}
	var res []model.Outage
	for _, o := range h.eng.Outages(window) {
		if o.Ongoing {
			res = append(res, o)
		}
	}
	stored, err := h.db.outages(ctx, sp.from, 0)
	return append(res, stored...), err
}
