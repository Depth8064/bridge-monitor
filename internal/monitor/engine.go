package monitor

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
)

type Sample struct {
	Target string
	OK     bool
	RTT    time.Duration
	Err    string
}

type Outage struct {
	Target   string    `json:"target"`
	Role     string    `json:"role"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Duration float64   `json:"duration_s"`
	Lost     int       `json:"lost"`
	Ongoing  bool      `json:"ongoing"`
}

func newOutage(t config.Target, start, end int64, lost int, ongoing bool) Outage {
	return Outage{
		Target: t.Name, Role: t.Role,
		Start: time.Unix(0, start), End: time.Unix(0, end),
		Duration: float64(end-start) / 1e9, Lost: lost, Ongoing: ongoing,
	}
}

type point struct {
	t   int64
	rtt float32 // ms; negative = lost
}

type tstate struct {
	cfg        config.Target
	pts        []point
	lastErr    string
	stateSince int64
	failRun    int
	runStart   int64
}

func (s *tstate) last() (point, bool) {
	if len(s.pts) == 0 {
		return point{}, false
	}
	return s.pts[len(s.pts)-1], true
}

func (s *tstate) add(ts int64, smp Sample, threshold int, gap int64) (Outage, bool) {
	var out Outage
	closed := false
	last, hasLast := s.last()
	// Monitor was not running across the gap: end the run rather than counting the gap as downtime.
	if hasLast && ts-last.t > gap && s.failRun > 0 {
		out, closed = s.closeRun(last.t, threshold)
	}
	p := point{t: ts, rtt: -1}
	if smp.OK {
		p.rtt = float32(smp.RTT.Seconds() * 1000)
		if s.failRun > 0 {
			out, closed = s.closeRun(ts, threshold)
		}
	} else {
		if s.failRun == 0 {
			s.runStart = ts
		}
		s.failRun++
	}
	if !hasLast || (last.rtt >= 0) != smp.OK {
		s.stateSince = ts
	}
	s.lastErr = smp.Err
	s.pts = append(s.pts, p)
	return out, closed
}

func (s *tstate) closeRun(end int64, threshold int) (Outage, bool) {
	n := s.failRun
	s.failRun = 0
	if n < threshold {
		return Outage{}, false
	}
	return newOutage(s.cfg, s.runStart, end, n, false), true
}

// round holds per-role health for one probe round: -1 no targets, 0 any failed, 1 all ok.
type round struct {
	t                   int64
	local, remote, inet int8
}

func (r *round) apply(role string, ok bool) {
	var p *int8
	switch role {
	case config.RoleLocal:
		p = &r.local
	case config.RoleRemote:
		p = &r.remote
	case config.RoleInternet:
		p = &r.inet
	default:
		return
	}
	if !ok {
		*p = 0
	} else if *p == -1 {
		*p = 1
	}
}

func (r round) bridgeFault() bool { return r.remote == 0 && r.local != 0 && r.inet != 0 }

const maxOutageLog = 10000

type Engine struct {
	cfg     *config.Config
	gap     int64
	started time.Time

	mu      sync.RWMutex
	targets []*tstate
	byName  map[string]*tstate
	rounds  []round
	outages []Outage
	version uint64

	cacheMu      sync.Mutex
	cacheVersion uint64
	cache        map[string]any

	// OnOutage is called (outside the lock) whenever a target outage ends.
	OnOutage func(Outage)
}

func NewEngine(cfg *config.Config) *Engine {
	gap := max(10*cfg.Interval.Duration, 30*time.Second)
	e := &Engine{
		cfg: cfg, gap: int64(gap), started: time.Now(),
		byName: map[string]*tstate{}, cache: map[string]any{},
	}
	for _, t := range cfg.Targets {
		st := &tstate{cfg: t}
		e.targets = append(e.targets, st)
		e.byName[t.Name] = st
	}
	return e
}

func (e *Engine) Record(t time.Time, samples []Sample) {
	ts := t.UnixNano()
	r := round{t: ts, local: -1, remote: -1, inet: -1}
	var closed []Outage
	matched := false

	e.mu.Lock()
	for _, s := range samples {
		st, ok := e.byName[s.Target]
		if !ok {
			continue
		}
		matched = true
		if o, c := st.add(ts, s, e.cfg.OutageThreshold, e.gap); c {
			closed = append(closed, o)
		}
		r.apply(st.cfg.Role, s.OK)
	}
	if matched {
		e.rounds = append(e.rounds, r)
	}
	e.outages = append(e.outages, closed...)
	if n := len(e.outages) - maxOutageLog; n > 0 {
		e.outages = e.outages[n:]
	}
	e.trim(ts - int64(e.cfg.Retention.Duration))
	e.version++
	cb := e.OnOutage
	e.mu.Unlock()

	if cb != nil {
		for _, o := range closed {
			cb(o)
		}
	}
}

func (e *Engine) trim(cutoff int64) {
	for _, st := range e.targets {
		if i := idxPts(st.pts, cutoff); i > 0 {
			st.pts = st.pts[i:]
		}
	}
	if i := idxRounds(e.rounds, cutoff); i > 0 {
		e.rounds = e.rounds[i:]
	}
}

func idxPts(p []point, from int64) int {
	return sort.Search(len(p), func(i int) bool { return p[i].t >= from })
}

func idxRounds(r []round, from int64) int {
	return sort.Search(len(r), func(i int) bool { return r[i].t >= from })
}

// cached memoises results until the next recorded round.
func (e *Engine) cached(key string, build func() any) any {
	e.mu.RLock()
	v := e.version
	e.mu.RUnlock()
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	if e.cacheVersion != v {
		clear(e.cache)
		e.cacheVersion = v
	}
	if r, ok := e.cache[key]; ok {
		return r
	}
	r := build()
	e.cache[key] = r
	return r
}

func (e *Engine) windowStart(now int64, window time.Duration) int64 {
	if window > 0 {
		return now - int64(window)
	}
	from := now
	for _, st := range e.targets {
		if len(st.pts) > 0 && st.pts[0].t < from {
			from = st.pts[0].t
		}
	}
	return min(from, now-int64(time.Minute))
}

// ---- stats ----

type Stats struct {
	Sent          int     `json:"sent"`
	Lost          int     `json:"lost"`
	LossPct       float64 `json:"loss_pct"`
	RTTMin        float64 `json:"rtt_min"`
	RTTAvg        float64 `json:"rtt_avg"`
	RTTP50        float64 `json:"rtt_p50"`
	RTTP95        float64 `json:"rtt_p95"`
	RTTP99        float64 `json:"rtt_p99"`
	RTTMax        float64 `json:"rtt_max"`
	Jitter        float64 `json:"jitter"`
	Spikes        int     `json:"spikes"`
	LossBursts    int     `json:"loss_bursts"`
	MaxBurst      int     `json:"max_burst"`
	Outages       int     `json:"outages"`
	Downtime      float64 `json:"downtime"`
	AvgOutage     float64 `json:"avg_outage"`
	LongestOutage float64 `json:"longest_outage"`
	MTBO          float64 `json:"mtbo"`
	Availability  float64 `json:"availability"`
	Covered       float64 `json:"covered"`
}

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

func quantile(sorted []float64, q float64) float64 {
	return sorted[int(math.Round(q*float64(len(sorted)-1)))]
}

func (e *Engine) computeStats(pts []point, now int64) Stats {
	var s Stats
	if len(pts) == 0 {
		return s
	}
	thr := e.cfg.OutageThreshold
	rtts := make([]float64, 0, len(pts))
	var sum, jsum float64
	jn, run := 0, 0
	prev := -1.0
	var covered, prevT, runStart int64

	endRun := func(end int64) {
		if run == 0 {
			return
		}
		s.LossBursts++
		s.MaxBurst = max(s.MaxBurst, run)
		if run >= thr {
			d := float64(end-runStart) / 1e9
			s.Outages++
			s.Downtime += d
			s.LongestOutage = max(s.LongestOutage, d)
		}
		run = 0
	}

	for i, p := range pts {
		if i > 0 {
			if dt := p.t - prevT; dt > e.gap {
				endRun(prevT)
				prev = -1
			} else {
				covered += dt
			}
		}
		prevT = p.t
		s.Sent++
		if p.rtt < 0 {
			s.Lost++
			if run == 0 {
				runStart = p.t
			}
			run++
			prev = -1
			continue
		}
		endRun(p.t)
		v := float64(p.rtt)
		rtts = append(rtts, v)
		sum += v
		if v > e.cfg.SpikeMs {
			s.Spikes++
		}
		if prev >= 0 {
			jsum += math.Abs(v - prev)
			jn++
		}
		prev = v
	}
	if now-prevT > e.gap {
		endRun(prevT)
		covered += int64(e.cfg.Interval.Duration)
	} else {
		covered += now - prevT
		endRun(now)
	}

	s.LossPct = pct(s.Lost, s.Sent)
	if n := len(rtts); n > 0 {
		s.RTTAvg = sum / float64(n)
		sort.Float64s(rtts)
		s.RTTMin, s.RTTMax = rtts[0], rtts[n-1]
		s.RTTP50, s.RTTP95, s.RTTP99 = quantile(rtts, .50), quantile(rtts, .95), quantile(rtts, .99)
	}
	if jn > 0 {
		s.Jitter = jsum / float64(jn)
	}
	s.Covered = float64(covered) / 1e9
	if s.Covered > 0 {
		s.Availability = max(0, 100*(1-s.Downtime/s.Covered))
	}
	if s.Outages > 0 {
		s.AvgOutage = s.Downtime / float64(s.Outages)
		s.MTBO = s.Covered / float64(s.Outages)
	}
	return s
}

// ---- verdict (cross-target correlation) ----

type Verdict struct {
	Rounds          int     `json:"rounds"`
	HasLocal        bool    `json:"has_local"`
	HasRemote       bool    `json:"has_remote"`
	HasInternet     bool    `json:"has_internet"`
	LocalFail       int     `json:"local_fail"`
	RemoteFail      int     `json:"remote_fail"`
	InternetFail    int     `json:"internet_fail"`
	LocalFailPct    float64 `json:"local_fail_pct"`
	RemoteFailPct   float64 `json:"remote_fail_pct"`
	InternetFailPct float64 `json:"internet_fail_pct"`
	BridgeFault     int     `json:"bridge_fault"`
	BridgeFaultPct  float64 `json:"bridge_fault_pct"`
	AttributionPct  float64 `json:"attribution_pct"`
	BridgeOutages   int     `json:"bridge_outages"`
	BridgeDowntime  float64 `json:"bridge_downtime"`
	BridgeAvg       float64 `json:"bridge_avg"`
	BridgeLongest   float64 `json:"bridge_longest"`
	BridgeMTBO      float64 `json:"bridge_mtbo"`
	Covered         float64 `json:"covered"`
}

func (e *Engine) verdict(rs []round, now int64) Verdict {
	var v Verdict
	if len(rs) == 0 {
		return v
	}
	thr := e.cfg.OutageThreshold
	run := 0
	var runStart, prevT, covered int64
	endRun := func(end int64) {
		if run >= thr {
			d := float64(end-runStart) / 1e9
			v.BridgeOutages++
			v.BridgeDowntime += d
			v.BridgeLongest = max(v.BridgeLongest, d)
		}
		run = 0
	}
	for i, r := range rs {
		if i > 0 {
			if dt := r.t - prevT; dt > e.gap {
				endRun(prevT)
			} else {
				covered += dt
			}
		}
		prevT = r.t
		v.Rounds++
		if r.local >= 0 {
			v.HasLocal = true
			if r.local == 0 {
				v.LocalFail++
			}
		}
		if r.remote >= 0 {
			v.HasRemote = true
			if r.remote == 0 {
				v.RemoteFail++
			}
		}
		if r.inet >= 0 {
			v.HasInternet = true
			if r.inet == 0 {
				v.InternetFail++
			}
		}
		if r.bridgeFault() {
			v.BridgeFault++
			if run == 0 {
				runStart = r.t
			}
			run++
		} else {
			endRun(r.t)
		}
	}
	if now-prevT > e.gap {
		endRun(prevT)
		covered += int64(e.cfg.Interval.Duration)
	} else {
		covered += now - prevT
		endRun(now)
	}
	v.LocalFailPct = pct(v.LocalFail, v.Rounds)
	v.RemoteFailPct = pct(v.RemoteFail, v.Rounds)
	v.InternetFailPct = pct(v.InternetFail, v.Rounds)
	v.BridgeFaultPct = pct(v.BridgeFault, v.Rounds)
	v.AttributionPct = pct(v.BridgeFault, v.RemoteFail)
	v.Covered = float64(covered) / 1e9
	if v.BridgeOutages > 0 {
		v.BridgeAvg = v.BridgeDowntime / float64(v.BridgeOutages)
		v.BridgeMTBO = v.Covered / float64(v.BridgeOutages)
	}
	return v
}

// ---- snapshots ----

type TargetState struct {
	Name       string    `json:"name"`
	Role       string    `json:"role"`
	Type       string    `json:"type"`
	Host       string    `json:"host"`
	Color      string    `json:"color,omitempty"`
	HasData    bool      `json:"has_data"`
	Up         bool      `json:"up"`
	LastRTT    float64   `json:"last_rtt_ms"`
	LastError  string    `json:"last_error"`
	LastSeen   time.Time `json:"last_seen"`
	StateSince time.Time `json:"state_since"`
	FailStreak int       `json:"fail_streak"`
	Window     Stats     `json:"window"`
}

type State struct {
	Now       time.Time     `json:"now"`
	Started   time.Time     `json:"started"`
	DataSince time.Time     `json:"data_since"`
	WindowS   float64       `json:"window_s"`
	IntervalS float64       `json:"interval_s"`
	TimeoutS  float64       `json:"timeout_s"`
	Threshold int           `json:"outage_threshold"`
	SpikeMs   float64       `json:"spike_ms"`
	Targets   []TargetState `json:"targets"`
	Verdict   Verdict       `json:"verdict"`
	Outages   []Outage      `json:"outages"`
}

func (e *Engine) State(window time.Duration) State {
	return e.cached(fmt.Sprintf("state:%d", window), func() any { return e.state(window) }).(State)
}

func (e *Engine) state(window time.Duration) State {
	e.mu.RLock()
	defer e.mu.RUnlock()
	now := time.Now().UnixNano()
	from := e.windowStart(now, window)
	s := State{
		Now: time.Unix(0, now), Started: e.started,
		WindowS:   float64(now-from) / 1e9,
		IntervalS: e.cfg.Interval.Seconds(), TimeoutS: e.cfg.Timeout.Seconds(),
		Threshold: e.cfg.OutageThreshold, SpikeMs: e.cfg.SpikeMs,
	}
	earliest := int64(math.MaxInt64)
	for _, ts := range e.targets {
		out := TargetState{
			Name: ts.cfg.Name, Role: ts.cfg.Role, Type: ts.cfg.Type, Host: ts.cfg.Host, Color: ts.cfg.Color,
			FailStreak: ts.failRun, LastError: ts.lastErr,
		}
		if last, ok := ts.last(); ok {
			out.HasData = true
			out.Up = last.rtt >= 0
			if out.Up {
				out.LastRTT = float64(last.rtt)
			}
			out.LastSeen = time.Unix(0, last.t)
			out.StateSince = time.Unix(0, ts.stateSince)
			earliest = min(earliest, ts.pts[0].t)
		}
		out.Window = e.computeStats(ts.pts[idxPts(ts.pts, from):], now)
		s.Targets = append(s.Targets, out)
	}
	if earliest != math.MaxInt64 {
		s.DataSince = time.Unix(0, earliest)
	}
	s.Verdict = e.verdict(e.rounds[idxRounds(e.rounds, from):], now)
	s.Outages = e.outagesSince(from, now, 200)
	return s
}

func (e *Engine) Outages(window time.Duration) []Outage {
	e.mu.RLock()
	defer e.mu.RUnlock()
	now := time.Now().UnixNano()
	return e.outagesSince(e.windowStart(now, window), now, 0)
}

// outagesSince returns newest-first outages ending after from, ongoing ones first.
func (e *Engine) outagesSince(from, now int64, limit int) []Outage {
	res := []Outage{}
	for _, ts := range e.targets {
		if ts.failRun < e.cfg.OutageThreshold {
			continue
		}
		last, _ := ts.last()
		if now-last.t > e.gap {
			res = append(res, newOutage(ts.cfg, ts.runStart, last.t, ts.failRun, false))
		} else {
			res = append(res, newOutage(ts.cfg, ts.runStart, now, ts.failRun, true))
		}
	}
	for i := len(e.outages) - 1; i >= 0; i-- {
		if limit > 0 && len(res) >= limit {
			break
		}
		o := e.outages[i]
		if o.End.UnixNano() < from {
			break
		}
		res = append(res, o)
	}
	return res
}

// ---- time series ----

type SeriesTarget struct {
	Name string     `json:"name"`
	Avg  []*float64 `json:"avg"`
	Max  []*float64 `json:"max"`
	Loss []*float64 `json:"loss"`
}

type Series struct {
	Start   int64          `json:"start"` // unix ms
	Step    int64          `json:"step"`  // ms
	Buckets int            `json:"buckets"`
	Targets []SeriesTarget `json:"targets"`
	Bridge  []*float64     `json:"bridge,omitempty"`
}

func f64(v float64) *float64 {
	v = math.Round(v*1000) / 1000
	return &v
}

func (e *Engine) Series(window time.Duration, buckets int) Series {
	key := fmt.Sprintf("series:%d:%d", window, buckets)
	return e.cached(key, func() any { return e.series(window, buckets) }).(Series)
}

func (e *Engine) series(window time.Duration, buckets int) Series {
	e.mu.RLock()
	defer e.mu.RUnlock()
	now := time.Now().UnixNano()
	from := e.windowStart(now, window)
	ms := int64(time.Millisecond)
	step := max((now-from+int64(buckets)-1)/int64(buckets), int64(e.cfg.Interval.Duration))
	step = (step + ms - 1) / ms * ms
	start := from / step * step
	n := int((now - start + step - 1) / step)
	out := Series{Start: start / ms, Step: step / ms, Buckets: n}

	hasRemote := false
	for _, ts := range e.targets {
		hasRemote = hasRemote || ts.cfg.Role == config.RoleRemote
		cnt, lost := make([]int, n), make([]int, n)
		sum, mx := make([]float64, n), make([]float64, n)
		for _, p := range ts.pts[idxPts(ts.pts, start):] {
			b := int((p.t - start) / step)
			if b >= n {
				continue
			}
			cnt[b]++
			if p.rtt < 0 {
				lost[b]++
				continue
			}
			v := float64(p.rtt)
			sum[b] += v
			mx[b] = max(mx[b], v)
		}
		st := SeriesTarget{Name: ts.cfg.Name, Avg: make([]*float64, n), Max: make([]*float64, n), Loss: make([]*float64, n)}
		for b := range n {
			if cnt[b] == 0 {
				continue
			}
			st.Loss[b] = f64(pct(lost[b], cnt[b]))
			if ok := cnt[b] - lost[b]; ok > 0 {
				st.Avg[b] = f64(sum[b] / float64(ok))
				st.Max[b] = f64(mx[b])
			}
		}
		out.Targets = append(out.Targets, st)
	}

	if hasRemote {
		cnt, fault := make([]int, n), make([]int, n)
		for _, r := range e.rounds[idxRounds(e.rounds, start):] {
			b := int((r.t - start) / step)
			if b >= n {
				continue
			}
			cnt[b]++
			if r.bridgeFault() {
				fault[b]++
			}
		}
		out.Bridge = make([]*float64, n)
		for b := range n {
			if cnt[b] > 0 {
				out.Bridge[b] = f64(pct(fault[b], cnt[b]))
			}
		}
	}
	return out
}
