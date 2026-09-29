package storage

import (
	"math"

	"github.com/Depth8064/bridge-monitor/internal/model"
)

// Agg is one target's mergeable summary for a time bucket.
type Agg struct {
	Sent, Lost       int64
	Sum, Min, Max    float64 // RTT ms over received probes; Min/Max valid when Recv() > 0
	JitSum           float64
	JitN             int64
	Spikes           int64
	Bursts, MaxBurst int64 // loss runs, attributed to the bucket where the run ended
	Hist             Hist
}

func newAgg() *Agg { return &Agg{Hist: Hist{}} }

func (a *Agg) Recv() int64 { return a.Sent - a.Lost }

func (a *Agg) addRTT(ms, spikeMs float64) {
	if a.Recv() == 0 {
		a.Min, a.Max = ms, ms
	} else {
		a.Min, a.Max = math.Min(a.Min, ms), math.Max(a.Max, ms)
	}
	a.Sent++
	a.Sum += ms
	a.Hist.Add(ms)
	if ms > spikeMs {
		a.Spikes++
	}
}

func (a *Agg) endBurst(n int64) {
	a.Bursts++
	a.MaxBurst = max(a.MaxBurst, n)
}

func (a *Agg) Merge(b *Agg) {
	if b.Recv() > 0 {
		if a.Recv() == 0 {
			a.Min, a.Max = b.Min, b.Max
		} else {
			a.Min, a.Max = math.Min(a.Min, b.Min), math.Max(a.Max, b.Max)
		}
	}
	a.Sent += b.Sent
	a.Lost += b.Lost
	a.Sum += b.Sum
	a.JitSum += b.JitSum
	a.JitN += b.JitN
	a.Spikes += b.Spikes
	a.Bursts += b.Bursts
	a.MaxBurst = max(a.MaxBurst, b.MaxBurst)
	a.Hist.Merge(b.Hist)
}

// VAgg summarises bridge-verdict round classifications for a time bucket.
type VAgg struct {
	Rounds                          int64
	LocalN, RemoteN, InetN          int64 // rounds where the role had targets
	LocalFail, RemoteFail, InetFail int64
	FarFail, BridgeFault            int64
	InetBridge, Upstream            int64
}

func (v *VAgg) AddRound(r model.Round, inetRemote bool) {
	v.Rounds++
	count := func(state int8, n, fail *int64) {
		if state >= 0 {
			*n++
			if state == 0 {
				*fail++
			}
		}
	}
	count(r.Local, &v.LocalN, &v.LocalFail)
	count(r.Remote, &v.RemoteN, &v.RemoteFail)
	count(r.Inet, &v.InetN, &v.InetFail)
	if r.FarSide(inetRemote) == 0 {
		v.FarFail++
	}
	if r.BridgeFault(inetRemote) {
		v.BridgeFault++
	}
	if r.InetBridge(inetRemote) {
		v.InetBridge++
	}
	if r.Upstream(inetRemote) {
		v.Upstream++
	}
}

func (v *VAgg) Merge(o *VAgg) {
	v.Rounds += o.Rounds
	v.LocalN += o.LocalN
	v.RemoteN += o.RemoteN
	v.InetN += o.InetN
	v.LocalFail += o.LocalFail
	v.RemoteFail += o.RemoteFail
	v.InetFail += o.InetFail
	v.FarFail += o.FarFail
	v.BridgeFault += o.BridgeFault
	v.InetBridge += o.InetBridge
	v.Upstream += o.Upstream
}

// carry is per-target state that must survive bucket and chunk boundaries while rolling up raw samples.
type carry struct {
	prev float64 // last RTT for jitter; <0 none
	run  int64   // current loss run
	last int64   // ms timestamp of last sample
}

type aggKey struct {
	bucket int64
	tid    int64
}

// rawAcc folds raw samples (in ts order) into per-bucket target and verdict aggregates.
type rawAcc struct {
	size, gapMs int64
	spikeMs     float64
	inetRemote  bool
	roles       map[int64]string
	carry       map[int64]*carry
	aggs        map[aggKey]*Agg
	vaggs       map[int64]*VAgg

	curTS int64
	round model.Round
}

func newRawAcc(size, gapMs int64, spikeMs float64, inetRemote bool, roles map[int64]string, c map[int64]*carry) *rawAcc {
	if c == nil {
		c = map[int64]*carry{}
	}
	return &rawAcc{
		size: size, gapMs: gapMs, spikeMs: spikeMs, inetRemote: inetRemote, roles: roles,
		carry: c, aggs: map[aggKey]*Agg{}, vaggs: map[int64]*VAgg{}, curTS: -1,
	}
}

func (r *rawAcc) bucket(ts int64) int64 { return floorTo(ts, r.size) }

func (r *rawAcc) flushRound() {
	if r.curTS < 0 {
		return
	}
	b := r.bucket(r.curTS)
	v := r.vaggs[b]
	if v == nil {
		v = &VAgg{}
		r.vaggs[b] = v
	}
	v.AddRound(r.round, r.inetRemote)
}

// add must be called in ts order; rtt < 0 means lost.
func (r *rawAcc) add(ts, tid int64, rtt float64) {
	if ts != r.curTS {
		r.flushRound()
		r.curTS = ts
		r.round = model.NewRound()
	}
	r.round.Apply(r.roles[tid], rtt >= 0)

	k := aggKey{r.bucket(ts), tid}
	a := r.aggs[k]
	if a == nil {
		a = newAgg()
		r.aggs[k] = a
	}
	c := r.carry[tid]
	if c == nil {
		c = &carry{prev: -1}
		r.carry[tid] = c
	}
	if c.last != 0 && ts-c.last > r.gapMs {
		if c.run > 0 {
			a.endBurst(c.run)
			c.run = 0
		}
		c.prev = -1
	}
	c.last = ts
	if rtt < 0 {
		a.Sent++
		a.Lost++
		c.run++
		c.prev = -1
		return
	}
	if c.run > 0 {
		a.endBurst(c.run)
		c.run = 0
	}
	a.addRTT(rtt, r.spikeMs)
	if c.prev >= 0 {
		a.JitSum += math.Abs(rtt - c.prev)
		a.JitN++
	}
	c.prev = rtt
}

func (r *rawAcc) finish() { r.flushRound() }

func floorTo(v, size int64) int64 {
	if v >= 0 {
		return v / size * size
	}
	return (v - size + 1) / size * size
}
