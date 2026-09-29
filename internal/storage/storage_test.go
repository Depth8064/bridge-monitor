package storage

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
	"github.com/Depth8064/bridge-monitor/internal/model"
	"github.com/Depth8064/bridge-monitor/internal/monitor"
)

func TestHistQuantileAndEncode(t *testing.T) {
	h := Hist{}
	for i := 1; i <= 1000; i++ {
		h.Add(float64(i) / 10) // 0.1 .. 100 ms
	}
	for _, c := range []struct{ q, want float64 }{{.5, 50}, {.95, 95}, {.99, 99}} {
		if got := h.Quantile(c.q); math.Abs(got-c.want)/c.want > 0.03 {
			t.Errorf("p%v = %.3f, want ~%v", c.q*100, got, c.want)
		}
	}
	back := Hist{}
	if err := DecodeHist(h.Encode(), back); err != nil {
		t.Fatal(err)
	}
	if len(back) != len(h) {
		t.Fatalf("decoded %d buckets, want %d", len(back), len(h))
	}
	for k, v := range h {
		if back[k] != v {
			t.Fatalf("bucket %d: %d != %d", k, back[k], v)
		}
	}
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.Targets = []config.Target{
		{Name: "L", Role: config.RoleLocal, Type: config.TypeICMP, Host: "10.0.0.1"},
		{Name: "R", Role: config.RoleRemote, Type: config.TypeICMP, Host: "10.0.0.2"},
		{Name: "I", Role: config.RoleInternet, Type: config.TypeICMP, Host: "1.1.1.1"},
	}
	return cfg
}

// seed writes one round per second for dur. R loses a 10s outage at 1h and one probe every 100s.
func seed(t *testing.T, db *DB, dur time.Duration) (rounds, rLost int) {
	t.Helper()
	for s := 0; s < int(dur.Seconds()); s++ {
		rOK := !(s >= 3600 && s < 3610) && s%100 != 50
		if !rOK {
			rLost++
		}
		samples := []model.Sample{
			{Target: "L", OK: true, RTT: time.Duration(1000+s%500) * time.Microsecond},
			{Target: "R", OK: rOK, RTT: 3 * time.Millisecond, Err: map[bool]string{false: "timeout"}[rOK]},
			{Target: "I", OK: true, RTT: 12 * time.Millisecond},
		}
		if err := db.WriteRound(t0.Add(time.Duration(s)*time.Second), samples); err != nil {
			t.Fatal(err)
		}
		rounds++
	}
	return rounds, rLost
}

func openTest(t *testing.T, cfg *config.Config) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "test.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func sumTier(t *testing.T, db *DB, tierName, target string) (sent, lost, bursts, maxBurst, faults int64) {
	t.Helper()
	id, _ := db.targetID(target)
	err := db.db.QueryRow(`SELECT COALESCE(SUM(sent),0), COALESCE(SUM(lost),0), COALESCE(SUM(bursts),0), COALESCE(MAX(max_burst),0)
		FROM agg_`+tierName+` WHERE target_id = ?`, id).Scan(&sent, &lost, &bursts, &maxBurst)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRow(`SELECT COALESCE(SUM(bridge_fault),0) FROM verdict_` + tierName).Scan(&faults); err != nil {
		t.Fatal(err)
	}
	return
}

func TestRollupMatchesRawAndIsIdempotent(t *testing.T) {
	cfg := testConfig()
	db := openTest(t, cfg)
	dur := 2*time.Hour + 10*time.Minute
	rounds, rLost := seed(t, db, dur)
	ds := NewDownsampler(db, cfg)
	now := t0.Add(dur + time.Minute)

	for run := 0; run < 2; run++ {
		if err := ds.Rollup(context.Background(), now); err != nil {
			t.Fatal(err)
		}
		sent, lost, bursts, maxBurst, faults := sumTier(t, db, "1m", "R")
		if sent != int64(rounds) || lost != int64(rLost) {
			t.Fatalf("run %d: 1m R sent/lost = %d/%d, want %d/%d", run, sent, lost, rounds, rLost)
		}
		if faults != int64(rLost) {
			t.Fatalf("run %d: bridge faults = %d, want %d", run, faults, rLost)
		}
		// 10s outage + one single loss every 100s outside it
		wantBursts := int64(1 + (rLost - 10))
		if bursts != wantBursts || maxBurst != 10 {
			t.Fatalf("run %d: bursts=%d max=%d, want %d/10", run, bursts, maxBurst, wantBursts)
		}
	}

	// Only complete hours are rolled into 1h.
	sent, lost, _, _, _ := sumTier(t, db, "1h", "R")
	if sent != 7200 {
		t.Fatalf("1h R sent = %d, want 7200", sent)
	}
	var wantLost int64
	for s := 0; s < 7200; s++ {
		if (s >= 3600 && s < 3610) || s%100 == 50 {
			wantLost++
		}
	}
	if lost != wantLost {
		t.Fatalf("1h R lost = %d, want %d", lost, wantLost)
	}
}

func TestPruneKeepsUnrolledData(t *testing.T) {
	cfg := testConfig()
	cfg.Storage.Raw = config.Duration{Duration: time.Minute}
	db := openTest(t, cfg)
	seed(t, db, 30*time.Minute)

	ds := NewDownsampler(db, cfg)
	far := t0.Add(365 * 24 * time.Hour)
	// Nothing rolled up yet: pruning must not drop anything.
	if err := ds.Prune(context.Background(), far); err != nil {
		t.Fatal(err)
	}
	var n int
	db.db.QueryRow(`SELECT COUNT(*) FROM samples`).Scan(&n)
	if n != 30*60*3 {
		t.Fatalf("samples after prune without rollup = %d, want %d", n, 30*60*3)
	}

	if err := ds.Rollup(context.Background(), t0.Add(20*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := ds.Prune(context.Background(), far); err != nil {
		t.Fatal(err)
	}
	var minTS int64
	db.db.QueryRow(`SELECT COUNT(*), MIN(ts) FROM samples`).Scan(&n, &minTS)
	wm, _ := db.watermark(context.Background(), db.db, "1m")
	if minTS < wm {
		t.Fatalf("raw sample at %d survived below watermark %d", minTS, wm)
	}
	if n == 0 {
		t.Fatal("prune removed samples that were not rolled up")
	}
}

func TestHistoryStitchesTiers(t *testing.T) {
	cfg := testConfig()
	db := openTest(t, cfg)
	dur := 2*time.Hour + 10*time.Minute
	rounds, rLost := seed(t, db, dur)
	// Roll up only part of the data so history must stitch 1h, 1m and raw together.
	if err := NewDownsampler(db, cfg).Rollup(context.Background(), t0.Add(2*time.Hour+5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	h := NewHistory(db, monitor.NewEngine(cfg), cfg)
	sd, err := h.buildState(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	r := sd.aggs["R"]
	if r == nil || r.Sent != int64(rounds) || r.Lost != int64(rLost) {
		t.Fatalf("history R = %+v, want sent %d lost %d", r, rounds, rLost)
	}
	if sd.v.Rounds != int64(rounds) || sd.v.BridgeFault != int64(rLost) {
		t.Fatalf("history verdict rounds=%d faults=%d, want %d/%d", sd.v.Rounds, sd.v.BridgeFault, rounds, rLost)
	}
	st := h.stats(sd.aggs["L"], nil, sd.sp.from)
	if st.RTTMin < 0.99 || st.RTTMax > 1.5 || math.Abs(st.RTTP50-1.25) > 0.06 {
		t.Fatalf("L rtt min/p50/max = %.3f/%.3f/%.3f", st.RTTMin, st.RTTP50, st.RTTMax)
	}
}
