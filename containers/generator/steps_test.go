package main

import (
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"
)

// A step list reads as capacity, rates and percents, each with its duration, and a step that is
// none of these is refused with the step in the message.
func TestParseStepsReadsEachKindOfStep(t *testing.T) {
	steps, err := parseSteps(" capacity:60s, 1000:2m ,50%:90s,")
	if err != nil {
		t.Fatal(err)
	}
	want := []stepSpec{
		{Name: "capacity", Kind: stepCapacity, Duration: time.Minute},
		{Name: "1000", Kind: stepRate, Value: 1000, Duration: 2 * time.Minute},
		{Name: "50%", Kind: stepPercent, Value: 50, Duration: 90 * time.Second},
	}
	if len(steps) != len(want) {
		t.Fatalf("parsed %+v, want %+v", steps, want)
	}
	for i := range want {
		if steps[i] != want[i] {
			t.Fatalf("step %d is %+v, want %+v", i, steps[i], want[i])
		}
	}
	if got := stepsDuration(steps); got != 4*time.Minute+30*time.Second {
		t.Fatalf("the steps last %s, want 4m30s", got)
	}
	for _, bad := range []string{"1000", "fast:10s", "0:10s", "-5%:10s", "100:0s", "100:soon"} {
		if _, err := parseSteps(bad); err == nil {
			t.Errorf("the step %q was taken", bad)
		}
	}
}

// The histogram's quantiles are within 3.5% of the true ones, and the maximum is exact.
func TestTheHistogramIsWithinThreePercent(t *testing.T) {
	var h durHist
	for i := 1; i <= 10000; i++ {
		h.record(time.Duration(i) * time.Microsecond)
	}
	s := h.summary()
	for _, c := range []struct {
		name      string
		got, want float64
	}{{"p50", s.P50Us, 5000}, {"p90", s.P90Us, 9000}, {"p99", s.P99Us, 9900}, {"p99.9", s.P999Us, 9990}} {
		if math.Abs(c.got-c.want)/c.want > 0.035 {
			t.Errorf("%s is %.1f µs, want within 3.5%% of %.0f", c.name, c.got, c.want)
		}
	}
	if s.MaxUs != 10000 || s.Count != 10000 {
		t.Fatalf("the summary is %+v, want 10000 values with the maximum 10000 µs", s)
	}
	var empty durHist
	if empty.summary() != nil {
		t.Fatal("an empty histogram has a summary")
	}
}

// slowServer answers every post after delay.
func slowServer(t *testing.T, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// stepFixture is what runAckStep needs, for one target.
func stepFixture(t *testing.T, target string, slots int) (config, ackConfig, []*ackSlot, *ackStats, *targetPicker, []string) {
	t.Helper()
	cfg := config{Mode: "http_ack", Target: target, LineSize: 64, Connections: slots}
	ac := ackConfig{targets: []string{target}, policy: "round_robin", batch: 10, timeout: 5 * time.Second,
		backoff: time.Millisecond, contentType: "text/plain"}
	all := make([]*ackSlot, slots)
	for i := range all {
		all[i] = newAckSlot(i, ac)
	}
	stats := &ackStats{status: map[string]int64{}, perConn: map[string]int64{}, perTarget: map[string]int64{}, start: time.Now()}
	return cfg, ac, all, stats, newTargetPicker(ac.targets, ac.policy, 0, 0), []string{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
}

// Under open load, a post's latency counts from its place on the timeline. One worker posts every
// 10 ms to a target that takes 30 ms to answer, so the posts fall further behind their times, and
// the latency grows far beyond 30 ms. A closed load would report about 30 ms for the same target:
// it waits for each answer, and so hides the backlog.
func TestOpenLoadCountsLatencyFromTheTimeline(t *testing.T) {
	srv := slowServer(t, 30*time.Millisecond)
	cfg, ac, slots, stats, picker, pads := stepFixture(t, srv.URL, 1)

	open := runAckStep(cfg, ac, stepSpec{Name: "1000", Kind: stepRate, Value: 1000, Duration: 300 * time.Millisecond},
		0, "", slots, stats, picker, &sendClock{}, pads, time.Second)
	if open.Error != "" || open.AckLatencyUs == nil || open.Load != "open" || open.RateAsked != 1000 {
		t.Fatalf("the open step is %+v", open)
	}
	if open.AckLatencyUs.MaxUs < 150_000 || open.LatePosts == 0 {
		t.Fatalf("the open step's latency is %+v with %d late posts, want a growing backlog past 150 ms", open.AckLatencyUs, open.LatePosts)
	}

	closed := runAckStep(cfg, ac, stepSpec{Name: "capacity", Kind: stepCapacity, Duration: 200 * time.Millisecond},
		0, "", slots, stats, picker, &sendClock{}, pads, time.Second)
	if closed.Load != "closed" || closed.AckLatencyUs == nil || closed.AckLatencyUs.MaxUs > 100_000 {
		t.Fatalf("the closed step is %+v, want answers of about 30 ms", closed)
	}
	// The worker went on with its own sequence across the steps
	if got := slots[0].seq; got != int64(10*(open.Posts+closed.Posts)) {
		t.Fatalf("the worker is at SEQ %d after %d posts of 10 lines", got, open.Posts+closed.Posts)
	}
}

// A percent step takes its share of the capacity that the capacity step before it measured, and a
// percent step with no capacity before it says so instead of running.
func TestAPercentStepTakesAShareOfTheCapacity(t *testing.T) {
	srv := slowServer(t, 0)
	cfg, ac, slots, stats, picker, pads := stepFixture(t, srv.URL, 2)

	orphan := runAckStep(cfg, ac, stepSpec{Name: "50%", Kind: stepPercent, Value: 50, Duration: 100 * time.Millisecond},
		0, "", slots, stats, picker, &sendClock{}, pads, time.Second)
	if orphan.Error == "" || orphan.Posts != 0 {
		t.Fatalf("a percent step with no capacity ran: %+v", orphan)
	}

	steps := []stepSpec{
		{Name: "capacity", Kind: stepCapacity, Duration: 300 * time.Millisecond},
		{Name: "25%", Kind: stepPercent, Value: 25, Duration: 300 * time.Millisecond},
	}
	res := runAckSteps(cfg, ac, steps, slots, stats, picker, &sendClock{}, pads, time.Second)
	if len(res) != 2 || res[0].AckedPerSec <= 0 {
		t.Fatalf("the steps are %+v", res)
	}
	want := res[0].AckedPerSec / 4
	if got := res[1].RateAsked; math.Abs(got-want) > want*0.001 {
		t.Fatalf("the 25%% step asked for %.0f lines a second, want %.0f", got, want)
	}
	if got := res[1].AckedPerSec; got < want*0.5 || got > want*1.5 {
		t.Fatalf("the 25%% step reached %.0f lines a second, want about %.0f", got, want)
	}
}

// The disk baseline times both tests on the folder, and leaves nothing behind.
func TestTheDiskBaselineTimesBothTests(t *testing.T) {
	dir := t.TempDir()
	res, err := measureDisk(dir, 150*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if res.WriteFdatasync == nil || res.WriteFdatasyncOps <= 0 || res.NewFileFsync == nil || res.NewFileFsyncOps <= 0 {
		t.Fatalf("the baseline is %+v", res)
	}
	left, _ := os.ReadDir(dir)
	if len(left) != 0 {
		t.Fatalf("the baseline left %v in %s", left, dir)
	}
	if res.Path != dir || res.Seconds != 0.15 {
		t.Fatalf("the baseline names %q and %s seconds", res.Path, strconv.FormatFloat(res.Seconds, 'f', -1, 64))
	}
}
