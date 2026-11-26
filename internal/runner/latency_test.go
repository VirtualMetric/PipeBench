package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/results"
)

func TestBucketQuantileReturnsTheTopOfTheBucketThatHoldsTheRank(t *testing.T) {
	// 90 values up to 1 ms, 9 up to 2 ms, 1 up to 10 ms
	buckets := [][2]uint64{{1_000_000, 90}, {2_000_000, 9}, {10_000_000, 1}}
	for _, c := range []struct {
		q    float64
		want float64
	}{{0.5, 1000}, {0.9, 1000}, {0.95, 2000}, {0.99, 2000}, {0.999, 10000}, {1, 10000}} {
		if got := bucketQuantileUs(buckets, 100, c.q); got != c.want {
			t.Errorf("q=%v: %v µs, want %v", c.q, got, c.want)
		}
	}
}

func reading(at time.Time, stages ...stageCounts) stageReading {
	return stageReading{At: at, Stages: stages}
}

func TestDiffStagesCountsOnlyWhatHappenedBetweenTheReadings(t *testing.T) {
	t0 := time.Unix(1000, 0)
	a := reading(t0,
		stageCounts{Stage: "buffer.sync", Count: 10, Buckets: [][2]uint64{{1_000_000, 10}}},
		stageCounts{Stage: "store.put", Part: "ingester", Kind: "memory", Count: 5, Buckets: [][2]uint64{{2000, 5}}},
	)
	b := reading(t0.Add(time.Minute),
		// 10 more syncs, 2 of them slow
		stageCounts{Stage: "buffer.sync", Count: 20, Buckets: [][2]uint64{{1_000_000, 18}, {8_000_000, 2}}},
		// nothing new
		stageCounts{Stage: "store.put", Part: "ingester", Kind: "memory", Count: 5, Buckets: [][2]uint64{{2000, 5}}},
		// a stage that started after the first reading
		stageCounts{Stage: "target.round_trip", Kind: "tcp", Count: 4, Buckets: [][2]uint64{{500_000, 4}}},
	)
	got := diffStages(a, b)
	if len(got) != 2 {
		t.Fatalf("got %d stages, want 2 (the idle stage left out): %+v", len(got), got)
	}
	sync, rt := got[0], got[1]
	if sync.Stage != "buffer.sync" || sync.Count != 10 || sync.P50Us != 1000 || sync.P90Us != 8000 || sync.MaxUs != 8000 {
		t.Errorf("buffer.sync = %+v, want 10 syncs, p50 1000 µs, p90 and max 8000 µs", sync)
	}
	if rt.Stage != "target.round_trip" || rt.Kind != "tcp" || rt.Count != 4 || rt.P99Us != 500 {
		t.Errorf("target.round_trip = %+v, want 4 round trips at 500 µs", rt)
	}
}

func TestPickReadingsTakesTheReadingsAroundTheWindow(t *testing.T) {
	t0 := time.Unix(2000, 0)
	var rs []stageReading
	for i := 0; i < 10; i++ {
		rs = append(rs, reading(t0.Add(time.Duration(i)*5*time.Second)))
	}
	// shuffled order must not matter
	rs[2], rs[7] = rs[7], rs[2]

	a, b, ok := pickReadings(rs, t0.Add(12*time.Second), t0.Add(31*time.Second))
	if !ok || !a.At.Equal(t0.Add(10*time.Second)) || !b.At.Equal(t0.Add(35*time.Second)) {
		t.Fatalf("got %v..%v ok=%v, want the readings at 10s and 35s", a.At.Sub(t0), b.At.Sub(t0), ok)
	}
	// A window past the last reading takes the last one at its end
	a, b, ok = pickReadings(rs, t0.Add(20*time.Second), t0.Add(time.Hour))
	if !ok || !a.At.Equal(t0.Add(20*time.Second)) || !b.At.Equal(t0.Add(45*time.Second)) {
		t.Fatalf("got %v..%v ok=%v, want 20s and the last reading at 45s", a.At.Sub(t0), b.At.Sub(t0), ok)
	}
	if _, _, ok := pickReadings(rs[:1], t0, t0.Add(time.Second)); ok {
		t.Fatal("one reading gave a window")
	}
	if _, _, ok := pickReadings(rs, t0.Add(time.Hour), t0.Add(2*time.Hour)); ok {
		t.Fatal("a window after every reading gave a window of no readings")
	}
}

func TestTheSamplerKeepsEveryReadingAndCountsFailures(t *testing.T) {
	var n atomic.Int64
	failed := make(chan struct{})
	var once sync.Once
	read := func(_ context.Context, container string) (stageReading, error) {
		if container != "bench-subject-vmetric" {
			return stageReading{}, fmt.Errorf("read %q", container)
		}
		if n.Add(1)%3 == 0 {
			once.Do(func() { close(failed) })
			return stageReading{}, errors.New("no answer")
		}
		return reading(time.Now()), nil
	}
	s := runStageSampler(context.Background(), "bench-subject-vmetric", 10*time.Millisecond, read)
	// Wait for the first failed read, however slowly the sampler starts
	select {
	case <-failed:
	case <-time.After(10 * time.Second):
		t.Fatal("the sampler did not read three times within 10s")
	}
	first := s.finish(context.Background())
	second := s.finish(context.Background())
	if len(first) == 0 || len(first) != len(second) {
		t.Fatalf("finish gave %d then %d readings, want the same readings twice", len(first), len(second))
	}
	if total := int64(len(first) + s.failures); total != n.Load() {
		t.Fatalf("%d readings and %d failures, but %d reads", len(first), s.failures, n.Load())
	}
	if s.failures == 0 || s.lastErr != "no answer" {
		t.Fatalf("failures = %d, last error %q", s.failures, s.lastErr)
	}
}

// receiverOn serves h on a local port and returns the port.
func receiverOn(t *testing.T, h http.Handler) int {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().(*net.TCPAddr).Port
}

func TestCollectLatencyGivesEachStepItsEndToEndAndStageTimes(t *testing.T) {
	// Unix nanoseconds above 2^53: a float64 would round them, and the receiver would be asked for
	// another window
	from1, to1 := int64(1_763_200_000_123_456_789), int64(1_763_200_060_123_456_789)
	from2, to2 := to1+20_000_000_000, to1+140_000_000_000
	var mu sync.Mutex
	var asked []string
	port := receiverOn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/latency" {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		asked = append(asked, r.URL.RawQuery)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count": 600, "p50_us": 800.5, "p90_us": 1200, "p99_us": 3100, "p999_us": 9000, "max_us": 12000, "p95_ms": 2,
		})
	}))

	gen := fmt.Sprintf(`{"progress":{"lines":10}}
{"ack":{"ack_latency_us":{"count":1200,"p99_us":2500},"steps":[
 {"name":"capacity","measure_from_ns":%d,"end_ns":%d,"acked_per_sec":55000},
 {"name":"1000","measure_from_ns":%d,"end_ns":%d,"acked_per_sec":1000}]}}`, from1, to1, from2, to2)

	at := func(ns int64) time.Time { return time.Unix(0, ns) }
	readings := []stageReading{
		reading(at(from1-time.Second.Nanoseconds()), stageCounts{Stage: "buffer.sync", Count: 1, Buckets: [][2]uint64{{1_000_000, 1}}}),
		reading(at(to1+time.Second.Nanoseconds()), stageCounts{Stage: "buffer.sync", Count: 11, Buckets: [][2]uint64{{1_000_000, 11}}}),
	}
	baseline := map[string]any{"write_4k_fdatasync_us": map[string]any{"p99_us": 900}}
	res := collectLatency(context.Background(), gen, port, readings, baseline, nil)

	if res["error"] != nil {
		t.Fatalf("error: %v", res["error"])
	}
	if res["disk_baseline"] == nil || res["ack_latency_us"] == nil {
		t.Fatalf("the baseline or the acknowledgement latency is missing: %v", res)
	}
	want := []string{fmt.Sprintf("from_ns=%d&to_ns=%d", from1, to1), fmt.Sprintf("from_ns=%d&to_ns=%d", from2, to2)}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(asked, " ") != strings.Join(want, " ") {
		t.Fatalf("the receiver was asked for %v, want %v", asked, want)
	}
	steps := res["steps"].([]map[string]any)
	e2e := steps[0]["end_to_end_us"].(map[string]any)
	if e2e["p99_us"] != 3100.0 || e2e["count"] != 600.0 || e2e["p95_ms"] != nil {
		t.Errorf("step 1's end-to-end latency = %v", e2e)
	}
	st, ok := steps[0]["stages"].([]stageWindow)
	if !ok || len(st) != 1 || st[0].Count != 10 {
		t.Errorf("step 1's stages = %#v, want 10 syncs", steps[0]["stages"])
	}
	// No reading after the second step's start, so it has no stage times
	if steps[1]["stages"] != nil {
		t.Errorf("step 2 has stage times without readings around it: %v", steps[1]["stages"])
	}

	// The saved row keeps the nanoseconds exact
	out, err := json.Marshal(steps[1])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), fmt.Sprintf(`"end_ns":%d`, to2)) {
		t.Errorf("the saved step lost its end time's digits: %s", out)
	}
}

// A step whose end-to-end latency could not be read says why, and has no numbers. Once the run is
// stopped, no step asks the receiver.
func TestCollectLatencySaysWhyAStepHasNoEndToEndLatency(t *testing.T) {
	var asked atomic.Int64
	port := receiverOn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "no tracker"})
	}))
	gen := `{"ack":{"steps":[{"name":"1000","measure_from_ns":10,"end_ns":20}]}}`

	res := collectLatency(context.Background(), gen, port, nil, nil, nil)
	step := res["steps"].([]map[string]any)[0]
	if step["end_to_end_us"] != nil {
		t.Errorf("a 500 gave end-to-end numbers: %v", step["end_to_end_us"])
	}
	if msg, _ := step["end_to_end_error"].(string); !strings.Contains(msg, "500") {
		t.Errorf("end_to_end_error = %v, want the receiver's status", step["end_to_end_error"])
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := asked.Load()
	res = collectLatency(ctx, gen, port, nil, nil, nil)
	step = res["steps"].([]map[string]any)[0]
	if asked.Load() != before || step["end_to_end_error"] == nil {
		t.Errorf("a stopped run asked the receiver %d times, error %v", asked.Load()-before, step["end_to_end_error"])
	}
}

func TestCollectLatencySaysWhenTheGeneratorGaveNoSummary(t *testing.T) {
	res := collectLatency(context.Background(), `{"progress":{"lines":10}}`, 1, nil, nil, nil)
	if msg, _ := res["error"].(string); !strings.Contains(msg, "no acknowledgement summary") {
		t.Fatalf("error = %v", res["error"])
	}
}

func TestPrepareSubjectDataMakesAFolderOfTheRunThatTheSubjectCanWrite(t *testing.T) {
	base := t.TempDir()
	now := time.Unix(0, 1_763_200_000_000_000_001)
	host, dst, err := prepareSubjectData(base, "latency_case", "vmetric", now)
	if err != nil {
		t.Fatal(err)
	}
	if dst != "/opt/vmetric/storage" || host != filepath.Join(base, "latency_case-1763200000000000001") {
		t.Fatalf("host %q dst %q", host, dst)
	}
	fi, err := os.Stat(host)
	if err != nil || !fi.IsDir() {
		t.Fatalf("the run's folder is missing: %v", err)
	}
	// The subject runs as a user of its own
	if fi.Mode().Perm() != 0o777 {
		t.Fatalf("the run's folder has mode %v, want 0777", fi.Mode().Perm())
	}

	if host, dst, err := prepareSubjectData("", "latency_case", "vmetric", now); err != nil || host != "" || dst != "" {
		t.Fatalf("without --subject-data: %q %q %v", host, dst, err)
	}
	if _, _, err := prepareSubjectData(base, "latency_case", "vector", now); err == nil {
		t.Fatal("a subject whose data folder the harness does not know got a folder")
	}
}

func TestASavedRowKeepsItsLatencyAndHardware(t *testing.T) {
	old := readHardware
	t.Cleanup(func() { readHardware = old })
	var askedFor string
	readHardware = func(dataPath string) map[string]any {
		askedFor = dataPath
		return map[string]any{"cpus": 8, "cloud_vm_size": "Standard_L8as_v4"}
	}

	r, dir := newTestRunner(t, context.Background(), "4", "")
	r.subjectDataHost = "/mnt/nvme/subject-data/latency_case-1"
	if _, err := r.saveResult(results.RunResult{
		TestName: "latency_case",
		Subject:  "vmetric",
		Hardware: "custom",
		Latency:  map[string]any{"steps": []any{map[string]any{"name": "capacity"}}},
	}, ""); err != nil {
		t.Fatalf("saveResult: %v", err)
	}
	if askedFor != "/mnt/nvme/subject-data/latency_case-1" {
		t.Errorf("the hardware block describes %q, not the run's data folder", askedFor)
	}
	entry := readEntry(t, dir, "custom", "vmetric")
	hw, _ := entry["hardware_info"].(map[string]any)
	if hw["cloud_vm_size"] != "Standard_L8as_v4" {
		t.Errorf("hardware_info = %v", entry["hardware_info"])
	}
	lat, _ := entry["latency"].(map[string]any)
	if lat == nil || lat["steps"] == nil {
		t.Errorf("latency = %v", entry["latency"])
	}
}
