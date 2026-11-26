package runner

// Latency cases: a case's `latency:` block (config.LatencyConfig) runs the http_ack
// generator through steps of load. This file gives the run what the generator cannot see itself:
//
//   - the subject's data folder on a chosen disk (--subject-data), and that disk's own sync
//     latency before the run (the generator's disk_baseline mode);
//   - the stage times of a vmetric director: `director -latency`, read every few seconds while the
//     load runs, and the difference of the readings around each step;
//   - the end-to-end latency of each step's lines, from the receiver's /latency.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// subjectDataPaths are the data folders of the subjects that --subject-data can mount.
var subjectDataPaths = map[string]string{
	"vmetric": "/opt/vmetric/storage",
}

// vmetricLatencyCommand prints a vmetric director's stage times, as JSON (service/profiler).
var vmetricLatencyCommand = []string{"/opt/vmetric/director", "-latency"}

// prepareSubjectData makes the run's own folder under base, the --subject-data folder, and returns
// it with the subject's data folder in its container. It returns empty strings without base.
func prepareSubjectData(base, caseName, subject string, now time.Time) (host, dst string, err error) {
	if base == "" {
		return "", "", nil
	}
	dst, ok := subjectDataPaths[subject]
	if !ok {
		return "", "", fmt.Errorf("--subject-data: the harness does not know the data folder of subject %q", subject)
	}
	abs, err := filepath.Abs(base)
	if err != nil {
		return "", "", fmt.Errorf("--subject-data: %w", err)
	}
	host = filepath.Join(abs, fmt.Sprintf("%s-%d", caseName, now.UnixNano()))
	if err := os.MkdirAll(host, 0o777); err != nil {
		return "", "", fmt.Errorf("--subject-data: %w", err)
	}
	// The subject runs as a user of its own, and must write there
	if err := os.Chmod(host, 0o777); err != nil {
		return "", "", fmt.Errorf("--subject-data: %w", err)
	}
	return host, dst, nil
}

// removeSubjectData removes the run's data folder. The subject's user owns what it wrote there, so
// a container of image removes it, as root: the image's own user could not.
func removeSubjectData(image, host string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	parent, name := filepath.Dir(host), filepath.Base(host)
	_ = exec.CommandContext(ctx, "docker", "run", "--rm", "--user", "0", "--entrypoint", "rm", "-v", parent+":/data", image, "-rf", "/data/"+name).Run()
	_ = os.Remove(host)
}

// runDiskBaseline measures the disk under host with the generator image's disk_baseline mode, and
// returns its result.
func runDiskBaseline(ctx context.Context, image, host string) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "run", "--rm",
		"-e", "GENERATOR_MODE=disk_baseline", "-e", "GENERATOR_DISK_PATH=/disk",
		"-v", host+":/disk", image).Output()
	if err != nil {
		return nil, fmt.Errorf("the disk baseline did not run: %w", err)
	}
	var res struct {
		DiskBaseline map[string]any `json:"disk_baseline"`
	}
	if err := json.Unmarshal(jsonObject(string(out)), &res); err != nil || res.DiskBaseline == nil {
		return nil, fmt.Errorf("the disk baseline's answer does not read: %q", strings.TrimSpace(string(out)))
	}
	res.DiskBaseline["host_path"] = host
	return res.DiskBaseline, nil
}

// jsonObject returns the text from the first { to the last }, or the text itself.
func jsonObject(s string) []byte {
	start, end := strings.Index(s, "{"), strings.LastIndex(s, "}")
	if start < 0 || end < start {
		return []byte(s)
	}
	return []byte(s[start : end+1])
}

// stageCounts is one histogram of a director's -latency answer.
type stageCounts struct {
	Stage   string      `json:"stage"`
	Part    string      `json:"part,omitempty"`
	Kind    string      `json:"kind,omitempty"`
	Count   uint64      `json:"count"`
	Buckets [][2]uint64 `json:"buckets"`
}

// stageReading is one -latency answer: every histogram, and when the director took it.
type stageReading struct {
	At     time.Time     `json:"at"`
	Stages []stageCounts `json:"stages"`
}

// stageSampler reads a director's stage times every interval while the load runs.
type stageSampler struct {
	container string
	interval  time.Duration
	read      func(ctx context.Context, container string) (stageReading, error)

	mu       sync.Mutex
	readings []stageReading
	failures int
	lastErr  string

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// readStages runs the director's -latency in container.
func readStages(ctx context.Context, container string) (stageReading, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	args := append([]string{"exec", container}, vmetricLatencyCommand...)
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return stageReading{}, fmt.Errorf("%s -latency: %w", container, err)
	}
	var r stageReading
	if err := json.Unmarshal(jsonObject(string(out)), &r); err != nil {
		return stageReading{}, fmt.Errorf("%s -latency answered %q: %w", container, strings.TrimSpace(string(out)), err)
	}
	return r, nil
}

// startStageSampler starts reading the stage times of container every interval.
func startStageSampler(ctx context.Context, container string, interval time.Duration) *stageSampler {
	return runStageSampler(ctx, container, interval, readStages)
}

// runStageSampler starts reading container's stage times with read every interval.
func runStageSampler(ctx context.Context, container string, interval time.Duration,
	read func(ctx context.Context, container string) (stageReading, error),
) *stageSampler {
	s := &stageSampler{container: container, interval: interval, read: read, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			s.sample(ctx)
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return s
}

func (s *stageSampler) sample(ctx context.Context) {
	r, err := s.read(ctx, s.container)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.failures++
		s.lastErr = err.Error()
		return
	}
	s.readings = append(s.readings, r)
}

// finish stops the sampler, takes one last reading, and returns every reading. A second call
// returns the same readings.
func (s *stageSampler) finish(ctx context.Context) []stageReading {
	s.stopOnce.Do(func() {
		close(s.stop)
		<-s.done
		s.sample(ctx)
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]stageReading(nil), s.readings...)
}

// stageWindow is one stage's times between two readings, in microseconds. Each quantile is the top
// of the bucket that holds it, so it can be up to a sixteenth above the true value.
type stageWindow struct {
	Stage  string  `json:"stage"`
	Part   string  `json:"part,omitempty"`
	Kind   string  `json:"kind,omitempty"`
	Count  uint64  `json:"count"`
	P50Us  float64 `json:"p50_us"`
	P90Us  float64 `json:"p90_us"`
	P99Us  float64 `json:"p99_us"`
	P999Us float64 `json:"p999_us"`
	MaxUs  float64 `json:"max_us"`
}

// pickReadings returns the readings around [from, to]: the last one at or before from, and the
// first one at or after to. Without such a reading, the nearest one at that end stands in. ok is
// false when fewer than two readings are left.
func pickReadings(readings []stageReading, from, to time.Time) (a, b stageReading, ok bool) {
	if len(readings) < 2 {
		return stageReading{}, stageReading{}, false
	}
	sorted := append([]stageReading(nil), readings...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].At.Before(sorted[j].At) })
	ai, bi := 0, len(sorted)-1
	for i, r := range sorted {
		if !r.At.After(from) {
			ai = i
		}
	}
	for i := len(sorted) - 1; i >= 0; i-- {
		if !sorted[i].At.Before(to) {
			bi = i
		}
	}
	if bi <= ai {
		return stageReading{}, stageReading{}, false
	}
	return sorted[ai], sorted[bi], true
}

// diffStages returns what each histogram counted between reading a and the later reading b.
func diffStages(a, b stageReading) []stageWindow {
	type key struct{ stage, part, kind string }
	before := map[key]map[uint64]uint64{}
	for _, s := range a.Stages {
		m := map[uint64]uint64{}
		for _, bk := range s.Buckets {
			m[bk[0]] = bk[1]
		}
		before[key{s.Stage, s.Part, s.Kind}] = m
	}
	var out []stageWindow
	for _, s := range b.Stages {
		was := before[key{s.Stage, s.Part, s.Kind}]
		var buckets [][2]uint64
		total := uint64(0)
		for _, bk := range s.Buckets {
			if n := bk[1] - min(was[bk[0]], bk[1]); n > 0 {
				buckets = append(buckets, [2]uint64{bk[0], n})
				total += n
			}
		}
		if total == 0 {
			continue
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i][0] < buckets[j][0] })
		out = append(out, stageWindow{
			Stage: s.Stage, Part: s.Part, Kind: s.Kind, Count: total,
			P50Us: bucketQuantileUs(buckets, total, 0.50), P90Us: bucketQuantileUs(buckets, total, 0.90),
			P99Us: bucketQuantileUs(buckets, total, 0.99), P999Us: bucketQuantileUs(buckets, total, 0.999),
			MaxUs: float64(buckets[len(buckets)-1][0]) / 1000,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Stage != out[j].Stage {
			return out[i].Stage < out[j].Stage
		}
		if out[i].Part != out[j].Part {
			return out[i].Part < out[j].Part
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

// bucketQuantileUs returns, in microseconds, the top of the bucket below which q of the total fall.
func bucketQuantileUs(buckets [][2]uint64, total uint64, q float64) float64 {
	rank := min(max(uint64(q*float64(total)+0.5), 1), total)
	seen := uint64(0)
	for _, bk := range buckets {
		seen += bk[1]
		if seen >= rank {
			return float64(bk[0]) / 1000
		}
	}
	return float64(buckets[len(buckets)-1][0]) / 1000
}

// collectLatency builds the latency result of a run: each step of the generator, with its
// end-to-end latency from the receiver on metricsPort and, with readings, the subject's stage
// times. baseline is the disk baseline, or nil. A step whose end-to-end latency could not be read
// says why in end_to_end_error; once ctx is cancelled, no step asks the receiver.
func collectLatency(ctx context.Context, genStdout string, metricsPort int, readings []stageReading, baseline map[string]any, sampler *stageSampler) map[string]any {
	res := map[string]any{}
	if baseline != nil {
		res["disk_baseline"] = baseline
	}
	ack, err := generatorSteps(genStdout)
	if err != nil {
		res["error"] = err.Error()
		return res
	}
	if ack.AckLatencyUs != nil {
		res["ack_latency_us"] = ack.AckLatencyUs
	}
	if sampler != nil {
		sampler.mu.Lock()
		res["stage_readings"] = len(sampler.readings)
		if sampler.failures > 0 {
			res["stage_reading_failures"] = sampler.failures
			res["stage_reading_error"] = sampler.lastErr
		}
		sampler.mu.Unlock()
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for _, step := range ack.Steps {
		from, to := int64Of(step["measure_from_ns"]), int64Of(step["end_ns"])
		if from <= 0 || to <= from {
			continue
		}
		url := fmt.Sprintf("http://localhost:%d/latency?from_ns=%d&to_ns=%d", metricsPort, from, to)
		if ctx.Err() != nil {
			step["end_to_end_error"] = "the run was stopped before the receiver was asked"
		} else if e2e, err := receiverLatency(ctx, client, url); err != nil {
			step["end_to_end_error"] = err.Error()
		} else {
			step["end_to_end_us"] = map[string]any{
				"count": e2e["count"], "p50_us": e2e["p50_us"], "p90_us": e2e["p90_us"],
				"p99_us": e2e["p99_us"], "p999_us": e2e["p999_us"], "max_us": e2e["max_us"],
			}
		}
		if a, b, ok := pickReadings(readings, time.Unix(0, from), time.Unix(0, to)); ok {
			step["stages"] = diffStages(a, b)
			step["stages_from_ns"], step["stages_to_ns"] = a.At.UnixNano(), b.At.UnixNano()
		}
	}
	res["steps"] = ack.Steps
	return res
}

// receiverLatency asks the receiver for the first-arrival latency of the lines sent in one step.
func receiverLatency(ctx context.Context, client *http.Client, url string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("asking the receiver for the step's latency: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the receiver's /latency answered %s", resp.Status)
	}
	var e2e map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&e2e); err != nil {
		return nil, fmt.Errorf("reading the receiver's /latency: %w", err)
	}
	return e2e, nil
}

// generatorAckSteps is the part of the generator's final answer that a latency case reads.
type generatorAckSteps struct {
	AckLatencyUs map[string]any   `json:"ack_latency_us"`
	Steps        []map[string]any `json:"steps"`
}

// generatorSteps reads the steps from the generator's output. The output can hold progress lines
// before the final answer, so it takes the first JSON object that has "ack". UseNumber keeps the
// steps' Unix nanoseconds exact: a float64 would round them.
func generatorSteps(stdout string) (*generatorAckSteps, error) {
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.UseNumber()
	for {
		var g struct {
			Ack *generatorAckSteps `json:"ack"`
		}
		if err := dec.Decode(&g); err != nil {
			return nil, fmt.Errorf("the generator's output has no acknowledgement summary: %w", err)
		}
		if g.Ack != nil {
			return g.Ack, nil
		}
	}
}

// int64Of reads a JSON number as an int64.
func int64Of(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}
