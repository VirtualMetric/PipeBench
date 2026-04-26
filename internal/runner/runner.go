package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"github.com/VirtualMetric/PipeBench/internal/orchestrator"
	"github.com/VirtualMetric/PipeBench/internal/results"
)

// Options configure a test run.
type Options struct {
	CasesDir       string
	ResultsDir     string
	GeneratorImage string
	ReceiverImage  string
	CollectorImage string
	// Override subject version (empty = use registry default)
	SubjectVersion string
	// Override configuration name (empty = "default")
	ConfigName string
	// Skip teardown — leave containers running (useful for debugging)
	NoCleanup bool
	// Port on the host that the receiver metrics endpoint is exposed on
	ReceiverHostPort int
	// Maximum time to wait for the test to complete
	Timeout time.Duration
	// Platform: "docker" or "kubernetes"
	Platform string
	// Resource limits for the subject container (both platforms)
	CPULimit string // e.g. "1", "4", "0.5" — number of cores
	MemLimit string // e.g. "1g", "16g", "512m"
}

func (o *Options) applyDefaults() {
	if o.GeneratorImage == "" {
		o.GeneratorImage = "vmetric/bench-generator:latest"
	}
	if o.ReceiverImage == "" {
		o.ReceiverImage = "vmetric/bench-receiver:latest"
	}
	if o.CollectorImage == "" {
		o.CollectorImage = "vmetric/bench-collector:latest"
	}
	if o.ConfigName == "" {
		o.ConfigName = "default"
	}
	if o.ReceiverHostPort == 0 {
		o.ReceiverHostPort = 19001
	}
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Minute
	}
	if o.Platform == "" {
		o.Platform = "docker"
	}
}

// ReceiverMetrics is the JSON response from the receiver's /metrics endpoint.
type ReceiverMetrics struct {
	LinesReceived  int64    `json:"lines_received"`
	BytesReceived  int64    `json:"bytes_received"`
	Done           bool     `json:"done"`
	Passed         *bool    `json:"passed,omitempty"`
	Errors         []string `json:"errors,omitempty"`
	UniqueLines    int64    `json:"unique_lines,omitempty"`
	Duplicates     int64    `json:"duplicates,omitempty"`
	MalformedLines int64    `json:"malformed_lines,omitempty"`
	LatencyP50Ms   float64  `json:"latency_p50_ms,omitempty"`
	LatencyP95Ms   float64  `json:"latency_p95_ms,omitempty"`
	LatencyP99Ms   float64  `json:"latency_p99_ms,omitempty"`
}

// GeneratorResult is the JSON output from the generator container.
type GeneratorResult struct {
	LinesSent  int64 `json:"lines_sent"`
	BytesSent  int64 `json:"bytes_sent"`
	DurationMs int64 `json:"duration_ms"`
}

// Runner executes a single test case against a single subject.
type Runner struct {
	opts  Options
	store *results.Store
}

// hardwareID returns the BENCH_HARDWARE env var or "custom" when unset.
// Used to tag each RunResult and pick the results/<hardware>/ subtree so
// PipeBench can group results by machine class.
func hardwareID() string {
	if h := os.Getenv("BENCH_HARDWARE"); h != "" {
		return h
	}
	return "custom"
}

// New creates a Runner.
func New(opts Options) *Runner {
	opts.applyDefaults()
	return &Runner{
		opts:  opts,
		store: results.NewStore(opts.ResultsDir),
	}
}

// Run executes the test and returns the persisted result.
func (r *Runner) Run(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	if tc.Type == "persistence_correctness" {
		return r.runPersistenceCorrectness(tc, subject)
	}
	if tc.Type == "persistence_restart_correctness" {
		return r.runPersistenceRestartCorrectness(tc, subject)
	}

	configName := r.opts.ConfigName

	// Resolve subject version override
	if r.opts.SubjectVersion != "" {
		subject = subject.WithVersion(r.opts.SubjectVersion)
	}

	fmt.Printf("→ test=%s  subject=%s  version=%s  config=%s\n",
		tc.Name, subject.Name, subject.Version, configName)

	// Locate subject config file — must be absolute for Docker bind mounts.
	configSrc, err := tc.ConfigFilePath(r.opts.CasesDir, configName, subject)
	if err != nil {
		return results.RunResult{}, err
	}
	configSrc, err = filepath.Abs(configSrc)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving config path: %w", err)
	}

	// Create temp dir for compose file + raw metrics
	tmpDir, err := os.MkdirTemp("", "bench-"+tc.Name+"-")
	if err != nil {
		return results.RunResult{}, err
	}
	// MkdirTemp creates mode 0700, but containers with hardened images
	// (e.g. bench-collector runs as a non-root uid) bind-mount this dir
	// and need to write into it. Widen permissions so any uid can write.
	if err := os.Chmod(tmpDir, 0o777); err != nil {
		return results.RunResult{}, fmt.Errorf("chmod tmpdir: %w", err)
	}
	defer func() {
		if !r.opts.NoCleanup {
			os.RemoveAll(tmpDir)
		}
	}()

	// Resolve extra env from named configuration
	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		for k, v := range cfg.Env {
			extraEnv[k] = v
		}
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
	}

	var orch orchestrator.Orchestrator
	switch r.opts.Platform {
	case "kubernetes":
		kubeCfg := orchestrator.KubeConfig{
			RunConfig: runCfg,
		}
		kr, err := orchestrator.NewKubeRunner(kubeCfg)
		if err != nil {
			return results.RunResult{}, fmt.Errorf("kubernetes setup: %w", err)
		}
		orch = kr
	default: // docker
		cr, err := orchestrator.NewComposeRunner(runCfg)
		if err != nil {
			return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
		}
		orch = cr
	}

	// Force cleanup any leftover containers from previous runs to prevent
	// name collisions when running multiple subjects sequentially.
	cleanupContainers := []string{
		"bench-generator", "bench-receiver", "bench-collector",
		"bench-subject-" + subject.Name,
	}
	for _, c := range cleanupContainers {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()

	startTime := time.Now()

	fmt.Println("  starting containers…")
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting infrastructure: %w", err)
	}

	// Teardown on exit (unless --no-cleanup)
	cleanup := func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}
	defer cleanup()

	// Wait for the generator to finish (duration + warmup + buffer)
	duration := tc.DurationOrDefault(2 * time.Minute)
	warmup := tc.WarmupOrDefault(10 * time.Second)
	genTimeout := duration + warmup + 2*time.Minute
	if genTimeout > r.opts.Timeout {
		genTimeout = r.opts.Timeout
	}

	fmt.Printf("  waiting for generator (up to %s)…\n", genTimeout)
	if err := orch.WaitForGeneratorExit(genTimeout); err != nil {
		return results.RunResult{}, fmt.Errorf("waiting for generator: %w", err)
	}

	// Get a local port to reach the receiver's /metrics endpoint, then
	// poll until the receiver's line count stops moving (3 stable rounds)
	// or we hit the drain deadline. The previous fixed 5-second sleep
	// missed the tail when subjects had multi-GB in-flight buffers — we
	// observed regex_mask reporting 60% "loss" that turned out to be the
	// receiver still draining 200M+ lines after the harness had moved on.
	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	fmt.Println("  waiting for receiver to drain…")
	const drainPoll = 5 * time.Second
	const drainTimeout = 2 * time.Minute
	drainDeadline := time.Now().Add(drainTimeout)
	var drainStable int
	var drainLast int64
	for time.Now().Before(drainDeadline) {
		time.Sleep(drainPoll)
		rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			continue
		}
		fmt.Printf("    received: %s lines\n", formatCount(rm.LinesReceived))
		if rm.LinesReceived == drainLast && rm.LinesReceived > 0 {
			drainStable++
			if drainStable >= 3 {
				fmt.Println("    receiver stable — drain complete")
				break
			}
		} else {
			drainStable = 0
		}
		drainLast = rm.LinesReceived
	}

	// Fetch final metrics from receiver — this is the core result, so fail if unavailable.
	recvMetrics, err := r.queryReceiverMetrics(metricsPort, 30*time.Second)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("querying receiver metrics: %w", err)
	}

	elapsed := time.Since(startTime).Seconds()

	// Copy the metrics CSV first (collector writes rows incrementally),
	// then stop the collector. This order is required for Kubernetes where
	// stopping the pod deletes the emptyDir volume.
	metricsCSVSrc := filepath.Join(tmpDir, "metrics.csv")
	fmt.Println("  collecting metrics…")
	if err := orch.CopyMetricsCSV(metricsCSVSrc); err != nil {
		fmt.Fprintf(os.Stderr, "  warning: metrics CSV not available: %v\n", err)
		metricsCSVSrc = ""
	}

	fmt.Println("  stopping collector…")
	if err := orch.StopCollector(); err != nil {
		fmt.Fprintf(os.Stderr, "  warning: stopping collector: %v\n", err)
	}

	// Parse generator output for lines_in / bytes_in
	genStats := r.parseGeneratorStats(orch.GeneratorStdout())

	// Get system info (CPU cores, memory)
	sysCPUs, sysMemMB := getSystemInfo()

	// Aggregate all metrics from CSV
	var metrics results.AggregatedMetrics
	if metricsCSVSrc != "" {
		metrics, _ = results.AggregateAllMetricsFromCSV(metricsCSVSrc)
	}

	// Compute derived stats — use the generator's actual send duration for
	// throughput, not total elapsed time. This ensures tools with slow startup
	// (Cribl, Logstash, OTel Collector) are measured fairly on actual processing
	// time, not boot time.
	sendDuration := elapsed
	if genStats.DurationMs > 0 {
		sendDuration = float64(genStats.DurationMs) / 1000.0
	}
	// For blackhole / discard-target tests, the receiver never gets any
	// data (by design) — reporting throughput as lines_out/duration would
	// always be 0. Fall back to the generator's send rate (lines_in/duration)
	// so the reported number reflects how fast the subject can accept
	// input, which is what the test actually measures. Loss percentage
	// stays based on lines_out vs lines_in, which is still correct
	// (100% for blackhole — nothing comes out, by design).
	linesForRate := recvMetrics.LinesReceived
	if linesForRate == 0 && genStats.LinesSent > 0 {
		linesForRate = genStats.LinesSent
	}
	linesPerSec := 0.0
	if sendDuration > 0 {
		linesPerSec = float64(linesForRate) / sendDuration
	}
	lossPct := 0.0
	if genStats.LinesSent > 0 {
		lossPct = 100.0 * (1.0 - float64(recvMetrics.LinesReceived)/float64(genStats.LinesSent))
		if lossPct < 0 {
			lossPct = 0
		}
	}

	result := results.RunResult{
		TestName:        tc.Name,
		Config:          configName,
		Subject:         subject.Name,
		Version:         subject.Version,
		Hardware:        hardwareID(),
		Timestamp:       startTime,
		DurationSec:     sendDuration,
		LinesIn:         genStats.LinesSent,
		LinesOut:        recvMetrics.LinesReceived,
		BytesIn:         genStats.BytesSent,
		BytesOut:        recvMetrics.BytesReceived,
		LinesPerSec:     linesPerSec,
		LossPercent:     lossPct,
		AvgCPUPercent:   metrics.CPUAvg,
		MaxCPUPercent:   metrics.CPUMax,
		AvgMemMB:        metrics.MemAvgMB,
		MaxMemMB:        metrics.MemMaxMB,
		DiskReadBytes:   metrics.DiskRead,
		DiskWriteBytes:  metrics.DiskWrite,
		NetRecvBytes:    metrics.NetRecv,
		NetSendBytes:    metrics.NetSend,
		IOThroughputAvg: metrics.IOThroughputAvg,
		LoadAvg1:        metrics.LoadAvg1,
		LoadAvg5:        metrics.LoadAvg5,
		LoadAvg15:       metrics.LoadAvg15,
		SystemCPUs:      sysCPUs,
		SystemMemMB:     sysMemMB,
		SubjectCPULimit: r.opts.CPULimit,
		SubjectMemLimit: r.opts.MemLimit,
		LatencyP50Ms:    recvMetrics.LatencyP50Ms,
		LatencyP95Ms:    recvMetrics.LatencyP95Ms,
		LatencyP99Ms:    recvMetrics.LatencyP99Ms,
		Passed:          recvMetrics.Passed,
	}
	if recvMetrics.Passed != nil && !*recvMetrics.Passed {
		result.FailReason = strings.Join(recvMetrics.Errors, "; ")
	}

	// Plain correctness tests (type: correctness) typically don't enable
	// validate_order/dedup/content, so the receiver leaves Passed=nil.
	// Without a verdict, the UI renders ☠ even on a clean lines_in==
	// lines_out run. Compute pass/fail from loss_percent vs the case's
	// expected_loss_pct so plain correctness tests get a real green/red.
	// Also fail on over-delivery (sender duplicated records).
	if tc.Type == "correctness" && result.Passed == nil {
		passed := lossPct <= tc.Correctness.ExpectedLossPct
		var failReasons []string
		if !passed {
			failReasons = append(failReasons, fmt.Sprintf(
				"expected loss <= %.2f%%, got %.2f%%",
				tc.Correctness.ExpectedLossPct, lossPct))
		}
		if recvMetrics.LinesReceived > genStats.LinesSent {
			passed = false
			extra := recvMetrics.LinesReceived - genStats.LinesSent
			failReasons = append(failReasons, fmt.Sprintf(
				"over-delivery: received %s lines but only %s were sent (%s extra/duplicate lines)",
				formatCount(recvMetrics.LinesReceived), formatCount(genStats.LinesSent), formatCount(extra)))
		}
		result.Passed = &passed
		if !passed {
			result.FailReason = strings.Join(failReasons, "; ")
		}
	}

	dir, err := r.store.Save(result, metricsCSVSrc)
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}

	fmt.Printf("  done. results → %s\n", dir)
	fmt.Printf("  throughput: %s lines/s\n", formatCount(int64(linesPerSec)))
	fmt.Printf("  lines in: %s  lines out: %s  loss: %.2f%%\n",
		formatCount(genStats.LinesSent), formatCount(recvMetrics.LinesReceived), lossPct)
	fmt.Printf("  cpu: avg %.1f%% max %.1f%%  mem: avg %.0f MB max %.0f MB\n",
		metrics.CPUAvg, metrics.CPUMax, metrics.MemAvgMB, metrics.MemMaxMB)
	if metrics.DiskRead > 0 || metrics.DiskWrite > 0 {
		fmt.Printf("  disk: read %.0f MB  write %.0f MB\n",
			float64(metrics.DiskRead)/(1024*1024), float64(metrics.DiskWrite)/(1024*1024))
	}
	if metrics.IOThroughputAvg > 0 {
		fmt.Printf("  io throughput: avg %.1f MB/s\n", metrics.IOThroughputAvg/(1024*1024))
	}
	if metrics.LoadAvg1 > 0 {
		fmt.Printf("  load avg: 1m=%.2f 5m=%.2f 15m=%.2f\n",
			metrics.LoadAvg1, metrics.LoadAvg5, metrics.LoadAvg15)
	}
	if recvMetrics.LatencyP50Ms > 0 {
		fmt.Printf("  latency: p50=%.1fms p95=%.1fms p99=%.1fms\n",
			recvMetrics.LatencyP50Ms, recvMetrics.LatencyP95Ms, recvMetrics.LatencyP99Ms)
	}
	if tc.Correctness.ValidateContent || tc.Correctness.ValidateDedup {
		fmt.Printf("  malformed lines: %s\n", formatCount(recvMetrics.MalformedLines))
	}
	if r.opts.CPULimit != "" || r.opts.MemLimit != "" {
		fmt.Printf("  subject limits: cpu=%s mem=%s\n",
			defaultVal(r.opts.CPULimit, "unlimited"), defaultVal(r.opts.MemLimit, "unlimited"))
	}
	fmt.Printf("  system: %d CPUs, %d MB RAM  send: %.1fs  total: %.1fs\n", sysCPUs, sysMemMB, sendDuration, elapsed)

	if recvMetrics.Passed != nil {
		if *recvMetrics.Passed {
			fmt.Println("  correctness: PASSED")
		} else {
			fmt.Println("  correctness: FAILED")
			for _, e := range recvMetrics.Errors {
				fmt.Printf("    - %s\n", e)
			}
		}
	}

	// If no data was received, dump container logs to help diagnose
	if recvMetrics.LinesReceived == 0 {
		fmt.Fprintln(os.Stderr, "\n  WARNING: 0 lines received. Container logs:")
		fmt.Fprintf(os.Stderr, "\n  --- generator ---\n%s", orch.Logs("generator", 30))
		fmt.Fprintf(os.Stderr, "\n  --- subject ---\n%s", orch.Logs("subject", 30))
		fmt.Fprintf(os.Stderr, "\n  --- receiver ---\n%s", orch.Logs("receiver", 30))
	}

	return result, nil
}

// runPersistenceCorrectness tests store-and-forward: sends logs while the
// receiver is down, then starts the receiver and verifies all logs arrive.
//
// Flow:
//  1. Start subject + collector (no receiver, no generator)
//  2. Start generator — sends sequenced logs for the configured duration
//  3. Generator finishes, wait a moment for subject to persist
//  4. Start receiver
//  5. Wait for subject to forward buffered logs to receiver
//  6. Verify: all logs should arrive with 0% loss
func (r *Runner) runPersistenceCorrectness(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	if r.opts.SubjectVersion != "" {
		subject = subject.WithVersion(r.opts.SubjectVersion)
	}

	fmt.Printf("→ test=%s  subject=%s  version=%s  config=%s\n",
		tc.Name, subject.Name, subject.Version, configName)

	configSrc, err := tc.ConfigFilePath(r.opts.CasesDir, configName, subject)
	if err != nil {
		return results.RunResult{}, err
	}
	configSrc, err = filepath.Abs(configSrc)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving config path: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "bench-"+tc.Name+"-")
	if err != nil {
		return results.RunResult{}, err
	}
	// MkdirTemp creates mode 0700, but containers with hardened images
	// (e.g. bench-collector runs as a non-root uid) bind-mount this dir
	// and need to write into it. Widen permissions so any uid can write.
	if err := os.Chmod(tmpDir, 0o777); err != nil {
		return results.RunResult{}, fmt.Errorf("chmod tmpdir: %w", err)
	}
	defer func() {
		if !r.opts.NoCleanup {
			os.RemoveAll(tmpDir)
		}
	}()

	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		for k, v := range cfg.Env {
			extraEnv[k] = v
		}
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
	}

	cr, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}
	orch := cr

	// Cleanup leftovers
	for _, c := range []string{"bench-generator", "bench-receiver", "bench-collector", "bench-subject-" + subject.Name} {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()

	startTime := time.Now()

	cleanup := func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}
	defer cleanup()

	// PHASE 1: Start subject + collector only (receiver is deliberately down)
	fmt.Println("  phase 1: starting subject (receiver is DOWN)…")
	if err := orch.UpServices("subject", "collector"); err != nil {
		return results.RunResult{}, fmt.Errorf("starting subject: %w", err)
	}

	// PHASE 2: Start generator — sends logs to subject while receiver is down
	fmt.Println("  phase 2: sending logs (receiver still DOWN)…")
	if err := orch.UpServices("generator"); err != nil {
		return results.RunResult{}, fmt.Errorf("starting generator: %w", err)
	}

	duration := tc.DurationOrDefault(10 * time.Second)
	warmup := tc.WarmupOrDefault(10 * time.Second)
	genTimeout := duration + warmup + 2*time.Minute
	if genTimeout > r.opts.Timeout {
		genTimeout = r.opts.Timeout
	}

	fmt.Printf("  waiting for generator (up to %s)…\n", genTimeout)
	if err := orch.WaitForGeneratorExit(genTimeout); err != nil {
		return results.RunResult{}, fmt.Errorf("waiting for generator: %w", err)
	}

	genStats := r.parseGeneratorStats(orch.GeneratorStdout())
	fmt.Printf("  generator sent %s lines\n", formatCount(genStats.LinesSent))

	// PHASE 3: Wait for subject to persist all buffered data
	fmt.Println("  phase 3: waiting for subject to persist data…")
	time.Sleep(10 * time.Second)

	// PHASE 4: Start receiver — subject should now forward persisted logs
	fmt.Println("  phase 4: starting receiver (subject should forward buffered logs)…")
	if err := orch.UpServices("receiver"); err != nil {
		return results.RunResult{}, fmt.Errorf("starting receiver: %w", err)
	}

	// PHASE 5: Wait for logs to arrive
	drainTimeout := 2 * time.Minute
	fmt.Printf("  phase 5: waiting for logs to drain (up to %s)…\n", drainTimeout)

	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	// Poll receiver until lines stabilize or timeout
	var lastCount int64
	stableRounds := 0
	drainDeadline := time.Now().Add(drainTimeout)
	for time.Now().Before(drainDeadline) {
		time.Sleep(5 * time.Second)
		rm, err := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if err != nil {
			continue
		}
		fmt.Printf("    received: %s / %s lines\n", formatCount(rm.LinesReceived), formatCount(genStats.LinesSent))
		if rm.LinesReceived == lastCount && rm.LinesReceived > 0 {
			stableRounds++
			if stableRounds >= 3 {
				fmt.Println("    receiver stable — all logs drained")
				break
			}
		} else {
			stableRounds = 0
		}
		lastCount = rm.LinesReceived
	}

	// Final metrics
	recvMetrics, err := r.queryReceiverMetrics(metricsPort, 30*time.Second)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("querying receiver metrics: %w", err)
	}

	elapsed := time.Since(startTime).Seconds()

	// Compute results
	lossPct := 0.0
	if genStats.LinesSent > 0 {
		lossPct = 100.0 * (1.0 - float64(recvMetrics.LinesReceived)/float64(genStats.LinesSent))
		if lossPct < 0 {
			lossPct = 0
		}
	}

	passed := lossPct <= tc.Correctness.ExpectedLossPct
	var errors []string
	if !passed {
		errors = append(errors, fmt.Sprintf("expected loss <= %.2f%%, got %.2f%% (%s of %s lines lost)",
			tc.Correctness.ExpectedLossPct, lossPct,
			formatCount(genStats.LinesSent-recvMetrics.LinesReceived), formatCount(genStats.LinesSent)))
	}
	if recvMetrics.LinesReceived > genStats.LinesSent {
		passed = false
		extra := recvMetrics.LinesReceived - genStats.LinesSent
		errors = append(errors, fmt.Sprintf("over-delivery: received %s lines but only %s were sent (%s extra/duplicate lines)",
			formatCount(recvMetrics.LinesReceived), formatCount(genStats.LinesSent), formatCount(extra)))
	}
	if tc.Correctness.ValidateDedup && recvMetrics.Duplicates > 0 {
		passed = false
		errors = append(errors, fmt.Sprintf("expected 0 duplicates, got %s",
			formatCount(recvMetrics.Duplicates)))
	}
	if (tc.Correctness.ValidateDedup || tc.Correctness.ValidateContent) && recvMetrics.MalformedLines > 0 {
		passed = false
		errors = append(errors, fmt.Sprintf("expected 0 malformed lines, got %s (memory corruption)",
			formatCount(recvMetrics.MalformedLines)))
	}

	result := results.RunResult{
		TestName:    tc.Name,
		Config:      configName,
		Subject:     subject.Name,
		Version:     subject.Version,
		Hardware:    hardwareID(),
		Timestamp:   startTime,
		DurationSec: elapsed,
		LinesIn:     genStats.LinesSent,
		LinesOut:    recvMetrics.LinesReceived,
		BytesIn:     genStats.BytesSent,
		BytesOut:    recvMetrics.BytesReceived,
		LossPercent: lossPct,
		Passed:      &passed,
	}
	if !passed {
		result.FailReason = strings.Join(errors, "; ")
	}

	dir, err := r.store.Save(result, "")
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}

	fmt.Printf("  done. results → %s\n", dir)
	fmt.Printf("  lines sent: %s  lines received: %s  loss: %.2f%%\n",
		formatCount(genStats.LinesSent), formatCount(recvMetrics.LinesReceived), lossPct)
	if tc.Correctness.ValidateDedup {
		fmt.Printf("  unique lines: %s  duplicates: %s  malformed: %s\n",
			formatCount(recvMetrics.UniqueLines), formatCount(recvMetrics.Duplicates), formatCount(recvMetrics.MalformedLines))
	} else if tc.Correctness.ValidateContent {
		fmt.Printf("  malformed: %s\n", formatCount(recvMetrics.MalformedLines))
	}
	fmt.Printf("  total time: %.1fs\n", elapsed)

	if passed {
		fmt.Println("  persistence correctness: PASSED ✓")
	} else {
		fmt.Println("  persistence correctness: FAILED ✗")
		for _, e := range errors {
			fmt.Printf("    - %s\n", e)
		}
	}

	if recvMetrics.LinesReceived == 0 {
		fmt.Fprintln(os.Stderr, "\n  WARNING: 0 lines received. Container logs:")
		fmt.Fprintf(os.Stderr, "\n  --- generator ---\n%s", orch.Logs("generator", 30))
		fmt.Fprintf(os.Stderr, "\n  --- subject ---\n%s", orch.Logs("subject", 30))
		fmt.Fprintf(os.Stderr, "\n  --- receiver ---\n%s", orch.Logs("receiver", 30))
	}

	return result, nil
}

// runPersistenceRestartCorrectness tests true durable persistence across a
// subject restart:
//
//  1. Start subject + collector (receiver DOWN)
//  2. Start generator — sends sequenced logs
//  3. Wait for subject to persist
//  4. Stop subject (SIGTERM, graceful shutdown → must flush state to disk)
//  5. Start receiver
//  6. Restart subject — it should read from persistent store and forward
//  7. Drain and verify all logs arrive with 0% loss, 0 duplicates
func (r *Runner) runPersistenceRestartCorrectness(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	if r.opts.SubjectVersion != "" {
		subject = subject.WithVersion(r.opts.SubjectVersion)
	}

	fmt.Printf("→ test=%s  subject=%s  version=%s  config=%s\n",
		tc.Name, subject.Name, subject.Version, configName)

	configSrc, err := tc.ConfigFilePath(r.opts.CasesDir, configName, subject)
	if err != nil {
		return results.RunResult{}, err
	}
	configSrc, err = filepath.Abs(configSrc)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving config path: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "bench-"+tc.Name+"-")
	if err != nil {
		return results.RunResult{}, err
	}
	// MkdirTemp creates mode 0700, but containers with hardened images
	// (e.g. bench-collector runs as a non-root uid) bind-mount this dir
	// and need to write into it. Widen permissions so any uid can write.
	if err := os.Chmod(tmpDir, 0o777); err != nil {
		return results.RunResult{}, fmt.Errorf("chmod tmpdir: %w", err)
	}
	defer func() {
		if !r.opts.NoCleanup {
			os.RemoveAll(tmpDir)
		}
	}()

	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		for k, v := range cfg.Env {
			extraEnv[k] = v
		}
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
	}

	cr, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}
	orch := cr

	// Cleanup leftovers
	for _, c := range []string{"bench-generator", "bench-receiver", "bench-collector", "bench-subject-" + subject.Name} {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()

	startTime := time.Now()

	cleanup := func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}
	defer cleanup()

	// PHASE 1: Start subject + collector only (receiver is deliberately down)
	fmt.Println("  phase 1: starting subject (receiver is DOWN)…")
	if err := orch.UpServices("subject", "collector"); err != nil {
		return results.RunResult{}, fmt.Errorf("starting subject: %w", err)
	}

	// PHASE 2: Start generator
	fmt.Println("  phase 2: sending logs (receiver still DOWN)…")
	if err := orch.UpServices("generator"); err != nil {
		return results.RunResult{}, fmt.Errorf("starting generator: %w", err)
	}

	duration := tc.DurationOrDefault(10 * time.Second)
	warmup := tc.WarmupOrDefault(10 * time.Second)
	genTimeout := duration + warmup + 2*time.Minute
	if genTimeout > r.opts.Timeout {
		genTimeout = r.opts.Timeout
	}

	fmt.Printf("  waiting for generator (up to %s)…\n", genTimeout)
	if err := orch.WaitForGeneratorExit(genTimeout); err != nil {
		return results.RunResult{}, fmt.Errorf("waiting for generator: %w", err)
	}

	genStats := r.parseGeneratorStats(orch.GeneratorStdout())
	fmt.Printf("  generator sent %s lines\n", formatCount(genStats.LinesSent))

	// PHASE 3: immediately stop subject — SIGTERM must flush in-flight state to disk
	fmt.Println("  phase 3: stopping subject immediately (SIGTERM)…")
	if err := orch.StopServices("subject"); err != nil {
		return results.RunResult{}, fmt.Errorf("stopping subject: %w", err)
	}

	// PHASE 4: Start receiver while subject is stopped
	fmt.Println("  phase 4: starting receiver (subject still stopped)…")
	if err := orch.UpServices("receiver"); err != nil {
		return results.RunResult{}, fmt.Errorf("starting receiver: %w", err)
	}

	// Small pause so receiver is fully ready before subject comes back online
	time.Sleep(3 * time.Second)

	// PHASE 5: Restart subject — it should read persisted logs and forward them
	fmt.Println("  phase 5: restarting subject (should replay persisted logs)…")
	if err := orch.UpServices("subject"); err != nil {
		return results.RunResult{}, fmt.Errorf("restarting subject: %w", err)
	}

	// PHASE 6: Wait for logs to drain
	drainTimeout := 3 * time.Minute
	fmt.Printf("  phase 6: waiting for logs to drain (up to %s)…\n", drainTimeout)

	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	var lastCount int64
	stableRounds := 0
	drainDeadline := time.Now().Add(drainTimeout)
	for time.Now().Before(drainDeadline) {
		time.Sleep(5 * time.Second)
		rm, err := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if err != nil {
			continue
		}
		fmt.Printf("    received: %s / %s lines\n", formatCount(rm.LinesReceived), formatCount(genStats.LinesSent))
		if rm.LinesReceived == lastCount && rm.LinesReceived > 0 {
			stableRounds++
			if stableRounds >= 3 {
				fmt.Println("    receiver stable — all logs drained")
				break
			}
		} else {
			stableRounds = 0
		}
		lastCount = rm.LinesReceived
	}

	recvMetrics, err := r.queryReceiverMetrics(metricsPort, 30*time.Second)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("querying receiver metrics: %w", err)
	}

	elapsed := time.Since(startTime).Seconds()

	lossPct := 0.0
	if genStats.LinesSent > 0 {
		lossPct = 100.0 * (1.0 - float64(recvMetrics.LinesReceived)/float64(genStats.LinesSent))
		if lossPct < 0 {
			lossPct = 0
		}
	}

	passed := lossPct <= tc.Correctness.ExpectedLossPct
	var errors []string
	if !passed {
		errors = append(errors, fmt.Sprintf("expected loss <= %.2f%%, got %.2f%% (%s of %s lines lost)",
			tc.Correctness.ExpectedLossPct, lossPct,
			formatCount(genStats.LinesSent-recvMetrics.LinesReceived), formatCount(genStats.LinesSent)))
	}
	if recvMetrics.LinesReceived > genStats.LinesSent {
		passed = false
		extra := recvMetrics.LinesReceived - genStats.LinesSent
		errors = append(errors, fmt.Sprintf("over-delivery: received %s lines but only %s were sent (%s extra/duplicate lines)",
			formatCount(recvMetrics.LinesReceived), formatCount(genStats.LinesSent), formatCount(extra)))
	}
	if tc.Correctness.ValidateDedup && recvMetrics.Duplicates > 0 {
		passed = false
		errors = append(errors, fmt.Sprintf("expected 0 duplicates, got %s",
			formatCount(recvMetrics.Duplicates)))
	}
	if (tc.Correctness.ValidateDedup || tc.Correctness.ValidateContent) && recvMetrics.MalformedLines > 0 {
		passed = false
		errors = append(errors, fmt.Sprintf("expected 0 malformed lines, got %s (memory corruption)",
			formatCount(recvMetrics.MalformedLines)))
	}

	result := results.RunResult{
		TestName:    tc.Name,
		Config:      configName,
		Subject:     subject.Name,
		Version:     subject.Version,
		Hardware:    hardwareID(),
		Timestamp:   startTime,
		DurationSec: elapsed,
		LinesIn:     genStats.LinesSent,
		LinesOut:    recvMetrics.LinesReceived,
		BytesIn:     genStats.BytesSent,
		BytesOut:    recvMetrics.BytesReceived,
		LossPercent: lossPct,
		Passed:      &passed,
	}
	if !passed {
		result.FailReason = strings.Join(errors, "; ")
	}

	dir, err := r.store.Save(result, "")
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}

	fmt.Printf("  done. results → %s\n", dir)
	fmt.Printf("  lines sent: %s  lines received: %s  loss: %.2f%%\n",
		formatCount(genStats.LinesSent), formatCount(recvMetrics.LinesReceived), lossPct)
	if tc.Correctness.ValidateDedup {
		fmt.Printf("  unique lines: %s  duplicates: %s  malformed: %s\n",
			formatCount(recvMetrics.UniqueLines), formatCount(recvMetrics.Duplicates), formatCount(recvMetrics.MalformedLines))
	} else if tc.Correctness.ValidateContent {
		fmt.Printf("  malformed: %s\n", formatCount(recvMetrics.MalformedLines))
	}
	fmt.Printf("  total time: %.1fs\n", elapsed)

	if passed {
		fmt.Println("  persistence restart correctness: PASSED ✓")
	} else {
		fmt.Println("  persistence restart correctness: FAILED ✗")
		for _, e := range errors {
			fmt.Printf("    - %s\n", e)
		}
	}

	if recvMetrics.LinesReceived == 0 {
		fmt.Fprintln(os.Stderr, "\n  WARNING: 0 lines received. Container logs:")
		fmt.Fprintf(os.Stderr, "\n  --- generator ---\n%s", orch.Logs("generator", 30))
		fmt.Fprintf(os.Stderr, "\n  --- subject ---\n%s", orch.Logs("subject", 30))
		fmt.Fprintf(os.Stderr, "\n  --- receiver ---\n%s", orch.Logs("receiver", 30))
	}

	return result, nil
}

func (r *Runner) queryReceiverMetrics(port int, timeout time.Duration) (ReceiverMetrics, error) {
	url := fmt.Sprintf("http://localhost:%d/metrics", port)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		var m ReceiverMetrics
		if err := json.Unmarshal(body, &m); err != nil {
			time.Sleep(time.Second)
			continue
		}
		return m, nil
	}
	return ReceiverMetrics{}, fmt.Errorf("receiver metrics not available after %s", timeout)
}

// parseGeneratorStats extracts the JSON result from the generator's stdout.
func (r *Runner) parseGeneratorStats(stdout string) GeneratorResult {
	// The generator prints JSON to stdout. Find the first '{' and last '}'.
	start := strings.Index(stdout, "{")
	end := strings.LastIndex(stdout, "}")
	if start < 0 || end < 0 || end <= start {
		return GeneratorResult{}
	}
	var g GeneratorResult
	if err := json.Unmarshal([]byte(stdout[start:end+1]), &g); err != nil {
		fmt.Fprintf(os.Stderr, "  warning: could not parse generator output: %v\n", err)
		return GeneratorResult{}
	}
	return g
}

// getSystemInfo returns the number of CPU cores and total memory in MB.
func getSystemInfo() (cpus int, memMB int64) {
	cpus = runtime.NumCPU()

	// Try to read total memory from /proc/meminfo (Linux)
	data, err := os.ReadFile("/proc/meminfo")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					kb, _ := strconv.ParseInt(fields[1], 10, 64)
					memMB = kb / 1024
					return
				}
			}
		}
	}

	// Fallback: not available (Windows, etc.)
	return cpus, 0
}

func defaultVal(val, def string) string {
	if val != "" {
		return val
	}
	return def
}

func formatCount(n int64) string {
	s := strconv.FormatInt(n, 10)
	if n < 1000 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}
