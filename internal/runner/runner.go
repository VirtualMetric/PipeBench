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
	// Override subject image repository (empty = use registry default)
	SubjectImage string
	// Override configuration name (empty = "default")
	ConfigName string
	// Skip teardown — leave containers running (useful for debugging)
	NoCleanup bool
	// Port on the host that the receiver metrics endpoint is exposed on
	ReceiverHostPort int
	// Maximum time to wait for the test to complete
	Timeout time.Duration
	// Resource limits for the subject container
	CPULimit string // e.g. "1", "4", "0.5" — number of cores
	MemLimit string // e.g. "1g", "16g", "512m"

	// Drain, when > 0, switches performance tests into diagnostic drain mode:
	// after the generator exits we wait up to this long for the receiver to
	// go idle (instead of the short fixed grace), recompute EPS over the
	// receiver window (last_received − first_received), and skip persisting
	// the result. Used to tell apart real data loss from "the test ended
	// before the subject finished forwarding its queue."
	Drain time.Duration
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
}

// ReceiverMetrics is the JSON response from the receiver's /metrics endpoint.
type ReceiverMetrics struct {
	LinesReceived   int64    `json:"lines_received"`
	BytesReceived   int64    `json:"bytes_received"`
	Done            bool     `json:"done"`
	FirstReceivedNs int64    `json:"first_received_ns"`
	LastReceivedNs  int64    `json:"last_received_ns"`
	Passed          *bool    `json:"passed,omitempty"`
	Errors          []string `json:"errors,omitempty"`
	UniqueLines     int64    `json:"unique_lines,omitempty"`
	Duplicates      int64    `json:"duplicates,omitempty"`
	MalformedLines  int64    `json:"malformed_lines,omitempty"`
	InvalidJSONLines int64   `json:"invalid_json_lines,omitempty"`
	LatencyP50Ms    float64  `json:"latency_p50_ms,omitempty"`
	LatencyP95Ms    float64  `json:"latency_p95_ms,omitempty"`
	LatencyP99Ms    float64  `json:"latency_p99_ms,omitempty"`
}

// GeneratorResult is the JSON output from the generator container.
type GeneratorResult struct {
	LinesSent   int64 `json:"lines_sent"`
	BytesSent   int64 `json:"bytes_sent"`
	DurationMs  int64 `json:"duration_ms"`
	FirstSentNs int64 `json:"first_sent_ns"`
	LastSentNs  int64 `json:"last_sent_ns"`
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

// applySubjectOverrides returns the subject with the CLI image/version
// overrides applied. Each override is independent: an empty value leaves the
// registry default in place, so callers can override the image, the version,
// both, or neither.
func (r *Runner) applySubjectOverrides(subject config.Subject) config.Subject {
	if r.opts.SubjectImage != "" {
		subject = subject.WithImage(r.opts.SubjectImage)
	}
	subject = r.applySubjectOverrides(subject)
	return subject
}

// Run executes the test and returns the persisted result.
func (r *Runner) Run(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	if tc.Type == "persistence_correctness" {
		return r.runPersistenceCorrectness(tc, subject)
	}
	if tc.Type == "persistence_restart_correctness" {
		return r.runPersistenceShutdownCorrectness(tc, subject, false)
	}
	if tc.Type == "persistence_crash_correctness" {
		return r.runPersistenceShutdownCorrectness(tc, subject, true)
	}
	if tc.Type == "persistence_file_restart_correctness" {
		return r.runPersistenceFileRestartCorrectness(tc, subject)
	}

	configName := r.opts.ConfigName

	// Resolve subject image/version overrides (empty = registry default)
	subject = r.applySubjectOverrides(subject)

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

	// TLS prep: when any generator opts into TLS, generate a self-signed
	// CA + leaf cert set into <tmpDir>/certs and pass the path down to
	// the orchestrator so it's bind-mounted into both the subject and the
	// generator container(s). Subjects that don't declare tls_tcp in
	// their Capabilities cause the case to fail fast (cleaner than
	// starting and silently producing zero ingest).
	tlsCertsHost := ""
	if tlsRequested(tc) {
		if !subject.HasCapability("tls_tcp") {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare TLS support (capability \"tls_tcp\")", subject.Name)
		}
		certsDir := filepath.Join(tmpDir, "certs")
		path, err := orchestrator.GenerateTLSCerts(certsDir, []string{"subject", "localhost"})
		if err != nil {
			return results.RunResult{}, fmt.Errorf("generating TLS certs: %w", err)
		}
		tlsCertsHost = path
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
		TLSCertsHost:     tlsCertsHost,
	}

	cr, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}
	var orch orchestrator.Orchestrator = cr

	// Force cleanup any leftover containers from previous runs to prevent
	// name collisions when running multiple subjects sequentially.
	cleanupContainers := []string{
		"bench-generator", "bench-receiver", "bench-collector",
		"bench-subject-" + subject.Name,
	}
	// Plural-mode containers (bench-generator-<id>, bench-receiver-<id>)
	// need cleanup too, otherwise a re-run of the same case can collide.
	for _, c := range cr.GeneratorContainers() {
		cleanupContainers = append(cleanupContainers, c)
	}
	for _, name := range cr.ReceiverMetricsPorts() {
		_ = name // ports, not names; container cleanup happens via Down() + explicit list below
	}
	if tc.MultiReceiver() {
		for _, rc := range tc.Receivers {
			cleanupContainers = append(cleanupContainers, "bench-receiver-"+rc.ID)
		}
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

	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	if tc.Type == "performance" && r.opts.Drain > 0 {
		// Diagnostic drain mode: poll the receiver until it goes idle (or the
		// configured ceiling fires). Same pattern as the correctness path
		// below, but bounded by --drain.
		fmt.Printf("  drain mode: waiting up to %s for receiver to go idle…\n", r.opts.Drain)
		const drainPoll = 5 * time.Second
		const quietPolls = 6 // 30s stable window
		ports := orch.ReceiverMetricsPorts()
		drainDeadline := time.Now().Add(r.opts.Drain)
		var drainStable int
		var drainLast int64
		drainStart := time.Now()
		for time.Now().Before(drainDeadline) {
			time.Sleep(drainPoll)
			var totalLines int64
			if tc.MultiReceiver() {
				agg, _, qerr := r.aggregateReceivers(ports, 10*time.Second)
				if qerr != nil {
					continue
				}
				totalLines = agg.LinesReceived
			} else {
				rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
				if qerr != nil {
					continue
				}
				totalLines = rm.LinesReceived
			}
			fmt.Printf("    received: %s lines\n", formatCount(totalLines))
			if totalLines == drainLast && totalLines > 0 {
				drainStable++
				if drainStable >= quietPolls {
					fmt.Printf("    receiver stable — drain complete after %s\n", time.Since(drainStart).Round(time.Second))
					break
				}
			} else {
				drainStable = 0
			}
			drainLast = totalLines
		}
	} else if tc.Type == "performance" {
		drainGrace := tc.DrainGraceOrDefault(5 * time.Second)
		if drainGrace > 0 {
			fmt.Printf("  waiting post-send receive grace (%s)…\n", drainGrace)
			time.Sleep(drainGrace)
		}
	} else {
		// Correctness tests need completeness rather than a fixed SLA window:
		// wait until the receiver stops moving or the bounded drain timeout hits.
		// In drain-aware mode (correctness.drain_seconds set) the timeout
		// follows the case; otherwise the default 2-minute ceiling applies.
		fmt.Println("  waiting for receiver to drain…")
		const drainPoll = 5 * time.Second
		drainTimeout := 2 * time.Minute
		if tc.Correctness.DrainSeconds > 0 {
			drainTimeout = time.Duration(tc.Correctness.DrainSeconds) * time.Second
		}
		quietWindow := parseDurationOr(tc.Correctness.DrainQuietWindow, 0)
		// Convert quietWindow into a poll count (default behaviour:
		// 12 stable polls of 5s = 60s, same as before).
		quietPolls := 12
		if quietWindow > 0 {
			quietPolls = int(quietWindow / drainPoll)
			if quietPolls < 1 {
				quietPolls = 1
			}
		}
		ports := orch.ReceiverMetricsPorts()
		drainDeadline := time.Now().Add(drainTimeout)
		var drainStable int
		var drainLast int64
		for time.Now().Before(drainDeadline) {
			time.Sleep(drainPoll)
			var totalLines int64
			if tc.MultiReceiver() {
				agg, _, qerr := r.aggregateReceivers(ports, 10*time.Second)
				if qerr != nil {
					continue
				}
				totalLines = agg.LinesReceived
			} else {
				rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
				if qerr != nil {
					continue
				}
				totalLines = rm.LinesReceived
			}
			fmt.Printf("    received: %s lines\n", formatCount(totalLines))
			if totalLines == drainLast && totalLines > 0 {
				drainStable++
				if drainStable >= quietPolls {
					fmt.Println("    receiver stable — drain complete")
					break
				}
			} else {
				drainStable = 0
			}
			drainLast = totalLines
		}
	}

	// Fetch final metrics from receiver(s).
	var recvMetrics ReceiverMetrics
	var perReceiver []PerReceiverMetrics
	if tc.MultiReceiver() {
		ports := orch.ReceiverMetricsPorts()
		recvMetrics, perReceiver, err = r.aggregateReceivers(ports, 30*time.Second)
		if err != nil {
			return results.RunResult{}, fmt.Errorf("querying receiver metrics: %w", err)
		}
	} else {
		recvMetrics, err = r.queryReceiverMetrics(metricsPort, 30*time.Second)
		if err != nil {
			return results.RunResult{}, fmt.Errorf("querying receiver metrics: %w", err)
		}
	}

	elapsed := time.Since(startTime).Seconds()

	// The result cutoff has been captured. Stop the subject now as cleanup;
	// lines flushed during this SIGTERM grace are intentionally outside the
	// scored performance window. A failure here (e.g. stop timeout) must not
	// discard the metrics already collected — the deferred Down() will force
	// teardown regardless. Warn and continue, like the collector stop below.
	fmt.Println("  stopping subject (SIGTERM, 5s grace)…")
	if err := orch.StopServices(5*time.Second, "subject"); err != nil {
		fmt.Fprintf(os.Stderr, "  warning: stopping subject: %v\n", err)
	}

	// Copy the metrics CSV first (collector writes rows incrementally),
	// then stop the collector.
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

	// Parse generator output for lines_in / bytes_in. In multi-generator
	// mode this sums every generator's stdout JSON blob.
	genStats := r.aggregateGenerators(orch)

	// Get system info (CPU cores, memory)
	sysCPUs, sysMemMB := getSystemInfo()

	// Compute the active benchmark window. Startup/warmup is excluded, while
	// send back-pressure and in-grace receiver drain are included.
	sendDuration := elapsed
	if genStats.DurationMs > 0 {
		sendDuration = float64(genStats.DurationMs) / 1000.0
	}
	activeStartNs, activeEndNs, rateDuration := benchmarkWindow(genStats, recvMetrics, sendDuration)

	// In drain mode, EPS reflects the receiver's actual active window so the
	// number is "lines delivered / time the subject was actively delivering"
	// rather than "lines delivered / send window" — that lets us tell apart
	// real loss (subject dropped data) from queue tail (subject still
	// forwarding when the fixed grace expired).
	if r.opts.Drain > 0 && recvMetrics.LastReceivedNs > recvMetrics.FirstReceivedNs {
		rateDuration = float64(recvMetrics.LastReceivedNs-recvMetrics.FirstReceivedNs) / 1e9
	}

	// Aggregate resource metrics over the active work window so averages are
	// not diluted by cold start or post-cutoff idle samples.
	var metrics results.AggregatedMetrics
	if metricsCSVSrc != "" {
		if activeStartNs > 0 && activeEndNs > activeStartNs {
			metrics, _ = results.AggregateAllMetricsFromCSVWindow(metricsCSVSrc, activeStartNs, activeEndNs)
			if metrics.Samples == 0 {
				metrics, _ = results.AggregateAllMetricsFromCSV(metricsCSVSrc)
			}
		} else {
			metrics, _ = results.AggregateAllMetricsFromCSV(metricsCSVSrc)
		}
	}
	// For blackhole / discard-target tests, the receiver never gets any
	// data (by design) — reporting throughput as lines_out/duration would
	// always be 0. Fall back to the generator's send rate (lines_in/duration)
	// so the reported number reflects how fast the subject can accept
	// input, which is what the test actually measures. Loss percentage
	// stays based on lines_out vs lines_in, which is still correct
	// (100% for blackhole — nothing comes out, by design).
	//
	// Gated on the case name containing "blackhole" so a real failure on a
	// regular performance test (subject crashed, wire-format mismatch,
	// receiver got 0 lines) doesn't silently report the generator's send
	// rate as throughput — it should report 0 EPS so the summary row
	// matches reality.
	isBlackholeCase := strings.Contains(tc.Name, "blackhole")
	linesForRate := recvMetrics.LinesReceived
	if isBlackholeCase && linesForRate == 0 && genStats.LinesSent > 0 {
		linesForRate = genStats.LinesSent
	}
	linesPerSec := 0.0
	if rateDuration > 0 {
		linesPerSec = float64(linesForRate) / rateDuration
	}
	// expected_multiplier scales the generator total for fan-out cases:
	// with M receivers each seeing every record, the expected receiver
	// total is lines_in * M. Defaults to 1 (no fan-out) so existing
	// math is unchanged.
	expectedMul := tc.Correctness.ExpectedMultiplier
	if expectedMul < 1 {
		expectedMul = 1
	}
	expectedOut := genStats.LinesSent * int64(expectedMul)
	lossPct := 0.0
	if expectedOut > 0 {
		lossPct = 100.0 * (1.0 - float64(recvMetrics.LinesReceived)/float64(expectedOut))
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
		DurationSec:     rateDuration,
		FirstSentNs:     genStats.FirstSentNs,
		LastSentNs:      genStats.LastSentNs,
		FirstReceivedNs: recvMetrics.FirstReceivedNs,
		LastReceivedNs:  recvMetrics.LastReceivedNs,
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

	// A performance test that delivered zero records while the generator
	// successfully sent some is a catastrophic failure (subject crashed,
	// wire-format mismatch, target endpoint wrong, …) — not a healthy
	// 0-EPS run. Mark it FAIL so the summary status row doesn't claim
	// OK on a 100%-loss outcome. Blackhole cases are excluded — 100%
	// loss is the designed behavior there.
	if tc.Type == "performance" && !isBlackholeCase &&
		recvMetrics.LinesReceived == 0 && genStats.LinesSent > 0 {
		f := false
		result.Passed = &f
		if result.FailReason == "" {
			result.FailReason = "100% loss: subject delivered zero lines (likely crashed or rejected all input — check container logs)"
		}
	}

	// Per-receiver counts are persisted onto the result for multi-receiver
	// runs (Feature C / E) so the UI and load-balance validator can see
	// each sink. Empty for singular-receiver cases.
	if len(perReceiver) > 0 {
		pr := make(map[string]int64, len(perReceiver))
		for _, m := range perReceiver {
			pr[m.ID] = m.LinesReceived
		}
		result.PerReceiver = pr
	}

	// For type: correctness, the loss budget (expected_loss_pct) and the
	// over-delivery check are authoritative. AND them with the receiver's
	// verdict — receiver Passed=nil means "no opinion" and doesn't veto.
	// Previously this block was gated on result.Passed == nil, which meant
	// validate_dedup runs with zero received lines reported PASSED: the
	// dedup check trivially passes over an empty set, so the receiver set
	// Passed=true and the loss check below was skipped entirely.
	if tc.Type == "correctness" {
		lossOK := lossPct <= tc.Correctness.ExpectedLossPct
		overOK := recvMetrics.LinesReceived <= expectedOut
		recvOK := result.Passed == nil || *result.Passed

		var failReasons []string
		if result.FailReason != "" {
			failReasons = append(failReasons, result.FailReason)
		}
		if !lossOK {
			failReasons = append(failReasons, fmt.Sprintf(
				"expected loss <= %.2f%%, got %.2f%%",
				tc.Correctness.ExpectedLossPct, lossPct))
		}
		if !overOK {
			extra := recvMetrics.LinesReceived - expectedOut
			failReasons = append(failReasons, fmt.Sprintf(
				"over-delivery: received %s lines but only %s were expected (%s extra/duplicate lines)",
				formatCount(recvMetrics.LinesReceived), formatCount(expectedOut), formatCount(extra)))
		}

		passed := lossOK && overOK && recvOK
		result.Passed = &passed
		if passed {
			result.FailReason = ""
		} else {
			result.FailReason = strings.Join(failReasons, "; ")
		}
	}

	// Optional load-balance fairness check (Feature E). Disabled cases
	// return Passed=true and the result has no LoadBalance key.
	if tc.Correctness.LoadBalance.Enabled() && len(perReceiver) > 0 {
		lb := applyLoadBalance(tc.Correctness.LoadBalance, perReceiver)
		result.LoadBalance = map[string]any{
			"min_share_ratio_observed": lb.MinShareRatioObserved,
			"min_share_ratio_required": lb.MinShareRatioRequired,
			"pass":                     lb.Passed,
			"per_receiver_counts":      lb.PerReceiverCounts,
		}
		if !lb.Passed {
			merged := false
			if result.Passed != nil && !*result.Passed {
				result.FailReason = result.FailReason + "; " + lb.FailureReason
				merged = true
			}
			if !merged {
				f := false
				result.Passed = &f
				result.FailReason = lb.FailureReason
			}
		}
	}

	// Optional rate-ceiling check (Feature D). Pulls per-record arrival
	// timestamps from each receiver and slides a window across them. The
	// timestamps endpoint is only populated when the case enables
	// rate_ceiling, so the overhead is opt-in.
	if tc.Correctness.RateCeiling.Enabled() {
		ports := orch.ReceiverMetricsPorts()
		// Merge timestamps from every receiver — multi-receiver fan-out
		// or LB cases see the rate ceiling on the combined stream.
		var all []int64
		for _, port := range ports {
			ts, terr := r.receiverTimestamps(port, 30*time.Second)
			if terr != nil {
				fmt.Fprintf(os.Stderr, "  warning: arrival_times unavailable on port %d: %v\n", port, terr)
				continue
			}
			all = append(all, ts...)
		}
		rw := applyRateCeiling(tc.Correctness.RateCeiling, all)
		result.RateWindow = map[string]any{
			"max_observed_eps":      rw.MaxObservedEPS,
			"overshoot_count":       rw.OvershootCount,
			"first_overshoot_ns":    rw.FirstOvershootStartNs,
			"pass":                  rw.Passed,
		}
		if !rw.Passed {
			merged := false
			if result.Passed != nil && !*result.Passed {
				result.FailReason = result.FailReason + "; " + rw.FailureReason
				merged = true
			}
			if !merged {
				f := false
				result.Passed = &f
				result.FailReason = rw.FailureReason
			}
		}
	}

	if r.opts.Drain > 0 {
		// Drain mode is for local diagnosis only — do not overwrite the
		// canonical results file the web UI consumes.
		fmt.Println("  done. (drain mode — result not persisted)")
	} else {
		dir, err := r.store.Save(result, metricsCSVSrc)
		if err != nil {
			return result, fmt.Errorf("saving results: %w", err)
		}
		fmt.Printf("  done. results → %s\n", dir)
	}
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
	if tc.Correctness.ValidateContent {
		fmt.Printf("  malformed lines: %s\n", formatCount(recvMetrics.MalformedLines))
	}
	if r.opts.CPULimit != "" || r.opts.MemLimit != "" {
		fmt.Printf("  subject limits: cpu=%s mem=%s\n",
			defaultVal(r.opts.CPULimit, "unlimited"), defaultVal(r.opts.MemLimit, "unlimited"))
	}
	recvWindow := 0.0
	if recvMetrics.FirstReceivedNs > 0 && recvMetrics.LastReceivedNs > recvMetrics.FirstReceivedNs {
		recvWindow = float64(recvMetrics.LastReceivedNs-recvMetrics.FirstReceivedNs) / 1e9
	}
	fmt.Printf("  system: %d CPUs, %d MB RAM  send: %.1fs  recv: %.1fs  active: %.1fs  total: %.1fs\n",
		sysCPUs, sysMemMB, sendDuration, recvWindow, rateDuration, elapsed)

	// Print the final, merged verdict — not the receiver-only verdict.
	// The block above ANDs the loss budget and over-delivery checks in;
	// printing recvMetrics.Passed here would silently disagree with what
	// got persisted to result.Passed.
	if result.Passed != nil {
		if *result.Passed {
			fmt.Println("  correctness: PASSED")
		} else {
			fmt.Println("  correctness: FAILED")
			if result.FailReason != "" {
				for _, e := range strings.Split(result.FailReason, "; ") {
					fmt.Printf("    - %s\n", e)
				}
			} else {
				for _, e := range recvMetrics.Errors {
					fmt.Printf("    - %s\n", e)
				}
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
	subject = r.applySubjectOverrides(subject)

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
			if stableRounds >= 12 {
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
	if tc.Correctness.ValidateContent && recvMetrics.MalformedLines > 0 {
		passed = false
		errors = append(errors, fmt.Sprintf("expected 0 malformed lines, got %s (memory corruption)",
			formatCount(recvMetrics.MalformedLines)))
	}
	if tc.Correctness.ValidateJSON && recvMetrics.InvalidJSONLines > 0 {
		passed = false
		errors = append(errors, fmt.Sprintf("expected 0 invalid-JSON lines, got %s",
			formatCount(recvMetrics.InvalidJSONLines)))
	}

	result := results.RunResult{
		TestName:        tc.Name,
		Config:          configName,
		Subject:         subject.Name,
		Version:         subject.Version,
		Hardware:        hardwareID(),
		Timestamp:       startTime,
		DurationSec:     elapsed,
		FirstSentNs:     genStats.FirstSentNs,
		LastSentNs:      genStats.LastSentNs,
		FirstReceivedNs: recvMetrics.FirstReceivedNs,
		LastReceivedNs:  recvMetrics.LastReceivedNs,
		LinesIn:         genStats.LinesSent,
		LinesOut:        recvMetrics.LinesReceived,
		BytesIn:         genStats.BytesSent,
		BytesOut:        recvMetrics.BytesReceived,
		LossPercent:     lossPct,
		Passed:          &passed,
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
		fmt.Printf("  unique lines: %s  duplicates: %s\n",
			formatCount(recvMetrics.UniqueLines), formatCount(recvMetrics.Duplicates))
	}
	if tc.Correctness.ValidateContent {
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

// runPersistenceShutdownCorrectness tests durable persistence across a subject
// shutdown. `crash=false` does a graceful SIGTERM (matches the original
// restart-correctness flow); `crash=true` SIGKILLs the subject mid-flight to
// verify recovery without any chance to flush state.
//
//  1. Start subject + collector (receiver DOWN)
//  2. Start generator — sends sequenced logs
//  3. Wait for generator to finish writing
//  4. Stop subject (SIGTERM 30s if !crash, SIGKILL if crash)
//  5. Start receiver
//  6. Restart subject — it should read from persistent store and forward
//  7. Drain and verify all logs arrive with 0% loss, 0 duplicates
func (r *Runner) runPersistenceShutdownCorrectness(tc *config.TestCase, subject config.Subject, crash bool) (results.RunResult, error) {
	configName := r.opts.ConfigName
	subject = r.applySubjectOverrides(subject)

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

	// PHASE 3: stop subject. SIGTERM (restart variant) lets the subject flush
	// state to disk gracefully; SIGKILL (crash variant) gives no chance for
	// cleanup — only writes already persisted before the kill are recoverable.
	if crash {
		fmt.Println("  phase 3: killing subject (SIGKILL — no graceful shutdown)…")
		if err := orch.KillServices("subject"); err != nil {
			return results.RunResult{}, fmt.Errorf("killing subject: %w", err)
		}
	} else {
		fmt.Println("  phase 3: stopping subject immediately (SIGTERM)…")
		if err := orch.StopServices(30*time.Second, "subject"); err != nil {
			return results.RunResult{}, fmt.Errorf("stopping subject: %w", err)
		}
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
			if stableRounds >= 12 {
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
	if tc.Correctness.ValidateContent && recvMetrics.MalformedLines > 0 {
		passed = false
		errors = append(errors, fmt.Sprintf("expected 0 malformed lines, got %s (memory corruption)",
			formatCount(recvMetrics.MalformedLines)))
	}
	if tc.Correctness.ValidateJSON && recvMetrics.InvalidJSONLines > 0 {
		passed = false
		errors = append(errors, fmt.Sprintf("expected 0 invalid-JSON lines, got %s",
			formatCount(recvMetrics.InvalidJSONLines)))
	}

	result := results.RunResult{
		TestName:        tc.Name,
		Config:          configName,
		Subject:         subject.Name,
		Version:         subject.Version,
		Hardware:        hardwareID(),
		Timestamp:       startTime,
		DurationSec:     elapsed,
		FirstSentNs:     genStats.FirstSentNs,
		LastSentNs:      genStats.LastSentNs,
		FirstReceivedNs: recvMetrics.FirstReceivedNs,
		LastReceivedNs:  recvMetrics.LastReceivedNs,
		LinesIn:         genStats.LinesSent,
		LinesOut:        recvMetrics.LinesReceived,
		BytesIn:         genStats.BytesSent,
		BytesOut:        recvMetrics.BytesReceived,
		LossPercent:     lossPct,
		Passed:          &passed,
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
		fmt.Printf("  unique lines: %s  duplicates: %s\n",
			formatCount(recvMetrics.UniqueLines), formatCount(recvMetrics.Duplicates))
	}
	if tc.Correctness.ValidateContent {
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

// runPersistenceFileRestartCorrectness verifies that a file-tail subject
// recovers correctly when it's offline across a file rotation.
//
//  1. Start subject + receiver + collector + generator together. Subject
//     tails the input file and forwards to the receiver in real time.
//  2. Generator does a `create`-mode rotation at FileRotation.At — rename
//     /data/input.log → /data/input.log.1 and create a fresh /data/input.log.
//  3. Shortly after the rotation fires, SIGTERM the subject (its file-tail
//     position and any pending forwards must persist to disk so a re-read
//     can resume in the right place).
//  4. Generator continues writing post-rotation events to the new
//     /data/input.log while the subject is offline.
//  5. Generator finishes.
//  6. Restart subject. To pass, it must:
//        - read the un-read tail of input.log.1 (events written between its
//          last forwarded byte and the rotation point)
//        - read the new input.log from offset 0 (post-rotation events)
//        - NOT re-forward anything already sent before SIGTERM
//
// This catches subjects whose file source watches a single path with no
// persistent state — they can't see input.log.1 after restart, so all
// un-read pre-rotation events are lost.
func (r *Runner) runPersistenceFileRestartCorrectness(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	subject = r.applySubjectOverrides(subject)

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

	// PHASE 1: start every service together — subject tails file, receiver
	// records, generator writes.
	fmt.Println("  phase 1: starting all services (subject tails + forwards in real time)…")
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting services: %w", err)
	}

	// PHASE 2: wait until just after the rotation event has fired.
	rotateAt := 30 * time.Second
	if s := tc.Generator.FileRotation.At; s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			rotateAt = d
		}
	}
	warmup := tc.WarmupOrDefault(10 * time.Second)
	stopAfter := warmup + rotateAt + 5*time.Second
	fmt.Printf("  phase 2: waiting %s (rotation fires at warmup+%s)…\n", stopAfter, rotateAt)
	time.Sleep(stopAfter)

	// PHASE 3: SIGTERM subject. Its file-tail state must be flushed to disk
	// (persist file, position file, sincedb, …) so the restart can resume.
	fmt.Println("  phase 3: stopping subject (SIGTERM, must persist file-tail position)…")
	if err := orch.StopServices(30*time.Second, "subject"); err != nil {
		return results.RunResult{}, fmt.Errorf("stopping subject: %w", err)
	}

	// PHASE 4: generator continues writing the rest of the test budget to
	// the new file while the subject is offline.
	duration := tc.DurationOrDefault(60 * time.Second)
	genTimeout := duration + warmup + 2*time.Minute
	if genTimeout > r.opts.Timeout {
		genTimeout = r.opts.Timeout
	}
	fmt.Printf("  phase 4: waiting for generator to finish writing (up to %s)…\n", genTimeout)
	if err := orch.WaitForGeneratorExit(genTimeout); err != nil {
		return results.RunResult{}, fmt.Errorf("waiting for generator: %w", err)
	}
	genStats := r.parseGeneratorStats(orch.GeneratorStdout())
	fmt.Printf("  generator sent %s lines\n", formatCount(genStats.LinesSent))

	// PHASE 5: restart subject — must catch up on un-read tail of input.log.1
	// AND the new input.log from offset 0.
	fmt.Println("  phase 5: restarting subject (must read .1 archive + new file)…")
	if err := orch.UpServices("subject"); err != nil {
		return results.RunResult{}, fmt.Errorf("restarting subject: %w", err)
	}

	// PHASE 6: drain wait.
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
	var recvMetrics ReceiverMetrics
	for time.Now().Before(drainDeadline) {
		time.Sleep(5 * time.Second)
		rm, err := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if err != nil {
			continue
		}
		recvMetrics = rm
		fmt.Printf("    received: %s / %s lines\n", formatCount(rm.LinesReceived), formatCount(genStats.LinesSent))
		if rm.LinesReceived == lastCount && rm.LinesReceived > 0 {
			stableRounds++
			if stableRounds >= 12 {
				fmt.Println("    receiver stable — all logs drained")
				break
			}
		} else {
			stableRounds = 0
		}
		lastCount = rm.LinesReceived
	}

	// Evaluate.
	elapsed := time.Since(startTime).Seconds()
	lossPct := 0.0
	if genStats.LinesSent > 0 {
		lossPct = 100.0 * (1.0 - float64(recvMetrics.LinesReceived)/float64(genStats.LinesSent))
		if lossPct < 0 {
			lossPct = 0
		}
	}
	passed := true
	var perrs []string
	if recvMetrics.LinesReceived < genStats.LinesSent {
		loss := genStats.LinesSent - recvMetrics.LinesReceived
		perrs = append(perrs, fmt.Sprintf("loss: %s lines (%.2f%%)", formatCount(loss), lossPct))
		passed = false
	}
	if recvMetrics.LinesReceived > genStats.LinesSent {
		extra := recvMetrics.LinesReceived - genStats.LinesSent
		perrs = append(perrs, fmt.Sprintf("over-delivery: received %s, sent %s (%s extra/duplicate)",
			formatCount(recvMetrics.LinesReceived), formatCount(genStats.LinesSent), formatCount(extra)))
		passed = false
	}
	if tc.Correctness.ValidateDedup && recvMetrics.Duplicates > 0 {
		perrs = append(perrs, fmt.Sprintf("expected 0 duplicates, got %s", formatCount(recvMetrics.Duplicates)))
		passed = false
	}

	result := results.RunResult{
		TestName:        tc.Name,
		Config:          configName,
		Subject:         subject.Name,
		Version:         subject.Version,
		Hardware:        hardwareID(),
		Timestamp:       startTime,
		DurationSec:     elapsed,
		FirstSentNs:     genStats.FirstSentNs,
		LastSentNs:      genStats.LastSentNs,
		FirstReceivedNs: recvMetrics.FirstReceivedNs,
		LastReceivedNs:  recvMetrics.LastReceivedNs,
		LinesIn:         genStats.LinesSent,
		LinesOut:        recvMetrics.LinesReceived,
		BytesIn:         genStats.BytesSent,
		BytesOut:        recvMetrics.BytesReceived,
		LossPercent:     lossPct,
		Passed:          &passed,
	}
	if !passed {
		result.FailReason = strings.Join(perrs, "; ")
	}

	dir, err := r.store.Save(result, "")
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}

	fmt.Printf("  done. results → %s\n", dir)
	fmt.Printf("  lines sent: %s  lines received: %s  loss: %.2f%%\n",
		formatCount(genStats.LinesSent), formatCount(recvMetrics.LinesReceived), lossPct)
	if tc.Correctness.ValidateDedup {
		fmt.Printf("  unique lines: %s  duplicates: %s\n",
			formatCount(recvMetrics.UniqueLines), formatCount(recvMetrics.Duplicates))
	}
	fmt.Printf("  total time: %.1fs\n", elapsed)

	if passed {
		fmt.Println("  file-rotation restart correctness: PASSED ✓")
	} else {
		fmt.Println("  file-rotation restart correctness: FAILED ✗")
		for _, e := range perrs {
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

func benchmarkWindow(gen GeneratorResult, recv ReceiverMetrics, sendDuration float64) (startNs, endNs int64, durationSec float64) {
	startNs = gen.FirstSentNs
	endNs = gen.LastSentNs
	durationSec = sendDuration

	if startNs > 0 && endNs <= startNs && sendDuration > 0 {
		endNs = startNs + int64(sendDuration*1e9)
	}
	if startNs == 0 && endNs > 0 && sendDuration > 0 {
		startNs = endNs - int64(sendDuration*1e9)
	}

	if startNs == 0 && recv.FirstReceivedNs > 0 {
		startNs = recv.FirstReceivedNs
	}
	if recv.LastReceivedNs > endNs {
		endNs = recv.LastReceivedNs
	}

	if startNs > 0 && endNs > startNs {
		durationSec = float64(endNs-startNs) / 1e9
		return startNs, endNs, durationSec
	}

	if recv.FirstReceivedNs > 0 && recv.LastReceivedNs > recv.FirstReceivedNs {
		recvDuration := float64(recv.LastReceivedNs-recv.FirstReceivedNs) / 1e9
		if recvDuration > durationSec {
			durationSec = recvDuration
		}
	}
	return startNs, endNs, durationSec
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
