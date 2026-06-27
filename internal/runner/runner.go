package runner

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
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
	"gopkg.in/yaml.v3"
)

// Options configure a test run.
type Options struct {
	CasesDir       string
	ResultsDir     string
	GeneratorImage string
	ReceiverImage  string
	CollectorImage string
	VerifierImage  string
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
	if o.VerifierImage == "" {
		o.VerifierImage = "vmetric/bench-verifier:latest"
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
	LinesReceived    int64    `json:"lines_received"`
	BytesReceived    int64    `json:"bytes_received"`
	Done             bool     `json:"done"`
	FirstReceivedNs  int64    `json:"first_received_ns"`
	LastReceivedNs   int64    `json:"last_received_ns"`
	Passed           *bool    `json:"passed,omitempty"`
	Errors           []string `json:"errors,omitempty"`
	UniqueLines      int64    `json:"unique_lines,omitempty"`
	Duplicates       int64    `json:"duplicates,omitempty"`
	MalformedLines   int64    `json:"malformed_lines,omitempty"`
	InvalidJSONLines int64    `json:"invalid_json_lines,omitempty"`
	LatencyP50Ms     float64  `json:"latency_p50_ms,omitempty"`
	LatencyP95Ms     float64  `json:"latency_p95_ms,omitempty"`
	LatencyP99Ms     float64  `json:"latency_p99_ms,omitempty"`
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
	if r.opts.SubjectVersion != "" {
		subject = subject.WithVersion(r.opts.SubjectVersion)
	}
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
	// Kafka crash/restart correctness reuses the shutdown flow: produce to
	// the broker while the receiver is down (the subject consumes from Kafka
	// and buffers to its crash-resistant queue), kill/stop the subject, bring
	// the receiver up, restart the subject, and verify every record is
	// delivered. Restart = SIGTERM (graceful), crash = SIGKILL.
	if tc.Type == "kafka_restart_correctness" {
		return r.runPersistenceShutdownCorrectness(tc, subject, false)
	}
	if tc.Type == "kafka_crash_correctness" {
		return r.runPersistenceShutdownCorrectness(tc, subject, true)
	}
	// Kafka in-flight crash: receiver stays UP and records flow to it; the
	// subject is SIGKILLed mid-delivery to exercise the at-least-once
	// over-delivery window (delivered-but-uncommitted records re-delivered on
	// restart). Verifies no loss; reports duplicates.
	if tc.Type == "kafka_inflight_crash_correctness" {
		return r.runKafkaInflightCrash(tc, subject)
	}
	// Kafka offset-commit restart: receiver stays UP, ALL records are
	// delivered cleanly, then the subject is restarted gracefully. A
	// consumer whose offset commits actually persist resumes from the
	// committed offsets and re-delivers (close to) nothing. Verifies no
	// loss AND bounded over-delivery — the inverse of the in-flight crash
	// case, which tolerates unlimited duplicates and is therefore blind to
	// a subject whose acknowledgments never reach the broker at all.
	if tc.Type == "kafka_offset_commit_restart" {
		return r.runKafkaOffsetCommitRestart(tc, subject)
	}
	// Kafka cert rotation: an mTLS broker whose server leaf is re-signed (same
	// CA) and reloaded mid-delivery; the subject's consumer must reconnect over
	// TLS and continue with no loss (over-delivery from the reconnect allowed).
	if tc.Type == "kafka_cert_rotation_correctness" {
		return r.runKafkaCertRotation(tc, subject)
	}
	// Syslog TLS Vault cert rotation: a syslog device whose TLS server cert is
	// sourced from HashiCorp Vault. Mid-run the cert is rotated to an untrusted
	// leaf (generator TLS must fail) then restored (director must recover).
	if tc.Type == "syslog_tls_vault_cert_rotation_correctness" {
		return r.runSyslogVaultCertRotation(tc, subject)
	}
	// Director↔agent TLS cert rotation: the director deploys an agent that
	// streams back over its proxy_tls listener; mid-run the director's serving
	// cert/CA is rotated on disk and the director is bounced so the enrolled
	// agent must re-handshake and reconnect. Subject-driven (no generator).
	if tc.IsDirectorAgentRotationType() {
		return r.runDirectorAgentCertRotation(tc, subject)
	}
	// Director↔agent ACL (allowed_ips) hot-reload rotation: an agent container
	// streams into the director; mid-run the harness rewrites the director's
	// mounted config (swapping acl.allowed_ips) and the director's own refreshACL
	// picks it up within acl.update_interval — NO restart. Verifies delivery
	// starts (recover) or stops (revoke) as the live allowlist changes.
	if tc.IsDirectorAgentACLRotationType() {
		return r.runDirectorAgentACLRotation(tc, subject)
	}
	if tc.IsDirectorClusterType() {
		return r.runDirectorClusterCorrectness(tc, subject)
	}
	if tc.IsFleetAutomationType() {
		return r.runFleetAutomationCorrectness(tc, subject)
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
		maps.Copy(extraEnv, cfg.Env)
	}

	// Capability guard: a case's `requires:` lists the capabilities every
	// subject must declare. Failing fast beats starting a run that silently
	// produces zero ingest.
	for _, capName := range tc.Requires {
		if !subject.HasCapability(capName) {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare capability %q required by case %q",
				subject.Name, capName, tc.Name)
		}
	}

	// TLS prep: when any generator opts into TLS, generate a self-signed
	// CA + leaf cert set into <tmpDir>/certs and pass the path down to
	// the orchestrator so it's bind-mounted into both the subject and the
	// generator container(s). Subjects that don't declare tls_tcp in
	// their Capabilities cause the case to fail fast (cleaner than
	// starting and silently producing zero ingest).
	tlsCertsHost := ""
	kafkaTLS := tc.Kafka != nil && tc.Kafka.UsesTLS()
	if tlsRequested(tc) || kafkaTLS {
		// The tls_tcp capability gates the TCP-listener TLS path only; Kafka
		// broker TLS is handled by the broker + client libraries and needs no
		// subject capability.
		if tlsRequested(tc) && !subject.HasCapability("tls_tcp") {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare TLS support (capability \"tls_tcp\")", subject.Name)
		}
		serverHosts := []string{"subject", "localhost"}
		if kafkaTLS {
			// The broker cert must be valid for the hostname clients dial.
			serverHosts = append(serverHosts, "redpanda")
		}
		certsDir := filepath.Join(tmpDir, "certs")
		path, err := orchestrator.GenerateTLSCerts(certsDir, serverHosts)
		if err != nil {
			return results.RunResult{}, fmt.Errorf("generating TLS certs: %w", err)
		}
		tlsCertsHost = path
	}

	// CaseDir must be absolute: the orchestrator turns it into a host bind
	// mount for sample_file replay, and Docker resolves relative bind paths
	// against the compose file's directory (the temp dir), not our cwd.
	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		CaseDir:          caseDir,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		VerifierImage:    r.opts.VerifierImage,
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
		"bench-localstack", "bench-azurite", "bench-azure-init",
	}
	// Supporting-service containers have fixed names too; a crashed prior run
	// belongs to a different compose project, so Down() won't remove them and
	// the new run would collide on the name.
	if tc.UsesKafka() {
		cleanupContainers = append(cleanupContainers, "bench-redpanda", "bench-redpanda-init")
	}
	if tc.UsesVault() {
		cleanupContainers = append(cleanupContainers, "bench-vault", "bench-vault-init")
	}
	// Plural-mode containers (bench-generator-<id>, bench-receiver-<id>)
	// need cleanup too, otherwise a re-run of the same case can collide.
	cleanupContainers = append(cleanupContainers, cr.GeneratorContainers()...)
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
	// Hard wall on total runtime. applyDefaults() guarantees Timeout > 0.
	// In generator mode WaitForGeneratorExit is already capped by Timeout;
	// the generator-less path and the drain loops below have no send phase
	// to bound them, so clamp their waits/deadlines to this so a run never
	// overruns Options.Timeout.
	runDeadline := startTime.Add(r.opts.Timeout)

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
	if tc.HasGenerator() {
		genTimeout := min(duration+warmup+2*time.Minute, r.opts.Timeout)

		fmt.Printf("  waiting for generator (up to %s)…\n", genTimeout)
		if err := orch.WaitForGeneratorExit(genTimeout); err != nil {
			if tc.Correctness.ExpectFailure {
				// A negative test EXPECTS the data path to fail — e.g. the
				// generator gets 401s because auth correctly rejects a wrong
				// credential — so a non-zero generator exit is not fatal here.
				// Proceed to the expect_failure verdict, which asserts that
				// (almost) nothing was delivered to the receiver.
				fmt.Printf("  (generator exited non-zero — expected for expect_failure: %v)\n", err)
			} else {
				return results.RunResult{}, fmt.Errorf("waiting for generator: %w", err)
			}
		}
	} else {
		// No generator: the subject drives data on its own (e.g. an agentless
		// deploy that collects from an endpoint and forwards to the receiver).
		// There's no send phase to wait on — give the subject a brief head start,
		// then fall through to the receiver-drain loop, which waits for data to
		// arrive and stabilize (bounded by correctness.drain_seconds).
		headStart := warmup
		if rem := time.Until(runDeadline); rem < headStart {
			headStart = rem
		}
		fmt.Printf("  no generator — letting the subject run (head start %s)…\n", headStart)
		if headStart > 0 {
			time.Sleep(headStart)
		}
	}

	// The verifier path replaces the receiver entirely: the subject's sink is
	// S3, nothing listens on TCP, and the s3 receiver's destructive drain would
	// corrupt the verifier's read. Verifier cases skip the receiver drain loop
	// and read the DuckDB verdict instead.
	var recvMetrics ReceiverMetrics
	var perReceiver []PerReceiverMetrics
	if tc.UsesVerifier() {
		recvMetrics, err = r.runVerifier(orch, tc, tmpDir, runDeadline)
		if err != nil {
			return results.RunResult{}, err
		}
	} else {
		metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
		if err != nil {
			return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
		}
		defer stopPortFwd()

		if tc.IsPerformanceType() && r.opts.Drain > 0 {
			// Diagnostic drain mode: poll the receiver until it goes idle (or the
			// configured ceiling fires). Same pattern as the correctness path
			// below, but bounded by --drain.
			fmt.Printf("  drain mode: waiting up to %s for receiver to go idle…\n", r.opts.Drain)
			const drainPoll = 5 * time.Second
			const quietPolls = 6 // 30s stable window
			ports := orch.ReceiverMetricsPorts()
			drainDeadline := time.Now().Add(r.opts.Drain)
			if drainDeadline.After(runDeadline) {
				drainDeadline = runDeadline
			}
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
		} else if tc.IsPerformanceType() {
			drainGrace := tc.DrainGraceOrDefault(5 * time.Second)
			if rem := time.Until(runDeadline); rem < drainGrace {
				drainGrace = rem
			}
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
				quietPolls = max(int(quietWindow/drainPoll), 1)
			}
			ports := orch.ReceiverMetricsPorts()
			drainDeadline := time.Now().Add(drainTimeout)
			if drainDeadline.After(runDeadline) {
				drainDeadline = runDeadline
			}
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
	expectedMul := max(tc.Correctness.ExpectedMultiplier, 1)
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
	if tc.IsPerformanceType() && !isBlackholeCase &&
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
	if tc.IsCorrectnessType() && tc.Correctness.ExpectFailure {
		// NEGATIVE test: the data path is supposed to be blocked (e.g. a client
		// with the wrong basic-auth password must be 401'd by a Vault-sourced
		// HTTP device). Pass iff the receiver saw at most the allowed number of
		// lines; if records got through, the control under test was bypassed.
		cap := tc.Correctness.ExpectFailureMaxReceived
		blocked := recvMetrics.LinesReceived <= cap
		result.Passed = &blocked
		if blocked {
			result.FailReason = ""
			fmt.Printf("  expect_failure: data path blocked as required — receiver saw %s line(s) (<= %s) ✓\n",
				formatCount(recvMetrics.LinesReceived), formatCount(cap))
		} else {
			result.FailReason = fmt.Sprintf(
				"expect_failure: data path was NOT blocked — receiver observed %s line(s) (> %s); "+
					"the control under test (e.g. auth) appears bypassed",
				formatCount(recvMetrics.LinesReceived), formatCount(cap))
		}
	} else if tc.IsCorrectnessType() && !tc.HasGenerator() {
		// No generator: there's no expected line count to derive loss or
		// over-delivery from, so those guards don't apply. Success = the subject
		// delivered at least MinReceived records to the receiver (default 1) and
		// the receiver didn't flag a content failure (JSON/dedup/etc.).
		minRecv := tc.Correctness.MinReceived
		if minRecv <= 0 {
			minRecv = 1
		}
		recvOK := result.Passed == nil || *result.Passed
		gotEnough := recvMetrics.LinesReceived >= minRecv
		var failReasons []string
		if result.FailReason != "" {
			failReasons = append(failReasons, result.FailReason)
		}
		if !gotEnough {
			failReasons = append(failReasons, fmt.Sprintf(
				"expected >= %s received records, got %s",
				formatCount(minRecv), formatCount(recvMetrics.LinesReceived)))
		}
		passed := gotEnough && recvOK
		result.Passed = &passed
		if passed {
			result.FailReason = ""
		} else {
			result.FailReason = strings.Join(failReasons, "; ")
		}
	} else if tc.IsCorrectnessType() && !tc.UsesVerifier() {
		// Verifier cases are excluded: the DuckDB verifier is the correctness
		// oracle and already encoded the verdict (loss, duplicates, NULLs, and
		// its own over-delivery policy) into recvMetrics.Passed/.Errors above.
		// Re-deriving pass/fail from line-count loss + a strict over-delivery
		// cap here would wrongly flip a valid allow_overdelivery verifier pass.
		lossOK := lossPct <= tc.Correctness.ExpectedLossPct
		// Kafka consumption is at-least-once: the consumer may re-deliver a
		// fetch batch on its initial group join / rebalance, so allow bounded
		// over-delivery (Correctness.MaxOverDeliveryPct, default 0 = exact).
		// Non-kafka correctness stays strict.
		overCap := expectedOut
		if tc.IsKafkaType() {
			overCap += int64(float64(expectedOut) * tc.Correctness.MaxOverDeliveryPct / 100.0)
		}
		overOK := recvMetrics.LinesReceived <= overCap
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
			"max_observed_eps":   rw.MaxObservedEPS,
			"overshoot_count":    rw.OvershootCount,
			"first_overshoot_ns": rw.FirstOvershootStartNs,
			"pass":               rw.Passed,
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
				for e := range strings.SplitSeq(result.FailReason, "; ") {
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
		maps.Copy(extraEnv, cfg.Env)
	}

	// CaseDir must be absolute: the orchestrator turns it into a host bind
	// mount for sample_file replay, and Docker resolves relative bind paths
	// against the compose file's directory (the temp dir), not our cwd.
	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		CaseDir:          caseDir,
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
	genTimeout := min(duration+warmup+2*time.Minute, r.opts.Timeout)

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
		extra := recvMetrics.LinesReceived - genStats.LinesSent
		if tc.IsKafkaType() || tc.Correctness.AllowOverDelivery {
			// At-least-once transports (Kafka, S3-via-SQS notifications, SQS,
			// Kinesis): crash/restart recovery may re-deliver records already
			// buffered. That is correct behavior — duplicates, never loss —
			// so over-delivery is informational here, not a failure.
			fmt.Printf("  note: over-delivery of %s lines (at-least-once duplicates from recovery — not a failure)\n", formatCount(extra))
		} else {
			passed = false
			errors = append(errors, fmt.Sprintf("over-delivery: received %s lines but only %s were sent (%s extra/duplicate lines)",
				formatCount(recvMetrics.LinesReceived), formatCount(genStats.LinesSent), formatCount(extra)))
		}
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
		maps.Copy(extraEnv, cfg.Env)
	}

	// CaseDir must be absolute: the orchestrator turns it into a host bind
	// mount for sample_file replay, and Docker resolves relative bind paths
	// against the compose file's directory (the temp dir), not our cwd.
	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		CaseDir:          caseDir,
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
	genTimeout := min(duration+warmup+2*time.Minute, r.opts.Timeout)

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
		extra := recvMetrics.LinesReceived - genStats.LinesSent
		if tc.IsKafkaType() || tc.Correctness.AllowOverDelivery {
			// At-least-once transports (Kafka, S3-via-SQS notifications, SQS,
			// Kinesis): crash/restart recovery may re-deliver records already
			// buffered. That is correct behavior — duplicates, never loss —
			// so over-delivery is informational here, not a failure.
			fmt.Printf("  note: over-delivery of %s lines (at-least-once duplicates from recovery — not a failure)\n", formatCount(extra))
		} else {
			passed = false
			errors = append(errors, fmt.Sprintf("over-delivery: received %s lines but only %s were sent (%s extra/duplicate lines)",
				formatCount(recvMetrics.LinesReceived), formatCount(genStats.LinesSent), formatCount(extra)))
		}
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

// midDeliveryFlow parameterizes the shared kafka correctness driver
// (runKafkaMidDeliveryAction): produce to the broker with the receiver live,
// fire one disruptive action once the receiver has seen ~half of total_lines,
// then drain and assert no loss (over-delivery from at-least-once recovery is
// reported, not failed). The action is the only thing that varies between the
// flows — an in-flight subject crash, a broker cert rotation, etc.
type midDeliveryFlow struct {
	// verdictLabel names the flow in the PASS/FAIL line, e.g.
	// "kafka cert rotation correctness".
	verdictLabel string
	// actionLog is printed when the mid-delivery action fires.
	actionLog string
	// overDelivNote explains why duplicates are expected, shown on the
	// over-delivery line.
	overDelivNote string
	// totalLinesErr is returned when generator.total_lines <= 0.
	totalLinesErr string
	// prepare runs after RunConfig is built but before the compose runner is
	// created, so it can add run state (e.g. generate TLS certs and set
	// rc.TLSCertsHost). May be nil.
	prepare func(tmpDir string, rc *orchestrator.RunConfig) error
	// extraCleanup lists fixed container names to force-remove pre-run beyond
	// the standard generator/receiver/collector/subject set.
	extraCleanup []string
	// action is the disruptive hook run once at mid-delivery. It owns any
	// settle sleeps it needs.
	action func(orch orchestrator.Orchestrator) error
}

// runKafkaMidDeliveryAction is the shared driver behind the kafka in-flight
// crash and cert-rotation flows: both bring everything up with the receiver
// live, wait until the receiver has seen half the records, fire one disruptive
// action, then drain and apply the same no-loss / at-least-once verdict. Only
// the action (and a little setup/labelling) differs — see midDeliveryFlow.
func (r *Runner) runKafkaMidDeliveryAction(tc *config.TestCase, subject config.Subject, f midDeliveryFlow) (results.RunResult, error) {
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

	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		maps.Copy(extraEnv, cfg.Env)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		CaseDir:          caseDir,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
	}

	// Per-flow setup (e.g. generate TLS certs and set rc.TLSCertsHost) before
	// the compose runner reads RunConfig.
	if f.prepare != nil {
		if err := f.prepare(tmpDir, &runCfg); err != nil {
			return results.RunResult{}, err
		}
	}

	orch, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}

	cleanup := []string{"bench-generator", "bench-receiver", "bench-collector", "bench-subject-" + subject.Name}
	cleanup = append(cleanup, f.extraCleanup...)
	for _, c := range cleanup {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()

	startTime := time.Now()
	defer func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}()

	n := tc.Generator.TotalLines
	if n <= 0 {
		return results.RunResult{}, errors.New(f.totalLinesErr)
	}
	mid := n / 2

	// Everything up, receiver INCLUDED — data will be flowing to the target.
	fmt.Println("  starting all services (receiver UP throughout)…")
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting services: %w", err)
	}

	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	// Wait until the receiver has seen ~half the records, then fire the
	// disruptive action. Driving off receiver progress (not a fixed sleep)
	// guarantees it lands mid-delivery regardless of run speed.
	fmt.Printf("  waiting for mid-delivery (receiver >= %s of %s)…\n", formatCount(mid), formatCount(n))
	fired := false
	deadline := time.Now().Add(r.opts.Timeout)
	for time.Now().Before(deadline) {
		rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr == nil {
			fmt.Printf("    received: %s\n", formatCount(rm.LinesReceived))
			if rm.LinesReceived >= mid {
				fmt.Printf("  mid-delivery reached — %s…\n", f.actionLog)
				if err := f.action(orch); err != nil {
					return results.RunResult{}, err
				}
				fired = true
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !fired {
		return results.RunResult{}, fmt.Errorf("never reached mid-delivery (%s) before timeout", formatCount(mid))
	}

	// The generator produces to Kafka independently of the subject; collect
	// its final count.
	duration := tc.DurationOrDefault(60 * time.Second)
	warmup := tc.WarmupOrDefault(30 * time.Second)
	genTimeout := min(duration+warmup+2*time.Minute, r.opts.Timeout)
	if err := orch.WaitForGeneratorExit(genTimeout); err != nil {
		fmt.Printf("  (generator wait: %v)\n", err)
	}
	genStats := r.parseGeneratorStats(orch.GeneratorStdout())
	fmt.Printf("  generator sent %s lines\n", formatCount(genStats.LinesSent))

	// Drain until the receiver count stabilizes.
	drainTimeout := 3 * time.Minute
	fmt.Printf("  draining (up to %s)…\n", drainTimeout)
	var lastCount int64
	stableRounds := 0
	drainDeadline := time.Now().Add(drainTimeout)
	for time.Now().Before(drainDeadline) {
		time.Sleep(5 * time.Second)
		rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			continue
		}
		fmt.Printf("    received: %s / %s\n", formatCount(rm.LinesReceived), formatCount(genStats.LinesSent))
		if rm.LinesReceived == lastCount && rm.LinesReceived > 0 {
			stableRounds++
			if stableRounds >= 6 {
				fmt.Println("    receiver stable — drained")
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
	var errs []string
	if !passed {
		errs = append(errs, fmt.Sprintf("expected loss <= %.2f%%, got %.2f%% (%s of %s lines lost)",
			tc.Correctness.ExpectedLossPct, lossPct,
			formatCount(genStats.LinesSent-recvMetrics.LinesReceived), formatCount(genStats.LinesSent)))
	}
	if recvMetrics.LinesReceived > genStats.LinesSent {
		extra := recvMetrics.LinesReceived - genStats.LinesSent
		overPct := 100.0 * float64(extra) / float64(genStats.LinesSent)
		fmt.Printf("  over-delivery: %s duplicate lines (%.2f%%) — at-least-once, %s\n",
			formatCount(extra), overPct, f.overDelivNote)
	}

	fmt.Printf("  lines sent: %s  lines received: %s  loss: %.2f%%\n",
		formatCount(genStats.LinesSent), formatCount(recvMetrics.LinesReceived), lossPct)
	if passed {
		fmt.Printf("  %s: PASSED ✓\n", f.verdictLabel)
	} else {
		fmt.Printf("  %s: FAILED ✗\n", f.verdictLabel)
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
		result.FailReason = strings.Join(errs, "; ")
	}

	// Persist the result like every other run path — Run's contract is to
	// return the *persisted* result.
	dir, err := r.store.Save(result, "")
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}
	fmt.Printf("  done. results → %s\n", dir)

	return result, nil
}

// rotateAndReload performs a generic in-flight rotation: mutate a mounted
// artifact (rotate) then reload the service by bouncing it, so a live consumer
// must reconnect. The broker server-cert case is the one instance today;
// client-cert / credential / config rotations are new rotate closures +
// service names, with no driver changes.
func rotateAndReload(orch orchestrator.Orchestrator, service string, rotate func() error) error {
	if err := rotate(); err != nil {
		return fmt.Errorf("rotate %s artifact: %w", service, err)
	}
	if err := orch.StopServices(10*time.Second, service); err != nil {
		return fmt.Errorf("stopping %s: %w", service, err)
	}
	if err := orch.UpServices(service); err != nil {
		return fmt.Errorf("restarting %s: %w", service, err)
	}
	return nil
}

// runKafkaInflightCrash crashes the subject WHILE it is actively delivering
// consumed Kafka records to the receiver (receiver stays UP), exercising the
// at-least-once over-delivery window: records delivered but not yet
// offset-committed are re-consumed on restart. Verdict: no loss; duplicates are
// reported, not failed.
func (r *Runner) runKafkaInflightCrash(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	return r.runKafkaMidDeliveryAction(tc, subject, midDeliveryFlow{
		verdictLabel:  "kafka in-flight crash correctness",
		actionLog:     "SIGKILL subject (no graceful shutdown), then restart",
		overDelivNote: "expected for a mid-delivery crash",
		totalLinesErr: "kafka_inflight_crash requires generator.total_lines > 0",
		action: func(orch orchestrator.Orchestrator) error {
			if err := orch.KillServices("subject"); err != nil {
				return fmt.Errorf("killing subject: %w", err)
			}
			// Settle before the consumer rejoins and replays uncommitted offsets.
			time.Sleep(3 * time.Second)
			if err := orch.UpServices("subject"); err != nil {
				return fmt.Errorf("restarting subject: %w", err)
			}
			return nil
		},
	})
}

// runKafkaCertRotation verifies the subject's broker-cert handling over mTLS in
// TWO halves, so the run fails if EITHER property breaks:
//
//  1. VALIDATION (negative half): mid-delivery the broker leaf is re-signed
//     under a brand-new UNTRUSTED CA and the broker is bounced. A subject that
//     actually verifies the broker cert MUST reject it — we assert a TLS
//     verify error (x509: certificate signed by unknown authority) appears in
//     the subject's log. A subject that skipped verification would accept the
//     bad leaf and keep delivering (no error), which fails the run loudly.
//  2. RECOVERY (positive half): the broker leaf is then re-signed under the
//     ORIGINAL trusted CA and bounced again; the consumer must reconnect over
//     TLS and the run must finish with no loss (the shared driver's verdict).
//
// This replaces the old single same-CA rotation, which only proved reconnect
// and would have passed even with broker-cert validation disabled.
func (r *Runner) runKafkaCertRotation(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	// certsDir is set by prepare and read by action; both close over it here,
	// and prepare runs before action, so the capture is well-ordered.
	var certsDir string
	hosts := []string{"subject", "localhost", "redpanda"}
	return r.runKafkaMidDeliveryAction(tc, subject, midDeliveryFlow{
		verdictLabel:  "kafka cert rotation correctness",
		actionLog:     "rotating broker cert to an UNTRUSTED CA (must be rejected), then back to a trusted cert",
		overDelivNote: "expected across the broker reconnects",
		totalLinesErr: "kafka_cert_rotation_correctness requires generator.total_lines > 0",
		extraCleanup:  []string{"bench-redpanda", "bench-redpanda-init"},
		prepare: func(tmpDir string, rc *orchestrator.RunConfig) error {
			// mTLS broker: generate the CA + server/client leaves the broker and
			// clients mount at /certs. certsDir is kept so the server leaf can be
			// re-signed mid-run (under a wrong CA, then the real CA).
			certsDir = filepath.Join(tmpDir, "certs")
			if _, err := orchestrator.GenerateTLSCerts(certsDir, hosts); err != nil {
				return fmt.Errorf("generating TLS certs: %w", err)
			}
			rc.TLSCertsHost = certsDir
			return nil
		},
		action: func(orch orchestrator.Orchestrator) error {
			subj := orch.SubjectContainer()

			// ---- Phase 1: UNTRUSTED rotation — the subject MUST reject it. ----
			_, before := subjectLogStats(subj)
			if err := rotateAndReload(orch, "redpanda", func() error {
				return orchestrator.RotateServerCertWrongCA(certsDir, hosts)
			}); err != nil {
				return err
			}
			// Give the broker time to come back and the consumer time to
			// bounce-detect and retry against the untrusted leaf (each attempt
			// should fail certificate verification).
			time.Sleep(25 * time.Second)

			lines, after := subjectLogStats(subj)
			if lines == 0 {
				return fmt.Errorf("subject produced no console logs, cannot verify cert rejection — " +
					"the cert-rotation case config must set debug.console.status: true")
			}
			if after <= before {
				return fmt.Errorf("SECURITY: subject did NOT reject the untrusted broker cert "+
					"(no new TLS verify error after wrong-CA rotation; before=%d after=%d) — "+
					"broker-cert validation appears disabled", before, after)
			}
			fmt.Printf("  untrusted broker cert REJECTED by subject (%d new TLS verify error(s)) ✓\n", after-before)

			// ---- Phase 2: TRUSTED rotation — recovery (no-loss verdict follows). ----
			if err := rotateAndReload(orch, "redpanda", func() error {
				return orchestrator.RotateServerCert(certsDir, hosts)
			}); err != nil {
				return err
			}
			time.Sleep(5 * time.Second)
			fmt.Println("  broker cert restored under the trusted CA — expecting delivery to resume")
			return nil
		},
	})
}

// subjectHasAgentPackage reports whether the subject (director) container ships
// the baked vmetric-agent that the push-deploy pushes to endpoints. The
// agent-capable image (vmetric/director-enterprise) ships
// /opt/vmetric/package/agent; the default vmetric/director image has no package
// dir at all, so a director↔agent case run on it can never deploy an agent.
//
// conclusive is false when the probe could not run (empty container, exec not
// ready, neither marker printed) so callers do NOT fast-fail on noise — they
// fall through to the timed wait. Only (present=false, conclusive=true) — the
// image demonstrably lacks the agent — is a safe fast-fail signal.
func subjectHasAgentPackage(container string) (present, conclusive bool) {
	if container == "" {
		return false, false
	}
	// Bound the probe: a stalled `docker exec` must not hang the whole run. A
	// timeout (or any error) leaves the result inconclusive so the caller falls
	// through to the timed delivery wait rather than failing on a flaky probe.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, _ := exec.CommandContext(ctx, "docker", "exec", container, "sh", "-c",
		"test -d /opt/vmetric/package/agent && echo PRESENT || echo ABSENT").CombinedOutput()
	s := string(out)
	switch {
	case strings.Contains(s, "PRESENT"):
		return true, true
	case strings.Contains(s, "ABSENT"):
		return false, true
	default:
		return false, false
	}
}

// subjectLogStats returns (total non-empty log lines, cert-verification-error
// lines) from the subject container's console log. The cert-error count is the
// signature of a consumer rejecting an untrusted broker leaf; the total-line
// count lets the caller distinguish "no rejection" from "subject isn't logging
// to console at all" (debug.console.status off).
func subjectLogStats(container string) (int, int) {
	if container == "" {
		return 0, 0
	}
	out, _ := exec.Command("docker", "logs", container).CombinedOutput()
	total, certErrs := 0, 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		total++
		l := strings.ToLower(line)
		if strings.Contains(l, "x509") ||
			strings.Contains(l, "unknown authority") ||
			strings.Contains(l, "failed to verify") ||
			strings.Contains(l, "bad certificate") ||
			strings.Contains(l, "certificate is not trusted") {
			certErrs++
		}
	}
	return total, certErrs
}

// runDirectorAgentCertRotation drives the director↔agent TLS cert-rotation
// correctness flow. Unlike the kafka/syslog rotation cases (a generator feeds
// the subject), this case is SUBJECT-DRIVEN: the director SSH-deploys an agent
// onto an endpoint, the agent streams collected logs back over the director's
// proxy_tls listener, and the receiver counts what arrives. There is no
// generator, so the verdict rests on min_received plus proof that delivery
// RESUMED after the mid-run rotation (a live reconnect, not just replay of
// pre-rotation records).
//
// Disruption: the director's serving cert is a file path under the subject's
// CertDir, bind-mounted from a host dir the harness owns. Mid-run the harness
// rotates those files and bounces the director (StopServices→UpServices). A
// live wss/NATS session does not re-handshake on its own, so the bounce is the
// reconnect trigger; the already-running agent (the director re-attaches to it
// on restart — it does not re-deploy) reconnects against the rotated cert.
//
// Modes (rotation.mode):
//   - same_ca:        leaf re-signed under the same CA — transparent reconnect.
//   - new_ca_recover: CA rolled over and re-served at /dl/cert.pem — a bootstrap
//     agent must re-fetch the new CA and reconnect.
//   - new_ca_reject:  two-phase. Phase 1 rotates to an UNTRUSTED, unserved leaf;
//     the agent MUST fail validation, so delivery STALLS (a missing stall is a
//     SECURITY failure — the agent accepted an untrusted cert). Phase 2 restores
//     a trusted leaf and delivery must resume.
func (r *Runner) runDirectorAgentCertRotation(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	subject = r.applySubjectOverrides(subject)

	// This specialized handler is dispatched before the generic capability guard
	// in Run, so honor the case's `requires:` here too — otherwise a rotation case
	// declaring a capability could start against a subject that lacks it.
	for _, capName := range tc.Requires {
		if !subject.HasCapability(capName) {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare capability %q required by case %q",
				subject.Name, capName, tc.Name)
		}
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
	// World-writable so container UIDs can write the mounted cert/compose files,
	// plus the sticky bit so a co-tenant on a multi-user host can't unlink or
	// replace this run's generated certs/compose.
	if err := os.Chmod(tmpDir, 0o1777); err != nil {
		return results.RunResult{}, fmt.Errorf("chmod tmpdir: %w", err)
	}
	defer func() {
		if !r.opts.NoCleanup {
			os.RemoveAll(tmpDir)
		}
	}()

	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		maps.Copy(extraEnv, cfg.Env)
	}

	// The director's proxy_tls leaf is served to the agent on the "subject"
	// alias; bake that (and localhost) into the SAN set. certsDir is rotated
	// in place mid-run and reflected into the subject via the CertDir bind mount.
	certsDir := filepath.Join(tmpDir, "certs")
	hosts := []string{"subject", "localhost"}
	if _, err := orchestrator.GenerateTLSCerts(certsDir, hosts); err != nil {
		return results.RunResult{}, fmt.Errorf("generating TLS certs: %w", err)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		CaseDir:          caseDir,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
		TLSCertsHost:     certsDir,
	}

	orch, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}

	cleanup := []string{"bench-receiver", "bench-collector", "bench-subject-" + subject.Name}
	for _, e := range tc.Endpoints {
		cleanup = append(cleanup, "bench-"+e.Name)
	}
	for _, c := range cleanup {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()

	startTime := time.Now()
	// All rotation waits (settle, stall, drain) are clamped to the overall run
	// deadline so a run can't keep sleeping/polling past Options.Timeout — the
	// same bound the persistence drivers apply via runDeadline.
	runDeadline := startTime.Add(r.opts.Timeout)
	sleepWithinDeadline := func(d time.Duration) error {
		rem := time.Until(runDeadline)
		if rem <= 0 {
			return fmt.Errorf("run timeout (%s) exceeded before the rotation wait completed", r.opts.Timeout)
		}
		if d > rem {
			return fmt.Errorf("run timeout (%s) exceeded during the rotation wait", r.opts.Timeout)
		}
		time.Sleep(d)
		return nil
	}
	defer func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}()

	minRecv := tc.Correctness.MinReceived
	if minRecv <= 0 {
		minRecv = 1
	}

	fmt.Println("  starting all services (director deploys the agent; receiver UP throughout)…")
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting services: %w", err)
	}

	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	// Pre-flight: these cases need the agent-capable image. The default
	// vmetric/director image ships no baked vmetric-agent (no
	// /opt/vmetric/package/agent), so the director can never push/run an agent
	// — the run would otherwise sit at 0 records until the initial-wait timeout
	// and fail with a misleading "deploy/enroll/connect failed". Detect the wrong
	// image up front and fail immediately with the actual cause. Inconclusive
	// probes (container not ready) fall through to the timed wait below.
	if present, conclusive := subjectHasAgentPackage(orch.SubjectContainer()); conclusive && !present {
		return results.RunResult{}, fmt.Errorf(
			"subject image %s:%s has no baked vmetric-agent (/opt/vmetric/package/agent missing): "+
				"director↔agent cert-rotation cases require the agent-capable image — "+
				"re-run with VMETRIC_IMAGE=vmetric/director-enterprise",
			subject.Image, subject.Version,
		)
	}

	// Phase 0 — wait for the initial deploy→enroll→stream chain to deliver at
	// least min_received records, proving the agent connected before we disturb
	// it. Budget off warmup + a deploy allowance, capped by the overall timeout.
	warmup := tc.WarmupOrDefault(30 * time.Second)
	initialBudget := min(warmup+3*time.Minute, time.Until(runDeadline))
	if initialBudget <= 0 {
		return results.RunResult{}, fmt.Errorf("run timeout (%s) exceeded before the initial delivery wait", r.opts.Timeout)
	}
	fmt.Printf("  waiting for initial delivery (receiver >= %s, up to %s)…\n", formatCount(minRecv), initialBudget)
	var countAtRotation int64
	initialDeadline := time.Now().Add(initialBudget)
	established := false
	for time.Now().Before(initialDeadline) {
		rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr == nil {
			fmt.Printf("    received: %s\n", formatCount(rm.LinesReceived))
			if rm.LinesReceived >= minRecv {
				countAtRotation = rm.LinesReceived
				established = true
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	if !established {
		return results.RunResult{}, fmt.Errorf(
			"agent never delivered the initial %s records within %s (subject image %s:%s) — "+
				"deploy/enroll/connect failed; if this is not the agent-capable image, "+
				"re-run with VMETRIC_IMAGE=vmetric/director-enterprise",
			formatCount(minRecv), initialBudget, subject.Image, subject.Version,
		)
	}
	fmt.Printf("  agent established — %s records before rotation ✓\n", formatCount(countAtRotation))

	// Phase 1 — rotate the director cert per mode and bounce the director so the
	// agent must re-handshake. resumeBaseline is the receiver count captured
	// immediately before the disruption (and, for reject, AFTER the stall) so the
	// "delivery resumed" verdict reflects only genuine post-disruption reconnects —
	// not pre-rotation arrivals still draining, nor the tolerated stall leak.
	settle := time.Duration(tc.Rotation.SettleSecondsOrDefault()) * time.Second
	var resumeBaseline int64
	var stallFailure string
	switch tc.Rotation.Mode {
	case config.RotationSameCA:
		rmBefore, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			return results.RunResult{}, fmt.Errorf("sampling receiver before same-CA rotation: %w", qerr)
		}
		resumeBaseline = rmBefore.LinesReceived
		fmt.Println("  rotating director leaf under the SAME CA, then bouncing the director…")
		if err := rotateAndReload(orch, "subject", func() error {
			return orchestrator.RotateServerCert(certsDir, hosts)
		}); err != nil {
			return results.RunResult{}, err
		}
		if err := sleepWithinDeadline(settle); err != nil {
			return results.RunResult{}, err
		}

	case config.RotationNewCARecover:
		rmBefore, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			return results.RunResult{}, fmt.Errorf("sampling receiver before CA rollover: %w", qerr)
		}
		resumeBaseline = rmBefore.LinesReceived
		fmt.Println("  rolling the director CA over (re-served at /dl/cert.pem), then bouncing the director…")
		if err := rotateAndReload(orch, "subject", func() error {
			return orchestrator.RotateServerCertNewCA(certsDir, hosts)
		}); err != nil {
			return results.RunResult{}, err
		}
		if err := sleepWithinDeadline(settle); err != nil {
			return results.RunResult{}, err
		}

	case config.RotationNewCAReject:
		// Phase 1a (negative/security): untrusted, unserved leaf — the agent MUST
		// reject it, so delivery stalls. Sample the count, rotate+bounce, wait the
		// stall window, then require the count did NOT advance.
		rmBefore, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			return results.RunResult{}, fmt.Errorf("sampling receiver before untrusted rotation: %w", qerr)
		}
		stall := time.Duration(tc.Rotation.StallSecondsOrDefault()) * time.Second
		fmt.Printf("  rotating director leaf to an UNTRUSTED CA (must be rejected), bouncing, then holding %s…\n", stall)
		if err := rotateAndReload(orch, "subject", func() error {
			return orchestrator.RotateServerCertWrongCA(certsDir, hosts)
		}); err != nil {
			return results.RunResult{}, err
		}
		if err := sleepWithinDeadline(stall); err != nil {
			return results.RunResult{}, err
		}
		rmStall, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			return results.RunResult{}, fmt.Errorf("sampling receiver during untrusted window: %w", qerr)
		}
		// A handful of in-flight records may land right as the session drops;
		// tolerate a tiny leak but fail loudly if delivery clearly continued
		// (which would mean the agent accepted the untrusted cert).
		const stallLeak = 3
		advanced := rmStall.LinesReceived - rmBefore.LinesReceived
		if advanced > stallLeak {
			// The subject kept delivering under an untrusted cert — a genuine
			// SECURITY failure of the subject, not a harness error. Record it as a
			// verdict (folded into `passed`/`errs` below) and let the flow continue
			// through Phase 1b/2 so the failed RunResult is still persisted.
			stallFailure = fmt.Sprintf(
				"SECURITY: delivery did NOT stall after rotating to an untrusted director cert "+
					"(%s new records during the %s window; before=%s after=%s) — the agent appears to accept untrusted certs",
				formatCount(advanced), stall, formatCount(rmBefore.LinesReceived), formatCount(rmStall.LinesReceived),
			)
			fmt.Printf("  %s\n", stallFailure)
		} else {
			fmt.Printf("  delivery STALLED under the untrusted cert (%s new records) ✓\n", formatCount(advanced))
		}
		// Resume must be measured against the stalled level, not the pre-rotation
		// count — the ≤stallLeak in-flight records are not a reconnect.
		resumeBaseline = rmStall.LinesReceived

		// Phase 1b (recovery): restore a trusted leaf under the original CA and
		// bounce again; delivery must resume.
		fmt.Println("  restoring a trusted director leaf, then bouncing the director…")
		if err := rotateAndReload(orch, "subject", func() error {
			return orchestrator.RotateServerCert(certsDir, hosts)
		}); err != nil {
			return results.RunResult{}, err
		}
		if err := sleepWithinDeadline(settle); err != nil {
			return results.RunResult{}, err
		}

	default:
		return results.RunResult{}, fmt.Errorf("unknown rotation.mode %q", tc.Rotation.Mode)
	}

	// Phase 2 — wait for the post-rotation RESUME, then drain to stable. The
	// agent's reconnect is not instant: it must detect the dropped session and
	// (for a CA rollover) re-fetch /dl/cert.pem with jitter + backoff, so the
	// count legitimately sits flat at resumeBaseline for a while. Crucially the
	// stable-exit is gated on resume having been observed (count > resumeBaseline)
	// — otherwise that recovery gap looks "stable" and the drain bails before the
	// agent ever reconnects (a false failure). Until resume, we keep polling for
	// the full window; once resumed, six unchanged rounds means drained.
	drainTimeout := 3 * time.Minute
	if tc.Correctness.DrainSeconds > 0 {
		drainTimeout = time.Duration(tc.Correctness.DrainSeconds) * time.Second
	}
	fmt.Printf("  waiting for resume then draining (up to %s)…\n", drainTimeout)
	var lastCount int64
	stableRounds := 0
	resumedObserved := false
	// Clamp the drain to the overall run deadline so it can't poll past Options.Timeout.
	drainDeadline := time.Now().Add(drainTimeout)
	if drainDeadline.After(runDeadline) {
		drainDeadline = runDeadline
	}
	for time.Now().Before(drainDeadline) {
		time.Sleep(5 * time.Second)
		rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			continue
		}
		fmt.Printf("    received: %s\n", formatCount(rm.LinesReceived))
		if rm.LinesReceived > resumeBaseline && !resumedObserved {
			resumedObserved = true
			fmt.Println("    delivery resumed after rotation ✓")
		}
		if resumedObserved && rm.LinesReceived == lastCount && rm.LinesReceived > 0 {
			stableRounds++
			if stableRounds >= 6 {
				fmt.Println("    receiver stable — drained")
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

	// Verdict: enough records overall, delivery resumed past the pre-rotation
	// baseline (a live reconnect, not just drained backlog), AND the receiver
	// raised no content failure. The reject case additionally required the stall,
	// already enforced inline above.
	gotEnough := recvMetrics.LinesReceived >= minRecv
	resumed := recvMetrics.LinesReceived > resumeBaseline
	recvOK := recvMetrics.Passed == nil || *recvMetrics.Passed
	passed := gotEnough && resumed && recvOK && stallFailure == ""
	var errs []string
	if stallFailure != "" {
		errs = append(errs, stallFailure)
	}
	if !recvOK {
		if len(recvMetrics.Errors) > 0 {
			errs = append(errs, recvMetrics.Errors...)
		} else {
			errs = append(errs, "receiver flagged a content failure")
		}
	}
	if !gotEnough {
		errs = append(errs, fmt.Sprintf("expected >= %s received records, got %s",
			formatCount(minRecv), formatCount(recvMetrics.LinesReceived)))
	}
	if !resumed {
		errs = append(errs, fmt.Sprintf(
			"delivery did not resume after rotation — %s records pre-rotation, %s at drain (agent never reconnected)",
			formatCount(resumeBaseline), formatCount(recvMetrics.LinesReceived),
		))
	}

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("  pre-rotation baseline: %s  final: %s\n", formatCount(resumeBaseline), formatCount(recvMetrics.LinesReceived))
	if passed {
		fmt.Printf("  director↔agent cert rotation (%s): PASSED ✓\n", tc.Rotation.Mode)
	} else {
		fmt.Printf("  director↔agent cert rotation (%s): FAILED ✗\n", tc.Rotation.Mode)
	}

	result := results.RunResult{
		TestName:        tc.Name,
		Config:          configName,
		Subject:         subject.Name,
		Version:         subject.Version,
		Hardware:        hardwareID(),
		Timestamp:       startTime,
		DurationSec:     elapsed,
		FirstReceivedNs: recvMetrics.FirstReceivedNs,
		LastReceivedNs:  recvMetrics.LastReceivedNs,
		LinesOut:        recvMetrics.LinesReceived,
		BytesOut:        recvMetrics.BytesReceived,
		Passed:          &passed,
	}
	if !passed {
		result.FailReason = strings.Join(errs, "; ")
	}

	dir, err := r.store.Save(result, "")
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}
	fmt.Printf("  done. results → %s\n", dir)

	return result, nil
}

// runDirectorAgentACLRotation drives the director↔agent ACL (allowed_ips)
// hot-reload rotation case. Unlike the cert-rotation driver it does NOT bounce
// the director: it rewrites the director's mounted config in place mid-run and
// relies on the director's refreshACL ticker (re-reads the config every
// acl.update_interval seconds) to apply the new allowlist live. The agent runs
// in a separate container (the `agent:` block), so its bench-network source IP
// — not a director-local address — is what the ACL admits or rejects.
//
//	expect: recover — start BLOCKED (initial allowed_ips excludes the agent → 0
//	                  records); confirm the block, rotate to an allowlist that
//	                  admits the agent, then require delivery to START.
//	expect: revoke  — start ALLOWED (records flow); rotate to an allowlist that
//	                  excludes the agent and require delivery to STOP (the next
//	                  /agent/vmf POST after the refresh is rejected).
func (r *Runner) runDirectorAgentACLRotation(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	subject = r.applySubjectOverrides(subject)

	for _, capName := range tc.Requires {
		if !subject.HasCapability(capName) {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare capability %q required by case %q",
				subject.Name, capName, tc.Name)
		}
	}

	fmt.Printf("→ test=%s  subject=%s  version=%s  config=%s\n",
		tc.Name, subject.Name, subject.Version, configName)

	// Initial (pre-rotation) config and the rotated config we swap in mid-run.
	// The driver edits ONLY acl.allowed_ips in this single config mid-run.
	initialSrc, err := tc.ConfigFilePath(r.opts.CasesDir, configName, subject)
	if err != nil {
		return results.RunResult{}, err
	}
	initialSrc, err = filepath.Abs(initialSrc)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving config path: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "bench-"+tc.Name+"-")
	if err != nil {
		return results.RunResult{}, err
	}
	if err := os.Chmod(tmpDir, 0o1777); err != nil {
		return results.RunResult{}, fmt.Errorf("chmod tmpdir: %w", err)
	}
	defer func() {
		if !r.opts.NoCleanup {
			os.RemoveAll(tmpDir)
		}
	}()

	// Own the mounted config file: copy the initial config into tmpDir and point
	// the orchestrator at THAT, so the live mounted file is under our control and
	// we can rewrite it in place mid-run. (A non-templated config would otherwise
	// be bind-mounted straight from the read-only case source.)
	mountedSrc := filepath.Join(tmpDir, subject.ConfigFile())
	initialBytes, err := os.ReadFile(initialSrc)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("reading initial config %s: %w", initialSrc, err)
	}
	if err := os.WriteFile(mountedSrc, initialBytes, 0o644); err != nil {
		return results.RunResult{}, fmt.Errorf("staging initial config: %w", err)
	}

	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		maps.Copy(extraEnv, cfg.Env)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    mountedSrc,
		CaseDir:          caseDir,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
	}

	orch, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}

	cleanup := []string{"bench-receiver", "bench-collector", "bench-subject-" + subject.Name, "bench-agent"}
	for _, c := range cleanup {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()

	startTime := time.Now()
	runDeadline := startTime.Add(r.opts.Timeout)
	sleepWithinDeadline := func(d time.Duration) error {
		rem := time.Until(runDeadline)
		if rem <= 0 {
			return fmt.Errorf("run timeout (%s) exceeded before the rotation wait completed", r.opts.Timeout)
		}
		if d > rem {
			return fmt.Errorf("run timeout (%s) exceeded during the rotation wait", r.opts.Timeout)
		}
		time.Sleep(d)
		return nil
	}
	defer func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}()

	minRecv := tc.Correctness.MinReceived
	if minRecv <= 0 {
		minRecv = 1
	}

	fmt.Println("  starting all services (agent dials into the director; receiver UP throughout)…")
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting services: %w", err)
	}

	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	if present, conclusive := subjectHasAgentPackage(orch.SubjectContainer()); conclusive && !present {
		return results.RunResult{}, fmt.Errorf(
			"subject image %s:%s has no baked vmetric-agent (/opt/vmetric/package/agent missing): "+
				"director↔agent ACL-rotation cases require the agent-capable image — "+
				"re-run with VMETRIC_IMAGE=vmetric/director-enterprise",
			subject.Image, subject.Version,
		)
	}

	// patchAllowedIPs reads the live mounted config, overwrites acl.allowed_ips
	// (and only that key) at every `acl:` mapping, and rewrites the file in place.
	// In-place truncate+write keeps the bind-mounted inode (a rename would orphan
	// the mount) and advances the mtime — exactly what the director's refreshACL
	// change-detection keys on. Editing only allowed_ips means the rotation cannot
	// perturb anything else in the config, so the verdict is never confounded by
	// an unrelated diff.
	patchAllowedIPs := func(ips []string) error {
		raw, err := os.ReadFile(mountedSrc)
		if err != nil {
			return err
		}
		var root map[string]any
		if err := yaml.Unmarshal(raw, &root); err != nil {
			return fmt.Errorf("parsing director config: %w", err)
		}
		if n := setAllowedIPs(root, ips); n == 0 {
			return fmt.Errorf("no acl block found in director config %s — nothing to rotate", mountedSrc)
		}
		out, err := yaml.Marshal(root)
		if err != nil {
			return fmt.Errorf("re-marshaling director config: %w", err)
		}
		return os.WriteFile(mountedSrc, out, 0o644)
	}

	settle := time.Duration(tc.ACLRotation.SettleSecondsOrDefault()) * time.Second
	const aclLeak = 3 // tolerate a few in-flight records around the flip

	var passed bool
	var errs []string
	var beforeCount, finalCount int64

	switch tc.ACLRotation.Expect {
	case config.ACLRotationRecover:
		// Phase 0: confirm the agent is BLOCKED — the receiver count must stay at
		// ~0 across the baseline window. This proves the case really starts blocked
		// (otherwise "delivery after rotation" is vacuous). It also gives the agent
		// time to attempt and retry while denied.
		baseline := time.Duration(tc.ACLRotation.BaselineSecondsOrDefault()) * time.Second
		fmt.Printf("  confirming the agent is BLOCKED for %s (receiver must stay ~0)…\n", baseline)
		if err := sleepWithinDeadline(baseline); err != nil {
			return results.RunResult{}, err
		}
		rmBlocked, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			return results.RunResult{}, fmt.Errorf("sampling receiver during the blocked window: %w", qerr)
		}
		fmt.Printf("    received while blocked: %s\n", formatCount(rmBlocked.LinesReceived))
		blockedOK := rmBlocked.LinesReceived <= aclLeak
		if !blockedOK {
			errs = append(errs, fmt.Sprintf(
				"agent was NOT blocked before rotation (%s records during the %s baseline) — the initial allowlist already admits it; the recover transition is untested",
				formatCount(rmBlocked.LinesReceived), baseline))
		}

		// Phase 1: rotate to the allow config; the director's refreshACL must pick
		// it up within acl.update_interval and admit the agent.
		fmt.Println("  rotating director ACL to ADMIT the agent (acl.allowed_ips rewrite, no bounce)…")
		if err := patchAllowedIPs(tc.ACLRotation.AllowedIPs); err != nil {
			return results.RunResult{}, fmt.Errorf("rewriting director config for recover: %w", err)
		}
		if err := sleepWithinDeadline(settle); err != nil {
			return results.RunResult{}, err
		}

		// Phase 2: delivery must START. Drain until min_received or deadline.
		drainTimeout := 3 * time.Minute
		if tc.Correctness.DrainSeconds > 0 {
			drainTimeout = time.Duration(tc.Correctness.DrainSeconds) * time.Second
		}
		fmt.Printf("  waiting for delivery to start (receiver >= %s, up to %s)…\n", formatCount(minRecv), drainTimeout)
		drainDeadline := time.Now().Add(drainTimeout)
		if drainDeadline.After(runDeadline) {
			drainDeadline = runDeadline
		}
		for time.Now().Before(drainDeadline) {
			rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
			if qerr == nil {
				fmt.Printf("    received: %s\n", formatCount(rm.LinesReceived))
				finalCount = rm.LinesReceived
				if rm.LinesReceived >= minRecv {
					break
				}
			}
			time.Sleep(3 * time.Second)
		}
		started := finalCount >= minRecv
		if !started {
			errs = append(errs, fmt.Sprintf(
				"delivery did not start after admitting the agent — %s records (expected >= %s); the ACL hot-reload did not take effect",
				formatCount(finalCount), formatCount(minRecv)))
		}
		passed = blockedOK && started

	case config.ACLRotationRevoke:
		// Phase 0: establish delivery — wait until the agent is enrolled and the
		// receiver count clears min_received.
		warmup := tc.WarmupOrDefault(30 * time.Second)
		initialBudget := min(warmup+3*time.Minute, time.Until(runDeadline))
		if initialBudget <= 0 {
			return results.RunResult{}, fmt.Errorf("run timeout (%s) exceeded before the initial delivery wait", r.opts.Timeout)
		}
		fmt.Printf("  waiting for initial delivery (receiver >= %s, up to %s)…\n", formatCount(minRecv), initialBudget)
		initialDeadline := time.Now().Add(initialBudget)
		established := false
		for time.Now().Before(initialDeadline) {
			rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
			if qerr == nil {
				fmt.Printf("    received: %s\n", formatCount(rm.LinesReceived))
				if rm.LinesReceived >= minRecv {
					beforeCount = rm.LinesReceived
					established = true
					break
				}
			}
			time.Sleep(2 * time.Second)
		}
		if !established {
			return results.RunResult{}, fmt.Errorf(
				"agent never delivered the initial %s records — cannot test revocation (subject image %s:%s); "+
					"if this is not the agent-capable image, re-run with VMETRIC_IMAGE=vmetric/director-enterprise",
				formatCount(minRecv), subject.Image, subject.Version)
		}
		fmt.Printf("  delivery established — %s records before revocation ✓\n", formatCount(beforeCount))

		// Phase 1: rotate to the block config; the next /agent/vmf POST after the
		// refresh must be rejected, so delivery STOPS.
		fmt.Println("  rotating director ACL to BLOCK the agent (acl.allowed_ips rewrite, no bounce)…")
		if err := patchAllowedIPs(tc.ACLRotation.AllowedIPs); err != nil {
			return results.RunResult{}, fmt.Errorf("rewriting director config for revoke: %w", err)
		}
		if err := sleepWithinDeadline(settle); err != nil {
			return results.RunResult{}, err
		}

		// Phase 2: confirm delivery STOPPED. Sample once after settle, then again
		// after a second settle; the count must not meaningfully advance between
		// the two post-rotation samples (a few in-flight records right at the flip
		// are tolerated, but no NEW records once the refresh has applied).
		rmAfter1, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			return results.RunResult{}, fmt.Errorf("sampling receiver after revocation: %w", qerr)
		}
		if err := sleepWithinDeadline(settle); err != nil {
			return results.RunResult{}, err
		}
		rmAfter2, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			return results.RunResult{}, fmt.Errorf("re-sampling receiver after revocation: %w", qerr)
		}
		finalCount = rmAfter2.LinesReceived
		advanced := rmAfter2.LinesReceived - rmAfter1.LinesReceived
		fmt.Printf("  post-revocation: before=%s after1=%s after2=%s (Δ across the block window: %s)\n",
			formatCount(beforeCount), formatCount(rmAfter1.LinesReceived), formatCount(rmAfter2.LinesReceived), formatCount(advanced))
		// A negative delta means the receiver counter regressed (e.g. a restart or
		// metrics reset) — the monotonic-count assumption the "stopped" check rests
		// on no longer holds, so the result is inconclusive rather than a pass.
		stopped := advanced >= 0 && advanced <= aclLeak
		if advanced < 0 {
			errs = append(errs, fmt.Sprintf(
				"inconclusive: receiver count decreased after blocking the agent (after1=%s after2=%s) — the counter regressed, cannot confirm delivery stopped",
				formatCount(rmAfter1.LinesReceived), formatCount(rmAfter2.LinesReceived)))
		} else if !stopped {
			errs = append(errs, fmt.Sprintf(
				"delivery did NOT stop after blocking the agent (%s new records across the block window) — the ACL was not enforced on the live data path",
				formatCount(advanced)))
		}
		passed = stopped

	default:
		return results.RunResult{}, fmt.Errorf("unknown acl_rotation.expect %q", tc.ACLRotation.Expect)
	}

	if rm, qerr := r.queryReceiverMetrics(metricsPort, 30*time.Second); qerr == nil {
		finalCount = rm.LinesReceived
	}

	elapsed := time.Since(startTime).Seconds()
	if passed {
		fmt.Printf("  director↔agent ACL rotation (%s): PASSED ✓\n", tc.ACLRotation.Expect)
	} else {
		fmt.Printf("  director↔agent ACL rotation (%s): FAILED ✗\n", tc.ACLRotation.Expect)
	}

	result := results.RunResult{
		TestName:    tc.Name,
		Config:      configName,
		Subject:     subject.Name,
		Version:     subject.Version,
		Hardware:    hardwareID(),
		Timestamp:   startTime,
		DurationSec: elapsed,
		LinesOut:    finalCount,
		Passed:      &passed,
	}
	if !passed {
		result.FailReason = strings.Join(errs, "; ")
	}

	dir, err := r.store.Save(result, "")
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}
	fmt.Printf("  done. results → %s\n", dir)

	return result, nil
}

// setAllowedIPs sets acl.allowed_ips = ips on every `acl:` mapping reachable in
// the decoded YAML tree (robust to env/cluster/node nesting), returning how many
// it patched. Only that one key is touched; everything else round-trips
// unchanged. Used by runDirectorAgentACLRotation to rotate the director's live
// allowlist in place.
func setAllowedIPs(node any, ips []string) int {
	count := 0
	switch n := node.(type) {
	case map[string]any:
		for k, v := range n {
			if k == "acl" {
				if aclMap, ok := v.(map[string]any); ok {
					seq := make([]any, len(ips))
					for i, s := range ips {
						seq[i] = s
					}
					aclMap["allowed_ips"] = seq
					count++
				}
			}
			count += setAllowedIPs(v, ips)
		}
	case []any:
		for _, e := range n {
			count += setAllowedIPs(e, ips)
		}
	}
	return count
}

// ===== director cluster driver ================================================

// dockerLogsAll returns the full combined logs of a container ("" on error).
func dockerLogsAll(container string) string {
	out, _ := exec.Command("docker", "logs", container).CombinedOutput()
	return string(out)
}

// clusterNodeContainers returns the N node container names the cluster compose
// emits: bench-subject-<name>-1 .. -N (1-based, matching subject-1..N hostnames).
func clusterNodeContainers(subjectName string, n int) []string {
	cs := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		cs = append(cs, fmt.Sprintf("bench-subject-%s-%d", subjectName, i))
	}
	return cs
}

// containerRunning reports whether a docker container is currently running (not
// stopped/exited). Used so a STOPPED ex-leader (whose log buffer still holds its
// old "became leader" line) is never counted as the current leader.
func containerRunning(container string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container).CombinedOutput()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "true"
}

// clusterFormed reports whether every node logged cluster formation.
func clusterFormed(containers []string) bool {
	for _, c := range containers {
		if !strings.Contains(dockerLogsAll(c), "Cluster initially formed") {
			return false
		}
	}
	return true
}

// waitClusterReady polls until every node has formed the cluster AND a leader is
// elected, or the deadline passes. Returns the leader index and whether ready.
func waitClusterReady(containers []string, deadline time.Time) (int, bool) {
	for time.Now().Before(deadline) {
		if clusterFormed(containers) {
			if l, ok := leaderExistsNow(containers); ok {
				return l, true
			}
		}
		time.Sleep(3 * time.Second)
	}
	l, _ := leaderExistsNow(containers)
	return l, false
}

// agentlessDeviceOwner scans all nodes for the most recent agentless device
// placement and returns the owning node NAME (director node.name = "1".."N"), or
// "". The leader logs the FIRST placement in one of two forms depending on the
// code path that runs it:
//   - "Assigned new device {id} ({name}) to node {N}"        (NewDevice branch), or
//   - "Reassigned device {id} ({name}) from  to {N}"          (reassign branch,
//     empty source = initial placement).
// Both end with the target node name, so we recognize either and take the last
// whitespace token. (Failover is detected separately by clusterReassignedFrom,
// which matches the specific "from <owner> to" so it can't trip on either of these
// initial-placement lines.)
func agentlessDeviceOwner(containers []string) string {
	owner := ""
	for _, c := range containers {
		for _, line := range strings.Split(dockerLogsAll(c), "\n") {
			if strings.Contains(line, "Assigned new device") || strings.Contains(line, "Reassigned device") {
				if fields := strings.Fields(line); len(fields) > 0 {
					owner = fields[len(fields)-1]
				}
			}
		}
	}
	return owner
}

// agentlessCollectingNow reports whether a node is ACTIVELY collecting from an
// agentless device right now: its recent logs show the collector forwarding
// records to the router ("Forwarded N log entries from device ... to router").
// This is the director-level proof that a deployed agent is delivering data to
// its owning node — independent of the downstream receiver (cluster E2E delivery
// has a separate gap; see runDirectorClusterCorrectness agentless_failover).
func agentlessCollectingNow(container string) bool {
	return strings.Contains(dockerLogsSince(container, "30s"), "entries from device")
}

// waitAgentlessCollecting waits until the agentless device is assigned to a node
// AND that owner is actively collecting (forwarding to the router). Returns the
// owner node name ("1".."N"), its container, and whether it became ready.
func waitAgentlessCollecting(subjectName string, containers []string, deadline time.Time) (string, string, bool) {
	for time.Now().Before(deadline) {
		if owner := agentlessDeviceOwner(containers); owner != "" {
			ownerContainer := fmt.Sprintf("bench-subject-%s-%s", subjectName, owner)
			if agentlessCollectingNow(ownerContainer) {
				return owner, ownerContainer, true
			}
		}
		time.Sleep(5 * time.Second)
	}
	return agentlessDeviceOwner(containers), "", false
}

// waitAgentlessCollectingExcluding waits until SOME node other than `exclude`
// (the stopped owner, identified by node name "1".."N") is actively collecting
// the agentless device — i.e. the device failed over and the new owner re-deployed
// and resumed collection. Returns the new owner's name, container, and readiness.
func waitAgentlessCollectingExcluding(containers []string, exclude string, deadline time.Time) (string, string, bool) {
	for time.Now().Before(deadline) {
		for i, c := range containers {
			name := strconv.Itoa(i + 1)
			if name == exclude {
				continue
			}
			if agentlessCollectingNow(c) {
				return name, c, true
			}
		}
		time.Sleep(5 * time.Second)
	}
	return "", "", false
}

// clusterReassignedFrom returns the "Reassigned device ... from <owner> to Y"
// failover line found across all nodes, and whether one exists. It matches the
// specific source node (the owner we stopped) so it can NOT be satisfied by the
// INITIAL placement, which the backend logs as "Reassigned device ... from  to N"
// (empty source) — that would make the failover assertion pass without a failover.
func clusterReassignedFrom(containers []string, owner string) (string, bool) {
	needle := fmt.Sprintf("from %s to ", owner)
	for _, c := range containers {
		for _, line := range strings.Split(dockerLogsAll(c), "\n") {
			if strings.Contains(line, "Reassigned device") && strings.Contains(line, needle) {
				return strings.TrimSpace(line), true
			}
		}
	}
	return "", false
}

// dockerLogsSince returns a container's logs from the last `since` window (e.g.
// "30s") — used to read CURRENT cluster state, robust to stale history across
// restarts (full logs keep old "became leader" lines forever).
func dockerLogsSince(container, since string) string {
	out, _ := exec.Command("docker", "logs", "--since", since, container).CombinedOutput()
	return string(out)
}

// leaderExistsNow reports whether the cluster currently has a leader and which
// node it is (1-based). It is robust to a SILENT leader: the elected leader logs
// "became leader on" once and then can go quiet for long stretches (followers, by
// contrast, disclaim every few seconds with "not cluster leader"), so a
// recent-window-only signal misses it.
//
// A node is the current leader when ALL of:
//   - it is running (a stopped ex-leader's old "became leader" line still sits in
//     docker's log buffer — exclude it);
//   - its full-history net leadership count is positive (more "became leader on"
//     than "is no longer the leader");
//   - it is NOT disclaiming leadership in the recent window — this rejects a
//     RESTARTED ex-leader that rejoined as a follower (its pre-restart "became
//     leader" line persists, but it now logs "not cluster leader" again).
//
// During quorum loss every survivor disclaims and no node holds net leadership →
// returns (0,false).
func leaderExistsNow(containers []string) (int, bool) {
	for i, c := range containers {
		if !containerRunning(c) {
			continue
		}
		full := dockerLogsAll(c)
		net := strings.Count(full, "became leader on") - strings.Count(full, "is no longer the leader")
		if net <= 0 {
			continue
		}
		if strings.Contains(dockerLogsSince(c, "20s"), "not cluster leader") {
			continue // restarted ex-leader, now a follower
		}
		return i + 1, true
	}
	return 0, false
}

// electedLeaderSince reports whether any node logged a FRESH leader election
// ("became leader on") within the last `since` window — i.e. a (re-)election just
// happened. Returns the electing node index too.
func electedLeaderSince(containers []string, since string) (int, bool) {
	for i, c := range containers {
		if strings.Contains(dockerLogsSince(c, since), "became leader on") {
			return i + 1, true
		}
	}
	return 0, false
}

// pickFollowerAvoiding returns a 1-based node index that is neither the leader nor
// `avoid` (e.g. the generator's target node), falling back if none qualifies.
func pickFollowerAvoiding(leader, avoid, n int) int {
	for i := 1; i <= n; i++ {
		if i != leader && i != avoid {
			return i
		}
	}
	for i := 1; i <= n; i++ {
		if i != leader {
			return i
		}
	}
	return 1
}

// nodeHasClusterIP reports whether the node container currently has ip bound on one
// of its interfaces. The director leader adds the cluster IP at runtime (ip addr
// add), so it shows up as an "inet <ip>/<prefix>" line in `ip -o addr show`. Returns
// false if the address is absent OR the lookup fails (e.g. the container is down
// mid-restart).
func nodeHasClusterIP(container, ip string) bool {
	out, err := exec.Command("docker", "exec", container, "ip", "-o", "addr", "show").CombinedOutput()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "inet "+ip+"/")
}

// waitNodeHasClusterIP polls until the node holds ip, or the deadline passes.
func waitNodeHasClusterIP(container, ip string, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		if nodeHasClusterIP(container, ip) {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// runDirectorClusterCorrectness drives a multi-node director cluster: it brings up
// N director nodes (rendered one config per node via {{@.NodeID@}}), waits for the
// cluster to form and elect a leader, drives the case workload (a generator at
// subject-1 or an agentless endpoint), then performs the optional disruptive
// cluster.action and asserts the cluster recovers (re-elects a leader / fails a
// device over / regains quorum) while delivery continues.
func (r *Runner) runDirectorClusterCorrectness(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	subject = r.applySubjectOverrides(subject)

	for _, capName := range tc.Requires {
		if !subject.HasCapability(capName) {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare capability %q required by case %q",
				subject.Name, capName, tc.Name)
		}
	}

	fmt.Printf("→ test=%s  subject=%s  version=%s  config=%s  (cluster: %d nodes, tls=%v, action=%q)\n",
		tc.Name, subject.Name, subject.Version, configName, tc.Cluster.Nodes, tc.Cluster.TLS, tc.Cluster.Action)

	srcCfg, err := tc.ConfigFilePath(r.opts.CasesDir, configName, subject)
	if err != nil {
		return results.RunResult{}, err
	}
	srcCfg, err = filepath.Abs(srcCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving config path: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "bench-"+tc.Name+"-")
	if err != nil {
		return results.RunResult{}, err
	}
	if err := os.Chmod(tmpDir, 0o1777); err != nil {
		return results.RunResult{}, fmt.Errorf("chmod tmpdir: %w", err)
	}
	defer func() {
		if !r.opts.NoCleanup {
			os.RemoveAll(tmpDir)
		}
	}()

	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		maps.Copy(extraEnv, cfg.Env)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    srcCfg,
		CaseDir:          caseDir,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
	}

	orch, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}

	nodes := clusterNodeContainers(subject.Name, tc.Cluster.Nodes)
	cleanup := append([]string{"bench-receiver", "bench-collector", "bench-agent"}, nodes...)
	for _, c := range cleanup {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()
	defer func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}()

	startTime := time.Now()
	runDeadline := startTime.Add(r.opts.Timeout)

	minRecv := tc.Correctness.MinReceived
	if minRecv <= 0 {
		minRecv = 1
	}

	fmt.Printf("  starting %d-node cluster + workload…\n", tc.Cluster.Nodes)
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting cluster: %w", err)
	}

	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	// Phase 1: wait for the cluster to FORM + elect a leader (slow — JetStream
	// quorum stabilization). Budget the case warmup (clusters need ~90-120s).
	warmup := tc.WarmupOrDefault(120 * time.Second)
	formDeadline := time.Now().Add(warmup)
	if formDeadline.After(runDeadline) {
		formDeadline = runDeadline
	}
	fmt.Printf("  waiting for cluster to form + elect a leader (up to %s)…\n", time.Until(formDeadline).Round(time.Second))
	leader, ready := waitClusterReady(nodes, formDeadline)

	var passed bool
	var errs []string
	var finalCount int64

	if !ready {
		errs = append(errs, fmt.Sprintf("cluster did not form/elect a leader within %s (formed=%v leaderIdx=%d)", warmup, clusterFormed(nodes), leader))
		passed = false
	} else {
		fmt.Printf("  cluster formed; leader = node %d\n", leader)

		// Phase 2: baseline. Two shapes:
		//  - agentless cases assert at the DIRECTOR level — the device deploys and the
		//    OWNING node collects (forwards to the router). Downstream receiver delivery
		//    is NOT the baseline gate here: cluster agentless E2E delivery has a known
		//    gap (a router job can be processed on a different node than the one that
		//    wrote the payload, and cross-node payload object-store reads can return
		//    NotFound → "payload evicted"), so it is logged as a soft signal only.
		//  - all other cases assert downstream receiver delivery reaches min_received
		//    (the tcp "direct path" delivers reliably in a cluster).
		drainTimeout := 3 * time.Minute
		if tc.Correctness.DrainSeconds > 0 {
			drainTimeout = time.Duration(tc.Correctness.DrainSeconds) * time.Second
		}
		drainDeadline := time.Now().Add(drainTimeout)
		if drainDeadline.After(runDeadline) {
			drainDeadline = runDeadline
		}

		isAgentless := tc.Cluster.Action == "agentless_failover"
		isClusterIP := tc.Cluster.Action == "cluster_ip_failover"
		var baselineOK bool
		var owner, ownerContainer string

		if isAgentless {
			fmt.Printf("  baseline: waiting for the agentless device to deploy + collect (up to %s)…\n", time.Until(drainDeadline).Round(time.Second))
			owner, ownerContainer, baselineOK = waitAgentlessCollecting(subject.Name, nodes, drainDeadline)
			if baselineOK {
				fmt.Printf("  agentless device deployed; owner = node %s (%s), collecting ✓\n", owner, ownerContainer)
			} else {
				errs = append(errs, "agentless device did not deploy/collect on any node during baseline")
			}
			r.sampleDelivery(metricsPort, 0) // soft: downstream E2E delivery (see note above)
		} else if isClusterIP {
			// The VIP test's baseline is simply a formed cluster with a leader (already
			// asserted by waitClusterReady above). Downstream delivery is a SOFT signal
			// here — logged, not gated — so the verdict isolates VIP placement/migration
			// from cluster data-plane behavior.
			baselineOK = true
			if rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second); qerr == nil {
				finalCount = rm.LinesReceived
				fmt.Printf("  (soft) baseline delivery: %s\n", formatCount(finalCount))
			}
		} else {
			fmt.Printf("  baseline: waiting for delivery (receiver >= %s, up to %s)…\n", formatCount(minRecv), time.Until(drainDeadline).Round(time.Second))
			for time.Now().Before(drainDeadline) {
				if rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second); qerr == nil {
					finalCount = rm.LinesReceived
					fmt.Printf("    received: %s\n", formatCount(finalCount))
					if finalCount >= minRecv {
						break
					}
				}
				time.Sleep(3 * time.Second)
			}
			baselineOK = finalCount >= minRecv
			if !baselineOK {
				errs = append(errs, fmt.Sprintf("baseline delivery did not reach min_received (%s < %s)", formatCount(finalCount), formatCount(minRecv)))
			}
		}

		// Phase 3: the disruptive action + recovery assertion.
		actionOK := true
		settle := time.Duration(tc.Cluster.SettleOrDefault()) * time.Second
		switch tc.Cluster.Action {
		case "":
			// baseline only — already asserted (form + leader + flow).
		case "restart_follower":
			// Restart a follower that is NOT node 1 (the generator's target), so the
			// data path stays up and we test pure follower churn.
			follower := pickFollowerAvoiding(leader, 1, tc.Cluster.Nodes)
			fmt.Printf("  restarting FOLLOWER node %d (%s)…\n", follower, nodes[follower-1])
			if rerr := exec.Command("docker", "restart", "-t", "10", nodes[follower-1]).Run(); rerr != nil {
				actionOK = false
				errs = append(errs, fmt.Sprintf("docker restart %s failed: %v (disruption did not happen)", nodes[follower-1], rerr))
			}
			time.Sleep(settle)
			if _, ok := leaderExistsNow(nodes); !ok {
				actionOK = false
				errs = append(errs, "no leader after restarting a follower (cluster unexpectedly lost leadership)")
			} else {
				fmt.Println("  cluster still has a leader after the follower restart ✓")
			}
			r.sampleDelivery(metricsPort, finalCount)
		case "restart_leader":
			fmt.Printf("  restarting LEADER node %d (%s)…\n", leader, nodes[leader-1])
			if rerr := exec.Command("docker", "restart", "-t", "10", nodes[leader-1]).Run(); rerr != nil {
				actionOK = false
				errs = append(errs, fmt.Sprintf("docker restart %s failed: %v (disruption did not happen)", nodes[leader-1], rerr))
			}
			restartedAt := time.Now()
			time.Sleep(settle)
			// A leader restart must trigger a re-election. Anchor the search window to
			// the instant the restart completed so a stale pre-restart "became leader"
			// line can't satisfy the check; fall back to a leader simply being present.
			elIdx, elected := electedLeaderSince(nodes, fmt.Sprintf("%ds", int(time.Since(restartedAt).Seconds())+2))
			nowIdx, haveLeader := leaderExistsNow(nodes)
			if !elected && !haveLeader {
				actionOK = false
				errs = append(errs, "no leader re-elected after restarting the leader")
			} else if elected {
				fmt.Printf("  re-election after leader restart: node %d became leader ✓\n", elIdx)
			} else {
				fmt.Printf("  leader present after restart: node %d ✓\n", nowIdx)
			}
			r.sampleDelivery(metricsPort, finalCount)
		case "stop_two_recover":
			// Stop a quorum-removing majority of the highest-index nodes (keep node 1 =
			// subject-1, the generator target, up). For N nodes quorum is N/2+1, so
			// stopping ceil(N/2) nodes leaves the survivors below quorum (no leader) —
			// stopping a fixed two would NOT lose quorum for N>3. Then restart them and
			// assert the cluster regains quorum and elects a leader.
			toStop := (tc.Cluster.Nodes + 1) / 2
			var stopped []string
			for i := 0; i < toStop; i++ {
				stopped = append(stopped, nodes[tc.Cluster.Nodes-1-i])
			}
			fmt.Printf("  stopping %d/%d nodes (%s) to lose quorum…\n", len(stopped), tc.Cluster.Nodes, strings.Join(stopped, ", "))
			for _, c := range stopped {
				if serr := exec.Command("docker", "stop", "-t", "10", c).Run(); serr != nil {
					actionOK = false
					errs = append(errs, fmt.Sprintf("docker stop %s failed: %v (disruption did not happen)", c, serr))
				}
			}
			time.Sleep(settle)
			if _, ok := leaderExistsNow(nodes); ok {
				fmt.Printf("  note: a leader still appears present with %d/%d down (unexpected — quorum should be lost)\n", len(stopped), tc.Cluster.Nodes)
			} else {
				fmt.Printf("  quorum lost (no leader with %d/%d nodes down), as expected ✓\n", len(stopped), tc.Cluster.Nodes)
			}
			fmt.Println("  restarting the stopped nodes to restore quorum…")
			for _, c := range stopped {
				if serr := exec.Command("docker", "start", c).Run(); serr != nil {
					actionOK = false
					errs = append(errs, fmt.Sprintf("docker start %s failed: %v", c, serr))
				}
			}
			recDeadline := time.Now().Add(warmup)
			if recDeadline.After(runDeadline) {
				recDeadline = runDeadline
			}
			fmt.Printf("  waiting for cluster to recover + re-elect a leader (up to %s)…\n", time.Until(recDeadline).Round(time.Second))
			l2, ok2 := waitClusterReady(nodes, recDeadline)
			if !ok2 {
				actionOK = false
				errs = append(errs, fmt.Sprintf("cluster did not recover a leader after restarting the two nodes (leaderIdx=%d)", l2))
			} else {
				fmt.Printf("  cluster recovered; leader = node %d ✓\n", l2)
			}
		case "agentless_failover":
			// owner/ownerContainer were resolved in the baseline phase.
			if owner == "" {
				actionOK = false
				errs = append(errs, "could not determine the agentless device owner (no 'Assigned new device'/'Reassigned device' log) — failover untestable")
			} else {
				// STOP the owner, wait past the heartbeat timeout for the leader to
				// reassign the device, THEN start it again. A plain `docker restart`
				// brings the node back within its ~10s graceful-stop window — shorter
				// than the ~15s heartbeat timeout — so the leader may never see it as
				// down and never reassign. The stop/wait/start sequence guarantees the
				// owner is down long enough, while still returning the cluster to full
				// strength afterwards (matching "the node that starts the agentless
				// machine gets restarted").
				fmt.Printf("  agentless device owner = node %s (%s); stopping it to force a failover…\n", owner, ownerContainer)
				preFailover := finalCount
				if serr := exec.Command("docker", "stop", "-t", "10", ownerContainer).Run(); serr != nil {
					actionOK = false
					errs = append(errs, fmt.Sprintf("docker stop %s failed: %v (disruption did not happen)", ownerContainer, serr))
				}
				fmt.Printf("  waiting %s for the leader to detect the down node and reassign the device…\n", settle)
				time.Sleep(settle)

				// HARD 1: the device is reassigned AWAY FROM the stopped owner — the
				//   automatic ownership failover the case exists to prove ("the new owner is
				//   another node automatically").
				if line, ok := clusterReassignedFrom(nodes, owner); ok {
					fmt.Printf("  failover observed: %s\n", line)
				} else {
					actionOK = false
					errs = append(errs, fmt.Sprintf("no device reassignment ('Reassigned device ... from %s to Y') after the owning node went down — failover did not happen", owner))
				}
				// HARD 2: a leader still exists (the cluster stayed healthy across the loss).
				if _, ok := leaderExistsNow(nodes); !ok {
					actionOK = false
					errs = append(errs, "no leader after the owning node went down")
				}
				// Bring the owner back so the cluster returns to full strength.
				fmt.Printf("  starting node %s again to restore the cluster…\n", ownerContainer)
				if serr := exec.Command("docker", "start", ownerContainer).Run(); serr != nil {
					actionOK = false
					errs = append(errs, fmt.Sprintf("docker start %s failed: %v", ownerContainer, serr))
				}
				// SOFT: a survivor resuming collection, and end-to-end receiver delivery, are
				//   logged but NOT asserted. Cluster agentless data-plane recovery after a node
				//   loss is a known gap: the degraded 2/3 window can stall the new owner's ingest,
				//   and cross-node payload object-store reads can return NotFound → "payload
				//   evicted". The hard verdict is the automatic ownership failover above.
				collectDeadline := time.Now().Add(settle + 90*time.Second)
				if collectDeadline.After(runDeadline) {
					collectDeadline = runDeadline
				}
				if newOwner, _, ok := waitAgentlessCollectingExcluding(nodes, owner, collectDeadline); ok {
					fmt.Printf("  (soft) a surviving node (%s) is collecting the device after failover ✓\n", newOwner)
				} else {
					fmt.Println("  (soft) no survivor observed collecting within the window — known cluster data-plane gap")
				}
				r.sampleDelivery(metricsPort, preFailover)
			}
		case "cluster_ip_failover":
			// The elected leader must hold the virtual IP; followers must not. Then
			// restart the leader and assert the IP migrates to the newly elected leader
			// (and the old leader, now a follower, no longer holds it).
			vip := tc.Cluster.IP
			ipDeadline := time.Now().Add(settle)
			if ipDeadline.After(runDeadline) {
				ipDeadline = runDeadline
			}
			fmt.Printf("  asserting leader node %d holds the cluster IP %s…\n", leader, vip)
			if !waitNodeHasClusterIP(nodes[leader-1], vip, ipDeadline) {
				actionOK = false
				errs = append(errs, fmt.Sprintf("leader node %d did not bind the cluster IP %s", leader, vip))
			} else {
				fmt.Printf("  leader holds %s ✓\n", vip)
			}
			for i, c := range nodes {
				if i+1 != leader && nodeHasClusterIP(c, vip) {
					actionOK = false
					errs = append(errs, fmt.Sprintf("follower node %d also holds the cluster IP %s (should be leader-only)", i+1, vip))
				}
			}
			// STOP the leader (not `docker restart`): a restart's brief downtime can be
			// shorter than the RAFT election window, letting the same node regain
			// leadership so the VIP never actually moves (and the "released" check below
			// would be skipped). Stopping it guarantees a DIFFERENT survivor wins and
			// binds the VIP; we then start it again and assert it rejoined WITHOUT the IP.
			oldLeader := leader
			fmt.Printf("  stopping LEADER node %d (%s) to force the cluster IP to migrate…\n", oldLeader, nodes[oldLeader-1])
			if serr := exec.Command("docker", "stop", "-t", "10", nodes[oldLeader-1]).Run(); serr != nil {
				actionOK = false
				errs = append(errs, fmt.Sprintf("docker stop %s failed: %v (disruption did not happen)", nodes[oldLeader-1], serr))
			}
			time.Sleep(settle)
			newLeader, haveLeader := leaderExistsNow(nodes)
			switch {
			case !haveLeader:
				actionOK = false
				errs = append(errs, "no leader elected among survivors after stopping the leader (cluster IP cannot migrate)")
			case newLeader == oldLeader:
				// The stopped node is excluded by leaderExistsNow (containerRunning);
				// reaching here would mean leadership never actually moved.
				actionOK = false
				errs = append(errs, fmt.Sprintf("leadership did not move off the stopped node %d", oldLeader))
			default:
				fmt.Printf("  new leader = node %d\n", newLeader)
				migDeadline := time.Now().Add(settle)
				if migDeadline.After(runDeadline) {
					migDeadline = runDeadline
				}
				if !waitNodeHasClusterIP(nodes[newLeader-1], vip, migDeadline) {
					actionOK = false
					errs = append(errs, fmt.Sprintf("cluster IP %s did not migrate to the new leader (node %d)", vip, newLeader))
				} else {
					fmt.Printf("  cluster IP %s migrated to node %d ✓\n", vip, newLeader)
				}
			}
			// Bring the old leader back; it must rejoin as a follower WITHOUT the VIP.
			fmt.Printf("  starting node %s again to restore the cluster…\n", nodes[oldLeader-1])
			if serr := exec.Command("docker", "start", nodes[oldLeader-1]).Run(); serr != nil {
				actionOK = false
				errs = append(errs, fmt.Sprintf("docker start %s failed: %v", nodes[oldLeader-1], serr))
			} else {
				// Brief wait for the container to come up: a fresh start wipes the netns
				// and a follower never binds the VIP, so it must be absent. Check soon
				// (before any later re-election could legitimately move leadership back).
				if d := time.Until(runDeadline); d > 0 {
					time.Sleep(min(10*time.Second, d))
				}
				if nodeHasClusterIP(nodes[oldLeader-1], vip) {
					actionOK = false
					errs = append(errs, fmt.Sprintf("restarted node %d (former leader) holds the cluster IP %s despite being a follower", oldLeader, vip))
				} else {
					fmt.Printf("  former leader node %d does not hold the cluster IP ✓\n", oldLeader)
				}
			}
			r.sampleDelivery(metricsPort, finalCount)
		}

		passed = baselineOK && actionOK
	}

	if rm, qerr := r.queryReceiverMetrics(metricsPort, 30*time.Second); qerr == nil {
		finalCount = rm.LinesReceived
	}

	elapsed := time.Since(startTime).Seconds()
	if passed {
		fmt.Printf("  director cluster (%s): PASSED ✓\n", clusterActionLabel(tc.Cluster.Action))
	} else {
		fmt.Printf("  director cluster (%s): FAILED ✗\n", clusterActionLabel(tc.Cluster.Action))
		// On failure, dump a short tail of each node's log to aid diagnosis.
		for _, c := range nodes {
			out, _ := exec.Command("docker", "logs", "--tail", "12", c).CombinedOutput()
			fmt.Printf("  --- %s (tail) ---\n%s\n", c, string(out))
		}
	}

	result := results.RunResult{
		TestName:    tc.Name,
		Config:      configName,
		Subject:     subject.Name,
		Version:     subject.Version,
		Hardware:    hardwareID(),
		Timestamp:   startTime,
		DurationSec: elapsed,
		LinesOut:    finalCount,
		Passed:      &passed,
	}
	if !passed {
		result.FailReason = strings.Join(errs, "; ")
	}

	dir, err := r.store.Save(result, "")
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}
	fmt.Printf("  done. results → %s\n", dir)
	return result, nil
}

// sampleDelivery logs the current receiver count after a disruption WITHOUT
// failing the verdict — used for restart cases where the finite generator may have
// already drained, so continued delivery is informative but not required (the
// cluster-health assertion is the hard verdict; the baseline proved data flows).
func (r *Runner) sampleDelivery(metricsPort int, before int64) {
	if rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second); qerr == nil {
		fmt.Printf("    post-action received: %s (was %s)\n", formatCount(rm.LinesReceived), formatCount(before))
	}
}

func clusterActionLabel(action string) string {
	if action == "" {
		return "baseline"
	}
	return action
}

// ── Fleet automation simulator control ─────────────────────────────────────────

// fleetStatus mirrors the bench fleet simulator's /status JSON. Each director's
// inbound frames are keyed by "<action>.<command>" (e.g. "req.health",
// "rep.remote_check") with a count and the last payload seen.
type fleetStatus struct {
	Directors map[string]struct {
		Connected bool `json:"connected"`
		Connects  int  `json:"connects"`
		Inbound   map[string]struct {
			Count    int    `json:"count"`
			LastData string `json:"last_data"`
		} `json:"inbound"`
	} `json:"directors"`
}

func (st *fleetStatus) connected(id string) bool {
	d, ok := st.Directors[id]
	return ok && d.Connected
}
func (st *fleetStatus) connects(id string) int { return st.Directors[id].Connects }
func (st *fleetStatus) count(id, key string) int {
	d, ok := st.Directors[id]
	if !ok {
		return 0
	}
	return d.Inbound[key].Count
}
func (st *fleetStatus) lastData(id, key string) string {
	d, ok := st.Directors[id]
	if !ok {
		return ""
	}
	return d.Inbound[key].LastData
}

// fleetSimStatus reads the simulator's observation snapshot via `docker exec wget`.
// We use the busybox `wget` that ships in the simulator's alpine base rather than
// curl, so the bench-fleetsim image needs no extra package (and carries no extra
// CVE surface — it scores Docker Scout grade A like the other bench helpers). The
// control API is not host-published; it is reached inside the bench network.
// busybox wget has no request-timeout flag, so the exec is bounded by a context.
func fleetSimStatus(simContainer string) (*fleetStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "exec", simContainer,
		"wget", "-q", "-O", "-", "http://127.0.0.1:8090/status").Output()
	if err != nil {
		return nil, err
	}
	var st fleetStatus
	if err := json.Unmarshal(out, &st); err != nil {
		return nil, fmt.Errorf("decode sim status: %w (raw: %.200s)", err, string(out))
	}
	return &st, nil
}

// fleetSimSend tells the simulator to send a platform→director command. busybox
// wget POSTs the JSON body via --post-data (the sim's sendHandler reads the raw
// body with json.Unmarshal, so the request content-type is irrelevant).
func fleetSimSend(simContainer, dirID, command string, params map[string]any) (string, error) {
	body, _ := json.Marshal(map[string]any{"director": dirID, "command": command, "params": params})
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "exec", simContainer,
		"wget", "-q", "-O", "-", "--post-data="+string(body),
		"http://127.0.0.1:8090/send").CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("sim send %s: %w (out: %.200s)", command, err, string(out))
	}
	return string(out), nil
}

// fleetWaitConnected polls until the director shows connected at the simulator.
func fleetWaitConnected(simContainer, dirID string, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		if st, err := fleetSimStatus(simContainer); err == nil && st.connected(dirID) {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// fleetWaitCount polls until inbound[key].count >= min for the director, returning
// the final count and whether the threshold was reached.
func fleetWaitCount(simContainer, dirID, key string, min int, deadline time.Time) (int, bool) {
	last := 0
	for time.Now().Before(deadline) {
		if st, err := fleetSimStatus(simContainer); err == nil {
			last = st.count(dirID, key)
			if last >= min {
				return last, true
			}
		}
		time.Sleep(3 * time.Second)
	}
	return last, false
}

// fleetStr returns v, or def when v is empty.
func fleetStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// zipSingleFile returns a ZIP archive containing one entry (name → content). Used
// to package a config for the director's config-set path, which unzips + parses a
// ZIP as a plain config tree (vs treating raw bytes as a packaged .vmf).
func zipSingleFile(name string, content []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(name)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(content); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// runFleetAutomationCorrectness drives the director's fleet/automation path against
// the bench fleet simulator: the director dials the simulator's WebSocket
// (fleet.type=vmetric + a custom ws URL to the fleet-sim endpoint), and the driver
// asserts the platform<->director automation functions for tc.Fleet.Scenario.
func (r *Runner) runFleetAutomationCorrectness(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	subject = r.applySubjectOverrides(subject)
	fc := tc.Fleet

	// Dispatched before the generic capability guard in Run, so honor the case's
	// requires: here too — otherwise a fleet case could start against a subject
	// that lacks a required capability.
	for _, capName := range tc.Requires {
		if !subject.HasCapability(capName) {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare capability %q required by case %q",
				subject.Name, capName, tc.Name)
		}
	}

	fmt.Printf("→ test=%s  subject=%s  version=%s  (fleet scenario=%q)\n",
		tc.Name, subject.Name, subject.Version, fc.Scenario)

	srcCfg, err := tc.ConfigFilePath(r.opts.CasesDir, configName, subject)
	if err != nil {
		return results.RunResult{}, err
	}
	srcCfg, err = filepath.Abs(srcCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving config path: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "bench-"+tc.Name+"-")
	if err != nil {
		return results.RunResult{}, err
	}
	if err := os.Chmod(tmpDir, 0o1777); err != nil {
		return results.RunResult{}, fmt.Errorf("chmod tmpdir: %w", err)
	}
	defer func() {
		if !r.opts.NoCleanup {
			os.RemoveAll(tmpDir)
		}
	}()

	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		maps.Copy(extraEnv, cfg.Env)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    srcCfg,
		CaseDir:          caseDir,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
	}

	orch, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}

	simContainer := fc.SimContainerOrDefault()
	dirID := fc.DirectorIDOrDefault()
	subjectContainer := "bench-subject-" + subject.Name
	cleanup := []string{"bench-receiver", "bench-collector", "bench-agent", "bench-generator", subjectContainer, simContainer}
	for _, c := range cleanup {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()
	defer func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}()

	startTime := time.Now()
	runDeadline := startTime.Add(r.opts.Timeout)
	settle := time.Duration(fc.SettleOrDefault()) * time.Second
	warmup := tc.WarmupOrDefault(20 * time.Second)

	fmt.Println("  starting director + fleet simulator…")
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting fleet topology: %w", err)
	}

	var passed bool
	var errs []string
	var finalCount int64

	// bad_token: the director's token is wrong; assert it NEVER connects. Watch
	// the monotonic connects COUNTER (not just the current connected snapshot):
	// a brief successful auth that drops between polls still bumps the counter,
	// so any increase over the baseline means auth was not enforced.
	if fc.Scenario == "bad_token" {
		before := 0
		if st0, e := fleetSimStatus(simContainer); e == nil {
			before = st0.connects(dirID)
		}
		deadline := time.Now().Add(warmup + settle)
		if deadline.After(runDeadline) {
			deadline = runDeadline
		}
		fmt.Printf("  asserting the director does NOT connect with a bad token (up to %s)…\n", time.Until(deadline).Round(time.Second))
		connected := false
		for time.Now().Before(deadline) {
			if st, e := fleetSimStatus(simContainer); e == nil && (st.connected(dirID) || st.connects(dirID) > before) {
				connected = true
				break
			}
			time.Sleep(3 * time.Second)
		}
		if connected {
			errs = append(errs, "director connected despite a bad fleet token (auth not enforced)")
		} else {
			fmt.Println("  director did not connect with a bad token, as expected ✓")
		}
		passed = len(errs) == 0
		return r.saveFleetResult(tc, subject, configName, startTime, finalCount, passed, errs, simContainer, subjectContainer)
	}

	// All other scenarios: wait for the director to connect first.
	connDeadline := time.Now().Add(warmup + 90*time.Second)
	if connDeadline.After(runDeadline) {
		connDeadline = runDeadline
	}
	fmt.Printf("  waiting for the director to connect to the fleet simulator (up to %s)…\n", time.Until(connDeadline).Round(time.Second))
	if !fleetWaitConnected(simContainer, dirID, connDeadline) {
		errs = append(errs, "director never connected to the fleet simulator")
		passed = false
		return r.saveFleetResult(tc, subject, configName, startTime, finalCount, passed, errs, simContainer, subjectContainer)
	}
	fmt.Println("  director connected to the fleet simulator ✓")

	// Deliver the audited operational config (the platform's job). A fleet
	// director loads its operational config (environments/devices/targets/routes/
	// proxy_tls) only from an encoded vmetric.vmf delivered by the platform — it
	// rejects plain YAML by design — so scenarios needing a real pipeline
	// (live_data/stats) or the agent-comm interface (enrollment) must have it
	// pushed here. The director writes it to Path.Config/vmetric.vmf, applies, and
	// reloads (SystemDB), bringing up its listeners + agent-comm HTTPS server.
	if fc.DeliverConfig != "" {
		vmfPath := filepath.Join(caseDir, "configs", fc.DeliverConfig)
		vmfBytes, e := os.ReadFile(vmfPath)
		if e != nil {
			errs = append(errs, "cannot read deliver_config "+fc.DeliverConfig+": "+e.Error())
			passed = false
			return r.saveFleetResult(tc, subject, configName, startTime, finalCount, passed, errs, simContainer, subjectContainer)
		}
		fmt.Printf("  delivering operational config (%s, %d bytes vmf)…\n", fc.DeliverConfig, len(vmfBytes))
		b64 := base64.StdEncoding.EncodeToString(vmfBytes)
		if _, se := fleetSimSend(simContainer, dirID, "config", map[string]any{"data_b64": b64}); se != nil {
			errs = append(errs, "failed to deliver operational config: "+se.Error())
		}
		// Wait for the director to apply + reload it (its listeners / agent-comm
		// HTTPS server come up once SystemDB is populated). Poll the subject's logs.
		applyDeadline := time.Now().Add(settle + 60*time.Second)
		if applyDeadline.After(runDeadline) {
			applyDeadline = runDeadline
		}
		applied := false
		for time.Now().Before(applyDeadline) {
			out, _ := exec.Command("docker", "logs", "--tail", "200", subjectContainer).CombinedOutput()
			s := string(out)
			if strings.Contains(s, "Director HTTP Server started") || strings.Contains(s, "Listener (") {
				applied = true
				break
			}
			time.Sleep(3 * time.Second)
		}
		if applied {
			fmt.Println("  director applied the delivered config (pipeline + agent-comm up) ✓")
		} else {
			fmt.Println("  (warning) did not observe the director bring up its pipeline/agent-comm after config delivery")
		}
	}

	scenarioDeadline := func() time.Time {
		d := time.Now().Add(settle + 60*time.Second)
		if d.After(runDeadline) {
			d = runDeadline
		}
		return d
	}

	switch fc.Scenario {
	case "connect":
		// Heartbeat/health + connection_state must be published periodically.
		minHealth := fc.MinHealth
		if minHealth <= 0 {
			minHealth = 2
		}
		fmt.Printf("  waiting for >= %d health frames + connection_state…\n", minHealth)
		hc, okH := fleetWaitCount(simContainer, dirID, "req.health", minHealth, scenarioDeadline())
		finalCount = int64(hc)
		if !okH {
			errs = append(errs, fmt.Sprintf("director did not publish >= %d health frames (got %d)", minHealth, hc))
		} else {
			fmt.Printf("  health frames: %d ✓\n", hc)
		}
		if cs, okC := fleetWaitCount(simContainer, dirID, "req.connection_state", 1, scenarioDeadline()); !okC {
			errs = append(errs, "director did not publish connection_state")
		} else {
			fmt.Printf("  connection_state frames: %d ✓\n", cs)
		}
		// The director should also have requested its config on connect.
		if st, e := fleetSimStatus(simContainer); e != nil {
			errs = append(errs, "could not read simulator status to confirm the initial config request: "+e.Error())
		} else if st.count(dirID, "req.config") < 1 {
			errs = append(errs, "director did not send an initial config request")
		} else {
			fmt.Println("  initial config request seen ✓")
		}

	case "remote_check":
		expect := fc.ExpectRemoteResultOrDefault()
		params := map[string]any{
			"address":  fc.RemoteAddress,
			"username": fleetStr(fc.RemoteUsername, "root"),
			"password": fc.RemotePassword,
			"port":     fc.RemotePort,
			"timeout":  10,
		}
		fmt.Printf("  sending remote_check (ssh %s:%d), expecting result=%d…\n", fc.RemoteAddress, fc.RemotePort, expect)
		if _, e := fleetSimSend(simContainer, dirID, "remote_check_ssh", params); e != nil {
			errs = append(errs, "failed to send remote_check: "+e.Error())
		}
		if _, ok := fleetWaitCount(simContainer, dirID, "rep.remote_check", 1, scenarioDeadline()); !ok {
			errs = append(errs, "no remote_check reply from director")
		} else {
			last := ""
			if st, e := fleetSimStatus(simContainer); e == nil {
				last = st.lastData(dirID, "rep.remote_check")
			}
			fmt.Printf("  remote_check reply: %s\n", last)
			needle := fmt.Sprintf("\"result\":%d", expect)
			if !strings.Contains(last, needle) {
				errs = append(errs, fmt.Sprintf("remote_check reply did not report result=%d: %s", expect, last))
			} else {
				fmt.Printf("  remote_check result=%d ✓\n", expect)
			}
		}

	case "config_update":
		// Push a new config (Update Triggered). The new config is read from the
		// case's configs/update.yml and shipped as a ZIP containing vmetric.yml —
		// the director's config-set path treats a non-zip payload as a packaged
		// .vmf, while a ZIP is unzipped and parsed as a plain config tree. The
		// director validates + atomically swaps it in and replies executed=true.
		updPath := filepath.Join(caseDir, "configs", "update.yml")
		raw, e := os.ReadFile(updPath)
		if e != nil {
			errs = append(errs, "cannot read configs/update.yml for config_update: "+e.Error())
		} else if zipped, ze := zipSingleFile("vmetric.yml", raw); ze != nil {
			errs = append(errs, "cannot zip update config: "+ze.Error())
		} else {
			b64 := base64.StdEncoding.EncodeToString(zipped)
			fmt.Printf("  pushing config update (%d bytes raw, %d zipped)…\n", len(raw), len(zipped))
			if _, se := fleetSimSend(simContainer, dirID, "config", map[string]any{"data_b64": b64}); se != nil {
				errs = append(errs, "failed to push config: "+se.Error())
			}
			if _, ok := fleetWaitCount(simContainer, dirID, "rep.config", 1, scenarioDeadline()); !ok {
				errs = append(errs, "no config reply from director after push")
			} else {
				last := ""
				if st, e := fleetSimStatus(simContainer); e == nil {
					last = st.lastData(dirID, "rep.config")
				}
				fmt.Printf("  config reply: %.300s\n", last)
				if !strings.Contains(last, "\"executed\":true") {
					errs = append(errs, "director did not report the config as executed: "+last)
				} else {
					fmt.Println("  config applied (executed) ✓")
				}
			}
		}

	case "live_data", "console_log":
		cmd := "live_data"
		params := map[string]any{
			"capture_time": 15,
			"capture_line": 100,
			"where":        fleetStr(fc.LiveWhere, "before-pre-process"),
			"source_type":  fleetStr(fc.LiveSource, "director"),
		}
		if fc.Scenario == "console_log" {
			cmd = "console_log"
			params = map[string]any{"capture_time": 15, "capture_line": 100}
		}
		fmt.Printf("  starting %s capture session…\n", cmd)
		if _, e := fleetSimSend(simContainer, dirID, cmd, params); e != nil {
			errs = append(errs, "failed to start "+cmd+": "+e.Error())
		}
		// The capture streams results back on a "<cmd>_reply" subject (action req),
		// e.g. vmetric.fleet.req.platform.director.<id>.<reqID>.live_data_reply.
		// Generator traffic (if the case ships one) flows through the director so
		// records get captured.
		key := "req." + cmd + "_reply"
		if c, ok := fleetWaitCount(simContainer, dirID, key, 1, scenarioDeadline()); !ok {
			errs = append(errs, fmt.Sprintf("no %s capture data streamed back (got %d frames)", cmd, c))
		} else {
			finalCount = int64(c)
			fmt.Printf("  %s streamed %d frame(s) ✓\n", cmd, c)
		}

	case "stats":
		// The director forwards stats/metrics partitions as traffic flows. The case
		// ships a generator → director pipeline; assert metrics frames arrive.
		fmt.Println("  waiting for stats/metrics frames…")
		c1, ok1 := fleetWaitCount(simContainer, dirID, "req.metricsvmf", 1, scenarioDeadline())
		c2 := 0
		if !ok1 {
			c2, _ = fleetWaitCount(simContainer, dirID, "req.metrics", 1, scenarioDeadline())
		}
		finalCount = int64(c1 + c2)
		if c1 < 1 && c2 < 1 {
			errs = append(errs, "director did not forward any stats/metrics frames")
		} else {
			fmt.Printf("  stats frames: metricsvmf=%d metrics=%d ✓\n", c1, c2)
		}

	case "reconnect":
		st0, _ := fleetSimStatus(simContainer)
		before := 0
		if st0 != nil {
			before = st0.connects(dirID)
		}
		fmt.Printf("  restarting the fleet simulator (connects=%d)…\n", before)
		_ = exec.Command("docker", "restart", "-t", "5", simContainer).Run()
		// After the sim restarts, the director must redial and re-register.
		fmt.Println("  waiting for the director to reconnect…")
		reconnected := false
		rd := scenarioDeadline()
		for time.Now().Before(rd) {
			if st, e := fleetSimStatus(simContainer); e == nil && st.connected(dirID) && st.connects(dirID) >= 1 {
				reconnected = true
				break
			}
			time.Sleep(3 * time.Second)
		}
		if !reconnected {
			errs = append(errs, "director did not reconnect after the simulator restarted")
		} else {
			// And resume publishing health on the fresh connection.
			if hc, ok := fleetWaitCount(simContainer, dirID, "req.health", 1, scenarioDeadline()); ok {
				finalCount = int64(hc)
				fmt.Printf("  director reconnected and resumed health (%d) ✓\n", hc)
			} else {
				errs = append(errs, "director reconnected but did not resume health publishing")
			}
		}

	case "self_managed":
		// A self-managed director still publishes health but must IGNORE platform
		// commands (ListenRequests is not started). Send a remote_check and assert
		// NO reply, while health keeps flowing.
		if hc, ok := fleetWaitCount(simContainer, dirID, "req.health", 1, scenarioDeadline()); ok {
			fmt.Printf("  self-managed director still publishes health (%d) ✓\n", hc)
			finalCount = int64(hc)
		} else {
			errs = append(errs, "self-managed director did not publish health")
		}
		fmt.Println("  sending remote_check; it must be IGNORED…")
		if _, e := fleetSimSend(simContainer, dirID, "remote_check_ssh",
			map[string]any{"address": "10.255.255.1", "port": 22, "timeout": 5}); e != nil {
			errs = append(errs, "failed to send remote_check: "+e.Error())
		}
		// Give the director a chance to (wrongly) reply, but never sleep past the
		// overall run deadline the other waits respect.
		if d := time.Until(runDeadline); d > 0 {
			if d > settle {
				d = settle
			}
			time.Sleep(d)
		}
		if st, e := fleetSimStatus(simContainer); e != nil {
			errs = append(errs, "could not read simulator status to confirm the command was ignored: "+e.Error())
		} else if st.count(dirID, "rep.remote_check") > 0 {
			errs = append(errs, "self-managed director replied to a platform command (should ignore it)")
		} else {
			fmt.Println("  self-managed director ignored the platform command ✓")
		}

	case "enrollment":
		// Agent → platform connectivity. An agent (an agent: container in enrollment
		// mode — config hash device_id 0, enrollment_id != 0) connects to the
		// director and sends a check_enrollment request; the director (as JetStream
		// leader of its own node) forwards it to the platform over the fleet link.
		// The simulator auto-approves (FLEETSIM_AUTOENROLL=1) and the director relays
		// the approval to the agent. Verdict: the simulator observes the forwarded
		// enrollment (rep.check_enrollment), proving the agent→director→platform path.
		fmt.Println("  waiting for the agent's enrollment to be forwarded to the platform…")
		if c, ok := fleetWaitCount(simContainer, dirID, "rep.check_enrollment", 1, scenarioDeadline()); !ok {
			errs = append(errs, fmt.Sprintf("director did not forward agent enrollment to the platform (got %d frames)", c))
		} else {
			finalCount = int64(c)
			fmt.Printf("  agent enrollment forwarded to platform (%d) ✓\n", c)
		}
	}

	passed = len(errs) == 0
	return r.saveFleetResult(tc, subject, configName, startTime, finalCount, passed, errs, simContainer, subjectContainer)
}

// saveFleetResult records the verdict and, on failure, dumps short log tails of
// the simulator and director to aid diagnosis.
func (r *Runner) saveFleetResult(tc *config.TestCase, subject config.Subject, configName string,
	startTime time.Time, finalCount int64, passed bool, errs []string, simContainer, subjectContainer string) (results.RunResult, error) {

	elapsed := time.Since(startTime).Seconds()
	label := tc.Fleet.Scenario
	if passed {
		fmt.Printf("  fleet automation (%s): PASSED ✓\n", label)
	} else {
		fmt.Printf("  fleet automation (%s): FAILED ✗\n", label)
		for _, c := range []string{simContainer, subjectContainer} {
			out, _ := exec.Command("docker", "logs", "--tail", "20", c).CombinedOutput()
			fmt.Printf("  --- %s (tail) ---\n%s\n", c, string(out))
		}
	}

	result := results.RunResult{
		TestName:    tc.Name,
		Config:      configName,
		Subject:     subject.Name,
		Version:     subject.Version,
		Hardware:    hardwareID(),
		Timestamp:   startTime,
		DurationSec: elapsed,
		LinesOut:    finalCount,
		Passed:      &passed,
	}
	if !passed {
		result.FailReason = strings.Join(errs, "; ")
	}
	dir, err := r.store.Save(result, "")
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}
	fmt.Printf("  done. results → %s\n", dir)
	return result, nil
}

// runKafkaOffsetCommitRestart verifies that delivery-bound source
// acknowledgments actually persist: every produced record is delivered to a
// LIVE receiver, the subject is then restarted GRACEFULLY, and the restarted
// consumer must resume from the committed offsets instead of re-consuming
// the topic.
//
// This is the inverse of runKafkaInflightCrash: that case kills mid-delivery
// and tolerates unlimited duplicates (at-least-once permits them), which
// makes it structurally blind to "offsets are never committed at all" — a
// bug that produces zero loss and 100% over-delivery. This case closes that
// gap with a hard over-delivery ceiling (correctness.max_overdelivery_pct,
// zero = strict).
//
// Verdict: no loss (loss <= expected_loss_pct) AND over-delivery <=
// max_overdelivery_pct.
func (r *Runner) runKafkaOffsetCommitRestart(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
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

	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	extraEnv := map[string]string{}
	if cfg, ok := tc.Configurations[configName]; ok {
		maps.Copy(extraEnv, cfg.Env)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		CaseDir:          caseDir,
		TmpDir:           tmpDir,
		GeneratorImage:   r.opts.GeneratorImage,
		ReceiverImage:    r.opts.ReceiverImage,
		CollectorImage:   r.opts.CollectorImage,
		ReceiverHostPort: r.opts.ReceiverHostPort,
		ExtraSubjectEnv:  extraEnv,
		CPULimit:         r.opts.CPULimit,
		MemLimit:         r.opts.MemLimit,
	}

	orch, err := orchestrator.NewComposeRunner(runCfg)
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}

	for _, c := range []string{"bench-generator", "bench-receiver", "bench-collector", "bench-subject-" + subject.Name} {
		_ = exec.Command("docker", "rm", "-f", c).Run()
	}
	_ = orch.Down()

	startTime := time.Now()
	defer func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			_ = orch.Down()
		}
	}()

	n := tc.Generator.TotalLines
	if n <= 0 {
		return results.RunResult{}, fmt.Errorf("kafka_offset_commit_restart requires generator.total_lines > 0")
	}

	// Everything up, receiver INCLUDED — the whole stream must deliver
	// cleanly before the restart.
	fmt.Println("  starting all services (receiver UP throughout)…")
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting services: %w", err)
	}

	metricsPort, stopPortFwd, err := orch.ReceiverMetricsPort()
	if err != nil {
		return results.RunResult{}, fmt.Errorf("setting up receiver access: %w", err)
	}
	defer stopPortFwd()

	// Let the generator finish producing, then collect its final count.
	duration := tc.DurationOrDefault(60 * time.Second)
	warmup := tc.WarmupOrDefault(30 * time.Second)
	genTimeout := min(duration+warmup+2*time.Minute, r.opts.Timeout)
	if err := orch.WaitForGeneratorExit(genTimeout); err != nil {
		fmt.Printf("  (generator wait: %v)\n", err)
	}
	genStats := r.parseGeneratorStats(orch.GeneratorStdout())
	fmt.Printf("  generator sent %s lines\n", formatCount(genStats.LinesSent))
	sent := genStats.LinesSent
	if sent <= 0 {
		sent = int64(n)
	}

	// Wait for FULL delivery — the restart must land after every record
	// reached the target, so any post-restart arrival is re-consumption.
	fmt.Printf("  waiting for full delivery (receiver >= %s)…\n", formatCount(sent))
	delivered := false
	deliverDeadline := time.Now().Add(r.opts.Timeout)
	for time.Now().Before(deliverDeadline) {
		rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr == nil {
			fmt.Printf("    received: %s\n", formatCount(rm.LinesReceived))
			if rm.LinesReceived >= sent {
				delivered = true
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	if !delivered {
		return results.RunResult{}, fmt.Errorf("receiver never reached full delivery (%s) before timeout", formatCount(sent))
	}

	// Settle so the delivery-bound offset commits land at the broker; the
	// graceful stop below additionally drains pending commits on shutdown.
	fmt.Println("  full delivery reached — settling 5s, then graceful restart…")
	time.Sleep(5 * time.Second)

	if err := orch.StopServices(30*time.Second, "subject"); err != nil {
		return results.RunResult{}, fmt.Errorf("stopping subject: %w", err)
	}
	time.Sleep(3 * time.Second)
	fmt.Println("  restarting subject (must resume from committed offsets)…")
	if err := orch.UpServices("subject"); err != nil {
		return results.RunResult{}, fmt.Errorf("restarting subject: %w", err)
	}

	// Observation window: watch for re-consumption. The receiver count is
	// already at `sent`; anything beyond it is over-delivery. Stable rounds
	// mirror the in-flight case's drain.
	fmt.Println("  observing for re-consumption…")
	var lastCount int64
	stableRounds := 0
	observeDeadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(observeDeadline) {
		time.Sleep(5 * time.Second)
		rm, qerr := r.queryReceiverMetrics(metricsPort, 10*time.Second)
		if qerr != nil {
			continue
		}
		fmt.Printf("    received: %s / %s sent\n", formatCount(rm.LinesReceived), formatCount(sent))
		if rm.LinesReceived == lastCount && rm.LinesReceived > 0 {
			stableRounds++
			if stableRounds >= 4 {
				fmt.Println("    receiver stable")
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
	if sent > 0 {
		lossPct = 100.0 * (1.0 - float64(recvMetrics.LinesReceived)/float64(sent))
		if lossPct < 0 {
			lossPct = 0
		}
	}
	overPct := 0.0
	var extra int64
	if recvMetrics.LinesReceived > sent && sent > 0 {
		extra = recvMetrics.LinesReceived - sent
		overPct = 100.0 * float64(extra) / float64(sent)
	}

	var errors []string
	if lossPct > tc.Correctness.ExpectedLossPct {
		errors = append(errors, fmt.Sprintf("expected loss <= %.2f%%, got %.2f%% (%s of %s lines lost)",
			tc.Correctness.ExpectedLossPct, lossPct,
			formatCount(sent-recvMetrics.LinesReceived), formatCount(sent)))
	}
	if overPct > tc.Correctness.MaxOverDeliveryPct {
		errors = append(errors, fmt.Sprintf(
			"expected over-delivery <= %.2f%%, got %.2f%% (%s duplicate lines) — restart re-consumed records whose offsets should have been committed",
			tc.Correctness.MaxOverDeliveryPct, overPct, formatCount(extra)))
	}
	passed := len(errors) == 0

	fmt.Printf("  lines sent: %s  lines received: %s  loss: %.2f%%  over-delivery: %.2f%%",
		formatCount(sent), formatCount(recvMetrics.LinesReceived), lossPct, overPct)
	if recvMetrics.Duplicates > 0 {
		fmt.Printf("  (receiver dedup counted %s duplicates)", formatCount(recvMetrics.Duplicates))
	}
	fmt.Println()
	if passed {
		fmt.Println("  kafka offset-commit restart correctness: PASSED ✓")
	} else {
		fmt.Println("  kafka offset-commit restart correctness: FAILED ✗")
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
		LinesIn:         sent,
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
//     - read the un-read tail of input.log.1 (events written between its
//     last forwarded byte and the rotation point)
//     - read the new input.log from offset 0 (post-rotation events)
//     - NOT re-forward anything already sent before SIGTERM
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
		maps.Copy(extraEnv, cfg.Env)
	}

	// CaseDir must be absolute: the orchestrator turns it into a host bind
	// mount for sample_file replay, and Docker resolves relative bind paths
	// against the compose file's directory (the temp dir), not our cwd.
	caseDir, err := filepath.Abs(filepath.Join(r.opts.CasesDir, tc.Name))
	if err != nil {
		return results.RunResult{}, fmt.Errorf("resolving case directory: %w", err)
	}

	runCfg := orchestrator.RunConfig{
		TestCase:         tc,
		Subject:          subject,
		ConfigName:       configName,
		ConfigSrcPath:    configSrc,
		CaseDir:          caseDir,
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
	genTimeout := min(duration+warmup+2*time.Minute, r.opts.Timeout)
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

// runVerifier starts the one-shot DuckDB verifier (a profiled compose service,
// so the initial Up skipped it), waits for it to finish, and reads the verdict
// it wrote to the shared results volume. The verifier owns drain detection
// (poll-until-stable on the bucket), so the runner just waits for it to exit.
// The verdict uses the same JSON shape as the receiver's /metrics, so it maps
// through ReceiverMetrics into the RunResult unchanged. A failed verdict
// (passed=false) is a normal outcome read from the file, not an error here —
// only an infrastructure failure (verifier never finished, no verdict written)
// returns an error.
func (r *Runner) runVerifier(orch orchestrator.Orchestrator, tc *config.TestCase, tmpDir string, runDeadline time.Time) (ReceiverMetrics, error) {
	fmt.Println("  starting DuckDB verifier…")
	if err := orch.UpServices("verifier"); err != nil {
		return ReceiverMetrics{}, fmt.Errorf("starting verifier: %w", err)
	}

	// Give the verifier its own timeout plus a small buffer, capped by the run
	// deadline so a hung verifier can't overrun Options.Timeout.
	wait := tc.Verifier.TimeoutDuration() + 30*time.Second
	rem := time.Until(runDeadline)
	if rem <= 0 {
		return ReceiverMetrics{}, fmt.Errorf("run deadline (Options.Timeout) reached before the verifier could start")
	}
	if rem < wait {
		wait = rem
	}
	if err := orch.WaitForVerifierExit(wait); err != nil {
		return ReceiverMetrics{}, fmt.Errorf("%w\nverifier logs:\n%s", err, orch.Logs("verifier", 30))
	}

	verdictPath := filepath.Join(tmpDir, "verdict.json")
	data, err := os.ReadFile(verdictPath)
	if err != nil {
		return ReceiverMetrics{}, fmt.Errorf("reading verifier verdict %s: %w\nverifier logs:\n%s",
			verdictPath, err, orch.Logs("verifier", 30))
	}
	var m ReceiverMetrics
	if err := json.Unmarshal(data, &m); err != nil {
		return ReceiverMetrics{}, fmt.Errorf("parsing verifier verdict: %w", err)
	}
	passed := m.Passed != nil && *m.Passed
	fmt.Printf("  verifier: rows=%d unique=%d duplicates=%d passed=%v\n",
		m.LinesReceived, m.UniqueLines, m.Duplicates, passed)
	return m, nil
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
		for line := range strings.SplitSeq(string(data), "\n") {
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

// runSyslogVaultCertRotation verifies the director's handling of a TLS syslog
// device whose server cert is sourced from (and rotated in) HashiCorp Vault.
//
// The run proceeds in two phases:
//
//  1. ROTATION (negative half): the syslog server cert is replaced with a leaf
//     signed by an UNTRUSTED CA and re-seeded in Vault. Once the director's
//     credential cache expires (cache_ttl in the credentials store, set to a
//     low value in vmetric.yml) and its collector monitor loop detects the cert
//     change (has_vault_credentials flag), it restarts the syslog collector with
//     the bad leaf. The generator's TLS client then cannot connect — its lines
//     are lost during this window, and the receiver count stalls.
//
//  2. RECOVERY (positive half): the trusted-CA cert is restored in Vault. The
//     director detects the change again, restarts the collector with the good
//     cert, and the generator reconnects. The run drains and the loss verdict
//     is applied; expected_loss_pct in case.yaml accounts for lines dropped
//     during the bad-cert window.
//
// Requires: a `vault:` block in case.yaml (triggers Vault topology), and
// generator.total_lines > 0 (mid-delivery poll needs a finite target).
func (r *Runner) runSyslogVaultCertRotation(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	hosts := []string{"subject", "localhost"}
	var certsDir string

	return r.runKafkaMidDeliveryAction(tc, subject, midDeliveryFlow{
		verdictLabel:  "syslog TLS vault cert rotation correctness",
		actionLog:     "rotating syslog server cert to UNTRUSTED CA (generator TLS must fail), then restoring trusted cert",
		overDelivNote: "expected after the trusted cert is restored and the generator reconnects",
		totalLinesErr: "syslog_tls_vault_cert_rotation_correctness requires generator.total_lines > 0",
		extraCleanup:  []string{"bench-vault", "bench-vault-init"},
		prepare: func(tmpDir string, rc *orchestrator.RunConfig) error {
			// Generate the initial cert set. rc.TLSCertsHost makes the harness
			// mount ca.crt + server.crt/key into the subject at /opt/vmetric/certs
			// AND gives the generator ca.crt at /certs so it trusts the server.
			certsDir = filepath.Join(tmpDir, "certs")
			if _, err := orchestrator.GenerateTLSCerts(certsDir, hosts); err != nil {
				return fmt.Errorf("generating initial TLS certs: %w", err)
			}
			rc.TLSCertsHost = certsDir

			// Populate the Vault secret with the runtime-generated cert PEM.
			// PrepareVault (called by NewComposeRunner after prepare) will write
			// these bytes to the vault-secrets JSON file so Vault's initial seed
			// contains real cert material, not the case.yaml placeholders.
			certPEM, keyPEM, err := readServerCertAndKey(certsDir)
			if err != nil {
				return fmt.Errorf("reading initial certs for Vault seed: %w", err)
			}
			if rc.TestCase.Vault == nil {
				return fmt.Errorf("case %q requires a vault: block in case.yaml", tc.Name)
			}
			if rc.TestCase.Vault.Secrets == nil {
				rc.TestCase.Vault.Secrets = make(map[string]map[string]string)
			}
			rc.TestCase.Vault.Secrets["bench/syslog-tls"] = map[string]string{
				"cert": certPEM,
				"key":  keyPEM,
			}
			return nil
		},
		action: func(orch orchestrator.Orchestrator) error {
			// One context covers all docker exec / cp calls in the action.
			// A 3-minute budget is well above the sum of both phase deadlines.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			mount := tc.Vault.MountOrDefault()
			token := tc.Vault.TokenOrDefault()
			// Receiver metrics port is already forwarded by runKafkaMidDeliveryAction.
			metricsPort := orch.ReceiverMetricsPorts()["default"]

			// ---- Phase 1: UNTRUSTED cert — generator TLS must fail ----
			// RotateServerCertWrongCA overwrites server.crt/server.key in certsDir
			// with a leaf signed under a brand-new throwaway CA; ca.crt (the
			// generator's trust anchor) is left untouched.
			if err := orchestrator.RotateServerCertWrongCA(certsDir, hosts); err != nil {
				return fmt.Errorf("phase 1: rotate to untrusted cert: %w", err)
			}
			certPEM, keyPEM, err := readServerCertAndKey(certsDir)
			if err != nil {
				return fmt.Errorf("phase 1: reading rotated cert: %w", err)
			}
			if err := orchestrator.ReseedVaultSecret(ctx, "bench-vault", mount, token,
				"bench/syslog-tls", map[string]string{"cert": certPEM, "key": keyPEM}); err != nil {
				return fmt.Errorf("phase 1: reseeding vault with untrusted cert: %w", err)
			}
			fmt.Println("  phase 1: untrusted cert seeded in Vault — polling receiver for TLS rejection (stall)…")

			// Poll until the receiver's line count stops advancing: that proves
			// the director served the untrusted leaf and the generator's TLS
			// client cannot complete the handshake. Deadline: cache_ttl (5 s in
			// vmetric.yml) + monitor tick + collector restart + retry window.
			phase1Deadline := time.Now().Add(45 * time.Second)
			lastCount := int64(-1)
			stallRounds := 0
			rejected := false
			for time.Now().Before(phase1Deadline) {
				time.Sleep(2 * time.Second)
				rm, qerr := r.queryReceiverMetrics(metricsPort, 5*time.Second)
				if qerr != nil {
					continue
				}
				if rm.LinesReceived == lastCount && lastCount >= 0 {
					stallRounds++
					if stallRounds >= 4 {
						fmt.Printf("  phase 1: receiver stalled at %d lines — "+
							"untrusted cert active, generator TLS rejected ✓\n", rm.LinesReceived)
						rejected = true
						break
					}
				} else {
					stallRounds = 0
				}
				lastCount = rm.LinesReceived
			}
			if !rejected {
				return fmt.Errorf(
					"SECURITY: receiver count never stalled after wrong-CA rotation "+
						"(last count: %d) — director did not serve the untrusted cert, "+
						"or generator ignored cert validation; check debug.console.status: true logs",
					lastCount)
			}

			// ---- Phase 2: TRUSTED cert restored — recovery ----
			if err := orchestrator.RotateServerCert(certsDir, hosts); err != nil {
				return fmt.Errorf("phase 2: rotate to trusted cert: %w", err)
			}
			certPEM, keyPEM, err = readServerCertAndKey(certsDir)
			if err != nil {
				return fmt.Errorf("phase 2: reading restored cert: %w", err)
			}
			if err := orchestrator.ReseedVaultSecret(ctx, "bench-vault", mount, token,
				"bench/syslog-tls", map[string]string{"cert": certPEM, "key": keyPEM}); err != nil {
				return fmt.Errorf("phase 2: reseeding vault with trusted cert: %w", err)
			}
			// Allow time for cache_ttl expiry + monitor tick + collector restart.
			// Recovery is confirmed by the final loss verdict: if the director
			// does not recover, lines_out < expected and loss_percent > ceiling.
			fmt.Println("  phase 2: trusted cert restored in Vault — waiting 25s for director to detect and recover…")
			time.Sleep(25 * time.Second)
			fmt.Println("  phase 2: director should have restarted with the restored cert — delivery resuming")
			return nil
		},
	})
}

// readServerCertAndKey reads server.crt and server.key from dir as PEM strings.
// Both files are written by GenerateTLSCerts and overwritten by RotateServerCert*.
func readServerCertAndKey(dir string) (certPEM, keyPEM string, err error) {
	certBytes, err := os.ReadFile(filepath.Join(dir, "server.crt"))
	if err != nil {
		return "", "", fmt.Errorf("reading server.crt: %w", err)
	}
	keyBytes, err := os.ReadFile(filepath.Join(dir, "server.key"))
	if err != nil {
		return "", "", fmt.Errorf("reading server.key: %w", err)
	}
	return string(certBytes), string(keyBytes), nil
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
