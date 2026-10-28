package runner

// director_cluster_scenario_correctness: drive a director cluster through a
// list of steps (cluster.scenario.steps) under a steady load, measure what
// the cluster does at each step, and check at the end that every record the
// cluster acknowledged arrived.
//
// The load is the http_ack generator. It posts numbered lines to the nodes'
// HTTP device and counts a line only once a post that carried it was
// answered 2xx, retrying a refused post on the next node. The receiver
// tracks which numbered lines arrived (RECEIVER_TRACK_SEQ), so loss is
// exact: acknowledged lines that never arrived. Duplicates are reported.
// A case with several receivers (`receivers:`, one per target of the
// route) needs every acknowledged line at each of them, and its delivery
// steps, timelines and latencies read the first one.
//
// A prober reads every node's /health once a second (cluster_probe.go). The
// waits read it to see a leader, a healthy cluster or a lost one, and the
// leadership analysis counts the time two nodes both named themselves the
// leader.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"github.com/VirtualMetric/PipeBench/internal/orchestrator"
	"github.com/VirtualMetric/PipeBench/internal/results"
)

// wait_delivery counts only lines sent at least deliveryMargin after the
// disruption, and needs deliveryLines of them: a tenth of a second of the
// cases' load of 1,000 lines a second.
const (
	deliveryMargin = time.Second
	deliveryLines  = 100
)

// scenarioStepReport is what one step did, for the report.
type scenarioStepReport struct {
	Index      int       `json:"index"`
	Do         string    `json:"do"`
	Node       int       `json:"node,omitempty"`
	Start      time.Time `json:"start"`
	DurationMs int64     `json:"duration_ms"`
	OK         bool      `json:"ok"`
	Soft       bool      `json:"soft,omitempty"`
	Detail     string    `json:"detail,omitempty"`
}

// scenarioEvent is a disruption: delivery and latency are measured after it.
type scenarioEvent struct {
	Step int       `json:"step"`
	Do   string    `json:"do"`
	Node int       `json:"node,omitempty"`
	At   time.Time `json:"at"`
	// Filled in at the end from the receiver's timeline and latency.
	GapMs        int64         `json:"delivery_gap_ms"`
	ResumeMs     int64         `json:"delivery_resumed_after_ms"`
	LatencyAfter *latencyStats `json:"latency_of_lines_sent_next_60s,omitempty"`
}

type scenarioMark struct {
	Label string    `json:"label"`
	At    time.Time `json:"at"`
}

// pendingCount is one count_pending step: the lines sent before a mark that
// were still on their way when the step ran.
type pendingCount struct {
	Name       string    `json:"name"`
	Mark       string    `json:"mark"`
	SentBefore time.Time `json:"sent_before"`
	CountedAt  time.Time `json:"counted_at"`
	// Arrived is how many lines sent before the mark had arrived at the step.
	Arrived int64 `json:"arrived_at_step"`
	// ArrivedLater is how many more arrived after the step, counted after
	// the drain: the lines that were still on their way at the step. It is
	// nil when the count after the drain could not be read.
	ArrivedLater *int64 `json:"arrived_later,omitempty"`
}

// arrivedCheck is one expect_arrived step: the lines sent between two marks
// that must, or must not, reach a receiver. The step only names the window.
// The run counts after the drain, so that a line still on its way at the step
// counts too.
type arrivedCheck struct {
	Name       string    `json:"name"`
	Receiver   string    `json:"receiver,omitempty"`
	SentAfter  string    `json:"sent_after"`
	SentBefore string    `json:"sent_before"`
	From       time.Time `json:"from"`
	To         time.Time `json:"to"`
	Min        *int64    `json:"min,omitempty"`
	Max        *int64    `json:"max,omitempty"`
	CompareTo  string    `json:"compare_to,omitempty"`
	MinShare   *float64  `json:"min_share,omitempty"`
	MaxShare   *float64  `json:"max_share,omitempty"`
	Soft       bool      `json:"soft,omitempty"`
	// Arrived is how many of the window's lines reached the receiver, counted
	// after the drain. It is nil when the receiver could not be read.
	Arrived *int64 `json:"arrived,omitempty"`
	// ComparedArrived is how many of the window's lines reached the
	// compare_to receiver, when the step names one.
	ComparedArrived *int64 `json:"compared_arrived,omitempty"`

	port        int
	comparePort int
}

// judgeArrived returns why an expect_arrived count fails its bounds, or "".
// compared is what the compare_to receiver got of the window, and is read
// only when the step names one.
func judgeArrived(c arrivedCheck, arrived, compared int64) string {
	at := ""
	if c.Receiver != "" {
		at = " at receiver " + c.Receiver
	}
	switch {
	case c.Min != nil && arrived < *c.Min:
		return fmt.Sprintf("%s: %d lines sent between marks %q and %q arrived%s, fewer than min %d", c.Name, arrived, c.SentAfter, c.SentBefore, at, *c.Min)
	case c.Max != nil && arrived > *c.Max:
		return fmt.Sprintf("%s: %d lines sent between marks %q and %q arrived%s, more than max %d", c.Name, arrived, c.SentAfter, c.SentBefore, at, *c.Max)
	}
	if c.CompareTo == "" || (c.MinShare == nil && c.MaxShare == nil) {
		return ""
	}
	// No line of the window reached the receiver the share is of, so there
	// is nothing to compare with: the window tested nothing.
	if compared == 0 {
		return fmt.Sprintf("%s: no line sent between marks %q and %q reached receiver %s, so the share%s is not known", c.Name, c.SentAfter, c.SentBefore, c.CompareTo, at)
	}
	share := float64(arrived) / float64(compared)
	switch {
	case c.MinShare != nil && share < *c.MinShare:
		return fmt.Sprintf("%s: %d lines sent between marks %q and %q arrived%s, %.3f of the %d that reached receiver %s, less than min_share %v",
			c.Name, arrived, c.SentAfter, c.SentBefore, at, share, compared, c.CompareTo, *c.MinShare)
	case c.MaxShare != nil && share > *c.MaxShare:
		return fmt.Sprintf("%s: %d lines sent between marks %q and %q arrived%s, %.3f of the %d that reached receiver %s, more than max_share %v",
			c.Name, arrived, c.SentAfter, c.SentBefore, at, share, compared, c.CompareTo, *c.MaxShare)
	}
	return ""
}

// arrivedSince is how many lines arrived between a count_pending step and
// the end of the run, from the two counts of lines sent before its mark. The
// receiver counts each line once, at its first arrival, so the count cannot
// fall; a smaller second count would mean a restarted receiver, and reads 0.
func arrivedSince(atStep, atEnd int64) int64 {
	return max(atEnd-atStep, 0)
}

// markAt returns the time of the latest mark with the label.
func (sr *scenarioRun) markAt(label string) (time.Time, bool) {
	for i := len(sr.marks) - 1; i >= 0; i-- {
		if sr.marks[i].Label == label {
			return sr.marks[i].At, true
		}
	}
	return time.Time{}, false
}

type latencyStats struct {
	Count     int     `json:"count"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
	P99Ms     float64 `json:"p99_ms"`
	MaxMs     float64 `json:"max_ms"`
	MaxSentNs int64   `json:"max_sent_ns,omitempty"`
}

type seqCheckResult struct {
	Acked          int64    `json:"acked"`
	ReceivedAcked  int64    `json:"received_of_acked"`
	Missing        int64    `json:"missing"`
	Extra          int64    `json:"extra"`
	Unique         int64    `json:"unique"`
	Duplicates     int64    `json:"duplicates"`
	Unparsed       int64    `json:"unparsed"`
	UnexpectedConn []string `json:"unexpected_conns,omitempty"`
	Conns          map[string]struct {
		Acked         int64      `json:"acked"`
		Received      int64      `json:"received"`
		Missing       int64      `json:"missing"`
		Extra         int64      `json:"extra"`
		MissingRanges [][2]int64 `json:"missing_ranges,omitempty"`
	} `json:"conns"`
}

type receiverTimeline struct {
	StartNs  int64   `json:"start_ns"`
	BucketMs int64   `json:"bucket_ms"`
	Unique   []int32 `json:"unique"`
	All      []int32 `json:"all"`
}

// generatorAck is the http_ack summary in the generator's final JSON.
type generatorAck struct {
	LinesSent int64 `json:"lines_sent"`
	Ack       *struct {
		Targets        []string         `json:"targets"`
		LinesAcked     int64            `json:"lines_acked"`
		LinesUnacked   int64            `json:"lines_unacked"`
		Requests       int64            `json:"requests"`
		Failures       int64            `json:"failures"`
		Failovers      int64            `json:"failovers"`
		StatusCounts   map[string]int64 `json:"status_counts"`
		AckedPerConn   map[string]int64 `json:"acked_per_conn"`
		AckedPerTarget map[string]int64 `json:"acked_per_target"`
		MaxAckStallMs  int64            `json:"max_ack_stall_ms"`
		AckLatency     *struct {
			Count int   `json:"count"`
			P50   int64 `json:"p50"`
			P95   int64 `json:"p95"`
			P99   int64 `json:"p99"`
			Max   int64 `json:"max"`
		} `json:"ack_latency_ms"`
		AckLatencyMaxPerSec []int64 `json:"ack_latency_max_ms_per_second"`
	} `json:"ack"`
}

// genProgress is the last PROGRESS line the http_ack generator wrote.
type genProgress struct {
	At       time.Time
	Acked    int64
	Failures int64
	Pending  int64
	// LastAck is when the client last got a 2xx; zero before the first one.
	LastAck time.Time
	// AckedPerTarget is how many records each target confirmed, by the
	// target as the generator's target list names it. It is nil on the
	// progress lines of a generator older than PipeBench v0.4.17, which did
	// not report it.
	AckedPerTarget map[string]int64
}

var (
	progressRE = regexp.MustCompile(`PROGRESS t_ms=(\d+) acked=(\d+) unacked=\d+ requests=\d+ failures=(\d+) failovers=\d+ pending=(\d+) last_ack_ns=(\d+)`)
	// The counts per target are a JSON object, which holds no space, ahead
	// of last_error, whose text can hold anything.
	progressPerTargetRE = regexp.MustCompile(` acked_per_target=(\{[^ ]*\})`)
)

type scenarioRun struct {
	r       *Runner
	ctx     context.Context
	tc      *config.TestCase
	sc      *config.ScenarioConfig
	subject config.Subject
	orch    orchestrator.Orchestrator

	containers []string     // every node container, index = node-1, spares included
	active     map[int]bool // the nodes that are cluster members now
	size       int          // the cluster size the node configs name
	tmpDir     string
	configSrc  string
	network    string
	// receivers are the run's receivers, in the case's order. recvPort is
	// the first one's port: the delivery steps, the timelines and the
	// latencies read that receiver.
	receivers []scenarioReceiver
	recvPort  int

	probe *clusterProbe

	// links holds the link faults the steps have put in force, and filled
	// the nodes whose disk a fill_disk step filled.
	links  *linkFaults
	filled map[int]bool

	names        map[string]int
	disconnected map[int]bool
	// down holds the members a step took out of service on purpose (killed,
	// stopped, paused). Every other member must be up for the cluster to be
	// healthy, so a probe round taken while a start was still under way
	// cannot pass for a healthy cluster.
	down           map[int]bool
	lastDisruption time.Time
	// disruptionAcked is the client's acknowledged line count at the last
	// disruption. disruptionAckedOK is false when it could not be read.
	disruptionAcked   int64
	disruptionAckedOK bool
	upAt              time.Time
	loadStarted       time.Time

	steps    []scenarioStepReport
	events   []scenarioEvent
	marks    []scenarioMark
	pending  []pendingCount
	arrived  []arrivedCheck
	measures map[string]float64
	counts   map[string]int64
	failures []string
	notes    []string

	imageID     string
	imageLabels map[string]string
}

// scenarioReceiver is one receiver of a scenario run: the case's one
// `receiver:` (ID empty), or one of its `receivers:`.
type scenarioReceiver struct {
	ID        string
	Container string
	Port      int // the host port of its /metrics and query endpoints
}

// scenarioReceivers lists the run's receivers in the case's order, with the
// host ports the orchestrator gave them (ports, keyed by id, or "default"
// for the one receiver of a case without `receivers:`).
func scenarioReceivers(tc *config.TestCase, ports map[string]int, defaultPort int) []scenarioReceiver {
	if !tc.MultiReceiver() {
		port := defaultPort
		if p, ok := ports["default"]; ok {
			port = p
		}
		return []scenarioReceiver{{Container: "bench-receiver", Port: port}}
	}
	out := make([]scenarioReceiver, 0, len(tc.Receivers))
	for _, rc := range tc.Receivers {
		out = append(out, scenarioReceiver{ID: rc.ID, Container: "bench-receiver-" + rc.ID, Port: ports[rc.ID]})
	}
	return out
}

// receiverByID returns the receiver with that id. An empty id is the case's
// one receiver.
func (sr *scenarioRun) receiverByID(id string) (scenarioReceiver, bool) {
	for _, rc := range sr.receivers {
		if rc.ID == id {
			return rc, true
		}
	}
	return scenarioReceiver{}, false
}

// receiverContainers names the run's receiver containers.
func receiverContainers(recvs []scenarioReceiver) []string {
	out := make([]string, len(recvs))
	for i, rc := range recvs {
		out[i] = rc.Container
	}
	return out
}

func (r *Runner) runClusterScenario(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	subject = r.applySubjectOverrides(subject)
	sc := tc.Cluster.Scenario

	for _, capName := range tc.Requires {
		if !subject.HasCapability(capName) {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare capability %q required by case %q",
				subject.Name, capName, tc.Name)
		}
	}

	// Every node gets a CPU and memory cap. A case or the command line can
	// set them; otherwise each node gets one core and 1 GiB.
	if r.runCPULimit == "" {
		r.runCPULimit = "1"
	}
	if r.runMemLimit == "" {
		r.runMemLimit = "1g"
	}

	fmt.Printf("→ test=%s  subject=%s  version=%s  config=%s  (cluster scenario: %d nodes + %d spare, %d steps, node limits %s CPU / %s)\n",
		tc.Name, subject.Name, subject.Version, configName, tc.Cluster.Nodes, sc.SpareNodes, len(sc.Steps), r.runCPULimit, r.runMemLimit)
	imageID, imageLabels := imageIdentity(subject.ImageRef())
	fmt.Printf("  image %s is %s %v\n", subject.ImageRef(), imageID, imageLabels)

	srcCfg, err := tc.ConfigFilePath(r.opts.CasesDir, configName, subject)
	if err != nil {
		return results.RunResult{}, err
	}
	if srcCfg, err = filepath.Abs(srcCfg); err != nil {
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

	orch, err := orchestrator.NewComposeRunner(r.ctx, orchestrator.RunConfig{
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
		CPULimit:         r.runCPULimit,
		MemLimit:         r.runMemLimit,
	})
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}

	containers := clusterNodeContainers(subject.Name, tc.Cluster.Nodes+sc.SpareNodes)
	recvs := scenarioReceivers(tc, orch.ReceiverMetricsPorts(), r.opts.ReceiverHostPort)
	// bench-receiver stays on the list in every case, so that one an
	// interrupted earlier run left behind goes too.
	extras := []string{"bench-generator", "bench-receiver", "bench-collector", "bench-toxiproxy", "bench-witness-init"}
	if tc.MultiReceiver() {
		extras = append(extras, receiverContainers(recvs)...)
	}
	removeContainers := func() {
		for _, c := range append(append([]string{}, extras...), containers...) {
			_ = exec.Command("docker", "unpause", c).Run()
			_ = exec.Command("docker", "rm", "-f", c).Run()
		}
	}
	removeContainers()
	_ = orch.Down()
	defer func() {
		if !r.opts.NoCleanup {
			fmt.Println("  tearing down…")
			// Services under a compose profile (the deferred generator, spare
			// nodes) and paused containers are removed by name first, so
			// `down` can remove the network.
			removeContainers()
			_ = orch.Down()
		}
	}()

	sr := &scenarioRun{
		r: r, ctx: r.ctx, tc: tc, sc: sc, subject: subject, orch: orch,
		containers: containers, active: map[int]bool{}, size: tc.Cluster.Nodes,
		tmpDir: tmpDir, configSrc: srcCfg, receivers: recvs, recvPort: recvs[0].Port,
		names: map[string]int{}, disconnected: map[int]bool{}, down: map[int]bool{}, measures: map[string]float64{},
		counts: map[string]int64{}, imageID: imageID, imageLabels: imageLabels, filled: map[int]bool{},
	}
	for i := 1; i <= tc.Cluster.Nodes; i++ {
		sr.active[i] = true
	}

	startTime := time.Now()
	fmt.Printf("  starting %d director nodes…\n", tc.Cluster.Nodes)
	if err := orch.Up(); err != nil {
		return results.RunResult{}, fmt.Errorf("starting cluster: %w", err)
	}
	sr.upAt = time.Now()
	sr.lastDisruption = sr.upAt
	sr.network = containerNetwork(containers[0])
	sr.links = newLinkFaults(r.opts.NettoolImage, sr.network)
	// Whatever happens, no link fault or full disk outlives the run: with
	// --no-cleanup the containers stay, and so would their rules.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		sr.restoreAtEnd(ctx)
	}()

	sr.probe = newClusterProbe(containers, sc.ProbeIntervalOrDefault())
	sr.probe.start(r.ctx)
	defer sr.probe.stop()

	for i, st := range sc.Steps {
		if err := r.ctx.Err(); err != nil {
			return results.RunResult{}, fmt.Errorf("interrupted: %w", err)
		}
		if err := sr.runStep(i, st); err != nil {
			return results.RunResult{}, err
		}
	}
	sr.restoreAtEnd(r.ctx)
	if sr.loadStarted.IsZero() {
		sr.notes = append(sr.notes, "no step started the load, so it ran from the end of the steps")
		if err := sr.startLoad(); err != nil {
			return results.RunResult{}, err
		}
	}

	report, verdict, err := sr.finish()
	if err != nil {
		return results.RunResult{}, err
	}
	sr.probe.stop()

	// Keep every container's log next to the report: the evidence for a
	// failure is in the nodes' logs, and teardown removes the containers.
	logged := append(append(append([]string{}, containers...), "bench-generator"), receiverContainers(recvs)...)
	if dir, err := r.saveScenarioLogs(tc.Name, logged); err == nil {
		report.LogsDir = dir
		fmt.Printf("  container logs → %s\n", dir)
	} else {
		fmt.Printf("  (could not save the container logs: %v)\n", err)
	}

	metrics, metricsCSVSrc := r.harvestResourceMetrics(orch, tmpDir)
	sysCPUs, sysMemMB := getSystemInfo()

	passed := verdict.passed
	if passed {
		fmt.Printf("  director cluster scenario: PASSED ✓\n")
	} else {
		fmt.Printf("  director cluster scenario: FAILED ✗\n")
		for _, f := range verdict.reasons {
			fmt.Printf("    - %s\n", f)
		}
		for i, c := range containers {
			if !sr.active[i+1] && i >= tc.Cluster.Nodes {
				continue
			}
			out, _ := exec.Command("docker", "logs", "--tail", "15", c).CombinedOutput()
			fmt.Printf("  --- %s (tail) ---\n%s\n", c, string(out))
		}
	}

	result := results.RunResult{
		TestName:        tc.Name,
		Config:          configName,
		Subject:         subject.Name,
		Version:         subject.Version,
		Hardware:        hardwareID(),
		Timestamp:       startTime,
		DurationSec:     time.Since(startTime).Seconds(),
		LinesIn:         report.Acked,
		LinesOut:        report.Received,
		UniqueOut:       report.Unique,
		LatencyP50Ms:    report.Latency.P50Ms,
		LatencyP95Ms:    report.Latency.P95Ms,
		LatencyP99Ms:    report.Latency.P99Ms,
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
		Passed:          &passed,
		Scenario:        report.summary(),
	}
	if report.Acked > 0 {
		result.LossPercent = 100 * float64(report.Missing) / float64(report.Acked)
	}
	if !passed {
		result.FailReason = strings.Join(verdict.reasons, "; ")
	}

	if path, err := r.writeScenarioReport(tc.Name, report); err == nil {
		fmt.Printf("  scenario report → %s\n", path)
	} else {
		fmt.Printf("  (could not write the scenario report: %v)\n", err)
	}
	dir, err := r.saveResult(result, metricsCSVSrc)
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}
	fmt.Printf("  done. results → %s\n", dir)
	return result, nil
}

// ===== steps ==================================================================

func (sr *scenarioRun) runStep(i int, st config.ScenarioStep) error {
	rep := scenarioStepReport{Index: i, Do: st.Do, Start: time.Now(), Soft: st.Soft}
	fmt.Printf("  [step %d] %s%s\n", i, st.Do, describeStepArgs(st))
	ok, detail, err := sr.doStep(i, st, &rep)
	if err != nil {
		return err
	}
	rep.OK = ok
	rep.Detail = detail
	rep.DurationMs = time.Since(rep.Start).Milliseconds()
	sr.steps = append(sr.steps, rep)
	switch {
	case ok:
		if detail != "" {
			fmt.Printf("    ✓ %s\n", detail)
		}
	case st.Soft:
		fmt.Printf("    (soft) %s\n", detail)
		sr.notes = append(sr.notes, fmt.Sprintf("step %d (%s): %s", i, st.Do, detail))
	default:
		fmt.Printf("    ✗ %s\n", detail)
		sr.failures = append(sr.failures, fmt.Sprintf("step %d (%s): %s", i, st.Do, detail))
	}
	return nil
}

func describeStepArgs(st config.ScenarioStep) string {
	var parts []string
	add := func(k, v string) {
		if v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	add("node", st.Node)
	add("as", st.As)
	add("within", st.Within)
	add("for", st.For)
	add("record", st.Record)
	add("label", st.Label)
	add("signal", st.Signal)
	add("recover", st.Recover)
	add("proxy", st.Proxy)
	add("latency", st.Latency)
	add("log", st.Log)
	add("logs", strings.Join(st.Logs, " | "))
	add("since", st.Since)
	add("sent_before", st.SentBefore)
	add("receiver", st.Receiver)
	add("peers", strings.Join(st.PeerList(), ","))
	add("loss", st.Loss)
	add("jitter", st.Jitter)
	add("leave", st.Leave)
	if st.OneWay {
		add("one_way", "true")
	}
	if st.EmptyDisk {
		add("empty_disk", "true")
	}
	if len(st.Groups) > 0 {
		parts := make([]string, len(st.Groups))
		for i, g := range st.Groups {
			parts[i] = strings.Join(g, ",")
		}
		add("groups", strings.Join(parts, " | "))
	}
	if st.Nodes > 0 {
		add("nodes", strconv.Itoa(st.Nodes))
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, " ") + ")"
}

// doStep runs one step. It returns whether the step's expectation held, a
// line for the report, and an error only when the run must stop (an
// interrupt).
func (sr *scenarioRun) doStep(i int, st config.ScenarioStep, rep *scenarioStepReport) (bool, string, error) {
	switch st.Do {
	case "wait_ready", "wait_healthy":
		within := st.DurationOr("within", 180*time.Second)
		since := sr.lastDisruption
		if st.Do == "wait_ready" {
			since = sr.upAt
		}
		round, ok, err := sr.waitRound(within, func(r healthRound) bool { return sr.healthy(r) })
		if err != nil {
			return false, "", err
		}
		if !ok {
			return false, fmt.Sprintf("the cluster was not healthy within %s: %s", within, describeRound(round)), nil
		}
		ms := time.Since(since).Milliseconds()
		sr.record(st.Record, float64(ms))
		leader, _ := agreedLeader(round)
		return true, fmt.Sprintf("healthy after %s, leader node %d", fmtMs(ms), leader), nil

	case "start_load":
		if err := sr.startLoad(); err != nil {
			return false, "", err
		}
		return true, "load started", nil

	case "sleep":
		if err := sleepCtx(sr.ctx, st.DurationOr("for", 0)); err != nil {
			return false, "", fmt.Errorf("interrupted: %w", err)
		}
		return true, "", nil

	case "mark":
		sr.marks = append(sr.marks, scenarioMark{Label: st.Label, At: time.Now()})
		return true, "", nil

	case "kill", "stop", "start", "restart", "pause", "unpause", "disconnect", "connect":
		return sr.nodeAction(i, st, rep)

	case "cut_link", "shape_link", "restore_link", "split":
		return sr.linkAction(i, st, rep)

	case "fill_disk", "free_disk":
		return sr.diskAction(i, st, rep)

	case "wait_leader":
		within := st.DurationOr("within", 90*time.Second)
		since := sr.lastDisruption
		var exclude int
		if st.Node != "" {
			n, err := sr.resolveNode(st.Node)
			if err != nil {
				return false, err.Error(), nil
			}
			exclude = n
		}
		// With groups, a leader agreed within one group is enough: the sides of
		// a split cannot vote for each other.
		var groups [][]int
		for _, g := range st.Groups {
			var ns []int
			for _, sel := range g {
				n, err := sr.resolveNode(sel)
				if err != nil {
					return false, err.Error(), nil
				}
				ns = append(ns, n)
			}
			groups = append(groups, ns)
		}
		leaderOf := func(r healthRound) (int, []int, bool) {
			if len(groups) == 0 {
				l, ok := agreedLeader(r)
				return l, nil, ok
			}
			return oneSideLeader(r, groups)
		}
		round, ok, err := sr.waitRound(within, func(r healthRound) bool {
			l, _, ok := leaderOf(r)
			return ok && l != exclude && !sr.disconnected[l]
		})
		if err != nil {
			return false, "", err
		}
		if !ok {
			if len(groups) > 0 {
				return false, fmt.Sprintf("not exactly one of the groups %s agreed on a leader within %s: %s", describeSides(groups), within, describeRound(round)), nil
			}
			return false, fmt.Sprintf("no agreed leader within %s: %s", within, describeRound(round)), nil
		}
		ms := round.At.Sub(since).Milliseconds()
		leader, side, _ := leaderOf(round)
		sr.record(st.Record, float64(ms))
		detail := fmt.Sprintf("node %d leads, first seen %s after the disruption", leader, fmtMs(ms))
		if side != nil {
			detail = fmt.Sprintf("node %d leads the side %v, first seen %s after the disruption", leader, side, fmtMs(ms))
		}
		at, found, err := firstLogLineSince(sr.containers[leader-1], "became leader on", since)
		if err != nil {
			return false, fmt.Sprintf("%s, but its log could not be read for when it logged the election: %v", detail, err), nil
		}
		if found {
			logMs := at.Sub(since).Milliseconds()
			sr.record(st.Record+"_log", float64(logMs))
			detail += fmt.Sprintf(" (its log says %s)", fmtMs(logMs))
		}
		return true, detail, nil

	case "expect_no_leader":
		// The cluster must stop naming a leader within `within`, and then
		// name none for `for`. A node that lost its peers can keep naming
		// itself for a few seconds; the time it took is recorded.
		within := st.DurationOr("within", 30*time.Second)
		hold := st.DurationOr("for", 30*time.Second)
		since := sr.lastDisruption
		round, ok, err := sr.waitRound(within, func(r healthRound) bool { return len(claimedLeaders(r)) == 0 })
		if err != nil {
			return false, "", err
		}
		if !ok {
			return false, fmt.Sprintf("a node still named itself the leader %s after the disruption: %s", within, describeRound(round)), nil
		}
		lostMs := round.At.Sub(since).Milliseconds()
		sr.record(st.Record, float64(lostMs))
		end := time.Now().Add(hold)
		for time.Now().Before(end) {
			if err := sleepCtx(sr.ctx, time.Second); err != nil {
				return false, "", fmt.Errorf("interrupted: %w", err)
			}
			if r, ok := sr.probe.latest(); ok {
				if claims := claimedLeaders(r); len(claims) > 0 {
					return false, fmt.Sprintf("node(s) %v named themselves the leader while the cluster should have none: %s", claims, describeRound(r)), nil
				}
			}
		}
		return true, fmt.Sprintf("no leader after %s, and none for %s", fmtMs(lostMs), hold), nil

	case "wait_delivery":
		// Delivery is back once lines sent after the disruption arrive. Only
		// lines sent at least deliveryMargin after it count, so that lines
		// already on their way, and a small difference between this host's
		// clock and the containers' clock, cannot satisfy the wait.
		within := st.DurationOr("within", 90*time.Second)
		since := sr.lastDisruption
		deadline := time.Now().Add(within)
		for {
			ls, err := sr.receiverLatency(since.Add(deliveryMargin), time.Time{})
			if err == nil && ls.Count >= deliveryLines {
				ms := time.Since(since).Milliseconds()
				sr.record(st.Record, float64(ms))
				return true, fmt.Sprintf("lines sent after the disruption arrive (seen %s after it)", fmtMs(ms)), nil
			}
			if time.Now().After(deadline) {
				return false, fmt.Sprintf("no line sent after the disruption arrived within %s", within), nil
			}
			if err := sleepCtx(sr.ctx, 250*time.Millisecond); err != nil {
				return false, "", fmt.Errorf("interrupted: %w", err)
			}
		}

	case "expect_writes_stop":
		// The cluster must stop acknowledging writes within `within` and
		// acknowledge none for `for`. Writes the director keeps on its own
		// disk until the cluster comes back are acknowledged, so the
		// acknowledgements that came after the disruption are recorded.
		within := st.DurationOr("within", 60*time.Second)
		hold := st.DurationOr("for", 20*time.Second)
		since := sr.lastDisruption
		startProg, _ := readGeneratorProgress()
		// Count from the disruption when the client's count at that moment
		// is known, and from the start of this step otherwise.
		base, from := startProg.Acked, "since this step began"
		if sr.disruptionAckedOK {
			base, from = sr.disruptionAcked, "since the disruption"
		}
		deadline := since.Add(within)
		var flatSince time.Time
		var last genProgress
		for {
			p, ok := readGeneratorProgress()
			if ok {
				if p.Acked != last.Acked || flatSince.IsZero() {
					flatSince = time.Now()
					last = p
				}
				if time.Since(flatSince) >= hold {
					after := last.Acked - base
					// The writes stopped with the client's last 2xx, which can
					// be well before this step saw the count stand still.
					stopped := flatSince
					if !last.LastAck.IsZero() {
						stopped = last.LastAck
					}
					stoppedMs := max(stopped.Sub(since).Milliseconds(), 0)
					sr.record(st.Record, float64(stoppedMs))
					sr.recordCount(st.Record, "_acked_after", after)
					return true, fmt.Sprintf("writes stopped %s after the disruption (the client's last 2xx); %d lines were acknowledged %s", fmtMs(stoppedMs), after, from), nil
				}
			}
			if time.Now().After(deadline.Add(hold)) {
				sr.recordCount(st.Record, "_acked_after", last.Acked-base)
				return false, fmt.Sprintf("the cluster kept acknowledging writes: %d lines %s", last.Acked-base, from), nil
			}
			if err := sleepCtx(sr.ctx, 500*time.Millisecond); err != nil {
				return false, "", fmt.Errorf("interrupted: %w", err)
			}
		}

	case "expect_delivery_stops":
		// No line may reach the receiver for `for`, starting within `within`
		// of the disruption: a cluster without a quorum must not pass records
		// on. A director in crash-resistant mode goes on acknowledging what
		// it can keep on its own disk, so this, not expect_writes_stop, is
		// what shows that the cluster stopped. Lines that were already past
		// the cluster's queues may still arrive at first.
		within := st.DurationOr("within", 60*time.Second)
		hold := st.DurationOr("for", 20*time.Second)
		since := sr.lastDisruption
		deadline := since.Add(within).Add(hold)
		var last time.Time
		var after int64
		for {
			if tl, err := sr.receiverTimeline(); err == nil {
				last, after = arrivalsAfter(tl, since)
				if time.Since(last) >= hold {
					stoppedMs := max(last.Sub(since).Milliseconds(), 0)
					sr.record(st.Record, float64(stoppedMs))
					sr.recordCount(st.Record, "_lines_after", after)
					return true, fmt.Sprintf("deliveries stopped %s after the disruption; %d lines arrived after it", fmtMs(stoppedMs), after), nil
				}
			}
			if time.Now().After(deadline) {
				sr.recordCount(st.Record, "_lines_after", after)
				return false, fmt.Sprintf("lines kept arriving: %d since the disruption, the last %s ago", after, fmtMs(time.Since(last).Milliseconds())), nil
			}
			if err := sleepCtx(sr.ctx, time.Second); err != nil {
				return false, "", fmt.Errorf("interrupted: %w", err)
			}
		}

	case "expect_writes_continue":
		hold := st.DurationOr("for", 20*time.Second)
		// The window counts from a progress line read at its start. Without
		// one, the count would include every line confirmed before the window.
		first, ok := readProgressBaseline(sr.ctx, readGeneratorProgress, 5, 500*time.Millisecond)
		if !ok {
			if sr.ctx.Err() != nil {
				return false, "", fmt.Errorf("interrupted: %w", sr.ctx.Err())
			}
			return false, "could not read the client's progress at the start of the window", nil
		}
		gained, maxFlat, err := watchWrites(sr.ctx, first, hold, 500*time.Millisecond, readGeneratorProgress)
		if err != nil {
			return false, "", fmt.Errorf("interrupted: %w", err)
		}
		sr.record(st.Record, float64(maxFlat.Milliseconds()))
		// How many lines the client got confirmed in the window: with the
		// receiver paused, the records a node holds and still has to deliver.
		sr.recordCount(st.Record, "_lines_acked", gained)
		var gap time.Duration
		if st.Gap != "" {
			gap = st.DurationOr("gap", 10*time.Second)
		}
		return judgeWrites(gained, maxFlat, hold, gap)

	case "expect_delivery_continues":
		// Lines sent in the window must keep arriving: the count of lines
		// sent after the window began (and deliveryMargin, so lines already
		// on their way do not count) that reached the receiver must grow at
		// least once every gap. wait_delivery is satisfied by one handful of
		// lines; this shows that delivery went on.
		hold := st.DurationOr("for", 30*time.Second)
		gap := st.DurationOr("gap", 10*time.Second)
		start := time.Now()
		res, err := watchDelivery(sr.ctx, start, hold, gap, 250*time.Millisecond, func() (int64, error) {
			ls, err := sr.receiverLatency(start.Add(deliveryMargin), time.Time{})
			return int64(ls.Count), err
		})
		if err != nil {
			return false, "", err
		}
		sr.record(st.Record, float64(res.longest.Milliseconds()))
		sr.recordCount(st.Record, "_lines", res.lines)
		if !res.ok {
			return false, res.detail, nil
		}
		return true, fmt.Sprintf("%d lines sent in the window arrived in %s (longest stall %s)", res.lines, hold, fmtMs(res.longest.Milliseconds())), nil

	case "witness_down", "witness_up":
		return sr.witnessAction(i, st)

	case "proxy_latency", "proxy_down", "proxy_up":
		return sr.proxyAction(i, st)

	case "receiver_pause", "receiver_unpause":
		// A paused receiver keeps its counts, which a stopped one would lose,
		// and the targets that send to it build a backlog meanwhile.
		rc, ok := sr.receiverByID(st.Receiver)
		if !ok {
			return false, fmt.Sprintf("receiver %q is not one of the case's receivers", st.Receiver), nil
		}
		verb := strings.TrimPrefix(st.Do, "receiver_")
		if err := dockerCmd(sr.ctx, 30*time.Second, verb, rc.Container); err != nil {
			return false, fmt.Sprintf("docker %s %s: %v", verb, rc.Container, err), nil
		}
		sr.disrupt(i, st.Do, 0)
		if rc.ID != "" {
			return true, fmt.Sprintf("receiver %s %sd", rc.ID, verb), nil
		}
		return true, "", nil

	case "scale_to":
		return sr.scaleTo(i, st)

	case "rolling_restart":
		return sr.rollingRestart(i, st)

	case "expect_no_log":
		// The node must not log the text for the window: an old owner that
		// must not collect any more, for one. The window starts at this step,
		// or at the mark that since names, so that it also covers the steps
		// in between. It ends `for` after this step starts. With node all,
		// no node may log it: every node container is read, the stopped ones
		// too, because a node a step stopped in the window logged until then.
		hold := st.DurationOr("for", 30*time.Second)
		nodes, err := sr.noLogNodes(st.Node)
		if err != nil {
			return false, err.Error(), nil
		}
		if st.Node != "all" {
			rep.Node = nodes[0]
		}
		from, fromWhat := time.Now(), "this step"
		if st.Since != "" {
			at, ok := sr.markAt(st.Since)
			if !ok {
				return false, fmt.Sprintf("no mark %q was set before this step", st.Since), nil
			}
			from, fromWhat = at, fmt.Sprintf("mark %q", st.Since)
		}
		end := time.Now().Add(hold)
		if err := sleepCtx(sr.ctx, hold); err != nil {
			return false, "", fmt.Errorf("interrupted: %w", err)
		}
		// The read stops at the window's end: a line logged between the end
		// and the read is outside the window. The first line in time is the
		// one reported. If a log cannot be read, the step fails, because the
		// line may be in that log.
		n, at, line, found, err := earliestLogMatch(nodes, func(n int) (time.Time, string, bool, error) {
			at, line, found, err := firstLogLineInWindow(sr.containers[n-1], st.LogLines(), from, end)
			// A spare node that never started has no container, and so
			// logged nothing.
			if errors.Is(err, errNoContainer) && !sr.active[n] {
				return time.Time{}, "", false, nil
			}
			return at, line, found, err
		})
		if found {
			rep.Node = n
			return false, fmt.Sprintf("node %d logged %q %s after %s", n, line, fmtMs(at.Sub(from).Milliseconds()), fromWhat), nil
		}
		if err != nil {
			return false, fmt.Sprintf("could not read the log, so it is not known whether %s was logged: %v", quoteLines(st.LogLines()), err), nil
		}
		window := fmtMs(end.Sub(from).Milliseconds())
		if st.Node == "all" {
			return true, fmt.Sprintf("no node logged %s from %s for %s", quoteLines(st.LogLines()), fromWhat, window), nil
		}
		return true, fmt.Sprintf("node %d did not log %s from %s for %s", nodes[0], quoteLines(st.LogLines()), fromWhat, window), nil

	case "expect_arrived":
		// Name the window now, while its marks are the latest of their
		// labels. The run counts the window after the drain.
		from, okFrom := sr.markAt(st.SentAfter)
		to, okTo := sr.markAt(st.SentBefore)
		if !okFrom || !okTo {
			return false, fmt.Sprintf("marks %q and %q must both be set before this step", st.SentAfter, st.SentBefore), nil
		}
		if !to.After(from) {
			return false, fmt.Sprintf("mark %q is not before mark %q, so the window is empty", st.SentAfter, st.SentBefore), nil
		}
		rc, ok := sr.receiverByID(st.Receiver)
		if !ok {
			return false, fmt.Sprintf("receiver %q is not one of the case's receivers", st.Receiver), nil
		}
		check := arrivedCheck{
			Name: st.Record, Receiver: rc.ID, SentAfter: st.SentAfter, SentBefore: st.SentBefore,
			From: from, To: to, Min: st.Min, Max: st.Max, Soft: st.Soft, port: rc.Port,
			CompareTo: st.CompareTo, MinShare: st.MinShare, MaxShare: st.MaxShare,
		}
		if st.CompareTo != "" {
			other, ok := sr.receiverByID(st.CompareTo)
			if !ok {
				return false, fmt.Sprintf("receiver %q is not one of the case's receivers", st.CompareTo), nil
			}
			check.comparePort = other.Port
		}
		sr.arrived = append(sr.arrived, check)
		return true, fmt.Sprintf("the lines sent between marks %q and %q are counted at %s after the drain", st.SentAfter, st.SentBefore, rc.describe()), nil

	case "count_pending":
		// Count the lines sent before the mark that have reached the receiver
		// so far. The run counts them again after the drain: the difference
		// is how many were still on their way at this step and arrived later.
		before, ok := sr.markAt(st.SentBefore)
		if !ok {
			return false, fmt.Sprintf("no mark %q was set before this step", st.SentBefore), nil
		}
		ls, err := sr.receiverLatency(time.Time{}, before)
		if err != nil {
			return false, fmt.Sprintf("could not read the receiver's counts: %v", err), nil
		}
		sr.pending = append(sr.pending, pendingCount{Name: st.Record, Mark: st.SentBefore, SentBefore: before, CountedAt: time.Now(), Arrived: int64(ls.Count)})
		return true, fmt.Sprintf("%d lines sent before mark %q had arrived; the ones still on their way are counted after the drain", ls.Count, st.SentBefore), nil

	case "wait_stable":
		// Wait until no running node has logged a sign of the cluster still
		// settling (a stream or consumer moving to another Raft group, a
		// pipeline stage starting) for `for`. A cluster reports itself
		// healthy before every node's pipeline stages have started, and a
		// stage that starts late changes the shared streams; a case that
		// measures something else starts its load after this.
		hold := st.DurationOr("for", 20*time.Second)
		within := st.DurationOr("within", 180*time.Second)
		started := time.Now()
		deadline := started.Add(within)
		for {
			last := time.Time{}
			for n := 1; n <= len(sr.containers); n++ {
				if !sr.active[n] {
					continue
				}
				running, err := containerRunningState(sr.containers[n-1])
				if err != nil {
					return false, fmt.Sprintf("could not tell whether node %d runs, so it is not known whether the cluster settled: %v", n, err), nil
				}
				if !running {
					continue
				}
				at, found, err := lastLogLineSince(sr.containers[n-1], settlingMarkers, time.Now().Add(-hold))
				if err != nil {
					return false, fmt.Sprintf("could not read node %d's log, so it is not known whether the cluster settled: %v", n, err), nil
				}
				if found && at.After(last) {
					last = at
				}
			}
			if last.IsZero() {
				ms := time.Since(started).Milliseconds()
				sr.record(st.Record, float64(ms))
				return true, fmt.Sprintf("no node settled anything for %s (waited %s)", hold, fmtMs(ms)), nil
			}
			if time.Now().After(deadline) {
				return false, fmt.Sprintf("the cluster was still settling after %s (last change at %s)", within, last.UTC().Format("15:04:05")), nil
			}
			if err := sleepCtx(sr.ctx, 2*time.Second); err != nil {
				return false, "", fmt.Errorf("interrupted: %w", err)
			}
		}

	case "wait_log":
		within := st.DurationOr("within", 120*time.Second)
		since := sr.lastDisruption
		var nodes []int
		if st.Node != "" {
			n, err := sr.resolveNode(st.Node)
			if err != nil {
				return false, err.Error(), nil
			}
			nodes = []int{n}
		} else {
			for n := range sr.containers {
				nodes = append(nodes, n+1)
			}
		}
		deadline := time.Now().Add(within)
		for {
			// Every node is read, and the first line in time wins, so the
			// measure and the report name the first node to log it. A node
			// whose log cannot be read is read again in the next round. If
			// the wait runs out, the step names the error.
			n, at, line, found, readErr := earliestLogMatch(nodes, func(n int) (time.Time, string, bool, error) {
				// Without a node, only running nodes count: the last lines of a
				// node a step just killed must not satisfy the wait.
				if st.Node == "" {
					running, err := containerRunningState(sr.containers[n-1])
					if err != nil {
						return time.Time{}, "", false, fmt.Errorf("could not tell whether node %d runs: %w", n, err)
					}
					if !running {
						return time.Time{}, "", false, nil
					}
				}
				// The read stops at the deadline: a line logged after it is
				// too late, even when the read happens to see it.
				return firstLogLineInWindow(sr.containers[n-1], st.LogLines(), since, deadline)
			})
			if found {
				ms := at.Sub(since).Milliseconds()
				sr.record(st.Record, float64(ms))
				rep.Node = n
				return true, fmt.Sprintf("node %d logged %q %s after the disruption", n, line, fmtMs(ms)), nil
			}
			if time.Now().After(deadline) {
				if readErr != nil {
					return false, fmt.Sprintf("no node logged %s within %s, and a log could not be read: %v", quoteLines(st.LogLines()), within, readErr), nil
				}
				return false, fmt.Sprintf("no node logged %s within %s", quoteLines(st.LogLines()), within), nil
			}
			if err := sleepCtx(sr.ctx, time.Second); err != nil {
				return false, "", fmt.Errorf("interrupted: %w", err)
			}
		}
	}
	return false, fmt.Sprintf("unknown action %q", st.Do), nil
}

func (sr *scenarioRun) record(name string, v float64) {
	if name != "" {
		sr.measures[name] = v
	}
}

// recordCount stores a number of lines that a step counted beside its time,
// under the step's record name with suffix added. The report keeps counts
// apart from the measures, which are times in milliseconds. A step with no
// record name stores no count.
func (sr *scenarioRun) recordCount(record, suffix string, n int64) {
	if record != "" {
		sr.counts[record+suffix] = n
	}
}

// disrupt notes a disruption: later waits measure from it, and the report
// measures delivery after it.
func (sr *scenarioRun) disrupt(step int, do string, node int) {
	at := time.Now()
	sr.lastDisruption = at
	sr.events = append(sr.events, scenarioEvent{Step: step, Do: do, Node: node, At: at})
	// Remember how many lines the client had acknowledged at this moment, so
	// that a later step can count the acknowledgements that came after it.
	sr.disruptionAcked, sr.disruptionAckedOK = 0, false
	if !sr.loadStarted.IsZero() {
		if p, ok := readGeneratorProgress(); ok {
			sr.disruptionAcked, sr.disruptionAckedOK = p.Acked, true
		}
	}
}

func (sr *scenarioRun) nodeAction(i int, st config.ScenarioStep, rep *scenarioStepReport) (bool, string, error) {
	n, err := sr.resolveNode(st.Node)
	if err != nil {
		return false, err.Error(), nil
	}
	rep.Node = n
	if st.As != "" {
		sr.names[st.As] = n
	}
	c := sr.containers[n-1]
	grace := st.DurationOr("grace", 30*time.Second)
	graceSecs := strconv.Itoa(int(grace.Seconds()))
	start := time.Now()

	switch st.Do {
	case "kill":
		sig := st.Signal
		if sig == "" {
			sig = "SIGKILL"
		}
		sr.disrupt(i, "kill "+sig, n)
		// SIGCONT wakes a node that SIGSTOP froze; every other signal this
		// step sends takes the node out of service.
		if sig == "SIGCONT" {
			delete(sr.down, n)
		} else {
			sr.down[n] = true
		}
		if err := dockerCmd(sr.ctx, 30*time.Second, "kill", "-s", sig, c); err != nil {
			return false, fmt.Sprintf("docker kill -s %s %s: %v", sig, c, err), nil
		}
		return true, fmt.Sprintf("sent %s to node %d", sig, n), nil
	case "stop":
		sr.disrupt(i, "stop", n)
		sr.down[n] = true
		if err := dockerCmd(sr.ctx, grace+30*time.Second, "stop", "-t", graceSecs, c); err != nil {
			return false, fmt.Sprintf("docker stop %s: %v", c, err), nil
		}
		if why := ungracefulStop(n, c, grace); why != "" {
			return false, why, nil
		}
		return true, fmt.Sprintf("stopped node %d in %s", n, fmtMs(time.Since(start).Milliseconds())), nil
	case "restart":
		sr.disrupt(i, "restart", n)
		delete(sr.down, n)
		if err := dockerCmd(sr.ctx, grace+60*time.Second, "restart", "-t", graceSecs, c); err != nil {
			return false, fmt.Sprintf("docker restart %s: %v", c, err), nil
		}
		sr.relink(n)
		return true, fmt.Sprintf("restarted node %d in %s", n, fmtMs(time.Since(start).Milliseconds())), nil
	case "start":
		argsFile := orchestrator.NodeArgsPath(sr.tmpDir, n)
		if st.Recover != "" {
			if err := os.WriteFile(argsFile, []byte("-recover="+st.Recover), 0o644); err != nil {
				return false, fmt.Sprintf("writing %s: %v", argsFile, err), nil
			}
		}
		sr.disrupt(i, "start", n)
		delete(sr.down, n)
		if st.EmptyDisk {
			// A new container from the same service starts on an empty disk, as
			// a node whose disk was replaced does. The director image declares
			// no volume, so its storage lives in the container. The step checks
			// that the container really is a new one: the old one would start
			// on its old disk.
			oldID := containerID(sr.ctx, c)
			if err := sr.orch.RecreateServices(fmt.Sprintf("subject-%d", n)); err != nil {
				_ = os.Remove(argsFile)
				return false, fmt.Sprintf("starting a new container for node %d: %v", n, err), nil
			}
			if newID := containerID(sr.ctx, c); newID == "" || newID == oldID {
				_ = os.Remove(argsFile)
				return false, fmt.Sprintf("node %d's container was not replaced (id %.12s before, %.12s after), so its disk is not empty", n, oldID, newID), nil
			}
		} else if err := dockerCmd(sr.ctx, 60*time.Second, "start", c); err != nil {
			_ = os.Remove(argsFile)
			return false, fmt.Sprintf("docker start %s: %v", c, err), nil
		}
		sr.active[n] = true
		sr.relink(n)
		if st.Recover != "" {
			// The wrapper reads the file when the container starts. Remove it
			// once the node said it took it, so the next start is a plain one.
			used := false
			var readErr error
			for range 30 {
				_, found, err := firstLogLineSince(c, "bench: starting with", start)
				if err != nil {
					readErr = err
				} else if found {
					used = true
					break
				}
				if err := sleepCtx(sr.ctx, 500*time.Millisecond); err != nil {
					return false, "", fmt.Errorf("interrupted: %w", err)
				}
			}
			_ = os.Remove(argsFile)
			if !used {
				if readErr != nil {
					return false, fmt.Sprintf("node %d did not report starting with -recover=%s, and its log could not be read: %v", n, st.Recover, readErr), nil
				}
				return false, fmt.Sprintf("node %d did not report starting with -recover=%s", n, st.Recover), nil
			}
			return true, fmt.Sprintf("started node %d once with -recover=%s", n, st.Recover), nil
		}
		if st.EmptyDisk {
			return true, fmt.Sprintf("started node %d on an empty disk", n), nil
		}
		return true, fmt.Sprintf("started node %d", n), nil
	case "pause", "unpause":
		if st.Do == "pause" {
			sr.disrupt(i, "pause", n)
			sr.down[n] = true
		} else {
			delete(sr.down, n)
		}
		if err := dockerCmd(sr.ctx, 30*time.Second, st.Do, c); err != nil {
			return false, fmt.Sprintf("docker %s %s: %v", st.Do, c, err), nil
		}
		return true, fmt.Sprintf("%sd node %d", st.Do, n), nil
	case "disconnect":
		if sr.network == "" {
			return false, "could not find the bench network's name", nil
		}
		sr.disrupt(i, "disconnect", n)
		if err := dockerCmd(sr.ctx, 30*time.Second, "network", "disconnect", sr.network, c); err != nil {
			return false, fmt.Sprintf("docker network disconnect %s %s: %v", sr.network, c, err), nil
		}
		sr.disconnected[n] = true
		return true, fmt.Sprintf("cut node %d off the network", n), nil
	case "connect":
		sr.disrupt(i, "connect", n)
		alias := fmt.Sprintf("subject-%d", n)
		if err := dockerCmd(sr.ctx, 30*time.Second, "network", "connect", "--alias", alias, sr.network, c); err != nil {
			return false, fmt.Sprintf("docker network connect %s %s: %v", sr.network, c, err), nil
		}
		delete(sr.disconnected, n)
		// The node has a new interface, and maybe a new address.
		sr.relink(n)
		return true, fmt.Sprintf("put node %d back on the network", n), nil
	}
	return false, "unknown node action", nil
}

// resolveNode turns a selector into a node number.
func (sr *scenarioRun) resolveNode(sel string) (int, error) {
	switch {
	case strings.HasPrefix(sel, "@"):
		n, ok := sr.names[sel[1:]]
		if !ok {
			return 0, fmt.Errorf("no node was remembered as %q", sel[1:])
		}
		return n, nil
	case sel == "owner":
		// The node the leader last placed a placement-managed device on, from
		// its "Assigned new device" / "Reassigned device" log lines.
		owner := agentlessDeviceOwner(sr.containers)
		n, err := strconv.Atoi(owner)
		if err != nil || n < 1 || n > len(sr.containers) {
			return 0, fmt.Errorf("no device placement found in the logs (owner %q)", owner)
		}
		return n, nil
	case sel == "leader" || sel == "follower":
		// Wait briefly for a round with an agreed leader.
		var leader int
		for range 20 {
			if r, ok := sr.probe.latest(); ok {
				if l, ok := agreedLeader(r); ok {
					leader = l
					break
				}
			}
			if err := sleepCtx(sr.ctx, 500*time.Millisecond); err != nil {
				return 0, err
			}
		}
		if leader == 0 {
			return 0, errors.New("no agreed leader to pick a node by")
		}
		if sel == "leader" {
			return leader, nil
		}
		for n := 1; n <= len(sr.containers); n++ {
			if sr.active[n] && n != leader && !sr.disconnected[n] && !sr.down[n] && containerRunning(sr.containers[n-1]) {
				return n, nil
			}
		}
		return 0, errors.New("no running follower")
	}
	n, err := strconv.Atoi(sel)
	if err != nil || n < 1 || n > len(sr.containers) {
		return 0, fmt.Errorf("node %q is not a node of this run", sel)
	}
	return n, nil
}

// healthy reports whether every member node that is meant to work runs,
// answers, has its message queue up and agrees on one leader, with no
// stream short of replicas. A member that a step took out of service on
// purpose (killed, stopped, paused, cut off) is left out. Any other member
// that is not running yet, such as one a step is still starting, makes the
// cluster not healthy.
func (sr *scenarioRun) healthy(r healthRound) bool {
	leaders := map[string]bool{}
	checked := 0
	for n := range sr.active {
		if sr.down[n] || sr.disconnected[n] {
			continue
		}
		h, ok := r.node(n)
		checked++
		if !ok || h.State != "running" || !h.Reachable || h.Phase != "running" || h.VMMQ != "ok" {
			return false
		}
		if h.Clustered {
			if h.MetaLeader == "" || h.UnderRepl > 0 || h.Reconfiguring {
				return false
			}
			leaders[h.MetaLeader] = true
		}
	}
	return checked > 0 && len(leaders) <= 1
}

// waitRound waits for a round that satisfies ok, reading the prober's
// newest round twice a second.
func (sr *scenarioRun) waitRound(within time.Duration, ok func(healthRound) bool) (healthRound, bool, error) {
	deadline := time.Now().Add(within)
	var last healthRound
	for {
		if r, have := sr.probe.latest(); have {
			last = r
			if r.At.After(sr.lastDisruption) && ok(r) {
				return r, true, nil
			}
		}
		if time.Now().After(deadline) {
			return last, false, nil
		}
		if err := sleepCtx(sr.ctx, 500*time.Millisecond); err != nil {
			return last, false, fmt.Errorf("interrupted: %w", err)
		}
	}
}

func (sr *scenarioRun) startLoad() error {
	if !sr.loadStarted.IsZero() {
		return nil
	}
	if !sr.tc.HasGenerator() {
		sr.loadStarted = time.Now()
		return nil
	}
	fmt.Println("  starting the load (http_ack generator)…")
	// --no-deps: the generator depends on node 1, and a plain `up` would
	// start node 1 again if a step had stopped it.
	if err := sr.orch.RestartServices("generator"); err != nil {
		return fmt.Errorf("starting the generator: %w", err)
	}
	sr.loadStarted = time.Now()
	return nil
}

func (sr *scenarioRun) witnessAction(i int, st config.ScenarioStep) (bool, string, error) {
	down := st.Do == "witness_down"
	if down {
		sr.disrupt(i, st.Do, 0)
	}
	switch sr.sc.Witness {
	case "dir":
		mode := "0777"
		if down {
			mode = "000"
		}
		// Any running node can change the shared volume.
		for n := 1; n <= len(sr.containers); n++ {
			if !containerRunning(sr.containers[n-1]) {
				continue
			}
			if err := dockerCmd(sr.ctx, 30*time.Second, "exec", "-u", "0", sr.containers[n-1], "chmod", mode, sr.sc.WitnessDirOrDefault()); err != nil {
				return false, fmt.Sprintf("chmod %s on the witness directory: %v", mode, err), nil
			}
			if down {
				return true, "the witness directory refuses access (mode 000)", nil
			}
			return true, "the witness directory is writable again", nil
		}
		return false, "no running node to change the witness directory from", nil
	default:
		// A witness behind a proxy named "witness" is cut at the proxy, so
		// the store keeps its records. Otherwise the store is paused.
		for _, p := range sr.sc.Proxies {
			if p.Name == "witness" {
				if err := toxiproxySetEnabled(sr.sc.ToxiproxyHostPortOrDefault(), "witness", !down); err != nil {
					return false, err.Error(), nil
				}
				if down {
					return true, "the witness proxy refuses connections", nil
				}
				return true, "the witness proxy accepts connections again", nil
			}
		}
		container := "bench-azurite"
		if sr.sc.Witness == "s3" {
			container = "bench-minio"
		}
		verb := "unpause"
		if down {
			verb = "pause"
		}
		if err := dockerCmd(sr.ctx, 30*time.Second, verb, container); err != nil {
			return false, fmt.Sprintf("docker %s %s: %v", verb, container, err), nil
		}
		return true, fmt.Sprintf("%sd %s", verb, container), nil
	}
}

func (sr *scenarioRun) proxyAction(i int, st config.ScenarioStep) (bool, string, error) {
	port := sr.sc.ToxiproxyHostPortOrDefault()
	switch st.Do {
	case "proxy_down", "proxy_up":
		down := st.Do == "proxy_down"
		if down {
			sr.disrupt(i, st.Do, 0)
		}
		if err := toxiproxySetEnabled(port, st.Proxy, !down); err != nil {
			return false, err.Error(), nil
		}
		return true, "", nil
	}
	latency, _ := time.ParseDuration(st.Latency)
	jitter, _ := time.ParseDuration(st.Jitter)
	sr.disrupt(i, "proxy_latency "+st.Latency, 0)
	if err := toxiproxySetLatency(port, st.Proxy, latency, jitter); err != nil {
		return false, err.Error(), nil
	}
	if latency == 0 {
		return true, fmt.Sprintf("removed the latency on %s", st.Proxy), nil
	}
	return true, fmt.Sprintf("%s now adds %s each way", st.Proxy, latency), nil
}

// scaleTo grows or shrinks the cluster: it rewrites every member's config
// to name the new member list, starts the nodes that join and stops the
// nodes that leave.
func (sr *scenarioRun) scaleTo(i int, st config.ScenarioStep) (bool, string, error) {
	target := st.Nodes
	if target == sr.size {
		return true, "already at that size", nil
	}
	sr.disrupt(i, fmt.Sprintf("scale %d→%d", sr.size, target), 0)
	members := max(target, sr.size)
	// Rewrite the configs of every node that stays or joins.
	for n := 1; n <= members; n++ {
		if n > target {
			continue
		}
		path := orchestrator.ClusterNodeConfigPath(sr.tmpDir, n)
		if err := orchestrator.RenderClusterNodeConfig(sr.configSrc, path, n, target); err != nil {
			return false, fmt.Sprintf("rendering node %d's config for %d nodes: %v", n, target, err), nil
		}
	}
	var started, stopped []int
	if target > sr.size {
		var services []string
		for n := sr.size + 1; n <= target; n++ {
			services = append(services, fmt.Sprintf("subject-%d", n))
			started = append(started, n)
			sr.active[n] = true
		}
		if err := sr.orch.RestartServices(services...); err != nil {
			return false, fmt.Sprintf("starting %v: %v", services, err), nil
		}
		for _, n := range started {
			sr.relink(n)
		}
	} else {
		for n := target + 1; n <= sr.size; n++ {
			if err := dockerCmd(sr.ctx, 60*time.Second, "stop", "-t", "30", sr.containers[n-1]); err != nil {
				return false, fmt.Sprintf("stopping node %d: %v", n, err), nil
			}
			delete(sr.active, n)
			stopped = append(stopped, n)
		}
	}
	prev := sr.size
	sr.size = target
	return true, fmt.Sprintf("cluster %d → %d nodes (started %v, stopped %v)", prev, target, started, stopped), nil
}

// rollingRestart restarts every active node in turn, followers first and
// the leader last, and waits for a healthy cluster after each.
func (sr *scenarioRun) rollingRestart(i int, st config.ScenarioStep) (bool, string, error) {
	within := st.DurationOr("within", 180*time.Second)
	grace := st.DurationOr("grace", 30*time.Second)
	leader := 0
	if r, ok := sr.probe.latest(); ok {
		leader, _ = agreedLeader(r)
	}
	var order []int
	for n := 1; n <= len(sr.containers); n++ {
		if sr.active[n] && n != leader {
			order = append(order, n)
		}
	}
	if leader != 0 {
		order = append(order, leader)
	}
	var lines []string
	for _, n := range order {
		start := time.Now()
		sr.disrupt(i, "rolling restart", n)
		if err := dockerCmd(sr.ctx, grace+60*time.Second, "restart", "-t", strconv.Itoa(int(grace.Seconds())), sr.containers[n-1]); err != nil {
			return false, fmt.Sprintf("restarting node %d: %v", n, err), nil
		}
		sr.relink(n)
		round, ok, err := sr.waitRound(within, func(r healthRound) bool { return sr.healthy(r) })
		if err != nil {
			return false, "", err
		}
		ms := time.Since(start).Milliseconds()
		sr.record(fmt.Sprintf("%s_node%d", defaultStr(st.Record, "rolling_restart"), n), float64(ms))
		if !ok {
			return false, fmt.Sprintf("the cluster was not healthy within %s after restarting node %d: %s", within, n, describeRound(round)), nil
		}
		lines = append(lines, fmt.Sprintf("node %d %s", n, fmtMs(ms)))
	}
	return true, "restarted and healthy again: " + strings.Join(lines, ", "), nil
}

func defaultStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// ===== end of run =============================================================

// scenarioReport is the run's full record, written next to the results.
type scenarioReport struct {
	Case  string `json:"case"`
	Image string `json:"image"`
	// ImageID and ImageLabels say exactly which build ran: a tag can move,
	// the ID cannot, and a build usually labels the commit it came from.
	ImageID     string            `json:"image_id,omitempty"`
	ImageLabels map[string]string `json:"image_labels,omitempty"`
	Nodes       int               `json:"nodes"`
	NodeCPU     string            `json:"node_cpu_limit"`
	NodeMem     string            `json:"node_mem_limit"`
	Acked       int64             `json:"acked"`
	// Received, Unique, Missing, Duplicates and Extra are the receiver's
	// counts. With several receivers they are the counts of the receiver
	// that missed the most lines, and Receivers lists each one.
	Received   int64                   `json:"received"`
	Unique     int64                   `json:"unique"`
	Missing    int64                   `json:"missing"`
	Duplicates int64                   `json:"duplicates"`
	Extra      int64                   `json:"extra"`
	Latency    latencyStats            `json:"latency"`
	Windows    map[string]latencyStats `json:"latency_windows,omitempty"`
	// AckMaxWindows is the slowest acknowledgement the client waited for in
	// each window between marks, in milliseconds.
	AckMaxWindows map[string]int64   `json:"ack_latency_max_ms_windows,omitempty"`
	Measures      map[string]float64 `json:"measures_ms"`
	// Counts holds the numbers of lines that steps counted beside their
	// times, such as <record>_lines_acked. They are lines, not milliseconds.
	Counts map[string]int64 `json:"counts,omitempty"`
	// Rows compares a database's rows with the rows that arrived
	// (cluster.scenario.rows).
	Rows *rowsCheck `json:"rows,omitempty"`
	// Pending holds the count_pending steps' counts: the lines sent before a
	// mark that were still on their way when the step ran.
	Pending []pendingCount `json:"pending,omitempty"`
	// Arrived holds the expect_arrived steps' counts: the lines sent between
	// two marks that reached a receiver, counted after the drain.
	Arrived    []arrivedCheck       `json:"arrived,omitempty"`
	Events     []scenarioEvent      `json:"events"`
	Steps      []scenarioStepReport `json:"steps"`
	Leadership leadershipStats      `json:"leadership"`
	Generator  *generatorAck        `json:"generator,omitempty"`
	SeqCheck   *seqCheckResult      `json:"seqcheck,omitempty"`
	// Receivers holds each receiver's counts in a case with several
	// (`receivers:`). Every acknowledged line must reach each of them.
	Receivers []receiverResult `json:"receivers,omitempty"`
	// DuplicateRanges groups the lines that arrived twice, with when the
	// first copy and the second copy arrived.
	DuplicateRanges []duplicateRange `json:"duplicate_ranges,omitempty"`
	Failures        []string         `json:"failures,omitempty"`
	Notes           []string         `json:"notes,omitempty"`
	HealthTrail     []string         `json:"health_trail,omitempty"`
	// Timeline is the receiver's count of arrivals per 100 ms, so that a
	// delivery gap can be measured again after the run.
	Timeline *receiverTimeline `json:"timeline,omitempty"`
	// LogsDir holds every container's log of the run, with docker's
	// timestamps.
	LogsDir string `json:"logs_dir,omitempty"`
}

// summary is the compact part of the report kept in the result row.
func (rep scenarioReport) summary() map[string]any {
	out := map[string]any{
		"image":                    rep.Image,
		"image_id":                 rep.ImageID,
		"acked":                    rep.Acked,
		"missing":                  rep.Missing,
		"duplicates":               rep.Duplicates,
		"latency_max_ms":           rep.Latency.MaxMs,
		"split_brain_ms":           rep.Leadership.SplitBrainMs,
		"max_self_claimed_leaders": rep.Leadership.MaxSelfClaimed,
		"stream_split_ms":          rep.Leadership.StreamSplitMs,
		"leader_changes":           rep.Leadership.LeaderChanges,
	}
	for k, v := range rep.ImageLabels {
		if lk := strings.ToLower(k); strings.Contains(lk, "commit") || strings.Contains(lk, "revision") {
			out["image_revision"] = v
		}
	}
	for _, rr := range rep.Receivers {
		out["missing_"+rr.ID] = rr.Missing
		out["duplicates_"+rr.ID] = rr.Duplicates
	}
	for k, v := range rep.Measures {
		out["ms_"+k] = v
	}
	for k, v := range rep.Counts {
		out["count_"+k] = v
	}
	if rep.Rows != nil && rep.Rows.Error == "" {
		out["rows_written"] = rep.Rows.Written
		out["rows_missing"] = rep.Rows.Missing
	}
	for _, p := range rep.Pending {
		// A count that could not be finished is left out, not reported as 0.
		if p.ArrivedLater != nil {
			out["pending_"+p.Name] = *p.ArrivedLater
		}
	}
	for _, a := range rep.Arrived {
		// A window that could not be counted is left out, not reported as 0.
		if a.Arrived != nil {
			out["arrived_"+a.Name] = *a.Arrived
		}
	}
	for i, ev := range rep.Events {
		out[fmt.Sprintf("event%d_%s_gap_ms", i, strings.ReplaceAll(ev.Do, " ", "_"))] = ev.GapMs
	}
	return out
}

type scenarioVerdict struct {
	passed  bool
	reasons []string
}

func (sr *scenarioRun) finish() (scenarioReport, scenarioVerdict, error) {
	rep := scenarioReport{
		Case:        sr.tc.Name,
		Image:       sr.subject.ImageRef(),
		ImageID:     sr.imageID,
		ImageLabels: sr.imageLabels,
		Nodes:       sr.tc.Cluster.Nodes,
		NodeCPU:     sr.r.runCPULimit,
		NodeMem:     sr.r.runMemLimit,
		Measures:    sr.measures,
		Counts:      sr.counts,
		Notes:       sr.notes,
	}

	if !sr.tc.HasGenerator() {
		return sr.finishWithoutLoad(rep)
	}

	// The load ends after its duration plus the generator's drain timeout,
	// in which it retries any batch not yet acknowledged.
	genBudget := sr.tc.DurationOrDefault(2*time.Minute) + 3*time.Minute
	fmt.Printf("  waiting for the load to finish (up to %s)…\n", genBudget.Round(time.Second))
	if err := sr.orch.WaitForGeneratorExit(genBudget); err != nil {
		if sr.ctx.Err() != nil {
			return rep, scenarioVerdict{}, fmt.Errorf("interrupted: %w", sr.ctx.Err())
		}
		sr.failures = append(sr.failures, fmt.Sprintf("the generator did not finish cleanly: %v", firstLine(err.Error())))
	}
	gen := parseGeneratorAck(sr.orch.GeneratorStdout())
	rep.Generator = gen
	acked := map[string]int64{}
	if gen != nil && gen.Ack != nil {
		acked = gen.Ack.AckedPerConn
		rep.Acked = gen.Ack.LinesAcked
		fmt.Printf("  generator: %d lines acknowledged, %d requests, %d failed, %d failovers, %d left unacknowledged\n",
			gen.Ack.LinesAcked, gen.Ack.Requests, gen.Ack.Failures, gen.Ack.Failovers, gen.Ack.LinesUnacked)
	} else {
		sr.failures = append(sr.failures, "the generator's acknowledgement summary is missing")
	}

	// Drain: wait until every acknowledged line arrived at every receiver,
	// or nothing new arrives at one for 30 seconds, within the drain budget.
	drain := sr.sc.DrainOrDefault()
	fmt.Printf("  draining (up to %s)…\n", drain)
	deadline := time.Now().Add(drain)
	recvResults := make([]receiverResult, 0, len(sr.receivers))
	for _, rc := range sr.receivers {
		res, err := sr.checkReceiver(rc, acked, deadline)
		if err != nil {
			return rep, scenarioVerdict{}, err
		}
		recvResults = append(recvResults, res)
	}
	// The top-level counts are those of the receiver that missed the most
	// lines; with one receiver, its own. A case with several receivers
	// lists each of them too.
	worst := recvResults[worstReceiver(recvResults)]
	rep.SeqCheck = worst.SeqCheck
	rep.Missing = worst.Missing
	rep.Duplicates = worst.Duplicates
	rep.Extra = worst.Extra
	rep.Unique = worst.Unique
	rep.Received = worst.Received
	rep.DuplicateRanges = worst.DuplicateRanges
	if sr.tc.MultiReceiver() {
		rep.Receivers = recvResults
	}
	// The count_pending steps: count again, now that every line that will
	// arrive has arrived. The lines that arrived after the step were still
	// on their way when it ran.
	for i := range sr.pending {
		p := &sr.pending[i]
		ls, err := sr.receiverLatency(time.Time{}, p.SentBefore)
		if err != nil {
			sr.notes = append(sr.notes, fmt.Sprintf("count_pending %s: could not read the receiver's counts after the drain, so it has no count: %v", p.Name, err))
			fmt.Printf("  %s: could not be counted after the drain (%v)\n", p.Name, err)
			continue
		}
		later := arrivedSince(p.Arrived, int64(ls.Count))
		p.ArrivedLater = &later
		fmt.Printf("  %s: %d lines sent before mark %q were still on their way at %s and arrived after it\n",
			p.Name, later, p.Mark, p.CountedAt.UTC().Format("15:04:05.000"))
	}
	// The receiver counts lines by when they were sent, whether or not the
	// client had them confirmed. The client retries a batch until it is
	// confirmed, so only lines still unconfirmed at the end of the run can be
	// counted without a confirmation. Say so when there are any.
	if len(sr.pending) > 0 && gen != nil && gen.Ack != nil && gen.Ack.LinesUnacked > 0 {
		sr.notes = append(sr.notes, fmt.Sprintf("the client left %d lines unconfirmed; a count_pending count can include those of them sent before its mark", gen.Ack.LinesUnacked))
	}
	rep.Pending = sr.pending

	// The expect_arrived steps: count each window now that every line that
	// will arrive has arrived.
	for i := range sr.arrived {
		c := &sr.arrived[i]
		var ls, cs latencyStats
		err := receiverGetAt(c.port, latencyPath(c.From, c.To), &ls)
		if err == nil && c.CompareTo != "" {
			err = receiverGetAt(c.comparePort, latencyPath(c.From, c.To), &cs)
		}
		if err != nil {
			reason := fmt.Sprintf("%s: could not count the lines that arrived, so the expectation is not known to hold: %v", c.Name, err)
			if c.Soft {
				sr.notes = append(sr.notes, reason)
			} else {
				sr.failures = append(sr.failures, reason)
			}
			continue
		}
		arrived, compared := int64(ls.Count), int64(cs.Count)
		c.Arrived = &arrived
		at := ""
		if c.Receiver != "" {
			at = " at receiver " + c.Receiver
		}
		of := ""
		if c.CompareTo != "" {
			c.ComparedArrived = &compared
			of = fmt.Sprintf(", and %d at receiver %s", compared, c.CompareTo)
		}
		fmt.Printf("  %s: %d lines sent between marks %q and %q arrived%s%s\n", c.Name, arrived, c.SentAfter, c.SentBefore, at, of)
		if reason := judgeArrived(*c, arrived, compared); reason != "" {
			if c.Soft {
				sr.notes = append(sr.notes, reason)
			} else {
				sr.failures = append(sr.failures, reason)
			}
		}
	}
	rep.Arrived = sr.arrived

	// Latency: the whole run, each window between marks, and the lines
	// sent in the minute after each disruption.
	if ls, err := sr.receiverLatency(time.Time{}, time.Time{}); err == nil {
		rep.Latency = ls
		fmt.Printf("  latency (first arrival): p50 %.0f ms, p95 %.0f ms, p99 %.0f ms, max %.0f ms over %d lines\n",
			ls.P50Ms, ls.P95Ms, ls.P99Ms, ls.MaxMs, ls.Count)
	}
	if len(sr.marks) > 0 {
		rep.Windows = map[string]latencyStats{}
		for i, m := range sr.marks {
			var to time.Time
			if i+1 < len(sr.marks) {
				to = sr.marks[i+1].At
			}
			if ls, err := sr.receiverLatency(m.At, to); err == nil {
				rep.Windows[m.Label] = ls
				fmt.Printf("    window %-16s p50 %6.0f ms  p95 %6.0f ms  p99 %6.0f ms  max %6.0f ms  (%d lines)\n",
					m.Label, ls.P50Ms, ls.P95Ms, ls.P99Ms, ls.MaxMs, ls.Count)
			}
		}
		// The client's slowest confirmation per window, from the generator's
		// per-second maxima. The generator's second 0 is the start of the
		// load, give or take its start-up.
		if gen != nil && gen.Ack != nil && len(gen.Ack.AckLatencyMaxPerSec) > 0 && !sr.loadStarted.IsZero() {
			rep.AckMaxWindows = map[string]int64{}
			for i, m := range sr.marks {
				from := int(m.At.Sub(sr.loadStarted) / time.Second)
				to := len(gen.Ack.AckLatencyMaxPerSec)
				if i+1 < len(sr.marks) {
					to = int(sr.marks[i+1].At.Sub(sr.loadStarted)/time.Second) + 1
				}
				var worst int64
				for s := max(from, 0); s < min(to, len(gen.Ack.AckLatencyMaxPerSec)); s++ {
					worst = max(worst, gen.Ack.AckLatencyMaxPerSec[s])
				}
				rep.AckMaxWindows[m.Label] = worst
			}
		}
	}
	if gen != nil && gen.Ack != nil && gen.Ack.AckLatency != nil {
		a := gen.Ack.AckLatency
		fmt.Printf("  confirmations (client post to 2xx): p50 %d ms, p95 %d ms, p99 %d ms, max %d ms over %d posts\n",
			a.P50, a.P95, a.P99, a.Max, a.Count)
	}
	timeline, terr := sr.receiverTimeline()
	if terr == nil {
		rep.Timeline = &timeline
	}
	for i := range sr.events {
		ev := &sr.events[i]
		// A gap belongs to this disruption if it starts before the next one,
		// so that a later disruption's gap is not reported again here.
		window := 120 * time.Second
		if i+1 < len(sr.events) {
			window = min(window, sr.events[i+1].At.Sub(ev.At))
		}
		if terr == nil {
			ev.GapMs, ev.ResumeMs = deliveryGapAfter(timeline, ev.At, window)
		}
		if ls, err := sr.receiverLatency(ev.At.Add(-2*time.Second), ev.At.Add(60*time.Second)); err == nil && ls.Count > 0 {
			ev.LatencyAfter = &ls
		}
		lat := ""
		if ev.LatencyAfter != nil {
			lat = fmt.Sprintf(", max latency %.0f ms", ev.LatencyAfter.MaxMs)
		}
		fmt.Printf("    event %-22s node %d: longest delivery gap %s, resumed %s after%s\n",
			ev.Do, ev.Node, fmtMs(ev.GapMs), fmtMs(ev.ResumeMs), lat)
	}
	rep.Events = sr.events
	rep.Steps = sr.steps

	rounds := sr.probe.all()
	rep.Leadership = analyzeLeadership(rounds)
	rep.HealthTrail = healthTrail(rounds)
	fmt.Printf("  leadership: %d rounds read, %d leader changes, most self-named leaders at once %d, two-leader time %s, stream two-leader time %s\n",
		rep.Leadership.Rounds, rep.Leadership.LeaderChanges, rep.Leadership.MaxSelfClaimed,
		fmtMs(rep.Leadership.SplitBrainMs), fmtMs(rep.Leadership.StreamSplitMs))

	// Verdict.
	v := scenarioVerdict{reasons: append([]string(nil), sr.failures...)}
	ex := sr.sc.Expect
	if ex.MinAcked > 0 && rep.Acked < ex.MinAcked {
		v.reasons = append(v.reasons, fmt.Sprintf("only %d lines were acknowledged, fewer than min_acked %d", rep.Acked, ex.MinAcked))
	}
	if len(rep.Receivers) > 0 {
		v.reasons = append(v.reasons, receiverVerdict(rep.Receivers, ex)...)
	} else {
		if ex.MaxLoss != nil && rep.Missing > *ex.MaxLoss {
			v.reasons = append(v.reasons, fmt.Sprintf("%d acknowledged lines never arrived (max_loss %d)", rep.Missing, *ex.MaxLoss))
		}
		if ex.MaxDuplicates != nil && rep.Duplicates > *ex.MaxDuplicates {
			v.reasons = append(v.reasons, fmt.Sprintf("%d lines arrived more than once (max_duplicates %d)", rep.Duplicates, *ex.MaxDuplicates))
		}
	}
	if ex.MaxSplitBrainMs != nil && rep.Leadership.SplitBrainMs > *ex.MaxSplitBrainMs {
		v.reasons = append(v.reasons, fmt.Sprintf("two nodes named themselves the leader for %s (max_split_brain_ms %d)",
			fmtMs(rep.Leadership.SplitBrainMs), *ex.MaxSplitBrainMs))
	}
	// Validation keeps rows to cases without a generator, so a run with load
	// has none. Were that to change, a judged rows check that never ran fails
	// here instead of passing unseen.
	if reason, _ := judgeRows(rep.Rows, ex.MaxMissingRows, ex.MaxRowsTwice); reason != "" {
		v.reasons = append(v.reasons, reason)
	}
	rep.Failures = v.reasons
	rep.Notes = sr.notes
	v.passed = len(v.reasons) == 0
	return rep, v, nil
}

// receiverResult is what one receiver got. In a case with several receivers
// (`receivers:`) every acknowledged line must reach each of them, and the
// report lists each one.
type receiverResult struct {
	ID string `json:"id"`
	// Checked is false when the receiver never answered the sequence check,
	// so that its loss is unknown.
	Checked    bool            `json:"checked"`
	Received   int64           `json:"received"`
	Unique     int64           `json:"unique"`
	Missing    int64           `json:"missing"`
	Duplicates int64           `json:"duplicates"`
	Extra      int64           `json:"extra"`
	SeqCheck   *seqCheckResult `json:"seqcheck,omitempty"`
	// DuplicateRanges groups the lines that arrived at this receiver twice.
	DuplicateRanges []duplicateRange `json:"duplicate_ranges,omitempty"`
}

// describe names the receiver in a sentence.
func (rc scenarioReceiver) describe() string {
	if rc.ID == "" {
		return "the receiver"
	}
	return "receiver " + rc.ID
}

// checkReceiver drains one receiver and reads its counts. It waits until
// every acknowledged line arrived there, or nothing new arrives for 30
// seconds, until the deadline. A receiver that never answers is a failure:
// a zero count would pass max_loss on a check that never ran.
func (sr *scenarioRun) checkReceiver(rc scenarioReceiver, acked map[string]int64, deadline time.Time) (receiverResult, error) {
	check, checked, err := drainSeqCheck(sr.ctx, func() (seqCheckResult, error) { return receiverSeqCheckAt(rc.Port, acked) },
		deadline, 30*time.Second, 2*time.Second)
	if sr.ctx.Err() != nil {
		return receiverResult{}, fmt.Errorf("interrupted: %w", sr.ctx.Err())
	}
	res := receiverResult{ID: rc.ID, Checked: checked, SeqCheck: &check,
		Unique: check.Unique, Missing: check.Missing, Duplicates: check.Duplicates, Extra: check.Extra}
	if !checked {
		sr.failures = append(sr.failures, fmt.Sprintf("%s never answered the sequence check, so loss is unknown: %v", rc.describe(), err))
	}
	if m, err := sr.r.queryReceiverMetrics(rc.Port, 10*time.Second); err == nil {
		res.Received = m.LinesReceived
	}
	at := ""
	if rc.ID != "" {
		at = " at receiver " + rc.ID
	}
	fmt.Printf("  delivery%s: %d acknowledged, %d of them arrived, %d missing, %d duplicates, %d extra\n",
		at, check.Acked, check.ReceivedAcked, check.Missing, check.Duplicates, check.Extra)
	for conn, cc := range check.Conns {
		if cc.Missing > 0 {
			fmt.Printf("    conn %s: missing %d, first ranges %v\n", conn, cc.Missing, cc.MissingRanges)
		}
	}
	if check.Duplicates > 0 {
		var d duplicatesView
		if err := receiverGetAt(rc.Port, "/duplicates", &d); err == nil {
			res.DuplicateRanges = summarizeDuplicates(d.Samples, 50)
			for i, dr := range res.DuplicateRanges {
				if i == 10 {
					fmt.Printf("    … %d duplicate ranges in all, see the report\n", len(res.DuplicateRanges))
					break
				}
				fmt.Printf("    duplicates conn %d seq %d-%d: sent %s, the copy arrived %s after the first\n",
					dr.Conn, dr.FromSeq, dr.ToSeq, dr.Sent.UTC().Format("15:04:05.000"), fmtMs(dr.CopyAfterMs))
			}
		}
	}
	return res, nil
}

// worstReceiver returns the index of the receiver that missed the most
// lines, the first of them on a tie. A receiver that never answered comes
// before one that did, because its loss is unknown.
func worstReceiver(rs []receiverResult) int {
	worst := 0
	for i, r := range rs {
		w := rs[worst]
		if (!r.Checked && w.Checked) || (r.Checked == w.Checked && r.Missing > w.Missing) {
			worst = i
		}
	}
	return worst
}

// receiverVerdict judges each receiver of a case with several against
// max_loss and max_duplicates: every acknowledged line must reach each one.
// A receiver that expect.receivers names is judged by its own limits there,
// and must miss at least its min_loss.
func receiverVerdict(rs []receiverResult, ex config.ScenarioExpect) []string {
	var reasons []string
	for _, r := range rs {
		maxLoss, maxDuplicates, minLoss := ex.ReceiverLimits(r.ID)
		if minLoss != nil && r.Missing < *minLoss {
			reasons = append(reasons, fmt.Sprintf("only %d acknowledged lines never arrived at receiver %s, fewer than min_loss %d: the loss the case tests did not happen", r.Missing, r.ID, *minLoss))
		}
		if maxLoss != nil && r.Missing > *maxLoss {
			reasons = append(reasons, fmt.Sprintf("%d acknowledged lines never arrived at receiver %s (max_loss %d)", r.Missing, r.ID, *maxLoss))
		}
		if maxDuplicates != nil && r.Duplicates > *maxDuplicates {
			reasons = append(reasons, fmt.Sprintf("%d lines arrived at receiver %s more than once (max_duplicates %d)", r.Duplicates, r.ID, *maxDuplicates))
		}
	}
	return reasons
}

// judgeRows judges the rows check of a case whose data came from a database
// (cluster.scenario.rows; config validation allows it only without a
// generator). It returns a reason the run fails, or a note when the case does
// not judge its rows, or neither.
func judgeRows(rows *rowsCheck, maxMissing, maxTwice *int64) (reason, note string) {
	// Either limit makes the comparison one the run depends on.
	judge := maxMissing != nil || maxTwice != nil
	if rows == nil {
		if judge {
			return "the database rows were not compared with what arrived", ""
		}
		return "", ""
	}
	var reasons []string
	switch {
	case rows.Error != "" && judge:
		// An unknown count is not zero.
		return "the database rows could not be compared with what arrived: " + rows.Error, ""
	case rows.Error != "":
		return "", "the database rows could not be compared with what arrived: " + rows.Error
	case judge && rows.Written == 0:
		return "the rows query returned no rows, so nothing was compared", ""
	}
	if maxMissing != nil && rows.Missing > *maxMissing {
		reasons = append(reasons, fmt.Sprintf("%d of %d database rows never arrived (max_missing_rows %d); the first: %s",
			rows.Missing, rows.Written, *maxMissing, strings.Join(rows.MissingKeys, ", ")))
	}
	if maxTwice != nil && rows.ArrivedTwice > *maxTwice {
		reasons = append(reasons, fmt.Sprintf("%d of %d database rows arrived more than once (max_rows_twice %d); the first: %s",
			rows.ArrivedTwice, rows.Written, *maxTwice, strings.Join(rows.TwiceKeys, ", ")))
	}
	return strings.Join(reasons, "; "), ""
}

// finishWithoutLoad ends a scenario whose data came from a device the
// cluster collected itself: there are no acknowledged lines, so the receiver's
// own counts (and its hash-based duplicate check, when the case turns it on)
// are reported as they are.
func (sr *scenarioRun) finishWithoutLoad(rep scenarioReport) (scenarioReport, scenarioVerdict, error) {
	if sr.sc.Rows != nil {
		rows := sr.checkRows()
		rep.Rows = &rows
	}
	m, metricsErr := sr.r.queryReceiverMetrics(sr.recvPort, 10*time.Second)
	if metricsErr == nil {
		rep.Received = m.LinesReceived
		rep.Unique = m.UniqueLines
		rep.Duplicates = m.Duplicates
		fmt.Printf("  delivery: %d lines received, %d distinct, %d duplicates\n", m.LinesReceived, m.UniqueLines, m.Duplicates)
	}
	rep.Events = sr.events
	rep.Steps = sr.steps
	rounds := sr.probe.all()
	rep.Leadership = analyzeLeadership(rounds)
	rep.HealthTrail = healthTrail(rounds)
	fmt.Printf("  leadership: %d rounds read, %d leader changes, most self-named leaders at once %d, two-leader time %s\n",
		rep.Leadership.Rounds, rep.Leadership.LeaderChanges, rep.Leadership.MaxSelfClaimed, fmtMs(rep.Leadership.SplitBrainMs))
	v := scenarioVerdict{reasons: append([]string(nil), sr.failures...)}
	reasons, notes := withoutLoadVerdict(rep, sr.sc.Expect, sr.tc.Correctness.ValidateDedup, metricsErr)
	v.reasons = append(v.reasons, reasons...)
	sr.notes = append(sr.notes, notes...)
	rep.Failures = v.reasons
	rep.Notes = sr.notes
	v.passed = len(v.reasons) == 0
	return rep, v, nil
}

// withoutLoadVerdict judges a run whose data came from a device the cluster
// collected itself. dedup says whether the receiver hashes every line
// (correctness.validate_dedup), which is the only duplicate count such a run
// has. metricsErr is the error of reading the receiver's counts.
//
// A count that was never taken is not zero. The first version of these cases
// judged max_duplicates over a count that the receiver's sequence tracker had
// replaced with zero, so rows that arrived twice went unseen.
func withoutLoadVerdict(rep scenarioReport, ex config.ScenarioExpect, dedup bool, metricsErr error) (reasons, notes []string) {
	if ex.MaxDuplicates != nil {
		switch {
		case !dedup:
			reasons = append(reasons, "max_duplicates needs correctness.validate_dedup: without a generator, the receiver counts duplicates only by hashing every line")
		case metricsErr != nil:
			reasons = append(reasons, "the receiver's counts could not be read, so duplicates were not checked: "+metricsErr.Error())
		case rep.Received > 0 && rep.Unique == 0:
			reasons = append(reasons, fmt.Sprintf("the receiver got %d lines and counted none as distinct, so duplicates were not checked", rep.Received))
		case rep.Duplicates > *ex.MaxDuplicates:
			reasons = append(reasons, fmt.Sprintf("%d lines arrived more than once (max_duplicates %d)", rep.Duplicates, *ex.MaxDuplicates))
		}
	}
	if ex.MaxSplitBrainMs != nil && rep.Leadership.SplitBrainMs > *ex.MaxSplitBrainMs {
		reasons = append(reasons, fmt.Sprintf("two nodes named themselves the leader for %s (max_split_brain_ms %d)",
			fmtMs(rep.Leadership.SplitBrainMs), *ex.MaxSplitBrainMs))
	}
	// The rows are judged here, in the only verdict a case with rows reaches.
	// A proof run found them reported and never judged: a row was missing, and
	// the case passed.
	reason, note := judgeRows(rep.Rows, ex.MaxMissingRows, ex.MaxRowsTwice)
	if reason != "" {
		reasons = append(reasons, reason)
	}
	if note != "" {
		notes = append(notes, note)
	}
	return reasons, notes
}

// drainSeqCheck reads the receiver's sequence check until every acknowledged
// line has arrived, until the count of arrived lines has not changed for
// quiet, or until deadline. It returns the last check that succeeded.
// checked is false when no read succeeded at all, and err is then the last
// error. The loss of such a run is unknown, not zero.
func drainSeqCheck(ctx context.Context, read func() (seqCheckResult, error), deadline time.Time, quiet, poll time.Duration) (check seqCheckResult, checked bool, err error) {
	var lastReceived int64 = -1
	quietSince := time.Now()
	for {
		c, readErr := read()
		if readErr == nil {
			check, checked = c, true
			if c.Missing == 0 {
				return check, true, nil
			}
			if c.ReceivedAcked != lastReceived {
				lastReceived = c.ReceivedAcked
				quietSince = time.Now()
			} else if time.Since(quietSince) > quiet {
				return check, true, nil
			}
		} else {
			err = readErr
		}
		if time.Now().After(deadline) {
			if checked {
				err = nil
			}
			return check, checked, err
		}
		if sleepErr := sleepCtx(ctx, poll); sleepErr != nil {
			return check, checked, sleepErr
		}
	}
}

// deliveryGapAfter finds the longest stretch without a first arrival that
// starts within window after at, and how long after at arrivals resumed. A
// gap that starts within the window is measured to its end, even past the
// window: the window says which gaps belong to this disruption, not how long
// they may last.
func deliveryGapAfter(tl receiverTimeline, at time.Time, window time.Duration) (gapMs, resumeMs int64) {
	if tl.BucketMs <= 0 || len(tl.Unique) == 0 {
		return 0, 0
	}
	bucket := time.Duration(tl.BucketMs) * time.Millisecond
	// A gap starts where its first empty bucket starts. The gaps of this
	// disruption start at or after at and before at+window.
	first := min(bucketAtOrAfter(tl, at), len(tl.Unique))
	last := min(bucketAtOrAfter(tl, at.Add(window)), len(tl.Unique))
	i := first
	// A gap already under way at the first bucket started before at, so it
	// belongs to an earlier disruption: skip it, and look for gaps that
	// start after it.
	if i > 0 && i < len(tl.Unique) && tl.Unique[i-1] == 0 {
		for i < len(tl.Unique) && tl.Unique[i] == 0 {
			i++
		}
	}
	bestLen, bestEnd := 0, -1
	for i < last {
		if tl.Unique[i] != 0 {
			i++
			continue
		}
		start := i
		for i < len(tl.Unique) && tl.Unique[i] == 0 {
			i++
		}
		if i-start > bestLen {
			bestLen, bestEnd = i-start, i
		}
	}
	if bestLen == 0 {
		return 0, 0
	}
	gapMs = int64(bestLen) * tl.BucketMs
	resumeAt := tl.StartNs + int64(bestEnd)*int64(bucket)
	resumeMs = (resumeAt - at.UnixNano()) / int64(time.Millisecond)
	if resumeMs < 0 {
		resumeMs = 0
	}
	return gapMs, resumeMs
}

// bucketAtOrAfter returns the index of the first timeline bucket that starts
// at or after t.
func bucketAtOrAfter(tl receiverTimeline, t time.Time) int {
	b := tl.BucketMs * int64(time.Millisecond)
	off := t.UnixNano() - tl.StartNs
	if off <= 0 {
		return 0
	}
	return int((off + b - 1) / b)
}

// duplicatesView is the receiver's /duplicates answer.
type duplicatesView struct {
	Count     int64       `json:"count"`
	Samples   []dupSample `json:"samples"`
	Truncated bool        `json:"truncated"`
}

// dupSample is one arrival of a line that had arrived before. FirstNs is
// zero when the receiver does not know the first arrival.
type dupSample struct {
	Conn    int64 `json:"conn"`
	Seq     int64 `json:"seq"`
	SentNs  int64 `json:"sent_ns"`
	FirstNs int64 `json:"first_ns"`
	AtNs    int64 `json:"at_ns"`
}

// duplicateRange is a run of consecutive lines of one CONN that arrived
// twice. Sent is the send time of its first line, First the earliest first
// arrival, Again the earliest arrival of a copy, and CopyAfterMs how long
// after the first copy the second one came.
type duplicateRange struct {
	Conn        int64     `json:"conn"`
	FromSeq     int64     `json:"from_seq"`
	ToSeq       int64     `json:"to_seq"`
	Copies      int       `json:"copies"`
	Sent        time.Time `json:"sent"`
	First       time.Time `json:"first_arrival"`
	Again       time.Time `json:"copy_arrival"`
	CopyAfterMs int64     `json:"copy_after_first_ms"`
}

// summarizeDuplicates groups duplicate arrivals into runs of consecutive
// lines per CONN, at most limit runs.
func summarizeDuplicates(samples []dupSample, limit int) []duplicateRange {
	s := append([]dupSample(nil), samples...)
	sort.Slice(s, func(i, j int) bool {
		if s[i].Conn != s[j].Conn {
			return s[i].Conn < s[j].Conn
		}
		if s[i].Seq != s[j].Seq {
			return s[i].Seq < s[j].Seq
		}
		return s[i].AtNs < s[j].AtNs
	})
	var out []duplicateRange
	minTime := func(cur time.Time, ns int64) time.Time {
		if ns <= 0 {
			return cur
		}
		t := time.Unix(0, ns)
		if cur.IsZero() || t.Before(cur) {
			return t
		}
		return cur
	}
	for _, d := range s {
		n := len(out)
		if n > 0 && out[n-1].Conn == d.Conn && d.Seq <= out[n-1].ToSeq+1 {
			r := &out[n-1]
			r.ToSeq = max(r.ToSeq, d.Seq)
			r.Copies++
			r.Sent = minTime(r.Sent, d.SentNs)
			r.First = minTime(r.First, d.FirstNs)
			r.Again = minTime(r.Again, d.AtNs)
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, duplicateRange{
			Conn: d.Conn, FromSeq: d.Seq, ToSeq: d.Seq, Copies: 1,
			Sent: minTime(time.Time{}, d.SentNs), First: minTime(time.Time{}, d.FirstNs), Again: minTime(time.Time{}, d.AtNs),
		})
	}
	for i := range out {
		if !out[i].First.IsZero() && !out[i].Again.IsZero() {
			out[i].CopyAfterMs = out[i].Again.Sub(out[i].First).Milliseconds()
		}
	}
	return out
}

// arrivalsAfter returns when the last line reached the receiver, and how
// many distinct lines arrived after at. When none arrived after at, last is
// at itself.
func arrivalsAfter(tl receiverTimeline, at time.Time) (last time.Time, count int64) {
	last = at
	if tl.BucketMs <= 0 {
		return last, 0
	}
	bucket := time.Duration(tl.BucketMs) * time.Millisecond
	first := max(int((at.UnixNano()-tl.StartNs)/int64(bucket)), 0)
	for i := first; i < len(tl.Unique); i++ {
		if tl.Unique[i] == 0 {
			continue
		}
		count += int64(tl.Unique[i])
		if end := time.Unix(0, tl.StartNs+int64(i+1)*int64(bucket)); end.After(last) {
			last = end
		}
	}
	return last, count
}

// healthTrail keeps one line for every round whose state differs from the
// round before, so the report shows what changed without every round.
func healthTrail(rounds []healthRound) []string {
	var out []string
	prev := ""
	for _, r := range rounds {
		d := describeRound(r)
		if d != prev {
			out = append(out, r.At.UTC().Format("15:04:05.000")+" "+d)
			prev = d
		}
	}
	return out
}

// saveScenarioLogs writes each container's log, with docker's timestamps, to
// <results>/<hardware>/scenarios/<case>-logs/<container>.log. A container
// that does not exist (a spare node that never started) is skipped.
func (r *Runner) saveScenarioLogs(name string, containers []string) (string, error) {
	dir := filepath.Join(r.opts.ResultsDir, hardwareID(), "scenarios", name+"-logs")
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	for _, c := range containers {
		out, err := exec.Command("docker", "logs", "-t", c).CombinedOutput()
		if err != nil && len(out) == 0 {
			continue
		}
		if werr := os.WriteFile(filepath.Join(dir, c+".log"), out, 0o644); werr != nil {
			return "", werr
		}
	}
	return dir, nil
}

func (r *Runner) writeScenarioReport(name string, rep scenarioReport) (string, error) {
	dir := filepath.Join(r.opts.ResultsDir, hardwareID(), "scenarios")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, name+".json")
	return path, os.WriteFile(path, b, 0o644)
}

// ===== helpers ================================================================

func fmtMs(ms int64) string {
	if ms < 1000 {
		return fmt.Sprintf("%d ms", ms)
	}
	return fmt.Sprintf("%.1f s", float64(ms)/1000)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// ungracefulStop returns why node n, which docker stop ended, did not stop by
// itself, or "" when it did. docker stop sends SIGTERM, and then SIGKILL when
// the grace period runs out. Exit code 137 means that the node needed the
// SIGKILL.
func ungracefulStop(n int, container string, grace time.Duration) string {
	code, oom, err := containerExit(container)
	switch {
	case err != nil:
		return fmt.Sprintf("could not read how node %d's process ended: %v", n, err)
	case oom:
		return fmt.Sprintf("node %d ran out of memory, so its process did not stop by itself", n)
	case code == 137:
		return fmt.Sprintf("node %d did not exit within %s of the SIGTERM, so Docker killed it (exit code 137)", n, grace)
	}
	return ""
}

// dockerCmd runs one docker command with a timeout. Tests replace it.
var dockerCmd = func(ctx context.Context, timeout time.Duration, args ...string) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := exec.CommandContext(cctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// containerID returns a container's full id, or "" when docker cannot inspect
// it.
func containerID(ctx context.Context, container string) string {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(cctx, "docker", "inspect", "-f", "{{.Id}}", container).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// containerNetwork returns the name of the (only) network the container is on.
func containerNetwork(container string) string {
	out, err := exec.Command("docker", "inspect", "-f", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}", container).Output()
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// firstLogLineSince returns the docker timestamp of the first line since t
// that contains needle. It returns an error when the log cannot be read, so
// that a caller does not take an unread log for a log without the line.
func firstLogLineSince(container, needle string, t time.Time) (time.Time, bool, error) {
	at, _, found, err := firstLogLineInWindow(container, []string{needle}, t, time.Time{})
	return at, found, err
}

// errNoContainer is the error readLogWindow returns for a container that does
// not exist: a spare node that never started.
var errNoContainer = errors.New("no such container")

// readLogWindow returns what a container logged from since up to until, as
// `docker logs -t` prints it. A zero until reads up to the moment of the read.
// Tests replace it.
var readLogWindow = func(container string, since, until time.Time) (string, error) {
	args := []string{"logs", "-t", "--since", since.UTC().Format(time.RFC3339Nano)}
	if !until.IsZero() {
		args = append(args, "--until", until.UTC().Format(time.RFC3339Nano))
	}
	out, err := exec.Command("docker", append(args, container)...).CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "No such container") {
			return "", fmt.Errorf("%w: %s", errNoContainer, container)
		}
		return "", fmt.Errorf("docker logs %s: %w: %s", container, err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// firstLogLineInWindow returns the docker timestamp of the first line from
// since up to until that contains any of the needles, and the needle it
// contains. A zero until leaves the window open: up to the moment of the read.
// It returns an error when the log cannot be read.
func firstLogLineInWindow(container string, needles []string, since, until time.Time) (time.Time, string, bool, error) {
	out, err := readLogWindow(container, since, until)
	if err != nil {
		return time.Time{}, "", false, err
	}
	at, needle, found := firstMatchingLine(out, needles, until)
	return at, needle, found, nil
}

// firstMatchingLine finds the first line of `docker logs -t` output that
// contains any of the needles, and returns its timestamp and that needle. A
// line stamped after a non-zero until is outside the window and does not count.
func firstMatchingLine(out string, needles []string, until time.Time) (time.Time, string, bool) {
	for _, line := range strings.Split(out, "\n") {
		for _, needle := range needles {
			if !strings.Contains(line, needle) {
				continue
			}
			ts, _, _ := strings.Cut(line, " ")
			at, perr := time.Parse(time.RFC3339Nano, ts)
			if perr != nil || (!until.IsZero() && at.After(until)) {
				continue
			}
			return at, needle, true
		}
	}
	return time.Time{}, "", false
}

// readProgressBaseline reads the client's progress, trying up to tries times,
// wait apart. It reports false when no read succeeded or the run stopped.
func readProgressBaseline(ctx context.Context, read func() (genProgress, bool), tries int, wait time.Duration) (genProgress, bool) {
	for i := 0; i < tries; i++ {
		if p, ok := read(); ok {
			return p, true
		}
		if i+1 < tries {
			if err := sleepCtx(ctx, wait); err != nil {
				return genProgress{}, false
			}
		}
	}
	return genProgress{}, false
}

// noLogNodes returns the nodes an expect_no_log step reads: every node
// container for "all", else the one node the selector names.
func (sr *scenarioRun) noLogNodes(sel string) ([]int, error) {
	if sel == "all" {
		nodes := make([]int, len(sr.containers))
		for i := range sr.containers {
			nodes[i] = i + 1
		}
		return nodes, nil
	}
	n, err := sr.resolveNode(sel)
	if err != nil {
		return nil, err
	}
	return []int{n}, nil
}

// earliestLogMatch asks each node for its first matching line and returns the
// node whose line came first, its time and the text it matched. find reports
// a node's first match, or false when it has none, or an error when it cannot
// read the node's log. The returned error names every node whose log could not
// be read. A step that checks that a line was not logged must not pass on a
// log it never read.
func earliestLogMatch(nodes []int, find func(n int) (time.Time, string, bool, error)) (int, time.Time, string, bool, error) {
	bestNode, bestAt, bestLine := 0, time.Time{}, ""
	var errs []error
	for _, n := range nodes {
		at, line, found, err := find(n)
		if err != nil {
			errs = append(errs, fmt.Errorf("node %d: %w", n, err))
			continue
		}
		if !found {
			continue
		}
		if bestNode == 0 || at.Before(bestAt) {
			bestNode, bestAt, bestLine = n, at, line
		}
	}
	return bestNode, bestAt, bestLine, bestNode != 0, errors.Join(errs...)
}

// quoteLines names the texts a log step looks for: one quoted text, or the
// quoted texts joined with "or".
func quoteLines(lines []string) string {
	quoted := make([]string, len(lines))
	for i, l := range lines {
		quoted[i] = strconv.Quote(l)
	}
	return strings.Join(quoted, " or ")
}

// readGeneratorProgress returns the generator's last PROGRESS line.
func readGeneratorProgress() (genProgress, bool) {
	return readGeneratorProgressOf("bench-generator")
}

// readGeneratorProgressOf returns the last PROGRESS line of the generator
// container with that name.
func readGeneratorProgressOf(container string) (genProgress, bool) {
	out, err := exec.Command("docker", "logs", "--tail", "6", container).CombinedOutput()
	if err != nil {
		return genProgress{}, false
	}
	return parseGeneratorProgress(string(out))
}

// parseGeneratorProgress returns the last PROGRESS line of a generator's log.
func parseGeneratorProgress(out string) (genProgress, bool) {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		m := progressRE.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		tms, _ := strconv.ParseInt(m[1], 10, 64)
		acked, _ := strconv.ParseInt(m[2], 10, 64)
		fails, _ := strconv.ParseInt(m[3], 10, 64)
		pending, _ := strconv.ParseInt(m[4], 10, 64)
		p := genProgress{At: time.UnixMilli(tms), Acked: acked, Failures: fails, Pending: pending}
		if ns, _ := strconv.ParseInt(m[5], 10, 64); ns > 0 {
			p.LastAck = time.Unix(0, ns)
		}
		if pt := progressPerTargetRE.FindStringSubmatch(lines[i]); pt != nil {
			var perTarget map[string]int64
			if json.Unmarshal([]byte(pt[1]), &perTarget) == nil {
				p.AckedPerTarget = perTarget
			}
		}
		return p, true
	}
	return genProgress{}, false
}

// parseGeneratorAck reads the generator's final JSON.
func parseGeneratorAck(stdout string) *generatorAck {
	dec := json.NewDecoder(strings.NewReader(stdout))
	for {
		var g generatorAck
		if err := dec.Decode(&g); err != nil {
			return nil
		}
		if g.Ack != nil {
			return &g
		}
	}
}

// receiverClient bounds every receiver query: a step may have paused the
// receiver, and a query to a paused container would otherwise wait forever.
var receiverClient = &http.Client{Timeout: 10 * time.Second}

// receiverGet queries the first receiver, which the delivery steps, the
// timelines and the latencies read.
func (sr *scenarioRun) receiverGet(path string, out any) error {
	return receiverGetAt(sr.recvPort, path, out)
}

// receiverGetAt queries the receiver whose endpoints are on port.
func receiverGetAt(port int, path string, out any) error {
	resp, err := receiverClient.Get(fmt.Sprintf("http://localhost:%d%s", port, path)) //nolint:noctx // short local call
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("receiver %s: %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, out)
}

// receiverSeqCheckAt asks the receiver on port which acknowledged lines it
// got.
func receiverSeqCheckAt(port int, acked map[string]int64) (seqCheckResult, error) {
	keys := make([]string, 0, len(acked))
	for k := range acked {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+":"+strconv.FormatInt(acked[k], 10))
	}
	var out seqCheckResult
	err := receiverGetAt(port, "/seqcheck?acked="+url.QueryEscape(strings.Join(parts, ",")), &out)
	return out, err
}

func (sr *scenarioRun) receiverLatency(from, to time.Time) (latencyStats, error) {
	var out latencyStats
	err := sr.receiverGet(latencyPath(from, to), &out)
	return out, err
}

// latencyPath is the receiver query for the lines sent from from up to to. A
// zero time leaves that end open.
func latencyPath(from, to time.Time) string {
	q := url.Values{}
	if !from.IsZero() {
		q.Set("from_ns", strconv.FormatInt(from.UnixNano(), 10))
	}
	if !to.IsZero() {
		q.Set("to_ns", strconv.FormatInt(to.UnixNano(), 10))
	}
	return "/latency?" + q.Encode()
}

func (sr *scenarioRun) receiverTimeline() (receiverTimeline, error) {
	var out receiverTimeline
	err := sr.receiverGet("/timeline", &out)
	return out, err
}

// ===== Toxiproxy ==============================================================

func toxiproxyDo(port int, method, path string, body any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = strings.NewReader(string(b))
	}
	// Toxiproxy's API is published on IPv4 loopback only, so name it exactly.
	req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), rd) //nolint:noctx // short local call
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 && !(method == http.MethodDelete && resp.StatusCode == http.StatusNotFound) {
		return fmt.Errorf("toxiproxy %s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}

func toxiproxySetEnabled(port int, proxy string, enabled bool) error {
	return toxiproxyDo(port, http.MethodPost, "/proxies/"+proxy, map[string]any{"enabled": enabled})
}

// toxiproxySetLatency replaces the proxy's latency, in both directions.
// Zero removes it.
func toxiproxySetLatency(port int, proxy string, latency, jitter time.Duration) error {
	for _, stream := range []string{"downstream", "upstream"} {
		name := "bench_latency_" + stream
		if err := toxiproxyDo(port, http.MethodDelete, "/proxies/"+proxy+"/toxics/"+name, nil); err != nil {
			return err
		}
		if latency <= 0 {
			continue
		}
		toxic := map[string]any{
			"name":     name,
			"type":     "latency",
			"stream":   stream,
			"toxicity": 1.0,
			"attributes": map[string]any{
				"latency": int(latency.Milliseconds()),
				"jitter":  int(jitter.Milliseconds()),
			},
		}
		if err := toxiproxyDo(port, http.MethodPost, "/proxies/"+proxy+"/toxics", toxic); err != nil {
			return err
		}
	}
	return nil
}

// imageIdentity returns the local image's short ID and its labels, so a
// result always says which build it ran on.
func imageIdentity(ref string) (string, map[string]string) {
	out, err := exec.Command("docker", "image", "inspect", "-f", "{{.Id}}|{{json .Config.Labels}}", ref).Output()
	if err != nil {
		return "", nil
	}
	id, labelsJSON, _ := strings.Cut(strings.TrimSpace(string(out)), "|")
	id = strings.TrimPrefix(id, "sha256:")
	if len(id) > 12 {
		id = id[:12]
	}
	var labels map[string]string
	_ = json.Unmarshal([]byte(labelsJSON), &labels)
	return id, labels
}

// settlingMarkers are director log lines that mean the cluster is still
// settling: a stream or consumer moved to a new Raft group (its replica
// count changed), or a pipeline stage started or retried its start.
var settlingMarkers = []string{
	"stream remapping",
	"consumer remapping",
	"workqueue service started",
	"Initialization attempt",
}

// lastLogLineSince returns the docker timestamp of the last line since t that
// contains any of the needles. It returns an error when the log cannot be
// read.
func lastLogLineSince(container string, needles []string, t time.Time) (time.Time, bool, error) {
	out, err := readLogWindow(container, t, time.Time{})
	if err != nil {
		return time.Time{}, false, err
	}
	var last time.Time
	found := false
	for _, line := range strings.Split(out, "\n") {
		for _, needle := range needles {
			if !strings.Contains(line, needle) {
				continue
			}
			ts, _, _ := strings.Cut(line, " ")
			if at, perr := time.Parse(time.RFC3339Nano, ts); perr == nil && at.After(last) {
				last, found = at, true
			}
			break
		}
	}
	return last, found, nil
}
