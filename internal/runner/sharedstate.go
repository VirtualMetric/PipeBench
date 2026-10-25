package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"github.com/VirtualMetric/PipeBench/internal/orchestrator"
	"github.com/VirtualMetric/PipeBench/internal/results"
)

// runDirectorSharedStateCorrectness drives the replicas of one serverless
// director that keep the state they share in a store: Azurite here, standing
// in for Azure Blob Storage (the director's shared_state setting). The
// replicas run one config with no node list, as a container platform runs
// the replicas of one app.
//
// The driver starts the replicas, in groups when the case says so, and checks
// that each replica's state bus connects, that one replica leads, that each
// placed device runs on exactly one replica, and that /health reports the
// shared state as the case expects. It then starts the generators, runs the
// case's action (a crash, a graceful stop, ...), and checks the recovery:
// a new leader, the moved devices running again, and how long each took.
// At the end it checks every stream for loss and duplicates, every replica's
// log for a time two replicas ran the same device, and every log and /health
// answer for a secret. The replicas' logs, Azurite's access log and every
// /health answer are kept in the run's evidence directory.
func (r *Runner) runDirectorSharedStateCorrectness(tc *config.TestCase, subject config.Subject) (results.RunResult, error) {
	configName := r.opts.ConfigName
	subject = r.applySubjectOverrides(subject)
	ss := tc.SharedState

	for _, capName := range tc.Requires {
		if !subject.HasCapability(capName) {
			return results.RunResult{}, fmt.Errorf("subject %q does not declare capability %q required by case %q",
				subject.Name, capName, tc.Name)
		}
	}

	fmt.Printf("→ test=%s  subject=%s  version=%s  config=%s  (shared state: %d replicas + %d standby, mode=%s, action=%q)\n",
		tc.Name, subject.Name, subject.Version, configName, ss.Replicas, ss.Standby, ss.ModeOrDefault(), ss.Action)

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
		// The replicas are the subject. Everything around them is held to a
		// core and a gigabyte each, so a run cannot take the machine over.
		HelperCPULimit: "1",
		HelperMemLimit: "1g",
	})
	if err != nil {
		return results.RunResult{}, fmt.Errorf("compose setup: %w", err)
	}

	run := newSSRun(r, tc, subject, orch)
	for _, rep := range run.replicas {
		_ = exec.Command("docker", "rm", "-f", rep.Container).Run()
	}
	_ = orch.Down()
	defer run.teardown()

	run.started = time.Now()
	run.deadline = run.started.Add(r.opts.Timeout)
	passed := run.drive()

	// Resource usage: the collector sums every replica container per tick.
	metrics, metricsCSVSrc := r.harvestResourceMetrics(orch, tmpDir)
	sysCPUs, sysMemMB := getSystemInfo()

	if passed {
		fmt.Printf("  director shared state (%s): PASSED ✓\n", sharedStateActionLabel(ss.Action))
	} else {
		fmt.Printf("  director shared state (%s): FAILED ✗\n", sharedStateActionLabel(ss.Action))
		for _, e := range run.errs {
			fmt.Printf("    - %s\n", e)
		}
	}
	fmt.Printf("  evidence: %s\n", run.evidence)

	result := results.RunResult{
		TestName:        tc.Name,
		Config:          configName,
		Subject:         subject.Name,
		Version:         subject.Version,
		Hardware:        hardwareID(),
		Timestamp:       run.started,
		DurationSec:     time.Since(run.started).Seconds(),
		LinesIn:         run.linesIn,
		LinesOut:        run.linesOut,
		UniqueOut:       run.uniqueOut,
		PerReceiver:     run.perReceiver,
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
		SharedState:     run.measures,
	}
	if run.linesIn > 0 {
		result.LossPercent = 100 * float64(run.linesIn-min(run.uniqueOut, run.linesIn)) / float64(run.linesIn)
	}
	if !passed {
		result.FailReason = strings.Join(run.errs, "; ")
	}
	dir, err := r.saveResult(result, metricsCSVSrc)
	if err != nil {
		return result, fmt.Errorf("saving results: %w", err)
	}
	fmt.Printf("  done. results → %s\n", dir)
	return result, nil
}

func sharedStateActionLabel(action string) string {
	if action == "" {
		return "start"
	}
	return action
}

// ssReplica is one replica of the director: a compose service and its
// container.
type ssReplica struct {
	N         int
	Service   string
	Container string
}

// ssRun is one run of a shared-state case.
type ssRun struct {
	r       *Runner
	tc      *config.TestCase
	ss      *config.SharedStateConfig
	subject config.Subject
	orch    *orchestrator.ComposeRunner
	book    *ssLogBook

	replicas []ssReplica
	running  map[int]bool
	// ran are the replicas whose containers ran at some time in the run. The
	// final checks read every one of their logs, whatever they hold by then.
	ran map[int]bool
	// bad is the replica whose store credentials are wrong
	// (wrong_credentials), or 0.
	bad int
	// ended are the process ends the harness caused, which no replica logs.
	ended []ssEvent

	started, deadline time.Time
	evidence          string
	health            map[string]string

	errs     []string
	measures map[string]any

	linesIn, linesOut, uniqueOut int64
	perReceiver                  map[string]int64
}

func newSSRun(r *Runner, tc *config.TestCase, subject config.Subject, orch *orchestrator.ComposeRunner) *ssRun {
	run := &ssRun{
		r: r, tc: tc, ss: tc.SharedState, subject: subject, orch: orch,
		book:        newSSLogBook(),
		running:     map[int]bool{},
		ran:         map[int]bool{},
		health:      map[string]string{},
		measures:    map[string]any{},
		perReceiver: map[string]int64{},
		evidence:    filepath.Join(r.opts.ResultsDir, hardwareID(), "shared-state", tc.Name),
	}
	run.measures["evidence"] = run.evidence
	if tc.SharedState.Action == config.SharedStateActionWrongCredentials {
		run.bad = tc.SharedState.BadReplica
	}
	for n := 1; n <= tc.ReplicaNodes(); n++ {
		run.replicas = append(run.replicas, ssReplica{
			N:         n,
			Service:   fmt.Sprintf("subject-%d", n),
			Container: fmt.Sprintf("bench-subject-%s-%d", subject.Name, n),
		})
	}
	return run
}

func (run *ssRun) replica(n int) ssReplica { return run.replicas[n-1] }

// markStarted records that replica n's container runs.
func (run *ssRun) markStarted(n int) {
	run.running[n] = true
	run.ran[n] = true
}

// endReplica records that the harness ended replica n's process at `at`,
// and how. It first reads the lines the replica wrote last: a container that
// is replaced takes its log with it, and a stopped one that is left alone is
// only read again by the final checks. A read that fails here fails the run,
// because the checks would then pass on part of the evidence; endReplica
// reports whether the read succeeded.
//
// stopped says the harness asked the process to stop (docker stop: SIGTERM,
// then SIGKILL 30 seconds later). Such a process must have exited by itself:
// one that Docker had to kill did not stop gracefully, whatever else the
// case then measures.
func (run *ssRun) endReplica(n int, at time.Time, how string, stopped bool) bool {
	rep := run.replica(n)
	err := run.book.refresh(rep.Container)
	if err != nil {
		run.fail("reading the last lines of %s's log after the %s: %v", rep.Service, how, err)
	}
	run.running[n] = false
	run.ended = append(run.ended, ssEvent{At: at, Container: rep.Container, Kind: ssProcessEnd, Text: "harness: " + how})
	if stopped {
		run.checkGracefulExit(rep, how)
	}
	return err == nil
}

// containerExit reads how a container's process ended: its exit code, and
// whether the kernel killed it for memory. Tests replace it.
var containerExit = func(container string) (code int, oomKilled bool, err error) {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.ExitCode}} {{.State.OOMKilled}}", container).Output()
	if err != nil {
		return 0, false, fmt.Errorf("docker inspect %s: %w", container, err)
	}
	_, err = fmt.Sscanf(strings.TrimSpace(string(out)), "%d %t", &code, &oomKilled)
	return code, oomKilled, err
}

// checkGracefulExit fails the run when a replica the harness stopped did not
// exit by itself, and keeps its exit code.
func (run *ssRun) checkGracefulExit(rep ssReplica, how string) {
	code, oom, err := containerExit(rep.Container)
	if err != nil {
		run.fail("reading how %s's process ended after the %s: %v", rep.Service, how, err)
		return
	}
	codes, _ := run.measures["stop_exit_codes"].(map[string][]int)
	if codes == nil {
		codes = map[string][]int{}
		run.measures["stop_exit_codes"] = codes
	}
	codes[rep.Service] = append(codes[rep.Service], code)
	switch {
	case oom:
		run.fail("%s ran out of memory in the %s, so its process did not stop by itself", rep.Service, how)
	case code == 137:
		run.fail("%s did not exit within 30s of the SIGTERM of the %s, so Docker killed it (exit code 137): the stop was not graceful", rep.Service, how)
	}
}

// fail records why the case fails.
func (run *ssRun) fail(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	run.errs = append(run.errs, msg)
	fmt.Printf("  ✗ %s\n", msg)
}

// note prints an observation, and keeps it in the run's measures.
func (run *ssRun) note(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	notes, _ := run.measures["notes"].([]string)
	run.measures["notes"] = append(notes, msg)
	fmt.Printf("  %s\n", msg)
}

// drive runs the case's phases and reports whether every check passed. A
// phase that cannot go on returns early: the checks after it would only
// repeat its failure.
func (run *ssRun) drive() bool {
	ss := run.ss

	if !run.startInfrastructure() {
		return false
	}
	if !run.startReplicas() {
		return false
	}
	if ss.SecondStage {
		if !run.secondStage() {
			return false
		}
	}
	run.checkSettled("start", time.Now().Add(90*time.Second))
	run.checkHealthAll("start")
	if len(run.errs) > 0 {
		return false
	}
	if ss.Action == config.SharedStateActionStoreCalls {
		run.measureIdle()
	}

	run.measures["generators_started"] = time.Now().UTC()
	fmt.Println("  starting the generators…")
	if err := run.orch.RestartServices("generator"); err != nil {
		run.fail("starting the generators: %v", err)
		return false
	}
	if !run.waitBaseline() {
		return false
	}
	if d := time.Duration(run.ss.ActionDelaySeconds) * time.Second; d > 0 {
		fmt.Printf("  waiting %s before the action…\n", d)
		if sleepCtx(run.r.ctx, d) != nil {
			return false
		}
	}

	run.act()

	run.drain()
	run.analyse()
	return len(run.errs) == 0
}

// startInfrastructure starts what the replicas need before they start:
// Azurite and its init, the store when it is another (MinIO and its init, an
// endpoint such as Redis, or the share's init), the receivers and the
// collector. It waits for every init to finish and for every endpoint with a
// healthcheck to be healthy.
func (run *ssRun) startInfrastructure() bool {
	services, inits, names := []string{"azurite", "azure-init"}, []string{"azure-init"}, []string{"Azurite"}
	if run.tc.UsesMinio() {
		services, inits, names = append(services, "minio", "minio-init"), append(inits, "minio-init"), append(names, "MinIO")
	}
	if run.ss.StoreOrDefault() == config.SharedStateStoreShare {
		services, inits = append(services, "witness-init"), append(inits, "witness-init")
	}
	var healthy []string
	for _, e := range run.tc.Endpoints {
		services, names = append(services, e.Name), append(names, e.Name)
		if e.Healthcheck != nil {
			healthy = append(healthy, e.Name)
		}
	}
	services = append(services, "receiver", "collector")
	fmt.Printf("  starting %s, the receivers and the collector…\n", strings.Join(names, ", "))
	if err := run.orch.UpServices(services...); err != nil {
		run.fail("starting the infrastructure: %v", err)
		return false
	}
	deadline := time.Now().Add(2 * time.Minute)
	for _, one := range inits {
		if !run.waitContainer("bench-"+one, "{{.State.Status}} {{.State.ExitCode}}", "exited 0", deadline) {
			return false
		}
	}
	for _, name := range healthy {
		if !run.waitContainer("bench-"+name, "{{.State.Health.Status}}", "healthy", deadline) {
			return false
		}
	}
	return true
}

// waitContainer waits until docker inspect's format says want for the
// container, and fails the run when it never does. A container that has
// exited fails it at once, with its last log lines: a health status, for one,
// keeps its last value after the container is gone.
func (run *ssRun) waitContainer(container, format, want string, deadline time.Time) bool {
	name := strings.TrimPrefix(container, "bench-")
	var state string
	for {
		out, _ := containerInspect(container, "{{.State.Status}}|"+format)
		status, value, _ := strings.Cut(strings.TrimSpace(out), "|")
		state = value
		if value == want {
			return true
		}
		if status == "exited" || status == "dead" {
			run.fail("%s has %s (%s): %s", name, status, value, containerTail(container))
			return false
		}
		if time.Now().After(deadline) || sleepCtx(run.r.ctx, time.Second) != nil {
			break
		}
	}
	run.fail("%s was not %s within 2 minutes (%s)", name, want, state)
	return false
}

// containerInspect runs docker inspect with a format. Tests replace it.
var containerInspect = func(container, format string) (string, error) {
	out, err := exec.Command("docker", "inspect", "-f", format, container).Output()
	return string(out), err
}

// containerTail returns a container's last 20 log lines. Tests replace it.
var containerTail = func(container string) string {
	out, _ := exec.Command("docker", "logs", "--tail", "20", container).CombinedOutput()
	return strings.TrimSpace(string(out))
}

// startReplicas starts the replicas group by group, and waits for each group
// to be ready before the next starts.
func (run *ssRun) startReplicas() bool {
	formation := map[string]float64{}
	for _, group := range run.ss.StartGroups() {
		var services []string
		for _, n := range group {
			services = append(services, run.replica(n).Service)
		}
		at := time.Now()
		fmt.Printf("  starting %s…\n", strings.Join(services, ", "))
		// --no-deps: Azurite and its init already ran, and the init must not
		// run again.
		if err := run.orch.RestartServices(services...); err != nil {
			run.fail("starting %s: %v", strings.Join(services, ", "), err)
			return false
		}
		for _, n := range group {
			run.markStarted(n)
		}
		deadline := earliest(time.Now().Add(run.replicaWarmup()), run.deadline)
		for _, n := range group {
			rep := run.replica(n)
			if n == run.bad {
				// A replica with wrong store credentials never gets ready. It
				// must say why on /health instead.
				run.waitBadHealth(n, deadline)
				continue
			}
			// The process's start, on the clock its log lines carry.
			since := containerStartedAt(rep.Container, at)
			readyAt, ok := run.waitReady(n, since, deadline)
			if !ok {
				run.fail("%s was not ready %s after its start: %s", rep.Service, time.Since(at).Round(time.Second), run.readiness(n, since))
				return false
			}
			formation[rep.Service] = readyAt.Sub(since).Seconds()
			fmt.Printf("    %s ready after %.1fs (%s)\n", rep.Service, readyAt.Sub(since).Seconds(), run.readiness(n, since))
		}
		if run.ss.SharesBus() {
			if leader, ok := run.waitOneLeader(deadline); ok {
				fmt.Printf("    leader: %s\n", run.replica(leader).Service)
			}
		}
	}
	run.measures["ready_seconds"] = formation
	return true
}

// waitBadHealth waits for the bad replica's /health to report the status the
// case expects for its shared_state, and keeps the reason it gives.
func (run *ssRun) waitBadHealth(n int, deadline time.Time) {
	rep := run.replica(n)
	want := run.ss.BadHealthOrDefault()
	var status, message string
	for {
		status, message, _, _ = run.healthOf(n, "bad")
		if status == want && message != "" {
			break
		}
		if time.Now().After(deadline) || sleepCtx(run.r.ctx, 3*time.Second) != nil {
			run.fail("%s (wrong store credentials): /health reports shared_state %q (%q), want %q with a reason", rep.Service, status, message, want)
			return
		}
	}
	run.measures["bad_replica_health"] = map[string]string{"status": status, "message": message}
	fmt.Printf("    %s (wrong store credentials): /health shared_state %s: %s\n", rep.Service, status, message)
}

// replicaWarmup is the case's warmup: how long a group of replicas may take to
// start.
func (run *ssRun) replicaWarmup() time.Duration {
	return run.tc.WarmupOrDefault(2 * time.Minute)
}

// secondStage renders the replicas' configs again with Stage 2, once the
// devices of the first stage run.
func (run *ssRun) secondStage() bool {
	deadline := earliest(time.Now().Add(90*time.Second), run.deadline)
	for _, d := range run.ss.FirstStageDevices {
		for {
			run.refreshAll()
			o := run.owners(d, current())
			if len(o) == 1 {
				fmt.Printf("  first stage: device %d runs on %s\n", d, run.names(o))
				break
			}
			if time.Now().After(deadline) || sleepCtx(run.r.ctx, time.Second) != nil {
				run.fail("first stage: device %d runs on %d replicas (%s), not one", d, len(o), run.names(o))
				return false
			}
		}
	}
	fmt.Println("  rendering the replicas' configs for the second stage…")
	if err := run.orch.RenderReplicaConfigs(2); err != nil {
		run.fail("rendering the second stage: %v", err)
		return false
	}
	run.measures["second_stage_at"] = time.Now().UTC()
	return true
}

// readiness says what a replica has logged of its start since t.
func (run *ssRun) readiness(n int, since time.Time) string {
	rep := run.replica(n)
	run.book.refresh(rep.Container)
	var seen []string
	for _, e := range run.events(rep.Container) {
		if e.At.Before(since) {
			continue
		}
		switch e.Kind {
		case ssBusStarted:
			seen = append(seen, "state bus started as "+e.Member)
		case ssBusReady:
			seen = append(seen, "state bus ready")
		case ssManagerShared:
			seen = append(seen, "manager on the shared bus")
		case ssHeldBack:
			seen = append(seen, fmt.Sprintf("device %d held back", e.Device))
		}
	}
	if len(seen) == 0 {
		return "nothing logged about the shared state yet"
	}
	return strings.Join(slices.Compact(seen), ", ")
}

// waitReady waits for replica n to be ready, as its mode defines it, after
// time since.
func (run *ssRun) waitReady(n int, since, deadline time.Time) (time.Time, bool) {
	rep := run.replica(n)
	for {
		run.book.refresh(rep.Container)
		if at, ok := run.readyAt(rep.Container, since); ok {
			return at, true
		}
		if time.Now().After(deadline) || sleepCtx(run.r.ctx, time.Second) != nil {
			return time.Time{}, false
		}
	}
}

// readyAt returns when the container became ready after since. A state bus
// replica is ready once its state bus connected and the director manager
// runs on it; an embedded queue's once the manager runs on the shared bus;
// a memory replica's once it held back every placed device.
func (run *ssRun) readyAt(container string, since time.Time) (time.Time, bool) {
	var busReady, manager time.Time
	held := map[int64]time.Time{}
	for _, e := range run.events(container) {
		if e.At.Before(since) {
			continue
		}
		switch e.Kind {
		case ssBusReady:
			if busReady.IsZero() {
				busReady = e.At
			}
		case ssManagerShared:
			if manager.IsZero() {
				manager = e.At
			}
		case ssHeldBack:
			if _, ok := held[e.Device]; !ok {
				held[e.Device] = e.At
			}
		}
	}
	switch run.ss.ModeOrDefault() {
	case config.SharedStateModeStateBus:
		if busReady.IsZero() || manager.IsZero() {
			return time.Time{}, false
		}
		return later(busReady, manager), true
	case config.SharedStateModeEmbedded:
		return manager, !manager.IsZero()
	default:
		var last time.Time
		for _, d := range run.ss.PlacedDevices {
			at, ok := held[d]
			if !ok {
				return time.Time{}, false
			}
			last = later(last, at)
		}
		if len(run.ss.PlacedDevices) == 0 {
			return since.Add(15 * time.Second), time.Since(since) > 15*time.Second
		}
		return last, true
	}
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// events returns a container's events, and the process ends the harness
// caused, in time order.
func (run *ssRun) events(container string) []ssEvent {
	events := ssEventsOf(container, run.book.all(container))
	for _, e := range run.ended {
		if e.Container == container {
			events = append(events, e)
		}
	}
	sortSSEvents(events)
	return events
}

// refreshAll reads the new lines of every replica that ran. It returns the
// reads that failed.
func (run *ssRun) refreshAll() []error {
	var errs []error
	for _, rep := range run.replicas {
		if run.ran[rep.N] {
			if err := run.book.refresh(rep.Container); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errs
}

// leaders returns the running replicas that lead at time t.
func (run *ssRun) leaders(t time.Time) []int {
	var out []int
	for _, rep := range run.replicas {
		if run.running[rep.N] && ssLeadingAt(run.events(rep.Container), t) {
			out = append(out, rep.N)
		}
	}
	return out
}

// owners returns the running replicas that run the device at time t.
func (run *ssRun) owners(device int64, t time.Time) []int {
	var out []int
	for _, rep := range run.replicas {
		if run.running[rep.N] && ssHoldsAt(run.events(rep.Container), device, t) {
			out = append(out, rep.N)
		}
	}
	return out
}

// waitOneLeader waits until exactly one running replica leads.
func (run *ssRun) waitOneLeader(deadline time.Time) (int, bool) {
	var last []int
	for {
		run.refreshAll()
		last = run.leaders(current())
		if len(last) == 1 {
			return last[0], true
		}
		if time.Now().After(deadline) || sleepCtx(run.r.ctx, time.Second) != nil {
			return 0, false
		}
	}
}

// checkSettled waits until the replica set is in the state the mode expects,
// and fails the case when it does not get there by the deadline: one leader
// and each placed device on exactly one replica for a shared bus, every
// placed device held back on every replica for memory.
func (run *ssRun) checkSettled(phase string, deadline time.Time) bool {
	var why string
	for {
		run.refreshAll()
		why = run.unsettled()
		if why == "" {
			run.reportPlacement(phase)
			return true
		}
		if time.Now().After(deadline) || sleepCtx(run.r.ctx, 2*time.Second) != nil {
			break
		}
	}
	run.fail("%s: the replicas did not settle: %s", phase, why)
	return false
}

// unsettled says what is not yet as the mode expects, or "".
func (run *ssRun) unsettled() string {
	now := current()
	if !run.ss.SharesBus() {
		for _, rep := range run.replicas {
			if !run.running[rep.N] {
				continue
			}
			for _, d := range run.ss.PlacedDevices {
				if !slices.ContainsFunc(run.events(rep.Container), func(e ssEvent) bool { return e.Kind == ssHeldBack && e.Device == d }) {
					return fmt.Sprintf("%s has not held device %d back", rep.Service, d)
				}
			}
		}
		for _, d := range run.ss.PlacedDevices {
			if o := run.owners(d, now); len(o) > 0 {
				return fmt.Sprintf("device %d runs on %s although the shared state is in memory", d, run.names(o))
			}
		}
		return ""
	}
	if l := run.leaders(now); len(l) != 1 {
		return fmt.Sprintf("%d replicas lead (%s)", len(l), run.names(l))
	}
	for _, d := range run.ss.PlacedDevices {
		if o := run.owners(d, now); len(o) != 1 {
			return fmt.Sprintf("device %d runs on %d replicas (%s)", d, len(o), run.names(o))
		}
	}
	return ""
}

// reportPlacement prints and keeps who leads and who runs which device.
func (run *ssRun) reportPlacement(phase string) {
	now := current()
	placement := map[string]string{}
	var parts []string
	for _, d := range run.ss.PlacedDevices {
		owners := run.names(run.owners(d, now))
		placement[strconv.FormatInt(d, 10)] = owners
		parts = append(parts, fmt.Sprintf("device %d on %s", d, owners))
	}
	leader := run.names(run.leaders(now))
	run.measures[phase+"_leader"] = leader
	run.measures[phase+"_placement"] = placement
	if run.ss.SharesBus() {
		fmt.Printf("  %s: leader %s; %s\n", phase, leader, strings.Join(parts, ", "))
	} else {
		fmt.Printf("  %s: no leader (memory); every placed device held back on every replica\n", phase)
	}
}

func (run *ssRun) names(ns []int) string {
	if len(ns) == 0 {
		return "none"
	}
	var out []string
	for _, n := range ns {
		out = append(out, run.replica(n).Service)
	}
	return strings.Join(out, "+")
}

// healthOf asks replica n's /health endpoint, which listens on loopback only,
// from inside its container. It keeps the answer as evidence.
func (run *ssRun) healthOf(n int, label string) (status, message, overall string, ok bool) {
	rep := run.replica(n)
	out, err := exec.Command("docker", "exec", rep.Container, "wget", "-qO-", "-T", "5", "http://127.0.0.1:15551/health").CombinedOutput()
	run.health[fmt.Sprintf("%s-%s", label, rep.Service)] = string(out)
	if err != nil {
		return "", strings.TrimSpace(string(out)), "", false
	}
	var h struct {
		Status     string `json:"status"`
		Components map[string]struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"components"`
	}
	if json.Unmarshal(out, &h) != nil {
		return "", "unreadable /health answer", "", false
	}
	c := h.Components["shared_state"]
	return c.Status, c.Message, h.Status, true
}

// checkHealthAll checks the shared_state component of every running replica.
func (run *ssRun) checkHealthAll(label string) {
	got := map[string]string{}
	for _, rep := range run.replicas {
		if !run.running[rep.N] {
			continue
		}
		want := run.ss.ExpectHealthOrDefault()
		if rep.N == run.bad {
			want = run.ss.BadHealthOrDefault()
		}
		status, message, overall, ok := run.healthOf(rep.N, label)
		if !ok {
			run.fail("%s: /health of %s did not answer: %s", label, rep.Service, message)
			continue
		}
		got[rep.Service] = status
		if status != want {
			run.fail("%s: /health of %s reports shared_state %q (%s), want %q", label, rep.Service, status, message, want)
			continue
		}
		fmt.Printf("    /health %s: shared_state %s (overall %s)\n", rep.Service, status, overall)
	}
	run.measures[label+"_health"] = got
}

// waitBaseline waits until every stream's receiver holds its baseline.
func (run *ssRun) waitBaseline() bool {
	deadline := earliest(time.Now().Add(2*time.Minute), run.deadline)
	ports := run.orch.ReceiverMetricsPorts()
	for _, st := range run.ss.Streams {
		if st.ExpectNothing {
			continue
		}
		want := max(st.MinBaseline, 1)
		var have int64
		for {
			if m, err := run.r.queryReceiverMetrics(ports[st.Receiver], 5*time.Second); err == nil {
				have = m.LinesReceived
				if have >= want {
					break
				}
			}
			if time.Now().After(deadline) || sleepCtx(run.r.ctx, 2*time.Second) != nil {
				run.fail("stream %s: the receiver holds %d records, below its baseline of %d", st.StreamName(), have, want)
				return false
			}
		}
		fmt.Printf("  stream %s: baseline reached (%d records)\n", st.StreamName(), have)
	}
	run.measures["baseline_at"] = time.Now().UTC()
	return true
}

// act runs the case's action.
func (run *ssRun) act() {
	ss := run.ss
	run.measures["action"] = sharedStateActionLabel(ss.Action)
	switch ss.Action {
	case config.SharedStateActionNone:
	case config.SharedStateActionPlacement:
		run.watchPlacement(time.Duration(ss.HoldOrDefault()) * time.Second)
	case config.SharedStateActionKillLeader:
		if leader, ok := run.oneLeader(); ok {
			run.disrupt(leader, "kill")
		}
	case config.SharedStateActionStopLeader:
		if leader, ok := run.oneLeader(); ok {
			run.disrupt(leader, "stop")
		}
	case config.SharedStateActionKillOwner:
		if owner, ok := run.ownerNotLeading(); ok {
			run.disrupt(owner, "kill")
		}
	case config.SharedStateActionPauseStore:
		run.pauseStore()
	case config.SharedStateActionIsolateReplica:
		run.isolateReplica()
	case config.SharedStateActionScaleOutIn:
		run.scaleOutIn()
	case config.SharedStateActionColdRestart:
		run.coldRestart()
	case config.SharedStateActionWrongCredentials:
		// The start was the test; the run goes on under load to show the good
		// replicas are not disturbed.
	case config.SharedStateActionStoreCalls:
		run.storeCalls()
	case config.SharedStateActionScaleBacklog:
		run.scaleBacklog()
	case config.SharedStateActionMoveThenKill:
		run.moveThenKill()
	case config.SharedStateActionStopOwner:
		if owner, ok := run.ownerNotLeading(); ok {
			run.disruptAndReplace(owner, "stop", false)
		}
	case config.SharedStateActionRollingRestart:
		run.rollingRestart()
	case config.SharedStateActionMoveAcrossCutLink:
		run.moveAcrossCutLink()
	}
}

func (run *ssRun) oneLeader() (int, bool) {
	run.refreshAll()
	l := run.leaders(current())
	if len(l) != 1 {
		run.fail("the action needs one leader, and %d replicas lead (%s)", len(l), run.names(l))
		return 0, false
	}
	return l[0], true
}

// ownerNotLeading returns a running replica that runs a placed device and
// does not lead, preferring one no generator sends to.
func (run *ssRun) ownerNotLeading() (int, bool) {
	run.refreshAll()
	now := current()
	leaders := run.leaders(now)
	targeted := run.generatorTargets()
	var candidates []int
	for _, d := range run.ss.PlacedDevices {
		for _, o := range run.owners(d, now) {
			if !slices.Contains(leaders, o) && !slices.Contains(candidates, o) {
				candidates = append(candidates, o)
			}
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return !targeted[candidates[i]] && targeted[candidates[j]]
	})
	if len(candidates) == 0 {
		run.fail("no replica runs a placed device without leading (leader %s): the action cannot run", run.names(leaders))
		return 0, false
	}
	return candidates[0], true
}

// generatorTargets returns the replicas a generator sends to.
func (run *ssRun) generatorTargets() map[int]bool {
	out := map[int]bool{}
	for _, g := range run.tc.AllGenerators() {
		host, _, _ := strings.Cut(g.Target, ":")
		if n, err := strconv.Atoi(strings.TrimPrefix(host, "subject-")); err == nil {
			out[n] = true
		}
	}
	return out
}

// watchPlacement watches the replicas for d and fails the case if a placed
// device ever runs on more or fewer than one replica, or leadership changes.
func (run *ssRun) watchPlacement(d time.Duration) {
	fmt.Printf("  watching the placement for %s…\n", d)
	leaderBefore := run.names(run.leaders(current()))
	end := earliest(time.Now().Add(d), run.deadline)
	for time.Now().Before(end) {
		run.refreshAll()
		if why := run.unsettled(); why != "" {
			run.fail("placement: %s", why)
			return
		}
		if sleepCtx(run.r.ctx, 3*time.Second) != nil {
			return
		}
	}
	if leaderAfter := run.names(run.leaders(current())); leaderAfter != leaderBefore {
		run.fail("placement: the leader changed from %s to %s with no disruption", leaderBefore, leaderAfter)
	}
	run.reportPlacement("hold")
}

// disrupt kills replica n (SIGKILL) or stops it (docker stop, SIGTERM then
// up to 30 seconds), measures how long the others take to lead and to run the
// devices it ran, and then replaces it with a new container, as a container
// platform would, and measures how long that takes to rejoin.
func (run *ssRun) disrupt(n int, how string) {
	run.disruptAndReplace(n, how, true)
}

// disruptAndReplace kills or stops replica n, measures the takeover and, when
// replace is set, replaces the replica as the platform does. Without it, the
// replica set must settle on the replicas that are left, as after a
// scale-in.
func (run *ssRun) disruptAndReplace(n int, how string, replace bool) {
	rep := run.replica(n)
	run.refreshAll()
	now := current()
	wasLeader := slices.Contains(run.leaders(now), n)
	var owned []int64
	for _, d := range run.ss.PlacedDevices {
		if slices.Contains(run.owners(d, now), n) {
			owned = append(owned, d)
		}
	}
	role := "follower"
	if wasLeader {
		role = "leader"
	}
	fmt.Printf("  %s %s (%s, runs devices %v)…\n", map[string]string{"kill": "killing", "stop": "stopping"}[how], rep.Service, role, owned)
	run.measures["victim"] = map[string]any{"replica": rep.Service, "leader": wasLeader, "devices": owned, "how": how}
	if why := untestedDeviceFailover(run.ss.PlacedDevices, owned); why != "" {
		run.fail("%s", why)
	}

	// A read that fails here loses nothing: endReplica reads every line
	// since the last one read.
	_ = run.book.refresh(rep.Container)
	issued := time.Now()
	var err error
	if how == "kill" {
		err = exec.Command("docker", "kill", "-s", "KILL", rep.Container).Run()
	} else {
		err = exec.Command("docker", "stop", "-t", "30", rep.Container).Run()
	}
	if err != nil {
		run.fail("docker %s %s: %v (the disruption did not happen)", how, rep.Service, err)
		return
	}
	took := time.Since(issued)
	// The daemon's own time of the end is on the clock the log lines carry.
	endedAt := containerFinishedAt(rep.Container, time.Now())
	run.endReplica(n, endedAt, "docker "+how, how == "stop")
	// A kill ends the process at once. A stop starts the handover when the
	// signal is sent: the end, less the time the stop took, on the same clock.
	from := endedAt
	if how == "stop" {
		from = endedAt.Add(-took)
		fmt.Printf("    %s exited %.1fs after the stop began\n", rep.Service, took.Seconds())
		run.measures["stop_exit_seconds"] = took.Seconds()
	}

	settle := earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline)
	failover := map[string]float64{}
	var newLeaderAt time.Time
	for {
		run.refreshAll()
		done := true
		if wasLeader && newLeaderAt.IsZero() {
			newLeaderAt = run.firstAfter(ssLeaderUp, 0, from, rep.Container)
			done = done && !newLeaderAt.IsZero()
		}
		for _, d := range owned {
			key := strconv.FormatInt(d, 10)
			if _, ok := failover[key]; ok {
				continue
			}
			if at := run.firstAfter(ssLeaseAcquired, d, from, rep.Container); !at.IsZero() {
				failover[key] = at.Sub(from).Seconds()
			} else {
				done = false
			}
		}
		if done || time.Now().After(settle) || sleepCtx(run.r.ctx, time.Second) != nil {
			break
		}
	}
	if wasLeader {
		if newLeaderAt.IsZero() {
			run.fail("no other replica led within %ds of the %s", run.ss.SettleOrDefault(), how)
		} else {
			secs := newLeaderAt.Sub(from).Seconds()
			run.measures["leader_failover_seconds"] = secs
			fmt.Printf("    a new leader within %.1fs\n", secs)
		}
	}
	var worst float64
	for _, d := range owned {
		key := strconv.FormatInt(d, 10)
		secs, ok := failover[key]
		if !ok {
			run.fail("device %d did not run on another replica within %ds of the %s", d, run.ss.SettleOrDefault(), how)
			continue
		}
		worst = max(worst, secs)
		fmt.Printf("    device %d runs again after %.1fs\n", d, secs)
	}
	run.measures["device_failover_seconds"] = failover
	if len(failover) > 0 {
		run.measures["device_failover_worst_seconds"] = worst
		if limit := run.ss.MaxFailoverSeconds; limit > 0 && worst > limit {
			run.fail("the slowest moved device took %.1fs to run again, above max_failover_seconds %.0f", worst, limit)
		}
	}

	if !replace {
		run.checkSettled("after", earliest(time.Now().Add(60*time.Second), run.deadline))
		run.checkHealthAll("after")
		return
	}

	// The platform replaces the lost replica with a new one, on an empty disk.
	fmt.Printf("  replacing %s with a new container…\n", rep.Service)
	// The replacement takes the container's log with it, so this is the last
	// read of the lost replica's lines. An earlier read that failed is caught
	// up here.
	if err := run.book.refresh(rep.Container); err != nil {
		run.fail("reading %s's log before replacing it: %v", rep.Service, err)
	}
	if err := run.orch.RecreateServices(rep.Service); err != nil {
		run.fail("replacing %s: %v", rep.Service, err)
		return
	}
	run.markStarted(n)
	replacedAt := containerStartedAt(rep.Container, time.Now())
	if readyAt, ok := run.waitReady(n, replacedAt, earliest(time.Now().Add(run.replicaWarmup()), run.deadline)); ok {
		run.measures["rejoin_seconds"] = readyAt.Sub(replacedAt).Seconds()
		fmt.Printf("    %s rejoined after %.1fs\n", rep.Service, readyAt.Sub(replacedAt).Seconds())
	} else {
		run.fail("%s did not rejoin after it was replaced: %s", rep.Service, run.readiness(n, replacedAt))
	}
	run.checkSettled("after", earliest(time.Now().Add(60*time.Second), run.deadline))
	run.checkHealthAll("after")
}

// untestedDeviceFailover returns why a disruption does not test the failover
// of the placed devices: the replica it ends runs none of them. Without this
// check, a case whose victim ran no placed device passed with no device
// checked at all. It returns "" when the victim runs a placed device, or when
// the case places none.
func untestedDeviceFailover(placed, owned []int64) string {
	if len(placed) > 0 && len(owned) == 0 {
		return "the victim ran no placed device, so the failover of the devices was not tested"
	}
	return ""
}

// firstAfter returns the time of the first event of the kind (for the device,
// when one is given) that a container other than except logged after t, or
// the zero time.
func (run *ssRun) firstAfter(kind ssKind, device int64, t time.Time, except string) time.Time {
	var first time.Time
	for _, rep := range run.replicas {
		if rep.Container == except {
			continue
		}
		for _, e := range run.events(rep.Container) {
			if e.Kind != kind || !e.At.After(t) || (device != 0 && e.Device != device) {
				continue
			}
			if first.IsZero() || e.At.Before(first) {
				first = e.At
			}
			break
		}
	}
	return first
}

// current is a time after every event seen so far: the replica set's state
// as its logs tell it now. The log lines carry the Docker daemon's clock,
// which on Docker Desktop can drift from the host's, so "now" on the host is
// no bound for them.
func current() time.Time { return time.Now().Add(time.Hour) }

// containerFinishedAt returns when Docker saw the container's process end,
// on the daemon's clock, or fallback when it cannot say.
func containerFinishedAt(container string, fallback time.Time) time.Time {
	return containerStateTime(container, "{{.State.FinishedAt}}", fallback)
}

// containerStartedAt returns when Docker started the container's process, on
// the daemon's clock, or fallback when it cannot say.
func containerStartedAt(container string, fallback time.Time) time.Time {
	return containerStateTime(container, "{{.State.StartedAt}}", fallback)
}

func containerStateTime(container, format string, fallback time.Time) time.Time {
	out, err := exec.Command("docker", "inspect", "-f", format, container).Output()
	if err != nil {
		return fallback
	}
	at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(out)))
	if err != nil || at.Year() < 2000 {
		return fallback
	}
	return at
}

// lastEvent returns the time of the last line any replica logged, or the
// host's time when none did.
func (run *ssRun) lastEvent() time.Time {
	last := time.Time{}
	for _, rep := range run.replicas {
		if lines := run.book.all(rep.Container); len(lines) > 0 {
			last = later(last, lines[len(lines)-1].At)
		}
	}
	if last.IsZero() {
		return time.Now()
	}
	return last
}

// drain waits for the generators to finish and the receivers to settle.
func (run *ssRun) drain() {
	remaining := time.Until(run.deadline) - time.Minute
	if remaining > 0 {
		if err := run.orch.WaitForGeneratorExit(remaining); err != nil {
			run.note("the generators did not finish in time: %v", err)
		}
	}
	quiet := 15 * time.Second
	if d, err := time.ParseDuration(run.tc.Correctness.DrainQuietWindow); err == nil && d > 0 {
		quiet = d
	}
	limit := 2 * time.Minute
	if run.tc.Correctness.DrainSeconds > 0 {
		limit = time.Duration(run.tc.Correctness.DrainSeconds) * time.Second
	}
	deadline := earliest(time.Now().Add(limit), run.deadline)
	ports := run.orch.ReceiverMetricsPorts()
	last := map[string]int64{}
	stableSince := time.Now()
	fmt.Println("  draining…")
	for time.Now().Before(deadline) {
		changed := false
		for id, port := range ports {
			if m, err := run.r.queryReceiverMetrics(port, 5*time.Second); err == nil && m.LinesReceived != last[id] {
				last[id] = m.LinesReceived
				changed = true
			}
		}
		if changed {
			stableSince = time.Now()
		} else if time.Since(stableSince) >= quiet {
			break
		}
		if sleepCtx(run.r.ctx, 2*time.Second) != nil {
			return
		}
	}
}

// analyse checks the streams, the ownership over the whole run, the secrets,
// and counts the store calls.
func (run *ssRun) analyse() {
	// The ownership and secret checks read the replicas' logs. A log the
	// harness could not read would let them pass on part of the evidence.
	for _, err := range run.refreshAll() {
		run.fail("reading a replica's log for the final checks: %v", err)
	}
	run.checkStreams()
	run.checkOwnershipOverlaps()
	run.countStoreCalls()
	run.checkSecrets()
	run.checkLogOnce()
}

// checkStreams compares what each generator sent with what its receiver
// holds. It counts records by the sequence numbers the generator put in
// them, so a line that arrived cut or garbled is not taken for a record.
func (run *ssRun) checkStreams() {
	ports := run.orch.ReceiverMetricsPorts()
	streams := map[string]any{}
	for _, st := range run.ss.Streams {
		out, _ := exec.Command("docker", "logs", "bench-generator-"+st.Generator).Output()
		sent := run.r.parseGeneratorStats(string(out)).LinesSent
		m, err := run.r.queryReceiverMetrics(ports[st.Receiver], 10*time.Second)
		if err != nil {
			run.fail("stream %s: the receiver did not answer: %v", st.StreamName(), err)
			continue
		}
		seq, err := receiverSeqCounts(ports[st.Receiver])
		if err != nil {
			run.fail("stream %s: the receiver did not count the records' sequence numbers (%v); these cases need the receiver of PipeBench v0.4.7 or later", st.StreamName(), err)
			continue
		}
		got := ssStreamCounts{Received: m.LinesReceived, Unique: seq.Unique, Duplicates: seq.Duplicates, Unparsed: seq.Unparsed}
		loss := max(sent-got.Unique, 0)
		dupPct := got.duplicatePct()
		streams[st.StreamName()] = map[string]any{
			"sent": sent, "received": got.Received, "unique": got.Unique,
			"duplicates": got.Duplicates, "duplicate_pct": dupPct, "lost": loss,
			"unnumbered": got.Unparsed,
		}
		run.linesIn += sent
		run.linesOut += got.Received
		run.uniqueOut += got.Unique
		run.perReceiver[st.Receiver] = got.Received
		fmt.Printf("  stream %s: sent %d, received %d (%d distinct, %d repeated, %.2f%%, %d without a number), lost %d\n",
			st.StreamName(), sent, got.Received, got.Unique, got.Duplicates, dupPct, got.Unparsed, loss)
		for _, why := range judgeStream(st, sent, got) {
			run.fail("%s", why)
		}
	}
	run.measures["streams"] = streams
}

// ssStreamCounts is what a receiver holds of one stream: every line that
// arrived, the distinct records among them by their sequence numbers, the
// copies of records that arrived before, and the lines that carry no number.
type ssStreamCounts struct {
	Received, Unique, Duplicates, Unparsed int64
}

func (c ssStreamCounts) duplicatePct() float64 {
	if c.Unique == 0 {
		return 0
	}
	return 100 * float64(c.Duplicates) / float64(c.Unique)
}

// receiverSeqCounts reads what a receiver's sequence tracking counted.
func receiverSeqCounts(port int) (seqCheckResult, error) {
	var out seqCheckResult
	resp, err := receiverClient.Get(fmt.Sprintf("http://localhost:%d/seqcheck?acked=", port)) //nolint:noctx // short local call
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, err
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("/seqcheck: %s", resp.Status)
	}
	return out, json.Unmarshal(body, &out)
}

// judgeStream returns why a stream fails, if it does, from what its generator
// sent and what its receiver holds.
func judgeStream(st config.SharedStateStream, sent int64, got ssStreamCounts) []string {
	// A stream whose devices must not run is judged on arrivals alone. Its
	// generator may send nothing at all: a TCP sender whose listener never
	// started can give up before its first write.
	if st.ExpectNothing {
		if got.Received > 0 {
			return []string{fmt.Sprintf("stream %s: %d records arrived, and none may: its devices must not run", st.StreamName(), got.Received)}
		}
		return nil
	}
	if sent == 0 {
		return []string{fmt.Sprintf("stream %s: the generator sent nothing", st.StreamName())}
	}
	var out []string
	if got.Unparsed > 0 {
		out = append(out, fmt.Sprintf("stream %s: %d lines arrived without a record's sequence number, so they arrived cut or garbled", st.StreamName(), got.Unparsed))
	}
	if got.Unique > sent {
		out = append(out, fmt.Sprintf("stream %s: %d distinct records arrived, %d more than the generator sent", st.StreamName(), got.Unique, got.Unique-sent))
	}
	if loss := max(sent-got.Unique, 0); loss > st.MaxLoss {
		out = append(out, fmt.Sprintf("stream %s: %d records were lost, above max_loss %d", st.StreamName(), loss, st.MaxLoss))
	}
	dupPct := got.duplicatePct()
	switch {
	case st.MaxDuplicates != nil:
		if got.Duplicates > *st.MaxDuplicates {
			out = append(out, fmt.Sprintf("stream %s: %d records arrived more than once, above max_duplicates %d", st.StreamName(), got.Duplicates, *st.MaxDuplicates))
		}
	case dupPct > st.MaxDuplicatePct:
		out = append(out, fmt.Sprintf("stream %s: %d records arrived more than once (%.2f%%), above max_duplicate_pct %.2f", st.StreamName(), got.Duplicates, dupPct, st.MaxDuplicatePct))
	}
	return out
}

// checkOwnershipOverlaps fails the case for every time two replicas ran the
// same placed device at once.
func (run *ssRun) checkOwnershipOverlaps() {
	until := run.lastEvent()
	var intervals []ssInterval
	for _, rep := range run.replicas {
		intervals = append(intervals, ssOwnership(rep.Container, run.events(rep.Container), until)...)
	}
	overlaps := ssOverlaps(intervals, 0)
	var report []map[string]any
	for _, o := range overlaps {
		report = append(report, map[string]any{
			"device": o.Device, "a": o.A, "b": o.B,
			"from": o.From.UTC(), "seconds": o.Seconds(), "judged": o.To.Sub(o.From) > ssOverlapTolerance,
		})
	}
	fails, notes := judgeOverlaps(overlaps)
	for _, f := range fails {
		run.fail("%s", f)
	}
	for _, n := range notes {
		run.note("%s", n)
	}
	run.measures["ownership_overlaps"] = report
	run.measures["ownership_intervals"] = len(intervals)
}

// ssOverlapTolerance is how long two replicas may seem to run one device at
// once before the case fails. Docker stamps each line as it reads it from its
// container, so the lines of two containers can come out a few milliseconds
// out of order. A double run of a tenth of a second is no such jitter.
const ssOverlapTolerance = 100 * time.Millisecond

// judgeOverlaps fails every overlap longer than ssOverlapTolerance, and
// reports the shorter ones as notes, so that none goes unseen.
func judgeOverlaps(overlaps []ssOverlap) (fails, notes []string) {
	for _, o := range overlaps {
		what := fmt.Sprintf("device %d ran on %s and %s at once for %.3fs, from %s", o.Device, o.A, o.B, o.Seconds(), o.From.UTC().Format("15:04:05.000"))
		if o.To.Sub(o.From) > ssOverlapTolerance {
			fails = append(fails, what)
		} else {
			notes = append(notes, what+fmt.Sprintf(": within the %s the log stamps of two containers can be apart, so it is reported, not judged", ssOverlapTolerance))
		}
	}
	return fails, notes
}

// countStoreCalls counts the replicas' calls to the shared state's container,
// over the run and under load.
func (run *ssRun) countStoreCalls() {
	if !run.ss.CountsStoreCalls() {
		why := fmt.Sprintf("not counted: the shared state is in %s, and the harness counts store calls only from Azurite's access log", run.ss.StoreName())
		run.measures["store_calls"] = why
		fmt.Printf("  store calls %s\n", why)
		return
	}
	if err := run.book.refresh("bench-azurite"); err != nil {
		run.fail("reading Azurite's access log to count the store calls: %v", err)
	}
	calls := parseAzuriteCalls(run.book.all("bench-azurite"))
	container := run.ss.StoreContainerOrDefault()
	whole := storeCallRates(calls, container, run.started, time.Now())
	rates := map[string]any{"whole_run": whole}
	if from, ok := run.measures["baseline_at"].(time.Time); ok {
		to := time.Now()
		if victim, ok := run.measures["victim"]; ok && victim != nil {
			// Under load and undisturbed: from the baseline to the action.
			for _, e := range run.ended {
				if e.At.After(from) && e.At.Before(to) {
					to = e.At
				}
			}
		}
		if to.Sub(from) >= 10*time.Second {
			rates["under_load"] = storeCallRates(calls, container, from, to)
		}
	}
	run.measures["store_calls"] = rates
	fmt.Printf("  store calls to %q: %.2f writes/s, %.2f reads and lists/s over the run (%d failed)\n",
		container, whole.WritesPerSec, whole.ReadsPerSec, whole.FailedRequests)
	printCallKinds(whole)
	if l, ok := rates["under_load"].(ssCallRates); ok {
		fmt.Printf("  store calls under load: %.2f writes/s, %.2f reads and lists/s over %.0fs\n", l.WritesPerSec, l.ReadsPerSec, l.Seconds)
		printCallKinds(l)
	}
}

// checkSecrets fails the case for every place a secret appears in a
// replica's log or /health answer.
func (run *ssRun) checkSecrets() {
	if len(run.ss.SecretValues) == 0 {
		return
	}
	texts := map[string]string{}
	for _, rep := range run.replicas {
		texts[rep.Service+" log"] = run.book.text(rep.Container)
	}
	for label, body := range run.health {
		texts["/health "+label] = body
	}
	hits := scanForSecrets(run.ss.SecretValues, texts)
	run.measures["secret_hits"] = hits
	for _, h := range hits {
		run.fail("a secret appears in %s: %s, in: %s", h.Where, h.What, h.Line)
	}
	if len(hits) == 0 {
		fmt.Printf("  no secret in %d logs and /health answers ✓\n", len(texts))
	}
}

// teardown keeps the evidence and removes every container of the run.
func (run *ssRun) teardown() {
	run.refreshAll()
	run.book.refresh("bench-azurite")
	run.saveEvidence()
	if run.r.opts.NoCleanup {
		return
	}
	fmt.Println("  tearing down…")
	for _, rep := range run.replicas {
		_ = exec.Command("docker", "rm", "-f", rep.Container).Run()
	}
	_ = run.orch.Down()
}

// saveEvidence writes every replica's log, Azurite's access log, every
// /health answer and the measures into the run's evidence directory.
func (run *ssRun) saveEvidence() {
	_ = os.RemoveAll(run.evidence)
	if err := os.MkdirAll(run.evidence, 0o755); err != nil {
		fmt.Printf("  warning: cannot keep the evidence: %v\n", err)
		return
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(run.evidence, name), []byte(body), 0o644); err != nil {
			fmt.Printf("  warning: cannot write %s: %v\n", name, err)
		}
	}
	for _, rep := range run.replicas {
		if text := run.book.text(rep.Container); text != "" {
			write(rep.Service+".log", text)
		}
	}
	write("azurite.log", run.book.text("bench-azurite"))
	if service := run.ss.StoreService(); service != "" && service != "azurite" {
		out, _ := exec.Command("docker", "logs", "-t", "bench-"+service).CombinedOutput()
		write(service+".log", string(out))
	}
	for label, body := range run.health {
		write("health-"+label+".json", body)
	}
	for _, g := range run.tc.AllGenerators() {
		out, _ := exec.Command("docker", "logs", "bench-generator-"+g.ID).CombinedOutput()
		write("generator-"+g.ID+".log", string(out))
	}
	run.measures["errors"] = run.errs
	if data, err := json.MarshalIndent(run.measures, "", "  "); err == nil {
		write("measures.json", string(data))
	}
}
