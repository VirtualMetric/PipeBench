package runner

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The actions of a shared-state case that disturb the store, the network or
// the size of the replica set, rather than one replica.

// pauseStore pauses Azurite for the outage, so no replica can reach the
// store, then lets it go on. It measures when the leader stops leading and
// when the replicas stop their placed devices, samples /health and the
// outage streams while the store is away, and measures how long the replica
// set takes to lead and run every placed device again once it is back.
func (run *ssRun) pauseStore() {
	outage := time.Duration(run.ss.OutageOrDefault()) * time.Second
	hold := time.Duration(run.ss.OutageHoldOthersSeconds) * time.Second
	run.refreshAll()
	// With a hold, the replica that leads now is the one that must take the
	// lease again once the store is back.
	leader := 0
	if hold > 0 {
		var ok bool
		if leader, ok = run.oneLeader(); !ok {
			return
		}
	}
	fmt.Printf("  pausing Azurite for %s…\n", outage)
	paused := time.Now()
	if err := exec.Command("docker", "pause", "bench-azurite").Run(); err != nil {
		run.fail("docker pause bench-azurite: %v (the outage did not happen)", err)
		return
	}
	run.measures["outage_from"] = paused.UTC()

	// While the store is away: /health on every replica, and the streams.
	health := map[string]map[string]int{}
	end := earliest(paused.Add(outage), run.deadline)
	base := newOutageBaseline(paused)
	for time.Now().Before(end) {
		base.take(run.streamCounts)
		for _, rep := range run.replicas {
			if !run.running[rep.N] {
				continue
			}
			status, _, _, _ := run.healthOf(rep.N, "outage")
			if health[rep.Service] == nil {
				health[rep.Service] = map[string]int{}
			}
			health[rep.Service][status]++
		}
		if sleepCtx(run.r.ctx, 3*time.Second) != nil {
			break
		}
	}
	during := run.streamCounts()
	held := run.holdOthers(leader, hold)
	if err := exec.Command("docker", "unpause", "bench-azurite").Run(); err != nil {
		run.fail("docker unpause bench-azurite: %v", err)
		run.thaw(held)
		return
	}
	resumed := time.Now()
	if len(held) > 0 {
		_ = sleepCtx(run.r.ctx, hold)
		run.thaw(held)
	}
	run.measures["outage_to"] = resumed.UTC()
	run.measures["outage_health"] = health
	fmt.Printf("    /health while the store was away: %v\n", health)
	run.judgeOutageStreams(base, during, resumed)

	run.recoverAfterOutage(paused, resumed, "")

	if leader > 0 {
		next, at := run.firstLeaderAfter(resumed.Add(run.daemonSkew()))
		if next == 0 {
			return
		}
		run.measures["outage_leader_after"] = map[string]any{
			"replica":                  run.replica(next).Service,
			"led_before":               next == leader,
			"seconds_after_store_back": at.Sub(resumed.Add(run.daemonSkew())).Seconds(),
		}
		if next != leader {
			run.note("%s led after the outage, not %s, which led before it: the hold did not give the old leader the lease", run.replica(next).Service, run.replica(leader).Service)
		} else {
			fmt.Printf("    %s, which led before the outage, led again after it\n", run.replica(leader).Service)
		}
	}
}

// holdOthers freezes every running replica but keep, when hold is set, so
// keep alone can take the store's lease as the store comes back. It returns
// the replicas it froze.
func (run *ssRun) holdOthers(keep int, hold time.Duration) []ssReplica {
	if hold <= 0 {
		return nil
	}
	var held []ssReplica
	var names []string
	for _, rep := range run.replicas {
		if !run.running[rep.N] || rep.N == keep {
			continue
		}
		if err := exec.Command("docker", "pause", rep.Container).Run(); err != nil {
			run.fail("docker pause %s: %v", rep.Service, err)
			continue
		}
		held = append(held, rep)
		names = append(names, rep.Service)
	}
	run.measures["outage_held_replicas"] = names
	fmt.Printf("    froze %s for %s, so that %s, which led before the outage, alone can take the lease\n", strings.Join(names, ", "), hold, run.replica(keep).Service)
	return held
}

// thaw lets the replicas holdOthers froze run on.
func (run *ssRun) thaw(held []ssReplica) {
	for _, rep := range held {
		if err := exec.Command("docker", "unpause", rep.Container).Run(); err != nil {
			run.fail("docker unpause %s: %v", rep.Service, err)
		}
	}
}

// firstLeaderAfter returns the replica whose log says first, after t, that
// it became leader, and when. It returns 0 when none did.
func (run *ssRun) firstLeaderAfter(t time.Time) (int, time.Time) {
	n, first := 0, time.Time{}
	for _, rep := range run.replicas {
		for _, e := range run.events(rep.Container) {
			if e.Kind != ssLeaderUp || !e.At.After(t) {
				continue
			}
			if first.IsZero() || e.At.Before(first) {
				n, first = rep.N, e.At
			}
			break
		}
	}
	return n, first
}

// isolateReplica disconnects the leader from the bench network for the
// outage, so it reaches neither the store nor the other replicas, then
// connects it again. The cut-off replica must stop its devices before the
// others take them over, and must rejoin without taking any back.
func (run *ssRun) isolateReplica() {
	victim, ok := run.oneLeader()
	if !ok {
		return
	}
	rep := run.replica(victim)
	network := containerNetwork(rep.Container)
	if network == "" {
		run.fail("cannot find the network of %s", rep.Service)
		return
	}
	run.refreshAll()
	var owned []int64
	for _, d := range run.ss.PlacedDevices {
		if slices.Contains(run.owners(d, current()), victim) {
			owned = append(owned, d)
		}
	}
	outage := time.Duration(run.ss.OutageOrDefault()) * time.Second
	fmt.Printf("  cutting %s (the leader, runs devices %v) off the network for %s…\n", rep.Service, owned, outage)
	run.measures["victim"] = map[string]any{"replica": rep.Service, "leader": true, "devices": owned, "how": "isolate"}
	cut := time.Now()
	if err := exec.Command("docker", "network", "disconnect", network, rep.Container).Run(); err != nil {
		run.fail("docker network disconnect %s: %v (the outage did not happen)", rep.Service, err)
		return
	}
	run.measures["outage_from"] = cut.UTC()
	statuses := map[string]int{}
	end := earliest(cut.Add(outage), run.deadline)
	base := newOutageBaseline(cut)
	for time.Now().Before(end) {
		base.take(run.streamCounts)
		status, _, _, _ := run.healthOf(victim, "isolated")
		statuses[status]++
		if sleepCtx(run.r.ctx, 3*time.Second) != nil {
			break
		}
	}
	during := run.streamCounts()
	if err := exec.Command("docker", "network", "connect", network, rep.Container).Run(); err != nil {
		run.fail("docker network connect %s: %v", rep.Service, err)
		return
	}
	back := time.Now()
	run.measures["outage_to"] = back.UTC()
	run.measures["isolated_health"] = statuses
	fmt.Printf("    /health of %s while it was cut off: %v\n", rep.Service, statuses)
	run.judgeOutageStreams(base, during, back)

	run.recoverAfterOutage(cut, back, rep.Container)
}

// recoverAfterOutage measures the outage's effect from the logs, and waits for
// the replica set to settle again once the outage ended. victim is the
// container that was cut off, or "" when every replica lost the store.
func (run *ssRun) recoverAfterOutage(from, to time.Time, victim string) {
	// The logs carry the daemon's clock. Shift the host's times onto it by the
	// difference the daemon reports for "now".
	skew := run.daemonSkew()
	from, to = from.Add(skew), to.Add(skew)
	run.refreshAll()

	stops := map[string]float64{}
	for _, rep := range run.replicas {
		if victim != "" && rep.Container != victim {
			continue
		}
		for _, e := range run.events(rep.Container) {
			if e.At.Before(from) || e.At.After(to) {
				continue
			}
			switch e.Kind {
			case ssLeaderDown:
				if _, ok := stops["leader"]; !ok {
					stops["leader"] = e.At.Sub(from).Seconds()
				}
			case ssLeaseEnded:
				key := "device " + strconv.FormatInt(e.Device, 10)
				if _, ok := stops[key]; !ok {
					stops[key] = e.At.Sub(from).Seconds()
				}
			}
		}
	}
	run.measures["outage_stops_seconds"] = stops
	fmt.Printf("    after the outage began, stopped: %v (seconds)\n", stops)

	// A cut-off replica's devices: when another replica ran them, against when
	// the cut-off one stopped them.
	if victim != "" {
		takeovers := map[string]float64{}
		for _, d := range run.ss.PlacedDevices {
			if at := run.firstAfter(ssLeaseAcquired, d, from, victim); !at.IsZero() && at.Before(to) {
				takeovers[strconv.FormatInt(d, 10)] = at.Sub(from).Seconds()
			}
		}
		run.measures["outage_takeover_seconds"] = takeovers
		fmt.Printf("    the others ran the cut-off replica's devices after: %v (seconds)\n", takeovers)
		if at := run.firstAfter(ssLeaderUp, 0, from, victim); !at.IsZero() && at.Before(to) {
			run.measures["outage_new_leader_seconds"] = at.Sub(from).Seconds()
			fmt.Printf("    a new leader after %.1fs\n", at.Sub(from).Seconds())
		}
	}

	// Recovery, from the end of the outage. The replica set must stay settled
	// for 15 seconds: right after an outage a replica can look like the
	// leader, and run the devices, for a moment before its bus fails again.
	settle := earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline)
	settled := false
	var calmSince time.Time
	for {
		run.refreshAll()
		if run.unsettled() == "" {
			if calmSince.IsZero() {
				calmSince = time.Now()
			}
			if time.Since(calmSince) >= 15*time.Second {
				settled = true
				break
			}
		} else {
			calmSince = time.Time{}
		}
		if time.Now().After(settle) || sleepCtx(run.r.ctx, 2*time.Second) != nil {
			break
		}
	}
	// Delivery must resume too: every stream that still sends must grow.
	if settled {
		before := run.streamCounts()
		if sleepCtx(run.r.ctx, 20*time.Second) == nil {
			after := run.streamCounts()
			for _, st := range run.ss.Streams {
				if st.ExpectNothing {
					continue
				}
				if after[st.StreamName()] <= before[st.StreamName()] && run.generatorRunning(st.Generator) {
					run.fail("stream %s did not resume after the outage: no record arrived in 20s while its generator sent", st.StreamName())
				}
			}
		}
	}
	if !settled {
		run.fail("the replicas did not settle within %ds after the outage ended: %s", run.ss.SettleOrDefault(), run.unsettled())
	} else {
		recovered := map[string]float64{}
		for _, d := range run.ss.PlacedDevices {
			if at := run.firstAfter(ssLeaseAcquired, d, to, ""); !at.IsZero() {
				recovered[strconv.FormatInt(d, 10)] = at.Sub(to).Seconds()
			}
		}
		run.measures["outage_recovery_seconds"] = recovered
		fmt.Printf("    after the outage ended, the devices ran again after: %v (seconds)\n", recovered)
		run.reportPlacement("after")
	}
	run.checkHealthAll("after")
}

// judgeOutageStreams fails the case for an outage stream that stopped
// arriving during the outage. It judges the growth from the baseline, taken
// 15 seconds into the outage, to the outage's end: records sent before the
// outage began may still be on their way in its first seconds.
func (run *ssRun) judgeOutageStreams(base *outageBaseline, during map[string]int64, ended time.Time) {
	if base.counts == nil {
		// The outage ended before its baseline was due: there is no window to
		// judge. Validation refuses outage_streams with such a short outage.
		return
	}
	window := ended.Sub(base.at)
	growth := outageGrowth(base.counts, during)
	run.measures["outage_stream_growth"] = growth
	run.measures["outage_stream_window_seconds"] = window.Seconds()
	fmt.Printf("    records that arrived in the last %s of the outage: %v\n", window.Round(time.Second), growth)
	for _, name := range run.ss.OutageStreams {
		if growth[name] <= 0 {
			run.fail("stream %s stopped during the outage: no record arrived in its last %s", name, window.Round(time.Second))
		}
	}
}

// outageBaselineDelay is how far into an outage the driver counts the outage
// streams for the first time. Records sent before the outage can still be on
// their way in its first seconds, and a count taken earlier would let a
// stream that stopped when the outage began pass on them.
const outageBaselineDelay = 15 * time.Second

// outageBaseline is the count of each stream, taken once outageBaselineDelay
// into an outage.
type outageBaseline struct {
	began  time.Time
	at     time.Time
	counts map[string]int64
}

func newOutageBaseline(began time.Time) *outageBaseline { return &outageBaseline{began: began} }

// take counts the streams with count once the delay has passed, and only
// once.
func (b *outageBaseline) take(count func() map[string]int64) {
	if b.counts != nil || time.Since(b.began) < outageBaselineDelay {
		return
	}
	b.at = time.Now()
	b.counts = count()
}

// outageGrowth is how many records each stream gained between two counts.
func outageGrowth(before, after map[string]int64) map[string]int64 {
	growth := map[string]int64{}
	for name, n := range after {
		growth[name] = n - before[name]
	}
	return growth
}

// streamCounts returns how many records each stream's receiver holds.
func (run *ssRun) streamCounts() map[string]int64 {
	ports := run.orch.ReceiverMetricsPorts()
	out := map[string]int64{}
	for _, st := range run.ss.Streams {
		if m, err := run.r.queryReceiverMetrics(ports[st.Receiver], 5*time.Second); err == nil {
			out[st.StreamName()] = m.LinesReceived
		}
	}
	return out
}

// generatorRunning reports whether a generator's container still runs.
func (run *ssRun) generatorRunning(id string) bool {
	return containerRunning("bench-generator-" + id)
}

// daemonSkew is how far the clock of Docker's containers is ahead of the
// host's, read inside a running replica: every container shares the daemon's
// clock, which stamps the log lines. It is 0 when it cannot be measured.
func (run *ssRun) daemonSkew() time.Duration {
	for _, rep := range run.replicas {
		if !run.running[rep.N] {
			continue
		}
		before := time.Now()
		out, err := exec.Command("docker", "exec", rep.Container, "date", "-u", "+%s.%N").Output()
		after := time.Now()
		if err != nil {
			continue
		}
		secs, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		if err != nil {
			continue
		}
		daemon := time.Unix(0, int64(secs*1e9))
		return daemon.Sub(before.Add(after.Sub(before) / 2))
	}
	return 0
}

// scaleOutIn starts the standby replica, as a scale-out does, checks that it
// joins and that the store lists one more member, then stops it gracefully,
// as a scale-in does, and checks that the store lists one fewer.
func (run *ssRun) scaleOutIn() {
	n := run.ss.Replicas + 1
	rep := run.replica(n)
	fmt.Printf("  scaling out: starting %s…\n", rep.Service)
	if err := run.orch.RestartServices(rep.Service); err != nil {
		run.fail("starting %s: %v", rep.Service, err)
		return
	}
	run.running[n] = true
	since := containerStartedAt(rep.Container, time.Now())
	readyAt, ok := run.waitReady(n, since, earliest(time.Now().Add(run.replicaWarmup()), run.deadline))
	if !ok {
		run.fail("%s did not join after the scale-out: %s", rep.Service, run.readiness(n, since))
		return
	}
	run.measures["scale_out_join_seconds"] = readyAt.Sub(since).Seconds()
	fmt.Printf("    %s joined after %.1fs\n", rep.Service, readyAt.Sub(since).Seconds())
	if got := run.waitStoreMembers(run.ss.Replicas+1, 30*time.Second); got != run.ss.Replicas+1 {
		run.fail("after the scale-out the store lists %d live members, want %d", got, run.ss.Replicas+1)
	}
	run.checkSettled("scaled-out", earliest(time.Now().Add(30*time.Second), run.deadline))
	run.checkHealthAll("scaled-out")
	if sleepCtx(run.r.ctx, 20*time.Second) != nil {
		return
	}

	run.refreshAll()
	var owned []int64
	for _, d := range run.ss.PlacedDevices {
		if slices.Contains(run.owners(d, current()), n) {
			owned = append(owned, d)
		}
	}
	fmt.Printf("  scaling in: stopping %s (runs devices %v)…\n", rep.Service, owned)
	issued := time.Now()
	if err := exec.Command("docker", "stop", "-t", "30", rep.Container).Run(); err != nil {
		run.fail("docker stop %s: %v", rep.Service, err)
		return
	}
	took := time.Since(issued)
	endedAt := containerFinishedAt(rep.Container, time.Now())
	if err := run.book.refresh(rep.Container); err != nil {
		run.fail("reading %s's log after the scale-in: %v", rep.Service, err)
	}
	run.running[n] = false
	run.ended = append(run.ended, ssEvent{At: endedAt, Container: rep.Container, Kind: ssProcessEnd, Text: "harness: scale-in"})
	run.measures["scale_in_exit_seconds"] = took.Seconds()
	fmt.Printf("    %s exited %.1fs after the stop began\n", rep.Service, took.Seconds())
	if got := run.waitStoreMembers(run.ss.Replicas, 30*time.Second); got != run.ss.Replicas {
		run.fail("after the scale-in the store lists %d live members, want %d", got, run.ss.Replicas)
	}
	run.checkSettled("scaled-in", earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline))
	run.checkHealthAll("scaled-in")
}

// waitStoreMembers waits up to d for the store to list want live members, and
// returns how many it lists at the end.
func (run *ssRun) waitStoreMembers(want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	got := 0
	for {
		if err := run.book.refresh("bench-azurite"); err != nil {
			run.fail("reading Azurite's access log: %v", err)
			return got
		}
		got = len(liveStoreMembers(parseAzuriteCalls(run.book.all("bench-azurite")), run.ss.StoreContainerOrDefault(), 6*time.Second))
		if got == want || time.Now().After(deadline) || sleepCtx(run.r.ctx, 2*time.Second) != nil {
			return got
		}
	}
}

// liveStoreMembers returns the members whose record a replica wrote in the
// last window of the calls: each replica renews its own every 2 seconds.
func liveStoreMembers(calls []ssStoreCall, container string, window time.Duration) []string {
	if len(calls) == 0 {
		return nil
	}
	last := calls[len(calls)-1].At
	seen := map[string]bool{}
	var members []string
	for _, c := range calls {
		if c.Method != "PUT" || c.At.Before(last.Add(-window)) || !c.inContainer(container) {
			continue
		}
		path, err := url.PathUnescape(c.Path)
		if err != nil {
			continue
		}
		_, member, ok := strings.Cut(path, "shared/members/")
		if !ok {
			continue
		}
		member, _, _ = strings.Cut(member, "?")
		if member != "" && !seen[member] {
			seen[member] = true
			members = append(members, member)
		}
	}
	slices.Sort(members)
	return members
}

// coldRestart stops every replica at once, gracefully, and starts a new
// container for each, on an empty disk. What the replicas share must come
// back from the store: the devices resume from their checkpoints, and the
// stored placement is read again.
func (run *ssRun) coldRestart() {
	var running []ssReplica
	for _, rep := range run.replicas {
		if run.running[rep.N] {
			running = append(running, rep)
		}
	}
	run.refreshAll()
	run.measures["cold_restart_placement_before"] = run.measures["start_placement"]
	fmt.Printf("  stopping all %d replicas…\n", len(running))
	stopped := time.Now()
	errs := make(chan error, len(running))
	for _, rep := range running {
		go func() { errs <- exec.Command("docker", "stop", "-t", "30", rep.Container).Run() }()
	}
	for range running {
		if err := <-errs; err != nil {
			run.fail("stopping a replica: %v", err)
		}
	}
	for _, rep := range running {
		// The new containers take the old ones' logs with them, so this is
		// the last read of what the replicas logged before the restart.
		if err := run.book.refresh(rep.Container); err != nil {
			run.fail("reading %s's log before replacing it: %v", rep.Service, err)
		}
		run.running[rep.N] = false
		run.ended = append(run.ended, ssEvent{At: containerFinishedAt(rep.Container, time.Now()), Container: rep.Container, Kind: ssProcessEnd, Text: "harness: cold restart"})
	}
	fmt.Printf("    all stopped after %.1fs; starting new containers…\n", time.Since(stopped).Seconds())
	var services []string
	for _, rep := range running {
		services = append(services, rep.Service)
	}
	started := time.Now()
	if err := run.orch.RecreateServices(services...); err != nil {
		run.fail("starting new containers: %v", err)
		return
	}
	for _, rep := range running {
		run.running[rep.N] = true
	}
	ready := map[string]float64{}
	for _, rep := range running {
		since := containerStartedAt(rep.Container, started)
		if at, ok := run.waitReady(rep.N, since, earliest(time.Now().Add(run.replicaWarmup()), run.deadline)); ok {
			ready[rep.Service] = at.Sub(since).Seconds()
		} else {
			run.fail("%s was not ready after the cold restart: %s", rep.Service, run.readiness(rep.N, since))
		}
	}
	run.measures["cold_restart_ready_seconds"] = ready
	if !run.checkSettled("after", earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline)) {
		return
	}
	// The placement map the replicas left behind names members that are gone.
	// A leader that read it moves each device away from its old owner; one that
	// found no map places each device as new.
	run.refreshAll()
	moved, fresh := 0, 0
	since := started.Add(run.daemonSkew())
	for _, rep := range running {
		for _, e := range run.events(rep.Container) {
			if e.Kind != ssPlaced || e.At.Before(since) {
				continue
			}
			if strings.Contains(e.Text, "Reassigned device") {
				moved++
			} else {
				fresh++
			}
		}
	}
	run.measures["cold_restart_placements"] = map[string]int{"moved_from_the_stored_map": moved, "placed_as_new": fresh}
	fmt.Printf("    placements after the restart: %d moved off the stored map's old owners, %d placed as new\n", moved, fresh)
	run.checkHealthAll("after")
}

// measureIdle counts the store calls while no generator runs.
func (run *ssRun) measureIdle() {
	d := time.Duration(run.ss.IdleOrDefault()) * time.Second
	fmt.Printf("  counting the store calls while no data flows, for %s…\n", d)
	from := time.Now()
	if sleepCtx(run.r.ctx, d) != nil {
		return
	}
	if err := run.book.refresh("bench-azurite"); err != nil {
		run.fail("reading Azurite's access log: %v", err)
		return
	}
	skew := run.daemonSkew()
	rates := storeCallRates(parseAzuriteCalls(run.book.all("bench-azurite")), run.ss.StoreContainerOrDefault(), from.Add(skew), time.Now().Add(skew))
	run.measures["store_calls_idle"] = rates
	fmt.Printf("    idle: %.2f writes/s, %.2f reads and lists/s\n", rates.WritesPerSec, rates.ReadsPerSec)
	printCallKinds(rates)
}

// storeCalls counts the store calls under load, and judges them against the
// case's bounds.
func (run *ssRun) storeCalls() {
	d := time.Duration(run.ss.LoadOrDefault()) * time.Second
	fmt.Printf("  counting the store calls under load, for %s…\n", d)
	from := time.Now()
	if sleepCtx(run.r.ctx, d) != nil {
		return
	}
	if err := run.book.refresh("bench-azurite"); err != nil {
		run.fail("reading Azurite's access log: %v", err)
		return
	}
	skew := run.daemonSkew()
	to := time.Now()
	calls := parseAzuriteCalls(run.book.all("bench-azurite"))
	rates := storeCallRates(calls, run.ss.StoreContainerOrDefault(), from.Add(skew), to.Add(skew))
	run.measures["store_calls_load"] = rates
	fmt.Printf("    under load: %.2f writes/s, %.2f reads and lists/s (%d failed)\n", rates.WritesPerSec, rates.ReadsPerSec, rates.FailedRequests)
	printCallKinds(rates)
	if limit := run.ss.MaxStoreWritesPerSec; limit > 0 && rates.WritesPerSec > limit {
		run.fail("under load the replicas wrote to the store %.2f times a second, above max_store_writes_per_sec %.2f", rates.WritesPerSec, limit)
	}
	if limit := run.ss.MaxStoreReadsPerSec; limit > 0 && rates.ReadsPerSec > limit {
		run.fail("under load the replicas read or listed the store %.2f times a second, above max_store_reads_per_sec %.2f", rates.ReadsPerSec, limit)
	}
	run.checkStateWrites(calls, from.Add(skew), to.Add(skew))
}

// checkStateWrites reports when the replica that runs the shared bus wrote
// the state's segments in a window, and, when the case names the director's
// write interval, checks each gap between two writes against it. The window
// is one of steady load: no device moves in it, and a move is written at once
// whatever the interval.
func (run *ssRun) checkStateWrites(calls []ssStoreCall, from, to time.Time) {
	writer, ts := busiestWriter(segmentWrites(calls, run.ss.StoreContainerOrDefault(), from, to))
	gaps := writeGaps(ts)
	secs := make([]float64, len(gaps))
	for i, g := range gaps {
		secs[i] = g.Seconds()
	}
	run.measures["state_writes_load"] = map[string]any{"writer": writer, "writes": len(ts), "gaps_seconds": secs}
	fmt.Printf("    state writes under load: %d by the bus's writer, %d apart (seconds): %v\n", len(ts), len(gaps), roundSeconds(secs))
	n := run.ss.StateWriteIntervalSeconds
	if n <= 0 {
		return
	}
	want := time.Duration(n) * time.Second
	tol := run.ss.StateWriteToleranceOrDefault()
	if len(gaps) == 0 {
		run.fail("the bus's writer wrote %d state segments in %s of load; checking a %s write interval needs at least two", len(ts), to.Sub(from).Round(time.Second), want)
		return
	}
	for i, g := range gaps {
		if g < want-tol || g > want+tol {
			run.fail("state write %d came %s after the one before it; the write interval is %s, give or take %s", i+2, g.Round(100*time.Millisecond), want, tol)
		}
	}
}

// roundSeconds rounds each value to a tenth, for printing.
func roundSeconds(secs []float64) []float64 {
	out := make([]float64, len(secs))
	for i, s := range secs {
		out[i] = float64(int(s*10+0.5)) / 10
	}
	return out
}

// printCallKinds prints the write and read rates of each kind of store call.
func printCallKinds(rates ssCallRates) {
	kinds := make([]string, 0, len(rates.ByKind))
	for k := range rates.ByKind {
		kinds = append(kinds, k)
	}
	slices.Sort(kinds)
	for _, k := range kinds {
		c := rates.ByKind[k]
		fmt.Printf("      %-9s %.2f writes/s, %.2f reads and lists/s\n", k, c.WritesPerSec, c.ReadsPerSec)
	}
}

// scaleBacklog stops the receiver of the scale stream, so the replica its
// generator sends to keeps what it cannot deliver, and reads /scale on every
// replica. Each must report the replica set's backlog, not only its own.
func (run *ssRun) scaleBacklog() {
	var st *SharedStateStreamRef
	for i := range run.ss.Streams {
		if run.ss.Streams[i].StreamName() == run.ss.ScaleStream {
			st = &SharedStateStreamRef{Receiver: run.ss.Streams[i].Receiver, Generator: run.ss.Streams[i].Generator}
		}
	}
	if st == nil {
		run.fail("scale_backlog: no stream named %q", run.ss.ScaleStream)
		return
	}
	loaded := 0
	for _, g := range run.tc.AllGenerators() {
		if g.ID != st.Generator {
			continue
		}
		host, _, _ := strings.Cut(g.Target, ":")
		loaded, _ = strconv.Atoi(strings.TrimPrefix(host, "subject-"))
	}
	if loaded == 0 {
		run.fail("scale_backlog: generator %q does not send to a replica", st.Generator)
		return
	}
	receiver := "bench-receiver-" + st.Receiver
	pause := time.Duration(run.ss.OutageOrDefault()) * time.Second
	fmt.Printf("  pausing %s for %s, so %s keeps a backlog…\n", receiver, pause, run.replica(loaded).Service)
	if err := exec.Command("docker", "pause", receiver).Run(); err != nil {
		run.fail("docker pause %s: %v", receiver, err)
		return
	}
	defer func() {
		if err := exec.Command("docker", "unpause", receiver).Run(); err != nil {
			run.fail("docker unpause %s: %v", receiver, err)
		}
	}()
	if sleepCtx(run.r.ctx, pause) != nil {
		return
	}
	// Each replica publishes its sample every few seconds, and the replicas
	// read each other's at different moments, so one round of answers can
	// catch a replica a sample behind. The driver asks every replica every 5
	// seconds for up to 30, and keeps each replica's highest total and the
	// loaded replica's highest own backlog. A replica whose total never
	// reaches its share of that backlog reports only itself.
	last := map[string]ssScale{}
	highest := map[string]int64{}
	var loadedLocal int64
	met := func() bool {
		want := int64(float64(loadedLocal) * run.ss.ScaleMinShareOrDefault())
		if loadedLocal <= 0 || len(highest) == 0 {
			return false
		}
		for _, rep := range run.replicas {
			if run.running[rep.N] && highest[rep.Service] < want {
				return false
			}
		}
		return true
	}
	deadline := earliest(time.Now().Add(30*time.Second), run.deadline)
	for {
		for _, rep := range run.replicas {
			if !run.running[rep.N] {
				continue
			}
			s, err := run.scaleOf(rep.N)
			if err != nil {
				run.note("/scale of %s: %v", rep.Service, err)
				continue
			}
			last[rep.Service] = s
			highest[rep.Service] = max(highest[rep.Service], s.Backlog.Total)
			if rep.N == loaded {
				loadedLocal = max(loadedLocal, s.Backlog.Local)
			}
		}
		if met() || time.Now().After(deadline) || sleepCtx(run.r.ctx, 5*time.Second) != nil {
			break
		}
	}
	run.measures["scale"] = map[string]any{"last": last, "highest_total": highest, "loaded_local": loadedLocal}
	for name, s := range last {
		fmt.Printf("    /scale %s: backlog total %d (highest %d), local %d, unacked %d, members %d, coordination %q, pressure %d\n",
			name, s.Backlog.Total, highest[name], s.Backlog.Local, s.Backlog.Unacked, s.Members, s.Coordination, s.Pressure)
	}
	if loadedLocal <= 0 {
		run.fail("scale_backlog: %s reports no backlog of its own after its receiver was paused for %s", run.replica(loaded).Service, pause)
		return
	}
	want := int64(float64(loadedLocal) * run.ss.ScaleMinShareOrDefault())
	for _, rep := range run.replicas {
		if run.running[rep.N] && highest[rep.Service] < want {
			run.fail("/scale on %s never reported more than %d; the replica set held up to %d (%s's own)", rep.Service, highest[rep.Service], loadedLocal, run.replica(loaded).Service)
		}
	}
}

// SharedStateStreamRef names a stream's ends.
type SharedStateStreamRef struct{ Receiver, Generator string }

// ssScale is what the director's /scale endpoint answers.
type ssScale struct {
	Pressure int `json:"pressure"`
	Members  int `json:"members"`
	Backlog  struct {
		Total   int64 `json:"total"`
		Local   int64 `json:"local"`
		Unacked int64 `json:"unacked"`
		Max     int64 `json:"max"`
	} `json:"backlog"`
	Coordinated  bool   `json:"coordinated"`
	Coordination string `json:"coordination"`
}

// scaleOf asks replica n's /scale endpoint, from inside its container. The
// director serves it on its agent port: plain HTTP on 8080 when it has no TLS
// certificate, as in the bench.
func (run *ssRun) scaleOf(n int) (ssScale, error) {
	rep := run.replica(n)
	out, err := exec.Command("docker", "exec", rep.Container, "wget", "-qO-", "-T", "5", "http://127.0.0.1:8080/scale").CombinedOutput()
	run.health["scale-"+rep.Service] = string(out)
	if err != nil {
		return ssScale{}, fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	var s ssScale
	if err := json.Unmarshal(out, &s); err != nil {
		return ssScale{}, fmt.Errorf("unreadable answer: %s", strings.TrimSpace(string(out)))
	}
	return s, nil
}

// moveThenKill stops, gracefully, a replica that runs placed devices and does
// not lead, so the leader moves its devices at once. As soon as one of them
// has a new owner, it kills the replica that runs the shared bus with
// SIGKILL. A device's change of owner must reach the store when it happens,
// whatever the director's write interval: a new bus that read an older owner
// from the store would hand the device to a third replica while its new
// owner still runs it. The overlap check at the end of the case catches that.
func (run *ssRun) moveThenKill() {
	owner, ok := run.ownerNotLeading()
	if !ok {
		return
	}
	run.refreshAll()
	var moving []int64
	for _, d := range run.ss.PlacedDevices {
		if slices.Contains(run.owners(d, current()), owner) {
			moving = append(moving, d)
		}
	}
	orep := run.replica(owner)
	fmt.Printf("  stopping %s (runs devices %v), and killing the bus's replica once a device has moved…\n", orep.Service, moving)
	run.measures["victim"] = map[string]any{"replica": orep.Service, "devices": moving, "how": "move_then_kill"}

	skew := run.daemonSkew()
	issued := time.Now()
	stopped := make(chan error, 1)
	go func() { stopped <- exec.Command("docker", "stop", "-t", "30", orep.Container).Run() }()

	// Watch the other replicas for the first new owner of a moved device.
	since := issued.Add(skew)
	deadline := earliest(time.Now().Add(60*time.Second), run.deadline)
	var newOwner int
	var device int64
	var tookAt time.Time
	for newOwner == 0 && time.Now().Before(deadline) {
		for _, rep := range run.replicas {
			if rep.N == owner || !run.running[rep.N] {
				continue
			}
			_ = run.book.refresh(rep.Container)
			for _, e := range run.events(rep.Container) {
				if e.Kind == ssLeaseAcquired && e.At.After(since) && slices.Contains(moving, e.Device) {
					newOwner, device, tookAt = rep.N, e.Device, e.At
					break
				}
			}
			if newOwner != 0 {
				break
			}
		}
		if newOwner == 0 && sleepCtx(run.r.ctx, 100*time.Millisecond) != nil {
			break
		}
	}
	if newOwner == 0 {
		run.fail("no replica took over a device of %s within 60s of its stop", orep.Service)
		<-stopped
		return
	}

	// The replica that runs the bus is the one that leads now. When the new
	// owner leads, it is the one killed.
	leaders := run.leaders(current())
	if len(leaders) != 1 {
		run.fail("%d replicas lead (%s) when the device moved; the kill needs one", len(leaders), run.names(leaders))
		<-stopped
		return
	}
	target := run.replica(leaders[0])
	if err := exec.Command("docker", "kill", "-s", "KILL", target.Container).Run(); err != nil {
		run.fail("docker kill %s: %v (the kill did not happen)", target.Service, err)
		<-stopped
		return
	}
	killedAt := containerFinishedAt(target.Container, time.Now())
	lag := killedAt.Sub(tookAt)
	fmt.Printf("    %s took device %d; %s, which runs the bus, was killed %.2fs later\n", run.replica(newOwner).Service, device, target.Service, lag.Seconds())
	run.measures["move_then_kill"] = map[string]any{
		"stopped": orep.Service, "device": device, "new_owner": run.replica(newOwner).Service,
		"killed": target.Service, "new_owner_was_killed": newOwner == leaders[0], "kill_lag_seconds": lag.Seconds(),
	}
	if within := run.ss.MoveKillWithinOrDefault(); lag > within {
		run.fail("the kill came %.2fs after the device moved, later than move_kill_within_ms %d", lag.Seconds(), within.Milliseconds())
	}
	if err := run.book.refresh(target.Container); err != nil {
		run.fail("reading %s's log after the kill: %v", target.Service, err)
	}
	run.running[target.N] = false
	run.ended = append(run.ended, ssEvent{At: killedAt, Container: target.Container, Kind: ssProcessEnd, Text: "harness: docker kill"})

	if err := <-stopped; err != nil {
		run.fail("docker stop %s: %v", orep.Service, err)
	}
	if err := run.book.refresh(orep.Container); err != nil {
		run.fail("reading %s's log after its stop: %v", orep.Service, err)
	}
	run.running[owner] = false
	run.ended = append(run.ended, ssEvent{At: containerFinishedAt(orep.Container, time.Now()), Container: orep.Container, Kind: ssProcessEnd, Text: "harness: docker stop"})

	// The replica set must settle on what is left before the platform brings
	// the two back.
	if !run.checkSettled("after the kill", earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline)) {
		return
	}
	run.replaceAll([]int{owner, target.N})
	run.checkSettled("after", earliest(time.Now().Add(60*time.Second), run.deadline))
	run.checkHealthAll("after")
}

// replaceAll replaces lost replicas with new containers on empty disks, as the
// platform does, and waits for each to rejoin.
func (run *ssRun) replaceAll(ns []int) {
	var services []string
	for _, n := range ns {
		services = append(services, run.replica(n).Service)
	}
	fmt.Printf("  replacing %s with new containers…\n", strings.Join(services, ", "))
	started := time.Now()
	if err := run.orch.RecreateServices(services...); err != nil {
		run.fail("replacing %s: %v", strings.Join(services, ", "), err)
		return
	}
	for _, n := range ns {
		run.running[n] = true
	}
	for _, n := range ns {
		rep := run.replica(n)
		since := containerStartedAt(rep.Container, started)
		if at, ok := run.waitReady(n, since, earliest(time.Now().Add(run.replicaWarmup()), run.deadline)); ok {
			fmt.Printf("    %s rejoined after %.1fs\n", rep.Service, at.Sub(since).Seconds())
		} else {
			run.fail("%s did not rejoin after it was replaced: %s", rep.Service, run.readiness(n, since))
		}
	}
}

// rollingRestart stops each replica in turn, gracefully, and replaces it with
// a new container on an empty disk, as a new revision of a container app does:
// the followers first, the leader last. Before it stops the next replica, the
// new one must have rejoined and the replica set must have settled.
func (run *ssRun) rollingRestart() {
	run.refreshAll()
	leaders := run.leaders(current())
	var order []int
	for _, rep := range run.replicas {
		if run.running[rep.N] && !slices.Contains(leaders, rep.N) {
			order = append(order, rep.N)
		}
	}
	order = append(order, leaders...)
	exits := map[string]float64{}
	run.measures["rolling_restart_exit_seconds"] = exits
	for _, n := range order {
		rep := run.replica(n)
		fmt.Printf("  restarting %s…\n", rep.Service)
		issued := time.Now()
		if err := exec.Command("docker", "stop", "-t", "30", rep.Container).Run(); err != nil {
			run.fail("docker stop %s: %v", rep.Service, err)
			return
		}
		exits[rep.Service] = time.Since(issued).Seconds()
		fmt.Printf("    %s exited %.1fs after the stop began\n", rep.Service, exits[rep.Service])
		endedAt := containerFinishedAt(rep.Container, time.Now())
		// The new container takes the old one's log with it.
		if err := run.book.refresh(rep.Container); err != nil {
			run.fail("reading %s's log before replacing it: %v", rep.Service, err)
		}
		run.running[n] = false
		run.ended = append(run.ended, ssEvent{At: endedAt, Container: rep.Container, Kind: ssProcessEnd, Text: "harness: rolling restart"})
		run.replaceAll([]int{n})
		if !run.checkSettled("after restarting "+rep.Service, earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline)) {
			return
		}
	}
	run.checkHealthAll("after")
}

// checkLogOnce fails the case for a replica that did not log a line of
// expect_log_once exactly once for every time its director started.
func (run *ssRun) checkLogOnce() {
	for _, want := range run.ss.ExpectLogOnce {
		for _, rep := range run.replicas {
			lines := run.book.all(rep.Container)
			if len(lines) == 0 {
				continue // a standby that never ran
			}
			starts, seen := countStartsAndLines(lines, want)
			if seen != starts {
				run.fail("%s logged %q %d times over %d starts of its director; want it once a start", rep.Service, want, seen, starts)
			}
		}
	}
}

// countStartsAndLines counts the starts of a director in a replica's log, and
// the lines that contain want.
func countStartsAndLines(lines []ssLine, want string) (starts, seen int) {
	for _, l := range lines {
		if strings.Contains(l.Text, "vmetric-director started") {
			starts++
		}
		if strings.Contains(l.Text, want) {
			seen++
		}
	}
	return starts, seen
}
