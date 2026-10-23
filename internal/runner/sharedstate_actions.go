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
	before := run.streamCounts()
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
	for time.Now().Before(end) {
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
	run.judgeOutageStreams(before, during, resumed.Sub(paused))

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
	before := run.streamCounts()
	cut := time.Now()
	if err := exec.Command("docker", "network", "disconnect", network, rep.Container).Run(); err != nil {
		run.fail("docker network disconnect %s: %v (the outage did not happen)", rep.Service, err)
		return
	}
	run.measures["outage_from"] = cut.UTC()
	statuses := map[string]int{}
	end := earliest(cut.Add(outage), run.deadline)
	for time.Now().Before(end) {
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
	run.judgeOutageStreams(before, during, back.Sub(cut))

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
// arriving while the store was away. The first 15 seconds of the outage are
// left out: records sent before it began may still be on their way.
func (run *ssRun) judgeOutageStreams(before, during map[string]int64, outage time.Duration) {
	growth := map[string]int64{}
	for name, n := range during {
		growth[name] = n - before[name]
	}
	run.measures["outage_stream_growth"] = growth
	fmt.Printf("    records that arrived during the outage: %v\n", growth)
	if outage < 20*time.Second {
		return
	}
	for _, name := range run.ss.OutageStreams {
		if growth[name] <= 0 {
			run.fail("stream %s stopped while the store was away: no record arrived in %s", name, outage.Round(time.Second))
		}
	}
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
	run.book.refresh(rep.Container)
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
		run.book.refresh("bench-azurite")
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
		run.book.refresh(rep.Container)
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
	run.book.refresh("bench-azurite")
	skew := run.daemonSkew()
	rates := storeCallRates(parseAzuriteCalls(run.book.all("bench-azurite")), run.ss.StoreContainerOrDefault(), from.Add(skew), time.Now().Add(skew))
	run.measures["store_calls_idle"] = rates
	fmt.Printf("    idle: %.2f writes/s, %.2f reads and lists/s\n", rates.WritesPerSec, rates.ReadsPerSec)
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
	run.book.refresh("bench-azurite")
	skew := run.daemonSkew()
	rates := storeCallRates(parseAzuriteCalls(run.book.all("bench-azurite")), run.ss.StoreContainerOrDefault(), from.Add(skew), time.Now().Add(skew))
	run.measures["store_calls_load"] = rates
	fmt.Printf("    under load: %.2f writes/s, %.2f reads and lists/s (%d failed)\n", rates.WritesPerSec, rates.ReadsPerSec, rates.FailedRequests)
	if limit := run.ss.MaxStoreWritesPerSec; limit > 0 && rates.WritesPerSec > limit {
		run.fail("under load the replicas wrote to the store %.2f times a second, above max_store_writes_per_sec %.2f", rates.WritesPerSec, limit)
	}
	if limit := run.ss.MaxStoreReadsPerSec; limit > 0 && rates.ReadsPerSec > limit {
		run.fail("under load the replicas read or listed the store %.2f times a second, above max_store_reads_per_sec %.2f", rates.ReadsPerSec, limit)
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
	samples := map[string]ssScale{}
	var loadedLocal int64
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
			samples[rep.Service] = s
			if rep.N == loaded {
				loadedLocal = s.Backlog.Local
			}
		}
		if loadedLocal > 0 || time.Now().After(deadline) || sleepCtx(run.r.ctx, 5*time.Second) != nil {
			break
		}
	}
	run.measures["scale"] = samples
	for name, s := range samples {
		fmt.Printf("    /scale %s: backlog total %d, local %d, members %d, coordination %q, pressure %d\n", name, s.Backlog.Total, s.Backlog.Local, s.Members, s.Coordination, s.Pressure)
	}
	if loadedLocal <= 0 {
		run.fail("scale_backlog: %s reports no backlog of its own after its receiver was paused for %s", run.replica(loaded).Service, pause)
		return
	}
	want := int64(float64(loadedLocal) * run.ss.ScaleMinShareOrDefault())
	for name, s := range samples {
		if s.Backlog.Total < want {
			run.fail("/scale on %s reports a backlog of %d; the replica set holds at least %d (%s's own)", name, s.Backlog.Total, loadedLocal, run.replica(loaded).Service)
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
		Total int64 `json:"total"`
		Local int64 `json:"local"`
		Max   int64 `json:"max"`
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
