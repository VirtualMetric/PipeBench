package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"github.com/VirtualMetric/PipeBench/internal/orchestrator"
)

// The actions of a shared-state case that disturb the store, the network or
// the size of the replica set, rather than one replica.

// pauseStore takes the store away for the outage, so no replica can reach it,
// then gives it back: it pauses Azurite, MinIO or Redis, or makes the share
// refuse every access. It measures when the leader stops leading and
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
	if run.ss.StoreOrDefault() == config.SharedStateStoreShare {
		what := fmt.Sprintf("The share refuses access for %d s (chmod 000). This fails fast, unlike a network share that stops answering.", run.ss.OutageOrDefault())
		run.measures["outage_kind"] = what
		fmt.Printf("  %s\n", what)
	} else {
		fmt.Printf("  pausing %s for %s…\n", run.ss.StoreName(), outage)
	}
	if err := run.takeStore(true); err != nil {
		run.fail("%v (the outage did not happen)", err)
		return
	}
	if run.ss.StoreOrDefault() == config.SharedStateStoreShare {
		if err := run.confirmShareRefused(); err != nil {
			run.fail("%v", err)
			if back := run.takeStore(false); back != nil {
				run.fail("%v", back)
			}
			return
		}
	}
	paused := time.Now()
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
	if err := run.takeStore(false); err != nil {
		run.fail("%v", err)
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

// takeStore takes the store away (away) or gives it back.
func (run *ssRun) takeStore(away bool) error {
	via := ""
	if run.ss.StoreOrDefault() == config.SharedStateStoreShare {
		via = run.replicas[0].Container
	}
	args := storeOutageArgs(run.ss, away, via, run.subject.ImageRef())
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("docker %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// confirmShareRefused checks, inside every running replica and as the
// replica's own user, that the share now refuses access. chmod 000 does not
// stop a process that runs as root, so an outage the replicas could ignore
// fails the run, instead of passing as one they survived.
func (run *ssRun) confirmShareRefused() error {
	checked := 0
	for _, rep := range run.replicas {
		if !run.running[rep.N] {
			continue
		}
		checked++
		opens, err := shareOpens(rep.Container, run.ss.SharePath)
		if err != nil {
			return fmt.Errorf("checking that the share refuses %s: %v", rep.Service, err)
		}
		if opens {
			return fmt.Errorf("the share at %s still opens for %s's user after chmod 000, so the outage did not happen: a process that runs as root ignores the mode", run.ss.SharePath, rep.Service)
		}
	}
	if checked == 0 {
		return fmt.Errorf("no replica runs, so nothing confirms that the share at %s refuses access", run.ss.SharePath)
	}
	return nil
}

// shareOpens reports whether a container's own user can open the directory:
// docker exec runs as the user the container runs as. Tests replace it.
var shareOpens = func(container, dir string) (bool, error) {
	out, err := exec.Command("docker", "exec", container, "sh", "-c", `cd "$1"`, "sh", dir).CombinedOutput()
	if err == nil {
		return true, nil
	}
	if strings.Contains(string(out), "ermission denied") {
		return false, nil
	}
	return false, fmt.Errorf("docker exec %s: %v: %s", container, err, strings.TrimSpace(string(out)))
}

// storeOutageArgs returns the docker arguments that take the store away, or
// give it back. A paused container stops answering. A share that is chmod 000
// refuses every access at once: a short container of the subject's image,
// with the volumes of replica via (running, paused or stopped), changes the
// mode as root, so the replicas' own user can no longer reach the share.
func storeOutageArgs(ss *config.SharedStateConfig, away bool, via, image string) []string {
	if ss.StoreOrDefault() == config.SharedStateStoreShare {
		mode := "0777"
		if away {
			mode = "000"
		}
		return []string{"run", "--rm", "--volumes-from", via, "--user", "0:0", "--network", "none",
			"--cpus", "0.5", "--memory", "64m", "--entrypoint", "chmod", image, mode, ss.SharePath}
	}
	if away {
		return []string{"pause", "bench-" + ss.StoreService()}
	}
	return []string{"unpause", "bench-" + ss.StoreService()}
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
	takeovers, _ := run.measures["outage_takeover_seconds"].(map[string]float64)
	for _, p := range isolateTakeoverProblems(owned, takeovers, back.Sub(cut), run.ss.MaxFailoverSeconds) {
		run.fail("%s", p)
	}
}

// isolateTakeoverProblems judges how soon another replica ran each device the
// cut-off replica ran, counted from the cut, against max_failover_seconds. A
// device that no other replica ran during the cut was too slow when the cut
// lasted longer than the limit. With a shorter cut it is not judged: it may
// have run again on the replica that came back.
func isolateTakeoverProblems(owned []int64, takeovers map[string]float64, cut time.Duration, limit float64) []string {
	if limit <= 0 {
		return nil
	}
	var problems []string
	for _, d := range owned {
		secs, ok := takeovers[strconv.FormatInt(d, 10)]
		switch {
		case ok && secs > limit:
			problems = append(problems, fmt.Sprintf("device %d of the cut-off replica ran on another replica %.1fs after the cut, above max_failover_seconds %.0f", d, secs, limit))
		case !ok && cut.Seconds() > limit:
			problems = append(problems, fmt.Sprintf("device %d of the cut-off replica did not run on another replica within max_failover_seconds %.0f of the cut, which lasted %.0fs", d, limit, cut.Seconds()))
		}
	}
	return problems
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
		if why := outageShortfall(name, growth[name], window, run.streamPace(name)); why != "" {
			run.fail("%s", why)
		}
	}
}

// outageShortfall says why an outage stream's arrivals in the window fall
// short, or "". A record or two would only show that something arrived, so
// the stream must keep at least half the pace its generator sends at.
func outageShortfall(name string, growth int64, window time.Duration, perSec float64) string {
	if growth <= 0 {
		return fmt.Sprintf("stream %s stopped during the outage: no record arrived in its last %s", name, window.Round(time.Second))
	}
	paced := perSec * window.Seconds()
	if perSec > 0 && float64(growth) < paced/2 {
		return fmt.Sprintf("stream %s slowed during the outage: in its last %s, %s arrived of the %.0f its generator sends in that time; at least half must arrive", name, window.Round(time.Second), countOf(growth, "record"), paced)
	}
	return ""
}

// countOf says n of a thing: "1 record", "2 records".
func countOf(n int64, thing string) string {
	if n == 1 {
		return "1 " + thing
	}
	return strconv.FormatInt(n, 10) + " " + thing + "s"
}

// streamPace is how many records a second the generator of the named stream
// sends: its rate on each of its connections, counted as the compose file
// counts them, so a generator that leaves connections unset gets the host's
// default. It is 0 for a stream whose generator has no rate limit.
func (run *ssRun) streamPace(name string) float64 {
	for _, st := range run.ss.Streams {
		if st.StreamName() != name {
			continue
		}
		for _, g := range run.tc.AllGenerators() {
			if g.ID == st.Generator {
				return float64(g.Rate) * float64(orchestrator.ResolveGeneratorConnections(g.Connections))
			}
		}
	}
	return 0
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
	run.markStarted(n)
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
	run.endReplica(n, containerFinishedAt(rep.Container, time.Now()), "scale-in", true)
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
		got = len(liveStoreMembers(parseAzuriteCalls(run.book.all("bench-azurite")), run.ss.StoreContainerOrDefault()))
		if got == want || time.Now().After(deadline) || sleepCtx(run.r.ctx, 2*time.Second) != nil {
			return got
		}
	}
}

// ssMemberRecordInterval is how often a replica writes its member record, used
// until the store's log shows a gap that tells. A newer director writes a
// replica's record when it starts, again as soon as it knows its route
// address, and then every 30 seconds: the replicas check on each other over
// their own connections, not through the store. An older director renews the
// record every 2 seconds.
const ssMemberRecordInterval = 30 * time.Second

// ssStoreSilence is how long the state container can go without a call that
// the store answered before the bench takes it for a store outage. Every
// replica reads its lease every 2 seconds, so a store that is up answers a
// call at least that often.
const ssStoreSilence = 10 * time.Second

// ssSilence is a stretch of the store's log in which the store answered no
// call to the state container.
type ssSilence struct{ from, to time.Time }

// ssMemberRecord is what the store's log shows of one member record: when a
// replica wrote it, and when it was last deleted.
type ssMemberRecord struct {
	writes  []time.Time
	deleted time.Time
}

// liveStoreMembers returns the members whose record is live at the last call
// in the log. A record is live when a replica wrote it within
// memberRecordWindow and nobody has deleted it since. A replica deletes its
// own record when it stops cleanly, so a scale-in shows at once, whatever the
// interval.
func liveStoreMembers(calls []ssStoreCall, container string) []string {
	if len(calls) == 0 {
		return nil
	}
	last := calls[len(calls)-1].At
	records := memberRecords(calls, container)
	window := memberRecordWindow(records, storeSilences(calls, container))
	var members []string
	for member, r := range records {
		if len(r.writes) == 0 {
			continue
		}
		written := r.writes[len(r.writes)-1]
		if r.deleted.After(written) || last.Sub(written) > window {
			continue
		}
		members = append(members, member)
	}
	slices.Sort(members)
	return members
}

// memberRecords collects the writes and deletes of each member record in the
// container. A write or a delete that the store refused does not count, but
// deleting a record that is already gone does.
func memberRecords(calls []ssStoreCall, container string) map[string]*ssMemberRecord {
	records := map[string]*ssMemberRecord{}
	for _, c := range calls {
		if (c.Method != "PUT" && c.Method != "DELETE") || !c.inContainer(container) {
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
		if member == "" {
			continue
		}
		r := records[member]
		if r == nil {
			r = &ssMemberRecord{}
			records[member] = r
		}
		switch {
		case c.Method == "PUT" && c.Status/100 == 2:
			r.writes = append(r.writes, c.At)
		case c.Method == "DELETE" && (c.Status/100 == 2 || c.Status == 404):
			r.deleted = c.At
		}
	}
	return records
}

// memberRecordWindow is how long a member record stays live after a replica
// last wrote it: one and a half times the interval the replicas write their
// records at, and at least 4 seconds more than one interval. The interval is
// the median time between two writes of the same record, so the window
// follows the director under test. A director that renews its record every 2
// seconds gets 6 seconds, as before. A director that writes it every
// 30 seconds gets about 45 seconds. That is close to the 45 seconds such a
// director itself trusts a record for: the record interval plus 15 seconds.
//
// Two kinds of gap do not count. The first gap of each record does not: a
// newer director writes its record again soon after it starts, once it knows
// its route address, and a gap of a second would make the window a few
// seconds long. A gap in which the store was silent does not count either:
// the replicas could not write their records then, so such a gap says
// nothing about how often they write them. Counted, the gaps of a long store
// outage would make the window minutes long, and a replica killed during the
// outage would still count as live after it.
func memberRecordWindow(records map[string]*ssMemberRecord, silences []ssSilence) time.Duration {
	var gaps []time.Duration
	for _, r := range records {
		for i := 2; i < len(r.writes); i++ {
			from, to := r.writes[i-1], r.writes[i]
			if silentBetween(silences, from, to) {
				continue
			}
			gaps = append(gaps, to.Sub(from))
		}
	}
	interval := ssMemberRecordInterval
	if len(gaps) > 0 {
		slices.Sort(gaps)
		interval = gaps[len(gaps)/2]
	}
	return max(interval*3/2, interval+4*time.Second)
}

// storeSilences returns the stretches in which the store answered no call to
// the container for longer than ssStoreSilence: it was paused, unreachable or
// failing. A call that the store answered with a status below 500 counts, so a
// refused write or a missing record shows that the store was up.
func storeSilences(calls []ssStoreCall, container string) []ssSilence {
	var silences []ssSilence
	var answered time.Time
	for _, c := range calls {
		if c.Status >= 500 || !c.inContainer(container) {
			continue
		}
		if !answered.IsZero() && c.At.Sub(answered) > ssStoreSilence {
			silences = append(silences, ssSilence{from: answered, to: c.At})
		}
		answered = c.At
	}
	return silences
}

// silentBetween reports whether the store was silent at some point between
// from and to.
func silentBetween(silences []ssSilence, from, to time.Time) bool {
	for _, s := range silences {
		if s.from.Before(to) && s.to.After(from) {
			return true
		}
	}
	return false
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
		run.endReplica(rep.N, containerFinishedAt(rep.Container, time.Now()), "cold restart", true)
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
		run.markStarted(rep.N)
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
	// The window must be steady. A replica that stopped leading, a device
	// that moved, a bus that started again or a store that stood still all
	// make the replicas write at once, whatever the interval, so the window
	// would say nothing about the interval.
	run.refreshAll()
	var events []ssEvent
	for _, rep := range run.replicas {
		events = append(events, run.events(rep.Container)...)
	}
	disturbed := windowDisturbances(events, calls, run.ss.StoreContainerOrDefault(), from.Add(skew), to.Add(skew))
	run.measures["store_calls_load_disturbances"] = disturbed
	for _, d := range disturbed {
		run.fail("the load window was not steady, so its store calls are no evidence: %s", d)
	}
	run.checkStateWrites(calls, from.Add(skew), to.Add(skew), len(disturbed) == 0)
}

// ssStoreStall is how long Azurite may serve no request at all before the
// driver calls it a stall. Every replica reads the lease every two seconds,
// and the replica that holds it renews it as often, so a store that is quiet
// this long stood still, or the machine did. The member records do not keep
// the store busy: a newer director writes a replica's record only every 30
// seconds.
const ssStoreStall = 5 * time.Second

// windowDisturbances lists what happened in [from, to) that a steady window
// does not hold: a replica that started or stopped leading, a device that
// started, stopped or was placed, a bus or process that started or stopped,
// the state bus writing as more than one writer, and Azurite serving no
// request for ssStoreStall or longer.
func windowDisturbances(events []ssEvent, calls []ssStoreCall, container string, from, to time.Time) []string {
	var out []string
	sorted := slices.Clone(events)
	sortSSEvents(sorted)
	for _, e := range sorted {
		if e.At.Before(from) || !e.At.Before(to) {
			continue
		}
		if what := disturbanceOf(e); what != "" {
			out = append(out, fmt.Sprintf("%s %s at %s", e.Container, what, e.At.UTC().Format("15:04:05.000")))
		}
	}
	writers := slices.Sorted(maps.Keys(segmentWrites(calls, container, from, to)))
	if len(writers) > 1 {
		out = append(out, fmt.Sprintf("the state bus wrote its segments as %d writers (%s): it moved or started again", len(writers), strings.Join(writers, ", ")))
	}
	last := from
	for _, c := range calls {
		if c.At.Before(from) || !c.At.Before(to) {
			continue
		}
		if gap := c.At.Sub(last); gap >= ssStoreStall {
			out = append(out, fmt.Sprintf("Azurite served no request for %.1fs from %s", gap.Seconds(), last.UTC().Format("15:04:05.000")))
		}
		last = c.At
	}
	if gap := to.Sub(last); gap >= ssStoreStall {
		out = append(out, fmt.Sprintf("Azurite served no request for %.1fs from %s", gap.Seconds(), last.UTC().Format("15:04:05.000")))
	}
	return out
}

// disturbanceOf says what an event changed, or "" for an event a steady
// replica set also logs.
func disturbanceOf(e ssEvent) string {
	switch e.Kind {
	case ssProcessStart:
		return "started its director"
	case ssProcessEnd:
		return "ended its process"
	case ssBusStarted:
		return "started its state bus"
	case ssBusStopped:
		return "stopped its state bus"
	case ssFenced:
		return "could no longer write its bus's streams"
	case ssLeaderUp:
		return "became the leader"
	case ssLeaderDown:
		return "stopped leading"
	case ssLeaseAcquired:
		return fmt.Sprintf("started device %d", e.Device)
	case ssLeaseEnded:
		return fmt.Sprintf("stopped device %d", e.Device)
	case ssPlaced:
		return fmt.Sprintf("placed device %d on %s", e.Device, e.Member)
	}
	return ""
}

// checkStateWrites reports when the replica that runs the shared bus wrote
// the state's segments in a window, and when the snapshot rounds came. When
// judge is set and the case names the director's write interval, it checks
// each gap between two writes against it. The window must be one of steady
// load (windowDisturbances): a move is written at once whatever the interval.
func (run *ssRun) checkStateWrites(calls []ssStoreCall, from, to time.Time, judge bool) {
	container := run.ss.StoreContainerOrDefault()
	writer, ts := busiestWriter(segmentWrites(calls, container, from, to))
	gaps := writeGaps(ts)
	secs := make([]float64, len(gaps))
	for i, g := range gaps {
		secs[i] = g.Seconds()
	}
	// A stream's snapshot writes what is waiting first, whatever the interval:
	// such a segment write comes within a second of the snapshot's own writes.
	snaps := snapshotWrites(calls, container, from, to)
	forced := withSnapshot(ts, snaps)
	nForced := 0
	for _, f := range forced {
		if f {
			nForced++
		}
	}
	snapRounds := writeRounds(snaps, time.Second)
	roundGaps := writeGaps(snapRounds)
	roundSecs := make([]float64, len(roundGaps))
	for i, g := range roundGaps {
		roundSecs[i] = g.Seconds()
	}
	writeTimes := make([]string, len(ts))
	for i, t := range ts {
		writeTimes[i] = t.UTC().Format("15:04:05.000")
	}
	roundTimes := make([]string, len(snapRounds))
	for i, t := range snapRounds {
		roundTimes[i] = t.UTC().Format("15:04:05.000")
	}
	run.measures["state_writes_load"] = map[string]any{
		"writer": writer, "writes": len(ts), "write_times": writeTimes, "gaps_seconds": secs, "with_snapshot": nForced,
		"snapshot_rounds": len(snapRounds), "snapshot_round_times": roundTimes, "snapshot_round_gaps_seconds": roundSecs,
	}
	fmt.Printf("    state writes under load: %d by the bus's writer (%d with a snapshot), at %v, %d apart (seconds): %v\n", len(ts), nForced, writeTimes, len(gaps), roundSeconds(secs))
	fmt.Printf("    snapshot rounds under load: %d, at %v, apart (seconds): %v\n", len(snapRounds), roundTimes, roundSeconds(roundSecs))
	n := run.ss.StateWriteIntervalSeconds
	if n <= 0 {
		return
	}
	if !judge {
		fmt.Printf("    the write interval is not judged: the window was not steady\n")
		return
	}
	want := time.Duration(n) * time.Second
	tol := run.ss.StateWriteToleranceOrDefault()
	problems, judged := judgeWriteGaps(ts, want, tol)
	for _, p := range problems {
		run.fail("%s", p)
	}
	if judged == 0 {
		run.fail("no state write in %s of load followed the %s write interval: the bus's writer wrote %d segments, %d of them with a snapshot, and the snapshot rounds came every %v seconds",
			to.Sub(from).Round(time.Second), want, len(ts), nForced, roundSeconds(roundSecs))
	}
}

// judgeWriteGaps judges the gap before each round of state writes, and
// returns what is wrong and how many gaps it judged. A round is the writes
// that came within a second of each other, or within a tenth of the interval
// when that is shorter: a snapshot round writes a segment for each stream it
// takes a snapshot of, a few milliseconds apart. A write that comes with a
// snapshot counts as an interval write. A bus takes its snapshots with its
// interval writes, so a snapshot that comes between two of them adds a store
// call of its own, and its gap is judged like any other.
func judgeWriteGaps(ts []time.Time, want, tol time.Duration) ([]string, int) {
	rounds := writeRounds(ts, min(time.Second, want/10))
	var problems []string
	for i := 1; i < len(rounds); i++ {
		if g := rounds[i].Sub(rounds[i-1]); g < want-tol || g > want+tol {
			problems = append(problems, fmt.Sprintf("state write round %d came %s after the one before it; the write interval is %s, give or take %s", i+1, g.Round(100*time.Millisecond), want, tol))
		}
	}
	return problems, max(len(rounds)-1, 0)
}

// snapshotWrites returns when the store accepted a stream's snapshot or its
// manifest in [from, to): the writes a snapshot makes.
func snapshotWrites(calls []ssStoreCall, container string, from, to time.Time) []time.Time {
	var out []time.Time
	for _, c := range calls {
		if c.At.Before(from) || !c.At.Before(to) || !c.isWrite() || c.Status >= 300 || !c.inContainer(container) {
			continue
		}
		if k := c.kind(); k == "snapshots" || k == "streams" {
			out = append(out, c.At)
		}
	}
	return out
}

// withSnapshot reports, for each write, whether a snapshot write came within a
// second of it.
func withSnapshot(writes, snaps []time.Time) []bool {
	out := make([]bool, len(writes))
	for i, w := range writes {
		for _, s := range snaps {
			if d := s.Sub(w); d > -time.Second && d < time.Second {
				out[i] = true
				break
			}
		}
	}
	return out
}

// writeRounds returns the first time of each round of writes: writes less
// than within apart belong to one round.
func writeRounds(ts []time.Time, within time.Duration) []time.Time {
	var rounds []time.Time
	var last time.Time
	for _, t := range ts {
		if len(rounds) == 0 || t.Sub(last) >= within {
			rounds = append(rounds, t)
		}
		last = t
	}
	return rounds
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
	run.endReplica(target.N, killedAt, "docker kill", false)

	if err := <-stopped; err != nil {
		run.fail("docker stop %s: %v", orep.Service, err)
	}
	run.endReplica(owner, containerFinishedAt(orep.Container, time.Now()), "docker stop", true)

	// The replica set must settle on what is left before the platform brings
	// the two back.
	if !run.checkSettled("after the kill", earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline)) {
		return
	}
	if !run.replaceAll([]int{owner, target.N}) {
		return
	}
	run.checkSettled("after", earliest(time.Now().Add(60*time.Second), run.deadline))
	run.checkHealthAll("after")
}

// replaceAll replaces lost replicas with new containers on empty disks, as the
// platform does, and waits for each to rejoin. It reports whether every one
// of them rejoined.
func (run *ssRun) replaceAll(ns []int) bool {
	var services []string
	for _, n := range ns {
		services = append(services, run.replica(n).Service)
	}
	fmt.Printf("  replacing %s with new containers…\n", strings.Join(services, ", "))
	started := time.Now()
	if err := run.orch.RecreateServices(services...); err != nil {
		run.fail("replacing %s: %v", strings.Join(services, ", "), err)
		return false
	}
	for _, n := range ns {
		run.markStarted(n)
	}
	rejoined := true
	for _, n := range ns {
		rep := run.replica(n)
		since := containerStartedAt(rep.Container, started)
		if at, ok := run.waitReady(n, since, earliest(time.Now().Add(run.replicaWarmup()), run.deadline)); ok {
			fmt.Printf("    %s rejoined after %.1fs\n", rep.Service, at.Sub(since).Seconds())
		} else {
			run.fail("%s did not rejoin after it was replaced: %s", rep.Service, run.readiness(n, since))
			rejoined = false
		}
	}
	return rejoined
}

// rollingRestart stops each replica in turn, gracefully, and replaces it with
// a new container on an empty disk, as a new revision of a container app does:
// the followers first, the leader last. Before it stops the next replica, the
// new one must have rejoined and the replica set must have settled. When one
// has not, the restart ends there: stopping another replica of a set that is
// already short would test something else.
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
	if !inTurn(order, func(n int) bool { return run.restartOne(n, exits) }) {
		return
	}
	run.checkHealthAll("after")
}

// inTurn calls step for each replica in order, and stops at the first step
// that fails. It reports whether every step succeeded.
func inTurn(order []int, step func(n int) bool) bool {
	for _, n := range order {
		if !step(n) {
			return false
		}
	}
	return true
}

// restartOne stops replica n gracefully, replaces it with a new container,
// and waits for the replica set to settle. It reports whether all of that
// happened.
func (run *ssRun) restartOne(n int, exits map[string]float64) bool {
	rep := run.replica(n)
	fmt.Printf("  restarting %s…\n", rep.Service)
	issued := time.Now()
	if err := exec.Command("docker", "stop", "-t", "30", rep.Container).Run(); err != nil {
		run.fail("docker stop %s: %v", rep.Service, err)
		return false
	}
	exits[rep.Service] = time.Since(issued).Seconds()
	fmt.Printf("    %s exited %.1fs after the stop began\n", rep.Service, exits[rep.Service])
	// The new container takes the old one's log with it.
	if !run.endReplica(n, containerFinishedAt(rep.Container, time.Now()), "rolling restart", true) {
		return false
	}
	if !run.replaceAll([]int{n}) {
		return false
	}
	return run.checkSettled("after restarting "+rep.Service, earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline))
}

// checkLogOnce fails the case for a replica that did not log a line of
// expect_log_once exactly once in each run of its director.
func (run *ssRun) checkLogOnce() {
	for _, want := range run.ss.ExpectLogOnce {
		for _, rep := range run.replicas {
			if !run.ran[rep.N] {
				continue // a standby that never ran
			}
			for _, p := range logOnceProblems(run.book.all(rep.Container), want) {
				run.fail("%s %s", rep.Service, p)
			}
		}
	}
}

// ssDirectorStart is in the first line the director logs when it starts.
const ssDirectorStart = "vmetric-director started"

// logOnceProblems splits a replica's log into the runs of its director, each
// from one start line to the next, and says where want is not logged exactly
// once in a run. A count over the whole log would pass a run that logged the
// line twice beside a run that never logged it.
func logOnceProblems(lines []ssLine, want string) []string {
	var runs []int // the lines that contain want, in each run
	before := 0    // and before the first start
	for _, l := range lines {
		start := strings.Contains(l.Text, ssDirectorStart)
		if start && strings.Contains(l.Text, want) {
			// The line would be found once in every run whatever the
			// director did, so the check would prove nothing.
			return []string{fmt.Sprintf("logged %q in the line that starts the director, so the check cannot tell one run from the next", want)}
		}
		switch {
		case start:
			runs = append(runs, 0)
		case !strings.Contains(l.Text, want):
		case len(runs) == 0:
			before++
		default:
			runs[len(runs)-1]++
		}
	}
	if len(runs) == 0 {
		return []string{fmt.Sprintf("never logged %q, so the harness cannot tell the runs of its director apart", ssDirectorStart)}
	}
	var problems []string
	switch {
	case before == 1:
		problems = append(problems, fmt.Sprintf("logged %q once before its director first started", want))
	case before > 1:
		problems = append(problems, fmt.Sprintf("logged %q %d times before its director first started", want, before))
	}
	for i, n := range runs {
		switch {
		case n == 0:
			problems = append(problems, fmt.Sprintf("did not log %q in run %d of its director (of %d); want it once in each run", want, i+1, len(runs)))
		case n > 1:
			problems = append(problems, fmt.Sprintf("logged %q %d times in run %d of its director (of %d); want it once in each run", want, n, i+1, len(runs)))
		}
	}
	return problems
}

// moveAcrossCutLink cuts the link between the replica that runs move_device,
// which does not lead, and another replica that does not lead. Both keep the
// store and the leader. Then it renders the config's third stage, in which
// {{@.MoveTarget@}} is the second replica's member name: the case allows the
// device only there, so the leader moves it across the cut link at once. The
// device must start on the second replica. The run's ownership check fails it
// if the device ever runs on both. The link stays cut for hold_seconds after
// the move, and is then restored.
func (run *ssRun) moveAcrossCutLink() {
	d := run.ss.MoveDevice
	run.refreshAll()
	now := current()
	leaders := run.leaders(now)
	if len(leaders) != 1 {
		run.fail("the move needs one leader, and %d replicas lead (%s)", len(leaders), run.names(leaders))
		return
	}
	leader := leaders[0]
	owners := run.owners(d, now)
	if len(owners) != 1 {
		run.fail("device %d runs on %d replicas (%s); the move needs one owner", d, len(owners), run.names(owners))
		return
	}
	from := owners[0]
	if from == leader {
		run.fail("device %d runs on the leader %s; the move needs an owner that does not lead", d, run.replica(from).Service)
		return
	}
	to := 0
	for _, rep := range run.replicas {
		if run.running[rep.N] && rep.N != from && rep.N != leader {
			to = rep.N
			break
		}
	}
	if to == 0 {
		run.fail("no third replica runs, to move device %d to", d)
		return
	}
	frep, trep := run.replica(from), run.replica(to)
	member := run.memberOf(to)
	if member == "" {
		run.fail("%s has not logged its member name, so the config cannot name it", trep.Service)
		return
	}

	links := newLinkFaults(run.r.opts.NettoolImage, containerNetwork(frep.Container))
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := links.restoreAll(ctx); err != nil {
			run.fail("restoring the link between %s and %s: %v", frep.Service, trep.Service, err)
		}
	}
	defer restore()

	fmt.Printf("  cutting the link between %s (runs device %d) and %s; both keep the store and the leader %s…\n",
		frep.Service, d, trep.Service, run.replica(leader).Service)
	cut := map[linkKey]linkFault{
		{frep.Container, trep.Container}: {Cut: true},
		{trep.Container, frep.Container}: {Cut: true},
	}
	if err := links.set(run.r.ctx, cut); err != nil {
		run.fail("cutting the link between %s and %s: %v", frep.Service, trep.Service, err)
		return
	}
	cutAt := time.Now()
	if sleepCtx(run.r.ctx, time.Duration(run.ss.CutHoldOrDefault())*time.Second) != nil {
		return
	}

	skew := run.daemonSkew()
	issued := time.Now()
	if err := run.orch.RenderReplicaConfigsMoving(3, member); err != nil {
		run.fail("rendering the config's third stage: %v", err)
		return
	}
	fmt.Printf("  rendered the third stage: device %d may run only on %s (%s)\n", d, trep.Service, member)

	since := issued.Add(skew)
	deadline := earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline)
	var tookAt time.Time
	for tookAt.IsZero() && time.Now().Before(deadline) {
		_ = run.book.refresh(trep.Container)
		for _, e := range run.events(trep.Container) {
			if e.Kind == ssLeaseAcquired && e.Device == d && e.At.After(since) {
				tookAt = e.At
				break
			}
		}
		if tookAt.IsZero() && sleepCtx(run.r.ctx, 200*time.Millisecond) != nil {
			return
		}
	}
	if tookAt.IsZero() {
		run.fail("device %d did not start on %s within %ds of the third stage", d, trep.Service, run.ss.SettleOrDefault())
		return
	}
	moved := tookAt.Sub(since)
	fmt.Printf("    %s took device %d %.2fs after the third stage; the link stays cut for %ds more\n",
		trep.Service, d, moved.Seconds(), run.ss.HoldOrDefault())
	run.measures["move_across_cut_link"] = map[string]any{
		"device": d, "from": frep.Service, "to": trep.Service, "leader": run.replica(leader).Service,
		"cut_before_move_seconds": issued.Sub(cutAt).Seconds(), "moved_seconds": moved.Seconds(),
	}

	// An old owner that does not let go shows it while the link is still cut.
	if sleepCtx(run.r.ctx, time.Duration(run.ss.HoldOrDefault())*time.Second) != nil {
		return
	}
	restore()
	run.checkSettled("after the move", earliest(time.Now().Add(time.Duration(run.ss.SettleOrDefault())*time.Second), run.deadline))
	run.checkHealthAll("after")
}

// memberOf returns the member name replica n's director last logged: the
// name the leader places devices on.
func (run *ssRun) memberOf(n int) string {
	rep := run.replica(n)
	_ = run.book.refresh(rep.Container)
	member := ""
	for _, e := range run.events(rep.Container) {
		if (e.Kind == ssManagerShared || e.Kind == ssBusStarted) && e.Member != "" {
			member = e.Member
		}
	}
	return member
}
