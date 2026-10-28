package runner

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// This file judges min_after_rejoin: that every replica an action replaced
// takes records again once it has rejoined. A sender that posts to several
// replicas moves on to the next one when a post fails, so the stream's total
// after the action (min_after_action) grows even when one new replica never
// answers. The sender's counts per target show each replica on its own.

// countConfirmedAtRejoin reads, for each stream that sets min_after_rejoin
// and whose sender posts to replica n, how many records the sender had got
// confirmed through n when n rejoined at `at`: the count on the sender's
// first progress line written at that time or later. Both times are on the
// clock of the Docker host, which stamps the replica's log and runs the
// sender. checkAckedStream subtracts the count from the sender's final one.
// A replica replaced again is counted from its last rejoin.
//
// A sender whose progress cannot be read, or does not report its counts per
// target, fails the run: what n confirmed after it rejoined could then not
// be told apart from what it confirmed before.
func (run *ssRun) countConfirmedAtRejoin(n int, at time.Time) {
	gens := map[string]config.GeneratorConfig{}
	for _, g := range run.tc.AllGenerators() {
		gens[g.ID] = g
	}
	rep := run.replica(n)
	for _, st := range run.ss.Streams {
		if st.MinAfterRejoin <= 0 || !slices.Contains(config.TargetReplicas(gens[st.Generator].Target), n) {
			continue
		}
		container := "bench-generator-" + st.Generator
		tries, wait := progressWait(gens[st.Generator], run.deadline)
		p, ok := readProgressBaseline(run.r.ctx, func() (genProgress, bool) {
			p, ok := readGeneratorProgressOf(container)
			return p, ok && !p.At.Before(at)
		}, tries, wait)
		if !ok {
			run.fail("stream %s: the sender wrote no progress line after %s rejoined, so the records it confirmed after that cannot be counted (did the sender stop first?)", st.StreamName(), rep.Service)
			continue
		}
		if p.AckedPerTarget == nil {
			run.fail("stream %s: the sender's progress does not say what each replica confirmed, so min_after_rejoin cannot be judged; it needs the generator of PipeBench v0.4.17 or later", st.StreamName())
			continue
		}
		atRejoin := run.confirmedAtRejoin[st.StreamName()]
		if atRejoin == nil {
			atRejoin = map[int]int64{}
			run.confirmedAtRejoin[st.StreamName()] = atRejoin
		}
		atRejoin[n] = confirmedThrough(p.AckedPerTarget, n)
		fmt.Printf("    stream %s: %d records confirmed through %s when it rejoined\n", st.StreamName(), atRejoin[n], rep.Service)
	}
}

// confirmedThrough adds up the records that the targets naming replica n
// confirmed. perTarget is a sender's count per target, by the target as its
// target list names it.
func confirmedThrough(perTarget map[string]int64, n int) int64 {
	var total int64
	for target, count := range perTarget {
		if slices.Contains(config.TargetReplicas(target), n) {
			total += count
		}
	}
	return total
}

// confirmedAfterRejoin returns how many records each replaced replica
// confirmed after it rejoined: its share of the sender's final counts per
// target, less what it had confirmed when it rejoined.
func confirmedAfterRejoin(final map[string]int64, atRejoin map[int]int64) map[int]int64 {
	out := map[int]int64{}
	for n, before := range atRejoin {
		out[n] = confirmedThrough(final, n) - before
	}
	return out
}

// judgeAfterRejoin returns why a stream fails min_after_rejoin, if it does:
// after is how many records each replaced replica that the sender posts to
// confirmed after it rejoined.
func judgeAfterRejoin(st config.SharedStateStream, after map[int]int64) []string {
	if st.MinAfterRejoin <= 0 {
		return nil
	}
	if len(after) == 0 {
		return []string{fmt.Sprintf("stream %s sets min_after_rejoin, but no replica that its sender posts to was replaced and counted, so no new replica was judged", st.StreamName())}
	}
	var out []string
	for _, n := range slices.Sorted(maps.Keys(after)) {
		if after[n] < st.MinAfterRejoin {
			out = append(out, fmt.Sprintf("stream %s: the sender got %d records confirmed through subject-%d after it rejoined, below min_after_rejoin %d, so the new replica did not take records again as it should", st.StreamName(), after[n], n, st.MinAfterRejoin))
		}
	}
	return out
}

// progressStep is how far apart the reads of a sender's progress are.
const progressStep = 250 * time.Millisecond

// progressWait returns how many times, and how far apart, to read a sender's
// progress line before giving up. The sender writes one every
// GENERATOR_PROGRESS_INTERVAL, which is 1 second unless the case sets it. So
// the reads cover two of its intervals, and at least five seconds, but never
// go past the run's deadline.
func progressWait(g config.GeneratorConfig, deadline time.Time) (tries int, wait time.Duration) {
	interval := time.Second
	if v := strings.TrimSpace(g.Env["GENERATOR_PROGRESS_INTERVAL"]); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			interval = d
		}
	}
	window := max(5*time.Second, 2*interval)
	if !deadline.IsZero() {
		window = max(min(window, time.Until(deadline)), progressStep)
	}
	return int(window / progressStep), progressStep
}
