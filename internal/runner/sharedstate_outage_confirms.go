package runner

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// This file judges what a sender gets confirmed around a pause_store outage.
// A director that confirms a record only once the store holds it confirms
// nothing while the store is away (max_confirmed_during_outage). And once the
// store is back, every replica must take records again
// (min_each_after_outage): a sender whose post fails moves on to the next
// replica, so the stream's total grows even when one replica never answers
// again.

// outageConfirms is each stream's sender's last progress line at one moment
// of an outage, by stream name. Only the streams that set
// max_confirmed_during_outage are read.
type outageConfirms map[string]genProgress

// readOutageConfirms reads the last progress line of the sender of each stream
// that sets max_confirmed_during_outage. The sender writes one a second, so a
// line read while the store is away was written while it was away, unless the
// outage began less than a second before.
func (run *ssRun) readOutageConfirms() outageConfirms {
	out := outageConfirms{}
	for _, st := range run.ss.Streams {
		if st.MaxConfirmedDuringOutage == nil {
			continue
		}
		if p, ok := readGeneratorProgressOf("bench-generator-" + st.Generator); ok {
			out[st.StreamName()] = p
		}
	}
	return out
}

// judgeOutageConfirms fails the case for each stream whose sender got more
// records confirmed between the progress lines in from and to, both read
// while the store was away, than max_confirmed_during_outage allows.
func (run *ssRun) judgeOutageConfirms(from, to outageConfirms) {
	confirmed := map[string]int64{}
	for _, st := range run.ss.Streams {
		if st.MaxConfirmedDuringOutage == nil {
			continue
		}
		name := st.StreamName()
		a, okFrom := from[name]
		b, okTo := to[name]
		if !okFrom || !okTo {
			run.fail("stream %s: the sender's progress could not be read during the outage, so max_confirmed_during_outage cannot be judged", name)
			continue
		}
		n := b.Acked - a.Acked
		confirmed[name] = n
		window := b.At.Sub(a.At)
		fmt.Printf("    stream %s: %s confirmed in %s late in the outage\n", name, countOf(n, "record"), window.Round(time.Second))
		if why := outageConfirmProblem(st, n, window); why != "" {
			run.fail("%s", why)
		}
	}
	if len(confirmed) > 0 {
		run.measures["outage_confirmed"] = confirmed
	}
}

// outageConfirmProblem says why a stream fails max_confirmed_during_outage, or
// "": n records were confirmed in the window late in the outage.
func outageConfirmProblem(st config.SharedStateStream, n int64, window time.Duration) string {
	if st.MaxConfirmedDuringOutage == nil || n <= *st.MaxConfirmedDuringOutage {
		return ""
	}
	return fmt.Sprintf("stream %s: the sender got %s confirmed in %s late in the outage, above max_confirmed_during_outage %d: a replica confirmed records while it could not store them",
		st.StreamName(), countOf(n, "record"), window.Round(time.Second), *st.MaxConfirmedDuringOutage)
}

// countConfirmedAtResume reads, for each stream that sets
// min_each_after_outage, how many records its sender had got confirmed
// through each replica it posts to when the store came back at `at`: the
// counts on the sender's first progress line written at that time or later.
// A sender whose progress cannot be read, or does not report its counts per
// target, fails the run.
func (run *ssRun) countConfirmedAtResume(at time.Time) {
	gens := map[string]config.GeneratorConfig{}
	for _, g := range run.tc.AllGenerators() {
		gens[g.ID] = g
	}
	for _, st := range run.ss.Streams {
		if st.MinEachAfterOutage <= 0 {
			continue
		}
		g := gens[st.Generator]
		container := "bench-generator-" + st.Generator
		tries, wait := progressWait(g, run.deadline)
		p, ok := readProgressBaseline(run.r.ctx, func() (genProgress, bool) {
			p, ok := readGeneratorProgressOf(container)
			return p, ok && !p.At.Before(at)
		}, tries, wait)
		if !ok {
			run.fail("stream %s: the sender wrote no progress line after the store came back, so the records each replica confirmed after the outage cannot be counted", st.StreamName())
			continue
		}
		if p.AckedPerTarget == nil {
			run.fail("stream %s: the sender's progress does not say what each replica confirmed, so min_each_after_outage cannot be judged; it needs the generator of PipeBench v0.4.17 or later", st.StreamName())
			continue
		}
		atResume := map[int]int64{}
		for _, n := range config.TargetReplicas(g.Target) {
			if n >= 1 && n <= len(run.replicas) {
				atResume[n] = confirmedThrough(p.AckedPerTarget, n)
			}
		}
		run.confirmedAtResume[st.StreamName()] = atResume
	}
}

// judgeEachAfterOutage returns why a stream fails min_each_after_outage, if it
// does: after is how many records each replica the sender posts to confirmed
// after the store came back.
func judgeEachAfterOutage(st config.SharedStateStream, after map[int]int64) []string {
	if st.MinEachAfterOutage <= 0 {
		return nil
	}
	if len(after) == 0 {
		return []string{fmt.Sprintf("stream %s sets min_each_after_outage, but no replica that its sender posts to was counted after the outage", st.StreamName())}
	}
	var out []string
	for _, n := range slices.Sorted(maps.Keys(after)) {
		if after[n] < st.MinEachAfterOutage {
			out = append(out, fmt.Sprintf("stream %s: the sender got %d records confirmed through subject-%d after the store came back, below min_each_after_outage %d, so that replica did not take records again as it should", st.StreamName(), after[n], n, st.MinEachAfterOutage))
		}
	}
	return out
}
