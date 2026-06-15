package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"github.com/VirtualMetric/PipeBench/internal/orchestrator"
)

// PerReceiverMetrics holds per-receiver counts and the receive-window
// timestamps used by validators. Aggregated by aggregateReceivers; saved
// onto RunResult so the load-balance fairness check can introspect them.
type PerReceiverMetrics struct {
	ID              string
	LinesReceived   int64
	BytesReceived   int64
	FirstReceivedNs int64
	LastReceivedNs  int64
	Duplicates      int64
	MalformedLines  int64
	InvalidJSON     int64
}

// aggregateReceivers fans /metrics queries out across every receiver host
// port and folds the results into a single ReceiverMetrics plus a
// per-receiver slice for downstream fairness checks. The aggregated
// LinesReceived is the SUM across receivers; the first/last timestamps
// take the earliest and latest across the set.
func (r *Runner) aggregateReceivers(ports map[string]int, timeout time.Duration) (ReceiverMetrics, []PerReceiverMetrics, error) {
	if len(ports) == 0 {
		return ReceiverMetrics{}, nil, fmt.Errorf("no receivers to query")
	}
	per := make([]PerReceiverMetrics, 0, len(ports))
	var combined ReceiverMetrics
	var firstErr error
	// Deterministic order so reports and load-balance lists match the
	// case.yaml insertion order whenever possible.
	ids := make([]string, 0, len(ports))
	for id := range ports {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		port := ports[id]
		m, err := r.queryReceiverMetrics(port, timeout)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("receiver %q metrics: %w", id, err)
			}
			continue
		}
		per = append(per, PerReceiverMetrics{
			ID:              id,
			LinesReceived:   m.LinesReceived,
			BytesReceived:   m.BytesReceived,
			FirstReceivedNs: m.FirstReceivedNs,
			LastReceivedNs:  m.LastReceivedNs,
			Duplicates:      m.Duplicates,
			MalformedLines:  m.MalformedLines,
			InvalidJSON:     m.InvalidJSONLines,
		})
		combined.LinesReceived += m.LinesReceived
		combined.BytesReceived += m.BytesReceived
		combined.Duplicates += m.Duplicates
		combined.MalformedLines += m.MalformedLines
		combined.InvalidJSONLines += m.InvalidJSONLines
		if combined.FirstReceivedNs == 0 || (m.FirstReceivedNs > 0 && m.FirstReceivedNs < combined.FirstReceivedNs) {
			combined.FirstReceivedNs = m.FirstReceivedNs
		}
		if m.LastReceivedNs > combined.LastReceivedNs {
			combined.LastReceivedNs = m.LastReceivedNs
		}
		// Latency metrics: take the first non-zero set we see. Per-receiver
		// latency is more useful here than averaging, but the singular
		// RunResult shape only has one set of fields; expose more later if
		// needed.
		if combined.LatencyP50Ms == 0 && m.LatencyP50Ms > 0 {
			combined.LatencyP50Ms = m.LatencyP50Ms
			combined.LatencyP95Ms = m.LatencyP95Ms
			combined.LatencyP99Ms = m.LatencyP99Ms
		}
	}
	return combined, per, firstErr
}

// aggregateGenerators parses each generator container's stdout (the JSON
// result blob) and sums the totals. FirstSentNs is the earliest across all
// generators; LastSentNs is the latest. Stdout from multiple generators is
// concatenated by GeneratorStdout(); we split on '}' boundaries and parse
// each blob.
func (r *Runner) aggregateGenerators(orch orchestrator.Orchestrator) GeneratorResult {
	containers := orch.GeneratorContainers()
	if len(containers) <= 1 {
		return r.parseGeneratorStats(orch.GeneratorStdout())
	}
	var combined GeneratorResult
	combinedDuration := int64(0)
	stdout := orch.GeneratorStdout()
	for _, blob := range splitJSONBlobs(stdout) {
		var g GeneratorResult
		if err := json.Unmarshal([]byte(blob), &g); err != nil {
			continue
		}
		combined.LinesSent += g.LinesSent
		combined.BytesSent += g.BytesSent
		if combined.FirstSentNs == 0 || (g.FirstSentNs > 0 && g.FirstSentNs < combined.FirstSentNs) {
			combined.FirstSentNs = g.FirstSentNs
		}
		if g.LastSentNs > combined.LastSentNs {
			combined.LastSentNs = g.LastSentNs
		}
		if g.DurationMs > combinedDuration {
			combinedDuration = g.DurationMs
		}
	}
	combined.DurationMs = combinedDuration
	return combined
}

// splitJSONBlobs walks a concatenated stdout buffer and yields each
// top-level JSON object as a substring. Cheap state machine — only counts
// braces outside of strings, which is enough for the well-formed
// JSON-with-indent the generator emits.
func splitJSONBlobs(s string) []string {
	var out []string
	depth := 0
	inStr := false
	esc := false
	start := -1
	for i, c := range s {
		if esc {
			esc = false
			continue
		}
		if c == '\\' && inStr {
			esc = true
			continue
		}
		if c == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		if c == '{' {
			if depth == 0 {
				start = i
			}
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 && start >= 0 {
				out = append(out, s[start:i+1])
				start = -1
			}
		}
	}
	return out
}

// receiverTimestamps fetches arrival nano-second timestamps from a single
// receiver's /arrival_times endpoint. Returns sorted ascending. Only
// populated when the case enables correctness.rate_ceiling — every other
// run keeps the endpoint silent.
func (r *Runner) receiverTimestamps(port int, timeout time.Duration) ([]int64, error) {
	url := fmt.Sprintf("http://localhost:%d/arrival_times", port)
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url) //nolint:noctx
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var ts []int64
	if err := json.Unmarshal(body, &ts); err != nil {
		return nil, fmt.Errorf("decoding arrival_times: %w", err)
	}
	return ts, nil
}

// RateWindowResult holds the per-window EPS validator outcome. Persisted
// onto the per-run extras blob so an analyst can drill into a failed
// throttle-correctness run without re-instrumenting the receiver.
type RateWindowResult struct {
	MaxObservedEPS        float64 `json:"rate_window_max_observed_eps"`
	OvershootCount        int64   `json:"rate_window_overshoot_count"`
	FirstOvershootStartNs int64   `json:"rate_window_overshoot_first_window_start_ns,omitempty"`
	Passed                bool    `json:"rate_window_passed"`
	FailureReason         string  `json:"rate_window_failure,omitempty"`
}

// applyRateCeiling slides a Window-sized window across arrival timestamps
// and checks that EPS never exceeds MaxEPS * (1 + Tolerance). Skips warmup
// and cooldown bands at the start and end of the receive window. With
// Sample == "peak" the check only fails when the maximum across all
// windows exceeds the threshold; with "every" (default) it fails on the
// first overshoot.
//
// Before the sliding analysis, two unconditional gates catch the case
// where the subject bypassed throttling and burst the entire payload in
// well under one window: (1) delivery span shorter than `window`, which
// makes a windowed analysis nonsense; (2) average EPS across the
// delivery span over threshold. Without these, sub-second bursts either
// land entirely inside skipWarmup (returning max=0) or produce a
// single-window count near the ceiling (returning max≈ceiling), both
// false PASSes.
func applyRateCeiling(rc config.RateCeilingConfig, timestamps []int64) RateWindowResult {
	if !rc.Enabled() || len(timestamps) == 0 {
		return RateWindowResult{Passed: true}
	}
	window := parseDurationOr(rc.Window, 1*time.Second)
	if window <= 0 {
		window = 1 * time.Second
	}
	skipWarmup := parseDurationOr(rc.SkipWarmup, 0)
	skipCooldown := parseDurationOr(rc.SkipCooldown, 0)
	tolerance := rc.Tolerance
	threshold := rc.MaxEPS * (1.0 + tolerance)

	slices.Sort(timestamps)
	first := timestamps[0]
	last := timestamps[len(timestamps)-1]
	deliverySpan := time.Duration(last - first)

	avgEPS := 0.0
	if deliverySpan > 0 {
		avgEPS = float64(len(timestamps)) / deliverySpan.Seconds()
	}

	// Gate 1: a rate_ceiling case implicitly expects throttling, so a
	// delivery that completed faster than one window is a defect — the
	// sliding analysis below cannot honestly evaluate it (no full window
	// fits inside the receive band).
	if deliverySpan < window {
		return RateWindowResult{
			MaxObservedEPS:        avgEPS,
			OvershootCount:        1,
			FirstOvershootStartNs: first,
			Passed:                false,
			FailureReason: fmt.Sprintf(
				"delivery completed in %v, shorter than rate_ceiling.window (%v) — throttle bypassed (avg EPS %.2f, ceiling %.2f)",
				deliverySpan.Round(time.Millisecond), window, avgEPS, threshold),
		}
	}

	// Gate 2: avg EPS over the delivery span. Independent of windowing
	// and skip_* framing, so it catches bursts that the slider misses
	// because skipWarmup ate them or because count/window happened to
	// land near the ceiling.
	if avgEPS > threshold {
		return RateWindowResult{
			MaxObservedEPS:        avgEPS,
			OvershootCount:        1,
			FirstOvershootStartNs: first,
			Passed:                false,
			FailureReason: fmt.Sprintf(
				"average EPS %.2f over %v exceeded ceiling %.2f (max_eps=%.2f tolerance=%.2f)",
				avgEPS, deliverySpan.Round(time.Millisecond), threshold, rc.MaxEPS, tolerance),
		}
	}

	// Bound the analysis to [first+skipWarmup, last-skipCooldown].
	windowStart := first + skipWarmup.Nanoseconds()
	windowEnd := last - skipCooldown.Nanoseconds()
	if windowEnd <= windowStart {
		// skipWarmup+skipCooldown swallow the entire receive band. The
		// avg-EPS gate above already verified the bypass-bypass case;
		// returning max=avgEPS surfaces a real number on the dashboard
		// instead of zero.
		return RateWindowResult{MaxObservedEPS: avgEPS, Passed: true}
	}

	// Sliding window via two indexes. For each starting position, count
	// timestamps in [t, t+window) and divide by window seconds. We step
	// in window/10-sized increments to catch overshoots inside a window
	// without re-running the count for every nanosecond.
	step := window / 10
	if step <= 0 {
		step = 1 * time.Millisecond
	}
	winNs := window.Nanoseconds()

	var maxEPS float64
	var overshoots int64
	var firstOvershoot int64

	left := 0
	for t := windowStart; t+winNs <= windowEnd; t += step.Nanoseconds() {
		// Advance left until inside [t, t+winNs)
		for left < len(timestamps) && timestamps[left] < t {
			left++
		}
		right := left
		for right < len(timestamps) && timestamps[right] < t+winNs {
			right++
		}
		count := right - left
		eps := float64(count) / window.Seconds()
		if eps > maxEPS {
			maxEPS = eps
		}
		if eps > threshold {
			overshoots++
			if firstOvershoot == 0 {
				firstOvershoot = t
			}
		}
	}

	res := RateWindowResult{
		MaxObservedEPS: maxEPS,
		OvershootCount: overshoots,
		Passed:         true,
	}
	if rc.Sample == "peak" {
		if maxEPS > threshold {
			res.Passed = false
			res.FirstOvershootStartNs = firstOvershoot
			res.FailureReason = fmt.Sprintf(
				"peak EPS %.2f exceeded ceiling %.2f (max_eps=%.2f tolerance=%.2f)",
				maxEPS, threshold, rc.MaxEPS, tolerance)
		}
	} else {
		if overshoots > 0 {
			res.Passed = false
			res.FirstOvershootStartNs = firstOvershoot
			res.FailureReason = fmt.Sprintf(
				"%d windows exceeded ceiling %.2f (max_eps=%.2f tolerance=%.2f, max_observed=%.2f)",
				overshoots, threshold, rc.MaxEPS, tolerance, maxEPS)
		}
	}
	return res
}

// LoadBalanceResult holds the fairness check outcome plus the per-receiver
// counts so a failure includes the exact split.
type LoadBalanceResult struct {
	MinShareRatioObserved float64          `json:"min_share_ratio_observed"`
	MinShareRatioRequired float64          `json:"min_share_ratio_required"`
	Passed                bool             `json:"pass"`
	FailureReason         string           `json:"failure,omitempty"`
	PerReceiverCounts     map[string]int64 `json:"per_receiver_counts"`
}

// applyLoadBalance computes min(counts)/max(counts) across the configured
// receivers and reports pass/fail against MinShareRatio. Skips the check
// (returns Passed=true) when total counts are below MinSampleSize — small
// samples produce noisy fairness ratios.
func applyLoadBalance(lb config.LoadBalanceConfig, per []PerReceiverMetrics) LoadBalanceResult {
	if !lb.Enabled() || len(per) == 0 {
		return LoadBalanceResult{Passed: true}
	}
	// Pick the relevant receiver subset.
	include := map[string]bool{}
	if len(lb.Receivers) == 0 {
		for _, m := range per {
			include[m.ID] = true
		}
	} else {
		for _, id := range lb.Receivers {
			include[id] = true
		}
	}

	counts := map[string]int64{}
	var total int64
	for _, m := range per {
		if !include[m.ID] {
			continue
		}
		counts[m.ID] = m.LinesReceived
		total += m.LinesReceived
	}
	if lb.MinSampleSize > 0 && total < lb.MinSampleSize {
		return LoadBalanceResult{
			Passed:                true,
			MinShareRatioRequired: lb.MinShareRatio,
			PerReceiverCounts:     counts,
		}
	}
	if len(counts) == 0 {
		return LoadBalanceResult{Passed: true, PerReceiverCounts: counts, MinShareRatioRequired: lb.MinShareRatio}
	}
	var minC, maxC int64 = -1, -1
	for _, v := range counts {
		if minC < 0 || v < minC {
			minC = v
		}
		if maxC < 0 || v > maxC {
			maxC = v
		}
	}
	var ratio float64
	if maxC > 0 {
		ratio = float64(minC) / float64(maxC)
	}
	res := LoadBalanceResult{
		MinShareRatioObserved: ratio,
		MinShareRatioRequired: lb.MinShareRatio,
		Passed:                ratio >= lb.MinShareRatio,
		PerReceiverCounts:     counts,
	}
	if !res.Passed {
		// Build a stable id=count summary for the failure message.
		ids := make([]string, 0, len(counts))
		for id := range counts {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		parts := make([]string, 0, len(ids))
		for _, id := range ids {
			parts = append(parts, fmt.Sprintf("%s=%d", id, counts[id]))
		}
		res.FailureReason = fmt.Sprintf(
			"load-balance ratio %.2f below required %.2f (counts: %s)",
			ratio, lb.MinShareRatio, strings.Join(parts, ", "),
		)
	}
	return res
}

// tlsRequested returns true when any generator in the case has TLS enabled.
// Used to gate cert auto-generation and the subject capability check.
func tlsRequested(tc *config.TestCase) bool {
	if tc.Generator.TLS.Enabled {
		return true
	}
	for _, g := range tc.Generators {
		if g.TLS.Enabled {
			return true
		}
	}
	return false
}

func parseDurationOr(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}
