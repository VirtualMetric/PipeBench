package main

// Latency steps for http_ack.
//
// GENERATOR_STEPS is a comma-separated list of steps that run one after the other:
//
//	capacity:60s   every worker posts as fast as it is answered (closed load), and the step's rate
//	               is the capacity that later percent steps take a share of
//	1000:120s      open load at 1,000 lines a second
//	50%:120s       open load at half the capacity that the last capacity step measured
//
// A subject can confirm lines faster than it delivers them, and keep the rest in a queue. So with
// GENERATOR_RECEIVER_METRICS (the receiver's metrics address), the capacity is the rate at which
// lines arrived at the receiver in the capacity step's window, and a share of it is a load the
// subject can deliver. Without a receiver, or with GENERATOR_STEP_CAPACITY=acknowledgements, it is
// the rate of acknowledged lines. Each percent step says which one it took.
//
// With a receiver, each step but the last also waits, up to GENERATOR_STEP_DRAIN (default 60s),
// until the receiver has every line acknowledged so far, so that the next step starts with an
// empty queue. Each step records how long that took, and the rate its lines arrived at.
//
// Under open load, each post has a time on a fixed timeline, and its latency counts from that
// time, not from when a worker got to it. So a subject that stalls shows the stall as latency,
// instead of slowing the load down: the timeline does not wait for answers. The workers keep
// their CONN ids and sequences across the steps, and each sends its batches one at a time, so the
// receiver still sees each CONN id's lines acknowledged in order.
//
// The first GENERATOR_STEP_WARMUP of each step (default 20s) is left out of its numbers.

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// What the percent steps take a share of.
const (
	capacityFromReceiver = "receiver"
	capacityFromAcks     = "acknowledgements"
)

// stepKind is how a step sets its load.
type stepKind string

const (
	stepCapacity stepKind = "capacity"
	stepRate     stepKind = "rate"
	stepPercent  stepKind = "percent"
)

// stepSpec is one step of GENERATOR_STEPS.
type stepSpec struct {
	Name     string
	Kind     stepKind
	Value    float64 // lines a second for a rate step, percent for a percent step
	Duration time.Duration
}

// parseSteps reads GENERATOR_STEPS. An empty list is no steps.
func parseSteps(s string) ([]stepSpec, error) {
	var steps []stepSpec
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		load, dur, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("step %q has no duration: write it as load:duration, such as 1000:120s", part)
		}
		d, err := time.ParseDuration(strings.TrimSpace(dur))
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("step %q has no valid duration", part)
		}
		load = strings.TrimSpace(load)
		st := stepSpec{Name: load, Duration: d}
		switch {
		case load == "capacity":
			st.Kind = stepCapacity
		case strings.HasSuffix(load, "%"):
			v, err := strconv.ParseFloat(strings.TrimSuffix(load, "%"), 64)
			if err != nil || !finitePositive(v) {
				return nil, fmt.Errorf("step %q has no valid percent", part)
			}
			st.Kind, st.Value = stepPercent, v
		default:
			v, err := strconv.ParseFloat(load, 64)
			if err != nil || !finitePositive(v) {
				return nil, fmt.Errorf("step %q is neither capacity, a rate nor a percent", part)
			}
			st.Kind, st.Value = stepRate, v
		}
		steps = append(steps, st)
	}
	return steps, nil
}

// finitePositive is whether v is a rate or a percent: ParseFloat takes NaN and Inf too.
func finitePositive(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v > 0
}

// stepsDuration is how long the steps last together.
func stepsDuration(steps []stepSpec) time.Duration {
	var d time.Duration
	for _, st := range steps {
		d += st.Duration
	}
	return d
}

// ackStepResult is what one step measured. The times are Unix nanoseconds. Its numbers cover the
// measured window, from MeasureFromNs to EndNs.
type ackStepResult struct {
	Name string `json:"name"`
	// Load is closed (capacity) or open.
	Load string `json:"load"`
	// RateAsked is the lines a second that an open step asked for.
	RateAsked float64 `json:"rate_asked,omitempty"`
	StartNs   int64   `json:"start_ns"`
	// MeasureFromNs is the end of the step's warm-up, and EndNs the moment its last post ended.
	MeasureFromNs int64 `json:"measure_from_ns"`
	EndNs         int64 `json:"end_ns"`
	// LinesAcked are the lines acknowledged in the measured window, and AckedPerSec their rate.
	LinesAcked  int64   `json:"lines_acked"`
	AckedPerSec float64 `json:"acked_per_sec"`
	Posts       int64   `json:"posts"`
	// LatePosts are the open load's posts that started more than a millisecond after their time:
	// every worker was still busy. Their latency counts from their time.
	LatePosts int64 `json:"late_posts,omitempty"`
	// Unacked are the lines of posts that no target acknowledged before the run's end.
	Unacked int64 `json:"lines_unacked,omitempty"`
	// AckLatencyUs is the time from each post's time to its acknowledgement: from its place on the
	// timeline under open load, and from its send under closed load.
	AckLatencyUs *latencyUs `json:"ack_latency_us,omitempty"`
	// CapacityPerSec is the capacity that a percent step took its share of, and CapacityFrom where
	// it came from: "receiver" (the rate lines arrived at the receiver in the capacity step) or
	// "acknowledgements" (the rate the capacity step's lines were acknowledged).
	CapacityPerSec float64 `json:"capacity_per_sec,omitempty"`
	CapacityFrom   string  `json:"capacity_from,omitempty"`
	// DeliveredPerSec is the rate the lines arrived at the receiver in the measured window.
	DeliveredPerSec float64 `json:"delivered_per_sec,omitempty"`
	// DrainMs is how long the receiver took after the step to have every line acknowledged so far,
	// and DrainTimedOut says it did not within GENERATOR_STEP_DRAIN.
	DrainMs       int64  `json:"drain_ms,omitempty"`
	DrainTimedOut bool   `json:"drain_timed_out,omitempty"`
	Error         string `json:"error,omitempty"`
}

// stepRecorder gathers one step's numbers. Its methods are safe for concurrent use.
type stepRecorder struct {
	from time.Time
	mu   sync.Mutex
	hist durHist

	posts, acked, late, unacked atomic.Int64
}

// note counts a post acknowledged at ackedAt whose time was at, with lines lines.
func (r *stepRecorder) note(at, ackedAt time.Time, lines int) {
	if at.Before(r.from) {
		return
	}
	r.posts.Add(1)
	r.acked.Add(int64(lines))
	r.mu.Lock()
	r.hist.record(ackedAt.Sub(at))
	r.mu.Unlock()
}

// runAckSteps runs the steps one after the other on slots, and returns their results and the end
// of the last step. The posts of a step that are still being tried when the step ends go on
// until hardStop.
func runAckSteps(cfg config, ac ackConfig, steps []stepSpec, slots []*ackSlot, stats *ackStats,
	picker *targetPicker, clock *sendClock, pads []string, drain time.Duration,
) []ackStepResult {
	results := make([]ackStepResult, 0, len(steps))
	capacity, from := 0.0, ""
	var rv *receiverView
	if ac.receiver != "" {
		rv = &receiverView{base: ac.receiver, client: &http.Client{Timeout: 10 * time.Second}}
	}
	for i, st := range steps {
		res := runAckStep(cfg, ac, st, capacity, from, slots, stats, picker, clock, pads, drain)
		// The next step starts once the receiver has every line acknowledged so far
		if rv != nil && i < len(steps)-1 {
			waited, ok := rv.waitFor(stats.acked.Load(), ac.stepDrain)
			res.DrainMs, res.DrainTimedOut = waited.Milliseconds(), !ok
		}
		if rv != nil {
			if tl, err := rv.timeline(); err == nil {
				res.DeliveredPerSec, _ = tl.rate(res.MeasureFromNs, res.EndNs)
			}
		}
		if st.Kind == stepCapacity && res.Error == "" {
			// A receiver that got nothing, or whose timeline could not be read, gives no
			// capacity: the acknowledged rate is a load the subject may not deliver.
			capacity, from = res.AckedPerSec, capacityFromAcks
			if ac.capacityFrom == capacityFromReceiver {
				capacity, from = res.DeliveredPerSec, capacityFromReceiver
			}
		}
		results = append(results, res)
	}
	return results
}

// receiverView reads the receiver's timeline of arrivals (/timeline of its metrics address).
type receiverView struct {
	base   string
	client *http.Client
}

// arrivals is the receiver's /timeline: the lines that arrived for the first time, in buckets of
// BucketMs from StartNs.
type arrivals struct {
	StartNs  int64   `json:"start_ns"`
	BucketMs int64   `json:"bucket_ms"`
	Unique   []int64 `json:"unique"`
}

func (rv *receiverView) timeline() (arrivals, error) {
	var tl arrivals
	resp, err := rv.client.Get(rv.base + "/timeline")
	if err != nil {
		return tl, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return tl, fmt.Errorf("the receiver's /timeline answered %s", resp.Status)
	}
	return tl, json.NewDecoder(resp.Body).Decode(&tl)
}

// total is every line that arrived.
func (tl arrivals) total() int64 {
	var n int64
	for _, v := range tl.Unique {
		n += v
	}
	return n
}

// rate is the lines a second that arrived in the whole buckets between fromNs and toNs. ok is false
// when no whole bucket lies between them.
func (tl arrivals) rate(fromNs, toNs int64) (float64, bool) {
	if tl.BucketMs <= 0 || tl.StartNs <= 0 {
		return 0, false
	}
	b := tl.BucketMs * int64(time.Millisecond)
	first := ceilDiv(fromNs-tl.StartNs, b)
	last := floorDiv(toNs-tl.StartNs, b) // exclusive
	if last <= first {
		return 0, false
	}
	var n int64
	for i := first; i < last; i++ {
		if i >= 0 && i < int64(len(tl.Unique)) {
			n += tl.Unique[i]
		}
	}
	return float64(n) / (float64((last-first)*b) / float64(time.Second)), true
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

func ceilDiv(a, b int64) int64 { return -floorDiv(-a, b) }

// waitFor waits until the receiver has at least lines lines, or until limit. It returns how long it
// waited, and whether the receiver got there.
func (rv *receiverView) waitFor(lines int64, limit time.Duration) (time.Duration, bool) {
	start := time.Now()
	for {
		if tl, err := rv.timeline(); err == nil && tl.total() >= lines {
			return time.Since(start), true
		}
		if time.Since(start) >= limit {
			return time.Since(start), false
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// runAckStep runs one step. capacity is the rate of the last capacity step, or 0, and from says
// where it came from.
func runAckStep(cfg config, ac ackConfig, st stepSpec, capacity float64, from string, slots []*ackSlot, stats *ackStats,
	picker *targetPicker, clock *sendClock, pads []string, drain time.Duration,
) ackStepResult {
	start := time.Now()
	end := start.Add(st.Duration)
	warmup := min(ac.stepWarmup, st.Duration/2)
	rec := &stepRecorder{from: start.Add(warmup)}
	res := ackStepResult{Name: st.Name, Load: "open", StartNs: start.UnixNano(), MeasureFromNs: rec.from.UnixNano()}

	rate := st.Value
	switch st.Kind {
	case stepCapacity:
		res.Load = "closed"
	case stepPercent:
		if capacity <= 0 {
			res.Error = "no capacity step before it measured a rate"
			if from != "" {
				res.Error += " (capacity_from: " + from + ")"
			}
			res.EndNs = start.UnixNano()
			return res
		}
		rate = capacity * st.Value / 100
		res.CapacityPerSec, res.CapacityFrom = capacity, from
	}
	if res.Load == "open" {
		res.RateAsked = rate
	}
	hardStop := end.Add(drain)

	var wg sync.WaitGroup
	if st.Kind == stepCapacity {
		for _, slot := range slots {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(end) {
					sent := time.Now()
					ackedAt, ok := slot.send(cfg, ac, stats, picker, clock, pads, sent.UnixNano(), hardStop)
					if !ok {
						rec.unacked.Add(int64(ac.batch))
						return
					}
					rec.note(sent, ackedAt, ac.batch)
				}
			}()
		}
	} else {
		// Post i's time is start + i*interval. Each worker takes the next time when it is free.
		interval := float64(ac.batch) / rate * float64(time.Second)
		var next atomic.Int64
		for _, slot := range slots {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					at := start.Add(time.Duration(float64(next.Add(1)-1) * interval))
					if !at.Before(end) {
						return
					}
					if d := time.Until(at); d > 0 {
						time.Sleep(d)
					} else if -d > time.Millisecond && !at.Before(rec.from) {
						rec.late.Add(1)
					}
					ackedAt, ok := slot.send(cfg, ac, stats, picker, clock, pads, at.UnixNano(), hardStop)
					if !ok {
						rec.unacked.Add(int64(ac.batch))
						return
					}
					rec.note(at, ackedAt, ac.batch)
				}
			}()
		}
	}
	wg.Wait()
	// An open step's workers stop once the next post's time is at or after end, which is
	// up to one interval before it. The step still lasts until end, so that its rate is over
	// the time it was asked to run and the next step starts on time. Posts that overran end
	// keep the later stop.
	if d := time.Until(end); d > 0 && st.Kind != stepCapacity {
		time.Sleep(d)
	}

	stop := time.Now()
	res.EndNs = stop.UnixNano()
	res.Posts, res.LinesAcked, res.LatePosts, res.Unacked = rec.posts.Load(), rec.acked.Load(), rec.late.Load(), rec.unacked.Load()
	if window := stop.Sub(rec.from).Seconds(); window > 0 {
		res.AckedPerSec = float64(res.LinesAcked) / window
	}
	rec.mu.Lock()
	res.AckLatencyUs = rec.hist.summary()
	rec.mu.Unlock()
	if res.Posts == 0 && res.Error == "" {
		res.Error = "no post was acknowledged in the measured window"
	}
	return res
}
