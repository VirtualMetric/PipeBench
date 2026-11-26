package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// LatencyConfig is a case's `latency:` block. It runs the http_ack generator through a
// list of steps, each at a load of its own, and records for each step:
//
//   - the acknowledgement latency that the generator measured, in microseconds;
//   - the end-to-end latency that the receiver measured for the lines sent in the step;
//   - with stages, how long each stage of a vmetric director took in the step, from the
//     director's own histograms (`director -latency`).
//
// With disk_baseline, the harness first measures how fast the disk under the subject's data
// folder syncs (--subject-data). Every number is in microseconds, with p99.9 and the maximum.
//
//	latency:
//	  steps: ["capacity:60s", "1000:120s", "10000:120s", "50%:120s", "80%:120s"]
//	  step_warmup: 20s
//	  disk_baseline: true
//	  stages: true
//
// A step is capacity, a rate of lines a second, or a percent of the rate that the last capacity
// step measured, each with its duration. Under a rate or a percent, the load is open: a post's
// latency counts from its place on a fixed timeline, so a stall shows as latency instead of
// slowing the load down.
//
// A subject can confirm lines faster than it delivers them. So the capacity that a percent step
// takes a share of is, by default, the rate the lines arrived at the receiver in the capacity
// step (capacity_from: receiver), not the rate they were confirmed at (capacity_from:
// acknowledgements). Each step but the last waits, up to step_drain (default 60s), for the receiver
// to have every confirmed line, so that the next step starts with an empty queue.
type LatencyConfig struct {
	Steps []string `yaml:"steps"`
	// StepWarmup is the part of each step that its numbers leave out. Default 20s.
	StepWarmup string `yaml:"step_warmup"`
	// DiskBaseline measures the disk under --subject-data before the subject starts.
	DiskBaseline bool `yaml:"disk_baseline"`
	// Stages reads a vmetric director's stage times every StageInterval (default 5s) while the
	// load runs, and gives each step the difference of the readings around it.
	Stages        bool   `yaml:"stages"`
	StageInterval string `yaml:"stage_interval"`
	// CapacityFrom is what the percent steps take a share of: "receiver" (the default) or
	// "acknowledgements".
	CapacityFrom string `yaml:"capacity_from"`
	// StepDrain bounds each step's wait for the receiver to catch up. Default 60s.
	StepDrain string `yaml:"step_drain"`
}

const defaultStepDrain = 60 * time.Second

// StepDrainOrDefault returns how long a step waits at most for the receiver to catch up.
func (l *LatencyConfig) StepDrainOrDefault() time.Duration {
	if d, err := time.ParseDuration(l.StepDrain); err == nil && d >= 0 {
		return d
	}
	return defaultStepDrain
}

const defaultStepWarmup = 20 * time.Second

// StageIntervalOrDefault returns how often the harness reads the director's stage times.
func (l *LatencyConfig) StageIntervalOrDefault() time.Duration {
	if d, err := time.ParseDuration(l.StageInterval); err == nil && d > 0 {
		return d
	}
	return 5 * time.Second
}

// StepWarmupOrDefault returns the part of each step that its numbers leave out.
func (l *LatencyConfig) StepWarmupOrDefault() time.Duration {
	if d, err := time.ParseDuration(l.StepWarmup); err == nil && d >= 0 {
		return d
	}
	return defaultStepWarmup
}

// StepsDuration returns how long the steps last together. It is 0 while a step does not parse.
func (l *LatencyConfig) StepsDuration() time.Duration {
	var total time.Duration
	for _, s := range l.Steps {
		_, d, err := parseLatencyStep(s)
		if err != nil {
			return 0
		}
		total += d
	}
	return total
}

// MinDuration returns how long the generator may need for the steps: the steps, and the longest
// wait for the receiver after each step but the last. It is 0 while a step does not parse.
func (l *LatencyConfig) MinDuration() time.Duration {
	d := l.StepsDuration()
	if d == 0 {
		return 0
	}
	return d + time.Duration(len(l.Steps)-1)*l.StepDrainOrDefault()
}

// parseLatencyStep reads one step: capacity, a rate or a percent, a colon, and a duration. It
// returns the load part and the duration.
func parseLatencyStep(s string) (string, time.Duration, error) {
	load, dur, ok := strings.Cut(strings.TrimSpace(s), ":")
	if !ok {
		return "", 0, fmt.Errorf("step %q has no duration: write it as load:duration, such as 1000:120s", s)
	}
	d, err := time.ParseDuration(strings.TrimSpace(dur))
	if err != nil || d <= 0 {
		return "", 0, fmt.Errorf("step %q has no valid duration", s)
	}
	load = strings.TrimSpace(load)
	if load == "capacity" {
		return load, d, nil
	}
	// ParseFloat takes NaN and Inf, which no rate or percent is
	v, err := strconv.ParseFloat(strings.TrimSuffix(load, "%"), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return "", 0, fmt.Errorf("step %q is neither capacity, a rate nor a percent", s)
	}
	return load, d, nil
}

// validateLatency checks a case's latency block.
func (tc *TestCase) validateLatency() error {
	l := tc.Latency
	if l == nil {
		return nil
	}
	// The performance flow and the director cluster scenario read the latency block; the other
	// flows would leave it unread
	if tc.Type != "performance" && tc.Type != ClusterScenarioType {
		return fmt.Errorf("case %q: latency needs a performance case or a director cluster scenario, not %q", tc.Name, tc.Type)
	}
	if tc.Generator.Mode != "http_ack" || len(tc.Generators) > 0 {
		return fmt.Errorf("case %q: latency needs one generator in http_ack mode", tc.Name)
	}
	if len(tc.Receivers) > 0 {
		return fmt.Errorf("case %q: latency needs one receiver", tc.Name)
	}
	// A scenario caps the receiver at 512m unless the case says otherwise, and the receiver keeps
	// every line's latency: 512m holds about 14 million lines, which a latency run passes in minutes
	if tc.Type == ClusterScenarioType && (tc.Cluster == nil || tc.Cluster.Scenario == nil || tc.Cluster.Scenario.HelperMemLimit == "") {
		return fmt.Errorf("case %q: a latency scenario sets cluster.scenario.helper_mem_limit: the receiver keeps every line's latency, and the default 512m holds about 14 million lines", tc.Name)
	}
	if len(l.Steps) == 0 {
		return fmt.Errorf("case %q: latency.steps names no step", tc.Name)
	}
	capacity := false
	for _, s := range l.Steps {
		load, _, err := parseLatencyStep(s)
		if err != nil {
			return fmt.Errorf("case %q: latency.steps: %w", tc.Name, err)
		}
		if load == "capacity" {
			capacity = true
		}
		if strings.HasSuffix(load, "%") && !capacity {
			return fmt.Errorf("case %q: latency step %q takes a share of a capacity that no step before it measures", tc.Name, s)
		}
	}
	if l.StepWarmup != "" {
		if d, err := time.ParseDuration(l.StepWarmup); err != nil || d < 0 {
			return fmt.Errorf("case %q: latency.step_warmup %q is not a duration", tc.Name, l.StepWarmup)
		}
	}
	if l.StageInterval != "" {
		if d, err := time.ParseDuration(l.StageInterval); err != nil || d <= 0 {
			return fmt.Errorf("case %q: latency.stage_interval %q is not a duration", tc.Name, l.StageInterval)
		}
	}
	if l.StepDrain != "" {
		if d, err := time.ParseDuration(l.StepDrain); err != nil || d < 0 {
			return fmt.Errorf("case %q: latency.step_drain %q is not a duration", tc.Name, l.StepDrain)
		}
	}
	switch l.CapacityFrom {
	case "", "receiver", "acknowledgements":
	default:
		return fmt.Errorf("case %q: latency.capacity_from is %q, and can be receiver or acknowledgements", tc.Name, l.CapacityFrom)
	}
	if d := tc.DurationOrDefault(0); d > 0 && d < l.MinDuration() {
		return fmt.Errorf("case %q: duration %s is shorter than the latency steps and the longest waits between them (step_drain), %s",
			tc.Name, d, l.MinDuration())
	}
	return nil
}

// applyLatency hands a case's latency block to its generator and receiver: the generator runs the
// steps and reads the receiver's arrivals, and the receiver records the first-arrival latency of
// every line. A case without a duration lasts as long as its steps and their longest waits.
func (tc *TestCase) applyLatency() {
	l := tc.Latency
	if l == nil {
		return
	}
	if tc.Generator.Env == nil {
		tc.Generator.Env = map[string]string{}
	}
	tc.Generator.Env["GENERATOR_STEPS"] = strings.Join(l.Steps, ",")
	tc.Generator.Env["GENERATOR_STEP_WARMUP"] = l.StepWarmupOrDefault().String()
	tc.Generator.Env["GENERATOR_STEP_DRAIN"] = l.StepDrainOrDefault().String()
	if _, set := tc.Generator.Env["GENERATOR_RECEIVER_METRICS"]; !set {
		tc.Generator.Env["GENERATOR_RECEIVER_METRICS"] = "http://receiver:9090"
	}
	if l.CapacityFrom != "" {
		tc.Generator.Env["GENERATOR_STEP_CAPACITY"] = l.CapacityFrom
	}
	if tc.Receiver.Env == nil {
		tc.Receiver.Env = map[string]string{}
	}
	tc.Receiver.Env["RECEIVER_TRACK_SEQ"] = "true"
	if tc.Duration == "" {
		if d := l.MinDuration(); d > 0 {
			tc.Duration = d.String()
		}
	}
}
