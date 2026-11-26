package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeCase writes body as the case.yaml of case name under a new cases folder, and returns it.
func writeCase(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name, "case.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

const latencyCase = `name: latency_case
type: performance
generator:
  mode: http_ack
  target: "http://subject:9000/"
  connections: 8
receiver:
  mode: tcp
  listen: ":9001"
latency:
  steps: ["capacity:60s", "1000:120s", "50%:120s"]
  disk_baseline: true
  stages: true
subjects:
  - vmetric
`

// A latency block hands its steps to the generator, turns on the receiver's per-line tracking,
// and gives a case without a duration the steps' duration.
func TestALatencyBlockSetsUpTheGeneratorAndTheReceiver(t *testing.T) {
	tc, err := LoadCase(writeCase(t, "latency_case", latencyCase), "latency_case")
	if err != nil {
		t.Fatalf("LoadCase: %v", err)
	}
	if got := tc.Generator.Env["GENERATOR_STEPS"]; got != "capacity:60s,1000:120s,50%:120s" {
		t.Errorf("GENERATOR_STEPS = %q", got)
	}
	if got := tc.Generator.Env["GENERATOR_STEP_WARMUP"]; got != "20s" {
		t.Errorf("GENERATOR_STEP_WARMUP = %q, want the default 20s", got)
	}
	if got := tc.Receiver.Env["RECEIVER_TRACK_SEQ"]; got != "true" {
		t.Errorf("RECEIVER_TRACK_SEQ = %q, want true", got)
	}
	// The steps last 5 minutes, and each step but the last waits up to a minute for the receiver
	if got := tc.DurationOrDefault(0); got != 420*time.Second {
		t.Errorf("duration = %s, want the steps' 5m0s and two waits of 1m0s", got)
	}
	if got := tc.Generator.Env["GENERATOR_RECEIVER_METRICS"]; got != "http://receiver:9090" {
		t.Errorf("GENERATOR_RECEIVER_METRICS = %q", got)
	}
	if got := tc.Generator.Env["GENERATOR_STEP_DRAIN"]; got != "1m0s" {
		t.Errorf("GENERATOR_STEP_DRAIN = %q", got)
	}
	if _, set := tc.Generator.Env["GENERATOR_STEP_CAPACITY"]; set {
		t.Errorf("GENERATOR_STEP_CAPACITY is set without capacity_from: %q", tc.Generator.Env["GENERATOR_STEP_CAPACITY"])
	}
	if got := tc.Latency.StageIntervalOrDefault(); got != 5*time.Second {
		t.Errorf("stage interval = %s, want the default 5s", got)
	}
}

func TestALatencyBlockThatCannotRunIsRefused(t *testing.T) {
	cases := map[string]struct{ from, to, want string }{
		"a percent before any capacity step": {
			`["capacity:60s", "1000:120s", "50%:120s"]`, `["50%:120s", "capacity:60s"]`, "no step before it measures",
		},
		"a step without a duration": {
			`["capacity:60s", "1000:120s", "50%:120s"]`, `["capacity"]`, "has no duration",
		},
		"a step that is no load": {
			`["capacity:60s", "1000:120s", "50%:120s"]`, `["fast:60s"]`, "neither capacity, a rate nor a percent",
		},
		"a rate that is no number": {
			`["capacity:60s", "1000:120s", "50%:120s"]`, `["NaN:60s"]`, "neither capacity, a rate nor a percent",
		},
		"an infinite rate": {
			`["capacity:60s", "1000:120s", "50%:120s"]`, `["+Inf:60s"]`, "neither capacity, a rate nor a percent",
		},
		"a percent that is no number": {
			`["capacity:60s", "1000:120s", "50%:120s"]`, `["capacity:60s", "NaN%:120s"]`, "neither capacity, a rate nor a percent",
		},
		"a duration that leaves the steps no time to drain": {
			"type: performance", "type: performance\nduration: 300s", "the longest waits between them",
		},
		"another generator mode": {
			"mode: http_ack", "mode: tcp", "http_ack",
		},
		"a correctness case": {
			"type: performance", "type: correctness", "a performance case",
		},
		"an unknown capacity source": {
			"  stages: true\n", "  stages: true\n  capacity_from: confirmations\n", "can be receiver or acknowledgements",
		},
		"a step drain that is no duration": {
			"  stages: true\n", "  stages: true\n  step_drain: soon\n", "step_drain",
		},
		"a duration shorter than the steps": {
			"type: performance", "type: performance\nduration: 60s", "shorter than the latency steps",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			body := strings.Replace(latencyCase, c.from, c.to, 1)
			if body == latencyCase {
				t.Fatalf("the test's replacement %q is not in the case", c.from)
			}
			_, err := LoadCase(writeCase(t, "latency_case", body), "latency_case")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("LoadCase error = %v, want one that says %q", err, c.want)
			}
		})
	}
}

// A case without a latency block is untouched.
func TestACaseWithoutALatencyBlockKeepsItsEnvironment(t *testing.T) {
	body := strings.Replace(latencyCase, "latency:\n  steps: [\"capacity:60s\", \"1000:120s\", \"50%:120s\"]\n  disk_baseline: true\n  stages: true\n", "", 1)
	if body == latencyCase {
		t.Fatal("the test did not remove the latency block")
	}
	tc, err := LoadCase(writeCase(t, "latency_case", body), "latency_case")
	if err != nil {
		t.Fatalf("LoadCase: %v", err)
	}
	if tc.Latency != nil || tc.Generator.Env["GENERATOR_STEPS"] != "" || tc.Receiver.Env["RECEIVER_TRACK_SEQ"] != "" {
		t.Fatalf("a case without latency got latency settings: %+v %v %v", tc.Latency, tc.Generator.Env, tc.Receiver.Env)
	}
}

const latencyScenarioCase = `name: latency_scenario
type: director_cluster_scenario_correctness
cluster:
  nodes: 3
  scenario:
    steps:
      - do: wait_ready
        within: 180s
      - do: start_load
    expect:
      max_loss: 0
    helper_mem_limit: 8g
generator:
  mode: http_ack
  target: "http://subject-1:9000/"
  connections: 32
receiver:
  mode: tcp
  listen: ":9001"
latency:
  steps: ["capacity:60s", "50%:120s"]
  stages: true
subjects:
  - vmetric
`

// A director cluster scenario takes its load from the latency steps, so it needs no rate. Without
// the latency block, it still does.
func TestAClusterScenarioTakesItsLoadFromTheLatencySteps(t *testing.T) {
	tc, err := LoadCase(writeCase(t, "latency_scenario", latencyScenarioCase), "latency_scenario")
	if err != nil {
		t.Fatalf("LoadCase: %v", err)
	}
	if got := tc.Generator.Env["GENERATOR_STEPS"]; got != "capacity:60s,50%:120s" {
		t.Errorf("GENERATOR_STEPS = %q", got)
	}
	body := strings.Replace(latencyScenarioCase, "latency:\n  steps: [\"capacity:60s\", \"50%:120s\"]\n  stages: true\n", "", 1)
	if body == latencyScenarioCase {
		t.Fatal("the test did not remove the latency block")
	}
	if _, err := LoadCase(writeCase(t, "latency_scenario", body), "latency_scenario"); err == nil ||
		!strings.Contains(err.Error(), "generator.rate explicitly") {
		t.Fatalf("a scenario without a rate or a latency block loaded: %v", err)
	}
}

// A latency scenario must lift the receiver's memory cap, or the receiver runs out of memory
// part-way through the run and every later step loses its end-to-end latency.
func TestALatencyScenarioSetsTheHelperMemoryLimit(t *testing.T) {
	body := strings.Replace(latencyScenarioCase, "    helper_mem_limit: 8g\n", "", 1)
	if body == latencyScenarioCase {
		t.Fatal("the test did not remove helper_mem_limit")
	}
	if _, err := LoadCase(writeCase(t, "latency_scenario", body), "latency_scenario"); err == nil ||
		!strings.Contains(err.Error(), "helper_mem_limit") {
		t.Fatalf("a latency scenario with the default helper memory loaded: %v", err)
	}
}

// capacity_from: acknowledgements hands the generator its choice.
func TestCapacityFromAcknowledgementsReachesTheGenerator(t *testing.T) {
	body := strings.Replace(latencyCase, "  stages: true\n", "  stages: true\n  capacity_from: acknowledgements\n  step_drain: 30s\n", 1)
	tc, err := LoadCase(writeCase(t, "latency_case", body), "latency_case")
	if err != nil {
		t.Fatalf("LoadCase: %v", err)
	}
	if got := tc.Generator.Env["GENERATOR_STEP_CAPACITY"]; got != "acknowledgements" {
		t.Errorf("GENERATOR_STEP_CAPACITY = %q", got)
	}
	if got := tc.DurationOrDefault(0); got != 360*time.Second {
		t.Errorf("duration = %s, want the steps' 5m0s and two waits of 30s", got)
	}
}
