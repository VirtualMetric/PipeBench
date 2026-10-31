package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// throughputScenarioCase is a valid throughput case: a load far above
// MaxScenarioRate, three windows, and a comparison of the last two with the
// first.
func throughputScenarioCase() *TestCase {
	return &TestCase{
		Name:      "throughput",
		Type:      ClusterScenarioType,
		Generator: GeneratorConfig{Mode: "http_ack", Target: "http://subject-1:9000/", Connections: 48, Rate: 100000},
		Receiver:  ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Cluster: &ClusterConfig{Nodes: 3, Scenario: &ScenarioConfig{
			Throughput: true,
			SpareNodes: 3,
			Steps: []ScenarioStep{
				{Do: "wait_ready", Within: "150s"},
				{Do: "start_load"},
				{Do: "mark", Label: "three"},
				{Do: "sleep", For: "60s"},
				{Do: "scale_to", Nodes: 6},
				{Do: "mark", Label: "six"},
				{Do: "sleep", For: "60s"},
				{Do: "mark", Label: "end"},
			},
			Expect: ScenarioExpect{
				Throughput: []ScenarioThroughputExpect{{Window: "six", Over: "three", MinRatio: 1.7}},
			},
		}},
	}
}

func TestValidateClusterScenarioAcceptsAThroughputCase(t *testing.T) {
	if err := throughputScenarioCase().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateClusterScenarioRejectsBadThroughputCases(t *testing.T) {
	cases := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"a load above the cap without throughput": {func(tc *TestCase) {
			tc.Cluster.Scenario.Throughput = false
			tc.Cluster.Scenario.Expect.Throughput = nil
		}, "lines a second"},
		"an expectation without throughput": {func(tc *TestCase) {
			tc.Cluster.Scenario.Throughput = false
			tc.Generator.Rate = 10
		}, "needs cluster.scenario.throughput: true"},
		"no window": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Throughput[0].Window = ""
		}, "needs window and over"},
		"a window compared with itself": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Throughput[0].Over = "six"
		}, "with itself"},
		"an unknown mark": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Throughput[0].Over = "two"
		}, "which no step sets"},
		"the last mark": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Throughput[0].Window = "end"
		}, "the last mark"},
		"no ratio": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Throughput[0].MinRatio = 0
		}, "min_ratio must be more than 0"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := throughputScenarioCase()
			c.edit(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func TestScenarioThroughputParses(t *testing.T) {
	var sc ScenarioConfig
	src := `
throughput: true
expect:
  throughput:
    - window: six
      over: three
      min_ratio: 1.7
`
	if err := yaml.Unmarshal([]byte(src), &sc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !sc.Throughput {
		t.Fatal("throughput did not parse")
	}
	want := ScenarioThroughputExpect{Window: "six", Over: "three", MinRatio: 1.7}
	if len(sc.Expect.Throughput) != 1 || sc.Expect.Throughput[0] != want {
		t.Fatalf("expect.throughput = %+v, want [%+v]", sc.Expect.Throughput, want)
	}
}
