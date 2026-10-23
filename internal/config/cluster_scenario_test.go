package config

import (
	"strings"
	"testing"
)

func validScenarioCase() *TestCase {
	return &TestCase{
		Name:      "scenario",
		Type:      ClusterScenarioType,
		Generator: GeneratorConfig{Mode: "http_ack", Target: "http://subject-1:9000/", Connections: 1, Rate: 1000},
		Receiver:  ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Cluster: &ClusterConfig{Nodes: 3, Scenario: &ScenarioConfig{
			NodeArgs:   true,
			SpareNodes: 1,
			Steps: []ScenarioStep{
				{Do: "wait_ready", Within: "150s"},
				{Do: "start_load"},
				{Do: "kill", Node: "leader", As: "victim"},
				{Do: "wait_leader", Within: "60s", Record: "election"},
				{Do: "start", Node: "@victim", Recover: "reset"},
				{Do: "scale_to", Nodes: 4},
				{Do: "sleep", For: "5s"},
				{Do: "mark", Label: "end"},
				{Do: "expect_delivery_stops", Within: "60s", For: "20s", Record: "stopped"},
			},
		}},
	}
}

func TestValidateClusterScenarioAcceptsAValidCase(t *testing.T) {
	if err := validScenarioCase().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateClusterScenarioRejects(t *testing.T) {
	cases := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"unknown action":          {func(tc *TestCase) { tc.Cluster.Scenario.Steps[0].Do = "explode" }, "unknown action"},
		"forgotten node name":     {func(tc *TestCase) { tc.Cluster.Scenario.Steps[4].Node = "@ghost" }, "was not remembered"},
		"node out of range":       {func(tc *TestCase) { tc.Cluster.Scenario.Steps[2].Node = "9" }, "node must be"},
		"recover without wrapper": {func(tc *TestCase) { tc.Cluster.Scenario.NodeArgs = false }, "node_args"},
		"bad recover":             {func(tc *TestCase) { tc.Cluster.Scenario.Steps[4].Recover = "maybe" }, "recover must be"},
		"bad duration":            {func(tc *TestCase) { tc.Cluster.Scenario.Steps[0].Within = "soon" }, "within"},
		"scale past spares":       {func(tc *TestCase) { tc.Cluster.Scenario.Steps[5].Nodes = 5 }, "nodes must be between"},
		"sleep without for":       {func(tc *TestCase) { tc.Cluster.Scenario.Steps[6].For = "" }, "needs for"},
		"wrong generator":         {func(tc *TestCase) { tc.Generator.Mode = "tcp" }, "http_ack"},
		"too much load":           {func(tc *TestCase) { tc.Generator.Connections = 3 }, "lines a second"},
		"implicit workers":        {func(tc *TestCase) { tc.Generator.Connections = 0 }, "explicitly"},
		"no steps":                {func(tc *TestCase) { tc.Cluster.Scenario.Steps = nil }, "requires cluster.scenario.steps"},
		"witness without store":   {func(tc *TestCase) { tc.Cluster.Scenario.Witness = "azure" }, "azure:"},
		"undeclared proxy": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps, ScenarioStep{Do: "proxy_latency", Proxy: "witness", Latency: "200ms"})
		}, "not declared"},
		"action from the other driver": {func(tc *TestCase) { tc.Cluster.Action = "restart_leader" }, "belong to director_cluster_correctness"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := validScenarioCase()
			c.edit(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func TestScenarioBlockOnlyForScenarioType(t *testing.T) {
	tc := validScenarioCase()
	tc.Type = "director_cluster_correctness"
	tc.Generator.Mode = "tcp"
	if err := tc.Validate(); err == nil || !strings.Contains(err.Error(), "only valid for type "+ClusterScenarioType) {
		t.Fatalf("Validate() = %v", err)
	}
}

func TestScenarioDefaults(t *testing.T) {
	var sc *ScenarioConfig
	if sc.ProbeIntervalOrDefault().Seconds() != 1 || sc.WitnessDirOrDefault() != "/witness" || sc.ToxiproxyHostPortOrDefault() != 18474 {
		t.Fatalf("nil scenario defaults are wrong")
	}
	cpu, mem := sc.HelperLimits()
	if cpu != "1" || mem != "512m" {
		t.Fatalf("helper limits %s %s", cpu, mem)
	}
	if !strings.Contains(sc.ToxiproxyImageOrDefault(), ":2.") {
		t.Fatalf("the Toxiproxy image must be pinned to a release")
	}
}
