package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"gopkg.in/yaml.v3"
)

// scenarioCase builds a minimal director_cluster_scenario_correctness case.
func scenarioCase(sc *config.ScenarioConfig) *config.TestCase {
	return &config.TestCase{
		Name:      "director_cluster_scenario_render",
		Type:      config.ClusterScenarioType,
		Cluster:   &config.ClusterConfig{Nodes: 3, Scenario: sc},
		Generator: config.GeneratorConfig{Mode: "http_ack", Target: "http://subject-1:9000/,http://subject-2:9000/", Rate: 1000, Connections: 1},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001", Env: map[string]string{"RECEIVER_TRACK_SEQ": "true"}},
		Azure:     &config.AzureConfig{Containers: []string{"witness"}},
	}
}

func renderScenario(t *testing.T, tc *config.TestCase, srcConfig string) (map[string]any, string, string) {
	t.Helper()
	tmp := t.TempDir()
	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "vmetric.yml")
	if err := os.WriteFile(src, []byte(srcConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	subj := config.Subject{
		Name:       "vmetric",
		Image:      "vmetric/director-enterprise",
		Version:    "dev",
		ConfigPath: "/config.yml",
		Entrypoint: []string{"/opt/vmetric/director"},
		Command:    []string{"-config-path", "/config.yml"},
	}
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase:         tc,
		Subject:          subj,
		ConfigName:       "default",
		ConfigSrcPath:    src,
		TmpDir:           tmp,
		GeneratorImage:   "img-gen",
		ReceiverImage:    "img-recv",
		CollectorImage:   "img-coll",
		ReceiverHostPort: 19001,
		CPULimit:         "1",
		MemLimit:         "1g",
		ExtraSubjectEnv:  map[string]string{"WITNESS_CONN": "BlobEndpoint=http://toxiproxy:20000/devstoreaccount1"},
	}
	if err := writeCompose(composePath, cfg); err != nil {
		t.Fatalf("writeCompose: %v", err)
	}
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("the rendered compose file is not YAML: %v\n%s", err, raw)
	}
	return doc, string(raw), tmp
}

func service(t *testing.T, doc map[string]any, name string) map[string]any {
	t.Helper()
	services, _ := doc["services"].(map[string]any)
	svc, ok := services[name].(map[string]any)
	if !ok {
		t.Fatalf("service %q missing; services: %v", name, keys(services))
	}
	return svc
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestClusterScenarioComposeRendersTopology covers what a scenario case adds
// to a cluster's compose file: caps and environment on every node, a spare
// node under a profile with a longer member list, the witness directory and
// its init, Toxiproxy, the one-start-arguments wrapper, a deferred
// generator, and caps on every other container.
func TestClusterScenarioComposeRendersTopology(t *testing.T) {
	sc := &config.ScenarioConfig{
		Steps:      []config.ScenarioStep{{Do: "wait_ready"}},
		Witness:    "dir",
		Proxies:    []config.ScenarioProxy{{Name: "witness", Listen: 20000, Upstream: "azurite:10000"}},
		SpareNodes: 1,
		NodeArgs:   true,
	}
	src := "director:\n  id: {{@.NodeID@}}\nnodes:\n{{@- range .ClusterNodeIDs @}}\n  - name: \"{{@ . @}}\"\n{{@- end @}}\n"
	doc, raw, tmp := renderScenario(t, scenarioCase(sc), src)

	for _, name := range []string{"subject-1", "subject-2", "subject-3"} {
		svc := service(t, doc, name)
		if _, spare := svc["profiles"]; spare {
			t.Fatalf("%s must start with the cluster", name)
		}
		limits := svc["deploy"].(map[string]any)["resources"].(map[string]any)["limits"].(map[string]any)
		if limits["cpus"] != "1" || limits["memory"] != "1g" {
			t.Fatalf("%s limits: %v", name, limits)
		}
		env := svc["environment"].(map[string]any)
		if env["WITNESS_CONN"] == nil || env["BENCH_NODE_ID"] == nil {
			t.Fatalf("%s environment: %v", name, env)
		}
		deps := svc["depends_on"].(map[string]any)
		for _, d := range []string{"witness-init", "azure-init", "toxiproxy"} {
			if deps[d] == nil {
				t.Fatalf("%s must wait for %s: %v", name, d, deps)
			}
		}
		ep := svc["entrypoint"].([]any)
		if ep[0] != "/bin/sh" || !strings.Contains(ep[2].(string), "node-args-$$BENCH_NODE_ID") ||
			!strings.Contains(ep[2].(string), "exec '/opt/vmetric/director' '-config-path' '/config.yml' $$A") {
			t.Fatalf("%s entrypoint wrapper: %v", name, ep)
		}
		vols := svc["volumes"].([]any)
		found := false
		for _, v := range vols {
			if v == "witness-dir:/witness" {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s must mount the witness directory: %v", name, vols)
		}
	}
	spare := service(t, doc, "subject-4")
	if p, _ := spare["profiles"].([]any); len(p) != 1 || p[0] != "standby" {
		t.Fatalf("subject-4 must be a spare node, behind the standby profile: %v", spare["profiles"])
	}
	gen := service(t, doc, "generator")
	if p, _ := gen["profiles"].([]any); len(p) != 1 || p[0] != "load" {
		t.Fatalf("the generator must wait for start_load: %v", gen["profiles"])
	}
	for _, name := range []string{"generator", "receiver", "collector", "witness-init", "toxiproxy", "azurite", "azure-init"} {
		svc := service(t, doc, name)
		if svc["cpus"] != "1" || svc["mem_limit"] != "512m" {
			t.Fatalf("%s must be capped: cpus=%v mem_limit=%v", name, svc["cpus"], svc["mem_limit"])
		}
	}
	if _, ok := doc["volumes"].(map[string]any)["witness-dir"]; !ok {
		t.Fatalf("the witness-dir volume is not declared")
	}
	// witness-init runs as root, so the case's directory reaches its shell
	// as data, not as part of the command.
	wi := service(t, doc, "witness-init")
	if env, _ := wi["environment"].(map[string]any); env["WITNESS_DIR"] != "/witness" {
		t.Fatalf("witness-init must get the directory as WITNESS_DIR: %v", wi["environment"])
	}
	if cmd, _ := wi["command"].([]any); len(cmd) != 1 || strings.Contains(cmd[0].(string), "/witness") {
		t.Fatalf("witness-init's command must not carry the directory itself: %v", wi["command"])
	}
	// Toxiproxy's API can change the cluster's network, so only this host's
	// loopback may reach it.
	if !strings.Contains(raw, `"127.0.0.1:18474:8474"`) {
		t.Fatalf("Toxiproxy's API port must be published on 127.0.0.1 only")
	}

	// Members 1..3 name three nodes; the spare names four.
	for n, want := range map[int]int{1: 3, 3: 3, 4: 4} {
		b, err := os.ReadFile(ClusterNodeConfigPath(tmp, n))
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Count(string(b), "- name:"); got != want {
			t.Fatalf("node %d config names %d nodes, want %d:\n%s", n, got, want, b)
		}
	}
	// A scale_to step rewrites node 1 for four members, in place.
	before, _ := os.Stat(ClusterNodeConfigPath(tmp, 1))
	if err := RenderClusterNodeConfig(filepath.Join(filepath.Dir(tmp), "missing.yml"), ClusterNodeConfigPath(tmp, 1), 1, 4); err == nil {
		t.Fatalf("rendering a missing template must fail")
	}
	_ = before
	toxi, err := os.ReadFile(filepath.Join(tmp, "toxiproxy", "toxiproxy.json"))
	if err != nil || !strings.Contains(string(toxi), `"upstream": "azurite:10000"`) || !strings.Contains(string(toxi), `"listen": "0.0.0.0:20000"`) {
		t.Fatalf("toxiproxy config: %s (%v)", toxi, err)
	}
}

// TestClusterComposeKeepsOtherCasesUnchanged makes sure a plain cluster case
// gets no scenario services and keeps its generator in the initial start.
func TestClusterComposeKeepsOtherCasesUnchanged(t *testing.T) {
	tc := &config.TestCase{
		Name:      "director_cluster_basic",
		Type:      "director_cluster_correctness",
		Cluster:   &config.ClusterConfig{Nodes: 3},
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject-1:9000", Rate: 100},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	doc, raw, _ := renderScenario(t, tc, "director:\n  id: {{@.NodeID@}}\n")
	services, _ := doc["services"].(map[string]any)
	for _, name := range []string{"toxiproxy", "witness-init", "subject-4"} {
		if _, ok := services[name]; ok {
			t.Fatalf("a plain cluster case rendered the scenario service %s", name)
		}
	}
	if strings.Contains(raw, "profiles:") || strings.Contains(raw, "BENCH_NODE_ID") {
		t.Fatalf("a plain cluster case rendered scenario settings:\n%s", raw)
	}
	if svc := service(t, doc, "generator"); svc["cpus"] != nil {
		t.Fatalf("a plain cluster case must not get helper caps")
	}
	// Node caps now render for every cluster case that sets them.
	limits := service(t, doc, "subject-2")["deploy"].(map[string]any)["resources"].(map[string]any)["limits"].(map[string]any)
	if limits["cpus"] != "1" {
		t.Fatalf("cluster node caps: %v", limits)
	}
	if env, _ := service(t, doc, "receiver")["environment"].(map[string]any); env["RECEIVER_TRACK_SEQ"] != nil {
		t.Fatalf("a plain cluster case must not get sequence tracking: %v", env)
	}
}

// TestClusterScenarioComposeTracksSequences checks that every receiver of a
// scenario case tracks sequences, even when the case does not ask for it:
// the driver's loss check reads /seqcheck.
func TestClusterScenarioComposeTracksSequences(t *testing.T) {
	steps := []config.ScenarioStep{{Do: "wait_ready"}}
	src := "director:\n  id: {{@.NodeID@}}\n"

	tc := scenarioCase(&config.ScenarioConfig{Steps: steps})
	tc.Receiver.Env = nil
	doc, _, _ := renderScenario(t, tc, src)
	if env, _ := service(t, doc, "receiver")["environment"].(map[string]any); env["RECEIVER_TRACK_SEQ"] != "true" {
		t.Fatalf("the receiver of a scenario case must track sequences: %v", env)
	}

	tc = scenarioCase(&config.ScenarioConfig{Steps: steps})
	tc.Receiver = config.ReceiverConfig{}
	tc.Receivers = []config.ReceiverConfig{{ID: "a", Mode: "tcp", Listen: ":9001"}, {ID: "b", Mode: "tcp", Listen: ":9002"}}
	doc, _, _ = renderScenario(t, tc, src)
	for _, name := range []string{"receiver-a", "receiver-b"} {
		if env, _ := service(t, doc, name)["environment"].(map[string]any); env["RECEIVER_TRACK_SEQ"] != "true" {
			t.Fatalf("%s of a scenario case must track sequences: %v", name, env)
		}
	}
}

func TestLimitHelperServicesLeavesNodesAndExistingCaps(t *testing.T) {
	in := []byte("services:\n  subject-1:\n    image: a\n  receiver:\n    image: b\n  db:\n    image: c\n    deploy:\n      resources: {}\n")
	out, err := limitHelperServices(in, "0.5", "256m")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]map[string]map[string]any
	if err := yaml.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["services"]["subject-1"]["cpus"] != nil {
		t.Fatalf("a director node must keep its own caps")
	}
	if doc["services"]["receiver"]["cpus"] != "0.5" || doc["services"]["receiver"]["mem_limit"] != "256m" {
		t.Fatalf("receiver caps: %v", doc["services"]["receiver"])
	}
	if doc["services"]["db"]["cpus"] != nil {
		t.Fatalf("a service with a deploy block keeps it")
	}
}
