package orchestrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"gopkg.in/yaml.v3"
)

// nodeIDs returns 1..n.
func nodeIDs(n int) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i + 1
	}
	return ids
}

// RenderClusterNodeConfig renders a cluster case's config template for one
// node, naming nodes 1..size as the cluster's members. It writes outPath in
// place (truncate and write) so a running node's bind mount keeps seeing the
// file: a rename would leave the container on the old inode. A scenario's
// scale_to step calls it to grow or shrink the cluster.
func RenderClusterNodeConfig(srcPath, outPath string, nodeID, size int) error {
	return renderConfigToFile(srcPath, outPath, configTemplateContext{
		CPUs:           runtime.NumCPU(),
		NodeID:         nodeID,
		ClusterNodeIDs: nodeIDs(size),
		T0:             time.Now().UTC(),
		Stage:          1,
	})
}

// ClusterNodeConfigPath is where writeCompose renders node n's config.
func ClusterNodeConfigPath(tmpDir string, n int) string {
	return replicaConfigPath(tmpDir, n)
}

// NodeArgsPath is the file a node's start wrapper reads its extra command
// line arguments from (cluster.scenario.node_args). The driver writes it
// before a start and removes it once the node has started.
func NodeArgsPath(tmpDir string, n int) string {
	return filepath.Join(tmpDir, fmt.Sprintf("node-args-%d", n))
}

// applyClusterScenarioTopology fills the compose variables a
// director_cluster_scenario_correctness case needs. Other cases are left
// as they were.
func applyClusterScenarioTopology(vars *composeVars, cfg RunConfig) error {
	tc := cfg.TestCase
	if tc.Cluster == nil || tc.Cluster.Scenario == nil {
		return nil
	}
	sc := tc.Cluster.Scenario
	s := cfg.Subject

	// The load starts once the cluster is ready, at the start_load step.
	vars.GenDeferred = true

	// The driver checks every acknowledged line against the receiver's
	// sequence tracking (/seqcheck). Without it the loss of the run is
	// unknown, so it is on for every receiver of a scenario case.
	trackSeq := map[string]string{"RECEIVER_TRACK_SEQ": "true"}
	vars.RecvEnv = mergeEnv(vars.RecvEnv, trackSeq)
	for i := range vars.Receivers {
		vars.Receivers[i].Env = mergeEnv(vars.Receivers[i].Env, trackSeq)
	}

	// Every cluster node already waits for azure-init when the case uses
	// Azurite (the node block's AzureEnabled entry).
	if tc.UsesMinio() {
		vars.NodeDependsOn = append(vars.NodeDependsOn, nodeDependency{"minio-init", "service_completed_successfully"})
	}
	if sc.Witness == "dir" {
		vars.WitnessDirMount = sc.WitnessDirOrDefault()
		vars.NodeDependsOn = append(vars.NodeDependsOn, nodeDependency{"witness-init", "service_completed_successfully"})
	}

	if len(sc.Proxies) > 0 {
		dir := filepath.Join(cfg.TmpDir, "toxiproxy")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		if err := writeToxiproxyConfig(filepath.Join(dir, "toxiproxy.json"), sc.Proxies); err != nil {
			return err
		}
		vars.ToxiproxyEnabled = true
		vars.ToxiproxyImage = sc.ToxiproxyImageOrDefault()
		vars.ToxiproxyConfigHost = filepath.ToSlash(dir)
		vars.ToxiproxyHostPort = sc.ToxiproxyHostPortOrDefault()
		vars.NodeDependsOn = append(vars.NodeDependsOn, nodeDependency{"toxiproxy", "service_started"})
	}

	if sc.NodeArgs {
		vars.NodeArgsEntrypoint = nodeArgsEntrypoint(s.Entrypoint, s.Command)
	}
	return nil
}

// writeToxiproxyConfig writes Toxiproxy's start-up proxy list.
func writeToxiproxyConfig(path string, proxies []config.ScenarioProxy) error {
	type proxy struct {
		Name     string `json:"name"`
		Listen   string `json:"listen"`
		Upstream string `json:"upstream"`
		Enabled  bool   `json:"enabled"`
	}
	out := make([]proxy, 0, len(proxies))
	for _, p := range proxies {
		out = append(out, proxy{Name: p.Name, Listen: fmt.Sprintf("0.0.0.0:%d", p.Listen), Upstream: p.Upstream, Enabled: true})
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// nodeArgsEntrypoint wraps the subject's entrypoint and command in a shell
// that appends the contents of /results/node-args-$BENCH_NODE_ID, when the
// file exists. exec keeps the director as the container's main process, so
// stop and kill signals still reach it.
func nodeArgsEntrypoint(entrypoint, command []string) string {
	parts := make([]string, 0, len(entrypoint)+len(command))
	for _, p := range append(append([]string{}, entrypoint...), command...) {
		parts = append(parts, shellQuote(p))
	}
	script := `A=""; F="/results/node-args-$BENCH_NODE_ID"; if [ -f "$F" ]; then A="$(cat "$F")"; echo "bench: starting with $A" >&2; fi; exec ` +
		strings.Join(parts, " ") + ` $A`
	// Compose interpolates "$"; "$$" passes a literal one to the shell.
	script = strings.ReplaceAll(script, "$", "$$")
	return formatYAMLList([]string{"/bin/sh", "-c", script})
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// limitHelperServices gives every service that is not a director node a CPU
// and memory cap, unless it already has a deploy block or caps of its own.
// Director nodes (the services named subject or subject-<n>) keep the
// limits the case set for them.
func limitHelperServices(composeYAML []byte, cpu, mem string) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(composeYAML, &doc); err != nil {
		return nil, fmt.Errorf("parsing rendered compose file: %w", err)
	}
	if len(doc.Content) == 0 {
		return composeYAML, nil
	}
	root := doc.Content[0]
	services := mappingValue(root, "services")
	if services == nil || services.Kind != yaml.MappingNode {
		return composeYAML, nil
	}
	for i := 0; i+1 < len(services.Content); i += 2 {
		name := services.Content[i].Value
		svc := services.Content[i+1]
		if svc.Kind != yaml.MappingNode || name == "subject" || strings.HasPrefix(name, "subject-") {
			continue
		}
		if mappingValue(svc, "deploy") != nil || mappingValue(svc, "cpus") != nil || mappingValue(svc, "mem_limit") != nil {
			continue
		}
		svc.Content = append(svc.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "cpus"},
			&yaml.Node{Kind: yaml.ScalarNode, Value: cpu, Style: yaml.DoubleQuotedStyle},
			&yaml.Node{Kind: yaml.ScalarNode, Value: "mem_limit"},
			&yaml.Node{Kind: yaml.ScalarNode, Value: mem, Style: yaml.DoubleQuotedStyle},
		)
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func mappingValue(m *yaml.Node, key string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}
