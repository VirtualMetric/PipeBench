package config

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ClusterScenarioType is the case type whose driver runs a director cluster
// through a list of steps (cluster.scenario): kill or pause nodes, cut them
// off the network, take the witness away, slow the store down, add or
// remove a node. It measures what the cluster does at each step and checks
// at the end that no acknowledged record was lost.
const ClusterScenarioType = "director_cluster_scenario_correctness"

// MaxScenarioRate caps a scenario case's load, in lines a second. The cases
// test failover, not throughput, and run on shared machines.
const MaxScenarioRate = 2000

// IsClusterScenarioType reports whether the case is a cluster scenario case.
func (tc *TestCase) IsClusterScenarioType() bool {
	return tc.Type == ClusterScenarioType
}

// ScenarioConfig is the `cluster.scenario` block.
type ScenarioConfig struct {
	// Steps run in order. The load (the http_ack generator) starts at the
	// start_load step, or right after the cluster is ready when no step
	// starts it.
	Steps []ScenarioStep `yaml:"steps"`

	// Expect holds the end-of-run verdict thresholds.
	Expect ScenarioExpect `yaml:"expect"`

	// ProbeInterval is how often the harness reads every node's /health
	// (default 1s). The leader timeline and the two-leader check come from
	// these reads.
	ProbeInterval string `yaml:"probe_interval"`

	// Witness wires a witness store into every node. "dir" mounts a shared
	// volume at WitnessDir (default /witness), writable by the director.
	// "azure" and "s3" only add the start-up order: the case declares the
	// `azure:` or `minio:` block and names the store in its config. Empty
	// means no witness.
	Witness    string `yaml:"witness"`
	WitnessDir string `yaml:"witness_dir"`

	// Proxies put Toxiproxy between the nodes and a service, so a step can
	// slow the connection down or cut it (the witness store, for example).
	// The node config then names "toxiproxy:<listen>" instead of the service.
	Proxies []ScenarioProxy `yaml:"proxies"`

	// ToxiproxyImage overrides the Toxiproxy image.
	ToxiproxyImage string `yaml:"toxiproxy_image"`

	// ToxiproxyHostPort is the host port of Toxiproxy's API (default 18474).
	ToxiproxyHostPort int `yaml:"toxiproxy_host_port"`

	// SpareNodes adds node services that do not start with the cluster. A
	// scale_to step starts them. Their configs, and the configs a scale_to
	// step writes, list every node up to the new size.
	SpareNodes int `yaml:"spare_nodes"`

	// NodeArgs starts every node through a small shell wrapper that adds the
	// arguments in /results/node-args-<n> to the director's command line, so
	// a start step can pass -recover for one start only.
	NodeArgs bool `yaml:"node_args"`

	// HelperCPULimit and HelperMemLimit cap every container that is not a
	// director node: generator, receiver, collector, emulators, Toxiproxy and
	// the one-shot init containers. Defaults "1" and "512m".
	HelperCPULimit string `yaml:"helper_cpu_limit"`
	HelperMemLimit string `yaml:"helper_mem_limit"`

	// DrainSeconds bounds the wait, after the load stops, for every
	// acknowledged line to arrive (default 180).
	DrainSeconds int `yaml:"drain_seconds"`
}

// ScenarioProxy is one Toxiproxy proxy.
type ScenarioProxy struct {
	Name     string `yaml:"name"`
	Listen   int    `yaml:"listen"`
	Upstream string `yaml:"upstream"`
}

// ScenarioExpect is the end-of-run verdict. A nil pointer means "report,
// do not judge".
type ScenarioExpect struct {
	// MaxLoss is how many acknowledged lines may be missing at the receiver.
	MaxLoss *int64 `yaml:"max_loss"`
	// MaxDuplicates is how many lines may arrive more than once.
	MaxDuplicates *int64 `yaml:"max_duplicates"`
	// MaxSplitBrainMs is how long two nodes may both name themselves the
	// cluster's leader in the same round of /health reads.
	MaxSplitBrainMs *int64 `yaml:"max_split_brain_ms"`
	// MinAcked is the fewest lines the generator must have had
	// acknowledged, so a run that sent nothing cannot pass.
	MinAcked int64 `yaml:"min_acked"`
}

// ScenarioStep is one step. Do names the action; the other fields are its
// parameters, and each action reads only the ones it needs.
type ScenarioStep struct {
	Do string `yaml:"do"`

	// Node selects the node: "leader" (the current leader), "follower" (the
	// lowest-numbered running node that is not the leader), "owner" (the node
	// the leader last placed a placement-managed device on), a number, or
	// "@name" for a node an earlier step remembered with As.
	Node string `yaml:"node"`
	// As remembers the node this step acted on, for a later "@name".
	As string `yaml:"as"`

	// Within bounds a wait; For is how long an expectation must hold, or a
	// sleep's length.
	Within string `yaml:"within"`
	For    string `yaml:"for"`

	// Record names the measurement a wait stores: the time from the last
	// disruption to the moment the wait was satisfied.
	Record string `yaml:"record"`
	// Label names a mark.
	Label string `yaml:"label"`

	// Signal is the signal a kill sends (default SIGKILL).
	Signal string `yaml:"signal"`
	// Grace is the stop timeout for stop and restart (default 30s).
	Grace string `yaml:"grace"`

	// Recover passes -recover=<value> to a start (accept-loss or reset).
	// Needs scenario.node_args.
	Recover string `yaml:"recover"`

	// Proxy names a Toxiproxy proxy; Latency and Jitter set its latency
	// (0 removes it).
	Proxy   string `yaml:"proxy"`
	Latency string `yaml:"latency"`
	Jitter  string `yaml:"jitter"`

	// Nodes is the cluster size a scale_to step moves to.
	Nodes int `yaml:"nodes"`

	// Log is the text wait_log waits for.
	Log string `yaml:"log"`

	// Soft makes a failed expectation a note instead of a failure.
	Soft bool `yaml:"soft"`
}

// scenarioActions lists every step action and whether it needs Node.
var scenarioActions = map[string]bool{
	"wait_ready":             false,
	"start_load":             false,
	"sleep":                  false,
	"mark":                   false,
	"kill":                   true,
	"stop":                   true,
	"start":                  true,
	"restart":                true,
	"pause":                  true,
	"unpause":                true,
	"disconnect":             true,
	"connect":                true,
	"wait_leader":            false,
	"expect_no_leader":       false,
	"wait_delivery":          false,
	"expect_writes_stop":     false,
	"expect_writes_continue": false,
	"expect_delivery_stops":  false,
	"wait_healthy":           false,
	"witness_down":           false,
	"witness_up":             false,
	"proxy_latency":          false,
	"proxy_down":             false,
	"proxy_up":               false,
	"receiver_pause":         false,
	"receiver_unpause":       false,
	"scale_to":               false,
	"rolling_restart":        false,
	"wait_log":               false,
	"expect_no_log":          true,
	"wait_stable":            false,
}

// nodeActionSteps are the actions that act on a node, so their `as` can
// remember it. The driver records a name only for these.
var nodeActionSteps = map[string]bool{
	"kill": true, "stop": true, "start": true, "restart": true,
	"pause": true, "unpause": true, "disconnect": true, "connect": true,
}

// ScenarioActionNeedsNode reports whether the action takes a node.
func ScenarioActionNeedsNode(do string) bool { return scenarioActions[do] }

// DurationOr parses s, or returns def when s is empty.
func (s ScenarioStep) DurationOr(field string, def time.Duration) time.Duration {
	v := s.Within
	if field == "for" {
		v = s.For
	} else if field == "grace" {
		v = s.Grace
	}
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// minProbeInterval is the shortest cluster.scenario.probe_interval a case
// may set.
const minProbeInterval = 200 * time.Millisecond

// ProbeIntervalOrDefault returns the /health read interval.
func (sc *ScenarioConfig) ProbeIntervalOrDefault() time.Duration {
	if sc == nil || sc.ProbeInterval == "" {
		return time.Second
	}
	if d, err := time.ParseDuration(sc.ProbeInterval); err == nil && d >= minProbeInterval {
		return d
	}
	return time.Second
}

// WitnessDirOrDefault returns the mount point of the directory witness.
func (sc *ScenarioConfig) WitnessDirOrDefault() string {
	if sc == nil || sc.WitnessDir == "" {
		return "/witness"
	}
	return sc.WitnessDir
}

// ToxiproxyImageOrDefault returns the Toxiproxy image, pinned to a release.
func (sc *ScenarioConfig) ToxiproxyImageOrDefault() string {
	if sc != nil && sc.ToxiproxyImage != "" {
		return sc.ToxiproxyImage
	}
	return "ghcr.io/shopify/toxiproxy:2.9.0"
}

// ToxiproxyHostPortOrDefault returns the host port of Toxiproxy's API.
func (sc *ScenarioConfig) ToxiproxyHostPortOrDefault() int {
	if sc != nil && sc.ToxiproxyHostPort > 0 {
		return sc.ToxiproxyHostPort
	}
	return 18474
}

// HelperLimits returns the CPU and memory caps for non-node containers.
func (sc *ScenarioConfig) HelperLimits() (cpu, mem string) {
	cpu, mem = "1", "512m"
	if sc != nil && sc.HelperCPULimit != "" {
		cpu = sc.HelperCPULimit
	}
	if sc != nil && sc.HelperMemLimit != "" {
		mem = sc.HelperMemLimit
	}
	return cpu, mem
}

// DrainOrDefault returns how long to wait for every acknowledged line.
func (sc *ScenarioConfig) DrainOrDefault() time.Duration {
	if sc == nil || sc.DrainSeconds <= 0 {
		return 180 * time.Second
	}
	return time.Duration(sc.DrainSeconds) * time.Second
}

// witnessDirPattern is the form a witness directory must take: an absolute
// path of letters, digits, '.', '_', '-' and '/'. The path goes into every
// node's volume entry and into witness-init, which runs as root.
var witnessDirPattern = regexp.MustCompile(`^/[A-Za-z0-9._/-]+$`)

// checkWitnessDir says what is wrong with a cluster.scenario.witness_dir, or
// returns nil.
func checkWitnessDir(dir string) error {
	if !witnessDirPattern.MatchString(dir) {
		return fmt.Errorf("must be an absolute path made only of letters, digits, '.', '_', '-' and '/', with no spaces, ':' or shell characters")
	}
	for _, part := range strings.Split(dir, "/") {
		if part == ".." {
			return fmt.Errorf("must not contain '..'")
		}
	}
	if strings.Trim(dir, "/.") == "" {
		return fmt.Errorf("must name a directory below /")
	}
	return nil
}

// validateClusterScenario checks a director_cluster_scenario_correctness case.
func (tc *TestCase) validateClusterScenario() error {
	if tc.Cluster == nil {
		return fmt.Errorf("case %q: type %s requires a `cluster:` block", tc.Name, ClusterScenarioType)
	}
	if tc.Cluster.Nodes < 1 {
		return fmt.Errorf("case %q: cluster.nodes must be >= 1, got %d", tc.Name, tc.Cluster.Nodes)
	}
	if tc.Cluster.Action != "" || tc.Cluster.IP != "" {
		return fmt.Errorf("case %q: cluster.action and cluster.ip belong to director_cluster_correctness; a scenario case uses cluster.scenario.steps", tc.Name)
	}
	sc := tc.Cluster.Scenario
	if sc == nil || len(sc.Steps) == 0 {
		return fmt.Errorf("case %q: type %s requires cluster.scenario.steps", tc.Name, ClusterScenarioType)
	}
	switch {
	case !tc.HasGenerator():
		// A case whose data comes from a device the cluster collects itself
		// (a database poller, for example) runs without the http_ack load.
		// It has no acknowledged lines, so it cannot judge loss.
		if sc.Expect.MaxLoss != nil || sc.Expect.MinAcked > 0 {
			return fmt.Errorf("case %q: expect.max_loss and expect.min_acked need the http_ack generator", tc.Name)
		}
	case tc.Generator.Mode != "http_ack":
		return fmt.Errorf("case %q: a scenario case counts acknowledged lines, so its generator must use mode http_ack (got %q)", tc.Name, tc.Generator.Mode)
	// The rate is per worker, and an unset worker count means three per CPU
	// (ResolveGeneratorConnections), so a scenario case states both and stays
	// at a load a shared machine can carry.
	case tc.Generator.Connections < 1 || tc.Generator.Rate < 1:
		return fmt.Errorf("case %q: a scenario case sets generator.connections and generator.rate explicitly", tc.Name)
	}
	if total := tc.Generator.Connections * tc.Generator.Rate; tc.HasGenerator() && total > MaxScenarioRate {
		return fmt.Errorf("case %q: generator.connections × generator.rate is %d lines a second, more than the %d a scenario case may send", tc.Name, total, MaxScenarioRate)
	}
	switch sc.Witness {
	case "", "dir", "azure", "s3":
	default:
		return fmt.Errorf("case %q: cluster.scenario.witness must be dir, azure or s3, got %q", tc.Name, sc.Witness)
	}
	if sc.WitnessDir != "" {
		if err := checkWitnessDir(sc.WitnessDir); err != nil {
			return fmt.Errorf("case %q: cluster.scenario.witness_dir %q %v", tc.Name, sc.WitnessDir, err)
		}
	}
	if sc.Witness == "azure" && !tc.UsesAzure() {
		return fmt.Errorf("case %q: cluster.scenario.witness azure needs the case's `azure:` block", tc.Name)
	}
	if sc.Witness == "s3" && !tc.UsesMinio() {
		return fmt.Errorf("case %q: cluster.scenario.witness s3 needs the case's `minio:` block", tc.Name)
	}
	if sc.SpareNodes < 0 {
		return fmt.Errorf("case %q: cluster.scenario.spare_nodes must be >= 0", tc.Name)
	}
	proxies := map[string]bool{}
	for _, p := range sc.Proxies {
		if p.Name == "" || p.Listen <= 0 || p.Upstream == "" {
			return fmt.Errorf("case %q: every cluster.scenario.proxies entry needs name, listen and upstream", tc.Name)
		}
		if proxies[p.Name] {
			return fmt.Errorf("case %q: proxy %q is declared twice", tc.Name, p.Name)
		}
		proxies[p.Name] = true
	}
	if sc.ProbeInterval != "" {
		d, err := time.ParseDuration(sc.ProbeInterval)
		if err != nil {
			return fmt.Errorf("case %q: cluster.scenario.probe_interval: %w", tc.Name, err)
		}
		// Each round runs `docker exec` on every node; faster than this,
		// rounds overlap and the probe slows the nodes it measures.
		if d < minProbeInterval {
			return fmt.Errorf("case %q: cluster.scenario.probe_interval must be at least %s, got %s", tc.Name, minProbeInterval, sc.ProbeInterval)
		}
	}

	maxNodes := tc.Cluster.Nodes + sc.SpareNodes
	names := map[string]bool{}
	for i, st := range sc.Steps {
		where := fmt.Sprintf("case %q: cluster.scenario.steps[%d] (%s)", tc.Name, i, st.Do)
		needsNode, known := scenarioActions[st.Do]
		if !known {
			return fmt.Errorf("case %q: cluster.scenario.steps[%d]: unknown action %q", tc.Name, i, st.Do)
		}
		// A step that acts on a node must name it. A wait that may name one
		// (wait_leader, wait_log) must name it the same way when it does.
		if needsNode || st.Node != "" {
			if err := validateNodeSelector(st.Node, maxNodes, names); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
		}
		if st.As != "" {
			if !nodeActionSteps[st.Do] {
				return fmt.Errorf("%s: as remembers the node a step acts on, and %s acts on none", where, st.Do)
			}
			if strings.HasPrefix(st.As, "@") {
				return fmt.Errorf("%s: as names a node without the @", where)
			}
			names[st.As] = true
		}
		for field, v := range map[string]string{"within": st.Within, "for": st.For, "grace": st.Grace, "latency": st.Latency, "jitter": st.Jitter} {
			if v == "" {
				continue
			}
			if _, err := time.ParseDuration(v); err != nil {
				return fmt.Errorf("%s: %s: %w", where, field, err)
			}
		}
		switch st.Do {
		case "sleep":
			if st.For == "" {
				return fmt.Errorf("%s: needs for", where)
			}
		case "mark":
			if st.Label == "" {
				return fmt.Errorf("%s: needs label", where)
			}
		case "start":
			switch st.Recover {
			case "", "accept-loss", "reset":
			default:
				return fmt.Errorf("%s: recover must be accept-loss or reset, got %q", where, st.Recover)
			}
			if st.Recover != "" && !sc.NodeArgs {
				return fmt.Errorf("%s: recover needs cluster.scenario.node_args: true", where)
			}
		case "witness_down", "witness_up":
			if sc.Witness == "" {
				return fmt.Errorf("%s: the case has no witness", where)
			}
		case "proxy_latency", "proxy_down", "proxy_up":
			if !proxies[st.Proxy] {
				return fmt.Errorf("%s: proxy %q is not declared in cluster.scenario.proxies", where, st.Proxy)
			}
		case "scale_to":
			if st.Nodes < 1 || st.Nodes > maxNodes {
				return fmt.Errorf("%s: nodes must be between 1 and %d (cluster.nodes + spare_nodes), got %d", where, maxNodes, st.Nodes)
			}
		case "wait_log", "expect_no_log":
			if st.Log == "" {
				return fmt.Errorf("%s: needs log", where)
			}
		}
	}
	return nil
}

func validateNodeSelector(sel string, maxNodes int, names map[string]bool) error {
	switch {
	case sel == "leader" || sel == "follower" || sel == "owner":
		return nil
	case strings.HasPrefix(sel, "@"):
		if !names[sel[1:]] {
			return fmt.Errorf("node %q was not remembered by an earlier step's as", sel)
		}
		return nil
	case sel == "":
		return fmt.Errorf("needs node")
	}
	n, err := strconv.Atoi(sel)
	if err != nil || n < 1 || n > maxNodes {
		return fmt.Errorf("node must be leader, follower, owner, @name or 1..%d, got %q", maxNodes, sel)
	}
	return nil
}
