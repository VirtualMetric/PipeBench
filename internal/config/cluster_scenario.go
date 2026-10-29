package config

import (
	"fmt"
	"math"
	"path"
	"regexp"
	"sort"
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

	// NodeDisk gives chosen nodes a small disk of their own at a path: a
	// size-limited tmpfs, which a fill_disk step can fill.
	NodeDisk *ScenarioNodeDisk `yaml:"node_disk"`

	// Rows compares the rows a database holds with the rows that reached the
	// receiver, for a case whose data comes from a database device. A row
	// that never arrives counts as missing. A case with the http_ack load can
	// have it too: the receiver then counts the load's lines and the rows
	// apart, and the first receiver is the one whose rows are compared.
	Rows *ScenarioRows `yaml:"rows"`

	// StartImage is the image every node starts on, with its tag, such as an
	// earlier release (vmetric/director:2.6.0). Empty means the image under
	// test. A start or rolling_restart step with `image` moves nodes to
	// another image later in the run.
	StartImage string `yaml:"start_image"`

	// NodeVolume keeps one directory of every node, such as the director's
	// storage directory, on a Docker volume of the node's own. The volume
	// outlives the node's container. A step that moves a node to another
	// image gives it a new container, and the new container finds the node's
	// data in the volume, as a director that an upgrade replaced on its
	// machine finds its data on the machine's disk.
	NodeVolume string `yaml:"node_volume"`
}

// SubjectImageKeyword is the value of a step's image that names the image
// under test (the --image and --version of the run).
const SubjectImageKeyword = "subject"

// imageRefPattern is the form of an image reference a case may name: an
// optional registry host, a repository, and a tag or a digest. The reference
// goes into the compose file, so it allows no spaces, quotes or $.
var imageRefPattern = regexp.MustCompile(`^(?:[a-z0-9][a-z0-9.-]*(?::[0-9]+)?/)?[a-z0-9]+(?:[._-]+[a-z0-9]+)*(?:/[a-z0-9]+(?:[._-]+[a-z0-9]+)*)*(?::[A-Za-z0-9_][A-Za-z0-9._-]{0,127})?(?:@sha256:[a-f0-9]{64})?$`)

// checkImageRef says what is wrong with an image reference, or returns nil.
// A reference must carry a tag or a digest: an image without one is
// "latest", which moves, so the case would not say which build it ran.
func checkImageRef(ref string) error {
	if !imageRefPattern.MatchString(ref) {
		return fmt.Errorf("is not an image reference such as vmetric/director:2.6.0")
	}
	name := ref
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		name = ref[i+1:]
	}
	if !strings.Contains(name, ":") && !strings.Contains(ref, "@sha256:") {
		return fmt.Errorf("names no tag; name the image with its tag, such as vmetric/director:2.6.0")
	}
	return nil
}

// ScenarioNodeDisk is the cluster.scenario.node_disk block.
type ScenarioNodeDisk struct {
	// Path is where the disk is mounted in each node: the director's storage
	// directory, to fill the disk the director writes to.
	Path string `yaml:"path"`
	// Size is the disk's size, such as 1g. The memory a tmpfs uses counts
	// against the node's memory limit.
	Size string `yaml:"size"`
	// Nodes are the nodes that get the disk. Empty means every node.
	Nodes []int `yaml:"nodes"`
}

// HasNode reports whether node n gets the disk.
func (d *ScenarioNodeDisk) HasNode(n int) bool {
	if d == nil {
		return false
	}
	if len(d.Nodes) == 0 {
		return true
	}
	for _, x := range d.Nodes {
		if x == n {
			return true
		}
	}
	return false
}

// ScenarioRows is the cluster.scenario.rows block.
type ScenarioRows struct {
	// Query returns the key of every row the device should deliver, one per
	// row, such as SELECT message FROM events.
	Query string `yaml:"query"`
	// Key is a regular expression that finds a row's key in a line the
	// receiver gets. With a group, the first group is the key.
	Key string `yaml:"key"`
	// Writer is an endpoint that writes rows while the case runs. The harness
	// stops it before it counts, so every row it wrote has had a chance to
	// arrive.
	Writer string `yaml:"writer"`
	// DrainSeconds bounds the wait for the last rows to arrive (default 60).
	DrainSeconds int `yaml:"drain_seconds"`
}

// DrainOrDefault returns how long to wait for the last rows.
func (r *ScenarioRows) DrainOrDefault() time.Duration {
	if r == nil || r.DrainSeconds <= 0 {
		return 60 * time.Second
	}
	return time.Duration(r.DrainSeconds) * time.Second
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
	// A case with several receivers (`receivers:`) needs every acknowledged
	// line at each of them, so the limit applies to each receiver.
	MaxLoss *int64 `yaml:"max_loss"`
	// MaxDuplicates is how many lines may arrive more than once, at each
	// receiver when there are several.
	MaxDuplicates *int64 `yaml:"max_duplicates"`
	// MaxSplitBrainMs is how long two nodes may both name themselves the
	// cluster's leader in the same round of /health reads.
	MaxSplitBrainMs *int64 `yaml:"max_split_brain_ms"`
	// MaxStreamLeaderlessMs is how long one stream of the cluster may go
	// without a leader: every reachable node that reports it names none.
	MaxStreamLeaderlessMs *int64 `yaml:"max_stream_leaderless_ms"`
	// MinAcked is the fewest lines the generator must have had
	// acknowledged, so a run that sent nothing cannot pass.
	MinAcked int64 `yaml:"min_acked"`
	// MaxMissingRows is how many database rows (cluster.scenario.rows) may
	// never arrive.
	MaxMissingRows *int64 `yaml:"max_missing_rows"`
	// MaxRowsTwice is how many database rows may arrive more than once. It
	// counts by the row's key, so a row collected again counts even when its
	// second line differs from the first (a collection time, the node), which
	// the receiver's hash-based duplicate count does not see.
	MaxRowsTwice *int64 `yaml:"max_rows_twice"`

	// Receivers judges chosen receivers of a case with several on their own,
	// keyed by the receiver's id. A limit set for a receiver here replaces
	// the case's own for that receiver, and the other receivers keep the
	// case's. A case that tests data a subject drops on purpose at one target
	// (at a size limit, for one) names that target's receiver here, while
	// every other receiver must still get every line.
	Receivers map[string]ScenarioReceiverExpect `yaml:"receivers"`
}

// ScenarioReceiverExpect is how one receiver is judged
// (cluster.scenario.expect.receivers.<id>).
type ScenarioReceiverExpect struct {
	// MaxLoss and MaxDuplicates replace the case's own for this receiver.
	MaxLoss       *int64 `yaml:"max_loss"`
	MaxDuplicates *int64 `yaml:"max_duplicates"`
	// MinLoss is the fewest acknowledged lines that must be missing at this
	// receiver. A case that tests a loss the subject makes on purpose fails
	// when nothing was lost, because it then tested nothing.
	MinLoss *int64 `yaml:"min_loss"`
}

// ReceiverLimits returns the limits receiver id is judged by: the case's
// max_loss and max_duplicates, each replaced by the receiver's own where
// expect.receivers sets one, and the receiver's min_loss.
func (ex ScenarioExpect) ReceiverLimits(id string) (maxLoss, maxDuplicates, minLoss *int64) {
	maxLoss, maxDuplicates = ex.MaxLoss, ex.MaxDuplicates
	own, ok := ex.Receivers[id]
	if !ok {
		return maxLoss, maxDuplicates, nil
	}
	if own.MaxLoss != nil {
		maxLoss = own.MaxLoss
	}
	if own.MaxDuplicates != nil {
		maxDuplicates = own.MaxDuplicates
	}
	return maxLoss, maxDuplicates, own.MinLoss
}

// ScenarioStep is one step. Do names the action; the other fields are its
// parameters, and each action reads only the ones it needs.
type ScenarioStep struct {
	Do string `yaml:"do"`

	// Node selects the node: "leader" (the current leader), "follower" (the
	// lowest-numbered running node that is not the leader), "owner" (the node
	// the leader last placed a placement-managed device on), a number, or
	// "@name" for a node an earlier step remembered with As. An
	// expect_no_log step may also name "all": no node may log the text.
	Node string `yaml:"node"`
	// As remembers the node this step acted on, for a later "@name".
	As string `yaml:"as"`

	// Within bounds a wait; For is how long an expectation must hold, or a
	// sleep's length.
	Within string `yaml:"within"`
	For    string `yaml:"for"`

	// Record names the measurement a wait stores: the time from the last
	// disruption to the moment the wait was satisfied. expect_writes_stop,
	// expect_delivery_stops and expect_writes_continue also count lines.
	// The report lists those counts apart from the times, under the same
	// name with _acked_after, _lines_after or _lines_acked added.
	Record string `yaml:"record"`
	// Label names a mark.
	Label string `yaml:"label"`

	// Signal is the signal a kill sends (default SIGKILL).
	Signal string `yaml:"signal"`
	// Grace is the stop timeout for stop and restart (default 30s).
	Grace string `yaml:"grace"`
	// Gap is the longest expect_delivery_continues allows between two
	// arrivals of lines sent in its window (default 10s), and the longest
	// pause between two acknowledgements expect_writes_continue allows (not
	// judged when unset).
	Gap string `yaml:"gap"`

	// Recover passes -recover=<value> to a start (accept-loss or reset).
	// Needs scenario.node_args.
	Recover string `yaml:"recover"`

	// Proxy names a Toxiproxy proxy; Latency and Jitter set its latency
	// (0 removes it).
	Proxy   string `yaml:"proxy"`
	Latency string `yaml:"latency"`
	Jitter  string `yaml:"jitter"`

	// Receiver names, by its id, the receiver a receiver_pause or
	// receiver_unpause step acts on. A case with several receivers
	// (`receivers:`) names one; a case with one receiver names none.
	Receiver string `yaml:"receiver"`

	// Nodes is the cluster size a scale_to step moves to.
	Nodes int `yaml:"nodes"`
	// Stage is the stage a set_stage step renders every member's config at:
	// the config template reads it as {{@.Stage@}}, which is 1 at the start.
	// A case changes a cluster setting at a stage, as the platform changes a
	// setting on every member at once.
	Stage int `yaml:"stage"`

	// Log is the text wait_log waits for, or that expect_no_log must not see.
	Log string `yaml:"log"`
	// Logs lists several such texts, for an outcome a node can log in more
	// than one way: wait_log is satisfied by the first of them to appear, and
	// expect_no_log fails on any of them. A step names log or logs, not both.
	Logs []string `yaml:"logs"`

	// Since names an earlier mark. An expect_no_log step then checks the
	// node's log from that mark to the end of its own window, not from its
	// own start, so one check also covers the steps between the mark and
	// itself. A case uses it to check that an old owner collected nothing
	// from the moment it woke up, through the wait for the line that says it
	// gave the device up, and for a window after that.
	Since string `yaml:"since"`

	// SentBefore names an earlier mark for a count_pending step. The step
	// counts the acknowledged lines sent before that mark that have not
	// reached the receiver when the step runs, and that arrive later. For
	// example, records that killed nodes still held on their own disks,
	// counted just before the nodes start again.
	SentBefore string `yaml:"sent_before"`

	// SentAfter names an earlier mark for an expect_arrived step, which
	// counts the lines sent between SentAfter and SentBefore that reached its
	// receiver. It counts after the drain, so a line still on its way at the
	// step counts too.
	SentAfter string `yaml:"sent_after"`
	// Min and Max bound an expect_arrived step's count.
	Min *int64 `yaml:"min"`
	Max *int64 `yaml:"max"`
	// CompareTo names another receiver for an expect_arrived step, and
	// MinShare and MaxShare bound the step's count as a share of what that
	// receiver got of the same window, from 0 to 1. A receiver that gets every
	// line is the measure of how many were sent, whatever rate the load
	// reached. A step sets at least one of min, max, min_share and max_share.
	CompareTo string   `yaml:"compare_to"`
	MinShare  *float64 `yaml:"min_share"`
	MaxShare  *float64 `yaml:"max_share"`

	// Soft makes a failed expectation a note instead of a failure.
	Soft bool `yaml:"soft"`

	// Peer and Peers name the other end of the links a link step acts on:
	// node selectors, "all" (every other node), or another container of the
	// run by name (bench-receiver, bench-azurite). A step names one of them.
	Peer  string   `yaml:"peer"`
	Peers []string `yaml:"peers"`
	// OneWay limits a link step to the traffic from node to its peers. The
	// peers' traffic to node still arrives.
	OneWay bool `yaml:"one_way"`
	// Loss is the share of packets a shape_link step drops, such as 20%.
	// latency and jitter set its delay.
	Loss string `yaml:"loss"`
	// Groups are the sides of a split: every link between two nodes of
	// different groups is cut, both ways.
	Groups [][]string `yaml:"groups"`

	// EmptyDisk makes a start step replace the node's container with a new
	// one, which starts on an empty disk, as a node whose disk was replaced.
	EmptyDisk bool `yaml:"empty_disk"`
	// Image makes a start step start the node on another image, and a
	// rolling_restart step move every node to it in turn: "subject" for the
	// image under test, or an image reference with its tag. The node gets a
	// new container from that image, on the data its volume keeps
	// (cluster.scenario.node_volume). A start step with image starts a node
	// that an earlier step stopped or killed, so the case says how the old
	// version stopped.
	Image string `yaml:"image"`
	// PauseLoadAtStop makes a rolling_restart step pause the load from a few
	// seconds before it stops each node until the node has stopped, as a
	// pause_load step does. The node then holds no record it took in a
	// moment before its stop: the case tests what the restart keeps, not
	// what a version does with the records of its last moment.
	PauseLoadAtStop bool `yaml:"pause_load_at_stop"`
	// Leave is how much free space a fill_disk step leaves on the node's disk
	// (default 1m).
	Leave string `yaml:"leave"`
	// DiskBPS and DiskIOPS are how fast a slow_disk step lets the node read
	// and write the disk that holds its volume: bytes a second, such as 1m,
	// and operations a second. A step sets one of them or both.
	DiskBPS  string `yaml:"disk_bps"`
	DiskIOPS int    `yaml:"disk_iops"`
}

// PeerList returns the peers a link step names: its peers, or its one peer.
func (s ScenarioStep) PeerList() []string {
	if len(s.Peers) > 0 {
		return s.Peers
	}
	if s.Peer == "" {
		return nil
	}
	return []string{s.Peer}
}

// LossPercent parses a loss such as "20%" into 20. An empty loss is 0.
func LossPercent(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	num, ok := strings.CutSuffix(strings.TrimSpace(s), "%")
	if !ok {
		return 0, fmt.Errorf("loss %q must be a percentage such as 20%%", s)
	}
	p, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
	// ParseFloat reads "NaN", and NaN fails every comparison.
	if err != nil || math.IsNaN(p) || p <= 0 || p > 100 {
		return 0, fmt.Errorf("loss %q must be a percentage above 0%% and at most 100%%", s)
	}
	return p, nil
}

// scenarioActions lists every step action and whether it needs Node.
var scenarioActions = map[string]bool{
	"wait_ready":                false,
	"start_load":                false,
	"pause_load":                false,
	"resume_load":               false,
	"sleep":                     false,
	"mark":                      false,
	"kill":                      true,
	"stop":                      true,
	"start":                     true,
	"restart":                   true,
	"pause":                     true,
	"unpause":                   true,
	"disconnect":                true,
	"connect":                   true,
	"wait_leader":               false,
	"expect_no_leader":          false,
	"wait_delivery":             false,
	"expect_writes_stop":        false,
	"expect_writes_continue":    false,
	"expect_delivery_stops":     false,
	"expect_delivery_continues": false,
	"wait_healthy":              false,
	"witness_down":              false,
	"witness_up":                false,
	"proxy_latency":             false,
	"proxy_down":                false,
	"proxy_up":                  false,
	"receiver_pause":            false,
	"receiver_unpause":          false,
	"scale_to":                  false,
	"set_stage":                 false,
	"rolling_restart":           false,
	"wait_log":                  false,
	"expect_no_log":             true,
	"wait_stable":               false,
	"count_pending":             false,
	"expect_arrived":            false,
	// The link steps check their node themselves: it may be "all".
	"cut_link":     false,
	"shape_link":   false,
	"restore_link": false,
	"split":        false,
	"fill_disk":    true,
	"free_disk":    true,
	"slow_disk":    true,
	"restore_disk": true,
}

// loadCountingSteps are the steps that count the load's lines or wait for
// them. None may run while the load is paused, because no line is sent then.
var loadCountingSteps = map[string]bool{
	"expect_writes_continue": true, "expect_delivery_continues": true, "wait_delivery": true,
	"expect_writes_stop": true,
}

// checkLoadPause follows the pause_load and resume_load steps in order: the
// load must run to be paused, and be paused to be resumed.
func checkLoadPause(do string, paused *bool) error {
	switch do {
	case "pause_load":
		if *paused {
			return fmt.Errorf("the load is paused already")
		}
		*paused = true
	case "resume_load":
		if !*paused {
			return fmt.Errorf("no earlier pause_load step paused the load")
		}
		*paused = false
	}
	return nil
}

// pausedAtEnd reports whether the steps leave the load paused.
func pausedAtEnd(steps []ScenarioStep) bool {
	paused := false
	for _, st := range steps {
		switch st.Do {
		case "pause_load":
			paused = true
		case "resume_load":
			paused = false
		}
	}
	return paused
}

// checkDiskLimit checks the limit of a slow_disk step: disk_bps, disk_iops,
// or both, each above 0.
func checkDiskLimit(st ScenarioStep) error {
	if st.DiskBPS == "" && st.DiskIOPS == 0 {
		return fmt.Errorf("needs disk_bps, disk_iops or both: how fast the node may use its disk")
	}
	if st.DiskBPS != "" {
		bps, err := ParseByteSize(st.DiskBPS)
		if err != nil {
			return fmt.Errorf("disk_bps %q: %w", st.DiskBPS, err)
		}
		if bps <= 0 {
			return fmt.Errorf("disk_bps must be above 0, got %q", st.DiskBPS)
		}
	}
	if st.DiskIOPS < 0 {
		return fmt.Errorf("disk_iops must be above 0, got %d", st.DiskIOPS)
	}
	return nil
}

// linkSteps are the actions that change the links between containers.
var linkSteps = map[string]bool{"cut_link": true, "shape_link": true, "restore_link": true, "split": true}

// nodeActionSteps are the actions that act on a node, so their `as` can
// remember it. The driver records a name only for these.
var nodeActionSteps = map[string]bool{
	"kill": true, "stop": true, "start": true, "restart": true,
	"pause": true, "unpause": true, "disconnect": true, "connect": true,
	"fill_disk": true, "free_disk": true, "slow_disk": true, "restore_disk": true,
}

// containerPeerPattern is the form of a link step's peer that names another
// container of the run rather than a node.
var containerPeerPattern = regexp.MustCompile(`^bench-[a-z0-9][a-z0-9._-]*$`)

// minNodeDisk is the smallest cluster.scenario.node_disk size: a director
// cannot start on less.
const minNodeDisk = 64 << 20

// ScenarioActionNeedsNode reports whether the action takes a node.
func ScenarioActionNeedsNode(do string) bool { return scenarioActions[do] }

// LogLines returns the texts a wait_log or expect_no_log step looks for: its
// logs, or its one log.
func (s ScenarioStep) LogLines() []string {
	if len(s.Logs) > 0 {
		return s.Logs
	}
	if s.Log == "" {
		return nil
	}
	return []string{s.Log}
}

// DurationOr parses s, or returns def when s is empty.
func (s ScenarioStep) DurationOr(field string, def time.Duration) time.Duration {
	v := s.Within
	if field == "for" {
		v = s.For
	} else if field == "grace" {
		v = s.Grace
	} else if field == "gap" {
		v = s.Gap
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
	// With several receivers, the driver checks the load's acknowledged lines
	// at each of them. Without the load there is nothing to check them by.
	if tc.MultiReceiver() && !tc.HasGenerator() {
		return fmt.Errorf("case %q: a scenario case with several receivers checks the load's lines at each of them, so it needs the http_ack generator", tc.Name)
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
	if err := tc.validateScenarioImages(); err != nil {
		return err
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

	if pausedAtEnd(sc.Steps) {
		return fmt.Errorf("case %q: a pause_load step pauses the load and no later resume_load step resumes it, so the run would wait for a load that never ends", tc.Name)
	}
	maxNodes := tc.Cluster.Nodes + sc.SpareNodes
	if err := tc.validateScenarioDiskAndRows(maxNodes); err != nil {
		return err
	}
	if err := tc.validateReceiverExpect(sc.Expect); err != nil {
		return fmt.Errorf("case %q: %w", tc.Name, err)
	}
	if sc.Expect.MaxStreamLeaderlessMs != nil && *sc.Expect.MaxStreamLeaderlessMs < 0 {
		return fmt.Errorf("case %q: expect.max_stream_leaderless_ms must not be negative, got %d", tc.Name, *sc.Expect.MaxStreamLeaderlessMs)
	}
	if sc.Expect.MaxStreamLeaderlessMs != nil && tc.Cluster.Nodes+sc.SpareNodes < 2 {
		return fmt.Errorf("case %q: expect.max_stream_leaderless_ms measures the leaders of a cluster's streams, and one director leads its streams itself", tc.Name)
	}
	names := map[string]bool{}
	// marks holds the labels of the marks set so far: since and sent_before
	// may only name a mark an earlier step sets.
	marks := map[string]bool{}
	// paused says whether a pause_load step has paused the load at this
	// point of the steps.
	paused := false
	for i, st := range sc.Steps {
		where := fmt.Sprintf("case %q: cluster.scenario.steps[%d] (%s)", tc.Name, i, st.Do)
		needsNode, known := scenarioActions[st.Do]
		if !known {
			return fmt.Errorf("case %q: cluster.scenario.steps[%d]: unknown action %q", tc.Name, i, st.Do)
		}
		// A step that acts on a node must name it. A wait that may name one
		// (wait_leader, wait_log) must name it the same way when it does. A
		// link step checks its own node, which may be "all", and so may an
		// expect_no_log step.
		allNodes := st.Do == "expect_no_log" && st.Node == "all"
		if (needsNode || st.Node != "") && !linkSteps[st.Do] && !allNodes {
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
		for field, v := range map[string]string{"within": st.Within, "for": st.For, "grace": st.Grace, "gap": st.Gap, "latency": st.Latency, "jitter": st.Jitter} {
			if v == "" {
				continue
			}
			if _, err := time.ParseDuration(v); err != nil {
				return fmt.Errorf("%s: %s: %w", where, field, err)
			}
		}
		if (st.Do == "pause_load" || st.Do == "resume_load") && !tc.HasGenerator() {
			return fmt.Errorf("%s: pauses and resumes the load, and the case has no generator", where)
		}
		if st.PauseLoadAtStop && !tc.HasGenerator() {
			return fmt.Errorf("%s: pause_load_at_stop pauses the load, and the case has no generator", where)
		}
		if st.PauseLoadAtStop && paused {
			return fmt.Errorf("%s: pause_load_at_stop pauses the load, which a pause_load step has paused already", where)
		}
		if paused && loadCountingSteps[st.Do] {
			return fmt.Errorf("%s: counts the load's lines while a pause_load step has paused the load; resume it first", where)
		}
		if err := checkLoadPause(st.Do, &paused); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
		if st.Do == "expect_delivery_continues" && !tc.HasGenerator() {
			return fmt.Errorf("%s: counts the load's lines, and the case has no generator", where)
		}
		if st.Do == "expect_delivery_continues" || (st.Do == "expect_writes_continue" && st.Gap != "") {
			// The windows default to 30s and 20s, as the driver runs them.
			window := 30 * time.Second
			if st.Do == "expect_writes_continue" {
				window = 20 * time.Second
			}
			gap := st.DurationOr("gap", 10*time.Second)
			if gap < 2*time.Second {
				return fmt.Errorf("%s: gap must be at least 2s: a line or a confirmation needs time to come", where)
			}
			if st.DurationOr("for", window) <= gap {
				return fmt.Errorf("%s: for must be longer than gap", where)
			}
		}
		if st.Since != "" {
			if st.Do != "expect_no_log" {
				return fmt.Errorf("%s: since only applies to expect_no_log", where)
			}
			if !marks[st.Since] {
				return fmt.Errorf("%s: since names mark %q, which no earlier step sets", where, st.Since)
			}
		}
		if st.SentBefore != "" && st.Do != "count_pending" && st.Do != "expect_arrived" {
			return fmt.Errorf("%s: sent_before only applies to count_pending and expect_arrived", where)
		}
		// A logs key that is present counts, even as an empty list: logs: []
		// is a mistake to report, not a field to ignore.
		if st.Logs != nil && st.Do != "wait_log" && st.Do != "expect_no_log" {
			return fmt.Errorf("%s: logs only applies to wait_log and expect_no_log", where)
		}
		if err := checkStepFieldOwners(st); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
		if linkSteps[st.Do] {
			if err := validateLinkStep(st, maxNodes, names); err != nil {
				return fmt.Errorf("%s: %w", where, err)
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
			marks[st.Label] = true
		case "count_pending":
			if !tc.HasGenerator() {
				return fmt.Errorf("%s: counts the load's lines, and the case has no generator", where)
			}
			if st.SentBefore == "" {
				return fmt.Errorf("%s: needs sent_before", where)
			}
			if !marks[st.SentBefore] {
				return fmt.Errorf("%s: sent_before names mark %q, which no earlier step sets", where, st.SentBefore)
			}
			if st.Record == "" {
				return fmt.Errorf("%s: needs record, the name the count is reported under", where)
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
		case "wait_leader":
			if st.Groups != nil {
				if len(st.Groups) == 0 {
					return fmt.Errorf("%s: groups is empty; name at least one group of nodes", where)
				}
				if err := validateGroups(st.Groups, maxNodes, names); err != nil {
					return fmt.Errorf("%s: %w", where, err)
				}
			}
		case "fill_disk", "free_disk":
			if sc.NodeDisk == nil {
				return fmt.Errorf("%s: needs cluster.scenario.node_disk, the disk it fills", where)
			}
			if n, err := strconv.Atoi(st.Node); err == nil && !sc.NodeDisk.HasNode(n) {
				return fmt.Errorf("%s: node %d has no disk of its own (cluster.scenario.node_disk.nodes)", where, n)
			}
			if st.Leave != "" {
				if _, err := ParseByteSize(st.Leave); err != nil {
					return fmt.Errorf("%s: leave %q: %w", where, st.Leave, err)
				}
			}
		case "slow_disk", "restore_disk":
			if sc.NodeVolume == "" {
				return fmt.Errorf("%s: limits the disk that holds the node's volume, so it needs cluster.scenario.node_volume", where)
			}
			if st.Do == "slow_disk" {
				if err := checkDiskLimit(st); err != nil {
					return fmt.Errorf("%s: %w", where, err)
				}
			}
		case "witness_down", "witness_up":
			if sc.Witness == "" {
				return fmt.Errorf("%s: the case has no witness", where)
			}
		case "proxy_latency", "proxy_down", "proxy_up":
			if !proxies[st.Proxy] {
				return fmt.Errorf("%s: proxy %q is not declared in cluster.scenario.proxies", where, st.Proxy)
			}
		case "set_stage":
			if st.Stage < 1 {
				return fmt.Errorf("%s: needs stage, 1 or more: the stage every member's config is rendered at", where)
			}
		case "scale_to":
			if st.Nodes < 1 || st.Nodes > maxNodes {
				return fmt.Errorf("%s: nodes must be between 1 and %d (cluster.nodes + spare_nodes), got %d", where, maxNodes, st.Nodes)
			}
		case "receiver_pause", "receiver_unpause":
			if err := tc.validateScenarioReceiver(st.Receiver); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
		case "expect_arrived":
			if err := tc.validateExpectArrived(st, marks); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
		case "wait_log", "expect_no_log":
			if st.Log != "" && st.Logs != nil {
				return fmt.Errorf("%s: names log and logs; name one of them", where)
			}
			if st.Logs != nil && len(st.Logs) == 0 {
				return fmt.Errorf("%s: logs is empty; name at least one line", where)
			}
			if st.Log == "" && len(st.Logs) == 0 {
				return fmt.Errorf("%s: needs log", where)
			}
			for _, l := range st.Logs {
				if l == "" {
					return fmt.Errorf("%s: logs has an empty line, which every log line contains", where)
				}
			}
		}
	}
	return nil
}

// validateScenarioImages checks start_image, node_volume and the image of
// every step that moves nodes to another image.
func (tc *TestCase) validateScenarioImages() error {
	sc := tc.Cluster.Scenario
	if sc.StartImage != "" {
		if err := checkImageRef(sc.StartImage); err != nil {
			return fmt.Errorf("case %q: cluster.scenario.start_image %q %v", tc.Name, sc.StartImage, err)
		}
	}
	if sc.NodeVolume != "" {
		if err := checkWitnessDir(sc.NodeVolume); err != nil {
			return fmt.Errorf("case %q: cluster.scenario.node_volume %q %v", tc.Name, sc.NodeVolume, err)
		}
		if sc.NodeDisk != nil && path.Clean(sc.NodeDisk.Path) == path.Clean(sc.NodeVolume) {
			return fmt.Errorf("case %q: cluster.scenario.node_volume and node_disk.path are both %q; a directory is on a volume or on a small disk, not both", tc.Name, sc.NodeVolume)
		}
	}
	testsSubject := false
	for i, st := range sc.Steps {
		where := fmt.Sprintf("case %q: cluster.scenario.steps[%d] (%s)", tc.Name, i, st.Do)
		if st.Do == "start" && st.EmptyDisk && sc.NodeVolume != "" {
			return fmt.Errorf("%s: empty_disk starts the node on an empty disk, but cluster.scenario.node_volume keeps the node's data in a volume; a case uses one of them", where)
		}
		if st.Image == "" {
			continue
		}
		if st.Image == SubjectImageKeyword {
			testsSubject = true
		} else if err := checkImageRef(st.Image); err != nil {
			return fmt.Errorf("%s: image %q %v, or subject for the image under test", where, st.Image, err)
		}
		if sc.NodeVolume == "" {
			return fmt.Errorf("%s: image gives the node a new container, which would start on an empty disk; set cluster.scenario.node_volume to keep the node's data", where)
		}
		if st.EmptyDisk {
			return fmt.Errorf("%s: names image and empty_disk; a start with image keeps the node's data", where)
		}
	}
	// A case whose nodes start on another image must move them to the image
	// under test at some step. Otherwise it would pass or fail whatever the
	// build under test does.
	if sc.StartImage != "" && !testsSubject {
		return fmt.Errorf("case %q: cluster.scenario.start_image starts the nodes on %s, and no step moves a node to the image under test (image: subject), so the case would not test it", tc.Name, sc.StartImage)
	}
	return nil
}

// validateReceiverExpect checks cluster.scenario.expect.receivers: each
// entry names one of the case's several receivers, and its limits make sense.
func (tc *TestCase) validateReceiverExpect(ex ScenarioExpect) error {
	if ex.Receivers == nil {
		return nil
	}
	if !tc.MultiReceiver() {
		return fmt.Errorf("expect.receivers judges receivers of a case with several, and the case has one; use expect's own limits")
	}
	ids := make([]string, 0, len(ex.Receivers))
	for id := range ex.Receivers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		own := ex.Receivers[id]
		if err := tc.validateScenarioReceiver(id); err != nil {
			return fmt.Errorf("expect.receivers: %w", err)
		}
		for field, v := range map[string]*int64{"max_loss": own.MaxLoss, "max_duplicates": own.MaxDuplicates, "min_loss": own.MinLoss} {
			if v != nil && *v < 0 {
				return fmt.Errorf("expect.receivers.%s.%s must be 0 or more, got %d", id, field, *v)
			}
		}
		maxLoss, _, minLoss := ex.ReceiverLimits(id)
		if minLoss != nil && maxLoss != nil && *minLoss > *maxLoss {
			return fmt.Errorf("expect.receivers.%s: min_loss %d is more than max_loss %d", id, *minLoss, *maxLoss)
		}
		if own.MaxLoss == nil && own.MaxDuplicates == nil && own.MinLoss == nil {
			return fmt.Errorf("expect.receivers.%s sets no limit", id)
		}
	}
	return nil
}

// validateExpectArrived checks an expect_arrived step: a window between two
// earlier marks, the receiver it counts at, and at least one bound.
func (tc *TestCase) validateExpectArrived(st ScenarioStep, marks map[string]bool) error {
	if !tc.HasGenerator() {
		return fmt.Errorf("counts the load's lines, and the case has no generator")
	}
	if st.SentAfter == "" || st.SentBefore == "" {
		return fmt.Errorf("needs sent_after and sent_before, the marks the lines it counts were sent between")
	}
	for _, m := range []string{st.SentAfter, st.SentBefore} {
		if !marks[m] {
			return fmt.Errorf("names mark %q, which no earlier step sets", m)
		}
	}
	if st.SentAfter == st.SentBefore {
		return fmt.Errorf("sent_after and sent_before are both %q, so the window is empty", st.SentAfter)
	}
	if st.Min == nil && st.Max == nil && st.MinShare == nil && st.MaxShare == nil {
		return fmt.Errorf("needs min, max, min_share or max_share")
	}
	if (st.MinShare != nil || st.MaxShare != nil) != (st.CompareTo != "") {
		return fmt.Errorf("min_share and max_share are shares of what the compare_to receiver got, so they go together with compare_to")
	}
	for field, v := range map[string]*float64{"min_share": st.MinShare, "max_share": st.MaxShare} {
		if v != nil && (math.IsNaN(*v) || *v < 0 || *v > 1) {
			return fmt.Errorf("%s must be a share from 0 to 1, got %v", field, *v)
		}
	}
	if st.MinShare != nil && st.MaxShare != nil && *st.MinShare > *st.MaxShare {
		return fmt.Errorf("min_share %v is more than max_share %v", *st.MinShare, *st.MaxShare)
	}
	if st.CompareTo != "" {
		if !tc.MultiReceiver() {
			return fmt.Errorf("compare_to names another receiver, and the case has one")
		}
		if st.CompareTo == st.Receiver {
			return fmt.Errorf("compare_to names the receiver the step counts at, %q", st.CompareTo)
		}
		if err := tc.validateScenarioReceiver(st.CompareTo); err != nil {
			return fmt.Errorf("compare_to: %w", err)
		}
	}
	if (st.Min != nil && *st.Min < 0) || (st.Max != nil && *st.Max < 0) {
		return fmt.Errorf("min and max must be 0 or more")
	}
	if st.Min != nil && st.Max != nil && *st.Min > *st.Max {
		return fmt.Errorf("min %d is more than max %d", *st.Min, *st.Max)
	}
	if st.Record == "" {
		return fmt.Errorf("needs record, the name the count is reported under")
	}
	return tc.validateScenarioReceiver(st.Receiver)
}

// validateScenarioReceiver checks the receiver a receiver_pause or
// receiver_unpause step names: one of the case's receivers when it has
// several, and none when it has one.
func (tc *TestCase) validateScenarioReceiver(id string) error {
	if !tc.MultiReceiver() {
		if id != "" {
			return fmt.Errorf("names receiver %q, but the case has one receiver; receiver names one of several", id)
		}
		return nil
	}
	if id == "" {
		return fmt.Errorf("needs receiver: the case has several receivers, so the step names the one it acts on")
	}
	for _, r := range tc.Receivers {
		if r.ID == id {
			return nil
		}
	}
	return fmt.Errorf("receiver %q is not one of the case's receivers", id)
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

// stepFieldOwners lists, for the fields that belong to a few actions only,
// the actions that read them. A field on any other step is a mistake.
var stepFieldOwners = []struct {
	field   string
	set     func(ScenarioStep) bool
	actions map[string]bool
}{
	{"peer", func(s ScenarioStep) bool { return s.Peer != "" }, map[string]bool{"cut_link": true, "shape_link": true, "restore_link": true}},
	{"peers", func(s ScenarioStep) bool { return s.Peers != nil }, map[string]bool{"cut_link": true, "shape_link": true, "restore_link": true}},
	{"one_way", func(s ScenarioStep) bool { return s.OneWay }, map[string]bool{"cut_link": true, "shape_link": true, "restore_link": true}},
	{"loss", func(s ScenarioStep) bool { return s.Loss != "" }, map[string]bool{"shape_link": true}},
	{"groups", func(s ScenarioStep) bool { return s.Groups != nil }, map[string]bool{"split": true, "wait_leader": true}},
	{"empty_disk", func(s ScenarioStep) bool { return s.EmptyDisk }, map[string]bool{"start": true}},
	{"image", func(s ScenarioStep) bool { return s.Image != "" }, map[string]bool{"start": true, "rolling_restart": true}},
	{"stage", func(s ScenarioStep) bool { return s.Stage != 0 }, map[string]bool{"set_stage": true}},
	{"pause_load_at_stop", func(s ScenarioStep) bool { return s.PauseLoadAtStop }, map[string]bool{"rolling_restart": true}},
	{"leave", func(s ScenarioStep) bool { return s.Leave != "" }, map[string]bool{"fill_disk": true}},
	{"disk_bps", func(s ScenarioStep) bool { return s.DiskBPS != "" }, map[string]bool{"slow_disk": true}},
	{"disk_iops", func(s ScenarioStep) bool { return s.DiskIOPS != 0 }, map[string]bool{"slow_disk": true}},
	{"gap", func(s ScenarioStep) bool { return s.Gap != "" }, map[string]bool{"expect_delivery_continues": true, "expect_writes_continue": true}},
	{"receiver", func(s ScenarioStep) bool { return s.Receiver != "" }, map[string]bool{"receiver_pause": true, "receiver_unpause": true, "expect_arrived": true}},
	{"sent_after", func(s ScenarioStep) bool { return s.SentAfter != "" }, map[string]bool{"expect_arrived": true}},
	{"min", func(s ScenarioStep) bool { return s.Min != nil }, map[string]bool{"expect_arrived": true}},
	{"max", func(s ScenarioStep) bool { return s.Max != nil }, map[string]bool{"expect_arrived": true}},
	{"compare_to", func(s ScenarioStep) bool { return s.CompareTo != "" }, map[string]bool{"expect_arrived": true}},
	{"min_share", func(s ScenarioStep) bool { return s.MinShare != nil }, map[string]bool{"expect_arrived": true}},
	{"max_share", func(s ScenarioStep) bool { return s.MaxShare != nil }, map[string]bool{"expect_arrived": true}},
}

func checkStepFieldOwners(st ScenarioStep) error {
	for _, o := range stepFieldOwners {
		if o.set(st) && !o.actions[st.Do] {
			names := make([]string, 0, len(o.actions))
			for a := range o.actions {
				names = append(names, a)
			}
			sort.Strings(names)
			return fmt.Errorf("%s only applies to %s", o.field, strings.Join(names, ", "))
		}
	}
	return nil
}

// validateLinkStep checks a cut_link, shape_link, restore_link or split step.
func validateLinkStep(st ScenarioStep, maxNodes int, names map[string]bool) error {
	if st.Peer != "" && st.Peers != nil {
		return fmt.Errorf("names peer and peers; name one of them")
	}
	if st.Peers != nil && len(st.Peers) == 0 {
		return fmt.Errorf("peers is empty; name at least one peer")
	}
	switch st.Do {
	case "split":
		if st.Node != "" {
			return fmt.Errorf("takes groups, not node")
		}
		if len(st.Groups) < 2 {
			return fmt.Errorf("needs groups: at least two lists of nodes, the sides of the split")
		}
		return validateGroups(st.Groups, maxNodes, names)
	case "restore_link":
		if st.Node == "" {
			if len(st.PeerList()) > 0 || st.OneWay {
				return fmt.Errorf("without node it restores every link, so peers and one_way need a node")
			}
			return nil
		}
	}
	if st.Node == "" {
		return fmt.Errorf("needs node, one end of the links (a node selector or all)")
	}
	if st.Node != "all" {
		if err := validateNodeSelector(st.Node, maxNodes, names); err != nil {
			return err
		}
	}
	peers := st.PeerList()
	if len(peers) == 0 {
		return fmt.Errorf("needs peer or peers, the other end of the links")
	}
	for _, p := range peers {
		if err := validateLinkPeer(p, maxNodes, names); err != nil {
			return err
		}
		if p == st.Node && p != "all" {
			return fmt.Errorf("a link has two ends, and node and peer are both %q", p)
		}
	}
	if st.Do == "shape_link" {
		loss, err := LossPercent(st.Loss)
		if err != nil {
			return err
		}
		if st.Latency == "" && loss == 0 {
			return fmt.Errorf("needs latency or loss")
		}
		if st.Jitter != "" && st.Latency == "" {
			return fmt.Errorf("jitter varies the latency, so it needs latency")
		}
	}
	return nil
}

// validateGroups checks the groups of a split or a wait_leader: lists of
// nodes, none empty, and no node in two of them.
func validateGroups(groups [][]string, maxNodes int, names map[string]bool) error {
	side := map[string]int{}
	for gi, g := range groups {
		if len(g) == 0 {
			return fmt.Errorf("groups[%d] is empty", gi)
		}
		for _, sel := range g {
			if sel == "all" || containerPeerPattern.MatchString(sel) {
				return fmt.Errorf("a group lists nodes, not %q", sel)
			}
			if err := validateNodeSelector(sel, maxNodes, names); err != nil {
				return fmt.Errorf("groups[%d]: %w", gi, err)
			}
			if prev, seen := side[sel]; seen && prev != gi {
				return fmt.Errorf("%q is in groups[%d] and groups[%d]", sel, prev, gi)
			}
			side[sel] = gi
		}
	}
	return nil
}

// validateLinkPeer checks one end of a link: a node selector, all, or another
// container of the run.
func validateLinkPeer(p string, maxNodes int, names map[string]bool) error {
	if p == "all" || containerPeerPattern.MatchString(p) {
		return nil
	}
	if err := validateNodeSelector(p, maxNodes, names); err != nil {
		return fmt.Errorf("peer %q: %w (or all, or a container such as bench-receiver)", p, err)
	}
	return nil
}

// validateScenarioDiskAndRows checks cluster.scenario.node_disk and
// cluster.scenario.rows.
func (tc *TestCase) validateScenarioDiskAndRows(maxNodes int) error {
	sc := tc.Cluster.Scenario
	if d := sc.NodeDisk; d != nil {
		if err := checkWitnessDir(d.Path); err != nil {
			return fmt.Errorf("case %q: cluster.scenario.node_disk.path %q %v", tc.Name, d.Path, err)
		}
		size, err := ParseByteSize(d.Size)
		if err != nil {
			return fmt.Errorf("case %q: cluster.scenario.node_disk.size %q: %w", tc.Name, d.Size, err)
		}
		if size < minNodeDisk {
			return fmt.Errorf("case %q: cluster.scenario.node_disk.size must be at least 64m; a director cannot start on less", tc.Name)
		}
		for _, n := range d.Nodes {
			if n < 1 || n > maxNodes {
				return fmt.Errorf("case %q: cluster.scenario.node_disk.nodes: %d is not a node (1..%d)", tc.Name, n, maxNodes)
			}
		}
	}
	if r := sc.Rows; r != nil {
		if strings.TrimSpace(r.Query) == "" {
			return fmt.Errorf("case %q: cluster.scenario.rows needs query, which returns the key of every row", tc.Name)
		}
		if r.Key == "" {
			return fmt.Errorf("case %q: cluster.scenario.rows needs key, the expression that finds a row's key in a received line", tc.Name)
		}
		if _, err := regexp.Compile(r.Key); err != nil {
			return fmt.Errorf("case %q: cluster.scenario.rows.key: %w", tc.Name, err)
		}
		// The key reaches the receiver through the compose file, inside double
		// quotes and past compose's own $ handling.
		if strings.ContainsAny(r.Key, "\"\\$") {
			return fmt.Errorf("case %q: cluster.scenario.rows.key must not contain a backslash, a double quote or $; write [0-9] for \\d", tc.Name)
		}
		if tc.Database == nil || tc.Database.Engine != "postgres" {
			return fmt.Errorf("case %q: cluster.scenario.rows reads the case's database, which must use engine postgres", tc.Name)
		}
		if r.Writer != "" && !tc.hasEndpoint(r.Writer) {
			return fmt.Errorf("case %q: cluster.scenario.rows.writer %q is not one of the case's endpoints", tc.Name, r.Writer)
		}
		if r.DrainSeconds < 0 {
			return fmt.Errorf("case %q: cluster.scenario.rows.drain_seconds must not be negative", tc.Name)
		}
	}
	if sc.Expect.MaxMissingRows != nil && sc.Rows == nil {
		return fmt.Errorf("case %q: expect.max_missing_rows needs cluster.scenario.rows", tc.Name)
	}
	if sc.Expect.MaxRowsTwice != nil && sc.Rows == nil {
		return fmt.Errorf("case %q: expect.max_rows_twice needs cluster.scenario.rows", tc.Name)
	}
	return nil
}

func (tc *TestCase) hasEndpoint(name string) bool {
	for _, e := range tc.Endpoints {
		if e.Name == name {
			return true
		}
	}
	return false
}
