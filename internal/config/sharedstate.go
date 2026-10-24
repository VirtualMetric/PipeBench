package config

import (
	"encoding/base64"
	"fmt"
	"path"
	"regexp"
	"slices"
	"time"
)

// The actions a director_shared_state_correctness case can run once its
// replicas are up and its placed devices are running. Each one disrupts the
// replica set in the way a serverless platform (Azure Container Apps) or its
// store can, and the driver then checks that the replicas recover.
const (
	// SharedStateActionNone checks the start only: every replica's state bus
	// connects, one replica leads, each placed device runs on one replica, and
	// data flows.
	SharedStateActionNone = ""
	// SharedStateActionPlacement watches the replicas for hold_seconds after the
	// start and checks that no placed device ever runs on two replicas.
	SharedStateActionPlacement = "placement"
	// SharedStateActionKillLeader kills the leader with SIGKILL.
	SharedStateActionKillLeader = "kill_leader"
	// SharedStateActionKillOwner kills, with SIGKILL, a replica that runs a
	// placed device and does not lead.
	SharedStateActionKillOwner = "kill_owner"
	// SharedStateActionStopLeader stops the leader gracefully (docker stop).
	SharedStateActionStopLeader = "stop_leader"
	// SharedStateActionPauseStore pauses Azurite for outage_seconds, so no
	// replica can reach the store, and then lets it go on.
	SharedStateActionPauseStore = "pause_store"
	// SharedStateActionIsolateReplica disconnects the leader from the network
	// for outage_seconds, so it reaches neither the store nor the other
	// replicas, and then connects it again.
	SharedStateActionIsolateReplica = "isolate_replica"
	// SharedStateActionScaleOutIn starts the standby replica, as a scale-out
	// does, and later stops it gracefully, as a scale-in does.
	SharedStateActionScaleOutIn = "scale_out_in"
	// SharedStateActionColdRestart stops every replica and starts new
	// containers for all of them, on empty disks.
	SharedStateActionColdRestart = "cold_restart"
	// SharedStateActionWrongCredentials runs one replica (bad_replica) with a
	// config whose store credentials are wrong. It has no action after the
	// start: the start is the test.
	SharedStateActionWrongCredentials = "wrong_credentials"
	// SharedStateActionStoreCalls counts the store calls while no data flows
	// (idle_seconds) and under load (load_seconds).
	SharedStateActionStoreCalls = "store_calls"
	// SharedStateActionScaleBacklog stops the receiver of scale_stream, so the
	// replica that stream's generator sends to keeps a backlog, and reads
	// /scale on every replica.
	SharedStateActionScaleBacklog = "scale_backlog"
	// SharedStateActionMoveThenKill stops, gracefully, a replica that runs a
	// placed device and does not lead, so the leader moves the device at once,
	// and kills the replica that runs the shared bus with SIGKILL as soon as
	// the device's new owner says it took the device over.
	SharedStateActionMoveThenKill = "move_then_kill"
	// SharedStateActionStopOwner stops, gracefully, a replica that runs placed
	// devices and does not lead, and does not replace it: a scale-in of a
	// replica that has work.
	SharedStateActionStopOwner = "stop_owner"
	// SharedStateActionRollingRestart stops each replica in turn, gracefully,
	// and replaces it with a new container on an empty disk, as a new revision
	// of a container app does. It waits for each new replica to rejoin and for
	// the replica set to settle before it stops the next. The leader goes last.
	SharedStateActionRollingRestart = "rolling_restart"
	// SharedStateActionMoveAcrossCutLink cuts the link between the replica that
	// runs move_device, which does not lead, and another replica that does not
	// lead, while both keep the store and the leader. Then it renders the
	// config's third stage, in which {{@.MoveTarget@}} is the second replica's
	// member name: the case allows the device only there, so the leader moves
	// it across the cut link at once. The device must start on the second
	// replica and must never run on both.
	SharedStateActionMoveAcrossCutLink = "move_across_cut_link"
)

var sharedStateActions = []string{
	SharedStateActionNone,
	SharedStateActionPlacement,
	SharedStateActionKillLeader,
	SharedStateActionKillOwner,
	SharedStateActionStopLeader,
	SharedStateActionPauseStore,
	SharedStateActionIsolateReplica,
	SharedStateActionScaleOutIn,
	SharedStateActionColdRestart,
	SharedStateActionWrongCredentials,
	SharedStateActionStoreCalls,
	SharedStateActionScaleBacklog,
	SharedStateActionMoveThenKill,
	SharedStateActionStopOwner,
	SharedStateActionRollingRestart,
	SharedStateActionMoveAcrossCutLink,
}

// SharedStateConfig describes the replicas of one serverless director that
// keep the state they share in a store (the director's `shared_state`
// setting): which replica leads, which replica runs which device, and every
// device's checkpoint. The harness starts Replicas director containers from
// one config, with no node list, the way Azure Container Apps starts the
// replicas of one app, and the store is the harness's Azurite.
type SharedStateConfig struct {
	// Replicas is how many replicas the harness starts (at least 2). They are
	// subject-1 to subject-<Replicas>.
	Replicas int `yaml:"replicas"`
	// Standby is how many more replicas the harness renders without starting
	// them, for actions that scale out. They follow the started ones.
	Standby int `yaml:"standby"`
	// StartOrder starts the replicas in groups, one group after the other: a
	// group starts once every replica of the group before it has a connected
	// state bus. The first replica to connect takes the store's lease and
	// leads, so this decides which replica leads at the start. Replicas left
	// out start in a last group. Empty starts every replica at once.
	StartOrder [][]int `yaml:"start_order"`
	// Mode is how the replicas share their state, which sets what the driver
	// expects to see: "state_bus" (the default) when shared_state names a store
	// and the pipeline bus is not embedded, "embedded" when the director's own
	// bus runs shared on the store, and "memory" for shared_state.type memory.
	Mode string `yaml:"mode"`
	// SecondStage renders every replica's config again, with {{@.Stage@}} set
	// to 2, once every replica runs and every placed device the first stage
	// enabled runs. A case uses it to enable devices after the leader has
	// placed the first ones: the leader then puts the new devices on the
	// replica that runs the fewest.
	SecondStage bool `yaml:"second_stage"`
	// FirstStageDevices are placed devices the first stage enables. The
	// driver renders the second stage only once each runs on one replica.
	FirstStageDevices []int64 `yaml:"first_stage_devices"`
	// Action is the disruption to run (see the SharedStateAction constants).
	Action string `yaml:"action"`
	// SettleSeconds bounds the wait for the replicas to recover after the
	// action: a new leader, the placed devices running again (default 60).
	SettleSeconds int `yaml:"settle_seconds"`
	// HoldSeconds is how long the placement action watches the replicas
	// (default 60).
	HoldSeconds int `yaml:"hold_seconds"`
	// PlacedDevices are the ids of the devices the leader places on one
	// replica each. The driver follows them through the ownership lease lines
	// the replicas log.
	PlacedDevices []int64 `yaml:"placed_devices"`
	// MaxFailoverSeconds fails the case when a placed device the action moved
	// takes longer than this to run again, counted from the action. 0 only
	// reports the time.
	MaxFailoverSeconds float64 `yaml:"max_failover_seconds"`
	// ExpectHealth is the status the director's /health endpoint must report
	// for its shared_state component on every running replica: "ok" (the
	// default) for a state bus, "not_configured" when no state bus runs.
	ExpectHealth string `yaml:"expect_health"`
	// StoreContainer is the blob container that holds the shared state, so the
	// driver can count the store calls the replicas make (default
	// "director-state").
	StoreContainer string `yaml:"store_container"`
	// Store is where the replicas keep their shared state. It tells the driver
	// what pause_store takes away and whether it can count the store calls:
	// "azure_blob" (the default) is Azurite, "s3" is the case's MinIO
	// (`minio:`), "redis" is the case's endpoint named redis, and "share" is
	// a directory every replica mounts at SharePath.
	Store string `yaml:"store"`
	// SharePath is where every replica mounts the share (store "share").
	SharePath string `yaml:"share_path"`
	// SecretValues must never appear in a replica's log or in its /health
	// answer: the store's account key, for one. The driver also looks for
	// three 16-character pieces of each, from its start, its middle and its
	// end.
	SecretValues []string `yaml:"secret_values"`
	// Streams say how the data of each generator must arrive at its receiver.
	Streams []SharedStateStream `yaml:"streams"`

	// OutageSeconds is how long pause_store pauses the store, how long
	// isolate_replica keeps the replica off the network, and how long
	// scale_backlog pauses the receiver (default 30).
	OutageSeconds int `yaml:"outage_seconds"`
	// OutageStreams are streams that must go on arriving while the store is
	// away: their senders feed devices that keep no shared state.
	OutageStreams []string `yaml:"outage_streams"`
	// OutageHoldOthersSeconds makes pause_store freeze every replica but the
	// one that led before the outage while the store comes back, and for
	// this many seconds after (0, the default, freezes none). The replica
	// that led is then the only one that can take the store's lease again,
	// which is the takeover a director must survive most carefully: its own
	// state from before the outage meets its new state. It must stay under
	// the 15 seconds after which a replica that renews nothing counts as
	// gone.
	OutageHoldOthersSeconds int `yaml:"outage_hold_others_seconds"`
	// BadReplica is the replica whose config carries wrong store credentials
	// (wrong_credentials). The case's config gives it them through
	// {{@.NodeID@}}.
	BadReplica int `yaml:"bad_replica"`
	// BadHealth is the shared_state status /health must report on the bad
	// replica (default "error").
	BadHealth string `yaml:"bad_health"`
	// IdleSeconds and LoadSeconds are the windows store_calls counts the store
	// calls over, before the generators start and while they run (default 60
	// each).
	IdleSeconds int `yaml:"idle_seconds"`
	LoadSeconds int `yaml:"load_seconds"`
	// MaxStoreWritesPerSec and MaxStoreReadsPerSec fail store_calls when the
	// replicas make more calls a second than this under load. 0 only reports.
	MaxStoreWritesPerSec float64 `yaml:"max_store_writes_per_sec"`
	MaxStoreReadsPerSec  float64 `yaml:"max_store_reads_per_sec"`
	// ScaleStream is the stream whose receiver scale_backlog stops, so the
	// replica its generator sends to builds a backlog.
	ScaleStream string `yaml:"scale_stream"`
	// ScaleMinShare is the share of the loaded replica's own backlog that
	// /scale must report as the total on every other replica (default 0.5).
	ScaleMinShare float64 `yaml:"scale_min_share"`
	// StateWriteIntervalSeconds is how often the replica that runs the shared
	// bus should write the state to the store, when the case sets the
	// director's write interval. store_calls then checks, under load, that
	// each gap between two rounds of that replica's segment writes lasts the
	// interval, give or take StateWriteToleranceSeconds. A round is the writes
	// that come within a second of each other: a snapshot writes a segment for
	// each stream. A round that comes with a snapshot counts as an interval
	// write, because a bus takes its snapshots with its interval writes.
	// Membership and lease writes keep their own pace and are not judged. 0
	// only reports the gaps.
	StateWriteIntervalSeconds int `yaml:"state_write_interval_seconds"`
	// StateWriteToleranceSeconds is how far a gap may stray from the interval
	// (default: a tenth of the interval, at least 2 seconds).
	StateWriteToleranceSeconds int `yaml:"state_write_tolerance_seconds"`
	// MoveKillWithinMs is how soon after the moved device's new owner took it
	// over move_then_kill must have sent SIGKILL to the replica that runs the
	// shared bus (default 2000).
	MoveKillWithinMs int `yaml:"move_kill_within_ms"`
	// ActionDelaySeconds is how long the driver waits, once every stream has
	// reached its baseline, before it runs the action (default 0). A director
	// that writes its state to the store only every so often has more to lose
	// the longer it has run since its last write.
	ActionDelaySeconds int `yaml:"action_delay_seconds"`
	// MoveDevice is the placed device move_across_cut_link moves.
	MoveDevice int64 `yaml:"move_device"`
	// CutHoldSeconds is how long move_across_cut_link keeps the link cut
	// before it moves the device, so both replicas notice the cut first
	// (default 10).
	CutHoldSeconds int `yaml:"cut_hold_seconds"`
	// ExpectLogOnce are lines each replica must log exactly once for every
	// time its director starts, matched as plain text.
	ExpectLogOnce []string `yaml:"expect_log_once"`
}

// StateWriteToleranceOrDefault returns StateWriteToleranceSeconds, or a tenth
// of the interval, at least 2 seconds.
func (s *SharedStateConfig) StateWriteToleranceOrDefault() time.Duration {
	if s.StateWriteToleranceSeconds > 0 {
		return time.Duration(s.StateWriteToleranceSeconds) * time.Second
	}
	return max(2*time.Second, time.Duration(s.StateWriteIntervalSeconds)*time.Second/10)
}

// CutHoldOrDefault returns CutHoldSeconds, or 10.
func (s *SharedStateConfig) CutHoldOrDefault() int {
	if s.CutHoldSeconds > 0 {
		return s.CutHoldSeconds
	}
	return 10
}

// MoveKillWithinOrDefault returns MoveKillWithinMs, or 2 seconds.
func (s *SharedStateConfig) MoveKillWithinOrDefault() time.Duration {
	if s.MoveKillWithinMs > 0 {
		return time.Duration(s.MoveKillWithinMs) * time.Millisecond
	}
	return 2 * time.Second
}

// MinOutageForStreamsSeconds is the shortest outage whose outage_streams the
// driver can judge: it counts them first 15 seconds into the outage, and a
// stream needs a few seconds after that to show it still arrives.
const MinOutageForStreamsSeconds = 20

// OutageOrDefault returns OutageSeconds, or 30.
func (s *SharedStateConfig) OutageOrDefault() int {
	if s == nil || s.OutageSeconds <= 0 {
		return 30
	}
	return s.OutageSeconds
}

// BadHealthOrDefault returns BadHealth, or "error".
func (s *SharedStateConfig) BadHealthOrDefault() string {
	if s == nil || s.BadHealth == "" {
		return "error"
	}
	return s.BadHealth
}

// IdleOrDefault and LoadOrDefault return the store_calls windows, or 60.
func (s *SharedStateConfig) IdleOrDefault() int {
	if s == nil || s.IdleSeconds <= 0 {
		return 60
	}
	return s.IdleSeconds
}

func (s *SharedStateConfig) LoadOrDefault() int {
	if s == nil || s.LoadSeconds <= 0 {
		return 60
	}
	return s.LoadSeconds
}

// ScaleMinShareOrDefault returns ScaleMinShare, or 0.5.
func (s *SharedStateConfig) ScaleMinShareOrDefault() float64 {
	if s == nil || s.ScaleMinShare <= 0 {
		return 0.5
	}
	return s.ScaleMinShare
}

// SharedStateStream ties one generator to the receiver its records reach, and
// says how many may be missing or repeated.
type SharedStateStream struct {
	// Name labels the stream in the driver's output (default: the receiver id).
	Name string `yaml:"name"`
	// Generator is the id of the generator that sends the records.
	Generator string `yaml:"generator"`
	// Receiver is the id of the receiver they reach.
	Receiver string `yaml:"receiver"`
	// MinBaseline is how many records the receiver must hold before the action
	// runs (default 1).
	MinBaseline int64 `yaml:"min_baseline"`
	// MaxLoss is how many of the generator's records may never arrive
	// (default 0).
	MaxLoss int64 `yaml:"max_loss"`
	// MaxDuplicatePct is how many repeated records may arrive, as a percentage
	// of the distinct ones (default 0).
	MaxDuplicatePct float64 `yaml:"max_duplicate_pct"`
	// MaxDuplicates is how many repeated records may arrive, as a count. When
	// set, it replaces max_duplicate_pct: a bound such as "no more than one
	// write interval of records" is a number of records, not a share.
	MaxDuplicates *int64 `yaml:"max_duplicates"`
	// ExpectNothing says that no record of the stream may arrive: its devices
	// must not run at all, as on a director that keeps its shared state in
	// memory. Loss is not judged then.
	ExpectNothing bool `yaml:"expect_nothing"`
}

// StreamName returns the stream's label.
func (s SharedStateStream) StreamName() string {
	if s.Name != "" {
		return s.Name
	}
	return s.Receiver
}

// SettleOrDefault returns SettleSeconds, or 60.
func (s *SharedStateConfig) SettleOrDefault() int {
	if s == nil || s.SettleSeconds <= 0 {
		return 60
	}
	return s.SettleSeconds
}

// HoldOrDefault returns HoldSeconds, or 60.
func (s *SharedStateConfig) HoldOrDefault() int {
	if s == nil || s.HoldSeconds <= 0 {
		return 60
	}
	return s.HoldSeconds
}

// The modes a shared-state case can run in.
const (
	SharedStateModeStateBus = "state_bus"
	SharedStateModeEmbedded = "embedded"
	SharedStateModeMemory   = "memory"
)

// ModeOrDefault returns Mode, or "state_bus".
func (s *SharedStateConfig) ModeOrDefault() string {
	if s == nil || s.Mode == "" {
		return SharedStateModeStateBus
	}
	return s.Mode
}

// ExpectHealthOrDefault returns ExpectHealth, or what the mode implies: "ok"
// for a state bus, "not_configured" otherwise.
func (s *SharedStateConfig) ExpectHealthOrDefault() string {
	switch {
	case s != nil && s.ExpectHealth != "":
		return s.ExpectHealth
	case s.ModeOrDefault() == SharedStateModeStateBus:
		return "ok"
	default:
		return "not_configured"
	}
}

// SharesBus reports whether the replicas coordinate through a shared bus,
// with a leader and ownership leases: a state bus or an embedded queue.
func (s *SharedStateConfig) SharesBus() bool {
	return s.ModeOrDefault() != SharedStateModeMemory
}

// The stores the replicas can keep their shared state in (Store).
const (
	SharedStateStoreAzureBlob = "azure_blob"
	SharedStateStoreS3        = "s3"
	SharedStateStoreRedis     = "redis"
	SharedStateStoreShare     = "share"
)

// StoreOrDefault returns Store, or "azure_blob".
func (s *SharedStateConfig) StoreOrDefault() string {
	if s == nil || s.Store == "" {
		return SharedStateStoreAzureBlob
	}
	return s.Store
}

// StoreService returns the compose service that serves the store, which
// pause_store pauses: azurite, minio or redis. A share has none.
func (s *SharedStateConfig) StoreService() string {
	switch s.StoreOrDefault() {
	case SharedStateStoreAzureBlob:
		return "azurite"
	case SharedStateStoreS3:
		return "minio"
	case SharedStateStoreRedis:
		return "redis"
	}
	return ""
}

// StoreName names the store for people: Azurite, MinIO, Redis or the share.
func (s *SharedStateConfig) StoreName() string {
	switch s.StoreOrDefault() {
	case SharedStateStoreAzureBlob:
		return "Azurite"
	case SharedStateStoreS3:
		return "MinIO"
	case SharedStateStoreRedis:
		return "Redis"
	}
	return "the share"
}

// CountsStoreCalls reports whether the driver can count the replicas' store
// calls. It reads them from Azurite's access log, and no other store here
// logs every request.
func (s *SharedStateConfig) CountsStoreCalls() bool {
	return s.StoreOrDefault() == SharedStateStoreAzureBlob
}

// StoreContainerOrDefault returns StoreContainer, or "director-state".
func (s *SharedStateConfig) StoreContainerOrDefault() string {
	if s == nil || s.StoreContainer == "" {
		return "director-state"
	}
	return s.StoreContainer
}

// StartGroups returns the replica numbers in the groups they start in: the
// configured start order, then the replicas it leaves out, all at once.
func (s *SharedStateConfig) StartGroups() [][]int {
	listed := map[int]bool{}
	var groups [][]int
	for _, g := range s.StartOrder {
		if len(g) == 0 {
			continue
		}
		groups = append(groups, slices.Clone(g))
		for _, n := range g {
			listed[n] = true
		}
	}
	var rest []int
	for n := 1; n <= s.Replicas; n++ {
		if !listed[n] {
			rest = append(rest, n)
		}
	}
	if len(rest) > 0 {
		groups = append(groups, rest)
	}
	return groups
}

// IsDirectorSharedStateType reports whether the case drives replicas of one
// director that share their state through a store
// (runDirectorSharedStateCorrectness).
func (tc *TestCase) IsDirectorSharedStateType() bool {
	return tc.Type == "director_shared_state_correctness"
}

// ReplicaNodes returns how many director containers the compose file renders
// one config each for: the nodes of a director cluster, or the replicas of a
// director that shares its state through a store (started and standby). 0
// means one singular subject.
func (tc *TestCase) ReplicaNodes() int {
	switch {
	case tc.Cluster != nil && tc.Cluster.Nodes > 0:
		return tc.Cluster.Nodes
	case tc.SharedState != nil && tc.SharedState.Replicas > 0:
		return tc.SharedState.Replicas + max(tc.SharedState.Standby, 0)
	default:
		return 0
	}
}

// StandbyReplica reports whether replica n (1-based) is rendered but not
// started with the others.
func (tc *TestCase) StandbyReplica(n int) bool {
	return tc.SharedState != nil && n > tc.SharedState.Replicas
}

// validateSharedState checks the `shared_state:` block and the
// director_shared_state_correctness type's requirements.
func (tc *TestCase) validateSharedState() error {
	if !tc.IsDirectorSharedStateType() {
		if tc.SharedState != nil {
			return fmt.Errorf("case %q: `shared_state:` is only valid for type director_shared_state_correctness", tc.Name)
		}
		return nil
	}
	s := tc.SharedState
	if s == nil {
		return fmt.Errorf("case %q: type director_shared_state_correctness requires a `shared_state:` block", tc.Name)
	}
	if tc.Azure == nil {
		return fmt.Errorf("case %q: type director_shared_state_correctness requires an `azure:` block: Azurite serves the blob pollers' data, and the shared state unless shared_state.store names another store", tc.Name)
	}
	if s.Replicas < 2 {
		return fmt.Errorf("case %q: shared_state.replicas must be at least 2, got %d", tc.Name, s.Replicas)
	}
	if s.Standby < 0 {
		return fmt.Errorf("case %q: shared_state.standby must not be negative, got %d", tc.Name, s.Standby)
	}
	if !slices.Contains(sharedStateActions, s.Action) {
		return fmt.Errorf("case %q: unknown shared_state.action %q", tc.Name, s.Action)
	}
	switch s.ModeOrDefault() {
	case SharedStateModeStateBus, SharedStateModeEmbedded, SharedStateModeMemory:
	default:
		return fmt.Errorf("case %q: shared_state.mode must be state_bus, embedded or memory, got %q", tc.Name, s.Mode)
	}
	switch s.StoreOrDefault() {
	case SharedStateStoreAzureBlob:
	case SharedStateStoreS3:
		if !tc.UsesMinio() {
			return fmt.Errorf("case %q: shared_state.store s3 needs a `minio:` block: MinIO serves the bucket", tc.Name)
		}
	case SharedStateStoreRedis:
		if !slices.ContainsFunc(tc.Endpoints, func(e Endpoint) bool { return e.Name == "redis" }) {
			return fmt.Errorf("case %q: shared_state.store redis needs an endpoint named redis, which serves the store", tc.Name)
		}
	case SharedStateStoreShare:
		if !path.IsAbs(s.SharePath) || path.Clean(s.SharePath) == "/" {
			return fmt.Errorf("case %q: shared_state.store share needs shared_state.share_path: the absolute path, below /, at which every replica mounts the share", tc.Name)
		}
	default:
		return fmt.Errorf("case %q: shared_state.store must be azure_blob, s3, redis or share, got %q", tc.Name, s.Store)
	}
	if s.SharePath != "" && s.StoreOrDefault() != SharedStateStoreShare {
		return fmt.Errorf("case %q: shared_state.share_path is only used by shared_state.store share", tc.Name)
	}
	if !s.CountsStoreCalls() && (s.Action == SharedStateActionStoreCalls || s.Action == SharedStateActionScaleOutIn) {
		return fmt.Errorf("case %q: shared_state.action %s counts store calls in Azurite's access log, so it needs shared_state.store azure_blob", tc.Name, s.Action)
	}
	if s.StoreOrDefault() == SharedStateStoreShare && s.Action == SharedStateActionIsolateReplica {
		return fmt.Errorf("case %q: shared_state.action isolate_replica cuts a replica off the network, and a share is a local mount, so the replica would keep its store", tc.Name)
	}
	seen := map[int]bool{}
	for _, g := range s.StartOrder {
		for _, n := range g {
			if n < 1 || n > s.Replicas {
				return fmt.Errorf("case %q: shared_state.start_order names replica %d, outside 1..%d", tc.Name, n, s.Replicas)
			}
			if seen[n] {
				return fmt.Errorf("case %q: shared_state.start_order names replica %d twice", tc.Name, n)
			}
			seen[n] = true
		}
	}
	switch s.ExpectHealthOrDefault() {
	case "ok", "not_configured", "error":
	default:
		return fmt.Errorf("case %q: shared_state.expect_health must be ok, not_configured or error, got %q", tc.Name, s.ExpectHealth)
	}
	if (s.Action == SharedStateActionKillOwner || s.Action == SharedStateActionMoveThenKill || s.Action == SharedStateActionStopOwner) && len(s.PlacedDevices) == 0 {
		return fmt.Errorf("case %q: shared_state.action %s needs shared_state.placed_devices", tc.Name, s.Action)
	}
	if s.MaxFailoverSeconds < 0 {
		return fmt.Errorf("case %q: shared_state.max_failover_seconds must not be negative", tc.Name)
	}
	gens := map[string]GeneratorConfig{}
	for _, g := range tc.Generators {
		gens[g.ID] = g
	}
	recvs := map[string]bool{}
	for _, r := range tc.Receivers {
		recvs[r.ID] = true
	}
	names := map[string]bool{}
	for i, st := range s.Streams {
		g, ok := gens[st.Generator]
		if !ok {
			return fmt.Errorf("case %q: shared_state.streams[%d] names generator %q, which `generators:` does not define", tc.Name, i, st.Generator)
		}
		// The driver counts loss and duplicates from numbered lines. A sample
		// file is replayed without numbers, so its repeated lines would count
		// as duplicates.
		if g.SampleFile != "" {
			return fmt.Errorf("case %q: shared_state stream %q uses generator %q, which replays sample_file %q; a shared-state stream needs the numbered lines the generator makes itself", tc.Name, st.StreamName(), g.ID, g.SampleFile)
		}
		if !recvs[st.Receiver] {
			return fmt.Errorf("case %q: shared_state.streams[%d] names receiver %q, which `receivers:` does not define", tc.Name, i, st.Receiver)
		}
		if names[st.StreamName()] {
			return fmt.Errorf("case %q: shared_state.streams has two streams named %q", tc.Name, st.StreamName())
		}
		names[st.StreamName()] = true
		if st.MaxLoss < 0 || st.MaxDuplicatePct < 0 || st.MinBaseline < 0 || (st.MaxDuplicates != nil && *st.MaxDuplicates < 0) {
			return fmt.Errorf("case %q: shared_state.streams[%d]: max_loss, max_duplicates, max_duplicate_pct and min_baseline must not be negative", tc.Name, i)
		}
		if st.MaxDuplicates != nil && st.MaxDuplicatePct > 0 {
			return fmt.Errorf("case %q: shared_state.streams[%d] sets both max_duplicates and max_duplicate_pct; set one", tc.Name, i)
		}
	}
	for i, v := range s.SecretValues {
		if len(v) < 8 {
			return fmt.Errorf("case %q: shared_state.secret_values[%d] is shorter than 8 characters, which would match by chance", tc.Name, i)
		}
	}
	for i, v := range s.ExpectLogOnce {
		if len(v) < 8 {
			return fmt.Errorf("case %q: shared_state.expect_log_once[%d] is shorter than 8 characters, which would match by chance", tc.Name, i)
		}
	}
	for _, name := range s.OutageStreams {
		if !names[name] {
			return fmt.Errorf("case %q: shared_state.outage_streams names %q, which shared_state.streams does not define", tc.Name, name)
		}
	}
	if len(s.OutageStreams) > 0 {
		switch s.Action {
		case SharedStateActionPauseStore, SharedStateActionIsolateReplica:
			// The driver counts the outage streams first 15 seconds into the
			// outage, and judges their growth from there to its end.
			if s.OutageOrDefault() < MinOutageForStreamsSeconds {
				return fmt.Errorf("case %q: shared_state.outage_streams needs outage_seconds of %d or more: the streams are counted from 15 seconds into the outage, so a shorter outage leaves too little to judge", tc.Name, MinOutageForStreamsSeconds)
			}
		default:
			return fmt.Errorf("case %q: shared_state.outage_streams is only judged by the pause_store and isolate_replica actions", tc.Name)
		}
	}
	switch s.Action {
	case SharedStateActionWrongCredentials:
		if s.BadReplica < 1 || s.BadReplica > s.Replicas {
			return fmt.Errorf("case %q: shared_state.action wrong_credentials needs shared_state.bad_replica in 1..%d", tc.Name, s.Replicas)
		}
	case SharedStateActionScaleOutIn:
		if s.Standby < 1 {
			return fmt.Errorf("case %q: shared_state.action scale_out_in needs a standby replica (shared_state.standby)", tc.Name)
		}
	case SharedStateActionScaleBacklog:
		if !names[s.ScaleStream] {
			return fmt.Errorf("case %q: shared_state.action scale_backlog needs shared_state.scale_stream to name a stream", tc.Name)
		}
	}
	if s.Action == SharedStateActionMoveAcrossCutLink {
		if s.Replicas < 3 {
			return fmt.Errorf("case %q: shared_state.action move_across_cut_link needs three replicas: the leader, the device's owner and the replica the device moves to", tc.Name)
		}
		if s.MoveDevice == 0 || !slices.Contains(s.PlacedDevices, s.MoveDevice) {
			return fmt.Errorf("case %q: shared_state.action move_across_cut_link needs shared_state.move_device, one of placed_devices", tc.Name)
		}
		if !s.SharesBus() {
			return fmt.Errorf("case %q: shared_state.action move_across_cut_link moves a placed device, and with the shared state in memory no device is placed", tc.Name)
		}
	}
	if s.MoveDevice != 0 && s.Action != SharedStateActionMoveAcrossCutLink {
		return fmt.Errorf("case %q: shared_state.move_device is only used by the move_across_cut_link action", tc.Name)
	}
	if s.CutHoldSeconds < 0 {
		return fmt.Errorf("case %q: shared_state.cut_hold_seconds must not be negative", tc.Name)
	}
	if s.BadReplica != 0 && s.Action != SharedStateActionWrongCredentials {
		return fmt.Errorf("case %q: shared_state.bad_replica is only used by the wrong_credentials action", tc.Name)
	}
	if s.OutageHoldOthersSeconds != 0 {
		if s.Action != SharedStateActionPauseStore {
			return fmt.Errorf("case %q: shared_state.outage_hold_others_seconds is only used by the pause_store action", tc.Name)
		}
		if s.OutageHoldOthersSeconds < 0 || s.OutageHoldOthersSeconds > 12 {
			return fmt.Errorf("case %q: shared_state.outage_hold_others_seconds must be 1 to 12: a replica that renews nothing for 15 seconds counts as gone", tc.Name)
		}
	}
	switch s.BadHealthOrDefault() {
	case "ok", "not_configured", "error", "starting":
	default:
		return fmt.Errorf("case %q: shared_state.bad_health must be ok, not_configured, error or starting, got %q", tc.Name, s.BadHealth)
	}
	if s.StateWriteIntervalSeconds != 0 && s.Action != SharedStateActionStoreCalls {
		return fmt.Errorf("case %q: shared_state.state_write_interval_seconds is only judged by the store_calls action", tc.Name)
	}
	if s.OutageSeconds < 0 || s.IdleSeconds < 0 || s.LoadSeconds < 0 || s.MaxStoreWritesPerSec < 0 || s.MaxStoreReadsPerSec < 0 || s.ScaleMinShare < 0 ||
		s.StateWriteIntervalSeconds < 0 || s.StateWriteToleranceSeconds < 0 || s.MoveKillWithinMs < 0 || s.ActionDelaySeconds < 0 {
		return fmt.Errorf("case %q: shared_state's seconds, call rates and shares must not be negative", tc.Name)
	}
	return nil
}

// azureAccountName is what Azurite and Azure accept as a storage account name.
var azureAccountName = regexp.MustCompile(`^[a-z0-9]{3,24}$`)

// AzureAccount is a storage account Azurite serves beside its development
// account, with a key of its own: a test constant, not a secret, that the
// subject must still treat as one.
type AzureAccount struct {
	Name string `yaml:"name"`
	// Key is the account key, in base64 as Azure writes it.
	Key string `yaml:"key"`
}

// AzuriteAccountsEnv returns the AZURITE_ACCOUNTS value that makes Azurite
// serve the extra accounts, or "" when the case declares none. Azurite drops
// its development account once the variable is set, so the value names that
// account too.
func (a *AzureConfig) AzuriteAccountsEnv() string {
	if a == nil || len(a.Accounts) == 0 {
		return ""
	}
	value := AzuriteAccount + ":" + AzuriteKey
	for _, acc := range a.Accounts {
		value += ";" + acc.Name + ":" + acc.Key
	}
	return value
}

func (tc *TestCase) validateAzureAccounts() error {
	seen := map[string]bool{AzuriteAccount: true}
	for i, acc := range tc.Azure.Accounts {
		if !azureAccountName.MatchString(acc.Name) {
			return fmt.Errorf("case %q: azure.accounts[%d].name %q must be 3 to 24 lowercase letters and digits", tc.Name, i, acc.Name)
		}
		if seen[acc.Name] {
			return fmt.Errorf("case %q: azure.accounts names %q twice (or the development account)", tc.Name, acc.Name)
		}
		seen[acc.Name] = true
		if _, err := base64.StdEncoding.DecodeString(acc.Key); err != nil || acc.Key == "" {
			return fmt.Errorf("case %q: azure.accounts[%d].key must be base64, as Azure writes account keys", tc.Name, i)
		}
	}
	return nil
}
