package config

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"slices"
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
)

var sharedStateActions = []string{
	SharedStateActionNone,
	SharedStateActionPlacement,
	SharedStateActionKillLeader,
	SharedStateActionKillOwner,
	SharedStateActionStopLeader,
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
	// SecretValues must never appear in a replica's log or in its /health
	// answer: the store's account key, for one. The driver also looks for
	// three 16-character pieces of each, from its start, its middle and its
	// end.
	SecretValues []string `yaml:"secret_values"`
	// Streams say how the data of each generator must arrive at its receiver.
	Streams []SharedStateStream `yaml:"streams"`
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
		return fmt.Errorf("case %q: type director_shared_state_correctness requires an `azure:` block: the replicas keep their shared state in Azurite", tc.Name)
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
	if s.Action == SharedStateActionKillOwner && len(s.PlacedDevices) == 0 {
		return fmt.Errorf("case %q: shared_state.action kill_owner needs shared_state.placed_devices", tc.Name)
	}
	if s.MaxFailoverSeconds < 0 {
		return fmt.Errorf("case %q: shared_state.max_failover_seconds must not be negative", tc.Name)
	}
	gens := map[string]bool{}
	for _, g := range tc.Generators {
		gens[g.ID] = true
	}
	recvs := map[string]bool{}
	for _, r := range tc.Receivers {
		recvs[r.ID] = true
	}
	names := map[string]bool{}
	for i, st := range s.Streams {
		if !gens[st.Generator] {
			return fmt.Errorf("case %q: shared_state.streams[%d] names generator %q, which `generators:` does not define", tc.Name, i, st.Generator)
		}
		if !recvs[st.Receiver] {
			return fmt.Errorf("case %q: shared_state.streams[%d] names receiver %q, which `receivers:` does not define", tc.Name, i, st.Receiver)
		}
		if names[st.StreamName()] {
			return fmt.Errorf("case %q: shared_state.streams has two streams named %q", tc.Name, st.StreamName())
		}
		names[st.StreamName()] = true
		if st.MaxLoss < 0 || st.MaxDuplicatePct < 0 || st.MinBaseline < 0 {
			return fmt.Errorf("case %q: shared_state.streams[%d]: max_loss, max_duplicate_pct and min_baseline must not be negative", tc.Name, i)
		}
	}
	for i, v := range s.SecretValues {
		if len(v) < 8 {
			return fmt.Errorf("case %q: shared_state.secret_values[%d] is shorter than 8 characters, which would match by chance", tc.Name, i)
		}
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
