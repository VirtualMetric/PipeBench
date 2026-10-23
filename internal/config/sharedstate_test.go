package config

import (
	"reflect"
	"strings"
	"testing"
)

func sharedStateCase() *TestCase {
	return &TestCase{
		Name:  "director_shared_state_test",
		Type:  "director_shared_state_correctness",
		Azure: &AzureConfig{Containers: []string{"bench-in"}},
		Generators: []GeneratorConfig{
			{ID: "blob", Mode: "azure_blob", Target: "http://azurite:10000"},
			{ID: "syslog", Mode: "tcp", Target: "subject-2:9000"},
		},
		Receivers: []ReceiverConfig{
			{ID: "blob", Mode: "tcp", Listen: ":9001"},
			{ID: "syslog", Mode: "tcp", Listen: ":9001"},
		},
		SharedState: &SharedStateConfig{
			Replicas:      3,
			Standby:       1,
			Action:        SharedStateActionKillLeader,
			PlacedDevices: []int64{11, 12, 13},
			StartOrder:    [][]int{{1}},
			Streams: []SharedStateStream{
				{Generator: "blob", Receiver: "blob"},
				{Name: "listener", Generator: "syslog", Receiver: "syslog"},
			},
		},
	}
}

func TestSharedStateValidates(t *testing.T) {
	tc := sharedStateCase()
	if err := tc.Validate(); err != nil {
		t.Fatalf("a valid case was refused: %v", err)
	}
	if got := tc.ReplicaNodes(); got != 4 {
		t.Errorf("ReplicaNodes = %d, want 4: three started and one standby", got)
	}
	if tc.StandbyReplica(3) || !tc.StandbyReplica(4) {
		t.Error("only the replica after the started ones is a standby")
	}
	if got := tc.SharedState.StartGroups(); !reflect.DeepEqual(got, [][]int{{1}, {2, 3}}) {
		t.Errorf("StartGroups = %v, want [[1] [2 3]]", got)
	}
	if got := tc.SharedState.ExpectHealthOrDefault(); got != "ok" {
		t.Errorf("a state bus expects shared_state ok, got %q", got)
	}
	tc.SharedState.Mode = SharedStateModeEmbedded
	if got := tc.SharedState.ExpectHealthOrDefault(); got != "not_configured" {
		t.Errorf("an embedded queue runs no state bus, so /health says not_configured; got %q", got)
	}
	if !tc.SharedState.SharesBus() {
		t.Error("an embedded queue shares a bus")
	}
	tc.SharedState.Mode = SharedStateModeMemory
	if tc.SharedState.SharesBus() {
		t.Error("memory shares no bus")
	}
}

// The driver counts loss from unique lines, so a shared-state case tracks
// them even when its file does not turn on dedup validation. Without them,
// every line sent would count as lost.
func TestSharedStateTracksUniqueLines(t *testing.T) {
	tc := sharedStateCase()
	if tc.Correctness.ValidateDedup {
		t.Fatal("the test case must leave dedup validation off")
	}
	if !tc.TracksUniqueLines() {
		t.Error("a shared-state case does not track unique lines")
	}
}

func TestSharedStateRefusals(t *testing.T) {
	cases := map[string]struct {
		change func(*TestCase)
		want   string
	}{
		"no block":          {func(tc *TestCase) { tc.SharedState = nil }, "requires a `shared_state:` block"},
		"block, other type": {func(tc *TestCase) { tc.Type = "correctness" }, "only valid for type director_shared_state_correctness"},
		"no azure":          {func(tc *TestCase) { tc.Azure = nil }, "requires an `azure:` block"},
		"with cluster":      {func(tc *TestCase) { tc.Cluster = &ClusterConfig{Nodes: 3} }, "only valid for type director_cluster_correctness"},
		"one replica":       {func(tc *TestCase) { tc.SharedState.Replicas = 1 }, "at least 2"},
		"unknown action":    {func(tc *TestCase) { tc.SharedState.Action = "reboot" }, "unknown shared_state.action"},
		"unknown mode":      {func(tc *TestCase) { tc.SharedState.Mode = "disk" }, "shared_state.mode must be"},
		"start order range": {func(tc *TestCase) { tc.SharedState.StartOrder = [][]int{{4}} }, "outside 1..3"},
		"start order twice": {func(tc *TestCase) { tc.SharedState.StartOrder = [][]int{{1}, {1}} }, "twice"},
		"bad health":        {func(tc *TestCase) { tc.SharedState.ExpectHealth = "green" }, "expect_health must be"},
		"kill owner, no devices": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionKillOwner
			tc.SharedState.PlacedDevices = nil
		}, "needs shared_state.placed_devices"},
		"unknown generator": {func(tc *TestCase) { tc.SharedState.Streams[0].Generator = "kafka" }, "names generator \"kafka\""},
		"unknown receiver":  {func(tc *TestCase) { tc.SharedState.Streams[0].Receiver = "s3" }, "names receiver \"s3\""},
		"same stream twice": {func(tc *TestCase) { tc.SharedState.Streams[1].Name = "blob" }, "two streams named"},
		"short secret":      {func(tc *TestCase) { tc.SharedState.SecretValues = []string{"abc"} }, "shorter than 8"},
		"bad account":       {func(tc *TestCase) { tc.Azure.Accounts = []AzureAccount{{Name: "State_Store", Key: "a2V5"}} }, "lowercase letters and digits"},
		"account key":       {func(tc *TestCase) { tc.Azure.Accounts = []AzureAccount{{Name: "statestore", Key: "not base64!"}} }, "must be base64"},
		"dev account again": {func(tc *TestCase) { tc.Azure.Accounts = []AzureAccount{{Name: AzuriteAccount, Key: "a2V5"}} }, "twice"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := sharedStateCase()
			c.change(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func TestAzuriteAccountsEnv(t *testing.T) {
	a := &AzureConfig{Containers: []string{"x"}}
	if got := a.AzuriteAccountsEnv(); got != "" {
		t.Errorf("no extra account: got %q, want empty", got)
	}
	a.Accounts = []AzureAccount{{Name: "statestore", Key: "c2VjcmV0"}}
	want := AzuriteAccount + ":" + AzuriteKey + ";statestore:c2VjcmV0"
	if got := a.AzuriteAccountsEnv(); got != want {
		t.Errorf("got %q, want %q: Azurite drops its development account once the variable is set", got, want)
	}
}
