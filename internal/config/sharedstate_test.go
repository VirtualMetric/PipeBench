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

// TestSharedStateStores: each store names the service pause_store takes
// away, and only Azurite lets the driver count the store calls.
func TestSharedStateStores(t *testing.T) {
	for store, want := range map[string]struct {
		service, name string
		counts        bool
		set           func(tc *TestCase)
	}{
		"":                        {"azurite", "Azurite", true, func(*TestCase) {}},
		SharedStateStoreS3:        {"minio", "MinIO", false, func(tc *TestCase) { tc.Minio = &MinioConfig{Buckets: []string{"director-state"}} }},
		SharedStateStoreRedis:     {"redis", "Redis", false, func(tc *TestCase) { tc.Endpoints = []Endpoint{{Name: "redis", Image: "redis:7-alpine"}} }},
		SharedStateStoreShare:     {"", "the share", false, func(tc *TestCase) { tc.SharedState.SharePath = "/mnt/vm-state" }},
		SharedStateStoreAzureBlob: {"azurite", "Azurite", true, func(*TestCase) {}},
	} {
		tc := sharedStateCase()
		tc.SharedState.Store = store
		want.set(tc)
		if err := tc.Validate(); err != nil {
			t.Errorf("store %q: %v", store, err)
		}
		ss := tc.SharedState
		if ss.StoreService() != want.service || ss.StoreName() != want.name || ss.CountsStoreCalls() != want.counts {
			t.Errorf("store %q: service %q, name %q, counts %v; want %q, %q, %v", store, ss.StoreService(), ss.StoreName(), ss.CountsStoreCalls(), want.service, want.name, want.counts)
		}
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
		"sample file":       {func(tc *TestCase) { tc.Generators[1].SampleFile = "samples/syslog.log" }, "stream \"listener\" uses generator \"syslog\", which replays sample_file"},
		"same stream twice": {func(tc *TestCase) { tc.SharedState.Streams[1].Name = "blob" }, "two streams named"},
		"short secret":      {func(tc *TestCase) { tc.SharedState.SecretValues = []string{"abc"} }, "shorter than 8"},
		"bad account":       {func(tc *TestCase) { tc.Azure.Accounts = []AzureAccount{{Name: "State_Store", Key: "a2V5"}} }, "lowercase letters and digits"},
		"account key":       {func(tc *TestCase) { tc.Azure.Accounts = []AzureAccount{{Name: "statestore", Key: "not base64!"}} }, "must be base64"},
		"dev account again": {func(tc *TestCase) { tc.Azure.Accounts = []AzureAccount{{Name: AzuriteAccount, Key: "a2V5"}} }, "twice"},
		"outage stream":     {func(tc *TestCase) { tc.SharedState.OutageStreams = []string{"nope"} }, "outage_streams names \"nope\""},
		"outage stream, no outage": {func(tc *TestCase) {
			tc.SharedState.OutageStreams = []string{"listener"}
		}, "only judged by the pause_store and isolate_replica actions"},
		"outage stream, short pause": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionPauseStore
			tc.SharedState.OutageSeconds = 15
			tc.SharedState.OutageStreams = []string{"listener"}
		}, "needs outage_seconds of 20 or more"},
		"outage stream, short isolation": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionIsolateReplica
			tc.SharedState.OutageSeconds = 19
			tc.SharedState.OutageStreams = []string{"listener"}
		}, "needs outage_seconds of 20 or more"},
		"wrong credentials, no replica": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionWrongCredentials
		}, "needs shared_state.bad_replica"},
		"bad replica, other action": {func(tc *TestCase) { tc.SharedState.BadReplica = 2 }, "only used by the wrong_credentials action"},
		"scale out, no standby": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionScaleOutIn
			tc.SharedState.Standby = 0
		}, "needs a standby replica"},
		"scale backlog, no stream": {func(tc *TestCase) { tc.SharedState.Action = SharedStateActionScaleBacklog }, "scale_stream to name a stream"},
		"bad replica health":       {func(tc *TestCase) { tc.SharedState.BadHealth = "red" }, "bad_health must be"},
		"negative outage":          {func(tc *TestCase) { tc.SharedState.OutageSeconds = -1 }, "must not be negative"},
		"hold, other action":       {func(tc *TestCase) { tc.SharedState.OutageHoldOthersSeconds = 5 }, "only used by the pause_store action"},
		"hold too long": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionPauseStore
			tc.SharedState.OutageHoldOthersSeconds = 15
		}, "must be 1 to 12"},
		"negative max_duplicates": {func(tc *TestCase) {
			n := int64(-1)
			tc.SharedState.Streams[0].MaxDuplicates = &n
		}, "must not be negative"},
		"both duplicate bounds": {func(tc *TestCase) {
			n := int64(10)
			tc.SharedState.Streams[0].MaxDuplicates = &n
			tc.SharedState.Streams[0].MaxDuplicatePct = 5
		}, "set one"},
		"interval, other action": {func(tc *TestCase) { tc.SharedState.StateWriteIntervalSeconds = 60 }, "only judged by the store_calls action"},
		"negative interval": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionStoreCalls
			tc.SharedState.StateWriteIntervalSeconds = -60
		}, "must not be negative"},
		"move then kill, no devices": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionMoveThenKill
			tc.SharedState.PlacedDevices = nil
		}, "move_then_kill needs shared_state.placed_devices"},
		"stop owner, no devices": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionStopOwner
			tc.SharedState.PlacedDevices = nil
		}, "stop_owner needs shared_state.placed_devices"},
		"negative action delay": {func(tc *TestCase) { tc.SharedState.ActionDelaySeconds = -1 }, "must not be negative"},
		"short expected line":   {func(tc *TestCase) { tc.SharedState.ExpectLogOnce = []string{"ignores"} }, "expect_log_once[0] is shorter than 8"},
		"unknown store":         {func(tc *TestCase) { tc.SharedState.Store = "nfs" }, "store must be azure_blob, s3, redis or share"},
		"s3 without minio":      {func(tc *TestCase) { tc.SharedState.Store = SharedStateStoreS3 }, "needs a `minio:` block"},
		"redis without redis":   {func(tc *TestCase) { tc.SharedState.Store = SharedStateStoreRedis }, "needs an endpoint named redis"},
		"share without a path":  {func(tc *TestCase) { tc.SharedState.Store = SharedStateStoreShare }, "needs shared_state.share_path"},
		"share at the root": {func(tc *TestCase) {
			tc.SharedState.Store = SharedStateStoreShare
			tc.SharedState.SharePath = "/"
		}, "needs shared_state.share_path"},
		"share path, other store": {func(tc *TestCase) { tc.SharedState.SharePath = "/mnt/vm-state" }, "only used by shared_state.store share"},
		"store calls on s3": {func(tc *TestCase) {
			tc.Minio = &MinioConfig{Buckets: []string{"director-state"}}
			tc.SharedState.Store = SharedStateStoreS3
			tc.SharedState.Action = SharedStateActionStoreCalls
		}, "store_calls counts store calls in Azurite's access log"},
		"scale out on redis": {func(tc *TestCase) {
			tc.Endpoints = []Endpoint{{Name: "redis", Image: "redis:7-alpine"}}
			tc.SharedState.Store = SharedStateStoreRedis
			tc.SharedState.Action = SharedStateActionScaleOutIn
		}, "scale_out_in counts store calls in Azurite's access log"},
		"isolate on a share": {func(tc *TestCase) {
			tc.SharedState.Store = SharedStateStoreShare
			tc.SharedState.SharePath = "/mnt/vm-state"
			tc.SharedState.Action = SharedStateActionIsolateReplica
		}, "a share is a local mount"},
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

func TestOutageStreamsNeedAnOutageToJudge(t *testing.T) {
	for _, outage := range []int{0, 20, 30} {
		tc := sharedStateCase()
		tc.SharedState.Action = SharedStateActionPauseStore
		tc.SharedState.OutageSeconds = outage
		tc.SharedState.OutageStreams = []string{"listener"}
		if err := tc.Validate(); err != nil {
			t.Errorf("outage_seconds %d (0 means the default, 30) with outage_streams was refused: %v", outage, err)
		}
	}
}

func TestMoveAcrossCutLinkValidation(t *testing.T) {
	valid := func() *TestCase {
		tc := sharedStateCase()
		tc.SharedState.Action = SharedStateActionMoveAcrossCutLink
		tc.SharedState.MoveDevice = 12
		return tc
	}
	if err := valid().Validate(); err != nil {
		t.Fatalf("a valid case was refused: %v", err)
	}
	cases := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"two replicas":      {func(tc *TestCase) { tc.SharedState.Replicas = 2 }, "needs three replicas"},
		"no device":         {func(tc *TestCase) { tc.SharedState.MoveDevice = 0 }, "needs shared_state.move_device"},
		"device not placed": {func(tc *TestCase) { tc.SharedState.MoveDevice = 99 }, "one of placed_devices"},
		"device, no move":   {func(tc *TestCase) { tc.SharedState.Action = SharedStateActionKillLeader }, "only used by the move_across_cut_link"},
		"negative cut hold": {func(tc *TestCase) { tc.SharedState.CutHoldSeconds = -1 }, "cut_hold_seconds"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := valid()
			c.edit(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate = %v, want an error about %q", err, c.want)
			}
		})
	}
	if got := (&SharedStateConfig{}).CutHoldOrDefault(); got != 10 {
		t.Fatalf("CutHoldOrDefault = %d, want 10", got)
	}
}
