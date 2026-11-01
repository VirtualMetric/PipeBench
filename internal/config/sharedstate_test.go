package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
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

// TestSharedStateShareServers: a share on an SMB or an NFS server names the
// server's service for pause_store and its image, and a share in a Docker
// volume has neither.
func TestSharedStateShareServers(t *testing.T) {
	for server, want := range map[string]struct {
		service, name, image string
		inVolume             bool
	}{
		"":                        {"", "the share", "", true},
		SharedStateShareServerSMB: {ShareServerService, "the SMB share", "vmetric/bench-share-smb:latest", false},
		SharedStateShareServerNFS: {ShareServerService, "the NFS share", "vmetric/bench-share-nfs:latest", false},
	} {
		tc := sharedStateCase()
		tc.SharedState.Store = SharedStateStoreShare
		tc.SharedState.SharePath = "/mnt/vm-state"
		tc.SharedState.ShareServer = server
		if err := tc.Validate(); err != nil {
			t.Errorf("share server %q: %v", server, err)
		}
		ss := tc.SharedState
		if ss.StoreService() != want.service || ss.StoreName() != want.name || ss.ShareServerImage() != want.image || ss.ShareInVolume() != want.inVolume {
			t.Errorf("share server %q: service %q, name %q, image %q, in a volume %v; want %q, %q, %q, %v", server,
				ss.StoreService(), ss.StoreName(), ss.ShareServerImage(), ss.ShareInVolume(), want.service, want.name, want.image, want.inVolume)
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
		"share server, other store": {func(tc *TestCase) {
			tc.SharedState.ShareServer = SharedStateShareServerSMB
		}, "share_server is only used by shared_state.store share"},
		"unknown share server": {func(tc *TestCase) {
			tc.SharedState.Store = SharedStateStoreShare
			tc.SharedState.SharePath = "/mnt/vm-state"
			tc.SharedState.ShareServer = "webdav"
		}, "share_server must be smb or nfs"},
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

// TestTargetReplicas: kill_receiver needs to know which replica a generator
// sends to. The http_ack generator takes a URL, or a list of them, and a
// host:port reading took http://subject-2:9000/ for the host "http".
func TestTargetReplicas(t *testing.T) {
	for target, want := range map[string][]int{
		"subject-2:9000":                 {2},
		"http://subject-2:9000/":         {2},
		"https://subject-12:8443/ingest": {12},
		"subject-3":                      {3},
		"http://subject-1:9000/, http://subject-3:9000/": {1, 3},
		"subject-2:9000,subject-2:9001":                  {2},
		"http://azurite:10000":                           nil,
		"azurite:10000":                                  nil,
		"subject-x:9000":                                 nil,
		"subject-0:9000":                                 nil,
		"":                                               nil,
	} {
		if got := TargetReplicas(target); !reflect.DeepEqual(got, want) {
			t.Errorf("TargetReplicas(%q) = %v, want %v", target, got, want)
		}
	}
}

// TestKillReceiverValidation: kill_receiver kills the replica a generator
// sends to, so a case whose generators send to no replica is refused.
func TestKillReceiverValidation(t *testing.T) {
	tc := sharedStateCase()
	tc.SharedState.Action = SharedStateActionKillReceiver
	if err := tc.Validate(); err != nil {
		t.Fatalf("a case whose syslog generator sends to subject-2 was refused: %v", err)
	}
	tc.Generators[1] = GeneratorConfig{ID: "syslog", Mode: "http_ack", Target: "http://subject-3:9000/"}
	if err := tc.Validate(); err != nil {
		t.Fatalf("a case whose http_ack generator sends to http://subject-3:9000/ was refused: %v", err)
	}
	for name, target := range map[string]string{
		"sends to no replica":     "relay:9000",
		"sends past the replicas": "http://subject-4:9000/",
	} {
		t.Run(name, func(t *testing.T) {
			tc := sharedStateCase()
			tc.SharedState.Action = SharedStateActionKillReceiver
			tc.Generators[1].Target = target
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), "no generator sends to a replica") {
				t.Fatalf("Validate = %v, want an error that no generator sends to a replica", err)
			}
		})
	}
}

// TestKillAllAndRevisionUpdateValidation: kill_all needs nothing more, and
// replaces every replica, so a stream may count what the replicas took after
// they rejoined. revision_update starts the standby replicas as the new
// revision, so it needs one, and only it uses grace_seconds (VM-8792).
func TestKillAllAndRevisionUpdateValidation(t *testing.T) {
	tc := sharedStateCase()
	tc.SharedState.Action = SharedStateActionKillAll
	if err := tc.Validate(); err != nil {
		t.Fatalf("a kill_all case was refused: %v", err)
	}

	for _, action := range []string{SharedStateActionSlowStore, SharedStateActionSplitLeader} {
		tc = sharedStateCase()
		tc.SharedState.Action = action
		tc.SharedState.OutageSeconds = 60
		tc.SharedState.OutageStreams = []string{tc.SharedState.Streams[0].StreamName()}
		if action == SharedStateActionSlowStore {
			tc.SharedState.StoreDelayMs = 500
		}
		if err := tc.Validate(); err != nil {
			t.Fatalf("a %s case with outage streams was refused: %v", action, err)
		}
	}
	if got := (&SharedStateConfig{}).StoreDelayOrDefault(); got != 200*time.Millisecond {
		t.Errorf("StoreDelayOrDefault without store_delay_ms = %v, want 200ms", got)
	}

	tc = sharedStateCase()
	tc.SharedState.Action = SharedStateActionPauseReceivers
	tc.SharedState.OutageSeconds = 120
	if err := tc.Validate(); err != nil {
		t.Fatalf("a pause_receivers case was refused: %v", err)
	}

	tc = sharedStateCase()
	tc.SharedState.Action = SharedStateActionRevisionUpdate
	tc.SharedState.GraceSeconds = 30
	if err := tc.Validate(); err != nil {
		t.Fatalf("a revision_update case with a standby replica was refused: %v", err)
	}
	if got := tc.SharedState.GraceOrDefault(); got != 30 {
		t.Errorf("GraceOrDefault = %d, want 30", got)
	}
	tc.SharedState.GraceSeconds = 0
	if got := tc.SharedState.GraceOrDefault(); got != 30 {
		t.Errorf("GraceOrDefault without grace_seconds = %d, want the default of 30", got)
	}

	for name, c := range map[string]struct {
		change func(*TestCase)
		want   string
	}{
		"revision update without a standby": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionRevisionUpdate
			tc.SharedState.Standby = 0
		}, "revision_update starts the standby replicas"},
		"grace for another action": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionKillLeader
			tc.SharedState.GraceSeconds = 30
		}, "grace_seconds is only used by the revision_update action"},
		"store delay for another action": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionPauseStore
			tc.SharedState.StoreDelayMs = 500
		}, "store_delay_ms is only used by the slow_store action"},
		"negative store delay": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionSlowStore
			tc.SharedState.StoreDelayMs = -1
		}, "store_delay_ms must not be negative"},
		"slow store that is not Azurite": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionSlowStore
			tc.SharedState.Store = SharedStateStoreShare
			tc.SharedState.SharePath = "/share"
		}, "slow_store slows Azurite down"},
		"copies for another action": {func(tc *TestCase) {
			tc.SharedState.Mode = SharedStateModeStoreCluster
			tc.SharedState.MinCopies = 2
		}, "only used by the scale_out_in action"},
		"copies without a store cluster": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionScaleOutIn
			tc.SharedState.ScaledOutCopies = 3
		}, "they need mode store_cluster"},
		"negative copies": {func(tc *TestCase) {
			tc.SharedState.MinCopies = -1
		}, "must not be negative"},
		"negative grace": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionRevisionUpdate
			tc.SharedState.GraceSeconds = -1
		}, "grace_seconds must not be negative"},
	} {
		t.Run(name, func(t *testing.T) {
			tc := sharedStateCase()
			c.change(tc)
			if err := tc.Validate(); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate = %v, want an error that says %q", err, c.want)
			}
		})
	}
}

// The store_cluster mode is valid, and expects what the embedded mode
// expects of the shared_state component: no state bus (VM-8792).
func TestStoreClusterMode(t *testing.T) {
	tc := sharedStateCase()
	tc.SharedState.Mode = SharedStateModeStoreCluster
	if err := tc.Validate(); err != nil {
		t.Fatalf("a store_cluster case was refused: %v", err)
	}
	if got := tc.SharedState.ExpectHealthOrDefault(); got != "not_configured" {
		t.Errorf("ExpectHealthOrDefault = %q, want not_configured", got)
	}
	if !tc.SharedState.SharesBus() {
		t.Error("a store cluster shares a bus")
	}
	tc.SharedState.Action = SharedStateActionScaleOutIn
	tc.SharedState.MinCopies = 2
	tc.SharedState.ScaledOutCopies = 3
	if err := tc.Validate(); err != nil {
		t.Fatalf("a store_cluster scale_out_in case with copies to judge was refused: %v", err)
	}
	tc.SharedState.Mode = "cluster"
	if err := tc.Validate(); err == nil || !strings.Contains(err.Error(), "store_cluster") {
		t.Fatalf("Validate of an unknown mode = %v, want an error that names store_cluster", err)
	}
}

// TestRevisionUpdateBacklogAndSwitchValidation: backlog_seconds and
// store_cluster_within_seconds belong to revision_update, and the switch starts
// from mode embedded, which is what the old replicas run.
func TestRevisionUpdateBacklogAndSwitchValidation(t *testing.T) {
	base := func() *TestCase {
		tc := sharedStateCase()
		tc.SharedState.Mode = SharedStateModeEmbedded
		tc.SharedState.Action = SharedStateActionRevisionUpdate
		tc.SharedState.Standby = 2
		return tc
	}
	tc := base()
	tc.SharedState.BacklogSeconds = 30
	tc.SharedState.StoreClusterWithinSeconds = 240
	if err := tc.Validate(); err != nil {
		t.Fatalf("a revision_update case that switches to the store cluster with a backlog was refused: %v", err)
	}
	for name, c := range map[string]struct {
		edit func(*TestCase)
		want string
	}{
		"a backlog without the update": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionKillLeader
			tc.SharedState.BacklogSeconds = 30
		}, "backlog_seconds is only used by the revision_update action"},
		"a switch without the update": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionKillLeader
			tc.SharedState.StoreClusterWithinSeconds = 240
		}, "store_cluster_within_seconds is only used by the revision_update action"},
		"a switch from another mode": {func(tc *TestCase) {
			tc.SharedState.Mode = SharedStateModeStoreCluster
			tc.SharedState.StoreClusterWithinSeconds = 240
		}, "needs mode embedded"},
	} {
		t.Run(name, func(t *testing.T) {
			tc := base()
			c.edit(tc)
			if err := tc.Validate(); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate = %v, want an error that says %q", err, c.want)
			}
		})
	}
}
