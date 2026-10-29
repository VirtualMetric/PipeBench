package config

import (
	"strings"
	"testing"
)

// upgradeScenarioCase is one director that starts on an earlier release,
// stops, and starts again on the image under test, on the same data.
func upgradeScenarioCase() *TestCase {
	return &TestCase{
		Name:      "upgrade",
		Type:      ClusterScenarioType,
		Generator: GeneratorConfig{Mode: "http_ack", Target: "http://subject-1:9000/", Connections: 4, Rate: 250},
		Receiver:  ReceiverConfig{Mode: "http", Listen: ":9001"},
		Cluster: &ClusterConfig{Nodes: 1, Scenario: &ScenarioConfig{
			StartImage: "vmetric/director:2.6.0",
			NodeVolume: "/opt/vmetric/storage",
			Steps: []ScenarioStep{
				{Do: "wait_ready"},
				{Do: "start_load"},
				{Do: "sleep", For: "30s"},
				{Do: "stop", Node: "1"},
				{Do: "start", Node: "1", Image: "subject"},
				{Do: "wait_healthy"},
				{Do: "stop", Node: "1"},
				{Do: "start", Node: "1", Image: "vmetric/director:2.6.0"},
			},
		}},
	}
}

func TestValidateScenarioImagesAcceptsAnUpgrade(t *testing.T) {
	if err := upgradeScenarioCase().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	// A rolling restart can move every node to another image.
	rolling := upgradeScenarioCase()
	rolling.Cluster.Nodes = 3
	rolling.Cluster.Scenario.Steps = []ScenarioStep{{Do: "wait_ready"}, {Do: "rolling_restart", Image: "subject"}}
	if err := rolling.Validate(); err != nil {
		t.Fatalf("Validate a rolling move: %v", err)
	}
	// Without start_image the nodes start on the image under test, and a
	// step may still move them to another one.
	down := upgradeScenarioCase()
	down.Cluster.Scenario.StartImage = ""
	down.Cluster.Scenario.Steps[4].Image = "vmetric/director:2.6.0"
	if err := down.Validate(); err != nil {
		t.Fatalf("Validate a move away from the image under test: %v", err)
	}
	for _, ref := range []string{
		"vmetric/director:2.6.0",
		"vmetric/director-enterprise:vm8615-270-7c5cbcde12",
		"localhost:5000/team/director:1.2",
		"director@sha256:" + strings.Repeat("a", 64),
	} {
		if err := checkImageRef(ref); err != nil {
			t.Errorf("checkImageRef(%q) = %v, want nil", ref, err)
		}
	}
}

func TestValidateScenarioImagesRejects(t *testing.T) {
	cases := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"a start image without a tag": {func(tc *TestCase) { tc.Cluster.Scenario.StartImage = "vmetric/director" }, "names no tag"},
		"a start image with a space": {func(tc *TestCase) {
			tc.Cluster.Scenario.StartImage = "vmetric/director:2.6.0 --privileged"
		}, "is not an image reference"},
		"a start image with a variable": {func(tc *TestCase) {
			tc.Cluster.Scenario.StartImage = "vmetric/director:${TAG}"
		}, "is not an image reference"},
		"a start image no step leaves for the image under test": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[4].Image = "vmetric/director:2.6.1"
		}, "no step moves a node to the image under test"},
		"a step image without a tag": {func(tc *TestCase) { tc.Cluster.Scenario.Steps[7].Image = "vmetric/director" }, "names no tag"},
		"an image without a volume":  {func(tc *TestCase) { tc.Cluster.Scenario.NodeVolume = "" }, "set cluster.scenario.node_volume"},
		"an image on a stop":         {func(tc *TestCase) { tc.Cluster.Scenario.Steps[3].Image = "subject" }, "image only applies to rolling_restart, start"},
		"an image with empty_disk": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[4].EmptyDisk = true
		}, "a case uses one of them"},
		"empty_disk beside a volume": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps, ScenarioStep{Do: "stop", Node: "1"}, ScenarioStep{Do: "start", Node: "1", EmptyDisk: true})
		}, "a case uses one of them"},
		"a relative volume path":     {func(tc *TestCase) { tc.Cluster.Scenario.NodeVolume = "storage" }, "absolute path"},
		"a volume path climbing out": {func(tc *TestCase) { tc.Cluster.Scenario.NodeVolume = "/opt/../etc" }, "'..'"},
		"a volume where the small disk is": {func(tc *TestCase) {
			tc.Cluster.Scenario.NodeDisk = &ScenarioNodeDisk{Path: "/opt/vmetric/storage/", Size: "1g"}
		}, "not both"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := upgradeScenarioCase()
			c.edit(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, c.want)
			}
		})
	}
}

// TestValidateSlowDisk checks the slow_disk and restore_disk steps: they
// limit the disk that holds a node's volume, so they need node_volume, and a
// slow_disk step says how fast the disk may be.
func TestValidateSlowDisk(t *testing.T) {
	withSteps := func(steps ...ScenarioStep) *TestCase {
		tc := upgradeScenarioCase()
		tc.Cluster.Scenario.StartImage = ""
		tc.Cluster.Scenario.Steps = append([]ScenarioStep{{Do: "wait_ready"}, {Do: "start_load"}}, steps...)
		return tc
	}
	good := withSteps(
		ScenarioStep{Do: "slow_disk", Node: "1", DiskBPS: "1m", DiskIOPS: 50, As: "slow"},
		ScenarioStep{Do: "sleep", For: "30s"},
		ScenarioStep{Do: "restore_disk", Node: "@slow"},
	)
	if err := good.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := withSteps(ScenarioStep{Do: "slow_disk", Node: "1", DiskIOPS: 20}).Validate(); err != nil {
		t.Fatalf("Validate with disk_iops alone: %v", err)
	}
	bad := map[string]struct {
		tc   *TestCase
		want string
	}{
		"no limit":         {withSteps(ScenarioStep{Do: "slow_disk", Node: "1"}), "needs disk_bps, disk_iops or both"},
		"a bad size":       {withSteps(ScenarioStep{Do: "slow_disk", Node: "1", DiskBPS: "fast"}), "disk_bps"},
		"a negative count": {withSteps(ScenarioStep{Do: "slow_disk", Node: "1", DiskIOPS: -5}), "disk_iops must be above 0"},
		"no node":          {withSteps(ScenarioStep{Do: "slow_disk", DiskBPS: "1m"}), "needs node"},
		"a limit on restore": {withSteps(ScenarioStep{Do: "restore_disk", Node: "1", DiskBPS: "1m"}),
			"disk_bps only applies to slow_disk"},
		"no volume": {func() *TestCase {
			tc := withSteps(ScenarioStep{Do: "slow_disk", Node: "1", DiskBPS: "1m"})
			tc.Cluster.Scenario.NodeVolume = ""
			return tc
		}(), "needs cluster.scenario.node_volume"},
	}
	for name, c := range bad {
		err := c.tc.Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Validate = %v, want an error with %q", name, err, c.want)
		}
	}
}

// TestValidatePauseLoadAtStop checks rolling_restart's pause_load_at_stop.
func TestValidatePauseLoadAtStop(t *testing.T) {
	rolling := upgradeScenarioCase()
	rolling.Cluster.Nodes = 3
	rolling.Cluster.Scenario.Steps = []ScenarioStep{{Do: "wait_ready"}, {Do: "start_load"}, {Do: "rolling_restart", Image: "subject", PauseLoadAtStop: true}}
	if err := rolling.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	bad := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"on another step": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[2] = ScenarioStep{Do: "start", Node: "1", Image: "subject", PauseLoadAtStop: true}
		}, "pause_load_at_stop only applies to rolling_restart"},
		"without a load": {func(tc *TestCase) { tc.Generator = GeneratorConfig{} }, "has no generator"},
		"while paused": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps = []ScenarioStep{{Do: "wait_ready"}, {Do: "start_load"}, {Do: "pause_load"},
				{Do: "rolling_restart", Image: "subject", PauseLoadAtStop: true}, {Do: "resume_load"}}
		}, "paused already"},
	}
	for name, c := range bad {
		tc := upgradeScenarioCase()
		tc.Cluster.Nodes = 3
		tc.Cluster.Scenario.Steps = []ScenarioStep{{Do: "wait_ready"}, {Do: "start_load"}, {Do: "rolling_restart", Image: "subject", PauseLoadAtStop: true}}
		c.edit(tc)
		err := tc.Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Validate = %v, want an error with %q", name, err, c.want)
		}
	}
}
