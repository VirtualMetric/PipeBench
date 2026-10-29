package runner

import (
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

func TestParseMountDevice(t *testing.T) {
	mountinfo := strings.Join([]string{
		"1290 1171 0:183 / / rw,relatime master:1 - overlay overlay rw",
		"1316 1290 8:48 /docker/volumes/x_node-1-data/_data /opt/vmetric/storage rw,relatime - ext4 /dev/sdd rw",
		"1317 1290 0:61 / /opt/vmetric/storage/other rw - tmpfs tmpfs rw",
	}, "\n")
	dev, err := parseMountDevice(mountinfo, "/opt/vmetric/storage")
	if err != nil || dev != "8:48" {
		t.Fatalf("parseMountDevice = %q, %v, want 8:48", dev, err)
	}
	if _, err := parseMountDevice(mountinfo, "/opt/vmetric"); err == nil {
		t.Fatalf("a directory with no mount of its own must be an error")
	}
}

func TestDiskLimitIOMax(t *testing.T) {
	for _, c := range []struct {
		limit diskLimit
		want  string
		str   string
	}{
		{diskLimit{BPS: 1 << 20, IOPS: 50}, "rbps=1048576 wbps=1048576 riops=50 wiops=50", "1.0 MiB/s and 50 operations/s"},
		{diskLimit{IOPS: 20}, "rbps=max wbps=max riops=20 wiops=20", "20 operations/s"},
		{diskLimit{}, "rbps=max wbps=max riops=max wiops=max", "no limit"},
	} {
		if got := c.limit.ioMax(); got != c.want {
			t.Errorf("ioMax(%+v) = %q, want %q", c.limit, got, c.want)
		}
		if got := c.limit.String(); got != c.str {
			t.Errorf("String(%+v) = %q, want %q", c.limit, got, c.str)
		}
	}
	script := diskLimitScript(4242, "8:49", diskLimit{BPS: 1 << 20})
	for _, want := range []string{"/proc/4242/cgroup", "d=8:49", "/sys/dev/block/$d/partition", `echo "$d rbps=1048576 wbps=1048576 riops=max wiops=max" > /sys/fs/cgroup$cg/io.max`} {
		if !strings.Contains(script, want) {
			t.Errorf("the script lacks %q:\n%s", want, script)
		}
	}
}

// A device number goes into a script that runs privileged on the host, so a
// mountinfo line that carries anything else is refused.
func TestParseMountDeviceRefusesWhatIsNotADeviceNumber(t *testing.T) {
	for _, dev := range []string{"$(id)", "8:48;reboot", "8", "8:48:1", "a:b"} {
		line := "1316 1290 " + dev + " /x /opt/vmetric/storage rw - ext4 /dev/sdd rw"
		if got, err := parseMountDevice(line, "/opt/vmetric/storage"); err == nil {
			t.Errorf("device number %q was taken as %q", dev, got)
		}
	}
}

// restore_disk on a node that does not run succeeds: the node's limit went
// with its process, and its next start must not put the limit back.
func TestRestoreDiskOnAStoppedNode(t *testing.T) {
	orig := containerRunningState
	t.Cleanup(func() { containerRunningState = orig })
	containerRunningState = func(string) (bool, error) { return false, nil }

	sr := &scenarioRun{
		containers: []string{"bench-subject-vmetric-1"},
		names:      map[string]int{},
		slowed:     map[int]diskLimit{1: {BPS: 1 << 20}},
		sc:         &config.ScenarioConfig{NodeVolume: "/opt/vmetric/storage"},
	}
	var rep scenarioStepReport
	ok, detail, err := sr.slowDiskAction(3, config.ScenarioStep{Do: "restore_disk", Node: "1"}, &rep)
	if err != nil || !ok {
		t.Fatalf("restore_disk on a stopped node = %v, %q, %v; want success", ok, detail, err)
	}
	if _, kept := sr.slowed[1]; kept {
		t.Fatalf("the limit is still tracked, so the node's next start would put it back")
	}
}
