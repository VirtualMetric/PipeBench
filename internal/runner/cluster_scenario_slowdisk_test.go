package runner

import (
	"strings"
	"testing"
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
