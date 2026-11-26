package runner

import "testing"

const testMountinfo = `22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
25 22 0:22 / /proc rw,nosuid shared:12 - proc proc rw
30 22 259:0 / /mnt/nvme rw,relatime shared:20 - xfs /dev/nvme0n1 rw,attr2
31 22 259:5 / /mnt/nvme-other rw,relatime shared:21 - ext4 /dev/nvme1n1 rw
32 22 8:17 / /var/lib/docker rw,relatime shared:22 - ext4 /dev/sdb1 rw
33 22 259:7 / /mnt/fast\040disk rw,relatime shared:23 - ext4 /dev/mapper/fast\040x rw
`

func TestMountOfFindsTheMountThatHoldsAPath(t *testing.T) {
	for _, c := range []struct{ path, point, fs, source, dev string }{
		{"/mnt/nvme/subject-data/latency_case-1", "/mnt/nvme", "xfs", "/dev/nvme0n1", "259:0"},
		{"/mnt/nvme", "/mnt/nvme", "xfs", "/dev/nvme0n1", "259:0"},
		// a mount whose name starts like another's is not inside it
		{"/mnt/nvme-other/x", "/mnt/nvme-other", "ext4", "/dev/nvme1n1", "259:5"},
		{"/var/lib/docker/volumes/", "/var/lib/docker", "ext4", "/dev/sdb1", "8:17"},
		{"/home/farm", "/", "ext4", "/dev/sda1", "8:1"},
		// the kernel writes a space in a path as \040
		{"/mnt/fast disk/run", "/mnt/fast disk", "ext4", "/dev/mapper/fast x", "259:7"},
	} {
		m, ok := mountOf(testMountinfo, c.path)
		if !ok || m.point != c.point || m.fsType != c.fs || m.source != c.source || m.majorMinor != c.dev {
			t.Errorf("%s: %+v ok=%v, want %s %s %s %s", c.path, m, ok, c.point, c.fs, c.source, c.dev)
		}
	}
	if _, ok := mountOf("", "/x"); ok {
		t.Error("an empty mountinfo held a mount")
	}
}

func TestCPUModelAndMemoryAreReadFromProc(t *testing.T) {
	cpuinfo := "processor\t: 0\nvendor_id\t: AuthenticAMD\nmodel name\t: AMD EPYC 7763 64-Core Processor\nflags\t\t: fpu\n\nprocessor\t: 1\nmodel name\t: AMD EPYC 7763 64-Core Processor\n"
	if got := cpuModel(cpuinfo); got != "AMD EPYC 7763 64-Core Processor" {
		t.Errorf("cpuModel = %q", got)
	}
	if got := cpuModel("processor\t: 0\n"); got != "" {
		t.Errorf("cpuModel without a model = %q", got)
	}
	meminfo := "MemTotal:       65863268 kB\nMemFree:        60000000 kB\n"
	if got := memTotalMB(meminfo); got != 64319 {
		t.Errorf("memTotalMB = %d, want 64319", got)
	}
	if got := memTotalMB("MemFree: 1 kB\n"); got != 0 {
		t.Errorf("memTotalMB without MemTotal = %d", got)
	}
}
