package runner

// Slow disks. A slow_disk step limits how fast one node may read and write
// the disk that holds its volume (cluster.scenario.node_volume), and a
// restore_disk step lifts the limit again. The limit is the io.max of the
// node's cgroup (Linux cgroup v2): so many bytes and so many operations a
// second on that one block device. The kernel holds the node's writes back
// to that pace, and a sync waits for them, as on a slow or busy disk. Nothing
// in the image under test changes.
//
// Only the host can write a container's cgroup, so the harness runs a
// short-lived helper (the nettool image) privileged, in the host's process
// and cgroup namespaces. The limit belongs to the container: it goes when the
// container stops. The harness puts it back whenever a step starts the node
// again, as it does with link faults.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// diskLimit is how fast a node may use its disk. Zero means no limit.
type diskLimit struct {
	BPS  int64 // bytes a second, for reads and for writes
	IOPS int   // operations a second, for reads and for writes
}

func (l diskLimit) none() bool { return l.BPS <= 0 && l.IOPS <= 0 }

// ioMax returns the io.max settings of the limit, without the device.
func (l diskLimit) ioMax() string {
	val := func(n int64) string {
		if n <= 0 {
			return "max"
		}
		return strconv.FormatInt(n, 10)
	}
	b, o := val(l.BPS), val(int64(l.IOPS))
	return fmt.Sprintf("rbps=%s wbps=%s riops=%s wiops=%s", b, b, o, o)
}

func (l diskLimit) String() string {
	if l.none() {
		return "no limit"
	}
	var parts []string
	if l.BPS > 0 {
		parts = append(parts, fmtBytes(l.BPS)+"/s")
	}
	if l.IOPS > 0 {
		parts = append(parts, strconv.Itoa(l.IOPS)+" operations/s")
	}
	return strings.Join(parts, " and ")
}

// parseMountDevice returns the device number (major:minor) of the file system
// mounted at dir, from /proc/self/mountinfo.
func parseMountDevice(mountinfo, dir string) (string, error) {
	for _, line := range strings.Split(mountinfo, "\n") {
		f := strings.Fields(line)
		if len(f) >= 5 && f[4] == dir {
			return f[2], nil
		}
	}
	return "", fmt.Errorf("nothing is mounted at %s", dir)
}

// volumeDevice returns the device number of the file system mounted at dir
// in a running container.
func volumeDevice(container, dir string) (string, error) {
	out, err := exec.Command("docker", "exec", container, "cat", "/proc/self/mountinfo").Output()
	if err != nil {
		return "", fmt.Errorf("reading the mounts of %s: %w", container, err)
	}
	dev, err := parseMountDevice(string(out), dir)
	if err != nil {
		return "", fmt.Errorf("%s: %w", container, err)
	}
	return dev, nil
}

// diskLimitScript writes limit into the io.max of process pid's cgroup, for
// device dev, and prints the line now in force. A limit applies to a whole
// disk, so a partition is turned into the disk it is on.
func diskLimitScript(pid int, dev string, limit diskLimit) string {
	return fmt.Sprintf(`set -e
cg=$(sed -n 's/^0:://p' /proc/%d/cgroup)
if [ -z "$cg" ]; then echo "the process has no cgroup v2 path" >&2; exit 3; fi
d=%s
if [ -e /sys/dev/block/$d/partition ]; then d=$(cat /sys/dev/block/$d/../dev); fi
echo "$d %s" > /sys/fs/cgroup$cg/io.max
grep "^$d " /sys/fs/cgroup$cg/io.max || echo "$d no limit"`, pid, dev, limit.ioMax())
}

// setDiskLimit puts limit in force for the disk that holds dir in container.
// It returns the io.max line the kernel now holds for the container.
func setDiskLimit(ctx context.Context, image, container, dir string, limit diskLimit) (string, error) {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Pid}}", container).Output()
	if err != nil {
		return "", fmt.Errorf("docker inspect %s: %w", container, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil || pid <= 0 {
		return "", fmt.Errorf("%s is not running (process %q)", container, strings.TrimSpace(string(out)))
	}
	dev, err := volumeDevice(container, dir)
	if err != nil {
		return "", err
	}
	name := fmt.Sprintf("bench-slowdisk-%s-%d-%d", container, os.Getpid(), nettoolSeq.Add(1))
	cctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	res, err := exec.CommandContext(cctx, "docker", "run", "--rm", "--name", name,
		"--privileged", "--pid", "host", "--cgroupns", "host",
		"-v", "/sys/fs/cgroup:/sys/fs/cgroup",
		"--cpus", "0.5", "--memory", "64m",
		image, "-c", diskLimitScript(pid, dev, limit)).CombinedOutput()
	if err != nil {
		_ = exec.Command("docker", "rm", "-f", name).Run()
		return "", fmt.Errorf("setting the disk limit of %s: %v: %s", container, err, strings.TrimSpace(string(res)))
	}
	return strings.TrimSpace(string(res)), nil
}

// slowDiskAction runs slow_disk and restore_disk.
func (sr *scenarioRun) slowDiskAction(i int, st config.ScenarioStep, rep *scenarioStepReport) (bool, string, error) {
	n, err := sr.resolveNode(st.Node)
	if err != nil {
		return false, err.Error(), nil
	}
	rep.Node = n
	if st.As != "" {
		sr.names[st.As] = n
	}
	var limit diskLimit
	if st.Do == "slow_disk" {
		if st.DiskBPS != "" {
			if limit.BPS, err = config.ParseByteSize(st.DiskBPS); err != nil {
				return false, fmt.Sprintf("disk_bps %q: %v", st.DiskBPS, err), nil
			}
		}
		limit.IOPS = st.DiskIOPS
		sr.disrupt(i, "slow_disk", n)
	}
	line, err := setDiskLimit(sr.ctx, sr.r.opts.NettoolImage, sr.containers[n-1], sr.sc.NodeVolume, limit)
	if err != nil {
		return false, err.Error(), nil
	}
	if limit.none() {
		delete(sr.slowed, n)
		return true, fmt.Sprintf("node %d's disk has no limit again (io.max: %s)", n, line), nil
	}
	sr.slowed[n] = limit
	return true, fmt.Sprintf("node %d's disk is limited to %s (io.max: %s)", n, limit, line), nil
}

// reslow puts node n's disk limit back after a step started the node: the
// limit went with the container's process.
func (sr *scenarioRun) reslow(n int) {
	limit, ok := sr.slowed[n]
	if !ok {
		return
	}
	// The case goes on believing the disk is slow, so a failure here fails
	// the run.
	if _, err := setDiskLimit(sr.ctx, sr.r.opts.NettoolImage, sr.containers[n-1], sr.sc.NodeVolume, limit); err != nil {
		sr.failures = append(sr.failures, fmt.Sprintf("could not put node %d's disk limit back after its start: %v", n, err))
	}
}

// restoreDisks lifts every disk limit the steps left in force. A node that
// does not run has no limit left: it went with its process.
func (sr *scenarioRun) restoreDisks(ctx context.Context) {
	for n := 1; n <= len(sr.containers); n++ {
		limit, ok := sr.slowed[n]
		if !ok {
			continue
		}
		delete(sr.slowed, n)
		if !containerRunning(sr.containers[n-1]) {
			continue
		}
		if _, err := setDiskLimit(ctx, sr.r.opts.NettoolImage, sr.containers[n-1], sr.sc.NodeVolume, diskLimit{}); err != nil {
			sr.failures = append(sr.failures, fmt.Sprintf("could not lift node %d's disk limit at the end of the steps: %v", n, err))
			continue
		}
		sr.notes = append(sr.notes, fmt.Sprintf("the steps left node %d's disk limited to %s, which the harness lifted before the drain", n, limit))
	}
}
