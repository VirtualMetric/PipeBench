package runner

// The hardware block of every result row. A latency number means little without the
// machine and the disk it came from, and --hardware is only a label that a person types. So each
// row also says what the harness found: the cloud's size for the machine, the CPU, the memory, and
// the disk under the subject's data.

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// cloudInfo is what the cloud's instance metadata says about the machine, read once a process.
var (
	cloudOnce sync.Once
	cloudName string
	cloudSize string
)

// readCloud asks Azure's and then AWS's instance metadata for the machine's size. Each has a
// second to answer; outside a cloud, both stay empty.
func readCloud() (cloud, size string) {
	cloudOnce.Do(func() {
		client := &http.Client{Timeout: time.Second}
		get := func(req *http.Request) string {
			resp, err := client.Do(req)
			if err != nil {
				return ""
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return ""
			}
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			return strings.TrimSpace(string(b))
		}
		if req, err := http.NewRequest(http.MethodGet, "http://169.254.169.254/metadata/instance/compute/vmSize?api-version=2021-02-01&format=text", nil); err == nil {
			req.Header.Set("Metadata", "true")
			if v := get(req); v != "" {
				cloudName, cloudSize = "azure", v
				return
			}
		}
		// AWS IMDSv2: a token first, then the instance type
		req, err := http.NewRequest(http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
		if err != nil {
			return
		}
		req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "60")
		token := get(req)
		if token == "" {
			return
		}
		if req, err := http.NewRequest(http.MethodGet, "http://169.254.169.254/latest/meta-data/instance-type", nil); err == nil {
			req.Header.Set("X-aws-ec2-metadata-token", token)
			if v := get(req); v != "" {
				cloudName, cloudSize = "aws", v
			}
		}
	})
	return cloudName, cloudSize
}

// dockerRoot is Docker's data folder, read once a process: the subject's data is there when the
// run names no --subject-data.
var (
	dockerRootOnce sync.Once
	dockerRootDir  string
)

func dockerRoot() string {
	dockerRootOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.DockerRootDir}}").Output(); err == nil {
			dockerRootDir = strings.TrimSpace(string(out))
		}
	})
	return dockerRootDir
}

// readHardware is hardwareInfo; a test replaces it, so that saving a row asks no cloud and no Docker.
var readHardware = hardwareInfo

// hardwareInfo returns the hardware block of a row whose subject kept its data under dataPath, or
// under Docker's data folder when dataPath is empty.
func hardwareInfo(dataPath string) map[string]any {
	info := map[string]any{"cpus": runtime.NumCPU()}
	if cloud, size := readCloud(); size != "" {
		info["cloud"], info["cloud_vm_size"] = cloud, size
	}
	if runtime.GOOS != "linux" {
		return info
	}
	if model := cpuModel(readFile("/proc/cpuinfo")); model != "" {
		info["cpu_model"] = model
	}
	if mb := memTotalMB(readFile("/proc/meminfo")); mb > 0 {
		info["mem_mb"] = mb
	}
	if k := strings.TrimSpace(readFile("/proc/sys/kernel/osrelease")); k != "" {
		info["kernel"] = k
	}
	if dataPath == "" {
		dataPath = dockerRoot()
	}
	if dataPath == "" {
		return info
	}
	info["data_path"] = dataPath
	if resolved, err := filepath.EvalSymlinks(dataPath); err == nil {
		dataPath = resolved
	}
	m, ok := mountOf(readFile("/proc/self/mountinfo"), dataPath)
	if !ok {
		return info
	}
	info["data_mount"], info["data_fs"], info["data_device"] = m.point, m.fsType, m.source
	if model, rotational, ok := blockDevice("/sys/dev/block/" + m.majorMinor); ok {
		if model != "" {
			info["data_disk_model"] = model
		}
		info["data_disk_rotational"] = rotational
	}
	return info
}

func readFile(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return string(b)
}

// cpuModel returns the first "model name" of /proc/cpuinfo.
func cpuModel(cpuinfo string) string {
	sc := bufio.NewScanner(strings.NewReader(cpuinfo))
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(k) == "model name" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// memTotalMB returns MemTotal of /proc/meminfo in MiB.
func memTotalMB(meminfo string) int64 {
	sc := bufio.NewScanner(strings.NewReader(meminfo))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) >= 2 && f[0] == "MemTotal:" {
			kb, err := strconv.ParseInt(f[1], 10, 64)
			if err != nil {
				return 0
			}
			return kb / 1024
		}
	}
	return 0
}

// mount is one line of /proc/self/mountinfo.
type mount struct {
	majorMinor, point, fsType, source string
}

// mountOf returns the mount that holds target: the one whose mount point is the longest prefix of
// it. mountinfo's paths are Linux paths, so target is cleaned as one, on any host.
func mountOf(mountinfo, target string) (mount, bool) {
	p := path.Clean(filepath.ToSlash(target))
	var best mount
	found := false
	sc := bufio.NewScanner(strings.NewReader(mountinfo))
	for sc.Scan() {
		left, right, ok := strings.Cut(sc.Text(), " - ")
		if !ok {
			continue
		}
		lf, rf := strings.Fields(left), strings.Fields(right)
		if len(lf) < 5 || len(rf) < 2 {
			continue
		}
		point := unescapeMountinfo(lf[4])
		if point != "/" && p != point && !strings.HasPrefix(p, point+"/") {
			continue
		}
		if !found || len(point) > len(best.point) {
			best, found = mount{majorMinor: lf[2], point: point, fsType: rf[0], source: unescapeMountinfo(rf[1])}, true
		}
	}
	return best, found
}

// unescapeMountinfo decodes the octal escapes the kernel writes in /proc/self/mountinfo for a space,
// a tab, a newline or a backslash in a path: /mnt/fast disk is /mnt/fast\040disk there.
func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	octal := func(c byte) bool { return c >= '0' && c <= '7' }
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && octal(s[i+1]) && octal(s[i+2]) && octal(s[i+3]) {
			b.WriteByte(byte(int(s[i+1]-'0')<<6 | int(s[i+2]-'0')<<3 | int(s[i+3]-'0')))
			i += 3
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// blockDevice reads the model and whether the disk spins, for the block device at sysPath
// (/sys/dev/block/<major:minor>). A partition reads its disk's.
func blockDevice(sysPath string) (model string, rotational bool, ok bool) {
	dir, err := filepath.EvalSymlinks(sysPath)
	if err != nil {
		return "", false, false
	}
	if _, err := os.Stat(filepath.Join(dir, "partition")); err == nil {
		dir = filepath.Dir(dir)
	}
	rot := strings.TrimSpace(readFile(filepath.Join(dir, "queue", "rotational")))
	if rot == "" {
		return "", false, false
	}
	return strings.TrimSpace(readFile(filepath.Join(dir, "device", "model"))), rot == "1", true
}
