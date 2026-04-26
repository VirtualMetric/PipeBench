package results

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RunResult is the full per-run record produced by the runner. It is used
// in-memory when a test finishes; what gets persisted to disk is a flatter
// ResultEntry inside a SubjectFile (see below).
type RunResult struct {
	TestName    string    `json:"test_name"`
	Config      string    `json:"config"`
	Subject     string    `json:"subject"`
	Version     string    `json:"version"`
	Timestamp   time.Time `json:"timestamp"`
	DurationSec float64   `json:"duration_secs"`

	// Hardware identity — set via BENCH_HARDWARE env var or the
	// `--hardware` flag on `harness test` (e.g. "c7i.4xlarge", "m7i.8xlarge").
	// Defaults to "custom" for unlabeled local runs. Results live under
	// results/<hardware>/<subject>.json so the PipeBench UI can present
	// one tab per hardware tier and anybody can drop a downloaded bundle
	// into the repo.
	Hardware string `json:"hardware,omitempty"`

	// Throughput
	LinesIn     int64   `json:"lines_in"`
	LinesOut    int64   `json:"lines_out"`
	BytesIn     int64   `json:"bytes_in"`
	BytesOut    int64   `json:"bytes_out"`
	LinesPerSec float64 `json:"lines_per_sec"`
	LossPercent float64 `json:"loss_percent"`

	// Resource usage (aggregated from metrics.csv during the run)
	AvgCPUPercent float64 `json:"avg_cpu_percent"`
	MaxCPUPercent float64 `json:"max_cpu_percent"`
	AvgMemMB      float64 `json:"avg_mem_mb"`
	MaxMemMB      float64 `json:"max_mem_mb"`

	// Disk I/O (total bytes over test duration)
	DiskReadBytes  int64 `json:"disk_read_bytes"`
	DiskWriteBytes int64 `json:"disk_write_bytes"`

	// Network I/O (total bytes over test duration)
	NetRecvBytes int64 `json:"net_recv_bytes"`
	NetSendBytes int64 `json:"net_send_bytes"`

	// IO throughput (average bytes/sec across disk + network)
	IOThroughputAvg float64 `json:"io_throughput_avg_bytes_per_sec"`

	// Load averages (last sample)
	LoadAvg1  float64 `json:"load_avg_1"`
	LoadAvg5  float64 `json:"load_avg_5"`
	LoadAvg15 float64 `json:"load_avg_15"`

	// System info
	SystemCPUs  int   `json:"system_cpus"`
	SystemMemMB int64 `json:"system_mem_mb"`

	// Subject resource limits (empty = no limit)
	SubjectCPULimit string `json:"subject_cpu_limit,omitempty"`
	SubjectMemLimit string `json:"subject_mem_limit,omitempty"`

	// Latency (p50/p95/p99 in milliseconds, 0 if not measured)
	LatencyP50Ms float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95Ms float64 `json:"latency_p95_ms,omitempty"`
	LatencyP99Ms float64 `json:"latency_p99_ms,omitempty"`

	// Correctness only — nil means not applicable.
	Passed     *bool  `json:"passed,omitempty"`
	FailReason string `json:"fail_reason,omitempty"`

	// MetricsFile used to point at a copy of metrics.csv; with the
	// subject-file layout we don't archive the raw CSV per run any more.
	// Kept on the struct so old code paths compile; always empty now.
	MetricsFile string `json:"metrics_file,omitempty"`
}

// SubjectFile is the on-disk representation at results/<hardware>/<subject>.json.
// One file per (hardware, subject). Re-running a test REPLACES the matching
// (test, config) entry — no per-run history is retained. The `version` field
// here is the latest subject image version seen (usually stable, but updates
// if you bump the subject image).
type SubjectFile struct {
	Hardware    string        `json:"hardware"`
	Subject     string        `json:"subject"`
	Version     string        `json:"version,omitempty"`
	GeneratedAt time.Time     `json:"generated_at"`
	Results     []ResultEntry `json:"results"`
}

// ResultEntry is one (test, config) row inside a SubjectFile. It omits
// the redundant subject/hardware/version fields that live on the parent.
type ResultEntry struct {
	Test        string    `json:"test"`
	Config      string    `json:"config"`
	Timestamp   time.Time `json:"timestamp"`
	DurationSec float64   `json:"duration_secs"`

	LinesIn     int64   `json:"lines_in"`
	LinesOut    int64   `json:"lines_out"`
	BytesIn     int64   `json:"bytes_in"`
	BytesOut    int64   `json:"bytes_out"`
	LinesPerSec float64 `json:"lines_per_sec"`
	LossPercent float64 `json:"loss_percent"`

	AvgCPUPercent float64 `json:"avg_cpu_percent"`
	MaxCPUPercent float64 `json:"max_cpu_percent"`
	AvgMemMB      float64 `json:"avg_mem_mb"`
	MaxMemMB      float64 `json:"max_mem_mb"`

	DiskReadBytes  int64 `json:"disk_read_bytes"`
	DiskWriteBytes int64 `json:"disk_write_bytes"`
	NetRecvBytes   int64 `json:"net_recv_bytes"`
	NetSendBytes   int64 `json:"net_send_bytes"`

	IOThroughputAvg float64 `json:"io_throughput_avg_bytes_per_sec"`

	LoadAvg1  float64 `json:"load_avg_1"`
	LoadAvg5  float64 `json:"load_avg_5"`
	LoadAvg15 float64 `json:"load_avg_15"`

	SystemCPUs  int   `json:"system_cpus,omitempty"`
	SystemMemMB int64 `json:"system_mem_mb,omitempty"`

	SubjectCPULimit string `json:"subject_cpu_limit,omitempty"`
	SubjectMemLimit string `json:"subject_mem_limit,omitempty"`

	LatencyP50Ms float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95Ms float64 `json:"latency_p95_ms,omitempty"`
	LatencyP99Ms float64 `json:"latency_p99_ms,omitempty"`

	Passed     *bool  `json:"passed,omitempty"`
	FailReason string `json:"fail_reason,omitempty"`
}

// Store manages writing the subject-file layout.
type Store struct {
	BaseDir string
	mu      sync.Mutex // guards concurrent Save() calls in the same process
}

// NewStore creates a Store rooted at baseDir.
func NewStore(baseDir string) *Store {
	return &Store{BaseDir: baseDir}
}

// SubjectPath returns the on-disk path for a (hardware, subject) file.
func (s *Store) SubjectPath(hardware, subject string) string {
	if hardware == "" {
		hardware = "custom"
	}
	return filepath.Join(s.BaseDir, hardware, subject+".json")
}

// EnsureSubjectFile guarantees that <hw>/<subject>.json exists, even if
// no test has produced a successful entry yet. The harness calls this
// before running a subject's tests so that subjects which fail every
// run still appear in the UI (as a row of ☠ markers across the tests)
// rather than being silently absent from the index.
//
// If the file already exists, it's left intact — only the version field
// gets refreshed when the caller has a non-empty value, so a re-run
// against a newer image bumps the metadata without dropping existing
// results.
func (s *Store) EnsureSubjectFile(hardware, subject, version string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if hardware == "" {
		hardware = "custom"
	}
	dir := filepath.Join(s.BaseDir, hardware)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating subject dir: %w", err)
	}
	path := filepath.Join(dir, subject+".json")

	var sf SubjectFile
	existed := false
	if data, err := os.ReadFile(path); err == nil {
		if jerr := json.Unmarshal(data, &sf); jerr == nil {
			existed = true
		}
	}

	if !existed {
		sf = SubjectFile{Results: []ResultEntry{}}
	}
	sf.Hardware = hardware
	sf.Subject = subject
	if version != "" {
		sf.Version = version
	}
	if sf.Results == nil {
		sf.Results = []ResultEntry{}
	}
	sf.GeneratedAt = time.Now().UTC()

	buf, err := json.MarshalIndent(sf, "", "  ")
	if err != nil {
		return "", err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return "", fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("renaming %s → %s: %w", tmp, path, err)
	}
	return path, nil
}

// Save merges one RunResult into the subject file. Replaces any existing
// entry with matching (test, config) — re-running the same test overwrites
// the previous row in place. Appends if no match. The metrics CSV argument
// is accepted for backwards-compat with callers but is NOT persisted;
// CPU/mem summary fields on RunResult are authoritative now.
//
// Returns the absolute path of the written subject file.
func (s *Store) Save(r RunResult, _unusedMetricsCSVSrc string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hw := r.Hardware
	if hw == "" {
		hw = "custom"
	}
	dir := filepath.Join(s.BaseDir, hw)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating subject dir: %w", err)
	}
	path := filepath.Join(dir, r.Subject+".json")

	// Read existing subject file if present (ignore errors — corrupt file
	// gets overwritten rather than halting the run).
	var sf SubjectFile
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &sf)
	}

	entry := ResultEntry{
		Test:            r.TestName,
		Config:          r.Config,
		Timestamp:       r.Timestamp,
		DurationSec:     r.DurationSec,
		LinesIn:         r.LinesIn,
		LinesOut:        r.LinesOut,
		BytesIn:         r.BytesIn,
		BytesOut:        r.BytesOut,
		LinesPerSec:     r.LinesPerSec,
		LossPercent:     r.LossPercent,
		AvgCPUPercent:   r.AvgCPUPercent,
		MaxCPUPercent:   r.MaxCPUPercent,
		AvgMemMB:        r.AvgMemMB,
		MaxMemMB:        r.MaxMemMB,
		DiskReadBytes:   r.DiskReadBytes,
		DiskWriteBytes:  r.DiskWriteBytes,
		NetRecvBytes:    r.NetRecvBytes,
		NetSendBytes:    r.NetSendBytes,
		IOThroughputAvg: r.IOThroughputAvg,
		LoadAvg1:        r.LoadAvg1,
		LoadAvg5:        r.LoadAvg5,
		LoadAvg15:       r.LoadAvg15,
		SystemCPUs:      r.SystemCPUs,
		SystemMemMB:     r.SystemMemMB,
		SubjectCPULimit: r.SubjectCPULimit,
		SubjectMemLimit: r.SubjectMemLimit,
		LatencyP50Ms:    r.LatencyP50Ms,
		LatencyP95Ms:    r.LatencyP95Ms,
		LatencyP99Ms:    r.LatencyP99Ms,
		Passed:          r.Passed,
		FailReason:      r.FailReason,
	}

	// Replace matching (test, config) or append.
	replaced := false
	for i, e := range sf.Results {
		if e.Test == entry.Test && e.Config == entry.Config {
			sf.Results[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		sf.Results = append(sf.Results, entry)
	}

	sf.Hardware = hw
	sf.Subject = r.Subject
	if r.Version != "" {
		sf.Version = r.Version
	}
	sf.GeneratedAt = time.Now().UTC()

	buf, err := json.MarshalIndent(sf, "", "  ")
	if err != nil {
		return "", err
	}
	// Atomic-ish write: write tmp then rename.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return "", fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("renaming %s → %s: %w", tmp, path, err)
	}
	return path, nil
}
