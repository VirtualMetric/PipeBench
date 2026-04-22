package results

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// RunResult is written to summary.json after a test run.
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
	// results/<hardware>/… so the PipeBench UI can present one tab per
	// hardware tier and anybody can drop a downloaded bundle into the repo.
	Hardware string `json:"hardware,omitempty"`

	// Throughput
	LinesIn     int64   `json:"lines_in"`
	LinesOut    int64   `json:"lines_out"`
	BytesIn     int64   `json:"bytes_in"`
	BytesOut    int64   `json:"bytes_out"`
	LinesPerSec float64 `json:"lines_per_sec"`
	LossPercent float64 `json:"loss_percent"`

	// Resource usage (aggregated from metrics.csv)
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
	SystemCPUs   int   `json:"system_cpus"`
	SystemMemMB  int64 `json:"system_mem_mb"`

	// Subject resource limits (empty = no limit)
	SubjectCPULimit string `json:"subject_cpu_limit,omitempty"`
	SubjectMemLimit string `json:"subject_mem_limit,omitempty"`

	// Latency (p50/p95/p99 in milliseconds, 0 if not measured)
	LatencyP50Ms float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95Ms float64 `json:"latency_p95_ms,omitempty"`
	LatencyP99Ms float64 `json:"latency_p99_ms,omitempty"`

	// Correctness only — nil means not applicable.
	Passed      *bool  `json:"passed,omitempty"`
	FailReason  string `json:"fail_reason,omitempty"`
	MetricsFile string `json:"metrics_file"`
}

// Store manages writing results to the local filesystem.
type Store struct {
	BaseDir string
}

// NewStore creates a Store rooted at baseDir.
func NewStore(baseDir string) *Store {
	return &Store{BaseDir: baseDir}
}

// Dir returns the results directory for a specific run (does not create it).
// The hardware argument is the top-level tier: results live under
// <BaseDir>/<hardware>/<testName>/<config>/<subject>/<version>/<timestamp>/
// so a downloaded per-tier bundle can be dropped in whole.
func (s *Store) Dir(hardware, testName, configName, subject, version string, ts time.Time) string {
	if hardware == "" {
		hardware = "custom"
	}
	return filepath.Join(
		s.BaseDir,
		hardware,
		testName,
		configName,
		subject,
		version,
		ts.UTC().Format("2006-01-02T150405Z"),
	)
}

// Save writes result.json and copies the metrics CSV into the results directory.
// metricsCSVSrc is the path to the CSV file produced by the collector; it will
// be moved into the results directory.
func (s *Store) Save(r RunResult, metricsCSVSrc string) (string, error) {
	dir := s.Dir(r.Hardware, r.TestName, r.Config, r.Subject, r.Version, r.Timestamp)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating results dir: %w", err)
	}

	// Copy metrics CSV first so we can set MetricsFile before writing summary.json.
	if metricsCSVSrc != "" {
		dstCSV := filepath.Join(dir, "metrics.csv")
		if err := copyFile(metricsCSVSrc, dstCSV); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not copy metrics CSV: %v\n", err)
		} else {
			r.MetricsFile = dstCSV
		}
	}

	// Write summary.json (includes the MetricsFile path set above).
	summaryPath := filepath.Join(dir, "summary.json")
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(summaryPath, data, 0o644); err != nil {
		return "", fmt.Errorf("writing summary.json: %w", err)
	}

	return dir, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	buf := make([]byte, 32*1024)
	for {
		n, err := in.Read(buf)
		if n > 0 {
			if _, werr := out.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return out.Sync()
}
