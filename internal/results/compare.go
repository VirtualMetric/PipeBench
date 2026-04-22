package results

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

// CompareOptions controls what the comparison loads and how it's rendered.
type CompareOptions struct {
	TestName   string
	ConfigName string
	ResultsDir string
	Format     string // "table" | "json"
	SortMetric string // column name from metrics CSV to sort by (default: cpu_usr)
}

// SubjectSummary holds the comparison data for one subject.
type SubjectSummary struct {
	Subject    string  `json:"subject"`
	Version    string  `json:"version"`
	LinesOut   int64   `json:"lines_out"`
	BytesOut   int64   `json:"bytes_out"`
	DurationS  float64 `json:"duration_secs"`
	Throughput float64 `json:"throughput_lines_per_sec"`

	// Aggregated from metrics.csv (averages over test duration)
	AvgCPU          float64 `json:"avg_cpu_pct"`
	MaxCPU          float64 `json:"max_cpu_pct"`
	AvgMemMB        float64 `json:"avg_mem_mb"`
	MaxMemMB        float64 `json:"max_mem_mb"`
	TotalNetMB      float64 `json:"total_net_mb"`
	DiskReadMB      float64 `json:"disk_read_mb"`
	DiskWriteMB     float64 `json:"disk_write_mb"`
	IOThroughputAvg float64 `json:"io_throughput_avg_bytes_per_sec"`
	LoadAvg1        float64 `json:"load_avg_1"`
	LoadAvg5        float64 `json:"load_avg_5"`
	LoadAvg15       float64 `json:"load_avg_15"`

	// Correctness (nil for performance tests)
	Passed     *bool  `json:"passed,omitempty"`
	FailReason string `json:"fail_reason,omitempty"`
}

// Compare loads the most recent result for each subject under a given test/config
// and prints (or returns) a comparison.
func Compare(opts CompareOptions) error {
	if opts.ConfigName == "" {
		opts.ConfigName = "default"
	}
	if opts.SortMetric == "" {
		opts.SortMetric = "throughput"
	}
	if opts.Format == "" {
		opts.Format = "table"
	}

	base := filepath.Join(opts.ResultsDir, opts.TestName, opts.ConfigName)
	subjectDirs, err := os.ReadDir(base)
	if err != nil {
		return fmt.Errorf("no results found at %s: %w", base, err)
	}

	var summaries []SubjectSummary

	for _, sd := range subjectDirs {
		if !sd.IsDir() {
			continue
		}
		subject := sd.Name()

		// Walk into subject/<version>/ and find the latest timestamped run
		subjectPath := filepath.Join(base, subject)
		latestRun, err := findLatestRun(subjectPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  skipping %s: %v\n", subject, err)
			continue
		}

		summary, err := loadRunSummary(subject, latestRun)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  skipping %s: %v\n", subject, err)
			continue
		}

		summaries = append(summaries, summary)
	}

	if len(summaries) == 0 {
		return fmt.Errorf("no results found for test %q config %q", opts.TestName, opts.ConfigName)
	}

	// Detect if results contain correctness data
	isCorrectness := summaries[0].Passed != nil

	// Sort
	sortSummaries(summaries, opts.SortMetric)

	switch opts.Format {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(summaries)
	case "html":
		if isCorrectness {
			return writeHTMLCorrectness(os.Stdout, opts.TestName, opts.ConfigName, summaries)
		}
		return writeHTMLPerformance(os.Stdout, opts.TestName, opts.ConfigName, summaries)
	default:
		if isCorrectness {
			printCorrectnessTable(opts.TestName, opts.ConfigName, summaries)
		} else {
			printTable(opts.TestName, opts.ConfigName, summaries)
		}
		return nil
	}
}

// findLatestRun walks subject/<version>/<timestamp>/ dirs and returns the path
// to the most recent one (alphabetically last timestamp).
func findLatestRun(subjectPath string) (string, error) {
	var latest string

	err := filepath.Walk(subjectPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Name() == "summary.json" {
			dir := filepath.Dir(path)
			if dir > latest {
				latest = dir
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if latest == "" {
		return "", fmt.Errorf("no summary.json found under %s", subjectPath)
	}
	return latest, nil
}

func loadRunSummary(subject, runDir string) (SubjectSummary, error) {
	// Load summary.json
	data, err := os.ReadFile(filepath.Join(runDir, "summary.json"))
	if err != nil {
		return SubjectSummary{}, err
	}
	var r RunResult
	if err := json.Unmarshal(data, &r); err != nil {
		return SubjectSummary{}, err
	}

	s := SubjectSummary{
		Subject:    subject,
		Version:    r.Version,
		LinesOut:   r.LinesOut,
		BytesOut:   r.BytesOut,
		DurationS:  r.DurationSec,
		Passed:     r.Passed,
		FailReason: r.FailReason,
	}
	if s.DurationS > 0 {
		s.Throughput = float64(s.LinesOut) / s.DurationS
	}

	// Try to load metrics.csv for resource aggregates
	metricsPath := filepath.Join(runDir, "metrics.csv")
	if m, err := AggregateAllMetricsFromCSV(metricsPath); err == nil {
		s.AvgCPU = m.CPUAvg
		s.MaxCPU = m.CPUMax
		s.AvgMemMB = m.MemAvgMB
		s.MaxMemMB = m.MemMaxMB
		s.TotalNetMB = m.NetTotalMB
		s.DiskReadMB = float64(m.DiskRead) / (1024 * 1024)
		s.DiskWriteMB = float64(m.DiskWrite) / (1024 * 1024)
		s.IOThroughputAvg = m.IOThroughputAvg
		s.LoadAvg1 = m.LoadAvg1
		s.LoadAvg5 = m.LoadAvg5
		s.LoadAvg15 = m.LoadAvg15
	}

	return s, nil
}

// AggregatedMetrics holds all metrics extracted from a collector CSV.
type AggregatedMetrics struct {
	CPUAvg     float64
	CPUMax     float64
	MemAvgMB   float64
	MemMaxMB   float64
	NetRecv    int64   // total bytes received
	NetSend    int64   // total bytes sent
	NetTotalMB float64 // (recv+send) in MB
	DiskRead   int64   // total bytes read
	DiskWrite  int64   // total bytes written
	LoadAvg1   float64 // last sample
	LoadAvg5   float64
	LoadAvg15  float64
	IOThroughputAvg float64 // avg bytes/sec (disk+net combined)
	Samples    int
}

// AggregateMetricsFromCSV returns the legacy 5-value tuple for backward compatibility.
func AggregateMetricsFromCSV(csvPath string) (cpuAvg, cpuMax, memAvgMB, memMaxMB, netTotalMB float64, err error) {
	m, err := AggregateAllMetricsFromCSV(csvPath)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	return m.CPUAvg, m.CPUMax, m.MemAvgMB, m.MemMaxMB, m.NetTotalMB, nil
}

// AggregateAllMetricsFromCSV extracts all metrics from the collector CSV.
func AggregateAllMetricsFromCSV(csvPath string) (AggregatedMetrics, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return AggregatedMetrics{}, err
	}
	defer f.Close()

	reader := csv.NewReader(f)

	header, err := reader.Read()
	if err != nil {
		return AggregatedMetrics{}, err
	}

	cpuIdx := indexOf(header, "cpu_usr")
	memIdx := indexOf(header, "mem_used")
	netRecvIdx := indexOf(header, "net_recv")
	netSendIdx := indexOf(header, "net_send")
	dskReadIdx := indexOf(header, "dsk_read")
	dskWritIdx := indexOf(header, "dsk_writ")
	load1Idx := indexOf(header, "load_avg1")
	load5Idx := indexOf(header, "load_avg5")
	load15Idx := indexOf(header, "load_avg15")

	var m AggregatedMetrics
	var cpuSum, memSum float64
	var netSum, dskSum int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if cpuIdx >= 0 && cpuIdx < len(record) {
			v, _ := strconv.ParseFloat(record[cpuIdx], 64)
			cpuSum += v
			m.CPUMax = math.Max(m.CPUMax, v)
		}
		if memIdx >= 0 && memIdx < len(record) {
			v, _ := strconv.ParseFloat(record[memIdx], 64)
			mb := v / (1024 * 1024)
			memSum += mb
			m.MemMaxMB = math.Max(m.MemMaxMB, mb)
		}
		if netRecvIdx >= 0 && netRecvIdx < len(record) {
			v, _ := strconv.ParseFloat(record[netRecvIdx], 64)
			m.NetRecv += int64(v)
			netSum += int64(v)
		}
		if netSendIdx >= 0 && netSendIdx < len(record) {
			v, _ := strconv.ParseFloat(record[netSendIdx], 64)
			m.NetSend += int64(v)
			netSum += int64(v)
		}
		if dskReadIdx >= 0 && dskReadIdx < len(record) {
			v, _ := strconv.ParseFloat(record[dskReadIdx], 64)
			m.DiskRead += int64(v)
			dskSum += int64(v)
		}
		if dskWritIdx >= 0 && dskWritIdx < len(record) {
			v, _ := strconv.ParseFloat(record[dskWritIdx], 64)
			m.DiskWrite += int64(v)
			dskSum += int64(v)
		}
		// Load averages: keep last sample (most representative)
		if load1Idx >= 0 && load1Idx < len(record) {
			m.LoadAvg1, _ = strconv.ParseFloat(record[load1Idx], 64)
		}
		if load5Idx >= 0 && load5Idx < len(record) {
			m.LoadAvg5, _ = strconv.ParseFloat(record[load5Idx], 64)
		}
		if load15Idx >= 0 && load15Idx < len(record) {
			m.LoadAvg15, _ = strconv.ParseFloat(record[load15Idx], 64)
		}

		m.Samples++
	}

	if m.Samples > 0 {
		m.CPUAvg = cpuSum / float64(m.Samples)
		m.MemAvgMB = memSum / float64(m.Samples)
		// IO throughput: total disk+net bytes / number of seconds (1 sample = 1 second)
		m.IOThroughputAvg = float64(netSum+dskSum) / float64(m.Samples)
	}
	m.NetTotalMB = float64(netSum) / (1024 * 1024)

	return m, nil
}

func indexOf(slice []string, s string) int {
	for i, v := range slice {
		if strings.TrimSpace(v) == s {
			return i
		}
	}
	return -1
}

func sortSummaries(ss []SubjectSummary, metric string) {
	sort.Slice(ss, func(i, j int) bool {
		switch metric {
		case "cpu":
			return ss[i].AvgCPU < ss[j].AvgCPU
		case "memory":
			return ss[i].AvgMemMB < ss[j].AvgMemMB
		default: // throughput — higher is better, sort descending
			return ss[i].Throughput > ss[j].Throughput
		}
	})
}

func printTable(testName, configName string, ss []SubjectSummary) {
	fmt.Printf("\n  Test: %s  Config: %s\n\n", testName, configName)

	w := tabwriter.NewWriter(os.Stdout, 2, 8, 3, ' ', 0)
	fmt.Fprintln(w, "  SUBJECT\tVERSION\tTHROUGHPUT\tAVG CPU\tMAX CPU\tAVG MEM\tMAX MEM\tNET I/O\tDISK R\tDISK W\tIO AVG\tLOAD 1m")
	fmt.Fprintln(w, "  -------\t-------\t----------\t-------\t-------\t-------\t-------\t-------\t------\t------\t------\t-------")

	for _, s := range ss {
		fmt.Fprintf(w, "  %s\t%s\t%s lines/s\t%.1f%%\t%.1f%%\t%.0f MB\t%.0f MB\t%.0f MB\t%.0f MB\t%.0f MB\t%s/s\t%.2f\n",
			s.Subject,
			s.Version,
			formatInt(int64(s.Throughput)),
			s.AvgCPU,
			s.MaxCPU,
			s.AvgMemMB,
			s.MaxMemMB,
			s.TotalNetMB,
			s.DiskReadMB,
			s.DiskWriteMB,
			formatBytes(s.IOThroughputAvg),
			s.LoadAvg1,
		)
	}

	w.Flush()
	fmt.Println()
}

func printCorrectnessTable(testName, configName string, ss []SubjectSummary) {
	fmt.Printf("\n  Test: %s  Config: %s  (correctness)\n\n", testName, configName)

	w := tabwriter.NewWriter(os.Stdout, 2, 8, 3, ' ', 0)
	fmt.Fprintln(w, "  SUBJECT\tVERSION\tRESULT\tLINES OUT\tDURATION\tDETAILS")
	fmt.Fprintln(w, "  -------\t-------\t------\t---------\t--------\t-------")

	for _, s := range ss {
		result := "?"
		if s.Passed != nil {
			if *s.Passed {
				result = "PASS"
			} else {
				result = "FAIL"
			}
		}
		details := "-"
		if s.FailReason != "" {
			details = s.FailReason
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%.1fs\t%s\n",
			s.Subject,
			s.Version,
			result,
			formatInt(s.LinesOut),
			s.DurationS,
			details,
		)
	}

	w.Flush()
	fmt.Println()
}

func formatBytes(b float64) string {
	switch {
	case b >= 1024*1024*1024:
		return fmt.Sprintf("%.1f GB", b/(1024*1024*1024))
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", b/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", b/1024)
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

func formatInt(n int64) string {
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}
	s := strconv.FormatInt(n, 10)
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}
