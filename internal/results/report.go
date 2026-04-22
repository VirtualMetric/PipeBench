package results

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// reportCache coalesces BuildReport calls during burst loads (the UI fires
// index.json + N case files within a few ms; re-walking results/ N+1 times
// wastes ~N × filesystem walk). TTL is short so fresh runs still show up.
type reportCache struct {
	mu       sync.Mutex
	rep      *ReportData
	idx      *IndexData
	cases    map[string]*CaseData
	stamp    time.Time
	ttl      time.Duration
	srcDir   string
	catalog  func() []CatalogEntry
}

func (c *reportCache) get() (*ReportData, *IndexData, map[string]*CaseData, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rep != nil && time.Since(c.stamp) < c.ttl {
		return c.rep, c.idx, c.cases, nil
	}
	rep, err := BuildReport(c.srcDir)
	if err != nil {
		return nil, nil, nil, err
	}
	var cat []CatalogEntry
	if c.catalog != nil {
		cat = c.catalog()
	}
	idx, byCase := SplitReport(rep, cat)
	c.rep, c.idx, c.cases, c.stamp = rep, idx, byCase, time.Now()
	return c.rep, c.idx, c.cases, nil
}

// ReportEntry is a flattened summary.json suitable for the PipeBench UI.
// One entry per test × subject × hardware — latest run only.
type ReportEntry struct {
	Test          string    `json:"test"`
	Subject       string    `json:"subject"`
	Hardware      string    `json:"hardware"`
	Version       string    `json:"version"`
	Config        string    `json:"config"`
	Timestamp     time.Time `json:"timestamp"`
	DurationSec   float64   `json:"duration_secs"`
	LinesIn       int64     `json:"lines_in"`
	LinesOut      int64     `json:"lines_out"`
	BytesOut      int64     `json:"bytes_out"`
	LossPct       float64   `json:"loss_pct"`
	LinesPerSec   float64   `json:"lines_per_sec"`
	AvgCPUPct     float64   `json:"avg_cpu_pct"`
	MaxCPUPct     float64   `json:"max_cpu_pct"`
	AvgMemMB      float64   `json:"avg_mem_mb"`
	MaxMemMB      float64   `json:"max_mem_mb"`
	IOAvgBytesSec float64   `json:"io_avg_bytes_per_sec"`
	Passed        *bool     `json:"passed,omitempty"`
	FailReason    string    `json:"fail_reason,omitempty"`
}

// ReportData is the top-level JSON written to web/data.json.
type ReportData struct {
	GeneratedAt time.Time     `json:"generated_at"`
	Subjects    []string      `json:"subjects"`
	Tests       []string      `json:"tests"`
	Hardwares   []string      `json:"hardwares"`
	Results     []ReportEntry `json:"results"`
}

// BuildReport walks resultsDir/**/summary.json, keeps the latest run per
// (test, subject, hardware), and returns the aggregated data.
func BuildReport(resultsDir string) (*ReportData, error) {
	type key struct{ test, subject, hardware string }
	latest := map[key]ReportEntry{}

	err := filepath.Walk(resultsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Base(path) != "summary.json" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil // skip unreadable
		}
		var r RunResult
		if jsonErr := json.Unmarshal(data, &r); jsonErr != nil {
			return nil // skip malformed
		}
		hw := r.Hardware
		if hw == "" {
			hw = "custom"
		}
		k := key{r.TestName, r.Subject, hw}
		e := ReportEntry{
			Test:          r.TestName,
			Subject:       r.Subject,
			Hardware:      hw,
			Version:       r.Version,
			Config:        r.Config,
			Timestamp:     r.Timestamp,
			DurationSec:   r.DurationSec,
			LinesIn:       r.LinesIn,
			LinesOut:      r.LinesOut,
			BytesOut:      r.BytesOut,
			LossPct:       r.LossPercent,
			LinesPerSec:   r.LinesPerSec,
			AvgCPUPct:     r.AvgCPUPercent,
			MaxCPUPct:     r.MaxCPUPercent,
			AvgMemMB:      r.AvgMemMB,
			MaxMemMB:      r.MaxMemMB,
			IOAvgBytesSec: r.IOThroughputAvg,
			Passed:        r.Passed,
			FailReason:    r.FailReason,
		}
		if prev, ok := latest[k]; !ok || e.Timestamp.After(prev.Timestamp) {
			latest[k] = e
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking results dir: %w", err)
	}

	rep := &ReportData{GeneratedAt: time.Now().UTC()}
	seenS, seenT, seenH := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, e := range latest {
		rep.Results = append(rep.Results, e)
		seenS[e.Subject], seenT[e.Test], seenH[e.Hardware] = true, true, true
	}
	for s := range seenS {
		rep.Subjects = append(rep.Subjects, s)
	}
	for t := range seenT {
		rep.Tests = append(rep.Tests, t)
	}
	for h := range seenH {
		rep.Hardwares = append(rep.Hardwares, h)
	}
	sort.Strings(rep.Subjects)
	sort.Strings(rep.Tests)
	sort.Strings(rep.Hardwares)
	sort.Slice(rep.Results, func(i, j int) bool {
		a, b := rep.Results[i], rep.Results[j]
		if a.Test != b.Test {
			return a.Test < b.Test
		}
		if a.Hardware != b.Hardware {
			return a.Hardware < b.Hardware
		}
		return a.Subject < b.Subject
	})
	return rep, nil
}

// WriteReport generates the report JSON and writes it to outPath.
func WriteReport(resultsDir, outPath string) error {
	rep, err := BuildReport(resultsDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(outPath, buf, 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s (%d results, %d subjects, %d tests, %d hardwares)\n",
		outPath, len(rep.Results), len(rep.Subjects), len(rep.Tests), len(rep.Hardwares))
	return nil
}

// IndexEntry is one row in the split index.json — enough for the UI to
// render pills and kick off per-case fetches without seeing any results yet.
type IndexEntry struct {
	Test        string `json:"test"`
	File        string `json:"file"`
	Type        string `json:"type,omitempty"`
	Description string `json:"description,omitempty"`
	Count       int    `json:"count"`
	HasPass     bool   `json:"has_pass,omitempty"`
}

// CatalogEntry describes a test case that exists on disk (case.yaml) even if
// it has no runs yet. Passed into SplitReport so the index can advertise the
// full catalog with count:0 placeholders.
type CatalogEntry struct {
	Name        string
	Type        string
	Description string
}

// IndexData is the top-level split index written to <dir>/index.json.
type IndexData struct {
	GeneratedAt time.Time    `json:"generated_at"`
	Subjects    []string     `json:"subjects"`
	Hardwares   []string     `json:"hardwares"`
	Tests       []string     `json:"tests"`
	Cases       []IndexEntry `json:"cases"`
}

// CaseData is the per-case JSON: one file per test case.
type CaseData struct {
	Test    string        `json:"test"`
	Results []ReportEntry `json:"results"`
}

// SplitReport partitions a ReportData into an IndexData + a per-case map.
// If catalog is non-nil, cases in the catalog with no results get a count:0
// placeholder row in the index and an empty CaseData entry (so the UI can
// naively fetch every file listed in the index).
func SplitReport(rep *ReportData, catalog []CatalogEntry) (*IndexData, map[string]*CaseData) {
	byCase := map[string]*CaseData{}
	for _, e := range rep.Results {
		c, ok := byCase[e.Test]
		if !ok {
			c = &CaseData{Test: e.Test}
			byCase[e.Test] = c
		}
		c.Results = append(c.Results, e)
	}

	catalogByName := map[string]CatalogEntry{}
	for _, ce := range catalog {
		catalogByName[ce.Name] = ce
		if _, ok := byCase[ce.Name]; !ok {
			byCase[ce.Name] = &CaseData{Test: ce.Name, Results: []ReportEntry{}}
		}
	}

	// Build ordered list of test names: everything we have results for +
	// everything the catalog advertises, deduped and sorted.
	nameSet := map[string]bool{}
	for _, t := range rep.Tests {
		nameSet[t] = true
	}
	for _, ce := range catalog {
		nameSet[ce.Name] = true
	}
	tests := make([]string, 0, len(nameSet))
	for t := range nameSet {
		tests = append(tests, t)
	}
	sort.Strings(tests)

	idx := &IndexData{
		GeneratedAt: rep.GeneratedAt,
		Subjects:    append([]string(nil), rep.Subjects...),
		Hardwares:   append([]string(nil), rep.Hardwares...),
		Tests:       tests,
	}
	for _, t := range tests {
		c := byCase[t]
		hasPass := false
		if c != nil {
			for _, e := range c.Results {
				if e.Passed != nil {
					hasPass = true
					break
				}
			}
		}
		ce := catalogByName[t]
		idx.Cases = append(idx.Cases, IndexEntry{
			Test:        t,
			File:        t + ".json",
			Type:        ce.Type,
			Description: ce.Description,
			Count:       len(c.Results),
			HasPass:     hasPass,
		})
	}
	return idx, byCase
}

// WriteSplitReport writes index.json + one <case>.json per test into outDir.
// Stale per-case files (whose case no longer has any results and isn't in the
// catalog) are removed. Pass a non-nil catalog to list all known cases in the
// index, including ones with no results yet.
func WriteSplitReport(resultsDir, outDir string, catalog []CatalogEntry) error {
	rep, err := BuildReport(resultsDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	idx, byCase := SplitReport(rep, catalog)

	keep := map[string]bool{"index.json": true}
	for name, cd := range byCase {
		buf, err := json.MarshalIndent(cd, "", "  ")
		if err != nil {
			return err
		}
		file := name + ".json"
		if err := os.WriteFile(filepath.Join(outDir, file), buf, 0o644); err != nil {
			return err
		}
		keep[file] = true
	}

	idxBuf, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "index.json"), idxBuf, 0o644); err != nil {
		return err
	}

	entries, err := os.ReadDir(outDir)
	if err == nil {
		for _, ent := range entries {
			if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
				continue
			}
			if !keep[ent.Name()] {
				_ = os.Remove(filepath.Join(outDir, ent.Name()))
			}
		}
	}

	fmt.Printf("wrote %s/index.json + %d case files (%d results, %d subjects, %d hardwares)\n",
		outDir, len(byCase), len(rep.Results), len(rep.Subjects), len(rep.Hardwares))
	return nil
}

// ServeWeb starts an HTTP file server rooted at webDir on the given addr.
// /data/index.json and /data/<case>.json are regenerated on every request
// so fresh runs show up on reload. The legacy /data.json endpoint is still
// served (full aggregated report) for backwards-compat. If catalogFn is
// non-nil it is called on each cache refresh to advertise cases that exist
// on disk but have no runs yet.
func ServeWeb(webDir, resultsDir, addr string, catalogFn func() []CatalogEntry) error {
	abs, err := filepath.Abs(webDir)
	if err != nil {
		return err
	}
	if _, err := os.Stat(abs); err != nil {
		return fmt.Errorf("web dir %q not found: %w", abs, err)
	}

	mux := http.NewServeMux()
	cache := &reportCache{srcDir: resultsDir, ttl: 500 * time.Millisecond, catalog: catalogFn}

	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(v)
	}

	// Legacy single-file endpoint.
	mux.HandleFunc("/data.json", func(w http.ResponseWriter, r *http.Request) {
		rep, _, _, err := cache.get()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, rep)
	})

	// Split endpoints — index + per-case.
	mux.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/data/")
		if name == "" || strings.ContainsAny(name, "/\\") || !strings.HasSuffix(name, ".json") {
			http.NotFound(w, r)
			return
		}
		_, idx, byCase, err := cache.get()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if name == "index.json" {
			writeJSON(w, idx)
			return
		}
		caseName := strings.TrimSuffix(name, ".json")
		cd, ok := byCase[caseName]
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, cd)
	})

	// Everything else from the web dir.
	fs := http.FileServer(http.Dir(abs))
	mux.Handle("/", noCacheMiddleware(fs))

	fmt.Printf("PipeBench UI: http://localhost%s  (web=%s, results=%s)\n",
		normalizeAddr(addr), abs, resultsDir)
	return http.ListenAndServe(addr, mux)
}

func normalizeAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return addr
	}
	return ":" + addr
}

func noCacheMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
