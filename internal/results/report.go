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
	mu      sync.Mutex
	rep     *ReportData
	idx     *IndexData
	byHW    map[string]map[string]*CaseData
	stamp   time.Time
	ttl     time.Duration
	srcDir  string
	catalog func() []CatalogEntry
}

func (c *reportCache) get() (*ReportData, *IndexData, map[string]map[string]*CaseData, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rep != nil && time.Since(c.stamp) < c.ttl {
		return c.rep, c.idx, c.byHW, nil
	}
	rep, err := BuildReport(c.srcDir)
	if err != nil {
		return nil, nil, nil, err
	}
	var cat []CatalogEntry
	if c.catalog != nil {
		cat = c.catalog()
	}
	idx, byHW := SplitReport(rep, cat)
	c.rep, c.idx, c.byHW, c.stamp = rep, idx, byHW, time.Now()
	return c.rep, c.idx, c.byHW, nil
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
// ByHardware keys on hardware tier — each list is the catalog of cases for
// that tier, including zero-result placeholders for cases the tier hasn't
// run yet. The UI renders the hardware pill bar from Hardwares, then on
// selection fetches the per-(hardware,case) JSON files.
type IndexData struct {
	GeneratedAt time.Time               `json:"generated_at"`
	Subjects    []string                `json:"subjects"`
	Hardwares   []string                `json:"hardwares"`
	Tests       []string                `json:"tests"`
	ByHardware  map[string][]IndexEntry `json:"by_hardware"`
}

// CaseData is the per-(hardware,case) JSON: one file at data/<hw>/<case>.json.
type CaseData struct {
	Hardware string        `json:"hardware"`
	Test     string        `json:"test"`
	Results  []ReportEntry `json:"results"`
}

// SplitReport partitions a ReportData into an IndexData + a per-(hardware,case)
// map. The outer map key is hardware tier; inner key is test name. If catalog
// is non-nil, each hardware tier's case list is padded with count:0 entries
// for any catalog case that tier hasn't run, so the UI can still render rows.
func SplitReport(rep *ReportData, catalog []CatalogEntry) (*IndexData, map[string]map[string]*CaseData) {
	byHW := map[string]map[string]*CaseData{}
	for _, e := range rep.Results {
		hwMap, ok := byHW[e.Hardware]
		if !ok {
			hwMap = map[string]*CaseData{}
			byHW[e.Hardware] = hwMap
		}
		c, ok := hwMap[e.Test]
		if !ok {
			c = &CaseData{Hardware: e.Hardware, Test: e.Test}
			hwMap[e.Test] = c
		}
		c.Results = append(c.Results, e)
	}

	catalogByName := map[string]CatalogEntry{}
	for _, ce := range catalog {
		catalogByName[ce.Name] = ce
	}

	// Build ordered test list (union of everything seen + catalog).
	testSet := map[string]bool{}
	for _, t := range rep.Tests {
		testSet[t] = true
	}
	for _, ce := range catalog {
		testSet[ce.Name] = true
	}
	tests := make([]string, 0, len(testSet))
	for t := range testSet {
		tests = append(tests, t)
	}
	sort.Strings(tests)

	// Hardwares: union of what we have results for + "custom" if the
	// catalog is present AND there's at least one result. If the catalog
	// is present but there's nothing in results/ yet, don't invent a
	// "custom" tier out of thin air — let the UI show an empty tier list.
	hwSet := map[string]bool{}
	for hw := range byHW {
		hwSet[hw] = true
	}
	if catalog != nil && len(byHW) == 0 {
		hwSet["custom"] = true
	}
	hardwares := make([]string, 0, len(hwSet))
	for h := range hwSet {
		hardwares = append(hardwares, h)
	}
	sort.Strings(hardwares)

	idx := &IndexData{
		GeneratedAt: rep.GeneratedAt,
		Subjects:    append([]string(nil), rep.Subjects...),
		Hardwares:   hardwares,
		Tests:       tests,
		ByHardware:  map[string][]IndexEntry{},
	}

	for _, hw := range hardwares {
		hwMap := byHW[hw]
		if hwMap == nil {
			hwMap = map[string]*CaseData{}
			byHW[hw] = hwMap
		}
		entries := make([]IndexEntry, 0, len(tests))
		for _, t := range tests {
			c := hwMap[t]
			if c == nil && catalog != nil {
				// Pad with an empty case so fetching data/<hw>/<case>.json 200s
				// instead of 404 for tiers that haven't run that case yet.
				c = &CaseData{Hardware: hw, Test: t, Results: []ReportEntry{}}
				hwMap[t] = c
			}
			count := 0
			hasPass := false
			if c != nil {
				count = len(c.Results)
				for _, e := range c.Results {
					if e.Passed != nil {
						hasPass = true
						break
					}
				}
			}
			ce := catalogByName[t]
			entries = append(entries, IndexEntry{
				Test:        t,
				File:        hw + "/" + t + ".json",
				Type:        ce.Type,
				Description: ce.Description,
				Count:       count,
				HasPass:     hasPass,
			})
		}
		idx.ByHardware[hw] = entries
	}

	return idx, byHW
}

// WriteSplitReport writes a per-hardware layout into outDir:
//
//	outDir/index.json                       ← master index (hardwares, tests, subjects, by_hardware)
//	outDir/<hardware>/<case>.json           ← per-(hardware,case) results
//
// Stale files/directories (hardwares no longer present, cases dropped from the
// catalog) are pruned. Pass a non-nil catalog to include count:0 placeholders.
func WriteSplitReport(resultsDir, outDir string, catalog []CatalogEntry) error {
	rep, err := BuildReport(resultsDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	idx, byHW := SplitReport(rep, catalog)

	// Track which hardware dirs we wrote to, for prune-stale pass below.
	keepHW := map[string]bool{}
	totalCaseFiles := 0

	for hw, hwCases := range byHW {
		hwDir := filepath.Join(outDir, hw)
		if err := os.MkdirAll(hwDir, 0o755); err != nil {
			return err
		}
		keepHW[hw] = true

		keepCase := map[string]bool{}
		for name, cd := range hwCases {
			buf, err := json.MarshalIndent(cd, "", "  ")
			if err != nil {
				return err
			}
			file := name + ".json"
			if err := os.WriteFile(filepath.Join(hwDir, file), buf, 0o644); err != nil {
				return err
			}
			keepCase[file] = true
			totalCaseFiles++
		}

		// Prune stale case files within this hardware's dir.
		if entries, err := os.ReadDir(hwDir); err == nil {
			for _, ent := range entries {
				if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
					continue
				}
				if !keepCase[ent.Name()] {
					_ = os.Remove(filepath.Join(hwDir, ent.Name()))
				}
			}
		}
	}

	// Write master index.
	idxBuf, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "index.json"), idxBuf, 0o644); err != nil {
		return err
	}

	// Prune stale top-level entries — any .json (other than index.json) or any
	// hardware subdir we no longer have. Subdirs whose name isn't a known hardware
	// tier (e.g. the old flat layout's case files, or a removed tier) get removed.
	if entries, err := os.ReadDir(outDir); err == nil {
		for _, ent := range entries {
			name := ent.Name()
			if ent.IsDir() {
				if !keepHW[name] {
					_ = os.RemoveAll(filepath.Join(outDir, name))
				}
				continue
			}
			// Legacy flat case files — no longer wanted, only index.json stays.
			if strings.HasSuffix(name, ".json") && name != "index.json" {
				_ = os.Remove(filepath.Join(outDir, name))
			}
		}
	}

	fmt.Printf("wrote %s/index.json + %d hardware dirs (%d case files total, %d results, %d subjects)\n",
		outDir, len(byHW), totalCaseFiles, len(rep.Results), len(rep.Subjects))
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

	// Split endpoints — /data/index.json (master) + /data/<hardware>/<case>.json.
	mux.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/data/")
		if rest == "" || strings.Contains(rest, "\\") || strings.Contains(rest, "..") {
			http.NotFound(w, r)
			return
		}
		_, idx, byHW, err := cache.get()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if rest == "index.json" {
			writeJSON(w, idx)
			return
		}
		// Expect <hardware>/<case>.json
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 || !strings.HasSuffix(parts[1], ".json") {
			http.NotFound(w, r)
			return
		}
		hw := parts[0]
		caseName := strings.TrimSuffix(parts[1], ".json")
		hwCases, ok := byHW[hw]
		if !ok {
			http.NotFound(w, r)
			return
		}
		cd, ok := hwCases[caseName]
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
