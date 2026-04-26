package results

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// hardwareSortKey parses an AWS-style instance name (e.g. "c7i.2xlarge")
// into a (family, sizeRank) tuple suitable for sort comparisons. Sizes
// are ordered by their multiplier: xlarge=1, 2xlarge=2, 4xlarge=4, …,
// with `metal` ranked above all numeric sizes within its family.
// Anything that doesn't look like "<family>.<size>" gets a synthetic
// family that sorts to the end (e.g. "custom", "local-win").
func hardwareSortKey(name string) (family string, rank int) {
	i := strings.LastIndex(name, ".")
	if i < 0 {
		// Non-AWS-shaped names (custom, local-win, etc.) sort last.
		return "~" + name, 0
	}
	family = name[:i]
	size := name[i+1:]
	switch size {
	case "xlarge":
		return family, 1
	case "metal":
		return family, 9999
	}
	if strings.HasSuffix(size, "xlarge") {
		if n, err := strconv.Atoi(strings.TrimSuffix(size, "xlarge")); err == nil {
			return family, n
		}
	}
	// Unknown size token: keep deterministic but order it after the
	// known numeric sizes within the same family.
	return family, 10000
}

// sortHardwares orders AWS-style instance names so that, within a family,
// sizes go xlarge → 2xlarge → 4xlarge → … → metal. Across families it's
// alphabetical by family. Names without a dot (custom, local-*) trail.
func sortHardwares(names []string) {
	sort.SliceStable(names, func(i, j int) bool {
		fi, ri := hardwareSortKey(names[i])
		fj, rj := hardwareSortKey(names[j])
		if fi != fj {
			return fi < fj
		}
		return ri < rj
	})
}

// IndexEntry describes one test case for the UI's case list.
// Derived from case.yaml via the catalog supplier.
type IndexEntry struct {
	Test        string `json:"test"`
	Type        string `json:"type,omitempty"`
	Description string `json:"description,omitempty"`
}

// CatalogEntry describes a test case known on disk (case.yaml) even if
// no subject has run it yet. Passed to BuildIndex so the UI can render
// empty placeholders for unrun cases.
type CatalogEntry struct {
	Name        string
	Type        string
	Description string
}

// HardwareIndex is the list of subjects present for one hardware tier.
type HardwareIndex struct {
	Subjects []string `json:"subjects"`
}

// IndexData is the master index served at /results/index.json and also
// written to results/index.json by `harness report` for static hosting.
type IndexData struct {
	GeneratedAt time.Time                 `json:"generated_at"`
	Hardwares   []string                  `json:"hardwares"`
	Subjects    []string                  `json:"subjects"`
	Tests       []IndexEntry              `json:"tests"`
	ByHardware  map[string]*HardwareIndex `json:"by_hardware"`
}

// BuildIndex walks resultsDir/<hw>/<subject>.json and produces the master
// index. Entries in `Tests` include every case seen in the catalog plus
// any case appearing in a subject file. When results/ is empty the catalog
// alone populates Tests; the UI can render all cases as placeholders.
func BuildIndex(resultsDir string, catalog []CatalogEntry) (*IndexData, error) {
	idx := &IndexData{
		GeneratedAt: time.Now().UTC(),
		ByHardware:  map[string]*HardwareIndex{},
	}

	subjectSet := map[string]bool{}
	testSet := map[string]bool{}

	hwEntries, err := os.ReadDir(resultsDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	for _, hwEnt := range hwEntries {
		if !hwEnt.IsDir() {
			continue
		}
		hw := hwEnt.Name()
		hwDir := filepath.Join(resultsDir, hw)
		files, err := os.ReadDir(hwDir)
		if err != nil {
			continue
		}
		hi := &HardwareIndex{}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
				continue
			}
			subject := strings.TrimSuffix(f.Name(), ".json")
			// Skip index.json if present at tier level — no such subject.
			if subject == "index" {
				continue
			}
			hi.Subjects = append(hi.Subjects, subject)
			subjectSet[subject] = true

			// Parse the file just to enumerate its tests.
			data, err := os.ReadFile(filepath.Join(hwDir, f.Name()))
			if err != nil {
				continue
			}
			var sf SubjectFile
			if err := json.Unmarshal(data, &sf); err != nil {
				continue
			}
			for _, r := range sf.Results {
				testSet[r.Test] = true
			}
		}
		sort.Strings(hi.Subjects)
		if len(hi.Subjects) > 0 {
			idx.ByHardware[hw] = hi
			idx.Hardwares = append(idx.Hardwares, hw)
		}
	}

	// Add catalog-only tests so the UI can still show them as placeholders.
	catalogByName := map[string]CatalogEntry{}
	for _, ce := range catalog {
		catalogByName[ce.Name] = ce
		testSet[ce.Name] = true
	}

	// If results/ is empty but the catalog is present, invent a "custom"
	// tier so the UI has something to render into.
	if len(idx.Hardwares) == 0 && len(catalog) > 0 {
		idx.Hardwares = []string{"custom"}
		idx.ByHardware["custom"] = &HardwareIndex{}
	}

	for s := range subjectSet {
		idx.Subjects = append(idx.Subjects, s)
	}
	for t := range testSet {
		ce := catalogByName[t]
		idx.Tests = append(idx.Tests, IndexEntry{
			Test:        t,
			Type:        ce.Type,
			Description: ce.Description,
		})
	}
	sortHardwares(idx.Hardwares)
	sort.Strings(idx.Subjects)
	sort.Slice(idx.Tests, func(i, j int) bool { return idx.Tests[i].Test < idx.Tests[j].Test })
	return idx, nil
}

// WriteIndex regenerates <outDir>/index.json (scanning <outDir>/<hw>/<subject>.json).
// Used by `harness report` to produce a static index for offline hosting.
func WriteIndex(resultsDir string, catalog []CatalogEntry) error {
	idx, err := BuildIndex(resultsDir, catalog)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(resultsDir, "index.json")
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s — %d hardware tiers, %d subjects, %d tests\n",
		path, len(idx.Hardwares), len(idx.Subjects), len(idx.Tests))
	return nil
}

// indexCache coalesces repeated /results/index.json requests during
// page loads. 500 ms TTL keeps the UI snappy without letting stale data
// linger across real test runs.
type indexCache struct {
	mu      sync.Mutex
	idx     *IndexData
	stamp   time.Time
	ttl     time.Duration
	srcDir  string
	catalog func() []CatalogEntry
}

func (c *indexCache) get() (*IndexData, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.idx != nil && time.Since(c.stamp) < c.ttl {
		return c.idx, nil
	}
	var cat []CatalogEntry
	if c.catalog != nil {
		cat = c.catalog()
	}
	idx, err := BuildIndex(c.srcDir, cat)
	if err != nil {
		return nil, err
	}
	c.idx, c.stamp = idx, time.Now()
	return c.idx, nil
}

// ServeWeb starts an HTTP server exposing:
//
//	/                             static UI from webDir (index.html, favicon, …)
//	/results/index.json           live-generated master index (scanned from resultsDir)
//	/results/<hw>/<subject>.json  static pass-through of a subject file
//
// If catalogFn is non-nil it is called on each cache refresh to advertise
// tests that exist in case.yaml but haven't been run yet.
func ServeWeb(webDir, resultsDir, addr string, catalogFn func() []CatalogEntry) error {
	absWeb, err := filepath.Abs(webDir)
	if err != nil {
		return err
	}
	if _, err := os.Stat(absWeb); err != nil {
		return fmt.Errorf("web dir %q not found: %w", absWeb, err)
	}
	absResults, err := filepath.Abs(resultsDir)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	cache := &indexCache{srcDir: absResults, ttl: 500 * time.Millisecond, catalog: catalogFn}

	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(v)
	}

	// Live-generated master index.
	mux.HandleFunc("/results/index.json", func(w http.ResponseWriter, r *http.Request) {
		idx, err := cache.get()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, idx)
	})

	// Static pass-through of per-subject files: /results/<hw>/<subject>.json
	mux.HandleFunc("/results/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/results/")
		if rest == "" || strings.Contains(rest, "\\") || strings.Contains(rest, "..") {
			http.NotFound(w, r)
			return
		}
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 || !strings.HasSuffix(parts[1], ".json") {
			http.NotFound(w, r)
			return
		}
		file := filepath.Join(absResults, parts[0], parts[1])
		abs, err := filepath.Abs(file)
		if err != nil || !strings.HasPrefix(abs, absResults) {
			http.NotFound(w, r)
			return
		}
		data, err := os.ReadFile(file)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(data)
	})

	// Static UI files.
	fs := http.FileServer(http.Dir(absWeb))
	mux.Handle("/", noCacheMiddleware(fs))

	fmt.Printf("PipeBench UI: http://localhost%s  (web=%s, results=%s)\n",
		normalizeAddr(addr), absWeb, absResults)
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
