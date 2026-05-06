package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	Mode        string // tcp | file | http | otlp
	Listen      string // :9001 or file path
	MetricsPort string // port for /metrics HTTP endpoint
	Timeout     time.Duration

	// Correctness validation (all optional, off by default)
	ValidateOrder     bool
	ValidateDedup     bool
	ValidateContent   bool   // O(1) per line, no heap map — safe for high-volume tests
	ExpectedLines     int64  // 0 = don't check
	RequiredSubstring string // empty = don't check; protocol-agnostic decode check
}

// connStats holds per-connection counters. We keep the running totals in a
// per-connection struct (rather than a single shared counter) so multiple
// connection goroutines don't pingpong the same cache line on every line —
// at multi-million lines/s the contention dwarfed the actual work. The
// trailing padding rounds the struct to 64 bytes (one cache line) so two
// adjacent shards in memory never share a line.
type connStats struct {
	lines   atomic.Int64
	bytes   atomic.Int64
	firstNs atomic.Int64 // earliest line on this shard, unix nanos
	lastNs  atomic.Int64 // latest line on this shard, unix nanos
	_       [64 - 32]byte
}

// recordLine bumps line/byte counters and refreshes the receive-window
// timestamps. To bound `time.Now()` overhead at multi-million lines/s, we
// only sample the timestamp on the very first line of the shard and then
// every 1024 lines after. The receive window is at second-scale so 1024-
// line resolution is far more precision than the runner needs. Returns the
// new line count for callers that want it.
func (s *connStats) recordLine(byteCount int64) int64 {
	n := s.lines.Add(1)
	s.bytes.Add(byteCount)
	if n == 1 {
		now := time.Now().UnixNano()
		s.firstNs.CompareAndSwap(0, now)
		s.lastNs.Store(now)
	} else if n&0x3FF == 0 {
		s.lastNs.Store(time.Now().UnixNano())
	}
	return n
}

// finish records a closing timestamp regardless of where the 1024-line
// sampling left off, so the last bucket of lines in a connection isn't
// silently dropped from the receive window.
func (s *connStats) finish() {
	if s.lines.Load() > 0 {
		s.lastNs.Store(time.Now().UnixNano())
	}
}

// counters is the global aggregator. Hot paths never touch this struct
// after a shard is created — they write to their own connStats directly.
// /metrics walks all shards under a read lock and aggregates.
type counters struct {
	mu     sync.RWMutex
	shards []*connStats
	Done   atomic.Bool
}

// newShard registers and returns a fresh per-connection shard.
func (c *counters) newShard() *connStats {
	s := &connStats{}
	c.mu.Lock()
	c.shards = append(c.shards, s)
	c.mu.Unlock()
	return s
}

// totals aggregates every shard. The receive window spans from the
// EARLIEST first-received timestamp to the LATEST last-received timestamp
// across all connections, so a vmetric run with many parallel forwarders
// or staggered HTTP requests still produces an honest window.
func (c *counters) totals() (lines, bytes, firstNs, lastNs int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.shards {
		lines += s.lines.Load()
		bytes += s.bytes.Load()
		if f := s.firstNs.Load(); f > 0 && (firstNs == 0 || f < firstNs) {
			firstNs = f
		}
		if l := s.lastNs.Load(); l > lastNs {
			lastNs = l
		}
	}
	return
}

// validator tracks correctness state. All access is protected by mu except
// for the atomic counters in counters.
type validator struct {
	mu sync.Mutex

	// For ordering: track arrival order of sequence numbers
	sequences []int64

	// For dedup: track how many times each hash was seen
	hashes map[string]int

	// For latency: sampled round-trip times in nanoseconds
	latencies []int64

	// For content validation: capture first N lines (only when dedup enabled)
	receivedLines []string
	maxCapture    int

	// Memory-corruption detection: every line in sequenced-dedup mode must
	// match "CONN=<int> SEQ=<int> ...". A line that fails this check has
	// been corrupted between the generator and the receiver.
	malformedLines atomic.Int64
	malformedSamp  []string // up to 10 examples

	// Required-substring check: every line must contain a configured
	// substring (e.g. an IP prefix the generator embedded). Used by
	// protocol-decoding tests where the generator and the on-the-wire
	// output don't share a literal byte stream — a successful decode
	// is proven by the presence of a value the generator put in. Counts
	// + up to 10 sample violators.
	missingSubstr     atomic.Int64
	missingSubstrSamp []string
}

func newValidator() *validator {
	return &validator{
		hashes:     make(map[string]int),
		maxCapture: 200, // capture first 200 lines for content validation
	}
}

func (v *validator) recordLine(line []byte, cfg config) {
	// Latency: extract TS=<nanos> and compute delta (always, no config needed)
	if ts := extractTimestamp(line); ts > 0 {
		delta := time.Now().UnixNano() - ts
		if delta > 0 {
			v.mu.Lock()
			v.latencies = append(v.latencies, delta)
			v.mu.Unlock()
		}
	}

	if cfg.ValidateOrder {
		seq := extractSequence(line)
		if seq >= 0 {
			v.mu.Lock()
			v.sequences = append(v.sequences, seq)
			v.mu.Unlock()
		}
	}
	if cfg.ValidateDedup {
		h := hashLine(line)
		v.mu.Lock()
		v.hashes[h]++
		if len(v.receivedLines) < v.maxCapture {
			v.receivedLines = append(v.receivedLines, string(line))
		}
		v.mu.Unlock()
	}

	// Sequenced-mode structural integrity: every line must look like
	// "CONN=<digits> SEQ=<digits> <padding>". Random byte corruption in the
	// prefix almost always breaks one of these tokens. Runs independently of
	// ValidateDedup so performance tests can enable this cheap check without
	// building a 600M-entry hash map.
	if cfg.ValidateContent || cfg.ValidateDedup {
		if !isWellFormedSequenced(line) {
			v.malformedLines.Add(1)
			v.mu.Lock()
			if len(v.malformedSamp) < 10 {
				s := string(line)
				if len(s) > 120 {
					s = s[:120] + "…"
				}
				v.malformedSamp = append(v.malformedSamp, s)
			}
			v.mu.Unlock()
		}
	}

	// Protocol-agnostic decode check: every line must contain a
	// configured substring (e.g. an IP prefix the generator embedded
	// into NetFlow records, or "OTEL-<seq>" embedded into LogRecord
	// bodies). Catches subjects that emit garbage instead of decoded
	// records.
	if cfg.RequiredSubstring != "" {
		if !bytes.Contains(line, []byte(cfg.RequiredSubstring)) {
			v.missingSubstr.Add(1)
			v.mu.Lock()
			if len(v.missingSubstrSamp) < 10 {
				s := string(line)
				if len(s) > 120 {
					s = s[:120] + "…"
				}
				v.missingSubstrSamp = append(v.missingSubstrSamp, s)
			}
			v.mu.Unlock()
		}
	}
}

// isWellFormedSequenced returns true if line starts with "CONN=<digits> SEQ=<digits> ".
// Used to detect memory corruption: a line whose prefix has been mangled will fail
// this check. Padding-region corruption is caught separately by hash-dedup mismatch.
func isWellFormedSequenced(line []byte) bool {
	// "CONN="
	if len(line) < len("CONN=0 SEQ=0 ") || string(line[:5]) != "CONN=" {
		return false
	}
	i := 5
	// digits for conn id
	digits := 0
	for i < len(line) && line[i] >= '0' && line[i] <= '9' {
		i++
		digits++
	}
	if digits == 0 {
		return false
	}
	// " SEQ="
	if i+5 > len(line) || string(line[i:i+5]) != " SEQ=" {
		return false
	}
	i += 5
	// digits for seq
	digits = 0
	for i < len(line) && line[i] >= '0' && line[i] <= '9' {
		i++
		digits++
	}
	if digits == 0 {
		return false
	}
	// trailing " " before padding
	if i >= len(line) || line[i] != ' ' {
		return false
	}
	return true
}

// dedupStats returns the number of unique and duplicate lines seen.
// A "duplicate" is counted each time a line appears beyond the first occurrence.
func (v *validator) dedupStats() (unique, duplicates int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, count := range v.hashes {
		unique++
		if count > 1 {
			duplicates += int64(count) - 1
		}
	}
	return unique, duplicates
}

// latencyPercentiles returns p50, p95, p99 in milliseconds.
func (v *validator) latencyPercentiles() (p50, p95, p99 float64) {
	v.mu.Lock()
	defer v.mu.Unlock()

	n := len(v.latencies)
	if n == 0 {
		return 0, 0, 0
	}

	// Sort a copy
	sorted := make([]int64, n)
	copy(sorted, v.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	toMs := func(nanos int64) float64 { return float64(nanos) / 1e6 }

	p50 = toMs(sorted[n*50/100])
	p95 = toMs(sorted[n*95/100])
	idx99 := n * 99 / 100
	if idx99 >= n {
		idx99 = n - 1
	}
	p99 = toMs(sorted[idx99])
	return
}

// extractTimestamp looks for "TS=<nanos>" in a line and returns the nanosecond value.
func extractTimestamp(line []byte) int64 {
	s := string(line)
	idx := strings.Index(s, "TS=")
	if idx < 0 {
		return 0
	}
	rest := s[idx+3:]
	end := strings.IndexByte(rest, ' ')
	if end < 0 {
		end = len(rest)
	}
	n, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// validate runs all enabled checks and returns pass/fail + detail messages.
// Safe to call multiple times — results are computed fresh each call.
func (v *validator) validate(cfg config, totalLines int64) (bool, []string) {
	v.mu.Lock()
	defer v.mu.Unlock()

	passed := true
	var errors []string

	// Expected line count
	if cfg.ExpectedLines > 0 {
		if totalLines != cfg.ExpectedLines {
			errors = append(errors, fmt.Sprintf(
				"line count mismatch: expected %d, got %d (loss: %.2f%%)",
				cfg.ExpectedLines, totalLines,
				100.0*(1.0-float64(totalLines)/float64(cfg.ExpectedLines)),
			))
			passed = false
		}
	}

	// Ordering check
	if cfg.ValidateOrder && len(v.sequences) > 0 {
		outOfOrder := 0
		for i := 1; i < len(v.sequences); i++ {
			if v.sequences[i] < v.sequences[i-1] {
				outOfOrder++
			}
		}
		if outOfOrder > 0 {
			errors = append(errors, fmt.Sprintf(
				"ordering: %d out-of-order events out of %d total",
				outOfOrder, len(v.sequences),
			))
			passed = false
		}
	}

	// Dedup check
	if cfg.ValidateDedup {
		duplicates := 0
		for _, count := range v.hashes {
			if count > 1 {
				duplicates += count - 1
			}
		}
		if duplicates > 0 {
			errors = append(errors, fmt.Sprintf(
				"duplicates: %d duplicate events detected",
				duplicates,
			))
			passed = false
		}
	}

	// Content integrity (cheap, runs independently of dedup)
	if cfg.ValidateDedup || cfg.ValidateContent {
		if m := v.malformedLines.Load(); m > 0 {
			errors = append(errors, fmt.Sprintf(
				"malformed: %d lines failed structural check (memory corruption)",
				m,
			))
			passed = false
		}
	}

	return passed, errors
}

// extractSequence looks for a line of the form "SEQ=<number> ..." and returns
// the sequence number, or -1 if not found.
func extractSequence(line []byte) int64 {
	s := string(line)
	idx := strings.Index(s, "SEQ=")
	if idx < 0 {
		return -1
	}
	rest := s[idx+4:]
	end := strings.IndexByte(rest, ' ')
	if end < 0 {
		end = len(rest)
	}
	n, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil {
		return -1
	}
	return n
}

func hashLine(line []byte) string {
	h := sha256.Sum256(line)
	return hex.EncodeToString(h[:16]) // 128-bit prefix is enough for dedup
}

func main() {
	cfg := loadConfig()
	cnt := &counters{}
	val := newValidator()

	// Start metrics HTTP server on a separate port
	go serveMetrics(cfg.MetricsPort, cnt, val, cfg)

	// Always start HTTP data ingestion endpoint alongside the primary mode.
	// This allows subjects that output over HTTP/ES (e.g. Filebeat) to send
	// data to :9002 while TCP subjects send to the primary listen port.
	httpDataPort := getEnv("RECEIVER_HTTP_DATA_PORT", "9002")
	go func() {
		if err := startHTTPDataEndpoint(httpDataPort, cnt, val, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "receiver: http data endpoint: %v\n", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "receiver: mode=%s listen=%s http_data=:%s metrics=:%s order=%v dedup=%v content=%v\n",
		cfg.Mode, cfg.Listen, httpDataPort, cfg.MetricsPort, cfg.ValidateOrder, cfg.ValidateDedup, cfg.ValidateContent)

	var err error
	switch cfg.Mode {
	case "tcp":
		err = receiveTCP(cfg, cnt, val)
	case "file":
		err = receiveFile(cfg, cnt, val)
	case "http":
		err = receiveHTTP(cfg, cnt, val)
	case "otlp":
		// OTLP/HTTP on cfg.Listen + OTLP/gRPC on a fixed sibling
		// port so subjects can target either transport. The HTTP
		// listener accepts /v1/logs, /v1/metrics, /v1/traces over
		// protobuf or JSON with optional gzip. The gRPC listener
		// registers the standard OTLP service stubs and walks the
		// same per-record fan-out. One onLine() call per LogRecord
		// / data point / Span across both transports.
		otlpShard := cnt.newShard()
		onLine := func(line []byte) {
			otlpShard.recordLine(int64(len(line)) + 1)
			if cfg.ValidateOrder || cfg.ValidateDedup || cfg.ValidateContent || cfg.RequiredSubstring != "" {
				val.recordLine(line, cfg)
			}
		}
		grpcAddr := getEnv("RECEIVER_OTLP_GRPC_LISTEN", ":4317")
		go func() {
			if grpcErr := startOTLPGRPCEndpoint(grpcAddr, onLine); grpcErr != nil {
				fmt.Fprintf(os.Stderr, "receiver: otlp grpc: %v\n", grpcErr)
			}
		}()
		err = receiveOTLP(cfg, onLine)
	default:
		fmt.Fprintf(os.Stderr, "receiver: unknown mode %q\n", cfg.Mode)
		os.Exit(1)
	}

	cnt.Done.Store(true)

	if err != nil {
		fmt.Fprintf(os.Stderr, "receiver error: %v\n", err)
		os.Exit(1)
	}

	totalLines, totalBytes, _, _ := cnt.totals()

	// Run correctness validation
	passed, errors := val.validate(cfg, totalLines)

	summary := map[string]any{
		"lines_received": totalLines,
		"bytes_received": totalBytes,
	}
	if cfg.ValidateOrder || cfg.ValidateDedup || cfg.ExpectedLines > 0 {
		summary["passed"] = passed
		if len(errors) > 0 {
			summary["errors"] = errors
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		fmt.Fprintf(os.Stderr, "receiver: error encoding summary: %v\n", err)
	}

	fmt.Fprintf(os.Stderr, "receiver: done. lines=%d bytes=%d passed=%v\n",
		totalLines, totalBytes, passed)

	if !passed {
		for _, e := range errors {
			fmt.Fprintf(os.Stderr, "  FAIL: %s\n", e)
		}
		os.Exit(1)
	}
}

func receiveTCP(cfg config, cnt *counters, val *validator) error {
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	defer ln.Close()

	fmt.Fprintf(os.Stderr, "receiver: listening on %s\n", cfg.Listen)

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			wg.Wait()
			return nil
		}
		// One shard per connection — eliminates cache-line contention between
		// concurrent connection goroutines on the line/byte counters.
		shard := cnt.newShard()
		wg.Add(1)
		go func() {
			defer wg.Done()
			handleConn(conn, shard, val, cfg)
		}()
	}
}

func handleConn(conn net.Conn, shard *connStats, val *validator, cfg config) {
	defer conn.Close()
	defer shard.finish()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	needsValidation := cfg.ValidateOrder || cfg.ValidateDedup || cfg.ValidateContent
	// Pass the scanner's internal slice directly. shard.recordLine + validator
	// only read len() and copy via `string(line)` / hash when they need to
	// keep bytes — they never retain the slice past return. Skipping the
	// outer make+copy here saves one alloc and one memcpy per line, which at
	// multi-million-lines/s was the receiver's hot ceiling.
	for scanner.Scan() {
		b := scanner.Bytes()
		shard.recordLine(int64(len(b)) + 1)
		if needsValidation {
			val.recordLine(b, cfg)
		}
	}
}

func receiveFile(cfg config, cnt *counters, val *validator) error {
	shard := cnt.newShard()
	defer shard.finish()
	needsValidation := cfg.ValidateOrder || cfg.ValidateDedup || cfg.ValidateContent

	deadline := time.Now().Add(cfg.Timeout + 5*time.Minute) // generous for file tests
	f, err := os.Open(cfg.Listen)
	if err != nil {
		for time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)
			f, err = os.Open(cfg.Listen)
			if err == nil {
				break
			}
		}
		if err != nil {
			return fmt.Errorf("file %s not available: %w", cfg.Listen, err)
		}
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	lastActivity := time.Now()
	for {
		for scanner.Scan() {
			line := make([]byte, len(scanner.Bytes()))
			copy(line, scanner.Bytes())
			shard.recordLine(int64(len(line)) + 1)
			if needsValidation {
				val.recordLine(line, cfg)
			}
			lastActivity = time.Now()
		}
		if scanner.Err() != nil {
			return scanner.Err()
		}
		if time.Since(lastActivity) > cfg.Timeout {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func receiveHTTP(cfg config, cnt *counters, val *validator) error {
	// One shared shard for the HTTP server — HTTP rates are far below the
	// per-shard ceiling so contention here doesn't matter, and a single
	// shard keeps the per-request bookkeeping trivial.
	httpShard := cnt.newShard()
	needsValidation := cfg.ValidateOrder || cfg.ValidateDedup || cfg.ValidateContent
	mux := http.NewServeMux()

	// Generic POST handler — counts every line in the body
	genericHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "receiver: http read body: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		content := strings.TrimRight(string(body), "\n")
		if content != "" {
			lines := strings.Split(content, "\n")
			for _, l := range lines {
				lineBytes := []byte(l)
				httpShard.recordLine(int64(len(lineBytes)) + 1)
				if needsValidation {
					val.recordLine(lineBytes, cfg)
				}
			}
		} else {
			httpShard.bytes.Add(int64(len(body)))
		}
		w.WriteHeader(http.StatusOK)
	}

	// Elasticsearch bulk API handler — NDJSON format where even lines (0,2,4,…) are
	// action metadata and odd lines (1,3,5,…) are the actual documents.
	// We only count the document lines.
	bulkHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "receiver: http read body: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		content := strings.TrimRight(string(body), "\n")
		if content != "" {
			lines := strings.Split(content, "\n")
			for i, l := range lines {
				// Skip action/metadata lines (even-indexed: 0, 2, 4, …)
				if i%2 == 0 {
					continue
				}
				lineBytes := []byte(l)
				httpShard.recordLine(int64(len(lineBytes)) + 1)
				if needsValidation {
					val.recordLine(lineBytes, cfg)
				}
			}
		}
		// Respond with a minimal ES-compatible bulk response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"took":0,"errors":false,"items":[]}`)
	}

	// Route: anything ending in /_bulk uses the ES bulk handler,
	// everything else uses the generic line-per-line handler.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_bulk") {
			bulkHandler(w, r)
		} else {
			genericHandler(w, r)
		}
	})

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
	}
	return srv.ListenAndServe()
}

// startHTTPDataEndpoint runs a standalone HTTP server that accepts data via
// POST. It handles both plain line-per-request and ES /_bulk NDJSON format.
func startHTTPDataEndpoint(port string, cnt *counters, val *validator, cfg config) error {
	httpShard := cnt.newShard()
	needsValidation := cfg.ValidateOrder || cfg.ValidateDedup || cfg.ValidateContent
	lineCallback := func(line []byte) {
		httpShard.recordLine(int64(len(line)) + 1)
		if needsValidation {
			val.recordLine(line, cfg)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			// Filebeat/ES health check: GET / expects cluster info JSON
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			fmt.Fprintf(w, `{"name":"bench-receiver","cluster_name":"bench","version":{"number":"8.13.0","build_type":"docker"},"tagline":"You Know, for Search"}`)
			return
		}
		defer r.Body.Close()
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer gz.Close()
			reader = gz
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		isBulk := strings.HasSuffix(r.URL.Path, "/_bulk")
		content := strings.TrimRight(string(body), "\n")
		if content == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"took":0,"errors":false,"items":[]}`)
			return
		}

		lines := strings.Split(content, "\n")
		var docCount int
		for i, l := range lines {
			// In ES bulk format, even lines (0,2,4…) are action metadata — skip them
			if isBulk && i%2 == 0 {
				continue
			}
			lineCallback([]byte(l))
			docCount++
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusOK)
		if isBulk && docCount > 0 {
			// Build a proper ES bulk response with one item per document.
			// Filebeat checks each item for success/failure and will retry
			// the entire batch if items are missing.
			// Filebeat uses "create" action (not "index"), so we must match.
			// We parse the action lines to determine the correct action type.
			var buf strings.Builder
			buf.WriteString(`{"took":0,"errors":false,"items":[`)
			itemIdx := 0
			for i := 0; i < len(lines)-1; i += 2 {
				action := "create" // default for Filebeat
				// Try to detect action from the action line
				actionLine := lines[i]
				if strings.Contains(actionLine, `"index"`) {
					action = "index"
				}
				if itemIdx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(`{"`)
				buf.WriteString(action)
				buf.WriteString(`":{"_id":"_","_version":1,"result":"created","_shards":{"total":1,"successful":1,"failed":0},"status":201,"_seq_no":0,"_primary_term":1}}`)
				itemIdx++
			}
			buf.WriteString(`]}`)
			fmt.Fprint(w, buf.String())
		} else {
			fmt.Fprintf(w, `{"took":0,"errors":false,"items":[]}`)
		}
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}
	return srv.ListenAndServe()
}

func serveMetrics(port string, cnt *counters, val *validator, cfg config) {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Aggregate across all per-connection shards. firstNs is the earliest
		// first-line timestamp across every shard; lastNs is the latest.
		totalLines, totalBytes, firstNs, lastNs := cnt.totals()
		resp := map[string]any{
			"lines_received":    totalLines,
			"bytes_received":    totalBytes,
			"done":              cnt.Done.Load(),
			"first_received_ns": firstNs,
			"last_received_ns":  lastNs,
		}
		// Include correctness data if validation is enabled
		if cfg.ValidateOrder || cfg.ValidateDedup || cfg.ValidateContent || cfg.ExpectedLines > 0 {
			passed, errors := val.validate(cfg, totalLines)
			resp["passed"] = passed
			if len(errors) > 0 {
				resp["errors"] = errors
			}
			if cfg.ValidateDedup {
				unique, dupes := val.dedupStats()
				resp["unique_lines"] = unique
				resp["duplicates"] = dupes
				val.mu.Lock()
				if len(val.receivedLines) > 0 && len(val.receivedLines) <= 200 {
					resp["received_content"] = val.receivedLines
				}
				val.mu.Unlock()
			}
			if cfg.ValidateDedup || cfg.ValidateContent {
				resp["malformed_lines"] = val.malformedLines.Load()
				val.mu.Lock()
				if len(val.malformedSamp) > 0 {
					resp["malformed_samples"] = val.malformedSamp
				}
				val.mu.Unlock()
			}
		}
		// Include latency percentiles if any samples were collected
		p50, p95, p99 := val.latencyPercentiles()
		if p50 > 0 {
			resp["latency_p50_ms"] = p50
			resp["latency_p95_ms"] = p95
			resp["latency_p99_ms"] = p99
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := ":" + port
	srv := &http.Server{Addr: addr, Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "receiver: metrics server error: %v\n", err)
	}
}

func loadConfig() config {
	return config{
		Mode:          getEnv("RECEIVER_MODE", "tcp"),
		Listen:        getEnv("RECEIVER_LISTEN", ":9001"),
		MetricsPort:   getEnv("RECEIVER_METRICS_PORT", "9090"),
		Timeout:       time.Duration(getEnvInt("RECEIVER_TIMEOUT_SECS", 30)) * time.Second,
		ValidateOrder:     getEnvBool("RECEIVER_VALIDATE_ORDER", false),
		ValidateDedup:     getEnvBool("RECEIVER_VALIDATE_DEDUP", false),
		ValidateContent:   getEnvBool("RECEIVER_VALIDATE_CONTENT", false),
		ExpectedLines:     int64(getEnvInt("RECEIVER_EXPECTED_LINES", 0)),
		RequiredSubstring: getEnv("RECEIVER_REQUIRED_SUBSTRING", ""),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch v {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return def
	}
}

func getEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
