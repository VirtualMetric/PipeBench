package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	Mode        string        // tcp | file | http | udp_netflow_v5 | otlp
	Target      string        // host:port or file path or URL
	Rate        int           // lines/sec per connection, 0 = unlimited
	Duration    time.Duration // 0 = run until total lines
	TotalLines  int64         // 0 = use duration
	LineSize    int           // bytes per line
	Format      string        // raw | syslog | json
	Warmup      time.Duration
	Sequenced   bool  // embed SEQ=<n> in each line for correctness
	Connections int   // parallel TCP/HTTP connections (default 1)
	SeqOffset   int64 // starting sequence number — set per worker by the parallel dispatcher so global sequences don't overlap across workers (otherwise each worker emits 0..perWorker, breaking the receiver-side dedup check)
}

type result struct {
	LinesSent   int64 `json:"lines_sent"`
	BytesSent   int64 `json:"bytes_sent"`
	DurationMs  int64 `json:"duration_ms"`
	FirstSentNs int64 `json:"first_sent_ns,omitempty"`
	LastSentNs  int64 `json:"last_sent_ns,omitempty"`
}

// sendClock tracks the time from first successful send to last send across
// all connections. This ensures duration reflects actual data transfer only,
// excluding connection setup, retries, and warmup.
type sendClock struct {
	firstSend atomic.Int64 // unix nanos of first successful write
	lastSend  atomic.Int64 // unix nanos of last successful write
}

func (sc *sendClock) RecordSend() {
	now := time.Now().UnixNano()

	// Set firstSend only once (CAS from 0)
	sc.firstSend.CompareAndSwap(0, now)

	// Update lastSend to the latest value
	for {
		old := sc.lastSend.Load()
		if now <= old || sc.lastSend.CompareAndSwap(old, now) {
			break
		}
	}
}

func (sc *sendClock) Duration() time.Duration {
	first := sc.firstSend.Load()
	last := sc.lastSend.Load()
	if first == 0 || last == 0 || last <= first {
		return 0
	}
	return time.Duration(last - first)
}

func (sc *sendClock) Bounds() (first, last int64) {
	return sc.firstSend.Load(), sc.lastSend.Load()
}

func main() {
	cfg := loadConfig()

	if cfg.Warmup > 0 {
		waitForWarmup(cfg)
	}

	fmt.Fprintf(os.Stderr, "generator: mode=%s target=%s rate=%d/s duration=%s connections=%d\n",
		cfg.Mode, cfg.Target, cfg.Rate, cfg.Duration, cfg.Connections)

	var clock sendClock
	var linesSent, bytesSent int64
	var err error

	switch cfg.Mode {
	case "tcp":
		linesSent, bytesSent, err = runTCP(cfg, &clock)
	case "file":
		linesSent, bytesSent, err = runFile(cfg, &clock)
	case "http":
		linesSent, bytesSent, err = runHTTP(cfg, &clock)
	case "udp_netflow_v5":
		// "lines" here counts UDP datagrams sent. Each datagram carries
		// netflowV5RecordsPer flow records, so the receiver should see
		// linesSent * netflowV5RecordsPer lines emitted by the subject.
		linesSent, bytesSent, err = runNetflowV5(cfg, &clock)
	case "otlp":
		// OTLP/Logs over gRPC or HTTP (proto/json) — transport picked
		// via GENERATOR_OTLP_TRANSPORT. "lines" counts LogRecords sent
		// (not batches) so the harness's lines-sent vs lines-received
		// comparison stays meaningful: the receiver sees one TCP line
		// per decoded record.
		linesSent, bytesSent, err = runOTLPLogs(cfg, &clock)
	default:
		fmt.Fprintf(os.Stderr, "generator: unknown mode %q\n", cfg.Mode)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "generator error: %v\n", err)
		os.Exit(1)
	}

	sendDuration := clock.Duration()
	firstSent, lastSent := clock.Bounds()

	r := result{
		LinesSent:   linesSent,
		BytesSent:   bytesSent,
		DurationMs:  sendDuration.Milliseconds(),
		FirstSentNs: firstSent,
		LastSentNs:  lastSent,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		fmt.Fprintf(os.Stderr, "generator: error encoding result: %v\n", err)
	}

	fmt.Fprintf(os.Stderr, "generator: done. lines=%d bytes=%d send_duration=%s\n",
		linesSent, bytesSent, sendDuration)
}

func waitForWarmup(cfg config) {
	target, ok := readinessTarget(cfg)
	if !ok {
		fmt.Fprintf(os.Stderr, "generator: warmup %s\n", cfg.Warmup)
		time.Sleep(cfg.Warmup)
		return
	}

	fmt.Fprintf(os.Stderr, "generator: waiting up to %s for %s\n", cfg.Warmup, target)
	deadline := time.Now().Add(cfg.Warmup)
	var lastErr error
	for {
		conn, err := net.DialTimeout("tcp", target, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			fmt.Fprintf(os.Stderr, "generator: target ready: %s\n", target)
			return
		}
		lastErr = err
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "generator: warmup expired waiting for %s: %v\n", target, lastErr)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func readinessTarget(cfg config) (string, bool) {
	switch cfg.Mode {
	case "tcp":
		return cfg.Target, true
	case "http":
		u, err := url.Parse(cfg.Target)
		if err != nil || u.Host == "" {
			return "", false
		}
		host := u.Host
		if _, _, err := net.SplitHostPort(host); err == nil {
			return host, true
		}
		switch u.Scheme {
		case "https":
			return net.JoinHostPort(host, "443"), true
		default:
			return net.JoinHostPort(host, "80"), true
		}
	default:
		return "", false
	}
}

func loadConfig() config {
	cfg := config{
		Mode:     getEnv("GENERATOR_MODE", "tcp"),
		Target:   mustEnv("GENERATOR_TARGET"),
		LineSize: getEnvInt("GENERATOR_LINE_SIZE", 256),
		Format:   getEnv("GENERATOR_FORMAT", "raw"),
	}

	cfg.Rate = getEnvInt("GENERATOR_RATE", 0)

	if s := os.Getenv("GENERATOR_TOTAL_LINES"); s != "" {
		n, _ := strconv.ParseInt(s, 10, 64)
		cfg.TotalLines = n
	}

	durStr := getEnv("GENERATOR_DURATION", "120s")
	d, err := time.ParseDuration(durStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generator: invalid GENERATOR_DURATION %q: %v\n", durStr, err)
		os.Exit(1)
	}
	cfg.Duration = d

	cfg.Sequenced = getEnvBool("GENERATOR_SEQUENCED", false)
	cfg.Connections = getEnvInt("GENERATOR_CONNECTIONS", 1)
	if cfg.Connections < 1 {
		cfg.Connections = 1
	}

	warmupStr := getEnv("GENERATOR_WARMUP", "5s")
	w, err := time.ParseDuration(warmupStr)
	if err == nil {
		cfg.Warmup = w
	}

	return cfg
}

func dialTCP(target string) (net.Conn, error) {
	timeout := time.Duration(getEnvInt("GENERATOR_CONNECT_TIMEOUT", 120)) * time.Second
	deadline := time.Now().Add(timeout)
	var conn net.Conn
	var err error
	for time.Now().Before(deadline) {
		conn, err = net.DialTimeout("tcp", target, 5*time.Second)
		if err == nil {
			return conn, nil
		}
		fmt.Fprintf(os.Stderr, "generator: tcp connect %s: %v (retrying…)\n", target, err)
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("tcp connect %s after %s: %w", target, timeout, err)
}

func runTCP(cfg config, clock *sendClock) (int64, int64, error) {
	if cfg.Connections <= 1 {
		return runTCPSingle(cfg, clock)
	}
	return runTCPParallel(cfg, clock)
}

// applyWriteDeadline caps conn.Write() so a wedged subject can't hang the
// generator past cfg.Duration. Without this, if the subject stops draining
// its socket, TCP backpressure blocks Write indefinitely and the test only
// exits when the harness timeout (3m10s) kills the container.
func applyWriteDeadline(conn net.Conn, cfg config) {
	if cfg.Duration <= 0 {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(cfg.Duration + 5*time.Second))
}

func runTCPSingle(cfg config, clock *sendClock) (int64, int64, error) {
	conn, err := dialTCP(cfg.Target)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	applyWriteDeadline(conn, cfg)

	w := bufio.NewWriterSize(conn, 256*1024)
	sent, bytesSent, err := sendLinesConn(cfg, 0, clock, func(line []byte) error {
		_, werr := w.Write(line)
		return werr
	})
	ferr := w.Flush()
	if isDurationTimeout(err) {
		err = nil
	}
	if isDurationTimeout(ferr) {
		ferr = nil
	}
	if ferr != nil && err == nil {
		err = ferr
	}
	return sent, bytesSent, err
}

func runTCPParallel(cfg config, clock *sendClock) (int64, int64, error) {
	var totalLines, totalBytes atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	for i := 0; i < cfg.Connections; i++ {
		conn, err := dialTCP(cfg.Target)
		if err != nil {
			return 0, 0, err
		}
		applyWriteDeadline(conn, cfg)

		wg.Add(1)
		go func(id int, conn net.Conn) {
			defer wg.Done()
			defer conn.Close()

			w := bufio.NewWriterSize(conn, 256*1024)
			sent, bytes, err := sendLinesConn(cfg, id, clock, func(line []byte) error {
				_, werr := w.Write(line)
				return werr
			})
			ferr := w.Flush()
			if isDurationTimeout(err) {
				err = nil
			}
			if isDurationTimeout(ferr) {
				ferr = nil
			}

			totalLines.Add(sent)
			totalBytes.Add(bytes)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
			} else if ferr != nil {
				errOnce.Do(func() { firstErr = ferr })
			}

			fmt.Fprintf(os.Stderr, "generator: connection %d done: lines=%d bytes=%d\n",
				id, sent, bytes)
		}(i, conn)
	}

	wg.Wait()
	return totalLines.Load(), totalBytes.Load(), firstErr
}

// isDurationTimeout reports whether err is the write deadline firing —
// i.e. the test ran long enough that we cut the connection off. That's a
// successful exit, not a failure.
func isDurationTimeout(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

func runFile(cfg config, clock *sendClock) (int64, int64, error) {
	f, err := os.OpenFile(cfg.Target, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, 0, fmt.Errorf("open file %s: %w", cfg.Target, err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 64*1024)
	sent, bytes, err := sendLines(cfg, clock, func(line []byte) error {
		_, err := w.Write(line)
		return err
	})
	_ = w.Flush()
	return sent, bytes, err
}

func runHTTP(cfg config, clock *sendClock) (int64, int64, error) {
	if cfg.Connections <= 1 {
		return runHTTPSingle(cfg, clock)
	}
	return runHTTPParallel(cfg, clock)
}

func runHTTPSingle(cfg config, clock *sendClock) (int64, int64, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	batchSize := 100

	var linesSent, bytesSent int64
	var batch [][]byte

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		body := bytes.Join(batch, []byte("\n"))
		resp, err := client.Post(cfg.Target, "text/plain", bytes.NewReader(body)) //nolint:noctx
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("http POST %s: status %d", cfg.Target, resp.StatusCode)
		}
		bytesSent += int64(len(body))
		linesSent += int64(len(batch))
		clock.RecordSend()
		batch = batch[:0]
		return nil
	}

	_, _, err := sendLines(cfg, clock, func(line []byte) error {
		batch = append(batch, line)
		if len(batch) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if err := flush(); err != nil {
		return 0, 0, err
	}
	return linesSent, bytesSent, nil
}

func runHTTPParallel(cfg config, clock *sendClock) (int64, int64, error) {
	var totalLines, totalBytes atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	for i := 0; i < cfg.Connections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sent, bytes, err := runHTTPSingle(cfg, clock)
			totalLines.Add(sent)
			totalBytes.Add(bytes)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
			}
			fmt.Fprintf(os.Stderr, "generator: http client %d done: lines=%d bytes=%d\n",
				id, sent, bytes)
		}(i)
	}

	wg.Wait()
	return totalLines.Load(), totalBytes.Load(), firstErr
}

// sendLines drives the core send loop, calling write(line) for each generated line.
func sendLines(cfg config, clock *sendClock, write func([]byte) error) (int64, int64, error) {
	return sendLinesConn(cfg, 0, clock, write)
}

// sendLinesConn is like sendLines but tags sequenced lines with the connection id
// so duplicates across connections can be distinguished.
func sendLinesConn(cfg config, connID int, clock *sendClock, write func([]byte) error) (int64, int64, error) {
	// Pre-generate a template line for performance tests.
	// For sequenced (correctness) mode, each line is unique.
	templateLine := generateLine(cfg.LineSize, cfg.Format)

	// Pre-allocate a reusable line buffer for sequenced mode so we don't
	// regenerate random padding on every line (the old path ran rand.Intn
	// LineSize times per line — cratered perf when validate_content was on).
	// Padding is generated once and the prefix is rewritten in place.
	// Last byte is reserved for '\n' so the hot-loop write callback can
	// skip a per-line append-newline allocation.
	var seqBuf []byte
	if cfg.Sequenced {
		seqBuf = make([]byte, cfg.LineSize+1)
		copy(seqBuf, randString(cfg.LineSize))
		seqBuf[cfg.LineSize] = '\n'
	}

	var linesSent, bytesSent int64

	var deadline time.Time
	if cfg.Duration > 0 {
		deadline = time.Now().Add(cfg.Duration)
	}

	var rateLimiter <-chan time.Time
	if cfg.Rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(cfg.Rate))
		defer ticker.Stop()
		rateLimiter = ticker.C
	}

	// Sample clock recording: every 10,000 lines to avoid overhead
	const clockSampleInterval = 10000

	for {
		// Check exit conditions
		if cfg.TotalLines > 0 && linesSent >= cfg.TotalLines {
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}

		// Rate limiting
		if rateLimiter != nil {
			<-rateLimiter
		}

		var line []byte
		if cfg.Sequenced {
			line = writeSequencedPrefix(seqBuf, connID, linesSent)
		} else if linesSent%1000 == 0 {
			// Sample every 1000th line with a timestamp for latency measurement
			line = generateTimestampedLine(cfg.LineSize)
		} else {
			line = templateLine
		}

		if err := write(line); err != nil {
			return linesSent, bytesSent, err
		}

		linesSent++
		bytesSent += int64(len(line)) // line already includes the '\n'

		// Record first/last send timestamps (sampled to reduce overhead)
		if linesSent == 1 || linesSent%clockSampleInterval == 0 {
			clock.RecordSend()
		}
	}

	// Always record the final send
	if linesSent > 0 {
		clock.RecordSend()
	}

	return linesSent, bytesSent, nil
}

// generateTimestampedLine creates a line with an embedded nanosecond timestamp
// for latency measurement: "TS=<nanos> <padding...>\n" (newline included).
func generateTimestampedLine(size int) []byte {
	prefix := fmt.Sprintf("TS=%d ", time.Now().UnixNano())
	pad := size - len(prefix)
	if pad < 0 {
		pad = 0
	}
	return []byte(prefix + randString(pad) + "\n")
}

// generateSequencedLine creates a line with an embedded sequence number
// for correctness validation: "SEQ=<n> <padding...>\n" (newline included).
func generateSequencedLine(seq int64, size int) []byte {
	prefix := fmt.Sprintf("SEQ=%d ", seq)
	pad := size - len(prefix)
	if pad < 0 {
		pad = 0
	}
	return []byte(prefix + randString(pad) + "\n")
}

// generateSequencedLineConn creates a line uniquely identifiable across
// parallel connections: "CONN=<id> SEQ=<n> <padding...>\n".
func generateSequencedLineConn(connID int, seq int64, size int) []byte {
	prefix := fmt.Sprintf("CONN=%d SEQ=%d ", connID, seq)
	pad := size - len(prefix)
	if pad < 0 {
		pad = 0
	}
	return []byte(prefix + randString(pad) + "\n")
}

// writeSequencedPrefix rewrites the "CONN=<id> SEQ=<n> " header in place
// at the start of buf (which already holds pre-generated random padding from
// the first call). Returns buf so the caller can pass it straight to write.
// Keeps the same wire format as generateSequencedLineConn without re-randomizing
// the padding on every line — hot-path optimization for high-volume tests where
// validate_content is enabled.
func writeSequencedPrefix(buf []byte, connID int, seq int64) []byte {
	prefix := fmt.Sprintf("CONN=%d SEQ=%d ", connID, seq)
	if len(prefix) > len(buf) {
		// Line is shorter than the prefix — fall back to the slow path.
		// buf is sized LineSize+1 (last byte = '\n'); the slow path
		// produces size+1 bytes, so pass len(buf)-1 to match.
		return generateSequencedLineConn(connID, seq, len(buf)-1)
	}
	// Overwrite only the prefix; preserve the random padding AND the
	// trailing '\n' the caller pre-stamped at buf[len(buf)-1].
	copy(buf, prefix)
	return buf
}

// generateLine returns a `size`-byte line with a trailing '\n' already
// appended (final length size+1). Baking the newline in here lets the
// per-connection hot loop skip a `append(line, '\n')` allocation on
// every line — at 6 M+ lines/s × N connections that allocation alone
// caps throughput on garbage-collection pressure.
func generateLine(size int, format string) []byte {
	switch format {
	case "syslog":
		ts := time.Now().Format(time.RFC3339)
		prefix := fmt.Sprintf("<13>%s myhost myprog[1234]: ", ts)
		pad := size - len(prefix)
		if pad < 1 {
			pad = 1
		}
		line := make([]byte, 0, size+1)
		line = append(line, prefix...)
		line = append(line, randString(pad)...)
		return append(line, '\n')
	case "json":
		ts := time.Now().UnixMilli()
		msg := randString(size - 50)
		return []byte(fmt.Sprintf(`{"ts":%d,"level":"info","msg":"%s"}`+"\n", ts, msg))
	default: // raw
		b := make([]byte, size+1)
		for i := 0; i < size; i++ {
			b[i] = charset[rand.Intn(len(charset))]
		}
		b[size] = '\n'
		return b
	}
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "

func randString(n int) string {
	if n <= 0 {
		return "x"
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "generator: %s is required\n", key)
		os.Exit(1)
	}
	return v
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
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	v = strings.TrimSpace(v)
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
