package main

// http_ack mode: an at-least-once HTTP client for the director cluster cases.
//
// Every line it sends carries "CONN=<id> SEQ=<n> TS=<unix nanos> " followed by
// padding. A worker posts its lines in batches and moves on to the next batch
// only after a 2xx answer. So the lines a worker counts as acknowledged are
// exactly SEQ 0 to acked-1 of its CONN id, and the harness can ask the
// receiver which of those never arrived.
//
// A post that fails (a connection error, a timeout, or a status that is not
// 2xx) is sent again, byte for byte, to the next target. The receiver can
// then tell a re-sent line from a new one, and count it as a duplicate.
//
// TS is the time the batch was first posted. A retried batch keeps it, so the
// latency the receiver measures includes the time the client spent retrying,
// as a real client would see it.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ackResult is the http_ack summary the generator adds to its final JSON
// under "ack".
type ackResult struct {
	Targets        []string         `json:"targets"`
	Policy         string           `json:"policy"`
	Batch          int              `json:"batch"`
	LinesAcked     int64            `json:"lines_acked"`
	LinesUnacked   int64            `json:"lines_unacked"`
	Requests       int64            `json:"requests"`
	Failures       int64            `json:"failures"`
	Failovers      int64            `json:"failovers"`
	StatusCounts   map[string]int64 `json:"status_counts"`
	AckedPerConn   map[string]int64 `json:"acked_per_conn"`
	AckedPerTarget map[string]int64 `json:"acked_per_target"`
	MaxAckStallMs  int64            `json:"max_ack_stall_ms"`
	// AckLatency is how long the acknowledged posts took, from the request to
	// the 2xx answer: what a client waits for a confirmation. AckLatencyMaxPerSec
	// is the slowest acknowledged post in each second of the run.
	AckLatency          *ackLatency `json:"ack_latency_ms,omitempty"`
	AckLatencyMaxPerSec []int64     `json:"ack_latency_max_ms_per_second,omitempty"`
	FirstAckNs          int64       `json:"first_ack_ns,omitempty"`
	LastAckNs           int64       `json:"last_ack_ns,omitempty"`
}

// lastAckResult carries the http_ack summary from runHTTPAck to main, which
// prints it with the common result.
var lastAckResult *ackResult

type ackConfig struct {
	targets     []string
	policy      string
	batch       int
	timeout     time.Duration
	backoff     time.Duration
	cooldown    time.Duration
	maxCooldown time.Duration
	drain       time.Duration
	contentType string
	progress    time.Duration
}

func loadAckConfig(cfg config) ackConfig {
	ac := ackConfig{
		policy:      strings.ToLower(getEnv("GENERATOR_TARGET_POLICY", "round_robin")),
		batch:       getEnvInt("GENERATOR_HTTP_BATCH", 10),
		timeout:     envDuration("GENERATOR_HTTP_TIMEOUT", 5*time.Second),
		backoff:     envDuration("GENERATOR_RETRY_BACKOFF", 200*time.Millisecond),
		cooldown:    envDuration("GENERATOR_TARGET_COOLDOWN", 3*time.Second),
		maxCooldown: envDuration("GENERATOR_TARGET_COOLDOWN_MAX", 10*time.Second),
		drain:       envDuration("GENERATOR_DRAIN_TIMEOUT", 60*time.Second),
		contentType: getEnv("GENERATOR_HTTP_CONTENT_TYPE", "text/plain"),
		progress:    envDuration("GENERATOR_PROGRESS_INTERVAL", time.Second),
	}
	ac.targets = splitTargets(cfg.Target)
	if ac.batch < 1 {
		ac.batch = 1
	}
	if ac.policy != "failover" {
		ac.policy = "round_robin"
	}
	return ac
}

// splitTargets turns "a, b,,c" into ["a", "b", "c"].
func splitTargets(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func envDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d >= 0 {
		return d
	}
	return def
}

// targetPicker chooses the target for each post. A target that fails gets no
// post until its cooldown is over. The cooldown doubles with each failure in
// a row, up to maxCooldown. After the cooldown, one post at a time tries the
// target again, and the other workers keep away from it until that post
// succeeds. So a dead node costs one worker a failed attempt now and then. It
// does not stall every worker at once, which would also stall the load. When
// no target can take a post, the picker chooses one anyway.
type targetPicker struct {
	mu          sync.Mutex
	targets     []string
	policy      string
	cooldown    time.Duration
	maxCooldown time.Duration
	next        int
	until       []time.Time
	fails       []int
	probing     []bool
}

func newTargetPicker(targets []string, policy string, cooldown, maxCooldown time.Duration) *targetPicker {
	n := len(targets)
	return &targetPicker{
		targets: targets, policy: policy, cooldown: cooldown, maxCooldown: max(cooldown, maxCooldown),
		until: make([]time.Time, n), fails: make([]int, n), probing: make([]bool, n),
	}
}

// usable says whether target idx may take a post now. A target that failed
// last time takes one post at a time, once its cooldown is over.
func (p *targetPicker) usable(idx int, now time.Time) bool {
	if p.fails[idx] == 0 {
		return true
	}
	return !now.Before(p.until[idx]) && !p.probing[idx]
}

// pick returns the index of the target to post the next batch to. fresh is
// true for the first attempt of a batch: round_robin then moves on to the
// next target, and failover stays where it is.
func (p *targetPicker) pick(now time.Time, fresh bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.targets)
	start := p.next
	if fresh && p.policy == "round_robin" {
		start = (p.next + 1) % n
	}
	for i := range n {
		idx := (start + i) % n
		if p.usable(idx, now) {
			if p.fails[idx] > 0 {
				p.probing[idx] = true
			}
			p.next = idx
			return idx
		}
	}
	p.next = start
	return start
}

// failed starts a target's cooldown and moves the next pick past it.
func (p *targetPicker) failed(idx int, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fails[idx]++
	d := p.cooldown
	for i := 1; i < p.fails[idx] && d < p.maxCooldown; i++ {
		d *= 2
	}
	p.until[idx] = now.Add(min(d, p.maxCooldown))
	p.probing[idx] = false
	if p.next == idx {
		p.next = (idx + 1) % len(p.targets)
	}
}

// succeeded makes a target fully usable again.
func (p *targetPicker) succeeded(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fails[idx] = 0
	p.probing[idx] = false
	p.until[idx] = time.Time{}
}

type ackStats struct {
	acked     atomic.Int64
	ackedB    atomic.Int64
	unacked   atomic.Int64
	requests  atomic.Int64
	failures  atomic.Int64
	failovers atomic.Int64
	pending   atomic.Int64
	firstAck  atomic.Int64
	lastAck   atomic.Int64
	maxStall  atomic.Int64
	start     time.Time

	mu        sync.Mutex
	status    map[string]int64
	perConn   map[string]int64
	perTarget map[string]int64
	lastErr   string
	postLat   []int64 // ms, acknowledged posts
	perSecMax []int64 // ms, slowest acknowledged post per second since start
}

func (s *ackStats) noteStatus(key string) {
	s.mu.Lock()
	s.status[key]++
	s.mu.Unlock()
}

func (s *ackStats) noteError(msg string) {
	s.mu.Lock()
	if len(msg) > 120 {
		msg = msg[:120]
	}
	s.lastErr = msg
	s.mu.Unlock()
}

// noteAck records an acknowledged batch and the gap since the previous one.
func (s *ackStats) noteAck(conn int, target string, lines int, bytesN int, now int64) {
	s.acked.Add(int64(lines))
	s.ackedB.Add(int64(bytesN))
	s.firstAck.CompareAndSwap(0, now)
	for {
		prev := s.lastAck.Load()
		if prev != 0 {
			if gap := (now - prev) / int64(time.Millisecond); gap > s.maxStall.Load() {
				s.maxStall.Store(gap)
			}
		}
		if now <= prev || s.lastAck.CompareAndSwap(prev, now) {
			break
		}
	}
	s.mu.Lock()
	s.perConn[strconv.Itoa(conn)] += int64(lines)
	s.perTarget[target] += int64(lines)
	s.mu.Unlock()
}

// buildAckLine writes one line: "CONN=<id> SEQ=<n> TS=<ns> <padding>".
func buildAckLine(buf *bytes.Buffer, conn int, seq, tsNs int64, size int, pad string) {
	start := buf.Len()
	buf.WriteString("CONN=")
	buf.WriteString(strconv.Itoa(conn))
	buf.WriteString(" SEQ=")
	buf.WriteString(strconv.FormatInt(seq, 10))
	buf.WriteString(" TS=")
	buf.WriteString(strconv.FormatInt(tsNs, 10))
	buf.WriteByte(' ')
	if rest := size - (buf.Len() - start); rest > 0 {
		if rest > len(pad) {
			rest = len(pad)
		}
		buf.WriteString(pad[:rest])
	}
}

func runHTTPAck(cfg config, clock *sendClock) (int64, int64, error) {
	ac := loadAckConfig(cfg)
	if len(ac.targets) == 0 {
		return 0, 0, errors.New("http_ack: GENERATOR_TARGET names no target")
	}
	workers := cfg.Connections
	if workers < 1 {
		workers = 1
	}

	stats := &ackStats{status: map[string]int64{}, perConn: map[string]int64{}, perTarget: map[string]int64{}}
	picker := newTargetPicker(ac.targets, ac.policy, ac.cooldown, ac.maxCooldown)

	start := time.Now()
	stats.start = start
	deadline := start.Add(cfg.Duration)
	hardStop := deadline.Add(ac.drain)

	// Progress lines let the harness follow acknowledgements while the run
	// goes on: it reads the last one from the container log.
	done := make(chan struct{})
	var printer sync.WaitGroup
	printer.Add(1)
	go func() {
		defer printer.Done()
		ticker := time.NewTicker(ac.progress)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				printAckProgress(stats)
				return
			case <-ticker.C:
				printAckProgress(stats)
			}
		}
	}()

	pads := make([]string, 64)
	for i := range pads {
		pads[i] = randString(cfg.LineSize)
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(conn int) {
			defer wg.Done()
			runAckWorker(cfg, ac, conn, stats, picker, clock, pads, deadline, hardStop)
		}(cfg.ConnOffset + w)
	}
	wg.Wait()
	close(done)
	printer.Wait()

	stats.mu.Lock()
	res := &ackResult{
		Targets:             ac.targets,
		Policy:              ac.policy,
		Batch:               ac.batch,
		LinesAcked:          stats.acked.Load(),
		LinesUnacked:        stats.unacked.Load(),
		Requests:            stats.requests.Load(),
		Failures:            stats.failures.Load(),
		Failovers:           stats.failovers.Load(),
		StatusCounts:        stats.status,
		AckedPerConn:        stats.perConn,
		AckedPerTarget:      stats.perTarget,
		MaxAckStallMs:       stats.maxStall.Load(),
		AckLatency:          summarizeAckLatency(stats.postLat),
		AckLatencyMaxPerSec: stats.perSecMax,
		FirstAckNs:          stats.firstAck.Load(),
		LastAckNs:           stats.lastAck.Load(),
	}
	stats.mu.Unlock()
	lastAckResult = res
	return res.LinesAcked, stats.ackedB.Load(), nil
}

func printAckProgress(s *ackStats) {
	fmt.Fprint(os.Stderr, ackProgressLine(s, time.Now()))
}

// ackProgressLine is one progress line. acked_per_target is how many lines
// each target acknowledged so far, as a JSON object keyed by the target as
// GENERATOR_TARGET names it. It holds no space, and comes before last_error,
// whose text can hold anything, so a reader can cut it out of the line. A
// target that acknowledged nothing yet is left out.
func ackProgressLine(s *ackStats, now time.Time) string {
	s.mu.Lock()
	lastErr := s.lastErr
	perTarget, _ := json.Marshal(s.perTarget)
	s.mu.Unlock()
	return fmt.Sprintf("PROGRESS t_ms=%d acked=%d unacked=%d requests=%d failures=%d failovers=%d pending=%d last_ack_ns=%d acked_per_target=%s last_error=%q\n",
		now.UnixMilli(), s.acked.Load(), s.unacked.Load(), s.requests.Load(), s.failures.Load(),
		s.failovers.Load(), s.pending.Load(), s.lastAck.Load(), perTarget, lastErr)
}

// runAckWorker sends one CONN id's lines until the deadline, a batch at a
// time, and retries a batch until it is acknowledged or hardStop passes.
func runAckWorker(cfg config, ac ackConfig, conn int, stats *ackStats, picker *targetPicker,
	clock *sendClock, pads []string, deadline, hardStop time.Time,
) {
	client := &http.Client{
		Timeout: ac.timeout,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: ac.timeout}).DialContext,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  true,
		},
	}
	defer client.CloseIdleConnections()

	interval := time.Second
	if cfg.Rate > 0 {
		interval = time.Duration(float64(ac.batch) / float64(cfg.Rate) * float64(time.Second))
	}
	if interval <= 0 {
		interval = time.Millisecond
	}

	var seq int64
	var buf bytes.Buffer
	next := time.Now()
	last := -1

	for time.Now().Before(deadline) {
		// Pace: one batch per interval. After a long retry the schedule
		// restarts from now, so the client never bursts to catch up.
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		} else if -d > time.Second {
			next = time.Now()
		}
		next = next.Add(interval)

		ts := time.Now().UnixNano()
		buf.Reset()
		for i := range ac.batch {
			if i > 0 {
				buf.WriteByte('\n')
			}
			buildAckLine(&buf, conn, seq+int64(i), ts, cfg.LineSize, pads[(seq+int64(i))%int64(len(pads))])
		}
		body := append([]byte(nil), buf.Bytes()...)
		stats.pending.Add(int64(ac.batch))

		acked := false
		fresh := true
		for {
			now := time.Now()
			if now.After(hardStop) {
				break
			}
			idx := picker.pick(now, fresh)
			if !fresh && last >= 0 && idx != last {
				stats.failovers.Add(1)
			}
			fresh = false
			last = idx
			target := ac.targets[idx]

			stats.requests.Add(1)
			postStart := time.Now()
			code, err := postBatch(client, target, ac.contentType, body)
			switch {
			case err == nil && code >= 200 && code < 300:
				stats.noteStatus(strconv.Itoa(code))
				stats.noteAck(conn, target, ac.batch, len(body), time.Now().UnixNano())
				stats.notePostLatency(postStart, time.Since(postStart))
				clock.RecordSend()
				picker.succeeded(idx)
				acked = true
			case err != nil:
				stats.failures.Add(1)
				key := "error"
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					key = "timeout"
				}
				stats.noteStatus(key)
				stats.noteError(err.Error())
				picker.failed(idx, time.Now())
			default:
				stats.failures.Add(1)
				stats.noteStatus(strconv.Itoa(code))
				stats.noteError(fmt.Sprintf("%s answered %d", target, code))
				picker.failed(idx, time.Now())
			}
			if acked {
				break
			}
			time.Sleep(ac.backoff)
		}
		stats.pending.Add(-int64(ac.batch))
		if !acked {
			// The run is over and this batch was never acknowledged. Its lines
			// may still arrive, but they are not owed.
			stats.unacked.Add(int64(ac.batch))
			return
		}
		seq += int64(ac.batch)
	}
}

func postBatch(client *http.Client, target, contentType string, body []byte) (int, error) {
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(body)) //nolint:noctx // the client carries the timeout
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	return resp.StatusCode, nil
}

// ackLatency summarizes how long acknowledged posts took, in milliseconds.
type ackLatency struct {
	Count int   `json:"count"`
	P50   int64 `json:"p50"`
	P95   int64 `json:"p95"`
	P99   int64 `json:"p99"`
	Max   int64 `json:"max"`
}

// notePostLatency records one acknowledged post's round trip.
func (s *ackStats) notePostLatency(at time.Time, d time.Duration) {
	ms := d.Milliseconds()
	sec := int(at.Sub(s.start) / time.Second)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.postLat = append(s.postLat, ms)
	if sec < 0 {
		return
	}
	for len(s.perSecMax) <= sec {
		s.perSecMax = append(s.perSecMax, 0)
	}
	if ms > s.perSecMax[sec] {
		s.perSecMax[sec] = ms
	}
}

func summarizeAckLatency(vals []int64) *ackLatency {
	if len(vals) == 0 {
		return nil
	}
	sorted := append([]int64(nil), vals...)
	slices.Sort(sorted)
	at := func(p int) int64 {
		idx := len(sorted) * p / 100
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return &ackLatency{Count: len(sorted), P50: at(50), P95: at(95), P99: at(99), Max: sorted[len(sorted)-1]}
}
