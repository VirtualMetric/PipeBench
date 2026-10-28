package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var ackLineRE = regexp.MustCompile(`^CONN=(\d+) SEQ=(\d+) TS=(\d+) `)

// collector is a test server that records every line of every 2xx post.
type collector struct {
	mu     sync.Mutex
	bodies [][]byte
	lines  map[string]int // "conn/seq" -> times seen
}

func (c *collector) handler(fail func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if fail != nil && fail() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.bodies = append(c.bodies, body)
		for _, l := range bytes.Split(body, []byte("\n")) {
			m := ackLineRE.FindSubmatch(l)
			if m == nil {
				continue
			}
			c.lines[string(m[1])+"/"+string(m[2])]++
		}
	}
}

// TestHTTPAckRetriesUntilAcknowledged checks the at-least-once contract: a
// batch the target refused is sent again until it is answered 2xx, the lines
// counted as acknowledged are SEQ 0..acked-1 with no gap, and every line
// carries CONN, SEQ and TS.
func TestHTTPAckRetriesUntilAcknowledged(t *testing.T) {
	c := &collector{lines: map[string]int{}}
	var calls atomic.Int64
	// Refuse every third post.
	srv := httptest.NewServer(c.handler(func() bool { return calls.Add(1)%3 == 0 }))
	defer srv.Close()

	t.Setenv("GENERATOR_HTTP_BATCH", "5")
	t.Setenv("GENERATOR_RETRY_BACKOFF", "1ms")
	t.Setenv("GENERATOR_TARGET_COOLDOWN", "0s")
	t.Setenv("GENERATOR_PROGRESS_INTERVAL", "1h")
	cfg := config{Mode: "http_ack", Target: srv.URL, Rate: 2000, Duration: 300 * time.Millisecond, LineSize: 80, Connections: 1}

	acked, _, err := runHTTPAck(cfg, &sendClock{})
	if err != nil {
		t.Fatalf("runHTTPAck: %v", err)
	}
	res := lastAckResult
	if acked == 0 || res.LinesAcked != acked {
		t.Fatalf("acked %d, summary says %d", acked, res.LinesAcked)
	}
	if res.Failures == 0 {
		t.Fatalf("the server refused every third post, but no failure was counted")
	}
	if got := res.AckedPerConn["0"]; got != acked {
		t.Fatalf("acked_per_conn[0] = %d, want %d", got, acked)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for seq := int64(0); seq < acked; seq++ {
		if c.lines["0/"+strconv.FormatInt(seq, 10)] == 0 {
			t.Fatalf("SEQ %d was counted as acknowledged but never reached the server", seq)
		}
	}
	for _, body := range c.bodies {
		for _, l := range bytes.Split(body, []byte("\n")) {
			if !ackLineRE.Match(l) {
				t.Fatalf("line without CONN/SEQ/TS: %q", l)
			}
			if len(l) != 80 {
				t.Fatalf("line length %d, want 80: %q", len(l), l)
			}
		}
	}
}

// TestHTTPAckFailsOverAndResendsSameBytes checks that a batch refused by one
// target goes to the next one unchanged: the second target must see exactly
// the bytes the first one refused.
func TestHTTPAckFailsOverAndResendsSameBytes(t *testing.T) {
	var refused [][]byte
	var mu sync.Mutex
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		refused = append(refused, body)
		mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	c := &collector{lines: map[string]int{}}
	up := httptest.NewServer(c.handler(nil))
	defer up.Close()

	t.Setenv("GENERATOR_HTTP_BATCH", "4")
	t.Setenv("GENERATOR_RETRY_BACKOFF", "1ms")
	t.Setenv("GENERATOR_TARGET_POLICY", "failover")
	t.Setenv("GENERATOR_PROGRESS_INTERVAL", "1h")
	cfg := config{Mode: "http_ack", Target: down.URL + "," + up.URL, Rate: 400, Duration: 200 * time.Millisecond, LineSize: 64, Connections: 2}

	if _, _, err := runHTTPAck(cfg, &sendClock{}); err != nil {
		t.Fatalf("runHTTPAck: %v", err)
	}
	res := lastAckResult
	if res.Failovers == 0 {
		t.Fatalf("the first target refused every post, but no failover was counted")
	}
	if res.AckedPerTarget[down.URL] != 0 {
		t.Fatalf("a refusing target cannot acknowledge lines: %v", res.AckedPerTarget)
	}
	if len(res.AckedPerConn) != 2 {
		t.Fatalf("two workers must use two CONN ids: %v", res.AckedPerConn)
	}

	mu.Lock()
	defer mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(refused) == 0 {
		t.Fatalf("the refusing target was never tried")
	}
	for _, r := range refused {
		found := false
		for _, b := range c.bodies {
			if bytes.Equal(r, b) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("a refused batch was not resent byte for byte to the other target")
		}
	}
}

func TestTargetPickerSkipsCoolingTarget(t *testing.T) {
	p := newTargetPicker([]string{"a", "b", "c"}, "round_robin", time.Minute, time.Minute)
	now := time.Now()
	p.failed(1, now)
	for range 10 {
		if got := p.pick(now, true); got == 1 {
			t.Fatalf("picked the cooling target")
		}
	}
	p.failed(0, now)
	p.failed(2, now)
	// Every target cools down: one is still picked.
	if got := p.pick(now, true); got < 0 || got > 2 {
		t.Fatalf("pick out of range: %d", got)
	}
}

// TestTargetPickerTriesFailedTargetOneAtATime checks that once a failed
// target's cooldown is over, only one post at a time goes to it, and that it
// takes every worker's posts again after one of them succeeds.
func TestTargetPickerTriesFailedTargetOneAtATime(t *testing.T) {
	p := newTargetPicker([]string{"a", "b", "c"}, "round_robin", time.Second, 10*time.Second)
	now := time.Now()
	p.failed(1, now)
	later := now.Add(2 * time.Second)

	tries := 0
	for range 30 {
		if p.pick(later, true) == 1 {
			tries++
		}
	}
	if tries != 1 {
		t.Fatalf("target b was tried by %d posts at once after its cooldown, want 1", tries)
	}

	p.succeeded(1)
	tries = 0
	for range 30 {
		if p.pick(later, true) == 1 {
			tries++
		}
	}
	if tries != 10 {
		t.Fatalf("target b took %d of 30 round-robin posts after it recovered, want 10", tries)
	}
}

// TestTargetPickerCooldownGrows checks that the cooldown doubles with each
// failure in a row, stops at the maximum, and starts over after a success.
func TestTargetPickerCooldownGrows(t *testing.T) {
	p := newTargetPicker([]string{"a", "b"}, "round_robin", time.Second, 5*time.Second)
	now := time.Now()
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second} {
		p.failed(0, now)
		if got := p.until[0].Sub(now); got != want {
			t.Fatalf("cooldown after %d failures in a row is %s, want %s", p.fails[0], got, want)
		}
	}
	p.succeeded(0)
	p.failed(0, now)
	if got := p.until[0].Sub(now); got != time.Second {
		t.Fatalf("cooldown after a success and one failure is %s, want 1s", got)
	}
}

func TestSplitTargets(t *testing.T) {
	got := splitTargets(" http://a:1/ ,, http://b:2/")
	if len(got) != 2 || got[0] != "http://a:1/" || got[1] != "http://b:2/" {
		t.Fatalf("splitTargets: %q", got)
	}
}

// TestAckProgressLineCountsEachTarget: each progress line says how many lines
// each target acknowledged, so the harness can tell whether a replica that a
// container platform replaced takes posts again. The count sits ahead of
// last_error, whose text can hold spaces, so the harness can cut it out.
func TestAckProgressLineCountsEachTarget(t *testing.T) {
	s := &ackStats{status: map[string]int64{}, perConn: map[string]int64{}, perTarget: map[string]int64{}}
	line := ackProgressLine(s, time.UnixMilli(1000))
	if !strings.Contains(line, " acked_per_target={} last_error=") {
		t.Fatalf("before any acknowledgement, the line is %q, want an empty acked_per_target ahead of last_error", line)
	}
	s.noteAck(1, "http://subject-1:9000/", 10, 100, 5)
	s.noteAck(2, "http://subject-2:9000/", 10, 100, 6)
	s.noteAck(3, "http://subject-1:9000/", 10, 100, 7)
	s.noteError("dial tcp: lookup subject-3 on 127.0.0.11:53: no such host")
	line = ackProgressLine(s, time.UnixMilli(2000))
	want := `PROGRESS t_ms=2000 acked=30 unacked=0 requests=0 failures=0 failovers=0 pending=0 last_ack_ns=7 ` +
		`acked_per_target={"http://subject-1:9000/":20,"http://subject-2:9000/":10} ` +
		`last_error="dial tcp: lookup subject-3 on 127.0.0.11:53: no such host"` + "\n"
	if line != want {
		t.Fatalf("progress line\n got %q\nwant %q", line, want)
	}
}
