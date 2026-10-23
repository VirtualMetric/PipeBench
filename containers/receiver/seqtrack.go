package main

// Sequence tracking for the director cluster cases (RECEIVER_TRACK_SEQ).
//
// The http_ack generator writes "CONN=<id> SEQ=<n> TS=<unix nanos> " at the
// start of every line, and counts a line as acknowledged only once a post
// that carried it was answered 2xx. This tracker remembers which SEQ of each
// CONN arrived, so the harness can ask for exactly the acknowledged lines
// that never arrived (/seqcheck). A line seen again is a duplicate. That is a
// re-send by the generator or a re-delivery by the subject, and never loss.
//
// It also measures latency on the first arrival of each line only, from the
// TS the generator stamped to the arrival here. A duplicate says nothing
// about how long the record took. The samples keep their send time, so the
// harness can ask for the latency of the lines sent in one window, such as
// the seconds around a node kill (/latency).
//
// /timeline counts arrivals in 100 ms buckets, which shows when delivery
// stalled and when it came back.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const timelineBucket = 100 * time.Millisecond

type seqConn struct {
	bits   []uint64
	count  int64
	maxSeq int64
}

type latSample struct {
	sent int64 // TS the generator stamped, unix nanos
	lat  int64 // arrival minus sent, nanos
}

type tlBucket struct {
	unique int32
	all    int32
}

type seqTracker struct {
	mu       sync.Mutex
	start    int64 // arrival of the first line, unix nanos
	conns    map[int64]*seqConn
	unique   int64
	dups     int64
	unparsed int64
	samples  []latSample
	buckets  []tlBucket
}

func newSeqTracker() *seqTracker {
	return &seqTracker{conns: map[int64]*seqConn{}}
}

// parseSeqLine finds "CONN=<id> SEQ=<n>" and an optional " TS=<ns>" after it.
// The prefix is searched anywhere in the line, so a subject that wraps the
// record (a syslog header, for example) is still tracked.
func parseSeqLine(line []byte) (conn, seq, ts int64, ok bool) {
	s := string(line)
	i := strings.Index(s, "CONN=")
	if i < 0 {
		return 0, 0, 0, false
	}
	rest := s[i+len("CONN="):]
	conn, rest, ok = leadingInt(rest)
	if !ok || !strings.HasPrefix(rest, " SEQ=") {
		return 0, 0, 0, false
	}
	seq, rest, ok = leadingInt(rest[len(" SEQ="):])
	if !ok {
		return 0, 0, 0, false
	}
	if strings.HasPrefix(rest, " TS=") {
		if v, _, tok := leadingInt(rest[len(" TS="):]); tok {
			ts = v
		}
	}
	return conn, seq, ts, true
}

func leadingInt(s string) (int64, string, bool) {
	n := 0
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		n++
	}
	if n == 0 || n > 19 {
		return 0, s, false
	}
	v, err := strconv.ParseInt(s[:n], 10, 64)
	if err != nil {
		return 0, s, false
	}
	return v, s[n:], true
}

// record notes one received line that arrived at now (unix nanos).
func (t *seqTracker) record(line []byte, now int64) {
	conn, seq, ts, ok := parseSeqLine(line)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start == 0 {
		t.start = now
	}
	b := t.bucket(now)
	b.all++
	if !ok || seq < 0 {
		t.unparsed++
		return
	}
	c := t.conns[conn]
	if c == nil {
		c = &seqConn{maxSeq: -1}
		t.conns[conn] = c
	}
	word, bit := seq/64, uint(seq%64)
	if int64(len(c.bits)) <= word {
		grown := make([]uint64, word+1+word/2)
		copy(grown, c.bits)
		c.bits = grown
	}
	if c.bits[word]&(1<<bit) != 0 {
		t.dups++
		return
	}
	c.bits[word] |= 1 << bit
	c.count++
	if seq > c.maxSeq {
		c.maxSeq = seq
	}
	t.unique++
	b.unique++
	if ts > 0 && now > ts {
		t.samples = append(t.samples, latSample{sent: ts, lat: now - ts})
	}
}

// bucket returns the timeline bucket for now. Callers hold t.mu.
func (t *seqTracker) bucket(now int64) *tlBucket {
	idx := int((now - t.start) / int64(timelineBucket))
	if idx < 0 {
		idx = 0
	}
	for len(t.buckets) <= idx {
		t.buckets = append(t.buckets, tlBucket{})
	}
	return &t.buckets[idx]
}

func (c *seqConn) has(seq int64) bool {
	word := seq / 64
	if seq < 0 || word >= int64(len(c.bits)) {
		return false
	}
	return c.bits[word]&(1<<uint(seq%64)) != 0
}

type connCheck struct {
	Acked         int64      `json:"acked"`
	Received      int64      `json:"received"`
	Missing       int64      `json:"missing"`
	Extra         int64      `json:"extra"`
	MissingRanges [][2]int64 `json:"missing_ranges,omitempty"`
}

type seqCheck struct {
	Acked          int64                `json:"acked"`
	ReceivedAcked  int64                `json:"received_of_acked"`
	Missing        int64                `json:"missing"`
	Extra          int64                `json:"extra"`
	Unique         int64                `json:"unique"`
	Duplicates     int64                `json:"duplicates"`
	Unparsed       int64                `json:"unparsed"`
	Conns          map[string]connCheck `json:"conns"`
	UnexpectedConn []string             `json:"unexpected_conns,omitempty"`
}

// check compares what arrived with what the generator had acknowledged:
// acked maps a CONN id to the number of lines it had acknowledged, which
// are SEQ 0 to n-1. Up to maxRanges missing ranges are listed per CONN.
func (t *seqTracker) check(acked map[int64]int64, maxRanges int) seqCheck {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := seqCheck{Conns: map[string]connCheck{}, Unique: t.unique, Duplicates: t.dups, Unparsed: t.unparsed}
	for conn, n := range acked {
		cc := connCheck{Acked: n}
		c := t.conns[conn]
		var runStart int64 = -1
		for seq := int64(0); seq < n; seq++ {
			if c != nil && c.has(seq) {
				cc.Received++
				if runStart >= 0 {
					if len(cc.MissingRanges) < maxRanges {
						cc.MissingRanges = append(cc.MissingRanges, [2]int64{runStart, seq - 1})
					}
					runStart = -1
				}
				continue
			}
			cc.Missing++
			if runStart < 0 {
				runStart = seq
			}
		}
		if runStart >= 0 && len(cc.MissingRanges) < maxRanges {
			cc.MissingRanges = append(cc.MissingRanges, [2]int64{runStart, n - 1})
		}
		if c != nil {
			cc.Extra = c.count - cc.Received
		}
		out.Acked += cc.Acked
		out.ReceivedAcked += cc.Received
		out.Missing += cc.Missing
		out.Extra += cc.Extra
		out.Conns[strconv.FormatInt(conn, 10)] = cc
	}
	for conn, c := range t.conns {
		if _, ok := acked[conn]; !ok {
			out.Extra += c.count
			out.UnexpectedConn = append(out.UnexpectedConn, strconv.FormatInt(conn, 10))
		}
	}
	sort.Strings(out.UnexpectedConn)
	return out
}

type latencyStats struct {
	Count     int     `json:"count"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
	P99Ms     float64 `json:"p99_ms"`
	MaxMs     float64 `json:"max_ms"`
	MaxSentNs int64   `json:"max_sent_ns,omitempty"`
}

// latency summarizes the first-arrival latency of the lines sent in
// [from, to). A zero bound is open.
func (t *seqTracker) latency(from, to int64) latencyStats {
	t.mu.Lock()
	vals := make([]latSample, 0, len(t.samples))
	for _, s := range t.samples {
		if (from == 0 || s.sent >= from) && (to == 0 || s.sent < to) {
			vals = append(vals, s)
		}
	}
	t.mu.Unlock()
	return summarizeLatency(vals)
}

func summarizeLatency(vals []latSample) latencyStats {
	out := latencyStats{Count: len(vals)}
	if len(vals) == 0 {
		return out
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].lat < vals[j].lat })
	at := func(p int) float64 {
		idx := len(vals) * p / 100
		if idx >= len(vals) {
			idx = len(vals) - 1
		}
		return float64(vals[idx].lat) / 1e6
	}
	out.P50Ms, out.P95Ms, out.P99Ms = at(50), at(95), at(99)
	last := vals[len(vals)-1]
	out.MaxMs = float64(last.lat) / 1e6
	out.MaxSentNs = last.sent
	return out
}

type timelineView struct {
	StartNs  int64   `json:"start_ns"`
	BucketMs int64   `json:"bucket_ms"`
	Unique   []int32 `json:"unique"`
	All      []int32 `json:"all"`
}

func (t *seqTracker) timeline() timelineView {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := timelineView{StartNs: t.start, BucketMs: int64(timelineBucket / time.Millisecond)}
	v.Unique = make([]int32, len(t.buckets))
	v.All = make([]int32, len(t.buckets))
	for i, b := range t.buckets {
		v.Unique[i], v.All[i] = b.unique, b.all
	}
	return v
}

func (t *seqTracker) counts() (unique, dups int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.unique, t.dups
}

// parseAckedParam reads "0:123,1:456" into {0: 123, 1: 456}.
func parseAckedParam(s string) (map[int64]int64, error) {
	out := map[int64]int64{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, found := strings.Cut(part, ":")
		if !found {
			return nil, fmt.Errorf("acked entry %q is not conn:count", part)
		}
		conn, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("acked conn %q: %w", k, err)
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("acked count %q is not a count", v)
		}
		out[conn] = n
	}
	return out, nil
}

func queryInt(r *http.Request, key string) int64 {
	v, _ := strconv.ParseInt(r.URL.Query().Get(key), 10, 64)
	return v
}

// registerSeqHandlers adds the sequence-tracking endpoints to mux.
func registerSeqHandlers(mux *http.ServeMux, t *seqTracker) {
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("/seqcheck", func(w http.ResponseWriter, r *http.Request) {
		acked, err := parseAckedParam(r.URL.Query().Get("acked"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		maxRanges := int(queryInt(r, "ranges"))
		if maxRanges <= 0 {
			maxRanges = 20
		}
		writeJSON(w, t.check(acked, maxRanges))
	})
	mux.HandleFunc("/latency", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, t.latency(queryInt(r, "from_ns"), queryInt(r, "to_ns")))
	})
	mux.HandleFunc("/timeline", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, t.timeline())
	})
}
