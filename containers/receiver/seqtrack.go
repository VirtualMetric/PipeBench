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
// A sample takes 32 bytes, so a throughput case, which sends hundreds of
// millions of lines, keeps the latency of one line in N only
// (RECEIVER_LATENCY_SAMPLE_EVERY): the lines whose SEQ is a multiple of N.
// Every line still counts as arrived, so the loss check and the timeline stay
// exact.
//
// A generator that marks its tries (GENERATOR_MARK_TRIES) writes " TRY=<k>"
// after TS: 1 for the first post of a batch, 2 for the next, and so on. A
// line that arrives again is then one of two things. A copy of a try that
// arrived before is a **repeat**: the subject delivered the same line twice.
// A try that had not arrived before is a **resend**: the client sent the line
// again, because the subject did not confirm the first post, and the subject
// took both. The subject may not have answered, or may have answered with an
// error such as 503 while it kept the records. Without
// marks, a duplicate is neither, and only the total counts.
//
// /timeline counts arrivals in 100 ms buckets, which shows when delivery
// stalled and when it came back. /duplicates lists the lines that arrived
// more than once, with the time of each copy.

import (
	"encoding/json"
	"fmt"
	"math"
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
	seq  int64
	conn int64
}

// dupSample is one arrival of a line that had arrived before.
type dupSample struct {
	Conn    int64  `json:"conn"`
	Seq     int64  `json:"seq"`
	Try     int64  `json:"try,omitempty"`
	Kind    string `json:"kind,omitempty"` // "repeat" or "resend", with marked tries
	SentNs  int64  `json:"sent_ns,omitempty"`
	FirstNs int64  `json:"first_ns,omitempty"` // first arrival, when it is known
	AtNs    int64  `json:"at_ns"`
}

// maxDupSamples bounds the duplicates the tracker remembers one by one. It
// counts every duplicate either way.
const maxDupSamples = 2000

type tlBucket struct {
	unique int32
	all    int32
}

type seqTracker struct {
	mu     sync.Mutex
	start  int64 // arrival of the first line, unix nanos
	conns  map[int64]*seqConn
	unique int64
	dups   int64
	// repeats and resends split dups by the line's TRY mark (see above).
	repeats int64
	resends int64
	marked  bool
	// firstTry holds the try of a line's first arrival when it was not 1, and
	// tries the tries that arrived of each line that arrived more than once,
	// one bit for each try from 1 to 63.
	firstTry map[seqKey]int64
	tries    map[seqKey]uint64
	unparsed int64
	cut      int64
	samples  []latSample
	buckets  []tlBucket
	dupList  []dupSample
	// sampleEvery keeps the latency of the lines whose SEQ is a multiple of
	// it. 1 keeps every line's.
	sampleEvery int64
}

// seqKey names one line: its CONN and its SEQ.
type seqKey struct{ conn, seq int64 }

// newSeqTracker returns a tracker that keeps the latency of one line in
// sampleEvery. A sampleEvery of 1 or less keeps every line's.
func newSeqTracker(sampleEvery int64) *seqTracker {
	return &seqTracker{conns: map[int64]*seqConn{}, sampleEvery: max(sampleEvery, 1),
		firstTry: map[seqKey]int64{}, tries: map[seqKey]uint64{}}
}

// parseSeqLine finds "CONN=<id> SEQ=<n>" and an optional " TS=<ns>" after it.
// The prefix is searched anywhere in the line, so a subject that wraps the
// record (a syslog header, for example) is still tracked.
func parseSeqLine(line []byte) (conn, seq, ts int64, ok bool) {
	conn, seq, ts, _, ok = parseSeqLineTry(line)
	return conn, seq, ts, ok
}

// parseSeqLineTry is parseSeqLine, and also answers the line's try: the
// " TRY=<k>" after TS, or 0 when the line has no mark.
func parseSeqLineTry(line []byte) (conn, seq, ts, try int64, ok bool) {
	s := string(line)
	i := strings.Index(s, "CONN=")
	if i < 0 {
		return 0, 0, 0, 0, false
	}
	rest := s[i+len("CONN="):]
	conn, rest, ok = leadingInt(rest)
	if !ok || !strings.HasPrefix(rest, " SEQ=") {
		return 0, 0, 0, 0, false
	}
	seq, rest, ok = leadingInt(rest[len(" SEQ="):])
	if !ok {
		return 0, 0, 0, 0, false
	}
	if strings.HasPrefix(rest, " TS=") {
		if v, after, tok := leadingInt(rest[len(" TS="):]); tok {
			ts = v
			if strings.HasPrefix(after, " TRY=") {
				if k, _, kok := leadingInt(after[len(" TRY="):]); kok && k >= 1 && k <= 63 {
					try = k
				}
			}
		}
	}
	return conn, seq, ts, try, true
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

// recordCut notes a piece of a line, which arrived at now (unix nanos): what a
// connection held of its last line when it ended inside that line. A piece is
// never the record, even when it carries the record's number. The record has
// arrived only when it arrives whole, so a record whose only trace is a piece
// counts as missing.
func (t *seqTracker) recordCut(now int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start == 0 {
		t.start = now
	}
	t.bucket(now).all++
	t.cut++
}

// record notes one received line that arrived at now (unix nanos).
func (t *seqTracker) record(line []byte, now int64) {
	conn, seq, ts, try, ok := parseSeqLineTry(line)

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
	if try > 0 {
		t.marked = true
	}
	if c.bits[word]&(1<<bit) != 0 {
		t.dups++
		kind := t.classify(seqKey{conn, seq}, try)
		if len(t.dupList) < maxDupSamples {
			t.dupList = append(t.dupList, dupSample{Conn: conn, Seq: seq, Try: try, Kind: kind, SentNs: ts, AtNs: now})
		}
		return
	}
	if try > 1 {
		t.firstTry[seqKey{conn, seq}] = try
	}
	c.bits[word] |= 1 << bit
	c.count++
	if seq > c.maxSeq {
		c.maxSeq = seq
	}
	t.unique++
	b.unique++
	if ts > 0 && now > ts && seq%t.sampleEvery == 0 {
		t.samples = append(t.samples, latSample{sent: ts, lat: now - ts, seq: seq, conn: conn})
	}
}

// classify counts one more arrival of a line that arrived before, by its try,
// and answers "repeat", "resend", or "" for a line without a mark. Callers
// hold t.mu.
func (t *seqTracker) classify(k seqKey, try int64) string {
	if try <= 0 {
		return ""
	}
	seen, ok := t.tries[k]
	if !ok {
		first := int64(1)
		if f, fok := t.firstTry[k]; fok {
			first = f
		}
		seen = 1 << uint(first)
	}
	bit := uint64(1) << uint(try)
	t.tries[k] = seen | bit
	if seen&bit != 0 {
		t.repeats++
		return "repeat"
	}
	t.resends++
	return "resend"
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
	Repeats        int64                `json:"repeats"`
	Resends        int64                `json:"resends"`
	TriesMarked    bool                 `json:"tries_marked"`
	Unparsed       int64                `json:"unparsed"`
	Cut            int64                `json:"cut"`
	Conns          map[string]connCheck `json:"conns"`
	UnexpectedConn []string             `json:"unexpected_conns,omitempty"`
}

// check compares what arrived with what the generator had acknowledged:
// acked maps a CONN id to the number of lines it had acknowledged, which
// are SEQ 0 to n-1. Up to maxRanges missing ranges are listed per CONN.
func (t *seqTracker) check(acked map[int64]int64, maxRanges int) seqCheck {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := seqCheck{Conns: map[string]connCheck{}, Unique: t.unique, Duplicates: t.dups, Unparsed: t.unparsed, Cut: t.cut,
		Repeats: t.repeats, Resends: t.resends, TriesMarked: t.marked}
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
	// The same in microseconds, with p90 and p99.9
	P50Us  float64 `json:"p50_us"`
	P90Us  float64 `json:"p90_us"`
	P99Us  float64 `json:"p99_us"`
	P999Us float64 `json:"p999_us"`
	MaxUs  float64 `json:"max_us"`
	// SampleEvery is set when the receiver keeps the latency of one line in
	// SampleEvery only. Count is then the number of samples.
	SampleEvery int64 `json:"sample_every,omitempty"`
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
	out := summarizeLatency(vals)
	if t.sampleEvery > 1 {
		out.SampleEvery = t.sampleEvery
	}
	return out
}

func summarizeLatency(vals []latSample) latencyStats {
	out := latencyStats{Count: len(vals)}
	if len(vals) == 0 {
		return out
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].lat < vals[j].lat })
	// nth returns the latency below which per of every thousand values fall, in nanoseconds
	nth := func(per int) int64 {
		idx := len(vals) * per / 1000
		if idx >= len(vals) {
			idx = len(vals) - 1
		}
		return vals[idx].lat
	}
	ms := func(ns int64) float64 { return float64(ns) / 1e6 }
	us := func(ns int64) float64 { return math.Round(float64(ns)/100) / 10 }
	out.P50Ms, out.P95Ms, out.P99Ms = ms(nth(500)), ms(nth(950)), ms(nth(990))
	out.P50Us, out.P90Us, out.P99Us, out.P999Us = us(nth(500)), us(nth(900)), us(nth(990)), us(nth(999))
	last := vals[len(vals)-1]
	out.MaxMs, out.MaxUs = ms(last.lat), us(last.lat)
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

type duplicatesView struct {
	Count     int64       `json:"count"`
	Samples   []dupSample `json:"samples"`
	Truncated bool        `json:"truncated,omitempty"`
}

// duplicates lists the lines that arrived again, with the time each one
// first arrived, so the harness can tell a re-delivery seconds later from
// one minutes later.
func (t *seqTracker) duplicates() duplicatesView {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := duplicatesView{Count: t.dups, Samples: append([]dupSample(nil), t.dupList...)}
	out.Truncated = t.dups > int64(len(out.Samples))
	if len(out.Samples) == 0 {
		return out
	}
	type key struct{ conn, seq int64 }
	first := make(map[key]int64, len(out.Samples))
	for _, d := range out.Samples {
		first[key{d.Conn, d.Seq}] = 0
	}
	for _, s := range t.samples {
		k := key{s.conn, s.seq}
		if at, ok := first[k]; ok && at == 0 {
			first[k] = s.sent + s.lat
		}
	}
	for i := range out.Samples {
		out.Samples[i].FirstNs = first[key{out.Samples[i].Conn, out.Samples[i].Seq}]
	}
	return out
}

// seqCountsWin says whether the tracker's distinct and duplicate counts go in
// /metrics in place of the hash-based ones. They do once the tracker has
// counted a sequenced line, or when no hash-based count runs. A case whose
// lines carry no sequence, such as rows from a database, keeps the hash-based
// counts. The tracker is on for every scenario case, and its zeros used to
// replace them, so a duplicate check over such lines could never fail.
func seqCountsWin(seqUnique int64, validateDedup bool) bool {
	return seqUnique > 0 || !validateDedup
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
	mux.HandleFunc("/duplicates", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, t.duplicates())
	})
}
