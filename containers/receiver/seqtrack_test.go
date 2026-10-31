package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseSeqLine(t *testing.T) {
	cases := []struct {
		line          string
		conn, seq, ts int64
		ok            bool
	}{
		{"CONN=3 SEQ=17 TS=1700000000000000000 abc", 3, 17, 1700000000000000000, true},
		{"<14>Oct 23 host: CONN=0 SEQ=5 xyz", 0, 5, 0, true},
		{"CONN=1 SEQ= 5", 0, 0, 0, false},
		{"no sequence here", 0, 0, 0, false},
	}
	for _, c := range cases {
		conn, seq, ts, ok := parseSeqLine([]byte(c.line))
		if ok != c.ok || conn != c.conn || seq != c.seq || ts != c.ts {
			t.Errorf("parseSeqLine(%q) = %d %d %d %v, want %d %d %d %v", c.line, conn, seq, ts, ok, c.conn, c.seq, c.ts, c.ok)
		}
	}
}

// TestSeqTrackerCheck covers the loss verdict: acknowledged lines that never
// arrived are missing, a line seen twice is a duplicate, and lines past the
// acknowledged count are extra, not loss.
func TestSeqTrackerCheck(t *testing.T) {
	tr := newSeqTracker(1)
	now := time.Now().UnixNano()
	sent := now - int64(50*time.Millisecond)
	for seq := 0; seq < 100; seq++ {
		if seq >= 40 && seq <= 44 {
			continue // lost
		}
		tr.record([]byte(fmt.Sprintf("CONN=0 SEQ=%d TS=%d pad", seq, sent)), now)
	}
	tr.record([]byte(fmt.Sprintf("CONN=0 SEQ=7 TS=%d pad", sent)), now)   // duplicate
	tr.record([]byte(fmt.Sprintf("CONN=0 SEQ=100 TS=%d pad", sent)), now) // past the acked count
	tr.record([]byte("garbage"), now)

	got := tr.check(map[int64]int64{0: 100, 1: 10}, 20)
	if got.Acked != 110 || got.ReceivedAcked != 95 || got.Missing != 15 {
		t.Fatalf("check: acked %d received %d missing %d, want 110 95 15", got.Acked, got.ReceivedAcked, got.Missing)
	}
	if got.Duplicates != 1 || got.Extra != 1 || got.Unparsed != 1 {
		t.Fatalf("check: dups %d extra %d unparsed %d, want 1 1 1", got.Duplicates, got.Extra, got.Unparsed)
	}
	c0 := got.Conns["0"]
	if len(c0.MissingRanges) != 1 || c0.MissingRanges[0] != [2]int64{40, 44} {
		t.Fatalf("conn 0 missing ranges: %v", c0.MissingRanges)
	}
	if c1 := got.Conns["1"]; c1.Missing != 10 || len(c1.MissingRanges) != 1 || c1.MissingRanges[0] != [2]int64{0, 9} {
		t.Fatalf("conn 1 (nothing arrived): %+v", c1)
	}
}

// TestSeqTrackerLatencyWindow checks that latency counts first arrivals only
// and can be limited to the lines sent in a window.
func TestSeqTrackerLatencyWindow(t *testing.T) {
	tr := newSeqTracker(1)
	base := time.Now().UnixNano()
	for i := range 10 {
		sent := base + int64(i)*int64(time.Second)
		lat := int64(10 * time.Millisecond)
		if i == 5 {
			lat = int64(4 * time.Second)
		}
		line := []byte(fmt.Sprintf("CONN=0 SEQ=%d TS=%d x", i, sent))
		tr.record(line, sent+lat)
		tr.record(line, sent+lat+int64(time.Minute)) // a late duplicate must not count
	}
	all := tr.latency(0, 0)
	if all.Count != 10 || all.MaxMs != 4000 {
		t.Fatalf("whole run: count %d max %.1f, want 10 and 4000", all.Count, all.MaxMs)
	}
	win := tr.latency(base+int64(6*time.Second), 0)
	if win.Count != 4 || win.MaxMs != 10 {
		t.Fatalf("window: count %d max %.1f, want 4 and 10", win.Count, win.MaxMs)
	}
	tl := tr.timeline()
	var unique, every int32
	for i := range tl.Unique {
		unique += tl.Unique[i]
		every += tl.All[i]
	}
	if unique != 10 || every != 20 {
		t.Fatalf("timeline: unique %d all %d, want 10 and 20", unique, every)
	}
}

// TestSeqTrackerDuplicates checks that a line that arrives again is listed
// with its send time, its first arrival and the arrival of the copy.
func TestSeqTrackerDuplicates(t *testing.T) {
	tr := newSeqTracker(1)
	sent := time.Unix(1000, 0).UnixNano()
	first := sent + int64(200*time.Millisecond)
	again := sent + int64(2*time.Minute)
	for seq := 0; seq < 10; seq++ {
		tr.record([]byte(fmt.Sprintf("CONN=2 SEQ=%d TS=%d pad", seq, sent)), first)
	}
	tr.record([]byte(fmt.Sprintf("CONN=2 SEQ=7 TS=%d pad", sent)), again)

	d := tr.duplicates()
	if d.Count != 1 || len(d.Samples) != 1 || d.Truncated {
		t.Fatalf("duplicates = %+v, want one sample", d)
	}
	s := d.Samples[0]
	if s.Conn != 2 || s.Seq != 7 || s.SentNs != sent || s.FirstNs != first || s.AtNs != again {
		t.Fatalf("sample = %+v, want conn 2 seq 7 sent %d first %d at %d", s, sent, first, again)
	}
}

// TestHTTPInputsTrackSequences checks that lines that reach the receiver over
// HTTP count as arrived, as lines over TCP do: through the HTTP receive mode
// (plain and ES /_bulk bodies) and through the always-on HTTP data endpoint.
func TestHTTPInputsTrackSequences(t *testing.T) {
	saved := seqTrack
	t.Cleanup(func() { seqTrack = saved })

	post := func(h http.Handler, path, body string) {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("POST %s answered %d", path, rec.Code)
		}
	}
	ts := time.Now().UnixNano()
	line := func(conn, seq int) string { return fmt.Sprintf("CONN=%d SEQ=%d TS=%d pad", conn, seq, ts) }

	for name, h := range map[string]func() http.Handler{
		"receive mode":  func() http.Handler { return newHTTPReceiveHandler(config{}, &counters{}, newValidator()) },
		"data endpoint": func() http.Handler { return newHTTPDataHandler(config{}, &counters{}, newValidator()) },
	} {
		t.Run(name, func(t *testing.T) {
			seqTrack = newSeqTracker(1)
			handler := h()
			post(handler, "/", line(0, 0)+"\n"+line(0, 1)+"\n")
			post(handler, "/logs/_bulk", `{"create":{}}`+"\n"+line(0, 2)+"\n"+`{"create":{}}`+"\n"+line(0, 3)+"\n")
			c := seqTrack.check(map[int64]int64{0: 4}, 5)
			if c.Missing != 0 || c.Unique != 4 {
				t.Fatalf("after 4 tracked lines over HTTP: %+v", c)
			}
		})
	}
}

func TestParseAckedParam(t *testing.T) {
	got, err := parseAckedParam("0:12, 3:0,")
	if err != nil || len(got) != 2 || got[0] != 12 || got[3] != 0 {
		t.Fatalf("parseAckedParam: %v %v", got, err)
	}
	if _, err := parseAckedParam("x"); err == nil {
		t.Fatalf("parseAckedParam accepted an entry without a colon")
	}
}

// TestSeqTrackerSkipsACutLine: a record that arrives without its head is a
// distinct line to the hash check, so a loss counted as sent minus distinct
// lines comes out at 0. The tracker finds no sequence number in it: one
// record is missing and one line is unparsed.
func TestSeqTrackerSkipsACutLine(t *testing.T) {
	lines := []string{
		"CONN=0 SEQ=0 aaaaaaaaaaaaaaaaaaaaaaaa",
		"CONN=0 SEQ=1 bbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccc", // the tail of SEQ=2, whose head never arrived
	}
	val := newValidator()
	tr := newSeqTracker(1)
	now := time.Now().UnixNano()
	for _, l := range lines {
		val.recordLine([]byte(l), config{ValidateDedup: true})
		tr.record([]byte(l), now)
	}
	if unique, _ := val.dedupStats(); unique != 3 {
		t.Fatalf("hash check: %d distinct lines, want 3: the cut line counts as a record there", unique)
	}
	got := tr.check(map[int64]int64{0: 3}, 5)
	if got.Unique != 2 || got.Missing != 1 || got.Unparsed != 1 {
		t.Errorf("tracker: unique %d missing %d unparsed %d, want 2 1 1", got.Unique, got.Missing, got.Unparsed)
	}
}

// TestSeqCountsKeepTheHashCountsForLinesWithoutASequence: the tracker is on
// for every scenario case. Rows from a database carry no sequence number, so
// the tracker counts no line, and its zeros must not replace the hash-based
// counts, which see a row that arrived twice.
func TestSeqCountsKeepTheHashCountsForLinesWithoutASequence(t *testing.T) {
	val := newValidator()
	tr := newSeqTracker(1)
	now := time.Now().UnixNano()
	for _, l := range []string{"evt-1 collected", "evt-2 collected", "evt-1 collected"} {
		val.recordLine([]byte(l), config{ValidateDedup: true})
		tr.record([]byte(l), now)
	}
	seqUnique, _ := tr.counts()
	if seqCountsWin(seqUnique, true) {
		t.Fatalf("the tracker's counts (%d distinct) replaced the hash-based ones for lines without a sequence", seqUnique)
	}
	if _, dups := val.dedupStats(); dups != 1 {
		t.Fatalf("hash check: %d duplicates, want 1", dups)
	}
	// Sequenced lines: the tracker's counts win.
	tr.record([]byte("CONN=0 SEQ=0 aaaa"), now)
	if seqUnique, _ := tr.counts(); !seqCountsWin(seqUnique, true) {
		t.Fatalf("the tracker's counts lost to the hash-based ones after it counted a sequenced line")
	}
	// Without the hash-based check there is nothing else to report.
	if !seqCountsWin(0, false) {
		t.Fatalf("the tracker's counts must be reported when no hash-based check runs")
	}
}

// A tracker that samples keeps the latency of one line in N, the lines whose
// SEQ is a multiple of N, and still counts every line as arrived, so that the
// loss check stays exact (RECEIVER_LATENCY_SAMPLE_EVERY).
func TestSeqTrackerKeepsOneLatencySampleInN(t *testing.T) {
	tr := newSeqTracker(10)
	now := time.Now().UnixNano()
	sent := now - int64(20*time.Millisecond)
	for seq := 0; seq < 100; seq++ {
		tr.record([]byte(fmt.Sprintf("CONN=0 SEQ=%d TS=%d pad", seq, sent)), now)
	}

	if unique, dups := tr.counts(); unique != 100 || dups != 0 {
		t.Fatalf("counts = %d unique, %d duplicates, want 100 and 0", unique, dups)
	}
	if got := tr.check(map[int64]int64{0: 100}, 20); got.Missing != 0 || got.ReceivedAcked != 100 {
		t.Fatalf("check: received %d, missing %d, want 100 and 0", got.ReceivedAcked, got.Missing)
	}
	ls := tr.latency(0, 0)
	if ls.Count != 10 || ls.SampleEvery != 10 {
		t.Fatalf("latency: %d samples, sample_every %d, want 10 and 10", ls.Count, ls.SampleEvery)
	}
	if ls.P50Ms != 20 {
		t.Fatalf("latency p50 = %v ms, want 20", ls.P50Ms)
	}

	// Without sampling, every line has a sample and the answer names none
	every := newSeqTracker(1)
	for seq := 0; seq < 100; seq++ {
		every.record([]byte(fmt.Sprintf("CONN=0 SEQ=%d TS=%d pad", seq, sent)), now)
	}
	if ls := every.latency(0, 0); ls.Count != 100 || ls.SampleEvery != 0 {
		t.Fatalf("latency without sampling: %d samples, sample_every %d, want 100 and 0", ls.Count, ls.SampleEvery)
	}
}
