package main

import (
	"fmt"
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
	tr := newSeqTracker()
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
	tr := newSeqTracker()
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
	tr := newSeqTracker()
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

func TestParseAckedParam(t *testing.T) {
	got, err := parseAckedParam("0:12, 3:0,")
	if err != nil || len(got) != 2 || got[0] != 12 || got[3] != 0 {
		t.Fatalf("parseAckedParam: %v %v", got, err)
	}
	if _, err := parseAckedParam("x"); err == nil {
		t.Fatalf("parseAckedParam accepted an entry without a colon")
	}
}
