package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func h(node int, name, metaLeader string) nodeHealth {
	return nodeHealth{Node: node, State: "running", Reachable: true, Name: name, Phase: "running", VMMQ: "ok", Clustered: true, MetaLeader: metaLeader}
}

func TestAgreedLeader(t *testing.T) {
	r := healthRound{Nodes: []nodeHealth{h(1, "1", "2"), h(2, "2", "2"), h(3, "3", "2")}}
	if l, ok := agreedLeader(r); !ok || l != 2 {
		t.Fatalf("agreedLeader = %d %v, want 2", l, ok)
	}

	// A node cut off from the others still names itself; the majority names
	// node 1. Only node 1 is agreed.
	r = healthRound{Nodes: []nodeHealth{h(1, "1", "1"), h(2, "2", "1"), h(3, "3", "3")}}
	if l, ok := agreedLeader(r); !ok || l != 1 {
		t.Fatalf("partition: agreedLeader = %d %v, want 1", l, ok)
	}
	if c := claimedLeaders(r); len(c) != 2 {
		t.Fatalf("partition: claimedLeaders = %v, want two", c)
	}

	// The leader is dead and nobody has been elected yet.
	dead := nodeHealth{Node: 2, State: "exited"}
	r = healthRound{Nodes: []nodeHealth{h(1, "1", "2"), dead, h(3, "3", "2")}}
	if _, ok := agreedLeader(r); ok {
		t.Fatalf("a dead leader must not be agreed")
	}

	// A director that is not in a cluster leads itself.
	solo := nodeHealth{Node: 1, State: "running", Reachable: true, Name: "1", Phase: "running", VMMQ: "ok"}
	if l, ok := agreedLeader(healthRound{Nodes: []nodeHealth{solo}}); !ok || l != 1 {
		t.Fatalf("standalone director: %d %v", l, ok)
	}
}

func TestAnalyzeLeadershipMeasuresTwoLeaderTime(t *testing.T) {
	t0 := time.Unix(1000, 0)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	rounds := []healthRound{
		{At: at(0), Nodes: []nodeHealth{h(1, "1", "3"), h(2, "2", "3"), h(3, "3", "3")}},
		{At: at(1), Nodes: []nodeHealth{h(1, "1", "3"), h(2, "2", "3"), h(3, "3", "3")}},
		// node 3 is cut off: the others elect node 1, node 3 still names itself
		{At: at(2), Nodes: []nodeHealth{h(1, "1", "1"), h(2, "2", "1"), h(3, "3", "3")}},
		{At: at(3), Nodes: []nodeHealth{h(1, "1", "1"), h(2, "2", "1"), h(3, "3", "3")}},
		{At: at(4), Nodes: []nodeHealth{h(1, "1", "1"), h(2, "2", "1"), h(3, "3", "")}},
	}
	st := analyzeLeadership(rounds)
	if st.MaxSelfClaimed != 2 {
		t.Fatalf("MaxSelfClaimed = %d, want 2", st.MaxSelfClaimed)
	}
	if st.SplitBrainMs != 2000 || len(st.SplitIntervals) != 1 {
		t.Fatalf("SplitBrainMs = %d, intervals %v; want 2000 in one stretch", st.SplitBrainMs, st.SplitIntervals)
	}
	if st.LeaderChanges != 1 {
		t.Fatalf("LeaderChanges = %d, want 1", st.LeaderChanges)
	}
}

func TestAnalyzeLeadershipStreams(t *testing.T) {
	t0 := time.Unix(2000, 0)
	a := h(1, "1", "1")
	a.StreamLeaders = map[string]string{"vm-pipeline-sender": "1"}
	b := h(2, "2", "1")
	b.StreamLeaders = map[string]string{"vm-pipeline-sender": "2"}
	c := h(2, "2", "1")
	c.StreamLeaders = map[string]string{"vm-pipeline-sender": "1"}
	st := analyzeLeadership([]healthRound{
		{At: t0, Nodes: []nodeHealth{a, b}},
		{At: t0.Add(time.Second), Nodes: []nodeHealth{a, c}},
	})
	if st.MaxStreamClaims != 2 || st.StreamSplitMs != 1000 {
		t.Fatalf("stream split: max %d, %d ms", st.MaxStreamClaims, st.StreamSplitMs)
	}
}

func TestDeliveryGapAfter(t *testing.T) {
	start := time.Unix(5000, 0)
	tl := receiverTimeline{StartNs: start.UnixNano(), BucketMs: 100}
	// 1 s of steady arrivals, a 2.3 s gap, then arrivals again.
	for range 10 {
		tl.Unique = append(tl.Unique, 5)
	}
	for range 23 {
		tl.Unique = append(tl.Unique, 0)
	}
	for range 10 {
		tl.Unique = append(tl.Unique, 5)
	}
	disruption := start.Add(900 * time.Millisecond)
	gap, resume := deliveryGapAfter(tl, disruption, 60*time.Second)
	if gap != 2300 {
		t.Fatalf("gap = %d ms, want 2300", gap)
	}
	if resume != 2400 {
		t.Fatalf("resumed after %d ms, want 2400", resume)
	}
	if g, _ := deliveryGapAfter(receiverTimeline{}, disruption, time.Minute); g != 0 {
		t.Fatalf("an empty timeline has no gap")
	}
}

// TestDeliveryGapAfterWindow checks that a gap belongs to the disruption it
// starts after, and is measured to its end even past the window.
func TestDeliveryGapAfterWindow(t *testing.T) {
	start := time.Unix(5000, 0)
	tl := receiverTimeline{StartNs: start.UnixNano(), BucketMs: 100}
	add := func(n int, v int32) {
		for range n {
			tl.Unique = append(tl.Unique, v)
		}
	}
	// A 1 s gap at 1 s, arrivals, then a 5 s gap at 3 s, then arrivals.
	add(10, 5)
	add(10, 0)
	add(10, 5)
	add(50, 0)
	add(10, 5)

	// The first disruption at 0.9 s, the second at 2.9 s: the first one's
	// window ends where the second begins, so it does not take the 5 s gap.
	gap, resume := deliveryGapAfter(tl, start.Add(900*time.Millisecond), 2*time.Second)
	if gap != 1000 || resume != 1100 {
		t.Fatalf("first disruption: gap %d ms, resumed after %d ms; want 1000, 1100", gap, resume)
	}
	// The second one's 5 s gap starts inside a 1 s window and is measured
	// to its end.
	gap, resume = deliveryGapAfter(tl, start.Add(2900*time.Millisecond), time.Second)
	if gap != 5000 || resume != 5100 {
		t.Fatalf("second disruption: gap %d ms, resumed after %d ms; want 5000, 5100", gap, resume)
	}
}

// TestDeliveryGapAfterBoundaryBetweenBuckets checks that the window is
// bounded by time, not by the bucket a disruption falls in. With 100 ms
// buckets, a disruption at 950 ms and the next at 1050 ms, a gap that starts
// at 1000 ms belongs to the first one.
func TestDeliveryGapAfterBoundaryBetweenBuckets(t *testing.T) {
	start := time.Unix(5000, 0)
	tl := receiverTimeline{StartNs: start.UnixNano(), BucketMs: 100}
	add := func(n int, v int32) {
		for range n {
			tl.Unique = append(tl.Unique, v)
		}
	}
	add(10, 5) // 0-1000 ms
	add(5, 0)  // 1000-1500 ms: the gap
	add(10, 5)

	gap, resume := deliveryGapAfter(tl, start.Add(950*time.Millisecond), 100*time.Millisecond)
	if gap != 500 || resume != 550 {
		t.Fatalf("disruption at 950 ms: gap %d ms, resumed after %d ms; want 500, 550", gap, resume)
	}
	if b := bucketAtOrAfter(tl, start.Add(1000*time.Millisecond)); b != 10 {
		t.Fatalf("bucketAtOrAfter(1000 ms) = %d, want 10", b)
	}
	if b := bucketAtOrAfter(tl, start.Add(1001*time.Millisecond)); b != 11 {
		t.Fatalf("bucketAtOrAfter(1001 ms) = %d, want 11", b)
	}
	if b := bucketAtOrAfter(tl, start.Add(-time.Second)); b != 0 {
		t.Fatalf("bucketAtOrAfter(before the start) = %d, want 0", b)
	}
}

// TestDeliveryGapAfterTwoDisruptionsInOneGap checks that a gap is counted
// once, for the disruption it started after. A second disruption that comes
// while the gap is under way skips it and takes only the gaps that start
// later.
func TestDeliveryGapAfterTwoDisruptionsInOneGap(t *testing.T) {
	start := time.Unix(5000, 0)
	tl := receiverTimeline{StartNs: start.UnixNano(), BucketMs: 100}
	add := func(n int, v int32) {
		for range n {
			tl.Unique = append(tl.Unique, v)
		}
	}
	add(10, 5) // 0-1 s
	add(20, 0) // 1-3 s: one gap
	add(10, 5) // 3-4 s
	add(5, 0)  // 4-4.5 s: a later gap
	add(10, 5)

	gap, resume := deliveryGapAfter(tl, start.Add(950*time.Millisecond), 1050*time.Millisecond)
	if gap != 2000 || resume != 2050 {
		t.Fatalf("first disruption: gap %d ms, resumed after %d ms; want 2000, 2050", gap, resume)
	}
	// The second disruption at 2 s falls inside the first gap: it must not
	// report the rest of that gap, only the later one.
	gap, resume = deliveryGapAfter(tl, start.Add(2*time.Second), time.Minute)
	if gap != 500 || resume != 2500 {
		t.Fatalf("second disruption: gap %d ms, resumed after %d ms; want 500, 2500", gap, resume)
	}
	// With no later gap, it reports none.
	gap, _ = deliveryGapAfter(tl, start.Add(2*time.Second), 1500*time.Millisecond)
	if gap != 0 {
		t.Fatalf("second disruption with a short window: gap %d ms, want 0", gap)
	}
}

func TestArrivalsAfter(t *testing.T) {
	start := time.Unix(5000, 0)
	tl := receiverTimeline{StartNs: start.UnixNano(), BucketMs: 100}
	// 1 s of arrivals, 2 s without, then one bucket of 7 lines.
	for range 10 {
		tl.Unique = append(tl.Unique, 5)
	}
	for range 20 {
		tl.Unique = append(tl.Unique, 0)
	}
	tl.Unique = append(tl.Unique, 7)

	last, n := arrivalsAfter(tl, start.Add(500*time.Millisecond))
	if n != 5*5+7 {
		t.Fatalf("%d lines after the disruption, want 32", n)
	}
	if want := start.Add(3100 * time.Millisecond); !last.Equal(want) {
		t.Fatalf("last arrival at %s, want %s", last, want)
	}

	// Nothing arrived after the disruption: the last arrival is the disruption.
	at := start.Add(10 * time.Second)
	if last, n := arrivalsAfter(tl, at); n != 0 || !last.Equal(at) {
		t.Fatalf("arrivalsAfter(after the last line) = %s, %d; want %s, 0", last, n, at)
	}
	if last, n := arrivalsAfter(receiverTimeline{}, at); n != 0 || !last.Equal(at) {
		t.Fatalf("an empty timeline has no arrivals")
	}
}

func TestSummarizeDuplicates(t *testing.T) {
	base := time.Unix(2000, 0)
	ns := func(d time.Duration) int64 { return base.Add(d).UnixNano() }
	samples := []dupSample{
		// A batch of three lines on CONN 1, copied two minutes later.
		{Conn: 1, Seq: 12, SentNs: ns(0), FirstNs: ns(time.Second), AtNs: ns(121 * time.Second)},
		{Conn: 1, Seq: 10, SentNs: ns(0), FirstNs: ns(time.Second), AtNs: ns(121 * time.Second)},
		{Conn: 1, Seq: 11, SentNs: ns(0), FirstNs: ns(time.Second), AtNs: ns(121 * time.Second)},
		// A separate line on CONN 1, and one on CONN 0 whose first arrival
		// is unknown.
		{Conn: 1, Seq: 50, SentNs: ns(5 * time.Second), FirstNs: ns(6 * time.Second), AtNs: ns(7 * time.Second)},
		{Conn: 0, Seq: 3, SentNs: ns(time.Second), AtNs: ns(9 * time.Second)},
	}
	got := summarizeDuplicates(samples, 10)
	if len(got) != 3 {
		t.Fatalf("got %d ranges, want 3: %+v", len(got), got)
	}
	if r := got[0]; r.Conn != 0 || r.FromSeq != 3 || r.CopyAfterMs != 0 {
		t.Fatalf("first range = %+v", r)
	}
	if r := got[1]; r.Conn != 1 || r.FromSeq != 10 || r.ToSeq != 12 || r.Copies != 3 || r.CopyAfterMs != 120000 {
		t.Fatalf("second range = %+v, want conn 1 seq 10-12, 3 copies, copy after 120000 ms", r)
	}
	if r := got[2]; r.FromSeq != 50 || r.CopyAfterMs != 1000 {
		t.Fatalf("third range = %+v", r)
	}
	if len(summarizeDuplicates(samples, 1)) != 1 {
		t.Fatalf("the limit was not applied")
	}
}

// TestDrainSeqCheckNeverAnswered checks that a drain in which no read of the
// receiver succeeded says so, with the last error, instead of handing back
// a zero check that reads as no loss.
func TestDrainSeqCheckNeverAnswered(t *testing.T) {
	calls := 0
	read := func() (seqCheckResult, error) {
		calls++
		return seqCheckResult{}, errors.New("connection refused")
	}
	_, checked, err := drainSeqCheck(context.Background(), read, time.Now().Add(30*time.Millisecond), time.Minute, 5*time.Millisecond)
	if checked {
		t.Fatalf("no read succeeded, but the drain reports a check")
	}
	if err == nil || err.Error() != "connection refused" {
		t.Fatalf("err = %v, want the last read error", err)
	}
	if calls < 2 {
		t.Fatalf("the drain gave up after %d read(s), want it to retry until the deadline", calls)
	}
}

// TestDrainSeqCheckKeepsLastAnswer checks that a drain returns the last
// check that succeeded, even when later reads fail, and stops as soon as
// nothing is missing.
func TestDrainSeqCheckKeepsLastAnswer(t *testing.T) {
	answers := []struct {
		c   seqCheckResult
		err error
	}{
		{seqCheckResult{Missing: 5, ReceivedAcked: 95}, nil},
		{seqCheckResult{}, errors.New("timeout")},
		{seqCheckResult{Missing: 0, ReceivedAcked: 100}, nil},
	}
	i := 0
	read := func() (seqCheckResult, error) {
		a := answers[min(i, len(answers)-1)]
		i++
		return a.c, a.err
	}
	c, checked, err := drainSeqCheck(context.Background(), read, time.Now().Add(time.Minute), time.Minute, time.Millisecond)
	if !checked || err != nil || c.Missing != 0 || c.ReceivedAcked != 100 {
		t.Fatalf("drain = %+v %v %v, want the final complete check", c, checked, err)
	}

	// A read that fails after a good one leaves the good one in place.
	i = 0
	answers = answers[:2]
	c, checked, err = drainSeqCheck(context.Background(), read, time.Now().Add(20*time.Millisecond), time.Minute, time.Millisecond)
	if !checked || err != nil || c.Missing != 5 {
		t.Fatalf("drain = %+v %v %v, want the earlier good check", c, checked, err)
	}
}

func TestParseGeneratorAck(t *testing.T) {
	out := `{
  "lines_sent": 30,
  "bytes_sent": 900,
  "duration_ms": 1000,
  "ack": {
    "targets": ["http://subject-1:9000/"],
    "lines_acked": 30,
    "acked_per_conn": {"0": 20, "1": 10}
  }
}
`
	g := parseGeneratorAck(out)
	if g == nil || g.Ack == nil || g.Ack.LinesAcked != 30 || g.Ack.AckedPerConn["1"] != 10 {
		t.Fatalf("parseGeneratorAck: %+v", g)
	}
	if parseGeneratorAck(`{"lines_sent": 5}`) != nil {
		t.Fatalf("a result without the ack summary is not an http_ack result")
	}
}

func TestProgressLineMatches(t *testing.T) {
	line := `PROGRESS t_ms=1792775180000 acked=12340 unacked=0 requests=1300 failures=6 failovers=6 pending=10 last_ack_ns=1792775179999000000 last_error="x"`
	m := progressRE.FindStringSubmatch(line)
	if m == nil || m[2] != "12340" || m[3] != "6" || m[4] != "10" || m[5] != "1792775179999000000" {
		t.Fatalf("progress line not parsed: %v", m)
	}
}
