package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
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

func TestMarkAtTakesTheLatestMark(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	sr := &scenarioRun{marks: []scenarioMark{
		{Label: "held", At: t0},
		{Label: "other", At: t0.Add(time.Second)},
		{Label: "held", At: t0.Add(2 * time.Second)},
	}}
	if at, ok := sr.markAt("held"); !ok || !at.Equal(t0.Add(2*time.Second)) {
		t.Fatalf("markAt(held) = %v, %v; want the second held mark", at, ok)
	}
	if _, ok := sr.markAt("ghost"); ok {
		t.Fatalf("markAt(ghost) found a mark that was never set")
	}
}

func TestArrivedSince(t *testing.T) {
	cases := []struct{ atStep, atEnd, want int64 }{
		{atStep: 1000, atEnd: 3280, want: 2280},
		{atStep: 1000, atEnd: 1000, want: 0},
		// A receiver that restarted counts again from zero; that is not a
		// negative number of late lines.
		{atStep: 1000, atEnd: 400, want: 0},
	}
	for _, c := range cases {
		if got := arrivedSince(c.atStep, c.atEnd); got != c.want {
			t.Errorf("arrivedSince(%d, %d) = %d, want %d", c.atStep, c.atEnd, got, c.want)
		}
	}
}

func TestFirstMatchingLine(t *testing.T) {
	out := "2026-10-24T01:30:25.731000000Z Moving device 1 off node 1\n" +
		"not a timestamp Device pg-bench placement has been changed to 2\n" +
		"2026-10-24T01:30:25.732000000Z Device pg-bench placement has been changed to 2. Stopping the collector...\n" +
		"2026-10-24T01:30:26.000000000Z Ownership lease for device 1 (pg-bench) lost on node 1\n"
	needles := []string{"lost on node", "placement has been changed to"}
	at, line, ok := firstMatchingLine(out, needles, time.Time{})
	if !ok || line != "placement has been changed to" {
		t.Fatalf("firstMatchingLine = %v, %q, %v; want the placement line, the first with a timestamp", at, line, ok)
	}
	if want := time.Date(2026, 10, 24, 1, 30, 25, 732000000, time.UTC); !at.Equal(want) {
		t.Fatalf("at = %v, want %v", at, want)
	}
	if _, _, ok := firstMatchingLine(out, []string{"never logged"}, time.Time{}); ok {
		t.Fatalf("firstMatchingLine found a line that is not there")
	}
}

func TestQuoteLines(t *testing.T) {
	if got := quoteLines([]string{"a"}); got != `"a"` {
		t.Fatalf("quoteLines(a) = %s", got)
	}
	if got := quoteLines([]string{"a", "b"}); got != `"a" or "b"` {
		t.Fatalf("quoteLines(a, b) = %s", got)
	}
}

func TestFirstMatchingLineStopsAtTheWindowEnd(t *testing.T) {
	out := "2026-10-24T01:30:25.000000000Z still inside: Starting event collection of x\n" +
		"2026-10-24T01:30:41.000000000Z after the end: Starting event collection of x\n"
	needles := []string{"Starting event collection of"}
	end := time.Date(2026, 10, 24, 1, 30, 40, 0, time.UTC)
	if _, _, ok := firstMatchingLine(out[strings.Index(out, "\n")+1:], needles, end); ok {
		t.Fatalf("a line stamped after the window's end counted")
	}
	if at, _, ok := firstMatchingLine(out, needles, end); !ok || !at.Equal(time.Date(2026, 10, 24, 1, 30, 25, 0, time.UTC)) {
		t.Fatalf("firstMatchingLine = %v, %v; want the line inside the window", at, ok)
	}
}

func TestEarliestLogMatchAcrossNodes(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 1, 0, 0, 0, time.UTC)
	found := map[int]time.Time{1: t0.Add(8 * time.Second), 2: t0.Add(3 * time.Second)}
	n, at, line, ok, err := earliestLogMatch([]int{1, 2, 3}, func(n int) (time.Time, string, bool, error) {
		at, ok := found[n]
		return at, "lost on node", ok, nil
	})
	if err != nil || !ok || n != 2 || !at.Equal(t0.Add(3*time.Second)) || line != "lost on node" {
		t.Fatalf("earliestLogMatch = node %d at %v (%q, %v, %v); want node 2, the earlier line", n, at, line, ok, err)
	}
	if _, _, _, ok, err := earliestLogMatch([]int{1, 2}, func(int) (time.Time, string, bool, error) { return time.Time{}, "", false, nil }); ok || err != nil {
		t.Fatalf("earliestLogMatch found a match no node had (%v)", err)
	}
}

func TestEarliestLogMatchNamesTheNodesItCouldNotRead(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 1, 0, 0, 0, time.UTC)
	n, _, _, ok, err := earliestLogMatch([]int{1, 2, 3}, func(n int) (time.Time, string, bool, error) {
		switch n {
		case 1:
			return t0, "lost on node", true, nil
		case 3:
			return time.Time{}, "", false, errors.New("docker logs failed")
		}
		return time.Time{}, "", false, nil
	})
	if !ok || n != 1 {
		t.Fatalf("earliestLogMatch = node %d (%v); want node 1's line", n, ok)
	}
	if err == nil || !strings.Contains(err.Error(), "node 3: docker logs failed") {
		t.Fatalf("earliestLogMatch error = %v; want node 3's read error", err)
	}
}

// stubLogs makes the log reads of one test answer from logs, and fail with the
// error fail holds for a container.
func stubLogs(t *testing.T, logs map[string]string, fail map[string]error) {
	t.Helper()
	orig := readLogWindow
	readLogWindow = func(container string, since, until time.Time) (string, error) {
		if err, ok := fail[container]; ok {
			return "", err
		}
		return logs[container], nil
	}
	t.Cleanup(func() { readLogWindow = orig })
}

func TestExpectNoLogFailsOnALogItCannotRead(t *testing.T) {
	stubLogs(t, nil, map[string]error{"node-2": errors.New("docker logs node-2: exit status 1: the daemon did not answer")})
	sr := &scenarioRun{ctx: context.Background(), containers: []string{"node-1", "node-2", "node-3"}, active: map[int]bool{1: true, 2: true, 3: true}}
	for _, node := range []string{"2", "all"} {
		st := config.ScenarioStep{Do: "expect_no_log", Node: node, Log: "Falling back to in-process", For: "1ms"}
		var rep scenarioStepReport
		ok, detail, err := sr.doStep(0, st, &rep)
		if err != nil {
			t.Fatalf("node %s: %v", node, err)
		}
		if ok {
			t.Fatalf("node %s: expect_no_log passed on a log it could not read: %s", node, detail)
		}
		if !strings.Contains(detail, "could not read the log") || !strings.Contains(detail, "the daemon did not answer") {
			t.Fatalf("node %s: detail %q does not say the log could not be read and why", node, detail)
		}
	}
}

func TestExpectNoLogSkipsASpareThatNeverStarted(t *testing.T) {
	noContainer := func(c string) error { return fmt.Errorf("%w: %s", errNoContainer, c) }
	stubLogs(t, nil, map[string]error{"node-4": noContainer("node-4")})
	sr := &scenarioRun{ctx: context.Background(), containers: []string{"node-1", "node-2", "node-3", "node-4"}, active: map[int]bool{1: true, 2: true, 3: true}}
	st := config.ScenarioStep{Do: "expect_no_log", Node: "all", Log: "Falling back to in-process", For: "1ms"}
	var rep scenarioStepReport
	if ok, detail, err := sr.doStep(0, st, &rep); err != nil || !ok {
		t.Fatalf("expect_no_log failed on a spare node that never started: %v %s", err, detail)
	}
	// A member without a container is not a spare: its log is missing.
	sr.active[4] = true
	if ok, detail, err := sr.doStep(0, st, &rep); err != nil || ok {
		t.Fatalf("expect_no_log passed on a member with no container: %v %s", err, detail)
	}
}

// docker stop sends SIGTERM and, when the grace period runs out, SIGKILL. A
// node that needed the SIGKILL did not stop gracefully, and the stop step
// used to pass all the same.
func TestStopStepFailsWhenDockerHadToKillTheNode(t *testing.T) {
	var ran [][]string
	origCmd := dockerCmd
	dockerCmd = func(_ context.Context, _ time.Duration, args ...string) error {
		ran = append(ran, args)
		return nil
	}
	t.Cleanup(func() { dockerCmd = origCmd })
	sr := &scenarioRun{ctx: context.Background(), containers: []string{"node-1", "node-2"}, active: map[int]bool{1: true, 2: true}, down: map[int]bool{}}
	st := config.ScenarioStep{Do: "stop", Node: "2", Grace: "10s"}

	stubContainerExit(t, 137)
	var rep scenarioStepReport
	ok, detail, err := sr.doStep(0, st, &rep)
	if err != nil {
		t.Fatal(err)
	}
	if ok || detail != "node 2 did not exit within 10s of the SIGTERM, so Docker killed it (exit code 137)" {
		t.Fatalf("a stop that ended in SIGKILL: ok %v, detail %q; want the step failed and the kill named", ok, detail)
	}
	if len(ran) != 1 || strings.Join(ran[0], " ") != "stop -t 10 node-2" {
		t.Fatalf("docker commands = %v, want one stop of node-2 with a 10 s grace period", ran)
	}

	stubContainerExit(t, 0)
	if ok, detail, err := sr.doStep(0, st, &rep); err != nil || !ok {
		t.Fatalf("a stop that exited by itself: ok %v, detail %q, err %v; want the step passed", ok, detail, err)
	}
}

func TestWaitStableFailsOnALogItCannotRead(t *testing.T) {
	stubLogs(t, nil, map[string]error{"node-2": errors.New("docker logs node-2: exit status 1: the daemon did not answer")})
	stubRunning(t, func(string) (bool, error) { return true, nil })
	sr := &scenarioRun{ctx: context.Background(), containers: []string{"node-1", "node-2"}, active: map[int]bool{1: true, 2: true}}
	st := config.ScenarioStep{Do: "wait_stable", For: "1s", Within: "1s"}
	var rep scenarioStepReport
	ok, detail, err := sr.doStep(0, st, &rep)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("wait_stable passed on a log it could not read: %s", detail)
	}
	if !strings.Contains(detail, "could not read node 2's log") {
		t.Fatalf("detail %q does not name the node whose log could not be read", detail)
	}
}

// stubRunning makes the running checks of one test answer from running.
func stubRunning(t *testing.T, running func(string) (bool, error)) {
	t.Helper()
	orig := containerRunningState
	containerRunningState = running
	t.Cleanup(func() { containerRunningState = orig })
}

// wait_stable used to skip a node that docker could not inspect, as if the
// node had stopped. When that was every node, it passed without reading a log.
func TestWaitStableFailsWhenDockerCannotSayANodeRuns(t *testing.T) {
	stubLogs(t, nil, nil)
	stubRunning(t, func(c string) (bool, error) {
		if c == "node-2" {
			return false, errors.New("docker inspect node-2: exit status 1: the daemon did not answer")
		}
		return true, nil
	})
	sr := &scenarioRun{ctx: context.Background(), containers: []string{"node-1", "node-2"}, active: map[int]bool{1: true, 2: true}}
	st := config.ScenarioStep{Do: "wait_stable", For: "1s", Within: "1s"}
	var rep scenarioStepReport
	ok, detail, err := sr.doStep(0, st, &rep)
	if err != nil {
		t.Fatal(err)
	}
	if ok || !strings.Contains(detail, "could not tell whether node 2 runs") {
		t.Fatalf("ok %v, detail %q; want the step failed and node 2's unknown state named", ok, detail)
	}
}

// wait_log without a node skips the nodes that do not run. A node that
// docker could not inspect used to be skipped as well, without a word.
func TestWaitLogNamesANodeDockerCannotInspect(t *testing.T) {
	stubLogs(t, nil, nil)
	stubRunning(t, func(string) (bool, error) {
		return false, errors.New("docker inspect: exit status 1: the daemon did not answer")
	})
	sr := &scenarioRun{ctx: context.Background(), containers: []string{"node-1", "node-2"}, active: map[int]bool{1: true, 2: true}}
	st := config.ScenarioStep{Do: "wait_log", Log: "became leader on", Within: "1ms"}
	var rep scenarioStepReport
	ok, detail, err := sr.doStep(0, st, &rep)
	if err != nil {
		t.Fatal(err)
	}
	if ok || !strings.Contains(detail, "could not tell whether node") || !strings.Contains(detail, "the daemon did not answer") {
		t.Fatalf("ok %v, detail %q; want the step failed with the inspect error named", ok, detail)
	}
}

// firstLogLineSince used to drop the read error, so its callers took a log
// that could not be read for a log without the line.
func TestFirstLogLineSinceReturnsTheReadError(t *testing.T) {
	stubLogs(t, nil, map[string]error{"node-1": errors.New("docker logs node-1: exit status 1: the daemon did not answer")})
	if _, found, err := firstLogLineSince("node-1", "became leader on", time.Now()); found || err == nil {
		t.Fatalf("found %v, err %v; want the read error", found, err)
	}
}

func TestReadProgressBaselineRetries(t *testing.T) {
	calls := 0
	read := func() (genProgress, bool) {
		calls++
		return genProgress{Acked: 500}, calls == 3
	}
	if p, ok := readProgressBaseline(context.Background(), read, 5, time.Millisecond); !ok || p.Acked != 500 || calls != 3 {
		t.Fatalf("readProgressBaseline = %+v, %v after %d reads; want the third read", p, ok, calls)
	}
	never := func() (genProgress, bool) { return genProgress{}, false }
	if _, ok := readProgressBaseline(context.Background(), never, 3, time.Millisecond); ok {
		t.Fatalf("readProgressBaseline reported a baseline no read returned")
	}
}

func TestSummaryLeavesOutAnUnfinishedPendingCount(t *testing.T) {
	later := int64(820)
	rep := scenarioReport{Pending: []pendingCount{
		{Name: "held_by_killed_nodes", ArrivedLater: &later},
		{Name: "not_counted"},
	}}
	sum := rep.summary()
	if sum["pending_held_by_killed_nodes"] != int64(820) {
		t.Fatalf("summary pending_held_by_killed_nodes = %v, want 820", sum["pending_held_by_killed_nodes"])
	}
	if _, ok := sum["pending_not_counted"]; ok {
		t.Fatalf("summary reports a count that was never finished")
	}
}

func TestLineCountsStayApartFromTimes(t *testing.T) {
	sr := &scenarioRun{measures: map[string]float64{}, counts: map[string]int64{}}
	sr.record("held_writes", 1500)
	sr.recordCount("held_writes", "_lines_acked", 820)
	sr.recordCount("", "_lines_after", 7)

	if _, ok := sr.measures["held_writes_lines_acked"]; ok {
		t.Fatalf("a line count was stored with the times: %v", sr.measures)
	}
	if len(sr.counts) != 1 || sr.counts["held_writes_lines_acked"] != 820 {
		t.Fatalf("counts = %v, want only held_writes_lines_acked = 820", sr.counts)
	}

	rep := scenarioReport{Measures: sr.measures, Counts: sr.counts}
	sum := rep.summary()
	if sum["ms_held_writes"] != float64(1500) {
		t.Fatalf("summary ms_held_writes = %v, want 1500", sum["ms_held_writes"])
	}
	if sum["count_held_writes_lines_acked"] != int64(820) {
		t.Fatalf("summary count_held_writes_lines_acked = %v, want 820", sum["count_held_writes_lines_acked"])
	}
	if _, ok := sum["ms_held_writes_lines_acked"]; ok {
		t.Fatalf("summary exports a line count as milliseconds")
	}

	raw, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Measures map[string]float64 `json:"measures_ms"`
		Counts   map[string]int64   `json:"counts"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Measures["held_writes_lines_acked"]; ok || doc.Counts["held_writes_lines_acked"] != 820 {
		t.Fatalf("report measures_ms = %v, counts = %v; want the count under counts only", doc.Measures, doc.Counts)
	}
}
