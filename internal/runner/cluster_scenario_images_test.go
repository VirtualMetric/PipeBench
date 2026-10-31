package runner

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestKeptLogsWindow checks that the log of a container a step replaced is
// read by its stamps, so a log check sees the old version's lines in its
// window and none outside it.
func TestKeptLogsWindow(t *testing.T) {
	store := &containerLogStore{byContainer: map[string][]keptLog{}}
	base := time.Date(2026, 10, 29, 4, 0, 0, 0, time.UTC)
	stamp := func(s int) string { return base.Add(time.Duration(s) * time.Second).Format(time.RFC3339Nano) }
	store.keep("bench-subject-vmetric-1", keptLog{Step: 4, Image: "vmetric/director:2.6.0", Text: strings.Join([]string{
		stamp(0) + " Persistent storage activated.",
		stamp(10) + " The director dropped data that had waited longer",
		"a line without a stamp",
		stamp(20) + " Shutting down",
		"",
	}, "\n")})
	store.keep("bench-subject-vmetric-1", keptLog{Step: 9, Image: "vmetric/director:dev", Text: stamp(30) + " Persistent storage activated.\n"})

	got := store.window("bench-subject-vmetric-1", base.Add(5*time.Second), base.Add(25*time.Second))
	if !strings.Contains(got, "dropped data") || !strings.Contains(got, "Shutting down") {
		t.Fatalf("the window lost lines inside it:\n%s", got)
	}
	if strings.Contains(got, "activated") || strings.Contains(got, "without a stamp") {
		t.Fatalf("the window holds lines outside it:\n%s", got)
	}
	// An open window reads both kept logs, oldest first.
	all := store.window("bench-subject-vmetric-1", base, time.Time{})
	if first, last := strings.Index(all, stamp(0)), strings.Index(all, stamp(30)); first < 0 || last < 0 || first > last {
		t.Fatalf("an open window must hold both kept logs in order:\n%s", all)
	}
	if got := store.window("bench-subject-vmetric-2", base, time.Time{}); got != "" {
		t.Fatalf("another node's window holds lines: %q", got)
	}
	// firstMatchingLine, which the log checks use, finds the old version's
	// line through the window.
	at, needle, found := firstMatchingLine(all, []string{"dropped data"}, time.Time{})
	if !found || needle != "dropped data" || !at.Equal(base.Add(10*time.Second)) {
		t.Fatalf("firstMatchingLine = %v %q %v, want the kept line at +10s", at, needle, found)
	}
	store.reset()
	if got := store.window("bench-subject-vmetric-1", base, time.Time{}); got != "" {
		t.Fatalf("a reset store still holds lines: %q", got)
	}
}

// TestAnalyzeLeadershipStreamLeaderless measures the longest time a stream of
// a cluster had no leader, and judges it against the case's limit.
func TestAnalyzeLeadershipStreamLeaderless(t *testing.T) {
	t0 := time.Unix(3000, 0)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	node := func(n int, leaders map[string]string) nodeHealth {
		x := h(n, fmt.Sprint(n), "1")
		x.StreamLeaders = leaders
		return x
	}
	led := map[string]string{"vm-pipeline-sender": "2", "vm-pipeline-router": "1"}
	senderless := map[string]string{"vm-pipeline-sender": "", "vm-pipeline-router": "1"}
	down := nodeHealth{Node: 2, State: "exited"}
	rounds := []healthRound{
		{At: at(0), Nodes: []nodeHealth{node(1, led), node(2, led), node(3, led)}},
		// Node 2 stops. The sender stream's leader was node 2, and nobody
		// leads it until an election ends at second 5.
		{At: at(1), Nodes: []nodeHealth{node(1, senderless), down, node(3, senderless)}},
		{At: at(2), Nodes: []nodeHealth{node(1, senderless), down, node(3, senderless)}},
		{At: at(4), Nodes: []nodeHealth{node(1, senderless), down, node(3, senderless)}},
		{At: at(5), Nodes: []nodeHealth{node(1, led), down, node(3, led)}},
		// One node naming the leader is enough.
		{At: at(6), Nodes: []nodeHealth{node(1, senderless), down, node(3, led)}},
		{At: at(7), Nodes: []nodeHealth{node(1, led), down, node(3, led)}},
	}
	st := analyzeLeadership(rounds, time.Time{})
	if st.StreamLeaderlessMs != 4000 || st.StreamLeaderless == nil || st.StreamLeaderless.Stream != "vm-pipeline-sender" {
		t.Fatalf("stream leaderless = %d ms, %+v; want 4000 ms of vm-pipeline-sender", st.StreamLeaderlessMs, st.StreamLeaderless)
	}
	if st.StreamsReported != 2 {
		t.Fatalf("StreamsReported = %d, want 2", st.StreamsReported)
	}
	limit := int64(5000)
	if reason := judgeStreamLeaderless(st, &limit); reason != "" {
		t.Fatalf("4 s within a 5 s limit failed: %s", reason)
	}
	limit = 3000
	if reason := judgeStreamLeaderless(st, &limit); !strings.Contains(reason, "vm-pipeline-sender had no leader for 4.0 s") {
		t.Fatalf("4 s over a 3 s limit: %q", reason)
	}
	if reason := judgeStreamLeaderless(st, nil); reason != "" {
		t.Fatalf("no limit judged: %q", reason)
	}
	// Standalone directors lead their streams themselves: nothing is
	// measured, and a limit then fails rather than pass on nothing.
	solo := nodeHealth{Node: 1, State: "running", Reachable: true, Name: "1", Phase: "running", VMMQ: "ok",
		StreamLeaders: map[string]string{"vm-pipeline-sender": ""}}
	st = analyzeLeadership([]healthRound{{At: at(0), Nodes: []nodeHealth{solo}}, {At: at(1), Nodes: []nodeHealth{solo}}}, time.Time{})
	if st.StreamLeaderlessMs != 0 || st.StreamsReported != 0 {
		t.Fatalf("a standalone director was measured: %d ms over %d streams", st.StreamLeaderlessMs, st.StreamsReported)
	}
	if reason := judgeStreamLeaderless(st, &limit); !strings.Contains(reason, "no clustered node reported a stream") {
		t.Fatalf("a limit over nothing measured: %q", reason)
	}
}

// A stream still without a leader in the last round has no round after it:
// its stretch lasts until the rounds were read, so one round of it counts.
func TestAnalyzeLeadershipLeaderlessAtTheEnd(t *testing.T) {
	t0 := time.Unix(4000, 0)
	x := h(1, "1", "1")
	x.StreamLeaders = map[string]string{"vm-pipeline-router": ""}
	y := h(2, "2", "1")
	y.StreamLeaders = map[string]string{"vm-pipeline-router": ""}
	rounds := []healthRound{{At: t0, Nodes: []nodeHealth{x, y}}}
	st := analyzeLeadership(rounds, t0.Add(1500*time.Millisecond))
	if st.StreamLeaderlessMs != 1500 {
		t.Fatalf("a stream leaderless in the one round read lasted %d ms, want 1500 (until the rounds were read)", st.StreamLeaderlessMs)
	}
	limit := int64(0)
	if reason := judgeStreamLeaderless(st, &limit); !strings.Contains(reason, "had no leader for 1.5 s") {
		t.Fatalf("a limit of 0 must fail on it: %q", reason)
	}
	// A zero end, or one before the last round, ends it at the last round.
	if st := analyzeLeadership(rounds, time.Time{}); st.StreamLeaderlessMs != 0 {
		t.Fatalf("with no end the stretch lasted %d ms, want 0", st.StreamLeaderlessMs)
	}
	// A negative limit that validation would refuse fails instead of
	// panicking on a stretch that was never measured.
	none := analyzeLeadership([]healthRound{{At: t0, Nodes: []nodeHealth{h(1, "1", "1")}}}, t0)
	none.StreamsReported = 1
	negative := int64(-1)
	if reason := judgeStreamLeaderless(none, &negative); !strings.Contains(reason, "must not be negative") {
		t.Fatalf("a negative limit: %q", reason)
	}
}
