package runner

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

func TestLiveStoreMembers(t *testing.T) {
	lines := parseDockerLogLines(`2026-10-23T16:40:40.000Z 172.20.0.9 - - [23/Oct/2026:16:40:40 +0000] "PUT /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-4-old HTTP/1.1" 201 -
2026-10-23T16:40:47.000Z 172.20.0.5 - - [23/Oct/2026:16:40:47 +0000] "PUT /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-1-b29e HTTP/1.1" 201 -
2026-10-23T16:40:48.000Z 172.20.0.6 - - [23/Oct/2026:16:40:48 +0000] "PUT /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-2-3a9f HTTP/1.1" 201 -
2026-10-23T16:40:48.500Z 172.20.0.6 - - [23/Oct/2026:16:40:48 +0000] "GET /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-3-f4d7 HTTP/1.1" 200 302
2026-10-23T16:40:49.000Z 172.20.0.5 - - [23/Oct/2026:16:40:49 +0000] "PUT /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-1-b29e HTTP/1.1" 201 -
2026-10-23T16:40:49.500Z 172.20.0.8 - - [23/Oct/2026:16:40:49 +0000] "PUT /devstoreaccount1/bench-in/shared%2Fmembers%2Fnot-a-member HTTP/1.1" 201 -
`)
	got := liveStoreMembers(parseAzuriteCalls(lines), "director-state", 6*time.Second)
	want := []string{"subject-1-b29e", "subject-2-3a9f"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("live members = %v, want %v: a record renewed long ago, a read, and a write to another container do not count", got, want)
	}
	if got := liveStoreMembers(nil, "director-state", time.Second); len(got) != 0 {
		t.Errorf("no calls: got %v", got)
	}
}

func TestJudgeStream(t *testing.T) {
	nothing := config.SharedStateStream{Name: "blob", ExpectNothing: true}
	if got := judgeStream(nothing, 0, ReceiverMetrics{}); len(got) != 0 {
		t.Errorf("a stream that expects nothing, whose sender sent nothing: %v, want no failure", got)
	}
	if got := judgeStream(nothing, 300, ReceiverMetrics{LinesReceived: 5}); len(got) != 1 || !strings.Contains(got[0], "5 records arrived") {
		t.Errorf("a stream that expects nothing, with arrivals: %v, want one failure naming them", got)
	}
	normal := config.SharedStateStream{Name: "syslog", MaxLoss: 2, MaxDuplicatePct: 2}
	if got := judgeStream(normal, 0, ReceiverMetrics{}); len(got) != 1 || !strings.Contains(got[0], "sent nothing") {
		t.Errorf("a normal stream whose sender sent nothing: %v, want that failure", got)
	}
	if got := judgeStream(normal, 100, ReceiverMetrics{LinesReceived: 100, UniqueLines: 98, Duplicates: 1}); len(got) != 0 {
		t.Errorf("2 lost and about 1%% repeated, within bounds: %v", got)
	}
	if got := judgeStream(normal, 100, ReceiverMetrics{LinesReceived: 100, UniqueLines: 97, Duplicates: 3}); len(got) != 2 {
		t.Errorf("3 lost and about 3%% repeated: %v, want two failures", got)
	}
	limit := int64(3600)
	counted := config.SharedStateStream{Name: "blob", MaxDuplicates: &limit}
	if got := judgeStream(counted, 9000, ReceiverMetrics{LinesReceived: 12600, UniqueLines: 9000, Duplicates: 3600}); len(got) != 0 {
		t.Errorf("max_duplicates replaces the share: 3600 repeated of 9000 is within a bound of 3600: %v", got)
	}
	if got := judgeStream(counted, 9000, ReceiverMetrics{LinesReceived: 12601, UniqueLines: 9000, Duplicates: 3601}); len(got) != 1 || !strings.Contains(got[0], "max_duplicates 3600") {
		t.Errorf("3601 repeated: %v, want one failure naming max_duplicates", got)
	}
}

func TestOutageBaseline(t *testing.T) {
	calls := 0
	count := func() map[string]int64 { calls++; return map[string]int64{"syslog": 1304} }

	early := newOutageBaseline(time.Now())
	early.take(count)
	if early.counts != nil || calls != 0 {
		t.Fatalf("a count taken at the start of an outage would let records sent before it pass for growth: counts %v after %d calls", early.counts, calls)
	}

	due := newOutageBaseline(time.Now().Add(-outageBaselineDelay))
	due.take(count)
	due.take(count)
	if due.counts["syslog"] != 1304 || calls != 1 {
		t.Errorf("once the delay has passed the streams are counted once: counts %v after %d calls", due.counts, calls)
	}

	growth := outageGrowth(map[string]int64{"syslog": 1304, "blob": 40}, map[string]int64{"syslog": 1304, "blob": 60})
	if growth["syslog"] != 0 || growth["blob"] != 20 {
		t.Errorf("growth = %v, want syslog 0 and blob 20", growth)
	}
}

func TestLogOncePerRun(t *testing.T) {
	const (
		start  = "2026-10-24T08:0%d:00.000Z [2026-10-24 08:00:00] [Information] [director] vmetric-director started; reason recorded as unknown."
		notice = "2026-10-24T08:0%d:01.000Z [2026-10-24 08:00:01] [Warning] [director] This director runs in crash-resistant mode, so it ignores shared_state.write_interval. In this mode the director confirms each write only once the write is in the store."
		other  = "2026-10-24T08:0%d:02.000Z [2026-10-24 08:00:02] [Information] [director] The state bus is ready"
		want   = "so it ignores shared_state.write_interval"
	)
	log := func(lines ...string) []ssLine {
		var text []string
		for i, l := range lines {
			text = append(text, fmt.Sprintf(l, i))
		}
		return parseDockerLogLines(strings.Join(text, "\n"))
	}
	for name, tc := range map[string]struct {
		lines []ssLine
		bad   []string // a part of each problem, in order
	}{
		"once in each run": {log(start, notice, other, start, notice), nil},
		// The whole log holds the line once per start, so a count over the
		// whole log passed it.
		"twice in the first run and never in the second": {
			log(start, notice, notice, start, other),
			[]string{"2 times in run 1 of its director (of 2)", "did not log \"" + want + "\" in run 2 of its director (of 2)"},
		},
		"never":                    {log(start, other), []string{"did not log \"" + want + "\" in run 1"}},
		"before the director runs": {log(notice, start, notice), []string{"once before its director first started"}},
		"no start line":            {log(other, notice), []string{"never logged \"vmetric-director started\""}},
	} {
		t.Run(name, func(t *testing.T) {
			got := logOnceProblems(tc.lines, want)
			if len(got) != len(tc.bad) {
				t.Fatalf("problems = %q, want %d of them", got, len(tc.bad))
			}
			for i, part := range tc.bad {
				if !strings.Contains(got[i], part) {
					t.Errorf("problem %d = %q, want it to say %q", i, got[i], part)
				}
			}
		})
	}
	// A line that only the start line holds would be found once a run
	// whatever the director did.
	if got := logOnceProblems(log(start, other, start), "reason recorded as unknown"); len(got) != 1 || !strings.Contains(got[0], "cannot tell one run from the next") {
		t.Errorf("a want that the start line holds: problems = %q", got)
	}
}

func TestStateWritesBesideSnapshots(t *testing.T) {
	base := time.Date(2026, 10, 23, 22, 26, 39, 0, time.UTC)
	at := func(s float64) time.Time { return base.Add(time.Duration(s * float64(time.Second))) }
	// What the 10-second run wrote: every 10 s, and a snapshot round at 75 s
	// that wrote what was waiting twice, after which the interval counted on.
	writes := []time.Time{at(0), at(10), at(20), at(30), at(40), at(50), at(60), at(70), at(75), at(75.007), at(85.007), at(95.007)}
	snaps := []time.Time{at(75.002), at(75.005), at(75.008), at(75.011)}
	forced := withSnapshot(writes, snaps)
	if !forced[8] || !forced[9] || forced[7] || forced[10] {
		t.Fatalf("with snapshot = %v: only the two writes at 75 s came with it", forced)
	}
	if problems, judged := judgeWriteGaps(writes, forced, 10*time.Second, 2*time.Second); len(problems) != 0 || judged != 9 {
		t.Errorf("10 s interval with a snapshot round: problems %v, judged %d; want none and 9", problems, judged)
	}
	if rounds := writeRounds(snaps, time.Second); len(rounds) != 1 {
		t.Errorf("four snapshot writes within 10 ms are %d rounds, want 1", len(rounds))
	}

	// A 300-second interval whose every write a snapshot asked for, every 75 s:
	// no gap follows the interval, and none is judged.
	writes = []time.Time{at(0), at(75), at(150), at(225), at(300)}
	snaps = []time.Time{at(0.003), at(75.003), at(150.003), at(225.003), at(300.003)}
	if problems, judged := judgeWriteGaps(writes, withSnapshot(writes, snaps), 300*time.Second, 30*time.Second); len(problems) != 0 || judged != 0 {
		t.Errorf("every write with a snapshot: problems %v, judged %d; want none judged", problems, judged)
	}
	// And an interval write too early is named.
	writes = []time.Time{at(0), at(5)}
	if problems, _ := judgeWriteGaps(writes, []bool{false, false}, 10*time.Second, 2*time.Second); len(problems) != 1 {
		t.Errorf("a write 5 s after the last with a 10 s interval: %v, want one problem", problems)
	}
}

// TestRollingRestartStopsAtAFailedStep: a replica that did not rejoin, or a
// replica set that did not settle, ends the rolling restart before the next
// replica is stopped.
func TestRollingRestartStopsAtAFailedStep(t *testing.T) {
	var called []int
	if inTurn([]int{2, 3, 1}, func(n int) bool { called = append(called, n); return n != 3 }) {
		t.Error("inTurn reported success although a step failed")
	}
	if !reflect.DeepEqual(called, []int{2, 3}) {
		t.Errorf("steps called = %v, want [2 3]: the leader, 1, must not be stopped while 3 is missing", called)
	}
	called = nil
	if !inTurn([]int{2, 3, 1}, func(n int) bool { called = append(called, n); return true }) || !reflect.DeepEqual(called, []int{2, 3, 1}) {
		t.Errorf("steps called = %v, want every replica in order", called)
	}
}

// TestEndReplicaFailsOnAFailedRead: the read after the harness ends a
// replica is the last one of that container, so a failed read fails the run,
// and the final checks still read every replica that ran, whatever its book
// holds.
func TestEndReplicaFailsOnAFailedRead(t *testing.T) {
	reads := map[string]int{}
	fail := true
	book := newSSLogBook()
	book.read = func(container string, _ time.Time) ([]byte, error) {
		reads[container]++
		if fail {
			return nil, errors.New("no such container")
		}
		return []byte("2026-10-24T08:00:00.000Z [2026-10-24 08:00:00] [Information] [director] vmetric-director started; reason recorded as unknown.\n"), nil
	}
	run := &ssRun{
		book:    book,
		running: map[int]bool{1: true},
		ran:     map[int]bool{1: true},
		replicas: []ssReplica{
			{N: 1, Service: "subject-1", Container: "bench-subject-vmetric-1"},
			{N: 2, Service: "subject-2", Container: "bench-subject-vmetric-2"},
		},
	}
	if run.endReplica(1, time.Now(), "docker stop") {
		t.Error("endReplica reported a failed read as a success")
	}
	if len(run.errs) != 1 || !strings.Contains(run.errs[0], "reading the last lines of subject-1's log after the docker stop") {
		t.Errorf("errs = %q, want the failed read", run.errs)
	}
	if run.running[1] || len(run.ended) != 1 || run.ended[0].Text != "harness: docker stop" {
		t.Errorf("the replica must count as ended: running %v, ended %v", run.running[1], run.ended)
	}
	// subject-1's book is empty and it no longer runs, but it ran: the final
	// checks must read it, and report that they could not. subject-2 never ran.
	if errs := run.refreshAll(); len(errs) != 1 {
		t.Errorf("refreshAll reported %d failed reads, want subject-1's", len(errs))
	}
	if reads["bench-subject-vmetric-2"] != 0 {
		t.Errorf("refreshAll read subject-2, which never ran")
	}

	fail = false
	run.errs = nil
	run.markStarted(2)
	if !run.endReplica(2, time.Now(), "docker kill") || len(run.errs) != 0 || len(run.book.all("bench-subject-vmetric-2")) != 1 {
		t.Errorf("a read that works: errs %q, lines %d", run.errs, len(run.book.all("bench-subject-vmetric-2")))
	}
}
