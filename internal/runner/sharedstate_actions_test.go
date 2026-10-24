package runner

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"github.com/VirtualMetric/PipeBench/internal/orchestrator"
)

// storeMembers is where the member records are in the state container, as
// Azurite's access log shows the path.
const storeMembers = "/statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2F"

// storeLease is the path of the lease, which every replica reads every 2
// seconds, so the log goes on while no record is written.
const storeLease = "/statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Factive"

// azuriteLine is a line of Azurite's access log, as `docker logs -t` prints
// it, for a call at at.
func azuriteLine(at time.Time, method, path string, status int) string {
	return fmt.Sprintf("%s 172.20.0.5 - - [%s] \"%s %s HTTP/1.1\" %d -\n",
		at.UTC().Format(time.RFC3339Nano), at.UTC().Format("02/Jan/2006:15:04:05 -0700"), method, path, status)
}

// durSecs is a number of seconds as a duration.
func durSecs(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// leaseReads returns the lease reads of one replica every 2 seconds, from
// from to to.
func leaseReads(t0 time.Time, from, to float64) []ssStoreCall {
	var calls []ssStoreCall
	for s := from; s <= to; s += 2 {
		calls = append(calls, ssStoreCall{At: t0.Add(durSecs(s)), Method: "GET", Path: storeLease, Status: 200})
	}
	return calls
}

// recordWrites returns the writes of a member record at the given seconds
// after t0.
func recordWrites(t0 time.Time, member string, at ...float64) []ssStoreCall {
	var calls []ssStoreCall
	for _, s := range at {
		calls = append(calls, ssStoreCall{At: t0.Add(durSecs(s)), Method: "PUT", Path: storeMembers + member, Status: 201})
	}
	return calls
}

// inOrder sorts calls by time, as the log has them.
func inOrder(calls ...[]ssStoreCall) []ssStoreCall {
	all := slices.Concat(calls...)
	slices.SortStableFunc(all, func(a, b ssStoreCall) int { return a.At.Compare(b.At) })
	return all
}

// An older director renews its member record every 2 seconds, so
// a record that has not been written for more than 6 seconds is gone.
func TestLiveStoreMembersRenewedEveryTwoSeconds(t *testing.T) {
	t0 := time.Date(2026, 10, 23, 16, 40, 30, 0, time.UTC)
	var log strings.Builder
	for s := 0.0; s <= 20; s += 2 {
		log.WriteString(azuriteLine(t0.Add(durSecs(s)), "PUT", storeMembers+"subject-1-b29e", 201))
		log.WriteString(azuriteLine(t0.Add(durSecs(s+0.5)), "PUT", storeMembers+"subject-2-3a9f", 201))
		if s <= 12 {
			// subject-4 was killed: its record is still there, but nobody
			// renews it.
			log.WriteString(azuriteLine(t0.Add(durSecs(s+1)), "PUT", storeMembers+"subject-4-old", 201))
		}
	}
	// A replica reads subject-3's record, which nobody writes.
	log.WriteString(azuriteLine(t0.Add(durSecs(18.6)), "GET", storeMembers+"subject-3-f4d7", 200))
	// The store refuses a write of subject-5's record.
	log.WriteString(azuriteLine(t0.Add(durSecs(19.1)), "PUT", storeMembers+"subject-5-c1d2", 412))
	// A write to another container is not a member record.
	log.WriteString(azuriteLine(t0.Add(durSecs(19.6)), "PUT", "/devstoreaccount1/bench-in/shared%2Fmembers%2Fnot-a-member", 201))
	calls := parseAzuriteCalls(slices.SortedFunc(slices.Values(parseDockerLogLines(log.String())), func(a, b ssLine) int { return a.At.Compare(b.At) }))

	if got := memberRecordWindow(memberRecords(calls, "director-state")); got != 6*time.Second {
		t.Errorf("window = %v, want 6s for records renewed every 2 seconds", got)
	}
	got := liveStoreMembers(calls, "director-state")
	want := []string{"subject-1-b29e", "subject-2-3a9f"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("live members = %v, want %v: a record not renewed for 7.5 seconds, a read, a refused write and a write to another container do not count", got, want)
	}
	if got := liveStoreMembers(nil, "director-state"); len(got) != 0 {
		t.Errorf("no calls: got %v", got)
	}
}

// A newer director writes its member record when it starts and then
// every 30 seconds, and deletes it when it stops cleanly. A record is live
// for about 45 seconds after its last write, and gone at once when deleted.
func TestLiveStoreMembersWrittenEveryThirtySeconds(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 11, 9, 22, 0, time.UTC)
	replicas := [][]ssStoreCall{
		recordWrites(t0, "subject-1-195f", 0.0, 31.8, 63.6),
		recordWrites(t0, "subject-2-af09", 0.1, 31.0, 62.2),
		recordWrites(t0, "subject-3-ef0e", 0.2, 31.9, 63.9),
	}

	t.Run("before any record is written twice", func(t *testing.T) {
		calls := inOrder(recordWrites(t0, "subject-1-195f", 0.0), recordWrites(t0, "subject-2-af09", 0.1), recordWrites(t0, "subject-3-ef0e", 0.2), leaseReads(t0, 0, 20))
		if got := memberRecordWindow(memberRecords(calls, "director-state")); got != 45*time.Second {
			t.Errorf("window = %v, want 45s: with no record written twice yet, the interval is taken as 30 seconds", got)
		}
		want := []string{"subject-1-195f", "subject-2-af09", "subject-3-ef0e"}
		if got := liveStoreMembers(calls, "director-state"); !reflect.DeepEqual(got, want) {
			t.Errorf("20 s after the start: live members = %v, want %v", got, want)
		}
	})

	t.Run("scale-out", func(t *testing.T) {
		// A fourth replica starts 80 s in. At 90 s, the others last wrote
		// their records 26 to 28 seconds ago.
		calls := inOrder(slices.Concat(replicas...), recordWrites(t0, "subject-4-77aa", 80), leaseReads(t0, 0, 90))
		window := memberRecordWindow(memberRecords(calls, "director-state"))
		if window < 45*time.Second || window > 50*time.Second {
			t.Errorf("window = %v, want about 1.5 times the 31 s between two writes", window)
		}
		want := []string{"subject-1-195f", "subject-2-af09", "subject-3-ef0e", "subject-4-77aa"}
		if got := liveStoreMembers(calls, "director-state"); !reflect.DeepEqual(got, want) {
			t.Errorf("after the scale-out: live members = %v, want %v", got, want)
		}
	})

	t.Run("scale-in", func(t *testing.T) {
		// The fourth replica stops cleanly at 95 s and deletes its record.
		deleted := []ssStoreCall{{At: t0.Add(durSecs(95)), Method: "DELETE", Path: storeMembers + "subject-4-77aa", Status: 202}}
		calls := inOrder(slices.Concat(replicas...), recordWrites(t0, "subject-4-77aa", 80), deleted, leaseReads(t0, 0, 96))
		want := []string{"subject-1-195f", "subject-2-af09", "subject-3-ef0e"}
		if got := liveStoreMembers(calls, "director-state"); !reflect.DeepEqual(got, want) {
			t.Errorf("after the scale-in: live members = %v, want %v: a deleted record is gone at once", got, want)
		}
	})

	t.Run("a replica that was killed", func(t *testing.T) {
		// subject-4 wrote its record at 2 s and 33 s, and was killed. At
		// 96 s its record is 63 seconds old, more than 1.5 intervals.
		calls := inOrder(slices.Concat(replicas...), recordWrites(t0, "subject-4-dead", 2, 33), leaseReads(t0, 0, 96))
		want := []string{"subject-1-195f", "subject-2-af09", "subject-3-ef0e"}
		if got := liveStoreMembers(calls, "director-state"); !reflect.DeepEqual(got, want) {
			t.Errorf("after a kill: live members = %v, want %v", got, want)
		}
	})
}

func TestJudgeStream(t *testing.T) {
	nothing := config.SharedStateStream{Name: "blob", ExpectNothing: true}
	if got := judgeStream(nothing, 0, ssStreamCounts{}); len(got) != 0 {
		t.Errorf("a stream that expects nothing, whose sender sent nothing: %v, want no failure", got)
	}
	if got := judgeStream(nothing, 300, ssStreamCounts{Received: 5}); len(got) != 1 || !strings.Contains(got[0], "5 records arrived") {
		t.Errorf("a stream that expects nothing, with arrivals: %v, want one failure naming them", got)
	}
	normal := config.SharedStateStream{Name: "syslog", MaxLoss: 2, MaxDuplicatePct: 2}
	if got := judgeStream(normal, 0, ssStreamCounts{}); len(got) != 1 || !strings.Contains(got[0], "sent nothing") {
		t.Errorf("a normal stream whose sender sent nothing: %v, want that failure", got)
	}
	if got := judgeStream(normal, 100, ssStreamCounts{Received: 99, Unique: 98, Duplicates: 1}); len(got) != 0 {
		t.Errorf("2 lost and about 1%% repeated, within bounds: %v", got)
	}
	if got := judgeStream(normal, 100, ssStreamCounts{Received: 100, Unique: 97, Duplicates: 3}); len(got) != 2 {
		t.Errorf("3 lost and about 3%% repeated: %v, want two failures", got)
	}
	limit := int64(3600)
	counted := config.SharedStateStream{Name: "blob", MaxDuplicates: &limit}
	if got := judgeStream(counted, 9000, ssStreamCounts{Received: 12600, Unique: 9000, Duplicates: 3600}); len(got) != 0 {
		t.Errorf("max_duplicates replaces the share: 3600 repeated of 9000 is within a bound of 3600: %v", got)
	}
	if got := judgeStream(counted, 9000, ssStreamCounts{Received: 12601, Unique: 9000, Duplicates: 3601}); len(got) != 1 || !strings.Contains(got[0], "max_duplicates 3600") {
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
	stubContainerExit(t, 0)
	run := &ssRun{
		book:     book,
		running:  map[int]bool{1: true},
		ran:      map[int]bool{1: true},
		measures: map[string]any{},
		replicas: []ssReplica{
			{N: 1, Service: "subject-1", Container: "bench-subject-vmetric-1"},
			{N: 2, Service: "subject-2", Container: "bench-subject-vmetric-2"},
		},
	}
	if run.endReplica(1, time.Now(), "docker stop", true) {
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
	if !run.endReplica(2, time.Now(), "docker kill", false) || len(run.errs) != 0 || len(run.book.all("bench-subject-vmetric-2")) != 1 {
		t.Errorf("a read that works: errs %q, lines %d", run.errs, len(run.book.all("bench-subject-vmetric-2")))
	}
}

// stubContainerExit makes every container end with code, for one test.
func stubContainerExit(t *testing.T, code int) {
	t.Helper()
	saved := containerExit
	containerExit = func(string) (int, bool, error) { return code, false, nil }
	t.Cleanup(func() { containerExit = saved })
}

// TestEndReplicaFailsAStopThatDockerHadToKill: docker stop sends SIGTERM and,
// 30 seconds later, SIGKILL. A replica that needed the SIGKILL did not stop
// gracefully, and the case used to count it as a clean stop all the same.
func TestEndReplicaFailsAStopThatDockerHadToKill(t *testing.T) {
	book := newSSLogBook()
	book.read = func(string, time.Time) ([]byte, error) { return nil, nil }
	newRun := func() *ssRun {
		return &ssRun{
			book: book, running: map[int]bool{1: true}, ran: map[int]bool{1: true}, measures: map[string]any{},
			replicas: []ssReplica{{N: 1, Service: "subject-1", Container: "bench-subject-vmetric-1"}},
		}
	}

	stubContainerExit(t, 137)
	run := newRun()
	run.endReplica(1, time.Now(), "rolling restart", true)
	if len(run.errs) != 1 || !strings.Contains(run.errs[0], "Docker killed it (exit code 137)") {
		t.Errorf("a stop that ended in SIGKILL: errs %q, want the forced kill", run.errs)
	}
	if codes := run.measures["stop_exit_codes"].(map[string][]int); !reflect.DeepEqual(codes["subject-1"], []int{137}) {
		t.Errorf("stop_exit_codes = %v", codes)
	}
	// A kill is meant to end the process that way.
	run = newRun()
	run.endReplica(1, time.Now(), "docker kill", false)
	if len(run.errs) != 0 {
		t.Errorf("a kill: errs %q, want none", run.errs)
	}

	stubContainerExit(t, 0)
	run = newRun()
	run.endReplica(1, time.Now(), "docker stop", true)
	if len(run.errs) != 0 {
		t.Errorf("a stop that exited by itself: errs %q, want none", run.errs)
	}
}

// TestOutageShortfall: an outage stream used to pass when a single record
// arrived in the window. It must keep at least half its generator's pace.
func TestOutageShortfall(t *testing.T) {
	window := 15 * time.Second
	// The bad run: 400 records a second sent, 1 arrived in 15 seconds. The
	// old check asked only for growth above 0.
	if growth := int64(1); growth <= 0 {
		t.Fatal("the old check would have failed this run")
	}
	if why := outageShortfall("syslog", 1, window, 400); !strings.Contains(why, "slowed during the outage: in its last 15s, 1 record arrived of the 6000") {
		t.Errorf("1 record in 15s at 400/s: %q", why)
	}
	if why := outageShortfall("syslog", 0, window, 400); !strings.Contains(why, "stopped during the outage") {
		t.Errorf("no record: %q", why)
	}
	// What the outage cases measured: the full pace, give or take a burst
	// of the generator's buffer.
	if why := outageShortfall("syslog", 5600, window, 400); why != "" {
		t.Errorf("5600 records in 15s at 400/s: %q, want no failure", why)
	}
	// A generator without a rate limit has no pace to compare with.
	if why := outageShortfall("syslog", 1, window, 0); why != "" {
		t.Errorf("no rate limit: %q, want no failure", why)
	}
}

// TestJudgeStreamOnSequenceNumbers: 100 records were sent, and record 50
// arrived without its head. Counted as distinct lines, as the receiver's hash
// check counts them, its tail is a 100th distinct line, and the run lost
// nothing. Counted by sequence numbers, 99 records arrived and one line
// carries no number.
func TestJudgeStreamOnSequenceNumbers(t *testing.T) {
	st := config.SharedStateStream{Name: "blob"}
	byLine := ssStreamCounts{Received: 100, Unique: 100}
	if got := judgeStream(st, 100, byLine); len(got) != 0 {
		t.Fatalf("counted by distinct lines, the cut record hides the lost one: %v", got)
	}
	got := judgeStream(st, 100, ssStreamCounts{Received: 100, Unique: 99, Unparsed: 1})
	if len(got) != 2 || !strings.Contains(got[0], "1 lines arrived without a record's sequence number") || !strings.Contains(got[1], "1 records were lost") {
		t.Errorf("counted by sequence numbers: %q, want the unnumbered line and the lost record", got)
	}
	// More distinct records than were sent: the loss came out below zero and
	// was set to 0 without a word.
	if old := max(int64(100)-101, 0); old != 0 {
		t.Fatal("the old loss would have been above 0")
	}
	if got := judgeStream(st, 100, ssStreamCounts{Received: 101, Unique: 101}); len(got) != 1 || !strings.Contains(got[0], "101 distinct records arrived, 1 more than the generator sent") {
		t.Errorf("101 distinct of 100 sent: %q", got)
	}
}

// TestWindowDisturbances: the store-call window of a 300-second run in which
// Azurite stood still for 7 seconds. The bus replica stopped leading, wrote its
// last segment, and led again as a new writer. The old check judged only the
// busiest writer, whose gaps were in bounds, so it passed the run.
func TestWindowDisturbances(t *testing.T) {
	base := time.Date(2026, 10, 23, 23, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return base.Add(time.Duration(s * float64(time.Second))) }
	put := func(s float64, path string) ssStoreCall {
		return ssStoreCall{At: at(s), Method: "PUT", Path: "/statestore/director-state/" + path, Status: 201}
	}
	var calls []ssStoreCall
	// Steady traffic every second, but nothing from 613 to 620.
	for s := 0.0; s < 900; s++ {
		if s >= 614 && s <= 619 {
			continue
		}
		calls = append(calls, ssStoreCall{At: at(s), Method: "GET", Path: "/statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers", Status: 200})
	}
	calls = append(calls,
		put(0, "bench%2F7001%2Fstate%2Fo%2Fsegments%2Fsubject-2-a%2F1"),
		put(300, "bench%2F7001%2Fstate%2Fo%2Fsegments%2Fsubject-2-a%2F2"),
		put(600, "bench%2F7001%2Fstate%2Fo%2Fsegments%2Fsubject-2-a%2F3"),
		put(624.6, "bench%2F7001%2Fstate%2Fo%2Fsegments%2Fsubject-2-b%2F1"),
		put(629.6, "bench%2F7001%2Fstate%2Fo%2Fsegments%2Fsubject-2-b%2F2"),
	)
	sort.Slice(calls, func(i, j int) bool { return calls[i].At.Before(calls[j].At) })
	events := []ssEvent{
		{At: at(620.2), Container: "bench-subject-vmetric-2", Kind: ssLeaderDown},
		{At: at(624.2), Container: "bench-subject-vmetric-2", Kind: ssLeaderUp},
		{At: at(624.6), Container: "bench-subject-vmetric-3", Kind: ssLeaseAcquired, Device: 12},
		{At: at(1000), Container: "bench-subject-vmetric-1", Kind: ssLeaderDown}, // after the window
	}

	// The old check: the busiest writer's gaps, judged against 300 s.
	writer, ts := busiestWriter(segmentWrites(calls, "director-state", at(0), at(900)))
	if problems, judged := judgeWriteGaps(ts, make([]bool, len(ts)), 300*time.Second, 30*time.Second); writer != "subject-2-a" || len(problems) != 0 || judged != 2 {
		t.Fatalf("old check: writer %s, problems %v, judged %d; want subject-2-a passing on 2 gaps", writer, problems, judged)
	}

	got := windowDisturbances(events, calls, "director-state", at(0), at(900))
	want := []string{
		"bench-subject-vmetric-2 stopped leading at 23:10:20.200",
		"bench-subject-vmetric-2 became the leader at 23:10:24.200",
		"bench-subject-vmetric-3 started device 12 at 23:10:24.600",
		"the state bus wrote its segments as 2 writers (subject-2-a, subject-2-b): it moved or started again",
		"Azurite served no request for 7.0s from 23:10:13.000",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("disturbances:\n got %q\nwant %q", got, want)
	}
	// A steady window has none.
	if got := windowDisturbances(events, calls, "director-state", at(0), at(600)); len(got) != 0 {
		t.Errorf("a steady window: %q, want none", got)
	}
	// A store that goes quiet at the end of the window counts too.
	if got := windowDisturbances(nil, calls[:10], "director-state", at(0), at(30)); len(got) != 1 || !strings.Contains(got[0], "no request for 22.0s from 23:00:08.000") {
		t.Errorf("a store quiet at the end: %q", got)
	}
}

// TestStoreOutageArgs: pause_store pauses the container of a store that has
// one, and makes a share refuse every access through a short root container
// that mounts the replicas' volumes.
func TestStoreOutageArgs(t *testing.T) {
	for store, want := range map[string][2]string{
		"":                               {"pause bench-azurite", "unpause bench-azurite"},
		config.SharedStateStoreS3:        {"pause bench-minio", "unpause bench-minio"},
		config.SharedStateStoreRedis:     {"pause bench-redis", "unpause bench-redis"},
		config.SharedStateStoreAzureBlob: {"pause bench-azurite", "unpause bench-azurite"},
	} {
		ss := &config.SharedStateConfig{Store: store}
		if got := strings.Join(storeOutageArgs(ss, true, "", "img"), " "); got != want[0] {
			t.Errorf("store %q, away: %q, want %q", store, got, want[0])
		}
		if got := strings.Join(storeOutageArgs(ss, false, "", "img"), " "); got != want[1] {
			t.Errorf("store %q, back: %q, want %q", store, got, want[1])
		}
	}
	share := &config.SharedStateConfig{Store: config.SharedStateStoreShare, SharePath: "/mnt/vm-state"}
	away := strings.Join(storeOutageArgs(share, true, "bench-subject-vmetric-1", "vmetric/director-enterprise:dev"), " ")
	if away != "run --rm --volumes-from bench-subject-vmetric-1 --user 0:0 --network none --cpus 0.5 --memory 64m --entrypoint chmod vmetric/director-enterprise:dev 000 /mnt/vm-state" {
		t.Errorf("share, away: %q", away)
	}
	if back := storeOutageArgs(share, false, "bench-subject-vmetric-1", "img"); back[len(back)-2] != "0777" {
		t.Errorf("share, back: %q, want mode 0777", back)
	}
}

// TestStoreCallsNotCountedOutsideAzurite: the store calls of another store
// are reported as not counted, never as zero.
func TestStoreCallsNotCountedOutsideAzurite(t *testing.T) {
	run := &ssRun{ss: &config.SharedStateConfig{Store: config.SharedStateStoreRedis}, measures: map[string]any{}}
	run.countStoreCalls()
	if got, _ := run.measures["store_calls"].(string); !strings.Contains(got, "not counted: the shared state is in Redis") {
		t.Errorf("store_calls = %v", run.measures["store_calls"])
	}
}

// TestShareOutageMustRefuseTheReplicas: chmod 000 does not stop a process
// that runs as root. Such a replica keeps its store through the outage, and
// the case used to count the outage as survived. It now fails the run.
func TestShareOutageMustRefuseTheReplicas(t *testing.T) {
	saved := shareOpens
	t.Cleanup(func() { shareOpens = saved })
	run := &ssRun{
		ss:      &config.SharedStateConfig{Store: config.SharedStateStoreShare, SharePath: "/mnt/vm-state"},
		running: map[int]bool{1: true, 2: true},
		replicas: []ssReplica{
			{N: 1, Service: "subject-1", Container: "bench-subject-vmetric-1"},
			{N: 2, Service: "subject-2", Container: "bench-subject-vmetric-2"},
			{N: 3, Service: "subject-3", Container: "bench-subject-vmetric-3"},
		},
	}
	// subject-2 runs as root: the share still opens for it.
	shareOpens = func(container, _ string) (bool, error) { return container == "bench-subject-vmetric-2", nil }
	if err := run.confirmShareRefused(); err == nil || !strings.Contains(err.Error(), "still opens for subject-2's user") {
		t.Errorf("a root replica: %v, want the outage refused", err)
	}
	var asked []string
	shareOpens = func(container, _ string) (bool, error) { asked = append(asked, container); return false, nil }
	if err := run.confirmShareRefused(); err != nil {
		t.Errorf("every replica refused: %v", err)
	}
	if !reflect.DeepEqual(asked, []string{"bench-subject-vmetric-1", "bench-subject-vmetric-2"}) {
		t.Errorf("asked %v, want the two running replicas", asked)
	}
	run.running = map[int]bool{}
	if err := run.confirmShareRefused(); err == nil {
		t.Error("with no replica running, nothing confirmed the outage")
	}
}

// TestStreamPaceCountsTheDefaultConnections: a generator that leaves
// connections unset gets the host's default count from the compose file. The
// pace used to count it as one connection, so a stream that slowed to a
// fraction of its real pace passed the outage check.
func TestStreamPaceCountsTheDefaultConnections(t *testing.T) {
	conns := orchestrator.ResolveGeneratorConnections(0)
	if conns < 2 {
		t.Skipf("the host's default is %d connection; the case needs more than one", conns)
	}
	run := &ssRun{
		tc: &config.TestCase{Generators: []config.GeneratorConfig{{ID: "syslog", Rate: 10}}},
		ss: &config.SharedStateConfig{Streams: []config.SharedStateStream{{Name: "syslog", Generator: "syslog", Receiver: "syslog"}}},
	}
	pace := run.streamPace("syslog")
	if pace != float64(10*conns) {
		t.Fatalf("pace = %v, want %d: 10 a second on each of %d connections", pace, 10*conns, conns)
	}
	// The bad run: 150 records in 15 seconds, while the generator sent 10 a
	// second on every connection. At one connection the floor was 75.
	window := 15 * time.Second
	if why := outageShortfall("syslog", 150, window, 10); why != "" {
		t.Fatalf("the old pace should have passed this run: %q", why)
	}
	if why := outageShortfall("syslog", 150, window, pace); !strings.Contains(why, "slowed during the outage") {
		t.Errorf("at the real pace: %q, want the stream to fail", why)
	}
}

// TestWaitContainerFailsAtOnceOnAnExitedEndpoint: an endpoint that crashes
// keeps its last health status, so the wait used to run to its deadline and
// fail without the endpoint's logs. It now fails as soon as the container has
// exited, with its last lines.
func TestWaitContainerFailsAtOnceOnAnExitedEndpoint(t *testing.T) {
	savedInspect, savedTail := containerInspect, containerTail
	t.Cleanup(func() { containerInspect, containerTail = savedInspect, savedTail })
	containerInspect = func(string, string) (string, error) { return "exited|starting\n", nil }
	containerTail = func(string) string { return "redis: bad password config" }
	run := &ssRun{r: &Runner{ctx: context.Background()}}
	start := time.Now()
	if run.waitContainer("bench-redis", "{{.State.Health.Status}}", "healthy", time.Now().Add(time.Minute)) {
		t.Fatal("an exited endpoint counted as healthy")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the wait took %s; it should end at once", took)
	}
	if len(run.errs) != 1 || !strings.Contains(run.errs[0], "redis has exited (starting): redis: bad password config") {
		t.Errorf("errs = %q", run.errs)
	}

	// An init that finished is ready, and one that failed fails.
	containerInspect = func(string, string) (string, error) { return "exited|exited 0", nil }
	if !run.waitContainer("bench-minio-init", "{{.State.Status}} {{.State.ExitCode}}", "exited 0", time.Now().Add(time.Minute)) {
		t.Error("an init that exited 0 did not count as done")
	}
	containerInspect = func(string, string) (string, error) { return "exited|exited 1", nil }
	if run.waitContainer("bench-minio-init", "{{.State.Status}} {{.State.ExitCode}}", "exited 0", time.Now().Add(time.Minute)) {
		t.Error("an init that exited 1 counted as done")
	}
}
