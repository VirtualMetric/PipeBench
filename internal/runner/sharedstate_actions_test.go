package runner

import (
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

func TestCountStartsAndLines(t *testing.T) {
	lines := parseDockerLogLines(`2026-10-24T08:00:00.000Z [2026-10-24 08:00:00] [Information] [director] vmetric-director started; reason recorded as unknown.
2026-10-24T08:00:01.000Z [2026-10-24 08:00:01] [Warning] [director] This director runs in crash-resistant mode, so it ignores shared_state.write_interval. In this mode the director confirms each write only once the write is in the store.
2026-10-24T08:05:00.000Z [2026-10-24 08:05:00] [Information] [director] vmetric-director started; reason recorded as unknown.
2026-10-24T08:05:01.000Z [2026-10-24 08:05:01] [Warning] [director] This director runs in crash-resistant mode, so it ignores shared_state.write_interval. In this mode the director confirms each write only once the write is in the store.`)
	starts, seen := countStartsAndLines(lines, "so it ignores shared_state.write_interval")
	if starts != 2 || seen != 2 {
		t.Errorf("starts = %d, seen = %d; want 2 and 2: once for each start of the director", starts, seen)
	}
	if _, seen := countStartsAndLines(lines, "a line nobody logs"); seen != 0 {
		t.Errorf("a line nobody logs was seen %d times", seen)
	}
}
