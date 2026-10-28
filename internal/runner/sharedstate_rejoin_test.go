package runner

import (
	"reflect"
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestParseProgressPerTarget: a progress line of the generator of PipeBench
// v0.4.16 or later says what each target confirmed. An older line does not,
// which leaves AckedPerTarget nil, so the harness can tell "nothing
// confirmed yet" from "not reported".
func TestParseProgressPerTarget(t *testing.T) {
	line := `PROGRESS t_ms=2000 acked=30 unacked=0 requests=3 failures=1 failovers=1 pending=0 last_ack_ns=7 ` +
		`acked_per_target={"http://subject-1:9000/":20,"http://subject-2:9000/":10} last_error="lookup subject-3: no such host {x} acked_per_target={}"`
	p, ok := parseGeneratorProgress(line + "\n")
	if !ok || p.Acked != 30 {
		t.Fatalf("parseGeneratorProgress = %+v, %v, want acked 30", p, ok)
	}
	want := map[string]int64{"http://subject-1:9000/": 20, "http://subject-2:9000/": 10}
	if !reflect.DeepEqual(p.AckedPerTarget, want) {
		t.Errorf("AckedPerTarget = %v, want %v", p.AckedPerTarget, want)
	}

	p, _ = parseGeneratorProgress(`PROGRESS t_ms=1000 acked=0 unacked=0 requests=0 failures=0 failovers=0 pending=0 last_ack_ns=0 acked_per_target={} last_error=""`)
	if p.AckedPerTarget == nil || len(p.AckedPerTarget) != 0 {
		t.Errorf("a line before any confirmation: AckedPerTarget = %#v, want an empty map", p.AckedPerTarget)
	}

	p, _ = parseGeneratorProgress(`PROGRESS t_ms=1000 acked=9600 unacked=0 requests=960 failures=2 failovers=0 pending=310 last_ack_ns=2 last_error=""`)
	if p.AckedPerTarget != nil {
		t.Errorf("an older generator's line: AckedPerTarget = %v, want nil", p.AckedPerTarget)
	}
}

// TestConfirmedAfterRejoin: a replaced replica's count is its share of the
// sender's final counts per target, less what it had confirmed when it
// rejoined. A target that names no replica counts for none.
func TestConfirmedAfterRejoin(t *testing.T) {
	final := map[string]int64{
		"http://subject-1:9000/": 90000,
		"http://subject-2:9000/": 70000,
		"http://subject-3:9000/": 40000,
		"http://relay:9000/":     5000,
	}
	got := confirmedAfterRejoin(final, map[int]int64{2: 30000, 3: 39000, 1: 50000})
	want := map[int]int64{1: 40000, 2: 40000, 3: 1000}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("confirmedAfterRejoin = %v, want %v", got, want)
	}
	if got := confirmedThrough(final, 4); got != 0 {
		t.Errorf("a replica no target names confirmed %d, want 0", got)
	}
}

// TestJudgeAfterRejoin: min_after_rejoin fails each new replica that took
// too few records after it rejoined, by name. A sender that posts to every
// replica gets its records confirmed by the others when one never answers,
// so the stream's total after the action passes while that replica is dead.
func TestJudgeAfterRejoin(t *testing.T) {
	st := config.SharedStateStream{Name: "http", MinAfterRejoin: 2000}
	if got := judgeAfterRejoin(st, map[int]int64{1: 40000, 2: 2000, 3: 35000}); len(got) != 0 {
		t.Errorf("every replica took 2,000 records or more: %v, want no failure", got)
	}
	got := judgeAfterRejoin(st, map[int]int64{1: 40000, 2: 0, 3: 35000})
	if len(got) != 1 || !strings.Contains(got[0], "got 0 records confirmed through subject-2 after it rejoined, below min_after_rejoin 2000") {
		t.Errorf("subject-2 never answered after it rejoined: %v, want that failure", got)
	}
	got = judgeAfterRejoin(st, nil)
	if len(got) != 1 || !strings.Contains(got[0], "no new replica was judged") {
		t.Errorf("no replaced replica was counted: %v, want a failure that says nothing was judged", got)
	}
	if got := judgeAfterRejoin(config.SharedStateStream{Name: "http"}, nil); len(got) != 0 {
		t.Errorf("a stream that does not set min_after_rejoin: %v, want no failure", got)
	}
}

// TestFollowersFirst: a rolling action ends the replicas that do not lead
// first, by number, and the leader last. A replica that is not running is
// left out.
func TestFollowersFirst(t *testing.T) {
	replicas := []ssReplica{{N: 1}, {N: 2}, {N: 3}, {N: 4}}
	running := map[int]bool{1: true, 2: true, 3: true}
	if got := followersFirst(replicas, running, []int{2}); !reflect.DeepEqual(got, []int{1, 3, 2}) {
		t.Errorf("followersFirst with subject-2 leading = %v, want [1 3 2]", got)
	}
	if got := followersFirst(replicas, running, nil); !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Errorf("followersFirst with no leader = %v, want [1 2 3]", got)
	}
}
