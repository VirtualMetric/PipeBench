package runner

import (
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestOutageConfirmProblem: a stream fails max_confirmed_during_outage when
// its sender got more records confirmed late in the outage than the limit,
// and never when the limit is unset.
func TestOutageConfirmProblem(t *testing.T) {
	limit := int64(10)
	st := config.SharedStateStream{Name: "http", MaxConfirmedDuringOutage: &limit}
	if why := outageConfirmProblem(st, 10, 15*time.Second); why != "" {
		t.Errorf("10 records at a limit of 10: %q, want no problem", why)
	}
	why := outageConfirmProblem(st, 11, 15*time.Second)
	if !strings.Contains(why, "11 records confirmed in 15s late in the outage") || !strings.Contains(why, "max_confirmed_during_outage 10") {
		t.Errorf("11 records at a limit of 10: %q, want the count, the window and the limit", why)
	}
	if why := outageConfirmProblem(config.SharedStateStream{Name: "http"}, 5000, 15*time.Second); why != "" {
		t.Errorf("no limit: %q, want no problem", why)
	}
}

// TestJudgeEachAfterOutage: every replica the sender posts to must confirm at
// least min_each_after_outage records after the store came back, and a stream
// with no replica counted fails rather than pass unjudged.
func TestJudgeEachAfterOutage(t *testing.T) {
	st := config.SharedStateStream{Name: "http", MinEachAfterOutage: 1000}
	if got := judgeEachAfterOutage(st, map[int]int64{1: 5000, 2: 1000, 3: 2000}); len(got) != 0 {
		t.Errorf("every replica at 1,000 or more: %v, want no problem", got)
	}
	got := judgeEachAfterOutage(st, map[int]int64{1: 5000, 2: 0, 3: 2000})
	if len(got) != 1 || !strings.Contains(got[0], "subject-2") {
		t.Errorf("subject-2 confirmed nothing: %v, want one problem that names it", got)
	}
	if got := judgeEachAfterOutage(st, nil); len(got) != 1 || !strings.Contains(got[0], "no replica") {
		t.Errorf("no replica counted: %v, want one problem", got)
	}
	if got := judgeEachAfterOutage(config.SharedStateStream{Name: "http"}, nil); len(got) != 0 {
		t.Errorf("not set: %v, want no problem", got)
	}
}
