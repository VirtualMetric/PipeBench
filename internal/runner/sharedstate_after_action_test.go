package runner

import (
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestJudgeAfterAction: min_after_action fails a stream whose sender got too
// few records confirmed after the action, for example because a replaced
// receiver never answered a post again. Without it, such a run passed: the
// records confirmed before the action all arrived, and nothing judged the
// time after it.
func TestJudgeAfterAction(t *testing.T) {
	st := config.SharedStateStream{Name: "http", MinAfterAction: 50000}
	if got := judgeAfterAction(st, 250000); len(got) != 0 {
		t.Errorf("250,000 confirmed after the action, 50,000 wanted: %v, want no failure", got)
	}
	if got := judgeAfterAction(st, 50000); len(got) != 0 {
		t.Errorf("exactly the minimum: %v, want no failure", got)
	}
	got := judgeAfterAction(st, 0)
	if len(got) != 1 || !strings.Contains(got[0], "got 0 records confirmed after the action, below min_after_action 50000") {
		t.Errorf("nothing confirmed after the action: %v, want that failure", got)
	}
	if got := judgeAfterAction(config.SharedStateStream{Name: "http"}, 0); len(got) != 0 {
		t.Errorf("a stream that does not set min_after_action: %v, want no failure", got)
	}
}

// TestParseGeneratorProgress: the count before the action is the acked count
// on the sender's last progress line.
func TestParseGeneratorProgress(t *testing.T) {
	log := "starting\n" +
		"PROGRESS t_ms=1000 acked=4790 unacked=0 requests=479 failures=0 failovers=0 pending=320 last_ack_ns=1 last_error=\"\"\n" +
		"PROGRESS t_ms=2000 acked=9600 unacked=0 requests=960 failures=2 failovers=0 pending=310 last_ack_ns=2 last_error=\"\"\n" +
		"some other line\n"
	p, ok := parseGeneratorProgress(log)
	if !ok || p.Acked != 9600 || p.Failures != 2 || p.Pending != 310 {
		t.Fatalf("parseGeneratorProgress = %+v, %v, want the last line, acked 9600", p, ok)
	}
	if _, ok := parseGeneratorProgress("no progress yet\n"); ok {
		t.Error("a log without a progress line read as progress")
	}
}

// TestAckedStreamSentCountsUnconfirmed: a record that the sender posted but
// never got confirmed may still arrive. Judged against the confirmed records
// plus the unconfirmed ones, the stream passes. Judged against the confirmed
// records alone, which the generator reports as lines_sent in http_ack mode,
// it failed as more than the generator sent.
func TestAckedStreamSentCountsUnconfirmed(t *testing.T) {
	st := config.SharedStateStream{Name: "http", MaxDuplicatePct: 100}
	check := seqCheckResult{Acked: 1000, ReceivedAcked: 1000, Unique: 1010}
	arrived := ssStreamCounts{Received: 1010, Unique: 1010}
	if got := judgeAckedStream(st, 1000+10, 1000, check, arrived); len(got) != 0 {
		t.Errorf("10 unconfirmed records arrived, with sent counted as confirmed plus unconfirmed: %v, want no failure", got)
	}
	got := judgeAckedStream(st, 1000, 1000, check, arrived)
	if len(got) != 1 || !strings.Contains(got[0], "10 more than the generator sent") {
		t.Errorf("with sent counted as the confirmed records alone: %v, want the failure this change avoids", got)
	}
}
