package runner

import (
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestPickReceiver: kill_receiver kills a replica that a generator sends to,
// that runs, and that does not lead. A replica that both leads and takes the
// records in is not chosen: its kill would test the takeover of the lead.
func TestPickReceiver(t *testing.T) {
	running := map[int]bool{1: true, 2: true, 3: true}
	for name, c := range map[string]struct {
		targets map[int]bool
		leaders []int
		running map[int]bool
		want    int
		ok      bool
	}{
		"the only receiver":               {map[int]bool{2: true}, []int{1}, running, 2, true},
		"the receiver that does not lead": {map[int]bool{1: true, 3: true}, []int{1}, running, 3, true},
		"the lowest of two":               {map[int]bool{3: true, 2: true}, []int{1}, running, 2, true},
		"no leader known":                 {map[int]bool{2: true}, nil, running, 2, true},
		"the receiver leads":              {map[int]bool{1: true}, []int{1}, running, 0, false},
		"the receiver is down":            {map[int]bool{2: true}, []int{1}, map[int]bool{1: true, 3: true}, 0, false},
		"nothing sent to a replica":       {map[int]bool{}, []int{1}, running, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := pickReceiver(c.targets, c.leaders, c.running)
			if got != c.want || ok != c.ok {
				t.Fatalf("pickReceiver = %d, %v, want %d, %v", got, ok, c.want, c.ok)
			}
		})
	}
}

// TestGeneratorTargetsReadURLs: the http_ack generator's target is a URL. Read
// as host:port, http://subject-2:9000/ named the host "http", so kill_owner
// could pick the replica a generator sends to although it prefers one that
// none sends to.
func TestGeneratorTargetsReadURLs(t *testing.T) {
	run := &ssRun{tc: &config.TestCase{Generators: []config.GeneratorConfig{
		{ID: "http", Mode: "http_ack", Target: "http://subject-2:9000/"},
		{ID: "syslog", Mode: "tcp", Target: "subject-3:9000"},
		{ID: "blob", Mode: "azure_blob", Target: "http://azurite:10000"},
	}}}
	got := run.generatorTargets()
	if len(got) != 2 || !got[2] || !got[3] {
		t.Fatalf("generatorTargets = %v, want replicas 2 and 3", got)
	}
}

// TestJudgeAckedStream: a stream whose sender waits for a 2xx for each post
// (http_ack) is judged on the records the sender got confirmed. A record the
// sender never got confirmed may be missing: the sender knows that it was not
// taken. Before kill_receiver, a shared-state stream counted every record
// the generator sent, confirmed or not.
func TestJudgeAckedStream(t *testing.T) {
	st := config.SharedStateStream{Name: "http", MaxDuplicatePct: 100}
	whole := seqCheckResult{Acked: 1000, ReceivedAcked: 1000, Unique: 1000}
	if got := judgeAckedStream(st, 1020, 1000, whole, ssStreamCounts{Received: 1100, Unique: 1000, Duplicates: 100}); len(got) != 0 {
		t.Errorf("every confirmed record arrived, 20 unconfirmed ones did not: %v, want no failure", got)
	}

	lost := seqCheckResult{Acked: 1000, ReceivedAcked: 997, Missing: 3, Unique: 997}
	got := judgeAckedStream(st, 1000, 1000, lost, ssStreamCounts{Received: 997, Unique: 997})
	if len(got) != 1 || !strings.Contains(got[0], "3 of the 1000 records the sender got a 2xx for never arrived, above max_loss 0") {
		t.Errorf("3 confirmed records lost: %v, want one failure naming them", got)
	}
	st.MaxLoss = 3
	if got := judgeAckedStream(st, 1000, 1000, lost, ssStreamCounts{Received: 997, Unique: 997}); len(got) != 0 {
		t.Errorf("3 confirmed records lost, within max_loss 3: %v", got)
	}
	st.MaxLoss = 0

	// The receiver checked fewer confirmed records than the sender had. A
	// lost record among the others would go unchecked.
	short := seqCheckResult{Acked: 900, ReceivedAcked: 900, Unique: 1000}
	if got := judgeAckedStream(st, 1000, 1000, short, ssStreamCounts{Received: 1000, Unique: 1000}); len(got) != 1 || !strings.Contains(got[0], "checked 900 confirmed records, and the sender got 1000 confirmed") {
		t.Errorf("the receiver checked 900 of 1000: %v, want that failure", got)
	}

	if got := judgeAckedStream(st, 500, 0, seqCheckResult{}, ssStreamCounts{}); len(got) != 1 || !strings.Contains(got[0], "got no record confirmed") {
		t.Errorf("no record confirmed: %v, want that failure", got)
	}

	if got := judgeAckedStream(st, 1000, 1000, whole, ssStreamCounts{Received: 1001, Unique: 1000, Unparsed: 1}); len(got) != 1 || !strings.Contains(got[0], "without a record's sequence number") {
		t.Errorf("a line without a number: %v, want that failure", got)
	}

	st.MaxDuplicatePct = 5
	if got := judgeAckedStream(st, 1000, 1000, whole, ssStreamCounts{Received: 1100, Unique: 1000, Duplicates: 100}); len(got) != 1 || !strings.Contains(got[0], "above max_duplicate_pct 5.00") {
		t.Errorf("10%% repeated with a 5%% bound: %v, want that failure", got)
	}

	nothing := config.SharedStateStream{Name: "http", ExpectNothing: true}
	if got := judgeAckedStream(nothing, 0, 0, seqCheckResult{}, ssStreamCounts{}); len(got) != 0 {
		t.Errorf("a stream that expects nothing, and got nothing: %v", got)
	}
}
