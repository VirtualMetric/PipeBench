package runner

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

func TestScenarioReceiversOne(t *testing.T) {
	tc := &config.TestCase{Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"}}
	got := scenarioReceivers(tc, map[string]int{"default": 19005}, 19001)
	want := []scenarioReceiver{{Container: "bench-receiver", Port: 19005}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scenarioReceivers = %+v, want %+v", got, want)
	}
	// Without a port from the orchestrator, the run's receiver port is used.
	if got := scenarioReceivers(tc, nil, 19001); got[0].Port != 19001 {
		t.Fatalf("scenarioReceivers without ports = %+v, want port 19001", got)
	}
}

func TestScenarioReceiversSeveral(t *testing.T) {
	tc := &config.TestCase{Receivers: []config.ReceiverConfig{{ID: "a"}, {ID: "b"}}}
	got := scenarioReceivers(tc, map[string]int{"a": 19001, "b": 19002}, 19001)
	want := []scenarioReceiver{
		{ID: "a", Container: "bench-receiver-a", Port: 19001},
		{ID: "b", Container: "bench-receiver-b", Port: 19002},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scenarioReceivers = %+v, want %+v", got, want)
	}
	sr := &scenarioRun{receivers: got}
	if rc, ok := sr.receiverByID("b"); !ok || rc.Container != "bench-receiver-b" {
		t.Fatalf("receiverByID(b) = %+v %v", rc, ok)
	}
	if _, ok := sr.receiverByID(""); ok {
		t.Fatalf("a case with several receivers has no receiver without an id")
	}
	if got := receiverContainers(got); !reflect.DeepEqual(got, []string{"bench-receiver-a", "bench-receiver-b"}) {
		t.Fatalf("receiverContainers = %v", got)
	}
}

func TestWorstReceiver(t *testing.T) {
	cases := map[string]struct {
		rs   []receiverResult
		want int
	}{
		"the one that missed the most": {[]receiverResult{{ID: "a", Checked: true, Missing: 3}, {ID: "b", Checked: true, Missing: 1150}}, 1},
		"the first on a tie":           {[]receiverResult{{ID: "a", Checked: true, Missing: 5}, {ID: "b", Checked: true, Missing: 5}}, 0},
		"one that never answered": {[]receiverResult{
			{ID: "a", Checked: true, Missing: 40}, {ID: "b", Checked: false},
		}, 1},
		"the only one": {[]receiverResult{{Checked: true}}, 0},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := worstReceiver(c.rs); got != c.want {
				t.Fatalf("worstReceiver = %d, want %d", got, c.want)
			}
		})
	}
}

func TestReceiverVerdict(t *testing.T) {
	zero, two := int64(0), int64(2)
	rs := []receiverResult{
		{ID: "a", Checked: true, Missing: 0, Duplicates: 3},
		{ID: "b", Checked: true, Missing: 1150, Duplicates: 0},
	}
	got := receiverVerdict(rs, config.ScenarioExpect{MaxLoss: &zero, MaxDuplicates: &two})
	if len(got) != 2 {
		t.Fatalf("receiverVerdict = %q, want two reasons", got)
	}
	if !strings.Contains(got[0], "3 lines arrived at receiver a more than once (max_duplicates 2)") {
		t.Fatalf("first reason = %q", got[0])
	}
	if !strings.Contains(got[1], "1150 acknowledged lines never arrived at receiver b (max_loss 0)") {
		t.Fatalf("second reason = %q", got[1])
	}
	// A receiver that got every line passes, and without limits nothing is
	// judged.
	if got := receiverVerdict(rs[:1], config.ScenarioExpect{MaxLoss: &zero}); len(got) != 0 {
		t.Fatalf("receiverVerdict of a full receiver = %q", got)
	}
	if got := receiverVerdict(rs, config.ScenarioExpect{}); len(got) != 0 {
		t.Fatalf("receiverVerdict without limits = %q", got)
	}
}

func TestSummaryListsEachReceiver(t *testing.T) {
	rep := scenarioReport{Missing: 1150, Receivers: []receiverResult{
		{ID: "a", Checked: true, Duplicates: 4},
		{ID: "b", Checked: true, Missing: 1150},
	}}
	s := rep.summary()
	if s["missing_a"] != int64(0) || s["missing_b"] != int64(1150) || s["duplicates_a"] != int64(4) {
		t.Fatalf("summary = %v", s)
	}
	if _, ok := (scenarioReport{}).summary()["missing_"]; ok {
		t.Fatalf("a case with one receiver lists no receiver")
	}
}

func TestNoLogNodes(t *testing.T) {
	sr := &scenarioRun{containers: []string{"bench-subject-vmetric-1", "bench-subject-vmetric-2", "bench-subject-vmetric-3"}}
	if got, err := sr.noLogNodes("all"); err != nil || !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("noLogNodes(all) = %v %v", got, err)
	}
	if got, err := sr.noLogNodes("2"); err != nil || !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("noLogNodes(2) = %v %v", got, err)
	}
	if _, err := sr.noLogNodes("4"); err == nil {
		t.Fatalf("noLogNodes(4) of a three-node run must fail")
	}
}

// A receiver that expect.receivers names is judged by its own limits: the
// case's other receivers must still get every line, and the named one must
// miss at least its min_loss.
func TestReceiverVerdictPerReceiver(t *testing.T) {
	zero, planned, one := int64(0), int64(5000), int64(1)
	ex := config.ScenarioExpect{
		MaxLoss:   &zero,
		Receivers: map[string]config.ScenarioReceiverExpect{"b": {MaxLoss: &planned, MinLoss: &one}},
	}
	rs := []receiverResult{
		{ID: "a", Checked: true},
		{ID: "b", Checked: true, Missing: 1150},
	}
	if got := receiverVerdict(rs, ex); len(got) != 0 {
		t.Fatalf("receiverVerdict = %q, want none: b may miss up to 5000", got)
	}

	// The loss the case tests did not happen
	rs[1].Missing = 0
	got := receiverVerdict(rs, ex)
	if len(got) != 1 || !strings.Contains(got[0], "only 0 acknowledged lines never arrived at receiver b, fewer than min_loss 1") {
		t.Fatalf("receiverVerdict without the loss = %q", got)
	}

	// More than the receiver's own limit, and a line missing at a receiver
	// that keeps the case's
	rs[0].Missing, rs[1].Missing = 1, 5001
	got = receiverVerdict(rs, ex)
	if len(got) != 2 || !strings.Contains(got[0], "1 acknowledged lines never arrived at receiver a (max_loss 0)") ||
		!strings.Contains(got[1], "5001 acknowledged lines never arrived at receiver b (max_loss 5000)") {
		t.Fatalf("receiverVerdict = %q", got)
	}
}

func TestJudgeArrived(t *testing.T) {
	lo, hi := int64(100), int64(2000)
	c := arrivedCheck{Name: "early_kept", Receiver: "b", SentAfter: "b_down", SentBefore: "b_early", Max: &hi}
	if got := judgeArrived(c, 2000, 0); got != "" {
		t.Fatalf("judgeArrived at max = %q", got)
	}
	if got := judgeArrived(c, 2001, 0); !strings.Contains(got, `early_kept: 2001 lines sent between marks "b_down" and "b_early" arrived at receiver b, more than max 2000`) {
		t.Fatalf("judgeArrived over max = %q", got)
	}
	c.Min, c.Max, c.Receiver = &lo, nil, ""
	if got := judgeArrived(c, 99, 0); !strings.Contains(got, `arrived, fewer than min 100`) {
		t.Fatalf("judgeArrived under min = %q", got)
	}
	if got := judgeArrived(c, 100, 0); got != "" {
		t.Fatalf("judgeArrived at min = %q", got)
	}
}

// A share is of what the compare_to receiver got of the same window.
func TestJudgeArrivedShare(t *testing.T) {
	most, few := 0.9, 0.02
	late := arrivedCheck{Name: "b_newest", Receiver: "b", SentAfter: "late_from", SentBefore: "b_up", CompareTo: "a", MinShare: &most}
	if got := judgeArrived(late, 9000, 10000); got != "" {
		t.Fatalf("judgeArrived at min_share = %q", got)
	}
	if got := judgeArrived(late, 8999, 10000); !strings.Contains(got, "8999 lines sent between marks \"late_from\" and \"b_up\" arrived at receiver b, 0.900 of the 10000 that reached receiver a, less than min_share 0.9") {
		t.Fatalf("judgeArrived under min_share = %q", got)
	}
	early := arrivedCheck{Name: "b_oldest", Receiver: "b", SentAfter: "early_from", SentBefore: "early_to", CompareTo: "a", MaxShare: &few}
	if got := judgeArrived(early, 1000, 50000); got != "" {
		t.Fatalf("judgeArrived at max_share = %q", got)
	}
	if got := judgeArrived(early, 1001, 50000); !strings.Contains(got, "more than max_share 0.02") {
		t.Fatalf("judgeArrived over max_share = %q", got)
	}
	// With nothing at the receiver the share is of, the window tested
	// nothing, and the step fails
	if got := judgeArrived(early, 0, 0); !strings.Contains(got, "no line sent between marks \"early_from\" and \"early_to\" reached receiver a") {
		t.Fatalf("judgeArrived without lines to compare with = %q", got)
	}
}

func TestLatencyPath(t *testing.T) {
	from := time.Unix(0, 1_000)
	to := time.Unix(0, 2_000)
	if got := latencyPath(from, to); got != "/latency?from_ns=1000&to_ns=2000" {
		t.Fatalf("latencyPath = %q", got)
	}
	if got := latencyPath(time.Time{}, to); got != "/latency?to_ns=2000" {
		t.Fatalf("latencyPath with an open start = %q", got)
	}
}

func TestSummaryListsEachArrivedCount(t *testing.T) {
	n := int64(42)
	rep := scenarioReport{Arrived: []arrivedCheck{{Name: "late_kept", Arrived: &n}, {Name: "unread"}}}
	s := rep.summary()
	if s["arrived_late_kept"] != int64(42) {
		t.Fatalf("summary = %v", s)
	}
	if _, ok := s["arrived_unread"]; ok {
		t.Fatalf("a window that could not be counted is listed: %v", s)
	}
}

// A receiver's counted loss must add up to what is missing there. The numbers come
// from every node's log.
func TestCountedLossVerdict(t *testing.T) {
	orig := nodeLogText
	t.Cleanup(func() { nodeLogText = orig })
	logs := map[string]string{
		"node-1": "x\nso the director dropped the oldest data: 680 batches of records (6744 records, 132.52 MB) that waited for target \"http-b\".\n",
		"node-2": "so the director dropped the oldest data: 67 batches of records (648 records, 12.73 MB) that waited for target \"http-b\".\n" +
			"so the director dropped the oldest data: 5 batches of records (50 records, 1 MB) that waited for target \"http-a\".\n",
	}
	nodeLogText = func(c string) (string, error) { return logs[c], nil }
	sr := &scenarioRun{containers: []string{"node-1", "node-2"}}
	pattern := `dropped the oldest data: \d+ batches of records \((\d+) records[^"]*target "http-b"`
	ex := config.ScenarioExpect{Receivers: map[string]config.ScenarioReceiverExpect{"b": {CountedLoss: &config.CountedLoss{Pattern: pattern, MaxGapPct: 10}}}}

	// 7,392 counted for b, against 7,500 missing: 1.4% apart
	if got := sr.countedLossVerdict([]receiverResult{{ID: "b", Checked: true, Missing: 7500}}, ex); len(got) != 0 {
		t.Fatalf("a count within the margin failed: %v", got)
	}
	// The bench run of 2 November 2026: 201,450 missing
	got := sr.countedLossVerdict([]receiverResult{{ID: "b", Checked: true, Missing: 201450}}, ex)
	if len(got) != 1 || !strings.Contains(got[0], "counted 7392 lost lines for receiver b, and 201450 are missing") {
		t.Fatalf("an undercount passed or was described wrongly: %v", got)
	}
	if got := sr.countedLossVerdict([]receiverResult{{ID: "b", Missing: 7500}}, ex); len(got) != 1 || !strings.Contains(got[0], "never answered") {
		t.Fatalf("an unknown loss was judged: %v", got)
	}
	if got := sr.countedLossVerdict([]receiverResult{{ID: "a", Checked: true, Missing: 9}}, ex); len(got) != 0 {
		t.Fatalf("a receiver without counted_loss was judged: %v", got)
	}
}

// With max_under_pct and max_over, a count may be a little above what is missing,
// by records the subject counted while it still delivered them, but not below it,
// which would hide a loss. The log is a bench run's: the last warning names three
// queues, and the pattern finds each item of the list.
func TestCountedLossVerdictJudgesEachSide(t *testing.T) {
	orig := nodeLogText
	t.Cleanup(func() { nodeLogText = orig })
	const warn = "so the director dropped the oldest data: "
	logs := map[string]string{
		"node-2": warn + "64 batches of records (6190 records, 12.18 MB) that waited for target \"http-b\". The records in them are lost.\n" +
			warn + "1858 batches of records (187040 records, 367.42 MB) that waited for target \"http-b\". The records in them are lost.\n" +
			warn + "54 batches of records (5660 records, 11.12 MB) that waited for target \"http-b\"; 3 batches of records (310 records, 624.54 KB) " +
			"that waited for target \"http-a\"; 2 batches of records (300 records, 603.73 KB) in the router queue. The records in them are lost.\n",
	}
	nodeLogText = func(c string) (string, error) { return logs[c], nil }
	sr := &scenarioRun{containers: []string{"node-2"}}
	item := func(target string) string {
		return `(?:dropped the oldest data: |; )(?:\d+ batches|1 batch) of records \((\d+) records, [^)]*\) (?:that waited for target "` + target + `"|in the (?:ingester|router) queue)`
	}
	sides := func(maxUnderPct float64, maxOver int64) config.ScenarioExpect {
		return config.ScenarioExpect{Receivers: map[string]config.ScenarioReceiverExpect{
			"a": {CountedLoss: &config.CountedLoss{Pattern: item("http-a"), MaxUnderPct: &maxUnderPct, MaxOver: &maxOver}},
			"b": {CountedLoss: &config.CountedLoss{Pattern: item("http-b"), MaxUnderPct: &maxUnderPct, MaxOver: &maxOver}},
		}}
	}
	for _, tc := range []struct {
		name    string
		id      string
		missing int64
		ex      config.ScenarioExpect
		want    string // "" when the count passes
	}{
		// 199,190 counted for b, and 610 for a: each item, the router queue's for both
		{"b counted 2,090 above what is missing", "b", 197100, sides(1, 2500), ""},
		{"b counted more above than max_over", "b", 197100, sides(1, 2000), "counted 199190 lost lines for receiver b, and 197100 are missing there: 2090 too many"},
		{"b counted 1.1% below", "b", 201450, sides(2, 2500), ""},
		{"b counted further below than max_under_pct", "b", 201450, sides(1, 2500), "1.1% too few"},
		{"b counted a tenth", "b", 1991900, sides(1, 2500), "90.0% too few"},
		{"a counted batches that still arrived", "a", 100, sides(1, 2500), ""},
		{"a lost nothing, and counted what still arrived", "a", 0, sides(1, 2500), ""},
		{"a counted more than max_over", "a", 0, sides(1, 500), "610 too many"},
		{"a lost more than it counted", "a", 1000, sides(1, 2500), "39.0% too few"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sr.countedLossVerdict([]receiverResult{{ID: tc.id, Checked: true, Missing: tc.missing}}, tc.ex)
			switch {
			case tc.want == "" && len(got) != 0:
				t.Fatalf("the count failed: %v", got)
			case tc.want != "" && (len(got) != 1 || !strings.Contains(got[0], tc.want)):
				t.Fatalf("got %v, want one reason containing %q", got, tc.want)
			}
		})
	}
}
