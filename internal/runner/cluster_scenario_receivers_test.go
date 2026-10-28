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
