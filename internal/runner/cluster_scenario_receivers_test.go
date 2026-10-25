package runner

import (
	"reflect"
	"strings"
	"testing"

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
	got := receiverVerdict(rs, &zero, &two)
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
	if got := receiverVerdict(rs[:1], &zero, nil); len(got) != 0 {
		t.Fatalf("receiverVerdict of a full receiver = %q", got)
	}
	if got := receiverVerdict(rs, nil, nil); len(got) != 0 {
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
