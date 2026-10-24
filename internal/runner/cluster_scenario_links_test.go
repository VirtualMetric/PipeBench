package runner

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

func TestCompareRows(t *testing.T) {
	rows := []string{"evt-1", "evt-2", "evt-3", "evt-3", ""}
	received := map[string]int64{"evt-1": 1, "evt-2": 2, "evt-9": 1}
	got := compareRows(rows, received, 4)
	want := rowsCheck{Written: 3, Arrived: 2, Missing: 1, MissingKeys: []string{"evt-3"}, ArrivedTwice: 1, Unknown: 1, LinesWithoutKey: 4}
	if got.Written != want.Written || got.Arrived != want.Arrived || got.Missing != want.Missing ||
		got.ArrivedTwice != want.ArrivedTwice || got.Unknown != want.Unknown || got.LinesWithoutKey != want.LinesWithoutKey ||
		len(got.MissingKeys) != 1 || got.MissingKeys[0] != "evt-3" ||
		len(got.TwiceKeys) != 1 || got.TwiceKeys[0] != "evt-2" {
		t.Fatalf("compareRows = %+v, want %+v and evt-2 named as twice", got, want)
	}
}

func TestCompareRowsListsAtMostTwentyMissing(t *testing.T) {
	var rows []string
	for i := range 30 {
		rows = append(rows, "k"+string(rune('a'+i)))
	}
	got := compareRows(rows, map[string]int64{}, 0)
	if got.Missing != 30 || len(got.MissingKeys) != 20 {
		t.Fatalf("missing = %d with %d keys listed, want 30 with 20", got.Missing, len(got.MissingKeys))
	}
}

// A row that never arrived must fail the run. The first version reported the
// rows and judged them nowhere, so a proof run with a missing row passed.
func TestJudgeRows(t *testing.T) {
	zero := int64(0)
	cases := []struct {
		name       string
		rows       *rowsCheck
		max        *int64
		fails      bool
		hasNote    bool
		reasonPart string
	}{
		{"a missing row fails", &rowsCheck{Written: 521, Arrived: 520, Missing: 1, MissingKeys: []string{"evt-999999999"}}, &zero, true, false, "evt-999999999"},
		{"every row arrived", &rowsCheck{Written: 521, Arrived: 521}, &zero, false, false, ""},
		{"no rows to compare fails", &rowsCheck{}, &zero, true, false, "no rows"},
		{"a failed comparison fails", &rowsCheck{Error: "psql: connection refused"}, &zero, true, false, "connection refused"},
		{"a failed comparison is a note when not judged", &rowsCheck{Error: "psql: connection refused"}, nil, false, true, ""},
		{"rows not judged", &rowsCheck{Written: 5, Missing: 5}, nil, false, false, ""},
		{"judged rows that were never read fail", nil, &zero, true, false, "not compared"},
	}
	for _, c := range cases {
		reason, note := judgeRows(c.rows, c.max)
		if (reason != "") != c.fails {
			t.Errorf("%s: reason %q, want a failure: %v", c.name, reason, c.fails)
		}
		if !strings.Contains(reason, c.reasonPart) {
			t.Errorf("%s: reason %q does not mention %q", c.name, reason, c.reasonPart)
		}
		if (note != "") != c.hasNote {
			t.Errorf("%s: note %q, want a note: %v", c.name, note, c.hasNote)
		}
	}
}

// A run without load judges duplicates only from a count the receiver really
// took, and judges its rows.
func TestWithoutLoadVerdict(t *testing.T) {
	zero := int64(0)
	ex := config.ScenarioExpect{MaxDuplicates: &zero, MaxMissingRows: &zero}
	allRows := &rowsCheck{Written: 578, Arrived: 578}
	cases := []struct {
		name       string
		rep        scenarioReport
		dedup      bool
		metricsErr error
		reasonPart string // "" when the run must pass
	}{
		{"clean run", scenarioReport{Received: 578, Unique: 578, Rows: allRows}, true, nil, ""},
		{"a row delivered twice", scenarioReport{Received: 852, Unique: 578, Duplicates: 274, Rows: allRows}, true, nil, "274 lines arrived more than once"},
		{"lines counted as none distinct", scenarioReport{Received: 852, Rows: allRows}, true, nil, "counted none as distinct"},
		{"counts not read", scenarioReport{Rows: allRows}, true, errors.New("connection refused"), "could not be read"},
		{"no hashing", scenarioReport{Received: 852, Unique: 852, Rows: allRows}, false, nil, "needs correctness.validate_dedup"},
		{"a missing row", scenarioReport{Received: 520, Unique: 520, Rows: &rowsCheck{Written: 521, Arrived: 520, Missing: 1, MissingKeys: []string{"evt-999999999"}}}, true, nil, "evt-999999999"},
	}
	for _, c := range cases {
		reasons, _ := withoutLoadVerdict(c.rep, ex, c.dedup, c.metricsErr)
		got := strings.Join(reasons, "; ")
		if c.reasonPart == "" {
			if got != "" {
				t.Errorf("%s: failed with %q, want a pass", c.name, got)
			}
			continue
		}
		if !strings.Contains(got, c.reasonPart) {
			t.Errorf("%s: reasons %q, want one that mentions %q", c.name, got, c.reasonPart)
		}
	}
}

func TestParseDfFree(t *testing.T) {
	out := "Filesystem           1024-blocks    Used Available Capacity Mounted on\n" +
		"tmpfs                    1048576  1047552      1024 100% /opt/vmetric/storage\n"
	got, err := parseDfFree(out)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1024<<10 {
		t.Fatalf("parseDfFree = %d, want %d", got, 1024<<10)
	}
	if _, err := parseDfFree("Filesystem\n"); err == nil {
		t.Fatal("parseDfFree accepted output without a data line")
	}
}

func TestShapeOf(t *testing.T) {
	st := config.ScenarioStep{Do: "shape_link", Latency: "200ms", Jitter: "50ms", Loss: "20%"}
	f, err := shapeOf(st)
	if err != nil {
		t.Fatal(err)
	}
	if f.Delay != 200*time.Millisecond || f.Jitter != 50*time.Millisecond || f.LossPct != 20 || f.Cut {
		t.Fatalf("shapeOf = %+v", f)
	}
	if f, _ := shapeOf(config.ScenarioStep{Do: "cut_link", Latency: "1s"}); !f.none() {
		t.Fatalf("shapeOf a cut_link step = %+v, want none", f)
	}
	if _, err := shapeOf(config.ScenarioStep{Do: "shape_link", Loss: "20"}); err == nil {
		t.Fatal("shapeOf accepted a loss without %")
	}
}

func TestNetemArgs(t *testing.T) {
	cases := map[string]linkFault{
		" delay 200ms":          {Delay: 200 * time.Millisecond},
		" delay 200ms 50ms":     {Delay: 200 * time.Millisecond, Jitter: 50 * time.Millisecond},
		" loss 10%":             {LossPct: 10},
		" delay 30ms loss 0.5%": {Delay: 30 * time.Millisecond, LossPct: 0.5},
	}
	for want, f := range cases {
		if got := netemArgs(f); got != want {
			t.Errorf("netemArgs(%+v) = %q, want %q", f, got, want)
		}
	}
}

func TestDescribeSides(t *testing.T) {
	if got := describeSides([][]int{{1, 2}, {3, 4}}); got != "[1 2] | [3 4]" {
		t.Fatalf("describeSides = %q", got)
	}
}

func TestAgreedLeaderAmongASide(t *testing.T) {
	// A 2-and-2 split: nodes 1 and 2 name node 1, and nodes 3 and 4 name
	// nobody. No node has more than half of the four, but node 1 leads its
	// side.
	r := healthRound{Nodes: []nodeHealth{h(1, "n1", "n1"), h(2, "n2", "n1"), h(3, "n3", ""), h(4, "n4", "")}}
	if _, ok := agreedLeader(r); ok {
		t.Fatal("two nodes of four made an agreed leader of the whole cluster")
	}
	if l, ok := agreedLeaderAmong(r, []int{1, 2}); !ok || l != 1 {
		t.Fatalf("agreedLeaderAmong([1 2]) = %d, %v; want node 1", l, ok)
	}
	if l, ok := agreedLeaderAmong(r, []int{3, 4}); ok {
		t.Fatalf("the side without a leader agreed on node %d", l)
	}
	// A leader the side does not name is not its leader.
	r = healthRound{Nodes: []nodeHealth{h(1, "n1", "n1"), h(2, "n2", "n3"), h(3, "n3", "n3")}}
	if l, ok := agreedLeaderAmong(r, []int{1, 2}); ok {
		t.Fatalf("a side split between two leaders agreed on node %d", l)
	}
}
