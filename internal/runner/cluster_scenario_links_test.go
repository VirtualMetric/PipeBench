package runner

import (
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
		len(got.MissingKeys) != 1 || got.MissingKeys[0] != "evt-3" {
		t.Fatalf("compareRows = %+v, want %+v", got, want)
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
