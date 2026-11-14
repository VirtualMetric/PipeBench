package runner

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// A copy that is offline or more than a few operations behind is not current
// (VM-8792).
func TestCurrentCopies(t *testing.T) {
	for _, c := range []struct {
		s    streamCopies
		want int
	}{
		{streamCopies{Actual: 3}, 3},
		{streamCopies{Actual: 3, Offline: 1, MaxLag: 2}, 3},
		{streamCopies{Actual: 3, Offline: 1, MaxLag: 5000}, 2},
		{streamCopies{Actual: 1, Offline: 2, MaxLag: 5000}, 0},
	} {
		if got := currentCopies(c.s); got != c.want {
			t.Errorf("currentCopies(%+v) = %d, want %d", c.s, got, c.want)
		}
	}
}

// The sampler keeps the fewest current copies of each stream, and when the
// stream first had them. A read that saw no stream does not count.
func TestCopiesSamplerKeepsTheFewest(t *testing.T) {
	c := &copiesSampler{fewest: map[string]copiesLow{}}
	t0 := time.Date(2026, 10, 31, 18, 0, 0, 0, time.UTC)
	c.record(map[string]streamCopies{"A": {Actual: 2}, "B": {Actual: 2}}, t0)
	c.record(map[string]streamCopies{"A": {Actual: 3, Offline: 1, MaxLag: 900}, "B": {Actual: 3}}, t0.Add(2*time.Second))
	c.record(map[string]streamCopies{"A": {Actual: 1}}, t0.Add(4*time.Second))
	c.record(nil, t0.Add(6*time.Second))
	want := map[string]copiesLow{"A": {Current: 1, At: t0.Add(4 * time.Second)}, "B": {Current: 2, At: t0}}
	if !reflect.DeepEqual(c.fewest, want) || c.samples != 3 {
		t.Fatalf("fewest = %+v after %d reads, want %+v after 3", c.fewest, c.samples, want)
	}
}

// The sampler learns a replica's member name from its log, and reads only the
// streams that member leads from its /health.
func TestLedCopies(t *testing.T) {
	log := "x\n2026-10-31 Director manager on 7001 coordinates through its shared bus as member 7001-abc\n" +
		"y\n2026-10-31 Director manager on 7001 coordinates through its shared bus as member 7001-def\n"
	if got := memberInLog(log); got != "7001-def" {
		t.Fatalf("memberInLog = %q, want the last name, 7001-def", got)
	}
	if got := memberInLog("no such line"); got != "" {
		t.Fatalf("memberInLog of a log without the line = %q", got)
	}
	raw := []byte(`{"node":"7001","jetstream":{"streams":2,"stream_detail":[` +
		`{"name":"A","leader":"7001-def","desired_replicas":3,"actual_replicas":3},` +
		`{"name":"B","leader":"7001-abc","desired_replicas":3,"actual_replicas":2}]}}`)
	got := ledCopies(raw, "7001-def")
	if len(got) != 1 || got["A"].Actual != 3 || got["A"].Leader != "7001-def" {
		t.Fatalf("ledCopies = %+v, want stream A alone", got)
	}
	short := shortOfCopies(map[string]streamCopies{"A": {Actual: 3}, "B": {Actual: 2}, "C": {Actual: 3, Offline: 1, MaxLag: 400}}, 3)
	if strings.Join(short, "|") != "B has 2 current copies of 2|C has 2 current copies of 3" {
		t.Fatalf("shortOfCopies = %q", short)
	}
}
