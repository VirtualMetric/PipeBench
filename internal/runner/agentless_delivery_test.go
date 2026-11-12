package runner

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// Each line of a file gives one key: what the expression finds, or its first
// group. A line without a key is no row.
func TestLinesKeys(t *testing.T) {
	text := "CLUSTERFILE-1 first\nno key here\nCLUSTERFILE-22 second\n\n"
	if got, want := linesKeys(text, regexp.MustCompile("CLUSTERFILE-[0-9]+")), []string{"CLUSTERFILE-1", "CLUSTERFILE-22"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("keys %v, want %v", got, want)
	}
	if got, want := linesKeys(text, regexp.MustCompile("CLUSTERFILE-([0-9]+)")), []string{"1", "22"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("keys of the first group %v, want %v", got, want)
	}
}

// The agentless failover fails when the receiver's count does not grow past
// what it was when the owner stopped.
func TestWaitCountGrows(t *testing.T) {
	ctx := context.Background()
	soon := func() time.Time { return time.Now().Add(50 * time.Millisecond) }

	counts := []int64{10, 10, 11}
	next := func() (int64, error) {
		n := counts[0]
		if len(counts) > 1 {
			counts = counts[1:]
		}
		return n, nil
	}
	if got, ok := waitCountGrows(ctx, next, 10, time.Now().Add(time.Second), time.Millisecond); !ok || got != 11 {
		t.Fatalf("a count that grows: %d, %v, want 11, true", got, ok)
	}

	stuck := func() (int64, error) { return 10, nil }
	if got, ok := waitCountGrows(ctx, stuck, 10, soon(), time.Millisecond); ok || got != 10 {
		t.Fatalf("a count that stays: %d, %v, want 10, false", got, ok)
	}

	failing := func() (int64, error) { return 99, errors.New("the receiver does not answer") }
	if got, ok := waitCountGrows(ctx, failing, 10, soon(), time.Millisecond); ok || got != 10 {
		t.Fatalf("a count that cannot be read: %d, %v, want 10, false", got, ok)
	}
}

// A cluster that updates itself moves one node at a time. wait_version ends once
// every node has reported the version, and says which nodes never did.
func TestWaitVersion(t *testing.T) {
	round := func(versions ...string) healthRound {
		r := healthRound{At: time.Now().Add(time.Second)}
		for i, v := range versions {
			r.Nodes = append(r.Nodes, nodeHealth{Node: i + 1, Reachable: v != "", Version: v})
		}
		return r
	}
	run := func(r healthRound) (bool, string) {
		t.Helper()
		sr := &scenarioRun{ctx: context.Background(), containers: []string{"a", "b", "c"}, measures: map[string]float64{}, probe: &clusterProbe{rounds: []healthRound{r}}}
		ok, detail, err := sr.waitVersion(config.ScenarioStep{Do: "wait_version", Version: "2.7.0", Within: "1ms", Record: "updated"})
		if err != nil {
			t.Fatal(err)
		}
		return ok, detail
	}
	if ok, detail := run(round("2.7.0", "2.7.0", "2.7.0")); !ok {
		t.Fatalf("every node on the version: %s", detail)
	}
	ok, detail := run(round("2.7.0", "2.6.0", ""))
	if ok || !strings.Contains(detail, "1 of 3 nodes") {
		t.Fatalf("one node on the version: %v, %q", ok, detail)
	}
}
