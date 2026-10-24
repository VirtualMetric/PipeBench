package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRuleScriptDropsArrivingTraffic(t *testing.T) {
	s, err := ruleScript([]string{"172.18.0.5"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"$ipt -I INPUT -j PIPEBENCH",
		"$ipt -A PIPEBENCH -s 172.18.0.5 -j DROP",
		"tc qdisc del dev",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("script lacks %q:\n%s", want, s)
		}
	}
	// A cut drops packets as they arrive; dropping them as they leave would
	// fail the sender's writes at once.
	if strings.Contains(s, "OUTPUT") {
		t.Errorf("script touches OUTPUT:\n%s", s)
	}
	if strings.Contains(s, "netem") {
		t.Errorf("a cut alone adds netem:\n%s", s)
	}
}

func TestRuleScriptShapesLeavingTraffic(t *testing.T) {
	s, err := ruleScript(nil, []shapedPeer{
		{Addr: "172.18.0.7", Fault: linkFault{LossPct: 12.5}},
		{Addr: "172.18.0.6", Fault: linkFault{Delay: 200 * time.Millisecond, Jitter: 50 * time.Millisecond, LossPct: 20}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"root handle 1: prio bands 16 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0",
		// Peers take bands from 2 in address order; band 1 carries the rest.
		"parent 1:2 handle 20: netem delay 200ms 50ms loss 20%",
		"match ip dst 172.18.0.6/32 flowid 1:2",
		"parent 1:3 handle 30: netem loss 12.5%",
		"match ip dst 172.18.0.7/32 flowid 1:3",
		// With no cut, the chain goes.
		"$ipt -X PIPEBENCH",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("script lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "-j DROP") {
		t.Errorf("a shape alone drops packets:\n%s", s)
	}
}

func TestRuleScriptWithNothingRemovesEveryRule(t *testing.T) {
	s, err := ruleScript(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"$ipt -D INPUT -j PIPEBENCH", "$ipt -F PIPEBENCH", "$ipt -X PIPEBENCH", "tc qdisc del dev"} {
		if !strings.Contains(s, want) {
			t.Errorf("script lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "netem") || strings.Contains(s, "-j DROP") {
		t.Errorf("an empty rule set adds a rule:\n%s", s)
	}
}

func TestRuleScriptRefusesTooManyPeers(t *testing.T) {
	var peers []shapedPeer
	for i := range maxShapedPeers + 1 {
		peers = append(peers, shapedPeer{Addr: fmt.Sprintf("10.0.0.%d", i+1), Fault: linkFault{LossPct: 10}})
	}
	if _, err := ruleScript(nil, peers); err == nil {
		t.Fatal("ruleScript accepted more peers than a prio qdisc has bands")
	}
}

// fakeLinks is a linkFaults whose containers live in a map, and which keeps
// the last script each container got.
type fakeLinks struct {
	*linkFaults
	mu      sync.Mutex
	scripts map[string]string
	runs    map[string]int
}

func newFakeLinks(running map[string]string) *fakeLinks {
	f := &fakeLinks{scripts: map[string]string{}, runs: map[string]int{}}
	f.linkFaults = &linkFaults{
		faults:  map[linkKey]linkFault{},
		running: func(c string) bool { _, ok := running[c]; return ok },
		addrOf: func(c string) (string, error) {
			if a, ok := running[c]; ok {
				return a, nil
			}
			return "", errors.New("not running")
		},
		run: func(_ context.Context, c, script string) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.scripts[c] = script
			f.runs[c]++
			return nil
		},
	}
	return f
}

func TestOneWayCutRulesOnlyTheReceivingEnd(t *testing.T) {
	f := newFakeLinks(map[string]string{"a": "10.0.0.1", "b": "10.0.0.2", "c": "10.0.0.3"})
	if err := f.set(context.Background(), map[linkKey]linkFault{{"a", "b"}: {Cut: true}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.scripts["b"], "-s 10.0.0.1 -j DROP") {
		t.Errorf("b does not drop what a sends:\n%s", f.scripts["b"])
	}
	if strings.Contains(f.scripts["a"], "-j DROP") {
		t.Errorf("a drops traffic, although only a's traffic to b is cut:\n%s", f.scripts["a"])
	}
	if f.runs["c"] != 0 {
		t.Errorf("c was rebuilt, although no fault involves it")
	}
}

func TestShapeBothWaysShapesEachSender(t *testing.T) {
	f := newFakeLinks(map[string]string{"a": "10.0.0.1", "b": "10.0.0.2"})
	shape := linkFault{Delay: 200 * time.Millisecond, LossPct: 20}
	if err := f.set(context.Background(), map[linkKey]linkFault{{"a", "b"}: shape, {"b", "a"}: shape}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.scripts["a"], "match ip dst 10.0.0.2/32") || !strings.Contains(f.scripts["b"], "match ip dst 10.0.0.1/32") {
		t.Fatalf("each end must shape its own traffic to the other:\na: %s\nb: %s", f.scripts["a"], f.scripts["b"])
	}
}

func TestRestoreAllEmptiesEveryTouchedContainer(t *testing.T) {
	f := newFakeLinks(map[string]string{"a": "10.0.0.1", "b": "10.0.0.2"})
	ctx := context.Background()
	if err := f.set(ctx, map[linkKey]linkFault{{"a", "b"}: {Cut: true}, {"b", "a"}: {LossPct: 30}}); err != nil {
		t.Fatal(err)
	}
	if got := f.describe(); len(got) != 2 {
		t.Fatalf("describe = %v, want two directions", got)
	}
	if err := f.restoreAll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, c := range []string{"a", "b"} {
		if strings.Contains(f.scripts[c], "-j DROP") || strings.Contains(f.scripts[c], "netem") {
			t.Errorf("%s keeps a rule after restoreAll:\n%s", c, f.scripts[c])
		}
	}
	if got := f.describe(); len(got) != 0 {
		t.Fatalf("describe after restoreAll = %v, want none", got)
	}
}

func TestResyncRebuildsTheContainersAFaultJoins(t *testing.T) {
	f := newFakeLinks(map[string]string{"a": "10.0.0.1", "b": "10.0.0.2", "c": "10.0.0.3"})
	ctx := context.Background()
	if err := f.set(ctx, map[linkKey]linkFault{{"a", "b"}: {Cut: true}}); err != nil {
		t.Fatal(err)
	}
	before := map[string]int{"a": f.runs["a"], "b": f.runs["b"], "c": f.runs["c"]}
	if err := f.resync(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if f.runs["a"] != before["a"]+1 || f.runs["b"] != before["b"]+1 {
		t.Errorf("resync of a must rebuild a and b: runs %v, before %v", f.runs, before)
	}
	if f.runs["c"] != before["c"] {
		t.Errorf("resync of a rebuilt c, which shares no fault with a")
	}
}

func TestStoppedContainersAreSkipped(t *testing.T) {
	// b is down: it has no namespace to rebuild, and no address for a's rules.
	f := newFakeLinks(map[string]string{"a": "10.0.0.1"})
	ctx := context.Background()
	err := f.set(ctx, map[linkKey]linkFault{{"b", "a"}: {Cut: true}, {"a", "b"}: {LossPct: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if f.runs["b"] != 0 {
		t.Errorf("a stopped container was rebuilt")
	}
	if strings.Contains(f.scripts["a"], "-j DROP") || strings.Contains(f.scripts["a"], "netem") {
		t.Errorf("a has a rule for a peer with no address:\n%s", f.scripts["a"])
	}
	// The faults stay declared, for the resync after b starts.
	if got := f.describe(); len(got) != 2 {
		t.Fatalf("describe = %v, want both directions kept", got)
	}
}

func TestLinkFaultString(t *testing.T) {
	cases := map[string]linkFault{
		"none":                               {},
		"cut":                                {Cut: true},
		"delay 200ms, jitter 50ms, loss 20%": {Delay: 200 * time.Millisecond, Jitter: 50 * time.Millisecond, LossPct: 20},
	}
	for want, f := range cases {
		if got := f.String(); got != want {
			t.Errorf("%#v.String() = %q, want %q", f, got, want)
		}
	}
}
