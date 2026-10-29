package runner

import "testing"

// A node's /health answer gives each stream's copies as the node sees them,
// and how many copies a stream should have in its cluster: one on every
// member, five at most.
func TestParseNodeHealthReadsTheStreamsCopies(t *testing.T) {
	raw := []byte(`{"node":"2","startup_phase":"running","cluster":{"size":3,"meta_leader":"1"},
		"jetstream":{"streams":2,"stream_detail":[
			{"name":"a","leader":"2","desired_replicas":3,"actual_replicas":3},
			{"name":"b","leader":"1","desired_replicas":3,"actual_replicas":2,"offline_peers":["3"],"max_lag":42}
		]}}`)
	h := parseNodeHealth(2, raw)
	if !h.Reachable || !h.Clustered || h.Name != "2" {
		t.Fatalf("reachable %v, clustered %v, name %q; want true, true, 2", h.Reachable, h.Clustered, h.Name)
	}
	if h.ExpectedCopies != 3 {
		t.Fatalf("expected copies %d, want 3", h.ExpectedCopies)
	}
	if got := h.Copies["b"]; got != (streamCopies{Leader: "1", Desired: 3, Actual: 2, Offline: 1, MaxLag: 42}) {
		t.Fatalf("stream b: %+v", got)
	}

	// A cluster of six members keeps five copies of each stream
	six := parseNodeHealth(1, []byte(`{"node":"1","cluster":{"size":6},"jetstream":{"stream_detail":[]}}`))
	if six.ExpectedCopies != 5 {
		t.Fatalf("expected copies in a cluster of six %d, want 5", six.ExpectedCopies)
	}
}

// copiesComplete judges each stream by its leader's report alone. A node that
// does not lead a stream reports the other copies as behind, so its report
// does not count. A stream whose leader did not answer, one cut down to one
// copy, and one with a copy behind are not complete.
func TestCopiesComplete(t *testing.T) {
	full := streamCopies{Desired: 3, Actual: 3}
	node := func(name string, copies map[string]streamCopies) nodeHealth {
		return nodeHealth{Name: name, Reachable: true, Clustered: true, ExpectedCopies: 3, Copies: copies}
	}
	led := func(leader string, s streamCopies) streamCopies { s.Leader = leader; return s }
	cases := map[string]struct {
		round healthRound
		want  bool
	}{
		"every leader reports every copy": {healthRound{Nodes: []nodeHealth{
			node("1", map[string]streamCopies{"a": led("1", full), "b": led("2", streamCopies{Desired: 3, Actual: 3, Offline: 2})}),
			node("2", map[string]streamCopies{"a": led("1", streamCopies{Desired: 3, Actual: 3, Offline: 1}), "b": led("2", full)}),
		}}, true},
		"a leader reports a copy far behind": {healthRound{Nodes: []nodeHealth{
			node("1", map[string]streamCopies{"a": led("1", streamCopies{Desired: 3, Actual: 3, Offline: 1, MaxLag: 500})}),
		}}, false},
		"a leader reports a copy an operation behind, under load": {healthRound{Nodes: []nodeHealth{
			node("1", map[string]streamCopies{"a": led("1", streamCopies{Desired: 3, Actual: 3, Offline: 1, MaxLag: 1})}),
		}}, true},
		"a stream cut down to one copy": {healthRound{Nodes: []nodeHealth{
			node("1", map[string]streamCopies{"a": led("1", streamCopies{Desired: 1, Actual: 1})}),
		}}, false},
		"a stream whose leader did not answer": {healthRound{Nodes: []nodeHealth{
			node("1", map[string]streamCopies{"a": led("1", full), "b": led("3", full)}),
			{Name: "3"},
		}}, false},
		"a stream without a leader": {healthRound{Nodes: []nodeHealth{
			node("1", map[string]streamCopies{"a": led("", full)}),
		}}, false},
		"no node reports a stream": {healthRound{Nodes: []nodeHealth{node("1", nil), node("2", nil)}}, false},
	}
	for name, c := range cases {
		if got := copiesComplete(c.round); got != c.want {
			t.Errorf("%s: copiesComplete = %v, want %v", name, got, c.want)
		}
	}
}
