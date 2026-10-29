package runner

import "testing"

// A node's /health answer says how many of its streams lack a copy: a stream
// without a leader, with fewer copies than the cluster has members (five at
// most), or with a copy offline or behind. A stream that a node that started
// a moment ago brought down to one copy counts too, although its own count of
// copies matches what it asks for.
func TestParseNodeHealthCountsTheStreamsShortOfCopies(t *testing.T) {
	raw := []byte(`{"node":"2","startup_phase":"running","cluster":{"size":3,"meta_leader":"1"},
		"jetstream":{"streams":5,"stream_detail":[
			{"name":"all","leader":"1","desired_replicas":3,"actual_replicas":3},
			{"name":"one copy","leader":"1","desired_replicas":1,"actual_replicas":1},
			{"name":"catching up","leader":"1","desired_replicas":3,"actual_replicas":3,"offline_peers":["3"]},
			{"name":"no leader","leader":"","desired_replicas":3,"actual_replicas":3},
			{"name":"raised","leader":"1","desired_replicas":3,"actual_replicas":2}
		]}}`)
	h := parseNodeHealth(2, raw)
	if !h.Reachable || !h.Clustered {
		t.Fatalf("reachable %v, clustered %v; want both", h.Reachable, h.Clustered)
	}
	if h.Streams != 5 || h.CopiesShort != 4 {
		t.Fatalf("streams %d, short of copies %d; want 5 and 4", h.Streams, h.CopiesShort)
	}

	// A cluster of six members keeps five copies of each stream
	six := parseNodeHealth(1, []byte(`{"node":"1","cluster":{"size":6},"jetstream":{"stream_detail":[
		{"name":"s","leader":"1","desired_replicas":5,"actual_replicas":5}]}}`))
	if six.CopiesShort != 0 {
		t.Fatalf("a stream with five copies in a cluster of six counts as short of copies")
	}
}

// copiesComplete holds once every node that reports streams reports none short
// of copies, and at least one node reports streams.
func TestCopiesComplete(t *testing.T) {
	node := func(n, streams, short int) nodeHealth {
		return nodeHealth{Node: n, Reachable: true, Clustered: true, Streams: streams, CopiesShort: short}
	}
	cases := map[string]struct {
		round healthRound
		want  bool
	}{
		"every copy back":               {healthRound{Nodes: []nodeHealth{node(1, 5, 0), node(2, 5, 0), node(3, 5, 0)}}, true},
		"one node short of a copy":      {healthRound{Nodes: []nodeHealth{node(1, 5, 0), node(2, 5, 1), node(3, 5, 0)}}, false},
		"a node that does not answer":   {healthRound{Nodes: []nodeHealth{node(1, 5, 0), {Node: 2}, node(3, 5, 0)}}, true},
		"no node reports a stream":      {healthRound{Nodes: []nodeHealth{node(1, 0, 0), node(2, 0, 0)}}, false},
		"a node that reports no stream": {healthRound{Nodes: []nodeHealth{node(1, 5, 0), node(2, 0, 0)}}, true},
	}
	for name, c := range cases {
		if got := copiesComplete(c.round); got != c.want {
			t.Errorf("%s: copiesComplete = %v, want %v", name, got, c.want)
		}
	}
}
