package runner

// The cluster prober reads every director node's loopback /health once a
// second while a cluster scenario runs. Each read says which node that node
// believes leads the cluster (the message queue's meta leader), whether it
// counts a quorum, and who it believes leads each stream. The scenario
// steps wait on these reads, and the leadership analysis at the end looks
// for rounds in which two nodes each named themselves the leader.

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// healthDoc is the part of the director's /health answer the prober reads.
type healthDoc struct {
	Status       string `json:"status"`
	Node         string `json:"node"`
	StartupPhase string `json:"startup_phase"`
	Components   map[string]struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	} `json:"components"`
	Cluster *struct {
		Size           int    `json:"size"`
		PeersConnected int    `json:"peers_connected"`
		Quorum         string `json:"quorum"`
		MetaLeader     string `json:"meta_leader"`
		Reconfiguring  bool   `json:"reconfiguring"`
	} `json:"cluster"`
	JetStream *struct {
		Streams                int `json:"streams"`
		StreamsUnderReplicated int `json:"streams_under_replicated"`
		StreamDetail           []struct {
			Name            string `json:"name"`
			Leader          string `json:"leader"`
			DesiredReplicas int    `json:"desired_replicas"`
			ActualReplicas  int    `json:"actual_replicas"`
		} `json:"stream_detail"`
	} `json:"jetstream"`
}

// nodeHealth is one node's state in one round of reads.
type nodeHealth struct {
	Node      int    `json:"node"`
	State     string `json:"state"` // the container's state: running, paused, exited, ...
	Reachable bool   `json:"reachable"`
	Name      string `json:"name,omitempty"`
	Phase     string `json:"phase,omitempty"`
	VMMQ      string `json:"vmmq,omitempty"`
	// Clustered is false for a director that is not in a cluster: it leads
	// itself.
	Clustered     bool              `json:"clustered"`
	MetaLeader    string            `json:"meta_leader,omitempty"`
	Quorum        string            `json:"quorum,omitempty"`
	Peers         int               `json:"peers,omitempty"`
	UnderRepl     int               `json:"under_replicated,omitempty"`
	Reconfiguring bool              `json:"reconfiguring,omitempty"`
	StreamLeaders map[string]string `json:"-"`
}

// selfLeader reports whether the node names itself the leader.
func (h nodeHealth) selfLeader() bool {
	if !h.Reachable {
		return false
	}
	if !h.Clustered {
		return h.Phase == "running"
	}
	return h.MetaLeader != "" && h.MetaLeader == h.Name
}

type healthRound struct {
	At    time.Time    `json:"at"`
	Nodes []nodeHealth `json:"nodes"`
}

// node returns the read of node n in the round.
func (r healthRound) node(n int) (nodeHealth, bool) {
	for _, h := range r.Nodes {
		if h.Node == n {
			return h, true
		}
	}
	return nodeHealth{}, false
}

// claimedLeaders lists the nodes that name themselves the leader.
func claimedLeaders(r healthRound) []int {
	var out []int
	for _, h := range r.Nodes {
		if h.selfLeader() {
			out = append(out, h.Node)
		}
	}
	return out
}

// agreedLeader returns the leader of the round: a node that names itself
// the leader and that more than half of the reachable nodes name too. A
// node that cannot reach the others still names itself for a while after
// it lost the cluster; it is not agreed while the others name someone else.
func agreedLeader(r healthRound) (int, bool) {
	reachable := 0
	votes := map[string]int{}
	for _, h := range r.Nodes {
		if !h.Reachable {
			continue
		}
		reachable++
		if !h.Clustered {
			if h.Phase == "running" {
				votes[h.Name]++
			}
			continue
		}
		if h.MetaLeader != "" {
			votes[h.MetaLeader]++
		}
	}
	best, bestNode := 0, 0
	for _, h := range r.Nodes {
		if !h.selfLeader() {
			continue
		}
		if v := votes[h.Name]; v*2 > reachable && v > best {
			best, bestNode = v, h.Node
		}
	}
	return bestNode, best > 0
}

// streamSelfClaims returns, for each stream, the nodes that name themselves
// its leader in the round.
func streamSelfClaims(r healthRound) map[string][]int {
	out := map[string][]int{}
	for _, h := range r.Nodes {
		if !h.Reachable {
			continue
		}
		for stream, leader := range h.StreamLeaders {
			if leader != "" && leader == h.Name {
				out[stream] = append(out[stream], h.Node)
			}
		}
	}
	return out
}

// agreedLeaderAmong is agreedLeader within a set of nodes: a node of the set
// that names itself the leader and that more than half of the set's reachable
// nodes name too. A split's side uses it, because the other side cannot vote
// there.
func agreedLeaderAmong(r healthRound, nodes []int) (int, bool) {
	in := map[int]bool{}
	for _, n := range nodes {
		in[n] = true
	}
	side := healthRound{At: r.At}
	for _, h := range r.Nodes {
		if in[h.Node] {
			side.Nodes = append(side.Nodes, h)
		}
	}
	return agreedLeader(side)
}

// splitInterval is a stretch of rounds in which two or more nodes named
// themselves the leader.
type splitInterval struct {
	From    time.Time `json:"from"`
	To      time.Time `json:"to"`
	Nodes   []int     `json:"nodes"`
	Rounds  int       `json:"rounds"`
	Stream  string    `json:"stream,omitempty"`
	LastsMs int64     `json:"lasts_ms"`
}

type leadershipStats struct {
	Rounds             int             `json:"rounds"`
	MaxSelfClaimed     int             `json:"max_self_claimed_leaders"`
	SplitBrainMs       int64           `json:"split_brain_ms"`
	SplitIntervals     []splitInterval `json:"split_intervals,omitempty"`
	MaxStreamClaims    int             `json:"max_stream_self_claims"`
	StreamSplitMs      int64           `json:"stream_split_ms"`
	StreamSplits       []splitInterval `json:"stream_splits,omitempty"`
	LeaderChanges      int             `json:"leader_changes"`
	LeaderlessRoundsMs int64           `json:"leaderless_ms"`
}

// analyzeLeadership measures, over consecutive rounds, how long two or more
// nodes named themselves the cluster's leader (and each stream's). A
// stretch lasts from its first round to the round after its last, which is
// the most the overlap can have lasted given one read per round.
func analyzeLeadership(rounds []healthRound) leadershipStats {
	var st leadershipStats
	st.Rounds = len(rounds)
	var open *splitInterval
	streamOpen := map[string]*splitInterval{}
	prevLeader := 0

	closeAt := func(iv *splitInterval, at time.Time) splitInterval {
		iv.To = at
		iv.LastsMs = at.Sub(iv.From).Milliseconds()
		return *iv
	}

	for i, r := range rounds {
		next := r.At
		if i+1 < len(rounds) {
			next = rounds[i+1].At
		}
		claims := claimedLeaders(r)
		if len(claims) > st.MaxSelfClaimed {
			st.MaxSelfClaimed = len(claims)
		}
		if len(claims) >= 2 {
			if open == nil {
				open = &splitInterval{From: r.At, Nodes: claims}
			}
			open.Rounds++
		} else if open != nil {
			iv := closeAt(open, r.At)
			st.SplitIntervals = append(st.SplitIntervals, iv)
			st.SplitBrainMs += iv.LastsMs
			open = nil
		}

		if l, ok := agreedLeader(r); ok {
			if prevLeader != 0 && l != prevLeader {
				st.LeaderChanges++
			}
			prevLeader = l
		} else if len(r.Nodes) > 0 {
			st.LeaderlessRoundsMs += next.Sub(r.At).Milliseconds()
		}

		claimsByStream := streamSelfClaims(r)
		for stream, nodes := range claimsByStream {
			if len(nodes) > st.MaxStreamClaims {
				st.MaxStreamClaims = len(nodes)
			}
			if len(nodes) >= 2 {
				if streamOpen[stream] == nil {
					streamOpen[stream] = &splitInterval{From: r.At, Nodes: nodes, Stream: stream}
				}
				streamOpen[stream].Rounds++
			}
		}
		for stream, iv := range streamOpen {
			if len(claimsByStream[stream]) < 2 {
				done := closeAt(iv, r.At)
				st.StreamSplits = append(st.StreamSplits, done)
				st.StreamSplitMs += done.LastsMs
				delete(streamOpen, stream)
			}
		}
	}
	if open != nil && len(rounds) > 0 {
		iv := closeAt(open, rounds[len(rounds)-1].At)
		st.SplitIntervals = append(st.SplitIntervals, iv)
		st.SplitBrainMs += iv.LastsMs
	}
	for _, iv := range streamOpen {
		done := closeAt(iv, rounds[len(rounds)-1].At)
		st.StreamSplits = append(st.StreamSplits, done)
		st.StreamSplitMs += done.LastsMs
	}
	sort.Slice(st.StreamSplits, func(i, j int) bool { return st.StreamSplits[i].From.Before(st.StreamSplits[j].From) })
	return st
}

// clusterProbe reads every node's /health in rounds until stopped.
type clusterProbe struct {
	containers []string // index = node-1
	interval   time.Duration

	mu     sync.Mutex
	rounds []healthRound

	cancel context.CancelFunc
	done   chan struct{}
}

func newClusterProbe(containers []string, interval time.Duration) *clusterProbe {
	return &clusterProbe{containers: containers, interval: interval}
}

func (p *clusterProbe) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	p.cancel = cancel
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		for {
			r := readHealthRound(ctx, p.containers)
			p.mu.Lock()
			p.rounds = append(p.rounds, r)
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.interval):
			}
		}
	}()
}

func (p *clusterProbe) stop() {
	if p.cancel != nil {
		p.cancel()
		<-p.done
	}
}

// latest returns the newest round, or false before the first one.
func (p *clusterProbe) latest() (healthRound, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.rounds) == 0 {
		return healthRound{}, false
	}
	return p.rounds[len(p.rounds)-1], true
}

// since returns the rounds read at or after t.
func (p *clusterProbe) since(t time.Time) []healthRound {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []healthRound
	for _, r := range p.rounds {
		if !r.At.Before(t) {
			out = append(out, r)
		}
	}
	return out
}

func (p *clusterProbe) all() []healthRound {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]healthRound(nil), p.rounds...)
}

// containerStates returns each container's state (running, paused, exited,
// created) from one `docker ps -a` call.
func containerStates(ctx context.Context, containers []string) map[string]string {
	out := map[string]string{}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(cctx, "docker", "ps", "-a", "--filter", "name=^bench-subject-", "--format", "{{.Names}}|{{.State}}").Output()
	if err != nil {
		return out
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		name, state, ok := strings.Cut(strings.TrimSpace(line), "|")
		if ok {
			out[name] = state
		}
	}
	for _, c := range containers {
		if _, ok := out[c]; !ok {
			out[c] = "absent"
		}
	}
	return out
}

// readHealthRound reads every running node's /health at once.
func readHealthRound(ctx context.Context, containers []string) healthRound {
	round := healthRound{At: time.Now()}
	states := containerStates(ctx, containers)
	results := make([]nodeHealth, len(containers))
	var wg sync.WaitGroup
	for i, c := range containers {
		results[i] = nodeHealth{Node: i + 1, State: states[c]}
		if states[c] != "running" {
			continue
		}
		wg.Add(1)
		go func(i int, c string) {
			defer wg.Done()
			results[i] = readNodeHealth(ctx, i+1, c)
			results[i].State = "running"
		}(i, c)
	}
	wg.Wait()
	for _, h := range results {
		if h.State == "absent" {
			continue
		}
		round.Nodes = append(round.Nodes, h)
	}
	return round
}

// readNodeHealth reads one node's /health through docker exec: the endpoint
// listens on the container's loopback only.
func readNodeHealth(ctx context.Context, n int, container string) nodeHealth {
	h := nodeHealth{Node: n}
	cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(cctx, "docker", "exec", container,
		"wget", "-qO-", "-T", "2", "http://127.0.0.1:15551/health").Output()
	if err != nil {
		return h
	}
	var doc healthDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return h
	}
	h.Reachable = true
	h.Name = doc.Node
	if h.Name == "" {
		h.Name = strconv.Itoa(n)
	}
	h.Phase = doc.StartupPhase
	if c, ok := doc.Components["vmmq"]; ok {
		h.VMMQ = c.Status
	}
	if doc.Cluster != nil {
		h.Clustered = true
		h.MetaLeader = doc.Cluster.MetaLeader
		h.Quorum = doc.Cluster.Quorum
		h.Peers = doc.Cluster.PeersConnected
		h.Reconfiguring = doc.Cluster.Reconfiguring
	}
	if doc.JetStream != nil {
		h.UnderRepl = doc.JetStream.StreamsUnderReplicated
		h.StreamLeaders = map[string]string{}
		for _, s := range doc.JetStream.StreamDetail {
			h.StreamLeaders[s.Name] = s.Leader
		}
	}
	return h
}

// describeRound renders a round in one line for the console and the report.
func describeRound(r healthRound) string {
	parts := make([]string, 0, len(r.Nodes))
	for _, h := range r.Nodes {
		switch {
		case h.State != "running":
			parts = append(parts, fmt.Sprintf("n%d:%s", h.Node, h.State))
		case !h.Reachable:
			parts = append(parts, fmt.Sprintf("n%d:no-answer", h.Node))
		case !h.Clustered:
			parts = append(parts, fmt.Sprintf("n%d:%s/standalone", h.Node, h.Phase))
		default:
			ml := h.MetaLeader
			if ml == "" {
				ml = "-"
			}
			parts = append(parts, fmt.Sprintf("n%d:%s/leader=%s/q=%s/ur=%d", h.Node, h.Phase, ml, h.Quorum, h.UnderRepl))
		}
	}
	return strings.Join(parts, " ")
}
