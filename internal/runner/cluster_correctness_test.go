package runner

import (
	"strings"
	"testing"
	"time"
)

// stubClusterLogs makes the older cluster driver read the logs from logs, for
// one test. Each line starts with the time it was logged, so that a read
// since a time, or for the last few seconds, returns only the lines from then
// on. Only the containers in running run.
func stubClusterLogs(t *testing.T, logs map[string][]string, running ...string) {
	t.Helper()
	origAll, origSince, origRunning := dockerLogsAll, dockerLogsSince, containerRunning
	t.Cleanup(func() { dockerLogsAll, dockerLogsSince, containerRunning = origAll, origSince, origRunning })
	dockerLogsAll = func(c string) string { return strings.Join(logs[c], "\n") }
	dockerLogsSince = func(c, since string) string {
		from, err := time.Parse(time.RFC3339Nano, since)
		if err != nil {
			d, derr := time.ParseDuration(since)
			if derr != nil {
				t.Fatalf("the driver read the window %q, which is neither a time nor a duration", since)
			}
			from = time.Now().Add(-d)
		}
		var out []string
		for _, line := range logs[c] {
			stamp, _, _ := strings.Cut(line, " ")
			if at, err := time.Parse(time.RFC3339Nano, stamp); err == nil && !at.Before(from) {
				out = append(out, line)
			}
		}
		return strings.Join(out, "\n")
	}
	containerRunning = func(c string) bool {
		for _, r := range running {
			if r == c {
				return true
			}
		}
		return false
	}
}

// restart_leader used to pass whenever a leader was present after the
// restart, even when no node was elected again: the old leader, back after
// its restart, passed for a new one.
func TestRestartLeaderNeedsAReElection(t *testing.T) {
	began := time.Now().UTC().Add(-time.Minute)
	stamp := func(d time.Duration) string { return began.Add(d).Format(time.RFC3339Nano) }
	nodes := []string{"node-1", "node-2", "node-3"}

	// Node 1 led from the start, and leads again after its restart. Nobody
	// logged "became leader on" since the restart began.
	stubClusterLogs(t, map[string][]string{
		"node-1": {stamp(-10*time.Minute) + " Node 1 became leader on 7001", stamp(15*time.Second) + " director started"},
		"node-2": {stamp(-10*time.Minute) + " not cluster leader"},
		"node-3": {stamp(-10*time.Minute) + " not cluster leader"},
	}, nodes...)
	if n, why := reElection(nodes, began); why == "" {
		t.Fatalf("reElection = node %d, no problem; want a failure: the old leader is back, but no node was elected", n)
	}

	// Node 2 was elected a second after the restart began, while docker
	// restart was still running.
	stubClusterLogs(t, map[string][]string{
		"node-1": {stamp(-10*time.Minute) + " Node 1 became leader on 7001"},
		"node-2": {stamp(time.Second) + " Node 2 became leader on 7001"},
		"node-3": {stamp(-10*time.Minute) + " not cluster leader"},
	}, nodes...)
	if n, why := reElection(nodes, began); why != "" || n != 2 {
		t.Fatalf("reElection = node %d, %q; want node 2, elected during the restart", n, why)
	}
}

// stop_two_recover used to note, and not fail, a node that still led after
// the harness stopped enough nodes to take the quorum away.
func TestStopTwoRecoverFailsALeaderWithoutQuorum(t *testing.T) {
	now := time.Now().UTC()
	stamp := func(d time.Duration) string { return now.Add(d).Format(time.RFC3339Nano) }
	nodes := []string{"node-1", "node-2", "node-3"}

	// Node 1 is the only node left running, and it still leads.
	stubClusterLogs(t, map[string][]string{
		"node-1": {stamp(-10*time.Minute) + " Node 1 became leader on 7001"},
	}, "node-1")
	if why := leaderWithoutQuorum(nodes, 2, 3); !strings.Contains(why, "node 1 still led with 2 of 3 nodes stopped") {
		t.Fatalf("leaderWithoutQuorum = %q; want node 1 named as leading without a quorum", why)
	}

	// Node 1 gave up the leadership when it lost the quorum.
	stubClusterLogs(t, map[string][]string{
		"node-1": {stamp(-10*time.Minute) + " Node 1 became leader on 7001", stamp(-5*time.Second) + " Node 1 is no longer the leader"},
	}, "node-1")
	if why := leaderWithoutQuorum(nodes, 2, 3); why != "" {
		t.Fatalf("leaderWithoutQuorum = %q; want nothing when no node leads", why)
	}
}
