package runner

import (
	"context"
	"testing"
	"time"
)

// The agentless and device failover actions stop the owner's container. A
// baseline that timed out used to name no container, so the stop ran
// `docker stop ""` and the case reported a failover that was never tried.
func TestOwnerContainerOf(t *testing.T) {
	if got := ownerContainerOf("vmetric", "2"); got != "bench-subject-vmetric-2" {
		t.Errorf("ownerContainerOf(vmetric, 2) = %q, want bench-subject-vmetric-2", got)
	}
	if got := ownerContainerOf("vmetric", ""); got != "" {
		t.Errorf("with no owner, ownerContainerOf = %q, want none", got)
	}
}

// When the owner is known but never collects before the deadline, both waits
// still name its container, so the action stops the right node.
func TestBaselineWaitsNameTheOwnerWhenTheyTimeOut(t *testing.T) {
	at := time.Now().UTC().Format(time.RFC3339Nano)
	nodes := []string{"bench-subject-vmetric-1", "bench-subject-vmetric-2"}
	stubClusterLogs(t, map[string][]string{
		nodes[0]: {at + " [Debug] [director] Node is not cluster leader; skipping cluster update coordination"},
		// The owner was placed, and its collector never ran
		nodes[1]: {at + " [Information] [director] Assigned new device 1 (linuxcontainer) to node 2"},
	}, nodes...)
	past := time.Now().Add(-time.Second)

	owner, container, ok := waitAgentlessCollecting(context.Background(), "vmetric", nodes, past)
	if ok || owner != "2" || container != "bench-subject-vmetric-2" {
		t.Errorf("waitAgentlessCollecting = %q, %q, %v; want 2, bench-subject-vmetric-2, false", owner, container, ok)
	}
	owner, container, ok = waitDeviceCollecting(context.Background(), "vmetric", nodes, "Starting event collection of", past)
	if ok || owner != "2" || container != "bench-subject-vmetric-2" {
		t.Errorf("waitDeviceCollecting = %q, %q, %v; want 2, bench-subject-vmetric-2, false", owner, container, ok)
	}
}
