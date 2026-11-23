package runner

import "testing"

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
