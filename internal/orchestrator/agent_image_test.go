package orchestrator

import "testing"

// Agent-mode cases were silently running :latest because --version reached the
// subject but not the agent. The bug was invisible: the case passed, and read
// as agent coverage while exercising whatever agent was published last.
func TestApplyVersionIfUntagged(t *testing.T) {
	tests := []struct {
		name    string
		image   string
		version string
		want    string
	}{
		{"untagged gets the run version", "vmetric/agent", "2.1.0", "vmetric/agent:2.1.0"},
		{"bare name", "agent", "2.1.0", "agent:2.1.0"},

		// A pinned tag is a deliberate choice — upgrade-compat cases hold the
		// agent OLDER than the subject on purpose.
		{"explicit tag is kept", "vmetric/agent:oneknob", "2.1.0", "vmetric/agent:oneknob"},
		{"explicit latest is kept", "vmetric/agent:latest", "2.1.0", "vmetric/agent:latest"},
		{"digest pin is kept", "vmetric/agent@sha256:abc", "2.1.0", "vmetric/agent@sha256:abc"},

		// A registry port is a colon that is NOT a tag. Treating it as one
		// would leave the image untagged and silently reintroduce the bug.
		{"registry port, untagged", "registry:5000/vmetric/agent", "2.1.0", "registry:5000/vmetric/agent:2.1.0"},
		{"registry port, tagged", "registry:5000/vmetric/agent:oneknob", "2.1.0", "registry:5000/vmetric/agent:oneknob"},

		{"empty image", "", "2.1.0", ""},
		{"empty version", "vmetric/agent", "", "vmetric/agent"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := applyVersionIfUntagged(test.image, test.version); got != test.want {
				t.Errorf("applyVersionIfUntagged(%q, %q) = %q, want %q", test.image, test.version, got, test.want)
			}
		})
	}
}
