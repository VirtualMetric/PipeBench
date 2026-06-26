package config

import (
	"strings"
	"testing"
)

func TestValidateACLRotation(t *testing.T) {
	t.Parallel()

	// base is a valid director↔agent ACL-rotation case: subject-driven (agent
	// container, no generator), a min_received floor, and a well-formed
	// acl_rotation block.
	base := func() TestCase {
		return TestCase{
			Name:        "director_ip_allowlist_agent_rotation_recover",
			Type:        "director_agent_acl_rotation_correctness",
			Agent:       &AgentConfig{Image: "vmetric/director-enterprise"},
			Correctness: CorrectnessConfig{MinReceived: 4},
			ACLRotation: &ACLRotationConfig{
				Expect:     ACLRotationRecover,
				AllowedIPs: []string{"10.0.0.0/8", "172.16.0.0/12"},
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(tc *TestCase)
		wantErr string // substring of the expected error, "" = valid
	}{
		{name: "valid recover", mutate: func(*TestCase) {}},
		{
			name:    "empty allowed_ips",
			mutate:  func(tc *TestCase) { tc.ACLRotation.AllowedIPs = nil },
			wantErr: "allowed_ips must list",
		},
		{
			name:    "blank allowed_ips entry",
			mutate:  func(tc *TestCase) { tc.ACLRotation.AllowedIPs = []string{""} },
			wantErr: "allowed_ips[0]",
		},
		{
			name:    "whitespace allowed_ips entry",
			mutate:  func(tc *TestCase) { tc.ACLRotation.AllowedIPs = []string{"10.0.0.0/8", "   "} },
			wantErr: "allowed_ips[1]",
		},
		{
			name:    "unknown expect",
			mutate:  func(tc *TestCase) { tc.ACLRotation.Expect = "nope" },
			wantErr: "expect",
		},
		{
			name:    "missing agent",
			mutate:  func(tc *TestCase) { tc.Agent = nil },
			wantErr: "agent",
		},
		{
			name:    "min_received not set",
			mutate:  func(tc *TestCase) { tc.Correctness.MinReceived = 0 },
			wantErr: "min_received",
		},
		{
			name: "acl_rotation on wrong type",
			mutate: func(tc *TestCase) {
				tc.Type = "correctness"
			},
			wantErr: "only valid for type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tc := base()
			tt.mutate(&tc)
			err := tc.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}
