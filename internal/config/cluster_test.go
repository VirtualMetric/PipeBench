package config

import (
	"strings"
	"testing"
)

func TestValidateCluster(t *testing.T) {
	t.Parallel()

	// base is a valid cluster_ip_failover case: a 3-node director cluster whose
	// leader binds a virtual IP that must migrate on failover.
	base := func() TestCase {
		return TestCase{
			Name: "director_cluster_ip_failover_correctness",
			Type: "director_cluster_correctness",
			Cluster: &ClusterConfig{
				Nodes:  3,
				Action: "cluster_ip_failover",
				IP:     "172.30.0.250",
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(tc *TestCase)
		wantErr string // substring of the expected error, "" = valid
	}{
		{name: "valid cluster_ip_failover", mutate: func(*TestCase) {}},
		{
			name: "valid baseline (no action, no ip)",
			mutate: func(tc *TestCase) {
				tc.Cluster.Action = ""
				tc.Cluster.IP = ""
			},
		},
		{
			name:    "too few nodes",
			mutate:  func(tc *TestCase) { tc.Cluster.Nodes = 2 },
			wantErr: "nodes must be >= 3",
		},
		{
			name:    "unknown action",
			mutate:  func(tc *TestCase) { tc.Cluster.Action = "nope" },
			wantErr: "unknown cluster.action",
		},
		{
			name:    "cluster_ip_failover without ip",
			mutate:  func(tc *TestCase) { tc.Cluster.IP = "" },
			wantErr: "requires cluster.ip",
		},
		{
			name:    "cluster_ip_failover with invalid ip",
			mutate:  func(tc *TestCase) { tc.Cluster.IP = "not-an-ip" },
			wantErr: "must be an IPv4 address",
		},
		{
			name:    "cluster_ip_failover with IPv6 ip",
			mutate:  func(tc *TestCase) { tc.Cluster.IP = "fd00::1" },
			wantErr: "must be an IPv4 address",
		},
		{
			name:    "cluster_ip_failover ip outside bench subnet",
			mutate:  func(tc *TestCase) { tc.Cluster.IP = "10.0.0.5" },
			wantErr: "within the pinned bench subnet",
		},
		{
			name: "ip set without cluster_ip_failover action",
			mutate: func(tc *TestCase) {
				tc.Cluster.Action = "restart_leader"
			},
			wantErr: "only valid with cluster.action cluster_ip_failover",
		},
		{
			name: "cluster block on wrong type",
			mutate: func(tc *TestCase) {
				tc.Type = "correctness"
			},
			wantErr: "only valid for type director_cluster_correctness",
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
