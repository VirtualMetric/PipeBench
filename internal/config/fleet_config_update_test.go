package config

import (
	"strings"
	"testing"
)

func TestValidateFleetConfigUpdateDataPlane(t *testing.T) {
	t.Parallel()

	// base is a valid data-plane config_update case: a fleet director driven by a
	// generator, a min_received floor, the fleet-sim endpoint, and a deliver_config
	// naming the BEFORE pipeline (.vmf) delivered ahead of configs/update.vmf.
	base := func() TestCase {
		return TestCase{
			Name:        "target_config_update_correctness",
			Type:        "fleet_automation_correctness",
			Endpoints:   []Endpoint{{Name: "fleet-sim", Image: "vmetric/bench-fleetsim:latest"}},
			Generator:   GeneratorConfig{Mode: "tcp", Target: "subject:9000"},
			Correctness: CorrectnessConfig{MinReceived: 4},
			Subjects:    []string{"vmetric"},
			Fleet: &FleetConfig{
				Scenario:      "config_update",
				DeliverConfig: "before.vmf",
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(tc *TestCase)
		wantErr string // substring of the expected error, "" = valid
	}{
		{name: "valid data-plane config_update", mutate: func(*TestCase) {}},
		{
			name:    "data-plane without a generator",
			mutate:  func(tc *TestCase) { tc.Generator = GeneratorConfig{} },
			wantErr: "requires a generator",
		},
		{
			name:    "data-plane without min_received",
			mutate:  func(tc *TestCase) { tc.Correctness.MinReceived = 0 },
			wantErr: "min_received",
		},
		{
			// Legacy control-plane config_update (no deliver_config) stays valid
			// with no generator and no min_received.
			name: "legacy executed-only config_update",
			mutate: func(tc *TestCase) {
				tc.Fleet.DeliverConfig = ""
				tc.Generator = GeneratorConfig{}
				tc.Correctness.MinReceived = 0
			},
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
