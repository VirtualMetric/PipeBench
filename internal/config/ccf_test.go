package config

import (
	"strings"
	"testing"
)

func TestValidateCCF(t *testing.T) {
	t.Parallel()

	// base is a valid ccf_correctness case: a pagination scenario against the mock
	// CCF API endpoint.
	base := func() TestCase {
		return TestCase{
			Name:      "ccf_pagination_correctness",
			Type:      "ccf_correctness",
			Endpoints: []Endpoint{{Name: "ccf-api", Image: "vmetric/bench-ccfapi"}},
			CCF: &CCFConfig{
				Scenario:  "pagination",
				SeedCount: 100,
			},
		}
	}
	intp := func(n int) *int { return &n }

	tests := []struct {
		name    string
		mutate  func(tc *TestCase)
		wantErr string // substring of the expected error, "" = valid
	}{
		{name: "valid pagination", mutate: func(*TestCase) {}},
		{
			name:   "explicit zero expect_records allowed",
			mutate: func(tc *TestCase) { tc.CCF.ExpectRecords = intp(0) },
		},
		{
			name:   "positive expect_records allowed",
			mutate: func(tc *TestCase) { tc.CCF.ExpectRecords = intp(250) },
		},
		{
			name:    "negative expect_records rejected",
			mutate:  func(tc *TestCase) { tc.CCF.ExpectRecords = intp(-1) },
			wantErr: "expect_records must be >= 0",
		},
		{
			name:    "unknown scenario",
			mutate:  func(tc *TestCase) { tc.CCF.Scenario = "nope" },
			wantErr: "unknown ccf.scenario",
		},
		{
			name:    "missing endpoint",
			mutate:  func(tc *TestCase) { tc.Endpoints = nil },
			wantErr: "requires an endpoints entry",
		},
		{
			name: "ccf block on wrong type",
			mutate: func(tc *TestCase) {
				tc.Type = "correctness"
			},
			wantErr: "only valid for type ccf_correctness",
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
