package config

import (
	"strings"
	"testing"
)

func TestValidateVerifier(t *testing.T) {
	t.Parallel()

	// base is a valid verifier case: singular generator with total_lines, an
	// aws: block for S3 access, and a well-formed verifier block.
	base := func() TestCase {
		return TestCase{
			Name:      "tcp_to_s3_parquet_correctness",
			Type:      "correctness",
			Generator: GeneratorConfig{Mode: "tcp", Target: "subject:9000", TotalLines: 1000},
			AWS:       &AWSConfig{Buckets: []string{"bench-out"}},
			Verifier:  &VerifierConfig{S3Bucket: "bench-out", Format: "parquet"},
		}
	}

	tests := []struct {
		name    string
		mutate  func(tc *TestCase)
		wantErr string // substring of the expected error, "" = valid
	}{
		{name: "valid parquet verifier", mutate: func(*TestCase) {}},
		{
			name:    "missing total_lines disables exact-count",
			mutate:  func(tc *TestCase) { tc.Generator.TotalLines = 0 },
			wantErr: "total_lines",
		},
		{
			name: "plural generators unsupported",
			mutate: func(tc *TestCase) {
				tc.Generator = GeneratorConfig{}
				tc.Generators = []GeneratorConfig{{ID: "g1", Mode: "tcp", Target: "subject:9000", TotalLines: 1000}}
			},
			wantErr: "generators",
		},
		{
			name:    "missing source",
			mutate:  func(tc *TestCase) { tc.Verifier.S3Bucket = "" },
			wantErr: "s3_bucket",
		},
		{
			name:    "bad format",
			mutate:  func(tc *TestCase) { tc.Verifier.Format = "orc" },
			wantErr: "format",
		},
		{
			name:    "no s3 emulator",
			mutate:  func(tc *TestCase) { tc.AWS = nil },
			wantErr: "aws",
		},
		{
			name:    "plural receivers rejected",
			mutate:  func(tc *TestCase) { tc.Receivers = []ReceiverConfig{{ID: "r1", Mode: "tcp", Listen: ":9001"}} },
			wantErr: "receiver",
		},
		{
			name:    "singular receiver rejected",
			mutate:  func(tc *TestCase) { tc.Receiver = ReceiverConfig{Mode: "tcp", Listen: ":9001"} },
			wantErr: "receiver",
		},
		{
			name:    "bad quiet_window",
			mutate:  func(tc *TestCase) { tc.Verifier.QuietWindow = "soon" },
			wantErr: "quiet_window",
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

// TestValidateVerifierLocal covers the local-file verifier path (LocalDir set
// instead of S3Bucket): it needs no aws:/minio: emulator, and the two sources
// are mutually exclusive.
func TestValidateVerifierLocal(t *testing.T) {
	t.Parallel()

	// base is a valid local verifier case: no aws: block, a LocalDir source.
	base := func() TestCase {
		return TestCase{
			Name:      "file_target_parquet_correctness",
			Type:      "correctness",
			Generator: GeneratorConfig{Mode: "tcp", Target: "subject:9000", TotalLines: 1000},
			Verifier:  &VerifierConfig{LocalDir: "/data/out", Format: "parquet"},
		}
	}

	tests := []struct {
		name    string
		mutate  func(tc *TestCase)
		wantErr string
	}{
		{name: "valid local verifier needs no emulator", mutate: func(*TestCase) {}},
		{
			name:    "both sources rejected",
			mutate:  func(tc *TestCase) { tc.Verifier.S3Bucket = "bench-out" },
			wantErr: "mutually exclusive",
		},
		{
			name:    "neither source rejected",
			mutate:  func(tc *TestCase) { tc.Verifier.LocalDir = "" },
			wantErr: "exactly one",
		},
		{
			name:    "bad format still checked",
			mutate:  func(tc *TestCase) { tc.Verifier.Format = "orc" },
			wantErr: "format",
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
				if !tc.Verifier.IsLocal() {
					t.Errorf("IsLocal() = false, want true for LocalDir case")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}
