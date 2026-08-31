package runner

import (
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"github.com/VirtualMetric/PipeBench/internal/results"
)

func TestApplyMaxReceived(t *testing.T) {
	t.Parallel()

	passed, failed := true, false
	tests := []struct {
		name       string
		cap        int64
		received   int64
		in         results.RunResult
		wantPassed *bool
		wantReason string
	}{
		{"disabled", 0, 99999, results.RunResult{Passed: &passed}, &passed, ""},
		{"under cap untouched", 100, 100, results.RunResult{Passed: &passed}, &passed, ""},
		{"over cap fails a passing result", 100, 101, results.RunResult{Passed: &passed}, &failed, "max_received exceeded: expected <= 100 lines, got 101"},
		{"over cap appends to a failing result", 100, 250, results.RunResult{Passed: &failed, FailReason: "loss"}, &failed, "loss; max_received exceeded: expected <= 100 lines, got 250"},
		{"over cap with no verdict yet fails", 100, 101, results.RunResult{}, &failed, "max_received exceeded: expected <= 100 lines, got 101"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tc := &config.TestCase{Correctness: config.CorrectnessConfig{MaxReceived: tt.cap}}
			res := tt.in
			applyMaxReceived(tc, tt.received, &res)
			if (res.Passed == nil) != (tt.wantPassed == nil) || (res.Passed != nil && *res.Passed != *tt.wantPassed) {
				t.Fatalf("passed = %v want %v", res.Passed, tt.wantPassed)
			}
			if res.FailReason != tt.wantReason {
				t.Fatalf("reason = %q want %q", res.FailReason, tt.wantReason)
			}
		})
	}
}
