package runner

import (
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

func restartCase(c config.CorrectnessConfig) *config.TestCase {
	return &config.TestCase{Name: "restart", Type: "persistence_restart_correctness", Correctness: c}
}

func wantErrors(t *testing.T, v restartVerdict, parts ...string) {
	t.Helper()

	if len(v.Errors) != len(parts) {
		t.Fatalf("errors = %q, want %d", v.Errors, len(parts))
	}
	for i, part := range parts {
		if !strings.Contains(v.Errors[i], part) {
			t.Fatalf("error %d = %q, want it to say %q", i, v.Errors[i], part)
		}
	}
}

// A baseline that loses nothing built nothing to lose, so the floor fails it.
func TestRestartVerdictLossFloor(t *testing.T) {
	tc := restartCase(config.CorrectnessConfig{ExpectedLossPct: 100, MinLossPct: 5})

	wantErrors(t, judgeRestart(tc, 1000, ReceiverMetrics{LinesReceived: 1000}, ""), "expected loss >= 5.00%")
	wantErrors(t, judgeRestart(tc, 1000, ReceiverMetrics{LinesReceived: 960}, ""), "proves nothing")
	wantErrors(t, judgeRestart(tc, 1000, ReceiverMetrics{LinesReceived: 500}, ""))
}

// With the receiver tracking distinct lines, copies cannot hide a loss: 1,000
// lines arrived here, but 100 of them are copies.
func TestRestartVerdictCountsUniqueLinesWhenTracked(t *testing.T) {
	recv := ReceiverMetrics{LinesReceived: 1000, UniqueLines: 900, Duplicates: 100}

	tracked := restartCase(config.CorrectnessConfig{ValidateDedup: true, AllowOverDelivery: true})
	v := judgeRestart(tracked, 1000, recv, "")
	wantErrors(t, v, "100 of 1,000 lines lost, counted by unique")
	if v.Basis != "unique" || v.Accounted != 900 {
		t.Fatalf("basis %q, accounted %d", v.Basis, v.Accounted)
	}

	untracked := restartCase(config.CorrectnessConfig{})
	wantErrors(t, judgeRestart(untracked, 1000, recv, ""))
}

// Copies fail a case that tracks them, unless the case allows them. Loss still
// fails it.
func TestRestartVerdictCopiesUnderAllowOverDelivery(t *testing.T) {
	recv := ReceiverMetrics{LinesReceived: 1100, UniqueLines: 1000, Duplicates: 100}

	strict := restartCase(config.CorrectnessConfig{ValidateDedup: true})
	wantErrors(t, judgeRestart(strict, 1000, recv, ""), "over-delivery", "expected 0 duplicates")

	allowed := restartCase(config.CorrectnessConfig{ValidateDedup: true, AllowOverDelivery: true})
	v := judgeRestart(allowed, 1000, recv, "")
	wantErrors(t, v)
	if len(v.Notes) != 1 {
		t.Fatalf("notes = %q, want the over-delivery noted", v.Notes)
	}
}

// A required line must be in the stopped subject's log. The pattern can demand
// a wait of at least a second, so that a run with nothing to wait for fails.
func TestRestartVerdictRequiresAStopLogLine(t *testing.T) {
	const pattern = `held on the broker in ([0-9]+m)?[0-9.]+s\s*$`
	tc := restartCase(config.CorrectnessConfig{RequireStopLog: []string{pattern}})
	recv := ReceiverMetrics{LinesReceived: 10}

	for log, ok := range map[string]bool{
		"":                                       false,
		"some other line\n":                      false,
		"held on the broker in 536.524µs\n":      false,
		"held on the broker in 200.409757ms\n":   false,
		"held on the broker in 15.600951899s\n":  true,
		"a\nheld on the broker in 1m2.5s\r\nb\n": true,
	} {
		v := judgeRestart(tc, 10, recv, log)
		if ok && len(v.Errors) != 0 {
			t.Fatalf("log %q: errors %q", log, v.Errors)
		}
		if !ok && len(v.Errors) != 1 {
			t.Fatalf("log %q: errors %q, want the missing line named", log, v.Errors)
		}
	}
}
