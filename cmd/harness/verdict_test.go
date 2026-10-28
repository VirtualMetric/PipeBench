package main

import (
	"errors"
	"reflect"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/results"
)

// TestFailedVerdicts: --fail-on-verdict names the runs that finished but did
// not pass their checks. A run that could not finish is already named by its
// error, and a run without checks has no verdict to fail.
func TestFailedVerdicts(t *testing.T) {
	yes, no := true, false
	outcomes := []runOutcome{
		{testName: "passed", subjectName: "vmetric", result: results.RunResult{Passed: &yes}},
		{testName: "failed", subjectName: "vmetric", result: results.RunResult{Passed: &no}},
		{testName: "no_checks", subjectName: "vmetric"},
		{testName: "broke", subjectName: "vmetric", result: results.RunResult{Passed: &no}, runErr: errors.New("docker is down")},
	}
	if got, want := failedVerdicts(outcomes), []string{"failed/vmetric"}; !reflect.DeepEqual(got, want) {
		t.Errorf("failedVerdicts = %v, want %v", got, want)
	}
	if got := failedVerdicts(outcomes[:1]); len(got) != 0 {
		t.Errorf("one run that passed: %v, want none", got)
	}
}
