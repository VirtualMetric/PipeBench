package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/results"
)

// newTestRunner builds a Runner writing to a throwaway store. The store field is
// unexported, which is why this test lives in package runner rather than
// runner_test — saveResult is the thing under test and it is unexported too.
func newTestRunner(t *testing.T, ctx context.Context, cpuLimit, memLimit string) (*Runner, string) {
	t.Helper()
	dir := t.TempDir()
	return &Runner{
		ctx:         ctx,
		store:       results.NewStore(dir),
		runCPULimit: cpuLimit,
		runMemLimit: memLimit,
	}, dir
}

// readEntry returns the single persisted result entry as a raw map, so the test
// asserts on the JSON that actually lands on disk rather than on a Go struct
// that would happily zero-fill a missing key.
func readEntry(t *testing.T, baseDir, hardware, subject string) map[string]any {
	t.Helper()
	path := filepath.Join(baseDir, hardware, subject+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	var file struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("unmarshalling %s: %v", path, err)
	}
	if len(file.Results) != 1 {
		t.Fatalf("expected 1 persisted entry, got %d — %s", len(file.Results), data)
	}
	return file.Results[0]
}

// TestSaveResultStampsEffectiveLimits is the regression for the defect this
// function's stamp exists to close.
//
// The RunResult handed in has EMPTY limit fields — the shape six of the fourteen
// result constructors produce (runDirectorAgentCertRotation,
// runDirectorAgentACLRotation, runKafkaOffsetCommitRestart, saveCCFResult,
// saveHTTPSourceResult, saveAuxResult). Those runs were filed as unrestricted
// while the container ran under a ceiling. Because every one of the fourteen
// funnels through saveResult, asserting here covers all of them at once.
func TestSaveResultStampsEffectiveLimits(t *testing.T) {
	r, dir := newTestRunner(t, context.Background(), "2", "512m")

	if _, err := r.saveResult(results.RunResult{
		TestName: "some_correctness",
		Subject:  "vmetric",
		Hardware: "custom",
		// SubjectCPULimit / SubjectMemLimit deliberately unset.
	}, ""); err != nil {
		t.Fatalf("saveResult: %v", err)
	}

	entry := readEntry(t, dir, "custom", "vmetric")
	if got := entry["subject_cpu_limit"]; got != "2" {
		t.Errorf("subject_cpu_limit = %v, want %q", got, "2")
	}
	if got := entry["subject_mem_limit"]; got != "512m" {
		t.Errorf("subject_mem_limit = %v, want %q", got, "512m")
	}
}

// TestSaveResultOmitsAbsentLimits is the other half: the stamp must never invent
// a ceiling. An unconstrained run has to stay recorded as unconstrained, or the
// fix reintroduces the original defect with the sign flipped.
func TestSaveResultOmitsAbsentLimits(t *testing.T) {
	r, dir := newTestRunner(t, context.Background(), "", "")

	if _, err := r.saveResult(results.RunResult{
		TestName: "some_correctness",
		Subject:  "vmetric",
		Hardware: "custom",
	}, ""); err != nil {
		t.Fatalf("saveResult: %v", err)
	}

	entry := readEntry(t, dir, "custom", "vmetric")
	if _, ok := entry["subject_cpu_limit"]; ok {
		t.Error("subject_cpu_limit present for an unconstrained run — omitempty was defeated")
	}
	if _, ok := entry["subject_mem_limit"]; ok {
		t.Error("subject_mem_limit present for an unconstrained run — omitempty was defeated")
	}
}

// TestSaveResultInterruptedWritesNothing keeps the pre-existing guard intact: a
// verdict computed after cancellation reflects a half-finished run, and the
// stamp must not have moved the early return.
func TestSaveResultInterruptedWritesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r, dir := newTestRunner(t, ctx, "2", "512m")

	if _, err := r.saveResult(results.RunResult{
		TestName: "some_correctness",
		Subject:  "vmetric",
		Hardware: "custom",
	}, ""); err == nil {
		t.Fatal("saveResult on a cancelled context returned nil error")
	}

	if _, err := os.Stat(filepath.Join(dir, "custom", "vmetric.json")); !os.IsNotExist(err) {
		t.Errorf("an interrupted run wrote to the store (stat err = %v)", err)
	}
}
