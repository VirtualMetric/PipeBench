package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestWriteAWSInitSeedDelayAndToday verifies that a seed group's delay_seconds
// renders a sleep before its upload loop and that the $TODAY prefix token is
// substituted (UTC YYYY/MM/DD) by the harness, not left for the shell.
func TestWriteAWSInitSeedDelayAndToday(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "aws-init.sh")
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	aws := &config.AWSConfig{
		Buckets: []string{"bench-in"},
		SeedObjects: []config.AWSSeedObjects{
			{Bucket: "bench-in", Prefix: "old/", Objects: 2, Lines: 3, Marker: "OLD"},
			{Bucket: "bench-in", Prefix: "logs/$TODAY/", Objects: 2, Lines: 3, Marker: "NEW", DelaySeconds: 45},
			{Bucket: "bench-in", Prefix: "poison/", Objects: 1, Lines: 3, Marker: "BAD", Extension: ".gz"},
		},
	}
	if err := writeAWSInit(path, aws, now); err != nil {
		t.Fatalf("writeAWSInit: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)
	if !strings.Contains(out, "s3://bench-in/logs/2026/08/27/obj-") {
		t.Fatalf("$TODAY not substituted:\n%s", out)
	}
	if strings.Contains(out, "$TODAY") {
		t.Fatalf("$TODAY leaked into the script:\n%s", out)
	}
	sleepIdx := strings.Index(out, "sleep 45\n")
	newIdx := strings.Index(out, "logs/2026/08/27/")
	oldIdx := strings.Index(out, "s3://bench-in/old/")
	if sleepIdx < 0 || newIdx < 0 || oldIdx < 0 || !(oldIdx < sleepIdx && sleepIdx < newIdx) {
		t.Fatalf("expected old loop, then sleep 45, then new loop; got old=%d sleep=%d new=%d\n%s", oldIdx, sleepIdx, newIdx, out)
	}
	if strings.Count(out, "sleep ") != 1 {
		t.Fatalf("only the delayed group may sleep:\n%s", out)
	}
	// The default extension stays .log; an explicit extension renames the key
	// only (the body is still plain text, i.e. a poison object for .gz).
	if !strings.Contains(out, "s3://bench-in/old/obj-'\"$i\"'.log'") {
		t.Fatalf("default .log extension missing:\n%s", out)
	}
	if !strings.Contains(out, "s3://bench-in/poison/obj-'\"$i\"'.gz'") {
		t.Fatalf("explicit .gz extension missing:\n%s", out)
	}
}
