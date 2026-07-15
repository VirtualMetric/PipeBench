package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestParseByteSize covers the size strings subject_disk accepts — the
// set must stay consistent with config's subject_disk.size validation so
// a case that passes Validate can never fail at compose-render time.
func TestParseByteSize(t *testing.T) {
	good := map[string]int64{
		"64m":     64 << 20,
		"64M":     64 << 20,
		"64mb":    64 << 20,
		"10MB":    10 << 20,
		"1g":      1 << 30,
		"2GB":     2 << 30,
		"512k":    512 << 10,
		"512kb":   512 << 10,
		"1048576": 1 << 20,
		"64b":     64,
		" 64m ":   64 << 20,
	}
	for in, want := range good {
		got, err := parseByteSize(in)
		if err != nil {
			t.Errorf("parseByteSize(%q): unexpected error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseByteSize(%q) = %d, want %d", in, got, want)
		}
	}

	bad := []string{"", "abc", "-5m", "0", "m", "64x", "64bb", "1.5g"}
	for _, in := range bad {
		if got, err := parseByteSize(in); err == nil {
			t.Errorf("parseByteSize(%q) = %d, want error", in, got)
		}
	}
}

// TestComposeRendersSubjectDiskTmpfs verifies the subject_disk block
// renders a long-syntax tmpfs mount on the subject service with the size
// pre-parsed to bytes, and that cases without the block emit no tmpfs.
func TestComposeRendersSubjectDiskTmpfs(t *testing.T) {
	render := func(t *testing.T, disk *config.SubjectDiskConfig) string {
		t.Helper()
		tc := &config.TestCase{
			Name:     "disk-smoke",
			Type:     "correctness",
			Duration: "10s",
			Generator: config.GeneratorConfig{
				Mode:   "tcp",
				Target: "subject:9000",
			},
			Receiver: config.ReceiverConfig{
				Mode:   "tcp",
				Listen: ":9001",
			},
			SubjectDisk: disk,
		}
		subj := config.Subject{
			Name:       "vmetric",
			Image:      "vmetric/director",
			Version:    "dev",
			ConfigPath: "/config.yml",
		}
		tmp := t.TempDir()
		composePath := filepath.Join(tmp, "compose.yaml")
		cfg := RunConfig{
			TestCase:         tc,
			Subject:          subj,
			ConfigName:       "default",
			ConfigSrcPath:    composePath,
			TmpDir:           tmp,
			GeneratorImage:   "img-gen",
			ReceiverImage:    "img-recv",
			CollectorImage:   "img-coll",
			ReceiverHostPort: 19001,
		}
		if err := writeCompose(composePath, cfg); err != nil {
			t.Fatalf("writeCompose: %v", err)
		}
		data, err := os.ReadFile(composePath)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	out := render(t, &config.SubjectDiskConfig{Path: "/opt/vmetric/storage", Size: "64m"})
	mustContain(t, out, "- type: tmpfs")
	mustContain(t, out, `target: "/opt/vmetric/storage"`)
	mustContain(t, out, fmt.Sprintf("size: %d", int64(64<<20)))
	mustContain(t, out, "mode: 01777")

	out = render(t, nil)
	mustNotContain(t, out, "type: tmpfs")

	// A malformed size must fail the render with a clear error, not
	// produce a compose file docker rejects later.
	tc := &config.TestCase{
		Name: "disk-bad", Type: "correctness",
		Generator:   config.GeneratorConfig{Mode: "tcp", Target: "subject:9000"},
		Receiver:    config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		SubjectDisk: &config.SubjectDiskConfig{Path: "/data", Size: "sixty-four"},
	}
	tmp := t.TempDir()
	composePath := filepath.Join(tmp, "compose.yaml")
	err := writeCompose(composePath, RunConfig{
		TestCase: tc,
		Subject:  config.Subject{Name: "vmetric", Image: "img", ConfigPath: "/config.yml"},
		TmpDir:   tmp, ConfigSrcPath: composePath,
		GeneratorImage: "g", ReceiverImage: "r", CollectorImage: "c",
	})
	if err == nil {
		t.Fatal("expected error for malformed subject_disk.size, got nil")
	}
}
