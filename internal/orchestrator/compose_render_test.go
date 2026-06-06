package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestSingularComposeRendersClean verifies an existing singular-form
// case still produces a compose file with the singular `generator:` and
// `receiver:` service names and no unexpected blocks. Guards against
// the additive multi-receiver/multi-generator branches accidentally
// flipping on for legacy cases.
func TestSingularComposeRendersClean(t *testing.T) {
	tc := &config.TestCase{
		Name:     "smoke",
		Type:     "correctness",
		Duration: "10s",
		Warmup:   "5s",
		Generator: config.GeneratorConfig{
			Mode:     "tcp",
			Target:   "subject:9000",
			Rate:     100,
			LineSize: 256,
			Format:   "raw",
		},
		Receiver: config.ReceiverConfig{
			Mode:   "tcp",
			Listen: ":9001",
		},
	}
	subj := config.Subject{
		Name:       "vmetric",
		Image:      "vmetric/director",
		Version:    "2.0.1",
		ConfigPath: "/config.yml",
	}
	tmp, err := os.MkdirTemp("", "compose-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase:         tc,
		Subject:          subj,
		ConfigName:       "default",
		ConfigSrcPath:    composePath, // any existing path is fine for the template
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
	out := string(data)

	mustContain(t, out, "  generator:\n")
	mustContain(t, out, "  receiver:\n")
	mustContain(t, out, "container_name: \"bench-generator\"")
	mustContain(t, out, "container_name: \"bench-receiver\"")
	mustContain(t, out, "GENERATOR_MODE: \"tcp\"")
	mustContain(t, out, "RECEIVER_LISTEN: \":9001\"")
	mustNotContain(t, out, "GENERATOR_TLS:")
	mustNotContain(t, out, "RECEIVER_RECORD_ARRIVAL_TIMES:")
	mustNotContain(t, out, "/certs")
	// No plural service names should appear.
	mustNotContain(t, out, "generator-default:")
	mustNotContain(t, out, "receiver-default:")
}

// TestPluralComposeRenders verifies that opting into the plural form
// produces per-id services and the new env wiring.
func TestPluralComposeRenders(t *testing.T) {
	tc := &config.TestCase{
		Name:     "smoke-plural",
		Type:     "correctness",
		Duration: "10s",
		Warmup:   "5s",
		Generators: []config.GeneratorConfig{
			{ID: "src-a", Mode: "tcp", Target: "subject:9000", Rate: 50, LineSize: 256, Format: "raw", Connections: 1},
			{ID: "src-b", Mode: "udp", Target: "subject:9000", Rate: 50, LineSize: 256, Format: "raw", Connections: 1},
		},
		Receivers: []config.ReceiverConfig{
			{ID: "sink-a", Mode: "tcp", Listen: ":9001"},
			{ID: "sink-b", Mode: "http", Listen: ":9002"},
		},
		Correctness: config.CorrectnessConfig{
			RateCeiling: config.RateCeilingConfig{MaxEPS: 100, Window: "1s"},
		},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{
		Name:       "vmetric",
		Image:      "vmetric/director",
		Version:    "2.0.1",
		ConfigPath: "/config.yml",
	}
	tmp, err := os.MkdirTemp("", "compose-plural-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
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
	out := string(data)
	mustContain(t, out, "generator-src-a:")
	mustContain(t, out, "generator-src-b:")
	mustContain(t, out, "receiver-sink-a:")
	mustContain(t, out, "receiver-sink-b:")
	mustContain(t, out, "container_name: \"bench-receiver-sink-a\"")
	mustContain(t, out, "container_name: \"bench-generator-src-b\"")
	mustContain(t, out, "GENERATOR_ID: \"src-a\"")
	mustContain(t, out, "GENERATOR_CONN_OFFSET: \"0\"")
	mustContain(t, out, "GENERATOR_CONN_OFFSET: \"1\"")
	mustContain(t, out, "RECEIVER_RECORD_ARRIVAL_TIMES: \"true\"")
	mustContain(t, out, "19001:9090")
	mustContain(t, out, "19002:9090")
	mustNotContain(t, out, "  generator:\n")
	mustNotContain(t, out, "  receiver:\n")
}

func TestValidateRejectsBothForms(t *testing.T) {
	tc := &config.TestCase{
		Name:      "bad",
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "x:1"},
		Generators: []config.GeneratorConfig{
			{ID: "g1", Mode: "tcp", Target: "x:1"},
		},
	}
	if err := tc.Validate(); err == nil {
		t.Fatal("expected error when both singular and plural generator forms are set")
	}
}

// TestSingularComposeRendersSampleFile verifies the singular generator's
// sample_file replay wiring: the input file is bind-mounted read-only at
// /input/<base> and GENERATOR_SAMPLE_FILE / GENERATOR_REWRITE_TIMESTAMP are set.
func TestSingularComposeRendersSampleFile(t *testing.T) {
	tc := &config.TestCase{
		Name:     "smoke-sample",
		Type:     "correctness",
		Duration: "10s",
		Generator: config.GeneratorConfig{
			Mode:             "tcp",
			Target:           "subject:9000",
			Format:           "cef",
			SampleFile:       "input/sample.cef",
			RewriteTimestamp: true,
			TotalLines:       10,
		},
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.1", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-sample-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	// writeCompose now resolves and verifies the sample file on disk, so the
	// case-relative input must actually exist.
	writeSampleFile(t, tmp, "input/sample.cef")
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath, CaseDir: tmp, TmpDir: tmp,
		GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll",
		ReceiverHostPort: 19001,
	}
	if err := writeCompose(composePath, cfg); err != nil {
		t.Fatalf("writeCompose: %v", err)
	}
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)
	wantHost := filepath.ToSlash(filepath.Join(tmp, "input/sample.cef"))
	mustContain(t, out, wantHost+":/input/sample.cef:ro")
	mustContain(t, out, "GENERATOR_SAMPLE_FILE: \"/input/sample.cef\"")
	mustContain(t, out, "GENERATOR_REWRITE_TIMESTAMP: \"true\"")
}

// TestPluralComposeRendersSampleFile verifies per-generator sample_file replay
// in the plural form: the generator that opts in gets its own bind mount +
// GENERATOR_SAMPLE_FILE, and the one that doesn't gets neither (per-generator
// gating, not a global flag).
func TestPluralComposeRendersSampleFile(t *testing.T) {
	tc := &config.TestCase{
		Name:     "smoke-plural-sample",
		Type:     "correctness",
		Duration: "10s",
		Generators: []config.GeneratorConfig{
			{ID: "src-a", Mode: "tcp", Target: "subject:9000", Format: "cef", SampleFile: "input/a.cef", RewriteTimestamp: true, Connections: 1, TotalLines: 10},
			{ID: "src-b", Mode: "tcp", Target: "subject:9000", Format: "raw", Connections: 1},
		},
		Receivers: []config.ReceiverConfig{{ID: "sink", Mode: "tcp", Listen: ":9001"}},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.1", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-plural-sample-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	// The opted-in generator's sample file must exist on disk for writeCompose.
	writeSampleFile(t, tmp, "input/a.cef")
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath, CaseDir: tmp, TmpDir: tmp,
		GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll",
		ReceiverHostPort: 19001,
	}
	if err := writeCompose(composePath, cfg); err != nil {
		t.Fatalf("writeCompose: %v", err)
	}
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)
	wantHost := filepath.ToSlash(filepath.Join(tmp, "input/a.cef"))
	mustContain(t, out, wantHost+":/input/a.cef:ro")
	mustContain(t, out, "GENERATOR_SAMPLE_FILE: \"/input/a.cef\"")
	mustContain(t, out, "GENERATOR_REWRITE_TIMESTAMP: \"true\"")
	// Per-generator gating: src-b opted out, so exactly one of each appears.
	if n := strings.Count(out, "GENERATOR_SAMPLE_FILE:"); n != 1 {
		t.Errorf("expected exactly 1 GENERATOR_SAMPLE_FILE, got %d:\n%s", n, out)
	}
	if n := strings.Count(out, "GENERATOR_REWRITE_TIMESTAMP:"); n != 1 {
		t.Errorf("expected exactly 1 GENERATOR_REWRITE_TIMESTAMP, got %d:\n%s", n, out)
	}
}

// writeSampleFile creates a non-empty sample file at caseDir/relPath
// (creating parent dirs) so writeCompose's sample-file existence check passes.
func writeSampleFile(t *testing.T, caseDir, relPath string) {
	t.Helper()
	full := filepath.Join(caseDir, relPath)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("sample line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustContain(t *testing.T, hay, needle string) {
	t.Helper()
	if !strings.Contains(hay, needle) {
		t.Errorf("rendered compose missing %q:\n%s", needle, hay)
	}
}

func mustNotContain(t *testing.T, hay, needle string) {
	t.Helper()
	if strings.Contains(hay, needle) {
		t.Errorf("rendered compose unexpectedly contains %q:\n%s", needle, hay)
	}
}
