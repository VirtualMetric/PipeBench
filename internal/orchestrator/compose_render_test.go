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

// TestComposeRendersEndpoints verifies a case's `endpoints:` list renders as
// extra services on the bench network — name → service/container/hostname —
// with optional env and command, alongside the usual subject/generator/receiver.
func TestComposeRendersEndpoints(t *testing.T) {
	tc := &config.TestCase{
		Name:      "smoke-endpoint",
		Type:      "correctness",
		Duration:  "30s",
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject:9000", Rate: 10, LineSize: 64, Format: "raw"},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Endpoints: []config.Endpoint{
			{
				Name:    "linuxcontainer",
				Image:   "vmetric/bench-sshd-endpoint:latest",
				Env:     map[string]string{"SEED_INTERVAL": "5"},
				Command: []string{"/usr/sbin/sshd", "-D", "-e"},
			},
		},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.1", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-endpoint-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath, TmpDir: tmp,
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
	mustContain(t, out, "  linuxcontainer:\n")
	mustContain(t, out, "container_name: \"bench-linuxcontainer\"")
	mustContain(t, out, "hostname: \"linuxcontainer\"")
	mustContain(t, out, "image: \"vmetric/bench-sshd-endpoint:latest\"")
	mustContain(t, out, "SEED_INTERVAL: \"5\"")
	mustContain(t, out, "command: [\"/usr/sbin/sshd\", \"-D\", \"-e\"]")
	// Endpoint is a peer, not a generator/receiver alias.
	mustNotContain(t, out, "container_name: \"bench-generator-linuxcontainer\"")
}

// TestValidateRejectsBadEndpoints covers the endpoint validation rules:
// required name+image, no reserved names, no duplicates.
func TestValidateRejectsBadEndpoints(t *testing.T) {
	bad := map[string][]config.Endpoint{
		"reserved name": {{Name: "collector", Image: "x"}},
		"missing image": {{Name: "ep1"}},
		"missing name":  {{Image: "x"}},
		"duplicate":     {{Name: "ep1", Image: "x"}, {Name: "ep1", Image: "y"}},
	}
	for label, eps := range bad {
		tc := &config.TestCase{Name: "bad", Endpoints: eps}
		if err := tc.Validate(); err == nil {
			t.Errorf("%s: expected validation error, got nil", label)
		}
	}
}

// TestComposeOmitsGeneratorWhenNone verifies that a case with endpoints and no
// generator renders no generator service at all — subject + receiver +
// collector + endpoint only. This is the topology agentless-deploy cases use.
func TestComposeOmitsGeneratorWhenNone(t *testing.T) {
	tc := &config.TestCase{
		Name:     "no-gen",
		Type:     "correctness",
		Duration: "60s",
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Endpoints: []config.Endpoint{
			{Name: "linuxcontainer", Image: "alpine:3.22", Command: []string{"sh", "-c", "sshd -D -e"}},
		},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if tc.HasGenerator() {
		t.Fatal("HasGenerator() should be false with no generator config")
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.1", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-nogen-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath, TmpDir: tmp,
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
	// No generator service of any form.
	mustNotContain(t, out, "  generator:\n")
	mustNotContain(t, out, "bench-generator")
	mustNotContain(t, out, "GENERATOR_MODE")
	// Endpoint, receiver, and collector are present.
	mustContain(t, out, "  linuxcontainer:\n")
	mustContain(t, out, "  receiver:\n")
	mustContain(t, out, "  collector:\n")
}

// TestEndpointCommandDollarEscaping verifies "$" in an endpoint command/env is
// escaped to "$$" so docker-compose interpolation passes a literal "$" to the
// container shell (otherwise $(date)/${VAR} would be eaten by compose).
func TestEndpointCommandDollarEscaping(t *testing.T) {
	tc := &config.TestCase{
		Name:     "ep-esc",
		Type:     "correctness",
		Duration: "10s",
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Endpoints: []config.Endpoint{{
			Name:    "ep",
			Image:   "alpine:3.22",
			Env:     map[string]string{"FOO": "a$b"},
			Command: []string{"sh", "-c", "echo $(date) ${FOO}"},
		}},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.1", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-esc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath, TmpDir: tmp,
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
	mustContain(t, out, `echo $$(date) $${FOO}`)
	mustContain(t, out, `FOO: "a$$b"`)
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
