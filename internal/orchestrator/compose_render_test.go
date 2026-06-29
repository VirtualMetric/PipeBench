package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"gopkg.in/yaml.v3"
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
		Version:    "2.0.3",
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
		Version:    "2.0.3",
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
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
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
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
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
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
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
		"reserved name":               {{Name: "collector", Image: "x"}},
		"missing image":               {{Name: "ep1"}},
		"missing name":                {{Image: "x"}},
		"duplicate":                   {{Name: "ep1", Image: "x"}, {Name: "ep1", Image: "y"}},
		"dynamic generator collision": {{Name: "generator-1", Image: "x"}},
		"dynamic generator named id":  {{Name: "generator-xyz", Image: "x"}},
		"dynamic receiver collision":  {{Name: "receiver-42", Image: "x"}},
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
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
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
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
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

// TestComposeRendersKafka verifies a kafka case renders the redpanda broker +
// one-shot redpanda-init, wires the generator to produce to redpanda:9092 with
// the topic/batch env, and gates the generator and subject on redpanda-init
// completing.
func TestComposeRendersKafka(t *testing.T) {
	tc := &config.TestCase{
		Name:     "kafka-smoke",
		Type:     "kafka_performance",
		Duration: "30s",
		Kafka:    &config.KafkaConfig{Topic: "bench", Partitions: 3},
		Generator: config.GeneratorConfig{
			Mode: "kafka", Target: "redpanda:9092", Format: "json", KafkaBatch: 10,
		},
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-kafka-")
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
	// The whole rendered compose (including the new redpanda services) must be
	// valid YAML — guards the template indentation of the kafka block.
	var parsed map[string]any
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, out)
	}
	// Broker + init services.
	mustContain(t, out, "  redpanda:\n")
	mustContain(t, out, "container_name: \"bench-redpanda\"")
	mustContain(t, out, "PLAINTEXT://redpanda:9092") // advertised as the service name, not 127.0.0.1
	mustContain(t, out, "  redpanda-init:\n")
	mustContain(t, out, "rpk topic create bench -p 3")
	mustContain(t, out, "condition: service_completed_successfully")
	// Generator produces to the broker with the topic/batch env.
	mustContain(t, out, "GENERATOR_TARGET: \"redpanda:9092\"")
	mustContain(t, out, "GENERATOR_KAFKA_TOPIC: \"bench\"")
	mustContain(t, out, "GENERATOR_KAFKA_BATCH: \"10\"")
}

// TestComposeOmitsKafkaByDefault guards existing (non-kafka) cases: no redpanda
// service or kafka wiring should appear when the case has no `kafka:` block.
func TestComposeOmitsKafkaByDefault(t *testing.T) {
	tc := &config.TestCase{
		Name:      "plain-tcp",
		Type:      "performance",
		Duration:  "30s",
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject:9000", Rate: 10, LineSize: 64, Format: "raw"},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-nokafka-")
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
	mustNotContain(t, out, "redpanda")
	mustNotContain(t, out, "GENERATOR_KAFKA_TOPIC")
	mustNotContain(t, out, "service_completed_successfully")
}

// TestValidateRejectsBadKafka covers the kafka-type validation rules: the
// kafka block is required and the generator must run in kafka mode.
func TestValidateRejectsBadKafka(t *testing.T) {
	cases := map[string]*config.TestCase{
		"missing kafka block": {
			Name: "x", Type: "kafka_performance",
			Generator: config.GeneratorConfig{Mode: "kafka", Target: "redpanda:9092"},
			Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		},
		"wrong generator mode": {
			Name: "x", Type: "kafka_correctness",
			Kafka:     &config.KafkaConfig{Topic: "bench"},
			Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject:9000"},
			Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		},
	}
	for label, tc := range cases {
		if err := tc.Validate(); err == nil {
			t.Errorf("%s: expected validation error, got nil", label)
		}
	}
}

// TestComposeRendersAWS verifies an `aws:` case renders the LocalStack
// service with the rendered init script mounted, gates subject + cloud
// receiver on its healthcheck, and injects emulator env where needed.
func TestComposeRendersAWS(t *testing.T) {
	tc := &config.TestCase{
		Name:     "aws-smoke",
		Type:     "performance",
		Duration: "30s",
		AWS: &config.AWSConfig{
			Buckets: []string{"bench-in"},
			Queues:  []string{"bench-events"},
			BucketNotifications: []config.AWSBucketNotification{
				{Bucket: "bench-in", Queue: "bench-events"},
			},
		},
		Generator: config.GeneratorConfig{
			Mode: "s3", Target: "http://localstack:4566", Rate: 100, Format: "json",
			Env: map[string]string{"GENERATOR_S3_BUCKET": "bench-in"},
		},
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-aws-")
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
	var parsed map[string]any
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, out)
	}
	mustContain(t, out, "  localstack:\n")
	mustContain(t, out, "container_name: \"bench-localstack\"")
	mustContain(t, out, `SERVICES: "s3,sqs,sts"`)
	mustContain(t, out, "/etc/localstack/init/ready.d/init-bench.sh:ro")
	mustContain(t, out, "condition: service_healthy")
	// Subject and the s3-mode generator both carry the emulator env.
	mustContain(t, out, `AWS_ENDPOINT_URL: "http://localstack:4566"`)
	mustContain(t, out, `AWS_EC2_METADATA_DISABLED: "true"`)
	mustContain(t, out, `GENERATOR_S3_BUCKET: "bench-in"`)
	// The TCP receiver must not gain emulator deps or env.
	mustNotContain(t, out, "azurite")
	// Init script exists and wires the S3→SQS notification.
	script, err := os.ReadFile(filepath.Join(tmp, "aws-init.sh"))
	if err != nil {
		t.Fatalf("aws-init.sh not written: %v", err)
	}
	mustContain(t, string(script), "awslocal s3 mb 's3://bench-in'")
	mustContain(t, string(script), "awslocal sqs create-queue --queue-name 'bench-events'")
	mustContain(t, string(script), `"QueueArn":"arn:aws:sqs:us-east-1:000000000000:bench-events"`)
}

// TestComposeRendersAzure verifies an `azure:` case renders the Azurite
// service plus the one-shot azure-init (receiver image), gates the subject on
// init completion, and injects the connection string where needed.
func TestComposeRendersAzure(t *testing.T) {
	tc := &config.TestCase{
		Name:     "azure-smoke",
		Type:     "performance",
		Duration: "30s",
		Azure:    &config.AzureConfig{Containers: []string{"bench-out"}},
		Generator: config.GeneratorConfig{
			Mode: "tcp", Target: "subject:9000", Rate: 100, Format: "raw",
		},
		Receiver: config.ReceiverConfig{
			Mode: "azure_blob",
			Env:  map[string]string{"RECEIVER_AZURE_CONTAINER": "bench-out"},
		},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-azure-")
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
	var parsed map[string]any
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, out)
	}
	mustContain(t, out, "  azurite:\n")
	mustContain(t, out, "container_name: \"bench-azurite\"")
	mustContain(t, out, "  azure-init:\n")
	mustContain(t, out, `RECEIVER_MODE: "azure_init"`)
	mustContain(t, out, `AZURE_INIT_CONTAINERS: "bench-out"`)
	// azure-init reuses the receiver image.
	mustContain(t, out, `image: "img-recv"`)
	// Subject and the polling receiver gate on init completing.
	mustContain(t, out, "condition: service_completed_successfully")
	mustContain(t, out, "AccountName=devstoreaccount1")
	mustContain(t, out, `RECEIVER_AZURE_CONTAINER: "bench-out"`)
	mustNotContain(t, out, "localstack")
}

// TestComposeOmitsCloudByDefault guards existing cases: no emulator service
// or cloud env appears without an aws:/azure: block.
func TestComposeOmitsCloudByDefault(t *testing.T) {
	tc := &config.TestCase{
		Name:      "plain",
		Type:      "performance",
		Duration:  "30s",
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject:9000", Rate: 10, LineSize: 64, Format: "raw"},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-nocloud-")
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
	mustNotContain(t, out, "localstack")
	mustNotContain(t, out, "azurite")
	mustNotContain(t, out, "AWS_")
	mustNotContain(t, out, "AZURE_")
	if _, err := os.Stat(filepath.Join(tmp, "aws-init.sh")); !os.IsNotExist(err) {
		t.Errorf("aws-init.sh should not be written without an aws: block")
	}
}

// TestComposeRendersAgent verifies that a case with an `agent:` block renders
// an `agent:` compose service that depends_on the subject and carries the
// correct image, env, and command. Also confirms that a case without an
// `agent:` block produces no `agent:` service (opt-in, not default).
func TestComposeRendersAgent(t *testing.T) {
	tc := &config.TestCase{
		Name:     "smoke-agent",
		Type:     "correctness",
		Duration: "60s",
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Agent: &config.AgentConfig{
			Image:   "vmetric/director-enterprise:2.0.0",
			Env:     map[string]string{"VMETRIC_CONFIG_HASH": "dGVzdA=="},
			Command: []string{"sh", "-c", "echo hello && /package/agent/linux/amd64 -agentless"},
		},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director-enterprise", Version: "2.0.0", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-agent-")
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
	mustContain(t, out, "  agent:\n")
	mustContain(t, out, "container_name: \"bench-agent\"")
	mustContain(t, out, "hostname: \"agent\"")
	mustContain(t, out, "image: \"vmetric/director-enterprise:2.0.0\"")
	mustContain(t, out, "VMETRIC_CONFIG_HASH: \"dGVzdA==\"")
	mustContain(t, out, "/package/agent/linux/amd64")
	// The agent depends_on the subject — this is the semantic difference from an endpoint.
	mustContain(t, out, "depends_on:")
	mustContain(t, out, "service_started")
	// No shared-data volume by default.
	// (MountsSharedData was not set in this test case)

	// Without an agent: block, no agent service is rendered.
	tcNoAgent := &config.TestCase{
		Name:      "no-agent",
		Type:      "correctness",
		Duration:  "10s",
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject:9000", Rate: 10, LineSize: 64, Format: "raw"},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	composePath2 := filepath.Join(tmp, "compose2.yaml")
	cfg2 := RunConfig{
		TestCase: tcNoAgent, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath2, TmpDir: tmp,
		GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll",
		ReceiverHostPort: 19001,
	}
	if err := writeCompose(composePath2, cfg2); err != nil {
		t.Fatalf("writeCompose (no-agent): %v", err)
	}
	data2, err := os.ReadFile(composePath2)
	if err != nil {
		t.Fatal(err)
	}
	mustNotContain(t, string(data2), "  agent:\n")
	mustNotContain(t, string(data2), "container_name: \"bench-agent\"")
}

// TestValidateRejectsAgentNameAsEndpoint confirms the reserved name "agent"
// blocks an endpoint from colliding with the agent: service.
func TestValidateRejectsAgentNameAsEndpoint(t *testing.T) {
	tc := &config.TestCase{
		Name:      "bad-agent-ep",
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "x:1"},
		Endpoints: []config.Endpoint{{Name: "agent", Image: "x"}},
	}
	if err := tc.Validate(); err == nil {
		t.Fatal("expected validation error for endpoint named 'agent' (reserved)")
	}
}

// TestClusterIPComposeRendersVIPPlumbing verifies a cluster_ip_failover case
// renders the VIP plumbing: a pinned bench subnet (so the virtual IP is in range
// and won't collide with an auto-assigned container address), NET_ADMIN + root
// user on each director node (so the elected leader can `ip addr add` the VIP),
// one service per node, and a per-node config with director.id injected via the
// harness {{@.NodeID@}} placeholder.
func TestClusterIPComposeRendersVIPPlumbing(t *testing.T) {
	tmp, err := os.MkdirTemp("", "compose-clusterip-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	// Keep the source config OUT of TmpDir (as real cases do: the source lives in
	// the case dir, the bench renders into its own tmpdir) so the singular render
	// doesn't overwrite it. Minimal per-node config carrying the NodeID placeholder.
	srcDir, err := os.MkdirTemp("", "case-src-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(srcDir)
	srcCfg := filepath.Join(srcDir, "vmetric.yml")
	if err := os.WriteFile(srcCfg, []byte("director:\n  id: {{@.NodeID@}}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	tc := &config.TestCase{
		Name: "director_cluster_ip_failover_correctness",
		Type: "director_cluster_correctness",
		Cluster: &config.ClusterConfig{
			Nodes:  3,
			Action: "cluster_ip_failover",
			IP:     "172.30.0.250",
		},
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	subj := config.Subject{
		Name:       "vmetric",
		Image:      "vmetric/director-enterprise",
		Version:    "2.0.6",
		ConfigPath: "/config.yml",
	}
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase:         tc,
		Subject:          subj,
		ConfigName:       "default",
		ConfigSrcPath:    srcCfg,
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

	// Pinned subnet so the VIP is in range and won't collide with container IPs.
	mustContain(t, out, "ipam:")
	mustContain(t, out, "subnet: \"172.30.0.0/16\"")
	// Each node can add the VIP: NET_ADMIN cap + root user (the director image
	// runs as non-root, for whom the cap would not be effective).
	mustContain(t, out, "- NET_ADMIN")
	mustContain(t, out, "user: \"0:0\"")
	// One service per node; the singular subject service is replaced.
	mustContain(t, out, "container_name: \"bench-subject-vmetric-1\"")
	mustContain(t, out, "container_name: \"bench-subject-vmetric-2\"")
	mustContain(t, out, "container_name: \"bench-subject-vmetric-3\"")
	mustNotContain(t, out, "container_name: \"bench-subject-vmetric\"\n")

	// Per-node configs rendered with director.id taken from NodeID.
	for _, n := range []struct{ id, want string }{
		{"1", "id: 1"}, {"2", "id: 2"}, {"3", "id: 3"},
	} {
		b, err := os.ReadFile(filepath.Join(tmp, "cluster-node-"+n.id+".yml"))
		if err != nil {
			t.Fatalf("read node %s config: %v", n.id, err)
		}
		mustContain(t, string(b), n.want)
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
