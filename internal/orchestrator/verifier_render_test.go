package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"gopkg.in/yaml.v3"
)

// verifierSmokeCase is a minimal S3 columnar correctness case: TCP in, an
// awss3 sink (configured in the subject config, not here), an aws: emulator,
// and a verifier: block that drives correctness.
func verifierSmokeCase(format string) *config.TestCase {
	return &config.TestCase{
		Name: "tcp_to_s3_" + format + "_correctness",
		Type: "correctness",
		Generator: config.GeneratorConfig{
			Mode: "tcp", Target: "subject:9000",
			TotalLines: 100000, LineSize: 256, Format: "json",
		},
		AWS: &config.AWSConfig{Buckets: []string{"bench-out"}},
		Verifier: &config.VerifierConfig{
			S3Bucket: "bench-out", Format: format,
			MsgField: "msg", NullFields: []string{"msg"},
		},
		Requires: []string{"s3_" + format + "_sink"},
		Subjects: []string{"vmetric"},
	}
}

func renderVerifierCompose(t *testing.T, tc *config.TestCase) (string, map[string]any) {
	t.Helper()
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
	tmp := t.TempDir()
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath, TmpDir: tmp,
		GeneratorImage: "img-gen", ReceiverImage: "img-recv",
		CollectorImage: "img-coll", VerifierImage: "img-verifier",
		ReceiverHostPort: 19001,
	}
	if err := writeCompose(composePath, cfg); err != nil {
		t.Fatalf("writeCompose: %v", err)
	}
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, data)
	}
	return string(data), parsed
}

func TestComposeRendersVerifier(t *testing.T) {
	out, parsed := renderVerifierCompose(t, verifierSmokeCase("parquet"))
	services := parsed["services"].(map[string]any)

	// Verifier service present and configured.
	if _, ok := services["verifier"]; !ok {
		t.Fatalf("verifier service not rendered:\n%s", out)
	}
	mustContain(t, out, "container_name: \"bench-verifier\"")
	mustContain(t, out, "image: \"img-verifier\"")
	mustContain(t, out, "profiles: [\"verify\"]")
	mustContain(t, out, "VERIFIER_S3_BUCKET: \"bench-out\"")
	mustContain(t, out, "VERIFIER_OBJECT_FORMAT: \"parquet\"")
	mustContain(t, out, "VERIFIER_EXPECTED_LINES: \"100000\"")
	mustContain(t, out, "VERIFIER_MSG_FIELD: \"msg\"")
	mustContain(t, out, "VERIFIER_NULL_FIELDS: \"msg\"")
	// S3 access env injected from the aws: block.
	mustContain(t, out, "AWS_ENDPOINT_URL: \"http://localstack:4566\"")

	// The receiver is NOT rendered for verifier cases.
	if _, ok := services["receiver"]; ok {
		t.Errorf("receiver service should be omitted for verifier cases:\n%s", out)
	}
	mustNotContain(t, out, "container_name: \"bench-receiver\"")

	// The generator must not depend on a (nonexistent) receiver.
	gen := services["generator"].(map[string]any)
	if deps, ok := gen["depends_on"].(map[string]any); ok {
		if _, bad := deps["receiver"]; bad {
			t.Errorf("generator depends_on references receiver in a verifier case: %#v", deps)
		}
	}
}

func TestComposeRendersVerifierAvro(t *testing.T) {
	out, _ := renderVerifierCompose(t, verifierSmokeCase("avro"))
	mustContain(t, out, "VERIFIER_OBJECT_FORMAT: \"avro\"")
}

// TestComposeOmitsVerifierByDefault guards existing cases: no verifier service
// appears when the case has no verifier: block, and the receiver still renders.
func TestComposeOmitsVerifierByDefault(t *testing.T) {
	tc := &config.TestCase{
		Name:      "tcp_to_tcp",
		Type:      "correctness",
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject:9000"},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Subjects:  []string{"vmetric"},
	}
	out, parsed := renderVerifierCompose(t, tc)
	services := parsed["services"].(map[string]any)
	if _, ok := services["verifier"]; ok {
		t.Errorf("verifier service rendered without a verifier: block:\n%s", out)
	}
	if _, ok := services["receiver"]; !ok {
		t.Errorf("receiver service missing for a non-verifier case:\n%s", out)
	}
}
