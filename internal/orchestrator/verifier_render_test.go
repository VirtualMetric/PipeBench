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

// localVerifierCase is a minimal local-file columnar correctness case: TCP in,
// a `file` target (configured in the subject config) writing to the shared
// /data volume, and a verifier: block with local_dir (no aws:/minio:).
func localVerifierCase(format string) *config.TestCase {
	return &config.TestCase{
		Name: "file_target_" + format + "_correctness",
		Type: "correctness",
		Generator: config.GeneratorConfig{
			Mode: "tcp", Target: "subject:9000",
			TotalLines: 1000, LineSize: 256, Format: "json",
		},
		Verifier: &config.VerifierConfig{
			LocalDir: "/data/out", Format: format,
			MsgField: "msg", NullFields: []string{"msg"},
		},
		Subjects: []string{"vmetric"},
	}
}

func TestComposeRendersLocalVerifier(t *testing.T) {
	out, parsed := renderVerifierCompose(t, localVerifierCase("parquet"))
	services := parsed["services"].(map[string]any)

	verifier, ok := services["verifier"].(map[string]any)
	if !ok {
		t.Fatalf("verifier service not rendered:\n%s", out)
	}
	// Local mode: reads the shared volume, no S3 bucket/credentials.
	mustContain(t, out, "VERIFIER_LOCAL_DIR: \"/data/out\"")
	mustContain(t, out, "VERIFIER_OBJECT_FORMAT: \"parquet\"")
	mustNotContain(t, out, "VERIFIER_S3_BUCKET")
	mustNotContain(t, out, "AWS_ENDPOINT_URL")

	// The verifier must mount the shared-data volume to read the file target's
	// output.
	if !hasSharedDataMount(verifier) {
		t.Errorf("verifier missing shared-data:/data mount:\n%s", out)
	}

	// The subject must also mount shared-data so its file target can write there.
	subject := services["subject"].(map[string]any)
	if !hasSharedDataMount(subject) {
		t.Errorf("subject missing shared-data:/data mount for local verifier:\n%s", out)
	}

	// data-init passes the case-controlled dir via an env var and quotes it in
	// the root shell command, so a value with shell metacharacters can't alter
	// the command (the $$ escapes compose interpolation -> a literal $ for sh).
	mustContain(t, out, `command: ["mkdir -p -- \"$${VERIFIER_LOCAL_DIR}\" && chmod -R 0777 /data"]`)
	dataInit := services["data-init"].(map[string]any)
	env := dataInit["environment"].(map[string]any)
	if env["VERIFIER_LOCAL_DIR"] != "/data/out" {
		t.Errorf("data-init VERIFIER_LOCAL_DIR = %v, want /data/out:\n%s", env["VERIFIER_LOCAL_DIR"], out)
	}
}

// hasSharedDataMount reports whether a rendered compose service mounts the
// shared-data named volume at /data.
func hasSharedDataMount(svc map[string]any) bool {
	vols, _ := svc["volumes"].([]any)
	for _, v := range vols {
		if s, _ := v.(string); s == "shared-data:/data" {
			return true
		}
	}
	return false
}

// TestComposeRendersClusteredLocalVerifier guards the cluster + local_dir combo:
// every clustered subject node must wait for data-init and mount shared-data so
// the local verifier doesn't read an empty volume (the singular subject branch
// already does this). Calls writeCompose directly to render per-node services
// without going through Validate (mirrors the cluster failover render test).
func TestComposeRendersClusteredLocalVerifier(t *testing.T) {
	tmp := t.TempDir()
	srcCfg := filepath.Join(tmp, "vmetric.yml")
	if err := os.WriteFile(srcCfg, []byte("director:\n  id: {{@.NodeID@}}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tc := &config.TestCase{
		Name:      "director_cluster_local_verifier_correctness",
		Type:      "director_cluster_correctness",
		Cluster:   &config.ClusterConfig{Nodes: 3, Action: "none"},
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject-1:9000", TotalLines: 1000, LineSize: 256, Format: "json"},
		Verifier:  &config.VerifierConfig{LocalDir: "/data/out", Format: "parquet", MsgField: "msg"},
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"}
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: srcCfg, TmpDir: tmp,
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
	out := string(data)
	var parsed map[string]any
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, out)
	}
	services := parsed["services"].(map[string]any)

	for _, host := range []string{"subject-1", "subject-2", "subject-3"} {
		svc, ok := services[host].(map[string]any)
		if !ok {
			t.Fatalf("clustered node %q not rendered:\n%s", host, out)
		}
		if !hasSharedDataMount(svc) {
			t.Errorf("node %q missing shared-data:/data mount for local verifier:\n%s", host, out)
		}
		deps, _ := svc["depends_on"].(map[string]any)
		if _, ok := deps["data-init"]; !ok {
			t.Errorf("node %q missing data-init dependency for local verifier:\n%s", host, out)
		}
	}
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
