package orchestrator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"gopkg.in/yaml.v3"
)

// vaultSmokeCase returns a minimal vault-enabled correctness case with two
// secret paths declared out of sorted order (seed ordering is asserted below).
func vaultSmokeCase() *config.TestCase {
	return &config.TestCase{
		Name:     "vault-smoke",
		Type:     "correctness",
		Duration: "10s",
		Vault: &config.VaultConfig{
			Secrets: map[string]map[string]string{
				"bench/http-auth": {"username": "bench-user", "password": "bench-pass"},
				"bench/aux":       {"value": "v1"},
			},
		},
		Generator: config.GeneratorConfig{
			Mode: "http", Target: "http://u:p@subject:9000/", Rate: 10,
			LineSize: 64, Format: "raw", Connections: 1,
		},
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
}

// TestComposeRendersVault verifies a vault-enabled case renders the vault dev
// server + one-shot vault-init seeder, mounts the TLS dir into the subject,
// and gates the subject on seeding completing. Runs PrepareVault for real so
// the host dirs/seed files exercised here are the ones the runner would use.
func TestComposeRendersVault(t *testing.T) {
	tc := vaultSmokeCase()
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.2", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-vault-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	vp, err := PrepareVault(tmp, tc.Vault)
	if err != nil {
		t.Fatalf("PrepareVault: %v", err)
	}
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath, TmpDir: tmp,
		GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll",
		ReceiverHostPort: 19001,
		VaultTLSHost:     vp.TLSDir,
		VaultSecretsHost: vp.SecretsDir,
		VaultSeeds:       vp.Seeds,
	}
	if err := writeCompose(composePath, cfg); err != nil {
		t.Fatalf("writeCompose: %v", err)
	}
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)

	// The rendered compose (including the vault block) must be valid YAML —
	// guards the template indentation.
	var parsed map[string]any
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, out)
	}

	mustContain(t, out, "  vault:\n")
	mustContain(t, out, "container_name: \"bench-vault\"")
	mustContain(t, out, "hostname: \"vault\"")
	mustContain(t, out, "image: \"hashicorp/vault:1.20\"")
	mustContain(t, out, "-dev-tls-san=vault")
	mustContain(t, out, "-dev-listen-address=0.0.0.0:8200")
	mustContain(t, out, "VAULT_DEV_ROOT_TOKEN_ID: \"pipebench-dev-root\"")
	mustContain(t, out, "-tls-skip-verify")

	mustContain(t, out, "  vault-init:\n")
	mustContain(t, out, "container_name: \"bench-vault-init\"")
	mustContain(t, out, "VAULT_ADDR: \"https://vault:8200\"")
	mustContain(t, out, "VAULT_CACERT: \"/vault/tls/vault-ca.pem\"")
	// Seeds render in sorted-path order with per-path JSON files; only paths
	// and file names appear on the command line, never secret values.
	mustContain(t, out, "vault kv put -mount=secret bench/aux @/vault-secrets/0.json; "+
		"vault kv put -mount=secret bench/http-auth @/vault-secrets/1.json")
	mustNotContain(t, out, "bench-pass")

	// Subject gates on seeding and gets the TLS dir read-only at /vault-tls.
	mustContain(t, out, ":/vault-tls:ro")
	subjectSvc := parsed["services"].(map[string]any)["subject"].(map[string]any)
	deps, ok := subjectSvc["depends_on"].(map[string]any)
	if !ok {
		t.Fatalf("subject depends_on missing or wrong shape: %#v", subjectSvc["depends_on"])
	}
	if _, ok := deps["vault-init"]; !ok {
		t.Errorf("subject depends_on lacks vault-init: %#v", deps)
	}
}

// TestComposeOmitsVaultByDefault guards existing cases: no vault service or
// wiring should appear when the case has no `vault:` block.
func TestComposeOmitsVaultByDefault(t *testing.T) {
	tc := &config.TestCase{
		Name:      "plain-tcp",
		Type:      "performance",
		Duration:  "30s",
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject:9000", Rate: 10, LineSize: 64, Format: "raw"},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.2", ConfigPath: "/config.yml"}
	// Note: the tmp dir path is bind-mounted into the rendered compose, so its
	// name must not itself contain the strings asserted absent below.
	tmp, err := os.MkdirTemp("", "compose-plain-")
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
	mustNotContain(t, out, "vault")
	mustNotContain(t, out, "VAULT_")
	mustNotContain(t, out, "/vault-tls")
}

// TestComposeRendersKafkaAndVaultTogether is the regression test for the
// subject depends_on restructure: with both blocks set, the subject must gate
// on BOTH redpanda-init and vault-init and the file must stay valid YAML.
func TestComposeRendersKafkaAndVaultTogether(t *testing.T) {
	tc := vaultSmokeCase()
	tc.Name = "kafka-vault"
	tc.Type = "kafka_correctness"
	tc.Kafka = &config.KafkaConfig{Topic: "bench"}
	tc.Generator = config.GeneratorConfig{Mode: "kafka", Target: "redpanda:9092", Format: "json"}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.2", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-kafka-vault-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	vp, err := PrepareVault(tmp, tc.Vault)
	if err != nil {
		t.Fatalf("PrepareVault: %v", err)
	}
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default",
		ConfigSrcPath: composePath, TmpDir: tmp,
		GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll",
		ReceiverHostPort: 19001,
		VaultTLSHost:     vp.TLSDir,
		VaultSecretsHost: vp.SecretsDir,
		VaultSeeds:       vp.Seeds,
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
		t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, string(data))
	}
	subjectSvc := parsed["services"].(map[string]any)["subject"].(map[string]any)
	deps, ok := subjectSvc["depends_on"].(map[string]any)
	if !ok {
		t.Fatalf("subject depends_on missing or wrong shape: %#v", subjectSvc["depends_on"])
	}
	for _, want := range []string{"redpanda-init", "vault-init"} {
		if _, ok := deps[want]; !ok {
			t.Errorf("subject depends_on lacks %s: %#v", want, deps)
		}
	}
}

// TestWriteComposeRejectsUnpreparedVault verifies writeCompose fails fast when
// a vault case reaches it without PrepareVault having provisioned the host
// dirs (instead of rendering empty bind-mount sources).
func TestWriteComposeRejectsUnpreparedVault(t *testing.T) {
	tc := vaultSmokeCase()
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.2", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "compose-vault-unprep-")
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
	if err := writeCompose(composePath, cfg); err == nil {
		t.Fatal("expected error for vault case without prepared host dirs")
	}
}

// TestValidateRejectsBadVault covers the vault validation rules: secrets are
// mandatory and every compose-embedded string is charset-restricted.
func TestValidateRejectsBadVault(t *testing.T) {
	valid := map[string]map[string]string{"bench/ok": {"k": "v"}}
	cases := map[string]*config.TestCase{
		"empty secrets": {Name: "x", Vault: &config.VaultConfig{}},
		"path with command substitution": {Name: "x", Vault: &config.VaultConfig{
			Secrets: map[string]map[string]string{"bench/$(reboot)": {"k": "v"}}}},
		"path with space": {Name: "x", Vault: &config.VaultConfig{
			Secrets: map[string]map[string]string{"bench/a b": {"k": "v"}}}},
		"path with quote": {Name: "x", Vault: &config.VaultConfig{
			Secrets: map[string]map[string]string{`bench/a"b`: {"k": "v"}}}},
		"empty field map": {Name: "x", Vault: &config.VaultConfig{
			Secrets: map[string]map[string]string{"bench/empty": {}}}},
		"bad field key": {Name: "x", Vault: &config.VaultConfig{
			Secrets: map[string]map[string]string{"bench/ok": {"user name": "v"}}}},
		"bad token": {Name: "x", Vault: &config.VaultConfig{Token: "root$tok", Secrets: valid}},
		"bad mount": {Name: "x", Vault: &config.VaultConfig{Mount: "se cret", Secrets: valid}},
		"endpoint named vault": {Name: "x",
			Endpoints: []config.Endpoint{{Name: "vault", Image: "img"}}},
		"endpoint named vault-init": {Name: "x",
			Endpoints: []config.Endpoint{{Name: "vault-init", Image: "img"}}},
		"endpoint named redpanda": {Name: "x",
			Endpoints: []config.Endpoint{{Name: "redpanda", Image: "img"}}},
	}
	for label, tc := range cases {
		if err := tc.Validate(); err == nil {
			t.Errorf("%s: expected validation error, got nil", label)
		}
	}

	// Sanity: a well-formed vault block passes.
	good := vaultSmokeCase()
	if err := good.Validate(); err != nil {
		t.Errorf("valid vault case rejected: %v", err)
	}
}

// TestPrepareVaultWritesSeeds verifies the host-side provisioning: dir modes
// (the vault container's uid 100 must write the TLS dir; the secrets dir stays
// private), deterministic sorted seed ordering, and JSON round-tripping.
func TestPrepareVaultWritesSeeds(t *testing.T) {
	tmp, err := os.MkdirTemp("", "prepare-vault-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	v := &config.VaultConfig{
		Secrets: map[string]map[string]string{
			"z/last":  {"k": "v2"},
			"a/first": {"user": "u", "pass": "p"},
		},
	}
	vp, err := PrepareVault(tmp, v)
	if err != nil {
		t.Fatalf("PrepareVault: %v", err)
	}

	tlsInfo, err := os.Stat(vp.TLSDir)
	if err != nil {
		t.Fatalf("tls dir: %v", err)
	}
	if got := tlsInfo.Mode().Perm(); got != 0o777 {
		t.Errorf("tls dir mode: got %o, want 777", got)
	}
	secInfo, err := os.Stat(vp.SecretsDir)
	if err != nil {
		t.Fatalf("secrets dir: %v", err)
	}
	// Windows can't represent Unix permission bits on directories — os.Stat
	// always reports 0777 there regardless of the 0700 passed to MkdirAll.
	// The restrictive mode is only meaningful (and asserted) on Unix, which
	// is also where the CI runs.
	if runtime.GOOS != "windows" {
		if got := secInfo.Mode().Perm(); got != 0o700 {
			t.Errorf("secrets dir mode: got %o, want 700", got)
		}
	}

	want := []VaultSeed{{Path: "a/first", File: "0.json"}, {Path: "z/last", File: "1.json"}}
	if len(vp.Seeds) != len(want) {
		t.Fatalf("seeds: got %v, want %v", vp.Seeds, want)
	}
	for i, w := range want {
		if vp.Seeds[i] != w {
			t.Errorf("seed %d: got %+v, want %+v", i, vp.Seeds[i], w)
		}
		data, err := os.ReadFile(filepath.Join(vp.SecretsDir, w.File))
		if err != nil {
			t.Fatalf("reading seed %s: %v", w.File, err)
		}
		var fields map[string]string
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatalf("seed %s is not valid JSON: %v", w.File, err)
		}
		if len(fields) != len(v.Secrets[w.Path]) {
			t.Errorf("seed %s fields: got %v, want %v", w.File, fields, v.Secrets[w.Path])
		}
		for k, val := range v.Secrets[w.Path] {
			if fields[k] != val {
				t.Errorf("seed %s field %q: got %q, want %q", w.File, k, fields[k], val)
			}
		}
	}
}
