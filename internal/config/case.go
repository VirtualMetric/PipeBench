package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// TestCase represents a single test case loaded from case.yaml.
type TestCase struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"` // "performance" | "correctness"
	Description string `yaml:"description"`
	Duration    string `yaml:"duration"`
	Warmup      string `yaml:"warmup"`
	DrainGrace  string `yaml:"drain_grace"`

	// Singular generator/receiver: the original, single-source/single-sink
	// schema. These remain the canonical form for the case files in this
	// repo. Mutually exclusive with the plural forms (Generators/Receivers).
	Generator GeneratorConfig `yaml:"generator"`
	Receiver  ReceiverConfig  `yaml:"receiver"`

	// Plural generator/receiver lists (additive, opt-in). When set, the
	// harness fans out into N generator-<id> / receiver-<id> containers in
	// docker-compose. Setting both the singular and plural form is a parse
	// error — see Validate(). These hooks exist so future cases can model
	// multi-source/multi-sink topologies (dual-input listeners,
	// fan-out routing, load-balanced sinks) without changing the singular
	// path that every current case uses.
	Generators []GeneratorConfig `yaml:"generators"`
	Receivers  []ReceiverConfig  `yaml:"receivers"`

	// Endpoints are auxiliary containers added to the test topology — hosts the
	// subject connects to or acts on (e.g. an SSH target the director deploys an
	// agent onto), as opposed to a generator (input) or receiver (output). Each
	// becomes its own service on the bench network; Name doubles as the compose
	// service name, container hostname, and network alias, so the subject config
	// can reach the endpoint by Name. A case may run with no generator when it
	// drives data purely through endpoints.
	Endpoints []Endpoint `yaml:"endpoints"`

	// Kafka, when set, adds a Redpanda (Kafka-compatible) broker to the test
	// topology: the harness renders a `redpanda` service plus a one-shot
	// `redpanda-init` that creates the topic, and the generator (mode: kafka)
	// produces to it while the subject consumes from it. Used by the
	// kafka_performance / kafka_correctness types.
	Kafka *KafkaConfig `yaml:"kafka"`

	// PipelineBroker, when set, adds a SECOND, dedicated Redpanda broker
	// (hostname "pipeline-broker") for the SUBJECT's own internal transport —
	// e.g. VirtualMetric's `pipeline_bus: {type: kafka}`. It is independent of
	// the `kafka:` block above (which feeds the generator / subject device):
	// different hostname and port, so a case may use both at once with no
	// collision. The subject depends_on it being healthy before starting, so the
	// subject's broker dial can't race startup. Topics auto-create on first
	// produce (the bus owns its topics). Point the subject config at
	// "pipeline-broker:<port>" (default 19092).
	PipelineBroker *PipelineBrokerConfig `yaml:"pipeline_broker"`

	// Vault, when set, adds a HashiCorp Vault dev-mode server (TLS-enabled)
	// to the test topology: the harness renders a `vault` service plus a
	// one-shot `vault-init` that seeds the declared secrets, and the subject
	// gates on the seeding completing. Lets a case exercise a subject's
	// secret-store integration (e.g. vmetric's hashicorpvault credential
	// provider) without real infrastructure.
	Vault *VaultConfig `yaml:"vault"`

	// AWS, when set, adds a LocalStack emulator to the test topology and
	// creates the declared S3/SQS/SNS/Kinesis/CloudWatch resources before
	// the subject starts. See AWSConfig in cloud.go.
	AWS *AWSConfig `yaml:"aws"`

	// Azure, when set, adds an Azurite (Azure Storage emulator) to the test
	// topology plus a one-shot azure-init that creates the declared blob
	// containers. See AzureConfig in cloud.go.
	Azure *AzureConfig `yaml:"azure"`

	// Minio, when set, adds a MinIO (multi-core S3-compatible) emulator to
	// the test topology plus a one-shot minio-init that creates the declared
	// buckets. Mutually exclusive with AWS. See MinioConfig in cloud.go.
	Minio *MinioConfig `yaml:"minio"`

	// Requires lists subject capabilities every subject in this case must
	// declare (Subject.Capabilities); the runner fails fast on subjects
	// lacking one instead of starting a run that silently produces zero
	// ingest. Generalizes the original hardcoded tls_tcp guard.
	Requires []string `yaml:"requires"`

	Subjects       []string                 `yaml:"subjects"`
	Configurations map[string]Configuration `yaml:"configurations"`
	Correctness    CorrectnessConfig        `yaml:"correctness"`

	// Verifier, when set, replaces the receiver with a one-shot DuckDB
	// verifier container that reads the subject's Avro/Parquet objects
	// directly from the S3 emulator (httpfs) and emits a correctness verdict.
	// Used by the tcp_to_s3_{avro,parquet}_correctness cases — the receiver
	// can't decode columnar objects, and the s3 receiver's destructive drain
	// would corrupt the read. See VerifierConfig.
	Verifier *VerifierConfig `yaml:"verifier"`

	// Rotation, when set, parameterizes the
	// director_agent_tls_cert_rotation_correctness driver: which director TLS
	// cert/CA rotation to perform mid-run and how the enrolled agent is expected
	// to respond. Required by (and only meaningful for) that type. See
	// RotationConfig and runDirectorAgentCertRotation.
	Rotation *RotationConfig `yaml:"rotation"`
}

// VerifierConfig configures the post-drain DuckDB verifier container (see
// TestCase.Verifier). The verifier owns drain detection (it polls the bucket
// until the row count settles), then asserts exact row count, no duplicates,
// and no NULL payload fields over the columnar data.
type VerifierConfig struct {
	// S3Bucket is the bucket the subject wrote to (required). S3Prefix is an
	// optional key prefix to scope the scan to.
	S3Bucket string `yaml:"s3_bucket"`
	S3Prefix string `yaml:"s3_prefix"`

	// Format is the object format to read: "avro" | "parquet" (required).
	Format string `yaml:"format"`

	// MsgField is the column whose value is unique per source record, used for
	// the duplicate check (defaults to "msg" — the generator's json payload).
	// NullFields are columns that must be non-NULL on every row (defaults to
	// MsgField).
	MsgField   string   `yaml:"msg_field"`
	NullFields []string `yaml:"null_fields"`

	// AllowOverDelivery tolerates duplicate rows (at-least-once delivery);
	// loss is still a failure.
	AllowOverDelivery bool `yaml:"allow_overdelivery"`

	// QuietWindow is how long the bucket row count must hold steady before the
	// verifier treats the run as drained (default 15s). Timeout bounds the
	// whole verify (default 5m).
	QuietWindow string `yaml:"quiet_window"`
	Timeout     string `yaml:"timeout"`
}

// QuietWindowOrDefault returns the configured quiet window or 15s.
func (v *VerifierConfig) QuietWindowOrDefault() string {
	if v != nil && v.QuietWindow != "" {
		return v.QuietWindow
	}
	return "15s"
}

// TimeoutOrDefault returns the configured verifier timeout or 5m.
func (v *VerifierConfig) TimeoutOrDefault() string {
	if v != nil && v.Timeout != "" {
		return v.Timeout
	}
	return "5m"
}

// TimeoutDuration parses TimeoutOrDefault, falling back to 5m on a parse error.
func (v *VerifierConfig) TimeoutDuration() time.Duration {
	d, err := time.ParseDuration(v.TimeoutOrDefault())
	if err != nil {
		return 5 * time.Minute
	}
	return d
}

// KafkaConfig configures the in-topology Redpanda broker (see TestCase.Kafka).
// All fields are optional; the orchestrator applies the defaults noted below.
type KafkaConfig struct {
	// Image is the Redpanda container image (default
	// "redpandadata/redpanda:latest").
	Image string `yaml:"image"`
	// Topic is the Kafka topic the generator produces to and the subject
	// consumes from (default "bench"). It doubles as the value the generator
	// gets via GENERATOR_KAFKA_TOPIC.
	Topic string `yaml:"topic"`
	// Partitions is the topic partition count created by redpanda-init
	// (default 1).
	Partitions int `yaml:"partitions"`
	// Memory is the Redpanda --memory allotment (default "1G").
	Memory string `yaml:"memory"`
	// SMP is the Redpanda --smp core count (default 1).
	SMP int `yaml:"smp"`
	// Auth, when set, makes the broker require authentication. The zero value
	// (no `auth:` block) is the original PLAINTEXT, no-auth broker.
	Auth *KafkaAuth `yaml:"auth"`
}

// KafkaAuth configures broker authentication for the in-topology Redpanda
// broker (see KafkaConfig.Auth). It maps a case onto one of the supported
// SASL/TLS combinations:
//
//	mechanism=plain|scram-* , tls=none    → SASL over PLAINTEXT (Redpanda)
//	mechanism=plain|scram-* , tls=server  → SASL over TLS (SASL_SSL, Redpanda)
//	mechanism="",            tls=mutual   → client-certificate auth (mTLS, Redpanda)
//	mechanism=gssapi                      → Kerberos (Apache Kafka + KDC, see Realm)
type KafkaAuth struct {
	// Mechanism is the auth mechanism the broker requires and clients use:
	// "" (none), "plain", "scram-sha-256", "scram-sha-512", or "gssapi"
	// (Kerberos). Empty selects no SASL — only meaningful together with
	// tls: mutual (client-cert auth). "gssapi" switches the broker to Apache
	// Kafka + a KDC (Redpanda gates Kerberos behind an enterprise license).
	Mechanism string `yaml:"mechanism"`
	// TLS selects the broker's wire encryption / client-cert policy:
	// "" or "none" = PLAINTEXT, "server" = server-only TLS (SASL_SSL when a
	// mechanism is set), "mutual" = TLS requiring a client certificate.
	// Not used with gssapi (the Kerberos case is SASL_PLAINTEXT).
	TLS string `yaml:"tls"`
	// Realm is the Kerberos realm for the gssapi mechanism (default
	// "PIPEBENCH.LOCAL"). Ignored by the other mechanisms.
	Realm string `yaml:"realm"`
	// ServiceName is the Kerberos service name half of the broker SPN
	// (service/host@REALM; default "kafka"). Ignored by the other mechanisms.
	ServiceName string `yaml:"service_name"`
}

// SASLMechanism returns the lowercased SASL mechanism, or "" when the broker
// uses no SASL (mTLS-only or no auth).
func (k *KafkaConfig) SASLMechanism() string {
	if k != nil && k.Auth != nil {
		return strings.ToLower(strings.TrimSpace(k.Auth.Mechanism))
	}
	return ""
}

// TLSMode returns "none" | "server" | "mutual" — the broker's TLS policy.
func (k *KafkaConfig) TLSMode() string {
	if k != nil && k.Auth != nil {
		if m := strings.ToLower(strings.TrimSpace(k.Auth.TLS)); m != "" {
			return m
		}
	}
	return "none"
}

// UsesGSSAPI reports whether the case uses Kerberos — which the orchestrator
// serves with Apache Kafka + a KDC, NOT the Redpanda SASL/TLS path below.
func (k *KafkaConfig) UsesGSSAPI() bool { return k.SASLMechanism() == "gssapi" }

// AuthEnabled reports whether the case requested Redpanda broker auth (SCRAM/
// PLAIN SASL and/or TLS). It is false for gssapi (handled by the Apache Kafka
// path) and for no auth (the original PLAINTEXT broker).
func (k *KafkaConfig) AuthEnabled() bool {
	return k != nil && k.Auth != nil && (k.UsesSASL() || k.UsesTLS())
}

// UsesSASL / UsesTLS / RequireClientAuth split AuthEnabled into the knobs the
// orchestrator wires onto the Redpanda broker and clients. UsesSASL covers
// PLAIN/SCRAM only — gssapi is a distinct broker path (UsesGSSAPI).
func (k *KafkaConfig) UsesSASL() bool {
	m := k.SASLMechanism()
	return m != "" && m != "gssapi"
}
func (k *KafkaConfig) UsesTLS() bool           { return k.TLSMode() != "none" }
func (k *KafkaConfig) RequireClientAuth() bool { return k.TLSMode() == "mutual" }

// RealmOrDefault / ServiceNameOrDefault centralize the Kerberos defaults.
func (k *KafkaConfig) RealmOrDefault() string {
	if k != nil && k.Auth != nil && strings.TrimSpace(k.Auth.Realm) != "" {
		return strings.TrimSpace(k.Auth.Realm)
	}
	return "PIPEBENCH.LOCAL"
}

func (k *KafkaConfig) ServiceNameOrDefault() string {
	if k != nil && k.Auth != nil && strings.TrimSpace(k.Auth.ServiceName) != "" {
		return strings.TrimSpace(k.Auth.ServiceName)
	}
	return "kafka"
}

// KafkaImageOrDefault etc. centralize the broker defaults so the orchestrator
// and any caller render the same values.
func (k *KafkaConfig) KafkaImageOrDefault() string {
	if k != nil && k.Image != "" {
		return k.Image
	}
	return "redpandadata/redpanda:latest"
}

func (k *KafkaConfig) TopicOrDefault() string {
	if k != nil && k.Topic != "" {
		return k.Topic
	}
	return "bench"
}

func (k *KafkaConfig) PartitionsOrDefault() int {
	if k != nil && k.Partitions > 0 {
		return k.Partitions
	}
	return 1
}

func (k *KafkaConfig) MemoryOrDefault() string {
	if k != nil && k.Memory != "" {
		return k.Memory
	}
	return "1G"
}

func (k *KafkaConfig) SMPOrDefault() int {
	if k != nil && k.SMP > 0 {
		return k.SMP
	}
	return 1
}

// PipelineBrokerConfig configures the dedicated Redpanda broker the harness
// stands up for the subject's internal pipeline bus (see TestCase.PipelineBroker).
// All fields are optional; the orchestrator applies the defaults noted below.
type PipelineBrokerConfig struct {
	// Image is the Redpanda container image (default "redpandadata/redpanda:latest").
	Image string `yaml:"image"`
	// Memory is the Redpanda --memory allotment (default "1G").
	Memory string `yaml:"memory"`
	// SMP is the Redpanda --smp core count (default 1).
	SMP int `yaml:"smp"`
	// Port is the broker's advertised Kafka API port (default 19092). Kept
	// distinct from the `kafka:` broker's 9092 so both can run side by side.
	Port int `yaml:"port"`
	// AutoCreate controls broker-side topic auto-creation. Unset/true is the
	// Redpanda/Kafka convenience default; set false to mirror brokers that
	// disallow it (e.g. Azure Event Hubs), in which case Topics MUST list the
	// pipeline-bus topics to pre-create.
	AutoCreate *bool `yaml:"auto_create"`
	// Topics are pre-created by a one-shot (pipeline-broker-init) before the
	// subject starts. Required when AutoCreate is false; the subject then
	// depends_on that init completing rather than just the broker being healthy.
	Topics []string `yaml:"topics"`
}

// AutoCreateOrDefault reports whether broker-side topic auto-creation is on
// (default true).
func (p *PipelineBrokerConfig) AutoCreateOrDefault() bool {
	if p != nil && p.AutoCreate != nil {
		return *p.AutoCreate
	}
	return true
}

func (p *PipelineBrokerConfig) ImageOrDefault() string {
	if p != nil && p.Image != "" {
		return p.Image
	}
	return "redpandadata/redpanda:latest"
}

func (p *PipelineBrokerConfig) MemoryOrDefault() string {
	if p != nil && p.Memory != "" {
		return p.Memory
	}
	return "1G"
}

func (p *PipelineBrokerConfig) SMPOrDefault() int {
	if p != nil && p.SMP > 0 {
		return p.SMP
	}
	return 1
}

func (p *PipelineBrokerConfig) PortOrDefault() int {
	if p != nil && p.Port > 0 {
		return p.Port
	}
	return 19092
}

// VaultConfig configures the in-topology HashiCorp Vault dev server (see
// TestCase.Vault). All fields except Secrets are optional; the orchestrator
// applies the defaults noted below. The dev server listens TLS-only at
// https://vault:8200 with a self-generated CA the harness bind-mounts into
// the subject at /vault-tls (vault-ca.pem).
type VaultConfig struct {
	// Image is the Vault container image (default "hashicorp/vault:1.20").
	// Needs >= 1.12 for -dev-tls and `vault kv put -mount=...`.
	Image string `yaml:"image"`
	// Token is the deterministic dev-mode root token, fixed via
	// VAULT_DEV_ROOT_TOKEN_ID (default "pipebench-dev-root"). Test-only —
	// the bench Vault never holds real secrets.
	Token string `yaml:"token"`
	// Mount is the KV mount the secrets are seeded under (default "secret",
	// the mount dev mode auto-enables as KV v2).
	Mount string `yaml:"mount"`
	// Secrets maps secret path -> field name -> value. vault-init seeds each
	// path via `vault kv put -mount=<mount> <path> @<file>`; the values are
	// written to per-run JSON files, never onto a command line.
	Secrets map[string]map[string]string `yaml:"secrets"`
}

// VaultImageOrDefault etc. centralize the dev-server defaults so the
// orchestrator and any caller render the same values.
func (v *VaultConfig) VaultImageOrDefault() string {
	if v != nil && v.Image != "" {
		return v.Image
	}
	return "hashicorp/vault:1.20"
}

func (v *VaultConfig) TokenOrDefault() string {
	if v != nil && v.Token != "" {
		return v.Token
	}
	return "pipebench-dev-root"
}

func (v *VaultConfig) MountOrDefault() string {
	if v != nil && v.Mount != "" {
		return v.Mount
	}
	return "secret"
}

// Rotation parameterizes the director_agent_tls_cert_rotation_correctness
// driver (see TestCase.Rotation, runDirectorAgentCertRotation). The director
// deploys an agent that streams back over the proxy_tls listener; mid-run the
// director's serving cert/CA is rotated on disk and the director is bounced so
// the enrolled agent must re-handshake. Mode selects which rotation and the
// expected agent response.
type RotationConfig struct {
	// Mode selects the mid-run rotation:
	//   "same_ca"        — re-sign the director leaf under the SAME CA
	//                      (RotateServerCert). The agent reconnects transparently
	//                      because the chain still validates. Verdict: delivery
	//                      resumes (count grows after the bounce).
	//   "new_ca_recover" — rotate to a BRAND-NEW CA written to ca.crt and served
	//                      at /dl/cert.pem (RotateServerCertNewCA). A bootstrap
	//                      agent (no operator-pinned CA) must re-fetch the new CA
	//                      and reconnect. Verdict: delivery resumes.
	//   "new_ca_reject"  — TWO PHASE. Phase 1 re-signs the leaf under an UNTRUSTED
	//                      CA the director never serves (RotateServerCertWrongCA):
	//                      the agent MUST fail validation, so delivery STALLS
	//                      (a missing stall is a SECURITY failure — validation is
	//                      disabled). Phase 2 restores a trusted leaf
	//                      (RotateServerCert) and delivery must resume.
	Mode string `yaml:"mode"`

	// SettleSeconds is the pause after a rotation+bounce before the driver samples
	// the receiver, giving the director time to rebind its listener and the agent
	// time to detect the dropped session and reconnect (default 25s).
	SettleSeconds int `yaml:"settle_seconds"`

	// StallSeconds (new_ca_reject only) is how long the receiver count must hold
	// flat after the untrusted rotation for the bad cert to count as rejected
	// (default 20s). The case's endpoint seed loop MUST still be appending fresh
	// records during this window, else the stall is vacuous — see the case NOTES.
	StallSeconds int `yaml:"stall_seconds"`
}

// Rotation mode values for RotationConfig.Mode.
const (
	RotationSameCA       = "same_ca"
	RotationNewCARecover = "new_ca_recover"
	RotationNewCAReject  = "new_ca_reject"
)

// SettleSecondsOrDefault / StallSecondsOrDefault centralize the rotation timing
// defaults so the driver and any caller agree.
func (rc *RotationConfig) SettleSecondsOrDefault() int {
	if rc != nil && rc.SettleSeconds > 0 {
		return rc.SettleSeconds
	}
	return 25
}

func (rc *RotationConfig) StallSecondsOrDefault() int {
	if rc != nil && rc.StallSeconds > 0 {
		return rc.StallSeconds
	}
	return 20
}

// Endpoint is an auxiliary container in the test topology (see
// TestCase.Endpoints). It's a host the subject reaches on the bench network —
// not a generator or receiver.
type Endpoint struct {
	// Name is the compose service name, container hostname, and bench-network
	// alias. The subject config references the endpoint by this name (e.g. a
	// device address). Must be unique and must not collide with the reserved
	// service names (subject, generator, receiver, collector).
	Name string `yaml:"name"`
	// Image is the container image to run.
	Image string `yaml:"image"`
	// Env is extra environment passed to the container (optional).
	Env map[string]string `yaml:"env"`
	// Command overrides the image's default command (optional). Write it as
	// normal shell — "$" (e.g. $(date), ${VAR}) is passed literally to the
	// container; the harness escapes it so docker-compose interpolation leaves
	// it alone.
	Command []string `yaml:"command"`
}

// DurationOrDefault parses the Duration field, returning defaultVal on empty/error.
func (tc *TestCase) DurationOrDefault(defaultVal time.Duration) time.Duration {
	if tc.Duration == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(tc.Duration)
	if err != nil {
		return defaultVal
	}
	return d
}

// WarmupOrDefault parses the Warmup field, returning defaultVal on empty/error.
func (tc *TestCase) WarmupOrDefault(defaultVal time.Duration) time.Duration {
	if tc.Warmup == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(tc.Warmup)
	if err != nil {
		return defaultVal
	}
	return d
}

// DrainGraceOrDefault parses DrainGrace, returning defaultVal on empty/error.
// Performance tests use this as the fixed post-generator receive budget.
func (tc *TestCase) DrainGraceOrDefault(defaultVal time.Duration) time.Duration {
	if tc.DrainGrace == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(tc.DrainGrace)
	if err != nil {
		return defaultVal
	}
	return d
}

// ConfigNames returns the list of configuration names, defaulting to ["default"].
func (tc *TestCase) ConfigNames() []string {
	if len(tc.Configurations) == 0 {
		return []string{"default"}
	}
	names := make([]string, 0, len(tc.Configurations))
	for k := range tc.Configurations {
		names = append(names, k)
	}
	return names
}

// ConfigFilePath resolves the path to a subject config file for a given configuration.
// It checks for multi-config layout first (configs/<cfgName>/<subject>.<ext>),
// then falls back to flat layout (configs/<subject>.<ext>).
func (tc *TestCase) ConfigFilePath(casesDir, configName string, s Subject) (string, error) {
	caseDir := filepath.Join(casesDir, tc.Name)

	// Try multi-config layout
	multiPath := filepath.Join(caseDir, "configs", configName, s.ConfigFile())
	if _, err := os.Stat(multiPath); err == nil {
		return multiPath, nil
	}

	// Fall back to flat layout (only valid for "default")
	flatPath := filepath.Join(caseDir, "configs", s.ConfigFile())
	if _, err := os.Stat(flatPath); err == nil {
		return flatPath, nil
	}

	// Try subject name as directory (for subjects needing multiple config files per case)
	dirPath := filepath.Join(caseDir, "configs", s.Name)
	if info, err := os.Stat(dirPath); err == nil && info.IsDir() {
		return dirPath, nil
	}

	return "", fmt.Errorf("config file not found for subject %q in case %q (config %q)", s.Name, tc.Name, configName)
}

// AllGenerators returns the effective list of generator configs for this case.
// If the singular `generator:` form is set (the default for every current
// case in this repo) it returns a one-element slice with that generator;
// otherwise it returns the plural `generators:` list. The element returned
// from the singular form has its ID left blank — callers that need a stable
// per-generator hostname should default a blank ID to "default".
func (tc *TestCase) AllGenerators() []GeneratorConfig {
	if len(tc.Generators) > 0 {
		return tc.Generators
	}
	return []GeneratorConfig{tc.Generator}
}

// AllReceivers returns the effective list of receiver configs for this case,
// with the same singular/plural rules as AllGenerators.
func (tc *TestCase) AllReceivers() []ReceiverConfig {
	if len(tc.Receivers) > 0 {
		return tc.Receivers
	}
	return []ReceiverConfig{tc.Receiver}
}

// MultiGenerator reports whether the case uses the plural `generators:` form.
// The singular form keeps the original single-container compose plumbing;
// the plural form spins up one generator-<id> container per entry.
func (tc *TestCase) MultiGenerator() bool { return len(tc.Generators) > 0 }

// MultiReceiver reports whether the case uses the plural `receivers:` form.
func (tc *TestCase) MultiReceiver() bool { return len(tc.Receivers) > 0 }

// HasGenerator reports whether the case configures any traffic generator —
// either the singular `generator:` or the plural `generators:` form. A case
// with neither drives data through the subject another way (e.g. an endpoint
// the subject collects from); the harness then renders no generator service
// and does not gate the run on a generator exiting.
func (tc *TestCase) HasGenerator() bool {
	return tc.MultiGenerator() || tc.Generator.Mode != "" || tc.Generator.Target != ""
}

// UsesKafka reports whether the case adds a Redpanda broker to the topology.
func (tc *TestCase) UsesKafka() bool { return tc.Kafka != nil }

// UsesVault reports whether the case adds a Vault dev server to the topology.
func (tc *TestCase) UsesVault() bool { return tc.Vault != nil }

// UsesVerifier reports whether the case drives correctness through the one-shot
// DuckDB verifier container instead of a receiver.
func (tc *TestCase) UsesVerifier() bool { return tc.Verifier != nil }

// IsDirectorAgentRotationType reports whether the case is the director↔agent
// TLS cert-rotation correctness flow, which has its own subject-driven (no
// generator) driver — see runDirectorAgentCertRotation.
func (tc *TestCase) IsDirectorAgentRotationType() bool {
	return tc.Type == "director_agent_tls_cert_rotation_correctness"
}

// IsPerformanceType reports whether the case is scored as a throughput test —
// the plain `performance` type or the Kafka variant `kafka_performance`.
func (tc *TestCase) IsPerformanceType() bool {
	return tc.Type == "performance" || tc.Type == "kafka_performance"
}

// IsCorrectnessType reports whether the case is scored as a plain
// (non-persistence) correctness test — `correctness` or `kafka_correctness`.
func (tc *TestCase) IsCorrectnessType() bool {
	return tc.Type == "correctness" || tc.Type == "kafka_correctness"
}

// IsKafkaType reports whether the case drives the subject through a Kafka
// topology (any `kafka_*` type). Kafka consumption is at-least-once: crash and
// restart recovery can legitimately re-deliver records, so the verdict must
// allow over-delivery (duplicates) while still forbidding loss.
func (tc *TestCase) IsKafkaType() bool {
	return strings.HasPrefix(tc.Type, "kafka_")
}

// Validate runs structural checks that don't depend on runtime state.
// Returns an error for cases where the singular and plural forms are both
// set (ambiguous) or where required IDs on plural entries are missing.
func (tc *TestCase) Validate() error {
	if len(tc.Generators) > 0 && (tc.Generator.Mode != "" || tc.Generator.Target != "") {
		return fmt.Errorf("case %q: both `generator:` and `generators:` are set — pick one", tc.Name)
	}
	if len(tc.Receivers) > 0 && (tc.Receiver.Mode != "" || tc.Receiver.Listen != "") {
		return fmt.Errorf("case %q: both `receiver:` and `receivers:` are set — pick one", tc.Name)
	}
	ids := map[string]struct{}{}
	for i, g := range tc.Generators {
		if g.ID == "" {
			return fmt.Errorf("case %q: generators[%d] missing required `id`", tc.Name, i)
		}
		if _, dup := ids[g.ID]; dup {
			return fmt.Errorf("case %q: duplicate generator id %q", tc.Name, g.ID)
		}
		ids[g.ID] = struct{}{}
	}
	for _, g := range tc.AllGenerators() {
		if err := validateSampleFile(tc.Name, g.SampleFile); err != nil {
			return err
		}
	}
	ids = map[string]struct{}{}
	for i, r := range tc.Receivers {
		if r.ID == "" {
			return fmt.Errorf("case %q: receivers[%d] missing required `id`", tc.Name, i)
		}
		if _, dup := ids[r.ID]; dup {
			return fmt.Errorf("case %q: duplicate receiver id %q", tc.Name, r.ID)
		}
		ids[r.ID] = struct{}{}
	}
	// Endpoints: require name+image, unique, and not colliding with the fixed
	// service names the compose template always emits.
	reserved := map[string]struct{}{
		"subject": {}, "generator": {}, "receiver": {}, "collector": {},
		// Cloud emulator services rendered from the aws:/azure:/minio: blocks.
		"localstack": {}, "azurite": {}, "azure-init": {},
		"minio": {}, "minio-init": {},
		// Kafka broker services rendered from the kafka: block.
		"redpanda": {}, "redpanda-init": {},
		// Secret store services rendered from the vault: block.
		"vault": {}, "vault-init": {},
		// Dedicated pipeline-bus broker rendered from the pipeline_broker: block.
		"pipeline-broker": {}, "pipeline-broker-init": {},
	}
	epNames := map[string]struct{}{}
	for i, e := range tc.Endpoints {
		if e.Name == "" {
			return fmt.Errorf("case %q: endpoints[%d] missing required `name`", tc.Name, i)
		}
		if e.Image == "" {
			return fmt.Errorf("case %q: endpoint %q missing required `image`", tc.Name, e.Name)
		}
		if _, bad := reserved[e.Name]; bad {
			return fmt.Errorf("case %q: endpoint name %q is reserved", tc.Name, e.Name)
		}
		// Plural generators/receivers render dynamic compose services named
		// `generator-<id>` / `receiver-<id>`. An endpoint sharing that prefix
		// would collide with those service names, so reject it too.
		if strings.HasPrefix(e.Name, "generator-") || strings.HasPrefix(e.Name, "receiver-") {
			return fmt.Errorf("case %q: endpoint name %q is reserved (collides with dynamic generator-/receiver- service names)", tc.Name, e.Name)
		}
		if _, dup := epNames[e.Name]; dup {
			return fmt.Errorf("case %q: duplicate endpoint name %q", tc.Name, e.Name)
		}
		epNames[e.Name] = struct{}{}
	}
	// Kafka types require the broker block + a generator producing in kafka mode.
	if tc.IsKafkaType() {
		if tc.Kafka == nil {
			return fmt.Errorf("case %q: type %q requires a `kafka:` block", tc.Name, tc.Type)
		}
		gens := tc.AllGenerators()
		if len(gens) == 0 {
			return fmt.Errorf("case %q: type %q requires a generator with `mode: kafka`", tc.Name, tc.Type)
		}
		for _, g := range gens {
			if g.Mode != "kafka" {
				return fmt.Errorf("case %q: type %q requires generator mode \"kafka\", got %q", tc.Name, tc.Type, g.Mode)
			}
			if g.KafkaBatch < 0 {
				return fmt.Errorf("case %q: kafka_batch must be >= 1 (0/unset defaults to 1), got %d", tc.Name, g.KafkaBatch)
			}
		}
	}
	if tc.Correctness.MaxOverDeliveryPct < 0 {
		return fmt.Errorf("case %q: max_overdelivery_pct must be non-negative, got %.2f", tc.Name, tc.Correctness.MaxOverDeliveryPct)
	}
	if err := tc.validateVault(); err != nil {
		return err
	}
	if err := tc.validateKafkaAuth(); err != nil {
		return err
	}
	if err := tc.validatePipelineBroker(); err != nil {
		return err
	}
	if err := tc.validateCloud(); err != nil {
		return err
	}
	if err := tc.validateVerifier(); err != nil {
		return err
	}
	if err := tc.validateRotation(); err != nil {
		return err
	}
	return nil
}

// validateRotation checks the optional `rotation:` block and the
// director_agent_tls_cert_rotation_correctness type's structural requirements:
// the block is required for (and only meaningful to) that type, the mode must be
// known, and the case must be subject-driven (an endpoint the director deploys
// onto, no generator) with a min_received floor for the verdict.
func (tc *TestCase) validateRotation() error {
	if !tc.IsDirectorAgentRotationType() {
		if tc.Rotation != nil {
			return fmt.Errorf("case %q: `rotation:` is only valid for type director_agent_tls_cert_rotation_correctness", tc.Name)
		}
		return nil
	}
	if tc.Rotation == nil {
		return fmt.Errorf("case %q: type director_agent_tls_cert_rotation_correctness requires a `rotation:` block", tc.Name)
	}
	switch tc.Rotation.Mode {
	case RotationSameCA, RotationNewCARecover, RotationNewCAReject:
	default:
		return fmt.Errorf("case %q: rotation.mode %q must be one of %s, %s, %s",
			tc.Name, tc.Rotation.Mode, RotationSameCA, RotationNewCARecover, RotationNewCAReject)
	}
	// The director drives data by collecting from an endpoint and forwarding —
	// there is no generator, so the verdict rests on min_received plus the
	// post-rotation count behaviour. Guard the two structural preconditions.
	if tc.HasGenerator() {
		return fmt.Errorf("case %q: director_agent_tls_cert_rotation_correctness is subject-driven and must not declare a generator", tc.Name)
	}
	if len(tc.Endpoints) == 0 {
		return fmt.Errorf("case %q: director_agent_tls_cert_rotation_correctness requires an `endpoints:` block (the host the director deploys the agent onto)", tc.Name)
	}
	if tc.Correctness.MinReceived <= 0 {
		return fmt.Errorf("case %q: director_agent_tls_cert_rotation_correctness requires correctness.min_received > 0", tc.Name)
	}
	return nil
}

// validatePipelineBroker checks the optional `pipeline_broker:` block. When
// broker-side auto-create is disabled, topics can't be created on first produce,
// so the case must list the bus topics for the pipeline-broker-init one-shot to
// pre-create — otherwise the subject would dial a broker with no topics.
func (tc *TestCase) validatePipelineBroker() error {
	if tc.PipelineBroker == nil {
		return nil
	}
	if !tc.PipelineBroker.AutoCreateOrDefault() && len(tc.PipelineBroker.Topics) == 0 {
		return fmt.Errorf("case %q: pipeline_broker.auto_create is false but no `topics:` are listed — list the bus topics so they can be pre-created", tc.Name)
	}
	return nil
}

// validateVerifier checks the optional `verifier:` block: it needs a bucket, a
// known columnar format, and an S3 emulator (aws: or minio:) to read from.
func (tc *TestCase) validateVerifier() error {
	if tc.Verifier == nil {
		return nil
	}
	// The verifier's exact-count assertion derives EXPECTED_LINES from the
	// singular generator's total_lines (see orchestrator). A plural generators:
	// list or an unset/zero total_lines would silently disable exact-count
	// verification, so require a deterministic source.
	if tc.MultiGenerator() {
		return fmt.Errorf("case %q: verifier requires a singular `generator:`; `generators:` is not supported", tc.Name)
	}
	if tc.Generator.TotalLines <= 0 {
		return fmt.Errorf("case %q: verifier requires `generator.total_lines` > 0 for exact-count assertions", tc.Name)
	}
	// The verifier replaces the receiver entirely (the subject's sink is S3), and
	// the orchestrator renders no receiver for verifier cases. A `receiver:` /
	// `receivers:` block would render phantom services the orchestrator's
	// computed metadata omits, so reject the combination up front. (Mode/Listen
	// mirror the singular-receiver detection used in Validate().)
	if tc.MultiReceiver() || tc.Receiver.Mode != "" || tc.Receiver.Listen != "" {
		return fmt.Errorf("case %q: verifier replaces the receiver — remove the `receiver:`/`receivers:` block (the subject's sink is S3)", tc.Name)
	}
	if tc.Verifier.S3Bucket == "" {
		return fmt.Errorf("case %q: verifier requires `s3_bucket`", tc.Name)
	}
	if tc.Verifier.Format != "avro" && tc.Verifier.Format != "parquet" {
		return fmt.Errorf("case %q: verifier.format must be \"avro\" or \"parquet\", got %q", tc.Name, tc.Verifier.Format)
	}
	if !tc.UsesAWS() && !tc.UsesMinio() {
		return fmt.Errorf("case %q: verifier requires an `aws:` or `minio:` block for S3 access", tc.Name)
	}
	if _, err := time.ParseDuration(tc.Verifier.QuietWindowOrDefault()); err != nil {
		return fmt.Errorf("case %q: verifier.quiet_window %q is not a valid duration: %w", tc.Name, tc.Verifier.QuietWindow, err)
	}
	if _, err := time.ParseDuration(tc.Verifier.TimeoutOrDefault()); err != nil {
		return fmt.Errorf("case %q: verifier.timeout %q is not a valid duration: %w", tc.Name, tc.Verifier.Timeout, err)
	}
	return nil
}

// validateKafkaAuth checks the optional `kafka.auth` block: the mechanism and
// TLS mode must be from the known sets, and an `auth:` block that requests
// neither SASL nor TLS is a case-authoring mistake (a no-op).
func (tc *TestCase) validateKafkaAuth() error {
	if tc.Kafka == nil || tc.Kafka.Auth == nil {
		return nil
	}
	switch tc.Kafka.SASLMechanism() {
	case "", "plain", "scram-sha-256", "scram-sha-512", "gssapi":
	default:
		return fmt.Errorf("case %q: kafka.auth.mechanism %q must be one of plain, scram-sha-256, scram-sha-512, gssapi (or empty for mTLS)", tc.Name, tc.Kafka.Auth.Mechanism)
	}
	switch tc.Kafka.TLSMode() {
	case "none", "server", "mutual":
	default:
		return fmt.Errorf("case %q: kafka.auth.tls %q must be one of none, server, mutual", tc.Name, tc.Kafka.Auth.TLS)
	}
	if tc.Kafka.UsesGSSAPI() {
		// Kerberos runs on its own Apache Kafka + KDC topology (SASL_PLAINTEXT);
		// it does not combine with the Redpanda SASL/TLS knobs.
		if tc.Kafka.TLSMode() != "none" {
			return fmt.Errorf("case %q: kafka.auth.tls is not supported with mechanism gssapi (Kerberos case is SASL_PLAINTEXT)", tc.Name)
		}
		if !kerberosNameRe.MatchString(tc.Kafka.RealmOrDefault()) {
			return fmt.Errorf("case %q: kafka.auth.realm %q must match %s", tc.Name, tc.Kafka.RealmOrDefault(), kerberosNameRe)
		}
		if !kerberosNameRe.MatchString(tc.Kafka.ServiceNameOrDefault()) {
			return fmt.Errorf("case %q: kafka.auth.service_name %q must match %s", tc.Name, tc.Kafka.ServiceNameOrDefault(), kerberosNameRe)
		}
		return nil
	}
	// Server-only TLS without SASL is encryption with no client authentication.
	// The render path would set authentication_method=mtls_identity yet leave
	// require_client_auth false (an unauthenticated broker) — and the KafkaAuth
	// contract reserves an empty mechanism for tls: mutual (cert-only auth).
	if tc.Kafka.SASLMechanism() == "" && tc.Kafka.TLSMode() == "server" {
		return fmt.Errorf("case %q: kafka.auth.tls %q requires a SASL mechanism; use tls=mutual for cert-only auth", tc.Name, tc.Kafka.Auth.TLS)
	}
	if !tc.Kafka.AuthEnabled() {
		return fmt.Errorf("case %q: kafka.auth sets neither a SASL mechanism nor TLS — remove the block or configure one", tc.Name)
	}
	return nil
}

// kerberosNameRe constrains the realm/service-name strings that end up embedded
// in the rendered KDC bootstrap shell command — no shell or YAML metacharacter
// can pass (mirrors vaultPathRe). Realms are conventionally upper-case domains.
var kerberosNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// vaultPathRe / vaultTokenRe constrain the vault-config strings that end up
// embedded in the generated docker-compose file (the vault-init command line
// and environment): no shell or YAML metacharacter can pass. Secret VALUES
// are exempt — they are written to JSON seed files, never rendered inline.
var (
	vaultPathRe  = regexp.MustCompile(`^[A-Za-z0-9/_.-]+$`)
	vaultTokenRe = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

// validateVault checks the optional `vault:` block: seeding at least one
// secret is mandatory (a vault server nothing reads from is a case-authoring
// mistake), and every compose-embedded string is charset-restricted.
func (tc *TestCase) validateVault() error {
	if tc.Vault == nil {
		return nil
	}
	if len(tc.Vault.Secrets) == 0 {
		return fmt.Errorf("case %q: vault block requires at least one entry under `secrets`", tc.Name)
	}
	if !vaultTokenRe.MatchString(tc.Vault.TokenOrDefault()) {
		return fmt.Errorf("case %q: vault token must match %s", tc.Name, vaultTokenRe)
	}
	if !vaultPathRe.MatchString(tc.Vault.MountOrDefault()) {
		return fmt.Errorf("case %q: vault mount %q must match %s", tc.Name, tc.Vault.MountOrDefault(), vaultPathRe)
	}
	for path, fields := range tc.Vault.Secrets {
		if !vaultPathRe.MatchString(path) {
			return fmt.Errorf("case %q: vault secret path %q must match %s", tc.Name, path, vaultPathRe)
		}
		if len(fields) == 0 {
			return fmt.Errorf("case %q: vault secret %q has no fields", tc.Name, path)
		}
		for key := range fields {
			if !vaultTokenRe.MatchString(key) {
				return fmt.Errorf("case %q: vault secret %q field key %q must match %s", tc.Name, path, key, vaultTokenRe)
			}
		}
	}
	return nil
}

// validateSampleFile rejects sample_file paths that are absolute or escape the
// case directory. The orchestrator builds the host bind-mount path by joining
// the case directory with this value (see writeCompose); an absolute path or
// one containing ".." segments would let a case mount an arbitrary host file
// into the generator container. The path is required to stay case-relative.
func validateSampleFile(caseName, sampleFile string) error {
	if sampleFile == "" {
		return nil
	}
	if filepath.IsAbs(sampleFile) {
		return fmt.Errorf("case %q: sample_file %q must be a case-relative path, not absolute", caseName, sampleFile)
	}
	cleaned := filepath.Clean(sampleFile)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return fmt.Errorf("case %q: sample_file %q must not escape the case directory", caseName, sampleFile)
	}
	return nil
}

type GeneratorConfig struct {
	// ID is only meaningful in the plural `generators:` form. It names the
	// docker-compose service (`generator-<id>`) and, in sequenced/correctness
	// runs, distinguishes lines from different generators when they share a
	// downstream receiver. Required when used inside `generators:`; ignored
	// for the singular `generator:` form (the container is always
	// `bench-generator` there).
	ID          string `yaml:"id"`
	Mode        string `yaml:"mode"`        // "tcp" | "file" | "http" | "udp" | "udp_netflow_v5" | "otlp" | "kafka"
	Target      string `yaml:"target"`      // "subject:9000", file path, or "redpanda:9092" (kafka)
	Rate        int    `yaml:"rate"`        // lines/sec per connection, 0 = unlimited
	TotalLines  int64  `yaml:"total_lines"` // total lines to send, 0 = use duration
	LineSize    int    `yaml:"line_size"`   // bytes per line
	Format      string `yaml:"format"`      // "raw" | "syslog" | "json"
	Connections int    `yaml:"connections"` // parallel connections (default 1)
	// SampleFile replays a fixed input file instead of synthesizing lines.
	// Path is relative to the case directory (e.g. "input/sample.cef"). When
	// set, the generator sends the file's lines verbatim, cycling to reach
	// total_lines/duration — `format` no longer drives line content. The
	// harness bind-mounts the file into the generator container at /input.
	// Honored in both the singular `generator:` and plural `generators:` forms
	// (each plural generator gets its own bind mount + GENERATOR_SAMPLE_FILE).
	SampleFile string `yaml:"sample_file"`
	// RewriteTimestamp, with SampleFile, rewrites each replayed line's leading
	// RFC3164 syslog date ("Mmm _d hh:mm:ss") to the current time at send so
	// records aren't dropped as stale. No effect without SampleFile.
	RewriteTimestamp bool `yaml:"rewrite_timestamp"`
	// KafkaBatch (kafka mode only) packs N JSON records per Kafka message:
	// 1 = one JSON object per message, N>1 = a JSON array of N objects per
	// message. Defaults to 1 in the generator when unset. Lets a case compare
	// how a subject handles per-message vs batched-array ingestion.
	KafkaBatch int `yaml:"kafka_batch"`
	// Env is mode-specific extra env passed straight through to the
	// generator container (e.g. GENERATOR_OTLP_TRANSPORT=grpc). Lets a
	// case dial in transport variants without growing GeneratorConfig
	// for every new mode-specific knob.
	Env map[string]string `yaml:"env"`
	// FileRotation triggers a mid-test rotation/truncation of the target
	// file. Only valid when Mode == "file". Empty Mode = no rotation.
	FileRotation FileRotationConfig `yaml:"file_rotation"`
	// TLS is an optional generator-side TLS wrapper. When TLS.Enabled is
	// true the generator dials the target with crypto/tls instead of plain
	// TCP. The harness writes a self-signed CA + leaf cert pair into a
	// shared volume at warmup unless explicit paths are provided. Subjects
	// that don't declare TLS support (Capabilities) cause the case to fail
	// fast rather than starting and silently producing zero ingest.
	TLS TLSConfig `yaml:"tls"`
}

// FileRotationConfig drives a mid-test file rotation event so file-tail
// subjects are actually exercised against rotation, not just steady append.
type FileRotationConfig struct {
	// Mode picks the rotation style:
	//   "create"        — rename target → target+suffix, recreate target as a fresh inode
	//   "copytruncate"  — copy target → target+suffix, then truncate target to 0
	//   "truncate"      — truncate target to 0 with no copy
	//   ""              — no rotation (default)
	Mode string `yaml:"mode"`
	// At is the time offset from generator start at which to fire.
	At string `yaml:"at"`
	// Quiesce is the pause (with writes flushed and stopped) before the
	// destructive op runs — gives the subject's poller time to drain to EOF
	// so no pre-rotation lines are lost.
	Quiesce string `yaml:"quiesce"`
	// ArchiveSuffix is appended to the target path for the archive copy
	// in create/copytruncate modes (default ".1").
	ArchiveSuffix string `yaml:"archive_suffix"`
}

// TLSConfig describes a generator-side TLS wrapper. When Enabled is true
// the generator wraps its TCP dial in crypto/tls; otherwise this block is
// silently ignored, so existing cases that omit it are unaffected.
//
// Cert sourcing has two modes:
//   - Paths set (Cert/Key/CA): the generator loads from those paths inside
//     its container. Useful when a case bakes specific certs into a
//     volume.
//   - Paths empty: the harness auto-generates a self-signed CA + server/client
//     cert pair at warmup and mounts them at /certs/ in both the generator
//     and subject containers. The default leaf hostname is the subject
//     service alias ("subject") so the chain validates inside the bench
//     network.
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	Cert               string `yaml:"cert"`
	Key                string `yaml:"key"`
	CA                 string `yaml:"ca"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	MinVersion         string `yaml:"min_version"`
}

type ReceiverConfig struct {
	// ID is only meaningful in the plural `receivers:` form. It names the
	// docker-compose service (`receiver-<id>`) so subject configs can
	// reference each sink by stable hostname. Required when used inside
	// `receivers:`; ignored for the singular `receiver:` form (the
	// container is always `bench-receiver` there).
	ID     string `yaml:"id"`
	Mode   string `yaml:"mode"`   // "tcp" | "file" | "http" | "otlp" | "s3" | "azure_blob" | "sqs" | "kinesis" | "cloudwatch"
	Listen string `yaml:"listen"` // ":9001" or file path
	// Env is mode-specific extra env passed straight through to the
	// receiver container (e.g. RECEIVER_S3_BUCKET=bench-out), mirroring
	// GeneratorConfig.Env.
	Env map[string]string `yaml:"env"`
}

type Configuration struct {
	Description string            `yaml:"description"`
	Env         map[string]string `yaml:"env"`
}

type CorrectnessConfig struct {
	ValidateDedup bool `yaml:"validate_dedup"`
	// AllowOverDelivery permits duplicates in the correctness verdict for
	// at-least-once transports (S3-via-SQS notifications, SQS, Kinesis) —
	// the same allowance IsKafkaType() grants kafka_* cases. Loss is still
	// forbidden.
	AllowOverDelivery bool `yaml:"allow_overdelivery"`
	// ValidateContent runs a per-line structural check (CONN=/SEQ= prefix) to
	// detect memory corruption without building a full hash map. Cheap enough
	// for performance tests (O(1) per line, no heap growth).
	ValidateContent bool    `yaml:"validate_content"`
	ExpectedLossPct float64 `yaml:"expected_loss_pct"`
	// RequiredSubstring is a protocol-agnostic decode check: every emitted
	// line must contain the configured substring. Used by tests where the
	// generator and the on-the-wire output don't share a literal byte
	// stream (e.g. NetFlow → JSON pipelines, OTLP body extraction) — a
	// successful decode is proven by the presence of a value the generator
	// embedded in every record. Empty = check disabled.
	RequiredSubstring string `yaml:"required_substring"`
	// ValidateJSON, when true, requires every emitted line to parse as a
	// JSON object. Without this, a subject can pass a JSON-shape test by
	// truncating to a matching line count or by re-emitting binary garbage —
	// the receiver only counts newlines. Use for tests that exist to verify
	// JSON-handling correctness (e.g. wrapped_json_correctness).
	ValidateJSON bool `yaml:"validate_json"`

	// ExpectedMultiplier is the receiver fan-out factor: with N receivers
	// in a fan-out topology, every record reaches every receiver, so the
	// total observed at all receivers is ExpectedMultiplier * lines_in.
	// Defaults to 1 (no fan-out: receiver count equals generator count
	// after summing). Set explicitly in fan-out cases so loss accounting
	// stays honest.
	ExpectedMultiplier int `yaml:"expected_multiplier"`

	// MinReceived is the minimum number of records the receiver must observe
	// for a case with NO generator to pass (the subject produces data on its
	// own — e.g. an agentless deploy that collects from an endpoint and
	// forwards to the receiver). With no generator there's no line count to
	// derive loss/over-delivery from, so the verdict is simply
	// "LinesReceived >= MinReceived" (default 1) AND the receiver didn't flag a
	// content failure. Ignored when the case has a generator.
	MinReceived int64 `yaml:"min_received"`

	// DrainSeconds extends how long the harness waits for backlog to
	// arrive after the generator(s) stop. The case still finishes early if
	// the receiver stays quiet for DrainQuietWindow. Useful for cases that
	// validate throttled output draining a queue (the receiver is still
	// arriving long after the generator is done).
	DrainSeconds     int    `yaml:"drain_seconds"`
	DrainQuietWindow string `yaml:"drain_quiet_window"`

	// MaxOverDeliveryPct caps duplicate re-delivery as a percentage of
	// lines sent. Enforced by case types that verify source
	// acknowledgments actually persisted (kafka_offset_commit_restart:
	// a clean restart after full delivery must resume from committed
	// offsets, not re-consume the topic). Zero = strict, no
	// over-delivery allowed. Ignored by case types that don't document
	// it — at-least-once types deliberately tolerate duplicates.
	MaxOverDeliveryPct float64 `yaml:"max_overdelivery_pct"`

	// ExpectFailure inverts the verdict for NEGATIVE (security) tests: the case
	// PASSES iff the data path was BLOCKED — the receiver observed at most
	// ExpectFailureMaxReceived lines (default 0) — and FAILS if records got
	// through. Use to prove a control is load-bearing: e.g. a client presenting
	// the WRONG basic-auth password against a Vault-sourced HTTP device must be
	// 401'd so nothing is forwarded; if records arrive, auth was bypassed (a
	// regression a normal no-loss test would never catch). Requires a generator.
	ExpectFailure bool `yaml:"expect_failure"`
	// ExpectFailureMaxReceived is the inclusive upper bound on received lines
	// for an ExpectFailure case to still pass (default 0 = nothing may get
	// through). A small non-zero value tolerates a tiny in-flight leak.
	ExpectFailureMaxReceived int64 `yaml:"expect_failure_max_received"`

	// RateCeiling validates a per-window EPS ceiling on the receive side.
	// Empty MaxEPS = check disabled.
	RateCeiling RateCeilingConfig `yaml:"rate_ceiling"`

	// LoadBalance validates fairness across multiple receivers.
	// Empty Receivers + zero MinShareRatio = check disabled.
	LoadBalance LoadBalanceConfig `yaml:"load_balance"`
}

// RateCeilingConfig validates that the per-window EPS observed at the
// receiver never exceeds MaxEPS by more than Tolerance, ignoring the first
// SkipWarmup and last SkipCooldown seconds of receive time. Implemented by
// sliding a Window-sized window across receiver arrival timestamps; the
// receiver must therefore record per-record arrival timestamps when this
// check is enabled. See containers/receiver for the recorder side.
type RateCeilingConfig struct {
	MaxEPS       float64 `yaml:"max_eps"`
	Window       string  `yaml:"window"`        // e.g. "1s"
	Tolerance    float64 `yaml:"tolerance"`     // fraction over MaxEPS that's still acceptable (0.10 = 10%)
	SkipWarmup   string  `yaml:"skip_warmup"`   // ignore the first N seconds of receive time
	SkipCooldown string  `yaml:"skip_cooldown"` // ignore the last N seconds of receive time
	Sample       string  `yaml:"sample"`        // "every" (default) or "peak"
}

// Enabled reports whether the rate-ceiling check should run.
func (r RateCeilingConfig) Enabled() bool { return r.MaxEPS > 0 }

// LoadBalanceConfig validates that all participating receivers received a
// fair share of the stream: min(counts) / max(counts) >= MinShareRatio.
// Receivers names the receiver IDs to include; an empty list defaults to
// every receiver in the case. The check is skipped when total counts are
// below MinSampleSize (small samples produce spurious imbalance).
type LoadBalanceConfig struct {
	Receivers     []string `yaml:"receivers"`
	MinShareRatio float64  `yaml:"min_share_ratio"`
	MinSampleSize int64    `yaml:"min_sample_size"`
}

// Enabled reports whether the load-balance fairness check should run.
func (l LoadBalanceConfig) Enabled() bool { return l.MinShareRatio > 0 }

// LoadCase reads and parses a case.yaml from the given cases directory.
func LoadCase(casesDir, name string) (*TestCase, error) {
	path := filepath.Join(casesDir, name, "case.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading case %q: %w", name, err)
	}
	var tc TestCase
	if err := yaml.Unmarshal(data, &tc); err != nil {
		return nil, fmt.Errorf("parsing case %q: %w", name, err)
	}
	if tc.Name == "" {
		tc.Name = name
	}
	if err := tc.Validate(); err != nil {
		return nil, err
	}
	return &tc, nil
}

// ListCases returns all case names found in casesDir.
func ListCases(casesDir string) ([]string, error) {
	entries, err := os.ReadDir(casesDir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		p := filepath.Join(casesDir, e.Name(), "case.yaml")
		if _, err := os.Stat(p); err == nil {
			names = append(names, e.Name())
		}
	}
	return names, nil
}
