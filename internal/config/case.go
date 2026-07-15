package config

import (
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
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

	// Agent, when set, adds an external agent container to the test topology.
	// The agent connects INTO the subject (director) rather than being connected
	// to by it — useful for testing agent-mode device collection. The compose
	// service depends_on the subject (service_started) so the director's
	// WebSocket + HTTP endpoints are bound when the agent dials in.
	Agent *AgentConfig `yaml:"agent"`

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

	// Database, when set, adds a real database backend to the test topology,
	// keyed by `engine:` (see DatabaseEngines): the harness renders a
	// `database` service plus a one-shot `database-init` that creates the
	// database and runs the declared seed SQL, and the subject gates on the
	// seeding completing. Lets a case exercise a subject's database
	// poller/collector (e.g. vmetric's mssql customquery collector) against
	// a real engine instead of a mock.
	Database *DatabaseConfig `yaml:"database"`

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

	// SubjectDisk, when set, mounts a size-limited tmpfs at Path inside the
	// subject container, giving the subject a small dedicated "disk" for its
	// storage/buffer directory without a specially built image. Lets a case
	// exercise disk-full and disk-backpressure behavior: fill the volume with
	// more data than it can hold and observe whether the subject crashes,
	// drops, or backpressures. Used by the disk_pressure_correctness type but
	// honored on the singular subject service for any type.
	SubjectDisk *SubjectDiskConfig `yaml:"subject_disk"`

	// Requires lists subject capabilities every subject in this case must
	// declare (Subject.Capabilities); the runner fails fast on subjects
	// lacking one instead of starting a run that silently produces zero
	// ingest. Generalizes the original hardcoded tls_tcp guard.
	Requires []string `yaml:"requires"`

	// SubjectImage and SubjectVersion pin the subject container image for this
	// specific case. Applied after the registry default and before the global
	// CLI --image/--version flags, so: CLI flag > case pin > registry default.
	// Leave empty to use the registry default (or whatever --image/--version
	// specifies). Non-strict YAML decode means older harness binaries silently
	// ignore these fields — they fall back to the registry default.
	SubjectImage   string `yaml:"subject_image"`
	SubjectVersion string `yaml:"subject_version"`

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

	// ACLRotation, when set, parameterizes the
	// director_agent_acl_rotation_correctness driver: it rewrites the director's
	// on-disk config mid-run (swapping acl.allowed_ips) and relies on the
	// director's own refreshACL hot-reload — NO restart — to pick up the change.
	// Required by (and only meaningful for) that type. See ACLRotationConfig and
	// runDirectorAgentACLRotation.
	ACLRotation *ACLRotationConfig `yaml:"acl_rotation"`

	// Cluster, when set, makes the harness run the subject as an N-node director
	// cluster — each node is its own container (bench-subject-<name>-1..N,
	// hostnames subject-1..N), peers wired via the director cluster config, and
	// the per-node identity (director.id / node.name) injected by rendering the
	// subject config once per node with the {{@.NodeID@}} template var. It also
	// parameterizes the director_cluster_correctness driver. See ClusterConfig and
	// runDirectorClusterCorrectness.
	Cluster *ClusterConfig `yaml:"cluster"`

	// Fleet, when set, parameterizes the fleet_automation_correctness driver
	// (runFleetAutomationCorrectness): the director dials the bench fleet
	// simulator (an `endpoints:` container running vmetric/bench-fleetsim) over a
	// WebSocket and the driver asserts the platform<->director "automation"
	// functions — heartbeat/health, connection state, config push, remote check,
	// live data, console, stats — via the simulator's control API. See FleetConfig.
	Fleet *FleetConfig `yaml:"fleet"`

	// CCF, when set, parameterizes the ccf_correctness driver (runCCFCorrectness):
	// the director runs a `ccf` poller device that polls the bench mock CCF API (an
	// `endpoints:` container running vmetric/bench-ccfapi) using a real Sentinel
	// RestApiPoller connector definition, and forwards events to a TCP target the
	// bench receiver collects. The driver seeds the mock, then asserts pagination
	// completeness and time-cursor incrementality at the receiver. See CCFConfig.
	CCF *CCFConfig `yaml:"ccf"`

	// HTTPSource, when set, parameterizes the http_source_correctness driver
	// (runHTTPSourceCorrectness): the bench-httpsender container POSTs one event
	// per request (with optional HMAC/basic/header/HEC-token auth, gzip, TLS) to
	// the director's HTTP/Splunk-HEC source, which forwards to the receiver. The
	// driver asserts delivery (deliver) or rejection (reject). See HTTPSourceConfig.
	HTTPSource *HTTPSourceConfig `yaml:"http_source"`

	// ClickHouseTarget, when set, parameterizes the clickhouse_target_correctness
	// driver (runClickHouseTargetCorrectness): the generator drives TCP into a
	// director whose clickhouse target ingests via HTTP JSONEachRow into a real
	// clickhouse-server (an `endpoints:` container); the driver creates the table,
	// then verifies the row count by SQL. See ClickHouseTargetConfig.
	ClickHouseTarget *ClickHouseTargetConfig `yaml:"clickhouse_target"`

	// MQTTTarget parameterizes mqtt_target_correctness (runMQTTTargetCorrectness):
	// the generator drives TCP into a director whose mqtt target publishes to a
	// mosquitto broker (an `endpoints:` container); a mosquitto_sub sidecar records
	// every message and the driver verifies the count. See MQTTTargetConfig.
	MQTTTarget *MQTTTargetConfig `yaml:"mqtt_target"`

	// RedisSource parameterizes redis_source_correctness (runRedisSourceCorrectness):
	// a publisher sidecar PUBLISHes to a redis channel; the director's redis source
	// consumes and forwards to the receiver, which the driver counts. See RedisSourceConfig.
	RedisSource *RedisSourceConfig `yaml:"redis_source"`

	// HTTPVaultRotation parameterizes http_vault_rotation_correctness
	// (runHTTPVaultCertRotation): the http source serves HTTPS from a Vault-sourced
	// cert ($secret refs); the bench-httpsender posts continuously and records the
	// served cert fingerprint; the driver rotates the cert in Vault and asserts the
	// director hot-reloads it (fingerprint changes) while delivery continues. Requires
	// a `vault:` block. See HTTPVaultRotationConfig.
	HTTPVaultRotation *HTTPVaultRotationConfig `yaml:"http_vault_rotation"`

	// EndpointSource parameterizes endpoint_source_correctness
	// (runEndpointSourceCorrectness): a generic CLI-sender sidecar (snmptrap, tftp,
	// smtp, …) feeds the director's matching source, which forwards to the receiver;
	// the driver asserts the receiver count reaches expect_min and (optionally) sees
	// a required substring. One driver for any source the bench generator can't
	// drive. See EndpointSourceConfig.
	EndpointSource *EndpointSourceConfig `yaml:"endpoint_source"`
}

// VerifierConfig configures the post-drain DuckDB verifier container (see
// TestCase.Verifier). The verifier owns drain detection (it polls the bucket
// until the row count settles), then asserts exact row count, no duplicates,
// and no NULL payload fields over the columnar data.
type VerifierConfig struct {
	// S3Bucket is the bucket the subject wrote to. S3Prefix is an optional key
	// prefix to scope the scan to. Exactly one of S3Bucket / LocalDir must be
	// set (see validateVerifier).
	S3Bucket string `yaml:"s3_bucket"`
	S3Prefix string `yaml:"s3_prefix"`

	// LocalDir is the directory the subject's local `file` target wrote to,
	// reachable on the shared `/data` volume the harness mounts into both the
	// subject and the verifier. Set this instead of S3Bucket to verify a local
	// columnar file target: the verifier reads the files directly off disk
	// (read_parquet/read_avro, no httpfs/S3), so the case needs no aws:/minio:
	// emulator. Mutually exclusive with S3Bucket.
	LocalDir string `yaml:"local_dir"`

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

// IsLocal reports whether the verifier reads a local file target's output off
// the shared volume (LocalDir set) rather than the S3 emulator.
func (v *VerifierConfig) IsLocal() bool {
	return v != nil && v.LocalDir != ""
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

// DatabaseConfig configures the in-topology database backend (see
// TestCase.Database). Engine selects a DatabaseEngines entry, which supplies
// the image/credential defaults, env vars, healthcheck, and init command;
// Image/Password/Database override the engine's defaults per-case.
type DatabaseConfig struct {
	// Engine selects the backend (e.g. "mssql") — a key into DatabaseEngines.
	Engine string `yaml:"engine"`
	// Image overrides the engine's default container image.
	Image string `yaml:"image"`
	// Password overrides the engine's default admin/root credential. Flows
	// only through the compose `environment:` block and shell `$VAR`
	// expansion — never string-built into a command line. Test-only.
	Password string `yaml:"password"`
	// Database is the database name created by database-init (default
	// "bench").
	Database string `yaml:"database"`
	// SeedSQL is DDL/DML run once, after the server is healthy, via the
	// engine's CLI against Database. Mounted as a file — never rendered
	// inline into a command line.
	SeedSQL string `yaml:"seed_sql"`
	// TLS makes the database container present a per-run CA-signed server
	// certificate (SAN "database") so a device can verify it instead of using
	// insecure_skip_verify. The CA is mounted into the subject at
	// /opt/vmetric/certs/ca.crt; whether a device trusts it is the device's
	// own config choice (a negative case can withhold the CA to prove reject).
	TLS bool `yaml:"tls"`
}

// ImageOrDefault, PasswordOrDefault, DatabaseOrDefault centralize the
// database-backend defaults so the orchestrator and any caller render the
// same values.
func (d *DatabaseConfig) ImageOrDefault(engine DatabaseEngine) string {
	if d != nil && d.Image != "" {
		return d.Image
	}
	return engine.DefaultImage
}

func (d *DatabaseConfig) PasswordOrDefault(engine DatabaseEngine) string {
	if d != nil && d.Password != "" {
		return d.Password
	}
	return engine.DefaultPassword
}

func (d *DatabaseConfig) DatabaseOrDefault() string {
	if d != nil && d.Database != "" {
		return d.Database
	}
	return "bench"
}

// AgentConfig configures an external agent container in the test topology
// (see TestCase.Agent). Unlike endpoints (which the subject connects out to),
// the agent connects INTO the subject over the bench network — it starts after
// the subject (depends_on: subject service_started) so the director's
// WebSocket + HTTP endpoints are ready when the agent first dials in.
type AgentConfig struct {
	// Image is the container image to run. For vmetric agent-mode tests this
	// is typically vmetric/director-enterprise (which bakes vmetric-agent at
	// /package/agent/linux/amd64), avoiding a separate published image.
	Image string `yaml:"image"`
	// Env is the agent's environment — e.g. VMETRIC_CONFIG_HASH for the
	// vmetric-agent registration URL + device token + device ID.
	Env map[string]string `yaml:"env"`
	// Command overrides the image's default command (optional). Write it as
	// normal shell — "$" (e.g. $(date), ${VAR}) is passed literally to the
	// container; the harness escapes it for docker-compose interpolation.
	Command []string `yaml:"command"`
	// MountsSharedData, when true, mounts the shared-data volume at /data
	// inside the agent container (user 0:0). Use only when the agent needs
	// to write to /data.
	MountsSharedData bool `yaml:"mounts_shared_data"`
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
	// Healthcheck, when set, attaches a compose healthcheck to the endpoint so
	// the subject gates on it (depends_on … condition: service_healthy) before
	// starting. Without it the endpoint has no readiness signal and the subject
	// starts immediately alongside it. Needed when a subject's sink connects to
	// the endpoint eagerly at startup and crashes if it isn't up yet — e.g. the
	// otel-collector clickhouse exporter creates its schema on start, so the
	// clickhouse endpoint declares a healthcheck and otel waits for it.
	Healthcheck *EndpointHealthcheck `yaml:"healthcheck"`
}

// EndpointHealthcheck is a compose healthcheck for an Endpoint. Only Test is
// required; the timing fields default to a fast poll (see the OrDefault
// helpers) suitable for a local container coming up in a few seconds.
type EndpointHealthcheck struct {
	// Test is the command run inside the container, wrapped as CMD-SHELL — a
	// non-zero exit means unhealthy. Example (clickhouse-server):
	// "clickhouse-client --query 'SELECT 1'".
	Test string `yaml:"test"`
	// Interval between checks (default "2s").
	Interval string `yaml:"interval"`
	// Timeout per check (default "3s").
	Timeout string `yaml:"timeout"`
	// Retries before the container is marked unhealthy (default 30).
	Retries int `yaml:"retries"`
	// StartPeriod grace before failures count (default "2s").
	StartPeriod string `yaml:"start_period"`
}

func (h *EndpointHealthcheck) IntervalOrDefault() string {
	if h != nil && h.Interval != "" {
		return h.Interval
	}
	return "2s"
}
func (h *EndpointHealthcheck) TimeoutOrDefault() string {
	if h != nil && h.Timeout != "" {
		return h.Timeout
	}
	return "3s"
}
func (h *EndpointHealthcheck) RetriesOrDefault() int {
	if h != nil && h.Retries > 0 {
		return h.Retries
	}
	return 30
}
func (h *EndpointHealthcheck) StartPeriodOrDefault() string {
	if h != nil && h.StartPeriod != "" {
		return h.StartPeriod
	}
	return "2s"
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

// ACLRotationConfig parameterizes the director_agent_acl_rotation_correctness
// driver. Unlike the cert-rotation driver (which bounces the director), this
// driver rewrites the director's mounted config in place mid-run and relies on
// the director's own ACL hot-reload (refreshACL re-reads the config every
// acl.update_interval seconds — no restart). The case ships two configs that
// has a single config (configs/<configName>/vmetric.yml or configs/vmetric.yml)
// holding the INITIAL allowed_ips; the driver edits only acl.allowed_ips in place
// mid-run, setting it to AllowedIPs. Nothing else in the config can change, so
// the verdict is never confounded by an unrelated config diff.
type ACLRotationConfig struct {
	// Expect selects the direction and verdict:
	//   "recover" — start BLOCKED (initial allowed_ips excludes the agent → 0
	//               records), rotate to an allowlist that admits the agent;
	//               delivery must START. Verdict: ~0 before, >= min_received after.
	//   "revoke"  — start ALLOWED (records flow), rotate to an allowlist that
	//               excludes the agent; the next /agent/vmf POST after the
	//               refresh is rejected so delivery must STOP. Verdict:
	//               >= min_received before, no meaningful new records after.
	Expect string `yaml:"expect"`

	// AllowedIPs is the allowlist the driver rotates TO: mid-run it overwrites
	// acl.allowed_ips in the (single) mounted director config with exactly this
	// list. Required. recover sets it to ranges that ADMIT the agent's bench IP;
	// revoke sets it to ranges that EXCLUDE it (e.g. ["192.0.2.1"]).
	AllowedIPs []string `yaml:"allowed_ips"`

	// SettleSeconds is how long to wait after rewriting the config before
	// sampling the receiver. It MUST comfortably exceed the director's
	// acl.update_interval (set that low — e.g. 3 — in the case config) so the
	// refresh tick has fired (default 15).
	SettleSeconds int `yaml:"settle_seconds"`

	// BaselineSeconds (recover only) is how long the driver confirms the agent is
	// blocked (count stays ~0) before rotating, proving the case really starts in
	// the blocked state (default 20).
	BaselineSeconds int `yaml:"baseline_seconds"`
}

// ACL rotation Expect values for ACLRotationConfig.Expect.
const (
	ACLRotationRecover = "recover"
	ACLRotationRevoke  = "revoke"
)

// SettleSecondsOrDefault / BaselineSecondsOrDefault centralize the ACL-rotation
// defaults so the driver and any caller agree.
func (ac *ACLRotationConfig) SettleSecondsOrDefault() int {
	if ac != nil && ac.SettleSeconds > 0 {
		return ac.SettleSeconds
	}
	return 15
}

func (ac *ACLRotationConfig) BaselineSecondsOrDefault() int {
	if ac != nil && ac.BaselineSeconds > 0 {
		return ac.BaselineSeconds
	}
	return 20
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

// UsesDatabase reports whether the case adds a database backend to the topology.
func (tc *TestCase) UsesDatabase() bool { return tc.Database != nil }

// UsesDatabaseTLS reports whether the database container should terminate TLS
// with a per-run CA-signed cert (so devices can verify instead of skip-verify).
func (tc *TestCase) UsesDatabaseTLS() bool { return tc.Database != nil && tc.Database.TLS }

// UsesVerifier reports whether the case drives correctness through the one-shot
// DuckDB verifier container instead of a receiver.
func (tc *TestCase) UsesVerifier() bool { return tc.Verifier != nil }

// UsesAgent reports whether the case adds an external agent container to the
// topology. The agent connects into the subject rather than being connected to.
func (tc *TestCase) UsesAgent() bool { return tc.Agent != nil }

// IsDirectorAgentRotationType reports whether the case is the director↔agent
// TLS cert-rotation correctness flow, which has its own subject-driven (no
// generator) driver — see runDirectorAgentCertRotation.
func (tc *TestCase) IsDirectorAgentRotationType() bool {
	return tc.Type == "director_agent_tls_cert_rotation_correctness"
}

// IsDirectorAgentACLRotationType reports whether the case is the director↔agent
// ACL (allowed_ips) hot-reload rotation flow, which has its own subject-driven
// (no generator), no-bounce driver — see runDirectorAgentACLRotation.
func (tc *TestCase) IsDirectorAgentACLRotationType() bool {
	return tc.Type == "director_agent_acl_rotation_correctness"
}

// IsDirectorClusterType reports whether the case runs the subject as a multi-node
// director cluster with the cluster-aware driver (runDirectorClusterCorrectness):
// it brings up N director nodes, waits for the cluster to form + a leader to be
// elected, drives a workload, optionally disrupts a node (restart/stop) and
// asserts the cluster recovers / fails a device over to a surviving node.
func (tc *TestCase) IsDirectorClusterType() bool {
	return tc.Type == "director_cluster_correctness"
}

// IsFleetAutomationType reports whether the case exercises the director's "fleet"
// automation path against the bench fleet simulator (runFleetAutomationCorrectness).
// The director dials the simulator's WebSocket (fleet.type=vmetric + a custom ws
// URL) and the driver, via the simulator's control API, asserts the
// platform<->director automation functions (health/heartbeat, connection state,
// config update, remote check, live data, console, stats).
func (tc *TestCase) IsFleetAutomationType() bool {
	return tc.Type == "fleet_automation_correctness"
}

// IsCCFType reports whether the case exercises the director's "ccf" poller device
// (Codeless Connector Framework / Sentinel RestApiPoller) against the bench mock
// CCF API (runCCFCorrectness): the director polls the mock with a real connector
// definition and forwards events to the receiver, and the driver asserts
// pagination completeness and time-cursor incrementality.
func (tc *TestCase) IsCCFType() bool {
	return tc.Type == "ccf_correctness"
}

// IsHTTPSourceType reports whether the case drives the director's HTTP / Splunk-HEC
// SOURCE listener with the bench-httpsender (runHTTPSourceCorrectness).
func (tc *TestCase) IsHTTPSourceType() bool {
	return tc.Type == "http_source_correctness"
}

// IsClickHouseTargetType reports whether the case verifies the director's
// clickhouse target against a real clickhouse-server (runClickHouseTargetCorrectness).
func (tc *TestCase) IsClickHouseTargetType() bool {
	return tc.Type == "clickhouse_target_correctness"
}

// IsMQTTTargetType reports whether the case verifies the director's mqtt target
// against a real mosquitto broker (runMQTTTargetCorrectness).
func (tc *TestCase) IsMQTTTargetType() bool {
	return tc.Type == "mqtt_target_correctness"
}

// IsRedisSourceType reports whether the case drives the director's redis source
// via a redis publisher sidecar (runRedisSourceCorrectness).
func (tc *TestCase) IsRedisSourceType() bool {
	return tc.Type == "redis_source_correctness"
}

// IsHTTPVaultRotationType reports whether the case verifies the http source's
// Vault-sourced cert hot-reload (runHTTPVaultCertRotation).
func (tc *TestCase) IsHTTPVaultRotationType() bool {
	return tc.Type == "http_vault_rotation_correctness"
}

// IsEndpointSourceType reports whether the case drives a director source via a
// generic CLI-sender sidecar and counts at the receiver (runEndpointSourceCorrectness).
func (tc *TestCase) IsEndpointSourceType() bool {
	return tc.Type == "endpoint_source_correctness"
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

// subjectDiskSizeRe accepts the size strings the orchestrator's
// parseByteSize understands: plain bytes ("1048576"), plain bytes with a
// "b" suffix ("64b"), or a k/m/g unit with an optional trailing "b"
// ("64m", "64mb"), case-insensitive. Kept in lockstep with parseByteSize
// so a case that passes Validate can never fail at compose-render time.
var subjectDiskSizeRe = regexp.MustCompile(`(?i)^[0-9]+([kmg]b?|b)?$`)

// Validate runs structural checks that don't depend on runtime state.
// Returns an error for cases where the singular and plural forms are both
// set (ambiguous) or where required IDs on plural entries are missing.
func (tc *TestCase) Validate() error {
	if len(tc.Generators) > 0 && (tc.Generator.Mode != "" || tc.Generator.Target != "") {
		return fmt.Errorf("case %q: both `generator:` and `generators:` are set — pick one", tc.Name)
	}
	if tc.SubjectDisk != nil {
		if tc.SubjectDisk.Path == "" || tc.SubjectDisk.Size == "" {
			return fmt.Errorf("case %q: subject_disk requires both `path` and `size`", tc.Name)
		}
		if !strings.HasPrefix(tc.SubjectDisk.Path, "/") {
			return fmt.Errorf("case %q: subject_disk.path must be absolute, got %q", tc.Name, tc.SubjectDisk.Path)
		}
		if !subjectDiskSizeRe.MatchString(tc.SubjectDisk.Size) {
			return fmt.Errorf("case %q: subject_disk.size %q is not a valid tmpfs size (e.g. 64m, 1g, 1048576)", tc.Name, tc.SubjectDisk.Size)
		}
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
		// External agent service rendered from the agent: block.
		"agent": {},
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
	if err := tc.validateDatabase(); err != nil {
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
	if err := tc.validateACLRotation(); err != nil {
		return err
	}
	if err := tc.validateCluster(); err != nil {
		return err
	}
	if err := tc.validateFleet(); err != nil {
		return err
	}
	if err := tc.validateCCF(); err != nil {
		return err
	}
	if err := tc.validateHTTPSource(); err != nil {
		return err
	}
	if err := tc.validateClickHouseTarget(); err != nil {
		return err
	}
	if err := tc.validateMQTTTarget(); err != nil {
		return err
	}
	if err := tc.validateRedisSource(); err != nil {
		return err
	}
	if err := tc.validateHTTPVaultRotation(); err != nil {
		return err
	}
	if err := tc.validateEndpointSource(); err != nil {
		return err
	}
	return nil
}

// ClusterConfig parameterizes director_cluster_correctness: the harness starts
// `Nodes` director containers wired as a cluster (peers on hostnames subject-1..N,
// inter-node transport plaintext or TLS per `TLS`), waits for the cluster to form
// and elect a leader, then optionally performs `Action` (a disruptive event) and
// asserts recovery.
type ClusterConfig struct {
	// Nodes is the number of director nodes to start (>= 3 for a real quorum).
	Nodes int `yaml:"nodes"`
	// TLS enables the inter-node cluster transport over TLS (tls.status:true in the
	// cluster config). false = plaintext nats:// routes. When true the case must
	// ship the cluster cert material under configs/certs/ (mounted at
	// /opt/vmetric/certs) referenced by the cluster config.
	TLS bool `yaml:"tls"`
	// Action is the disruptive edge-case the driver performs mid-run, then asserts
	// the cluster recovers. "" = no disruption (baseline: form + leader + flow).
	// Known values:
	//   restart_follower   — restart a non-leader node; cluster stays up, flow continues.
	//   restart_leader     — restart the leader; a NEW leader is elected, flow continues.
	//   stop_two_recover   — stop a quorum-removing majority (ceil(N/2) of N nodes,
	//                        e.g. 2 of 3) to lose quorum, then start them again; the
	//                        cluster comes back and elects a leader.
	//   agentless_failover — restart the node that OWNS the agentless device; the
	//                        leader must reassign it to ANOTHER node (the hard verdict,
	//                        plus a leader still exists). Collection-resume + end-to-end
	//                        delivery are soft-logged (cluster data-plane recovery after
	//                        a node loss is a known director gap). Requires
	//                        persistent_storage in the subject config (collector payloads
	//                        must be cluster-shared via the NATS object store).
	//   cluster_ip_failover — the leader binds cluster.ip as a virtual IP on its
	//                        interface; restart the leader and assert the IP migrates
	//                        to the newly elected leader (and the old leader releases
	//                        it). Requires cluster.ip; the harness grants the node
	//                        containers NET_ADMIN and pins the bench subnet.
	Action string `yaml:"action"`
	// SettleSeconds is how long to wait after a disruptive Action before asserting
	// recovery (default 45 — must exceed the 15s heartbeat timeout + reassignment).
	SettleSeconds int `yaml:"settle_seconds"`
	// IP is the virtual/cluster IP the elected leader binds (cluster_ip_failover).
	// Must lie in the harness-pinned bench subnet 172.30.0.0/16. The harness grants
	// the node containers NET_ADMIN so the director can add it; the driver asserts
	// the leader holds it and that it migrates to the new leader on failover.
	IP string `yaml:"ip"`
}

// SettleOrDefault returns SettleSeconds or 45 when unset.
func (c *ClusterConfig) SettleOrDefault() int {
	if c == nil || c.SettleSeconds <= 0 {
		return 45
	}
	return c.SettleSeconds
}

// validateCluster checks the optional `cluster:` block and the
// director_cluster_correctness type's structural requirements.
func (tc *TestCase) validateCluster() error {
	if !tc.IsDirectorClusterType() {
		if tc.Cluster != nil {
			return fmt.Errorf("case %q: `cluster:` is only valid for type director_cluster_correctness", tc.Name)
		}
		return nil
	}
	if tc.Cluster == nil {
		return fmt.Errorf("case %q: type director_cluster_correctness requires a `cluster:` block", tc.Name)
	}
	if tc.Cluster.Nodes < 3 {
		return fmt.Errorf("case %q: cluster.nodes must be >= 3, got %d", tc.Name, tc.Cluster.Nodes)
	}
	switch tc.Cluster.Action {
	case "", "restart_follower", "restart_leader", "stop_two_recover", "agentless_failover", "cluster_ip_failover":
	default:
		return fmt.Errorf("case %q: unknown cluster.action %q", tc.Name, tc.Cluster.Action)
	}
	if tc.Cluster.Action == "cluster_ip_failover" {
		if tc.Cluster.IP == "" {
			return fmt.Errorf("case %q: cluster.action cluster_ip_failover requires cluster.ip", tc.Name)
		}
		ip := net.ParseIP(tc.Cluster.IP)
		if ip == nil || ip.To4() == nil {
			return fmt.Errorf("case %q: cluster.ip %q must be an IPv4 address", tc.Name, tc.Cluster.IP)
		}
		// The harness pins the bench network to 172.30.0.0/16 (clusterBenchSubnet in
		// internal/orchestrator/docker.go) so the VIP is in range; reject anything
		// outside it up front rather than failing later when the director can't use it.
		if _, benchNet, _ := net.ParseCIDR("172.30.0.0/16"); !benchNet.Contains(ip) {
			return fmt.Errorf("case %q: cluster.ip %q must be within the pinned bench subnet 172.30.0.0/16", tc.Name, tc.Cluster.IP)
		}
	} else if tc.Cluster.IP != "" {
		return fmt.Errorf("case %q: cluster.ip is only valid with cluster.action cluster_ip_failover", tc.Name)
	}
	return nil
}

// FleetConfig parameterizes fleet_automation_correctness: the director connects to
// the bench fleet simulator (an endpoints container, hostname fleet-sim, running
// vmetric/bench-fleetsim) over a WebSocket, and the driver drives + asserts one
// automation Scenario through the simulator's control API.
type FleetConfig struct {
	// Scenario selects what the driver does after the director connects:
	//   connect       — assert heartbeat/health + connection_state are published.
	//   config_update — push a new config (Update Triggered) and assert the
	//                   director applies it (replies executed).
	//   remote_check  — send a remote_check (SSH) command and assert the reply.
	//   live_data     — start a live-data capture and assert the session replies.
	//   console_log   — start a live-console capture and assert the session replies.
	//   stats         — assert the director forwards stats/metrics partitions.
	//   reconnect     — restart the simulator and assert the director reconnects.
	//   bad_token     — the director's token mismatches; assert it never connects.
	//   self_managed  — director is self-managed; assert it ignores platform commands
	//                   (a sent remote_check gets no reply) while still publishing health.
	Scenario string `yaml:"scenario"`
	// SimContainer is the endpoints container name running the simulator
	// (default "fleet-sim" → docker container "bench-fleet-sim").
	SimContainer string `yaml:"sim_container"`
	// DirectorID the simulator keys the director by (default "1").
	DirectorID string `yaml:"director_id"`
	// MinHealth is the minimum number of health frames the connect/self_managed
	// scenarios require (default 2).
	MinHealth int `yaml:"min_health"`
	// Remote* parameterize the remote_check scenario (SSH target on the bench net).
	RemoteAddress      string `yaml:"remote_address"`
	RemoteUsername     string `yaml:"remote_username"`
	RemotePassword     string `yaml:"remote_password"`
	RemotePort         int    `yaml:"remote_port"`
	ExpectRemoteResult *int   `yaml:"expect_remote_result"` // 1 success, 0 failure (nil → default 1)
	// LiveWhere/LiveSource parameterize the live_data scenario (default
	// before-pre-process / director).
	LiveWhere  string `yaml:"live_where"`
	LiveSource string `yaml:"live_source"`
	// LiveMinFrameBytes, when > 0, asserts the largest live_data/console_log
	// reply frame is at least this many bytes — i.e. a REAL data frame came back,
	// not just the tiny "Capture Started/Completed" lifecycle markers. Used by the
	// where:raw mis-frame case so a marker-only capture can't pass on count alone.
	LiveMinFrameBytes int `yaml:"live_min_frame_bytes"`
	// LiveSourceID targets a specific source for the live_data scenario: the
	// device id when live_source=device, the route id when live_source=advanced-route
	// (0 = "no specific route", which the director normalizes to a director-wide
	// routing-boundary capture). Ignored for live_source=director. Defaults to 0.
	LiveSourceID int `yaml:"live_source_id"`
	// SettleSeconds is how long to wait for a scenario's effect (default 45).
	SettleSeconds int `yaml:"settle_seconds"`
	// DeliverConfig, when set, is a file under the case's configs/ dir holding an
	// audited VMF operational config (environments/devices/targets/routes/proxy_tls)
	// that the simulator delivers to the director after it connects — mirroring the
	// real platform, since a fleet director rejects plain YAML and loads its
	// operational config only from a platform-delivered, encoded vmetric.vmf. Needed
	// by scenarios that require a real pipeline (live_data/stats) or the agent-comm
	// interface (enrollment). Produce the VMF with the backend cmd/configencoder.
	DeliverConfig string `yaml:"deliver_config"`
	// RestartMidRun (stats scenario) stops the director container partway
	// through the run and restarts it, to validate resilience: an agent that is
	// streaming should buffer + retry across the restart (no error storm), and
	// once the director is back every record should be accounted for in the
	// decoded stats (ExpectStats is asserted AFTER the restart). Pair with a
	// persistent_storage director so staged payloads survive the bounce.
	RestartMidRun bool `yaml:"restart_mid_run"`
	// ExpectStats (stats scenario only) asserts on the stats the simulator
	// DECODES from the director's forwarded VMF metric frames — not just that
	// frames arrived. Keyed by bucket "<inputtype>.<type>" (e.g. "route.in") →
	// counter name ("dropped_count" | "error_count" | "events_in" |
	// "events_out" | "bytes_in" | "bytes_out" | "events") → expected value. The
	// driver polls until every expected counter is reached and then requires an
	// EXACT match (deterministic: same input + same pipeline ⇒ same counts).
	// Example: {route.in: {events_in: 500, dropped_count: 500, events_out: 0}}.
	ExpectStats map[string]map[string]int64 `yaml:"expect_stats"`

	// BaselineSeconds (config_update data-plane mode only) is how long the driver
	// confirms delivery is suppressed after the BEFORE config is delivered, before
	// pushing the AFTER config. It proves the case really starts suppressed
	// (otherwise "delivery after the update" is vacuous). Default 20.
	BaselineSeconds int `yaml:"baseline_seconds"`

	// DownloadGate (agent_download_gate scenario) lists the per-device probes the
	// driver fires at the director's /dl/agent/<os>/<arch> endpoint: each GET
	// carries X-VM-Device-Id and must return ExpectCode (200 = served, 403 =
	// blocked). This exercises the director's authoritative update-download gate
	// against devices whose update policy is set in the subject's config.
	DownloadGate []DownloadGateProbe `yaml:"download_gate"`
	// DownloadOSArch is the "<os>/<arch>" path segment probed (default "linux/amd64").
	DownloadOSArch string `yaml:"download_os_arch"`
	// ThrottleCount (agent_download_gate) fires this many CONCURRENT downloads for
	// an allowed device; ThrottleMin429 asserts at least this many come back 429
	// (the director's download throttle engaged). 0 skips the throttle check.
	ThrottleCount  int `yaml:"throttle_count"`
	ThrottleMin429 int `yaml:"throttle_min_429"`
	// ThrottleDeviceID is the (allowed) device id used for the throttle burst
	// (default "0" would be blocked; set it to an immediate/authorized device).
	ThrottleDeviceID string `yaml:"throttle_device_id"`
}

// DownloadGateProbe is one agent_download_gate assertion: GET /dl as DeviceID,
// expect HTTP ExpectCode.
type DownloadGateProbe struct {
	DeviceID   string `yaml:"device_id"`
	ExpectCode int    `yaml:"expect_code"`
}

// BaselineSecondsOrDefault returns BaselineSeconds or 20 when unset.
func (f *FleetConfig) BaselineSecondsOrDefault() int {
	if f == nil || f.BaselineSeconds <= 0 {
		return 20
	}
	return f.BaselineSeconds
}

// IsConfigUpdateDataPlane reports whether the config_update scenario is in
// DATA-PLANE mode: a fleet director loads its operational pipeline only from an
// encoded vmetric.vmf, so the BEFORE pipeline is delivered via deliver_config
// (an encoded .vmf, like the live_data/stats scenarios). In that mode the driver
// confirms delivery is suppressed, pushes the AFTER .vmf (configs/update.vmf),
// and confirms delivery starts — proving a device/target/route/pipeline update
// takes effect on the data plane, not just that the director ACKs it. Without a
// deliver_config the scenario keeps its legacy control-plane (executed-only)
// behavior.
func (f *FleetConfig) IsConfigUpdateDataPlane() bool {
	return f != nil && f.Scenario == "config_update" && f.DeliverConfig != ""
}

// SettleOrDefault returns SettleSeconds or 45 when unset.
func (f *FleetConfig) SettleOrDefault() int {
	if f == nil || f.SettleSeconds <= 0 {
		return 45
	}
	return f.SettleSeconds
}

// SimContainerOrDefault returns the bench container name of the simulator.
func (f *FleetConfig) SimContainerOrDefault() string {
	name := "fleet-sim"
	if f != nil && f.SimContainer != "" {
		name = f.SimContainer
	}
	return "bench-" + name
}

// DirectorIDOrDefault returns the simulator's director key (default "1").
func (f *FleetConfig) DirectorIDOrDefault() string {
	if f != nil && f.DirectorID != "" {
		return f.DirectorID
	}
	return "1"
}

// ExpectRemoteResultOrDefault returns the expected remote_check result, defaulting
// to 1 (success) when the field is omitted, while still allowing an explicit 0.
func (f *FleetConfig) ExpectRemoteResultOrDefault() int {
	if f == nil || f.ExpectRemoteResult == nil {
		return 1
	}
	return *f.ExpectRemoteResult
}

// validateFleet checks the optional `fleet:` block and the
// fleet_automation_correctness type's structural requirements.
func (tc *TestCase) validateFleet() error {
	if !tc.IsFleetAutomationType() {
		if tc.Fleet != nil {
			return fmt.Errorf("case %q: `fleet:` is only valid for type fleet_automation_correctness", tc.Name)
		}
		return nil
	}
	if tc.Fleet == nil {
		return fmt.Errorf("case %q: type fleet_automation_correctness requires a `fleet:` block", tc.Name)
	}
	switch tc.Fleet.Scenario {
	case "config_change":
		// config_change asserts a live A→B target config transition: deliver_config
		// provides config A (the pre-step) and configs/update.vmf is the mid-run B.
		// Without a prior delivered config there is no transition to prove — the test
		// would only validate an initial config, so require deliver_config.
		if tc.Fleet.DeliverConfig == "" {
			return fmt.Errorf("case %q: fleet.scenario config_change requires fleet.deliver_config (the prior config A to transition away from)", tc.Name)
		}
	case "pipeline_data":
		// pipeline_data delivers an operational config carrying a real pipeline
		// (and any library assets it references) via deliver_config, drives the
		// generator through it, and asserts the transformed records reach the
		// receiver AND pass its content checks (required_substring / dedup). The
		// pipeline arrives only over the fleet VMF, so deliver_config is required;
		// the content assertion is what makes it a transform verdict rather than a
		// bytes-flowed one.
		if tc.Fleet.DeliverConfig == "" {
			return fmt.Errorf("case %q: fleet.scenario pipeline_data requires fleet.deliver_config (the VMF carrying the pipeline/library to apply)", tc.Name)
		}
		if tc.Correctness.RequiredSubstring == "" {
			return fmt.Errorf("case %q: fleet.scenario pipeline_data requires correctness.required_substring (the value the delivered pipeline/library must produce on every record)", tc.Name)
		}
	case "pipeline_verify":
		// pipeline_verify delivers an operational config whose TARGET writes columnar
		// objects (parquet/avro) using a delivered library schema, then runs the
		// DuckDB verifier (reused from the generic path) against the object store.
		// The verifier's intrinsic checks (no duplicates, non-NULL columns) are the
		// verdict — a library-defined column surviving the round-trip proves the
		// schema was delivered + applied. Needs the VMF and a verifier block.
		if tc.Fleet.DeliverConfig == "" {
			return fmt.Errorf("case %q: fleet.scenario pipeline_verify requires fleet.deliver_config (the VMF carrying the target/library schema)", tc.Name)
		}
		if !tc.UsesVerifier() {
			return fmt.Errorf("case %q: fleet.scenario pipeline_verify requires a `verifier:` block (the object-store verdict)", tc.Name)
		}
	case "pipeline_verify_change":
		// Live UPDATE of a target/library schema, verified via the object store.
		// deliver_config is config A (the target writes nothing — e.g. its library
		// schema is absent); configs/update.vmf is config B (adds the schema so the
		// target writes). After the push the DuckDB verifier asserts objects exist
		// with the library-defined columns intact. Needs the VMF + a verifier block;
		// configs/update.vmf is checked at run time (as config_change does).
		if tc.Fleet.DeliverConfig == "" {
			return fmt.Errorf("case %q: fleet.scenario pipeline_verify_change requires fleet.deliver_config (config A)", tc.Name)
		}
		if !tc.UsesVerifier() {
			return fmt.Errorf("case %q: fleet.scenario pipeline_verify_change requires a `verifier:` block (the object-store verdict)", tc.Name)
		}
	case "agent_download_gate":
		// The director serves /dl for agent binary downloads; the driver probes it
		// per-device and asserts the authorization gate (200/403) + throttle (429).
		// The subject config carries the device update policies directly (plain
		// YAML with an environments block — no deliver_config), so nothing extra is
		// required here beyond the download_gate probe list.
		if len(tc.Fleet.DownloadGate) == 0 && tc.Fleet.ThrottleCount == 0 {
			return fmt.Errorf("case %q: fleet.scenario agent_download_gate requires fleet.download_gate probes and/or fleet.throttle_count", tc.Name)
		}
		// When the throttle burst is enabled it must actually be able to engage the
		// throttle: require a positive 429 threshold (otherwise the 429 assertion is
		// vacuous) and an explicit allowed device (the default "0" is blocked and
		// would 403 before ever acquiring a download slot).
		if tc.Fleet.ThrottleCount > 0 {
			if tc.Fleet.ThrottleMin429 <= 0 {
				return fmt.Errorf("case %q: fleet.scenario agent_download_gate with throttle_count > 0 requires throttle_min_429 > 0 (else the 429 assertion is vacuous)", tc.Name)
			}
			if tc.Fleet.ThrottleDeviceID == "" {
				return fmt.Errorf("case %q: fleet.scenario agent_download_gate with throttle_count > 0 requires throttle_device_id (an allowed device; the default \"0\" is blocked and never reaches the throttle)", tc.Name)
			}
		}
	case "connect", "config_update", "remote_check", "live_data", "console_log", "stats", "reconnect", "bad_token", "self_managed", "enrollment":
	default:
		return fmt.Errorf("case %q: unknown fleet.scenario %q", tc.Name, tc.Fleet.Scenario)
	}
	// The simulator must be declared as an endpoints container (unless bad_token,
	// which still needs it to prove the rejection).
	want := tc.Fleet.SimContainerOrDefault()[len("bench-"):]
	found := false
	for _, e := range tc.Endpoints {
		if e.Name == want {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("case %q: fleet_automation_correctness requires an endpoints entry named %q (the simulator)", tc.Name, want)
	}
	// config_update data-plane mode (deliver_config set): the BEFORE pipeline is
	// delivered as an encoded .vmf, the driver drives traffic, then pushes the
	// AFTER .vmf (configs/update.vmf) and asserts the receiver count. It therefore
	// needs a generator and a positive min_received threshold.
	if tc.Fleet.IsConfigUpdateDataPlane() {
		if !tc.HasGenerator() {
			return fmt.Errorf("case %q: data-plane config_update requires a generator to drive traffic", tc.Name)
		}
		if tc.Correctness.MinReceived <= 0 {
			return fmt.Errorf("case %q: data-plane config_update requires correctness.min_received > 0", tc.Name)
		}
	}
	return nil
}

// CCFConfig parameterizes ccf_correctness: the director runs a `ccf` poller device
// against the bench mock CCF API (an endpoints container, hostname ccf-api, running
// vmetric/bench-ccfapi). The driver seeds the mock over its control API, lets the
// director poll + forward to the receiver, and asserts the Scenario.
type CCFConfig struct {
	// Scenario selects what the driver asserts:
	//   pagination   — seed SeedCount events spanning multiple pages; assert all
	//                  SeedCount arrive at the receiver exactly once (no page lost
	//                  or duplicated). Covers the paging type set in the connector.
	//   time_window  — seed SeedCount, let the first poll deliver them, then add
	//                  AddCount newer events; assert exactly AddCount more arrive on
	//                  the next poll (cursor advanced, no re-delivery of the first batch).
	//   auth         — like pagination, but the case exercises an auth mode
	//                  (apikey/basic/bearer/oauth2); correct creds must deliver all.
	//   format       — like pagination, but a csv/gzip response body.
	//   bad_auth     — negative: the device credential mismatches the mock; assert
	//                  NOTHING is delivered (the poller's requests are rejected 401).
	Scenario string `yaml:"scenario"`
	// APIContainer is the endpoints container name running the mock CCF API
	// (default "ccf-api" → docker container "bench-ccf-api").
	APIContainer string `yaml:"api_container"`
	// CtrlPort is the mock's control API port (default 9090).
	CtrlPort int `yaml:"ctrl_port"`
	// SeedCount is how many events to preload before the director first polls.
	SeedCount int `yaml:"seed_count"`
	// AddCount (time_window) is how many newer events to add after the first delivery.
	AddCount int `yaml:"add_count"`
	// ExpectRecords overrides the expected distinct record count at the receiver.
	// Defaults: pagination/auth/format → SeedCount; time_window → SeedCount+AddCount;
	// bad_auth → 0. A pointer so an explicit 0 is honored.
	ExpectRecords *int `yaml:"expect_records"`
	// MarkerPrefix is the substring every delivered record must contain
	// (default "CCFEVT-"); also used as the receiver's required-substring gate.
	MarkerPrefix string `yaml:"marker_prefix"`
	// SettleSeconds is how long to wait for delivery to settle (default 60).
	SettleSeconds int `yaml:"settle_seconds"`
}

// APIContainerOrDefault returns the bench container name of the mock CCF API.
func (c *CCFConfig) APIContainerOrDefault() string {
	name := "ccf-api"
	if c != nil && c.APIContainer != "" {
		name = c.APIContainer
	}
	return "bench-" + name
}

// CtrlPortOrDefault returns the mock's control API port (default 9090).
func (c *CCFConfig) CtrlPortOrDefault() int {
	if c != nil && c.CtrlPort > 0 {
		return c.CtrlPort
	}
	return 9090
}

// MarkerPrefixOrDefault returns the per-record marker substring (default "CCFEVT-").
func (c *CCFConfig) MarkerPrefixOrDefault() string {
	if c != nil && c.MarkerPrefix != "" {
		return c.MarkerPrefix
	}
	return "CCFEVT-"
}

// SettleOrDefault returns SettleSeconds or 60 when unset.
func (c *CCFConfig) SettleOrDefault() int {
	if c == nil || c.SettleSeconds <= 0 {
		return 60
	}
	return c.SettleSeconds
}

// ExpectRecordsOrDefault returns the expected distinct record count for the scenario.
func (c *CCFConfig) ExpectRecordsOrDefault() int {
	if c == nil {
		return 0
	}
	if c.ExpectRecords != nil {
		return *c.ExpectRecords
	}
	switch c.Scenario {
	case "time_window":
		return c.SeedCount + c.AddCount
	case "bad_auth":
		return 0
	default:
		return c.SeedCount
	}
}

// validateCCF checks the optional `ccf:` block and the ccf_correctness type's
// structural requirements.
func (tc *TestCase) validateCCF() error {
	if !tc.IsCCFType() {
		if tc.CCF != nil {
			return fmt.Errorf("case %q: `ccf:` is only valid for type ccf_correctness", tc.Name)
		}
		return nil
	}
	if tc.CCF == nil {
		return fmt.Errorf("case %q: type ccf_correctness requires a `ccf:` block", tc.Name)
	}
	switch tc.CCF.Scenario {
	case "pagination", "time_window", "auth", "format", "bad_auth":
	default:
		return fmt.Errorf("case %q: unknown ccf.scenario %q", tc.Name, tc.CCF.Scenario)
	}
	if tc.CCF.Scenario != "bad_auth" && tc.CCF.SeedCount <= 0 {
		return fmt.Errorf("case %q: ccf.seed_count must be > 0", tc.Name)
	}
	if tc.CCF.Scenario == "time_window" && tc.CCF.AddCount <= 0 {
		return fmt.Errorf("case %q: ccf.add_count must be > 0 for the time_window scenario", tc.Name)
	}
	// expect_records is a pointer so an explicit 0 is honored (bad_auth), but a
	// negative override is meaningless — reject it up front so ExpectRecordsOrDefault
	// only ever yields nil-default or a non-negative count.
	if tc.CCF.ExpectRecords != nil && *tc.CCF.ExpectRecords < 0 {
		return fmt.Errorf("case %q: ccf.expect_records must be >= 0, got %d", tc.Name, *tc.CCF.ExpectRecords)
	}
	// The mock CCF API must be declared as an endpoints container.
	want := tc.CCF.APIContainerOrDefault()[len("bench-"):]
	found := false
	for _, e := range tc.Endpoints {
		if e.Name == want {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("case %q: ccf_correctness requires an endpoints entry named %q (the mock CCF API)", tc.Name, want)
	}
	return nil
}

// HTTPSourceConfig parameterizes http_source_correctness: the bench-httpsender
// (an endpoints container) POSTs Count single-event requests to the director's
// HTTP/Splunk-HEC source; the source forwards to the receiver.
type HTTPSourceConfig struct {
	// Scenario: "deliver" (a valid credential → all Count events reach the
	// receiver) or "reject" (a bad credential → the source rejects every request,
	// nothing is delivered, and the sender records Count rejections).
	Scenario string `yaml:"scenario"`
	// Count is how many requests the sender sends (must match the endpoints
	// HTTPSENDER_COUNT env so the driver knows the expected delivery).
	Count int `yaml:"count"`
	// SenderContainer is the endpoints container running the sender
	// (default "http-sender" → docker container "bench-http-sender").
	SenderContainer string `yaml:"sender_container"`
	// SenderCtrlPort is the sender's /stats control port (default 9099).
	SenderCtrlPort int `yaml:"sender_ctrl_port"`
	// SettleSeconds is how long to wait for delivery to settle (default 60).
	SettleSeconds int `yaml:"settle_seconds"`
}

func (h *HTTPSourceConfig) SenderContainerOrDefault() string {
	name := "http-sender"
	if h != nil && h.SenderContainer != "" {
		name = h.SenderContainer
	}
	return "bench-" + name
}
func (h *HTTPSourceConfig) SenderCtrlPortOrDefault() int {
	if h != nil && h.SenderCtrlPort > 0 {
		return h.SenderCtrlPort
	}
	return 9099
}
func (h *HTTPSourceConfig) SettleOrDefault() int {
	if h == nil || h.SettleSeconds <= 0 {
		return 60
	}
	return h.SettleSeconds
}

// validateHTTPSource checks the optional `http_source:` block and the
// http_source_correctness type's structural requirements.
func (tc *TestCase) validateHTTPSource() error {
	if !tc.IsHTTPSourceType() {
		if tc.HTTPSource != nil {
			return fmt.Errorf("case %q: `http_source:` is only valid for type http_source_correctness", tc.Name)
		}
		return nil
	}
	if tc.HTTPSource == nil {
		return fmt.Errorf("case %q: type http_source_correctness requires an `http_source:` block", tc.Name)
	}
	switch tc.HTTPSource.Scenario {
	case "deliver", "reject", "drop":
	default:
		return fmt.Errorf("case %q: unknown http_source.scenario %q", tc.Name, tc.HTTPSource.Scenario)
	}
	if tc.HTTPSource.Count <= 0 {
		return fmt.Errorf("case %q: http_source.count must be > 0", tc.Name)
	}
	want := tc.HTTPSource.SenderContainerOrDefault()[len("bench-"):]
	found := false
	for _, e := range tc.Endpoints {
		if e.Name == want {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("case %q: http_source_correctness requires an endpoints entry named %q (the bench-httpsender)", tc.Name, want)
	}
	return nil
}

// ClickHouseTargetConfig parameterizes clickhouse_target_correctness: a generator
// drives TCP into the director's clickhouse target (HTTP JSONEachRow) writing to a
// real clickhouse-server endpoint; the driver creates the table then verifies the
// row count by SQL.
type ClickHouseTargetConfig struct {
	// CHContainer is the endpoints container running clickhouse-server
	// (default "clickhouse" → docker container "bench-clickhouse").
	CHContainer string `yaml:"ch_container"`
	// Database / Table the director's target writes to (and the driver creates + queries).
	Database string `yaml:"database"`
	Table    string `yaml:"table"`
	// ExpectRecords is the row count to assert in ClickHouse after the run
	// (normally the generator's total_lines).
	ExpectRecords int `yaml:"expect_records"`
	// SettleSeconds is how long to wait for the row count to reach ExpectRecords (default 60).
	SettleSeconds int `yaml:"settle_seconds"`
	// CreateTableSQL optionally overrides the default `(message, @timestamp, ts)`
	// table DDL — used by custom-table cases to define columns the event JSON maps
	// onto. Use {{TABLE}} as a placeholder for the fully-qualified db.table name.
	CreateTableSQL string `yaml:"create_table_sql"`
	// RestartMidRun, when true, `docker restart`s the clickhouse-server container
	// once mid-run (after the table exists and some rows have landed). The director's
	// CH target must reconnect and its queue must retry the failed batches so the
	// final row count still reaches ExpectRecords — the resilience dimension.
	RestartMidRun bool `yaml:"restart_mid_run"`
}

func (c *ClickHouseTargetConfig) CHContainerOrDefault() string {
	name := "clickhouse"
	if c != nil && c.CHContainer != "" {
		name = c.CHContainer
	}
	return "bench-" + name
}
func (c *ClickHouseTargetConfig) DatabaseOrDefault() string {
	if c != nil && c.Database != "" {
		return c.Database
	}
	return "bench"
}
func (c *ClickHouseTargetConfig) TableOrDefault() string {
	if c != nil && c.Table != "" {
		return c.Table
	}
	return "logs"
}
func (c *ClickHouseTargetConfig) SettleOrDefault() int {
	if c == nil || c.SettleSeconds <= 0 {
		return 60
	}
	return c.SettleSeconds
}

// validateClickHouseTarget checks the optional `clickhouse_target:` block.
func (tc *TestCase) validateClickHouseTarget() error {
	if !tc.IsClickHouseTargetType() {
		if tc.ClickHouseTarget != nil {
			return fmt.Errorf("case %q: `clickhouse_target:` is only valid for type clickhouse_target_correctness", tc.Name)
		}
		return nil
	}
	if tc.ClickHouseTarget == nil {
		return fmt.Errorf("case %q: type clickhouse_target_correctness requires a `clickhouse_target:` block", tc.Name)
	}
	if tc.ClickHouseTarget.ExpectRecords <= 0 {
		return fmt.Errorf("case %q: clickhouse_target.expect_records must be > 0", tc.Name)
	}
	want := tc.ClickHouseTarget.CHContainerOrDefault()[len("bench-"):]
	found := false
	for _, e := range tc.Endpoints {
		if e.Name == want {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("case %q: clickhouse_target_correctness requires an endpoints entry named %q (clickhouse-server)", tc.Name, want)
	}
	return nil
}

// MQTTTargetConfig parameterizes mqtt_target_correctness.
type MQTTTargetConfig struct {
	// SubContainer is the endpoints container running mosquitto_sub that records
	// received messages (default "mqtt-sub" → docker container "bench-mqtt-sub").
	SubContainer string `yaml:"sub_container"`
	// RecvFile is the file inside SubContainer that mosquitto_sub appends to
	// (default "/tmp/recv.txt"); the driver counts its lines.
	RecvFile string `yaml:"recv_file"`
	// ExpectRecords is the message count to assert (normally generator total_lines).
	ExpectRecords int `yaml:"expect_records"`
	// SettleSeconds is how long to wait for the count to settle (default 60).
	SettleSeconds int `yaml:"settle_seconds"`
}

func (c *MQTTTargetConfig) SubContainerOrDefault() string {
	name := "mqtt-sub"
	if c != nil && c.SubContainer != "" {
		name = c.SubContainer
	}
	return "bench-" + name
}
func (c *MQTTTargetConfig) RecvFileOrDefault() string {
	if c != nil && c.RecvFile != "" {
		return c.RecvFile
	}
	return "/tmp/recv.txt"
}
func (c *MQTTTargetConfig) SettleOrDefault() int {
	if c == nil || c.SettleSeconds <= 0 {
		return 60
	}
	return c.SettleSeconds
}

func (tc *TestCase) validateMQTTTarget() error {
	if !tc.IsMQTTTargetType() {
		if tc.MQTTTarget != nil {
			return fmt.Errorf("case %q: `mqtt_target:` is only valid for type mqtt_target_correctness", tc.Name)
		}
		return nil
	}
	if tc.MQTTTarget == nil {
		return fmt.Errorf("case %q: type mqtt_target_correctness requires a `mqtt_target:` block", tc.Name)
	}
	if tc.MQTTTarget.ExpectRecords <= 0 {
		return fmt.Errorf("case %q: mqtt_target.expect_records must be > 0", tc.Name)
	}
	want := tc.MQTTTarget.SubContainerOrDefault()[len("bench-"):]
	found := false
	for _, e := range tc.Endpoints {
		if e.Name == want {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("case %q: mqtt_target_correctness requires an endpoints entry named %q (mosquitto_sub)", tc.Name, want)
	}
	return nil
}

// RedisSourceConfig parameterizes redis_source_correctness.
type RedisSourceConfig struct {
	// PubContainer is the endpoints container that PUBLISHes (default "redis-pub"
	// → docker container "bench-redis-pub"). The driver counts at the bench receiver.
	PubContainer string `yaml:"pub_container"`
	// ExpectRecords is the published/expected message count.
	ExpectRecords int `yaml:"expect_records"`
	// SettleSeconds is how long to wait for delivery (default 60).
	SettleSeconds int `yaml:"settle_seconds"`
	// Reject inverts the verdict for a NEGATIVE test (e.g. wrong redis auth): the
	// source must NOT subscribe, so the driver asserts the receiver count stays
	// <= ExpectMax (default 0) after the settle window. Proves redis auth is enforced.
	Reject    bool `yaml:"reject"`
	ExpectMax int  `yaml:"expect_max"`
}

func (c *RedisSourceConfig) PubContainerOrDefault() string {
	name := "redis-pub"
	if c != nil && c.PubContainer != "" {
		name = c.PubContainer
	}
	return "bench-" + name
}
func (c *RedisSourceConfig) SettleOrDefault() int {
	if c == nil || c.SettleSeconds <= 0 {
		return 60
	}
	return c.SettleSeconds
}

func (tc *TestCase) validateRedisSource() error {
	if !tc.IsRedisSourceType() {
		if tc.RedisSource != nil {
			return fmt.Errorf("case %q: `redis_source:` is only valid for type redis_source_correctness", tc.Name)
		}
		return nil
	}
	if tc.RedisSource == nil {
		return fmt.Errorf("case %q: type redis_source_correctness requires a `redis_source:` block", tc.Name)
	}
	if !tc.RedisSource.Reject && tc.RedisSource.ExpectRecords <= 0 {
		return fmt.Errorf("case %q: redis_source.expect_records must be > 0 (or set reject: true)", tc.Name)
	}
	if tc.RedisSource.Reject && tc.RedisSource.ExpectMax < 0 {
		return fmt.Errorf("case %q: redis_source.expect_max must be >= 0", tc.Name)
	}
	want := tc.RedisSource.PubContainerOrDefault()[len("bench-"):]
	found := false
	for _, e := range tc.Endpoints {
		if e.Name == want {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("case %q: redis_source_correctness requires an endpoints entry named %q (the redis publisher)", tc.Name, want)
	}
	return nil
}

// EndpointSourceConfig parameterizes endpoint_source_correctness — a generic
// driver for any director source the bench generator can't drive (snmptrap, tftp,
// smtp, …). A CLI-sender endpoint feeds the source; the driver counts at the
// receiver and asserts the count reaches expect_min.
type EndpointSourceConfig struct {
	// SenderContainer is the endpoints container running the CLI sender (default
	// "source-sender" → docker container "bench-source-sender"). Its logs are
	// tailed on failure.
	SenderContainer string `yaml:"sender_container"`
	// ExpectMin is the minimum records the receiver must collect. A "min" (not
	// exact) bound tolerates the inherent best-effort loss of UDP senders like
	// snmptrap; set it equal to the sent count for reliable senders (tftp/smtp).
	ExpectMin int `yaml:"expect_min"`
	// SettleSeconds is how long to wait for delivery (default 60).
	SettleSeconds int `yaml:"settle_seconds"`
	// Reject inverts the assertion: a NEGATIVE test (e.g. wrong auth) where the
	// source must NOT deliver. The driver waits the settle window then asserts the
	// receiver count stays <= ExpectMax. Proves auth/validation is load-bearing.
	Reject bool `yaml:"reject"`
	// ExpectMax is the ceiling for a reject case (default 0 — nothing delivered).
	ExpectMax int `yaml:"expect_max"`
}

func (c *EndpointSourceConfig) SenderContainerOrDefault() string {
	name := "source-sender"
	if c != nil && c.SenderContainer != "" {
		name = c.SenderContainer
	}
	return "bench-" + name
}
func (c *EndpointSourceConfig) SettleOrDefault() int {
	if c == nil || c.SettleSeconds <= 0 {
		return 60
	}
	return c.SettleSeconds
}

func (tc *TestCase) validateEndpointSource() error {
	if !tc.IsEndpointSourceType() {
		if tc.EndpointSource != nil {
			return fmt.Errorf("case %q: `endpoint_source:` is only valid for type endpoint_source_correctness", tc.Name)
		}
		return nil
	}
	if tc.EndpointSource == nil {
		return fmt.Errorf("case %q: type endpoint_source_correctness requires an `endpoint_source:` block", tc.Name)
	}
	if !tc.EndpointSource.Reject && tc.EndpointSource.ExpectMin <= 0 {
		return fmt.Errorf("case %q: endpoint_source.expect_min must be > 0 (or set reject: true)", tc.Name)
	}
	if tc.EndpointSource.Reject && tc.EndpointSource.ExpectMax < 0 {
		return fmt.Errorf("case %q: endpoint_source.expect_max must be >= 0", tc.Name)
	}
	want := tc.EndpointSource.SenderContainerOrDefault()[len("bench-"):]
	found := false
	for _, e := range tc.Endpoints {
		if e.Name == want {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("case %q: endpoint_source_correctness requires an endpoints entry named %q (the CLI sender)", tc.Name, want)
	}
	return nil
}

// HTTPVaultRotationConfig parameterizes http_vault_rotation_correctness.
type HTTPVaultRotationConfig struct {
	// SenderContainer is the bench-httpsender endpoints container (default
	// "http-sender" → "bench-http-sender"); CtrlPort is its /stats+/certfp port (9099).
	SenderContainer string `yaml:"sender_container"`
	CtrlPort        int    `yaml:"ctrl_port"`
	// SecretPath is the Vault KV path holding the cert/key (default "bench/http-tls").
	SecretPath string `yaml:"secret_path"`
	// ExpectMin is the minimum records the receiver must collect across the run
	// (delivery must survive the rotation; allows a small loss during reload).
	ExpectMin int `yaml:"expect_min"`
	// SettleSeconds bounds the per-phase waits (default 60).
	SettleSeconds int `yaml:"settle_seconds"`
}

func (c *HTTPVaultRotationConfig) SenderContainerOrDefault() string {
	name := "http-sender"
	if c != nil && c.SenderContainer != "" {
		name = c.SenderContainer
	}
	return "bench-" + name
}
func (c *HTTPVaultRotationConfig) CtrlPortOrDefault() int {
	if c != nil && c.CtrlPort > 0 {
		return c.CtrlPort
	}
	return 9099
}
func (c *HTTPVaultRotationConfig) SecretPathOrDefault() string {
	if c != nil && c.SecretPath != "" {
		return c.SecretPath
	}
	return "bench/http-tls"
}
func (c *HTTPVaultRotationConfig) SettleOrDefault() int {
	if c == nil || c.SettleSeconds <= 0 {
		return 60
	}
	return c.SettleSeconds
}

func (tc *TestCase) validateHTTPVaultRotation() error {
	if !tc.IsHTTPVaultRotationType() {
		if tc.HTTPVaultRotation != nil {
			return fmt.Errorf("case %q: `http_vault_rotation:` is only valid for type http_vault_rotation_correctness", tc.Name)
		}
		return nil
	}
	if tc.HTTPVaultRotation == nil {
		return fmt.Errorf("case %q: type http_vault_rotation_correctness requires an `http_vault_rotation:` block", tc.Name)
	}
	if tc.Vault == nil {
		return fmt.Errorf("case %q: http_vault_rotation_correctness requires a `vault:` block", tc.Name)
	}
	if tc.HTTPVaultRotation.ExpectMin <= 0 {
		return fmt.Errorf("case %q: http_vault_rotation.expect_min must be > 0", tc.Name)
	}
	want := tc.HTTPVaultRotation.SenderContainerOrDefault()[len("bench-"):]
	found := false
	for _, e := range tc.Endpoints {
		if e.Name == want {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("case %q: http_vault_rotation_correctness requires an endpoints entry named %q (the bench-httpsender)", tc.Name, want)
	}
	return nil
}

// validateACLRotation checks the optional `acl_rotation:` block and the
// director_agent_acl_rotation_correctness type's structural requirements: the
// block is required for (and only meaningful to) that type, expect must be
// known, and the case is subject-driven (an `agent:` container that dials in, no
// generator) with a min_received floor for the verdict.
func (tc *TestCase) validateACLRotation() error {
	if !tc.IsDirectorAgentACLRotationType() {
		if tc.ACLRotation != nil {
			return fmt.Errorf("case %q: `acl_rotation:` is only valid for type director_agent_acl_rotation_correctness", tc.Name)
		}
		return nil
	}
	if tc.ACLRotation == nil {
		return fmt.Errorf("case %q: type director_agent_acl_rotation_correctness requires an `acl_rotation:` block", tc.Name)
	}
	switch tc.ACLRotation.Expect {
	case ACLRotationRecover, ACLRotationRevoke:
	default:
		return fmt.Errorf("case %q: acl_rotation.expect %q must be one of %s, %s",
			tc.Name, tc.ACLRotation.Expect, ACLRotationRecover, ACLRotationRevoke)
	}
	if len(tc.ACLRotation.AllowedIPs) == 0 {
		return fmt.Errorf("case %q: acl_rotation.allowed_ips must list the allowlist to rotate to (>= 1 entry)", tc.Name)
	}
	// A blank entry is silently skipped by the backend IP parser, which can
	// collapse the allowlist to empty — read by the director as "no IP
	// restrictions" (firewall off). Fail fast instead of writing it into the
	// rotated acl.allowed_ips.
	for i, ip := range tc.ACLRotation.AllowedIPs {
		if strings.TrimSpace(ip) == "" {
			return fmt.Errorf("case %q: acl_rotation.allowed_ips[%d] must be a non-empty IP or CIDR", tc.Name, i)
		}
	}
	if tc.ACLRotation.SettleSeconds < 0 {
		return fmt.Errorf("case %q: acl_rotation.settle_seconds must be >= 0 (0/unset defaults to 15), got %d", tc.Name, tc.ACLRotation.SettleSeconds)
	}
	if tc.ACLRotation.BaselineSeconds < 0 {
		return fmt.Errorf("case %q: acl_rotation.baseline_seconds must be >= 0 (0/unset defaults to 20), got %d", tc.Name, tc.ACLRotation.BaselineSeconds)
	}
	// Subject-driven via the agent container — no generator. The verdict rests on
	// min_received plus the before/after count behaviour around the rotation.
	if tc.HasGenerator() {
		return fmt.Errorf("case %q: director_agent_acl_rotation_correctness is subject-driven and must not declare a generator", tc.Name)
	}
	if !tc.UsesAgent() {
		return fmt.Errorf("case %q: director_agent_acl_rotation_correctness requires an `agent:` block (the agent container that dials into the director)", tc.Name)
	}
	if tc.Correctness.MinReceived <= 0 {
		return fmt.Errorf("case %q: director_agent_acl_rotation_correctness requires correctness.min_received > 0", tc.Name)
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
	// Source: exactly one of s3_bucket (S3 emulator) / local_dir (shared volume).
	switch {
	case tc.Verifier.S3Bucket == "" && tc.Verifier.LocalDir == "":
		return fmt.Errorf("case %q: verifier requires exactly one of `s3_bucket` or `local_dir`", tc.Name)
	case tc.Verifier.S3Bucket != "" && tc.Verifier.LocalDir != "":
		return fmt.Errorf("case %q: verifier `s3_bucket` and `local_dir` are mutually exclusive", tc.Name)
	}
	// The verifier container only mounts the shared volume at /data, so a
	// local_dir outside it would validate here and then deterministically fail
	// at runtime. Reject it up front. (path, not filepath: these are container
	// paths, always slash-separated; Clean also collapses any ".." traversal.)
	if tc.Verifier.LocalDir != "" {
		clean := path.Clean(tc.Verifier.LocalDir)
		if clean != "/data" && !strings.HasPrefix(clean, "/data/") {
			return fmt.Errorf("case %q: verifier.local_dir %q must be under the shared /data mount", tc.Name, tc.Verifier.LocalDir)
		}
	}
	if tc.Verifier.Format != "avro" && tc.Verifier.Format != "parquet" {
		return fmt.Errorf("case %q: verifier.format must be \"avro\" or \"parquet\", got %q", tc.Name, tc.Verifier.Format)
	}
	// The S3 source needs an emulator; the local source reads the shared volume
	// directly and needs neither aws: nor minio:.
	if !tc.Verifier.IsLocal() && !tc.UsesAWS() && !tc.UsesMinio() {
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

// databaseNameRe constrains the database name, the only DatabaseConfig field
// interpolated directly into a compose-embedded shell command (CREATE
// DATABASE <name>). SeedSQL is mounted as a file and Password flows only
// through a compose `environment:` value expanded by the shell at runtime,
// so neither needs charset restriction (mirrors vaultPathRe/vaultTokenRe).
var databaseNameRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// validateDatabase checks the optional `database:` block: engine must be a
// known DatabaseEngines entry, seeding at least some SQL is mandatory (a
// database nothing seeds is a case-authoring mistake), and the database name
// is charset-restricted.
func (tc *TestCase) validateDatabase() error {
	if tc.Database == nil {
		return nil
	}
	engine, ok := DatabaseEngines[tc.Database.Engine]
	if !ok {
		known := make([]string, 0, len(DatabaseEngines))
		for name := range DatabaseEngines {
			known = append(known, name)
		}
		sort.Strings(known)
		return fmt.Errorf("case %q: unknown database.engine %q, known engines: %s", tc.Name, tc.Database.Engine, strings.Join(known, ", "))
	}
	if tc.Database.SeedSQL == "" {
		return fmt.Errorf("case %q: database block requires `seed_sql`", tc.Name)
	}
	if !databaseNameRe.MatchString(tc.Database.DatabaseOrDefault()) {
		return fmt.Errorf("case %q: database.database %q must match %s", tc.Name, tc.Database.DatabaseOrDefault(), databaseNameRe)
	}
	// Reject database.tls for engines without TLS wiring at load time, rather
	// than letting it fail later during compose rendering. An empty
	// TLSServerCertPath is the registry's "no TLS support" marker (see the
	// DatabaseEngine struct doc and the docker.go compose guard) — keying off it
	// covers any current or future TLS-less engine (e.g. oracle) with no
	// per-engine list.
	if tc.Database.TLS && engine.TLSServerCertPath == "" {
		return fmt.Errorf("case %q: database.tls is not supported by engine %q", tc.Name, tc.Database.Engine)
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
	// 0/unset defaults via SettleSecondsOrDefault/StallSecondsOrDefault; reject
	// negatives so a typo like `settle_seconds: -5` can't be silently defaulted.
	if tc.Rotation.SettleSeconds < 0 {
		return fmt.Errorf("case %q: rotation.settle_seconds must be >= 0 (0/unset defaults to 25), got %d", tc.Name, tc.Rotation.SettleSeconds)
	}
	if tc.Rotation.StallSeconds < 0 {
		return fmt.Errorf("case %q: rotation.stall_seconds must be >= 0 (0/unset defaults to 20), got %d", tc.Name, tc.Rotation.StallSeconds)
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

// SubjectDiskConfig mounts a size-limited tmpfs inside the subject container
// (see TestCase.SubjectDisk). Path is the in-container mount point — for
// vmetric that's /opt/vmetric/storage, the root of its NATS JetStream
// StoreDir and queue/WAL files. Size is a docker-compose tmpfs size string
// ("64m", "1g", or plain bytes). The mount is created mode 01777 so
// non-root subject images can write to it.
type SubjectDiskConfig struct {
	Path string `yaml:"path"`
	Size string `yaml:"size"`
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
	// Reconnect, when true, has each parallel TCP connection redial and keep
	// sending after a transient break (subject reset/OOM under load) until the
	// run ceiling, instead of failing the whole run — the resilience the
	// single-connection path already has. Used by sink-bound performance cases
	// (e.g. clickhouse) where a subject that can't sustain peak rate resets the
	// socket rather than backpressuring cleanly; without it that shows as a run
	// ERROR instead of a measured throughput. Default off — every other case
	// keeps treating a mid-run break as a hard failure.
	Reconnect bool `yaml:"reconnect"`
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
