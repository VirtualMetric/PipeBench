package config

import (
	"fmt"
	"slices"
)

// Subject describes a benchmarked tool and its container configuration.
type Subject struct {
	Name       string
	Image      string
	Version    string
	ConfigPath string            // mount point inside container
	Ports      []string          // internal ports (not exposed to host)
	Command    []string          // optional entrypoint override
	User       string            // optional user override
	Entrypoint []string          // optional entrypoint override
	ConfigRW   bool              // mount config read-write (for tools that write state alongside config)
	Env        map[string]string // default environment variables for the subject

	// CertDir, when set, is an additional in-container path the harness mounts
	// the generated TLS cert dir at FOR THE SUBJECT (the generator always gets
	// them at /certs). Some subjects only accept cert paths inside their own
	// working/config root — vmetric's director resolves device cert_name
	// against its WorkingDir (/opt/vmetric) and rejects paths outside it, so
	// /certs is unreadable; mounting at /opt/vmetric/certs lets the device
	// reference certs/client.crt. Empty = mount only at /certs.
	CertDir string

	// VaultDir, when set, is the in-container path the harness mounts the
	// Vault-generated CA dir at FOR THE SUBJECT (vault cases). Same rationale as
	// CertDir: the director resolves ca_name under its root (/opt/vmetric) and
	// refuses paths outside it, so the CA must be mounted under that root — the
	// old fixed /vault-tls mount (outside the root) is unreadable. Empty = mount
	// at /vault-tls (subjects whose root is "/").
	VaultDir string

	// Capabilities is a free-form set of feature tags the subject is known
	// to support end-to-end through PipeBench (e.g. "tls_tcp", "tls_syslog").
	// The harness consults these when a case opts into a transport that
	// not every subject implements — currently only `generator.tls.enabled`
	// triggers a check. Subjects with no entry are assumed to lack the
	// capability and the case fails fast with a clear error, which is
	// safer than starting and silently producing zero ingest.
	Capabilities []string
}

// HasCapability returns true when the subject declares the named capability.
// The match is case-sensitive; capability names are kept lowercase by
// convention.
func (s Subject) HasCapability(name string) bool {
	return slices.Contains(s.Capabilities, name)
}

// ConfigFile returns the expected filename (basename) of the subject config.
func (s Subject) ConfigFile() string {
	switch s.Name {
	case "vector":
		return "vector.toml"
	case "fluent-bit":
		return "fluent-bit.conf"
	case "fluentd":
		return "fluentd.conf"
	case "logstash":
		return "logstash.conf"
	case "filebeat":
		return "filebeat.yml"
	case "telegraf":
		return "telegraf.conf"
	case "nxlog":
		return "nxlog.conf"
	case "axosyslog":
		return "axosyslog.conf"
	case "tenzir":
		return "tenzir.yaml"
	case "vmetric":
		return "vmetric.yml"
	case "otel-collector":
		return "otel-collector.yaml"
	case "grafana-alloy":
		return "grafana-alloy.alloy"
	case "bindplane-agent":
		return "bindplane-agent.yaml"
	case "cribl-stream":
		return "cribl-stream" // directory, not a file
	case "rotel":
		// Rotel takes no config file — every option is a CLI flag
		// or an env var. Per-case configs ship a wrapper script
		// (configs/rotel.sh) mounted at /entrypoint.sh that sets
		// ROTEL_* env vars and execs rotel.
		return "rotel.sh"
	default:
		return s.Name + ".conf"
	}
}

// ImageRef returns the full image reference with tag.
func (s Subject) ImageRef() string {
	return s.Image + ":" + s.Version
}

// Registry maps subject names to their definitions.
var Registry = map[string]Subject{
	"vector": {
		Name:       "vector",
		Image:      "timberio/vector",
		Version:    "0.54.0-alpine",
		ConfigPath: "/etc/vector/vector.toml",
		Command:    []string{"--config", "/etc/vector/vector.toml"},
		// All AWS sinks + the aws_s3 source take a custom `endpoint`;
		// azure_blob sink takes a connection string (Azurite-ready).
		Capabilities: []string{
			"s3_sink", "s3_source", "azure_blob_sink",
			"sqs_sink", "sns_sink", "kinesis_sink", "cloudwatch_logs_sink",
		},
	},
	"fluent-bit": {
		Name:       "fluent-bit",
		Image:      "fluent/fluent-bit",
		Version:    "5.0",
		ConfigPath: "/fluent-bit/etc/fluent-bit.conf",
		// s3 / azure_blob outputs document a custom `endpoint`. No S3
		// *input* exists in fluent-bit. kinesis_sink/cloudwatch_logs_sink
		// are intentionally NOT declared: fluent-bit 5.0's aws_client
		// hard-enforces TLS certificate verification on custom endpoints
		// (`tls off`/`tls.verify off` are ignored), so it cannot reach
		// LocalStack at all — see the comments in the kinesis/cloudwatch
		// case configs (configs/fluent-bit.conf) for the verification
		// history. Re-add once upstream ships an insecure-TLS option.
		Capabilities: []string{
			"s3_sink", "azure_blob_sink",
		},
	},
	"fluentd": {
		Name:       "fluentd",
		Image:      "fluent/fluentd",
		Version:    "v1.17-debian-1",
		ConfigPath: "/fluentd/etc/fluent.conf",
	},
	"logstash": {
		Name:       "logstash",
		Image:      "docker.elastic.co/logstash/logstash",
		Version:    "8.13.0",
		ConfigPath: "/usr/share/logstash/pipeline/logstash.conf",
		// logstash-integration-aws is bundled: s3 input (bucket polling,
		// `endpoint` + force_path_style), s3/sqs/sns outputs.
		Capabilities: []string{"s3_sink", "s3_source", "sqs_sink", "sns_sink"},
	},
	"filebeat": {
		Name:       "filebeat",
		Image:      "docker.elastic.co/beats/filebeat",
		Version:    "8.13.0",
		ConfigPath: "/usr/share/filebeat/filebeat.yml",
		User:       "root",
		Command:    []string{"-environment", "container", "--strict.perms=false", "-e"},
		// aws-s3 input supports custom endpoints (non_aws_bucket_name);
		// azureblobstorage input (GA since 8.12) takes a storage_url
		// override, so it points at Azurite directly.
		Capabilities: []string{"s3_source", "azure_blob_source"},
	},
	"telegraf": {
		Name:       "telegraf",
		Image:      "telegraf",
		Version:    "1.30-alpine",
		ConfigPath: "/etc/telegraf/telegraf.conf",
		// kinesis + cloudwatch_logs outputs take `endpoint_url`.
		Capabilities: []string{"kinesis_sink", "cloudwatch_logs_sink"},
	},
	"nxlog": {
		Name:       "nxlog",
		Image:      "nxlog/nxlog-ce",
		Version:    "3.2.2329",
		ConfigPath: "/etc/nxlog/nxlog.conf",
	},
	"tenzir": {
		Name:       "tenzir",
		Image:      "ghcr.io/tenzir/tenzir",
		Version:    "v5.30.0",
		ConfigPath: "/etc/tenzir/tenzir.yaml",
		User:       "root",
		Entrypoint: []string{"/opt/tenzir/bin/tenzir-node"},
		Command:    []string{"--config=/etc/tenzir/tenzir.yaml"},
		// No Azure Blob capabilities despite v5.30 shipping the
		// operators: load_azure_blob_storage reads a single blob URI
		// (no glob/watch), and save_azure_blob_storage writes one
		// blob whose block list only commits on pipeline shutdown —
		// verified empirically (8 GB buffered, 0 lines visible). The
		// rotating to_/from_azure_blob_storage operators landed in a
		// later release.
	},
	"axosyslog": {
		Name:       "axosyslog",
		Image:      "ghcr.io/axoflow/axosyslog",
		Version:    "4.25.0",
		ConfigPath: "/etc/syslog-ng/syslog-ng.conf",
	},
	"vmetric": {
		Name:       "vmetric",
		Image:      "vmetric/director",
		Version:    "2.0.6",
		ConfigPath: "/config.yml",
		// The director resolves device TLS cert_name against its WorkingDir
		// (/opt/vmetric) and rejects paths outside it, so mount the harness
		// certs there too; device configs reference /opt/vmetric/certs/*.
		CertDir: "/opt/vmetric/certs",
		// Same reasoning for the Vault CA: mount it under the director root so
		// ca_name can reference it (device configs use /opt/vmetric/vault-tls/*).
		VaultDir: "/opt/vmetric/vault-tls",
		// 2.0.3 moved the binary from /director to /opt/vmetric/director
		// (workdir /opt/vmetric) — keep the config path absolute so the
		// new workdir can't reroute it.
		Entrypoint: []string{"/opt/vmetric/director"},
		Command:    []string{"-config-path", "/config.yml"},
		ConfigRW:   true,
		// Cloud capabilities require a director build with emulator
		// endpoint support (awss3 listener `endpoint`/`use_path_style`,
		// azblob target `connection_string`) — 2.0.3 is the first
		// release that ships it; 2.0.2 starts but ingests nothing.
		//
		// s3_avro_sink / s3_parquet_sink gate the columnar S3 correctness
		// cases. The awss3 target writes Avro (OCF) and Parquet via the
		// shared filesystem-sender machinery (helper/sender/filesystem),
		// so only vmetric declares these for now — other subjects' Avro/
		// Parquet S3 support is unverified, so the `requires:` gate skips
		// them rather than producing a false correctness result.
		Capabilities: []string{
			"tls_tcp",
			"s3_sink", "s3_source", "azure_blob_sink", "azure_blob_source",
			"sqs_sink", "sns_sink", "kinesis_sink", "cloudwatch_logs_sink",
			"s3_avro_sink", "s3_parquet_sink",
		},
	},
	"otel-collector": {
		Name:       "otel-collector",
		Image:      "otel/opentelemetry-collector-contrib",
		Version:    "0.149.0",
		ConfigPath: "/etc/otelcol-contrib/config.yaml",
		// awss3exporter + awscloudwatchlogsexporter take `endpoint`. Note
		// the s3 exporter writes OTLP-JSON batches, not raw lines — keep
		// otel out of exact-count correctness cases.
		Capabilities: []string{"s3_sink", "cloudwatch_logs_sink"},
	},
	"grafana-alloy": {
		Name:       "grafana-alloy",
		Image:      "grafana/alloy",
		Version:    "v1.15.0",
		ConfigPath: "/etc/alloy/config.alloy",
		Command:    []string{"run", "/etc/alloy/config.alloy", "--storage.path=/var/lib/alloy/data", "--stability.level=experimental"},
	},
	"bindplane-agent": {
		Name:       "bindplane-agent",
		Image:      "observiq/bindplane-agent",
		Version:    "1.99.0",
		ConfigPath: "/etc/otel/config.yaml",
	},
	"cribl-stream": {
		Name:       "cribl-stream",
		Image:      "cribl/cribl",
		Version:    "4.17.0",
		ConfigPath: "/opt/cribl/local/cribl",
		ConfigRW:   true,
		// S3/SQS/SNS/Kinesis/CloudWatch destinations + S3 source all take
		// custom endpoints; azure_blob via connection string. The blob
		// *source* consumes EventGrid messages from a storage queue —
		// the bench generator enqueues synthetic ones (Azurite never
		// emits them itself).
		Capabilities: []string{
			"s3_sink", "s3_source", "azure_blob_sink", "azure_blob_source",
			"sqs_sink", "sns_sink", "kinesis_sink", "cloudwatch_logs_sink",
		},
	},
	// rotel — Streamfold's Rust-based OTel collector. No config file:
	// every option is a CLI flag or env var. Per-case configs/rotel.sh
	// is mounted at /entrypoint.sh and execs rotel with case-specific
	// args. Entrypoint overridden so the wrapper script runs even
	// though the upstream image's default entrypoint is rotel itself.
	"rotel": {
		Name:       "rotel",
		Image:      "streamfold/rotel",
		Version:    "v0.2.2",
		ConfigPath: "/entrypoint.sh",
		Entrypoint: []string{"/bin/sh", "/entrypoint.sh"},
	},
}

// Lookup returns a Subject by name or an error if not found.
func Lookup(name string) (Subject, error) {
	s, ok := Registry[name]
	if !ok {
		return Subject{}, fmt.Errorf("unknown subject %q (available: vmetric, vector, fluent-bit, fluentd, logstash, filebeat, telegraf, nxlog, axosyslog, tenzir, otel-collector, grafana-alloy, bindplane-agent, cribl-stream, rotel)", name)
	}
	return s, nil
}

// WithVersion returns a copy of the Subject with the version overridden.
func (s Subject) WithVersion(v string) Subject {
	s.Version = v
	return s
}

// WithImage returns a copy of the Subject with the image repository overridden.
func (s Subject) WithImage(img string) Subject {
	s.Image = img
	return s
}
