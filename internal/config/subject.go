package config

import (
	"fmt"
	"strings"
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
	for _, c := range s.Capabilities {
		if c == name {
			return true
		}
	}
	return false
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
	},
	"fluent-bit": {
		Name:       "fluent-bit",
		Image:      "fluent/fluent-bit",
		Version:    "5.0",
		ConfigPath: "/fluent-bit/etc/fluent-bit.conf",
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
	},
	"filebeat": {
		Name:       "filebeat",
		Image:      "docker.elastic.co/beats/filebeat",
		Version:    "8.13.0",
		ConfigPath: "/usr/share/filebeat/filebeat.yml",
		User:       "root",
		Command:    []string{"-environment", "container", "--strict.perms=false", "-e"},
	},
	"telegraf": {
		Name:       "telegraf",
		Image:      "telegraf",
		Version:    "1.30-alpine",
		ConfigPath: "/etc/telegraf/telegraf.conf",
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
	},
	"axosyslog": {
		Name:       "axosyslog",
		Image:      "ghcr.io/axoflow/axosyslog",
		Version:    "4.25.0",
		ConfigPath: "/etc/syslog-ng/syslog-ng.conf",
	},
	"vmetric": {
		Name:         "vmetric",
		Image:        "vmetric/director",
		Version:      "2.0.1",
		ConfigPath:   "/config.yml",
		Entrypoint:   []string{"/director"},
		Command:      []string{"-config-path", "config.yml"},
		ConfigRW:     true,
		Capabilities: []string{"tls_tcp"},
	},
	"otel-collector": {
		Name:       "otel-collector",
		Image:      "otel/opentelemetry-collector-contrib",
		Version:    "0.149.0",
		ConfigPath: "/etc/otelcol-contrib/config.yaml",
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

// WithImage returns a copy of the Subject with the image name overridden.
// The override accepts an optional ":tag" suffix; when present the tag is
// split off into Version so ImageRef() still produces a single "image:tag"
// reference. This lets a caller point a subject at an alternate build of the
// same tool (e.g. a locally built image) without disturbing the Name-keyed
// config/capability plumbing, which all keys off Name, not the image.
func (s Subject) WithImage(image string) Subject {
	if i := strings.LastIndex(image, ":"); i >= 0 && !strings.Contains(image[i+1:], "/") {
		s.Version = image[i+1:]
		image = image[:i]
	}
	s.Image = image
	return s
}
