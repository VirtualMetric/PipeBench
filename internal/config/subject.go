package config

import "fmt"

// Subject describes a benchmarked tool and its container configuration.
type Subject struct {
	Name       string
	Image      string
	Version    string
	ConfigPath string   // mount point inside container
	Ports      []string // internal ports (not exposed to host)
	Command    []string // optional entrypoint override
	User       string            // optional user override
	Entrypoint []string          // optional entrypoint override
	ConfigRW   bool              // mount config read-write (for tools that write state alongside config)
	Env        map[string]string // default environment variables for the subject
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
	case "axosyslog":
		return "axosyslog.conf"
	case "vmetric":
		return "vmetric.yml"
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
	"axosyslog": {
		Name:       "axosyslog",
		Image:      "ghcr.io/axoflow/axosyslog",
		Version:    "4.24.0",
		ConfigPath: "/etc/syslog-ng/syslog-ng.conf",
	},
	"vmetric": {
		Name:       "vmetric",
		Image:      "vmetric/director",
		Version:    "latest",
		ConfigPath: "/config.yml",
		Entrypoint: []string{"/director"},
		Command:    []string{"-config-path", "config.yml"},
		ConfigRW:   true,
	},
}

// Lookup returns a Subject by name or an error if not found.
func Lookup(name string) (Subject, error) {
	s, ok := Registry[name]
	if !ok {
		return Subject{}, fmt.Errorf("unknown subject %q (available: vmetric, vector, fluent-bit, fluentd, logstash, axosyslog)", name)
	}
	return s, nil
}

// WithVersion returns a copy of the Subject with the version overridden.
func (s Subject) WithVersion(v string) Subject {
	s.Version = v
	return s
}
