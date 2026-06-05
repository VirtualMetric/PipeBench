package config

import (
	"fmt"
	"os"
	"path/filepath"
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

	Subjects       []string                 `yaml:"subjects"`
	Configurations map[string]Configuration `yaml:"configurations"`
	Correctness    CorrectnessConfig        `yaml:"correctness"`
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
	Mode        string `yaml:"mode"`        // "tcp" | "file" | "http" | "udp" | "udp_netflow_v5" | "otlp"
	Target      string `yaml:"target"`      // "subject:9000" or file path
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
	Mode   string `yaml:"mode"`   // "tcp" | "file" | "http" | "otlp"
	Listen string `yaml:"listen"` // ":9001" or file path
}

type Configuration struct {
	Description string            `yaml:"description"`
	Env         map[string]string `yaml:"env"`
}

type CorrectnessConfig struct {
	ValidateDedup bool `yaml:"validate_dedup"`
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

	// DrainSeconds extends how long the harness waits for backlog to
	// arrive after the generator(s) stop. The case still finishes early if
	// the receiver stays quiet for DrainQuietWindow. Useful for cases that
	// validate throttled output draining a queue (the receiver is still
	// arriving long after the generator is done).
	DrainSeconds     int    `yaml:"drain_seconds"`
	DrainQuietWindow string `yaml:"drain_quiet_window"`

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
