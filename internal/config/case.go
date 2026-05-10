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
	Name           string                   `yaml:"name"`
	Type           string                   `yaml:"type"` // "performance" | "correctness"
	Description    string                   `yaml:"description"`
	Duration       string                   `yaml:"duration"`
	Warmup         string                   `yaml:"warmup"`
	DrainGrace     string                   `yaml:"drain_grace"`
	Generator      GeneratorConfig          `yaml:"generator"`
	Receiver       ReceiverConfig           `yaml:"receiver"`
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

type GeneratorConfig struct {
	Mode        string            `yaml:"mode"`        // "tcp" | "file" | "http" | "udp_netflow_v5" | "otlp"
	Target      string            `yaml:"target"`      // "subject:9000" or file path
	Rate        int               `yaml:"rate"`        // lines/sec per connection, 0 = unlimited
	TotalLines  int64             `yaml:"total_lines"` // total lines to send, 0 = use duration
	LineSize    int               `yaml:"line_size"`   // bytes per line
	Format      string            `yaml:"format"`      // "raw" | "syslog" | "json"
	Connections int               `yaml:"connections"` // parallel connections (default 1)
	// Env is mode-specific extra env passed straight through to the
	// generator container (e.g. GENERATOR_OTLP_TRANSPORT=grpc). Lets a
	// case dial in transport variants without growing GeneratorConfig
	// for every new mode-specific knob.
	Env         map[string]string `yaml:"env"`
}

type ReceiverConfig struct {
	Mode   string `yaml:"mode"`   // "tcp" | "file" | "http" | "otlp"
	Listen string `yaml:"listen"` // ":9001" or file path
}

type Configuration struct {
	Description string            `yaml:"description"`
	Env         map[string]string `yaml:"env"`
}

type CorrectnessConfig struct {
	ValidateOrder bool `yaml:"validate_order"`
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
}

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
