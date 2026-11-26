package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"gopkg.in/yaml.v3"
)

// composeBinds returns the bind mounts compose would give service, as
// target -> (source, read_only).
func composeBinds(t *testing.T, compose []byte, service string) map[string][2]any {
	t.Helper()
	var doc struct {
		Services map[string]struct {
			Volumes []any `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(compose, &doc); err != nil {
		t.Fatalf("the rendered compose file is not valid YAML: %v\n%s", err, compose)
	}
	svc, ok := doc.Services[service]
	if !ok {
		t.Fatalf("no service %q in:\n%s", service, compose)
	}
	binds := map[string][2]any{}
	for _, v := range svc.Volumes {
		m, ok := v.(map[string]any)
		if !ok || m["type"] != "bind" {
			continue
		}
		binds[m["target"].(string)] = [2]any{m["source"], m["read_only"]}
	}
	return binds
}

// mountFixture makes a case folder holding a file and a folder whose name has
// a space and a "$", the two things a bind mount's source must survive.
func mountFixture(t *testing.T) (caseDir, pkgDir, conf string) {
	t.Helper()
	root := t.TempDir()
	caseDir = filepath.Join(root, "cases", "with mounts")
	pkgDir = filepath.Join(root, "pack $age")
	for _, d := range []string{caseDir, pkgDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	conf = filepath.Join(caseDir, "extra.conf")
	if err := os.WriteFile(conf, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return caseDir, pkgDir, conf
}

func renderMounts(t *testing.T, tc *config.TestCase, caseDir string, vars map[string]string) []byte {
	t.Helper()
	if err := tc.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	mounts, err := tc.ResolveMounts(caseDir, vars)
	if err != nil {
		t.Fatalf("ResolveMounts: %v", err)
	}
	tc.RunMounts = mounts

	tmp := t.TempDir()
	src := filepath.Join(t.TempDir(), "vmetric.yml")
	if err := os.WriteFile(src, []byte("director:\n  id: {{@.NodeID@}}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, ConfigName: "default", ConfigSrcPath: src, CaseDir: caseDir, TmpDir: tmp,
		Subject:        config.Subject{Name: "product", Image: "example/product", Version: "1.0.0", ConfigPath: "/config.yml"},
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
	return data
}

func TestComposeRendersCaseMounts(t *testing.T) {
	caseDir, pkgDir, conf := mountFixture(t)
	tc := &config.TestCase{
		Name:      "with mounts",
		Type:      "correctness",
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Agent:     &config.AgentConfig{Image: "example/agent", MountsSharedData: true},
		Endpoints: []config.Endpoint{{Name: "host1", Image: "alpine:3.22"}},
		Mounts: map[string]map[string]string{
			"subject": {
				"/opt/product/package": "${PKG}",
				"/etc/product/extra":   "extra.conf:rw",
			},
			"agent": {"/opt/agent/package": "${PKG}"},
			"host1": {"/etc/host/extra.conf": "extra.conf"},
		},
	}
	out := renderMounts(t, tc, caseDir, map[string]string{"PKG": pkgDir})

	// Compose reads "$$" as one literal "$".
	pkg := strings.ReplaceAll(filepath.ToSlash(pkgDir), "$", "$$")
	extra := filepath.ToSlash(conf)

	for _, check := range []struct {
		service, target, source string
		readOnly                bool
	}{
		{"subject", "/opt/product/package", pkg, true},
		{"subject", "/etc/product/extra", extra, false},
		{"agent", "/opt/agent/package", pkg, true},
		{"host1", "/etc/host/extra.conf", extra, true},
	} {
		got, ok := composeBinds(t, out, check.service)[check.target]
		if !ok {
			t.Errorf("%s: no bind mount at %s:\n%s", check.service, check.target, out)
			continue
		}
		if got[0] != check.source || got[1] != check.readOnly {
			t.Errorf("%s %s = source %v, read_only %v; want %s, %v", check.service, check.target, got[0], got[1], check.source, check.readOnly)
		}
	}
	// The agent keeps its shared-data volume next to its mounts.
	mustContain(t, string(out), `- "shared-data:/data"`)
}

func TestComposeRendersCaseMountsOnEveryNode(t *testing.T) {
	caseDir, pkgDir, _ := mountFixture(t)
	tc := &config.TestCase{
		Name:     "with mounts",
		Type:     "director_cluster_correctness",
		Cluster:  &config.ClusterConfig{Nodes: 3, Action: "restart_leader"},
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Mounts:   map[string]map[string]string{"subject": {"/opt/product/package": "${PKG}"}},
	}
	out := renderMounts(t, tc, caseDir, map[string]string{"PKG": pkgDir})
	for _, node := range []string{"subject-1", "subject-2", "subject-3"} {
		if _, ok := composeBinds(t, out, node)["/opt/product/package"]; !ok {
			t.Errorf("%s has no bind mount at /opt/product/package:\n%s", node, out)
		}
	}
}

// TestComposeRefusesUnresolvedMounts pins the guard against a run path that
// writes a compose file without resolving the case's mounts: it must fail, not
// start the containers without them.
func TestComposeRefusesUnresolvedMounts(t *testing.T) {
	tmp := t.TempDir()
	tc := &config.TestCase{
		Name:     "with mounts",
		Type:     "correctness",
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Mounts:   map[string]map[string]string{"subject": {"/opt/x": "x"}},
	}
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, ConfigName: "default", ConfigSrcPath: composePath, TmpDir: tmp,
		Subject:        config.Subject{Name: "product", Image: "example/product", ConfigPath: "/config.yml"},
		GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll",
	}
	err := writeCompose(composePath, cfg)
	if err == nil || !strings.Contains(err.Error(), "not resolved") {
		t.Fatalf("writeCompose = %v, want the unresolved-mounts error", err)
	}
}
