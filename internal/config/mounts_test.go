package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// mountCase is a minimal valid case with an agent and one endpoint, so that
// every kind of container a mount can name exists.
func mountCase(mounts map[string]map[string]string) *TestCase {
	return &TestCase{
		Name:      "mounts",
		Type:      "correctness",
		Receiver:  ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Agent:     &AgentConfig{Image: "example/agent"},
		Endpoints: []Endpoint{{Name: "host1", Image: "alpine:3.22"}},
		Mounts:    mounts,
	}
}

func TestValidateMountsAccepts(t *testing.T) {
	tc := mountCase(map[string]map[string]string{
		"subject": {
			"/opt/product/plugins":     "../../shared/plugins",
			"/etc/product/license.lic": "${LICENSE}",
			"/opt/product/bin":         "${TOOLS}/bin:rw",
		},
		"agent": {"/var/lib/agent": "/abs/agent"},
		"host1": {"/etc/app/extra.conf": "files/extra.conf:rw"},
	})
	if err := tc.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got, want := tc.MountVarNames(), []string{"LICENSE", "TOOLS"}; !reflect.DeepEqual(got, want) {
		t.Errorf("MountVarNames = %v, want %v", got, want)
	}
}

func TestValidateMountsRejects(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mounts map[string]map[string]string
		noAgnt bool
		want   string
	}{
		{"unknown container", map[string]map[string]string{"generator": {"/x": "x"}}, false, `not a container this case runs`},
		{"agent without an agent block", map[string]map[string]string{"agent": {"/x": "x"}}, true, `not a container this case runs`},
		{"relative container path", map[string]map[string]string{"subject": {"opt/x": "x"}}, false, `must be absolute`},
		{"root container path", map[string]map[string]string{"subject": {"/": "x"}}, false, `clean path below /`},
		{"trailing slash", map[string]map[string]string{"subject": {"/opt/x/": "x"}}, false, `clean path below /`},
		{"dot dot", map[string]map[string]string{"subject": {"/opt/../x": "x"}}, false, `clean path below /`},
		{"no host path", map[string]map[string]string{"subject": {"/x": ""}}, false, `has no host path`},
		{"only the rw suffix", map[string]map[string]string{"subject": {"/x": ":rw"}}, false, `has no host path`},
		{"bare dollar", map[string]map[string]string{"subject": {"/x": "$HOME/x"}}, false, `not part of a ${NAME}`},
		{"bad name", map[string]map[string]string{"subject": {"/x": "${1BAD}"}}, false, `not part of a ${NAME}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc := mountCase(tt.mounts)
			if tt.noAgnt {
				tc.Agent = nil
			}
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

// TestMountsYAMLRejectsDuplicateTarget pins that the YAML decoder, not the
// harness, refuses two mounts at one container path: the container path is
// the map key.
func TestMountsYAMLRejectsDuplicateTarget(t *testing.T) {
	src := "mounts:\n  subject:\n    /opt/x: a\n    /opt/x: b\n"
	var tc TestCase
	if err := yaml.Unmarshal([]byte(src), &tc); err == nil {
		t.Fatal("decoding two mounts at one container path succeeded")
	}
}

func TestResolveMounts(t *testing.T) {
	root := t.TempDir()
	caseDir := filepath.Join(root, "cases", "mounts")
	shared := filepath.Join(root, "shared")
	tools := filepath.Join(root, "tools")
	for _, dir := range []string{caseDir, filepath.Join(caseDir, "files"), shared, filepath.Join(tools, "bin")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(caseDir, "files", "extra.conf"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	tc := mountCase(map[string]map[string]string{
		"subject": {
			"/opt/product/shared": "../../shared",
			"/opt/product/bin":    "${TOOLS}/bin:rw",
		},
		"host1": {"/etc/app/extra.conf": "files/extra.conf"},
	})
	got, err := tc.ResolveMounts(caseDir, map[string]string{"TOOLS": tools})
	if err != nil {
		t.Fatalf("ResolveMounts: %v", err)
	}
	want := map[string][]ResolvedMount{
		"subject": {
			// Ordered by container path.
			{Source: filepath.Join(tools, "bin"), Target: "/opt/product/bin", Writable: true},
			{Source: shared, Target: "/opt/product/shared"},
		},
		"host1": {{Source: filepath.Join(caseDir, "files", "extra.conf"), Target: "/etc/app/extra.conf"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ResolveMounts =\n%+v\nwant\n%+v", got, want)
	}
}

func TestResolveMountsFails(t *testing.T) {
	caseDir := t.TempDir()
	for _, tt := range []struct {
		name   string
		mounts map[string]map[string]string
		vars   map[string]string
		want   string
	}{
		{"a name the run did not pass", map[string]map[string]string{"subject": {"/x": "${AGENT_DIR}"}}, nil, "pass --mount AGENT_DIR=<host path>"},
		{"a path that does not exist", map[string]map[string]string{"subject": {"/x": "missing"}}, nil, "mounts.subject./x"},
		{"a filled path that does not exist", map[string]map[string]string{"subject": {"/x": "${D}/missing"}}, map[string]string{"D": caseDir}, "missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mountCase(tt.mounts).ResolveMounts(caseDir, tt.vars)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ResolveMounts = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

func TestResolveMountsWithoutMounts(t *testing.T) {
	got, err := mountCase(nil).ResolveMounts("", nil)
	if err != nil || got != nil {
		t.Fatalf("ResolveMounts = %v, %v; want nil, nil", got, err)
	}
}
