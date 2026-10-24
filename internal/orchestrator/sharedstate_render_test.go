package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestSharedStateComposeRendersReplicas checks the topology of a
// director_shared_state_correctness case: one service per replica from one
// config, a standby replica behind a compose profile, every replica waiting
// for Azurite's init and capped like the singular subject, Azurite serving
// the case's own account, and the helper containers capped too.
func TestSharedStateComposeRendersReplicas(t *testing.T) {
	srcDir := t.TempDir()
	srcCfg := filepath.Join(srcDir, "vmetric.yml")
	src := "director:\n  id: 7001\nstage: {{@.Stage@}}\nbad: {{@ if eq .NodeID 3 @}}yes{{@ else @}}no{{@ end @}}\n"
	if err := os.WriteFile(srcCfg, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}

	tc := &config.TestCase{
		Name: "director_shared_state_render",
		Type: "director_shared_state_correctness",
		Azure: &config.AzureConfig{
			Containers: []string{"bench-in"},
			Accounts:   []config.AzureAccount{{Name: "statestore", Key: "c2VjcmV0"}},
		},
		Generators: []config.GeneratorConfig{
			{ID: "blob", Mode: "azure_blob", Target: "http://azurite:10000", Rate: 10, Format: "raw"},
		},
		Receivers: []config.ReceiverConfig{{ID: "blob", Mode: "tcp", Listen: ":9001"}},
		SharedState: &config.SharedStateConfig{
			Replicas: 3,
			Standby:  1,
			Streams:  []config.SharedStateStream{{Generator: "blob", Receiver: "blob"}},
		},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	tmp := t.TempDir()
	cfg := RunConfig{
		TestCase:         tc,
		Subject:          config.Subject{Name: "vmetric", Image: "vmetric/director-enterprise", Version: "dev", ConfigPath: "/config.yml"},
		ConfigName:       "default",
		ConfigSrcPath:    srcCfg,
		TmpDir:           tmp,
		GeneratorImage:   "img-gen",
		ReceiverImage:    "img-recv",
		CollectorImage:   "img-coll",
		ReceiverHostPort: 19001,
		CPULimit:         "1.0",
		MemLimit:         "1g",
		HelperCPULimit:   "1",
		HelperMemLimit:   "1g",
	}
	cr, err := NewComposeRunner(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewComposeRunner: %v", err)
	}
	data, err := os.ReadFile(cr.composeFile)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)
	var parsed struct {
		Services map[string]struct {
			Profiles  []string          `yaml:"profiles"`
			DependsOn any               `yaml:"depends_on"`
			Env       map[string]string `yaml:"environment"`
			Deploy    struct {
				Resources struct {
					Limits map[string]string `yaml:"limits"`
				} `yaml:"resources"`
			} `yaml:"deploy"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, out)
	}

	for n, name := range []string{"subject-1", "subject-2", "subject-3", "subject-4"} {
		svc, ok := parsed.Services[name]
		if !ok {
			t.Fatalf("no service %s:\n%s", name, out)
		}
		standby := n == 3
		if got := len(svc.Profiles) > 0; got != standby {
			t.Errorf("%s: standby profile = %v, want %v", name, svc.Profiles, standby)
		}
		if deps, _ := svc.DependsOn.(map[string]any); deps["azure-init"] == nil {
			t.Errorf("%s does not wait for azure-init", name)
		}
		if svc.Deploy.Resources.Limits["cpus"] != "1.0" || svc.Deploy.Resources.Limits["memory"] != "1g" {
			t.Errorf("%s limits = %v, want 1.0 CPU and 1g", name, svc.Deploy.Resources.Limits)
		}
	}
	mustNotContain(t, out, "\n  subject:\n")
	for _, name := range []string{"azurite", "azure-init", "receiver-blob", "generator-blob", "collector"} {
		if l := parsed.Services[name].Deploy.Resources.Limits; l["cpus"] != "1" || l["memory"] != "1g" {
			t.Errorf("helper %s limits = %v, want 1 CPU and 1g", name, l)
		}
	}
	if got := parsed.Services["azurite"].Env["AZURITE_ACCOUNTS"]; got != config.AzuriteAccount+":"+config.AzuriteKey+";statestore:c2VjcmV0" {
		t.Errorf("AZURITE_ACCOUNTS = %q", got)
	}
	// Loss is counted on the records' sequence numbers. Without them a line
	// that arrived cut counts as a distinct record and hides a lost one.
	if got := parsed.Services["receiver-blob"].Env["RECEIVER_TRACK_SEQ"]; got != "true" {
		t.Errorf("receiver-blob RECEIVER_TRACK_SEQ = %q, want true", got)
	}

	// One config per replica, the same apart from what a case asks for.
	read := func(n int) string {
		b, err := os.ReadFile(replicaConfigPath(tmp, n))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if !strings.Contains(read(1), "stage: 1") || !strings.Contains(read(1), "bad: no") || !strings.Contains(read(3), "bad: yes") {
		t.Errorf("first render: replica 1 %q, replica 3 %q", read(1), read(3))
	}
	if err := cr.RenderReplicaConfigs(2); err != nil {
		t.Fatalf("RenderReplicaConfigs: %v", err)
	}
	for n := 1; n <= 4; n++ {
		if !strings.Contains(read(n), "stage: 2") {
			t.Errorf("replica %d after the second stage: %q", n, read(n))
		}
	}
}

// TestHelperLimitsOffByDefault guards every other case: without helper limits
// the compose file caps nothing but what it capped before.
func TestHelperLimitsOffByDefault(t *testing.T) {
	tc := &config.TestCase{
		Name:      "azure-smoke",
		Type:      "performance",
		Duration:  "30s",
		Azure:     &config.AzureConfig{Containers: []string{"bench-out"}},
		Generator: config.GeneratorConfig{Mode: "tcp", Target: "subject:9000", Rate: 100, Format: "raw"},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	tmp := t.TempDir()
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.3", ConfigPath: "/config.yml"},
		ConfigName: "default", ConfigSrcPath: composePath, TmpDir: tmp,
		GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll", ReceiverHostPort: 19001,
	}
	if err := writeCompose(composePath, cfg); err != nil {
		t.Fatalf("writeCompose: %v", err)
	}
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	mustNotContain(t, string(data), "deploy:")
	mustNotContain(t, string(data), "AZURITE_ACCOUNTS")
	mustNotContain(t, string(data), "profiles:")
}

// TestSharedStateComposeRendersOtherStores checks the stores beside Azurite:
// a share is one volume every replica mounts, made writable by an init the
// replicas wait for; MinIO comes with its init; and every container the
// template does not cap itself, Redis included, is held to the helper limits.
func TestSharedStateComposeRendersOtherStores(t *testing.T) {
	render := func(t *testing.T, change func(tc *config.TestCase)) map[string]map[string]any {
		t.Helper()
		srcCfg := filepath.Join(t.TempDir(), "vmetric.yml")
		if err := os.WriteFile(srcCfg, []byte("director:\n  id: 7001\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		tc := &config.TestCase{
			Name:       "director_shared_state_store_render",
			Type:       "director_shared_state_correctness",
			Azure:      &config.AzureConfig{Containers: []string{"bench-in"}},
			Generators: []config.GeneratorConfig{{ID: "blob", Mode: "azure_blob", Target: "http://azurite:10000", Rate: 10, Format: "raw"}},
			Receivers:  []config.ReceiverConfig{{ID: "blob", Mode: "tcp", Listen: ":9001"}},
			SharedState: &config.SharedStateConfig{
				Replicas: 2,
				Streams:  []config.SharedStateStream{{Generator: "blob", Receiver: "blob"}},
			},
		}
		change(tc)
		if err := tc.Validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		cr, err := NewComposeRunner(context.Background(), RunConfig{
			TestCase: tc, Subject: config.Subject{Name: "vmetric", Image: "vmetric/director-enterprise", Version: "dev", ConfigPath: "/config.yml"},
			ConfigName: "default", ConfigSrcPath: srcCfg, TmpDir: t.TempDir(),
			GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll", ReceiverHostPort: 19001,
			CPULimit: "1.0", MemLimit: "1g", HelperCPULimit: "1", HelperMemLimit: "1g",
		})
		if err != nil {
			t.Fatalf("NewComposeRunner: %v", err)
		}
		data, err := os.ReadFile(cr.composeFile)
		if err != nil {
			t.Fatal(err)
		}
		var parsed struct {
			Services map[string]map[string]any `yaml:"services"`
		}
		if err := yaml.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("rendered compose is not valid YAML: %v\n%s", err, data)
		}
		return parsed.Services
	}
	capped := func(svc map[string]any) bool {
		if svc["cpus"] != nil && svc["mem_limit"] != nil {
			return true
		}
		deploy, _ := svc["deploy"].(map[string]any)
		resources, _ := deploy["resources"].(map[string]any)
		limits, _ := resources["limits"].(map[string]any)
		return limits["cpus"] != nil && limits["memory"] != nil
	}

	t.Run("share", func(t *testing.T) {
		services := render(t, func(tc *config.TestCase) {
			tc.SharedState.Store = config.SharedStateStoreShare
			tc.SharedState.SharePath = "/mnt/vm-state"
		})
		for _, name := range []string{"subject-1", "subject-2"} {
			volumes, _ := services[name]["volumes"].([]any)
			if !slices.Contains(volumes, any("witness-dir:/mnt/vm-state")) {
				t.Errorf("%s volumes = %v, want the share at /mnt/vm-state", name, volumes)
			}
			if deps, _ := services[name]["depends_on"].(map[string]any); deps["witness-init"] == nil {
				t.Errorf("%s does not wait for the share's init", name)
			}
		}
		if svc := services["witness-init"]; svc == nil || !capped(svc) {
			t.Errorf("witness-init = %v, want it rendered and capped", svc)
		}
	})
	t.Run("s3", func(t *testing.T) {
		services := render(t, func(tc *config.TestCase) {
			tc.Minio = &config.MinioConfig{Buckets: []string{"director-state"}}
			tc.SharedState.Store = config.SharedStateStoreS3
		})
		for _, name := range []string{"minio", "minio-init"} {
			if svc := services[name]; svc == nil || !capped(svc) {
				t.Errorf("%s = %v, want it rendered and capped", name, svc)
			}
		}
		if deps, _ := services["subject-1"]["depends_on"].(map[string]any); deps["minio-init"] == nil {
			t.Error("subject-1 does not wait for minio-init")
		}
	})
	t.Run("redis", func(t *testing.T) {
		services := render(t, func(tc *config.TestCase) {
			tc.Endpoints = []config.Endpoint{{Name: "redis", Image: "redis:7-alpine"}}
			tc.SharedState.Store = config.SharedStateStoreRedis
		})
		if svc := services["redis"]; svc == nil || !capped(svc) {
			t.Errorf("redis = %v, want it rendered and capped", svc)
		}
		// The replicas are the subject, with limits of their own.
		if svc := services["subject-1"]; svc["cpus"] != nil {
			t.Errorf("subject-1 was given the helper limits: %v", svc)
		}
	})
}
