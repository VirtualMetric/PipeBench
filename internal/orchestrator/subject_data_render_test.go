package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"gopkg.in/yaml.v3"
)

// A run with a subject data folder mounts that host folder as the subject's data folder, and a run
// without one mounts nothing there.
func TestTheSubjectDataFolderIsMountedOnlyWhenSet(t *testing.T) {
	tc := &config.TestCase{
		Name: "latency", Type: "performance", Duration: "10s",
		Generator: config.GeneratorConfig{Mode: "http_ack", Target: "http://subject:9000/", LineSize: 256},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.7.1", ConfigPath: "/config.yml"}
	render := func(host string) string {
		t.Helper()
		tmp := t.TempDir()
		composePath := filepath.Join(tmp, "compose.yaml")
		cfg := RunConfig{
			TestCase: tc, Subject: subj, ConfigName: "default", ConfigSrcPath: composePath, TmpDir: tmp,
			GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll", ReceiverHostPort: 19001,
			SubjectDataHost: host, SubjectDataDst: "/opt/vmetric/storage",
		}
		if err := writeCompose(composePath, cfg); err != nil {
			t.Fatalf("writeCompose: %v", err)
		}
		data, err := os.ReadFile(composePath)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	with := render("/mnt/nvme/subject-data/latency-1")
	if !strings.Contains(with, `- "/mnt/nvme/subject-data/latency-1:/opt/vmetric/storage"`) {
		t.Fatalf("the compose file does not mount the data folder:\n%s", with)
	}
	if without := render(""); strings.Contains(without, "/opt/vmetric/storage") {
		t.Fatalf("a run without a data folder mounts one:\n%s", without)
	}
}

// With a subject data folder, each node of a cluster mounts a folder of its own in it as its data
// folder, which the harness makes before the nodes start. Without one, no node mounts anything there.
func TestEachClusterNodeMountsItsOwnDataFolder(t *testing.T) {
	src := "director:\n  id: {{@.NodeID@}}\n"
	render := func(host string) (map[string]any, string) {
		t.Helper()
		tmp, srcDir := t.TempDir(), t.TempDir()
		srcPath := filepath.Join(srcDir, "vmetric.yml")
		if err := os.WriteFile(srcPath, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		subj := config.Subject{Name: "vmetric", Image: "vmetric/director-enterprise", Version: "dev", ConfigPath: "/config.yml"}
		composePath := filepath.Join(tmp, "compose.yaml")
		cfg := RunConfig{
			TestCase: scenarioCase(&config.ScenarioConfig{Steps: []config.ScenarioStep{{Do: "wait_ready"}}}), Subject: subj,
			ConfigName: "default", ConfigSrcPath: srcPath, TmpDir: tmp,
			GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll", ReceiverHostPort: 19001,
			SubjectDataHost: host, SubjectDataDst: "/opt/vmetric/storage",
		}
		if err := writeCompose(composePath, cfg); err != nil {
			t.Fatalf("writeCompose: %v", err)
		}
		raw, err := os.ReadFile(composePath)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("the rendered compose file is not YAML: %v\n%s", err, raw)
		}
		return doc, string(raw)
	}

	host := filepath.Join(t.TempDir(), "latency-1")
	doc, _ := render(host)
	for _, node := range []string{"subject-1", "subject-2", "subject-3"} {
		want := filepath.ToSlash(filepath.Join(host, node)) + ":/opt/vmetric/storage"
		found := false
		for _, v := range service(t, doc, node)["volumes"].([]any) {
			if v == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s does not mount %s: %v", node, want, service(t, doc, node)["volumes"])
		}
		fi, err := os.Stat(filepath.Join(host, node))
		if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o777 {
			t.Fatalf("%s's folder: %v %v", node, fi, err)
		}
	}
	if _, raw := render(""); strings.Contains(raw, "/opt/vmetric/storage") {
		t.Fatalf("a cluster without a data folder mounts one:\n%s", raw)
	}
}
