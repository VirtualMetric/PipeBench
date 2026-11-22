package results

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// A case marked hidden in its case.yaml stays off the results page: the index
// doesn't list it, even when a subject file holds results for it. Its results
// stay in the file.
func TestBuildIndexLeavesOutHiddenCases(t *testing.T) {
	dir := t.TempDir()
	hw := filepath.Join(dir, "c7i.large")
	if err := os.MkdirAll(hw, 0o755); err != nil {
		t.Fatal(err)
	}
	sf := SubjectFile{
		Hardware: "c7i.large",
		Subject:  "vmetric",
		Results: []ResultEntry{
			{Test: "tcp_to_tcp_performance"},
			{Test: "tcp_to_s3_performance"},
		},
	}
	data, err := json.Marshal(sf)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hw, "vmetric.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	catalog := []CatalogEntry{
		{Name: "tcp_to_tcp_performance", Type: "performance"},
		{Name: "tcp_to_s3_performance", Type: "performance", Hidden: true},
		{Name: "tcp_to_sns_performance", Type: "performance", Hidden: true},
	}
	idx, err := BuildIndex(dir, catalog)
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	for _, e := range idx.Tests {
		names = append(names, e.Test)
	}
	if len(names) != 1 || names[0] != "tcp_to_tcp_performance" {
		t.Fatalf("index lists %v, want only tcp_to_tcp_performance", names)
	}
}
