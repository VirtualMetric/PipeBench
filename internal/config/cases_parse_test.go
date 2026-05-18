package config

import (
	"path/filepath"
	"testing"
)

// TestEveryCaseParses guards against the additive schema changes
// breaking any existing case.yaml. Every directory under cases/ that
// contains a case.yaml must still load and validate cleanly.
func TestEveryCaseParses(t *testing.T) {
	repoRoot, err := filepath.Abs("../../")
	if err != nil {
		t.Fatal(err)
	}
	casesDir := filepath.Join(repoRoot, "cases")
	names, err := ListCases(casesDir)
	if err != nil {
		t.Fatalf("list cases: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("expected at least one case under cases/")
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadCase(casesDir, name); err != nil {
				t.Fatalf("load %s: %v", name, err)
			}
		})
	}
}
