package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseMountFlags(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	abs := filepath.Join(t.TempDir(), "agent")

	got, err := parseMountFlags([]string{"AGENT_DIR=" + abs, "LICENSE=files/license.lic"})
	if err != nil {
		t.Fatalf("parseMountFlags: %v", err)
	}
	if got["AGENT_DIR"] != abs {
		t.Errorf("AGENT_DIR = %q, want %q", got["AGENT_DIR"], abs)
	}
	// A relative path is relative to the directory the harness runs in, not
	// to a case's folder.
	if want := filepath.Join(cwd, "files", "license.lic"); got["LICENSE"] != want {
		t.Errorf("LICENSE = %q, want %q", got["LICENSE"], want)
	}

	if got, err := parseMountFlags(nil); err != nil || got != nil {
		t.Errorf("parseMountFlags(nil) = %v, %v; want nil, nil", got, err)
	}
}

func TestParseMountFlagsRejects(t *testing.T) {
	for _, tt := range []struct {
		values []string
		want   string
	}{
		{[]string{"AGENT_DIR"}, "want NAME=<host path>"},
		{[]string{"AGENT_DIR="}, "want NAME=<host path>"},
		{[]string{"1AGENT=/x"}, "not a valid name"},
		{[]string{"AGENT-DIR=/x"}, "not a valid name"},
		{[]string{"A=/x", "A=/y"}, "given twice"},
	} {
		_, err := parseMountFlags(tt.values)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("parseMountFlags(%q) = %v, want an error containing %q", tt.values, err, tt.want)
		}
	}
}
