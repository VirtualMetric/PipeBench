package config

import (
	"strings"
	"testing"
)

// subjectDiskCase returns a minimal valid case with the given
// subject_disk block, so each test tweaks exactly one dimension. The
// generator is required by the disk_pressure_correctness type check, so
// it is part of the baseline rather than the dimension under test.
func subjectDiskCase(disk *SubjectDiskConfig) *TestCase {
	return &TestCase{
		Name:        "disk-validate",
		Type:        "disk_pressure_correctness",
		SubjectDisk: disk,
		Generator:   GeneratorConfig{Mode: "syslog", Target: "subject:514"},
	}
}

func TestValidateSubjectDisk(t *testing.T) {
	valid := []SubjectDiskConfig{
		{Path: "/opt/vmetric/storage", Size: "64m"},
		{Path: "/data", Size: "128MB"},
		{Path: "/data", Size: "1g"},
		{Path: "/data", Size: "512kb"},
		{Path: "/data", Size: "1048576"},
		{Path: "/data", Size: "64b"},
	}
	for _, d := range valid {
		if err := subjectDiskCase(&d).Validate(); err != nil {
			t.Errorf("subject_disk %+v: unexpected error: %v", d, err)
		}
	}

	invalid := []struct {
		disk    SubjectDiskConfig
		wantSub string
	}{
		{SubjectDiskConfig{Path: "", Size: "64m"}, "requires both"},
		{SubjectDiskConfig{Path: "/data", Size: ""}, "requires both"},
		{SubjectDiskConfig{Path: "data", Size: "64m"}, "must be absolute"},
		{SubjectDiskConfig{Path: "/data", Size: "sixty-four"}, "not a valid tmpfs size"},
		{SubjectDiskConfig{Path: "/data", Size: "1.5g"}, "not a valid tmpfs size"},
		{SubjectDiskConfig{Path: "/data", Size: "64bb"}, "not a valid tmpfs size"},
		{SubjectDiskConfig{Path: "/data", Size: "-5m"}, "not a valid tmpfs size"},
		{SubjectDiskConfig{Path: "/data", Size: "64x"}, "not a valid tmpfs size"},
		// Zero sizes: the shared parser rejects them, so Validate must
		// too — otherwise a case that loads cleanly blows up at
		// compose-render time.
		{SubjectDiskConfig{Path: "/data", Size: "0"}, "not a valid tmpfs size"},
		{SubjectDiskConfig{Path: "/data", Size: "0m"}, "not a valid tmpfs size"},
		{SubjectDiskConfig{Path: "/data", Size: "0b"}, "not a valid tmpfs size"},
		{SubjectDiskConfig{Path: "/data", Size: "0kb"}, "not a valid tmpfs size"},
	}
	for _, tt := range invalid {
		err := subjectDiskCase(&tt.disk).Validate()
		if err == nil {
			t.Errorf("subject_disk %+v: expected error containing %q, got nil", tt.disk, tt.wantSub)
			continue
		}
		if !strings.Contains(err.Error(), tt.wantSub) {
			t.Errorf("subject_disk %+v: error %q does not contain %q", tt.disk, err, tt.wantSub)
		}
	}

	// No subject_disk block on a type that doesn't require one — nothing
	// to validate.
	plain := subjectDiskCase(nil)
	plain.Type = "correctness"
	if err := plain.Validate(); err != nil {
		t.Errorf("nil subject_disk: unexpected error: %v", err)
	}
}

// TestValidateSubjectDiskRejectsCluster covers the mount only rendering on
// the singular subject service: paired with cluster: it would silently
// no-op and hand every node the host's full volume.
func TestValidateSubjectDiskRejectsCluster(t *testing.T) {
	tc := subjectDiskCase(&SubjectDiskConfig{Path: "/data", Size: "64m"})
	tc.Cluster = &ClusterConfig{}
	err := tc.Validate()
	if err == nil {
		t.Fatal("subject_disk + cluster: expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "cannot be combined with `cluster:`") {
		t.Errorf("subject_disk + cluster: unexpected error %q", err)
	}
}

// TestValidateDiskPressureRequirements covers the per-type preconditions:
// without them a disk_pressure_correctness run reports PASS against an
// unlimited volume, or starts a generator service that was never rendered.
func TestValidateDiskPressureRequirements(t *testing.T) {
	noDisk := subjectDiskCase(nil)
	err := noDisk.Validate()
	if err == nil || !strings.Contains(err.Error(), "requires a `subject_disk:` block") {
		t.Errorf("disk_pressure_correctness without subject_disk: got %v, want a subject_disk requirement error", err)
	}

	noGen := subjectDiskCase(&SubjectDiskConfig{Path: "/data", Size: "64m"})
	noGen.Generator = GeneratorConfig{}
	err = noGen.Validate()
	if err == nil || !strings.Contains(err.Error(), "requires a `generator:` block") {
		t.Errorf("disk_pressure_correctness without a generator: got %v, want a generator requirement error", err)
	}

	// The plural form satisfies the same requirement.
	pluralGen := subjectDiskCase(&SubjectDiskConfig{Path: "/data", Size: "64m"})
	pluralGen.Generator = GeneratorConfig{}
	pluralGen.Generators = []GeneratorConfig{{ID: "a", Mode: "syslog", Target: "subject:514"}}
	if err := pluralGen.Validate(); err != nil {
		t.Errorf("disk_pressure_correctness with generators[]: unexpected error: %v", err)
	}
}
