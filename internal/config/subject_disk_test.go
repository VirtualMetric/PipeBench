package config

import (
	"strings"
	"testing"
)

// subjectDiskCase returns a minimal valid case with the given
// subject_disk block, so each test tweaks exactly one dimension.
func subjectDiskCase(disk *SubjectDiskConfig) *TestCase {
	return &TestCase{
		Name:        "disk-validate",
		Type:        "disk_pressure_correctness",
		SubjectDisk: disk,
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

	// No subject_disk block — nothing to validate.
	if err := subjectDiskCase(nil).Validate(); err != nil {
		t.Errorf("nil subject_disk: unexpected error: %v", err)
	}
}
