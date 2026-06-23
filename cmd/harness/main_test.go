package main

import (
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// TestApplyCasePin verifies that a case-level subject pin is applied with the
// correct precedence:
//
//	CLI --image/--version  >  case subject_image/subject_version  >  registry default
func TestApplyCasePin(t *testing.T) {
	t.Run("pin overrides registry default", func(t *testing.T) {
		base := config.Subject{Image: "vmetric/director", Version: "2.0.3"}
		tc := &config.TestCase{
			SubjectImage:   "vmetric/director-enterprise",
			SubjectVersion: "latest",
		}
		got := applyCasePin(base, tc)
		if got.Image != "vmetric/director-enterprise" {
			t.Errorf("image: got %q, want %q", got.Image, "vmetric/director-enterprise")
		}
		if got.Version != "latest" {
			t.Errorf("version: got %q, want %q", got.Version, "latest")
		}
	})

	t.Run("empty pin leaves subject unchanged", func(t *testing.T) {
		base := config.Subject{Image: "vmetric/director", Version: "2.0.3"}
		tc := &config.TestCase{}
		got := applyCasePin(base, tc)
		if got.Image != "vmetric/director" {
			t.Errorf("image: got %q, want %q", got.Image, "vmetric/director")
		}
		if got.Version != "2.0.3" {
			t.Errorf("version: got %q, want %q", got.Version, "2.0.3")
		}
	})

	t.Run("CLI WithImage/WithVersion overrides pin (precedence)", func(t *testing.T) {
		// Simulate: registry default → applyCasePin (enterprise pin) →
		// Subject.WithImage/WithVersion (CLI flags, applied by applySubjectOverrides).
		base := config.Subject{Image: "vmetric/director", Version: "2.0.3"}
		tc := &config.TestCase{
			SubjectImage:   "vmetric/director-enterprise",
			SubjectVersion: "latest",
		}
		pinned := applyCasePin(base, tc)

		// CLI --image=vmetric/director overrides the enterprise pin.
		final := pinned.WithImage("vmetric/director").WithVersion("3.0.0")
		if final.Image != "vmetric/director" {
			t.Errorf("image: got %q, want %q (CLI must win)", final.Image, "vmetric/director")
		}
		if final.Version != "3.0.0" {
			t.Errorf("version: got %q, want %q (CLI must win)", final.Version, "3.0.0")
		}
		// Pin must not mutate the original.
		if pinned.Image != "vmetric/director-enterprise" {
			t.Errorf("pin must not mutate: got %q after WithImage call", pinned.Image)
		}
	})
}
