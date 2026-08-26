package main

import (
	"bufio"
	"io"
	"strings"
	"testing"
	"time"
)

// chunkedReader yields one fixed chunk per Read call with a small delay
// between chunks, so consecutive reads land on distinct timestamps.
type chunkedReader struct {
	chunks []string
	delay  time.Duration
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if len(c.chunks) == 0 {
		return 0, io.EOF
	}
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	n := copy(p, c.chunks[0])
	if n == len(c.chunks[0]) {
		c.chunks = c.chunks[1:]
	} else {
		c.chunks[0] = c.chunks[0][n:]
	}
	return n, nil
}

func TestStampingReader(t *testing.T) {
	// A burst far below recordLine's 1024-line sampling threshold must still
	// produce a non-empty receive window (lastNs > firstNs) when the data
	// arrives across multiple reads — the regression behind EPS 0 on
	// 1,000-line crash/restart cases.
	shard := &connStats{}
	src := &chunkedReader{
		chunks: []string{"a\nb\n", "c\nd\n"},
		delay:  5 * time.Millisecond,
	}
	scanner := bufio.NewScanner(&stampingReader{r: src, shard: shard})
	lines := 0
	for scanner.Scan() {
		shard.recordLine(int64(len(scanner.Bytes())) + 1)
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if lines != 4 {
		t.Fatalf("lines = %d, want 4", lines)
	}
	first, last := shard.firstNs.Load(), shard.lastNs.Load()
	if first == 0 {
		t.Fatal("firstNs not stamped")
	}
	if last <= first {
		t.Fatalf("lastNs (%d) <= firstNs (%d): receive window empty", last, first)
	}
}

func TestStampingReaderFirstNsStable(t *testing.T) {
	// firstNs is stamped once on the first read and never moves.
	shard := &connStats{}
	sr := &stampingReader{r: strings.NewReader("x\n"), shard: shard}
	buf := make([]byte, 16)
	if _, err := sr.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	first := shard.firstNs.Load()
	time.Sleep(2 * time.Millisecond)
	sr.r = strings.NewReader("y\n")
	if _, err := sr.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := shard.firstNs.Load(); got != first {
		t.Fatalf("firstNs moved: %d → %d", first, got)
	}
	if shard.lastNs.Load() <= first {
		t.Fatal("lastNs not refreshed on second read")
	}
}

// TestRequiredSubstringSkipInitialSeconds covers config.SkipInitialSeconds:
// a line missing the required substring is exempt from the check while it
// lands within the configured window of the first-ever line, and is caught
// exactly as before once that window has elapsed. SkipInitialSeconds: 0
// (unset) must reproduce today's strict, no-exemption behavior unchanged.
func TestRequiredSubstringSkipInitialSeconds(t *testing.T) {
	t.Run("zero value is fully backward compatible", func(t *testing.T) {
		v := newValidator()
		cfg := config{RequiredSubstring: "TOKEN"}
		v.recordLine([]byte("missing the marker"), cfg)
		passed, errs := v.validate(cfg, 1)
		if passed {
			t.Fatalf("expected failure with SkipInitialSeconds unset, got passed=true errs=%v", errs)
		}
	})

	t.Run("line within the skip window is exempt", func(t *testing.T) {
		v := newValidator()
		cfg := config{RequiredSubstring: "TOKEN", SkipInitialSeconds: 5}
		// First line stamps firstLineNs to "now" — this line is itself
		// within the window (elapsed ~0s < 5s), so it is exempt even
		// though it is missing the substring.
		v.recordLine([]byte("missing the marker"), cfg)
		// Backdate firstLineNs so a SECOND line lands past the window and
		// is actually evaluated — otherwise substrChecked stays 0 and the
		// zero-evaluated guard (Fix 4a) fails the run for an unrelated
		// reason, masking what this subtest exists to prove.
		v.firstLineNs.Store(time.Now().Add(-10 * time.Second).UnixNano())
		v.recordLine([]byte("has the TOKEN right here"), cfg)
		passed, errs := v.validate(cfg, 2)
		if !passed {
			t.Fatalf("expected pass — the in-window miss must be exempt, and the out-of-window line matches, got errs=%v", errs)
		}
	})

	t.Run("line after the skip window is still caught", func(t *testing.T) {
		v := newValidator()
		cfg := config{RequiredSubstring: "TOKEN", SkipInitialSeconds: 5}
		// Backdate firstLineNs (no real sleep — keeps the test fast) so the
		// very next line lands well past the 5s window.
		v.firstLineNs.Store(time.Now().Add(-10 * time.Second).UnixNano())
		v.recordLine([]byte("missing the marker"), cfg)
		passed, errs := v.validate(cfg, 1)
		if passed {
			t.Fatalf("expected failure for a line past the skip window, got passed=true errs=%v", errs)
		}
	})

	t.Run("matching lines pass regardless of the window", func(t *testing.T) {
		v := newValidator()
		cfg := config{RequiredSubstring: "TOKEN", SkipInitialSeconds: 5}
		// A matching line landing INSIDE the window is exempted the same
		// as any other line there (never evaluated at all, not
		// "evaluated and passed") — so a second, matching line OUTSIDE
		// the window is what actually exercises "matching lines pass",
		// same reasoning as the subtest above.
		v.recordLine([]byte("has the TOKEN right here"), cfg)
		v.firstLineNs.Store(time.Now().Add(-10 * time.Second).UnixNano())
		v.recordLine([]byte("has the TOKEN right here"), cfg)
		passed, errs := v.validate(cfg, 2)
		if !passed {
			t.Fatalf("expected pass for matching lines, got errs=%v", errs)
		}
	})

	t.Run("skip window does not exempt ValidateJSON", func(t *testing.T) {
		v := newValidator()
		cfg := config{ValidateJSON: true, SkipInitialSeconds: 5}
		// Within the window (recorded immediately after firstLineNs is
		// stamped) — SkipInitialSeconds only ever gates RequiredSubstring;
		// invalid JSON must still be caught regardless.
		v.recordLine([]byte("not json"), cfg)
		passed, errs := v.validate(cfg, 1)
		if passed {
			t.Fatalf("expected ValidateJSON failure inside the skip window, got passed=true errs=%v", errs)
		}
	})

	t.Run("required substring entirely inside the skip window fails via the evaluated counter", func(t *testing.T) {
		v := newValidator()
		cfg := config{RequiredSubstring: "TOKEN", SkipInitialSeconds: 60}
		// Every line — matching or not — lands inside a 60s window, so
		// substrChecked never leaves 0. This must fail loudly (a
		// misconfigured skip window that swallows the whole run is not
		// evidence of a passing check) rather than read as a silent pass
		// because missingSubstr also stayed 0.
		v.recordLine([]byte("has the TOKEN right here"), cfg)
		v.recordLine([]byte("has the TOKEN right here"), cfg)
		passed, errs := v.validate(cfg, 2)
		if passed {
			t.Fatalf("expected failure when the skip window covers every received line, got passed=true errs=%v", errs)
		}
		found := false
		for _, e := range errs {
			if strings.Contains(e, "never evaluated") {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected a %q error, got errs=%v", "never evaluated", errs)
		}
	})
}
