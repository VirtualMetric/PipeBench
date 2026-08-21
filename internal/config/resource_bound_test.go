package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// TestResourceBoundYAMLDecode guards against a silently vacuous assertion.
//
// ResourceBound's fields are pointers so "unset" is distinguishable from zero.
// If a yaml tag ever stops matching, decoding yields all-nil pointers, Check
// returns "pass" for every value, and a case that looks like it asserts exact
// cgroup ceilings would assert only that the row exists — while still printing
// a tick. This test fails instead.
func TestResourceBoundYAMLDecode(t *testing.T) {
	var fc struct {
		ExpectResources map[string]map[string]ResourceBound `yaml:"expect_resources"`
	}
	src := `
expect_resources:
  runtime.container:
    count: {eq: 1}
    total: {eq: 524288}
  cpu.container:
    used: {min: 0, max: 2000}
`
	if err := yaml.Unmarshal([]byte(src), &fc); err != nil {
		t.Fatal(err)
	}

	total := fc.ExpectResources["runtime.container"]["total"]
	if total.Eq == nil || *total.Eq != 524288 {
		t.Fatalf("total.eq did not decode: %+v", total)
	}
	if why := total.Check(524288); why != "" {
		t.Errorf("Check(524288) = %q, want pass", why)
	}
	// The exact value a pre-cgroup director reported: host RAM in KB.
	if why := total.Check(32528416); why == "" {
		t.Error("Check(host-sized value) passed — the bound is vacuous")
	}

	used := fc.ExpectResources["cpu.container"]["used"]
	if used.Min == nil || used.Max == nil {
		t.Fatalf("min/max did not decode: %+v", used)
	}
	if why := used.Check(2000); why != "" {
		t.Errorf("Check(2000) = %q, want pass at the ceiling", why)
	}
	if why := used.Check(2001); why == "" {
		t.Error("Check(over max) passed — max is vacuous")
	}
	if why := used.Check(-1); why == "" {
		t.Error("Check(under min) passed — min is vacuous")
	}
}

// TestResourceBoundEmptyMatchesAnything pins the documented "row must exist"
// form: a bound with nothing set accepts any value.
func TestResourceBoundEmptyMatchesAnything(t *testing.T) {
	var b ResourceBound
	for _, v := range []int64{-1, 0, 1 << 40} {
		if why := b.Check(v); why != "" {
			t.Errorf("empty bound rejected %d: %s", v, why)
		}
	}
}
