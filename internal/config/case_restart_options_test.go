package config

import (
	"strings"
	"testing"
	"time"
)

func restartOptionsCase() *TestCase {
	return &TestCase{Name: "restart", Type: "persistence_restart_correctness"}
}

// A setting that only the restart and crash drivers read fails the case when it
// loads if it is wrong, or if the case's type would ignore it.
func TestRestartOptionsAreValidated(t *testing.T) {
	for name, tc := range map[string]struct {
		edit func(*TestCase)
		want string
	}{
		"valid": {func(c *TestCase) {
			c.EmptyDiskOnRestart = true
			c.StopDelay = "2s"
			c.Correctness.ExpectedLossPct = 100
			c.Correctness.MinLossPct = 5
			c.Correctness.RequireStopLog = []string{"in [0-9.]+s$"}
		}, ""},
		"unreadable stop_delay":   {func(c *TestCase) { c.StopDelay = "soon" }, "stop_delay"},
		"negative stop_delay":     {func(c *TestCase) { c.StopDelay = "-1s" }, "must not be negative"},
		"stop_delay elsewhere":    {func(c *TestCase) { c.Type = "correctness"; c.StopDelay = "1s" }, "applies only"},
		"empty disk elsewhere":    {func(c *TestCase) { c.Type = "correctness"; c.EmptyDiskOnRestart = true }, "applies only"},
		"floor elsewhere":         {func(c *TestCase) { c.Type = "correctness"; c.Correctness.MinLossPct = 1 }, "applies only"},
		"log elsewhere":           {func(c *TestCase) { c.Type = "correctness"; c.Correctness.RequireStopLog = []string{"x"} }, "applies only"},
		"floor over 100":          {func(c *TestCase) { c.Correctness.MinLossPct = 101 }, "between 0 and 100"},
		"floor above the ceiling": {func(c *TestCase) { c.Correctness.ExpectedLossPct = 1; c.Correctness.MinLossPct = 5 }, "no run could pass"},
		"unreadable log pattern":  {func(c *TestCase) { c.Correctness.RequireStopLog = []string{"("} }, "require_stop_log"},
		"crash type takes the log": {func(c *TestCase) {
			c.Type = "persistence_crash_correctness"
			c.Correctness.RequireStopLog = []string{"x"}
		}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			c := restartOptionsCase()
			tc.edit(c)
			err := c.validateRestartOptions()
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("error = %v, want it to say %q", err, tc.want)
			}
		})
	}
}

func TestStopDelayOrZero(t *testing.T) {
	c := restartOptionsCase()
	if got := c.StopDelayOrZero(); got != 0 {
		t.Fatalf("no delay = %v", got)
	}
	c.StopDelay = " 3s "
	if got := c.StopDelayOrZero(); got != 3*time.Second {
		t.Fatalf("delay = %v, want 3s", got)
	}
}
