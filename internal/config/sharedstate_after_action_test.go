package config

import (
	"strings"
	"testing"
)

// TestMinAfterActionValidation: min_after_action counts the records a sender
// got confirmed after the action, so it needs a generator that reports its
// confirmations (http_ack) and an action to count from.
func TestMinAfterActionValidation(t *testing.T) {
	ack := func() *TestCase {
		tc := sharedStateCase()
		tc.SharedState.Action = SharedStateActionKillReceiver
		tc.Generators[1] = GeneratorConfig{ID: "syslog", Mode: "http_ack", Target: "http://subject-2:9000/"}
		tc.SharedState.Streams[1].MinAfterAction = 50000
		return tc
	}
	if err := ack().Validate(); err != nil {
		t.Fatalf("an http_ack stream with min_after_action and kill_receiver was refused: %v", err)
	}
	cases := map[string]struct {
		change func(*TestCase)
		want   string
	}{
		"negative": {func(tc *TestCase) { tc.SharedState.Streams[1].MinAfterAction = -1 }, "must not be negative"},
		"not http_ack": {func(tc *TestCase) {
			tc.Generators[1].Mode = "tcp"
			tc.Generators[1].Target = "subject-2:9000"
		}, "only a generator in http_ack mode"},
		"no action": {func(tc *TestCase) { tc.SharedState.Action = SharedStateActionNone }, "shared_state.action is not set"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := ack()
			c.change(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate = %v, want an error containing %q", err, c.want)
			}
		})
	}
}
