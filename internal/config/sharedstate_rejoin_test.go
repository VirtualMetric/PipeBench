package config

import (
	"strings"
	"testing"
)

// TestRollingKillIsAnAction: rolling_kill kills every replica in turn, so a
// case may name it.
func TestRollingKillIsAnAction(t *testing.T) {
	tc := sharedStateCase()
	tc.SharedState.Action = SharedStateActionRollingKill
	if err := tc.Validate(); err != nil {
		t.Fatalf("a case with rolling_kill was refused: %v", err)
	}
}

// TestMinAfterRejoinValidation: min_after_rejoin counts the records a sender
// got confirmed through each replaced replica after it rejoined. It needs a
// generator that reports what each target confirmed (http_ack), a target
// that names a replica, and an action that replaces replicas.
func TestMinAfterRejoinValidation(t *testing.T) {
	ack := func() *TestCase {
		tc := sharedStateCase()
		tc.SharedState.Action = SharedStateActionRollingKill
		tc.Generators[1] = GeneratorConfig{ID: "syslog", Mode: "http_ack", Target: "http://subject-1:9000/,http://subject-2:9000/,http://subject-3:9000/"}
		tc.SharedState.Streams[1].MinAfterRejoin = 2000
		return tc
	}
	if err := ack().Validate(); err != nil {
		t.Fatalf("an http_ack stream to every replica with min_after_rejoin and rolling_kill was refused: %v", err)
	}
	for _, action := range replacingActions {
		tc := ack()
		tc.SharedState.Action = action
		if action == SharedStateActionKillReceiver || action == SharedStateActionKillOwner || action == SharedStateActionMoveThenKill {
			tc.SharedState.PlacedDevices = []int64{11}
		}
		if err := tc.Validate(); err != nil && strings.Contains(err.Error(), "min_after_rejoin") {
			t.Errorf("min_after_rejoin with %s, which replaces replicas, was refused: %v", action, err)
		}
	}
	cases := map[string]struct {
		change func(*TestCase)
		want   string
	}{
		"negative": {func(tc *TestCase) { tc.SharedState.Streams[1].MinAfterRejoin = -1 }, "must not be negative"},
		"not http_ack": {func(tc *TestCase) {
			tc.Generators[1].Mode = "tcp"
			tc.Generators[1].Target = "subject-2:9000"
		}, "only a generator in http_ack mode"},
		"no action":                    {func(tc *TestCase) { tc.SharedState.Action = SharedStateActionNone }, "replaces no replica"},
		"an action that replaces none": {func(tc *TestCase) { tc.SharedState.Action = SharedStateActionPauseStore }, "replaces no replica"},
		"a scale-in replaces none": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionStopOwner
		}, "replaces no replica"},
		"posts to no replica":     {func(tc *TestCase) { tc.Generators[1].Target = "http://relay:9000/" }, "posts to no replica"},
		"posts past the replicas": {func(tc *TestCase) { tc.Generators[1].Target = "http://subject-4:9000/" }, "posts to no replica"},
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
