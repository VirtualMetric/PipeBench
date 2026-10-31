package config

import (
	"strings"
	"testing"
)

// TestOutageConfirmsValidation: min_each_after_outage and
// max_confirmed_during_outage judge what a sender got confirmed around a
// pause_store outage. They need a generator that reports what it got
// confirmed (http_ack) and the pause_store action. min_each_after_outage
// also needs a target that names a replica, and max_confirmed_during_outage
// an outage long enough to count from 15 seconds into it.
func TestOutageConfirmsValidation(t *testing.T) {
	ack := func() *TestCase {
		tc := sharedStateCase()
		tc.SharedState.Action = SharedStateActionPauseStore
		tc.Generators[1] = GeneratorConfig{ID: "syslog", Mode: "http_ack", Target: "http://subject-1:9000/,http://subject-2:9000/,http://subject-3:9000/"}
		zero := int64(0)
		tc.SharedState.Streams[1].MaxConfirmedDuringOutage = &zero
		tc.SharedState.Streams[1].MinEachAfterOutage = 1000
		return tc
	}
	if err := ack().Validate(); err != nil {
		t.Fatalf("an http_ack stream to every replica with both limits and pause_store was refused: %v", err)
	}
	cases := map[string]struct {
		change func(*TestCase)
		want   string
	}{
		"negative minimum": {func(tc *TestCase) { tc.SharedState.Streams[1].MinEachAfterOutage = -1 }, "must not be negative"},
		"negative maximum": {func(tc *TestCase) {
			n := int64(-1)
			tc.SharedState.Streams[1].MaxConfirmedDuringOutage = &n
		}, "must not be negative"},
		"not http_ack": {func(tc *TestCase) {
			tc.Generators[1].Mode = "tcp"
			tc.Generators[1].Target = "subject-2:9000"
		}, "only a generator in http_ack mode"},
		"another action": {func(tc *TestCase) { tc.SharedState.Action = SharedStateActionKillLeader }, "only the pause_store action judges"},
		"the maximum alone with another action": {func(tc *TestCase) {
			tc.SharedState.Action = SharedStateActionKillLeader
			tc.SharedState.Streams[1].MinEachAfterOutage = 0
		}, "max_confirmed_during_outage, which only the pause_store action judges"},
		"posts to no replica": {func(tc *TestCase) { tc.Generators[1].Target = "http://relay:9000/" }, "posts to no replica"},
		"a short outage":      {func(tc *TestCase) { tc.SharedState.OutageSeconds = 10 }, "needs outage_seconds"},
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
