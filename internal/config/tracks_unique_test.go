package config

import (
	"testing"
	"time"
)

// The predicate is the contract shared by two packages that cannot see each
// other's decision: the orchestrator switches receiver dedup (and generator
// sequencing) on with it, and the runner picks the verdict basis with it. If
// they disagree, a case either judges loss over a set nobody recorded or
// silently keeps the raw count that hides loss.
func TestTracksUniqueLines(t *testing.T) {
	t.Parallel()

	db := func() *DatabaseConfig { return &DatabaseConfig{Engine: "postgres"} }
	jsonGen := GeneratorConfig{Mode: "tcp", Format: "json"}
	syslogGen := GeneratorConfig{Mode: "tcp", Format: "syslog"}

	tests := []struct {
		name string
		tc   TestCase
		want bool
	}{
		{
			// The database crash case. It sets validate_dedup:false on purpose
			// — dedup VALIDATION fails on any duplicate, which a crash case
			// must tolerate — so the predicate must not key off that flag.
			name: "database source, no generator",
			tc:   TestCase{Type: "persistence_inflight_crash_correctness", Database: db()},
			want: true,
		},
		{
			name: "generator json on a mid-delivery type",
			tc:   TestCase{Type: "kafka_inflight_crash_correctness", Generator: jsonGen},
			want: true,
		},
		{
			// Sequenced mode would overwrite the head of every line regardless
			// of format, corrupting the syslog header this case exists to send.
			name: "generator syslog on a mid-delivery type",
			tc:   TestCase{Type: "syslog_tls_vault_cert_rotation_correctness", Generator: syslogGen},
			want: false,
		},
		{
			// Same payload shape, but its own verdict — must not be dragged in.
			name: "generator json outside the mid-delivery driver",
			tc:   TestCase{Type: "kafka_offset_commit_restart", Generator: jsonGen},
			want: false,
		},
		{
			name: "a case that asked for dedup explicitly",
			tc:   TestCase{Type: "throughput", Correctness: CorrectnessConfig{ValidateDedup: true}},
			want: true,
		},
		{
			// TotalLines is 0 for a duration-driven generator, so keying off it
			// would have turned tracking on for a case that HAS a generator.
			name: "database AND a duration-driven generator",
			tc:   TestCase{Type: "persistence_inflight_crash_correctness", Database: db(), Generator: jsonGen},
			want: true, // via the generator branch, not the database one
		},
		{
			name: "plain throughput case",
			tc:   TestCase{Type: "throughput", Generator: jsonGen},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.tc.TracksUniqueLines(); got != tt.want {
				t.Fatalf("TracksUniqueLines() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A case sets a log-anchored trigger precisely because the default one cannot
// catch what it is testing. So a malformed delay must be an error: silently
// falling back to receiver-progress would restore the very trigger the case was
// written to avoid, and the case would pass while proving nothing.
func TestMidDeliveryTrigger(t *testing.T) {
	t.Parallel()

	t.Run("unset", func(t *testing.T) {
		t.Parallel()

		_, _, ok, err := (&TestCase{}).MidDeliveryTrigger()
		if ok || err != nil {
			t.Fatalf("ok=%v err=%v, want the default trigger and no error", ok, err)
		}
	})

	t.Run("log and delay", func(t *testing.T) {
		t.Parallel()

		tc := &TestCase{MidDelivery: &MidDeliveryConfig{TriggerLog: "Starting event collection", TriggerDelay: "6s"}}
		line, delay, ok, err := tc.MidDeliveryTrigger()
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		if line != "Starting event collection" || delay != 6*time.Second {
			t.Fatalf("got %q/%v", line, delay)
		}
	})

	t.Run("malformed delay is an error", func(t *testing.T) {
		t.Parallel()

		tc := &TestCase{MidDelivery: &MidDeliveryConfig{TriggerLog: "x", TriggerDelay: "6 seconds"}}
		if _, _, _, err := tc.MidDeliveryTrigger(); err == nil {
			t.Fatal("a malformed delay must fail loudly, not fall back to the default trigger")
		}
	})

	t.Run("negative delay is an error", func(t *testing.T) {
		t.Parallel()

		tc := &TestCase{MidDelivery: &MidDeliveryConfig{TriggerLog: "x", TriggerDelay: "-3s"}}
		if _, _, _, err := tc.MidDeliveryTrigger(); err == nil {
			t.Fatal("a negative delay must be rejected")
		}
	})
}
