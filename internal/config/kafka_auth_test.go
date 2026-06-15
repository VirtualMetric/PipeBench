package config

import "testing"

// kafkaAuthCase builds a minimal valid kafka_correctness case carrying the
// given auth block, so Validate exercises only the auth rules.
func kafkaAuthCase(mechanism, tls string) *TestCase {
	return &TestCase{
		Name: "kafka_auth",
		Type: "kafka_correctness",
		Kafka: &KafkaConfig{
			Topic: "bench",
			Auth:  &KafkaAuth{Mechanism: mechanism, TLS: tls},
		},
		Generator: GeneratorConfig{Mode: "kafka", Target: "redpanda:9092"},
		Receiver:  ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Subjects:  []string{"vmetric"},
	}
}

func TestValidateKafkaAuth(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		tls       string
		wantErr   bool
	}{
		{"sasl plain", "plain", "", false},
		{"sasl scram256", "scram-sha-256", "none", false},
		{"sasl scram512", "scram-sha-512", "", false},
		{"sasl ssl", "scram-sha-512", "server", false},
		{"mtls only", "", "mutual", false},
		{"sasl over mutual", "plain", "mutual", false},
		{"gssapi", "gssapi", "", false},
		{"gssapi rejects tls", "gssapi", "server", true},
		{"server tls without sasl", "", "server", true},
		{"unknown mechanism", "digest-md5", "", true},
		{"unknown tls", "plain", "always", true},
		{"no-op auth block", "", "none", true},
		{"no-op empty", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := kafkaAuthCase(tt.mechanism, tt.tls).validateKafkaAuth()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateKafkaAuth() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKafkaAuthAccessors(t *testing.T) {
	k := &KafkaConfig{Auth: &KafkaAuth{Mechanism: "SCRAM-SHA-512", TLS: "Server"}}
	if got := k.SASLMechanism(); got != "scram-sha-512" {
		t.Errorf("SASLMechanism() = %q, want scram-sha-512 (lowercased)", got)
	}
	if got := k.TLSMode(); got != "server" {
		t.Errorf("TLSMode() = %q, want server (lowercased)", got)
	}
	if !k.UsesSASL() || !k.UsesTLS() || k.RequireClientAuth() {
		t.Errorf("server-TLS+SASL: UsesSASL/UsesTLS should be true, RequireClientAuth false")
	}

	mtls := &KafkaConfig{Auth: &KafkaAuth{TLS: "mutual"}}
	if mtls.UsesSASL() || !mtls.RequireClientAuth() {
		t.Errorf("mtls: UsesSASL should be false, RequireClientAuth true")
	}

	// Nil and no-auth configs must be inert.
	var nilCfg *KafkaConfig
	if nilCfg.AuthEnabled() || (&KafkaConfig{}).AuthEnabled() {
		t.Errorf("nil / empty KafkaConfig must report AuthEnabled() == false")
	}
}
