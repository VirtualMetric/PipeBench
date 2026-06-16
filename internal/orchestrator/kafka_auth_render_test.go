package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// renderKafkaAuthCompose renders the compose file for a kafka_correctness case
// carrying the given auth block and returns its contents. tlsCertsHost is set
// for TLS cases so the broker /certs mount and generator TLS env render.
func renderKafkaAuthCompose(t *testing.T, auth *config.KafkaAuth) string {
	t.Helper()
	tc := &config.TestCase{
		Name:     "kafka_auth",
		Type:     "kafka_correctness",
		Duration: "10s",
		Warmup:   "20s",
		Kafka:    &config.KafkaConfig{Topic: "bench", Partitions: 1, Auth: auth},
		Generator: config.GeneratorConfig{
			Mode: "kafka", Target: "redpanda:9092", Format: "json",
			Rate: 100, TotalLines: 1000,
		},
		Receiver: config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Subjects: []string{"vmetric"},
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "dev", ConfigPath: "/config.yml"}

	tmp, err := os.MkdirTemp("", "kafka-auth-render-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmp) })

	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase:         tc,
		Subject:          subj,
		ConfigName:       "default",
		ConfigSrcPath:    composePath,
		TmpDir:           tmp,
		GeneratorImage:   "img-gen",
		ReceiverImage:    "img-recv",
		CollectorImage:   "img-coll",
		ReceiverHostPort: 19001,
	}
	if tc.Kafka.UsesTLS() {
		cfg.TLSCertsHost = filepath.Join(tmp, "certs")
	}
	if err := writeCompose(composePath, cfg); err != nil {
		t.Fatalf("writeCompose: %v", err)
	}
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestKafkaSASLPlainRenders(t *testing.T) {
	out := renderKafkaAuthCompose(t, &config.KafkaAuth{Mechanism: "plain"})
	mustContain(t, out, "redpanda.enable_sasl=true")
	mustContain(t, out, "redpanda.kafka_api[0].authentication_method=sasl")
	mustContain(t, out, "redpanda.superusers=['bench']")
	mustContain(t, out, "redpanda.sasl_mechanisms=['SCRAM','PLAIN']")
	mustContain(t, out, "rpk security user create bench")
	mustContain(t, out, "SCRAM-SHA-256")
	mustContain(t, out, "GENERATOR_KAFKA_SASL: \"plain\"")
	mustContain(t, out, "GENERATOR_KAFKA_USER: \"bench\"")
	mustContain(t, out, "KAFKA_SASL_PASSWORD: \"pipebench-kafka-dev\"")
	// PLAINTEXT SASL → no TLS material.
	mustNotContain(t, out, "kafka_api_tls=[")
	mustNotContain(t, out, "GENERATOR_KAFKA_TLS:")
}

func TestKafkaSASLSSLRenders(t *testing.T) {
	out := renderKafkaAuthCompose(t, &config.KafkaAuth{Mechanism: "scram-sha-512", TLS: "server"})
	mustContain(t, out, "redpanda.enable_sasl=true")
	mustContain(t, out, "authentication_method=sasl")
	mustContain(t, out, "kafka_api_tls=[{\"name\":\"internal\"")
	mustContain(t, out, "\"require_client_auth\":false")
	mustContain(t, out, "GENERATOR_KAFKA_SASL: \"scram-sha-512\"")
	mustContain(t, out, "GENERATOR_KAFKA_TLS: \"true\"")
	// Broker must mount the certs dir.
	mustContain(t, out, ":/certs:ro")
}

func TestKafkaMTLSRenders(t *testing.T) {
	out := renderKafkaAuthCompose(t, &config.KafkaAuth{TLS: "mutual"})
	mustContain(t, out, "authentication_method=mtls_identity")
	mustContain(t, out, "\"require_client_auth\":true")
	mustContain(t, out, "GENERATOR_KAFKA_TLS: \"true\"")
	mustContain(t, out, ":/certs:ro")
	// mTLS-only → no SASL.
	mustNotContain(t, out, "enable_sasl")
	mustNotContain(t, out, "GENERATOR_KAFKA_SASL:")
	mustNotContain(t, out, "KAFKA_SASL_PASSWORD:")
}

func TestKafkaGSSAPIRenders(t *testing.T) {
	tc := &config.TestCase{
		Name:     "kafka_gssapi",
		Type:     "kafka_correctness",
		Duration: "10s",
		Warmup:   "35s",
		Kafka: &config.KafkaConfig{
			Image: "confluentinc/cp-kafka:7.7.1", Topic: "bench", Partitions: 1,
			Auth: &config.KafkaAuth{Mechanism: "gssapi", Realm: "PIPEBENCH.LOCAL"},
		},
		Generator: config.GeneratorConfig{Mode: "kafka", Target: "kafka:9092", Format: "json", Rate: 100, TotalLines: 1000},
		Receiver:  config.ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Subjects:  []string{"vmetric"},
	}
	subj := config.Subject{Name: "vmetric", Image: "vmetric/director", Version: "dev", ConfigPath: "/config.yml"}
	tmp, err := os.MkdirTemp("", "kafka-gssapi-render-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmp) })
	composePath := filepath.Join(tmp, "compose.yaml")
	cfg := RunConfig{
		TestCase: tc, Subject: subj, ConfigName: "default", ConfigSrcPath: composePath,
		TmpDir: tmp, GeneratorImage: "img-gen", ReceiverImage: "img-recv", CollectorImage: "img-coll",
		ReceiverHostPort: 19001,
		// Set directly so writeCompose renders without invoking the KDC prep.
		KrbHostDir: filepath.Join(tmp, "krb5"), KerberosInitCmd: "echo kdc-boot",
	}
	if err := writeCompose(composePath, cfg); err != nil {
		t.Fatalf("writeCompose: %v", err)
	}
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)

	mustContain(t, out, "  kdc:\n")
	// KDC binds privileged port 88 as a non-root user; the sysctl must be set
	// explicitly rather than relying on Docker's runtime default.
	mustContain(t, out, "net.ipv4.ip_unprivileged_port_start: 0")
	mustContain(t, out, "  kafka:\n")
	mustContain(t, out, "  kafka-init:\n")
	mustContain(t, out, "SASL_PLAINTEXT://0.0.0.0:9094")
	mustContain(t, out, "KAFKA_SASL_ENABLED_MECHANISMS: \"GSSAPI\"")
	mustContain(t, out, "KAFKA_LISTENER_NAME_SASL__PLAINTEXT_GSSAPI_SASL_JAAS_CONFIG")
	mustContain(t, out, "principal=\"kafka/kafka@PIPEBENCH.LOCAL\";")
	mustContain(t, out, ":/krb5:ro")
	// gssapi uses Apache Kafka, not Redpanda.
	mustNotContain(t, out, "redpanda")
}

func TestKafkaNoAuthUnchanged(t *testing.T) {
	// No auth block → the original PLAINTEXT Redpanda path, untouched.
	out := renderKafkaAuthCompose(t, nil)
	mustContain(t, out, "PLAINTEXT://0.0.0.0:9092")
	mustContain(t, out, "rpk topic create bench")
	mustNotContain(t, out, "enable_sasl")
	mustNotContain(t, out, "kafka_api_tls")
	mustNotContain(t, out, "authentication_method")
	mustNotContain(t, out, "GENERATOR_KAFKA_SASL:")
}
