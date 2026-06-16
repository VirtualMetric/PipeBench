package orchestrator

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

// decodeCertFile reads every CERTIFICATE PEM block from path and returns the
// parsed x509 certificates in order. Fatals on read or parse failure.
func decodeCertFile(t *testing.T, path string) []*x509.Certificate {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var certs []*x509.Certificate
	for {
		var blk *pem.Block
		blk, raw = pem.Decode(raw)
		if blk == nil {
			break
		}
		if blk.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			t.Fatalf("parse cert in %s: %v", path, err)
		}
		certs = append(certs, c)
	}
	return certs
}

// TestGenerateTLSCerts_ClientCertBundlesCA asserts that GenerateTLSCerts writes
// client.crt as a [leaf, CA] chain bundle — the core guarantee of the kafka TLS
// trust mechanism: the director reads the CA block from cert_name into
// tls.Config.RootCAs (applyRootCAs in pkg/kafka/helper.go) so that the
// self-signed broker cert verifies without insecure_skip_verify or SSL_CERT_FILE.
func TestGenerateTLSCerts_ClientCertBundlesCA(t *testing.T) {
	dir := t.TempDir()
	_, err := GenerateTLSCerts(dir, []string{"redpanda"})
	if err != nil {
		t.Fatalf("GenerateTLSCerts: %v", err)
	}

	clientCerts := decodeCertFile(t, filepath.Join(dir, "client.crt"))

	// client.crt must carry exactly two blocks: [leaf, CA].
	if len(clientCerts) != 2 {
		t.Fatalf("client.crt: want 2 CERTIFICATE blocks (leaf + CA), got %d", len(clientCerts))
	}

	// [0] is the leaf (bench-generator, not a CA).
	leaf := clientCerts[0]
	if leaf.Subject.CommonName != "bench-generator" {
		t.Errorf("client.crt[0] CommonName: want %q, got %q", "bench-generator", leaf.Subject.CommonName)
	}
	if leaf.IsCA {
		t.Errorf("client.crt[0] (leaf): IsCA must be false")
	}

	// [1] is the issuing CA (leaf first is what tls.X509KeyPair and redpanda expect).
	ca := clientCerts[1]
	if ca.Subject.CommonName != "PipeBench Bench CA" {
		t.Errorf("client.crt[1] CommonName: want %q, got %q", "PipeBench Bench CA", ca.Subject.CommonName)
	}
	if !ca.IsCA {
		t.Errorf("client.crt[1] (CA): IsCA must be true")
	}

	// Regression guard: ca.crt and server.crt must each be a single-block file
	// so that bundling does not leak into the files consumed by redpanda.
	caCerts := decodeCertFile(t, filepath.Join(dir, "ca.crt"))
	if len(caCerts) != 1 {
		t.Fatalf("ca.crt: want 1 CERTIFICATE block, got %d", len(caCerts))
	}
	srvCerts := decodeCertFile(t, filepath.Join(dir, "server.crt"))
	if len(srvCerts) != 1 {
		t.Fatalf("server.crt: want 1 CERTIFICATE block, got %d", len(srvCerts))
	}

	// Trust proof: build a pool from the CA carried inside client.crt and verify
	// the broker's server cert against it — this is exactly what the director does
	// at runtime via applyRootCAs(tlsConfig, kh.PEMCert).
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	srv := srvCerts[0]
	if _, err := srv.Verify(x509.VerifyOptions{
		Roots:   roots,
		DNSName: "redpanda",
	}); err != nil {
		t.Fatalf("server cert does not verify against the CA bundled in client.crt: %v", err)
	}
}

// TestRotateServerCert_PreservesClientBundle asserts that RotateServerCert
// leaves client.crt untouched (it is the CA carrier for the kafka_cert_rotation
// case) and that the rotated server leaf still verifies against the same bundled CA.
func TestRotateServerCert_PreservesClientBundle(t *testing.T) {
	dir := t.TempDir()
	if _, err := GenerateTLSCerts(dir, []string{"redpanda"}); err != nil {
		t.Fatalf("GenerateTLSCerts: %v", err)
	}

	clientBefore, err := os.ReadFile(filepath.Join(dir, "client.crt"))
	if err != nil {
		t.Fatalf("read client.crt before rotation: %v", err)
	}

	if err := RotateServerCert(dir, []string{"redpanda"}); err != nil {
		t.Fatalf("RotateServerCert: %v", err)
	}

	// client.crt must be byte-identical after rotation — it is never rewritten.
	clientAfter, err := os.ReadFile(filepath.Join(dir, "client.crt"))
	if err != nil {
		t.Fatalf("read client.crt after rotation: %v", err)
	}
	if !bytes.Equal(clientBefore, clientAfter) {
		t.Error("client.crt changed after RotateServerCert — rotation must not touch the client cert bundle")
	}

	// The rotated server.crt must still be a single leaf (not a bundle) that
	// verifies against the CA bundled inside the (now-confirmed-stable) client.crt.
	clientCerts := decodeCertFile(t, filepath.Join(dir, "client.crt"))
	if len(clientCerts) != 2 {
		t.Fatalf("client.crt after rotation: want 2 CERTIFICATE blocks, got %d", len(clientCerts))
	}
	roots := x509.NewCertPool()
	roots.AddCert(clientCerts[1]) // the CA block carried inside the bundle

	srvCerts := decodeCertFile(t, filepath.Join(dir, "server.crt"))
	if len(srvCerts) != 1 {
		t.Fatalf("server.crt after rotation: want 1 CERTIFICATE block, got %d", len(srvCerts))
	}
	if _, err := srvCerts[0].Verify(x509.VerifyOptions{
		Roots:   roots,
		DNSName: "redpanda",
	}); err != nil {
		t.Fatalf("rotated server cert does not verify against CA bundled in client.crt: %v", err)
	}
}
