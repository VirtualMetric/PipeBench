package orchestrator

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// GenerateTLSCerts writes a self-signed CA plus matching server and client
// leaf certs to outDir, suitable for a TLS-enabled bench run. It returns
// the absolute path of outDir so the caller can hand it to RunConfig.
//
// The files written are:
//   ca.crt      — root CA certificate (PEM)
//   ca.key      — root CA private key (PEM)
//   server.crt  — leaf cert with SAN serverHosts (PEM), signed by ca.crt
//   server.key  — leaf private key (PEM)
//   client.crt  — leaf cert for the generator (PEM), signed by ca.crt
//   client.key  — leaf private key (PEM)
//
// serverHosts is the list of SAN entries baked into server.crt. For the
// PipeBench network, "subject" is the only hostname that matters (the
// generator dials it by service alias). Callers can extend the list when
// a case wants additional aliases (e.g. "localhost").
//
// All keys are P-256 ECDSA — small, fast to generate, broadly supported,
// and faster to validate than 2048-bit RSA on every connection.
func GenerateTLSCerts(outDir string, serverHosts []string) (string, error) {
	abs, err := filepath.Abs(outDir)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", fmt.Errorf("creating cert dir: %w", err)
	}

	// 1. Root CA
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate ca key: %w", err)
	}
	caTpl := &x509.Certificate{
		SerialNumber: bigSerial(),
		Subject:      pkix.Name{CommonName: "PipeBench Bench CA"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		return "", fmt.Errorf("sign ca cert: %w", err)
	}
	if err := writePEMCert(filepath.Join(abs, "ca.crt"), caDER); err != nil {
		return "", err
	}
	if err := writePEMKey(filepath.Join(abs, "ca.key"), caKey); err != nil {
		return "", err
	}

	// 2. Server leaf (TLS endpoint inside the subject)
	if len(serverHosts) == 0 {
		serverHosts = []string{"subject"}
	}
	serverDNS, serverIP := splitHosts(serverHosts)
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate server key: %w", err)
	}
	srvTpl := &x509.Certificate{
		SerialNumber: bigSerial(),
		Subject:      pkix.Name{CommonName: serverHosts[0]},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     serverDNS,
		IPAddresses:  serverIP,
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTpl, caTpl, &srvKey.PublicKey, caKey)
	if err != nil {
		return "", fmt.Errorf("sign server cert: %w", err)
	}
	if err := writePEMCert(filepath.Join(abs, "server.crt"), srvDER); err != nil {
		return "", err
	}
	if err := writePEMKey(filepath.Join(abs, "server.key"), srvKey); err != nil {
		return "", err
	}

	// 3. Client leaf (the generator dials with this)
	cliKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate client key: %w", err)
	}
	cliTpl := &x509.Certificate{
		SerialNumber: bigSerial(),
		Subject:      pkix.Name{CommonName: "bench-generator"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTpl, caTpl, &cliKey.PublicKey, caKey)
	if err != nil {
		return "", fmt.Errorf("sign client cert: %w", err)
	}
	if err := writePEMCert(filepath.Join(abs, "client.crt"), cliDER); err != nil {
		return "", err
	}
	if err := writePEMKey(filepath.Join(abs, "client.key"), cliKey); err != nil {
		return "", err
	}

	return abs, nil
}

func writePEMCert(path string, der []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writePEMKey(path string, key *ecdsa.PrivateKey) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return pem.Encode(f, &pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func splitHosts(hosts []string) (dns []string, ips []net.IP) {
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			ips = append(ips, ip)
		} else {
			dns = append(dns, h)
		}
	}
	return dns, ips
}

func bigSerial() *big.Int {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	n, _ := rand.Int(rand.Reader, limit)
	return n
}
