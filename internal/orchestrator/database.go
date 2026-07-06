package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// PrepareDatabase writes the seed SQL for a `database:`-enabled case to the
// run temp dir, so database-init can mount it read-only and run it via the
// engine's CLI once the server is healthy.
func PrepareDatabase(tmpDir string, d *config.DatabaseConfig) (string, error) {
	seedPath := filepath.Join(tmpDir, "db-seed.sql")
	if err := os.WriteFile(seedPath, []byte(d.SeedSQL), 0o644); err != nil {
		return "", fmt.Errorf("writing database seed sql: %w", err)
	}
	return seedPath, nil
}

// DatabaseTLSPaths holds the host paths produced for a TLS-enabled database.
type DatabaseTLSPaths struct {
	// CertDir contains ca.crt, server.crt and server.key.
	CertDir string
	// ConfPath is the engine TLS config file to mount (empty if the engine
	// has no BuildTLSConf).
	ConfPath string
}

// PrepareDatabaseTLS generates a per-run CA + RSA server cert (SAN "database")
// and writes the engine's TLS config file into tmpDir. server.crt/server.key
// are mounted into the database container; ca.crt is mounted into the subject
// so a device can verify the server certificate against it.
func PrepareDatabaseTLS(tmpDir string, engine config.DatabaseEngine) (DatabaseTLSPaths, error) {
	certDir := filepath.Join(tmpDir, "db-certs")
	if _, err := GenerateDatabaseTLSCerts(certDir, []string{"database"}); err != nil {
		return DatabaseTLSPaths{}, fmt.Errorf("generating database tls certs: %w", err)
	}

	paths := DatabaseTLSPaths{CertDir: certDir}
	if engine.BuildTLSConf != nil {
		_, content := engine.BuildTLSConf()
		confPath := filepath.Join(tmpDir, "db-tls.conf")
		if err := os.WriteFile(confPath, []byte(content), 0o644); err != nil {
			return DatabaseTLSPaths{}, fmt.Errorf("writing database tls conf: %w", err)
		}
		paths.ConfPath = confPath
	}
	return paths, nil
}
