package orchestrator

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// VaultSeed pairs a secret path with the JSON file vault-init loads it from.
// Path is charset-validated at case load (see config.TestCase.Validate), so
// it is safe to embed in the vault-init command line; the secret values live
// only in the JSON file.
type VaultSeed struct {
	Path string // KV path the secret is seeded under
	File string // file name inside the mounted /vault-secrets dir
}

// VaultPaths is what PrepareVault provisions under the run temp dir for a
// `vault:`-enabled case.
type VaultPaths struct {
	// TLSDir is bind-mounted read-write into the vault container (the dev
	// server writes vault-ca.pem / vault-cert.pem / vault-key.pem there) and
	// read-only into the subject at /vault-tls so it can verify the chain.
	TLSDir string
	// SecretsDir holds one JSON file per seeded secret path and is
	// bind-mounted read-only into vault-init.
	SecretsDir string
	Seeds      []VaultSeed
}

// PrepareVault creates the host directories and secret seed files a
// `vault:`-enabled run needs. Seeds are ordered by sorted secret path so file
// numbering and the rendered vault-init command are deterministic.
func PrepareVault(tmpDir string, v *config.VaultConfig) (VaultPaths, error) {
	tlsDir := filepath.Join(tmpDir, "vault-tls")
	if err := os.MkdirAll(tlsDir, 0o777); err != nil {
		return VaultPaths{}, fmt.Errorf("creating vault tls dir: %w", err)
	}
	// MkdirAll is umask-filtered; chmod explicitly. World-writable because the
	// vault image drops to uid 100 (user "vault"), which must create the dev
	// TLS material in this host-owned dir — per-run throwaway certs, removed
	// with the temp dir.
	if err := os.Chmod(tlsDir, 0o777); err != nil {
		return VaultPaths{}, fmt.Errorf("chmod vault tls dir: %w", err)
	}

	secretsDir := filepath.Join(tmpDir, "vault-secrets")
	if err := os.MkdirAll(secretsDir, 0o700); err != nil {
		return VaultPaths{}, fmt.Errorf("creating vault secrets dir: %w", err)
	}

	seeds := make([]VaultSeed, 0, len(v.Secrets))
	for i, path := range slices.Sorted(maps.Keys(v.Secrets)) {
		data, err := json.Marshal(v.Secrets[path])
		if err != nil {
			return VaultPaths{}, fmt.Errorf("encoding vault secret %q: %w", path, err)
		}
		file := fmt.Sprintf("%d.json", i)
		if err := os.WriteFile(filepath.Join(secretsDir, file), data, 0o600); err != nil {
			return VaultPaths{}, fmt.Errorf("writing vault secret %q: %w", path, err)
		}
		seeds = append(seeds, VaultSeed{Path: path, File: file})
	}

	return VaultPaths{TLSDir: tlsDir, SecretsDir: secretsDir, Seeds: seeds}, nil
}
