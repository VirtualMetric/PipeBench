package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
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

// ReseedVaultSecret updates an existing KV v2 secret in the running Vault
// container mid-run without bouncing Vault. It writes the new field values to
// a temp file, docker-copies it into the container, then runs `vault kv put`
// inside the container. Use this for mid-run credential/cert rotations.
//
// vaultContainer defaults to "bench-vault" when empty.
// mount defaults to VaultConfig's default ("secret") when empty.
// token defaults to VaultConfig's default ("pipebench-dev-root") when empty.
func ReseedVaultSecret(ctx context.Context, vaultContainer, mount, token, path string, fields map[string]string) error {
	if vaultContainer == "" {
		vaultContainer = "bench-vault"
	}
	if mount == "" {
		mount = (&config.VaultConfig{}).MountOrDefault()
	}
	if token == "" {
		token = (&config.VaultConfig{}).TokenOrDefault()
	}

	data, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("encoding vault secret fields: %w", err)
	}

	tmp, err := os.CreateTemp("", "vault-reseed-*.json")
	if err != nil {
		return fmt.Errorf("creating reseed temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close() // idempotent after the explicit Close below; covers error paths

	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("writing reseed temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing reseed temp file: %w", err)
	}

	// Copy the JSON file into the container so PEM content never appears on
	// the command line. Use a unique in-container path derived from the host
	// temp name to avoid collisions across concurrent runs.
	inContainerPath := "/tmp/" + filepath.Base(tmp.Name())
	dst := vaultContainer + ":" + inContainerPath
	if out, err := exec.CommandContext(ctx, "docker", "cp", tmp.Name(), dst).CombinedOutput(); err != nil {
		return fmt.Errorf("docker cp to %s: %w\n%s", vaultContainer, err, out)
	}

	// Pass credentials via docker exec -e so they are environment variables
	// inside the container, not part of the shell command string visible in
	// the process's argv. This also eliminates the sh -c wrapper entirely.
	out, err := exec.CommandContext(ctx, "docker", "exec",
		"-e", "VAULT_ADDR=https://127.0.0.1:8200",
		"-e", "VAULT_TOKEN="+token,
		"-e", "VAULT_CACERT=/vault/tls/vault-ca.pem",
		vaultContainer,
		"vault", "kv", "put", "-mount="+mount, path, "@"+inContainerPath,
	).CombinedOutput()

	// Remove the temp file from the container; ignore errors (run is ephemeral).
	_ = exec.CommandContext(ctx, "docker", "exec", vaultContainer, "rm", "-f", inContainerPath).Run()

	if err != nil {
		return fmt.Errorf("vault kv put in %s: %w\n%s", vaultContainer, err, out)
	}
	return nil
}
