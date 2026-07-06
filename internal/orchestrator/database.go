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
