package results

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// UploadOptions configures result upload to S3-compatible storage.
type UploadOptions struct {
	ResultsDir string
	Bucket     string // e.g. "s3://my-bucket/bench-results" or "s3://my-bucket"
	Endpoint   string // optional: MinIO endpoint URL (e.g. "http://minio:9000")
}

// Upload syncs the local results directory to an S3-compatible bucket using
// the AWS CLI. For MinIO, set Endpoint. Credentials come from the environment
// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) or ~/.aws/credentials.
func Upload(opts UploadOptions) error {
	if opts.Bucket == "" {
		return fmt.Errorf("--bucket is required")
	}

	// Normalize: ensure bucket path ends without trailing slash
	bucket := strings.TrimRight(opts.Bucket, "/")

	// Verify results directory exists
	info, err := os.Stat(opts.ResultsDir)
	if err != nil {
		return fmt.Errorf("results directory %s not found: %w", opts.ResultsDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", opts.ResultsDir)
	}

	// Count files to upload
	count := 0
	if err := filepath.Walk(opts.ResultsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scanning results directory: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("no result files found in %s", opts.ResultsDir)
	}

	fmt.Printf("Uploading %d files from %s to %s\n", count, opts.ResultsDir, bucket)

	args := []string{"s3", "sync", opts.ResultsDir, bucket, "--no-progress"}
	if opts.Endpoint != "" {
		args = append(args, "--endpoint-url", opts.Endpoint)
	}

	cmd := exec.Command("aws", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("aws s3 sync failed: %w (is the AWS CLI installed?)", err)
	}

	fmt.Println("Upload complete.")
	return nil
}
