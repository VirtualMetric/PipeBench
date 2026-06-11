package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

// Cloud receiver modes (s3, azure_blob, sqs, kinesis, cloudwatch) drain an
// emulator by polling instead of listening. They share the plumbing below:
// a run-forever poll loop (the harness kills the container, same contract as
// receiveTCP's Accept loop) and a body→lines fan-out that feeds the exact
// onLine path the OTLP mode uses, so every RECEIVER_VALIDATE_* check works
// unmodified.

// cloudPollInterval is how long a polling mode sleeps between sweeps.
func cloudPollInterval() time.Duration {
	return time.Duration(getEnvInt("RECEIVER_CLOUD_POLL_MS", 1000)) * time.Millisecond
}

// newCloudOnLine returns the per-record callback for a polling mode: one
// counter shard + the validator fan-out. Call once per concurrent worker so
// shards don't contend.
func newCloudOnLine(cnt *counters, val *validator, cfg config) func([]byte) {
	shard := cnt.newShard()
	needsValidation := cfg.ValidateDedup || cfg.ValidateContent || cfg.ValidateJSON || cfg.RequiredSubstring != ""
	return func(line []byte) {
		shard.recordLine(int64(len(line)) + 1)
		if needsValidation {
			val.recordLine(line, cfg)
		}
	}
}

// countBody splits an object/message body into lines and feeds onLine.
// Gzip is auto-detected by magic bytes — subjects commonly compress object
// uploads. Trailing/blank lines are skipped so "a\nb\n" counts 2.
func countBody(body []byte, onLine func([]byte)) error {
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("gzip reader: %w", err)
		}
		decoded, err := io.ReadAll(zr)
		zr.Close()
		if err != nil {
			return fmt.Errorf("gunzip: %w", err)
		}
		body = decoded
	}
	for line := range bytes.SplitSeq(body, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		onLine(line)
	}
	return nil
}

// pollLoop runs fn forever at the given interval, logging transient errors
// instead of dying — an emulator hiccup must not zero out a benchmark run.
// Never returns; the error type keeps mode signatures uniform.
func pollLoop(name string, interval time.Duration, fn func(context.Context) error) error {
	ctx := context.Background()
	for {
		if err := fn(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "receiver: %s poll: %v\n", name, err)
		}
		time.Sleep(interval)
	}
}
