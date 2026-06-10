package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// runS3 uploads synthesized lines as S3 objects (LocalStack in the bench
// topology) for cases where the subject ingests FROM object storage.
// cfg.Target is the emulator endpoint URL. Objects trickle out for the whole
// duration — packed every GENERATOR_S3_LINES_PER_OBJECT lines — so
// SQS-notification-driven subjects are fed continuously, not in one bulk
// pre-upload. "lines" counts records (like kafka mode), keeping the
// harness's lines-sent vs lines-received comparison meaningful.
func runS3(cfg config, clock *sendClock) (int64, int64, error) {
	bucket := mustEnv("GENERATOR_S3_BUCKET")
	prefix := getEnv("GENERATOR_S3_PREFIX", "in/")
	linesPerObject := max(getEnvInt("GENERATOR_S3_LINES_PER_OBJECT", 10000), 1)
	compression := getEnv("GENERATOR_S3_COMPRESSION", "none")
	if compression != "none" && compression != "gzip" {
		return 0, 0, fmt.Errorf("GENERATOR_S3_COMPRESSION must be \"none\" or \"gzip\", got %q", compression)
	}

	awscfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return 0, 0, fmt.Errorf("aws config: %w", err)
	}
	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Target)
		// The emulator hostname has no per-bucket DNS, so path-style is required.
		o.UsePathStyle = true
	})

	upload := func(ctx context.Context, connID int, seq int64, data []byte) error {
		key := fmt.Sprintf("%s%d/%06d-%d.log", prefix, connID, seq, time.Now().UnixNano())
		if compression == "gzip" {
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)
			if _, err := zw.Write(data); err != nil {
				return fmt.Errorf("gzip: %w", err)
			}
			if err := zw.Close(); err != nil {
				return fmt.Errorf("gzip close: %w", err)
			}
			data = buf.Bytes()
			key += ".gz"
		}
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		}); err != nil {
			return fmt.Errorf("put object %s: %w", key, err)
		}
		// The send "lands" when the object is visible, not when a line is
		// buffered — record it so first/last send bounds stay honest.
		clock.RecordSend()
		return nil
	}

	return runObjectWorkers(cfg, func(connID int) (int64, int64, error) {
		return packLines(cfg, connID, clock, linesPerObject, func(seq int64, object []byte) error {
			return upload(context.Background(), connID, seq, object)
		})
	})
}

// packLines drives sendLinesConn, packing lines into objects of
// linesPerObject lines and handing each completed object (plus the final
// partial one) to flush.
func packLines(cfg config, connID int, clock *sendClock, linesPerObject int, flush func(seq int64, object []byte) error) (int64, int64, error) {
	var buf bytes.Buffer
	var inBuf int
	var seq int64

	sent, bytesSent, err := sendLinesConn(cfg, connID, clock, func(line []byte) error {
		buf.Write(line)
		inBuf++
		if inBuf < linesPerObject {
			return nil
		}
		object := buf.Bytes()
		ferr := flush(seq, object)
		buf.Reset()
		inBuf = 0
		seq++
		return ferr
	})
	if err == nil && inBuf > 0 {
		err = flush(seq, buf.Bytes())
	}
	return sent, bytesSent, err
}

// runObjectWorkers mirrors runTCPParallel's fan-out for object-storage
// modes: cfg.Connections workers, each with its own connID and object
// stream, first error wins.
func runObjectWorkers(cfg config, worker func(connID int) (int64, int64, error)) (int64, int64, error) {
	if cfg.Connections <= 1 {
		return worker(cfg.ConnOffset)
	}

	var totalLines, totalBytes atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	for i := range cfg.Connections {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sent, bytes, err := worker(id + cfg.ConnOffset)
			totalLines.Add(sent)
			totalBytes.Add(bytes)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
			}
			fmt.Fprintf(os.Stderr, "generator: uploader %d done: lines=%d bytes=%d\n", id, sent, bytes)
		}(i)
	}

	wg.Wait()
	return totalLines.Load(), totalBytes.Load(), firstErr
}
