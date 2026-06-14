package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// runMinioInit is the one-shot for the minio-init compose service: it creates
// the declared buckets over the S3 API, retrying until MinIO answers, then
// exits. MinIO has no LocalStack-style ready.d init hook, so this is both the
// bucket creator and the topology's readiness probe (the subject/generator/
// receiver gate on its successful completion) — the azure-init pattern.
func runMinioInit() {
	endpoint := getEnv("RECEIVER_S3_ENDPOINT", getEnv("AWS_ENDPOINT_URL", "http://minio:9000"))

	// Collect the non-empty bucket names up front. Validating the raw env
	// string is not enough: inputs like "," or " , " are non-empty yet
	// yield zero names, which would otherwise fall through the loop and
	// exit 0 — falsely signalling "ready" with no bucket created.
	var buckets []string
	for name := range strings.SplitSeq(os.Getenv("MINIO_INIT_BUCKETS"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			buckets = append(buckets, name)
		}
	}
	if len(buckets) == 0 {
		fmt.Fprintln(os.Stderr, "minio-init: MINIO_INIT_BUCKETS must list at least one bucket")
		os.Exit(1)
	}

	ctx := context.Background()
	awscfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "minio-init: aws config: %v\n", err)
		os.Exit(1)
	}
	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		// MinIO has no per-bucket DNS on the bench network, so path-style.
		o.UsePathStyle = true
	})

	for _, name := range buckets {
		// Per-bucket deadline so a slow bucket can't starve the rest.
		deadline := time.Now().Add(2 * time.Minute)
		for {
			// Per-request timeout so a connection that stalls mid-request
			// can't block past the deadline (the deadline check only runs
			// once CreateBucket returns).
			reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := client.CreateBucket(reqCtx, &s3.CreateBucketInput{Bucket: aws.String(name)})
			cancel()
			if err == nil || bucketExists(err) {
				break
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "minio-init: create bucket %s: %v\n", name, err)
				os.Exit(1)
			}
			time.Sleep(time.Second)
		}
		fmt.Fprintf(os.Stderr, "minio-init: bucket %q ready\n", name)
	}
	os.Exit(0)
}

// bucketExists reports whether err is the S3 "bucket already there" response —
// idempotent re-runs (or a bucket MinIO persisted) must not fail init.
func bucketExists(err error) bool {
	var owned *s3types.BucketAlreadyOwnedByYou
	var exists *s3types.BucketAlreadyExists
	return errors.As(err, &owned) || errors.As(err, &exists)
}
