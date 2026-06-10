package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// receiveS3 polls an S3 bucket (LocalStack in the bench topology), counts
// every line of every object, and deletes drained objects — the receiver
// owns the output bucket, and destructive drain keeps listings cheap and
// memory bounded on long runs. Objects only become visible when their PUT
// completes, so there is no partial-read race.
func receiveS3(cfg config, cnt *counters, val *validator) error {
	bucket := os.Getenv("RECEIVER_S3_BUCKET")
	if bucket == "" {
		return fmt.Errorf("RECEIVER_S3_BUCKET is required for s3 mode")
	}
	prefix := os.Getenv("RECEIVER_S3_PREFIX")
	endpoint := getEnv("RECEIVER_S3_ENDPOINT", getEnv("AWS_ENDPOINT_URL", "http://localstack:4566"))

	awscfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}
	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		// The emulator hostname has no per-bucket DNS, so path-style is required.
		o.UsePathStyle = true
	})

	onLine := newCloudOnLine(cnt, val, cfg)
	fmt.Fprintf(os.Stderr, "receiver: polling s3 bucket=%q prefix=%q endpoint=%s\n", bucket, prefix, endpoint)

	return pollLoop("s3", cloudPollInterval(), func(ctx context.Context) error {
		var continuation *string
		for {
			out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:            aws.String(bucket),
				Prefix:            aws.String(prefix),
				ContinuationToken: continuation,
			})
			if err != nil {
				return fmt.Errorf("list objects: %w", err)
			}
			deletions := make([]s3types.ObjectIdentifier, 0, len(out.Contents))
			for _, obj := range out.Contents {
				get, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    obj.Key,
				})
				if err != nil {
					return fmt.Errorf("get object %s: %w", aws.ToString(obj.Key), err)
				}
				body, err := io.ReadAll(get.Body)
				get.Body.Close()
				if err != nil {
					return fmt.Errorf("read object %s: %w", aws.ToString(obj.Key), err)
				}
				if err := countBody(body, onLine); err != nil {
					return fmt.Errorf("decode object %s: %w", aws.ToString(obj.Key), err)
				}
				deletions = append(deletions, s3types.ObjectIdentifier{Key: obj.Key})
			}
			if len(deletions) > 0 {
				if _, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(bucket),
					Delete: &s3types.Delete{Objects: deletions, Quiet: aws.Bool(true)},
				}); err != nil {
					return fmt.Errorf("delete objects: %w", err)
				}
			}
			if out.IsTruncated == nil || !*out.IsTruncated {
				return nil
			}
			continuation = out.NextContinuationToken
		}
	})
}
