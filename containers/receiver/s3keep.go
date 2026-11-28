package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// receiveS3Kept is the s3 mode with RECEIVER_S3_KEEP=true: it leaves the
// objects in the bucket, and counts what the bucket holds now (snapshot.go).
// The plain s3 mode deletes each object once it has read it, so an object that
// the subject writes again under its name counts twice there. Here it counts
// once, as the bucket holds it once: the second write replaced the first. A
// subject whose writes are safe to repeat names each object after its records,
// so this is the mode that tells whether a repeat reached the bucket as a
// second object.
//
// Each poll lists the bucket, and reads an object that is new or whose ETag
// changed since the last poll.
func receiveS3Kept(client *s3.Client, bucket, prefix string, cfg config, cnt *counters) error {
	snap := newSnapshotTracker(seqTrack, cfg.LatencySampleEvery, cnt.newShard())
	bytesPerLine := int64(getEnvInt("RECEIVER_S3_BYTES_PER_LINE", 256))
	type kept struct {
		etag string
		part snapshotPart
	}
	objects := map[string]*kept{}
	fmt.Fprintf(os.Stderr, "receiver: s3: keeping the objects of bucket=%q prefix=%q, and counting what the bucket holds\n", bucket, prefix)
	return pollLoop("s3", cloudPollInterval(), func(ctx context.Context) error {
		now := time.Now().UnixNano()
		listed := map[string]bool{}
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
			for _, obj := range out.Contents {
				key, etag := aws.ToString(obj.Key), aws.ToString(obj.ETag)
				listed[key] = true
				if k, ok := objects[key]; ok && k.etag == etag {
					continue
				}
				get, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: obj.Key})
				if err != nil {
					return fmt.Errorf("get object %s: %w", key, err)
				}
				body, err := io.ReadAll(get.Body)
				get.Body.Close()
				if err != nil {
					return fmt.Errorf("read object %s: %w", key, err)
				}
				lines, err := bodyLines(body)
				if err != nil {
					return fmt.Errorf("decode object %s: %w", key, err)
				}
				// A record of several lines written as JSON holds them escaped
				lines = valueLines(lines, true)
				seen := now
				if k, ok := objects[key]; ok {
					seen = k.part.seen
				}
				objects[key] = &kept{etag: etag, part: snapshotPart{name: key, seen: seen, lines: lines}}
			}
			if out.IsTruncated == nil || !*out.IsTruncated {
				break
			}
			continuation = out.NextContinuationToken
		}
		parts := make([]snapshotPart, 0, len(objects))
		for key, k := range objects {
			if !listed[key] {
				delete(objects, key)
				continue
			}
			parts = append(parts, k.part)
		}
		snap.rebuild(parts, bytesPerLine)
		return nil
	})
}

// bodyLines splits an object's body into its lines, as countBody does: gzip
// is found by its magic bytes, and blank lines are left out.
func bodyLines(body []byte) ([][]byte, error) {
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		decoded, err := io.ReadAll(zr)
		zr.Close()
		if err != nil {
			return nil, fmt.Errorf("gunzip: %w", err)
		}
		body = decoded
	}
	var out [][]byte
	for line := range bytes.SplitSeq(body, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) > 0 {
			out = append(out, append([]byte(nil), line...))
		}
	}
	return out, nil
}
