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
// changed since the last poll (keptObjects.poll).
func receiveS3Kept(client *s3.Client, bucket, prefix string, cfg config, cnt *counters) error {
	snap := newSnapshotTracker(seqTrack, cfg.LatencySampleEvery, cnt.newShard())
	bytesPerLine := int64(getEnvInt("RECEIVER_S3_BYTES_PER_LINE", 256))
	kept := newKeptObjects(s3Objects{client: client, bucket: bucket, prefix: prefix})
	fmt.Fprintf(os.Stderr, "receiver: s3: keeping the objects of bucket=%q prefix=%q, and counting what the bucket holds\n", bucket, prefix)
	return pollLoop("s3", cloudPollInterval(), func(ctx context.Context) error {
		parts, err := kept.poll(ctx, time.Now().UnixNano())
		if err != nil {
			return err
		}
		snap.rebuild(parts, bytesPerLine)
		return nil
	})
}

// objectInfo is a listed object: its key and its ETag.
type objectInfo struct {
	key, etag string
}

// objectStore lists a bucket's objects and reads one.
type objectStore interface {
	list(ctx context.Context) ([]objectInfo, error)
	get(ctx context.Context, key string) ([]byte, error)
}

// s3Objects is an objectStore of an S3 bucket, below a prefix.
type s3Objects struct {
	client         *s3.Client
	bucket, prefix string
}

func (o s3Objects) list(ctx context.Context) ([]objectInfo, error) {
	var out []objectInfo
	var continuation *string
	for {
		page, err := o.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(o.bucket),
			Prefix:            aws.String(o.prefix),
			ContinuationToken: continuation,
		})
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			out = append(out, objectInfo{key: aws.ToString(obj.Key), etag: aws.ToString(obj.ETag)})
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			return out, nil
		}
		continuation = page.NextContinuationToken
	}
}

func (o s3Objects) get(ctx context.Context, key string) ([]byte, error) {
	get, err := o.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(o.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer get.Body.Close()
	return io.ReadAll(get.Body)
}

// keptObjects keeps the lines of each object that a bucket holds, by its key.
type keptObjects struct {
	store   objectStore
	objects map[string]*keptObject
}

// keptObject is an object's ETag, and its lines with the time it was first seen.
type keptObject struct {
	etag string
	part snapshotPart
}

func newKeptObjects(store objectStore) *keptObjects {
	return &keptObjects{store: store, objects: map[string]*keptObject{}}
}

// poll lists the bucket, reads each object that is new or whose ETag changed,
// forgets the objects that are gone, and answers the lines of every object the
// bucket holds. An object written again keeps the time it was first seen, and
// its new lines replace its old ones.
func (k *keptObjects) poll(ctx context.Context, now int64) ([]snapshotPart, error) {
	listed, err := k.store.list(ctx)
	if err != nil {
		return nil, err
	}
	there := make(map[string]bool, len(listed))
	for _, obj := range listed {
		there[obj.key] = true
		if old, ok := k.objects[obj.key]; ok && old.etag == obj.etag {
			continue
		}
		body, err := k.store.get(ctx, obj.key)
		if err != nil {
			return nil, err
		}
		lines, err := bodyLines(body)
		if err != nil {
			return nil, fmt.Errorf("decode object %s: %w", obj.key, err)
		}
		// A record of several lines written as JSON holds them escaped
		lines = valueLines(lines, true)
		seen := now
		if old, ok := k.objects[obj.key]; ok {
			seen = old.part.seen
		}
		k.objects[obj.key] = &keptObject{etag: obj.etag, part: snapshotPart{name: obj.key, seen: seen, lines: lines}}
	}
	parts := make([]snapshotPart, 0, len(k.objects))
	for key, obj := range k.objects {
		if !there[key] {
			delete(k.objects, key)
			continue
		}
		parts = append(parts, obj.part)
	}
	return parts, nil
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
