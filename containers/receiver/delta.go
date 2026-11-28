package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
)

// receiveDelta is RECEIVER_MODE=delta: it reads a Delta Lake table that the
// subject writes, from an S3 bucket such as the case's MinIO
// (RECEIVER_DELTA_BUCKET and RECEIVER_DELTA_PREFIX, the table's folder in it),
// or from a local folder (RECEIVER_DELTA_DIR). Each poll reads the table's log
// for new commits, keeps the set of data files that the table holds now (each
// add, less each remove), reads the lines of the files it has not read yet
// from one string column (RECEIVER_DELTA_COLUMN, "message" by default), and
// counts the lines of every file that the table holds (snapshot.go). So a row
// that the table holds twice counts as a duplicate, and a row that a
// compaction rewrote counts once.
//
// It reads the log's JSON commits only, from the first one. A table that the
// subject made keeps them for days, so a run never needs the checkpoints.
func receiveDelta(cfg config, cnt *counters, _ *validator) error {
	store, where, err := deltaStoreFromEnv()
	if err != nil {
		return err
	}
	column := getEnv("RECEIVER_DELTA_COLUMN", "message")
	bytesPerLine := int64(getEnvInt("RECEIVER_DELTA_BYTES_PER_ROW", 256))
	tbl := newDeltaTable(store, column)
	snap := newSnapshotTracker(seqTrack, cfg.LatencySampleEvery, cnt.newShard())
	fmt.Fprintf(os.Stderr, "receiver: delta: reading the table at %s, column %q\n", where, column)
	interval := cloudPollInterval()
	for {
		if err := tbl.poll(); err != nil {
			fmt.Fprintf(os.Stderr, "receiver: delta: %v\n", err)
		} else {
			snap.rebuild(tbl.parts(), bytesPerLine)
		}
		time.Sleep(interval)
	}
}

// deltaStore reads the files of one table, by their path below the table.
type deltaStore interface {
	// read answers a file's bytes, and errNoFile when it is not there.
	read(rel string) ([]byte, error)
}

var errNoFile = errors.New("no such file")

// dirStore is a table in a local folder.
type dirStore string

func (d dirStore) read(rel string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(string(d), filepath.FromSlash(rel)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, errNoFile
	}
	return data, err
}

// s3Store is a table in an S3 bucket, below a prefix.
type s3Store struct {
	client         *s3.Client
	bucket, prefix string
}

func (s s3Store) read(rel string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(path.Join(s.prefix, rel))})
	if err != nil {
		var nf *s3types.NoSuchKey
		if errors.As(err, &nf) {
			return nil, errNoFile
		}
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// deltaStoreFromEnv answers the table's store, and where it is, for the log.
func deltaStoreFromEnv() (deltaStore, string, error) {
	if bucket := os.Getenv("RECEIVER_DELTA_BUCKET"); bucket != "" {
		prefix := strings.Trim(getEnv("RECEIVER_DELTA_PREFIX", "events"), "/")
		endpoint := getEnv("RECEIVER_S3_ENDPOINT", getEnv("AWS_ENDPOINT_URL", "http://minio:9000"))
		awscfg, err := awsconfig.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, "", fmt.Errorf("aws config: %w", err)
		}
		client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
		return s3Store{client: client, bucket: bucket, prefix: prefix}, fmt.Sprintf("s3://%s/%s (%s)", bucket, prefix, endpoint), nil
	}
	dir := getEnv("RECEIVER_DELTA_DIR", "/data/lake/events")
	return dirStore(dir), dir, nil
}

// deltaTable follows one Delta table's log.
type deltaTable struct {
	store  deltaStore
	column string
	// next is the version of the next commit to read.
	next int64
	// active holds the data files that the table holds now, by their path in
	// the log, and files the lines read from each one, kept while it is active.
	active map[string]bool
	files  map[string]*snapshotPart
}

func newDeltaTable(store deltaStore, column string) *deltaTable {
	return &deltaTable{store: store, column: column, active: map[string]bool{}, files: map[string]*snapshotPart{}}
}

// deltaAction is the part of a log line that the receiver reads.
type deltaAction struct {
	Add *struct {
		Path string `json:"path"`
		// DeletionVector marks rows of the file that the table no longer holds
		DeletionVector json.RawMessage `json:"deletionVector"`
	} `json:"add"`
	Remove *struct {
		Path string `json:"path"`
	} `json:"remove"`
}

// poll reads the commits after the last one read, then the data files that
// are new to the table. A commit that is not there yet ends the reading: the
// next poll goes on from it.
func (d *deltaTable) poll() error {
	for {
		data, err := d.store.read(fmt.Sprintf("_delta_log/%020d.json", d.next))
		if errors.Is(err, errNoFile) {
			break
		}
		if err != nil {
			return fmt.Errorf("read commit %d: %w", d.next, err)
		}
		if err := d.apply(data); err != nil {
			return fmt.Errorf("commit %d: %w", d.next, err)
		}
		d.next++
	}
	now := time.Now().UnixNano()
	for p := range d.active {
		if _, ok := d.files[p]; ok {
			continue
		}
		lines, err := d.readFile(p)
		if err != nil {
			return fmt.Errorf("data file %s: %w", p, err)
		}
		d.files[p] = &snapshotPart{name: p, seen: now, lines: lines}
	}
	for p := range d.files {
		if !d.active[p] {
			delete(d.files, p)
		}
	}
	return nil
}

// apply applies one commit's adds and removes to the active set.
func (d *deltaTable) apply(commit []byte) error {
	sc := bufio.NewScanner(bytes.NewReader(commit))
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var a deltaAction
		if err := json.Unmarshal(line, &a); err != nil {
			return fmt.Errorf("a line of the log is not JSON: %w", err)
		}
		if a.Add != nil {
			// The receiver reads every row of a file, so the rows that a deletion
			// vector removed would count as repeats: it refuses such a table
			if dv := bytes.TrimSpace(a.Add.DeletionVector); len(dv) > 0 && !bytes.Equal(dv, []byte("null")) {
				return fmt.Errorf("the file %s has a deletion vector, which this receiver does not read: turn the table's deletion vectors off", a.Add.Path)
			}
			d.active[a.Add.Path] = true
		}
		if a.Remove != nil {
			delete(d.active, a.Remove.Path)
		}
	}
	return sc.Err()
}

// readFile reads the column's values of one data file. A path in the log is
// relative to the table and escaped as a URI.
func (d *deltaTable) readFile(p string) ([][]byte, error) {
	rel, err := url.PathUnescape(p)
	if err != nil {
		return nil, err
	}
	data, err := d.store.read(rel)
	if err != nil {
		return nil, err
	}
	values, err := parquetColumn(bytes.NewReader(data), int64(len(data)), d.column)
	if err != nil {
		return nil, err
	}
	return valueLines(values, false), nil
}

// parquetColumn reads every value of one top-level column of a Parquet file,
// in order. A null value is left out.
func parquetColumn(r io.ReaderAt, size int64, column string) ([][]byte, error) {
	pf, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, err
	}
	leaf, ok := pf.Schema().Lookup(column)
	if !ok {
		return nil, fmt.Errorf("the file has no column %q", column)
	}
	var out [][]byte
	values := make([]parquet.Value, 1024)
	for _, rg := range pf.RowGroups() {
		pages := rg.ColumnChunks()[leaf.ColumnIndex].Pages()
		for {
			page, err := pages.ReadPage()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = pages.Close()
				return nil, err
			}
			vr := page.Values()
			for {
				n, err := vr.ReadValues(values)
				for _, v := range values[:n] {
					if !v.IsNull() {
						out = append(out, append([]byte(nil), v.ByteArray()...))
					}
				}
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					parquet.Release(page)
					_ = pages.Close()
					return nil, err
				}
			}
			parquet.Release(page)
		}
		if err := pages.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// parts answers the data files that the table holds now, with their lines.
func (d *deltaTable) parts() []snapshotPart {
	out := make([]snapshotPart, 0, len(d.files))
	for _, p := range d.files {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}
