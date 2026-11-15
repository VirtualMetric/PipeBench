package main

// disk_baseline mode: how fast the disk under GENERATOR_DISK_PATH (default /disk) syncs,
// so that a latency case can tell the subject's latency apart from the disk's own. It runs before
// the subject starts, on the folder that the harness later mounts as the subject's data folder.
// One writer runs at a time, for GENERATOR_DISK_SECONDS (default 10) a test:
//
//   - write_4k_fdatasync: 4 KiB writes into a file of 64 MiB written beforehand, each followed by an
//     fdatasync, from the start of the file over and over: what a write-ahead log on a file it
//     allocated does.
//   - new_file_4k_fsync: a new file, 4 KiB written into it, an fsync, a close and its removal: what a
//     store that writes each record into a file of its own does.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	diskBlock    = 4 << 10
	diskFileSize = 64 << 20
)

// diskBaseline is the result of disk_baseline mode.
type diskBaseline struct {
	Path              string     `json:"path"`
	Seconds           float64    `json:"seconds_per_test"`
	WriteFdatasync    *latencyUs `json:"write_4k_fdatasync_us"`
	WriteFdatasyncOps float64    `json:"write_4k_fdatasync_per_sec"`
	NewFileFsync      *latencyUs `json:"new_file_4k_fsync_us"`
	NewFileFsyncOps   float64    `json:"new_file_4k_fsync_per_sec"`
}

// runDiskBaseline measures the disk and prints the result as JSON on stdout.
func runDiskBaseline() error {
	dir := getEnv("GENERATOR_DISK_PATH", "/disk")
	seconds := 10.0
	if v, err := strconv.ParseFloat(getEnv("GENERATOR_DISK_SECONDS", "10"), 64); err == nil && v > 0 {
		seconds = v
	}
	res, err := measureDisk(dir, time.Duration(seconds*float64(time.Second)))
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		DiskBaseline *diskBaseline `json:"disk_baseline"`
	}{res})
}

// measureDisk runs both tests in dir, each for d, and removes what it wrote.
func measureDisk(dir string, d time.Duration) (*diskBaseline, error) {
	work, err := os.MkdirTemp(dir, ".disk-baseline-")
	if err != nil {
		return nil, fmt.Errorf("disk_baseline: %w", err)
	}
	defer os.RemoveAll(work)

	res := &diskBaseline{Path: dir, Seconds: d.Seconds()}
	block := make([]byte, diskBlock)
	for i := range block {
		block[i] = byte(i)
	}

	// The file of the first test, written and synced before it is timed
	f, err := os.OpenFile(filepath.Join(work, "log"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("disk_baseline: %w", err)
	}
	defer f.Close()
	for off := int64(0); off < diskFileSize; off += diskBlock {
		if _, err := f.WriteAt(block, off); err != nil {
			return nil, fmt.Errorf("disk_baseline: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("disk_baseline: %w", err)
	}

	var h durHist
	start := time.Now()
	for off := int64(0); time.Since(start) < d; off = (off + diskBlock) % diskFileSize {
		t := time.Now()
		if _, err := f.WriteAt(block, off); err != nil {
			return nil, fmt.Errorf("disk_baseline: %w", err)
		}
		if err := datasync(f); err != nil {
			return nil, fmt.Errorf("disk_baseline: %w", err)
		}
		h.record(time.Since(t))
	}
	res.WriteFdatasync, res.WriteFdatasyncOps = h.summary(), float64(h.count)/time.Since(start).Seconds()

	h = durHist{}
	start = time.Now()
	for i := 0; time.Since(start) < d; i++ {
		t := time.Now()
		name := filepath.Join(work, "file-"+strconv.Itoa(i))
		nf, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, fmt.Errorf("disk_baseline: %w", err)
		}
		_, err = nf.Write(block)
		if err == nil {
			err = nf.Sync()
		}
		if cerr := nf.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, fmt.Errorf("disk_baseline: %w", err)
		}
		h.record(time.Since(t))
		_ = os.Remove(name)
	}
	res.NewFileFsync, res.NewFileFsyncOps = h.summary(), float64(h.count)/time.Since(start).Seconds()
	return res, nil
}
