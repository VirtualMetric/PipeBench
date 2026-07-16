package main

import (
	"bufio"
	"io"
	"strings"
	"testing"
	"time"
)

// chunkedReader yields one fixed chunk per Read call with a small delay
// between chunks, so consecutive reads land on distinct timestamps.
type chunkedReader struct {
	chunks []string
	delay  time.Duration
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if len(c.chunks) == 0 {
		return 0, io.EOF
	}
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	n := copy(p, c.chunks[0])
	if n == len(c.chunks[0]) {
		c.chunks = c.chunks[1:]
	} else {
		c.chunks[0] = c.chunks[0][n:]
	}
	return n, nil
}

func TestStampingReader(t *testing.T) {
	// A burst far below recordLine's 1024-line sampling threshold must still
	// produce a non-empty receive window (lastNs > firstNs) when the data
	// arrives across multiple reads — the regression behind EPS 0 on
	// 1,000-line crash/restart cases.
	shard := &connStats{}
	src := &chunkedReader{
		chunks: []string{"a\nb\n", "c\nd\n"},
		delay:  5 * time.Millisecond,
	}
	scanner := bufio.NewScanner(&stampingReader{r: src, shard: shard})
	lines := 0
	for scanner.Scan() {
		shard.recordLine(int64(len(scanner.Bytes())) + 1)
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if lines != 4 {
		t.Fatalf("lines = %d, want 4", lines)
	}
	first, last := shard.firstNs.Load(), shard.lastNs.Load()
	if first == 0 {
		t.Fatal("firstNs not stamped")
	}
	if last <= first {
		t.Fatalf("lastNs (%d) <= firstNs (%d): receive window empty", last, first)
	}
}

func TestStampingReaderFirstNsStable(t *testing.T) {
	// firstNs is stamped once on the first read and never moves.
	shard := &connStats{}
	sr := &stampingReader{r: strings.NewReader("x\n"), shard: shard}
	buf := make([]byte, 16)
	if _, err := sr.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	first := shard.firstNs.Load()
	time.Sleep(2 * time.Millisecond)
	sr.r = strings.NewReader("y\n")
	if _, err := sr.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := shard.firstNs.Load(); got != first {
		t.Fatalf("firstNs moved: %d → %d", first, got)
	}
	if shard.lastNs.Load() <= first {
		t.Fatal("lastNs not refreshed on second read")
	}
}
