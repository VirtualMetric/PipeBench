package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestRunHTTPSingleBatchesDistinctLines guards the HTTP batcher against two
// regressions: reused-buffer aliasing (sequenced mode rewrites the CONN=/SEQ=
// prefix in place, so un-copied batch entries would all alias the final line)
// and newline doubling (generated lines already end in '\n'; joining batch
// entries with another '\n' inserted a blank line between records).
func TestRunHTTPSingleBatchesDistinctLines(t *testing.T) {
	var mu sync.Mutex
	var lines [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading body: %v", err)
			return
		}
		mu.Lock()
		lines = append(lines, bytes.Split(body, []byte("\n"))...)
		mu.Unlock()
	}))
	defer srv.Close()

	// 250 lines = two full 100-line batches plus a partial final flush.
	const total = 250
	cfg := config{
		Mode:        "http",
		Target:      srv.URL,
		TotalLines:  total,
		LineSize:    64,
		Format:      "raw",
		Sequenced:   true,
		Connections: 1,
	}

	sent, _, err := runHTTPSingle(cfg, &sendClock{})
	if err != nil {
		t.Fatalf("runHTTPSingle: %v", err)
	}
	if sent != total {
		t.Fatalf("lines sent: got %d, want %d", sent, total)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lines) != total {
		t.Fatalf("lines received: got %d, want %d (blank or missing lines)", len(lines), total)
	}
	seen := make(map[string]struct{}, total)
	for i, l := range lines {
		if len(l) == 0 {
			t.Fatalf("line %d is blank — newline-doubling regression", i)
		}
		var conn int
		var seq int64
		if _, err := fmt.Sscanf(string(l), "CONN=%d SEQ=%d", &conn, &seq); err != nil {
			t.Fatalf("line %d %q lacks CONN=/SEQ= prefix: %v", i, l, err)
		}
		key := fmt.Sprintf("%d/%d", conn, seq)
		if _, dup := seen[key]; dup {
			t.Fatalf("duplicate sequence %s — buffer-aliasing regression", key)
		}
		seen[key] = struct{}{}
	}
}
