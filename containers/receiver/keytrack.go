package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sync"
)

// keyTrack counts the key of every received line when RECEIVER_TRACK_KEYS is
// set, else nil.
var keyTrack *keyTracker

// keyTracker serves a case that compares the rows a database holds with what
// arrived. Each received line carries the key of the row it came from, such as
// evt-1234. The tracker finds the keys with a regular expression
// (RECEIVER_TRACK_KEYS): every match, or the first group of every match when
// the expression has one. It counts each distinct key once per line, so a line
// that carries several rows counts each of them, and a record that mentions
// its key twice is not taken for a duplicate. /keys returns the counts, so the
// harness can tell which rows never arrived and which arrived twice.
type keyTracker struct {
	re *regexp.Regexp

	mu      sync.Mutex
	counts  map[string]int64
	without int64 // lines in which the expression found no key
}

func newKeyTracker(expr string) (*keyTracker, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("RECEIVER_TRACK_KEYS %q: %w", expr, err)
	}
	return &keyTracker{re: re, counts: map[string]int64{}}, nil
}

func (k *keyTracker) record(line []byte) {
	matches := k.re.FindAllSubmatch(line, -1)
	keys := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		key := m[0]
		if len(m) > 1 {
			key = m[1]
		}
		if len(key) > 0 {
			keys[string(key)] = struct{}{}
		}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(keys) == 0 {
		k.without++
		return
	}
	for key := range keys {
		k.counts[key]++
	}
}

// keysReport is the /keys answer.
type keysReport struct {
	// Keys maps every key seen to the number of lines that carried it.
	Keys map[string]int64 `json:"keys"`
	// LinesWithoutKey counts the lines in which the expression found no key.
	LinesWithoutKey int64 `json:"lines_without_key"`
}

func (k *keyTracker) report() keysReport {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := keysReport{Keys: make(map[string]int64, len(k.counts)), LinesWithoutKey: k.without}
	for key, n := range k.counts {
		out.Keys[key] = n
	}
	return out
}

// registerKeyHandler adds /keys to mux.
func registerKeyHandler(mux *http.ServeMux, k *keyTracker) {
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(k.report())
	})
}
