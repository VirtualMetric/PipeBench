package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// A receiver can refuse for a while, as a destination that is down for a moment does: its HTTP data
// endpoints answer 503 Service Unavailable, with a Retry-After of one second, until the time is up.
// The harness asks for it with POST /refuse?for=<duration> on the metrics port. It never drops data
// that it took: a request that it refuses has delivered nothing, so the sender keeps its records and
// sends them again.

// refuseUntil is when the refusal ends, in Unix nanoseconds. Zero means that the receiver takes data.
var refuseUntil atomic.Int64

// refusedRequests counts the requests that the receiver refused.
var refusedRequests atomic.Int64

// refusing reports whether the receiver refuses data now.
func refusing() bool { return time.Now().UnixNano() < refuseUntil.Load() }

// refuseIfAsked answers 503 and reports true while the receiver refuses data.
func refuseIfAsked(w http.ResponseWriter) bool {
	if !refusing() {
		return false
	}
	refusedRequests.Add(1)
	w.Header().Set("Retry-After", "1")
	w.WriteHeader(http.StatusServiceUnavailable)
	return true
}

// handleRefuse is POST /refuse?for=<duration>. A duration of 0 ends a refusal at once.
func handleRefuse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	d, err := time.ParseDuration(r.URL.Query().Get("for"))
	if err != nil || d < 0 {
		http.Error(w, "for must be a duration of 0 or more, such as 40s", http.StatusBadRequest)
		return
	}
	until := time.Now().Add(d)
	refuseUntil.Store(until.UnixNano())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"until": until.UTC().Format(time.RFC3339Nano), "refused_so_far": refusedRequests.Load()})
}
