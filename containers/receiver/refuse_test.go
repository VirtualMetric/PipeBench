package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A receiver that is asked to refuse answers 503 to every data post until the
// time is up, on each of its HTTP data endpoints, and counts nothing that it
// refused. A request that posts no data is not counted as refused. A refusal
// of 0 ends it at once.
func TestTheReceiverRefusesDataWhileAsked(t *testing.T) {
	t.Cleanup(func() { refuseUntil.Store(0) })
	control := httptest.NewServer(http.HandlerFunc(handleRefuse))
	t.Cleanup(control.Close)
	ask := func(t *testing.T, d string) {
		t.Helper()
		resp, err := http.Post(control.URL+"/refuse?for="+d, "text/plain", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("refuse for %s: %v %v", d, resp, err)
		}
		resp.Body.Close()
	}

	endpoints := []struct {
		name    string
		handler func(*counters) http.Handler
		path    string
		body    func(n int) string
	}{
		{"the http mode", func(cnt *counters) http.Handler { return newHTTPReceiveHandler(config{}, cnt, newValidator()) }, "/",
			func(n int) string { return "line-" + strings.Repeat("x", n) + "\n" }},
		{"the http mode's bulk path", func(cnt *counters) http.Handler { return newHTTPReceiveHandler(config{}, cnt, newValidator()) }, "/logs/_bulk",
			func(n int) string { return "{\"index\":{}}\n{\"line\":\"" + strings.Repeat("x", n) + "\"}\n" }},
		{"the data endpoint", func(cnt *counters) http.Handler { return newHTTPDataHandler(config{}, cnt, newValidator()) }, "/",
			func(n int) string { return "line-" + strings.Repeat("x", n) + "\n" }},
	}
	for _, e := range endpoints {
		t.Run(e.name, func(t *testing.T) {
			t.Cleanup(func() { refuseUntil.Store(0) })
			cnt := &counters{}
			data := httptest.NewServer(e.handler(cnt))
			t.Cleanup(data.Close)
			post := func(n int) int {
				t.Helper()
				resp, err := http.Post(data.URL+e.path, "text/plain", strings.NewReader(e.body(n)))
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				return resp.StatusCode
			}

			if code := post(1); code != http.StatusOK {
				t.Fatalf("before the refusal the receiver answered %d", code)
			}
			ask(t, "1h")
			before := refusedRequests.Load()
			if code := post(2); code != http.StatusServiceUnavailable {
				t.Fatalf("while refusing the receiver answered %d, want 503", code)
			}
			if resp, err := http.Get(data.URL + e.path); err == nil {
				resp.Body.Close()
			}
			if got := refusedRequests.Load(); got != before+1 {
				t.Fatalf("the receiver counted %d refused requests, want one more than %d: the post only, not the get", got, before)
			}
			ask(t, "0s")
			if code := post(3); code != http.StatusOK {
				t.Fatalf("after the refusal ended the receiver answered %d", code)
			}
			if lines, _, _, _ := cnt.totals(); lines != 2 {
				t.Fatalf("the receiver counted %d lines, want 2: the refused one is not counted", lines)
			}
		})
	}
}
