package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A receiver that is asked to refuse answers 503 to every data request until the time is up, and
// counts nothing that it refused. A refusal of 0 ends it at once.
func TestTheReceiverRefusesDataWhileAsked(t *testing.T) {
	t.Cleanup(func() { refuseUntil.Store(0) })
	cnt := &counters{}
	data := httptest.NewServer(newHTTPReceiveHandler(config{}, cnt, newValidator()))
	t.Cleanup(data.Close)
	control := httptest.NewServer(http.HandlerFunc(handleRefuse))
	t.Cleanup(control.Close)

	post := func(body string) int {
		t.Helper()
		resp, err := http.Post(data.URL+"/", "text/plain", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	ask := func(d string) {
		t.Helper()
		resp, err := http.Post(control.URL+"/refuse?for="+d, "text/plain", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("refuse for %s: %v %v", d, resp, err)
		}
		resp.Body.Close()
	}

	if code := post("line-1\n"); code != http.StatusOK {
		t.Fatalf("before the refusal the receiver answered %d", code)
	}
	ask("1h")
	before := refusedRequests.Load()
	if code := post("line-2\n"); code != http.StatusServiceUnavailable {
		t.Fatalf("while refusing the receiver answered %d, want 503", code)
	}
	if refusedRequests.Load() != before+1 {
		t.Fatalf("the receiver counted %d refused requests, want one more than %d", refusedRequests.Load(), before)
	}
	ask("0s")
	if code := post("line-3\n"); code != http.StatusOK {
		t.Fatalf("after the refusal ended the receiver answered %d", code)
	}
	if lines, _, _, _ := cnt.totals(); lines != 2 {
		t.Fatalf("the receiver counted %d lines, want 2: the refused one is not counted", lines)
	}
}
