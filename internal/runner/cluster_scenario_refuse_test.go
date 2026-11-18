package runner

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A receiver_refuse step that no request reached tested no refusal, so the
// run fails. A receiver that refused requests passes, and the run counts them.
func TestARefusalThatNoRequestReachedFailsTheRun(t *testing.T) {
	receiver := func(refused int) scenarioReceiver {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/metrics" {
				http.NotFound(w, r)
				return
			}
			fmt.Fprintf(w, `{"lines_received": 10, "refused_requests": %d}`, refused)
		}))
		t.Cleanup(srv.Close)
		_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		var p int
		fmt.Sscan(port, &p)
		return scenarioReceiver{Port: p}
	}

	sr := &scenarioRun{counts: map[string]int64{}}
	sr.refused = []scenarioRefusal{{step: 4, receiver: receiver(3)}}
	sr.checkRefusals()
	if len(sr.failures) != 0 || sr.counts["refused_requests"] != 3 {
		t.Fatalf("a receiver that refused 3 requests: failures %q, counts %v", sr.failures, sr.counts)
	}

	sr = &scenarioRun{counts: map[string]int64{}}
	idle := receiver(0)
	idle.ID = "b"
	sr.refused = []scenarioRefusal{{step: 4, receiver: idle}, {step: 7, receiver: idle}}
	sr.checkRefusals()
	if len(sr.failures) != 1 || !strings.Contains(sr.failures[0], "receiver b refused no request") {
		t.Fatalf("a receiver that refused nothing: failures %q", sr.failures)
	}
	if v, ok := sr.counts["refused_requests_b"]; !ok || v != 0 {
		t.Fatalf("counts %v, want refused_requests_b 0", sr.counts)
	}
}
