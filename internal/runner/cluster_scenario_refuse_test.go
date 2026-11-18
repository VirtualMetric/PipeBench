package runner

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// fakeRefusingReceiver serves a receiver's /refuse and /metrics. refused is
// what its /metrics reports; noCounter makes it a receiver image that cannot
// refuse, whose /metrics has no refused_requests.
type fakeRefusingReceiver struct {
	refused   atomic.Int64
	asked     atomic.Int64
	noCounter bool
}

func (f *fakeRefusingReceiver) start(t *testing.T, id string) scenarioReceiver {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/refuse":
			f.asked.Add(1)
			fmt.Fprint(w, `{}`)
		case "/metrics":
			if f.noCounter {
				fmt.Fprint(w, `{"lines_received": 10}`)
				return
			}
			fmt.Fprintf(w, `{"lines_received": 10, "refused_requests": %d}`, f.refused.Load())
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return scenarioReceiver{ID: id, Port: p}
}

// A receiver_refuse step asks its receiver to refuse, and the run then holds
// each step to the requests its receiver refused during that step's refusal:
// at least the step's min, 1 when it has none. A second step on the same
// receiver counts only its own refusals, not the first step's.
func TestEachRefusalMustMeetItsOwnRequests(t *testing.T) {
	refuse := func(t *testing.T, sr *scenarioRun, i int, receiver string, min *int64) {
		t.Helper()
		ok, detail, err := sr.doStep(i, config.ScenarioStep{Do: "receiver_refuse", For: "1m", Receiver: receiver, Min: min}, &scenarioStepReport{})
		if err != nil || !ok {
			t.Fatalf("step %d: ok %v, %q, %v", i, ok, detail, err)
		}
	}
	two := int64(2)

	t.Run("a refusal that met enough requests", func(t *testing.T) {
		f := &fakeRefusingReceiver{}
		f.refused.Store(5)
		sr := &scenarioRun{counts: map[string]int64{}}
		sr.receivers = []scenarioReceiver{f.start(t, "b")}
		refuse(t, sr, 4, "b", &two)
		if f.asked.Load() != 1 || len(sr.refused) != 1 {
			t.Fatalf("the step asked the receiver %d times and recorded %d refusals, want 1 and 1", f.asked.Load(), len(sr.refused))
		}
		f.refused.Add(3)
		sr.checkRefusals()
		if len(sr.failures) != 0 || sr.counts["refused_requests_step_4"] != 3 {
			t.Fatalf("failures %q, counts %v: want none, and 3 refused during the step", sr.failures, sr.counts)
		}
	})

	t.Run("a second refusal that nothing met", func(t *testing.T) {
		f := &fakeRefusingReceiver{}
		sr := &scenarioRun{counts: map[string]int64{}}
		sr.receivers = []scenarioReceiver{f.start(t, "b")}
		refuse(t, sr, 4, "b", nil)
		f.refused.Add(2)
		refuse(t, sr, 7, "b", nil)
		sr.checkRefusals()
		if len(sr.failures) != 1 || !strings.Contains(sr.failures[0], "step 7") || !strings.Contains(sr.failures[0], "receiver b refused 0 requests") {
			t.Fatalf("failures %q: want step 7 alone to fail", sr.failures)
		}
	})

	t.Run("a refusal that met fewer than its min", func(t *testing.T) {
		f := &fakeRefusingReceiver{}
		sr := &scenarioRun{counts: map[string]int64{}}
		sr.receivers = []scenarioReceiver{f.start(t, "")}
		five := int64(5)
		refuse(t, sr, 4, "", &five)
		f.refused.Add(3)
		sr.checkRefusals()
		if len(sr.failures) != 1 || !strings.Contains(sr.failures[0], "refused 3 requests, fewer than the 5") {
			t.Fatalf("failures %q", sr.failures)
		}
	})

	t.Run("a receiver image that cannot refuse", func(t *testing.T) {
		f := &fakeRefusingReceiver{noCounter: true}
		sr := &scenarioRun{counts: map[string]int64{}}
		sr.receivers = []scenarioReceiver{f.start(t, "")}
		ok, detail, err := sr.doStep(4, config.ScenarioStep{Do: "receiver_refuse", For: "1m"}, &scenarioStepReport{})
		if err != nil || ok || !strings.Contains(detail, "cannot refuse") {
			t.Fatalf("ok %v, %q, %v: want the step to fail", ok, detail, err)
		}
	})
}
