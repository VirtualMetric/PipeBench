package runner

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// fakeLatencyReceiver answers a receiver's /latency query with count lines.
func fakeLatencyReceiver(t *testing.T, id string, count *atomic.Int64) scenarioReceiver {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/latency" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintf(w, `{"count": %d}`, count.Load())
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

// A wait_delivery step waits until lines sent after the disruption reach every
// receiver of the case, not only the first one: each receiver takes the
// records of a target of its own, and one target can be back while another is
// not. It names the receiver that got nothing when the wait runs out.
func TestWaitDeliveryWaitsForEveryReceiver(t *testing.T) {
	var a, b atomic.Int64
	a.Store(deliveryLines)
	sr := &scenarioRun{ctx: context.Background(), measures: map[string]float64{}, lastDisruption: time.Now()}
	sr.receivers = []scenarioReceiver{fakeLatencyReceiver(t, "a", &a), fakeLatencyReceiver(t, "b", &b)}
	step := config.ScenarioStep{Do: "wait_delivery", Within: "1s", Record: "back"}

	ok, detail, err := sr.doStep(3, step, &scenarioStepReport{})
	if err != nil || ok || !strings.Contains(detail, "receiver b") || strings.Contains(detail, "receiver a") {
		t.Fatalf("with receiver b behind: ok %v, %q, %v; want a failure that names receiver b alone", ok, detail, err)
	}

	b.Store(deliveryLines)
	ok, detail, err = sr.doStep(3, step, &scenarioStepReport{})
	if err != nil || !ok {
		t.Fatalf("with both receivers back: ok %v, %q, %v", ok, detail, err)
	}
}
