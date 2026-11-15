package main

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestArrivalsRateCountsTheWholeBucketsOfAWindow(t *testing.T) {
	start := int64(1_000_000_000_000)
	ms := int64(time.Millisecond)
	tl := arrivals{StartNs: start, BucketMs: 100, Unique: []int64{10, 20, 30, 40, 50}}
	// 150 ms to 450 ms holds the whole buckets 2 and 3 (200-300 and 300-400 ms): 70 lines in 0.2 s
	if got, ok := tl.rate(start+150*ms, start+450*ms); !ok || got != 350 {
		t.Fatalf("rate = %v ok=%v, want 350", got, ok)
	}
	// A window from before the receiver's first line counts nothing for that part
	if got, ok := tl.rate(start-200*ms, start+200*ms); !ok || got != 75 {
		t.Fatalf("rate from before the start = %v ok=%v, want 30 lines in 0.4 s = 75", got, ok)
	}
	// Past the last bucket nothing arrived
	if got, ok := tl.rate(start+400*ms, start+800*ms); !ok || got != 125 {
		t.Fatalf("rate past the end = %v ok=%v, want 50 lines in 0.4 s = 125", got, ok)
	}
	if _, ok := tl.rate(start+120*ms, start+180*ms); ok {
		t.Fatal("a window inside one bucket gave a rate")
	}
	if tl.total() != 150 {
		t.Fatalf("total = %d", tl.total())
	}
}

// deliveringReceiver stands in for a target that confirms every post at once and a receiver that
// gets only half of each post's lines in time; the other half arrives once catchUpAt has passed.
type deliveringReceiver struct {
	mu        sync.Mutex
	start     time.Time
	catchUpAt time.Time
	posts     []time.Time
	lines     int64
}

func (d *deliveringReceiver) target(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	d.mu.Lock()
	d.posts = append(d.posts, time.Now())
	d.lines += int64(strings.Count(string(body), "\n") + 1)
	d.mu.Unlock()
}

func (d *deliveringReceiver) timeline(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	bucket := 100 * time.Millisecond
	tl := arrivals{StartNs: d.start.UnixNano(), BucketMs: bucket.Milliseconds()}
	add := func(at time.Time, n int64) {
		i := int(at.Sub(d.start) / bucket)
		for len(tl.Unique) <= i {
			tl.Unique = append(tl.Unique, 0)
		}
		tl.Unique[i] += n
	}
	half := int64(0)
	for _, at := range d.posts {
		add(at, 5) // half of a post of ten lines
		half += 5
	}
	if now := time.Now(); now.After(d.catchUpAt) {
		add(now, d.lines-half)
	}
	_ = json.NewEncoder(w).Encode(tl)
}

// A subject that confirms lines faster than the receiver gets them: the percent step takes its
// share of the rate the lines arrived at, says so, and starts only once the receiver caught up.
func TestAPercentStepTakesItsShareOfWhatTheReceiverGot(t *testing.T) {
	for _, from := range []string{capacityFromReceiver, capacityFromAcks} {
		t.Run(from, func(t *testing.T) {
			d := &deliveringReceiver{start: time.Now()}
			d.catchUpAt = d.start.Add(450 * time.Millisecond)
			target := httptest.NewServer(http.HandlerFunc(d.target))
			t.Cleanup(target.Close)
			recv := httptest.NewServer(http.HandlerFunc(d.timeline))
			t.Cleanup(recv.Close)

			cfg, ac, slots, stats, picker, pads := stepFixture(t, target.URL, 2)
			ac.receiver, ac.stepDrain, ac.capacityFrom = recv.URL, 5*time.Second, from
			steps := []stepSpec{
				{Name: "capacity", Kind: stepCapacity, Duration: 400 * time.Millisecond},
				{Name: "25%", Kind: stepPercent, Value: 25, Duration: 300 * time.Millisecond},
			}
			res := runAckSteps(cfg, ac, steps, slots, stats, picker, &sendClock{}, pads, time.Second)
			if len(res) != 2 || res[0].AckedPerSec <= 0 || res[1].Error != "" {
				t.Fatalf("the steps are %+v", res)
			}
			capStep, share := res[0], res[1]
			// Half of each post arrived in the window
			if r := capStep.DeliveredPerSec / capStep.AckedPerSec; r < 0.35 || r > 0.65 {
				t.Fatalf("delivered %.0f/s of %.0f/s acknowledged, want about half", capStep.DeliveredPerSec, capStep.AckedPerSec)
			}
			// The share waited for the other half
			if capStep.DrainTimedOut || capStep.DrainMs < 20 {
				t.Fatalf("the wait after the capacity step: %d ms, timed out %v", capStep.DrainMs, capStep.DrainTimedOut)
			}
			want := capStep.DeliveredPerSec
			if from == capacityFromAcks {
				want = capStep.AckedPerSec
			}
			if share.CapacityFrom != from || share.CapacityPerSec != want {
				t.Fatalf("the share took %.0f/s from %q, want %.0f/s from %q", share.CapacityPerSec, share.CapacityFrom, want, from)
			}
			if got := share.RateAsked; math.Abs(got-want/4) > want*0.001 {
				t.Fatalf("the 25%% step asked for %.0f lines a second, want %.0f", got, want/4)
			}
			// The last step does not wait
			if share.DrainMs != 0 {
				t.Fatalf("the last step waited %d ms", share.DrainMs)
			}
		})
	}
}

// Without a receiver, the capacity is the rate of acknowledged lines, and no step waits.
func TestWithoutAReceiverThePercentStepTakesTheAcknowledgedRate(t *testing.T) {
	srv := slowServer(t, 0)
	cfg, ac, slots, stats, picker, pads := stepFixture(t, srv.URL, 2)
	res := runAckSteps(cfg, ac, []stepSpec{
		{Name: "capacity", Kind: stepCapacity, Duration: 200 * time.Millisecond},
		{Name: "10%", Kind: stepPercent, Value: 10, Duration: 200 * time.Millisecond},
	}, slots, stats, picker, &sendClock{}, pads, time.Second)
	if res[1].CapacityFrom != capacityFromAcks || res[1].CapacityPerSec != res[0].AckedPerSec || res[0].DrainMs != 0 || res[0].DeliveredPerSec != 0 {
		t.Fatalf("the steps are %+v", res)
	}
}

// GENERATOR_STEP_CAPACITY picks the acknowledgements, and the receiver is the default with a receiver.
func TestTheCapacitySourceComesFromTheEnvironment(t *testing.T) {
	t.Setenv("GENERATOR_RECEIVER_METRICS", "http://receiver:9090/")
	if ac := loadAckConfig(config{}); ac.capacityFrom != capacityFromReceiver || ac.receiver != "http://receiver:9090" || ac.stepDrain != time.Minute {
		t.Fatalf("with a receiver: %q %q %s", ac.capacityFrom, ac.receiver, ac.stepDrain)
	}
	t.Setenv("GENERATOR_STEP_CAPACITY", "Acknowledgements")
	if ac := loadAckConfig(config{}); ac.capacityFrom != capacityFromAcks {
		t.Fatalf("GENERATOR_STEP_CAPACITY=acknowledgements gave %q", ac.capacityFrom)
	}
	t.Setenv("GENERATOR_RECEIVER_METRICS", "")
	t.Setenv("GENERATOR_STEP_CAPACITY", "")
	if ac := loadAckConfig(config{}); ac.capacityFrom != capacityFromAcks || ac.receiver != "" {
		t.Fatalf("without a receiver: %q %q", ac.capacityFrom, ac.receiver)
	}
}
