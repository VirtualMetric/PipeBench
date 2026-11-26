package runner

import (
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// timelineOf returns a receiver timeline of 100 ms buckets from start, with
// the given number of first arrivals in each bucket. Each bucket also holds
// five copies, which do not count.
func timelineOf(start time.Time, buckets ...int32) receiverTimeline {
	tl := receiverTimeline{StartNs: start.UnixNano(), BucketMs: 100}
	for _, n := range buckets {
		tl.Unique = append(tl.Unique, n)
		tl.All = append(tl.All, n+5)
	}
	return tl
}

// repeat returns n buckets of count arrivals each.
func repeat(count int32, n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = count
	}
	return out
}

func TestWindowRatesCountTheArrivalsInEachWindow(t *testing.T) {
	start := time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)
	marks := []scenarioMark{
		{Label: "three", At: start},
		{Label: "six", At: start.Add(60 * time.Second)},
		{Label: "end", At: start.Add(90 * time.Second)},
	}
	// 1,000 lines a bucket in the first minute, then 1,800
	tl := timelineOf(start, append(repeat(1000, 600), repeat(1800, 300)...)...)

	rates := windowRates(marks, tl)

	if got := rates["three"]; got != 10000 {
		t.Fatalf("three = %v lines a second, want 10000", got)
	}
	if got := rates["six"]; got != 18000 {
		t.Fatalf("six = %v lines a second, want 18000", got)
	}
	// The last mark's window has no end
	if _, ok := rates["end"]; ok {
		t.Fatalf("the last mark has a rate: %v", rates)
	}
}

// A window in which delivery stopped counts the rest of the window as time in
// which nothing arrived.
func TestWindowRatesCountNothingAfterTheTimelineEnds(t *testing.T) {
	start := time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)
	marks := []scenarioMark{{Label: "a", At: start}, {Label: "b", At: start.Add(60 * time.Second)}}
	// Arrivals for the first 30 seconds only
	rates := windowRates(marks, timelineOf(start, repeat(1000, 300)...))
	if got := rates["a"]; got != 5000 {
		t.Fatalf("a = %v lines a second, want 5000", got)
	}
}

func TestWindowRatesWithoutATimeline(t *testing.T) {
	start := time.Now()
	marks := []scenarioMark{{Label: "a", At: start}, {Label: "b", At: start.Add(time.Second)}}
	if rates := windowRates(marks, receiverTimeline{}); len(rates) != 0 {
		t.Fatalf("rates = %v, want none", rates)
	}
}

func TestJudgeThroughput(t *testing.T) {
	expect := []config.ScenarioThroughputExpect{{Window: "six", Over: "three", MinRatio: 1.7}}
	cases := map[string]struct {
		rates map[string]float64
		want  string // empty: the comparison passes
	}{
		"grows enough":     {map[string]float64{"three": 10000, "six": 18000}, ""},
		"exactly enough":   {map[string]float64{"three": 10000, "six": 17000}, ""},
		"grows too little": {map[string]float64{"three": 10000, "six": 16000}, "1.60 times"},
		"no count":         {map[string]float64{"three": 10000}, "could not be compared"},
		"nothing arrived":  {map[string]float64{"three": 0, "six": 5}, "nothing to compare with"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			reasons := judgeThroughput(c.rates, expect)
			switch {
			case c.want == "" && len(reasons) > 0:
				t.Fatalf("reasons = %v, want none", reasons)
			case c.want != "" && (len(reasons) != 1 || !strings.Contains(reasons[0], c.want)):
				t.Fatalf("reasons = %v, want one containing %q", reasons, c.want)
			}
		})
	}
}

func TestJudgeThroughputComparesEachEntry(t *testing.T) {
	expect := []config.ScenarioThroughputExpect{
		{Window: "four", Over: "three", MinRatio: 1.25},
		{Window: "six", Over: "three", MinRatio: 1.7},
	}
	rates := map[string]float64{"three": 10000, "four": 11000, "six": 20000}
	reasons := judgeThroughput(rates, expect)
	if len(reasons) != 1 || !strings.Contains(reasons[0], `"four"`) {
		t.Fatalf("reasons = %v, want one, about window four", reasons)
	}
}
