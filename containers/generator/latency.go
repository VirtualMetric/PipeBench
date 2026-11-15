package main

import (
	"math"
	"math/bits"
	"time"
)

// durHist counts durations in log-linear buckets: 16 for each power of two of nanoseconds, from
// 2^10 ns (about 1 µs) to 2^42 ns (about 73 minutes). A reported value is the middle of its bucket,
// within about 3% of the true one, and the maximum is exact. It is the scheme of the director's own
// latency histograms, so the two can be read side by side. It is not safe for concurrent
// use: its owner holds a lock.
type durHist struct {
	buckets         [histBuckets]uint64
	count, sum, max uint64
}

const (
	histSubBits = 4
	histSub     = 1 << histSubBits
	histMinExp  = 10
	histMaxExp  = 42
	histBuckets = 1 + (histMaxExp-histMinExp+1)*histSub + 1
)

func histBucketOf(ns uint64) int {
	if ns < 1<<histMinExp {
		return 0
	}
	exp := bits.Len64(ns) - 1
	if exp > histMaxExp {
		return histBuckets - 1
	}
	return 1 + (exp-histMinExp)*histSub + int(ns>>uint(exp-histSubBits))&(histSub-1)
}

func histBucketLow(i int) uint64 {
	if i <= 0 {
		return 0
	}
	if i >= histBuckets-1 {
		return 1 << (histMaxExp + 1)
	}
	i--
	exp := uint(i/histSub + histMinExp)
	return 1<<exp + uint64(i%histSub)<<(exp-histSubBits)
}

func histBucketHigh(i int) uint64 {
	if i >= histBuckets-1 {
		return histBucketLow(i)
	}
	return histBucketLow(i + 1)
}

// record counts one duration. A negative one counts as zero.
func (h *durHist) record(d time.Duration) {
	ns := uint64(max(d, 0))
	h.buckets[histBucketOf(ns)]++
	h.count++
	h.sum += ns
	h.max = max(h.max, ns)
}

// quantile returns the value below which q of the counted values fall, in nanoseconds.
func (h *durHist) quantile(q float64) uint64 {
	if h.count == 0 {
		return 0
	}
	rank := min(max(uint64(q*float64(h.count)+0.5), 1), h.count)
	if rank == h.count {
		return h.max
	}
	seen := uint64(0)
	for i, n := range h.buckets {
		seen += n
		if seen >= rank {
			low, high := histBucketLow(i), histBucketHigh(i)
			return min(low+(high-low)/2, max(h.max, low))
		}
	}
	return h.max
}

// latencyUs is a summary of durations in microseconds.
type latencyUs struct {
	Count  uint64  `json:"count"`
	MeanUs float64 `json:"mean_us"`
	P50Us  float64 `json:"p50_us"`
	P90Us  float64 `json:"p90_us"`
	P99Us  float64 `json:"p99_us"`
	P999Us float64 `json:"p999_us"`
	MaxUs  float64 `json:"max_us"`
}

// summary returns the histogram's summary, or nil when it counted nothing.
func (h *durHist) summary() *latencyUs {
	if h.count == 0 {
		return nil
	}
	return &latencyUs{
		Count:  h.count,
		MeanUs: microseconds(h.sum / h.count),
		P50Us:  microseconds(h.quantile(0.50)),
		P90Us:  microseconds(h.quantile(0.90)),
		P99Us:  microseconds(h.quantile(0.99)),
		P999Us: microseconds(h.quantile(0.999)),
		MaxUs:  microseconds(h.max),
	}
}

// microseconds returns ns nanoseconds in microseconds, to a tenth.
func microseconds(ns uint64) float64 {
	return math.Round(float64(ns)/100) / 10
}
