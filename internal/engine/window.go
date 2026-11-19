package engine

import (
	"math"
	"time"
)

// Latency histogram: bucket i holds values in (g^(i-1)-1, g^i-1] milliseconds,
// so a quantile read from it overestimates by at most 10%.
const (
	histGrowth  = 1.1
	histBuckets = 170 // 1.1^170 ms is about 3.8 hours
)

var histUpper [histBuckets]float64
var lnGrowth = math.Log(histGrowth)

func init() {
	for i := range histUpper {
		histUpper[i] = math.Pow(histGrowth, float64(i)) - 1
	}
}

func histIndex(ms float64) int {
	if ms <= 0 {
		return 0
	}
	i := int(math.Ceil(math.Log(ms+1) / lnGrowth))
	if i >= histBuckets {
		return histBuckets - 1
	}
	return i
}

type bucket struct {
	idx    int64 // time / width; identifies which slice of time this slot holds
	count  uint64
	total  uint64 // request logs
	errors uint64 // request logs with status >= 500
	timed  uint64 // request logs that carried a duration
	hist   []uint32
}

// window is a ring of fixed-width time buckets keyed by event time, so late
// and out-of-order records still land in the slice of time they belong to.
type window struct {
	width   int64 // nanoseconds
	buckets []bucket
	latency bool
}

func newWindow(span time.Duration, latency bool) *window {
	n := int(span / time.Second)
	if n > 60 {
		n = 60
	}
	if n < 1 {
		n = 1
	}
	width := int64(span) / int64(n)
	return &window{width: width, buckets: make([]bucket, n), latency: latency}
}

// add records one observation at event time t. It returns false when t is
// older than the ring can hold.
func (w *window) add(t int64, isHTTP bool, status int, durMs float64) bool {
	idx := t / w.width
	b := &w.buckets[int(idx%int64(len(w.buckets)))]
	if b.idx != idx {
		if idx < b.idx {
			return false
		}
		h := b.hist
		*b = bucket{idx: idx}
		if h != nil {
			clear(h)
			b.hist = h
		}
	}
	b.count++
	if isHTTP {
		b.total++
		if status >= 500 {
			b.errors++
		}
		if w.latency && durMs >= 0 {
			if b.hist == nil {
				b.hist = make([]uint32, histBuckets)
			}
			b.hist[histIndex(durMs)]++
			b.timed++
		}
	}
	return true
}

type agg struct {
	count, total, errors, timed uint64
	hist                        [histBuckets]uint64
	newest                      int64
}

// sum aggregates the buckets inside (now-span, now].
func (w *window) sum(now int64, a *agg) {
	*a = agg{}
	cur := now / w.width
	lo := cur - int64(len(w.buckets)) + 1
	for i := range w.buckets {
		b := &w.buckets[i]
		if b.idx < lo || b.idx > cur || b.count == 0 {
			continue
		}
		a.count += b.count
		a.total += b.total
		a.errors += b.errors
		a.timed += b.timed
		if b.idx > a.newest {
			a.newest = b.idx
		}
		if b.hist != nil {
			for j, v := range b.hist {
				a.hist[j] += uint64(v)
			}
		}
	}
}

func (a *agg) quantile(q float64) float64 {
	if a.timed == 0 {
		return math.NaN()
	}
	rank := uint64(math.Ceil(q * float64(a.timed)))
	if rank == 0 {
		rank = 1
	}
	var seen uint64
	for i, v := range a.hist {
		seen += v
		if seen >= rank {
			return histUpper[i]
		}
	}
	return histUpper[histBuckets-1]
}
