package main

import (
	"sort"
	"time"
)

// A snapshot mode reads what a destination holds now, rather than taking each
// line as it arrives: a Delta Lake table (delta.go) or a database table
// (postgres.go). It reads the whole destination again in each poll, and the
// sequence tracker then counts exactly what the destination holds. So a row
// that the destination holds twice counts twice, and a row that it rewrote,
// as a compaction does, counts once.
//
// Each part of the destination (a data file, or a table) keeps the time the
// receiver first saw it, so the tracker's timeline and latency still show
// when each line first arrived.

// snapshotPart is one part of a destination and the lines it holds.
type snapshotPart struct {
	name  string
	seen  int64 // when the receiver first saw the part, unix nanos
	lines [][]byte
}

// snapshotTracker rebuilds the sequence tracker from the parts that a
// destination holds now.
type snapshotTracker struct {
	track       *seqTracker
	sampleEvery int64
	shard       *connStats
	// counted is how many lines the receiver's counters hold, so that each
	// poll adds only the lines that are new.
	counted int64
}

// newSnapshotTracker returns a tracker that rebuilds track, and counts the
// lines in shard. track may be nil, when RECEIVER_TRACK_SEQ is off.
func newSnapshotTracker(track *seqTracker, sampleEvery int64, shard *connStats) *snapshotTracker {
	return &snapshotTracker{track: track, sampleEvery: sampleEvery, shard: shard}
}

// rebuild makes the tracker count exactly the lines of parts, as if each part's
// lines had arrived at the time the part was first seen, in that order.
func (s *snapshotTracker) rebuild(parts []snapshotPart, bytesPerLine int64) {
	sort.SliceStable(parts, func(i, j int) bool { return parts[i].seen < parts[j].seen })
	var total int64
	for _, p := range parts {
		total += int64(len(p.lines))
	}
	if total > s.counted && s.shard != nil {
		s.shard.recordN(total-s.counted, (total-s.counted)*bytesPerLine)
		s.counted = total
	}
	if s.track == nil {
		return
	}
	fresh := newSeqTracker(s.sampleEvery)
	for _, p := range parts {
		at := p.seen
		if at == 0 {
			at = time.Now().UnixNano()
		}
		for _, l := range p.lines {
			fresh.record(l, at)
		}
	}
	s.track.adopt(fresh)
}

// adopt replaces what the tracker holds with what fresh holds, at once, so
// that a /seqcheck sees either the old count or the new one, never a part of
// it. fresh must not be used after.
func (t *seqTracker) adopt(fresh *seqTracker) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.start = fresh.start
	t.conns = fresh.conns
	t.unique = fresh.unique
	t.dups = fresh.dups
	t.repeats = fresh.repeats
	t.resends = fresh.resends
	t.marked = fresh.marked
	t.firstTry = fresh.firstTry
	t.tries = fresh.tries
	t.unparsed = fresh.unparsed
	t.cut = fresh.cut
	t.samples = fresh.samples
	t.buckets = fresh.buckets
	t.dupList = fresh.dupList
}
