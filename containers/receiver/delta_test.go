package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// deltaRow is a row of the test tables: a line in a "message" column, as the
// director's lake target writes a plain-text record.
type deltaRow struct {
	Epoch   int64   `parquet:"epoch"`
	Message *string `parquet:"message,optional"`
}

// writeDataFile writes rows as a Parquet file of the table, at a path below it.
// A line of "<null>" is a row whose message is NULL.
func writeDataFile(t *testing.T, table, rel string, lines ...string) {
	t.Helper()
	path := filepath.Join(table, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	rows := make([]deltaRow, len(lines))
	for i, l := range lines {
		rows[i] = deltaRow{Epoch: int64(i)}
		if l != "<null>" {
			rows[i].Message = &l
		}
	}
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatal(err)
	}
}

// writeCommit writes the table's commit of version v, one action a line.
func writeCommit(t *testing.T, table string, v int, actions ...string) {
	t.Helper()
	dir := filepath.Join(table, "_delta_log")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%020d.json", v)), []byte(strings.Join(actions, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func add(path string) string    { return `{"add":{"path":"` + path + `","size":1,"dataChange":true}}` }
func remove(path string) string { return `{"remove":{"path":"` + path + `","dataChange":false}}` }

// check polls the table and answers what the tracker counts of the lines of
// conn 0 that the generator acknowledged.
func pollAndCheck(t *testing.T, d *deltaTable, snap *snapshotTracker, tr *seqTracker, acked int64) seqCheck {
	t.Helper()
	if err := d.poll(); err != nil {
		t.Fatal(err)
	}
	snap.rebuild(d.parts(), 10)
	return tr.check(map[int64]int64{0: acked}, 5)
}

func TestTheDeltaReceiverCountsWhatTheTableHoldsNow(t *testing.T) {
	table := t.TempDir()
	line := func(seq, try int) string { return fmt.Sprintf("CONN=0 SEQ=%d TS=1 TRY=%d pad", seq, try) }
	tr := newSeqTracker(1)
	snap := newSnapshotTracker(tr, 1, nil)
	d := newDeltaTable(dirStore(table), "message")

	// An empty folder: no commit yet, nothing arrived
	if got := pollAndCheck(t, d, snap, tr, 4); got.Missing != 4 || got.Unique != 0 {
		t.Fatalf("before the first commit: %+v", got)
	}

	// Two commits of two rows each. A path with a partition folder and an
	// escaped character is read too.
	writeDataFile(t, table, "part-0.parquet", line(0, 1), "<null>", line(1, 1))
	writeDataFile(t, table, "day=2026-11-28/part 1.parquet", line(2, 1), line(3, 1))
	writeCommit(t, table, 0, `{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}`, add("part-0.parquet"))
	writeCommit(t, table, 1, add("day=2026-11-28/part%201.parquet"))
	if got := pollAndCheck(t, d, snap, tr, 4); got.Missing != 0 || got.Unique != 4 || got.Duplicates != 0 || got.Unparsed != 0 {
		t.Fatalf("after two commits, with a NULL row left out: %+v", got)
	}

	// A compaction rewrites both files into one: each row still counts once
	writeDataFile(t, table, "part-2.parquet", line(0, 1), line(1, 1), line(2, 1), line(3, 1))
	writeCommit(t, table, 2, remove("part-0.parquet"), remove("day=2026-11-28/part%201.parquet"), add("part-2.parquet"))
	if got := pollAndCheck(t, d, snap, tr, 4); got.Missing != 0 || got.Unique != 4 || got.Duplicates != 0 {
		t.Fatalf("after a compaction: %+v", got)
	}

	// A second run's commit adds two of the rows again: the subject's repeats
	writeDataFile(t, table, "part-3.parquet", line(2, 1), line(3, 1))
	writeCommit(t, table, 3, add("part-3.parquet"))
	if got := pollAndCheck(t, d, snap, tr, 4); got.Duplicates != 2 || got.Repeats != 2 || got.Resends != 0 {
		t.Fatalf("after a commit that adds rows again: %+v", got)
	}

	// A row that holds a post of three lines, as the director writes a post it
	// takes as one record, counts each line
	writeDataFile(t, table, "part-4.parquet", line(4, 1)+"\n"+line(5, 1)+"\r\n"+line(6, 1)+"\n")
	writeCommit(t, table, 4, add("part-4.parquet"))
	if got := pollAndCheck(t, d, snap, tr, 7); got.Missing != 0 || got.Unique != 7 || got.Unparsed != 0 {
		t.Fatalf("after a row of three lines: %+v", got)
	}

	// A commit that is not there yet ends the reading, and the next poll
	// goes on from it
	writeCommit(t, table, 6, remove("part-3.parquet"))
	if got := pollAndCheck(t, d, snap, tr, 7); got.Duplicates != 2 {
		t.Fatalf("a commit after a gap was read: %+v", got)
	}
	writeCommit(t, table, 5, `{"commitInfo":{"operation":"WRITE"}}`)
	if got := pollAndCheck(t, d, snap, tr, 7); got.Duplicates != 0 || got.Unique != 7 {
		t.Fatalf("after the gap closed: %+v", got)
	}
}

func TestTheDeltaReceiverNamesAColumnThatIsNotThere(t *testing.T) {
	table := t.TempDir()
	writeDataFile(t, table, "a.parquet", "CONN=0 SEQ=0 TS=1 x")
	writeCommit(t, table, 0, add("a.parquet"))
	err := newDeltaTable(dirStore(table), "msg").poll()
	if err == nil || !strings.Contains(err.Error(), `no column "msg"`) {
		t.Fatalf("poll: %v", err)
	}
}

// TestASnapshotCountsNewLinesOnceAndKeepsArrivalOrder checks the counters and
// the timeline of a snapshot mode: each poll adds only the lines that are new,
// and each line keeps the time its part was first seen.
func TestASnapshotCountsNewLinesOnceAndKeepsArrivalOrder(t *testing.T) {
	cnt := &counters{}
	shard := cnt.newShard()
	tr := newSeqTracker(1)
	snap := newSnapshotTracker(tr, 1, shard)
	p1 := snapshotPart{name: "b", seen: 2_000_000_000, lines: [][]byte{[]byte("CONN=0 SEQ=1 TS=1000000000 x")}}
	p0 := snapshotPart{name: "a", seen: 1_500_000_000, lines: [][]byte{[]byte("CONN=0 SEQ=0 TS=1000000000 x")}}
	snap.rebuild([]snapshotPart{p1, p0}, 10)
	snap.rebuild([]snapshotPart{p1, p0}, 10)
	if got := shard.lines.Load(); got != 2 {
		t.Errorf("the counters hold %d lines after two polls of the same two, want 2", got)
	}
	if lat := tr.latency(0, 0); lat.Count != 2 || lat.MaxMs != 1000 {
		t.Errorf("latency %+v, want two samples, the slowest 1000 ms", lat)
	}
	if got := tr.check(map[int64]int64{0: 2}, 5); got.Unique != 2 || got.Duplicates != 0 {
		t.Errorf("%+v", got)
	}
	// The part seen first starts the timeline, whatever order the parts came in
	if start := tr.timeline().StartNs; start != 1_500_000_000 {
		t.Errorf("the timeline starts at %d, want the first part seen, 1500000000", start)
	}
}

// TestAnObjectsLinesAreReadAsCountBodyReadsThem checks bodyLines, which the
// kept s3 mode reads an object with: gzip by its magic bytes, no blank lines.
func TestAnObjectsLinesAreReadAsCountBodyReadsThem(t *testing.T) {
	var gz strings.Builder
	w := gzipWriter(&gz)
	_, _ = w.Write([]byte("CONN=0 SEQ=0 TS=1 a\n\nCONN=0 SEQ=1 TS=1 b\n"))
	_ = w.Close()
	for _, body := range [][]byte{[]byte("CONN=0 SEQ=0 TS=1 a\n\nCONN=0 SEQ=1 TS=1 b\n"), []byte(gz.String())} {
		lines, err := bodyLines(body)
		if err != nil || len(lines) != 2 || string(lines[1]) != "CONN=0 SEQ=1 TS=1 b" {
			t.Fatalf("lines %q, %v", lines, err)
		}
	}
}

func gzipWriter(w io.Writer) *gzip.Writer { return gzip.NewWriter(w) }

// TestAValueOfSeveralLinesCountsEachLine checks valueLines: a value splits on
// newlines, and, for a destination that holds the record as JSON, on an escaped
// newline too.
func TestAValueOfSeveralLinesCountsEachLine(t *testing.T) {
	value := []byte(`{"message":"CONN=0 SEQ=0 TS=1 a\nCONN=0 SEQ=1 TS=1 b"}` + "\nCONN=0 SEQ=2 TS=1 c\r\n\n")
	if got := valueLines([][]byte{value}, true); len(got) != 3 || !strings.HasSuffix(string(got[1]), `b"}`) || string(got[2]) != "CONN=0 SEQ=2 TS=1 c" {
		t.Errorf("escaped: %q", got)
	}
	if got := valueLines([][]byte{value}, false); len(got) != 2 {
		t.Errorf("plain: %q", got)
	}
}

// A table whose files carry deletion vectors is refused: the receiver reads every
// row of a file, so the rows a vector removed would count as repeats.
func TestTheDeltaReceiverRefusesDeletionVectors(t *testing.T) {
	table := t.TempDir()
	writeDataFile(t, table, "a.parquet", "CONN=0 SEQ=0 TS=1 x")
	writeCommit(t, table, 0, `{"add":{"path":"a.parquet","size":1,"dataChange":true,"deletionVector":{"storageType":"i","pathOrInlineDv":"wi5b=000010000siXQKl0rr91000f$1d6mu","offset":1,"sizeInBytes":36,"cardinality":1}}}`)
	err := newDeltaTable(dirStore(table), "message").poll()
	if err == nil || !strings.Contains(err.Error(), "deletion vector") {
		t.Fatalf("poll: %v", err)
	}

	// An add whose vector is null is an add of every row
	other := t.TempDir()
	writeDataFile(t, other, "a.parquet", "CONN=0 SEQ=0 TS=1 x")
	writeCommit(t, other, 0, `{"add":{"path":"a.parquet","size":1,"dataChange":true,"deletionVector":null}}`)
	if err := newDeltaTable(dirStore(other), "message").poll(); err != nil {
		t.Fatalf("poll of a null vector: %v", err)
	}
}
