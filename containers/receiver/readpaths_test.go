package main

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeObjects is an objectStore in memory, which counts its reads.
type fakeObjects struct {
	objects map[string]fakeObject
	gets    int
}

type fakeObject struct {
	etag string
	body string
}

func (f *fakeObjects) list(context.Context) ([]objectInfo, error) {
	var out []objectInfo
	for key, obj := range f.objects {
		out = append(out, objectInfo{key: key, etag: obj.etag})
	}
	return out, nil
}

func (f *fakeObjects) get(_ context.Context, key string) ([]byte, error) {
	f.gets++
	return []byte(f.objects[key].body), nil
}

// keptCheck polls the kept objects and answers what the tracker counts of the
// four lines of conn 0 that the generator acknowledged.
func keptCheck(t *testing.T, kept *keptObjects, now int64) seqCheck {
	t.Helper()
	parts, err := kept.poll(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	tr := newSeqTracker(1)
	newSnapshotTracker(tr, 1, nil).rebuild(parts, 10)
	return tr.check(map[int64]int64{0: 4}, 5)
}

// The kept s3 mode counts what the bucket holds: an object written again under
// its name counts once, a second object with the same lines counts twice, and
// an object deleted counts no more. An object whose ETag did not change is not
// read again.
func TestTheKeptS3ModeCountsWhatTheBucketHolds(t *testing.T) {
	store := &fakeObjects{objects: map[string]fakeObject{
		"a": {etag: "1", body: "CONN=0 SEQ=0 TS=1 TRY=1 x\nCONN=0 SEQ=1 TS=1 TRY=1 x\n"},
	}}
	kept := newKeptObjects(store)
	if got := keptCheck(t, kept, 1_000); got.Unique != 2 || got.Duplicates != 0 {
		t.Fatalf("one object: %+v", got)
	}
	if got := keptCheck(t, kept, 2_000); got.Unique != 2 || store.gets != 1 {
		t.Fatalf("an object that did not change was read again (%d reads): %+v", store.gets, got)
	}

	// The subject writes the same object again, with the same lines and one more
	store.objects["a"] = fakeObject{etag: "2", body: "CONN=0 SEQ=0 TS=1 TRY=1 x\nCONN=0 SEQ=1 TS=1 TRY=1 x\nCONN=0 SEQ=2 TS=1 TRY=1 x\n"}
	if got := keptCheck(t, kept, 3_000); got.Unique != 3 || got.Duplicates != 0 {
		t.Fatalf("an object written again under its name counts once: %+v", got)
	}

	// A second object with lines the first holds is the subject's repeat, and a
	// record written as JSON holds its lines escaped
	store.objects["b"] = fakeObject{etag: "1", body: `{"message":"CONN=0 SEQ=2 TS=1 TRY=1 x\nCONN=0 SEQ=3 TS=1 TRY=1 x"}` + "\n"}
	if got := keptCheck(t, kept, 4_000); got.Unique != 4 || got.Repeats != 1 || got.Missing != 0 {
		t.Fatalf("a second object: %+v", got)
	}

	// An object deleted counts no more
	delete(store.objects, "a")
	if got := keptCheck(t, kept, 5_000); got.Unique != 2 || got.Duplicates != 0 || got.Missing != 2 {
		t.Fatalf("after a delete: %+v", got)
	}
}

// The postgres mode makes a part of each row, leaves out a NULL, splits a row
// that holds a record of several lines, and keeps the time a row was first seen.
func TestThePostgresModeCountsEachRow(t *testing.T) {
	text := func(s string) *string { return &s }
	seen := map[uint64]int64{}
	rows := []*string{
		text(`{"message":"CONN=0 SEQ=0 TS=1 TRY=1 a\nCONN=0 SEQ=1 TS=1 TRY=1 b"}`),
		nil,
		text("CONN=0 SEQ=2 TS=1 TRY=1 c"),
	}
	parts := postgresParts(rows, seen, 1_000)
	if len(parts) != 2 || len(parts[0].lines) != 2 || parts[0].seen != 1_000 {
		t.Fatalf("parts %+v", parts)
	}

	// The next poll finds the same rows and one row twice: the first rows keep
	// their time, and the row held twice counts twice
	rows = append(rows, text("CONN=0 SEQ=2 TS=1 TRY=1 c"))
	parts = postgresParts(rows, seen, 2_000)
	tr := newSeqTracker(1)
	newSnapshotTracker(tr, 1, nil).rebuild(parts, 10)
	if got := tr.check(map[int64]int64{0: 3}, 5); got.Unique != 3 || got.Repeats != 1 || got.Missing != 0 {
		t.Fatalf("the second poll: %+v", got)
	}
	for i, p := range parts {
		if p.seen != 1_000 {
			t.Errorf("part %d was first seen at %d, want 1000", i, p.seen)
		}
	}
}

// The kafka mode hands on each line of a record, leaves out blank lines, and
// reads committed records only.
func TestTheKafkaModeReadsEachLineOfACommittedRecord(t *testing.T) {
	var lines []string
	kafkaRecordLines(&kgo.Record{Value: []byte("CONN=0 SEQ=0 TS=1 a\n\nCONN=0 SEQ=1 TS=1 b\n")}, func(l []byte) { lines = append(lines, string(l)) })
	if len(lines) != 2 || lines[1] != "CONN=0 SEQ=1 TS=1 b" {
		t.Errorf("lines %q", lines)
	}

	client, err := kgo.NewClient(kafkaOptions([]string{"localhost:1"}, "events")...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// franz-go keeps the level as an int8: 0 reads uncommitted records, 1 committed ones
	if got, ok := client.OptValue(kgo.FetchIsolationLevel).(int8); !ok || got != 1 {
		t.Errorf("the receiver reads at isolation %v, want 1, read committed", client.OptValue(kgo.FetchIsolationLevel))
	}
}
