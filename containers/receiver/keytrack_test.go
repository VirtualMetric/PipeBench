package main

import "testing"

func TestKeyTrackerCountsEachKeyOncePerLine(t *testing.T) {
	k, err := newKeyTracker(`evt-[0-9]+`)
	if err != nil {
		t.Fatal(err)
	}
	k.record([]byte(`{"id":101,"message":"evt-101","raw":"evt-101"}`))
	k.record([]byte(`[{"message":"evt-102"},{"message":"evt-103"}]`))
	k.record([]byte(`{"message":"evt-102"}`))
	k.record([]byte(`a line without a key`))

	got := k.report()
	want := map[string]int64{"evt-101": 1, "evt-102": 2, "evt-103": 1}
	if len(got.Keys) != len(want) {
		t.Fatalf("keys = %v, want %v", got.Keys, want)
	}
	for key, n := range want {
		if got.Keys[key] != n {
			t.Fatalf("keys[%s] = %d, want %d (all: %v)", key, got.Keys[key], n, got.Keys)
		}
	}
	if got.LinesWithoutKey != 1 {
		t.Fatalf("lines without a key = %d, want 1", got.LinesWithoutKey)
	}
}

func TestKeyTrackerUsesTheFirstGroup(t *testing.T) {
	k, err := newKeyTracker(`"id":([0-9]+)`)
	if err != nil {
		t.Fatal(err)
	}
	k.record([]byte(`{"id":7,"message":"x"}`))
	if got := k.report().Keys; got["7"] != 1 || len(got) != 1 {
		t.Fatalf("keys = %v, want only 7", got)
	}
}

func TestKeyTrackerRefusesABadExpression(t *testing.T) {
	if _, err := newKeyTracker(`evt-(`); err == nil {
		t.Fatal("newKeyTracker accepted an expression that does not compile")
	}
}
