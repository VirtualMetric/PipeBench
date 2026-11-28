package main

import (
	"bytes"
	"compress/gzip"
	"testing"
)

func TestCountBody(t *testing.T) {
	gz := func(s string) []byte {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}

	tests := []struct {
		name    string
		body    []byte
		want    int
		wantErr bool
	}{
		{"plain lines with trailing newline", []byte("a\nb\nc\n"), 3, false},
		{"no trailing newline", []byte("a\nb"), 2, false},
		{"blank lines skipped", []byte("a\n\n\nb\n"), 2, false},
		{"empty body", nil, 0, false},
		{"gzip auto-detected", gz("x\ny\n"), 2, false},
		{"corrupt gzip errors", []byte{0x1f, 0x8b, 0x00, 0x00}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got int
			err := countBody(tt.body, func([]byte) { got++ })
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("countBody: %v", err)
			}
			if got != tt.want {
				t.Errorf("counted %d lines, want %d", got, tt.want)
			}
		})
	}
}

// TestACloudModeTracksEachEscapedLineOfARecord checks that the tracker hears of
// each line of a record that a subject wrote as JSON, with its newlines escaped,
// while the counters count the record as one line, as the destination holds it.
func TestACloudModeTracksEachEscapedLineOfARecord(t *testing.T) {
	saved := seqTrack
	t.Cleanup(func() { seqTrack = saved })
	seqTrack = newSeqTracker(1)
	cnt := &counters{}
	onLine := newCloudOnLine(cnt, &validator{}, config{})
	onLine([]byte(`{"message":"CONN=0 SEQ=0 TS=1 a\nCONN=0 SEQ=1 TS=1 b"}`))
	if got := seqTrack.check(map[int64]int64{0: 2}, 5); got.Unique != 2 || got.Missing != 0 {
		t.Errorf("the tracker heard %+v", got)
	}
	var lines int64
	for _, s := range cnt.shards {
		lines += s.lines.Load()
	}
	if lines != 1 {
		t.Errorf("the counters hold %d lines, want 1", lines)
	}
}
