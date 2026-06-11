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
