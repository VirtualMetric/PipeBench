package runner

import (
	"reflect"
	"testing"
	"time"
)

func TestLiveStoreMembers(t *testing.T) {
	lines := parseDockerLogLines(`2026-10-23T16:40:40.000Z 172.20.0.9 - - [23/Oct/2026:16:40:40 +0000] "PUT /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-4-old HTTP/1.1" 201 -
2026-10-23T16:40:47.000Z 172.20.0.5 - - [23/Oct/2026:16:40:47 +0000] "PUT /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-1-b29e HTTP/1.1" 201 -
2026-10-23T16:40:48.000Z 172.20.0.6 - - [23/Oct/2026:16:40:48 +0000] "PUT /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-2-3a9f HTTP/1.1" 201 -
2026-10-23T16:40:48.500Z 172.20.0.6 - - [23/Oct/2026:16:40:48 +0000] "GET /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-3-f4d7 HTTP/1.1" 200 302
2026-10-23T16:40:49.000Z 172.20.0.5 - - [23/Oct/2026:16:40:49 +0000] "PUT /statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2Fsubject-1-b29e HTTP/1.1" 201 -
2026-10-23T16:40:49.500Z 172.20.0.8 - - [23/Oct/2026:16:40:49 +0000] "PUT /devstoreaccount1/bench-in/shared%2Fmembers%2Fnot-a-member HTTP/1.1" 201 -
`)
	got := liveStoreMembers(parseAzuriteCalls(lines), "director-state", 6*time.Second)
	want := []string{"subject-1-b29e", "subject-2-3a9f"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("live members = %v, want %v: a record renewed long ago, a read, and a write to another container do not count", got, want)
	}
	if got := liveStoreMembers(nil, "director-state", time.Second); len(got) != 0 {
		t.Errorf("no calls: got %v", got)
	}
}
