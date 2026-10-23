package runner

import (
	"strings"
	"testing"
	"time"
)

// Lines as a director replica with a state bus writes them, from a run of
// three replicas on Azurite.
const replicaLog = `2026-10-23T16:40:46.595962178Z [2026-10-23 16:40:46] [Information] [director] vmetric-director started; reason recorded as unknown.
2026-10-23T16:40:47.486478465Z [2026-10-23 16:40:47] [Information] [director] The state bus started as subject-1-b29e7da7-d0ac-4624-b622-6805d5bc3430 on azblob:///director-state/bench/7001/state?auth=connection_string&connection_string_env=VMETRIC_SHARED_STATE_CONNECTION_STRING. It is connecting to the other replicas.
2026-10-23T16:40:55.240415505Z [2026-10-23 16:40:55] [Information] [director] The state bus is ready: the replicas share their state through azblob:///director-state/bench/7001/state?auth=connection_string&connection_string_env=VMETRIC_SHARED_STATE_CONNECTION_STRING.
2026-10-23T16:40:57.430465716Z [2026-10-23 16:40:57] [Information] [director] Director manager on 7001 coordinates through its shared bus as member subject-1-b29e7da7-d0ac-4624-b622-6805d5bc3430
2026-10-23T16:40:59.432359961Z [2026-10-23 16:40:59] [Information] [director] [azblob-11] Acquired ownership lease for device 11 (blob-0) on node 7001 (epoch 1)
2026-10-23T16:42:45.140107460Z [2026-10-23 16:42:45] [Warning] [director] Ownership lease renewal for device 11 (blob-0) on node subject-1-b29e7da7-d0ac-4624-b622-6805d5bc3430 is failing (6s since the last renewal: nats: no response from stream), keeping the collector, because a lease store that does not answer is not a reason to stop
2026-10-23T16:42:52.425450519Z [2026-10-23 16:42:52] [Information] [director] Node subject-1-b29e7da7-d0ac-4624-b622-6805d5bc3430 became leader on 7001
2026-10-23T16:44:55.421876314Z [2026-10-23 16:44:55] [Information] [director] Node subject-1-b29e7da7-d0ac-4624-b622-6805d5bc3430 is no longer the leader
2026-10-23T16:44:57.556702012Z [2026-10-23 16:44:57] [Warning] [director] [azblob-11] Stopping the collector of device 11 (blob-0) on node subject-1-b29e7da7-d0ac-4624-b622-6805d5bc3430: this replica cannot renew its member record in the shared bus's storage, so another replica is about to take the device over
2026-10-23T16:45:30.578068086Z [2026-10-23 16:45:30] [Information] [director] [azblob-11] Acquired ownership lease for device 11 (blob-0) on node 7001 (epoch 2)
2026-10-23T16:45:30.796703785Z [2026-10-23 16:45:30] [Warning] [director] State bus: JetStream segment store: this node can no longer write its streams; disabling JetStream
2026-10-23T16:45:34.082563280Z [2026-10-23 16:45:34] [Debug] [director] Ownership lease for device 11 (blob-0) was not released by node subject-1-b29e7da7-d0ac-4624-b622-6805d5bc3430: nats: no response from stream
`

func at(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestParseDockerLogLines(t *testing.T) {
	lines := parseDockerLogLines(replicaLog + "a line without a stamp\n")
	if len(lines) != 12 {
		t.Fatalf("got %d lines, want 12", len(lines))
	}
	if !lines[0].At.Equal(at(t, "2026-10-23T16:40:46.595962178Z")) {
		t.Errorf("first stamp = %s", lines[0].At)
	}
	if !strings.HasSuffix(lines[11].Text, "\na line without a stamp") {
		t.Errorf("a line without a stamp should continue the line before it, got %q", lines[11].Text)
	}
}

func TestParseSSLine(t *testing.T) {
	cases := []struct {
		line   string
		kind   ssKind
		device int64
		member string
	}{
		{"[Information] [director] vmetric-director started; reason recorded as unknown.", ssProcessStart, 0, ""},
		{"[Information] [director] The state bus started as subject-2-3a9f on azblob:///x. It is connecting to the other replicas.", ssBusStarted, 0, "subject-2-3a9f"},
		{"[Information] [director] The state bus is ready: the replicas share their state through azblob:///x.", ssBusReady, 0, ""},
		{"[Warning] [director] The state bus stopped by itself. The director starts it again.", ssBusStopped, 0, ""},
		{"[Information] [director] Director manager on 7001 coordinates through its shared bus as member subject-2-3a9f", ssManagerShared, 0, "subject-2-3a9f"},
		{"[Information] [director] Node subject-2-3a9f became leader on 7001", ssLeaderUp, 0, "subject-2-3a9f"},
		{"[Information] [director] Node subject-2-3a9f is no longer the leader", ssLeaderDown, 0, "subject-2-3a9f"},
		{"[Information] [director] [azblob-13] Acquired ownership lease for device 13 (blob-2) on node 7001 (epoch 2)", ssLeaseAcquired, 13, ""},
		{"[Information] [director] Released ownership lease for device 12 (blob-1) on node 7001", ssLeaseEnded, 12, ""},
		{"[Warning] [director] Ownership lease for device 12 (blob-1) lost on node 7001: taken over; stopping collector", ssLeaseEnded, 12, ""},
		{"[Debug] [director] Ownership lease for device 11 (blob-0) was not released by node subject-3: nats: no response from stream", ssLeaseEnded, 11, ""},
		{"[Warning] [director] [azblob-11] Stopping the collector of device 11 (blob-0) on node subject-3: this replica cannot renew its member record", ssLeaseEnded, 11, ""},
		{"[Information] [director] Assigned new device 13 (blob-2) to node subject-1-b29e", ssPlaced, 13, "subject-1-b29e"},
		{"[Information] [director] Reassigned device 11 (blob-0) from subject-3-f4d7 to subject-2-47d4", ssPlaced, 11, "subject-2-47d4"},
		{"[Information] [director] Reassigned device 11 (blob-0) from  to subject-2-47d4", ssPlaced, 11, "subject-2-47d4"},
		{"[Warning] [director] Device 11 (blob-0) does not start on this director, because the director keeps its shared state in memory. Devices of type azblob keep their progress", ssHeldBack, 11, ""},
		{"[Warning] [director] State bus: JetStream segment store: this node can no longer write its streams; disabling JetStream", ssFenced, 0, ""},
	}
	for _, c := range cases {
		kind, device, member, ok := parseSSLine(c.line)
		if !ok || kind != c.kind || device != c.device || member != c.member {
			t.Errorf("parseSSLine(%q) = %v, %d, %q, %v; want %v, %d, %q", c.line, kind, device, member, ok, c.kind, c.device, c.member)
		}
	}
	// A renewal that fails while the collector goes on ends nothing.
	if _, _, _, ok := parseSSLine("[Warning] [director] Ownership lease renewal for device 11 (blob-0) on node x is failing (6s since the last renewal: nats: no response from stream), keeping the collector"); ok {
		t.Error("a failing renewal that keeps the collector must not count as an event")
	}
	if _, _, _, ok := parseSSLine("[Debug] [director] [azblob-11] [AZBlob] List cycle for blob-0: prefixes=1 pages=1 success=true fail=false"); ok {
		t.Error("an unrelated line must not count as an event")
	}
}

func TestLeadershipAndOwnership(t *testing.T) {
	events := ssEventsOf("c1", parseDockerLogLines(replicaLog))
	if ssLeadingAt(events, at(t, "2026-10-23T16:42:50Z")) {
		t.Error("the replica does not lead before it logs it")
	}
	if !ssLeadingAt(events, at(t, "2026-10-23T16:43:00Z")) {
		t.Error("the replica leads once it logs it")
	}
	if ssLeadingAt(events, at(t, "2026-10-23T16:45:00Z")) {
		t.Error("the replica stops leading once it logs it")
	}
	if !ssHoldsAt(events, 11, at(t, "2026-10-23T16:41:00Z")) {
		t.Error("the replica runs device 11 once it took the lease")
	}
	if ssHoldsAt(events, 11, at(t, "2026-10-23T16:45:00Z")) {
		t.Error("the replica stopped device 11 when it could not renew its member record")
	}
	if ssHoldsAt(events, 12, at(t, "2026-10-23T16:41:00Z")) {
		t.Error("the replica never took device 12")
	}

	until := at(t, "2026-10-23T16:46:00Z")
	intervals := ssOwnership("c1", events, until)
	if len(intervals) != 2 {
		t.Fatalf("got %d intervals, want 2: %+v", len(intervals), intervals)
	}
	if got := intervals[0].To.Sub(intervals[0].From).Round(time.Second); got != 238*time.Second {
		t.Errorf("first interval lasted %s, want 3m58s", got)
	}
	if got := intervals[1].To.Sub(intervals[1].From).Round(time.Second); got != 4*time.Second {
		t.Errorf("second interval lasted %s, want 4s", got)
	}

	// A process end the harness caused closes what the process ran.
	killed := append(append([]ssEvent(nil), events[:5]...), ssEvent{At: at(t, "2026-10-23T16:41:30Z"), Container: "c1", Kind: ssProcessEnd})
	if ssHoldsAt(killed, 11, at(t, "2026-10-23T16:41:40Z")) {
		t.Error("a killed replica runs nothing")
	}
	if ssLeadingAt(killed, at(t, "2026-10-23T16:41:40Z")) {
		t.Error("a killed replica does not lead")
	}

	// A leader whose bus disables JetStream leads no more, although its
	// manager never says so: that is how the replica set got stuck after a
	// store outage.
	fenced := ssEventsOf("c1", parseDockerLogLines(`2026-10-23T16:45:30.420Z [2026-10-23 16:45:30] [Information] [director] Node subject-3-f4d7 became leader on 7001
2026-10-23T16:45:30.797Z [2026-10-23 16:45:30] [Warning] [director] State bus: JetStream segment store: this node can no longer write its streams; disabling JetStream`))
	if !ssLeadingAt(fenced, at(t, "2026-10-23T16:45:30.500Z")) {
		t.Error("the replica leads once it logs it")
	}
	if ssLeadingAt(fenced, at(t, "2026-10-23T16:45:31Z")) {
		t.Error("a replica whose bus disabled JetStream does not lead")
	}
}

func TestOverlaps(t *testing.T) {
	base := at(t, "2026-10-23T16:00:00Z")
	iv := func(c string, d int64, from, to int) ssInterval {
		return ssInterval{Container: c, Device: d, From: base.Add(time.Duration(from) * time.Second), To: base.Add(time.Duration(to) * time.Second)}
	}
	intervals := []ssInterval{
		iv("a", 11, 0, 100),
		iv("b", 11, 100, 200), // a clean handover: no overlap
		iv("c", 11, 150, 160), // b and c both run device 11 for 10 seconds
		iv("a", 12, 0, 100),   // another device: never compared with 11
		iv("b", 12, 100, 150), // a handover on the second: no overlap
		iv("b", 11, 300, 400), // the same container twice: never an overlap
		iv("b", 11, 350, 360),
	}
	overlaps := ssOverlaps(intervals, time.Second)
	if len(overlaps) != 1 {
		t.Fatalf("got %d overlaps, want 1: %+v", len(overlaps), overlaps)
	}
	o := overlaps[0]
	if o.Device != 11 || o.A != "b" || o.B != "c" || o.Seconds() != 10 {
		t.Errorf("overlap = %+v (%.0fs), want device 11 on b and c for 10s", o, o.Seconds())
	}
}

func TestStoreCallRates(t *testing.T) {
	lines := parseDockerLogLines(`2026-10-23T16:40:47.100Z 172.20.0.5 - - [23/Oct/2026:16:40:47 +0000] "PUT /devstoreaccount1/director-state?restype=container HTTP/1.1" 201 -
2026-10-23T16:40:47.200Z 172.20.0.7 - - [23/Oct/2026:16:40:47 +0000] "PUT /devstoreaccount1/director-state?restype=container HTTP/1.1" 409 -
2026-10-23T16:40:47.300Z 172.20.0.5 - - [23/Oct/2026:16:40:47 +0000] "GET /devstoreaccount1/director-state?comp=list&prefix=bench%2F7001%2Fstate%2Fr%2Fshared%2Fmembers%2F&restype=container HTTP/1.1" 200 -
2026-10-23T16:40:48.000Z 172.20.0.5 - - [23/Oct/2026:16:40:48 +0000] "GET /devstoreaccount1/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Factive HTTP/1.1" 200 302
2026-10-23T16:40:48.500Z 172.20.0.8 - - [23/Oct/2026:16:40:48 +0000] "PUT /devstoreaccount1/bench-in/in%2F2%2F000210-1.log HTTP/1.1" 201 -
2026-10-23T16:40:49.000Z 172.20.0.5 - - [23/Oct/2026:16:40:49 +0000] "DELETE /statestore/director-state/bench%2Fo%2Fx HTTP/1.1" 403 -
2026-10-23T16:40:49.100Z Azurite Blob service is successfully listening at http://0.0.0.0:10000
`)
	calls := parseAzuriteCalls(lines)
	if len(calls) != 6 {
		t.Fatalf("got %d calls, want 6", len(calls))
	}
	r := storeCallRates(calls, "director-state", at(t, "2026-10-23T16:40:47Z"), at(t, "2026-10-23T16:40:51Z"))
	if r.Writes != 3 || r.ReadsAndLists != 2 || r.Lists != 1 {
		t.Errorf("writes %d, reads %d, lists %d; want 3, 2, 1 (the bench-in upload is another container)", r.Writes, r.ReadsAndLists, r.Lists)
	}
	if r.FailedRequests != 1 {
		t.Errorf("failed = %d, want 1: a 409 is a lost race, a 403 is a failure", r.FailedRequests)
	}
	if r.WritesPerSec != 0.75 {
		t.Errorf("writes/s = %v, want 0.75", r.WritesPerSec)
	}
	if (ssStoreCall{Path: "/devstoreaccount1/director-states/x"}).inContainer("director-state") {
		t.Error("a container whose name only starts the same is another container")
	}
}

func TestStoreCallKinds(t *testing.T) {
	cases := map[string]string{
		"/statestore/director-state/bench%2F7001%2Fstate%2Fo%2Fsegments%2Fsubject-1-ab-cd-e7baf38d%2F00000000000000000015":    "segments",
		"/statestore/director-state/bench/7001/state/o/snapshots/$G/KV_devices/subject-1-ab-cd-e7baf38d-9":                    "snapshots",
		"/statestore/director-state/bench/7001/state/r/streams/$G/KV_persistent":                                              "streams",
		"/statestore/director-state/bench/7001/state/r/shared/members/subject-2-47d4":                                         "members",
		"/statestore/director-state/bench%2F7001%2Fstate%2Fr%2Fshared%2Factive":                                               "lease",
		"/statestore/director-state?comp=list&prefix=bench%2F7001%2Fstate%2Fo%2Fsegments%2Fsubject-1-ab%2F&restype=container": "segments",
		"/statestore/director-state?restype=container":                                                                        "other",
	}
	for path, want := range cases {
		if got := (ssStoreCall{Path: path}).kind(); got != want {
			t.Errorf("kind(%q) = %q, want %q", path, got, want)
		}
	}
	base := time.Date(2026, 10, 23, 18, 0, 0, 0, time.UTC)
	calls := []ssStoreCall{
		{At: base, Method: "PUT", Status: 201, Path: "/statestore/director-state/bench%2Fo%2Fsegments%2Fold-writer%2F00000000000000000001"},
		{At: base.Add(60 * time.Second), Method: "PUT", Status: 201, Path: "/statestore/director-state/bench/o/segments/w%2F00000000000000000001"},
		{At: base.Add(61 * time.Second), Method: "PUT", Status: 201, Path: "/statestore/director-state/bench/r/shared/members/m1"},
		{At: base.Add(120 * time.Second), Method: "PUT", Status: 201, Path: "/statestore/director-state/bench/o/segments/w%2F00000000000000000002"},
		{At: base.Add(121 * time.Second), Method: "PUT", Status: 409, Path: "/statestore/director-state/bench/o/segments/w%2F00000000000000000002"},
		{At: base.Add(181 * time.Second), Method: "PUT", Status: 201, Path: "/statestore/director-state/bench/o/segments/w%2F00000000000000000003"},
		{At: base.Add(182 * time.Second), Method: "GET", Status: 200, Path: "/statestore/director-state/bench/o/segments/w%2F00000000000000000003"},
	}
	r := storeCallRates(calls, "director-state", base, base.Add(200*time.Second))
	if r.ByKind["segments"].Writes != 5 || r.ByKind["members"].Writes != 1 || r.ByKind["segments"].ReadsAndLists != 1 {
		t.Errorf("by kind = %+v, want 5 segment writes, 1 segment read and 1 member write", r.ByKind)
	}
	writer, ts := busiestWriter(segmentWrites(calls, "director-state", base, base.Add(200*time.Second)))
	if writer != "w" || len(ts) != 3 {
		t.Fatalf("busiest writer = %q with %d writes, want w with 3: a refused write is not a write", writer, len(ts))
	}
	gaps := writeGaps(ts)
	if len(gaps) != 2 || gaps[0] != 60*time.Second || gaps[1] != 61*time.Second {
		t.Errorf("gaps = %v, want 1m0s and 1m1s", gaps)
	}
}

func TestScanForSecrets(t *testing.T) {
	key := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	texts := map[string]string{
		"clean":  "The state bus could not start: blob: 403 AuthenticationFailed [REDACTED]",
		"whole":  "connection_string=DefaultEndpointsProtocol=http;AccountKey=" + key + ";",
		"piece":  "signature " + key[30:60] + " did not match",
		"escape": "key=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq%2FK1SZFPTOtr%2FKBHBeksoGMGw%3D%3D",
	}
	hits := scanForSecrets([]string{key}, texts)
	where := map[string]bool{}
	for _, h := range hits {
		where[h.Where] = true
		if strings.Contains(h.Line, key) || strings.Contains(h.Line, key[:16]) {
			t.Errorf("a hit must mask the secret, got %q", h.Line)
		}
		if strings.Contains(h.What, key[:8]) {
			t.Errorf("a hit's label must not carry the secret, got %q", h.What)
		}
	}
	for _, name := range []string{"whole", "piece", "escape"} {
		if !where[name] {
			t.Errorf("no hit in %q", name)
		}
	}
	if where["clean"] {
		t.Error("a hit in a text without the secret")
	}
}

// A log the harness cannot read is an error, so the final checks can fail the
// case instead of passing on part of the evidence.
func TestSSLogBookRefreshReportsALogItCannotRead(t *testing.T) {
	book := newSSLogBook()
	if err := book.refresh("pipebench-test-no-such-container"); err == nil {
		t.Fatal("reading the log of a container that does not exist reported no error")
	}
	if lines := book.all("pipebench-test-no-such-container"); len(lines) != 0 {
		t.Fatalf("the book holds %d lines of a container it could not read", len(lines))
	}
}

// TestOverlapUnderASecond: the lines of a pause_store run against a dev build.
// Once the store was back, subject-1 took the three pollers' leases again.
// subject-2, the new leader, took them over a second later, because
// subject-1's member record had lapsed during the outage, and subject-1 found
// out a second after that. Both ran the pollers for 0.949 seconds. The old
// one-second tolerance passed the run.
func TestOverlapUnderASecond(t *testing.T) {
	s1 := parseDockerLogLines(`2026-10-23T20:30:35.762000000Z [2026-10-23 20:30:35] [Information] [director] [azblob-12] Acquired ownership lease for device 12 (blob-1) on node 7001 (epoch 1)
2026-10-23T20:30:37.763000000Z [2026-10-23 20:30:37] [Debug] [director] Ownership lease for device 12 (blob-1) was not released by node subject-1-8dae988b-b667-4da3-a493-6de981d7b8f6: devicekv: revision mismatch`)
	s2 := parseDockerLogLines(`2026-10-23T20:30:36.814000000Z [2026-10-23 20:30:36] [Information] [director] Taking over the ownership lease of device 12 from node subject-1-8dae988b-b667-4da3-a493-6de981d7b8f6, which is no longer a live member (lease valid for another 16s); epoch 2 on node subject-2-77344d38-320d-4401-9c61-cb9159653e41
2026-10-23T20:30:36.814000000Z [2026-10-23 20:30:36] [Information] [director] [azblob-12] Acquired ownership lease for device 12 (blob-1) on node 7001 (epoch 2)`)
	until := at(t, "2026-10-23T20:31:00Z")
	intervals := append(ssOwnership("subject-1", ssEventsOf("subject-1", s1), until), ssOwnership("subject-2", ssEventsOf("subject-2", s2), until)...)
	if old := ssOverlaps(intervals, time.Second); len(old) != 0 {
		t.Fatalf("the old tolerance found %v; the run passed", old)
	}
	fails, notes := judgeOverlaps(ssOverlaps(intervals, 0))
	if len(fails) != 1 || !strings.Contains(fails[0], "device 12 ran on subject-1 and subject-2 at once for 0.949s, from 20:30:36.814") || len(notes) != 0 {
		t.Errorf("fails %q, notes %q; want the 0.949s double run", fails, notes)
	}
	// Two containers' stamps a few milliseconds apart are reported, not judged.
	jitter := []ssOverlap{{Device: 11, A: "subject-1", B: "subject-2", From: until, To: until.Add(40 * time.Millisecond)}}
	if fails, notes := judgeOverlaps(jitter); len(fails) != 0 || len(notes) != 1 || !strings.Contains(notes[0], "reported, not judged") {
		t.Errorf("40ms: fails %q, notes %q", fails, notes)
	}
}
