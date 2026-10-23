package runner

import (
	"fmt"
	"net/url"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file reads what the replicas of a shared-state director log, and what
// Azurite logs for every request, into events the driver can reason about:
// which replica leads, which replica holds each placed device's ownership
// lease, and how many calls the replicas make to the store.

// ssLine is one line a container wrote, with the time Docker stamped on it.
type ssLine struct {
	At   time.Time
	Text string
}

// ssLogBook keeps every line each container wrote during a run, across
// restarts and replacements of the container: a replaced container's logs
// are gone from Docker, so the book is the only copy.
type ssLogBook struct {
	mu    sync.Mutex
	lines map[string][]ssLine
	last  map[string]time.Time
}

func newSSLogBook() *ssLogBook {
	return &ssLogBook{lines: map[string][]ssLine{}, last: map[string]time.Time{}}
}

// refresh reads the lines the container wrote since the last refresh. A read
// that fails leaves the book as it was, and the next refresh reads the lines
// it missed. So only the final checks need to act on the error.
func (b *ssLogBook) refresh(container string) error {
	b.mu.Lock()
	since := b.last[container]
	b.mu.Unlock()

	args := []string{"logs", "-t"}
	if !since.IsZero() {
		args = append(args, "--since", since.Add(time.Nanosecond).Format(time.RFC3339Nano))
	}
	out, err := exec.Command("docker", append(args, container)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker logs %s: %w", container, err)
	}
	fresh := parseDockerLogLines(string(out))

	b.mu.Lock()
	defer b.mu.Unlock()
	for _, l := range fresh {
		if !l.At.After(b.last[container]) {
			continue
		}
		b.lines[container] = append(b.lines[container], l)
		b.last[container] = l.At
	}
	return nil
}

// all returns a copy of every line the container wrote.
func (b *ssLogBook) all(container string) []ssLine {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]ssLine(nil), b.lines[container]...)
}

// text returns the container's lines as Docker printed them.
func (b *ssLogBook) text(container string) string {
	var sb strings.Builder
	for _, l := range b.all(container) {
		sb.WriteString(l.At.UTC().Format(time.RFC3339Nano))
		sb.WriteByte(' ')
		sb.WriteString(l.Text)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// parseDockerLogLines splits `docker logs -t` output into lines and their
// timestamps. A line without a timestamp continues the line before it.
func parseDockerLogLines(out string) []ssLine {
	var lines []ssLine
	for _, raw := range strings.Split(out, "\n") {
		raw = strings.TrimRight(raw, "\r")
		if raw == "" {
			continue
		}
		stamp, rest, _ := strings.Cut(raw, " ")
		at, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			if n := len(lines); n > 0 {
				lines[n-1].Text += "\n" + raw
			}
			continue
		}
		lines = append(lines, ssLine{At: at, Text: rest})
	}
	return lines
}

// ssKind is what a replica's log line says about the replica.
type ssKind int

const (
	ssNone ssKind = iota
	// ssProcessStart: a director process starts in the container.
	ssProcessStart
	// ssProcessEnd: the harness killed or stopped the process. The director
	// does not log this; the driver adds it.
	ssProcessEnd
	// ssBusStarted: the state bus started, under a member name of its own.
	ssBusStarted
	// ssBusReady: the state bus connected to the replica that runs it.
	ssBusReady
	// ssBusStopped: the state bus stopped, by itself or on a change.
	ssBusStopped
	// ssManagerShared: the director manager runs on a shared bus, as a member.
	ssManagerShared
	// ssLeaderUp and ssLeaderDown: the replica starts or stops leading.
	ssLeaderUp
	ssLeaderDown
	// ssLeaseAcquired: the replica took a device's ownership lease and starts
	// its collector.
	ssLeaseAcquired
	// ssLeaseEnded: the replica stopped a device's collector, or gave the
	// lease up.
	ssLeaseEnded
	// ssPlaced: the leader placed a device on a member.
	ssPlaced
	// ssHeldBack: the replica does not start a device because the director
	// keeps its shared state in memory.
	ssHeldBack
	// ssFenced: the replica's bus stopped writing its streams, because another
	// replica took them over or a write failed for good.
	ssFenced
)

var (
	reBusStarted    = regexp.MustCompile(`The state bus started as (\S+) on `)
	reManagerShared = regexp.MustCompile(`coordinates through its shared bus as member (\S+)`)
	reLeaderUp      = regexp.MustCompile(`Node (\S+) became leader on`)
	reLeaderDown    = regexp.MustCompile(`Node (\S+) is no longer the leader`)
	reLeaseAcquired = regexp.MustCompile(`Acquired ownership lease for device (\d+) `)
	reLeaseEnded    = regexp.MustCompile(`(?:Released ownership lease for device|Ownership lease for device|Stopping the collector of device) (\d+) `)
	rePlacedNew     = regexp.MustCompile(`Assigned new device (\d+) \(.*\) to node (\S+)`)
	rePlacedMove    = regexp.MustCompile(`Reassigned device (\d+) \(.*\) from \S* ?to (\S+)`)
	reHeldBack      = regexp.MustCompile(`Device (\d+) \(.*\) does not start on this director, because the director keeps its shared state in memory`)
)

// ssEvent is one thing a replica's log says happened, or one thing the
// harness did to the replica.
type ssEvent struct {
	At        time.Time
	Container string
	Kind      ssKind
	Device    int64
	Member    string
	Text      string
}

// parseSSLine reads what one log line of a replica says. ok is false for a
// line that says nothing the driver follows.
func parseSSLine(text string) (kind ssKind, device int64, member string, ok bool) {
	deviceOf := func(m []string) int64 {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		return n
	}
	switch {
	case strings.Contains(text, "vmetric-director started"):
		return ssProcessStart, 0, "", true
	case strings.Contains(text, "The state bus is ready"):
		return ssBusReady, 0, "", true
	case strings.Contains(text, "The state bus stopped"):
		return ssBusStopped, 0, "", true
	case strings.Contains(text, "this node can no longer write its streams"):
		return ssFenced, 0, "", true
	}
	if m := reBusStarted.FindStringSubmatch(text); m != nil {
		return ssBusStarted, 0, m[1], true
	}
	if m := reManagerShared.FindStringSubmatch(text); m != nil {
		return ssManagerShared, 0, m[1], true
	}
	if m := reLeaderUp.FindStringSubmatch(text); m != nil {
		return ssLeaderUp, 0, m[1], true
	}
	if m := reLeaderDown.FindStringSubmatch(text); m != nil {
		return ssLeaderDown, 0, m[1], true
	}
	if m := reLeaseAcquired.FindStringSubmatch(text); m != nil {
		return ssLeaseAcquired, deviceOf(m), "", true
	}
	if m := reLeaseEnded.FindStringSubmatch(text); m != nil {
		// "Ownership lease for device N ... renewal is failing ... keeping the
		// collector" keeps the collector running: it ends nothing.
		if strings.Contains(text, "keeping the collector") {
			return ssNone, 0, "", false
		}
		return ssLeaseEnded, deviceOf(m), "", true
	}
	if m := rePlacedNew.FindStringSubmatch(text); m != nil {
		return ssPlaced, deviceOf(m), m[2], true
	}
	if m := rePlacedMove.FindStringSubmatch(text); m != nil {
		return ssPlaced, deviceOf(m), m[2], true
	}
	if m := reHeldBack.FindStringSubmatch(text); m != nil {
		return ssHeldBack, deviceOf(m), "", true
	}
	return ssNone, 0, "", false
}

// ssEventsOf returns the events in a container's lines, oldest first.
func ssEventsOf(container string, lines []ssLine) []ssEvent {
	var events []ssEvent
	for _, l := range lines {
		kind, device, member, ok := parseSSLine(l.Text)
		if !ok {
			continue
		}
		events = append(events, ssEvent{At: l.At, Container: container, Kind: kind, Device: device, Member: member, Text: l.Text})
	}
	return events
}

// sortSSEvents orders events by time, and keeps the order of events that
// share a time.
func sortSSEvents(events []ssEvent) {
	sort.SliceStable(events, func(i, j int) bool { return events[i].At.Before(events[j].At) })
}

// ssLeadingAt reports, from one container's events in time order, whether
// the container leads at time t. A process that ends, a process that starts
// and a state bus that starts again all end a leadership: the director
// manager that led is gone, and a new one logs its own. So does a bus that
// stops writing its streams: it disables JetStream, and a replica without
// JetStream cannot lead, whether or not its manager says so.
func ssLeadingAt(events []ssEvent, t time.Time) bool {
	leading := false
	for _, e := range events {
		if e.At.After(t) {
			break
		}
		switch e.Kind {
		case ssLeaderUp:
			leading = true
		case ssLeaderDown, ssProcessStart, ssProcessEnd, ssBusStarted, ssBusStopped, ssFenced:
			leading = false
		}
	}
	return leading
}

// ssHoldsAt reports, from one container's events in time order, whether the
// container runs the device at time t: it took the device's lease and has not
// stopped its collector since, and its process has not ended.
func ssHoldsAt(events []ssEvent, device int64, t time.Time) bool {
	holds := false
	for _, e := range events {
		if e.At.After(t) {
			break
		}
		switch e.Kind {
		case ssLeaseAcquired:
			if e.Device == device {
				holds = true
			}
		case ssLeaseEnded:
			if e.Device == device {
				holds = false
			}
		case ssProcessStart, ssProcessEnd:
			holds = false
		}
	}
	return holds
}

// ssInterval is a time a container ran a device: from the lease it took to
// the collector it stopped, or to the end of its process.
type ssInterval struct {
	Container string
	Device    int64
	From, To  time.Time
}

// ssOwnership returns every interval in which a container ran one of the
// devices, from that container's events in time order. An interval still
// open at `until` ends there.
func ssOwnership(container string, events []ssEvent, until time.Time) []ssInterval {
	open := map[int64]time.Time{}
	var out []ssInterval
	closeAll := func(at time.Time) {
		for d, from := range open {
			out = append(out, ssInterval{Container: container, Device: d, From: from, To: at})
		}
		clear(open)
	}
	for _, e := range events {
		switch e.Kind {
		case ssLeaseAcquired:
			if _, running := open[e.Device]; !running {
				open[e.Device] = e.At
			}
		case ssLeaseEnded:
			if from, running := open[e.Device]; running {
				out = append(out, ssInterval{Container: container, Device: e.Device, From: from, To: e.At})
				delete(open, e.Device)
			}
		case ssProcessStart, ssProcessEnd:
			closeAll(e.At)
		}
	}
	closeAll(until)
	sort.Slice(out, func(i, j int) bool { return out[i].From.Before(out[j].From) })
	return out
}

// ssOverlap is a time two containers ran the same device at once.
type ssOverlap struct {
	Device   int64
	A, B     string
	From, To time.Time
}

// Seconds is how long the overlap lasted.
func (o ssOverlap) Seconds() float64 { return o.To.Sub(o.From).Seconds() }

// ssOverlaps returns every time two containers ran the same device at once
// for longer than tolerance.
func ssOverlaps(intervals []ssInterval, tolerance time.Duration) []ssOverlap {
	var out []ssOverlap
	for i := 0; i < len(intervals); i++ {
		for j := i + 1; j < len(intervals); j++ {
			a, b := intervals[i], intervals[j]
			if a.Device != b.Device || a.Container == b.Container {
				continue
			}
			from := a.From
			if b.From.After(from) {
				from = b.From
			}
			to := a.To
			if b.To.Before(to) {
				to = b.To
			}
			if to.Sub(from) > tolerance {
				out = append(out, ssOverlap{Device: a.Device, A: a.Container, B: b.Container, From: from, To: to})
			}
		}
	}
	return out
}

// ssStoreCall is one request Azurite served.
type ssStoreCall struct {
	At     time.Time
	Method string
	Path   string
	Status int
}

// reAzuriteAccess reads one line of Azurite's access log, as `docker logs -t`
// prints it: the request's method, path and status.
var reAzuriteAccess = regexp.MustCompile(`"([A-Z]+) (\S+) HTTP/[0-9.]+" (\d{3})`)

// parseAzuriteCalls returns the requests in Azurite's access log lines.
func parseAzuriteCalls(lines []ssLine) []ssStoreCall {
	var calls []ssStoreCall
	for _, l := range lines {
		m := reAzuriteAccess.FindStringSubmatch(l.Text)
		if m == nil {
			continue
		}
		status, _ := strconv.Atoi(m[3])
		calls = append(calls, ssStoreCall{At: l.At, Method: m[1], Path: m[2], Status: status})
	}
	return calls
}

// inContainer reports whether the call is to a blob container of that name,
// in any account: /<account>/<container>, then a slash, a query or the end.
func (c ssStoreCall) inContainer(container string) bool {
	_, rest, ok := strings.Cut(strings.TrimPrefix(c.Path, "/"), "/")
	if !ok {
		return false
	}
	return rest == container || strings.HasPrefix(rest, container+"/") || strings.HasPrefix(rest, container+"?")
}

// ssCallRates are the store calls per second in a window, split as Azure
// prices them: writes (PUT, POST, DELETE), and reads and lists (GET, HEAD).
type ssCallRates struct {
	Seconds        float64 `json:"seconds"`
	Writes         int     `json:"writes"`
	ReadsAndLists  int     `json:"reads_and_lists"`
	Lists          int     `json:"lists"`
	WritesPerSec   float64 `json:"writes_per_sec"`
	ReadsPerSec    float64 `json:"reads_and_lists_per_sec"`
	FailedRequests int     `json:"failed_requests"`
}

// storeCallRates counts the calls to one blob container in [from, to).
func storeCallRates(calls []ssStoreCall, container string, from, to time.Time) ssCallRates {
	r := ssCallRates{Seconds: to.Sub(from).Seconds()}
	for _, c := range calls {
		if c.At.Before(from) || !c.At.Before(to) || !c.inContainer(container) {
			continue
		}
		switch c.Method {
		case "PUT", "POST", "DELETE", "PATCH":
			r.Writes++
		default:
			r.ReadsAndLists++
			if strings.Contains(c.Path, "comp=list") {
				r.Lists++
			}
		}
		// 404 is how a store answers a read of a key that is not there, and
		// 409 and 412 are the answers to a conditional write that lost, which
		// the replicas make on purpose. Anything else from 400 up failed.
		if c.Status >= 400 && c.Status != 404 && c.Status != 409 && c.Status != 412 {
			r.FailedRequests++
		}
	}
	if r.Seconds > 0 {
		r.WritesPerSec = float64(r.Writes) / r.Seconds
		r.ReadsPerSec = float64(r.ReadsAndLists) / r.Seconds
	}
	return r
}

// secretNeedles returns what to look for to find a secret in text: the
// secret itself, its URL-encoded form, and three 16-character pieces of a
// long one, from its start, its middle and its end. Each comes with a label
// that names the piece and never the secret.
func secretNeedles(index int, secret string) map[string]string {
	label := func(part string) string { return "secret_values[" + strconv.Itoa(index) + "] (" + part + ")" }
	needles := map[string]string{secret: label("whole")}
	if enc := url.QueryEscape(secret); enc != secret {
		needles[enc] = label("URL-encoded")
	}
	if n := len(secret); n >= 24 {
		needles[secret[:16]] = label("first 16 characters")
		needles[secret[n/2-8:n/2+8]] = label("middle 16 characters")
		needles[secret[n-16:]] = label("last 16 characters")
	}
	return needles
}

// ssSecretHit is a place a secret was found.
type ssSecretHit struct {
	Where string `json:"where"`
	What  string `json:"what"`
	Line  string `json:"line"`
}

// scanForSecrets looks for each secret in each named text, and returns where
// it found one. The line it returns has every piece of the secret masked, and
// the hit names the longest piece found.
func scanForSecrets(secrets []string, texts map[string]string) []ssSecretHit {
	var hits []ssSecretHit
	names := make([]string, 0, len(texts))
	for name := range texts {
		names = append(names, name)
	}
	sort.Strings(names)
	for i, secret := range secrets {
		needles := secretNeedles(i, secret)
		// Longest first, so that a whole secret is masked before the pieces
		// inside it are looked for. A fixed order also makes the result the
		// same on every run.
		order := make([]string, 0, len(needles))
		for needle := range needles {
			order = append(order, needle)
		}
		sort.Slice(order, func(a, b int) bool {
			if len(order[a]) != len(order[b]) {
				return len(order[a]) > len(order[b])
			}
			return order[a] < order[b]
		})
		for _, name := range names {
			for _, line := range strings.Split(texts[name], "\n") {
				what, masked := "", line
				for _, needle := range order {
					if strings.Contains(masked, needle) {
						if what == "" {
							what = needles[needle]
						}
						masked = strings.ReplaceAll(masked, needle, "[SECRET]")
					}
				}
				if what == "" {
					continue
				}
				if len(masked) > 300 {
					masked = masked[:300] + "…"
				}
				hits = append(hits, ssSecretHit{Where: name, What: what, Line: masked})
			}
		}
	}
	return hits
}
