package runner

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// The copies of the streams of a director whose own bus keeps them on its
// replicas, as each stream's leader reports them on /health: the number of
// copies, and how many are offline or behind (cluster_probe.go reads them).
// scale_out_in judges them with min_copies and scaled_out_copies (VM-8792):
// replicas that join and leave under load must never leave a stream with
// fewer current copies than the floor.

// copiesSampleEvery is how often the sampler reads every replica's /health.
const copiesSampleEvery = 2 * time.Second

// currentCopies is how many of a stream's copies are current: a copy that is
// offline, or more than copiesLagSlack operations behind, does not count.
func currentCopies(s streamCopies) int {
	if s.Offline > 0 && s.MaxLag > copiesLagSlack {
		return max(0, s.Actual-s.Offline)
	}
	return s.Actual
}

// copiesLow is the fewest current copies a stream had while the sampler ran,
// and when it first had them.
type copiesLow struct {
	Current int       `json:"current"`
	At      time.Time `json:"at"`
}

// copiesSampler reads, every copiesSampleEvery, each stream's copies as the
// replica that leads the stream reports them, from the /health of each of its
// containers, and keeps the fewest current copies each stream had. It shares
// no state with the run: it learns each container's member name from the
// container's own log, once, so it can run beside the action.
type copiesSampler struct {
	containers []string
	members    map[string]string
	fewest     map[string]copiesLow
	samples    int
	stop       chan struct{}
	done       chan struct{}
}

func startCopiesSampler(containers []string) *copiesSampler {
	c := &copiesSampler{
		containers: containers,
		members:    map[string]string{},
		fewest:     map[string]copiesLow{},
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go c.loop()
	return c
}

func (c *copiesSampler) loop() {
	defer close(c.done)
	tick := time.NewTicker(copiesSampleEvery)
	defer tick.Stop()
	for {
		c.sample(time.Now())
		select {
		case <-c.stop:
			return
		case <-tick.C:
		}
	}
}

// sample reads every container once. A container that does not run, or has
// not logged its member name yet, says nothing.
func (c *copiesSampler) sample(now time.Time) {
	led := map[string]streamCopies{}
	for _, ct := range c.containers {
		member := c.members[ct]
		if member == "" {
			out, err := exec.Command("docker", "logs", ct).CombinedOutput()
			if err != nil {
				continue
			}
			if member = memberInLog(string(out)); member == "" {
				continue
			}
			c.members[ct] = member
		}
		raw, err := exec.Command("docker", "exec", ct, "wget", "-qO-", "-T", "2", "http://127.0.0.1:15551/health").Output()
		if err != nil {
			continue
		}
		for name, s := range ledCopies(raw, member) {
			led[name] = s
		}
	}
	c.record(led, now)
}

// record keeps the fewest current copies of each stream in led.
func (c *copiesSampler) record(led map[string]streamCopies, now time.Time) {
	if len(led) == 0 {
		return
	}
	c.samples++
	for name, s := range led {
		cur := currentCopies(s)
		if low, ok := c.fewest[name]; !ok || cur < low.Current {
			c.fewest[name] = copiesLow{Current: cur, At: now}
		}
	}
}

// finish stops the sampler and returns the fewest current copies of each
// stream, and how many reads saw a stream.
func (c *copiesSampler) finish() (map[string]copiesLow, int) {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	<-c.done
	return c.fewest, c.samples
}

// memberInLog returns the member name the director's manager last logged in
// log: the name its bus has among the replicas.
func memberInLog(log string) string {
	member := ""
	for _, line := range strings.Split(log, "\n") {
		if m := reManagerShared.FindStringSubmatch(line); m != nil {
			member = m[1]
		}
	}
	return member
}

// ledCopies returns the copies of the streams that member leads, from its
// /health answer raw.
func ledCopies(raw []byte, member string) map[string]streamCopies {
	out := map[string]streamCopies{}
	for name, s := range parseNodeHealth(0, raw).Copies {
		if s.Leader == member {
			out[name] = s
		}
	}
	return out
}

// streamCopiesNow returns each stream's copies as the running replica that
// leads it reports them now.
func (run *ssRun) streamCopiesNow() map[string]streamCopies {
	out := map[string]streamCopies{}
	for _, rep := range run.replicas {
		if !run.running[rep.N] {
			continue
		}
		member := run.memberOf(rep.N)
		if member == "" {
			continue
		}
		raw, err := exec.Command("docker", "exec", rep.Container, "wget", "-qO-", "-T", "2", "http://127.0.0.1:15551/health").Output()
		if err != nil {
			continue
		}
		for name, s := range ledCopies(raw, member) {
			out[name] = s
		}
	}
	return out
}

// waitStreamCopies waits until every stream that a running replica leads has
// at least want current copies, as its leader reports them. It returns the
// streams that have fewer at the last look, empty when every one has them.
func (run *ssRun) waitStreamCopies(want int, deadline time.Time) []string {
	for {
		copies := run.streamCopiesNow()
		short := shortOfCopies(copies, want)
		if len(copies) == 0 {
			short = []string{"no replica reports leading a stream"}
		}
		if len(short) == 0 || time.Now().After(deadline) || sleepCtx(run.r.ctx, 2*time.Second) != nil {
			return short
		}
	}
}

// shortOfCopies describes the streams of copies that have fewer than want
// current copies, sorted.
func shortOfCopies(copies map[string]streamCopies, want int) []string {
	var out []string
	for name, s := range copies {
		if cur := currentCopies(s); cur < want {
			out = append(out, fmt.Sprintf("%s has %d current copies of %d", name, cur, s.Actual))
		}
	}
	sort.Strings(out)
	return out
}

// judgeFewestCopies fails the run for each stream whose fewest current copies
// were below floor.
func (run *ssRun) judgeFewestCopies(fewest map[string]copiesLow, samples, floor int) {
	run.measures["copies_fewest"] = fewest
	run.measures["copies_samples"] = samples
	if samples == 0 {
		run.fail("min_copies: no read of /health showed the copies of a stream")
		return
	}
	names := make([]string, 0, len(fewest))
	for name := range fewest {
		names = append(names, name)
	}
	sort.Strings(names)
	low := -1
	for _, name := range names {
		f := fewest[name]
		if low < 0 || f.Current < low {
			low = f.Current
		}
		if f.Current < floor {
			run.fail("stream %s had %d current copies at %s, below min_copies %d", name, f.Current, f.At.UTC().Format("15:04:05"), floor)
		}
	}
	fmt.Printf("    copies: %d streams, the fewest current copies of any was %d (min_copies %d), in %d reads\n", len(names), low, floor, samples)
}
