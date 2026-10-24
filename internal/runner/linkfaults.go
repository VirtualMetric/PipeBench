package runner

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Link faults change the traffic between two containers of a run without
// changing the image under test. The harness runs a short-lived helper (the
// nettool image: Alpine with iptables and tc) in the network namespace of a
// container, with NET_ADMIN, and the helper sets that namespace's rules:
//
//   - A cut of the traffic from A to B drops, in B's namespace, every packet
//     that arrives from A's address (iptables, INPUT). A's packets vanish on
//     the way, as they do when a network fails, so A waits and times out.
//     Dropping them as they leave A would fail A's sends at once instead,
//     which no network does.
//   - Delay, jitter and loss on the traffic from A to B go through netem in
//     A's namespace, on a band of a prio qdisc that a filter selects by B's
//     address (tc). netem only acts on traffic that leaves a namespace. Every
//     other packet takes band 1, which has no netem.
//
// The harness keeps the faults every direction should have, and rebuilds a
// container's rules from nothing whenever one of its links changes, so a
// restore is the same operation with fewer faults. The rules live in the
// container's namespace: they go when the container stops, and a container
// that starts again starts clean, possibly on a new address. The harness
// therefore rebuilds the rules that involve a container after a step starts
// it. Only IPv4 traffic is changed.

// linkFault is what happens to the traffic from one container to another.
type linkFault struct {
	Cut     bool
	Delay   time.Duration
	Jitter  time.Duration
	LossPct float64
}

func (f linkFault) shaped() bool { return f.Delay > 0 || f.Jitter > 0 || f.LossPct > 0 }
func (f linkFault) none() bool   { return !f.Cut && !f.shaped() }

func (f linkFault) String() string {
	var parts []string
	if f.Cut {
		parts = append(parts, "cut")
	}
	if f.Delay > 0 {
		parts = append(parts, "delay "+f.Delay.String())
	}
	if f.Jitter > 0 {
		parts = append(parts, "jitter "+f.Jitter.String())
	}
	if f.LossPct > 0 {
		parts = append(parts, "loss "+strconv.FormatFloat(f.LossPct, 'f', -1, 64)+"%")
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

// linkKey is one direction: the traffic from container From to container To.
type linkKey struct{ From, To string }

// linkFaults is the set of faults a run has in force, by container name.
type linkFaults struct {
	mu     sync.Mutex
	faults map[linkKey]linkFault

	// running reports whether a container runs, addrOf returns a running
	// container's address on the run's network, and run applies a script in a
	// container's network namespace. Tests replace them.
	running func(container string) bool
	addrOf  func(container string) (string, error)
	run     func(ctx context.Context, container, script string) error
}

func newLinkFaults(image, network string) *linkFaults {
	return &linkFaults{
		faults:  map[linkKey]linkFault{},
		running: containerRunning,
		addrOf:  func(c string) (string, error) { return containerAddress(c, network) },
		run: func(ctx context.Context, c, script string) error {
			return runNettool(ctx, image, c, script)
		},
	}
}

// set puts changes in force: each direction gets its new fault, and a fault
// that is none removes the direction. It rebuilds the rules of every
// container at either end of a changed direction.
func (l *linkFaults) set(ctx context.Context, changes map[linkKey]linkFault) error {
	l.mu.Lock()
	touched := map[string]bool{}
	for k, f := range changes {
		if f.none() {
			delete(l.faults, k)
		} else {
			l.faults[k] = f
		}
		touched[k.From], touched[k.To] = true, true
	}
	l.mu.Unlock()
	return l.rebuild(ctx, touched)
}

// get returns the fault on one direction.
func (l *linkFaults) get(k linkKey) linkFault {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.faults[k]
}

// restoreAll removes every fault.
func (l *linkFaults) restoreAll(ctx context.Context) error {
	l.mu.Lock()
	touched := map[string]bool{}
	for k := range l.faults {
		touched[k.From], touched[k.To] = true, true
	}
	l.faults = map[linkKey]linkFault{}
	l.mu.Unlock()
	return l.rebuild(ctx, touched)
}

// resync rebuilds the rules of container and of every container it shares a
// fault with, after container started again: its own rules went with its old
// namespace, and its address may have changed.
func (l *linkFaults) resync(ctx context.Context, container string) error {
	l.mu.Lock()
	touched := map[string]bool{}
	for k := range l.faults {
		if k.From == container || k.To == container {
			touched[k.From], touched[k.To] = true, true
		}
	}
	l.mu.Unlock()
	if len(touched) == 0 {
		return nil
	}
	return l.rebuild(ctx, touched)
}

// describe lists the faults in force, one direction each, in a stable order.
func (l *linkFaults) describe() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.faults))
	for k, f := range l.faults {
		out = append(out, fmt.Sprintf("%s → %s: %s", k.From, k.To, f))
	}
	sort.Strings(out)
	return out
}

// rebuild sets the rules of every touched container, at once.
func (l *linkFaults) rebuild(ctx context.Context, touched map[string]bool) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for c := range touched {
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			if err := l.rebuildOne(ctx, c); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", c, err))
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// rebuildOne sets one container's rules to exactly the faults that involve
// it. A container that is not running has no namespace, so it has nothing to
// set; a peer that is not running has no address, so its rules wait for the
// resync that follows its start.
func (l *linkFaults) rebuildOne(ctx context.Context, c string) error {
	if !l.running(c) {
		return nil
	}
	l.mu.Lock()
	var drops []string
	shapes := map[string]linkFault{}
	for k, f := range l.faults {
		if k.To == c && f.Cut {
			drops = append(drops, k.From)
		}
		if k.From == c && f.shaped() {
			shapes[k.To] = f
		}
	}
	l.mu.Unlock()

	var dropAddrs []string
	for _, peer := range drops {
		if addr, err := l.addrOf(peer); err == nil {
			dropAddrs = append(dropAddrs, addr)
		}
	}
	var shaped []shapedPeer
	for peer, f := range shapes {
		if addr, err := l.addrOf(peer); err == nil {
			shaped = append(shaped, shapedPeer{Addr: addr, Fault: f})
		}
	}
	script, err := ruleScript(dropAddrs, shaped)
	if err != nil {
		return err
	}
	return l.run(ctx, c, script)
}

// shapedPeer is a peer address whose traffic gets delay, jitter or loss.
type shapedPeer struct {
	Addr  string
	Fault linkFault
}

// maxShapedPeers is how many peers one container can shape at once: a prio
// qdisc has 16 bands, and band 1 carries the rest of the traffic.
const maxShapedPeers = 15

// ruleScript returns the shell script that sets a network namespace's rules
// to exactly drops (addresses whose packets it drops on the way in) and
// shaped (addresses whose traffic it delays or drops on the way out). With
// neither, it removes every rule the harness set.
func ruleScript(drops []string, shaped []shapedPeer) (string, error) {
	if len(shaped) > maxShapedPeers {
		return "", fmt.Errorf("a container can delay or drop the traffic to at most %d peers at once, not %d", maxShapedPeers, len(shaped))
	}
	drops = append([]string{}, drops...)
	sort.Strings(drops)
	shaped = append([]shapedPeer{}, shaped...)
	sort.Slice(shaped, func(i, j int) bool { return shaped[i].Addr < shaped[j].Addr })

	var b strings.Builder
	b.WriteString("set -e\n")
	// Some kernels have only the legacy iptables back end.
	b.WriteString("ipt=iptables\n")
	b.WriteString("iptables -S INPUT >/dev/null 2>&1 || ipt=iptables-legacy\n")
	if len(drops) == 0 {
		b.WriteString("$ipt -D INPUT -j PIPEBENCH 2>/dev/null || true\n")
		b.WriteString("$ipt -F PIPEBENCH 2>/dev/null || true\n")
		b.WriteString("$ipt -X PIPEBENCH 2>/dev/null || true\n")
	} else {
		b.WriteString("$ipt -N PIPEBENCH 2>/dev/null || $ipt -F PIPEBENCH\n")
		b.WriteString("$ipt -C INPUT -j PIPEBENCH 2>/dev/null || $ipt -I INPUT -j PIPEBENCH\n")
		for _, a := range drops {
			fmt.Fprintf(&b, "$ipt -A PIPEBENCH -s %s -j DROP\n", a)
		}
	}
	// Remove the qdiscs an earlier rebuild left, on every interface.
	b.WriteString("for d in $(ls /sys/class/net); do [ \"$d\" = lo ] || tc qdisc del dev \"$d\" root 2>/dev/null || true; done\n")
	for i, p := range shaped {
		band := i + 2
		fmt.Fprintf(&b, "dev=$(ip -o route get %s | sed -n 's/.* dev \\([^ ]*\\).*/\\1/p')\n", p.Addr)
		b.WriteString("tc qdisc show dev \"$dev\" | grep -q 'qdisc prio 1:' || tc qdisc add dev \"$dev\" root handle 1: prio bands 16 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n")
		fmt.Fprintf(&b, "tc qdisc add dev \"$dev\" parent 1:%d handle %d0: netem%s\n", band, band, netemArgs(p.Fault))
		fmt.Fprintf(&b, "tc filter add dev \"$dev\" protocol ip parent 1:0 prio 1 u32 match ip dst %s/32 flowid 1:%d\n", p.Addr, band)
	}
	return b.String(), nil
}

// netemArgs returns netem's arguments for a fault, with a leading space.
func netemArgs(f linkFault) string {
	var b strings.Builder
	if f.Delay > 0 || f.Jitter > 0 {
		fmt.Fprintf(&b, " delay %dms", f.Delay.Milliseconds())
		if f.Jitter > 0 {
			fmt.Fprintf(&b, " %dms", f.Jitter.Milliseconds())
		}
	}
	if f.LossPct > 0 {
		fmt.Fprintf(&b, " loss %s%%", strconv.FormatFloat(f.LossPct, 'f', -1, 64))
	}
	return b.String()
}

// runNettool runs script in container's network namespace, in a helper that
// removes itself. Its name starts with bench- so that `harness clean` removes
// one an interrupted run left behind.
func runNettool(ctx context.Context, image, container, script string) error {
	name := fmt.Sprintf("bench-linkfault-%d", time.Now().UnixNano())
	cctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	out, err := exec.CommandContext(cctx, "docker", "run", "--rm", "--name", name,
		"--network", "container:"+container, "--cap-add", "NET_ADMIN",
		"--cpus", "0.5", "--memory", "64m",
		image, "-c", script).CombinedOutput()
	if err != nil {
		_ = exec.Command("docker", "rm", "-f", name).Run()
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// containerAddress returns a running container's IPv4 address on network, or
// on its first network when network is empty.
func containerAddress(container, network string) (string, error) {
	format := "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}"
	if network != "" {
		format = fmt.Sprintf("{{with index .NetworkSettings.Networks %q}}{{.IPAddress}}{{end}}", network)
	}
	out, err := exec.Command("docker", "inspect", "-f", format, container).Output()
	if err != nil {
		return "", fmt.Errorf("docker inspect %s: %w", container, err)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("%s has no address (it is not running, or not on the network)", container)
	}
	return fields[0], nil
}
