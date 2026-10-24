package runner

import (
	"context"
	"fmt"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// This file holds the scenario steps that change links and disks, and the
// rows check of a case whose data comes from a database.

// linkAction runs cut_link, shape_link, restore_link and split.
func (sr *scenarioRun) linkAction(i int, st config.ScenarioStep, rep *scenarioStepReport) (bool, string, error) {
	changes := map[linkKey]linkFault{}
	var what string
	switch st.Do {
	case "split":
		sides := make([][]string, len(st.Groups))
		var nodes [][]int
		for gi, g := range st.Groups {
			var ns []int
			for _, sel := range g {
				n, err := sr.resolveNode(sel)
				if err != nil {
					return false, err.Error(), nil
				}
				sides[gi] = append(sides[gi], sr.containers[n-1])
				ns = append(ns, n)
			}
			nodes = append(nodes, ns)
		}
		for gi := range sides {
			for gj := range sides {
				if gi == gj {
					continue
				}
				for _, a := range sides[gi] {
					for _, b := range sides[gj] {
						f := sr.links.get(linkKey{a, b})
						f.Cut = true
						changes[linkKey{a, b}] = f
					}
				}
			}
		}
		what = "split the nodes into " + describeSides(nodes)
	case "restore_link":
		if st.Node == "" {
			left := sr.links.describe()
			sr.disrupt(i, "restore_link", 0)
			if err := sr.links.restoreAll(sr.ctx); err != nil {
				return false, "restoring every link: " + err.Error(), nil
			}
			return true, fmt.Sprintf("restored every link (%d direction(s) had a fault)", len(left)), nil
		}
		fallthrough
	default:
		froms, fromNode, err := sr.linkEnds(st.Node, "")
		if err != nil {
			return false, err.Error(), nil
		}
		rep.Node = fromNode
		fault, err := shapeOf(st)
		if err != nil {
			return false, err.Error(), nil
		}
		var pairs int
		for _, from := range froms {
			for _, sel := range st.PeerList() {
				peers, _, err := sr.linkEnds(sel, from)
				if err != nil {
					return false, err.Error(), nil
				}
				for _, peer := range peers {
					keys := []linkKey{{from, peer}}
					if !st.OneWay {
						keys = append(keys, linkKey{peer, from})
					}
					for _, k := range keys {
						f := sr.links.get(k)
						if prev, ok := changes[k]; ok {
							f = prev
						}
						switch st.Do {
						case "cut_link":
							f.Cut = true
						case "shape_link":
							f.Delay, f.Jitter, f.LossPct = fault.Delay, fault.Jitter, fault.LossPct
						case "restore_link":
							f = linkFault{}
						}
						changes[k] = f
					}
					pairs++
				}
			}
		}
		if pairs == 0 {
			return false, "the step names no link: no peer is running", nil
		}
		dir := "both ways"
		if st.OneWay {
			dir = "one way, from " + st.Node
		}
		switch st.Do {
		case "cut_link":
			what = fmt.Sprintf("cut %d link(s) %s", pairs, dir)
		case "shape_link":
			what = fmt.Sprintf("set %s on %d link(s) %s", fault, pairs, dir)
		case "restore_link":
			what = fmt.Sprintf("restored %d link(s) %s", pairs, dir)
		}
	}
	sr.disrupt(i, st.Do, rep.Node)
	if err := sr.links.set(sr.ctx, changes); err != nil {
		return false, what + ", but: " + err.Error(), nil
	}
	return true, what, nil
}

// shapeOf reads a shape_link step's delay, jitter and loss.
func shapeOf(st config.ScenarioStep) (linkFault, error) {
	var f linkFault
	if st.Do != "shape_link" {
		return f, nil
	}
	if st.Latency != "" {
		d, err := time.ParseDuration(st.Latency)
		if err != nil {
			return f, fmt.Errorf("latency: %w", err)
		}
		f.Delay = d
	}
	if st.Jitter != "" {
		d, err := time.ParseDuration(st.Jitter)
		if err != nil {
			return f, fmt.Errorf("jitter: %w", err)
		}
		f.Jitter = d
	}
	loss, err := config.LossPercent(st.Loss)
	if err != nil {
		return f, err
	}
	f.LossPct = loss
	return f, nil
}

// linkEnds turns one end of a link into container names. "all" is every node
// that is a member now, except the container except names. A name that starts
// with bench- is another container of the run. Anything else is a node
// selector; node is then its number, else 0.
func (sr *scenarioRun) linkEnds(sel, except string) (containers []string, node int, err error) {
	switch {
	case sel == "all":
		for n := 1; n <= len(sr.containers); n++ {
			if sr.active[n] && sr.containers[n-1] != except {
				containers = append(containers, sr.containers[n-1])
			}
		}
		return containers, 0, nil
	case strings.HasPrefix(sel, "bench-"):
		// A container that does not run has no namespace to change, so a
		// fault on it would be reported and not be in force.
		if !sr.links.running(sel) {
			return nil, 0, fmt.Errorf("container %s does not run", sel)
		}
		return []string{sel}, 0, nil
	}
	n, err := sr.resolveNode(sel)
	if err != nil {
		return nil, 0, err
	}
	if c := sr.containers[n-1]; c != except {
		containers = []string{c}
	}
	return containers, n, nil
}

// describeSides writes a split's sides, such as [1 2] | [3 4].
func describeSides(sides [][]int) string {
	parts := make([]string, len(sides))
	for i, s := range sides {
		parts[i] = fmt.Sprint(s)
	}
	return strings.Join(parts, " | ")
}

// relink rebuilds the link faults that involve node n, after a step started
// it: its rules went with its old network namespace.
func (sr *scenarioRun) relink(n int) {
	// The case goes on believing the faults are in force, so a failure here
	// fails the run.
	if err := sr.links.resync(sr.ctx, sr.containers[n-1]); err != nil {
		sr.failures = append(sr.failures, fmt.Sprintf("could not put node %d's link faults back after its start: %v", n, err))
	}
}

// restoreAtEnd puts every link and every disk back as it was, once the steps
// have run, so that the drain measures delivery and not a fault a case left
// in force. A case that relies on this gets a note.
func (sr *scenarioRun) restoreAtEnd(ctx context.Context) {
	if left := sr.links.describe(); len(left) > 0 {
		if err := sr.links.restoreAll(ctx); err != nil {
			sr.failures = append(sr.failures, "could not restore the links at the end of the steps: "+err.Error())
		} else {
			sr.notes = append(sr.notes, fmt.Sprintf("the steps left %d link fault(s) in force, which the harness removed before the drain: %s",
				len(left), strings.Join(left, "; ")))
		}
	}
	nodes := make([]int, 0, len(sr.filled))
	for n := range sr.filled {
		nodes = append(nodes, n)
	}
	sort.Ints(nodes)
	for _, n := range nodes {
		if err := sr.freeDisk(ctx, n); err != nil {
			sr.failures = append(sr.failures, fmt.Sprintf("could not free node %d's disk at the end of the steps: %v", n, err))
			continue
		}
		sr.notes = append(sr.notes, fmt.Sprintf("the steps left node %d's disk full, which the harness freed before the drain", n))
	}
}

// fillerName is the file fill_disk writes on a node's disk.
const fillerName = ".pipebench-fill"

// diskAction runs fill_disk and free_disk.
func (sr *scenarioRun) diskAction(i int, st config.ScenarioStep, rep *scenarioStepReport) (bool, string, error) {
	n, err := sr.resolveNode(st.Node)
	if err != nil {
		return false, err.Error(), nil
	}
	rep.Node = n
	if st.As != "" {
		sr.names[st.As] = n
	}
	disk := sr.sc.NodeDisk
	if !disk.HasNode(n) {
		return false, fmt.Sprintf("node %d has no disk of its own (cluster.scenario.node_disk.nodes)", n), nil
	}
	c := sr.containers[n-1]
	if st.Do == "free_disk" {
		sr.disrupt(i, "free_disk", n)
		if err := sr.freeDisk(sr.ctx, n); err != nil {
			return false, err.Error(), nil
		}
		free, _ := diskFree(c, disk.Path)
		return true, fmt.Sprintf("freed node %d's disk: %s free", n, fmtBytes(free)), nil
	}
	leave := int64(1 << 20)
	if st.Leave != "" {
		if leave, err = config.ParseByteSize(st.Leave); err != nil {
			return false, fmt.Sprintf("leave %q: %v", st.Leave, err), nil
		}
	}
	free, err := diskFree(c, disk.Path)
	if err != nil {
		return false, err.Error(), nil
	}
	size := free - leave
	if size <= 0 {
		return false, fmt.Sprintf("node %d's disk has only %s free, no more than leave %s", n, fmtBytes(free), fmtBytes(leave)), nil
	}
	sr.disrupt(i, "fill_disk", n)
	filler := path.Join(disk.Path, fillerName)
	// fallocate takes the space at once; dd is for a file system without it.
	script := fmt.Sprintf("fallocate -l %d %s 2>/dev/null || dd if=/dev/zero of=%s bs=1048576 count=%d 2>/dev/null; true",
		size, filler, filler, size>>20)
	if err := dockerCmd(sr.ctx, 3*time.Minute, "exec", c, "sh", "-c", script); err != nil {
		return false, fmt.Sprintf("filling node %d's disk: %v", n, err), nil
	}
	sr.filled[n] = true
	after, err := diskFree(c, disk.Path)
	if err != nil {
		return false, err.Error(), nil
	}
	if after > leave+(1<<20) {
		return false, fmt.Sprintf("filled node %d's disk, but %s is still free (wanted %s)", n, fmtBytes(after), fmtBytes(leave)), nil
	}
	return true, fmt.Sprintf("filled node %d's disk: %s of %s free", n, fmtBytes(after), fmtBytes(free)), nil
}

// freeDisk removes the file fill_disk wrote on node n's disk.
func (sr *scenarioRun) freeDisk(ctx context.Context, n int) error {
	filler := path.Join(sr.sc.NodeDisk.Path, fillerName)
	if err := dockerCmd(ctx, time.Minute, "exec", sr.containers[n-1], "rm", "-f", filler); err != nil {
		return fmt.Errorf("freeing node %d's disk: %w", n, err)
	}
	delete(sr.filled, n)
	return nil
}

// diskFree returns the bytes free on the file system at dir in container.
func diskFree(container, dir string) (int64, error) {
	out, err := exec.Command("docker", "exec", container, "df", "-P", "-k", dir).Output()
	if err != nil {
		return 0, fmt.Errorf("reading the free space at %s on %s: %w", dir, container, err)
	}
	return parseDfFree(string(out))
}

// parseDfFree reads the free space from `df -P -k` output: the fourth column
// of the second line, in KiB.
func parseDfFree(out string) (int64, error) {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("unexpected df output %q", out)
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 4 {
		return 0, fmt.Errorf("unexpected df output %q", out)
	}
	kib, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unexpected df output %q", out)
	}
	return kib << 10, nil
}

func fmtBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return strconv.FormatFloat(float64(n)/(1<<30), 'f', 2, 64) + " GiB"
	case n >= 1<<20:
		return strconv.FormatFloat(float64(n)/(1<<20), 'f', 1, 64) + " MiB"
	case n >= 1<<10:
		return strconv.FormatFloat(float64(n)/(1<<10), 'f', 0, 64) + " KiB"
	}
	return strconv.FormatInt(n, 10) + " B"
}

// rowsCheck compares the rows a database holds with the rows whose key reached
// the receiver (cluster.scenario.rows).
type rowsCheck struct {
	// Written is how many rows the query returned.
	Written int64 `json:"written"`
	// Arrived is how many of those rows had their key arrive.
	Arrived int64 `json:"arrived"`
	// Missing is how many never did, and MissingKeys names the first of them.
	Missing     int64    `json:"missing"`
	MissingKeys []string `json:"missing_keys,omitempty"`
	// ArrivedTwice is how many rows arrived on more than one line, and
	// TwiceKeys names the first of them, in the query's order.
	ArrivedTwice int64    `json:"arrived_twice"`
	TwiceKeys    []string `json:"twice_keys,omitempty"`
	// Unknown is how many keys arrived that no row has.
	Unknown int64 `json:"unknown"`
	// LinesWithoutKey is how many received lines had no key at all.
	LinesWithoutKey int64 `json:"lines_without_key"`
	// Error says why the rows could not be compared. Missing is then unknown.
	Error string `json:"error,omitempty"`
}

// compareRows counts which rows arrived, from the keys the query returned and
// the keys the receiver saw.
func compareRows(rows []string, received map[string]int64, linesWithoutKey int64) rowsCheck {
	c := rowsCheck{LinesWithoutKey: linesWithoutKey}
	want := map[string]bool{}
	for _, r := range rows {
		if r == "" || want[r] {
			continue
		}
		want[r] = true
		c.Written++
		switch n := received[r]; {
		case n == 0:
			c.Missing++
			if len(c.MissingKeys) < 20 {
				c.MissingKeys = append(c.MissingKeys, r)
			}
		case n > 1:
			c.Arrived++
			c.ArrivedTwice++
			if len(c.TwiceKeys) < 20 {
				c.TwiceKeys = append(c.TwiceKeys, r)
			}
		default:
			c.Arrived++
		}
	}
	for k := range received {
		if !want[k] {
			c.Unknown++
		}
	}
	return c
}

// checkRows stops the case's row writer, waits for the rows to arrive, and
// compares them with what the database holds.
func (sr *scenarioRun) checkRows() rowsCheck {
	r := sr.sc.Rows
	if r.Writer != "" {
		// No new row may appear while the harness counts.
		if err := dockerCmd(sr.ctx, time.Minute, "stop", "-t", "5", "bench-"+r.Writer); err != nil {
			sr.notes = append(sr.notes, fmt.Sprintf("could not stop the row writer bench-%s: %v", r.Writer, err))
		}
	}
	deadline := time.Now().Add(r.DrainOrDefault())
	var last rowsCheck
	for {
		last = sr.compareRowsOnce()
		if last.Error == "" && last.Missing == 0 {
			break
		}
		if time.Now().After(deadline) || sleepCtx(sr.ctx, 2*time.Second) != nil {
			break
		}
	}
	if last.Error != "" {
		fmt.Printf("  rows: could not compare the database's rows with what arrived: %s\n", last.Error)
	} else {
		fmt.Printf("  rows: %d written, %d arrived, %d missing, %d arrived twice, %d unknown keys, %d lines without a key\n",
			last.Written, last.Arrived, last.Missing, last.ArrivedTwice, last.Unknown, last.LinesWithoutKey)
		if len(last.TwiceKeys) > 0 {
			fmt.Printf("    the first rows that arrived twice: %s\n", strings.Join(last.TwiceKeys, ", "))
		}
	}
	return last
}

func (sr *scenarioRun) compareRowsOnce() rowsCheck {
	rows, err := sr.queryRows(sr.sc.Rows.Query)
	if err != nil {
		return rowsCheck{Error: err.Error()}
	}
	var keys struct {
		Keys            map[string]int64 `json:"keys"`
		LinesWithoutKey int64            `json:"lines_without_key"`
	}
	if err := sr.receiverGet("/keys", &keys); err != nil {
		return rowsCheck{Error: "reading the receiver's keys: " + err.Error()}
	}
	return compareRows(rows, keys.Keys, keys.LinesWithoutKey)
}

// queryRows runs query against the case's database and returns one value a
// row.
func (sr *scenarioRun) queryRows(query string) ([]string, error) {
	db := sr.tc.Database
	engine, ok := config.DatabaseEngines[db.Engine]
	if !ok || engine.BuildQueryCmd == nil {
		return nil, fmt.Errorf("database engine %q cannot be queried", db.Engine)
	}
	args := append([]string{"exec", "bench-database"}, engine.BuildQueryCmd(db.DatabaseOrDefault(), query)...)
	ctx, cancel := context.WithTimeout(sr.ctx, time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("querying the database: %v: %s", err, strings.TrimSpace(string(out)))
	}
	var rows []string
	for _, line := range strings.Split(string(out), "\n") {
		if v := strings.TrimSpace(line); v != "" {
			rows = append(rows, v)
		}
	}
	return rows, nil
}
