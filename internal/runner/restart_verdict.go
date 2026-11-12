package runner

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// restartVerdict is what a restart or crash case concludes from what the
// generator sent, what the receiver counted, and the log of the subject that
// stopped.
type restartVerdict struct {
	// LossPct is the share of the lines sent that never arrived, counted by Basis.
	LossPct float64
	// Accounted is how many lines count as arrived, and Basis how they were
	// counted: "received", or "unique" when the receiver tracks distinct lines.
	Accounted int64
	Basis     string
	// Notes are for the run's output, and Errors are why the case failed.
	Notes  []string
	Errors []string
}

// judgeRestart judges a restart or crash case.
//
// Loss is counted over the distinct lines when the receiver tracks them, as the
// mid-delivery driver counts it. A recovery can deliver lines twice, and raw
// counts would let those copies hide lines that were lost.
//
// correctness.min_loss_pct is a floor for a baseline. A case that shows what a
// subject loses without a safeguard proves nothing when it loses nothing,
// because the run then built nothing to lose, and the case beside it passed
// without being tested.
func judgeRestart(tc *config.TestCase, sent int64, recv ReceiverMetrics, stoppedLog string) restartVerdict {
	c := tc.Correctness
	v := restartVerdict{Accounted: recv.LinesReceived, Basis: "received"}
	if tc.TracksUniqueLines() && recv.UniqueLines > 0 {
		v.Accounted, v.Basis = recv.UniqueLines, "unique"
	}

	if sent > 0 {
		v.LossPct = max(100.0*(1.0-float64(v.Accounted)/float64(sent)), 0)
	}
	if v.LossPct > c.ExpectedLossPct {
		v.Errors = append(v.Errors, fmt.Sprintf("expected loss <= %.2f%%, got %.2f%% (%s of %s lines lost, counted by %s)",
			c.ExpectedLossPct, v.LossPct, formatCount(sent-v.Accounted), formatCount(sent), v.Basis))
	}
	if c.MinLossPct > 0 && v.LossPct < c.MinLossPct {
		v.Errors = append(v.Errors, fmt.Sprintf("expected loss >= %.2f%%, got %.2f%%: the run built too little to lose, so it proves nothing",
			c.MinLossPct, v.LossPct))
	}

	if recv.LinesReceived > sent {
		extra := recv.LinesReceived - sent
		if tc.IsKafkaType() || c.AllowOverDelivery {
			// At-least-once transports (Kafka, S3-via-SQS notifications, SQS,
			// Kinesis): crash/restart recovery may re-deliver records already
			// buffered. That is correct behavior — duplicates, never loss —
			// so over-delivery is informational here, not a failure.
			v.Notes = append(v.Notes, fmt.Sprintf("over-delivery of %s lines (at-least-once duplicates from recovery — not a failure)", formatCount(extra)))
		} else {
			v.Errors = append(v.Errors, fmt.Sprintf("over-delivery: received %s lines but only %s were sent (%s extra/duplicate lines)",
				formatCount(recv.LinesReceived), formatCount(sent), formatCount(extra)))
		}
	}
	if c.ValidateDedup && recv.Duplicates > 0 && !c.AllowOverDelivery {
		v.Errors = append(v.Errors, fmt.Sprintf("expected 0 duplicates, got %s", formatCount(recv.Duplicates)))
	}
	if c.ValidateContent && recv.MalformedLines > 0 {
		v.Errors = append(v.Errors, fmt.Sprintf("expected 0 malformed lines, got %s (memory corruption)", formatCount(recv.MalformedLines)))
	}
	if c.ValidateJSON && recv.InvalidJSONLines > 0 {
		v.Errors = append(v.Errors, fmt.Sprintf("expected 0 invalid-JSON lines, got %s", formatCount(recv.InvalidJSONLines)))
	}

	for _, pattern := range c.RequireStopLog {
		// Validate compiled every pattern when the case was loaded
		if !regexp.MustCompile("(?m)" + pattern).MatchString(stoppedLog) {
			v.Errors = append(v.Errors, fmt.Sprintf("the stopped subject's log has no line matching %q", pattern))
		}
	}

	return v
}

// keepStoppedSubjectLog saves the log of the subject that just stopped next to
// the case's other results, and returns it. Docker drops a container's log with
// the container, and a case that replaces the subject needs that log as
// evidence of what the stop did.
func (r *Runner) keepStoppedSubjectLog(tc *config.TestCase, container string) (string, error) {
	out, err := exec.Command("docker", "logs", "-t", container).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("reading the log of %s: %w: %s", container, err, out)
	}

	dir := filepath.Join(r.opts.ResultsDir, hardwareID(), "restarts", tc.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating %s: %w", dir, err)
	}
	path := filepath.Join(dir, "stopped-subject.log")
	if err := os.WriteFile(path, out, 0o644); err != nil {
		return "", fmt.Errorf("writing %s: %w", path, err)
	}
	fmt.Printf("  the stopped subject's log → %s\n", path)

	return string(out), nil
}
