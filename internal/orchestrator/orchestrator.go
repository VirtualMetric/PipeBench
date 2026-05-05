package orchestrator

import "time"

// Orchestrator abstracts the lifecycle of a test run's infrastructure.
// Docker Compose is the only implementation today; the interface stays so
// alternative backends can be plugged in without changing the runner.
type Orchestrator interface {
	// Up starts all containers for the test run.
	Up() error

	// UpServices starts only the named compose services (e.g. "subject", "receiver").
	UpServices(services ...string) error

	// StopServices sends SIGTERM to the named services and waits up to `timeout`
	// for graceful exit before SIGKILL.
	//   - persistence_restart_correctness: 30s timeout (subject must flush state to disk)
	//   - performance tests: short cleanup timeout after the scored receiver
	//     cutoff has already been captured
	StopServices(timeout time.Duration, services ...string) error

	// Down tears down all containers and volumes.
	Down() error

	// WaitForGeneratorExit blocks until the generator finishes or timeout expires.
	WaitForGeneratorExit(timeout time.Duration) error

	// StopCollector signals the collector to flush its CSV and exit.
	StopCollector() error

	// CopyMetricsCSV copies the metrics CSV from the collector to a local path.
	CopyMetricsCSV(dst string) error

	// SubjectContainer returns the name/ID of the subject container.
	SubjectContainer() string

	// ReceiverMetricsPort returns the local port where the receiver's /metrics
	// endpoint is accessible (the host-mapped port for Docker).
	ReceiverMetricsPort() (int, func(), error)

	// Logs returns the last N lines of logs for a named container.
	Logs(name string, lines int) string

	// GeneratorStdout returns the stdout of the generator container (the JSON result).
	GeneratorStdout() string
}
