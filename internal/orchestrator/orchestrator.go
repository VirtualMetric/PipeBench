package orchestrator

import "time"

// Orchestrator abstracts the lifecycle of a test run's infrastructure.
// Docker Compose and Kubernetes each implement this interface.
type Orchestrator interface {
	// Up starts all containers/pods for the test run.
	Up() error

	// UpServices starts only the named compose services (e.g. "subject", "receiver").
	UpServices(services ...string) error

	// StopServices sends SIGTERM to the named services and waits for graceful exit.
	// Used by persistence_restart_correctness to simulate a subject restart.
	StopServices(services ...string) error

	// Down tears down all containers/pods, volumes, and namespaces.
	Down() error

	// WaitForGeneratorExit blocks until the generator finishes or timeout expires.
	WaitForGeneratorExit(timeout time.Duration) error

	// StopCollector signals the collector to flush its CSV and exit.
	StopCollector() error

	// CopyMetricsCSV copies the metrics CSV from the collector to a local path.
	CopyMetricsCSV(dst string) error

	// SubjectContainer returns the name/ID of the subject container or pod.
	SubjectContainer() string

	// ReceiverMetricsPort returns the local port where the receiver's /metrics
	// endpoint is accessible. For Docker this is the host-mapped port. For
	// Kubernetes this starts a port-forward and returns the local port.
	ReceiverMetricsPort() (int, func(), error)

	// Logs returns the last N lines of logs for a named container/pod.
	Logs(name string, lines int) string

	// GeneratorStdout returns the stdout of the generator container (the JSON result).
	GeneratorStdout() string
}
