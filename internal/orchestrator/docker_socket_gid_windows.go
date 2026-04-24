//go:build windows

package orchestrator

// dockerSocketGID is meaningless on Windows — Docker Desktop doesn't expose a
// real unix-domain socket to host. Returning "" makes the harness skip the
// compose group_add clause, which is the right behaviour: Windows containers
// are accessed via the Docker Desktop credential path instead.
func dockerSocketGID() string { return "" }
