//go:build unix

package orchestrator

import (
	"os"
	"strconv"
	"syscall"
)

func dockerSocketGID() string {
	fi, err := os.Stat("/var/run/docker.sock")
	if err != nil {
		return ""
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return strconv.FormatUint(uint64(st.Gid), 10)
}
