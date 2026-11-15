//go:build linux

package main

import (
	"os"
	"syscall"
)

// datasync writes out f's data and the metadata needed to read it back: fdatasync.
func datasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
