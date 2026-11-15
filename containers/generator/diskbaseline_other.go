//go:build !linux

package main

import "os"

// datasync writes out f's data. Outside Linux it is an fsync.
func datasync(f *os.File) error {
	return f.Sync()
}
