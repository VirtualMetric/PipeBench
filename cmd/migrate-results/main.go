// One-time migration tool: reads the OLD deep layout
//   results/<hw>/<test>/<config>/<subject>/<version>/<ts>/summary.json
// and writes the NEW subject-file layout
//   results/<hw>/<subject>.json
// merged in place, then removes the old tree.
//
// Run once after upgrading to the subject-file Store. Safe to rerun; idempotent.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/VirtualMetric/PipeBench/internal/results"
)

func main() {
	dir := flag.String("dir", "./results", "results directory to migrate")
	flag.Parse()

	store := results.NewStore(*dir)

	var oldFiles []string
	err := filepath.Walk(*dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Base(path) != "summary.json" {
			return nil
		}
		oldFiles = append(oldFiles, path)
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "walk:", err)
		os.Exit(1)
	}

	migrated := 0
	for _, f := range oldFiles {
		data, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, "read", f, err)
			continue
		}
		var r results.RunResult
		if err := json.Unmarshal(data, &r); err != nil {
			fmt.Fprintln(os.Stderr, "unmarshal", f, err)
			continue
		}
		if _, err := store.Save(r, ""); err != nil {
			fmt.Fprintln(os.Stderr, "save", f, err)
			continue
		}
		migrated++
	}
	fmt.Printf("migrated %d summary.json files → subject-file layout\n", migrated)

	// Remove old deep-layout directories. Anything under <dir>/<hw>/ that
	// is a directory (not a <subject>.json file) is old deep-layout data.
	hwEntries, err := os.ReadDir(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "readdir:", err)
		os.Exit(1)
	}
	removed := 0
	for _, hwEnt := range hwEntries {
		if !hwEnt.IsDir() {
			continue
		}
		hwDir := filepath.Join(*dir, hwEnt.Name())
		inner, err := os.ReadDir(hwDir)
		if err != nil {
			continue
		}
		for _, e := range inner {
			if e.IsDir() {
				p := filepath.Join(hwDir, e.Name())
				if err := os.RemoveAll(p); err != nil {
					fmt.Fprintln(os.Stderr, "rm", p, err)
					continue
				}
				removed++
			}
		}
	}
	fmt.Printf("removed %d old deep-layout directories\n", removed)
}
