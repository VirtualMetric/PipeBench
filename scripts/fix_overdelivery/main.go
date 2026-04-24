// One-off fixer: scan web/results/<tier>/<subject>.json files and mark any
// correctness result where lines_out > lines_in as failed with an
// over-delivery reason. The runner now catches this going forward; this
// script back-fills historical JSON so the UI reflects the corrected pass/fail.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type entry = map[string]any

type doc struct {
	raw map[string]any
}

func main() {
	root := "web/results"
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	var paths []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".json") {
			return nil
		}
		if filepath.Base(p) == "index.json" {
			return nil
		}
		paths = append(paths, p)
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "walk:", err)
		os.Exit(1)
	}
	sort.Strings(paths)

	var fixes []string
	for _, p := range paths {
		changed, log, err := fixFile(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", p, err)
			continue
		}
		if changed {
			fixes = append(fixes, log...)
		}
	}

	if len(fixes) == 0 {
		fmt.Println("no over-delivery correctness entries found")
		return
	}
	fmt.Printf("fixed %d entries:\n", len(fixes))
	for _, f := range fixes {
		fmt.Println("  " + f)
	}
}

func fixFile(path string) (bool, []string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return false, nil, err
	}
	var d map[string]any
	if err := json.Unmarshal(b, &d); err != nil {
		return false, nil, fmt.Errorf("parse: %w", err)
	}
	results, _ := d["results"].([]any)
	subj, _ := d["subject"].(string)
	rel, _ := filepath.Rel("web/results", path)

	changed := false
	var log []string
	for _, r := range results {
		e, ok := r.(map[string]any)
		if !ok {
			continue
		}
		test, _ := e["test"].(string)
		if !strings.Contains(test, "correctness") {
			continue
		}
		lin := asInt(e["lines_in"])
		lout := asInt(e["lines_out"])
		if lout <= lin {
			continue
		}
		extra := lout - lin
		msg := fmt.Sprintf("over-delivery: received %s lines but only %s were sent (%s extra/duplicate lines)",
			commas(lout), commas(lin), commas(extra))
		existing, _ := e["fail_reason"].(string)
		if !strings.Contains(existing, "over-delivery:") {
			if existing != "" {
				e["fail_reason"] = existing + "; " + msg
			} else {
				e["fail_reason"] = msg
			}
		}
		e["passed"] = false
		changed = true
		log = append(log, fmt.Sprintf("%s  %s/%s  in=%s out=%s (+%s)",
			rel, subj, test, commas(lin), commas(lout), commas(extra)))
	}

	if !changed {
		return false, nil, nil
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(d); err != nil {
		return false, nil, err
	}
	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		return false, nil, err
	}
	return true, log, nil
}

func asInt(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}

func commas(n int64) string {
	s := fmt.Sprintf("%d", n)
	neg := ""
	if strings.HasPrefix(s, "-") {
		neg = "-"
		s = s[1:]
	}
	// Insert commas from the right
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return neg + string(out)
}
