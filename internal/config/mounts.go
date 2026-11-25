package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// The containers a case's mounts can name, besides its endpoints.
const (
	MountSubject = "subject"
	MountAgent   = "agent"
)

// mountWritableSuffix, at the end of a mount's host path, makes the mount
// writable. Mounts are read-only otherwise, so that a subject can't change
// the files of the checkout it runs from.
const mountWritableSuffix = ":rw"

// mountVarRE matches one ${NAME} in a mount's host path.
var mountVarRE = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// mountVarNameRE is the form a --mount name must take.
var mountVarNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ResolvedMount is one mount ready for the compose file: Source is an absolute
// host path that exists, and Target is the absolute path in the container.
type ResolvedMount struct {
	Source   string
	Target   string
	Writable bool
}

// ValidMountVarName reports whether name can be used as ${name} in a mount.
func ValidMountVarName(name string) bool {
	return mountVarNameRE.MatchString(name)
}

// splitMountSource returns a mount's host path without its ":rw" suffix, and
// whether the suffix was there.
func splitMountSource(value string) (string, bool) {
	if strings.HasSuffix(value, mountWritableSuffix) {
		return strings.TrimSuffix(value, mountWritableSuffix), true
	}
	return value, false
}

// mountContainers returns the containers this case runs that a mount can name.
func (tc *TestCase) mountContainers() map[string]bool {
	names := map[string]bool{MountSubject: true}
	if tc.Agent != nil {
		names[MountAgent] = true
	}
	for _, e := range tc.Endpoints {
		names[e.Name] = true
	}
	return names
}

// validateMounts checks the mounts: block as far as it can without the run's
// --mount values. A mount that names a container the case doesn't run, or a
// host path the harness can't read, would otherwise leave the container
// without the files the case was written for, and the case would report on
// something it was never meant to test.
func (tc *TestCase) validateMounts() error {
	containers := tc.mountContainers()
	for _, container := range sortedKeys(tc.Mounts) {
		if !containers[container] {
			return fmt.Errorf("case %q: mounts names %q, which is not a container this case runs; use subject, agent (when the case has an agent: block) or an endpoint's name",
				tc.Name, container)
		}
		for _, target := range sortedKeys(tc.Mounts[container]) {
			if !strings.HasPrefix(target, "/") {
				return fmt.Errorf("case %q: mounts.%s: the container path %q must be absolute", tc.Name, container, target)
			}
			if target == "/" || path.Clean(target) != target {
				return fmt.Errorf("case %q: mounts.%s: the container path %q must be a clean path below / (no trailing slash, no . or ..)",
					tc.Name, container, target)
			}
			source, _ := splitMountSource(tc.Mounts[container][target])
			if source == "" {
				return fmt.Errorf("case %q: mounts.%s.%s has no host path", tc.Name, container, target)
			}
			// A "$" that isn't part of a ${NAME} would reach the host path as a
			// literal, and point at a folder nobody meant.
			if strings.Contains(mountVarRE.ReplaceAllString(source, ""), "$") {
				return fmt.Errorf("case %q: mounts.%s.%s: %q has a $ that is not part of a ${NAME}", tc.Name, container, target, source)
			}
		}
	}
	return nil
}

// MountVarNames returns the ${NAME}s the case's mounts use, sorted.
func (tc *TestCase) MountVarNames() []string {
	seen := map[string]bool{}
	for _, targets := range tc.Mounts {
		for _, value := range targets {
			for _, m := range mountVarRE.FindAllStringSubmatch(value, -1) {
				seen[m[1]] = true
			}
		}
	}
	return sortedKeys(seen)
}

// ResolveMounts turns the case's mounts into host paths for one run. It fills
// each ${NAME} from vars (the run's --mount values), resolves a relative path
// against caseDir, and checks that every path exists. It fails rather than
// skip a mount: a container started without the files its case mounts runs
// something other than what the case describes.
//
// The result maps each container to its mounts, ordered by container path.
func (tc *TestCase) ResolveMounts(caseDir string, vars map[string]string) (map[string][]ResolvedMount, error) {
	if len(tc.Mounts) == 0 {
		return nil, nil
	}
	resolved := make(map[string][]ResolvedMount, len(tc.Mounts))
	for _, container := range sortedKeys(tc.Mounts) {
		for _, target := range sortedKeys(tc.Mounts[container]) {
			source, writable := splitMountSource(tc.Mounts[container][target])

			var missing []string
			source = mountVarRE.ReplaceAllStringFunc(source, func(ref string) string {
				name := mountVarRE.FindStringSubmatch(ref)[1]
				value, ok := vars[name]
				if !ok {
					missing = append(missing, name)
				}
				return value
			})
			if len(missing) > 0 {
				return nil, fmt.Errorf("case %q: mounts.%s.%s uses ${%s}; pass --mount %s=<host path>",
					tc.Name, container, target, missing[0], missing[0])
			}

			if !filepath.IsAbs(source) {
				if caseDir == "" {
					return nil, fmt.Errorf("case %q: mounts.%s.%s: %q is relative, but the case's folder is unknown", tc.Name, container, target, source)
				}
				source = filepath.Join(caseDir, source)
			}
			abs, err := filepath.Abs(source)
			if err != nil {
				return nil, fmt.Errorf("case %q: mounts.%s.%s: %w", tc.Name, container, target, err)
			}
			if _, err := os.Stat(abs); err != nil {
				return nil, fmt.Errorf("case %q: mounts.%s.%s: %w", tc.Name, container, target, err)
			}
			resolved[container] = append(resolved[container], ResolvedMount{
				Source:   abs,
				Target:   target,
				Writable: writable,
			})
		}
	}
	return resolved, nil
}

// sortedKeys returns m's keys in order, so that checks and the compose file
// come out the same on every run.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
