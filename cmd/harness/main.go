package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/VirtualMetric/PipeBench/internal/config"
	"github.com/VirtualMetric/PipeBench/internal/results"
	"github.com/VirtualMetric/PipeBench/internal/runner"
)

// Set via ldflags at build time.
var (
	buildVersion = "dev"
	buildCommit  = "unknown"
	buildDate    = "unknown"
)

var (
	// Shared flags
	casesDir   string
	resultsDir string

	// test command flags
	testName         string
	subjectName      string
	allSubjects      bool
	allTests         bool
	typeFilter       string
	configName       string
	subjectVersion   string
	noCleanup        bool
	receiverHostPort int
	timeout          time.Duration
	generatorImage   string
	receiverImage    string
	collectorImage   string
	platform         string
	cpuLimit         string
	memLimit         string
	hardware         string
)

func main() {
	root := &cobra.Command{
		Use:   "harness",
		Short: "PipeBench — containerized data pipeline benchmarking",
		Long: `harness runs performance and correctness tests against data pipeline tools
(Vector, Fluent Bit, Fluentd, Logstash, Filebeat, Telegraf) using Docker containers.

No cloud account, Terraform, Ansible, or SSH required.`,
	}

	root.PersistentFlags().StringVar(&casesDir, "cases-dir", "./cases", "directory containing test cases")
	root.PersistentFlags().StringVar(&resultsDir, "results-dir", "./web/results", "directory to write results (lives under web/ so Cloudflare Pages / static hosting can serve it)")

	root.AddCommand(testCmd())
	root.AddCommand(compareCmd())
	root.AddCommand(listCmd())
	root.AddCommand(pushCmd())
	root.AddCommand(cleanCmd())
	root.AddCommand(reportCmd())
	root.AddCommand(serveCmd())
	root.AddCommand(versionCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func testCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Run a test case against one or more subjects",
		Example: `  harness test -t tcp_to_tcp_performance -s vector
  harness test -t tcp_to_tcp_performance -s vector --version 0.40.0
  harness test -t tcp_to_tcp_performance                     # all subjects listed in case.yaml
  harness test -s vmetric --all-tests --hardware c7i.8xlarge # all tests for one subject`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !allTests && testName == "" {
				return fmt.Errorf("--test (-t) is required (or pass --all-tests to iterate every case for one subject)")
			}
			if allTests && subjectName == "" {
				return fmt.Errorf("--all-tests requires -s <subject>; cannot loop every case × every subject")
			}
			if allTests && testName != "" {
				return fmt.Errorf("--all-tests and --test are mutually exclusive")
			}

			// Thread the --hardware flag through to the runner via BENCH_HARDWARE
			// so old callers that only look at the env var still pick it up. The
			// runner reads this to tag each RunResult and pick the output subtree.
			if hardware != "" {
				_ = os.Setenv("BENCH_HARDWARE", hardware)
			}
			hw := os.Getenv("BENCH_HARDWARE")
			if hw == "" {
				hw = "custom"
			}

			// Resolve the list of (test, subject) pairs to execute.
			type runPair struct {
				tc      *config.TestCase
				subject config.Subject
			}
			var pairs []runPair

			if allTests {
				// One subject × every case that lists it in subjects:.
				names, err := config.ListCases(casesDir)
				if err != nil {
					return fmt.Errorf("listing cases: %w", err)
				}
				for _, name := range names {
					tc, err := config.LoadCase(casesDir, name)
					if err != nil {
						fmt.Fprintf(os.Stderr, "skip %s (load error: %v)\n", name, err)
						continue
					}
					if typeFilter != "" && !matchesTypeFilter(tc.Type, typeFilter) {
						continue
					}
					subs, err := resolveSubjects(tc, subjectName)
					if err != nil {
						// subject not listed in this case — skip silently
						continue
					}
					for _, s := range subs {
						pairs = append(pairs, runPair{tc: tc, subject: s})
					}
				}
				if len(pairs) == 0 {
					return fmt.Errorf("subject %q is not listed in any case.yaml under %s", subjectName, casesDir)
				}
				fmt.Printf("--all-tests: %d case(s) will run for subject %s\n", len(pairs), subjectName)
			} else {
				tc, err := config.LoadCase(casesDir, testName)
				if err != nil {
					return err
				}
				var subjects []config.Subject
				if allSubjects {
					for _, s := range config.Registry {
						subjects = append(subjects, s)
					}
				} else {
					subjects, err = resolveSubjects(tc, subjectName)
					if err != nil {
						return err
					}
				}
				for _, s := range subjects {
					pairs = append(pairs, runPair{tc: tc, subject: s})
				}
			}

			// Clean previous results for pairs about to be tested (within
			// this hardware tier only — other tiers' data stays intact).
			cfgName := configName
			if cfgName == "" {
				cfgName = "default"
			}
			cleaned := map[string]bool{}
			for _, p := range pairs {
				key := p.tc.Name + "/" + p.subject.Name
				if cleaned[key] {
					continue
				}
				cleaned[key] = true
				old := filepath.Join(resultsDir, hw, p.tc.Name, cfgName, p.subject.Name)
				if _, err := os.Stat(old); err == nil {
					fmt.Printf("  cleaning old results for %s/%s…\n", p.tc.Name, p.subject.Name)
					os.RemoveAll(old)
				}
			}

			opts := runner.Options{
				CasesDir:         casesDir,
				ResultsDir:       resultsDir,
				GeneratorImage:   generatorImage,
				ReceiverImage:    receiverImage,
				CollectorImage:   collectorImage,
				SubjectVersion:   subjectVersion,
				ConfigName:       configName,
				NoCleanup:        noCleanup,
				ReceiverHostPort: receiverHostPort,
				Timeout:          timeout,
				Platform:         platform,
				CPULimit:         cpuLimit,
				MemLimit:         memLimit,
			}

			r := runner.New(opts)

			var failed []string
			for _, p := range pairs {
				if _, err := r.Run(p.tc, p.subject); err != nil {
					fmt.Fprintf(os.Stderr, "ERROR running %s/%s: %v\n", p.tc.Name, p.subject.Name, err)
					failed = append(failed, p.tc.Name+"/"+p.subject.Name)
				}
			}

			if len(failed) > 0 {
				return fmt.Errorf("%d run(s) failed: %v", len(failed), failed)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&testName, "test", "t", "", "test case name (required unless --all-tests)")
	cmd.Flags().StringVarP(&subjectName, "subject", "s", "", "subject to test (default: all subjects in case.yaml)")
	cmd.Flags().BoolVar(&allSubjects, "all-subjects", false, "run against all registered subjects")
	cmd.Flags().BoolVar(&allTests, "all-tests", false, "run every case where the -s subject appears in case.yaml")
	cmd.Flags().StringVarP(&configName, "config", "c", "default", "configuration name")
	cmd.Flags().StringVar(&subjectVersion, "version", "", "subject image version tag (overrides registry default)")
	cmd.Flags().BoolVar(&noCleanup, "no-cleanup", false, "leave containers running after test (for debugging)")
	cmd.Flags().IntVar(&receiverHostPort, "receiver-port", 19001, "host port for receiver metrics endpoint")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "maximum time to wait for test completion")
	cmd.Flags().StringVar(&generatorImage, "generator-image", "vmetric/bench-generator:latest", "generator container image")
	cmd.Flags().StringVar(&receiverImage, "receiver-image", "vmetric/bench-receiver:latest", "receiver container image")
	cmd.Flags().StringVar(&collectorImage, "collector-image", "vmetric/bench-collector:latest", "collector container image")
	cmd.Flags().StringVar(&platform, "platform", "docker", "platform: docker or kubernetes")
	cmd.Flags().StringVar(&cpuLimit, "cpu-limit", "", "CPU cores for subject container (e.g. \"1\", \"4\", \"0.5\")")
	cmd.Flags().StringVar(&memLimit, "mem-limit", "", "memory limit for subject container (e.g. \"1g\", \"4g\", \"512m\")")
	cmd.Flags().StringVar(&hardware, "hardware", "", "hardware tier label — groups results under results/<hardware>/ (e.g. \"c7i.4xlarge\"); defaults to $BENCH_HARDWARE or \"custom\"")
	cmd.Flags().StringVar(&typeFilter, "type", "", "with --all-tests, run only cases whose type matches (e.g. \"correctness\" also matches \"persistence_correctness\"); \"performance\" matches only the plain performance type")

	return cmd
}

// matchesTypeFilter reports whether a case's type: field satisfies the
// --type filter. "correctness" is treated as a family match — it also
// accepts "persistence_correctness" and "persistence_restart_correctness"
// so users can run every correctness-style test in one command.
// Other filter values require an exact match.
func matchesTypeFilter(caseType, filter string) bool {
	if filter == "correctness" {
		return strings.Contains(caseType, "correctness")
	}
	return caseType == filter
}

func compareCmd() *cobra.Command {
	var (
		cmpTest   string
		cmpConfig string
		cmpFormat string
		cmpSort   string
	)

	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare results across subjects for a test case",
		Example: `  harness compare -t tcp_to_tcp_performance
  harness compare -t tcp_to_tcp_performance --format json
  harness compare -t tcp_to_tcp_performance --sort cpu`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmpTest == "" {
				return fmt.Errorf("--test (-t) is required")
			}
			return results.Compare(results.CompareOptions{
				TestName:   cmpTest,
				ConfigName: cmpConfig,
				ResultsDir: resultsDir,
				Format:     cmpFormat,
				SortMetric: cmpSort,
			})
		},
	}

	cmd.Flags().StringVarP(&cmpTest, "test", "t", "", "test case to compare (required)")
	cmd.Flags().StringVarP(&cmpConfig, "config", "c", "default", "configuration name")
	cmd.Flags().StringVar(&cmpFormat, "format", "table", "output format: table, json, html")
	cmd.Flags().StringVar(&cmpSort, "sort", "throughput", "sort by: throughput, cpu, memory")

	return cmd
}

func listCmd() *cobra.Command {
	var showCases, showSubjects bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available test cases and subjects",
		RunE: func(cmd *cobra.Command, args []string) error {
			showBoth := !showCases && !showSubjects

			if showCases || showBoth {
				cases, err := config.ListCases(casesDir)
				if err != nil {
					return fmt.Errorf("listing cases: %w", err)
				}
				fmt.Println("Test Cases:")
				w := tabwriter.NewWriter(os.Stdout, 2, 8, 2, ' ', 0)
				fmt.Fprintln(w, "  NAME\tTYPE\tDESCRIPTION")
				for _, name := range cases {
					tc, err := config.LoadCase(casesDir, name)
					if err != nil {
						fmt.Fprintf(w, "  %s\t?\t(error loading: %v)\n", name, err)
						continue
					}
					fmt.Fprintf(w, "  %s\t%s\t%s\n", tc.Name, tc.Type, tc.Description)
				}
				w.Flush()
				fmt.Println()
			}

			if showSubjects || showBoth {
				fmt.Println("Subjects:")
				w := tabwriter.NewWriter(os.Stdout, 2, 8, 2, ' ', 0)
				fmt.Fprintln(w, "  NAME\tIMAGE\tVERSION")
				for name, s := range config.Registry {
					fmt.Fprintf(w, "  %s\t%s\t%s\n", name, s.Image, s.Version)
				}
				w.Flush()
			}

			return nil
		},
	}

	cmd.Flags().BoolVar(&showCases, "cases", false, "list test cases only")
	cmd.Flags().BoolVar(&showSubjects, "subjects", false, "list subjects only")
	return cmd
}

func cleanCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clean",
		Short: "Remove all bench containers and networks",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("Removing bench containers…")
			containers := []string{
				"bench-generator",
				"bench-receiver",
				"bench-collector",
			}
			for name := range config.Registry {
				containers = append(containers, "bench-subject-"+name)
			}
			for _, c := range containers {
				_ = exec.Command("docker", "rm", "-f", c).Run()
			}
			fmt.Println("Done.")
			return nil
		},
	}
}

func pushCmd() *cobra.Command {
	var (
		bucket   string
		endpoint string
	)

	cmd := &cobra.Command{
		Use:   "push",
		Short: "Upload results to S3-compatible storage (S3, MinIO, GCS)",
		Example: `  harness push --bucket s3://my-bucket/bench-results
  harness push --bucket s3://my-bucket --endpoint http://minio:9000`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return results.Upload(results.UploadOptions{
				ResultsDir: resultsDir,
				Bucket:     bucket,
				Endpoint:   endpoint,
			})
		},
	}

	cmd.Flags().StringVar(&bucket, "bucket", "", "S3 bucket URI (required), e.g. s3://my-bucket/bench-results")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "S3-compatible endpoint URL (for MinIO, etc.)")

	return cmd
}

func reportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "report",
		Short: "(Re)generate results/index.json by scanning results/<hw>/<subject>.json",
		Long: `report scans <results-dir>/<hardware>/<subject>.json files and writes
<results-dir>/index.json listing hardware tiers, subjects, and tests.

The harness serve command generates this index live on every request, so
you only need to run 'report' explicitly when publishing the results/
directory as static files (e.g. on Cloudflare Pages or GitHub Pages).`,
		Example: `  harness report
  harness report --results-dir ./web/results`,
		RunE: func(cmd *cobra.Command, args []string) error {
			catalog := loadCatalog(casesDir)
			return results.WriteIndex(resultsDir, catalog)
		},
	}
	return cmd
}

// loadCatalog reads cases/*/case.yaml to build the list of known test cases.
// Silently skips unreadable/malformed case files — report should never error
// because some case.yaml is broken.
func loadCatalog(casesDir string) []results.CatalogEntry {
	names, err := config.ListCases(casesDir)
	if err != nil {
		return nil
	}
	out := make([]results.CatalogEntry, 0, len(names))
	for _, n := range names {
		tc, err := config.LoadCase(casesDir, n)
		if err != nil {
			out = append(out, results.CatalogEntry{Name: n})
			continue
		}
		out = append(out, results.CatalogEntry{
			Name:        tc.Name,
			Type:        tc.Type,
			Description: tc.Description,
		})
	}
	return out
}

func serveCmd() *cobra.Command {
	var (
		webDir string
		addr   string
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve the PipeBench UI locally (re-aggregates /data.json on each request)",
		Example: `  harness serve
  harness serve --addr :8080
  harness serve --web ./web --addr 127.0.0.1:8080`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return results.ServeWeb(webDir, resultsDir, addr, func() []results.CatalogEntry {
				return loadCatalog(casesDir)
			})
		},
	}
	cmd.Flags().StringVar(&webDir, "web", "web", "path to the PipeBench UI directory")
	cmd.Flags().StringVar(&addr, "addr", ":18080", "listen address")
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("harness %s (commit: %s, built: %s)\n", buildVersion, buildCommit, buildDate)
		},
	}
}

func resolveSubjects(tc *config.TestCase, subjectFlag string) ([]config.Subject, error) {
	if subjectFlag != "" {
		// Support comma-separated subjects: -s vector,fluent-bit,logstash
		names := strings.Split(subjectFlag, ",")
		var subjects []config.Subject
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			s, err := config.Lookup(name)
			if err != nil {
				return nil, err
			}
			subjects = append(subjects, s)
		}
		return subjects, nil
	}

	var subjects []config.Subject
	for _, name := range tc.Subjects {
		s, err := config.Lookup(name)
		if err != nil {
			return nil, fmt.Errorf("case %q lists unknown subject %q: %w", tc.Name, name, err)
		}
		subjects = append(subjects, s)
	}
	if len(subjects) == 0 {
		return nil, fmt.Errorf("no subjects defined in case.yaml and --subject not specified")
	}
	return subjects, nil
}
