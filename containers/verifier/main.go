// Command verifier is a one-shot correctness oracle for PipeBench's S3
// Avro/Parquet cases. After the generator finishes, it reads the objects a
// subject wrote to the LocalStack S3 bucket DIRECTLY via DuckDB's httpfs
// extension (s3://…), waits until the bucket stops growing, then runs SQL
// assertions over the columnar data and emits a verdict JSON the harness
// reads.
//
// DuckDB is an INDEPENDENT decoder — a different implementation than the
// subject's writer — so a correct row count + no duplicates + no NULL payloads
// is strong evidence the columnar round-trip preserved every record. The
// container bundles the DuckDB CLI with the httpfs + avro extensions
// pre-installed at build time, so it runs fully offline (LOAD only, no
// INSTALL).
//
// The verdict JSON intentionally matches the fields the runner already parses
// from the receiver (lines_received / unique_lines / duplicates / passed /
// errors), so the runner maps it through the existing ReceiverMetrics path
// with no new schema.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// config is the verifier's runtime configuration, loaded from the environment
// the orchestrator injects. Field names mirror the RECEIVER_*/GENERATOR_*
// convention used by the sibling containers.
type config struct {
	DuckDBBin    string // path to the duckdb CLI
	ExtDir       string // DuckDB extension_directory (extensions baked at build time)
	Endpoint     string // S3 endpoint URL, e.g. http://localstack:4566
	Bucket       string // bucket the subject wrote to
	Prefix       string // optional key prefix inside the bucket
	Format       string // "avro" | "parquet"
	AccessKey    string
	SecretKey    string
	Region       string
	Expected     int64 // expected row count (= generator total_lines)
	AllowOverDel bool  // tolerate duplicates (at-least-once delivery)

	// MsgField is the column used for the duplicate check: it must hold a
	// value unique per source record. For the generator's json format that
	// is "msg" (random padding). NullFields must be non-NULL on every row.
	MsgField   string
	NullFields []string

	PollInterval time.Duration
	QuietWindow  time.Duration
	Timeout      time.Duration
	VerdictPath  string
}

// verdict is the JSON document handed back to the runner. The field tags match
// the runner's ReceiverMetrics struct so it deserializes through the existing
// path.
type verdict struct {
	LinesReceived int64    `json:"lines_received"`
	UniqueLines   int64    `json:"unique_lines"`
	Duplicates    int64    `json:"duplicates"`
	Passed        bool     `json:"passed"`
	Errors        []string `json:"errors,omitempty"`
}

// stats holds the aggregate query results used to build a verdict.
type stats struct {
	total    int64
	distinct int64
	nulls    int64
}

func main() {
	cfg := loadConfig()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		// A run-level failure (DuckDB unreachable, bucket never appeared,
		// query error) is itself a failed verdict — write one so the runner
		// always has a file to read, then exit non-zero.
		fmt.Fprintf(os.Stderr, "verifier: %v\n", err)
		v := verdict{Passed: false, Errors: []string{err.Error()}}
		_ = writeVerdict(cfg.VerdictPath, v)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	src := sourceExpr(cfg)
	fmt.Fprintf(os.Stderr, "verifier: format=%s bucket=%q prefix=%q expected=%d source=%s\n",
		cfg.Format, cfg.Bucket, cfg.Prefix, cfg.Expected, src)

	// Phase 1: drain detection. Wait until the row count stops growing across
	// the quiet window (or reaches the expected total), bounded by the context
	// deadline.
	if err := waitStable(ctx, cfg, src); err != nil {
		return err
	}

	// Phase 2: correctness assertions over the settled data.
	st, err := queryStats(ctx, cfg, src)
	if err != nil {
		return fmt.Errorf("stats query: %w", err)
	}

	v := buildVerdict(st, cfg)
	if err := writeVerdict(cfg.VerdictPath, v); err != nil {
		return fmt.Errorf("write verdict: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
	fmt.Fprintf(os.Stderr, "verifier: done. total=%d unique=%d duplicates=%d passed=%v\n",
		v.LinesReceived, v.UniqueLines, v.Duplicates, v.Passed)

	if !v.Passed {
		for _, e := range v.Errors {
			fmt.Fprintf(os.Stderr, "  FAIL: %s\n", e)
		}
		os.Exit(1)
	}
	return nil
}

// buildVerdict turns aggregate stats into a pass/fail verdict. Pure so it is
// unit-testable without DuckDB.
func buildVerdict(st stats, cfg config) verdict {
	duplicates := max(st.total-st.distinct, 0)
	v := verdict{
		LinesReceived: st.total,
		UniqueLines:   st.distinct,
		Duplicates:    duplicates,
		Passed:        true,
	}
	// Count check. Under allow-overdelivery (at-least-once) the invariant is
	// "no loss of UNIQUE rows" — total may legitimately exceed Expected from
	// duplicates — so assert on distinct. Otherwise require an exact total.
	if cfg.Expected > 0 {
		if cfg.AllowOverDel {
			if st.distinct != cfg.Expected {
				loss := 100.0 * (1.0 - float64(st.distinct)/float64(cfg.Expected))
				v.Errors = append(v.Errors, fmt.Sprintf(
					"unique row count mismatch: expected %d, got %d (loss: %.2f%%)",
					cfg.Expected, st.distinct, loss))
				v.Passed = false
			}
		} else if st.total != cfg.Expected {
			loss := 100.0 * (1.0 - float64(st.total)/float64(cfg.Expected))
			v.Errors = append(v.Errors, fmt.Sprintf(
				"row count mismatch: expected %d, got %d (loss: %.2f%%)",
				cfg.Expected, st.total, loss))
			v.Passed = false
		}
	}
	if duplicates > 0 && !cfg.AllowOverDel {
		v.Errors = append(v.Errors, fmt.Sprintf("%d duplicate rows detected", duplicates))
		v.Passed = false
	}
	if st.nulls > 0 {
		v.Errors = append(v.Errors, fmt.Sprintf(
			"%d rows with NULL required field(s) %v — columnar round-trip dropped data",
			st.nulls, cfg.NullFields))
		v.Passed = false
	}
	return v
}

// sourceExpr is the DuckDB table function + glob the queries read from.
func sourceExpr(cfg config) string {
	glob := fmt.Sprintf("s3://%s/%s**/*.%s", cfg.Bucket, cfg.Prefix, cfg.Format)
	reader := "read_parquet"
	if cfg.Format == "avro" {
		reader = "read_avro"
	}
	// sqlQuote escapes the glob for a single-quoted SQL literal. The glob is
	// built from our own config (bucket/prefix/format), never untrusted input.
	return fmt.Sprintf("%s(%s)", reader, sqlQuote(glob))
}

// prelude is the DuckDB session setup: load the bundled extensions and point
// httpfs at the (internal, fixed) S3 endpoint. Returns the SQL and a redacted
// form safe to log (credentials stripped).
func (cfg config) prelude() (sql, redacted string) {
	host, useSSL := splitEndpoint(cfg.Endpoint)
	var b strings.Builder
	if cfg.ExtDir != "" {
		fmt.Fprintf(&b, "SET extension_directory=%s;", sqlQuote(cfg.ExtDir))
	}
	b.WriteString("LOAD httpfs;")
	if cfg.Format == "avro" {
		b.WriteString("LOAD avro;")
	}
	fmt.Fprintf(&b, "SET s3_region=%s;", sqlQuote(cfg.Region))
	fmt.Fprintf(&b, "SET s3_endpoint=%s;", sqlQuote(host))
	fmt.Fprintf(&b, "SET s3_use_ssl=%t;", useSSL)
	b.WriteString("SET s3_url_style='path';")
	redacted = b.String()
	// Credentials appended last so the redacted prefix can be logged as-is.
	fmt.Fprintf(&b, "SET s3_access_key_id=%s;SET s3_secret_access_key=%s;",
		sqlQuote(cfg.AccessKey), sqlQuote(cfg.SecretKey))
	return b.String(), redacted
}

// waitStable polls the row count until it is unchanged across the quiet window
// or reaches the expected total. A query error early on (no objects yet) is
// treated as count 0 and retried — the subject may not have flushed anything.
func waitStable(ctx context.Context, cfg config, src string) error {
	quietPolls := max(int(cfg.QuietWindow/cfg.PollInterval), 1)
	var last int64 = -1
	stable := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "verifier: drain timeout — proceeding with current data\n")
			return nil
		default:
		}

		n, err := queryCount(ctx, cfg, src)
		if err != nil {
			// No files yet / transient emulator error: treat as empty and
			// keep waiting rather than failing the run.
			n = 0
		}
		if n == last {
			stable++
			if n > 0 && stable >= quietPolls {
				fmt.Fprintf(os.Stderr, "verifier: bucket stable at %d rows — drain complete\n", n)
				return nil
			}
		} else {
			stable = 0
		}
		if cfg.Expected > 0 && n >= cfg.Expected {
			fmt.Fprintf(os.Stderr, "verifier: reached expected %d rows\n", cfg.Expected)
			return nil
		}
		last = n
		fmt.Fprintf(os.Stderr, "verifier: rows=%d (stable %d/%d)\n", n, stable, quietPolls)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(cfg.PollInterval):
		}
	}
}

func queryCount(ctx context.Context, cfg config, src string) (int64, error) {
	out, err := runDuckDB(ctx, cfg, fmt.Sprintf("SELECT count(*) AS n FROM %s;", src))
	if err != nil {
		return 0, err
	}
	rows, err := parseRows(out)
	if err != nil || len(rows) == 0 {
		return 0, fmt.Errorf("parse count: %w", err)
	}
	return asInt(rows[0]["n"]), nil
}

func queryStats(ctx context.Context, cfg config, src string) (stats, error) {
	var nullPred strings.Builder
	for i, f := range cfg.NullFields {
		if i > 0 {
			nullPred.WriteString(" OR ")
		}
		fmt.Fprintf(&nullPred, "%s IS NULL", sqlIdent(f))
	}
	if nullPred.Len() == 0 {
		nullPred.WriteString("FALSE")
	}
	q := fmt.Sprintf(
		"SELECT count(*) AS total, count(DISTINCT %s) AS distinct, "+
			"count(*) FILTER (WHERE %s) AS nulls FROM %s;",
		sqlIdent(cfg.MsgField), nullPred.String(), src)
	out, err := runDuckDB(ctx, cfg, q)
	if err != nil {
		return stats{}, err
	}
	rows, err := parseRows(out)
	if err != nil || len(rows) == 0 {
		return stats{}, fmt.Errorf("parse stats: %w", err)
	}
	r := rows[0]
	return stats{
		total:    asInt(r["total"]),
		distinct: asInt(r["distinct"]),
		nulls:    asInt(r["nulls"]),
	}, nil
}

// runDuckDB executes a single statement against the DuckDB CLI with the session
// prelude prepended and JSON output. Uses an explicit argument list (never a
// shell) so neither the SQL nor the credentials can be reinterpreted by a
// shell — see GO-INJECT-002.
func runDuckDB(ctx context.Context, cfg config, query string) ([]byte, error) {
	prelude, redacted := cfg.prelude()
	cmd := exec.CommandContext(ctx, cfg.DuckDBBin, "-json", "-c", prelude+query)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		// Log the redacted prelude + query (no credentials) for debugging.
		return nil, fmt.Errorf("duckdb (%s%s): %w: %s",
			redacted, query, err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// parseRows decodes DuckDB's `-json` output (a JSON array of row objects).
// An empty result set prints "[]". DuckDB emits numbers unquoted, so values
// arrive as json.Number / float64 — asInt normalizes them.
func parseRows(out []byte) ([]map[string]any, error) {
	trimmed := strings.TrimSpace(string(out))
	if trimmed == "" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.UseNumber()
	var rows []map[string]any
	if err := dec.Decode(&rows); err != nil {
		return nil, fmt.Errorf("decode duckdb json: %w", err)
	}
	return rows, nil
}

// asInt coerces a DuckDB JSON scalar (json.Number, string, or float64) to int64.
func asInt(v any) int64 {
	switch n := v.(type) {
	case json.Number:
		i, _ := n.Int64()
		return i
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	default:
		return 0
	}
}

func writeVerdict(path string, v verdict) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// splitEndpoint converts an endpoint URL ("http://localstack:4566") into the
// host:port DuckDB wants plus whether TLS is in use.
func splitEndpoint(endpoint string) (host string, useSSL bool) {
	host = endpoint
	switch {
	case strings.HasPrefix(host, "https://"):
		host = strings.TrimPrefix(host, "https://")
		useSSL = true
	case strings.HasPrefix(host, "http://"):
		host = strings.TrimPrefix(host, "http://")
	}
	return strings.TrimSuffix(host, "/"), useSSL
}

// sqlQuote renders a single-quoted SQL string literal, doubling embedded quotes.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// sqlIdent renders a double-quoted SQL identifier, doubling embedded quotes.
func sqlIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func loadConfig() config {
	cfg := config{
		DuckDBBin:    getEnv("VERIFIER_DUCKDB_BIN", "duckdb"),
		ExtDir:       os.Getenv("VERIFIER_DUCKDB_EXT_DIR"),
		Endpoint:     getEnv("VERIFIER_S3_ENDPOINT", getEnv("AWS_ENDPOINT_URL", "http://localstack:4566")),
		Bucket:       os.Getenv("VERIFIER_S3_BUCKET"),
		Prefix:       os.Getenv("VERIFIER_S3_PREFIX"),
		Format:       getEnv("VERIFIER_OBJECT_FORMAT", "parquet"),
		AccessKey:    getEnv("AWS_ACCESS_KEY_ID", "test"),
		SecretKey:    getEnv("AWS_SECRET_ACCESS_KEY", "test"),
		Region:       getEnv("AWS_REGION", "us-east-1"),
		Expected:     int64(getEnvInt("VERIFIER_EXPECTED_LINES", 0)),
		AllowOverDel: getEnvBool("VERIFIER_ALLOW_OVERDELIVERY", false),
		MsgField:     getEnv("VERIFIER_MSG_FIELD", "msg"),
		NullFields:   splitCSV(getEnv("VERIFIER_NULL_FIELDS", "msg")),
		PollInterval: time.Duration(getEnvInt("VERIFIER_POLL_INTERVAL_MS", 2000)) * time.Millisecond,
		QuietWindow:  getEnvDuration("VERIFIER_QUIET_WINDOW", 15*time.Second),
		Timeout:      getEnvDuration("VERIFIER_TIMEOUT", 5*time.Minute),
		VerdictPath:  getEnv("VERIFIER_VERDICT_PATH", "/results/verdict.json"),
	}
	if cfg.Bucket == "" {
		fmt.Fprintln(os.Stderr, "verifier: VERIFIER_S3_BUCKET is required")
		os.Exit(2)
	}
	if cfg.Format != "avro" && cfg.Format != "parquet" {
		fmt.Fprintf(os.Stderr, "verifier: VERIFIER_OBJECT_FORMAT must be avro|parquet, got %q\n", cfg.Format)
		os.Exit(2)
	}
	// A prefix should end with '/' so the glob nests correctly; tolerate either.
	if cfg.Prefix != "" && !strings.HasSuffix(cfg.Prefix, "/") {
		cfg.Prefix += "/"
	}
	return cfg
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	switch strings.TrimSpace(strings.ToLower(os.Getenv(key))) {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return def
	}
}

func getEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitCSV(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
