package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// receiveClickHouse measures ClickHouse-sink throughput by polling a row-count
// query over the HTTP interface (:8123). Every subject in the clickhouse case
// writes into the same `bench` database — vmetric over the native TCP protocol
// (:9000), the others via HTTP JSONEachRow or a native exporter — so this
// receiver is deliberately protocol-agnostic: it only counts committed rows.
//
// The receiver also owns table readiness. A performance topology has no init
// container or DDL hook (unlike the correctness ClickHouse driver, which
// creates the table imperatively in Go), so this mode first creates the target
// database + table itself, retrying until clickhouse-server answers, and only
// then starts polling. The generator's warmup delay keeps data from reaching
// the subject until this completes, and every subject retries its sink
// connection regardless — so a not-yet-ready server at startup is harmless.
//
// Each poll records the delta since the previous poll as "received" lines so
// the runner's standard first/last-received EPS window works unchanged — the
// same poll-and-count model the S3/Azure-blob receivers use to drain an object
// store. The default count query sums active MergeTree parts across the whole
// database rather than a single table, so it captures whatever table(s) a
// subject created (bench.logs, otel_logs, …) without the receiver ever needing
// to know the table name a given subject chose.
func receiveClickHouse(cfg config, cnt *counters, val *validator) error {
	endpoint := getEnv("RECEIVER_CH_ENDPOINT", "http://clickhouse:8123")
	user := getEnv("RECEIVER_CH_USER", "bench")
	pass := getEnv("RECEIVER_CH_PASSWORD", "benchpass")
	query := getEnv("RECEIVER_CH_QUERY",
		"SELECT sum(rows) FROM system.parts WHERE database = 'bench' AND active")
	// Rows carry no wire bytes we can observe from a COUNT, so approximate the
	// byte throughput from the generator's line size (default matches the raw
	// TCP cases' 256-byte payload). Only affects the reported MB/s, not EPS.
	bytesPerRow := int64(getEnvInt("RECEIVER_CH_BYTES_PER_ROW", 256))

	client := &http.Client{Timeout: 15 * time.Second}

	// DDL owned by the receiver — see the function doc. Idempotent so a re-run
	// against a persisted volume is a no-op. Ordered: database, then table.
	initStmts := []string{
		getEnv("RECEIVER_CH_INIT_DB", "CREATE DATABASE IF NOT EXISTS bench"),
		getEnv("RECEIVER_CH_INIT_TABLE",
			"CREATE TABLE IF NOT EXISTS bench.logs (message String) ENGINE = MergeTree ORDER BY tuple()"),
	}
	if err := chInit(client, endpoint, user, pass, initStmts); err != nil {
		return err
	}
	// Best-effort: let JSONEachRow inserts from any subject tolerate columns the
	// shared table doesn't declare (a subject that emits {date, log, …} still
	// lands a row instead of failing the whole batch). Failure here is
	// non-fatal — a subject can set the same option client-side instead.
	if stmt := getEnv("RECEIVER_CH_INIT_SETTINGS",
		"ALTER USER "+user+" SETTINGS input_format_skip_unknown_fields = 1"); stmt != "" {
		if err := chExecHTTP(client, endpoint, user, pass, stmt); err != nil {
			fmt.Fprintf(os.Stderr, "receiver: clickhouse settings (non-fatal): %v\n", err)
		}
	}

	shard := cnt.newShard()
	var last int64
	queryURL := endpoint + "/?" + url.Values{"query": {query}}.Encode()

	fmt.Fprintf(os.Stderr, "receiver: polling clickhouse endpoint=%s query=%q\n", endpoint, query)

	return pollLoop("clickhouse", cloudPollInterval(), func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
		if err != nil {
			return err
		}
		// Auth via headers so credentials never appear in the query string
		// (which ClickHouse logs). Works on every 22.x+ server.
		req.Header.Set("X-ClickHouse-User", user)
		req.Header.Set("X-ClickHouse-Key", pass)
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("clickhouse http %d: %.200s", resp.StatusCode, string(body))
		}
		txt := strings.TrimSpace(string(body))
		if txt == "" {
			return nil
		}
		total, err := strconv.ParseInt(txt, 10, 64)
		if err != nil {
			return fmt.Errorf("parse count %q: %w", txt, err)
		}
		// COUNT is monotonic within a run (the volume is fresh each run), so
		// record only the increase. Recording deltas is what feeds the runner's
		// first/last-received EPS window.
		if total > last {
			shard.recordN(total-last, (total-last)*bytesPerRow)
			last = total
		}
		return nil
	})
}

// chInit runs the DDL statements in order, retrying the whole sequence until
// clickhouse-server accepts them or a 2-minute deadline passes. clickhouse-
// server takes a few seconds to open its HTTP port; this is the readiness gate.
func chInit(client *http.Client, endpoint, user, pass string, stmts []string) error {
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = nil
		for _, s := range stmts {
			if err := chExecHTTP(client, endpoint, user, pass, s); err != nil {
				lastErr = err
				break
			}
		}
		if lastErr == nil {
			fmt.Fprintf(os.Stderr, "receiver: clickhouse ready; schema created\n")
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("clickhouse never became ready: %w", lastErr)
}

// chExecHTTP POSTs a single statement to the ClickHouse HTTP interface.
func chExecHTTP(client *http.Client, endpoint, user, pass, sql string) error {
	req, err := http.NewRequest(http.MethodPost, endpoint+"/", strings.NewReader(sql))
	if err != nil {
		return err
	}
	req.Header.Set("X-ClickHouse-User", user)
	req.Header.Set("X-ClickHouse-Key", pass)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d: %.200s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
