package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

// receivePostgres is RECEIVER_MODE=postgres: it reads a table that the subject
// writes to, and counts the lines that the table holds now (snapshot.go). So a
// row that the table holds twice counts as a duplicate.
//
// At its start it runs RECEIVER_PG_INIT, when set, until the server answers:
// the statement that makes the table, as the subject's target needs the table
// to be there. Then each poll runs RECEIVER_PG_QUERY, which answers one text
// column. The line is found anywhere in the value, so a subject that writes
// the record as JSON is read as well.
//
// A row keeps the time it was first seen, by a hash of its text, so the
// timeline and latency show when each line first arrived.
func receivePostgres(cfg config, cnt *counters, _ *validator) error {
	dsn := getEnv("RECEIVER_PG_DSN", "postgres://bench:bench@postgres:5432/bench?sslmode=disable")
	query := getEnv("RECEIVER_PG_QUERY", "SELECT doc FROM events")
	initSQL := os.Getenv("RECEIVER_PG_INIT")
	bytesPerLine := int64(getEnvInt("RECEIVER_PG_BYTES_PER_ROW", 256))
	snap := newSnapshotTracker(seqTrack, cfg.LatencySampleEvery, cnt.newShard())
	seen := map[uint64]int64{}
	interval := cloudPollInterval()

	var conn *pgx.Conn
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if conn == nil || conn.IsClosed() {
			c, err := pgx.Connect(ctx, dsn)
			if err != nil {
				cancel()
				fmt.Fprintf(os.Stderr, "receiver: postgres: connect: %v\n", err)
				time.Sleep(interval)
				continue
			}
			conn = c
			if initSQL != "" {
				if _, err := conn.Exec(ctx, initSQL); err != nil {
					fmt.Fprintf(os.Stderr, "receiver: postgres: init: %v\n", err)
					_ = conn.Close(ctx)
					conn = nil
					cancel()
					time.Sleep(interval)
					continue
				}
			}
			fmt.Fprintf(os.Stderr, "receiver: postgres: reading %q\n", query)
		}
		parts, err := readPostgres(ctx, conn, query, seen)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "receiver: postgres: %v\n", err)
			_ = conn.Close(context.Background())
			conn = nil
		} else {
			snap.rebuild(parts, bytesPerLine)
		}
		time.Sleep(interval)
	}
}

// readPostgres runs the query and answers its rows, each as a part of its own
// with the time its text was first seen.
func readPostgres(ctx context.Context, conn *pgx.Conn, query string, seen map[uint64]int64) ([]snapshotPart, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now().UnixNano()
	var parts []snapshotPart
	for rows.Next() {
		var text *string
		if err := rows.Scan(&text); err != nil {
			return nil, err
		}
		if text == nil {
			continue
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(*text))
		k := h.Sum64()
		at, ok := seen[k]
		if !ok {
			at = now
			seen[k] = at
		}
		parts = append(parts, snapshotPart{seen: at, lines: [][]byte{[]byte(*text)}})
	}
	return parts, rows.Err()
}
