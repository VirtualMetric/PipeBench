package main

// OTLP/Logs over OTLP/gRPC, OTLP/HTTP+protobuf, and OTLP/HTTP+JSON.
//
// Patterned after streamfold/otel-loadgen (Apache 2.0) — the upstream
// loadgen only ships a traces variant, but its transport patterns
// (direct protobuf marshaling, ticker-driven batch loop, gzip on the
// HTTP path, gRPC compression option) port cleanly to logs since
// `ResourceSpans → ScopeSpans → Spans` is structurally identical to
// `ResourceLogs → ScopeLogs → LogRecords`.
//
// What's adapted from otel-loadgen:
//   - Direct protobuf marshaling using go.opentelemetry.io/proto/otlp
//     types — no SDK exporter.
//   - Resource preallocation outside the send loop.
//   - HTTP path: Content-Type: application/x-protobuf (or
//     application/json), gzip-encoded body, /v1/logs URL.
//   - gRPC path: insecure dialer, single Export() per batch.
//
// What's specific to this benchmark:
//   - Each emitted LogRecord carries body "OTEL-<seq>" so the bench
//     receiver's required_substring: "OTEL-" check works the same
//     way "10.99." works for NetFlow. Sequence number embedded in
//     the body so it travels through both protobuf and JSON paths
//     unchanged.
//   - Rate is interpreted at LogRecord granularity, not batch
//     granularity, so the harness's lines-sent vs lines-received
//     comparison stays meaningful.
//   - One generator "line" maps to one LogRecord — the receiver
//     should see one TCP line per decoded record.
//
// Reference: https://github.com/streamfold/otel-loadgen

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// otlpRecordsPerBatch is the LogRecord count per ExportLogsServiceRequest
// fired down the wire. 100 matches otel-loadgen's default and lands in
// the typical 1-3 MiB-per-batch sweet spot for both gRPC and HTTP — well
// below the 4 MiB receiver default.
//
// The receiver's required_substring + line-count check works at LogRecord
// granularity (one TCP line per decoded record) so this batch size
// affects throughput pacing only, not correctness.
const otlpRecordsPerBatch = 100

// runOTLPLogs sends OTLP/Logs to the configured target. mode picks the
// transport: "grpc" → OTLP/gRPC on the otlp_target_grpc endpoint;
// "http_proto" / "http_json" → OTLP/HTTP on the otlp_target_http
// endpoint with the matching Content-Type.
//
// When cfg.Connections > 1, fans out N goroutines, each running an
// independent send loop. Without this fan-out a single OTLP generator
// goroutine pegs at ~1 core (gzip + protobuf marshal + HTTP round-trip
// each per batch is fundamentally serial per goroutine), starving
// multi-core subjects of load. The TCP generator already does the
// same fan-out — we mirror its shape so the bench harness's
// auto-scaled GENERATOR_CONNECTIONS just works.
//
// Returns (records_sent, bytes_sent, err). Records — not requests —
// is the reported unit; see the package-level comment for why.
func runOTLPLogs(cfg config, clock *sendClock) (int64, int64, error) {
	transport := strings.ToLower(getEnv("GENERATOR_OTLP_TRANSPORT", "http_proto"))
	if cfg.Connections <= 1 {
		return runOTLPLogsSingle(cfg, clock, transport)
	}
	return runOTLPLogsParallel(cfg, clock, transport)
}

// runOTLPLogsSingle dispatches to the right transport with one
// goroutine driving the send loop.
func runOTLPLogsSingle(cfg config, clock *sendClock, transport string) (int64, int64, error) {
	switch transport {
	case "grpc":
		return runOTLPLogsGRPC(cfg, clock)
	case "http_proto", "http_json":
		return runOTLPLogsHTTP(cfg, clock, transport)
	default:
		return 0, 0, fmt.Errorf("generator: unknown otlp transport %q (want grpc | http_proto | http_json)", transport)
	}
}

// runOTLPLogsParallel runs N independent send loops concurrently and
// aggregates their counters. Each goroutine partitions its TotalLines
// and Duration share evenly (TotalLines/N records per worker; same
// Duration for all). On error the first goroutine's error wins; the
// rest still complete so the partial-batch counters are accurate.
func runOTLPLogsParallel(cfg config, clock *sendClock, transport string) (int64, int64, error) {
	var totalLines, totalBytes atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	// Partition TotalLines exactly. Integer division dropped the
	// remainder before — for total=500 / connections=7 every worker
	// got 71 records, summing to 497 instead of 500 and producing
	// a misleading "lost 3 records" signal in correctness tests.
	// Now: the first `remainder` workers each get one extra record so
	// the sum is exactly TotalLines.
	perWorker := cfg.TotalLines / int64(cfg.Connections)
	remainder := int64(0)
	if cfg.TotalLines > 0 {
		remainder = cfg.TotalLines % int64(cfg.Connections)
		if perWorker == 0 {
			perWorker = 1
		}
	}

	// Track the running offset across workers so SeqOffset is unique
	// per worker AND contiguous over the whole batch — every emitted
	// record gets a globally unique seq, which lets the receiver run
	// proper hash-dedup validation on OTLP-mode bodies.
	var nextOffset int64
	for i := 0; i < cfg.Connections; i++ {
		wg.Add(1)
		workerCfg := cfg
		workerCfg.Connections = 1 // each worker drives a single send loop
		workerCfg.SeqOffset = nextOffset
		if perWorker > 0 {
			workerCfg.TotalLines = perWorker
			if int64(i) < remainder {
				workerCfg.TotalLines++
			}
		}
		nextOffset += workerCfg.TotalLines
		go func(id int) {
			defer wg.Done()
			sent, bytes, err := runOTLPLogsSingle(workerCfg, clock, transport)
			totalLines.Add(sent)
			totalBytes.Add(bytes)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
			}
			fmt.Fprintf(os.Stderr, "generator: otlp worker %d done: records=%d bytes=%d\n",
				id, sent, bytes)
		}(i)
	}

	wg.Wait()
	return totalLines.Load(), totalBytes.Load(), firstErr
}

// runOTLPLogsGRPC drives the OTLP/gRPC path. One persistent gRPC
// connection, one Export per batch, gzip compression on the wire.
//
// The gRPC client handles marshal + gzip internally so we only need
// to reuse the proto tree across batches — no per-worker marshal
// buffer is needed here. Steady-state per-batch allocations are
// dominated by the per-record body string ("OTEL-<seq>"), nothing
// else.
func runOTLPLogsGRPC(cfg config, clock *sendClock) (int64, int64, error) {
	target := cfg.Target

	// Retry the dial briefly so the generator can come up before the
	// subject's gRPC listener is bound. Mirrors dialTCP / runNetflowV5
	// — slow subject startup shouldn't fail the whole test.
	timeout := time.Duration(getEnvInt("GENERATOR_CONNECT_TIMEOUT", 120)) * time.Second
	deadline := time.Now().Add(timeout)

	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.NewClient(target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.UseCompressor(grpcgzip.Name)),
		)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return 0, 0, fmt.Errorf("grpc dial %s after %s: %w", target, timeout, err)
		}
		fmt.Fprintf(os.Stderr, "generator: grpc dial %s: %v (retrying…)\n", target, err)
		time.Sleep(2 * time.Second)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)
	batch := newOTLPBatch(buildResource(), buildScope(), otlpRecordsPerBatch)

	send := func() (int, error) {
		// Per-call ctx with a generous timeout so a wedged subject
		// can't pin the generator forever. 30s matches the
		// otlp-side backpressure timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := client.Export(ctx, batch.msg); err != nil {
			return 0, err
		}
		// Wire-size approximation — proto.Size walks the message
		// once. Acceptable cost since send rate is dominated by
		// the network round-trip.
		return proto.Size(batch.msg), nil
	}

	return otlpDriveLoop(cfg, clock, batch, send)
}

// runOTLPLogsHTTP drives the OTLP/HTTP path. variant is "http_proto"
// (application/x-protobuf) or "http_json" (application/json). Body
// is always gzip-compressed — that's what otel-loadgen and rotel
// emit by default and the steady-state path the receiver is tuned
// for.
//
// Per-worker allocation profile: one preallocated proto tree (the
// otlpBatch — N records reused across iterations), one reusable
// proto.Marshal output buffer (proto.MarshalOptions.MarshalAppend),
// one bytes.Buffer for the gzip output, one gzip.Writer (Reset
// between batches). Steady state is ~N small string allocations per
// batch (the per-record "OTEL-<seq>" body) plus the http.Request
// struct that net/http requires per call. Everything else stays in
// place.
func runOTLPLogsHTTP(cfg config, clock *sendClock, variant string) (int64, int64, error) {
	url := cfg.Target
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	if !strings.Contains(url[7:], "/v1/") {
		url = strings.TrimRight(url, "/") + "/v1/logs"
	}

	contentType := "application/x-protobuf"
	if variant == "http_json" {
		contentType = "application/json"
	}

	// Retry the first dial (server health-check via empty POST) so the
	// generator can wait for the subject's HTTP listener to bind.
	timeout := time.Duration(getEnvInt("GENERATOR_CONNECT_TIMEOUT", 120)) * time.Second
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		_, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return 0, 0, fmt.Errorf("http GET %s after %s: %w", url, timeout, err)
		}
		fmt.Fprintf(os.Stderr, "generator: http probe %s: %v (retrying…)\n", url, err)
		time.Sleep(2 * time.Second)
	}

	// Reusable HTTP client with a generous timeout. 100 idle conns
	// per host matches otel-loadgen's default — keeps the connection
	// pool warm under bursty batch sends.
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	batch := newOTLPBatch(buildResource(), buildScope(), otlpRecordsPerBatch)

	// Per-worker reusable I/O buffers. Sized lazily — first batch
	// grows them to whatever proto.Marshal + gzip emit, after which
	// MarshalAppend / gzip.Reset use the existing capacity.
	var marshalBuf []byte
	gzipBuf := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(gzipBuf)
	protoOpts := proto.MarshalOptions{}
	protojsonOpts := protojson.MarshalOptions{}

	send := func() (int, error) {
		// Marshal into the per-worker reusable buffer.
		// MarshalAppend writes into the slice's underlying array
		// when capacity allows, growing only when needed —
		// steady-state allocs here drop to zero.
		var err error
		if variant == "http_json" {
			marshalBuf, err = protojsonOpts.MarshalAppend(marshalBuf[:0], batch.msg)
		} else {
			marshalBuf, err = protoOpts.MarshalAppend(marshalBuf[:0], batch.msg)
		}
		if err != nil {
			return 0, fmt.Errorf("marshal: %w", err)
		}

		// gzip the body. Reset() on a pooled writer + buffer
		// avoids the per-batch gzip.NewWriter allocation that the
		// previous version did.
		gzipBuf.Reset()
		gzipWriter.Reset(gzipBuf)
		if _, err := gzipWriter.Write(marshalBuf); err != nil {
			return 0, fmt.Errorf("gzip write: %w", err)
		}
		if err := gzipWriter.Close(); err != nil {
			return 0, fmt.Errorf("gzip close: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// bytes.NewReader is one tiny alloc per request — net/http
		// holds a reference until response is read, so we can't
		// reuse a single Reader. Live with it.
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(gzipBuf.Bytes()))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Content-Encoding", "gzip")

		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		// Drain + close so keep-alive can reuse the connection.
		_, _ = io.CopyN(io.Discard, resp.Body, 1024)
		_ = resp.Body.Close()

		if resp.StatusCode >= 400 {
			return 0, fmt.Errorf("http POST %s: status %d", url, resp.StatusCode)
		}
		return gzipBuf.Len(), nil
	}

	return otlpDriveLoop(cfg, clock, batch, send)
}

// otlpDriveLoop is the shared send loop for both gRPC and HTTP. It
// reuses one preallocated proto tree (the otlpBatch) across every
// iteration — only the per-record body string and seq attribute
// change, the rest of the proto structs stay in place. Compared
// with the earlier "build a fresh tree per batch" path this drops
// per-batch allocations from ~3*N + scope/resource down to ~N
// strings, which is the dominant cost saver on the generator side.
//
// Rate is enforced at LogRecord granularity — same convention as
// runNetflowV5 — so "1000 records/sec" with 100-record batches
// fires 10 batches/sec, paced by a per-record ticker.
func otlpDriveLoop(cfg config, clock *sendClock, batch *otlpBatch, send func() (int, error)) (int64, int64, error) {
	var deadline time.Time
	if cfg.Duration > 0 {
		deadline = time.Now().Add(cfg.Duration)
	}

	var rateLimiter <-chan time.Time
	if cfg.Rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(cfg.Rate))
		defer ticker.Stop()
		rateLimiter = ticker.C
	}

	var (
		linesSent int64
		bytesSent int64
		batchSeq  int64
	)

	for {
		// Exit conditions — total_lines target hit OR duration window
		// elapsed.
		if cfg.TotalLines > 0 && linesSent >= cfg.TotalLines {
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}

		// Cap batch size at remaining total_lines.
		batchN := otlpRecordsPerBatch
		if cfg.TotalLines > 0 && linesSent+int64(batchN) > cfg.TotalLines {
			batchN = int(cfg.TotalLines - linesSent)
		}

		// Trim the batch to batchN records (no-op when batchN ==
		// preallocated capacity, which is the common case).
		recs := batch.prepare(batchN)

		// Per-record fill: mutate the existing proto structs
		// rather than allocating fresh ones. Single timestamp for
		// the whole batch — they're emitted within a few
		// microseconds so this is indistinguishable from per-record
		// time.Now() in any downstream histogram.
		now := uint64(time.Now().UnixNano())
		for i := 0; i < batchN; i++ {
			if rateLimiter != nil {
				<-rateLimiter
			}
			seq := cfg.SeqOffset + linesSent + int64(i)
			seqStr := strconv.FormatInt(seq, 10)
			rec := recs[i]
			rec.TimeUnixNano = now
			rec.ObservedTimeUnixNano = now
			// Body and the loadgen.seq attribute are pre-allocated
			// AnyValue/KeyValue trees — we only swap the
			// StringValue field on the leaf, no new struct allocs.
			rec.Body.Value.(*commonpb.AnyValue_StringValue).StringValue = "OTEL-" + seqStr
			rec.Attributes[0].Value.Value.(*commonpb.AnyValue_StringValue).StringValue = seqStr
		}

		n, err := send()
		if err != nil {
			return linesSent, bytesSent, fmt.Errorf("batch %d: %w", batchSeq, err)
		}

		linesSent += int64(batchN)
		bytesSent += int64(n)
		batchSeq++

		// Sample the clock — match runNetflowV5 / sendLines
		// behavior so the harness's reported send_duration is
		// consistent across modes.
		if batchSeq == 1 || batchSeq%10 == 0 {
			clock.RecordSend()
		}
	}

	if linesSent > 0 {
		clock.RecordSend()
	}
	return linesSent, bytesSent, nil
}

// otlpBatch is the per-worker reusable proto tree. One ResourceLogs
// → one ScopeLogs → N preallocated LogRecords. Each LogRecord has a
// preallocated AnyValue body and one preallocated KeyValue attribute
// for "loadgen.seq" — all the proto structs are stable across
// batches so the only per-iteration work is mutating the body /
// seq StringValue strings.
type otlpBatch struct {
	msg     *collogspb.ExportLogsServiceRequest
	records []*logspb.LogRecord
}

// newOTLPBatch builds the reusable tree once per worker. capacity is
// the maximum batch size; prepare() trims to the actual count for
// the last (potentially smaller) batch when total_lines runs out.
func newOTLPBatch(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, capacity int) *otlpBatch {
	records := make([]*logspb.LogRecord, capacity)
	for i := range records {
		records[i] = preallocLogRecord()
	}
	return &otlpBatch{
		msg: &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{{
				Resource: resource,
				ScopeLogs: []*logspb.ScopeLogs{{
					Scope:      scope,
					LogRecords: records,
				}},
			}},
		},
		records: records,
	}
}

// prepare returns a slice of `count` LogRecord pointers ready to be
// mutated in-place. Trims the proto's ScopeLogs.LogRecords slice to
// match — proto.Marshal serializes only that subset.
func (b *otlpBatch) prepare(count int) []*logspb.LogRecord {
	if count > len(b.records) {
		count = len(b.records)
	}
	b.msg.ResourceLogs[0].ScopeLogs[0].LogRecords = b.records[:count]
	return b.records[:count]
}

// preallocLogRecord builds one LogRecord with the proto sub-trees
// the loop will mutate later (Body's AnyValue, the loadgen.seq
// KeyValue). Severity is fixed at INFO — the previous "rotate
// INFO/WARN/ERROR every 3 records" was cosmetic on the listener-
// side log.level distribution and isn't worth the per-record
// branching.
func preallocLogRecord() *logspb.LogRecord {
	return &logspb.LogRecord{
		SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		SeverityText:   "INFO",
		Body: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: ""},
		},
		Attributes: []*commonpb.KeyValue{
			{
				Key: "loadgen.seq",
				Value: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: ""},
				},
			},
		},
	}
}

// buildResource is a once-per-worker resource shape — service.name +
// instance id stamped onto every batch's ResourceLogs. Receivers can
// dedupe on these to confirm the same generator is producing the
// records.
func buildResource() *resourcepb.Resource {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "bench-generator"
	}
	return &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			stringAttr("service.name", "bench-generator"),
			stringAttr("service.instance.id", hostname),
			stringAttr("host.name", hostname),
		},
	}
}

// buildScope is a once-per-worker scope. Constant across every
// batch a worker emits so we build it once and let every
// ResourceLogs reference the same instance.
func buildScope() *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{Name: "bench-generator", Version: "1.0.0"}
}

// stringAttr is a one-liner builder for a string-valued KeyValue —
// the only attribute type the bench generator needs.
func stringAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: k,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: v},
		},
	}
}
