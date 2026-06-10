package main

// OTLP/Logs, OTLP/Metrics, and OTLP/Traces over OTLP/gRPC,
// OTLP/HTTP+protobuf, and OTLP/HTTP+JSON.
//
// Signal is picked by GENERATOR_OTLP_SIGNAL (logs|metrics|traces,
// default logs); transport by GENERATOR_OTLP_TRANSPORT. Logs support
// all three transports; metrics and traces are HTTP-only (the cases
// driving them isolate the HTTP ingress path, and the receiver counts
// datapoints/spans identically regardless of transport). The send
// loop, rate limiter, and batch reuse are shared across all three
// signals via the signalBatch interface — only the per-record fill
// (LogRecord body vs metric name vs span name) differs.
//
// Each emitted record carries a per-signal token embedding the global
// seq so the bench receiver's required_substring + hash-dedup checks
// work uniformly: "OTEL-<seq>" in a LogRecord body, "METRIC-<seq>" as
// a metric name (one Gauge datapoint each), "TRACE-<seq>" as a span
// name. One generator "line" maps to one LogRecord / one metric
// datapoint / one span, so the harness's lines-sent vs lines-received
// comparison stays meaningful.
//
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
	"encoding/binary"
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
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
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

// otlpSignal identifies which OTLP signal a run generates.
type otlpSignal int

const (
	signalLogs otlpSignal = iota
	signalMetrics
	signalTraces
)

// runOTLP sends OTLP to the configured target. GENERATOR_OTLP_SIGNAL
// picks the signal (logs|metrics|traces, default logs);
// GENERATOR_OTLP_TRANSPORT picks the transport. Logs may use gRPC or
// HTTP; metrics and traces are HTTP-only (gRPC metric/trace generation
// is intentionally out of scope — requesting it fails fast).
//
// When cfg.Connections > 1, fans out N goroutines, each running an
// independent send loop. Without this fan-out a single OTLP generator
// goroutine pegs at ~1 core (gzip + protobuf marshal + HTTP round-trip
// each per batch is fundamentally serial per goroutine), starving
// multi-core subjects of load. The TCP generator already does the
// same fan-out — we mirror its shape so the bench harness's
// auto-scaled GENERATOR_CONNECTIONS just works.
//
// Returns (records_sent, bytes_sent, err). Records — LogRecords for
// logs, metric datapoints for metrics, spans for traces — is the
// reported unit; see the package-level comment for why.
func runOTLP(cfg config, clock *sendClock) (int64, int64, error) {
	transport := strings.ToLower(getEnv("GENERATOR_OTLP_TRANSPORT", "http_proto"))
	signal := strings.ToLower(getEnv("GENERATOR_OTLP_SIGNAL", "logs"))

	switch signal {
	case "logs":
		return runOTLPSignal(cfg, clock, transport, signalLogs)
	case "metrics":
		if transport == "grpc" {
			return 0, 0, fmt.Errorf("generator: otlp metrics signal is http-only (got transport %q; want http_proto | http_json)", transport)
		}
		return runOTLPSignal(cfg, clock, transport, signalMetrics)
	case "traces":
		if transport == "grpc" {
			return 0, 0, fmt.Errorf("generator: otlp traces signal is http-only (got transport %q; want http_proto | http_json)", transport)
		}
		return runOTLPSignal(cfg, clock, transport, signalTraces)
	default:
		return 0, 0, fmt.Errorf("generator: unknown otlp signal %q (want logs | metrics | traces)", signal)
	}
}

// runOTLPSignal drives one signal end to end, fanning out across
// cfg.Connections workers when > 1.
func runOTLPSignal(cfg config, clock *sendClock, transport string, signal otlpSignal) (int64, int64, error) {
	single := func(wc config) (int64, int64, error) {
		return runOTLPSignalSingle(wc, clock, transport, signal)
	}
	if cfg.Connections <= 1 {
		return single(cfg)
	}
	return runOTLPParallel(cfg, single)
}

// runOTLPSignalSingle dispatches one goroutine to the right transport
// + signal batch. Logs honor grpc/http_proto/http_json; metrics and
// traces are HTTP-only (the caller has already rejected grpc).
func runOTLPSignalSingle(cfg config, clock *sendClock, transport string, signal otlpSignal) (int64, int64, error) {
	switch signal {
	case signalLogs:
		switch transport {
		case "grpc":
			return runOTLPLogsGRPC(cfg, clock)
		case "http_proto", "http_json":
			return runOTLPHTTP(cfg, clock, transport, "/v1/logs",
				newOTLPBatch(buildResource(), buildScope(), otlpRecordsPerBatch))
		default:
			return 0, 0, fmt.Errorf("generator: unknown otlp transport %q (want grpc | http_proto | http_json)", transport)
		}
	case signalMetrics:
		return runOTLPHTTP(cfg, clock, transport, "/v1/metrics",
			newOTLPMetricBatch(buildResource(), buildScope(), otlpRecordsPerBatch))
	case signalTraces:
		return runOTLPHTTP(cfg, clock, transport, "/v1/traces",
			newOTLPTraceBatch(buildResource(), buildScope(), otlpRecordsPerBatch))
	default:
		return 0, 0, fmt.Errorf("generator: unknown otlp signal %d", signal)
	}
}

// runOTLPParallel runs N independent send loops concurrently and
// aggregates their counters. `single` drives one worker given its
// partitioned workerCfg. Each goroutine partitions its TotalLines
// and Duration share evenly (TotalLines/N records per worker; same
// Duration for all). On error the first goroutine's error wins; the
// rest still complete so the partial-batch counters are accurate.
func runOTLPParallel(cfg config, single func(config) (int64, int64, error)) (int64, int64, error) {
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
	// proper hash-dedup validation on OTLP-mode payloads.
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
		go func(id int, wc config) {
			defer wg.Done()
			sent, bytes, err := single(wc)
			totalLines.Add(sent)
			totalBytes.Add(bytes)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
			}
			fmt.Fprintf(os.Stderr, "generator: otlp worker %d done: records=%d bytes=%d\n",
				id, sent, bytes)
		}(i, workerCfg)
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

// runOTLPHTTP drives the OTLP/HTTP path for any signal. variant is
// "http_proto" (application/x-protobuf) or "http_json"
// (application/json). signalPath is the OTLP path appended when the
// target carries no explicit /v1/ path ("/v1/logs", "/v1/metrics",
// "/v1/traces"). batch is the per-worker reusable proto tree the
// drive loop mutates in place. Body is always gzip-compressed —
// that's what otel-loadgen and rotel emit by default and the
// steady-state path the receiver is tuned for.
//
// Per-worker allocation profile: one preallocated proto tree (the
// signalBatch — N records reused across iterations), one reusable
// proto.Marshal output buffer (proto.MarshalOptions.MarshalAppend),
// one bytes.Buffer for the gzip output, one gzip.Writer (Reset
// between batches). Steady state is ~N small string allocations per
// batch (the per-record seq token) plus the http.Request struct that
// net/http requires per call. Everything else stays in place.
func runOTLPHTTP(cfg config, clock *sendClock, variant, signalPath string, batch signalBatch) (int64, int64, error) {
	url := cfg.Target
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	if !strings.Contains(url[7:], "/v1/") {
		url = strings.TrimRight(url, "/") + signalPath
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

	// Per-worker reusable I/O buffers. Sized lazily — first batch
	// grows them to whatever proto.Marshal + gzip emit, after which
	// MarshalAppend / gzip.Reset use the existing capacity.
	msg := batch.protoMsg()
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
			marshalBuf, err = protojsonOpts.MarshalAppend(marshalBuf[:0], msg)
		} else {
			marshalBuf, err = protoOpts.MarshalAppend(marshalBuf[:0], msg)
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

// signalBatch is the per-worker reusable proto tree for one OTLP
// signal. The drive loop owns the iteration; the batch owns how a
// record is laid out and mutated:
//
//   - protoMsg returns the message send() marshals.
//   - prepare trims the batch's repeated field to `count` records so
//     a short final batch (total_lines running out) serializes only
//     that subset.
//   - fillRecord mutates record i in place for sequence `seq` and
//     timestamp `now` — swapping a LogRecord body, a metric name +
//     datapoint, or a span name + ids, without allocating fresh
//     proto structs.
type signalBatch interface {
	protoMsg() proto.Message
	prepare(count int)
	fillRecord(i int, seq int64, now uint64)
}

// otlpDriveLoop is the shared send loop for every OTLP signal and
// transport. It reuses one preallocated proto tree (the signalBatch)
// across every iteration — only the per-record fields change, the
// rest of the proto structs stay in place. Compared with the earlier
// "build a fresh tree per batch" path this drops per-batch
// allocations from ~3*N + scope/resource down to ~N strings, which is
// the dominant cost saver on the generator side.
//
// Rate is enforced at record granularity — same convention as
// runNetflowV5 — so "1000 records/sec" with 100-record batches
// fires 10 batches/sec, paced by a per-record ticker.
func otlpDriveLoop(cfg config, clock *sendClock, batch signalBatch, send func() (int, error)) (int64, int64, error) {
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
		batch.prepare(batchN)

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
			batch.fillRecord(i, seq, now)
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

func (b *otlpBatch) protoMsg() proto.Message { return b.msg }

// prepare trims the proto's ScopeLogs.LogRecords slice to `count` —
// proto.Marshal serializes only that subset.
func (b *otlpBatch) prepare(count int) {
	if count > len(b.records) {
		count = len(b.records)
	}
	b.msg.ResourceLogs[0].ScopeLogs[0].LogRecords = b.records[:count]
}

// fillRecord stamps record i with the batch timestamp and the
// "OTEL-<seq>" body + loadgen.seq attribute. Body and the attribute
// are pre-allocated AnyValue/KeyValue trees — we only swap the
// StringValue field on the leaf, no new struct allocs.
func (b *otlpBatch) fillRecord(i int, seq int64, now uint64) {
	seqStr := strconv.FormatInt(seq, 10)
	rec := b.records[i]
	rec.TimeUnixNano = now
	rec.ObservedTimeUnixNano = now
	rec.Body.Value.(*commonpb.AnyValue_StringValue).StringValue = "OTEL-" + seqStr
	rec.Attributes[0].Value.Value.(*commonpb.AnyValue_StringValue).StringValue = seqStr
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

// otlpMetricBatch is the per-worker reusable proto tree for the
// metrics signal. One ResourceMetrics → one ScopeMetrics → N Metrics,
// each a Gauge holding exactly one NumberDataPoint. The Gauge/
// NumberDataPoint structs are preallocated; fillRecord only swaps the
// metric name and the datapoint value/timestamps.
//
// One Gauge datapoint per Metric, with a unique metric name per seq,
// is deliberate: the OTLP exporter on the subject side re-merges
// metric datapoints *by name* within each (resource, scope) bucket,
// so reusing names would collapse datapoints and undercount. Unique
// "METRIC-<seq>" names keep one datapoint per record end to end, and
// the bench receiver emits one line ("metric_name=METRIC-<seq>") per
// datapoint — so its required_substring "METRIC-" and hash-dedup
// checks line up with the logs path exactly.
type otlpMetricBatch struct {
	msg     *colmetricspb.ExportMetricsServiceRequest
	metrics []*metricspb.Metric
	points  []*metricspb.NumberDataPoint
}

func newOTLPMetricBatch(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, capacity int) *otlpMetricBatch {
	metrics := make([]*metricspb.Metric, capacity)
	points := make([]*metricspb.NumberDataPoint, capacity)
	for i := range metrics {
		dp := &metricspb.NumberDataPoint{
			Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 0},
		}
		points[i] = dp
		metrics[i] = &metricspb.Metric{
			Data: &metricspb.Metric_Gauge{
				Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{dp}},
			},
		}
	}
	return &otlpMetricBatch{
		msg: &colmetricspb.ExportMetricsServiceRequest{
			ResourceMetrics: []*metricspb.ResourceMetrics{{
				Resource: resource,
				ScopeMetrics: []*metricspb.ScopeMetrics{{
					Scope:   scope,
					Metrics: metrics,
				}},
			}},
		},
		metrics: metrics,
		points:  points,
	}
}

func (b *otlpMetricBatch) protoMsg() proto.Message { return b.msg }

func (b *otlpMetricBatch) prepare(count int) {
	if count > len(b.metrics) {
		count = len(b.metrics)
	}
	b.msg.ResourceMetrics[0].ScopeMetrics[0].Metrics = b.metrics[:count]
}

func (b *otlpMetricBatch) fillRecord(i int, seq int64, now uint64) {
	seqStr := strconv.FormatInt(seq, 10)
	b.metrics[i].Name = "METRIC-" + seqStr
	dp := b.points[i]
	dp.StartTimeUnixNano = now
	dp.TimeUnixNano = now
	// Mutate the preallocated wrapper's leaf — no per-record alloc.
	dp.Value.(*metricspb.NumberDataPoint_AsDouble).AsDouble = float64(seq)
}

// otlpTraceBatch is the per-worker reusable proto tree for the traces
// signal. One ResourceSpans → one ScopeSpans → N Spans. trace/span id
// byte slices are preallocated (16 and 8 bytes); fillRecord only
// swaps the span name, timestamps, and id bytes.
//
// Spans pass through the subject's pipeline as-is (no merge step like
// metrics), so the only correctness requirement is a unique name per
// seq — the bench receiver emits one line ("span_name=TRACE-<seq>")
// per span, matching required_substring "TRACE-" and hash-dedup. The
// ids are still populated with valid, unique, non-zero values so the
// span is spec-valid and the subject's decode→re-encode never drops
// it for a malformed/zero id.
type otlpTraceBatch struct {
	msg   *coltracepb.ExportTraceServiceRequest
	spans []*tracepb.Span
}

func newOTLPTraceBatch(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, capacity int) *otlpTraceBatch {
	spans := make([]*tracepb.Span, capacity)
	for i := range spans {
		spans[i] = &tracepb.Span{
			TraceId: make([]byte, 16),
			SpanId:  make([]byte, 8),
			Kind:    tracepb.Span_SPAN_KIND_INTERNAL,
		}
	}
	return &otlpTraceBatch{
		msg: &coltracepb.ExportTraceServiceRequest{
			ResourceSpans: []*tracepb.ResourceSpans{{
				Resource: resource,
				ScopeSpans: []*tracepb.ScopeSpans{{
					Scope: scope,
					Spans: spans,
				}},
			}},
		},
		spans: spans,
	}
}

func (b *otlpTraceBatch) protoMsg() proto.Message { return b.msg }

func (b *otlpTraceBatch) prepare(count int) {
	if count > len(b.spans) {
		count = len(b.spans)
	}
	b.msg.ResourceSpans[0].ScopeSpans[0].Spans = b.spans[:count]
}

func (b *otlpTraceBatch) fillRecord(i int, seq int64, now uint64) {
	span := b.spans[i]
	span.Name = "TRACE-" + strconv.FormatInt(seq, 10)
	span.StartTimeUnixNano = now
	span.EndTimeUnixNano = now
	// Derive a unique, non-zero 16-byte trace id + 8-byte span id from
	// seq. The high 8 trace-id bytes are offset by a constant so even
	// seq 0 yields an all-non-zero 16-byte id; span id is seq+1 so it
	// is never the all-zero (invalid) value either.
	binary.BigEndian.PutUint64(span.TraceId[0:8], uint64(seq))
	binary.BigEndian.PutUint64(span.TraceId[8:16], uint64(seq)+0x9e3779b97f4a7c15)
	binary.BigEndian.PutUint64(span.SpanId, uint64(seq)+1)
}
