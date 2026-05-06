package main

// OTLP/HTTP receiver mode for the bench. Lets us drive the same
// generator → subject → receiver harness against subjects whose only
// output protocol is OTLP — rotel, otel-collector, vmetric's new
// `otlp` target, vector's `opentelemetry` sink, fluent-bit's
// `opentelemetry` output. They all push OTLP/HTTP at us; we walk
// each LogRecord / Metric / Span and feed it into the existing
// `onLine` counter so the rest of the harness (validate_*, required
// substring, p50/p95/p99 latency, lines_received) just works.
//
// Built on valyala/fasthttp instead of net/http: at high RPS with
// our shape of work (small body, single Content-Type dispatch, lots
// of allocations) fasthttp's recycled *RequestCtx and pooled gzip
// readers are ~2× faster than net/http. The earlier net/http
// version was hitting ~7-8 cores' worth of CPU as a hard ceiling
// before any subject could push past ~700K records/sec; that
// ceiling was the bench harness's wall, not the subject's.
//
// Wire format coverage:
//   - Content-Type: application/x-protobuf  (proto.Unmarshal)
//   - Content-Type: application/json        (protojson.Unmarshal)
//   - Content-Encoding: gzip                (transparent decode)
//
// Per-record granularity:
//   - /v1/logs:    one onLine() per LogRecord, payload = body.stringValue.
//                  Lets `required_substring: "OTEL-"` work the same way
//                  it does for the TCP-line generator.
//   - /v1/metrics: one onLine() per data point. Payload = "metric_name=<name>".
//   - /v1/traces:  one onLine() per Span. Payload = "span_name=<name>".
//
// Counter semantics match the rest of the receiver: lines_received
// increments per LogRecord/data-point/Span (NOT per OTLP request),
// so the harness's lines_sent vs lines_received comparison stays
// meaningful — generator counts records sent, receiver counts
// records received, and a clean run has equal numbers.

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// otlpReceiverUnmarshalOpts mirrors the listener-side option set:
// ignore unknown fields so a future OTLP spec extension a newer
// sender added doesn't fail decode here. Hoisted to a package var
// to dodge the Go composite-literal-in-if parser ambiguity.
var otlpReceiverUnmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// HTTP header names + values we compare against. Pre-rendered as
// []byte once so the per-request path doesn't materialize fresh
// string→[]byte conversions.
var (
	hdrContentType     = []byte("Content-Type")
	hdrContentEncoding = []byte("Content-Encoding")
	valGzip            = []byte("gzip")
	pathLogs           = []byte("/v1/logs")
	pathMetrics        = []byte("/v1/metrics")
	pathTraces         = []byte("/v1/traces")
	contentTypeProto   = []byte("application/x-protobuf")
	contentTypeJSON    = []byte("application/json")
)

// receiveOTLP runs the bench's OTLP/HTTP receive mode using
// fasthttp. Three POST handlers (logs/metrics/traces) on a single
// dispatcher; everything else returns 404. Hands one onLine() call
// per LogRecord / data point / Span to the shared counter so
// existing validation paths keep working.
func receiveOTLP(cfg config, onLine func([]byte)) error {
	handler := buildOTLPHandler(onLine)

	srv := &fasthttp.Server{
		Handler: handler,
		// 64 MiB is roomy for any realistic OTLP batch — the
		// generator's typical 100-record batches land around 5-50
		// KiB compressed, but we don't want to gate batch-size
		// experiments on this cap.
		MaxRequestBodySize:            64 * 1024 * 1024,
		// Don't echo "Server: fasthttp" or "Content-Type: text/plain"
		// — saves a few bytes per response and keeps the receiver
		// looking like a generic OTLP endpoint.
		NoDefaultServerHeader:         true,
		NoDefaultContentType:          true,
		// Lower-case header names on the wire are spec-legal and
		// avoid the case-normalization the default does on every
		// header peek.
		DisableHeaderNamesNormalizing: true,
	}

	fmt.Fprintf(os.Stderr, "receiver: otlp listening on %s (paths: /v1/logs, /v1/metrics, /v1/traces) — fasthttp\n", cfg.Listen)
	return srv.ListenAndServe(cfg.Listen)
}

// buildOTLPHandler dispatches by URL path. Three fixed paths so a
// manual byte-compare beats any router framework — keeps the
// dispatch path zero-allocation.
func buildOTLPHandler(onLine func([]byte)) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		path := ctx.Path()
		switch {
		case bytesEqual(path, pathLogs):
			handleLogs(ctx, onLine)
		case bytesEqual(path, pathMetrics):
			handleMetrics(ctx, onLine)
		case bytesEqual(path, pathTraces):
			handleTraces(ctx, onLine)
		default:
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		}
	}
}

// handleLogs reads + unmarshals the request body, walks every
// LogRecord, and feeds each body string to onLine. fasthttp's
// `BodyGunzip()` uses a pooled flate reader so steady-state
// allocations on the gzip path stay flat instead of churning on
// the per-request `gzip.NewReader` the net/http version did.
func handleLogs(ctx *fasthttp.RequestCtx, onLine func([]byte)) {
	body, contentType, ok := readOTLPBody(ctx)
	if !ok {
		return
	}
	var req collogspb.ExportLogsServiceRequest
	if err := unmarshalOTLPRequest(body, contentType, &req); err != nil {
		ctxError(ctx, err, fasthttp.StatusBadRequest)
		return
	}
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				line := []byte(anyValueAsString(lr.GetBody()))
				onLine(line)
			}
		}
	}
	writeOTLPSuccess(ctx, contentType, &collogspb.ExportLogsServiceResponse{})
}

// handleMetrics walks the metrics envelope and emits one onLine()
// per data point.
func handleMetrics(ctx *fasthttp.RequestCtx, onLine func([]byte)) {
	body, contentType, ok := readOTLPBody(ctx)
	if !ok {
		return
	}
	var req colmetricspb.ExportMetricsServiceRequest
	if err := unmarshalOTLPRequest(body, contentType, &req); err != nil {
		ctxError(ctx, err, fasthttp.StatusBadRequest)
		return
	}
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				line := []byte("metric_name=" + m.GetName())
				countMetricDataPoints(m, line, onLine)
			}
		}
	}
	writeOTLPSuccess(ctx, contentType, &colmetricspb.ExportMetricsServiceResponse{})
}

// countMetricDataPoints fires onLine once per data point of each
// metric type. Per-data-point granularity matters because the
// generator-side rate is in data points/sec — counting at Metric
// granularity would undercount by ~100x for a typical histogram.
func countMetricDataPoints(m *metricspb.Metric, line []byte, onLine func([]byte)) {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for range d.Gauge.GetDataPoints() {
			onLine(line)
		}
	case *metricspb.Metric_Sum:
		for range d.Sum.GetDataPoints() {
			onLine(line)
		}
	case *metricspb.Metric_Histogram:
		for range d.Histogram.GetDataPoints() {
			onLine(line)
		}
	case *metricspb.Metric_ExponentialHistogram:
		for range d.ExponentialHistogram.GetDataPoints() {
			onLine(line)
		}
	case *metricspb.Metric_Summary:
		for range d.Summary.GetDataPoints() {
			onLine(line)
		}
	}
}

// handleTraces walks the spans envelope and emits one onLine() per
// Span.
func handleTraces(ctx *fasthttp.RequestCtx, onLine func([]byte)) {
	body, contentType, ok := readOTLPBody(ctx)
	if !ok {
		return
	}
	var req coltracepb.ExportTraceServiceRequest
	if err := unmarshalOTLPRequest(body, contentType, &req); err != nil {
		ctxError(ctx, err, fasthttp.StatusBadRequest)
		return
	}
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				line := []byte("span_name=" + span.GetName())
				onLine(line)
			}
		}
	}
	writeOTLPSuccess(ctx, contentType, &coltracepb.ExportTraceServiceResponse{})
}

// readOTLPBody validates method + Content-Type, transparently
// decompresses gzip-encoded payloads, and returns the body bytes
// + the cleaned Content-Type. Mirrors the listener-side logic so
// behavior stays symmetric.
//
// Returns (body, contentType, ok=true) on the happy path; on
// failure ok is false and an error response has already been
// written.
//
// The body slice aliases fasthttp's internal request buffer (or
// the gunzip output buffer) — valid for the duration of the
// handler call only. Callers MUST consume synchronously, which
// proto.Unmarshal does.
func readOTLPBody(ctx *fasthttp.RequestCtx) ([]byte, string, bool) {
	if !ctx.IsPost() {
		ctx.Response.Header.Set("Allow", "POST")
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		return nil, "", false
	}

	contentType := stripContentTypeParams(string(ctx.Request.Header.PeekBytes(hdrContentType)))
	if contentType != "application/x-protobuf" && contentType != "application/json" {
		ctx.SetStatusCode(fasthttp.StatusUnsupportedMediaType)
		return nil, "", false
	}

	var body []byte
	var err error
	if bytesEqualFold(ctx.Request.Header.PeekBytes(hdrContentEncoding), valGzip) {
		body, err = ctx.Request.BodyGunzip()
		if err != nil {
			ctxError(ctx, fmt.Errorf("gzip reader: %w", err), fasthttp.StatusBadRequest)
			return nil, "", false
		}
	} else {
		body = ctx.Request.Body()
	}
	return body, contentType, true
}

// unmarshalOTLPRequest dispatches on Content-Type to the right
// unmarshal path. dest must be a non-nil proto.Message pointer
// (typically &collogspb.ExportLogsServiceRequest{} etc.).
func unmarshalOTLPRequest(payload []byte, contentType string, dest proto.Message) error {
	switch contentType {
	case "application/x-protobuf":
		if err := proto.Unmarshal(payload, dest); err != nil {
			return fmt.Errorf("protobuf unmarshal: %w", err)
		}
		return nil
	case "application/json":
		if err := otlpReceiverUnmarshalOpts.Unmarshal(payload, dest); err != nil {
			return fmt.Errorf("json unmarshal: %w", err)
		}
		return nil
	}
	return errors.New("unsupported content type")
}

// writeOTLPSuccess writes the spec-compliant success response —
// the empty signal-specific Service Response message, marshaled
// in the same wire format the request used. fasthttp's response
// writers don't allocate per write; the Response struct is
// recycled with the RequestCtx between requests.
func writeOTLPSuccess(ctx *fasthttp.RequestCtx, contentType string, resp proto.Message) {
	switch contentType {
	case "application/x-protobuf":
		ctx.Response.Header.SetContentTypeBytes(contentTypeProto)
		body, err := proto.Marshal(resp)
		if err != nil {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBody(body)
	default:
		ctx.Response.Header.SetContentTypeBytes(contentTypeJSON)
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBodyString("{}")
	}
}

// ctxError logs the error to stderr and writes the matching status
// code. fasthttp doesn't take an error-string the way net/http's
// http.Error does; if we want body content we'd add it here, but
// status code alone is enough for the bench (no human reads these).
func ctxError(ctx *fasthttp.RequestCtx, err error, code int) {
	fmt.Fprintf(os.Stderr, "receiver: otlp: %v\n", err)
	ctx.SetStatusCode(code)
}

// stripContentTypeParams trims everything after the first ';' so a
// header like "application/json; charset=utf-8" matches a bare
// "application/json" comparison.
func stripContentTypeParams(ct string) string {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.TrimSpace(strings.ToLower(ct))
}

// bytesEqual / bytesEqualFold are tiny inline byte-slice
// comparators. fasthttp's header peek and path return []byte;
// using `bytes.Equal` from the std lib's `bytes` package would
// pull that import in just for these two call sites.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func bytesEqualFold(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// anyValueAsString renders an OTLP AnyValue to its string form.
// Receiver-side counterpart of the listener's anyValueString —
// kept independent so the receiver doesn't need to import
// listener packages.
//
// Strings pass through; primitives format with fmt.Sprint;
// complex types fall through to JSON via protojson. Bytes are
// emitted as hex (lossless and one allocation).
func anyValueAsString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		if x.BoolValue {
			return "true"
		}
		return "false"
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", x.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", x.DoubleValue)
	case *commonpb.AnyValue_BytesValue:
		return fmt.Sprintf("%x", x.BytesValue)
	}
	if b, err := protojson.Marshal(v); err == nil {
		return string(b)
	}
	return ""
}
