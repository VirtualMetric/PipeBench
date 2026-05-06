package main

// OTLP/gRPC receive side — counterpart to otlp.go's HTTP listener.
// Lets the bench measure subjects whose target is OTLP/gRPC (e.g.
// vmetric's gRPC pass-through path) without having to spin up a
// real OTel collector as the sink.
//
// The gRPC server registers the standard OTLP service stubs from
// go.opentelemetry.io/proto/otlp so the wire format is identical
// to a real OTel collector. Per-record fan-out: one onLine() per
// LogRecord / metric data point / Span, matching otlp.go's
// granularity so the same line counters and substring checks
// keep working across HTTP and gRPC tests.
//
// Concurrency: gRPC uses one goroutine per stream by default. The
// handlers are stateless beyond the onLine callback (which itself
// does atomic counter ops). No additional locking required.

import (
	"context"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// logsGRPCHandler implements collogspb.LogsServiceServer. Each
// service gets its own tiny handler so the Export() method name
// stays unambiguous (one OTLP service per Go type).
type logsGRPCHandler struct {
	collogspb.UnimplementedLogsServiceServer
	onLine func([]byte)
}

func (h *logsGRPCHandler) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				line := []byte(anyValueAsString(lr.GetBody()))
				h.onLine(line)
			}
		}
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

type metricsGRPCHandler struct {
	colmetricspb.UnimplementedMetricsServiceServer
	onLine func([]byte)
}

func (h *metricsGRPCHandler) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				line := []byte("metric_name=" + m.GetName())
				countMetricDataPoints(m, line, h.onLine)
			}
		}
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type tracesGRPCHandler struct {
	coltracepb.UnimplementedTraceServiceServer
	onLine func([]byte)
}

func (h *tracesGRPCHandler) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				line := []byte("span_name=" + span.GetName())
				h.onLine(line)
			}
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// startOTLPGRPCEndpoint listens on the given port and serves OTLP/
// gRPC requests for logs, metrics, and traces. Runs in its own
// goroutine; the caller must keep the program alive (the existing
// OTLP/HTTP listener does that on the main goroutine).
func startOTLPGRPCEndpoint(addr string, onLine func([]byte)) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc listen %s: %w", addr, err)
	}

	// Default gRPC limits cap message size at 4 MiB. Match the
	// HTTP path's 64 MiB ceiling so behavior stays symmetric.
	maxMsg := 64 * 1024 * 1024
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsg),
		grpc.MaxSendMsgSize(maxMsg),
	)

	collogspb.RegisterLogsServiceServer(srv, &logsGRPCHandler{onLine: onLine})
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsGRPCHandler{onLine: onLine})
	coltracepb.RegisterTraceServiceServer(srv, &tracesGRPCHandler{onLine: onLine})

	fmt.Fprintf(os.Stderr, "receiver: otlp grpc listening on %s\n", addr)
	return srv.Serve(ln)
}
