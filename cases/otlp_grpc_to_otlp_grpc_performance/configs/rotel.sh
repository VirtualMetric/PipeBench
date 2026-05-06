#!/bin/sh
# rotel → OTLP/gRPC head-to-head config (rotel takes no config file —
# every option is a CLI flag or env var, so the bench mounts this
# wrapper script at /entrypoint.sh and execs rotel from it).
#
# Receivers: OTLP/gRPC (4317) + OTLP/HTTP (4318) both on by default.
# Disable metrics + traces because the case sends logs only.
#
# Exporter: otlp over gRPC, gzip on (rotel's default), targeting the
# bench receiver's gRPC endpoint on 4317.

set -e

exec /rotel start \
    --otlp-grpc-endpoint 0.0.0.0:4317 \
    --otlp-http-endpoint 0.0.0.0:4318 \
    --otlp-receiver-traces-disabled \
    --otlp-receiver-metrics-disabled \
    --exporter otlp \
    --otlp-exporter-endpoint receiver:4317 \
    --otlp-exporter-protocol grpc \
    --otlp-exporter-compression gzip
