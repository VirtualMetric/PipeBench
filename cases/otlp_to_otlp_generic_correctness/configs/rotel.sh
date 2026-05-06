#!/bin/sh
# rotel → OTLP/HTTP head-to-head config (rotel takes no config file —
# every option is a CLI flag or env var, so the bench mounts this
# wrapper script at /entrypoint.sh and execs rotel from it).
#
# Receivers: OTLP/gRPC (4317) + OTLP/HTTP (4318) both on by default.
# We disable metrics + traces because the case sends logs only, and
# the disable flags exercise rotel's per-signal toggle the same way
# vmetric's otlp_metrics_status / otlp_traces_status do.
#
# Exporter: otlp over HTTP, gzip on (rotel's default), targeting the
# bench receiver's /v1/logs endpoint.

set -e

exec /rotel start \
    --otlp-grpc-endpoint 0.0.0.0:4317 \
    --otlp-http-endpoint 0.0.0.0:4318 \
    --otlp-receiver-traces-disabled \
    --otlp-receiver-metrics-disabled \
    --exporter otlp \
    --otlp-exporter-endpoint http://receiver:4318 \
    --otlp-exporter-protocol http \
    --otlp-exporter-compression gzip
