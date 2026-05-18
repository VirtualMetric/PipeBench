#!/bin/sh
# rotel → OTLP/gRPC head-to-head config (rotel takes no config file —
# every option is a CLI flag or env var, so the bench mounts this
# wrapper script at /entrypoint.sh and execs rotel from it).
#
# Receivers: OTLP/gRPC (4317) + OTLP/HTTP (4318) both on by default.
# Disable metrics + traces because the case sends logs only.
#
# Exporter: otlp over gRPC, compression disabled — matches the
# otlp/http rotel config in this repo. Rotel v0.2.2's gzip path
# produces payloads downstream OTLP receivers reject; keep parity
# across rotel cases so head-to-head numbers compare on the same
# uncompressed wire format. Targets the bench receiver's gRPC
# endpoint on 4317.

set -e

exec /rotel start \
    --otlp-grpc-endpoint 0.0.0.0:4317 \
    --otlp-http-endpoint 0.0.0.0:4318 \
    --otlp-receiver-traces-disabled \
    --otlp-receiver-metrics-disabled \
    --exporter otlp \
    --otlp-exporter-endpoint receiver:4317 \
    --otlp-exporter-protocol grpc \
    --otlp-exporter-compression none
