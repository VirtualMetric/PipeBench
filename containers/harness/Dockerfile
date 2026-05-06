# PipeBench harness CLI image — packages the orchestration tool that
# manages the bench-generator / bench-receiver / bench-collector
# containers around a subject under test.
#
# This image is a sibling to the bench-* images, but the harness has a
# different runtime shape: it shells out to `docker compose` to drive
# the test lifecycle, so the runtime image must ship a docker CLI and
# the container needs the host's docker socket mounted at runtime
# (`-v /var/run/docker.sock:/var/run/docker.sock`).
#
# Typical invocation from a downstream repo (e.g. virtualmetric-bench):
#   docker run --rm \
#     -v $(pwd)/cases:/work/cases \
#     -v $(pwd)/web/results:/work/web/results \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     vmetric/pipebench:latest \
#     test -t tcp_to_tcp_performance -s vmetric

FROM golang:1.26.2-alpine@sha256:f85330846cde1e57ca9ec309382da3b8e6ae3ab943d2739500e08c86393a21b1 AS builder

WORKDIR /build

# Copy the top-level module: harness binary + all its internal deps.
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILDDATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w \
      -X main.buildVersion=${VERSION} \
      -X main.buildCommit=${COMMIT} \
      -X main.buildDate=${BUILDDATE}" \
    -o /harness ./cmd/harness

FROM alpine:3.22.4@sha256:310c62b5e7ca5b08167e4384c68db0fd2905dd9c7493756d356e893909057601

# docker-cli ships the `docker` binary plus `docker compose` v2 plugin.
# The harness invokes `docker compose -f compose.yaml up -d` and friends,
# so both pieces are required at runtime. ca-certificates is needed for
# the docker daemon's TLS verification when pulling subject images.
RUN apk add --no-cache ca-certificates docker-cli docker-cli-compose

# Workdir defaults to /work so default flags `--cases-dir ./cases` and
# `--results-dir ./web/results` resolve against caller-mounted volumes.
WORKDIR /work

COPY --from=builder /harness /usr/local/bin/harness

# Run as root so the docker socket (typically owned by root or a host-side
# docker group not present in this minimal image) is accessible without
# extra group_add gymnastics on the caller's side. The harness itself
# only writes to mounted volumes; it doesn't touch the rootfs.
ENTRYPOINT ["/usr/local/bin/harness"]
