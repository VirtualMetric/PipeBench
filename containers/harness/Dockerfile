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

FROM golang:1.26.3-alpine@sha256:91eda9776261207ea25fd06b5b7fed8d397dd2c0a283e77f2ab6e91bfa71079d AS builder

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

# Runtime base is the official Docker CLI image rather than `alpine + apk
# add docker-cli`. The Alpine apk-packaged docker-cli lags upstream
# (Alpine 3.22.4 shipped docker-cli 28.1.1 built with Go 1.24.12, which
# pulled in a stack of stdlib + moby/buildkit CVEs). The Docker-Inc
# official image ships current docker-cli + compose plugin built freshly
# with current Go, and already includes ca-certificates.
FROM docker:29.5.0-cli-alpine3.23@sha256:864f36643a29d212f6cc88ec3aa1ebd2d939283df933c4c5c1074a982237f9bd

# Workdir defaults to /work so default flags `--cases-dir ./cases` and
# `--results-dir ./web/results` resolve against caller-mounted volumes.
WORKDIR /work

COPY --from=builder /harness /usr/local/bin/harness

# Run as root so the docker socket (typically owned by root or a host-side
# docker group not present in this minimal image) is accessible without
# extra group_add gymnastics on the caller's side. The harness itself
# only writes to mounted volumes; it doesn't touch the rootfs.
#
# Overrides the base image's ENTRYPOINT ["docker-entrypoint.sh"]; we
# don't need its DOCKER_HOST/TLS env-var bootstrapping since the harness
# talks to /var/run/docker.sock directly.
ENTRYPOINT ["/usr/local/bin/harness"]
