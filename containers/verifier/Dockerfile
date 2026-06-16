# syntax=docker/dockerfile:1
#
# bench-verifier — one-shot DuckDB correctness oracle for the S3 Avro/Parquet
# cases. Built ON TOP OF the official DuckDB container image (the duckdb binary
# and runtime come from there — we do not download or embed our own). We layer
# the Go wrapper (poll-until-stable + verdict logic) and PRE-INSTALL the httpfs
# + avro extensions at build time into a fixed directory, so the container runs
# fully offline (the verifier's prelude does LOAD only, never INSTALL).
#
# DUCKDB_IMAGE (the official DuckDB container) is a global ARG so the exact
# tag/digest can be pinned in the Makefile/CI without editing this file.
ARG DUCKDB_IMAGE=duckdb/duckdb:1.5.3@sha256:192ab5c91e012bc8b626772364993739c46b7a9b024e9b9b3adafc5709f480a0

# --- Go build stage --------------------------------------------------------
FROM golang:1.26.4-alpine@sha256:f1ddd9fe14fffc091dd98cb4bfa999f32c5fc77d2f2305ea9f0e2595c5437c14 AS gobuilder
WORKDIR /build
COPY containers/verifier/go.mod containers/verifier/go.sum* ./
RUN go mod download 2>/dev/null || true
COPY containers/verifier/*.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /verifier .

# --- Final image: the official DuckDB container -----------------------------
# The official image is distroless (no shell, no package manager — apk/adduser
# are unavailable), binary at /duckdb, and defaults to root. All build steps use
# exec-form. Bake the extensions against the image's own DuckDB binary (network
# available at build) into a fixed dir DuckDB creates on install (root-owned but
# world-readable); runtime then LOADs from it offline.
FROM ${DUCKDB_IMAGE}
RUN ["/duckdb", "-c", "SET extension_directory='/opt/duckdb/extensions'; INSTALL httpfs; INSTALL avro;"]
COPY --from=gobuilder /verifier /verifier
ENV VERIFIER_DUCKDB_BIN=/duckdb \
    VERIFIER_DUCKDB_EXT_DIR=/opt/duckdb/extensions

# Drop to a non-root uid (matches bench-generator/collector; a numeric UID needs
# no /etc/passwd entry). The DuckDB work — LOAD the baked extensions + count/
# stats over S3 via httpfs — runs fully as non-root: in-memory DB, streaming
# aggregates, no temp spill, and the extension dir is world-readable. The
# harness still pins `user: "0:0"` on the verifier service so its verdict write
# to the bind-mounted /results stays root-owned, so this only hardens the image
# itself for any direct/standalone run.
USER 10001:10001
ENTRYPOINT ["/verifier"]
