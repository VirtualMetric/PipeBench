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
ARG DUCKDB_IMAGE=duckdb/duckdb:1.3.2

# --- Go build stage --------------------------------------------------------
FROM golang:1.26.3-alpine@sha256:91eda9776261207ea25fd06b5b7fed8d397dd2c0a283e77f2ab6e91bfa71079d AS gobuilder
WORKDIR /build
COPY containers/verifier/go.mod containers/verifier/go.sum* ./
RUN go mod download 2>/dev/null || true
COPY containers/verifier/*.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /verifier .

# --- Final image: the official DuckDB container -----------------------------
# The official image is distroless (no shell, binary at /duckdb, runs as root),
# so all build steps use exec-form. Bake the extensions against the image's own
# DuckDB binary (network available at build) into a fixed directory DuckDB
# creates on install; runtime then LOADs from it offline.
FROM ${DUCKDB_IMAGE}
RUN ["/duckdb", "-c", "SET extension_directory='/opt/duckdb/extensions'; INSTALL httpfs; INSTALL avro;"]
COPY --from=gobuilder /verifier /verifier
ENV VERIFIER_DUCKDB_BIN=/duckdb \
    VERIFIER_DUCKDB_EXT_DIR=/opt/duckdb/extensions
ENTRYPOINT ["/verifier"]
