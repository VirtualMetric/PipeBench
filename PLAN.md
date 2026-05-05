# Vector Test Harness — Containerized Rewrite

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Structure](#2-repository-structure)
3. [Component Specifications](#3-component-specifications)
4. [Test Case Format](#4-test-case-format)
5. [Docker Compose Orchestration](#5-docker-compose-orchestration)
6. [Metrics Schema](#6-metrics-schema)
7. [Implementation Phases](#7-implementation-phases)
8. [Build and Runtime Requirements](#8-build-and-runtime-requirements)
9. [CLI Command Reference](#9-cli-command-reference)

> **Note (2026):** an earlier draft of this plan included a Kubernetes orchestrator and a corresponding implementation phase. That mode shipped, then was removed when the project consolidated on Docker Compose for fairness and maintenance reasons. Sections below have been renumbered; the supported runtime is Docker.

---

## 1. Project Overview

### What We Are Building

A fully containerized, cloud-free rewrite of the [Vector Test Harness](https://github.com/vectordotdev/vector-test-harness) — an end-to-end benchmarking and correctness-validation framework for data pipeline tools.

The original harness required AWS (EC2, S3, Athena, DynamoDB), Terraform 0.12, Ansible over SSH, Packer AMIs, and Debian Buster (EOL). This rewrite replaces every cloud and VM dependency with Docker containers, while preserving the full test matrix, the original 36-column metrics schema, and the comparative results tables.

### Test Matrix

**Performance tests** — measure CPU, memory, disk I/O, network I/O while each subject processes a fixed data volume:

| Slug | Description |
| --- | --- |
| `tcp_to_tcp` | Receive events over TCP, forward over TCP |
| `tcp_to_tcp_5min` | Same as above but 5-minute sustained run |
| `tcp_to_tcp_persistent` | TCP in, TCP out with disk persistence on the forwarding path |
| `file_to_tcp` | Tail a file, forward over TCP |
| `tcp_to_http` | Receive over TCP, POST to HTTP endpoint |
| `tcp_to_http_5min` | Same as above but 5-minute sustained run |
| `tcp_to_blackhole` | Receive over TCP, discard (baseline overhead) |
| `disk_buffer` | Receive over TCP, buffer to disk, forward |
| `regex_mask` | Regex mask applied to every record, forward |
| `syslog_parsing` | Receive over TCP, parse syslog message, forward |
| `set_field` | Receive over TCP, add one field via native transform, forward |
| `real_world_1` | Mixed transforms mimicking a real-world pipeline |

**Correctness tests** — validate data integrity, ordering, deduplication:

| Slug | Description |
| --- | --- |
| `disk_buffer_persistence` | Events survive subject restart with disk buffer |
| `tcp_to_tcp_persistent` | Logs sent while receiver is down are persisted and delivered when it comes up |
| `tcp_to_tcp_persistent_restart` | Same as above, plus the subject is restarted mid-test |
| `tcp_to_http_persistent` | Persistence correctness with an HTTP receiver as the target |
| `file_rotate_create` | New-file log rotation handled without data loss |
| `file_rotate_truncate` | Truncation-based log rotation handled correctly |
| `file_truncate` | Direct file truncation handled correctly |
| `sighup` | Config reload via SIGHUP without data loss |
| `wrapped_json` | JSON-in-string fields parsed and re-emitted correctly |

### Subjects

| Subject | Image | Phase |
| --- | --- | --- |
| VirtualMetric DataStream | `vmetric/director:<version>` | Phase 1 |
| Vector | `timberio/vector:<version>` | Phase 1 |
| Fluent Bit | `fluent/fluent-bit:<version>` | Phase 2 |
| Fluentd | `fluent/fluentd:<version>` | Phase 2 |
| Logstash | `docker.elastic.co/logstash/logstash:<version>` | Phase 2 |
| AxoSyslog | `ghcr.io/axoflow/axosyslog:<version>` | Phase 3 |

### Why the Original Is Broken

- Requires AWS account, EC2 key pairs, S3 buckets, DynamoDB tables, Athena
- Terraform 0.12.x (ancient, provider API changed)
- Ansible over SSH to provision VMs
- Packer AMIs (build pipeline is dead)
- `dstat` profiling tool (deprecated, replaced by `dool`)
- Debian Buster base image (EOL)
- Ruby + bash glue code with outdated gem dependencies

---

## 2. Repository Structure

```text
PipeBench/
├── cmd/
│   └── harness/
│       └── main.go                  # CLI entrypoint (cobra)
├── internal/
│   ├── config/
│   │   ├── case.go                  # TestCase struct + YAML parsing
│   │   └── subject.go               # Subject registry and definitions
│   ├── orchestrator/
│   │   ├── orchestrator.go          # Interface: Orchestrator
│   │   └── docker.go                # Docker Compose orchestration
│   ├── runner/
│   │   └── runner.go                # Test lifecycle: start → wait → collect → teardown
│   ├── metrics/
│   │   ├── collector.go             # Polls Docker stats API or Prometheus
│   │   └── schema.go                # MetricsRow struct, CSV serialization
│   └── results/
│       ├── store.go                 # Write results to disk
│       └── compare.go               # Load + tabulate results
├── containers/
│   ├── generator/
│   │   ├── Dockerfile
│   │   └── main.go                  # TCP/file/HTTP load generator
│   ├── receiver/
│   │   ├── Dockerfile
│   │   └── main.go                  # TCP/file/HTTP receiver + validator
│   └── collector/
│       ├── Dockerfile
│       └── main.go                  # Metrics collector sidecar
├── cases/
│   ├── tcp_to_tcp_performance/
│   │   ├── case.yaml
│   │   └── configs/
│   │       ├── vmetric.yml
│   │       ├── vector.toml
│   │       ├── fluent-bit.conf
│   │       ├── fluentd.conf
│   │       ├── logstash.conf
│   │       └── axosyslog.conf
│   ├── tcp_to_tcp_5min_performance/
│   ├── tcp_to_tcp_persistent_performance/
│   ├── file_to_tcp_performance/
│   ├── tcp_to_http_performance/
│   ├── tcp_to_http_5min_performance/
│   ├── tcp_to_blackhole_performance/
│   ├── disk_buffer_performance/
│   ├── regex_mask_performance/
│   ├── syslog_parsing_performance/
│   ├── set_field_performance/
│   ├── real_world_1_performance/
│   ├── disk_buffer_persistence_correctness/
│   ├── tcp_to_tcp_persistent_correctness/
│   ├── tcp_to_tcp_persistent_restart_correctness/
│   ├── tcp_to_http_persistent_correctness/
│   ├── file_rotate_create_correctness/
│   ├── file_rotate_truncate_correctness/
│   ├── file_truncate_correctness/
│   ├── sighup_correctness/
│   └── wrapped_json_correctness/
├── results/                         # gitignored — local run output
├── Makefile
├── go.mod
├── go.sum
├── PLAN.md
└── README.md
```

---

## 3. Component Specifications

### 3.1 Go CLI (`cmd/harness/main.go`)

Framework: `github.com/spf13/cobra`

#### Key Types (`internal/config/`)

```go
// case.go
type TestCase struct {
    Name        string            `yaml:"name"`
    Type        string            `yaml:"type"`        // "performance" | "correctness"
    Description string            `yaml:"description"`
    Duration    string            `yaml:"duration"`    // e.g. "120s"
    Warmup      string            `yaml:"warmup"`      // e.g. "10s"
    Generator   GeneratorConfig   `yaml:"generator"`
    Receiver    ReceiverConfig    `yaml:"receiver"`
    Subjects    []string          `yaml:"subjects"`
    Configurations map[string]Configuration `yaml:"configurations"`
}

type GeneratorConfig struct {
    Mode      string `yaml:"mode"`       // "tcp" | "file" | "http"
    Target    string `yaml:"target"`     // "subject:9000" or "/data/input.log"
    Rate      int    `yaml:"rate"`       // lines/sec, 0 = unlimited
    LineSize  int    `yaml:"line_size"`  // bytes per line
    Format    string `yaml:"format"`     // "syslog" | "json" | "raw"
}

type ReceiverConfig struct {
    Mode   string `yaml:"mode"`   // "tcp" | "file" | "http"
    Listen string `yaml:"listen"` // ":9001" or "/data/output.log"
}

type Configuration struct {
    Description string            `yaml:"description"`
    Env         map[string]string `yaml:"env"` // extra env vars for subject
}

// subject.go
type Subject struct {
    Name       string
    Image      string
    Version    string
    ConfigPath string   // mount point inside container for config file
    Ports      []string // ports to expose (internal)
    Command    []string // override entrypoint if needed
}

var Registry = map[string]Subject{
    "vector": {
        Name:       "vector",
        Image:      "timberio/vector",
        Version:    "latest-alpine",
        ConfigPath: "/etc/vector/vector.toml",
    },
    "fluent-bit": {
        Name:       "fluent-bit",
        Image:      "fluent/fluent-bit",
        Version:    "latest",
        ConfigPath: "/fluent-bit/etc/fluent-bit.conf",
    },
    "fluentd": {
        Name:       "fluentd",
        Image:      "fluent/fluentd",
        Version:    "latest",
        ConfigPath: "/fluentd/etc/fluent.conf",
    },
    "logstash": {
        Name:       "logstash",
        Image:      "docker.elastic.co/logstash/logstash",
        Version:    "8.13.0",
        ConfigPath: "/usr/share/logstash/pipeline/logstash.conf",
    },
    "axosyslog": {
        Name:       "axosyslog",
        Image:      "ghcr.io/axoflow/axosyslog",
        Version:    "4.24.0",
        ConfigPath: "/etc/syslog-ng/syslog-ng.conf",
    },
    "vmetric": {
        Name:       "vmetric",
        Image:      "vmetric/director",
        Version:    "latest",
        ConfigPath: "/config.yml",
    },
}
```

#### Result Types (`internal/results/`)

```go
type RunResult struct {
    TestName    string        `json:"test_name"`
    Subject     string        `json:"subject"`
    Version     string        `json:"version"`
    Config      string        `json:"config"`
    Timestamp   time.Time     `json:"timestamp"`
    Duration    float64       `json:"duration_secs"`
    LinesIn     int64         `json:"lines_in"`
    LinesOut    int64         `json:"lines_out"`
    BytesIn     int64         `json:"bytes_in"`
    BytesOut    int64         `json:"bytes_out"`
    Passed      *bool         `json:"passed,omitempty"` // nil for perf tests
    MetricsFile string        `json:"metrics_file"`
}
```

### 3.2 Generator Container (`containers/generator/`)

A single Go binary with no external dependencies.

**Environment variables:**

| Variable | Description | Default |
| --- | --- | --- |
| `GENERATOR_MODE` | `tcp`, `file`, `http` | `tcp` |
| `GENERATOR_TARGET` | `host:port` or file path | required |
| `GENERATOR_RATE` | lines/sec, `0` = unlimited | `0` |
| `GENERATOR_DURATION` | e.g. `120s`, `0` = indefinite | `120s` |
| `GENERATOR_LINE_SIZE` | bytes per log line | `256` |
| `GENERATOR_TOTAL_LINES` | stop after N lines (overrides duration) | unset |
| `GENERATOR_FORMAT` | `raw`, `syslog`, `json` | `raw` |
| `GENERATOR_WARMUP` | pause before starting (let subject boot) | `5s` |

**Behavior:**

- TCP mode: dials `GENERATOR_TARGET`, writes `\n`-delimited lines at the configured rate.
- File mode: writes to the file path in `GENERATOR_TARGET`, appending lines.
- HTTP mode: POSTs batches of lines to `GENERATOR_TARGET` URL.
- Exits with code `0` on completion, `1` on fatal error.
- Prints final stats to stdout as JSON: `{"lines_sent": N, "bytes_sent": N, "duration_ms": N}`.

**Dockerfile:**

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY containers/generator/ .
RUN CGO_ENABLED=0 go build -o /generator .

FROM scratch
COPY --from=builder /generator /generator
ENTRYPOINT ["/generator"]
```

### 3.3 Receiver Container (`containers/receiver/`)

**Environment variables:**

| Variable | Description | Default |
| --- | --- | --- |
| `RECEIVER_MODE` | `tcp`, `file`, `http` | `tcp` |
| `RECEIVER_LISTEN` | `host:port` or file path | `:9001` |
| `RECEIVER_EXPECTED_LINES` | for correctness tests: fail if not equal | unset |
| `RECEIVER_VALIDATE_ORDER` | check line ordering (correctness) | `false` |
| `RECEIVER_VALIDATE_DEDUP` | check for duplicates (correctness) | `false` |
| `RECEIVER_TIMEOUT` | max wait after generator done | `30s` |

**HTTP endpoints exposed by receiver:**

- `GET /metrics` — returns JSON `{"lines_received": N, "bytes_received": N, "done": bool}`
- `GET /health` — returns `200 OK`

**Output:** On exit, prints JSON summary to stdout. Exits `0` for pass (or perf test done), `1` for correctness failure.

### 3.4 Collector Container (`containers/collector/`)

Polls the Docker Stats API or reads cgroup v2 data and writes a CSV file in the original dstat schema format.

**Environment variables:**

| Variable | Description | Default |
| --- | --- | --- |
| `COLLECTOR_TARGET_CONTAINER` | container name/ID to monitor | required |
| `COLLECTOR_INTERVAL` | sampling interval | `1s` |
| `COLLECTOR_OUTPUT` | output CSV file path | `/results/metrics.csv` |
| `DOCKER_HOST` | Docker socket path | `unix:///var/run/docker.sock` |

**Docker socket mount:** The collector container requires `/var/run/docker.sock` mounted read-only so it can call the Docker Stats API.

---

## 4. Test Case Format

### `case.yaml` Full Schema

```yaml
name: tcp_to_tcp_performance
type: performance            # "performance" | "correctness"
description: "Measures throughput of a TCP-in to TCP-out pipeline with no transforms"

# How long to run the test (after warmup)
duration: 120s

# Wait for subject to be ready before starting generator
warmup: 10s

generator:
  mode: tcp                  # "tcp" | "file" | "http"
  target: "subject:9000"     # subject is the DNS name of the subject container
  rate: 0                    # lines/sec, 0 = unlimited
  line_size: 256             # bytes per log line
  format: raw                # "raw" | "syslog" | "json"

receiver:
  mode: tcp
  listen: ":9001"

# Which subjects to run this test against
subjects:
  - vmetric
  - vector
  - fluent-bit
  - fluentd
  - logstash
  - axosyslog

# Named configurations to test (each subject must have a config file per config name)
# Config files: cases/<name>/configs/<config_name>/<subject>.<ext>
# If omitted, defaults to a single "default" configuration.
configurations:
  default:
    description: "No transforms, raw passthrough"
  with_regex:
    description: "Regex parsing transform enabled"
    env:                     # additional env vars injected into the subject container
      PARSE_ENABLED: "true"

# Correctness-only fields
correctness:
  validate_order: false
  validate_dedup: false
  expected_loss_pct: 0.0     # allow up to N% line loss before failing
```

### Config File Layout

For a test case with multiple configurations:

```text
cases/tcp_to_tcp_performance/
  case.yaml
  configs/
    default/
      vmetric.yml
      vector.toml
      fluent-bit.conf
      fluentd.conf
      logstash.conf
      axosyslog.conf
    with_regex/
      vector.toml
      fluent-bit.conf
      ...
```

For a test case with a single `default` configuration, configs can be flat:

```text
cases/tcp_to_tcp_performance/
  case.yaml
  configs/
    vector.toml
    fluent-bit.conf
    ...
```

The harness checks for the multi-config layout first, then falls back to flat.

---

## 5. Docker Compose Orchestration

The harness generates a temporary `docker-compose.yaml` in a temp directory and runs `docker compose up` via the Docker Compose CLI (v2). After the test, it runs `docker compose down -v`.

### Generated Compose Structure

```yaml
# Auto-generated by harness — do not edit
version: "3.9"

networks:
  bench:
    driver: bridge

volumes:
  results:
  shared-data:       # for file-based tests

services:

  subject:
    image: "timberio/vector:latest-alpine"
    container_name: "bench-subject"
    networks: [bench]
    volumes:
      - "/tmp/harness-run-XYZ/vector.toml:/etc/vector/vector.toml:ro"
      - "results:/results"
    environment:
      VECTOR_LOG: "warn"
    healthcheck:
      test: ["CMD", "vector", "--version"]
      interval: 2s
      timeout: 5s
      retries: 10
    restart: "no"

  generator:
    image: "vmetric/bench-generator:latest"
    container_name: "bench-generator"
    networks: [bench]
    depends_on:
      subject:
        condition: service_healthy
    environment:
      GENERATOR_MODE: "tcp"
      GENERATOR_TARGET: "subject:9000"
      GENERATOR_RATE: "0"
      GENERATOR_DURATION: "120s"
      GENERATOR_LINE_SIZE: "256"
      GENERATOR_WARMUP: "10s"
    restart: "no"

  receiver:
    image: "vmetric/bench-receiver:latest"
    container_name: "bench-receiver"
    networks: [bench]
    ports:
      - "19001:9001"   # expose metrics endpoint to host
    environment:
      RECEIVER_MODE: "tcp"
      RECEIVER_LISTEN: ":9001"
    restart: "no"

  collector:
    image: "vmetric/bench-collector:latest"
    container_name: "bench-collector"
    networks: [bench]
    volumes:
      - "/var/run/docker.sock:/var/run/docker.sock:ro"
      - "results:/results"
    environment:
      COLLECTOR_TARGET_CONTAINER: "bench-subject"
      COLLECTOR_INTERVAL: "1s"
      COLLECTOR_OUTPUT: "/results/metrics.csv"
    restart: "no"
```

### Test Lifecycle (Docker mode)

```text
harness test -t tcp_to_tcp_performance -s vector
  │
  ├─ 1. Parse case.yaml
  ├─ 2. Resolve subject config file path
  ├─ 3. Generate docker-compose.yaml in temp dir
  ├─ 4. docker compose up -d (all services)
  ├─ 5. Wait for subject health check (up to 60s)
  ├─ 6. Wait for generator to exit (duration + warmup + 30s buffer)
  ├─ 7. Wait for receiver to drain (RECEIVER_TIMEOUT)
  ├─ 8. GET http://localhost:19001/metrics → final line counts
  ├─ 9. docker cp bench-collector:/results/metrics.csv → local results dir
  ├─ 10. docker compose down -v
  ├─ 11. Write summary.json to results/<test>/<config>/<subject>/<version>/<ts>/
  └─ 12. Print per-run summary table
```

---

## 6. Metrics Schema

The collector outputs a CSV matching the original dstat schema as closely as possible. This preserves backward compatibility with existing result analysis tooling.

### CSV Column Definitions

```text
epoch        — Unix timestamp (integer seconds)
cpu_usr      — User CPU % (0-100 per core, not normalized)
cpu_sys      — System CPU %
cpu_idl      — Idle CPU %
cpu_wai      — CPU wait (I/O wait) %
cpu_hiq      — Hardware IRQ %
cpu_siq      — Software IRQ %
mem_used     — Used memory (bytes)
mem_buff     — Buffer memory (bytes)
mem_cach     — Cache memory (bytes)
mem_free     — Free memory (bytes)
net_recv     — Network bytes received per interval
net_send     — Network bytes sent per interval
dsk_read     — Disk bytes read per interval
dsk_writ     — Disk bytes written per interval
load_avg1    — 1-minute load average
load_avg5    — 5-minute load average
load_avg15   — 15-minute load average
procs_run    — Running processes
procs_blk    — Blocked processes
tcp_lis      — TCP sockets listening (0 if unavailable)
tcp_act      — TCP active connections
tcp_syn      — TCP SYN_SENT sockets
tcp_tim      — TCP TIME_WAIT sockets
tcp_clo      — TCP CLOSE_WAIT sockets
```

Columns that cannot be derived from Docker Stats API are recorded as `0`. When using `cgroup` mode (Phase 2), more fields can be populated accurately.

### Go Struct

```go
// internal/metrics/schema.go
type MetricsRow struct {
    Epoch    int64   `csv:"epoch"`
    CpuUsr  float64 `csv:"cpu_usr"`
    CpuSys  float64 `csv:"cpu_sys"`
    CpuIdl  float64 `csv:"cpu_idl"`
    CpuWai  float64 `csv:"cpu_wai"`
    CpuHiq  float64 `csv:"cpu_hiq"`
    CpuSiq  float64 `csv:"cpu_siq"`
    MemUsed int64   `csv:"mem_used"`
    MemBuff int64   `csv:"mem_buff"`
    MemCach int64   `csv:"mem_cach"`
    MemFree int64   `csv:"mem_free"`
    NetRecv int64   `csv:"net_recv"`
    NetSend int64   `csv:"net_send"`
    DskRead int64   `csv:"dsk_read"`
    DskWrit int64   `csv:"dsk_writ"`
    Load1   float64 `csv:"load_avg1"`
    Load5   float64 `csv:"load_avg5"`
    Load15  float64 `csv:"load_avg15"`
    ProcsRun int    `csv:"procs_run"`
    ProcsBlk int    `csv:"procs_blk"`
    TcpLis  int     `csv:"tcp_lis"`
    TcpAct  int     `csv:"tcp_act"`
    TcpSyn  int     `csv:"tcp_syn"`
    TcpTim  int     `csv:"tcp_tim"`
    TcpClo  int     `csv:"tcp_clo"`
}
```

### Results Directory Layout

```text
results/
  tcp_to_tcp_performance/
    default/
      vector/
        0.38.0/
          2024-04-01T143022Z/
            summary.json
            metrics.csv
      fluent-bit/
        3.0.3/
          2024-04-01T143055Z/
            summary.json
            metrics.csv
```

---

## 7. Implementation Phases

### Phase 1 — Working Single-Subject Performance Test (Docker)

**Goal:** `harness test -t tcp_to_tcp -s vector` produces a local results directory with `metrics.csv` and `summary.json`.

Deliverables:

- [ ] `go.mod` with cobra, docker SDK dependencies
- [ ] `internal/config/case.go` — parse `case.yaml`
- [ ] `internal/config/subject.go` — Vector entry in registry
- [ ] `internal/orchestrator/docker.go` — generate + run docker-compose
- [ ] `internal/runner/runner.go` — full test lifecycle
- [ ] `internal/metrics/collector.go` — Docker Stats API polling → CSV
- [ ] `internal/results/store.go` — write `summary.json` + `metrics.csv`
- [ ] `containers/generator/` — TCP mode
- [ ] `containers/receiver/` — TCP mode + `/metrics` endpoint
- [ ] `cases/tcp_to_tcp_performance/` — `case.yaml` + `configs/vector.toml`
- [ ] `cmd/harness/main.go` — `test` and `list` commands
- [ ] `Makefile` — `build`, `build-containers`, `test-local`

### Phase 2 — Multi-Subject + All Performance Tests

**Goal:** `harness compare -t tcp_to_tcp` prints a comparison table across all subjects.

Deliverables:

- [ ] Add Fluent Bit, Fluentd, Logstash, AxoSyslog to subject registry
- [ ] Subject configs for all performance test cases
- [ ] `internal/results/compare.go` — load results and render tabular comparison
- [ ] `harness compare` command
- [ ] Generator: file and HTTP modes
- [ ] Receiver: file and HTTP modes
- [ ] All 8 performance test case directories with all subject configs
- [ ] `harness clean` command

### Phase 3 — Correctness Tests + AxoSyslog

**Goal:** `harness test -t file_rotate_create -s vector` passes/fails with explanation.

Deliverables:

- [ ] Receiver: `RECEIVER_VALIDATE_ORDER`, `RECEIVER_VALIDATE_DEDUP`, line hashing
- [ ] Correctness result type (pass/fail + details)
- [ ] All 7 correctness test case directories
- [ ] Correctness report in `harness compare` output
- [ ] AxoSyslog subject configs
- [ ] `harness test --all-subjects` shorthand

### Phase 4 — Polish, Reporting

**Goal:** Polished CLI and a reproducible reporting flow that produces the in-repo `web/` index.

Deliverables:

- [ ] JSON report output (`harness compare --format json`)
- [ ] HTML report output (`harness compare --format html`)
- [ ] Optional MinIO/S3 result upload (`harness push --bucket <name>`)
- [ ] `README.md` with quick-start guide
- [ ] `harness version` command showing embedded build info

---

## 8. Build and Runtime Requirements

### Build Requirements

| Tool | Version | Purpose |
| --- | --- | --- |
| Go | 1.22+ | CLI and container binaries |
| Docker | 24+ with Compose v2 | Local orchestration |
| Make | any | Build automation |

### Runtime Requirements

- Docker daemon running with access to `/var/run/docker.sock`
- 4+ GB RAM available (for running multiple subjects simultaneously in compare mode)
- Network access to pull images from Docker Hub and Elastic registry
- Disk space: ~500MB for images + results

### No Longer Required

| Removed | Replaced By |
| --- | --- |
| AWS account, EC2, S3, Athena, DynamoDB | Local filesystem + optional MinIO |
| Terraform | None |
| Ansible | None |
| Packer | None |
| SSH keys | None |
| dstat | Go collector reading Docker Stats API |
| Ruby, table_print gem | Go `text/tabwriter` |
| Debian Buster | `scratch` / `alpine` base images |

---

## 9. CLI Command Reference

### `harness test`

Run a single test case against one or more subjects.

```text
harness test -t <test_slug> [-s <subject>] [-c <config>] [flags]

Flags:
  -t, --test string        Test case name (required). Use 'harness list' to see all.
  -s, --subject string     Subject to test. Defaults to all subjects in case.yaml.
  -c, --config string      Configuration name. Defaults to "default".
      --version string     Subject image version tag (default: from subject registry)
      --no-cleanup         Leave containers running after test (debug)
      --results-dir string Directory to write results (default: ./results)
      --timeout duration   Max time to wait for test completion (default: 10m)
```

### `harness compare`

Load and compare results across subjects or time.

```text
harness compare -t <test_slug> [-c <config>] [flags]

Flags:
  -t, --test string     Test case to compare (required)
  -c, --config string   Configuration name (default: "default")
      --format string   Output format: "table", "json", "html" (default: "table")
      --metric string   Primary metric to sort by (default: "cpu_usr")
      --results-dir string  Directory to read results from (default: ./results)
```

### `harness list`

List all available test cases and subjects.

```text
harness list [flags]

Flags:
  --cases     List test cases only
  --subjects  List subjects only
```

### `harness clean`

Remove all bench containers, networks, and temp files.

```text
harness clean [flags]

Flags:
  --all   Also remove result volumes (irreversible)
```

### `harness version`

Print build version information.

```text
harness version
```

---

## Appendix: Key Go Dependencies

```go
// go.mod
require (
    github.com/spf13/cobra v1.8.0
    github.com/docker/docker v26.0.0+incompatible
    github.com/docker/compose/v2 v2.26.0
    gopkg.in/yaml.v3 v3.0.1
    github.com/gocarina/gocsv v0.0.0-20231116093920-b87c2d0e983a
    text/tabwriter  // stdlib
)
```

## Appendix: Makefile Targets

<!-- markdownlint-disable MD010 -->
```makefile
.PHONY: build build-containers test-local clean

build:
	go build -o bin/harness ./cmd/harness

build-containers:
	docker build -t vmetric/bench-generator:latest -f containers/generator/Dockerfile .
	docker build -t vmetric/bench-receiver:latest  -f containers/receiver/Dockerfile  .
	docker build -t vmetric/bench-collector:latest -f containers/collector/Dockerfile .

test-local: build build-containers
	./bin/harness test -t tcp_to_tcp_performance -s vector

compare: build
	./bin/harness compare -t tcp_to_tcp_performance

clean:
	./bin/harness clean
	rm -f bin/harness
```
<!-- markdownlint-enable MD010 -->
