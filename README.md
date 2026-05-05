# PipeBench — Data Pipeline Benchmark

A containerized benchmarking and correctness testing framework for data pipeline tools. Compare **VirtualMetric DataStream**, **Vector**, **Fluent Bit**, **Fluentd**, **Logstash**, and **AxoSyslog** side by side. Everything runs in Docker — clone the repo, build the harness and helper images, and reproduce any published result on the same hardware tier with one command.

## Getting started

PipeBench runs on Docker — local machine or single-node EC2. Follow [README-DOCKER.md](README-DOCKER.md) to install dependencies, build the harness and helper images, and run your first test.

## Project guides

| Guide | What it covers |
| --- | --- |
| [METHODOLOGY.md](METHODOLOGY.md) | How tests are run, what is measured, and how fairness is handled |
| [ADDING-SUBJECTS.md](ADDING-SUBJECTS.md) | How to add a new subject and submit comparable result files |
| [REPRODUCING-RESULTS.md](REPRODUCING-RESULTS.md) | How to reproduce published results locally or on matching AWS hardware |
| [REPORTING-MISTAKES.md](REPORTING-MISTAKES.md) | How to report unfair configs, bad results, broken tests, or documentation errors |

## What this project does

The harness runs a test by spinning up four containers:

1. **Subject** — the tool being tested (Vector, Fluent Bit, etc.) with a test-specific config
2. **Generator** — sends log data to the subject at a controlled rate
3. **Receiver** — captures the subject's output and counts lines/bytes
4. **Collector** — monitors the subject's CPU, memory, network, and disk usage every second

After the test, the result is merged into a single per-(hardware, subject) JSON file at `web/results/<hardware>/<subject>.json`. Re-running the same `(test, config)` replaces the previous row in place — the UI always shows the latest run.

## Available tests

### Performance tests (13)

| Test | What it does |
| --- | --- |
| `tcp_to_tcp_performance` | TCP in, TCP out (raw passthrough baseline) |
| `tcp_to_tcp_5min_performance` | Same as above but 5-minute sustained run |
| `tcp_to_tcp_persistent_performance` | TCP in, TCP out with disk persistence on the forwarding path |
| `file_to_tcp_performance` | Tail a file, forward over TCP |
| `tcp_to_http_performance` | TCP in, HTTP POST out |
| `tcp_to_http_5min_performance` | Same as above but 5-minute sustained run |
| `tcp_to_blackhole_performance` | TCP in, discard output (overhead baseline) |
| `disk_buffer_performance` | TCP in, disk buffer, TCP out |
| `regex_mask_performance` | TCP in, regex mask on every record (e.g. `CONN=\d+` → `CONN=***`), TCP out |
| `syslog_parsing_performance` | TCP in, parse syslog message, TCP out |
| `set_field_performance` | TCP in, add one field via native transform, TCP out |
| `real_world_1_performance` | Parse, filter, and route (mixed pipeline) |

### Correctness tests (8)

| Test | What it checks |
| --- | --- |
| `disk_buffer_persistence_correctness` | Events survive subject restart with disk buffer |
| `tcp_to_tcp_persistent_correctness` | Logs sent while receiver is down are persisted and delivered when it comes up |
| `tcp_to_tcp_persistent_restart_correctness` | Same as above, plus the subject is restarted mid-test |
| `tcp_to_http_persistent_correctness` | Persistence correctness with an HTTP receiver as the target |
| `file_rotate_create_correctness` | New-file log rotation handled without loss |
| `file_rotate_truncate_correctness` | Truncation-based log rotation handled correctly |
| `file_truncate_correctness` | Direct file truncation handled correctly |
| `sighup_correctness` | Config reload via SIGHUP without data loss |
| `wrapped_json_correctness` | JSON-in-string fields parsed correctly |

## Subjects

| Name | Image | Version |
| --- | --- | --- |
| VirtualMetric DataStream | `vmetric/director` | `latest` |
| Vector | `timberio/vector` | `0.54.0-alpine` |
| Fluent Bit | `fluent/fluent-bit` | `5.0` |
| Fluentd | `fluent/fluentd` | `v1.17-debian-1` |
| Logstash | `docker.elastic.co/logstash/logstash` | `8.13.0` |
| AxoSyslog | `ghcr.io/axoflow/axosyslog` | `4.24.0` |

## Why this list?

PipeBench deliberately keeps the subject list short. Every tool here meets the same bar, which is what makes the numbers comparable.

### What every subject must support

- **Disk persistence on the forwarding path.** If the downstream dies, events survive a restart. This rules out tools that only buffer in memory.
- **Basic pipeline primitives.** TCP/HTTP in and out, file tailing, regex parsing and masking, simple routing. Anything less and most cases can't even run.
- **Realistic enterprise use.** Production-grade agents that organizations actually ship to fleets — not single-purpose shippers or experimental collectors.

A tool that can't do these three things isn't in the same category, and benchmarking it here would be misleading.

### Why some well-known tools aren't here

- **Cribl Stream.** The free tier caps throughput, so any performance number would reflect the licence gate, not the engine. Including it would misrepresent the product.
- **Splunk Heavy Forwarder.** Licensing and EULA constraints make publishing head-to-head results awkward at best. We'd rather leave it out than risk misrepresenting Splunk.
- **Filebeat, Telegraf, NXLog, Tenzir, OpenTelemetry Collector, Grafana Alloy, BindPlane Agent.** All capable tools, but each fails at least one bar above (e.g. memory-only buffering, narrow scope, missing transforms) which would make cross-comparison apples-to-oranges.

### Submitting your own results

If you maintain a tool on this list — or want to make the case for adding one — **you can run PipeBench yourself and submit a pull request with the generated `results/` directory**. We'll publish vendor-submitted numbers clearly labelled as such. The harness is fully reproducible (Docker, cases pinned in-repo), so submitted results are auditable against a re-run.

## Project structure

```text
PipeBench/
  cmd/harness/           CLI binary
  internal/              Config, orchestration (Docker Compose), runner, results
  containers/
    generator/           Sends test load (TCP, file, or HTTP)
    receiver/            Receives output, counts lines, validates correctness
    collector/           Polls Docker stats API, writes metrics CSV
    vmetric/             Dockerfile + pre-built binary for the VirtualMetric Director subject
  cases/                 22 test cases, each with per-subject configs
  web/                   Static PipeBench UI (single HTML + per-(hardware, subject) JSON under web/results/)
```

## Credits

PipeBench stands on the shoulders of two prior projects:

- **[Vector Test Harness](https://github.com/vectordotdev/vector-test-harness)** — the original benchmarking framework that defined the test cases, the metrics schema, and the comparative results tables PipeBench inherits. The upstream project is archived, and its AWS + Terraform + Ansible + Packer + Debian Buster (EOL) toolchain is no longer practical to stand up. PipeBench keeps the test matrix and methodology intact while replacing the entire deployment story with Docker Compose: clone, `make build build-containers`, run.
- **[ClickBench](https://github.com/ClickHouse/ClickBench)** — the inspiration for the comparative results UI in [web/](web/). We simplified the layout around standard hardware tiers (one tab per EC2 instance class) and the smaller subject set, but the side-by-side ranking-card style is theirs.
