# PipeBench — Data Pipeline Benchmark

A containerized benchmarking and correctness testing framework for data pipeline tools. Compare **VirtualMetric Director**, **Vector**, **Fluent Bit**, **Fluentd**, **Logstash**, **Filebeat**, **Cribl Stream**, **Telegraf**, **Splunk HF**, **NXLog**, **AxoSyslog**, **Tenzir**, **OpenTelemetry Collector**, **Grafana Alloy**, and **BindPlane Agent** side by side — no cloud account, Terraform, or Ansible required.

## Choose your platform

| Platform | Guide | When to use |
|---|---|---|
| **Docker** | [README-DOCKER.md](README-DOCKER.md) | Local machine, single-node testing, getting started |
| **Kubernetes** | [README-KUBERNETES.md](README-KUBERNETES.md) | Cluster-based benchmarking, fair resource isolation |
| **GitHub Actions** | [README-CI.md](README-CI.md) | Automated CI/CD benchmarks, scheduled runs, HTML reports |

All platforms use the same `harness` CLI, the same test cases, and produce the same results format.

## What this project does

The harness runs a test by spinning up four containers:

1. **Subject** — the tool being tested (Vector, Fluent Bit, etc.) with a test-specific config
2. **Generator** — sends log data to the subject at a controlled rate
3. **Receiver** — captures the subject's output and counts lines/bytes
4. **Collector** — monitors the subject's CPU, memory, network, and disk usage every second

After the test, results are saved locally as `summary.json` + `metrics.csv`.

## Available tests

### Performance tests (13)

| Test | What it does |
|---|---|
| `tcp_to_tcp_performance` | TCP in, TCP out (raw passthrough baseline) |
| `tcp_to_tcp_5min_performance` | Same as above but 5-minute sustained run |
| `tcp_to_tcp_persistent_performance` | TCP in, TCP out with disk persistence on the forwarding path |
| `file_to_tcp_performance` | Tail a file, forward over TCP |
| `tcp_to_http_performance` | TCP in, HTTP POST out |
| `tcp_to_http_5min_performance` | Same as above but 5-minute sustained run |
| `tcp_to_blackhole_performance` | TCP in, discard output (overhead baseline) |
| `disk_buffer_performance` | TCP in, disk buffer, TCP out |
| `regex_mask_performance` | TCP in, regex mask on every record (e.g. `CONN=\d+` → `CONN=***`), TCP out |
| `regex_parsing_performance` | TCP in, regex syslog parse, TCP out |
| `lua_base_performance` | TCP in, scripting transform, TCP out |
| `real_world_1_performance` | Parse, filter, and route (mixed pipeline) |

### Correctness tests (8)

| Test | What it checks |
|---|---|
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
| VirtualMetric Director | `vmetric/director` | `latest` |
| Vector | `timberio/vector` | `0.54.0-alpine` |
| Fluent Bit | `fluent/fluent-bit` | `3.2` |
| Fluentd | `fluent/fluentd` | `v1.17-debian-1` |
| Logstash | `docker.elastic.co/logstash/logstash` | `8.13.0` |
| Filebeat | `docker.elastic.co/beats/filebeat` | `8.13.0` |
| Cribl Stream | `cribl/cribl` | `4.17.0` |
| Telegraf | `telegraf` | `1.30-alpine` |
| Splunk HF | `splunk/splunk` | `latest` |
| NXLog CE | `nxlog/nxlog-ce` | `latest` |
| AxoSyslog | `ghcr.io/axoflow/axosyslog` | `4.24.0` |
| Tenzir | `ghcr.io/tenzir/tenzir` | `v5.30.0` |
| OpenTelemetry Collector | `otel/opentelemetry-collector-contrib` | `0.149.0` |
| Grafana Alloy | `grafana/alloy` | `v1.15.0` |
| BindPlane Agent | `observiq/bindplane-agent` | `latest` |

## Project structure

```
PipeBench/
  cmd/harness/           CLI binary
  internal/              Config, orchestration (Docker + K8s), runner, results
  containers/
    generator/           Sends test load (TCP, file, or HTTP)
    receiver/            Receives output, counts lines, validates correctness
    collector/           Polls Docker stats API, writes metrics CSV
    vmetric/             Dockerfile + pre-built binary for the VirtualMetric Director subject
  cases/                 22 test cases, each with per-subject configs
  web/                   Static PipeBench UI (single HTML + data/index.json + per-case JSON)
  results/               Created at runtime: results/<hardware>/<test>/<config>/<subject>/…
```
