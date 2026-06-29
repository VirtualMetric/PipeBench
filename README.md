# PipeBench — Data Pipeline Benchmark

A containerized benchmarking and correctness testing framework for data pipeline tools. Compare **VirtualMetric DataStream**, **Vector**, **Fluent Bit**, **Fluentd**, **Logstash**, **Filebeat**, **Telegraf**, **AxoSyslog**, **NXLog**, **Tenzir**, **OpenTelemetry Collector**, **Grafana Alloy**, **BindPlane Agent**, **Cribl Stream**, and **Streamfold Rotel** side by side. Everything runs in Docker — clone the repo, build the harness and helper images, and reproduce any published result on the same hardware tier with one command.

## Getting started

PipeBench runs on Docker — local machine or single-node EC2. Follow [README-DOCKER.md](README-DOCKER.md) to install dependencies, build the harness and helper images, and run your first test.

## Project guides

| Guide | What it covers |
| --- | --- |
| [METHODOLOGY.md](METHODOLOGY.md) | How tests are run, what is measured, and how fairness is handled |
| [ADDING-SUBJECTS.md](ADDING-SUBJECTS.md) | How to add a new subject and submit comparable result files |
| [REPRODUCING-RESULTS.md](REPRODUCING-RESULTS.md) | How to reproduce published results locally or on matching AWS hardware |
| [REPORTING-MISTAKES.md](REPORTING-MISTAKES.md) | How to report unfair configs, bad results, broken tests, or documentation errors |
| [FUTURE-CAPABILITIES.md](FUTURE-CAPABILITIES.md) | Additive, opt-in case-schema capabilities (multi-receiver, multi-generator, TLS, rate-ceiling, load-balance) prepared for future test cases |

## What this project does

The harness runs a test by spinning up four containers:

1. **Subject** — the tool being tested (Vector, Fluent Bit, etc.) with a test-specific config
2. **Generator** — sends log data to the subject at a controlled rate
3. **Receiver** — captures the subject's output and counts lines/bytes
4. **Collector** — monitors the subject's CPU, memory, network, and disk usage every second

After the test, the result is merged into a single per-(hardware, subject) JSON file at `web/results/<hardware>/<subject>.json`. Re-running the same `(test, config)` replaces the previous row in place — the UI always shows the latest run.

## Available tests

### Performance tests (25)

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
| `disk_buffer_crash_performance` | Same as above but every subject in its strongest crash-safe (fsync-per-write) disk-buffer mode — measures the EPS cost of zero-loss durability |
| `regex_mask_performance` | TCP in, regex mask on every record (e.g. `CONN=\d+` → `CONN=***`), TCP out |
| `syslog_parsing_performance` | TCP in, parse syslog message, TCP out |
| `set_field_performance` | TCP in, add one field via native transform, TCP out |
| `real_world_1_performance` | Parse, filter, and route (mixed pipeline) |
| `netflow_to_tcp_performance` | NetFlow v5 over UDP in, decoded records as TCP lines out |
| `otlp_to_otlp_generic_performance` | OTLP/HTTP+protobuf in, OTLP/HTTP+protobuf out (no transforms) |
| `otlp_grpc_to_otlp_grpc_performance` | OTLP/gRPC end-to-end (isolates protocol cost vs. HTTP+protobuf) |
| `otlp_pipeline_to_otlp_performance` | OTLP/HTTP in, OTLP/HTTP out with one add-attribute transform per record |
| `s3_to_tcp_performance` | Ingest from S3 (LocalStack emulator, S3→SQS notifications), TCP out |
| `tcp_to_s3_performance` | TCP in, S3 objects out (LocalStack emulator) |
| `azure_blob_to_tcp_performance` | Ingest from Azure Blob Storage (Azurite emulator, BlobCreated queue events), TCP out |
| `tcp_to_azure_blob_performance` | TCP in, Azure block blobs out (Azurite emulator) |
| `tcp_to_sqs_performance` | TCP in, SQS messages out (LocalStack emulator) |
| `tcp_to_sns_performance` | TCP in, SNS topic out, observed via an SQS subscription (LocalStack emulator) |
| `tcp_to_kinesis_performance` | TCP in, Kinesis stream out across 4 shards (LocalStack emulator) |
| `tcp_to_cloudwatch_performance` | TCP in, CloudWatch Logs out (LocalStack emulator) |

### Correctness tests (17)

| Test | What it checks |
| --- | --- |
| `disk_buffer_persistence_correctness` | Events survive subject restart with disk buffer |
| `tcp_to_tcp_persistent_correctness` | Logs sent while receiver is down are persisted and delivered when it comes up |
| `tcp_to_tcp_persistent_restart_correctness` | Same as above, plus the subject is restarted mid-test (SIGTERM, graceful) |
| `tcp_to_tcp_persistent_crash_correctness` | Same as above, but the subject is SIGKILL'd (no graceful flush — only fsync'd writes survive) |
| `tcp_to_http_persistent_correctness` | Persistence correctness with an HTTP receiver as the target |
| `file_rotate_create_correctness` | New-file log rotation handled without loss |
| `file_rotate_restart_correctness` | File-tail subject recovers across a rotation it was offline for: read partway, SIGTERM, log rotated (create mode) and more events written while offline, restart, catch up without re-emitting |
| `file_rotate_truncate_correctness` | Truncation-based log rotation handled correctly |
| `file_truncate_correctness` | Direct file truncation handled correctly |
| `sighup_correctness` | Config reload via SIGHUP without data loss |
| `wrapped_json_correctness` | JSON-in-string fields parsed correctly |
| `otlp_to_otlp_generic_correctness` | OTLP/HTTP+protobuf round-trip preserves every LogRecord body |
| `otlp_grpc_to_otlp_grpc_correctness` | OTLP/gRPC round-trip preserves every LogRecord body |
| `otlp_pipeline_to_otlp_correctness` | OTLP/HTTP round-trip with one add-attribute transform preserves every body |
| `tcp_to_s3_correctness` | Every TCP line lands in S3 exactly once (LocalStack emulator) |
| `s3_to_tcp_correctness` | Every S3 record is emitted over TCP; SQS-notification consumption is at-least-once, so duplicates are tolerated, loss is not |
| `tcp_to_azure_blob_correctness` | Every TCP line lands in Azure Blob Storage exactly once (Azurite emulator) |

### Cloud emulator cases (Azurite + LocalStack)

Cloud cases run entirely against local emulators added to the compose topology by a case's `aws:` / `azure:` block — **[LocalStack](https://localstack.cloud)** for S3/SQS/SNS/Kinesis/CloudWatch Logs and **[Azurite](https://github.com/Azure/Azurite)** for Azure Blob Storage. The harness creates the declared buckets/queues/topics/streams/containers before the subject starts and injects the emulator endpoint plus the standard dummy credentials (`test`/`test` for AWS, Azurite's published `devstoreaccount1` development account) into every container. No real cloud account is touched: `AWS_ENDPOINT_URL` is pinned to the emulator, IMDS probing is disabled, and the emulators are reachable only on the isolated bench network.

Two caveats when reading the numbers:

- **Compare across subjects, not across case types.** The emulators (Python/Node single-process services) cap throughput far below the raw TCP cases. Every subject faces the same ceiling, so rankings are meaningful; absolute EPS is not comparable to `tcp_to_tcp_performance`.
- **Out of scope:** Azure Event Hubs, Service Bus, Sentinel and Data Explorer have no usable local emulator; AWS Firehose and DynamoDB delivery are LocalStack pro-tier. Cases for those services would silently measure a mock, so they don't exist.

## Subjects

| Name | Image | Version |
| --- | --- | --- |
| VirtualMetric DataStream | `vmetric/director` | `2.0.6` |
| Vector | `timberio/vector` | `0.54.0-alpine` |
| Fluent Bit | `fluent/fluent-bit` | `5.0` |
| Fluentd | `fluent/fluentd` | `v1.17-debian-1` |
| Logstash | `docker.elastic.co/logstash/logstash` | `8.13.0` |
| Filebeat | `docker.elastic.co/beats/filebeat` | `8.13.0` |
| Telegraf | `telegraf` | `1.30-alpine` |
| AxoSyslog | `ghcr.io/axoflow/axosyslog` | `4.25.0` |
| NXLog CE | `nxlog/nxlog-ce` | `3.2.2329` |
| Tenzir | `ghcr.io/tenzir/tenzir` | `v5.30.0` |
| OpenTelemetry Collector (contrib) | `otel/opentelemetry-collector-contrib` | `0.149.0` |
| Grafana Alloy | `grafana/alloy` | `v1.15.0` |
| BindPlane Agent | `observiq/bindplane-agent` | `1.99.0` |
| Cribl Stream | `cribl/cribl` | `4.17.0` |
| Streamfold Rotel | `streamfold/rotel` | `v0.2.2` |

Not every subject participates in every case — coverage depends on each tool's native capabilities. For example, only `vmetric`, `otel-collector`, `bindplane-agent`, and `cribl-stream` ship a first-party NetFlow v5 listener; OTLP cases skip subjects whose OTLP support is partial (e.g. Vector's OTLP sink is HTTP-only). Each `case.yaml` lists the subjects that case actually runs.

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
  cases/                 51 test cases (27 performance + 24 correctness), each with per-subject configs
  web/                   Static PipeBench UI (single HTML + per-(hardware, subject) JSON under web/results/)
```

## Credits

PipeBench stands on the shoulders of two prior projects:

- **[Vector Test Harness](https://github.com/vectordotdev/vector-test-harness)** — the original benchmarking framework that defined the test cases, the metrics schema, and the comparative results tables PipeBench inherits. The upstream project is archived, and its AWS + Terraform + Ansible + Packer + Debian Buster (EOL) toolchain is no longer practical to stand up. PipeBench keeps the test matrix and methodology intact while replacing the entire deployment story with Docker Compose: clone, `make build build-containers`, run.
- **[ClickBench](https://github.com/ClickHouse/ClickBench)** — the inspiration for the comparative results UI in [web/](web/). We simplified the layout around standard hardware tiers (one tab per EC2 instance class) and the smaller subject set, but the side-by-side ranking-card style is theirs.
