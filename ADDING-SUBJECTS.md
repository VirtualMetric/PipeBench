# Adding A New Subject

This guide explains how to add a new pipeline subject to PipeBench and submit comparable results.

A subject is a containerized pipeline tool such as Vector, Fluent Bit, Logstash, or VirtualMetric DataStream. It must be able to run from a container image and be configured by files mounted from `cases/<test>/configs/`.

## Before You Add One

PipeBench keeps the subject list intentionally narrow. A new subject should support the same class of work as the existing subjects:

- TCP and/or HTTP input and output.
- File tailing for file-based cases.
- Basic transforms such as parsing, field mutation, filtering, routing, and regex masking.
- Disk persistence on the forwarding path for persistence cases.
- A production-style deployment model, not only a toy or single-purpose shipper.

If a subject cannot run a case honestly, do not fake the result. Either leave it out of that case or explain the limitation in the PR.

## Code Changes

1. Add the subject to `internal/config/subject.go`.

```go
"mytool": {
    Name:       "mytool",
    Image:      "example/mytool",
    Version:    "1.2.3",
    ConfigPath: "/etc/mytool/config.conf",
},
```

2. If the config filename is not `<subject>.conf`, update `ConfigFile()` in `internal/config/subject.go`.

3. Update the `Lookup()` error message in the same file so the new subject appears in the available list.

4. Add case config files for the subject.

For most subjects, add one config file per case:

```text
cases/tcp_to_tcp_performance/configs/mytool.conf
cases/tcp_to_http_performance/configs/mytool.conf
cases/regex_mask_performance/configs/mytool.conf
```

If the subject needs multiple files for a case, use a directory named after the subject:

```text
cases/tcp_to_tcp_performance/configs/mytool/
  config.yml
  pipelines/
    main.yml
```

5. Add the subject name to the `subjects:` list in each `case.yaml` where the config is present.

6. Build and smoke test locally.

```bash
make build build-containers
./bin/harness list
./bin/harness test -t tcp_to_tcp_performance -s mytool
```

7. Run correctness tests for the subject before submitting performance results.

```bash
./bin/harness test -s mytool --all-tests --type correctness
```

## Config Expectations

Each subject config should implement the same logical case as the other subjects.

Examples:

- `tcp_to_tcp_performance`: receive raw TCP on `:9000`, forward raw TCP to `receiver:9001`.
- `tcp_to_http_performance`: receive TCP on `:9000`, POST records to the receiver HTTP endpoint.
- `regex_mask_performance`: apply the same masking semantics as the existing configs.
- Persistence cases: use durable disk buffering, not memory-only buffering.

Avoid subject-specific shortcuts that change the work. For example, a config should not drop fields, batch away records, disable required parsing, or bypass the receiver just to improve throughput.

## Generating Results For A PR

If you only want review of the subject integration, a local smoke result is enough. If you want the official published comparison to include the subject, run the same hardware tiers used by the project.

### AWS Instance Setup

Create EC2 instances that match the target hardware labels exactly. For example, if the published tier is `c7i.4xlarge`, run on an actual `c7i.4xlarge` and pass that same label to the harness.

Recommended setup:

- Ubuntu 22.04 or 24.04.
- x86_64 unless the published tier says otherwise.
- Non-burstable EC2 families for performance comparisons.
- Enough EBS space for Docker images, result files, and disk-buffer tests.
- No unrelated workloads running during the benchmark.
- Same PipeBench commit for all subjects being compared.

Install Docker and Go using [README-DOCKER.md](README-DOCKER.md), then build:

```bash
make build build-containers
```

Run the full matrix for your subject on each hardware tier:

```bash
./bin/harness test -s mytool --all-tests --hardware c7i.4xlarge
```

If a case has multiple configurations, run each configuration explicitly:

```bash
./bin/harness test -t tcp_to_tcp_performance -s mytool --config default --hardware c7i.4xlarge
```

Regenerate the result index after running:

```bash
./bin/harness report
```

The important output for a PR is usually:

```text
web/results/<hardware>/mytool.json
web/results/index.json
```

## Pull Request Checklist

Include:

- The `internal/config/subject.go` changes.
- Subject config files under every supported case.
- Updated `subjects:` lists in `case.yaml`.
- Result JSON files for the claimed hardware tiers.
- The exact subject image and version.
- The exact PipeBench commit used to generate results.
- Notes for unsupported cases, if any.

Run before opening the PR:

```bash
go test ./...
cd containers/generator && go test ./...
cd ../receiver && go test ./...
cd ../collector && go test ./...
```

## Review Criteria

Maintainers will review:

- Whether the subject belongs in the comparison set.
- Whether the configs perform equivalent work.
- Whether correctness cases pass.
- Whether result files match the submitted code and hardware labels.
- Whether the methodology was followed closely enough for the numbers to be published.

If the subject is useful but the configs or results are not yet comparable, the PR may be accepted without publishing benchmark numbers.
