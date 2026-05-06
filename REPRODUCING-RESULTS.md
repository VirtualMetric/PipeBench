# Reproducing Results

PipeBench results are meant to be reproducible. The most important rule is simple: use the same repository commit, same subject image versions, same helper images, same hardware tier, and same test cases.

For the benchmark method itself, read [METHODOLOGY.md](METHODOLOGY.md).

## Set up your environment

Follow [README-DOCKER.md](README-DOCKER.md) to install Docker, Go, and build the harness + helper container images.

Docker is the only supported way to run PipeBench. For published comparable numbers, run on a matching EC2 instance type with no other heavy workloads.

## Reproduce A Single Test

```bash
git clone https://github.com/VirtualMetric/PipeBench.git
cd PipeBench

make build build-containers

./bin/harness test \
  -t tcp_to_tcp_performance \
  -s vector \
  --hardware custom
```

The result is written under:

```text
web/results/<hardware>/<subject>.json
```

For the command above:

```text
web/results/custom/vector.json
```

## Reproduce A Published Hardware Tier

If a published result is labeled `c7i.4xlarge`, use the same EC2 instance type.

Recommended process:

1. Launch a fresh EC2 instance with the same type as the hardware label.
2. Use Ubuntu 22.04 or 24.04.
3. Install Docker and Go from [README-DOCKER.md](README-DOCKER.md).
4. Clone the same PipeBench commit used by the result you are checking.
5. Build the harness and helper images.
6. Run the same test and subject with the same `--hardware` label.

```bash
make build build-containers

./bin/harness test \
  -t tcp_to_tcp_performance \
  -s vector \
  --hardware c7i.4xlarge
```

Avoid running other heavy workloads on the machine while reproducing results. CPU scheduling, disk pressure, network pressure, and noisy neighbors can all move benchmark numbers.

## Reproduce All Tests For One Subject

```bash
./bin/harness test -s vector --all-tests --hardware c7i.4xlarge
./bin/harness report
```

To run only correctness tests:

```bash
./bin/harness test -s vector --all-tests --type correctness --hardware c7i.4xlarge
```

To run only performance tests:

```bash
./bin/harness test -s vector --all-tests --type performance --hardware c7i.4xlarge
```

If you need to run a specific configuration:

```bash
./bin/harness test \
  -t tcp_to_tcp_performance \
  -s vector \
  --config default \
  --hardware c7i.4xlarge
```

## Reproduce Across Subjects

Run the same test against each subject on the same machine:

```bash
./bin/harness test -t tcp_to_tcp_performance -s vmetric          --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s vector           --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s fluent-bit       --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s fluentd          --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s logstash         --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s filebeat         --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s telegraf         --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s axosyslog        --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s nxlog            --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s tenzir           --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s splunk-hf        --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s otel-collector   --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s grafana-alloy    --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s bindplane-agent  --hardware c7i.4xlarge
./bin/harness test -t tcp_to_tcp_performance -s cribl-stream     --hardware c7i.4xlarge
```

Or run all subjects listed by the case:

```bash
./bin/harness test -t tcp_to_tcp_performance --hardware c7i.4xlarge
```

Then regenerate the index:

```bash
./bin/harness report
```

## View Results

Serve the local UI:

```bash
./bin/harness serve
```

Then open the printed URL, usually:

```text
http://localhost:18080
```

For command-line comparison:

```bash
./bin/harness compare -t tcp_to_tcp_performance
```

## What To Record

When reporting a reproduction, include:

- PipeBench commit SHA.
- Subject name and image tag.
- Test name and configuration name.
- Hardware label and actual machine type.
- Operating system and kernel version.
- Docker version.
- Exact command used.
- Result JSON from `web/results/<hardware>/<subject>.json`.

## Expected Variation

Small differences are normal. Results can vary because of:

- CPU model and turbo behavior.
- Docker version.
- Kernel version.
- Disk type and free space.
- Network stack behavior.
- Background services.
- Cloud noisy neighbors.

Large differences should be investigated. See [REPORTING-MISTAKES.md](REPORTING-MISTAKES.md) for how to file a useful issue.
