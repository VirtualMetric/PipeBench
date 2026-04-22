# PipeBench — Docker Guide

A containerized benchmarking and correctness testing framework for data pipeline tools. Compare **VirtualMetric DataStream**, **Vector**, **Fluent Bit**, **Fluentd**, **Logstash**, and **AxoSyslog** side by side — no cloud account required.

> This guide covers running tests locally with Docker. For Kubernetes deployments, see [README-KUBERNETES.md](README-KUBERNETES.md).

---

## Quick Start Guide (Ubuntu Linux, from scratch)

This guide assumes you have a fresh Ubuntu machine (22.04 or 24.04) and have never used Linux before. Every command is copy-paste ready. You will run these commands in a **terminal** — think of it as the Linux version of Command Prompt.

### Step 1: Open a terminal

If you are connected via SSH (for example from PuTTY or Windows Terminal), you already have a terminal. If you are on a desktop, press `Ctrl + Alt + T` to open one.

### Step 2: Update the system

Before installing anything, update the package list. You will be asked for your password — type it and press Enter (nothing will appear on screen while you type, that is normal).

```bash
sudo apt update && sudo apt upgrade -y
```

### Step 3: Install Docker

Docker runs the containers that make up each test. These commands install Docker and configure it so you can use it without `sudo`.

```bash
# Install Docker's official GPG key and repository
sudo apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

Now add yourself to the `docker` group so you do not need `sudo` for every Docker command:

```bash
sudo usermod -aG docker $USER
```

**Important:** Log out and log back in for the group change to take effect. If you are on SSH, close the connection and reconnect. Then verify Docker works:

```bash
docker run --rm hello-world
```

You should see "Hello from Docker!" — that means Docker is ready.

### Step 4: Install Go

The harness CLI and the helper containers are written in Go. Install Go 1.22 or newer:

```bash
# Download Go (this gets version 1.22.5 — any 1.22+ works)
curl -fsSL https://go.dev/dl/go1.22.5.linux-amd64.tar.gz -o /tmp/go.tar.gz

# Extract it into /usr/local
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf /tmp/go.tar.gz

# Add Go to your PATH (this makes it permanent)
echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.bashrc
source ~/.bashrc

# Verify
go version
```

You should see something like `go version go1.22.5 linux/amd64`.

### Step 5: Install Git and clone the repository

```bash
sudo apt install -y git make

git clone https://github.com/VirtualMetric/PipeBench.git
cd PipeBench
```

From this point on, all commands are run from inside the `PipeBench` folder.

### Step 6: Build everything

This builds the `harness` CLI binary and the three helper container images (generator, receiver, collector):

```bash
make build
make build-containers
```

The first run will take a few minutes because Docker needs to download the Go base image. Subsequent builds are fast.

> **Skip the container builds?** Prebuilt images are published to Docker Hub — `vmetric/bench-generator`, `vmetric/bench-receiver`, `vmetric/bench-collector` (both `:latest` and a pinned `:sha-<shortsha>` tag per release). If you only plan to run the harness (not modify the helpers), you can skip `make build-containers` and Docker will pull them on first use. Build locally when you want to change the generator/receiver/collector source.

### Step 7: Run your first test

Run the TCP-to-TCP performance test against Vector:

```bash
./bin/harness test -t tcp_to_tcp_performance -s vector
```

What happens behind the scenes:

1. The harness generates a `docker-compose.yaml` in a temp directory.
2. Four containers start on an internal Docker network:
   - **subject** — Vector, configured to listen on TCP port 9000 and forward to port 9001.
   - **generator** — sends log lines to Vector as fast as possible for 60 seconds.
   - **receiver** — listens on port 9001, counts every line it receives.
   - **collector** — polls Docker for CPU/memory/network stats on the Vector container once per second.
3. After the generator finishes, the harness fetches the final line count from the receiver, stops the collector (which writes its CSV), and tears everything down.
4. Results are saved to `results/tcp_to_tcp_performance/default/vector/<version>/<timestamp>/`.

The output will look something like:

```
→ test=tcp_to_tcp_performance  subject=vector  version=0.45.0-alpine  config=default
  starting containers…
  waiting for generator (up to 2m40s)…
  waiting for receiver to drain…
  stopping collector…
  done. results → results/tcp_to_tcp_performance/default/vector/0.45.0-alpine/2026-04-04T120000Z
  lines received: 12345678  bytes received: 3160493568  elapsed: 142.3s
```

### Step 8: Look at the results

Each test run produces two files:

```bash
# Human-readable summary
cat results/tcp_to_tcp_performance/default/vector/*/*/summary.json

# Per-second CPU, memory, network, disk metrics (CSV)
head results/tcp_to_tcp_performance/default/vector/*/*/metrics.csv
```

`summary.json` contains the total lines received, bytes, and elapsed time. `metrics.csv` has one row per second with columns like `cpu_usr`, `mem_used`, `net_recv`, etc.

---

## Common commands

| What you want to do | Command |
|---|---|
| List all available tests and subjects | `./bin/harness list` |
| Run a test against a specific subject | `./bin/harness test -t tcp_to_tcp_performance -s vector` |
| Run a test against multiple subjects | `./bin/harness test -t tcp_to_tcp_performance -s vector,fluent-bit,logstash` |
| Run a test against all subjects in the case | `./bin/harness test -t tcp_to_tcp_performance` |
| Run a test against every registered subject | `./bin/harness test -t tcp_to_tcp_performance --all-subjects` |
| Run with resource limits (1 core, 1 GB) | `./bin/harness test -t tcp_to_tcp_performance -s vector --cpu-limit 1 --mem-limit 1g` |
| Run with more resources (4 cores, 16 GB) | `./bin/harness test -t tcp_to_tcp_performance -s vector --cpu-limit 4 --mem-limit 16g` |
| Run a test with a specific subject version | `./bin/harness test -t tcp_to_tcp_performance -s vector --version 0.40.0-alpine` |
| Run a correctness test | `./bin/harness test -t sighup_correctness -s vector` |
| Compare results across subjects | `./bin/harness compare -t tcp_to_tcp_performance` |
| Compare results as JSON | `./bin/harness compare -t tcp_to_tcp_performance --format json` |
| Compare sorted by CPU usage | `./bin/harness compare -t tcp_to_tcp_performance --sort cpu` |
| Keep containers running after test (debug) | `./bin/harness test -t tcp_to_tcp_performance -s vector --no-cleanup` |
| Remove leftover containers | `./bin/harness clean` |
| Rebuild after code changes | `make build build-containers` |

### Comparing results

After running the same test against multiple subjects, use `compare` to see them side by side. You can run multiple subjects in a single command — the harness runs them sequentially, automatically cleaning up previous results and containers between runs:

```bash
# Run three subjects and compare
./bin/harness test -t tcp_to_tcp_performance -s vector,fluent-bit,fluentd
./bin/harness compare -t tcp_to_tcp_performance

# Or run all subjects defined in the test case
./bin/harness test -t tcp_to_tcp_performance
./bin/harness compare -t tcp_to_tcp_performance
```

This prints a table like:

```
  Test: tcp_to_tcp_performance  Config: default

  SUBJECT      VERSION          THROUGHPUT       AVG CPU   MAX CPU   AVG MEM   MAX MEM   NET I/O
  -------      -------          ----------       -------   -------   -------   -------   -------
  vector       0.45.0-alpine    524,288 lines/s  12.3%     18.7%     42 MB     58 MB     1,024 MB
  fluent-bit   3.2              412,000 lines/s  8.1%      14.2%     28 MB     35 MB     890 MB
  fluentd      v1.17-1          98,000 lines/s   45.2%     62.0%     180 MB    210 MB    720 MB
```

For correctness tests, the table shows PASS/FAIL instead of throughput:

```
  Test: sighup_correctness  Config: default  (correctness)

  SUBJECT      VERSION          RESULT   LINES OUT   DURATION   DETAILS
  -------      -------          ------   ---------   --------   -------
  vector       0.45.0-alpine    PASS     60,000      72.3s      -
  fluent-bit   3.2              PASS     60,000      71.8s      -
  fluentd      v1.17-1          FAIL     59,847      73.1s      line count mismatch: expected 60000, got 59847 (loss: 0.26%)
```

---

## Tuning load generation

The generator opens parallel TCP (or HTTP) connections to push data into the subject. The number of connections is set per test case in `case.yaml`:

```yaml
generator:
  mode: tcp
  target: "subject:9000"
  rate: 0           # 0 = unlimited (as fast as possible)
  line_size: 256
  connections: 4    # parallel TCP connections
```

All TCP-based performance tests default to `connections: 4`. You can change this to match your hardware and the subject under test:

| connections | When to use |
|---|---|
| `1` | Correctness tests, low-throughput subjects, single-core machines |
| `4` | Default for performance tests. Good baseline for most subjects. |
| `8` | High-performance subjects (Vector) on 4+ core machines |
| `16+` | Stress testing. Saturates even the fastest subjects on large machines. |

Each connection runs in its own goroutine with a 256KB write buffer, so the generator itself uses minimal CPU per connection. The bottleneck should always be the subject, not the generator.

To change the connection count, edit the `connections` field in the test case YAML under `cases/<test_name>/case.yaml`. The change takes effect on the next `harness test` run — no rebuild needed.

The `rate` field controls lines per second **per connection**. Setting `rate: 1000` with `connections: 4` produces 4,000 lines/sec total. Setting `rate: 0` (the default for performance tests) means each connection pushes as fast as the TCP stack allows.

---

## Hardware profiles

By default, the subject container runs with no resource limits — it uses all available CPU and memory on the host. To benchmark across different hardware profiles, use `--cpu-limit` and `--mem-limit`:

```bash
# Small: 1 core, 1 GB (simulates a constrained sidecar)
./bin/harness test -t tcp_to_tcp_performance -s vector --cpu-limit 1 --mem-limit 1g

# Medium: 4 cores, 16 GB (typical production node)
./bin/harness test -t tcp_to_tcp_performance -s vector --cpu-limit 4 --mem-limit 16g

# Large: 16 cores, 64 GB (dedicated log pipeline)
./bin/harness test -t tcp_to_tcp_performance -s vector --cpu-limit 16 --mem-limit 64g

# No limits (default — uses all host resources)
./bin/harness test -t tcp_to_tcp_performance -s vector
```

Resource limits are applied only to the **subject** container (the tool being benchmarked). The generator, receiver, and collector are always unconstrained so they don't become bottlenecks.

The limits are recorded in `summary.json` (`subject_cpu_limit`, `subject_mem_limit`) so you can compare results across profiles. Run the same test with different limits to see how each tool scales:

```bash
# Compare Vector at different hardware tiers
./bin/harness test -t tcp_to_tcp_performance -s vector --cpu-limit 1 --mem-limit 1g
./bin/harness test -t tcp_to_tcp_performance -s vector --cpu-limit 4 --mem-limit 4g
./bin/harness test -t tcp_to_tcp_performance -s vector --cpu-limit 8 --mem-limit 8g
```

| Flag | Format | Examples |
| --- | --- | --- |
| `--cpu-limit` | Number of CPU cores | `1`, `2`, `4`, `0.5` |
| `--mem-limit` | Memory with unit suffix | `512m`, `1g`, `4g`, `16g` |

---

## Understanding the results

Each test run produces a `summary.json` with all metrics:

```json
{
  "test_name": "tcp_to_tcp_performance",
  "subject": "vector",
  "version": "0.54.0-alpine",
  "duration_secs": 140.4,
  "lines_in": 35013133,
  "lines_out": 35005840,
  "lines_per_sec": 250425,
  "loss_percent": 0.02,
  "avg_cpu_percent": 119.7,
  "max_cpu_percent": 142.3,
  "avg_mem_mb": 63,
  "max_mem_mb": 78,
  "system_cpus": 2,
  "system_mem_mb": 7938,
  "subject_cpu_limit": "1",
  "subject_mem_limit": "1g",
  "latency_p50_ms": 1.2,
  "latency_p95_ms": 3.4,
  "latency_p99_ms": 8.1
}
```

| Field | Meaning |
|---|---|
| `lines_in` / `lines_out` | Lines sent by generator vs received by receiver |
| `lines_per_sec` | Throughput (lines out / duration) |
| `loss_percent` | Percentage of lines lost in transit |
| `avg_cpu_percent` | Average CPU usage (100% = 1 core, 200% = 2 cores) |
| `max_cpu_percent` | Peak CPU usage during the test |
| `avg_mem_mb` / `max_mem_mb` | Memory usage in megabytes |
| `system_cpus` / `system_mem_mb` | Host machine resources (for context) |
| `subject_cpu_limit` / `subject_mem_limit` | Resource limits applied to the subject |
| `latency_p50/p95/p99_ms` | End-to-end latency percentiles (sampled) |

The per-second `metrics.csv` file has one row per second with CPU, memory, network, and disk I/O columns for detailed analysis.

---

## Troubleshooting

### "permission denied" when running docker commands

You forgot to log out and back in after Step 3. Close your terminal or SSH session, reconnect, and try again.

### "Cannot connect to the Docker daemon"

Docker is not running. Start it:

```bash
sudo systemctl start docker
sudo systemctl enable docker
```

### Generator times out connecting

The subject container may be slow to start (especially Logstash, which is Java-based). The generator retries for 30 seconds. If it still fails, try running the test again — Docker may have been pulling the image on the first attempt.

### "image not found" errors

Your machine needs internet access to pull container images from Docker Hub. If you are behind a corporate proxy, configure Docker's proxy settings:

```bash
sudo mkdir -p /etc/systemd/system/docker.service.d
sudo tee /etc/systemd/system/docker.service.d/proxy.conf <<EOF
[Service]
Environment="HTTP_PROXY=http://your-proxy:port"
Environment="HTTPS_PROXY=http://your-proxy:port"
EOF
sudo systemctl daemon-reload
sudo systemctl restart docker
```

### Low throughput numbers

Make sure no other heavy processes are running. For fair benchmarking, a machine with at least 4 CPU cores and 4 GB RAM is recommended. Running inside a small VM will produce lower numbers than bare metal.

---

## Available tests

### Performance tests (13)

These measure throughput, CPU, memory, and I/O while each subject processes data at maximum speed for 2 minutes.

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

These verify data integrity — no lost, duplicated, or reordered events. The generator embeds sequence numbers in each line, and the receiver validates them.

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

---

## Project structure (for the curious)

```
PipeBench/
  cmd/harness/           CLI binary (the tool you run)
  internal/              Go packages for config, orchestration, metrics, results
  containers/
    generator/           Sends test load (TCP, file, or HTTP)
    receiver/            Receives output, counts lines, validates correctness
    collector/           Polls Docker stats API, writes metrics CSV
  cases/
    tcp_to_tcp_performance/
      case.yaml          Test definition (duration, generator/receiver modes)
      configs/
        vector.toml      Vector config for this test
        fluent-bit.conf  Fluent Bit config for this test
        fluentd.conf     ...and so on for each subject
    sighup_correctness/
      case.yaml          Correctness test with validation flags
      configs/
        vector.toml
        fluent-bit.conf
        fluentd.conf
  results/               Created at runtime, one folder per test run
  Makefile               Build shortcuts
  PLAN.md                Full technical design document
```

---

## What is being measured

For each test, the harness captures these metrics every second:

| Metric | What it means |
|---|---|
| `cpu_usr` | CPU time spent in userspace (higher = subject is working harder) |
| `mem_used` | Memory used by the subject container in bytes |
| `net_recv` / `net_send` | Network bytes received and sent per second |
| `dsk_read` / `dsk_writ` | Disk bytes read and written per second |

The `summary.json` gives you the bottom line: how many log lines went in, how many came out, and how long it took.
