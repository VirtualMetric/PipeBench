# PipeBench Methodology

PipeBench exists to compare data pipeline subjects under repeatable, end-to-end workloads. The goal is not to prove that one tool is universally best. The goal is to show how each subject behaves when it receives the same input, runs the same pipeline shape, and has the same hardware budget.

The main questions are:

- How many records can the subject accept and deliver?
- How much CPU, memory, disk I/O, and network I/O does it use while doing real work?
- Does it lose, duplicate, corrupt, or reorder records in correctness-sensitive cases?
- How does the subject scale when the hardware tier or generator concurrency increases?
- How does it behave under pressure: back-pressure, disk buffering, transforms, parsing, routing, and receiver outages?

## Test Model

Each test run starts four roles:

1. **Subject**: the pipeline tool being benchmarked.
2. **Generator**: produces log records and sends them to the subject.
3. **Receiver**: receives the subject's output and counts lines, bytes, validation errors, and timestamps.
4. **Collector**: samples CPU, memory, disk, and network metrics from the subject once per second.

The subject is configured with a case-specific config from `cases/<test>/configs/`. The generator and receiver behavior comes from the test's `case.yaml`.

## Run Lifecycle

For a normal performance test, the harness does this:

1. Removes leftover benchmark containers from prior runs.
2. Resolves the test case, subject image, subject config, and selected configuration name.
3. Starts subject, generator, receiver, and collector containers.
4. Gives the subject a warmup/readiness budget from `warmup`.
5. Runs the generator for the test `duration`.
6. Waits a fixed post-send receive budget from `drain_grace` (default `5s`).
7. Captures final receiver metrics.
8. Stops the subject as cleanup.
9. Copies the collector CSV and stops the collector.
10. Computes derived metrics and writes results.

Correctness and persistence tests may use a different lifecycle because their goal is completeness, restart behavior, or receiver outage recovery rather than fixed-window throughput.

## Warmup

Warmup is not part of the throughput score.

For TCP and HTTP generator modes, the generator waits up to the configured warmup period for the target port to become reachable. If the port is ready early, the generator starts early. If the port does not become ready before the warmup budget expires, the generator continues and the normal connection retry/error path applies.

This avoids punishing slower-starting subjects, such as JVM-based tools, for process boot time. PipeBench is measuring pipeline work, not how fast a container starts.

## Drain Grace

Performance tests use a fixed post-send receive budget. By default, the receiver gets `5s` after the generator finishes to count records that are already in flight or being flushed.

This is intentionally bounded. A subject with a large internal buffer should not get unlimited time to finish delivery and then be scored as if that delivery happened during the main test window. At the same time, the subject is not killed before the receiver metrics are captured, so every subject gets the same post-send opportunity.

You can override this per case:

```yaml
duration: 60s
warmup: 10s
drain_grace: 5s
```

## Throughput

Throughput is reported as:

```text
lines_per_sec = lines_for_rate / active_duration_seconds
```

For normal output-producing tests, `lines_for_rate` is `lines_out`: the number of records the receiver counted by the fixed cutoff.

For blackhole tests, the receiver is expected to receive nothing, so the harness falls back to `lines_in` to report how quickly the subject accepted input. Loss is still computed from `lines_out` versus `lines_in`.

The active duration is:

```text
first generator send -> max(last generator send, last received line before cutoff)
```

This matters. Using only `first_received -> last_received` can inflate buffered subjects that accept input for 60 seconds but emit later in a short burst. Using the active window includes send back-pressure and in-grace drain without charging startup time.

## Loss

Loss is reported as:

```text
loss_percent = 100 * (1 - lines_out / lines_in)
```

Negative loss is clamped to zero for display, but over-delivery is still treated as a correctness problem in correctness tests because it means duplicate or unexpected records were received.

## Resource Metrics

The collector samples the subject's CPU, memory, disk, and network counters once per second.

Average CPU and memory are computed over the active benchmark window, not over the whole container lifetime. This avoids diluting averages with startup, warmup, cleanup, or idle samples. Max CPU and max memory are also taken from the same active window when timestamps are available.

The important fields are:

- `avg_cpu_percent`: average subject CPU use, where `100%` means roughly one full core.
- `max_cpu_percent`: peak sampled subject CPU use.
- `avg_mem_mb`: average subject memory use in MB.
- `max_mem_mb`: peak sampled subject memory use in MB.
- `disk_read_bytes` / `disk_write_bytes`: disk activity during sampled rows.
- `net_recv_bytes` / `net_send_bytes`: network activity during sampled rows.

## Correctness Tests

Correctness tests focus on data integrity rather than maximum throughput.

Depending on the case, the generator emits sequenced records and the receiver validates:

- Expected loss percentage.
- Duplicate records.
- Malformed or corrupted records.
- Ordering when the case enables ordering validation.
- Persistence across receiver downtime or subject restart.

Correctness tests usually wait for the receiver to stabilize instead of using a small fixed `drain_grace`, because the question is whether the data eventually arrives correctly under the tested failure mode.

## Hardware Comparisons

Results are grouped by hardware label, usually the EC2 instance type:

```bash
./bin/harness test -t tcp_to_tcp_performance -s vector --hardware c7i.4xlarge
```

The label is stored in result files under `web/results/<hardware>/<subject>.json`.

To compare scaling, run the same test matrix on each hardware tier. The same subject, same case files, same image version, same helper image versions, and same operating system setup should be used across tiers.

## Fairness Rules

PipeBench tries to keep comparisons fair by:

- Running every subject through the same generator, receiver, and collector.
- Using the same input format, line size, duration, and receiver mode for a given case.
- Excluding startup from throughput and average resource metrics.
- Giving each performance run the same fixed post-send drain budget.
- Measuring throughput over the full active work window, not just the receiver burst.
- Recording loss separately from throughput.
- Keeping raw result fields in JSON so results can be audited.
- Requiring subject configs to implement the same logical pipeline.

## Known Limits

No benchmark is perfectly universal.

- A case measures one pipeline shape, not every possible production deployment.
- Docker adds its own scheduling and networking behavior on top of the host.
- A small `drain_grace` favors subjects that can deliver close to real time, by design.
- Very large internal buffers may be useful in production, but PipeBench records delayed delivery as backlog or loss for fixed-window performance tests.
- Hardware, kernel, Docker version, image version, and noisy neighbors can change results.

When results matter, reproduce them on the same commit and the same hardware tier. See [REPRODUCING-RESULTS.md](REPRODUCING-RESULTS.md).
