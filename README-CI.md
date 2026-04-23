# PipeBench — GitHub Actions CI Guide

Run automated benchmarks in CI/CD pipelines using GitHub Actions. Every push and pull request builds the harness, builds the container images, and runs a smoke test to verify nothing is broken.

> For local Docker testing, see [README-DOCKER.md](README-DOCKER.md). For Kubernetes, see [README-KUBERNETES.md](README-KUBERNETES.md).

---

## What the built-in workflow does

The repository includes a ready-to-use workflow at [.github/workflows/ci.yml](.github/workflows/ci.yml). It runs three jobs on every push to `main` and every pull request:

| Job | What it does | Duration |
| --- | --- | --- |
| **build** | Compiles the harness CLI and all three container modules, runs `go vet` | ~1 min |
| **containers** | Builds the generator, receiver, and collector Docker images | ~2 min |
| **smoke-test** | Builds everything, runs `tcp_to_tcp_performance` against Vector, uploads results as an artifact | ~4 min |

The smoke test runs a real end-to-end benchmark for 2 minutes and verifies that the harness produces valid `summary.json` and `metrics.csv` output. Results are saved as a downloadable artifact for 30 days.

---

## Using the workflow as-is

No configuration needed. Just push to `main` or open a PR and the workflow runs automatically.

To see the results:

1. Go to the **Actions** tab in your GitHub repository.
2. Click on the latest workflow run.
3. Click the **smoke-test** job.
4. In the **Show results** step, you will see the `summary.json` content.
5. Scroll down to **Artifacts** and download `bench-results` to get the full results directory including `metrics.csv`.

---

## Running more tests in CI

To run additional tests or subjects beyond the smoke test, add steps to the workflow. Here are common patterns.

### Run all subjects for one test

```yaml
      - name: Run tcp_to_tcp against all subjects
        run: |
          ./bin/harness test -t tcp_to_tcp_performance -s vector
          ./bin/harness test -t tcp_to_tcp_performance -s fluent-bit
          ./bin/harness test -t tcp_to_tcp_performance -s fluentd

      - name: Compare results
        run: ./bin/harness compare -t tcp_to_tcp_performance
```

### Run multiple tests with a matrix

```yaml
  benchmark:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        test:
          - tcp_to_tcp_performance
          - file_to_tcp_performance
          - syslog_parsing_performance
        subject:
          - vector
          - fluent-bit
          - fluentd
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "stable"
      - run: make build build-containers
      - run: ./bin/harness test -t ${{ matrix.test }} -s ${{ matrix.subject }}
      - uses: actions/upload-artifact@v4
        with:
          name: results-${{ matrix.test }}-${{ matrix.subject }}
          path: results/
```

### Benchmark across hardware profiles

Use a matrix to test each subject at different resource limits:

```yaml
  benchmark:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        subject: [vector, fluent-bit, fluentd]
        profile:
          - { cpu: "1", mem: "1g", name: "small" }
          - { cpu: "2", mem: "4g", name: "medium" }
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-go@v5
        with:
          go-version: "stable"
      - run: make build build-containers
      - run: |
          ./bin/harness test -t tcp_to_tcp_performance \
            -s ${{ matrix.subject }} \
            --cpu-limit ${{ matrix.profile.cpu }} \
            --mem-limit ${{ matrix.profile.mem }}
      - uses: actions/upload-artifact@v4
        with:
          name: results-${{ matrix.subject }}-${{ matrix.profile.name }}
          path: results/
```

### Generate an HTML report

```yaml
      - name: Generate HTML report
        run: ./bin/harness compare -t tcp_to_tcp_performance --format html > report.html

      - name: Upload HTML report
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-report
          path: report.html
```

### Upload results to S3

```yaml
      - name: Upload results to S3
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          AWS_DEFAULT_REGION: us-east-1
        run: |
          pip install awscli
          ./bin/harness push --bucket s3://my-bucket/bench-results/${{ github.sha }}
```

For MinIO or other S3-compatible storage:

```yaml
      - name: Upload results to MinIO
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.MINIO_ACCESS_KEY }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.MINIO_SECRET_KEY }}
        run: |
          pip install awscli
          ./bin/harness push \
            --bucket s3://bench-results/${{ github.sha }} \
            --endpoint ${{ secrets.MINIO_ENDPOINT }}
```

---

## Running correctness tests in CI

Correctness tests take the same time as performance tests but validate data integrity instead of measuring throughput.

```yaml
      - name: Run correctness tests
        run: |
          ./bin/harness test -t sighup_correctness -s vector
          ./bin/harness test -t file_rotate_create_correctness -s vector
          ./bin/harness test -t disk_buffer_persistence_correctness -s vector

      - name: Compare correctness results
        run: |
          ./bin/harness compare -t sighup_correctness
          ./bin/harness compare -t file_rotate_create_correctness
          ./bin/harness compare -t disk_buffer_persistence_correctness
```

Correctness tests exit with a non-zero status if any validation fails, so the CI step will fail automatically.

---

## Scheduled benchmarks

To run benchmarks on a schedule (e.g., nightly) and track performance over time, add a `schedule` trigger:

```yaml
on:
  schedule:
    # Run every night at 2 AM UTC
    - cron: "0 2 * * *"
  workflow_dispatch: # Allow manual triggering
```

Combine with S3 upload to build a historical results archive:

```yaml
      - name: Upload to S3 with date prefix
        run: |
          DATE=$(date -u +%Y-%m-%d)
          ./bin/harness push --bucket s3://my-bucket/bench-results/$DATE
```

---

## Self-hosted runners

GitHub-hosted runners have 2 CPUs and 7 GB RAM. For more consistent and higher-throughput benchmarks, use a self-hosted runner:

1. Set up a [self-hosted runner](https://docs.github.com/en/actions/hosting-your-own-runners) on a dedicated machine with at least 4 CPU cores and 8 GB RAM.
2. Change `runs-on` in the workflow:

```yaml
    runs-on: self-hosted
```

3. Make sure Docker is installed on the runner machine.

Self-hosted runners give you consistent hardware, which makes benchmark numbers comparable across runs. GitHub-hosted runners share hardware with other users and can have noisy-neighbor effects.

---

## Troubleshooting CI failures

### Build step fails with "go: module not found"

The Go module cache may be stale. Add a cache step:

```yaml
      - uses: actions/setup-go@v5
        with:
          go-version: "1.22"
          cache: true
```

### Smoke test fails with "generator did not exit within 5m"

The GitHub-hosted runner may be too slow to pull the Vector image and complete the test in time. Increase the timeout:

```yaml
      - run: ./bin/harness test -t tcp_to_tcp_performance -s vector --timeout 10m
```

### Container build fails with "no space left on device"

GitHub-hosted runners have ~14 GB of free disk space. If you are building many images, free up space first:

```yaml
      - name: Free disk space
        run: |
          sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc
          docker system prune -af
```

### Results artifact is empty

Make sure the test step completed successfully before the upload step. The `if: always()` condition on the upload step ensures it runs even if the test fails:

```yaml
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: bench-results
          path: results/
```

---

## Full example workflow

Here is a complete workflow that runs three performance tests against three subjects, generates an HTML report, and uploads everything:

```yaml
name: Benchmark

on:
  push:
    branches: [main]
  schedule:
    - cron: "0 2 * * 1"  # Every Monday at 2 AM UTC
  workflow_dispatch:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        test:
          - tcp_to_tcp_performance
          - file_to_tcp_performance
          - syslog_parsing_performance
        subject:
          - vector
          - fluent-bit
          - fluentd
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.22"
          cache: true
      - run: make build build-containers
      - run: ./bin/harness test -t ${{ matrix.test }} -s ${{ matrix.subject }} --timeout 10m
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: results-${{ matrix.test }}-${{ matrix.subject }}
          path: results/

  report:
    runs-on: ubuntu-latest
    needs: benchmark
    if: always()
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.22"
      - run: make build

      - uses: actions/download-artifact@v4
        with:
          path: results/
          merge-multiple: true

      - name: Generate reports
        run: |
          for test in tcp_to_tcp_performance file_to_tcp_performance syslog_parsing_performance; do
            ./bin/harness compare -t $test --format html > "${test}_report.html" 2>/dev/null || true
            ./bin/harness compare -t $test --format table 2>/dev/null || true
          done

      - uses: actions/upload-artifact@v4
        with:
          name: benchmark-reports
          path: "*_report.html"
```
