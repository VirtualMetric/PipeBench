# Reporting Mistakes

Benchmarks are only useful when mistakes can be found, explained, and fixed. If you see a suspicious result, unfair config, broken test, or documentation problem, please open an issue or PR.

## What Counts As A Mistake

Useful reports include:

- A subject config does different work than the others.
- A test case favors one subject because of an avoidable config choice.
- A result cannot be reproduced on the same hardware tier.
- A correctness test passes even though records are lost, duplicated, reordered, or corrupted.
- A performance result includes startup, idle time, or cleanup in a misleading way.
- A subject image tag is outdated or points to the wrong major version.
- Documentation gives a command that no longer works.

Methodology disagreements are welcome too. Explain the benchmark goal you think is being missed and what change would make the comparison fairer.

## Open An Issue

Please include as much of this as you can:

- PipeBench commit SHA.
- Test name, for example `tcp_to_tcp_performance`.
- Configuration name, usually `default`.
- Subject name and image tag.
- Hardware label, for example `c7i.4xlarge`.
- Actual machine type if different from the label.
- Operating system, kernel version, Docker version.
- Exact command you ran.
- The result JSON file or relevant excerpt.
- Any logs from generator, subject, receiver, or collector.
- What you expected to happen.
- What happened instead.

A strong issue title is specific:

```text
regex_mask_performance: logstash config does not apply the same mask as vector
```

or:

```text
tcp_to_tcp_performance: vector c7i.4xlarge result is not reproducible on current main
```

## Attach Evidence

Good evidence makes fixes much faster:

- Result file from `web/results/<hardware>/<subject>.json`.
- A small diff showing the config problem.
- The generated `docker-compose.yaml` if orchestration looks wrong.
- Container logs from a failed run.
- A short reproduction command.
- Screenshots only when the issue is about the web UI.

Do not include credentials, license keys, private hostnames, or private customer data in logs or configs.

## Open A PR

If you already know the fix, a PR is very welcome.

Common PRs:

- Update a case config under `cases/<test>/configs/`.
- Correct a `case.yaml` setting.
- Add missing validation.
- Improve methodology docs.
- Replace a stale result file with a reproduced one.
- Add a new subject using [ADDING-SUBJECTS.md](ADDING-SUBJECTS.md).

Before submitting a PR, run:

```bash
go test ./...
cd containers/generator && go test ./...
cd ../receiver && go test ./...
cd ../collector && go test ./...
```

If the PR changes benchmark behavior, also run at least one affected test and include the command and result summary in the PR description.

## Result Corrections

If you believe an existing published result is wrong, include:

- The old result value.
- Your reproduced result value.
- The exact hardware and command used.
- Whether you changed any case config.
- Whether the old result should be removed, replaced, or marked as disputed.

Maintainers may ask for a rerun on the same AWS instance type before accepting a replacement result.

## Fairness Discussions

For fairness discussions, please separate facts from proposals.

Helpful structure:

1. The current behavior.
2. Why it affects fairness.
3. Which subjects are affected.
4. A proposed rule or code change.
5. A small before/after result, if available.

See [METHODOLOGY.md](METHODOLOGY.md) for the current benchmark rules.
