# Future case-schema capabilities

This document describes harness capabilities that exist in PipeBench but
are not yet exercised by any case in `cases/`. They are present so future
test cases can model topologies the original singular-generator,
singular-receiver schema can't express: multi-source ingest, fan-out and
load-balanced output routing, transport-level TLS, throttle-correctness
validation, and so on.

Every feature documented here is **additive and opt-in**. Cases that
don't use the new keys are byte-identical to how they ran before — no
existing case in this repo references any of the schema added on this
page, and every subject's behaviour is unchanged unless it sees one of
the new env vars.

Skip this page unless you're authoring a new case that needs one of these
shapes; the existing [METHODOLOGY.md](METHODOLOGY.md) already covers the
schema every current case uses.

---

## Multi-receiver cases (`receivers:`)

A case can declare more than one receiver. Each entry produces its own
`bench-receiver-<id>` container, with hostname `receiver-<id>` inside the
bench network so subject configs can target each sink by stable alias.

```yaml
receivers:
  - id: tcp-sink
    mode: tcp
    listen: ":9001"
  - id: http-sink
    mode: http
    listen: ":9002"
```

Rules:

- `receivers:` and the singular `receiver:` form are mutually exclusive
  (the harness errors out on a case that sets both).
- Each entry must have a unique `id`.
- Host port mapping is sequential from `--receiver-port` (default
  `19001`): the first entry is bound to `19001:9090`, the second to
  `19002:9090`, etc.
- Per-receiver counts land in the result file under
  `per_receiver: { "<id>": N, … }`. The case-level `lines_out` is the
  sum.
- `expected_loss_pct` is applied to the SUM across receivers vs the
  expected output (see `expected_multiplier` below).

### Fan-out and `expected_multiplier`

When the same record reaches every receiver — a fan-out topology — the
expected receiver total is `lines_in * N`, not `lines_in`. Set
`correctness.expected_multiplier` to the receiver count so loss
accounting stays honest:

```yaml
correctness:
  expected_multiplier: 2   # one TCP sink + one HTTP sink, each sees every record
  expected_loss_pct: 0.0
  validate_dedup: false    # fan-out: each record reaches every sink, dedup would flag every record
```

Defaults to `1` (no fan-out). Singular-receiver cases never need to set
it.

---

## Multi-generator cases (`generators:`)

A case can declare more than one generator. Each entry produces its own
`bench-generator-<id>` container, started in parallel and observing a
shared "stop now" budget so a single hung generator can't extend the
case past `duration`.

```yaml
generators:
  - id: tcp-side
    mode: tcp
    target: "subject:9000"
    rate: 50
    line_size: 256
    format: syslog
  - id: udp-side
    mode: udp
    target: "subject:9000"
    rate: 50
    line_size: 256
    format: syslog
```

Rules:

- `generators:` and the singular `generator:` form are mutually
  exclusive.
- Each entry must have a unique `id`.
- The harness assigns each generator a non-overlapping `CONN=` offset
  based on cumulative connection counts before it. Sequenced-mode lines
  (`CONN=<n> SEQ=<m>`) from different generators therefore never share a
  `(CONN, SEQ)` pair, so receiver-side dedup keeps working over a
  merged stream.
- A new `udp` generator mode sends each record as a single
  newline-stripped UDP datagram, suitable for transport pairs that share
  a port (e.g. a syslog listener bound to both TCP and UDP).
- The generator JSON result (`lines_sent`, `bytes_sent`, first/last
  send timestamps) is summed across containers when computing the
  case-level totals.

`generator.connections` keeps its meaning per-entry: it's the number of
parallel transport streams that one generator container opens.

---

## Generator-side TLS (`generator.tls`)

The generator can wrap its TCP dial in `crypto/tls`. Used to verify that
a subject terminates TLS correctly end-to-end.

```yaml
generator:
  mode: tcp
  target: "subject:6514"
  rate: 100
  line_size: 256
  format: syslog
  tls:
    enabled: true
    # All fields below are optional. With paths omitted, the harness
    # auto-generates a self-signed CA + leaf cert pair at warmup and
    # bind-mounts /certs/ in both the generator and the subject
    # containers. The subject's config file references the cert paths
    # under /certs/ (server.crt, server.key, ca.crt).
    cert: "/certs/client.crt"
    key:  "/certs/client.key"
    ca:   "/certs/ca.crt"
    insecure_skip_verify: false
    min_version: "1.2"
```

Auto-generation flow (when paths are omitted): the harness uses
`crypto/x509` to write `ca.crt`, `ca.key`, `server.crt`, `server.key`,
`client.crt`, `client.key` into the run's tmp dir, then bind-mounts that
dir at `/certs:ro` in both the subject and the generator. The server
leaf carries SAN `subject` (and `localhost`) so the chain validates
inside the bench network.

### Capability check

TLS is opt-in per subject. Each `Subject` in
[internal/config/subject.go](internal/config/subject.go) has a
`Capabilities []string` field; the harness refuses to run a TLS-enabled
case when the subject does not declare `tls_tcp` there. The default
registry leaves `Capabilities` empty for every subject — add the tag on
a subject only after end-to-end TLS handling is verified for it.

---

## Drain-aware correctness runs

For correctness cases where the subject is expected to keep emitting
records well after the generator stops (queue draining, throttled
forwarding), two knobs replace the fixed 2-minute drain timeout:

```yaml
correctness:
  drain_seconds: 60          # extra time the receiver continues to accept records after generators stop
  drain_quiet_window: 5s     # finish early if receive count stays flat this long
```

Both are opt-in; omitting them keeps the prior 2-minute / 12 stable
polls behaviour. `drain_seconds` is also the upper bound on how long the
case will wait — there's no path where a slow drain keeps the harness
running indefinitely.

---

## Per-window EPS validator (`correctness.rate_ceiling`)

Validates that the per-window EPS observed at the receiver never exceeds
a configured ceiling, ignoring warmup and cooldown bands.

```yaml
correctness:
  rate_ceiling:
    max_eps: 100
    window: 1s            # rolling window size for the EPS calculation
    tolerance: 0.10       # 10% jitter above max_eps allowed in any single window
    skip_warmup: 5s       # ignore the first N seconds after first arrival
    skip_cooldown: 5s     # ignore the last N seconds before drain finishes
    sample: every         # "every" (default) or "peak"
```

When enabled, the receiver records per-record arrival nanoseconds into
an in-memory slice exposed at `/arrival_times`. The harness fetches that
list after drain, slides a `window`-sized window across it, and fails
the case the first time EPS exceeds `max_eps * (1 + tolerance)` (or, with
`sample: peak`, only if the max across all windows breaches).

The arrival-time recorder is fully gated on this field — receivers
without `rate_ceiling` set don't allocate any per-record buffer, and
existing performance runs are byte-identical.

Result JSON gains:

```json
"rate_window": {
  "max_observed_eps": 103.2,
  "overshoot_count": 0,
  "first_overshoot_ns": null,
  "pass": true
}
```

Bounding rate: keep cases that enable `rate_ceiling` to controlled rates
(hundreds of EPS, not millions). The recorder uses a mutex-guarded
slice; it's fine at correctness scale, not perf scale.

---

## Load-balance fairness validator (`correctness.load_balance`)

Validates that all participating receivers received a fair share of the
stream after drain.

```yaml
correctness:
  load_balance:
    receivers: [tcp-sink-a, tcp-sink-b, tcp-sink-c]
    min_share_ratio: 0.75   # smallest_count / largest_count >= 0.75
    min_sample_size: 100    # skip the check if total records < N (small samples produce noisy ratios)
```

After drain, the harness reads per-receiver counts, computes
`min(counts) / max(counts)`, and fails the case if the ratio is below
`min_share_ratio`. The failure message lists every receiver's count so
the imbalance is debuggable at a glance.

`receivers:` defaults to "every receiver in the case" if omitted. The
explicit list is for cases where only a subset of receivers participate
in load balancing.

Result JSON gains:

```json
"load_balance": {
  "min_share_ratio_observed": 0.81,
  "min_share_ratio_required": 0.75,
  "pass": true,
  "per_receiver_counts": { "tcp-sink-a": 850, "tcp-sink-b": 690, "tcp-sink-c": 740 }
}
```

---

## In-topology Vault secret store (`vault:`)

A case can add a HashiCorp Vault dev server to the topology so a subject's
secret-store integration (e.g. VirtualMetric DataStream's `hashicorpvault`
credential provider) can be exercised end-to-end without real
infrastructure. Mirrors the `kafka:` supporting-service pattern: the
harness renders a `vault` service plus a one-shot `vault-init` that seeds
the declared secrets, and the subject gates on the seeding completing
(`service_completed_successfully`), so config-load-time secret resolution
never races it.

```yaml
vault:
  image: "hashicorp/vault:1.20"   # optional, default shown (needs >= 1.12)
  token: "pipebench-dev-root"     # optional dev root token, default shown
  mount: "secret"                 # optional KV mount, default shown (dev mode auto-enables it as KV v2)
  secrets:                        # required: path -> field -> value
    bench/http-auth:
      username: "bench-user"
      password: "bench-pass-12345"
```

How it runs:

- The server runs in dev mode with TLS (`-dev-tls`): it generates its own
  CA + leaf into `/vault/tls` (a per-run host dir), with SAN `vault` so
  the chain validates for `https://vault:8200` inside the bench network.
  Subjects whose secret providers are HTTPS-only work unmodified.
- The harness bind-mounts that dir read-only into the subject at
  `/vault-tls`, so the CA's in-container path is `/vault-tls/vault-ca.pem`.
  How a subject references it depends on its config convention — vmetric's
  `ca_name` takes a path relative to the service root (the directory holding
  the binary, `/` in the bench images), so it's written without the leading
  slash: `ca_name: "vault-tls/vault-ca.pem"`.
- `vault-init` waits for the server's healthcheck, then seeds each
  declared path via `vault kv put -mount=<mount> <path> @<file>`. Secret
  values travel from `case.yaml` into per-run `0600` JSON files — they
  never appear in the compose file, a command line, or `docker inspect`
  output. Paths, field keys, mount, and token are charset-validated at
  case load (`[A-Za-z0-9/_.-]`) because they do render into the compose
  file.
- The token is a deterministic test-only value (`VAULT_DEV_ROOT_TOKEN_ID`);
  the bench Vault holds nothing but the seeded test secrets and exists
  for the lifetime of one run.

Rules:

- `secrets:` must declare at least one path, and every path at least one
  field.
- `vault`, `vault-init` are reserved — an `endpoints:` entry can't use
  those names.
- Composes with `kafka:`: a case may declare both blocks; the subject
  then gates on both init containers.

---

## Compatibility

None of the schema described on this page exists in any case in `cases/`
in the current repo. Existing case files don't reference any of the new
keys, and the compose templates emit the same shape for them they
always have. New keys are silently ignored by older versions of the
harness — adopt them at the same time you adopt a harness build that
supports them.
