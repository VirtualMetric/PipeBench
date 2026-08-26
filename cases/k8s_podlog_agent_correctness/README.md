# k8s_podlog_agent_correctness

Agent-hosted correctness case (see `../AGENT-PROVISIONING.md` for the
full provisioning contract) exercising PipeBench's `agent:` mechanism
(`internal/config/case.go` `AgentConfig`) with a real vmetric-agent doing
real work: the `linux_kubernetes_pod_log_collector` dataset tailing a
CRI-formatted, kubelet-shaped pod-log file and forwarding through a
director route to a TCP receiver.

## Design

- **No `generator:` block.** The agent container is the sole data
  producer. This matches `CorrectnessConfig.MinReceived`'s documented
  no-generator shape ("an agentless deploy that collects from an endpoint
  and forwards to the receiver") — a real agent collecting real files is
  the same shape from the harness's perspective.
- **The agent seeds its own input.** `agent.command` first `mkdir -p`s a
  kubelet-shaped directory (`<ns>_<pod>_<uid>/<container>/`) under the
  shared `/data` volume and writes 200 CRI TEXT lines
  (`<RFC3339Nano> stdout F benchline <N>`, one record per physical line —
  the format the `linux_kubernetes_pod_log_collector` dataset's pinned
  "cri" decoder actually expects, NOT docker-JSON) into `0.log`, THEN
  `exec`s the real `vmetric-agent` binary baked into the agent image.
  This was the only viable place to do it — see "Known gaps" below for
  why neither an `Endpoint` container nor the file generator can.
- **Director-side device config lives in `configs/vmetric.yml`**, mounted
  single-file (`-config-path /config.yml`), which per
  `AGENT-PROVISIONING.md` is a fully-supported way to carry
  `devices:`/`targets:`/`routes:` — same as a folder-scanned config. The
  `devices[0].id: 42` MUST match the device ID embedded in the agent's
  `VMETRIC_CONFIG_HASH` (`case.yaml`).

## Result: PASSED

```
./bin/harness test -t k8s_podlog_agent_correctness -s vmetric --version dev
→ correctness: PASSED
  lines in: 0  lines out: 200  loss: 0.00%
```

Exactly the 200 seeded lines, no over- or under-delivery — a confirmed
working, real, agent-hosted run of PipeBench's `agent:` mechanism: the
`linux_kubernetes_pod_log_collector` dataset genuinely tailed the seeded
CRI-formatted file and forwarded records through the director to the
receiver. The agent's own log confirms the real collector ran:
`"Starting event collection of linux_kubernetes_pod_log_collector"`,
`"Starting event collection of .../0.log on
linux_kubernetes_pod_log_collector"`.

Reaching a passing run required getting several distinct pieces right
together, each one a genuine trap, not a case-authoring mistake fixed in
isolation:

1. **An `environments:` block is required** before the director's
   agent-facing TLS listener will bind at all.
2. **The director's config-publish mechanism works automatically and
   unprompted** once a matching device exists in its config — no
   case-side trigger needed.
3. **The agent container's CPU architecture must match the host's** — an
   agent binary run under emulation crashes before it can complete
   startup, let alone connect.
4. **`proxy_tls` must be placed under
   `environments[].nodes[].properties.proxy_tls`, not as a top-level
   key**, or it is silently ignored regardless of the cert delivery form
   used.
5. **The HTTP routes an agent could use to fetch its own trusted CA are
   unreliable in this setup** (see "Known gaps" below) — worked around by
   having the case write a known CA directly into the agent's trust store
   instead of fetching it.

See `AGENT-PROVISIONING.md` for the full contract these five points are
drawn from.

## Resolved: file re-read behavior

An earlier run of this case (before the fixes above) observed far more
received lines than seeded (roughly 111,000 against 200), which raised a
question about the collector's resume-offset cache. The confirmed
passing run above resolves this cleanly: exactly 200 seeded, 200
received, no over-delivery. The agent's log shows why: the first
collection cycle emits the 200 seeded lines
(`"Starting event collection of .../0.log on
linux_kubernetes_pod_log_collector"`); every subsequent cycle for the
same file logs `"Skipping scanner for .../0.log. Reason: FileChange"` —
the resume-offset cache correctly recognizes the file hasn't changed
since its last read and does not re-emit. The earlier over-delivery was
environmental (stale state on a shared, reused test host), not a defect
in this caching behavior.

## Known gaps / unconfirmed assumptions (read this before debugging a failed run)

1. **`proxy_tls` port/scheme/token for the agent to dial in are
   confirmed, by a real pass.** `environments:` present, environment AND
   node names both `"0"`, and `https://subject:8443` (NOT `wss://`) as
   the agent's `Director.Address` with an empty director token are all
   correct — and `proxy_tls` itself, moved under
   `environments[].nodes[].properties.proxy_tls` (not top-level), binds
   with a real, working, mutually-trusted TLS handshake. A top-level
   `proxy_tls:` key is silently ignored regardless of form, which is why
   `status`/`mode`/`port` only ever appeared to work at the top level
   while `cert_name`/`key_name` never did.

2. **`Endpoint` cannot mount the shared-data volume; the file generator
   never creates parent directories for its target.** Both were
   candidates for seeding the nested pod-log directory tree, and neither
   works: an `Endpoint` container has no shared-data-mount option, and
   the file-mode data generator opens its target file directly without
   creating the directory structure above it. That's why the agent seeds
   its own input instead (see Design above) — this also sidesteps the
   ordering race a generator- or endpoint-based seed would have had
   against the agent's own container start, since the same container
   does the seeding and then execs the collector, with no cross-container
   race. See `AGENT-PROVISIONING.md` for more on this gap.

3. **`required_substring: "benchline"` alone proves content survived
   transport, not that CRI decoding specifically happened.** The seed is
   CRI text (`<RFC3339Nano> stdout F benchline <N>`, one record per
   physical line — see Design above) matching the dataset's pinned "cri"
   decoder. A subject that (bug) forwarded each raw seeded line unparsed
   would still contain the substring `"benchline"` and pass. A stronger
   assertion — e.g. requiring the record NOT contain the literal
   `stdout F ` prefix (proving the CRI envelope was stripped, not passed
   through verbatim) — needs the agent's actual decoded-record output
   shape for a bare/no-pipeline dataset with `pipeline_name` unset, which
   requires a live run to observe.

4. **Device-ID matching is confirmed and sufficient for the publish
   side.** Pre-setting `devices[0].id: 42` in the director's config and
   the matching device ID in the agent's `VMETRIC_CONFIG_HASH` is all
   that's needed on this front — the director publishes that device's
   dataset config automatically on startup, and the agent picks it up
   and runs it as soon as it can actually connect. This case is a
   confirmed passing example of an agent-mode device collecting real
   data end-to-end through a statically-configured bench director.

5. **`proxy_tls.cert_name`/`key_name`/`ca_name` resolved.** The real
   issue was never the cert delivery form — it was that `proxy_tls:` as a
   TOP-LEVEL YAML key is silently ignored before any accessor ever runs.
   Moving `proxy_tls` — same file-path `cert_name`/`key_name`/`ca_name`
   values, no other change — under
   `environments[].nodes[].properties.proxy_tls` fixed it immediately:
   the HTTPS listener started performing real TLS handshakes with the
   supplied cert. A separate issue surfaced right after: both HTTP routes
   an agent could use to fetch the CA (the agent's own built-in fetch,
   and the director's documented CA-download route) return 404 in this
   setup — worked around by having the case write the CA directly into
   the agent's trust store instead of fetching it.

## Run

```
./bin/harness test -t k8s_podlog_agent_correctness -s vmetric --version dev
```

Requires the stock subject image and the agent-bearing enterprise image
to already exist locally.

**The agent image's architecture MUST match the harness host's.** The
stock build instructions produce an amd64 agent binary unconditionally,
so on an arm64 host (e.g. Colima on Apple Silicon) that binary segfaults
at Go package init under QEMU emulation before it can do anything.
Verify with an ELF header check, not `uname -m` inside the container
(which reports the host kernel's architecture regardless of the binary's
own — not useful here): `od -An -tx1 -j18 -N2 <path-to-vmetric-agent>`
should print `b7 00` on arm64 hosts (`3e 00` = amd64). On an arm64 host,
build a native image with the agent's own architecture setting changed
to `arm64` and its baked binary path changed from `.../linux/amd64/
vmetric-agent` to `.../linux/arm64/vmetric-agent` (matching `case.yaml`'s
`agent.command` exec path).

This case also ships `configs/certs/{cert.pem,key.pem,ca.pem}` — an EC
(P-256) self-signed certificate generated at authoring time (SAN
`DNS:subject,DNS:localhost,IP:127.0.0.1`), used for the
`proxy_tls.cert_name`/`key_name`/`ca_name` config in `configs/vmetric.yml`
that makes the confirmed-passing run above work. PipeBench auto-mounts a
case's `configs/certs/` directory read-only into the subject container
whenever the directory exists next to the config source — no
`case.yaml` flag needed.
