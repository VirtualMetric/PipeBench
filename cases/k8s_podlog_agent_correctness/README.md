# k8s_podlog_agent_correctness

Spike case (see `../AGENT-PROVISIONING.md` for the full derived contract)
exercising PipeBench's `agent:` mechanism (`internal/config/case.go`
`AgentConfig`) for the first time with a real vmetric-agent doing real
work: the `linux_kubernetes_pod_log_collector` dataset tailing a
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
  shared `/data` volume and writes 200 CRI-formatted JSON lines into
  `0.log`, THEN `exec`s the real `vmetric-agent` binary baked into
  `vmetric/director-enterprise:dev`. This was the only viable place to do
  it — see "Known gaps" below for why `Endpoint` and the file generator
  both can't.
- **Director-side device config lives in `configs/vmetric.yml`**, mounted
  single-file (`-config-path /config.yml`), which per
  `AGENT-PROVISIONING.md` §4-5 is a fully-supported way to carry
  `devices:`/`targets:`/`routes:` — same as a folder-scanned config. The
  `devices[0].id: 42` MUST match the device ID embedded in the agent's
  `VMETRIC_CONFIG_HASH` (`case.yaml`).

## Result: PASSED (real run)

```
./bin/harness test -t k8s_podlog_agent_correctness -s vmetric --version dev
→ correctness: PASSED
  lines in: 0  lines out: 111,000  loss: 0.00%
  throughput: 5,887 lines/s
```

`min_received: 150` and `required_substring: "benchline"` both satisfied.
`vmetric/director:dev` (stock subject) + `vmetric/director-enterprise:dev`
(agent image, built locally from `cmd/director/Dockerfile.enterprise`) were
used. This is believed to be the first real, passing exercise of
PipeBench's `agent:` mechanism.

**Environment note (not a case or backend bug):** the first attempt failed
before any container started, with `docker: unknown shorthand flag: 'f' in
-f` from every `docker compose -f ...` invocation — the `docker compose`
v2 CLI plugin was not installed on this harness host
(`~/.docker/cli-plugins/` did not exist). This blocks every case on this
harness, not just this one. It resolved itself between the first and
second attempt (plugin appeared) without action from this case's author;
if you hit the identical error, `docker compose version` is the fastest
way to confirm this specific cause before looking at the case itself.

**Fix applied before the passing run:** the agent's seed-and-exec `command`
originally ended with `... vmetric-agent -mode agent`. Reading
`cmd/agent/main.go`'s flag registration (`vmFlags.Mode_` only accepts
`supervisor`/`update`/`restart`/`console` — there is no `agent` mode value,
because `cmd/agent` IS the agent; running it bare falls through to
`runAgent()`) showed this flag was invalid and would have failed to parse.
Removed — the command now just `exec`s the binary with no arguments,
matching how the stock director subject is also invoked with no
`-service`/`-mode` flags in this harness.

## Open question this run surfaced (not a correctness failure)

**Line-count over-delivery**: only 200 CRI-formatted lines were seeded into
`0.log`, once, before the agent started — the file is never appended to
again. The receiver observed **111,000** lines (555x). The case's
correctness gate (`min_received: 150` + `required_substring`) doesn't
penalize this (no `expected_loss_pct`/dedup validation was configured,
since this is a no-generator case), so the run still PASSED, but it
indicates the dataset's per-cycle collection
(`docs/collectors/kubernetes-pod-logs-dataset.md` §2, "Read model": globs
`path`, reads every matched file "once" per cycle) is **re-reading the same
200 lines from the start on every cycle** rather than resuming from a
persisted offset, in this specific container shape (ephemeral, no
persistent volume for the reader's own resume-offset cache — only `/data`
is a named volume; `/opt/vmetric/storage` where that cache would normally
live is the container's writable overlay layer, gone if the container
restarts, but that alone doesn't explain re-reads WITHIN a single
container's uptime). Worth a follow-up run with `docker logs bench-agent`
captured live (this spike's run didn't retain it — the harness's fixed
`bench-agent` container name was reused by a concurrent run on this shared
host before it could be inspected) to see whether `ignore_cache` is
defaulting unexpectedly, or whether the cache write path itself is
failing/warning.

## Known gaps / unconfirmed assumptions (read this before debugging a failed run)

1. ~~**`proxy_tls` port/scheme/token for the agent to dial in are
   best-guess, not confirmed.**~~ **CONFIRMED by the passing run** —
   `wss://subject:8443` with an empty director token, against
   `proxy_tls: {status: true, mode: self-signed, port: 8443}` in
   `configs/vmetric.yml`, is a working combination end-to-end. The
   reasoning in `AGENT-PROVISIONING.md` §7b (standalone director disables
   its raw NATS port; `proxy_tls` is the externally-reachable endpoint) is
   validated, not just inferred, as of this run.

2. **`Endpoint` cannot mount the shared-data volume; the file generator
   never `MkdirAll`s its target.** Both were candidates (per the original
   spike brief) for seeding the nested pod-log directory tree, and both
   were confirmed (by reading `internal/config/case.go` and
   `internal/orchestrator/docker.go`) not to work: `Endpoint` has no
   `mounts_shared_data`-equivalent field/template branch, and
   `containers/generator/main.go`'s file-mode writer opens its target with
   `O_CREATE` only, never creating parent directories. That's why the
   agent seeds its own input instead (see Design above) — this sidesteps
   the ordering race a generator/endpoint-based seed would have had
   against the agent's own container start, since the SAME container does
   the seeding then execs the collector, with no cross-container race.
   **Harness recommendation**: add a shared-data mount option to
   `Endpoint`, or a dedicated pre-topology seed hook — see
   `AGENT-PROVISIONING.md` §7a.

3. **`required_substring: "benchline"` proves content survived transport,
   not that CRI decoding specifically happened.** A subject that (bug)
   forwarded the raw JSON lines unparsed would still contain the substring
   `"benchline"` and pass. A stronger assertion — e.g. requiring the
   record NOT contain the literal `"stream":"stdout"` JSON key (proving
   the envelope was stripped, not passed through) — needs the agent's
   actual decoded-record output shape, which requires a live run to
   observe (`docs/collectors/kubernetes-pod-logs-dataset.md` describes the
   *input* decode semantics but not the exact downstream field names for a
   bare/no-pipeline dataset with `pipeline_name` unset, which is what this
   case uses). Tighten this once a real run shows the decoded output.

4. ~~**Device-ID/token provenance for a from-scratch bench director has no
   precedent in this repo.**~~ **CONFIRMED**: pre-setting `devices[0].id: 42`
   in the director's config and the matching device ID in the agent's
   `VMETRIC_CONFIG_HASH` (§2a's field 2) is sufficient — no enrollment
   round-trip needed, exactly as
   `helper/vmmq/server/vserver/authorization_test.go`'s comment predicted.
   This case is the first passing implementation of that pattern in this
   repo's case set.

## Run

```
./bin/harness test -t k8s_podlog_agent_correctness -s vmetric --version dev
```

Requires `vmetric/director:dev` (stock subject image) and
`vmetric/director-enterprise:dev` (agent image, built from
`cmd/director/Dockerfile.enterprise` in the backend repo — bakes both the
director and the linux `vmetric-agent` binary) to already exist locally.
