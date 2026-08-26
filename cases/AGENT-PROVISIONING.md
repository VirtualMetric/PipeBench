# Authoring Agent-Hosted Cases

This is the contract for writing a PipeBench case that runs a real
`vmetric-agent` process via the `agent:` block in `case.yaml`, rather than
feeding a director-fronted listener directly. It covers: the provisioning
env var, the director-side config shapes an agent needs to connect and
receive its device definition, the TLS/trust bootstrap workaround this
setup requires, the container-arch requirement, and the known observable
gaps to plan around. See `cases/k8s_podlog_agent_correctness/` for a
working, passing example of everything below.

## 1. `VMETRIC_CONFIG_HASH`

The agent (and, with a different field shape, a serverless/fleet
director) is provisioned via a single environment variable,
`VMETRIC_CONFIG_HASH`, set at container start. The agent decodes it once
on first start and never reads it again — it's consumed, then persisted
to the agent's own local `vmetric.yml`.

**Agent-mode shape**: base64 of a `;`-delimited string with at least 4
fields:

```
base64("<directorAddress>;<directorToken>;<deviceID>;<enrollmentID>")
```

| Index | Field | Notes |
|---|---|---|
| 0 | Director address | URL, **must** use `http`/`https` scheme, not `ws`/`wss` — the agent's own address validator rejects `ws`/`wss` outright. Internally the agent converts `https` to `wss` (plus a `/ws` path) for the actual connection. |
| 1 | Director token | May be empty if the target device has no auth configured. |
| 2 | Device ID | Optional int, but at least one of Device ID / Enrollment ID must be non-zero. |
| 3 | Enrollment ID | Optional int. Pre-setting a known device ID and leaving this `0` is the supported way to skip an interactive enrollment handshake in a scripted context. |

A malformed hash fails the agent's startup immediately (loud failure, not
a silent no-op) before any other agent subsystem starts.

Instead of the env var, a case can also write the agent's own
`vmetric.yml` directly (same fields: `device.id`, `director.address`,
`director.token`), which is useful when the case also wants to set a
`debug:` block the decode-and-boot path doesn't expose a way to set (see
§6). Either form produces the same runtime result once the agent starts.

## 2. Director-side config required for an agent to connect

### 2a. `environments:` is required for the agent-facing listener to bind

The director's TLS listener that agents dial into (`proxy_tls`) **never
binds at all** — no error, no timeout, just an indefinite wait — while
the parsed system config has zero `environments:` entries. An
`environments:` block is not optional decoration; it's required any time
an agent needs to connect.

### 2b. Environment and node names must match the director's self-identity

The listener additionally requires an environment `name` and, within it,
a node `name` that exactly match this director's own self-identity.
With no `director.id`/`node.name` configured, a director's own name
defaults to `"0"` for both — do not assume any other default (e.g. `"1"`)
without checking; wrong names produce the same generic "can not find
node information" failure as no `environments:` block at all. Verify the
correct values via the director's own `/health` endpoint, which reports
its resolved node name.

### 2c. `proxy_tls` must live under `nodes[].properties`, not top-level

A **top-level** `proxy_tls:` key in a director's config parses without
error and can even look like it's partially working (`status`/`mode`/
`port` appear honored) — but in director/agent mode it is silently
ignored before anything reads it: local per-service TLS settings are
platform-delivered-only and are stripped from a locally-supplied config.
The only path that survives is under a matching node's `properties:`
block, which is merged into the live config. This applies specifically
to `proxy_tls`; `debug:` is not subject to the same stripping, which is
why a top-level `debug:` block behaves as expected while a top-level
`proxy_tls:` block quietly does nothing.

Confirmed working shape:

```yaml
environments:
  - name: "0"        # MUST match this director's self-identity — verify
    status: true      # via its own /health endpoint's resolved node
    nodes:              # name; defaults to "0" only when no
      - name: "0"          # director.id/node.name is configured.
        status: true
        properties:
          proxy_tls:
            status: true
            mode: self-signed
            port: 8443
            # Resolves relative to the binary's working dir against
            # configs/certs/, auto-mounted read-only into the container
            # by PipeBench whenever a case's configs/certs/ directory
            # exists — no case.yaml flag needed (see §3).
            cert_name: certs/cert.pem
            key_name: certs/key.pem
            ca_name: certs/ca.pem
```

### 2d. The device definition the agent will run

The director's config (single-file `-config-path` mode, same
`devices:`/`targets:`/`routes:` shape as a folder-scanned config) needs a
`devices:` entry whose `id:` matches the device ID embedded in the
agent's `VMETRIC_CONFIG_HASH` (§1, field 2), with a `definitions:` block
naming the desired collector:

```yaml
devices:
  - id: <N>
    name: <device-name>
    type: linux
    status: true
    definitions:
      - name: linux_kubernetes_pod_log_collector
        status: true
        inputs:
          - name: pod-logs
            properties:
              path: "/data/pods/*/*/*.log"
              pipeline_name: "<pipeline-name-or-empty>"
targets: [...]
routes:
  - devices: [{name: <device-name>}]
    targets: [...]
```

Once the device ID matches, the director publishes that device's config
to the connecting agent automatically on startup — no case-side trigger
is needed beyond the device existing in the director's config.

For a pod-log collector specifically, the input format is CRI text, one
record per physical line: `<RFC3339Nano> <stdout|stderr> <P|F>
<message>`. The dataset pins decoder `cri`, so a docker-JSON envelope
will not decode even if it is otherwise valid JSON.

## 3. Cert delivery

A case ships its own cert material under `configs/certs/` (e.g.
`cert.pem`, `key.pem`, `ca.pem`). PipeBench auto-mounts that directory,
read-only, into the subject container whenever it exists next to the
case's config source — no `case.yaml` flag needed. `cert_name`/
`key_name`/`ca_name` under `proxy_tls` (§2c) then resolve against it.

## 4. Agent trust bootstrap workaround

Once `proxy_tls` is correctly placed (§2c) and presenting a real
certificate, the agent still needs to trust it. Both HTTP routes that
could deliver that trust automatically are observed to fail in this
setup:

- The agent's own built-in CA-fetch (a `/dl/cert.pem`-style path) returns
  404 — the path it requests does not exist on this director build.
- The director's documented CA-download route (a `/ca.pem`-style path,
  gated on self-signed mode + discovery being enabled, both true by
  default here) **also** 404s, even though a sibling route under the
  identical gate returns 200 in the same container in the same run. Not
  fully root-caused.

**Workaround**: since a case already knows its own CA at authoring time
(it's the same file used for `proxy_tls.ca_name` in §2c), have the
agent's container command write that CA directly to the agent's expected
local trust-store path (`storage/cert/ca.pem`, relative to the agent
binary's working directory) before exec'ing the agent binary, instead of
relying on either HTTP route.

## 5. Container architecture must match the host

The agent binary's CPU architecture must match the container host's. An
amd64 agent binary run under QEMU emulation on an arm64 host (e.g.
Apple Silicon via Colima) crashes with `SIGSEGV` during Go runtime
package initialization, before `main()` runs and before any I/O —
meaning it never gets far enough to attempt a connection at all. Build
or select an arch-native agent image rather than relying on a
convention-based path.

To verify a binary's actual architecture, check its ELF header rather
than `uname -m` inside the container (which reports the host kernel's
architecture regardless of the binary's own):

```
od -An -tx1 -j18 -N2 <path-to-vmetric-agent>
```

`b7 00` indicates arm64; `3e 00` indicates amd64.

## 6. Diagnostics

Setting `debug: {level: 5, console: {status: true}}` in the director's
config surfaces verbose console logging useful for diagnosing
provisioning problems (config-publish activity, device agent-mode
handling, TLS cert resolution). Note that `console.status: true` alone
is not sufficient — the default log level filters out everything below
Error/Warning, so `level: 5` (Verbose) is required as well.

## 7. Known observable gaps to plan around

- **A one-shot endpoint container cannot seed the shared-data volume.**
  Endpoint containers in this harness have no shared-data mount support,
  and a generic file-mode data generator does not create parent
  directories for its write target — it fails immediately if the
  directory doesn't already exist. Of the container types available to a
  case, only the agent service itself can both mount shared data and
  have its command overridden. If a case needs nested directory
  structure seeded before an agent-hosted collector can read it, do the
  seeding from inside the agent container's own command, before
  exec'ing the real agent binary — this also avoids any cross-container
  startup-ordering race, since the same container does both steps in
  sequence.
- **The director's readiness is not guaranteed by container-start
  ordering alone.** A `depends_on: {condition: service_started}`
  relationship only guarantees the director's process has started, not
  that it has finished parsing config and binding `proxy_tls` yet. Have
  the agent's command wait (e.g. poll with a TCP connect check, with a
  generous timeout and a fail-open fallback) for the director's agent
  port to actually accept connections before exec'ing the agent binary.
- **The two 404 routes in §4** are a live backend gap, not a
  misconfiguration — plan for the trust-store workaround rather than the
  HTTP fetch path.

## Summary: confirmed-working shape

```text
Director-side config.yml (mounted at /config.yml, -config-path /config.yml):
  environments:
    - name: "0"   # match this director's self-identity — see §2b
      status: true
      nodes:
        - name: "0"
          status: true
          properties:
            proxy_tls:            # NOT top-level — see §2c
              status: true
              mode: self-signed
              port: 8443
              cert_name: certs/cert.pem
              key_name: certs/key.pem
              ca_name: certs/ca.pem
  devices:
    - id: <N>
      name: <device-name>
      type: linux
      status: true
      definitions:
        - name: linux_kubernetes_pod_log_collector
          status: true
          inputs:
            - name: pod-logs
              properties:
                path: "/data/pods/*/*/*.log"
                pipeline_name: "<pipeline-name-or-empty>"
  targets: [...]
  routes:
    - devices: [{name: <device-name>}]
      targets: [...]

Agent container (image rebuilt/selected to match the harness host's
architecture — see §5):
  command override -> the baked agent binary for the matching arch
    (the enterprise image's default entrypoint runs the director, not
    the agent), preceded by:
      - any input seeding the case needs (§7)
      - writing the agent's own bootstrap vmetric.yml (or relying on
        VMETRIC_CONFIG_HASH — see §1), with device.id and
        director.address = https://<subject-host>:<proxy_tls.port>
      - writing the known CA directly into the agent's trust store
        path (storage/cert/ca.pem) — see §4
  env:
    VMETRIC_CONFIG_HASH: base64("https://<subject-host>:<proxy_tls.port>;<director-token>;<N>;0")
  mounts_shared_data: true   # if the agent needs to seed /data itself
```
