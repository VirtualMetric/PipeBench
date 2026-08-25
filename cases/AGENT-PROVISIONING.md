# Agent Provisioning Contract (derived from virtualmetric-backend, read-only spike)

> **Update — confirmed by a real run.** Every assumption in §7b below (an
> explicit `proxy_tls: {status: true, mode: self-signed, port: 8443}` block
> in the director's single-file config, `wss://subject:8443` as the agent's
> `Director.Address`, and an EMPTY `Director.Token`) was validated end-to-end
> by `cases/k8s_podlog_agent_correctness`: `./bin/harness test -t
> k8s_podlog_agent_correctness -s vmetric --version dev` → **PASSED**
> (`correctness: PASSED`, `loss: 0.00%`, `lines_out: 111000`,
> `min_received: 150` and `required_substring: "benchline"` both satisfied).
> The average received line size (~209.6 bytes) was ~2.5x the raw seeded CRI
> JSON line (~81 bytes), consistent with the agent actually CRI-decoding and
> re-shaping each record (not passing the raw `{"time","stream","log"}` JSON
> through byte-for-byte). See `cases/k8s_podlog_agent_correctness/README.md`
> for the one caveat this run surfaced (line-count over-delivery, not a
> correctness failure — the case's thresholds don't penalize it, but it's an
> open question about the reader's resume-offset cache in this container
> shape) and for the one non-backend-code environment blocker that had to
> clear first (`docker compose` CLI plugin missing on the harness host,
> unrelated to the case or backend code — resolved outside this agent's
> action, see README "Environment note").

PipeBench's `internal/config/case.go` `AgentConfig` comments admit the harness
has never exercised this path for real. This doc is the contract that was
missing: how a `vmetric-agent` actually registers with a director and starts
collecting, traced end-to-end from the backend source
(the `virtualmetric-backend` source, read-only). Every
claim below is a file:line citation, not inference from docs.

## 1. The install one-liner and `VMETRIC_CONFIG_HASH`

Both the **agent** one-liner and the **serverless-director** deploy
templates work the same way: a shell script downloads the binary and sets
`VMETRIC_CONFIG_HASH` in the environment; the binary decodes it on first
start and never sees it again (it's consumed once, then persisted to disk).

- `web/api/director/installscripts.go` builds the **director** (not agent)
  bootstrap one-liner via `toolkit.GetInstallerURL` (`toolkit/product.go:147`)
  — `https://<host>/dl`, environment-selected (prod vs dev host).
- `migrations/seed_serverless_install_templates.sql` shows the **serverless
  director** deploy templates (docker-compose, k8s Deployment, ACI, ACA) all
  inject the SAME env var, `VMETRIC_CONFIG_HASH`, sourced from
  `{{.EncodedAPIKey}}` — e.g. the k8s template stores it in a Secret and
  wires it in via `secretKeyRef`.
- The **agent** install path uses the identical env var name — confirmed by
  the constant `_configHash = "VMETRIC_CONFIG_HASH"` duplicated in both
  `service/agent/main.go:25` and `service/director/main.go:26`.

## 2. What `VMETRIC_CONFIG_HASH` actually decodes to

Decoding happens in `helper/service/config.go`, function `RunConfigurator`
(line 111). It is base64-decoded, then split on `;`. **Two different shapes
exist depending on which binary consumes it** — the function branches on
`mode == "agent"`:

### 2a. Agent-mode shape (`RunConfigurator(hash, "agent")`, line 113-172)

Semicolon-delimited fields, **≥4 required**:

| Index | Field | Struct written |
|---|---|---|
| 0 | Director address (URL) | `DeviceConfig.Director.Address` |
| 1 | Director token | `DeviceConfig.Director.Token` |
| 2 | Device ID (optional, int64) | `DeviceConfig.Device.ID` |
| 3 | Enrollment ID (optional, int64) | `DeviceConfig.Enrollment.ID` |

At least one of Device.ID / Enrollment.ID must be non-zero (line 149) or
`RunConfigurator` errors out. The decoded struct
(`model.DeviceConfig`, `model/device_config.go`) is YAML-marshaled and
written to `<currentPath>/vmetric.yml` (`writeToYAMLFile`, config.go:427 —
`toolkit.ServiceName + ".yml"`), replacing any existing file. This is the
**agent's own bootstrap config file**, distinct from the director-side
device definition.

The **encoder** for this exact format lives in
`helper/vmmq/tools/tools.go:179` (`ReadConfigHash`):
```go
configHash := fmt.Sprintf("%s;%s;%d;%d", apiURL, token, deviceID, 0)
return base64.StdEncoding.EncodeToString([]byte(configHash))
```
i.e. `base64("<directorAddress>;<directorToken>;<deviceID>;0")` — field 3
(enrollment ID) is always `0` when a real device ID is already known, which
is exactly the bench-agent shortcut noted in
`helper/vmmq/server/vserver/authorization_test.go:668-670`:

> "The bench agent cases cannot catch this: they pre-set the device id in
> `VMETRIC_CONFIG_HASH` and skip enrollment entirely."

This is a load-bearing precedent: pre-setting a device ID in the hash is
the SUPPORTED way to skip the interactive enrollment handshake in a
scripted/bench context — it's not a hack PipeBench would be inventing.

### 2b. Non-agent (director/fleet) shape (line 175-224)

`instance;directorID;token[;selfManagedToken]` — 3 or 4 fields, decoding
into `FleetConfig{Fleet:{Instance,Token,Type}, Director:{ID}}`. This is the
shape used by the serverless director templates in §1 and is NOT what an
agent container consumes. (Included here only to avoid the two shapes being
conflated — they share an encoding scheme but not a schema.)

## 3. How the agent container boots with it

`service/agent/main.go:26-36` (`Start()`): on process start, if
`os.Getenv("VMETRIC_CONFIG_HASH")` is non-empty, it calls
`service.RunConfigurator(resolved, "agent")` **before** anything else (before
`NewServiceModel`/health monitor/controller). A failure here calls
`os.Exit(1)` immediately — so a malformed hash fails loud and fast, not as a
silent no-op.

After that, normal agent startup proceeds
(`controller.StartAgent`), which reads the just-written `vmetric.yml`
(`Director.Address`, `Director.Token`, `Device.ID`) to open its VMMQ
(NATS-over-websocket) connection to the director and identify itself as
`DeviceID`.

## 4. Does `cmd/director/Dockerfile.enterprise` bake the agent? Yes.

`cmd/director/Dockerfile.enterprise` (backend repo root as build context)
is a two-stage build:

- Stage `build`: compiles `./cmd/director` (`-tags docker`) **and**, in the
  same stage, `./cmd/agent` (`-tags client`) — see lines 61-73. The comment
  at line 69-72 is explicit about why: building the agent from source in
  the SAME stage keeps it on the SAME vendored deps/Go toolchain as the
  director, rather than risking a stale pre-built copy.
- Final stage: copies the director binary to `/opt/vmetric/director`
  (the stock `vmetric/director` entrypoint path, so it's a drop-in subject
  image) **and** the linux/amd64 agent binary to
  `/opt/vmetric/package/agent/linux/amd64/vmetric-agent` (line 107) — the
  path the director itself would serve an agent installer from
  (`<Root>/package/agent/<os>/<arch>/`).
- `ENTRYPOINT ["/opt/vmetric/director"]` — this image's default command
  always runs the **director**. To run the **agent** binary inside a
  container built FROM this same image (which is exactly what
  `AgentConfig.Image` documents — "vmetric/director-enterprise... avoiding a
  separate published image"), the case must override the container command:
  `["/opt/vmetric/package/agent/linux/amd64/vmetric-agent"]` (or a shell
  wrapper — see §7 below for why a wrapper is actually required here).
- No `vmetric.yml` is baked in for the director role either — the comment
  block at the top of the Dockerfile says the subject mounts the case
  config at `/config.yml` and runs `-config-path /config.yml`. Confirmed in
  `helper/service/config.go:62` (`SetConfigFilePath` doc) and
  `model/config_system.go:582-593` (`CheckSystemConfig`): **single-file mode
  is a first-class, fully-supported way to carry `devices`, `targets`,
  `routes` — the exact same top-level keys that a folder-scanned config
  provides** ("the system config (devices, targets, routes) lives in one
  file, not the Path.Config directory the scanner watches").

## 5. How the agent gets its DEVICE definition (the dataset) after registering

This is the part the task description calls out as needing tracing, and
it's a **pull**, not a push at container-start time:

1. Agent boots with `Device.ID` already known (§2a) — no enrollment
   round-trip needed.
2. Agent connects to the director's VMMQ (NATS JetStream over websocket)
   using `Director.Address` / `Director.Token`.
3. `helper/automation/helpers.go:GetLastConfigMessage` (line 97) is how the
   agent (or a director acting on the agent's behalf) polls for its most
   recent config: it reads the JetStream `vmetric-fleet-req` stream,
   filtered to subject
   `vmetric.fleet.req.agent.director.<DeviceID>.*.config`
   (`GetLastMsgForSubject`), and if found, decodes + executes it through the
   registered "config" `ICommand` — this is what actually applies the
   device's dataset definitions to the running agent process.
4. That message is produced by the **director**, in
   `helper/automation/helpers.go:PublishConfigByID` (line 20): it looks up
   the device's config via `command.GetDeviceConfig(serviceInfo, device.ID)`
   and publishes it to
   `vmetric.fleet.req.agent.director.<deviceID>.<unixTime>.config` via
   `publishConf` (JetStream publish, line 82-92).
5. `PublishConfigByID` is driven off `serviceInfo.Devices()`
   (`model/config_system.go:61`, `map[int64]config.ConfigItem`) — i.e. the
   **director's own parsed system config** (§4's `devices:` key, whether
   from a folder scan or a single `-config-path` file). There is no
   separate "push config to agent" trigger traced here beyond: a device
   exists in the director's config with the given ID, and something calls
   `PublishConfigByID`/the device-collector manager notices a config change
   (`service/director/collector/device_manager.go` — polls every 1s,
   compares `SystemConfigLastUpdate()`).

**Net effect for a bench case**: the director's mounted `/config.yml` must
contain a `devices:` entry whose `id:` matches the device ID baked into the
agent's `VMETRIC_CONFIG_HASH`, with a `definitions:` block naming
`linux_kubernetes_pod_log_collector` — exactly the shape shown in
`config/devices/linux-filelog.example.yaml` (device `type: linux`,
`definitions: [{name: linux_file_log_collector, inputs: [...]}]` — the pod
log collector definition is documented in
`docs/collectors/kubernetes-pod-logs-dataset.md`, format:
```yaml
definitions:
  - name: linux_kubernetes_pod_log_collector
    status: true
    inputs:
      - name: pod-logs
        properties:
          path: "/data/pods/*/*/*.log"
          pipeline_name: "kubernetes-pod-logs"
```
). The director then routes that device's output like any other device —
existing PipeBench cases (`cases/sighup_correctness/configs/vmetric.yml`)
show the `devices:`/`targets:`/`routes:` single-file shape already in
active use, just for a `type: tcp` (director-listener) device rather than a
`type: linux` (agent-fed) device.

## 6. What PipeBench's `agent:` block already renders (confirmed, no gap)

`internal/config/case.go:608-624` (`AgentConfig`) and
`internal/orchestrator/docker.go:705-734` / `:2615-2644` together already
support everything §1-§4 need:

- `image:` — pin to the locally-built `vmetric/director-enterprise:dev`.
- `env:` — set `VMETRIC_CONFIG_HASH` here (base64 computed as in §2a).
- `command:` — overrides entrypoint+command (needed, since the enterprise
  image's default `ENTRYPOINT` runs the director, not the agent — §4).
- `mounts_shared_data: true` — mounts the SAME `shared-data:/data` volume
  the generator/receiver/subject use, at `/data`, running as `0:0`.
- Compose service is named `agent`, network `bench`, `depends_on:
  <subject>: {condition: service_started}` — so it dials in only after the
  director's listeners are bound.
- `applyVersionIfUntagged` (docker.go:2628-2640) applies `--version` to an
  untagged agent image the same way the subject gets one — pin an explicit
  tag (`:dev`) to opt out, same as the documented
  `director_old_agent_compat_deploy` pattern.

**This part of the mechanism is NOT broken or unused-and-bitrotted** — it
renders correctly. What's actually unused is the END-TO-END STORY around
it: no case has ever supplied a real `VMETRIC_CONFIG_HASH` + a matching
director-side device definition together. That composition is what this
spike case adds.

## 7. Two genuine gaps found (not resolved by reading — need a live run or a harness change)

### 7a. `Endpoint` cannot seed files onto the shared-data volume

The task brief's fallback plan ("if the file generator can't write nested
paths, use an endpoints one-shot container to seed the tree") **does not
work with the current harness**: `Endpoint` (`internal/config/case.go:653`)
has no `mounts_shared_data`-equivalent field, and the compose template's
per-endpoint block (`docker.go` endpoint range, ~line 678-704) never mounts
`shared-data:/data` — only the generator/receiver/subject/verifier(local)/
agent(opt-in) services do (grep hits: `docker.go:167,239,330,422,634,653,720`
all belong to those five, never to the `{{- range .Endpoints }}` block).
So an `Endpoint` cannot pre-create
`/data/<ns>_<pod>_<uid>/<container>/` for the generator to write into.

Also confirmed: `containers/generator/main.go`'s file-mode writer
(`runFileSingle`/`runFileParallel`) opens its target with
`os.OpenFile(path, O_APPEND|O_CREATE|O_WRONLY, 0o644)` and **never calls
`os.MkdirAll`** — the parent directory must already exist, or the generator
fails immediately (no retry).

**The only container in this topology that (a) can mount `shared-data`
AND (b) supports a `command:` override is the `agent` service itself**
(`AgentConfig.Command` + `AgentConfig.MountsSharedData`). This case works
around the gap by having the agent's own command wrap
`mkdir -p /data/<pod-log-dir> && exec vmetric-agent ...` before running the
real agent binary — but that only works if the agent container starts
(and completes its mkdir) before the generator's first file open, and
nothing in the harness enforces that ordering between `agent` and
`generator` (no `depends_on` between them). This is a race, not a fix; it's
documented as such in the case README.

**Recommendation for a real (non-spike) fix**: add a
`mounts_shared_data`-equivalent option to `Endpoint` (or a
`pre_seed`/`init` hook independent of the generator/agent timing), so a
one-shot endpoint can reliably create directory structure before ANYTHING
else in the topology starts writing — this is a harness gap, not a vmetric
gap.

### 7b. Director listen address/port and auth token for a from-scratch bench director are unconfirmed

`RunConfigurator`'s `Director.Token` (§2a field 1) is written into the
agent's `vmetric.yml` verbatim and presumably validated by the director's
VMMQ auth layer (`helper/vmmq/server/vserver/authorization.go`) before the
agent's JetStream consumers are authorized.

**Concrete, confirmed finding (not a guess): the raw NATS port is NOT
externally dialable in standalone mode.** `helper/vmmq/server/server.go`,
`resolveVMMQServer` (~line 165-192): the default node config comment says
explicitly —

> "The NATS server binds loopback only and is never externally exposed, so
> address/port are not user-configurable: the client port is disabled in
> standalone (`DontListen`) and the WebSocket port is assigned
> dynamically."

This branch (`resolved.NodeConfig` built from `config.DefaultVMMQAddress` /
`config.DefaultVMMQPort`) is taken whenever `systemConfig.Environments` is
empty (line ~198: `if len(systemConfig.Environments) > 0 { resolved.NodeConfig
= config.ConfigItem{} }` — i.e. only a config WITH an `environments:` block
overrides this default). A bare bench `/config.yml` with just
`devices/targets/routes` (§4, no `environments:` block) is exactly this
"standalone" case — meaning **the internal NATS/WS port an agent would
naively dial is neither fixed nor externally reachable at all.** The
reachable endpoint has to be the director's `proxy_tls` HTTPS/WSS listener,
which fronts/proxies to that internal dynamic port — the same conclusion
implied by every serverless install template (§1) treating `proxy_tls.port`
as mandatory, and by `helper/vmmq/tools/tools.go:GetNATSURL` deriving the
agent-facing NATS URL from `proxy_tls`/`external_url`, never from a raw
node/cluster address.

**Practical implication for this case**: the mounted `/config.yml` almost
certainly needs an explicit `proxy_tls:` block (`status`, `port`, `mode` —
`self-signed` | `custom` | `offloaded`, per
`helper/config/enum.go:65-67`) for the agent to have anything reachable to
dial, and `Director.Address` in the `VMETRIC_CONFIG_HASH` must point at
that `proxy_tls.port`, not a guessed NATS port. **This was not confirmed
empirically** (a live run was needed to nail down the exact scheme/host
agents expect — `ws://`? `wss://`? does the bench director require TLS
verification the harness would need to disable?) — treat this as the
single highest-probability first failure point for a real run, ahead of
the token question. Diagnose via `docker compose logs agent` (dial
failure = wrong address/port; explicit auth rejection = token) and
`docker compose logs subject` (whether `proxy_tls` bound successfully at
all).

## Summary contract (for the case author)

```
Director-side config.yml (mounted at /config.yml, -config-path /config.yml):
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

Agent container (image: vmetric/director-enterprise:dev, built from
cmd/director/Dockerfile.enterprise):
  command override -> the baked
    /opt/vmetric/package/agent/linux/amd64/vmetric-agent binary
    (default ENTRYPOINT runs the director, not the agent)
  env:
    VMETRIC_CONFIG_HASH: base64("<director-address>;<director-token>;<N>;0")
  mounts_shared_data: true   # only if the agent needs to seed /data itself
```
