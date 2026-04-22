# PipeBench — Kubernetes Guide

Run data pipeline benchmarks on any Kubernetes cluster. Compare **Vector**, **Fluent Bit**, **Fluentd**, **Logstash**, **Filebeat**, **Cribl Stream**, **Telegraf**, **Splunk HF**, **NXLog**, **AxoSyslog**, **Tenzir**, **OpenTelemetry Collector**, **Grafana Alloy**, **BindPlane Agent**, and more with fair resource isolation using Guaranteed QoS pods.

> This guide covers Kubernetes deployments. For local Docker testing, see [README-DOCKER.md](README-DOCKER.md).

---

## Quick Start Guide (from scratch)

This guide assumes you have a Linux machine (Ubuntu 22.04 or 24.04) with no Kubernetes tooling installed. If you are coming from a Windows background, every command here is copy-paste ready — just run them in your terminal (SSH or `Ctrl + Alt + T` on desktop).

### Step 1: Update the system

```bash
sudo apt update && sudo apt upgrade -y
```

### Step 2: Install Docker

Docker is needed to build the helper container images (generator, receiver, collector). Even though the tests run on Kubernetes, the images must be built first.

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

Add yourself to the docker group so you do not need `sudo`:

```bash
sudo usermod -aG docker $USER
```

**Log out and log back in**, then verify:

```bash
docker run --rm hello-world
```

### Step 3: Install kubectl

`kubectl` is the command-line tool for talking to Kubernetes clusters.

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl

# Verify
kubectl version --client
```

### Step 4: Set up a Kubernetes cluster

You need a running cluster that `kubectl` can reach. Pick one of the options below.

#### Option A: Local cluster with k3s (easiest, single-node)

k3s is a lightweight Kubernetes distribution that installs in 30 seconds. Good for trying things out on a single machine.

```bash
curl -sfL https://get.k3s.io | sh -

# k3s writes its kubeconfig to a protected file. Copy it to the standard location:
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER:$USER ~/.kube/config

# Verify
kubectl get nodes
```

You should see one node in `Ready` state.

#### Option B: Local cluster with kind (Docker-in-Docker)

kind runs a Kubernetes cluster inside Docker containers. Good if you already have Docker and want something disposable.

```bash
# Install kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-amd64
sudo install -o root -g root -m 0755 kind /usr/local/bin/kind
rm kind

# Create a cluster
kind create cluster --name bench

# Verify
kubectl get nodes
```

#### Option C: Existing remote cluster

If you already have a cluster (EKS, GKE, AKS, on-prem), make sure your kubeconfig is set up:

```bash
# Verify you can reach it
kubectl get nodes
```

The cluster needs at least **4 GB of allocatable memory** and the ability to create namespaces, ConfigMaps, Services, Deployments, and Jobs.

### Step 5: Install Go

```bash
curl -fsSL https://go.dev/dl/go1.22.5.linux-amd64.tar.gz -o /tmp/go.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf /tmp/go.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.bashrc
source ~/.bashrc

# Verify
go version
```

### Step 6: Clone and build

```bash
sudo apt install -y git make

git clone https://github.com/VirtualMetric/virtualmetric-bench.git
cd virtualmetric-bench

make build
make build-containers
```

### Step 7: Push container images to a registry

Your Kubernetes cluster needs to pull the helper images. How you do this depends on your setup.

#### If using k3s (local single-node)

k3s can use locally built images if you import them:

```bash
# k3s uses containerd, so import directly:
docker save virtualmetric/bench-generator:latest | sudo k3s ctr images import -
docker save virtualmetric/bench-receiver:latest  | sudo k3s ctr images import -
docker save virtualmetric/bench-collector:latest  | sudo k3s ctr images import -
```

#### If using kind

```bash
kind load docker-image virtualmetric/bench-generator:latest --name bench
kind load docker-image virtualmetric/bench-receiver:latest  --name bench
kind load docker-image virtualmetric/bench-collector:latest  --name bench
```

#### If using a remote cluster

Push to a registry your cluster can reach (Docker Hub, ECR, GCR, ACR, etc.):

```bash
# Example for Docker Hub (replace with your username):
docker tag virtualmetric/bench-generator:latest yourusername/bench-generator:latest
docker tag virtualmetric/bench-receiver:latest  yourusername/bench-receiver:latest
docker tag virtualmetric/bench-collector:latest  yourusername/bench-collector:latest

docker push yourusername/bench-generator:latest
docker push yourusername/bench-receiver:latest
docker push yourusername/bench-collector:latest

# Then run tests with custom image names:
./bin/harness test -t tcp_to_tcp_performance -s vector --platform kubernetes \
  --generator-image yourusername/bench-generator:latest \
  --receiver-image  yourusername/bench-receiver:latest \
  --collector-image yourusername/bench-collector:latest
```

### Step 8: Run your first test

```bash
./bin/harness test -t tcp_to_tcp_performance -s vector --platform kubernetes
```

What happens behind the scenes:

1. A namespace `bench-tcp-to-tcp-performance-<timestamp>` is created.
2. A ConfigMap is created with the Vector config file.
3. RBAC resources are created (ServiceAccount, Role, RoleBinding) so the collector can query the Kubernetes Metrics API.
4. Two headless Services are created for DNS: `subject` and `receiver`.
5. A Deployment starts the **subject** pod (Vector) with Guaranteed QoS (2 CPU, 2Gi RAM by default).
6. A Deployment starts the **receiver** pod.
7. A Job starts the **generator** — it sends data for 2 minutes.
8. A Job starts the **collector** — it polls the Kubernetes Metrics API every second for the subject pod's CPU and memory usage, writing rows to a CSV file incrementally.
9. After the generator Job completes, the harness runs `kubectl port-forward` to reach the receiver's `/metrics` endpoint and fetches the final line count.
10. Metrics CSV is copied from the collector pod via `kubectl cp`.
11. The collector pod is deleted for cleanup.
12. The namespace is deleted (complete cleanup).
13. Results are saved to `results/` locally.

The output will look something like:

```
→ test=tcp_to_tcp_performance  subject=vector  version=0.45.0-alpine  config=default
  starting containers…
  waiting for subject pod to be ready…
  waiting for generator (up to 2m40s)…
  waiting for receiver to drain…
  stopping collector…
  done. results → results/tcp_to_tcp_performance/default/vector/0.45.0-alpine/2026-04-04T120000Z
  lines received: 12345678  bytes received: 3160493568  elapsed: 142.3s
  tearing down…
```

### Step 9: Look at the results

```bash
cat results/tcp_to_tcp_performance/default/vector/*/*/summary.json
head results/tcp_to_tcp_performance/default/vector/*/*/metrics.csv
```

---

## Kubernetes-specific flags

| Flag | Default | Description |
| --- | --- | --- |
| `--platform kubernetes` | `docker` | Run the test on Kubernetes instead of Docker |
| `--cpu-limit` | (none) | CPU cores for the subject pod (e.g. `1`, `4`, `16`) |
| `--mem-limit` | (none) | Memory limit for the subject pod (e.g. `1g`, `4g`, `64g`) |

When limits are set, the subject pod runs with **Guaranteed QoS** (requests == limits) so benchmarks get consistent, isolated resources. The generator, receiver, and collector pods are always unconstrained.

### Hardware profiles

Use `--cpu-limit` and `--mem-limit` to benchmark across different hardware tiers. These flags work on both Docker and Kubernetes:

```bash
# Small: 1 core, 1 GB
./bin/harness test -t tcp_to_tcp_performance -s vector \
  --platform kubernetes --cpu-limit 1 --mem-limit 1g

# Medium: 4 cores, 16 GB
./bin/harness test -t tcp_to_tcp_performance -s vector \
  --platform kubernetes --cpu-limit 4 --mem-limit 16g

# Large: 16 cores, 64 GB
./bin/harness test -t tcp_to_tcp_performance -s vector \
  --platform kubernetes --cpu-limit 16 --mem-limit 64g
```

The limits are recorded in `summary.json` so you can compare results across profiles.

---

## Common commands

| What you want to do | Command |
| --- | --- |
| List all tests and subjects | `./bin/harness list` |
| Run a performance test | `./bin/harness test -t tcp_to_tcp_performance -s vector --platform kubernetes` |
| Run with resource limits | `./bin/harness test -t tcp_to_tcp_performance -s vector --platform kubernetes --cpu-limit 4 --mem-limit 4g` |
| Run a correctness test | `./bin/harness test -t sighup_correctness -s vector --platform kubernetes` |
| Run all subjects in a case | `./bin/harness test -t tcp_to_tcp_performance --platform kubernetes` |
| Run against every registered subject | `./bin/harness test -t tcp_to_tcp_performance --all-subjects --platform kubernetes` |
| Compare results | `./bin/harness compare -t tcp_to_tcp_performance` |
| Compare as JSON | `./bin/harness compare -t tcp_to_tcp_performance --format json` |
| Debug: keep namespace alive | `./bin/harness test -t tcp_to_tcp_performance -s vector --platform kubernetes --no-cleanup` |
| Clean up a stuck namespace | `kubectl delete namespace bench-tcp-to-tcp-performance-20260404-120000` |

### Comparing results

After running the same test against multiple subjects:

```bash
./bin/harness test -t tcp_to_tcp_performance -s vector --platform kubernetes
./bin/harness test -t tcp_to_tcp_performance -s fluent-bit --platform kubernetes
./bin/harness test -t tcp_to_tcp_performance -s fluentd --platform kubernetes
./bin/harness compare -t tcp_to_tcp_performance
```

This prints:

```
  Test: tcp_to_tcp_performance  Config: default

  SUBJECT      VERSION          THROUGHPUT       AVG CPU   MAX CPU   AVG MEM   MAX MEM   NET I/O
  -------      -------          ----------       -------   -------   -------   -------   -------
  vector       0.45.0-alpine    524,288 lines/s  12.3%     18.7%     42 MB     58 MB     1,024 MB
  fluent-bit   3.2              412,000 lines/s  8.1%      14.2%     28 MB     35 MB     890 MB
  fluentd      v1.17-1          98,000 lines/s   45.2%     62.0%     180 MB    210 MB    720 MB
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

All TCP-based performance tests default to `connections: 4`. You can increase this for high-performance subjects:

| connections | When to use |
| --- | --- |
| `1` | Correctness tests, low-throughput subjects |
| `4` | Default for performance tests |
| `8` | High-performance subjects (Vector) on 4+ core nodes |
| `16+` | Stress testing on large nodes |

Edit the `connections` field in `cases/<test_name>/case.yaml`. No rebuild needed — takes effect on the next run.

On Kubernetes, make sure the generator pod has enough CPU to drive the connections. The generator uses Burstable QoS by default so it can burst as needed, but if your cluster enforces strict quotas you may need to adjust.

---

## Troubleshooting

### "error: unable to connect to the server"

Your kubeconfig is not set up or the cluster is not reachable. Check:

```bash
kubectl cluster-info
```

If this fails, review Step 4 for your cluster type.

### "ImagePullBackOff" or "ErrImagePull"

The cluster cannot pull the helper container images. See Step 7 for how to make images available to your cluster.

To debug, check the pod events:

```bash
# Find the namespace (starts with "bench-")
kubectl get namespaces | grep bench

# Check pod status
kubectl -n bench-tcp-to-tcp-performance-TIMESTAMP get pods
kubectl -n bench-tcp-to-tcp-performance-TIMESTAMP describe pod <pod-name>
```

### Generator Job never completes

The subject may have crashed or is not accepting connections. Check its logs:

```bash
kubectl -n <namespace> logs deployment/subject
kubectl -n <namespace> logs job/generator
```

### "receiver metrics not available after 30s"

The port-forward to the receiver failed. Make sure nothing else is using port 19001 on your machine. You can change the port:

```bash
./bin/harness test -t tcp_to_tcp_performance -s vector \
  --platform kubernetes --receiver-port 29001
```

### Namespace stuck in "Terminating" state

This can happen if finalizers are blocking deletion. Force it:

```bash
kubectl delete namespace <namespace> --grace-period=0 --force
```

### Low throughput on kind or k3s

Local lightweight clusters share resources with your host. For accurate benchmarks, use a dedicated cluster with nodes that have at least 4 CPU cores and 8 GB RAM. Set `--kube-cpu` and `--kube-mem` to match the available resources.

---

## How Kubernetes mode differs from Docker mode

| Aspect | Docker | Kubernetes |
| --- | --- | --- |
| Isolation | Containers share host kernel, no resource limits by default | Subject pod gets Guaranteed QoS (CPU + memory limits) |
| Networking | Docker bridge network, port mapping to host | ClusterIP Services, port-forward for metrics collection |
| Cleanup | `docker compose down` removes containers | `kubectl delete namespace` removes everything |
| Metrics collection | Collector reads Docker Stats API via socket | Collector queries Kubernetes Metrics API via service account |
| File-based tests | Docker volume shared between containers | `emptyDir` volume shared between pods |
| Image access | Local images used directly | Images must be in a registry or imported into the cluster |

---

## Architecture on Kubernetes

```
  namespace: bench-<test>-<timestamp>
  ┌─────────────────────────────────────────────┐
  │                                             │
  │  Service: subject        Service: receiver  │
  │      │                       │              │
  │  ┌───▼───┐              ┌───▼───┐          │
  │  │Subject │  TCP:9000→   │Receiver│         │
  │  │  Pod   │──────────────│  Pod   │         │
  │  │(Vector)│   :9001      │ :9090  │←── port-forward ──→ harness CLI
  │  └───▲───┘              └────────┘          │
  │      │                                      │
  │  ┌───┴───┐              ┌────────┐          │
  │  │Generat│              │Collect │          │
  │  │or Job │              │or Job  │          │
  │  └───────┘              └────────┘          │
  │                                             │
  └─────────────────────────────────────────────┘
```

All resources are created inside a single namespace. When the test is done, the entire namespace is deleted — nothing is left behind.
