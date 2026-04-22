package orchestrator

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/template"
	"time"
)

// KubeConfig holds parameters for a Kubernetes-based test run.
type KubeConfig struct {
	RunConfig // embeds the common config (includes CPULimit, MemLimit)

	// Kubernetes-specific
	Namespace string // auto-generated if empty
}

// KubeRunner manages a Kubernetes namespace lifecycle for one test run.
type KubeRunner struct {
	cfg       KubeConfig
	namespace string
}

// Ensure KubeRunner implements Orchestrator.
var _ Orchestrator = (*KubeRunner)(nil)

// NewKubeRunner creates a KubeRunner, generates the namespace and manifests.
func NewKubeRunner(cfg KubeConfig) (*KubeRunner, error) {
	ns := cfg.Namespace
	if ns == "" {
		ts := time.Now().UTC().Format("20060102-150405")
		safe := strings.ReplaceAll(cfg.TestCase.Name, "_", "-")
		ns = fmt.Sprintf("bench-%s-%s", safe, ts)
		// K8s namespaces max 63 chars
		if len(ns) > 63 {
			ns = ns[:63]
		}
	}

	if err := os.MkdirAll(cfg.TmpDir, 0o755); err != nil {
		return nil, err
	}

	return &KubeRunner{cfg: cfg, namespace: ns}, nil
}

// UpServices is not supported on Kubernetes — falls back to starting everything.
func (k *KubeRunner) UpServices(_ ...string) error {
	return k.Up()
}

// StopServices is not supported on Kubernetes.
func (k *KubeRunner) StopServices(_ ...string) error {
	return fmt.Errorf("StopServices not supported on Kubernetes")
}

func (k *KubeRunner) Up() error {
	// 1. Create namespace
	if err := kubectl("create", "namespace", k.namespace); err != nil {
		return fmt.Errorf("creating namespace %s: %w", k.namespace, err)
	}

	// 2. Create ConfigMap with the subject config file
	configData, err := os.ReadFile(k.cfg.ConfigSrcPath)
	if err != nil {
		return fmt.Errorf("reading subject config: %w", err)
	}
	s := k.cfg.Subject
	cmFile := fmt.Sprintf("--from-literal=%s=%s", s.ConfigFile(), string(configData))
	if err := kubectl("-n", k.namespace, "create", "configmap", "subject-config", cmFile); err != nil {
		return fmt.Errorf("creating configmap: %w", err)
	}

	// 3. Apply the combined manifest
	manifest, err := k.renderManifest()
	if err != nil {
		return fmt.Errorf("rendering manifest: %w", err)
	}
	if err := kubectlStdin(manifest, "-n", k.namespace, "apply", "-f", "-"); err != nil {
		return fmt.Errorf("applying manifest: %w", err)
	}

	// 4. Wait for subject pod to be ready
	fmt.Printf("  waiting for subject pod to be ready…\n")
	if err := kubectl("-n", k.namespace, "wait", "--for=condition=ready", "pod", "-l", "app=subject", "--timeout=120s"); err != nil {
		return fmt.Errorf("waiting for subject: %w", err)
	}

	return nil
}

func (k *KubeRunner) Down() error {
	return kubectl("delete", "namespace", k.namespace, "--ignore-not-found=true")
}

func (k *KubeRunner) WaitForGeneratorExit(timeout time.Duration) error {
	timeoutStr := fmt.Sprintf("--timeout=%ds", int(timeout.Seconds()))
	return kubectl("-n", k.namespace, "wait", "--for=condition=complete", "job/generator", timeoutStr)
}

func (k *KubeRunner) StopCollector() error {
	// The collector writes CSV incrementally, so we copy first (in CopyMetricsCSV),
	// then stop here. Since the collector runs in a scratch container (no kill binary),
	// we delete the pod with a grace period so it gets SIGTERM.
	podName, err := k.collectorPodName()
	if err != nil {
		return nil // collector may not have started; not fatal
	}
	return kubectl("-n", k.namespace, "delete", "pod", podName,
		"--grace-period=5", "--wait=true")
}

func (k *KubeRunner) GeneratorStdout() string {
	out, _ := exec.Command("kubectl", "-n", k.namespace, "logs",
		"-l", "app=generator", "--tail", "100").Output()
	return string(out)
}

func (k *KubeRunner) Logs(name string, lines int) string {
	out, err := exec.Command("kubectl", "-n", k.namespace, "logs",
		"--tail", fmt.Sprintf("%d", lines), "-l", "app="+name).CombinedOutput()
	if err != nil {
		return fmt.Sprintf("(could not get logs: %v)", err)
	}
	return string(out)
}

func (k *KubeRunner) CopyMetricsCSV(dst string) error {
	// The collector writes CSV rows incrementally and syncs after each row,
	// so the file on disk is always up-to-date. Copy it while the pod is still running.
	podName, err := k.collectorPodName()
	if err != nil {
		return fmt.Errorf("collector pod not found: %w", err)
	}
	return kubectl("-n", k.namespace, "cp", podName+":/results/metrics.csv", dst)
}

func (k *KubeRunner) collectorPodName() (string, error) {
	out, err := exec.Command("kubectl", "-n", k.namespace, "get", "pods",
		"-l", "job-name=collector", "-o", "jsonpath={.items[0].metadata.name}").Output()
	if err != nil {
		return "", fmt.Errorf("finding collector pod: %w", err)
	}
	podName := strings.TrimSpace(string(out))
	if podName == "" {
		return "", fmt.Errorf("collector pod not found")
	}
	return podName, nil
}

func (k *KubeRunner) SubjectContainer() string {
	return "subject" // pod name in K8s
}

// ReceiverMetricsPort starts a kubectl port-forward to the receiver pod's
// metrics port (9090) and returns the local port + a cleanup function.
func (k *KubeRunner) ReceiverMetricsPort() (int, func(), error) {
	localPort := k.cfg.ReceiverHostPort
	if localPort == 0 {
		localPort = 19001
	}

	portArg := fmt.Sprintf("%d:9090", localPort)
	cmd := exec.Command("kubectl", "-n", k.namespace, "port-forward", "svc/receiver", portArg)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, nil, fmt.Errorf("kubectl port-forward: %w", err)
	}

	// Give port-forward a moment to bind
	time.Sleep(2 * time.Second)

	cleanup := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}

	return localPort, cleanup, nil
}

// --- Manifest rendering ---

// kubeManifestTemplate must use spaces, not tabs — YAML rejects tabs.
// It is left-aligned (no Go indentation) to produce valid YAML output.
const kubeManifestTemplate = `---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: bench-collector
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: bench-collector
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: ["metrics.k8s.io"]
    resources: ["pods"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: bench-collector
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: bench-collector
subjects:
  - kind: ServiceAccount
    name: bench-collector
---
apiVersion: v1
kind: Service
metadata:
  name: subject
spec:
  selector:
    app: subject
  ports:
    - port: 9000
      targetPort: 9000
      name: input
  clusterIP: None
---
apiVersion: v1
kind: Service
metadata:
  name: receiver
spec:
  selector:
    app: receiver
  ports:
    - port: 9001
      targetPort: 9001
      name: data
    - port: 9090
      targetPort: 9090
      name: metrics
  clusterIP: None
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: subject
spec:
  replicas: 1
  selector:
    matchLabels:
      app: subject
  template:
    metadata:
      labels:
        app: subject
    spec:
      containers:
        - name: subject
          image: "{{ .SubjectImage }}"
{{- if .SubjectUser }}
          securityContext:
            runAsUser: 0
{{- end }}
{{- if .SubjectArgs }}
          args:
{{- range .SubjectArgs }}
            - "{{ . }}"
{{- end }}
{{- end }}
          volumeMounts:
            - name: config
              mountPath: "{{ .ConfigDir }}"
              readOnly: true
{{- if .UseSharedData }}
            - name: shared-data
              mountPath: /data
{{- end }}
          resources:
            requests:
              cpu: "{{ .CPULimit }}"
              memory: "{{ .MemLimit }}"
            limits:
              cpu: "{{ .CPULimit }}"
              memory: "{{ .MemLimit }}"
{{- if .SubjectEnv }}
          env:
{{- range $k, $v := .SubjectEnv }}
            - name: "{{ $k }}"
              value: "{{ $v }}"
{{- end }}
{{- end }}
      volumes:
        - name: config
          configMap:
            name: subject-config
{{- if .UseSharedData }}
        - name: shared-data
          emptyDir: {}
{{- end }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: receiver
spec:
  replicas: 1
  selector:
    matchLabels:
      app: receiver
  template:
    metadata:
      labels:
        app: receiver
    spec:
      containers:
        - name: receiver
          image: "{{ .ReceiverImage }}"
          ports:
            - containerPort: 9001
            - containerPort: 9090
          env:
            - name: RECEIVER_MODE
              value: "{{ .RecvMode }}"
            - name: RECEIVER_LISTEN
              value: "{{ .RecvListen }}"
            - name: RECEIVER_METRICS_PORT
              value: "9090"
            - name: RECEIVER_VALIDATE_ORDER
              value: "{{ .RecvValidateOrder }}"
            - name: RECEIVER_VALIDATE_DEDUP
              value: "{{ .RecvValidateDedup }}"
---
apiVersion: batch/v1
kind: Job
metadata:
  name: generator
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: generator
    spec:
      restartPolicy: Never
{{- if .UseSharedData }}
      volumes:
        - name: shared-data
          emptyDir: {}
{{- end }}
      containers:
        - name: generator
          image: "{{ .GeneratorImage }}"
{{- if .UseSharedData }}
          volumeMounts:
            - name: shared-data
              mountPath: /data
{{- end }}
          env:
            - name: GENERATOR_MODE
              value: "{{ .GenMode }}"
            - name: GENERATOR_TARGET
              value: "{{ .GenTarget }}"
            - name: GENERATOR_RATE
              value: "{{ .GenRate }}"
            - name: GENERATOR_DURATION
              value: "{{ .GenDuration }}"
            - name: GENERATOR_LINE_SIZE
              value: "{{ .GenLineSize }}"
            - name: GENERATOR_FORMAT
              value: "{{ .GenFormat }}"
            - name: GENERATOR_WARMUP
              value: "{{ .GenWarmup }}"
            - name: GENERATOR_SEQUENCED
              value: "{{ .GenSequenced }}"
            - name: GENERATOR_CONNECTIONS
              value: "{{ .GenConnections }}"
---
apiVersion: batch/v1
kind: Job
metadata:
  name: collector
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: collector
        job-name: collector
    spec:
      restartPolicy: Never
      serviceAccountName: bench-collector
      containers:
        - name: collector
          image: "{{ .CollectorImage }}"
          env:
            - name: COLLECTOR_MODE
              value: "kubernetes"
            - name: COLLECTOR_NAMESPACE
              value: "{{ .Namespace }}"
            - name: COLLECTOR_POD_LABEL
              value: "app=subject"
            - name: COLLECTOR_INTERVAL_SECS
              value: "1"
            - name: COLLECTOR_OUTPUT
              value: "/results/metrics.csv"
          volumeMounts:
            - name: results
              mountPath: /results
      volumes:
        - name: results
          emptyDir: {}
`

type kubeVars struct {
	SubjectImage      string
	SubjectContainer  string
	ConfigDir         string
	CPULimit          string
	MemLimit          string
	Namespace         string
	SubjectEnv        map[string]string
	SubjectUser       string
	SubjectArgs       []string
	UseSharedData     bool
	GeneratorImage    string
	ReceiverImage     string
	CollectorImage    string
	GenMode           string
	GenTarget         string
	GenRate           string
	GenDuration       string
	GenLineSize       string
	GenFormat         string
	GenWarmup         string
	GenSequenced      string
	GenConnections    string
	RecvMode          string
	RecvListen        string
	RecvValidateOrder string
	RecvValidateDedup string
}

func (k *KubeRunner) renderManifest() (string, error) {
	tc := k.cfg.TestCase
	s := k.cfg.Subject

	warmup := tc.WarmupOrDefault(10 * time.Second).String()
	duration := tc.DurationOrDefault(2 * time.Minute).String()

	genLineSize := tc.Generator.LineSize
	if genLineSize == 0 {
		genLineSize = 256
	}
	genFormat := tc.Generator.Format
	if genFormat == "" {
		genFormat = "raw"
	}

	cpuLimit := k.cfg.CPULimit
	if cpuLimit == "" {
		cpuLimit = "2"
	}
	memLimit := k.cfg.MemLimit
	if memLimit == "" {
		memLimit = "2Gi"
	}

	// Resolve the config file mount directory from the subject's ConfigPath.
	// e.g. /etc/vector/vector.toml → /etc/vector
	configDir := s.ConfigPath[:strings.LastIndex(s.ConfigPath, "/")]

	env := map[string]string{}
	for kk, v := range s.Env {
		env[kk] = v
	}
	for kk, v := range k.cfg.ExtraSubjectEnv {
		env[kk] = v
	}

	vars := kubeVars{
		SubjectImage:      s.ImageRef(),
		SubjectContainer:  "bench-subject-" + s.Name,
		ConfigDir:         configDir,
		CPULimit:          cpuLimit,
		MemLimit:          memLimit,
		Namespace:         k.namespace,
		SubjectEnv:        env,
		SubjectUser:       s.User,
		SubjectArgs:       s.Command,
		UseSharedData:     tc.Generator.Mode == "file",
		GeneratorImage:    k.cfg.GeneratorImage,
		ReceiverImage:     k.cfg.ReceiverImage,
		CollectorImage:    k.cfg.CollectorImage,
		GenMode:           tc.Generator.Mode,
		GenTarget:         tc.Generator.Target,
		GenRate:           fmt.Sprintf("%d", tc.Generator.Rate),
		GenDuration:       duration,
		GenLineSize:       fmt.Sprintf("%d", genLineSize),
		GenFormat:         genFormat,
		GenWarmup:         warmup,
		GenSequenced:      boolStr(tc.Type == "correctness"),
		GenConnections:    fmt.Sprintf("%d", max(tc.Generator.Connections, 1)),
		RecvMode:          tc.Receiver.Mode,
		RecvListen:        tc.Receiver.Listen,
		RecvValidateOrder: boolStr(tc.Correctness.ValidateOrder),
		RecvValidateDedup: boolStr(tc.Correctness.ValidateDedup),
	}

	tmpl, err := template.New("kube").Parse(kubeManifestTemplate)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// --- kubectl helpers ---

func kubectl(args ...string) error {
	cmd := exec.Command("kubectl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func kubectlStdin(input string, args ...string) error {
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(input)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
