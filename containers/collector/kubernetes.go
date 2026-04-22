package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// kubeMetrics is the response from the Kubernetes Metrics API for a single pod.
// Endpoint: /apis/metrics.k8s.io/v1beta1/namespaces/<ns>/pods/<pod>
type kubeMetrics struct {
	Containers []struct {
		Name  string `json:"name"`
		Usage struct {
			CPU    string `json:"cpu"`    // e.g. "250m" (millicores)
			Memory string `json:"memory"` // e.g. "64Mi"
		} `json:"usage"`
	} `json:"containers"`
}

// kubeCollector polls the Kubernetes Metrics API for a pod's resource usage.
type kubeCollector struct {
	client    *http.Client
	apiServer string
	token     string
	namespace string
	podLabel  string // label selector, e.g. "app=subject"
}

func newKubeCollector(namespace, podLabel string) (*kubeCollector, error) {
	// In-cluster config: the API server is at https://kubernetes.default.svc
	// and the service account token is mounted at a well-known path.
	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("reading service account token: %w (is the collector running in a K8s pod?)", err)
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // in-cluster CA verification not needed for metrics
			},
		},
	}

	return &kubeCollector{
		client:    client,
		apiServer: "https://kubernetes.default.svc",
		token:     string(token),
		namespace: namespace,
		podLabel:  podLabel,
	}, nil
}

// sample fetches current CPU and memory usage for the target pod.
func (kc *kubeCollector) sample() (cpuMillicores int64, memBytes int64, err error) {
	// First, find the pod name by label
	podName, err := kc.findPod()
	if err != nil {
		return 0, 0, err
	}

	// Query the metrics API for this specific pod
	url := fmt.Sprintf("%s/apis/metrics.k8s.io/v1beta1/namespaces/%s/pods/%s",
		kc.apiServer, kc.namespace, podName)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+kc.token)

	resp, err := kc.client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, 0, fmt.Errorf("metrics API returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}

	var metrics kubeMetrics
	if err := json.Unmarshal(body, &metrics); err != nil {
		return 0, 0, err
	}

	// Sum across all containers in the pod (typically just one: "subject")
	for _, c := range metrics.Containers {
		cpuMillicores += parseK8sQuantity(c.Usage.CPU)
		memBytes += parseK8sQuantity(c.Usage.Memory)
	}

	return cpuMillicores, memBytes, nil
}

// findPod returns the name of the first pod matching the label selector.
func (kc *kubeCollector) findPod() (string, error) {
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods?labelSelector=%s&limit=1",
		kc.apiServer, kc.namespace, kc.podLabel)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+kc.token)

	resp, err := kc.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var result struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	if len(result.Items) == 0 {
		return "", fmt.Errorf("no pod found with label %s in namespace %s", kc.podLabel, kc.namespace)
	}
	return result.Items[0].Metadata.Name, nil
}

// parseK8sQuantity parses Kubernetes resource quantity strings.
// Examples: "250m" → 250 (millicores), "64Mi" → 67108864 (bytes), "128974848" → 128974848
func parseK8sQuantity(s string) int64 {
	if s == "" {
		return 0
	}

	// Millicores: "250m"
	if s[len(s)-1] == 'm' {
		n := parseInt64(s[:len(s)-1])
		return n
	}

	// Binary units: Ki, Mi, Gi
	if len(s) >= 2 {
		suffix := s[len(s)-2:]
		val := parseInt64(s[:len(s)-2])
		switch suffix {
		case "Ki":
			return val * 1024
		case "Mi":
			return val * 1024 * 1024
		case "Gi":
			return val * 1024 * 1024 * 1024
		}
	}

	// Plain integer (bytes or nanocores)
	return parseInt64(s)
}

func parseInt64(s string) int64 {
	var n int64
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int64(c-'0')
		}
	}
	return n
}
