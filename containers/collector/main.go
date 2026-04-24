package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gocarina/gocsv"
)

// MetricsRow matches the original dstat CSV schema.
type MetricsRow struct {
	Epoch    int64   `csv:"epoch"`
	CpuUsr   float64 `csv:"cpu_usr"`
	CpuSys   float64 `csv:"cpu_sys"`
	CpuIdl   float64 `csv:"cpu_idl"`
	CpuWai   float64 `csv:"cpu_wai"`
	CpuHiq   float64 `csv:"cpu_hiq"`
	CpuSiq   float64 `csv:"cpu_siq"`
	MemUsed  int64   `csv:"mem_used"`
	MemBuff  int64   `csv:"mem_buff"`
	MemCach  int64   `csv:"mem_cach"`
	MemFree  int64   `csv:"mem_free"`
	NetRecv  int64   `csv:"net_recv"`
	NetSend  int64   `csv:"net_send"`
	DskRead  int64   `csv:"dsk_read"`
	DskWrit  int64   `csv:"dsk_writ"`
	Load1    float64 `csv:"load_avg1"`
	Load5    float64 `csv:"load_avg5"`
	Load15   float64 `csv:"load_avg15"`
	ProcsRun int     `csv:"procs_run"`
	ProcsBlk int     `csv:"procs_blk"`
	TcpLis   int     `csv:"tcp_lis"`
	TcpAct   int     `csv:"tcp_act"`
	TcpSyn   int     `csv:"tcp_syn"`
	TcpTim   int     `csv:"tcp_tim"`
	TcpClo   int     `csv:"tcp_clo"`
}

func main() {
	mode := getEnv("COLLECTOR_MODE", "docker")
	output := getEnv("COLLECTOR_OUTPUT", "/results/metrics.csv")
	intervalSecs := getEnvInt("COLLECTOR_INTERVAL_SECS", 1)
	interval := time.Duration(intervalSecs) * time.Second

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var rows []MetricsRow
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Open the CSV file for incremental writes so the data survives even if
	// the process is killed without a graceful SIGTERM. Create the parent
	// directory defensively — on some bind-mount races the container's
	// /results/ doesn't materialize in time and os.Create would error out,
	// leaving the harness' later `docker cp` unable to find the file.
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "collector: mkdir %s: %v\n", filepath.Dir(output), err)
		os.Exit(1)
	}
	csvFile, err := os.Create(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "collector: create %s: %v\n", output, err)
		os.Exit(1)
	}
	defer csvFile.Close()
	headerWritten := false

	appendRow := func(row MetricsRow) {
		rows = append(rows, row)
		if !headerWritten {
			// Write header + first row
			if err := gocsv.MarshalFile(&rows, csvFile); err != nil {
				fmt.Fprintf(os.Stderr, "collector: write csv: %v\n", err)
			}
			headerWritten = true
		} else {
			// Append just this row (no header)
			one := []MetricsRow{row}
			if err := gocsv.MarshalWithoutHeaders(&one, csvFile); err != nil {
				fmt.Fprintf(os.Stderr, "collector: write csv row: %v\n", err)
			}
		}
		_ = csvFile.Sync()
	}

	switch mode {
	case "kubernetes":
		fmt.Fprintf(os.Stderr, "collector: mode=kubernetes output=%s interval=%s\n", output, interval)
		runKubernetesMode(ctx, ticker, appendRow)
	default:
		fmt.Fprintf(os.Stderr, "collector: mode=docker output=%s interval=%s\n", output, interval)
		runDockerMode(ctx, ticker, appendRow)
	}

	fmt.Fprintf(os.Stderr, "collector: done. %d rows written to %s\n", len(rows), output)
}

func runDockerMode(ctx context.Context, ticker *time.Ticker, emit func(MetricsRow)) {
	container := mustEnv("COLLECTOR_TARGET_CONTAINER")
	dockerHost := getEnv("DOCKER_HOST", "unix:///var/run/docker.sock")

	httpClient := buildClient(dockerHost)
	var prev *dockerStats
	var errCount int

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := fetchDockerStats(httpClient, dockerHost, container)
			if err != nil {
				errCount++
				// Log the first error immediately, then every 30 ticks,
				// so a silent permission/DNS/container-missing failure is
				// diagnosable but we don't spam the run.
				if errCount == 1 || errCount%30 == 0 {
					fmt.Fprintf(os.Stderr, "collector: fetch stats (err #%d): %v\n", errCount, err)
				}
				continue
			}
			row := dockerStatsToRow(stats, prev)
			emit(row)
			prev = stats
		}
	}
}

func runKubernetesMode(ctx context.Context, ticker *time.Ticker, emit func(MetricsRow)) {
	namespace := mustEnv("COLLECTOR_NAMESPACE")
	podLabel := getEnv("COLLECTOR_POD_LABEL", "app=subject")

	kc, err := newKubeCollector(namespace, podLabel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "collector: failed to init kubernetes collector: %v\n", err)
		<-ctx.Done()
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cpuMillicores, memBytes, err := kc.sample()
			if err != nil {
				continue
			}
			cpuPct := float64(cpuMillicores) / 10.0
			row := MetricsRow{
				Epoch:   time.Now().Unix(),
				CpuUsr:  cpuPct,
				CpuIdl:  100.0 - cpuPct,
				MemUsed: memBytes,
			}
			emit(row)
		}
	}
}

// --- Docker Stats API ---

type dockerStats struct {
	CPUStats struct {
		CPUUsage struct {
			TotalUsage  uint64   `json:"total_usage"`
			PercpuUsage []uint64 `json:"percpu_usage"`
		} `json:"cpu_usage"`
		SystemUsage uint64 `json:"system_cpu_usage"`
		OnlineCPUs  uint   `json:"online_cpus"`
	} `json:"cpu_stats"`
	PreCPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemUsage uint64 `json:"system_cpu_usage"`
	} `json:"precpu_stats"`
	MemoryStats struct {
		Usage uint64            `json:"usage"`
		Limit uint64            `json:"limit"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
	Networks map[string]struct {
		RxBytes uint64 `json:"rx_bytes"`
		TxBytes uint64 `json:"tx_bytes"`
	} `json:"networks"`
	BlkioStats struct {
		IoServiceBytesRecursive []struct {
			Op    string `json:"op"`
			Value uint64 `json:"value"`
		} `json:"io_service_bytes_recursive"`
	} `json:"blkio_stats"`
}

func buildClient(dockerHost string) *http.Client {
	if strings.HasPrefix(dockerHost, "unix://") {
		socketPath := strings.TrimPrefix(dockerHost, "unix://")
		return &http.Client{Transport: unixSocketTransport(socketPath), Timeout: 5 * time.Second}
	}
	return &http.Client{Timeout: 5 * time.Second}
}

func fetchDockerStats(client *http.Client, dockerHost, container string) (*dockerStats, error) {
	var url string
	if strings.HasPrefix(dockerHost, "unix://") {
		url = "http://localhost/containers/" + container + "/stats?stream=false"
	} else {
		url = dockerHost + "/containers/" + container + "/stats?stream=false"
	}

	resp, err := client.Get(url) //nolint:noctx
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var s dockerStats
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func dockerStatsToRow(cur *dockerStats, prev *dockerStats) MetricsRow {
	now := time.Now().Unix()

	var cpuPct float64
	cpuDelta := float64(cur.CPUStats.CPUUsage.TotalUsage) - float64(cur.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(cur.CPUStats.SystemUsage) - float64(cur.PreCPUStats.SystemUsage)
	numCPU := float64(cur.CPUStats.OnlineCPUs)
	if numCPU == 0 {
		numCPU = float64(len(cur.CPUStats.CPUUsage.PercpuUsage))
	}
	if sysDelta > 0 && cpuDelta > 0 {
		cpuPct = (cpuDelta / sysDelta) * numCPU * 100.0
	}
	if cpuPct > 100*numCPU {
		cpuPct = 100 * numCPU
	}

	cache := cur.MemoryStats.Stats["cache"]
	memUsed := int64(cur.MemoryStats.Usage) - int64(cache)
	if memUsed < 0 {
		memUsed = int64(cur.MemoryStats.Usage)
	}
	memFree := int64(cur.MemoryStats.Limit) - int64(cur.MemoryStats.Usage)
	if memFree < 0 {
		memFree = 0
	}

	var netRecv, netSend int64
	for _, n := range cur.Networks {
		netRecv += int64(n.RxBytes)
		netSend += int64(n.TxBytes)
	}
	if prev != nil {
		var pr, ps int64
		for _, n := range prev.Networks {
			pr += int64(n.RxBytes)
			ps += int64(n.TxBytes)
		}
		netRecv -= pr
		netSend -= ps
		if netRecv < 0 {
			netRecv = 0
		}
		if netSend < 0 {
			netSend = 0
		}
	}

	var dskRead, dskWrit int64
	for _, b := range cur.BlkioStats.IoServiceBytesRecursive {
		switch b.Op {
		case "Read":
			dskRead += int64(b.Value)
		case "Write":
			dskWrit += int64(b.Value)
		}
	}
	if prev != nil {
		var pr, pw int64
		for _, b := range prev.BlkioStats.IoServiceBytesRecursive {
			switch b.Op {
			case "Read":
				pr += int64(b.Value)
			case "Write":
				pw += int64(b.Value)
			}
		}
		dskRead -= pr
		dskWrit -= pw
		if dskRead < 0 {
			dskRead = 0
		}
		if dskWrit < 0 {
			dskWrit = 0
		}
	}

	return MetricsRow{
		Epoch:   now,
		CpuUsr:  cpuPct,
		CpuIdl:  100.0 - cpuPct,
		MemUsed: memUsed,
		MemCach: int64(cache),
		MemFree: memFree,
		NetRecv: netRecv,
		NetSend: netSend,
		DskRead: dskRead,
		DskWrit: dskWrit,
	}
}

// --- Common ---

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "collector: %s is required\n", key)
		os.Exit(1)
	}
	return v
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
