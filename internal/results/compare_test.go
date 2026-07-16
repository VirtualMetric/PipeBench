package results

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeMetricsCSV(t *testing.T, rows string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metrics.csv")
	header := "epoch,cpu_usr,mem_used,net_recv,net_send,dsk_read,dsk_writ,load_avg1,load_avg5,load_avg15\n"
	if err := os.WriteFile(path, []byte(header+rows), 0o644); err != nil {
		t.Fatalf("writing csv: %v", err)
	}
	return path
}

func TestAggregateAllMetricsFromCSV(t *testing.T) {
	t.Parallel()

	// Stopped-container samples (cpu 0 AND mem 0) are dropped so the
	// crash/restart down window doesn't dilute the averages.
	t.Run("all-zero rows skipped", func(t *testing.T) {
		t.Parallel()
		csv := writeMetricsCSV(t,
			"100,0,0,0,0,0,0,0,0,0\n"+
				"101,10,104857600,0,0,0,0,0,0,0\n"+
				"102,0,0,0,0,0,0,0,0,0\n"+
				"103,20,209715200,0,0,0,0,0,0,0\n")
		m, err := AggregateAllMetricsFromCSV(csv)
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		if m.Samples != 2 {
			t.Fatalf("Samples = %d, want 2", m.Samples)
		}
		if m.CPUAvg != 15 || m.CPUMax != 20 {
			t.Fatalf("cpu avg/max = %v/%v, want 15/20", m.CPUAvg, m.CPUMax)
		}
		if m.MemAvgMB != 150 || m.MemMaxMB != 200 {
			t.Fatalf("mem avg/max = %v/%v, want 150/200", m.MemAvgMB, m.MemMaxMB)
		}
	})

	// A running-but-idle sample (cpu 0, mem > 0) is a real sample and stays.
	t.Run("idle row kept", func(t *testing.T) {
		t.Parallel()
		csv := writeMetricsCSV(t,
			"100,0,104857600,0,0,0,0,0,0,0\n"+
				"101,10,104857600,0,0,0,0,0,0,0\n")
		m, err := AggregateAllMetricsFromCSV(csv)
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		if m.Samples != 2 {
			t.Fatalf("Samples = %d, want 2", m.Samples)
		}
		if m.CPUAvg != 5 {
			t.Fatalf("CPUAvg = %v, want 5", m.CPUAvg)
		}
	})

	// An unparseable cpu/mem field is not evidence of a stopped container:
	// the row is kept and its valid net/disk columns still count.
	t.Run("malformed cpu field kept", func(t *testing.T) {
		t.Parallel()
		csv := writeMetricsCSV(t,
			"100,x,0,100,200,300,400,0,0,0\n"+
				"101,0,0,0,0,0,0,0,0,0\n")
		m, err := AggregateAllMetricsFromCSV(csv)
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		if m.Samples != 1 {
			t.Fatalf("Samples = %d, want 1 (malformed row kept, all-zero row dropped)", m.Samples)
		}
		if m.NetRecv != 100 || m.NetSend != 200 || m.DiskRead != 300 || m.DiskWrite != 400 {
			t.Fatalf("io = net %d/%d disk %d/%d, want 100/200 300/400",
				m.NetRecv, m.NetSend, m.DiskRead, m.DiskWrite)
		}
	})

	// Net/disk totals accumulate only over surviving rows.
	t.Run("io totals over kept rows", func(t *testing.T) {
		t.Parallel()
		csv := writeMetricsCSV(t,
			"100,0,0,999,999,999,999,0,0,0\n"+
				"101,10,104857600,100,200,300,400,0,0,0\n")
		m, err := AggregateAllMetricsFromCSV(csv)
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		if m.NetRecv != 100 || m.NetSend != 200 || m.DiskRead != 300 || m.DiskWrite != 400 {
			t.Fatalf("io = net %d/%d disk %d/%d, want 100/200 300/400",
				m.NetRecv, m.NetSend, m.DiskRead, m.DiskWrite)
		}
	})
}

func TestAggregateAllMetricsFromCSVWindow(t *testing.T) {
	t.Parallel()

	// The epoch window filter still applies alongside the zero-row skip.
	csv := writeMetricsCSV(t,
		"100,10,104857600,0,0,0,0,0,0,0\n"+
			"200,0,0,0,0,0,0,0,0,0\n"+
			"201,30,314572800,0,0,0,0,0,0,0\n"+
			"300,50,524288000,0,0,0,0,0,0,0\n")
	startNs := int64(200) * int64(time.Second)
	endNs := int64(250) * int64(time.Second)
	m, err := AggregateAllMetricsFromCSVWindow(csv, startNs, endNs)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if m.Samples != 1 {
		t.Fatalf("Samples = %d, want 1 (window keeps 200-201, zero row dropped)", m.Samples)
	}
	if m.CPUAvg != 30 || m.MemMaxMB != 300 {
		t.Fatalf("cpu/mem = %v/%v, want 30/300", m.CPUAvg, m.MemMaxMB)
	}
}
