package main

import "testing"

func TestMemUsage(t *testing.T) {
	tests := []struct {
		name      string
		usage     uint64
		stats     map[string]uint64
		wantUsed  int64
		wantCache int64
	}{
		{
			name:  "cgroup v2 excludes file cache",
			usage: 200 << 20,
			stats: map[string]uint64{
				"file":          130 << 20,
				"inactive_file": 120 << 20,
				"active_file":   10 << 20,
				"anon":          60 << 20,
			},
			wantUsed:  70 << 20,
			wantCache: 130 << 20,
		},
		{
			name:  "cgroup v2 keeps shmem counted",
			usage: 200 << 20,
			stats: map[string]uint64{
				"file":  130 << 20,
				"shmem": 30 << 20,
			},
			wantUsed:  100 << 20,
			wantCache: 130 << 20,
		},
		{
			name:  "cgroup v1 uses total_cache and total_shmem",
			usage: 200 << 20,
			stats: map[string]uint64{
				"total_cache": 110 << 20,
				"total_shmem": 20 << 20,
				"cache":       90 << 20,
			},
			wantUsed:  110 << 20,
			wantCache: 110 << 20,
		},
		{
			name:  "cgroup v1 plain cache key fallback",
			usage: 200 << 20,
			stats: map[string]uint64{
				"cache": 90 << 20,
				"shmem": 10 << 20,
			},
			wantUsed:  120 << 20,
			wantCache: 90 << 20,
		},
		{
			name:  "cache >= usage falls back to raw usage",
			usage: 50 << 20,
			stats: map[string]uint64{
				"file": 60 << 20,
			},
			wantUsed:  50 << 20,
			wantCache: 60 << 20,
		},
		{
			name:  "shmem > cache falls back to raw usage",
			usage: 100 << 20,
			stats: map[string]uint64{
				"file":  10 << 20,
				"shmem": 20 << 20,
			},
			wantUsed:  100 << 20,
			wantCache: 10 << 20,
		},
		{
			name:      "empty stats map reports raw usage",
			usage:     70 << 20,
			stats:     map[string]uint64{},
			wantUsed:  70 << 20,
			wantCache: 0,
		},
		{
			name:      "nil stats map reports raw usage",
			usage:     70 << 20,
			stats:     nil,
			wantUsed:  70 << 20,
			wantCache: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			used, cache := memUsage(tt.usage, tt.stats)
			if used != tt.wantUsed {
				t.Errorf("used = %d, want %d", used, tt.wantUsed)
			}
			if cache != tt.wantCache {
				t.Errorf("cache = %d, want %d", cache, tt.wantCache)
			}
		})
	}
}

func TestAddRow(t *testing.T) {
	var dst MetricsRow
	addRow(&dst, MetricsRow{CpuUsr: 40, MemUsed: 100, MemCach: 10, MemFree: 50, NetRecv: 1, NetSend: 2, DskRead: 3, DskWrit: 4})
	addRow(&dst, MetricsRow{CpuUsr: 15, MemUsed: 200, MemCach: 20, MemFree: 70, NetRecv: 10, NetSend: 20, DskRead: 30, DskWrit: 40})

	if dst.CpuUsr != 55 {
		t.Errorf("CpuUsr = %v, want 55", dst.CpuUsr)
	}
	if dst.MemUsed != 300 || dst.MemCach != 30 || dst.MemFree != 120 {
		t.Errorf("mem = %d/%d/%d, want 300/30/120", dst.MemUsed, dst.MemCach, dst.MemFree)
	}
	if dst.NetRecv != 11 || dst.NetSend != 22 || dst.DskRead != 33 || dst.DskWrit != 44 {
		t.Errorf("io = %d/%d/%d/%d, want 11/22/33/44", dst.NetRecv, dst.NetSend, dst.DskRead, dst.DskWrit)
	}
	// Epoch and CpuIdl are derived per tick by the caller, never summed.
	if dst.Epoch != 0 || dst.CpuIdl != 0 {
		t.Errorf("Epoch/CpuIdl = %d/%v, want 0/0", dst.Epoch, dst.CpuIdl)
	}
}

func TestDockerStatsToRowLeavesPerTickFieldsZero(t *testing.T) {
	// Epoch and CpuIdl are derived once per tick by runDockerMode after the
	// per-target rows are summed (CpuIdl floored at 0 there, since summed
	// usage can exceed 100). A per-container row must leave them zero, or a
	// stale per-container idle would leak into the combined row's contract.
	var s dockerStats
	s.CPUStats.CPUUsage.TotalUsage = 2_000_000
	s.CPUStats.SystemUsage = 10_000_000
	s.CPUStats.OnlineCPUs = 4
	s.PreCPUStats.CPUUsage.TotalUsage = 1_000_000
	s.PreCPUStats.SystemUsage = 9_000_000

	row := dockerStatsToRow(&s, nil)
	if row.CpuUsr <= 0 {
		t.Fatalf("CpuUsr = %v, want > 0 (sanity: stats deltas present)", row.CpuUsr)
	}
	if row.Epoch != 0 || row.CpuIdl != 0 {
		t.Fatalf("Epoch/CpuIdl = %d/%v, want 0/0 (caller-derived per tick)", row.Epoch, row.CpuIdl)
	}
}
