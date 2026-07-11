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
