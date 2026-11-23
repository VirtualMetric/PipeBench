package runner

import (
	"fmt"
	"strings"
	"testing"
)

// The leader may place a new device on itself. move_across_cut_link then takes
// the next candidate that runs on a replica that does not lead, and fails only
// when every candidate runs on the leader.
func TestPickMoveDevice(t *testing.T) {
	service := func(n int) string { return fmt.Sprintf("subject-%d", n) }
	placement := func(owners map[int64][]int) func(int64) []int {
		return func(d int64) []int { return owners[d] }
	}
	const leader = 1

	cases := map[string]struct {
		candidates []int64
		owners     map[int64][]int
		device     int64
		from       int
		problem    string
	}{
		"the first runs off the leader": {
			candidates: []int64{11, 12, 13},
			owners:     map[int64][]int{11: {2}, 12: {3}, 13: {1}},
			device:     11, from: 2,
		},
		// A placement seen in a real run: the leader took device 11 itself
		"the first runs on the leader": {
			candidates: []int64{11, 12, 13},
			owners:     map[int64][]int{11: {1}, 12: {2}, 13: {3}},
			device:     12, from: 2,
		},
		"one device, on the leader": {
			candidates: []int64{11},
			owners:     map[int64][]int{11: {1}},
			problem:    "device 11 runs on the leader subject-1; the move needs an owner that does not lead",
		},
		"every device on the leader": {
			candidates: []int64{11, 12},
			owners:     map[int64][]int{11: {1}, 12: {1}},
			problem:    "devices 11, 12 all run on the leader subject-1",
		},
		"a device on two replicas": {
			candidates: []int64{11, 12},
			owners:     map[int64][]int{11: {2, 3}, 12: {2}},
			problem:    "device 11 runs on 2 replicas (subject-2, subject-3)",
		},
		"a device on no replica": {
			candidates: []int64{11, 12},
			owners:     map[int64][]int{11: {1}, 12: nil},
			problem:    "device 12 runs on 0 replicas",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			device, from, problem := pickMoveDevice(c.candidates, leader, placement(c.owners), service)
			if c.problem != "" {
				if problem == "" || !strings.Contains(problem, c.problem) {
					t.Fatalf("problem = %q, want one that says %q", problem, c.problem)
				}
				return
			}
			if problem != "" || device != c.device || from != c.from {
				t.Fatalf("pickMoveDevice = device %d on replica %d, problem %q; want device %d on replica %d", device, from, problem, c.device, c.from)
			}
		})
	}
}
