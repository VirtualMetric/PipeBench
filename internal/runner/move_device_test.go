package runner

import (
	"fmt"
	"reflect"
	"testing"
)

// The leader may place a new device on itself, and a device may not be placed
// yet, or run on two replicas. move_across_cut_link then skips that candidate
// and takes the next one that runs on one replica that does not lead. It fails
// only when no candidate can move, and then names every candidate and why.
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
		skipped    []string
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
			skipped: []string{"device 11 runs on the leader subject-1"},
		},
		"a device on two replicas is skipped": {
			candidates: []int64{11, 12},
			owners:     map[int64][]int{11: {2, 3}, 12: {2}},
			device:     12, from: 2,
			skipped: []string{"device 11 runs on 2 replicas (subject-2, subject-3)"},
		},
		"a device on the leader and one not placed are skipped": {
			candidates: []int64{11, 12, 13},
			owners:     map[int64][]int{11: {1}, 12: nil, 13: {3}},
			device:     13, from: 3,
			skipped: []string{"device 11 runs on the leader subject-1", "device 12 runs on 0 replicas ()"},
		},
		"one device, on the leader": {
			candidates: []int64{11},
			owners:     map[int64][]int{11: {1}},
			problem:    "device 11 runs on the leader subject-1; the move needs an owner that does not lead",
		},
		"one device, on two replicas": {
			candidates: []int64{11},
			owners:     map[int64][]int{11: {2, 3}},
			problem:    "device 11 runs on 2 replicas (subject-2, subject-3); the move needs one owner",
		},
		"no device can move": {
			candidates: []int64{11, 12, 13},
			owners:     map[int64][]int{11: {1}, 12: nil, 13: {2, 3}},
			problem: "no device can move: device 11 runs on the leader subject-1; device 12 runs on 0 replicas (); " +
				"device 13 runs on 2 replicas (subject-2, subject-3). The move needs a device that runs on one replica, which does not lead",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			device, from, skipped, problem := pickMoveDevice(c.candidates, leader, placement(c.owners), service)
			if c.problem != "" {
				if problem != c.problem {
					t.Fatalf("problem = %q, want %q", problem, c.problem)
				}
				return
			}
			if problem != "" || device != c.device || from != c.from {
				t.Fatalf("pickMoveDevice = device %d on replica %d, problem %q; want device %d on replica %d", device, from, problem, c.device, c.from)
			}
			if !reflect.DeepEqual(skipped, c.skipped) {
				t.Errorf("skipped = %q, want %q", skipped, c.skipped)
			}
		})
	}
}
