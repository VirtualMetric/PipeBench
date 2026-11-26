package runner

import (
	"strings"
	"testing"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// A sleep with up_to lasts a random time between its for and its up_to, and says which. One without
// sleeps its for.
func TestASleepWithUpToLastsBetweenItsBounds(t *testing.T) {
	if d, said := sleepLength(config.ScenarioStep{Do: "sleep", For: "5s"}); d != 5*time.Second || said != "" {
		t.Fatalf("a plain sleep lasts %s (%q)", d, said)
	}
	seen := map[time.Duration]bool{}
	for range 200 {
		d, said := sleepLength(config.ScenarioStep{Do: "sleep", For: "5s", UpTo: "30s"})
		if d < 5*time.Second || d > 30*time.Second || !strings.Contains(said, d.String()) {
			t.Fatalf("a random sleep lasts %s (%q), want between 5s and 30s and said", d, said)
		}
		seen[d] = true
	}
	if len(seen) < 10 {
		t.Fatalf("200 random sleeps took only %d lengths", len(seen))
	}
}
