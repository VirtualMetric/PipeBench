package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func validScenarioCase() *TestCase {
	return &TestCase{
		Name:      "scenario",
		Type:      ClusterScenarioType,
		Generator: GeneratorConfig{Mode: "http_ack", Target: "http://subject-1:9000/", Connections: 1, Rate: 1000},
		Receiver:  ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Cluster: &ClusterConfig{Nodes: 3, Scenario: &ScenarioConfig{
			NodeArgs:   true,
			SpareNodes: 1,
			Steps: []ScenarioStep{
				{Do: "wait_ready", Within: "150s"},
				{Do: "start_load"},
				{Do: "kill", Node: "leader", As: "victim"},
				{Do: "wait_leader", Within: "60s", Record: "election"},
				{Do: "start", Node: "@victim", Recover: "reset"},
				{Do: "scale_to", Nodes: 4},
				{Do: "sleep", For: "5s"},
				{Do: "mark", Label: "end"},
				{Do: "expect_delivery_stops", Within: "60s", For: "20s", Record: "stopped"},
				{Do: "expect_no_log", Node: "1", Log: "collecting", For: "10s", Since: "end"},
				{Do: "count_pending", SentBefore: "end", Record: "held"},
			},
		}},
	}
}

func TestValidateClusterScenarioAcceptsAValidCase(t *testing.T) {
	if err := validScenarioCase().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateClusterScenarioRejects(t *testing.T) {
	cases := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"unknown action":          {func(tc *TestCase) { tc.Cluster.Scenario.Steps[0].Do = "explode" }, "unknown action"},
		"forgotten node name":     {func(tc *TestCase) { tc.Cluster.Scenario.Steps[4].Node = "@ghost" }, "was not remembered"},
		"node out of range":       {func(tc *TestCase) { tc.Cluster.Scenario.Steps[2].Node = "9" }, "node must be"},
		"recover without wrapper": {func(tc *TestCase) { tc.Cluster.Scenario.NodeArgs = false }, "node_args"},
		"bad recover":             {func(tc *TestCase) { tc.Cluster.Scenario.Steps[4].Recover = "maybe" }, "recover must be"},
		"bad duration":            {func(tc *TestCase) { tc.Cluster.Scenario.Steps[0].Within = "soon" }, "within"},
		"scale past spares":       {func(tc *TestCase) { tc.Cluster.Scenario.Steps[5].Nodes = 5 }, "nodes must be between"},
		"sleep without for":       {func(tc *TestCase) { tc.Cluster.Scenario.Steps[6].For = "" }, "needs for"},
		"wrong generator":         {func(tc *TestCase) { tc.Generator.Mode = "tcp" }, "http_ack"},
		"too much load":           {func(tc *TestCase) { tc.Generator.Connections = 3 }, "lines a second"},
		"implicit workers":        {func(tc *TestCase) { tc.Generator.Connections = 0 }, "explicitly"},
		"no steps":                {func(tc *TestCase) { tc.Cluster.Scenario.Steps = nil }, "requires cluster.scenario.steps"},
		"witness without store":   {func(tc *TestCase) { tc.Cluster.Scenario.Witness = "azure" }, "azure:"},
		"undeclared proxy": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps, ScenarioStep{Do: "proxy_latency", Proxy: "witness", Latency: "200ms"})
		}, "not declared"},
		"action from the other driver": {func(tc *TestCase) { tc.Cluster.Action = "restart_leader" }, "belong to director_cluster_correctness"},
		"relative witness dir":         {func(tc *TestCase) { tc.Cluster.Scenario.WitnessDir = "witness" }, "absolute path"},
		"witness dir with a space":     {func(tc *TestCase) { tc.Cluster.Scenario.WitnessDir = "/wit ness" }, "absolute path"},
		"witness dir with a colon":     {func(tc *TestCase) { tc.Cluster.Scenario.WitnessDir = "/witness:rw" }, "absolute path"},
		"witness dir with a shell command": {func(tc *TestCase) {
			tc.Cluster.Scenario.WitnessDir = "/witness;rm -rf /"
		}, "absolute path"},
		"witness dir with a substitution": {func(tc *TestCase) { tc.Cluster.Scenario.WitnessDir = "/w$(id)" }, "absolute path"},
		"witness dir climbing out":        {func(tc *TestCase) { tc.Cluster.Scenario.WitnessDir = "/witness/../etc" }, "'..'"},
		"witness dir at the root":         {func(tc *TestCase) { tc.Cluster.Scenario.WitnessDir = "/" }, "absolute path"},
		"witness dir of dots":             {func(tc *TestCase) { tc.Cluster.Scenario.WitnessDir = "/./." }, "below /"},
		"probe interval too short":        {func(tc *TestCase) { tc.Cluster.Scenario.ProbeInterval = "100ms" }, "at least 200ms"},
		"as on a wait":                    {func(tc *TestCase) { tc.Cluster.Scenario.Steps[3].As = "elected" }, "acts on none"},
		"wait naming a forgotten node":    {func(tc *TestCase) { tc.Cluster.Scenario.Steps[3].Node = "@ghost" }, "was not remembered"},
		"wait naming a node out of range": {func(tc *TestCase) { tc.Cluster.Scenario.Steps[3].Node = "9" }, "node must be"},
		"since on another action":         {func(tc *TestCase) { tc.Cluster.Scenario.Steps[3].Since = "end" }, "only applies to expect_no_log"},
		"since naming no mark":            {func(tc *TestCase) { tc.Cluster.Scenario.Steps[9].Since = "ghost" }, "which no earlier step sets"},
		"since naming a later mark": {func(tc *TestCase) {
			steps := tc.Cluster.Scenario.Steps
			tc.Cluster.Scenario.Steps = append([]ScenarioStep{steps[9]}, steps...)
		}, "which no earlier step sets"},
		"sent_before on another action":       {func(tc *TestCase) { tc.Cluster.Scenario.Steps[3].SentBefore = "end" }, "only applies to count_pending"},
		"count_pending without sent_before":   {func(tc *TestCase) { tc.Cluster.Scenario.Steps[10].SentBefore = "" }, "needs sent_before"},
		"count_pending naming no mark":        {func(tc *TestCase) { tc.Cluster.Scenario.Steps[10].SentBefore = "ghost" }, "which no earlier step sets"},
		"count_pending without a record name": {func(tc *TestCase) { tc.Cluster.Scenario.Steps[10].Record = "" }, "needs record"},
		"count_pending without a load":        {func(tc *TestCase) { tc.Generator = GeneratorConfig{} }, "has no generator"},
		"log and logs together": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[9].Logs = []string{"stopping collector"}
		}, "name one of them"},
		"an empty line in logs": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[9].Log = ""
			tc.Cluster.Scenario.Steps[9].Logs = []string{"stopping collector", ""}
		}, "empty line"},
		"logs on another action": {func(tc *TestCase) { tc.Cluster.Scenario.Steps[3].Logs = []string{"x"} }, "only applies to wait_log and expect_no_log"},
		"an empty logs list on another action": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[3].Logs = []string{}
		}, "only applies to wait_log and expect_no_log"},
		"an empty logs list beside log": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[9].Logs = []string{}
		}, "name one of them"},
		"an empty logs list alone": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[9].Log = ""
			tc.Cluster.Scenario.Steps[9].Logs = []string{}
		}, "logs is empty"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := validScenarioCase()
			c.edit(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func TestScenarioBlockOnlyForScenarioType(t *testing.T) {
	tc := validScenarioCase()
	tc.Type = "director_cluster_correctness"
	tc.Generator.Mode = "tcp"
	if err := tc.Validate(); err == nil || !strings.Contains(err.Error(), "only valid for type "+ClusterScenarioType) {
		t.Fatalf("Validate() = %v", err)
	}
}

func TestScenarioDefaults(t *testing.T) {
	var sc *ScenarioConfig
	if sc.ProbeIntervalOrDefault().Seconds() != 1 || sc.WitnessDirOrDefault() != "/witness" || sc.ToxiproxyHostPortOrDefault() != 18474 {
		t.Fatalf("nil scenario defaults are wrong")
	}
	cpu, mem := sc.HelperLimits()
	if cpu != "1" || mem != "512m" {
		t.Fatalf("helper limits %s %s", cpu, mem)
	}
	if !strings.Contains(sc.ToxiproxyImageOrDefault(), ":2.") {
		t.Fatalf("the Toxiproxy image must be pinned to a release")
	}
}

func TestValidateClusterScenarioAcceptsLogs(t *testing.T) {
	tc := validScenarioCase()
	st := &tc.Cluster.Scenario.Steps[9]
	st.Log = ""
	st.Logs = []string{"lost on node", "placement has been changed to"}
	if err := tc.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := st.LogLines(); len(got) != 2 || got[1] != "placement has been changed to" {
		t.Fatalf("LogLines() = %q", got)
	}
	if got := (ScenarioStep{Log: "one"}).LogLines(); len(got) != 1 || got[0] != "one" {
		t.Fatalf("LogLines() of a step with log = %q", got)
	}
	if got := (ScenarioStep{}).LogLines(); got != nil {
		t.Fatalf("LogLines() of a step with neither = %q", got)
	}
}

// An empty logs list in a case file must reach the validator as present, so
// that it is refused instead of read as no logs key at all.
func TestEmptyLogsListDecodesAsPresent(t *testing.T) {
	var st ScenarioStep
	if err := yaml.Unmarshal([]byte("do: wait_log\nlog: ready\nlogs: []\n"), &st); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if st.Logs == nil {
		t.Fatalf("logs: [] decoded as absent; the validator could not refuse it")
	}
	var absent ScenarioStep
	if err := yaml.Unmarshal([]byte("do: wait_log\nlog: ready\n"), &absent); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if absent.Logs != nil {
		t.Fatalf("a step without logs decoded with a logs list")
	}
}

// twoReceiverScenarioCase is a valid scenario case with two receivers, the
// second of which a step pauses and unpauses.
func twoReceiverScenarioCase() *TestCase {
	tc := validScenarioCase()
	tc.Receiver = ReceiverConfig{}
	tc.Receivers = []ReceiverConfig{
		{ID: "a", Mode: "tcp", Listen: ":9001"},
		{ID: "b", Mode: "tcp", Listen: ":9001"},
	}
	tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps,
		ScenarioStep{Do: "receiver_pause", Receiver: "b"},
		ScenarioStep{Do: "sleep", For: "20s"},
		ScenarioStep{Do: "receiver_unpause", Receiver: "b"},
	)
	return tc
}

func TestValidateClusterScenarioAcceptsTwoReceivers(t *testing.T) {
	if err := twoReceiverScenarioCase().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateClusterScenarioReceiverSteps(t *testing.T) {
	last := func(tc *TestCase) *ScenarioStep {
		return &tc.Cluster.Scenario.Steps[len(tc.Cluster.Scenario.Steps)-1]
	}
	cases := map[string]struct {
		build func() *TestCase
		edit  func(tc *TestCase)
		want  string
	}{
		"a pause without a receiver": {twoReceiverScenarioCase, func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[len(tc.Cluster.Scenario.Steps)-3].Receiver = ""
		}, "needs receiver"},
		"an unpause naming no receiver of the case": {twoReceiverScenarioCase, func(tc *TestCase) { last(tc).Receiver = "c" }, `receiver "c" is not one of the case's receivers`},
		"a receiver named with one receiver": {validScenarioCase, func(tc *TestCase) {
			tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps, ScenarioStep{Do: "receiver_pause", Receiver: "a"})
		}, "the case has one receiver"},
		"a receiver on another action": {twoReceiverScenarioCase, func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[6].Receiver = "a"
		}, "receiver only applies to expect_arrived, receiver_pause, receiver_unpause"},
		"several receivers without the load": {twoReceiverScenarioCase, func(tc *TestCase) { tc.Generator = GeneratorConfig{} }, "needs the http_ack generator"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := c.build()
			c.edit(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, c.want)
			}
		})
	}
}

// A case with one receiver pauses it without naming it, as before.
func TestValidateClusterScenarioPausesItsOneReceiver(t *testing.T) {
	tc := validScenarioCase()
	tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps,
		ScenarioStep{Do: "receiver_pause"}, ScenarioStep{Do: "receiver_unpause"})
	if err := tc.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateClusterScenarioExpectNoLogOnEveryNode(t *testing.T) {
	tc := validScenarioCase()
	tc.Cluster.Scenario.Steps[9].Node = "all"
	if err := tc.Validate(); err != nil {
		t.Fatalf("Validate with node all: %v", err)
	}
	// Only expect_no_log reads every node this way: a kill still needs one.
	tc = validScenarioCase()
	tc.Cluster.Scenario.Steps[2].Node = "all"
	if err := tc.Validate(); err == nil || !strings.Contains(err.Error(), "node must be") {
		t.Fatalf("Validate() of a kill on all = %v, want a node error", err)
	}
	// And expect_no_log still needs a node.
	tc = validScenarioCase()
	tc.Cluster.Scenario.Steps[9].Node = ""
	if err := tc.Validate(); err == nil || !strings.Contains(err.Error(), "needs node") {
		t.Fatalf("Validate() of expect_no_log without a node = %v, want needs node", err)
	}
}

// int64p returns a pointer to v.
func int64p(v int64) *int64 { return &v }

// float64p returns a pointer to v.
func float64p(v float64) *float64 { return &v }

// A receiver that expect.receivers names is judged by its own limits, and the
// others by the case's.
func TestScenarioExpectReceiverLimits(t *testing.T) {
	ex := ScenarioExpect{
		MaxLoss:       int64p(0),
		MaxDuplicates: int64p(10),
		Receivers:     map[string]ScenarioReceiverExpect{"b": {MaxLoss: int64p(5000), MinLoss: int64p(1)}},
	}
	maxLoss, maxDuplicates, minLoss := ex.ReceiverLimits("a")
	if *maxLoss != 0 || *maxDuplicates != 10 || minLoss != nil {
		t.Fatalf("receiver a: max_loss %v, max_duplicates %v, min_loss %v; want the case's, and no min_loss", maxLoss, maxDuplicates, minLoss)
	}
	maxLoss, maxDuplicates, minLoss = ex.ReceiverLimits("b")
	if *maxLoss != 5000 || *maxDuplicates != 10 || *minLoss != 1 {
		t.Fatalf("receiver b: max_loss %d, max_duplicates %d, min_loss %d; want its own, and the case's max_duplicates", *maxLoss, *maxDuplicates, *minLoss)
	}
}

// expectArrivedCase is a valid case with two receivers whose last steps mark
// a window and count the lines sent in it at receiver b.
func expectArrivedCase() *TestCase {
	tc := twoReceiverScenarioCase()
	tc.Cluster.Scenario.Expect.MaxLoss = int64p(0)
	tc.Cluster.Scenario.Expect.Receivers = map[string]ScenarioReceiverExpect{"b": {MaxLoss: int64p(50000), MinLoss: int64p(1)}}
	tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps,
		ScenarioStep{Do: "mark", Label: "early"},
		ScenarioStep{Do: "sleep", For: "20s"},
		ScenarioStep{Do: "mark", Label: "early_end"},
		ScenarioStep{Do: "expect_arrived", Receiver: "b", SentAfter: "early", SentBefore: "early_end", Max: int64p(2000), Record: "early_kept"},
	)
	return tc
}

func TestValidateClusterScenarioAcceptsExpectArrived(t *testing.T) {
	if err := expectArrivedCase().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	// A share is of what another receiver got of the same window.
	shared := expectArrivedCase()
	shared.Cluster.Scenario.Steps = append(shared.Cluster.Scenario.Steps,
		ScenarioStep{Do: "expect_arrived", Receiver: "b", SentAfter: "early", SentBefore: "early_end", CompareTo: "a", MaxShare: float64p(0.02), Record: "early_share"},
	)
	if err := shared.Validate(); err != nil {
		t.Fatalf("Validate with a share: %v", err)
	}
	// A case with one receiver counts at it without naming it.
	tc := validScenarioCase()
	tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps,
		ScenarioStep{Do: "mark", Label: "late"},
		ScenarioStep{Do: "expect_arrived", SentAfter: "end", SentBefore: "late", Min: int64p(1), Record: "kept"},
	)
	if err := tc.Validate(); err != nil {
		t.Fatalf("Validate with one receiver: %v", err)
	}
}

func TestValidateClusterScenarioRejectsBadReceiverExpectations(t *testing.T) {
	last := func(tc *TestCase) *ScenarioStep {
		return &tc.Cluster.Scenario.Steps[len(tc.Cluster.Scenario.Steps)-1]
	}
	cases := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"expect.receivers with one receiver": {func(tc *TestCase) {
			tc.Receivers = nil
			tc.Receiver = ReceiverConfig{Mode: "tcp", Listen: ":9001"}
			last(tc).Receiver = ""
			for i := range tc.Cluster.Scenario.Steps {
				if tc.Cluster.Scenario.Steps[i].Do == "receiver_pause" || tc.Cluster.Scenario.Steps[i].Do == "receiver_unpause" {
					tc.Cluster.Scenario.Steps[i].Receiver = ""
				}
			}
		}, "the case has one; use expect's own limits"},
		"expect.receivers naming no receiver of the case": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Receivers["c"] = ScenarioReceiverExpect{MaxLoss: int64p(1)}
		}, `receiver "c" is not one of the case's receivers`},
		"a negative limit": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Receivers["b"] = ScenarioReceiverExpect{MaxLoss: int64p(-1)}
		}, "expect.receivers.b.max_loss must be 0 or more"},
		"min_loss above max_loss": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Receivers["b"] = ScenarioReceiverExpect{MaxLoss: int64p(10), MinLoss: int64p(11)}
		}, "min_loss 11 is more than max_loss 10"},
		"min_loss above the case's max_loss": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Receivers["b"] = ScenarioReceiverExpect{MinLoss: int64p(1)}
		}, "min_loss 1 is more than max_loss 0"},
		"an entry without a limit": {func(tc *TestCase) {
			tc.Cluster.Scenario.Expect.Receivers["a"] = ScenarioReceiverExpect{}
		}, "expect.receivers.a sets no limit"},
		"expect_arrived without a window":       {func(tc *TestCase) { last(tc).SentAfter = "" }, "needs sent_after and sent_before"},
		"expect_arrived naming an unknown mark": {func(tc *TestCase) { last(tc).SentBefore = "later" }, `names mark "later", which no earlier step sets`},
		"expect_arrived with one mark twice":    {func(tc *TestCase) { last(tc).SentAfter = "early_end" }, "the window is empty"},
		"expect_arrived without a bound":        {func(tc *TestCase) { last(tc).Max = nil }, "needs min, max, min_share or max_share"},
		"a share without compare_to":            {func(tc *TestCase) { last(tc).MinShare = float64p(0.5) }, "go together with compare_to"},
		"compare_to without a share":            {func(tc *TestCase) { last(tc).CompareTo = "a" }, "go together with compare_to"},
		"a share above 1": {func(tc *TestCase) {
			last(tc).CompareTo, last(tc).MaxShare = "a", float64p(1.5)
		}, "max_share must be a share from 0 to 1"},
		"min_share above max_share": {func(tc *TestCase) {
			last(tc).CompareTo, last(tc).MinShare, last(tc).MaxShare = "a", float64p(0.5), float64p(0.1)
		}, "min_share 0.5 is more than max_share 0.1"},
		"compare_to its own receiver": {func(tc *TestCase) {
			last(tc).CompareTo, last(tc).MinShare = "b", float64p(0.5)
		}, "compare_to names the receiver the step counts at"},
		"compare_to an unknown receiver": {func(tc *TestCase) {
			last(tc).CompareTo, last(tc).MinShare = "c", float64p(0.5)
		}, `compare_to: receiver "c" is not one of the case's receivers`},
		"expect_arrived with min above max":   {func(tc *TestCase) { last(tc).Min = int64p(3000) }, "min 3000 is more than max 2000"},
		"expect_arrived without record":       {func(tc *TestCase) { last(tc).Record = "" }, "needs record"},
		"expect_arrived without its receiver": {func(tc *TestCase) { last(tc).Receiver = "" }, "needs receiver"},
		"sent_after on another step": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[6].SentAfter = "end"
		}, "sent_after only applies to expect_arrived"},
		"min on another step": {func(tc *TestCase) {
			tc.Cluster.Scenario.Steps[6].Min = int64p(1)
		}, "min only applies to expect_arrived"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := expectArrivedCase()
			c.edit(tc)
			err := tc.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, c.want)
			}
		})
	}
}

// The new keys read from case.yaml as the cases write them.
func TestScenarioReceiverExpectationsParse(t *testing.T) {
	var sc ScenarioConfig
	err := yaml.Unmarshal([]byte(`
steps:
  - do: expect_arrived
    receiver: b
    sent_after: b_down
    sent_before: b_early
    max: 2000
    record: early_kept
expect:
  max_loss: 0
  receivers:
    b:
      max_loss: 200000
      min_loss: 1
`), &sc)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	st := sc.Steps[0]
	if st.SentAfter != "b_down" || st.SentBefore != "b_early" || st.Max == nil || *st.Max != 2000 || st.Min != nil {
		t.Fatalf("step = %+v", st)
	}
	b := sc.Expect.Receivers["b"]
	if b.MaxLoss == nil || *b.MaxLoss != 200000 || b.MinLoss == nil || *b.MinLoss != 1 || b.MaxDuplicates != nil {
		t.Fatalf("expect.receivers.b = %+v", b)
	}
}
