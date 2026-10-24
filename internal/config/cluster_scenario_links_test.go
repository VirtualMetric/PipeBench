package config

import (
	"strings"
	"testing"
)

// linkScenarioCase is validScenarioCase with every link and disk step added.
func linkScenarioCase() *TestCase {
	tc := validScenarioCase()
	sc := tc.Cluster.Scenario
	sc.NodeDisk = &ScenarioNodeDisk{Path: "/opt/vmetric/storage", Size: "1g", Nodes: []int{3}}
	sc.Steps = append(sc.Steps,
		ScenarioStep{Do: "cut_link", Node: "leader", Peer: "follower", OneWay: true},
		ScenarioStep{Do: "shape_link", Node: "all", Peers: []string{"all"}, Latency: "200ms", Jitter: "20ms", Loss: "20%"},
		ScenarioStep{Do: "split", Groups: [][]string{{"1", "2"}, {"3", "4"}}},
		ScenarioStep{Do: "restore_link", Node: "1", Peer: "2"},
		ScenarioStep{Do: "cut_link", Node: "1", Peer: "bench-receiver"},
		ScenarioStep{Do: "restore_link"},
		ScenarioStep{Do: "fill_disk", Node: "3", Leave: "4m", As: "full"},
		ScenarioStep{Do: "free_disk", Node: "@full"},
		ScenarioStep{Do: "start", Node: "3", EmptyDisk: true},
	)
	return tc
}

func TestValidateAcceptsLinkAndDiskSteps(t *testing.T) {
	if err := linkScenarioCase().Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateRejectsBadLinkAndDiskSteps(t *testing.T) {
	// last is the index of a step linkScenarioCase appended, counted from the
	// end: -1 is the last.
	step := func(tc *TestCase, last int) *ScenarioStep {
		s := tc.Cluster.Scenario.Steps
		return &s[len(s)+last]
	}
	const (
		cut      = -9
		shape    = -8
		split    = -7
		restore1 = -6
		cutRecv  = -5
		restore  = -4
		fill     = -3
		start    = -1
	)
	cases := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"peer and peers":            {func(tc *TestCase) { step(tc, cut).Peers = []string{"2"} }, "names peer and peers"},
		"empty peers":               {func(tc *TestCase) { step(tc, shape).Peers = []string{} }, "peers is empty"},
		"cut without node":          {func(tc *TestCase) { step(tc, cut).Node = "" }, "needs node"},
		"cut without peer":          {func(tc *TestCase) { step(tc, cut).Peer = "" }, "needs peer or peers"},
		"bad peer":                  {func(tc *TestCase) { step(tc, cut).Peer = "receiver" }, `peer "receiver"`},
		"node is its own peer":      {func(tc *TestCase) { step(tc, restore1).Peer = "1" }, "a link has two ends"},
		"shape with nothing":        {func(tc *TestCase) { s := step(tc, shape); s.Latency, s.Jitter, s.Loss = "", "", "" }, "needs latency or loss"},
		"jitter without latency":    {func(tc *TestCase) { step(tc, shape).Latency = "" }, "jitter varies the latency"},
		"loss without percent":      {func(tc *TestCase) { step(tc, shape).Loss = "20" }, "percentage"},
		"loss over 100":             {func(tc *TestCase) { step(tc, shape).Loss = "120%" }, "at most 100"},
		"split with one side":       {func(tc *TestCase) { step(tc, split).Groups = [][]string{{"1", "2"}} }, "at least two lists"},
		"split side empty":          {func(tc *TestCase) { step(tc, split).Groups = [][]string{{"1"}, {}} }, "groups[1] is empty"},
		"node on both sides":        {func(tc *TestCase) { step(tc, split).Groups = [][]string{{"1", "2"}, {"2", "3"}} }, "is in groups[0] and groups[1]"},
		"all in a split":            {func(tc *TestCase) { step(tc, split).Groups = [][]string{{"1"}, {"all"}} }, "a group lists nodes"},
		"split with node":           {func(tc *TestCase) { step(tc, split).Node = "1" }, "takes groups, not node"},
		"restore all with peers":    {func(tc *TestCase) { step(tc, restore).Peer = "2" }, "need a node"},
		"loss on a cut":             {func(tc *TestCase) { step(tc, cutRecv).Loss = "10%" }, "loss only applies to shape_link"},
		"one_way on a split":        {func(tc *TestCase) { step(tc, split).OneWay = true }, "one_way only applies to"},
		"groups on a cut":           {func(tc *TestCase) { step(tc, cut).Groups = [][]string{{"1"}, {"2"}} }, "groups only applies to split"},
		"empty_disk on a stop":      {func(tc *TestCase) { tc.Cluster.Scenario.Steps[2].EmptyDisk = true }, "empty_disk only applies to start"},
		"leave on free_disk":        {func(tc *TestCase) { step(tc, -2).Leave = "1m" }, "leave only applies to fill_disk"},
		"fill without a disk":       {func(tc *TestCase) { tc.Cluster.Scenario.NodeDisk = nil }, "needs cluster.scenario.node_disk"},
		"fill on a node without":    {func(tc *TestCase) { step(tc, fill).Node = "2" }, "node 2 has no disk of its own"},
		"bad leave":                 {func(tc *TestCase) { step(tc, fill).Leave = "lots" }, `leave "lots"`},
		"disk path not absolute":    {func(tc *TestCase) { tc.Cluster.Scenario.NodeDisk.Path = "storage" }, "node_disk.path"},
		"disk too small":            {func(tc *TestCase) { tc.Cluster.Scenario.NodeDisk.Size = "16m" }, "at least 64m"},
		"disk on a node that isn't": {func(tc *TestCase) { tc.Cluster.Scenario.NodeDisk.Nodes = []int{7} }, "7 is not a node"},
		"start on empty disk, bad":  {func(tc *TestCase) { step(tc, start).Node = "" }, "needs node"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := linkScenarioCase()
			c.edit(tc)
			err := tc.Validate()
			if err == nil {
				t.Fatalf("Validate accepted the case")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate error %q does not mention %q", err, c.want)
			}
		})
	}
}

// rowsScenarioCase is a database case: no generator, a postgres database, a
// writer, and the rows check.
func rowsScenarioCase() *TestCase {
	zero := int64(0)
	return &TestCase{
		Name:      "rows",
		Type:      ClusterScenarioType,
		Receiver:  ReceiverConfig{Mode: "tcp", Listen: ":9001"},
		Database:  &DatabaseConfig{Engine: "postgres", SeedSQL: "CREATE TABLE events (id BIGINT, message TEXT);"},
		Endpoints: []Endpoint{{Name: "pg-writer", Image: "postgres:16.2"}},
		Cluster: &ClusterConfig{Nodes: 3, Scenario: &ScenarioConfig{
			Steps: []ScenarioStep{{Do: "wait_ready"}, {Do: "sleep", For: "30s"}},
			Rows:  &ScenarioRows{Query: "SELECT message FROM events", Key: "evt-[0-9]+", Writer: "pg-writer"},
			Expect: ScenarioExpect{
				MaxMissingRows: &zero,
			},
		}},
	}
}

func TestValidateRowsCheck(t *testing.T) {
	if err := rowsScenarioCase().validateClusterScenario(); err != nil {
		t.Fatalf("validateClusterScenario: %v", err)
	}
	cases := map[string]struct {
		edit func(tc *TestCase)
		want string
	}{
		"no query":           {func(tc *TestCase) { tc.Cluster.Scenario.Rows.Query = " " }, "needs query"},
		"no key":             {func(tc *TestCase) { tc.Cluster.Scenario.Rows.Key = "" }, "needs key"},
		"bad key":            {func(tc *TestCase) { tc.Cluster.Scenario.Rows.Key = "evt-(" }, "rows.key"},
		"backslash in key":   {func(tc *TestCase) { tc.Cluster.Scenario.Rows.Key = `evt-\d+` }, "must not contain a backslash"},
		"with a generator":   {func(tc *TestCase) { tc.Generator = GeneratorConfig{Mode: "http_ack", Connections: 1, Rate: 10} }, "has a generator"},
		"not postgres":       {func(tc *TestCase) { tc.Database.Engine = "mysql" }, "engine postgres"},
		"no database":        {func(tc *TestCase) { tc.Database = nil }, "engine postgres"},
		"unknown writer":     {func(tc *TestCase) { tc.Cluster.Scenario.Rows.Writer = "ghost" }, `writer "ghost"`},
		"negative drain":     {func(tc *TestCase) { tc.Cluster.Scenario.Rows.DrainSeconds = -1 }, "drain_seconds"},
		"judge without rows": {func(tc *TestCase) { tc.Cluster.Scenario.Rows = nil }, "max_missing_rows needs"},
		"twice without rows": {func(tc *TestCase) {
			tc.Cluster.Scenario.Rows = nil
			tc.Cluster.Scenario.Expect.MaxMissingRows = nil
			zero := int64(0)
			tc.Cluster.Scenario.Expect.MaxRowsTwice = &zero
		}, "max_rows_twice needs"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tc := rowsScenarioCase()
			c.edit(tc)
			err := tc.validateClusterScenario()
			if err == nil {
				t.Fatalf("validateClusterScenario accepted the case")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not mention %q", err, c.want)
			}
		})
	}
}

func TestLossPercent(t *testing.T) {
	for in, want := range map[string]float64{"": 0, "20%": 20, " 12.5 %": 12.5, "100%": 100} {
		got, err := LossPercent(in)
		if err != nil || got != want {
			t.Errorf("LossPercent(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"20", "0%", "-5%", "101%", "x%", "NaN%", "nan%"} {
		if _, err := LossPercent(in); err == nil {
			t.Errorf("LossPercent(%q) accepted it", in)
		}
	}
}

func TestNodeDiskHasNode(t *testing.T) {
	var none *ScenarioNodeDisk
	if none.HasNode(1) {
		t.Fatal("a nil node_disk gave node 1 a disk")
	}
	every := &ScenarioNodeDisk{Path: "/d", Size: "1g"}
	if !every.HasNode(2) {
		t.Fatal("node_disk without nodes left node 2 out")
	}
	some := &ScenarioNodeDisk{Path: "/d", Size: "1g", Nodes: []int{3}}
	if some.HasNode(2) || !some.HasNode(3) {
		t.Fatal("node_disk.nodes [3] chose the wrong nodes")
	}
}

func TestValidateWaitLeaderGroups(t *testing.T) {
	withGroups := func(g [][]string) *TestCase {
		tc := validScenarioCase()
		tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps, ScenarioStep{Do: "wait_leader", Groups: g})
		return tc
	}
	if err := withGroups([][]string{{"1", "2"}, {"3", "4"}}).Validate(); err != nil {
		t.Fatalf("wait_leader with two sides was refused: %v", err)
	}
	if err := withGroups([][]string{{"1", "2", "3"}}).Validate(); err != nil {
		t.Fatalf("wait_leader with one group was refused: %v", err)
	}
	for name, c := range map[string]struct {
		groups [][]string
		want   string
	}{
		"empty groups":       {[][]string{}, "groups is empty"},
		"empty group":        {[][]string{{"1"}, {}}, "groups[1] is empty"},
		"node on both sides": {[][]string{{"1", "2"}, {"2"}}, "is in groups[0] and groups[1]"},
		"bad node":           {[][]string{{"9"}}, "node must be"},
	} {
		t.Run(name, func(t *testing.T) {
			err := withGroups(c.groups).Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate = %v, want an error about %q", err, c.want)
			}
		})
	}
	tc := validScenarioCase()
	tc.Cluster.Scenario.Steps[2].Groups = [][]string{{"1"}, {"2"}}
	if err := tc.Validate(); err == nil || !strings.Contains(err.Error(), "groups only applies to split, wait_leader") {
		t.Fatalf("groups on a kill: Validate = %v", err)
	}
}

func TestValidateExpectDeliveryContinues(t *testing.T) {
	withStep := func(st ScenarioStep) *TestCase {
		tc := linkScenarioCase()
		tc.Cluster.Scenario.Steps = append(tc.Cluster.Scenario.Steps, st)
		return tc
	}
	if err := withStep(ScenarioStep{Do: "expect_delivery_continues", For: "60s", Gap: "10s"}).Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := withStep(ScenarioStep{Do: "expect_delivery_continues"}).Validate(); err != nil {
		t.Fatalf("Validate with the defaults: %v", err)
	}
	if err := withStep(ScenarioStep{Do: "expect_writes_continue", For: "30s", Gap: "10s"}).Validate(); err != nil {
		t.Fatalf("Validate expect_writes_continue with a gap: %v", err)
	}
	// The step counts the load's lines: a case without a generator has none.
	noLoad := rowsScenarioCase()
	noLoad.Cluster.Scenario.Steps = append(noLoad.Cluster.Scenario.Steps, ScenarioStep{Do: "expect_delivery_continues"})
	if err := noLoad.validateClusterScenario(); err == nil || !strings.Contains(err.Error(), "has no generator") {
		t.Errorf("Validate without a generator = %v, want the generator error", err)
	}
	bad := map[string]struct {
		st   ScenarioStep
		want string
	}{
		"gap too short":    {ScenarioStep{Do: "expect_delivery_continues", Gap: "1s"}, "gap must be at least 2s"},
		"for within gap":   {ScenarioStep{Do: "expect_delivery_continues", For: "5s", Gap: "10s"}, "for must be longer than gap"},
		"gap not a time":   {ScenarioStep{Do: "expect_delivery_continues", Gap: "soon"}, "gap"},
		"gap on another":   {ScenarioStep{Do: "wait_delivery", Gap: "10s"}, "gap only applies to expect_delivery_continues, expect_writes_continue"},
		"writes gap short": {ScenarioStep{Do: "expect_writes_continue", For: "30s", Gap: "1s"}, "gap must be at least 2s"},
		"writes gap long":  {ScenarioStep{Do: "expect_writes_continue", Gap: "25s"}, "for must be longer than gap"},
	}
	for name, c := range bad {
		err := withStep(c.st).Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Validate = %v, want an error with %q", name, err, c.want)
		}
	}
}
