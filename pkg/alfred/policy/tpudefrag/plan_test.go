package tpudefrag

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
)

var now = testutil.ReferenceTime

// full fills a 2x2x4 partition with two 2x2x2 Instances.
func (c *cluster) full(id string) *cluster {
	return c.partition("pod-a", id).instance("prod/"+id+"-x", id+"-0", id+"-1").instance("prod/"+id+"-y", id+"-2", id+"-3")
}

// TestBuildDrainsTheCheapestFeasiblePartition: two partitions each hold one
// single-host Instance. Draining A moves its Instance into B's tightest hole;
// adding B as well leaves nowhere outside the fence, so the drain is A alone.
func TestBuildDrainsTheCheapestFeasiblePartition(t *testing.T) {
	snap := newCluster(t).
		partition("pod-a", "A").partition("pod-a", "B").full("C").full("D").
		instance("prod/a", "A-0").instance("prod/b", "B-0").build()
	plan := Build(snap, testConfig(), now, nil)
	if plan.Unit != "2x2x4" || len(plan.Intents) != 1 {
		t.Fatalf("plan = %+v, want one drain on 2x2x4", plan)
	}
	d := plan.Intents[0]
	if !reflect.DeepEqual(d.Partitions, []string{"A"}) || len(d.Moves) != 1 || d.Moves[0].Key != key("prod/a", 0) || d.Moves[0].FromNode != "A-0" {
		t.Fatalf("drain = %+v, want partition A moving prod/a from A-0", d)
	}
	if !reflect.DeepEqual(d.Hosts, []string{"A-0", "A-1", "A-2", "A-3"}) || !reflect.DeepEqual(d.Busy, []string{"A-0"}) {
		t.Fatalf("drain hosts=%v busy=%v", d.Hosts, d.Busy)
	}
	if !d.Deadline.Equal(now.Add(240 * time.Minute)) {
		t.Fatalf("deadline = %v", d.Deadline)
	}
}

// TestBuildNeverBreaksAWholeFreeUnit: A's two-host Instance fits only in the
// wholly free partition D, so draining A would trade one free 2x2x4 for
// another. B's two single-host Instances fit in A's free half instead.
func TestBuildNeverBreaksAWholeFreeUnit(t *testing.T) {
	c := newCluster(t).
		partition("pod-a", "A").partition("pod-a", "B").full("C").partition("pod-a", "D").
		instance("prod/a", "A-0", "A-1").instance("prod/b0", "B-0").instance("prod/b2", "B-2")
	plan := Build(c.build(), testConfig(), now, nil)
	if len(plan.Intents) != 1 || !reflect.DeepEqual(plan.Intents[0].Partitions, []string{"B"}) {
		t.Fatalf("intents = %+v, want only B", plan.Intents)
	}

	// Without B to drain, A alone is never proposed.
	c = newCluster(t).
		partition("pod-a", "A").partition("pod-a", "B").full("C").partition("pod-a", "D").
		instance("prod/a", "A-0", "A-1").instance("prod/b0", "B-0").instance("prod/b2", "B-2")
	snap := c.build()
	for _, name := range []string{"prod/b0", "prod/b2"} {
		snap.Workloads[key(name, 0).Workload].Movable = false
	}
	if plan := Build(snap, testConfig(), now, nil); len(plan.Intents) != 0 {
		t.Fatalf("intents = %+v, want none: A's only placement breaks free partition D", plan.Intents)
	}
}

// TestBuildSkipsPartitionsItCannotEmpty covers every reason a partition is
// not a drain candidate.
func TestBuildSkipsPartitionsItCannotEmpty(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*snapshot.ClusterSnapshot)
	}{
		{"instance opted out", func(s *snapshot.ClusterSnapshot) { s.Workloads[key("prod/a", 0).Workload].Movable = false }},
		{"instance not steady", func(s *snapshot.ClusterSnapshot) {
			s.Workloads[key("prod/a", 0).Workload].Components["engine"].Instances[0].ReadyPods = 0
		}},
		{"host holds a non-OME accelerator pod", func(s *snapshot.ClusterSnapshot) {
			s.Nodes["A-1"].OtherOccupants = []snapshot.PodInfo{{Name: "batch", TPUs: 4}}
		}},
		{"host carries a slice no Instance owns", func(s *snapshot.ClusterSnapshot) {
			s.Nodes["A-2"].Labels["cloud.google.com/gke-tpu-slice"] = "orphan"
		}},
		{"host cordoned by someone else", func(s *snapshot.ClusterSnapshot) { s.Nodes["A-3"].Cordoned = true }},
		{"host under maintenance", func(s *snapshot.ClusterSnapshot) { s.Nodes["A-3"].Maintenance.Requested = true }},
		{"partition unhealthy", func(s *snapshot.ClusterSnapshot) { s.Nodes["A-3"].Labels[stateLabel("2x2x4")] = "UNHEALTHY" }},
		{"partition missing a host", func(s *snapshot.ClusterSnapshot) { delete(s.Nodes, "A-3") }},
		{"TPU slice moves disabled", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := newCluster(t).partition("pod-a", "A").partition("pod-a", "B").full("C").full("D").
				instance("prod/a", "A-0").instance("prod/b", "B-0").build()
			snap.Workloads[key("prod/b", 0).Workload].Movable = false
			cfg := testConfig()
			if tt.mutate != nil {
				tt.mutate(snap)
			} else {
				off := false
				cfg.TPUSliceMigrationEnabled = &off
			}
			if plan := Build(snap, cfg, now, nil); len(plan.Intents) != 0 {
				t.Fatalf("intents = %+v, want none", plan.Intents)
			}
		})
	}
}

// TestBuildRespectsLimits covers minGap, maxBusyHostsPerPartition,
// maxMovesPerWorkloadPerDrain, maxDrainPartitions and backoff. Partitions A
// and B each hold one Instance of prod/a; F holds two unmovable Instances and
// two free hosts, room for both moves.
func TestBuildRespectsLimits(t *testing.T) {
	base := func() *snapshot.ClusterSnapshot {
		snap := newCluster(t).partition("pod-a", "A").partition("pod-a", "B").partition("pod-a", "F").full("C").
			instance("prod/a", "A-0").instance("prod/a", "B-0").
			instance("prod/f0", "F-0").instance("prod/f2", "F-2").build()
		snap.Workloads[key("prod/f0", 0).Workload].Movable = false
		snap.Workloads[key("prod/f2", 0).Workload].Movable = false
		return snap
	}
	if plan := Build(base(), testConfig(), now, nil); len(plan.Intents) != 1 || !reflect.DeepEqual(plan.Intents[0].Partitions, []string{"A", "B"}) {
		t.Fatalf("intents = %+v, want one drain of A and B", plan.Intents)
	}
	cfg := testConfig()
	cfg.Policies.TPUDefragmentation.MinGap = 3
	if plan := Build(base(), cfg, now, nil); len(plan.Intents) != 0 {
		t.Fatalf("gap 2 under minGap 3: intents = %+v", plan.Intents)
	}
	cfg = testConfig()
	cfg.Policies.TPUDefragmentation.MaxMovesPerWorkloadPerDrain = 1
	if plan := Build(base(), cfg, now, nil); len(plan.Intents) != 1 || len(plan.Intents[0].Partitions) != 1 {
		t.Fatalf("one move per workload: intents = %+v, want one partition", plan.Intents)
	}
	cfg = testConfig()
	cfg.Policies.TPUDefragmentation.MaxDrainPartitions = 1
	if plan := Build(base(), cfg, now, nil); len(plan.Intents) != 1 || len(plan.Intents[0].Partitions) != 1 {
		t.Fatalf("maxDrainPartitions 1: intents = %+v", plan.Intents)
	}
	if plan := Build(base(), testConfig(), now, func(pod string) bool { return pod == "pod-a" }); len(plan.Intents) != 0 {
		t.Fatalf("pod in backoff: intents = %+v", plan.Intents)
	}

	busy := func() *snapshot.ClusterSnapshot {
		snap := newCluster(t).partition("pod-a", "A").partition("pod-a", "E").partition("pod-a", "F").full("C").
			instance("prod/a", "A-0").instance("prod/b", "A-2").
			instance("prod/e", "E-0").instance("prod/f", "F-0").build()
		snap.Workloads[key("prod/e", 0).Workload].Movable = false
		snap.Workloads[key("prod/f", 0).Workload].Movable = false
		return snap
	}
	if plan := Build(busy(), testConfig(), now, nil); len(plan.Intents) != 1 || len(plan.Intents[0].Moves) != 2 {
		t.Fatalf("two busy hosts within the limit: intents = %+v", plan.Intents)
	}
	cfg = testConfig()
	cfg.Policies.TPUDefragmentation.MaxBusyHostsPerPartition = 1
	if plan := Build(busy(), cfg, now, nil); len(plan.Intents) != 0 {
		t.Fatalf("two busy hosts over the limit of one: intents = %+v", plan.Intents)
	}
}

// TestBuildPredictsAcrossTPUPods: A's Instance runs only on tpu7x hosts, and
// the only other free host in pod-a is not one, so its replacement must land
// in pod-b, which slice-scheduler may also use.
func TestBuildPredictsAcrossTPUPods(t *testing.T) {
	build := func(withPodB bool) *snapshot.ClusterSnapshot {
		c := newCluster(t).partition("pod-a", "A").partition("pod-a", "B").full("C").
			instance("prod/a", "A-0").instance("prod/b0", "B-0").instance("prod/b1", "B-1").instance("prod/b2", "B-2")
		if withPodB {
			c.partition("pod-b", "Z").instance("prod/z0", "Z-0").instance("prod/z2", "Z-2")
		}
		snap := c.build()
		for name, w := range snap.Workloads {
			if name.Name != "a" {
				w.Movable = false
			}
		}
		snap.Nodes["B-3"].Labels["node.kubernetes.io/instance-type"] = "other"
		pods := snap.Workloads[key("prod/a", 0).Workload].Components["engine"].Instances[0].Pods
		pods[0].NodeSelector["node.kubernetes.io/instance-type"] = "tpu7x"
		return snap
	}
	plan := Build(build(true), testConfig(), now, nil)
	if len(plan.Intents) != 1 || plan.Intents[0].Pod != "pod-a" || !reflect.DeepEqual(plan.Intents[0].Partitions, []string{"A"}) {
		t.Fatalf("intents = %+v, want A in pod-a", plan.Intents)
	}
	if plan := Build(build(false), testConfig(), now, nil); len(plan.Intents) != 0 {
		t.Fatalf("intents = %+v, want none without room in pod-b", plan.Intents)
	}
}

// TestBuildOpensOneDrainPerCluster: every TPU pod below has a gap, and each
// pod's drain would move its Instances into the other pod. Two drains at once
// could fence each other's destinations, so the plan holds at most one, in
// the pod with the largest gap, and none while a drain is open.
func TestBuildOpensOneDrainPerCluster(t *testing.T) {
	podA := func(c *cluster, opts ...func(int) []testutil.NodeOption) *cluster {
		return c.partition("pod-a", "A", opts...).partition("pod-a", "B").
			instance("prod/a", "A-0").instance("prod/b", "B-0")
	}
	podB := func(c *cluster, partitions ...string) *cluster {
		for _, p := range partitions {
			c.partition("pod-b", p).instance("prod/"+p, p+"-0")
		}
		return c
	}
	tests := []struct {
		name    string
		snap    *snapshot.ClusterSnapshot
		wantPod string
	}{
		{"equal gaps go to the lower pod id", podB(podA(newCluster(t)), "Y", "Z").build(), "pod-a"},
		{"the larger gap goes first", podB(podA(newCluster(t)), "X", "Y", "Z").build(), "pod-b"},
		{"an open drain blocks every pod", podB(podA(newCluster(t),
			func(i int) []testutil.NodeOption { return fenced("d1", "A", now.Add(-time.Minute), i != 0)(i) }),
			"X", "Y", "Z").build(), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := Build(tc.snap, executeConfig(), now, nil)
			if tc.wantPod == "" {
				if len(plan.Open) != 1 || len(plan.Intents) != 0 {
					t.Fatalf("plan = %+v, want the open drain and no intent", plan)
				}
				return
			}
			if len(plan.Intents) != 1 || plan.Intents[0].Pod != tc.wantPod {
				t.Fatalf("intents = %+v, want one drain in %s", plan.Intents, tc.wantPod)
			}
		})
	}
	// Each pod on its own would be drained: the limit, not feasibility,
	// keeps the second drain out.
	for _, pod := range []string{"pod-a", "pod-b"} {
		backoff := func(p string) bool { return p != pod }
		plan := Build(podB(podA(newCluster(t)), "Y", "Z").build(), executeConfig(), now, backoff)
		if len(plan.Intents) != 1 || plan.Intents[0].Pod != pod {
			t.Fatalf("with only %s eligible: intents = %+v", pod, plan.Intents)
		}
	}
}

// TestOpenDrainLifecycle reads a drain back from its fence and checks when it
// settles, which partitions empty, and when it must end.
func TestOpenDrainLifecycle(t *testing.T) {
	settled := now.Add(-2 * time.Minute)
	build := func(fencedAt time.Time, extra func(*cluster)) *snapshot.ClusterSnapshot {
		c := newCluster(t).
			partition("pod-a", "A", func(i int) []testutil.NodeOption { return fenced("d1", "A", fencedAt, i != 0)(i) }).
			partition("pod-a", "B", func(i int) []testutil.NodeOption { return fenced("d1", "B", fencedAt, true)(i) }).
			partition("pod-a", "E").full("C").
			instance("prod/a", "A-0")
		if extra != nil {
			extra(c)
		}
		return c.build()
	}
	plan := Build(build(settled, nil), executeConfig(), now, nil)
	if len(plan.Open) != 1 || len(plan.Intents) != 0 {
		t.Fatalf("plan = %+v, want one open drain and no new one", plan)
	}
	d := plan.Open[0]
	if d.ID != "d1" || !d.Settled || d.End != "" || !reflect.DeepEqual(d.Empty, []string{"B"}) ||
		len(d.Missing) != 0 || len(d.Moves) != 1 || d.Moves[0].Key != key("prod/a", 0) {
		t.Fatalf("open drain = %+v", d)
	}

	if d := Build(build(now.Add(-10*time.Second), nil), executeConfig(), now, nil).Open[0]; d.Settled {
		t.Fatalf("a fence younger than fenceSettleSeconds settled: %+v", d)
	}

	shortWindow := func() *config.Config {
		cfg := executeConfig()
		cfg.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Thu"}, Start: "00:00", End: "12:30"}}
		return cfg
	}
	tests := []struct {
		name  string
		at    time.Time
		cfg   func() *config.Config
		extra func(*cluster)
		want  string
	}{
		{"deadline", now.Add(time.Hour), executeConfig, nil, OutcomeDeadline},
		{"window closed", now.Add(45 * time.Minute), shortWindow, nil, OutcomeWindowClosed},
		{"recommend-only", now, testConfig, nil, OutcomeAborted},
		{"slice landed on a free fenced host", now, executeConfig, func(c *cluster) { c.instance("prod/new", "B-1") }, OutcomeFenceNotHonored},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Build(build(settled, tt.extra), tt.cfg(), tt.at, nil).Open[0].End; got != tt.want {
				t.Fatalf("End = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestEvaluateEmitsMovesOnlyForSettledDrains: a settled drain yields an
// executable TPU slice move; a settling one and a planned one yield
// advisories.
func TestEvaluateEmitsMovesOnlyForSettledDrains(t *testing.T) {
	busyAtFence := func(fencedAt time.Time) func(int) []testutil.NodeOption {
		return func(i int) []testutil.NodeOption { return fenced("d1", "A", fencedAt, i != 0)(i) }
	}
	snap := newCluster(t).
		partition("pod-a", "A", busyAtFence(now.Add(-2*time.Minute))).
		partition("pod-a", "E").full("C").
		instance("prod/a", "A-0").build()
	got := (&Policy{}).Evaluate(snap, executeConfig())
	if len(got) != 1 {
		t.Fatalf("candidates = %+v, want one", got)
	}
	c := got[0]
	if c.Policy != PolicyName || c.Reason != policy.ReasonTPUFragmentation || !c.Executable || c.TPUSlice == nil ||
		c.TPUSlice.Topology != "2x2x1" || c.FromNode != "A-0" || c.Workload != key("prod/a", 0).Workload {
		t.Fatalf("candidate = %+v", c)
	}
	if again := (&Policy{}).Evaluate(snap, executeConfig()); !reflect.DeepEqual(got, again) {
		t.Fatalf("Evaluate is not a pure function of the snapshot:\n%+v\n%+v", got, again)
	}

	settling := newCluster(t).
		partition("pod-a", "A", busyAtFence(now)).
		partition("pod-a", "E").full("C").
		instance("prod/a", "A-0").build()
	if got := (&Policy{}).Evaluate(settling, executeConfig()); len(got) != 1 || got[0].Executable ||
		got[0].AdvisoryReason != policy.AdvisoryTPUDrainFenceSettling {
		t.Fatalf("settling candidates = %+v", got)
	}

	planned := newCluster(t).partition("pod-a", "A").partition("pod-a", "B").full("C").full("D").
		instance("prod/a", "A-0").instance("prod/b", "B-0").build()
	if got := (&Policy{}).Evaluate(planned, testConfig()); len(got) != 1 || got[0].Executable ||
		got[0].AdvisoryReason != policy.AdvisoryTPUDrainPlanned {
		t.Fatalf("planned candidates = %+v", got)
	}

	cfg := testConfig()
	off := false
	cfg.Policies.TPUDefragmentation.Enabled = &off
	if got := (&Policy{}).Evaluate(planned, cfg); got != nil {
		t.Fatalf("disabled policy emitted %+v", got)
	}
}

// TestPublishPartitions reports whole free partitions and the packing bound.
func TestPublishPartitions(t *testing.T) {
	snap := newCluster(t).partition("pod-a", "A").partition("pod-a", "B").full("C").
		instance("prod/a", "A-0").instance("prod/b", "B-0").build()
	m := metrics.New(prometheus.NewRegistry())
	PublishPartitions(snap, testConfig(), m)
	for _, tt := range []struct {
		vec   *prometheus.GaugeVec
		shape string
		want  float64
	}{
		{m.TPUFreePartitions, "2x2x4", 0},
		{m.TPUPackingBound, "2x2x4", 1},
		{m.TPUFreePartitions, "2x2x2", 2},
		{m.TPUPackingBound, "2x2x2", 3},
		{m.TPUFreePartitions, "2x2x1", 6},
	} {
		if got := promtestutil.ToFloat64(tt.vec.WithLabelValues("pod-a", tt.shape)); got != tt.want {
			t.Errorf("%s = %v, want %v", tt.shape, got, tt.want)
		}
	}
}

// TestDecodeFenceRejectsForeignValues: a value Alfred did not write makes the
// host someone else's cordon.
func TestDecodeFenceRejectsForeignValues(t *testing.T) {
	for _, v := range []string{"", "true", `{"drain":"d1"}`} {
		if _, ok := DecodeFence(v); ok {
			t.Errorf("DecodeFence(%q) accepted", v)
		}
	}
	snap := newCluster(t).partition("pod-a", "A", func(int) []testutil.NodeOption {
		return []testutil.NodeOption{testutil.NodeCordoned(), func(n *snapshot.Node) { n.DefragmentationFence = "garbage" }}
	}).build()
	if plan := Build(snap, executeConfig(), now, nil); len(plan.Open) != 0 {
		t.Fatalf("a foreign fence value opened a drain: %+v", plan.Open)
	}
}

// TestBuildPlansWithoutAMigrationAPI: recommend-only mode without a configured
// migration API still shows the drain, with the real advisory reason.
func TestBuildPlansWithoutAMigrationAPI(t *testing.T) {
	snap := newCluster(t).partition("pod-a", "A").partition("pod-a", "B").full("C").full("D").
		instance("prod/a", "A-0").instance("prod/b", "B-0").build()
	snap.OMENativeExecutor.Available = false
	if plan := Build(snap, testConfig(), now, nil); len(plan.Intents) != 1 {
		t.Fatalf("intents = %+v, want one", plan.Intents)
	}
	got := (&Policy{}).Evaluate(snap, testConfig())
	if len(got) != 1 || got[0].Executable || got[0].AdvisoryReason != policy.AdvisoryTPUDrainPlanned {
		t.Fatalf("candidates = %+v", got)
	}
	if snap.OMENativeExecutor.Available {
		t.Fatal("planning changed the caller's snapshot")
	}
}

// TestOpenDrainNeverRefencesAnEmptiedPartition: B emptied, and releasing it
// failed on B-2 alone. B's other hosts are not missing from the fence, since
// an emptied partition is only ever released, so A's moves still settle.
func TestOpenDrainNeverRefencesAnEmptiedPartition(t *testing.T) {
	settled := now.Add(-2 * time.Minute)
	snap := newCluster(t).
		partition("pod-a", "A", func(i int) []testutil.NodeOption { return fenced("d1", "A", settled, i != 0)(i) }).
		partition("pod-a", "B", func(i int) []testutil.NodeOption {
			if i == 2 {
				return fenced("d1", "B", settled, true)(i)
			}
			return nil
		}).
		partition("pod-a", "E").full("C").
		instance("prod/a", "A-0").build()
	plan := Build(snap, executeConfig(), now, nil)
	if len(plan.Open) != 1 {
		t.Fatalf("open = %+v, want one drain", plan.Open)
	}
	if d := plan.Open[0]; len(d.Missing) != 0 || !reflect.DeepEqual(d.Empty, []string{"B"}) || !d.Settled || d.End != "" {
		t.Fatalf("open drain = %+v, want B empty, no host missing, settled", d)
	}
}

// TestOpenDrainStopsOrWaitsWhenItsHostsChange: a drain ends when a fenced
// host was uncordoned or no move can be submitted, and does not settle while
// a source partition lacks a host.
func TestOpenDrainStopsOrWaitsWhenItsHostsChange(t *testing.T) {
	build := func() *snapshot.ClusterSnapshot {
		return newCluster(t).
			partition("pod-a", "A", func(i int) []testutil.NodeOption { return fenced("d1", "A", now.Add(-2*time.Minute), i != 0)(i) }).
			partition("pod-a", "E").full("C").
			instance("prod/a", "A-0").build()
	}
	if d := Build(build(), executeConfig(), now, nil).Open[0]; !d.Settled || d.End != "" {
		t.Fatalf("unchanged drain = %+v, want settled and open", d)
	}
	stripPartitionLabels := func(s *snapshot.ClusterSnapshot) {
		for k := range s.Nodes["A-1"].Labels {
			if strings.HasPrefix(k, "cloud.google.com/gke-tpu-partition-") {
				delete(s.Nodes["A-1"].Labels, k)
			}
		}
	}
	tests := []struct {
		name   string
		mutate func(*snapshot.ClusterSnapshot)
		want   string
	}{
		{"someone uncordoned a fenced host", func(s *snapshot.ClusterSnapshot) { s.Nodes["A-1"].Cordoned = false }, OutcomeFenceNotHonored},
		{"no migration API is configured", func(s *snapshot.ClusterSnapshot) { s.OMENativeExecutor.Available = false }, OutcomeAborted},
		{"a source host left the cluster", func(s *snapshot.ClusterSnapshot) { delete(s.Nodes, "A-3") }, ""},
		{"a fenced host lost its partition labels", stripPartitionLabels, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := build()
			tt.mutate(snap)
			plan := Build(snap, executeConfig(), now, nil)
			if len(plan.Open) != 1 {
				t.Fatalf("open = %+v, want one drain", plan.Open)
			}
			d := plan.Open[0]
			if d.End != tt.want {
				t.Fatalf("End = %q, want %q", d.End, tt.want)
			}
			if tt.want == "" && d.Settled {
				t.Fatalf("drain settled with a source partition short of a host: %+v", d)
			}
			if _, ok := snap.Nodes["A-1"]; ok && !contains(d.Fenced, "A-1") {
				t.Fatalf("fenced = %v, want A-1 still read back", d.Fenced)
			}
		})
	}
}

// TestBuildPredictsInDispatchOrder: A holds a 2x2x1 Instance and a 2x2x2 one
// (prod/b) that runs only on tpu7x hosts. Outside A, G-01 is the only free
// 2x2x2 on tpu7x hosts; M-01 is free but on other hosts. Both moves leave
// the same partition, so the dispatcher submits them by name. When the 2x2x1
// goes first, best fit puts it on G-0 and prod/b has nowhere to go.
func TestBuildPredictsInDispatchOrder(t *testing.T) {
	build := func(small string) *snapshot.ClusterSnapshot {
		snap := newCluster(t).
			partition("pod-a", "A").partition("pod-a", "G").partition("pod-a", "M").full("C").
			instance(small, "A-0").instance("prod/b", "A-2", "A-3").
			instance("prod/g", "G-2", "G-3").instance("prod/m", "M-2", "M-3").build()
		for _, name := range []string{"prod/g", "prod/m"} {
			snap.Workloads[key(name, 0).Workload].Movable = false
		}
		for i := 0; i < 4; i++ {
			snap.Nodes[fmt.Sprintf("M-%d", i)].Labels["node.kubernetes.io/instance-type"] = "other"
		}
		for _, pod := range snap.Workloads[key("prod/b", 0).Workload].Components["engine"].Instances[0].Pods {
			pod.NodeSelector["node.kubernetes.io/instance-type"] = "tpu7x"
		}
		return snap
	}
	if plan := Build(build("prod/a"), testConfig(), now, nil); len(plan.Intents) != 0 {
		t.Fatalf("intents = %+v, want none: prod/a moves first and takes G-0, which prod/b needs", plan.Intents)
	}
	if plan := Build(build("prod/z"), testConfig(), now, nil); len(plan.Intents) != 1 {
		t.Fatalf("intents = %+v, want A drained: prod/b moves first and takes G-01", plan.Intents)
	}
}

// TestHostsWithoutATPUPodAreNeverPlanned: hosts without the pod label could
// belong to different TPU pods, so they get no drain and no gauge.
func TestHostsWithoutATPUPodAreNeverPlanned(t *testing.T) {
	c := newCluster(t)
	for _, id := range []string{"A", "B", "C", "D"} {
		c.partition("", id)
	}
	snap := c.instance("prod/c0", "C-0", "C-1").instance("prod/c2", "C-2", "C-3").
		instance("prod/d0", "D-0", "D-1").instance("prod/d2", "D-2", "D-3").
		instance("prod/a", "A-0").instance("prod/b", "B-0").build()
	if plan := Build(snap, testConfig(), now, nil); len(plan.Intents) != 0 {
		t.Fatalf("intents = %+v, want none", plan.Intents)
	}
	m := metrics.New(prometheus.NewRegistry())
	PublishPartitions(snap, testConfig(), m)
	if n := promtestutil.CollectAndCount(m.TPUFreePartitions); n != 0 {
		t.Fatalf("published %d free-partition series for unlabeled hosts, want 0", n)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
