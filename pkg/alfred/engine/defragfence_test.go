package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/policy/tpudefrag"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const fenceMaintenanceLabel = "maintenance.example.com/start-at"

var fenceNow = testutil.ReferenceTime

// fenceCluster builds 2x2x4 partitions of four hosts each, as in the
// tpudefrag tests: partition P has hosts P-0..P-3 and halves P-01 and P-23.
type fenceCluster struct{ b *testutil.SnapshotBuilder }

func newFenceCluster() *fenceCluster { return &fenceCluster{b: testutil.NewSnapshot()} }

func (c *fenceCluster) partition(id string, opts ...func(int) []testutil.NodeOption) *fenceCluster {
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("%s-%d", id, i)
		half := id + "-01"
		if i >= 2 {
			half = id + "-23"
		}
		labels := map[string]string{"cloud.google.com/gce-topology-block": "pod-a"}
		for shape, pid := range map[string]string{"2x2x4": id, "2x2x2": half, "2x2x1": name} {
			labels["cloud.google.com/gke-tpu-partition-"+shape+"-id"] = pid
			labels["cloud.google.com/gke-tpu-partition-"+shape+"-state"] = "HEALTHY"
		}
		nodeOpts := []testutil.NodeOption{testutil.NodeLabels(labels)}
		for _, o := range opts {
			nodeOpts = append(nodeOpts, o(i)...)
		}
		c.b.WithNode(name, "", 0, nodeOpts...)
	}
	return c
}

func (c *fenceCluster) instance(workload string, nodes ...string) *fenceCluster {
	c.b.WithMultiPodTPUInstance(workload, v1beta1.EngineComponent, constants.OMENative, 4, nodes...)
	return c
}

func (c *fenceCluster) full(id string) *fenceCluster {
	return c.partition(id).instance("prod/"+id+"-x", id+"-0", id+"-1").instance("prod/"+id+"-y", id+"-2", id+"-3")
}

func (c *fenceCluster) build() *snapshot.ClusterSnapshot {
	snap := c.b.Build()
	topology := map[int]string{1: "2x2x1", 2: "2x2x2", 4: "2x2x4"}
	for _, w := range snap.Workloads {
		for _, comp := range w.Components {
			for _, inst := range comp.Instances {
				for i := range inst.Pods {
					inst.Pods[i].TPUSliceProvisioned = true
					inst.Pods[i].NodeSelector = map[string]string{"cloud.google.com/gke-tpu-topology": topology[len(inst.Pods)]}
				}
			}
		}
	}
	return snap
}

// fenceOn marks hosts as fenced by drain d1 on the given partition; host 0
// held a slice when it was fenced.
func fenceOn(partition string, deadline time.Time) func(int) []testutil.NodeOption {
	return func(i int) []testutil.NodeOption {
		raw, _ := json.Marshal(tpudefrag.Fence{Drain: "d1", Unit: "2x2x4", Partition: partition,
			Deadline: deadline, FencedAt: fenceNow.Add(-5 * time.Minute), FreeAtFence: i != 0})
		return []testutil.NodeOption{testutil.NodeCordoned(), func(n *snapshot.Node) { n.DefragmentationFence = string(raw) }}
	}
}

func fenceConfig(mode string) *config.Config {
	cfg := config.Default()
	on := true
	cfg.Mode = mode
	cfg.TPUSliceMigrationEnabled = &on
	td := &cfg.Policies.TPUDefragmentation
	td.Enabled = &on
	td.TargetShapes = []string{"2x2x4"}
	td.MinGap = 1
	td.MaxBusyHostsPerPartition = 3
	cfg.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Thu"}, Start: "00:00", End: "23:59"}}
	cfg.Policies.NodeHealth.Maintenance.Triggers = []config.MaintenanceTrigger{
		{Name: "de-schedule", Label: &config.MaintenanceLabel{Key: fenceMaintenanceLabel, ValueIsStartTime: true}, Cordon: true},
	}
	return cfg
}

// liveNodes mirrors the snapshot's nodes as API objects.
func liveNodes(snap *snapshot.ClusterSnapshot) []client.Object {
	var out []client.Object
	for _, n := range snap.Nodes {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: n.Name, UID: n.UID, Labels: map[string]string{}, Annotations: map[string]string{}}}
		for k, v := range n.Labels {
			node.Labels[k] = v
		}
		node.Spec.Unschedulable = n.Cordoned
		if n.DefragmentationFence != "" {
			node.Annotations[snapshot.DefragmentationFenceAnnotation] = n.DefragmentationFence
		}
		if n.HoldCordon {
			node.Annotations[snapshot.HoldCordonAnnotation] = "true"
		}
		out = append(out, node)
	}
	return out
}

type fenceFixture struct {
	f  *DefragmentationFencer
	cl client.Client
	m  *metrics.Metrics
}

func newFenceFixture(snap *snapshot.ClusterSnapshot, mutate func(*corev1.Node)) *fenceFixture {
	return newFenceFixtureWith(snap, mutate, interceptor.Funcs{})
}

// newFenceFixtureWith is newFenceFixture with funcs intercepting API calls.
func newFenceFixtureWith(snap *snapshot.ClusterSnapshot, mutate func(*corev1.Node), funcs interceptor.Funcs) *fenceFixture {
	objs := liveNodes(snap)
	if mutate != nil {
		for _, o := range objs {
			mutate(o.(*corev1.Node))
		}
	}
	cl := fake.NewClientBuilder().WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
	m := metrics.New(prometheus.NewRegistry())
	return &fenceFixture{
		f:  &DefragmentationFencer{Client: cl, Recorder: record.NewFakeRecorder(64), Metrics: m, Log: logr.Discard(), Now: func() time.Time { return fenceNow }},
		cl: cl,
		m:  m,
	}
}

func (x *fenceFixture) node(t *testing.T, name string) corev1.Node {
	t.Helper()
	var node corev1.Node
	if err := x.cl.Get(context.Background(), types.NamespacedName{Name: name}, &node); err != nil {
		t.Fatal(err)
	}
	return node
}

func (x *fenceFixture) drains(outcome string) float64 {
	return promtestutil.ToFloat64(x.m.TPUDrains.WithLabelValues(outcome))
}

// drainable: A and B each hold one single-host Instance, C and D are full.
// The policy proposes draining A.
func drainable() *fenceCluster {
	return newFenceCluster().partition("A").partition("B").full("C").full("D").
		instance("prod/a", "A-0").instance("prod/b", "B-0")
}

func TestFencerFencesTheProposedDrain(t *testing.T) {
	snap := drainable().build()
	x := newFenceFixture(snap, nil)
	x.f.Reconcile(context.Background(), snap, fenceConfig(config.ModeExecute))
	var drain string
	for i := 0; i < 4; i++ {
		node := x.node(t, fmt.Sprintf("A-%d", i))
		fence, ok := tpudefrag.DecodeFence(node.Annotations[snapshot.DefragmentationFenceAnnotation])
		if !node.Spec.Unschedulable || !ok || fence.Partition != "A" || fence.Unit != "2x2x4" ||
			!fence.Deadline.Equal(fenceNow.Add(240*time.Minute)) || fence.FreeAtFence != (i != 0) {
			t.Fatalf("A-%d not fenced as expected: unschedulable=%v fence=%+v", i, node.Spec.Unschedulable, fence)
		}
		if drain == "" {
			drain = fence.Drain
		} else if fence.Drain != drain {
			t.Fatalf("hosts carry different drains %q and %q", drain, fence.Drain)
		}
	}
	if node := x.node(t, "B-1"); node.Spec.Unschedulable {
		t.Fatal("B was fenced too")
	}
	if got := promtestutil.ToFloat64(x.m.TPUFencedHosts); got != 4 {
		t.Fatalf("fenced hosts = %v, want 4", got)
	}
}

func TestFencerFencesNothingOutsideExecuteOrWindow(t *testing.T) {
	recommend := fenceConfig(config.ModeRecommendOnly)
	closed := fenceConfig(config.ModeExecute)
	closed.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Sun"}, Start: "00:00", End: "01:00"}}
	for name, cfg := range map[string]*config.Config{"recommend-only": recommend, "window closed": closed} {
		t.Run(name, func(t *testing.T) {
			snap := drainable().build()
			x := newFenceFixture(snap, nil)
			x.f.Reconcile(context.Background(), snap, cfg)
			if node := x.node(t, "A-1"); node.Spec.Unschedulable || len(node.Annotations) != 0 {
				t.Fatalf("A-1 was written: %+v", node)
			}
		})
	}
}

// TestFencerRemovesAPartialFence: a host the snapshot saw free was cordoned by
// someone else before the fencer reached it. The fencer removes the fence it
// already wrote and leaves that host's cordon alone.
func TestFencerRemovesAPartialFence(t *testing.T) {
	snap := drainable().build()
	x := newFenceFixture(snap, func(n *corev1.Node) {
		if n.Name == "A-2" {
			n.Spec.Unschedulable = true
		}
	})
	x.f.Reconcile(context.Background(), snap, fenceConfig(config.ModeExecute))
	for _, name := range []string{"A-0", "A-1", "A-3"} {
		if node := x.node(t, name); node.Spec.Unschedulable || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
			t.Fatalf("%s kept the fence: %+v", name, node)
		}
	}
	if node := x.node(t, "A-2"); !node.Spec.Unschedulable {
		t.Fatal("the other cordon on A-2 was removed")
	}
	if got := x.drains(tpudefrag.OutcomeCordonFailed); got != 1 {
		t.Fatalf("cordon_failed = %v, want 1", got)
	}
}

// TestFencerReleasesEmptiedPartitions: B emptied, A still holds an Instance.
// B is uncordoned now; once A empties too, the drain completes.
func TestFencerReleasesEmptiedPartitions(t *testing.T) {
	deadline := fenceNow.Add(time.Hour)
	snap := newFenceCluster().partition("A", fenceOn("A", deadline)).partition("B", fenceOn("B", deadline)).
		partition("E").full("C").instance("prod/a", "A-0").build()
	x := newFenceFixture(snap, nil)
	cfg := fenceConfig(config.ModeExecute)
	x.f.Reconcile(context.Background(), snap, cfg)
	for i := 0; i < 4; i++ {
		if node := x.node(t, fmt.Sprintf("B-%d", i)); node.Spec.Unschedulable || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
			t.Fatalf("B-%d was not released: %+v", i, node)
		}
		if node := x.node(t, fmt.Sprintf("A-%d", i)); !node.Spec.Unschedulable {
			t.Fatalf("A-%d was released while it still holds an Instance", i)
		}
	}
	if got := x.drains(tpudefrag.OutcomeCompleted); got != 0 {
		t.Fatalf("completed = %v before A emptied", got)
	}

	emptied := newFenceCluster().partition("A", fenceOn("A", deadline)).partition("E").full("C").build()
	x.f.Client, x.f.Reader = x.cl, nil
	x.f.Reconcile(context.Background(), emptied, cfg)
	if node := x.node(t, "A-0"); node.Spec.Unschedulable {
		t.Fatal("A-0 was not released after A emptied")
	}
	if got := x.drains(tpudefrag.OutcomeCompleted); got != 1 {
		t.Fatalf("completed = %v, want 1", got)
	}
}

// TestFencerEndsADrainByTheHandOffRules: at the deadline, a healthy host is
// uncordoned; an unhealthy host, a host under a cordoning maintenance rule and
// a host with hold-cordon stay cordoned.
func TestFencerEndsADrainByTheHandOffRules(t *testing.T) {
	expired := fenceNow.Add(-time.Minute)
	snap := newFenceCluster().
		partition("A", fenceOn("A", expired), func(i int) []testutil.NodeOption {
			switch i {
			case 1:
				return []testutil.NodeOption{testutil.NodeUnhealthy()}
			case 2:
				return []testutil.NodeOption{func(n *snapshot.Node) { n.HoldCordon = true }}
			case 3:
				return []testutil.NodeOption{testutil.NodeLabels(map[string]string{fenceMaintenanceLabel: "1"}), testutil.NodeMaintenance("de-schedule")}
			}
			return nil
		}).
		partition("E").full("C").instance("prod/a", "A-0").build()
	x := newFenceFixture(snap, nil)
	x.f.Reconcile(context.Background(), snap, fenceConfig(config.ModeExecute))
	for i, wantCordoned := range []bool{false, true, true, true} {
		node := x.node(t, fmt.Sprintf("A-%d", i))
		if node.Spec.Unschedulable != wantCordoned || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
			t.Fatalf("A-%d: unschedulable=%v annotations=%v, want cordoned=%v and no fence", i, node.Spec.Unschedulable, node.Annotations, wantCordoned)
		}
	}
	if got := x.node(t, "A-3").Annotations[CordonedForMaintenanceAnnotation]; got != "de-schedule" {
		t.Fatalf("A-3 maintenance annotation = %q, want de-schedule", got)
	}
	if got := x.drains(tpudefrag.OutcomeDeadline); got != 1 {
		t.Fatalf("deadline = %v, want 1", got)
	}
}

// TestFencerBacksOffAfterAFenceIsNotHonored: a slice landed on a host that was
// free when fenced. The drain ends, and no new drain opens in that TPU pod.
func TestFencerBacksOffAfterAFenceIsNotHonored(t *testing.T) {
	deadline := fenceNow.Add(time.Hour)
	snap := newFenceCluster().partition("A", fenceOn("A", deadline)).partition("B").full("C").full("D").
		instance("prod/a", "A-0").instance("prod/b", "B-0").instance("prod/new", "A-1").build()
	x := newFenceFixture(snap, nil)
	cfg := fenceConfig(config.ModeExecute)
	x.f.Reconcile(context.Background(), snap, cfg)
	if got := x.drains(tpudefrag.OutcomeFenceNotHonored); got != 1 {
		t.Fatalf("fence_not_honored = %v, want 1", got)
	}
	if node := x.node(t, "A-2"); node.Spec.Unschedulable {
		t.Fatal("the fence was not removed")
	}

	fresh := drainable().build()
	y := newFenceFixture(fresh, nil)
	x.f.Client = y.cl
	x.f.Reconcile(context.Background(), fresh, cfg)
	if node := y.node(t, "A-1"); node.Spec.Unschedulable {
		t.Fatal("a new drain opened in a TPU pod under backoff")
	}
}

// TestFencerAbortsDrainsWhenThePolicyStops: recommend-only mode ends every
// open drain.
func TestFencerAbortsDrainsWhenThePolicyStops(t *testing.T) {
	snap := newFenceCluster().partition("A", fenceOn("A", fenceNow.Add(time.Hour))).partition("E").full("C").
		instance("prod/a", "A-0").build()
	x := newFenceFixture(snap, nil)
	x.f.Reconcile(context.Background(), snap, fenceConfig(config.ModeRecommendOnly))
	if node := x.node(t, "A-0"); node.Spec.Unschedulable || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
		t.Fatalf("A-0 kept the fence: %+v", node)
	}
	if got := x.drains(tpudefrag.OutcomeAborted); got != 1 {
		t.Fatalf("aborted = %v, want 1", got)
	}
}

// TestFencerNeverUncordonsAnotherCordon: a node whose fence names another
// drain, or that carries no fence, is left alone when drain d1 ends.
func TestFencerNeverUncordonsAnotherCordon(t *testing.T) {
	snap := newFenceCluster().partition("A", fenceOn("A", fenceNow.Add(-time.Minute))).partition("E").full("C").build()
	x := newFenceFixture(snap, func(n *corev1.Node) {
		if n.Name == "A-1" {
			delete(n.Annotations, snapshot.DefragmentationFenceAnnotation)
		}
	})
	x.f.Reconcile(context.Background(), snap, fenceConfig(config.ModeExecute))
	if node := x.node(t, "A-1"); !node.Spec.Unschedulable {
		t.Fatal("a cordon without this drain's fence was removed")
	}
	if node := x.node(t, "A-0"); node.Spec.Unschedulable {
		t.Fatal("A-0 was not released")
	}
}

// TestFencerNeverRefencesAnEmptiedPartition: releasing emptied partition B
// failed on B-2 alone last pass. This pass releases B-2, leaves B's other
// hosts uncordoned, and keeps A fenced.
func TestFencerNeverRefencesAnEmptiedPartition(t *testing.T) {
	deadline := fenceNow.Add(time.Hour)
	snap := newFenceCluster().partition("A", fenceOn("A", deadline)).
		partition("B", func(i int) []testutil.NodeOption {
			if i == 2 {
				return fenceOn("B", deadline)(i)
			}
			return nil
		}).
		partition("E").full("C").instance("prod/a", "A-0").build()
	x := newFenceFixture(snap, nil)
	x.f.Reconcile(context.Background(), snap, fenceConfig(config.ModeExecute))
	for i := 0; i < 4; i++ {
		if node := x.node(t, fmt.Sprintf("B-%d", i)); node.Spec.Unschedulable || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
			t.Fatalf("B-%d is fenced after B emptied: %+v", i, node)
		}
		if node := x.node(t, fmt.Sprintf("A-%d", i)); !node.Spec.Unschedulable {
			t.Fatalf("A-%d was released while A still holds an Instance", i)
		}
	}
}

// TestFencerNeedsAMigrationAPI: without a configured migration API no move can
// be submitted, so the fencer opens no drain and ends the open one.
func TestFencerNeedsAMigrationAPI(t *testing.T) {
	cfg := fenceConfig(config.ModeExecute)
	snap := drainable().build()
	snap.OMENativeExecutor.Available = false
	x := newFenceFixture(snap, nil)
	x.f.Reconcile(context.Background(), snap, cfg)
	if node := x.node(t, "A-1"); node.Spec.Unschedulable || len(node.Annotations) != 0 {
		t.Fatalf("A-1 was fenced without a migration API: %+v", node)
	}

	open := newFenceCluster().partition("A", fenceOn("A", fenceNow.Add(time.Hour))).partition("E").full("C").
		instance("prod/a", "A-0").build()
	open.OMENativeExecutor.Available = false
	y := newFenceFixture(open, nil)
	y.f.Reconcile(context.Background(), open, cfg)
	if node := y.node(t, "A-1"); node.Spec.Unschedulable || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
		t.Fatalf("A-1 kept the fence without a migration API: %+v", node)
	}
	if got := y.drains(tpudefrag.OutcomeAborted); got != 1 {
		t.Fatalf("aborted = %v, want 1", got)
	}
}

// TestFencerReadsLiveHealthBeforeUncordoning: A-1 turned unhealthy after the
// snapshot. When the drain ends, A-1 stays cordoned and A-0 is uncordoned.
func TestFencerReadsLiveHealthBeforeUncordoning(t *testing.T) {
	cfg := fenceConfig(config.ModeExecute)
	snap := newFenceCluster().partition("A", fenceOn("A", fenceNow.Add(-time.Minute))).partition("E").full("C").build()
	x := newFenceFixture(snap, func(n *corev1.Node) {
		if n.Name == "A-1" {
			n.Status.Conditions = []corev1.NodeCondition{{
				Type:               corev1.NodeConditionType(cfg.Policies.NodeHealth.TriggerConditions[0]),
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(fenceNow.Add(-time.Second)),
			}}
		}
	})
	x.f.Reconcile(context.Background(), snap, cfg)
	if node := x.node(t, "A-1"); !node.Spec.Unschedulable || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
		t.Fatalf("A-1: unschedulable=%v annotations=%v, want cordoned without the fence", node.Spec.Unschedulable, node.Annotations)
	}
	if node := x.node(t, "A-0"); node.Spec.Unschedulable {
		t.Fatal("A-0 was not released")
	}
}

// TestFencerReleasesAFencedNodeThatLostItsPartitionLabels: A-1 lost every
// partition label during the drain. Its fence still ends with the drain.
func TestFencerReleasesAFencedNodeThatLostItsPartitionLabels(t *testing.T) {
	snap := newFenceCluster().partition("A", fenceOn("A", fenceNow.Add(-time.Minute))).partition("E").full("C").build()
	for k := range snap.Nodes["A-1"].Labels {
		if strings.HasPrefix(k, "cloud.google.com/gke-tpu-partition-") {
			delete(snap.Nodes["A-1"].Labels, k)
		}
	}
	x := newFenceFixture(snap, nil)
	x.f.Reconcile(context.Background(), snap, fenceConfig(config.ModeExecute))
	if node := x.node(t, "A-1"); node.Spec.Unschedulable || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
		t.Fatalf("A-1 kept the fence: unschedulable=%v annotations=%v", node.Spec.Unschedulable, node.Annotations)
	}
	if got := x.drains(tpudefrag.OutcomeDeadline); got != 1 {
		t.Fatalf("deadline = %v, want 1", got)
	}
}

// TestFencerKeepsTheEvidenceUntilEveryOtherHostIsReleased: someone uncordoned
// A-1, so the drain ends as fence_not_honored, and releasing A-2 fails once.
// A-1 keeps its fence until every other host is released, so the next pass
// ends the drain again instead of re-fencing the released hosts. The TPU pod
// backs off at once.
func TestFencerKeepsTheEvidenceUntilEveryOtherHostIsReleased(t *testing.T) {
	deadline := fenceNow.Add(time.Hour)
	uncordoned := func(i int) []testutil.NodeOption {
		if i == 1 {
			return []testutil.NodeOption{func(n *snapshot.Node) { n.Cordoned = false }}
		}
		return nil
	}
	snap := newFenceCluster().partition("A", fenceOn("A", deadline), uncordoned).partition("E").full("C").
		instance("prod/a", "A-0").build()
	failA2 := true
	x := newFenceFixtureWith(snap, nil, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if obj.GetName() == "A-2" && failA2 {
				failA2 = false
				return fmt.Errorf("injected conflict")
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	cfg := fenceConfig(config.ModeExecute)
	x.f.Reconcile(context.Background(), snap, cfg)
	fenced := func(name string) bool {
		return x.node(t, name).Annotations[snapshot.DefragmentationFenceAnnotation] != ""
	}
	if fenced("A-0") || fenced("A-3") || !fenced("A-2") || !fenced("A-1") {
		t.Fatalf("after a failed release of A-2: fenced A-0=%v A-1=%v A-2=%v A-3=%v, want only A-1 and A-2",
			fenced("A-0"), fenced("A-1"), fenced("A-2"), fenced("A-3"))
	}
	if !x.f.inBackoff(fenceNow)("pod-a") {
		t.Fatal("the TPU pod is not backed off after the release failed")
	}
	if got := x.drains(tpudefrag.OutcomeFenceNotHonored); got != 0 {
		t.Fatalf("fence_not_honored = %v before the release finished, want 0", got)
	}

	// The next pass reads the two remaining fences back.
	remaining := func(i int) []testutil.NodeOption {
		if i == 1 || i == 2 {
			return fenceOn("A", deadline)(i)
		}
		return nil
	}
	again := newFenceCluster().partition("A", remaining, uncordoned).partition("E").full("C").
		instance("prod/a", "A-0").build()
	x.f.Reconcile(context.Background(), again, cfg)
	for i := 0; i < 4; i++ {
		if node := x.node(t, fmt.Sprintf("A-%d", i)); node.Spec.Unschedulable || node.Annotations[snapshot.DefragmentationFenceAnnotation] != "" {
			t.Fatalf("A-%d: unschedulable=%v annotations=%v, want released and not fenced again", i, node.Spec.Unschedulable, node.Annotations)
		}
	}
	if got := x.drains(tpudefrag.OutcomeFenceNotHonored); got != 1 {
		t.Fatalf("fence_not_honored = %v, want 1", got)
	}
}
