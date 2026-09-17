package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// writeStatus runs aggregateAndWriteStatus for tests that only care about the
// write outcome, discarding the availability wake.
func writeStatus(r *Reconciler, ir *v1beta1.InferenceReplica, plan workload.ComponentPlan, target *appsv1.ControllerRevision, holdObserved bool, hold *workload.RolloutHold) error {
	_, err := r.aggregateAndWriteStatus(context.Background(), ir, plan, target, holdObserved, hold)
	return err
}

func materializeInlinePublicationStatuses(t *testing.T, insts []v1beta1.OMENativeInstanceStatus, byIndex map[int32][]*corev1.Pod, availableByPod map[string]struct{}) []v1beta1.OMENativeInstanceStatus {
	t.Helper()
	observation, err := workload.NewOwnedPublicationObservation(
		v1beta1convert.InstanceStatusSliceToWorkload(insts),
		workload.NewCachedSelectorPodObservation(nil, byIndex),
		availableByPod, workload.AvailabilityWindow{},
	)
	if err != nil {
		t.Fatalf("build publication observation: %v", err)
	}
	statuses, err := observation.TakeInlineV1Statuses()
	if err != nil {
		t.Fatalf("materialize publication status: %v", err)
	}
	return v1beta1convert.InstanceStatusSliceFromWorkload(statuses)
}

// TestPublicationObservation_StampsLivePodState pins the
// per-Instance projection: handed a (status slice, by-index pod
// map, available-pod set) tuple, it stamps each Instance's per-pod
// counters off the live pod state. This is the load-bearing step that
// lets the downstream publication classifier see the
// real pod state instead of a Phase-only proxy.
//
// Regression guard: an IR aggregator that never populates per-Instance
// ReadyPodCount makes downstream rollups read zero on every Instance
// even when the underlying pods are ContainersReady. This test pins
// the exact counter-write contract so a future refactor can't silently
// drop it.
func TestPublicationObservation_StampsLivePodState(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod0.Spec.NodeName = "node-a"
	pod1 := podForIR(ir, 1, "default", 0, false, false)
	insts := []v1beta1.OMENativeInstanceStatus{
		// Pretend the legacy Phase signal lags reality: Instance 0
		// still Phase=Updating from a prior reconcile, but its pod is
		// already ContainersReady + serving. The fix should report
		// ReadyPodCount=1 / ServingPodCount=1 / AvailablePodCount=1
		// off the live pod state regardless of Phase.
		{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, PodCount: 0, ReadyPodCount: 0},
		// Pending Instance with a pod that hasn't flipped Ready —
		// counters should all be zero except PodCount.
		{Index: 1, Phase: v1beta1.OMENativeInstancePending, PodCount: 0, ReadyPodCount: 0},
	}
	byIndex := map[int32][]*corev1.Pod{
		0: {pod0},
		1: {pod1},
	}
	availableByPod := map[string]struct{}{pod0.Name: {}}

	insts = materializeInlinePublicationStatuses(t, insts, byIndex, availableByPod)

	g.Expect(insts[0].PodCount).To(gomega.Equal(int32(1)),
		"Instance 0 has 1 live pod")
	g.Expect(insts[0].ReadyPodCount).To(gomega.Equal(int32(1)),
		"Instance 0's pod is ContainersReady; ReadyPodCount must be 1 even though Phase=Updating")
	g.Expect(insts[0].ServingPodCount).To(gomega.Equal(int32(1)),
		"Instance 0's pod has serving=True; ServingPodCount must be 1")
	g.Expect(insts[0].AvailablePodCount).To(gomega.Equal(int32(1)),
		"Instance 0's pod is in the available EndpointSlice set")
	g.Expect(insts[0].NodesOccupied).To(gomega.Equal([]string{"node-a"}),
		"NodesOccupied must reflect the scheduling node")

	g.Expect(insts[1].PodCount).To(gomega.Equal(int32(1)),
		"Instance 1 has 1 live pod (not yet Ready)")
	g.Expect(insts[1].ReadyPodCount).To(gomega.Equal(int32(0)),
		"Instance 1's pod is not ContainersReady")
	g.Expect(insts[1].AvailablePodCount).To(gomega.Equal(int32(0)),
		"Instance 1's pod is not in the available set")
}

// TestPublicationObservation_EmptyPodSet pins the no-pods edge
// case: an Instance with zero matching pods (e.g., post-drain pre-
// recreate, scale-down victim mid-delete) must end up with all-zero
// counters. Catches a future bug where the loop accidentally retains
// stale counter values from a prior reconcile.
func TestPublicationObservation_EmptyPodSet(t *testing.T) {
	g := gomega.NewWithT(t)
	insts := []v1beta1.OMENativeInstanceStatus{
		// Stale stamps from a prior reconcile when this Instance had pods.
		{Index: 0, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, AvailablePodCount: 1,
			NodesOccupied: []string{"node-x"}},
	}
	insts = materializeInlinePublicationStatuses(t, insts, map[int32][]*corev1.Pod{}, map[string]struct{}{})

	g.Expect(insts[0].PodCount).To(gomega.BeZero())
	g.Expect(insts[0].ReadyPodCount).To(gomega.BeZero())
	g.Expect(insts[0].ServingPodCount).To(gomega.BeZero())
	g.Expect(insts[0].AvailablePodCount).To(gomega.BeZero())
	g.Expect(insts[0].NodesOccupied).To(gomega.BeNil(),
		"NodesOccupied must round-trip nil ↔ nil rather than nil ↔ empty-slice")
}

// TestPublicationObservation_Admitted verifies the Admitted signal
// is computed correctly: true when the Instance has pods and none carry
// an admission scheduling gate (Kueue has granted quota), false while
// gated/queued or before pods exist. Used by the multi-cluster control
// plane to decide the fan-out race winner.
func TestPublicationObservation_Admitted(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 3)

	// Index 0 → one normal (ungated) running pod
	pod0 := podForIR(ir, 0, "default", 0, true, true)

	// Index 1 → one pod with Kueue admission gate still present (queued)
	pod1 := podForIR(ir, 1, "default", 0, false, false)
	pod1.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}}

	// Index 2 → no pods (empty instance)

	insts := []v1beta1.OMENativeInstanceStatus{
		{Index: 0},
		{Index: 1},
		{Index: 2},
	}
	byIndex := map[int32][]*corev1.Pod{
		0: {pod0},
		1: {pod1},
		// 2: no entry (zero pods)
	}

	insts = materializeInlinePublicationStatuses(t, insts, byIndex, nil)

	g.Expect(insts[0].Admitted).To(gomega.BeTrue(),
		"Instance 0 has a pod and it is not admission-gated → Admitted=true")
	g.Expect(insts[1].Admitted).To(gomega.BeFalse(),
		"Instance 1's pod still carries the Kueue admission gate → Admitted=false")
	g.Expect(insts[2].Admitted).To(gomega.BeFalse(),
		"Instance 2 has no pods yet → Admitted=false")
}

func TestAggregateAndWriteStatusPersistsCompactRowsAndMirrorsCurrentObservations(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("model-engine", "default", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
		ReadyPodCount: 9, ScheduledPodCount: 9, NodesOccupied: []string{"legacy-node"},
	}}
	pod := podForIR(ir, 0, "default", 0, true, true)
	pod.Spec.NodeName = "node-a"
	r, stored := newReconciler(t, ir, pod)
	statusWrites := 0
	r.Client = interceptor.NewClient(stored.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			assertCompactInferenceReplicaWrite(t, obj)
			statusWrites++
			return c.SubResource(subresource).Update(ctx, obj, opts...)
		},
	})
	r.APIReader = r.Client
	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
	}

	g.Expect(writeStatus(r, ir, plan, nil, false, nil)).To(gomega.Succeed())
	got := &v1beta1.InferenceReplica{}
	g.Expect(stored.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].Admitted).To(gomega.BeTrue())
	g.Expect(got.Status.InstanceStatuses[0].PodCount).To(gomega.Equal(int32(1)))
	g.Expect(got.Status.InstanceStatuses[0].ServingPodCount).To(gomega.Equal(int32(1)))
	g.Expect(got.Status.InstanceStatuses[0].ReadyPodCount).To(gomega.BeZero())
	g.Expect(got.Status.InstanceStatuses[0].ScheduledPodCount).To(gomega.BeZero())
	g.Expect(got.Status.InstanceStatuses[0].NodesOccupied).To(gomega.BeNil())
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(1)),
		"component readiness is computed from the current Pod observation")
	g.Expect(ir.Status.InstanceStatuses[0].Admitted).To(gomega.BeFalse(),
		"the caller snapshot retains its reconcile-entry admission value")
	g.Expect(ir.Status.InstanceStatuses[0].ReadyPodCount).To(gomega.Equal(int32(1)))
	g.Expect(ir.Status.InstanceStatuses[0].ScheduledPodCount).To(gomega.Equal(int32(1)))
	g.Expect(ir.Status.InstanceStatuses[0].NodesOccupied).To(gomega.Equal([]string{"node-a"}),
		"same-pass observations remain available without being persisted")

	rvAfterCompaction := got.ResourceVersion
	g.Expect(writeStatus(r, got.DeepCopy(), plan, nil, false, nil)).To(gomega.Succeed())
	steady := &v1beta1.InferenceReplica{}
	g.Expect(stored.Get(context.Background(), client.ObjectKeyFromObject(ir), steady)).To(gomega.Succeed())
	g.Expect(steady.ResourceVersion).To(gomega.Equal(rvAfterCompaction),
		"a steady compact publication must perform zero status writes")
	g.Expect(statusWrites).To(gomega.Equal(1),
		"legacy compaction writes once and the steady compact pass writes nothing")
}

func TestMirrorInstanceCountersUsesTransientPublicationIntersection(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
		{Index: 3, Phase: v1beta1.OMENativeInstanceDeleting, Admitted: true, PodCount: 9, NodesOccupied: []string{"old-node"}},
		{Index: 9, Phase: v1beta1.OMENativeInstanceReady, PodCount: 7, ReadyPodCount: 6},
		{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating},
	}}
	publication := []workload.InstanceStatus{
		{Index: 1, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ScheduledPodCount: 1, AvailablePodCount: 1, NodesOccupied: []string{"node-a"}},
		{Index: 3, PodCount: 2, ReadyPodCount: 2, NodesOccupied: []string{"superseded"}},
		{Index: 3, PodCount: 4, ReadyPodCount: 3, ServingPodCount: 2, ScheduledPodCount: 4, AvailablePodCount: 1},
		{Index: 7, PodCount: 1, ReadyPodCount: 1},
	}

	mirrorInstanceCounters(status, publication)

	g.Expect(status.InstanceStatuses[0]).To(gomega.Equal(v1beta1.OMENativeInstanceStatus{
		Index: 3, Phase: v1beta1.OMENativeInstanceDeleting, Admitted: true,
		PodCount: 4, ReadyPodCount: 3, ServingPodCount: 2, ScheduledPodCount: 4, AvailablePodCount: 1,
	}), "the last duplicate publication row wins without changing lifecycle or admission")
	g.Expect(status.InstanceStatuses[1]).To(gomega.Equal(v1beta1.OMENativeInstanceStatus{
		Index: 9, Phase: v1beta1.OMENativeInstanceReady, PodCount: 7, ReadyPodCount: 6,
	}), "caller-only rows remain untouched")
	g.Expect(status.InstanceStatuses[2].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceUpdating))
	g.Expect(status.InstanceStatuses[2].NodesOccupied).To(gomega.Equal([]string{"node-a"}))

	publication[0].NodesOccupied[0] = "mutated"
	g.Expect(status.InstanceStatuses[2].NodesOccupied).To(gomega.Equal([]string{"node-a"}),
		"mirrored node slices must not alias the transient publication")
	mirrorInstanceCounters(nil, publication)
}

// TestAggregateStatus_AvailableReplicas_MatchesReadyWithEndpointSlices
// pins the AvailableReplicas counter via the end-to-end Reconcile path.
// With pods ContainersReady AND published as Ready in the per-Component
// headless Service's EndpointSlice, AvailableReplicas should collapse
// onto ReadyReplicas. The aggregator post-fix reads availability off
// the slice exactly like the omenative direct path
// (TestAggregateAndWriteStatus_PerInstanceCountersFromObservedPods),
// so the two adapters produce byte-identical counters.
//
// Replaces the legacy assertion that AvailableReplicas always mirrors
// ReadyReplicas. The two CAN diverge — see
// TestAggregateStatus_AvailableReplicas_ZeroWhenSliceNotReady — so the
// surface tested here is "with EndpointSlices published Ready, the two
// match".
func TestAggregateStatus_AvailableReplicas_MatchesReadyWithEndpointSlices(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 3)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		{Index: 2, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			PodCount: 1, ReadyPodCount: 0, ServingPodCount: 0, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod1 := podForIR(ir, 1, "default", 0, true, true)
	pod2 := podForIR(ir, 2, "default", 0, false, false)
	slice0 := sliceForIRPod(ir, pod0, true)
	slice1 := sliceForIRPod(ir, pod1, true)
	r, c := newReconciler(t, ir, pod0, pod1, pod2, slice0, slice1)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(2)),
		"ReadyReplicas should be 2 (two pods ContainersReady)")
	g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(2)),
		"AvailableReplicas should match ReadyReplicas when EndpointSlices publish both pods Ready")
}

// TestReconcile_WakesWhenMinReadyWindowElapses pins the requeue that keeps
// the windowed Available counters current: a pod in rotation and Ready but
// still inside spec.minReadySeconds crosses the window with no watch event,
// so the reconcile result must wake exactly when the earliest such pod does.
// A pod past the window, or a zero window, schedules nothing.
func TestReconcile_WakesWhenMinReadyWindowElapses(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	reconcile := func(t *testing.T, minReadySeconds int32, readyAt time.Time) (ctrl.Result, *v1beta1.InferenceReplica) {
		t.Helper()
		g := gomega.NewWithT(t)
		ir := baselineIR("llama-engine", "prod", 1)
		ir.Spec.MinReadySeconds = minReadySeconds
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
				PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		}
		pod0 := podForIR(ir, 0, "default", 0, true, true)
		pod0.Status.Conditions = append(pod0.Status.Conditions, corev1.PodCondition{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(readyAt),
		})
		slice0 := sliceForIRPod(ir, pod0, true)
		r, c := newReconciler(t, ir, pod0, slice0)
		r.Clock = clocktesting.NewFakeClock(now)

		result, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		got := &v1beta1.InferenceReplica{}
		g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
		return result, got
	}

	t.Run("inside the window wakes at the remaining window", func(t *testing.T) {
		g := gomega.NewWithT(t)
		result, got := reconcile(t, 20, now.Add(-5*time.Second))
		g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(1)))
		g.Expect(got.Status.AvailableReplicas).To(gomega.BeZero(),
			"a pod inside the window is Ready but not yet Available")
		g.Expect(result.RequeueAfter).To(gomega.Equal(15*time.Second),
			"the reconcile must wake exactly when the pod crosses the window")
	})

	t.Run("past the window schedules nothing", func(t *testing.T) {
		g := gomega.NewWithT(t)
		result, got := reconcile(t, 20, now.Add(-30*time.Second))
		g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(1)))
		g.Expect(result.RequeueAfter).To(gomega.BeZero())
	})

	t.Run("zero window schedules nothing", func(t *testing.T) {
		g := gomega.NewWithT(t)
		result, got := reconcile(t, 0, now.Add(-5*time.Second))
		g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(1)),
			"a zero window makes Available the same as in rotation")
		g.Expect(result.RequeueAfter).To(gomega.BeZero())
	})
}

// TestAggregateStatus_AvailableReplicas_ZeroWhenSliceNotReady pins the
// EndpointSlice-gated availability semantic: a pod ContainersReady but
// NOT yet in any EndpointSlice's Ready endpoints reads
// AvailablePodCount=0 → AvailableReplicas=0, while ReadyReplicas still
// reports 1. Mirrors the omenative direct path's
// TestAggregateAndWriteStatus_AvailableRequiresEndpointSliceReady.
//
// This is the "newly-Ready pod hasn't been picked up by kube-proxy
// yet" window — operators get a useful signal that the pod is alive
// but not in rotation, instead of the previous always-mirrors-ready
// behavior that hid the kube-proxy lag.
func TestAggregateStatus_AvailableReplicas_ZeroWhenSliceNotReady(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	// EndpointSlice publishes the endpoint as Ready=false (kube-proxy
	// hasn't picked it up yet).
	slice0 := sliceForIRPod(ir, pod0, false)
	r, c := newReconciler(t, ir, pod0, slice0)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(1)),
		"ReadyReplicas counts the ContainersReady pod regardless of EndpointSlice state")
	g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(0)),
		"AvailableReplicas waits for EndpointSlice Ready=true (kube-proxy gate)")
}

// TestAggregateStatus_ReadyCondition_TrueWhenAllReady_RolloutDone pins
// the happy-path Ready=True branch: ReadyReplicas == Replicas AND
// CurrentRevision == UpdateRevision → Ready=True/AllInstancesReady.
//
// Tests computeReadyCondition directly because the full-Reconcile path
// is sensitive to the exact ControllerRevision-name shape the workload
// pipeline computes. The helper is the load-bearing logic; the
// integration test TestAggregateStatus_AvailableReplicas_MatchesReadyForSimpleCase
// covers the Reconcile-side wiring.
func TestAggregateStatus_ReadyCondition_TrueWhenAllReady_RolloutDone(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:        1,
		ReadyReplicas:   1,
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		},
	}
	cond := computeReadyCondition(status, nil, nil, nil)
	g.Expect(cond.Type).To(gomega.Equal(InferenceReplicaConditionReady))
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue),
		"Ready=True when ReadyReplicas == Replicas AND CurrentRevision == UpdateRevision")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonAllInstancesReady))
}

// TestAggregateStatus_ReadyCondition_UnknownDuringRollout pins the
// in-flight-rollout branch: CurrentRevision != UpdateRevision →
// Ready=Unknown/RolloutInProgress.
//
// The explicit Unknown status (not False) is load-bearing: operators
// running `kubectl wait --for=condition=Ready` see "still churning,
// check back" instead of a False that would short-circuit their wait.
func TestAggregateStatus_ReadyCondition_UnknownDuringRollout(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:             2,
		ReadyReplicas:        1,
		UpdatedReadyReplicas: 1,
		CurrentRevision:      "rev-a",
		UpdateRevision:       "rev-b",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-b"},
			{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: "rev-a",
				TargetRevision: "rev-b"},
		},
	}
	cond := computeReadyCondition(status, nil, nil, nil)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionUnknown),
		"Ready=Unknown while CurrentRevision != UpdateRevision (rollout in flight)")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonRolloutInProgress))
}

// TestAggregateStatus_ReadyCondition_StagedAtPartition pins the
// staged branch: a static rollingUpdate.partition holds Instances on the
// prior revision by design (CurrentRevision != UpdateRevision forever), so
// Ready must be True/Staged — NOT Unknown/RolloutInProgress.
func TestAggregateStatus_ReadyCondition_StagedAtPartition(t *testing.T) {
	g := gomega.NewWithT(t)
	part := int32(1)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:             3,
		ReadyReplicas:        3,
		UpdatedReadyReplicas: 2,
		CurrentRevision:      "rev-a",
		UpdateRevision:       "rev-b",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-b"},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-b"},
			{Index: 2, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-a"}, // held by partition
		},
	}
	cond := computeReadyCondition(status, nil, nil, &v1beta1.InferenceReplicaPacing{Partition: &part})
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue),
		"Ready=True when converged to a static partition (2 on target + 1 held, all Ready)")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonStaged))

	// Not yet converged (held instance still updating) → back to RolloutInProgress.
	status.InstanceStatuses[2].Phase = v1beta1.OMENativeInstanceUpdating
	status.ReadyReplicas = 2
	cond = computeReadyCondition(status, nil, nil, &v1beta1.InferenceReplicaPacing{Partition: &part})
	g.Expect(cond.Reason).To(gomega.Equal(ReasonRolloutInProgress),
		"not staged until the partitioned shape is fully reached")
}

// TestAggregateStatus_ReadyCondition_FalseOnStuck pins the
// stuck-replica-count branch: ReadyReplicas < Replicas with
// CurrentRevision == UpdateRevision (no rollout in flight) →
// Ready=False/ReplicaCountMismatch.
//
// Operationally this catches: a steady-state Component lost an Instance
// (pod evicted, node drained, etc.) but the workload pipeline hasn't
// either recovered or escalated yet. The False signal pages operators
// instead of letting kubectl-wait hang on a missing condition.
func TestAggregateStatus_ReadyCondition_FalseOnStuck(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:        2,
		ReadyReplicas:   1,
		ServingReplicas: 1,
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-a"},
			// Pending (not Failed) so the InstanceFailed override doesn't
			// fire; we want the ReplicaCountMismatch branch.
			{Index: 1, Phase: v1beta1.OMENativeInstancePending, RunningRevision: "rev-a"},
		},
	}
	// nil lifecycle keeps the strict floor, so serving 1/2 is below it.
	cond := computeReadyCondition(status, nil, nil, nil)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse),
		"Ready=False when serving is below the availability floor with no rollout in flight")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonReplicaCountMismatch))
}

// TestAggregateStatus_ReadyCondition_TrueWhenServingWithinBudget verifies that
// lifecycle rollingUpdate.maxUnavailable supplies the Instance readiness floor.
func TestAggregateStatus_ReadyCondition_TrueWhenServingWithinBudget(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:        10,
		ReadyReplicas:   9,
		ServingReplicas: 9,
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
	}
	mu := intstr.FromString("25%") // floor = 10 - ceil(2.5) = 7
	lifecycle := &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &mu},
		},
	}
	cond := computeReadyCondition(status, nil, lifecycle, nil)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue),
		"Ready=True when serving (9) >= floor (7) even though ReadyReplicas < Replicas")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonMinimumAvailable))
}

func TestAggregateStatus_ReadyCondition_SurgeUsesDesiredReplicaFloor(t *testing.T) {
	g := gomega.NewWithT(t)
	desired := int32(16)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:        32,
		ReadyReplicas:   16,
		ServingReplicas: 16,
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
	}
	mu := intstr.FromString("25%")
	lifecycle := &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &mu},
		},
	}

	cond := computeReadyCondition(status, &desired, lifecycle, nil)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue))
	g.Expect(cond.Reason).To(gomega.Equal(ReasonMinimumAvailable))
	g.Expect(cond.Message).To(gomega.Equal("16/32 Instances serving (min 12)"))
}

func TestAggregateStatus_ReadyCondition_PacingBudgetDoesNotRelaxReadiness(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:        10,
		ReadyReplicas:   9,
		ServingReplicas: 9,
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
	}
	mu := intstr.FromString("100%")
	cond := computeReadyCondition(status, nil, nil, &v1beta1.InferenceReplicaPacing{MaxUnavailable: &mu})
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(cond.Reason).To(gomega.Equal(ReasonReplicaCountMismatch))
}

// TestAggregateStatus_ReadyCondition_FalseOnFailed pins the
// InstanceFailed override: any Instance Phase=Failed → Ready=False/
// InstanceFailed, regardless of revision state.
//
// This is the highest-priority branch in computeReadyCondition's
// precedence chain — a failed Instance overrides RolloutInProgress
// (rollout is permanently wedged), AllInstancesReady (one bad apple),
// and ReplicaCountMismatch (Failed is the more specific reason).
func TestAggregateStatus_ReadyCondition_FalseOnFailed(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:      2,
		ReadyReplicas: 1,
		// CurrentRevision != UpdateRevision would normally flip to
		// Unknown; the Failed override must win regardless.
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-b",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
			{Index: 1, Phase: v1beta1.OMENativeInstanceFailed},
		},
	}
	cond := computeReadyCondition(status, nil, nil, nil)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse),
		"Ready=False when any Instance has Phase=Failed, overriding rollout-in-flight")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonInstanceFailed))
}

// TestAggregateStatus_ReadyCondition_ObservedGenerationStamped pins
// that the Ready condition carries ObservedGeneration off the
// IR.Status.ObservedGeneration field so consumers can correlate
// condition transitions to a specific spec generation. Matches the
// pattern apimeta.SetStatusCondition + the standard K8s convention.
func TestAggregateStatus_ReadyCondition_ObservedGenerationStamped(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		ObservedGeneration: 7,
		Replicas:           1,
		ReadyReplicas:      1,
		CurrentRevision:    "rev-a",
		UpdateRevision:     "rev-a",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		},
	}
	cond := computeReadyCondition(status, nil, nil, nil)
	g.Expect(cond.ObservedGeneration).To(gomega.Equal(int64(7)),
		"Ready condition must stamp ObservedGeneration off status.ObservedGeneration")
}

// TestAggregateStatus_RecreatePath_ReadyReplicasReflectsLivePods:
// during a recreate or SurgeThenDrain rollout the per-Instance
// lifecycle Phase can lag the live pod state (Phase=Updating while the
// new pod has flipped ContainersReady but the workload pipeline hasn't
// yet stamped Phase=Ready). The IR rollup MUST count the Instance as
// Ready off the live pod state rather than the lifecycle bookkeeping —
// otherwise ISVC's engine.omenative.readyReplicas reads 0 even though
// the pod is already serving.
//
// Setup mirrors the post-recreate moment: Instance 0 carries
// Phase=Updating with a deep operation snapshot (drain step
// committed), but its replacement pod is live and ContainersReady.
// After Reconcile, IR.Status.ReadyReplicas must be 1.
func TestAggregateStatus_RecreatePath_ReadyReplicasReflectsLivePods(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	// Pretend a recreate is mid-flight: the controller has stamped
	// Phase=Updating with an Update operation but the new pod just
	// flipped ContainersReady. The aggregator must observe the live
	// pod state and count the Instance Ready regardless of Phase.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     2,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "rev-a",
			TargetRevision:  "rev-b",
		},
	}
	// Live pod is ContainersReady + serving (post-recreate, pre-Phase
	// bump). The aggregator computes readiness directly from this pod.
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	// EndpointSlice published — kube-proxy has picked the pod up so
	// AvailableReplicas can also reach 1.
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(1)),
		"ReadyReplicas must reflect live pod state during recreate (Phase=Updating but pod ContainersReady)")
	g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(1)),
		"AvailableReplicas must match ReadyReplicas when EndpointSlice publishes the pod Ready")
	g.Expect(got.Status.InstanceStatuses[0].ReadyPodCount).To(gomega.BeZero(),
		"per-Instance readiness is derived rather than persisted")
}

// TestAggregateStatus_RecreatePath_OldPodCountsReadyDuringDrain pins
// the operationally-critical surge-tolerant counter behavior: during
// the drain phase of a recreate, the OLD pod is still serving (the
// drain hasn't yet completed). The IR rollup MUST count it as Ready
// throughout drain so SurgeThenDrain / RecreatePod observers see a
// continuous ServingReplicas signal, not a drop-to-zero glitch that
// would page operators or trip the RatioBalanced gate.
//
// Setup: Instance Phase=Updating, drain step in progress. The
// canonical pod still carries ContainersReady=True. The aggregator
// must observe the pod state and count it Ready off
// surge-tolerant readiness classifier.
func TestAggregateStatus_RecreatePath_OldPodCountsReadyDuringDrain(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "rev-a",
			TargetRevision:  "rev-b",
			// The drain marker is in flight; the pod is still alive
			// and ContainersReady but the workload pipeline has set
			// Phase=Updating.
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate,
				Step: "Drain",
			},
		},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	// Critical: ReadyReplicas stays 1 throughout drain even though
	// Phase=Updating. Without the fix this read as 0, breaking
	// SurgeThenDrain's "serving >= 1" continuity promise.
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(1)),
		"the still-serving canonical pod must count Ready during drain (Phase=Updating)")
}

// TestAggregateStatus_ReadyCondition_PersistedToInformerCache pins the
// end-to-end wiring: a reconciled IR with a real status update lands
// the Ready condition on the apiserver-side IR object, observable via
// apimeta.FindStatusCondition. Catches a future regression where the
// computeReadyCondition helper is correct but the aggregator forgets
// to call SetStatusCondition (or writes to the wrong slice).
//
// Uses the same setup as TestReconcile_StatusAggregator_RollsUpCounters:
// real pods backing the pre-seeded Instances so the workload pipeline
// doesn't reset state. ReadyReplicas=2 and empty CurrentRevision (first
// reconcile) means the True branch can't fire — we just assert the
// condition is present and well-formed.
func TestAggregateStatus_ReadyCondition_PersistedToInformerCache(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod1 := podForIR(ir, 1, "default", 0, true, true)
	r, c := newReconciler(t, ir, pod0, pod1)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	cond := apimeta.FindStatusCondition(got.Status.Conditions, InferenceReplicaConditionReady)
	g.Expect(cond).NotTo(gomega.BeNil(),
		"Ready condition must be persisted to IR.Status.Conditions after every reconcile pass")
	g.Expect(cond.Type).To(gomega.Equal(InferenceReplicaConditionReady))
	// LastTransitionTime is stamped by apimeta.SetStatusCondition when
	// the condition is first inserted.
	g.Expect(cond.LastTransitionTime.IsZero()).To(gomega.BeFalse(),
		"LastTransitionTime must be set by SetStatusCondition")
}

// TestAggregateAndWriteStatus_NoWriteWhenUnchanged pins the no-op
// short-circuit: once an IR's status has converged, a second
// aggregateAndWriteStatus pass that recomputes the identical status must
// perform ZERO Status().Update writes. Each IR status write re-triggers
// this controller AND the irprojector rollup onto the parent ISVC, so at
// scale the prior unconditional write amplified idle reconciles into
// a write storm.
//
// Verified via ResourceVersion: the fake client bumps RV on every
// Status().Update. The first pass changes status (RV bumps); the second
// pass over an already-converged IR must leave RV untouched. The knative
// SetCondition / apimeta.SetStatusCondition LastTransitionTime-preserving
// semantics are what make the second-pass status DeepEqual the first.
func TestAggregateAndWriteStatus_NoWriteWhenUnchanged(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	// First pass: status changes (per-Instance counters, conditions,
	// LabelSelector, ObservedGeneration all get stamped) → a write lands.
	g.Expect(writeStatus(r, ir, plan, nil, false, nil)).To(gomega.Succeed())
	afterFirst := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, afterFirst)).To(gomega.Succeed())
	rvAfterFirst := afterFirst.ResourceVersion

	// Second pass over the now-converged IR: recomputed status equals
	// live status → the DeepEqual guard must skip Status().Update, so the
	// ResourceVersion is unchanged.
	g.Expect(writeStatus(r, afterFirst.DeepCopy(), plan, nil, false, nil)).To(gomega.Succeed())
	afterSecond := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, afterSecond)).To(gomega.Succeed())
	g.Expect(afterSecond.ResourceVersion).To(gomega.Equal(rvAfterFirst),
		"a no-op aggregateAndWriteStatus pass must perform ZERO writes (ResourceVersion unchanged)")
}

func TestAggregateAndWriteStatusReusesPublicationReadsAcrossConflictRetry(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("model-engine", "default", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
		ReadyPodCount: 3, ScheduledPodCount: 3, NodesOccupied: []string{"legacy-node"},
	}}
	pod := podForIR(ir, 0, "default", 0, true, true)
	slice := sliceForIRPod(ir, pod, true)
	podLists := 0
	endpointSliceLists := 0
	statusWrites := 0
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *corev1.PodList:
				podLists++
			case *discoveryv1.EndpointSliceList:
				endpointSliceLists++
			}
			return c.List(ctx, list, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			assertCompactInferenceReplicaWrite(t, obj)
			statusWrites++
			if statusWrites == 1 {
				latest := &v1beta1.InferenceReplica{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), latest); err != nil {
					return err
				}
				latest.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceDeleting
				latest.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
					ID: "delete-0", Type: v1beta1.InstanceOperationDelete, Step: "Drain",
				}
				latest.Status.InstanceStatuses[0].RunningRevision = "rev-target"
				latest.Status.InstanceStatuses[0].ReadyPodCount = 5
				latest.Status.InstanceStatuses[0].ScheduledPodCount = 5
				latest.Status.InstanceStatuses[0].NodesOccupied = []string{"concurrent-node"}
				latest.Status.InstanceStatuses = append(latest.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
					Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-target",
					ReadyPodCount: 2, ScheduledPodCount: 2, NodesOccupied: []string{"concurrent-node"},
				})
				if err := c.SubResource(subresource).Update(ctx, latest); err != nil {
					return err
				}
				return apierrors.NewConflict(
					schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
					obj.GetName(), errors.New("conflict"))
			}
			return c.SubResource(subresource).Update(ctx, obj, opts...)
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir, pod, slice).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor).
		WithInterceptorFuncs(funcs).
		Build()
	r := &Reconciler{Client: c, APIReader: c, Log: logf.Log.WithName("test"), Expectations: workload.NewExpectations()}
	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
	}

	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev-target"}}
	g.Expect(writeStatus(r, ir, plan, target, false, nil)).To(gomega.Succeed())
	g.Expect(statusWrites).To(gomega.Equal(2))
	g.Expect(podLists).To(gomega.Equal(1), "the publication Pod observation is captured outside conflict retry")
	g.Expect(endpointSliceLists).To(gomega.Equal(1), "the publication availability observation is captured outside conflict retry")
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceDeleting))
	g.Expect(got.Status.InstanceStatuses[0].Operation).NotTo(gomega.BeNil())
	g.Expect(got.Status.InstanceStatuses[0].Operation.ID).To(gomega.Equal("delete-0"))
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(2))
	for _, status := range got.Status.InstanceStatuses {
		g.Expect(status.ReadyPodCount).To(gomega.BeZero())
		g.Expect(status.ScheduledPodCount).To(gomega.BeZero())
		g.Expect(status.NodesOccupied).To(gomega.BeNil())
	}
	g.Expect(got.Status.Replicas).To(gomega.Equal(int32(2)))
	g.Expect(got.Status.UpdatedReplicas).To(gomega.Equal(int32(2)))
	g.Expect(got.Status.UpdatedReadyReplicas).To(gomega.Equal(int32(1)))
}

func TestAggregateAndWriteStatus_RebasesAfterAdjacentLifecycleWrite(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
	}}
	stale := ir.DeepCopy()
	apiClient := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	key := client.ObjectKeyFromObject(ir)
	live := &v1beta1.InferenceReplica{}
	g.Expect(apiClient.Get(context.Background(), key, live)).To(gomega.Succeed())
	live.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceDeleting
	live.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		ID: "delete-0", Type: v1beta1.InstanceOperationDelete, Step: "Drain",
	}
	g.Expect(apiClient.Status().Update(context.Background(), live)).To(gomega.Succeed())

	staleCache := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(stale).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	r := &Reconciler{
		Client:       &staleReadingClient{Client: apiClient, reader: staleCache},
		APIReader:    apiClient,
		Log:          logf.Log.WithName("test"),
		Expectations: workload.NewExpectations(),
	}
	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
	}

	g.Expect(writeStatus(r, stale, plan, nil, false, nil)).To(gomega.Succeed())
	got := &v1beta1.InferenceReplica{}
	g.Expect(apiClient.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1))
	g.Expect(got.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceDeleting))
	g.Expect(got.Status.InstanceStatuses[0].Operation).NotTo(gomega.BeNil())
	g.Expect(got.Status.InstanceStatuses[0].Operation.ID).To(gomega.Equal("delete-0"),
		"aggregate status must preserve the adjacent lifecycle commit")
}

func TestAggregateAndWriteStatus_SameNameReplacementIsUntouched(t *testing.T) {
	g := gomega.NewWithT(t)
	stale := baselineIR("llama-engine", "prod", 1)
	replacement := stale.DeepCopy()
	replacement.UID = types.UID("replacement-uid")
	replacement.Status.Replicas = 7
	replacement.Status.ReadyReplicas = 6
	replacement.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 9, Phase: v1beta1.OMENativeInstanceDeleting,
	}}
	wantStatus := replacement.Status.DeepCopy()
	live, writes := newCountingStatusClient(t, 0, replacement)
	r := &Reconciler{
		Client:       live,
		APIReader:    live,
		Log:          logf.Log.WithName("test"),
		Expectations: workload.NewExpectations(),
	}
	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(stale.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
	}

	g.Expect(writeStatus(r, stale, plan, nil, false, nil)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(0))
	got := &v1beta1.InferenceReplica{}
	g.Expect(live.Get(context.Background(), client.ObjectKeyFromObject(replacement), got)).To(gomega.Succeed())
	g.Expect(got.UID).To(gomega.Equal(replacement.UID))
	g.Expect(got.Status).To(gomega.Equal(*wantStatus))
}

func TestAggregateAndWriteStatus_GenerationChangeAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	stale := baselineIR("llama-engine", "prod", 1)
	liveObject := stale.DeepCopy()
	liveObject.Generation++
	liveObject.Status.Replicas = 7
	liveObject.Status.ReadyReplicas = 6
	wantStatus := liveObject.Status.DeepCopy()
	live, writes := newCountingStatusClient(t, 0, liveObject)
	r := &Reconciler{
		Client:       live,
		APIReader:    live,
		Log:          logf.Log.WithName("test"),
		Expectations: workload.NewExpectations(),
	}
	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(stale.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
	}
	metricBefore := make(map[string]float64)
	for _, result := range []string{obsmetrics.ResultSuccess, obsmetrics.ResultConflict, obsmetrics.ResultNotFound, obsmetrics.ResultError} {
		metricBefore[result] = irStatusUpdateMetric(t, result)
	}

	err := writeStatus(r, stale, plan, nil, false, nil)
	g.Expect(errors.Is(err, workload.ErrStatusMutationPrecondition)).To(gomega.BeTrue())
	g.Expect(*writes).To(gomega.Equal(0))
	for result, before := range metricBefore {
		g.Expect(irStatusUpdateMetric(t, result)).To(gomega.Equal(before),
			"a generation precondition must not report a terminal status-write outcome")
	}
	got := &v1beta1.InferenceReplica{}
	g.Expect(live.Get(context.Background(), client.ObjectKeyFromObject(liveObject), got)).To(gomega.Succeed())
	g.Expect(got.Generation).To(gomega.Equal(liveObject.Generation))
	g.Expect(got.Status).To(gomega.Equal(*wantStatus))
}

// liveReadsThroughPromotion is how many InferenceReplica reads reach the
// authoritative reader up to and including the CurrentRevision promotion:
// the migration-entry sync, the aggregate-condition write, then promotion.
// Every conflict-retry closure re-reads its base live, so promotion is not
// the first live read.
const liveReadsThroughPromotion = 3

type firstIRGenerationReader struct {
	client.Reader
	generation int64
	// staleUntil is how many leading InferenceReplica reads report the
	// pre-change generation. The window has to cover everything up to and
	// including promotion so the generation change lands between promotion
	// and aggregation.
	staleUntil int
	reads      int
}

func (r *firstIRGenerationReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return nil
	}
	r.reads++
	if r.reads <= r.staleUntil {
		ir.Generation = r.generation
	}
	return nil
}

func TestReconcile_GenerationChangeRequeuesBeforeDeferredEffects(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		changeBeforeAggregation bool
	}{
		{name: "promotion observes the new generation"},
		{name: "aggregation observes the new generation", changeBeforeAggregation: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			stale := baselineIR("llama-engine", "prod", 1)
			stale.Finalizers = []string{TeardownFinalizer}
			stale.Spec.Paused = true
			// Retention is configured (per-IR spec limit) so the sweep
			// WOULD trim the fence if the deferred pass wrongly ran it.
			stale.Spec.RevisionHistoryLimit = ptr.To(int32(testRevisionRetention))
			liveObject := stale.DeepCopy()
			liveObject.Generation++

			objects := []client.Object{liveObject}
			oldestRevision := ""
			for i := 0; i < testRevisionRetention+2; i++ {
				revision := seedControllerRevision(liveObject, fmt.Sprintf("fence%02d", i), int64(i+1))
				if i == 0 {
					oldestRevision = revision.Name
				}
				objects = append(objects, revision)
			}
			r, live := newReconciler(t, objects...)
			staleCache := fake.NewClientBuilder().
				WithScheme(testScheme(t)).
				WithObjects(stale).
				Build()
			r.Client = &staleReadingClient{Client: live, reader: staleCache}
			var generationReader *firstIRGenerationReader
			if tc.changeBeforeAggregation {
				generationReader = &firstIRGenerationReader{Reader: live, generation: stale.Generation, staleUntil: liveReadsThroughPromotion}
				r.APIReader = generationReader
			} else {
				r.APIReader = live
			}

			result, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(stale),
			})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(result.Requeue).To(gomega.BeTrue())
			g.Expect(result.RequeueAfter).To(gomega.BeZero())
			if generationReader != nil {
				g.Expect(generationReader.reads).To(gomega.Equal(liveReadsThroughPromotion+1),
					"promotion must accept the starting generation before aggregation observes the change")
			}

			got := &v1beta1.InferenceReplica{}
			g.Expect(live.Get(context.Background(), client.ObjectKeyFromObject(liveObject), got)).To(gomega.Succeed())
			g.Expect(got.Status.ObservedGeneration).NotTo(gomega.Equal(got.Generation))
			survivors := listRevisionNames(t, live, liveObject.Namespace)
			g.Expect(survivors).To(gomega.HaveKey(oldestRevision),
				"the stale deferred pass must not run revision retention")
		})
	}
}

// TestAggregateAndWriteStatus_ConflictSurfacesAsConflict pins the
// terminal-conflict contract the deferred Reconcile handler keys on: when
// every status write loses the optimistic-lock race (RetryOnConflict
// exhausted), aggregateAndWriteStatus returns an error that
// apierrors.IsConflict recognizes — so the caller maps it to a benign
// requeue instead of an ERROR-level "Reconciler error".
func TestAggregateAndWriteStatus_ConflictSurfacesAsConflict(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)

	// Force every status write to lose the optimistic-lock race so the
	// aggregator returns the terminal conflict RetryOnConflict surfaces.
	conflict := interceptor.Funcs{
		SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			return apierrors.NewConflict(
				schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
				obj.GetName(), fmt.Errorf("the object has been modified"))
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir, pod0).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(conflict).
		Build()
	r := &Reconciler{Client: c, APIReader: c, Log: logf.Log.WithName("test"), Expectations: workload.NewExpectations()}

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	err := writeStatus(r, ir, plan, nil, false, nil)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(apierrors.IsConflict(err)).To(gomega.BeTrue(),
		"a terminal status conflict must remain recognizable so the caller can requeue instead of logging ERROR")
}

// TestAggregateStatus_BadImageSurge_EscalatesInstanceToFailed pins the
// end-to-end fast-escalation path: a SurgeThenDrain surge pod stuck in
// ImagePullBackOff past the config-driven stuck-pod grace
// (lifecycle stuckPodGracePeriod) must escalate the Instance Phase to
// Failed within the grace window — not after the full 30-minute
// InstanceReadyTimeout — so the parent ISVC's coordination layer sees
// Component.Failed promptly rather than the group sitting at Surging.
//
// Setup mirrors a realistic mid-surge shape:
//   - Instance idx=0 carries Phase=Updating with an in-flight
//     Operation{Type=Update, Step=Surge} (the surge stamp the workload
//     pipeline writes when starting SurgeThenDrain).
//   - Two pods at ordinal 0 / 1 (old serving + surge).
//   - The surge pod (ordinal 1) is past the grace window with
//     ImagePullBackOff in its ContainerStatuses.
//
// Expected post-Reconcile: Instance Phase flips to Failed via the
// workload escalation pass at the end of workload.Reconcile. Without it
// the test fails: Phase stays at Updating because nothing consumes the
// stuck-pod evidence.
func TestAggregateStatus_BadImageSurge_EscalatesInstanceToFailed(t *testing.T) {
	g := gomega.NewWithT(t)

	ir := baselineIR("llama-engine", "prod", 1)
	// Mid-surge state: Instance Phase=Updating with an in-flight Surge
	// step. Status pre-seeded so the workload dispatcher's
	// DetectUpdateTrigger sees the in-progress operation rather than
	// firing a fresh surge.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "rev-a",
			TargetRevision:  "rev-b",
			ActiveOrdinal:   0,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate,
				Step: "Surge",
				// Far-future deadline so the slow-path expiry
				// (production: 30 min) doesn't race the fast-path
				// escalator we're verifying.
				Deadline: metav1.NewTime(time.Now().Add(30 * time.Minute)),
			},
		},
	}
	// Old (canonical) pod at ordinal 0 — serving v1 traffic.
	oldPod := podForIR(ir, 0, "default", 0, true /*ready*/, true /*serving*/)
	// Surge pod at ordinal 1 — image pulled into ImagePullBackOff.
	// CreationTimestamp set well past the grace window so
	// PodStuckPullFailure's age check fires.
	surgePod := podForIR(ir, 0, "default", 1, false /*ready*/, false /*serving*/)
	surgePod.CreationTimestamp = metav1.NewTime(time.Now().Add(-5 * time.Second))
	surgePod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "ome-container",
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"},
			},
		},
	}

	r, c := newReconcilerWithGrace(t, 1*time.Millisecond, ir, oldPod, surgePod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1))
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		To(gomega.Equal(v1beta1.OMENativeInstanceFailed),
			"workload escalation pass must consume the stuck-pod evidence: surge pod stuck in ImagePullBackOff past grace must flip Instance Phase to Failed")
	// The deadline disposition owns single-pod update escalations:
	// ImagePullBackOff is a workload-caused failure, so the Operation is
	// CLEARED in the same transition (Failed-with-no-Operation is a fresh
	// start; the old Failed-with-Operation continuation re-armed the
	// stampers every pass — the churn loop) and the target revision is
	// recorded in a RetryBlock (Held here: no updateRetry policy is
	// configured, fail-safe).
	g.Expect(got.Status.InstanceStatuses[0].Operation).To(gomega.BeNil(),
		"disposition must clear the failed attempt's Operation in the same transition")
	g.Expect(got.Status.RetryBlocks).To(gomega.HaveLen(1),
		"workload-caused failure must record a RetryBlock for the attempt's target revision")
	g.Expect(got.Status.RetryBlocks[0].State).To(gomega.Equal(v1beta1.RetryBlockHeld),
		"nil retry policy fails safe: first failure Holds")
	g.Expect(got.Status.RetryBlocks[0].Reason).To(gomega.Equal("ImagePullBackOff"))
	g.Expect(got.Status.InstanceStatuses[0].LastFailure).NotTo(gomega.BeNil())
	g.Expect(got.Status.InstanceStatuses[0].LastFailure.Reason).To(gomega.Equal("ImagePullBackOff"))
}

// TestAggregateStatus_StuckPodWithinGrace_NoEscalation pins the
// IR-path debounce: a stuck pod still inside the config-driven
// stuck-pod grace (lifecycle stuckPodGracePeriod) must NOT escalate.
// Without this guard, a brief transient pull failure
// on a flaky registry would mis-classify a pod that's about to recover.
// Mirrors the equivalent omenative-direct-path test
// TestSurgeUpdate_ImagePullBackOff_WithinGrace_NoEscalation.
func TestAggregateStatus_StuckPodWithinGrace_NoEscalation(t *testing.T) {
	g := gomega.NewWithT(t)

	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "rev-a",
			TargetRevision:  "rev-b",
			ActiveOrdinal:   0,
			Operation: &v1beta1.InstanceOperation{
				Type:     v1beta1.InstanceOperationUpdate,
				Step:     "Surge",
				Deadline: metav1.NewTime(time.Now().Add(30 * time.Minute)),
			},
		},
	}
	oldPod := podForIR(ir, 0, "default", 0, true, true)
	surgePod := podForIR(ir, 0, "default", 1, false, false)
	surgePod.CreationTimestamp = metav1.NewTime(time.Now().Add(-5 * time.Second))
	surgePod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "ome-container", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
	}
	// Generous explicit grace so the 5-second-old pod stays inside the
	// window with fast escalation ACTIVE (a zero grace would disable the
	// fast path entirely and make this test vacuous).
	r, c := newReconcilerWithGrace(t, 60*time.Second, ir, oldPod, surgePod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		To(gomega.Equal(v1beta1.OMENativeInstanceUpdating),
			"stuck pod still inside grace window must NOT escalate (debounce holds)")
}

// TestAggregateStatus_DeadlineExpired_EscalatesInstanceToFailed pins the
// broad backstop end-to-end: an Instance stuck in a transient phase whose
// in-flight Operation.Deadline (InstanceReadyTimeout) has elapsed must
// flip to Phase=Failed via the workload escalation pass — even when no
// pod is in a terminal kubelet waiting state, so the fast-path stuck-pod
// evidence CANNOT be what fails it.
//
// This is the regression guard for the InstanceReadyTimeout consumer:
// without the deadline branch of the escalation pass the timeout is
// inert and a never-Ready gang sits at Phase=Updating indefinitely.
func TestAggregateStatus_DeadlineExpired_EscalatesInstanceToFailed(t *testing.T) {
	g := gomega.NewWithT(t)

	ir := baselineIR("llama-engine", "prod", 1)
	// Create-stuck Instance: Phase=Creating with an in-flight Create
	// operation whose Deadline already elapsed. patchInstanceStatusCreating
	// is idempotent (Phase=Creating + Op.Type=Create + Incarnation>0 → no
	// re-stamp), so the dispatcher's Create pass leaves the expired
	// Deadline intact for the backstop to read. A pod exists at the
	// canonical ordinal but never went Ready — the never-converging gang
	// the InstanceReadyTimeout is meant to bound.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:         0,
			Incarnation:   1,
			Phase:         v1beta1.OMENativeInstanceCreating,
			ActiveOrdinal: 0,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationCreate,
				Step: "CreatePods",
				// Deadline already in the past → the InstanceReadyTimeout
				// backstop must escalate this Instance.
				Deadline: metav1.NewTime(time.Now().Add(-1 * time.Minute)),
			},
		},
	}
	// Pod exists at the canonical ordinal but is not Ready (Pending) and
	// has NO terminal kubelet waiting reason, so the stuck-pod escalator
	// stays out of it.
	pendingPod := podForIR(ir, 0, "default", 0, false /*ready*/, false /*serving*/)

	// Generous stuck-pod grace so the fast path can't fire — the ONLY
	// thing that can fail this Instance is the deadline backstop under
	// test.
	r, c := newReconcilerWithGrace(t, 60*time.Second, ir, pendingPod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1))
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		To(gomega.Equal(v1beta1.OMENativeInstanceFailed),
			"workload escalation pass must run the deadline backstop: an Operation.Deadline in the past must flip the Instance Phase to Failed")
	// The deadline disposition owns Create-attempt expiries: no
	// workload-caused pod evidence and no relocation budget configured →
	// terminal branch clears the Operation (fresh start next pass) and
	// records no RetryBlock (failure not proven revision-scoped).
	g.Expect(got.Status.InstanceStatuses[0].Operation).To(gomega.BeNil(),
		"disposition must clear the expired attempt's Operation")
	g.Expect(got.Status.RetryBlocks).To(gomega.BeEmpty(),
		"terminal disposition must not record a RetryBlock")
	// LastFailure records the DeadlineExceeded cause.
	g.Expect(got.Status.InstanceStatuses[0].LastFailure).NotTo(gomega.BeNil(),
		"deadline escalation must record a LastFailure diagnostic")
	g.Expect(got.Status.InstanceStatuses[0].LastFailure.Reason).
		To(gomega.Equal(workload.DeadlineExceededReason))
}

// TestAggregateStatus_GatedInstance_DeadlineParked_NotEscalated pins the
// admission-gating exemption: an Instance whose Operation.Deadline is in
// the past but whose pod is held by a scheduling gate (queued for
// admission, e.g. by Kueue) is NOT stuck — it is queued. The IR aggregator
// must park its deadline (zero it) so the backstop never fails it. Without
// this, every Kueue-gated OME workload would die at InstanceReadyTimeout.
func TestAggregateStatus_GatedInstance_DeadlineParked_NotEscalated(t *testing.T) {
	g := gomega.NewWithT(t)

	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:         0,
			Incarnation:   1,
			Phase:         v1beta1.OMENativeInstanceCreating,
			ActiveOrdinal: 0,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationCreate,
				Step: "CreatePods",
				// Past deadline: would be escalated WERE the pod not gated.
				Deadline: metav1.NewTime(time.Now().Add(-1 * time.Minute)),
			},
		},
	}
	// Pod exists at the canonical ordinal but is held by a scheduling gate
	// (queued for admission), not Ready and not stuck.
	gatedPod := podForIR(ir, 0, "default", 0, false /*ready*/, false /*serving*/)
	gatedPod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}}

	// Explicit generous grace: fast escalation is active but the gated
	// pod carries no stuck evidence, so only the deadline backstop could
	// fail it — and the gate exemption must stop that.
	r, c := newReconcilerWithGrace(t, 60*time.Second, ir, gatedPod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1))
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		NotTo(gomega.Equal(v1beta1.OMENativeInstanceFailed),
			"a Kueue-gated Instance is queued, not stuck: the deadline backstop must NOT fail it")
	g.Expect(got.Status.InstanceStatuses[0].Operation).NotTo(gomega.BeNil())
	g.Expect(got.Status.InstanceStatuses[0].Operation.Deadline.IsZero()).
		To(gomega.BeTrue(),
			"a gated Instance's deadline must be parked (zeroed) so it never expires while queued")
}

// TestReconcileHeldDeadlines_GangSurgeSourceParked pins the surge-pair
// parking rule: while a gang-surge attempt's pods (living in the
// source's Operation.SurgeIndex bucket) queue for admission, BOTH the
// target marker's deadline (own bucket gated) and the SOURCE's deadline
// must park. Without the source-side park, only the target waits — the
// source's clock keeps running and expires during a legitimate queue
// wait, tearing down the queued gang.
func TestReconcileHeldDeadlines_GangSurgeSourceParked(t *testing.T) {
	g := gomega.NewWithT(t)

	surgeIdx := int32(1)
	future := metav1.NewTime(time.Now().Add(30 * time.Minute))
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:       0,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{
				Type:       v1beta1.InstanceOperationUpdate,
				Step:       "Surge",
				SurgeIndex: &surgeIdx,
				Deadline:   future,
			},
		},
		{
			Index:       1,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				Type:     v1beta1.InstanceOperationUpdate,
				Step:     workload.UpdateStepGangSurgeTarget,
				Deadline: future,
			},
		},
	}
	sourcePod := podForIR(ir, 0, "default", 0, true, true)
	gatedPod := podForIR(ir, 1, "default", 0, false, false)
	gatedPod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}}

	r, c := newReconciler(t, ir, sourcePod, gatedPod)
	byIndex := map[int32][]*corev1.Pod{0: {sourcePod}, 1: {gatedPod}}
	g.Expect(r.reconcileHeldDeadlines(context.Background(), ir, byIndex, false, 30*time.Minute)).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(2))
	g.Expect(got.Status.InstanceStatuses[0].Operation.Deadline.IsZero()).
		To(gomega.BeTrue(),
			"gang-surge SOURCE deadline must park while its SurgeIndex bucket is admission-gated")
	g.Expect(got.Status.InstanceStatuses[1].Operation.Deadline.IsZero()).
		To(gomega.BeTrue(),
			"gang-surge target deadline must park while its own pods are admission-gated")
}

// TestAggregateStatus_PausedInstance_DeadlineParkedAndRearmed verifies that
// pausing an in-flight operation is a real circuit breaker: neither terminal
// pod state nor an elapsed deadline may fail it while paused, and unpausing
// restarts the timeout window instead of expiring the old deadline.
func TestAggregateStatus_PausedInstance_DeadlineParkedAndRearmed(t *testing.T) {
	g := gomega.NewWithT(t)

	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Paused = true
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:         0,
		Incarnation:   1,
		Phase:         v1beta1.OMENativeInstanceCreating,
		ActiveOrdinal: 0,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationCreate,
			Step:     "CreatePods",
			Deadline: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
	}}
	stuckPod := podForIR(ir, 0, "default", 0, false, false)
	stuckPod.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
	stuckPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "ome-container",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: "CrashLoopBackOff",
		}},
	}}

	r, c := newReconcilerWithGrace(t, time.Millisecond, ir, stuckPod)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	paused := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, paused)).To(gomega.Succeed())
	g.Expect(paused.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceCreating))
	g.Expect(paused.Status.InstanceStatuses[0].LastFailure).To(gomega.BeNil())
	g.Expect(paused.Status.InstanceStatuses[0].Operation.Deadline.IsZero()).To(gomega.BeTrue())

	// Simulate the container recovering while the operator investigates. The
	// unpause assertion below isolates deadline rearming from the independent
	// stuck-pod fast-failure policy.
	recoveredPod := &corev1.Pod{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(stuckPod), recoveredPod)).To(gomega.Succeed())
	recoveredPod.Status.ContainerStatuses = nil
	g.Expect(c.Status().Update(context.Background(), recoveredPod)).To(gomega.Succeed())

	paused.Spec.Paused = false
	g.Expect(c.Update(context.Background(), paused)).To(gomega.Succeed())
	beforeUnpause := time.Now()
	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	unpaused := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, unpaused)).To(gomega.Succeed())
	g.Expect(unpaused.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceCreating))
	g.Expect(unpaused.Status.InstanceStatuses[0].Operation.Deadline.Time).To(gomega.BeTemporally(">", beforeUnpause))
}

// TestAggregateStatus_DeadlineFuture_NoEscalation pins the negative
// case: a transient-phase Instance whose Operation.Deadline is still in
// the future must NOT be escalated by the deadline backstop.
func TestAggregateStatus_DeadlineFuture_NoEscalation(t *testing.T) {
	g := gomega.NewWithT(t)

	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:         0,
			Incarnation:   1,
			Phase:         v1beta1.OMENativeInstanceCreating,
			ActiveOrdinal: 0,
			Operation: &v1beta1.InstanceOperation{
				Type:     v1beta1.InstanceOperationCreate,
				Step:     "CreatePods",
				Deadline: metav1.NewTime(time.Now().Add(30 * time.Minute)),
			},
		},
	}
	pendingPod := podForIR(ir, 0, "default", 0, false, false)

	// Explicit generous grace: fast escalation active, but the pending
	// pod has no stuck evidence — only the (future) deadline could fire.
	r, c := newReconcilerWithGrace(t, 60*time.Second, ir, pendingPod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		To(gomega.Equal(v1beta1.OMENativeInstanceCreating),
			"Operation.Deadline in the future must NOT escalate (backstop respects the timeout window)")
}

// TestComputeRolloutStalledCondition pins the advisory RolloutStalled
// condition: True only when a rollout is in flight AND an Instance carries a
// terminal failure while not yet on the target; and it is a DISTINCT condition
// type from Ready (advisory, not a Ready dependent).
func TestComputeRolloutStalledCondition(t *testing.T) {
	const target = "isvc-engine-5af56222"
	const cur = "isvc-engine-0f41b090"
	mk := func() *v1beta1.InferenceReplicaStatus {
		return &v1beta1.InferenceReplicaStatus{Replicas: 3, CurrentRevision: cur, UpdateRevision: target}
	}

	// in-flight rollout + an Instance failing while NOT on target → stalled.
	s := mk()
	s.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, RunningRevision: target},
		{Index: 1, RunningRevision: cur, LastFailure: &v1beta1.InstanceTermination{Reason: "CrashLoopBackOff"}},
	}
	if c := computeRolloutStalledCondition(s); c.Status != metav1.ConditionTrue || c.Reason != ReasonInstancesFailing {
		t.Errorf("failing Instance mid-rollout: got %s/%s want True/%s", c.Status, c.Reason, ReasonInstancesFailing)
	}

	// stale failure on an Instance ALREADY on target → not stalled.
	s2 := mk()
	s2.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, RunningRevision: target, LastFailure: &v1beta1.InstanceTermination{Reason: "CrashLoopBackOff"}},
	}
	if c := computeRolloutStalledCondition(s2); c.Status != metav1.ConditionFalse {
		t.Errorf("stale failure on updated Instance must not stall: got %s", c.Status)
	}

	// no rollout in flight (converged) → not stalled even with a failure.
	s3 := mk()
	s3.CurrentRevision = target
	s3.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, RunningRevision: cur, LastFailure: &v1beta1.InstanceTermination{Reason: "CrashLoopBackOff"}},
	}
	if c := computeRolloutStalledCondition(s3); c.Status != metav1.ConditionFalse {
		t.Errorf("converged rollout must not stall: got %s", c.Status)
	}

	// Advisory: distinct condition type from Ready (never folds into it).
	if computeRolloutStalledCondition(mk()).Type == InferenceReplicaConditionReady {
		t.Errorf("RolloutStalled must be a distinct condition type from Ready")
	}

	// Failure the current Operation post-dates → retry, not a wedge.
	old := metav1.NewTime(time.Now().Add(-33 * time.Hour))
	s4 := mk()
	s4.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, RunningRevision: target},
		{
			Index:           1,
			RunningRevision: cur,
			LastFailure:     &v1beta1.InstanceTermination{Reason: "DeadlineExceeded", Time: old},
			Operation: &v1beta1.InstanceOperation{
				Type:           v1beta1.InstanceOperationUpdate,
				Step:           "Drain",
				StartedAt:      metav1.NewTime(time.Now().Add(-10 * time.Minute)),
				LastProgressAt: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
				Deadline:       metav1.NewTime(time.Now().Add(2 * time.Hour)),
				TargetRevision: target,
			},
		},
	}
	if c := computeRolloutStalledCondition(s4); c.Status != metav1.ConditionFalse {
		t.Errorf("failure older than the in-flight Operation must not stall: got %s (%s)", c.Status, c.Message)
	}

	// Failure during the in-flight Operation → still stalls.
	s5 := mk()
	s5.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			RunningRevision: cur,
			LastFailure:     &v1beta1.InstanceTermination{Reason: "CrashLoopBackOff", Time: metav1.NewTime(time.Now())},
			Operation: &v1beta1.InstanceOperation{
				Type:           v1beta1.InstanceOperationUpdate,
				StartedAt:      metav1.NewTime(time.Now().Add(-30 * time.Minute)),
				TargetRevision: target,
			},
		},
	}
	if c := computeRolloutStalledCondition(s5); c.Status != metav1.ConditionTrue {
		t.Errorf("failure during the in-flight Operation must still stall: got %s", c.Status)
	}

	// No Operation → unchanged, still stalls.
	s6 := mk()
	s6.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, RunningRevision: cur, LastFailure: &v1beta1.InstanceTermination{Reason: "CrashLoopBackOff", Time: old}},
	}
	if c := computeRolloutStalledCondition(s6); c.Status != metav1.ConditionTrue {
		t.Errorf("failure with no in-flight Operation must stall: got %s", c.Status)
	}
}

// TestNextRolloutHold_StableSinceAcrossIdenticalGateTarget pins the
// churn-safety contract behind status.rolloutHold: repeated observations
// reporting the SAME Gate+Target preserve the prior Since even when
// Reason text drifts pass to pass (e.g. a fluctuating in-flight count),
// and a change in either Gate or Target restarts the clock.
func TestNextRolloutHold_StableSinceAcrossIdenticalGateTarget(t *testing.T) {
	t0 := metav1.NewTime(time.Unix(1000, 0))
	t1 := metav1.NewTime(time.Unix(2000, 0))

	first := nextRolloutHold(nil, &v1beta1.RolloutHold{
		Gate: v1beta1.RolloutHoldGateBudget, Reason: "budget 2 exhausted (would become 3)", Target: "rev-a",
	}, t0)
	if first == nil || !first.Since.Equal(&t0) {
		t.Fatalf("first observation must stamp Since=now: got %+v", first)
	}

	// Same Gate+Target, DIFFERENT Reason text (a fluctuating count) ->
	// Since must be preserved from the first call, and no update storm
	// results from Reason text alone (verified end-to-end in
	// TestAggregateAndWriteStatus_RolloutHold_ChurnSafeAcrossReconciles).
	second := nextRolloutHold(first, &v1beta1.RolloutHold{
		Gate: v1beta1.RolloutHoldGateBudget, Reason: "budget 2 exhausted (would become 4)", Target: "rev-a",
	}, t1)
	if second == nil || !second.Since.Equal(&t0) {
		t.Errorf("same Gate+Target must preserve Since despite Reason drift: got %+v", second)
	}
	if second == nil || second.Reason != "budget 2 exhausted (would become 4)" {
		t.Errorf("Reason must still update to the latest observation: got %+v", second)
	}

	// A different Target (e.g. a corrected revision published) restarts
	// the clock even with the same Gate.
	third := nextRolloutHold(second, &v1beta1.RolloutHold{
		Gate: v1beta1.RolloutHoldGateBudget, Reason: "budget 2 exhausted (would become 3)", Target: "rev-b",
	}, t1)
	if third == nil || !third.Since.Equal(&t1) {
		t.Errorf("a new Target must restart Since: got %+v", third)
	}

	// A different Gate (the denial moved to a different layer) also
	// restarts the clock even with the same Target.
	fourth := nextRolloutHold(third, &v1beta1.RolloutHold{
		Gate: v1beta1.RolloutHoldGateSequential, Reason: "Sequential waiting on decoder", Target: "rev-b",
	}, t1)
	if fourth == nil || fourth.Gate != v1beta1.RolloutHoldGateSequential || !fourth.Since.Equal(&t1) {
		t.Errorf("a new Gate must restart Since and report the newly observed layer: got %+v", fourth)
	}

	// nil observation clears the hold outright (progressed or converged).
	if cleared := nextRolloutHold(fourth, nil, t1); cleared != nil {
		t.Errorf("nil observation must clear the hold: got %+v", cleared)
	}
}

// TestComputeRetryBlockRolloutHold pins the RetryBlock -> RolloutHold
// projection used when the Update pass never ran this reconcile (every
// candidate Instance was denied before reaching it).
func TestComputeRetryBlockRolloutHold(t *testing.T) {
	now := time.Unix(5000, 0)
	future := metav1.NewTime(now.Add(time.Minute))
	past := metav1.NewTime(now.Add(-time.Minute))

	// Held: unbounded, gate=Held regardless of NextRetryAt (unset here,
	// matching "Held has no time bound").
	held := &v1beta1.InferenceReplicaStatus{
		UpdateRevision: "bad-rev",
		RetryBlocks: []v1beta1.RetryBlock{
			{TargetRevision: "bad-rev", State: v1beta1.RetryBlockHeld, AttemptsStarted: 2, Reason: "ImagePullBackOff"},
		},
	}
	if h := computeRetryBlockRolloutHold(held, now); h == nil || h.Gate != v1beta1.RolloutHoldGateHeld || h.Target != "bad-rev" {
		t.Errorf("Held block must produce gate=Held target=bad-rev: got %+v", h)
	}

	// Backoff not yet due: gate=RetryBlock.
	backoff := &v1beta1.InferenceReplicaStatus{
		UpdateRevision: "bad-rev",
		RetryBlocks: []v1beta1.RetryBlock{
			{TargetRevision: "bad-rev", State: v1beta1.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &future, Reason: "CrashLoopBackOff"},
		},
	}
	if h := computeRetryBlockRolloutHold(backoff, now); h == nil || h.Gate != v1beta1.RolloutHoldGateRetryBlock {
		t.Errorf("not-yet-due Backoff must produce gate=RetryBlock: got %+v", h)
	}

	// Backoff already due: about to retry, not currently held.
	due := &v1beta1.InferenceReplicaStatus{
		UpdateRevision: "bad-rev",
		RetryBlocks: []v1beta1.RetryBlock{
			{TargetRevision: "bad-rev", State: v1beta1.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &past, Reason: "CrashLoopBackOff"},
		},
	}
	if h := computeRetryBlockRolloutHold(due, now); h != nil {
		t.Errorf("an elapsed Backoff window must not report a hold: got %+v", h)
	}

	// RetryInProgress: an attempt is authorized, not a hold.
	inProgress := &v1beta1.InferenceReplicaStatus{
		UpdateRevision: "bad-rev",
		RetryBlocks: []v1beta1.RetryBlock{
			{TargetRevision: "bad-rev", State: v1beta1.RetryBlockRetryInProgress, AttemptsStarted: 1},
		},
	}
	if h := computeRetryBlockRolloutHold(inProgress, now); h != nil {
		t.Errorf("RetryInProgress must not report a hold: got %+v", h)
	}

	// No block for the current target.
	none := &v1beta1.InferenceReplicaStatus{UpdateRevision: "other-rev"}
	if h := computeRetryBlockRolloutHold(none, now); h != nil {
		t.Errorf("no matching RetryBlock must not report a hold: got %+v", h)
	}
}

// TestEffectiveRolloutHold pins the top-level decision effectiveRolloutHold
// makes each reconcile: no rollout / converged always clears regardless of
// a stale signal; an observed Update-pass verdict is authoritative when
// the Update pass ran; the persisted RetryBlock/Held state is the fallback
// when it did not.
func TestEffectiveRolloutHold(t *testing.T) {
	now := time.Unix(9000, 0)

	// No rollout in flight: nothing to hold, even with an exec verdict.
	noRollout := &v1beta1.InferenceReplicaStatus{}
	if h := effectiveRolloutHold(true, &workload.RolloutHold{Gate: workload.RolloutHoldGateBudget, Reason: "x", Target: "y"}, noRollout, now); h != nil {
		t.Errorf("no UpdateRevision must clear regardless of exec verdict: got %+v", h)
	}

	// Converged (CurrentRevision == UpdateRevision): nothing to hold.
	converged := &v1beta1.InferenceReplicaStatus{CurrentRevision: "rev-a", UpdateRevision: "rev-a"}
	if h := effectiveRolloutHold(true, &workload.RolloutHold{Gate: workload.RolloutHoldGateBudget, Reason: "x", Target: "rev-a"}, converged, now); h != nil {
		t.Errorf("converged rollout must clear regardless of exec verdict: got %+v", h)
	}

	inFlight := &v1beta1.InferenceReplicaStatus{CurrentRevision: "rev-a", UpdateRevision: "rev-b"}

	// Update pass ran and found nothing to hold (forward progress) ->
	// clear even though the rollout is still in flight.
	if h := effectiveRolloutHold(true, nil, inFlight, now); h != nil {
		t.Errorf("Update pass ran with a nil verdict must clear: got %+v", h)
	}

	// Update pass ran and denied a fresh start -> the exec verdict wins,
	// converted to the v1beta1 shape.
	h := effectiveRolloutHold(true, &workload.RolloutHold{
		Gate: workload.RolloutHoldGateSequential, Reason: "Sequential waiting on decoder", Target: "rev-b",
	}, inFlight, now)
	if h == nil || h.Gate != v1beta1.RolloutHoldGateSequential || h.Reason != "Sequential waiting on decoder" || h.Target != "rev-b" {
		t.Errorf("an observed exec denial must be surfaced verbatim: got %+v", h)
	}

	// Update pass did NOT run this reconcile -> fall back to the
	// persisted RetryBlock/Held state.
	inFlight.RetryBlocks = []v1beta1.RetryBlock{
		{TargetRevision: "rev-b", State: v1beta1.RetryBlockHeld, AttemptsStarted: 2, Reason: "ImagePullBackOff"},
	}
	if h := effectiveRolloutHold(false, nil, inFlight, now); h == nil || h.Gate != v1beta1.RolloutHoldGateHeld {
		t.Errorf("Update pass not run must fall back to the persisted RetryBlock/Held state: got %+v", h)
	}
}

// TestAggregateAndWriteStatus_RolloutHold_ChurnSafeAcrossReconciles proves
// the end-to-end write contract: a fresh denial writes and stamps Since; a
// byte-identical repeat denial performs ZERO writes (ResourceVersion
// unchanged); and forward progress (a nil exec verdict) clears the hold
// with a write.
func TestAggregateAndWriteStatus_RolloutHold_ChurnSafeAcrossReconciles(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.CurrentRevision = "rev-prior"
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-prior"},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev-target"}}
	key := client.ObjectKeyFromObject(ir)

	budgetHold := func(inFlight int) *workload.RolloutHold {
		return &workload.RolloutHold{
			Gate:   workload.RolloutHoldGateBudget,
			Reason: fmt.Sprintf("per-Component surge budget 2 exhausted (would become %d)", inFlight),
			Target: target.Name,
		}
	}

	// Pass 1: a fresh denial -> the hold is recorded with Since stamped.
	g.Expect(writeStatus(r, ir, plan, target, true, budgetHold(3))).To(gomega.Succeed())
	afterFirst := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, afterFirst)).To(gomega.Succeed())
	g.Expect(afterFirst.Status.RolloutHold).NotTo(gomega.BeNil())
	g.Expect(afterFirst.Status.RolloutHold.Gate).To(gomega.Equal(v1beta1.RolloutHoldGateBudget))
	since := afterFirst.Status.RolloutHold.Since
	g.Expect(since.IsZero()).To(gomega.BeFalse())

	// Pass 2: byte-identical denial -> ZERO writes (ResourceVersion
	// unchanged) and Since untouched.
	rvAfterFirst := afterFirst.ResourceVersion
	g.Expect(writeStatus(r, afterFirst.DeepCopy(), plan, target, true, budgetHold(3))).To(gomega.Succeed())
	afterRepeat := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, afterRepeat)).To(gomega.Succeed())
	g.Expect(afterRepeat.ResourceVersion).To(gomega.Equal(rvAfterFirst),
		"an identical repeat denial must perform ZERO writes")
	g.Expect(afterRepeat.Status.RolloutHold.Since.Equal(&since)).To(gomega.BeTrue(),
		"Since must not move across a byte-identical repeat denial")

	// Pass 3: forward progress (nil exec verdict) -> the hold clears, and
	// this DOES write (a real state change).
	g.Expect(writeStatus(r, afterRepeat.DeepCopy(), plan, target, true, nil)).To(gomega.Succeed())
	afterClear := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, afterClear)).To(gomega.Succeed())
	g.Expect(afterClear.Status.RolloutHold).To(gomega.BeNil(), "forward progress must clear the hold")
	g.Expect(afterClear.ResourceVersion).NotTo(gomega.Equal(rvAfterFirst), "clearing the hold is a real state change")
}

// TestAggregateAndWriteStatus_RolloutHold_SoakChurnSafeAcrossMultipleReconciles
// is the regression pin for a live-countdown reason string turning a
// multi-minute Sequential.Soak window into a per-reconcile status-write
// storm: the coordination gate's soak-denial message
// (coordination.CheckSequential) carries only stable facts (component +
// configured duration), so driving several reconciles "mid-soak" with the
// identical exec verdict must perform ZERO writes after the first and
// must never move Since — exactly the same contract
// TestAggregateAndWriteStatus_RolloutHold_ChurnSafeAcrossReconciles proves
// for a Budget hold, pinned here against the specific reason shape a
// Sequential soak produces.
func TestAggregateAndWriteStatus_RolloutHold_SoakChurnSafeAcrossMultipleReconciles(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.CurrentRevision = "rev-prior"
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-prior"},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev-target"}}
	key := client.ObjectKeyFromObject(ir)

	// coordination.CheckSequential's soak-denial message carries only stable
	// facts for the whole soak window: component, configured duration, and the
	// completed predecessor, with no elapsed/remaining countdown.
	soakHold := &workload.RolloutHold{
		Gate:   workload.RolloutHoldGateSequential,
		Reason: "Sequential.Soak: engine waiting out the 15s soak after decoder",
		Target: target.Name,
	}

	g.Expect(writeStatus(r, ir, plan, target, true, soakHold)).To(gomega.Succeed())
	first := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, first)).To(gomega.Succeed())
	g.Expect(first.Status.RolloutHold).NotTo(gomega.BeNil())
	g.Expect(first.Status.RolloutHold.Gate).To(gomega.Equal(v1beta1.RolloutHoldGateSequential))
	rv := first.ResourceVersion
	since := first.Status.RolloutHold.Since
	g.Expect(since.IsZero()).To(gomega.BeFalse())

	// Simulate several reconciles landing mid-soak (e.g. the dispatcher's
	// short requeue firing repeatedly while the soak elapses). Every one
	// observes the identical exec verdict, since the gate's message does not
	// depend on elapsed time.
	live := first
	for i := 0; i < 4; i++ {
		g.Expect(writeStatus(r, live.DeepCopy(), plan, target, true, soakHold)).To(gomega.Succeed())
		next := &v1beta1.InferenceReplica{}
		g.Expect(c.Get(context.Background(), key, next)).To(gomega.Succeed())
		g.Expect(next.ResourceVersion).To(gomega.Equal(rv),
			"reconcile %d mid-soak must perform ZERO writes (identical Sequential.Soak reason)", i+2)
		g.Expect(next.Status.RolloutHold.Since.Equal(&since)).To(gomega.BeTrue(),
			"reconcile %d mid-soak must not move Since", i+2)
		live = next
	}
}

// TestAggregateAndWriteStatus_RolloutHold_PausedRetryBlockFallbackChurnSafe
// checks the hold surface under an operator pause. Plan returns before
// ever reaching Update selection while paused (see workload/plan_decision.go),
// so executeUpdatePass never runs and RecordRolloutHold is never invoked —
// every paused reconcile calls aggregateAndWriteStatus with
// holdObserved=false, exercising the same RetryBlock/Held fallback used
// when nothing was gated. A standing Held block must keep reporting
// (and keep its Since) across repeated paused reconciles with ZERO
// writes — pausing must not fabricate a fresh Since nor storm the
// apiserver.
func TestAggregateAndWriteStatus_RolloutHold_PausedRetryBlockFallbackChurnSafe(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.CurrentRevision = "rev-prior"
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-prior"},
	}
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{
		{TargetRevision: "rev-target", State: v1beta1.RetryBlockHeld, AttemptsStarted: 2, Reason: "ImagePullBackOff"},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev-target"}}
	key := client.ObjectKeyFromObject(ir)

	// holdObserved=false on every call: the Update pass is parked by
	// Paused, exactly as it would be on every reconcile while paused.
	g.Expect(writeStatus(r, ir, plan, target, false, nil)).To(gomega.Succeed())
	first := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, first)).To(gomega.Succeed())
	g.Expect(first.Status.RolloutHold).NotTo(gomega.BeNil())
	g.Expect(first.Status.RolloutHold.Gate).To(gomega.Equal(v1beta1.RolloutHoldGateHeld))
	rv := first.ResourceVersion
	since := first.Status.RolloutHold.Since
	g.Expect(since.IsZero()).To(gomega.BeFalse())

	live := first
	for i := 0; i < 3; i++ {
		g.Expect(writeStatus(r, live.DeepCopy(), plan, target, false, nil)).To(gomega.Succeed())
		next := &v1beta1.InferenceReplica{}
		g.Expect(c.Get(context.Background(), key, next)).To(gomega.Succeed())
		g.Expect(next.ResourceVersion).To(gomega.Equal(rv),
			"paused reconcile %d must perform ZERO writes", i+2)
		g.Expect(next.Status.RolloutHold.Since.Equal(&since)).To(gomega.BeTrue(),
			"paused reconcile %d must not fabricate a fresh Since", i+2)
		live = next
	}
}
