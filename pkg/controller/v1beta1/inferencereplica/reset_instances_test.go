package inferencereplica

// Reset-mailbox coverage for the ome.io/reset-instances annotation: a
// Failed Instance named by index (or by "all") has every pod deleted and
// its preserved Operation cleared while Phase=Failed and LastFailure
// survive; a still-serving Instance and one parked behind a rollout- or
// migration-owned continuation are skipped untouched; non-Failed or
// unknown targets and malformed values consume as explanatory no-ops; a
// re-delivered request finds nothing to do.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// newResetFixture builds a fake-client Reconciler (with recorder and a
// fresh Expectations cache) around the IR plus its pods, optionally
// behind interceptor funcs, and re-reads the IR so the caller holds the
// apiserver copy.
func newResetFixture(t *testing.T, ir *v1beta1.InferenceReplica, pods []*corev1.Pod, funcs *interceptor.Funcs) (*Reconciler, client.Client, *record.FakeRecorder, *v1beta1.InferenceReplica) {
	t.Helper()
	objs := []client.Object{ir}
	for _, p := range pods {
		objs = append(objs, p)
	}
	b := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{})
	if funcs != nil {
		b = b.WithInterceptorFuncs(*funcs)
	}
	c := b.Build()
	rec := record.NewFakeRecorder(16)
	r := &Reconciler{
		Client:               c,
		APIReader:            c,
		Log:                  logf.Log.WithName("test"),
		Recorder:             rec,
		Expectations:         workload.NewExpectations(),
		InstanceStatusTarget: irstatus.EncodingDenseV1,
	}
	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	return r, c, rec, fresh
}

// resetIR is baselineIR (five replicas) plus the reset annotation and
// five Instances: 0 Ready; 1 Failed behind a deadline-expired Restart
// (with a LastFailure trace); 2 Failed behind a preserved Create; 3
// Failed behind a preserved Update (rollout-owned); 4 Failed behind a
// Restart whose pod is still in the serving rotation.
func resetIR(annotationValue string) *v1beta1.InferenceReplica {
	ir := baselineIR("llama-engine", "default", 5)
	ir.Annotations[constants.ResetInstancesAnnotationKey] = annotationValue
	now := metav1.Now()
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "llama-engine-aaaaaaaa", Incarnation: 1},
		{
			Index: 1, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: "llama-engine-aaaaaaaa", Incarnation: 2,
			Operation: &v1beta1.InstanceOperation{
				ID: "restart-1-1", Type: v1beta1.InstanceOperationRestart, Step: "Recreate",
				StartedAt: now, LastProgressAt: now, Deadline: now, Reason: "pod count 1 below desired 2",
			},
			LastFailure: &v1beta1.InstanceTermination{PodName: "llama-engine-1-worker-0", Reason: "OOMKilled", ExitCode: ptr.To(int32(137)), Time: now},
		},
		{
			Index: 2, Phase: v1beta1.OMENativeInstanceFailed, Incarnation: 1,
			Operation: &v1beta1.InstanceOperation{
				ID: "create-2-1", Type: v1beta1.InstanceOperationCreate, Step: "CreatePods",
				StartedAt: now, LastProgressAt: now, Deadline: now, TargetRevision: "llama-engine-aaaaaaaa",
			},
		},
		{
			Index: 3, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: "llama-engine-aaaaaaaa", Incarnation: 1,
			Operation: &v1beta1.InstanceOperation{
				ID: "update-3-1", Type: v1beta1.InstanceOperationUpdate, Step: "WaitReady",
				StartedAt: now, LastProgressAt: now, Deadline: now, TargetRevision: "llama-engine-bbbbbbbb",
			},
		},
		{
			Index: 4, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: "llama-engine-aaaaaaaa", Incarnation: 1,
			Operation: &v1beta1.InstanceOperation{
				ID: "restart-4-1", Type: v1beta1.InstanceOperationRestart, Step: "Drain",
				StartedAt: now, LastProgressAt: now, Deadline: now,
			},
		},
	}
	return ir
}

// resetPods returns the pod set behind resetIR: one Ready pod at 0, a
// leader+worker gang at 1 (neither Ready), a Phase=Failed pod at 2, a
// Running pod at 3, and a pod still serving at 4.
func resetPods(ir *v1beta1.InferenceReplica) []*corev1.Pod {
	failed := podForIR(ir, 2, "default", 0, false, false)
	failed.Status.Phase = corev1.PodFailed
	return []*corev1.Pod{
		podForIR(ir, 0, "default", 0, true, true),
		podForIR(ir, 1, "leader", 0, false, false),
		podForIR(ir, 1, "worker", 0, false, false),
		failed,
		podForIR(ir, 3, "default", 0, false, false),
		podForIR(ir, 4, "default", 0, true, true),
	}
}

// resetPodCounts is the per-instance pod count behind resetPods.
var resetPodCounts = map[int32]int{0: 1, 1: 2, 2: 1, 3: 1, 4: 1}

// livePodsForInstance lists the pods carrying the given instance-index
// label on the apiserver copy.
func livePodsForInstance(t *testing.T, c client.Client, ir *v1beta1.InferenceReplica, idx int32) []corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	if err := c.List(context.Background(), list, client.InNamespace(ir.Namespace),
		client.MatchingLabels{query.LabelInstanceIdx: intToLabel(int64(idx))}); err != nil {
		t.Fatalf("list pods for instance %d: %v", idx, err)
	}
	return list.Items
}

// assertResetAnnotationConsumed checks the annotation is gone from both
// the apiserver copy and the caller's in-memory IR.
func assertResetAnnotationConsumed(t *testing.T, g *gomega.WithT, c client.Client, ir *v1beta1.InferenceReplica) {
	t.Helper()
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Annotations).NotTo(gomega.HaveKey(constants.ResetInstancesAnnotationKey),
		"the annotation must be consumed on the apiserver copy")
	g.Expect(ir.Annotations).NotTo(gomega.HaveKey(constants.ResetInstancesAnnotationKey),
		"the consumed annotation must be mirrored off the in-memory IR")
}

// assertInstanceReset checks the rebuild shape for idx on the apiserver
// copy and the in-memory mirror: no pods, Operation cleared, Phase still
// Failed, LastFailure untouched.
func assertInstanceReset(t *testing.T, g *gomega.WithT, c client.Client, ir *v1beta1.InferenceReplica, idx int32, want v1beta1.OMENativeInstanceStatus) {
	t.Helper()
	g.Expect(livePodsForInstance(t, c, ir, idx)).To(gomega.BeEmpty(),
		"every pod of instance %d must be deleted", idx)
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	got := instanceStatusAt(t, fresh, idx)
	g.Expect(got.Operation).To(gomega.BeNil(), "instance %d must have its preserved Operation cleared", idx)
	g.Expect(got.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceFailed), "instance %d must stay Failed", idx)
	g.Expect(got.LastFailure).To(gomega.Equal(want.LastFailure), "instance %d must keep LastFailure", idx)
	g.Expect(got.Incarnation).To(gomega.Equal(want.Incarnation), "instance %d must keep its Incarnation", idx)
	g.Expect(got.RunningRevision).To(gomega.Equal(want.RunningRevision), "instance %d must keep its RunningRevision", idx)
	g.Expect(instanceStatusAt(t, ir, idx)).To(gomega.Equal(got),
		"the committed status must be mirrored onto the in-memory IR")
}

// assertInstanceUntouched checks idx kept every pod and its status
// entry exactly as seeded.
func assertInstanceUntouched(t *testing.T, g *gomega.WithT, c client.Client, ir *v1beta1.InferenceReplica, idx int32, want v1beta1.OMENativeInstanceStatus) {
	t.Helper()
	g.Expect(livePodsForInstance(t, c, ir, idx)).To(gomega.HaveLen(resetPodCounts[idx]),
		"instance %d must keep its pods", idx)
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(instanceStatusAt(t, fresh, idx)).To(gomega.Equal(want), "instance %d must keep its status", idx)
}

// TestConsumeResetInstances_All_ResetsEveryFailedInstance pins the "all"
// form: the Failed Instances parked behind a Restart and a Create lose
// every pod (regardless of pod phase) and their Operation, the Ready
// Instance is untouched, the rollout-owned and still-serving Failed
// Instances are skipped untouched, the annotation is consumed, one
// InstancesReset event names the indices reset, and the delete
// expectations are recorded so the Create pass waits for the watch.
func TestConsumeResetInstances_All_ResetsEveryFailedInstance(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR(constants.ResetInstancesAll)
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
	want := map[int32]v1beta1.OMENativeInstanceStatus{}
	for idx := int32(0); idx < 5; idx++ {
		want[idx] = instanceStatusAt(t, ir, idx)
	}

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	assertInstanceReset(t, g, c, ir, 1, want[1])
	assertInstanceReset(t, g, c, ir, 2, want[2])
	assertInstanceUntouched(t, g, c, ir, 0, want[0])
	assertInstanceUntouched(t, g, c, ir, 3, want[3])
	assertInstanceUntouched(t, g, c, ir, 4, want[4])

	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	for _, idx := range []int32{1, 2} {
		g.Expect(r.Expectations.Satisfied(ir.Namespace, ir.Spec.ParentRef.Name, component, idx)).To(gomega.BeFalse(),
			"deletes must be expected for instance %d until the watch observes them", idx)
	}
	for _, idx := range []int32{0, 3, 4} {
		g.Expect(r.Expectations.Satisfied(ir.Namespace, ir.Spec.ParentRef.Name, component, idx)).To(gomega.BeTrue(),
			"no expectation may be recorded for untouched instance %d", idx)
	}

	assertResetAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.HaveLen(1))
	g.Expect(eventsContaining(events, "instance(s) 1,2 ")).NotTo(gomega.BeEmpty(),
		"the event must name the indices reset")
	g.Expect(eventsContaining(events, "operator request")).NotTo(gomega.BeEmpty())
	skips := eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))
	g.Expect(skips).To(gomega.HaveLen(1), "the guarded Failed instances share one skip event")
	g.Expect(skips[0]).To(gomega.ContainSubstring("3 (owned by Update)"))
	g.Expect(skips[0]).To(gomega.ContainSubstring("4 (still serving)"))
	g.Expect(skips[0]).NotTo(gomega.ContainSubstring("0 ("), "\"all\" must not report the non-Failed instance as skipped")
}

// TestConsumeResetInstances_ExplicitList_ResetsOnlyNamed pins the index
// form: only the named Failed Instance is reset; the other Failed one
// keeps its pods and its Operation.
func TestConsumeResetInstances_ExplicitList_ResetsOnlyNamed(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR("2")
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
	want1, want2 := instanceStatusAt(t, ir, 1), instanceStatusAt(t, ir, 2)

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	assertInstanceReset(t, g, c, ir, 2, want2)
	assertInstanceUntouched(t, g, c, ir, 1, want1)

	assertResetAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.HaveLen(1))
	g.Expect(eventsContaining(events, "instance(s) 2 ")).NotTo(gomega.BeEmpty())
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))).To(gomega.BeEmpty())
}

// TestConsumeResetInstances_RepairOwnedTargets_Reset pins the scope of
// the verb: attempts parked by the repair passes — a Restart and a
// Create — are both reset like an Operation-free Instance would be.
func TestConsumeResetInstances_RepairOwnedTargets_Reset(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR("1,2")
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
	want1, want2 := instanceStatusAt(t, ir, 1), instanceStatusAt(t, ir, 2)
	g.Expect(want1.Operation.Type).To(gomega.Equal(v1beta1.InstanceOperationRestart))
	g.Expect(want2.Operation.Type).To(gomega.Equal(v1beta1.InstanceOperationCreate))

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	assertInstanceReset(t, g, c, ir, 1, want1)
	assertInstanceReset(t, g, c, ir, 2, want2)
	assertResetAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, "instance(s) 1,2 ")).To(gomega.HaveLen(1))
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))).To(gomega.BeEmpty())
}

// TestConsumeResetInstances_ServingTarget_SkipConsumed pins the serving
// guard: a Failed Instance with a pod still in the serving rotation is
// left exactly as it was — pod and preserved Operation — no expectation
// is recorded, the annotation is consumed, and the skip event names the
// index with "still serving".
func TestConsumeResetInstances_ServingTarget_SkipConsumed(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR("4")
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
	want4 := instanceStatusAt(t, ir, 4)
	g.Expect(want4.Operation).NotTo(gomega.BeNil())

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	assertInstanceUntouched(t, g, c, ir, 4, want4)
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	g.Expect(r.Expectations.Satisfied(ir.Namespace, ir.Spec.ParentRef.Name, component, 4)).To(gomega.BeTrue(),
		"a skipped instance must record no delete expectation")

	assertResetAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.BeEmpty())
	skips := eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))
	g.Expect(skips).To(gomega.HaveLen(1))
	g.Expect(skips[0]).To(gomega.ContainSubstring("4 (still serving)"))
}

// TestConsumeResetInstances_RolloutOwnedTarget_SkipConsumed pins the
// operation-owner guard: a Failed Instance parked behind an Update or a
// Migrate continuation is skipped with its owner named, nothing is
// deleted, and the Operation survives.
func TestConsumeResetInstances_RolloutOwnedTarget_SkipConsumed(t *testing.T) {
	for _, opType := range []v1beta1.InstanceOperationType{v1beta1.InstanceOperationUpdate, v1beta1.InstanceOperationMigrate} {
		t.Run(string(opType), func(t *testing.T) {
			g := gomega.NewWithT(t)
			seed := resetIR("3")
			seed.Status.InstanceStatuses[3].Operation.Type = opType
			r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
			want3 := instanceStatusAt(t, ir, 3)

			requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(requeue).To(gomega.BeFalse())

			assertInstanceUntouched(t, g, c, ir, 3, want3)
			component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
			g.Expect(r.Expectations.Satisfied(ir.Namespace, ir.Spec.ParentRef.Name, component, 3)).To(gomega.BeTrue(),
				"a skipped instance must record no delete expectation")

			assertResetAnnotationConsumed(t, g, c, ir)

			events := drainEvents(rec)
			g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.BeEmpty())
			skips := eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))
			g.Expect(skips).To(gomega.HaveLen(1))
			g.Expect(skips[0]).To(gomega.ContainSubstring(fmt.Sprintf("3 (owned by %s)", opType)))
		})
	}
}

// TestConsumeResetInstances_MixedTargets_ResetsValidSkipsRest pins a list
// mixing a Failed index, a Ready index, and an index with no Instance:
// the Failed one is reset, the other two are skipped in one event that
// names each with its reason, and the annotation is consumed.
func TestConsumeResetInstances_MixedTargets_ResetsValidSkipsRest(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR(" 1, 0 ,42,1")
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
	want0, want1 := instanceStatusAt(t, ir, 0), instanceStatusAt(t, ir, 1)

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	assertInstanceReset(t, g, c, ir, 1, want1)
	assertInstanceUntouched(t, g, c, ir, 0, want0)
	g.Expect(livePodsForInstance(t, c, ir, 2)).To(gomega.HaveLen(1), "an unnamed instance must not be touched")

	assertResetAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.HaveLen(1))
	g.Expect(eventsContaining(events, "instance(s) 1 ")).NotTo(gomega.BeEmpty(),
		"the duplicate index must collapse to one reset")
	skips := eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))
	g.Expect(skips).To(gomega.HaveLen(1), "all skipped targets share one event")
	g.Expect(skips[0]).To(gomega.ContainSubstring("0 (Phase=Ready)"))
	g.Expect(skips[0]).To(gomega.ContainSubstring("42 (no such instance)"))
}

// TestConsumeResetInstances_NonFailedTarget_SkipConsumed pins the
// not-Failed branch: a Ready target is left exactly as it was — pods
// and status — the annotation is still consumed, and the skip event
// explains why nothing changed.
func TestConsumeResetInstances_NonFailedTarget_SkipConsumed(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR("0")
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
	before := ir.Status.DeepCopy()

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	g.Expect(livePodsForInstance(t, c, ir, 0)).To(gomega.HaveLen(1))
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status).To(gomega.Equal(*before), "a skipped request must write no status")

	assertResetAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.BeEmpty())
	skips := eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))
	g.Expect(skips).To(gomega.HaveLen(1))
	g.Expect(skips[0]).To(gomega.ContainSubstring("0 (Phase=Ready)"))
}

// TestConsumeResetInstances_AllWithNoFailed_SkipConsumed pins "all" on a
// Component with no Failed Instance: nothing changes, one skip event
// says so, and the annotation is consumed.
func TestConsumeResetInstances_AllWithNoFailed_SkipConsumed(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR(constants.ResetInstancesAll)
	seed.Status.InstanceStatuses = seed.Status.InstanceStatuses[:1]
	pods := resetPods(seed)[:1]
	r, c, rec, ir := newResetFixture(t, seed, pods, nil)

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	g.Expect(livePodsForInstance(t, c, ir, 0)).To(gomega.HaveLen(1))
	assertResetAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.BeEmpty())
	skips := eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))
	g.Expect(skips).To(gomega.HaveLen(1))
	g.Expect(skips[0]).To(gomega.ContainSubstring("none is Failed"))
}

// TestConsumeResetInstances_MalformedValue_WarningConsumed pins the
// malformed branch for each rejected shape: nothing is deleted or
// written, a Warning names the value, and the annotation is consumed.
func TestConsumeResetInstances_MalformedValue_WarningConsumed(t *testing.T) {
	for _, val := range []string{"", "1,x", "-1", "1,,2", "ALL"} {
		t.Run(val, func(t *testing.T) {
			g := gomega.NewWithT(t)
			seed := resetIR(val)
			r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
			before := ir.Status.DeepCopy()

			requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(requeue).To(gomega.BeFalse())

			for idx, n := range resetPodCounts {
				g.Expect(livePodsForInstance(t, c, ir, idx)).To(gomega.HaveLen(n), "a rejected request must delete nothing")
			}
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
			g.Expect(fresh.Status).To(gomega.Equal(*before), "a rejected request must write no status")

			assertResetAnnotationConsumed(t, g, c, ir)

			events := drainEvents(rec)
			warnings := eventsContaining(events, string(workload.EventReasonInstancesResetRejected))
			g.Expect(warnings).To(gomega.HaveLen(1))
			g.Expect(warnings[0]).To(gomega.HavePrefix(corev1.EventTypeWarning))
			g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.BeEmpty())
		})
	}
}

// TestConsumeResetInstances_CrashBeforeConsume_RedeliveryIsNoOp pins the
// write order and idempotent re-delivery. Pass one commits the pod
// deletes and the status write, then fails the annotation delete (the
// simulated crash): the annotation survives and the error re-drives the
// request. Pass two finds nothing to do — no delete, no status write,
// no InstancesReset event — and just consumes.
func TestConsumeResetInstances_CrashBeforeConsume_RedeliveryIsNoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	crash := true
	deletes := 0
	funcs := &interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*v1beta1.InferenceReplica); ok && crash {
				return errors.New("simulated crash before the annotation delete")
			}
			return cl.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deletes++
			return cl.Delete(ctx, obj, opts...)
		},
	}
	seed := resetIR("1")
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), funcs)
	want1 := instanceStatusAt(t, ir, 1)

	_, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).To(gomega.HaveOccurred(), "a failed annotation delete must surface so the request re-drives")
	g.Expect(deletes).To(gomega.Equal(2), "both gang pods are deleted before the annotation is touched")
	assertInstanceReset(t, g, c, ir, 1, want1)
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Annotations).To(gomega.HaveKeyWithValue(constants.ResetInstancesAnnotationKey, "1"),
		"the effects commit before the annotation is removed")
	g.Expect(ir.Annotations).To(gomega.HaveKey(constants.ResetInstancesAnnotationKey),
		"an unconsumed request must not be mirrored off the in-memory IR")
	g.Expect(eventsContaining(drainEvents(rec), string(workload.EventReasonInstancesReset)+" ")).To(gomega.HaveLen(1))
	afterCrash := fresh.Status.DeepCopy()

	// Re-delivery after the crash: the request is still in the mailbox.
	crash = false
	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	g.Expect(deletes).To(gomega.Equal(2), "re-delivery must issue no further deletes")
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status).To(gomega.Equal(*afterCrash), "re-delivery must write no status")
	assertResetAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonInstancesReset)+" ")).To(gomega.BeEmpty(),
		"re-delivery must not report a second reset")
	skips := eventsContaining(events, string(workload.EventReasonInstancesResetSkipped))
	g.Expect(skips).To(gomega.HaveLen(1))
	g.Expect(skips[0]).To(gomega.ContainSubstring("1 (nothing to reset)"))
}

// TestConsumeResetInstances_PodDeleteError_LeavesAnnotation pins the
// error path: a failed pod delete rolls its expectation back, surfaces
// the error, and leaves the annotation in place so the request
// re-drives next pass.
func TestConsumeResetInstances_PodDeleteError_LeavesAnnotation(t *testing.T) {
	g := gomega.NewWithT(t)
	funcs := &interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return errors.New("simulated apiserver outage")
		},
	}
	seed := resetIR("2")
	r, c, _, ir := newResetFixture(t, seed, resetPods(seed), funcs)
	before := ir.Status.DeepCopy()

	_, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).To(gomega.HaveOccurred())

	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	g.Expect(r.Expectations.Satisfied(ir.Namespace, ir.Spec.ParentRef.Name, component, 2)).To(gomega.BeTrue(),
		"a failed delete RPC fires no watch event, so its expectation must be rolled back")
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Annotations).To(gomega.HaveKeyWithValue(constants.ResetInstancesAnnotationKey, "2"),
		"the request must stay in the mailbox until the reset commits")
	g.Expect(fresh.Status).To(gomega.Equal(*before), "the Operation is cleared only after the pods are gone")
}

// TestConsumeResetInstances_NoAnnotation_NoOp pins the empty-mailbox fast
// path: no annotation means no writes and no events.
func TestConsumeResetInstances_NoAnnotation_NoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR("1")
	delete(seed.Annotations, constants.ResetInstancesAnnotationKey)
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
	rv := ir.ResourceVersion

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.ResourceVersion).To(gomega.Equal(rv), "empty mailbox must perform zero writes")
	g.Expect(livePodsForInstance(t, c, ir, 1)).To(gomega.HaveLen(2))
	g.Expect(drainEvents(rec)).To(gomega.BeEmpty())
}

// TestConsumeResetInstances_ParentIsEventTarget pins the event routing:
// with a resolvable parent, the reset event lands on the ISVC (the
// user-facing stream), matching the rest of the IR event surface.
func TestConsumeResetInstances_ParentIsEventTarget(t *testing.T) {
	g := gomega.NewWithT(t)
	seed := resetIR("1")
	r, c, rec, ir := newResetFixture(t, seed, resetPods(seed), nil)
	parent := migrationParent(nil, false)

	requeue, err := r.consumeResetInstancesRequest(context.Background(), r.Log, ir, parent)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	assertResetAnnotationConsumed(t, g, c, ir)
	g.Expect(eventsContaining(drainEvents(rec), string(workload.EventReasonInstancesReset)+" ")).To(gomega.HaveLen(1))
}

// TestParseResetInstancesValue pins the accepted and rejected value
// shapes of the annotation.
func TestParseResetInstancesValue(t *testing.T) {
	g := gomega.NewWithT(t)

	req, err := parseResetInstancesValue(" all ")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(req.all).To(gomega.BeTrue())
	g.Expect(req.indices).To(gomega.BeEmpty())

	req, err = parseResetInstancesValue("13, 14,13,0")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(req.all).To(gomega.BeFalse())
	g.Expect(req.indices).To(gomega.Equal([]int32{13, 14, 0}), "duplicates collapse in first-seen order")

	for _, bad := range []string{"", " ", ",", "1,", "1,,2", "x", "-1", "1.5", "ALL", "all,1", "99999999999"} {
		_, err := parseResetInstancesValue(bad)
		g.Expect(err).To(gomega.HaveOccurred(), "value %q must be rejected", bad)
	}
}
