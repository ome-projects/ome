package ops_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Test file is in package ops_test so tests only see the workload.*
// API surface — no privileged access to ops/ internals.

// newFakeClient builds a controller-runtime fake client with the
// scheme the workload reconciler needs.
func newFakeClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(initObjs...).
		Build()
}

// irName is the InferenceReplica name the ops_test helpers persist per-instance
// status on (matches irprojector.InferenceReplicaName: isvcName-component).
func irName(isvc *v1beta1.InferenceService, component workload.ComponentType) string {
	return isvc.Name + "-" + string(component)
}

// instanceIR builds the InferenceReplica carrying the given per-instance
// statuses for (isvc, component). The IR is the source of truth for
// per-instance detail (the ISVC carries none), so fixtures seed it here
// and pass the returned IR to newFakeClient alongside the ISVC.
func instanceIR(isvc *v1beta1.InferenceService, component workload.ComponentType, insts ...v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: irName(isvc, component)},
		Status:     v1beta1.InferenceReplicaStatus{InstanceStatuses: insts},
	}
}

// instanceStatusesOnIR re-reads the InferenceReplica and returns its persisted
// per-instance statuses, the authoritative copy assertions check. Returns nil
// when the IR does not exist.
func instanceStatusesOnIR(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) []v1beta1.OMENativeInstanceStatus {
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: irName(isvc, component)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		return nil
	}
	return ir.Status.InstanceStatuses
}

// minimalISVC builds an ISVC with the minimum metadata Create() needs.
// The fake-client status-write path needs a concrete owner object to
// round-trip; workload code reads the resulting ReconcileInput
// opaquely.
func minimalISVC(name, ns string, replicas int) *v1beta1.InferenceService {
	mr := replicas
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &mr,
				},
			},
		},
	}
}

// testRevisionHash is the synthetic revision hash stamped on test
// pods (matches what production Render emits via ome.io/revision-hash).
const testRevisionHash = "testrev1"

// testPodLabels reproduces the label set workload/ops.Render stamps
// on every emitted pod. Duplicated here because Render's helper is
// private to ops/; tests in ops_test fabricate pods directly.
func testPodLabels(isvc string, component workload.ComponentType, instanceIdx int32, runner string, incarnation int64, ordinal int32) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelInstanceIdx:                fmt.Sprintf("%d", instanceIdx),
		query.LabelInstanceIncarnation:        fmt.Sprintf("%d", incarnation),
		query.LabelRunner:                     runner,
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelPodOrdinal:                 fmt.Sprintf("%d", ordinal),
	}
}

// buildTestInput projects the ISVC under test onto a
// workload.ReconcileInput. MutateInstance round-trips through the
// fake client's Status().Update so tests can assert on the persisted
// ISVC.Status.
// testRequeueIntervals is the dispatcher cadence the ops tests run
// under, standing in for the operator config a deployed chart supplies.
// A test pinning the unconfigured path clears ReconcileInput.Requeue.
var testRequeueIntervals = workload.RequeueIntervals{
	Operation: 5 * time.Second,
	Gate:      3 * time.Second,
}

func buildTestInput(isvc *v1beta1.InferenceService, c client.Client, component workload.ComponentType) workload.ReconcileInput {
	// Delegate to the production converter rather than re-listing fields:
	// a field the reconciler reads but the helper forgets to copy is a
	// silently-passing test.
	instances := v1beta1convert.InstanceStatusSliceToWorkload(instanceStatusesOnIR(c, isvc, component))
	return workload.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workload.Key{
			Namespace:   isvc.Namespace,
			OwnerName:   isvc.Name,
			Component:   workload.ComponentType(component),
			OwnerLabels: isvc.Labels,
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: instances,
		},
		MutateInstance: testMutateInstance(c, isvc, component),
		Requeue:        testRequeueIntervals,
	}
}

// testMutateInstance is the test-side persistence layer for
// ReconcileInput.MutateInstance. Skips retry.RetryOnConflict (fake
// client has no optimistic-concurrency failures to retry).
func testMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: irName(isvc, component)}
		create := false
		if err := c.Get(ctx, key, ir); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("get IR: %w", err)
			}
			ir = &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
			create = true
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		var w workload.InstanceStatus
		if pos == -1 {
			w = workload.InstanceStatus{Index: idx}
		} else {
			s := ir.Status.InstanceStatuses[pos]
			w = workload.InstanceStatus{
				Index:           s.Index,
				Incarnation:     s.Incarnation,
				Phase:           workload.InstancePhase(s.Phase),
				RunningRevision: s.RunningRevision,
				TargetRevision:  s.TargetRevision,
				ActiveOrdinal:   s.ActiveOrdinal,
				Operation:       fromV1beta1Op(s.Operation),
				LastFailure:     fromV1beta1Termination(s.LastFailure),
			}
		}
		if !mutate(&w) {
			return nil
		}
		updated := v1beta1.OMENativeInstanceStatus{
			Index:           w.Index,
			Incarnation:     w.Incarnation,
			Phase:           v1beta1.OMENativeInstancePhase(w.Phase),
			RunningRevision: w.RunningRevision,
			TargetRevision:  w.TargetRevision,
			ActiveOrdinal:   w.ActiveOrdinal,
			Operation:       toV1beta1Op(w.Operation),
			LastFailure:     toV1beta1Termination(w.LastFailure),
		}
		if pos == -1 {
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, updated)
		} else {
			ir.Status.InstanceStatuses[pos] = updated
		}
		if create {
			bare := &v1beta1.InferenceReplica{ObjectMeta: ir.ObjectMeta}
			if err := c.Create(ctx, bare); err != nil {
				return fmt.Errorf("create IR: %w", err)
			}
			bare.Status = ir.Status
			ir = bare
		}
		return c.Status().Update(ctx, ir)
	}
}

// fromV1beta1Op / toV1beta1Op delegate to the production converters for
// the reason buildTestInput does: a field the reconciler writes but a
// hand-listed helper forgets to copy is a silently-passing test.
func fromV1beta1Op(op *v1beta1.InstanceOperation) *workload.InstanceOperation {
	return v1beta1convert.InstanceOperationToWorkload(op)
}

func toV1beta1Op(op *workload.InstanceOperation) *v1beta1.InstanceOperation {
	return v1beta1convert.InstanceOperationFromWorkload(op)
}

func fromV1beta1Termination(t *v1beta1.InstanceTermination) *workload.InstanceTermination {
	if t == nil {
		return nil
	}
	out := &workload.InstanceTermination{
		PodName:       t.PodName,
		ContainerName: t.ContainerName,
		Reason:        t.Reason,
		Message:       t.Message,
		Time:          t.Time,
	}
	if t.ExitCode != nil {
		e := *t.ExitCode
		out.ExitCode = &e
	}
	return out
}

func toV1beta1Termination(t *workload.InstanceTermination) *v1beta1.InstanceTermination {
	if t == nil {
		return nil
	}
	out := &v1beta1.InstanceTermination{
		PodName:       t.PodName,
		ContainerName: t.ContainerName,
		Reason:        t.Reason,
		Message:       t.Message,
		Time:          t.Time,
	}
	if t.ExitCode != nil {
		e := *t.ExitCode
		out.ExitCode = &e
	}
	return out
}

// findInstanceStatusOnIR looks up the InstanceStatus by (component, idx) on the
// authoritative InferenceReplica (the ISVC carries no per-instance detail).
// Returns nil when the IR or the instance is absent.
func findInstanceStatusOnIR(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType, idx int32) *v1beta1.OMENativeInstanceStatus {
	if isvc == nil {
		return nil
	}
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: irName(isvc, component)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		return nil
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == idx {
			return &ir.Status.InstanceStatuses[i]
		}
	}
	return nil
}

// buildPlanSinglePodEngine builds the workload.ComponentPlan a
// single-pod engine reconcile uses. Replicas drives the count.
func buildPlanSinglePodEngine(replicas int32) workload.ComponentPlan {
	instances := make([]workload.InstancePlan, replicas)
	for i := int32(0); i < replicas; i++ {
		instances[i] = workload.InstancePlan{
			Index:       i,
			Incarnation: 1,
			Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
		}
	}
	return workload.ComponentPlan{
		Component:            workload.ComponentEngine,
		Replicas:             replicas,
		Instances:            instances,
		InstanceReadyTimeout: 30 * time.Minute,
	}
}

// resetExpectations re-seats the expectations singleton so back-to-
// back tests don't observe prior ExpectCreates entries.
func resetExpectations(t *testing.T) {
	t.Helper()
	workload.DefaultExpectations = workload.NewExpectations()
}

// promotablePodForInstance is podForInstance in the state the shared promote
// bar accepts: ContainersReady, the serving gate satisfied, and kubelet's
// PodReady folded from that gate — the state a pass that wrote the gate
// leaves behind for the next one.
func promotablePodForInstance(isvc *v1beta1.InferenceService, instanceIdx int32) *corev1.Pod {
	pod := podForInstance(isvc, instanceIdx, true /* ready */, true /* serving */)
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.PodReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Now()),
	})
	return pod
}

// podForInstance fabricates a pod matching what Render() would
// produce for the given (ISVC, instance) pair. The `ready` knob
// synthesizes ContainersReady=True, not PodReady=True — PodReady
// requires every readiness gate (including ome.io/serving), which is
// exactly what the controller is about to write.
func podForInstance(isvc *v1beta1.InferenceService, instanceIdx int32, ready, serving bool) *corev1.Pod {
	labels := testPodLabels(isvc.Name, workload.ComponentEngine, instanceIdx, "default", 1, 0)
	labels[query.LabelRevisionHash] = testRevisionHash
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, instanceIdx, "default", 0),
			Namespace: isvc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
	}
	now := metav1.NewTime(time.Now())
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.ContainersReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               query.ServingConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	return pod
}

func TestCreate_NilClient(t *testing.T) {
	resetExpectations(t)
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}
	_, err := ops.Create(context.Background(), workload.Deps{}, workload.ReconcileInput{}, plan, nil)
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

// TestCreate_UnconfiguredCadenceUsesBackoff pins the unset path: with
// no operator cadence a pass that is still converging asks for the
// controller's rate-limited backoff instead of an interval the binary
// invented, and an explicit wait — here a promote window still open —
// supersedes that backoff.
func TestCreate_UnconfiguredCadenceUsesBackoff(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.Requeue = workload.RequeueIntervals{}
	plan := buildPlanSinglePodEngine(2)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter: got %v want 0 (no configured cadence to wait)", result.RequeueAfter)
	}
	if !result.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
		t.Fatalf("a converging pass with no cadence must ride the rate-limited backoff; got %+v", result)
	}

	// An explicit wait wins over the bare backoff.
	resetExpectations(t)
	explicit := buildTestInput(isvc, c, workload.ComponentEngine)
	explicit.Requeue = workload.RequeueIntervals{}
	explicit.PromoteWindow = &workload.PromoteWindow{}
	explicit.PromoteWindow.Observe(90 * time.Second)
	result, err = ops.Create(context.Background(), workload.Deps{Client: c}, explicit, plan, nil)
	if err != nil {
		t.Fatalf("Create with a pending promote window: %v", err)
	}
	if result.RequeueAfter != 90*time.Second {
		t.Fatalf("RequeueAfter: got %v want 90s (the explicit wait)", result.RequeueAfter)
	}
	if result.Requeue { //nolint:staticcheck // an explicit wait must not also set the bare flag
		t.Errorf("an explicit wait must supersede the bare backoff; got %+v", result)
	}
}

func TestCreate_EmptyClusterCreatesPodsAndRequeues(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(2)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected Requeue, got %+v", result)
	}

	// Assert two pods exist with the expected stable names.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("expected 2 pods, got %d", len(pods.Items))
	}
	want := map[string]bool{
		"llama-70b-engine-0-default-0": true,
		"llama-70b-engine-1-default-0": true,
	}
	for _, pod := range pods.Items {
		if !want[pod.Name] {
			t.Errorf("unexpected pod: %s", pod.Name)
		}
		delete(want, pod.Name)
		// Initial create stamps Incarnation=1 on every pod.
		if got := pod.Labels[query.LabelInstanceIncarnation]; got != "1" {
			t.Errorf("pod %s %s label: got %q want 1", pod.Name, query.LabelInstanceIncarnation, got)
		}
	}
	if len(want) > 0 {
		t.Errorf("missing pods: %v", want)
	}

	// InstanceStatus for each Instance should be Creating with an Operation.
	insts := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)
	if len(insts) != 2 {
		t.Fatalf("InstanceStatuses: got %d want 2", len(insts))
	}
	for _, s := range insts {
		if s.Phase != v1beta1.OMENativeInstanceCreating {
			t.Errorf("instance %d Phase: got %q want Creating", s.Index, s.Phase)
		}
		if s.Incarnation != 1 {
			t.Errorf("instance %d Incarnation: got %d want 1", s.Index, s.Incarnation)
		}
		if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationCreate {
			t.Errorf("instance %d Operation: %+v", s.Index, s.Operation)
			continue
		}
		// Deadline must be a strictly future time — proves InstanceReadyTimeout
		// is being threaded through to the status anchor and isn't left zero.
		if !s.Operation.Deadline.After(s.Operation.StartedAt.Time) {
			t.Errorf("instance %d Operation.Deadline: got %v, want > StartedAt %v",
				s.Index, s.Operation.Deadline, s.Operation.StartedAt)
		}
	}
}

func TestCreate_AllPodsExistButNotReady_HoldsCreating(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	pod := podForInstance(isvc, 0, false /* ready */, false /* serving */)
	c := newFakeClient(t, isvc, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected Requeue while waiting for Ready")
	}
	// Pod should not have been re-created (still 1).
	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	if len(pods.Items) != 1 {
		t.Errorf("pods: got %d want 1", len(pods.Items))
	}
}

// TestCreate_AllPodsReady_FlipsServingAndMarksReady: ContainersReady buys the
// serving-gate write and nothing more — the Ready stamp waits for the pass
// that observes kubelet folding that gate into PodReady.
func TestCreate_AllPodsReady_FlipsServingAndMarksReady(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	pod := podForInstance(isvc, 0, true /* ready */, false /* serving */)
	c := newFakeClient(t, isvc, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected a requeue while the pod is not PodReady yet, got %+v", result)
	}
	if insts := instanceStatusesOnIR(c, isvc, workload.ComponentEngine); len(insts) != 0 {
		t.Errorf("Ready stamped before the pod became PodReady: %+v", insts)
	}

	// Pod should have ome.io/serving=True now.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if !podreadiness.IsServing(got) {
		t.Errorf("pod %s ome.io/serving not flipped: conditions=%+v", got.Name, got.Status.Conditions)
	}

	makeNewPodReady(t, c, isvc.Namespace, pod.Name, 1)
	result, err = ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create after PodReady: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no Requeue once the pod cleared the promote bar, got %+v", result)
	}

	// InstanceStatus should be Ready with Operation=nil.
	insts := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)
	if len(insts) != 1 {
		t.Fatalf("status missing: %+v", insts)
	}
	is := insts[0]
	if is.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", is.Phase)
	}
	if is.Operation != nil {
		t.Errorf("Operation: want nil, got %+v", is.Operation)
	}
}

func TestCreate_PartialPods_CreatesOnlyMissing(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	// Instance 0 already exists; Instance 1 missing.
	existing := podForInstance(isvc, 0, false, false)
	c := newFakeClient(t, isvc, existing)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(2)

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	if len(pods.Items) != 2 {
		t.Fatalf("expected 2 pods, got %d", len(pods.Items))
	}
}

// A corrective edit can advance the live target while an initial gang Create
// is only partially materialized. The surviving pod still belongs to the
// pinned Create attempt, so backfilling the missing member from the new target
// would produce a mixed-revision gang that can never converge as one attempt.
func TestCreate_CorrectiveEditDuringPartialGangDoesNotMixRevisions(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	const priorRevision = "llama-70b-engine-bad0bad0"
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-good0bad", Namespace: "prod"},
	}
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		PodCount:    1,
		Operation: &v1beta1.InstanceOperation{
			ID:             "create-0-1",
			Type:           v1beta1.InstanceOperationCreate,
			Step:           "CreatePods",
			TargetRevision: priorRevision,
		},
	})
	leader := gangPod(isvc, 0, "leader", 0, 1, false, false)
	leader.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(priorRevision)
	c := newFakeClient(t, isvc, ir, leader)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyNone)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("superseded Create retirement must requeue immediately: %+v", result)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("superseded partial Create must not mix revisions: got %d pods, want the one prior-revision survivor", len(pods.Items))
	}
	requireRetiredIntoFreshAttempt(t, findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0),
		"create-0-1", target.Name)
}

func TestCreate_CorrectiveEditDoesNotAdoptUnpinnedPartialGang(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	const priorRevision = "llama-70b-engine-bad0bad0"
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-good0bad", Namespace: "prod"},
	}
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		PodCount:    1,
		Operation: &v1beta1.InstanceOperation{
			ID:   "create-0-1",
			Type: v1beta1.InstanceOperationCreate,
			Step: "CreatePods",
		},
	})
	leader := gangPod(isvc, 0, "leader", 0, 1, false, false)
	leader.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(priorRevision)
	c := newFakeClient(t, isvc, ir, leader)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyNone)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("superseded Create retirement must requeue immediately: %+v", result)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("unpinned partial Create must not mix revisions: got %d pods, want the one prior-revision survivor", len(pods.Items))
	}
	requireRetiredIntoFreshAttempt(t, findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0),
		"create-0-1", target.Name)
}

func TestCreate_CorrectiveEditRetiresFullyMaterializedGatedGang(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	const priorRevision = "llama-70b-engine-bad0bad0"
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-good0bad", Namespace: "prod"},
	}
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			ID:             "create-0-1",
			Type:           v1beta1.InstanceOperationCreate,
			Step:           "CreatePods",
			TargetRevision: priorRevision,
		},
	})
	leader := gangPod(isvc, 0, "leader", 0, 1, false, false)
	worker := gangPod(isvc, 0, "worker", 0, 1, false, false)
	leader.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(priorRevision)
	worker.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(priorRevision)
	leader.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.com/admission"}}
	worker.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.com/admission"}}
	c := newFakeClient(t, isvc, ir, leader, worker)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyNone)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("superseded gated Create must requeue immediately: %+v", result)
	}
	requireRetiredIntoFreshAttempt(t, findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0),
		"create-0-1", target.Name)
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("retirement must not mutate the live pod set: got %d pods want 2", len(pods.Items))
	}
}

func TestCreate_AdoptsUnpinnedAttemptWithoutPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-good0bad", Namespace: "prod"},
	}
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			ID:   "create-0-1",
			Type: v1beta1.InstanceOperationCreate,
			Step: "CreatePods",
		},
	})
	c := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyNone)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, target); err != nil {
		t.Fatalf("Create: %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Operation == nil || s.Operation.TargetRevision != target.Name {
		t.Fatalf("safe persisted attempt must be pinned to %q; got %+v", target.Name, s)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("adopted Create must materialize the gang: got %d pods, want 2", len(pods.Items))
	}
}

func TestCreate_IdempotentOnAlreadyExists(t *testing.T) {
	resetExpectations(t)
	// Two reconciles back-to-back without the cache observing the first
	// batch — the second call's client.Create returns AlreadyExists,
	// which we treat as a no-op.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	// First call creates the pod.
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	// Reset expectations so the second call attempts to re-create.
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	// Second call should not error even though the pod already exists.
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("second Create: %v", err)
	}
}

// Promote to Ready must record RunningRevision so detectUpdateTrigger's
// fast path short-circuits on the next reconcile. Without it, every
// fresh Instance triggers a spurious recreate on its second pass (the
// per-pod diff false-positives against post-Render mutations).
//
// Guard: Create's promote only stamps RunningRevision=target.Name
// when the existing pods actually carry target's revision hash — see
// existingPodsMatchTargetRevision. The test pod is given a rev-hash
// label matching the target so the normal-flow promote fires.
func TestCreate_PromoteRecordsRunningRevision(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-deadbeef", Namespace: "prod"},
	}
	// Pre-seed a pod past the promote bar so reconcileInstance jumps
	// straight to the promote step. The pod's rev-hash matches target's
	// suffix — the production-normal case (createMissingPods stamps the
	// label from revisionHashFromTarget(target)).
	pod := promotablePodForInstance(isvc, 0)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	c := newFakeClient(t, isvc, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, target); err != nil {
		t.Fatalf("Create: %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil {
		t.Fatal("InstanceStatus[0] must exist after promote")
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	if s.RunningRevision != target.Name {
		t.Errorf("RunningRevision: got %q want %q (Create promote must record target hash)",
			s.RunningRevision, target.Name)
	}
}

// TestCreate_PromoteSkipsRunningRevisionForOffTargetPods pins the
// bump-during-bump rule in Create.reconcileInstance:
// when existing pods are runtime-ready but carry a different revision
// hash from target (e.g., they were created by an earlier surge cycle
// pinned to a now-superseded target), the promote MUST NOT stamp
// RunningRevision=target.Name — that would falsely advertise the pods
// as on target. status.StampReady (no RunningRevision write)
// preserves whatever revision the prior op recorded, so the next
// reconcile's detectUpdateTrigger fires a fresh surge cycle to roll
// the pods to target.
func TestCreate_PromoteSkipsRunningRevisionForOffTargetPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	// Seed status: Instance is in a transient Phase (e.g., Creating
	// after an earlier surge promote) with the prior revision recorded
	// as RunningRevision. The Create promote should NOT clobber this.
	priorRev := "llama-70b-engine-priorrev"
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceCreating,
		RunningRevision: priorRev,
	})
	// Pre-seed a pod past the promote bar, labeled with the PRIOR
	// revision's hash (not target's), so reconcileInstance finds it and
	// reaches the promote step.
	pod := promotablePodForInstance(isvc, 0)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(priorRev)
	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, target); err != nil {
		t.Fatalf("Create: %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil {
		t.Fatal("InstanceStatus[0] must exist after promote")
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	// The load-bearing assertion: RunningRevision must NOT have been
	// updated to target.Name — the pod is genuinely on priorRev.
	if s.RunningRevision != priorRev {
		t.Errorf("RunningRevision: got %q want %q (Create promote must NOT clobber RunningRevision when pods are off-target)",
			s.RunningRevision, priorRev)
	}
}

// Inverse: when target is nil (scale-down-only reconciles), Create
// promotes without writing RunningRevision.
func TestCreate_PromoteWithNilTargetSkipsRunningRevision(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	pod := promotablePodForInstance(isvc, 0)
	c := newFakeClient(t, isvc, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("expected Phase=Ready; got %+v", s)
	}
	if s.RunningRevision != "" {
		t.Errorf("RunningRevision: got %q want empty (nil target)", s.RunningRevision)
	}
}

// Gang RecreateInstance race.
//
// A pod loss in an already-Ready Instance can be recovered two ways: the
// Restart pass (whole-Instance drain+recreate — the RecreateInstance
// contract) and the Create pass (backfill the missing pod by name). These
// race. Were Create to win it would stamp Phase=Creating, which latches
// the outcome by making DetectRestartTrigger skip on every later pass — so
// RecreateInstance would silently degrade to "recreate just the dead pod"
// (the leader and surviving workers keep their incarnation).
//
// Create therefore defers partial-Instance recovery to Restart under
// RecreateInstance, so Restart deterministically owns whole-gang recreate.

// gangPod fabricates a leader/worker gang pod (podForInstance only emits
// the single-pod "default" runner).
func gangPod(isvc *v1beta1.InferenceService, idx int32, runner string, ordinal int32, incarnation int64, ready, serving bool) *corev1.Pod {
	labels := testPodLabels(isvc.Name, workload.ComponentEngine, idx, runner, incarnation, ordinal)
	labels[query.LabelRevisionHash] = testRevisionHash
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, runner, ordinal),
			Namespace: isvc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
	}
	now := metav1.NewTime(time.Now())
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: now,
		})
	}
	return pod
}

// buildPlanGangEngine builds a multi-pod (leader+worker) engine plan with
// the given restart policy. Instance 0 sits at incarnation 2 — an
// established Instance that has already been Ready.
func buildPlanGangEngine(restart workload.RestartPolicy) workload.ComponentPlan {
	return workload.ComponentPlan{
		Component:            workload.ComponentEngine,
		Replicas:             1,
		RestartPolicy:        restart,
		InstanceReadyTimeout: 30 * time.Minute,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 2,
			Runners:     []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		}},
	}
}

// seedReadyInstance stamps the ISVC status with one already-Ready engine
// Instance at the given index/incarnation.
func seedReadyInstance(isvc *v1beta1.InferenceService, idx int32, incarnation int64) *v1beta1.InferenceReplica {
	return instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       idx,
		Incarnation: incarnation,
		Phase:       v1beta1.OMENativeInstanceReady,
	})
}

// An already-Ready gang that loses a pod must NOT be self-healed
// pod-by-pod by Create under RecreateInstance — that races Restart's
// whole-gang recreate and degrades the policy. Create must defer.
func TestCreate_RecreateInstance_DefersPartialGangToRestart(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := seedReadyInstance(isvc, 0, 2)
	// Established gang lost its worker: leader present, worker gone.
	leader := gangPod(isvc, 0, "leader", 0, 2, true /* ready */, true /* serving */)
	c := newFakeClient(t, isvc, ir, leader)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// The missing worker MUST NOT be backfilled — Restart owns whole-gang recreate.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("Create must defer partial-gang recovery to Restart under RecreateInstance: got %d pod(s), want 1 (leader only)", len(pods.Items))
	}
	// Phase MUST stay Ready — stamping Creating here is what latches the race.
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("Phase must remain Ready (Create must not stamp Creating): got %+v", s)
	}
}

// Contrast: under a non-RecreateInstance policy, Create still self-heals a
// missing pod — Restart isn't responsible for recovery there.
func TestCreate_NonRecreatePolicy_SelfHealsMissingPod(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := seedReadyInstance(isvc, 0, 2)
	leader := gangPod(isvc, 0, "leader", 0, 2, true, true)
	c := newFakeClient(t, isvc, ir, leader)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyNone)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("non-RecreateInstance policy must self-heal the missing pod: got %d pod(s), want 2", len(pods.Items))
	}
}

// The guard must not block legitimate fresh creation: an Instance that has
// never been Ready (no status) under RecreateInstance is still
// materialized by Create — otherwise initial bring-up deadlocks (Create
// defers, Restart can't fire on a never-Ready Instance).
func TestCreate_RecreateInstance_StillMaterializesFreshInstance(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	// No instance status seeded → fresh, never Ready.
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("RecreateInstance must still materialize a fresh gang Instance: got %d pod(s), want 2 (leader+worker)", len(pods.Items))
	}
}

// seedMaterializedGang stamps a Create-owned gang Instance that was
// already observed complete (PodCount == 2) at the given phase — the
// production shape a drained node leaves behind.
func seedMaterializedGang(isvc *v1beta1.InferenceService, phase v1beta1.OMENativeInstancePhase, podCount int32) *v1beta1.InferenceReplica {
	return instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     2,
		Phase:           phase,
		PodCount:        podCount,
		RunningRevision: "llama-70b-engine-" + testRevisionHash,
		TargetRevision:  "llama-70b-engine-" + testRevisionHash,
		Operation: &v1beta1.InstanceOperation{
			ID: "create-0-1", Type: v1beta1.InstanceOperationCreate, Step: "CreatePods",
		},
	})
}

// A gang that lost a member while below Ready must reach Restart, not
// Create's per-pod backfill: backfilling leaves the survivor at the old
// incarnation, holding the domain the replacement needs.
func TestCreate_RecreateInstance_DefersNonReadyPartialGang(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := seedMaterializedGang(isvc, v1beta1.OMENativeInstanceCreating, 2)
	leader := gangPod(isvc, 0, "leader", 0, 2, false /* ready */, false /* serving */)
	c := newFakeClient(t, isvc, ir, leader)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("Create must defer a non-Ready partial gang to Restart: got %d pod(s), want 1 (leader only)", len(pods.Items))
	}
}

// Once CreatePods is committed, an interrupted gang is repaired by Restart
// as one unit. Create must not backfill the missing member at the old
// incarnation even when the latest published PodCount reflects only the
// survivor.
func TestCreate_RecreateInstance_DefersInterruptedGangMaterialization(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := seedMaterializedGang(isvc, v1beta1.OMENativeInstanceCreating, 1)
	leader := gangPod(isvc, 0, "leader", 0, 2, false /* ready */, false /* serving */)
	c := newFakeClient(t, isvc, ir, leader)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("Create must defer an interrupted gang to Restart: got %d pod(s), want 1", len(pods.Items))
	}
}

// seedInstanceStatuses stamps the ISVC status with the given engine
// InstanceStatuses verbatim — lets a test set per-index Phase to model
// a mid-rollout snapshot.
func seedInstanceStatuses(isvc *v1beta1.InferenceService, statuses ...v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	return instanceIR(isvc, workload.ComponentEngine, statuses...)
}

// CreateFreshIndices must materialize brand-new (surge-free) indices
// while leaving an index mid-update untouched: with index 0 Updating and
// indices 1,2 absent, it creates pods for 1 and 2 only and never
// duplicates index 0's in-flight pod.
func TestCreateFreshIndices_CreatesAbsentSkipsUpdating(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 3)
	// Index 0 is mid-surge (Phase=Updating); indices 1,2 have no status.
	ir := seedInstanceStatuses(isvc, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "llama-70b-engine-priorrev",
	})
	// Index 0's existing in-flight pod — present so we can assert it is
	// neither touched nor duplicated.
	pod0 := podForInstance(isvc, 0, true /* ready */, true /* serving */)
	c := newFakeClient(t, isvc, ir, pod0)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(3)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	if _, err := ops.CreateFreshIndices(context.Background(), workload.Deps{Client: c}, input, plan, target); err != nil {
		t.Fatalf("CreateFreshIndices: %v", err)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	got := map[string]bool{}
	for _, p := range pods.Items {
		got[p.Name] = true
	}
	// Index 0: exactly the original pod, no duplicate at any ordinal.
	if !got["llama-70b-engine-0-default-0"] {
		t.Errorf("index 0's in-flight pod must remain; got %v", got)
	}
	if got["llama-70b-engine-0-default-1"] {
		t.Errorf("index 0 must NOT be duplicated at ordinal 1; got %v", got)
	}
	// Indices 1,2: fresh pods created.
	if !got["llama-70b-engine-1-default-0"] {
		t.Errorf("fresh index 1 pod must be created; got %v", got)
	}
	if !got["llama-70b-engine-2-default-0"] {
		t.Errorf("fresh index 2 pod must be created; got %v", got)
	}
	if len(pods.Items) != 3 {
		t.Fatalf("expected 3 pods (index 0 in-flight + fresh 1,2), got %d: %v", len(pods.Items), got)
	}

	// Index 0's status MUST be left untouched at Phase=Updating —
	// CreateFreshIndices never reconciles it.
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("index 0 Phase must stay Updating (not reconciled by fresh pass); got %+v", s)
	}
	for _, idx := range []int32{1, 2} {
		s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, idx)
		if s == nil || s.Operation == nil {
			t.Errorf("fresh index %d must have a Create operation; got %+v", idx, s)
			continue
		}
		if s.Operation.TargetRevision != target.Name {
			t.Errorf("fresh index %d Create TargetRevision: got %q want %q",
				idx, s.Operation.TargetRevision, target.Name)
		}
	}
}

// A scale-up never disturbs a Ready Instance: the added replicas are
// brand-new indices created alongside it, and the Ready row keeps its
// phase, its incarnation and its pod.
func TestCreateFreshIndices_ReadyIndexUntouchedByScaleUp(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	ir := seedInstanceStatuses(isvc, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: "llama-70b-engine-currentrev",
	})
	pod0 := podForInstance(isvc, 0, true /* ready */, true /* serving */)
	c := newFakeClient(t, isvc, ir, pod0)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(2)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-currentrev", Namespace: "prod"},
	}

	if _, err := ops.CreateFreshIndices(context.Background(), workload.Deps{Client: c}, input, plan, target); err != nil {
		t.Fatalf("CreateFreshIndices: %v", err)
	}

	before := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if before == nil || before.Phase != v1beta1.OMENativeInstanceReady ||
		before.Incarnation != 3 || before.Operation != nil {
		t.Fatalf("the Ready index must be left exactly as it was, got %+v", before)
	}
	fresh := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 1)
	if fresh == nil || fresh.Operation == nil || fresh.Operation.Type != v1beta1.InstanceOperationCreate {
		t.Fatalf("the added index must open its own Create, got %+v", fresh)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	got := map[string]bool{}
	for _, p := range pods.Items {
		got[p.Name] = true
	}
	if len(pods.Items) != 2 || !got[pod0.Name] || !got["llama-70b-engine-1-default-0"] {
		t.Fatalf("expected the Ready pod plus one fresh pod, got %v", got)
	}
}

// CreateFreshIndices must be a no-op when the only Instance is mid-update
// — pure rollouts must not trigger any create work.
func TestCreateFreshIndices_NoOpWhenOnlyIndexUpdating(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := seedInstanceStatuses(isvc, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "llama-70b-engine-priorrev",
	})
	pod0 := podForInstance(isvc, 0, true, true)
	c := newFakeClient(t, isvc, ir, pod0)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	res, err := ops.CreateFreshIndices(context.Background(), workload.Deps{Client: c}, input, plan, target)
	if err != nil {
		t.Fatalf("CreateFreshIndices: %v", err)
	}
	// No qualifying index → quick no-op result (no requeue scheduled).
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected no-op result for pure rollout, got %+v", res)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("CreateFreshIndices must not create/duplicate anything for a pure rollout; got %d pods", len(pods.Items))
	}
}

// CreateFreshIndices must not treat a surge TARGET marker as a
// surge-free index: the replacement-gang slot is Phase=Creating but
// carries the owning op (Update for a gang surge, Migrate for a
// migration surge). Overwriting the marker with a Create stamp unpins
// the index from the plan and scale-down churn-deletes the in-flight
// replacement. With index 1 absent (genuine scale-up) and index 2 a
// marker, only index 1 may be reconciled.
func TestCreateFreshIndices_PreservesSurgeTargetMarker(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opType workload.InstanceOperationType
		step   string
	}{
		{name: "gang surge target", opType: workload.InstanceOperationUpdate, step: workload.UpdateStepGangSurgeTarget},
		{name: "migration surge target", opType: workload.InstanceOperationMigrate, step: "CreateSurge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetExpectations(t)
			isvc := minimalISVC("llama-70b", "prod", 3)
			markerOp := &v1beta1.InstanceOperation{
				ID:   "op-marker",
				Type: v1beta1.InstanceOperationType(tc.opType),
				Step: tc.step,
			}
			ir := seedInstanceStatuses(isvc,
				v1beta1.OMENativeInstanceStatus{
					Index: 0, Incarnation: 1,
					Phase:           v1beta1.OMENativeInstanceUpdating,
					RunningRevision: "llama-70b-engine-priorrev",
				},
				v1beta1.OMENativeInstanceStatus{
					Index: 2, Incarnation: 1,
					Phase:     v1beta1.OMENativeInstanceCreating,
					Operation: markerOp,
				},
			)
			pod0 := podForInstance(isvc, 0, true, true)
			c := newFakeClient(t, isvc, ir, pod0)
			input := buildTestInput(isvc, c, workload.ComponentEngine)
			// buildTestInput doesn't project Operation; the predicate under
			// test reads it from ObservedState, so mirror the marker there.
			for i := range input.ObservedState.InstanceStatuses {
				if input.ObservedState.InstanceStatuses[i].Index == 2 {
					input.ObservedState.InstanceStatuses[i].Operation = &workload.InstanceOperation{
						ID: "op-marker", Type: tc.opType, Step: tc.step,
					}
				}
			}
			plan := buildPlanSinglePodEngine(3)
			target := &appsv1.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
			}

			if _, err := ops.CreateFreshIndices(context.Background(), workload.Deps{Client: c}, input, plan, target); err != nil {
				t.Fatalf("CreateFreshIndices: %v", err)
			}

			pods := &corev1.PodList{}
			if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			got := map[string]bool{}
			for _, p := range pods.Items {
				got[p.Name] = true
			}
			if !got["llama-70b-engine-1-default-0"] {
				t.Errorf("fresh index 1 pod must be created; got %v", got)
			}
			if got["llama-70b-engine-2-default-0"] {
				t.Errorf("marker index 2 must not be materialized by the fresh pass; got %v", got)
			}
			s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 2)
			if s == nil || s.Operation == nil ||
				s.Operation.Type != v1beta1.InstanceOperationType(tc.opType) ||
				s.Operation.Step != tc.step {
				t.Errorf("surge target marker must be preserved; got %+v", s)
			}
		})
	}
}

// TestCreate_PromotionWaitsOutTheWindowAndRequeuesAtItsRemainder: a set that
// is PodReady but still inside the Component's minReadySeconds window is not
// promoted, and the pass requeues at what is left of the window — computed
// from the Ready transition, not from the create poll interval. Once the
// window elapses the promote still comes from the pass that re-checks the
// whole bar, not from the timer.
func TestCreate_PromotionWaitsOutTheWindowAndRequeuesAtItsRemainder(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	pod := promotablePodForInstance(isvc, 0)
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(start.Add(-5 * time.Second))
		}
	}
	c := newFakeClient(t, isvc, pod)
	clk := clocktesting.NewFakeClock(start)
	plan := buildPlanSinglePodEngine(1)
	plan.MinReadySeconds = 20

	create := func() ctrl.Result {
		t.Helper()
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		input.Clock = clk
		// The dispatcher owns one sink per pass; wire one so the remainder
		// this pass deposits reaches its own requeue.
		input.PromoteWindow = &workload.PromoteWindow{}
		result, err := ops.Create(context.Background(), workload.Deps{Client: c, Clock: clk}, input, plan, nil)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		return result
	}

	result := create()
	if got, want := result.RequeueAfter, 15*time.Second; got != want {
		t.Fatalf("requeue inside the window: got %s, want the window remainder %s", got, want)
	}
	if insts := instanceStatusesOnIR(c, isvc, workload.ComponentEngine); len(insts) != 0 {
		t.Fatalf("promoted inside the window: %+v", insts)
	}

	clk.SetTime(start.Add(15 * time.Second))
	result = create()
	if result.RequeueAfter != 0 {
		t.Fatalf("requeue after the window elapsed: got %s, want none", result.RequeueAfter)
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("promote after the window: got %+v, want Phase=Ready", s)
	}
}

type batchMutationRecorder struct {
	statuses map[int32]workload.InstanceStatus
	batches  [][]int32
	events   []string
	fail     error
}

func newBatchMutationRecorder(observed []workload.InstanceStatus) *batchMutationRecorder {
	r := &batchMutationRecorder{statuses: make(map[int32]workload.InstanceStatus, len(observed))}
	for _, status := range observed {
		r.statuses[status.Index] = status
	}
	return r
}

func (r *batchMutationRecorder) apply(_ context.Context, mutations []workload.InstanceMutation) error {
	if len(mutations) == 0 {
		return nil
	}
	indices := make([]int32, 0, len(mutations))
	for _, mutation := range mutations {
		indices = append(indices, mutation.Index)
	}
	r.batches = append(r.batches, indices)
	r.events = append(r.events, fmt.Sprintf("status:%v", indices))
	if r.fail != nil {
		return r.fail
	}
	type committedMutation struct {
		onCommit func(previous, current *workload.InstanceStatus)
		previous *workload.InstanceStatus
		current  *workload.InstanceStatus
	}
	committed := make([]committedMutation, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation.Remove {
			status, found := r.statuses[mutation.Index]
			if !found || (mutation.Precondition != nil && !mutation.Precondition(&status)) {
				continue
			}
			delete(r.statuses, mutation.Index)
			if mutation.OnCommit != nil {
				previous := status
				committed = append(committed, committedMutation{onCommit: mutation.OnCommit, previous: &previous})
			}
			continue
		}
		status, ok := r.statuses[mutation.Index]
		var previous *workload.InstanceStatus
		if !ok {
			status = workload.InstanceStatus{Index: mutation.Index}
		} else {
			copy := status
			previous = &copy
		}
		if mutation.Precondition != nil && !mutation.Precondition(&status) {
			continue
		}
		if mutation.Mutate(&status) {
			r.statuses[mutation.Index] = status
			if mutation.OnCommit != nil {
				current := status
				committed = append(committed, committedMutation{onCommit: mutation.OnCommit, previous: previous, current: &current})
			}
		}
	}
	for _, mutation := range committed {
		mutation.onCommit(mutation.previous, mutation.current)
	}
	return nil
}

type podCreateObserver struct {
	client.Client
	beforeCreate func(*corev1.Pod) error
}

func (c *podCreateObserver) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && c.beforeCreate != nil {
		if err := c.beforeCreate(pod); err != nil {
			return err
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

type failingPodStatusClient struct {
	client.Client
	podName string
	err     error
}

func (c *failingPodStatusClient) Status() client.SubResourceWriter {
	return &failingPodStatusWriter{
		SubResourceWriter: c.Client.Status(),
		podName:           c.podName,
		err:               c.err,
	}
}

type failingPodStatusWriter struct {
	client.SubResourceWriter
	podName string
	err     error
}

func (w *failingPodStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && pod.Name == w.podName {
		return w.err
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

// newReadyBatchFixture seeds `replicas` mid-Create Instances whose pods all
// clear the shared promote bar, so one pass promotes the whole batch.
func newReadyBatchFixture(t *testing.T, name string, replicas int32) (*v1beta1.InferenceService, client.Client, workload.ReconcileInput) {
	t.Helper()
	return newBatchFixture(t, name, replicas, replicas)
}

// newBatchFixture is newReadyBatchFixture with the first `promotable`
// Instances past the promote bar and the rest only ContainersReady — the
// state a pass finds when it still owes those Instances their serving-gate
// write, which is the write the promote bar waits on.
func newBatchFixture(t *testing.T, name string, replicas, promotable int32) (*v1beta1.InferenceService, client.Client, workload.ReconcileInput) {
	t.Helper()
	isvc := minimalISVC(name, "prod", int(replicas))
	statuses := make([]v1beta1.OMENativeInstanceStatus, 0, replicas)
	objects := []client.Object{isvc}
	for idx := int32(0); idx < replicas; idx++ {
		statuses = append(statuses, v1beta1.OMENativeInstanceStatus{
			Index:       idx,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				Type:      v1beta1.InstanceOperationCreate,
				StartedAt: metav1.Now(),
			},
		})
		if idx < promotable {
			objects = append(objects, promotablePodForInstance(isvc, idx))
			continue
		}
		objects = append(objects, podForInstance(isvc, idx, true /* ready */, false /* serving */))
	}
	objects = append(objects, instanceIR(isvc, workload.ComponentEngine, statuses...))
	c := newFakeClient(t, objects...)
	return isvc, c, buildTestInput(isvc, c, workload.ComponentEngine)
}

func requireMutationPodsServing(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, mutations []workload.InstanceMutation) error {
	for _, mutation := range mutations {
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, mutation.Index, "default", 0),
		}
		if err := c.Get(ctx, key, pod); err != nil {
			return fmt.Errorf("get ready pod for instance %d: %w", mutation.Index, err)
		}
		if !podreadiness.IsServing(pod) {
			return fmt.Errorf("Ready status for instance %d was committed before its Pod became serving", mutation.Index)
		}
	}
	return nil
}

func TestCreate_BatchedFreshStartsHonorConfiguredCapAndCommitBeforePods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batched", "prod", 5)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	observedCreates := make([]int32, 0, podBatchSize)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return fmt.Errorf("parse instance index on pod %s: %w", pod.Name, err)
		}
		idx := int32(idx64)
		status, ok := recorder.statuses[idx]
		if !ok || status.Phase != workload.InstancePhaseCreating || status.Operation == nil ||
			status.Operation.Type != workload.InstanceOperationCreate || status.Operation.Step != "CreatePods" {
			return fmt.Errorf("pod %s created before its Creating intent was committed: %+v", pod.Name, status)
		}
		recorder.events = append(recorder.events, fmt.Sprintf("pod:%d", idx))
		observedCreates = append(observedCreates, idx)
		return nil
	}}

	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(5), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("a capped pass with deferred fresh Instances must requeue")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1}) {
		t.Fatalf("Creating batches: got %v, want one batch [0 1]", recorder.batches)
	}
	if !reflect.DeepEqual(observedCreates, []int32{0, 1}) {
		t.Fatalf("created Instance indices: got %v, want [0 1]", observedCreates)
	}
	wantEvents := []string{"status:[0 1]", "pod:0", "pod:1"}
	if !reflect.DeepEqual(recorder.events, wantEvents) {
		t.Fatalf("write-ahead order: got %v, want %v", recorder.events, wantEvents)
	}

	pods := &corev1.PodList{}
	if err := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != int(podBatchSize) {
		t.Fatalf("pods created in capped pass: got %d, want %d", len(pods.Items), podBatchSize)
	}
}

func TestCreate_BatchedTwoThousandReplicaScaleUpSelectsOneConfiguredWave(t *testing.T) {
	resetExpectations(t)
	const replicas int32 = 2001
	const waveSize int32 = 100
	isvc := minimalISVC("batch-large-scale", "prod", int(replicas))
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	configuredWaveSize := waveSize
	input.ScaleUpPodBatchSize = &configuredWaveSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	result, err := ops.Create(context.Background(), workload.Deps{Client: base}, input, buildPlanSinglePodEngine(replicas), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("a large capped scale-up must requeue the deferred replicas")
	}
	if len(recorder.batches) != 1 || len(recorder.batches[0]) != int(waveSize) {
		t.Fatalf("Creating batches: got lengths %v, want one batch of %d", recorder.batches, waveSize)
	}
	for idx, got := range recorder.batches[0] {
		if got != int32(idx) {
			t.Fatalf("Creating batch index %d: got %d, want stable prefix index %d", idx, got, idx)
		}
	}
	pods := &corev1.PodList{}
	if err := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list Pods: %v", err)
	}
	if len(pods.Items) != int(waveSize) {
		t.Fatalf("Pods created in first 2,001-replica wave: got %d, want %d", len(pods.Items), waveSize)
	}
	if len(recorder.statuses) != int(waveSize) {
		t.Fatalf("Creating statuses in first wave: got %d, want %d", len(recorder.statuses), waveSize)
	}
}

func TestCreate_BatchedNilPodBudgetIsUnbounded(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-unbounded", "prod", 5)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ScaleUpPodBatchSize = nil
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(5), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	wantBatch := []int32{0, 1, 2, 3, 4}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], wantBatch) {
		t.Fatalf("Creating batches with nil budget: got %v, want %v", recorder.batches, wantBatch)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 5 {
		t.Fatalf("pods created with nil budget: got %d, want 5", len(pods.Items))
	}
}

func TestCreate_BatchedPodBudgetExactFit(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-exact-fit", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(10)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(3)
	plan.Instances[1].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}
	createdByInstance := map[int32]int{}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		createdByInstance[int32(idx64)]++
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("exact-fit Creating batch: got %v, want [[0 1 2]]", recorder.batches)
	}
	wantCreates := map[int32]int{0: 1, 1: 8, 2: 1}
	if !reflect.DeepEqual(createdByInstance, wantCreates) {
		t.Fatalf("exact-fit pod creates: got %v, want %v", createdByInstance, wantCreates)
	}
}

func TestCreate_BatchedZeroPodBudgetBlocksOversizedGang(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-zero", "prod", 1)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(0)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(1)
	plan.Instances[0].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}
	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("zero Pod budget must requeue deferred work")
	}
	if len(recorder.batches) != 0 {
		t.Fatalf("Creating batches with zero budget: got %v, want none", recorder.batches)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods created with zero budget: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_BatchedCreatingPersistenceFailureCreatesNoPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-failure", "prod", 3)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	recorder.fail = errors.New("status unavailable")
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if err == nil {
		t.Fatal("Create succeeded despite a failed Creating status batch")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1}) {
		t.Fatalf("attempted Creating batches: got %v, want [[0 1]]", recorder.batches)
	}

	pods := &corev1.PodList{}
	if listErr := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list pods: %v", listErr)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pods created after failed write-ahead status: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_BatchedMissingStatusOwnerCreatesNoPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-owner-gone", "prod", 3)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = func(context.Context, []workload.InstanceMutation) error {
		t.Fatal("owner-aware atomic status capability was bypassed")
		return nil
	}
	atomicCalls := 0
	input.ApplyInstanceMutationsWithRetryBlock = func(_ context.Context, mutations []workload.InstanceMutation, revision string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		atomicCalls++
		if len(mutations) != 2 {
			t.Fatalf("atomic batch length: got %d, want 2", len(mutations))
		}
		if !reflect.DeepEqual([]int32{mutations[0].Index, mutations[1].Index}, []int32{0, 1}) {
			t.Fatalf("atomic batch indices: got [%d %d], want [0 1]", mutations[0].Index, mutations[1].Index)
		}
		if revision != "" || mutate != nil {
			t.Fatalf("nil-target atomic write: revision=%q mutate=%v, want empty and nil", revision, mutate != nil)
		}
		return workload.ErrStatusOwnerGone
	}

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create after owner deletion: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("Create result after owner deletion: got %+v, want zero", result)
	}
	if atomicCalls != 1 {
		t.Fatalf("atomic status calls: got %d, want 1", atomicCalls)
	}
	pods := &corev1.PodList{}
	if listErr := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list pods: %v", listErr)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pods created after the status owner disappeared: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_BatchedAtomicRetryBlockFailureCreatesNoStatusOrPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-retry-failure", "prod", 2)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-retry-failure-engine-deadbeef", Namespace: isvc.Namespace},
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: target.Name,
		State:          workload.RetryBlockBackoff,
	}}
	retryErr := errors.New("retry block unavailable")
	retryCalls := 0
	var retryDisposition workload.RetryBlockDisposition
	var attemptedBatch []int32
	input.ApplyInstanceMutationsWithRetryBlock = func(_ context.Context, mutations []workload.InstanceMutation, revision string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		retryCalls++
		for _, mutation := range mutations {
			attemptedBatch = append(attemptedBatch, mutation.Index)
		}
		block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, revision)
		if block == nil {
			return fmt.Errorf("missing RetryBlock for %s", revision)
		}
		copy := *block
		retryDisposition = mutate(&copy)
		return retryErr
	}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(2), target)
	if !errors.Is(err, retryErr) {
		t.Fatalf("Create error: got %v, want retry persistence failure", err)
	}
	if !reflect.DeepEqual(attemptedBatch, []int32{0, 1}) {
		t.Fatalf("attempted atomic batch: got %v, want [0 1]", attemptedBatch)
	}
	if len(recorder.batches) != 0 {
		t.Fatalf("separate InstanceStatus batches after atomic failure: got %v, want none", recorder.batches)
	}
	if retryCalls != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d, want 1", retryCalls)
	}
	if retryDisposition != workload.RetryBlockPersist {
		t.Fatalf("RetryBlock disposition: got %v, want Persist", retryDisposition)
	}
	for _, idx := range []int32{0, 1} {
		if _, ok := recorder.statuses[idx]; ok {
			t.Errorf("instance %d status committed despite atomic RetryBlock failure", idx)
		}
	}
	pods := &corev1.PodList{}
	if listErr := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list pods: %v", listErr)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pods created after failed RetryBlock write: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_InstanceOnlyFallbackRetryBlockFailureStopsAfterFirstStatus(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-retry-fallback", "prod", 2)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-retry-fallback-engine-deadbeef", Namespace: isvc.Namespace},
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: target.Name,
		State:          workload.RetryBlockBackoff,
	}}
	retryErr := errors.New("retry block unavailable")
	retryCalls := 0
	input.MutateRetryBlock = func(_ context.Context, revision string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		retryCalls++
		if revision != target.Name {
			return fmt.Errorf("retry revision: got %q, want %q", revision, target.Name)
		}
		block := input.ObservedState.RetryBlocks[0]
		if got := mutate(&block); got != workload.RetryBlockPersist {
			return fmt.Errorf("RetryBlock disposition: got %v, want Persist", got)
		}
		return retryErr
	}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(2), target)
	if !errors.Is(err, retryErr) {
		t.Fatalf("Create error: got %v, want retry persistence failure", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0}}) {
		t.Fatalf("fallback Creating batches: got %v, want [[0]]", recorder.batches)
	}
	if retryCalls != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d, want 1", retryCalls)
	}
	if status := recorder.statuses[0]; status.Phase != workload.InstancePhaseCreating || status.Operation == nil {
		t.Fatalf("first fallback status: got %+v, want durable Creating intent", status)
	}
	if _, found := recorder.statuses[1]; found {
		t.Fatal("fallback path committed a later Creating status after RetryBlock failure")
	}
	pods := &corev1.PodList{}
	if listErr := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list Pods: %v", listErr)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pods created after failed RetryBlock write: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_BatchedAtomicCreatingFailureDoesNotBlockReadyInstances(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-atomic-isolation", "prod", 2)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-atomic-isolation-engine-" + testRevisionHash, Namespace: isvc.Namespace},
	}
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
	})
	pod := promotablePodForInstance(isvc, 0)
	base := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{TargetRevision: target.Name, State: workload.RetryBlockBackoff}}
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	atomicErr := errors.New("atomic Creating status unavailable")
	input.ApplyInstanceMutationsWithRetryBlock = func(ctx context.Context, mutations []workload.InstanceMutation, _ string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		if mutate == nil {
			return recorder.apply(ctx, mutations)
		}
		block := workload.RetryBlock{TargetRevision: target.Name, State: workload.RetryBlockBackoff}
		if got := mutate(&block); got != workload.RetryBlockPersist {
			return fmt.Errorf("RetryBlock disposition: got %v, want Persist", got)
		}
		return atomicErr
	}

	_, err := ops.Create(context.Background(), workload.Deps{Client: base}, input, buildPlanSinglePodEngine(2), target)
	if !errors.Is(err, atomicErr) {
		t.Fatalf("Create error: got %v, want atomic failure", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0}) {
		t.Fatalf("unrelated Ready batch: got %v, want [[0]]", recorder.batches)
	}
	if status := recorder.statuses[0]; status.Phase != workload.InstancePhaseReady {
		t.Fatalf("ready instance was blocked by Creating failure: got %+v", status)
	}
	if _, ok := recorder.statuses[1]; ok {
		t.Fatal("missing instance status committed despite atomic Creating failure")
	}
	freshPod := &corev1.Pod{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), freshPod); err != nil {
		t.Fatalf("get ready Pod: %v", err)
	}
	if !podreadiness.IsServing(freshPod) {
		t.Fatal("ready instance Pod did not become serving")
	}
}

func TestCreate_BatchedActionsPreservePlanOrder(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-plan-order", "prod", 3)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       1,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
	})
	readyPod := promotablePodForInstance(isvc, 1)
	base := newFakeClient(t, isvc, ir, readyPod)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		recorder.events = append(recorder.events, fmt.Sprintf("pod:%d", idx))
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	wantEvents := []string{"status:[0]", "pod:0", "status:[1]", "status:[2]", "pod:2"}
	if !reflect.DeepEqual(recorder.events, wantEvents) {
		t.Fatalf("action order: got %v, want %v", recorder.events, wantEvents)
	}
	freshReadyPod := &corev1.Pod{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(readyPod), freshReadyPod); err != nil {
		t.Fatalf("get ready Pod: %v", err)
	}
	if !podreadiness.IsServing(freshReadyPod) {
		t.Fatal("ready instance Pod did not become serving")
	}
}

func TestCreate_BatchedCreatingFailurePreservesNoOpPrefix(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-creating-noop-prefix", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
	})
	base := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	statusErr := errors.New("Creating status unavailable")
	statusCalls := 0
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		statusCalls++
		if statusCalls == 2 {
			return statusErr
		}
		return recorder.apply(ctx, mutations)
	}
	input.MutateInstance = unexpectedPerInstanceMutation
	attempted := make([]int32, 0, 1)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		attempted = append(attempted, int32(idx))
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(2), nil)
	if !errors.Is(err, statusErr) {
		t.Fatalf("Create error: got %v, want Creating status failure", err)
	}
	if !reflect.DeepEqual(attempted, []int32{0}) {
		t.Fatalf("Pod-create attempts before status failure: got %v, want [0]", attempted)
	}
	if _, exists := recorder.statuses[1]; exists {
		t.Fatal("failing changed-status action committed unexpectedly")
	}
}

func TestCreate_BatchedReadyFailurePreservesNoOpPrefix(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-ready-noop-prefix", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady},
		v1beta1.OMENativeInstanceStatus{
			Index:       1,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
		},
	)
	pod0 := promotablePodForInstance(isvc, 0)
	pod1 := promotablePodForInstance(isvc, 1)
	base := newFakeClient(t, isvc, ir, pod0, pod1)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	statusErr := errors.New("Ready status unavailable")
	statusCalls := 0
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		statusCalls++
		if statusCalls == 2 {
			return statusErr
		}
		return recorder.apply(ctx, mutations)
	}
	input.MutateInstance = unexpectedPerInstanceMutation

	_, err := ops.Create(context.Background(), workload.Deps{Client: base}, input, buildPlanSinglePodEngine(2), nil)
	if !errors.Is(err, statusErr) {
		t.Fatalf("Create error: got %v, want Ready status failure", err)
	}
	for idx := int32(0); idx < 2; idx++ {
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		if getErr := base.Get(context.Background(), key, pod); getErr != nil {
			t.Fatalf("get pod %d: %v", idx, getErr)
		}
		if !podreadiness.IsServing(pod) {
			t.Errorf("pod %d must retain the serving transition reached before its status result", idx)
		}
	}
	if status := recorder.statuses[0]; status.Phase != workload.InstancePhaseReady {
		t.Fatalf("no-op prefix status: got %+v, want Ready", status)
	}
	if status := recorder.statuses[1]; status.Phase != workload.InstancePhaseCreating {
		t.Fatalf("failed status action: got %+v, want Creating", status)
	}
}

func TestCreate_BatchedPodCreateFailureStopsAndRollsBackUnattemptedInstances(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-create-failure", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	createErr := errors.New("pod create unavailable")
	attempted := make([]int32, 0, 3)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		idx := int32(idx64)
		attempted = append(attempted, idx)
		if idx == 1 {
			return createErr
		}
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, createErr) {
		t.Fatalf("Create error: got %v, want Pod-create failure", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {2}}) {
		t.Fatalf("Creating and rollback batches: got %v, want [[0 1 2] [2]]", recorder.batches)
	}
	if !reflect.DeepEqual(attempted, []int32{0, 1}) {
		t.Fatalf("Pod-create attempts: got %v, want [0 1]", attempted)
	}
	for idx := int32(0); idx < 2; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseCreating || status.Operation == nil {
			t.Errorf("instance %d after create attempts: got %+v, want durable Creating intent", idx, status)
		}
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		getErr := base.Get(context.Background(), key, pod)
		if idx == 1 {
			if getErr == nil {
				t.Errorf("failed instance %d unexpectedly has a Pod", idx)
			}
			continue
		}
		if getErr != nil {
			t.Errorf("healthy instance %d Pod was not created: %v", idx, getErr)
		}
	}
	if _, ok := recorder.statuses[2]; ok {
		t.Fatal("unattempted instance 2 retained a speculative Creating status")
	}
	unattemptedPod := &corev1.Pod{}
	unattemptedKey := client.ObjectKey{
		Namespace: isvc.Namespace,
		Name:      query.PodName(isvc.Name, workload.ComponentEngine, 2, "default", 0),
	}
	if getErr := base.Get(context.Background(), unattemptedKey, unattemptedPod); getErr == nil {
		t.Fatal("unattempted instance 2 unexpectedly has a Pod")
	}
}

func TestCreate_BatchedPodCreateFailureRestoresPriorUnattemptedStatus(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-restore-status", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{
			Index:           0,
			Incarnation:     3,
			Phase:           v1beta1.OMENativeInstanceFailed,
			RunningRevision: "revision-a",
			ActiveOrdinal:   1,
		},
		v1beta1.OMENativeInstanceStatus{
			Index:           1,
			Incarnation:     7,
			Phase:           v1beta1.OMENativeInstanceFailed,
			RunningRevision: "revision-b",
			ActiveOrdinal:   1,
			PodCount:        4,
			ReadyPodCount:   2,
			NodesOccupied:   []string{"worker-a", "worker-b"},
		},
	)
	base := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	prior := recorder.statuses[1]
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	createErr := errors.New("pod create unavailable")
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(*corev1.Pod) error {
		return createErr
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(2), nil)
	if !errors.Is(err, createErr) {
		t.Fatalf("Create error: got %v, want Pod-create failure", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1}, {1}}) {
		t.Fatalf("Creating and restore batches: got %v, want [[0 1] [1]]", recorder.batches)
	}
	if got := recorder.statuses[1]; !reflect.DeepEqual(got, prior) {
		t.Fatalf("unattempted instance status was not restored:\n got: %+v\nwant: %+v", got, prior)
	}
	if got := recorder.statuses[0]; got.Phase != workload.InstancePhaseCreating || got.Operation == nil {
		t.Fatalf("attempted instance status: got %+v, want Creating intent", got)
	}
}

func TestCreate_BatchedRollbackPreservesConcurrentStatusChange(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-concurrent-status", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	createErr := errors.New("pod create unavailable")
	concurrent := workload.InstanceStatus{
		Index:           2,
		Incarnation:     9,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "revision-concurrent",
	}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		if idx == 1 {
			recorder.statuses[2] = concurrent
			return createErr
		}
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, createErr) {
		t.Fatalf("Create error: got %v, want Pod-create failure", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {2}}) {
		t.Fatalf("Creating and conditional rollback batches: got %v, want [[0 1 2] [2]]", recorder.batches)
	}
	if got := recorder.statuses[2]; !reflect.DeepEqual(got, concurrent) {
		t.Fatalf("conditional rollback clobbered concurrent status:\n got: %+v\nwant: %+v", got, concurrent)
	}
}

func TestCreate_BatchedCancellationStopsFurtherPodCreates(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-cancel", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return recorder.apply(ctx, mutations)
	}
	input.MutateInstance = unexpectedPerInstanceMutation

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempted := make([]int32, 0, 2)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		idx := int32(idx64)
		attempted = append(attempted, idx)
		if idx == 1 {
			cancel()
			return context.Canceled
		}
		return nil
	}}

	_, err := ops.Create(ctx, workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Create error: got %v, want context cancellation", err)
	}
	if !reflect.DeepEqual(attempted, []int32{0, 1}) {
		t.Fatalf("Pod-create attempts after cancellation: got %v, want [0 1]", attempted)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}}) {
		t.Fatalf("durable Creating batches after cancellation: got %v, want [[0 1 2]]", recorder.batches)
	}
	if status := recorder.statuses[2]; status.Phase != workload.InstancePhaseCreating || status.Operation == nil {
		t.Fatalf("unattempted status after canceled recovery write: got %+v, want resumable Creating intent", status)
	}
	pods := &corev1.PodList{}
	if listErr := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list Pods: %v", listErr)
	}
	wantPodName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 0)
	if len(pods.Items) != 1 || pods.Items[0].Name != wantPodName {
		t.Fatalf("Pods after cancellation: got %+v, want only %s", pods.Items, wantPodName)
	}
}

func TestCreate_BatchedGlobalAPIFailureStopsFurtherPodCreates(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-api-overload", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	overloadErr := apierrors.NewTooManyRequests("apiserver overloaded", 1)
	attempted := make([]int32, 0, 2)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		idx := int32(idx64)
		attempted = append(attempted, idx)
		if idx == 1 {
			return overloadErr
		}
		return nil
	}}

	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create error: got %v, want nil — a throttled apiserver is paced, not failed", err)
	}
	// The server's suggested delay is a floor. A 1s suggestion is already
	// satisfied by the ordinary create interval, so the pass keeps it.
	if result.RequeueAfter != testRequeueIntervals.Operation || result.RequeueAfter < time.Second {
		t.Fatalf("RequeueAfter: got %v, want %v (>= the server's suggested 1s)", result.RequeueAfter, testRequeueIntervals.Operation)
	}
	if !reflect.DeepEqual(attempted, []int32{0, 1}) {
		t.Fatalf("Pod-create attempts after API overload: got %v, want [0 1]", attempted)
	}
	pods := &corev1.PodList{}
	if listErr := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list Pods: %v", listErr)
	}
	if len(pods.Items) != 1 || pods.Items[0].Labels[query.LabelInstanceIdx] != "0" {
		t.Fatalf("Pods after API overload: got %+v, want only instance 0", pods.Items)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {2}}) {
		t.Fatalf("Creating and API-failure rollback batches: got %v, want [[0 1 2] [2]]", recorder.batches)
	}
}

func TestCreate_BatchedTransportFailureStopsFurtherPodCreates(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-transport-failure", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	transportErr := &net.DNSError{Err: "connection reset", Name: "kube-apiserver", IsTemporary: true}
	attempted := make([]int32, 0, 2)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		idx := int32(idx64)
		attempted = append(attempted, idx)
		if idx == 1 {
			return transportErr
		}
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, transportErr) {
		t.Fatalf("Create error: got %v, want transport failure", err)
	}
	if !reflect.DeepEqual(attempted, []int32{0, 1}) {
		t.Fatalf("Pod-create attempts after transport failure: got %v, want [0 1]", attempted)
	}
	pods := &corev1.PodList{}
	if listErr := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list Pods: %v", listErr)
	}
	if len(pods.Items) != 1 || pods.Items[0].Labels[query.LabelInstanceIdx] != "0" {
		t.Fatalf("Pods after transport failure: got %+v, want only instance 0", pods.Items)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {2}}) {
		t.Fatalf("Creating and transport-failure rollback batches: got %v, want [[0 1 2] [2]]", recorder.batches)
	}
}

func TestCreate_BatchedResumedCreatingConsumesMissingPodBudget(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-resume", "prod", 3)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationCreate,
		},
	})
	base := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(1)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	created := make([]int32, 0, 2)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		created = append(created, int32(idx64))
		return nil
	}}
	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("a pass with deferred missing Instances must requeue")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0}) {
		t.Fatalf("resumed Creating revalidation batch: got %v, want [[0]]", recorder.batches)
	}
	if !reflect.DeepEqual(created, []int32{0}) {
		t.Fatalf("created Instance indices: got %v, want only resumed index 0", created)
	}
}

func TestCreate_BatchedPartiallyMaterializedGangChargesOnlyMissingPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-partial-gang", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationCreate,
		},
	})
	base := newFakeClient(t,
		isvc,
		ir,
		gangPod(isvc, 0, "leader", 0, 1, false, false),
		gangPod(isvc, 0, "worker", 0, 1, false, false),
		gangPod(isvc, 0, "worker", 1, 1, false, false),
	)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(6)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	plan := buildPlanSinglePodEngine(2)
	plan.Instances[0].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}

	createdByInstance := map[int32]int{}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		createdByInstance[int32(idx64)]++
		return nil
	}}
	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0}, {1}}) {
		t.Fatalf("Creating batches: got %v, want resumed no-op gang [0] then fresh singleton [1]", recorder.batches)
	}
	wantCreates := map[int32]int{0: 5, 1: 1}
	if !reflect.DeepEqual(createdByInstance, wantCreates) {
		t.Fatalf("created missing pods: got %v, want %v", createdByInstance, wantCreates)
	}

	pods := &corev1.PodList{}
	if err := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	finalByInstance := map[string]int{}
	for i := range pods.Items {
		finalByInstance[pods.Items[i].Labels[query.LabelInstanceIdx]]++
	}
	wantFinal := map[string]int{"0": 8, "1": 1}
	if !reflect.DeepEqual(finalByInstance, wantFinal) {
		t.Fatalf("final pod topology: got %v, want %v", finalByInstance, wantFinal)
	}
}

func TestCreate_BatchedPodBudgetKeepsGangAtomicAndClosesStablePrefix(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-gang", "prod", 4)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(10)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(4)
	plan.Instances[1].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}
	plan.Instances[2].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}

	createdByInstance := map[int32]int{}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return fmt.Errorf("parse instance index on pod %s: %w", pod.Name, err)
		}
		idx := int32(idx64)
		status, ok := recorder.statuses[idx]
		if !ok || status.Phase != workload.InstancePhaseCreating {
			return fmt.Errorf("pod %s created before gang intent was committed", pod.Name)
		}
		createdByInstance[idx]++
		return nil
	}}

	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("the deferred second gang must cause a requeue")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1}) {
		t.Fatalf("Creating batch: got %v, want singleton 0 plus gang 1", recorder.batches)
	}
	if createdByInstance[0] != 1 || createdByInstance[1] != 8 {
		t.Fatalf("created pod weights: got %v, want instance 0=1 and instance 1=8", createdByInstance)
	}
	if createdByInstance[2] != 0 {
		t.Fatalf("second gang was split or admitted past the pod budget: got %d pod(s)", createdByInstance[2])
	}
	if createdByInstance[3] != 0 {
		t.Fatalf("later singleton bypassed the deferred gang: got %d pod(s)", createdByInstance[3])
	}
}

func TestCreate_BatchedPodBudgetAllowsOneOversizedGang(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-oversized-gang", "prod", 2)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(5)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(2)
	plan.Instances[0].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}

	createdByInstance := map[int32]int{}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		createdByInstance[int32(idx64)]++
		return nil
	}}

	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("the deferred singleton must cause a requeue")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0}) {
		t.Fatalf("Creating batch: got %v, want only oversized gang 0", recorder.batches)
	}
	if createdByInstance[0] != 8 || createdByInstance[1] != 0 {
		t.Fatalf("oversized gang admission: got %v, want instance 0=8 and instance 1=0", createdByInstance)
	}
}

func TestCreate_BatchedWavesAdvancePastOversizedGangAndConverge(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-converge", "prod", 3)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(3)
	plan.Instances[1].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 2},
	}
	syncObserved := func() {
		input.ObservedState.InstanceStatuses = input.ObservedState.InstanceStatuses[:0]
		for _, instance := range plan.Instances {
			if status, ok := recorder.statuses[instance.Index]; ok {
				input.ObservedState.InstanceStatuses = append(input.ObservedState.InstanceStatuses, status)
			}
		}
	}
	observedPods := map[string]struct{}{}
	for pass, wantPods := range []int{1, 4, 5} {
		syncObserved()
		result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
		if err != nil {
			t.Fatalf("Create pass %d: %v", pass+1, err)
		}
		if result.RequeueAfter == 0 {
			t.Fatalf("Create pass %d did not requeue while Pods were becoming ready", pass+1)
		}

		pods := &corev1.PodList{}
		if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
			t.Fatalf("list Pods after pass %d: %v", pass+1, err)
		}
		if len(pods.Items) != wantPods {
			t.Fatalf("Pods after pass %d: got %d, want %d", pass+1, len(pods.Items), wantPods)
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if _, seen := observedPods[pod.Name]; seen {
				continue
			}
			idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
			if err != nil {
				t.Fatalf("parse Instance index on %s: %v", pod.Name, err)
			}
			workload.DefaultExpectations.ObservedCreate(isvc.Namespace, isvc.Name, workload.ComponentEngine, int32(idx64))
			observedPods[pod.Name] = struct{}{}
		}
	}

	wantCreatingBatches := [][]int32{{0}, {1}, {2}}
	if !reflect.DeepEqual(recorder.batches, wantCreatingBatches) {
		t.Fatalf("Creating waves: got %v, want %v", recorder.batches, wantCreatingBatches)
	}
	// Two more passes to promote: the first writes the serving gate on every
	// pod, the second observes kubelet folding it into PodReady.
	var result ctrl.Result
	for pass := 0; pass < 2; pass++ {
		pods := &corev1.PodList{}
		if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
			t.Fatalf("list Pods before Ready pass %d: %v", pass+1, err)
		}
		for i := range pods.Items {
			makeNewPodReady(t, c, isvc.Namespace, pods.Items[i].Name, 1)
		}
		syncObserved()
		var err error
		result, err = ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
		if err != nil {
			t.Fatalf("Ready pass %d: %v", pass+1, err)
		}
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("converged Ready pass requeued: %+v", result)
	}
	wantAllBatches := [][]int32{{0}, {1}, {2}, {0, 1, 2}}
	if !reflect.DeepEqual(recorder.batches, wantAllBatches) {
		t.Fatalf("all mutation batches: got %v, want %v", recorder.batches, wantAllBatches)
	}
	for idx := int32(0); idx < 3; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseReady || status.Operation != nil {
			t.Errorf("instance %d after convergence: got %+v", idx, status)
		}
	}
}

// TestCreate_BatchedGateDeniedInstanceConsumesNoBudget: the pod budget is
// spent by the Instances the pass actually selects, so a gate-denied one
// leaves the whole budget to its neighbour. The block is a not-yet-due
// Backoff because that state gates a fresh start only: index 0 is denied and
// index 1, already materializing, keeps its authorization.
func TestCreate_BatchedGateDeniedInstanceConsumesNoBudget(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-gate", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{
			Index:       0,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceFailed,
		},
		v1beta1.OMENativeInstanceStatus{
			Index:       1,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationCreate,
			},
		},
	)
	base := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-gate-engine-deadbeef", Namespace: isvc.Namespace},
	}
	notDue := metav1.NewTime(time.Now().Add(time.Hour))
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: target.Name,
		State:          workload.RetryBlockBackoff,
		NextRetryAt:    &notDue,
	}}
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(1)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	created := make([]int32, 0, 1)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		created = append(created, int32(idx64))
		return nil
	}}
	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(2), target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{1}) {
		t.Fatalf("Creating batch after gate denial: got %v, want [[1]]", recorder.batches)
	}
	if !reflect.DeepEqual(created, []int32{1}) {
		t.Fatalf("created Instance indices: got %v, want eligible index 1", created)
	}
}

func TestCreate_BatchesSupersededAttemptRetirement(t *testing.T) {
	resetExpectations(t)
	const replicas = int32(3)
	isvc := minimalISVC("batch-retire", "prod", int(replicas))
	statuses := make([]v1beta1.OMENativeInstanceStatus, 0, replicas)
	objects := []client.Object{isvc}
	const priorRevision = "batch-retire-engine-bad0bad0"
	for idx := int32(0); idx < replicas; idx++ {
		statuses = append(statuses, v1beta1.OMENativeInstanceStatus{
			Index:       idx,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				ID:             fmt.Sprintf("create-%d", idx),
				Type:           v1beta1.InstanceOperationCreate,
				TargetRevision: priorRevision,
			},
		})
		leader := gangPod(isvc, idx, "leader", 0, 1, false, false)
		leader.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(priorRevision)
		objects = append(objects, leader)
	}
	objects = append(objects, instanceIR(isvc, workload.ComponentEngine, statuses...))
	base := newFakeClient(t, objects...)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-retire-engine-good0bad", Namespace: isvc.Namespace},
	}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  replicas,
	}
	for idx := int32(0); idx < replicas; idx++ {
		plan.Instances = append(plan.Instances, workload.InstancePlan{
			Index:       idx,
			Incarnation: 1,
			Runners:     []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		})
	}

	result, err := ops.Create(context.Background(), workload.Deps{Client: base}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("retirement batch must requeue immediately: %+v", result)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("retirement batches: got %v, want one batch [0 1 2]", recorder.batches)
	}
	for idx := int32(0); idx < replicas; idx++ {
		status := recorder.statuses[idx]
		op := status.Operation
		if status.Phase != workload.InstancePhaseCreating || op == nil {
			t.Errorf("instance %d after retirement: got %+v, want Creating under a fresh attempt", idx, status)
			continue
		}
		if op.ID == fmt.Sprintf("create-%d", idx) || op.TargetRevision != target.Name {
			t.Errorf("instance %d replacement attempt: got {id:%s target:%s}, want a new id pinned to %s",
				idx, op.ID, op.TargetRevision, target.Name)
		}
	}
	pods := &corev1.PodList{}
	if err := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != int(replicas) {
		t.Fatalf("retirement must not create missing workers: got %d pods want %d leaders", len(pods.Items), replicas)
	}
}

func TestCreate_BatchedReadyPromotionsUseOneMutationBatch(t *testing.T) {
	resetExpectations(t)
	isvc, c, input := newReadyBatchFixture(t, "batch-ready", 3)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		if err := requireMutationPodsServing(ctx, c, isvc, mutations); err != nil {
			return err
		}
		return recorder.apply(ctx, mutations)
	}

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("fully ready batch requeued: %+v", result)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("Ready batches: got %v, want one batch [0 1 2]", recorder.batches)
	}
	for idx := int32(0); idx < 3; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseReady || status.Operation != nil {
			t.Errorf("instance %d after Ready batch: got %+v", idx, status)
		}
	}
}

func TestCreate_BatchedReadyOnRevisionPromotesAndPrunesOnce(t *testing.T) {
	resetExpectations(t)
	isvc, c, input := newReadyBatchFixture(t, "batch-ready-revision", 3)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "batch-ready-revision-engine-" + testRevisionHash,
			Namespace: isvc.Namespace,
		},
	}
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	for idx := int32(0); idx < 3; idx++ {
		status := recorder.statuses[idx]
		status.TargetRevision = target.Name
		recorder.statuses[idx] = status
	}
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		if err := requireMutationPodsServing(ctx, c, isvc, mutations); err != nil {
			return err
		}
		return recorder.apply(ctx, mutations)
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: target.Name,
		State:          workload.RetryBlockHeld,
	}}
	pruneCalls := 0
	var pruneDisposition workload.RetryBlockDisposition
	input.MutateRetryBlock = func(_ context.Context, revision string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		if revision != target.Name {
			return fmt.Errorf("prune revision: got %q, want %q", revision, target.Name)
		}
		pruneCalls++
		block := workload.RetryBlock{TargetRevision: revision, State: workload.RetryBlockHeld}
		pruneDisposition = mutate(&block)
		return nil
	}

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("fully ready revision batch requeued: %+v", result)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("Ready-on-revision batches: got %v, want [[0 1 2]]", recorder.batches)
	}
	if pruneCalls != 1 || pruneDisposition != workload.RetryBlockRemove {
		t.Fatalf("RetryBlock prune: calls=%d disposition=%v, want one Remove", pruneCalls, pruneDisposition)
	}
	for idx := int32(0); idx < 3; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseReady || status.Operation != nil || status.RunningRevision != target.Name || status.TargetRevision != "" {
			t.Errorf("instance %d after revision promotion: got %+v", idx, status)
		}
	}
}

func TestCreate_BatchedServingMarkFailureStopsAtFailedInstance(t *testing.T) {
	resetExpectations(t)
	// Only instance 0 is past the promote bar; instances 1 and 2 still owe
	// their serving-gate write, and instance 1's is the one that fails.
	isvc, base, input := newBatchFixture(t, "batch-serving-failure", 3, 1)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = recorder.apply
	markErr := errors.New("serving status unavailable")
	failedPodName := query.PodName(isvc.Name, workload.ComponentEngine, 1, "default", 0)
	c := &failingPodStatusClient{Client: base, podName: failedPodName, err: markErr}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, markErr) {
		t.Fatalf("Create error: got %v, want serving-mark failure", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0}) {
		t.Fatalf("Ready prefix after serving-mark failure: got %v, want [[0]]", recorder.batches)
	}
	for idx, wantServing := range map[int32]bool{0: true, 1: false, 2: false} {
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		if getErr := base.Get(context.Background(), key, pod); getErr != nil {
			t.Fatalf("get pod %d: %v", idx, getErr)
		}
		if got := podreadiness.IsServing(pod); got != wantServing {
			t.Errorf("pod %d serving after failed mark: got %t, want %t", idx, got, wantServing)
		}
		status := recorder.statuses[idx]
		wantPhase := workload.InstancePhaseReady
		if idx >= 1 {
			wantPhase = workload.InstancePhaseCreating
		}
		if status.Phase != wantPhase {
			t.Errorf("instance %d phase after failed mark: got %s, want %s", idx, status.Phase, wantPhase)
		}
	}
}

// TestCreate_BatchedReadyPersistenceFailureRestoresFailFastServingPrefix: a
// failed Ready batch leaves every Instance where it was. Each of these pods
// was already in rotation before the pass — the promote bar requires the
// serving gate to be satisfied already — so the restored prefix is the empty
// one: the batch has no gate write of its own to take back.
func TestCreate_BatchedReadyPersistenceFailureRestoresFailFastServingPrefix(t *testing.T) {
	resetExpectations(t)
	isvc, c, input := newReadyBatchFixture(t, "batch-ready-failure", 3)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	readyErr := errors.New("Ready status unavailable")
	recorder.fail = readyErr
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		if err := requireMutationPodsServing(ctx, c, isvc, mutations); err != nil {
			return err
		}
		return recorder.apply(ctx, mutations)
	}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, readyErr) {
		t.Fatalf("Create error: got %v, want Ready persistence failure", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("attempted Ready batch: got %v, want [[0 1 2]]", recorder.batches)
	}
	for idx := int32(0); idx < 3; idx++ {
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		if getErr := c.Get(context.Background(), key, pod); getErr != nil {
			t.Fatalf("get pod %d: %v", idx, getErr)
		}
		if !podreadiness.IsServing(pod) {
			t.Errorf("pod %d left rotation after the Ready batch failed", idx)
		}
		if status := recorder.statuses[idx]; status.Phase != workload.InstancePhaseCreating {
			t.Errorf("instance %d after failed Ready batch: got %+v, want Creating", idx, status)
		}
	}
}

func TestCreate_BatchedRetryBlockPruneFailureRestoresFailFastPrefix(t *testing.T) {
	resetExpectations(t)
	isvc, c, input := newReadyBatchFixture(t, "batch-prune-failure", 3)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-prune-failure-engine-" + testRevisionHash, Namespace: isvc.Namespace},
	}
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = recorder.apply
	pruneErr := errors.New("RetryBlock prune unavailable")
	pruneCalls := 0
	input.MutateRetryBlock = func(_ context.Context, _ string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		pruneCalls++
		block := workload.RetryBlock{TargetRevision: target.Name, State: workload.RetryBlockHeld}
		if got := mutate(&block); got != workload.RetryBlockRemove {
			return fmt.Errorf("prune disposition: got %v, want Remove", got)
		}
		return pruneErr
	}
	events := record.NewFakeRecorder(10)

	_, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: events}, input, buildPlanSinglePodEngine(3), target)
	if !errors.Is(err, pruneErr) {
		t.Fatalf("Create error: got %v, want prune failure", err)
	}
	if pruneCalls != 1 {
		t.Fatalf("RetryBlock prune calls: got %d, want 1", pruneCalls)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {1, 2}}) {
		t.Fatalf("Ready and rollback batches: got %v, want [[0 1 2] [1 2]]", recorder.batches)
	}
	for idx := int32(0); idx < 3; idx++ {
		wantPhase := workload.InstancePhaseCreating
		if idx == 0 {
			wantPhase = workload.InstancePhaseReady
		}
		if status := recorder.statuses[idx]; status.Phase != wantPhase {
			t.Errorf("instance %d after prune failure: got %+v, want phase %s", idx, status, wantPhase)
		}
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		if getErr := c.Get(context.Background(), key, pod); getErr != nil {
			t.Fatalf("get pod %d: %v", idx, getErr)
		}
		// Every pod was already in rotation before the pass: the promote bar
		// reads PodReady, which the serving gate is a precondition of.
		if !podreadiness.IsServing(pod) {
			t.Errorf("pod %d left rotation after the prune failed", idx)
		}
	}
	select {
	case event := <-events.Events:
		t.Fatalf("unexpected Ready event after failed first prune: %q", event)
	default:
	}
}

func unexpectedPerInstanceMutation(_ context.Context, idx int32, _ func(*workload.InstanceStatus) bool) error {
	return fmt.Errorf("unexpected per-Instance status mutation for index %d", idx)
}

// gatePodAtOrdinal builds one pod of a multi-pod Instance that is
// ContainersReady but still held out of rotation — the state whose only
// remaining pass action is the serving-gate write.
func gatePodAtOrdinal(isvc *v1beta1.InferenceService, instanceIdx, ordinal int32) *corev1.Pod {
	pod := podForInstance(isvc, instanceIdx, true /* ready */, false /* serving */)
	pod.Name = query.PodName(isvc.Name, workload.ComponentEngine, instanceIdx, "default", ordinal)
	pod.Labels = testPodLabels(isvc.Name, workload.ComponentEngine, instanceIdx, "default", 1, ordinal)
	pod.Labels[query.LabelRevisionHash] = testRevisionHash
	return pod
}

// TestCreate_BatchedServingRollbackUndoesGateWritesWhenReadyBatchFails: an
// Instance below the promote bar contributes a serving-gate write and no
// status mutation. When a later pod of that Instance cannot be written and
// the Ready batch for the promoted Instance also fails, the gate writes the
// pass already made are taken back, so no pod is left in rotation for an
// Instance the pass could not record.
func TestCreate_BatchedServingRollbackUndoesGateWritesWhenReadyBatchFails(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-serving-rollback", "prod", 2)
	// Instance 0 is already Ready and past the bar, so its promote is a
	// no-change mutation that batches with instance 1's gate-only action.
	ready := promotablePodForInstance(isvc, 0)
	gateFirst := gatePodAtOrdinal(isvc, 1, 0)
	gateSecond := gatePodAtOrdinal(isvc, 1, 1)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady},
		v1beta1.OMENativeInstanceStatus{
			Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate, StartedAt: metav1.Now()},
		},
	)
	base := newFakeClient(t, isvc, ir, ready, gateFirst, gateSecond)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	readyErr := errors.New("Ready status unavailable")
	recorder.fail = readyErr
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	markErr := errors.New("serving status unavailable")
	c := &failingPodStatusClient{Client: base, podName: gateSecond.Name, err: markErr}

	plan := buildPlanSinglePodEngine(2)
	plan.Instances[1].Runners = []workload.RunnerPlan{{Name: "default", Size: 2}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if !errors.Is(err, readyErr) {
		t.Fatalf("Create error: got %v, want the Ready batch failure", err)
	}
	live := &corev1.Pod{}
	if getErr := base.Get(context.Background(), client.ObjectKeyFromObject(gateFirst), live); getErr != nil {
		t.Fatalf("get gate-written pod: %v", getErr)
	}
	if podreadiness.IsServing(live) {
		t.Error("pod left in rotation for an Instance the pass could not record")
	}
	if getErr := base.Get(context.Background(), client.ObjectKeyFromObject(ready), live); getErr != nil {
		t.Fatalf("get promoted-Instance pod: %v", getErr)
	}
	if !podreadiness.IsServing(live) {
		t.Error("a pod that was already in rotation before the pass must stay in it")
	}
}

// Create-path apiserver-rejection tests. An apiserver answer is evidence:
// a 422 names a broken pod template, a namespace-terminating refusal names
// a dead environment, a quota refusal names missing capacity, a 429 names
// an overloaded server. Each one gets its own outcome instead of an opaque
// error retried on blind backoff until the operation deadline lies about
// what happened.

// rejectionTargetCR is the revision the fixture's creates converge toward.
// Its name follows the "<owner>-<component>-<hash>" shape the revision
// helpers parse.
func rejectionTargetCR() *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-testrev1", Namespace: "prod"},
	}
}

// rejectionFixture builds a Create pass over `replicas` single-pod
// Instances whose Pod creates are answered by rejectPod: a nil return
// lets the create through, a non-nil return is the apiserver's rejection
// of that pod name.
func rejectionFixture(t *testing.T, replicas int32, rejectPod func(podName string) error) (
	client.Client,
	*v1beta1.InferenceService,
	workload.ReconcileInput,
	workload.ComponentPlan,
	*appsv1.ControllerRevision,
	*record.FakeRecorder,
	*[]retryBlockWrite,
) {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", int(replicas))
	target := rejectionTargetCR()

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("build scheme: %v", err)
		}
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, target).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if pod, ok := obj.(*corev1.Pod); ok && rejectPod != nil {
					if err := rejectPod(pod.Name); err != nil {
						return err
					}
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).
		Build()

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = target.Name
	// The dispatcher allocates one pacing sink per pass; these tests call
	// the op directly, so they supply it themselves.
	input.Pacing = &workload.APIPacing{}
	writes := &[]retryBlockWrite{}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		d := mutate(&b)
		if d != workload.RetryBlockUnchanged {
			*writes = append(*writes, retryBlockWrite{rev: rev, block: b})
		}
		return nil
	}
	return c, isvc, input, buildPlanSinglePodEngine(replicas), target, record.NewFakeRecorder(16), writes
}

// retryBlockWrite is one committed RetryBlock mutation.
type retryBlockWrite struct {
	rev   string
	block workload.RetryBlock
}

// rejectionEvents drains the recorder.
func rejectionEvents(recorder *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-recorder.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func countRejectionEvents(events []string, reason workload.EventReason) int {
	n := 0
	for _, ev := range events {
		if strings.Contains(ev, string(reason)) {
			n++
		}
	}
	return n
}

func rejectionPodNames(t *testing.T, c client.Client) []string {
	t.Helper()
	list := &corev1.PodList{}
	if err := c.List(context.Background(), list, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	names := make([]string, 0, len(list.Items))
	for _, p := range list.Items {
		names = append(names, p.Name)
	}
	return names
}

func invalidPodError(name string) error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, name, field.ErrorList{
		field.Invalid(field.NewPath("spec", "containers").Index(0).Child("resources", "limits"), "-1", "must be greater than or equal to 0"),
	})
}

func namespaceTerminatingPodError(name string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    403,
		Reason:  metav1.StatusReasonForbidden,
		Message: `pods "` + name + `" is forbidden: unable to create new content in namespace prod because it is being terminated`,
		Details: &metav1.StatusDetails{
			Kind: "pods", Name: name,
			Causes: []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause, Message: "namespace prod is being terminated"}},
		},
	}}
}

func quotaPodError(name string) error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, name, errors.New(
		"exceeded quota: team-quota, requested: requests.nvidia.com/gpu=8, used: requests.nvidia.com/gpu=56, limited: requests.nvidia.com/gpu=64"))
}

// TestCreate_InvalidPodSpec_FailsOnlyTheRejectedInstance: a 422 is the
// apiserver saying the pod template is unacceptable. Retrying the same
// revision reproduces it, so the rejected Instance is failed with a
// RetryBlock against its target revision — while every other Instance in
// the same batch is created as usual.
func TestCreate_InvalidPodSpec_FailsOnlyTheRejectedInstance(t *testing.T) {
	c, isvc, input, plan, target, recorder, writes := rejectionFixture(t, 2, func(podName string) error {
		if podName == "llama-70b-engine-0-default-0" {
			return invalidPodError(podName)
		}
		return nil
	})

	result, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v (a permanent rejection is disposed, not returned)", err)
	}
	if result.RequeueAfter != testRequeueIntervals.Operation {
		t.Errorf("RequeueAfter: got %v want %v", result.RequeueAfter, testRequeueIntervals.Operation)
	}

	rejected := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if rejected == nil {
		t.Fatalf("instance 0 status: missing")
	}
	if rejected.Phase != v1beta1.OMENativeInstanceFailed || rejected.Operation != nil {
		t.Errorf("instance 0: got phase=%q op=%+v want Failed with the operation cleared", rejected.Phase, rejected.Operation)
	}
	if rejected.LastFailure == nil || rejected.LastFailure.Reason != workload.RejectionReasonInvalidPodSpec {
		t.Fatalf("instance 0 LastFailure: got %+v want reason=%s", rejected.LastFailure, workload.RejectionReasonInvalidPodSpec)
	}
	if !strings.Contains(rejected.LastFailure.Message, "resources.limits") {
		t.Errorf("instance 0 LastFailure.Message: got %q want the apiserver's own detail", rejected.LastFailure.Message)
	}

	if len(*writes) != 1 || (*writes)[0].rev != target.Name {
		t.Fatalf("RetryBlock writes: got %+v want one for %s", *writes, target.Name)
	}

	// The sibling Instance is untouched by its neighbour's bad luck.
	sibling := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 1)
	if sibling == nil || sibling.Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("instance 1: got %+v want Creating", sibling)
	}
	names := rejectionPodNames(t, c)
	if len(names) != 1 || names[0] != "llama-70b-engine-1-default-0" {
		t.Errorf("pods: got %v want only llama-70b-engine-1-default-0", names)
	}

	events := rejectionEvents(recorder)
	if got := countRejectionEvents(events, workload.EventReasonInstanceRejected); got != 1 {
		t.Errorf("rejection events: got %d want 1 (%v)", got, events)
	}
}

// TestCreate_NamespaceTerminating_FailsWithoutBlamingTheRevision: the
// namespace is going away, which no revision can fix. The Instance is
// failed so the operator sees why, but the revision keeps a clean retry
// ladder.
func TestCreate_NamespaceTerminating_FailsWithoutBlamingTheRevision(t *testing.T) {
	c, isvc, input, plan, target, recorder, writes := rejectionFixture(t, 1, namespaceTerminatingPodError)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err != nil {
		t.Fatalf("Create: %v (an environment rejection is disposed, not returned)", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("instance 0: got %+v want Failed with the operation cleared", s)
	}
	if s.LastFailure == nil || s.LastFailure.Reason != workload.RejectionReasonNamespaceTerminating {
		t.Errorf("LastFailure: got %+v want reason=%s", s.LastFailure, workload.RejectionReasonNamespaceTerminating)
	}
	if len(*writes) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none (the revision is blameless)", *writes)
	}
	if got := countRejectionEvents(rejectionEvents(recorder), workload.EventReasonInstanceRejected); got != 1 {
		t.Errorf("rejection events: got %d want 1", got)
	}
	if names := rejectionPodNames(t, c); len(names) != 0 {
		t.Errorf("pods: got %v want none", names)
	}
}

// TestCreate_QuotaExceeded_WaitsWithoutFailing: out of quota is a
// capacity wait, not a failure. The Instance keeps its operation, records
// the quota as its waiting token (which parks the InstanceReadyTimeout
// clock — see ReconcileGatedDeadlines), and the pass retries on the
// ordinary create interval.
//
// The apiserver's quota message is DIFFERENT on every pass — it names
// current usage and the pod that lost the race — so the test varies it
// across passes to pin that neither status nor the event follows it: one
// status write and one event for the whole episode, however long the
// namespace stays full.
func TestCreate_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	pass := 0
	c, isvc, input, plan, target, recorder, writes := rejectionFixture(t, 1, func(podName string) error {
		pass++
		return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, podName, fmt.Errorf(
			"exceeded quota: team-quota, requested: requests.nvidia.com/gpu=8, used: requests.nvidia.com/gpu=%d, limited: requests.nvidia.com/gpu=64", 48+pass))
	})

	result, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v (a quota refusal is a wait, not an error)", err)
	}
	if result.RequeueAfter != testRequeueIntervals.Operation {
		t.Errorf("RequeueAfter: got %v want %v", result.RequeueAfter, testRequeueIntervals.Operation)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil {
		t.Fatalf("instance 0: got %+v want Creating with its operation intact", s)
	}
	if !workload.OperationCapacityRefused(fromV1beta1Op(s.Operation)) {
		t.Errorf("Operation: got %+v want the quota refusal recorded", s.Operation)
	}
	if s.Operation.Reason != "" {
		t.Errorf("Operation.Reason: got %q want untouched — Reason names why the operation exists", s.Operation.Reason)
	}
	if s.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %q want none — the hold pass is what reports the wait", s.Operation.Waiting)
	}
	if s.LastFailure != nil {
		t.Errorf("LastFailure: got %+v want none (a wait is not a failure)", s.LastFailure)
	}
	if len(*writes) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none", *writes)
	}
	events := rejectionEvents(recorder)
	if got := countRejectionEvents(events, workload.EventReasonInstanceQuotaBlocked); got != 1 {
		t.Fatalf("quota events after the first pass: got %d want 1", got)
	}
	if !anyEventContains(events, "used: requests.nvidia.com/gpu=49") {
		t.Errorf("quota event must carry the apiserver's message; got %v", events)
	}
	rvAfterBlock := irResourceVersion(t, c, isvc)

	// Two more still-blocked passes, each answered with a DIFFERENT quota
	// message: no second event and no further status write.
	for i := 0; i < 2; i++ {
		next := buildTestInput(isvc, c, workload.ComponentEngine)
		next.ObservedState.UpdateRevision = target.Name
		next.Pacing = &workload.APIPacing{}
		next.MutateRetryBlock = input.MutateRetryBlock
		if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, next, plan, target); err != nil {
			t.Fatalf("Create (blocked pass %d): %v", i+2, err)
		}
	}
	if got := countRejectionEvents(rejectionEvents(recorder), workload.EventReasonInstanceQuotaBlocked); got != 0 {
		t.Errorf("quota events on repeat blocked passes: got %d want 0 (one per episode)", got)
	}
	if rv := irResourceVersion(t, c, isvc); rv != rvAfterBlock {
		t.Errorf("status resourceVersion moved on repeat blocked passes: %s -> %s (the episode must write once)", rvAfterBlock, rv)
	}
}

// TestCreate_QuotaCleared_OnAlreadyExistingPods: a blocked Instance whose
// targets all come back AlreadyExists creates nothing, yet it is plainly
// no longer blocked — the pods are there. Releasing the token on pass
// COMPLETION rather than on a create count is what stops such an Instance
// keeping a stale block and a parked deadline forever.
func TestCreate_QuotaCleared_OnAlreadyExistingPods(t *testing.T) {
	blocked := true
	c, isvc, input, plan, target, recorder, _ := rejectionFixture(t, 1, func(podName string) error {
		if blocked {
			return quotaPodError(podName)
		}
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, podName)
	})

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err != nil {
		t.Fatalf("Create (blocked): %v", err)
	}
	if s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0); s == nil ||
		!workload.OperationCapacityRefused(fromV1beta1Op(s.Operation)) {
		t.Fatalf("instance 0 after the blocked pass: got %+v want the quota refusal recorded", s)
	}

	blocked = false
	resetExpectations(t)
	next := buildTestInput(isvc, c, workload.ComponentEngine)
	next.ObservedState.UpdateRevision = target.Name
	next.Pacing = &workload.APIPacing{}
	next.MutateRetryBlock = input.MutateRetryBlock
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, next, plan, target); err != nil {
		t.Fatalf("Create (all targets AlreadyExists): %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Operation == nil {
		t.Fatalf("instance 0: got %+v want a live operation", s)
	}
	if s.Operation.Waiting != "" {
		t.Fatalf("Operation.Waiting: got %q want cleared — no create landed, but nothing is blocking either", s.Operation.Waiting)
	}
	// The cleared token is what lets the parking step re-arm the deadline.
	insts := v1beta1convert.InstanceStatusSliceToWorkload(
		[]v1beta1.OMENativeInstanceStatus{*s})
	if workload.OperationCapacityRefused(insts[0].Operation) {
		t.Errorf("instance 0 still reads as capacity-blocked: %+v", insts[0].Operation)
	}
}

// TestCreate_QuotaCleared_ReleasesTheWaitingToken: once quota frees up
// and the create lands, the waiting token is removed — which is what
// re-arms the parked deadline from that moment.
func TestCreate_QuotaCleared_ReleasesTheWaitingToken(t *testing.T) {
	blocked := true
	c, isvc, input, plan, target, recorder, _ := rejectionFixture(t, 1, func(podName string) error {
		if blocked {
			return quotaPodError(podName)
		}
		return nil
	})

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err != nil {
		t.Fatalf("Create (blocked): %v", err)
	}
	blocked = false
	resetExpectations(t)
	input2 := buildTestInput(isvc, c, workload.ComponentEngine)
	input2.ObservedState.UpdateRevision = target.Name
	input2.Pacing = &workload.APIPacing{}
	input2.MutateRetryBlock = input.MutateRetryBlock
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input2, plan, target); err != nil {
		t.Fatalf("Create (unblocked): %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Operation == nil {
		t.Fatalf("instance 0: got %+v want a live operation", s)
	}
	if s.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %q want cleared once the create landed", s.Operation.Waiting)
	}
	if names := rejectionPodNames(t, c); len(names) != 1 {
		t.Errorf("pods: got %v want the create to have landed", names)
	}
}

// TestCreate_QuotaExceeded_AnnouncedOnlyWhenItBecomesTheReport: a row
// already reporting another authority's fact keeps that report when a
// quota refusal lands on it. The refusal is recorded — the deadline parks
// on it and the hold pass reads it — but no InstanceQuotaBlocked event
// is announced, because the wait an operator sees on the row has not
// changed.
func TestCreate_QuotaExceeded_AnnouncedOnlyWhenItBecomesTheReport(t *testing.T) {
	c, isvc, input, plan, target, recorder, _ := rejectionFixture(t, 1, quotaPodError)
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err != nil {
		t.Fatalf("Create (first refusal): %v", err)
	}
	if got := countRejectionEvents(rejectionEvents(recorder), workload.EventReasonInstanceQuotaBlocked); got != 1 {
		t.Fatalf("quota events on an unheld row: got %d want 1", got)
	}

	// The scheduler takes the row between passes, and the record is
	// retired by a create that landed; the next refusal re-records it.
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: irName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("read IR: %v", err)
	}
	ir.Status.InstanceStatuses[0].Operation.Waiting = workload.WaitingReasonUnschedulable
	ir.Status.InstanceStatuses[0].Operation.CapacityRefusedAt = nil
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed the scheduler's token: %v", err)
	}
	resetExpectations(t)
	next := buildTestInput(isvc, c, workload.ComponentEngine)
	next.ObservedState.UpdateRevision = target.Name
	next.Pacing = &workload.APIPacing{}
	next.MutateRetryBlock = input.MutateRetryBlock
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, next, plan, target); err != nil {
		t.Fatalf("Create (refused behind the scheduler): %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || !workload.OperationCapacityRefused(fromV1beta1Op(s.Operation)) {
		t.Fatalf("instance 0: got %+v want the refusal recorded behind the scheduler's token", s)
	}
	if s.Operation.Waiting != workload.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %q want the scheduler's token kept", s.Operation.Waiting)
	}
	if got := countRejectionEvents(rejectionEvents(recorder), workload.EventReasonInstanceQuotaBlocked); got != 0 {
		t.Errorf("quota events on a row the scheduler reports: got %d want 0", got)
	}
}

// TestCreate_Throttled_HonorsServerDelay: a 429 carrying Retry-After is
// the apiserver pacing us. The pass waits exactly that long and touches
// no status — the attempt is intact.
func TestCreate_Throttled_HonorsServerDelay(t *testing.T) {
	c, isvc, input, plan, target, recorder, writes := rejectionFixture(t, 1, func(podName string) error {
		return apierrors.NewTooManyRequests("apiserver is shedding load", 7)
	})

	result, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v (throttling is paced, not failed)", err)
	}
	if result.RequeueAfter != 7*time.Second {
		t.Errorf("RequeueAfter: got %v want the server's suggested 7s", result.RequeueAfter)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil {
		t.Fatalf("instance 0: got %+v want Creating with its operation intact", s)
	}
	if s.Operation.Reason != "" || s.LastFailure != nil {
		t.Errorf("instance 0: got reason=%q lastFailure=%+v want both unset (throttling writes no status)", s.Operation.Reason, s.LastFailure)
	}
	if len(*writes) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none", *writes)
	}
	if events := rejectionEvents(recorder); countRejectionEvents(events, workload.EventReasonInstanceRejected) != 0 {
		t.Errorf("rejection events: got %v want none", events)
	}
}

// TestCreate_Conflict_StaysAnOpaqueError: an unclassified rejection is not
// interpreted — the error surfaces and controller-runtime's own backoff
// owns the retry.
func TestCreate_Conflict_StaysAnOpaqueError(t *testing.T) {
	c, isvc, input, plan, target, recorder, _ := rejectionFixture(t, 1, func(podName string) error {
		return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, podName, errors.New("object was modified"))
	})

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err == nil {
		t.Fatal("Create: got nil want the conflict surfaced as an error")
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s != nil && s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Errorf("instance 0: got Failed want the attempt left alone for the caller's backoff")
	}
}

// irResourceVersion reads the persisted InferenceReplica's
// resourceVersion — the object every instance-status write lands on, so
// an unchanged value proves a pass wrote no status.
func irResourceVersion(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) string {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: irName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("read IR: %v", err)
	}
	return ir.ResourceVersion
}

func anyEventContains(events []string, want string) bool {
	for _, ev := range events {
		if strings.Contains(ev, want) {
			return true
		}
	}
	return false
}

// supersededFixture is one Instance whose Create attempt is pinned to a
// revision the target has since moved off, with the attempt half built:
// some of its pods exist, carrying the pinned revision's hash.
type supersededFixture struct {
	isvc     *v1beta1.InferenceService
	client   client.Client
	target   *appsv1.ControllerRevision
	plan     workload.ComponentPlan
	recorder *record.FakeRecorder
	priorOp  v1beta1.InstanceOperation
	// blocks seeds ObservedState.RetryBlocks for the pass.
	blocks []workload.RetryBlock
}

const supersededPriorRevision = "llama-70b-engine-bad0bad0"

// newSupersededFixture builds the half-built attempt. gang selects a
// two-Runner Instance whose worker is still missing; otherwise the Instance
// is single-pod with its one pod already created and not yet Ready.
func newSupersededFixture(t *testing.T, gang bool) *supersededFixture {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	priorOp := v1beta1.InstanceOperation{
		ID:             "create-0-1",
		Type:           v1beta1.InstanceOperationCreate,
		Step:           "CreatePods",
		TargetRevision: supersededPriorRevision,
		StartedAt:      metav1.NewTime(time.Now().Add(-time.Hour)),
		LastProgressAt: metav1.NewTime(time.Now().Add(-time.Hour)),
		Deadline:       metav1.NewTime(time.Now().Add(-30 * time.Minute)),
	}
	op := priorOp
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		PodCount:    1,
		Operation:   &op,
	})
	var pod *corev1.Pod
	plan := buildPlanSinglePodEngine(1)
	if gang {
		pod = gangPod(isvc, 0, "leader", 0, 1, false, false)
		plan = buildPlanGangEngine(workload.RestartPolicyNone)
		plan.Instances[0].Incarnation = 1
	} else {
		pod = podForInstance(isvc, 0, false, false)
	}
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(supersededPriorRevision)
	c := newFakeClient(t, isvc, ir, pod)
	return &supersededFixture{
		isvc:     isvc,
		client:   c,
		target:   &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-good0bad", Namespace: "prod"}},
		plan:     plan,
		recorder: record.NewFakeRecorder(8),
		priorOp:  priorOp,
	}
}

// run drives one Create pass against freshly observed status, the way the
// dispatcher does.
func (f *supersededFixture) run(t *testing.T) (writes int) {
	t.Helper()
	input := buildTestInput(f.isvc, f.client, workload.ComponentEngine)
	input.ObservedState.RetryBlocks = f.blocks
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		writes++
		return inner(ctx, idx, mutate)
	}
	if _, err := ops.Create(context.Background(), workload.Deps{Client: f.client, Recorder: f.recorder},
		input, f.plan, f.target); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return writes
}

// requireRetiredIntoFreshAttempt asserts the shape a superseded Create
// attempt is left in: still Creating under a new Create operation pinned to
// the current target, with nothing recorded against the revision it was
// building. retiredID is the identity of the attempt that was replaced.
func requireRetiredIntoFreshAttempt(t *testing.T, s *v1beta1.OMENativeInstanceStatus, retiredID, targetName string) {
	t.Helper()
	if s == nil {
		t.Fatalf("InstanceStatus missing")
	}
	if s.Phase != v1beta1.OMENativeInstanceCreating || s.LastFailure != nil {
		t.Fatalf("retired attempt: got {phase:%s lastFailure:%+v}, want Creating with no failure",
			s.Phase, s.LastFailure)
	}
	op := s.Operation
	if op == nil || op.Type != v1beta1.InstanceOperationCreate {
		t.Fatalf("retired attempt must be replaced by a fresh Create operation; got %+v", op)
	}
	if op.ID == retiredID {
		t.Errorf("Operation.ID = %q, want a new attempt identity", op.ID)
	}
	if op.TargetRevision != targetName {
		t.Errorf("Operation.TargetRevision = %q, want %q", op.TargetRevision, targetName)
	}
}

func TestCreate_SupersededAttempt_ReplacedByFreshAttempt(t *testing.T) {
	for _, gang := range []bool{false, true} {
		name := "single pod"
		if gang {
			name = "gang"
		}
		t.Run(name, func(t *testing.T) {
			f := newSupersededFixture(t, gang)

			if writes := f.run(t); writes != 1 {
				t.Fatalf("status writes in the retiring pass: got %d, want exactly 1", writes)
			}

			s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
			if s == nil {
				t.Fatalf("InstanceStatus disappeared")
			}
			if s.Phase != v1beta1.OMENativeInstanceCreating {
				t.Errorf("Phase = %q, want Creating: a superseded spec is not a failure of the retired revision", s.Phase)
			}
			if s.LastFailure != nil {
				t.Errorf("LastFailure = %+v, want nil", s.LastFailure)
			}
			op := s.Operation
			if op == nil {
				t.Fatalf("Operation cleared; the attempt must be replaced by a fresh one")
			}
			if op.Type != v1beta1.InstanceOperationCreate || op.Step != "CreatePods" {
				t.Errorf("Operation = {type:%s step:%s}, want {Create CreatePods}", op.Type, op.Step)
			}
			if op.ID == f.priorOp.ID {
				t.Errorf("Operation.ID = %q: the replacement must be a new attempt, not the retired one", op.ID)
			}
			if op.TargetRevision != f.target.Name {
				t.Errorf("Operation.TargetRevision = %q, want %q", op.TargetRevision, f.target.Name)
			}
			if !op.Deadline.After(f.priorOp.Deadline.Time) {
				t.Errorf("Operation.Deadline = %s, want re-armed past the retired attempt's %s",
					op.Deadline, f.priorOp.Deadline)
			}

			events := drainEvents(f.recorder)
			if len(events) != 1 {
				t.Fatalf("events: got %v, want exactly one", events)
			}
			if !strings.HasPrefix(events[0], "Normal ") {
				t.Errorf("event type: got %q, want Normal — a retired revision did not fail", events[0])
			}
			if !strings.Contains(events[0], supersededPriorRevision) || !strings.Contains(events[0], f.target.Name) {
				t.Errorf("event %q must name both the superseded and the new revision", events[0])
			}

			// A second pass observes the replacement it already wrote, so
			// the retirement neither re-fires nor re-announces itself.
			before := *op
			if writes := f.run(t); writes != 0 {
				t.Errorf("status writes in the second pass: got %d, want 0", writes)
			}
			for _, e := range drainEvents(f.recorder) {
				if strings.Contains(e, string(workload.EventReasonCreateAttemptSuperseded)) {
					t.Errorf("second pass re-announced the retirement: %q", e)
				}
			}
			after := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0).Operation
			if after == nil || after.ID != before.ID || after.TargetRevision != before.TargetRevision {
				t.Errorf("second pass replaced the attempt again: %+v", after)
			}
		})
	}
}

func TestCreate_FreshAttemptRecreatesRetiredPodsAtItsRevision(t *testing.T) {
	f := newSupersededFixture(t, true /* gang */)
	f.run(t)

	// The pass after the retirement owns the retired attempt's pods: they
	// carry a revision the pinned attempt is not converging to.
	f.run(t)
	pods := &corev1.PodList{}
	if err := f.client.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	targetHash := query.RevisionHashFromControllerRevisionName(f.target.Name)
	for i := range pods.Items {
		if got := pods.Items[i].Labels[query.LabelRevisionHash]; got == targetHash {
			continue
		}
		if pods.Items[i].DeletionTimestamp != nil {
			continue
		}
		t.Errorf("pod %s survived on revision hash %q; the fresh attempt must recreate it at %q",
			pods.Items[i].Name, pods.Items[i].Labels[query.LabelRevisionHash], targetHash)
	}
}

// onlyPod re-reads the fixture's single pod from the fake client.
func (f *supersededFixture) onlyPod(t *testing.T) *corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := f.client.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("fixture pods: got %d, want 1", len(pods.Items))
	}
	return &pods.Items[0]
}

func TestCreate_RuntimeReadyAttemptPromotesRatherThanRetiring(t *testing.T) {
	// A fully materialized, runtime-ready attempt is one promote away from
	// serving. Retiring it drops pods that are already routed and throws away
	// a warmed accelerator; the update machinery rolls the Instance to the new
	// target afterwards, with a surge and without a traffic gap.
	for _, tc := range []struct {
		name           string
		minReadyWindow int32
		wantPhase      v1beta1.OMENativeInstancePhase
	}{
		{"inside the availability window", 3600, v1beta1.OMENativeInstanceCreating},
		{"window elapsed", 0, v1beta1.OMENativeInstanceReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSupersededFixture(t, false /* gang */)
			f.plan.MinReadySeconds = tc.minReadyWindow
			pod := f.onlyPod(t)
			pod.Status = promotablePodForInstance(f.isvc, 0).Status
			if err := f.client.Status().Update(context.Background(), pod); err != nil {
				t.Fatalf("make the attempt's pod runtime-ready: %v", err)
			}

			f.run(t)

			s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
			if s.Phase != tc.wantPhase {
				t.Errorf("Phase = %q, want %q", s.Phase, tc.wantPhase)
			}
			if s.Operation != nil && s.Operation.TargetRevision == f.target.Name {
				t.Errorf("the attempt was retired one promote short of serving: %+v", s.Operation)
			}
			for _, e := range drainEvents(f.recorder) {
				if strings.Contains(e, string(workload.EventReasonCreateAttemptSuperseded)) {
					t.Errorf("retirement announced for a runtime-ready attempt: %q", e)
				}
			}
			if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				t.Errorf("the runtime-ready pod was deleted (%v)", err)
			}
		})
	}
}

func TestCreate_RetargetOntoBlockedRevisionDefersRetirement(t *testing.T) {
	// Replacing an attempt starts a fresh one, so the new target's RetryBlock
	// gates it exactly as it gates a first attempt. Retiring first and
	// discovering the block afterwards would tear down the old attempt and
	// then refuse to build anything.
	for _, tc := range []struct {
		name  string
		block workload.RetryBlock
	}{
		{"held", workload.RetryBlock{State: workload.RetryBlockHeld}},
		{"backing off", workload.RetryBlock{State: workload.RetryBlockBackoff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSupersededFixture(t, false /* gang */)
			block := tc.block
			block.TargetRevision = f.target.Name
			if block.State == workload.RetryBlockBackoff {
				next := metav1.NewTime(time.Now().Add(time.Hour))
				block.NextRetryAt = &next
			}
			f.blocks = []workload.RetryBlock{block}

			if writes := f.run(t); writes != 0 {
				t.Errorf("status writes: got %d, want 0 while the new target is denied", writes)
			}
			s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
			if s.Operation == nil || s.Operation.TargetRevision != supersededPriorRevision {
				t.Errorf("the old attempt was retired onto a denied target: %+v", s.Operation)
			}
			if events := drainEvents(f.recorder); len(events) != 0 {
				t.Errorf("events: got %v, want none — nothing happened", events)
			}
			pods := &corev1.PodList{}
			if err := f.client.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			targetHash := query.RevisionHashFromControllerRevisionName(f.target.Name)
			for i := range pods.Items {
				if pods.Items[i].Labels[query.LabelRevisionHash] == targetHash {
					t.Errorf("pod %s was built at the denied revision", pods.Items[i].Name)
				}
			}
		})
	}
}

func TestCreate_PromotedInstanceRepairIsNotRetired(t *testing.T) {
	// A promoted Instance whose pod is being rebuilt runs a Create attempt on
	// a row that records a RunningRevision. Its pods belong to a serving
	// materialization, so rolling them to a new target is the update
	// machinery's business — and the Create pass's own cleanup declines them
	// forever, which would leave the retirement announced and unfinished.
	f := newSupersededFixture(t, false /* gang */)
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: "prod", Name: irName(f.isvc, workload.ComponentEngine)}
	if err := f.client.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir.Status.InstanceStatuses[0].RunningRevision = supersededPriorRevision
	if err := f.client.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed the running revision: %v", err)
	}

	f.run(t)

	s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
	if s.Operation == nil || s.Operation.TargetRevision != supersededPriorRevision {
		t.Errorf("a promoted Instance's repair was retired by the Create pass: %+v", s.Operation)
	}
	for _, e := range drainEvents(f.recorder) {
		if strings.Contains(e, string(workload.EventReasonCreateAttemptSuperseded)) {
			t.Errorf("retirement announced for a row the cleanup declines: %q", e)
		}
	}
}

func TestCreate_RetargetDefersWhileAnotherAttemptRetries(t *testing.T) {
	// A RetryInProgress block admits one attempt at a time. An in-flight
	// Create is allowed to carry no TargetRevision at all, so a
	// revision-scoped reading of "is anyone attempting this" answers no and
	// lets a second attempt start beside the first.
	f := newSupersededFixture(t, false /* gang */)
	f.plan.Replicas = 2
	f.plan.Instances = append(f.plan.Instances, workload.InstancePlan{
		Index:       1,
		Incarnation: 1,
		Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
	})
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: "prod", Name: irName(f.isvc, workload.ComponentEngine)}
	if err := f.client.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
		Index:       1,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			ID:   "create-1-1",
			Type: v1beta1.InstanceOperationCreate,
			Step: "CreatePods",
		},
	})
	if err := f.client.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed the sibling attempt: %v", err)
	}
	f.blocks = []workload.RetryBlock{{
		TargetRevision: f.target.Name,
		State:          workload.RetryBlockRetryInProgress,
	}}

	f.run(t)

	s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
	if s.Operation == nil || s.Operation.TargetRevision != supersededPriorRevision {
		t.Errorf("retired onto a revision another attempt already holds: %+v", s.Operation)
	}
	for _, e := range drainEvents(f.recorder) {
		if strings.Contains(e, string(workload.EventReasonCreateAttemptSuperseded)) {
			t.Errorf("retirement announced while the revision is one-at-a-time: %q", e)
		}
	}
}

// unknownPhasePod is a pod whose kubelet has stopped reporting: phase
// Unknown, bound to node, neither terminal nor ready.
func unknownPhasePod(pod *corev1.Pod, node string) *corev1.Pod {
	pod.Spec.NodeName = node
	pod.Status.Phase = corev1.PodUnknown
	pod.Status.Conditions = nil
	return pod
}

func unknownNodeUnreachable(name string, since time.Duration) *corev1.Node {
	added := metav1.NewTime(time.Now().Add(-since))
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionUnknown,
			LastTransitionTime: added,
		}}},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{
			Key:       corev1.TaintNodeUnreachable,
			Effect:    corev1.TaintEffectNoExecute,
			TimeAdded: &added,
		}}},
	}
}

func unknownNodeReady(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
		}}},
	}
}

// restartingAtIncarnation seeds an Instance mid-restart at incarnation 2
// with a live attempt deadline, the shape Phase B of a restart runs in.
func restartingAtIncarnation(ir *v1beta1.InferenceReplica) {
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			ID: "restart-0-1", Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "pod lost",
			StartedAt:      metav1.Now(),
			LastProgressAt: metav1.Now(),
			Deadline:       metav1.NewTime(time.Now().Add(time.Hour)),
		},
	}
}

// TestRestart_UnknownPhasePodHoldsWithoutForceDeletePolicy: a pod at the
// bumped incarnation whose phase went Unknown keeps its stable name — a
// returning kubelet may still be running it, so recycling the name could
// double-run the container. With no force-delete policy configured
// nothing can prove the node dead, so the repair reports the wait
// instead of spending its deadline in silence: the Restart operation
// carries the NodeUnknown token, LastFailure names the pod and its node,
// and the pod is left alone.
func TestRestart_UnknownPhasePodHoldsWithoutForceDeletePolicy(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	restartingAtIncarnation(ir)
	quiet := unknownPhasePod(podAtIncarnation(isvc, 0, 2, false, false), "node-a")
	c := newFakeClient(t, isvc, ir, quiet, unknownNodeUnreachable("node-a", time.Hour))
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: rec}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	done, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("Restart must not complete while a pod of the rebuild is in phase Unknown")
	}
	if !podExists(c, quiet) {
		t.Fatalf("pod %s must not be deleted without a force-delete policy", quiet.Name)
	}
	if n := countRejectionEvents(drainEvents(rec), workload.EventReasonPodForceDeleted); n != 0 {
		t.Fatalf("PodForceDeleted events: got %d want 0 (no policy configured)", n)
	}
}

// TestRestart_UnknownPhasePodForceDeletedOnProvenNodeDeath: with a
// force-delete policy configured and the node unreachable past the
// configured threshold, the same sweep that clears stuck-Terminating
// pods frees the name — grace zero, UID-preconditioned — and the restart
// rebuilds it on a later pass.
func TestRestart_UnknownPhasePodForceDeletedOnProvenNodeDeath(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	restartingAtIncarnation(ir)
	quiet := unknownPhasePod(podAtIncarnation(isvc, 0, 2, false, false), "node-a")
	c := newFakeClient(t, isvc, ir, quiet, unknownNodeUnreachable("node-a", time.Hour))
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: rec}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ForceDelete = &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if podExists(c, quiet) {
		t.Fatalf("pod %s must be force-deleted once its node is provably gone", quiet.Name)
	}
	if n := countRejectionEvents(drainEvents(rec), workload.EventReasonPodForceDeleted); n != 1 {
		t.Fatalf("PodForceDeleted events: got %d want 1", n)
	}

	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	plan = buildPlanSinglePodEngineForRestart(c, isvc)
	if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost"); err != nil {
		t.Fatalf("Restart pass 2: %v", err)
	}
	got := listPods(t, c, "prod")
	if len(got) != 1 || got[0].Status.Phase == corev1.PodUnknown {
		t.Fatalf("expected one freshly recreated pod, got %+v", got)
	}
}

// TestRestart_UnknownPhasePodOnLiveNodeIsNeverDeleted: node-death
// evidence, not the pod's phase, authorizes the delete. A node still
// posting Ready=True means the kubelet is merely slow, so the name is
// left occupied and the wait stays visible.
func TestRestart_UnknownPhasePodOnLiveNodeIsNeverDeleted(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	restartingAtIncarnation(ir)
	quiet := unknownPhasePod(podAtIncarnation(isvc, 0, 2, false, false), "node-a")
	c := newFakeClient(t, isvc, ir, quiet, unknownNodeReady("node-a"))
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ForceDelete = &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "pod lost"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !podExists(c, quiet) {
		t.Fatalf("pod %s must survive: a Ready node vetoes every node-death branch", quiet.Name)
	}
}

// TestCreate_UnknownPhasePodRoutesToTheForceDeleteSweep: the create's
// missing-pod diff reads an Unknown pod as present, so without this the
// attempt polls a name no kubelet is reporting on until its deadline.
// The same node-death evidence frees it.
func TestCreate_UnknownPhasePodRoutesToTheForceDeleteSweep(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	quiet := unknownPhasePod(podForInstance(isvc, 0, false, false), "node-a")
	c := newFakeClient(t, isvc, quiet, unknownNodeUnreachable("node-a", time.Hour))
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	plan := buildPlanSinglePodEngine(1)

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ForceDelete = &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if podExists(c, quiet) {
		t.Fatalf("pod %s must be force-deleted once its node is provably gone", quiet.Name)
	}
}

// TestCreate_UnknownPhasePodKeepsThePassRequeuing: a held Instance is
// dropped from the pass's selection, which on its own would leave the
// pass looking converged and asking for no requeue at all. The wait ends
// on a clock over a node that emits no events, so the pass has to keep
// waking itself or the sweep fires only when something unrelated pokes
// the owner.
func TestCreate_UnknownPhasePodKeepsThePassRequeuing(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	quiet := unknownPhasePod(podForInstance(isvc, 0, false, false), "node-a")
	policy := &workload.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	// Unreachable for less than the threshold: the name stays held and
	// the evidence turns actionable only once the remainder elapses.
	young := time.Minute
	c := newFakeClient(t, isvc, quiet, unknownNodeUnreachable("node-a", young))
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	plan := buildPlanSinglePodEngine(1)

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ForceDelete = policy
	res, err := ops.Create(context.Background(), deps, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !podExists(c, quiet) {
		t.Fatalf("pod %s must survive: its node has not been unreachable long enough", quiet.Name)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter: got %v want a positive delay; nothing else wakes this pass", res.RequeueAfter)
	}
	if remaining := policy.NodeUnreachableThreshold - young; res.RequeueAfter > remaining {
		t.Errorf("RequeueAfter: got %v want at most the threshold remainder %v", res.RequeueAfter, remaining)
	}
}
