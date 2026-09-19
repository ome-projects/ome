package ops_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func fromV1beta1Op(op *v1beta1.InstanceOperation) *workload.InstanceOperation {
	if op == nil {
		return nil
	}
	out := &workload.InstanceOperation{
		ID:             op.ID,
		Type:           workload.InstanceOperationType(op.Type),
		Step:           op.Step,
		StartedAt:      op.StartedAt,
		LastProgressAt: op.LastProgressAt,
		Deadline:       op.Deadline,
		RetryCount:     op.RetryCount,
		TargetRevision: op.TargetRevision,
		Reason:         op.Reason,
		Waiting:        op.Waiting,
		FromNode:       op.FromNode,
		RequestUUID:    op.RequestUUID,
	}
	if op.SurgeIndex != nil {
		s := *op.SurgeIndex
		out.SurgeIndex = &s
	}
	if op.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), op.HintTargetNodes...)
	}
	return out
}

func toV1beta1Op(op *workload.InstanceOperation) *v1beta1.InstanceOperation {
	if op == nil {
		return nil
	}
	out := &v1beta1.InstanceOperation{
		ID:             op.ID,
		Type:           v1beta1.InstanceOperationType(op.Type),
		Step:           op.Step,
		StartedAt:      op.StartedAt,
		LastProgressAt: op.LastProgressAt,
		Deadline:       op.Deadline,
		RetryCount:     op.RetryCount,
		TargetRevision: op.TargetRevision,
		Reason:         op.Reason,
		Waiting:        op.Waiting,
		FromNode:       op.FromNode,
		RequestUUID:    op.RequestUUID,
	}
	if op.SurgeIndex != nil {
		s := *op.SurgeIndex
		out.SurgeIndex = &s
	}
	if op.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), op.HintTargetNodes...)
	}
	return out
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
	if result.RequeueAfter != 0 {
		t.Errorf("expected no Requeue when all pods Ready, got %+v", result)
	}

	// Pod should have ome.io/serving=True now.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if !podreadiness.IsServing(got) {
		t.Errorf("pod %s ome.io/serving not flipped: conditions=%+v", got.Name, got.Status.Conditions)
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
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("superseded pinned Create must retire; got %+v", s)
	}
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
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("superseded unpinned Create must retire; got %+v", s)
	}
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
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("superseded gated Create must retire; got %+v", s)
	}
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
	// Pre-seed a runtime-ready, serving pod so reconcileInstance jumps
	// straight to the promote step. The pod's rev-hash matches target's
	// suffix — the production-normal case (createMissingPods stamps the
	// label from revisionHashFromTarget(target)).
	pod := podForInstance(isvc, 0, true /* ready */, true /* serving */)
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
// X-2 bump-during-bump guard in Create.reconcileInstance:
// when existing pods are runtime-ready but carry a different revision
// hash from target (e.g., they were created by an earlier surge cycle
// pinned to a now-superseded target), the promote MUST NOT stamp
// RunningRevision=target.Name — that would falsely advertise the pods
// as on target. patchInstanceStatusReady (no RunningRevision write)
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
	// Pre-seed a runtime-ready, serving pod labeled with the PRIOR
	// revision's hash (not target's). reconcileInstance will find the
	// pod, see it's runtime-ready, and reach the promote step.
	pod := podForInstance(isvc, 0, true /* ready */, true /* serving */)
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
		t.Errorf("RunningRevision: got %q want %q (X-2 guard — Create promote must NOT clobber RunningRevision when pods are off-target)",
			s.RunningRevision, priorRev)
	}
}

// Inverse: when target is nil (scale-down-only reconciles), Create
// promotes without writing RunningRevision.
func TestCreate_PromoteWithNilTargetSkipsRunningRevision(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	pod := podForInstance(isvc, 0, true, true)
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
// race. When Create wins it stamps Phase=Creating, which latches the
// outcome by making DetectRestartTrigger skip on every later pass — so
// RecreateInstance silently degrades to "recreate just the dead pod" (the
// leader and surviving workers keep their incarnation). Empirically
// non-deterministic: the same 3-pod gang recovered the whole gang in one
// run and only the dead pod in another.
//
// Fix: Create defers partial-Instance recovery to Restart under
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
