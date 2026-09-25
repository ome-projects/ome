package ops

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
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// White-box test scaffolding (package ops) for the Surge / Update
// state-machine tests. Distinct from the create_test.go fixtures
// (package ops_test) because both packages need similar helpers but
// can't share them across the package boundary.

// testRevisionHashLegacy is the synthetic revision hash stamped on
// test pods.
const testRevisionHashLegacy = "testrev1"

// legacyMinimalISVC builds an ISVC with the minimum metadata Update /
// Surge state-machine tests need.
func legacyMinimalISVC(name, ns string, replicas int) *v1beta1.InferenceService {
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

// legacyTestPodLabels reproduces the label set Render stamps on every
// emitted pod.
func legacyTestPodLabels(isvc string, component workload.ComponentType, instanceIdx int32, runner string, incarnation int64, ordinal int32) map[string]string {
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

// legacyResetExpectations re-seats workload.DefaultExpectations so
// back-to-back tests don't observe prior ExpectCreates entries.
func legacyResetExpectations(t *testing.T) {
	t.Helper()
	workload.DefaultExpectations = workload.NewExpectations()
}

// legacyNewFakeClient builds a controller-runtime fake client with
// the schemes the surge / update tests touch.
func legacyNewFakeClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(initObjs...).
		Build()
}

// legacyIRName is the InferenceReplica name the workload helpers persist
// per-instance status on. Mirrors irprojector.InferenceReplicaName
// (isvcName-component); reproduced here to avoid the import.
func legacyIRName(isvc *v1beta1.InferenceService, component workload.ComponentType) string {
	return isvc.Name + "-" + string(component)
}

// legacyInstanceIR builds the InferenceReplica that carries the given
// per-instance statuses for (isvc, component). The IR is the source of
// truth for instance detail (the ISVC carries none), so fixtures seed it
// here and pass the returned IR to legacyNewFakeClient alongside the ISVC.
func legacyInstanceIR(isvc *v1beta1.InferenceService, component workload.ComponentType, insts ...v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: isvc.Namespace,
			Name:      legacyIRName(isvc, component),
		},
		Status: v1beta1.InferenceReplicaStatus{InstanceStatuses: insts},
	}
}

// legacyInstanceStatusesOnIR re-reads the InferenceReplica and returns its
// persisted per-instance statuses, the authoritative copy assertions check.
// Returns nil when the IR does not exist.
func legacyInstanceStatusesOnIR(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) []v1beta1.OMENativeInstanceStatus {
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: legacyIRName(isvc, component)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		return nil
	}
	return ir.Status.InstanceStatuses
}

// legacyPodForInstance fabricates a pod matching what Render() would
// produce for the given (ISVC, instance) pair.
func legacyPodForInstance(isvc *v1beta1.InferenceService, instanceIdx int32, ready, serving bool) *corev1.Pod {
	labels := legacyTestPodLabels(isvc.Name, workload.ComponentEngine, instanceIdx, "default", 1, 0)
	labels[query.LabelRevisionHash] = testRevisionHashLegacy
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

// legacyFoldServingIntoPodReady is the fake kubelet a unit fixture lacks:
// once the controller's serving gate is satisfied, kubelet folds it into the
// pod's Ready condition, which is what the shared promote bar reads. readyAt
// is the Ready transition the minReadySeconds window measures from.
func legacyFoldServingIntoPodReady(t *testing.T, c client.Client, pod *corev1.Pod, readyAt time.Time) {
	t.Helper()
	live := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
		t.Fatalf("get pod before folding PodReady: %v", err)
	}
	live.Status.Conditions = append(live.Status.Conditions, corev1.PodCondition{
		Type:               corev1.PodReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(readyAt),
	})
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("fold serving gate into PodReady: %v", err)
	}
}

// legacyPodAtIncarnation extends legacyPodForInstance to stamp a
// specific incarnation label.
func legacyPodAtIncarnation(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ready, serving bool) *corev1.Pod {
	pod := legacyPodForInstance(isvc, instanceIdx, ready, serving)
	pod.Labels[query.LabelInstanceIncarnation] = fmt.Sprintf("%d", incarnation)
	return pod
}

// legacyTestInput projects the ISVC + Component onto a
// workload.ReconcileInput. MutateInstance round-trips through the
// fake client's Status().Update so tests can assert on the persisted
// ISVC.Status. The closure also mirrors the just-committed Operation
// back onto the in-memory ISVC.Status snapshot the test holds a
// pointer to, so subsequent passes within the same reconcile observe
// the just-stamped Phase / Operation without re-reading the
// apiserver.
func legacyTestInput(isvc *v1beta1.InferenceService, c client.Client, component workload.ComponentType) workload.ReconcileInput {
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
			InstanceStatuses: legacyInstanceStatuses(c, isvc, component),
		},
		MutateInstance: legacyMutateInstance(c, isvc, component),
		// The dispatcher cadence a chart-configured deployment supplies.
		Requeue: workload.RequeueIntervals{Operation: 5 * time.Second, Gate: 3 * time.Second},
	}
}

// legacyInstanceStatuses reads the authoritative InferenceReplica and
// converts its per-instance statuses into the workload-owned mirror. The
// IR is the source of truth (the ISVC carries no projection of it).
func legacyInstanceStatuses(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) []workload.InstanceStatus {
	var out []workload.InstanceStatus
	for _, s := range legacyInstanceStatusesOnIR(c, isvc, component) {
		out = append(out, workload.InstanceStatus{
			Index:           s.Index,
			Incarnation:     s.Incarnation,
			Phase:           workload.InstancePhase(s.Phase),
			RunningRevision: s.RunningRevision,
			TargetRevision:  s.TargetRevision,
			ActiveOrdinal:   s.ActiveOrdinal,
			Operation:       legacyFromV1beta1Op(s.Operation),
			LastFailure:     legacyFromV1beta1Termination(s.LastFailure),
		})
	}
	return out
}

// legacyMutateInstance is the test-side persistence layer for
// ReconcileInput.MutateInstance. It reads-modifies-writes the
// InferenceReplica status (the source of truth), mirroring production's
// IR-side write-back. Creates the IR on first write when a fixture didn't
// pre-seed one (fresh-create tests). Skips retry.RetryOnConflict (fake
// client has no optimistic-concurrency failures to retry).
func legacyMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: legacyIRName(isvc, component)}
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
				Operation:       legacyFromV1beta1Op(s.Operation),
				LastFailure:     legacyFromV1beta1Termination(s.LastFailure),
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
			Operation:       legacyToV1beta1Op(w.Operation),
			LastFailure:     legacyToV1beta1Termination(w.LastFailure),
		}
		if pos == -1 {
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, updated)
		} else {
			ir.Status.InstanceStatuses[pos] = updated
		}
		if create {
			// Create seeds the object; status subresource is written separately.
			bare := &v1beta1.InferenceReplica{ObjectMeta: ir.ObjectMeta}
			if err := c.Create(ctx, bare); err != nil {
				return fmt.Errorf("create IR: %w", err)
			}
			bare.Status = ir.Status
			ir = bare
		}
		if err := c.Status().Update(ctx, ir); err != nil {
			return err
		}
		return nil
	}
}

// legacyFromV1beta1Termination / legacyToV1beta1Termination round-trip
// LastFailure so fixtures observe the failure diagnostics the
// dispositions record, not just the phase flip.
func legacyFromV1beta1Termination(t *v1beta1.InstanceTermination) *workload.InstanceTermination {
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

func legacyToV1beta1Termination(t *workload.InstanceTermination) *v1beta1.InstanceTermination {
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

// legacyFromV1beta1Op / legacyToV1beta1Op mirror the production converter
// by hand: the internal test package cannot import v1beta1convert without a
// cycle. Every field the state machines read has to be listed, or a fixture
// silently loses the one under test.
func legacyFromV1beta1Op(op *v1beta1.InstanceOperation) *workload.InstanceOperation {
	if op == nil {
		return nil
	}
	return &workload.InstanceOperation{
		ID:                op.ID,
		Type:              workload.InstanceOperationType(op.Type),
		Step:              op.Step,
		StartedAt:         op.StartedAt,
		LastProgressAt:    op.LastProgressAt,
		Deadline:          op.Deadline,
		TargetRevision:    op.TargetRevision,
		Strategy:          workload.UpdateStrategyType(op.Strategy),
		Reason:            op.Reason,
		Waiting:           op.Waiting,
		CapacityRefusedAt: op.CapacityRefusedAt,
		RequestUUID:       op.RequestUUID,
		// SurgeIndex round-trips so gang-surge fixtures (Op.Step=Surge with
		// a SurgeIndex pointer) survive the projection. Without it the
		// gangSurgeUpdate "surging" detection sees SurgeIndex==nil and
		// re-allocates a fresh surge index instead of resuming the
		// in-flight one.
		SurgeIndex: op.SurgeIndex,
	}
}

func legacyToV1beta1Op(op *workload.InstanceOperation) *v1beta1.InstanceOperation {
	if op == nil {
		return nil
	}
	return &v1beta1.InstanceOperation{
		ID:                op.ID,
		Type:              v1beta1.InstanceOperationType(op.Type),
		Step:              op.Step,
		StartedAt:         op.StartedAt,
		LastProgressAt:    op.LastProgressAt,
		Deadline:          op.Deadline,
		TargetRevision:    op.TargetRevision,
		Strategy:          string(op.Strategy),
		Reason:            op.Reason,
		Waiting:           op.Waiting,
		CapacityRefusedAt: op.CapacityRefusedAt,
		RequestUUID:       op.RequestUUID,
		SurgeIndex:        op.SurgeIndex,
	}
}

// legacyBoolPtr is a tiny pointer helper used by
// MarkNotReadyDuringLifecycle / similar bool-pointer test fixtures.
func legacyBoolPtr(v bool) *bool { return &v }

// legacyTargetSpecImage builds the basic single-container PodSpec
// used as a rollout target throughout the Update / SurgeThenDrain
// tests.
func legacyTargetSpecImage(image string) *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Image: image},
		},
	}
}

// legacyRunningPodAtRevision shapes a pod the controller would
// consider running at revName — labeled with the appropriate
// incarnation, image, runtime-ready + serving, and PodReady because
// kubelet has folded that satisfied gate into the Ready condition.
func legacyRunningPodAtRevision(isvc *v1beta1.InferenceService, instIdx int32, incarnation int64, image string) *corev1.Pod {
	pod := legacyPodAtIncarnation(isvc, instIdx, incarnation, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: image}}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.PodReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	})
	return pod
}

// legacyISVCReadyAtIncarnation builds an ISVC plus its engine
// InferenceReplica whose InstanceStatus for index 0 is Phase=Ready at the
// given Incarnation — the steady-state from which an Update trigger fires.
// Per-instance detail lives on the returned IR (the source of truth);
// callers seed both into legacyNewFakeClient.
func legacyISVCReadyAtIncarnation(name, ns string, incarnation int64) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	isvc := legacyMinimalISVC(name, ns, 1)
	ir := legacyInstanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
	)
	return isvc, ir
}

// legacySliceWithEndpoint builds an EndpointSlice with one endpoint
// targeting pod with the given Ready condition. Used by drain
// fixtures across the Update / Recreate flows.
func legacySliceWithEndpoint(namespace, sliceName, serviceName string, pod *corev1.Pod, ready bool) *discoveryv1.EndpointSlice {
	readyPtr := ready
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sliceName,
			Namespace: namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: &readyPtr,
				},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			},
		},
	}
}

// legacyEngineRevisionKey returns the per-Component revision.Key the
// production ISVC adapter emits.
func legacyEngineRevisionKey(isvc *v1beta1.InferenceService) revision.Key {
	return revision.Key{
		Namespace: isvc.Namespace,
		Name:      isvc.Name + "-" + string(workload.ComponentEngine),
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(workload.ComponentEngine),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// legacyEnsureTargetCR creates the deterministic CR for spec in the
// fake client so Update has something to dispatch against.
func legacyEnsureTargetCR(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, spec *corev1.PodSpec) *appsv1.ControllerRevision {
	t.Helper()
	return legacyEnsureTargetCRWithMeta(t, c, isvc, spec, nil)
}

// legacyStampPodRevisionHash makes a pod fixture represent a fully observed
// in-place rollout at the supplied ControllerRevision.
func legacyStampPodRevisionHash(t *testing.T, c client.Client, pod *corev1.Pod, revisionName string) {
	t.Helper()
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err != nil {
		t.Fatalf("get pod for revision hash: %v", err)
	}
	fresh.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(revisionName)
	if err := c.Update(context.Background(), fresh); err != nil {
		t.Fatalf("stamp pod revision hash: %v", err)
	}
}

// legacyEnsureTargetCRWithMeta is legacyEnsureTargetCR's PodMeta-aware
// sibling. Captures user-intent annotations via meta so the in-place
// annotation-propagation regression tests can verify those values
// reach the live pod after an in-place rollout. nil meta degrades to
// the no-meta behavior.
func legacyEnsureTargetCRWithMeta(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, spec *corev1.PodSpec, meta *metav1.ObjectMeta) *appsv1.ControllerRevision {
	t.Helper()
	cr, _, err := revision.EnsureControllerRevision(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		spec, meta, nil, isvc.UID,
	)
	if err != nil {
		t.Fatalf("revision.EnsureControllerRevision: %v", err)
	}
	return cr
}

// legacySeedRunningRevision creates a ControllerRevision capturing
// spec as the Instance's recorded running template and writes its
// name onto InstanceStatus[idx].RunningRevision. The Update state
// machine's inPlaceEligible reads the recorded baseline (NOT the live
// pod) when deciding image-only-vs-bigger-diff.
func legacySeedRunningRevision(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType, idx int32, spec *corev1.PodSpec) {
	t.Helper()
	legacySeedRunningRevisionWithMeta(t, c, isvc, component, idx, spec, nil)
}

// legacySeedRunningRevisionWithMeta captures the previous-revision
// PodMeta so the in-place annotation diff has a baseline to subtract
// from.
func legacySeedRunningRevisionWithMeta(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType, idx int32, spec *corev1.PodSpec, meta *metav1.ObjectMeta) {
	t.Helper()
	cr, _, err := revision.EnsureControllerRevision(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		spec, meta, nil, isvc.UID,
	)
	if err != nil {
		t.Fatalf("legacySeedRunningRevisionWithMeta: %v", err)
	}
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: legacyIRName(isvc, component)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == idx {
			ir.Status.InstanceStatuses[i].RunningRevision = cr.Name
			if err := c.Status().Update(context.Background(), ir); err != nil {
				t.Fatalf("set RunningRevision: %v", err)
			}
			return
		}
	}
	t.Fatalf("no InstanceStatus for idx=%d to attach RunningRevision", idx)
}

// legacyComponentPlan builds the single-pod engine ComponentPlan
// used by the Update / SurgeThenDrain tests. Reproduced inline
// because workload/ops tests can't import omenative/core. strategy
// controls the UpdateStrategy.Type.
func legacyComponentPlan(strategy workload.UpdateStrategyType, inPlaceStrategy *workload.InPlaceUpdateStrategy) workload.ComponentPlan {
	return workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: workload.UpdateStrategy{
			Type:                  strategy,
			InPlaceUpdateStrategy: inPlaceStrategy,
		},
	}
}

// legacyMultiPodComponentPlan is legacyComponentPlan's multi-pod
// sibling — used by the multi-pod SurgeThenDrain fallback tests where
// the chooser must downgrade surge / in-place to recreate.
func legacyMultiPodComponentPlan(strategy workload.UpdateStrategyType) workload.ComponentPlan {
	return workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners: []workload.RunnerPlan{
				{Name: "leader", Size: 1},
				{Name: "worker", Size: 1},
			},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: workload.UpdateStrategy{
			Type: strategy,
		},
	}
}

// legacyTestDeps builds the standard workload.Deps. Recorder is nil —
// event helpers are nil-safe and tests don't assert on the event
// stream.
func legacyTestDeps(c client.Client) workload.Deps {
	return workload.Deps{Client: c}
}

// Keep the revision import live even when partial test builds don't
// reference any helper that uses it.
var _ = revision.Key{}

// legacyRemoveInstance is the test-side persistence layer for
// ReconcileInput.RemoveInstance. Reads from and writes to the IR.
func legacyRemoveInstance(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: legacyIRName(isvc, component)}
		if err := c.Get(ctx, key, ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("get IR: %w", err)
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		if pos == -1 {
			return false, nil
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses[:pos], ir.Status.InstanceStatuses[pos+1:]...)
		if err := c.Status().Update(ctx, ir); err != nil {
			return false, fmt.Errorf("update IR: %w", err)
		}
		workload.DefaultExpectations.Forget(isvc.Namespace, isvc.Name, component, idx)
		return true, nil
	}
}

type terminalMutationStore struct {
	ownerUID   k8stypes.UID
	statuses   map[int32]workload.InstanceStatus
	retryBlock map[string]workload.RetryBlock
	readErr    error
	applyErr   error
	applyCall  int
	writes     int
	log        *[]string
}

func (s *terminalMutationStore) apply(
	_ context.Context,
	mutations []workload.InstanceMutation,
	targetRevision string,
	mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition,
) error {
	s.applyCall++
	if s.log != nil {
		*s.log = append(*s.log, "status")
	}
	if s.readErr != nil {
		return s.readErr
	}
	snapshot := workload.InstanceMutationSnapshot{
		OwnerUID:  s.ownerUID,
		Instances: cloneTerminalStatuses(s.statuses),
	}
	for _, mutation := range mutations {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			return workload.ErrStatusMutationPrecondition
		}
	}

	next := cloneTerminalStatuses(s.statuses)
	type callback struct {
		fn      func(*workload.InstanceStatus, *workload.InstanceStatus)
		before  *workload.InstanceStatus
		current *workload.InstanceStatus
	}
	callbacks := make([]callback, 0, len(mutations))
	changed := false
	for _, mutation := range mutations {
		current, found := next[mutation.Index]
		if mutation.Remove {
			if !found {
				continue
			}
			before := cloneTerminalStatus(current)
			if mutation.Precondition != nil && !mutation.Precondition(&before) {
				continue
			}
			delete(next, mutation.Index)
			changed = true
			if mutation.OnCommit != nil {
				callbacks = append(callbacks, callback{fn: mutation.OnCommit, before: &before})
			}
			continue
		}
		if !found {
			current = workload.InstanceStatus{Index: mutation.Index}
		}
		before := cloneTerminalStatus(current)
		if mutation.Precondition != nil && !mutation.Precondition(&before) {
			continue
		}
		if !mutation.Mutate(&current) {
			continue
		}
		next[mutation.Index] = current
		changed = true
		if mutation.OnCommit != nil {
			committed := cloneTerminalStatus(current)
			callbacks = append(callbacks, callback{fn: mutation.OnCommit, before: &before, current: &committed})
		}
	}
	nextRetryBlocks := cloneTerminalRetryBlocks(s.retryBlock)
	retryBlockChanged := false
	if mutateRetryBlock != nil {
		block, found := nextRetryBlocks[targetRevision]
		if !found {
			block = workload.RetryBlock{TargetRevision: targetRevision}
		}
		switch mutateRetryBlock(&block) {
		case workload.RetryBlockPersist:
			block.TargetRevision = targetRevision
			nextRetryBlocks[targetRevision] = block
			retryBlockChanged = true
		case workload.RetryBlockRemove:
			if found {
				delete(nextRetryBlocks, targetRevision)
				retryBlockChanged = true
			}
		}
	}
	if !changed && !retryBlockChanged {
		return nil
	}
	if s.applyErr != nil {
		return s.applyErr
	}
	s.statuses = next
	s.retryBlock = nextRetryBlocks
	s.writes++
	for _, callback := range callbacks {
		callback.fn(callback.before, callback.current)
	}
	return nil
}

func cloneTerminalRetryBlocks(in map[string]workload.RetryBlock) map[string]workload.RetryBlock {
	out := make(map[string]workload.RetryBlock, len(in))
	for revision, block := range in {
		copy := block
		if block.NextRetryAt != nil {
			next := *block.NextRetryAt
			copy.NextRetryAt = &next
		}
		if block.FirstFailureAt != nil {
			first := *block.FirstFailureAt
			copy.FirstFailureAt = &first
		}
		if block.LastFailureAt != nil {
			last := *block.LastFailureAt
			copy.LastFailureAt = &last
		}
		out[revision] = copy
	}
	return out
}

func cloneTerminalStatuses(in map[int32]workload.InstanceStatus) map[int32]workload.InstanceStatus {
	out := make(map[int32]workload.InstanceStatus, len(in))
	for index, status := range in {
		out[index] = cloneTerminalStatus(status)
	}
	return out
}

func cloneTerminalStatus(status workload.InstanceStatus) workload.InstanceStatus {
	out := status
	if status.Operation != nil {
		operation := *status.Operation
		operation.HintTargetNodes = append([]string(nil), status.Operation.HintTargetNodes...)
		if status.Operation.SurgeIndex != nil {
			surge := *status.Operation.SurgeIndex
			operation.SurgeIndex = &surge
		}
		out.Operation = &operation
	}
	out.NodesOccupied = append([]string(nil), status.NodesOccupied...)
	out.Conditions = append([]metav1.Condition(nil), status.Conditions...)
	return out
}

func terminalStatusFixture(index int32) workload.InstanceStatus {
	surge := int32(9)
	started := metav1.NewTime(time.Date(2026, time.August, 15, 10, 0, 0, 987654321, time.UTC))
	return workload.InstanceStatus{
		Index:           index,
		Incarnation:     7,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "rev-old",
		TargetRevision:  "rev-new",
		ActiveOrdinal:   1,
		PodCount:        8,
		ReadyPodCount:   7,
		Operation: &workload.InstanceOperation{
			ID:              "gang-update-7",
			Type:            workload.InstanceOperationUpdate,
			Step:            workload.UpdateStepSurge,
			RequestUUID:     "request-a",
			TargetRevision:  "rev-new",
			RetryCount:      2,
			Reason:          "rollout",
			FromNode:        "node-a",
			HintTargetNodes: []string{"node-b", "node-c"},
			SurgeIndex:      &surge,
			StartedAt:       started,
			LastProgressAt:  started,
			Deadline:        metav1.NewTime(started.Add(time.Hour)),
		},
	}
}

// minReadyWindowStart is the fake "now" every test below anchors on.
var minReadyWindowStart = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// podReadyAt appends a PodReady=True condition whose lastTransitionTime is
// readyAt — the timestamp the availability rule measures from.
func podReadyAt(pod *corev1.Pod, readyAt time.Time) {
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.PodReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(readyAt),
	})
}

// nonSurgeFixture is one Instance with a roll in flight and one pod. The
// caller decorates the pod, edits the input or the plan, then runs a pass
// and reads the row back.
type nonSurgeFixture struct {
	isvc       *v1beta1.InferenceService
	c          client.Client
	targetCR   *appsv1.ControllerRevision
	targetSpec *corev1.PodSpec
	pod        *corev1.Pod
	strategy   workload.UpdateStrategyType
	// blocks records every RetryBlock the pass asked to write.
	blocks *[]string
}

// inPlaceInFlightFixture seeds an in-place patch already stamped on the
// row: Phase=Updating, Op{Update, InPlace} at the target, and the pod it
// is patching still on the running revision's image.
func inPlaceInFlightFixture(t *testing.T) *nonSurgeFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir, _ := inPlaceRollWithoutPods(t)
	ir.Status.InstanceStatuses[0].Operation.Strategy = string(v1beta1.UpdateStrategyInPlaceIfPossible)
	targetSpec := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, false /* not ready */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, targetSpec)
	return &nonSurgeFixture{
		isvc: isvc, c: c, targetCR: tcr, targetSpec: targetSpec, pod: pod,
		strategy: workload.UpdateStrategyInPlaceIfPossible,
	}
}

// recreateDrainInFlightFixture seeds a recreate already stamped on the
// row: Phase=Updating, Op{Update, Drain} at the bumped Incarnation. The
// pod the caller supplies decides which phase of the step the pass lands
// in — an old-incarnation pod keeps it in Phase A, a bumped one in C.
func recreateDrainInFlightFixture(t *testing.T) *nonSurgeFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 2)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	targetSpec := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, targetSpec)

	live := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), live); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	live.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	live.Status.InstanceStatuses[0].Incarnation = 2
	live.Status.InstanceStatuses[0].TargetRevision = tcr.Name
	live.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		ID:             "update-0-1",
		Type:           v1beta1.InstanceOperationUpdate,
		Step:           workload.UpdateStepDrain,
		TargetRevision: tcr.Name,
		Strategy:       string(v1beta1.UpdateStrategyRecreatePod),
		StartedAt:      metav1.NewTime(time.Now().Add(-time.Minute)),
		LastProgressAt: metav1.NewTime(time.Now().Add(-time.Minute)),
		Deadline:       metav1.NewTime(time.Now().Add(29 * time.Minute)),
	}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("seed the in-flight recreate: %v", err)
	}
	return &nonSurgeFixture{
		isvc: isvc, c: c, targetCR: tcr, targetSpec: targetSpec,
		strategy: workload.UpdateStrategyRecreatePod,
	}
}

// input builds a fresh ReconcileInput and wires the RetryBlock recorder.
func (f *nonSurgeFixture) input() workload.ReconcileInput {
	in := legacyTestInput(f.isvc, f.c, workload.ComponentEngine)
	in.ObservedState.UpdateRevision = f.targetCR.Name
	blocks := &[]string{}
	f.blocks = blocks
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}
	return in
}

// nonSurgeOutcome is everything a pass over an in-flight roll may move.
type nonSurgeOutcome struct {
	done        bool
	phase       v1beta1.OMENativeInstancePhase
	step        string
	incarnation int64
	failed      bool
	blocks      int
	podSurvives bool
	// podImage is the image on the observed pod after the pass, so an
	// edit that silently changed what was written to it is visible.
	podImage string
}

// run drives one pass with pods and reports what moved. tweak may edit
// the input and the plan the way an operator config change would.
func (f *nonSurgeFixture) run(t *testing.T, pods []*corev1.Pod, tweak func(*workload.ReconcileInput, *workload.ComponentPlan)) nonSurgeOutcome {
	t.Helper()
	in := f.input()
	plan := legacyComponentPlan(f.strategy, nil)
	if tweak != nil {
		tweak(&in, &plan)
	}
	deps := workload.Deps{Client: f.c, Recorder: record.NewFakeRecorder(32)}
	done, err := UpdateWithPods(context.Background(), deps, in, plan, plan.Instances[0], f.targetCR, f.targetSpec, pods)
	if err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	s := legacyInstanceStatusesOnIR(f.c, f.isvc, workload.ComponentEngine)[0]
	out := nonSurgeOutcome{
		done:        done,
		phase:       s.Phase,
		incarnation: s.Incarnation,
		failed:      s.LastFailure != nil,
		blocks:      len(*f.blocks),
		podSurvives: true,
	}
	if s.Operation != nil {
		out.step = s.Operation.Step
	}
	if len(pods) > 0 {
		fresh := &corev1.Pod{}
		err := f.c.Get(context.Background(), client.ObjectKeyFromObject(pods[0]), fresh)
		out.podSurvives = err == nil
		if err == nil && len(fresh.Spec.Containers) > 0 {
			out.podImage = fresh.Spec.Containers[0].Image
		}
	}
	return out
}

// terminatingPod deletes pod through a finalizer so the fake client keeps
// the object with a deletionTimestamp — what the live List hands a pass
// over a pod the apiserver has not collected yet.
func terminatingPod(t *testing.T, c client.Client, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	live := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), live); err != nil {
		t.Fatalf("re-read pod %s: %v", pod.Name, err)
	}
	live.Finalizers = append(live.Finalizers, "ome.io/test-hold")
	if err := c.Update(ctx, live); err != nil {
		t.Fatalf("pin pod %s with a finalizer: %v", pod.Name, err)
	}
	if err := c.Delete(ctx, live); err != nil {
		t.Fatalf("delete pod %s: %v", pod.Name, err)
	}
	out := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), out); err != nil {
		t.Fatalf("re-read terminating pod %s: %v", pod.Name, err)
	}
	if out.DeletionTimestamp == nil {
		t.Fatalf("pod %s did not become Terminating", pod.Name)
	}
	return out
}

// configChange is one operator-config edit under a short name.
type configChange struct {
	name  string
	tweak func(*workload.ReconcileInput, *workload.ComponentPlan)
}

// configChangeCases is the set of knobs an in-flight roll must not read.
// retryBlockHistoryLimit has no field here on purpose:
// it is consumed by the status writer's end-of-pass prune, so what an
// update pass owes it is to write no RetryBlock at all — which every
// case here asserts through the outcome's block count.
func configChangeCases() []configChange {
	return []configChange{
		{
			name: "update retry policy",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.UpdateRetryPolicy = &workload.RetryPolicy{
					MaxAttempts: 7, InitialDelay: time.Minute, MaxDelay: time.Hour, Multiplier: 2,
				}
			},
		},
		{
			name: "force-delete policy",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.ForceDelete = fdPolicy()
			},
		},
		{
			name: "scale-down cadence",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.ScaleDownRequeueInterval = 90 * time.Second
			},
		},
		{
			name: "requeue cadence",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.Requeue = workload.RequeueIntervals{Operation: 11 * time.Minute, Gate: 7 * time.Minute}
			},
		},
		{
			name: "gang schedule clamp",
			tweak: func(_ *workload.ReconcileInput, plan *workload.ComponentPlan) {
				plan.GangScheduleTimeout = &workload.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}
			},
		},
		{
			name: "migration audit caps",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.MigrationAudit = &workload.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 2, Window: time.Hour}
			},
		},
		{
			name: "superseded RetryBlock history",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.ObservedState.RetryBlocks = []workload.RetryBlock{
					{TargetRevision: "llama-70b-engine-stale001", State: workload.RetryBlockHeld},
					{TargetRevision: "llama-70b-engine-stale002", State: workload.RetryBlockBackoff},
				}
			},
		},
	}
}

// gangSurgeTargetMarkerAt reports the live replacement marker as the row
// shows it: Creating at the GangSurgeTarget step, pinned to revision.
func gangSurgeTargetMarkerAt(row *workload.InstanceStatus, revision string) bool {
	return row != nil && row.Phase == workload.InstancePhaseCreating &&
		row.Operation != nil && row.Operation.Type == workload.InstanceOperationUpdate &&
		row.Operation.Step == workload.UpdateStepGangSurgeTarget &&
		row.Operation.TargetRevision == revision
}
