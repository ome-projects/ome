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

func legacyFromV1beta1Op(op *v1beta1.InstanceOperation) *workload.InstanceOperation {
	if op == nil {
		return nil
	}
	return &workload.InstanceOperation{
		ID:             op.ID,
		Type:           workload.InstanceOperationType(op.Type),
		Step:           op.Step,
		StartedAt:      op.StartedAt,
		LastProgressAt: op.LastProgressAt,
		Deadline:       op.Deadline,
		TargetRevision: op.TargetRevision,
		Reason:         op.Reason,
		Waiting:        op.Waiting,
		RequestUUID:    op.RequestUUID,
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
		ID:             op.ID,
		Type:           v1beta1.InstanceOperationType(op.Type),
		Step:           op.Step,
		StartedAt:      op.StartedAt,
		LastProgressAt: op.LastProgressAt,
		Deadline:       op.Deadline,
		TargetRevision: op.TargetRevision,
		Reason:         op.Reason,
		Waiting:        op.Waiting,
		RequestUUID:    op.RequestUUID,
		SurgeIndex:     op.SurgeIndex,
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
// incarnation, image, runtime-ready + serving.
func legacyRunningPodAtRevision(isvc *v1beta1.InferenceService, instIdx int32, incarnation int64, image string) *corev1.Pod {
	pod := legacyPodAtIncarnation(isvc, instIdx, incarnation, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: image}}
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
