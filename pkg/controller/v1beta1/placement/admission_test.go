package placement

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// isvcWithInstances builds a derived ISVC declaring `component` with an empty
// Lifecycle summary. Used by the controller reconcile tests that seed a derived
// ISVC on a worker cluster; the per-instance data lives on the paired
// InferenceReplica (see irWithInstances), which the predicates read via
// DecodedComponentIRFor. The variadic arg exists only for call-site symmetry with
// irWithInstances; it does not populate the ISVC.
func isvcWithInstances(component v1beta1.ComponentType, _ ...bool) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				component: {Lifecycle: &v1beta1.LifecycleStatus{}},
			},
		},
	}
	declareComponent(isvc, component)
	return isvc
}

// admittedStatusMap builds the authoritative per-component IR status map the
// placement predicates now consume, mirroring what the reconcile assembles from
// the workload cluster's InferenceReplicas.
func admittedStatusMap(component v1beta1.ComponentType, admitted ...bool) map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus {
	insts := make([]v1beta1.OMENativeInstanceStatus, len(admitted))
	for i, a := range admitted {
		insts[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Admitted: a}
	}
	return map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		component: {InstanceStatuses: insts},
	}
}

// irWithInstances builds an InferenceReplica named "svc-<component>" in "prod"
// (matching the derived ISVC the reconcile tests use) whose status carries the
// given admitted instances, for seeding on a worker fake client so the reconcile
// reads it via DecodedComponentIRFor.
func irWithInstances(component v1beta1.ComponentType, admitted ...bool) *v1beta1.InferenceReplica {
	insts := make([]v1beta1.OMENativeInstanceStatus, len(admitted))
	for i, a := range admitted {
		insts[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Admitted: a}
	}
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc-" + string(component)},
		Status:     v1beta1.InferenceReplicaStatus{InstanceStatuses: insts},
	}
}

// declareComponent adds a minimal spec block for component so AllComponentsAdmitted
// (which reads declared components off the spec) sees it as declared.
func declareComponent(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) {
	switch component {
	case v1beta1.EngineComponent:
		isvc.Spec.Engine = &v1beta1.EngineSpec{}
	case v1beta1.DecoderComponent:
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	case v1beta1.RouterComponent:
		isvc.Spec.Router = &v1beta1.RouterSpec{}
	}
}

func TestAllComponentsAdmitted(t *testing.T) {
	engine := &v1beta1.InferenceService{}
	declareComponent(engine, v1beta1.EngineComponent)

	// single declared component, admitted -> true.
	assert.True(t, AllComponentsAdmitted(engine, admittedStatusMap(v1beta1.EngineComponent, false, true)))
	// single declared component, all gated -> false.
	assert.False(t, AllComponentsAdmitted(engine, admittedStatusMap(v1beta1.EngineComponent, false, false)))
	// no declared components -> never a winner.
	assert.False(t, AllComponentsAdmitted(&v1beta1.InferenceService{}, nil))

	// PD: engine admitted but decoder still gated -> NOT a winner.
	pd := &v1beta1.InferenceService{}
	declareComponent(pd, v1beta1.EngineComponent)
	pd.Spec.Decoder = &v1beta1.DecoderSpec{}
	mixed := map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		v1beta1.EngineComponent:  {InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: true}}},
		v1beta1.DecoderComponent: {InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: false}}},
	}
	assert.False(t, AllComponentsAdmitted(pd, mixed), "engine admitted, decoder gated must not win")

	// PD: both admitted -> winner.
	both := map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		v1beta1.EngineComponent:  {InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: true}}},
		v1beta1.DecoderComponent: {InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: true}}},
	}
	assert.True(t, AllComponentsAdmitted(pd, both), "both components admitted must win")

	// declared component with no status entry yet -> false (no panic).
	missingStatus := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}
	assert.False(t, AllComponentsAdmitted(missingStatus, nil))
}

// claimCase is one reading of componentIRStatuses: the engine replica's rows
// (admitted or not) and ready count, an edit of the service, the replica or
// its pod, and whether the pod list comes back cut short.
type claimCase struct {
	name  string
	rows  []bool
	ready int32
	edit  func(isvc *v1beta1.InferenceService, engine *v1beta1.InferenceReplica, pod *corev1.Pod)
	cut   bool
	want  claimResult
}

// claimResult is what the lock compares: the error text, whether the pods
// were listed, and the engine status as read (present, ready count, each
// row's admission).
type claimResult struct {
	Err      string
	Listed   bool
	Present  bool
	Ready    int32
	Admitted []bool
}

// recordingPodList records whether a pod list was read and, when asked, cuts
// the list short the way an apiserver page does.
func recordingPodList(listed *bool, cut bool) func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
	return func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*corev1.PodList); ok {
			*listed = true
		}
		if err := c.List(ctx, list, opts...); err != nil {
			return err
		}
		if cut {
			list.SetContinue("more")
		}
		return nil
	}
}

// claimCaseResult builds the member of one case (a service declaring an
// engine and a decoder, an engine replica with its pods, no decoder replica),
// reads it through componentIRStatuses and projects the result.
func claimCaseResult(t *testing.T, tt claimCase) claimResult {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc", UID: "member-uid"}}
	declareComponent(isvc, v1beta1.EngineComponent)
	declareComponent(isvc, v1beta1.DecoderComponent)
	engine := placementIR(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, tt.rows...)
	engine.Status.ReadyReplicas = tt.ready
	objects := observedWorkerObjects(isvc, engine)
	var pod *corev1.Pod
	for _, obj := range objects {
		if p, ok := obj.(*corev1.Pod); ok {
			pod = p
		}
	}
	if tt.edit != nil {
		tt.edit(isvc, engine, pod)
	}
	listed := false
	worker := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{List: recordingPodList(&listed, tt.cut)}).Build()
	statuses, err := componentIRStatuses(t.Context(), (&Reconciler{}).instanceStatusReader(worker), isvc)
	got := claimResult{Listed: listed}
	if err != nil {
		got.Err = err.Error()
		return got
	}
	if decoder, present := statuses[v1beta1.DecoderComponent]; !present || decoder != nil {
		t.Fatalf("a declared component without a replica must be present as nil: present=%t %v", present, decoder)
	}
	if status := statuses[v1beta1.EngineComponent]; status != nil {
		got.Present, got.Ready = true, status.ReadyReplicas
		for _, row := range status.InstanceStatuses {
			got.Admitted = append(got.Admitted, row.Admitted)
		}
	}
	return got
}

// TestComponentIRStatusesVerifiesEveryAdmissionClaim holds which component
// statuses are checked against the member's pods. A replica with an admitted
// row or a ready count claims and is verified, and the pods are listed only
// when some replica claims; a replica with neither is reported as read; a
// declared component without a replica is reported as nil. A claiming replica
// must name the service as its controller by UID, kind and API version. A pod
// the replica does not control, or one another service's label keeps out of
// the list, completes no cohort, and its gang is not read. A list the server
// cut short is refused.
func TestComponentIRStatusesVerifiesEveryAdmissionClaim(t *testing.T) {
	const unverifiedOwner = `component "svc-engine" has unverified service ownership`
	partial := claimResult{Listed: true, Present: true, Admitted: []bool{false}}
	for _, tt := range []claimCase{
		{name: "neither an admitted row nor a ready count claims", rows: []bool{false}, want: claimResult{Present: true, Admitted: []bool{false}}},
		{name: "a ready count alone claims and is verified", rows: []bool{false}, ready: 1, want: partial},
		{name: "an admitted row alone claims and is verified", rows: []bool{true}, want: claimResult{Listed: true, Present: true, Admitted: []bool{true}}},
		{name: "a service without a UID controls no claiming replica", rows: []bool{true}, edit: func(isvc *v1beta1.InferenceService, engine *v1beta1.InferenceReplica, _ *corev1.Pod) {
			isvc.UID, engine.OwnerReferences[0].UID = "", ""
		}, want: claimResult{Err: unverifiedOwner}},
		{name: "a claiming replica controlled by another kind", rows: []bool{true}, edit: func(_ *v1beta1.InferenceService, engine *v1beta1.InferenceReplica, _ *corev1.Pod) {
			engine.OwnerReferences[0].Kind = "Deployment"
		}, want: claimResult{Err: unverifiedOwner}},
		{name: "a claiming replica controlled through another API version", rows: []bool{true}, edit: func(_ *v1beta1.InferenceService, engine *v1beta1.InferenceReplica, _ *corev1.Pod) {
			engine.OwnerReferences[0].APIVersion = "apps/v1"
		}, want: claimResult{Err: unverifiedOwner}},
		{name: "a replica of another owner that claims nothing is read", rows: []bool{false}, edit: func(_ *v1beta1.InferenceService, engine *v1beta1.InferenceReplica, _ *corev1.Pod) {
			engine.OwnerReferences[0].UID = "other"
		}, want: claimResult{Present: true, Admitted: []bool{false}}},
		{name: "a pod of another controller completes no cohort", rows: []bool{true}, edit: func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, pod *corev1.Pod) {
			pod.OwnerReferences[0].UID = "other-replica"
		}, want: partial},
		{name: "a pod under another service's label completes no cohort", rows: []bool{true}, edit: func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, pod *corev1.Pod) {
			pod.Labels[constants.InferenceServicePodLabelKey] = "other"
		}, want: partial},
		{name: "the gang of another controller's pod is not read", rows: []bool{true}, edit: func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, pod *corev1.Pod) {
			pod.OwnerReferences[0].UID, pod.Labels[query.LabelPodGroup] = "other-replica", "other-gang"
		}, want: partial},
		{name: "a pod list the server cut short is refused", rows: []bool{true}, cut: true, want: claimResult{Err: "member pod inventory is incomplete", Listed: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, claimCaseResult(t, tt)); diff != "" {
				t.Fatalf("verified statuses (-want +got):\n%s", diff)
			}
		})
	}
}
