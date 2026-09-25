package escalation_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Fixtures the escalation tests share with the reconcile root's: the
// fake scheme, the minimal input and plan, and the atomic mutation store
// the batched status writes land in.

// actionKinds projects the Decision's action kinds in order.
func actionKinds(d workload.Decision) []workload.ActionKind {
	kinds := make([]workload.ActionKind, 0, len(d.Actions))
	for _, a := range d.Actions {
		kinds = append(kinds, a.Kind)
	}
	return kinds
}

// findAction returns the first action of the given kind, or nil.
func findAction(d workload.Decision, kind workload.ActionKind) *workload.PlannedAction {
	for i := range d.Actions {
		if d.Actions[i].Kind == kind {
			return &d.Actions[i]
		}
	}
	return nil
}

// forbidMutations wires every mutation callback on input to a t.Error
// recorder, so any write attempted during Plan fails the test.
func forbidMutations(t *testing.T, input *workloadtypes.ReconcileInput) {
	t.Helper()
	input.MutateInstance = func(_ context.Context, idx int32, _ func(*workloadtypes.InstanceStatus) bool) error {
		t.Errorf("Plan must not call MutateInstance (idx=%d)", idx)
		return nil
	}
	input.ApplyInstanceMutations = func(_ context.Context, muts []workloadtypes.InstanceMutation) error {
		t.Errorf("Plan must not call ApplyInstanceMutations (%d mutations)", len(muts))
		return nil
	}
	input.RemoveInstance = func(_ context.Context, idx int32) (bool, error) {
		t.Errorf("Plan must not call RemoveInstance (idx=%d)", idx)
		return false, nil
	}
	input.WriteAggregateCondition = func(_ context.Context, cond metav1.Condition) error {
		t.Errorf("Plan must not call WriteAggregateCondition (%s)", cond.Type)
		return nil
	}
	input.WarnInstanceFailed = func(idx int32, _, _ string) {
		t.Errorf("Plan must not call WarnInstanceFailed (idx=%d)", idx)
	}
	input.WarnRetryHeld = func(rev string, _ int32, _ string) {
		t.Errorf("Plan must not call WarnRetryHeld (%s)", rev)
	}
	input.MutateMigration = func(_ context.Context, uuid string, _ func(*workloadtypes.MigrationRecord) bool) error {
		t.Errorf("Plan must not call MutateMigration (%s)", uuid)
		return nil
	}
	input.AppendMigration = func(_ context.Context, rec workloadtypes.MigrationRecord) error {
		t.Errorf("Plan must not call AppendMigration (%s)", rec.RequestUUID)
		return nil
	}
	input.MutateRetryBlock = func(_ context.Context, rev string, _ func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		t.Errorf("Plan must not call MutateRetryBlock (%s)", rev)
		return nil
	}
	input.UpdateGate = func(_ workloadtypes.UpdateStrategyType, _, _ int32) (bool, workloadtypes.RolloutHoldGate, string) {
		t.Error("Plan must not consult UpdateGate (Execute owns the consult)")
		return true, "", ""
	}
}

// planSnapshot builds a snapshot over the given pods the way a reconcile
// does: through a client that holds them, so Plan lists and buckets them
// by their labels.
func planSnapshot(input workloadtypes.ReconcileInput, byIdx map[int32][]*corev1.Pod) *workload.ObservedSnapshot {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	var objs []client.Object
	for _, pods := range byIdx {
		for _, p := range pods {
			objs = append(objs, p)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c}
	return workload.NewObservedSnapshot(deps, input, input.Key.Component, input.ObservedState.InstanceStatuses)
}

// planTargetOrFail runs Plan against a target ControllerRevision and
// fails the test on error.
func planTargetOrFail(t *testing.T, input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, target *appsv1.ControllerRevision, snapshot *workload.ObservedSnapshot) workload.Decision {
	t.Helper()
	d, err := workload.Plan(context.Background(), input, plan, target, snapshot)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	return d
}

// updateTarget is the target ControllerRevision the update-selection
// tests roll toward.
func updateTarget() *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}
}

// enginePod fabricates a single-pod "default" engine pod at ordinal 0
// with the labels Render stamps (managed-by + instance-idx + ordinal +
// component + isvc), so query selectors and instance-index filters
// recognize it.
func enginePod(isvc, ns string, idx int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc, workloadtypes.ComponentEngine, idx, "default", 0),
			Namespace: ns,
			UID:       types.UID(fmt.Sprintf("%s-engine-%d-uid", isvc, idx)),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
				query.LabelInstanceIdx:                fmt.Sprintf("%d", idx),
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     "default",
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 "0",
				query.LabelRevisionHash:               "priorrev",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
	}
}

// makeScheme builds the runtime.Scheme the fake client needs.
func makeScheme(t *testing.T) *runtime.Scheme {
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
	return scheme
}

func minimalInput(t *testing.T) workloadtypes.ReconcileInput {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b", Namespace: "prod", UID: "uid-1",
	}}
	in := workloadtypes.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workloadtypes.Key{
			Namespace: "prod",
			Component: workloadtypes.ComponentEngine,
			OwnerName: "llama-70b",
		},
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
		Requeue:                  testRequeueIntervals,
		DesiredSpec: workloadtypes.WorkloadDesiredSpec{
			Replicas: 1,
			PodSpec: &corev1.PodSpec{Containers: []corev1.Container{
				{Name: "main", Image: "test:v1"},
			}},
		},
	}
	stubInputCallbacks(&in)
	return in
}

// minimalPlan returns a ComponentPlan covering a single Instance at
// index 0 with a single-pod "default" Runner.
func minimalPlan() workloadtypes.ComponentPlan {
	return workloadtypes.ComponentPlan{
		Component: workloadtypes.ComponentEngine,
		Replicas:  1,
		Instances: []workloadtypes.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{
				{Name: "default", Size: 1},
			}},
		},
	}
}

// stubInputCallbacks fills the callback fields that
// workload.ReconcileInput panics on if left nil. Tests only set
// behavior on the callbacks they care about; the rest stay no-ops.
func stubInputCallbacks(input *workloadtypes.ReconcileInput) {
	if input.MutateInstance == nil {
		input.MutateInstance = func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
			return nil
		}
	}
	if input.RemoveInstance == nil {
		input.RemoveInstance = func(_ context.Context, _ int32) (bool, error) { return false, nil }
	}
	if input.WriteAggregateCondition == nil {
		input.WriteAggregateCondition = func(_ context.Context, _ metav1.Condition) error { return nil }
	}
	if input.WarnInstanceFailed == nil {
		input.WarnInstanceFailed = func(_ int32, _, _ string) {}
	}
	if input.MutateMigration == nil {
		input.MutateMigration = func(_ context.Context, _ string, _ func(*workloadtypes.MigrationRecord) bool) error {
			return nil
		}
	}
}

// testRequeueIntervals is the dispatcher cadence these tests run under,
// standing in for the operator config a deployed chart supplies. A test
// pinning the unconfigured path clears ReconcileInput.Requeue.
var testRequeueIntervals = workloadtypes.RequeueIntervals{
	Operation: 5 * time.Second,
	Gate:      3 * time.Second,
}

// minimalInput builds a ReconcileInput with the bare minimum fields
// the dispatcher reads. Tests pad observed state + plan as needed.
const testScaleDownRequeueInterval = 37 * time.Second
