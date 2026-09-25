package workload_test

import (
	"context"
	"fmt"
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
	types "k8s.io/apimachinery/pkg/types"
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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Reconcile is a thin dispatch layer over the workload/ops state
// machines. The tests below assert the orchestration shape — nil-deps
// guards, scale-down-then-create ordering, and the early-return
// requeue on partial scale-down — by driving Reconcile against a real
// fake client and the real workload/ops bodies.
//
// Per-op coverage (Update gate ordering, Restart trigger detection,
// Migrate surge lifecycle) lives in workload/ops/*_test.go; those tests
// drive the per-op functions directly and need no Reconcile wrapper.

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

type testAtomicMutationStore struct {
	owner    client.Object
	statuses []workloadtypes.InstanceStatus
	writes   int
}

func installTestAtomicMutationStore(input *workloadtypes.ReconcileInput) *testAtomicMutationStore {
	store := &testAtomicMutationStore{
		owner:    input.OwnerObject,
		statuses: cloneTestInstanceStatuses(input.ObservedState.InstanceStatuses),
	}
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	return store
}

func (s *testAtomicMutationStore) apply(_ context.Context, mutations []workloadtypes.InstanceMutation, _ string, mutateRetryBlock func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
	if mutateRetryBlock != nil {
		return fmt.Errorf("test atomic mutation store does not support RetryBlock mutations")
	}
	snapshot := workloadtypes.InstanceMutationSnapshot{Instances: make(map[int32]workloadtypes.InstanceStatus, len(s.statuses))}
	if s.owner != nil {
		snapshot.OwnerUID = s.owner.GetUID()
		snapshot.OwnerGeneration = s.owner.GetGeneration()
	}
	for _, status := range s.statuses {
		snapshot.Instances[status.Index] = cloneTestInstanceStatus(status)
	}
	for _, mutation := range mutations {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			return workloadtypes.ErrStatusMutationPrecondition
		}
	}

	type committedMutation struct {
		callback func(*workloadtypes.InstanceStatus, *workloadtypes.InstanceStatus)
		before   *workloadtypes.InstanceStatus
		after    *workloadtypes.InstanceStatus
	}
	next := cloneTestInstanceStatuses(s.statuses)
	committed := make([]committedMutation, 0, len(mutations))
	for _, mutation := range mutations {
		position := -1
		for i := range next {
			if next[i].Index == mutation.Index {
				position = i
				break
			}
		}
		if mutation.Remove {
			if position < 0 {
				continue
			}
			before := cloneTestInstanceStatus(next[position])
			if mutation.Precondition != nil && !mutation.Precondition(&before) {
				continue
			}
			next = append(next[:position], next[position+1:]...)
			committed = append(committed, committedMutation{callback: mutation.OnCommit, before: &before})
			continue
		}

		status := workloadtypes.InstanceStatus{Index: mutation.Index}
		var before *workloadtypes.InstanceStatus
		if position >= 0 {
			status = cloneTestInstanceStatus(next[position])
			copy := cloneTestInstanceStatus(status)
			before = &copy
		}
		if mutation.Precondition != nil && !mutation.Precondition(&status) {
			continue
		}
		if !mutation.Mutate(&status) {
			continue
		}
		if position >= 0 {
			next[position] = cloneTestInstanceStatus(status)
		} else {
			next = append(next, cloneTestInstanceStatus(status))
		}
		after := cloneTestInstanceStatus(status)
		committed = append(committed, committedMutation{callback: mutation.OnCommit, before: before, after: &after})
	}
	if len(committed) == 0 {
		return nil
	}
	s.statuses = next
	s.writes++
	for _, mutation := range committed {
		if mutation.callback != nil {
			mutation.callback(mutation.before, mutation.after)
		}
	}
	return nil
}

func (s *testAtomicMutationStore) sync(input *workloadtypes.ReconcileInput) {
	input.ObservedState.InstanceStatuses = cloneTestInstanceStatuses(s.statuses)
}

func (s *testAtomicMutationStore) status(index int32) *workloadtypes.InstanceStatus {
	for i := range s.statuses {
		if s.statuses[i].Index == index {
			status := cloneTestInstanceStatus(s.statuses[i])
			return &status
		}
	}
	return nil
}

func cloneTestInstanceStatuses(statuses []workloadtypes.InstanceStatus) []workloadtypes.InstanceStatus {
	cloned := make([]workloadtypes.InstanceStatus, len(statuses))
	for i := range statuses {
		cloned[i] = cloneTestInstanceStatus(statuses[i])
	}
	return cloned
}

func cloneTestInstanceStatus(status workloadtypes.InstanceStatus) workloadtypes.InstanceStatus {
	converted := v1beta1convert.InstanceStatusFromWorkload(status)
	return v1beta1convert.InstanceStatusToWorkload(*converted.DeepCopy())
}

// minimalInput builds a ReconcileInput with the bare minimum fields
// the dispatcher reads. Tests pad observed state + plan as needed.
const testScaleDownRequeueInterval = 37 * time.Second

// testRequeueIntervals is the dispatcher cadence these tests run under,
// standing in for the operator config a deployed chart supplies. A test
// pinning the unconfigured path clears ReconcileInput.Requeue.
var testRequeueIntervals = workloadtypes.RequeueIntervals{
	Operation: 5 * time.Second,
	Gate:      3 * time.Second,
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

func roundTripMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component workloadtypes.ComponentType) func(context.Context, int32, func(*workloadtypes.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-" + string(component)}
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
		slot := v1beta1.OMENativeInstanceStatus{Index: idx}
		if pos != -1 {
			slot = ir.Status.InstanceStatuses[pos]
		}
		w := v1beta1convert.InstanceStatusToWorkload(slot)
		if !mutate(&w) {
			return nil
		}
		updated := v1beta1convert.InstanceStatusFromWorkload(w)
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

// instanceStatusByIndex looks up an InstanceStatus on the component's InferenceReplica.
func instanceStatusByIndex(c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) *v1beta1.OMENativeInstanceStatus {
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-" + string(component)}
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

// TestReconcile_NilClient_Errors asserts the nil-deps.Client guard
// fires up front. The dispatcher MUST refuse to run without a client
// so the per-op state machines never NPE on a missing reader.
func TestReconcile_NilClient_Errors(t *testing.T) {
	in := minimalInput(t)
	_, err := workload.Reconcile(context.Background(), workloadtypes.Deps{}, in, minimalPlan(), nil)
	if err == nil {
		t.Fatalf("expected nil-Client to error, got nil")
	}
}

// TestReconcile_NilTarget_DrivesCreate covers the cold-start path:
// no observed instances, no target ControllerRevision, plan asks for
// one Instance. Reconcile MUST fall through to the Create pass without
// scale-down / restart / migration work; Create's first action is to
// allocate the Instance, which the fake client accepts.
//
// We pass target=nil because Create's signature allows it (MinReplicas=0
// would render a nil target; here we exercise the cold-create path
// where the renderer hasn't materialized a revision yet).
func TestReconcile_NilTarget_DrivesCreate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	plan := minimalPlan()

	// Cold-start path: target nil short-circuits the Update pass.
	// Create's nil-target branch returns done=true without rendering.
	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_UnconfiguredCadenceUsesBackoff pins the unset path at
// the dispatcher: a pass that still has Instances coming up and no
// operator cadence asks for the controller's rate-limited backoff
// rather than an interval the binary invented, while the configured
// cadence produces an explicit wake-up.
func TestReconcile_UnconfiguredCadenceUsesBackoff(t *testing.T) {
	scheme := makeScheme(t)
	plan := minimalPlan()
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-v1", Namespace: "prod"},
	}

	configured := fake.NewClientBuilder().WithScheme(scheme).Build()
	res, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: configured}, minimalInput(t), plan, target)
	if err != nil {
		t.Fatalf("Reconcile with a configured cadence: %v", err)
	}
	if res.RequeueAfter != testRequeueIntervals.Operation {
		t.Fatalf("configured cadence: got RequeueAfter %v want %v", res.RequeueAfter, testRequeueIntervals.Operation)
	}

	unconfigured := fake.NewClientBuilder().WithScheme(scheme).Build()
	in := minimalInput(t)
	in.Requeue = workloadtypes.RequeueIntervals{}
	res, err = workload.Reconcile(context.Background(), workloadtypes.Deps{Client: unconfigured}, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile with no configured cadence: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("unconfigured cadence: got RequeueAfter %v want 0", res.RequeueAfter)
	}
	if !res.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
		t.Fatalf("a converging pass with no cadence must ride the rate-limited backoff; got %+v", res)
	}
}

// TestReconcile_HeldWorkWakesThePass pins the held-work sink at the
// dispatcher: work an op pass held on operator configuration — which no
// watch event announces — leaves a wake-up on the pass result even when
// every other pass is steady and asks for none.
func TestReconcile_HeldWorkWakesThePass(t *testing.T) {
	scheme := makeScheme(t)
	plan := minimalPlan()

	for _, tc := range []struct {
		name      string
		cadence   time.Duration
		wantAfter time.Duration
	}{
		{name: "configured cadence", cadence: 5 * time.Second, wantAfter: 5 * time.Second},
		{name: "unconfigured cadence rides the backoff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			in := minimalInput(t)
			in.Requeue = workloadtypes.RequeueIntervals{}
			// Nothing for the passes to do: the held work is the only
			// reason to come back.
			in.DesiredSpec.Replicas = 0
			wake := &workloadtypes.PassWake{}
			wake.Observe(tc.cadence)
			in.PassWake = wake

			res, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: c}, in, plan, nil)
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if res.RequeueAfter != tc.wantAfter {
				t.Fatalf("RequeueAfter: got %v want %v", res.RequeueAfter, tc.wantAfter)
			}
			if tc.wantAfter == 0 && !res.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
				t.Fatalf("held work with no cadence must ride the rate-limited backoff; got %+v", res)
			}
		})
	}
}

// TestReconcile_PausedSkipsCreate proves InferenceReplica.spec.paused is a
// real circuit breaker: a missing Instance must not be allocated while the
// plan is paused.
func TestReconcile_PausedSkipsCreate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	allocateCalls := 0
	in.MutateInstance = func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
		allocateCalls++
		return nil
	}
	plan := minimalPlan()
	plan.Paused = true

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("paused reconcile must stay quiescent, got %+v", result)
	}
	if allocateCalls != 0 {
		t.Fatalf("paused reconcile allocated %d Instances, want 0", allocateCalls)
	}
}

// TestReconcile_PausedStillScalesDown keeps the safety boundary explicit:
// pausing lifecycle churn must not prevent a deliberate replica reduction
// from deleting an extra Instance.
func TestReconcile_PausedStillScalesDown(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	plan := minimalPlan()
	plan.Paused = true

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("paused scale-down admission must requeue immediately, got %+v", result)
	}
	status := store.status(1)
	if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting || status.Operation == nil || status.Operation.Type != workloadtypes.InstanceOperationDelete {
		t.Fatalf("paused scale-down did not durably admit index 1: %+v", status)
	}
	if retained := store.status(0); retained == nil || retained.Phase != workloadtypes.InstancePhaseReady {
		t.Fatalf("paused scale-down changed retained index 0: %+v", retained)
	}
}

// TestReconcile_ScaleDownExtra_DrivesDelete covers an InstanceStatus whose
// index is outside the plan. Fresh Delete admission is committed before any
// Pod effect and immediately requeues for a new observation.
func TestReconcile_ScaleDownExtra_DrivesDelete(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Phase: workloadtypes.InstancePhaseReady}, // extra
	}
	plan := minimalPlan() // covers only index 0
	store := installTestAtomicMutationStore(&in)

	if extras := workload.ScaleDownExtras(in.ObservedState.InstanceStatuses, plan); len(extras) != 1 || extras[0] != 1 {
		t.Fatalf("ScaleDownExtras: got %v, want [1]", extras)
	}
	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || store.writes != 1 {
		t.Fatalf("scale-down admission result/writes = %+v/%d, want immediate requeue/1", result, store.writes)
	}
}

// TestReconcile_MigrationInFlight_NotScaleDown asserts that an
// Instance with Phase=Migrating is NOT scale-down-deleted, even when
// the plan doesn't cover its index — otherwise Migrate's surge
// would be ripped out by the dispatcher's first pass.
func TestReconcile_MigrationInFlight_NotScaleDown(t *testing.T) {
	observed := []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},
		{Index: 7, Phase: workloadtypes.InstancePhaseMigrating},
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ScaleDownExtras(observed, plan)
	for _, idx := range extras {
		if idx == 7 {
			t.Errorf("index 7 (Phase=Migrating) must not be in scale-down extras, got %v", extras)
		}
	}
}

// TestReconcile_MigrationOperationOwned_NotScaleDown is the dual to
// the above: an Instance whose Operation.Type=Migrate (the surge side)
// MUST be excluded even when Phase=Creating (surge mid-spin-up).
func TestReconcile_MigrationOperationOwned_NotScaleDown(t *testing.T) {
	observed := []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},
		{
			Index: 3,
			Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				Type: workloadtypes.InstanceOperationMigrate,
			},
		},
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ScaleDownExtras(observed, plan)
	for _, idx := range extras {
		if idx == 3 {
			t.Errorf("index 3 (Operation.Migrate) must not be in scale-down extras, got %v", extras)
		}
	}
}

// fixedMigrationTime anchors the work-selection ordering assertions.
func fixedMigrationTime() time.Time {
	return time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
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

// listCountingReader wraps a client.Reader and counts PodList List calls.
// Used to prove the dispatcher's restart pass lists pods ONCE per
// reconcile (via the live APIReader) instead of once per Instance.
type listCountingReader struct {
	client.Reader
	podListCalls int
}

func (r *listCountingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		r.podListCalls++
	}
	return r.Reader.List(ctx, list, opts...)
}

// failedEnginePod is enginePod with Phase=Failed — a restart trigger.
func failedEnginePod(isvc, ns string, idx int32) *corev1.Pod {
	pod := enginePod(isvc, ns, idx)
	pod.Status.Phase = corev1.PodFailed
	return pod
}

type restartScaleTestFixture struct {
	isvc   *v1beta1.InferenceService
	client client.Client
	input  workloadtypes.ReconcileInput
	plan   workloadtypes.ComponentPlan
	target *appsv1.ControllerRevision
}

func podNameSet(t *testing.T, c client.Client, namespace string) map[string]bool {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	names := make(map[string]bool, len(pods.Items))
	for i := range pods.Items {
		names[pods.Items[i].Name] = true
	}
	return names
}

func TestReconcile_PausedCompletesTheOpenRecreate(t *testing.T) {
	name := "paused-recreate"
	targetName := name + "-engine-target"
	f := newRestartScaleTestFixture(t, name, 1, true, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workloadtypes.InstancePhaseUpdating,
		RunningRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "update-0", Type: workloadtypes.InstanceOperationUpdate, Step: "Drain",
			TargetRevision: targetName,
		},
	}, []workloadtypes.RunnerPlan{{Name: "default", Size: 1}})
	f.plan.RestartPolicy = ""
	f.plan.UpdateStrategy = workloadtypes.UpdateStrategy{Type: workloadtypes.UpdateStrategyRecreatePod}

	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	want := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 0, "default", 0)
	if !names[want] {
		t.Errorf("paused reconcile left the recreate half-done; pods = %v", names)
	}
}

// TestReconcile_PausedCompletesTheCommittedCreate: a create that already
// committed finishes the set it started materializing, while an index
// that never opened one stays empty until the pause is cleared. Holding
// a half-built set would leave the Instance with nothing serving.
func TestReconcile_PausedCompletesTheCommittedCreate(t *testing.T) {
	name := "paused-create"
	targetName := name + "-engine-target"
	member := enginePod(name, "prod", 0)
	f := newRestartScaleTestFixture(t, name, 2, true, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
		RunningRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "create-0", Type: workloadtypes.InstanceOperationCreate, Step: "CreatePods",
			TargetRevision: targetName,
		},
	}, []workloadtypes.RunnerPlan{{Name: "default", Size: 2}}, member)
	// The gang-member-loss repair owns a partial set under the recreate
	// policy; this test is about the Create pass finishing its own.
	f.plan.RestartPolicy = ""

	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	missing := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 0, "default", 1)
	if !names[missing] {
		t.Errorf("paused reconcile left the committed gang half-built; pods = %v", names)
	}
	absent := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 1, "default", 0)
	if names[absent] {
		t.Errorf("paused reconcile materialized an index that never committed a create; pods = %v", names)
	}
	if s := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 1); s != nil {
		t.Errorf("paused reconcile allocated status for an uncommitted index: %+v", s)
	}
}

// TestReconcile_PausedLeavesASupersededCreateAlone: a revision edit
// arriving mid-pause does not reach a half-built set. Finishing it is
// not what the Create pass would do — it retires the attempt and opens a
// fresh one at the new revision, with a new identity, a new deadline and
// re-rendered pods — and that is the new operation a pause withholds.
// The retirement runs on the unpause.
func TestReconcile_PausedLeavesASupersededCreateAlone(t *testing.T) {
	name := "paused-superseded-create"
	pinned := name + "-engine-pinned"
	member := enginePod(name, "prod", 0)
	build := func(t *testing.T, paused bool) *restartScaleTestFixture {
		// RunningRevision empty: a first materialization, which is the
		// shape whose pods the attempt owns and can hand to a successor.
		f := newRestartScaleTestFixture(t, name, 1, paused, workloadtypes.InstanceStatus{
			Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
			Operation: &workloadtypes.InstanceOperation{
				ID: "create-0", Type: workloadtypes.InstanceOperationCreate, Step: "CreatePods",
				TargetRevision: pinned,
			},
		}, []workloadtypes.RunnerPlan{{Name: "default", Size: 2}}, member)
		f.plan.RestartPolicy = ""
		// The edit: the pass now converges to a revision the in-flight
		// attempt was not opened for.
		f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
			Name: name + "-engine-edited", Namespace: f.isvc.Namespace,
		}}
		return f
	}

	f := build(t, true)
	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile paused: %v", err)
	}
	got := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if got == nil || got.Operation == nil || got.Operation.ID != "create-0" ||
		got.Operation.TargetRevision != pinned {
		t.Errorf("paused reconcile replaced the in-flight attempt: %+v", got.Operation)
	}
	if names := podNameSet(t, f.client, f.isvc.Namespace); len(names) != 1 {
		t.Errorf("paused reconcile built pods for the edited revision; pods = %v", names)
	}

	f = build(t, false)
	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile unpaused: %v", err)
	}
	got = instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if got == nil || got.Operation == nil || got.Operation.ID == "create-0" {
		t.Errorf("unpaused reconcile did not retire the superseded attempt: %+v", got.Operation)
	}
}

// TestExecute_PausedRecordsAndReleasesTheHold: a paused pass reports the
// hold on the attempts the pause is actually holding, so the wait is
// visible on the operation and the deadline-parking step can stop its
// clock. The attempts a pause does not hold report nothing: scale-down
// runs while paused, a committed create finishes the set it started, and
// repair keeps running unless the pause is frozen. A row with no attempt
// has nothing to report. Clearing the pause releases the token.
func TestReconcile_PausedFinishesACreateItWouldNotRetire(t *testing.T) {
	name := "paused-lost-member"
	pinned := name + "-engine-pinned"
	missing := query.PodName(name, workloadtypes.ComponentEngine, 0, "default", 1)

	build := func(t *testing.T, runningRevision string) *restartScaleTestFixture {
		f := newRestartScaleTestFixture(t, name, 1, true, workloadtypes.InstanceStatus{
			Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
			RunningRevision: runningRevision,
			Operation: &workloadtypes.InstanceOperation{
				ID: "create-0", Type: workloadtypes.InstanceOperationCreate, Step: "CreatePods",
				TargetRevision: pinned,
			},
		}, []workloadtypes.RunnerPlan{{Name: "default", Size: 2}}, enginePod(name, "prod", 0))
		// The gang-member-loss repair owns a partial set under the
		// recreate policy; this is about the Create pass finishing its own.
		f.plan.RestartPolicy = ""
		return f
	}

	t.Run("a serving Instance rebuilding a lost member", func(t *testing.T) {
		// RunningRevision set: the Instance was promoted once, so the
		// retirement rule leaves its attempt in place whatever the target
		// says.
		f := build(t, pinned)
		f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
			Name: name + "-engine-edited", Namespace: f.isvc.Namespace,
		}}
		recorder := record.NewFakeRecorder(8)

		if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
			Client: f.client, APIReader: f.client, Recorder: recorder,
			Expectations: workloadtypes.NewExpectations(),
		}, f.input, f.plan, f.target); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		if names := podNameSet(t, f.client, f.isvc.Namespace); !names[missing] {
			t.Errorf("paused reconcile left the Instance short of its set; pods = %v", names)
		}
		got := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
		if got == nil || got.Operation == nil || got.Operation.ID != "create-0" ||
			got.Operation.TargetRevision != pinned {
			t.Errorf("paused reconcile replaced the attempt: %+v", got.Operation)
		}
		select {
		case ev := <-recorder.Events:
			t.Errorf("paused reconcile announced %q; finishing a set is not an event", ev)
		default:
		}
	})

	t.Run("no target to retarget to", func(t *testing.T) {
		f := build(t, "")
		if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
			Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
		}, f.input, f.plan, nil); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if names := podNameSet(t, f.client, f.isvc.Namespace); !names[missing] {
			t.Errorf("paused reconcile with no target left the set half-built; pods = %v", names)
		}
	})
}

// TestPause_UnbackedReadyInstanceDemotesToPending: the pod of a converged
// Instance is deleted while the Component is paused. No pass may recreate it,
// and the row must fall back to Pending rather than keep claiming Ready.
func TestPause_UnbackedReadyInstanceDemotesToPending(t *testing.T) {
	h := newRecoveryHarness(t, false)
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged")
	}

	h.desired.Paused = true
	h.losePods(0)
	h.step()

	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhasePending || s.Operation != nil {
		h.dumpState("paused with no pods")
		t.Fatalf("demotion: got %+v, want Phase=Pending with no operation", s)
	}
	h.step()
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("paused pass recreated %d pod(s)", len(pods))
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhasePending {
		t.Fatalf("demotion did not hold across passes: %+v", s)
	}

	h.desired.Paused = false
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("after unpause")
		t.Fatalf("Instance never recovered after unpause")
	}
}

// pausedUpdate is a single-pod Update attempt already carrying the
// operator's pause token — an attempt held at a step boundary, with its
// deadline parked by the adapter.
func pausedUpdate(waiting string) []workloadtypes.InstanceStatus {
	return []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:    workloadtypes.InstanceOperationUpdate,
			Step:    workloadtypes.UpdateStepSurge,
			Waiting: waiting,
		},
	}}
}

// pausedRecreate is the recreate roll's Phase B row under the same
// pause — the shape the gang arm judges, since a surge SOURCE is never
// read against its own group.
func pausedRecreate(waiting string) []workloadtypes.InstanceStatus {
	rows := pausedUpdate(waiting)
	rows[0].Operation.Step = "Drain"
	return rows
}

// runHoldEvidence drives the hold pass on its own, the way Execute runs
// it for a paused Component: the authorities report, and no repair
// follows.
func runHoldEvidence(t *testing.T, input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, byIdx map[int32][]*corev1.Pod) error {
	t.Helper()
	return workload.HoldPassForTest(context.Background(), workloadtypes.Deps{}, input, plan,
		workload.SnapshotWithPodsForTest(input, byIdx))
}

// TestHoldEvidence_PausedRowRecordsTheSchedulerHold: a pod the scheduler
// cannot place while the Component is paused still gets its hold
// recorded. The scheduler reports a FACT about the Instance and the
// pause reports a decision, so the fact takes the row; the operator sees
// which constraint has no placement instead of a pause token that hides
// it.
func TestHoldEvidence_PausedRowRecordsTheSchedulerHold(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(pausedUpdate(workloadtypes.WaitingReasonPaused))

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q (the fact takes the paused row)", got.Operation, workloadtypes.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
		t.Errorf("LastFailure: got %+v want the scheduler message %q", got.LastFailure, schedulerMessage)
	}
	if got.Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (the row is queued, not failed)", got.Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestHoldEvidence_PausedRowTakesNoRepairAction: the same row with the
// operator's placement grace long elapsed. Unpaused that is a terminal,
// environment-caused failure; paused it is only a hold, because the half
// that decides repair does not run.
func TestHoldEvidence_PausedRowTakesNoRepairAction(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(pausedUpdate(workloadtypes.WaitingReasonPaused))
	input.UnschedulableGrace = time.Minute

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase == workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got Failed want Updating (a pause withholds the escalation)")
	}
	if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workloadtypes.WaitingReasonUnschedulable)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock writes: got %v want none", rec.blocks)
	}
}

// TestHoldEvidence_PausedRowReleasesTheSchedulerHold: the release runs
// under a pause too. A hold that outlives the placement that ended it
// would keep naming a wait that is over, and the row would report it
// until the operator unpaused.
func TestHoldEvidence_PausedRowReleasesTheSchedulerHold(t *testing.T) {
	input, rec := escalationFixture(pausedUpdate(workloadtypes.WaitingReasonUnschedulable))

	placed := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodPending,
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
		},
	}
	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {placed}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want the scheduler token cleared", got.Operation)
	}
	if got.Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating", got.Phase)
	}
}

// TestHoldEvidence_PausedRowRecordsTheGangHold: the gang arm runs under
// a pause on the same terms. A deterministic name still being collected
// is a fact about the Instance, so it takes the paused row and parks the
// clock on the wait that is real.
func TestHoldEvidence_PausedRowRecordsTheGangHold(t *testing.T) {
	input, rec := escalationFixture(pausedRecreate(workloadtypes.WaitingReasonPaused))
	input.Gangs = gangObservations(0, workloadtypes.GangStateTerminating, gangTerminatingMessage)

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1), nil); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonPodGroupTerminating {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workloadtypes.WaitingReasonPodGroupTerminating)
	}
	if got.Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating", got.Phase)
	}
}

// TestHoldEvidence_PausedRowKeepsTheIncumbentFact: precedence between
// two facts is unchanged by the pause. A row already reporting that its
// source is out of rotation keeps that token — first writer wins between
// authorities that both state something about the Instance, and only the
// pause is ordered below them.
func TestHoldEvidence_PausedRowKeepsTheIncumbentFact(t *testing.T) {
	now := time.Now()
	rows := pausedUpdate(workloadtypes.WaitingReasonSourceUnrouted)
	rows[0].Operation.TargetRevision = "own-engine-targethash"
	input, rec := escalationFixture(rows)

	// The source is out of rotation and the replacement has no
	// placement: two facts, and the one that got there first keeps the
	// row.
	source := servingPod("engine-0-default-0")
	source.Status.Conditions[1].Status = corev1.ConditionFalse
	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {source, unschedulablePod("engine-0-default-1", now.Add(-5*time.Minute))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonSourceUnrouted {
		t.Errorf("Operation.Waiting: got %+v want %q kept", got.Operation, workloadtypes.WaitingReasonSourceUnrouted)
	}
}

// Corrective-edit recovery: after a rollout toward a broken revision
// exhausts its retries (Held), a corrective spec edit — roll-forward or
// roll-back — must reconcile to convergence without operator action.
//
// These tests drive the REAL exhaustion flow — bad target revision →
// stuck pods → escalation → disposition/abandon → Backoff → retry →
// Held — through workload.Reconcile against a fake client with a
// simulated kubelet and a fake clock, then apply a corrective edit and
// assert the loop converges with zero manual intervention. The acceptance
// matrix covers:
//
//   - direction: roll-forward (corrective NEW revision) and roll-back
//     (target returns to the source's exact RunningRevision — zero
//     revision distance, so the update trigger correctly never fires
//     and recovery must arrive via Plan's wreckage scan);
//   - topology: single-pod (surge-ordinal wreckage) and gang
//     (GangSurgeTarget marker + replacement-gang wreckage);
//   - timing: the settled at-Held state and the dirty mid-wreckage
//     state (corrective edit lands while the source is
//     Failed-with-Operation and the crashed attempt's pods are live).
//
// A partial initial Create retarget exercises the same recovery loop before
// any serving source exists.
//
// The serving source must stay in rotation throughout every recovery:
// cleanup only removes superseded-revision debris, never the healthy
// pod (asserted per step via runWithInvariant).

const (
	recoveryNS    = "prod"
	recoveryOwner = "llama-70b"

	goodImage  = "registry.test/serving:v1"
	badImage   = "registry.test/serving:broken"
	fixedImage = "registry.test/serving:v2"
)

// recoveryHarness is the closed reconcile loop: fake client + simulated
// kubelet + fake clock + in-memory RetryBlock store + IR-backed
// InstanceStatus round-trip.
type recoveryHarness struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	isvc *v1beta1.InferenceService
	clk  *clocktesting.FakeClock

	multiPod  bool
	replicas  int32
	lifecycle workloadtypes.Lifecycle
	desired   workloadtypes.WorkloadDesiredSpec
	target    *appsv1.ControllerRevision

	// mutateErr rejects every InstanceStatus write for the listed
	// indices — the API server refusing that Instance's status update
	// on every pass — while other indices round-trip normally.
	mutateErr map[int32]error

	blocks []workloadtypes.RetryBlock
	// requeue is the dispatcher cadence the harness runs under; a test
	// pinning the unconfigured path zeroes it.
	requeue         workloadtypes.RequeueIntervals
	currentRevision string
	heldWarnings    []string

	revV1    *appsv1.ControllerRevision
	revBad   *appsv1.ControllerRevision
	revFixed *appsv1.ControllerRevision
}

func recoveryPodSpec(image string) *corev1.PodSpec {
	return &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
}

// newRecoveryHarness builds the harness plus the three ControllerRevisions
// (v1 good, bad, fixed) minted through the real revision machinery so
// names, hashes, and payloads match production exactly.
func newRecoveryHarness(t *testing.T, multiPod bool) *recoveryHarness {
	t.Helper()
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: recoveryOwner, Namespace: recoveryNS, UID: "uid-1",
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc).Build()

	h := &recoveryHarness{
		t:        t,
		ctx:      context.Background(),
		c:        c,
		isvc:     isvc,
		clk:      clocktesting.NewFakeClock(time.Now()),
		multiPod: multiPod,
		replicas: 1,
		requeue:  testRequeueIntervals,
	}
	h.revV1 = h.ensureRevision(recoveryPodSpec(goodImage))
	h.revBad = h.ensureRevision(recoveryPodSpec(badImage))
	h.revFixed = h.ensureRevision(recoveryPodSpec(fixedImage))
	h.setTarget(h.revV1, goodImage)
	h.currentRevision = ""
	return h
}

func (h *recoveryHarness) revisionKey() revision.Key {
	return revision.Key{
		Namespace: recoveryNS,
		Name:      recoveryOwner + "-" + string(workloadtypes.ComponentEngine),
	}
}

func (h *recoveryHarness) ensureRevision(spec *corev1.PodSpec) *appsv1.ControllerRevision {
	h.t.Helper()
	var workerSpec *corev1.PodSpec
	if h.multiPod {
		workerSpec = spec.DeepCopy()
	}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		h.ctx, h.c, h.c, h.isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		h.revisionKey(), spec, workerSpec, nil, nil, h.isvc.UID,
	)
	if err != nil {
		h.t.Fatalf("EnsureControllerRevision: %v", err)
	}
	return cr
}

// setTarget points the loop at (revision, image) — the harness analogue
// of the ISVC/runtime edit that re-renders the desired pod spec and
// republishes the target ControllerRevision.
func (h *recoveryHarness) setTarget(cr *appsv1.ControllerRevision, image string) {
	h.target = cr
	spec := recoveryPodSpec(image)
	h.desired = workloadtypes.WorkloadDesiredSpec{
		Replicas:  h.replicas,
		PodSpec:   spec,
		Lifecycle: h.lifecycle,
	}
	if h.multiPod {
		h.desired.MultiPod = true
		h.desired.WorkerPodSpec = spec.DeepCopy()
		h.desired.Runners = []workloadtypes.Runner{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	} else {
		h.desired.Runners = []workloadtypes.Runner{{Name: "default", Size: 1}}
	}
}

// kubelet simulates node-side convergence for every live pod: the bad
// image parks in ImagePullBackOff forever; every other image becomes
// ContainersReady, and PodReady once the controller's serving gate is
// True (kubelet ANDs readiness gates into PodReady).
func (h *recoveryHarness) kubelet() {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("kubelet list: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.CreationTimestamp.IsZero() {
			// PodStuckInTerminalWaiting measures stuck age from CreationTimestamp
			// against the fake clock; stamp it deterministically.
			pod.CreationTimestamp = metav1.NewTime(h.clk.Now())
			if err := h.c.Update(h.ctx, pod); err != nil {
				h.t.Fatalf("kubelet stamp creation %s: %v", pod.Name, err)
			}
		}
		if pod.Spec.Containers[0].Image == badImage {
			pod.Status.Phase = corev1.PodPending
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: pod.Spec.Containers[0].Name,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "ImagePullBackOff",
					Message: "Back-off pulling image " + badImage,
				}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
		} else {
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  pod.Spec.Containers[0].Name,
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, h.clk.Now())
			ready := corev1.ConditionFalse
			for _, cond := range pod.Status.Conditions {
				if cond.Type == query.ServingConditionType && cond.Status == corev1.ConditionTrue {
					ready = corev1.ConditionTrue
				}
			}
			setPodCondition(pod, corev1.PodReady, ready, h.clk.Now())
		}
		// Pod status is a built-in subresource on the fake client — a
		// plain Update silently drops status changes.
		if err := h.c.Status().Update(h.ctx, pod); err != nil {
			h.t.Fatalf("kubelet update %s: %v", pod.Name, err)
		}
	}
}

func setPodCondition(pod *corev1.Pod, condType corev1.PodConditionType, status corev1.ConditionStatus, now time.Time) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == condType {
			pod.Status.Conditions[i].Status = status
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type: condType, Status: status, LastTransitionTime: metav1.NewTime(now),
	})
}

func (h *recoveryHarness) irKey() types.NamespacedName {
	return types.NamespacedName{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(workloadtypes.ComponentEngine)}
}

func (h *recoveryHarness) irStatuses() []workloadtypes.InstanceStatus {
	h.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := h.c.Get(h.ctx, h.irKey(), ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		h.t.Fatalf("get IR: %v", err)
	}
	out := make([]workloadtypes.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for _, s := range ir.Status.InstanceStatuses {
		out = append(out, v1beta1convert.InstanceStatusToWorkload(s))
	}
	return out
}

func (h *recoveryHarness) removeInstance() func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		ir := &v1beta1.InferenceReplica{}
		if err := h.c.Get(ctx, h.irKey(), ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
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
		if err := h.c.Status().Update(ctx, ir); err != nil {
			return false, err
		}
		return true, nil
	}
}

// mutateInstance is the harness's status-write seam: the IR round-trip
// for every index, except those mutateErr rejects.
func (h *recoveryHarness) mutateInstance() func(context.Context, int32, func(*workloadtypes.InstanceStatus) bool) error {
	delegate := roundTripMutateInstance(h.c, h.isvc, workloadtypes.ComponentEngine)
	return func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		if err := h.mutateErr[idx]; err != nil {
			return err
		}
		return delegate(ctx, idx, mutate)
	}
}

func (h *recoveryHarness) buildInput() workloadtypes.ReconcileInput {
	in := workloadtypes.ReconcileInput{
		OwnerObject: h.isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: h.isvc,
		Key: workloadtypes.Key{
			Namespace: recoveryNS,
			Component: workloadtypes.ComponentEngine,
			OwnerName: recoveryOwner,
		},
		DesiredSpec: h.desired,
		ObservedState: workloadtypes.WorkloadObservedState{
			InstanceStatuses: h.irStatuses(),
			RetryBlocks:      append([]workloadtypes.RetryBlock(nil), h.blocks...),
			CurrentRevision:  h.currentRevision,
			UpdateRevision:   h.target.Name,
		},
		MutateInstance:    h.mutateInstance(),
		RemoveInstance:    h.removeInstance(),
		UpdateRetryPolicy: &workloadtypes.RetryPolicy{MaxAttempts: 2, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2},
		StuckPodGrace:     30 * time.Second,
		Requeue:           h.requeue,
		Clock:             h.clk,
		WarnRetryHeld: func(rev string, attempts int32, reason string) {
			h.heldWarnings = append(h.heldWarnings, fmt.Sprintf("%s attempts=%d %s", rev, attempts, reason))
		},
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			pos := -1
			b := workloadtypes.RetryBlock{TargetRevision: rev}
			for i := range h.blocks {
				if h.blocks[i].TargetRevision == rev {
					pos, b = i, h.blocks[i]
					break
				}
			}
			switch mutate(&b) {
			case workloadtypes.RetryBlockPersist:
				if pos == -1 {
					h.blocks = append(h.blocks, b)
				} else {
					h.blocks[pos] = b
				}
			case workloadtypes.RetryBlockRemove:
				if pos != -1 {
					h.blocks = append(h.blocks[:pos], h.blocks[pos+1:]...)
				}
			}
			return nil
		},
	}
	stubInputCallbacks(&in)
	return in
}

// step runs one reconcile: kubelet convergence, fresh expectations
// (watch caught up), Reconcile, aggregate CurrentRevision, clock
// advance (the op's own requeue interval, or a 10s baseline). A
// Reconcile error fails the test.
func (h *recoveryHarness) step() {
	h.t.Helper()
	if _, err := h.stepResult(); err != nil {
		h.t.Fatalf("Reconcile: %v", err)
	}
}

// stepResult is step returning the pass's result and error, for tests
// that expect a pass to fail.
func (h *recoveryHarness) stepResult() (ctrl.Result, error) {
	h.t.Helper()
	h.kubelet()
	deps := workloadtypes.Deps{Client: h.c, APIReader: h.c, Expectations: workloadtypes.NewExpectations()}
	in := h.buildInput()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		h.t.Fatalf("BuildPlan: %v", err)
	}
	res, err := workload.Reconcile(h.ctx, deps, in, plan, h.target)
	if status.RolloutComplete(h.irStatuses(), h.target.Name) {
		h.currentRevision = h.target.Name
	}
	// A 30s floor keeps stuck-pod grace and Backoff windows crossing in
	// a handful of passes, and lets an 80-pass wedge run outlive the 30m
	// Operation deadline — proving the deadline backstop is also inert.
	advance := 30 * time.Second
	if res.RequeueAfter > advance {
		advance = res.RequeueAfter
	}
	h.clk.Step(advance)
	return res, err
}

// run steps until pred returns true, up to max iterations. Returns
// whether pred was ever satisfied.
func (h *recoveryHarness) run(max int, pred func() bool) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		if pred() {
			return true
		}
		h.step()
	}
	return pred()
}

// runWithInvariant is run with a per-step invariant check: inv is
// evaluated after every step (and before the first), so a transient
// violation mid-recovery fails the test even if the end state is fine.
func (h *recoveryHarness) runWithInvariant(max int, pred func() bool, inv func()) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		inv()
		if pred() {
			return true
		}
		h.step()
	}
	inv()
	return pred()
}

// servingPods returns the live pods currently carrying the serving
// gate (in LB rotation).
func (h *recoveryHarness) servingPods() []*corev1.Pod {
	var out []*corev1.Pod
	for _, p := range h.livePods() {
		if podreadiness.IsServing(p) {
			out = append(out, p)
		}
	}
	return out
}

func (h *recoveryHarness) findBlock(rev string) *workloadtypes.RetryBlock {
	for i := range h.blocks {
		if h.blocks[i].TargetRevision == rev {
			return &h.blocks[i]
		}
	}
	return nil
}

func (h *recoveryHarness) livePods() []*corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			out = append(out, &pods.Items[i])
		}
	}
	return out
}

func (h *recoveryHarness) badPods() []*corev1.Pod {
	var out []*corev1.Pod
	for _, p := range h.livePods() {
		if p.Spec.Containers[0].Image == badImage {
			out = append(out, p)
		}
	}
	return out
}

// converged reports full convergence on rev: exactly one InstanceStatus,
// Phase=Ready, RunningRevision=rev, no Operation, and zero bad-image
// pods left anywhere (wreckage cleaned).
func (h *recoveryHarness) converged(rev string) bool {
	sts := h.irStatuses()
	if len(sts) != 1 {
		return false
	}
	s := sts[0]
	if s.Phase != workloadtypes.InstancePhaseReady || s.RunningRevision != rev || s.Operation != nil {
		return false
	}
	return len(h.badPods()) == 0
}

func (h *recoveryHarness) dumpState(label string) {
	h.t.Logf("--- %s ---", label)
	for _, s := range h.irStatuses() {
		op := "nil"
		if s.Operation != nil {
			op = fmt.Sprintf("{type=%s step=%s target=%s surgeIdx=%v}", s.Operation.Type, s.Operation.Step, s.Operation.TargetRevision, s.Operation.SurgeIndex)
		}
		h.t.Logf("  instance %d: phase=%s running=%s target=%s op=%s", s.Index, s.Phase, s.RunningRevision, s.TargetRevision, op)
	}
	for _, b := range h.blocks {
		h.t.Logf("  block %s: state=%s attempts=%d", b.TargetRevision, b.State, b.AttemptsStarted)
	}
	for _, p := range h.livePods() {
		h.t.Logf("  pod %s image=%s rev=%s", p.Name, p.Spec.Containers[0].Image, p.Labels[query.LabelRevisionHash])
	}
}

// driveToReadyOnV1 converges the initial create on the good revision.
func (h *recoveryHarness) driveToReadyOnV1() {
	h.t.Helper()
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("initial create")
		h.t.Fatalf("initial create never converged on v1")
	}
}

// driveToHeld publishes the bad revision and drives the real exhaustion
// flow until retryBlocks[bad]=Held.
func (h *recoveryHarness) driveToHeld() {
	h.t.Helper()
	h.setTarget(h.revBad, badImage)
	held := func() bool {
		b := h.findBlock(h.revBad.Name)
		return b != nil && b.State == workloadtypes.RetryBlockHeld
	}
	if !h.run(120, held) {
		h.dumpState("exhaustion")
		h.t.Fatalf("retry exhaustion never reached Held for %s", h.revBad.Name)
	}
	if len(h.heldWarnings) != 1 || !strings.HasPrefix(h.heldWarnings[0], h.revBad.Name) {
		h.t.Fatalf("WarnRetryHeld: got %v, want exactly one warning for %s", h.heldWarnings, h.revBad.Name)
	}
}

// settle runs a few extra passes so the post-Held state reaches its
// steady shape (in-flight abandon/dispose passes complete; nothing new
// starts because the Held gate denies the bad target).
func (h *recoveryHarness) settle(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		h.step()
	}
}

func TestCorrectiveRecovery_Gang_InitialCreateRetarget(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.setTarget(h.revBad, badImage)
	h.step()

	initial := h.livePods()
	if len(initial) != 2 {
		t.Fatalf("initial Create: got %d pods want leader and worker", len(initial))
	}
	removedWorker := false
	for _, pod := range initial {
		if pod.Labels[query.LabelRunner] != "worker" {
			continue
		}
		if err := h.c.Delete(h.ctx, pod); err != nil {
			t.Fatalf("delete worker %s: %v", pod.Name, err)
		}
		removedWorker = true
		break
	}
	if !removedWorker {
		t.Fatal("initial Create did not produce a worker pod")
	}

	h.setTarget(h.revFixed, fixedImage)
	converged := h.runWithInvariant(80,
		func() bool { return h.converged(h.revFixed.Name) },
		func() {
			revisionsByInstance := map[string]map[string]struct{}{}
			for _, pod := range h.livePods() {
				index := pod.Labels[query.LabelInstanceIdx]
				if revisionsByInstance[index] == nil {
					revisionsByInstance[index] = map[string]struct{}{}
				}
				if hash := pod.Labels[query.LabelRevisionHash]; hash != "" {
					revisionsByInstance[index][hash] = struct{}{}
				}
			}
			for index, revisions := range revisionsByInstance {
				if len(revisions) > 1 {
					t.Fatalf("instance %s contains pods from multiple revisions: %v; statuses=%+v", index, revisions, h.irStatuses())
				}
			}
		})
	if !converged {
		h.dumpState("initial Create retarget wedged")
		t.Fatalf("gang initial Create did not converge after a corrective edit")
	}
}

// ---------------------------------------------------------------------------
// Roll-forward: corrective NEW revision.
// ---------------------------------------------------------------------------

// TestCorrectiveRecovery_SinglePod_RollForward covers the corrective
// release of a single-pod instance: Held + Phase=Failed + the
// bad-revision pod still occupying the surge ordinal, then a corrective
// NEW revision. The rollout must converge on the corrective revision
// with zero manual intervention: reclassifyByRevisionHash
// (ops/update_surge.go) routes the dead pod — labeled a third revision,
// neither RunningRevision nor the pinned target — into the drain set,
// the stale-slot eviction clears the surge ordinal, and the fresh surge
// drives to the fixed revision. The serving source must
// stay in rotation throughout: the recovery is SurgeThenDrain, so
// capacity never drops to zero while the wreckage is cleaned.
func TestCorrectiveRecovery_SinglePod_RollForward(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (single-pod)")

	h.setTarget(h.revFixed, fixedImage)
	converged := h.runWithInvariant(80,
		func() bool { return h.converged(h.revFixed.Name) },
		func() {
			if len(h.servingPods()) == 0 {
				t.Fatalf("no pod serving mid-recovery: the cleanup pulled the healthy source out of rotation before the corrective surge was ready")
			}
		})
	if !converged {
		h.dumpState("roll-forward wedged (single-pod)")
		t.Fatalf("single-pod roll-forward after Held did not converge")
	}
}

// TestCorrectiveRecovery_Gang_RollForward pins gang recovery from the
// settled at-Held state (which the abandon path leaves CLEAN: source
// Ready on v1, marker removed, surge pods deleted).
func TestCorrectiveRecovery_Gang_RollForward(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (gang)")

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(80, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("roll-forward wedged")
		t.Fatalf("gang roll-forward after Held did not converge")
	}
}

// TestCorrectiveRecovery_Gang_RollForward_MidWreckage pins the dirty
// timing: the corrective revision lands while the source is
// Failed-with-Operation toward the bad revision, the GangSurgeTarget
// marker still exists, and the crashed surge gang's pods are still
// live. The Failed continuation must dispatch, abandon the stale surge,
// and re-surge toward the corrective revision.
func TestCorrectiveRecovery_Gang_RollForward_MidWreckage(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()

	h.setTarget(h.revBad, badImage)
	if !h.run(60, func() bool { return h.gangMidWreckage() }) {
		h.dumpState("exhaustion")
		t.Fatalf("never reached the mid-wreckage state (source Failed-with-op + marker + live bad pods)")
	}
	h.dumpState("mid-wreckage (gang)")

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(80, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("roll-forward wedged")
		t.Fatalf("gang mid-wreckage roll-forward did not converge")
	}
}

// gangMidWreckage reports the dirty gang state: source instance
// Phase=Failed with a preserved gang-surge Update Operation, the surge
// marker present, and the bad gang's pods live.
func (h *recoveryHarness) gangMidWreckage() bool {
	var source, marker *workloadtypes.InstanceStatus
	sts := h.irStatuses()
	for i := range sts {
		s := &sts[i]
		if s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationUpdate && s.Operation.SurgeIndex != nil {
			source = s
		}
		if s.Operation != nil && s.Operation.Step == workloadtypes.UpdateStepGangSurgeTarget {
			marker = s
		}
	}
	return source != nil && source.Phase == workloadtypes.InstancePhaseFailed &&
		marker != nil && len(h.badPods()) > 0
}

// ---------------------------------------------------------------------------
// Roll-back: corrective target == the exact prior revision — zero
// revision distance, so the update trigger never fires and recovery is
// owned by the wreckage-cleanup path.
// ---------------------------------------------------------------------------

// TestCorrectiveRecovery_SinglePod_RollBack reproduces the undo path
// from the settled at-Held state: the operator reverts the spec so the
// target revision equals the instance's RunningRevision. Stock
// Deployments recover here for free; OMENative must too.
// The update trigger stays revision-diff-keyed and correctly declines
// (pinned below) — recovery arrives via Plan's wreckage scan
// (UpdateItem.CleanupOnly -> ops.CleanupWreckage), which deletes the
// dead surge pod without ever touching the serving source: the
// original v1 pod stays in rotation through the whole undo.
func TestCorrectiveRecovery_SinglePod_RollBack(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (single-pod)")

	// The healthy v1 pod that must keep serving through the undo.
	serving := h.servingPods()
	if len(serving) != 1 || serving[0].Spec.Containers[0].Image != goodImage {
		t.Fatalf("precondition: exactly the good v1 pod serving at Held, got %d serving", len(serving))
	}
	originalPod := serving[0].Name

	// Pin the trigger contract: the pure evaluation declines
	// (RunningRevision == target fast-path) — cleanup is Plan's wreckage
	// scan, not a widened trigger.
	h.setTarget(h.revV1, goodImage)
	in := h.buildInput()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	dec := workloadops.EvaluateUpdateTrigger(in, plan.Instances[0], h.target, nil)
	if dec.Trigger || dec.AdoptRevision || dec.RetryAfter != 0 {
		t.Fatalf("expected the update trigger to keep declining (RunningRevision==target fast-path), got %+v", dec)
	}

	converged := h.runWithInvariant(60,
		func() bool { return h.converged(h.revV1.Name) },
		func() {
			for _, p := range h.servingPods() {
				if p.Name != originalPod {
					t.Fatalf("pod %s entered rotation during the undo; only the original %s may serve", p.Name, originalPod)
				}
			}
			if len(h.servingPods()) == 0 {
				t.Fatalf("the original pod left rotation during the undo (capacity outage)")
			}
		})
	if !converged {
		h.dumpState("roll-back wedged (single-pod)")
		t.Fatalf("single-pod roll-back after Held did not converge")
	}
	if got := h.servingPods(); len(got) != 1 || got[0].Name != originalPod {
		t.Fatalf("the original pod must still be the serving pod after the undo, got %v", got)
	}
}

// TestCorrectiveRecovery_Gang_RollBack_MidWreckage is the gang variant
// of the undo path at the dirty timing: the roll-back lands while the
// source is Failed-with-Operation toward the bad revision, the
// GangSurgeTarget marker is present, and the crashed replacement gang's
// pods are live. Convergence must be automatic: either the
// Failed continuation's abandon tears the wreckage down, or — once the
// source is re-adopted Ready and its operation cleared — the orphaned
// marker falls out of the plan's pinned set (marker-liveness invariant,
// plan.go updateSurgeInFlightIndices) and the scale-down pass reaps the
// dead gang.
func TestCorrectiveRecovery_Gang_RollBack_MidWreckage(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()

	h.setTarget(h.revBad, badImage)
	if !h.run(60, func() bool { return h.gangMidWreckage() }) {
		h.dumpState("exhaustion")
		t.Fatalf("never reached the mid-wreckage state")
	}
	h.dumpState("mid-wreckage (gang)")

	h.setTarget(h.revV1, goodImage)
	if !h.run(80, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("roll-back wedged (gang)")
		t.Fatalf("gang mid-wreckage roll-back did not converge")
	}
}

// TestCorrectiveRecovery_Gang_RollBack_PostHeld: the gang roll-back from
// the SETTLED at-Held state needs no work: the final abandon left the
// source Ready on v1 with no wreckage, so a target equal to v1 is already
// met. The dirty timing is covered by
// TestCorrectiveRecovery_Gang_RollBack_MidWreckage.
func TestCorrectiveRecovery_Gang_RollBack_PostHeld(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (gang)")

	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("post-Held roll-back")
		t.Fatalf("gang roll-back from the clean at-Held state must converge trivially")
	}
}

// A row parked at Failed holds no attempt, so most of what an operator
// can change mid-flight has no reader on it. These tests pin that: the
// row, its RetryBlock and the cluster come out of the pass exactly as
// they went in, whichever knob moved.

// failedRowTarget is the revision the parked row is held on.
const failedRowTarget = "llama-70b-engine-target01"

// failedRowFixture is a disposed fresh start: one Instance stamped
// Failed with no operation, held on its target revision by a Held
// RetryBlock, and no pods. Without the block the Create pass would
// re-materialize it, so the block is what keeps the row parked for the
// length of a pass.
func failedRowFixture(t *testing.T) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan, *appsv1.ControllerRevision, *testAtomicMutationStore, *[]string) {
	t.Helper()
	in := minimalInput(t)
	in.ObservedState.UpdateRevision = failedRowTarget
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseFailed, PodCount: 1},
	}
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{
		{TargetRevision: failedRowTarget, State: workloadtypes.RetryBlockHeld},
	}
	removed := recordRetryBlockRemovals(&in)
	store := installTestAtomicMutationStore(&in)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: failedRowTarget}}
	return in, minimalPlan(), target, store, removed
}

// runFailedRowPass reconciles the fixture once and asserts the row came
// out untouched: no status write, still Failed with no operation, no
// pod materialized, and its block intact.
func runFailedRowPass(t *testing.T, in workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, target *appsv1.ControllerRevision, store *testAtomicMutationStore, removed *[]string) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: c}, in, plan, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if store.writes != 0 {
		t.Errorf("status writes: got %d want 0", store.writes)
	}
	got := store.status(0)
	if got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Operation != nil {
		t.Errorf("row: got %+v want Failed with no operation", got)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(in.Key.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("pods: got %d want none; the Held block denies the fresh start", len(pods.Items))
	}
	if len(*removed) != 0 {
		t.Errorf("RetryBlock removals: got %v want none; the row's block is for a live revision", *removed)
	}
}

// TestFailedRow_OperatorConfigChangesLeaveItParked: the requeue cadence,
// the gang schedule clamp and the migration audit caps are all consulted
// by work in flight — a wake-up to schedule, a PodGroup to build, a
// migration to admit. A Failed row has none of those, so changing any of
// them reaches it with no reader.
func TestFailedRow_OperatorConfigChangesLeaveItParked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*workloadtypes.ReconcileInput, *workloadtypes.ComponentPlan)
	}{
		{
			name: "requeue cadence",
			change: func(in *workloadtypes.ReconcileInput, _ *workloadtypes.ComponentPlan) {
				in.Requeue = workloadtypes.RequeueIntervals{Operation: 90 * time.Second, Gate: 45 * time.Second}
			},
		},
		{
			name: "gang schedule clamp",
			change: func(_ *workloadtypes.ReconcileInput, plan *workloadtypes.ComponentPlan) {
				plan.GangScheduleTimeout = &workloadtypes.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}
			},
		},
		{
			name: "migration audit caps",
			change: func(in *workloadtypes.ReconcileInput, _ *workloadtypes.ComponentPlan) {
				in.MigrationAudit = &workloadtypes.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 2, Window: time.Hour}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, plan, target, store, removed := failedRowFixture(t)
			tc.change(&in, &plan)
			runFailedRowPass(t, in, plan, target, store, removed)
		})
	}
}

// TestFailedRow_ExcludedAnnotationEditDoesNotRedriveIt: an edit to an
// annotation the revision payload filters out leaves the hash where it
// was, so the row's target does not move and the pass has nothing new to
// act on. The hash check is half the claim — without it, a pass that
// wrote nothing would only mean the edit never reached the renderer.
func TestFailedRow_ExcludedAnnotationEditDoesNotRedriveIt(t *testing.T) {
	in, plan, target, store, removed := failedRowFixture(t)
	before := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"}}
	after := &metav1.ObjectMeta{Annotations: map[string]string{
		"ome.io/base-model-name":                   "llama-7b",
		constants.ReleaseHeldRevisionAnnotationKey: failedRowTarget,
	}}
	hBefore, _, err := revision.Hash(in.DesiredSpec.PodSpec, before, nil, "")
	if err != nil {
		t.Fatalf("hash before the edit: %v", err)
	}
	hAfter, _, err := revision.Hash(in.DesiredSpec.PodSpec, after, nil, "")
	if err != nil {
		t.Fatalf("hash after the edit: %v", err)
	}
	if hBefore != hAfter {
		t.Fatalf("an excluded annotation moved the hash: before=%s after=%s", hBefore, hAfter)
	}

	in.DesiredSpec.PodTemplateObjectMeta = after
	runFailedRowPass(t, in, plan, target, store, removed)
}

// TestFailedRow_SupersedePruneKeepsTheBlockItWaitsOn: the end-of-pass
// prune is what bounds persisted RetryBlock history, and it is scoped to
// revisions nothing targets anymore. However much history it drops, the
// block holding this row — which is for a live revision — is never a
// candidate, and the row itself is not written either way.
func TestFailedRow_SupersedePruneKeepsTheBlockItWaitsOn(t *testing.T) {
	in, plan, target, store, removed := failedRowFixture(t)
	in.ObservedState.RetryBlocks = append(in.ObservedState.RetryBlocks,
		workloadtypes.RetryBlock{TargetRevision: "llama-70b-engine-stale001", State: workloadtypes.RetryBlockHeld},
		workloadtypes.RetryBlock{TargetRevision: "llama-70b-engine-stale002", State: workloadtypes.RetryBlockBackoff},
	)

	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: c}, in, plan, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	want := []string{"llama-70b-engine-stale001", "llama-70b-engine-stale002"}
	if len(*removed) != len(want) {
		t.Fatalf("pruned: got %v want %v", *removed, want)
	}
	for i, rev := range want {
		if (*removed)[i] != rev {
			t.Fatalf("pruned: got %v want %v", *removed, want)
		}
	}
	if store.writes != 0 {
		t.Errorf("status writes: got %d want 0", store.writes)
	}
	if got := store.status(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Operation != nil {
		t.Errorf("row: got %+v want Failed with no operation", got)
	}
}

type staleInPlacePodClient struct {
	client.Client
	stale bool
	pods  []*corev1.Pod
}

func (c *staleInPlacePodClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if !c.stale {
		return c.Client.List(ctx, list, opts...)
	}
	pods, ok := list.(*corev1.PodList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	pods.Items = make([]corev1.Pod, 0, len(c.pods))
	for _, pod := range c.pods {
		pods.Items = append(pods.Items, *pod.DeepCopy())
	}
	return nil
}

// crashLoopRepairRevision is the revision the wedged pods below carry;
// its hash matches the label enginePod stamps.
const crashLoopRepairRevision = "llama-70b-engine-priorrev"

// Teardown-mode dispatcher contract (owner deletion): the planned index
// set is treated as empty so EVERY observed Instance runs the ordinary
// Delete pipeline, and nothing else runs — no Paused gate, no
// Restart / Migrate / Update / Create. The caller owns completion
// detection and finalizer decisions, so the dispatcher only reports
// "deletes still in flight" (the configured poll interval) or "all observed
// Instances resolved" (zero result).

// TestReconcile_Teardown_DeletesEveryObservedInstance drives teardown
// against three observed Instances (with live pods) while the plan
// covers only index 0. All three indices — INCLUDING the planned one —
// must be Delete-dispatched, no pod may be created, and neither the
// migration detector nor the update pass may run.
func TestReconcile_Teardown_DeletesEveryObservedInstance(t *testing.T) {
	scheme := makeScheme(t)
	in := minimalInput(t)
	objs := []client.Object{
		enginePod("llama-70b", "prod", 0),
		enginePod("llama-70b", "prod", 1),
		enginePod("llama-70b", "prod", 2),
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	// A non-terminal Manual record that would be dispatched on a normal
	// reconcile; teardown must never reach the migration pass.
	in.ObservedState.Migrations = []workloadtypes.MigrationRecord{{
		RequestUUID: "teardown-mig", Trigger: workloadtypes.MigrationTriggerManual,
		Phase: workloadtypes.MigrationPhaseAccepted, SourceInstance: 0,
	}}
	migrationMutated := false
	in.MutateMigration = func(_ context.Context, _ string, _ func(*workloadtypes.MigrationRecord) bool) error {
		migrationMutated = true
		return nil
	}

	plan := minimalPlan() // covers only index 0
	plan.MigrationMode = workloadtypes.MigrationModeAuto
	// A non-nil target would arm the Update pass on a normal reconcile;
	// teardown must never reach it.
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("teardown admission must requeue immediately, got %+v", result)
	}
	for _, idx := range []int32{0, 1, 2} {
		status := store.status(idx)
		if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting || status.Operation == nil || status.Operation.Type != workloadtypes.InstanceOperationDelete {
			t.Errorf("instance %d was not durably admitted for teardown: %+v", idx, status)
		}
	}
	if migrationMutated {
		t.Errorf("teardown must not run the migration pass")
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 3 {
		t.Fatalf("admission pass must have zero Pod effects; got %d Pods", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("effect pass: %v", err)
	}
	if result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("teardown effect pass must use delete cadence, got %+v", result)
	}
	pods = &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods after effect pass: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("teardown must delete every Pod and create none; %d remain", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("completion pass: %v", err)
	}
	if !result.Requeue || len(store.statuses) != 0 {
		t.Fatalf("teardown completion result/statuses = %+v/%+v", result, store.statuses)
	}
}

// TestReconcile_Teardown_DeletesMigratingPair pins the mid-migration
// teardown contract: a Phase=Migrating source AND its
// Operation.Type=Migrate surge — both excluded from the NORMAL
// scale-down pass so Migrate isn't ripped apart — must each get a
// Delete dispatch under Teardown. Excluding them would leave their
// pods with no Delete op, no drain, no force-delete escalation: the
// teardown wedges forever (strict hold) or falls to un-drained GC at
// the deadline.
func TestReconcile_Teardown_DeletesMigratingPair(t *testing.T) {
	scheme := makeScheme(t)
	objs := []client.Object{
		enginePod("llama-70b", "prod", 0), // migration source
		enginePod("llama-70b", "prod", 7), // surge
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseMigrating},
		{Index: 7, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{Type: workloadtypes.InstanceOperationMigrate}},
	}
	store := installTestAtomicMutationStore(&in)

	plan := minimalPlan() // covers only index 0
	plan.MigrationMode = workloadtypes.MigrationModeAuto

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("mid-migration teardown admission must requeue immediately, got %+v", result)
	}
	for _, idx := range []int32{0, 7} {
		status := store.status(idx)
		if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting || status.Operation == nil || status.Operation.Type != workloadtypes.InstanceOperationDelete {
			t.Errorf("mid-migration instance %d was not durably admitted: %+v", idx, status)
		}
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("admission pass must leave the migration pair untouched; %d Pods remain", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("effect pass: %v", err)
	}
	if result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("mid-migration teardown effect result = %+v", result)
	}
	pods = &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods after effect pass: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("teardown must delete the source and surge Pods; %d remain", len(pods.Items))
	}
}

// TestReconcile_Teardown_NoInstancesNoPods_CleanReturn pins the empty
// case: nothing observed, nothing live → immediate zero result, no
// callback fires. The caller's completion check owns the rest.
func TestReconcile_Teardown_NoInstancesNoPods_CleanReturn(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	mutations, removals := 0, 0
	in.MutateInstance = func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
		mutations++
		return nil
	}
	in.RemoveInstance = func(_ context.Context, _ int32) (bool, error) {
		removals++
		return false, nil
	}

	result, err := workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("empty teardown must return a zero result, got %+v", result)
	}
	if mutations != 0 || removals != 0 {
		t.Errorf("empty teardown must fire no status callbacks; mutations=%d removals=%d", mutations, removals)
	}
}

// TestReconcile_Teardown_PodlessInstances_RemovedAndDone pins the
// converged shape: observed Instances whose Pods are already gone still use a
// durable admission pass, a batched removal pass, and a final verification.
func TestReconcile_Teardown_PodlessInstances_RemovedAndDone(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseDeleting},
	}
	store := installTestAtomicMutationStore(&in)

	result, err := workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("admission pass: %v", err)
	}
	if !result.Requeue || len(store.statuses) != 2 {
		t.Fatalf("podless admission result/statuses = %+v/%+v", result, store.statuses)
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("completion pass: %v", err)
	}
	if !result.Requeue || len(store.statuses) != 0 {
		t.Fatalf("podless completion result/statuses = %+v/%+v", result, store.statuses)
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("verification pass: %v", err)
	}
	if !result.IsZero() {
		t.Errorf("fully resolved teardown must return zero, got %+v", result)
	}
}

// TestReconcile_Teardown_IgnoresPaused pins that teardown short-circuits
// ahead of the Paused gate: an operator pause must not hold a deletion's
// Delete dispatch (deletion is the stronger intent).
func TestReconcile_Teardown_IgnoresPaused(t *testing.T) {
	scheme := makeScheme(t)
	pod := enginePod("llama-70b", "prod", 0)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	plan := minimalPlan()
	plan.Paused = true

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("admission pass: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("paused teardown admission must requeue immediately, got %+v", result)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("admission pass must leave the Pod untouched; %d remain", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("effect pass: %v", err)
	}
	if result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("paused teardown effect pass must use delete cadence, got %+v", result)
	}
	pods = &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods after effect pass: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("paused teardown must still delete the Pod; %d remain", len(pods.Items))
	}
}

// Terminal-pod recovery: a pod the kubelet rejects at admission (phase
// Failed, no container ever started) still occupies its stable name. The
// owning operation must delete it and recreate the target within the same
// attempt, paced by the update retry ladder and bounded by the attempt's
// deadline; a gang recycles as a whole.
//
// The harness is the closed-loop recoveryHarness with a kubelet that
// rejects admissions on demand: fake client + fake clock + IR-backed
// InstanceStatus round-trip + in-memory RetryBlocks.

const (
	admissionRejectReason = "UnexpectedAdmissionError"
	// admissionStep is the loop's clock advance per pass — the op requeue
	// interval, so pacing can be observed at that resolution.
	admissionStep = 5 * time.Second
)

// admission records one pod object the kubelet saw for the first time.
type admission struct {
	name        string
	incarnation string
	at          time.Time
	rejected    bool
	// op is the Instance's operation that issued the pod (nil when none).
	op *workloadtypes.InstanceOperation
}

type admissionHarness struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	isvc *v1beta1.InferenceService
	clk  *clocktesting.FakeClock

	desired workloadtypes.WorkloadDesiredSpec
	target  *appsv1.ControllerRevision
	policy  *workloadtypes.RetryPolicy

	// reject decides whether the kubelet rejects the n-th pod object
	// (1-based) it sees under a name.
	reject func(name string, n int) bool
	seen   map[string]int

	admissions      []admission
	currentRevision string
	blocks          []workloadtypes.RetryBlock
	phases          map[workloadtypes.InstancePhase]int
	opTypes         map[workloadtypes.InstanceOperationType]int
}

func newAdmissionHarness(t *testing.T, multiPod bool, reject func(name string, n int) bool) *admissionHarness {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: recoveryOwner, Namespace: recoveryNS, UID: "uid-1",
	}}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc).Build()
	h := &admissionHarness{
		t:    t,
		ctx:  context.Background(),
		c:    c,
		isvc: isvc,
		// Status timestamps round-trip at second precision.
		clk:     clocktesting.NewFakeClock(time.Now().Truncate(time.Second)),
		policy:  &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2},
		reject:  reject,
		seen:    map[string]int{},
		phases:  map[workloadtypes.InstancePhase]int{},
		opTypes: map[workloadtypes.InstanceOperationType]int{},
	}
	spec := recoveryPodSpec(goodImage)
	var workerSpec *corev1.PodSpec
	if multiPod {
		workerSpec = spec.DeepCopy()
	}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		h.ctx, h.c, h.c, h.isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		revision.Key{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(workloadtypes.ComponentEngine)},
		spec, workerSpec, nil, nil, h.isvc.UID,
	)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	h.target = cr
	// BuildPlan carries only the per-resource readiness window — the operator
	// ConfigMap fallback is the adapter's job — so the harness sets one.
	h.desired = workloadtypes.WorkloadDesiredSpec{
		Replicas: 1,
		PodSpec:  spec,
		Lifecycle: workloadtypes.Lifecycle{
			InstanceReadyTimeout: &metav1.Duration{Duration: 30 * time.Minute},
		},
	}
	if multiPod {
		h.desired.MultiPod = true
		h.desired.WorkerPodSpec = workerSpec
		h.desired.Runners = []workloadtypes.Runner{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	} else {
		h.desired.Runners = []workloadtypes.Runner{{Name: "default", Size: 1}}
	}
	return h
}

func (h *admissionHarness) irKey() types.NamespacedName {
	return types.NamespacedName{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(workloadtypes.ComponentEngine)}
}

func (h *admissionHarness) irStatuses() []workloadtypes.InstanceStatus {
	h.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := h.c.Get(h.ctx, h.irKey(), ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		h.t.Fatalf("get IR: %v", err)
	}
	out := make([]workloadtypes.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for _, s := range ir.Status.InstanceStatuses {
		out = append(out, v1beta1convert.InstanceStatusToWorkload(s))
	}
	return out
}

// status returns the single Instance's status, or nil before it exists.
func (h *admissionHarness) status() *workloadtypes.InstanceStatus {
	for _, s := range h.irStatuses() {
		if s.Index == 0 {
			return &s
		}
	}
	return nil
}

func (h *admissionHarness) removeInstance() func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		ir := &v1beta1.InferenceReplica{}
		if err := h.c.Get(ctx, h.irKey(), ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index != idx {
				continue
			}
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses[:i], ir.Status.InstanceStatuses[i+1:]...)
			return true, h.c.Status().Update(ctx, ir)
		}
		return false, nil
	}
}

func (h *admissionHarness) buildInput() workloadtypes.ReconcileInput {
	in := workloadtypes.ReconcileInput{
		OwnerObject: h.isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: h.isvc,
		Key: workloadtypes.Key{
			Namespace: recoveryNS,
			Component: workloadtypes.ComponentEngine,
			OwnerName: recoveryOwner,
		},
		DesiredSpec: h.desired,
		ObservedState: workloadtypes.WorkloadObservedState{
			InstanceStatuses: h.irStatuses(),
			RetryBlocks:      append([]workloadtypes.RetryBlock(nil), h.blocks...),
			CurrentRevision:  h.currentRevision,
			UpdateRevision:   h.target.Name,
		},
		MutateInstance:    roundTripMutateInstance(h.c, h.isvc, workloadtypes.ComponentEngine),
		RemoveInstance:    h.removeInstance(),
		UpdateRetryPolicy: h.policy,
		StuckPodGrace:     30 * time.Second,
		Clock:             h.clk,
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			pos := -1
			b := workloadtypes.RetryBlock{TargetRevision: rev}
			for i := range h.blocks {
				if h.blocks[i].TargetRevision == rev {
					pos, b = i, h.blocks[i]
					break
				}
			}
			switch mutate(&b) {
			case workloadtypes.RetryBlockPersist:
				if pos == -1 {
					h.blocks = append(h.blocks, b)
				} else {
					h.blocks[pos] = b
				}
			case workloadtypes.RetryBlockRemove:
				if pos != -1 {
					h.blocks = append(h.blocks[:pos], h.blocks[pos+1:]...)
				}
			}
			return nil
		},
	}
	stubInputCallbacks(&in)
	return in
}

// kubelet decides admission for every pod object it sees for the first
// time: a rejected pod parks in phase Failed with no container started; an
// admitted pod runs, becomes ContainersReady, and PodReady once the
// controller's serving gate is True. Terminal pods are never touched again.
func (h *admissionHarness) kubelet() {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("kubelet list: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) {
			continue
		}
		if pod.Status.Phase == "" {
			pod.CreationTimestamp = metav1.NewTime(h.clk.Now())
			if err := h.c.Update(h.ctx, pod); err != nil {
				h.t.Fatalf("kubelet stamp creation %s: %v", pod.Name, err)
			}
			h.seen[pod.Name]++
			rejected := h.reject != nil && h.reject(pod.Name, h.seen[pod.Name])
			var op *workloadtypes.InstanceOperation
			if s := h.status(); s != nil {
				op = s.Operation
			}
			h.admissions = append(h.admissions, admission{
				name:        pod.Name,
				incarnation: pod.Labels[query.LabelInstanceIncarnation],
				at:          h.clk.Now(),
				rejected:    rejected,
				op:          op,
			})
			if rejected {
				pod.Status.Phase = corev1.PodFailed
				pod.Status.Reason = admissionRejectReason
				pod.Status.Message = "Pod was rejected: node resources unavailable"
				pod.Status.ContainerStatuses = nil
				setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
				setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
				if err := h.c.Status().Update(h.ctx, pod); err != nil {
					h.t.Fatalf("kubelet reject %s: %v", pod.Name, err)
				}
				continue
			}
		}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  pod.Spec.Containers[0].Name,
			Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, h.clk.Now())
		ready := corev1.ConditionFalse
		for _, cond := range pod.Status.Conditions {
			if cond.Type == query.ServingConditionType && cond.Status == corev1.ConditionTrue {
				ready = corev1.ConditionTrue
			}
		}
		setPodCondition(pod, corev1.PodReady, ready, h.clk.Now())
		if err := h.c.Status().Update(h.ctx, pod); err != nil {
			h.t.Fatalf("kubelet update %s: %v", pod.Name, err)
		}
	}
}

func (h *admissionHarness) pods() []corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}

// step runs one pass: kubelet, fresh expectations (watch caught up),
// Reconcile, bookkeeping, then the clock advances by the pass's requeue
// (at least admissionStep).
func (h *admissionHarness) step() {
	h.t.Helper()
	h.kubelet()
	deps := workloadtypes.Deps{Client: h.c, APIReader: h.c, Expectations: workloadtypes.NewExpectations(), Clock: h.clk}
	in := h.buildInput()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		h.t.Fatalf("BuildPlan: %v", err)
	}
	res, err := workload.Reconcile(h.ctx, deps, in, plan, h.target)
	if err != nil {
		h.t.Fatalf("Reconcile: %v", err)
	}
	if status.RolloutComplete(h.irStatuses(), h.target.Name) {
		h.currentRevision = h.target.Name
	}
	h.observe()
	advance := admissionStep
	if res.RequeueAfter > advance {
		advance = res.RequeueAfter
	}
	h.clk.Step(advance)
}

// observe records the phases and operations seen after a pass and checks
// the per-pass invariants: a pod created this pass was issued by an
// attempt that has not passed its deadline, and a recycling operation
// (Create or Restart) never creates a replacement while a terminal pod of
// the same Instance is still standing. A SurgeThenDrain Update is exempt
// from the second check: its replacement legitimately runs at the other
// ordinal next to the pod it replaces.
func (h *admissionHarness) observe() {
	h.t.Helper()
	s := h.status()
	if s != nil {
		h.phases[s.Phase]++
		if s.Operation != nil {
			h.opTypes[s.Operation.Type]++
		}
	}
	fresh, terminal := 0, 0
	pods := h.pods()
	for i := range pods {
		switch {
		case query.IsTerminalPod(&pods[i]):
			terminal++
		case pods[i].Status.Phase == "":
			fresh++
		}
	}
	if fresh > 0 {
		if s == nil || s.Operation == nil {
			h.t.Fatalf("a pod was created with no operation owning the Instance: %+v", s)
		}
		if h.clk.Now().After(s.Operation.Deadline.Time) {
			h.t.Fatalf("a pod was created at %v, after the attempt's deadline %v", h.clk.Now(), s.Operation.Deadline.Time)
		}
	}
	recycling := s != nil && s.Operation != nil &&
		(s.Operation.Type == workloadtypes.InstanceOperationCreate || s.Operation.Type == workloadtypes.InstanceOperationRestart)
	if recycling && fresh > 0 && terminal > 0 {
		h.dump("mixed fresh and terminal pods")
		h.t.Fatalf("%s created a replacement next to %d terminal pod(s) of the same Instance", s.Operation.Type, terminal)
	}
}

func (h *admissionHarness) run(max int, pred func() bool) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		if pred() {
			return true
		}
		h.step()
	}
	return pred()
}

// converged reports Phase=Ready on the target with no operation and every
// pod live and ContainersReady.
func (h *admissionHarness) converged() bool {
	s := h.status()
	if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.RunningRevision != h.target.Name || s.Operation != nil {
		return false
	}
	pods := h.pods()
	if len(pods) == 0 {
		return false
	}
	for i := range pods {
		if query.IsTerminalPod(&pods[i]) || status.CountReadyPods([]*corev1.Pod{&pods[i]}) == 0 {
			return false
		}
	}
	return true
}

func (h *admissionHarness) dump(label string) {
	h.t.Logf("--- %s at %v ---", label, h.clk.Now())
	if s := h.status(); s != nil {
		op := "nil"
		if s.Operation != nil {
			op = fmt.Sprintf("{type=%s step=%s retries=%d deadline=%v}", s.Operation.Type, s.Operation.Step, s.Operation.RetryCount, s.Operation.Deadline.Time)
		}
		h.t.Logf("  instance %d: phase=%s incarnation=%d op=%s", s.Index, s.Phase, s.Incarnation, op)
	}
	for _, p := range h.pods() {
		h.t.Logf("  pod %s phase=%s incarnation=%s", p.Name, p.Status.Phase, p.Labels[query.LabelInstanceIncarnation])
	}
	for _, a := range h.admissions {
		id := "-"
		if a.op != nil {
			id = fmt.Sprintf("%s#%d", a.op.ID, a.op.RetryCount)
		}
		h.t.Logf("  admission %s at %v rejected=%v op=%s", a.name, a.at, a.rejected, id)
	}
}

// (a) A single-pod Instance whose pod dies at admission is repaired within
// the same Create attempt: the dead pod is deleted, the target recreated,
// and the Instance reaches Ready when the replacement does.
func TestTerminalPodRecovery_SinglePod_RecycledWithinAttempt(t *testing.T) {
	h := newAdmissionHarness(t, false, func(_ string, n int) bool { return n == 1 })
	if !h.run(40, h.converged) {
		h.dump("single-pod recycle wedged")
		t.Fatalf("Instance never reached Ready after its pod died at admission")
	}
	if len(h.admissions) != 2 || !h.admissions[0].rejected || h.admissions[1].rejected || h.admissions[0].name != h.admissions[1].name {
		h.dump("admissions")
		t.Fatalf("expected one rejected admission followed by one admitted replacement of the same name, got %d admissions", len(h.admissions))
	}
	first, second := h.admissions[0].op, h.admissions[1].op
	if first == nil || second == nil || first.ID != second.ID || first.Type != workloadtypes.InstanceOperationCreate {
		h.dump("attempts")
		t.Fatalf("the replacement must be issued by the original Create attempt, got %+v then %+v", first, second)
	}
	if second.RetryCount != 1 {
		t.Fatalf("the replacement must follow one recorded recycle, got RetryCount=%d", second.RetryCount)
	}
	if h.phases[workloadtypes.InstancePhaseFailed] > 0 {
		t.Fatalf("the attempt must not escalate to Failed while it is repairing itself")
	}
	if s := h.status(); s.LastFailure == nil || s.LastFailure.Reason != admissionRejectReason {
		t.Fatalf("LastFailure must preserve the admission failure, got %+v", s.LastFailure)
	}
}

// (b) A gang whose every member dies at admission is deleted and recreated
// together, by Create at the same incarnation, and reaches Ready.
func TestTerminalPodRecovery_Gang_AllMembersRecycledTogether(t *testing.T) {
	h := newAdmissionHarness(t, true, func(_ string, n int) bool { return n == 1 })
	if !h.run(40, h.converged) {
		h.dump("gang recycle wedged")
		t.Fatalf("gang never reached Ready after both members died at admission")
	}
	if len(h.admissions) != 4 {
		h.dump("admissions")
		t.Fatalf("expected two rejected admissions and two admitted replacements, got %d", len(h.admissions))
	}
	if !h.admissions[2].at.Equal(h.admissions[3].at) {
		t.Fatalf("replacements must be created together, got %v and %v", h.admissions[2].at, h.admissions[3].at)
	}
	if h.opTypes[workloadtypes.InstanceOperationRestart] > 0 {
		t.Fatalf("total loss is Create's to rebuild; no Restart may run")
	}
	if s := h.status(); s.Incarnation != 1 {
		t.Fatalf("a whole-gang recycle by Create keeps the incarnation, got %d", s.Incarnation)
	}
	for _, p := range h.pods() {
		if p.Labels[query.LabelInstanceIncarnation] != "1" {
			t.Fatalf("pod %s carries incarnation %q, want 1", p.Name, p.Labels[query.LabelInstanceIncarnation])
		}
	}
}

// (d) Repeated rejections space recreates by the retry ladder within one
// attempt, and no recreate outlives the attempt's deadline; the expired
// attempt is escalated and disposed, and only a different operation may
// touch the Instance afterwards.
func TestTerminalPodRecovery_RecreatesFollowLadderWithinDeadline(t *testing.T) {
	h := newAdmissionHarness(t, false, func(string, int) bool { return true })
	h.desired.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 4 * time.Minute}
	for i := 0; i < 60; i++ {
		h.step()
	}

	// Group admissions by attempt, in order of first appearance.
	var attempts [][]admission
	byID := map[string]int{}
	for _, a := range h.admissions {
		if a.op == nil {
			h.dump("admission without an attempt")
			t.Fatalf("admission of %s at %v was not issued by an attempt", a.name, a.at)
		}
		pos, ok := byID[a.op.ID]
		if !ok {
			pos = len(attempts)
			byID[a.op.ID] = pos
			attempts = append(attempts, nil)
		}
		attempts[pos] = append(attempts[pos], a)
	}
	if len(attempts) < 2 {
		h.dump("attempts")
		t.Fatalf("expected the expired attempt to be disposed and a fresh one started, got %d attempt(s)", len(attempts))
	}
	first := attempts[0]
	if len(first) < 4 {
		h.dump("first attempt")
		t.Fatalf("expected several paced recreates within the first attempt, got %d admission(s)", len(first))
	}
	for i := range first {
		if a := first[i]; a.at.After(a.op.Deadline.Time) {
			t.Fatalf("recreate %d at %v outlived the attempt's deadline %v", i, a.at, a.op.Deadline.Time)
		}
		if first[i].op.RetryCount != int32(i) {
			t.Fatalf("recreate %d must follow %d recorded recycle(s), got RetryCount=%d", i, i, first[i].op.RetryCount)
		}
	}
	// The first recycle is immediate; each later one waits the ladder delay
	// measured from the previous recycle.
	if gap := first[1].at.Sub(first[0].at); gap > 2*admissionStep {
		t.Fatalf("first recycle must be immediate, got %v", gap)
	}
	for i := 1; i+1 < len(first); i++ {
		want := h.policy.NextRetryDelay(int32(i))
		gap := first[i+1].at.Sub(first[i].at)
		if gap < want || gap > want+admissionStep {
			h.dump("ladder")
			t.Fatalf("recreate %d -> %d spaced %v, want the ladder delay %v", i, i+1, gap, want)
		}
	}
	if h.phases[workloadtypes.InstancePhaseFailed] == 0 {
		t.Fatalf("the expired attempt must escalate to Failed at its deadline")
	}
	if second := attempts[1]; !second[0].at.After(first[0].op.Deadline.Time) {
		t.Fatalf("the fresh attempt's first recreate at %v must follow the expired attempt's deadline %v", second[0].at, first[0].op.Deadline.Time)
	}
}

// admissionsByIncarnation counts admitted-or-rejected pod objects per
// incarnation label.
func (h *admissionHarness) admissionsByIncarnation() map[string]int {
	out := map[string]int{}
	for _, a := range h.admissions {
		out[a.incarnation]++
	}
	return out
}

// requireNoGapFill fails when a pod was created at an incarnation older
// than one already seen: a gang gap filled next to survivors instead of a
// whole-gang rebuild at a bumped incarnation.
func (h *admissionHarness) requireNoGapFill() {
	h.t.Helper()
	newest := int64(0)
	for _, a := range h.admissions {
		inc, err := strconv.ParseInt(a.incarnation, 10, 64)
		if err != nil {
			h.t.Fatalf("pod %s carries an unreadable incarnation label %q: %v", a.name, a.incarnation, err)
		}
		if inc < newest {
			h.dump("gap fill")
			h.t.Fatalf("pod %s was created at incarnation %d after incarnation %d existed: a gap fill next to survivors", a.name, inc, newest)
		}
		newest = inc
	}
}

// (c) A gang with one dead member and one live member is rebuilt as a
// whole by Restart: incarnation bump, the survivor drained and deleted,
// both members recreated together — never a single-member gap fill next
// to the survivor.
func TestTerminalPodRecovery_Gang_SurvivorRebuiltAsWhole(t *testing.T) {
	leader := query.PodName(recoveryOwner, workloadtypes.ComponentEngine, 0, "leader", 0)
	worker := query.PodName(recoveryOwner, workloadtypes.ComponentEngine, 0, "worker", 0)
	h := newAdmissionHarness(t, true, func(name string, n int) bool { return name == leader && n == 1 })
	if !h.run(40, h.converged) {
		h.dump("gang with survivor wedged")
		t.Fatalf("gang never reached Ready after one member died at admission")
	}
	if h.opTypes[workloadtypes.InstanceOperationRestart] == 0 {
		t.Fatalf("a gang with a survivor must be rebuilt by Restart, not gap-filled by Create")
	}
	if s := h.status(); s.Incarnation != 2 {
		t.Fatalf("the rebuild must bump the incarnation, got %d", s.Incarnation)
	}
	h.requireNoGapFill()
	if byInc := h.admissionsByIncarnation(); byInc["1"] != 2 || byInc["2"] != 2 || len(h.admissions) != 4 {
		h.dump("admissions")
		t.Fatalf("expected the initial pair at incarnation 1 and one whole-gang recreate at incarnation 2, got %v", byInc)
	}
	seenWorker := 0
	for _, a := range h.admissions {
		if a.name == worker {
			seenWorker++
		}
	}
	if seenWorker != 2 {
		t.Fatalf("the surviving worker must be drained and recreated with the gang, got %d worker admissions", seenWorker)
	}
	for _, p := range h.pods() {
		if p.Labels[query.LabelInstanceIncarnation] != "2" {
			t.Fatalf("pod %s still runs at incarnation %q after the rebuild", p.Name, p.Labels[query.LabelInstanceIncarnation])
		}
	}
}

// (c, spent attempt) When the dead member keeps dying, the Restart attempt
// expires to Failed with its operation preserved; that spent attempt must
// re-arm into a new whole-gang Restart (another incarnation bump, survivor
// drained again) instead of wedging or letting Create gap-fill.
func TestTerminalPodRecovery_Gang_SpentRestartReArms(t *testing.T) {
	leader := query.PodName(recoveryOwner, workloadtypes.ComponentEngine, 0, "leader", 0)
	worker := query.PodName(recoveryOwner, workloadtypes.ComponentEngine, 0, "worker", 0)
	h := newAdmissionHarness(t, true, func(name string, n int) bool { return name == leader && n <= 5 })
	h.desired.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 2 * time.Minute}
	if !h.run(80, h.converged) {
		h.dump("spent restart wedged")
		t.Fatalf("gang never reached Ready after its Restart attempt expired")
	}
	if h.phases[workloadtypes.InstancePhaseFailed] == 0 {
		t.Fatalf("the first Restart attempt must expire to Failed before re-arming")
	}
	s := h.status()
	if s.Incarnation < 3 {
		t.Fatalf("the re-armed Restart must bump the incarnation again, got %d", s.Incarnation)
	}
	h.requireNoGapFill()
	seenWorker := 0
	for _, a := range h.admissions {
		if a.name == worker {
			seenWorker++
		}
	}
	if seenWorker != int(s.Incarnation) {
		h.dump("worker admissions")
		t.Fatalf("every rebuild must drain and recreate the survivor: %d worker admissions across %d incarnations", seenWorker, s.Incarnation)
	}
}

// Dispatcher-level contracts for the migration expiry pass: it runs
// BEFORE the drive pass (an expired record is consumed, never driven),
// it runs regardless of MigrationMode (a mode flip to Never cannot
// strand a non-terminal record), and the post-expiry unpinned surge is
// an ordinary scale-down extra. The full expiry transition (pair
// unpinning, source restore from observation, surge teardown, retry
// acceptance) is covered by the workload/ops expiry tests.

// expiryDispatchFixture wires a Reconcile input holding one Manual
// record and observation stubs that record what the dispatcher did.
func expiryDispatchFixture(t *testing.T, rec workloadtypes.MigrationRecord) (workloadtypes.ReconcileInput, workloadtypes.Deps, *[]workloadtypes.MigrationRecord, *bool) {
	t.Helper()
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	clk := clocktesting.NewFakeClock(time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC))

	records := []workloadtypes.MigrationRecord{rec}
	migrateStamped := false

	in := minimalInput(t)
	in.Clock = clk
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady, RunningRevision: "rev-1"},
	}
	in.ObservedState.Migrations = append([]workloadtypes.MigrationRecord(nil), records...)
	in.MutateMigration = func(_ context.Context, uuid string, mutate func(*workloadtypes.MigrationRecord) bool) error {
		for i := range records {
			if records[i].RequestUUID == uuid {
				r := records[i]
				if mutate(&r) {
					records[i] = r
				}
				return nil
			}
		}
		return nil
	}
	in.MutateInstance = func(_ context.Context, _ int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		s := workloadtypes.InstanceStatus{Index: 0, Phase: workloadtypes.InstancePhaseReady}
		if mutate(&s) && s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationMigrate {
			migrateStamped = true
		}
		return nil
	}
	return in, workloadtypes.Deps{Client: c, Clock: clk}, &records, &migrateStamped
}

// pastDeadlineRecord builds a non-terminal Manual record whose Deadline
// already elapsed relative to the fixture clock.
func pastDeadlineRecord(uuid string) workloadtypes.MigrationRecord {
	base := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	return workloadtypes.MigrationRecord{
		RequestUUID:    uuid,
		Trigger:        workloadtypes.MigrationTriggerManual,
		Phase:          workloadtypes.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       "node-a",
		StartedAt:      metav1.NewTime(base.Add(-time.Hour)),
		Deadline:       metav1.NewTime(base.Add(-30 * time.Minute)),
	}
}

// TestReconcile_ExpiryBeforeDrive pins the precedence rule: a record
// past its Deadline is consumed by the expiry pass BEFORE the drive
// pass can pick it — the record closes Failed, no Migrate op is ever
// stamped, and the dispatcher requeues immediately so the next pass
// rebuilds plan + ObservedState from the post-expiry status.
func TestReconcile_ExpiryBeforeDrive(t *testing.T) {
	in, deps, records, migrateStamped := expiryDispatchFixture(t, pastDeadlineRecord("u-expired"))
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: "superseded"}}
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = workloadtypes.MigrationModeAuto

	res, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Requeue {
		t.Errorf("dispatcher must requeue immediately after an expiry; got %+v", res)
	}
	got := (*records)[0]
	if got.Phase != workloadtypes.MigrationPhaseFailed || got.CompletedAt == nil {
		t.Fatalf("expired record must close Failed; got %+v", got)
	}
	if *migrateStamped {
		t.Errorf("the drive pass must never stamp a Migrate op for an expired record")
	}
	if prunes != 1 {
		t.Errorf("non-scale-down immediate requeue pruned %d RetryBlocks, want 1", prunes)
	}
}

// TestReconcile_ExpiryRunsUnderModeNever pins that the expiry pass is
// NOT gated on MigrationMode: a mode flip to Never after a record was
// accepted must not strand the non-terminal record forever.
func TestReconcile_ExpiryRunsUnderModeNever(t *testing.T) {
	in, deps, records, migrateStamped := expiryDispatchFixture(t, pastDeadlineRecord("u-never-mode"))
	plan := minimalPlan()
	plan.MigrationMode = workloadtypes.MigrationModeNever

	res, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Requeue {
		t.Errorf("dispatcher must requeue after the expiry; got %+v", res)
	}
	if got := (*records)[0]; got.Phase != workloadtypes.MigrationPhaseFailed {
		t.Fatalf("record must expire under MigrationMode=Never too; got %+v", got)
	}
	if *migrateStamped {
		t.Errorf("no Migrate op may be stamped under MigrationMode=Never")
	}
}

// TestReconcile_NotYetExpiredRecordNotConsumed is the negative
// dispatcher control: a non-terminal record whose Deadline is still in
// the future is left alone by the expiry pass. MigrationMode=Never
// isolates the expiry pass (the drive pass — which owns a live record
// — is skipped, and would in this podless stub environment terminally
// reject it for unrelated reasons).
func TestReconcile_NotYetExpiredRecordNotConsumed(t *testing.T) {
	rec := pastDeadlineRecord("u-live")
	rec.Deadline = metav1.NewTime(time.Date(2026, 7, 24, 13, 0, 0, 0, time.UTC)) // 1h ahead of the fixture clock
	in, deps, records, _ := expiryDispatchFixture(t, rec)
	plan := minimalPlan()
	plan.MigrationMode = workloadtypes.MigrationModeNever

	if _, err := workload.Reconcile(context.Background(), deps, in, plan, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := (*records)[0]; got.Phase.Terminal() {
		t.Fatalf("record before its Deadline must not be expired; got %+v", got)
	}
}

// TestScaleDownExtras_UnpinnedSurgeIsExtra pins the teardown
// hand-off: once expiry clears the surge's Migrate op, the surviving
// Creating-phase status is an ordinary scale-down extra (contrast
// TestReconcile_MigrationOperationOwned_NotScaleDown, where the pinned
// op excludes it) — the standard Delete pipeline owns the teardown. The
// restored source stays covered by the plan and can never become an
// extra.
func TestScaleDownExtras_UnpinnedSurgeIsExtra(t *testing.T) {
	observed := []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},    // restored source, in plan
		{Index: 1, Phase: workloadtypes.InstancePhaseCreating}, // unpinned surge, op cleared
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ScaleDownExtras(observed, plan)
	if len(extras) != 1 || extras[0] != 1 {
		t.Fatalf("unpinned surge must be the sole scale-down extra; got %v", extras)
	}
}

// The end-of-pass block (escalation, then the RetryBlock supersede-prune)
// is per-Instance and re-derives its own evidence, so it must survive an
// op pass that errored for one Instance. These tests drive
// workload.Execute with an op pass that fails and assert what the pass
// still does — and, just as importantly, what it must leave alone.

// rejectedStatusWrite is the 422 an apiserver returns for a status write
// it will never accept, whatever the pass retries.
func rejectedStatusWrite() error {
	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ome.io", Kind: "InferenceReplica"},
		"llama-70b-engine",
		field.ErrorList{field.Invalid(field.NewPath("status", "instances"), "", "rejected")},
	)
}

// restartOnlyDecision selects exactly one Instance for the restart pass
// and leaves the end-of-pass block enabled.
func restartOnlyDecision(inst workloadtypes.InstancePlan) workload.Decision {
	return workload.Decision{
		Actions: []workload.PlannedAction{{
			Kind:     workload.ActionRestart,
			Restarts: []workload.RestartSelection{{Instance: inst, Reason: "pod count 0 below desired 1"}},
		}},
		Escalate: true,
	}
}

// threeInstancePlan covers indices 0..2 with one pod each.
func threeInstancePlan() workloadtypes.ComponentPlan {
	plan := workloadtypes.ComponentPlan{Component: workloadtypes.ComponentEngine, Replicas: 3}
	for idx := int32(0); idx < 3; idx++ {
		plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
			Index: idx, Incarnation: 1,
			Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
		})
	}
	return plan
}

// TestExecuteErroredOpPassStillEscalatesExpiredDeadline: the restart pass
// fails on instance 1 (its status write is rejected 422 and the
// classifier has no attempt to dispose, so the error propagates), and the
// pass still escalates instance 0's elapsed operation deadline and prunes
// the superseded RetryBlock. Instance 2 is parked on a quota wait: the
// pass must leave its Waiting token and its zeroed deadline exactly as
// they were, because escalation only reads holds and re-derives them from
// live evidence.
func TestExecuteErroredOpPassStillEscalatesExpiredDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	surgeIdx := int32(10)
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseUpdating,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "update-0",
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepSurge,
				SurgeIndex:     &surgeIdx,
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseReady},
		{
			Index: 2, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				ID:                "create-2",
				Type:              workloadtypes.InstanceOperationCreate,
				Step:              "CreatePods",
				StartedAt:         metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt:    metav1.NewTime(now.Add(-2 * time.Minute)),
				Waiting:           workloadtypes.RejectionReasonQuotaExceeded,
				CapacityRefusedAt: ptrTime(now.Add(-2 * time.Minute)),
			},
		},
	}
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: "superseded"}}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	rejected := rejectedStatusWrite()
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		if idx == 1 {
			return rejected
		}
		return store.apply(ctx, []workloadtypes.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	plan := threeInstancePlan()
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}
	snapshot := workload.SnapshotWithPodsForTest(in, nil)

	result, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))

	if err == nil {
		t.Fatal("the pass must still report the restart failure")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("returned error must carry the apiserver rejection; got %v", err)
	}
	if !strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("returned error must name the failed Instance; got %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %+v, want the errored pass's empty result", result)
	}
	if got := store.status(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed ||
		got.LastFailure == nil || got.LastFailure.Reason != escalation.DeadlineExceededReason || got.Operation == nil {
		t.Fatalf("expired Instance was not escalated: %+v", got)
	}
	if got := store.status(1); got == nil || got.Phase != workloadtypes.InstancePhaseReady || got.Operation != nil {
		t.Fatalf("instance 1 must be untouched while its writes are rejected: %+v", got)
	}
	held := store.status(2)
	if held == nil || held.Phase != workloadtypes.InstancePhaseCreating || held.Operation == nil {
		t.Fatalf("held Instance changed phase: %+v", held)
	}
	if held.Operation.Waiting != workloadtypes.RejectionReasonQuotaExceeded {
		t.Fatalf("held Instance lost its Waiting token: %q", held.Operation.Waiting)
	}
	if !held.Operation.Deadline.IsZero() {
		t.Fatalf("held Instance's parked deadline was re-armed: %v", held.Operation.Deadline)
	}
	if warnings != 1 || prunes != 1 {
		t.Fatalf("end-of-pass effects: warnings=%d prunes=%d, want 1/1", warnings, prunes)
	}
}

// TestExecuteErroredPodReadDoesNotFabricateStuckEvidence: the restart
// pass fails because the Pod list failed, so the escalation pass reading
// the same memoized source fails too, before it derives any evidence — a
// read failure never invents a stuck pod — so nothing is stamped Failed,
// and the joined error keeps the op error first.
func TestExecuteErroredPodReadDoesNotFabricateStuckEvidence(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	surgeIdx := int32(10)
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.StuckPodGrace = time.Minute
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseUpdating,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "update-0",
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepSurge,
				SurgeIndex:     &surgeIdx,
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		return store.apply(ctx, []workloadtypes.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }

	shed := apierrors.NewServiceUnavailable("the apiserver shed the list")
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return shed
			},
		}).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}
	snapshot := workload.NewObservedSnapshot(deps, in, workloadtypes.ComponentEngine, in.ObservedState.InstanceStatuses)

	plan := minimalPlan()
	plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
		Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
	})
	_, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))

	if err == nil {
		t.Fatal("the pass must report both the restart failure and the escalation read failure")
	}
	opFirst := strings.Index(err.Error(), "restart instance 1")
	escalationSecond := strings.Index(err.Error(), "list pods for escalation pass")
	if opFirst < 0 || escalationSecond < 0 {
		t.Fatalf("joined error must name both failures; got %v", err)
	}
	if opFirst > escalationSecond {
		t.Fatalf("the op error must be reported first; got %v", err)
	}
	if got := store.status(0); got != nil && got.Phase == workloadtypes.InstancePhaseFailed {
		t.Fatalf("a failed pod read must not stamp Failed: %+v", got)
	}
	if warnings != 0 {
		t.Fatalf("WarnInstanceFailed fired %d times on a failed read", warnings)
	}
}

// TestExecuteErroredSiblingKeepsRelocationFirstTry: which
// DisposeExpiredAttempt branch runs is decided by the blamed pod set, not
// by whether a sibling's operation errored this pass. A single-pod Create
// parked Running-but-unready on one node, under Auto migration policy
// with budget, still takes the relocation branch on its first expiry
// while the restart pass fails for another Instance — a terminal
// AutoRecover directive naming the suspect node, no RetryBlock, and the
// born-terminal status mirror.
func TestExecuteErroredSiblingKeepsRelocationFirstTry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	const suspectNode = "node-a"
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.UpdateRevision = "llama-70b-engine-abcde"
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "create-0",
				Type:           workloadtypes.InstanceOperationCreate,
				Step:           "CreatePods",
				TargetRevision: "llama-70b-engine-abcde",
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	in.Disposition = workloadtypes.DispositionDeps{AutoMigrateMaxAttempts: 3}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	rejected := rejectedStatusWrite()
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		if idx == 1 {
			return rejected
		}
		return store.apply(ctx, []workloadtypes.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	blocks := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		blocks++
		return nil
	}
	var mirrored []workloadtypes.MigrationRecord
	in.AppendMigration = func(_ context.Context, rec workloadtypes.MigrationRecord) error {
		mirrored = append(mirrored, rec)
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = workloadtypes.MigrationModeAuto
	plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
		Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
	})
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}
	limbo := nodeScopedLimboPod("llama-70b-engine-0-default-0", in.Key.Namespace, suspectNode, "abcde", now.Add(-2*time.Minute))
	snapshot := workload.SnapshotWithPodsForTest(in, map[int32][]*corev1.Pod{0: {limbo}})

	_, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))
	if err == nil || !strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("the sibling restart must still fail the pass; got %v", err)
	}

	ledger, lerr := audit.LoadLedgerForOwner(ctx, c, in.OwnerObject)
	if lerr != nil {
		t.Fatalf("load ledger: %v", lerr)
	}
	if len(ledger.Entries) != 1 {
		t.Fatalf("ledger entries: got %d want the first relocation directive", len(ledger.Entries))
	}
	e := ledger.Entries[0]
	if e.Phase != audit.PhaseCompleted || e.Reason != audit.ReasonAutoRecover || e.Outcome != audit.OutcomeRelocateRecreate {
		t.Errorf("entry: got phase=%q reason=%q outcome=%q want a terminal AutoRecover relocate-recreate directive",
			e.Phase, e.Reason, e.Outcome)
	}
	if e.FromNode != suspectNode || e.SourceInstance != 0 || e.SurgeInstance != 0 {
		t.Errorf("entry identity: got fromNode=%q source=%d surge=%d want %s/0/0 (a directive allocates no surge)",
			e.FromNode, e.SourceInstance, e.SurgeInstance, suspectNode)
	}
	if len(mirrored) != 1 || mirrored[0].Trigger != workloadtypes.MigrationTriggerAuto ||
		mirrored[0].Phase != workloadtypes.MigrationPhaseRelocated || mirrored[0].Attempt != 1 {
		t.Errorf("status mirror: got %+v want one born-terminal Auto record on attempt 1", mirrored)
	}
	if blocks != 0 {
		t.Errorf("MutateRetryBlock calls: got %d want none (relocation never holds a revision)", blocks)
	}
	if got := store.status(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Operation != nil {
		t.Fatalf("relocated Instance: got %+v want Failed with the operation cleared", got)
	}
}

// nodeScopedLimboPod is the readiness-limbo shape pinned to one node and
// one revision — the node-scoped fault the relocation branch is for.
func nodeScopedLimboPod(name, namespace, node, revisionHash string, since time.Time) *corev1.Pod {
	pod := runningNotReadyPod(name, since)
	pod.Namespace = namespace
	pod.Spec.NodeName = node
	pod.Labels = map[string]string{query.LabelRevisionHash: revisionHash}
	return pod
}

// ptrTime is the recorded-refusal stamp a quota-held fixture carries.
func ptrTime(t time.Time) *metav1.Time {
	at := metav1.NewTime(t)
	return &at
}
func TestReconcileDeleteOwnedReboundFinishesBeforeRecreate(t *testing.T) {
	ctx := context.Background()
	scheme := makeScheme(t)
	in := minimalInput(t)
	plan := minimalPlan()
	plan.InstanceReadyTimeout = time.Minute
	started := metav1.NewTime(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	in.Clock = clocktesting.NewFakeClock(started.Add(2 * time.Minute))
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{{
		Index:       0,
		Incarnation: 1,
		Phase:       workloadtypes.InstancePhaseDeleting,
		Operation: &workloadtypes.InstanceOperation{
			ID:             "delete-0-rebound",
			Type:           workloadtypes.InstanceOperationDelete,
			Step:           "Drain",
			StartedAt:      started,
			LastProgressAt: started,
			Deadline:       metav1.NewTime(started.Add(time.Minute)),
		},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: in.Key.Namespace,
		Name:      query.PodName(in.Key.OwnerName, workloadtypes.ComponentEngine, 0, "default", 0),
		UID:       "rebound-pod-uid",
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: in.Key.OwnerName,
			constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelInstanceIdx:                "0",
		},
	}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	expectations := workloadtypes.NewExpectations()
	deps := workloadtypes.Deps{Client: base, APIReader: base, Expectations: expectations}
	store := installTestAtomicMutationStore(&in)
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	finalized := 0
	in.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalized++
		return true, nil
	}
	in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{
		Pods:       []*corev1.Pod{pod},
		ByInstance: map[int32][]*corev1.Pod{0: {pod}},
	}

	result, err := workload.Reconcile(ctx, deps, in, plan, nil)
	if err != nil {
		t.Fatalf("delete pass: %v", err)
	}
	if result.Requeue || result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("delete pass result = %+v", result)
	}
	status := store.status(0)
	if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting ||
		status.Operation == nil || status.Operation.Type != workloadtypes.InstanceOperationDelete {
		t.Fatalf("delete pass altered durable status: writes=%d status=%+v", store.writes, store.status(0))
	}
	// The row's drain is past its deadline, which is announced on the
	// record and nowhere else: no failure, no phase move, no attempt
	// spent. Everything the rest of this test drives still runs.
	if status.LastFailure == nil || status.LastFailure.Reason != workloadtypes.DrainOverdueReason {
		t.Fatalf("overdue drain not announced on the record: %+v", status.LastFailure)
	}
	if warnings != 0 {
		t.Fatalf("delete pass emitted %d generic failure warnings", warnings)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("delete pass did not remove old Pod: %v", err)
	}
	if finalized != 0 {
		t.Fatalf("resources finalized before Pods disappeared: %d", finalized)
	}

	// One write per overdue episode. Writes are counted cumulatively,
	// so the later passes assert their own against this baseline.
	drained := store.writes
	if drained != 1 {
		t.Fatalf("delete pass writes = %d, want exactly 1 (the announcement)", drained)
	}
	expectations.ObservedDelete(in.Key.Namespace, in.Key.OwnerName, in.Key.Component, 0)
	store.sync(&in)
	in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	result, err = workload.Reconcile(ctx, deps, in, plan, nil)
	if err != nil {
		t.Fatalf("completion pass: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("completion pass must force a fresh plan, got %+v", result)
	}
	if finalized != 1 || store.writes != drained+1 || store.status(0) != nil {
		t.Fatalf("completion state = finalized:%d writes:%d status:%+v", finalized, store.writes, store.status(0))
	}
	pods := &corev1.PodList{}
	if err := base.List(ctx, pods, client.InNamespace(in.Key.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("completion pass recreated %d Pods from its stale plan", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(ctx, deps, in, plan, nil)
	if err != nil {
		t.Fatalf("recreate pass: %v", err)
	}
	if result.RequeueAfter != testRequeueIntervals.Operation {
		t.Fatalf("recreate pass result = %+v", result)
	}
	status = store.status(0)
	if status == nil || status.Phase != workloadtypes.InstancePhaseCreating || store.writes != drained+2 {
		t.Fatalf("recreated status/writes = %+v/%d", status, store.writes)
	}
	pods = &corev1.PodList{}
	if err := base.List(ctx, pods, client.InNamespace(in.Key.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("recreate pass Pods = %d, want 1", len(pods.Items))
	}
}

func TestReconcileFreshScaleDownAdmissionEndsThePass(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	expired := metav1.NewTime(now.Add(-time.Minute))
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{
			Index:       1,
			Incarnation: 1,
			PodCount:    1,
			Phase:       workloadtypes.InstancePhaseRestarting,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "restart-1",
				Type:           workloadtypes.InstanceOperationRestart,
				Step:           "WaitReady",
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       expired,
			},
		},
	}
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: "superseded"}}
	in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	plan := minimalPlan()
	plan.InstanceReadyTimeout = time.Minute
	client := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()

	result, err := workload.Reconcile(ctx, workloadtypes.Deps{Client: client, APIReader: client}, in, plan, nil)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %+v, want immediate requeue", result)
	}
	status := store.status(1)
	if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting || status.Operation == nil ||
		status.Operation.Type != workloadtypes.InstanceOperationDelete || status.LastFailure != nil {
		t.Fatalf("admitted status = %+v, want durable Delete ownership", status)
	}
	if store.writes != 1 {
		t.Fatalf("status writes = %d, want only the Delete admission", store.writes)
	}
	if warnings != 0 || prunes != 0 {
		t.Fatalf("post-boundary effects: warnings=%d RetryBlock prunes=%d, want 0/0", warnings, prunes)
	}
}

func TestExecuteActiveScaleDownExcludesDeferredExtrasFromEscalation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	expiredOperation := func(id string) *workloadtypes.InstanceOperation {
		return &workloadtypes.InstanceOperation{
			ID:             id,
			Type:           workloadtypes.InstanceOperationRestart,
			Step:           "WaitReady",
			StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
			LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
			Deadline:       metav1.NewTime(now.Add(-time.Minute)),
		}
	}
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseRestarting, Operation: expiredOperation("retained")},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseRestarting, Operation: expiredOperation("deferred-extra")},
		{
			Index:       2,
			Incarnation: 1,
			PodCount:    1,
			Phase:       workloadtypes.InstancePhaseDeleting,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "active-delete",
				Type:           workloadtypes.InstanceOperationDelete,
				Step:           "Drain",
				StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-time.Minute)),
			},
		},
	}
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: "superseded"}}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	pod := enginePod(in.Key.OwnerName, in.Key.Namespace, 2)
	client := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pod).Build()
	deps := workloadtypes.Deps{Client: client, APIReader: client, Expectations: workloadtypes.NewExpectations()}
	snapshot := workload.SnapshotWithPodsForTest(in, map[int32][]*corev1.Pod{2: {pod}})
	decision := workload.Decision{
		Actions:  []workload.PlannedAction{{Kind: workload.ActionScaleDown, Extras: []int32{1, 2}}},
		Escalate: true,
	}

	result, err := workload.Execute(ctx, deps, in, minimalPlan(), nil, snapshot, decision)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Requeue || result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("result = %+v, want configured scale-down poll", result)
	}
	if got := store.status(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed {
		t.Fatalf("retained expired Instance was not escalated: %+v", got)
	}
	if got := store.status(1); got == nil || got.Phase != workloadtypes.InstancePhaseRestarting || got.LastFailure != nil {
		t.Fatalf("deferred scale-down extra was mutated: %+v", got)
	}
	if got := store.status(2); got == nil || got.Phase != workloadtypes.InstancePhaseDeleting {
		t.Fatalf("active Delete ownership changed: %+v", got)
	}
	if store.writes != 1 || warnings != 1 || prunes != 1 {
		t.Fatalf("end-of-pass effects: writes=%d warnings=%d prunes=%d, want 1/1/1", store.writes, warnings, prunes)
	}
}

func TestReconcileActiveScaleDownWithoutPollSchedulesForceDeleteBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ScaleDownRequeueInterval = 0
	in.ForceDelete = &workloadtypes.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{
			Index:       1,
			Incarnation: 1,
			Phase:       workloadtypes.InstancePhaseDeleting,
			Operation: &workloadtypes.InstanceOperation{
				ID:        "active-delete",
				Type:      workloadtypes.InstanceOperationDelete,
				Step:      "Drain",
				StartedAt: metav1.NewTime(now.Add(-time.Minute)),
			},
		},
	}
	deletionTimestamp := metav1.NewTime(now.Add(-time.Minute))
	pod := enginePod(in.Key.OwnerName, in.Key.Namespace, 1)
	pod.DeletionTimestamp = &deletionTimestamp
	in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{
		Pods:       []*corev1.Pod{pod},
		ByInstance: map[int32][]*corev1.Pod{1: {pod}},
	}
	store := installTestAtomicMutationStore(&in)
	client := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()

	result, err := workload.Reconcile(ctx, workloadtypes.Deps{
		Client: client, APIReader: client, Expectations: workloadtypes.NewExpectations(),
	}, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != time.Minute+time.Nanosecond {
		t.Fatalf("result = %+v, want force-delete boundary in 1m+1ns", result)
	}
	if got := store.status(1); got == nil || got.Phase != workloadtypes.InstancePhaseDeleting {
		t.Fatalf("active Delete status changed: %+v", got)
	}
}

// The restart pass gives every selected Instance its turn each pass. These
// tests drive two single-pod Instances under RecreateInstance through the
// closed-loop recoveryHarness: both lose their pod so both are selected for
// restart in the same pass, and one Instance's status writes are rejected
// on every pass.

// losePods deletes every live pod of the given Instances so the next pass
// selects each of them for restart (pod count below desired).
func (h *recoveryHarness) losePods(indices ...int32) {
	h.t.Helper()
	lose := map[int32]bool{}
	for _, idx := range indices {
		lose[idx] = true
	}
	for _, pod := range h.livePods() {
		if idx, ok := query.InstanceIdxFromLabels(pod); !ok || !lose[idx] {
			continue
		}
		if err := h.c.Delete(h.ctx, pod); err != nil {
			h.t.Fatalf("delete pod %s: %v", pod.Name, err)
		}
	}
}

func (h *recoveryHarness) instance(idx int32) *workloadtypes.InstanceStatus {
	sts := h.irStatuses()
	for i := range sts {
		if sts[i].Index == idx {
			return &sts[i]
		}
	}
	return nil
}

func (h *recoveryHarness) podsOf(idx int32) []*corev1.Pod {
	var out []*corev1.Pod
	for _, pod := range h.livePods() {
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out
}

// allReady reports whether exactly n Instances exist, each Ready with no
// Operation and exactly one live pod.
func (h *recoveryHarness) allReady(n int32) bool {
	sts := h.irStatuses()
	if len(sts) != int(n) {
		return false
	}
	for _, s := range sts {
		if s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || len(h.podsOf(s.Index)) != 1 {
			return false
		}
	}
	return true
}

// atIncarnation reports whether the Instance's status and its live pods all
// carry the given incarnation.
func (h *recoveryHarness) atIncarnation(idx int32, incarnation int64) bool {
	s := h.instance(idx)
	if s == nil || s.Incarnation != incarnation {
		return false
	}
	pods := h.podsOf(idx)
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if pod.Labels[query.LabelInstanceIncarnation] != strconv.FormatInt(incarnation, 10) {
			return false
		}
	}
	return true
}

// unavailableModeStep reports whether an Operation step belongs to a mode
// that takes its Instance's pod out of rotation (in-place patch, or the
// recreate drain). Surge steps do not: the replacement rotates in before
// the source rotates out.
func unavailableModeStep(step string) bool {
	switch step {
	case workloadtypes.UpdateStepSurge, workloadtypes.UpdateStepSurgeDrain, "SurgeDrainSettle",
		workloadtypes.UpdateStepGangSurgeTarget, workloadtypes.UpdateStepGangSurgeTargetCleanup:
		return false
	case "":
		return false
	default:
		return true
	}
}

// Op-less rows: an index with no status row (Empty), a paused row that
// lost its pods (Pending) and a converged row (Ready) all carry no
// attempt. Most of what reaches them — a pod condition flipping, a
// PodGroup verdict, an operator edit the revision payload filters out —
// therefore has no reader, and the pass must come out of it having
// written nothing. These tests are the proof of that inertness, one
// event shape at a time.

// opLessRowFixture is a paused Component holding the given rows, so the
// lifecycle passes are parked and whatever the pass does write is the
// event's own doing.
func opLessRowFixture(t *testing.T, rows []workloadtypes.InstanceStatus) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan, *appsv1.ControllerRevision, *testAtomicMutationStore) {
	t.Helper()
	in := minimalInput(t)
	in.DesiredSpec.Paused = true
	in.ObservedState.UpdateRevision = opLessRowRevision
	in.ObservedState.CurrentRevision = opLessRowRevision
	in.ObservedState.InstanceStatuses = rows
	store := installTestAtomicMutationStore(&in)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: opLessRowRevision}}
	plan := minimalPlan()
	plan.Paused = true
	return in, plan, target, store
}

// opLessRowRevision is the revision every op-less fixture row is both
// running and targeting, so nothing about the revision is in question.
const opLessRowRevision = "llama-70b-engine-noop0001"

// TestPendingRow_PodObservationsAreInert: a Pending row is a
// status-only demotion — it holds no operation and claims no pods, and
// it exists only under pause, which is the reconcile shape the truth
// pass that writes it runs on. A pod of that index turning ready,
// entering or leaving the serving rotation, or disappearing altogether
// therefore reaches nothing: the promote is the Create pass re-deriving
// the whole set once the pause clears, not any one of these
// observations.
func TestPendingRow_PodObservationsAreInert(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arrange puts the index's pod into the observed shape.
		arrange func(*testing.T, *recoveryHarness)
		want    int
	}{
		{name: "pod ready", arrange: func(*testing.T, *recoveryHarness) {}, want: 1},
		{name: "pod serving on", arrange: func(t *testing.T, h *recoveryHarness) {
			h.flipServing(t, 0, corev1.ConditionTrue)
		}, want: 1},
		{name: "pod serving off", arrange: func(t *testing.T, h *recoveryHarness) {
			h.flipServing(t, 0, corev1.ConditionFalse)
		}, want: 1},
		{name: "pod deleted", arrange: func(_ *testing.T, h *recoveryHarness) { h.losePods(0) }, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarness(t, false)
			if !h.run(30, func() bool { return h.allReady(1) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never converged")
			}
			h.desired.Paused = true
			h.forcePhase(t, 0, workloadtypes.InstancePhasePending)
			tc.arrange(t, h)

			h.step()

			got := h.instance(0)
			if got == nil || got.Phase != workloadtypes.InstancePhasePending || got.Operation != nil {
				t.Fatalf("row: got %+v want Pending with no operation", got)
			}
			if n := len(h.podsOf(0)); n != tc.want {
				t.Errorf("pods: got %d want %d (the observation creates and deletes nothing)", n, tc.want)
			}
		})
	}
}

// forcePhase stamps a phase onto an existing row through the harness's
// own status round-trip, standing in for the truth pass that writes it.
func (h *recoveryHarness) forcePhase(t *testing.T, idx int32, phase workloadtypes.InstancePhase) {
	t.Helper()
	if err := h.mutateInstance()(h.ctx, idx, func(s *workloadtypes.InstanceStatus) bool {
		s.Phase = phase
		return true
	}); err != nil {
		t.Fatalf("force phase %q on instance %d: %v", phase, idx, err)
	}
}

// flipServing sets the controller-owned serving gate on the index's
// single pod, the condition the load-balancer rotation is keyed on.
func (h *recoveryHarness) flipServing(t *testing.T, idx int32, status corev1.ConditionStatus) {
	t.Helper()
	pods := h.podsOf(idx)
	if len(pods) != 1 {
		t.Fatalf("pods of instance %d: got %d want 1", idx, len(pods))
	}
	setPodCondition(pods[0], query.ServingConditionType, status, h.clk.Now())
	if err := h.c.Status().Update(h.ctx, pods[0]); err != nil {
		t.Fatalf("flip the serving gate: %v", err)
	}
}

// TestOpLessRows_ExcludedAnnotationEditStartsNoWork: an edit to an
// annotation the revision payload filters out leaves the hash where it
// was, so the target does not move and no pass has anything new to act
// on. The hash check is half the claim — without it a pass that wrote
// nothing would only mean the edit never reached the renderer; the
// paused fixture is the other half, isolating the edit from the
// lifecycle work an unpaused index would do anyway.
func TestOpLessRows_ExcludedAnnotationEditStartsNoWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []workloadtypes.InstanceStatus
		want workloadtypes.InstancePhase
	}{
		{"empty index", nil, ""},
		{"pending row", []workloadtypes.InstanceStatus{{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhasePending}}, workloadtypes.InstancePhasePending},
		{"ready row", []workloadtypes.InstanceStatus{{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady, PodCount: 1, RunningRevision: opLessRowRevision}}, workloadtypes.InstancePhaseReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, plan, target, store := opLessRowFixture(t, tc.rows)

			before := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"}}
			after := &metav1.ObjectMeta{Annotations: map[string]string{
				"ome.io/base-model-name":                   "llama-7b",
				constants.ReleaseHeldRevisionAnnotationKey: "llama-70b-engine-old00001",
			}}
			hBefore, _, err := revision.Hash(in.DesiredSpec.PodSpec, before, nil, "")
			if err != nil {
				t.Fatalf("hash before the edit: %v", err)
			}
			hAfter, _, err := revision.Hash(in.DesiredSpec.PodSpec, after, nil, "")
			if err != nil {
				t.Fatalf("hash after the edit: %v", err)
			}
			if hBefore != hAfter {
				t.Fatalf("an excluded annotation moved the hash: before=%s after=%s", hBefore, hAfter)
			}
			in.DesiredSpec.PodTemplateObjectMeta = after

			c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
			if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: c, APIReader: c}, in, plan, target); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			if store.writes != 0 {
				t.Errorf("status writes: got %d want 0", store.writes)
			}
			got := store.status(0)
			if tc.want == "" {
				if got != nil {
					t.Errorf("row: got %+v want none (the edit plans nothing for an empty index)", got)
				}
			} else if got == nil || got.Phase != tc.want || got.Operation != nil {
				t.Errorf("row: got %+v want %q with no operation", got, tc.want)
			}
			pods := &corev1.PodList{}
			if err := c.List(context.Background(), pods, client.InNamespace(in.Key.Namespace)); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			if len(pods.Items) != 0 {
				t.Errorf("pods: got %d want none", len(pods.Items))
			}
		})
	}
}

// TestUnpause_EmptyIndexIsMaterializedLikeAPendingRow: clearing the
// pause lets the Create pass materialize a planned index whose pods are
// missing. An index with no status row at all takes the same path a
// demoted Pending row takes — nothing about the unpause is special to
// having a row.
func TestUnpause_EmptyIndexIsMaterializedLikeAPendingRow(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.desired.Paused = true

	h.step()
	if s := h.instance(0); s != nil {
		t.Fatalf("paused pass materialized the index: %+v", s)
	}
	if pods := h.livePods(); len(pods) != 0 {
		t.Fatalf("paused pass created %d pod(s)", len(pods))
	}

	h.desired.Paused = false
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("after unpause")
		t.Fatalf("the index was never materialized after the unpause")
	}
}

// TestReadyRow_ServingGateFlipsStampNoTransition: the serving gate is
// the load-balancer rotation, not the Instance lifecycle. A pod entering
// it is what the Create promote already wrote, and a pod leaving it
// drops the serving count the coordination gate reads — but neither is
// a per-Instance transition, so the converged row keeps its phase and
// stays operation-free through both.
func TestReadyRow_ServingGateFlipsStampNoTransition(t *testing.T) {
	for _, serving := range []bool{true, false} {
		name := "serving off"
		if serving {
			name = "serving on"
		}
		t.Run(name, func(t *testing.T) {
			h := newRecoveryHarness(t, false)
			if !h.run(30, func() bool { return h.allReady(1) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never converged")
			}
			before := h.instance(0)

			status := corev1.ConditionFalse
			if serving {
				status = corev1.ConditionTrue
			}
			h.flipServing(t, 0, status)

			h.step()

			got := h.instance(0)
			if got == nil || got.Phase != workloadtypes.InstancePhaseReady || got.Operation != nil {
				t.Fatalf("row: got %+v want Ready with no operation", got)
			}
			if got.Incarnation != before.Incarnation {
				t.Errorf("Incarnation: got %d want %d (no repair is armed)", got.Incarnation, before.Incarnation)
			}
			if got.RunningRevision != before.RunningRevision {
				t.Errorf("RunningRevision: got %q want %q", got.RunningRevision, before.RunningRevision)
			}
		})
	}
}
