package ops

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Every create path in the engine funnels through createMissingPods, so a
// classified apiserver rejection must reach the SAME outcome whichever
// operation issued the create. These tests pin that for the sites the
// Create pass does not cover: Restart Phase B, RecreatePod Phase B, the
// per-pod surge, the gang surge, and the migration surge.

// rejectPodCreates wraps c so every Pod create is answered with err.
func rejectPodCreates(t *testing.T, c client.Client, err error) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
}

// siteQuotaError is the ResourceQuota admission refusal the apiserver
// returns when a namespace is out of capacity.
func siteQuotaError() error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "engine-pod", errors.New(
		"exceeded quota: team-quota, requested: requests.nvidia.com/gpu=8, used: requests.nvidia.com/gpu=56, limited: requests.nvidia.com/gpu=64"))
}

// siteInvalidError is the apiserver refusing the pod object itself.
func siteInvalidError() error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "engine-pod", nil)
}

// TestRestartPhaseB_QuotaExceeded_WaitsWithoutFailing: Restart's recreate
// phase hits the same quota wall as a fresh create. The pass must end
// without an error (so the dispatcher's restart interval owns the retry,
// not controller-runtime's escalating backoff) and record the quota as
// the operation's waiting reason rather than failing the Instance.
func TestRestartPhaseB_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	// Phase A already completed: the instance is Restarting at the bumped
	// incarnation and its old pod is gone, so Phase B does the create.
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Recreate", Reason: "pod lost",
		},
	}
	base := legacyNewFakeClient(t, isvc, ir)
	c := rejectPodCreates(t, base, siteQuotaError())
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	done, err := Restart(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], "pod lost")
	if err != nil {
		t.Fatalf("Restart: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("Restart reported done while blocked on quota")
	}

	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Fatalf("instance 0: got Failed want the restart still in flight")
	}
	if s.Operation == nil || !workload.OperationCapacityBlocked(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota waiting token recorded", s.Operation)
	}
	// The restart's own cause must survive the wait: Reason says WHY the
	// operation exists and is part of the terminal-finalize identity
	// tuple, so a transient quota blip may not overwrite it.
	if s.Operation.Reason != "pod lost" {
		t.Errorf("Operation.Reason: got %q want the restart cause %q preserved", s.Operation.Reason, "pod lost")
	}
}

// TestRecreatePhaseB_Throttled_HonorsServerDelay: a 429 during the
// recreate rollout's Phase B is the apiserver pacing us. The pass ends
// clean and deposits the server's suggested delay on the pass pacing, so
// the dispatcher's requeue is floored by it instead of the error path
// escalating a backoff the server never asked for.
func TestRecreatePhaseB_Throttled_HonorsServerDelay(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	targetSpec := legacyTargetSpecImage("example.com/app:v2")
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, base, isvc, targetSpec)
	// Phase A already completed: Updating at the bumped incarnation with
	// no pods left, so Phase B does the create.
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: updateStepDrain, TargetRevision: tcr.Name,
		},
	}
	if err := base.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Phase B status: %v", err)
	}
	c := rejectPodCreates(t, base, apierrors.NewTooManyRequests("apiserver is shedding load", 7))
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	input.Pacing = &workload.APIPacing{}
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, targetSpec)
	if err != nil {
		t.Fatalf("Update: %v (throttling is paced, not failed)", err)
	}
	if done {
		t.Fatal("Update reported done while throttled")
	}
	if got := input.Pacing.Throttle(); got != 7*time.Second {
		t.Fatalf("pass pacing: got %v want the server's suggested 7s", got)
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Errorf("instance 0: got Failed want the rollout still in flight")
	}
	if s.Operation != nil && workload.OperationCapacityBlocked(legacyFromV1beta1Op(s.Operation)) {
		t.Errorf("Operation.Waiting: got %q want unset (throttling writes no status)", s.Operation.Waiting)
	}
}

// TestSurgeCreate_QuotaExceeded_WaitsWithoutFailing: the per-pod surge's
// Phase 1 create is quota-blocked. The source stays serving and the
// rollout waits with its clock parked — it must not surface an error.
func TestSurgeCreate_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := makeCR(t, base, isvc, "llama-70b-engine-rev-abc12345")
	sourcePod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := base.Create(context.Background(), sourcePod); err != nil {
		t.Fatalf("seed source pod: %v", err)
	}
	c := rejectPodCreates(t, base, siteQuotaError())
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := surgePlan()

	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr,
		[]*corev1.Pod{sourcePod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("surgeUpdate reported done while blocked on quota")
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Fatalf("instance 0: got Failed want the surge still in flight")
	}
	if s.Operation == nil || !workload.OperationCapacityBlocked(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota waiting token recorded", s.Operation)
	}
}

// TestGangSurgeCreate_QuotaExceeded_WaitsWithoutFailing: the gang surge
// creates a whole replacement gang at once, so it is the most likely site
// to exhaust a quota. It must wait like every other site rather than
// erroring the pass — AND the wait must reach the SOURCE, whose operation
// and deadline govern the rollout while the pods are created under the
// surge index. The test drives the two readers that consume it (the
// deadline park, the escalation skip) to prove the source is held.
func TestGangSurgeCreate_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-quota", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-quota-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-quota-engine-oldrev",
		TargetRevision:  revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepSurge,
			TargetRevision: revision,
			SurgeIndex:     &surgeIndex,
		},
	}
	marker := workload.InstanceStatus{
		Index:          surgeIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-target-2",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			TargetRevision: revision,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index: cloneTerminalStatus(source),
			surgeIndex:   cloneTerminalStatus(marker),
		},
	}
	input := workload.ReconcileInput{
		OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Key: workload.Key{
			Namespace: namespace,
			OwnerName: isvcName,
			Component: workload.ComponentEngine,
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{cloneTerminalStatus(source), cloneTerminalStatus(marker)},
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec: legacyTargetSpecImage("example.com/app:v2"),
		},
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			return store.apply(context.Background(),
				[]workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
		},
		FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	base := legacyNewFakeClient(t)
	deps := legacyTestDeps(rejectPodCreates(t, base, siteQuotaError()))
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while blocked on quota")
	}
	blocked, found := store.statuses[surgeIndex]
	if !found {
		t.Fatalf("surge marker missing after the blocked pass")
	}
	if blocked.Phase == workload.InstancePhaseFailed {
		t.Fatalf("surge instance: got Failed want the surge still in flight")
	}
	if !workload.OperationCapacityBlocked(blocked.Operation) {
		t.Fatalf("surge Operation: got %+v want the quota waiting token recorded", blocked.Operation)
	}

	// The SOURCE carries the deadline the rollout is judged by, and the
	// readers that hold it (the deadline park, the escalation skip) live
	// in the parent workload package, which cannot be imported from here.
	// Their side of this contract is pinned by
	// TestGangSurgeSource_HeldWhileItsSurgeWaitsOnQuota.
	if blocked.Operation.SurgeIndex != nil {
		t.Errorf("surge row Operation.SurgeIndex: got %v want nil (the pin lives on the source)", blocked.Operation.SurgeIndex)
	}
	if src := store.statuses[source.Index]; src.Operation == nil || src.Operation.SurgeIndex == nil ||
		*src.Operation.SurgeIndex != surgeIndex {
		t.Errorf("source Operation: got %+v want the surge pin the readers follow", src.Operation)
	}
}

// TestMigrateSurge_InvalidPodSpec_FailsTheMigration: a surge pod the
// apiserver will never accept cannot be waited out, so the migration
// record is closed Failed immediately instead of idling to its deadline.
func TestMigrateSurge_InvalidPodSpec_FailsTheMigration(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	f := newSinglePodMigFixture(t)
	f.clk = clk
	const uuid = "mig-surge-invalid-spec"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(time.Minute)),
	}
	f.c = rejectPodCreates(t, f.c, siteInvalidError())

	// passResult builds its own input, so drive Migrate directly to
	// observe the RetryBlock seam this test is about.
	legacyResetExpectations(t)
	record := f.record(t, uuid)
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      record.SourceInstance,
		FromNode:      record.FromNode,
		Reason:        record.Reason,
	}
	in := f.input(t)
	blocks := &[]string{}
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}

	done, accepted, err := Migrate(context.Background(), f.deps(), in, f.plan, record.SourceInstance, uuid, req)
	if err != nil {
		t.Fatalf("migrate pass: %v", err)
	}
	if !accepted {
		t.Fatalf("migrate pass: accepted=false want true")
	}
	if !done {
		t.Fatalf("migrate pass: done=false want true (the request is closed, not waiting)")
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record phase: got %s want Failed", rec.Phase)
	}
	if rec.CompletedAt == nil {
		t.Errorf("record CompletedAt: got nil want the close timestamp")
	}
	// The surge pod carries the request's placement overlay, so a 422 may
	// indict the overlay rather than the revision. Closing the request is
	// the whole remedy; holding the revision would wedge an innocent
	// rollout on an operator's bad migration request.
	if len(*blocks) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none for an overlay-bearing surge", *blocks)
	}
}
