package ops

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Tests for the per-Instance SurgeThenDrain state machine.

// surgePodAtOrdinal builds a fixture pod for a SurgeThenDrain test. Same
// shape as legacyPodAtIncarnation but stamps LabelPodOrdinal explicitly
// so partitionPodsBySurgeOrdinal can place it in the right bucket.
func surgePodAtOrdinal(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ordinal int32, ready, serving bool) *corev1.Pod {
	pod := legacyPodAtIncarnation(isvc, instanceIdx, incarnation, ready, serving)
	pod.Name = query.PodName(isvc.Name, workload.ComponentEngine, instanceIdx, "default", ordinal)
	pod.Labels[query.LabelPodOrdinal] = fmt.Sprintf("%d", ordinal)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "test:v1"}}
	return pod
}

// surgeISVCMultiInstance builds a 2-replica ISVC with SurgeThenDrain
// configured and an IR with InstanceStatuses for each (Phase=Ready,
// ActiveOrdinal=0).
func surgeISVCMultiInstance(name, ns string, incarnation int64) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	isvc := legacyMinimalISVC(name, ns, 2)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategySurgeThenDrain,
		},
	}
	ir := legacyInstanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
		v1beta1.OMENativeInstanceStatus{Index: 1, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
	)
	return isvc, ir
}

// readRV returns the ResourceVersion of the persisted InferenceReplica —
// the object every instance-status write in these tests lands on. Used
// by idempotency tests to confirm a re-invocation didn't bump RV.
func readRV(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) string {
	t.Helper()
	fresh := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, fresh); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	return fresh.ResourceVersion
}

// makeCR fabricates a ControllerRevision pre-created in the fake client
// at the given name. The Data payload is left empty; the surge state
// machine only reads target.Name + (in inPlaceUpdate, via
// loadControllerRevisionPayload) target.Data — surge tests don't go
// through in-place so the empty payload is fine.
func makeCR(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, name string) *appsv1.ControllerRevision {
	t.Helper()
	cr := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: isvc.Namespace,
		},
	}
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create CR %s: %v", name, err)
	}
	return cr
}

// surgePlan builds the single-Instance ComponentPlan a SurgeThenDrain
// test consumes.
func surgePlan() workload.ComponentPlan {
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
			Type: workload.UpdateStrategySurgeThenDrain,
		},
	}
}

// TestSurgeUpdate_PersistedSettleStepDeletesTheSource pins the
// compatibility read: Step=SurgeDrainSettle means the row's drain is
// confirmed, so the pass that observes it deletes the source. The step
// is read only, never written.
func TestSurgeUpdate_PersistedSettleStepDeletesTheSource(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	// Source drained out of rotation, replacement serving: the shape a
	// row carrying that step is in.
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, false)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b-engine-rev-v2hash", Namespace: isvc.Namespace,
	}}
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	podReadyAt(surgePod, minReadyWindowStart.Add(-time.Minute))
	c := legacyNewFakeClient(t, isvc, ir, oldPod, surgePod, target)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(minReadyWindowStart)
	plan := surgePlan()
	if err := status.StampSurging(context.Background(), input, 0, target.Name,
		workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	persisted := func(s *workload.InstanceStatus) bool {
		op := *s.Operation
		op.Step = workload.UpdateStepSurgeDrainSettle
		s.Operation = &op
		return true
	}
	if err := input.MutateInstance(context.Background(), 0, persisted); err != nil {
		t.Fatalf("pre-stamp persisted settle step: %v", err)
	}
	input.ObservedState.InstanceStatuses[0].Phase = workload.InstancePhaseUpdating
	input.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepSurgeDrainSettle,
		TargetRevision: target.Name,
	}

	if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
		plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod}); err != nil {
		t.Fatalf("surgeUpdate on a persisted settle row: %v", err)
	}

	fresh := &corev1.Pod{}
	err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), fresh)
	if err == nil && fresh.DeletionTimestamp == nil {
		t.Fatalf("a persisted settle row must delete its drained source, got %+v", fresh.ObjectMeta)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("read source: %v", err)
	}
}

// surgeISVCReady is the steady-state Instance the surge tests start
// from — Phase=Ready at the given incarnation, no pods alive yet (the
// test seeds them). Returns ISVC + IR.
func surgeISVCReady(name, ns string, incarnation int64) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	isvc := legacyMinimalISVC(name, ns, 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategySurgeThenDrain,
		},
	}
	ir := legacyInstanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
	)
	return isvc, ir
}

// TestPatchInstanceStatusSurgingForUpdate_IdempotentOnSurgeDrainStep
// pins the surge entry helper's idempotency on the SurgeDrain step.
// The first pass through Phase 2 transitions Step from Surge to
// SurgeDrain; on a subsequent pass the entry-point stamp must NOT
// regress that Step back to Surge (which would re-fire the create branch
// and resurrect the just-deleted old pod), and the Operation.ID must
// stay stable so timing baselines don't reset.
func TestPatchInstanceStatusSurgingForUpdate_IdempotentOnSurgeDrainStep(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)

	if err := status.StampSurging(context.Background(), input, 0, "rev-x", workload.UpdateStrategySurgeThenDrain, 30*time.Second); err != nil {
		t.Fatalf("stamp Surge: %v", err)
	}
	if err := status.StampSurgeDrainStep(context.Background(), input, 0); err != nil {
		t.Fatalf("stamp SurgeDrain: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	beforeOpID := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0].Operation.ID

	// Re-invoke with the same target — must be a no-op.
	if err := status.StampSurging(context.Background(), input, 0, "rev-x", workload.UpdateStrategySurgeThenDrain, 30*time.Second); err != nil {
		t.Fatalf("re-stamp Surge (idempotency check): %v", err)
	}
	afterRV := readRV(t, c, isvc)
	if beforeRV != afterRV {
		t.Errorf("idempotent re-invoke bumped RV: %s -> %s", beforeRV, afterRV)
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil {
		t.Fatalf("Operation cleared by idempotent re-invoke")
	}
	if s.Operation.Step != workload.UpdateStepSurgeDrain {
		t.Errorf("Step regressed to %q on idempotent re-invoke; want SurgeDrain", s.Operation.Step)
	}
	if s.Operation.ID != beforeOpID {
		t.Errorf("Operation.ID changed across idempotent re-invoke: %q -> %q (would lose timing baselines)",
			beforeOpID, s.Operation.ID)
	}
}

// TestPatchInstanceStatusSurgingForUpdate_FailedStickyOnSameTarget pins the
// escalation-vs-dispatch anti-ping-pong: once the stuck-pod escalator flips a
// surging Instance to Failed, re-stamping the surge toward the SAME target
// must be a no-op (Phase stays Failed) — otherwise the resurrect to Updating
// re-arms the escalator into a Failed<->Updating write storm for a workload
// stuck on a bad revision with no corrective edit. A DIFFERENT target (a real
// corrective edit) must still re-stamp Updating so recovery proceeds.
func TestPatchInstanceStatusSurgingForUpdate_FailedStickyOnSameTarget(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)

	// Enter a surge toward rev-x, then have the escalator flip it to Failed.
	if err := status.StampSurging(context.Background(), input, 0, "rev-x", workload.UpdateStrategySurgeThenDrain, 30*time.Second); err != nil {
		t.Fatalf("stamp Surge: %v", err)
	}
	if err := input.MutateInstance(context.Background(), 0, func(s *workload.InstanceStatus) bool {
		s.Phase = workload.InstancePhaseFailed
		return true
	}); err != nil {
		t.Fatalf("flip to Failed: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	beforeOpID := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0].Operation.ID

	// Same target: must be a no-op — Phase stays Failed, no RV churn, no new op.
	if err := status.StampSurging(context.Background(), input, 0, "rev-x", workload.UpdateStrategySurgeThenDrain, 30*time.Second); err != nil {
		t.Fatalf("re-stamp Surge on Failed (same target): %v", err)
	}
	if got := readRV(t, c, isvc); got != beforeRV {
		t.Errorf("Failed instance re-stamp bumped RV: %s -> %s (should be sticky)", beforeRV, got)
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: got %q want Failed (must not resurrect to Updating)", s.Phase)
	}
	if s.Operation == nil || s.Operation.ID != beforeOpID {
		t.Errorf("Operation churned on Failed re-stamp (would reset deadline): before=%q after=%+v", beforeOpID, s.Operation)
	}

	// Different target (corrective edit): must re-stamp Updating toward it.
	if err := status.StampSurging(context.Background(), input, 0, "rev-good", workload.UpdateStrategySurgeThenDrain, 30*time.Second); err != nil {
		t.Fatalf("re-stamp Surge toward corrective target: %v", err)
	}
	s = legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase after corrective re-stamp: got %q want Updating", s.Phase)
	}
	if s.TargetRevision != "rev-good" {
		t.Errorf("TargetRevision after corrective re-stamp: got %q want rev-good", s.TargetRevision)
	}
}

// TestMutateInstance_MirrorsCommittedStateOntoInMemoryISVC pins the
// MutateInstance mirror requirement. After a MutateInstance
// closure commits a status change to the apiserver, the closure must
// ALSO mirror the new state onto the in-memory ISVC the test (and
// production callers in the same reconcile) hold a pointer to —
// without the mirror, the next pass within the same reconcile reads
// the stale pre-mutation status.
func TestMutateInstance_MirrorsCommittedStateOntoInMemoryISVC(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)

	// Commit a Phase transition.
	if err := status.StampReadyAtOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
		t.Fatalf("MutateInstance: %v", err)
	}

	// In-memory ISVC pointer should reflect the new ActiveOrdinal=1
	// without a re-Get. The omenative status.MutateInstance wraps the
	// apiserver round-trip and ALSO writes the result back onto the
	// in-memory mirror; the workload-side MutateInstance callback
	// preserves that contract by going through the same omenative
	// status writer.
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.ActiveOrdinal != 1 {
		t.Errorf("ActiveOrdinal mirror: in-memory got %d want 1 (committed)", s.ActiveOrdinal)
	}
	if s.RunningRevision != "rev-abc" {
		t.Errorf("RunningRevision mirror: in-memory got %q want %q", s.RunningRevision, "rev-abc")
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase mirror: in-memory got %q want Ready", s.Phase)
	}
}

// TestExpectedPodNamesForInstance_RespectsActiveOrdinal pins the
// SurgeThenDrain drain-completion contract: when a single-pod
// Instance has a status entry whose ActiveOrdinal has been advanced by
// a prior surge promote (e.g., from 0 → 1), expectedPodNamesForInstance
// must emit a single target at THAT ordinal — NOT hard-code 0.
func TestExpectedPodNamesForInstance_RespectsActiveOrdinal(t *testing.T) {
	cases := []struct {
		name          string
		activeOrdinal int32
		withStatus    bool
		wantOrdinal   int32
		wantName      string
	}{
		{
			name:        "no status entry defaults to ordinal 0",
			withStatus:  false,
			wantOrdinal: 0,
			wantName:    "llama-70b-engine-0-default-0",
		},
		{
			name:          "ActiveOrdinal=0 emits target at ordinal 0",
			activeOrdinal: 0,
			withStatus:    true,
			wantOrdinal:   0,
			wantName:      "llama-70b-engine-0-default-0",
		},
		{
			name:          "ActiveOrdinal=1 (post-surge promote) emits target at ordinal 1",
			activeOrdinal: 1,
			withStatus:    true,
			wantOrdinal:   1,
			wantName:      "llama-70b-engine-0-default-1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := legacyMinimalISVC("llama-70b", "prod", 1)
			objs := []client.Object{isvc}
			if tc.withStatus {
				objs = append(objs, legacyInstanceIR(isvc, workload.ComponentEngine,
					v1beta1.OMENativeInstanceStatus{Index: 0, ActiveOrdinal: tc.activeOrdinal, Phase: v1beta1.OMENativeInstanceReady},
				))
			}
			c := legacyNewFakeClient(t, objs...)
			input := legacyTestInput(isvc, c, workload.ComponentEngine)
			plan := surgePlan()
			inst := workload.InstancePlan{
				Index:       0,
				Incarnation: 1,
				Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
			}
			got := expectedPodNamesForInstance(input, plan, inst)
			if len(got) != 1 {
				t.Fatalf("len(targets): got %d want 1", len(got))
			}
			if got[0].Ordinal != tc.wantOrdinal {
				t.Errorf("Ordinal: got %d want %d", got[0].Ordinal, tc.wantOrdinal)
			}
			if got[0].Name != tc.wantName {
				t.Errorf("Name: got %q want %q", got[0].Name, tc.wantName)
			}
		})
	}
}

// TestExpectedPodNamesForInstance_StaleActiveOrdinalDoesNotResurrect
// pins the post-promote ord-resurrection invariant: after a surge
// promote stamps ActiveOrdinal=1, the next steady-state Create pass
// must NOT see "ordinal 0 is missing" and resurrect a pod there.
// Walking the function with a status carrying ActiveOrdinal=1 must
// emit exactly one target at ordinal 1.
func TestExpectedPodNamesForInstance_StaleActiveOrdinalDoesNotResurrect(t *testing.T) {
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	// Simulate post-promote state: ActiveOrdinal=1.
	ir.Status.InstanceStatuses[0].ActiveOrdinal = 1
	c := legacyNewFakeClient(t, isvc, ir)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	inst := workload.InstancePlan{
		Index:       0,
		Incarnation: 1,
		Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
	}
	got := expectedPodNamesForInstance(input, plan, inst)
	if len(got) != 1 {
		t.Fatalf("len(targets): got %d want 1 (single-pod must not resurrect ord 0)", len(got))
	}
	if got[0].Ordinal != 1 {
		t.Errorf("Ordinal: got %d want 1 (the post-promote canonical slot)", got[0].Ordinal)
	}
}

// TestExpectedPodNamesForInstance_MultiPodPreservesAllOrdinals pins
// that the ActiveOrdinal handling is a single-pod-only branch; gang
// Runners (Size > 1) keep emitting every 0..Size-1 ordinal.
func TestExpectedPodNamesForInstance_MultiPodPreservesAllOrdinals(t *testing.T) {
	isvc := legacyMinimalISVC("llama-70b", "prod", 1)
	ir := legacyInstanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, ActiveOrdinal: 1, Phase: v1beta1.OMENativeInstanceReady},
	)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	inst := workload.InstancePlan{
		Index:       0,
		Incarnation: 1,
		Runners:     []workload.RunnerPlan{{Name: "default", Size: 4}},
	}
	got := expectedPodNamesForInstance(input, plan, inst)
	if len(got) != 4 {
		t.Fatalf("len(targets): got %d want 4 (Size=4 must enumerate all ordinals)", len(got))
	}
	wantOrdinals := map[int32]bool{0: true, 1: true, 2: true, 3: true}
	for _, target := range got {
		if !wantOrdinals[target.Ordinal] {
			t.Errorf("unexpected ordinal: %d (want one of 0..3)", target.Ordinal)
		}
		delete(wantOrdinals, target.Ordinal)
	}
	if len(wantOrdinals) > 0 {
		t.Errorf("missing ordinals: %v", wantOrdinals)
	}
}

// TestReclassifyByRevisionHash_KeepsValidSurge pins the keep-in-surge
// contract: a pod labeled with the pinned target, or carrying no
// revision-hash label at all (legacy — ordinal classification wins),
// stays in the surge bucket.
func TestReclassifyByRevisionHash_KeepsValidSurge(t *testing.T) {
	mk := func(rev string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{query.LabelRevisionHash: rev},
			},
		}
	}
	validSurge := mk("rev3")
	missingLabel := &corev1.Pod{}

	surge, drain := reclassifyByRevisionHash(
		[]*corev1.Pod{validSurge, missingLabel},
		nil, query.RevisionFromHash("rev3"),
	)
	if len(drain) != 0 {
		t.Errorf("drain bucket should be empty (target-labeled + unlabeled); got %d", len(drain))
	}
	if len(surge) != 2 {
		t.Errorf("surge bucket should keep both pods; got %d", len(surge))
	}
}

// TestReclassifyByRevisionHash_DrainsStaleByRunningRev pins the stale-pod
// reclassification: a pod labeled with the just-promoted RunningRevision
// but sitting in the surge bucket MUST be moved to drain so Phase 2
// deletes it instead of Phase 3 promoting the wrong revision.
func TestReclassifyByRevisionHash_DrainsStaleByRunningRev(t *testing.T) {
	mk := func(rev string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{query.LabelRevisionHash: rev},
			},
		}
	}
	staleRev2 := mk("rev2")
	validRev3 := mk("rev3")

	surge, drain := reclassifyByRevisionHash(
		[]*corev1.Pod{staleRev2, validRev3},
		[]*corev1.Pod{}, query.RevisionFromHash("rev3"),
	)
	if len(drain) != 1 || drain[0] != staleRev2 {
		t.Errorf("drain bucket should contain the rev2-labeled stale pod; got %d entries", len(drain))
	}
	if len(surge) != 1 || surge[0] != validRev3 {
		t.Errorf("surge bucket should keep only the rev3-labeled valid surge; got %d entries", len(surge))
	}
}

// TestReclassifyByRevisionHash_DrainsAlienRev pins the corrective-
// recovery contract: a surge pod on a revision that matches NEITHER the
// running rev NOR the pinned target — the dead pod an exhausted attempt
// toward a superseded revision left at the surge ordinal — is drained,
// never kept as the in-flight surge. Only a target-labeled pod is kept.
func TestReclassifyByRevisionHash_DrainsAlienRev(t *testing.T) {
	mk := func(rev string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{query.LabelRevisionHash: rev}}}
	}
	surge, drain := reclassifyByRevisionHash([]*corev1.Pod{mk("revBAD")}, nil, query.RevisionFromHash("revGOOD"))
	if len(drain) != 1 || len(surge) != 0 {
		t.Fatalf("alien-rev pod must drain; surge=%d drain=%d", len(surge), len(drain))
	}
	// A pod already on the target is not churned.
	surge, drain = reclassifyByRevisionHash([]*corev1.Pod{mk("revGOOD")}, nil, query.RevisionFromHash("revGOOD"))
	if len(drain) != 0 || len(surge) != 1 {
		t.Fatalf("target-rev pod must stay in surge; surge=%d drain=%d", len(surge), len(drain))
	}
}

// TestSurgeUpdate_PostPromoteCreateDoesNotResurrectDrainedOrdinal
// pins the integration-shaped surge-cycle invariant. After surge has
// completed (drain + promote), the next reconcile pass that lands in
// Create must NOT recreate a pod at the now-drained ordinal slot.
func TestSurgeUpdate_PostPromoteCreateDoesNotResurrectDrainedOrdinal(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	tcr := makeCR(t, c, isvc, "llama-70b-engine-rev-abc12345")
	// Promote the surge: bumps ActiveOrdinal to 1 and stamps RunningRevision.
	if err := status.StampReadyAtOrdinal(context.Background(), input, 0, tcr.Name, 1); err != nil {
		t.Fatalf("patch promote: %v", err)
	}
	// Surge pod at ordinal 1, runtime-ready and serving.
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	if err := c.Create(context.Background(), surgePod); err != nil {
		t.Fatalf("seed surge pod: %v", err)
	}

	// Re-read ISVC so the input has the promoted status (ActiveOrdinal=1).
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	input2 := legacyTestInput(fresh, c, workload.ComponentEngine)
	plan := surgePlan()

	// Drive Create as the dispatcher would after Update returns done=true.
	if _, err := Create(context.Background(), workload.Deps{Client: c}, input2, plan, tcr); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// CRITICAL: there must STILL be only one pod for instance 0 — at ordinal 1.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Fatalf("Create resurrected a stale ordinal! got %d pods (%v), want 1 (the surge pod at ord=1)", len(pods.Items), names)
	}
	if pods.Items[0].Name != surgePod.Name {
		t.Errorf("unexpected pod survived: got %q want %q", pods.Items[0].Name, surgePod.Name)
	}
}

// TestSurgeUpdate_MultiInstance_NoOrdResurrectionAfterPromote pins
// the multi-instance shape: even when BOTH instances have been
// promoted to ActiveOrdinal=1, the next Create pass must not see
// "ordinal 0 is missing" for either instance and resurrect a drained
// slot.
func TestSurgeUpdate_MultiInstance_NoOrdResurrectionAfterPromote(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCMultiInstance("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	tcr := makeCR(t, c, isvc, "llama-70b-engine-rev-abc12345")

	// Simulate post-promote state for BOTH instances: ActiveOrdinal=1.
	for _, idx := range []int32{0, 1} {
		if err := status.StampReadyAtOrdinal(context.Background(), input, idx, tcr.Name, 1); err != nil {
			t.Fatalf("patch promote instance %d: %v", idx, err)
		}
		surgePod := surgePodAtOrdinal(isvc, idx, 1, 1, true, true)
		if err := c.Create(context.Background(), surgePod); err != nil {
			t.Fatalf("seed surge pod for instance %d: %v", idx, err)
		}
	}

	// Re-read ISVC so input has the post-promote state for both instances.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	input2 := legacyTestInput(fresh, c, workload.ComponentEngine)
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		InstanceReadyTimeout: 30 * time.Minute,
	}

	if _, err := Create(context.Background(), workload.Deps{Client: c}, input2, plan, tcr); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// CRITICAL: there must STILL be only 2 pods — one per Instance at ordinal 1.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Fatalf("Create resurrected stale ordinals across instances! got %d pods (%v), want 2", len(pods.Items), names)
	}
}

// TestSurgeUpdate_MultiInstance_InterleavedPromotes pins the
// post-promote mirror behavior under interleaved promotes. Promoting
// instance 0 and then promoting instance 1 in the same reconcile must
// NOT lose the ActiveOrdinal bump on either instance.
func TestSurgeUpdate_MultiInstance_InterleavedPromotes(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCMultiInstance("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	tcr := makeCR(t, c, isvc, "llama-70b-engine-rev-abc12345")

	// Promote instance 0.
	if err := status.StampReadyAtOrdinal(context.Background(), input, 0, tcr.Name, 1); err != nil {
		t.Fatalf("promote 0: %v", err)
	}
	// Build a fresh input from the in-memory ISVC (mirror should have
	// updated it). Promote instance 1.
	input2 := legacyTestInput(isvc, c, workload.ComponentEngine)
	if err := status.StampReadyAtOrdinal(context.Background(), input2, 1, tcr.Name, 1); err != nil {
		t.Fatalf("promote 1: %v", err)
	}

	// Both ActiveOrdinals must be 1 on the persisted status.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	for _, idx := range []int32{0, 1} {
		found := false
		for _, s := range legacyInstanceStatusesOnIR(c, fresh, workload.ComponentEngine) {
			if s.Index == idx {
				found = true
				if s.ActiveOrdinal != 1 {
					t.Errorf("instance %d ActiveOrdinal: got %d want 1", idx, s.ActiveOrdinal)
				}
				if s.RunningRevision != tcr.Name {
					t.Errorf("instance %d RunningRevision: got %q want %q", idx, s.RunningRevision, tcr.Name)
				}
			}
		}
		if !found {
			t.Errorf("instance %d missing from status", idx)
		}
	}
}

// TestSurgeUpdate_BumpDuringBump_ConvergesToFinalRev pins the
// bump-during-bump target-stability invariant. A mid-surge spec bump
// to a new rev must NOT corrupt the in-flight surge — the surge in
// progress continues to drive to its pinned target, and the next surge
// cycle starts naturally for the newer rev after the in-flight one
// promotes.
func TestSurgeUpdate_BumpDuringBump_ConvergesToFinalRev(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()

	// Old pod at ord=0.
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := c.Create(context.Background(), oldPod); err != nil {
		t.Fatalf("seed old pod: %v", err)
	}

	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")

	// Pass 1 against rev2: stamps Op{Surge, rev2}, creates surge at ord=1.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev2, []*corev1.Pod{oldPod}); err != nil {
		t.Fatalf("pass 1 (rev2 surge create): %v", err)
	}

	// User bumps to rev3 mid-surge.
	rev3 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev3hash")

	// Re-read so input observes the recorded Op{TargetRevision=rev2}.
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	input2 := legacyTestInput(freshISVC, c, workload.ComponentEngine)
	s := legacyInstanceStatusesOnIR(c, freshISVC, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Fatalf("expected recorded Op.TargetRevision=rev2 after pass 1, got %+v", s.Operation)
	}

	// Pass 2 against rev3 (the bump-during-bump). surgeUpdate should
	// pin to the in-flight rev2 — NOT drive to rev3 yet.
	rev2SurgeName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 1)
	rev2Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev2SurgeName}, rev2Surge); err != nil {
		t.Fatalf("rev2 surge pod missing: %v", err)
	}
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input2, plan, plan.Instances[0], rev3, []*corev1.Pod{oldPod, rev2Surge}); err != nil {
		t.Fatalf("pass 2 (mid-surge rev3 bump): %v", err)
	}

	freshISVC2 := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC2); err != nil {
		t.Fatalf("re-read ISVC after pass 2: %v", err)
	}
	s = legacyInstanceStatusesOnIR(c, freshISVC2, workload.ComponentEngine)[0]
	// Op.TargetRevision MUST still be rev2 — surge pinned.
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Errorf("Op.TargetRevision should stay pinned to rev2 across the mid-surge rev3 bump; got %+v", s.Operation)
	}
}

// TestSurgeUpdate_SupersededSurge_AbandonsAndKeepsSource pins the level-triggered
// redirect: an in-flight surge toward a rev that's been superseded by a newer
// desired target, whose surge pod is NOT yet Ready, is abandoned — the stuck surge
// pod is deleted so the next reconcile re-surges toward the current target, while
// the source pod at the old ordinal keeps serving (capacity holds; unlike the
// Failed-recovery path this must NOT drain the source). This guards against the
// deadlock where a never-Ready, never-escalated surge pinned to a dead rev
// holds the maxSurge budget until instanceReadyTimeout.
func TestSurgeUpdate_SupersededSurge_AbandonsAndKeepsSource(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()

	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true) // source at ord=0, serving
	if err := c.Create(context.Background(), oldPod); err != nil {
		t.Fatalf("seed source pod: %v", err)
	}
	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")

	// Pass 1: surge to rev2 — creates the surge pod at ord=1 (not yet Ready).
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev2, []*corev1.Pod{oldPod}); err != nil {
		t.Fatalf("pass 1 (rev2 surge create): %v", err)
	}
	surgeName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 1)
	rev2Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: surgeName}, rev2Surge); err != nil {
		t.Fatalf("rev2 surge pod missing after pass 1: %v", err)
	}

	// Bump to rev3 (supersede) while the rev2 surge pod is still not Ready.
	rev3 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev3hash")
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	input2 := legacyTestInput(freshISVC, c, workload.ComponentEngine)

	// Simulate the informer observing pass 1's create so the abandon's
	// Satisfied() gate (which guards the delete) passes.
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)

	// Pass 2 (superseding rev3 bump): abandon the stuck rev2 surge pod.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input2, plan, plan.Instances[0], rev3, []*corev1.Pod{oldPod, rev2Surge}); err != nil {
		t.Fatalf("pass 2 (superseding rev3 bump): %v", err)
	}

	// The stuck rev2 surge pod (ord=1) must be deleted (abandoned).
	gone := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: surgeName}, gone); err == nil && gone.DeletionTimestamp == nil {
		t.Errorf("superseded rev2 surge pod should be deleted; it is still present")
	}
	// The source pod at ord=0 must be untouched — capacity holds during the redirect.
	oldName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 0)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: oldName}, &corev1.Pod{}); err != nil {
		t.Errorf("source pod (ord=0) must be kept during the redirect; got %v", err)
	}
}

// TestSurgeUpdate_SingleBump_DoesNotShiftTarget pins the
// target-stability invariant: a single bump (no in-flight surge) followed by a
// re-invoke at the SAME target must NOT alter the recorded
// Op.TargetRevision — idempotent on the in-flight surge.
func TestSurgeUpdate_SingleBump_DoesNotShiftTarget(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()

	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := c.Create(context.Background(), oldPod); err != nil {
		t.Fatalf("seed old pod: %v", err)
	}

	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")

	// First surge pass.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev2, []*corev1.Pod{oldPod}); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	beforeRV := freshISVC.ResourceVersion

	// Re-invoke at the same target (rev2). The Op.TargetRevision should
	// remain pinned without status churn.
	input2 := legacyTestInput(freshISVC, c, workload.ComponentEngine)
	rev2SurgeName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 1)
	rev2Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev2SurgeName}, rev2Surge); err != nil {
		t.Fatalf("rev2 surge pod missing: %v", err)
	}
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input2, plan, plan.Instances[0], rev2, []*corev1.Pod{oldPod, rev2Surge}); err != nil {
		t.Fatalf("pass 2 (idempotent): %v", err)
	}
	freshISVC2 := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC2); err != nil {
		t.Fatalf("re-read after pass 2: %v", err)
	}
	s := legacyInstanceStatusesOnIR(c, freshISVC2, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.TargetRevision != rev2.Name {
		t.Errorf("Op.TargetRevision shifted unexpectedly: got %+v want rev2", s.Operation)
	}
	// Note: the surge pod's serving flip + the SurgeDrain stamp DO bump
	// RV (legitimate state-machine progress), so we don't assert
	// strict RV equality across passes here. The TargetRevision pinning
	// is the load-bearing assertion.
	_ = beforeRV
}

// TestSurgeUpdate_PostV2Promote_V2PodsDrainedWhenV3SurgeFires pins that
// after a v1→v2 surge cycle COMPLETES with RunningRevision=v2 and
// ActiveOrdinal=1, a fresh spec bump to v3 drives the Instance to
// RunningRevision=v3 with the v2 pod fully drained.
func TestSurgeUpdate_PostV2Promote_V2PodsDrainedWhenV3SurgeFires(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()

	// Simulate post-v2-promote state: RunningRevision=rev2, ActiveOrdinal=1.
	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")
	if err := status.StampReadyAtOrdinal(context.Background(), input, 0, rev2.Name, 1); err != nil {
		t.Fatalf("seed post-v2-promote: %v", err)
	}
	v2Pod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	v2Pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev2.Name)
	if err := c.Create(context.Background(), v2Pod); err != nil {
		t.Fatalf("seed v2 pod: %v", err)
	}
	// Re-read so input observes the post-promote ActiveOrdinal=1 + RunningRevision=rev2.
	input = legacyTestInput(isvc, c, workload.ComponentEngine)

	rev3 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev3hash")

	// Pass 1: Phase 1 entry for the v3 cycle. Stamps Op{Surge, rev3}
	// and creates the v3 surge pod at ord=0.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{v2Pod}); err != nil {
		t.Fatalf("pass 1 (v3 surge create): %v", err)
	}
	rev3SurgeName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 0)
	rev3Surge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev3SurgeName}, rev3Surge); err != nil {
		t.Fatalf("v3 surge pod missing after pass 1: %v", err)
	}
	if got := rev3Surge.Labels[query.LabelRevisionHash]; got != query.RevisionHashFromControllerRevisionName(rev3.Name) {
		t.Errorf("v3 surge pod hash label: got %q want %q", got, query.RevisionHashFromControllerRevisionName(rev3.Name))
	}
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)

	// Pass 2: ContainersReady enables the serving gate, but the v2 source
	// remains until kubelet reports the replacement PodReady.
	rev3Surge.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionFalse},
	}
	if err := c.Status().Update(context.Background(), rev3Surge); err != nil {
		t.Fatalf("flip v3 surge ContainersReady: %v", err)
	}
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{v2Pod, rev3Surge}); err != nil {
		t.Fatalf("pass 2 (enable v3 serving): %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(v2Pod), &corev1.Pod{}); err != nil {
		t.Fatalf("v2 pod was removed before v3 became PodReady: %v", err)
	}

	// Pass 3: PodReady confirms v3 is in rotation, so v2 may drain.
	rev3SurgeFresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: rev3SurgeName}, rev3SurgeFresh); err != nil {
		t.Fatalf("re-read v3 surge: %v", err)
	}
	rev3SurgeFresh.Status.Conditions = append(rev3SurgeFresh.Status.Conditions, corev1.PodCondition{
		Type: corev1.PodReady, Status: corev1.ConditionTrue,
	})
	if err := c.Status().Update(context.Background(), rev3SurgeFresh); err != nil {
		t.Fatalf("flip v3 surge PodReady: %v", err)
	}
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{v2Pod, rev3SurgeFresh}); err != nil {
		t.Fatalf("pass 3 (drain + delete v2): %v", err)
	}
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(v2Pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Errorf("v2 pod should be deleted after v3 became PodReady: %v", err)
	}

	// Pass 4: v3 surge promoted.
	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{rev3SurgeFresh})
	if err != nil {
		t.Fatalf("pass 4 (promote): %v", err)
	}
	if !done {
		t.Fatalf("pass 4 should return done=true after promote")
	}

	// Final assertions.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("post-v3-promote pod count: got %d want 1", len(pods.Items))
	}
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read after promote: %v", err)
	}
	s := legacyInstanceStatusesOnIR(c, freshISVC, workload.ComponentEngine)[0]
	if s.RunningRevision != rev3.Name {
		t.Errorf("post-promote RunningRevision: got %q want %q", s.RunningRevision, rev3.Name)
	}
	if s.ActiveOrdinal != 0 {
		t.Errorf("post-promote ActiveOrdinal: got %d want 0 (alternated back)", s.ActiveOrdinal)
	}
}

// TestSurgeUpdate_StaleRevPodAtSurgeSlot_Drained pins the stale-slot
// eviction path. When a pod labeled with the just-promoted
// RunningRevision survives at the surge ordinal, the rev-hash recheck
// must route it out of the surge bucket and the stale-slot branch must
// delete it — and ONLY it: the canonical pod at the old ordinal keeps
// serving until the real Phase 2 drain, after the correct-rev surge pod
// is Ready (cleanup never pulls the healthy source out of rotation).
// Phase 3 must NOT promote on the stale pod.
func TestSurgeUpdate_StaleRevPodAtSurgeSlot_Drained(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()

	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")
	if err := status.StampReadyAtOrdinal(context.Background(), input, 0, rev2.Name, 1); err != nil {
		t.Fatalf("seed post-v2-promote: %v", err)
	}

	canonical := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	canonical.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev2.Name)
	if err := c.Create(context.Background(), canonical); err != nil {
		t.Fatalf("seed canonical: %v", err)
	}

	// STALE pod at ord=0 — also labeled with rev2.
	stale := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	stale.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev2.Name)
	if err := c.Create(context.Background(), stale); err != nil {
		t.Fatalf("seed stale: %v", err)
	}
	// Re-read so input observes the post-v2-promote state.
	input = legacyTestInput(isvc, c, workload.ComponentEngine)

	rev3 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev3hash")

	// Pass 1: evict the stale pod at the surge slot; leave the canonical.
	if _, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], rev3, []*corev1.Pod{canonical, stale}); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)

	// Only the stale pod at the surge slot is deleted; the canonical pod
	// keeps serving until the real Phase 2 drain.
	freshCanonical := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(canonical), freshCanonical); err != nil {
		t.Errorf("canonical pod must survive the stale-slot eviction: %v", err)
	} else {
		for _, cond := range freshCanonical.Status.Conditions {
			if cond.Type == query.ServingConditionType && cond.Status != corev1.ConditionTrue {
				t.Errorf("canonical pod's serving gate was flipped by the stale-slot eviction: %+v", cond)
			}
		}
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(stale), &corev1.Pod{}); err == nil {
		t.Errorf("stale pod at surge slot should be deleted, but still exists")
	}

	// Status MUST still say Updating with TargetRev=rev3 — no promote on a drained slot.
	freshISVC := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshISVC); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	s := legacyInstanceStatusesOnIR(c, freshISVC, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating (no promote on a drained slot)", s.Phase)
	}
	if s.Operation == nil || s.Operation.TargetRevision != rev3.Name {
		t.Fatalf("Op.TargetRevision: got %+v want rev3 (%q)", s.Operation, rev3.Name)
	}
	if s.RunningRevision != rev2.Name {
		t.Errorf("RunningRevision: got %q want rev2 (%q) — must NOT advance to rev3 yet",
			s.RunningRevision, rev2.Name)
	}
}

// TestSurgeUpdate_Phase1StampsStepSurgeAndCreatesSurgePod pins the
// entry point: from a steady-state Instance (1 old pod at ordinal 0),
// surgeUpdate stamps Op{Step=Surge, TargetRev} and creates a new pod
// at ordinal 1. The old pod is untouched (still serving). ActiveOrdinal
// stays at 0 — it advances only after promote.
func TestSurgeUpdate_Phase1StampsStepSurgeAndCreatesSurgePod(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir, oldPod)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: Phase 1 just stamped + created surge pod")
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := legacyInstanceStatusesOnIR(c, fresh, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", s.Phase)
	}
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepSurge {
		t.Errorf("Operation.Step: want Surge, got %+v", s.Operation)
	}
	if s.TargetRevision != tcr.Name {
		t.Errorf("TargetRevision: got %q want %q", s.TargetRevision, tcr.Name)
	}
	if s.ActiveOrdinal != 0 {
		t.Errorf("ActiveOrdinal should stay 0 until promote, got %d", s.ActiveOrdinal)
	}

	// Surge pod should now exist at ordinal 1.
	surgeName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 1)
	surgePod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: surgeName}, surgePod); err != nil {
		t.Fatalf("surge pod %s not created: %v", surgeName, err)
	}
	if got := surgePod.Labels[query.LabelPodOrdinal]; got != "1" {
		t.Errorf("surge pod ordinal label: got %q want 1", got)
	}
}

// TestSurgeUpdate_Phase1WaitsForSurgeReady pins the no-downtime
// invariant: surge pod exists but ContainersReady=False → caller does
// NOT touch the old pod's serving gate. The old pod stays in rotation
// until the surge proves ready.
func TestSurgeUpdate_Phase1WaitsForSurgeReady(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	// Surge pod exists but NOT ContainersReady.
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, false, false)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir, oldPod, surgePod)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	// Pre-stamp Step=Surge so we test the "waiting" branch, not the entry.
	if err := status.StampSurging(context.Background(), input, 0, tcr.Name, workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp: %v", err)
	}

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod, surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: surge not yet Ready")
	}

	// Old pod's serving gate must still be True (no drain started).
	freshOld := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld)
	for _, cond := range freshOld.Status.Conditions {
		if cond.Type == query.ServingConditionType && cond.Status != corev1.ConditionTrue {
			t.Errorf("old pod's serving gate was flipped prematurely: %+v", cond)
		}
	}
}

// TestSurgeUpdate_HoldsSourceDrainUntilReplacementPodReady verifies that
// source overlap is preserved until kubelet reports the replacement ready.
func TestSurgeUpdate_HoldsSourceDrainUntilReplacementPodReady(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, false)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b-engine-rev-v2hash",
			Namespace: isvc.Namespace,
		},
	}
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	c := legacyNewFakeClient(t, isvc, ir, oldPod, surgePod, target)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	if err := status.StampSurging(context.Background(), input, 0, target.Name, workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp: %v", err)
	}

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate before PodReady: %v", err)
	}
	if done {
		t.Fatalf("expected done=false before replacement PodReady")
	}

	freshOld := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld); err != nil {
		t.Fatalf("source missing before replacement PodReady: %v", err)
	}
	if !podreadiness.IsServing(freshOld) {
		t.Fatalf("source left rotation before replacement PodReady")
	}
	freshSurge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(surgePod), freshSurge); err != nil {
		t.Fatalf("get replacement: %v", err)
	}
	if !podreadiness.IsServing(freshSurge) {
		t.Fatalf("replacement serving gate was not enabled")
	}
	if podreadiness.IsPodReady(freshSurge) {
		t.Fatalf("replacement unexpectedly PodReady")
	}
	row := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if row.Operation == nil || row.Operation.Step != workload.UpdateStepSurge {
		t.Fatalf("operation advanced before replacement PodReady: %+v", row.Operation)
	}

	freshSurge.Status.Conditions = append(freshSurge.Status.Conditions, corev1.PodCondition{
		Type: corev1.PodReady, Status: corev1.ConditionTrue,
	})
	if err := c.Status().Update(context.Background(), freshSurge); err != nil {
		t.Fatalf("mark replacement PodReady: %v", err)
	}
	done, err = surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, []*corev1.Pod{freshOld, freshSurge})
	if err != nil {
		t.Fatalf("surgeUpdate after PodReady: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while deleting source")
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("source was not deleted after replacement PodReady: %v", err)
	}
}

// TestSurgeUpdate_Phase3PromotesAndAdvancesActiveOrdinal pins the
// terminator: when no old pods remain (Phase A complete + delete
// observed), surgeUpdate returns done=true, sets Phase=Ready,
// RunningRevision=target, clears Operation, AND advances ActiveOrdinal
// to the new slot.
func TestSurgeUpdate_Phase3PromotesAndAdvancesActiveOrdinal(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	// Only the surge pod exists, runtime-ready and serving (post-Phase 2).
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir, surgePod)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	// The surge pod carries the pinned target's rev-hash, exactly as
	// createMissingPods stamps it — reclassifyByRevisionHash keeps only
	// target-labeled pods in the surge bucket.
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionFromName(tcr.Name).Hash()
	if err := c.Update(context.Background(), surgePod); err != nil {
		t.Fatalf("restamp surge pod rev-hash: %v", err)
	}
	// Status reflects mid-surge state at Step=Drain.
	if err := status.StampSurging(context.Background(), input, 0, tcr.Name, workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	if err := status.StampSurgeDrainStep(context.Background(), input, 0); err != nil {
		t.Fatalf("pre-stamp drain: %v", err)
	}

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, []*corev1.Pod{surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true: no old pods, surge ready, promote")
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := legacyInstanceStatusesOnIR(c, fresh, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	if s.RunningRevision != tcr.Name {
		t.Errorf("RunningRevision: got %q want %q", s.RunningRevision, tcr.Name)
	}
	if s.Operation != nil {
		t.Errorf("Operation should be cleared, got %+v", s.Operation)
	}
	if s.TargetRevision != "" {
		t.Errorf("TargetRevision should be cleared, got %q", s.TargetRevision)
	}
	if s.ActiveOrdinal != 1 {
		t.Errorf("ActiveOrdinal should advance to 1 (the new slot), got %d", s.ActiveOrdinal)
	}
}

// TestSurgeUpdate_AlternatesOrdinalAcrossSurges pins the toggle: after
// a previous surge promoted ActiveOrdinal=1, the next surge creates a
// pod at ordinal 0 (not at ordinal 2 — names alternate, bounded set).
func TestSurgeUpdate_AlternatesOrdinalAcrossSurges(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	// Simulate post-previous-surge state: ActiveOrdinal=1, pod at ord=1.
	ir.Status.InstanceStatuses[0].ActiveOrdinal = 1
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := legacyTargetSpecImage("llama:v3")
	c := legacyNewFakeClient(t, isvc, ir, oldPod)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	_, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], tcr, []*corev1.Pod{oldPod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}

	// New surge pod should now exist at ordinal 0 (since old is at 1).
	newSurgeName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 0)
	surgePod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: newSurgeName}, surgePod); err != nil {
		t.Fatalf("next-surge pod %s should exist at ordinal 0: %v", newSurgeName, err)
	}
	if got := surgePod.Labels[query.LabelPodOrdinal]; got != "0" {
		t.Errorf("ordinal label: got %q want 0", got)
	}
}

// TestSurgeUpdate_GangSurges pins that surgeUpdate branches a multi-pod
// Instance to gangSurgeUpdate, which on its first pass stamps the source
// for a surge and requeues (done=false) rather than refusing to act.
func TestSurgeUpdate_GangSurges(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	// Synthesize a multi-pod (gang) Instance.
	plan.Instances[0].Runners[0].Size = 4
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("gang surge: unexpected error: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: gang surge is multi-pass")
	}
}

// gangSurgePlan builds a multi-pod (leader + worker) single-Instance
// SurgeThenDrain ComponentPlan — the multi-node engine and PD
// multi-node topologies. inst.TotalPods() == 2 routes surgeUpdate to
// gangSurgeUpdate.
func gangSurgePlan() workload.ComponentPlan {
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
			Type: workload.UpdateStrategySurgeThenDrain,
		},
	}
}

// gangPodAt fabricates a leader/worker gang member pod for a multi-pod
// SurgeThenDrain instance, labeled with revHash so the convergence
// assertions can tell which revision the pod actually carries.
func gangPodAt(isvc *v1beta1.InferenceService, instanceIdx int32, runner, revHash string, ready, serving bool) *corev1.Pod {
	pod := legacyPodForInstance(isvc, instanceIdx, ready, serving)
	pod.Name = query.PodName(isvc.Name, workload.ComponentEngine, instanceIdx, runner, 0)
	pod.Labels[query.LabelRunner] = runner
	pod.Labels[query.LabelPodOrdinal] = "0"
	pod.Labels[query.LabelRevisionHash] = revHash
	return pod
}

// gangSurgeInFlightIR builds the IR carrying in-flight v2 gang-surge status: the
// source (idx=0) is Phase=Updating with an Operation pinned to v2
// (Step=Surge, SurgeIndex=1), and the surge index (idx=1) carries the
// GangSurgeTarget marker. The dispatcher's `target` pointer may already
// have moved to a newer rev (the re-bump), but the Operation stays pinned.
func gangSurgeInFlightIR(isvc *v1beta1.InferenceService, runningRev, pinnedRev string) *v1beta1.InferenceReplica {
	return legacyInstanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: runningRev,
			TargetRevision:  pinnedRev,
			Operation: &v1beta1.InstanceOperation{
				ID:             "gangsurge-0-1",
				Type:           v1beta1.InstanceOperationType(workload.InstanceOperationUpdate),
				Step:           workload.UpdateStepSurge,
				SurgeIndex:     ptrInt32(1),
				TargetRevision: pinnedRev,
			},
		},
		v1beta1.OMENativeInstanceStatus{
			Index:          1,
			Incarnation:    1,
			Phase:          v1beta1.OMENativeInstanceCreating,
			TargetRevision: pinnedRev,
			Operation: &v1beta1.InstanceOperation{
				ID:             "gangsurgetarget-1-1",
				Type:           v1beta1.InstanceOperationType(workload.InstanceOperationUpdate),
				Step:           workload.UpdateStepGangSurgeTarget,
				TargetRevision: pinnedRev,
			},
		},
	)
}

// gangInputWithRemove is legacyTestInput with RemoveInstance wired —
// gangSurgeUpdate's promote step drops the source instance from the IR, so the
// bare legacyTestInput (RemoveInstance == nil) would NPE before reaching
// the convergence assertion.
func gangInputWithRemove(isvc *v1beta1.InferenceService, c client.Client) workload.ReconcileInput {
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.RemoveInstance = legacyRemoveInstance(c, isvc, workload.ComponentEngine)
	return input
}

// TestGangSurgeUpdate_BumpDuringBump_PromotesPinnedRev pins the gang
// (multi-pod) "re-bump mid-roll" convergence-safety invariant.
//
// Setup is the moment the v2 gang surge reaches its promote step: a v2
// replacement gang at the surge index (idx=1) is Ready + serving, the
// source gang (idx=0) is already drained, and the source's recorded
// Operation is pinned to v2 (Step=Surge, SurgeIndex=1, TargetRevision=v2).
// A second spec bump to v3 has already moved the dispatcher's `target`
// pointer to v3 (the re-bump mid-roll) — but the surge in flight is
// committed to v2.
//
// The promote MUST stamp RunningRevision=v2 (the rev the surged pods
// actually carry), NOT v3 (the latest target). Stamping v3 would leave
// status claiming v3 while the pods run v2, and
// DetectUpdateTrigger's fast path (RunningRevision == target) would then
// short-circuit forever — the rollout never re-fires to drive the pods to
// v3, leaving them stranded on the intermediate revision. With the
// running rev pinned to v2, the next reconcile sees RunningRevision=v2 !=
// target=v3 and starts a fresh surge cycle toward v3 — convergence.
func TestGangSurgeUpdate_BumpDuringBump_PromotesPinnedRev(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "llama-70b-engine-rev-v1hash"
	v2Name := "llama-70b-engine-rev-v2hash"
	v3Name := "llama-70b-engine-rev-v3hash"
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)

	// Seed the in-flight v2 gang surge at its promote step (on IR).
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)

	c := legacyNewFakeClient(t, isvc, ir)
	makeCR(t, c, isvc, v2Name)
	makeCR(t, c, isvc, v3Name)

	// v2 replacement gang (leader + worker) at the surge index, Ready +
	// serving — the source has been drained, so step 5 (promote) is next.
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 1, runner, v2Hash, true, true)); err != nil {
			t.Fatalf("seed v2 surge pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	// Dispatcher target is now v3 (the second bump landed mid-surge).
	v3 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v3Name}, v3); err != nil {
		t.Fatalf("get v3 CR: %v", err)
	}

	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], v3, nil)
	if err != nil {
		t.Fatalf("gang surge promote pass: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true after promote (source drained, surge Ready)")
	}

	// The promoted surge Instance (idx=1) must carry RunningRevision=v2 —
	// the rev its pods actually run — NOT the latest target v3.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC after promote: %v", err)
	}
	var promoted *v1beta1.OMENativeInstanceStatus
	statuses := legacyInstanceStatusesOnIR(c, fresh, workload.ComponentEngine)
	for i := range statuses {
		if statuses[i].Index == 1 {
			promoted = &statuses[i]
		}
	}
	if promoted == nil {
		t.Fatalf("surge Instance idx=1 missing after promote")
	}
	if promoted.RunningRevision != v2Name {
		t.Errorf("re-bump during an in-flight surge: promoted RunningRevision=%q, want %q (the rev the pods actually carry). "+
			"Stamping the latest target (%q) strands the pods on the intermediate rev — "+
			"DetectUpdateTrigger's fast path short-circuits and the rollout never converges.",
			promoted.RunningRevision, v2Name, v3Name)
	}
}

// TestGangSurgeUpdate_DrainsSourceFromRotationBeforeDelete pins half of the
// multi-node RatioBalanced invariant: once the replacement gang is PodReady
// (in rotation) and the drain step fires, the SOURCE gang's pods must be
// flipped OUT of serving rotation (serving=False) before being Deleted. A
// Deleted-but-terminating pod keeps serving=True through its grace window, so
// an undrained source would linger in ServingReplicas beside the replacement
// (a durable N+1) for the multi-reconcile drain window. Single-pod surgeUpdate
// already drains-then-deletes; this gives the gang path the same flip. (The
// complementary half — NOT draining until the replacement is PodReady so the
// gap never troughs below N — is covered by
// TestGangSurgeUpdate_HoldsSourceDrainUntilReplacementPodReady.)
func TestGangSurgeUpdate_DrainsSourceFromRotationBeforeDelete(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "llama-70b-engine-rev-v1hash"
	v2Name := "llama-70b-engine-rev-v2hash"
	v1Hash := query.RevisionHashFromControllerRevisionName(v1Name)
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)

	// In-flight gang surge: source idx=0 (Step=Surge → idx 1), target idx=1 (on IR).
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)

	c := legacyNewFakeClient(t, isvc, ir)
	makeCR(t, c, isvc, v2Name)

	// Source gang (idx=0) still alive and SERVING — not yet drained.
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, v1Hash, true, true)); err != nil {
			t.Fatalf("seed source gang pod (%s): %v", runner, err)
		}
	}
	// Replacement gang (idx=1) Ready + serving + PodReady → it is in rotation,
	// so the drain step may now flip the source out (the drain is gated on the
	// replacement being PodReady, not merely ContainersReady).
	for _, runner := range []string{"leader", "worker"} {
		p := gangPodAt(isvc, 1, runner, v2Hash, true, true)
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		})
		if err := c.Create(context.Background(), p); err != nil {
			t.Fatalf("seed surge gang pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}

	// Leave a delete-expectation outstanding on the source so the pass stops
	// at the post-drain Satisfied() gate WITHOUT deleting — letting us observe
	// the serving flip on the still-present source pods. The drain MUST run
	// before that gate.
	workload.DefaultExpectations.ExpectDeletes("prod", "llama-70b", workload.ComponentEngine, 0, 1)

	if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge drain pass: %v", err)
	}

	// Every still-present source pod must now be drained from rotation.
	srcPods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "llama-70b", workload.ComponentEngine, 0)
	if err != nil {
		t.Fatalf("list source gang pods: %v", err)
	}
	if len(srcPods) == 0 {
		t.Fatalf("source gang pods were deleted; expected them held at the " +
			"post-drain Satisfied() gate so the serving flip is observable")
	}
	for _, pod := range srcPods {
		if pod.DeletionTimestamp != nil {
			continue // already gone from rotation by deletion
		}
		if podreadiness.IsServing(pod) {
			t.Errorf("source gang pod %s is still serving=True during the drain "+
				"step — it must be flipped out of rotation before delete, else a "+
				"terminating-but-serving source lingers as a durable N+1 and "+
				"RatioBalanced pacing breaks for gangs", pod.Name)
		}
	}
}

// TestGangSurgeUpdate_HoldsSourceDeleteUntilEndpointsDrop pins the other half
// of the gang drain: clearing the serving gate only STARTS the withdrawal, and
// until the endpoint controller has republished the source leader as not-ready
// kube-proxy is still sending it requests. Deleting on the gate write sheds
// those requests. The gang waits on the same per-revision routed Service the
// single-pod surge waits on, so neither path sheds traffic.
func TestGangSurgeUpdate_HoldsSourceDeleteUntilEndpointsDrop(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "llama-70b-engine-rev-v1hash"
	v2Name := "llama-70b-engine-rev-v2hash"
	v1Hash := query.RevisionHashFromControllerRevisionName(v1Name)
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)

	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)
	c := legacyNewFakeClient(t, isvc, ir)
	makeCR(t, c, isvc, v2Name)

	// Source gang (idx=0) alive and serving on the old revision.
	sourceLeader := gangPodAt(isvc, 0, "leader", v1Hash, true, true)
	if err := c.Create(context.Background(), sourceLeader); err != nil {
		t.Fatalf("seed source leader: %v", err)
	}
	if err := c.Create(context.Background(), gangPodAt(isvc, 0, "worker", v1Hash, true, true)); err != nil {
		t.Fatalf("seed source worker: %v", err)
	}
	// Replacement gang (idx=1) PodReady, so the drain step is free to fire.
	for _, runner := range []string{"leader", "worker"} {
		p := gangPodAt(isvc, 1, runner, v2Hash, true, true)
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		})
		if err := c.Create(context.Background(), p); err != nil {
			t.Fatalf("seed replacement pod (%s): %v", runner, err)
		}
	}
	// The source leader is published Ready in its per-revision routed Service:
	// the data plane is still sending it traffic.
	sourceService := query.PerRevisionServiceName("llama-70b", workload.ComponentEngine, v1Hash)
	if err := c.Create(context.Background(),
		legacySliceWithEndpoint("prod", "source-slice", sourceService, sourceLeader, true)); err != nil {
		t.Fatalf("seed source endpointslice: %v", err)
	}

	input := gangInputWithRemove(isvc, c)
	deps := legacyTestDeps(c)
	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}

	livePods := func(when string) []*corev1.Pod {
		pods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "llama-70b", workload.ComponentEngine, 0)
		if err != nil {
			t.Fatalf("list source gang pods (%s): %v", when, err)
		}
		return pods
	}

	// Pass 1: the source leaves rotation but is still an endpoint, so nothing
	// may be deleted and the pass asks to be re-driven.
	done, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil)
	if err != nil {
		t.Fatalf("gang surge drain pass: %v", err)
	}
	if done {
		t.Fatalf("gang surge reported complete while the source is still routed")
	}
	held := livePods("routed")
	if len(held) != 2 {
		t.Fatalf("source gang has %d pods, want 2 held while still routed", len(held))
	}
	for _, pod := range held {
		if pod.DeletionTimestamp != nil {
			t.Errorf("source gang pod %s deleted while still published in %s — "+
				"the requests kube-proxy is still routing to it are shed", pod.Name, sourceService)
		}
		if podreadiness.IsServing(pod) {
			t.Errorf("source gang pod %s still serving=True; the drain flip must precede the wait", pod.Name)
		}
	}

	// The endpoint controller catches up: the leader is no longer routable.
	converged := &discoveryv1.EndpointSlice{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "source-slice"}, converged); err != nil {
		t.Fatalf("get source endpointslice: %v", err)
	}
	notReady := false
	converged.Endpoints[0].Conditions.Ready = &notReady
	if err := c.Update(context.Background(), converged); err != nil {
		t.Fatalf("converge source endpointslice: %v", err)
	}

	// Pass 2: endpoints converged → the whole source gang is deleted.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge delete pass: %v", err)
	}
	for _, pod := range livePods("drained") {
		t.Errorf("source gang pod %s survived the pass after its endpoint dropped", pod.Name)
	}
}

// TestGangSurgeUpdate_BumpDuringBump_CreatesPinnedRevPods pins the
// create-side half of the same invariant: while a gang surge is in flight
// with Op pinned to v2, a second bump to v3 must NOT cause the surge gang's
// pods to be created with the v3 revision-hash. The in-flight surge is
// committed to v2; its pods must be labeled v2 so the per-revision drain
// Service and the post-promote running-rev anchor stay consistent.
// TestGangSurgeUpdate_BumpDuringBump_AbandonsSupersededSurge pins the
// level-triggered redirect for gangs: a mid-surge spec bump to a newer rev, while
// the in-flight surge's replacement gang is NOT yet up (pods not created / not
// Ready), ABANDONS the superseded surge — it does NOT create pods for the now-dead
// rev. The source gang at idx=0 is left untouched (capacity holds), and the source
// Instance is reset to Ready so the next reconcile re-surges toward the current
// desired through the normal gated path. (A surge already Ready and about to
// promote instead keeps its pin — see _PromotesPinnedRev — so its promote stays
// truthful; that's the no-false-promote invariant this redirect preserves.)
//
// Without the redirect (creating pods on the pinned intermediate rev), an
// intermediate surge that never became Ready and never escalated would
// deadlock, holding the maxSurge budget on a dead rev.
func TestGangSurgeUpdate_BumpDuringBump_AbandonsSupersededSurge(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "llama-70b-engine-rev-v1hash"
	v2Name := "llama-70b-engine-rev-v2hash"
	v3Name := "llama-70b-engine-rev-v3hash"
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)

	// In-flight v2 gang surge, BEFORE the surge pods are created (on IR).
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)

	c := legacyNewFakeClient(t, isvc, ir)
	makeCR(t, c, isvc, v2Name)
	makeCR(t, c, isvc, v3Name)
	// Source gang still alive at idx=0 (capacity must hold across the redirect).
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, v2Hash, true, true)); err != nil {
			t.Fatalf("seed source pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	v3 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v3Name}, v3); err != nil {
		t.Fatalf("get v3 CR: %v", err)
	}

	// Bump to v3 mid-surge, before the v2 surge gang exists. The superseded v2
	// surge must be abandoned — NOT created on v2.
	if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], v3, nil); err != nil {
		t.Fatalf("gang surge redirect pass: %v", err)
	}

	// No surge pods created at idx=1 (the superseded v2 surge was abandoned).
	surgePods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "llama-70b", workload.ComponentEngine, 1)
	if err != nil {
		t.Fatalf("list surge gang pods: %v", err)
	}
	if len(surgePods) != 0 {
		t.Errorf("superseded v2 surge should be abandoned; got %d pod(s) created at idx=1", len(surgePods))
	}

	// Source gang at idx=0 untouched — capacity holds during the redirect.
	srcPods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "llama-70b", workload.ComponentEngine, 0)
	if err != nil {
		t.Fatalf("list source gang pods: %v", err)
	}
	if len(srcPods) != 2 {
		t.Errorf("source gang at idx=0 must be untouched (2 pods) during the redirect; got %d", len(srcPods))
	}

	// Source Instance reset to Ready (the v2 surge Op is dropped) so the next
	// reconcile re-surges toward the current desired (v3).
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read isvc: %v", err)
	}
	for _, s := range legacyInstanceStatusesOnIR(c, fresh, workload.ComponentEngine) {
		if s.Index == 0 {
			if s.Phase != v1beta1.OMENativeInstanceReady {
				t.Errorf("source Instance idx=0 should reset to Ready after abandon; got %q", s.Phase)
			}
			if s.Operation != nil {
				t.Errorf("source Instance idx=0 Operation should be cleared after abandon; got %+v", s.Operation)
			}
		}
	}
}

// ptrInt32 returns a pointer to v — local helper for SurgeIndex fixtures.
func ptrInt32(v int32) *int32 { return &v }

// TestSurgeUpdate_PartitionByLabelHandlesLegacyPods pins backward
// compatibility for partitionPodsBySurgeOrdinal: pods without the
// LabelPodOrdinal label (pre-feature pods on the cluster) fall through
// to ordinal=0 via the PodOrdinalFromLabels default. Mixing one legacy
// pod (treated as ordinal 0) with a surge pod at ordinal 1 must NOT
// classify the legacy pod as a straggler.
func TestSurgeUpdate_PartitionByLabelHandlesLegacyPods(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
	// Legacy old pod — has all the standard labels EXCEPT LabelPodOrdinal.
	legacyOld := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	delete(legacyOld.Labels, query.LabelPodOrdinal)
	surgePodFresh := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)

	old, surge, stragglers := partitionPodsBySurgeOrdinal([]*corev1.Pod{legacyOld, surgePodFresh}, 0, 1)
	if len(stragglers) != 0 {
		t.Errorf("legacy pod should be treated as ordinal 0, not stragglers; got %d stragglers", len(stragglers))
	}
	if len(old) != 1 {
		t.Errorf("old: got %d want 1 (legacy pod with default ordinal 0)", len(old))
	}
	if len(surge) != 1 {
		t.Errorf("surge: got %d want 1 (surge pod at ordinal 1)", len(surge))
	}
}

// TestPatchInstanceStatusReadyOnRevisionWithOrdinal_Idempotent pins the
// promote helper's idempotency: re-invoking with the same target and
// ordinal is a no-op (no ResourceVersion bump).
func TestPatchInstanceStatusReadyOnRevisionWithOrdinal_Idempotent(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)

	if err := status.StampReadyAtOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
		t.Fatalf("first call: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	if err := status.StampReadyAtOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
		t.Fatalf("second call: %v", err)
	}
	afterRV := readRV(t, c, isvc)
	if beforeRV != afterRV {
		t.Errorf("idempotent call bumped ResourceVersion: %s -> %s", beforeRV, afterRV)
	}
}

// The SurgeThenDrain cycle runs two ordinal slots on one Instance index,
// so almost everything that happens to it while it is in flight has to be
// answered without moving the step: the source is capacity until the
// drain says otherwise, and the replacement is the only route back to it.
// These tests pin that half of the contract for both surge states — what
// an observation, an operator edit or an apiserver refusal does NOT move.

// surgeOutcome is everything one pass over a surge in flight may move.
type surgeOutcome struct {
	done          bool
	phase         v1beta1.OMENativeInstancePhase
	step          string
	activeOrdinal int32
	waiting       string
	failed        bool
	blocks        int
	sourceAlive   bool
	sourceServing bool
	targetAlive   bool
	targetServing bool
}

// runSurgePass drives one UpdateWithPods pass over the fixture's live pod
// set and reports what moved. tweak may edit the input and the plan the
// way an operator config change would.
func runSurgePass(t *testing.T, f *midSurgeFixture, tweak func(*workload.ReconcileInput, *workload.ComponentPlan)) surgeOutcome {
	t.Helper()
	in := legacyTestInput(f.isvc, f.client, workload.ComponentEngine)
	in.ObservedState.UpdateRevision = f.targetCR.Name
	blocks := &[]string{}
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	if tweak != nil {
		tweak(&in, &plan)
	}
	done, err := UpdateWithPods(context.Background(), legacyTestDeps(f.client), in, plan,
		plan.Instances[0], f.targetCR, f.targetSpec, f.survivingPods(t))
	if err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	s := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	out := surgeOutcome{
		done:          done,
		phase:         s.Phase,
		activeOrdinal: s.ActiveOrdinal,
		failed:        s.LastFailure != nil,
		blocks:        len(*blocks),
	}
	if s.Operation != nil {
		out.step, out.waiting = s.Operation.Step, s.Operation.Waiting
	}
	if pod, alive := f.livePod(t, f.sourcePod); alive {
		out.sourceAlive, out.sourceServing = true, podreadiness.IsServing(pod)
	}
	if pod, alive := f.livePod(t, f.surgePod); alive {
		out.targetAlive, out.targetServing = true, podreadiness.IsServing(pod)
	}
	return out
}

// surgeConfigChangeCases is the operator-config edit set that no surge
// step reads. It is the shared set plus the relocation budget, which the
// surge rows name and the non-surge rows do not.
//
// Two knobs in those cells have no field to set here. teardownDeadline
// lives on the owner's teardown clock and retryBlockHistoryLimit on the
// status writer's end-of-pass prune; what a surge pass owes either is to
// write nothing of its own, which the outcome comparison asserts.
func surgeConfigChangeCases() []configChange {
	return append(configChangeCases(), configChange{
		name: "auto-migration budget",
		tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
			in.Disposition.AutoMigrateMaxAttempts = 3
		},
	})
}

// TestSurge_OperatorConfigChangesDoNotChangeWhatItDecides: none of these
// knobs is read where a surge decides its next action. The retry policy
// and the relocation budget are consulted only once an escalation fires,
// the teardown and scale-down knobs belong to their own passes, the gang
// clamp to the PodGroup build, the audit caps to migration admission, and
// the requeue cadence only paces how soon the row is looked at again. An
// edited pass must therefore leave the row and both ordinal slots exactly
// where the unedited one does.
func TestSurge_OperatorConfigChangesDoNotChangeWhatItDecides(t *testing.T) {
	for _, step := range []struct {
		name          string
		step          string
		surgeReady    bool
		sourceServing bool
	}{
		{"surge", workload.UpdateStepSurge, false, true},
		{"drain", workload.UpdateStepSurgeDrain, true, false},
	} {
		t.Run(step.name, func(t *testing.T) {
			want := runSurgePass(t, newMidSurgeFixture(t, step.step, step.surgeReady, step.sourceServing), nil)
			for _, tc := range surgeConfigChangeCases() {
				t.Run(tc.name, func(t *testing.T) {
					f := newMidSurgeFixture(t, step.step, step.surgeReady, step.sourceServing)
					if got := runSurgePass(t, f, tc.tweak); got != want {
						t.Errorf("the %s edit moved the cycle: got %+v want the unedited %+v", tc.name, got, want)
					}
				})
			}
		})
	}
}

// TestSurge_PodObservationsThatHoldTheStep: the surge advances on one
// thing — the replacement clearing the promote bar — and the drain on
// one other — the source's pods disappearing. Everything else the
// kubelet can report about either slot is a wait: none of them ends the
// attempt, recreates the occupied slot, or moves the step. The operation
// deadline is what bounds them.
func TestSurge_PodObservationsThatHoldTheStep(t *testing.T) {
	for _, tc := range []struct {
		name          string
		step          string
		surgeReady    bool
		sourceServing bool
		// observe decorates the fixture in place, in the cluster, so the
		// pass's own live read sees it.
		observe func(t *testing.T, f *midSurgeFixture)
	}{
		{
			name: "a container of the replacement restarting",
			step: workload.UpdateStepSurge, sourceServing: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				patchPodStatus(t, f, f.surgePod, func(pod *corev1.Pod) {
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
						Name: "main", RestartCount: 4,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
					}}
				})
			},
		},
		{
			name: "the replacement wedged Terminating at the surge ordinal",
			step: workload.UpdateStepSurge, sourceServing: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				terminatingPod(t, f.client, f.surgePod)
			},
		},
		{
			name: "the replacement in a terminal Failed phase",
			step: workload.UpdateStepSurge, sourceServing: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				patchPodStatus(t, f, f.surgePod, func(pod *corev1.Pod) {
					pod.Status.Phase = corev1.PodFailed
				})
			},
		},
		{
			name: "the draining source wedged Terminating",
			step: workload.UpdateStepSurgeDrain, surgeReady: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				terminatingPod(t, f.client, f.sourcePod)
			},
		},
		{
			name: "the node under the draining source gone",
			step: workload.UpdateStepSurgeDrain, surgeReady: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				terminatingPod(t, f.client, f.sourcePod)
				patchPodStatus(t, f, f.sourcePod, func(pod *corev1.Pod) {
					pod.Status.Reason = "NodeLost"
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, tc.step, tc.surgeReady, tc.sourceServing)
			tc.observe(t, f)

			got := runSurgePass(t, f, nil)

			if got.done {
				t.Errorf("done: got true want false (none of these completes the cycle)")
			}
			if got.phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("Phase: got %q want Updating", got.phase)
			}
			if got.step != tc.step {
				t.Errorf("Operation.Step: got %q want %q", got.step, tc.step)
			}
			if got.activeOrdinal != 0 {
				t.Errorf("ActiveOrdinal: got %d want 0 (no promote)", got.activeOrdinal)
			}
			if got.failed {
				t.Errorf("LastFailure: got one want none; only the deadline ends this cycle")
			}
			if got.blocks != 0 {
				t.Errorf("RetryBlock writes: got %d want none", got.blocks)
			}
			if !got.sourceAlive {
				t.Errorf("the source was deleted; nothing here converges a drain")
			}
			if !got.targetAlive {
				t.Errorf("the occupied surge slot was recreated or collected")
			}
		})
	}
}

// patchPodStatus applies mutate to the live copy of pod and writes the
// status back, so the pass's own read observes it.
func patchPodStatus(t *testing.T, f *midSurgeFixture, pod *corev1.Pod, mutate func(*corev1.Pod)) {
	t.Helper()
	live, alive := f.livePod(t, pod)
	if !alive {
		t.Fatalf("pod %s is gone", pod.Name)
	}
	mutate(live)
	if err := f.client.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("patch pod %s status: %v", pod.Name, err)
	}
}

// TestSurgeDrain_RepeatedUnrouteAfterTheDeleteWritesNothing: the drain
// takes the source out of rotation itself and then deletes it. Once that
// delete is issued, the source being reported out of rotation again — by
// its gate or by its endpoint — is a restatement of what the step already
// acted on, so the pass re-derives the same wait and commits nothing.
//
// The delete is held open with a finalizer, which is what keeps the row
// on SurgeDrain for the second observation instead of promoting past it.
func TestSurgeDrain_RepeatedUnrouteAfterTheDeleteWritesNothing(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurgeDrain, true /* surgeReady */, false /* sourceServing */)
	terminatingPod(t, f.client, f.sourcePod)

	first := runSurgePass(t, f, nil)
	if first.step != workload.UpdateStepSurgeDrain || first.done {
		t.Fatalf("first pass: got %+v want the drain still waiting on its source", first)
	}
	rv := readRV(t, f.client, f.isvc)

	// The same two observations again, on a source whose delete is
	// already in flight: the gate is off and no endpoint is left.
	second := runSurgePass(t, f, nil)

	if second != first {
		t.Errorf("re-observed unroute moved the row: got %+v want the unchanged %+v", second, first)
	}
	if got := readRV(t, f.client, f.isvc); got != rv {
		t.Errorf("owner resourceVersion: got %s want %s (a repeated unroute writes nothing)", got, rv)
	}
}

// TestSurgeUpdate_SourceLostBeforeTheDrainPromotesTheReplacement: a
// single-pod source that disappears before the drain step skips the whole
// drain block — there is nothing left to take out of rotation — but not
// the promote bar. The replacement is promoted onto the pinned revision
// with ActiveOrdinal advanced to its slot, and the restart trigger never
// claims the loss because the row is not Ready.
func TestSurgeUpdate_SourceLostBeforeTheDrainPromotesTheReplacement(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurge, true /* surgeReady */, true /* sourceServing */)
	if err := f.client.Delete(context.Background(), f.sourcePod); err != nil {
		t.Fatalf("lose the source: %v", err)
	}

	got := runSurgePass(t, f, nil)

	if !got.done {
		t.Fatalf("the promote must complete the cycle once the source is gone: %+v", got)
	}
	if got.phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", got.phase)
	}
	if got.activeOrdinal != 1 {
		t.Errorf("ActiveOrdinal: got %d want 1 (the replacement's slot)", got.activeOrdinal)
	}
	if !got.targetAlive {
		t.Errorf("the replacement must be the Instance's pod, not collected with the source")
	}
}

// TestSurgeUpdate_LateReadyReplacementDoesNotUnfailAStuckSurge: the
// stuck-pod escalator has already ended this attempt, and a replacement
// that reaches Ready afterwards is not a retraction. The surge entry
// stamp refuses to resurrect a Failed row at the same target, so the row
// stays Failed and recovery stays where it belongs — a corrective
// revision or the retry gate.
func TestSurgeUpdate_LateReadyReplacementDoesNotUnfailAStuckSurge(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurge, true /* surgeReady */, true /* sourceServing */)
	failInstanceRow(t, f, "StuckPod")

	got := runSurgePass(t, f, nil)

	if got.phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: got %q want Failed (a late readiness observation does not un-fail the row)", got.phase)
	}
	if got.activeOrdinal != 0 {
		t.Errorf("ActiveOrdinal: got %d want 0 (nothing is promoted out of Failed)", got.activeOrdinal)
	}
	if got.done {
		t.Errorf("done: got true want false")
	}
}

// failInstanceRow stamps the row Failed with reason, the shape the
// escalation pass leaves behind.
func failInstanceRow(t *testing.T, f *midSurgeFixture, reason string) {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: f.isvc.Namespace, Name: legacyIRName(f.isvc, workload.ComponentEngine)}
	if err := f.client.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceFailed
	ir.Status.InstanceStatuses[0].LastFailure = &v1beta1.InstanceTermination{
		Reason: reason, Time: metav1.Now(),
	}
	if err := f.client.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("stamp the row Failed: %v", err)
	}
}

// TestSurge_ExcludedAnnotationEditLeavesTheCycleAlone: annotations and
// labels the revision payload filters out are not part of the hash, so
// the pinned target does not move and neither surge state has anything
// new to act on. The hash check is half the claim — without it a pass
// that decided the same thing would only mean the edit never reached the
// renderer.
func TestSurge_ExcludedAnnotationEditLeavesTheCycleAlone(t *testing.T) {
	before := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"}}
	after := &metav1.ObjectMeta{Annotations: map[string]string{
		"ome.io/base-model-name":                   "llama-7b",
		constants.ReleaseHeldRevisionAnnotationKey: "llama-70b-engine-old00001",
	}}

	for _, step := range []struct {
		name          string
		step          string
		surgeReady    bool
		sourceServing bool
	}{
		{"surge", workload.UpdateStepSurge, false, true},
		{"drain", workload.UpdateStepSurgeDrain, true, false},
	} {
		t.Run(step.name, func(t *testing.T) {
			// One fixture per pass: they share the process-wide
			// expectations cache, which every build resets.
			baseline := newMidSurgeFixture(t, step.step, step.surgeReady, step.sourceServing)
			hBefore, _, err := revision.Hash(baseline.targetSpec, before, nil, "")
			if err != nil {
				t.Fatalf("hash before the edit: %v", err)
			}
			hAfter, _, err := revision.Hash(baseline.targetSpec, after, nil, "")
			if err != nil {
				t.Fatalf("hash after the edit: %v", err)
			}
			if hBefore != hAfter {
				t.Fatalf("an excluded annotation moved the hash: before=%s after=%s", hBefore, hAfter)
			}
			want := runSurgePass(t, baseline, nil)

			f := newMidSurgeFixture(t, step.step, step.surgeReady, step.sourceServing)
			got := runSurgePass(t, f, func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.DesiredSpec.PodTemplateObjectMeta = after
			})
			if got != want {
				t.Errorf("the annotation edit moved the cycle: got %+v want the unedited %+v", got, want)
			}
		})
	}
}

// TestSurge_StrategyEditBesideAnotherWriteChangesNothing: the mechanism
// of an attempt is pinned on its operation, so an edit that lands in the
// same window as the step stamp changes neither the mode this attempt
// finishes under nor the write in flight. The pass that advances the step
// under an edited strategy is the same pass, ordinal for ordinal, as the
// one that advances it under the pinned one.
func TestSurge_StrategyEditBesideAnotherWriteChangesNothing(t *testing.T) {
	// The fixture sits exactly at the step boundary: the replacement has
	// cleared the promote bar, so this pass writes SurgeDrain.
	want := runSurgePass(t, newMidSurgeFixture(t, workload.UpdateStepSurge, true, true), nil)
	if want.step != workload.UpdateStepSurgeDrain {
		t.Fatalf("fixture: got step %q want the pass to stamp %q", want.step, workload.UpdateStepSurgeDrain)
	}

	for _, strategy := range []workload.UpdateStrategyType{
		workload.UpdateStrategyInPlaceIfPossible,
		workload.UpdateStrategyInPlaceOnly,
		workload.UpdateStrategyRecreatePod,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurge, true, true)
			boundary := runSurgePass(t, f, func(_ *workload.ReconcileInput, plan *workload.ComponentPlan) {
				plan.UpdateStrategy.Type = strategy
			})
			if boundary != want {
				t.Errorf("boundary pass under %s: got %+v want the pinned %+v", strategy, boundary, want)
			}
			// And the pass after the stamp finishes the cycle as a
			// surge: the drained source is gone and the replacement is
			// promoted onto its own ordinal, which is a shape none of
			// the edited-to strategies has.
			after := runSurgePass(t, f, func(_ *workload.ReconcileInput, plan *workload.ComponentPlan) {
				plan.UpdateStrategy.Type = strategy
			})
			if !after.done || after.phase != v1beta1.OMENativeInstanceReady || after.activeOrdinal != 1 {
				t.Errorf("after the stamp under %s: got %+v want the surge cycle completing on ordinal 1", strategy, after)
			}
		})
	}
}

// TestSurgeUpdate_ConflictOnTheServingFlipCostsAPassNotTheAttempt: both
// surge states write a serving gate — the surge puts the replacement in
// rotation, the drain takes the source out — and both writes pin the
// resourceVersion of the read they were computed from. A concurrent
// writer on the same pod therefore rejects the patch. The row is not a
// casualty of that: it keeps its phase, its step and its pin, no revision
// is blamed, and the pass simply has to run again.
func TestSurgeUpdate_ConflictOnTheServingFlipCostsAPassNotTheAttempt(t *testing.T) {
	for _, tc := range []struct {
		name string
		step string
	}{
		{name: "surge marks the replacement serving", step: workload.UpdateStepSurge},
		{name: "drain takes the source out of rotation", step: workload.UpdateStepSurgeDrain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, tc.step, true /* surgeReady */, true /* sourceServing */)
			// The replacement is runtime-ready but not yet gated, so the
			// surge step's own write is the one that races.
			clearServingGate(t, f, f.surgePod)

			in := legacyTestInput(f.isvc, f.client, workload.ComponentEngine)
			in.ObservedState.UpdateRevision = f.targetCR.Name
			blocks := &[]string{}
			in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				b := workload.RetryBlock{TargetRevision: rev}
				if d := mutate(&b); d != workload.RetryBlockUnchanged {
					*blocks = append(*blocks, rev)
				}
				return nil
			}
			plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
			deps := legacyTestDeps(conflictOnPodStatusPatch(t, f.client))

			done, err := UpdateWithPods(context.Background(), deps, in, plan, plan.Instances[0],
				f.targetCR, f.targetSpec, f.survivingPods(t))
			if done {
				t.Errorf("done: got true want false (the gate write never landed)")
			}
			if err == nil {
				t.Errorf("error: got nil want the conflict surfaced so the pass runs again")
			}

			s := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
			if s.Phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("Phase: got %q want Updating (a conflict costs a pass, not the attempt)", s.Phase)
			}
			if s.Operation == nil || s.Operation.TargetRevision != f.targetCR.Name {
				t.Errorf("Operation: got %+v want the surge still pinned to %s", s.Operation, f.targetCR.Name)
			}
			if s.ActiveOrdinal != 0 {
				t.Errorf("ActiveOrdinal: got %d want 0 (nothing was promoted)", s.ActiveOrdinal)
			}
			if len(*blocks) != 0 {
				t.Errorf("RetryBlock writes: got %v want none (a conflict blames no revision)", *blocks)
			}
		})
	}
}

// clearServingGate drops the lifecycle serving condition from pod, the
// state a replacement is in between ContainersReady and the surge's own
// gate write.
func clearServingGate(t *testing.T, f *midSurgeFixture, pod *corev1.Pod) {
	t.Helper()
	patchPodStatus(t, f, pod, func(live *corev1.Pod) {
		kept := live.Status.Conditions[:0]
		for _, cond := range live.Status.Conditions {
			if cond.Type == query.ServingConditionType {
				continue
			}
			kept = append(kept, cond)
		}
		live.Status.Conditions = kept
	})
}

// conflictOnPodStatusPatch wraps c so every pod status patch loses the
// race, the shape a concurrent writer on the same object produces.
func conflictOnPodStatusPatch(t *testing.T, c client.Client) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return apierrors.NewConflict(corev1.Resource("pods"), obj.GetName(),
					errors.New("the object has been modified; please apply your changes to the latest version"))
			}
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

// TestSurgeDrainRecreate_RejectionDispositions: past the surge step the
// source is already out of rotation, so the create that rebuilds a lost
// replacement is the row's only way back to capacity. It meets the same
// classified rejections a fresh create does and is disposed the same way:
// a 422 is permanent and blames the pinned revision, a terminating
// namespace is permanent and blames nothing, a quota refusal is a wait
// that parks the clock, and a 429 is pacing that writes nothing at all.
func TestSurgeDrainRecreate_RejectionDispositions(t *testing.T) {
	for _, tc := range surgeRejectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurgeDrain, true /* surgeReady */, false /* sourceServing */)
			// The replacement is lost, so this pass's work is the rebuild
			// create — the only write a rejection can land on here.
			if err := f.client.Delete(context.Background(), f.surgePod); err != nil {
				t.Fatalf("lose the replacement: %v", err)
			}
			assertSurgeRejection(t, f, tc)
		})
	}
}

// TestSurgeCreate_RejectionDispositions: the surge's Phase 1 create meets
// the same classified rejections, and the source keeps serving through
// every one of them. A 422 ends the attempt with the pinned revision
// blamed, a terminating namespace ends it with nothing blamed, a quota
// refusal waits with the clock parked, and a 429 writes nothing.
func TestSurgeCreate_RejectionDispositions(t *testing.T) {
	for _, tc := range surgeRejectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurge, false /* surgeReady */, true /* sourceServing */)
			if err := f.client.Delete(context.Background(), f.surgePod); err != nil {
				t.Fatalf("clear the surge slot: %v", err)
			}
			assertSurgeRejection(t, f, tc)
		})
	}
}

// surgeRejection is one classified apiserver refusal and what the surge
// owes it.
type surgeRejection struct {
	name       string
	reject     error
	wantFailed bool
	wantReason string
	wantBlocks int
	wantWait   bool
	wantPacing time.Duration
}

func surgeRejectionCases() []surgeRejection {
	return []surgeRejection{
		{
			name:       "invalid pod spec",
			reject:     siteInvalidError(),
			wantFailed: true,
			wantReason: workload.RejectionReasonInvalidPodSpec,
			wantBlocks: 1,
		},
		{
			name:       "namespace terminating",
			reject:     siteNamespaceTerminatingError("engine-pod"),
			wantFailed: true,
			wantReason: workload.RejectionReasonNamespaceTerminating,
		},
		{
			name:     "quota exceeded",
			reject:   siteQuotaError(),
			wantWait: true,
		},
		{
			name:       "throttled",
			reject:     apierrors.NewTooManyRequests("apiserver is shedding load", 7),
			wantPacing: 7 * time.Second,
		},
	}
}

// assertSurgeRejection drives one pass whose pod creates are refused with
// tc.reject and checks the row against what the disposition owes.
func assertSurgeRejection(t *testing.T, f *midSurgeFixture, tc surgeRejection) {
	t.Helper()
	base := f.client
	in := legacyTestInput(f.isvc, base, workload.ComponentEngine)
	in.ObservedState.UpdateRevision = f.targetCR.Name
	in.Pacing = &workload.APIPacing{}
	blocks := &[]string{}
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	deps := legacyTestDeps(rejectPodCreates(t, base, tc.reject))

	done, err := UpdateWithPods(context.Background(), deps, in, plan, plan.Instances[0],
		f.targetCR, f.targetSpec, f.survivingPods(t))
	if err != nil {
		t.Fatalf("UpdateWithPods: %v (a classified rejection is disposed, not returned)", err)
	}
	if done {
		t.Fatal("a refused create must not complete the cycle")
	}

	s := legacyInstanceStatusesOnIR(base, f.isvc, workload.ComponentEngine)[0]
	switch {
	case tc.wantFailed:
		if s.Phase != v1beta1.OMENativeInstanceFailed {
			t.Fatalf("Phase: got %q want Failed (the rejection is permanent)", s.Phase)
		}
		if s.Operation != nil {
			t.Errorf("Operation: got %+v want cleared with the disposal", s.Operation)
		}
		if s.LastFailure == nil || s.LastFailure.Reason != tc.wantReason {
			t.Errorf("LastFailure: got %+v want reason %s", s.LastFailure, tc.wantReason)
		}
	default:
		if s.Phase != v1beta1.OMENativeInstanceUpdating {
			t.Errorf("Phase: got %q want Updating (the cycle is still in flight)", s.Phase)
		}
		capacityRefused := s.Operation != nil && workload.OperationCapacityRefused(legacyFromV1beta1Op(s.Operation))
		if capacityRefused != tc.wantWait {
			t.Errorf("Operation: got %+v want capacity-refused=%v", s.Operation, tc.wantWait)
		}
	}
	if len(*blocks) != tc.wantBlocks {
		t.Errorf("RetryBlock writes: got %v want %d (only a bad pod spec blames the revision)", *blocks, tc.wantBlocks)
	}
	if got := in.Pacing.Pending(); got != tc.wantPacing {
		t.Errorf("pass pacing: got %v want %v", got, tc.wantPacing)
	}
}

// TestGangSurge_ReplacementPodsLostAroundThePromote: the gang handoff is
// one write that retires the source row and makes the replacement index
// the Instance, and it is preconditioned on the replacement still being
// the whole gang it claimed to be. A replacement whose pods vanished
// before that write is therefore not promoted — the pass rebuilds the
// missing members and the source row stays exactly where it was. Once
// the write has landed the question belongs to the other row: the source
// is gone, the replacement carries no operation, and nothing in the
// surge machine is left to claim a pod it loses.
func TestGangSurge_ReplacementPodsLostAroundThePromote(t *testing.T) {
	const isvcName, ns, targetRev = "gang-handoff", "test-ns", "gang-handoff-engine-newrev"
	surgeIdx := int32(2)

	// build returns the drain-step pair and the client holding whatever
	// replacement pods the caller asked for. The source has no pods left,
	// so the promote is the only work the pass has.
	build := func(t *testing.T, withPods bool) (workload.ReconcileInput, *workload.InstanceStatus, *[]int32, workload.ComponentPlan, *appsv1.ControllerRevision, client.Client) {
		t.Helper()
		legacyResetExpectations(t)
		var objs []client.Object
		if withPods {
			for _, runner := range []string{"leader", "worker"} {
				pod := gangSurgePod(isvcName, ns, surgeIdx, runner, "newrev")
				pod.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
					{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
				}
				objs = append(objs, pod)
			}
		}
		c := legacyNewFakeClient(t, objs...)
		src := &workload.InstanceStatus{
			Index:           0,
			Phase:           workload.InstancePhaseUpdating,
			RunningRevision: "gang-handoff-engine-oldrev",
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepSurgeDrain,
				SurgeIndex:     &surgeIdx,
				TargetRevision: targetRev,
			},
		}
		removed := &[]int32{}
		in := gangAbandonInput(isvcName, ns, src, removed)
		isvc := legacyMinimalISVC(isvcName, ns, 1)
		in.EventTarget, in.OwnerObject = isvc, isvc
		in.OwnerGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")
		in.DesiredSpec = workload.WorkloadDesiredSpec{
			PodSpec:       legacyTargetSpecImage("example.com/app:v2"),
			WorkerPodSpec: legacyTargetSpecImage("example.com/app:v2"),
		}
		in.ObservedState.InstanceStatuses = []workload.InstanceStatus{*src, {
			Index:          surgeIdx,
			Incarnation:    1,
			Phase:          workload.InstancePhaseCreating,
			TargetRevision: targetRev,
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepGangSurgeTarget,
				TargetRevision: targetRev,
			},
		}}
		return in, src, removed, legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain),
			&appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRev}}, c
	}

	t.Run("the replacement's pods vanish before the write", func(t *testing.T) {
		in, src, removed, plan, target, c := build(t, false /* no replacement pods */)

		done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), in, plan, plan.Instances[0], target)
		if err != nil {
			t.Fatalf("gangSurgeUpdate: %v", err)
		}
		if done {
			t.Fatalf("the handoff must not complete with the replacement gang missing")
		}
		if len(*removed) != 0 {
			t.Errorf("RemoveInstance: got %v want none (the source row stays)", *removed)
		}
		if src.Operation == nil || src.Operation.Step != workload.UpdateStepSurgeDrain {
			t.Errorf("source Operation: got %+v want the drain still open at %s", src.Operation, workload.UpdateStepSurgeDrain)
		}
		if src.Phase != workload.InstancePhaseUpdating {
			t.Errorf("source Phase: got %q want Updating", src.Phase)
		}
	})

	t.Run("the write has landed", func(t *testing.T) {
		in, src, removed, plan, target, c := build(t, true /* replacement gang serving */)

		done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), in, plan, plan.Instances[0], target)
		if err != nil {
			t.Fatalf("gangSurgeUpdate: %v", err)
		}
		if !done {
			t.Fatalf("the handoff must complete with the replacement gang serving: src=%+v", src)
		}
		if len(*removed) != 1 || (*removed)[0] != 0 {
			t.Fatalf("RemoveInstance: got %v want the source row retired", *removed)
		}
		// Nothing of the surge is left on the replacement index, so a pod
		// it loses from here is the create and restart passes' business.
		for _, s := range in.ObservedState.InstanceStatuses {
			if s.Index != surgeIdx {
				continue
			}
			if s.Operation != nil && s.Operation.SurgeIndex != nil {
				t.Errorf("replacement row: got %+v want no surge pin left to claim a lost pod", s.Operation)
			}
		}
	})
}

type failedCreateContainerSurgeFixture struct {
	isvc         *v1beta1.InferenceService
	client       client.Client
	recording    *gracefulDeleteRecordingClient
	expectations *workload.Expectations
	input        workload.ReconcileInput
	plan         workload.ComponentPlan
	target       *appsv1.ControllerRevision
	source       *corev1.Pod
	failed       *corev1.Pod
}

func newFailedCreateContainerSurgeFixture(t *testing.T, excludedNodes []string) failedCreateContainerSurgeFixture {
	t.Helper()
	legacyResetExpectations(t)

	isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
	sourceRevision := "llama-70b-engine-rev-sourcehash"
	targetRevision := "llama-70b-engine-rev-targethash"
	failedName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 1)
	ir := legacyInstanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceFailed,
		RunningRevision: sourceRevision,
		TargetRevision:  targetRevision,
		ActiveOrdinal:   0,
		LastFailure: &v1beta1.InstanceTermination{
			PodName: failedName,
			Reason:  createContainerErrorReason,
		},
	})

	source := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	source.UID = k8stypes.UID("source-uid")
	source.Spec.NodeName = "node-source"
	source.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(sourceRevision)

	failed := surgePodAtOrdinal(isvc, 0, 1, 1, false, false)
	failed.UID = k8stypes.UID("failed-target-uid")
	failed.Spec.NodeName = "node-target"
	failed.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(targetRevision)
	failed.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason:  createContainerErrorReason,
			Message: "runtime mount unavailable",
		}},
	}}

	base := legacyNewFakeClient(t, isvc, ir, source, failed)
	recording := &gracefulDeleteRecordingClient{Client: base}
	target := makeCR(t, recording, isvc, targetRevision)
	input := legacyTestInput(isvc, recording, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = targetRevision
	input.ObservedState.InstanceStatuses[0].LastFailure = &workload.InstanceTermination{
		PodName: failedName,
		Reason:  createContainerErrorReason,
	}
	plan := surgePlan()
	plan.Instances[0].ExcludedNodes = append([]string(nil), excludedNodes...)

	return failedCreateContainerSurgeFixture{
		isvc:         isvc,
		client:       base,
		recording:    recording,
		expectations: workload.NewExpectations(),
		input:        input,
		plan:         plan,
		target:       target,
		source:       source,
		failed:       failed,
	}
}

func (f failedCreateContainerSurgeFixture) deps() workload.Deps {
	return workload.Deps{Client: f.recording, Expectations: f.expectations}
}

func TestSurgeUpdate_RecyclesFailedCreateContainerTargetThenRetries(t *testing.T) {
	f := newFailedCreateContainerSurgeFixture(t, []string{"node-target"})

	done, err := surgeUpdate(context.Background(), f.deps(), f.input, f.plan, f.plan.Instances[0], f.target,
		[]*corev1.Pod{f.source, f.failed})
	if err != nil {
		t.Fatalf("recycle failed target: %v", err)
	}
	if done {
		t.Fatal("failed-target cleanup must not report rollout completion")
	}
	if f.recording.deleteCalls != 1 || len(f.recording.deleteUIDs) != 1 ||
		f.recording.deleteUIDs[0] == nil || *f.recording.deleteUIDs[0] != f.failed.UID {
		t.Fatalf("delete calls/UIDs = %d/%v, want one delete preconditioned on %q",
			f.recording.deleteCalls, f.recording.deleteUIDs, f.failed.UID)
	}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.failed), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed target still exists after recycle: %v", err)
	}
	storedSource := &corev1.Pod{}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.source), storedSource); err != nil {
		t.Fatalf("serving source was deleted: %v", err)
	}
	if !podreadiness.IsServing(storedSource) {
		t.Fatal("serving source left rotation during failed-target cleanup")
	}
	status := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if status.Phase != v1beta1.OMENativeInstanceFailed || status.Operation != nil {
		t.Fatalf("status after delete = Phase %q Operation %+v, want Failed with no operation until deletion is observed",
			status.Phase, status.Operation)
	}

	// Simulate the delete watch observation. The next pass must create a fresh
	// target in the same ordinal with the recorded node exclusion applied.
	f.expectations.ObservedDelete("prod", "llama-70b", workload.ComponentEngine, 0)
	input := legacyTestInput(f.isvc, f.recording, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = f.target.Name
	input.ObservedState.InstanceStatuses[0].LastFailure = &workload.InstanceTermination{
		PodName: f.failed.Name,
		Reason:  createContainerErrorReason,
	}
	if _, err := surgeUpdate(context.Background(), f.deps(), input, f.plan, f.plan.Instances[0], f.target,
		[]*corev1.Pod{storedSource}); err != nil {
		t.Fatalf("start replacement attempt: %v", err)
	}
	replacement := &corev1.Pod{}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.failed), replacement); err != nil {
		t.Fatalf("fresh target was not created: %v", err)
	}
	if got := hostnameNotInValues(replacement); len(got) != 1 || got[0] != "node-target" {
		t.Fatalf("replacement hostname exclusions = %v, want [node-target]", got)
	}
	if !podreadiness.IsServing(storedSource) {
		t.Fatal("serving source changed while replacement was created")
	}
}

func TestSurgeUpdate_FailedCreateContainerTargetParksWithoutRelocationAuthorization(t *testing.T) {
	f := newFailedCreateContainerSurgeFixture(t, []string{"previous-node-a", "previous-node-b"})

	if _, err := surgeUpdate(context.Background(), f.deps(), f.input, f.plan, f.plan.Instances[0], f.target,
		[]*corev1.Pod{f.source, f.failed}); err != nil {
		t.Fatalf("park failed target: %v", err)
	}
	if f.recording.deleteCalls != 0 {
		t.Fatalf("delete calls = %d, want zero without a matching relocation directive", f.recording.deleteCalls)
	}
	storedTarget := &corev1.Pod{}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.failed), storedTarget); err != nil {
		t.Fatalf("failed target was removed without authorization: %v", err)
	}
	status := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if status.Phase != v1beta1.OMENativeInstanceFailed || status.Operation != nil {
		t.Fatalf("parked status = Phase %q Operation %+v, want Failed with no operation", status.Phase, status.Operation)
	}
}

func TestSurgeUpdate_FailedCreateContainerTargetRequiresServingSource(t *testing.T) {
	f := newFailedCreateContainerSurgeFixture(t, []string{"node-target"})
	liveSource := &corev1.Pod{}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.source), liveSource); err != nil {
		t.Fatalf("get source: %v", err)
	}
	for i := range liveSource.Status.Conditions {
		if liveSource.Status.Conditions[i].Type == query.ServingConditionType {
			liveSource.Status.Conditions[i].Status = corev1.ConditionFalse
		}
	}
	if err := f.client.Status().Update(context.Background(), liveSource); err != nil {
		t.Fatalf("mark source non-serving: %v", err)
	}

	if _, err := surgeUpdate(context.Background(), f.deps(), f.input, f.plan, f.plan.Instances[0], f.target,
		[]*corev1.Pod{liveSource, f.failed}); err != nil {
		t.Fatalf("park without a serving source: %v", err)
	}
	if f.recording.deleteCalls != 0 {
		t.Fatalf("delete calls = %d, want zero while the source is not serving", f.recording.deleteCalls)
	}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.failed), &corev1.Pod{}); err != nil {
		t.Fatalf("failed target was removed without a serving source: %v", err)
	}
}

// Tests for a mid-rollout UpdateStrategy.Type change against an Instance
// that is already inside the SurgeThenDrain state machine.
//
// UpdateStrategy is not part of the ControllerRevision payload, so editing
// it does not retarget the roll — and the strategy is pinned on the
// operation, so an attempt already under way keeps the mechanism it opened
// with. A mode that took over mid-flight would have no knowledge of the
// pods, ordinal slot, or serving-gate holds the surge left behind.
//
// Each test runs the identical fixture under the retained strategy (the
// control) and under each flip, and expects the same outcome from both.

// midSurgeFixture is one Instance inside a SurgeThenDrain roll from
// runningImage to targetImage: a source pod at ordinal 0 and a replacement
// at ordinal 1, with the Instance's Operation parked at step.
type midSurgeFixture struct {
	client     client.Client
	isvc       *v1beta1.InferenceService
	targetSpec *corev1.PodSpec
	targetCR   *appsv1.ControllerRevision
	sourcePod  *corev1.Pod
	surgePod   *corev1.Pod
}

// newMidSurgeFixture builds the shared mid-surge state. surgeReady controls
// whether the replacement has reached ContainersReady + PodReady and taken
// over serving; sourceServing controls whether the source is still in
// rotation (false models a source the surge already drained).
func newMidSurgeFixture(t *testing.T, step string, surgeReady, sourceServing bool) *midSurgeFixture {
	t.Helper()
	legacyResetExpectations(t)

	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	runningSpec := legacyTargetSpecImage("test:v1")
	targetSpec := legacyTargetSpecImage("test:v2")

	sourcePod := surgePodAtOrdinal(isvc, 0, 1, 0, true /* ready */, sourceServing)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, surgeReady, surgeReady)
	if surgeReady {
		surgePod.Status.Conditions = append(surgePod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		})
	}

	c := legacyNewFakeClient(t, isvc, ir, sourcePod, surgePod)
	runningCR := legacyEnsureTargetCR(t, c, isvc, runningSpec)
	targetCR := legacyEnsureTargetCR(t, c, isvc, targetSpec)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, runningSpec)

	// The source carries the running revision's hash and the replacement the
	// target's, exactly as createMissingPods stamps them. reclassifyByRevisionHash
	// and inPlaceUpdate's on-target skip both key on this label.
	legacyStampPodRevisionHash(t, c, sourcePod, runningCR.Name)
	legacyStampPodRevisionHash(t, c, surgePod, targetCR.Name)
	sourcePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(runningCR.Name)
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(targetCR.Name)

	seedInFlightUpdateOperation(t, c, isvc, 0, step, targetCR.Name)
	if sourceServing != podreadiness.IsServing(sourcePod) {
		t.Fatalf("fixture: source serving gate is %v, want %v", podreadiness.IsServing(sourcePod), sourceServing)
	}

	return &midSurgeFixture{
		client:     c,
		isvc:       isvc,
		targetSpec: targetSpec,
		targetCR:   targetCR,
		sourcePod:  sourcePod,
		surgePod:   surgePod,
	}
}

// run drives one UpdateWithPods pass under strategy, returning the pass's
// done flag. The ReconcileInput is rebuilt per pass because its ObservedState
// is a snapshot — production re-reads it every reconcile.
func (f *midSurgeFixture) run(t *testing.T, strategy workload.UpdateStrategyType) bool {
	t.Helper()
	plan := legacyComponentPlan(strategy, nil)
	input := legacyTestInput(f.isvc, f.client, workload.ComponentEngine)
	done, err := UpdateWithPods(context.Background(), legacyTestDeps(f.client), input, plan,
		plan.Instances[0], f.targetCR, f.targetSpec, f.survivingPods(t))
	if err != nil {
		t.Fatalf("UpdateWithPods (strategy=%s): %v", strategy, err)
	}
	return done
}

// survivingPods re-reads the Instance's pods, dropping any the previous pass
// deleted. The dispatcher hands the update op a freshly listed pod set each
// reconcile; a stale slice would keep feeding it a pod that no longer exists.
func (f *midSurgeFixture) survivingPods(t *testing.T) []*corev1.Pod {
	t.Helper()
	out := make([]*corev1.Pod, 0, 2)
	for _, pod := range []*corev1.Pod{f.sourcePod, f.surgePod} {
		if fresh, found := f.livePod(t, pod); found {
			out = append(out, fresh)
		}
	}
	return out
}

// livePod re-reads pod from the fake client. found=false means it was deleted.
func (f *midSurgeFixture) livePod(t *testing.T, pod *corev1.Pod) (*corev1.Pod, bool) {
	t.Helper()
	fresh := &corev1.Pod{}
	err := f.client.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("re-read pod %s: %v", pod.Name, err)
	}
	return fresh, true
}

// seedInFlightUpdateOperation writes an in-flight Update operation onto the
// InferenceReplica so a ReconcileInput built afterwards observes the state a
// prior reconcile would have left.
func seedInFlightUpdateOperation(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, idx int32, step, targetRev string) {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index != idx {
			continue
		}
		ir.Status.InstanceStatuses[i].Phase = v1beta1.OMENativeInstanceUpdating
		ir.Status.InstanceStatuses[i].TargetRevision = targetRev
		ir.Status.InstanceStatuses[i].Operation = &v1beta1.InstanceOperation{
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: targetRev,
			Strategy:       string(v1beta1.UpdateStrategySurgeThenDrain),
			StartedAt:      metav1.Now(),
			LastProgressAt: metav1.Now(),
		}
		if err := c.Status().Update(context.Background(), ir); err != nil {
			t.Fatalf("seed in-flight operation: %v", err)
		}
		return
	}
	t.Fatalf("no InstanceStatus for idx=%d", idx)
}

// TestUpdateWithPods_StrategyFlipAtStepSurge_HoldsSourceInRotation pins the
// no-downtime invariant across a mid-roll strategy change.
//
// The Instance has surged a replacement that is not yet Ready, so its source
// is the only pod carrying traffic. SurgeThenDrain holds that source in
// rotation until the replacement reports Ready. Flipping the strategy does
// not change the fact that the replacement is unready, so the source must
// still hold — the surge capacity was already spent, and dropping the source
// now takes the Instance to zero serving pods for the whole time the
// replacement needs to come up.
func TestUpdateWithPods_StrategyFlipAtStepSurge_HoldsSourceInRotation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy workload.UpdateStrategyType
	}{
		{"SurgeThenDrain retained", workload.UpdateStrategySurgeThenDrain},
		{"flipped to InPlaceIfPossible", workload.UpdateStrategyInPlaceIfPossible},
		{"flipped to InPlaceOnly", workload.UpdateStrategyInPlaceOnly},
		{"flipped to RecreatePod", workload.UpdateStrategyRecreatePod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurge,
				false /* surgeReady */, true /* sourceServing */)

			if done := f.run(t, tc.strategy); done {
				t.Fatalf("expected done=false: the replacement is not Ready yet")
			}

			source, found := f.livePod(t, f.sourcePod)
			if !found {
				t.Fatalf("source pod was deleted while the replacement was still unready")
			}
			if !podreadiness.IsServing(source) {
				t.Errorf("source left rotation while the replacement was still unready: %+v",
					source.Status.Conditions)
			}
		})
	}
}

// syncFakeKubelet advances every pod's runtime container status to the images
// currently in its spec — the step the in-place path's runtime-image proof
// waits on — and folds a satisfied serving gate into PodReady, which is what
// the shared promote bar reads.
func (f *midSurgeFixture) syncFakeKubelet(t *testing.T) {
	t.Helper()
	for _, pod := range []*corev1.Pod{f.sourcePod, f.surgePod} {
		fresh, found := f.livePod(t, pod)
		if !found {
			continue
		}
		statuses := make([]corev1.ContainerStatus, 0, len(fresh.Spec.Containers))
		for _, container := range fresh.Spec.Containers {
			statuses = append(statuses, corev1.ContainerStatus{
				Name:        container.Name,
				Image:       container.Image,
				ContainerID: "containerd://" + container.Image,
			})
		}
		fresh.Status.ContainerStatuses = statuses
		if podreadiness.IsServing(fresh) && !podreadiness.IsPodReady(fresh) {
			fresh.Status.Conditions = append(fresh.Status.Conditions, corev1.PodCondition{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
			})
		}
		if err := f.client.Status().Update(context.Background(), fresh); err != nil {
			t.Fatalf("advance runtime status for %s: %v", fresh.Name, err)
		}
	}
}

// instancePodCount counts the Instance's surviving pods across both ordinal
// slots.
func (f *midSurgeFixture) instancePodCount(t *testing.T) int {
	t.Helper()
	n := 0
	for _, pod := range []*corev1.Pod{f.sourcePod, f.surgePod} {
		if _, found := f.livePod(t, pod); found {
			n++
		}
	}
	return n
}

// TestUpdateWithPods_StrategyFlipAtStepSurge_ConvergesToOnePod pins the
// steady-state shape a single-pod Instance must reach.
//
// SurgeThenDrain runs two pods only inside its surge window; the promote that
// ends the roll deletes the source and advances ActiveOrdinal onto the
// replacement's slot. A mid-roll flip hands the Instance to a mode that
// reuses one pod name and never learned the other slot exists, so its
// promote leaves both pods alive on the target revision. Neither is an alien
// revision, so the superseded-wreckage sweep does not reclaim them either:
// the Instance settles permanently at double its accelerator footprint.
func TestUpdateWithPods_StrategyFlipAtStepSurge_ConvergesToOnePod(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy workload.UpdateStrategyType
	}{
		{"SurgeThenDrain retained", workload.UpdateStrategySurgeThenDrain},
		{"flipped to InPlaceIfPossible", workload.UpdateStrategyInPlaceIfPossible},
		{"flipped to InPlaceOnly", workload.UpdateStrategyInPlaceOnly},
		{"flipped to RecreatePod", workload.UpdateStrategyRecreatePod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurge,
				true /* surgeReady */, true /* sourceServing */)

			const maxPasses = 12
			var done bool
			for pass := 0; pass < maxPasses && !done; pass++ {
				f.syncFakeKubelet(t)
				done = f.run(t, tc.strategy)
			}
			if !done {
				t.Fatalf("rollout did not converge in %d passes", maxPasses)
			}

			if got := f.instancePodCount(t); got != 1 {
				t.Errorf("Instance settled with %d pods; a single-pod Instance must converge to 1", got)
			}
		})
	}
}

// TestUpdateWithPods_StrategyFlipAtUncommittedSurge_FinishesOnItsPin pins
// WHEN the edit takes effect: at the Instance's next attempt, not this one.
//
// The replacement has not come up yet, so the surge looks uncommitted — but
// its capacity is already spent, its ordinal slot already taken, and the
// source is held in rotation by the surge's own writer key. Unwinding all of
// that mid-flight is how a mode that never learned the slot exists ends up
// stranding a pod. The attempt therefore finishes on SurgeThenDrain, and the
// edited strategy is what the Instance's next roll resolves against.
func TestUpdateWithPods_StrategyFlipAtUncommittedSurge_FinishesOnItsPin(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurge,
		false /* surgeReady */, true /* sourceServing */)

	for pass := 0; pass < 3; pass++ {
		f.syncFakeKubelet(t)
		if done := f.run(t, workload.UpdateStrategyInPlaceIfPossible); done {
			t.Fatalf("the roll reported done while its replacement was still unready")
		}
	}
	if _, found := f.livePod(t, f.surgePod); !found {
		t.Fatalf("the replacement was reclaimed: the edit unwound capacity the surge had already spent")
	}
	held := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if held.Operation == nil || !status.SurgeUpdateStep(held.Operation.Step) {
		t.Fatalf("Operation = %+v, want the surge still in control", held.Operation)
	}
	if held.Operation.Strategy != string(v1beta1.UpdateStrategySurgeThenDrain) {
		t.Errorf("Operation.Strategy = %q, want the pin the attempt opened with", held.Operation.Strategy)
	}

	// The replacement comes up and the cycle completes on its own mechanism.
	f.markSurgePodReady(t)
	const maxPasses = 12
	var done bool
	for pass := 0; pass < maxPasses && !done; pass++ {
		f.syncFakeKubelet(t)
		done = f.run(t, workload.UpdateStrategyInPlaceIfPossible)
	}
	if !done {
		t.Fatalf("the surge did not finish its cycle in %d passes", maxPasses)
	}
	if _, found := f.livePod(t, f.sourcePod); found {
		t.Errorf("the drained source survived: the surge cycle did not complete")
	}

	row := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if row.Phase != v1beta1.OMENativeInstanceReady || row.RunningRevision != f.targetCR.Name {
		t.Errorf("final status = {phase:%s running:%s}, want {phase:Ready running:%s}",
			row.Phase, row.RunningRevision, f.targetCR.Name)
	}
	// With the attempt over, nothing pins a strategy any more: the next roll
	// resolves against the edited one.
	next := effectiveUpdateStrategy(&workload.InstanceStatus{
		Phase:     workload.InstancePhase(row.Phase),
		Operation: legacyFromV1beta1Op(row.Operation),
	}, workload.UpdateStrategyInPlaceIfPossible)
	if next != workload.UpdateStrategyInPlaceIfPossible {
		t.Errorf("next attempt resolves to %q, want the edited InPlaceIfPossible", next)
	}
}

// markSurgePodReady advances the replacement to ContainersReady + PodReady,
// the state the promote bar reads.
func (f *midSurgeFixture) markSurgePodReady(t *testing.T) {
	t.Helper()
	fresh, found := f.livePod(t, f.surgePod)
	if !found {
		t.Fatalf("replacement pod is gone")
	}
	for _, condition := range []corev1.PodConditionType{corev1.ContainersReady, corev1.PodReady} {
		fresh.Status.Conditions = append(fresh.Status.Conditions, corev1.PodCondition{
			Type:               condition,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		})
	}
	if err := f.client.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("mark replacement ready: %v", err)
	}
}

// TestUpdateWithPods_StrategyFlipAtStepSurgeDrain_FinishesSurgeCycle pins the
// point-of-no-return rule the surge state machine states for itself: past
// Step=Surge the source is already draining, so the cycle must run to
// completion (source deleted, ActiveOrdinal advanced onto the replacement's
// slot) rather than be abandoned partway.
//
// A strategy flip at this point hands the Instance to a mode that has no
// concept of the ordinal slot or of the surge-drain hold on the source. The
// hold is only ever released by deleting the source, so a mode that keeps
// the source alive strands it: alive, occupying an accelerator, and
// permanently out of rotation.
func TestUpdateWithPods_StrategyFlipAtStepSurgeDrain_FinishesSurgeCycle(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy workload.UpdateStrategyType
	}{
		{"SurgeThenDrain retained", workload.UpdateStrategySurgeThenDrain},
		{"flipped to InPlaceIfPossible", workload.UpdateStrategyInPlaceIfPossible},
		{"flipped to InPlaceOnly", workload.UpdateStrategyInPlaceOnly},
		{"flipped to RecreatePod", workload.UpdateStrategyRecreatePod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurgeDrain,
				true /* surgeReady */, false /* sourceServing */)
			// The surge holds the drained source out of rotation under its own
			// writer key. Only the source's deletion removes that entry.
			if err := podreadiness.MarkPodNotServing(context.Background(), f.client, f.client,
				f.sourcePod, podreadiness.WriterUpdateSurgeDrain, surgeDrainKey(0, 1)); err != nil {
				t.Fatalf("seed surge drain hold: %v", err)
			}

			f.run(t, tc.strategy)

			if _, found := f.livePod(t, f.sourcePod); found {
				t.Errorf("drained source survived the pass: the surge cycle was abandoned past its point of no return")
			}
			// Deletion alone does not prove the surge finished the cycle —
			// recreate deletes the source too, by tearing the Instance down. The
			// Operation still being on a surge step is what separates "the surge
			// ran to completion" from "another mode took the pod away".
			row := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
			if row.Operation != nil && !status.SurgeUpdateStep(row.Operation.Step) {
				t.Errorf("Operation.Step = %q: the cycle was handed to another mode instead of finishing",
					row.Operation.Step)
			}
		})
	}
}

// TestSurgeUpdate_HoldsSourceDrainForMinReadySeconds: a PodReady surge pod
// that has not yet been Ready for minReadySeconds keeps the source in
// rotation and the Operation at Step=Surge (budget slot held); once the
// window elapses — inclusive at the exact boundary — the same pass drains
// and deletes the source.
func TestSurgeUpdate_HoldsSourceDrainForMinReadySeconds(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b-engine-rev-v2hash", Namespace: isvc.Namespace,
	}}
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	podReadyAt(surgePod, minReadyWindowStart.Add(-10*time.Second))
	c := legacyNewFakeClient(t, isvc, ir, oldPod, surgePod, target)

	clk := clocktesting.NewFakeClock(minReadyWindowStart)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clk
	plan := surgePlan()
	plan.MinReadySeconds = 20
	if err := status.StampSurging(context.Background(), input, 0, target.Name, workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp: %v", err)
	}

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate inside window: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while the replacement is inside the minReadySeconds window")
	}
	freshOld := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld); err != nil {
		t.Fatalf("source missing inside the window: %v", err)
	}
	if !podreadiness.IsServing(freshOld) {
		t.Fatalf("source left rotation before the replacement was Available")
	}
	row := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if row.Operation == nil || row.Operation.Step != workload.UpdateStepSurge {
		t.Fatalf("operation advanced past Surge inside the window: %+v", row.Operation)
	}

	// Exactly readyAt + 20s: the boundary counts as Available.
	clk.SetTime(minReadyWindowStart.Add(10 * time.Second))
	freshSurge := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(surgePod), freshSurge); err != nil {
		t.Fatalf("get replacement: %v", err)
	}
	done, err = surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, []*corev1.Pod{freshOld, freshSurge})
	if err != nil {
		t.Fatalf("surgeUpdate after window: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while deleting source")
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("source was not deleted once the replacement became Available: %v", err)
	}
}

// TestSurgeUpdate_WindowGatesOnlyStepSurge: the window decides when the
// source may leave rotation, so it applies at Step=Surge only. Once the
// source is marked not-serving (Step=SurgeDrain), a replacement that is
// Ready but inside the window — as after a readiness flap — must not hold
// the drained source out of service; the pass proceeds to delete it.
func TestSurgeUpdate_WindowGatesOnlyStepSurge(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, false /* already drained */)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b-engine-rev-v2hash", Namespace: isvc.Namespace,
	}}
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	podReadyAt(surgePod, minReadyWindowStart.Add(-10*time.Second))
	c := legacyNewFakeClient(t, isvc, ir, oldPod, surgePod, target)

	clk := clocktesting.NewFakeClock(minReadyWindowStart)
	plan := surgePlan()
	plan.MinReadySeconds = 20
	stamp := legacyTestInput(isvc, c, workload.ComponentEngine)
	stamp.Clock = clk
	if err := status.StampSurging(context.Background(), stamp, 0, target.Name, workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	if err := status.StampSurgeDrainStep(context.Background(), stamp, 0); err != nil {
		t.Fatalf("pre-stamp drain: %v", err)
	}
	// Observe the persisted Step=SurgeDrain, as a later pass would.
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clk

	done, err := surgeUpdate(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate past Step=Surge: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while deleting source")
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("drained source must be deleted without waiting out the window again: %v", err)
	}
}

// A pause holds an attempt at a STEP boundary: the step under way runs
// to its end, the next one does not start. The surge machine is where
// that distinction is visible — its cycle crosses three steps and each
// boundary leaves a different pair of pods behind.

// pausedSurgeFixture builds the single-Instance surge at the point where
// the replacement has reached the promote bar and the source is still
// serving: the boundary between the surge step and the drain.
func pausedSurgeFixture(t *testing.T) (*v1beta1.InferenceService, client.Client, workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *corev1.Pod, *corev1.Pod) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b-engine-rev-v2hash", Namespace: isvc.Namespace,
	}}
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	podReadyAt(surgePod, minReadyWindowStart.Add(-time.Minute))
	c := legacyNewFakeClient(t, isvc, ir, oldPod, surgePod, target)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(minReadyWindowStart)
	plan := surgePlan()
	if err := status.StampSurging(context.Background(), input, 0, target.Name,
		workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	return isvc, c, input, plan, target, oldPod, surgePod
}

// TestSurgeUpdate_PausedHoldsAtTheSurgeStepBoundary: the replacement is
// created and reaches the promote bar under a pause — that is the step
// already under way — but the drain is a new step and does not start, so
// the source keeps serving for the length of the hold. Clearing the
// pause runs the drain.
func TestSurgeUpdate_PausedHoldsAtTheSurgeStepBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		freeze bool
	}{{name: "standard pause"}, {name: "frozen pause", freeze: true}} {
		t.Run(tc.name, func(t *testing.T) {
			isvc, c, input, plan, target, oldPod, surgePod := pausedSurgeFixture(t)
			plan.Paused = true
			plan.PauseFreeze = tc.freeze

			done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
				plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod})
			if err != nil {
				t.Fatalf("surgeUpdate paused: %v", err)
			}
			if done {
				t.Fatalf("a held surge must not report the roll complete")
			}
			freshOld := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld); err != nil {
				t.Fatalf("source missing under pause: %v", err)
			}
			if !podreadiness.IsServing(freshOld) {
				t.Errorf("the source left rotation while paused; a pause may not cost the Instance capacity")
			}
			row := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if row.Operation == nil || row.Operation.Step != workload.UpdateStepSurge {
				t.Errorf("step under pause: got %+v want %s (the drain is a new step)", row.Operation, workload.UpdateStepSurge)
			}

			// The replacement still had to be finished: it is serving and
			// holds the surge ordinal.
			freshSurge := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(surgePod), freshSurge); err != nil {
				t.Fatalf("replacement missing under pause: %v", err)
			}
			if !podreadiness.IsServing(freshSurge) {
				t.Errorf("the replacement never joined rotation; the surge step did not run to its boundary")
			}

			plan.Paused, plan.PauseFreeze = false, false
			if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
				plan.Instances[0], target, []*corev1.Pod{freshOld, freshSurge}); err != nil {
				t.Fatalf("surgeUpdate unpaused: %v", err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Errorf("the drain did not resume on unpause: %v", err)
			}
		})
	}
}

// TestSurgeUpdate_PausedDrainDeletesAndPromotes: a pause taken while the
// drain step is under way does not freeze the operation in flight — it
// starts no NEW one. The drain runs to completion: the source leaves
// rotation, is deleted, and the row promotes onto the replacement's
// slot, which served throughout.
func TestSurgeUpdate_PausedDrainDeletesAndPromotes(t *testing.T) {
	isvc, c, input, plan, target, oldPod, surgePod := pausedSurgeFixture(t)
	if err := status.StampSurgeDrainStep(context.Background(), input, 0); err != nil {
		t.Fatalf("pre-stamp drain: %v", err)
	}
	// The observation this pass reads is the persisted row of the pass
	// that stamped the drain.
	input.ObservedState.InstanceStatuses[0].Phase = workload.InstancePhaseUpdating
	input.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepSurgeDrain, TargetRevision: target.Name,
	}
	plan.Paused = true

	if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
		plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod}); err != nil {
		t.Fatalf("surgeUpdate paused at drain: %v", err)
	}

	freshOld := &corev1.Pod{}
	err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld)
	if err == nil && freshOld.DeletionTimestamp == nil {
		t.Fatalf("the drain must delete its source under a pause too; got %+v", freshOld.ObjectMeta)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("read source: %v", err)
	}
	if podreadiness.IsServing(freshOld) {
		t.Errorf("the source left rotation before the delete")
	}

	// The promote lands on the pass that observes the source gone.
	input.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepSurgeDrain, TargetRevision: target.Name,
	}
	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
		plan.Instances[0], target, []*corev1.Pod{surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate promote pass: %v", err)
	}
	if !done {
		t.Fatalf("the promote must complete the roll once the source is gone")
	}
	row := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if row.Phase != v1beta1.OMENativeInstanceReady || row.ActiveOrdinal != 1 {
		t.Fatalf("promote under pause: got phase=%s activeOrdinal=%d want Ready on ordinal 1",
			row.Phase, row.ActiveOrdinal)
	}
}

// quietSurgeTarget binds the fixture's replacement to node and turns it
// into a pod the kubelet has stopped reporting: phase Unknown, no
// conditions. The node is never seeded, so node-death evidence reads it
// as gone.
func quietSurgeTarget(t *testing.T, f *midSurgeFixture, node string) {
	t.Helper()
	live, alive := f.livePod(t, f.surgePod)
	if !alive {
		t.Fatalf("fixture: replacement %s is gone", f.surgePod.Name)
	}
	live.Spec.NodeName = node
	if err := f.client.Update(context.Background(), live); err != nil {
		t.Fatalf("bind replacement %s to %s: %v", live.Name, node, err)
	}
	patchPodStatus(t, f, f.surgePod, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodUnknown
		pod.Status.Conditions = nil
	})
}

// withForceDeletePolicy is the operator's force-delete policy as a
// runSurgePass tweak.
func withForceDeletePolicy(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
	in.ForceDelete = &workload.ForceDeletePolicy{OverdueSlack: 2 * time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
}

// TestSurgeUpdate_UnknownTargetSweptOnProvenNodeDeath: a replacement whose
// kubelet has stopped reporting can never clear the promote bar, and its
// name is not recycled the way a dead pod's is, because a returning
// kubelet may still be running it. With a force-delete policy configured
// and its node provably gone, the sweep frees the surge ordinal on the
// pass that reads it and the same attempt rebuilds the replacement there;
// the source keeps serving throughout and no step moves.
func TestSurgeUpdate_UnknownTargetSweptOnProvenNodeDeath(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurge, false, true)
	quietSurgeTarget(t, f, "node-gone")

	got := runSurgePass(t, f, withForceDeletePolicy)

	if got.done || got.phase != v1beta1.OMENativeInstanceUpdating || got.step != workload.UpdateStepSurge {
		t.Errorf("pass 1: got done=%v phase=%s step=%s want the surge still at Surge", got.done, got.phase, got.step)
	}
	if got.targetAlive {
		t.Fatalf("the replacement must be force-deleted once its node is provably gone")
	}
	if !got.sourceAlive || !got.sourceServing {
		t.Fatalf("the source must stay in rotation: alive=%v serving=%v", got.sourceAlive, got.sourceServing)
	}
	if got.failed || got.blocks != 0 {
		t.Errorf("the sweep is not a failure: lastFailure=%v retryBlocks=%d", got.failed, got.blocks)
	}

	got = runSurgePass(t, f, withForceDeletePolicy)

	if !got.targetAlive {
		t.Fatalf("the same attempt must rebuild the replacement at the surge ordinal")
	}
	rebuilt, _ := f.livePod(t, f.surgePod)
	if rebuilt.Status.Phase == corev1.PodUnknown {
		t.Fatalf("the surge ordinal still holds the quiet pod, not a fresh one")
	}
	if ord, ok := query.PodOrdinalFromLabels(rebuilt); !ok || ord != 1 {
		t.Errorf("rebuilt ordinal: got %d (%v) want 1", ord, ok)
	}
	if got.step != workload.UpdateStepSurge || got.activeOrdinal != 0 || !got.sourceServing {
		t.Errorf("pass 2: got step=%s activeOrdinal=%d sourceServing=%v want the source untouched at Surge",
			got.step, got.activeOrdinal, got.sourceServing)
	}
}

// TestSurgeUpdate_UnknownTargetHeldWithoutForceDeletePolicy: with no
// force-delete policy nothing can prove the node dead, so the quiet
// replacement is left where it is and the surge neither creates over its
// name nor moves; the wait it reports is the hold pass's.
func TestSurgeUpdate_UnknownTargetHeldWithoutForceDeletePolicy(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurge, false, true)
	quietSurgeTarget(t, f, "node-gone")

	got := runSurgePass(t, f, nil)

	if got.done || got.step != workload.UpdateStepSurge || got.activeOrdinal != 0 {
		t.Errorf("got done=%v step=%s activeOrdinal=%d want the surge held at Surge", got.done, got.step, got.activeOrdinal)
	}
	if !got.targetAlive {
		t.Fatalf("the replacement must not be deleted without a force-delete policy")
	}
	if live, _ := f.livePod(t, f.surgePod); live.Status.Phase != corev1.PodUnknown {
		t.Fatalf("the quiet replacement was replaced: phase=%s", live.Status.Phase)
	}
	if !got.sourceAlive || !got.sourceServing {
		t.Fatalf("the source must stay in rotation: alive=%v serving=%v", got.sourceAlive, got.sourceServing)
	}
	if got.failed || got.blocks != 0 {
		t.Errorf("a held name is not a failure: lastFailure=%v retryBlocks=%d", got.failed, got.blocks)
	}
}
