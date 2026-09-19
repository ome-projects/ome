package ops

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
	zeroGrace := int32(0)
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
			InPlaceUpdateStrategy: &workload.InPlaceUpdateStrategy{
				GracePeriodSeconds: &zeroGrace,
			},
		},
	}
}

func TestWaitForSurgeDrainSettle(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()
	grace := int32(30)
	plan.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds = &grace
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x", plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("stamp surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepDrain(context.Background(), input, 0); err != nil {
		t.Fatalf("stamp drain: %v", err)
	}

	status := &input.ObservedState.InstanceStatuses[0]
	status.Phase = workload.InstancePhaseUpdating
	status.Operation = &workload.InstanceOperation{
		Type:           workload.InstanceOperationUpdate,
		Step:           updateStepSurgeDrain,
		TargetRevision: "rev-x",
		LastProgressAt: metav1.Now(),
	}

	settled, err := waitForSurgeDrainSettle(context.Background(), input, plan, 0)
	if err != nil {
		t.Fatalf("start settle: %v", err)
	}
	if settled {
		t.Fatalf("new settle phase must not allow immediate deletion")
	}
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("read settle status: %v", err)
	}
	insts := legacyInstanceStatusesOnIR(c, fresh, workload.ComponentEngine)
	if len(insts) == 0 {
		t.Fatalf("expected instance 0 on IR")
	}
	got := insts[0].Operation
	if got == nil || got.Step != updateStepSurgeDrainSettle {
		t.Fatalf("step after EndpointSlice drain: got %+v, want %s", got, updateStepSurgeDrainSettle)
	}

	status.Operation.Step = updateStepSurgeDrainSettle
	status.Operation.LastProgressAt = metav1.NewTime(time.Now().Add(-31 * time.Second))
	settled, err = waitForSurgeDrainSettle(context.Background(), input, plan, 0)
	if err != nil {
		t.Fatalf("finish settle: %v", err)
	}
	if !settled {
		t.Fatalf("settle phase should complete after grace period")
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

	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x", 30*time.Second); err != nil {
		t.Fatalf("stamp Surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepDrain(context.Background(), input, 0); err != nil {
		t.Fatalf("stamp SurgeDrain: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	beforeOpID := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0].Operation.ID

	// Re-invoke with the same target — must be a no-op.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x", 30*time.Second); err != nil {
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
	if s.Operation.Step != updateStepSurgeDrain {
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
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x", 30*time.Second); err != nil {
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
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x", 30*time.Second); err != nil {
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
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-good", 30*time.Second); err != nil {
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
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
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

// TestReclassifyByRevisionHash_DrainsStaleByRunningRev pins the X-2
// recovery: a pod labeled with the just-promoted RunningRevision but
// sitting in the surge bucket MUST be moved to drain so Phase 2
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
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, tcr.Name, 1); err != nil {
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
// the multi-instance follow-up. Even when BOTH instances have been
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
		if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, idx, tcr.Name, 1); err != nil {
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
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, tcr.Name, 1); err != nil {
		t.Fatalf("promote 0: %v", err)
	}
	// Build a fresh input from the in-memory ISVC (mirror should have
	// updated it). Promote instance 1.
	input2 := legacyTestInput(isvc, c, workload.ComponentEngine)
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input2, 1, tcr.Name, 1); err != nil {
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
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, rev2.Name, 1); err != nil {
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

// TestSurgeUpdate_StaleRevPodAtSurgeSlot_Drained pins the X-2 follow-up
// invariant for the stale-slot eviction path. When a pod labeled with
// the just-promoted RunningRevision survives at the surge ordinal, the
// rev-hash recheck must route it out of the surge bucket and the
// stale-slot branch must delete it — and ONLY it: the canonical pod at
// the old ordinal keeps serving until the real Phase 2 drain, after
// the correct-rev surge pod is Ready (cleanup never pulls the
// healthy source out of rotation). Phase 3 must NOT promote on the
// stale pod.
func TestSurgeUpdate_StaleRevPodAtSurgeSlot_Drained(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := surgePlan()

	rev2 := makeCR(t, c, isvc, "llama-70b-engine-rev-rev2hash")
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, rev2.Name, 1); err != nil {
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
	if s.Operation == nil || s.Operation.Step != updateStepSurge {
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
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, tcr.Name, plan.InstanceReadyTimeout); err != nil {
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
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, target.Name, plan.InstanceReadyTimeout); err != nil {
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
	status := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if status.Operation == nil || status.Operation.Step != updateStepSurge {
		t.Fatalf("operation advanced before replacement PodReady: %+v", status.Operation)
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
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, tcr.Name, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepDrain(context.Background(), input, 0); err != nil {
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
				Step:           updateStepSurge,
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
// actually carry), NOT v3 (the latest target). Stamping v3 is the X-2
// corruption: status would claim v3 while the pods run v2, and
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
		t.Errorf("X-2 re-bump: promoted RunningRevision=%q, want %q (the rev the pods actually carry). "+
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
	// before that gate (the fix); the original code returned at the gate with
	// the source still serving.
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

	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
		t.Fatalf("first call: %v", err)
	}
	beforeRV := readRV(t, c, isvc)
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), input, 0, "rev-abc", 1); err != nil {
		t.Fatalf("second call: %v", err)
	}
	afterRV := readRV(t, c, isvc)
	if beforeRV != afterRV {
		t.Errorf("idempotent call bumped ResourceVersion: %s -> %s", beforeRV, afterRV)
	}
}
