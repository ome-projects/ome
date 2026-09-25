package escalation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Escalation-pass guards: the pass decides the Failed transition from
// snapshot evidence, and it must never stamp Failed over an Instance
// whose blamed pod set is actually serving (the stale-bookkeeping /
// wedged-Operation shape), while still firing identically for a
// genuinely wedged pod set.

// servingPod builds a pod that is ContainersReady AND carries the
// ome.io/serving readiness gate — a pod in the load-balancer rotation.
func servingPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: "ome.io/serving", Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// escalationFixture wires a ReconcileInput whose MutateInstance applies
// the callback to the local store and whose WarnInstanceFailed /
// MutateRetryBlock record their calls.
type escalationRecorder struct {
	store  []workloadtypes.InstanceStatus
	warns  []string
	blocks []workloadtypes.RetryBlock
}

func escalationFixture(insts []workloadtypes.InstanceStatus) (workloadtypes.ReconcileInput, *escalationRecorder) {
	rec := &escalationRecorder{store: append([]workloadtypes.InstanceStatus(nil), insts...)}
	input := workloadtypes.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range rec.store {
				if rec.store[i].Index == idx {
					mutate(&rec.store[i])
					return nil
				}
			}
			return nil
		},
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			b := workloadtypes.RetryBlock{TargetRevision: rev}
			mutate(&b)
			rec.blocks = append(rec.blocks, b)
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, reason string) {
			rec.warns = append(rec.warns, reason)
		},
	}
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)
	return input, rec
}

// TestEscalation_DeadlineElapsedButServing_NoFailedStamp pins the
// Failed-while-serving guard: an Instance whose Operation.Deadline has
// elapsed but whose pods are all present, Ready AND serving must NOT be
// stamped Failed (no mutation, no WarnInstanceFailed, no RetryBlock).
// Stamping Failed over a serving workload is a status lie — the pods
// are fine, only the Operation bookkeeping is stale.
func TestEscalation_DeadlineElapsedButServing_NoFailedStamp(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {servingPod("engine-0-default-0")}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (serving Instance must not be stamped Failed)", rec.store[0].Phase)
	}
	if rec.store[0].Operation == nil {
		t.Errorf("Operation: got nil want untouched (guard skips the Instance entirely)")
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
}

// TestEscalation_DeadlineElapsedBelowDesiredServing_StillEscalates pins
// the guard's bound: pods serving but FEWER than desired do not prove
// health — the deadline expiry still fires (a partially-serving gang
// that never converged is exactly what InstanceReadyTimeout bounds).
func TestEscalation_DeadlineElapsedBelowDesiredServing_StillEscalates(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseCreating,
		PodCount: 2,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {servingPod("engine-0-leader-0")}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (1/2 serving is not converged)", rec.store[0].Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_StuckPodNotServing_IdenticalDispositionOutcome pins the
// evidence-driven fast path: a genuinely stuck pod (ImagePullBackOff
// past grace, not serving) on a single-pod Update attempt still lands
// the full disposition outcome — RetryBlock for the attempt's target
// revision, Operation cleared, Phase=Failed, one operator warning.
func TestEscalation_StuckPodNotServing_IdenticalDispositionOutcome(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           workloadtypes.UpdateStepDrain,
			TargetRevision: "own-engine-badhash",
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.StuckPodGrace = time.Second

	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-default-1",
			CreationTimestamp: metav1.NewTime(now.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
			}},
		},
	}

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", rec.store[0].Phase)
	}
	if rec.store[0].Operation != nil {
		t.Errorf("Operation: got %+v want nil (disposition clears the failed attempt)", rec.store[0].Operation)
	}
	if len(rec.blocks) != 1 || rec.blocks[0].TargetRevision != "own-engine-badhash" {
		t.Errorf("RetryBlock: got %+v want one block for own-engine-badhash", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_SinglePodSurgeRelocatesTheFailedTarget pins the
// source/target split for a single-pod SurgeThenDrain attempt. The healthy
// source and failed target share one Instance index, but relocation evidence
// must name only the target's node so the retry can be rendered away from it.
func TestEscalation_SinglePodSurgeRelocatesTheFailedTarget(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(now)
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 2,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           workloadtypes.UpdateStepSurge,
			TargetRevision: "own-engine-targethash",
			StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	c := fakeLedgerClient(t)
	input, rec := dispositionFixtureInput(fc, &insts, "own-engine-targethash", nil, ledgerOwnerCM())
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)
	input.StuckPodGrace = time.Second
	input.Disposition.AutoMigrateMaxAttempts = 3

	source := servingPod("engine-0-default-0")
	source.Spec.NodeName = "node-source"
	target := waitingPod("engine-0-default-1", "CreateContainerError", "node-target", now.Add(-time.Minute))

	plan := singleInstancePlan(0, 1)
	plan.MigrationMode = workloadtypes.MigrationModeAuto
	if err := runEscalationPass(t, workloadtypes.Deps{Client: c, Clock: fc}, input, plan,
		map[int32][]*corev1.Pod{0: {source, target}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	ledger := loadLedger(t, c)
	if len(ledger.Entries) != 1 || ledger.Entries[0].FromNode != "node-target" {
		t.Fatalf("relocation entries: got %+v want one directive for node-target", ledger.Entries)
	}
	if insts[0].Phase != workloadtypes.InstancePhaseFailed || insts[0].Operation != nil {
		t.Fatalf("instance: got Phase=%q Operation=%+v want Failed with no operation", insts[0].Phase, insts[0].Operation)
	}
	if insts[0].LastFailure == nil || insts[0].LastFailure.PodName != target.Name ||
		insts[0].LastFailure.Reason != "CreateContainerError" {
		t.Fatalf("LastFailure: got %+v want target pod CreateContainerError", insts[0].LastFailure)
	}
	if len(rec.blockCalls) != 0 {
		t.Fatalf("RetryBlocks: got %+v want none for ambiguous node-local failure", rec.blockCalls)
	}
}

// TestEscalation_SinglePodSurgeDeadlineRelocatesOffTheTargetNode pins the
// deadline path of the same split. The source serves on its own node and
// the replacement sits on another; when the operation deadline ends the
// attempt, the directive must name the replacement's node — the source
// is not the attempt's, so it can neither be blamed nor, by sitting on a
// second node, turn the reading empty and lose the directive. Two shapes
// reach the deadline without the fast escalator: a replacement bound but
// never started, and a wedged one with the stuck-pod grace disabled.
func TestEscalation_SinglePodSurgeDeadlineRelocatesOffTheTargetNode(t *testing.T) {
	const oldRev, targetRev = "own-engine-oldhash", "own-engine-targethash"
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	label := func(pod *corev1.Pod, rev string) *corev1.Pod {
		pod.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(rev).Hash()}
		return pod
	}
	boundNeverStarted := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-1", CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour))},
		Spec:       corev1.PodSpec{NodeName: "node-target"},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	for _, tc := range []struct {
		name   string
		target *corev1.Pod
		grace  time.Duration
	}{
		{name: "bound but never started", target: boundNeverStarted, grace: time.Second},
		{name: "wedged with the fast escalator disabled", target: waitingPod("engine-0-default-1", "CrashLoopBackOff", "node-target", now.Add(-2*time.Hour)), grace: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := clocktesting.NewFakeClock(now)
			insts := []workloadtypes.InstanceStatus{{
				Index:           0,
				Phase:           workloadtypes.InstancePhaseUpdating,
				PodCount:        1,
				RunningRevision: oldRev,
				Operation: &workloadtypes.InstanceOperation{
					Type:           workloadtypes.InstanceOperationUpdate,
					Step:           workloadtypes.UpdateStepSurge,
					TargetRevision: targetRev,
					StartedAt:      metav1.NewTime(now.Add(-2 * time.Hour)),
					Deadline:       metav1.NewTime(now.Add(-time.Hour)),
				},
			}}
			c := fakeLedgerClient(t)
			input, rec := dispositionFixtureInput(fc, &insts, targetRev, nil, ledgerOwnerCM())
			input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)
			input.StuckPodGrace = tc.grace
			input.Disposition.AutoMigrateMaxAttempts = 3

			source := label(servingPod("engine-0-default-0"), oldRev)
			source.Spec.NodeName = "node-source"
			target := label(tc.target.DeepCopy(), targetRev)

			plan := singleInstancePlan(0, 1)
			plan.MigrationMode = workloadtypes.MigrationModeAuto
			if err := runEscalationPass(t, workloadtypes.Deps{Client: c, Clock: fc}, input, plan,
				map[int32][]*corev1.Pod{0: {source, target}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			ledger := loadLedger(t, c)
			if len(ledger.Entries) != 1 || ledger.Entries[0].FromNode != "node-target" {
				t.Fatalf("relocation entries: got %+v want one directive for node-target", ledger.Entries)
			}
			if insts[0].Phase != workloadtypes.InstancePhaseFailed || insts[0].Operation != nil {
				t.Fatalf("instance: got Phase=%q Operation=%+v want Failed with no operation", insts[0].Phase, insts[0].Operation)
			}
			if len(rec.blockCalls) != 0 {
				t.Fatalf("RetryBlocks: got %+v want none (the deadline blames no revision)", rec.blockCalls)
			}
		})
	}
}

// TestEscalation_MultiInstanceStamps_OneBatchedWrite pins the batched
// flush: when the adapter wires ApplyInstanceMutations, a
// multi-instance escalation of plain Failed stamps lands in ONE batched
// write — zero per-instance MutateInstance calls — with every warning
// fired after the write, one per instance.
func TestEscalation_MultiInstanceStamps_OneBatchedWrite(t *testing.T) {
	now := time.Now()
	var insts []workloadtypes.InstanceStatus
	for idx := int32(0); idx < 3; idx++ {
		insts = append(insts, workloadtypes.InstanceStatus{
			Index:    idx,
			Phase:    workloadtypes.InstancePhaseRestarting,
			PodCount: 1,
			Operation: &workloadtypes.InstanceOperation{
				Type:     workloadtypes.InstanceOperationRestart,
				Deadline: metav1.NewTime(now.Add(-time.Hour)),
			},
		})
	}
	store := append([]workloadtypes.InstanceStatus(nil), insts...)
	batchCalls, mutateCalls := 0, 0
	var warns []int32
	warnsBeforeWrite := -1
	input := workloadtypes.ReconcileInput{
		ApplyInstanceMutations: func(_ context.Context, muts []workloadtypes.InstanceMutation) error {
			batchCalls++
			warnsBeforeWrite = len(warns)
			for _, m := range muts {
				for i := range store {
					if store[i].Index == m.Index {
						m.Mutate(&store[i])
					}
				}
			}
			return nil
		},
		MutateInstance: func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
			mutateCalls++
			return nil
		},
		WarnInstanceFailed: func(idx int32, _, _ string) {
			warns = append(warns, idx)
		},
	}
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if batchCalls != 1 {
		t.Errorf("ApplyInstanceMutations calls: got %d want 1 (k stamps must coalesce into one batched write)", batchCalls)
	}
	if mutateCalls != 0 {
		t.Errorf("MutateInstance calls: got %d want 0 (plain stamps must route through the batch)", mutateCalls)
	}
	for i := range store {
		if store[i].Phase != workloadtypes.InstancePhaseFailed {
			t.Errorf("instance %d Phase: got %q want Failed", store[i].Index, store[i].Phase)
		}
		if store[i].Operation == nil {
			t.Errorf("instance %d Operation: got nil want preserved (deadline stamp keeps the Operation)", store[i].Index)
		}
	}
	if len(warns) != 3 {
		t.Errorf("WarnInstanceFailed: got %v want one call per instance", warns)
	}
	if warnsBeforeWrite != 0 {
		t.Errorf("warnings fired before the batched write: got %d want 0 (warn follows its write)", warnsBeforeWrite)
	}
}

// TestEscalation_BatchedWriteFails_NoWarnings pins the flush error
// contract: a failed batched write emits no warnings (same as the
// immediate path — the stamp's warning follows its write) and surfaces
// the error.
func TestEscalation_BatchedWriteFails_NoWarnings(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseRestarting,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationRestart,
			Deadline: metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	warned := 0
	input := workloadtypes.ReconcileInput{
		ApplyInstanceMutations: func(_ context.Context, _ []workloadtypes.InstanceMutation) error {
			return context.DeadlineExceeded
		},
		WarnInstanceFailed: func(_ int32, _, _ string) { warned++ },
	}
	input.ObservedState.InstanceStatuses = insts

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil); err == nil {
		t.Fatalf("escalation pass: got nil error, want the flush failure surfaced")
	}
	if warned != 0 {
		t.Errorf("WarnInstanceFailed: got %d calls want 0 (no warning without a landed write)", warned)
	}
}

// TestEscalation_DispositionFlushesPendingStamps pins the write-ahead
// preservation: when a buffered plain stamp is followed by a
// disposition-routed instance, the buffer flushes BEFORE the
// disposition's own writes (RetryBlock upsert, then the immediate
// op-clear MutateInstance) so the overall write order matches the
// unbatched pass.
func TestEscalation_DispositionFlushesPendingStamps(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{
		{
			// Buffered: Restart deadline expiry takes the plain stamp.
			Index:    0,
			Phase:    workloadtypes.InstancePhaseRestarting,
			PodCount: 1,
			Operation: &workloadtypes.InstanceOperation{
				Type:     workloadtypes.InstanceOperationRestart,
				Deadline: metav1.NewTime(now.Add(-time.Hour)),
			},
		},
		{
			// Disposition: single-pod Update attempt with a stuck pod.
			Index:    1,
			Phase:    workloadtypes.InstancePhaseUpdating,
			PodCount: 1,
			Operation: &workloadtypes.InstanceOperation{
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepDrain,
				TargetRevision: "own-engine-badhash",
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
	}
	store := append([]workloadtypes.InstanceStatus(nil), insts...)
	var sequence []string
	input := workloadtypes.ReconcileInput{
		ApplyInstanceMutations: func(_ context.Context, muts []workloadtypes.InstanceMutation) error {
			sequence = append(sequence, "batch")
			for _, m := range muts {
				for i := range store {
					if store[i].Index == m.Index {
						m.Mutate(&store[i])
					}
				}
			}
			return nil
		},
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			sequence = append(sequence, "mutate")
			for i := range store {
				if store[i].Index == idx {
					mutate(&store[i])
				}
			}
			return nil
		},
		MutateRetryBlock: func(_ context.Context, _ string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			sequence = append(sequence, "retryblock")
			b := workloadtypes.RetryBlock{}
			mutate(&b)
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) {},
	}
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)
	input.StuckPodGrace = time.Second

	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-1-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
			}},
		},
	}

	if err := runEscalationPass(t, workloadtypes.Deps{}, input,
		workloadtypes.ComponentPlan{Instances: []workloadtypes.InstancePlan{
			{Index: 0, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
		}},
		map[int32][]*corev1.Pod{1: {stuck}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	want := []string{"batch", "retryblock", "mutate"}
	if len(sequence) != len(want) {
		t.Fatalf("write sequence: got %v want %v", sequence, want)
	}
	for i := range want {
		if sequence[i] != want[i] {
			t.Fatalf("write sequence: got %v want %v (pending stamps must flush before the disposition's write-ahead-ordered writes)", sequence, want)
		}
	}
	if store[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("instance 0 Phase: got %q want Failed", store[0].Phase)
	}
	if store[1].Phase != workloadtypes.InstancePhaseFailed || store[1].Operation != nil {
		t.Errorf("instance 1: got Phase=%q Operation=%v want Failed with Operation cleared (disposition path)", store[1].Phase, store[1].Operation)
	}
}

// TestEscalation_MigratePair_StuckSurgePod_NotStamped pins the
// migration-authority contract on the FAST path: a migration surge pod
// stuck in a terminal waiting state past the stuck-pod grace must NOT
// stamp either pair side Failed. podsForStuckCheck blames a Migrate
// pair through own-plus-sibling pods, so without the exclusion the
// stuck surge pod fails the healthy still-serving source — and a
// Failed source drops out of migrationSourceIndices, letting the plan
// allocate a phantom index. The migration record's expiry pass owns
// migration failure.
func TestEscalation_MigratePair_StuckSurgePod_NotStamped(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(1)
	sourceIdx := int32(0)
	future := metav1.NewTime(now.Add(30 * time.Minute))
	insts := []workloadtypes.InstanceStatus{
		{
			Index:    0,
			Phase:    workloadtypes.InstancePhaseMigrating,
			PodCount: 1,
			Operation: &workloadtypes.InstanceOperation{
				Type:        workloadtypes.InstanceOperationMigrate,
				Step:        "CreateSurge",
				RequestUUID: "mig-1",
				SurgeIndex:  &surgeIdx,
				Deadline:    future,
			},
		},
		{
			Index: 1,
			Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				Type:        workloadtypes.InstanceOperationMigrate,
				Step:        "CreateSurge",
				RequestUUID: "mig-1",
				SurgeIndex:  &sourceIdx,
				Deadline:    future,
			},
		},
	}
	input, rec := escalationFixture(insts)
	input.StuckPodGrace = time.Second

	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-1-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			}},
		},
	}

	if err := runEscalationPass(t, workloadtypes.Deps{}, input,
		workloadtypes.ComponentPlan{Instances: []workloadtypes.InstancePlan{
			{Index: 0, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
		}},
		map[int32][]*corev1.Pod{
			0: {servingPod("engine-0-default-0")},
			1: {stuck},
		}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0]; got.Phase != workloadtypes.InstancePhaseMigrating || got.Operation == nil {
		t.Errorf("migration source must be untouched by the stuck-pod fast path; got phase=%q op=%+v", got.Phase, got.Operation)
	}
	if got := rec.store[1]; got.Phase != workloadtypes.InstancePhaseCreating || got.Operation == nil {
		t.Errorf("migration surge must be untouched by the stuck-pod fast path; got phase=%q op=%+v", got.Phase, got.Operation)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none (Migrate pairs are record-owned)", rec.warns)
	}
}

// TestEscalation_GangSurgeSource_GatedAttemptPods_DeadlineSkipped pins
// the gate exemption across the surge pair: a gang-surge SOURCE's
// attempt pods live in its Operation.SurgeIndex bucket, so while they
// queue for admission the source's elapsed deadline must not expire —
// failing it would tear down the queued gang (losing queue position)
// and RetryBlock a healthy target revision.
func TestEscalation_GangSurgeSource_GatedAttemptPods_DeadlineSkipped(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(2)
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           "Surge",
			TargetRevision: "own-engine-newhash",
			SurgeIndex:     &surgeIdx,
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	gated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-2-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-time.Hour)),
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}},
		},
	}

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{
			0: {servingPod("engine-0-default-0")},
			2: {gated},
		}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (queued attempt pods must park the source's deadline, not expire it)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
}

// A Restart attempt whose Operation.Deadline elapsed is not disposable:
// the stamp lands Phase=Failed with the Operation PRESERVED (a Restart
// parked at Failed is a spent attempt the gang trigger may re-arm) and a
// bare DeadlineExceeded LastFailure when no pod names a better cause.
func TestEscalation_RestartDeadlineElapsed_FailsInstance(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:       0,
		Incarnation: 2,
		Phase:       workloadtypes.InstancePhaseRestarting,
		PodCount:    1,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationRestart,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(-time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {waitingPod("engine-0-default-0", "PodInitializing", "node-a", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.Operation == nil || got.Operation.Type != workloadtypes.InstanceOperationRestart {
		t.Errorf("Operation: got %+v want the Restart attempt preserved", got.Operation)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != escalation.DeadlineExceededReason {
		t.Errorf("LastFailure: got %+v want Reason=%s", got.LastFailure, escalation.DeadlineExceededReason)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// DeadlineExceeded is outside the terminal waiting set, so the fast
// escalation never fires on it: a Restart whose pod holds that reason
// past the stuck-pod grace keeps its step and waits out its own
// operation deadline. A terminal reason on the same shape does escalate,
// which is what makes the negative meaningful.
func TestEscalation_RestartDeadlineExceededWaiting_NoFastFail(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		reason string
		want   workloadtypes.InstancePhase
	}{
		{reason: escalation.DeadlineExceededReason, want: workloadtypes.InstancePhaseRestarting},
		{reason: "CrashLoopBackOff", want: workloadtypes.InstancePhaseFailed},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			insts := []workloadtypes.InstanceStatus{{
				Index:       0,
				Incarnation: 2,
				Phase:       workloadtypes.InstancePhaseRestarting,
				PodCount:    1,
				Operation: &workloadtypes.InstanceOperation{
					Type:     workloadtypes.InstanceOperationRestart,
					Step:     "Drain",
					Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
				},
			}}
			input, rec := escalationFixture(insts)
			input.StuckPodGrace = time.Second

			if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: {waitingPod("engine-0-default-0", tc.reason, "node-a", now.Add(-time.Hour))}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			if rec.store[0].Phase != tc.want {
				t.Errorf("Phase: got %q want %q", rec.store[0].Phase, tc.want)
			}
			if tc.want == workloadtypes.InstancePhaseRestarting {
				if rec.store[0].Operation == nil {
					t.Errorf("Operation: got nil want the in-flight Restart untouched")
				}
				if len(rec.warns) != 0 {
					t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
				}
			}
		})
	}
}

// TestEscalation_AdmissionGatedInstance_DeadlineSkipped pins the gated
// skip: an Instance whose pod is still held by an admission scheduling
// gate is queued, not stuck — an elapsed deadline observed before the
// parking step zeroes it must not expire the Instance.
func TestEscalation_AdmissionGatedInstance_DeadlineSkipped(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseCreating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	gated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-time.Hour)),
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}},
		},
	}

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {gated}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (gated Instance is queued, not stuck)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_DrainingPodDoesNotMaskStuckReplacement pins
// podSetFullyServing's deleting-pod rule from three sides. A completed
// surge leaves the drained source next to its serving replacement, so a
// deleting pod must not disqualify an otherwise healthy set (a); but it
// proves nothing on its own, so it cannot cover the desired count (b),
// and it cannot vouch for a replacement the set is actually waiting on
// (c). Only (a) suppresses escalation.
func TestEscalation_DrainingPodDoesNotMaskStuckReplacement(t *testing.T) {
	now := time.Now()
	deleting := func(pod *corev1.Pod) *corev1.Pod {
		ts := metav1.NewTime(now.Add(-time.Minute))
		pod.DeletionTimestamp = &ts
		pod.Finalizers = []string{"ome.io/test"}
		return pod
	}
	terminalPod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: metav1.NewTime(now.Add(-time.Hour)),
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
				Conditions: []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
					{Type: "ome.io/serving", Status: corev1.ConditionTrue},
				},
			},
		}
	}
	surgeDrainInstance := func() []workloadtypes.InstanceStatus {
		return []workloadtypes.InstanceStatus{{
			Index:    0,
			Phase:    workloadtypes.InstancePhaseUpdating,
			PodCount: 1,
			Operation: &workloadtypes.InstanceOperation{
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepSurgeDrain,
				TargetRevision: "own-engine-newhash",
				Deadline:       metav1.NewTime(now.Add(-time.Hour)),
			},
		}}
	}

	cases := []struct {
		name     string
		pods     []*corev1.Pod
		escalate bool
	}{
		{
			name:     "drained source beside its serving replacement",
			pods:     []*corev1.Pod{deleting(servingPod("engine-0-default-0")), servingPod("engine-0-default-1")},
			escalate: false,
		},
		{
			name:     "deleting pod alone cannot cover the desired count",
			pods:     []*corev1.Pod{deleting(servingPod("engine-0-default-0"))},
			escalate: true,
		},
		{
			name:     "drain in flight beside a terminal replacement",
			pods:     []*corev1.Pod{deleting(servingPod("engine-0-default-0")), terminalPod("engine-0-default-1")},
			escalate: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture(surgeDrainInstance())
			if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: tc.pods}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			failed := rec.store[0].Phase == workloadtypes.InstancePhaseFailed
			if failed != tc.escalate {
				t.Fatalf("Phase: got %q (escalated=%v) want escalated=%v", rec.store[0].Phase, failed, tc.escalate)
			}
			want := 0
			if tc.escalate {
				want = 1
			}
			if len(rec.warns) != want {
				t.Errorf("WarnInstanceFailed: got %v want %d call(s)", rec.warns, want)
			}
		})
	}
}

func TestEscalation_PausedHoldSkipsStuckPodFastPath(t *testing.T) {
	now := time.Now()
	insts := func(waiting string) []workloadtypes.InstanceStatus {
		return []workloadtypes.InstanceStatus{{
			Index:    0,
			Phase:    workloadtypes.InstancePhaseUpdating,
			PodCount: 1,
			Operation: &workloadtypes.InstanceOperation{
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepSurge,
				TargetRevision: "own-engine-badhash",
				Waiting:        waiting,
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		}}
	}
	stuck := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "engine-0-default-1",
				CreationTimestamp: metav1.NewTime(now.Add(-time.Minute)),
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "main",
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
				}},
			},
		}
	}

	input, rec := escalationFixture(insts(workloadtypes.WaitingReasonPaused))
	input.StuckPodGrace = time.Second
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck()}}); err != nil {
		t.Fatalf("escalation pass (held): %v", err)
	}
	if rec.store[0].Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("held Phase: got %q want Updating (an operator holds the row)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}

	input, rec = escalationFixture(insts(""))
	input.StuckPodGrace = time.Second
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck()}}); err != nil {
		t.Fatalf("escalation pass (released): %v", err)
	}
	if rec.store[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("released Phase: got %q want Failed", rec.store[0].Phase)
	}

	// The guard is scoped to the pause token, so a row whose surge took
	// the token over to report its source out of rotation is still the
	// pass's business once the pause is cleared.
	input, rec = escalationFixture(insts(workloadtypes.WaitingReasonSourceUnrouted))
	input.StuckPodGrace = time.Second
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {stuck()}}); err != nil {
		t.Fatalf("escalation pass (unrouted): %v", err)
	}
	if rec.store[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("unrouted Phase: got %q want Failed (the pause guard must not cover it)", rec.store[0].Phase)
	}
}

// wedgedButGatedPod is the shape the failed-while-serving guard must not
// read as healthy capacity: conditions still say ContainersReady and the
// serving gate is still True, while the container is parked in a
// terminal waiting reason.
func wedgedButGatedPod(name, reason string) *corev1.Pod {
	pod := servingPod(name)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
	return pod
}

func surgingInstanceWithWaiting(waiting string) []workloadtypes.InstanceStatus {
	return []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           workloadtypes.UpdateStepSurge,
			TargetRevision: "own-engine-targethash",
			Deadline:       metav1.NewTime(time.Now().Add(30 * time.Minute)),
			Waiting:        waiting,
		},
	}}
}

// TestEscalation_UnroutedSourceDoesNotExemptAWedgedSurge: the
// failed-while-serving guard exempts an Instance whose pod set reads
// healthy, and pod conditions alone can read healthy while nothing is in
// rotation. A surge that has reported its source out of rotation is
// exactly that case, so the exemption must not apply and the wedged pod
// must still escalate.
func TestEscalation_UnroutedSourceDoesNotExemptAWedgedSurge(t *testing.T) {
	insts := surgingInstanceWithWaiting(workloadtypes.WaitingReasonSourceUnrouted)
	input, rec := escalationFixture(insts)
	input.StuckPodGrace = time.Second

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {wedgedButGatedPod("engine-0-default-0", "ImagePullBackOff")}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (nothing is in rotation to exempt the row)", rec.store[0].Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_RoutedSourceKeepsTheServingExemption pins the other
// side: with no report of an unrouted source the guard stands, and a row
// whose pods read healthy is left alone whatever the evidence says.
func TestEscalation_RoutedSourceKeepsTheServingExemption(t *testing.T) {
	insts := surgingInstanceWithWaiting("")
	input, rec := escalationFixture(insts)
	input.StuckPodGrace = time.Second

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {wedgedButGatedPod("engine-0-default-0", "ImagePullBackOff")}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (a serving pod set is never stamped Failed)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// The hold arms judge an Instance on pod health alone. A row whose pods
// are all serving releases the token it was parked behind — the PodGroup
// bookkeeping may still say the name is being collected, but a gang
// already in rotation is not waiting on it, and a row left holding a
// stale token has its own deadline parked with it. The stricter capacity
// question, which a source out of rotation answers no, belongs to the
// failed-while-serving exemption alone: it decides what is exempt from
// escalating, never who owns the Waiting token.
func TestEscalation_ServingRowKeepsTheHoldArmsOnPodHealth(t *testing.T) {
	t.Run("gang hold released", func(t *testing.T) {
		input, rec := escalationFixture(surgingInstanceWithWaiting(workloadtypes.WaitingReasonPodGroupTerminating))
		input.Gangs = gangObservations(0, workloadtypes.GangStateTerminating, gangTerminatingMessage)

		if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {servingPod("engine-0-default-0")}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}

		got := rec.store[0]
		if got.Operation == nil || got.Operation.Waiting != "" {
			t.Errorf("Operation.Waiting: got %+v want cleared; serving pods are not waiting on a name", got.Operation)
		}
		if got.Phase != workloadtypes.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating", got.Phase)
		}
	})
	t.Run("rotation report kept", func(t *testing.T) {
		// The source is healthy but out of rotation: the surge's own report
		// stands, and the gang arm reading a Terminating group neither takes
		// the row from it nor ends the attempt.
		input, rec := escalationFixture(surgingInstanceWithWaiting(workloadtypes.WaitingReasonSourceUnrouted))
		input.Gangs = gangObservations(0, workloadtypes.GangStateTerminating, gangTerminatingMessage)
		source := servingPod("engine-0-default-0")
		for i := range source.Status.Conditions {
			if source.Status.Conditions[i].Type == query.ServingConditionType {
				source.Status.Conditions[i].Status = corev1.ConditionFalse
			}
		}

		if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {source}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}

		got := rec.store[0]
		if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonSourceUnrouted {
			t.Errorf("Operation.Waiting: got %+v want the report kept; no arm may take a row from it", got.Operation)
		}
		if got.Phase != workloadtypes.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating", got.Phase)
		}
		if len(rec.warns) != 0 {
			t.Errorf("WarnInstanceFailed: got %v want none (the attempt has time left)", rec.warns)
		}
	})
}

// TestEscalation_CorrectiveRevisionDoesNotRideTheFailingSurge: a
// revision edit that lands in the window where the attempt is being ended
// — by its operation deadline or by the stuck-pod fast path — does not
// retarget it. The escalation commits against the revision the surge was
// pinned to, so the blame lands where the evidence is, and the row that
// comes out of it is Failed. From there the corrective revision is a
// different subject: the Failed row owns the decision to re-drive, and
// what it drives is a fresh attempt rather than a continuation.
func TestEscalation_CorrectiveRevisionDoesNotRideTheFailingSurge(t *testing.T) {
	const pinned = "llama-70b-engine-pinnedrv"
	now := time.Now()

	// failingSurge is a single-pod surge committed to pinned, with the
	// operator's corrective revision already observed.
	failingSurge := func(deadline time.Time) []workloadtypes.InstanceStatus {
		return []workloadtypes.InstanceStatus{{
			Index:           0,
			Incarnation:     1,
			Phase:           workloadtypes.InstancePhaseUpdating,
			PodCount:        1,
			RunningRevision: "llama-70b-engine-priorrev",
			TargetRevision:  pinned,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "surge-0",
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepSurge,
				TargetRevision: pinned,
				StartedAt:      metav1.NewTime(now.Add(-time.Hour)),
				Deadline:       metav1.NewTime(deadline),
			},
		}}
	}

	for _, tc := range []struct {
		name     string
		deadline time.Time
		pods     map[int32][]*corev1.Pod
		grace    time.Duration
	}{
		{
			name:     "the operation deadline ends it",
			deadline: now.Add(-time.Minute),
			pods:     map[int32][]*corev1.Pod{0: {waitingPod("engine-0-default-1", "ContainerCreating", "node-a", now.Add(-time.Hour))}},
		},
		{
			name:     "the stuck-pod fast path ends it",
			deadline: now.Add(30 * time.Minute),
			pods:     map[int32][]*corev1.Pod{0: {waitingPod("engine-0-default-1", "ImagePullBackOff", "node-a", now.Add(-time.Hour))}},
			grace:    time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Boundary: the corrective revision is observed on the very
			// pass that ends the attempt.
			input, rec := escalationFixture(failingSurge(tc.deadline))
			input.StuckPodGrace = tc.grace
			input.ObservedState.UpdateRevision = updateTarget().Name
			if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1), tc.pods); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			got := rec.store[0]
			if got.Phase != workloadtypes.InstancePhaseFailed {
				t.Fatalf("Phase: got %q want Failed (the stamp is what this window commits)", got.Phase)
			}
			for _, b := range rec.blocks {
				if b.TargetRevision != pinned {
					t.Errorf("RetryBlock subject: got %q want the pinned %q (the edit is not what failed)",
						b.TargetRevision, pinned)
				}
			}

			// After: the row is Failed and the corrective revision drives a
			// fresh attempt, not the surge that just ended.
			in := minimalInput(t)
			forbidMutations(t, &in)
			// The row the escalation just wrote, carried forward.
			failed := rec.store[0]
			failed.RunningRevision = "prior-rev"
			in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{failed}
			d := planTargetOrFail(t, in, minimalPlan(), updateTarget(),
				planSnapshot(in, map[int32][]*corev1.Pod{0: {enginePod(in.Key.OwnerName, in.Key.Namespace, 0)}}))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil {
				t.Fatalf("the corrective revision drove nothing from Failed; got %v", actionKinds(d))
			}
			if !ua.Update.Items[0].StartingFresh {
				t.Errorf("update item: got %+v want a fresh attempt, not the ended surge continued", ua.Update.Items[0])
			}
		})
	}
}

// Verifies the escalation pass consults the injected clock for deadline
// expiry:
// one tick before the deadline nothing expires; one tick after, the
// instance fails with DeadlineExceeded. Only possible with a fake clock.

func TestExpireOperations_ExactBoundary(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	deadline := t0.Add(30 * time.Minute)
	fc := clocktesting.NewFakeClock(deadline.Add(-time.Second)) // 1s BEFORE

	instances := []workloadtypes.InstanceStatus{{
		Index: 0, Phase: workloadtypes.InstancePhaseCreating,
		Operation: &workloadtypes.InstanceOperation{
			Type:      workloadtypes.InstanceOperationCreate,
			StartedAt: metav1.NewTime(t0), Deadline: metav1.NewTime(deadline),
		},
	}}

	var mutated []int32
	var warned []int32
	input := workloadtypes.ReconcileInput{
		Clock: fc,
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			if mutate(&instances[idx]) {
				mutated = append(mutated, idx)
			}
			return nil
		},
		WarnInstanceFailed: func(idx int32, podName, reason string) {
			warned = append(warned, idx)
		},
	}
	// Same backing slice: the second and third pass observe the prior
	// pass's mutations, like consecutive reconciles would.
	input.ObservedState.InstanceStatuses = instances

	if err := escalation.EscalateFromEvidenceForTest(context.Background(), workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil, nil); err != nil {
		t.Fatalf("expire (before deadline): %v", err)
	}
	if len(mutated) != 0 {
		t.Fatalf("1s before deadline must not expire; mutated %v", mutated)
	}

	// AT the deadline: now.After(deadline) is false — still no expiry.
	fc.SetTime(deadline)
	if err := escalation.EscalateFromEvidenceForTest(context.Background(), workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil, nil); err != nil {
		t.Fatalf("expire (at deadline): %v", err)
	}
	if len(mutated) != 0 {
		t.Fatalf("now == deadline must not expire (After, not !Before); mutated %v", mutated)
	}

	fc.SetTime(deadline.Add(time.Second)) // 1s AFTER
	if err := escalation.EscalateFromEvidenceForTest(context.Background(), workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{}, nil, nil); err != nil {
		t.Fatalf("expire (after deadline): %v", err)
	}
	if len(mutated) != 1 || mutated[0] != 0 {
		t.Fatalf("1s after deadline must expire instance 0; mutated %v", mutated)
	}
	if len(warned) != 1 || warned[0] != 0 {
		t.Fatalf("expiry must emit exactly one operator-facing warning for instance 0; warned %v", warned)
	}
}

// Gate limbo: every container runs and passes its probes, but the pod's
// Ready condition never follows because a readiness gate it declares stays
// unsatisfied. The pod is not eligible for its Service, so every promote bar
// it stands behind waits forever and the operation deadline is the only exit.
// The record that exit leaves must name the gate rather than the clock.

// gateNotFoldedPod builds the gate-limbo shape: phase Running, the container
// running and Ready, ContainersReady=True, and a declared readiness gate
// whose condition nobody has satisfied, so Ready stays False.
func gateNotFoldedPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Spec: corev1.PodSpec{
			ReadinessGates: []corev1.PodReadinessGate{{ConditionType: podreadiness.ConditionType}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:               corev1.ContainersReady,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.NewTime(since),
				},
				{
					Type:               corev1.PodReady,
					Status:             corev1.ConditionFalse,
					Reason:             "ReadinessGatesNotReady",
					Message:            "corresponding condition of pod readiness gate \"ome.io/serving\" does not exist",
					LastTransitionTime: metav1.NewTime(since),
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// TestEscalation_DeadlineWithGateNotFoldedPod_NamesTheGate: the deadline
// expires on a Create attempt whose pod passed every probe but never became
// eligible for its Service. The record names the unsatisfied gate. Evidence
// only — the blame is the one the attempt would have taken with no evidence
// at all, because an unsatisfied gate is ambiguous between a gate nobody
// owns and a second writer deliberately holding one.
func TestEscalation_DeadlineWithGateNotFoldedPod_NamesTheGate(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseCreating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationCreate,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {gateNotFoldedPod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != evidence.ReasonReadinessGateNotSatisfied {
		t.Fatalf("LastFailure.Reason: got %+v want %q", got.LastFailure, evidence.ReasonReadinessGateNotSatisfied)
	}
	if !strings.Contains(got.LastFailure.Message, string(podreadiness.ConditionType)) {
		t.Errorf("LastFailure.Message: got %q want the unsatisfied gate named", got.LastFailure.Message)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (an unsatisfied gate is ambiguous, not revision-scoped)", rec.blocks)
	}
}

// TestEscalation_RestartDeadlineWithGateNotFoldedPod_NamesTheGate: the same
// evidence on the repair path. A restart whose rebuilt pod passes its probes
// but never returns to its Service fails on the deadline with the gate named,
// and the attempt keeps the blame it would have had without the evidence.
func TestEscalation_RestartDeadlineWithGateNotFoldedPod_NamesTheGate(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:       0,
		Incarnation: 2,
		Phase:       workloadtypes.InstancePhaseRestarting,
		PodCount:    1,
		Operation: &workloadtypes.InstanceOperation{
			Type:     workloadtypes.InstanceOperationRestart,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {gateNotFoldedPod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != evidence.ReasonReadinessGateNotSatisfied {
		t.Fatalf("LastFailure.Reason: got %+v want %q", got.LastFailure, evidence.ReasonReadinessGateNotSatisfied)
	}
	if !strings.Contains(got.LastFailure.Message, string(podreadiness.ConditionType)) {
		t.Errorf("LastFailure.Message: got %q want the unsatisfied gate named", got.LastFailure.Message)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (an unsatisfied gate is ambiguous, not revision-scoped)", rec.blocks)
	}
}

// Running-but-never-Ready limbo: the pod runs every container, the
// kubelet reports no waiting reason, and ContainersReady simply never
// flips. The operation deadline is the only exit, and the record it
// leaves must say what was never ready rather than "the clock ran out".

const probeMessage = "containers with unready status: [main]"

// runningNotReadyPod builds the limbo shape: phase Running, every
// container started, ContainersReady=False with the kubelet's own reason
// and message.
func runningNotReadyPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.ContainersReady,
				Status:             corev1.ConditionFalse,
				Reason:             evidence.ReasonContainersNotReady,
				Message:            probeMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// TestEscalation_DeadlineWithRunningNotReadyPod_NamesTheProbe: when the
// deadline expires on a single-pod Create attempt whose pod runs but
// never reports ready, the failure record names the unready container
// and the readiness condition. Evidence only — the disposition is the
// one the attempt would have taken with no evidence at all, because a
// pod that runs yet never passes its probes is ambiguous between the
// revision and the node it landed on.
func TestEscalation_DeadlineWithRunningNotReadyPod_NamesTheProbe(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseCreating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationCreate,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {runningNotReadyPod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != evidence.ReasonContainersNotReady {
		t.Fatalf("LastFailure.Reason: got %+v want %q", got.LastFailure, evidence.ReasonContainersNotReady)
	}
	if got.LastFailure.ContainerName != "main" {
		t.Errorf("LastFailure.ContainerName: got %q want the unready container \"main\"", got.LastFailure.ContainerName)
	}
	if !strings.Contains(got.LastFailure.Message, probeMessage) {
		t.Errorf("LastFailure.Message: got %q want the readiness condition message", got.LastFailure.Message)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (readiness limbo is ambiguous, not revision-scoped)", rec.blocks)
	}
}

// TestEscalation_DeadlineWithCrashLoopPod_KeepsAmbiguousBlame: a pod
// backing off is ambiguous between a broken binary and broken hardware,
// so the deadline must not start blaming the revision for it just
// because the pod is also not ready.
func TestEscalation_DeadlineWithCrashLoopPod_KeepsAmbiguousBlame(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseCreating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationCreate,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	pod := runningNotReadyPod("engine-0-default-0", now.Add(-time.Hour))
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off 5m0s"},
	}
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {pod}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", rec.store[0].Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (CrashLoopBackOff stays ambiguous)", rec.blocks)
	}
}

// TestEscalation_RunningNotReadyBeforeDeadline_Untouched: the readiness
// wait itself changes nothing. Only the deadline ends it, so a live
// deadline leaves the attempt alone however long the probes have failed.
func TestEscalation_RunningNotReadyBeforeDeadline_Untouched(t *testing.T) {
	now := time.Now()
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseCreating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationCreate,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {runningNotReadyPod("engine-0-default-0", now.Add(-24*time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workloadtypes.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (the deadline is the only exit)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 || len(rec.blocks) != 0 {
		t.Errorf("writes before the deadline: warns=%v blocks=%v want none", rec.warns, rec.blocks)
	}
}

// TestEscalation_GangDeadlineWithRunningNotReadyPods_NamesTheProbe: a
// gang surge keeps its Operation for the abandon path, and the record it
// leaves behind must still name the readiness failure — that record is
// what the abandon charges the revision's ladder on.
func TestEscalation_GangDeadlineWithRunningNotReadyPods_NamesTheProbe(t *testing.T) {
	now := time.Now()
	surge := int32(1)
	insts := []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 2,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           workloadtypes.UpdateStepGangSurgeTarget,
			SurgeIndex:     &surge,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{1: {
			runningNotReadyPod("engine-1-leader-0", now.Add(-time.Hour)),
			runningNotReadyPod("engine-1-worker-0", now.Add(-time.Hour)),
		}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.Operation == nil {
		t.Errorf("Operation: got nil want preserved (the abandon path consumes the continuation)")
	}
	if got.LastFailure == nil || got.LastFailure.Reason != evidence.ReasonContainersNotReady {
		t.Fatalf("LastFailure.Reason: got %+v want %q", got.LastFailure, evidence.ReasonContainersNotReady)
	}
	if workloadtypes.IsWorkloadCausedReason(got.LastFailure.Reason) {
		t.Errorf("IsWorkloadCausedReason(%q): got true want false — the abandon must not charge the ladder for an ambiguous failure", got.LastFailure.Reason)
	}
}

// TestEscalation_SurgeDeadlineBlamesTheTargetNotTheSource: a single-pod
// surge shares its Instance index with the source it is replacing, so
// the deadline sees both pods. When the replacement is in readiness
// limbo and the source's probes are flapping too, the EVIDENCE must name
// the replacement — the pod the attempt is about — rather than a source
// that belongs to a superseded revision.
func TestEscalation_SurgeDeadlineBlamesTheTargetNotTheSource(t *testing.T) {
	now := time.Now()
	const targetRev = "own-engine-newhash"
	insts := []workloadtypes.InstanceStatus{{
		Index:           0,
		Phase:           workloadtypes.InstancePhaseUpdating,
		PodCount:        1,
		RunningRevision: "own-engine-oldhash",
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           workloadtypes.UpdateStepSurge,
			TargetRevision: targetRev,
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.ObservedState.UpdateRevision = targetRev

	// The source is listed first, so a set-wide scan reaches it first.
	source := runningNotReadyPod("engine-0-default-0", now.Add(-2*time.Hour))
	source.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName("own-engine-oldhash").Hash()}
	target := runningNotReadyPod("engine-0-default-1", now.Add(-time.Hour))
	target.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(targetRev).Hash()}

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {source, target}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (an expired deadline must resolve)", got.Phase)
	}
	if got.Operation != nil {
		t.Errorf("Operation: got %+v want cleared (a single-pod surge is a disposable attempt)", got.Operation)
	}
	if got.LastFailure == nil || got.LastFailure.PodName != target.Name {
		t.Errorf("LastFailure: got %+v want the replacement %s blamed", got.LastFailure, target.Name)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (readiness limbo blames nobody)", rec.blocks)
	}
}

// A migration pair is owned by its record: the record's Deadline is the
// timeout authority for both rows, and the escalation pass is the place
// that has to respect it. These tests pin the two readings that reach a
// pinned row from this package — the Instance operation deadline, which
// is stamped but never consumed, and the Instance's PodGroup.

// TestEscalation_MigrationPairIgnoresTheInstanceOperationDeadline: both
// rows of a pair carry an operation with a deadline, and both deadlines
// are long past here. Neither ends anything: the fast and the broad
// escalation paths skip the Migrate pin, because failing a healthy
// serving source on a clock the migration does not own would cost
// capacity for a move the record is still driving.
func TestEscalation_MigrationPairIgnoresTheInstanceOperationDeadline(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(migrationPair(now.Add(-time.Hour)))
	plan := workloadtypes.ComponentPlan{Instances: []workloadtypes.InstancePlan{
		{Index: 0, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
		{Index: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
	}}

	// The source is serving and the replacement has not come up: exactly
	// the shape the broad backstop would end on any unpinned row.
	pending := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-1-default-0"},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, plan, map[int32][]*corev1.Pod{
		0: {servingPod("engine-0-default-0")},
		1: {pending},
	}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	for _, s := range rec.store {
		if s.Phase == workloadtypes.InstancePhaseFailed {
			t.Errorf("instance %d: got Failed want the record left in charge of the move", s.Index)
		}
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
}

// TestHasWedgedPodAgainstCurrent pins the wedged-pod recovery probe:
// disagreement between pod's revision-hash label and currentHash flags
// the wedge; empty currentHash is the no-rollout-ever guard that
// suppresses false fires.
func TestHasWedgedPodAgainstCurrent(t *testing.T) {
	label := "ome.io/revision-hash"
	mk := func(hash string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{label: hash}}}
	}

	cases := []struct {
		name        string
		pods        []*corev1.Pod
		currentHash string
		labelKey    string
		want        bool
	}{
		{name: "empty currentHash never flags", pods: []*corev1.Pod{mk("anything")}, currentHash: "", labelKey: label, want: false},
		{name: "empty labelKey never flags", pods: []*corev1.Pod{mk("anything")}, currentHash: "x", labelKey: "", want: false},
		{name: "all pods match", pods: []*corev1.Pod{mk("x"), mk("x")}, currentHash: "x", labelKey: label, want: false},
		{name: "one pod disagrees", pods: []*corev1.Pod{mk("x"), mk("y")}, currentHash: "x", labelKey: label, want: true},
		{name: "pods missing label are skipped", pods: []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{}}}, currentHash: "x", labelKey: label, want: false},
		{name: "nil pods are skipped", pods: []*corev1.Pod{nil}, currentHash: "x", labelKey: label, want: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := escalation.HasWedgedPodAgainstCurrent(tt.pods, tt.currentHash, tt.labelKey); got != tt.want {
				t.Errorf("got %v want %v", got, tt.want)
			}
		})
	}
}

// TestShouldCheckForStuckPods pins the two-path qualifier: transient
// phase + Operation qualifies; wedged-pod (label-hash disagreement)
// qualifies; Failed / Deleting never qualify; empty currentHash
// suppresses the wedged branch.
func TestShouldCheckForStuckPods(t *testing.T) {
	label := "ome.io/revision-hash"
	podWithHash := func(hash string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{label: hash}}}
	}

	cases := []struct {
		name        string
		status      *workloadtypes.InstanceStatus
		pods        []*corev1.Pod
		currentHash string
		want        bool
	}{
		{name: "nil status", status: nil, want: false},
		{name: "Failed never qualifies",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseFailed},
			want:   false,
		},
		{name: "Deleting never qualifies",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseDeleting},
			want:   false,
		},
		{name: "Creating + Operation qualifies",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseCreating, Operation: &workloadtypes.InstanceOperation{}},
			want:   true,
		},
		{name: "Updating + Operation qualifies",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseUpdating, Operation: &workloadtypes.InstanceOperation{}},
			want:   true,
		},
		{name: "Restarting + Operation qualifies",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseRestarting, Operation: &workloadtypes.InstanceOperation{}},
			want:   true,
		},
		{name: "Migrating + Operation qualifies",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseMigrating, Operation: &workloadtypes.InstanceOperation{}},
			want:   true,
		},
		{name: "Creating without Operation falls to wedged-pod check",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseCreating},
			pods:   []*corev1.Pod{podWithHash("x")}, currentHash: "y",
			want: true,
		},
		{name: "Ready + wedged-pod qualifies",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseReady},
			pods:   []*corev1.Pod{podWithHash("y")}, currentHash: "x",
			want: true,
		},
		{name: "Ready + matching pod hash does not qualify",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseReady},
			pods:   []*corev1.Pod{podWithHash("x")}, currentHash: "x",
			want: false,
		},
		{name: "Ready + wedged pod but empty currentHash does not qualify",
			status: &workloadtypes.InstanceStatus{Phase: workloadtypes.InstancePhaseReady},
			pods:   []*corev1.Pod{podWithHash("y")}, currentHash: "",
			want: false,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := escalation.ShouldCheckForStuckPods(tt.status, tt.pods, tt.currentHash, label); got != tt.want {
				t.Errorf("got %v want %v", got, tt.want)
			}
		})
	}
}

// TestEscalateStuckPodFailures_EscalatesAndEmitsEvent pins the
// integration contract for the escalation pass's wedged-pod recovery
// branch: a wedged pod past grace whose revision-hash label disagrees
// with CurrentRevision flips the InstanceStatus Phase to Failed via
// MutateInstance and fires WarnInstanceFailed exactly once.
func TestEscalateStuckPodFailures_EscalatesAndEmitsEvent(t *testing.T) {
	grace := 1 * time.Millisecond

	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "newhash"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
		},
	}

	// Local mirror of the per-Instance status the closure mutates.
	insts := []workloadtypes.InstanceStatus{{Index: 0, Phase: workloadtypes.InstancePhaseReady, RunningRevision: "own-engine-oldhash"}}
	var eventFired struct {
		idx    int32
		pod    string
		reason string
		count  int
	}
	input := workloadtypes.ReconcileInput{
		StuckPodGrace: grace,
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range insts {
				if insts[i].Index == idx {
					mutate(&insts[i])
					return nil
				}
			}
			return nil
		},
		WarnInstanceFailed: func(idx int32, podName, reason string) {
			eventFired.idx = idx
			eventFired.pod = podName
			eventFired.reason = reason
			eventFired.count++
		},
	}
	input.ObservedState.InstanceStatuses = insts
	input.ObservedState.CurrentRevision = "own-engine-oldhash"

	err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{},
		map[int32][]*corev1.Pod{0: {stuck}})
	if err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", insts[0].Phase)
	}
	if eventFired.count != 1 {
		t.Errorf("event count: got %d want 1", eventFired.count)
	}
	if eventFired.pod != stuck.Name {
		t.Errorf("event pod: got %q want %q", eventFired.pod, stuck.Name)
	}
	if eventFired.reason != "ImagePullBackOff" {
		t.Errorf("event reason: got %q want ImagePullBackOff", eventFired.reason)
	}
}

// TestEscalateStuckPodFailures_GangSurgeAttributedToSource pins the
// gang-surge attribution: a SOURCE Instance whose in-flight surge lives at
// a SEPARATE index must escalate to Failed when the SURGE gang wedges, even
// though the source's own pods are healthy. Without attributing the surge
// index's stuck pods to the source, a bad-revision gang surge hangs the
// rollout at Phase=Updating forever.
func TestEscalateStuckPodFailures_GangSurgeAttributedToSource(t *testing.T) {
	grace := 1 * time.Millisecond

	now := time.Now()
	healthy := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-leader-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "oldhash"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		},
	}
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	surgeStuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-2-leader-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "badhash"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
		},
	}

	surgeIdx := int32(2)
	insts := []workloadtypes.InstanceStatus{{
		Index:           0,
		Phase:           workloadtypes.InstancePhaseUpdating,
		RunningRevision: "own-engine-oldhash",
		Operation: &workloadtypes.InstanceOperation{
			Type:       workloadtypes.InstanceOperationUpdate,
			Step:       "Surge",
			SurgeIndex: &surgeIdx,
		},
	}}
	var eventFired struct {
		idx   int32
		pod   string
		count int
	}
	input := workloadtypes.ReconcileInput{
		StuckPodGrace: grace,
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range insts {
				if insts[i].Index == idx {
					mutate(&insts[i])
					return nil
				}
			}
			return nil
		},
		WarnInstanceFailed: func(idx int32, podName, _ string) {
			eventFired.idx = idx
			eventFired.pod = podName
			eventFired.count++
		},
	}
	input.ObservedState.InstanceStatuses = insts
	input.ObservedState.CurrentRevision = "own-engine-oldhash"

	err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{},
		map[int32][]*corev1.Pod{0: {healthy}, 2: {surgeStuck}})
	if err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if insts[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("source Phase: got %q want Failed", insts[0].Phase)
	}
	if eventFired.count != 1 || eventFired.idx != 0 {
		t.Errorf("event: got count=%d idx=%d want count=1 idx=0", eventFired.count, eventFired.idx)
	}
	if eventFired.pod != surgeStuck.Name {
		t.Errorf("event pod: got %q want %q (the wedged surge pod)", eventFired.pod, surgeStuck.Name)
	}
}

// TestEscalateStuckPodFailures_GangSurgeIgnoresFailedSource pins corrective
// rollout behavior: once a replacement gang is in flight, a terminal failure
// on the old source must not be attributed to that surge. Otherwise the gang
// recovery path abandons each new replacement before it can start.
func TestEscalateStuckPodFailures_GangSurgeIgnoresFailedSource(t *testing.T) {
	grace := 1 * time.Millisecond

	now := time.Now()
	oldStuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-leader-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "oldhash"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
			}},
		},
	}
	replacementHealthy := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-2-leader-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "newhash"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	replacementStuck := replacementHealthy.DeepCopy()
	replacementStuck.Name = "engine-2-worker-0"
	replacementStuck.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"},
	}

	for _, tc := range []struct {
		name         string
		replacement  []*corev1.Pod
		wantPhase    workloadtypes.InstancePhase
		wantEventPod string
	}{
		{name: "replacement pods not created yet", wantPhase: workloadtypes.InstancePhaseUpdating},
		{name: "replacement pods healthy", replacement: []*corev1.Pod{replacementHealthy}, wantPhase: workloadtypes.InstancePhaseUpdating},
		{name: "replacement pod also stuck", replacement: []*corev1.Pod{replacementStuck}, wantPhase: workloadtypes.InstancePhaseFailed, wantEventPod: replacementStuck.Name},
	} {
		t.Run(tc.name, func(t *testing.T) {
			surgeIdx := int32(2)
			insts := []workloadtypes.InstanceStatus{{
				Index:           0,
				Phase:           workloadtypes.InstancePhaseUpdating,
				RunningRevision: "own-engine-oldhash",
				Operation: &workloadtypes.InstanceOperation{
					Type:       workloadtypes.InstanceOperationUpdate,
					Step:       "Surge",
					SurgeIndex: &surgeIdx,
				},
			}}
			var eventPod string
			input := workloadtypes.ReconcileInput{
				StuckPodGrace: grace,
				MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
					for i := range insts {
						if insts[i].Index == idx {
							mutate(&insts[i])
						}
					}
					return nil
				},
				WarnInstanceFailed: func(_ int32, podName, _ string) { eventPod = podName },
			}
			input.ObservedState.InstanceStatuses = insts
			input.ObservedState.CurrentRevision = "own-engine-newhash"

			err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{},
				map[int32][]*corev1.Pod{
					0: {oldStuck},
					2: tc.replacement,
				})
			if err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			if insts[0].Phase != tc.wantPhase {
				t.Errorf("source Phase: got %q want %q", insts[0].Phase, tc.wantPhase)
			}
			if eventPod != tc.wantEventPod {
				t.Errorf("event pod: got %q want %q", eventPod, tc.wantEventPod)
			}
		})
	}
}

// TestEscalateStuckPodFailures_AlreadyFailedNoOp pins the idempotency:
// the escalator must not re-fire on an already-Failed Instance even
// though the wedged pod is still in front of it.
func TestEscalateStuckPodFailures_AlreadyFailedNoOp(t *testing.T) {
	grace := 1 * time.Millisecond

	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "newhash"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
		},
	}

	insts := []workloadtypes.InstanceStatus{{Index: 0, Phase: workloadtypes.InstancePhaseFailed}}
	var eventFired int
	input := workloadtypes.ReconcileInput{
		StuckPodGrace: grace,
		MutateInstance: func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
			t.Errorf("MutateInstance must not be called for already-Failed Instance")
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) { eventFired++ },
	}
	input.ObservedState.InstanceStatuses = insts
	input.ObservedState.CurrentRevision = "own-engine-oldhash"

	err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{},
		map[int32][]*corev1.Pod{0: {stuck}})
	if err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	if eventFired != 0 {
		t.Errorf("event count: got %d want 0 (already-Failed must not re-fire)", eventFired)
	}
}

// TestEscalateStuckPodFailures_NoOpWarnCallback pins the panic-on-nil
// contract escape hatch: callers that don't wire a real recorder
// (workload-side unit tests, adapters with no event surface) set an
// explicit no-op closure and the Phase=Failed mutation still lands.
func TestEscalateStuckPodFailures_NoOpWarnCallback(t *testing.T) {
	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
		},
	}
	insts := []workloadtypes.InstanceStatus{{Index: 0, Phase: workloadtypes.InstancePhaseUpdating, Operation: &workloadtypes.InstanceOperation{Type: workloadtypes.InstanceOperationUpdate, Step: "Drain"}}}
	input := workloadtypes.ReconcileInput{
		StuckPodGrace: 1 * time.Millisecond,
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range insts {
				if insts[i].Index == idx {
					mutate(&insts[i])
				}
			}
			return nil
		},
		// Explicit no-op stub — the panic-on-nil contract requires a
		// non-nil callback; adapters that don't emit events wire this
		// shape (see workload.ReconcileInput docs).
		WarnInstanceFailed: func(_ int32, _, _ string) {},
	}
	input.ObservedState.InstanceStatuses = insts
	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{},
		map[int32][]*corev1.Pod{0: {stuck}}); err != nil {
		t.Fatalf("escalation pass with no-op WarnInstanceFailed: %v", err)
	}
	if insts[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (escalation must land even with no-op warn callback)", insts[0].Phase)
	}
}

// Stuck-pod escalation must preserve the wedged pod's diagnostics
// into InstanceStatus.LastFailure when it flips the Instance to Failed —
// the escalation is followed by a recreate / teardown that deletes the
// wedged pod, so this is the surviving trace operators read.
func TestEscalateStuckPodFailures_CapturesLastFailure(t *testing.T) {
	grace := 1 * time.Millisecond

	now := time.Now()
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-leader-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "newhash"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "back-off pulling image"}},
			}},
		},
	}

	insts := []workloadtypes.InstanceStatus{{Index: 0, Phase: workloadtypes.InstancePhaseUpdating, RunningRevision: "own-engine-oldhash", Operation: &workloadtypes.InstanceOperation{Type: workloadtypes.InstanceOperationUpdate, Step: "Drain"}}}
	input := workloadtypes.ReconcileInput{
		StuckPodGrace: grace,
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range insts {
				if insts[i].Index == idx {
					mutate(&insts[i])
					return nil
				}
			}
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) {},
	}
	input.ObservedState.InstanceStatuses = insts
	input.ObservedState.CurrentRevision = "own-engine-oldhash"

	if err := runEscalationPass(t, workloadtypes.Deps{}, input, workloadtypes.ComponentPlan{},
		map[int32][]*corev1.Pod{0: {stuck}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != workloadtypes.InstancePhaseFailed {
		t.Fatalf("Phase: got %q want Failed", insts[0].Phase)
	}
	lf := insts[0].LastFailure
	if lf == nil {
		t.Fatalf("LastFailure: nil, want the wedged pod's diagnostics")
	}
	if lf.PodName != stuck.Name {
		t.Errorf("LastFailure.PodName: got %q want %q", lf.PodName, stuck.Name)
	}
	if lf.Reason != "ImagePullBackOff" {
		t.Errorf("LastFailure.Reason: got %q want ImagePullBackOff", lf.Reason)
	}
	if lf.ContainerName != "main" {
		t.Errorf("LastFailure.ContainerName: got %q want main", lf.ContainerName)
	}
	// A stuck waiting-state wedge never ran a process → no exit code.
	if lf.ExitCode != nil {
		t.Errorf("LastFailure.ExitCode: got %v want nil for a waiting-state wedge", lf.ExitCode)
	}
}
