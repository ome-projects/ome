package escalation_test

// Deadline-disposition tests: the three-branch classify-then-act that
// replaced the bare Phase=Failed stamps for expired/stuck Create and
// single-pod Update attempts.
//
//   Branch 1 (workload-caused): RetryBlock recorded + Operation cleared
//     + Phase=Failed in ONE MutateInstance mutation.
//   Branch 2 (relocatable): a TERMINAL AutoRecover ledger entry (the
//     relocation directive: Phase=Completed, Outcome=relocate-recreate)
//     recorded, budget-gated, then the SAME clear-Op + Failed backstop
//     as branch 3. RetryBlocks untouched; the rebuild is steered off
//     the recorded node by the render NotIn overlay.
//   Branch 3 (terminal): Operation cleared + Phase=Failed, NO RetryBlock.
//
// Fixtures follow the closure-recorder pattern (see
// ops/update_retryblock_test.go retryWriterInput) with a fake clock and
// a controller-runtime fake client for the audit-ledger branch.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// dispositionCommit snapshots one committed (mutate returned true)
// MutateInstance call so tests can assert the Operation-clear and the
// Failed stamp landed in the SAME mutation.
type dispositionCommit struct {
	idx   int32
	after types.InstanceStatus
}

// dispositionRecorders collects every observable side effect of one
// DisposeExpiredAttempt call.
type dispositionRecorders struct {
	commits    []dispositionCommit
	blockCalls []struct {
		rev   string
		disp  types.RetryBlockDisposition
		block types.RetryBlock
	}
	warns []struct {
		idx    int32
		pod    string
		reason string
	}
}

// dispositionFixtureInput wires the ReconcileInput closure recorders
// over insts. updateRevision seeds ObservedState.UpdateRevision (the
// Create-op fallback target). owner, when non-nil, becomes OwnerObject
// (the audit-ledger owner for branch 2).
func dispositionFixtureInput(fc *clocktesting.FakeClock, insts *[]types.InstanceStatus, updateRevision string, policy *types.RetryPolicy, owner client.Object) (types.ReconcileInput, *dispositionRecorders) {
	rec := &dispositionRecorders{}
	input := types.ReconcileInput{
		Clock:             fc,
		OwnerObject:       owner,
		OwnerGVK:          corev1.SchemeGroupVersion.WithKind("ConfigMap"),
		Key:               types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "own"},
		UpdateRetryPolicy: policy,
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			for i := range *insts {
				if (*insts)[i].Index == idx {
					if mutate(&(*insts)[i]) {
						rec.commits = append(rec.commits, dispositionCommit{idx: idx, after: (*insts)[i]})
					}
					return nil
				}
			}
			return nil
		},
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*types.RetryBlock) types.RetryBlockDisposition) error {
			b := types.RetryBlock{TargetRevision: rev}
			d := mutate(&b)
			rec.blockCalls = append(rec.blockCalls, struct {
				rev   string
				disp  types.RetryBlockDisposition
				block types.RetryBlock
			}{rev: rev, disp: d, block: b})
			return nil
		},
		WarnInstanceFailed: func(idx int32, pod, reason string) {
			rec.warns = append(rec.warns, struct {
				idx    int32
				pod    string
				reason string
			}{idx: idx, pod: pod, reason: reason})
		},
	}
	input.ObservedState.UpdateRevision = updateRevision
	return input, rec
}

// waitingPod builds a pod parked in the given container waiting reason,
// scheduled on node (empty = unscheduled).
func waitingPod(name, reason, node string, created time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "ns",
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
			}},
		},
	}
}

func fakeLedgerClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	// The source-rotation hold reads a labeled source's routed Service
	// membership; a scheme that cannot list EndpointSlices fails that
	// read instead of abstaining the way an absent Service does.
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1 to scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func ledgerOwnerCM() *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "own", Namespace: "ns", UID: "owner-uid"}}
}

func loadLedger(t *testing.T, c client.Client) *audit.Ledger {
	t.Helper()
	l, err := audit.LoadLedgerForOwner(context.Background(), c, ledgerOwnerCM())
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	return l
}

// TestDispose_WorkloadCaused_EachReason: every reason in the
// workload-caused set records a RetryBlock (Backoff with a policy,
// Held with nil policy) AND clears the Operation AND stamps
// Phase=Failed in the SAME single MutateInstance mutation.
func TestDispose_WorkloadCaused_EachReason(t *testing.T) {
	reasons := []string{"ImagePullBackOff", "ErrImagePull", "InvalidImageName", "CreateContainerConfigError"}
	for _, reason := range reasons {
		for _, tc := range []struct {
			name      string
			policy    *types.RetryPolicy
			wantState types.RetryBlockState
		}{
			{name: "policy set backs off", policy: &types.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}, wantState: types.RetryBlockBackoff},
			{name: "nil policy holds", policy: nil, wantState: types.RetryBlockHeld},
		} {
			t.Run(reason+"/"+tc.name, func(t *testing.T) {
				t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
				fc := clocktesting.NewFakeClock(t0)
				insts := []types.InstanceStatus{{
					Index: 0,
					Phase: types.InstancePhaseUpdating,
					Operation: &types.InstanceOperation{
						Type:           types.InstanceOperationUpdate,
						TargetRevision: "rev-bad",
					},
				}}
				input, rec := dispositionFixtureInput(fc, &insts, "rev-bad", tc.policy, nil)
				pod := waitingPod("engine-0-default-0", reason, "node-a", t0.Add(-time.Minute))

				outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
					types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, reason)
				if err != nil {
					t.Fatalf("DisposeExpiredAttempt: %v", err)
				}
				if outcome != escalation.DispositionHeldRevision {
					t.Fatalf("outcome: got %v want DispositionHeldRevision", outcome)
				}

				// ONE MutateInstance commit whose mutation did BOTH: Failed + op cleared.
				if len(rec.commits) != 1 {
					t.Fatalf("MutateInstance commits: got %d want 1", len(rec.commits))
				}
				after := rec.commits[0].after
				if after.Phase != types.InstancePhaseFailed {
					t.Errorf("Phase after commit: got %q want Failed", after.Phase)
				}
				if after.Operation != nil {
					t.Errorf("Operation after commit: got %+v want nil (cleared in the same mutation)", after.Operation)
				}
				if after.LastFailure == nil || after.LastFailure.Reason != reason || after.LastFailure.PodName != pod.Name {
					t.Errorf("LastFailure: got %+v want Reason=%s PodName=%s", after.LastFailure, reason, pod.Name)
				}

				// RetryBlock recorded for the attempt's target revision.
				if len(rec.blockCalls) != 1 {
					t.Fatalf("MutateRetryBlock calls: got %d want 1", len(rec.blockCalls))
				}
				bc := rec.blockCalls[0]
				if bc.rev != "rev-bad" || bc.disp != types.RetryBlockPersist {
					t.Errorf("block call: got (rev=%q, disp=%v) want (rev-bad, Persist)", bc.rev, bc.disp)
				}
				if bc.block.State != tc.wantState {
					t.Errorf("block state: got %q want %q", bc.block.State, tc.wantState)
				}
				if bc.block.Reason != reason {
					t.Errorf("block reason: got %q want %q", bc.block.Reason, reason)
				}
				if tc.wantState == types.RetryBlockBackoff {
					if bc.block.NextRetryAt == nil || !bc.block.NextRetryAt.Time.Equal(t0.Add(time.Minute)) {
						t.Errorf("NextRetryAt: got %v want %v", bc.block.NextRetryAt, t0.Add(time.Minute))
					}
				}

				// Invariant: no in-flight Operation targeting the blocked
				// revision coexists with the recorded block.
				for _, s := range insts {
					if s.Operation != nil && s.Operation.TargetRevision == "rev-bad" {
						t.Errorf("Backoff/Held block coexists with in-flight op at rev-bad: %+v", s)
					}
				}

				if len(rec.warns) != 1 || rec.warns[0].pod != pod.Name {
					t.Errorf("WarnInstanceFailed: got %+v want one call naming %s", rec.warns, pod.Name)
				}
			})
		}
	}
}

// TestDispose_WorkloadCaused_CreateFallsBackToUpdateRevision: a Create
// Operation carries no TargetRevision — the RetryBlock lands on the
// owner's UpdateRevision instead.
func TestDispose_WorkloadCaused_CreateFallsBackToUpdateRevision(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index:     0,
		Phase:     types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-update", nil, nil)
	pod := waitingPod("engine-0-default-0", "ImagePullBackOff", "node-a", t0.Add(-time.Minute))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
		types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, "ImagePullBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionHeldRevision {
		t.Fatalf("outcome: got %v want DispositionHeldRevision", outcome)
	}
	if len(rec.blockCalls) != 1 || rec.blockCalls[0].rev != "rev-update" {
		t.Fatalf("block calls: got %+v want one call for rev-update", rec.blockCalls)
	}
	if len(rec.commits) != 1 || rec.commits[0].after.Operation != nil || rec.commits[0].after.Phase != types.InstancePhaseFailed {
		t.Errorf("commit: got %+v want single Failed-no-op commit", rec.commits)
	}
}

func TestDispose_PinnedCreateUsesAttemptRevisionAfterCorrectiveEdit(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationCreate,
			TargetRevision: "own-engine-oldbad",
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "own-engine-newgood", nil, nil)
	pod := waitingPod("engine-0-leader-0", "ImagePullBackOff", "node-a", t0.Add(-time.Minute))
	pod.Labels = map[string]string{"ome.io/revision-hash": "oldbad"}

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
		types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, "ImagePullBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionHeldRevision {
		t.Fatalf("outcome: got %v want DispositionHeldRevision", outcome)
	}
	if len(rec.blockCalls) != 1 || rec.blockCalls[0].rev != "own-engine-oldbad" {
		t.Fatalf("block calls: got %+v want one call for own-engine-oldbad", rec.blockCalls)
	}
	if len(rec.commits) != 1 || rec.commits[0].after.Operation != nil || rec.commits[0].after.Phase != types.InstancePhaseFailed {
		t.Fatalf("commit: got %+v want one Failed-no-operation commit", rec.commits)
	}
}

func TestDispose_UnpinnedCreateDoesNotChargeCorrectiveRevision(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index:     0,
		Phase:     types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "own-engine-newgood", nil, nil)
	pod := waitingPod("engine-0-leader-0", "ImagePullBackOff", "node-a", t0.Add(-time.Minute))
	pod.Labels = map[string]string{"ome.io/revision-hash": "oldbad"}

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
		types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, "ImagePullBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal {
		t.Fatalf("outcome: got %v want DispositionTerminal", outcome)
	}
	if len(rec.blockCalls) != 0 {
		t.Fatalf("corrective revision must not be charged; got %+v", rec.blockCalls)
	}
	if len(rec.commits) != 1 || rec.commits[0].after.Operation != nil || rec.commits[0].after.Phase != types.InstancePhaseFailed {
		t.Fatalf("commit: got %+v want one Failed-no-operation commit", rec.commits)
	}
	if rec.commits[0].after.LastFailure == nil || rec.commits[0].after.LastFailure.PodName != pod.Name {
		t.Fatalf("LastFailure: got %+v want diagnostics for %s", rec.commits[0].after.LastFailure, pod.Name)
	}
}

// TestDispose_SupersededLeftover_DoesNotPoisonCorrectiveRevision pins the
// superseded-leftover guard: after a corrective edit retargets an instance to a new
// revision, the prior failed attempt's bad pod can linger in its drain
// window. Attributing that pod's failure to the freshly-stamped
// Operation.TargetRevision would record a RetryBlock against the GOOD
// corrective revision and wedge recovery. A cause pod whose revision-hash
// is not the target's must be skipped: no block, no instance mutation.
func TestDispose_SupersededLeftover_DoesNotPoisonCorrectiveRevision(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			TargetRevision: "own-engine-newgood",
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "own-engine-newgood", nil, nil)
	pod := waitingPod("engine-0-default-0", "ImagePullBackOff", "node-a", t0.Add(-time.Minute))
	pod.Labels = map[string]string{"ome.io/revision-hash": "oldbad"} // prior bad rev

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
		types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, "ImagePullBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionSkippedSuperseded {
		t.Fatalf("outcome: got %v want DispositionSkippedSuperseded", outcome)
	}
	if len(rec.blockCalls) != 0 {
		t.Fatalf("no RetryBlock must be recorded for a superseded leftover; got %+v", rec.blockCalls)
	}
	if len(rec.commits) != 0 {
		t.Fatalf("no instance mutation must occur (Operation stays intact); got %+v", rec.commits)
	}
}

// TestDispose_TargetRevisionPod_StillHeld guards the non-poisoning case:
// when the stuck pod actually belongs to the current target revision, the
// block is still recorded against it (the guard must not over-fire), and a
// label-less pod (unknown revision) also still records — the guard is inert
// unless the pod is provably on a different revision.
func TestDispose_TargetRevisionPod_StillHeld(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			TargetRevision: "own-engine-newgood",
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "own-engine-newgood", nil, nil)
	pod := waitingPod("engine-0-default-0", "ImagePullBackOff", "node-a", t0.Add(-time.Minute))
	pod.Labels = map[string]string{"ome.io/revision-hash": "newgood"} // belongs to target

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
		types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, "ImagePullBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionHeldRevision {
		t.Fatalf("outcome: got %v want DispositionHeldRevision", outcome)
	}
	if len(rec.blockCalls) != 1 || rec.blockCalls[0].rev != "own-engine-newgood" {
		t.Fatalf("block calls: got %+v want one call for own-engine-newgood", rec.blockCalls)
	}
}

// TestDispose_RelocationDirective_GPUCase covers the deliberate
// GPU-on-Ready-node handling: CrashLoopBackOff is NOT workload-caused
// evidence, so a single-node Auto-mode attempt with budget records a
// TERMINAL AutoRecover directive (Phase=Completed,
// Outcome=relocate-recreate) AND applies the unconditional terminal
// backstop (Op cleared + Failed) in the same flow — mover, not copier.
// RetryBlocks are never touched. Repeated dispositions consume the
// budget one entry at a time; the over-budget call disposes terminal
// with no new entry (cap-event cadence is pinned separately in
// TestDispose_RelocationDirective_CapEventDampedToTransition).
func TestDispose_RelocationDirective_GPUCase(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	owner := ledgerOwnerCM()
	deps := types.Deps{Client: c, Clock: fc}
	var directives []string
	dd := types.DispositionDeps{
		AutoMigrateMaxAttempts: 3,
		MigrationMode:          types.MigrationModeAuto,
		OnRelocationDirective:  func(component string) { directives = append(directives, component) },
	}
	pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))

	newAttempt := func() ([]types.InstanceStatus, types.ReconcileInput, *dispositionRecorders) {
		insts := []types.InstanceStatus{{
			Index: 0,
			Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				TargetRevision: "rev-x",
			},
		}}
		input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, owner)
		return insts, input, rec
	}

	insts, input, rec := newAttempt()
	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("outcome: got %v want DispositionRelocationDirective", outcome)
	}

	// Terminal backstop applied in the same flow: ONE commit doing
	// Failed + op-clear, one Warning, and NO RetryBlock write.
	if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
		t.Fatalf("commit: got %+v want single Failed-no-op commit (backstop unconditional)", rec.commits)
	}
	if len(rec.blockCalls) != 0 {
		t.Errorf("MutateRetryBlock calls: got %+v want none (relocation never touches a RetryBlock)", rec.blockCalls)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %+v want one call", rec.warns)
	}
	if len(directives) != 1 || directives[0] != "engine" {
		t.Errorf("OnRelocationDirective: got %v want one call for engine", directives)
	}

	// The directive ledger entry, field for field — TERMINAL record.
	ledger := loadLedger(t, c)
	if len(ledger.Entries) != 1 {
		t.Fatalf("ledger entries: got %d want 1", len(ledger.Entries))
	}
	e := ledger.Entries[0]
	if e.RequestUUID == "" {
		t.Errorf("RequestUUID: empty, want a generated uuid")
	}
	if e.Component != "engine" || e.SourceInstance != 0 {
		t.Errorf("entry identity: got component=%q instance=%d want engine/0", e.Component, e.SourceInstance)
	}
	if e.Phase != audit.PhaseCompleted || e.Reason != audit.ReasonAutoRecover {
		t.Errorf("entry: got phase=%q reason=%q want Completed/AutoRecover (directive is a record, not a work order)", e.Phase, e.Reason)
	}
	if e.Outcome != audit.OutcomeRelocateRecreate {
		t.Errorf("Outcome: got %q want %q", e.Outcome, audit.OutcomeRelocateRecreate)
	}
	if e.FromNode != "node-a" {
		t.Errorf("FromNode: got %q want node-a", e.FromNode)
	}
	if want := t0.UTC().Format(time.RFC3339); e.StartedAt != want || e.CompletedAt != want {
		t.Errorf("timestamps: got started=%q completed=%q want both %q", e.StartedAt, e.CompletedAt, want)
	}

	// Two more attempts consume the rest of the budget (entries are
	// terminal — no in-flight dedup; each disposition is one attempt).
	for i := 2; i <= 3; i++ {
		insts, input, _ = newAttempt()
		outcome, err = escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
		if err != nil {
			t.Fatalf("DisposeExpiredAttempt (attempt %d): %v", i, err)
		}
		if outcome != escalation.DispositionRelocationDirective {
			t.Fatalf("attempt %d outcome: got %v want DispositionRelocationDirective", i, outcome)
		}
	}
	if got := len(loadLedger(t, c).Entries); got != 3 {
		t.Fatalf("ledger entries after 3 attempts: got %d want 3", got)
	}

	// Fourth attempt: budget exhausted → terminal, no new entry.
	insts, input, rec = newAttempt()
	outcome, err = escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt (over budget): %v", err)
	}
	if outcome != escalation.DispositionTerminal {
		t.Fatalf("over-budget outcome: got %v want DispositionTerminal", outcome)
	}
	if got := len(loadLedger(t, c).Entries); got != 3 {
		t.Errorf("ledger entries after over-budget call: got %d want 3 (no new entry)", got)
	}
	if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
		t.Errorf("over-budget commit: got %+v want single Failed-no-op commit", rec.commits)
	}
	if len(directives) != 3 {
		t.Errorf("OnRelocationDirective calls: got %d want 3 (not fired over budget)", len(directives))
	}
}

// TestDispose_RelocationDirective_MirrorsStatusRecord: branch 2 writes
// the born-terminal Auto visibility mirror through AppendMigration
// AFTER the ledger persist — one record per directive, uuid matching
// the ledger row, Attempt matching the ledger's attempt count, and the
// born-terminal shape: Phase=Relocated, StartedAt=CompletedAt=now,
// Deadline stamped = now (required field, no deadline semantics),
// Succeeded unset at birth.
func TestDispose_RelocationDirective_MirrorsStatusRecord(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	owner := ledgerOwnerCM()
	deps := types.Deps{Client: c, Clock: fc}
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}
	pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))

	var appended []types.MigrationRecord
	dispose := func() escalation.DispositionOutcome {
		insts := []types.InstanceStatus{{
			Index:     0,
			Phase:     types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, TargetRevision: "rev-x"},
		}}
		input, _ := dispositionFixtureInput(fc, &insts, "rev-x", nil, owner)
		input.AppendMigration = func(_ context.Context, rec types.MigrationRecord) error {
			appended = append(appended, rec)
			return nil
		}
		outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
		if err != nil {
			t.Fatalf("DisposeExpiredAttempt: %v", err)
		}
		return outcome
	}

	if outcome := dispose(); outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("outcome: got %v want DispositionRelocationDirective", outcome)
	}
	if len(appended) != 1 {
		t.Fatalf("AppendMigration calls: got %d want 1", len(appended))
	}
	rec := appended[0]
	ledger := loadLedger(t, c)
	if len(ledger.Entries) != 1 || rec.RequestUUID != ledger.Entries[0].RequestUUID {
		t.Errorf("record uuid: got %q want the ledger directive's %q", rec.RequestUUID, ledger.Entries[0].RequestUUID)
	}
	if rec.Trigger != types.MigrationTriggerAuto || rec.Phase != types.MigrationPhaseRelocated {
		t.Errorf("record: got trigger=%q phase=%q want Auto/Relocated (born terminal)", rec.Trigger, rec.Phase)
	}
	if rec.SourceInstance != 0 || rec.FromNode != "node-a" {
		t.Errorf("record identity: got instance=%d fromNode=%q want 0/node-a", rec.SourceInstance, rec.FromNode)
	}
	if rec.Attempt != 1 {
		t.Errorf("Attempt: got %d want 1 (CountAutoRecoverAttempts before the write + 1)", rec.Attempt)
	}
	if rec.Reason != audit.ReasonAutoRecover {
		t.Errorf("Reason: got %q want %q", rec.Reason, audit.ReasonAutoRecover)
	}
	if !rec.StartedAt.Time.Equal(t0) || rec.CompletedAt == nil || !rec.CompletedAt.Time.Equal(t0) {
		t.Errorf("timestamps: got started=%v completed=%v want both %v", rec.StartedAt, rec.CompletedAt, t0)
	}
	if !rec.Deadline.Time.Equal(t0) {
		t.Errorf("Deadline: got %v want %v (stamped = now; no deadline semantics on a born-terminal record)", rec.Deadline, t0)
	}
	if rec.Succeeded != nil {
		t.Errorf("Succeeded: got %v want nil at birth (stamped post-hoc on Ready)", *rec.Succeeded)
	}
	if rec.SurgeInstance != nil || rec.AllocatedAt != nil {
		t.Errorf("record: got surge=%v allocatedAt=%v want both nil (Auto never allocates — capacity stays blind)", rec.SurgeInstance, rec.AllocatedAt)
	}

	// Second attempt: Attempt tracks the ledger count.
	fc.SetTime(t0.Add(5 * time.Minute))
	if outcome := dispose(); outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("attempt 2 outcome: got %v want DispositionRelocationDirective", outcome)
	}
	if len(appended) != 2 || appended[1].Attempt != 2 {
		t.Fatalf("attempt 2 record: got %+v want a second record with Attempt=2", appended)
	}
}

// TestDispose_RelocationDirective_RecordMirrorFailureTolerated: the
// status record is a mirror — a failed AppendMigration must NOT fail
// the disposition. The ledger directive (the exclusion-memory
// authority) persists, the outcome stays RelocationDirective, and the
// terminal backstop completes.
func TestDispose_RelocationDirective_RecordMirrorFailureTolerated(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	insts := []types.InstanceStatus{{
		Index:     0,
		Phase:     types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, TargetRevision: "rev-x"},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
	input.AppendMigration = func(_ context.Context, _ types.MigrationRecord) error {
		return fmt.Errorf("apiserver unavailable")
	}
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}
	pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc}, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v (a mirror failure must not fail the disposition)", err)
	}
	if outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("outcome: got %v want DispositionRelocationDirective", outcome)
	}
	// The ledger row landed — the exclusion overlay is unaffected.
	ledger := loadLedger(t, c)
	if len(ledger.Entries) != 1 || ledger.Entries[0].Reason != audit.ReasonAutoRecover || ledger.Entries[0].FromNode != "node-a" {
		t.Fatalf("ledger: got %+v want the directive row despite the mirror failure", ledger.Entries)
	}
	if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
		t.Errorf("commit: got %+v want single Failed-no-op commit", rec.commits)
	}
}

// drainEvents empties the FakeRecorder channel into a slice.
func drainEvents(recorder *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-recorder.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func countEventsWithReason(events []string, reason types.EventReason) int {
	n := 0
	for _, ev := range events {
		if strings.Contains(ev, string(reason)) {
			n++
		}
	}
	return n
}

// TestDispose_RelocationDirective_CapEventDampedToTransition pins the
// cap-event cadence: AutoMigrationCapReached fires exactly ONCE, when
// the directive filling the final budget slot is recorded. Post-cap
// dispose cycles keep disposing terminal (no new entry) but stay silent
// — no per-cycle re-warning.
func TestDispose_RelocationDirective_CapEventDampedToTransition(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	owner := ledgerOwnerCM()
	recorder := record.NewFakeRecorder(16)
	deps := types.Deps{Client: c, Clock: fc, Recorder: recorder}
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 2, MigrationMode: types.MigrationModeAuto}

	dispose := func(opStartedAt time.Time, node string) (escalation.DispositionOutcome, *dispositionRecorders) {
		insts := []types.InstanceStatus{{
			Index: 0,
			Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				TargetRevision: "rev-x",
				StartedAt:      metav1.NewTime(opStartedAt),
			},
		}}
		input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, owner)
		pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", node, opStartedAt)
		outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
		if err != nil {
			t.Fatalf("DisposeExpiredAttempt: %v", err)
		}
		return outcome, rec
	}

	// Attempt 1 (budget 1/2): directive recorded, no cap event yet.
	outcome, _ := dispose(t0.Add(-10*time.Minute), "node-a")
	if outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("attempt 1 outcome: got %v want DispositionRelocationDirective", outcome)
	}
	events := drainEvents(recorder)
	if got := countEventsWithReason(events, types.EventReasonAutoMigrationCapReached); got != 0 {
		t.Errorf("cap events after attempt 1: got %d want 0 (%v)", got, events)
	}

	// Attempt 2 fills the final slot → Triggered AND exactly one
	// cap-reached warning (the transition).
	fc.SetTime(t0.Add(5 * time.Minute))
	outcome, _ = dispose(t0.Add(2*time.Minute), "node-b")
	if outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("attempt 2 outcome: got %v want DispositionRelocationDirective", outcome)
	}
	events = drainEvents(recorder)
	if got := countEventsWithReason(events, types.EventReasonAutoMigrationTriggered); got != 1 {
		t.Errorf("triggered events after attempt 2: got %d want 1 (%v)", got, events)
	}
	if got := countEventsWithReason(events, types.EventReasonAutoMigrationCapReached); got != 1 {
		t.Errorf("cap events after attempt 2: got %d want 1 (%v)", got, events)
	}

	// Attempts 3..4: over budget. Terminal, no new entry, and SILENT —
	// the damping under test.
	for i, start := range []time.Time{t0.Add(7 * time.Minute), t0.Add(12 * time.Minute)} {
		fc.SetTime(start.Add(3 * time.Minute))
		outcome, rec := dispose(start, "node-c")
		if outcome != escalation.DispositionTerminal {
			t.Fatalf("post-cap attempt %d outcome: got %v want DispositionTerminal", i+3, outcome)
		}
		if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
			t.Errorf("post-cap commit: got %+v want single Failed-no-op commit", rec.commits)
		}
		events = drainEvents(recorder)
		if len(events) != 0 {
			t.Errorf("post-cap attempt %d events: got %v want none (cap announced only at the transition)", i+3, events)
		}
	}
	if got := len(loadLedger(t, c).Entries); got != 2 {
		t.Errorf("ledger entries: got %d want 2 (post-cap disposals record nothing)", got)
	}
}

// TestDispose_RelocationDirective_ReplayDedup: a directive persisted on
// a prior pass that crashed (or lost a stale-cache race) before the
// op-clear landed must NOT be re-recorded when the disposition re-runs.
// The replay is detected by the newest AutoRecover entry carrying the
// same FromNode with a CompletedAt newer than the Operation's StartedAt
// — that entry IS this attempt's directive. The re-run completes the
// op-clear + Failed backstop with no second entry, event, or metric.
func TestDispose_RelocationDirective_ReplayDedup(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	owner := ledgerOwnerCM()

	// The prior pass's directive: completed at t0, AFTER the op began.
	seeded := &audit.Ledger{}
	seeded.UpsertEntry(audit.Entry{
		RequestUUID: "u-replay", Component: "engine", SourceInstance: 0,
		Phase: audit.PhaseCompleted, Reason: audit.ReasonAutoRecover,
		Outcome: audit.OutcomeRelocateRecreate, FromNode: "node-a",
		StartedAt:   t0.UTC().Format(time.RFC3339),
		CompletedAt: t0.UTC().Format(time.RFC3339),
	})
	if err := audit.PersistLedgerForOwner(context.Background(), c, owner, corev1.SchemeGroupVersion.WithKind("ConfigMap"), seeded); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	// The op is still present (the crash window) and predates the entry.
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			TargetRevision: "rev-x",
			StartedAt:      metav1.NewTime(t0.Add(-5 * time.Minute)),
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, owner)
	appendCalls := 0
	input.AppendMigration = func(_ context.Context, _ types.MigrationRecord) error {
		appendCalls++
		return nil
	}
	recorder := record.NewFakeRecorder(8)
	var directives []string
	dd := types.DispositionDeps{
		AutoMigrateMaxAttempts: 3,
		MigrationMode:          types.MigrationModeAuto,
		OnRelocationDirective:  func(component string) { directives = append(directives, component) },
	}
	fc.SetTime(t0.Add(time.Minute))
	pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Hour))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("outcome: got %v want DispositionRelocationDirective (replay counts as recorded)", outcome)
	}
	// Exactly one entry — the replayed write is deduped.
	ledger := loadLedger(t, c)
	if len(ledger.Entries) != 1 || ledger.Entries[0].RequestUUID != "u-replay" {
		t.Fatalf("ledger: got %+v want the single seeded entry (no duplicate)", ledger.Entries)
	}
	// The op-clear + Failed backstop still completes.
	if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
		t.Errorf("commit: got %+v want single Failed-no-op commit", rec.commits)
	}
	// No duplicate side effects: no Triggered event, no metric callback.
	if events := drainEvents(recorder); len(events) != 0 {
		t.Errorf("events: got %v want none on replay", events)
	}
	if len(directives) != 0 {
		t.Errorf("OnRelocationDirective: got %v want none on replay", directives)
	}
	if appendCalls != 0 {
		t.Errorf("AppendMigration calls: got %d want 0 (a replay writes no second status record)", appendCalls)
	}
}

// TestDispose_RelocationDirective_AffinityPinDisposesTerminal: an
// instance whose template REQUIRES the suspect node (required
// In[node-a] pin) must not receive a relocation directive — the
// exclusion would render In[node-a] AND NotIn[node-a] permanently
// Pending. The disposition takes the terminal branch instead. A pin
// that still permits another host records normally.
func TestDispose_RelocationDirective_AffinityPinDisposesTerminal(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	pinnedSpec := func(hosts ...string) *corev1.PodSpec {
		return &corev1.PodSpec{
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: hosts,
							}},
						}},
					},
				},
			},
		}
	}
	for _, tc := range []struct {
		name        string
		spec        *corev1.PodSpec
		wantOutcome escalation.DispositionOutcome
		wantEntries int
	}{
		{name: "pin on suspect node disposes terminal", spec: pinnedSpec("node-a"),
			wantOutcome: escalation.DispositionTerminal, wantEntries: 0},
		{name: "pin permitting another host records", spec: pinnedSpec("node-a", "node-b"),
			wantOutcome: escalation.DispositionRelocationDirective, wantEntries: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := clocktesting.NewFakeClock(t0)
			c := fakeLedgerClient(t)
			insts := []types.InstanceStatus{{
				Index:     0,
				Phase:     types.InstancePhaseUpdating,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, TargetRevision: "rev-x"},
			}}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
			dd := types.DispositionDeps{
				AutoMigrateMaxAttempts: 3,
				MigrationMode:          types.MigrationModeAuto,
				PodSpec:                tc.spec,
			}
			pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))

			outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc}, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
			if err != nil {
				t.Fatalf("DisposeExpiredAttempt: %v", err)
			}
			if outcome != tc.wantOutcome {
				t.Fatalf("outcome: got %v want %v", outcome, tc.wantOutcome)
			}
			if got := len(loadLedger(t, c).Entries); got != tc.wantEntries {
				t.Errorf("ledger entries: got %d want %d", got, tc.wantEntries)
			}
			if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
				t.Errorf("commit: got %+v want single Failed-no-op commit", rec.commits)
			}
			if len(rec.blockCalls) != 0 {
				t.Errorf("MutateRetryBlock calls: got %+v want none", rec.blockCalls)
			}
		})
	}
}

// TestDispose_Terminal covers the branch-3 gates: Mode=Never, a
// multi-NODE attempt (no single suspect node to record — relocation
// covers single-pod instances and single-host gangs only), and no
// resolvable node at all. All clear the Operation + stamp Failed with
// NO RetryBlock and NO ledger entry.
func TestDispose_Terminal(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	splitGang := []*corev1.Pod{
		waitingPod("engine-0-leader-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute)),
		waitingPod("engine-0-worker-0", "CrashLoopBackOff", "node-b", t0.Add(-time.Minute)),
	}
	for _, tc := range []struct {
		name string
		dd   types.DispositionDeps
		pods []*corev1.Pod
	}{
		{name: "mode Never",
			dd:   types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeNever},
			pods: []*corev1.Pod{waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))}},
		{name: "multi-node gang",
			dd:   types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto},
			pods: splitGang},
		{name: "no resolvable node",
			dd:   types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto},
			pods: []*corev1.Pod{waitingPod("engine-0-default-0", "CrashLoopBackOff", "", t0.Add(-time.Minute))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := clocktesting.NewFakeClock(t0)
			c := fakeLedgerClient(t)
			insts := []types.InstanceStatus{{
				Index:     0,
				Phase:     types.InstancePhaseCreating,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate},
			}}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
			deps := types.Deps{Client: c, Clock: fc}

			outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, tc.dd, insts[0], tc.pods, "CrashLoopBackOff")
			if err != nil {
				t.Fatalf("DisposeExpiredAttempt: %v", err)
			}
			if outcome != escalation.DispositionTerminal {
				t.Fatalf("outcome: got %v want DispositionTerminal", outcome)
			}
			if len(rec.blockCalls) != 0 {
				t.Errorf("MutateRetryBlock calls: got %+v want none", rec.blockCalls)
			}
			if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
				t.Errorf("commit: got %+v want single Failed-no-op commit", rec.commits)
			}
			if got := len(loadLedger(t, c).Entries); got != 0 {
				t.Errorf("ledger entries: got %d want 0 (terminal branch files nothing)", got)
			}
			if len(rec.warns) != 1 {
				t.Errorf("WarnInstanceFailed: got %+v want one call", rec.warns)
			}
		})
	}
}

// TestDispose_WorkloadCaused_NoResolvableRevision_Terminal: a
// workload-caused reason with NO resolvable target revision (no
// Operation.TargetRevision, empty UpdateRevision) has nothing to hold —
// the disposition falls through to terminal instead of reporting a held
// revision with no block.
func TestDispose_WorkloadCaused_NoResolvableRevision_Terminal(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index:     0,
		Phase:     types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "" /* no UpdateRevision */, nil, nil)
	pod := waitingPod("engine-0-default-0", "ImagePullBackOff", "node-a", t0.Add(-time.Minute))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
		types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, "ImagePullBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal {
		t.Fatalf("outcome: got %v want DispositionTerminal (no revision to hold)", outcome)
	}
	if len(rec.blockCalls) != 0 {
		t.Errorf("MutateRetryBlock calls: got %+v want none", rec.blockCalls)
	}
	if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
		t.Errorf("commit: got %+v want single Failed-no-op commit", rec.commits)
	}
}

// TestBuildPlan_ThreadsExcludedNodes pins the exclusion projection:
// ObservedState.ExcludedNodesByInstance lands on the matching
// InstancePlan.ExcludedNodes (and only there) for Render to apply.
func TestBuildPlan_ThreadsExcludedNodes(t *testing.T) {
	desired := types.WorkloadDesiredSpec{Replicas: 2}
	observed := types.WorkloadObservedState{
		ExcludedNodesByInstance: map[int32][]string{1: {"node-bad"}},
	}
	plan, err := workload.BuildPlan(types.ComponentEngine, desired, observed)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	for _, inst := range plan.Instances {
		switch inst.Index {
		case 1:
			if len(inst.ExcludedNodes) != 1 || inst.ExcludedNodes[0] != "node-bad" {
				t.Errorf("instance 1 ExcludedNodes: got %v want [node-bad]", inst.ExcludedNodes)
			}
		default:
			if len(inst.ExcludedNodes) != 0 {
				t.Errorf("instance %d ExcludedNodes: got %v want empty", inst.Index, inst.ExcludedNodes)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Escalator integration: the fast stuck-pod escalator and the deadline
// backstop route disposable attempts through the disposition (not the
// old bare stamps), while the gang path keeps today's Failed-with-Op
// behavior (pinned in deadline_test.go / escalation_external_test.go).
// ---------------------------------------------------------------------------

// TestEscalateStuckPodFailures_DisposesSinglePodWorkloadCaused: fast
// escalator + single-pod Update op + ImagePullBackOff past grace →
// branch 1: RetryBlock recorded AND Operation cleared AND Phase=Failed,
// instead of the legacy Failed-preserving-Operation stamp.
func TestEscalateStuckPodFailures_DisposesSinglePodWorkloadCaused(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           "Drain",
			TargetRevision: "rev-bad",
		},
	}}
	policy := &types.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-bad", policy, nil)
	input.ObservedState.InstanceStatuses = insts
	input.StuckPodGrace = time.Second
	pod := waitingPod("engine-0-default-0", "ImagePullBackOff", "node-a", t0.Add(-time.Minute))

	err := runEscalationPass(t, types.Deps{Clock: fc}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {pod}})
	if err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", insts[0].Phase)
	}
	if insts[0].Operation != nil {
		t.Errorf("Operation: got %+v want nil (disposition clears the failed attempt)", insts[0].Operation)
	}
	if len(rec.blockCalls) != 1 || rec.blockCalls[0].rev != "rev-bad" || rec.blockCalls[0].block.State != types.RetryBlockBackoff {
		t.Errorf("RetryBlock: got %+v want one Backoff block for rev-bad", rec.blockCalls)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %+v want one call", rec.warns)
	}
}

// TestExpireOperations_SinglePodCrashLoopRecordsDirective: deadline
// expiry + crash-loop pod + Mode=Auto + budget → branch 2: a TERMINAL
// AutoRecover directive is recorded AND the instance is disposed
// Failed-with-no-Operation in the same pass (the backstop is
// unconditional — no more pend-forever wedge).
func TestExpireOperations_SinglePodCrashLoopRecordsDirective(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           "Drain",
			TargetRevision: "rev-x",
			Deadline:       metav1.NewTime(t0.Add(-time.Minute)),
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
	input.ObservedState.InstanceStatuses = insts
	input.Disposition = types.DispositionDeps{AutoMigrateMaxAttempts: 3}
	pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Hour))

	plan := singleInstancePlan(0, 1)
	plan.MigrationMode = types.MigrationModeAuto
	err := runEscalationPass(t, types.Deps{Client: c, Clock: fc}, input, plan,
		map[int32][]*corev1.Pod{0: {pod}})
	if err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != types.InstancePhaseFailed || insts[0].Operation != nil {
		t.Errorf("instance: got %+v want Failed-no-op (unconditional backstop)", insts[0])
	}
	if len(rec.blockCalls) != 0 {
		t.Errorf("MutateRetryBlock calls: got %+v want none", rec.blockCalls)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %+v want one call", rec.warns)
	}
	ledger := loadLedger(t, c)
	if len(ledger.Entries) != 1 ||
		ledger.Entries[0].Reason != audit.ReasonAutoRecover ||
		ledger.Entries[0].Phase != audit.PhaseCompleted ||
		ledger.Entries[0].Outcome != audit.OutcomeRelocateRecreate ||
		ledger.Entries[0].FromNode != "node-a" {
		t.Fatalf("ledger: got %+v want one terminal relocate-recreate AutoRecover directive for node-a", ledger.Entries)
	}
}

// TestExpireOperations_GangCreateDisposed documents the gang-CREATE
// decision: creates have no abandon path, so a multi-pod Create expiry
// with workload-caused evidence routes through the disposition —
// RetryBlock on the owner's UpdateRevision + Operation cleared + Failed
// (a Failed-no-Operation gang create rebuilds via the ordinary trigger
// through the RetryBlock gate).
func TestExpireOperations_GangCreateDisposed(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationCreate,
			Deadline: metav1.NewTime(t0.Add(-time.Minute)),
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-update", nil, nil)
	input.ObservedState.InstanceStatuses = insts
	leader := waitingPod("engine-0-leader-0", "ImagePullBackOff", "node-a", t0.Add(-time.Hour))

	err := runEscalationPass(t, types.Deps{Clock: fc}, input, singleInstancePlan(0, 3),
		map[int32][]*corev1.Pod{0: {leader}})
	if err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != types.InstancePhaseFailed || insts[0].Operation != nil {
		t.Errorf("instance: got %+v want Failed-no-op", insts[0])
	}
	if len(rec.blockCalls) != 1 || rec.blockCalls[0].rev != "rev-update" {
		t.Errorf("RetryBlock: got %+v want one block for rev-update (Create fallback target)", rec.blockCalls)
	}
}

// TestDispose_Terminal_SuppressesRepeatWarnForSameFailure: an instance stuck
// oscillating on the SAME terminal reason (e.g. a same-target update whose
// new-revision pod persistently CrashLoopBackOffs) must warn once, not on
// every disposition. The warn is keyed on the prior LastFailure reason.
func TestDispose_Terminal_SuppressesRepeatWarnForSameFailure(t *testing.T) {
	t0 := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	owner := ledgerOwnerCM()
	deps := types.Deps{Client: c, Clock: fc}
	// MigrationMode unset → no relocation → pure Terminal backstop path.
	dd := types.DispositionDeps{}
	pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))

	dispose := func(prior *types.InstanceTermination) *dispositionRecorders {
		insts := []types.InstanceStatus{{
			Index:       0,
			Phase:       types.InstancePhaseUpdating,
			Operation:   &types.InstanceOperation{Type: types.InstanceOperationUpdate, TargetRevision: "rev-x"},
			LastFailure: prior,
		}}
		input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, owner)
		if _, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff"); err != nil {
			t.Fatalf("DisposeExpiredAttempt: %v", err)
		}
		return rec
	}

	// First disposition (no prior failure) → warn fires once.
	if rec := dispose(nil); len(rec.warns) != 1 {
		t.Fatalf("first dispose: got %d warns want 1", len(rec.warns))
	}
	// Repeat with the SAME prior terminal reason → suppressed (debounced).
	if rec := dispose(&types.InstanceTermination{Reason: "CrashLoopBackOff"}); len(rec.warns) != 0 {
		t.Errorf("repeat dispose of same CrashLoopBackOff failure: got %d warns want 0 (debounced)", len(rec.warns))
	}
	// A DIFFERENT prior reason is a genuinely new failure → NOT suppressed.
	if rec := dispose(&types.InstanceTermination{Reason: "OOMKilled"}); len(rec.warns) != 1 {
		t.Errorf("dispose with different prior reason: got %d warns want 1 (not debounced)", len(rec.warns))
	}
}

// runningNotReadyOnNode builds the readiness-limbo shape the KIND
// auto-migrate fixture produces: phase Running on a named node, every
// container started, and a ContainersReady=False the kubelet never
// flips because the fault is node-scoped rather than a waiting reason.
func runningNotReadyOnNode(name, node string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.ContainersReady,
				Status:             corev1.ConditionFalse,
				Reason:             "ContainersNotReady",
				Message:            "containers with unready status: [ome-container]",
				LastTransitionTime: metav1.NewTime(since),
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "ome-container",
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// TestDispose_RunningNotReadyRelocatesFirst pins the ordering a
// node-scoped fault depends on: a pod that runs yet never reports ready
// is AMBIGUOUS between the revision and the node it landed on, so a
// single-node Auto attempt with budget records a relocation directive
// and blames no revision. The readiness detail is EVIDENCE — it names
// the container on the failure record and travels with the directive —
// and must never promote the attempt into the workload-caused branch,
// which would hold the revision and leave the suspect node in service.
func TestDispose_RunningNotReadyRelocatesFirst(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	deps := types.Deps{Client: fakeLedgerClient(t), Clock: fc}
	dd := types.DispositionDeps{
		AutoMigrateMaxAttempts: 3,
		MigrationMode:          types.MigrationModeAuto,
	}
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationCreate,
			TargetRevision: "rev-x",
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
	pod := runningNotReadyOnNode("engine-0-default-0", "node-a", t0.Add(-time.Minute))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0],
		[]*corev1.Pod{pod}, "DeadlineExceeded: Create/CreatePods exceeded InstanceReadyTimeout")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("outcome: got %v want DispositionRelocationDirective (relocation gets the first try)", outcome)
	}
	if len(rec.blockCalls) != 0 {
		t.Errorf("MutateRetryBlock calls: got %+v want none (the node is the suspect, not the revision)", rec.blockCalls)
	}
	if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed {
		t.Fatalf("commit: got %+v want a single Failed commit", rec.commits)
	}
	got := rec.commits[0].after.LastFailure
	if got == nil || got.Reason != "ContainersNotReady" {
		t.Fatalf("LastFailure.Reason: got %+v want ContainersNotReady", got)
	}
	if got.ContainerName != "ome-container" {
		t.Errorf("LastFailure.ContainerName: got %q want the unready container", got.ContainerName)
	}
	if !strings.Contains(got.Message, "containers with unready status") {
		t.Errorf("LastFailure.Message: got %q want the readiness condition message", got.Message)
	}
}

// TestDispose_UnschedulableCannotSuppressRelocation: an unplaceable pod
// was never assigned a node, so it can name no suspect to steer off.
// The scheduler hold therefore disposes terminal rather than consuming
// a relocation attempt on a placement that never happened.
func TestDispose_UnschedulableCannotSuppressRelocation(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	deps := types.Deps{Client: fakeLedgerClient(t), Clock: fc}
	dd := types.DispositionDeps{
		AutoMigrateMaxAttempts: 3,
		MigrationMode:          types.MigrationModeAuto,
	}
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationCreate,
			TargetRevision: "rev-x",
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             "Unschedulable",
			Message:            "0/3 nodes are available: 3 Insufficient nvidia.com/gpu",
			LastTransitionTime: metav1.NewTime(t0.Add(-time.Hour)),
		}}},
	}

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0],
		[]*corev1.Pod{pod}, "Unschedulable: 0/3 nodes are available: 3 Insufficient nvidia.com/gpu")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal {
		t.Errorf("outcome: got %v want DispositionTerminal (no node to steer off)", outcome)
	}
	if len(rec.blockCalls) != 0 {
		t.Errorf("MutateRetryBlock calls: got %+v want none", rec.blockCalls)
	}
	if got := rec.commits[0].after.LastFailure; got == nil || got.Reason != "Unschedulable" {
		t.Errorf("LastFailure.Reason: got %+v want Unschedulable", got)
	}
}
