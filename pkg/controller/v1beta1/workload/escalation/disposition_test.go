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
//     the recorded node by the render NotIn overlay. A crash loop never
//     takes it, under either face: a container that keeps exiting is the
//     workload's fault wherever it runs.
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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
	// The observation the pass judges on carries every row, the way the
	// adapter's snapshot does; the disposition reads sibling rows from it.
	input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), (*insts)...)
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
// GPU-on-Ready-node handling: a runtime start rejection
// (RunContainerError) is NOT workload-caused evidence and may be the
// node's, so a single-node Auto-mode attempt with budget records a
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
	pod := waitingPod("engine-0-default-0", "RunContainerError", "node-a", t0.Add(-time.Minute))

	newAttempt := func() ([]types.InstanceStatus, types.ReconcileInput, *dispositionRecorders) {
		// A sibling serves rev-x, so the crash on node-a is the node's
		// likely fault rather than the revision's.
		insts := []types.InstanceStatus{{
			Index: 0,
			Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				TargetRevision: "rev-x",
			},
		}, servingRowOn(1, "rev-x")}
		input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, owner)
		return insts, input, rec
	}

	insts, input, rec := newAttempt()
	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
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
	if e.Revision != "rev-x" {
		t.Errorf("Revision: got %q want rev-x (the exclusion binds the attempt's revision only)", e.Revision)
	}
	if want := t0.UTC().Format(time.RFC3339); e.StartedAt != want || e.CompletedAt != want {
		t.Errorf("timestamps: got started=%q completed=%q want both %q", e.StartedAt, e.CompletedAt, want)
	}

	// Two more attempts consume the rest of the budget (entries are
	// terminal — no in-flight dedup; each disposition is one attempt).
	for i := 2; i <= 3; i++ {
		insts, input, _ = newAttempt()
		outcome, err = escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
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
	outcome, err = escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
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
	pod := waitingPod("engine-0-default-0", "RunContainerError", "node-a", t0.Add(-time.Minute))

	var appended []types.MigrationRecord
	dispose := func() escalation.DispositionOutcome {
		insts := []types.InstanceStatus{{
			Index:     0,
			Phase:     types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, TargetRevision: "rev-x"},
		}, servingRowOn(1, "rev-x")}
		input, _ := dispositionFixtureInput(fc, &insts, "rev-x", nil, owner)
		input.AppendMigration = func(_ context.Context, rec types.MigrationRecord) error {
			appended = append(appended, rec)
			return nil
		}
		outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
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
	}, servingRowOn(1, "rev-x")}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
	input.AppendMigration = func(_ context.Context, _ types.MigrationRecord) error {
		return fmt.Errorf("apiserver unavailable")
	}
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}
	pod := waitingPod("engine-0-default-0", "RunContainerError", "node-a", t0.Add(-time.Minute))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc}, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
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
		}, servingRowOn(1, "rev-x")}
		input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, owner)
		pod := waitingPod("engine-0-default-0", "RunContainerError", node, opStartedAt)
		outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
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
	}, servingRowOn(1, "rev-x")}
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
	pod := waitingPod("engine-0-default-0", "RunContainerError", "node-a", t0.Add(-time.Hour))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
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
			}, servingRowOn(1, "rev-x")}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
			dd := types.DispositionDeps{
				AutoMigrateMaxAttempts: 3,
				MigrationMode:          types.MigrationModeAuto,
				PodSpec:                tc.spec,
			}
			pod := waitingPod("engine-0-default-0", "RunContainerError", "node-a", t0.Add(-time.Minute))

			outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc}, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
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
// resolvable node at all. The revision serves on a sibling and the
// failure is one a node can cause, so the gate alone keeps each attempt
// off the relocation branch. All clear the Operation + stamp Failed with
// NO RetryBlock and NO ledger entry.
func TestDispose_Terminal(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	splitGang := []*corev1.Pod{
		waitingPod("engine-0-leader-0", "RunContainerError", "node-a", t0.Add(-time.Minute)),
		waitingPod("engine-0-worker-0", "RunContainerError", "node-b", t0.Add(-time.Minute)),
	}
	for _, tc := range []struct {
		name string
		dd   types.DispositionDeps
		pods []*corev1.Pod
	}{
		{name: "mode Never",
			dd:   types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeNever},
			pods: []*corev1.Pod{waitingPod("engine-0-default-0", "RunContainerError", "node-a", t0.Add(-time.Minute))}},
		{name: "multi-node gang",
			dd:   types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto},
			pods: splitGang},
		{name: "no resolvable node",
			dd:   types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto},
			pods: []*corev1.Pod{waitingPod("engine-0-default-0", "RunContainerError", "", t0.Add(-time.Minute))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := clocktesting.NewFakeClock(t0)
			c := fakeLedgerClient(t)
			insts := []types.InstanceStatus{{
				Index:     0,
				Phase:     types.InstancePhaseCreating,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate},
			}, servingRowOn(1, "rev-x")}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
			deps := types.Deps{Client: c, Clock: fc}

			outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, tc.dd, insts[0], tc.pods, "RunContainerError")
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

// The disposition decides on the pass's observation and lands on the
// fresh row. A row the same pass promoted concluded the attempt, so
// nothing is written or announced — no ladder wave, no row write —
// whichever branch classified it and whether the end would have parked
// or cleared the attempt. A row re-opened under another attempt is not
// the observed attempt's to end either (the wave it earns is pinned by
// TestDispose_AttemptReopenedSinceObservedIsNotParked).
func TestDispose_RowThatMovedOnSinceTheObservationIsLeftAlone(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	observed := types.InstanceStatus{
		Index: 0, Incarnation: 1, Phase: types.InstancePhaseUpdating,
		RunningRevision: "rev-a", TargetRevision: "rev-b",
		Operation: &types.InstanceOperation{
			ID: "update-0-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepInPlace,
			TargetRevision: "rev-b", Deadline: metav1.NewTime(t0.Add(-time.Minute)),
		},
	}
	promoted := types.InstanceStatus{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-b"}
	reopened := observed
	reopened.Operation = &types.InstanceOperation{
		ID: "update-0-2", Type: types.InstanceOperationUpdate, Step: types.UpdateStepInPlace,
		TargetRevision: "rev-c", Deadline: metav1.NewTime(t0.Add(time.Hour)),
	}
	limbo := func() *corev1.Pod {
		pod := runningNotReadyOnNode("engine-0-default-0", "node-a", t0.Add(-5*time.Minute))
		pod.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName("rev-b").Hash()}
		return pod
	}
	const deadline = "DeadlineExceeded: Update/InPlace exceeded InstanceReadyTimeout"
	for _, tc := range []struct {
		name   string
		fresh  types.InstanceStatus
		policy *types.RetryPolicy
		pod    *corev1.Pod
		reason string
	}{
		{name: "promoted, cleared end", fresh: promoted, pod: limbo(), reason: deadline},
		{name: "promoted, parked end", fresh: promoted, policy: ladderPolicy(), pod: limbo(), reason: deadline},
		{name: "promoted, workload-caused reason", fresh: promoted, policy: ladderPolicy(),
			pod: waitingPod("engine-0-default-0", "ImagePullBackOff", "node-a", t0.Add(-5*time.Minute)), reason: "ImagePullBackOff"},
		{name: "re-opened under another attempt", fresh: reopened, pod: limbo(), reason: deadline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := clocktesting.NewFakeClock(t0)
			insts := []types.InstanceStatus{observed}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-b", tc.policy, nil)
			insts[0] = tc.fresh

			outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
				types.DispositionDeps{}, observed, []*corev1.Pod{tc.pod}, tc.reason)
			if err != nil {
				t.Fatalf("DisposeExpiredAttempt: %v", err)
			}
			if outcome != escalation.DispositionWithheld {
				t.Fatalf("outcome: got %v want DispositionWithheld", outcome)
			}
			if len(rec.commits) != 0 {
				t.Fatalf("commits: got %+v want none", rec.commits)
			}
			if len(rec.blockCalls) != 0 {
				t.Fatalf("MutateRetryBlock calls: got %+v want none: a wave is not counted for an attempt that is not on the row", rec.blockCalls)
			}
			if got := insts[0]; got.Phase != tc.fresh.Phase || got.LastFailure != nil ||
				(got.Operation == nil) != (tc.fresh.Operation == nil) {
				t.Fatalf("row = %+v, want it left as the fresh row %+v", got, tc.fresh)
			}
			if len(rec.warns) != 0 {
				t.Fatalf("WarnInstanceFailed: got %+v want none for a row the pass did not fail", rec.warns)
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
	exclusion := types.NodeExclusion{Node: "node-bad", Revision: "rev-x"}
	observed := types.WorkloadObservedState{
		ExcludedNodesByInstance: map[int32][]types.NodeExclusion{1: {exclusion}},
	}
	plan, err := workload.BuildPlan(types.ComponentEngine, desired, observed)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	for _, inst := range plan.Instances {
		switch inst.Index {
		case 1:
			if len(inst.ExcludedNodes) != 1 || inst.ExcludedNodes[0] != exclusion {
				t.Errorf("instance 1 ExcludedNodes: got %v want [%v]", inst.ExcludedNodes, exclusion)
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
	if op := insts[0].Operation; !types.OperationParked(op) || op.Waiting != string(types.RolloutHoldGateRetryBlock) {
		t.Errorf("Operation: got %+v want the attempt parked on the ladder (its pod is alive)", op)
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
// unconditional).
// TestExpireOperations_SinglePodNodeFaultRecordsDirective: the deadline
// path of the escalation pass records a relocation directive for a
// single-pod attempt refused a start on one node while the revision
// serves on a sibling.
func TestExpireOperations_SinglePodNodeFaultRecordsDirective(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	// rev-x serves on Instance 1, so Instance 0's start rejection on
	// node-a is the node's to answer for; a revision nobody serves takes
	// the ladder instead (TestDispose_CrashLoopOnUnservedRevisionBlamesNoNode),
	// and so does a crash loop wherever the revision serves
	// (TestExpireOperations_SinglePodCrashLoopRecordsNoDirective).
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           "Drain",
			TargetRevision: "rev-x",
			Deadline:       metav1.NewTime(t0.Add(-time.Minute)),
		},
	}, servingRowOn(1, "rev-x")}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
	input.ObservedState.InstanceStatuses = insts
	input.Disposition = types.DispositionDeps{AutoMigrateMaxAttempts: 3}
	pod := waitingPod("engine-0-default-0", "RunContainerError", "node-a", t0.Add(-time.Hour))

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

// TestExpireOperations_SinglePodCrashLoopRecordsNoDirective is the
// crash-loop twin of the directive test above: the same served revision,
// the same single node, and nothing is filed — no ledger row, no status
// mirror, no AutoMigrationTriggered event — because a container that
// keeps exiting is the workload's fault wherever it runs. The row goes
// Failed with the crash named on it.
func TestExpireOperations_SinglePodCrashLoopRecordsNoDirective(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	recorder := record.NewFakeRecorder(8)
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           "Drain",
			TargetRevision: "rev-x",
			Deadline:       metav1.NewTime(t0.Add(-time.Minute)),
		},
	}, servingRowOn(1, "rev-x")}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", nil, ledgerOwnerCM())
	input.ObservedState.InstanceStatuses = insts
	input.Disposition = types.DispositionDeps{AutoMigrateMaxAttempts: 3}
	mirrored := 0
	input.AppendMigration = func(context.Context, types.MigrationRecord) error { mirrored++; return nil }
	pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Hour))

	plan := singleInstancePlan(0, 1)
	plan.MigrationMode = types.MigrationModeAuto
	if err := runEscalationPass(t, types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, plan,
		map[int32][]*corev1.Pod{0: {pod}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != types.InstancePhaseFailed || insts[0].LastFailure == nil || insts[0].LastFailure.Reason != "CrashLoopBackOff" {
		t.Errorf("instance: got %+v want Failed with the crash loop on LastFailure", insts[0])
	}
	if ledger := loadLedger(t, c); len(ledger.Entries) != 0 {
		t.Fatalf("ledger: got %+v want no row (a crash loop steers nothing off its node)", ledger.Entries)
	}
	if mirrored != 0 {
		t.Errorf("AppendMigration calls: got %d want 0", mirrored)
	}
	if events := drainEvents(recorder); countEventsWithReason(events, types.EventReasonAutoMigrationTriggered) != 0 {
		t.Errorf("events: got %v want no AutoMigrationTriggered", events)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %+v want one call", rec.warns)
	}
}

// TestExpireOperations_GangLeaderCrashLoopRecordsNoDirective: a gang
// create whose members all sit on one host is the one gang shape the
// relocation branch could steer, and a crash-looping leader is still not
// a reason to. The pass disposes the attempt terminal with no ledger
// row, the crash named on the row and the wave counted on the
// revision's ladder, the same as the single-pod shape.
func TestExpireOperations_GangLeaderCrashLoopRecordsNoDirective(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	recorder := record.NewFakeRecorder(8)
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 2,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationCreate,
			Step:           "CreatePods",
			TargetRevision: "rev-x",
			Deadline:       metav1.NewTime(t0.Add(-time.Minute)),
		},
	}, servingRowOn(1, "rev-x")}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", ladderPolicy(), ledgerOwnerCM())
	input.ObservedState.InstanceStatuses = insts
	input.Disposition = types.DispositionDeps{AutoMigrateMaxAttempts: 3}
	leader := waitingPod("engine-0-leader-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Hour))
	worker := readyPodOnNode("engine-0-worker-0", "node-a")

	plan := singleInstancePlan(0, 2)
	plan.MigrationMode = types.MigrationModeAuto
	if err := runEscalationPass(t, types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, plan,
		map[int32][]*corev1.Pod{0: {leader, worker}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != types.InstancePhaseFailed || insts[0].Operation != nil {
		t.Errorf("instance: got %+v want Failed-no-op", insts[0])
	}
	if got := insts[0].LastFailure; got == nil || got.PodName != leader.Name || got.Reason != "CrashLoopBackOff" {
		t.Errorf("LastFailure: got %+v want the crashing leader under CrashLoopBackOff", got)
	}
	if ledger := loadLedger(t, c); len(ledger.Entries) != 0 {
		t.Fatalf("ledger: got %+v want no row (a gang's crash loop steers nothing off its host)", ledger.Entries)
	}
	if events := drainEvents(recorder); countEventsWithReason(events, types.EventReasonAutoMigrationTriggered) != 0 {
		t.Errorf("events: got %v want no AutoMigrationTriggered", events)
	}
	if len(rec.blockCalls) != 1 || rec.blockCalls[0].rev != "rev-x" || rec.blockCalls[0].block.Reason != "CrashLoopBackOff" {
		t.Errorf("RetryBlock: got %+v want one wave on rev-x with the crash loop as its reason", rec.blockCalls)
	}
}

// readyPodOnNode is a healthy gang member: Running on node with its
// container Ready, so the host it shares with a crashing leader reads as
// the gang's single node.
func readyPodOnNode(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
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
	}, servingRowOn(1, "rev-x")}
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
	}, servingRowOn(1, "rev-x")}
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

// ladderPolicy is a retry policy whose ladder Holds a revision after two
// failed attempts, whatever their cause.
func ladderPolicy() *types.RetryPolicy {
	return &types.RetryPolicy{MaxAttempts: 2, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
}

// storeRetryBlocks replaces the fixture's zero-block recorder with a
// store seeded from persisted, so a wave observes what earlier waves
// wrote, and records the commits already landed when each block write
// arrives, so a test can check the write-ahead order. WarnRetryHeld
// calls are collected in heldWarnings.
func storeRetryBlocks(input *types.ReconcileInput, rec *dispositionRecorders, persisted []types.RetryBlock) (blocks *[]types.RetryBlock, commitsAtBlockWrite *[]int, heldWarnings *[]string) {
	store := append([]types.RetryBlock(nil), persisted...)
	blocks, commitsAtBlockWrite, heldWarnings = &store, &[]int{}, &[]string{}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*types.RetryBlock) types.RetryBlockDisposition) error {
		pos := -1
		b := types.RetryBlock{TargetRevision: rev}
		for i := range *blocks {
			if (*blocks)[i].TargetRevision == rev {
				pos, b = i, (*blocks)[i]
			}
		}
		d := mutate(&b)
		rec.blockCalls = append(rec.blockCalls, struct {
			rev   string
			disp  types.RetryBlockDisposition
			block types.RetryBlock
		}{rev: rev, disp: d, block: b})
		*commitsAtBlockWrite = append(*commitsAtBlockWrite, len(rec.commits))
		switch d {
		case types.RetryBlockPersist:
			if pos == -1 {
				*blocks = append(*blocks, b)
			} else {
				(*blocks)[pos] = b
			}
		case types.RetryBlockRemove:
			if pos != -1 {
				*blocks = append((*blocks)[:pos], (*blocks)[pos+1:]...)
			}
		}
		return nil
	}
	input.WarnRetryHeld = func(rev string, attempts int32, reason string) {
		*heldWarnings = append(*heldWarnings, fmt.Sprintf("%s attempts=%d %s", rev, attempts, reason))
	}
	return blocks, commitsAtBlockWrite, heldWarnings
}

// stuckSurgeAttempt is a single-pod surge attempt at targetRev whose
// replacement is wedged — the pod a test passes names the wedge — with
// the fixture's recorders wired under policy and owner. siblings are the
// other rows the pass observes; a sibling serving targetRev is what lets
// the disposition blame the node for a failure a node can cause.
func stuckSurgeAttempt(fc *clocktesting.FakeClock, targetRev string, policy *types.RetryPolicy, owner client.Object, siblings ...types.InstanceStatus) ([]types.InstanceStatus, types.ReconcileInput, *dispositionRecorders) {
	insts := append([]types.InstanceStatus{{
		Index:     0,
		Phase:     types.InstancePhaseUpdating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: targetRev},
	}}, siblings...)
	input, rec := dispositionFixtureInput(fc, &insts, targetRev, policy, owner)
	return insts, input, rec
}

// TestDispose_Terminal_WaveCountsOnTheLadder pins the terminal branch: a
// crash-looping attempt that no relocation directive claims charges the
// target revision's block (Backoff, one attempt, the crash loop as its
// reason) before the Operation is cleared, exactly as a workload-caused
// wave does, and the wave that reaches MaxAttempts Holds the revision
// with one RetryHeld warning.
func TestDispose_Terminal_WaveCountsOnTheLadder(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	pod := waitingPod("engine-0-default-1", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeNever}

	dispose := func(persisted []types.RetryBlock) (escalation.DispositionOutcome, *dispositionRecorders, *[]types.RetryBlock, *[]int, *[]string) {
		insts, input, rec := stuckSurgeAttempt(fc, "own-engine-crash", ladderPolicy(), ledgerOwnerCM())
		blocks, order, warns := storeRetryBlocks(&input, rec, persisted)
		outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: fakeLedgerClient(t), Clock: fc}, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
		if err != nil {
			t.Fatalf("DisposeExpiredAttempt: %v", err)
		}
		if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
			t.Fatalf("commit: got %+v want single Failed-no-op commit", rec.commits)
		}
		return outcome, rec, blocks, order, warns
	}

	outcome, rec, blocks, order, warns := dispose(nil)
	if outcome != escalation.DispositionTerminal {
		t.Fatalf("first wave outcome: got %v want DispositionTerminal", outcome)
	}
	if len(rec.blockCalls) != 1 || (*order)[0] != 0 {
		t.Fatalf("first wave: got block calls %+v (commits landed before each: %v) want one write ahead of the op-clear", rec.blockCalls, *order)
	}
	first := (*blocks)[0]
	if first.TargetRevision != "own-engine-crash" || first.State != types.RetryBlockBackoff || first.AttemptsStarted != 1 || first.Reason != "CrashLoopBackOff" {
		t.Fatalf("first wave block: got %+v want Backoff attempts=1 reason=CrashLoopBackOff on own-engine-crash", first)
	}
	if first.NextRetryAt == nil || !first.NextRetryAt.Time.Equal(t0.Add(time.Minute)) {
		t.Fatalf("first wave NextRetryAt: got %v want %v", first.NextRetryAt, t0.Add(time.Minute))
	}
	if len(*warns) != 0 {
		t.Fatalf("first wave must not warn Held: %v", *warns)
	}

	// The retry gate admitted the next attempt and flipped the block.
	first.State = types.RetryBlockRetryInProgress
	outcome, rec, blocks, _, warns = dispose([]types.RetryBlock{first})
	if outcome != escalation.DispositionTerminal {
		t.Fatalf("second wave outcome: got %v want DispositionTerminal", outcome)
	}
	if len(rec.blockCalls) != 1 {
		t.Fatalf("second wave: got %d block calls want 1", len(rec.blockCalls))
	}
	held := (*blocks)[0]
	if held.State != types.RetryBlockHeld || held.AttemptsStarted != 2 || held.NextRetryAt != nil || held.Reason != "CrashLoopBackOff" {
		t.Fatalf("second wave block: got %+v want Held attempts=2 with no retry time", held)
	}
	if len(*warns) != 1 || (*warns)[0] != "own-engine-crash attempts=2 CrashLoopBackOff" {
		t.Fatalf("RetryHeld warning: got %v want exactly one for the held revision", *warns)
	}
}

// TestDispose_Terminal_DeadlineWaveCountsOnTheLadder pins that a bare
// elapsed deadline with no pod evidence is still an attempt at the target
// that failed, so it counts on the ladder with the deadline as its
// reason.
func TestDispose_Terminal_DeadlineWaveCountsOnTheLadder(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts, input, rec := stuckSurgeAttempt(fc, "own-engine-slow", ladderPolicy(), nil)
	blocks, _, _ := storeRetryBlocks(&input, rec, nil)

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input, types.DispositionDeps{}, insts[0], nil, "DeadlineExceeded: Update/Surge exceeded InstanceReadyTimeout")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal {
		t.Fatalf("outcome: got %v want DispositionTerminal", outcome)
	}
	if len(*blocks) != 1 || (*blocks)[0].State != types.RetryBlockBackoff || (*blocks)[0].AttemptsStarted != 1 || (*blocks)[0].Reason != "DeadlineExceeded" {
		t.Fatalf("block: got %+v want one Backoff attempt with reason DeadlineExceeded", *blocks)
	}
}

// TestDispose_Terminal_NoRetryPolicyWritesNoBlock pins the arm with no
// updateRetry configured: the terminal branch writes no RetryBlock, so a
// crash-looping attempt is reopened by its operation reconciler rather
// than Held.
func TestDispose_Terminal_NoRetryPolicyWritesNoBlock(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts, input, rec := stuckSurgeAttempt(fc, "own-engine-crash", nil, nil)
	pod := waitingPod("engine-0-default-1", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input, types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal || len(rec.blockCalls) != 0 {
		t.Fatalf("got (outcome=%v, block calls=%+v) want (Terminal, none)", outcome, rec.blockCalls)
	}
}

// TestDispose_RelocationDirective_DoesNotCountOnTheLadder pins that one
// wave counts once: an attempt the relocation branch claims is counted in
// the relocation ledger alone and writes no block.
func TestDispose_RelocationDirective_DoesNotCountOnTheLadder(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	insts, input, rec := stuckSurgeAttempt(fc, "own-engine-crash", ladderPolicy(), ledgerOwnerCM(), servingRowOn(1, "own-engine-crash"))
	pod := waitingPod("engine-0-default-1", "RunContainerError", "node-a", t0.Add(-time.Minute))
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc}, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionRelocationDirective {
		t.Fatalf("outcome: got %v want DispositionRelocationDirective", outcome)
	}
	if len(rec.blockCalls) != 0 {
		t.Fatalf("a relocated wave must write no RetryBlock; got %+v", rec.blockCalls)
	}
	if got := len(loadLedger(t, c).Entries); got != 1 {
		t.Fatalf("ledger entries: got %d want 1 (the relocation directive)", got)
	}
}

// TestDispose_RelocationBudgetRunsOutBeforeTheLadderHolds pins the whole
// ladder for a revision that serves on a sibling yet is refused a start
// on every node this Instance is tried on, with auto-migration on: the
// relocation branch spends autoMigrate.maxAttempts first, each wave
// steering the rebuild off another node and touching no block, and only
// then do the waves count on the revision's ladder, Held at
// updateRetry.maxAttempts like any other failure. A revision that
// failed on that many nodes in a row was not the node's fault; one held
// wrongly is released by the operator with the release annotation.
func TestDispose_RelocationBudgetRunsOutBeforeTheLadderHolds(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	deps := types.Deps{Client: c, Clock: fc}
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 2, MigrationMode: types.MigrationModeAuto}
	policy := ladderPolicy()
	nodes := []string{"node-a", "node-b", "node-c", "node-d"}

	var persisted []types.RetryBlock
	for wave := 1; wave <= int(dd.AutoMigrateMaxAttempts)+int(policy.MaxAttempts); wave++ {
		insts, input, rec := stuckSurgeAttempt(fc, "own-engine-crash", policy, ledgerOwnerCM(), servingRowOn(1, "own-engine-crash"))
		blocks, _, warns := storeRetryBlocks(&input, rec, persisted)
		pod := waitingPod("engine-0-default-1", "RunContainerError", nodes[wave-1], t0.Add(-time.Minute))
		outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "RunContainerError")
		if err != nil {
			t.Fatalf("wave %d: DisposeExpiredAttempt: %v", wave, err)
		}
		if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
			t.Fatalf("wave %d: commit got %+v want single Failed-no-op commit", wave, rec.commits)
		}
		relocations := len(loadLedger(t, c).Entries)
		if wave <= int(dd.AutoMigrateMaxAttempts) {
			if outcome != escalation.DispositionRelocationDirective || relocations != wave || len(*blocks) != 0 {
				t.Fatalf("wave %d: got (outcome=%v, directives=%d, blocks=%+v) want (RelocationDirective, %d, none): the relocation budget is spent first", wave, outcome, relocations, *blocks, wave)
			}
			continue
		}
		counted := int32(wave) - dd.AutoMigrateMaxAttempts
		if outcome != escalation.DispositionTerminal || relocations != int(dd.AutoMigrateMaxAttempts) {
			t.Fatalf("wave %d: got (outcome=%v, directives=%d) want (Terminal, %d): past the budget nothing relocates", wave, outcome, relocations, dd.AutoMigrateMaxAttempts)
		}
		if len(*blocks) != 1 || (*blocks)[0].AttemptsStarted != counted || (*blocks)[0].Reason != "RunContainerError" {
			t.Fatalf("wave %d: block got %+v want attempts=%d reason=RunContainerError", wave, *blocks, counted)
		}
		if counted < policy.MaxAttempts {
			if (*blocks)[0].State != types.RetryBlockBackoff || len(*warns) != 0 {
				t.Fatalf("wave %d: got (state=%q, warnings=%v) want (Backoff, none)", wave, (*blocks)[0].State, *warns)
			}
			// The retry gate admitted the next attempt and flipped the block.
			next := (*blocks)[0]
			next.State = types.RetryBlockRetryInProgress
			persisted = []types.RetryBlock{next}
			continue
		}
		if (*blocks)[0].State != types.RetryBlockHeld || (*blocks)[0].NextRetryAt != nil {
			t.Fatalf("wave %d: block got %+v want Held with no retry time", wave, (*blocks)[0])
		}
		if len(*warns) != 1 || (*warns)[0] != fmt.Sprintf("own-engine-crash attempts=%d RunContainerError", policy.MaxAttempts) {
			t.Fatalf("wave %d: RetryHeld warning got %v want exactly one for the held revision", wave, *warns)
		}
	}
}

// TestDispose_Terminal_SupersededLeftoverDoesNotChargeTheTarget pins the
// same guard branch 1 applies: when the only evidence is a pod on a
// known, different revision than the attempt's target, the wave charges
// nothing — the corrective revision must not inherit the leftover's crash.
func TestDispose_Terminal_SupersededLeftoverDoesNotChargeTheTarget(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts, input, rec := stuckSurgeAttempt(fc, "own-engine-newgood", ladderPolicy(), nil)
	pod := waitingPod("engine-0-default-1", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))
	pod.Labels = map[string]string{"ome.io/revision-hash": "oldcrash"}

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input, types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal || len(rec.blockCalls) != 0 {
		t.Fatalf("got (outcome=%v, block calls=%+v) want (Terminal, none: the leftover's crash is not the target's)", outcome, rec.blockCalls)
	}
}

// TestDispose_Terminal_SchedulerHoldCountsNothing pins that a pod the
// scheduler could not place is an environment cause: the attempt is
// disposed terminal and the revision's block is untouched, however the
// ladder is configured.
func TestDispose_Terminal_SchedulerHoldCountsNothing(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	insts := []types.InstanceStatus{{
		Index:     0,
		Phase:     types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate, TargetRevision: "own-engine-big"},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "own-engine-big", ladderPolicy(), nil)
	pending := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0", Namespace: "ns", CreationTimestamp: metav1.NewTime(t0.Add(-time.Hour))},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input, types.DispositionDeps{}, insts[0], []*corev1.Pod{pending}, types.WaitingReasonUnschedulable+": 0/3 nodes are available")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal || len(rec.blockCalls) != 0 {
		t.Fatalf("got (outcome=%v, block calls=%+v) want (Terminal, none: no placement is not the revision's fault)", outcome, rec.blockCalls)
	}
}

// servingRowOn is an Instance observed Ready on rev: proof that rev
// serves somewhere, so the same revision failing elsewhere may be the
// node's doing rather than the revision's.
func servingRowOn(idx int32, rev string) types.InstanceStatus {
	return types.InstanceStatus{
		Index: idx, Phase: types.InstancePhaseReady,
		PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1,
		RunningRevision: rev,
	}
}

// crashingBetweenRestarts is the other face of a crash loop: the kubelet
// has just restarted the container, so for the moment it is Running and
// no container is Waiting, yet the restart count and the last termination
// say the process keeps exiting.
func crashingBetweenRestarts(name, node string, now time.Time) *corev1.Pod {
	pod := runningNotReadyOnNode(name, node, now.Add(-time.Minute))
	pod.Status.ContainerStatuses[0].RestartCount = 3
	pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			ExitCode:   1,
			Reason:     "Error",
			FinishedAt: metav1.NewTime(now.Add(-30 * time.Second)),
		},
	}
	return pod
}

// unschedulableRebuild is a Pending pod the scheduler could not place,
// rendered with the required hostname NotIn[excluded] the relocation
// directive projects, carrying the scheduler's own verdict.
func unschedulableRebuild(name, excluded, message string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", CreationTimestamp: metav1.NewTime(since.Add(-time.Minute))},
		Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpNotIn, Values: []string{excluded},
				}}}},
			},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             types.WaitingReasonUnschedulable,
			Message:            message,
			LastTransitionTime: metav1.NewTime(since),
		}}},
	}
}

// TestDispose_CrashLoopOnUnservedRevisionBlamesNoNode: three single-pod
// Instances; the third rolls to a revision no Instance has ever served
// and crash-loops there. Nothing says the node is at fault, so the
// disposition records no relocation directive, leaves no exclusion for
// the rebuild to render, and counts the wave on the revision's ladder —
// whichever face of the crash loop the pass happens to see: the
// container parked in CrashLoopBackOff, or Running again between two
// crashes with the restarts on its status.
func TestDispose_CrashLoopOnUnservedRevisionBlamesNoNode(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name           string
		pod            *corev1.Pod
		reason         string
		wantWaveReason string
		wantExitCode   *int32
	}{
		{
			name:           "parked in CrashLoopBackOff",
			pod:            waitingPod("engine-2-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute)),
			reason:         "CrashLoopBackOff",
			wantWaveReason: "CrashLoopBackOff",
		},
		{
			name:           "running between two crashes",
			pod:            crashingBetweenRestarts("engine-2-default-0", "node-a", t0),
			reason:         "DeadlineExceeded: Update/Drain exceeded InstanceReadyTimeout",
			wantWaveReason: "Error",
			wantExitCode:   func() *int32 { c := int32(1); return &c }(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := clocktesting.NewFakeClock(t0)
			c := fakeLedgerClient(t)
			insts := []types.InstanceStatus{servingRowOn(0, "rev-old"), servingRowOn(1, "rev-old"), {
				Index: 2, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-old",
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-new"},
			}}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-new", ladderPolicy(), ledgerOwnerCM())
			blocks, _, _ := storeRetryBlocks(&input, rec, nil)
			mirrored := 0
			input.AppendMigration = func(context.Context, types.MigrationRecord) error { mirrored++; return nil }
			dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}

			outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc}, input, dd, insts[2], []*corev1.Pod{tc.pod}, tc.reason)
			if err != nil {
				t.Fatalf("DisposeExpiredAttempt: %v", err)
			}
			if outcome != escalation.DispositionTerminal {
				t.Fatalf("outcome: got %v want DispositionTerminal (a revision nobody serves is the suspect, not the node)", outcome)
			}
			if got := len(loadLedger(t, c).Entries); got != 0 {
				t.Errorf("ledger entries: got %d want none (no node blame for a revision that never served)", got)
			}
			if mirrored != 0 {
				t.Errorf("AppendMigration calls: got %d want 0", mirrored)
			}
			if len(*blocks) != 1 || (*blocks)[0].TargetRevision != "rev-new" || (*blocks)[0].AttemptsStarted != 1 || (*blocks)[0].Reason != tc.wantWaveReason {
				t.Fatalf("block: got %+v want one attempt on rev-new with reason %s", *blocks, tc.wantWaveReason)
			}
			if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || !types.OperationParked(rec.commits[0].after.Operation) {
				t.Fatalf("commit: got %+v want a single commit parking the attempt at Failed (its pod is alive)", rec.commits)
			}
			got := rec.commits[0].after.LastFailure
			if got == nil || got.PodName != tc.pod.Name {
				t.Fatalf("LastFailure: got %+v want the crashing pod named", got)
			}
			if tc.wantExitCode != nil && (got.ExitCode == nil || *got.ExitCode != *tc.wantExitCode || got.Reason != tc.wantWaveReason) {
				t.Errorf("LastFailure: got %+v want the crash (reason %s, exit %d), not a readiness limbo", got, tc.wantWaveReason, *tc.wantExitCode)
			}
		})
	}
}

// TestDispose_CrashLoopOnServedRevisionTakesTheLadder: the same crash
// loop on a revision that serves on a sibling, or that this Instance
// itself ran before the attempt, is still the workload's failure: a
// container that keeps exiting does so on any node. The first expiry
// blames no node — no directive, no status mirror, no
// AutoMigrationTriggered event, the relocation budget untouched — and
// the wave counts on the revision's ladder with the crash named on the
// row, under either face of the loop, exactly as any other workload
// fault's wave does.
func TestDispose_CrashLoopOnServedRevisionTakesTheLadder(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	for _, shape := range []struct {
		name   string
		rows   []types.InstanceStatus
		failed int
	}{
		{
			name: "a sibling serves the revision",
			rows: []types.InstanceStatus{servingRowOn(0, "rev-x"), {
				Index: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-old",
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-x"},
			}},
			failed: 1,
		},
		{
			name: "this Instance ran the revision before",
			rows: []types.InstanceStatus{{
				Index: 0, Phase: types.InstancePhaseCreating, RunningRevision: "rev-x",
				Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate, TargetRevision: "rev-x"},
			}},
		},
	} {
		for _, face := range []struct {
			name       string
			pod        func(name string) *corev1.Pod
			reason     string
			waveReason string
		}{
			{"parked in CrashLoopBackOff", func(name string) *corev1.Pod {
				return waitingPod(name, "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))
			}, "CrashLoopBackOff", "CrashLoopBackOff"},
			{"running between two crashes", func(name string) *corev1.Pod {
				return crashingBetweenRestarts(name, "node-a", t0)
			}, "DeadlineExceeded: Create/CreatePods exceeded InstanceReadyTimeout", "Error"},
		} {
			t.Run(shape.name+", "+face.name, func(t *testing.T) {
				fc := clocktesting.NewFakeClock(t0)
				c := fakeLedgerClient(t)
				recorder := record.NewFakeRecorder(8)
				insts := append([]types.InstanceStatus(nil), shape.rows...)
				input, rec := dispositionFixtureInput(fc, &insts, "rev-x", ladderPolicy(), ledgerOwnerCM())
				blocks, _, _ := storeRetryBlocks(&input, rec, nil)
				mirrored := 0
				input.AppendMigration = func(context.Context, types.MigrationRecord) error { mirrored++; return nil }
				directives := 0
				dd := types.DispositionDeps{
					AutoMigrateMaxAttempts: 3,
					MigrationMode:          types.MigrationModeAuto,
					OnRelocationDirective:  func(string) { directives++ },
				}
				row := insts[shape.failed]
				pod := face.pod(fmt.Sprintf("engine-%d-default-0", row.Index))

				outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, dd, row, []*corev1.Pod{pod}, face.reason)
				if err != nil {
					t.Fatalf("DisposeExpiredAttempt: %v", err)
				}
				if outcome != escalation.DispositionTerminal {
					t.Fatalf("outcome: got %v want DispositionTerminal (a crash loop is the workload's failure, not the node's)", outcome)
				}
				ledger := loadLedger(t, c)
				if len(ledger.Entries) != 0 || audit.CountAutoRecoverAttempts(ledger, "engine", row.Index) != 0 {
					t.Fatalf("ledger: got %+v want no directive and an untouched relocation budget", ledger.Entries)
				}
				if mirrored != 0 || directives != 0 {
					t.Errorf("relocation side effects: mirrored=%d directives=%d want none", mirrored, directives)
				}
				if events := drainEvents(recorder); countEventsWithReason(events, types.EventReasonAutoMigrationTriggered) != 0 {
					t.Errorf("events: got %v want no AutoMigrationTriggered", events)
				}
				if len(*blocks) != 1 || (*blocks)[0].TargetRevision != "rev-x" || (*blocks)[0].AttemptsStarted != 1 || (*blocks)[0].Reason != face.waveReason {
					t.Fatalf("block: got %+v want one attempt on rev-x with reason %s", *blocks, face.waveReason)
				}
				if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed {
					t.Fatalf("commit: got %+v want a single Failed commit", rec.commits)
				}
				if got := rec.commits[0].after.LastFailure; got == nil || got.PodName != pod.Name || got.Reason != face.waveReason {
					t.Errorf("LastFailure: got %+v want the crashing pod under reason %s", got, face.waveReason)
				}
			})
		}
	}
}

// TestDispose_CrashLoopLadderHoldsWithNoRelocation walks a crash loop on
// a served revision up the whole ladder: every wave disposes terminal,
// the relocation ledger never gains a row and the budget is never
// spent, the waves count on the revision's block alone, and the bound
// Holds the revision with one RetryHeld warning. No attempt number and
// no ladder state earns the crash loop a node to be steered off.
func TestDispose_CrashLoopLadderHoldsWithNoRelocation(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	recorder := record.NewFakeRecorder(16)
	deps := types.Deps{Client: c, Clock: fc, Recorder: recorder}
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}
	policy := &types.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	nodes := []string{"node-a", "node-b", "node-c"}

	var persisted []types.RetryBlock
	for wave := int32(1); wave <= policy.MaxAttempts; wave++ {
		insts, input, rec := stuckSurgeAttempt(fc, "own-engine-crash", policy, ledgerOwnerCM(), servingRowOn(1, "own-engine-crash"))
		blocks, _, warns := storeRetryBlocks(&input, rec, persisted)
		pod := waitingPod("engine-0-default-1", "CrashLoopBackOff", nodes[wave-1], t0.Add(-time.Minute))
		outcome, err := escalation.DisposeExpiredAttempt(context.Background(), deps, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
		if err != nil {
			t.Fatalf("wave %d: DisposeExpiredAttempt: %v", wave, err)
		}
		if outcome != escalation.DispositionTerminal {
			t.Fatalf("wave %d: outcome got %v want DispositionTerminal (a crash loop never relocates)", wave, outcome)
		}
		if ledger := loadLedger(t, c); len(ledger.Entries) != 0 || audit.CountAutoRecoverAttempts(ledger, "engine", 0) != 0 {
			t.Fatalf("wave %d: ledger got %+v want no directive and an unspent budget", wave, ledger.Entries)
		}
		if len(*blocks) != 1 || (*blocks)[0].AttemptsStarted != wave || (*blocks)[0].Reason != "CrashLoopBackOff" {
			t.Fatalf("wave %d: block got %+v want attempts=%d reason=CrashLoopBackOff", wave, *blocks, wave)
		}
		if wave < policy.MaxAttempts {
			if (*blocks)[0].State != types.RetryBlockBackoff || len(*warns) != 0 {
				t.Fatalf("wave %d: got (state=%q, warnings=%v) want (Backoff, none)", wave, (*blocks)[0].State, *warns)
			}
			// The retry gate admitted the next attempt and flipped the block.
			next := (*blocks)[0]
			next.State = types.RetryBlockRetryInProgress
			persisted = []types.RetryBlock{next}
			continue
		}
		if (*blocks)[0].State != types.RetryBlockHeld || (*blocks)[0].NextRetryAt != nil {
			t.Fatalf("wave %d: block got %+v want Held with no retry time", wave, (*blocks)[0])
		}
		if len(*warns) != 1 || (*warns)[0] != fmt.Sprintf("own-engine-crash attempts=%d CrashLoopBackOff", policy.MaxAttempts) {
			t.Fatalf("wave %d: RetryHeld warning got %v want exactly one for the held revision", wave, *warns)
		}
	}
	if events := drainEvents(recorder); countEventsWithReason(events, types.EventReasonAutoMigrationTriggered) != 0 {
		t.Errorf("events: got %v want no AutoMigrationTriggered across the ladder", events)
	}
}

// TestDispose_GangLeaderCrashLoopBlamesNoNode: a gang whose members all
// sit on one host is the one gang shape the relocation branch could
// steer, and a crash-looping leader is still not a reason to. A gang
// create and a gang recreate dispose terminal alike, under either face
// of the loop: no directive, the crash named on the row, the wave
// counted on the revision's ladder — the same reading as the single-pod
// shape.
func TestDispose_GangLeaderCrashLoopBlamesNoNode(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	for _, shape := range []struct {
		name  string
		phase types.InstancePhase
		op    *types.InstanceOperation
	}{
		{"gang create", types.InstancePhaseCreating, &types.InstanceOperation{Type: types.InstanceOperationCreate, TargetRevision: "rev-x"}},
		{"gang recreate", types.InstancePhaseUpdating, &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-x"}},
	} {
		for _, face := range []struct {
			name       string
			leader     func() *corev1.Pod
			reason     string
			waveReason string
		}{
			{"parked in CrashLoopBackOff", func() *corev1.Pod {
				return waitingPod("engine-0-leader-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))
			}, "CrashLoopBackOff", "CrashLoopBackOff"},
			{"running between two crashes", func() *corev1.Pod {
				return crashingBetweenRestarts("engine-0-leader-0", "node-a", t0)
			}, "DeadlineExceeded: Update/Drain exceeded InstanceReadyTimeout", "Error"},
		} {
			t.Run(shape.name+", "+face.name, func(t *testing.T) {
				fc := clocktesting.NewFakeClock(t0)
				c := fakeLedgerClient(t)
				recorder := record.NewFakeRecorder(8)
				insts := []types.InstanceStatus{{
					Index: 0, Phase: shape.phase, PodCount: 2, RunningRevision: "rev-old", Operation: shape.op,
				}, servingRowOn(1, "rev-x")}
				input, rec := dispositionFixtureInput(fc, &insts, "rev-x", ladderPolicy(), ledgerOwnerCM())
				blocks, _, _ := storeRetryBlocks(&input, rec, nil)
				mirrored := 0
				input.AppendMigration = func(context.Context, types.MigrationRecord) error { mirrored++; return nil }
				dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}
				leader := face.leader()
				pods := []*corev1.Pod{leader, readyPodOnNode("engine-0-worker-0", "node-a")}

				outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, dd, insts[0], pods, face.reason)
				if err != nil {
					t.Fatalf("DisposeExpiredAttempt: %v", err)
				}
				if outcome != escalation.DispositionTerminal {
					t.Fatalf("outcome: got %v want DispositionTerminal (a gang's crash loop is the workload's failure, not its host's)", outcome)
				}
				if ledger := loadLedger(t, c); len(ledger.Entries) != 0 {
					t.Fatalf("ledger: got %+v want no directive", ledger.Entries)
				}
				if mirrored != 0 {
					t.Errorf("AppendMigration calls: got %d want 0", mirrored)
				}
				if events := drainEvents(recorder); countEventsWithReason(events, types.EventReasonAutoMigrationTriggered) != 0 {
					t.Errorf("events: got %v want no AutoMigrationTriggered", events)
				}
				if len(*blocks) != 1 || (*blocks)[0].TargetRevision != "rev-x" || (*blocks)[0].AttemptsStarted != 1 || (*blocks)[0].Reason != face.waveReason {
					t.Fatalf("block: got %+v want one attempt on rev-x with reason %s", *blocks, face.waveReason)
				}
				if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed {
					t.Fatalf("commit: got %+v want a single Failed commit", rec.commits)
				}
				if got := rec.commits[0].after.LastFailure; got == nil || got.PodName != leader.Name || got.Reason != face.waveReason {
					t.Errorf("LastFailure: got %+v want the crashing leader under reason %s", got, face.waveReason)
				}
			})
		}
	}
}

// TestDispose_NodeFaultStillRelocates pins the classes the relocation
// branch takes, a crash loop not among them: a runtime start
// rejection on a served revision, and a pod whose node stopped
// reporting it, each on one resolvable node under Auto with budget,
// record a directive against that node, charge no ladder and announce
// the relocation once.
func TestDispose_NodeFaultStillRelocates(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		pod    *corev1.Pod
		reason string
	}{
		{"runtime start rejection", waitingPod("engine-0-default-0", "RunContainerError", "node-a", t0.Add(-time.Minute)), "RunContainerError"},
		{"node stopped reporting the pod", podOnLostNode("engine-0-default-0", "node-a", t0), "DeadlineExceeded: Update/Drain exceeded InstanceReadyTimeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := clocktesting.NewFakeClock(t0)
			c := fakeLedgerClient(t)
			recorder := record.NewFakeRecorder(8)
			insts := []types.InstanceStatus{{
				Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-old",
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-x"},
			}, servingRowOn(1, "rev-x")}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-x", ladderPolicy(), ledgerOwnerCM())
			blocks, _, _ := storeRetryBlocks(&input, rec, nil)
			mirrored := 0
			input.AppendMigration = func(context.Context, types.MigrationRecord) error { mirrored++; return nil }
			dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}

			outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, dd, insts[0], []*corev1.Pod{tc.pod}, tc.reason)
			if err != nil {
				t.Fatalf("DisposeExpiredAttempt: %v", err)
			}
			if outcome != escalation.DispositionRelocationDirective {
				t.Fatalf("outcome: got %v want DispositionRelocationDirective (a failure a node can cause, on a revision that serves)", outcome)
			}
			ledger := loadLedger(t, c)
			if len(ledger.Entries) != 1 || ledger.Entries[0].FromNode != "node-a" || ledger.Entries[0].Revision != "rev-x" || ledger.Entries[0].Reason != audit.ReasonAutoRecover {
				t.Fatalf("ledger: got %+v want one directive against node-a for rev-x", ledger.Entries)
			}
			if mirrored != 1 {
				t.Errorf("AppendMigration calls: got %d want 1", mirrored)
			}
			if events := drainEvents(recorder); countEventsWithReason(events, types.EventReasonAutoMigrationTriggered) != 1 {
				t.Errorf("events: got %v want one AutoMigrationTriggered", events)
			}
			if len(*blocks) != 0 {
				t.Errorf("blocks: got %+v want none (a relocated wave does not count on the ladder)", *blocks)
			}
		})
	}
}

// podOnLostNode is a pod whose node stopped reporting it: Running with its
// container up and never crashed, Ready withdrawn by the node lifecycle
// controller. No container names a failure, so only the deadline ends the
// attempt, and the node it sits on is the suspect.
func podOnLostNode(name, node string, now time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))},
		Spec:       corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionFalse,
				Reason:             "NodeNotReady",
				LastTransitionTime: metav1.NewTime(now.Add(-10 * time.Minute)),
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// TestDispose_CrashLoopLeavesARequestedMoveAlone: an operator's move of
// another Instance is in flight in the ledger when this Instance's
// attempt crash-loops on the served revision. The crash loop files
// nothing: the request's row stays the only row and stays in flight, no
// directive or status mirror is written for the crashing Instance, and
// its wave counts on the revision's ladder.
func TestDispose_CrashLoopLeavesARequestedMoveAlone(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	owner := ledgerOwnerCM()
	recorder := record.NewFakeRecorder(8)

	// The operator's move of Instance 1, accepted and not yet finished.
	seeded := &audit.Ledger{}
	seeded.UpsertEntry(audit.Entry{
		RequestUUID: "u-request", Component: "engine", SourceInstance: 1, SurgeInstance: 2,
		Phase: audit.PhaseStarted, Reason: "operator requested", FromNode: "node-b",
		StartedAt: t0.Add(-time.Minute).UTC().Format(time.RFC3339),
	})
	if err := audit.PersistLedgerForOwner(context.Background(), c, owner, corev1.SchemeGroupVersion.WithKind("ConfigMap"), seeded); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	insts := []types.InstanceStatus{{
		Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-old",
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-x"},
	}, servingRowOn(1, "rev-x")}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", ladderPolicy(), owner)
	blocks, _, _ := storeRetryBlocks(&input, rec, nil)
	mirrored := 0
	input.AppendMigration = func(context.Context, types.MigrationRecord) error { mirrored++; return nil }
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}
	pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, dd, insts[0], []*corev1.Pod{pod}, "CrashLoopBackOff")
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal {
		t.Fatalf("outcome: got %v want DispositionTerminal", outcome)
	}
	ledger := loadLedger(t, c)
	if len(ledger.Entries) != 1 || ledger.Entries[0].RequestUUID != "u-request" {
		t.Fatalf("ledger: got %+v want the request's row alone", ledger.Entries)
	}
	if !audit.HasInFlightMigrationForInstance(ledger, "engine", 1) {
		t.Errorf("the requested move must still be in flight after the crash loop's disposition; ledger %+v", ledger.Entries)
	}
	if audit.CountAutoRecoverAttempts(ledger, "engine", 0) != 0 || mirrored != 0 {
		t.Errorf("crashing Instance: got %d directives and %d mirrored records want none", audit.CountAutoRecoverAttempts(ledger, "engine", 0), mirrored)
	}
	if events := drainEvents(recorder); countEventsWithReason(events, types.EventReasonAutoMigrationTriggered) != 0 {
		t.Errorf("events: got %v want no AutoMigrationTriggered", events)
	}
	if len(*blocks) != 1 || (*blocks)[0].TargetRevision != "rev-x" || (*blocks)[0].Reason != "CrashLoopBackOff" {
		t.Fatalf("block: got %+v want the crash loop's wave on rev-x", *blocks)
	}
}

// TestDispose_UnschedulableRebuildReleasesNodeExclusion: a rebuild
// rendered with the Instance's recorded exclusion found no node with
// room and outlasted the scheduler grace. The exclusion is what blocks
// placement, so the disposition releases it: the next rebuild renders
// without it, the relocation budget already spent stays spent, no
// further directive is recorded, and one Normal event says why.
func TestDispose_UnschedulableRebuildReleasesNodeExclusion(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	const message = "0/2 nodes are available: 1 node(s) didn't match Pod's node affinity/selector, 1 Insufficient nvidia.com/gpu"
	fc := clocktesting.NewFakeClock(t0)
	c := fakeLedgerClient(t)
	owner := ledgerOwnerCM()
	seeded := &audit.Ledger{}
	seeded.UpsertEntry(audit.Entry{
		RequestUUID: "u-directive", Component: "engine", SourceInstance: 0,
		Phase: audit.PhaseCompleted, Reason: audit.ReasonAutoRecover,
		Outcome: audit.OutcomeRelocateRecreate, FromNode: "node-a", Revision: "rev-x",
		StartedAt:   t0.Add(-time.Hour).UTC().Format(time.RFC3339),
		CompletedAt: t0.Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	if err := audit.PersistLedgerForOwner(context.Background(), c, owner, corev1.SchemeGroupVersion.WithKind("ConfigMap"), seeded); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	insts := []types.InstanceStatus{{
		Index: 0, Phase: types.InstancePhaseCreating, RunningRevision: "rev-x",
		Operation: &types.InstanceOperation{
			Type: types.InstanceOperationCreate, TargetRevision: "rev-x",
			StartedAt: metav1.NewTime(t0.Add(-30 * time.Minute)),
			Waiting:   types.WaitingReasonUnschedulable,
		},
	}}
	input, rec := dispositionFixtureInput(fc, &insts, "rev-x", ladderPolicy(), owner)
	blocks, _, _ := storeRetryBlocks(&input, rec, nil)
	mirrored := 0
	input.AppendMigration = func(context.Context, types.MigrationRecord) error { mirrored++; return nil }
	recorder := record.NewFakeRecorder(8)
	dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}
	pod := unschedulableRebuild("engine-0-default-0", "node-a", message, t0.Add(-20*time.Minute))

	outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, dd, insts[0],
		[]*corev1.Pod{pod}, types.WaitingReasonUnschedulable+": "+message)
	if err != nil {
		t.Fatalf("DisposeExpiredAttempt: %v", err)
	}
	if outcome != escalation.DispositionTerminal {
		t.Fatalf("outcome: got %v want DispositionTerminal", outcome)
	}
	ledger := loadLedger(t, c)
	if exclusions := audit.RecentAutoRecoverExclusions(ledger, "engine", 0, 3); len(exclusions) != 0 {
		t.Errorf("exclusions after the release: got %v want none (the next rebuild must be free to land where there is room)", exclusions)
	}
	if got := audit.CountAutoRecoverAttempts(ledger, "engine", 0); got != 1 {
		t.Errorf("relocation attempts after the release: got %d want 1 (a released directive still counts on the budget)", got)
	}
	if len(ledger.Entries) != 1 {
		t.Errorf("ledger entries: got %d want 1 (no further directive)", len(ledger.Entries))
	}
	if mirrored != 0 || len(*blocks) != 0 {
		t.Errorf("side effects: got mirrored=%d blocks=%+v want none (no placement is neither a relocation nor the revision's fault)", mirrored, *blocks)
	}
	events := drainEvents(recorder)
	if len(events) != 1 || countEventsWithReason(events, types.EventReasonNodeExclusionReleased) != 1 ||
		!strings.HasPrefix(events[0], corev1.EventTypeNormal) || !strings.Contains(events[0], "node-a") {
		t.Errorf("events: got %v want exactly one Normal %s event naming node-a", events, types.EventReasonNodeExclusionReleased)
	}

	if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed || rec.commits[0].after.Operation != nil {
		t.Fatalf("commit: got %+v want single Failed-no-op commit", rec.commits)
	}
	if got := rec.commits[0].after.LastFailure; got == nil || got.Reason != types.WaitingReasonUnschedulable {
		t.Errorf("LastFailure: got %+v want the scheduler's verdict", got)
	}

	// A second pass over the same verdict has nothing left to release:
	// no ledger write and no repeated event.
	insts[0].Phase, insts[0].Operation = types.InstancePhaseCreating, &types.InstanceOperation{
		Type: types.InstanceOperationCreate, TargetRevision: "rev-x", Waiting: types.WaitingReasonUnschedulable,
	}
	if _, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, dd, insts[0],
		[]*corev1.Pod{pod}, types.WaitingReasonUnschedulable+": "+message); err != nil {
		t.Fatalf("DisposeExpiredAttempt (second pass): %v", err)
	}
	if again := drainEvents(recorder); len(again) != 0 {
		t.Errorf("second pass events: got %v want none (nothing left to release)", again)
	}
}

// An attempt disposed while its own pod set is alive is parked rather
// than ended: the operation stays on the row with the ladder's wait named
// on it and its deadline parked, the row reads Failed for the set that is
// down, the failure is recorded and the ladder is charged exactly as for
// a cleared attempt. An attempt whose pods are gone, or whose wave no
// ladder counts, is cleared.
func TestDispose_LiveSetParksTheAttempt(t *testing.T) {
	ladder := &types.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	for _, tc := range []struct {
		name      string
		reason    string
		step      string
		served    bool
		gone      bool
		policy    *types.RetryPolicy
		outcome   escalation.DispositionOutcome
		parked    bool
		wantState types.RetryBlockState
		wantWait  string
	}{
		{name: "crash loop of a recreated set that served", reason: "CrashLoopBackOff", step: types.UpdateStepDrain, served: true, policy: ladder,
			outcome: escalation.DispositionTerminal, parked: true, wantState: types.RetryBlockBackoff, wantWait: string(types.RolloutHoldGateRetryBlock)},
		{name: "config key missing on a patched pod that served", reason: "CreateContainerConfigError", step: types.UpdateStepInPlace, served: true, policy: ladder,
			outcome: escalation.DispositionHeldRevision, parked: true, wantState: types.RetryBlockBackoff, wantWait: string(types.RolloutHoldGateRetryBlock)},
		{name: "held at once with no ladder configured", reason: "CreateContainerConfigError", step: types.UpdateStepDrain, served: true,
			outcome: escalation.DispositionHeldRevision, parked: true, wantState: types.RetryBlockHeld, wantWait: string(types.RolloutHoldGateHeld)},
		{name: "crash loop of a set that never served", reason: "CrashLoopBackOff", step: types.UpdateStepDrain, policy: ladder,
			outcome: escalation.DispositionTerminal, parked: true, wantState: types.RetryBlockBackoff, wantWait: string(types.RolloutHoldGateRetryBlock)},
		{name: "pull failure of a set that never started", reason: "ImagePullBackOff", step: types.UpdateStepDrain, policy: ladder,
			outcome: escalation.DispositionHeldRevision, parked: true, wantState: types.RetryBlockBackoff, wantWait: string(types.RolloutHoldGateRetryBlock)},
		{name: "pull failure of a set that is gone", reason: "ImagePullBackOff", step: types.UpdateStepDrain, policy: ladder, gone: true,
			outcome: escalation.DispositionTerminal, wantState: types.RetryBlockBackoff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
			fc := clocktesting.NewFakeClock(t0)
			insts := []types.InstanceStatus{{
				Index: 0, Incarnation: 2, Phase: types.InstancePhaseUpdating,
				RunningRevision: "rev-good", TargetRevision: "rev-bad",
				Operation: &types.InstanceOperation{
					ID: "update-0-1", Type: types.InstanceOperationUpdate, Step: tc.step,
					TargetRevision: "rev-bad", Deadline: metav1.NewTime(t0.Add(-time.Minute)),
				},
			}}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-bad", tc.policy, nil)
			pod := waitingPod("engine-0-default-0", tc.reason, "node-a", t0.Add(-5*time.Minute))
			if tc.served {
				pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue})
			}
			if tc.gone {
				// The pod the disposition blames is on its way out: nothing
				// of the set is left for the row to follow.
				deleting := metav1.NewTime(t0)
				pod.DeletionTimestamp = &deleting
			}

			outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
				types.DispositionDeps{}, insts[0], []*corev1.Pod{pod}, tc.reason)
			if err != nil {
				t.Fatalf("DisposeExpiredAttempt: %v", err)
			}
			if outcome != tc.outcome {
				t.Fatalf("outcome: got %v want %v", outcome, tc.outcome)
			}
			if len(rec.commits) != 1 {
				t.Fatalf("MutateInstance commits: got %d want 1", len(rec.commits))
			}
			after := rec.commits[0].after
			if after.Phase != types.InstancePhaseFailed {
				t.Errorf("Phase after commit: got %q want Failed (the set is down at the disposal)", after.Phase)
			}
			if after.LastFailure == nil || after.LastFailure.Reason != tc.reason || (!tc.gone && after.LastFailure.PodName != pod.Name) {
				t.Errorf("LastFailure: got %+v want Reason=%s PodName=%s", after.LastFailure, tc.reason, pod.Name)
			}
			if len(rec.blockCalls) != 1 || rec.blockCalls[0].rev != "rev-bad" {
				t.Fatalf("MutateRetryBlock calls: got %+v want one for rev-bad", rec.blockCalls)
			}
			if block := rec.blockCalls[0].block; block.State != tc.wantState || block.AttemptsStarted != 1 {
				t.Errorf("block: got (state=%s attempts=%d) want (%s, 1)", block.State, block.AttemptsStarted, tc.wantState)
			}
			if !tc.parked {
				if after.Operation != nil {
					t.Fatalf("a set that is gone has nothing to park on; the operation must be cleared, got %+v", after.Operation)
				}
				return
			}
			op := after.Operation
			if op == nil || op.ID != "update-0-1" || op.Type != types.InstanceOperationUpdate || op.Step != types.UpdateStepParked || op.TargetRevision != "rev-bad" {
				t.Fatalf("the attempt must stay on the row on the parked step, got %+v", op)
			}
			if op.Waiting != tc.wantWait {
				t.Errorf("Waiting: got %q want %q (the wait the ladder names)", op.Waiting, tc.wantWait)
			}
			if !op.Deadline.IsZero() {
				t.Errorf("Deadline: got %v want parked (zero): the clock that ended the attempt must not end it again", op.Deadline)
			}
		})
	}
}

// The park lands only on the attempt the disposition observed. A row that
// carries another Update attempt by the time the write lands - one opened
// over the expired attempt in the same pass, at a corrected revision or
// the same one - is neither parked with the old attempt's wait nor
// cleared: a fresh attempt is not the disposition's to end. The wave is
// still counted on the expired attempt's revision.
func TestDispose_AttemptReopenedSinceObservedIsNotParked(t *testing.T) {
	ladder := &types.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		revision string
	}{
		{name: "re-opened at a corrected revision", revision: "rev-fixed"},
		{name: "re-opened at the same revision and step", revision: "rev-bad"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := clocktesting.NewFakeClock(t0)
			insts := []types.InstanceStatus{{
				Index: 0, Incarnation: 2, Phase: types.InstancePhaseUpdating,
				RunningRevision: "rev-good", TargetRevision: "rev-bad",
				Operation: &types.InstanceOperation{
					ID: "update-0-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain,
					TargetRevision: "rev-bad", Deadline: metav1.NewTime(t0.Add(-time.Minute)),
				},
			}}
			input, rec := dispositionFixtureInput(fc, &insts, "rev-bad", ladder, nil)
			observed := insts[0]
			fresh := &types.InstanceOperation{
				ID: "update-0-2", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain,
				TargetRevision: tc.revision, StartedAt: metav1.NewTime(t0), Deadline: metav1.NewTime(t0.Add(time.Hour)),
			}
			insts[0].Operation = fresh
			pod := waitingPod("engine-0-default-0", "CrashLoopBackOff", "node-a", t0.Add(-5*time.Minute))
			pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue})

			if _, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{}, input,
				types.DispositionDeps{}, observed, []*corev1.Pod{pod}, "CrashLoopBackOff"); err != nil {
				t.Fatalf("DisposeExpiredAttempt: %v", err)
			}
			if len(rec.commits) != 0 {
				t.Fatalf("MutateInstance commits: got %+v want none, the fresh attempt is not the disposition's to end", rec.commits)
			}
			if row := insts[0]; row.Phase != types.InstancePhaseUpdating || row.Operation != fresh || row.LastFailure != nil ||
				row.Operation.Step != types.UpdateStepDrain || row.Operation.Waiting != "" || row.Operation.Deadline.IsZero() {
				t.Fatalf("row = %+v, want the fresh attempt left exactly as it was", row)
			}
			if len(rec.blockCalls) != 1 || rec.blockCalls[0].rev != "rev-bad" {
				t.Fatalf("MutateRetryBlock calls: got %+v want the wave counted on rev-bad", rec.blockCalls)
			}
		})
	}
}

// TestDispose_HeldRevisionBlamesNoNode: the crash loop of a revision that
// served, once the ladder has Held that revision, is the revision's own
// failure. No directive is recorded and no node excluded, the Held block
// is left as it is, and the attempt ends as a terminal wave does — a
// surge or a Create cleared at Failed, a recreate parked behind the hold
// with its live pod — under either face of the loop.
func TestDispose_HeldRevisionBlamesNoNode(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	for _, shape := range []struct {
		name     string
		rows     []types.InstanceStatus
		failed   int
		parked   bool
		deadline string
	}{
		{
			name: "a surge beside a sibling that serves the revision",
			rows: []types.InstanceStatus{servingRowOn(0, "rev-x"), {
				Index: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-old",
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: "rev-x"},
			}},
			failed:   1,
			deadline: "DeadlineExceeded: Update/Surge exceeded InstanceReadyTimeout",
		},
		{
			name: "a recreate beside a sibling that serves the revision",
			rows: []types.InstanceStatus{servingRowOn(0, "rev-x"), {
				Index: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-old",
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-x"},
			}},
			failed:   1,
			parked:   true,
			deadline: "DeadlineExceeded: Update/Drain exceeded InstanceReadyTimeout",
		},
		{
			name: "a Create on the Instance that ran the revision before",
			rows: []types.InstanceStatus{{
				Index: 0, Phase: types.InstancePhaseCreating, RunningRevision: "rev-x",
				Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate, TargetRevision: "rev-x"},
			}},
			deadline: "DeadlineExceeded: Create/CreatePods exceeded InstanceReadyTimeout",
		},
	} {
		for _, face := range []struct {
			name   string
			pod    func(name string) *corev1.Pod
			reason string
		}{
			{"parked in CrashLoopBackOff", func(name string) *corev1.Pod {
				return waitingPod(name, "CrashLoopBackOff", "node-a", t0.Add(-time.Minute))
			}, "CrashLoopBackOff"},
			{"running between two crashes", func(name string) *corev1.Pod {
				return crashingBetweenRestarts(name, "node-a", t0)
			}, shape.deadline},
		} {
			t.Run(shape.name+", "+face.name, func(t *testing.T) {
				fc := clocktesting.NewFakeClock(t0)
				c := fakeLedgerClient(t)
				insts := append([]types.InstanceStatus(nil), shape.rows...)
				policy := ladderPolicy()
				input, rec := dispositionFixtureInput(fc, &insts, "rev-x", policy, ledgerOwnerCM())
				held := types.RetryBlock{TargetRevision: "rev-x", State: types.RetryBlockHeld, AttemptsStarted: policy.MaxAttempts, Reason: "Error"}
				blocks, _, warns := storeRetryBlocks(&input, rec, []types.RetryBlock{held})
				input.ObservedState.RetryBlocks = []types.RetryBlock{held}
				mirrored := 0
				input.AppendMigration = func(context.Context, types.MigrationRecord) error { mirrored++; return nil }
				dd := types.DispositionDeps{AutoMigrateMaxAttempts: 3, MigrationMode: types.MigrationModeAuto}
				row := insts[shape.failed]
				pod := face.pod(fmt.Sprintf("engine-%d-default-0", row.Index))

				outcome, err := escalation.DisposeExpiredAttempt(context.Background(), types.Deps{Client: c, Clock: fc}, input, dd, row, []*corev1.Pod{pod}, face.reason)
				if err != nil {
					t.Fatalf("DisposeExpiredAttempt: %v", err)
				}
				if outcome != escalation.DispositionTerminal {
					t.Fatalf("outcome: got %v want DispositionTerminal (the ladder Held the revision, so the revision is the suspect, not the node)", outcome)
				}
				if got := len(loadLedger(t, c).Entries); got != 0 {
					t.Errorf("ledger entries: got %d want none (no node blame for a revision the ladder Held)", got)
				}
				if mirrored != 0 {
					t.Errorf("AppendMigration calls: got %d want 0", mirrored)
				}
				if len(*blocks) != 1 || (*blocks)[0] != held {
					t.Errorf("block: got %+v want the Held block left as it was", *blocks)
				}
				if len(*warns) != 0 {
					t.Errorf("RetryHeld warnings: got %v want none (the revision was already Held)", *warns)
				}
				if len(rec.commits) != 1 || rec.commits[0].after.Phase != types.InstancePhaseFailed {
					t.Fatalf("commit: got %+v want a single commit ending the attempt at Failed", rec.commits)
				}
				after := rec.commits[0].after
				if shape.parked {
					if !types.OperationParked(after.Operation) || after.Operation.Waiting != string(types.RolloutHoldGateHeld) {
						t.Fatalf("operation: got %+v want the recreate parked behind the hold (its pod is alive)", after.Operation)
					}
				} else if after.Operation != nil {
					t.Fatalf("operation: got %+v want the attempt cleared", after.Operation)
				}
			})
		}
	}
}
