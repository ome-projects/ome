package status_test

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestUniqueNodes locks the deterministic ordering + nil-preservation
// contract NodesOccupied callers rely on (status equality checks key on
// the rendered field; non-determinism would churn LastTransitionTime).
func TestUniqueNodes(t *testing.T) {
	pods := []*corev1.Pod{
		{Spec: corev1.PodSpec{NodeName: "a"}},
		{Spec: corev1.PodSpec{NodeName: "b"}},
		{Spec: corev1.PodSpec{NodeName: "a"}},
		{Spec: corev1.PodSpec{}}, // unscheduled — skipped
	}
	got := status.UniqueNodes(pods)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("UniqueNodes: got %v want [a b]", got)
	}
	if got := status.UniqueNodes(nil); got != nil {
		t.Errorf("nil input should return nil; got %v", got)
	}
}

// TestInstanceMeetsThreshold pins the surge-tolerant classifier: the
// canonical pods always count when their satisfying count meets the
// desired threshold, even if PodCount briefly exceeds desired during a
// surge.
func TestInstanceMeetsThreshold(t *testing.T) {
	tests := []struct {
		name     string
		observed int32
		sat      int32
		desired  int32
		want     bool
	}{
		{name: "all observed satisfying (no plan info)", observed: 2, sat: 2, desired: 0, want: true},
		{name: "fewer observed satisfying than total (no plan info)", observed: 2, sat: 1, desired: 0, want: false},
		{name: "no pods observed", observed: 0, sat: 0, desired: 1, want: false},
		{name: "surge: satisfying < observed but >= desired", observed: 2, sat: 1, desired: 1, want: true},
		{name: "surge: satisfying matches desired exactly", observed: 3, sat: 2, desired: 2, want: true},
		{name: "surge: satisfying short of desired", observed: 3, sat: 1, desired: 2, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := status.InstanceMeetsThreshold(tt.observed, tt.sat, tt.desired); got != tt.want {
				t.Errorf("InstanceMeetsThreshold(observed=%d sat=%d desired=%d): got %v want %v",
					tt.observed, tt.sat, tt.desired, got, tt.want)
			}
		})
	}
}

// TestDesiredFor pins the lookup-with-fallback semantic: present in the
// map → use the value; absent (or nil map) → fall back to observed.
func TestDesiredFor(t *testing.T) {
	m := map[int32]int32{0: 2, 1: 3}
	if got := status.DesiredFor(m, 0, 7); got != 2 {
		t.Errorf("present: got %d want 2", got)
	}
	if got := status.DesiredFor(m, 99, 7); got != 7 {
		t.Errorf("absent: got %d want fallback 7", got)
	}
	if got := status.DesiredFor(nil, 0, 7); got != 7 {
		t.Errorf("nil map: got %d want fallback 7", got)
	}
}

// TestRolloutComplete pins the CurrentRevision promotion gate: every
// Instance must be Phase=Ready AND on the target revision; any deviation
// holds CurrentRevision in place.
func TestRolloutComplete(t *testing.T) {
	allReady := []types.InstanceStatus{
		{Phase: types.InstancePhaseReady, RunningRevision: "isvc-engine-aaaaaaaa"},
		{Phase: types.InstancePhaseReady, RunningRevision: "isvc-engine-aaaaaaaa"},
	}
	if !status.RolloutComplete(allReady, "isvc-engine-aaaaaaaa") {
		t.Errorf("all Ready on rev should be complete")
	}
	oneCreating := []types.InstanceStatus{
		{Phase: types.InstancePhaseCreating, RunningRevision: "isvc-engine-aaaaaaaa"},
		{Phase: types.InstancePhaseReady, RunningRevision: "isvc-engine-aaaaaaaa"},
	}
	if status.RolloutComplete(oneCreating, "isvc-engine-aaaaaaaa") {
		t.Errorf("Phase=Creating should keep rollout incomplete")
	}
	stale := []types.InstanceStatus{
		{Phase: types.InstancePhaseReady, RunningRevision: "isvc-engine-bbbbbbbb"},
	}
	if status.RolloutComplete(stale, "isvc-engine-aaaaaaaa") {
		t.Errorf("stale RunningRevision should keep rollout incomplete")
	}
	if status.RolloutComplete(nil, "isvc-engine-aaaaaaaa") {
		t.Errorf("empty instance list should not be complete")
	}
}

// TestCurrentRevisionFor pins the rollup both ways: promoted once every
// Instance is Ready on the update revision and its retry ladder records
// no failure; withdrawn when current already names the update revision
// while an Instance still runs another or is pinned to one by its update
// operation; left as recorded on every other shape, a repair on the
// current revision included. A revision whose block still stands has not
// landed, whatever its rows read between crashes, so a push never lands
// by attrition.
func TestCurrentRevisionFor(t *testing.T) {
	const prior, next = "isvc-engine-aaaaaaaa", "isvc-engine-bbbbbbbb"
	row := func(phase types.InstancePhase, rev string) types.InstanceStatus {
		return types.InstanceStatus{Phase: phase, RunningRevision: rev}
	}
	// pinned is a row mid-surge: it runs running while its operation is
	// pinned to target.
	pinned := func(phase types.InstancePhase, running, target string) types.InstanceStatus {
		return types.InstanceStatus{Phase: phase, RunningRevision: running, Operation: &types.InstanceOperation{
			Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: target}}
	}
	surgeIdx := int32(2)
	gangSource := func(phase types.InstancePhase, running, target string) types.InstanceStatus {
		s := pinned(phase, running, target)
		s.Operation.SurgeIndex = &surgeIdx
		return s
	}
	// marker is the row claiming a replacement gang's index: it has run
	// nothing, and its claim names the replacement's revision.
	marker := func(step, target string) types.InstanceStatus {
		return types.InstanceStatus{Index: surgeIdx, Phase: types.InstancePhaseCreating, TargetRevision: target,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: step, TargetRevision: target}}
	}
	block := func(state types.RetryBlockState) []types.RetryBlock {
		return []types.RetryBlock{{TargetRevision: next, State: state, AttemptsStarted: 1}}
	}
	// heldPrior is the ladder after an attempt at the prior revision failed
	// for good: what a rolled-back Instance leaves behind when it comes
	// back broken.
	heldPrior := []types.RetryBlock{{TargetRevision: prior, State: types.RetryBlockHeld, AttemptsStarted: 1}}
	// withdrawn names the revision the current revision was withdrawn
	// from while the status still records that withdrawal; "" when nothing
	// was withdrawn, or the update revision has moved on since.
	cases := []struct {
		name            string
		rows            []types.InstanceStatus
		current, update string
		blocks          []types.RetryBlock
		withdrawn       string
		want            string
	}{
		{"every Instance Ready on the update revision promotes it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next, nil, "", next},
		{"every Instance Ready on a Held revision does not land it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next, block(types.RetryBlockHeld), "", prior},
		{"every Instance Ready on a revision in backoff does not land it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next, block(types.RetryBlockBackoff), "", prior},
		{"every Instance Ready on a revision with an attempt in flight does not land it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next, block(types.RetryBlockRetryInProgress), "", prior},
		{"a block on another revision does not withhold the landing",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next,
			[]types.RetryBlock{{TargetRevision: prior, State: types.RetryBlockHeld}}, "", next},
		{"a withdrawn current stays withdrawn while the update revision's block stands",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, "", next, block(types.RetryBlockHeld), prior, ""},
		{"a forward roll in flight keeps the prior revision",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseUpdating, prior)}, prior, next, nil, "", prior},
		{"a rollback onto the current revision withdraws it while every Instance runs the superseded one",
			[]types.InstanceStatus{row(types.InstancePhaseUpdating, next), row(types.InstancePhaseReady, next)}, prior, prior, nil, "", ""},
		{"one Instance still on the superseded revision is enough to withdraw",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, next)}, prior, prior, nil, "", ""},
		{"a repair on the current revision keeps it",
			[]types.InstanceStatus{row(types.InstancePhaseRestarting, prior), row(types.InstancePhaseReady, prior)}, prior, prior, nil, "", prior},
		{"an Instance that has not run anything yet decides nothing",
			[]types.InstanceStatus{row(types.InstancePhaseCreating, ""), row(types.InstancePhaseReady, prior)}, prior, prior, nil, "", prior},
		{"withdrawn stays withdrawn until every Instance is Ready on the update revision",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseUpdating, next)}, "", prior, nil, prior, ""},
		{"no update revision leaves current alone",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior)}, prior, "", nil, "", prior},
		{"no Instances leave current alone", nil, prior, prior, nil, "", prior},
		{"a rollback onto the current revision withdraws it while a surge is still pinned to the superseded one",
			[]types.InstanceStatus{pinned(types.InstancePhaseUpdating, prior, next), row(types.InstancePhaseReady, prior)}, prior, prior, nil, "", ""},
		{"a rollback onto the current revision withdraws it while a gang surge marker is still pinned to the superseded one",
			[]types.InstanceStatus{gangSource(types.InstancePhaseUpdating, prior, next), row(types.InstancePhaseReady, prior),
				marker(types.UpdateStepGangSurgeTarget, next)}, prior, prior, nil, "", ""},
		{"the cleanup marker of an abandoned gang surge keeps the current revision withdrawn",
			[]types.InstanceStatus{gangSource(types.InstancePhaseUpdating, prior, next), row(types.InstancePhaseReady, prior),
				marker(types.UpdateStepGangSurgeTargetCleanup, next)}, prior, prior, nil, "", ""},
		{"a lingering gang surge marker alone keeps the current revision withdrawn",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior),
				marker(types.UpdateStepGangSurgeTargetCleanup, next)}, prior, prior, nil, "", ""},
		{"a Failed source still pinned to the superseded revision withdraws the current revision",
			[]types.InstanceStatus{gangSource(types.InstancePhaseFailed, prior, next), row(types.InstancePhaseReady, prior),
				marker(types.UpdateStepGangSurgeTarget, next)}, prior, prior, nil, "", ""},
		{"withdrawn stays withdrawn while an operation is still pinned to the superseded revision",
			[]types.InstanceStatus{pinned(types.InstancePhaseUpdating, prior, next), row(types.InstancePhaseReady, prior)}, "", prior, nil, prior, ""},
		{"an operation pinned to the update revision itself keeps it",
			[]types.InstanceStatus{pinned(types.InstancePhaseUpdating, prior, prior), row(types.InstancePhaseReady, prior)}, prior, prior, nil, "", prior},
		{"a withdrawn current comes back once every Instance is back on the withdrawn revision, a Failed one and its block included",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior),
				row(types.InstancePhaseFailed, prior)}, "", prior, heldPrior, prior, prior},
		{"a withdrawn current stays withdrawn while an Instance is still pinned to another revision, however the others read",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior),
				pinned(types.InstancePhaseFailed, prior, next)}, "", prior, heldPrior, prior, ""},
		{"a withdrawn current comes back when the Failed Instance has no pod left to run anything",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior),
				row(types.InstancePhaseFailed, "")}, "", prior, heldPrior, prior, prior},
		{"a revision pushed after the withdrawal does not land while an Instance is Failed on it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next),
				row(types.InstancePhaseFailed, next)}, "", next, block(types.RetryBlockHeld), prior, ""},
		{"with no withdrawal on record a fleet with a Failed Instance stays unlanded",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, prior),
				row(types.InstancePhaseFailed, prior)}, "", prior, heldPrior, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.CurrentRevisionFor(tc.rows, tc.current, tc.update, tc.blocks, tc.withdrawn, nil); got != tc.want {
				t.Errorf("CurrentRevisionFor(current=%q, update=%q, withdrawn=%q) = %q, want %q", tc.current, tc.update, tc.withdrawn, got, tc.want)
			}
		})
	}
}

// TestCurrentRevisionFor_ReadsTheRevisionThePodsCarry: a live pod decides
// what its Instance runs, over the revision its row still records; a row
// with no labelled live pod runs what it records.
func TestCurrentRevisionFor_ReadsTheRevisionThePodsCarry(t *testing.T) {
	const prior, next = "isvc-engine-aaaaaaaa", "isvc-engine-bbbbbbbb"
	at := func(idx int32, phase types.InstancePhase, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: phase, RunningRevision: rev}
	}
	// parkedOn is the row a failed recreate onto target leaves: Failed with
	// the attempt parked on target and the revision it ran before still
	// recorded as running.
	parkedOn := func(idx int32, running, target string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseFailed, RunningRevision: running, TargetRevision: target,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepParked, TargetRevision: target}}
	}
	on := func(revs ...string) []query.RevisionID {
		ids := make([]query.RevisionID, 0, len(revs))
		for _, rev := range revs {
			ids = append(ids, query.RevisionFromHash(query.RevisionFromName(rev).Hash()))
		}
		return ids
	}
	heldPrior := []types.RetryBlock{{TargetRevision: prior, State: types.RetryBlockHeld, AttemptsStarted: 3}}
	cases := []struct {
		name            string
		rows            []types.InstanceStatus
		pods            status.PodRevisions
		current, update string
		blocks          []types.RetryBlock
		withdrawn       string
		want            string
	}{
		{"a withdrawn current comes back once every Instance's pods carry the withdrawn revision, whatever a Failed row still records",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), parkedOn(1, next, prior)},
			status.PodRevisions{0: on(prior), 1: on(prior, prior)}, "", prior, heldPrior, prior, prior},
		{"the restore waits while a pod of another revision is alive under an Instance",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), parkedOn(1, next, prior)},
			status.PodRevisions{0: on(prior), 1: on(next)}, "", prior, heldPrior, prior, ""},
		{"the restore waits while one member of a gang still carries another revision",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), parkedOn(1, next, prior)},
			status.PodRevisions{0: on(prior), 1: on(prior, next)}, "", prior, heldPrior, prior, ""},
		{"an Instance with no labelled live pod runs the revision its row records",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), parkedOn(1, next, prior)},
			status.PodRevisions{0: on(prior)}, "", prior, heldPrior, prior, ""},
		{"a pod of another revision under an index with no row keeps the current revision withdrawn",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), at(1, types.InstancePhaseFailed, prior)},
			status.PodRevisions{0: on(prior), 1: on(prior), 2: on(next)}, "", prior, heldPrior, prior, ""},
		{"a pod of the withdrawn revision under an index with no row decides nothing",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), at(1, types.InstancePhaseFailed, prior)},
			status.PodRevisions{0: on(prior), 1: on(prior), 2: on(prior)}, "", prior, heldPrior, prior, prior},
		{"a rollback onto the current revision withdraws it while a pod of the superseded revision is alive, whatever the row records",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), at(1, types.InstancePhaseRestarting, prior)},
			status.PodRevisions{0: on(prior), 1: on(next)}, prior, prior, nil, "", ""},
		{"a rollback onto the current revision keeps it while every pod carries it, whatever a mid-roll row records",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), parkedOn(1, next, prior)},
			status.PodRevisions{0: on(prior), 1: on(prior)}, prior, prior, heldPrior, "", prior},
		{"an operation pinned to another revision is off it whatever its pods carry",
			[]types.InstanceStatus{at(0, types.InstancePhaseReady, prior), parkedOn(1, prior, next)},
			status.PodRevisions{0: on(prior), 1: on(prior)}, "", prior, heldPrior, prior, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.CurrentRevisionFor(tc.rows, tc.current, tc.update, tc.blocks, tc.withdrawn, tc.pods); got != tc.want {
				t.Errorf("CurrentRevisionFor(current=%q, update=%q, withdrawn=%q, pods=%v) = %q, want %q", tc.current, tc.update, tc.withdrawn, tc.pods, got, tc.want)
			}
		})
	}
}

// TestCurrentRevisionFor_AnAttemptInFlightKeepsTheRollbackOpen: a roll back
// onto the withdrawn revision rebuilds an Instance's pods on it before
// anything promotes the row, so while the new set starts its pods carry
// the withdrawn revision and the row still records the superseded one.
// The current revision stays withdrawn until that attempt promotes the
// Instance; a spent attempt leaves the pods to decide, so a rollback whose
// Instance comes back broken and held still reads landed.
func TestCurrentRevisionFor_AnAttemptInFlightKeepsTheRollbackOpen(t *testing.T) {
	const prior, next = "isvc-decoder-aaaaaaaa", "isvc-decoder-bbbbbbbb"
	ready := func(idx int32, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseReady, RunningRevision: rev}
	}
	// inFlight is a row whose update attempt from running onto target a
	// pass still drives: a recreate past its drain, or an in-place patch.
	inFlight := func(idx int32, step, running, target string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseUpdating, RunningRevision: running, TargetRevision: target,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: step, TargetRevision: target}}
	}
	// parked is the same attempt spent: Failed, parked on target, the
	// revision it ran before still recorded.
	parked := func(idx int32, running, target string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseFailed, RunningRevision: running, TargetRevision: target,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepParked, TargetRevision: target}}
	}
	on := func(revs ...string) []query.RevisionID {
		ids := make([]query.RevisionID, 0, len(revs))
		for _, rev := range revs {
			ids = append(ids, query.RevisionFromHash(query.RevisionFromName(rev).Hash()))
		}
		return ids
	}
	heldPrior := []types.RetryBlock{{TargetRevision: prior, State: types.RetryBlockHeld, AttemptsStarted: 3}}
	// Three gangs of two roll back onto prior one at a time: the first and
	// the third are Ready on it again, and the second has just had its new
	// gang created, nothing Ready yet, with its row still on next.
	lastGangStarting := []types.InstanceStatus{ready(0, prior), inFlight(1, types.UpdateStepDrain, next, prior), ready(2, prior)}
	gangsOnPrior := status.PodRevisions{0: on(prior, prior), 1: on(prior, prior), 2: on(prior, prior)}
	cases := []struct {
		name            string
		rows            []types.InstanceStatus
		pods            status.PodRevisions
		current, update string
		blocks          []types.RetryBlock
		withdrawn       string
		want            string
	}{
		{"the restore waits while the last Instance's recreate onto the withdrawn revision has created its gang but not promoted it",
			lastGangStarting, gangsOnPrior, "", prior, nil, prior, ""},
		{"the restore waits while an in-place patch back onto the withdrawn revision has relabelled the pod but not promoted the Instance",
			[]types.InstanceStatus{ready(0, prior), inFlight(1, types.UpdateStepInPlace, next, prior)},
			status.PodRevisions{0: on(prior), 1: on(prior)}, "", prior, nil, prior, ""},
		{"the promote of the last Instance brings the current revision back",
			[]types.InstanceStatus{ready(0, prior), ready(1, prior), ready(2, prior)}, gangsOnPrior, "", prior, nil, prior, prior},
		{"a rollback onto the current revision withdraws it while an attempt in flight still records an Instance on the superseded revision, whatever its pods carry",
			lastGangStarting, gangsOnPrior, prior, prior, nil, "", ""},
		{"an attempt in flight whose row already names the cleared revision holds the restore until it promotes the Instance",
			[]types.InstanceStatus{ready(0, prior), inFlight(1, types.UpdateStepInPlace, prior, prior)},
			status.PodRevisions{0: on(prior), 1: on(prior)}, "", prior, nil, prior, ""},
		{"a spent attempt leaves the pods to decide: a Failed Instance whose pods carry the withdrawn revision does not hold the restore",
			[]types.InstanceStatus{ready(0, prior), parked(1, next, prior)},
			status.PodRevisions{0: on(prior), 1: on(prior)}, "", prior, heldPrior, prior, prior},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.CurrentRevisionFor(tc.rows, tc.current, tc.update, tc.blocks, tc.withdrawn, tc.pods); got != tc.want {
				t.Errorf("CurrentRevisionFor(current=%q, update=%q, withdrawn=%q) = %q, want %q", tc.current, tc.update, tc.withdrawn, got, tc.want)
			}
		})
	}
}

// TestCurrentRevisionFor_ARestoreWaitsForEveryAttemptInFlight: the current
// revision, cleared because the update revision was pushed again while an
// Instance was off it, comes back once no attempt a pass still drives is
// open and every pod carries the revision. An Instance whose lost set is
// built again toward the revision holds the restore while its new pods come
// up, whatever label they carry, as does a repair in flight; a spent
// attempt on a Failed Instance leaves the pods to decide, so a roll back
// whose Instance comes back broken still lands. A push over the same rows
// reads as it always has: the revision it had until every Instance is Ready
// on the new one.
func TestCurrentRevisionFor_ARestoreWaitsForEveryAttemptInFlight(t *testing.T) {
	const one, two = "isvc-engine-aaaaaaaa", "isvc-engine-bbbbbbbb"
	ready := func(idx int32, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseReady, RunningRevision: rev}
	}
	// rebuilt is a row whose pods were lost and are created again toward
	// target by the Create pass; the row keeps the revision it ran.
	rebuilt := func(idx int32, running, target string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseCreating, RunningRevision: running, TargetRevision: target,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate, Step: status.CreateStepCreatePods, TargetRevision: target}}
	}
	// lost is a row whose pods are gone before the Create pass has committed
	// the rebuild: Pending, no operation.
	lost := func(idx int32, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhasePending, RunningRevision: rev}
	}
	// repairing is a row the restart pass drives on its own revision.
	repairing := func(idx int32, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseRestarting, RunningRevision: rev,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}}
	}
	// broken is a spent attempt: Failed on its revision, nothing driving it.
	broken := func(idx int32, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseFailed, RunningRevision: rev}
	}
	// stopped is a recreate back onto target that ended on a pod that does
	// not come up: Failed, the revision it ran still recorded.
	stopped := func(idx int32, running, target string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseFailed, RunningRevision: running, TargetRevision: target,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepParked, TargetRevision: target}}
	}
	on := func(revs ...string) []query.RevisionID {
		ids := make([]query.RevisionID, 0, len(revs))
		for _, rev := range revs {
			ids = append(ids, query.RevisionFromHash(query.RevisionFromName(rev).Hash()))
		}
		return ids
	}
	heldTwo := []types.RetryBlock{{TargetRevision: two, State: types.RetryBlockHeld, AttemptsStarted: 3}}
	allOnTwo := status.PodRevisions{0: on(two), 1: on(two), 2: on(two)}
	cases := []struct {
		name            string
		rows            []types.InstanceStatus
		pods            status.PodRevisions
		current, update string
		blocks          []types.RetryBlock
		cleared         string
		want            string
	}{
		{"an Instance rebuilt toward the cleared revision holds the restore while its new pod comes up, whatever label the pod carries",
			[]types.InstanceStatus{ready(0, two), ready(1, two), rebuilt(2, one, two)}, allOnTwo, "", two, nil, two, ""},
		{"an Instance that ran the cleared revision and lost its pods holds the restore until its rebuild ends",
			[]types.InstanceStatus{ready(0, two), ready(1, two), rebuilt(2, two, two)}, allOnTwo, "", two, nil, two, ""},
		{"an Instance whose pods are gone before the rebuild is committed holds the restore",
			[]types.InstanceStatus{ready(0, two), ready(1, two), lost(2, two)}, status.PodRevisions{0: on(two), 1: on(two)}, "", two, nil, two, ""},
		{"a repair in flight on the cleared revision holds the restore",
			[]types.InstanceStatus{ready(0, two), ready(1, two), repairing(2, two)}, allOnTwo, "", two, nil, two, ""},
		{"a spent attempt on a Failed Instance leaves the pods to decide: a roll back over a broken Instance lands, its Held record notwithstanding",
			[]types.InstanceStatus{ready(0, two), ready(1, two), broken(2, two)}, allOnTwo, "", two, heldTwo, two, two},
		{"a recreate that ended broken on the cleared revision leaves its pods to decide too",
			[]types.InstanceStatus{ready(0, two), ready(1, two), stopped(2, one, two)}, allOnTwo, "", two, heldTwo, two, two},
		{"the restore lands once the last attempt ends with the Instance Ready",
			[]types.InstanceStatus{ready(0, two), ready(1, two), ready(2, two)}, allOnTwo, "", two, heldTwo, two, two},
		{"a push over an Instance rebuilt toward the new revision keeps the revision it had",
			[]types.InstanceStatus{ready(0, two), ready(1, two), rebuilt(2, one, two)}, allOnTwo, one, two, nil, "", one},
		{"a push lands once every Instance is Ready on the new revision",
			[]types.InstanceStatus{ready(0, two), ready(1, two), ready(2, two)}, allOnTwo, one, two, nil, "", two},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.CurrentRevisionFor(tc.rows, tc.current, tc.update, tc.blocks, tc.cleared, tc.pods); got != tc.want {
				t.Errorf("CurrentRevisionFor(current=%q, update=%q, cleared=%q) = %q, want %q", tc.current, tc.update, tc.cleared, got, tc.want)
			}
		})
	}
}

// TestPodRevisionsByInstance pins what counts as a pod's revision for the
// rollup: the revision-hash label of a pod that is not in a terminal phase,
// a pod being deleted included; a pod without the label is left out.
func TestPodRevisionsByInstance(t *testing.T) {
	pod := func(hash string, phase corev1.PodPhase, deleting bool) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p-" + hash + string(phase), Labels: map[string]string{}}}
		if hash != "" {
			p.Labels[query.LabelRevisionHash] = hash
		}
		p.Status.Phase = phase
		if deleting {
			now := metav1.Now()
			p.DeletionTimestamp = &now
		}
		return p
	}
	byInstance := map[int32][]*corev1.Pod{
		0: {pod("aaaaaaaa", corev1.PodRunning, false), pod("bbbbbbbb", corev1.PodFailed, false)},
		1: {pod("bbbbbbbb", corev1.PodRunning, true), pod("", corev1.PodRunning, false)},
		2: {pod("cccccccc", corev1.PodSucceeded, false)},
	}
	got := status.PodRevisionsByInstance(byInstance)
	want := status.PodRevisions{
		0: {query.RevisionFromHash("aaaaaaaa")},
		1: {query.RevisionFromHash("bbbbbbbb")},
	}
	if len(got) != len(want) {
		t.Fatalf("PodRevisionsByInstance = %v, want %v", got, want)
	}
	for idx, revs := range want {
		if len(got[idx]) != len(revs) || !got[idx][0].Same(revs[0]) {
			t.Errorf("instance %d: got %v, want %v", idx, got[idx], revs)
		}
	}
}

func TestReachedDesiredShape(t *testing.T) {
	ready := func(rev string) types.InstanceStatus {
		return types.InstanceStatus{Phase: types.InstancePhaseReady, RunningRevision: rev}
	}
	newRev, oldRev := "isvc-engine-newaaaaa", "isvc-engine-oldbbbbb"
	// 8 instances, partition=2 → want 6 on new + 2 on old, all Ready.
	staged := []types.InstanceStatus{ready(newRev), ready(newRev), ready(newRev), ready(newRev),
		ready(newRev), ready(newRev), ready(oldRev), ready(oldRev)}
	if !status.ReachedDesiredShape(staged, newRev, 2, 8) {
		t.Fatal("6 new + 2 held, all Ready → reached")
	}
	if status.ReachedDesiredShape(staged, newRev, 0, 8) { // partition 0 wants all 8 new
		t.Fatal("partition 0 must require all on new")
	}
	degraded := append([]types.InstanceStatus{}, staged...)
	degraded[7].Phase = types.InstancePhaseUpdating
	if status.ReachedDesiredShape(degraded, newRev, 2, 8) {
		t.Fatal("held instance not Ready → not reached")
	}
	allNew := []types.InstanceStatus{ready(newRev), ready(newRev)}
	if status.RolloutComplete(allNew, newRev) != status.ReachedDesiredShape(allNew, newRev, 0, 2) {
		t.Fatal("RolloutComplete must equal ReachedDesiredShape(...,0,len)")
	}
}

// TestCountAvailablePods_MinReadySecondsWindow pins the Available counter's
// two inputs — EndpointSlice rotation membership always, and the Ready-age
// window only when the Component sets minReadySeconds — and the wake it
// reports: the remaining window of the earliest in-rotation pod still inside
// it, nothing once every such pod is Available or when no window applies.
func TestCountAvailablePods_MinReadySecondsWindow(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	readyPod := func(name string, readyAt time.Time) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(readyAt),
			}}},
		}
	}
	aged := readyPod("aged", now.Add(-30*time.Second))
	fresh := readyPod("fresh", now.Add(-5*time.Second))
	notInRotation := readyPod("out", now.Add(-30*time.Second))
	pods := []*corev1.Pod{aged, fresh, notInRotation}
	inRotation := map[string]struct{}{aged.Name: {}, fresh.Name: {}}

	if got, wake := status.CountAvailablePods(pods, inRotation, status.AvailabilityWindow{Now: now}); got != 2 || wake != 0 {
		t.Fatalf("zero window: got %d/%s want 2/0s (rotation membership alone, nothing pending)", got, wake)
	}
	window := status.AvailabilityWindow{MinReadySeconds: 20, Now: now}
	if got, wake := status.CountAvailablePods(pods, inRotation, window); got != 1 || wake != 15*time.Second {
		t.Fatalf("20s window: got %d/%s want 1/15s (only the aged pod; the fresh pod crosses in 15s)", got, wake)
	}
	if got, wake := status.CountAvailablePods([]*corev1.Pod{fresh, notInRotation}, map[string]struct{}{notInRotation.Name: {}}, window); got != 1 || wake != 0 {
		t.Fatalf("out-of-rotation pod inside the window: got %d/%s want 1/0s (no wake for a pod rotation will not admit by time alone)", got, wake)
	}
	window.Now = now.Add(15 * time.Second)
	if got, wake := status.CountAvailablePods(pods, inRotation, window); got != 2 || wake != 0 {
		t.Fatalf("20s window after it elapsed: got %d/%s want 2/0s", got, wake)
	}
	counters, _ := status.CountersForInstance(pods, inRotation, status.AvailabilityWindow{MinReadySeconds: 20, Now: now}, nil)
	if counters.AvailablePodCount != 1 || counters.ReadyPodCount != 0 || counters.NextAvailableIn != 15*time.Second {
		t.Fatalf("counters: got available=%d ready=%d nextAvailableIn=%s want available=1 ready=0 nextAvailableIn=15s (Ready counts ContainersReady)",
			counters.AvailablePodCount, counters.ReadyPodCount, counters.NextAvailableIn)
	}
}

// A settled Ready row is demotable under every restart policy: the rule
// reads the row alone, because the running revision the demotion keeps is
// what each policy's rebuild path keys on. A row an operation owns, and a
// row in any other phase, is not the truth pass's to move.
func TestDemotableReady(t *testing.T) {
	tests := []struct {
		name string
		row  types.InstanceStatus
		want bool
	}{
		{name: "settled Ready row", row: types.InstanceStatus{Phase: types.InstancePhaseReady, RunningRevision: "rev-a"}, want: true},
		{name: "settled Ready row without a revision", row: types.InstanceStatus{Phase: types.InstancePhaseReady}, want: true},
		{name: "Ready row an operation owns", row: types.InstanceStatus{Phase: types.InstancePhaseReady,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge}}, want: false},
		{name: "already Pending", row: types.InstanceStatus{Phase: types.InstancePhasePending, RunningRevision: "rev-a"}, want: false},
		{name: "Restarting", row: types.InstanceStatus{Phase: types.InstancePhaseRestarting,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}}, want: false},
		{name: "Failed", row: types.InstanceStatus{Phase: types.InstancePhaseFailed}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := test.row
			if got := status.DemotableReady(&row); got != test.want {
				t.Errorf("DemotableReady(%+v) = %t, want %t", row, got, test.want)
			}
		})
	}
	if status.DemotableReady(nil) {
		t.Errorf("a nil row is the Empty slot, never a demotion candidate")
	}
}

// TestCountServingPods_RequiresThePodReadyCondition: a pod is serving only
// when the pod's Ready condition and the serving gate are both True.
// ContainersReady and the gate are the kubelet's and the controller's last
// writes; on a node whose kubelet stopped both keep their values while the
// node lifecycle controller sets Ready False, and such a pod is in no
// Service's endpoints. The same shape is a gate the kubelet has not folded
// into Ready yet, and that pod is not in the endpoints either.
func TestCountServingPods_RequiresThePodReadyCondition(t *testing.T) {
	condition := func(conditionType corev1.PodConditionType, value corev1.ConditionStatus) corev1.PodCondition {
		return corev1.PodCondition{Type: conditionType, Status: value}
	}
	pod := func(conditions ...corev1.PodCondition) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
			Status:     corev1.PodStatus{Conditions: conditions},
		}
	}
	containersReady := condition(corev1.ContainersReady, corev1.ConditionTrue)
	ready := condition(corev1.PodReady, corev1.ConditionTrue)
	readyRevoked := condition(corev1.PodReady, corev1.ConditionFalse)
	gateOn := condition(podreadiness.ConditionType, corev1.ConditionTrue)
	gateOff := condition(podreadiness.ConditionType, corev1.ConditionFalse)

	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want int32
	}{
		{name: "Ready with the gate on", pod: pod(containersReady, ready, gateOn), want: 1},
		{name: "Ready revoked by the control plane, containers and gate unchanged", pod: pod(containersReady, readyRevoked, gateOn), want: 0},
		{name: "gate off, Ready not yet folded", pod: pod(containersReady, ready, gateOff), want: 0},
		{name: "gate on, Ready never reported", pod: pod(containersReady, gateOn), want: 0},
		{name: "no conditions", pod: pod(), want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.CountServingPods([]*corev1.Pod{tc.pod}); got != tc.want {
				t.Fatalf("CountServingPods = %d, want %d", got, tc.want)
			}
		})
	}

	lost := pod(containersReady, readyRevoked, gateOn)
	counters, _ := status.CountersForInstance([]*corev1.Pod{lost}, map[string]struct{}{}, status.AvailabilityWindow{}, nil)
	if counters.PodCount != 1 || counters.ServingPodCount != 0 {
		t.Fatalf("counters under a revoked Ready: pods=%d serving=%d, want pods=1 serving=0", counters.PodCount, counters.ServingPodCount)
	}
}

// A deleted pod keeps its Ready condition and its serving gate until its
// containers stop, but the EndpointSlice controller has already taken it out
// of every Service's ready endpoints, so it serves nothing new. The serving
// count follows the rotation: a gang that is losing a member stops counting
// as whole on the pass that sees the deletion, not on the one that sees the
// pod gone.
func TestCountServingPods_ExcludesAMemberLeavingTheGang(t *testing.T) {
	servingPod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue},
			}},
		}
	}
	leader := servingPod("engine-0-leader-0")
	worker := servingPod("engine-0-worker-0")
	gang := []*corev1.Pod{leader, worker}
	desired := map[int32]int32{0: 2}

	whole, _ := status.CountersForInstance(gang, map[string]struct{}{leader.Name: {}, worker.Name: {}}, status.AvailabilityWindow{}, nil)
	if whole.PodCount != 2 || whole.ServingPodCount != 2 || whole.AvailablePodCount != 2 {
		t.Fatalf("whole gang: %s, want pods=2 serving=2 available=2", whole)
	}
	if !status.InstanceMeetsThreshold(whole.PodCount, whole.ServingPodCount, status.DesiredFor(desired, 0, whole.PodCount)) {
		t.Fatalf("the whole gang is short of the serving threshold, want a serving Instance")
	}

	// The worker is deleted: still Ready, still gated on, already out of
	// the endpoints. Its conditions do not change until its containers stop.
	now := metav1.Now()
	worker.DeletionTimestamp = &now
	leaving, _ := status.CountersForInstance(gang, map[string]struct{}{leader.Name: {}}, status.AvailabilityWindow{}, nil)
	if leaving.PodCount != 2 {
		t.Fatalf("PodCount = %d, want 2: the leaving member is still a pod of the Instance", leaving.PodCount)
	}
	if leaving.ServingPodCount != 1 {
		t.Fatalf("ServingPodCount = %d, want 1: a member leaving the gang is out of rotation", leaving.ServingPodCount)
	}
	if leaving.AvailablePodCount != 1 {
		t.Fatalf("AvailablePodCount = %d, want 1", leaving.AvailablePodCount)
	}
	if status.InstanceMeetsThreshold(leaving.PodCount, leaving.ServingPodCount, status.DesiredFor(desired, 0, leaving.PodCount)) {
		t.Fatalf("a gang with a member leaving meets the serving threshold, want no serving Instance")
	}
}

func TestWithdrawnRevisionAfter(t *testing.T) {
	const prior, next = "isvc-engine-aaaaaaaa", "isvc-engine-bbbbbbbb"
	cases := []struct {
		name                                string
		before, oldCurrent, current, update string
		want                                string
	}{
		{"the write that withdraws current records the revision it named", "", prior, "", prior, prior},
		{"the record stays while current stays empty on the same update revision", prior, "", "", prior, prior},
		{"the record is dropped once the update revision moves on", prior, "", "", next, ""},
		{"the record is dropped once a current revision is recorded again", prior, "", prior, prior, ""},
		{"a promotion records no withdrawal", "", prior, next, next, ""},
		{"a current that was never set records no withdrawal", "", "", "", prior, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.WithdrawnRevisionAfter(tc.before, tc.oldCurrent, tc.current, tc.update); got != tc.want {
				t.Errorf("WithdrawnRevisionAfter(before=%q, oldCurrent=%q, current=%q, update=%q) = %q, want %q",
					tc.before, tc.oldCurrent, tc.current, tc.update, got, tc.want)
			}
		})
	}
}

// An Instance serves through its routable pod: the leader of a gang, or
// the one pod of a single-pod Instance. A Service never publishes a
// worker, so an Instance with no routable serving pod serves nothing,
// whatever its workers read, and its serving count is 0. While the
// routable pod serves, the count takes in every serving member: a whole
// gang counts N and a gang with a worker gone counts N-1.
func TestCountServingPods_ReadsAGangWithoutItsLeaderAsServingNothing(t *testing.T) {
	servingPod := func(name, runner string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{query.LabelRunner: runner}},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue},
			}},
		}
	}
	// The pod's node died: the node lifecycle controller revoked PodReady
	// while the containers and the gate keep their last values.
	nodeLost := func(pod *corev1.Pod) *corev1.Pod {
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == corev1.PodReady {
				pod.Status.Conditions[i].Status = corev1.ConditionFalse
			}
		}
		return pod
	}
	leaving := func(pod *corev1.Pod) *corev1.Pod {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		return pod
	}
	leader := func() *corev1.Pod { return servingPod("engine-0-leader-0", types.RunnerLeader) }
	workers := func(n int) []*corev1.Pod {
		out := make([]*corev1.Pod, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, servingPod(fmt.Sprintf("engine-0-worker-%d", i), types.RunnerWorker))
		}
		return out
	}
	withLeader := func(lead *corev1.Pod, workers []*corev1.Pod) []*corev1.Pod {
		return append([]*corev1.Pod{lead}, workers...)
	}
	single := func() *corev1.Pod { return servingPod("engine-1-default-0", types.RunnerDefault) }

	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		want int32
	}{
		{name: "the whole gang counts every member", pods: withLeader(leader(), workers(7)), want: 8},
		{name: "a worker gone leaves the rest counting", pods: withLeader(leader(), workers(6)), want: 7},
		{name: "a worker's node died, the rest keep counting", pods: withLeader(leader(), append(workers(6), nodeLost(servingPod("engine-0-worker-6", types.RunnerWorker)))), want: 7},
		{name: "the leader is gone and every worker keeps the gate", pods: workers(7), want: 0},
		{name: "the leader's node died and every worker keeps the gate", pods: withLeader(nodeLost(leader()), workers(7)), want: 0},
		{name: "the leader is being deleted and every worker keeps the gate", pods: withLeader(leaving(leader()), workers(7)), want: 0},
		{name: "a single-pod Instance serves through its one pod", pods: []*corev1.Pod{single()}, want: 1},
		{name: "a single-pod Instance whose node died serves nothing", pods: []*corev1.Pod{nodeLost(single())}, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.CountServingPods(tc.pods); got != tc.want {
				t.Fatalf("CountServingPods = %d, want %d", got, tc.want)
			}
		})
	}

	// The counters an Instance publishes and the serving threshold the
	// Component applies to them follow the same rule: a gang with its leader
	// gone is as far out of rotation as one with no pod at all, and a gang
	// with a worker gone is short of its desired count as before.
	desired := map[int32]int32{0: 8}
	serving := func(c status.InstanceCounters) bool {
		return status.InstanceMeetsThreshold(c.PodCount, c.ServingPodCount, status.DesiredFor(desired, 0, c.PodCount))
	}
	whole, _ := status.CountersForInstance(withLeader(leader(), workers(7)), nil, status.AvailabilityWindow{}, nil)
	if whole.PodCount != 8 || whole.ServingPodCount != 8 || !serving(whole) {
		t.Fatalf("whole gang: %s, serving %t; want pods=8 serving=8 and a serving Instance", whole, serving(whole))
	}
	workerGone, _ := status.CountersForInstance(withLeader(leader(), workers(6)), nil, status.AvailabilityWindow{}, nil)
	if workerGone.PodCount != 7 || workerGone.ServingPodCount != 7 || serving(workerGone) {
		t.Fatalf("gang with a worker gone: %s, serving %t; want pods=7 serving=7 and no serving Instance", workerGone, serving(workerGone))
	}
	leaderGone, _ := status.CountersForInstance(workers(7), nil, status.AvailabilityWindow{}, nil)
	if leaderGone.PodCount != 7 || leaderGone.ServingPodCount != 0 || serving(leaderGone) {
		t.Fatalf("gang with its leader gone: %s, serving %t; want pods=7 serving=0 and no serving Instance", leaderGone, serving(leaderGone))
	}
}

// TestReachedDesiredShape_APartitionAboveTheOldCountIsMetAtWhatItHolds records
// the staged shape under a partition the roll cannot fill: a partition holds
// the Instances still on the prior revision, so a member whose every
// Instance is Ready and whose Instances off the target number between one
// and the partition rests staged. 12 Instances with 8 on the new revision
// and 4 on the old are staged under a partition of 8 (raised after 8 moved)
// and under a partition of 4 (met exactly), not under a partition of 2
// (two more must move). 4 Instances all on the old revision are staged
// under a partition of 6 (the count scaled below the partition) and under
// one of 4. A member with nothing left to hold is complete, not staged, and
// a held Instance that is not Ready keeps the member in progress.
func TestReachedDesiredShape_APartitionAboveTheOldCountIsMetAtWhatItHolds(t *testing.T) {
	ready := func(rev string) types.InstanceStatus {
		return types.InstanceStatus{Phase: types.InstancePhaseReady, RunningRevision: rev}
	}
	newRev, oldRev := "isvc-engine-newaaaaa", "isvc-engine-oldbbbbb"
	fleet := func(onNew, onOld int) []types.InstanceStatus {
		var out []types.InstanceStatus
		for i := 0; i < onNew; i++ {
			out = append(out, ready(newRev))
		}
		for i := 0; i < onOld; i++ {
			out = append(out, ready(oldRev))
		}
		return out
	}
	for _, tc := range []struct {
		name      string
		insts     []types.InstanceStatus
		partition int32
		want      bool
	}{
		{name: "partition 8 raised after 8 of 12 moved holds the 4 still old", insts: fleet(8, 4), partition: 8, want: true},
		{name: "partition 4 on 8 of 12 moved is met exactly", insts: fleet(8, 4), partition: 4, want: true},
		{name: "partition 2 on 8 of 12 moved still has two to move", insts: fleet(8, 4), partition: 2, want: false},
		{name: "partition 6 on a count scaled to 4, all still old", insts: fleet(0, 4), partition: 6, want: true},
		{name: "partition 4 on 4 Instances all still old", insts: fleet(0, 4), partition: 4, want: true},
		{name: "partition 20 above the count holds every Instance", insts: fleet(0, 12), partition: 20, want: true},
		{name: "partition 8 with every Instance moved is complete, not staged", insts: fleet(12, 0), partition: 8, want: false},
		{name: "partition 0 with every Instance moved is complete", insts: fleet(12, 0), partition: 0, want: true},
		{name: "partition 0 with one Instance still old is in progress", insts: fleet(11, 1), partition: 0, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.ReachedDesiredShape(tc.insts, newRev, tc.partition, int32(len(tc.insts))); got != tc.want {
				t.Fatalf("ReachedDesiredShape(%d on new, %d on old, partition %d) = %v, want %v",
					len(tc.insts)-int(status.HeldInstanceCount(tc.insts, newRev)), status.HeldInstanceCount(tc.insts, newRev), tc.partition, got, tc.want)
			}
		})
	}

	notReady := fleet(8, 4)
	notReady[10].Phase = types.InstancePhaseRestarting
	if status.ReachedDesiredShape(notReady, newRev, 8, 12) {
		t.Fatal("a held Instance that is not Ready keeps the member off its staged shape")
	}
	if status.ReachedDesiredShape(fleet(8, 4), newRev, 8, 13) {
		t.Fatal("a member short of an Instance is not at its staged shape")
	}
	if got := status.HeldInstanceCount(fleet(8, 4), newRev); got != 4 {
		t.Fatalf("HeldInstanceCount = %d, want the 4 Instances off the target", got)
	}
	unset := fleet(8, 3)
	unset = append(unset, types.InstanceStatus{Phase: types.InstancePhaseReady})
	if got := status.HeldInstanceCount(unset, newRev); got != 4 {
		t.Fatalf("HeldInstanceCount = %d, want an unset running revision counted as off the target", got)
	}
}
