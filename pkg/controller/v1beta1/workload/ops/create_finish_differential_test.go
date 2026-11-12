package ops

import (
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// finishByTrial is the finish decided the copying way: the Ready mutation
// built for the Instance and tried on a copy of its row, the answer kept
// as the mutation's mark. The pass decides on the row first instead; this
// form is kept here to hold the two together.
func finishByTrial(observed *workload.InstanceStatus, idx int32, rev string, now time.Time) (workload.InstanceMutation, bool) {
	row := workload.InstanceStatus{Index: idx}
	if observed != nil {
		row = *observed
	}
	mutation := status.ReadyOnRevisionMutation(idx, rev, now)
	changes := mutation.Mutate(&row)
	mutation.UnchangedOnCopy = !changes
	return mutation, changes
}

// TestCreateFinish_AgreesWithTheTrialFormOnRandomRows holds the pass's
// reading of a finish and the mutation it hands the writer to the copying
// form over random rows and revisions: the same answer on whether the row
// changes, the same mark on the mutation, and, applied to any row the
// writer may hold, the same answer and the same row afterwards, whichever
// Instance of the pass the shared mutation was handed to first.
func TestCreateFinish_AgreesWithTheTrialFormOnRandomRows(t *testing.T) {
	rng := rand.New(rand.NewPCG(2026, 1107))
	now := createFinishNow
	revisions := []string{"", finishRevisionA, finishRevisionB, "unnamed"}
	pass := &createPass{input: workload.ReconcileInput{Clock: clocktesting.NewFakeClock(now)}}
	var unchanged, changed, applied int
	for draw := 0; draw < 250000; draw++ {
		if draw%5000 == 0 {
			pass.readyOnRevision = nil
		}
		idx := int32(rng.IntN(6))
		rev := revisions[rng.IntN(len(revisions))]
		observed := randomFinishRow(rng, revisions)
		trial, changes := finishByTrial(observed, idx, rev, now)
		if got := !status.ReadyOnRevisionUnchanged(observed, rev); got != changes {
			t.Fatalf("draw %d: row %+v rev %q: the reading says changes=%t, the trial %t", draw, observed, rev, got, changes)
		}
		if changes {
			changed++
			continue
		}
		unchanged++
		shared := pass.unchangedFinishMutation(idx, rev)
		if shared.Index != idx || !shared.UnchangedOnCopy || shared.Mutate == nil || shared.Precondition == nil {
			t.Fatalf("draw %d: the shared mutation is %+v, want Instance %d marked unchanged with its closures", draw, shared, idx)
		}
		for _, held := range []*workload.InstanceStatus{observed, randomFinishRow(rng, revisions)} {
			applied++
			holdSameEffect(t, draw, trial, shared, held)
		}
	}
	t.Logf("250,000 draws: %d unchanged (each applied to two rows, %d in all), %d changed", unchanged, applied, changed)
}

// holdSameEffect applies the two mutations to copies of the row the writer
// holds and holds them to the same precondition answer, the same change
// answer and the same row afterwards.
func holdSameEffect(t *testing.T, draw int, trial, shared workload.InstanceMutation, held *workload.InstanceStatus) {
	t.Helper()
	byTrial, byShared := workload.InstanceStatus{Index: trial.Index}, workload.InstanceStatus{Index: shared.Index}
	if held != nil {
		byTrial, byShared = status.Clone(*held), status.Clone(*held)
	}
	if trial.Precondition(&byTrial) != shared.Precondition(&byShared) {
		t.Fatalf("draw %d: the preconditions disagree on %+v", draw, held)
	}
	if trial.Mutate(&byTrial) != shared.Mutate(&byShared) || !reflect.DeepEqual(byTrial, byShared) {
		t.Fatalf("draw %d: applied to %+v the trial form leaves %+v and the shared mutation %+v", draw, held, byTrial, byShared)
	}
}

// randomFinishRow is nil one draw in six, else a row in any phase on any
// revision or none, with or without an open operation, a target and a
// Ready time.
func randomFinishRow(rng *rand.Rand, revisions []string) *workload.InstanceStatus {
	if rng.IntN(6) == 0 {
		return nil
	}
	phases := []workload.InstancePhase{"", workload.InstancePhasePending, workload.InstancePhaseCreating, workload.InstancePhaseReady,
		workload.InstancePhaseUpdating, workload.InstancePhaseRestarting, workload.InstancePhaseFailed, workload.InstancePhaseDeleting}
	row := &workload.InstanceStatus{Index: int32(rng.IntN(6)), Incarnation: int64(rng.IntN(3)), Phase: phases[rng.IntN(len(phases))],
		RunningRevision: revisions[rng.IntN(len(revisions))], Admitted: rng.IntN(2) == 0}
	if rng.IntN(2) == 0 {
		row.TargetRevision = revisions[rng.IntN(len(revisions))]
	}
	if rng.IntN(2) == 0 {
		at := metav1.NewTime(createFinishNow.Add(-time.Duration(rng.IntN(3600)) * time.Second))
		row.ReadySince = &at
	}
	if rng.IntN(3) == 0 {
		kinds := []workload.InstanceOperationType{workload.InstanceOperationCreate, workload.InstanceOperationUpdate, workload.InstanceOperationRestart}
		row.Operation = &workload.InstanceOperation{ID: "op", Type: kinds[rng.IntN(len(kinds))], TargetRevision: revisions[rng.IntN(len(revisions))]}
	}
	return row
}
