package status

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The reading a pass takes before it builds a Ready mutation: whether the
// mutation would leave the row as it is. It must agree with the mutation
// on every row and every revision, so the pass never drops a finish the
// writer would have committed and never tries one it would not.

const (
	unchangedRevisionA = "example-engine-0a1b2c3d"
	unchangedRevisionB = "example-engine-4e5f6a7b"
)

// readyUnchangedPictures are the rows the reading is asked about, with the
// revision asked and the answer.
func readyUnchangedPictures() []struct {
	name string
	row  *types.InstanceStatus
	rev  string
	want bool
} {
	now := metav1.NewTime(time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC))
	ready := func(rev string) *types.InstanceStatus {
		return &types.InstanceStatus{Index: 3, Phase: types.InstancePhaseReady, RunningRevision: rev, ReadySince: &now}
	}
	withOperation := ready(unchangedRevisionA)
	withOperation.Operation = &types.InstanceOperation{ID: "create-3", Type: types.InstanceOperationCreate}
	withTarget := ready(unchangedRevisionA)
	withTarget.TargetRevision = unchangedRevisionA
	return []struct {
		name string
		row  *types.InstanceStatus
		rev  string
		want bool
	}{
		{"Ready on the revision asked", ready(unchangedRevisionA), unchangedRevisionA, true},
		{"Ready on another revision", ready(unchangedRevisionA), unchangedRevisionB, false},
		{"Ready with an operation still open", withOperation, unchangedRevisionA, false},
		{"Ready still naming a target", withTarget, unchangedRevisionA, false},
		{"Creating on the revision asked", &types.InstanceStatus{Index: 3, Phase: types.InstancePhaseCreating, TargetRevision: unchangedRevisionA}, unchangedRevisionA, false},
		{"Failed on the revision asked", &types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, RunningRevision: unchangedRevisionA}, unchangedRevisionA, false},
		{"Pending on the revision asked", &types.InstanceStatus{Index: 3, Phase: types.InstancePhasePending, RunningRevision: unchangedRevisionA}, unchangedRevisionA, false},
		{"Restarting on the revision asked", &types.InstanceStatus{Index: 3, Phase: types.InstancePhaseRestarting, RunningRevision: unchangedRevisionA}, unchangedRevisionA, false},
		{"no row", nil, unchangedRevisionA, false},
		{"an empty row", &types.InstanceStatus{Index: 3}, unchangedRevisionA, false},
		{"no revision asked of a Ready row", ready(unchangedRevisionA), "", true},
		{"no revision asked of no row", nil, "", true},
		{"a revision with no hash asked", ready("unnamed"), "unnamed", true},
	}
}

// TestReadyOnRevisionUnchanged_OnEveryPicture holds the reading on each
// picture and holds it to the mutation: the mutation reports a change
// exactly when the reading says the row would change, and leaves the row
// as it was otherwise.
func TestReadyOnRevisionUnchanged_OnEveryPicture(t *testing.T) {
	now := time.Date(2026, time.January, 1, 12, 30, 0, 0, time.UTC)
	for _, pic := range readyUnchangedPictures() {
		t.Run(pic.name, func(t *testing.T) {
			if got := ReadyOnRevisionUnchanged(pic.row, pic.rev); got != pic.want {
				t.Fatalf("ReadyOnRevisionUnchanged(%+v, %q) = %t, want %t", pic.row, pic.rev, got, pic.want)
			}
			tried := types.InstanceStatus{Index: 3}
			if pic.row != nil {
				tried = Clone(*pic.row)
			}
			before := Clone(tried)
			changed := ReadyOnRevisionMutation(3, pic.rev, now).Mutate(&tried)
			if changed == pic.want {
				t.Fatalf("the mutation reported changed=%t where the reading said unchanged=%t", changed, pic.want)
			}
			if !changed && !reflect.DeepEqual(tried, before) {
				t.Fatalf("the mutation reported no change and left the row as %+v, was %+v", tried, before)
			}
		})
	}
}

// TestReadyOnRevisionUnchanged_AgreesWithTheMutationOnRandomRows holds the
// reading to the mutation over random rows and revisions: the mutation
// changes a row exactly when the reading says so, an unchanged row is left
// as it was, and a changed one reads Ready on the revision with no
// operation and no target afterwards.
func TestReadyOnRevisionUnchanged_AgreesWithTheMutationOnRandomRows(t *testing.T) {
	rng := rand.New(rand.NewPCG(2026, 1106))
	now := time.Date(2026, time.January, 1, 12, 30, 0, 0, time.UTC)
	revisions := []string{"", unchangedRevisionA, unchangedRevisionB, "unnamed", "example-engine-"}
	var unchanged, changed, noRow int
	for draw := 0; draw < 250000; draw++ {
		rev := revisions[rng.IntN(len(revisions))]
		row := randomReadyUnchangedRow(rng, revisions)
		if row == nil {
			noRow++
		}
		want := ReadyOnRevisionUnchanged(row, rev)
		tried := types.InstanceStatus{Index: 3}
		if row != nil {
			tried = Clone(*row)
		}
		before := Clone(tried)
		got := ReadyOnRevisionMutation(3, rev, now).Mutate(&tried)
		if got == want {
			t.Fatalf("draw %d: row %+v rev %q: the mutation reported changed=%t where the reading said unchanged=%t", draw, row, rev, got, want)
		}
		if want {
			unchanged++
			if !reflect.DeepEqual(tried, before) {
				t.Fatalf("draw %d: an unchanged row was written: %+v, was %+v", draw, tried, before)
			}
			continue
		}
		changed++
		if tried.Phase != types.InstancePhaseReady || tried.RunningRevision != rev || tried.Operation != nil || tried.TargetRevision != "" {
			t.Fatalf("draw %d: a changed row reads %+v, want Ready on %q with no operation and no target", draw, tried, rev)
		}
	}
	t.Logf("250,000 draws: %d unchanged, %d changed, %d with no row", unchanged, changed, noRow)
}

// randomReadyUnchangedRow is nil one draw in eight, else a row in any
// phase, on any of the revisions or none, with or without an operation, a
// target and a Ready time.
func randomReadyUnchangedRow(rng *rand.Rand, revisions []string) *types.InstanceStatus {
	if rng.IntN(8) == 0 {
		return nil
	}
	phases := []types.InstancePhase{"", types.InstancePhasePending, types.InstancePhaseCreating, types.InstancePhaseReady, types.InstancePhaseUpdating,
		types.InstancePhaseRestarting, types.InstancePhaseMigrating, types.InstancePhaseFailed, types.InstancePhaseDeleting}
	row := &types.InstanceStatus{Index: int32(rng.IntN(5)), Incarnation: int64(rng.IntN(3)), Phase: phases[rng.IntN(len(phases))],
		RunningRevision: revisions[rng.IntN(len(revisions))], Admitted: rng.IntN(2) == 0}
	if rng.IntN(2) == 0 {
		row.TargetRevision = revisions[rng.IntN(len(revisions))]
	}
	if rng.IntN(2) == 0 {
		at := metav1.NewTime(time.Date(2026, time.January, 1, 11, rng.IntN(60), 0, 0, time.UTC))
		row.ReadySince = &at
	}
	if rng.IntN(3) == 0 {
		kinds := []types.InstanceOperationType{types.InstanceOperationCreate, types.InstanceOperationUpdate, types.InstanceOperationRestart, types.InstanceOperationMigrate, types.InstanceOperationDelete}
		row.Operation = &types.InstanceOperation{ID: fmt.Sprintf("op-%d", rng.IntN(100)), Type: kinds[rng.IntN(len(kinds))], TargetRevision: revisions[rng.IntN(len(revisions))]}
	}
	return row
}
