package holds

import (
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// copiedRow is a pass row as RowsForPlan built it before it read the
// observed row in place: the row copied beside the same three readings.
type copiedRow struct {
	Status   types.InstanceStatus
	Desired  int32
	Instance *types.InstancePlan
	Excluded bool
}

// rowsForPlanCopied is the build RowsForPlan replaces, kept here as the
// reference.
func rowsForPlanCopied(plan types.ComponentPlan, statuses []types.InstanceStatus, excluded map[int32]struct{}) []copiedRow {
	desiredByIdx := status.DesiredPodCountByInstance(plan)
	planned := make(map[int32]*types.InstancePlan, len(plan.Instances))
	for i := range plan.Instances {
		planned[plan.Instances[i].Index] = &plan.Instances[i]
	}
	rows := make([]copiedRow, 0, len(statuses))
	for _, row := range statuses {
		_, skip := excluded[row.Index]
		rows = append(rows, copiedRow{
			Status:   row,
			Desired:  status.DesiredFor(desiredByIdx, row.Index, row.PodCount),
			Instance: planned[row.Index],
			Excluded: skip,
		})
	}
	return rows
}

// randomComponent is one plan over sparse indexes, the observed rows in any
// order with repeated indexes, rows the plan covers or not in any phase with
// any pod count and an operation on some, and the rows the scale-down holds
// out.
func randomComponent(rng *rand.Rand) (types.ComponentPlan, []types.InstanceStatus, map[int32]struct{}) {
	phases := []types.InstancePhase{types.InstancePhaseReady, types.InstancePhaseCreating, types.InstancePhaseUpdating, types.InstancePhaseDeleting, types.InstancePhaseFailed, types.InstancePhaseRestarting}
	plan := types.ComponentPlan{}
	for i, n := 0, rng.IntN(13); i < n; i++ {
		plan.Instances = append(plan.Instances, types.InstancePlan{Index: int32(rng.IntN(16)), Runners: []types.RunnerPlan{{Name: types.RunnerLeader, Size: 1}, {Name: types.RunnerWorker, Size: int32(rng.IntN(8))}}})
	}
	var statuses []types.InstanceStatus
	excluded := map[int32]struct{}{}
	for i, n := 0, rng.IntN(13); i < n; i++ {
		row := types.InstanceStatus{Index: int32(rng.IntN(16)), Incarnation: int64(rng.IntN(3)), Phase: phases[rng.IntN(len(phases))], PodCount: int32(rng.IntN(9)), ServingPodCount: int32(rng.IntN(9))}
		if rng.IntN(2) == 0 {
			row.Operation = &types.InstanceOperation{ID: "op", Type: types.InstanceOperationUpdate, Step: "InPlace", Waiting: types.WaitingReasonPaused}
		}
		if rng.IntN(4) == 0 {
			excluded[row.Index] = struct{}{}
		}
		statuses = append(statuses, row)
	}
	if rng.IntN(3) == 0 {
		excluded = nil
	}
	return plan, statuses, excluded
}

// TestRowsForPlan_AgreesWithTheCopyingFormOnRandomRows draws 100,000
// Components and holds every pass row to the copied one: the observed row
// itself, read in place; the same desired count, the same entry and the
// same exclusion, in the same order.
func TestRowsForPlan_AgreesWithTheCopyingFormOnRandomRows(t *testing.T) {
	rng := rand.New(rand.NewPCG(27, 1106))
	const draws = 100000
	var rows, covered, excludedRows int
	for draw := 0; draw < draws; draw++ {
		plan, statuses, excluded := randomComponent(rng)
		want := rowsForPlanCopied(plan, statuses, excluded)
		got := RowsForPlan(plan, statuses, excluded)
		if len(got) != len(want) {
			t.Fatalf("draw %d: %d rows, the copy built %d", draw, len(got), len(want))
		}
		for i := range got {
			if got[i].Status != &statuses[i] {
				t.Fatalf("draw %d, row %d: the row is not the observed row itself", draw, i)
			}
			if diff := cmp.Diff(want[i].Status, *got[i].Status); diff != "" {
				t.Fatalf("draw %d, row %d: (-copied +read):\n%s", draw, i, diff)
			}
			if got[i].Desired != want[i].Desired || got[i].Instance != want[i].Instance || got[i].Excluded != want[i].Excluded {
				t.Fatalf("draw %d, row %d: desired %d, entry %p, excluded %t; the copy reads %d, %p, %t", draw, i, got[i].Desired, got[i].Instance, got[i].Excluded, want[i].Desired, want[i].Instance, want[i].Excluded)
			}
			rows++
			if got[i].Instance != nil {
				covered++
			}
			if got[i].Excluded {
				excludedRows++
			}
		}
	}
	t.Logf("%d draws: %d rows, %d covered by the plan, %d held out", draws, rows, covered, excludedRows)
}
