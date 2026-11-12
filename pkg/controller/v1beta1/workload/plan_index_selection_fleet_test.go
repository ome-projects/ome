package workload

import (
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

// indexSelectionShape is one shape of a Component: how many Instances it
// has and how many pods each runs.
type indexSelectionShape struct {
	name            string
	instances, pods int
}

// indexSelectionShapes are the budget's shapes: the largest reference
// replica of single-pod Instances and a quarter of it, 160 gangs of a
// leader and seven workers, and the largest reference gang service,
// 1,725 gangs of a leader and one worker.
func indexSelectionShapes() []indexSelectionShape {
	return []indexSelectionShape{
		{name: "1000x1", instances: 1000, pods: 1},
		{name: "4000x1", instances: 4000, pods: 1},
		{name: "160x8", instances: 160, pods: 8},
		{name: "1725x2", instances: 1725, pods: 2},
	}
}

// indexSelectionMoment is one moment of a Component's life the selection
// is read at: the rows it records then and the indexes the plan keeps.
type indexSelectionMoment struct {
	name    string
	picture func(shape indexSelectionShape) indexPicture
	want    func(shape indexSelectionShape) []int32
}

// pairedEvery is how many Instances share one surge pair at the moments
// that have pairs: one Instance in ten is a source.
const pairedEvery = 10

// indexSelectionMoments are the moments: at rest, every row Ready; a
// scale-up with a quarter of the count still to add; one Instance in ten
// mid gang surge with its replacement row claimed; the same pairs after
// the attempt failed; and the same pairs with the replacement promoted
// and the source retiring.
func indexSelectionMoments() []indexSelectionMoment {
	return []indexSelectionMoment{
		{name: "at-rest",
			picture: func(s indexSelectionShape) indexPicture {
				return indexPicture{rows: restingRows(s.instances, s.pods), replicas: int32(s.instances)}
			},
			want: func(s indexSelectionShape) []int32 { return ascendingIndexes(0, s.instances) }},
		{name: "scale-up",
			picture: func(s indexSelectionShape) indexPicture {
				return indexPicture{rows: restingRows(s.instances*3/4, s.pods), replicas: int32(s.instances)}
			},
			want: func(s indexSelectionShape) []int32 { return ascendingIndexes(0, s.instances) }},
		{name: "surge-pairs",
			picture: func(s indexSelectionShape) indexPicture {
				return indexPicture{rows: pairedRows(s, types.InstancePhaseUpdating, types.UpdateStepSurge), replicas: int32(s.instances)}
			},
			want: func(s indexSelectionShape) []int32 { return ascendingIndexes(0, s.instances+pairsOf(s)) }},
		{name: "failed-pairs",
			picture: func(s indexSelectionShape) indexPicture {
				return indexPicture{rows: pairedRows(s, types.InstancePhaseFailed, types.UpdateStepSurge), replicas: int32(s.instances)}
			},
			want: func(s indexSelectionShape) []int32 { return ascendingIndexes(0, s.instances+pairsOf(s)) }},
		{name: "retiring",
			picture: func(s indexSelectionShape) indexPicture {
				return indexPicture{rows: pairedRows(s, types.InstancePhaseUpdating, types.UpdateStepSurgeDrain), replicas: int32(s.instances)}
			},
			want: func(s indexSelectionShape) []int32 {
				var kept []int32
				for _, idx := range ascendingIndexes(0, s.instances+pairsOf(s)) {
					if idx >= int32(s.instances) || idx%pairedEvery != 0 {
						kept = append(kept, idx)
					}
				}
				return kept
			}},
	}
}

func pairsOf(s indexSelectionShape) int { return (s.instances + pairedEvery - 1) / pairedEvery }

func ascendingIndexes(from, to int) []int32 {
	out := make([]int32, 0, to-from)
	for idx := from; idx < to; idx++ {
		out = append(out, int32(idx))
	}
	return out
}

// restingRows are count rows at rest: every Instance Ready on the spec
// revision with all its pods serving.
func restingRows(count, pods int) []types.InstanceStatus {
	rows := make([]types.InstanceStatus, count)
	for i := range rows {
		rows[i] = types.InstanceStatus{
			Index: int32(i), Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: pictureRevisionA,
			PodCount: int32(pods), ServingPodCount: int32(pods), AvailablePodCount: int32(pods), Admitted: true,
		}
	}
	return rows
}

// pairedRows are the shape's rows with every tenth Instance the source of
// a gang surge, at the phase and step given, and its replacement row
// claimed above the count: a Creating surge-target row while the source
// is at the Surge step, a Failed one once the source has failed, and a
// promoted Ready row on the target revision once the source drains.
func pairedRows(s indexSelectionShape, phase types.InstancePhase, step string) []types.InstanceStatus {
	rows := restingRows(s.instances, s.pods)
	for source := 0; source < s.instances; source += pairedEvery {
		surge := int32(s.instances + source/pairedEvery)
		rows[source].Phase = phase
		rows[source].TargetRevision = pictureRevisionB
		rows[source].Operation = &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: step, SurgeIndex: &surge, TargetRevision: pictureRevisionB}
		switch {
		case step == types.UpdateStepSurgeDrain:
			rows = append(rows, promotedReplacement(surge, pictureRevisionB))
		case phase == types.InstancePhaseFailed:
			rows = append(rows, gangSurgeReplacementRow(surge, types.InstancePhaseFailed, types.UpdateStepGangSurgeTarget))
		default:
			rows = append(rows, gangSurgeReplacementRow(surge, types.InstancePhaseCreating, types.UpdateStepGangSurgeTarget))
		}
	}
	return rows
}

// restingPlanInput is the desired spec and the observed state BuildPlan
// reads for a Component at rest of the shape.
func restingPlanInput(s indexSelectionShape) (types.WorkloadDesiredSpec, types.WorkloadObservedState) {
	desired := types.WorkloadDesiredSpec{Replicas: int32(s.instances), Runners: []types.Runner{{Name: types.RunnerDefault, Size: 1}}}
	if s.pods > 1 {
		desired.MultiPod = true
		desired.Runners = []types.Runner{{Name: types.RunnerLeader, Size: 1}, {Name: types.RunnerWorker, Size: int32(s.pods - 1)}}
	}
	return desired, types.WorkloadObservedState{InstanceStatuses: restingRows(s.instances, s.pods)}
}

// TestIndexSelectionStaysWithinBudget holds the allocations and the bytes
// of the plan's index selection at each shape and moment, and of the whole
// plan at rest, to the recorded golden, so a change in the plan's cost is
// made on purpose and carries its reason. Beside the golden, the indexes
// the selection keeps at each moment are the ones the moment names.
func TestIndexSelectionStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "index_selection_allocations.json"), *updateIndexSelection)
	defer golden.Close()
	for _, shape := range indexSelectionShapes() {
		for _, moment := range indexSelectionMoments() {
			pic := moment.picture(shape)
			if diff := cmp.Diff(moment.want(shape), pic.chosen()); diff != "" {
				t.Errorf("%s/%s: the indexes kept (-want +got):\n%s", shape.name, moment.name, diff)
			}
			prefix := shape.name + "/" + moment.name + "/selection"
			golden.Check(prefix+"/allocations", func() { pic.chosen() })
			golden.CheckBytes(prefix+"/bytes", func() { pic.chosen() })
		}
		desired, observed := restingPlanInput(shape)
		prefix := shape.name + "/at-rest/BuildPlan"
		golden.Check(prefix+"/allocations", func() { mustBuildPlan(t, desired, observed) })
		golden.CheckBytes(prefix+"/bytes", func() { mustBuildPlan(t, desired, observed) })
	}
}

func mustBuildPlan(tb testing.TB, desired types.WorkloadDesiredSpec, observed types.WorkloadObservedState) {
	if _, err := BuildPlan(types.ComponentEngine, desired, observed); err != nil {
		tb.Fatal(err)
	}
}

// BenchmarkIndexSelectionAtRest is the cost of the plan's index selection
// at each shape and moment, and of the whole plan at rest.
func BenchmarkIndexSelectionAtRest(b *testing.B) {
	for _, shape := range indexSelectionShapes() {
		for _, moment := range indexSelectionMoments() {
			pic := moment.picture(shape)
			b.Run(shape.name+"/"+moment.name+"/selection", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					pic.chosen()
				}
			})
		}
		desired, observed := restingPlanInput(shape)
		b.Run(shape.name+"/at-rest/BuildPlan", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				mustBuildPlan(b, desired, observed)
			}
		})
	}
}
