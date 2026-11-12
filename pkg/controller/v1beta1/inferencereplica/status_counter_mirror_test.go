package inferencereplica

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// mirrorRow is one stored row of a picture: an admitted row on a revision of
// its own, so the fields the mirror leaves alone are visible, with its five
// pod counters counting up from base and the node list it carries.
func mirrorRow(index int32, phase v1beta1.OMENativeInstancePhase, base int32, nodes ...string) v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{
		Index: index, Incarnation: 2, Phase: phase, RunningRevision: fmt.Sprintf("rev-%d", index), Admitted: true,
		PodCount: base, ReadyPodCount: base + 1, ServingPodCount: base + 2, ScheduledPodCount: base + 3, AvailablePodCount: base + 4,
		NodesOccupied: nodes,
	}
}

// mirrorEntry is one publication entry of a picture, its five pod counters
// counting up from base.
func mirrorEntry(index int32, phase workloadtypes.InstancePhase, base int32, nodes []string) workloadtypes.InstanceStatus {
	return workloadtypes.InstanceStatus{
		Index: index, Phase: phase,
		PodCount: base, ReadyPodCount: base + 1, ServingPodCount: base + 2, ScheduledPodCount: base + 3, AvailablePodCount: base + 4,
		NodesOccupied: nodes,
	}
}

// mirrorPicture is one picture of the mirror: the rows the pass's copy
// holds, the publication the status write computed, and the rows after the
// mirror.
type mirrorPicture struct {
	name        string
	rows        []v1beta1.OMENativeInstanceStatus
	publication []workloadtypes.InstanceStatus
	want        []v1beta1.OMENativeInstanceStatus
}

func mirrorPictures() []mirrorPicture {
	ready, restarting := v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceRestarting
	wReady, wRestarting, wPending := workloadtypes.InstancePhaseReady, workloadtypes.InstancePhaseRestarting, workloadtypes.InstancePhasePending
	return []mirrorPicture{
		{
			name: "a publication built from the rows lists the same Instances in the same order; each row takes the entry at its position",
			rows: []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10), mirrorRow(1, ready, 20), mirrorRow(2, ready, 30)},
			publication: []workloadtypes.InstanceStatus{
				mirrorEntry(0, wReady, 1, []string{"node-a"}), mirrorEntry(1, wRestarting, 2, []string{"node-b", "node-c"}), mirrorEntry(2, wPending, 3, nil)},
			want: []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 1, "node-a"), mirrorRow(1, restarting, 2, "node-b", "node-c"), mirrorRow(2, v1beta1.OMENativeInstancePending, 3)},
		},
		{
			name:        "a row the publication leaves out keeps its phase, counters and nodes",
			rows:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10, "old"), mirrorRow(1, ready, 20, "old"), mirrorRow(2, ready, 30, "old")},
			publication: []workloadtypes.InstanceStatus{mirrorEntry(0, wReady, 1, []string{"node-a"}), mirrorEntry(2, wReady, 3, []string{"node-c"})},
			want:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 1, "node-a"), mirrorRow(1, ready, 20, "old"), mirrorRow(2, ready, 3, "node-c")},
		},
		{
			name:        "an entry of an Instance with no row is ignored",
			rows:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10), mirrorRow(1, ready, 20)},
			publication: []workloadtypes.InstanceStatus{mirrorEntry(0, wReady, 1, nil), mirrorEntry(1, wReady, 2, nil), mirrorEntry(7, wReady, 3, []string{"node-x"})},
			want:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 1), mirrorRow(1, ready, 2)},
		},
		{
			name:        "rows in another order than an ascending publication each take the entry of their own Instance",
			rows:        []v1beta1.OMENativeInstanceStatus{mirrorRow(2, ready, 30), mirrorRow(0, ready, 10), mirrorRow(1, ready, 20)},
			publication: []workloadtypes.InstanceStatus{mirrorEntry(0, wReady, 1, []string{"node-a"}), mirrorEntry(1, wReady, 2, []string{"node-b"}), mirrorEntry(2, wReady, 3, []string{"node-c"})},
			want:        []v1beta1.OMENativeInstanceStatus{mirrorRow(2, ready, 3, "node-c"), mirrorRow(0, ready, 1, "node-a"), mirrorRow(1, ready, 2, "node-b")},
		},
		{
			name:        "a publication out of index order is read by Instance",
			rows:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10), mirrorRow(1, ready, 20), mirrorRow(2, ready, 30)},
			publication: []workloadtypes.InstanceStatus{mirrorEntry(2, wReady, 3, []string{"node-c"}), mirrorEntry(0, wReady, 1, []string{"node-a"}), mirrorEntry(1, wReady, 2, nil)},
			want:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 1, "node-a"), mirrorRow(1, ready, 2), mirrorRow(2, ready, 3, "node-c")},
		},
		{
			name:        "the last entry of a repeated Instance wins",
			rows:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10), mirrorRow(1, ready, 20)},
			publication: []workloadtypes.InstanceStatus{mirrorEntry(0, wReady, 1, nil), mirrorEntry(1, wReady, 2, []string{"first"}), mirrorEntry(1, wRestarting, 5, []string{"last"})},
			want:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 1), mirrorRow(1, restarting, 5, "last")},
		},
		{
			name:        "a repeated Instance among rows aligned with repeated entries: every row of it takes the last entry",
			rows:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10), mirrorRow(1, ready, 20), mirrorRow(1, ready, 30)},
			publication: []workloadtypes.InstanceStatus{mirrorEntry(0, wReady, 1, nil), mirrorEntry(1, wReady, 2, []string{"first"}), mirrorEntry(1, wPending, 5, []string{"last"})},
			want:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 1), mirrorRow(1, v1beta1.OMENativeInstancePending, 5, "last"), mirrorRow(1, v1beta1.OMENativeInstancePending, 5, "last")},
		},
		{
			name:        "an entry with no node list clears the row's, an empty list too",
			rows:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10, "old-a"), mirrorRow(1, ready, 20, "old-b")},
			publication: []workloadtypes.InstanceStatus{mirrorEntry(0, wReady, 1, nil), mirrorEntry(1, wReady, 2, []string{})},
			want:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 1), mirrorRow(1, ready, 2)},
		},
		{
			name:        "an empty publication mirrors nothing",
			rows:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10, "old")},
			publication: nil,
			want:        []v1beta1.OMENativeInstanceStatus{mirrorRow(0, ready, 10, "old")},
		},
	}
}

// Each picture of the mirror reads as spelled: a row takes the phase and the
// five pod counters of its Instance's entry, the last one when the Instance
// repeats, and the entry's node list as a list of its own; a row with no
// entry keeps what it had, and nothing else on a row moves.
func TestMirrorInstanceCounters_OnEveryPicture(t *testing.T) {
	for _, picture := range mirrorPictures() {
		t.Run(picture.name, func(t *testing.T) {
			status := &v1beta1.InferenceReplicaStatus{InstanceStatuses: slices.Clone(picture.rows)}
			mirrorInstanceCounters(status, picture.publication)
			if !reflect.DeepEqual(status.InstanceStatuses, picture.want) {
				t.Errorf("rows after the mirror:\n got %+v\nwant %+v", status.InstanceStatuses, picture.want)
			}
		})
	}
}

// A row's node list is the row's own: neither the publication's list nor a
// neighbour row's. Writing through the publication after the mirror and
// growing one row's list leave every other list as it was.
func TestMirrorInstanceCounters_MirroredNodeListsAreTheRowsOwn(t *testing.T) {
	status := &v1beta1.InferenceReplicaStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
		mirrorRow(0, v1beta1.OMENativeInstanceReady, 10), mirrorRow(1, v1beta1.OMENativeInstanceReady, 20), mirrorRow(2, v1beta1.OMENativeInstanceReady, 30)}}
	publication := []workloadtypes.InstanceStatus{
		mirrorEntry(0, workloadtypes.InstancePhaseReady, 1, []string{"node-a"}),
		mirrorEntry(1, workloadtypes.InstancePhaseReady, 2, []string{"node-b", "node-c"}),
		mirrorEntry(2, workloadtypes.InstancePhaseReady, 3, []string{"node-d"}),
	}
	mirrorInstanceCounters(status, publication)
	rows := status.InstanceStatuses
	for i := range publication {
		if &rows[i].NodesOccupied[0] == &publication[i].NodesOccupied[0] {
			t.Errorf("Instance %d: the row's node list is the publication's list", i)
		}
		publication[i].NodesOccupied[0] = "changed"
	}
	rows[0].NodesOccupied = append(rows[0].NodesOccupied, "grown")
	rows[1].NodesOccupied = append(rows[1].NodesOccupied, "grown")
	want := [][]string{{"node-a", "grown"}, {"node-b", "node-c", "grown"}, {"node-d"}}
	for i := range rows {
		if !slices.Equal(rows[i].NodesOccupied, want[i]) {
			t.Errorf("Instance %d: nodes = %v, want %v", i, rows[i].NodesOccupied, want[i])
		}
	}
}

// A nil status mirrors nothing and does not fail.
func TestMirrorInstanceCounters_NoStatusMirrorsNothing(t *testing.T) {
	mirrorInstanceCounters(nil, []workloadtypes.InstanceStatus{mirrorEntry(0, workloadtypes.InstancePhaseReady, 1, []string{"node-a"})})
}

// lastEntryOf is the entry of an Instance in a publication, the last one
// when the Instance repeats.
func lastEntryOf(publication []workloadtypes.InstanceStatus, index int32) (workloadtypes.InstanceStatus, bool) {
	for i := len(publication) - 1; i >= 0; i-- {
		if publication[i].Index == index {
			return publication[i], true
		}
	}
	return workloadtypes.InstanceStatus{}, false
}

// mirroredRow is a row after the mirror, spelled field by field: the
// entry's phase and five pod counters, and a copy of its node list, none
// when it has none.
func mirroredRow(row v1beta1.OMENativeInstanceStatus, entry workloadtypes.InstanceStatus) v1beta1.OMENativeInstanceStatus {
	row.Phase = v1beta1convert.InstancePhaseFromWorkload(entry.Phase)
	row.PodCount, row.ReadyPodCount, row.ServingPodCount = entry.PodCount, entry.ReadyPodCount, entry.ServingPodCount
	row.ScheduledPodCount, row.AvailablePodCount = entry.ScheduledPodCount, entry.AvailablePodCount
	row.NodesOccupied = nil
	if len(entry.NodesOccupied) > 0 {
		row.NodesOccupied = slices.Clone(entry.NodesOccupied)
	}
	return row
}

// mirrorDraw is one random picture of the mirror: rows in ascending,
// shuffled or repeating index order, and a publication built from them with
// entries dropped, added, repeated or shuffled.
type mirrorDraw struct {
	rows        []v1beta1.OMENativeInstanceStatus
	publication []workloadtypes.InstanceStatus
}

func drawMirrorPicture(r *rand.Rand) mirrorDraw {
	phases := []workloadtypes.InstancePhase{workloadtypes.InstancePhasePending, workloadtypes.InstancePhaseReady,
		workloadtypes.InstancePhaseRestarting, workloadtypes.InstancePhaseUpdating, workloadtypes.InstancePhaseDeleting, workloadtypes.InstancePhaseFailed}
	nodes := func() []string {
		switch r.IntN(10) {
		case 0, 1, 2:
			return nil
		case 3:
			return []string{}
		}
		out := make([]string, 1+r.IntN(3))
		for i := range out {
			out[i] = fmt.Sprintf("node-%d", r.IntN(6))
		}
		return out
	}
	indexes := r.Perm(16)[:r.IntN(13)]
	switch r.IntN(4) {
	case 0, 1:
		slices.Sort(indexes)
	case 2:
		if len(indexes) > 1 {
			indexes = append(indexes, indexes[r.IntN(len(indexes))])
		}
	}
	var draw mirrorDraw
	for _, index := range indexes {
		draw.rows = append(draw.rows, mirrorRow(int32(index), v1beta1.OMENativeInstancePhase(v1beta1convert.InstancePhaseFromWorkload(phases[r.IntN(len(phases))])), int32(r.IntN(9)), nodes()...))
		draw.publication = append(draw.publication, mirrorEntry(int32(index), phases[r.IntN(len(phases))], int32(r.IntN(9)), nodes()))
	}
	switch r.IntN(5) {
	case 0:
		draw.publication = slices.DeleteFunc(draw.publication, func(workloadtypes.InstanceStatus) bool { return r.IntN(3) == 0 })
	case 1:
		draw.publication = append(draw.publication, mirrorEntry(int32(16+r.IntN(4)), phases[r.IntN(len(phases))], int32(r.IntN(9)), nodes()))
	case 2:
		if len(draw.publication) > 0 {
			draw.publication = append(draw.publication, mirrorEntry(draw.publication[r.IntN(len(draw.publication))].Index, phases[r.IntN(len(phases))], int32(r.IntN(9)), nodes()))
		}
	case 3:
		r.Shuffle(len(draw.publication), func(i, j int) { draw.publication[i], draw.publication[j] = draw.publication[j], draw.publication[i] })
	}
	return draw
}

// Over random pictures the mirror agrees, row by row, with the mirror
// spelled field by field through the last entry of each Instance, and keeps
// agreeing after the publication's node lists are written through.
func TestMirrorInstanceCounters_AgreesWithTheSpelledMirrorOnRandomPictures(t *testing.T) {
	r := rand.New(rand.NewPCG(33, 1108))
	for draw := 0; draw < 20000; draw++ {
		picture := drawMirrorPicture(r)
		want := slices.Clone(picture.rows)
		for i := range want {
			if entry, found := lastEntryOf(picture.publication, want[i].Index); found {
				want[i] = mirroredRow(want[i], entry)
			}
		}
		status := &v1beta1.InferenceReplicaStatus{InstanceStatuses: slices.Clone(picture.rows)}
		mirrorInstanceCounters(status, picture.publication)
		if !reflect.DeepEqual(status.InstanceStatuses, want) {
			t.Fatalf("draw %d: rows after the mirror:\n got %+v\nwant %+v\npublication %+v", draw, status.InstanceStatuses, want, picture.publication)
		}
		for i := range picture.publication {
			for j := range picture.publication[i].NodesOccupied {
				picture.publication[i].NodesOccupied[j] = "written-through"
			}
		}
		if !reflect.DeepEqual(status.InstanceStatuses, want) {
			t.Fatalf("draw %d: a row's node list changed when the publication's was written through:\n got %+v\nwant %+v", draw, status.InstanceStatuses, want)
		}
	}
}
