package inferencereplica

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// writePublicationOverACopy is the status write's row write as a reading
// of every row: each row's publication entry written onto a deep copy of
// the row, the pod-derived fields among them, and the three pod-derived
// fields cleared as the store never holds them. The copy is what the write
// would store; compared with the row it came from, it says whether the row
// moves.
func writePublicationOverACopy(rows []v1beta1.OMENativeInstanceStatus, publication []workloadtypes.InstanceStatus) []v1beta1.OMENativeInstanceStatus {
	copied := make([]v1beta1.OMENativeInstanceStatus, len(rows))
	for i := range rows {
		copied[i] = *rows[i].DeepCopy()
		row := &copied[i]
		row.Phase = v1beta1convert.InstancePhaseFromWorkload(publication[i].Phase)
		row.PodCount = publication[i].PodCount
		row.ReadyPodCount = publication[i].ReadyPodCount
		row.ServingPodCount = publication[i].ServingPodCount
		row.AvailablePodCount = publication[i].AvailablePodCount
		row.ScheduledPodCount = publication[i].ScheduledPodCount
		row.Admitted = publication[i].Admitted
		row.NodesOccupied = publication[i].NodesOccupied
	}
	irstatus.ClearPodDerivedObservations(copied)
	return copied
}

// randomPublishedRows draws up to six rows with every field random, each
// with a publication entry: half the entries carry the row's five published
// values, so the settled answer is reached as often as the unsettled one;
// the three pod-derived fields are cleared on most rows and set on some,
// and the entries carry pod-derived values of their own.
func randomPublishedRows(r *rand.Rand, now time.Time) ([]v1beta1.OMENativeInstanceStatus, []workloadtypes.InstanceStatus) {
	phases := []v1beta1.OMENativeInstancePhase{"", v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating,
		v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting,
		v1beta1.OMENativeInstanceMigrating, v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceDeleting}
	enginePhases := []workloadtypes.InstancePhase{workloadtypes.InstancePhaseEmpty, workloadtypes.InstancePhasePending,
		workloadtypes.InstancePhaseCreating, workloadtypes.InstancePhaseReady, workloadtypes.InstancePhaseUpdating,
		workloadtypes.InstancePhaseRestarting, workloadtypes.InstancePhaseMigrating, workloadtypes.InstancePhaseFailed,
		workloadtypes.InstancePhaseDeleting}
	revisions := []string{"", "rev-a", "rev-b"}
	nodes := func() []string { return [][]string{nil, {}, {"node-1"}}[r.IntN(3)] }
	at := metav1.NewTime(now)
	count := r.IntN(7)
	rows := make([]v1beta1.OMENativeInstanceStatus, count)
	publication := make([]workloadtypes.InstanceStatus, count)
	for i := range rows {
		row := v1beta1.OMENativeInstanceStatus{
			Index: int32(i), Incarnation: int64(r.IntN(3)), Phase: phases[r.IntN(len(phases))],
			RunningRevision: revisions[r.IntN(3)], TargetRevision: revisions[r.IntN(3)],
			PodCount: int32(r.IntN(4)), ServingPodCount: int32(r.IntN(4)), AvailablePodCount: int32(r.IntN(4)),
			Admitted: r.IntN(2) == 0, ActiveOrdinal: int32(r.IntN(2)),
		}
		if r.IntN(5) == 0 {
			row.ReadyPodCount, row.ScheduledPodCount, row.NodesOccupied = int32(r.IntN(3)), int32(r.IntN(3)), nodes()
		}
		if r.IntN(2) == 0 {
			row.ReadySince = &at
		}
		if r.IntN(3) == 0 {
			row.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "Surge", StartedAt: at}
		}
		if r.IntN(4) == 0 {
			row.LastFailure = &v1beta1.InstanceTermination{Reason: "OOMKilled", Time: at}
		}
		if r.IntN(3) == 0 {
			row.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, LastTransitionTime: at}}
		}
		if r.IntN(3) == 0 {
			row.Announced = []string{"engine-0"}
		}
		rows[i] = row
		entry := workloadtypes.InstanceStatus{
			Index: row.Index, Phase: enginePhases[r.IntN(len(enginePhases))],
			PodCount: int32(r.IntN(4)), ServingPodCount: int32(r.IntN(4)), AvailablePodCount: int32(r.IntN(4)),
			ReadyPodCount: int32(r.IntN(3)), ScheduledPodCount: int32(r.IntN(3)), Admitted: r.IntN(2) == 0, NodesOccupied: nodes(),
		}
		if r.IntN(2) == 0 {
			entry.Phase = v1beta1convert.InstancePhaseToWorkload(row.Phase)
			entry.PodCount, entry.ServingPodCount, entry.AvailablePodCount, entry.Admitted = row.PodCount, row.ServingPodCount, row.AvailablePodCount, row.Admitted
		}
		publication[i] = entry
	}
	return rows, publication
}

// The in-place reading of the rows agrees with the write over a copy of
// every row: over generated rows and publications, publishedRowsSettled
// answers what comparing each row with its publication written onto a
// deep copy answers, under the compare the status write uses. The cases
// reach rows that settle, rows that move in a published field, and rows
// that move only because a pod-derived field is set on the copy's side.
func TestPublishedRowsSettled_AgreesWithTheWriteOverACopyOfEveryRow(t *testing.T) {
	const cases = 300000
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewPCG(22, 18))
	reached := map[string]int{}
	for n := 0; n < cases; n++ {
		rows, publication := randomPublishedRows(r, now)
		written := writePublicationOverACopy(rows, publication)
		want := instanceStatusesEqual(rows, written)
		got := publishedRowsSettled(rows, publication)
		if got != want {
			t.Fatalf("case %d: publishedRowsSettled() = %v, the write over a copy says %v\nrows %+v\npublication %+v", n, got, want, rows, publication)
		}
		switch {
		case want:
			reached["settled"]++
		case publishedFieldsMatch(rows, publication):
			reached["moved by a pod-derived field alone"]++
		default:
			reached["moved in a published field"]++
		}
		if len(rows) == 0 {
			reached["no rows"]++
		}
	}
	for _, outcome := range []string{"settled", "moved in a published field", "moved by a pod-derived field alone", "no rows"} {
		if reached[outcome] == 0 {
			t.Errorf("no generated case reached %q; the generator is too narrow", outcome)
		}
	}
	t.Logf("%d cases: %v", cases, reached)
}

// publishedFieldsMatch reports whether every row carries its entry's five
// published values, whatever its pod-derived fields hold.
func publishedFieldsMatch(rows []v1beta1.OMENativeInstanceStatus, publication []workloadtypes.InstanceStatus) bool {
	for i := range rows {
		row, published := &rows[i], &publication[i]
		if row.Phase != v1beta1convert.InstancePhaseFromWorkload(published.Phase) || row.PodCount != published.PodCount ||
			row.ServingPodCount != published.ServingPodCount || row.AvailablePodCount != published.AvailablePodCount || row.Admitted != published.Admitted {
			return false
		}
	}
	return true
}

// The row write and the in-place reading agree on the same rows after the
// write: once a publication is written over rows, the rows are settled
// against it, and a second write changes nothing.
func TestPublishRows_LeavesTheRowsSettledAgainstThePublication(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewPCG(22, 19))
	for n := 0; n < 20000; n++ {
		rows, publication := randomPublishedRows(r, now)
		publishRows(rows, publication)
		if !publishedRowsSettled(rows, publication) {
			t.Fatalf("case %d: rows just written are not settled against their publication\nrows %+v\npublication %+v", n, rows, publication)
		}
		again := writePublicationOverACopy(rows, publication)
		if !instanceStatusesEqual(rows, again) {
			t.Fatalf("case %d: a second write changes the rows: %s", n, fmt.Sprintf("%+v vs %+v", rows, again))
		}
	}
}
