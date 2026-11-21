package status_test

import (
	"math/rand"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// currentRevisionPicture is one draw of the rollup's inputs: the rows, the
// revision pair, the cleared-from revision, the retry blocks and the pods.
type currentRevisionPicture struct {
	rows            []types.InstanceStatus
	current, update string
	clearedFrom     string
	blocks          []types.RetryBlock
	byInstance      map[int32][]*corev1.Pod
}

// randomCurrentRevisionPicture draws zero to eight rows over indexes 0 to
// 11, duplicates allowed, in any phase, running and converging to any of
// three revisions or none, with no operation or an update, restart, create
// or migrate attempt at one of its steps; a revision pair and a cleared-from
// revision from the same three revisions or none; a retry block on the update
// revision now and then; and random pod buckets.
func randomCurrentRevisionPicture(r *rand.Rand) currentRevisionPicture {
	revisions := []string{"", "isvc-engine-aaaaaaaa", "isvc-engine-bbbbbbbb", "isvc-engine-cccccccc"}
	phases := []types.InstancePhase{types.InstancePhaseReady, types.InstancePhaseReady, types.InstancePhaseUpdating, types.InstancePhaseRestarting,
		types.InstancePhaseFailed, types.InstancePhasePending, types.InstancePhaseCreating, types.InstancePhaseMigrating}
	pick := func() string { return revisions[r.Intn(len(revisions))] }
	operation := func() *types.InstanceOperation {
		switch r.Intn(6) {
		case 0:
			return &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: pick()}
		case 1:
			return &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: pick()}
		case 2:
			return &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepParked, TargetRevision: pick()}
		case 3:
			return &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}
		case 4:
			return &types.InstanceOperation{Type: types.InstanceOperationCreate, Step: status.CreateStepCreatePods, TargetRevision: pick()}
		}
		return nil
	}
	rows := make([]types.InstanceStatus, r.Intn(9))
	for i := range rows {
		rows[i] = types.InstanceStatus{Index: int32(r.Intn(12)), Phase: phases[r.Intn(len(phases))], RunningRevision: pick(), TargetRevision: pick(), Operation: operation()}
	}
	pic := currentRevisionPicture{rows: rows, current: pick(), update: pick(), clearedFrom: pick(), byInstance: randomPodBuckets(r)}
	if r.Intn(4) == 0 {
		pic.blocks = []types.RetryBlock{{TargetRevision: pic.update, State: types.RetryBlockHeld, AttemptsStarted: 2}}
	}
	return pic
}

// TestCurrentRevisionOverPodsAgreesWithTheRevisionMapOnRandomPictures holds
// the rollup over the pods' buckets to the rollup over the revisions read
// from them beforehand, on random pictures: the same current revision from
// the same rows, pair, cleared-from revision, blocks and pods, whichever rule decides.
func TestCurrentRevisionOverPodsAgreesWithTheRevisionMapOnRandomPictures(t *testing.T) {
	r := rand.New(rand.NewSource(47))
	decidedByPods, promoted, cleared, kept := 0, 0, 0, 0
	for draw := 0; draw < 50000; draw++ {
		pic := randomCurrentRevisionPicture(r)
		got := status.CurrentRevisionOverPods(pic.rows, pic.current, pic.update, pic.blocks, pic.clearedFrom, pic.byInstance)
		want := status.CurrentRevisionFor(pic.rows, pic.current, pic.update, pic.blocks, pic.clearedFrom, status.PodRevisionsByInstance(pic.byInstance))
		if got != want {
			t.Fatalf("draw %d: CurrentRevisionOverPods = %q, CurrentRevisionFor over the revisions read first = %q (rows %+v, current %q, update %q, cleared from %q, blocks %v, pods %v)",
				draw, got, want, pic.rows, pic.current, pic.update, pic.clearedFrom, pic.blocks, status.PodRevisionsByInstance(pic.byInstance))
		}
		if status.CurrentRevisionFor(pic.rows, pic.current, pic.update, pic.blocks, pic.clearedFrom, nil) != got {
			decidedByPods++
		}
		switch {
		case got == pic.current:
			kept++
		case got == "":
			cleared++
		default:
			promoted++
		}
	}
	t.Logf("50000 draws: %d kept the recorded revision, %d cleared it, %d promoted or restored one; the pods decided %d", kept, cleared, promoted, decidedByPods)
}

// TestCurrentRevisionOverPodsReadsNoPodWhenTheRowsDecide: a Component whose
// rows read complete on the update revision is decided from the rows
// alone, with nothing read off its pods and nothing allocated, whatever
// label the pods carry; a row under repair hands the decision to the pods.
func TestCurrentRevisionOverPodsReadsNoPodWhenTheRowsDecide(t *testing.T) {
	const target, other = "isvc-engine-aaaaaaaa", "isvc-engine-bbbbbbbb"
	rows := make([]types.InstanceStatus, 8)
	byInstance := make(map[int32][]*corev1.Pod, len(rows))
	for i := range rows {
		rows[i] = types.InstanceStatus{Index: int32(i), Phase: types.InstancePhaseReady, RunningRevision: target}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Labels: map[string]string{query.LabelRevisionHash: query.RevisionFromName(other).Hash()}}}
		pod.Status.Phase = corev1.PodRunning
		byInstance[int32(i)] = []*corev1.Pod{pod}
	}
	var got string
	allocations := testing.AllocsPerRun(50, func() {
		got = status.CurrentRevisionOverPods(rows, target, target, nil, "", byInstance)
	})
	if got != target || allocations != 0 {
		t.Errorf("rows complete on the update revision: current %q with %g allocations, want %q with none: the rows decide and no pod is read", got, allocations, target)
	}
	rows[3].Phase = types.InstancePhaseRestarting
	rows[3].Operation = &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}
	if got := status.CurrentRevisionOverPods(rows, target, target, nil, "", byInstance); got != "" {
		t.Errorf("a row under repair whose pod carries another revision: current %q, want it cleared: the pods decide", got)
	}
}
