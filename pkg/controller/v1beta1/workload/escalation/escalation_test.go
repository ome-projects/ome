package escalation

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestEvidenceFor_Deadline: the pass's evidence reports DeadlinePassed
// for a transient-phase instance whose Operation.Deadline is in the past,
// and not otherwise. Evidence only — no writes. (No stuck pod: no pods
// are observed, so StuckPod stays nil.)
func TestEvidenceFor_Deadline(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	insts := []types.InstanceStatus{
		{ // deadline in the past → DeadlinePassed
			Index: 0, Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{Deadline: metav1.NewTime(now.Add(-time.Minute))},
		},
		{ // deadline in the future → not passed
			Index: 1, Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{Deadline: metav1.NewTime(now.Add(time.Minute))},
		},
	}

	if ev := evidenceFor(insts, nil, 0, now, 30*time.Second, ""); !ev.DeadlinePassed || ev.StuckPod != nil {
		t.Errorf("instance 0: got DeadlinePassed=%v StuckPod=%v, want true/nil", ev.DeadlinePassed, ev.StuckPod)
	}
	if ev := evidenceFor(insts, nil, 1, now, 30*time.Second, ""); ev.DeadlinePassed {
		t.Errorf("instance 1: future deadline must not be passed")
	}
}
