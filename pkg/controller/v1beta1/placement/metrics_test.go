package placement

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// resetPlacementMetrics clears every vector so whole-vector counts are
// deterministic. Required because other tests in this package reconcile ISVCs
// into these same globals, and CollectAndCount sizes the whole vector rather
// than one label set.
func resetPlacementMetrics() {
	placementPhase.Reset()
	placementWinner.Reset()
	placementCandidate.Reset()
	placementCandidateAdmitted.Reset()
	placementCandidateReady.Reset()
	placementPolicyInfo.Reset()
	placementSplitReplicas.Reset()
	placementSplitMaxPerCluster.Reset()
	placementSplitMinPerCluster.Reset()
}

func splitISVC(ns, name string, replicas int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{
				Mode:            v1beta1.PlacementModeSplit,
				Requirements:    "accelerator=tpu7x",
				ClusterSelector: "region=us-east",
				Split: &v1beta1.SplitSpec{
					Replicas:              &replicas,
					Spread:                true,
					MaxReplicasPerCluster: 4,
					MinReplicasPerCluster: 1,
				},
			},
		},
	}
}

// TestRecordPlacement_PublishesPhaseWinnerAndCandidates: one pass emits exactly
// one phase series, one winner series, and per-candidate state plus replica
// counts.
func TestRecordPlacement_PublishesPhaseWinnerAndCandidates(t *testing.T) {
	resetPlacementMetrics()

	isvc := splitISVC("ns", "svc", 8)
	res := placementResult{
		winner: "cluster-a",
		phase:  v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{
			{Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitted, AdmittedReplicas: 5, ReadyReplicas: 4},
			{Cluster: "cluster-b", Phase: v1beta1.CandidatePhaseAdmitting, AdmittedReplicas: 3, ReadyReplicas: 0},
		},
	}
	recordPlacement(isvc, res)

	if got := testutil.CollectAndCount(placementPhase); got != 1 {
		t.Fatalf("phase series = %d, want exactly 1", got)
	}
	if got := testutil.ToFloat64(placementPhase.WithLabelValues("ns", "svc", "Placed")); got != 1 {
		t.Errorf("phase{Placed} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(placementWinner.WithLabelValues("ns", "svc", "cluster-a")); got != 1 {
		t.Errorf("winner{cluster-a} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(placementCandidate); got != 2 {
		t.Errorf("candidate series = %d, want 2", got)
	}
	if got := testutil.ToFloat64(placementCandidateAdmitted.WithLabelValues("ns", "svc", "cluster-a")); got != 5 {
		t.Errorf("admitted{cluster-a} = %v, want 5", got)
	}
	if got := testutil.ToFloat64(placementCandidateReady.WithLabelValues("ns", "svc", "cluster-b")); got != 0 {
		t.Errorf("ready{cluster-b} = %v, want 0", got)
	}
}

// TestRecordPlacement_NoStaleSeriesOnTransition is the reason every vector is
// reset before it is written: an ISVC that moves Admitting -> Placed, changes
// winner, and drops a candidate must not keep reporting the old label values.
func TestRecordPlacement_NoStaleSeriesOnTransition(t *testing.T) {
	resetPlacementMetrics()

	isvc := splitISVC("ns", "svc", 8)
	recordPlacement(isvc, placementResult{
		winner: "cluster-a",
		phase:  v1beta1.PlacementPhaseAdmitting,
		candidates: []v1beta1.CandidatePlacement{
			{Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitting},
			{Cluster: "cluster-b", Phase: v1beta1.CandidatePhaseAdmitting},
		},
	})
	recordPlacement(isvc, placementResult{
		winner: "cluster-b",
		phase:  v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{
			{Cluster: "cluster-b", Phase: v1beta1.CandidatePhaseAdmitted},
		},
	})

	if got := testutil.CollectAndCount(placementPhase); got != 1 {
		t.Errorf("phase series = %d, want 1 (Admitting must not linger)", got)
	}
	if got := testutil.ToFloat64(placementPhase.WithLabelValues("ns", "svc", "Placed")); got != 1 {
		t.Errorf("phase{Placed} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(placementWinner); got != 1 {
		t.Errorf("winner series = %d, want 1 (cluster-a must not linger)", got)
	}
	if got := testutil.CollectAndCount(placementCandidate); got != 1 {
		t.Errorf("candidate series = %d, want 1 (swept loser must not linger)", got)
	}
}

// TestRecordPolicy_SplitFieldsAndModeChange: Split publishes the replica band;
// switching to a non-Split mode clears it rather than leaving a stale band.
func TestRecordPolicy_SplitFieldsAndModeChange(t *testing.T) {
	resetPlacementMetrics()

	isvc := splitISVC("ns", "svc", 8)
	recordPolicy(isvc)

	if got := testutil.ToFloat64(placementPolicyInfo.WithLabelValues(
		"ns", "svc", "Split", "accelerator=tpu7x", "region=us-east", "true")); got != 1 {
		t.Errorf("policy_info = %v, want 1", got)
	}
	if got := testutil.ToFloat64(placementSplitReplicas.WithLabelValues("ns", "svc")); got != 8 {
		t.Errorf("split_replicas = %v, want 8", got)
	}
	if got := testutil.ToFloat64(placementSplitMaxPerCluster.WithLabelValues("ns", "svc")); got != 4 {
		t.Errorf("max_per_cluster = %v, want 4", got)
	}

	isvc.Spec.Placement = &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSingle}
	recordPolicy(isvc)

	if got := testutil.CollectAndCount(placementSplitReplicas); got != 0 {
		t.Errorf("split_replicas series = %d, want 0 after switching to Single", got)
	}
	if got := testutil.CollectAndCount(placementPolicyInfo); got != 1 {
		t.Errorf("policy_info series = %d, want 1 (Split labels must not linger)", got)
	}
}

// TestRecordPolicy_NoPlacementEmitsNothing: an ISVC with no placement block is
// not a multi-cluster ISVC and must not appear in the policy metrics.
func TestRecordPolicy_NoPlacementEmitsNothing(t *testing.T) {
	resetPlacementMetrics()

	recordPolicy(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "plain"},
	})

	if got := testutil.CollectAndCount(placementPolicyInfo); got != 0 {
		t.Errorf("policy_info series = %d, want 0", got)
	}
}

// TestDeleteForISVC_DropsEverything: teardown must leave no series behind, or a
// deleted ISVC keeps reporting a phase and a winning cluster forever.
func TestDeleteForISVC_DropsEverything(t *testing.T) {
	resetPlacementMetrics()

	recordPlacement(splitISVC("ns", "svc", 8), placementResult{
		winner:     "cluster-a",
		phase:      v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitted}},
	})
	DeleteForISVC("ns", "svc")

	for name, n := range map[string]int{
		"phase":           testutil.CollectAndCount(placementPhase),
		"winner":          testutil.CollectAndCount(placementWinner),
		"candidate":       testutil.CollectAndCount(placementCandidate),
		"admitted":        testutil.CollectAndCount(placementCandidateAdmitted),
		"ready":           testutil.CollectAndCount(placementCandidateReady),
		"policy_info":     testutil.CollectAndCount(placementPolicyInfo),
		"split_replicas":  testutil.CollectAndCount(placementSplitReplicas),
		"max_per_cluster": testutil.CollectAndCount(placementSplitMaxPerCluster),
		"min_per_cluster": testutil.CollectAndCount(placementSplitMinPerCluster),
	} {
		if n != 0 {
			t.Errorf("%s series = %d, want 0 after DeleteForISVC", name, n)
		}
	}
}
