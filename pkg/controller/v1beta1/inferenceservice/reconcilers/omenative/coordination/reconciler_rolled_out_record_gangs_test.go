package coordination

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestRolledOutRecordReadsGangsByTheirServingLeader holds the rolled-out
// record on gangs of a leader and a worker. The record starts on version 1
// while no pod runs version 1, as a status written from outside the
// controller leaves it. A pass writes the record when one version alone has
// serving pods, and a gang serves through its leader, so a worker that is
// still Ready keeps no version in the count.
func TestRolledOutRecordReadsGangsByTheirServingLeader(t *testing.T) {
	leader, worker := v1beta1.RunnerNameLeader, v1beta1.RunnerNameWorker
	const writtenBack = "llama-engine-rev-v1"
	for _, row := range []struct {
		name string
		// pods are the gang pods the pass lists.
		pods []countedPod
		// target is the version the replica is headed for.
		target string
		// latest is the version the record names as rolled out after the pass.
		latest string
	}{
		{
			name:   "two gangs serve version 2, so the pass records version 2",
			pods:   []countedPod{{"v2", 0, leader, podServing}, {"v2", 0, worker, podServing}, {"v2", 1, leader, podServing}, {"v2", 1, worker, podServing}},
			target: "v2",
			latest: "llama-engine-rev-v2",
		},
		{
			name:   "a gang of version 2 and a gang of version 3 serve through their leaders, so the pass leaves the record alone",
			pods:   []countedPod{{"v2", 0, leader, podServing}, {"v2", 0, worker, podServing}, {"v3", 1, leader, podServing}, {"v3", 1, worker, podServing}},
			target: "v3",
			latest: writtenBack,
		},
		{
			name:   "the old gang's leader is out of rotation and its worker is still Ready, so the pass records version 3",
			pods:   []countedPod{{"v2", 0, leader, podHeldOut}, {"v2", 0, worker, podServing}, {"v3", 1, leader, podServing}, {"v3", 1, worker, podServing}},
			target: "v3",
			latest: "llama-engine-rev-v3",
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			isvc := testOMENativeISVC()
			isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {LatestRolledoutRevision: writtenBack},
			}
			replica := &v1beta1.InferenceReplica{
				ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
				Status:     v1beta1.InferenceReplicaStatus{UpdateRevision: isvc.Name + "-engine-" + row.target},
			}
			counts := servingPodsByRevision(servingCountPods(isvc, row.pods))
			err := updateTrafficStatus(context.Background(), testClient(replica), isvc,
				[]v1beta1.ComponentType{v1beta1.EngineComponent},
				map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: counts}, 1)
			if err != nil {
				t.Fatalf("the pass failed: %v", err)
			}
			if got := isvc.Status.Components[v1beta1.EngineComponent].LatestRolledoutRevision; got != row.latest {
				t.Errorf("the record names %q as rolled out, want %q (the pass counted %v)", got, row.latest, counts)
			}
		})
	}
}
