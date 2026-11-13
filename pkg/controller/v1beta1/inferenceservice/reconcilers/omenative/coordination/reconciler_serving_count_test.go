package coordination

import (
	"maps"
	"math/rand/v2"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// podState is the state one pod of the serving count's table is in.
type podState int

const (
	// podServing is Running, Ready and in rotation.
	podServing podState = iota
	// podNotReady is Running with PodReady False.
	podNotReady
	// podPending has not started running.
	podPending
	// podDeleting is Running and Ready with a deletion timestamp.
	podDeleting
	// podHeldOut is Running and Ready, held out of rotation by the serving gate.
	podHeldOut
	// podWithoutRevision serves and carries no revision hash.
	podWithoutRevision
)

// countedPod is one pod of the table: its revision, Instance index, runner
// and state.
type countedPod struct {
	hash   string
	index  int
	runner v1beta1.RunnerName
	state  podState
}

// servingCountPods builds the table's pods; pods of one Instance get
// distinct names.
func servingCountPods(isvc *v1beta1.InferenceService, specs []countedPod) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, len(specs))
	for serial, spec := range specs {
		pod := gangMemberPod(isvc, v1beta1.EngineComponent, spec.hash, spec.index, spec.runner, spec.state != podNotReady)
		pod.Name += "-" + strconv.Itoa(serial)
		switch spec.state {
		case podPending:
			pod.Status.Phase = corev1.PodPending
		case podDeleting:
			now := metav1.Now()
			pod.DeletionTimestamp = &now
		case podHeldOut:
			for i := range pod.Status.Conditions {
				if pod.Status.Conditions[i].Type == podreadiness.ConditionType {
					pod.Status.Conditions[i].Status = corev1.ConditionFalse
				}
			}
		case podWithoutRevision:
			delete(pod.Labels, query.LabelRevisionHash)
		case podServing, podNotReady:
		}
		pods = append(pods, pod)
	}
	return pods
}

// servingCountByTheRule is the count's rule written out: a pod with a
// revision that serves counts for that revision when a routed pod of its
// Instance under the same revision serves.
func servingCountByTheRule(pods []*corev1.Pod) map[string]int32 {
	out := map[string]int32{}
	for _, pod := range pods {
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == "" || !podReadyAndServing(pod) {
			continue
		}
		for _, routed := range pods {
			if query.RoutedRunner(routed) && podReadyAndServing(routed) &&
				routed.Labels[query.LabelRevisionHash] == hash &&
				routed.Labels[query.LabelInstanceIdx] == pod.Labels[query.LabelInstanceIdx] {
				out[hash]++
				break
			}
		}
	}
	return out
}

// servingCountRows is the table: what each picture of a Component counts
// per revision.
func servingCountRows() []struct {
	name string
	pods []countedPod
	want map[string]int32
} {
	single, leader, worker := v1beta1.RunnerNameDefault, v1beta1.RunnerNameLeader, v1beta1.RunnerNameWorker
	return []struct {
		name string
		pods []countedPod
		want map[string]int32
	}{
		{"a single pod that serves counts for its revision", []countedPod{{"a", 0, single, podServing}}, map[string]int32{"a": 1}},
		{"a single pod that is not Ready counts for nothing", []countedPod{{"a", 0, single, podNotReady}}, map[string]int32{}},
		{"a pod that has not started counts for nothing", []countedPod{{"a", 0, single, podPending}}, map[string]int32{}},
		{"a pod being deleted counts for nothing", []countedPod{{"a", 0, single, podDeleting}}, map[string]int32{}},
		{"a pod held out of rotation by the serving gate counts for nothing", []countedPod{{"a", 0, single, podHeldOut}}, map[string]int32{}},
		{"a pod without a revision counts for nothing", []countedPod{{"a", 0, single, podWithoutRevision}}, map[string]int32{}},
		{"a revision with no serving pod has no entry", []countedPod{{"a", 0, single, podServing}, {"b", 1, single, podNotReady}}, map[string]int32{"a": 1}},
		{"each revision counts its own serving pods", []countedPod{{"a", 0, single, podServing}, {"a", 1, single, podServing}, {"b", 2, single, podServing}, {"b", 3, single, podServing}, {"b", 4, single, podServing}, {"b", 5, single, podNotReady}}, map[string]int32{"a": 2, "b": 3}},
		{"a replacement serving beside its source counts for its own revision", []countedPod{{"a", 0, single, podServing}, {"b", 0, single, podServing}}, map[string]int32{"a": 1, "b": 1}},
		{"two routed pods of one Instance both count while both serve", []countedPod{{"a", 0, single, podServing}, {"a", 0, single, podServing}}, map[string]int32{"a": 2}},
		{"a gang whose leader serves counts every serving member", []countedPod{{"a", 0, leader, podServing}, {"a", 0, worker, podServing}, {"a", 0, worker, podServing}, {"a", 0, worker, podServing}}, map[string]int32{"a": 4}},
		{"a gang whose leader serves counts the workers that serve", []countedPod{{"a", 0, leader, podServing}, {"a", 0, worker, podServing}, {"a", 0, worker, podNotReady}}, map[string]int32{"a": 2}},
		{"a leader that serves counts without its workers", []countedPod{{"a", 0, leader, podServing}, {"a", 0, worker, podPending}, {"a", 0, worker, podPending}}, map[string]int32{"a": 1}},
		{"a gang whose leader is not Ready counts nothing, whatever its workers report", []countedPod{{"a", 0, leader, podNotReady}, {"a", 0, worker, podServing}, {"a", 0, worker, podServing}}, map[string]int32{}},
		{"a gang whose leader is held out of rotation counts nothing", []countedPod{{"a", 0, leader, podHeldOut}, {"a", 0, worker, podServing}}, map[string]int32{}},
		{"a gang whose leader is being deleted counts nothing", []countedPod{{"a", 0, leader, podDeleting}, {"a", 0, worker, podServing}}, map[string]int32{}},
		{"a worker whose leader pod is gone counts for nothing", []countedPod{{"a", 0, worker, podServing}}, map[string]int32{}},
		{"a worker counts with a leader of its own revision, never with another revision's", []countedPod{{"a", 0, worker, podServing}, {"b", 0, leader, podServing}}, map[string]int32{"b": 1}},
		{"a worker counts with a leader of its own Instance, never with a neighbour's", []countedPod{{"a", 0, worker, podServing}, {"a", 1, leader, podServing}}, map[string]int32{"a": 1}},
		{"two gangs under one index count with their own leaders", []countedPod{{"a", 0, leader, podServing}, {"a", 0, worker, podServing}, {"b", 0, leader, podServing}, {"b", 0, worker, podNotReady}}, map[string]int32{"a": 2, "b": 1}},
		{"a worker listed before its leader counts once the list is read", []countedPod{{"a", 0, worker, podServing}, {"a", 0, leader, podServing}}, map[string]int32{"a": 2}},
		{"a worker listed before a leader that is not Ready counts for nothing", []countedPod{{"a", 0, worker, podServing}, {"a", 0, leader, podNotReady}}, map[string]int32{}},
		{"workers of two gangs listed before their leaders count with their own", []countedPod{{"a", 0, worker, podServing}, {"b", 0, worker, podServing}, {"a", 0, leader, podServing}, {"b", 0, leader, podNotReady}}, map[string]int32{"a": 2}},
		{"gangs and single pods count side by side", []countedPod{{"a", 0, leader, podServing}, {"a", 0, worker, podServing}, {"a", 1, single, podServing}, {"a", 2, leader, podNotReady}, {"a", 2, worker, podServing}}, map[string]int32{"a": 3}},
		{"no pods count for nothing", nil, map[string]int32{}},
	}
}

// TestServingPodsByRevision_CountsAPodWhenItsInstancesRoutedPodServes holds
// the serving count's answer on every picture of the table, and the rule
// written out beside it.
func TestServingPodsByRevision_CountsAPodWhenItsInstancesRoutedPodServes(t *testing.T) {
	isvc := testOMENativeISVC()
	for _, row := range servingCountRows() {
		t.Run(row.name, func(t *testing.T) {
			pods := servingCountPods(isvc, row.pods)
			got := servingPodsByRevision(pods)
			if got == nil {
				t.Fatal("the serving count returned no map; callers read it by revision")
			}
			if !maps.Equal(got, row.want) {
				t.Errorf("the serving count reads %v, want %v", got, row.want)
			}
			if byTheRule := servingCountByTheRule(pods); !maps.Equal(byTheRule, row.want) {
				t.Errorf("the rule written out reads %v, want %v: the table and the rule disagree", byTheRule, row.want)
			}
		})
	}
}

// TestServingPodsByRevision_AgreesWithTheRuleOnRandomLists draws lists of
// pods over two revisions, three Instances, the three runners and every
// state, in any order, and holds the count to the rule written out.
func TestServingPodsByRevision_AgreesWithTheRuleOnRandomLists(t *testing.T) {
	isvc := testOMENativeISVC()
	hashes := []string{"a", "b"}
	runners := []v1beta1.RunnerName{v1beta1.RunnerNameDefault, v1beta1.RunnerNameLeader, v1beta1.RunnerNameWorker}
	states := []podState{podServing, podNotReady, podPending, podDeleting, podHeldOut, podWithoutRevision}
	r := rand.New(rand.NewPCG(44, 4000))
	for round := 0; round < 3000; round++ {
		specs := make([]countedPod, r.IntN(16))
		for i := range specs {
			specs[i] = countedPod{hash: hashes[r.IntN(len(hashes))], index: r.IntN(3), runner: runners[r.IntN(len(runners))], state: states[r.IntN(len(states))]}
		}
		pods := servingCountPods(isvc, specs)
		got, want := servingPodsByRevision(pods), servingCountByTheRule(pods)
		if !maps.Equal(got, want) {
			t.Fatalf("round %d: the serving count reads %v over %+v, the rule written out %v", round, got, specs, want)
		}
	}
}
