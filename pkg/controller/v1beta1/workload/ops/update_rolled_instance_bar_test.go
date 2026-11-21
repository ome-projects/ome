package ops

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// rolledPodPicture is one pod of an Instance on the target as the roll's
// budget reads it: the three conditions and the runner it is.
type rolledPodPicture struct {
	name                         string
	worker                       bool
	ready, containersReady, gate corev1.ConditionStatus
}

func (p rolledPodPicture) pod(target string, at time.Time) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: p.name, Labels: map[string]string{
			query.LabelRevisionHash: query.RevisionHashFromControllerRevisionName(target),
		}},
		Spec: corev1.PodSpec{ReadinessGates: []corev1.PodReadinessGate{{ConditionType: query.ServingConditionType}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: p.ready, LastTransitionTime: metav1.NewTime(at)},
			{Type: corev1.ContainersReady, Status: p.containersReady, LastTransitionTime: metav1.NewTime(at)},
			{Type: query.ServingConditionType, Status: p.gate, LastTransitionTime: metav1.NewTime(at)},
		}},
	}
	if p.worker {
		pod.Labels[query.LabelRunner] = workload.RunnerWorker
	}
	return pod
}

// TestRolledInstanceNotServing_TheBarAMovedSetIsReadAt holds the bar the
// roll's budget reads an Instance it already moved at, pod by pod. A pod
// serves with its containers ready and the serving gate on. A pod without
// the gate does not serve, whatever its Ready condition says: the kubelet
// folds a gate the engine just turned off into Ready a moment later. A pod
// whose Ready alone is False, with its containers ready and the gate on,
// sits on a node that stopped reporting and takes no traffic. The budget
// reads that pod's Instance as serving all the same, on a single pod and on
// a gang whose leader serves: the roll of
// TestAvailableMeansReady_ANodeUnderAMovedPodStaysAway goes on beside it,
// two pods down against one allowed.
func TestRolledInstanceNotServing_TheBarAMovedSetIsReadAt(t *testing.T) {
	const target = "svc-decoder-rev-b"
	yes, no := corev1.ConditionTrue, corev1.ConditionFalse
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	serving := rolledPodPicture{name: "leader", ready: yes, containersReady: yes, gate: yes}
	for _, tc := range []struct {
		name       string
		pods       []rolledPodPicture
		notServing bool
	}{
		{name: "a pod Ready with the gate serves", pods: []rolledPodPicture{serving}},
		{name: "a pod whose containers are not ready does not serve", pods: []rolledPodPicture{{name: "leader", ready: no, containersReady: no, gate: yes}}, notServing: true},
		{name: "a pod the engine holds out of rotation does not serve", pods: []rolledPodPicture{{name: "leader", ready: no, containersReady: yes, gate: no}}, notServing: true},
		{name: "a pod still Ready whose gate the engine turned off does not serve", pods: []rolledPodPicture{{name: "leader", ready: yes, containersReady: yes, gate: no}}, notServing: true},
		{name: "a pod whose Ready the node took away is read as serving", pods: []rolledPodPicture{{name: "leader", ready: no, containersReady: yes, gate: yes}}},
		{name: "a gang with every member Ready serves", pods: []rolledPodPicture{serving, {name: "worker", worker: true, ready: yes, containersReady: yes, gate: yes}}},
		{name: "a gang whose worker's gate the engine turned off does not serve", pods: []rolledPodPicture{serving, {name: "worker", worker: true, ready: yes, containersReady: yes, gate: no}}, notServing: true},
		{name: "a gang whose worker's Ready the node took away is read as serving", pods: []rolledPodPicture{serving, {name: "worker", worker: true, ready: no, containersReady: yes, gate: yes}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readySince := metav1.NewTime(now.Add(-time.Hour))
			row := &workload.InstanceStatus{Index: 0, Phase: workload.InstancePhaseReady, RunningRevision: target, ReadySince: &readySince}
			pods := make([]*corev1.Pod, 0, len(tc.pods))
			for _, p := range tc.pods {
				pods = append(pods, p.pod(target, now.Add(-time.Minute)))
			}
			got, _, why := RolledInstanceNotServing(row, target, now.Add(-2*time.Hour), pods, int32(len(pods)), 60, now)
			if got != tc.notServing {
				t.Fatalf("not serving = %t (%q), want %t", got, why, tc.notServing)
			}
		})
	}
}
