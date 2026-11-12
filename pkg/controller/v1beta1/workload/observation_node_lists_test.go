package workload_test

import (
	"fmt"
	"slices"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// publishOverPods runs one publication over rows and the pods bucketed by
// Instance, every pod in its Service's rotation, and returns the rows it
// publishes.
func publishOverPods(t *testing.T, rows []types.InstanceStatus, byInstance map[int32][]*corev1.Pod, desired map[int32]int32, target string) []types.InstanceStatus {
	t.Helper()
	available := map[string]struct{}{}
	for _, pods := range byInstance {
		for _, pod := range pods {
			available[pod.Name] = struct{}{}
		}
	}
	observation, err := workload.NewOwnedPublicationObservation(
		rows, workload.NewCachedSelectorPodObservation(nil, byInstance), available, status.AvailabilityWindow{})
	if err != nil {
		t.Fatalf("NewOwnedPublicationObservation: %v", err)
	}
	published, _, err := observation.TakeInlineV1Publication(desired, target)
	if err != nil {
		t.Fatalf("TakeInlineV1Publication: %v", err)
	}
	return published
}

// nodesUnder is the set of Nodes pods run on, in name order, nil when no
// pod is scheduled: what a published row's node list reads.
func nodesUnder(pods []*corev1.Pod) []string {
	seen := map[string]struct{}{}
	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			seen[pod.Spec.NodeName] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// checkNodeList holds one published row's node list to the set of Nodes
// its pods run on, in name order and with no room to grow, so an append
// to one row's list can never reach another row's; the per-Instance
// reader (status.UniqueNodes) reads the same list.
func checkNodeList(t *testing.T, label string, row types.InstanceStatus, pods []*corev1.Pod) {
	t.Helper()
	want := nodesUnder(pods)
	if !slices.Equal(row.NodesOccupied, want) || (row.NodesOccupied == nil) != (want == nil) {
		t.Errorf("%s: Instance %d's node list = %#v, want %#v", label, row.Index, row.NodesOccupied, want)
	}
	if cap(row.NodesOccupied) != len(row.NodesOccupied) {
		t.Errorf("%s: Instance %d's node list has room for %d more names", label, row.Index, cap(row.NodesOccupied)-len(row.NodesOccupied))
	}
	if got := status.UniqueNodes(pods); !slices.Equal(got, want) || (got == nil) != (want == nil) {
		t.Errorf("%s: UniqueNodes = %#v, want %#v, the list the publication wrote", label, got, want)
	}
}

// TestPublicationNodeListsNameEachNodeOnceInOrder: the node list a
// publication writes on a row names each Node its pods run on once, in
// name order, whatever order the pods come in; a pod not yet scheduled
// adds no name, and a row with no scheduled pod carries no list.
func TestPublicationNodeListsNameEachNodeOnceInOrder(t *testing.T) {
	readySince := metav1.Unix(1700000000, 0)
	pictures := []struct {
		name  string
		nodes []string
	}{
		{name: "one pod", nodes: []string{"node-b"}},
		{name: "a gang over two Nodes, one of them twice, out of name order", nodes: []string{"node-c", "node-a", "node-c"}},
		{name: "a gang with a pod not yet scheduled", nodes: []string{"node-a", ""}},
		{name: "no pod scheduled", nodes: []string{""}},
		{name: "no pod", nodes: nil},
	}
	for _, picture := range pictures {
		t.Run(picture.name, func(t *testing.T) {
			var pods []*corev1.Pod
			for i, node := range picture.nodes {
				pods = append(pods, observationPod(fmt.Sprintf("pod-%d", i), node, true, true, false))
			}
			byInstance := map[int32][]*corev1.Pod{}
			if len(pods) > 0 {
				byInstance[0] = pods
			}
			rows := []types.InstanceStatus{{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince}}
			published := publishOverPods(t, rows, byInstance, map[int32]int32{0: int32(len(pods))}, "rev-a")
			checkNodeList(t, picture.name, published[0], pods)
		})
	}
}

// TestPublicationNodeListsAtFleetShapes: at 1,000 and 4,000 single-pod
// Instances, 160 gangs of eight spread over eight Nodes and 160 gangs of
// eight on two Nodes, every published row's node list is the set of Nodes
// its pods run on, in name order and with no room to grow, every row reads
// Ready, and the publication leaves every bucket as it found it.
func TestPublicationNodeListsAtFleetShapes(t *testing.T) {
	shapes := []struct {
		name                   string
		instances, pods, nodes int
	}{
		{name: "1000x1", instances: 1000, pods: 1, nodes: 1},
		{name: "4000x1", instances: 4000, pods: 1, nodes: 1},
		{name: "160x8", instances: 160, pods: 8, nodes: 8},
		{name: "160x8-on-two-Nodes", instances: 160, pods: 8, nodes: 2},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			byInstance := fleetPods(shape.instances, shape.pods)
			before := make(map[int32][]*corev1.Pod, len(byInstance))
			desired := make(map[int32]int32, len(byInstance))
			for index, pods := range byInstance {
				for ordinal, pod := range pods {
					pod.Spec.NodeName = fmt.Sprintf("node-%d", ordinal%shape.nodes)
				}
				before[index] = slices.Clone(pods)
				desired[index] = int32(shape.pods)
			}
			stored := irstatustest.UniformRows(shape.instances)
			for i := range stored {
				stored[i].PodCount, stored[i].ServingPodCount, stored[i].AvailablePodCount = int32(shape.pods), int32(shape.pods), int32(shape.pods)
			}
			published := publishOverPods(t, v1beta1convert.InstanceStatusSliceToWorkload(stored), byInstance, desired, irstatustest.Revision)
			if len(published) != shape.instances {
				t.Fatalf("%s: %d rows published for %d Instances", shape.name, len(published), shape.instances)
			}
			for _, row := range published {
				checkNodeList(t, shape.name, row, byInstance[row.Index])
				if row.Phase != types.InstancePhaseReady {
					t.Errorf("%s: Instance %d reads %s, want Ready", shape.name, row.Index, row.Phase)
				}
			}
			for index, pods := range byInstance {
				if !slices.Equal(pods, before[index]) {
					t.Errorf("%s: the publication changed Instance %d's bucket", shape.name, index)
				}
			}
		})
	}
}

// revokedByNode is a pod that was serving until the node lifecycle
// controller revoked its Ready: its kubelet stopped reporting, so the
// container readiness and the serving gate it still carries are stale.
func revokedByNode(name, node string) *corev1.Pod {
	pod := observationPod(name, node, true, true, false)
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			pod.Status.Conditions[i].Status = corev1.ConditionFalse
			pod.Status.Conditions[i].Reason = podreadiness.NodeNotReadyReason
		}
	}
	return pod
}

// TestPublicationLeavesARevokedPodOutOfTheServingReading: a pod whose
// Ready the node lifecycle controller revoked is left out of the reading
// that decides whether a Ready row still serves, wherever it sits in the
// set; the pods that report decide, against the desired count. The
// revoked pod stays in the row's pod count and node list, and the bucket
// holds every pod in its order after the publication.
func TestPublicationLeavesARevokedPodOutOfTheServingReading(t *testing.T) {
	readySince := metav1.Unix(1700000000, 0)
	serving := func(name, node string) *corev1.Pod { return observationPod(name, node, true, true, false) }
	pictures := []struct {
		name        string
		pods        []*corev1.Pod
		desired     int32
		wantPhase   types.InstancePhase
		wantServing int32
	}{
		{name: "the revoked pod after the serving one, one pod wanted",
			pods: []*corev1.Pod{serving("pod-0", "node-a"), revokedByNode("pod-1", "node-b")}, desired: 1, wantPhase: types.InstancePhaseReady, wantServing: 1},
		{name: "the revoked pod before the serving one, one pod wanted",
			pods: []*corev1.Pod{revokedByNode("pod-0", "node-b"), serving("pod-1", "node-a")}, desired: 1, wantPhase: types.InstancePhaseReady, wantServing: 1},
		{name: "the revoked pod between two serving pods, two pods wanted",
			pods: []*corev1.Pod{serving("pod-0", "node-a"), revokedByNode("pod-1", "node-b"), serving("pod-2", "node-c")}, desired: 2, wantPhase: types.InstancePhaseReady, wantServing: 2},
		{name: "two pods wanted of a serving one and a revoked one",
			pods: []*corev1.Pod{serving("pod-0", "node-a"), revokedByNode("pod-1", "node-b")}, desired: 2, wantPhase: types.InstancePhaseRestarting, wantServing: 1},
		{name: "every pod revoked",
			pods: []*corev1.Pod{revokedByNode("pod-0", "node-b"), revokedByNode("pod-1", "node-c")}, desired: 1, wantPhase: types.InstancePhaseRestarting, wantServing: 0},
	}
	for _, picture := range pictures {
		t.Run(picture.name, func(t *testing.T) {
			byInstance := map[int32][]*corev1.Pod{0: picture.pods}
			before := slices.Clone(picture.pods)
			rows := []types.InstanceStatus{{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince}}
			row := publishOverPods(t, rows, byInstance, map[int32]int32{0: picture.desired}, "rev-a")[0]
			if row.Phase != picture.wantPhase || row.PodCount != int32(len(picture.pods)) || row.ServingPodCount != picture.wantServing {
				t.Errorf("row = phase %s, %d pod(s), %d serving; want %s, %d pod(s), %d serving",
					row.Phase, row.PodCount, row.ServingPodCount, picture.wantPhase, len(picture.pods), picture.wantServing)
			}
			checkNodeList(t, picture.name, row, picture.pods)
			if !slices.Equal(byInstance[0], before) {
				t.Errorf("the publication changed the bucket: %d pods, want %d in their order", len(byInstance[0]), len(before))
			}
		})
	}
}

// onRevision labels pod with the revision hash of rev, as the renderer
// labels the pods of an Instance running rev.
func onRevision(pod *corev1.Pod, rev string) *corev1.Pod {
	pod.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(rev).Hash()}
	return pod
}

// TestPublicationReadsAFailedRowReadyOverTheReportingPodsOfItsRevision: a
// Failed row that still runs revision A and records a spent attempt at B
// publishes Ready while the reporting pods on A meet the desired count; a
// pod on A whose node stopped reporting counts for nothing either way, a
// pod on B is not read, and the bucket holds every pod in its order after
// the publication.
func TestPublicationReadsAFailedRowReadyOverTheReportingPodsOfItsRevision(t *testing.T) {
	readySince := metav1.Unix(1700000000, 0)
	servingOnA := func(name string) *corev1.Pod {
		return onRevision(observationPod(name, "node-a", true, true, false), "rev-a")
	}
	revokedOnA := func(name string) *corev1.Pod { return onRevision(revokedByNode(name, "node-b"), "rev-a") }
	downOnB := func(name string) *corev1.Pod {
		return onRevision(observationPod(name, "node-c", false, false, false), "rev-b")
	}
	pictures := []struct {
		name      string
		pods      []*corev1.Pod
		desired   int32
		wantPhase types.InstancePhase
	}{
		{name: "a serving pod on A beside a revoked one and the failed replacement",
			pods: []*corev1.Pod{revokedOnA("pod-0"), servingOnA("pod-1"), downOnB("pod-2")}, desired: 1, wantPhase: types.InstancePhaseReady},
		{name: "only a revoked pod on A beside the failed replacement",
			pods: []*corev1.Pod{revokedOnA("pod-0"), downOnB("pod-1")}, desired: 1, wantPhase: types.InstancePhaseFailed},
		{name: "two pods wanted of a serving pod on A and a revoked one",
			pods: []*corev1.Pod{servingOnA("pod-0"), revokedOnA("pod-1")}, desired: 2, wantPhase: types.InstancePhaseFailed},
	}
	for _, picture := range pictures {
		t.Run(picture.name, func(t *testing.T) {
			byInstance := map[int32][]*corev1.Pod{0: picture.pods}
			before := slices.Clone(picture.pods)
			rows := []types.InstanceStatus{{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", TargetRevision: "rev-b", ReadySince: &readySince}}
			row := publishOverPods(t, rows, byInstance, map[int32]int32{0: picture.desired}, "rev-b")[0]
			if row.Phase != picture.wantPhase || row.PodCount != int32(len(picture.pods)) {
				t.Errorf("row = phase %s with %d pod(s); want %s with %d", row.Phase, row.PodCount, picture.wantPhase, len(picture.pods))
			}
			checkNodeList(t, picture.name, row, picture.pods)
			if !slices.Equal(byInstance[0], before) {
				t.Errorf("the publication changed the bucket: %d pods, want %d in their order", len(byInstance[0]), len(before))
			}
		})
	}
}
