package workload_test

import (
	"fmt"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPublicationNodeListsStayDistinctAcrossRows: when every Instance runs on
// Nodes of its own, each published row names its own Nodes and no other
// row's, before and after another row's list is written through; the lists
// of neighbouring rows never share an element.
func TestPublicationNodeListsStayDistinctAcrossRows(t *testing.T) {
	shapes := []struct {
		name            string
		instances, pods int
	}{
		{name: "50x1", instances: 50, pods: 1},
		{name: "20x3-two-Nodes-each", instances: 20, pods: 3},
		{name: "8x8", instances: 8, pods: 8},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			rows, byInstance, desired := fleetOnNodesOfItsOwn(shape.instances, shape.pods)
			published := publishOverPods(t, rows, byInstance, desired, "rev-a")
			if len(published) != shape.instances {
				t.Fatalf("%d rows published for %d Instances", len(published), shape.instances)
			}
			checkEachRowNamesItsOwnNodes(t, published, byInstance)
			checkWritingThroughOneRowLeavesTheOthers(t, published)
		})
	}
}

// fleetOnNodesOfItsOwn builds instances Ready rows of pods each; Instance i's
// pods sit on node-i-b and node-i-a, out of name order and one of them twice.
func fleetOnNodesOfItsOwn(instances, pods int) ([]types.InstanceStatus, map[int32][]*corev1.Pod, map[int32]int32) {
	readySince := metav1.Unix(1700000000, 0)
	byInstance := map[int32][]*corev1.Pod{}
	desired := map[int32]int32{}
	var rows []types.InstanceStatus
	for i := 0; i < instances; i++ {
		for ordinal := 0; ordinal < pods; ordinal++ {
			node := fmt.Sprintf("node-%03d-%s", i, []string{"b", "a"}[ordinal%2])
			byInstance[int32(i)] = append(byInstance[int32(i)], observationPod(fmt.Sprintf("pod-%d-%d", i, ordinal), node, true, true, false))
		}
		desired[int32(i)] = int32(pods)
		rows = append(rows, types.InstanceStatus{Index: int32(i), Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince})
	}
	return rows, byInstance, desired
}

// checkEachRowNamesItsOwnNodes: every published row lists the Nodes under its
// own pods, each once in name order, with no room to grow.
func checkEachRowNamesItsOwnNodes(t *testing.T, published []types.InstanceStatus, byInstance map[int32][]*corev1.Pod) {
	t.Helper()
	for _, row := range published {
		want := nodesUnder(byInstance[row.Index])
		if !slices.Equal(row.NodesOccupied, want) {
			t.Errorf("Instance %d's node list = %v, want %v", row.Index, row.NodesOccupied, want)
		}
		if cap(row.NodesOccupied) != len(row.NodesOccupied) {
			t.Errorf("Instance %d's node list has room for %d more names", row.Index, cap(row.NodesOccupied)-len(row.NodesOccupied))
		}
	}
}

// checkWritingThroughOneRowLeavesTheOthers: writing through one row's list,
// and growing it, leaves every other row's list as published.
func checkWritingThroughOneRowLeavesTheOthers(t *testing.T, published []types.InstanceStatus) {
	t.Helper()
	before := make([][]string, len(published))
	for i := range published {
		before[i] = slices.Clone(published[i].NodesOccupied)
	}
	for i := range published {
		if len(published[i].NodesOccupied) > 0 {
			published[i].NodesOccupied[0] = "written-through"
			published[i].NodesOccupied = append(published[i].NodesOccupied, "grown")
		}
		for j := range published {
			if j == i {
				continue
			}
			if !slices.Equal(published[j].NodesOccupied, before[j]) {
				t.Fatalf("writing Instance %d's list changed Instance %d's: %v, want %v", published[i].Index, published[j].Index, published[j].NodesOccupied, before[j])
			}
		}
		published[i].NodesOccupied = before[i]
	}
}
