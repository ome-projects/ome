package status_test

import (
	"fmt"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
)

// TestUniqueNodesNamesEachNodeOnceInOrder: the set names each Node once,
// in name order whatever order the pods come in, leaves out a pod not yet
// scheduled, is nil when no pod is scheduled, has no room to grow, and
// leaves the pods as they were.
func TestUniqueNodesNamesEachNodeOnceInOrder(t *testing.T) {
	pictures := []struct {
		name  string
		nodes []string
		want  []string
	}{
		{name: "one pod", nodes: []string{"node-b"}, want: []string{"node-b"}},
		{name: "two pods on one Node", nodes: []string{"node-a", "node-a"}, want: []string{"node-a"}},
		{name: "a gang over three Nodes, out of name order, one twice", nodes: []string{"node-c", "node-a", "node-b", "node-c"}, want: []string{"node-a", "node-b", "node-c"}},
		{name: "a pod not yet scheduled beside one that is", nodes: []string{"", "node-a"}, want: []string{"node-a"}},
		{name: "no pod scheduled", nodes: []string{""}, want: nil},
		{name: "no pod", nodes: []string{}, want: nil},
		{name: "nil", nodes: nil, want: nil},
	}
	for _, picture := range pictures {
		t.Run(picture.name, func(t *testing.T) {
			var pods []*corev1.Pod
			for i, node := range picture.nodes {
				pods = append(pods, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", i)}, Spec: corev1.PodSpec{NodeName: node}})
			}
			before := slices.Clone(pods)
			got := status.UniqueNodes(pods)
			if !slices.Equal(got, picture.want) || (got == nil) != (picture.want == nil) {
				t.Errorf("UniqueNodes = %#v, want %#v", got, picture.want)
			}
			if cap(got) != len(got) {
				t.Errorf("UniqueNodes has room for %d more names", cap(got)-len(got))
			}
			if !slices.Equal(pods, before) {
				t.Errorf("UniqueNodes changed the pods it read")
			}
			for i, pod := range pods {
				if pod.Spec.NodeName != picture.nodes[i] {
					t.Errorf("pod %d now reads Node %q, want %q", i, pod.Spec.NodeName, picture.nodes[i])
				}
			}
		})
	}
}
