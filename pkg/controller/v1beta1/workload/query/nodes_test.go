package query

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testNode(name string, cordoned bool) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.NodeSpec{Unschedulable: cordoned}}
}

// TestCordonedNodes_IndexAndFallbackAgree: the indexed read and the
// index-less fallback name the same cordoned nodes.
func TestCordonedNodes_IndexAndFallbackAgree(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	nodes := []client.Object{testNode("node-a", true), testNode("node-b", false), testNode("node-c", true)}
	want := map[string]struct{}{"node-a": {}, "node-c": {}}

	indexed := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodes...).
		WithIndex(&corev1.Node{}, NodeUnschedulableField, NodeUnschedulableIndexExtractor).Build()
	plain := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodes...).Build()
	for name, reader := range map[string]client.Reader{"indexed": indexed, "index-less": plain} {
		got, err := CordonedNodes(context.Background(), reader)
		if err != nil {
			t.Fatalf("%s: CordonedNodes: %v", name, err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%s: cordoned nodes (-want +got):\n%s", name, diff)
		}
	}
}

func TestCordonedNodes_NoneCordonedIsNil(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testNode("node-a", false)).Build()
	got, err := CordonedNodes(context.Background(), c)
	if err != nil {
		t.Fatalf("CordonedNodes: %v", err)
	}
	if got != nil {
		t.Errorf("cordoned nodes = %v, want nil", got)
	}
}

// TestCordonedInstances_CountsOnlyPodsStillOnTheNode: an Instance counts
// when any one of its pods runs on a cordoned node. A pod that is
// terminating, finished or unbound holds nothing there.
func TestCordonedInstances_CountsOnlyPodsStillOnTheNode(t *testing.T) {
	on := func(name string, idx int32, node string) *corev1.Pod {
		pod := newDiscoverPod(name, idx, 1, false)
		pod.Spec.NodeName = node
		pod.Status.Phase = corev1.PodRunning
		return pod
	}
	terminating := on("terminating", 3, "node-a")
	terminating.DeletionTimestamp = &metav1.Time{}
	finished := on("finished", 4, "node-a")
	finished.Status.Phase = corev1.PodFailed
	unbound := on("unbound", 5, "")
	unbound.Status.Phase = corev1.PodPending

	byInstance := BucketPodsByInstanceIdx([]*corev1.Pod{
		on("single", 0, "node-a"),
		on("elsewhere", 1, "node-b"),
		on("gang-leader", 2, "node-b"), on("gang-worker", 2, "node-a"),
		terminating, finished, unbound,
	})
	cordoned := map[string]struct{}{"node-a": {}}

	want := map[int32]struct{}{0: {}, 2: {}}
	if diff := cmp.Diff(want, CordonedInstances(byInstance, cordoned)); diff != "" {
		t.Errorf("cordoned instances (-want +got):\n%s", diff)
	}
	if got := CordonedInstances(byInstance, nil); got != nil {
		t.Errorf("cordoned instances with no cordoned node = %v, want nil", got)
	}
}
