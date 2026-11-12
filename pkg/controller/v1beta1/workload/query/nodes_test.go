package query

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
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

// countingNodeReader counts the Node reads a reader serves.
type countingNodeReader struct {
	client.Reader
	gets int
	err  error
}

func (r *countingNodeReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Node); ok {
		r.gets++
		if r.err != nil {
			return r.err
		}
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestNodeLookup_AnswersFromTheCacheAndConfirmsAMissingNodeLive(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	cache := &countingNodeReader{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(testNode("node-a", false)).Build()}
	live := &countingNodeReader{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(testNode("node-a", false), testNode("node-b", false)).Build()}
	lookup := NewNodeLookup(workload.NodeReaders{Cache: cache, Live: live})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		exists, err := lookup.NodeExists(ctx, "node-a")
		if err != nil || !exists {
			t.Fatalf("node-a: got exists=%v err=%v, want the cache's yes", exists, err)
		}
	}
	if cache.gets != 1 || live.gets != 0 {
		t.Fatalf("a node the cache holds costs one cached read and no live read; got cache=%d live=%d", cache.gets, live.gets)
	}

	exists, err := lookup.NodeExists(ctx, "node-b")
	if err != nil || !exists {
		t.Fatalf("node-b: got exists=%v err=%v, want the live reader's yes for a node the cache has not received", exists, err)
	}
	if cache.gets != 2 || live.gets != 1 {
		t.Fatalf("a node the cache lacks is confirmed live once; got cache=%d live=%d", cache.gets, live.gets)
	}

	for i := 0; i < 2; i++ {
		exists, err = lookup.NodeExists(ctx, "node-c")
		if err != nil || exists {
			t.Fatalf("node-c: got exists=%v err=%v, want missing on both readers", exists, err)
		}
	}
	if cache.gets != 3 || live.gets != 2 {
		t.Fatalf("a missing node is read once on each reader and then remembered; got cache=%d live=%d", cache.gets, live.gets)
	}
}

func TestNodeLookup_WithoutALiveReaderTheCacheIsTheLastWord(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	cache := &countingNodeReader{Reader: fake.NewClientBuilder().WithScheme(scheme).Build()}
	exists, err := NewNodeLookup(workload.NodeReaders{Cache: cache}).NodeExists(context.Background(), "node-a")
	if err != nil || exists {
		t.Fatalf("got exists=%v err=%v, want missing", exists, err)
	}
}

func TestNodeLookup_ReturnsAReadErrorAndRemembersNothing(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	cache := &countingNodeReader{Reader: fake.NewClientBuilder().WithScheme(scheme).Build(), err: errors.New("apiserver unavailable")}
	lookup := NewNodeLookup(workload.NodeReaders{Cache: cache})
	if _, err := lookup.NodeExists(context.Background(), "node-a"); err == nil {
		t.Fatalf("a read error must reach the caller, so no pod is read as gone on it")
	}
	cache.err = nil
	if _, err := lookup.NodeExists(context.Background(), "node-a"); err != nil {
		t.Fatalf("the failed read is not remembered: %v", err)
	}
	if cache.gets != 2 {
		t.Fatalf("got %d cached reads, want 2 (the failed one and the retry)", cache.gets)
	}
}

// nodeSet answers the Node reading from a fixed set of names.
type nodeSet struct {
	held map[string]bool
	err  error
}

func (n nodeSet) NodeExists(_ context.Context, name string) (bool, error) {
	if n.err != nil {
		return false, n.err
	}
	return n.held[name], nil
}

func boundPod(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func sameNames(got []*corev1.Pod, want ...string) bool {
	names := podNames(got)
	if len(names) != len(want) {
		return false
	}
	for i := range names {
		if names[i] != want[i] {
			return false
		}
	}
	return true
}

func TestSplitPodsOnMissingNodes_OnlyAPodBoundToAMissingNodeIsGone(t *testing.T) {
	nodes := nodeSet{held: map[string]bool{"node-a": true}}
	onPresent := boundPod("c-on-present", "node-a")
	onMissing := boundPod("b-on-missing", "node-b")
	unbound := boundPod("d-unbound", "")
	terminal := boundPod("a-terminal", "node-b")
	terminal.Status.Phase = corev1.PodFailed
	held := boundPod("e-held", "node-b")
	held.Finalizers = []string{"example.com/hold"}
	another := boundPod("a-on-missing", "node-c")
	pods := []*corev1.Pod{onPresent, onMissing, unbound, terminal, held, another}

	present, gone, err := SplitPodsOnMissingNodes(context.Background(), nodes, pods)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if !sameNames(present, "c-on-present", "d-unbound", "a-terminal") {
		t.Fatalf("present = %v; want the pod on a held node, the unbound one and the terminal one, in input order", podNames(present))
	}
	if !sameNames(gone, "a-on-missing", "b-on-missing", "e-held") {
		t.Fatalf("gone = %v; want the pods bound to missing nodes, the finalizer-held one included, in name order", podNames(gone))
	}
}

func TestSplitPodsOnMissingNodes_NothingGoneReturnsTheInputSlice(t *testing.T) {
	pods := []*corev1.Pod{boundPod("a", "node-a"), boundPod("b", "")}
	present, gone, err := SplitPodsOnMissingNodes(context.Background(), nodeSet{held: map[string]bool{"node-a": true}}, pods)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(gone) != 0 || len(present) != 2 || &present[0] != &pods[0] {
		t.Fatalf("with nothing gone the input slice itself is returned; got present=%v gone=%v", podNames(present), podNames(gone))
	}
	present, gone, err = SplitPodsOnMissingNodes(context.Background(), nil, []*corev1.Pod{boundPod("a", "node-z")})
	if err != nil || len(gone) != 0 || len(present) != 1 {
		t.Fatalf("with no Node reading every pod is present; got present=%v gone=%v err=%v", podNames(present), podNames(gone), err)
	}
}

func TestSplitPodsOnMissingNodes_AReadErrorReadsNoPodAsGone(t *testing.T) {
	_, _, err := SplitPodsOnMissingNodes(context.Background(), nodeSet{err: errors.New("apiserver unavailable")}, []*corev1.Pod{boundPod("a", "node-a")})
	if err == nil {
		t.Fatalf("a Node read error must reach the caller instead of reading the pod as gone")
	}
}
