package sliceprovision

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// sliceNode is a ready node of slice.
func sliceNode(name, slice string, mutate ...func(*corev1.Node)) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{keySlice: slice}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
		}},
	}
	for _, m := range mutate {
		m(n)
	}
	return n
}

func cordoned(n *corev1.Node) { n.Spec.Unschedulable = true }

func readiness(status corev1.ConditionStatus) func(*corev1.Node) {
	return func(n *corev1.Node) { n.Status.Conditions[0].Status = status }
}

func noConditions(n *corev1.Node) { n.Status.Conditions = nil }

func tolerating(spec *corev1.PodSpec, tolerations ...corev1.Toleration) *corev1.PodSpec {
	spec.Tolerations = append(spec.Tolerations, tolerations...)
	return spec
}

func TestUnavailableHosts(t *testing.T) {
	tolerateCordon := corev1.Toleration{Key: corev1.TaintNodeUnschedulable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}
	tolerateNotReady := corev1.Toleration{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists}
	tolerateEverything := corev1.Toleration{Operator: corev1.TolerationOpExists}
	tolerateOtherEffect := corev1.Toleration{Key: corev1.TaintNodeUnschedulable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute}
	for _, tt := range []struct {
		name  string
		nodes []client.Object
		pods  []*corev1.PodSpec
		want  []string
	}{
		{
			name:  "ready",
			nodes: []client.Object{sliceNode("node-a", "s")},
			pods:  []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)},
		},
		{
			name:  "cordoned",
			nodes: []client.Object{sliceNode("node-a", "s", cordoned)},
			pods:  []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)},
			want:  []string{"node node-a is cordoned"},
		},
		{
			name:  "not ready",
			nodes: []client.Object{sliceNode("node-a", "s", readiness(corev1.ConditionFalse))},
			pods:  []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)},
			want:  []string{"node node-a is not ready"},
		},
		{
			name:  "unreachable",
			nodes: []client.Object{sliceNode("node-a", "s", readiness(corev1.ConditionUnknown))},
			pods:  []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)},
			want:  []string{"node node-a is unreachable"},
		},
		{
			name:  "no Ready condition yet",
			nodes: []client.Object{sliceNode("node-a", "s", noConditions)},
			pods:  []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)},
			want:  []string{"node node-a is not ready"},
		},
		{
			name:  "a pod tolerates the cordon",
			nodes: []client.Object{sliceNode("node-a", "s", cordoned)},
			pods:  []*corev1.PodSpec{podSpec("tpu-a", "2x2x2", 4), tolerating(podSpec("tpu-a", "2x2x2", 4), tolerateCordon)},
		},
		{
			name:  "a toleration of another effect",
			nodes: []client.Object{sliceNode("node-a", "s", cordoned)},
			pods:  []*corev1.PodSpec{tolerating(podSpec("tpu-a", "2x2x1", 4), tolerateOtherEffect)},
			want:  []string{"node node-a is cordoned"},
		},
		{
			name:  "a pod tolerates not ready",
			nodes: []client.Object{sliceNode("node-a", "s", readiness(corev1.ConditionFalse))},
			pods:  []*corev1.PodSpec{tolerating(podSpec("tpu-a", "2x2x1", 4), tolerateNotReady)},
		},
		{
			name:  "a pod tolerates every taint",
			nodes: []client.Object{sliceNode("node-a", "s", cordoned, readiness(corev1.ConditionUnknown))},
			pods:  []*corev1.PodSpec{tolerating(podSpec("tpu-a", "2x2x1", 4), tolerateEverything)},
		},
		{
			name:  "a cordon outranks readiness",
			nodes: []client.Object{sliceNode("node-a", "s", cordoned, readiness(corev1.ConditionFalse))},
			pods:  []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)},
			want:  []string{"node node-a is cordoned"},
		},
		{
			name: "only the slice's nodes, sorted",
			nodes: []client.Object{
				sliceNode("node-c", "s", readiness(corev1.ConditionFalse)),
				sliceNode("node-b", "s"),
				sliceNode("node-a", "s", cordoned),
				sliceNode("node-d", "other", cordoned),
			},
			pods: []*corev1.PodSpec{podSpec("tpu-a", "2x2x2", 4), podSpec("tpu-a", "2x2x2", 4)},
			want: []string{"node node-a is cordoned", "node node-c is not ready"},
		},
		{
			name: "no nodes labeled yet",
			pods: []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			nodes := fakeclient.New(t, fakeclient.WithObjects(tt.nodes...))
			v, err := unavailableHosts(context.Background(), nodes, nil, keySlice, "s", chipResource, tt.pods)
			if err != nil {
				t.Fatalf("unavailableHosts: %v", err)
			}
			got, found := v.unhealthy, v.found
			if len(v.short) != 0 {
				t.Fatalf("unavailableHosts short = %v, want none for pods that request only chips", v.short)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("unavailableHosts mismatch (-want +got):\n%s", diff)
			}
			want := 0
			for _, n := range tt.nodes {
				if n.GetLabels()[keySlice] == "s" {
					want++
				}
			}
			if found != want {
				t.Fatalf("unavailableHosts found %d hosts, want %d", found, want)
			}
		})
	}
}

func TestUnavailableHostsListError(t *testing.T) {
	boom := errors.New("boom")
	nodes := fakeclient.New(t, fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall, boom))
	if _, err := unavailableHosts(context.Background(), nodes, nil, keySlice, "s", chipResource, []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)}); !errors.Is(err, boom) {
		t.Fatalf("unavailableHosts error = %v, want the node read's", err)
	}
}

// allocatable gives a node the allocatable resources.
func allocatable(list corev1.ResourceList) func(*corev1.Node) {
	return func(n *corev1.Node) { n.Status.Allocatable = list }
}

// requesting has the pod's container request the resources.
func requesting(spec *corev1.PodSpec, list corev1.ResourceList) *corev1.PodSpec {
	spec.Containers[0].Resources.Requests = list
	return spec
}

// boundPod is a running pod bound to node, requesting the resources.
func boundPod(name, node string, requests corev1.ResourceList, mutate ...func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "other"},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "main", Resources: corev1.ResourceRequirements{Requests: requests}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, m := range mutate {
		m(p)
	}
	return p
}

func gib(n int64) resource.Quantity { return *resource.NewQuantity(n<<30, resource.BinarySI) }

// ownedBy makes kind the pod's controller.
func ownedBy(kind string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: "owner", UID: "owner-uid", Controller: ptr.To(true)}}
	}
}

func TestUnavailableHostsWithTooFewResourcesLeft(t *testing.T) {
	host := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100"), corev1.ResourceMemory: gib(200), chipResource: resource.MustParse("4")}
	cache := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: gib(40)}
	engine := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50"), corev1.ResourceMemory: gib(180)}
	for _, tt := range []struct {
		name  string
		nodes []client.Object
		bound []client.Object
		pods  []*corev1.PodSpec
		want  []string
	}{
		{
			name:  "enough left beside another pod",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache)},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), corev1.ResourceList{corev1.ResourceMemory: gib(150)})},
		},
		{
			name:  "too little memory left beside another pod",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache)},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), engine)},
			want:  []string{"node node-a has 160.0Gi of memory left unrequested, but a pod requests 180.0Gi"},
		},
		{
			name:  "only the short nodes, sorted",
			nodes: []client.Object{sliceNode("node-b", "s", allocatable(host)), sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-b", cache)},
			pods:  repeat(requesting(podSpec("tpu-a", "2x2x2", 4), engine), 2),
			want:  []string{"node node-b has 160.0Gi of memory left unrequested, but a pod requests 180.0Gi"},
		},
		{
			name:  "a finished pod frees what it requested",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache, func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded })},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), engine)},
		},
		{
			name:  "a pod leaving the same slice is left out",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("old-engine", "node-a", engine, func(p *corev1.Pod) { p.Spec.NodeSelector = map[string]string{keySlice: "s"} })},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), engine)},
		},
		{
			name:  "chips are not counted",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("other", "node-a", corev1.ResourceList{chipResource: resource.MustParse("4")})},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), corev1.ResourceList{chipResource: resource.MustParse("4"), corev1.ResourceMemory: gib(1)})},
		},
		{
			name:  "one pod that fits is enough",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache)},
			pods: []*corev1.PodSpec{
				requesting(podSpec("tpu-a", "2x2x1", 4), engine),
				requesting(podSpec("tpu-a", "2x2x1", 4), corev1.ResourceList{corev1.ResourceMemory: gib(10)}),
			},
		},
		{
			name:  "too little CPU left",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("80")})},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50")})},
			want:  []string{"node node-a has 20 of cpu left unrequested, but a pod requests 50"},
		},
		{
			name:  "a resource the node does not offer",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), corev1.ResourceList{"example.com/device": resource.MustParse("1")})},
			want:  []string{"node node-a has 0 of example.com/device left next to its DaemonSet and static pods, but a pod requests 1"},
		},
		{
			name:  "a limit with no request counts as the request",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache)},
			pods: []*corev1.PodSpec{func() *corev1.PodSpec {
				spec := podSpec("tpu-a", "2x2x1", 4)
				spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = gib(180)
				return spec
			}()},
			want: []string{"node node-a has 160.0Gi of memory left unrequested, but a pod requests 180.0Gi"},
		},
		{
			name:  "an init container's limit with no request counts too",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache)},
			pods: []*corev1.PodSpec{func() *corev1.PodSpec {
				spec := podSpec("tpu-a", "2x2x1", 4)
				spec.InitContainers = []corev1.Container{{Name: "init", Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{corev1.ResourceMemory: gib(180)},
				}}}
				return spec
			}()},
			want: []string{"node node-a has 160.0Gi of memory left unrequested, but a pod requests 180.0Gi"},
		},
		{
			name:  "a request below its limit is what counts",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache)},
			pods: []*corev1.PodSpec{func() *corev1.PodSpec {
				spec := requesting(podSpec("tpu-a", "2x2x1", 4), corev1.ResourceList{corev1.ResourceMemory: gib(150)})
				spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = gib(180)
				return spec
			}()},
		},
		{
			name:  "a DaemonSet pod leaves too little on every host",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache, ownedBy("DaemonSet"))},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), engine)},
			want:  []string{"node node-a has 160.0Gi of memory left next to its DaemonSet and static pods, but a pod requests 180.0Gi"},
		},
		{
			name:  "a static pod counts as node-wide",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache, func(p *corev1.Pod) {
				p.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "hash"}
			})},
			pods: []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), engine)},
			want: []string{"node node-a has 160.0Gi of memory left next to its DaemonSet and static pods, but a pod requests 180.0Gi"},
		},
		{
			name:  "a pod larger than the host",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), corev1.ResourceList{corev1.ResourceMemory: gib(300)})},
			want:  []string{"node node-a has 200.0Gi of memory left next to its DaemonSet and static pods, but a pod requests 300.0Gi"},
		},
		{
			name:  "a small DaemonSet pod beside a large cache pod is short, not too small",
			nodes: []client.Object{sliceNode("node-a", "s", allocatable(host))},
			bound: []client.Object{
				boundPod("agent", "node-a", corev1.ResourceList{corev1.ResourceMemory: gib(10)}, ownedBy("DaemonSet")),
				boundPod("cache", "node-a", cache),
			},
			pods: []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), engine)},
			want: []string{"node node-a has 150.0Gi of memory left unrequested, but a pod requests 180.0Gi"},
		},
		{
			name:  "an unhealthy node is reported for its health only",
			nodes: []client.Object{sliceNode("node-a", "s", cordoned, allocatable(host))},
			bound: []client.Object{boundPod("cache", "node-a", cache)},
			pods:  []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), engine)},
			want:  []string{"node node-a is cordoned"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			nodes := fakeclient.New(t, fakeclient.WithObjects(tt.nodes...))
			v, err := unavailableHosts(context.Background(), nodes, newFakeClient(tt.bound...), keySlice, "s", chipResource, tt.pods)
			if err != nil {
				t.Fatalf("unavailableHosts: %v", err)
			}
			got := v.reasons()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("unavailableHosts mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Unhealthy, too small and short nodes are told apart, so OME takes the
// right action and the warning names the right cause.
func TestUnavailableHostsSeparatesHealthFromShortfall(t *testing.T) {
	host := corev1.ResourceList{corev1.ResourceMemory: gib(200)}
	nodes := fakeclient.New(t, fakeclient.WithObjects(
		sliceNode("node-a", "s", cordoned, allocatable(host)),
		sliceNode("node-b", "s", allocatable(host)),
		sliceNode("node-c", "s", allocatable(host)),
	))
	bound := newFakeClient(
		boundPod("cache", "node-b", corev1.ResourceList{corev1.ResourceMemory: gib(40)}),
		boundPod("agent", "node-c", corev1.ResourceList{corev1.ResourceMemory: gib(40)}, ownedBy("DaemonSet")),
	)
	pods := repeat(requesting(podSpec("tpu-a", "2x2x2", 4), corev1.ResourceList{corev1.ResourceMemory: gib(180)}), 3)
	v, err := unavailableHosts(context.Background(), nodes, bound, keySlice, "s", chipResource, pods)
	if err != nil {
		t.Fatalf("unavailableHosts: %v", err)
	}
	want := hostReport{
		unhealthy: []string{"node node-a is cordoned"},
		tooSmall:  []string{"node node-c has 160.0Gi of memory left next to its DaemonSet and static pods, but a pod requests 180.0Gi"},
		short:     []string{"node node-b has 160.0Gi of memory left unrequested, but a pod requests 180.0Gi"},
		found:     3,
	}
	if diff := cmp.Diff(want, v, cmp.AllowUnexported(hostReport{})); diff != "" {
		t.Fatalf("unavailableHosts mismatch (-want +got):\n%s", diff)
	}
}

func TestUnavailableHostsBoundPodListError(t *testing.T) {
	boom := errors.New("boom")
	nodes := fakeclient.New(t, fakeclient.WithObjects(sliceNode("node-a", "s")))
	bound := fakeclient.New(t, fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall, boom))
	pods := []*corev1.PodSpec{requesting(podSpec("tpu-a", "2x2x1", 4), corev1.ResourceList{corev1.ResourceMemory: gib(1)})}
	if _, err := unavailableHosts(context.Background(), nodes, bound, keySlice, "s", chipResource, pods); !errors.Is(err, boom) {
		t.Fatalf("unavailableHosts error = %v, want the bound pod read's", err)
	}
}
