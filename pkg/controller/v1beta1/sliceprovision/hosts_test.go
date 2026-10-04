package sliceprovision

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
			nodes := fake.NewClientBuilder().WithObjects(tt.nodes...).Build()
			got, found, err := unavailableHosts(context.Background(), nodes, keySlice, "s", tt.pods)
			if err != nil {
				t.Fatalf("unavailableHosts: %v", err)
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
	nodes := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return boom },
	}).Build()
	if _, _, err := unavailableHosts(context.Background(), nodes, keySlice, "s", []*corev1.PodSpec{podSpec("tpu-a", "2x2x1", 4)}); !errors.Is(err, boom) {
		t.Fatalf("unavailableHosts error = %v, want the node read's", err)
	}
}
