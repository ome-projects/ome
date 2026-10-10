package inferencereplica

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// cordonFixture is an IR with three Ready single-pod Instances, Instance i
// on node-i, at the given desired replica count, with the named nodes
// cordoned.
func cordonFixture(t *testing.T, replicas int32, cordoned ...string) (*Reconciler, client.Client, *v1beta1.InferenceReplica) {
	t.Helper()
	ir := baselineIR("llama-engine", "prod", replicas)
	objects := []client.Object{ir}
	isCordoned := map[string]bool{}
	for _, name := range cordoned {
		isCordoned[name] = true
	}
	for idx, node := range []string{"node-0", "node-1", "node-2"} {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: int32(idx), Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1,
		})
		pod := podForIR(ir, int32(idx), "default", 0, true, true)
		pod.Spec.NodeName = node
		pod.Status.Phase = corev1.PodRunning
		objects = append(objects, pod,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node}, Spec: corev1.NodeSpec{Unschedulable: isCordoned[node]}})
	}
	r, c := newReconciler(t, objects...)
	return r, c, ir
}

// admittedVictims returns the indices the stored IR carries as
// Delete-owned.
func admittedVictims(t *testing.T, c client.Client, ir *v1beta1.InferenceReplica) []int32 {
	t.Helper()
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	var out []int32
	for _, row := range stored.Status.InstanceStatuses {
		if row.Phase == v1beta1.OMENativeInstanceDeleting && row.Operation != nil &&
			row.Operation.Type == v1beta1.InstanceOperationDelete {
			out = append(out, row.Index)
		}
	}
	return out
}

// A replica scaled from three Instances to two admits the Instance whose
// pod sits on a cordoned node, even the healthy lowest index, and admits
// the highest index when no node is cordoned.
func TestReconcile_ScaleDownAdmitsTheInstanceOnACordonedNodeFirst(t *testing.T) {
	cases := []struct {
		name     string
		cordoned []string
		want     int32
	}{
		{name: "no cordoned node", want: 2},
		{name: "Instance 0 on a cordoned node", cordoned: []string{"node-0"}, want: 0},
		{name: "Instance 1 on a cordoned node", cordoned: []string{"node-1"}, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c, ir := cordonFixture(t, 2, tc.cordoned...)
			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)})
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if !result.Requeue {
				t.Fatalf("the admission commit must requeue immediately, got %+v", result)
			}
			if got := admittedVictims(t, c, ir); len(got) != 1 || got[0] != tc.want {
				t.Fatalf("admitted victims = %v, want [%d]", got, tc.want)
			}
		})
	}
}

// The cordon is read only while the replica holds more Instances than it
// wants: a replica at its count lists no Nodes on any pass.
func TestReconcile_CordonedNodesReadOnlyWhileScalingDown(t *testing.T) {
	for _, tc := range []struct {
		name      string
		replicas  int32
		wantReads bool
	}{
		{name: "at its count", replicas: 3, wantReads: false},
		{name: "scaling down", replicas: 2, wantReads: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, base, ir := cordonFixture(t, tc.replicas, "node-0")
			nodeLists := 0
			r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.NodeList); ok {
						nodeLists++
					}
					return c.List(ctx, list, opts...)
				},
			})
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if got := nodeLists > 0; got != tc.wantReads {
				t.Fatalf("Node lists = %d, want reads=%t", nodeLists, tc.wantReads)
			}
		})
	}
}
