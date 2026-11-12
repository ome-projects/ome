package inferencereplica

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The topology key of a gang Component is part of its revision, so moving
// it rolls every gang: the old pods are drained and deleted, and the gang is
// built again on the new key. The PodGroup pass publishes the key, and the
// engine renders the rebuilt gang on the key it published. Both decide on
// the one pod list the pass takes from the cache.

const (
	gangKeyBefore = "topology.example.com/rack"
	gangKeyAfter  = "topology.example.com/row"
)

// readyGangOnKey is a Ready one-worker gang on the revision rev, built as
// the engine builds one: the pods carry the group label, the worker carries
// the generated term that names its leader on key, and the group records
// key.
func readyGangOnKey(ir *v1beta1.InferenceReplica, rev, key string) []client.Object {
	objs := seedReadyGang(ir, rev, 1)
	groupName := query.PodGroupName(ir.Spec.ParentRef.Name, workloadtypes.ComponentEngine, 0)
	for _, obj := range objs {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			continue
		}
		pod.Labels[query.LabelPodGroup] = groupName
		if pod.Labels[query.LabelRunner] != workloadtypes.RunnerWorker {
			continue
		}
		pod.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey: key,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
					constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
					query.LabelInstanceIdx:                "0",
					query.LabelRunner:                     workloadtypes.RunnerLeader,
				}},
			}},
		}}
	}
	group := ownedPodGroupForIR(ir, groupName, 0)
	group.Annotations = map[string]string{query.AnnotationTopologyKey: key}
	group.Spec.MinMember = 2
	return append(objs, group)
}

// gangGroupKey is the topology key the gang's PodGroup records.
func gangGroupKey(t *testing.T, c client.Client, ir *v1beta1.InferenceReplica) string {
	t.Helper()
	group := &schedulingv1alpha1.PodGroup{}
	name := query.PodGroupName(ir.Spec.ParentRef.Name, workloadtypes.ComponentEngine, 0)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ir.Namespace, Name: name}, group); err != nil {
		t.Fatalf("get the gang's PodGroup: %v", err)
	}
	return group.Annotations[query.AnnotationTopologyKey]
}

// workerTermKeys lists the topology key of every required pod-affinity term
// the stored worker pods carry, in store order.
func workerTermKeys(pods []corev1.Pod) []string {
	var keys []string
	for i := range pods {
		pod := &pods[i]
		if pod.Labels[query.LabelRunner] != workloadtypes.RunnerWorker || pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
			continue
		}
		for _, term := range pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			keys = append(keys, term.TopologyKey)
		}
	}
	return keys
}

// deleteOldGang runs the roll until the old gang is gone from the store
// and returns each pod as the engine deleted it: that is what a cache one
// step behind still lists.
func deleteOldGang(t *testing.T, r *Reconciler, base client.Client, ir *v1beta1.InferenceReplica) []corev1.Pod {
	t.Helper()
	var deleted []corev1.Pod
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if pod, ok := obj.(*corev1.Pod); ok {
				stored := &corev1.Pod{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(pod), stored); err == nil {
					deleted = append(deleted, *stored)
				}
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}
	for pass := 0; pass < 4 && len(listPods(t, base, ir.Namespace)) > 0; pass++ {
		r.Expectations = workloadtypes.NewExpectations() // the watch has confirmed the last pass's writes
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("drain pass %d: %v", pass, err)
		}
	}
	if left := listPods(t, base, ir.Namespace); len(deleted) != 2 || len(left) != 0 {
		t.Fatalf("premise: the roll must delete the old gang's two pods; deleted %d, %d still stored", len(deleted), len(left))
	}
	return deleted
}

// cacheListingOnce is a cache that lists pods on the first pod list and
// the store's pods after that; served counts the lists of pods.
func cacheListingOnce(base client.Client, pods []corev1.Pod, served *int) client.Client {
	return interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			podList, ok := list.(*corev1.PodList)
			if !ok || *served > 0 {
				return c.List(ctx, list, opts...)
			}
			*served++
			podList.Items = append([]corev1.Pod(nil), pods...)
			return nil
		},
	})
}

// requireGangOnKey fails unless the store holds a leader and a worker on
// revision rev, the worker's affinity term names key, and the group
// records key.
func requireGangOnKey(t *testing.T, c client.Client, ir *v1beta1.InferenceReplica, rev, key string) {
	t.Helper()
	pods := listPods(t, c, ir.Namespace)
	if len(pods) != 2 {
		t.Fatalf("the rebuilt gang must have a leader and a worker, got %d pod(s)", len(pods))
	}
	for i := range pods {
		if got := pods[i].Labels[query.LabelRevisionHash]; got != query.RevisionFromName(rev).Hash() {
			t.Fatalf("pod %s carries revision hash %q, want the new revision's %q", pods[i].Name, got, query.RevisionFromName(rev).Hash())
		}
	}
	if keys := workerTermKeys(pods); len(keys) != 1 || keys[0] != key {
		t.Fatalf("the rebuilt worker's affinity term must name %q, got %v", key, keys)
	}
	if got := gangGroupKey(t, c, ir); got != key {
		t.Fatalf("the group records %q after the rebuild, want %q", got, key)
	}
}

// TestReconcile_GangKeyChangeRebuildsOnTheNewKeyFromOnePodList: the
// operator moves the topology key of a one-worker gang. The roll deletes
// the old pods. On the pass that follows, the cache still lists the deleted
// pods when the pass takes its list and has caught up by the time anything
// lists again; the apiserver lists none. The pass must create nothing and
// its group must record the new key, so that the gang built on the next
// pass carries the new key. A pass whose PodGroup pass and engine each
// listed the cache would settle the group on the old pods and build the
// gang on the old key, with the new revision's hash on it.
func TestReconcile_GangKeyChangeRebuildsOnTheNewKeyFromOnePodList(t *testing.T) {
	ir := gangWidthIR("llama-engine", "prod", 1, v1beta1.UpdateStrategyRecreatePod)
	ir.Spec.TopologyKey = ptr.To(gangKeyBefore)
	before := targetRevisionNameFor(t, ir)
	objs := readyGangOnKey(ir, before, gangKeyBefore)
	ir.Spec.TopologyKey = ptr.To(gangKeyAfter)
	ir.Generation++
	after := targetRevisionNameFor(t, ir)
	if after == before {
		t.Fatalf("premise: a key change must cut a new revision; both keys resolve to %s", before)
	}
	r, base := newReconciler(t, append([]client.Object{ir}, objs...)...)
	r.GangSchedulingAvailable = true
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}
	ctx := context.Background()
	deleted := deleteOldGang(t, r, base, ir)

	staleLists := 0
	r.Client = cacheListingOnce(base, deleted, &staleLists)
	r.APIReader = base
	r.Expectations = workloadtypes.NewExpectations()
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("the pass behind the deletes: %v", err)
	}
	if staleLists != 1 {
		t.Fatalf("premise: the pass must take its pod list from the cache once; it took %d", staleLists)
	}
	if pods := listPods(t, base, ir.Namespace); len(pods) != 0 {
		t.Fatalf("the pass whose cache still listed the deleted gang created %d pod(s), worker terms on %v; it must create none", len(pods), workerTermKeys(pods))
	}
	if key := gangGroupKey(t, base, ir); key != gangKeyAfter {
		t.Fatalf("the group records %q once the gang is empty on the apiserver, want %q", key, gangKeyAfter)
	}

	// The cache has caught up: the gang is built on the new key.
	r.Client = base
	for pass := 0; pass < 4 && len(listPods(t, base, ir.Namespace)) < 2; pass++ {
		r.Expectations = workloadtypes.NewExpectations()
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("rebuild pass %d: %v", pass, err)
		}
	}
	requireGangOnKey(t, base, ir, after, gangKeyAfter)
}
