package inferencereplica

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// setLeaderGoneGrace sets spec.lifecycle.leaderGoneGracePeriod on a replica
// and keeps whatever else its lifecycle block holds.
func setLeaderGoneGrace(ir *v1beta1.InferenceReplica, grace time.Duration) {
	if ir.Spec.Lifecycle == nil {
		ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{}
	}
	ir.Spec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{Duration: grace}
}

// TestTeardown_LeaderGone_WorkersTakeTheReplicasGrace: a service whose one
// gang has a leader and three workers is deleted. The leader's pod is gone
// already, and the three workers are still terminating with the 480 s they
// were deleted with, so they hold the owner's finalizer.
//
// With lifecycle.leaderGoneGracePeriod at 30 s on the replica, the teardown
// pass sends each worker one more delete with 30 s under the pod's UID, and
// says so in one event. With the field unset it sends no such delete and no
// such event.
func TestTeardown_LeaderGone_WorkersTakeTheReplicasGrace(t *testing.T) {
	cases := []struct {
		name      string
		grace     time.Duration
		shortened bool
	}{
		{name: "the field at 30 s", grace: 30 * time.Second, shortened: true},
		{name: "the field unset"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := time.Now()
			ir, objs, workers := deletedGangWithoutItsLeader(base, tc.grace)
			r, stored := newReconciler(t, objs...)
			var shortened []string
			c := watchingShorterGraceDeletes(t, stored, &shortened)
			r.Client, r.APIReader = c, c
			r.Clock = clocktesting.NewFakeClock(base)
			withLifecycleConfig(r, `{}`)
			rec := record.NewFakeRecorder(32)
			r.Recorder = rec

			if _, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
			}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			said := eventsContaining(drainEvents(rec), string(workloadtypes.EventReasonLeaderGoneGraceShortened))
			if tc.shortened {
				checkWorkersTookTheGrace(t, shortened, said, workers)
				return
			}
			if len(shortened)+len(said) != 0 {
				t.Fatalf("with the field unset every worker keeps its 480 s: shorter-grace deletes %v, events %v", shortened, said)
			}
		})
	}
}

// deletedGangWithoutItsLeader is the replica of a deleted service with one
// gang of a leader and three workers, as a teardown pass finds it 5 s after
// the pods were deleted: the leader's pod is gone, and each worker is still
// terminating with its own 480 s. A grace above zero is set as the replica's
// lifecycle.leaderGoneGracePeriod. It returns the replica, the objects to
// seed and the workers' names.
//
// A finalizer keeps each worker in the fake client, as a kubelet that is
// still stopping containers keeps it on a cluster. The workers' node
// exists: a pod on a node whose Node object is gone is freed at once by
// another rule, before this one reads it.
func deletedGangWithoutItsLeader(base time.Time, grace time.Duration) (*v1beta1.InferenceReplica, []client.Object, []string) {
	ir := terminatingIR("llama-engine", "prod", 1, base.Add(-time.Minute))
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		TerminationGracePeriodSeconds: ptr.To(int64(480)),
		Containers:                    []corev1.Container{{Name: "ome-container", Image: "registry.example.com/runtime:v1"}},
	}}
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: template},
		{Name: v1beta1.RunnerNameWorker, Size: 3, Template: template},
	}
	if grace > 0 {
		setLeaderGoneGrace(ir, grace)
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceDeleting},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
		}},
	}
	deletionTime := metav1.NewTime(base.Add(475 * time.Second))
	objs := []client.Object{ir, node}
	var workers []string
	for ordinal := int32(0); ordinal < 3; ordinal++ {
		pod := podForIR(ir, 0, "worker", ordinal, false, false)
		pod.Spec.NodeName = node.Name
		pod.Spec.TerminationGracePeriodSeconds = ptr.To(int64(480))
		pod.DeletionTimestamp = &deletionTime
		pod.DeletionGracePeriodSeconds = ptr.To(int64(480))
		pod.Finalizers = []string{"test.ome.io/hold"}
		objs = append(objs, pod)
		workers = append(workers, pod.Name)
	}
	return ir, objs, workers
}

// watchingShorterGraceDeletes wraps the client so that every pod delete
// that names a grace is noted in shortened. Such a delete must name 30 s
// and carry the pod's UID.
func watchingShorterGraceDeletes(t *testing.T, stored client.Client, shortened *[]string) client.WithWatch {
	t.Helper()
	return interceptor.NewClient(stored.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			do := &client.DeleteOptions{}
			for _, o := range opts {
				o.ApplyToDelete(do)
			}
			if _, isPod := obj.(*corev1.Pod); isPod && do.GracePeriodSeconds != nil {
				if *do.GracePeriodSeconds != 30 {
					t.Errorf("pod %s is deleted with a grace of %d s, want 30 s", obj.GetName(), *do.GracePeriodSeconds)
				}
				if do.Preconditions == nil || do.Preconditions.UID == nil || *do.Preconditions.UID != obj.GetUID() {
					t.Errorf("the shorter-grace delete of pod %s must carry the pod's UID", obj.GetName())
				}
				*shortened = append(*shortened, obj.GetName())
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
}

// checkWorkersTookTheGrace reads that each worker got one shorter-grace
// delete, and that one event names the three, both graces and the field.
func checkWorkersTookTheGrace(t *testing.T, shortened, said, workers []string) {
	t.Helper()
	slices.Sort(shortened)
	if !slices.Equal(shortened, workers) {
		t.Fatalf("shorter-grace deletes went to %v, want one for each of %v", shortened, workers)
	}
	if len(said) != 1 {
		t.Fatalf("events = %v, want one that names the three workers", said)
	}
	for _, want := range append([]string{"from 480s to 30s", "lifecycle.leaderGoneGracePeriod"}, workers...) {
		if !strings.Contains(said[0], want) {
			t.Errorf("the event must say %q, got %q", want, said[0])
		}
	}
}
