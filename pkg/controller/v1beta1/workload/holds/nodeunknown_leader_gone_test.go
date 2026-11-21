package holds

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// TestNodeUnknown_AShortenedWorkerIsReadOnItsKubeletsCount: the worker of a
// gang under rebuild stopped reporting and is being deleted. It shows 30 s
// of its 480 s, the grace a delete after its first one named. Its node is
// gone, the policy's slack is 10 s, and the pass comes 20 s past the
// deletion time the API server shows. With lifecycle.leaderGoneGracePeriod
// unset the force-delete frees the name on this pass, so the row has no
// wait to report. With the field at 30 s the worker's kubelet may still be
// counting until 40 s after that deletion time: the name is still held, and
// the row reports the wait.
func TestNodeUnknown_AShortenedWorkerIsReadOnItsKubeletsCount(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	policy := &types.ForceDeletePolicy{OverdueSlack: 10 * time.Second, NodeUnreachableThreshold: time.Minute}
	name := query.PodName("svc-a", types.ComponentEngine, 0, types.RunnerWorker, 0)
	deletionTime := metav1.NewTime(now.Add(-20 * time.Second))
	own, shown := int64(480), int64(30)
	worker := quietPod(name, "node-a")
	worker.Labels = map[string]string{query.LabelRunner: types.RunnerWorker}
	worker.Spec.TerminationGracePeriodSeconds = &own
	worker.DeletionTimestamp = &deletionTime
	worker.DeletionGracePeriodSeconds = &shown
	status := types.InstanceStatus{Index: 0}
	row := Row{
		Status:  &status,
		Desired: 3,
		Instance: &types.InstancePlan{Index: 0, Runners: []types.RunnerPlan{
			{Name: types.RunnerLeader, Size: 1},
			{Name: types.RunnerWorker, Size: 2},
		}},
	}
	for _, tc := range []struct {
		name        string
		field       time.Duration
		wantWaiting bool
	}{
		{name: "the field unset: the force-delete frees the name on this pass"},
		{name: "the field at 30 s: the kubelet may still be counting, so the name is held", field: 30 * time.Second, wantWaiting: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := PassInput{
				Deps: types.Deps{Client: fakeclient.New(t)},
				Input: types.ReconcileInput{
					Key:         types.Key{Namespace: "ns", OwnerName: "svc-a", Component: types.ComponentEngine},
					ForceDelete: policy,
					Clock:       clocktesting.NewFakeClock(now),
				},
				Plan: types.ComponentPlan{Component: types.ComponentEngine},
				Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
					return map[int32][]*corev1.Pod{0: {worker}}, nil
				},
			}
			if tc.field > 0 {
				in.Input.DesiredSpec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{Duration: tc.field}
			}
			got, err := reportNodeUnknown(context.Background(), in, row)
			if err != nil {
				t.Fatal(err)
			}
			if got.waiting != tc.wantWaiting || got.release == tc.wantWaiting {
				t.Fatalf("waiting = %v, release = %v, want waiting = %v and release = %v", got.waiting, got.release, tc.wantWaiting, !tc.wantWaiting)
			}
			if _, named := got.nodeLost[name]; !named {
				t.Fatalf("the pods on a node that stopped reporting are %v, want %s among them", got.nodeLost, name)
			}
		})
	}
}
