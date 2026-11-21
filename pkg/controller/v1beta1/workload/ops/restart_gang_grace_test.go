package ops

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

var (
	gangGraceSinglePod = workload.InstancePlan{Runners: []workload.RunnerPlan{{Name: workload.RunnerDefault, Size: 1}}}
	gangGraceTwoPods   = workload.InstancePlan{Runners: []workload.RunnerPlan{{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 1}}}
)

// gangGracePod is a pod with the termination grace its spec carries; nil
// leaves the spec without one.
func gangGracePod(name string, grace *int64) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: k8stypes.UID(name + "-uid")},
		Spec:       corev1.PodSpec{TerminationGracePeriodSeconds: grace},
	}
}

// TestGangRestartDeleteOptions checks the grace an old pod of an Instance
// under repair is deleted with: the configured bound, rounded up to whole
// seconds, for a pod of a multi-pod Instance whose own grace is longer;
// the pod's own grace (no option) for a single-pod Instance, an
// unconfigured bound, or a pod whose own grace is no longer than the bound.
func TestGangRestartDeleteOptions(t *testing.T) {
	cases := []struct {
		name  string
		grace time.Duration
		inst  workload.InstancePlan
		pod   *corev1.Pod
		want  *int64
	}{
		{name: "unconfigured keeps the pod's own grace", grace: 0, inst: gangGraceTwoPods, pod: gangGracePod("p", ptrInt64(600)), want: nil},
		{name: "negative reads as unconfigured", grace: -time.Second, inst: gangGraceTwoPods, pod: gangGracePod("p", ptrInt64(600)), want: nil},
		{name: "a gang pod with a longer grace takes the bound", grace: 30 * time.Second, inst: gangGraceTwoPods, pod: gangGracePod("p", ptrInt64(600)), want: ptrInt64(30)},
		{name: "the bound rounds up to whole seconds", grace: 1500 * time.Millisecond, inst: gangGraceTwoPods, pod: gangGracePod("p", ptrInt64(600)), want: ptrInt64(2)},
		{name: "a gang pod whose own grace is shorter keeps it", grace: 30 * time.Second, inst: gangGraceTwoPods, pod: gangGracePod("p", ptrInt64(10)), want: nil},
		{name: "a gang pod whose own grace equals the bound keeps it", grace: 30 * time.Second, inst: gangGraceTwoPods, pod: gangGracePod("p", ptrInt64(30)), want: nil},
		{name: "a gang pod with no grace in its spec takes the bound", grace: 30 * time.Second, inst: gangGraceTwoPods, pod: gangGracePod("p", nil), want: ptrInt64(30)},
		{name: "a single-pod Instance keeps the pod's own grace", grace: 30 * time.Second, inst: gangGraceSinglePod, pod: gangGracePod("p", ptrInt64(600)), want: nil},
		{name: "a nil pod gets no option", grace: 30 * time.Second, inst: gangGraceTwoPods, pod: nil, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := workload.ReconcileInput{GangRestartGrace: tc.grace}
			got := recordedGrace(gangRestartDeleteOptions(input, tc.inst, tc.pod))
			switch {
			case tc.want == nil && got != nil:
				t.Fatalf("grace = %d, want the pod's own grace", *got)
			case tc.want != nil && (got == nil || *got != *tc.want):
				t.Fatalf("grace = %v, want %d", got, *tc.want)
			}
		})
	}
}

// TestDeleteOldSet_GangGrace drives the repair's delete step over the two
// old pods of a gang: each delete carries the pod's UID, and the bound
// replaces the pods' own 600 s grace only when it is configured.
func TestDeleteOldSet_GangGrace(t *testing.T) {
	for _, tc := range []struct {
		name  string
		grace time.Duration
		want  *int64
	}{
		{name: "configured bound", grace: 30 * time.Second, want: ptrInt64(30)},
		{name: "unconfigured", grace: 0, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := gangGracePod("svc-decoder-0-leader-0", ptrInt64(600))
			worker := gangGracePod("svc-decoder-0-worker-0", ptrInt64(600))
			var deletes []recordedDeleteOpts
			funcs := fdDeleteRecorder(&deletes)
			c := fdFakeClient(t, &funcs, leader.DeepCopy(), worker.DeepCopy())
			r := &instanceRepair{
				deps: workload.Deps{Client: c, Expectations: workload.NewExpectations()},
				input: workload.ReconcileInput{
					Key:              workload.Key{Namespace: "prod", Component: workload.ComponentDecoder, OwnerName: "svc"},
					GangRestartGrace: tc.grace,
				},
				inst:   gangGraceTwoPods,
				oldSet: []*corev1.Pod{leader, worker},
			}
			stop, err := r.deleteOldSet(context.Background())
			if err != nil {
				t.Fatalf("deleteOldSet: %v", err)
			}
			if !stop {
				t.Fatalf("deleteOldSet must end the pass after issuing the deletes")
			}
			if len(deletes) != 2 {
				t.Fatalf("deletes = %d, want one per old pod", len(deletes))
			}
			for _, d := range deletes {
				if d.uid == nil || *d.uid != k8stypes.UID(d.name+"-uid") {
					t.Errorf("%s: delete must carry the observed UID, got %v", d.name, d.uid)
				}
				switch {
				case tc.want == nil && d.grace != nil:
					t.Errorf("%s: grace = %d, want the pod's own grace", d.name, *d.grace)
				case tc.want != nil && (d.grace == nil || *d.grace != *tc.want):
					t.Errorf("%s: grace = %v, want %d", d.name, d.grace, *tc.want)
				}
			}
		})
	}
}
