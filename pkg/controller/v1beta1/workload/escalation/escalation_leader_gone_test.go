package escalation

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestRepairSetGone_AShortenedWorkerIsReadOnItsKubeletsCount: the one pod
// left of a repair's drained set is a worker that shows 30 s of its 480 s
// and stands 5 s past the deletion time the API server shows. With
// lifecycle.leaderGoneGracePeriod unset the worker reads as a pod no delete
// in flight will free, so the set is not gone. With the field at 30 s its
// kubelet may still be counting, so the set reads as on its way out.
func TestRepairSetGone_AShortenedWorkerIsReadOnItsKubeletsCount(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	deletionTime := metav1.NewTime(now.Add(-5 * time.Second))
	worker := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-engine-0-worker-0",
			Namespace: "prod",
			UID:       "svc-engine-0-worker-0-uid",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "svc",
				constants.OMEComponentLabel:           string(types.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                "0",
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     types.RunnerWorker,
			},
			DeletionTimestamp:          &deletionTime,
			DeletionGracePeriodSeconds: ptr.To(int64(30)),
			Finalizers:                 []string{"test.example.com/keeps-the-object"},
		},
		Spec: corev1.PodSpec{
			NodeName:                      "node-a",
			TerminationGracePeriodSeconds: ptr.To(int64(480)),
			Containers:                    []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	deps := types.Deps{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker).Build()}
	plan := types.ComponentPlan{Component: types.ComponentEngine}
	input := types.ReconcileInput{
		Key:   types.Key{Namespace: "prod", OwnerName: "svc", Component: types.ComponentEngine},
		Clock: clocktesting.NewFakeClock(now),
	}

	gone, err := repairSetGone(context.Background(), deps, input, plan, 0)
	if err != nil || gone {
		t.Fatalf("with the field unset: gone = %v, err = %v, want a set that is not gone", gone, err)
	}
	input.DesiredSpec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{Duration: 30 * time.Second}
	gone, err = repairSetGone(context.Background(), deps, input, plan, 0)
	if err != nil || !gone {
		t.Fatalf("with the field at 30 s: gone = %v, err = %v, want a set on its way out", gone, err)
	}
}
