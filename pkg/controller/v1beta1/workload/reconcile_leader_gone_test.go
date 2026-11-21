package workload

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// leaderGoneSidePod is one pod of Instance 0 of one side of the service
// svc. A deleted pod shows a grace of 480 s and a deletion time 480 s
// ahead of now.
func leaderGoneSidePod(component types.ComponentType, runner string, ordinal int, deleted bool, now time.Time) *corev1.Pod {
	name := "svc-" + string(component) + "-0-" + runner + "-" + strconv.Itoa(ordinal)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "prod",
			UID:       k8stypes.UID(name + "-uid"),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "svc",
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                "0",
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     runner,
			},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if deleted {
		at := metav1.NewTime(now.Add(480 * time.Second))
		grace := int64(480)
		pod.DeletionTimestamp = &at
		pod.DeletionGracePeriodSeconds = &grace
		pod.Finalizers = []string{"test.example.com/keeps-the-object"}
	}
	return pod
}

// TestShortenGraceAfterLeaderGone_ThePassOfOneSideLeavesTheOtherSideAlone:
// one service has two sides, each a gang at Instance 0, and both set
// lifecycle.leaderGoneGracePeriod to 30 s. On the engine side the leader's
// pod is gone and two workers are being deleted. On the decoder side the
// leader runs, and its two workers are being deleted from outside. The pass
// of the engine side shortens the engine's workers only. The pass of the
// decoder side shortens nobody, because the decoder's own leader still
// runs.
func TestShortenGraceAfterLeaderGone_ThePassOfOneSideLeavesTheOtherSideAlone(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
	objs := []client.Object{
		node,
		leaderGoneSidePod(types.ComponentEngine, types.RunnerWorker, 0, true, now),
		leaderGoneSidePod(types.ComponentEngine, types.RunnerWorker, 1, true, now),
		leaderGoneSidePod(types.ComponentDecoder, types.RunnerLeader, 0, false, now),
		leaderGoneSidePod(types.ComponentDecoder, types.RunnerWorker, 0, true, now),
		leaderGoneSidePod(types.ComponentDecoder, types.RunnerWorker, 1, true, now),
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	var deleted []string
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteOption) error {
			deleted = append(deleted, obj.GetName())
			return nil
		},
	}).Build()
	owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "svc-uid"}}
	// passOf runs the step as the pass of one side does, and returns the
	// names of the pods it sent a delete.
	passOf := func(component types.ComponentType) string {
		deleted = nil
		deps := types.Deps{Client: c, Recorder: record.NewFakeRecorder(8)}
		input := types.ReconcileInput{
			OwnerObject: owner,
			OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
			EventTarget: owner,
			Key:         types.Key{Namespace: "prod", OwnerName: "svc", Component: component},
			Clock:       clocktesting.NewFakeClock(now),
		}
		input.DesiredSpec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{Duration: 30 * time.Second}
		snapshot := NewObservedSnapshot(deps, input, component, nil)
		if err := shortenGraceAfterLeaderGone(context.Background(), deps, input, types.ComponentPlan{Component: component}, snapshot.CachedPods); err != nil {
			t.Fatalf("%s: %v", component, err)
		}
		sort.Strings(deleted)
		return strings.Join(deleted, " ")
	}
	if got, want := passOf(types.ComponentEngine), "svc-engine-0-worker-0 svc-engine-0-worker-1"; got != want {
		t.Errorf("the pass of the engine side sent deletes to %q, want %q", got, want)
	}
	if got := passOf(types.ComponentDecoder); got != "" {
		t.Errorf("the pass of the decoder side sent deletes to %q, want none: its own leader still runs", got)
	}
}
