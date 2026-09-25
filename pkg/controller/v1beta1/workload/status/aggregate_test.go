package status_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestUniqueNodes locks the deterministic ordering + nil-preservation
// contract NodesOccupied callers rely on (status equality checks key on
// the rendered field; non-determinism would churn LastTransitionTime).
func TestUniqueNodes(t *testing.T) {
	pods := []*corev1.Pod{
		{Spec: corev1.PodSpec{NodeName: "a"}},
		{Spec: corev1.PodSpec{NodeName: "b"}},
		{Spec: corev1.PodSpec{NodeName: "a"}},
		{Spec: corev1.PodSpec{}}, // unscheduled — skipped
	}
	got := status.UniqueNodes(pods)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("UniqueNodes: got %v want [a b]", got)
	}
	if got := status.UniqueNodes(nil); got != nil {
		t.Errorf("nil input should return nil; got %v", got)
	}
}

// TestInstanceMeetsThreshold pins the surge-tolerant classifier: the
// canonical pods always count when their satisfying count meets the
// desired threshold, even if PodCount briefly exceeds desired during a
// surge.
func TestInstanceMeetsThreshold(t *testing.T) {
	tests := []struct {
		name     string
		observed int32
		sat      int32
		desired  int32
		want     bool
	}{
		{name: "all observed satisfying (no plan info)", observed: 2, sat: 2, desired: 0, want: true},
		{name: "fewer observed satisfying than total (no plan info)", observed: 2, sat: 1, desired: 0, want: false},
		{name: "no pods observed", observed: 0, sat: 0, desired: 1, want: false},
		{name: "surge: satisfying < observed but >= desired", observed: 2, sat: 1, desired: 1, want: true},
		{name: "surge: satisfying matches desired exactly", observed: 3, sat: 2, desired: 2, want: true},
		{name: "surge: satisfying short of desired", observed: 3, sat: 1, desired: 2, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := status.InstanceMeetsThreshold(tt.observed, tt.sat, tt.desired); got != tt.want {
				t.Errorf("InstanceMeetsThreshold(observed=%d sat=%d desired=%d): got %v want %v",
					tt.observed, tt.sat, tt.desired, got, tt.want)
			}
		})
	}
}

// TestDesiredFor pins the lookup-with-fallback semantic: present in the
// map → use the value; absent (or nil map) → fall back to observed.
func TestDesiredFor(t *testing.T) {
	m := map[int32]int32{0: 2, 1: 3}
	if got := status.DesiredFor(m, 0, 7); got != 2 {
		t.Errorf("present: got %d want 2", got)
	}
	if got := status.DesiredFor(m, 99, 7); got != 7 {
		t.Errorf("absent: got %d want fallback 7", got)
	}
	if got := status.DesiredFor(nil, 0, 7); got != 7 {
		t.Errorf("nil map: got %d want fallback 7", got)
	}
}

// TestRolloutComplete pins the CurrentRevision promotion gate: every
// Instance must be Phase=Ready AND on the target revision; any deviation
// holds CurrentRevision in place.
func TestRolloutComplete(t *testing.T) {
	allReady := []types.InstanceStatus{
		{Phase: types.InstancePhaseReady, RunningRevision: "isvc-engine-aaaaaaaa"},
		{Phase: types.InstancePhaseReady, RunningRevision: "isvc-engine-aaaaaaaa"},
	}
	if !status.RolloutComplete(allReady, "isvc-engine-aaaaaaaa") {
		t.Errorf("all Ready on rev should be complete")
	}
	oneCreating := []types.InstanceStatus{
		{Phase: types.InstancePhaseCreating, RunningRevision: "isvc-engine-aaaaaaaa"},
		{Phase: types.InstancePhaseReady, RunningRevision: "isvc-engine-aaaaaaaa"},
	}
	if status.RolloutComplete(oneCreating, "isvc-engine-aaaaaaaa") {
		t.Errorf("Phase=Creating should keep rollout incomplete")
	}
	stale := []types.InstanceStatus{
		{Phase: types.InstancePhaseReady, RunningRevision: "isvc-engine-bbbbbbbb"},
	}
	if status.RolloutComplete(stale, "isvc-engine-aaaaaaaa") {
		t.Errorf("stale RunningRevision should keep rollout incomplete")
	}
	if status.RolloutComplete(nil, "isvc-engine-aaaaaaaa") {
		t.Errorf("empty instance list should not be complete")
	}
}

func TestReachedDesiredShape(t *testing.T) {
	ready := func(rev string) types.InstanceStatus {
		return types.InstanceStatus{Phase: types.InstancePhaseReady, RunningRevision: rev}
	}
	newRev, oldRev := "isvc-engine-newaaaaa", "isvc-engine-oldbbbbb"
	// 8 instances, partition=2 → want 6 on new + 2 on old, all Ready.
	staged := []types.InstanceStatus{ready(newRev), ready(newRev), ready(newRev), ready(newRev),
		ready(newRev), ready(newRev), ready(oldRev), ready(oldRev)}
	if !status.ReachedDesiredShape(staged, newRev, 2, 8) {
		t.Fatal("6 new + 2 held, all Ready → reached")
	}
	if status.ReachedDesiredShape(staged, newRev, 0, 8) { // partition 0 wants all 8 new
		t.Fatal("partition 0 must require all on new")
	}
	degraded := append([]types.InstanceStatus{}, staged...)
	degraded[7].Phase = types.InstancePhaseUpdating
	if status.ReachedDesiredShape(degraded, newRev, 2, 8) {
		t.Fatal("held instance not Ready → not reached")
	}
	allNew := []types.InstanceStatus{ready(newRev), ready(newRev)}
	if status.RolloutComplete(allNew, newRev) != status.ReachedDesiredShape(allNew, newRev, 0, 2) {
		t.Fatal("RolloutComplete must equal ReachedDesiredShape(...,0,len)")
	}
}

// TestCountAvailablePods_MinReadySecondsWindow pins the Available counter's
// two inputs — EndpointSlice rotation membership always, and the Ready-age
// window only when the Component sets minReadySeconds — and the wake it
// reports: the remaining window of the earliest in-rotation pod still inside
// it, nothing once every such pod is Available or when no window applies.
func TestCountAvailablePods_MinReadySecondsWindow(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	readyPod := func(name string, readyAt time.Time) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(readyAt),
			}}},
		}
	}
	aged := readyPod("aged", now.Add(-30*time.Second))
	fresh := readyPod("fresh", now.Add(-5*time.Second))
	notInRotation := readyPod("out", now.Add(-30*time.Second))
	pods := []*corev1.Pod{aged, fresh, notInRotation}
	inRotation := map[string]struct{}{aged.Name: {}, fresh.Name: {}}

	if got, wake := status.CountAvailablePods(pods, inRotation, status.AvailabilityWindow{Now: now}); got != 2 || wake != 0 {
		t.Fatalf("zero window: got %d/%s want 2/0s (rotation membership alone, nothing pending)", got, wake)
	}
	window := status.AvailabilityWindow{MinReadySeconds: 20, Now: now}
	if got, wake := status.CountAvailablePods(pods, inRotation, window); got != 1 || wake != 15*time.Second {
		t.Fatalf("20s window: got %d/%s want 1/15s (only the aged pod; the fresh pod crosses in 15s)", got, wake)
	}
	if got, wake := status.CountAvailablePods([]*corev1.Pod{fresh, notInRotation}, map[string]struct{}{notInRotation.Name: {}}, window); got != 1 || wake != 0 {
		t.Fatalf("out-of-rotation pod inside the window: got %d/%s want 1/0s (no wake for a pod rotation will not admit by time alone)", got, wake)
	}
	window.Now = now.Add(15 * time.Second)
	if got, wake := status.CountAvailablePods(pods, inRotation, window); got != 2 || wake != 0 {
		t.Fatalf("20s window after it elapsed: got %d/%s want 2/0s", got, wake)
	}
	counters := status.CountersForInstance(pods, inRotation, status.AvailabilityWindow{MinReadySeconds: 20, Now: now})
	if counters.AvailablePodCount != 1 || counters.ReadyPodCount != 0 || counters.NextAvailableIn != 15*time.Second {
		t.Fatalf("counters: got available=%d ready=%d nextAvailableIn=%s want available=1 ready=0 nextAvailableIn=15s (Ready counts ContainersReady)",
			counters.AvailablePodCount, counters.ReadyPodCount, counters.NextAvailableIn)
	}
}
