package status_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
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

// TestCurrentRevisionFor pins the rollup both ways: promoted once every
// Instance is Ready on the update revision and its retry ladder records
// no failure; withdrawn when current already names the update revision
// while an Instance still runs another; left as recorded on every other
// shape, a repair on the current revision included. A revision whose
// block still stands has not landed, whatever its rows read between
// crashes, so a push never lands by attrition.
func TestCurrentRevisionFor(t *testing.T) {
	const prior, next = "isvc-engine-aaaaaaaa", "isvc-engine-bbbbbbbb"
	row := func(phase types.InstancePhase, rev string) types.InstanceStatus {
		return types.InstanceStatus{Phase: phase, RunningRevision: rev}
	}
	block := func(state types.RetryBlockState) []types.RetryBlock {
		return []types.RetryBlock{{TargetRevision: next, State: state, AttemptsStarted: 1}}
	}
	cases := []struct {
		name            string
		rows            []types.InstanceStatus
		current, update string
		blocks          []types.RetryBlock
		want            string
	}{
		{"every Instance Ready on the update revision promotes it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next, nil, next},
		{"every Instance Ready on a Held revision does not land it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next, block(types.RetryBlockHeld), prior},
		{"every Instance Ready on a revision in backoff does not land it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next, block(types.RetryBlockBackoff), prior},
		{"every Instance Ready on a revision with an attempt in flight does not land it",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next, block(types.RetryBlockRetryInProgress), prior},
		{"a block on another revision does not withhold the landing",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, prior, next,
			[]types.RetryBlock{{TargetRevision: prior, State: types.RetryBlockHeld}}, next},
		{"a withdrawn current stays withdrawn while the update revision's block stands",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseReady, next)}, "", next, block(types.RetryBlockHeld), ""},
		{"a forward roll in flight keeps the prior revision",
			[]types.InstanceStatus{row(types.InstancePhaseReady, next), row(types.InstancePhaseUpdating, prior)}, prior, next, nil, prior},
		{"a rollback onto the current revision withdraws it while every Instance runs the superseded one",
			[]types.InstanceStatus{row(types.InstancePhaseUpdating, next), row(types.InstancePhaseReady, next)}, prior, prior, nil, ""},
		{"one Instance still on the superseded revision is enough to withdraw",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseReady, next)}, prior, prior, nil, ""},
		{"a repair on the current revision keeps it",
			[]types.InstanceStatus{row(types.InstancePhaseRestarting, prior), row(types.InstancePhaseReady, prior)}, prior, prior, nil, prior},
		{"an Instance that has not run anything yet decides nothing",
			[]types.InstanceStatus{row(types.InstancePhaseCreating, ""), row(types.InstancePhaseReady, prior)}, prior, prior, nil, prior},
		{"withdrawn stays withdrawn until every Instance is Ready on the update revision",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior), row(types.InstancePhaseUpdating, next)}, "", prior, nil, ""},
		{"no update revision leaves current alone",
			[]types.InstanceStatus{row(types.InstancePhaseReady, prior)}, prior, "", nil, prior},
		{"no Instances leave current alone", nil, prior, prior, nil, prior},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.CurrentRevisionFor(tc.rows, tc.current, tc.update, tc.blocks); got != tc.want {
				t.Errorf("CurrentRevisionFor(current=%q, update=%q) = %q, want %q", tc.current, tc.update, got, tc.want)
			}
		})
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

// A settled Ready row is demotable under every restart policy: the rule
// reads the row alone, because the running revision the demotion keeps is
// what each policy's rebuild path keys on. A row an operation owns, and a
// row in any other phase, is not the truth pass's to move.
func TestDemotableReady(t *testing.T) {
	tests := []struct {
		name string
		row  types.InstanceStatus
		want bool
	}{
		{name: "settled Ready row", row: types.InstanceStatus{Phase: types.InstancePhaseReady, RunningRevision: "rev-a"}, want: true},
		{name: "settled Ready row without a revision", row: types.InstanceStatus{Phase: types.InstancePhaseReady}, want: true},
		{name: "Ready row an operation owns", row: types.InstanceStatus{Phase: types.InstancePhaseReady,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge}}, want: false},
		{name: "already Pending", row: types.InstanceStatus{Phase: types.InstancePhasePending, RunningRevision: "rev-a"}, want: false},
		{name: "Restarting", row: types.InstanceStatus{Phase: types.InstancePhaseRestarting,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}}, want: false},
		{name: "Failed", row: types.InstanceStatus{Phase: types.InstancePhaseFailed}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := test.row
			if got := status.DemotableReady(&row); got != test.want {
				t.Errorf("DemotableReady(%+v) = %t, want %t", row, got, test.want)
			}
		})
	}
	if status.DemotableReady(nil) {
		t.Errorf("a nil row is the Empty slot, never a demotion candidate")
	}
}

// TestCountServingPods_RequiresThePodReadyCondition: a pod is serving only
// when the pod's Ready condition and the serving gate are both True.
// ContainersReady and the gate are the kubelet's and the controller's last
// writes; on a node whose kubelet stopped both keep their values while the
// node lifecycle controller sets Ready False, and such a pod is in no
// Service's endpoints. The same shape is a gate the kubelet has not folded
// into Ready yet, and that pod is not in the endpoints either.
func TestCountServingPods_RequiresThePodReadyCondition(t *testing.T) {
	condition := func(conditionType corev1.PodConditionType, value corev1.ConditionStatus) corev1.PodCondition {
		return corev1.PodCondition{Type: conditionType, Status: value}
	}
	pod := func(conditions ...corev1.PodCondition) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
			Status:     corev1.PodStatus{Conditions: conditions},
		}
	}
	containersReady := condition(corev1.ContainersReady, corev1.ConditionTrue)
	ready := condition(corev1.PodReady, corev1.ConditionTrue)
	readyRevoked := condition(corev1.PodReady, corev1.ConditionFalse)
	gateOn := condition(podreadiness.ConditionType, corev1.ConditionTrue)
	gateOff := condition(podreadiness.ConditionType, corev1.ConditionFalse)

	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want int32
	}{
		{name: "Ready with the gate on", pod: pod(containersReady, ready, gateOn), want: 1},
		{name: "Ready revoked by the control plane, containers and gate unchanged", pod: pod(containersReady, readyRevoked, gateOn), want: 0},
		{name: "gate off, Ready not yet folded", pod: pod(containersReady, ready, gateOff), want: 0},
		{name: "gate on, Ready never reported", pod: pod(containersReady, gateOn), want: 0},
		{name: "no conditions", pod: pod(), want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.CountServingPods([]*corev1.Pod{tc.pod}); got != tc.want {
				t.Fatalf("CountServingPods = %d, want %d", got, tc.want)
			}
		})
	}

	lost := pod(containersReady, readyRevoked, gateOn)
	counters := status.CountersForInstance([]*corev1.Pod{lost}, map[string]struct{}{}, status.AvailabilityWindow{})
	if counters.PodCount != 1 || counters.ServingPodCount != 0 {
		t.Fatalf("counters under a revoked Ready: pods=%d serving=%d, want pods=1 serving=0", counters.PodCount, counters.ServingPodCount)
	}
}

// A deleted pod keeps its Ready condition and its serving gate until its
// containers stop, but the EndpointSlice controller has already taken it out
// of every Service's ready endpoints, so it serves nothing new. The serving
// count follows the rotation: a gang that is losing a member stops counting
// as whole on the pass that sees the deletion, not on the one that sees the
// pod gone.
func TestCountServingPods_ExcludesAMemberLeavingTheGang(t *testing.T) {
	servingPod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue},
			}},
		}
	}
	leader := servingPod("engine-0-leader-0")
	worker := servingPod("engine-0-worker-0")
	gang := []*corev1.Pod{leader, worker}
	desired := map[int32]int32{0: 2}

	whole := status.CountersForInstance(gang, map[string]struct{}{leader.Name: {}, worker.Name: {}}, status.AvailabilityWindow{})
	if whole.PodCount != 2 || whole.ServingPodCount != 2 || whole.AvailablePodCount != 2 {
		t.Fatalf("whole gang: %s, want pods=2 serving=2 available=2", whole)
	}
	if got := status.CountServingInstances([]types.InstanceStatus{{Index: 0, PodCount: whole.PodCount, ServingPodCount: whole.ServingPodCount}}, desired); got != 1 {
		t.Fatalf("serving Instances with the whole gang = %d, want 1", got)
	}

	// The worker is deleted: still Ready, still gated on, already out of
	// the endpoints. Its conditions do not change until its containers stop.
	now := metav1.Now()
	worker.DeletionTimestamp = &now
	leaving := status.CountersForInstance(gang, map[string]struct{}{leader.Name: {}}, status.AvailabilityWindow{})
	if leaving.PodCount != 2 {
		t.Fatalf("PodCount = %d, want 2: the leaving member is still a pod of the Instance", leaving.PodCount)
	}
	if leaving.ServingPodCount != 1 {
		t.Fatalf("ServingPodCount = %d, want 1: a member leaving the gang is out of rotation", leaving.ServingPodCount)
	}
	if leaving.AvailablePodCount != 1 {
		t.Fatalf("AvailablePodCount = %d, want 1", leaving.AvailablePodCount)
	}
	if got := status.CountServingInstances([]types.InstanceStatus{{Index: 0, PodCount: leaving.PodCount, ServingPodCount: leaving.ServingPodCount}}, desired); got != 0 {
		t.Fatalf("serving Instances with a member leaving = %d, want 0", got)
	}
}
