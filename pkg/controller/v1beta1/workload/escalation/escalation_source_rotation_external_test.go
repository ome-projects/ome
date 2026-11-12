package escalation_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// surgeDrainHoldKey is the serving-gate key the single-pod surge's drain
// step writes on the source of Instance 0 while its replacement stands at
// ordinal 1 (ops.surgeDrainKey). A test that puts the hold on with it and
// reads the gate True afterwards proves the ending released that key.
const surgeDrainHoldKey = "update-surge-drain-0-1"

// drainedSourcePod is a healthy source the surge's drain step took out of
// rotation: the container runs, ContainersReady is true, and the serving
// gate reads False under the surge-drain writer's key, so PodReady is
// false too. The pod is created in c so the release can re-read it.
func drainedSourcePod(t *testing.T, c client.Client, name string) *corev1.Pod {
	t.Helper()
	pod := servingPod(name)
	pod.Namespace = "ns"
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatalf("create the source pod: %v", err)
	}
	if err := podreadiness.MarkPodNotServing(context.Background(), c, c, pod, podreadiness.WriterUpdateSurgeDrain, surgeDrainHoldKey); err != nil {
		t.Fatalf("take the source out of rotation: %v", err)
	}
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err != nil {
		t.Fatalf("read the drained source: %v", err)
	}
	for i := range fresh.Status.Conditions {
		if fresh.Status.Conditions[i].Type == corev1.PodReady {
			fresh.Status.Conditions[i].Status = corev1.ConditionFalse
		}
	}
	return fresh
}

// surgeAttemptRow is a single-pod surge attempt of Instance 0 at step,
// opened a minute before now toward the target revision, with the deadline
// given.
func surgeAttemptRow(step string, now, deadline time.Time) workloadtypes.InstanceStatus {
	return workloadtypes.InstanceStatus{
		Index:           0,
		Phase:           workloadtypes.InstancePhaseUpdating,
		PodCount:        1,
		RunningRevision: "own-engine-runninghash",
		TargetRevision:  "own-engine-targethash",
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: "own-engine-targethash",
			StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
			Deadline:       metav1.NewTime(deadline),
		},
	}
}

// sourceRotationCase is one ending of a single-pod surge attempt: the step
// the attempt stands at, its deadline, the replacement at the surge slot,
// and whether a source pod stands and began the pass out of rotation.
type sourceRotationCase struct {
	name string
	step string
	// deadline is the attempt's deadline; a past one ends the attempt
	// because it elapsed, a future one leaves the ending to the grace.
	deadline time.Time
	// replacement builds the pod at the surge slot as of now.
	replacement func(now time.Time) *corev1.Pod
	// source says whether a source pod stands, and sourceDrained whether
	// it begins the pass out of rotation.
	source, sourceDrained bool
	// counted says whether the ending charges the target revision.
	counted bool
}

// sourcePod creates the case's source pod in c, or returns nil when the
// source is already gone.
func (tc sourceRotationCase) sourcePod(t *testing.T, c client.Client) *corev1.Pod {
	t.Helper()
	switch {
	case !tc.source:
		return nil
	case tc.sourceDrained:
		return drainedSourcePod(t, c, "engine-0-default-0")
	}
	source := servingPod("engine-0-default-0")
	source.Namespace = "ns"
	if err := c.Create(context.Background(), source); err != nil {
		t.Fatalf("create the source pod: %v", err)
	}
	return source
}

// run ends the attempt with one escalation pass, then checks the row, the
// retry record and, when a source stands, its serving gate.
func (tc sourceRotationCase) run(t *testing.T, now time.Time, retry *workloadtypes.RetryPolicy) {
	fc := clocktesting.NewFakeClock(now)
	c := fakeLedgerClient(t)
	insts := []workloadtypes.InstanceStatus{surgeAttemptRow(tc.step, now, tc.deadline)}
	input, rec := dispositionFixtureInput(fc, &insts, "own-engine-targethash", retry, ledgerOwnerCM())
	input.StuckPodGrace = time.Second

	pods := []*corev1.Pod{tc.replacement(now)}
	source := tc.sourcePod(t, c)
	if source != nil {
		pods = append([]*corev1.Pod{source}, pods...)
	}

	if err := runEscalationPass(t, workloadtypes.Deps{Client: c, Clock: fc}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: pods}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != workloadtypes.InstancePhaseFailed || insts[0].Operation != nil {
		t.Fatalf("row: got Phase=%q Operation=%+v, want Failed with no operation", insts[0].Phase, insts[0].Operation)
	}
	if counted := len(rec.blockCalls) == 1; counted != tc.counted {
		t.Fatalf("retry record writes: got %d, want counted=%v", len(rec.blockCalls), tc.counted)
	}
	if source != nil {
		requireSourceServing(t, c, source)
	}
}

// requireSourceServing fails the test unless the source reads routable
// again with no surge-drain hold left on it.
func requireSourceServing(t *testing.T, c client.Client, source *corev1.Pod) {
	t.Helper()
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(source), fresh); err != nil {
		t.Fatalf("read the source after the pass: %v", err)
	}
	if !podreadiness.IsServing(fresh) || podreadiness.HeldNotServingBy(fresh, podreadiness.WriterUpdateSurgeDrain) {
		t.Fatalf("the source is out of rotation after the ending: conditions=%+v", fresh.Status.Conditions)
	}
}

// TestEscalation_AnEndedSurgeHandsItsSourceBackBeforeTheRowIsWritten
// walks the endings of a single-pod surge whose source the drain step
// took out of rotation. Whatever ends the attempt past that step, the
// revision held, the attempt counted or the deadline alone, the source is
// routed again in the same pass and before the row reads Failed: a healthy
// old pod serves the Instance while the next attempt, or the operator,
// decides what comes next. An attempt still at the Surge step never took
// its source out, so a serving source is left as it is, and a source
// already gone is nothing to write.
func TestEscalation_AnEndedSurgeHandsItsSourceBackBeforeTheRowIsWritten(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	retry := &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	crashed := func(now time.Time) *corev1.Pod {
		return waitingPod("engine-0-default-1", "CrashLoopBackOff", "node-b", now.Add(-30*time.Second))
	}
	for _, tc := range []sourceRotationCase{
		{
			name: "a crashed replacement past the grace, the source drained",
			step: workloadtypes.UpdateStepSurgeDrain, deadline: now.Add(30 * time.Minute),
			replacement: crashed, source: true, sourceDrained: true, counted: true,
		},
		{
			name: "a replacement that cannot pull its image past the grace, the source drained",
			step: workloadtypes.UpdateStepSurgeDrain, deadline: now.Add(30 * time.Minute),
			replacement: func(now time.Time) *corev1.Pod {
				return waitingPod("engine-0-default-1", "ImagePullBackOff", "node-b", now.Add(-30*time.Second))
			},
			source: true, sourceDrained: true, counted: true,
		},
		{
			name: "the deadline elapsed on a replacement that never came back, the source drained",
			step: workloadtypes.UpdateStepSurgeDrain, deadline: now.Add(-time.Second),
			replacement: func(now time.Time) *corev1.Pod {
				return runningNotReadyOnNode("engine-0-default-1", "node-b", now.Add(-30*time.Second))
			},
			source: true, sourceDrained: true, counted: true,
		},
		{
			name: "a crashed replacement past the grace, the source already gone",
			step: workloadtypes.UpdateStepSurgeDrain, deadline: now.Add(30 * time.Minute),
			replacement: crashed, counted: true,
		},
		{
			name: "a crashed replacement past the grace at the Surge step, the source serving",
			step: workloadtypes.UpdateStepSurge, deadline: now.Add(30 * time.Minute),
			replacement: crashed, source: true, counted: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, now, retry) })
	}
}

// TestEscalation_AGangSourceKeepsItsDrainHoldUntilThePairEnds keeps the
// boundary: the escalation writes a gang surge's source Failed with its
// surge kept, and the pair's own end, not this pass, hands the source
// back. A gang source past the drain step whose replacement crashed past
// the grace leaves this pass still out of rotation.
func TestEscalation_AGangSourceKeepsItsDrainHoldUntilThePairEnds(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(now)
	c := fakeLedgerClient(t)
	surgeIdx := int32(1)
	source := surgeAttemptRow(workloadtypes.UpdateStepSurgeDrain, now, now.Add(30*time.Minute))
	source.Operation.SurgeIndex = &surgeIdx
	replacement := workloadtypes.InstanceStatus{
		Index: surgeIdx, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type: workloadtypes.InstanceOperationUpdate, Step: workloadtypes.UpdateStepGangSurgeTarget,
			TargetRevision: "own-engine-targethash", StartedAt: metav1.NewTime(now.Add(-time.Minute)),
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}
	insts := []workloadtypes.InstanceStatus{source, replacement}
	input, _ := dispositionFixtureInput(fc, &insts, "own-engine-targethash", nil, ledgerOwnerCM())
	input.StuckPodGrace = time.Second
	sourcePod := drainedSourcePod(t, c, "engine-0-default-0")
	crashed := waitingPod("engine-1-default-0", "CrashLoopBackOff", "node-b", now.Add(-30*time.Second))

	if err := runEscalationPass(t, workloadtypes.Deps{Client: c, Clock: fc}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {sourcePod}, surgeIdx: {crashed}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if insts[0].Phase != workloadtypes.InstancePhaseFailed || insts[0].Operation == nil || insts[0].Operation.SurgeIndex == nil {
		t.Fatalf("source row: got Phase=%q Operation=%+v, want Failed with the surge kept", insts[0].Phase, insts[0].Operation)
	}
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(sourcePod), fresh); err != nil {
		t.Fatalf("read the source after the pass: %v", err)
	}
	if podreadiness.IsServing(fresh) {
		t.Fatalf("the gang source was routed by the escalation; the pair's end owns that: conditions=%+v", fresh.Status.Conditions)
	}
}
