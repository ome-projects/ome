package workload

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// RolledSetReadingForTest reads every planned Instance for the roll's open
// work alone (updateSelectionPass.recordRolledSet), as the update selection
// does before it reads an Instance's trigger, and returns what it recorded:
// the Instances on the target that do not serve it, the crashed ones among
// them, and the crashed ones out of rotation.
func RolledSetReadingForTest(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) (UpdateSelection, error) {
	p, err := readUpdateSelection(ctx, input, plan, target, snapshot)
	if err != nil {
		return UpdateSelection{}, err
	}
	for _, inst := range p.plan.Instances {
		p.recordRolledSet(p.candidate(inst))
	}
	return p.selection, nil
}

// rolledSetPicture is one Instance on the target revision as the rolled-set
// reading sees it: the layout the plan renders, the width the target
// records, the row and its pods, the grace a pod may be unready for.
type rolledSetPicture struct {
	name string
	// workers is the worker count the Component's spec asks for; 0 is a
	// single-pod Component.
	workers int32
	// targetWidth is the worker count recorded for the target revision; -1
	// records none, so the target renders the spec's layout.
	targetWidth int32
	row         types.InstanceStatus
	pods        []*corev1.Pod
	grace       time.Duration
	// entryWorkers, when above 0, is the worker count the plan's own entry
	// for the Instance renders, as an entry rendered at another revision's
	// width would; the reading counts the target's width all the same.
	entryWorkers int32
	// want is what the reading records about Instance 0 and the wait it
	// asks the pass for.
	want rolledSetReading
}

// rolledSetReading is what the rolled-set reading records about one
// Instance: whether it holds a slot, whether it is crashed and out of
// rotation, and the wait it asks the pass for before its next pass.
type rolledSetReading struct {
	notServing, crashed, outOfRotation bool
	wait                               time.Duration
}

const (
	rolledSetTarget  = "svc-engine-rev-b"
	rolledSetRunning = "svc-engine-rev-a"
)

// rolledSetPod is one pod of an Instance's set: labelled with rev's hash
// (no label when rev is empty), its main container running since started
// and, when serving, Ready behind the serving gate since readyAt. A
// container that began after the row entered Ready reads as a restart, so
// a pod that never restarted starts before the row's anchor.
func rolledSetPod(name, rev string, serving bool, started, readyAt time.Time) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{}}}
	if rev != "" {
		pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev)
	}
	ready := corev1.ConditionFalse
	if serving {
		ready = corev1.ConditionTrue
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, Ready: serving,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(started)}},
	}}
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: ready, LastTransitionTime: metav1.NewTime(readyAt)},
		{Type: corev1.PodReady, Status: ready, LastTransitionTime: metav1.NewTime(readyAt)},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readyAt)},
	}
	return pod
}

// restartedSince records one restart of pod's main container, as the
// kubelet records it: the run before ended at finishedAt and the current
// run began a second later.
func restartedSince(pod *corev1.Pod, finishedAt time.Time) *corev1.Pod {
	status := &pod.Status.ContainerStatuses[0]
	status.RestartCount = 1
	status.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ExitCode: 1, Reason: "Error",
		StartedAt:  metav1.NewTime(finishedAt.Add(-time.Hour)),
		FinishedAt: metav1.NewTime(finishedAt),
	}}
	status.State.Running.StartedAt = metav1.NewTime(finishedAt.Add(time.Second))
	return pod
}

func beingDeleted(pod *corev1.Pod, at time.Time) *corev1.Pod {
	pod.DeletionTimestamp = &metav1.Time{Time: at}
	return pod
}

// rolledSetRow is a Ready row on rev with pods pods, Ready since readySince.
func rolledSetRow(rev string, pods int32, readySince time.Time) types.InstanceStatus {
	return types.InstanceStatus{
		Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: rev,
		PodCount: pods, ServingPodCount: pods, AvailablePodCount: pods, Admitted: true,
		ReadySince: &metav1.Time{Time: readySince},
	}
}

// rolledSetPictures are the pictures of one Instance on the target that hold
// what the reading counts: the set's width is the target's recorded layout,
// the set's members are the live pods on the target or with no revision,
// and a comeback or a pod unready inside its grace asks for a next pass.
func rolledSetPictures(now time.Time) []rolledSetPicture {
	readySince := now.Add(-time.Hour)
	started := readySince.Add(-time.Minute)
	serving := func(name, rev string) *corev1.Pod { return rolledSetPod(name, rev, true, started, readySince) }
	return []rolledSetPicture{
		{name: "a gang serving every pod of the target's recorded width is not the roll's open work",
			workers: 3, targetWidth: 1, row: rolledSetRow(rolledSetTarget, 2, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget), serving("worker-0", rolledSetTarget)}},
		{name: "a gang short of the target's recorded width holds a slot",
			workers: 3, targetWidth: 1, row: rolledSetRow(rolledSetTarget, 2, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget)},
			want: rolledSetReading{notServing: true}},
		{name: "an entry rendered at another revision's width is read at the target's width all the same",
			workers: 3, targetWidth: 1, entryWorkers: 5, row: rolledSetRow(rolledSetTarget, 2, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget), serving("worker-0", rolledSetTarget)}},
		{name: "a target with no recorded width renders the spec's layout, which the set serves",
			workers: 3, targetWidth: -1, row: rolledSetRow(rolledSetTarget, 4, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget), serving("worker-0", rolledSetTarget), serving("worker-1", rolledSetTarget), serving("worker-2", rolledSetTarget)}},
		{name: "a target with no recorded width renders the spec's layout, which the set is short of",
			workers: 3, targetWidth: -1, row: rolledSetRow(rolledSetTarget, 4, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget), serving("worker-0", rolledSetTarget)},
			want: rolledSetReading{notServing: true}},
		{name: "a pod of another revision is not a member of the set",
			workers: 3, targetWidth: 1, row: rolledSetRow(rolledSetTarget, 2, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget), serving("worker-0", rolledSetRunning)},
			want: rolledSetReading{notServing: true}},
		{name: "a pod with no revision label is a member of the set",
			workers: 3, targetWidth: 1, row: rolledSetRow(rolledSetTarget, 2, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget), serving("worker-0", "")}},
		{name: "a pod being deleted is not a member of the set",
			workers: 3, targetWidth: 1, row: rolledSetRow(rolledSetTarget, 2, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget), beingDeleted(serving("worker-0", rolledSetTarget), now)},
			want: rolledSetReading{notServing: true}},
		{name: "a restart on a pod of another revision is not the set's crash",
			workers: 3, targetWidth: 1, row: rolledSetRow(rolledSetTarget, 2, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget),
				restartedSince(rolledSetPod("worker-0", rolledSetRunning, true, started, now.Add(-15*time.Second)), now.Add(-20*time.Second))},
			want: rolledSetReading{notServing: true}},
		{name: "a restart on a pod being deleted is not the set's crash",
			workers: 3, targetWidth: 1, row: rolledSetRow(rolledSetTarget, 2, readySince),
			pods: []*corev1.Pod{serving("leader", rolledSetTarget),
				beingDeleted(restartedSince(rolledSetPod("worker-0", rolledSetTarget, true, started, now.Add(-15*time.Second)), now.Add(-20*time.Second)), now)},
			want: rolledSetReading{notServing: true}},
		{name: "a runner Ready again for less than the window holds a slot and asks for the next pass when the window ends",
			targetWidth: -1, row: rolledSetRow(rolledSetTarget, 1, readySince),
			pods: []*corev1.Pod{restartedSince(rolledSetPod("engine", rolledSetTarget, true, started, now.Add(-15*time.Second)), now.Add(-20*time.Second))},
			want: rolledSetReading{notServing: true, crashed: true, wait: 15 * time.Second}},
		{name: "a runner down after a restart is crashed and out of rotation",
			targetWidth: -1, row: rolledSetRow(rolledSetTarget, 1, readySince),
			pods: []*corev1.Pod{restartedSince(rolledSetPod("engine", rolledSetTarget, false, started, now.Add(-15*time.Second)), now.Add(-20*time.Second))},
			want: rolledSetReading{notServing: true, crashed: true, outOfRotation: true}},
		{name: "a pod unready past the grace serves nothing and is not the roll's open work",
			targetWidth: -1, row: rolledSetRow(rolledSetTarget, 1, readySince), grace: time.Minute,
			pods: []*corev1.Pod{rolledSetPod("engine", rolledSetTarget, false, started, now.Add(-2*time.Minute))}},
		{name: "a pod unready inside the grace holds a slot and asks for the next pass when the grace ends",
			targetWidth: -1, row: rolledSetRow(rolledSetTarget, 1, readySince), grace: time.Minute,
			pods: []*corev1.Pod{rolledSetPod("engine", rolledSetTarget, false, started, now.Add(-30*time.Second))},
			want: rolledSetReading{notServing: true, wait: 30 * time.Second}},
		{name: "a row on another revision is not read",
			targetWidth: -1, row: rolledSetRow(rolledSetRunning, 1, readySince),
			pods: []*corev1.Pod{serving("engine", rolledSetRunning)}},
		{name: "a row with an attempt in flight is anchored by the in-flight counts, not read",
			targetWidth: -1, row: types.InstanceStatus{Index: 0, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: rolledSetTarget, PodCount: 1,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: rolledSetTarget}},
			pods: []*corev1.Pod{serving("engine", rolledSetTarget)}},
	}
}

// rolledSetInput builds the input of one picture: a Component of one
// Instance whose spec asks for the picture's layout, the row and the width
// the target records, a fixed clock and a sink for the waits asked for.
func rolledSetInput(t *testing.T, pic rolledSetPicture, now time.Time) (types.ReconcileInput, types.ComponentPlan, *appsv1.ControllerRevision, *ObservedSnapshot) {
	t.Helper()
	desired := singlePodDesired(1, types.Lifecycle{})
	if pic.workers > 0 {
		desired = multiPodDesired(1, pic.workers, types.Lifecycle{})
	}
	desired.MinReadySeconds = 30
	observed := types.WorkloadObservedState{InstanceStatuses: []types.InstanceStatus{pic.row}}
	if pic.targetWidth >= 0 {
		observed.WorkerSizeByRevision = map[string]int32{rolledSetTarget: pic.targetWidth}
	}
	input := types.ReconcileInput{
		Key:           types.Key{Namespace: "ns", Component: types.ComponentEngine, OwnerName: "svc"},
		DesiredSpec:   desired,
		ObservedState: observed,
		StuckPodGrace: pic.grace,
		PassWake:      &types.PassWake{},
		Clock:         clocktesting.NewFakeClock(now),
	}
	plan, err := BuildPlan(types.ComponentEngine, input.DesiredSpec, input.ObservedState)
	if err != nil {
		t.Fatal(err)
	}
	if pic.entryWorkers > 0 {
		plan.Instances[0].Runners = []types.RunnerPlan{{Name: types.RunnerLeader, Size: 1}, {Name: types.RunnerWorker, Size: pic.entryWorkers}}
	}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: rolledSetTarget, Namespace: "ns", CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
	}}
	return input, plan, target, SnapshotWithPodsForTest(input, map[int32][]*corev1.Pod{0: pic.pods})
}

// TestRecordRolledSet_ReadsTheSetAtTheTargetsWidth holds the rolled-set
// reading of the update selection on one Instance already on the target:
// the width it is read against is the target's recorded layout, not the
// spec's or the plan's own entry for the Instance; its members are the live pods on the
// target or carrying no revision; a comeback inside the window and a pod
// unready inside its grace hold a slot and ask for a next pass; a pod unready
// past the grace, a row on another revision and an attempt in flight
// are not the roll's open work.
func TestRecordRolledSet_ReadsTheSetAtTheTargetsWidth(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ctx := logr.NewContext(context.Background(), logr.Discard())
	for _, pic := range rolledSetPictures(now) {
		t.Run(pic.name, func(t *testing.T) {
			input, plan, target, snapshot := rolledSetInput(t, pic, now)
			selection, err := RolledSetReadingForTest(ctx, input, plan, target, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			got := rolledSetReading{
				notServing:    slices.Contains(selection.RolledNotServing, 0),
				crashed:       slices.Contains(selection.CrashedNotServing, 0),
				outOfRotation: slices.Contains(selection.CrashedOutOfRotation, 0),
				wait:          input.PassWake.Pending(),
			}
			if got != pic.want {
				t.Errorf("rolled-set reading = %+v, want %+v", got, pic.want)
			}
		})
	}
}
