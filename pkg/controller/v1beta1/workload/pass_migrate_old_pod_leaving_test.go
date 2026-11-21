package workload_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file follows the old pod of a move from the moment OME deletes it.
//
// The story. A service runs 12 prefill and 8 decode Instances and nothing
// is rolling. The operator asks OME to move prefill Instance 6 off its
// node, and then Instance 0 off its own. OME takes the two moves one
// after the other. For the first it builds a replacement on another
// node, waits until it serves, and deletes the old pod. Kubernetes gives
// the pod 60 seconds to stop.
//
// Two readers look at the old pod from then on. The plan asks whether
// the move has nothing left to do but wait, so that the next move may
// start. The move's own pass tells the operator: it writes the wait on
// the move's record and records one Warning.
//
// Both read the pod's deletion time against the clock. A deletion time
// is not a time the pod missed. The API server sets it one grace ahead
// when the pod is deleted. When the kubelet has stopped the pod it sends
// one last delete, and the API server answers in two writes: the first
// moves the deletion time to the present second, the second removes the
// pod. So a pod that stopped in 2 of its 60 seconds reads, between those
// writes, like a pod that never stopped.
//
// The plan lets the next move start as soon as the deletion time is
// behind the clock. The pass speaks only once the pod has stood past its
// deletion time for one operation requeue, the interval the operator
// sets for a pass to come back. A pod that is leaving is gone by then.
//
// The table below is what both readers make of every picture of the old
// pod: how far the clock stands from its deletion time, whether its node
// still reports, whether a force-delete policy is set, and whether a
// finalizer of another controller holds it. The walks after the table
// run four of those pictures on the engine's own cadence and say when
// each Warning comes.

const (
	// oldPodMoved and oldPodNext are the two Instances the operator asks
	// off their nodes, in that order.
	oldPodMoved int32 = 6
	oldPodNext  int32 = 0
	// oldPodTemplateGrace is the time the pod template gives a pod to stop.
	oldPodTemplateGrace = 60 * time.Second
	// oldPodStopsIn is how long the old pod takes to stop on a node that
	// reports.
	oldPodStopsIn = 2 * time.Second
	// oldPodIntoTheSecond is how far into a second a pass reads the pod.
	// The API keeps a deletion time in whole seconds, so a pod whose
	// deletion time is the present second reads as a fraction past it.
	oldPodIntoTheSecond = 500 * time.Millisecond
	// oldPodFinalizer is a finalizer another controller put on the pod.
	oldPodFinalizer = "example.com/hold"
	// oldPodStartPasses bounds the passes the first move takes to delete
	// its old pod.
	oldPodStartPasses = 40
	// oldPodPictures is the size of the table: five ages of the pod, each
	// in eight places.
	oldPodPictures = 40
	// oldPodUntruePictures is how many pictures get a Warning that says
	// something untrue.
	oldPodUntruePictures = 4
)

// What OME writes about a move that waits on its old pod. The record's
// text opens with one word of the engine's own for a move it has set
// aside, and goes on as oldPodPastItsTime says. With no force-delete
// policy it ends as oldPodOnlyAPerson says. The Warning repeats the
// record's text and ends as oldPodLaterMovesPass says. Until then the
// record reads oldPodDraining.
const (
	oldPodDraining       = "source draining"
	oldPodWaitOpens      = "on source teardown: "
	oldPodPastItsTime    = oldPodWaitOpens + "pod %s is Terminating past its own deletion deadline"
	oldPodOnlyAPerson    = "; no forceDelete policy is configured, so only its kubelet or an operator can remove it"
	oldPodLaterMovesPass = "; later migration requests for component=%s instance=%d dispatch ahead of it"
)

// What a Warning gets wrong, in the words the table prints.
const (
	// aFinalizerHoldsThePod: the Warning says only the pod's kubelet or an
	// operator can remove it. The kubelet cannot: the pod stays until the
	// finalizer's owner releases it.
	aFinalizerHoldsThePod = "a finalizer holds the pod: its kubelet cannot remove it, and the Warning names neither the finalizer nor its owner"
)

// oldPodAge is how far the clock stands from the old pod's deletion time.
type oldPodAge uint8

const (
	insideItsGrace oldPodAge = iota
	atThePresentSecond
	oneSecondPast
	oneRequeuePast
	minutesPast
)

var oldPodAges = [...]string{
	"inside its grace",
	"deletion time is the present second",
	"one second past",
	"one operation requeue past",
	"minutes past",
}

func (a oldPodAge) String() string { return oldPodAges[a] }

// past is how long the pod has stood past its deletion time at the pass
// that reads it.
func (a oldPodAge) past(requeue time.Duration) time.Duration {
	switch a {
	case atThePresentSecond:
		return oldPodIntoTheSecond
	case oneSecondPast:
		return time.Second
	case oneRequeuePast:
		return requeue
	case minutesPast:
		return 10 * time.Minute
	}
	return 0
}

// The three columns of the table: where the old pod stands.
type (
	oldPodNode   bool
	oldPodPolicy bool
	oldPodHold   bool
)

const (
	// nodeReports: the pod's node still reports. nodeStopped: it stopped
	// reporting when OME deleted the pod.
	nodeReports oldPodNode = true
	nodeStopped oldPodNode = false
	// policySet: the operator set a force-delete policy.
	policySet oldPodPolicy = true
	noPolicy  oldPodPolicy = false
	// finalizerHolds: a finalizer of another controller holds the pod.
	finalizerHolds oldPodHold = true
	noFinalizer    oldPodHold = false
)

// oldPodOutcome is what the plan and one pass make of a picture.
type oldPodOutcome uint8

const (
	// waitsUnsaid: no Warning. The record still reads "source draining".
	// The move stays first in line and the later move waits behind it.
	waitsUnsaid oldPodOutcome = iota
	// waitsWhileTheLaterMoveStarts: no Warning, and the record still reads
	// "source draining": the pod has not stood past its deletion time for
	// one operation requeue. The plan sets the move aside all the same
	// and the later move starts in the same pass.
	waitsWhileTheLaterMoveStarts
	// warnsOnlyAPerson: one Warning MigrationParked, with the same words
	// on the record: the pod is past its deletion time, no force-delete
	// policy is set, so only its kubelet or an operator can remove it.
	// The plan sets the move aside and the later move starts in the same
	// pass.
	warnsOnlyAPerson
	// warnsPastItsTime: one Warning MigrationParked that says the pod is
	// past its deletion time, and no more. The plan sets the move aside
	// and the later move starts in the same pass.
	warnsPastItsTime
	// warnsAndNamesTheFinalizer: as warnsPastItsTime, and a second Warning,
	// PodDeleteBlockedByFinalizer, names the finalizer and says its owner
	// must release it.
	warnsAndNamesTheFinalizer
	// policyRemovesThePod: the force-delete policy removes the pod with the
	// Warning PodForceDeleted, and the move completes in the same pass. No
	// MigrationParked is recorded. The later move starts on the next pass.
	policyRemovesThePod
)

var oldPodOutcomes = [...]string{
	"no Warning; the record reads \"source draining\"; the later move waits",
	"no Warning; the record reads \"source draining\"; the later move starts",
	"Warning: past its deletion time, no policy, only its kubelet or an operator can remove it; the later move starts",
	"Warning: past its deletion time; the later move starts",
	"Warning: past its deletion time; a second Warning names the finalizer; the later move starts",
	"no Warning of the move: the policy removes the pod and the move completes",
}

func (o oldPodOutcome) String() string { return oldPodOutcomes[o] }

// oldPodCell is one picture of the old pod and what OME makes of it.
type oldPodCell struct {
	age     oldPodAge
	node    oldPodNode
	policy  oldPodPolicy
	hold    oldPodHold
	outcome oldPodOutcome
	// untrue says what the Warning of this picture gets wrong. Empty: the
	// picture gets no Warning, or a true one.
	untrue string
}

func (c oldPodCell) String() string {
	return fmt.Sprintf("%s, %s", c.age, c.place())
}

// place names the three columns of the picture.
func (c oldPodCell) place() string {
	parts := []string{"node stopped reporting", "no policy", "no finalizer"}
	if c.node == nodeReports {
		parts[0] = "node reports"
	}
	if c.policy == policySet {
		parts[1] = "policy set"
	}
	if c.hold == finalizerHolds {
		parts[2] = "finalizer"
	}
	return strings.Join(parts, ", ")
}

// oldPodTable is what OME makes of every picture of the old pod.
var oldPodTable = []oldPodCell{
	// The pod has 30 of its 60 seconds left. Nothing is said, wherever it
	// stands.
	{insideItsGrace, nodeReports, noPolicy, noFinalizer, waitsUnsaid, ""},
	{insideItsGrace, nodeReports, noPolicy, finalizerHolds, waitsUnsaid, ""},
	{insideItsGrace, nodeReports, policySet, noFinalizer, waitsUnsaid, ""},
	{insideItsGrace, nodeReports, policySet, finalizerHolds, waitsUnsaid, ""},
	{insideItsGrace, nodeStopped, noPolicy, noFinalizer, waitsUnsaid, ""},
	{insideItsGrace, nodeStopped, noPolicy, finalizerHolds, waitsUnsaid, ""},
	{insideItsGrace, nodeStopped, policySet, noFinalizer, waitsUnsaid, ""},
	{insideItsGrace, nodeStopped, policySet, finalizerHolds, waitsUnsaid, ""},

	// The deletion time is the present second. On a node that reports
	// this is the kubelet's last delete at work: the pod is gone within
	// the second unless a finalizer holds it. Nothing is said yet,
	// wherever the pod stands, and the later move starts at once.
	{atThePresentSecond, nodeReports, noPolicy, noFinalizer, waitsWhileTheLaterMoveStarts, ""},
	{atThePresentSecond, nodeReports, noPolicy, finalizerHolds, waitsWhileTheLaterMoveStarts, ""},
	{atThePresentSecond, nodeReports, policySet, noFinalizer, waitsWhileTheLaterMoveStarts, ""},
	{atThePresentSecond, nodeReports, policySet, finalizerHolds, waitsWhileTheLaterMoveStarts, ""},
	{atThePresentSecond, nodeStopped, noPolicy, noFinalizer, waitsWhileTheLaterMoveStarts, ""},
	{atThePresentSecond, nodeStopped, noPolicy, finalizerHolds, waitsWhileTheLaterMoveStarts, ""},
	{atThePresentSecond, nodeStopped, policySet, noFinalizer, waitsWhileTheLaterMoveStarts, ""},
	{atThePresentSecond, nodeStopped, policySet, finalizerHolds, waitsWhileTheLaterMoveStarts, ""},

	// One second past. A pass that reads the pod right after the last
	// delete still sees this: the stored time is up to a second behind
	// the write. Nothing is said yet here either.
	{oneSecondPast, nodeReports, noPolicy, noFinalizer, waitsWhileTheLaterMoveStarts, ""},
	{oneSecondPast, nodeReports, noPolicy, finalizerHolds, waitsWhileTheLaterMoveStarts, ""},
	{oneSecondPast, nodeReports, policySet, noFinalizer, waitsWhileTheLaterMoveStarts, ""},
	{oneSecondPast, nodeReports, policySet, finalizerHolds, waitsWhileTheLaterMoveStarts, ""},
	{oneSecondPast, nodeStopped, noPolicy, noFinalizer, waitsWhileTheLaterMoveStarts, ""},
	{oneSecondPast, nodeStopped, noPolicy, finalizerHolds, waitsWhileTheLaterMoveStarts, ""},
	{oneSecondPast, nodeStopped, policySet, noFinalizer, waitsWhileTheLaterMoveStarts, ""},
	{oneSecondPast, nodeStopped, policySet, finalizerHolds, waitsWhileTheLaterMoveStarts, ""},

	// One operation requeue past: the pod outlived a whole pass interval
	// behind its deletion time, so OME says it stays. A kubelet that
	// reports is late with it.
	{oneRequeuePast, nodeReports, noPolicy, noFinalizer, warnsOnlyAPerson, ""},
	{oneRequeuePast, nodeReports, noPolicy, finalizerHolds, warnsOnlyAPerson, aFinalizerHoldsThePod},
	{oneRequeuePast, nodeReports, policySet, noFinalizer, warnsPastItsTime, ""},
	{oneRequeuePast, nodeReports, policySet, finalizerHolds, warnsPastItsTime, ""},
	{oneRequeuePast, nodeStopped, noPolicy, noFinalizer, warnsOnlyAPerson, ""},
	{oneRequeuePast, nodeStopped, noPolicy, finalizerHolds, warnsOnlyAPerson, aFinalizerHoldsThePod},
	{oneRequeuePast, nodeStopped, policySet, noFinalizer, warnsPastItsTime, ""},
	{oneRequeuePast, nodeStopped, policySet, finalizerHolds, warnsPastItsTime, ""},

	// Ten minutes past, beyond both thresholds of the policy. The policy
	// removes a pod on a node that stopped reporting, and names the
	// finalizer of a pod it may not remove.
	{minutesPast, nodeReports, noPolicy, noFinalizer, warnsOnlyAPerson, ""},
	{minutesPast, nodeReports, noPolicy, finalizerHolds, warnsOnlyAPerson, aFinalizerHoldsThePod},
	{minutesPast, nodeReports, policySet, noFinalizer, warnsPastItsTime, ""},
	{minutesPast, nodeReports, policySet, finalizerHolds, warnsAndNamesTheFinalizer, ""},
	{minutesPast, nodeStopped, noPolicy, noFinalizer, warnsOnlyAPerson, ""},
	{minutesPast, nodeStopped, noPolicy, finalizerHolds, warnsOnlyAPerson, aFinalizerHoldsThePod},
	{minutesPast, nodeStopped, policySet, noFinalizer, policyRemovesThePod, ""},
	{minutesPast, nodeStopped, policySet, finalizerHolds, warnsAndNamesTheFinalizer, ""},
}

// oldPodReading is what the plan and one pass made of a picture.
type oldPodReading struct {
	// laterMovePasses: the plan sets the move aside and takes the later
	// move as the one the pass drives.
	laterMovePasses bool
	// laterMoveStarts: the pass built the later move's replacement.
	laterMoveStarts bool
	// warning is the text of the Warning MigrationParked after the move's
	// id and the engine's one word, empty when the pass recorded none.
	warning string
	// record is the move's record after the pass: its phase, then its
	// text without the engine's one word.
	record string
	// also names the other events the pass recorded about the move or
	// its old pod.
	also string
}

// reading is what the outcome says the plan and the pass do.
func (o oldPodOutcome) reading(st *oldPodStage) oldPodReading {
	past := fmt.Sprintf(oldPodPastItsTime, st.pod)
	later := fmt.Sprintf(oldPodLaterMovesPass, st.side.comp, oldPodMoved)
	switch o {
	case waitsWhileTheLaterMoveStarts:
		return oldPodReading{laterMovePasses: true, laterMoveStarts: true, record: "Draining: " + oldPodDraining}
	case warnsOnlyAPerson:
		return oldPodReading{laterMovePasses: true, laterMoveStarts: true, warning: past + oldPodOnlyAPerson + later, record: "Draining: " + past + oldPodOnlyAPerson}
	case warnsPastItsTime:
		return oldPodReading{laterMovePasses: true, laterMoveStarts: true, warning: past + later, record: "Draining: " + past}
	case warnsAndNamesTheFinalizer:
		return oldPodReading{laterMovePasses: true, laterMoveStarts: true, warning: past + later, record: "Draining: " + past,
			also: string(workloadtypes.EventReasonPodDeleteBlockedByFinalizer)}
	case policyRemovesThePod:
		return oldPodReading{laterMovePasses: true, record: fmt.Sprintf("Completed: migrated to instance=%d", st.replacement),
			also: string(workloadtypes.EventReasonPodForceDeleted) + ", " + string(workloadtypes.EventReasonMigrationCompleted)}
	}
	return oldPodReading{record: "Draining: " + oldPodDraining}
}

// oldPodStage is the service with the first move's old pod deleted and
// the second move waiting its turn.
type oldPodStage struct {
	w    *moveWorld
	side *scaleSide
	// moved and next are the ids of the two moves' records.
	moved, next string
	// pod is the first move's old pod and node the node under it.
	pod, node string
	// replacement is the index the first move built its replacement at.
	replacement int32
	// deletedAt is when OME deleted the old pod.
	deletedAt time.Time
	// nextPass is when the engine asked for its next pass.
	nextPass time.Time
	// seen counts the events already read, and lines is what the passes
	// recorded since, each with the time since the delete.
	seen  int
	lines []string
	// laterStarted says the later move has built its replacement.
	laterStarted bool
}

// oldPodView shows the old pod the way the API server serves a pod on a
// node that reports: without the finalizer the model's kubelet keeps on a
// pod it has not finished stopping.
type oldPodView struct {
	client.Client
	pod string
}

func (v oldPodView) show(pod *corev1.Pod) {
	if pod.Name != v.pod {
		return
	}
	pod.Finalizers = removeString(pod.Finalizers, terminationFinalizer)
	if len(pod.Finalizers) == 0 {
		pod.Finalizers = nil
	}
}

func (v oldPodView) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := v.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if pod, ok := obj.(*corev1.Pod); ok {
		v.show(pod)
	}
	return nil
}

func (v oldPodView) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := v.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if pods, ok := list.(*corev1.PodList); ok {
		for i := range pods.Items {
			v.show(&pods.Items[i])
		}
	}
	return nil
}

// newOldPodWorld brings the service up, files the two move requests and
// runs the prefill side on the engine's own cadence until the first move
// deletes its old pod. It returns the world's picture at that instant.
// The model's kubelet leaves the pod as it is from there: the tests
// place it themselves.
func newOldPodWorld(t *testing.T) (*moveWorld, *scaleSnapshot) {
	t.Helper()
	recreate := workloadtypes.UpdateStrategyRecreatePod
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}}}
	w := newMoveWorld(t, cfg)
	w.reachIdle()
	w.startProbe(w.snapshot())
	s := w.prefill()
	s.h.podGrace = oldPodTemplateGrace
	moved := w.fileMoveRequest(s, oldPodMoved)
	w.clk.Step(time.Second)
	w.fileMoveRequest(s, oldPodNext)
	for i := 0; i < oldPodStartPasses; i++ {
		res, err := s.h.pass()
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if pod := deletedPodOf(w, s, oldPodMoved); pod != nil {
			if rec := s.h.migration(moved); rec == nil || rec.Phase != workloadtypes.MigrationPhaseDraining || rec.Message != oldPodDraining {
				t.Fatalf("the old pod is deleted while the move's record reads %v", rec)
			}
			if res.RequeueAfter != s.h.requeue.Operation {
				t.Fatalf("the pass that deleted the old pod asked to come back in %s, want the operation requeue %s", res.RequeueAfter, s.h.requeue.Operation)
			}
			s.h.stillStopping = map[string]int{pod.Name: 1 << 30}
			return w, w.snapshot()
		}
		w.clk.Step(max(res.RequeueAfter, s.h.requeue.Operation))
	}
	t.Fatalf("the first move never deleted its old pod; picture: %s", w.picture())
	return nil, nil
}

// deletedPodOf is the pod of Instance idx that has been asked to leave,
// nil when there is none.
func deletedPodOf(w *moveWorld, s *scaleSide, idx int32) *corev1.Pod {
	for _, pod := range w.pods(s) {
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx && pod.DeletionTimestamp != nil {
			return pod
		}
	}
	return nil
}

// openOldPodStage puts the world back at the instant OME deleted the old
// pod. Without a policy the force-delete policy is dropped, and a held
// pod gets a finalizer of another controller.
func openOldPodStage(t *testing.T, w *moveWorld, snap *scaleSnapshot, policy oldPodPolicy, hold oldPodHold) *oldPodStage {
	t.Helper()
	w.startProbe(snap)
	s := w.prefill()
	h := s.h
	pod := deletedPodOf(w, s, oldPodMoved)
	if pod == nil {
		t.Fatalf("the old pod is gone from the restored world")
	}
	st := &oldPodStage{w: w, side: s, pod: pod.Name, node: pod.Spec.NodeName, deletedAt: w.clk.Now()}
	for _, rec := range h.migrations {
		switch rec.SourceInstance {
		case oldPodMoved:
			st.moved = rec.RequestUUID
			if rec.SurgeInstance != nil {
				st.replacement = *rec.SurgeInstance
			}
		case oldPodNext:
			st.next = rec.RequestUUID
		}
	}
	if until := h.terminatingUntil[st.pod]; !until.Equal(st.deletedAt.Add(oldPodTemplateGrace)) {
		t.Fatalf("the old pod's deletion time stands %s after its delete, want its grace of %s", until.Sub(st.deletedAt), oldPodTemplateGrace)
	}
	if hold == finalizerHolds {
		pod.Finalizers = append(pod.Finalizers, oldPodFinalizer)
		if err := h.c.Update(h.ctx, pod); err != nil {
			t.Fatalf("add the finalizer: %v", err)
		}
	}
	if policy == noPolicy {
		h.forceDelete = nil
	} else if h.forceDelete == nil {
		t.Fatalf("the world carries no force-delete policy to set")
	}
	h.c = oldPodView{Client: h.c, pod: st.pod}
	st.seen = len(h.events)
	st.nextPass = st.deletedAt.Add(h.requeue.Operation)
	return st
}

// stageOldPodCell places the old pod as the cell says. A pod inside its
// grace has half of it left. Any other pod's deletion time is set to the
// present second, as the kubelet's last delete or the end of the grace
// leaves it, and the clock then moves past it by the cell's age.
func stageOldPodCell(t *testing.T, w *moveWorld, snap *scaleSnapshot, cell oldPodCell) *oldPodStage {
	t.Helper()
	st := openOldPodStage(t, w, snap, cell.policy, cell.hold)
	h := st.side.h
	if cell.node == nodeStopped {
		h.failNode(st.node)
	}
	if cell.age == insideItsGrace {
		w.clk.Step(oldPodTemplateGrace / 2)
		return st
	}
	h.terminatingUntil[st.pod] = w.clk.Now()
	w.clk.Step(cell.age.past(h.requeue.Operation))
	return st
}

// planLetsTheLaterMovePass is the plan's reading of the picture before
// the pass runs: true when it sets the first move aside as one that can
// only wait and takes the later move as the one the pass drives, false
// when the first move stays the one the pass drives.
func (st *oldPodStage) planLetsTheLaterMovePass(t *testing.T) bool {
	t.Helper()
	h := st.side.h
	in := h.buildInput()
	plan, err := workload.BuildPlan(h.component, h.desired, in.ObservedState)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	deps := workloadtypes.Deps{Client: h.c, APIReader: h.c, Expectations: workloadtypes.NewExpectations()}
	decision, err := workload.Plan(h.ctx, in, plan, h.target, workload.NewObservedSnapshot(deps, in, plan.Component, in.ObservedState.InstanceStatuses))
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	action := findAction(decision, workload.ActionMigrate)
	if action == nil || action.Migration == nil || action.Migration.Head == nil {
		t.Fatalf("the plan drives no move")
	}
	setAside := slices.ContainsFunc(action.Migration.Parked, func(rec workloadtypes.MigrationRecord) bool { return rec.RequestUUID == st.moved })
	head := action.Migration.Head.RequestUUID
	if setAside != (head == st.next) || !setAside != (head == st.moved) {
		t.Fatalf("the plan sets the first move aside: %t, and drives %s", setAside, head)
	}
	return setAside
}

// onePass reads the plan, runs one pass and returns what both made of
// the picture.
func (st *oldPodStage) onePass(t *testing.T) oldPodReading {
	t.Helper()
	got := oldPodReading{laterMovePasses: st.planLetsTheLaterMovePass(t)}
	h := st.side.h
	if _, err := h.pass(); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	moved, next := h.migration(st.moved), h.migration(st.next)
	if moved == nil || next == nil {
		t.Fatalf("a move's record is gone: %v, %v", moved, next)
	}
	got.laterMoveStarts = next.SurgeAllocated()
	got.record = string(moved.Phase) + ": " + afterTheFirstWord(moved.Message)
	var also []string
	said := "Warning " + string(workloadtypes.EventReasonMigrationParked) + " OMENative migration uuid=" + st.moved + " "
	for _, e := range h.events[st.seen:] {
		if text, ok := strings.CutPrefix(e, said); ok {
			if got.warning != "" {
				t.Fatalf("one pass recorded the Warning twice: %q", e)
			}
			got.warning = afterTheFirstWord(text)
			continue
		}
		if reason := oldPodEventReason(e); st.names(e) && reason != "" {
			also = append(also, reason)
		}
	}
	got.also = strings.Join(also, ", ")
	return got
}

// names reports an event that names the first move or its old pod.
func (st *oldPodStage) names(event string) bool {
	return strings.Contains(event, st.moved) || strings.Contains(event, st.pod)
}

// oldPodEventReason is the reason of an event the tests follow, empty for
// any other event.
func oldPodEventReason(event string) string {
	for _, reason := range []workloadtypes.EventReason{
		workloadtypes.EventReasonMigrationParked,
		workloadtypes.EventReasonMigrationCompleted,
		workloadtypes.EventReasonPodForceDeleted,
		workloadtypes.EventReasonPodDeleteBlockedByFinalizer,
	} {
		if strings.Contains(event, " "+string(reason)+" ") {
			return string(reason)
		}
	}
	return ""
}

// afterTheFirstWord drops the first word of a text that goes on as the
// wait on the old pod opens, and returns any other text as it is.
func afterTheFirstWord(text string) string {
	if _, rest, found := strings.Cut(text, " "); found && strings.HasPrefix(rest, oldPodWaitOpens) {
		return rest
	}
	return text
}

// TestReconcile_WhatAMoveSaysAboutItsOldPod holds the plan and one pass
// to oldPodTable, picture by picture, and prints the table. No picture
// of a pod that is leaving by itself gets a Warning. Four of the 40
// pictures get a Warning that says something untrue: a pod a finalizer
// holds, with no force-delete policy, a requeue or more past its
// deletion time.
func TestReconcile_WhatAMoveSaysAboutItsOldPod(t *testing.T) {
	w, snap := newOldPodWorld(t)
	seen := map[string]bool{}
	untrue := 0
	printed := []string{"| the old pod | where it stands | what OME does | what the Warning gets wrong |", "|---|---|---|---|"}
	for _, cell := range oldPodTable {
		seen[cell.String()] = true
		t.Run(cell.String(), func(t *testing.T) {
			st := stageOldPodCell(t, w, snap, cell)
			got, want := st.onePass(t), cell.outcome.reading(st)
			if got != want {
				t.Errorf("the plan and the pass read the picture another way:\n got  %+v\n want %+v", got, want)
			}
			if cell.untrue != "" && got.warning == "" {
				t.Errorf("the picture is listed as one whose Warning is untrue, and it records no Warning")
			}
		})
		if cell.untrue != "" {
			untrue++
		}
		printed = append(printed, fmt.Sprintf("| %s | %s | %s | %s |", cell.age, cell.place(), cell.outcome, cell.untrue))
	}
	if len(seen) != oldPodPictures || len(oldPodTable) != oldPodPictures {
		t.Errorf("the table holds %d pictures, %d of them distinct, want %d", len(oldPodTable), len(seen), oldPodPictures)
	}
	if untrue != oldPodUntruePictures {
		t.Errorf("%d pictures get a Warning that says something untrue, want %d", untrue, oldPodUntruePictures)
	}
	t.Log("\n" + strings.Join(printed, "\n"))
}

// advanceTo moves the clock to offset after the delete and runs, on the
// way, every pass the engine asked for.
func (st *oldPodStage) advanceTo(t *testing.T, offset time.Duration) {
	t.Helper()
	target := st.deletedAt.Add(offset)
	for !st.nextPass.After(target) {
		st.w.clk.Step(st.nextPass.Sub(st.w.clk.Now()))
		st.passNow(t)
	}
	st.w.clk.Step(target.Sub(st.w.clk.Now()))
}

// passNow runs one pass at the present instant, as a change to a pod or
// a node starts one, and keeps what it recorded about the move and its
// old pod.
func (st *oldPodStage) passNow(t *testing.T) {
	t.Helper()
	h := st.side.h
	res, err := h.pass()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	st.nextPass = st.w.clk.Now().Add(max(res.RequeueAfter, h.requeue.Operation))
	at := "+" + st.w.clk.Now().Sub(st.deletedAt).String()
	for _, e := range h.events[st.seen:] {
		if reason := oldPodEventReason(e); st.names(e) && reason != "" {
			kind, _, _ := strings.Cut(e, " ")
			st.lines = append(st.lines, at+" "+kind+" "+reason)
		}
	}
	st.seen = len(h.events)
	if next := h.migration(st.next); !st.laterStarted && next != nil && next.SurgeAllocated() {
		st.laterStarted = true
		st.lines = append(st.lines, at+" the later move starts")
	}
}

// lastDeleteMovesTheDeletionTime plays the first write of the kubelet's
// last delete: the API server moves the pod's deletion time to the
// present second. The pod stays until it is removed.
func (st *oldPodStage) lastDeleteMovesTheDeletionTime() {
	st.side.h.terminatingUntil[st.pod] = st.w.clk.Now().Truncate(time.Second)
}

// removeThePod takes the old pod out of the API server: the second write
// of the last delete, or the release of the last finalizer.
func (st *oldPodStage) removeThePod(t *testing.T) {
	t.Helper()
	h := st.side.h
	pod := deletedPodOf(st.w, st.side, oldPodMoved)
	if pod == nil {
		t.Fatalf("the old pod is already gone")
	}
	pod.Finalizers = nil
	if err := h.c.Update(h.ctx, pod); err != nil {
		t.Fatalf("remove the old pod: %v", err)
	}
	delete(h.terminatingUntil, st.pod)
}

// operatorForceDeletesThePod is what an operator does about a pod that
// nobody else removes: a delete with a grace of zero.
func (st *oldPodStage) operatorForceDeletesThePod(t *testing.T) {
	t.Helper()
	h := st.side.h
	pod := deletedPodOf(st.w, st.side, oldPodMoved)
	if pod == nil {
		t.Fatalf("the old pod is already gone")
	}
	if err := h.c.Delete(h.ctx, pod, client.GracePeriodSeconds(0)); err != nil {
		t.Fatalf("force-delete the old pod: %v", err)
	}
}

// recordReads is the first move's record: its phase, then its text
// without the engine's one word.
func (st *oldPodStage) recordReads(t *testing.T) string {
	t.Helper()
	rec := st.side.h.migration(st.moved)
	if rec == nil {
		t.Fatalf("the first move's record is gone")
	}
	return string(rec.Phase) + ": " + afterTheFirstWord(rec.Message)
}

// requireOldPodLines fails unless the passes recorded exactly want.
func requireOldPodLines(t *testing.T, st *oldPodStage, want ...string) {
	t.Helper()
	if !slices.Equal(st.lines, want) {
		t.Fatalf("the passes recorded\n  %s\nwant\n  %s", strings.Join(st.lines, "\n  "), strings.Join(want, "\n  "))
	}
}

// requireOldPodRecord fails unless the first move's record reads want.
func requireOldPodRecord(t *testing.T, st *oldPodStage, want string) {
	t.Helper()
	if got := st.recordReads(t); got != want {
		t.Fatalf("the move's record reads %q, want %q", got, want)
	}
}

// TestReconcile_MoveWhoseOldPodLeavesInTwoSeconds is the healthy move.
// The old pod stops 2 seconds into its 60. Its kubelet sends the last
// delete, and a pass reads the pod between the two writes of that delete:
// still there, its deletion time the present second.
//
// That pass says nothing. The record still reads "source draining" and
// no Warning is recorded, since the pod has not stood past its deletion
// time for an operation requeue. The later move starts in that pass. The
// API server removes the pod in the same instant and the next pass
// completes the move.
func TestReconcile_MoveWhoseOldPodLeavesInTwoSeconds(t *testing.T) {
	w, snap := newOldPodWorld(t)
	st := openOldPodStage(t, w, snap, noPolicy, noFinalizer)
	st.advanceTo(t, oldPodStopsIn+oldPodIntoTheSecond)
	requireOldPodLines(t, st)

	st.lastDeleteMovesTheDeletionTime()
	st.passNow(t)
	requireOldPodRecord(t, st, "Draining: "+oldPodDraining)

	st.removeThePod(t)
	st.passNow(t)
	requireOldPodLines(t, st,
		"+2.5s the later move starts",
		"+2.5s Normal MigrationCompleted",
	)
	requireOldPodRecord(t, st, fmt.Sprintf("Completed: migrated to instance=%d", st.replacement))
}

// TestReconcile_MoveWhoseOldPodSitsOnANodeThatStoppedReporting is the pod
// nobody stops. The node stops reporting 2 seconds after the delete, so
// the pod's deletion time stays where the delete put it, 60 seconds out,
// and the pod stays past it. No force-delete policy is set.
//
// The first pass after the 60 seconds runs 2 seconds past them. It lets
// the later move start and says nothing yet. The Warning comes one
// operation requeue later, 7 seconds past the deletion time. Its words
// are true: only the kubelet, once it is back, or an operator can remove
// the pod. Nothing else happens until the operator removes it.
func TestReconcile_MoveWhoseOldPodSitsOnANodeThatStoppedReporting(t *testing.T) {
	w, snap := newOldPodWorld(t)
	st := openOldPodStage(t, w, snap, noPolicy, noFinalizer)
	st.advanceTo(t, 2*time.Second)
	st.side.h.failNode(st.node)
	st.passNow(t)

	st.advanceTo(t, oldPodTemplateGrace)
	requireOldPodLines(t, st)
	requireOldPodRecord(t, st, "Draining: "+oldPodDraining)

	st.advanceTo(t, oldPodTemplateGrace+2*time.Second)
	requireOldPodLines(t, st, "+1m2s the later move starts")
	requireOldPodRecord(t, st, "Draining: "+oldPodDraining)

	st.advanceTo(t, 10*time.Minute)
	requireOldPodLines(t, st,
		"+1m2s the later move starts",
		"+1m7s Warning MigrationParked",
	)
	requireOldPodRecord(t, st, "Draining: "+fmt.Sprintf(oldPodPastItsTime, st.pod)+oldPodOnlyAPerson)

	st.operatorForceDeletesThePod(t)
	st.passNow(t)
	requireOldPodLines(t, st,
		"+1m2s the later move starts",
		"+1m7s Warning MigrationParked",
		"+10m0s Normal MigrationCompleted",
	)
}

// TestReconcile_MoveWhoseOldPodAFinalizerHolds is the pod another
// controller holds, on a node that reports. The pod stops 2 seconds into
// its 60 and its kubelet sends the last delete. The API server moves the
// deletion time to the present second and keeps the pod for the
// finalizer.
//
// The pass that reads that write lets the later move start and says
// nothing. The Warning comes one operation requeue later, when the pod
// has stood 5.5 seconds past its deletion time. With no force-delete
// policy the words say only the kubelet or an operator can remove the
// pod, which names the wrong party: the kubelet is done with it. With a
// policy set the words stop at the deletion time, and two minutes on, at
// the policy's own threshold, a second Warning names the finalizer. The
// move completes when the finalizer's owner releases the pod.
func TestReconcile_MoveWhoseOldPodAFinalizerHolds(t *testing.T) {
	w, snap := newOldPodWorld(t)
	t.Run("no force-delete policy", func(t *testing.T) {
		st := openOldPodStage(t, w, snap, noPolicy, finalizerHolds)
		st.advanceTo(t, oldPodStopsIn+oldPodIntoTheSecond)
		st.lastDeleteMovesTheDeletionTime()
		st.passNow(t)
		requireOldPodLines(t, st, "+2.5s the later move starts")
		requireOldPodRecord(t, st, "Draining: "+oldPodDraining)

		st.advanceTo(t, 10*time.Minute)
		requireOldPodLines(t, st,
			"+2.5s the later move starts",
			"+7.5s Warning MigrationParked",
		)
		requireOldPodRecord(t, st, "Draining: "+fmt.Sprintf(oldPodPastItsTime, st.pod)+oldPodOnlyAPerson)

		st.removeThePod(t)
		st.passNow(t)
		requireOldPodLines(t, st,
			"+2.5s the later move starts",
			"+7.5s Warning MigrationParked",
			"+10m0s Normal MigrationCompleted",
		)
	})
	t.Run("a force-delete policy is set", func(t *testing.T) {
		st := openOldPodStage(t, w, snap, policySet, finalizerHolds)
		st.advanceTo(t, oldPodStopsIn+oldPodIntoTheSecond)
		st.lastDeleteMovesTheDeletionTime()
		st.passNow(t)
		st.advanceTo(t, 3*time.Minute)
		requireOldPodLines(t, st,
			"+2.5s the later move starts",
			"+7.5s Warning MigrationParked",
			"+2m2.5s Warning PodDeleteBlockedByFinalizer",
		)
		requireOldPodRecord(t, st, "Draining: "+fmt.Sprintf(oldPodPastItsTime, st.pod))
	})
}

// TestReconcile_MoveWhoseOldPodTheForceDeletePolicyRemoves is the pod on
// a node that stopped reporting, with a force-delete policy set. The
// Warning comes as it does without a policy, one operation requeue after
// the first pass past the pod's 60 seconds, and its words stop at the
// deletion time.
//
// The policy acts on its own two thresholds, whatever the move has said.
// It removes the pod with the Warning PodForceDeleted once the node has
// not reported for the threshold, and the move completes in the same
// pass.
func TestReconcile_MoveWhoseOldPodTheForceDeletePolicyRemoves(t *testing.T) {
	w, snap := newOldPodWorld(t)
	st := openOldPodStage(t, w, snap, policySet, noFinalizer)
	st.advanceTo(t, 2*time.Second)
	st.side.h.failNode(st.node)
	st.passNow(t)

	st.advanceTo(t, 4*time.Minute)
	requireOldPodLines(t, st,
		"+1m2s the later move starts",
		"+1m7s Warning MigrationParked",
	)
	requireOldPodRecord(t, st, "Draining: "+fmt.Sprintf(oldPodPastItsTime, st.pod))

	st.advanceTo(t, 6*time.Minute)
	requireOldPodLines(t, st,
		"+1m2s the later move starts",
		"+1m7s Warning MigrationParked",
		"+5m2s Warning PodForceDeleted",
		"+5m2s Normal MigrationCompleted",
	)
}
