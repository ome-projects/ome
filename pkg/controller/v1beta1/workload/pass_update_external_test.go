package workload_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

// Two gangs of one leader and one worker serve version 1 under
// SurgeThenDrain with maxSurge 1 and maxUnavailable 0, and the cluster has
// no room for one more gang. The operator pushes version 2. OME builds the
// replacement gang at a new index, the scheduler refuses both of its pods,
// and the two old gangs keep serving. Inside the placement grace no row
// reads Failed. Once the grace has passed OME ends the attempt: the
// replacement's row reads Failed naming the scheduler's message, version 2
// is charged no retry record, and within a few passes both replacement pods
// are marked for deletion on the grace a retired replacement is given, and are gone.
// While the Failed row stands no second wait is recorded on it and no
// further InstanceFailed is raised. The source returns Ready on version 1
// and a fresh replacement gang is built while the room is still gone; once
// the room is back the roll finishes with both gangs on version 2.
func TestUnplaceableGangSurge_PastTheGraceTheReplacementIsRetiredAndRebuilt(t *testing.T) {
	const noRoom = "0/10 nodes are available: no domain has room for the gang."
	h := newRecoveryHarness(t, true)
	h.atomicStatus = true
	h.recorder = record.NewFakeRecorder(256)
	h.replicas = 2
	maxSurge, maxUnavailable := intstr.FromInt(1), intstr.FromInt(0)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	h.unschedulableGrace = 2 * time.Minute
	h.retiredReplacementGrace = 10 * time.Second
	h.podGrace = 10 * time.Minute
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("two gangs never settled on version 1")
	}

	// oldGangsServe is the promise the surge makes while the room is gone:
	// the four version 1 pods stay in rotation.
	oldGangsServe := func() {
		t.Helper()
		serving := h.servingPods()
		if len(serving) != 4 {
			h.dumpState("serving set")
			t.Fatalf("%d pod(s) serving while the room is gone, want the four version 1 pods", len(serving))
		}
		for _, pod := range serving {
			if pod.Spec.Containers[0].Image != goodImage {
				t.Fatalf("pod %s serves on %s while the room is gone, want version 1", pod.Name, pod.Spec.Containers[0].Image)
			}
		}
	}
	// pair finds the gang surge in flight: the source row that claims a
	// surge index and the replacement's row at that index.
	pair := func() (source, replacementRow *workloadtypes.InstanceStatus) {
		rows := h.irStatuses()
		for i := range rows {
			op := rows[i].Operation
			if op != nil && op.Type == workloadtypes.InstanceOperationUpdate && op.SurgeIndex != nil {
				source = &rows[i]
			}
		}
		if source != nil {
			replacementRow = h.instance(*source.Operation.SurgeIndex)
		}
		return source, replacementRow
	}

	h.holdPlacement(noRoom)
	h.setTarget(h.revFixed, fixedImage)
	var surgeIdx int32
	held := func() bool {
		_, replacementRow := pair()
		if replacementRow == nil || replacementRow.Operation == nil || replacementRow.Operation.Waiting != workloadtypes.WaitingReasonUnschedulable {
			return false
		}
		surgeIdx = replacementRow.Index
		return len(h.podsOf(surgeIdx)) == 2
	}
	if !h.runWithInvariant(12, held, oldGangsServe) {
		h.dumpState("push with no room")
		t.Fatalf("the replacement gang was never refused placement with the wait recorded on its row")
	}
	replacement := h.podsOf(surgeIdx)
	_, refusedSince, refused := workloadtypes.PodUnschedulable(replacement[0])
	if !refused {
		t.Fatalf("replacement pod %s is not one the scheduler refused", replacement[0].Name)
	}

	// Inside the grace the wait is no failure; past it the replacement's
	// row reads Failed.
	replacementRowFailed := func() bool {
		replacementRow := h.instance(surgeIdx)
		return replacementRow != nil && replacementRow.Phase == workloadtypes.InstancePhaseFailed
	}
	if !h.runWithInvariant(10, replacementRowFailed, func() {
		oldGangsServe()
		for _, row := range h.irStatuses() {
			if waited := h.clk.Now().Sub(refusedSince.Time); row.Phase == workloadtypes.InstancePhaseFailed && waited <= h.unschedulableGrace {
				t.Fatalf("Instance %d read Failed %s into a placement grace of %s", row.Index, waited, h.unschedulableGrace)
			}
		}
	}) {
		h.dumpState("past the grace")
		t.Fatalf("the replacement's row never read Failed after the placement grace")
	}
	replacementRow := h.instance(surgeIdx)
	if replacementRow.LastFailure == nil || replacementRow.LastFailure.Reason != workloadtypes.WaitingReasonUnschedulable || !strings.Contains(replacementRow.LastFailure.Message, noRoom) {
		t.Fatalf("the Failed replacement row does not name the scheduler's wait: %+v", replacementRow.LastFailure)
	}
	written := *replacementRow.LastFailure
	warnings := len(h.failedWarnings)
	if warnings == 0 || !strings.Contains(h.failedWarnings[warnings-1], noRoom) {
		t.Fatalf("no InstanceFailed warning names the scheduler's message: %v", h.failedWarnings)
	}
	noCharge := func(moment string) {
		t.Helper()
		block := h.findBlock(h.revFixed.Name)
		if block != nil && (block.AttemptsStarted != 0 || block.State == workloadtypes.RetryBlockBackoff || block.State == workloadtypes.RetryBlockHeld) {
			t.Fatalf("%s: version 2 was charged for a shortage of nodes: %+v", moment, block)
		}
	}
	noCharge("at the Failed write")
	if source, _ := pair(); source != nil {
		t.Logf("source row at the Failed write: phase=%s step=%s record=%+v", source.Phase, source.Operation.Step, source.LastFailure)
	}

	// podState reads one of the first replacement's pods as the apiserver
	// holds it now.
	podState := func(p *corev1.Pod) (deleting, gone bool) {
		t.Helper()
		pod := &corev1.Pod{}
		err := h.c.Get(h.ctx, client.ObjectKeyFromObject(p), pod)
		if apierrors.IsNotFound(err) {
			return true, true
		}
		if err != nil {
			t.Fatalf("get %s: %v", p.Name, err)
		}
		return pod.DeletionTimestamp != nil, false
	}
	// recordKept is what must hold while the ended attempt's pods are
	// retired: the old gangs serve, the Failed row keeps the record the
	// grace wrote, and InstanceFailed is not raised again.
	recordKept := func() {
		t.Helper()
		oldGangsServe()
		standing := false
		for _, p := range replacement {
			if _, gone := podState(p); !gone {
				standing = true
			}
		}
		if row := h.instance(surgeIdx); standing && row != nil {
			if row.Phase != workloadtypes.InstancePhaseFailed || row.LastFailure == nil || !sameTermination(*row.LastFailure, written) {
				t.Fatalf("the Failed replacement row was rewritten while its pods were retired: phase %s, record %+v, want %+v", row.Phase, row.LastFailure, written)
			}
		}
		if len(h.failedWarnings) != warnings {
			t.Fatalf("InstanceFailed was raised again while the ended attempt was retired: %v", h.failedWarnings[warnings:])
		}
	}
	retired := func() bool {
		for _, p := range replacement {
			if deleting, _ := podState(p); !deleting {
				return false
			}
		}
		return true
	}
	if !h.runWithInvariant(3, retired, recordKept) {
		h.dumpState("after the Failed write")
		t.Fatalf("the replacement's pods were not marked for deletion within three passes of the Failed write: the ended attempt's pods are kept and waited on")
	}
	gone := func() bool {
		for _, p := range replacement {
			if _, g := podState(p); !g {
				return false
			}
		}
		return true
	}
	if !h.runWithInvariant(3, gone, recordKept) {
		h.dumpState("retiring the replacement")
		t.Fatalf("the retired replacement's pods outlived the grace a retired replacement is given, %s", h.retiredReplacementGrace)
	}

	// The pair ends: the source returns Ready on version 1 and the roll
	// opens a fresh attempt with new pods, still refused while the room is
	// gone, and still at no charge to version 2.
	if !h.runWithInvariant(4, func() bool { return h.sawEvent(workloadtypes.EventReason("GangSurgeAbandoned")) }, oldGangsServe) {
		h.dumpState("after the retirement")
		t.Fatalf("the pair never ended with the GangSurgeAbandoned event; events = %v", h.events)
	}
	first := map[types.UID]struct{}{}
	for _, p := range replacement {
		first[p.UID] = struct{}{}
	}
	rebuilt := func() bool {
		_, replacementRow := pair()
		if replacementRow == nil {
			return false
		}
		pods := h.podsOf(replacementRow.Index)
		if len(pods) != 2 {
			return false
		}
		for _, p := range pods {
			if _, old := first[p.UID]; old {
				return false
			}
		}
		return true
	}
	if !h.runWithInvariant(8, rebuilt, oldGangsServe) {
		h.dumpState("fresh attempt")
		t.Fatalf("no fresh replacement gang was built after the ended attempt was retired")
	}
	noCharge("at the fresh attempt")

	h.releasePlacement()
	if !h.run(60, func() bool { return h.settledOn(h.revFixed, 2) }) {
		h.dumpState("after the room came back")
		t.Fatalf("the roll did not finish on version 2 once the gang could be placed")
	}
	if len(h.heldWarnings) != 0 {
		t.Fatalf("version 2 was held for a shortage of nodes: %v", h.heldWarnings)
	}
}

// sameTermination reports whether two failure records name the same pod,
// container, reason, message and moment.
func sameTermination(a, b workloadtypes.InstanceTermination) bool {
	return a.PodName == b.PodName && a.ContainerName == b.ContainerName && a.Reason == b.Reason &&
		a.Message == b.Message && a.Time.Equal(&b.Time)
}

// A replacement gang has taken over serving and the old gang has left
// rotation. One member of the replacement is lost while the cluster has no
// room for one more pod: the rebuilt member is refused by the scheduler and
// the Instance serves nothing meanwhile. Inside the placement grace nothing
// reads Failed. Once the grace has passed OME ends the attempt once, naming
// the scheduler's message, and charges version 2 nothing beyond the loss it
// already counted. The refused member is marked for deletion within the
// grace a retired replacement is given and is gone; the placed member keeps
// its placement; a fresh member is built as the next attempt while the room
// is still gone, and no further InstanceFailed is raised before that
// attempt's own grace has passed. A second grace ends the fresh attempt the
// same way. Once the room is back the member is placed and the roll
// finishes on version 2.
//
// The old gang cannot return to serving: it left rotation when the
// replacement took over. The one outcome that keeps the Instance's placed
// members is to rebuild the one member that has no placement.
func TestGangSurgePastTakeover_UnplaceableRebuiltMemberIsRetiredAndRebuiltPastTheGrace(t *testing.T) {
	const noRoom = "0/10 nodes are available: no domain has room for one more member."
	h, surgeIdx := gangSurgeHandedOver(t)
	h.unschedulableGrace = 2 * time.Minute
	h.retiredReplacementGrace = 10 * time.Second
	leader := h.podOf(surgeIdx, "leader").UID
	before := *h.instance(0).Operation

	h.holdPlacement(noRoom)
	h.loseRunner(surgeIdx, "worker")
	h.step()
	h.requireAttemptClosedByLoss(surgeIdx, 1)
	if !h.run(10, h.rearmedRebuild(1)) {
		h.dumpState("lost member")
		t.Fatalf("the lost member was not rebuilt after the loss")
	}
	h.requireFreshWindow(before)

	// member is the replacement's worker as the apiserver holds it now, nil
	// while no live one exists.
	member := func() *corev1.Pod {
		for _, pod := range h.podsOf(surgeIdx) {
			if pod.Labels[query.LabelRunner] == "worker" {
				return pod
			}
		}
		return nil
	}
	// leaderStands is the promise every step below keeps: the placed member
	// keeps its placement and its name, whatever happens to the other.
	leaderStands := func() {
		t.Helper()
		for _, pod := range h.podsOf(surgeIdx) {
			if pod.Labels[query.LabelRunner] == "leader" && pod.UID == leader {
				return
			}
		}
		h.dumpState("leader")
		t.Fatalf("the placed leader was removed along with the member that had no placement")
	}
	tokenOn := func(idx int32) bool {
		row := h.instance(idx)
		return row != nil && row.Operation != nil && row.Operation.Waiting == workloadtypes.WaitingReasonUnschedulable
	}
	refused := func() bool {
		pod := member()
		if pod == nil {
			return false
		}
		_, _, ok := workloadtypes.PodUnschedulable(pod)
		return ok && (tokenOn(surgeIdx) || tokenOn(0))
	}
	if !h.runWithInvariant(8, refused, leaderStands) {
		h.dumpState("rebuilt member with no room")
		t.Fatalf("the rebuilt member was never refused placement with the wait recorded on the pair")
	}
	first := member()
	_, refusedSince, _ := workloadtypes.PodUnschedulable(first)
	// From here deletes linger as Terminating for the pod's own grace, so a
	// retirement on the shorter grace a retired replacement is given can be
	// told from a plain delete.
	h.podGrace = 10 * time.Minute

	warnings := len(h.failedWarnings)
	charged := h.findBlock(h.revFixed.Name)
	if charged == nil || charged.AttemptsStarted != 1 {
		t.Fatalf("the loss is not the one attempt counted on version 2: %+v", charged)
	}
	// noFurtherCharge: a shortage of nodes adds nothing to the one attempt
	// the loss counted, and never holds version 2.
	noFurtherCharge := func(moment string) {
		t.Helper()
		block := h.findBlock(h.revFixed.Name)
		if block != nil && (block.AttemptsStarted != 1 || block.State == workloadtypes.RetryBlockBackoff || block.State == workloadtypes.RetryBlockHeld) {
			t.Fatalf("%s: version 2 was charged for a shortage of nodes: %+v", moment, block)
		}
	}
	// podState reads one pod as the apiserver holds it now; a pod of
	// another UID under the same name is the fresh one, so the first is
	// gone.
	podState := func(p *corev1.Pod) (deleting, gone bool) {
		t.Helper()
		pod := &corev1.Pod{}
		err := h.c.Get(h.ctx, client.ObjectKeyFromObject(p), pod)
		if apierrors.IsNotFound(err) || (err == nil && pod.UID != p.UID) {
			return true, true
		}
		if err != nil {
			t.Fatalf("get %s: %v", p.Name, err)
		}
		return pod.DeletionTimestamp != nil, false
	}
	// ended reports one more InstanceFailed than before; graceKept is what
	// holds until the grace has passed: nothing Failed, nothing charged.
	ended := func() bool { return len(h.failedWarnings) > warnings }
	graceKept := func(since metav1.Time) func() {
		return func() {
			t.Helper()
			leaderStands()
			noFurtherCharge("inside the grace")
			if waited := h.clk.Now().Sub(since.Time); waited <= h.unschedulableGrace {
				for _, row := range h.irStatuses() {
					if row.Phase == workloadtypes.InstancePhaseFailed {
						t.Fatalf("Instance %d read Failed %s into a placement grace of %s", row.Index, waited, h.unschedulableGrace)
					}
				}
			}
		}
	}
	// endedOnce: exactly one InstanceFailed, naming the scheduler's message.
	endedOnce := func() {
		t.Helper()
		if len(h.failedWarnings) != warnings+1 || !strings.Contains(h.failedWarnings[warnings], noRoom) {
			t.Fatalf("want exactly one more InstanceFailed naming the scheduler's message, got %v", h.failedWarnings[warnings:])
		}
		warnings++
	}
	// kept is what holds while the ended attempt's member is retired and
	// the next one built: the leader stands, nothing is charged, and no
	// InstanceFailed is raised again.
	kept := func() {
		t.Helper()
		leaderStands()
		noFurtherCharge("after the ending")
		if len(h.failedWarnings) != warnings {
			t.Fatalf("InstanceFailed was raised again for the same ended attempt: %v", h.failedWarnings[warnings:])
		}
	}
	retireAndRebuild := func(refusedMember *corev1.Pod) *corev1.Pod {
		t.Helper()
		deleting := func() bool { d, _ := podState(refusedMember); return d }
		if !h.runWithInvariant(3, deleting, kept) {
			h.dumpState("after the ending")
			t.Fatalf("the refused member was not marked for deletion within three passes of the ending: the ended attempt's pod is kept and waited on")
		}
		gone := func() bool { _, g := podState(refusedMember); return g }
		if !h.runWithInvariant(3, gone, kept) {
			h.dumpState("retiring the member")
			t.Fatalf("the retired member outlived the grace a retired replacement is given, %s", h.retiredReplacementGrace)
		}
		rebuilt := func() bool {
			pod := member()
			return pod != nil && pod.UID != refusedMember.UID
		}
		if !h.runWithInvariant(8, rebuilt, kept) {
			h.dumpState("fresh attempt")
			t.Fatalf("no fresh member was built after the refused one was retired")
		}
		noFurtherCharge("at the fresh attempt")
		return member()
	}

	if !h.runWithInvariant(12, ended, graceKept(refusedSince)) {
		h.dumpState("past the grace")
		t.Fatalf("the attempt was never ended after the placement grace")
	}
	endedOnce()
	second := retireAndRebuild(first)

	// The fresh attempt is refused in turn, waits out its own grace with
	// nothing Failed, and is ended once by it, as the first was.
	refusedAgain := func() bool {
		pod := member()
		if pod == nil || pod.UID != second.UID {
			return false
		}
		_, _, ok := workloadtypes.PodUnschedulable(pod)
		return ok
	}
	if !h.runWithInvariant(6, refusedAgain, kept) {
		h.dumpState("fresh member with no room")
		t.Fatalf("the fresh member was never refused placement")
	}
	_, secondSince, _ := workloadtypes.PodUnschedulable(member())
	if !h.runWithInvariant(12, ended, graceKept(secondSince)) {
		h.dumpState("past the second grace")
		t.Fatalf("the fresh attempt was never ended by its own grace: the ending ran once and the member built after it is not an attempt the grace can end")
	}
	endedOnce()
	third := retireAndRebuild(second)
	if third == nil {
		t.Fatalf("no member after the second retirement")
	}

	h.podGrace = 0
	h.releasePlacement()
	if !h.run(60, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("after the room came back")
		t.Fatalf("the roll did not finish on version 2 once the member could be placed")
	}
	leaderStands()
	if len(h.heldWarnings) != 0 {
		t.Fatalf("version 2 was held for a shortage of nodes: %v", h.heldWarnings)
	}
}

// One Instance of one pod serves version 1 under SurgeThenDrain with
// maxSurge 1 and maxUnavailable 0, and the cluster has no room for one more
// pod. The operator pushes version 2. OME builds the replacement at the
// other ordinal, the scheduler refuses it, and the source keeps serving.
// Inside the placement grace nothing reads Failed. Once the grace has
// passed OME ends the attempt once: one InstanceFailed naming the
// scheduler's message, version 2 charged nothing. The refused pod is marked
// for deletion within the grace a retired replacement is given and is gone,
// and one fresh replacement is built as the next attempt, which is refused
// in turn and is not ended before its own grace has passed. Once the room
// is back the roll finishes on version 2.
func TestUnplaceableSurgePod_PastTheGraceTheReplacementIsRetiredAndRebuiltOnce(t *testing.T) {
	const noRoom = "0/10 nodes are available: no node has room for one more pod."
	h := newRecoveryHarness(t, false)
	h.atomicStatus = true
	h.recorder = record.NewFakeRecorder(256)
	maxSurge, maxUnavailable := intstr.FromInt(1), intstr.FromInt(0)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	h.unschedulableGrace = 2 * time.Minute
	h.retiredReplacementGrace = 10 * time.Second
	h.podGrace = 10 * time.Minute
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 1) }) {
		h.dumpState("initial create")
		t.Fatalf("the Instance never settled on version 1")
	}

	// sourceServes is the promise the surge makes while the room is gone:
	// the version 1 pod stays in rotation, alone.
	sourceServes := func() {
		t.Helper()
		serving := h.servingPods()
		if len(serving) != 1 || serving[0].Spec.Containers[0].Image != goodImage {
			h.dumpState("serving set")
			t.Fatalf("%d pod(s) serving while the room is gone, want the one version 1 pod", len(serving))
		}
	}
	// replacement is the live version 2 pod of the Instance, nil while none
	// exists.
	replacement := func() *corev1.Pod {
		for _, pod := range h.podsOf(0) {
			if pod.Spec.Containers[0].Image == fixedImage {
				return pod
			}
		}
		return nil
	}
	tokenHeld := func() bool {
		row := h.instance(0)
		return row != nil && row.Operation != nil && row.Operation.Waiting == workloadtypes.WaitingReasonUnschedulable
	}
	// podState reads one pod as the apiserver holds it now; a pod of
	// another UID under the same name is the fresh one, so the first is
	// gone.
	podState := func(p *corev1.Pod) (deleting, gone bool) {
		t.Helper()
		pod := &corev1.Pod{}
		err := h.c.Get(h.ctx, client.ObjectKeyFromObject(p), pod)
		if apierrors.IsNotFound(err) || (err == nil && pod.UID != p.UID) {
			return true, true
		}
		if err != nil {
			t.Fatalf("get %s: %v", p.Name, err)
		}
		return pod.DeletionTimestamp != nil, false
	}
	noCharge := func(moment string) {
		t.Helper()
		block := h.findBlock(h.revFixed.Name)
		if block != nil && (block.AttemptsStarted != 0 || block.State == workloadtypes.RetryBlockBackoff || block.State == workloadtypes.RetryBlockHeld) {
			t.Fatalf("%s: version 2 was charged for a shortage of nodes: %+v", moment, block)
		}
	}

	h.holdPlacement(noRoom)
	h.setTarget(h.revFixed, fixedImage)
	refused := func() bool {
		pod := replacement()
		if pod == nil {
			return false
		}
		_, _, ok := workloadtypes.PodUnschedulable(pod)
		return ok && tokenHeld()
	}
	if !h.runWithInvariant(12, refused, sourceServes) {
		h.dumpState("push with no room")
		t.Fatalf("the replacement was never refused placement with the wait recorded on its row")
	}
	first := replacement()
	_, refusedSince, _ := workloadtypes.PodUnschedulable(first)

	warnings := len(h.failedWarnings)
	ended := func() bool { return len(h.failedWarnings) > warnings }
	graceKept := func(since metav1.Time) func() {
		return func() {
			t.Helper()
			sourceServes()
			noCharge("inside the grace")
			if waited := h.clk.Now().Sub(since.Time); waited <= h.unschedulableGrace {
				if row := h.instance(0); row.Phase == workloadtypes.InstancePhaseFailed {
					t.Fatalf("the Instance read Failed %s into a placement grace of %s", waited, h.unschedulableGrace)
				}
			}
		}
	}
	if !h.runWithInvariant(12, ended, graceKept(refusedSince)) {
		h.dumpState("past the grace")
		t.Fatalf("the attempt was never ended after the placement grace")
	}
	if len(h.failedWarnings) != warnings+1 || !strings.Contains(h.failedWarnings[warnings], noRoom) {
		t.Fatalf("want exactly one InstanceFailed naming the scheduler's message, got %v", h.failedWarnings[warnings:])
	}
	warnings++
	noCharge("at the ending")

	// kept is what holds while the ended attempt's pod is retired and the
	// next one built: the source serves, nothing is charged, and the same
	// ended attempt raises no second InstanceFailed.
	kept := func() {
		t.Helper()
		sourceServes()
		noCharge("after the ending")
		if len(h.failedWarnings) != warnings {
			t.Fatalf("InstanceFailed was raised again for the same ended attempt: %v", h.failedWarnings[warnings:])
		}
	}
	// paced is runWithInvariant that also reads what each pass asked for:
	// between the ending and the next attempt no pass may ask to run again
	// at once or faster than the gate cadence, since one that did would
	// turn the wait for the retired pod into passes with no pause. It counts
	// the passes it took and how many of them wrote the status.
	irVersion := func() string {
		t.Helper()
		ir := &v1beta1.InferenceReplica{}
		if err := h.c.Get(h.ctx, h.irKey(), ir); err != nil {
			t.Fatalf("get the InferenceReplica: %v", err)
		}
		return ir.ResourceVersion
	}
	passes, writes := 0, 0
	paced := func(max int, pred func() bool, inv func()) bool {
		t.Helper()
		for i := 0; i < max; i++ {
			inv()
			if pred() {
				return true
			}
			before := irVersion()
			res, err := h.stepResult()
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			passes++
			if res.Requeue || (res.RequeueAfter > 0 && res.RequeueAfter < h.requeue.Gate) {
				t.Fatalf("pass %d after the ending asked to run again at once (%+v) instead of on a cadence: passes with no pause", passes, res)
			}
			if irVersion() != before {
				writes++
			}
		}
		inv()
		return pred()
	}
	deleting := func() bool { d, _ := podState(first); return d }
	if !paced(3, deleting, kept) {
		h.dumpState("after the ending")
		t.Fatalf("the refused replacement was not marked for deletion within three passes of the ending: the ended attempt's pod is kept and ended again")
	}
	gone := func() bool { _, g := podState(first); return g }
	if !paced(3, gone, kept) {
		h.dumpState("retiring the replacement")
		t.Fatalf("the retired replacement outlived the grace a retired replacement is given, %s", h.retiredReplacementGrace)
	}
	rebuilt := func() bool {
		pod := replacement()
		return pod != nil && pod.UID != first.UID
	}
	if !paced(8, rebuilt, kept) {
		h.dumpState("fresh attempt")
		t.Fatalf("no fresh replacement was built after the refused one was retired")
	}
	if passes > 4 {
		t.Fatalf("%d passes from the ending to the fresh attempt, want at most 4: the wait for the retired pod ran as passes with no pause", passes)
	}
	t.Logf("from the ending to the fresh attempt: %d pass(es), %d of them wrote the status, every pass asked to run again on the cadence", passes, writes)
	second := replacement()
	refusedAgain := func() bool {
		pod := replacement()
		if pod == nil || pod.UID != second.UID {
			return false
		}
		_, _, ok := workloadtypes.PodUnschedulable(pod)
		return ok
	}
	if !h.runWithInvariant(6, refusedAgain, kept) {
		h.dumpState("fresh replacement with no room")
		t.Fatalf("the fresh replacement was never refused placement")
	}
	_, secondSince, _ := workloadtypes.PodUnschedulable(replacement())
	// The fresh attempt is one attempt: nothing ends it before its own
	// grace, and its grace ends it once.
	if !h.runWithInvariant(12, ended, graceKept(secondSince)) {
		h.dumpState("past the second grace")
		t.Fatalf("the fresh attempt was never ended by its own grace")
	}
	if len(h.failedWarnings) != warnings+1 {
		t.Fatalf("want exactly one InstanceFailed for the fresh attempt, got %v", h.failedWarnings[warnings:])
	}
	warnings++
	if !h.runWithInvariant(3, func() bool { d, _ := podState(second); return d }, kept) {
		h.dumpState("after the second ending")
		t.Fatalf("the fresh attempt's refused replacement was not marked for deletion after its ending")
	}
	if !h.runWithInvariant(3, func() bool { _, g := podState(second); return g }, kept) {
		h.dumpState("retiring the second replacement")
		t.Fatalf("the second retired replacement outlived the grace a retired replacement is given, %s", h.retiredReplacementGrace)
	}

	h.podGrace = 0
	h.releasePlacement()
	if !h.run(60, func() bool { return h.settledOn(h.revFixed, 1) }) {
		h.dumpState("after the room came back")
		t.Fatalf("the roll did not finish on version 2 once the pod could be placed")
	}
	if len(h.heldWarnings) != 0 {
		t.Fatalf("version 2 was held for a shortage of nodes: %v", h.heldWarnings)
	}
}

// gangFleetRoll is one pass of a push to version 2 over a replica of gangs
// of one leader and seven workers, every gang Ready on version 1 with its
// eight pods serving, under SurgeThenDrain at maxSurge 10 percent: the
// pass opens a gang surge on a tenth of the gangs. The pods are read from
// a snapshot built with them, the fake client holds version 1's revision,
// the status writes are no-ops, and the surge index each source claims is
// read off its row, which the pass reserves in the snapshot.
type gangFleetRoll struct {
	deps     workloadtypes.Deps
	input    workloadtypes.ReconcileInput
	plan     workloadtypes.ComponentPlan
	target   *appsv1.ControllerRevision
	snapshot *workload.ObservedSnapshot
	decision workload.Decision
}

func newGangFleetRoll(ctx context.Context, t *testing.T, instances int) gangFleetRoll {
	t.Helper()
	const running, pushed = "svc-engine-rev-a", "svc-engine-rev-b"
	const pods = 8
	input := fleetInput(instances, instances, pods, running)
	tenPercent := intstr.FromString("10%")
	input.DesiredSpec.Lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &tenPercent},
	}
	input.ObservedState.IndexRows()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, input.DesiredSpec, input.ObservedState)
	if err != nil {
		t.Fatal(err)
	}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: pushed, Namespace: "ns"}}
	snapshot := workload.SnapshotWithPodsForTest(input, fleetPods(instances, pods))
	decision, err := workload.Plan(ctx, input, plan, target, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithObjects(revisionWithPodSpec(t, running, "ns", input.DesiredSpec.PodSpec)).Build()
	return gangFleetRoll{
		deps:     workloadtypes.Deps{Client: c},
		input:    input,
		plan:     plan,
		target:   target,
		snapshot: snapshot,
		decision: decision,
	}
}

// execute runs the decided pass: the update pass that opens the surges,
// and the create pass that follows it.
func (r gangFleetRoll) execute(ctx context.Context) error {
	_, err := workload.Execute(ctx, r.deps, r.input, r.plan, r.target, r.snapshot, r.decision)
	return err
}

// surgesOpened is the surge index each source claimed in the pass, by
// source index.
func (r gangFleetRoll) surgesOpened() map[int32]int32 {
	opened := map[int32]int32{}
	for i := range r.input.ObservedState.InstanceStatuses {
		row := &r.input.ObservedState.InstanceStatuses[i]
		if row.Operation != nil && row.Operation.SurgeIndex != nil {
			opened[row.Index] = *row.Operation.SurgeIndex
		}
	}
	return opened
}

// TestUpdatePass_SurgesOfAFleetPassTakeTheNextFreeIndexesInOrder: the 400
// gang surges one pass opens over 4,000 gangs take the 400 indexes above
// the fleet, the lowest source first: source 0 surges at 4,000, source 1
// at 4,001, source 399 at 4,399, and no other source starts. The index is
// in the replacement gang's pod names, so the sequence is part of what an
// operator sees.
func TestUpdatePass_SurgesOfAFleetPassTakeTheNextFreeIndexesInOrder(t *testing.T) {
	const instances = 4000
	ctx := logr.NewContext(context.Background(), logr.Discard())
	roll := newGangFleetRoll(ctx, t, instances)
	if err := roll.execute(ctx); err != nil {
		t.Fatal(err)
	}
	opened := roll.surgesOpened()
	if len(opened) != instances/10 {
		t.Fatalf("surges opened: got %d want %d", len(opened), instances/10)
	}
	for source := int32(0); source < instances/10; source++ {
		if got, ok := opened[source]; !ok || got != instances+source {
			t.Errorf("source %d: surge index %d (claimed %v), want %d", source, got, ok, instances+source)
		}
	}
}

// TestUpdatePassOpeningSurgesCostGrowsWithTheInstanceCount: the pass that
// opens 400 gang surges over 4,000 gangs costs about eight times the pass
// that opens 50 over 500, not sixty-four: every surge of the pass takes
// its index from one set of the taken indexes, resumed where the last
// surge took its own, instead of each surge reading every row into a set
// of its own. The cost is read as the bytes the pass allocates, which are
// the same on every machine whatever else it runs, so one ceiling gates
// everywhere; a duration moves with the load beside the test. Bytes
// rather than the allocation count, because a set read per surge is a few
// dozen allocations holding two entries per row: it barely moves the
// count and multiplies the bytes.
func TestUpdatePassOpeningSurgesCostGrowsWithTheInstanceCount(t *testing.T) {
	if testing.Short() {
		t.Skip("fleet-size gate; skipped under -short")
	}
	const small, large = 500, 4000
	// A cost that grows with the count gives a ratio of 8 between the two
	// sizes, less the pass's fixed share, and one that grows with its
	// square gives 64. The ceiling sits just above the linear reading: the
	// bytes do not move with the machine, so it needs no room for load.
	const ceiling = 10.0
	ctx := logr.NewContext(context.Background(), logr.Discard())
	allocated := map[int]uint64{}
	for _, n := range []int{small, large} {
		allocated[n] = gangFleetPassBytes(t, ctx, n)
	}
	ratio := float64(allocated[large]) / float64(allocated[small])
	t.Logf("update pass opening %d surges: %d bytes at %d gangs, %d at %d, %.2fx (ceiling %.0fx)", large/10, allocated[small], small, allocated[large], large, ratio, ceiling)
	if ratio > ceiling {
		t.Errorf("the pass over %d gangs allocates %.2fx the bytes of the pass over %d, above the ceiling of %.0fx: the cost grows faster than the count", large, ratio, small, ceiling)
	}
}

// gangFleetPassBytes is the bytes one pass allocates opening the surges of
// a roll over n gangs: the smallest of allocbudget.Samples passes, each
// over its own roll built outside the reading, since the pass reserves the
// surges it opens in the snapshot and a second pass over the same rows
// would open none. Every pass must open a surge on a tenth of the gangs.
func gangFleetPassBytes(t *testing.T, ctx context.Context, n int) uint64 {
	t.Helper()
	var best uint64
	for i := 0; i < allocbudget.Samples; i++ {
		roll := newGangFleetRoll(ctx, t, n)
		reading, err := measureCall(func() error { return roll.execute(ctx) })
		if err != nil {
			t.Fatal(err)
		}
		if opened := len(roll.surgesOpened()); opened != n/10 {
			t.Fatalf("%d gangs: the pass opened %d surges, want %d", n, opened, n/10)
		}
		if i == 0 || reading.bytes < best {
			best = reading.bytes
		}
	}
	return best
}

// TestReconcile_FailedInstanceOnItsRevisionWithARefusedSet_RollRebuildsItUnderItsOwnBudgetSlot:
// two single-pod Instances serve version 1 under RecreatePod with an
// unavailability budget of one. The operator pushes version 2 while the
// cluster has no room: Instance 0's pod is drained and deleted and the pod
// rebuilt for version 2 is refused by the scheduler. The operator then
// pushes version 1 back, so the recreate reopens at version 1 and the pod
// it rebuilds is refused as well. Once the placement grace has passed the
// attempt is ended: the row reads Failed with no operation on the revision
// it targets, with its refused pod still live. That is not a resting
// state. The next pass opens a fresh recreate at the same revision under
// the budget slot the Instance's own outage already holds, deletes the
// refused pod and rebuilds it, while Instance 1 keeps serving untouched;
// once the cluster has room again Instance 0 serves version 1. The same
// holds for a gang under InPlaceIfPossible, which every in-place strategy
// rebuilds as a recreate: an in-place patch never unbinds a pod, so a
// refused set of a promoted row comes only from a rebuild.
func TestReconcile_FailedInstanceOnItsRevisionWithARefusedSet_RollRebuildsItUnderItsOwnBudgetSlot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		gang     bool
		strategy workloadtypes.UpdateStrategyType
	}{
		{name: "single pod under RecreatePod", strategy: workloadtypes.UpdateStrategyRecreatePod},
		{name: "gang under InPlaceIfPossible", gang: true, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failedInstanceOnItsRevisionWithARefusedSetIsRebuilt(t, tc.gang, tc.strategy)
		})
	}
}

func failedInstanceOnItsRevisionWithARefusedSetIsRebuilt(t *testing.T, gang bool, strategy workloadtypes.UpdateStrategyType) {
	t.Helper()
	const noRoom = "0/4 nodes are available: 4 node(s) had untolerated taint(s)."
	const replicas = 2
	h := offTargetCrashHarness(t, gang, strategy, 0, 1, workloadtypes.RestartPolicyNone, replicas)
	h.unschedulableGrace = 2 * time.Minute

	// refusedPodOn is Instance 0's live pod on rev that the scheduler refused.
	refusedPodOn := func(rev *appsv1.ControllerRevision) *corev1.Pod {
		for _, pod := range h.podsOf(0) {
			if pod.Labels[query.LabelRevisionHash] != query.RevisionOf(rev).Hash() {
				continue
			}
			if _, _, refused := workloadtypes.PodUnschedulable(pod); refused {
				return pod
			}
		}
		return nil
	}
	// otherUntouched is the promise every step keeps: Instance 1 serves
	// version 1 and the roll stays within its budget.
	otherUntouched := func() {
		t.Helper()
		rollStaysWithinBudget(t, h, strategy, 0, 1)
		if !h.untouchedOn(1, h.revV1) || !h.instanceServes(1) {
			h.dumpState("other Instance")
			t.Fatalf("Instance 1 was taken while Instance 0 serves nothing: the Instance that is down is the one the budget allows down")
		}
	}

	h.holdPlacement(noRoom)
	h.setTarget(h.revFixed, fixedImage)
	if !h.runWithInvariant(40, func() bool { return refusedPodOn(h.revFixed) != nil }, otherUntouched) {
		h.dumpState("push with no room")
		t.Fatalf("the pod rebuilt for version 2 was never refused placement")
	}
	h.setTarget(h.revV1, goodImage)
	if !h.runWithInvariant(40, func() bool { return refusedPodOn(h.revV1) != nil }, otherUntouched) {
		h.dumpState("push back with no room")
		t.Fatalf("the pod rebuilt for version 1 was never refused placement")
	}

	endedOnItsRevision := func() bool {
		row := h.instance(0)
		return row != nil && row.Phase == workloadtypes.InstancePhaseFailed && row.Operation == nil &&
			row.RunningRevision == h.revV1.Name && refusedPodOn(h.revV1) != nil
	}
	if !h.runWithInvariant(80, endedOnItsRevision, otherUntouched) {
		h.dumpState("grace")
		t.Fatalf("the placement grace never ended the attempt on the revision the row already runs")
	}
	refused := refusedPodOn(h.revV1).UID
	reopened := func() bool {
		row := h.instance(0)
		return row != nil && row.Phase == workloadtypes.InstancePhaseUpdating && row.Operation != nil && row.Operation.Type == workloadtypes.InstanceOperationUpdate
	}
	if !h.runWithInvariant(3, reopened, otherUntouched) {
		h.dumpState("resting Failed")
		t.Fatalf("a Failed row with no operation and a live refused pod rested instead of re-opening its recreate; last hold %+v", h.lastHold())
	}
	if hold := h.lastHold(); hold != nil && hold.Gate == workloadtypes.RolloutHoldGateBudget && strings.Contains(hold.Reason, "Instance 0 on revision") {
		t.Fatalf("the start was held by the slot its own outage holds: %+v", hold)
	}

	h.releasePlacement()
	if !h.runWithInvariant(120, func() bool { return h.settledOn(h.revV1, replicas) }, otherUntouched) {
		h.dumpState("room back")
		t.Fatalf("Instance 0 never served version 1 again once the cluster had room; last hold %+v", h.lastHold())
	}
	for _, pod := range h.livePods() {
		if pod.UID == refused {
			t.Fatalf("the refused pod %s is still in the API; the recreate did not replace it", pod.Name)
		}
	}
}
