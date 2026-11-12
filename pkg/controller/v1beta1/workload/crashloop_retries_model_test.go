package workload_test

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file models the retries of a crash-looping version on the repair
// world of pass_restart_external_test.go: a push of a revision whose
// runner exits after it has served, once per set (crash-after-serving,
// the kubelet keeps it down), on and off between the kubelet's restarts
// (flap-after-serving, the shape of a runner that dies under load and
// comes back), or at every start (crash-start), under RecreatePod and
// SurgeThenDrain, under both restart policies, on single pods and gangs,
// with the retry schedule the chart ships and a short one.
//
// The world's moves beside the engine's own passes: the clock moved to
// the next timer, the corrected push, a gang member the scheduler places
// one pass after the rest of its gang (the gang scheduler's wait for every
// member, which shifts when a set comes up against the other sets'
// crashes), and a pass whose copy of the rows is one write behind: the
// pass before it wrote a row and the next pass started before the cache
// showed the write. The stale pass lands after a fixed number of passes,
// at the next promote of a rebuilt set, or at the next open of a repair.
//
// What the model reads at every pass. An Instance's set number moves only
// with an event that opens the move and names the number (a repair
// opened, the roll's recreate, a repair that follows the roll target).
// Once the crashing revision's retry record exists, a new set of that
// revision at an Instance is the open attempt's one rebuild of it, the
// restart policy's one rebuild after the set's first death, or the roll's
// own first set; a second set inside one attempt, or a set built while
// the record waits in backoff, is a rebuild the status never published.
// No Instance gets more sets of the crashing revision than the roll's
// own, the policy's and one per attempt the schedule allows. The sets a
// gang surge builds at its surge slot are the roll's own and are not
// read against a schedule. The repair
// world's own readings run beside these: no set built after the record
// holds, the record charged once per loss, no churn, and every settled
// state served or named.
//
// OME_CRASHLOOP_HUNT=full widens the configurations and the bounds;
// OME_CRASHLOOP_HUNT_ONLY=<text> keeps the configurations whose name
// carries the text.

// The images the crash-loop push points the Component at, by the tag the
// push move names.
const (
	crashTagStaysDown = "crash-after-serving"
	crashTagFlaps     = "flap-after-serving"
	crashTagAtStart   = "crash-start"
)

var crashTags = []string{crashTagStaysDown, crashTagFlaps, crashTagAtStart}

// crashLoopConfig is one configuration: the repair world's, the image the
// push points at, and whether a gang's worker is placed late.
type crashLoopConfig struct {
	repair repairConfig
	tag    string
	// lateWorker has the scheduler place every rebuilt gang's worker one
	// pass after its leader.
	lateWorker bool
}

func (c crashLoopConfig) String() string {
	s := fmt.Sprintf("%s push:%s", c.repair, c.tag)
	if c.lateWorker {
		s += " late-worker"
	}
	return s
}

// crashLoopWorld drives the repair world through the push and the passes
// one write behind.
type crashLoopWorld struct {
	*repairWorld
	cfg crashLoopConfig
}

func newCrashLoopWorld(t *testing.T, cfg crashLoopConfig) *crashLoopWorld {
	t.Helper()
	return &crashLoopWorld{repairWorld: newRepairWorld(t, cfg.repair), cfg: cfg}
}

// crashNode is one explored state: the repair world's node and what this
// model remembers across passes.
type crashNode struct {
	*repairNode
	mem crashMemory
}

// crashMemory is what the model remembers to compare two passes: each
// row's set number, the sets of the crashing revision seen per Instance
// and the attempt each was built in, the policy's one free rebuild, and
// how many events it has read.
type crashMemory struct {
	setNumbers   map[int32]int64
	targets      map[int32]string
	setsSeen     map[int32]map[int64]bool
	setsInWindow map[string]int
	freeUsed     map[int32]bool
	eventsAt     int
	crashRev     string
}

func newCrashMemory() crashMemory {
	return crashMemory{setNumbers: map[int32]int64{}, targets: map[int32]string{}, setsSeen: map[int32]map[int64]bool{}, setsInWindow: map[string]int{}, freeUsed: map[int32]bool{}}
}

func (m crashMemory) clone() crashMemory {
	out := crashMemory{setNumbers: cloneMap(m.setNumbers), targets: cloneMap(m.targets), setsSeen: map[int32]map[int64]bool{}, setsInWindow: cloneMap(m.setsInWindow),
		freeUsed: cloneMap(m.freeUsed), eventsAt: m.eventsAt, crashRev: m.crashRev}
	for idx, sets := range m.setsSeen {
		out.setsSeen[idx] = cloneMap(sets)
	}
	return out
}

// The classes this file adds. knownEarlyAttempt is the current reading of a
// pass that counts one Instance's crash on the retry record, which moves
// the record to a fresh backoff, and opens another Instance's repair on
// the copy of the record it read before the count, which moves the live
// record from that backoff to in progress at once. An attempt in progress
// whose backoff is not yet due has started early: the sets it builds read
// as built before the record came due, their deaths count nothing, and
// the Instances are rebuilt again and again instead of the record holding
// after its attempts
// (TestCrashLoopRetries_ACrashCountedBesideARepairOpenedStartsTheNextAttemptEarly).
// The other two known classes are pictures this model reports and does
// not fail on.
const (
	classMovedWithoutEvent  = "set-number-moved-without-an-opening-event"
	classSecondSetInAttempt = "second-set-inside-one-attempt"
	classSetOutsideAttempt  = "set-built-outside-an-attempt"
	classMoreSetsThanSpec   = "more-sets-than-the-schedule-allows"
	knownEarlyAttempt       = "known-attempt-started-before-its-backoff-was-due"
	knownFailedRowWaits     = "known-failed-row-waits-on-a-pod-the-kubelet-keeps"
	knownRetargetNoEvent    = "known-corrected-push-retargets-an-open-recreate-without-an-event"
)

// stalePass runs one pass whose copy of the rows and retry records is
// the one the previous pass opened on, while every write goes to the
// live store, as a pass that starts before the cache shows the previous
// pass's write does. The pods are read fresh.
func (w *crashLoopWorld) stalePass(n *crashNode, h *recoveryHarness, rows []workloadtypes.InstanceStatus, blocks []workloadtypes.RetryBlock) {
	w.t.Helper()
	h.kubelet()
	if h.routed {
		h.endpointController()
	}
	deps := workloadtypes.Deps{Client: h.cachedClient(), APIReader: h.c, Expectations: workloadtypes.NewExpectations()}
	if h.recorder != nil {
		deps.Recorder = h.recorder
	}
	in := h.buildInput()
	in.ObservedState.InstanceStatuses = cloneTestInstanceStatuses(rows)
	in.ObservedState.RetryBlocks = append([]workloadtypes.RetryBlock(nil), blocks...)
	plan, err := workload.BuildPlan(h.component, h.desired, in.ObservedState)
	if err != nil {
		w.t.Fatalf("BuildPlan on the stale copy: %v", err)
	}
	res, err := workload.Reconcile(h.ctx, deps, in, plan, h.target)
	h.drainEvents()
	h.publish(plan)
	if status.RolloutComplete(h.irStatuses(), h.target.Name) {
		h.currentRevision = h.target.Name
	}
	w.passes++
	if err != nil {
		n.found = append(n.found, repairFinding{class: "error", message: fmt.Sprintf("Reconcile on a stale copy returned an error: %v", err)})
	}
	advance := h.stepFloor
	if res.RequeueAfter > advance {
		advance = res.RequeueAfter
	}
	if h.stepCap > 0 && advance > h.stepCap {
		advance = h.stepCap
	}
	h.clk.Step(advance)
	w.observe(n.repairNode, h)
	w.watch(n, h)
}

// prepare sets what this model keeps on every harness it restores: a
// crashing runner serves two passes before it dies, long enough for the
// roll to promote it and take the next Instance while it looks healthy,
// serving for twenty seconds; and the owner-aware
// status adapter the manager always has, which the gang
// surge's take-over of an old gang writes through.
func (w *crashLoopWorld) prepare(h *recoveryHarness) *recoveryHarness {
	h.atomicStatus = true
	h.crashAfterServed = 2
	return h
}

// fresh runs one pass on the live rows and reads the state after it.
func (w *crashLoopWorld) fresh(n *crashNode, h *recoveryHarness) {
	w.t.Helper()
	w.releaseLateWorkers(h)
	w.pass(n.repairNode, h)
	w.watch(n, h)
}

// releaseLateWorkers lets a worker the scheduler refused for one pass be
// placed on the next one.
func (w *crashLoopWorld) releaseLateWorkers(h *recoveryHarness) {
	for name := range h.unplaceable {
		pod := &corev1.Pod{}
		if err := h.c.Get(h.ctx, client.ObjectKey{Namespace: recoveryNS, Name: name}, pod); err != nil {
			continue
		}
		if _, _, refused := workloadtypes.PodUnschedulable(pod); refused {
			delete(h.unplaceable, name)
		}
	}
}

// openingEvents are the events that move an Instance's set number and
// name the new number.
var openingEvents = []workloadtypes.EventReason{workloadtypes.EventReasonRestartTriggered, workloadtypes.EventReasonRecreateUpdateStarted, workloadtypes.EventReasonRestartRetargeted}

// eventOpens reports whether one of the events opens a move of Instance
// idx to set number set.
func eventOpens(events []string, component workloadtypes.ComponentType, idx int32, set int64) bool {
	key, number := workloadtypes.InstanceKey(component, idx), fmt.Sprintf("incarnation=%d)", set)
	for _, e := range events {
		if !strings.Contains(e, key) || !strings.Contains(e, number) {
			continue
		}
		for _, reason := range openingEvents {
			if strings.Contains(e, " "+string(reason)+" ") {
				return true
			}
		}
	}
	return false
}

// watch reads the state after a pass against this file's readings.
func (w *crashLoopWorld) watch(n *crashNode, h *recoveryHarness) {
	rows := h.irStatuses()
	events := h.events[n.mem.eventsAt:]
	n.mem.eventsAt = len(h.events)
	for _, s := range rows {
		prev, seen := n.mem.setNumbers[s.Index]
		prevTarget := n.mem.targets[s.Index]
		n.mem.setNumbers[s.Index], n.mem.targets[s.Index] = s.Incarnation, s.TargetRevision
		if !seen || prev < 1 || s.Incarnation <= prev {
			continue
		}
		if eventOpens(events, h.component, s.Index, s.Incarnation) {
			continue
		}
		class := classMovedWithoutEvent
		if s.Phase == workloadtypes.InstancePhaseUpdating && s.TargetRevision == h.target.Name && prevTarget != "" && prevTarget != s.TargetRevision {
			// The roll replaces its own open recreate when the target
			// moves under it, and publishes nothing for the new set.
			class = knownRetargetNoEvent
		}
		n.found = append(n.found, repairFinding{class: class,
			message: fmt.Sprintf("Instance %d moved from set %d to set %d (%s) and no event opened the move", s.Index, prev, s.Incarnation, s.Phase)})
	}
	if n.mem.crashRev == "" {
		return
	}
	w.watchSets(n, h, rows)
}

// watchSets reads every new set of the crashing revision against the
// retry record as the pass left it.
func (w *crashLoopWorld) watchSets(n *crashNode, h *recoveryHarness, rows []workloadtypes.InstanceStatus) {
	hash := query.RevisionFromName(n.mem.crashRev).Hash()
	block := h.findBlock(n.mem.crashRev)
	policy := w.cfg.repair.policy == v1beta1.InstanceRestartPolicyRecreateInstance
	byIdx := map[int32][]*corev1.Pod{}
	for _, pod := range h.livePods() {
		if pod.Labels[query.LabelRevisionHash] != hash || pod.DeletionTimestamp != nil {
			continue
		}
		if idx, ok := query.InstanceIdxFromLabels(pod); ok {
			byIdx[idx] = append(byIdx[idx], pod)
		}
	}
	indices := make([]int32, 0, len(byIdx))
	for idx := range byIdx {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	for _, idx := range indices {
		if idx >= h.replicas {
			// A surge slot, the index above the spec's replicas where the
			// gang surge builds the gang that replaces an old one, hosts
			// one set per gang surged through it; its sets are the roll's
			// own and are not read against one Instance's schedule.
			continue
		}
		for _, pod := range byIdx[idx] {
			inc, ok := query.InstanceIncarnationFromLabels(pod)
			if !ok {
				continue
			}
			sets := n.mem.setsSeen[idx]
			if sets == nil {
				sets = map[int64]bool{}
				n.mem.setsSeen[idx] = sets
			}
			if sets[inc] {
				continue
			}
			ordinal := len(sets)
			sets[inc] = true
			w.classifySet(n, h, idx, inc, ordinal, block, policy)
		}
	}
}

// classifySet reads one new set of the crashing revision at Instance idx
// against the record: the roll's own first set, the policy's one free
// rebuild, the open attempt's one rebuild, or a set no event opened.
func (w *crashLoopWorld) classifySet(n *crashNode, h *recoveryHarness, idx int32, inc int64, ordinal int, block *workloadtypes.RetryBlock, policy bool) {
	allowed := 2 + int(w.cfg.repair.knobs.tries)
	if ordinal >= allowed {
		n.found = append(n.found, repairFinding{class: classMoreSetsThanSpec,
			message: fmt.Sprintf("Instance %d got its set number %d of the crashing revision (%d sets), more than the roll's own, the policy's and the %d attempts the schedule allows", idx, inc, ordinal+1, w.cfg.repair.knobs.tries)})
	}
	if block == nil || block.FirstFailureAt == nil || ordinal == 0 {
		return
	}
	switch block.State {
	case workloadtypes.RetryBlockRetryInProgress:
		if attemptStartedEarly(block, h.clk.Now()) {
			n.found = append(n.found, repairFinding{class: knownEarlyAttempt,
				message: fmt.Sprintf("Instance %d got set number %d inside attempt %d, in progress %s before its backoff is due", idx, inc, block.AttemptsStarted, block.NextRetryAt.Sub(h.clk.Now()).Round(time.Second))})
			return
		}
		key := fmt.Sprintf("attempt-%d:%d", block.AttemptsStarted, idx)
		n.mem.setsInWindow[key]++
		if n.mem.setsInWindow[key] > 1 {
			n.found = append(n.found, repairFinding{class: classSecondSetInAttempt,
				message: fmt.Sprintf("Instance %d got a second set (number %d) inside attempt %d of the crashing revision's retry record", idx, inc, block.AttemptsStarted)})
		}
	case workloadtypes.RetryBlockBackoff:
		if policy && ordinal == 1 && !n.mem.freeUsed[idx] {
			n.mem.freeUsed[idx] = true
			return
		}
		n.found = append(n.found, repairFinding{class: classSetOutsideAttempt,
			message: fmt.Sprintf("Instance %d got set number %d of the crashing revision while its retry record waits in backoff (%d attempts)", idx, inc, block.AttemptsStarted)})
	}
}

// attemptStartedEarly reports whether the record's attempt in progress
// started before its backoff was due: a record moves to in progress only
// once its retry time has passed, so a retry time still ahead names an
// attempt that started on a copy of the record from before its count.
func attemptStartedEarly(block *workloadtypes.RetryBlock, now time.Time) bool {
	return block != nil && block.State == workloadtypes.RetryBlockRetryInProgress && block.NextRetryAt != nil && now.Before(block.NextRetryAt.Time)
}

// copyRows is the rows and records a pass opens on, kept for a pass that
// starts before the cache shows the writes of the pass after.
func (w *crashLoopWorld) copyRows(h *recoveryHarness) ([]workloadtypes.InstanceStatus, []workloadtypes.RetryBlock) {
	return cloneTestInstanceStatuses(h.irStatuses()), append([]workloadtypes.RetryBlock(nil), h.blocks...)
}

// anyEventSince reports whether one of the events carries a reason of
// reasons.
func anyEventSince(events []string, reasons ...workloadtypes.EventReason) bool {
	for _, e := range events {
		for _, reason := range reasons {
			if strings.Contains(e, " "+string(reason)+" ") {
				return true
			}
		}
	}
	return false
}

// staleAt runs fresh passes, moving the clock to the next timer whenever
// nothing changes, until a pass promotes a rebuilt or recreated set
// (what "promote") or opens a repair or a recreate (what "open"), then
// runs the pass after it on the rows the publishing pass opened on. A
// world where neither comes and no timer is left is left as it is.
func (w *crashLoopWorld) staleAt(n *crashNode, h *recoveryHarness, what string) {
	w.t.Helper()
	reasons := []workloadtypes.EventReason{workloadtypes.EventReasonRestartCompleted, workloadtypes.EventReasonRecreateUpdateCompleted}
	if what == "open" {
		reasons = []workloadtypes.EventReason{workloadtypes.EventReasonRestartTriggered, workloadtypes.EventReasonRecreateUpdateStarted}
	}
	prev := ""
	for i := 0; i < 2*w.settlePasses; i++ {
		rows, blocks := w.copyRows(h)
		before := len(h.events)
		w.fresh(n, h)
		if anyEventSince(h.events[before:], reasons...) {
			w.stalePass(n, h, rows, blocks)
			return
		}
		key := w.stableKey(h)
		if key == prev {
			timers := w.timers(h)
			if len(timers) == 0 {
				return
			}
			stepPast(h, timers[0].at)
		}
		prev = key
	}
}

// stableKey is the state the model dedups and settles on: the rows' set
// numbers, operations and revisions, the records, the pods by name, set
// number, revision and deletion, and the spec. A crashing runner the
// kubelet restarts between two passes changes none of it.
func (w *crashLoopWorld) stableKey(h *recoveryHarness) string {
	var b strings.Builder
	rows := h.irStatuses()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Index < rows[j].Index })
	for _, s := range rows {
		fmt.Fprintf(&b, "%d:%s:%s:%d", s.Index, w.short(s.RunningRevision), w.short(s.TargetRevision), s.Incarnation)
		if s.Operation != nil {
			fmt.Fprintf(&b, ":%s/%s/%d", s.Operation.Type, s.Operation.Step, s.Operation.RetryCount)
		}
		b.WriteString("|")
	}
	b.WriteString("#")
	for _, blk := range h.blocks {
		fmt.Fprintf(&b, "%s:%s:%d;", w.short(blk.TargetRevision), blk.State, blk.AttemptsStarted)
	}
	b.WriteString("#")
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		w.t.Fatalf("stable key: list pods: %v", err)
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	for i := range pods.Items {
		pod := &pods.Items[i]
		fmt.Fprintf(&b, "%s:%s:%s:%t;", strings.TrimPrefix(pod.Name, recoveryOwner+"-"), w.short(revisionNameOf(pod)), pod.Labels[query.LabelInstanceIncarnation], pod.DeletionTimestamp != nil)
	}
	fmt.Fprintf(&b, "#target=%s replicas=%d partition=%d paused=%t late=%d", w.short(h.target.Name), h.replicas, partitionOf(h), h.desired.Paused, len(h.unplaceable))
	return b.String()
}

// settle runs passes until the stable key repeats twice in a row, so a
// flapping runner's two faces are both seen; a state that never settles
// is a churn finding.
func (w *crashLoopWorld) settle(n *crashNode, h *recoveryHarness) {
	before := len(h.livePods())
	seen := []string{w.stableKey(h)}
	n.quiet = false
	for pass := 0; pass < w.settlePasses && !n.quiet; pass++ {
		w.fresh(n, h)
		seen = append(seen, w.stableKey(h))
		last := len(seen) - 1
		n.quiet = last >= 2 && seen[last] == seen[last-1] && seen[last-1] == seen[last-2]
	}
	if !n.quiet {
		n.found = append(n.found, repairFinding{class: "churn",
			message: fmt.Sprintf("the state kept changing for %d passes (%d live pods before, %d after)", w.settlePasses, before, len(h.livePods()))})
	}
}

// finish records the node as the repair world does, with this model's
// key and its known waits.
func (w *crashLoopWorld) finish(n *crashNode, h *recoveryHarness) {
	w.repairWorld.finish(n.repairNode, h)
	n.key = w.stableKey(h) + " timers=" + strings.Join(w.timerKinds(h), ",")
	w.readKnownWaits(n, h)
}

// readKnownWaits names a settled picture the repair world reads as a
// named wait and this model keeps apart: under restart policy None a row
// held Failed whose record waits in backoff past its due time while
// the kubelet keeps its pod in CrashLoopBackOff; the Create pass waits
// for a pod that never comes up and nothing deletes it. The picture is
// a known one; the model reads it and does not fail on it.
func (w *crashLoopWorld) readKnownWaits(n *crashNode, h *recoveryHarness) {
	if !n.quiet || n.mem.crashRev == "" {
		return
	}
	block := h.findBlock(n.mem.crashRev)
	if block == nil || block.State != workloadtypes.RetryBlockBackoff || block.NextRetryAt == nil || h.clk.Now().Before(block.NextRetryAt.Time) {
		return
	}
	for _, s := range h.irStatuses() {
		if s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil || s.RunningRevision != n.mem.crashRev {
			continue
		}
		for _, pod := range h.podsOf(s.Index) {
			if cs := priorContainerStatus(pod); cs != nil && cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
				n.found = append(n.found, repairFinding{class: knownFailedRowWaits,
					message: fmt.Sprintf("Instance %d is held Failed on the crashing revision, its record's retry time has passed and its pod waits in CrashLoopBackOff; nothing rebuilds it", s.Index)})
				return
			}
		}
	}
}

// moves lists what the world may do next.
func (w *crashLoopWorld) moves(n *crashNode, h *recoveryHarness, all bool) []repairMove {
	var out []repairMove
	if n.timers > 0 {
		out = append(out, repairMove{kind: "tick"}, repairMove{kind: "tick-all"})
	}
	image := h.desired.PodSpec.Containers[0].Image
	switch {
	case image == goodImage:
		out = append(out, repairMove{kind: "push", arg: w.cfg.tag})
	case image != fixedImage:
		out = append(out, repairMove{kind: "push", arg: strings.TrimPrefix(fixedImage, rollImagePrefix)})
	}
	if n.mem.crashRev != "" {
		out = append(out, w.staleMoves(all)...)
		out = append(out, w.lateMoves(all)...)
	}
	if all {
		for _, mv := range w.rollMoves(h, true) {
			switch mv.kind {
			case "resize", "partition", "pause", "resume":
				out = append(out, mv)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// staleMoves lists the passes on a copy behind the live rows: after a
// number of fresh passes, and at the promote and the open of a repair.
func (w *crashLoopWorld) staleMoves(all bool) []repairMove {
	delays := []int{0, 1, 2}
	if all {
		delays = []int{0, 1, 2, 3, 4}
	}
	var out []repairMove
	for _, after := range delays {
		out = append(out, repairMove{kind: "stale", after: after})
	}
	return append(out, repairMove{kind: "stale-at", arg: "promote"}, repairMove{kind: "stale-at", arg: "open"})
}

// lateMoves lists the gang members whose worker finds no node for a
// while, for shapes with more than one pod per Instance.
func (w *crashLoopWorld) lateMoves(all bool) []repairMove {
	if w.cfg.repair.shape.pods <= 1 || !(w.cfg.lateWorker || all) {
		return nil
	}
	late := w.cfg.repair.shape.instances
	if !all && late > 2 {
		late = 2
	}
	var out []repairMove
	for idx := int32(0); idx < late; idx++ {
		out = append(out, repairMove{kind: "late", idx: idx})
	}
	return out
}

// apply plays one move on a copy of the node and settles the engine.
func (w *crashLoopWorld) apply(n *crashNode, mv repairMove) *crashNode {
	w.t.Helper()
	h := w.prepare(w.restore(n.snap))
	child := &crashNode{repairNode: &repairNode{mem: n.repairNode.mem.clone(), path: append(append([]repairMove(nil), n.path...), mv)}, mem: n.mem.clone()}
	child.repairNode.mem.eventsAt = len(h.events)
	if child.mem.eventsAt > len(h.events) {
		child.mem.eventsAt = len(h.events)
	}
	if mv.kind != "stale" {
		for i := 0; i < mv.after; i++ {
			w.fresh(child, h)
		}
	}
	w.play(child, h, mv)
	child.repairNode.mem.roll.noteMove(mv)
	w.settle(child, h)
	w.finish(child, h)
	return child
}

// play lands one move on the harness.
func (w *crashLoopWorld) play(n *crashNode, h *recoveryHarness, mv repairMove) {
	w.t.Helper()
	switch mv.kind {
	case "stale":
		rows, blocks := w.copyRows(h)
		for i := 0; i < mv.after; i++ {
			rows, blocks = w.copyRows(h)
			w.fresh(n, h)
		}
		w.stalePass(n, h, rows, blocks)
	case "stale-at":
		w.staleAt(n, h, mv.arg)
	case "late":
		if h.unplaceable == nil {
			h.unplaceable = map[string]bool{}
		}
		h.unplaceable[query.PodName(recoveryOwner, h.component, mv.idx, workloadtypes.RunnerWorker, 0)] = true
	case "push":
		w.repairWorld.play(n.repairNode, h, mv)
		for _, tag := range crashTags {
			if mv.arg == tag {
				n.mem.crashRev = h.target.Name
			}
		}
	default:
		w.repairWorld.play(n.repairNode, h, mv)
	}
}

// start is the idle Component on its first revision, with this model's
// memory primed on it.
func (w *crashLoopWorld) start() *crashNode {
	n := &crashNode{repairNode: w.repairWorld.start(), mem: newCrashMemory()}
	h := w.prepare(w.restore(n.snap))
	for _, s := range h.irStatuses() {
		n.mem.setNumbers[s.Index], n.mem.targets[s.Index] = s.Incarnation, s.TargetRevision
	}
	n.mem.eventsAt = len(h.events)
	n.key = w.stableKey(h) + " timers=" + strings.Join(w.timerKinds(h), ",")
	return n
}

// explore walks the configuration breadth-first to the bounds.
func (w *crashLoopWorld) explore(maxStates, maxDepth, passBudget int) repairReport {
	rep := newRepairReport()
	root := w.start()
	spent := w.passes
	rep.take(root.repairNode)
	visited := map[string]bool{root.key: true}
	queue := []*crashNode{root}
	for len(queue) > 0 && rep.states < maxStates && w.passes-spent < passBudget {
		n := queue[0]
		queue = queue[1:]
		rep.states++
		if len(n.path) > rep.depth {
			rep.depth = len(n.path)
		}
		if len(n.path) >= maxDepth {
			continue
		}
		h := w.prepare(w.restore(n.snap))
		for _, mv := range w.moves(n, h, false) {
			child := w.apply(n, mv)
			rep.take(child.repairNode)
			if visited[child.key] {
				continue
			}
			visited[child.key] = true
			queue = append(queue, child)
		}
		n.snap = nil
	}
	return rep
}

// randomRuns plays whole runs from the start with a fixed seed, drawing
// from every move, to reach the depths the walk does not.
func (w *crashLoopWorld) randomRuns(rep *repairReport, seed int64, runs, maxDepth, passBudget int) {
	rng := rand.New(rand.NewSource(seed))
	spent := w.passes
	for r := 0; r < runs && w.passes-spent < passBudget; r++ {
		n := w.start()
		rep.runs++
		for depth := 0; depth < maxDepth && w.passes-spent < passBudget; depth++ {
			h := w.prepare(w.restore(n.snap))
			moves := w.moves(n, h, true)
			if len(moves) == 0 {
				break
			}
			n = w.apply(n, moves[rng.Intn(len(moves))])
			rep.take(n.repairNode)
			if len(n.path) > rep.depth {
				rep.depth = len(n.path)
			}
		}
	}
}

// crashLoopConfigs lists the configurations: the reference shapes
// first (two gangs of two under RecreatePod and policy None, the
// runner coming back between crashes; two gangs of two under
// SurgeThenDrain rebuilt by the policy), then the other images, policies
// and strategies, and in full mode a seeded sample over the whole space.
func crashLoopConfigs(full bool) []crashLoopConfig {
	chart := repairKnobs{deadlines: "chart", tries: 3}
	short := repairKnobs{deadlines: "short", tries: 2, shortDelays: true, forceDelete: true}
	recreate, none := v1beta1.InstanceRestartPolicyRecreateInstance, v1beta1.InstanceRestartPolicyNone
	recreatePod, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	cfg := func(shape repairShape, policy v1beta1.InstanceRestartPolicy, knobs repairKnobs, strategy workloadtypes.UpdateStrategyType, tag string, late bool) crashLoopConfig {
		return crashLoopConfig{repair: repairConfig{shape: shape, policy: policy, knobs: knobs, story: storyPushCrashLoop, strategy: strategy}, tag: tag, lateWorker: late}
	}
	out := []crashLoopConfig{
		cfg(repairShape{2, 2}, none, chart, recreatePod, crashTagFlaps, false),
		cfg(repairShape{2, 2}, none, chart, recreatePod, crashTagFlaps, true),
		cfg(repairShape{2, 2}, recreate, chart, surge, crashTagStaysDown, false),
		cfg(repairShape{2, 1}, recreate, chart, recreatePod, crashTagFlaps, false),
		cfg(repairShape{2, 1}, none, chart, recreatePod, crashTagFlaps, false),
		cfg(repairShape{2, 1}, none, chart, recreatePod, crashTagStaysDown, false),
		cfg(repairShape{3, 1}, recreate, short, recreatePod, crashTagStaysDown, false),
		cfg(repairShape{4, 2}, none, short, surge, crashTagFlaps, false),
		cfg(repairShape{2, 2}, none, chart, recreatePod, crashTagAtStart, false),
		cfg(repairShape{4, 1}, recreate, chart, surge, crashTagAtStart, false),
		cfg(repairShape{3, 2}, recreate, chart, recreatePod, crashTagFlaps, true),
	}
	if !full {
		return out
	}
	shapes := []repairShape{{4, 1}, {8, 1}, {12, 1}, {16, 1}, {4, 2}, {8, 2}, {4, 4}, {2, 8}, {4, 8}, {3, 3}}
	rng := rand.New(rand.NewSource(20261110))
	for k := 0; k < 36; k++ {
		shape := shapes[rng.Intn(len(shapes))]
		policy := recreate
		if rng.Intn(2) == 0 {
			policy = none
		}
		knobs := chart
		if rng.Intn(3) == 0 {
			knobs = short
		}
		strategy := recreatePod
		if rng.Intn(2) == 0 {
			strategy = surge
		}
		tag := crashTags[rng.Intn(len(crashTags))]
		if policy == none && tag == crashTagStaysDown {
			tag = crashTagFlaps
		}
		out = append(out, cfg(shape, policy, knobs, strategy, tag, shape.pods > 1 && rng.Intn(2) == 0))
	}
	return out
}

// The classes that fail the test: this file's, and the repair world's.
var crashLoopFailingClasses = map[string]bool{
	classMovedWithoutEvent: true, classSecondSetInAttempt: true, classSetOutsideAttempt: true, classMoreSetsThanSpec: true,
	"no-way-out": true, "rebuild-never-comes": true, "rebuild-without-record": true, "rebuilt-after-held": true, "gang-member-alone": true,
	"churn": true, "error": true, "failed-never-starts-again": true, "repair-never-starts": true, "wait-never-ends": true, "attempt-never-ends": true, "charged-twice": true,
}

// TestCrashLoopRetries_EveryRebuildOfACrashingVersionHasAnOpeningEvent explores
// every configuration and fails on a set of the crashing revision no
// event opened, on more sets than the schedule allows, and on
// the repair world's own failing classes.
func TestCrashLoopRetries_EveryRebuildOfACrashingVersionHasAnOpeningEvent(t *testing.T) {
	t.Parallel()
	full := os.Getenv("OME_CRASHLOOP_HUNT") == "full"
	maxStates, maxDepth, runs, runDepth, passBudget := 40, 4, 3, 16, 2400
	if full {
		maxStates, maxDepth, runs, runDepth, passBudget = 120, 6, 6, 28, 16000
	}
	only := os.Getenv("OME_CRASHLOOP_HUNT_ONLY")
	var configs []crashLoopConfig
	for _, c := range crashLoopConfigs(full) {
		if only == "" || strings.Contains(c.String(), only) {
			configs = append(configs, c)
		}
	}
	type explored struct {
		rep    repairReport
		passes int
	}
	results := make([]explored, len(configs))
	started := time.Now()
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(fmt.Sprintf("%03d", i+1), func(t *testing.T) {
				t.Parallel()
				rep, passes := huntCrashLoopConfig(t, cfg, int64(i+1), maxStates, maxDepth, runs, runDepth, passBudget)
				results[i] = explored{rep: rep, passes: passes}
			})
		}
	})
	totals := map[string]int{}
	type example struct {
		cfg     crashLoopConfig
		finding repairFinding
	}
	examples := map[string][]example{}
	var states, passes, allRuns int
	for i, cfg := range configs {
		rep := results[i].rep
		states += rep.states
		passes += results[i].passes
		allRuns += rep.runs
		for class, n := range rep.counts {
			totals[class] += n
		}
		for class, f := range rep.findings {
			if len(examples[class]) < 8 {
				examples[class] = append(examples[class], example{cfg: cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d seeded runs, %d engine passes, %s", len(configs), states, allRuns, passes, time.Since(started).Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d states across %d configurations", class, totals[class], len(examples[class]))
		if strings.HasPrefix(class, "named-wait") || strings.HasPrefix(class, "known-") {
			continue
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state:\n      %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.picture)
		}
		if crashLoopFailingClasses[class] {
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
		}
	}
}

// huntCrashLoopConfig explores one configuration from its root, then
// drives its seeded random runs, and logs the classes it met.
func huntCrashLoopConfig(t *testing.T, cfg crashLoopConfig, seed int64, maxStates, maxDepth, runs, runDepth, passBudget int) (repairReport, int) {
	w := newCrashLoopWorld(t, cfg)
	rep := w.explore(maxStates, maxDepth, passBudget*2/3)
	w.randomRuns(&rep, seed, runs, runDepth, passBudget/3)
	t.Logf("[%s]: %d states, depth %d, %d runs, %d passes: %s", cfg, rep.states, rep.depth, rep.runs, w.passes, classCounts(rep.counts))
	return rep, w.passes
}

// classCounts renders a class count map as "class=n" pairs in class order.
func classCounts(counts map[string]int) string {
	parts := make([]string, 0, len(counts))
	for class, c := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", class, c))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// runUntilRepairEvent runs fresh passes, moving the clock to the next
// timer whenever nothing changes, until a pass publishes the reason for
// an Instance; it returns that Instance and the rows and records the
// publishing pass opened on.
func (w *crashLoopWorld) runUntilRepairEvent(n *crashNode, h *recoveryHarness, reason string) (int32, []workloadtypes.InstanceStatus, []workloadtypes.RetryBlock) {
	w.t.Helper()
	prev := ""
	for i := 0; i < 8*w.settlePasses; i++ {
		rows, blocks := w.copyRows(h)
		before := len(h.events)
		w.fresh(n, h)
		for _, e := range h.events[before:] {
			if !strings.Contains(e, " "+reason+" ") {
				continue
			}
			for _, s := range rows {
				if strings.Contains(e, workloadtypes.InstanceKey(h.component, s.Index)) {
					return s.Index, rows, blocks
				}
			}
		}
		key := w.stableKey(h)
		if key == prev {
			if timers := w.timers(h); len(timers) > 0 {
				stepPast(h, timers[0].at)
			}
		}
		prev = key
	}
	w.t.Fatalf("[%s] no pass published %s; picture:\n%s", w.cfg, reason, w.picture(h))
	return 0, nil, nil
}

// crashLoopLockConfigs are the reference shapes for the lock tests, all
// under RecreatePod: two gangs of two under policy None with the runner
// coming back between crashes, two gangs of two rebuilt by the policy
// with the runner staying down, and the single-pod twins of both.
func crashLoopLockConfigs() []crashLoopConfig {
	chart := repairKnobs{deadlines: "chart", tries: 3}
	none, recreate := v1beta1.InstanceRestartPolicyNone, v1beta1.InstanceRestartPolicyRecreateInstance
	cfg := func(shape repairShape, policy v1beta1.InstanceRestartPolicy, strategy workloadtypes.UpdateStrategyType, tag string) crashLoopConfig {
		return crashLoopConfig{repair: repairConfig{shape: shape, policy: policy, knobs: chart, story: storyPushCrashLoop, strategy: strategy}, tag: tag}
	}
	return []crashLoopConfig{
		cfg(repairShape{2, 2}, none, workloadtypes.UpdateStrategyRecreatePod, crashTagFlaps),
		cfg(repairShape{2, 2}, recreate, workloadtypes.UpdateStrategyRecreatePod, crashTagStaysDown),
		cfg(repairShape{2, 1}, none, workloadtypes.UpdateStrategyRecreatePod, crashTagFlaps),
		cfg(repairShape{2, 1}, recreate, workloadtypes.UpdateStrategyRecreatePod, crashTagStaysDown),
	}
}

// TestCrashLoopRetries_APassOneWriteBehindThePromoteLeavesThePromotedSet:
// two Instances are pushed to a revision whose runner exits after it has
// served. The retry schedule counts the crash, waits its backoff and
// rebuilds the Instance; the rebuilt set comes up and the pass promotes
// it (RestartCompleted). The next pass starts before the cache shows that
// write, so its copy of the row still reads Restarting while the live row
// is Ready. That pass opens nothing: the row keeps its set number and
// stays Ready, no RestartTriggered is published, and the promoted set is
// neither deleted nor built again; the crash loop goes on under the
// schedule's own events.
func TestCrashLoopRetries_APassOneWriteBehindThePromoteLeavesThePromotedSet(t *testing.T) {
	for _, cfg := range crashLoopLockConfigs() {
		t.Run(cfg.String(), func(t *testing.T) {
			w := newCrashLoopWorld(t, cfg)
			n := w.start()
			h := w.prepare(w.restore(n.snap))
			w.play(n, h, repairMove{kind: "push", arg: cfg.tag})
			idx, rows, blocks := w.runUntilRepairEvent(n, h, string(workloadtypes.EventReasonRestartCompleted))
			promoted := h.instance(idx)
			if promoted == nil || promoted.Phase != workloadtypes.InstancePhaseReady || promoted.Operation != nil {
				t.Fatalf("Instance %d should be Ready with nothing open after its repair completed; picture:\n%s", idx, w.picture(h))
			}
			set, triggered, pods := promoted.Incarnation, eventsNaming(h, workloadtypes.EventReasonRestartTriggered, idx), len(h.podsOf(idx))
			w.stalePass(n, h, rows, blocks)
			if got := eventsNaming(h, workloadtypes.EventReasonRestartTriggered, idx); got != triggered {
				t.Errorf("a pass behind the promote published %d RestartTriggered for Instance %d, want none", got-triggered, idx)
			}
			leftAlone(t, w, h, idx, set, pods)
			w.settle(n, h)
			noFailingClass(t, n, "after the pass behind the promote")
		})
	}
}

// leftAlone checks the picture after a pass behind the promote of
// Instance idx: the row keeps its set number and reads Ready with nothing
// open, and the promoted set of pods pods has none above set and none
// being deleted.
func leftAlone(t *testing.T, w *crashLoopWorld, h *recoveryHarness, idx int32, set int64, pods int) {
	t.Helper()
	after := h.instance(idx)
	if after == nil {
		t.Fatalf("Instance %d lost its row; picture:\n%s", idx, w.picture(h))
	}
	if after.Incarnation != set || after.Phase != workloadtypes.InstancePhaseReady || after.Operation != nil {
		t.Errorf("a pass behind the promote must leave Instance %d Ready at set %d; got set %d, %s; picture:\n%s", idx, set, after.Incarnation, after.Phase, w.picture(h))
	}
	if rebuilt, deleting := podsAboveSet(h, idx, set), podsDeleting(h, idx); rebuilt != 0 || deleting != 0 {
		t.Errorf("a pass behind the promote must keep the promoted set of Instance %d (%d pods): %d pods above set %d, %d being deleted; picture:\n%s", idx, pods, rebuilt, set, deleting, w.picture(h))
	}
}

// noFailingClass fails on every failing class the node found, naming
// the moment what.
func noFailingClass(t *testing.T, n *crashNode, what string) {
	t.Helper()
	for _, f := range n.found {
		if crashLoopFailingClasses[f.class] {
			t.Errorf("%s the model found %s: %s", what, f.class, f.message)
		}
	}
}

// TestCrashLoopRetries_APassOneWriteBehindARollsPromoteLeavesTheRolledSet:
// the Instances are pushed to the crashing revision and then to a
// corrected one under RecreatePod; the recreate of an Instance comes up
// and the pass promotes it (RecreateUpdateCompleted). The next pass
// starts on a copy of the row from before that write, so its copy still
// reads Updating at the drain step while the live row is Ready. That
// pass opens nothing: the row keeps its set number and stays Ready on
// the corrected revision, no RecreateUpdateStarted is published, and the
// promoted set is neither deleted nor built again.
func TestCrashLoopRetries_APassOneWriteBehindARollsPromoteLeavesTheRolledSet(t *testing.T) {
	for _, cfg := range crashLoopLockConfigs() {
		if cfg.repair.strategy != workloadtypes.UpdateStrategyRecreatePod {
			continue
		}
		t.Run(cfg.String(), func(t *testing.T) {
			w := newCrashLoopWorld(t, cfg)
			n := w.start()
			h := w.prepare(w.restore(n.snap))
			w.play(n, h, repairMove{kind: "push", arg: cfg.tag})
			w.settle(n, h)
			w.play(n, h, repairMove{kind: "push", arg: strings.TrimPrefix(fixedImage, rollImagePrefix)})
			idx, rows, blocks := w.runUntilRepairEvent(n, h, string(workloadtypes.EventReasonRecreateUpdateCompleted))
			promoted := h.instance(idx)
			if promoted == nil || promoted.Phase != workloadtypes.InstancePhaseReady || promoted.Operation != nil || promoted.RunningRevision != h.target.Name {
				t.Fatalf("Instance %d should be Ready on the corrected revision after its recreate completed; picture:\n%s", idx, w.picture(h))
			}
			set, started, pods := promoted.Incarnation, eventsNaming(h, workloadtypes.EventReasonRecreateUpdateStarted, idx), len(h.podsOf(idx))
			w.stalePass(n, h, rows, blocks)
			if got := eventsNaming(h, workloadtypes.EventReasonRecreateUpdateStarted, idx); got != started {
				t.Errorf("a pass behind the promote published %d RecreateUpdateStarted for Instance %d, want none", got-started, idx)
			}
			leftAlone(t, w, h, idx, set, pods)
			if after := h.instance(idx); after != nil && after.RunningRevision != h.target.Name {
				t.Errorf("a pass behind the promote must leave Instance %d on the corrected revision; it runs %s", idx, after.RunningRevision)
			}
			w.settle(n, h)
			if rebuilt := podsAboveSet(h, idx, set); rebuilt != 0 {
				t.Errorf("the corrected revision does not crash, yet Instance %d got %d pods above set %d after the pass behind the promote; picture:\n%s", idx, rebuilt, set, w.picture(h))
			}
			noFailingClass(t, n, "after the pass behind the promote")
		})
	}
}

// TestCrashLoopRetries_ACrashCountedBesideARepairOpenedStartsTheNextAttemptEarly
// holds the current behaviour on the shapes the policy
// rebuilds. After a repair's promote and the pass behind it, the two
// Instances' sets die close together: one pass counts the first crash on
// the retry record, which moves it to a fresh backoff, and opens the
// second Instance's repair on its copy of the record from before the
// count, which moves the live record from that backoff to in progress
// with its retry time still ahead. The right reading is that no attempt
// starts before its backoff is due; the expectation flips with the behaviour.
func TestCrashLoopRetries_ACrashCountedBesideARepairOpenedStartsTheNextAttemptEarly(t *testing.T) {
	for _, cfg := range crashLoopLockConfigs() {
		if cfg.repair.policy != v1beta1.InstanceRestartPolicyRecreateInstance {
			continue
		}
		t.Run(cfg.String(), func(t *testing.T) {
			w := newCrashLoopWorld(t, cfg)
			n := w.start()
			h := w.prepare(w.restore(n.snap))
			w.play(n, h, repairMove{kind: "push", arg: cfg.tag})
			_, rows, blocks := w.runUntilRepairEvent(n, h, string(workloadtypes.EventReasonRestartCompleted))
			w.stalePass(n, h, rows, blocks)
			left, early := w.runUntilEarlyAttemptOrHeld(n, h)
			if !early {
				t.Fatalf("current reading: the record reads in progress before its retry time; it never did; picture:\n%s", w.picture(h))
			}
			t.Logf("the record reads in progress %s before its retry time", left)
		})
	}
}

// runUntilEarlyAttemptOrHeld runs fresh passes, moving the clock to the
// next timer whenever nothing changes, until the crashing revision's
// record reads in progress with its retry time still ahead (returned with
// the time left), or reads Held, or the pass budget ends.
func (w *crashLoopWorld) runUntilEarlyAttemptOrHeld(n *crashNode, h *recoveryHarness) (time.Duration, bool) {
	w.t.Helper()
	prev := ""
	for i := 0; i < 12*w.settlePasses; i++ {
		w.fresh(n, h)
		block := h.findBlock(n.mem.crashRev)
		if attemptStartedEarly(block, h.clk.Now()) {
			return block.NextRetryAt.Sub(h.clk.Now()).Round(time.Second), true
		}
		if block != nil && block.State == workloadtypes.RetryBlockHeld {
			return 0, false
		}
		key := w.stableKey(h)
		if key == prev {
			if timers := w.timers(h); len(timers) > 0 {
				stepPast(h, timers[0].at)
			}
		}
		prev = key
	}
	return 0, false
}

// podsAboveSet counts the Instance's live pods labelled with a set number
// above the given one.
func podsAboveSet(h *recoveryHarness, idx int32, set int64) int {
	rebuilt := 0
	for _, pod := range h.podsOf(idx) {
		if inc, ok := query.InstanceIncarnationFromLabels(pod); ok && inc > set {
			rebuilt++
		}
	}
	return rebuilt
}

// podsDeleting counts the Instance's pods with a deletion timestamp.
func podsDeleting(h *recoveryHarness, idx int32) int {
	deleting := 0
	for _, pod := range h.podsOf(idx) {
		if pod.DeletionTimestamp != nil {
			deleting++
		}
	}
	return deleting
}

// TestCrashLoopRetries_APassOneWriteBehindTheOpenKeepsTheAttempt: the pass
// after a repair opens reads a copy from before the open, so its copy
// says Ready with the crash remembered while the live row is Restarting.
// The attempt goes on at the set number the open gave it: no second set
// number, no second set.
func TestCrashLoopRetries_APassOneWriteBehindTheOpenKeepsTheAttempt(t *testing.T) {
	for _, cfg := range crashLoopLockConfigs() {
		t.Run(cfg.String(), func(t *testing.T) {
			w := newCrashLoopWorld(t, cfg)
			n := w.start()
			h := w.prepare(w.restore(n.snap))
			w.play(n, h, repairMove{kind: "push", arg: cfg.tag})
			idx, rows, blocks := w.runUntilRepairEvent(n, h, string(workloadtypes.EventReasonRestartTriggered))
			opened := h.instance(idx)
			if opened == nil || opened.Phase != workloadtypes.InstancePhaseRestarting {
				t.Fatalf("Instance %d should be Restarting after its repair opened; picture:\n%s", idx, w.picture(h))
			}
			set := opened.Incarnation
			w.stalePass(n, h, rows, blocks)
			w.settle(n, h)
			after := h.instance(idx)
			if after == nil || after.Incarnation != set {
				t.Fatalf("the attempt must go on at set %d; got %+v; picture:\n%s", set, after, w.picture(h))
			}
			noFailingClass(t, n, "the pass behind the open must leave the attempt as it is;")
		})
	}
}
