package workload_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file widens the repair model of pass_restart_external_test.go from
// an idle Component to a roll in progress and to the operator's actions
// beside a fault. A configuration names the moment the faults land at:
// the roll idle; a push of a sound revision settling (the first attempt
// opened, nothing promoted yet), rotating (some Instances promoted, one
// attempt open, some still on the old version), draining (an attempt's
// old pods are leaving) or at its last Instance; a push whose retries are
// spent, the old Instances serving on; a canary step (a partition keeps
// every Instance but one on the old version); a roll paused mid-way. Each
// runs under RecreatePod or SurgeThenDrain at 10 percent, for gangs of
// one to eight pods. The faults are the idle model's, plus a pod the
// kubelet refused after placement. The operator's moves are a push back
// to the old version, a push forward, a resize by one Instance each way,
// a partition set or cleared, a pause and a resume.
//
// Two checks are added at every state. A revision's retry record is
// charged at most once per loss: a loss is an attempt that ended or a
// failure newly recorded on a row, counted along the path, and the
// record's attempts never run ahead of the losses. Once no fault stands
// in the world and nothing is left to wait for, every Instance serves
// whole unless its revision's retries are spent, which only the operator
// ends: a Failed row that never starts again, a repair that never opens
// while nothing holds it, an attempt or a wait that never ends each fail
// the test.

// The moments of a roll a configuration starts at.
const (
	momentIdle     = "idle"
	momentSettling = "settling"
	momentRotating = "rotating"
	momentDraining = "draining"
	momentLast     = "last"
	momentHeld     = "held"
	momentCanary   = "canary"
	momentPaused   = "paused"
)

var rollMoments = []string{momentIdle, momentSettling, momentRotating, momentDraining, momentLast, momentHeld, momentCanary, momentPaused}

// The stories this file adds to the idle model's.
const (
	storyRejected = "pod-rejected"
	storyOperator = "operator-actions"
)

// rollImagePrefix is the registry every image of the model is named under;
// a push move names the tag alone.
const rollImagePrefix = "registry.test/serving:"

// rollingUpdateFor is the Component's rolling update under strategy at a
// budget of 10 percent, rounded up: RecreatePod
// takes one Instance in ten out of serving and leaves the surge budget
// unset, SurgeThenDrain builds one in ten beside the fleet and leaves the
// unavailability budget unset. A partition above zero keeps that many
// Instances on the old version.
func rollingUpdateFor(strategy workloadtypes.UpdateStrategyType, partition int32) *workloadtypes.RollingUpdate {
	tenth := intstr.FromString("10%")
	rolling := &workloadtypes.RollingUpdate{MaxUnavailable: &tenth}
	if strategy == workloadtypes.UpdateStrategySurgeThenDrain {
		rolling = &workloadtypes.RollingUpdate{MaxSurge: &tenth}
	}
	if partition > 0 {
		rolling.Partition = &partition
	}
	return rolling
}

// rollMemory is what a node remembers for the checks of this file: the
// world-level faults that stand until their clearing move, whether the
// registry has the broken image again, and the retry-record accounting.
type rollMemory struct {
	faults    map[string]bool
	imageBack bool
	charges   rollCharges
}

func newRollMemory() rollMemory {
	return rollMemory{faults: map[string]bool{}, charges: newRollCharges()}
}

func (m rollMemory) clone() rollMemory {
	return rollMemory{faults: cloneMap(m.faults), imageBack: m.imageBack, charges: m.charges.clone()}
}

// noteMove records which world-level fault a move raised or cleared. A
// fault that hits one pod (an exit, a delete, an eviction, a refusal, one
// pod the kubelet cannot start) is over once the engine rebuilds the pod,
// so it is not recorded; the broken images are read off the target.
func (m *rollMemory) noteMove(mv repairMove) {
	if m.faults == nil {
		m.faults = map[string]bool{}
	}
	switch {
	case mv.kind == "node-lost":
		m.faults["node"] = true
	case mv.kind == "node-back":
		delete(m.faults, "node")
	case mv.kind == "kubelet-back":
		delete(m.faults, "kubelet")
	case strings.HasPrefix(mv.kind, "kubelet-"):
		m.faults["kubelet"] = true
	case mv.kind == "no-room":
		m.faults["room"] = true
	case mv.kind == "room-back":
		delete(m.faults, "room")
	case mv.kind == "webhook-down":
		m.faults["webhook"] = true
	case mv.kind == "webhook-back":
		delete(m.faults, "webhook")
	case mv.kind == "waiting-all":
		m.faults["start"] = true
	case mv.kind == "recover-waiting":
		delete(m.faults, "start")
	case mv.kind == "image-back":
		m.imageBack = true
	}
}

// faultStanding reports whether something in the world still stops an
// Instance from serving by design: a lost node or kubelet, no room, the
// admission webhook down, every new pod held by the kubelet, a target
// image that cannot run (until the registry has the broken image again
// or the operator pushes a sound one), or the operator's pause.
func (w *repairWorld) faultStanding(n *repairNode, h *recoveryHarness) bool {
	if len(n.mem.roll.faults) > 0 || h.desired.Paused {
		return true
	}
	image := h.desired.PodSpec.Containers[0].Image
	switch image {
	case goodImage, fixedImage:
		return false
	case badImage:
		return !n.mem.roll.imageBack
	}
	return true
}

// start is the state a configuration's exploration begins at: the idle
// Component, or the Component at the configuration's moment of a roll.
func (w *repairWorld) start() *repairNode {
	if w.startNode == nil {
		if w.cfg.moment == "" || w.cfg.moment == momentIdle {
			w.startNode = w.root()
		} else {
			w.startNode = w.rootAt(w.cfg.moment)
		}
	}
	n := *w.startNode
	n.mem = w.startNode.mem.clone()
	n.found = append([]repairFinding(nil), w.startNode.found...)
	return &n
}

// rootAt plays a push on the idle Component and runs the engine to the
// named moment of the roll, where the exploration's faults then land.
func (w *repairWorld) rootAt(moment string) *repairNode {
	w.t.Helper()
	idle := w.root()
	h := w.restore(idle.snap)
	n := &repairNode{mem: idle.mem.clone()}
	forward := repairMove{kind: "push", arg: strings.TrimPrefix(fixedImage, rollImagePrefix)}
	switch moment {
	case momentHeld:
		broken := repairMove{kind: "push", arg: strings.TrimPrefix(badImage, rollImagePrefix)}
		w.play(n, h, broken)
		n.mem.roll.noteMove(broken)
		w.runUntil(n, h, moment, func() bool {
			b := h.findBlock(h.target.Name)
			return b != nil && b.State == workloadtypes.RetryBlockHeld
		}, true)
	case momentCanary:
		w.setPartition(h, h.replicas-1)
		w.play(n, h, forward)
		w.runUntil(n, h, moment, func() bool { return w.rollReached(h, moment) }, false)
		w.settle(n, h)
	case momentPaused:
		w.play(n, h, forward)
		w.runUntil(n, h, moment, func() bool { return w.rollReached(h, momentRotating) || w.rollReached(h, momentSettling) }, false)
		h.desired.Paused = true
		w.settle(n, h)
	default:
		w.play(n, h, forward)
		w.runUntil(n, h, moment, func() bool { return w.rollReached(h, moment) }, false)
	}
	w.finish(n, h)
	return n
}

// runUntil runs passes until reached holds, up to the settle bound, and
// with tickTimers moves the clock to the next timer whenever the state
// stops changing on its own. A moment the roll never reaches is a wrong
// configuration, not a finding.
func (w *repairWorld) runUntil(n *repairNode, h *recoveryHarness, moment string, reached func() bool, tickTimers bool) {
	w.t.Helper()
	prev := ""
	for i := 0; i < 4*w.settlePasses; i++ {
		w.pass(n, h)
		if reached() {
			return
		}
		key := w.key(h)
		if tickTimers && key == prev {
			if timers := w.timers(h); len(timers) > 0 {
				stepPast(h, timers[0].at)
			}
		}
		prev = key
	}
	w.t.Fatalf("[%s] the roll never reached the %s moment; picture:\n%s", w.cfg, moment, w.picture(h))
}

// rollReached reads the moment of the roll off the rows: how many
// Instances are promoted on the target, how many have an attempt open,
// how many still serve the old version untouched, and whether an attempt
// is deleting its old pods. The last moment is the roll's last batch in
// flight with no untouched Instance left, one Instance or more wide as
// the budget allows. The canary step stands once every Instance the
// partition lets move is promoted and nothing is open.
func (w *repairWorld) rollReached(h *recoveryHarness, moment string) bool {
	rows := h.irStatuses()
	var promoted, open, old int32
	draining := false
	for _, s := range rows {
		switch {
		case s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationUpdate:
			open++
			for _, pod := range podsOfEveryPhase(h, s.Index) {
				if pod.DeletionTimestamp != nil {
					draining = true
				}
			}
		case s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision == h.target.Name:
			promoted++
		case s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil:
			old++
		}
	}
	switch moment {
	case momentSettling:
		return open > 0 && promoted == 0
	case momentRotating:
		return open > 0 && promoted > 0 && old > 0
	case momentDraining:
		return draining
	case momentLast:
		return open > 0 && old == 0 && promoted > 0
	case momentCanary:
		return open == 0 && promoted > 0 && promoted >= h.replicas-partitionOf(h)
	}
	return false
}

// podsOfEveryPhase is every pod object of Instance idx, the ones being
// deleted included.
func podsOfEveryPhase(h *recoveryHarness, idx int32) []*corev1.Pod {
	h.t.Helper()
	list := &corev1.PodList{}
	if err := h.c.List(h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	var out []*corev1.Pod
	for i := range list.Items {
		if i2, ok := query.InstanceIdxFromLabels(&list.Items[i]); ok && i2 == idx {
			out = append(out, &list.Items[i])
		}
	}
	return out
}

// partitionOf is the partition the Component's rolling update holds, zero
// for none.
func partitionOf(h *recoveryHarness) int32 {
	strategy := h.lifecycle.UpdateStrategy
	if strategy == nil || strategy.RollingUpdate == nil || strategy.RollingUpdate.Partition == nil {
		return 0
	}
	return *strategy.RollingUpdate.Partition
}

// setPartition writes a partition into a copy of the Component's update
// strategy, so the branches that share the earlier strategy keep it, and
// renders the spec again.
func (w *repairWorld) setPartition(h *recoveryHarness, partition int32) {
	strategy := *h.lifecycle.UpdateStrategy
	rolling := workloadtypes.RollingUpdate{}
	if strategy.RollingUpdate != nil {
		rolling = *strategy.RollingUpdate
	}
	rolling.Partition = nil
	if partition > 0 {
		rolling.Partition = &partition
	}
	strategy.RollingUpdate = &rolling
	h.lifecycle.UpdateStrategy = &strategy
	w.retarget(h, h.target, h.desired.PodSpec.Containers[0].Image)
}

// rollMoves lists the moves this file adds: the kubelet refusing a pod,
// and the operator's actions beside a lost pod or an exiting runner.
func (w *repairWorld) rollMoves(h *recoveryHarness, all bool) []repairMove {
	var out []repairMove
	story := func(name string) bool { return all || w.cfg.story == name }
	delays := []int{0, 2}
	if all {
		delays = []int{0, 1, 2, 4}
	}
	targets := w.targets(h)
	if story(storyRejected) {
		for _, tgt := range targets {
			for _, after := range delays {
				for _, reason := range []string{"OutOfcpu", "UnexpectedAdmissionError"} {
					out = append(out, repairMove{kind: "reject", idx: tgt.idx, runner: tgt.runner, arg: reason, after: after})
				}
			}
		}
	}
	if story(storyOperator) && w.cfg.moment != "" {
		image := h.desired.PodSpec.Containers[0].Image
		if image != goodImage {
			out = append(out, repairMove{kind: "push", arg: strings.TrimPrefix(goodImage, rollImagePrefix)})
		}
		if image != fixedImage {
			out = append(out, repairMove{kind: "push", arg: strings.TrimPrefix(fixedImage, rollImagePrefix)})
		}
		out = append(out, repairMove{kind: "resize", arg: "+1"})
		if h.replicas > 1 {
			out = append(out, repairMove{kind: "resize", arg: "-1"})
		}
		switch {
		case partitionOf(h) > 0:
			out = append(out, repairMove{kind: "partition", arg: "clear"})
		case h.replicas > 1:
			out = append(out, repairMove{kind: "partition", arg: strconv.Itoa(int(h.replicas - 1))})
		}
		if h.desired.Paused {
			out = append(out, repairMove{kind: "resume"})
		} else {
			out = append(out, repairMove{kind: "pause"})
		}
		for _, tgt := range targets {
			for _, after := range delays {
				out = append(out, repairMove{kind: "delete", idx: tgt.idx, runner: tgt.runner, after: after})
				out = append(out, repairMove{kind: "exit", idx: tgt.idx, runner: tgt.runner, after: after})
			}
		}
	}
	return out
}

// playRollMove lands one of this file's moves; false for a move it does
// not know.
func (w *repairWorld) playRollMove(h *recoveryHarness, mv repairMove) bool {
	w.t.Helper()
	switch mv.kind {
	case "reject":
		pod := w.podFor(h, mv.idx, mv.runner)
		if pod == nil || pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
			return true
		}
		if h.rejected == nil {
			h.rejected = map[string]string{}
		}
		h.rejected[string(pod.UID)] = mv.arg
		h.reportRejected(pod, mv.arg)
	case "resize":
		delta, err := strconv.Atoi(mv.arg)
		if err != nil {
			w.t.Fatalf("resize %q: %v", mv.arg, err)
		}
		if h.replicas+int32(delta) < 1 {
			return true
		}
		h.replicas += int32(delta)
		w.retarget(h, h.target, h.desired.PodSpec.Containers[0].Image)
	case "partition":
		if mv.arg == "clear" {
			w.setPartition(h, 0)
			return true
		}
		partition, err := strconv.Atoi(mv.arg)
		if err != nil {
			w.t.Fatalf("partition %q: %v", mv.arg, err)
		}
		w.setPartition(h, int32(partition))
	case "pause":
		h.desired.Paused = true
	case "resume":
		h.desired.Paused = false
	default:
		return false
	}
	return true
}

// rollCharges is the retry-record accounting carried along a path: what
// each row and record read at the previous pass, and the charges and
// losses counted per revision since the start.
type rollCharges struct {
	primed    bool
	attempts  map[string]int32
	ops       map[int32]string
	opTargets map[int32]string
	failed    map[int32]bool
	records   map[int32]string
	charged   map[string]int
	lost      map[string]int
}

func newRollCharges() rollCharges {
	return rollCharges{attempts: map[string]int32{}, ops: map[int32]string{}, opTargets: map[int32]string{}, failed: map[int32]bool{}, records: map[int32]string{}, charged: map[string]int{}, lost: map[string]int{}}
}

func (c rollCharges) clone() rollCharges {
	return rollCharges{primed: c.primed, attempts: cloneMap(c.attempts), ops: cloneMap(c.ops), opTargets: cloneMap(c.opTargets),
		failed: cloneMap(c.failed), records: cloneMap(c.records), charged: cloneMap(c.charged), lost: cloneMap(c.lost)}
}

// failureIdentity names one recorded failure: the pod, the container, the
// reason and the time, which together change once per failure.
func failureIdentity(t *workloadtypes.InstanceTermination) string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s/%d", t.PodName, t.ContainerName, t.Reason, t.Time.Unix())
}

// observeCharges runs after every pass: a row whose attempt ended, that
// turned Failed, or that recorded a new failure counts one loss for the
// revisions it runs and targets; a record whose attempts rose counts
// that many charges for its revision. Charges beyond the losses are a
// record charged twice for one loss.
func (w *repairWorld) observeCharges(n *repairNode, h *recoveryHarness, rows []workloadtypes.InstanceStatus) {
	c := &n.mem.roll.charges
	if c.attempts == nil {
		*c = newRollCharges()
	}
	seenRows := map[int32]bool{}
	for _, s := range rows {
		seenRows[s.Index] = true
		id, target := "", ""
		if s.Operation != nil {
			id, target = s.Operation.ID, s.Operation.TargetRevision
		}
		record := failureIdentity(s.LastFailure)
		if c.primed {
			ended := c.ops[s.Index] != "" && c.ops[s.Index] != id
			failed := !c.failed[s.Index] && s.Phase == workloadtypes.InstancePhaseFailed
			lost := record != "" && record != c.records[s.Index] && !waitReason(s.LastFailure.Reason)
			if ended || failed || lost {
				for _, rev := range []string{s.RunningRevision, s.TargetRevision, target, c.opTargets[s.Index]} {
					if rev != "" {
						c.lost[rev]++
					}
				}
			}
		}
		c.ops[s.Index], c.opTargets[s.Index], c.failed[s.Index], c.records[s.Index] = id, target, s.Phase == workloadtypes.InstancePhaseFailed, record
	}
	for idx := range c.ops {
		if !seenRows[idx] {
			delete(c.ops, idx)
			delete(c.opTargets, idx)
			delete(c.failed, idx)
			delete(c.records, idx)
		}
	}
	seen := map[string]bool{}
	for _, b := range h.blocks {
		rev := b.TargetRevision
		seen[rev] = true
		if c.primed && b.AttemptsStarted > c.attempts[rev] {
			c.charged[rev] += int(b.AttemptsStarted - c.attempts[rev])
		}
		c.attempts[rev] = b.AttemptsStarted
		if c.primed && c.charged[rev] > c.lost[rev] {
			n.found = append(n.found, repairFinding{class: "charged-twice",
				message: fmt.Sprintf("the retry record of %s was charged %d time(s) for %d loss(es) (now %s, %d attempts, reason %q)", w.short(rev), c.charged[rev], c.lost[rev], b.State, b.AttemptsStarted, b.Reason)})
			c.lost[rev] = c.charged[rev]
		}
	}
	for rev := range c.attempts {
		if !seen[rev] {
			delete(c.attempts, rev)
		}
	}
	c.primed = true
}

// knownFailedAheadOfCurrent is the class of the first finding of this
// model: an Instance the roll promoted onto the target revision before the
// roll completed (a canary step, or an early batch) whose pod then cannot
// start past the start grace. The crash-loop repair reads a held pod
// against the Component's current revision only, so it skips this pod,
// and the escalation writes the row Failed with no operation: no repair
// opens, nothing counts on the target's retry record, and the roll holds
// its budget on the Instance. The row follows its pod back to Ready once
// the kubelet can start it; nothing rebuilds it before that.
// TestRepair_ACanaryInstanceWhosePodCannotStartAfterPromotionIsFailedWithoutARepair
// holds the current reading and names the checks a change flips.
const knownFailedAheadOfCurrent = "known-promoted-ahead-of-current-failed-without-repair"

// failedAheadOfTheCurrentRevision reads the picture of knownFailedAheadOfCurrent
// off a row: Failed with no operation on the target revision, which is not
// yet the Component's current revision, with no retry record for it and a
// live pod of it held in a kubelet waiting state.
func (w *repairWorld) failedAheadOfTheCurrentRevision(h *recoveryHarness, s workloadtypes.InstanceStatus) bool {
	if s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil || s.RunningRevision != h.target.Name || h.currentRevision == h.target.Name {
		return false
	}
	if h.findBlock(h.target.Name) != nil {
		return false
	}
	for _, pod := range h.podsOf(s.Index) {
		if len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].State.Waiting != nil {
			return true
		}
	}
	return false
}

// retriesSpent reports whether the retry record of a revision the row
// runs or targets, or of the Component's target, is Held: the engine owes
// the row nothing more until an operator releases the revision or pushes
// another.
func (w *repairWorld) retriesSpent(h *recoveryHarness, s workloadtypes.InstanceStatus) bool {
	revisions := []string{s.RunningRevision, s.TargetRevision, h.target.Name}
	if s.Operation != nil {
		revisions = append(revisions, s.Operation.TargetRevision)
	}
	for _, rev := range revisions {
		if b := h.findBlock(rev); rev != "" && b != nil && b.State == workloadtypes.RetryBlockHeld {
			return true
		}
	}
	return false
}

// podHeldByTheKubelet reports whether a live pod of the row's set, or of
// the replacement set its operation names, has a container the kubelet
// still holds in a waiting state it cannot leave on its own (an image it
// cannot pull, a crash loop): the engine stops on such a pod or follows
// it, and the row names the wait.
func (w *repairWorld) podHeldByTheKubelet(h *recoveryHarness, s workloadtypes.InstanceStatus) bool {
	indices := []int32{s.Index}
	if s.Operation != nil && s.Operation.SurgeIndex != nil {
		indices = append(indices, *s.Operation.SurgeIndex)
	}
	for _, idx := range indices {
		for _, pod := range h.podsOf(idx) {
			for _, cs := range pod.Status.ContainerStatuses {
				if waiting := cs.State.Waiting; waiting != nil && waiting.Reason != "" && waiting.Reason != "ContainerCreating" && waiting.Reason != "PodInitializing" {
					return true
				}
			}
		}
	}
	return false
}

// anyPodHeldByTheKubelet reports whether the kubelet holds a pod of any
// row of the Component.
func (w *repairWorld) anyPodHeldByTheKubelet(h *recoveryHarness, rows []workloadtypes.InstanceStatus) bool {
	for _, s := range rows {
		if w.podHeldByTheKubelet(h, s) {
			return true
		}
	}
	return false
}

// attemptWaitsUnderThePause reports whether the row of Instance idx
// carries an attempt while the operator's pause stands: a repair or an
// update that stopped under the pause, with a pod of its set gone or
// terminal, goes on once the operator resumes and names nothing before.
func (w *repairWorld) attemptWaitsUnderThePause(h *recoveryHarness, idx int32) bool {
	if !h.desired.Paused {
		return false
	}
	s := h.instance(idx)
	return s != nil && s.Operation != nil
}

// waitsBehindAHeldPod names the wait of an Instance that does not serve
// while a roll is open and the kubelet holds a pod somewhere in the
// Component: the roll stops on the held pod (a surge pair that ended on it, or an
// Instance it cannot reach) and the repairs of the Instances still on the
// old revision yield to the roll, so every Instance behind it waits until
// the kubelet starts the pod. The engine publishes no hold for it.
func (w *repairWorld) waitsBehindAHeldPod(h *recoveryHarness, idx int32) bool {
	if h.currentRevision == h.target.Name {
		return false
	}
	return w.anyPodHeldByTheKubelet(h, h.irStatuses())
}

// knownStates maps every Instance whose picture is a known class of this
// file to that class, and reports whether the roll's hold names one of
// them: the other rows the hold queues wait behind a known state, not on
// a gap of their own.
func (w *repairWorld) knownStates(n *repairNode, h *recoveryHarness, rows []workloadtypes.InstanceStatus) (map[int32]string, bool) {
	known := map[int32]string{}
	holdNamesKnown := false
	for _, s := range rows {
		switch {
		case w.failedAheadOfTheCurrentRevision(h, s):
			known[s.Index] = knownFailedAheadOfCurrent
		default:
			continue
		}
		if strings.HasPrefix(w.named(n, h, s), "hold:") {
			holdNamesKnown = true
		}
	}
	return known, holdNamesKnown
}

// checkFaultsCleared reads a settled state once no fault stands in the
// world. The known classes are read first, as pictures. Then, unless the
// kubelet still holds a pod of the Component (the engine stops on such a
// pod or follows it, and the recover move clears it), every Instance
// serves whole or its retries are spent; anything else is a class that
// fails the test, named by what the row shows.
func (w *repairWorld) checkFaultsCleared(n *repairNode, h *recoveryHarness) {
	if w.faultStanding(n, h) {
		return
	}
	rows := h.irStatuses()
	known, holdNamesKnown := w.knownStates(n, h, rows)
	for _, s := range rows {
		named := w.named(n, h, s)
		switch known[s.Index] {
		case knownFailedAheadOfCurrent:
			n.found = append(n.found, repairFinding{class: knownFailedAheadOfCurrent,
				message: fmt.Sprintf("Instance %d was promoted onto %s before the roll completed; its pod could not start and the row was written Failed with no repair, no attempt and no retry record (%s)", s.Index, w.short(s.RunningRevision), named)})
		}
	}
	if w.anyPodHeldByTheKubelet(h, rows) {
		return
	}
	for _, s := range rows {
		if _, ok := known[s.Index]; ok || (s.Phase == workloadtypes.InstancePhaseReady && w.servingSet(h, s.Index)) {
			continue
		}
		named := w.named(n, h, s)
		if w.retriesSpent(h, s) {
			n.found = append(n.found, repairFinding{class: "known-retries-spent",
				message: fmt.Sprintf("Instance %d does not serve and the retries of its revision are spent (%s)", s.Index, named)})
			continue
		}
		if holdNamesKnown && strings.HasPrefix(named, "hold:") {
			n.found = append(n.found, repairFinding{class: "known-held-behind-a-known-instance",
				message: fmt.Sprintf("Instance %d (%s on %s) waits on the roll's hold, which names an Instance in a known state (%s)", s.Index, s.Phase, w.short(s.RunningRevision), named)})
			continue
		}
		class := "repair-never-starts"
		switch {
		case s.Operation != nil && s.Operation.Waiting != "":
			class = "wait-never-ends"
		case s.Phase == workloadtypes.InstancePhaseFailed:
			class = "failed-never-starts-again"
		case s.Operation != nil:
			class = "attempt-never-ends"
		}
		n.found = append(n.found, repairFinding{class: class,
			message: fmt.Sprintf("Instance %d (%s on %s) does not serve after every fault cleared; the state names %q, no deadline or grace is left to run", s.Index, s.Phase, w.short(s.RunningRevision), named)})
	}
}

// rollConfigs lists the configurations of this file: the reference
// shapes at every moment under both strategies, and a seeded sample over
// gangs of one to eight pods. The bounded mode takes one story per
// strategy and moment; the full mode takes them all.
func rollConfigs(full bool) []repairConfig {
	chart := repairKnobs{deadlines: "chart", tries: 3}
	short := repairKnobs{deadlines: "short", tries: 2, shortDelays: true, forceDelete: true}
	recreate, none := v1beta1.InstanceRestartPolicyRecreateInstance, v1beta1.InstanceRestartPolicyNone
	strategies := []workloadtypes.UpdateStrategyType{workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain}
	stories := []string{storyLost, storyNode, storyExit, storyRejected, storyWaiting, storyOperator}
	shapes := []repairShape{{8, 1}, {4, 2}, {4, 4}}
	var out []repairConfig
	i := 0
	for _, strategy := range strategies {
		for _, moment := range rollMoments {
			if full {
				for _, shape := range shapes {
					for _, story := range stories {
						out = append(out, repairConfig{shape: shape, policy: recreate, knobs: chart, story: story, moment: moment, strategy: strategy})
					}
				}
				continue
			}
			out = append(out, repairConfig{shape: shapes[i%len(shapes)], policy: recreate, knobs: chart, story: stories[i%len(stories)], moment: moment, strategy: strategy})
			i++
		}
	}
	rng := rand.New(rand.NewSource(20261102))
	sample := 8
	if full {
		sample = 64
	}
	for k := 0; k < sample; k++ {
		shape := repairShape{instances: []int32{4, 8, 12, 16}[rng.Intn(4)], pods: int32(1 + rng.Intn(8))}
		policy := recreate
		if rng.Intn(4) == 0 {
			policy = none
		}
		knobs := chart
		if rng.Intn(2) == 0 {
			knobs = short
		}
		out = append(out, repairConfig{shape: shape, policy: policy, knobs: knobs, story: stories[rng.Intn(len(stories))],
			moment: rollMoments[rng.Intn(len(rollMoments))], strategy: strategies[rng.Intn(len(strategies))]})
	}
	return out
}

// TestRepair_FaultsDuringARollEndServingOrNamed lands the faults while a
// push of a sound revision is settling, rotating, draining or at its last
// Instance, under RecreatePod and SurgeThenDrain.
func TestRepair_FaultsDuringARollEndServingOrNamed(t *testing.T) {
	repairExplore(t, func(c repairConfig) bool {
		switch c.moment {
		case momentSettling, momentRotating, momentDraining, momentLast:
			return c.story != storyOperator
		}
		return false
	})
}

// TestRepair_FaultsUnderAHeldCanaryOrPausedRollEndServingOrNamed lands the
// faults on a roll whose retries are spent, on a canary step and on a
// paused roll, and on the idle Component under both strategies.
func TestRepair_FaultsUnderAHeldCanaryOrPausedRollEndServingOrNamed(t *testing.T) {
	repairExplore(t, func(c repairConfig) bool {
		switch c.moment {
		case momentIdle, momentHeld, momentCanary, momentPaused:
			return c.story != storyOperator
		}
		return false
	})
}

// TestRepair_OperatorActionsBesideAFaultEndServingOrNamed plays the
// operator's actions (a push back or forward, a resize, a partition, a
// pause and a resume) beside a lost pod or an exiting runner at every
// moment.
func TestRepair_OperatorActionsBesideAFaultEndServingOrNamed(t *testing.T) {
	repairExplore(t, func(c repairConfig) bool { return c.moment != "" && c.story == storyOperator })
}

// TestRepair_ACanaryInstanceWhosePodCannotStartAfterPromotionIsFailedWithoutARepair
// holds the current reading of the model's first finding. Eight single-pod
// Instances; a canary step keeps seven on version 1 and moves Instance 7
// to version 2, which serves. Its pod then stops in ImagePullBackOff (the
// node lost the image after a container restart) and the start grace
// passes. The row is written Failed with no operation, no retry
// record names version 2, no repair opens, and the roll's hold names the
// Instance for good. A fix flips the three middle checks: the repair
// opens, or the failure counts on version 2's retry record so the
// Instance is rebuilt on the retry schedule and the revision is held once
// the retries are spent.
func TestRepair_ACanaryInstanceWhosePodCannotStartAfterPromotionIsFailedWithoutARepair(t *testing.T) {
	cfg := repairConfig{shape: repairShape{8, 1}, policy: v1beta1.InstanceRestartPolicyRecreateInstance,
		knobs: repairKnobs{deadlines: "chart", tries: 3}, story: storyWaiting, moment: momentCanary, strategy: workloadtypes.UpdateStrategyRecreatePod}
	w := newRepairWorld(t, cfg)
	n := w.walk(w.start(), "waiting:ImagePullBackOff@7", "tick-all")
	h := w.restore(n.snap)
	s := h.instance(7)
	if s == nil || s.RunningRevision != h.target.Name {
		t.Fatalf("Instance 7 should run the canary revision; picture:\n%s", w.picture(h))
	}
	if h.currentRevision == h.target.Name {
		t.Fatalf("the canary step should keep the current revision at version 1; picture:\n%s", w.picture(h))
	}
	if s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil {
		t.Errorf("current reading: Instance 7 is written Failed with no operation; got %s with operation %v; picture:\n%s", s.Phase, s.Operation, w.picture(h))
	}
	if b := h.findBlock(h.target.Name); b != nil {
		t.Errorf("current reading: no retry record names the canary revision; got %s with %d attempts", b.State, b.AttemptsStarted)
	}
	if restarts := eventsNaming(h, workloadtypes.EventReasonRestartTriggered, 7); restarts != 0 {
		t.Errorf("current reading: no repair opens on Instance 7; got %d RestartTriggered events", restarts)
	}
	if named := w.named(n, h, *s); !strings.HasPrefix(named, "hold:") && !strings.HasPrefix(named, "Failed:") {
		t.Errorf("the state should name Instance 7 through the roll's hold or its failure record; got %q", named)
	}
	for _, f := range n.found {
		if f.class == knownFailedAheadOfCurrent {
			return
		}
	}
	t.Errorf("the model should read the picture as %s; it found %v", knownFailedAheadOfCurrent, n.found)
}

// TestRepair_ARefusedWorkerDuringARebuildKeepsTheCrashOnTheRetrySchedule:
// four gangs of two pods are rebuilt whole on a pod restart, with a retry
// schedule of two attempts for the revision. The leader of gang 0 exits
// after Ready, the policy rebuilds the gang, and the row's record names
// the leader's container. The kubelet refuses the forming gang's worker;
// the refused member is deleted for its replacement and the record keeps
// naming the leader's exit. So when the rebuilt gang's leader exits again
// inside the crash window, the exit counts as attempt 1 of 2 on version
// 1's retry record and the next rebuild waits the backoff, exactly as it
// does without the refusal (the control).
func TestRepair_ARefusedWorkerDuringARebuildKeepsTheCrashOnTheRetrySchedule(t *testing.T) {
	cfg := repairConfig{shape: repairShape{4, 2}, policy: v1beta1.InstanceRestartPolicyRecreateInstance,
		knobs: repairKnobs{deadlines: "short", tries: 2, shortDelays: true, forceDelete: true}, story: storyExit}
	retryText := func(b *workloadtypes.RetryBlock) string {
		if b == nil {
			return "no retry record"
		}
		return fmt.Sprintf("%s after %d attempt(s)", b.State, b.AttemptsStarted)
	}

	control := newRepairWorld(t, cfg)
	n := control.walk(control.root(), "exit@0/leader", "exit@0/leader+5p")
	h := control.restore(n.snap)
	controlRow, controlBlock := h.instance(0), h.findBlock(h.target.Name)
	if controlRow == nil || controlBlock == nil || controlBlock.AttemptsStarted != 1 {
		t.Fatalf("without a refusal, the rebuilt gang's exit after Ready counts as attempt 1 of 2 on version 1's retry record; got %s; picture:\n%s", retryText(controlBlock), control.picture(h))
	}
	controlRestarts := eventsNaming(h, workloadtypes.EventReasonRestartTriggered, 0)

	w := newRepairWorld(t, cfg)
	n = w.walk(w.root(), "exit@0/leader", "reject:UnexpectedAdmissionError@0/worker+2p")
	h = w.restore(n.snap)
	s := h.instance(0)
	if s == nil || eventsNaming(h, workloadtypes.EventReasonRestartTriggered, 0) != 1 {
		t.Fatalf("the leader's exit should have opened one repair of gang 0; picture:\n%s", w.picture(h))
	}
	if eventsNaming(h, workloadtypes.EventReasonTerminalPodRecycled, 0) == 0 {
		t.Fatalf("the refused worker should have been deleted for its replacement; picture:\n%s", w.picture(h))
	}
	if b := h.findBlock(h.target.Name); b != nil {
		t.Fatalf("no exit has counted yet, so version 1 should have no retry record; got %s", retryText(b))
	}
	if s.LastFailure == nil || !strings.HasSuffix(s.LastFailure.PodName, "-leader-0") || s.LastFailure.ContainerName == "" {
		t.Errorf("the row's record must keep naming the leader's exit while the refused worker is replaced; got %s", terminationText(s.LastFailure))
	}
	for _, f := range n.found {
		if f.class == "rebuild-without-record" {
			t.Errorf("the model read the rebuild as one without a record: %s", f.message)
		}
	}

	n = w.walk(n, "exit@0/leader+5p")
	h = w.restore(n.snap)
	s = h.instance(0)
	if b := h.findBlock(h.target.Name); b == nil || b.AttemptsStarted != 1 || b.State != controlBlock.State {
		t.Errorf("the exit after the refusal must count on version 1's retry record as the exit without it does (%s); got %s; picture:\n%s", retryText(controlBlock), retryText(b), w.picture(h))
	}
	if s == nil || s.Phase != controlRow.Phase || eventsNaming(h, workloadtypes.EventReasonRestartTriggered, 0) != controlRestarts {
		t.Errorf("the gang must wait the retry schedule as it does without the refusal (%s, %d restart(s)); picture:\n%s", controlRow.Phase, controlRestarts, w.picture(h))
	}
}

// TestRepair_APromotedGangThatLosesAMemberUnderNoneIsRefilledWhileTheRollIsOpen:
// four gangs of two pods under restart policy None roll; at the rotating
// moment one gang is already promoted, one attempt is open and two gangs
// still run version 1, and at the canary moment one gang is promoted while
// a partition holds the other three. The promoted gang's leader is
// deleted. The policy rebuilds nothing, so the lost pod is the Create
// pass's to refill, and the open roll must not keep that pass from it:
// once the kubelet has let the deleted pod go, the leader is re-created,
// the gang serves whole again, no hold names it, and the roll goes on to
// its end once the operator lets it. The canary case is the control: with
// nothing to start the pass reaches the full Create scope on its own.
func TestRepair_APromotedGangThatLosesAMemberUnderNoneIsRefilledWhileTheRollIsOpen(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
		moment   string
	}{
		{"surge roll with one attempt open", workloadtypes.UpdateStrategySurgeThenDrain, momentRotating},
		{"drain-first roll with one attempt open", workloadtypes.UpdateStrategyRecreatePod, momentRotating},
		{"canary step holding the rest", workloadtypes.UpdateStrategySurgeThenDrain, momentCanary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := repairConfig{shape: repairShape{4, 2}, policy: v1beta1.InstanceRestartPolicyNone,
				knobs: repairKnobs{deadlines: "short", tries: 2, shortDelays: true, forceDelete: true}, story: storyExit, moment: tc.moment, strategy: tc.strategy}
			w := newRepairWorld(t, cfg)
			root := w.start()
			h := w.restore(root.snap)
			promoted := promotedGangOf(h)
			if promoted < 0 {
				t.Fatalf("the %s moment should hold a promoted gang; picture:\n%s", tc.moment, w.picture(h))
			}
			// The deleted pod leaves once its termination grace has run;
			// the tick steps the clock past it.
			n := w.walk(root, fmt.Sprintf("delete@%d/leader", promoted), "tick")
			h = w.restore(n.snap)
			if live := len(h.podsOf(promoted)); live != 2 {
				t.Errorf("the lost leader must be re-created while the roll is open; the gang has %d live pod(s); picture:\n%s", live, w.picture(h))
			}
			s := h.instance(promoted)
			if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != h.target.Name || !w.servingSet(h, promoted) {
				t.Errorf("the refilled gang must serve whole again on version 2 with no operation; picture:\n%s", w.picture(h))
			}
			if s != nil {
				if named := w.named(n, h, *s); strings.HasPrefix(named, "hold:") {
					t.Errorf("no hold may name the refilled gang; got %q; picture:\n%s", named, w.picture(h))
				}
			}
			if tc.moment == momentCanary {
				n = w.walk(n, "partition:clear")
			}
			n, h = w.tickUntilSettled(n, 12)
			if !w.settled(h) {
				t.Errorf("the roll must go on to its end once the gang serves whole; picture:\n%s", w.picture(h))
			}
			for _, f := range n.found {
				t.Errorf("the model must read nothing to report on this path; it found %s: %s", f.class, f.message)
			}
		})
	}
}

// promotedGangOf is the index of the first Instance Ready on the target
// with no operation, or -1 when none is.
func promotedGangOf(h *recoveryHarness) int32 {
	for _, s := range h.irStatuses() {
		if s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision == h.target.Name {
			return s.Index
		}
	}
	return -1
}

// tickUntilSettled steps the clock past the next timer, at most limit
// times, until every Instance serves whole on the target, and returns
// the node and the harness it reached.
func (w *repairWorld) tickUntilSettled(n *repairNode, limit int) (*repairNode, *recoveryHarness) {
	h := w.restore(n.snap)
	for i := 0; i < limit && !w.settled(h); i++ {
		n = w.walk(n, "tick")
		h = w.restore(n.snap)
	}
	return n, h
}

// TestRepair_AGangWhoseRefillRanPastTheDeadlineServesAgainOnceItsSetRuns:
// eight gangs of three under restart policy None, a canary step keeping
// seven on version 1. The kubelet refuses gang 0's leader; the engine
// deletes the refused pod and opens a Create for the gap; the clock runs
// past InstanceReadyTimeout before the refused pod has gone. The row is
// written Failed with no operation and version 1's retry record counts
// one attempt. The replacement leader then runs, and once the record's
// backoff has passed the gang, held on version 1 by the canary step, is
// put back into rotation: the row returns to Ready on version 1 with
// every pod serving, and the model reads nothing wrong in the picture.
func TestRepair_AGangWhoseRefillRanPastTheDeadlineServesAgainOnceItsSetRuns(t *testing.T) {
	cfg := repairConfig{shape: repairShape{8, 3}, policy: v1beta1.InstanceRestartPolicyNone,
		knobs: repairKnobs{deadlines: "short", tries: 2, shortDelays: true, forceDelete: true}, story: storyNode, moment: momentCanary, strategy: workloadtypes.UpdateStrategyRecreatePod}
	w := newRepairWorld(t, cfg)
	n := w.walk(w.start(), "reject:UnexpectedAdmissionError@0/leader", "tick-all")
	h := w.restore(n.snap)
	s := h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil || s.LastFailure == nil || s.LastFailure.Reason != "DeadlineExceeded" {
		t.Fatalf("the refill should have run past the deadline and left the row Failed with no operation; picture:\n%s", w.picture(h))
	}
	revision := s.RunningRevision
	if revision == "" || revision == h.target.Name {
		t.Fatalf("the canary step should keep gang 0 on version 1; picture:\n%s", w.picture(h))
	}
	if b := h.findBlock(revision); b == nil || b.AttemptsStarted != 1 {
		t.Fatalf("the deadline should count as attempt 1 on version 1's retry record; picture:\n%s", w.picture(h))
	}

	n = w.walk(n, "tick-all", "tick-1m")
	h = w.restore(n.snap)
	s = h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != revision || !w.servingSet(h, 0) {
		t.Errorf("once its set runs the gang must return to Ready on version 1 with every pod serving; picture:\n%s", w.picture(h))
	}
	serving := 0
	for _, pod := range h.podsOf(0) {
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil && podConditionTrue(pod, corev1.PodReady) {
			serving++
		}
	}
	if serving != 3 {
		t.Errorf("every pod of the gang must be in rotation; got %d of 3; picture:\n%s", serving, w.picture(h))
	}
	for _, f := range n.found {
		t.Errorf("the model should read nothing wrong once the gang serves again; it found %s: %s", f.class, f.message)
	}
}

// TestRepair_AHeldInstanceWhoseRebuildHadNoRoomServesAgainOnceItIsPlaced:
// eight single-pod Instances under restart policy None, a canary step
// keeping seven on version 1. The cluster has no room when Instance 0's
// pod is deleted, so the Create pass's rebuild on version 1 waits for
// placement until the placement grace ends it: the row is written Failed
// with no operation, and the failure, the environment's, counts on no
// retry record. When room comes back the pod is placed and runs, and the
// Instance, held on version 1 by the step, is put back into rotation at
// once: the row returns to Ready on version 1 with its pod serving.
func TestRepair_AHeldInstanceWhoseRebuildHadNoRoomServesAgainOnceItIsPlaced(t *testing.T) {
	cfg := repairConfig{shape: repairShape{8, 1}, policy: v1beta1.InstanceRestartPolicyNone,
		knobs: repairKnobs{deadlines: "short", tries: 2, shortDelays: true, forceDelete: true}, story: storyRoom, moment: momentCanary, strategy: workloadtypes.UpdateStrategyRecreatePod}
	w := newRepairWorld(t, cfg)
	n := w.walk(w.start(), "no-room", "delete@0", "tick-all", "tick-all")
	h := w.restore(n.snap)
	s := h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil || s.LastFailure == nil || s.LastFailure.Reason != "Unschedulable" {
		t.Fatalf("the placement grace should have ended the rebuild and left the row Failed with no operation; picture:\n%s", w.picture(h))
	}
	revision := s.RunningRevision
	if revision == "" || revision == h.target.Name {
		t.Fatalf("the canary step should keep Instance 0 on version 1; picture:\n%s", w.picture(h))
	}
	if b := h.findBlock(revision); b != nil {
		t.Fatalf("a failure the environment caused counts on no retry record; got %s after %d attempt(s)", b.State, b.AttemptsStarted)
	}

	n = w.walk(n, "room-back")
	h = w.restore(n.snap)
	s = h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != revision || !w.servingSet(h, 0) {
		t.Errorf("once its pod is placed and runs the Instance must return to Ready on version 1 with the pod serving; picture:\n%s", w.picture(h))
	}
	for _, f := range n.found {
		t.Errorf("the model should read nothing wrong once the Instance serves again; it found %s: %s", f.class, f.message)
	}
}

// TestRepair_AFailedInstanceTheRollMayTakeIsRecreatedByTheRoll holds the
// boundary of the held re-prove: eight single-pod Instances under restart
// policy None with a roll under way and nothing holding Instance 0 on
// version 1. The same placement wait ends its rebuild Failed with no
// operation; the row is the roll's, so once room comes back the roll
// recreates the Instance onto version 2 instead of the Create pass proving
// it on version 1, and the roll finishes with every Instance on version 2.
func TestRepair_AFailedInstanceTheRollMayTakeIsRecreatedByTheRoll(t *testing.T) {
	cfg := repairConfig{shape: repairShape{8, 1}, policy: v1beta1.InstanceRestartPolicyNone,
		knobs: repairKnobs{deadlines: "short", tries: 5, shortDelays: true, forceDelete: true}, story: storyRoom, moment: momentRotating, strategy: workloadtypes.UpdateStrategyRecreatePod}
	w := newRepairWorld(t, cfg)
	root := w.start()
	h := w.restore(root.snap)
	old := int32(-1)
	for _, s := range h.irStatuses() {
		if s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision != h.target.Name {
			old = s.Index
			break
		}
	}
	if old < 0 {
		t.Fatalf("the rotating moment should hold an Instance still on version 1; picture:\n%s", w.picture(h))
	}
	n := w.walk(root, "no-room", fmt.Sprintf("delete@%d", old), "tick-all", "tick-all")
	h = w.restore(n.snap)
	s := h.instance(old)
	if s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil {
		t.Fatalf("the placement grace should have ended the rebuild and left the row Failed with no operation; picture:\n%s", w.picture(h))
	}
	n = w.walk(n, "room-back", "tick-1m", "tick-1m", "tick-1m", "tick-1m")
	h = w.restore(n.snap)
	s = h.instance(old)
	if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != h.target.Name || !w.servingSet(h, old) {
		t.Errorf("a Failed Instance the roll may take is the roll's: it must end Ready on version 2 with its pod serving; picture:\n%s", w.picture(h))
	}
}
