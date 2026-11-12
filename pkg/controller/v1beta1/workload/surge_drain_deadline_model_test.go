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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the production engine through the second half of a
// SurgeThenDrain roll, the part after the replacement serves: the
// readiness window the replacement holds Ready for, the old pods leaving
// rotation, and their termination. A surge pair opens with one deadline
// on both rows (the attempt's InstanceReadyTimeout), the step advances
// Surge -> SurgeDrain without touching it, and the escalation leaves a
// pair whose pod set serves alone. The question is what the rows say when
// that deadline passes during a wait the engine has decided to sit out.
//
// The world is the recovery harness: one Component, gangs of one to eight
// pods, one to four Instances, SurgeThenDrain at a surge of one, the
// endpoint model on, the kubelet termination model holding a deleted pod
// for its grace. The old pods leave together, one at a time, or with one
// member whose kubelet stops reporting for a while or for good. Both
// schedulers' timing are configurations: every member placed together,
// or the workers placed one tick after the leader. The deadline is placed
// by a reference run of the same shape: inside the readiness window,
// inside the old pods' termination, or past everything.
//
// The probe at every pass: a row whose operation deadline has passed
// carries a waiting token, the Component carries a rollout hold, or the
// row has changed since the previous pass (the engine acted within one
// pass). A row that stands still past its deadline with neither is a
// finding; one that stands so for longer than the grace and a few passes
// is a finding of its own. The budgets hold at every pass (never more
// than one gang beside the fleet, no old gang deleted before its
// replacement serves), and every roll finishes on the target unless a
// kubelet never returns. OME_SURGE_DRAIN_HUNT=full widens the
// configurations and adds seeded random deadlines;
// OME_SURGE_DRAIN_HUNT_ONLY=<text> keeps the configurations whose name
// carries the text.

const (
	surgeDrainFullEnv = "OME_SURGE_DRAIN_HUNT"
	surgeDrainOnlyEnv = "OME_SURGE_DRAIN_HUNT_ONLY"
	// surgeDrainStep is the clock step of a pass: one operation requeue.
	surgeDrainStep = 5 * time.Second
	// surgeDrainTickBudget bounds the passes one run may take.
	surgeDrainTickBudget = 600
	// surgeDrainUnreportedPasses is how many passes a member's kubelet stops
	// reporting under the one-unreported pattern before it reports again.
	surgeDrainUnreportedPasses = 12
	// surgeDrainLongTimeout places the deadline past every wait.
	surgeDrainLongTimeout = 4 * time.Hour
)

// surgeLeaving is how the old pods leave once the engine deletes them.
type surgeLeaving string

const (
	// leaveTogether: every old pod finishes terminating at its grace.
	leaveTogether surgeLeaving = "together"
	// leaveOneByOne: the old pods finish one pass apart, in name order.
	leaveOneByOne surgeLeaving = "one-by-one"
	// leaveOneUnreported: one old pod's kubelet stops reporting at the
	// delete and reports again surgeDrainUnreportedPasses passes later.
	leaveOneUnreported surgeLeaving = "one-unreported"
	// leaveOneLost: one old pod's kubelet never reports again.
	leaveOneLost surgeLeaving = "one-lost"
)

// surgeMoment names where the pair's deadline falls.
type surgeMoment string

const (
	// momentInWindow: while the replacement holds Ready for the window.
	momentInWindow surgeMoment = "window"
	// momentInTermination: while the old pods terminate.
	momentInTermination surgeMoment = "termination"
	// momentNever: past every wait, so the deadline never passes.
	momentNever surgeMoment = "never"
	// momentRandom: a seeded offset between the pair's open and its end.
	momentRandom surgeMoment = "random"
)

// surgeDrainConfig is one configuration of the model.
type surgeDrainConfig struct {
	pods      int32
	instances int32
	grace     time.Duration
	leaving   surgeLeaving
	staggered bool
	window    int32
	moment    surgeMoment
	// seed drives the random moment's offset.
	seed int64
}

func (c surgeDrainConfig) String() string {
	timing := "gang-timing"
	if c.staggered {
		timing = "staggered-timing"
	}
	return fmt.Sprintf("%dx%d/grace%ds/%s/%s/window%d/%s", c.instances, c.pods, int(c.grace.Seconds()), c.leaving, timing, c.window, c.moment)
}

// surgeDrainShapes are the shapes of the bounded mode: the reference shape
// (three gangs of two), a single gang of four, two gangs of eight and two
// single-pod Instances.
var surgeDrainShapes = [][2]int32{{3, 2}, {1, 4}, {2, 8}, {2, 1}}

// surgeDrainConfigs lists the configurations. Bounded: the reference shape
// under both timings, every leaving pattern and both placed moments, plus
// a seeded sample of the other shapes. Full: the grid over gangs of one
// to eight pods, one to four Instances, four graces, every pattern, both
// timings and both windows, with a random moment per shape. Every
// configuration runs the batched status path, the one the manager wires.
func surgeDrainConfigs(full bool) []surgeDrainConfig {
	if full {
		return surgeDrainFullConfigs()
	}
	return surgeDrainBoundedConfigs()
}

// surgeDrainAdd appends c unless it places the deadline inside a window
// the configuration does not have.
func surgeDrainAdd(out []surgeDrainConfig, c surgeDrainConfig) []surgeDrainConfig {
	if c.moment == momentInWindow && c.window == 0 {
		return out
	}
	return append(out, c)
}

// surgeDrainBoundedConfigs is the CI set: the reference shape under both
// timings, three leaving patterns and both placed moments; a seeded sample
// over the other shapes; one pair whose kubelet never returns.
func surgeDrainBoundedConfigs() []surgeDrainConfig {
	var out []surgeDrainConfig
	for _, staggered := range []bool{false, true} {
		for _, leaving := range []surgeLeaving{leaveTogether, leaveOneByOne, leaveOneUnreported} {
			for _, moment := range []surgeMoment{momentInWindow, momentInTermination} {
				out = surgeDrainAdd(out, surgeDrainConfig{pods: 2, instances: 3, grace: 40 * time.Second, leaving: leaving, staggered: staggered, window: 90, moment: moment})
			}
		}
	}
	rng := rand.New(rand.NewSource(20261111))
	graces := []time.Duration{0, 10 * time.Second, 30 * time.Second, 60 * time.Second}
	for k := 0; k < 44; k++ {
		shape := surgeDrainShapes[k%len(surgeDrainShapes)]
		leaving := []surgeLeaving{leaveTogether, leaveOneByOne, leaveOneUnreported}[rng.Intn(3)]
		moment := []surgeMoment{momentInWindow, momentInTermination, momentNever}[rng.Intn(3)]
		window := []int32{0, 60}[rng.Intn(2)]
		out = surgeDrainAdd(out, surgeDrainConfig{pods: shape[1], instances: shape[0], grace: graces[rng.Intn(len(graces))], leaving: leaving,
			staggered: rng.Intn(2) == 0, window: window, moment: moment})
	}
	return surgeDrainAdd(out, surgeDrainConfig{pods: 2, instances: 2, grace: 30 * time.Second, leaving: leaveOneLost, staggered: false, window: 0, moment: momentInTermination})
}

// surgeDrainFullConfigs is the grid of the full mode.
func surgeDrainFullConfigs() []surgeDrainConfig {
	var out []surgeDrainConfig
	seed := int64(1)
	for _, pods := range []int32{1, 2, 3, 4, 6, 8} {
		for _, instances := range []int32{1, 2, 3, 4} {
			for _, grace := range []time.Duration{0, 10 * time.Second, 30 * time.Second, 60 * time.Second} {
				for _, leaving := range []surgeLeaving{leaveTogether, leaveOneByOne, leaveOneUnreported, leaveOneLost} {
					shape := surgeDrainConfig{pods: pods, instances: instances, grace: grace, leaving: leaving}
					out = surgeDrainShapeConfigs(out, shape, &seed)
				}
			}
		}
	}
	return out
}

// surgeDrainShapeConfigs adds one shape's configurations: both timings,
// both windows, the three placed moments and a seeded random one. A pair
// whose kubelet never returns runs with a grace, no window and no random
// moment.
func surgeDrainShapeConfigs(out []surgeDrainConfig, shape surgeDrainConfig, seed *int64) []surgeDrainConfig {
	for _, staggered := range []bool{false, true} {
		for _, window := range []int32{0, 60} {
			if shape.leaving == leaveOneLost && (shape.grace == 0 || window != 0) {
				continue
			}
			c := shape
			c.staggered, c.window = staggered, window
			for _, moment := range []surgeMoment{momentInWindow, momentInTermination, momentNever} {
				c.moment = moment
				out = surgeDrainAdd(out, c)
			}
			if shape.leaving != leaveOneLost {
				*seed++
				c.moment, c.seed = momentRandom, *seed
				out = surgeDrainAdd(out, c)
			}
		}
	}
	return out
}

// surgeRefTimings is what a reference run of a shape measures on its first
// pair, as offsets from the pair's open: when the replacement set was
// Ready, when the first old pod was asked to leave, when the last was gone.
type surgeRefTimings struct {
	ready, drain, gone time.Duration
}

// surgeFinding is one picture the model read.
type surgeFinding struct {
	message string
	state   string
}

// surgeDrainWorld is one configuration's world on the recovery harness.
type surgeDrainWorld struct {
	t       *testing.T
	cfg     surgeDrainConfig
	h       *recoveryHarness
	timeout time.Duration

	ticks, passes, probes int
	seen                  map[string]struct{}
	counts                map[string]int
	findings              map[string]surgeFinding
	// prev is each row's fingerprint after the previous pass while its
	// deadline stood passed; standing counts the passes it stood.
	prev     map[string]string
	standing map[string]int
	// longest is the longest a passed deadline stood with nothing named.
	longest time.Duration
	// unreported names the old pod whose kubelet stopped reporting, and
	// unreportedAt the pass it did.
	unreported   string
	unreportedAt int
	// spread remembers the old pods whose termination ends were spread
	// one pass apart.
	spread map[string]bool
	// classStates counts, per finding class, the row states it was read on.
	classStates map[string]map[workloadtypes.RowState]int
}

func newSurgeDrainWorld(t *testing.T, cfg surgeDrainConfig, timeout time.Duration) *surgeDrainWorld {
	t.Helper()
	h := newRecoveryHarnessFor(t, cfg.pods > 1, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(4096)
	h.workers = cfg.pods - 1
	h.replicas = cfg.instances
	maxSurge, maxUnavailable := intstr.FromInt(1), intstr.FromInt(0)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: timeout}
	h.minReadySeconds = cfg.window
	h.atomicStatus = true
	h.podGrace = cfg.grace
	h.routed = true
	h.stepFloor, h.stepCap = surgeDrainStep, surgeDrainStep
	h.setTarget(h.revV1, goodImage)
	w := &surgeDrainWorld{
		t: t, cfg: cfg, h: h, timeout: timeout,
		seen: map[string]struct{}{}, counts: map[string]int{}, findings: map[string]surgeFinding{},
		prev: map[string]string{}, standing: map[string]int{}, spread: map[string]bool{},
		classStates: map[string]map[workloadtypes.RowState]int{},
	}
	if !h.run(40+10*int(cfg.instances), func() bool { return w.settledOn(h.revV1.Name) }) {
		h.dumpState("initial create")
		t.Fatalf("[%s] the initial create never settled on the first revision", cfg)
	}
	h.events, h.holds, h.heldWarnings, h.failedWarnings = nil, nil, nil, nil
	return w
}

// allPods lists every pod of the Component, the terminating ones included,
// by Instance index.
func (w *surgeDrainWorld) allPods() map[int32][]*corev1.Pod {
	w.t.Helper()
	list := &corev1.PodList{}
	if err := w.h.c.List(w.h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	out := map[int32][]*corev1.Pod{}
	for i := range list.Items {
		pod := &list.Items[i]
		if idx, ok := query.InstanceIdxFromLabels(pod); ok {
			out[idx] = append(out[idx], pod)
		}
	}
	for _, pods := range out {
		sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	}
	return out
}

// settledOn reports whether every Instance serves whole on rev with no
// operation open, whichever indices the rows hold.
func (w *surgeDrainWorld) settledOn(rev string) bool {
	rows := w.h.irStatuses()
	if int32(len(rows)) != w.cfg.instances {
		return false
	}
	for _, s := range rows {
		if s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != rev {
			return false
		}
		pods := w.h.podsOf(s.Index)
		if int32(len(pods)) != w.cfg.pods {
			return false
		}
		for _, pod := range pods {
			if !podreadiness.IsServing(pod) || !podConditionTrue(pod, corev1.PodReady) {
				return false
			}
		}
	}
	return true
}

// staggerHook is the default scheduler's timing: a worker the kubelet has
// not seen yet is refused by the scheduler model for one pass and placed
// on the next, so a gang's members come up one pass apart.
func (w *surgeDrainWorld) staggerHook(pods map[int32][]*corev1.Pod) {
	if !w.cfg.staggered || w.cfg.pods == 1 {
		return
	}
	h := w.h
	if h.unplaceable == nil {
		h.unplaceable = map[string]bool{}
	}
	for _, set := range pods {
		for _, pod := range set {
			if pod.Labels[query.LabelRunner] != "worker" || pod.DeletionTimestamp != nil {
				continue
			}
			_, _, refused := workloadtypes.PodUnschedulable(pod)
			switch {
			case pod.Status.Phase == "" && !h.unplaceable[pod.Name]:
				h.unplaceable[pod.Name] = true
			case refused && h.unplaceable[pod.Name]:
				delete(h.unplaceable, pod.Name)
			}
		}
	}
}

// leavingHook shapes how the old pods leave once the engine has deleted
// them: their termination ends spread one pass apart, or one member's
// kubelet not reporting for a while or for good. The first old pod seen
// leaving decides which member it is.
func (w *surgeDrainWorld) leavingHook(pods map[int32][]*corev1.Pod) {
	h := w.h
	if w.cfg.leaving == leaveOneUnreported && w.unreported != "" && w.passes-w.unreportedAt >= surgeDrainUnreportedPasses {
		delete(h.kubeletGone, w.unreported)
	}
	for _, set := range pods {
		var leaving []*corev1.Pod
		for _, pod := range set {
			if pod.DeletionTimestamp != nil && !w.spread[pod.Name] {
				leaving = append(leaving, pod)
			}
		}
		if len(leaving) == 0 {
			continue
		}
		for k, pod := range leaving {
			w.spread[pod.Name] = true
			switch w.cfg.leaving {
			case leaveOneByOne:
				if until, ok := h.terminatingUntil[pod.Name]; ok {
					h.terminatingUntil[pod.Name] = until.Add(time.Duration(k) * surgeDrainStep)
				}
			case leaveOneUnreported, leaveOneLost:
				if w.unreported == "" {
					w.unreported, w.unreportedAt = pod.Name, w.passes
					h.kubeletGone[pod.Name] = true
				}
			}
		}
	}
}

// tick runs the hooks, one pass of the engine and the probe, and moves the
// clock one requeue.
func (w *surgeDrainWorld) tick() {
	w.t.Helper()
	pods := w.allPods()
	w.staggerHook(pods)
	w.leavingHook(pods)
	w.h.hold, w.h.verdict = nil, nil
	if _, err := w.h.pass(); err != nil {
		w.note("error", "Reconcile returned an error: %v", err)
	}
	w.passes++
	w.h.clk.Step(surgeDrainStep)
	w.ticks++
	w.probe()
}

// note records one picture under its class; the first of a class is kept
// as the example.
func (w *surgeDrainWorld) note(class, format string, args ...any) {
	w.counts[class]++
	if _, ok := w.findings[class]; !ok {
		w.findings[class] = surgeFinding{message: fmt.Sprintf(format, args...), state: w.picture()}
	}
}

// picture is the rows and the pods in one line.
func (w *surgeDrainWorld) picture() string {
	var parts []string
	for _, s := range w.h.irStatuses() {
		op := "-"
		if s.Operation != nil {
			dl := "none"
			if !s.Operation.Deadline.IsZero() {
				dl = s.Operation.Deadline.Time.Sub(w.h.clk.Now()).Round(time.Second).String()
			}
			surge := ""
			if s.Operation.SurgeIndex != nil {
				surge = fmt.Sprintf(" surge=%d", *s.Operation.SurgeIndex)
			}
			op = fmt.Sprintf("%s/%s deadline=%s waiting=%q%s", s.Operation.Type, s.Operation.Step, dl, s.Operation.Waiting, surge)
		}
		parts = append(parts, fmt.Sprintf("row %d %s on %s: %s", s.Index, s.Phase, w.short(s.RunningRevision), op))
	}
	for idx, set := range w.allPods() {
		for _, pod := range set {
			state := "running"
			switch {
			case pod.DeletionTimestamp != nil:
				state = "terminating"
			case podConditionTrue(pod, corev1.PodReady) && podreadiness.IsServing(pod):
				state = "serving"
			case podreadiness.HeldNotServingBy(pod, podreadiness.WriterUpdateSurgeDrain):
				state = "unrouted"
			}
			parts = append(parts, fmt.Sprintf("pod %d/%s %s %s", idx, pod.Name, w.short(pod.Labels[query.LabelRevisionHash]), state))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

func (w *surgeDrainWorld) short(rev string) string {
	if len(rev) > 12 {
		return rev[len(rev)-8:]
	}
	return rev
}

// surgeRowFingerprint is what counts as the engine acting on a row: its
// phase, revision, attempt, step, waiting token, deadline and partner.
func surgeRowFingerprint(s workloadtypes.InstanceStatus) string {
	if s.Operation == nil {
		return fmt.Sprintf("%d/%s/%s/-", s.Index, s.Phase, s.RunningRevision)
	}
	surge := int32(-1)
	if s.Operation.SurgeIndex != nil {
		surge = *s.Operation.SurgeIndex
	}
	return fmt.Sprintf("%d/%s/%s/%s/%s/%q/%d/%d", s.Index, s.Phase, s.RunningRevision, s.Operation.ID, s.Operation.Step, s.Operation.Waiting, s.Operation.Deadline.Unix(), surge)
}

// stateKey is the rows and the pods in one string, for counting states.
func (w *surgeDrainWorld) stateKey(rows []workloadtypes.InstanceStatus, pods map[int32][]*corev1.Pod) string {
	var parts []string
	for _, s := range rows {
		parts = append(parts, surgeRowFingerprint(s))
	}
	for idx, set := range pods {
		for _, pod := range set {
			parts = append(parts, fmt.Sprintf("%d/%s/%s/%t/%t/%t", idx, pod.Name, w.short(pod.Labels[query.LabelRevisionHash]),
				pod.DeletionTimestamp != nil, podConditionTrue(pod, corev1.PodReady), podreadiness.IsServing(pod)))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "|")
}

// pairPodsOfSource splits the pods of a source Instance's pair: the old
// pods are the source index's pods off the target revision; the
// replacement's are the surge index's pods for a gang, and the source
// index's pods on the target revision for a single pod.
func (w *surgeDrainWorld) pairPodsOfSource(source workloadtypes.InstanceStatus, pods map[int32][]*corev1.Pod) (old, replacement []*corev1.Pod) {
	for _, pod := range pods[source.Index] {
		if w.onTarget(pod) && (source.Operation == nil || source.Operation.SurgeIndex == nil) {
			replacement = append(replacement, pod)
			continue
		}
		if !w.onTarget(pod) {
			old = append(old, pod)
		}
	}
	if source.Operation != nil && source.Operation.SurgeIndex != nil {
		replacement = pods[*source.Operation.SurgeIndex]
	}
	return old, replacement
}

// onTarget reports whether the pod runs the target revision, by the hash
// its label carries.
func (w *surgeDrainWorld) onTarget(pod *corev1.Pod) bool {
	return query.RevisionFromPod(pod).Same(query.RevisionFromName(w.h.target.Name))
}

// pairPods finds the pair a surge row belongs to: a source names its
// replacement through its surge index or its own target-revision pod; a
// replacement row is named by the source whose surge index it is.
func (w *surgeDrainWorld) pairPods(row workloadtypes.InstanceStatus, rows []workloadtypes.InstanceStatus, pods map[int32][]*corev1.Pod) (old, replacement []*corev1.Pod, ok bool) {
	op := row.Operation
	if op == nil || op.Type != workloadtypes.InstanceOperationUpdate {
		return nil, nil, false
	}
	switch op.Step {
	case workloadtypes.UpdateStepGangSurgeTarget, workloadtypes.UpdateStepGangSurgeTargetCleanup:
		for _, s := range rows {
			if s.Operation != nil && s.Operation.SurgeIndex != nil && *s.Operation.SurgeIndex == row.Index {
				old, replacement = w.pairPodsOfSource(s, pods)
				return old, replacement, true
			}
		}
		return nil, pods[row.Index], true
	case workloadtypes.UpdateStepSurge, workloadtypes.UpdateStepSurgeDrain, workloadtypes.UpdateStepSurgeDrainSettle:
		old, replacement = w.pairPodsOfSource(row, pods)
		return old, replacement, true
	}
	return nil, nil, false
}

// waitKind names what the pair waits on, read off its pods: the old pods
// terminating, the old pods out of rotation before their delete, the
// replacement serving while the source still serves (the readiness
// window), or a replacement not yet Ready.
func surgeWaitKind(old, replacement []*corev1.Pod) string {
	for _, pod := range old {
		if pod.DeletionTimestamp != nil {
			return "old-pods-terminating"
		}
	}
	for _, pod := range old {
		if podreadiness.HeldNotServingBy(pod, podreadiness.WriterUpdateSurgeDrain) {
			return "old-pods-unrouted"
		}
	}
	if len(replacement) > 0 && surgeAllServing(replacement) {
		return "window"
	}
	return "replacement-not-ready"
}

func surgeAllServing(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || !podConditionTrue(pod, corev1.PodReady) || !podreadiness.IsServing(pod) {
			return false
		}
	}
	return len(pods) > 0
}

func surgeAnyTerminating(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			return true
		}
	}
	return false
}

// probe reads the rows after a pass: a row past its deadline with no
// waiting token and no rollout hold must have changed since the previous
// pass; the budgets hold; no old pod leaves before its replacement serves.
func (w *surgeDrainWorld) probe() {
	w.t.Helper()
	rows := w.h.irStatuses()
	pods := w.allPods()
	w.seen[w.stateKey(rows, pods)] = struct{}{}
	passAt := w.h.clk.Now().Add(-surgeDrainStep)
	hold := w.h.hold != nil
	live := map[string]bool{}
	for _, s := range rows {
		op := s.Operation
		if op == nil {
			continue
		}
		key := fmt.Sprintf("%d/%s", s.Index, op.ID)
		old, replacement, isPair := w.pairPods(s, rows, pods)
		if op.Deadline.IsZero() {
			if op.Waiting == "" && !hold && s.Phase != workloadtypes.InstancePhaseFailed && !surgeAnyTerminating(old) {
				w.note("no-clock-without-reason", "row %d (%s) carries no deadline, no waiting token and no rollout hold while %s", s.Index, workloadtypes.StateOf(&s), surgeWaitKind(old, replacement))
			}
			continue
		}
		if !passAt.After(op.Deadline.Time) || op.Waiting != "" || hold {
			continue
		}
		w.probes++
		fp := surgeRowFingerprint(s)
		live[key] = true
		if w.prev[key] == fp {
			wait := "not-a-surge-row"
			if isPair {
				wait = surgeWaitKind(old, replacement)
			}
			w.readStandingRow(s, key, wait, passAt.Sub(op.Deadline.Time))
		}
		w.prev[key] = fp
	}
	for key := range w.prev {
		if !live[key] {
			delete(w.prev, key)
			delete(w.standing, key)
		}
	}
	w.checkBudgets(rows, pods)
}

// readStandingRow records a row that stood a full pass past its deadline
// with nothing named: the class names what the pair waits on, and a pair
// held for good by a kubelet that never reports again is a class of its
// own.
func (w *surgeDrainWorld) readStandingRow(s workloadtypes.InstanceStatus, key, wait string, stood time.Duration) {
	w.standing[key]++
	if stood > w.longest {
		w.longest = stood
	}
	class := "deadline-without-reason/" + wait
	w.note(class, "row %d (%s) passed its deadline %s ago, carries no waiting token, the Component carries no rollout hold, and the pass changed nothing on it while %s",
		s.Index, workloadtypes.StateOf(&s), stood.Round(time.Second), wait)
	if w.classStates[class] == nil {
		w.classStates[class] = map[workloadtypes.RowState]int{}
	}
	w.classStates[class][workloadtypes.StateOf(&s)]++
	if w.cfg.leaving == leaveOneLost && w.unreported != "" && stood > w.cfg.grace+6*surgeDrainStep {
		w.note("deadline-passed-nothing-moves/lost-kubelet", "row %d (%s) has stood %s past its deadline while %s waits on a kubelet that never reports again",
			s.Index, workloadtypes.StateOf(&s), stood.Round(time.Second), w.unreported)
	}
}

// checkBudgets: never more than one gang beside the fleet, and no old pod
// asked to leave while its replacement does not serve whole.
func (w *surgeDrainWorld) checkBudgets(rows []workloadtypes.InstanceStatus, pods map[int32][]*corev1.Pod) {
	var running int32
	for _, set := range pods {
		for _, pod := range set {
			if pod.DeletionTimestamp == nil {
				running++
			}
		}
	}
	if running > (w.cfg.instances+1)*w.cfg.pods {
		w.note("surge-budget", "%d pods run for %d Instances of %d pods at a surge of one", running, w.cfg.instances, w.cfg.pods)
	}
	for _, s := range rows {
		if s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationUpdate {
			continue
		}
		if s.Operation.Step != workloadtypes.UpdateStepSurge && s.Operation.Step != workloadtypes.UpdateStepSurgeDrain {
			continue
		}
		old, replacement := w.pairPodsOfSource(s, pods)
		if surgeAnyTerminating(old) && !surgeAllServing(replacement) {
			w.note("old-pods-deleted-before-replacement-serves", "row %d (%s): an old pod is terminating while its replacement set does not serve whole", s.Index, workloadtypes.StateOf(&s))
		}
	}
}

// surgeRefProbe measures the first pair of a reference run: the pass its
// replacement set was serving whole, the pass its old pods left rotation,
// the pass the first was terminating, and the pass the last was gone.
type surgeRefProbe struct {
	source                       int32
	opened                       metav1.Time
	ready, unrouted, drain, gone time.Time
}

// firstPair reads the first row that opened an Update attempt.
func (p *surgeRefProbe) firstPair(rows []workloadtypes.InstanceStatus) bool {
	if p.source >= 0 {
		return true
	}
	for _, s := range rows {
		if s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationUpdate {
			p.source, p.opened = s.Index, s.Operation.StartedAt
			return true
		}
	}
	return false
}

// pairPods splits the first pair's pods; once its source row is gone the
// old pods are the source index's pods off the target revision.
func (p *surgeRefProbe) pairPods(w *surgeDrainWorld, rows []workloadtypes.InstanceStatus, pods map[int32][]*corev1.Pod) (old, replacement []*corev1.Pod) {
	for _, s := range rows {
		if s.Index == p.source && s.Operation != nil {
			old, replacement = w.pairPodsOfSource(s, pods)
		}
	}
	if len(old) == 0 && len(replacement) == 0 {
		for _, pod := range pods[p.source] {
			if !w.onTarget(pod) {
				old = append(old, pod)
			}
		}
	}
	return old, replacement
}

// observe records the pass's moments, each once.
func (p *surgeRefProbe) observe(w *surgeDrainWorld, passAt time.Time) {
	rows := w.h.irStatuses()
	pods := w.allPods()
	if !p.firstPair(rows) {
		return
	}
	old, replacement := p.pairPods(w, rows, pods)
	if p.ready.IsZero() && int32(len(replacement)) == w.cfg.pods && surgeAllServing(replacement) {
		p.ready = passAt
	}
	if p.unrouted.IsZero() && surgeWaitKind(old, replacement) == "old-pods-unrouted" {
		p.unrouted = passAt
	}
	if p.drain.IsZero() && surgeAnyTerminating(old) {
		p.drain = passAt
	}
	if (!p.drain.IsZero() || !p.unrouted.IsZero()) && p.gone.IsZero() && len(old) == 0 {
		p.gone = passAt
	}
}

// complete reports whether every moment was measured; with no grace the
// old pods vanish on the delete pass, one pass after they left rotation.
func (p *surgeRefProbe) complete() bool {
	if p.drain.IsZero() && !p.unrouted.IsZero() {
		p.drain = p.unrouted.Add(surgeDrainStep)
	}
	return !p.opened.IsZero() && !p.ready.IsZero() && !p.drain.IsZero() && !p.gone.IsZero()
}

// surgeReference runs the shape with a deadline past every wait and the
// old pods leaving together, and measures the first pair.
func surgeReference(t *testing.T, cfg surgeDrainConfig) surgeRefTimings {
	t.Helper()
	ref := cfg
	ref.leaving, ref.moment = leaveTogether, momentNever
	w := newSurgeDrainWorld(t, ref, surgeDrainLongTimeout)
	w.h.pushRevision(fixedImage)
	p := surgeRefProbe{source: -1}
	for i := 0; i < surgeDrainTickBudget && p.gone.IsZero(); i++ {
		w.tick()
		p.observe(w, w.h.clk.Now().Add(-surgeDrainStep))
	}
	if !p.complete() {
		w.h.dumpState("reference")
		t.Fatalf("[%s] the reference run measured no pair: opened=%v ready=%v unrouted=%v drain=%v gone=%v; %s", cfg, p.opened.Time, p.ready, p.unrouted, p.drain, p.gone, w.picture())
	}
	return surgeRefTimings{ready: p.ready.Sub(p.opened.Time), drain: p.drain.Sub(p.opened.Time), gone: p.gone.Sub(p.opened.Time)}
}

// surgeTimeoutFor places the deadline by the reference timings: in the
// middle of the readiness window, in the middle of the old pods'
// termination, past everything, or at a seeded offset between the
// replacement's readiness and the pair's end.
func surgeTimeoutFor(cfg surgeDrainConfig, ref surgeRefTimings) time.Duration {
	switch cfg.moment {
	case momentInWindow:
		return ref.ready + time.Duration(cfg.window)*time.Second/2
	case momentInTermination:
		return ref.drain + time.Second + cfg.grace/2
	case momentRandom:
		rng := rand.New(rand.NewSource(cfg.seed))
		span := ref.gone + 30*time.Second - ref.ready
		return ref.ready + time.Second + time.Duration(rng.Int63n(int64(span)))
	}
	return surgeDrainLongTimeout
}

// playOut pushes the target revision and runs the roll to its end: every
// Instance on the target with no row open, held twice over; or the bound.
func (w *surgeDrainWorld) playOut() {
	w.t.Helper()
	w.h.pushRevision(fixedImage)
	target := w.h.target.Name
	settled := 0
	for i := 0; i < surgeDrainTickBudget; i++ {
		if w.settledOn(target) {
			settled++
			if settled >= 2 {
				return
			}
		} else {
			settled = 0
		}
		if w.cfg.leaving == leaveOneLost && w.unreported != "" && w.passes-w.unreportedAt > int(w.cfg.grace/surgeDrainStep)+24 {
			w.note("roll-waits-on-a-lost-kubelet", "the roll stands while %s waits on a kubelet that never reports again: %s", w.unreported, w.picture())
			return
		}
		w.tick()
	}
	if !w.settledOn(target) {
		w.note("roll-did-not-finish", "after %d passes the roll stands: %s", w.passes, w.picture())
	}
}

// knownSurgeDrainClasses are the finding classes the model shows on main
// today, each with the picture behind it. A finding in a known class is
// reported and does not fail the run; a change of behaviour removes its entry and the lock
// test that holds the picture.
var knownSurgeDrainClasses = map[string]string{
	"deadline-without-reason/old-pods-terminating": "a surge pair's deadline is written once at the pair's open and never moved by a step advance (by design: one InstanceReadyTimeout covers the create, the readiness and the drain); once the replacement serves the escalation leaves the pair alone (by design: a pod set that serves whole is never written Failed), and no token names the old pods' termination, so both rows show a passed deadline with nothing named while the old pods leave (held by TestSurgeDrainDeadline_ThePairShowsAPassedDeadlineWhileTheOldGangLeaves)",
	"deadline-without-reason/old-pods-unrouted":    "the same pair between the drain claim and the delete: the old pods are out of rotation, the delete waits on the endpoint controller, and nothing on the rows names it",
	"deadline-without-reason/window":               "the same pair while the replacement holds Ready for minReadySeconds: the source still serves, the deadline has passed, and nothing on the rows names the window (an open question: name the window as a wait on the rows, or treat the deadline as a readiness clock met at the promote bar)",
	"deadline-passed-nothing-moves/lost-kubelet":   "a drain that waits on a kubelet that never reports again holds the pair for good with nothing named (an open question for a surge slot kept by a drain that waits on a lost kubelet: keep the slot, release it past the deadline once the node reads as lost, or write a hold naming the drain)",
	"roll-waits-on-a-lost-kubelet":                 "the same open question read at the end of the run: without a force-delete policy the roll stands until the node returns",
}

// surgeDrainResult is what one configuration's run found.
type surgeDrainResult struct {
	cfg                           surgeDrainConfig
	timeout                       time.Duration
	ref                           surgeRefTimings
	states, ticks, passes, probes int
	counts                        map[string]int
	findings                      map[string]surgeFinding
	longest                       time.Duration
	took                          time.Duration
}

// surgeDrainExplore runs the configurations keep admits, one subtest per
// configuration in parallel, and reports every class, failing on the
// classes that are not known.
func surgeDrainExplore(t *testing.T, keep func(surgeDrainConfig) bool) {
	t.Helper()
	full := os.Getenv(surgeDrainFullEnv) == "full"
	only := os.Getenv(surgeDrainOnlyEnv)
	var configs []surgeDrainConfig
	for _, cfg := range surgeDrainConfigs(full) {
		if keep(cfg) && (only == "" || strings.Contains(cfg.String(), only)) {
			configs = append(configs, cfg)
		}
	}
	results := make([]surgeDrainResult, len(configs))
	start := time.Now()
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				began := time.Now()
				ref := surgeReference(t, cfg)
				timeout := surgeTimeoutFor(cfg, ref)
				w := newSurgeDrainWorld(t, cfg, timeout)
				w.playOut()
				results[i] = surgeDrainResult{cfg: cfg, timeout: timeout, ref: ref, states: len(w.seen), ticks: w.ticks, passes: w.passes, probes: w.probes,
					counts: w.counts, findings: w.findings, longest: w.longest, took: time.Since(began)}
			})
		}
	})
	reportSurgeDrain(t, results, time.Since(start))
}

type surgeExample struct {
	cfg     surgeDrainConfig
	finding surgeFinding
}

// reportSurgeDrain logs every configuration and every class with its
// first pictures, and fails on a class that is not known.
func reportSurgeDrain(t *testing.T, results []surgeDrainResult, took time.Duration) {
	t.Helper()
	var states, ticks, passes, probes int
	var longest time.Duration
	classes := map[string]int{}
	configurations := map[string]int{}
	examples := map[string][]surgeExample{}
	for n, res := range results {
		states += res.states
		ticks += res.ticks
		passes += res.passes
		probes += res.probes
		if res.longest > longest {
			longest = res.longest
		}
		var parts []string
		for class, c := range res.counts {
			classes[class] += c
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: timeout %s (ready %s, drain %s, gone %s), %d states, %d ticks, %d probes, longest unnamed %s, %s: %s",
			n+1, len(results), res.cfg, res.timeout, res.ref.ready, res.ref.drain, res.ref.gone, res.states, res.ticks, res.probes, res.longest.Round(time.Second), res.took.Round(time.Second), strings.Join(parts, " "))
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 4 {
				examples[class] = append(examples[class], surgeExample{cfg: res.cfg, finding: f})
			}
		}
	}
	names := make([]string, 0, len(classes))
	for class := range classes {
		names = append(names, class)
	}
	sort.Strings(names)
	t.Logf("%d configurations, %d states, %d ticks, %d passes, %d probes, longest unnamed deadline %s, %s", len(results), states, ticks, passes, probes, longest.Round(time.Second), took.Round(time.Second))
	for _, class := range names {
		t.Logf("%s: %d states in %d configurations", class, classes[class], configurations[class])
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.state)
		}
		if known := knownSurgeDrainClasses[class]; known != "" {
			t.Logf("  known: %s", known)
			continue
		}
		t.Errorf("%s: %d states (first: %s: %s)", class, classes[class], examples[class][0].cfg, examples[class][0].finding.message)
	}
}

// TestSurgeDrainDeadline_APassedDeadlineIsNamedOrActedOn explores every
// configuration: under every shape, grace, leaving pattern, scheduler
// timing and window, a surge pair whose deadline passes
// after its replacement serves names the wait on its rows, carries a
// rollout hold, or moves within one pass; the budgets hold; the roll
// finishes unless a kubelet never returns.
func TestSurgeDrainDeadline_APassedDeadlineIsNamedOrActedOn(t *testing.T) {
	surgeDrainExplore(t, func(surgeDrainConfig) bool { return true })
}

// TestSurgeDrainDeadline_ThePairShowsAPassedDeadlineWhileTheOldGangLeaves
// holds the current reading of the model's finding. Three gangs of two under
// SurgeThenDrain at a surge of one, a readiness window of 90 s, the old
// pods owed 40 s of termination grace, InstanceReadyTimeout placed inside
// the first pair's termination (for example 180 s against a
// replacement Ready after about 90 s and a 90 s window). Both rows
// of the pair, the old gang's at Update/SurgeDrain and the new gang's at
// Update/GangSurgeTarget, show the deadline passed for the length of the
// termination with no waiting token and no rollout hold, and the roll
// finishes. A fix that names the wait makes this test fail: remove the
// known class and this lock with it.
func TestSurgeDrainDeadline_ThePairShowsAPassedDeadlineWhileTheOldGangLeaves(t *testing.T) {
	cfg := surgeDrainConfig{pods: 2, instances: 3, grace: 40 * time.Second, leaving: leaveTogether, window: 90, moment: momentInTermination}
	ref := surgeReference(t, cfg)
	w := newSurgeDrainWorld(t, cfg, surgeTimeoutFor(cfg, ref))
	w.playOut()
	for _, class := range []string{"error", "roll-did-not-finish", "surge-budget", "old-pods-deleted-before-replacement-serves"} {
		if w.counts[class] > 0 {
			t.Fatalf("%s: %s", class, w.findings[class].message)
		}
	}
	const class = "deadline-without-reason/old-pods-terminating"
	if w.counts[class] == 0 {
		t.Fatalf("the picture moved: no row stood past its deadline unnamed while the old gang left; if the rows now name the wait, remove the known class and this lock; classes: %v", w.counts)
	}
	for _, state := range []workloadtypes.RowState{workloadtypes.StateUpdateSurgeDrain, workloadtypes.StateUpdateGangSurgeTarget} {
		if w.classStates[class][state] == 0 {
			t.Fatalf("the picture moved: the %s row no longer stands past its deadline unnamed (%v); if it names the wait now, remove the known class and this lock", state, w.classStates[class])
		}
	}
	t.Logf("the pair stood %s past its deadline with nothing named while the old gang left; %d states, %d probes: %s", w.longest.Round(time.Second), len(w.seen), w.probes, w.findings[class].message)
}
