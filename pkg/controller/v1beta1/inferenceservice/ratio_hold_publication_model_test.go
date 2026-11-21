package inferenceservice

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	testingutils "sigs.k8s.io/ome/pkg/utils/testing"
)

// This file reads the rollout hold of a prefill/decode service the way an
// operator reads it: the gate, the reason text and the Instances the text
// names on each InferenceReplica's status, tick by tick, while the
// real service pass and the real replica controller drive a
// roll on one fake apiserver (the world of
// pass_under_refused_writes_model_test.go). The hold is computed by the
// coordination gate (the batch step's wait text, the serving band, the
// budgets) and by the update pass, and written by the replica status
// writer, which keeps a hold until a pass reaches the update position
// again; every one of those is the engine's own code here.
//
// The rolls. A plain push; a push whose decoder revision crashes after it
// serves, or whose image no pod can pull, until the retry schedule holds
// the revision, then a corrected push; a node whose kubelet stops under
// about half of each side at a moment of the roll, coming back soon or
// after the force-delete policy has rebuilt its pods elsewhere; and a
// cluster that refuses room to every new pod for a while. Each on the
// reference shapes under RecreatePod, SurgeThenDrain and the keep-ratio
// setting that names the decoder as the side that moves first.
//
// The readings, at every tick. A hold names only Instances that exist on
// the side it names (a status row, or a pod present or on its way out). A
// hold clears, or changes its text, within one pass of its named condition
// clearing: the peer batch it waits on has served, the Instance it names
// as not serving serves again, the retry record it names is gone, the
// roll it belongs to has converged. The counts the text carries ("1 of 2
// serving") equal what the peer's own rows say when the holder's pass
// ran. A hold does not go from one text to another and back while no pod,
// row or spec of either side changed. A hold that waits on the other side
// names that side, a wait it names is the named row's own, and an Instance
// it lists as down has no pod serving. Four pictures are read and kept as
// known. Two are how the text is meant to read: the text
// counts a replacement whose old pods are still leaving as not serving,
// because a surge replacement settles only once its old pods are gone;
// and the text names an Instance of the other side by that side's index,
// which a reader that looks it up among the holder's own rows will not
// find. In the other two the text can mislead a reader:
// the text says an Instance is not serving while its row is
// Ready and its pods serve, because they came back and have not held
// Ready for the proving window; and a text stands while a repair holds
// the holder's pass, since such a pass ends before the update position,
// so after a corrected push it can name the previous target.
//
// OME_RATIO_HOLD_HUNT=full widens the configurations, the moments and the
// random runs; OME_RATIO_HOLD_HUNT_ONLY=<text> keeps the configurations
// whose name carries the text.

const (
	ratioHoldFullEnv = "OME_RATIO_HOLD_HUNT"
	ratioHoldOnlyEnv = "OME_RATIO_HOLD_HUNT_ONLY"
	// ratioHoldSecondNode carries the odd-numbered Instances of every
	// side, so losing it takes about half of each.
	ratioHoldSecondNode = "node-2"
	// ratioHoldNoRoom is the scheduler's answer while the cluster has no
	// room for a pod.
	ratioHoldNoRoom = "0/2 nodes are available: insufficient accelerators"
	// ratioHoldPullReason and ratioHoldPullMessage are the kubelet's answer
	// on an image it cannot pull.
	ratioHoldPullReason  = "ImagePullBackOff"
	ratioHoldPullMessage = "Back-off pulling image"
	// ratioHoldCrashAfterTicks is how long a crashing runner serves before
	// it exits: four ticks of five seconds.
	ratioHoldCrashAfterTicks = 4
	// ratioHoldSlowLeaveTicks is how long a deleted pod stays Terminating
	// under the slow-leave setting: six ticks of five seconds.
	ratioHoldSlowLeaveTicks = 6
	// ratioHoldNodeOutageTicks is the force-delete policy's threshold on a
	// node that stopped reporting, in ticks (20 s).
	ratioHoldNodeOutageTicks = 4
	// ratioHoldCrashWaitTicks bounds the wait for the crashing revision's
	// retry schedule to hold before the corrected push.
	ratioHoldCrashWaitTicks = 60
	// ratioHoldWindowTicks is how long a no-room or lost-node window
	// stands in the bounded probes.
	ratioHoldWindowTicks = 6
	// ratioHoldChartLifecycle is the chart's lifecycle block, the operator
	// config the model runs under.
	ratioHoldChartLifecycle = "ome.controller.lifecycle"
)

// holdConfig is one configuration: the service-pass world's shape and
// settings, the keep-ratio setting of the operator config, and whether a
// deleted pod leaves slowly.
type holdConfig struct {
	refusedConfig
	keepRatio bool
	slowLeave bool
}

func (c holdConfig) String() string {
	s := c.refusedConfig.String()
	if c.keepRatio {
		s += "+keepRatio"
	}
	if c.slowLeave {
		s += "+slowLeave"
	}
	return s
}

// ratioHoldConfigData renders the operator ConfigMap: the chart's lifecycle
// block, with a retry schedule of two attempts (10 s, then 20 s) so a
// broken push reaches Held inside a probe's bound, the force-delete policy
// the chart leaves unset (20 s on a node that stopped reporting, the
// grace skipped), and the chart's prefill/decode rules with the decoder
// named first (the runs that leave a side Held need one side to lead), kept or removed
// per configuration; the coordination block carries the
// default tolerance and no deadband. A runner that exits twenty seconds
// after it serves is inside the chart's pod grace, so the schedule counts
// the crash.
func ratioHoldConfigData(t *testing.T, keepRatio bool) map[string]string {
	t.Helper()
	lifecycle := map[string]any{}
	if err := testingutils.ShippedChartValue(ratioHoldChartLifecycle, &lifecycle); err != nil {
		t.Fatalf("read the chart's lifecycle block: %v", err)
	}
	lifecycle["updateRetry"] = map[string]any{"maxAttempts": 2, "initialDelay": "10s", "maxDelay": "20s", "multiplier": 2}
	lifecycle["forceDelete"] = map[string]any{"overdueSlack": "5s", "nodeUnreachableThreshold": "20s", "skipGracePeriodOnUnreachableNode": true}
	if !keepRatio {
		delete(lifecycle, "prefillDecodeRoll")
	} else if rules, set := lifecycle["prefillDecodeRoll"].(map[string]any); !set {
		t.Fatalf("the chart's lifecycle block carries no prefill/decode rules")
	} else {
		rules["firstSide"] = "decoder"
	}
	coordination := map[string]any{"trafficWeightDeadbandPercent": 0, "defaultRatioTolerancePercent": 5}
	render := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	return map[string]string{"lifecycle": render(lifecycle), "coordination": render(coordination)}
}

// ratioHoldPodGrace is the pod grace the rendered config carries, read
// through the controller's own parser: the span a pod has to prove it is not
// crash-looping, and how long a pod that came back must hold Ready before
// the roll's own reader counts it as serving again.
func ratioHoldPodGrace(t *testing.T, configData map[string]string) time.Duration {
	t.Helper()
	var lc controllerconfig.LifecycleConfig
	if err := json.Unmarshal([]byte(configData["lifecycle"]), &lc); err != nil {
		t.Fatalf("parse the rendered lifecycle block: %v", err)
	}
	grace, err := lc.ToGracePeriod()
	if err != nil || grace <= 0 {
		t.Fatalf("the rendered lifecycle block carries no pod grace: %v", err)
	}
	return grace
}

// holdWorld is the service-pass world with this model's kubelet rules and
// readings.
type holdWorld struct {
	*refusedWorld
	cfg holdConfig

	// crashImage is the image whose decoder pods exit after they serve;
	// unpullable the image no pod can pull; noRoom the cluster refusing
	// every new pod; lostNode the second node's kubelet stopped.
	crashImage string
	unpullable string
	noRoom     bool
	lostNode   bool
	// readyAt is the tick a pod's containers last turned Ready, and
	// crashedAt the tick they last exited, by pod name.
	readyAt   map[string]int
	crashedAt map[string]int
	// podGrace is the pod grace of the operator config.
	podGrace time.Duration

	// lastSides is each side as the readings saw it at the end of the
	// previous tick, and history the last three hold texts of each side
	// with the pods of both sides at the time.
	lastSides map[v1beta1.ComponentType]*holdSide
	history   map[v1beta1.ComponentType][]holdStep
	// holdTicks counts the ticks on which some side carried a hold.
	holdTicks int
	// lastRepairTick is the last tick on which the restart pass had a
	// repair open on each side, and lastRebuildTick the last tick on which
	// each Instance was under a repair, a rebuild or a scale attempt.
	lastRepairTick  map[v1beta1.ComponentType]int
	lastRebuildTick map[v1beta1.ComponentType]map[int32]int
}

// holdState is the model's bookkeeping a snapshot carries.
type holdState struct {
	crashImage, unpullable string
	noRoom, lostNode       bool
	readyAt, crashedAt     map[string]int
}

func newHoldWorld(t *testing.T, cfg holdConfig) *holdWorld {
	t.Helper()
	h := &holdWorld{cfg: cfg, readyAt: map[string]int{}, crashedAt: map[string]int{}}
	h.resetReadings()
	configData := ratioHoldConfigData(t, cfg.keepRatio)
	h.podGrace = ratioHoldPodGrace(t, configData)
	ext := &worldExtension{
		configData: configData,
		objects:    []client.Object{ratioHoldSecondNodeObject()},
		lifecycle: func(_ v1beta1.ComponentType, lc *v1beta1.LifecycleSpec) {
			lc.RestartPolicy = ptr.To(v1beta1.InstanceRestartPolicyRecreateInstance)
		},
		advance:    func(_ *refusedWorld, pod *corev1.Pod) bool { return h.advance(pod) },
		letGoAfter: h.letGoAfter,
		observe:    func(*refusedWorld) { h.observeHolds() },
		snapshot: func() any {
			return holdState{crashImage: h.crashImage, unpullable: h.unpullable, noRoom: h.noRoom, lostNode: h.lostNode,
				readyAt: copyIntMap(h.readyAt), crashedAt: copyIntMap(h.crashedAt)}
		},
		restore: func(state any) {
			s, _ := state.(holdState)
			h.crashImage, h.unpullable, h.noRoom, h.lostNode = s.crashImage, s.unpullable, s.noRoom, s.lostNode
			h.readyAt, h.crashedAt = copyIntMap(s.readyAt), copyIntMap(s.crashedAt)
			if h.readyAt == nil {
				h.readyAt = map[string]int{}
			}
			if h.crashedAt == nil {
				h.crashedAt = map[string]int{}
			}
			h.resetReadings()
		},
	}
	h.refusedWorld = newRefusedWorldWithExtension(t, cfg.refusedConfig, ext)
	return h
}

func (h *holdWorld) resetReadings() {
	h.lastSides = map[v1beta1.ComponentType]*holdSide{}
	h.history = map[v1beta1.ComponentType][]holdStep{}
	h.lastRepairTick = map[v1beta1.ComponentType]int{}
	h.lastRebuildTick = map[v1beta1.ComponentType]map[int32]int{}
}

func ratioHoldSecondNodeObject() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: ratioHoldSecondNode, UID: "node-2-uid"},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

// The kubelet, the scheduler and the node lifecycle controller of this
// model, as one rule per pod.

// nodeFor places a pod: odd-numbered Instances of the engine and the
// decoder on the second node unless it is lost, everything else on the
// first.
func (h *holdWorld) nodeFor(pod *corev1.Pod) string {
	if h.lostNode || pod.Labels[constants.OMEComponentLabel] == string(v1beta1.RouterComponent) {
		return "node-1"
	}
	idx, ok := query.InstanceIdxFromLabels(pod)
	if ok && idx%2 == 1 {
		return ratioHoldSecondNode
	}
	return "node-1"
}

// crashes reports whether the pod runs the crashing decoder image.
func (h *holdWorld) crashes(pod *corev1.Pod) bool {
	return h.crashImage != "" && podImage(pod) == h.crashImage && pod.Labels[constants.OMEComponentLabel] == string(v1beta1.DecoderComponent)
}

// cannotPull reports whether the pod runs the image no kubelet can pull.
func (h *holdWorld) cannotPull(pod *corev1.Pod) bool {
	return h.unpullable != "" && podImage(pod) == h.unpullable && pod.Labels[constants.OMEComponentLabel] == string(v1beta1.DecoderComponent)
}

func podImage(pod *corev1.Pod) string {
	if len(pod.Spec.Containers) == 0 {
		return ""
	}
	return pod.Spec.Containers[0].Image
}

// advance is the kubelet rule: the scheduler's refusal while there is no
// room, the placement, a node that stopped reporting, an image that
// cannot be pulled, the containers turning Ready on the configured
// timing, a crashing runner exiting after it served and coming back, and
// the readiness gates folded into PodReady. One status write carries what
// changed.
func (h *holdWorld) advance(pod *corev1.Pod) bool {
	now := h.clock.Now()
	changed := false
	if pod.Spec.NodeName == "" {
		if !h.place(pod, now) {
			return true
		}
		changed = true
	}
	if h.lostNode && pod.Spec.NodeName == ratioHoldSecondNode {
		// The node lifecycle controller takes Ready away from a pod whose
		// kubelet stopped reporting; nothing else of the pod moves.
		if podCondition(pod, corev1.PodReady) == corev1.ConditionTrue {
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now)
			h.writePodStatus(pod)
		}
		return true
	}
	if h.cannotPull(pod) {
		h.holdPull(pod, now)
		return true
	}
	if h.start(pod, now) {
		changed = true
	}
	if h.crashStep(pod, now) {
		changed = true
	}
	ready := corev1.ConditionFalse
	if podCondition(pod, corev1.ContainersReady) == corev1.ConditionTrue && gatesSatisfied(pod) {
		ready = corev1.ConditionTrue
	}
	if podCondition(pod, corev1.PodReady) != ready {
		setPodCondition(pod, corev1.PodReady, ready, now)
		changed = true
	}
	if changed {
		h.writePodStatus(pod)
	}
	return true
}

// place is the scheduler: while there is no room it refuses the pod and
// reports false; else it binds the pod to its node.
func (h *holdWorld) place(pod *corev1.Pod, now time.Time) bool {
	w := h.refusedWorld
	if h.noRoom {
		if podCondition(pod, corev1.PodScheduled) != corev1.ConditionFalse {
			setPodCondition(pod, corev1.PodScheduled, corev1.ConditionFalse, now)
			cond := findPodCondition(pod, corev1.PodScheduled)
			cond.Reason, cond.Message = workloadtypes.WaitingReasonUnschedulable, ratioHoldNoRoom
			pod.Status.Phase = corev1.PodPending
			h.writePodStatus(pod)
		}
		return false
	}
	pod.Spec.NodeName = h.nodeFor(pod)
	if err := w.cli.Update(w.ctx, pod); err != nil {
		w.t.Fatalf("schedule pod %s: %v", pod.Name, err)
	}
	w.scheduledAt[pod.Name] = w.tickNo
	setPodCondition(pod, corev1.PodScheduled, corev1.ConditionTrue, now)
	cond := findPodCondition(pod, corev1.PodScheduled)
	cond.Reason, cond.Message = "", ""
	pod.Status.Phase = corev1.PodPending
	return true
}

// holdPull keeps a pod whose image no kubelet can pull Pending, with the
// kubelet's reason on its container.
func (h *holdWorld) holdPull(pod *corev1.Pod, now time.Time) {
	if len(pod.Status.ContainerStatuses) > 0 {
		return
	}
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  constants.MainContainerName,
		Image: podImage(pod),
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: ratioHoldPullReason, Message: ratioHoldPullMessage}},
	}}
	setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now)
	setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now)
	h.writePodStatus(pod)
}

// start turns a placed pod's containers Ready on the configured timing
// and reports whether it did.
func (h *holdWorld) start(pod *corev1.Pod, now time.Time) bool {
	w := h.refusedWorld
	readyAfter := 1
	if w.cfg.settings.gangTiming {
		readyAfter = 2
	}
	if pod.Status.Phase != corev1.PodPending || w.tickNo-w.scheduledAt[pod.Name] < readyAfter {
		return false
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.0.0.1"
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, Image: podImage(pod), Ready: true, Started: ptr.To(true),
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)}},
	}}
	setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, now)
	h.readyAt[pod.Name] = w.tickNo
	return true
}

// crashStep: a runner on the crashing image exits after it served and the
// kubelet starts it again at once; readiness fails until it is up. It
// reports whether the pod moved.
func (h *holdWorld) crashStep(pod *corev1.Pod, now time.Time) bool {
	w := h.refusedWorld
	if pod.Status.Phase != corev1.PodRunning || !h.crashes(pod) || len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	cs := &pod.Status.ContainerStatuses[0]
	switch {
	case podCondition(pod, corev1.ContainersReady) == corev1.ConditionTrue && w.tickNo-h.readyAt[pod.Name] >= ratioHoldCrashAfterTicks:
		started := metav1.NewTime(now)
		if cs.State.Running != nil {
			started = cs.State.Running.StartedAt
		}
		cs.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", StartedAt: started, FinishedAt: metav1.NewTime(now)}}
		cs.RestartCount++
		cs.Ready = false
		cs.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)}}
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now)
		h.crashedAt[pod.Name] = w.tickNo
		return true
	case podCondition(pod, corev1.ContainersReady) == corev1.ConditionFalse && w.tickNo > h.crashedAt[pod.Name]:
		cs.Ready = true
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, now)
		h.readyAt[pod.Name] = w.tickNo
		return true
	}
	return false
}

func (h *holdWorld) writePodStatus(pod *corev1.Pod) {
	if err := h.cli.Status().Update(h.ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		h.t.Fatalf("kubelet status of %s: %v", pod.Name, err)
	}
}

func findPodCondition(pod *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == t {
			return &pod.Status.Conditions[i]
		}
	}
	return &corev1.PodCondition{}
}

// letGoAfter is how long a deleted pod stays: a force-deleted pod goes at
// once, a pod on the lost node never goes while the node is lost, and the
// rest leave after one tick or the slow-leave grace.
func (h *holdWorld) letGoAfter(pod *corev1.Pod) int {
	switch {
	case h.forceDeleted[pod.Name]:
		return 0
	case h.lostNode && pod.Spec.NodeName == ratioHoldSecondNode:
		return 1 << 20
	case h.cfg.slowLeave:
		return ratioHoldSlowLeaveTicks
	}
	return 1
}

// The world's moves beside the operator's pushes.

// loseNode stops the second node's kubelet: the node reads Ready Unknown
// from now and its pods stop reporting.
func (h *holdWorld) loseNode() {
	h.lostNode = true
	h.setSecondNodeReady(corev1.ConditionUnknown)
	h.note("cluster: the kubelet of %s stopped", ratioHoldSecondNode)
}

// nodeBack starts the second node's kubelet again.
func (h *holdWorld) nodeBack() {
	h.lostNode = false
	h.setSecondNodeReady(corev1.ConditionTrue)
	h.note("cluster: the kubelet of %s is back", ratioHoldSecondNode)
}

func (h *holdWorld) setSecondNodeReady(status corev1.ConditionStatus) {
	node := &corev1.Node{}
	if err := h.cli.Get(h.ctx, client.ObjectKey{Name: ratioHoldSecondNode}, node); err != nil {
		h.t.Fatalf("read %s: %v", ratioHoldSecondNode, err)
	}
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == corev1.NodeReady {
			node.Status.Conditions[i].Status = status
			node.Status.Conditions[i].LastTransitionTime = metav1.NewTime(h.clock.Now())
		}
	}
	if err := h.cli.Status().Update(h.ctx, node); err != nil {
		h.t.Fatalf("write %s: %v", ratioHoldSecondNode, err)
	}
}

// refuseRoom starts or ends the window in which no new pod can be placed.
func (h *holdWorld) refuseRoom(on bool) {
	h.noRoom = on
	if on {
		h.note("cluster: no room for new pods")
	} else {
		h.note("cluster: room again")
	}
}

// The readings.

// holdSide is one side as the readings see it at the end of a tick.
type holdSide struct {
	comp     v1beta1.ComponentType
	hold     *v1beta1.RolloutHold
	rows     map[int32]bool
	target   string
	promoted int32
	// servingNew counts the Instances whose pods all serve the target.
	servingNew int32
	podIdx     map[int32]bool
	podPrint   string
	// rowPrint is the rows' phases, attempts and waits, which an attempt
	// moves before any pod does.
	rowPrint   string
	converged  bool
	heldRecord bool
	// generation is the replica's spec generation, which a push moves.
	generation int64
	// notServing lists the Instances on the target that the roll's own
	// reader counts as not serving: no serving pod, or pods that came back
	// and have not held Ready for the proving window. A budget or retry
	// hold names them.
	notServing map[int32]bool
	// proving lists the Instances among them whose row is Ready and whose
	// pods all serve the target: an operator reads them as up.
	proving map[int32]bool
	// waiting is what each Instance's attempt waits on, by its row.
	waiting map[int32]string
	// anyServing lists the Instances with a routed pod serving on any
	// revision.
	anyServing map[int32]bool
	// repairing is true while the restart pass has a repair open on the
	// side (a Restart attempt, or a row Restarting): that pass ends before
	// the update position and leaves the previous hold text standing.
	repairing bool
	// rebuilding lists the Instances under a repair, a rebuild or a scale
	// attempt (any attempt but an update, or a row Creating or
	// Restarting): what their rows say moves after the text was written.
	rebuilding map[int32]bool
}

// holdStep is one tick's hold text beside the pods of both sides.
type holdStep struct {
	text, pods string
}

var (
	holdBatchRe  = regexp.MustCompile(`^(engine|decoder|router) batch (\d+) waits for (engine|decoder|router) batch (\d+) to serve( first)? \((\d+) of (\d+) serving`)
	holdNamedRe  = regexp.MustCompile(`Instances? (\d+(?:, \d+)*)(?: to (\d+))?`)
	holdClauseRe = regexp.MustCompile(`Instances? ([\d, ]+) on revision (\S+) not serving`)
	// holdDownRe is an Instance a budget hold lists as down beside the roll.
	holdDownRe = regexp.MustCompile(`Instance (\d+) not serving \(no routed pod in rotation\)`)
	// holdWaitRe is a wait note: "decoder Instance 0 Unschedulable" or
	// "decoder Instance 0 Error, starts again".
	holdWaitRe = regexp.MustCompile(`(engine|decoder|router) Instance (\d+) ([A-Za-z]+)`)
)

// holdClauseRevision is the revision a hold's not-serving clause names, ""
// when the text carries none.
func holdClauseRevision(reason string) string {
	m := holdClauseRe.FindStringSubmatch(reason)
	if m == nil {
		return ""
	}
	return m[2]
}

// holdClauseInstances parses the Instances a hold's not-serving clause
// names.
func holdClauseInstances(reason string) []int32 {
	m := holdClauseRe.FindStringSubmatch(reason)
	if m == nil {
		return nil
	}
	var out []int32
	for _, f := range strings.Split(m[1], ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err == nil {
			out = append(out, int32(n))
		}
	}
	return out
}

// namedInstance is one Instance a hold's text names, on the side the text
// qualifies it with, the holder's own when none.
type namedInstance struct {
	comp v1beta1.ComponentType
	idx  int32
	// qualified is true when the text names the side.
	qualified bool
}

// holdNamedInstances parses the Instances a hold reason names.
func holdNamedInstances(holder v1beta1.ComponentType, reason string) []namedInstance {
	var out []namedInstance
	for _, m := range holdNamedRe.FindAllStringSubmatchIndex(reason, -1) {
		comp, qualified := holder, false
		before := strings.TrimRight(reason[:m[0]], " ")
		for _, side := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
			if strings.HasSuffix(before, string(side)) {
				comp, qualified = side, true
			}
		}
		list := reason[m[2]:m[3]]
		if m[4] >= 0 {
			from, _ := strconv.Atoi(list)
			to, _ := strconv.Atoi(reason[m[4]:m[5]])
			for i := from; i <= to; i++ {
				out = append(out, namedInstance{comp: comp, idx: int32(i), qualified: qualified})
			}
			continue
		}
		for _, s := range strings.Split(list, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(s))
			if err != nil {
				continue
			}
			out = append(out, namedInstance{comp: comp, idx: int32(n), qualified: qualified})
		}
	}
	return out
}

// batchWait is a batch-step hold text as parsed.
type batchWait struct {
	me, peer       v1beta1.ComponentType
	batch, on      int32
	promoted, mark int32
}

func parseBatchWait(reason string) (batchWait, bool) {
	m := holdBatchRe.FindStringSubmatch(reason)
	if m == nil {
		return batchWait{}, false
	}
	atoi := func(s string) int32 { n, _ := strconv.Atoi(s); return int32(n) }
	return batchWait{me: v1beta1.ComponentType(m[1]), batch: atoi(m[2]), peer: v1beta1.ComponentType(m[3]), on: atoi(m[4]), promoted: atoi(m[6]), mark: atoi(m[7])}, true
}

// revisionBorn is the birth of the named revision as the roll reads it,
// zero when the revision is not found.
func (h *holdWorld) revisionBorn(name string) time.Time {
	if name == "" {
		return time.Time{}
	}
	rev := &appsv1.ControllerRevision{}
	if err := h.cli.Get(h.ctx, client.ObjectKey{Namespace: refusedNamespace, Name: name}, rev); err != nil {
		return time.Time{}
	}
	return rev.CreationTimestamp.Time
}

// podsPerInstance is the pod count an Instance of the side serves at.
func (h *holdWorld) podsPerInstance(comp v1beta1.ComponentType) int32 {
	if comp == v1beta1.RouterComponent {
		return 1
	}
	return h.cfg.shape.pods
}

// readSide reads one side's replica and pods.
func (h *holdWorld) readSide(comp v1beta1.ComponentType, pods []*corev1.Pod) *holdSide {
	s := &holdSide{comp: comp, rows: map[int32]bool{}, podIdx: map[int32]bool{}, notServing: map[int32]bool{}, proving: map[int32]bool{}, waiting: map[int32]string{}, anyServing: map[int32]bool{}, rebuilding: map[int32]bool{}}
	ir := &v1beta1.InferenceReplica{}
	if err := h.cli.Get(h.ctx, h.replicaKey(comp), ir); err != nil {
		return s
	}
	s.generation = ir.Generation
	s.hold = ir.Status.RolloutHold
	s.target = ir.Status.UpdateRevision
	if p := ir.Spec.Pacing; p != nil && p.RollbackToRevision != nil && *p.RollbackToRevision != "" {
		s.target = *p.RollbackToRevision
	}
	sp := h.readSidePods(s, pods)
	h.readSideRows(s, ir, sp)
	for _, b := range ir.Status.RetryBlocks {
		if b.TargetRevision == s.target && b.State == v1beta1.RetryBlockHeld {
			s.heldRecord = true
		}
	}
	return s
}

// sidePods is what one side's pods say, by Instance: whether every live
// pod of the Instance serves the target, and the pods themselves.
type sidePods struct {
	serving map[int32]bool
	byIdx   map[int32][]*corev1.Pod
}

// readSidePods reads the side's pods into the side and reports them by
// Instance.
func (h *holdWorld) readSidePods(s *holdSide, pods []*corev1.Pod) sidePods {
	targetHash := query.RevisionHashFromControllerRevisionName(s.target)
	sp := sidePods{serving: map[int32]bool{}, byIdx: map[int32][]*corev1.Pod{}}
	anyPod := map[int32]bool{}
	var prints []string
	for _, pod := range pods {
		if pod.Labels[constants.OMEComponentLabel] != string(s.comp) {
			continue
		}
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		s.podIdx[idx] = true
		sp.byIdx[idx] = append(sp.byIdx[idx], pod)
		prints = append(prints, fmt.Sprintf("%s:%s:%s:%v:%v:%s", pod.Name, pod.Status.Phase, podCondition(pod, corev1.PodReady), pod.DeletionTimestamp != nil, podCondition(pod, corev1.ContainersReady), pod.Spec.NodeName))
		if pod.DeletionTimestamp != nil {
			continue
		}
		if !anyPod[idx] {
			sp.serving[idx] = true
		}
		anyPod[idx] = true
		serving := pod.Status.Phase == corev1.PodRunning && podCondition(pod, corev1.PodReady) == corev1.ConditionTrue
		// A gang is in rotation through its routed pod alone.
		if serving && query.RoutedRunner(pod) {
			s.anyServing[idx] = true
		}
		if !serving || pod.Labels[query.LabelRevisionHash] != targetHash {
			sp.serving[idx] = false
		}
	}
	sort.Strings(prints)
	s.podPrint = strings.Join(prints, " ")
	return sp
}

// classifyRow reads a row as the batch step does: a gang surge target row
// counts nothing; a row running the target whose attempt, if any, does
// not move it to another revision is promoted.
func classifyRow(row *v1beta1.OMENativeInstanceStatus, target string) (surgeTarget, promoted bool) {
	op := row.Operation
	update := op != nil && op.Type == v1beta1.InstanceOperationUpdate
	if update && (op.Step == workloadtypes.UpdateStepGangSurgeTarget || op.Step == workloadtypes.UpdateStepGangSurgeTargetCleanup) {
		return true, false
	}
	movingElsewhere := update && op.TargetRevision != "" && op.TargetRevision != target
	return false, target != "" && row.RunningRevision == target && !movingElsewhere
}

// readRow records a row's presence, its wait and whether a repair, a
// rebuild or a scale attempt holds it, and prints it.
func (s *holdSide) readRow(row *v1beta1.OMENativeInstanceStatus) string {
	s.rows[row.Index] = true
	step := ""
	if op := row.Operation; op != nil {
		s.waiting[row.Index] = op.Waiting
		step = fmt.Sprintf(":%s/%s:%s", op.Type, op.Step, op.Waiting)
		if op.Type != v1beta1.InstanceOperationUpdate {
			s.rebuilding[row.Index] = true
		}
		if op.Type == v1beta1.InstanceOperationRestart {
			s.repairing = true
		}
	}
	if row.Phase == v1beta1.OMENativeInstanceRestarting {
		s.repairing = true
	}
	if row.Phase == v1beta1.OMENativeInstanceRestarting || row.Phase == v1beta1.OMENativeInstanceCreating {
		s.rebuilding[row.Index] = true
	}
	return fmt.Sprintf("%d:%s:%s%s", row.Index, row.Phase, shortRev(row.RunningRevision), step)
}

// readSideRows reads the replica's rows into the side: the promoted
// count, the Instances the roll's own reader counts as not serving, the
// ones among them that serve by every check an operator can run, and
// whether the roll converged.
func (h *holdWorld) readSideRows(s *holdSide, ir *v1beta1.InferenceReplica, sp sidePods) {
	converged := ir.Status.UpdateRevision != "" && ir.Status.CurrentRevision == ir.Status.UpdateRevision
	born := h.revisionBorn(s.target)
	window := workloadops.ProvenWindowSeconds(0, h.podGrace)
	var rowPrints []string
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		rowPrints = append(rowPrints, s.readRow(row))
		surgeTarget, promoted := classifyRow(row, s.target)
		if surgeTarget {
			continue
		}
		if promoted {
			s.promoted++
			// The roll's own reader of an Instance already on the target.
			wrow := v1beta1convert.InstanceStatusToWorkload(*row)
			rolledNotServing, _, _ := workloadops.RolledInstanceNotServing(&wrow, s.target, born, sp.byIdx[row.Index], h.podsPerInstance(s.comp), window, h.clock.Now())
			if !sp.serving[row.Index] || rolledNotServing {
				s.notServing[row.Index] = true
			}
			if sp.serving[row.Index] && rolledNotServing && row.Phase == v1beta1.OMENativeInstanceReady {
				s.proving[row.Index] = true
			}
		}
		if row.Phase != v1beta1.OMENativeInstanceReady || row.Operation != nil || row.RunningRevision != s.target {
			converged = false
		}
	}
	s.rowPrint = strings.Join(rowPrints, " ")
	for idx, serving := range sp.serving {
		if serving && s.rows[idx] {
			s.servingNew++
		}
	}
	// A roll has nothing left to do only once every set on the target
	// serves by the roll's own reader; a promoted set that crashes after
	// the roll completed keeps the roll's hold, as the status writer says.
	s.converged = converged && len(ir.Status.InstanceStatuses) > 0 && len(s.notServing) == 0
}

func (h *holdWorld) listPods() []*corev1.Pod {
	list := &corev1.PodList{}
	if err := h.cli.List(h.ctx, list, client.InNamespace(refusedNamespace)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	out := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, &list.Items[i])
	}
	return out
}

func holdText(hold *v1beta1.RolloutHold) string {
	if hold == nil {
		return ""
	}
	return string(hold.Gate) + ": " + hold.Reason
}

func pairingPeerOf(c v1beta1.ComponentType) v1beta1.ComponentType {
	if c == v1beta1.EngineComponent {
		return v1beta1.DecoderComponent
	}
	return v1beta1.EngineComponent
}

// observeHolds runs the readings at the end of a tick.
func (h *holdWorld) observeHolds() {
	pods := h.listPods()
	sides := map[v1beta1.ComponentType]*holdSide{}
	for _, c := range h.components() {
		sides[c] = h.readSide(c, pods)
	}
	// A push moves the replicas' generations, and an attempt that opens or
	// ends moves the rows, before any pod moves; the print carries both
	// beside the pods.
	engine, decoder := sides[v1beta1.EngineComponent], sides[v1beta1.DecoderComponent]
	pairPods := fmt.Sprintf("gen %d/%d rows %s | %s pods %s | %s", engine.generation, decoder.generation, engine.rowPrint, decoder.rowPrint, engine.podPrint, decoder.podPrint)
	held := false
	for _, c := range h.components() {
		s := sides[c]
		if s.hold != nil {
			held = true
		}
		if s.repairing {
			h.lastRepairTick[c] = h.tickNo
		}
		for idx := range s.rebuilding {
			if h.lastRebuildTick[c] == nil {
				h.lastRebuildTick[c] = map[int32]int{}
			}
			h.lastRebuildTick[c][idx] = h.tickNo
		}
		h.readNamedInstances(s, sides)
		h.readWaitNotesOf(s, sides)
		h.readStaleHold(s, sides)
		h.readFlap(c, holdText(s.hold), pairPods)
	}
	if held {
		h.holdTicks++
	}
	h.lastSides = sides
}

// readNamedInstances: every Instance a hold names exists on the side it
// names; a named Instance of the other side that is not among the
// holder's rows is the known picture a reader must not misread.
func (h *holdWorld) readNamedInstances(s *holdSide, sides map[v1beta1.ComponentType]*holdSide) {
	if s.hold == nil {
		return
	}
	if (s.hold.Gate == v1beta1.RolloutHoldGateRatio || s.hold.Gate == v1beta1.RolloutHoldGatePairing || s.hold.Gate == v1beta1.RolloutHoldGateHeld) &&
		!strings.Contains(s.hold.Reason, string(pairingPeerOf(s.comp))) && !strings.Contains(s.hold.Reason, string(s.comp)) {
		h.record("hold-names-no-side", "%s hold %s names no side: %s", s.comp, s.hold.Gate, s.hold.Reason)
	}
	for _, n := range holdNamedInstances(s.comp, s.hold.Reason) {
		named := sides[n.comp]
		if named == nil || (!named.rows[n.idx] && !named.podIdx[n.idx]) {
			h.record("hold-names-a-gone-instance", "%s hold %s names %s Instance %d, which has no row and no pod: %s", s.comp, s.hold.Gate, n.comp, n.idx, s.hold.Reason)
			continue
		}
		if n.qualified && n.comp != s.comp && !s.rows[n.idx] && !s.podIdx[n.idx] {
			h.record("hold-names-a-peer-instance-absent-from-the-holders-rows", "%s hold %s names %s Instance %d, an index the %s has no row or pod for: %s", s.comp, s.hold.Gate, n.comp, n.idx, s.comp, s.hold.Reason)
		}
	}
}

// readWaitNotes: a wait a hold names ("decoder Instance 0 Unschedulable")
// is the wait that Instance's own row carried at one end of the tick,
// since the holder's pass may have run before the row moved; a note of an
// Instance that starts again is read by its row alone.
func (h *holdWorld) readWaitNotes(s *holdSide, sides map[v1beta1.ComponentType]*holdSide) {
	reason := s.hold.Reason
	matches := holdWaitRe.FindAllStringSubmatchIndex(reason, -1)
	for i, m := range matches {
		rest := reason[m[1]:]
		if i+1 < len(matches) {
			rest = reason[m[1]:matches[i+1][0]]
		}
		if end := strings.IndexAny(rest, ";)"); end >= 0 {
			rest = rest[:end]
		}
		if strings.Contains(rest, "starts again") {
			continue
		}
		comp, token := v1beta1.ComponentType(reason[m[2]:m[3]]), reason[m[6]:m[7]]
		idx, _ := strconv.Atoi(reason[m[4]:m[5]])
		got, before := "", ""
		if named := sides[comp]; named != nil {
			got = named.waiting[int32(idx)]
		}
		if named := h.lastSides[comp]; named != nil {
			before = named.waiting[int32(idx)]
		}
		if got != token && before != token {
			h.record("hold-names-a-wait-the-row-does-not-carry", "%s hold says %s Instance %d waits on %s while that row waited on %q and %q at the two ends of the tick: %s", s.comp, comp, idx, token, before, got, reason)
		}
	}
}

// readStaleHold: a hold whose named condition cleared at the end of the
// previous tick and still cleared now has changed its text or gone by the
// end of this tick; its counts equal the peer's rows; a converged roll
// carries no hold one tick on.
func (h *holdWorld) readStaleHold(s *holdSide, sides map[v1beta1.ComponentType]*holdSide) {
	prev := h.lastSides[s.comp]
	if prev == nil {
		return
	}
	text := holdText(s.hold)
	unchanged := text != "" && text == holdText(prev.hold)
	if prev.converged && s.converged && s.hold != nil {
		h.record("hold-stands-after-the-roll-converged", "%s converged on %s and still publishes %s", s.comp, shortRev(s.target), text)
	}
	if s.hold == nil {
		return
	}
	if wait, ok := parseBatchWait(s.hold.Reason); ok {
		h.readBatchWait(s, wait, unchanged, sides)
		return
	}
	if s.hold.Gate == v1beta1.RolloutHoldGateHeld && unchanged && !prev.heldRecord && !s.heldRecord && !strings.Contains(s.hold.Reason, "and the") {
		h.record("held-hold-outlives-its-record", "%s publishes a Held hold one pass after its record left: %s", s.comp, s.hold.Reason)
	}
	h.readNotServingClause(s, prev, unchanged)
}

// readWaitNotesOf runs the wait-note reading once the previous tick is known.
func (h *holdWorld) readWaitNotesOf(s *holdSide, sides map[v1beta1.ComponentType]*holdSide) {
	if s.hold == nil || h.lastSides[s.comp] == nil {
		return
	}
	h.readWaitNotes(s, sides)
}

// recordStale records a reading of a text that stood through the tick
// against a state that moved, or the known picture when the previous
// text is allowed to stand. A reading about the holder as a whole (a
// batch count, a wait on the peer) is allowed while the restart pass has
// a repair open on the holder, at either end of the tick, and for the two
// passes after the last pass that saw it: those passes end before the
// update position. A reading about one named Instance (idx at least
// zero) is allowed while that Instance is under a repair, a rebuild or a
// scale attempt, at either end of the tick, and for the two passes after
// the last pass that saw it: the pass that settles a rebuilt set, and the
// one after it, write nothing at the update position either. Nothing
// else is allowed.
func (h *holdWorld) recordStale(s, prev *holdSide, idx int32, class, format string, args ...any) {
	if h.previousTextStands(s, prev, idx) {
		h.record("hold-stands-while-a-repair-consumes-the-pass", "%s within two passes of a repair on the %s: "+format, append([]any{class, s.comp}, args...)...)
		return
	}
	h.record(class, format, args...)
}

// previousTextStands is the allowance recordStale describes.
func (h *holdWorld) previousTextStands(s, prev *holdSide, idx int32) bool {
	if idx >= 0 {
		last, seen := h.lastRebuildTick[s.comp][idx]
		return s.rebuilding[idx] || prev.rebuilding[idx] || (seen && h.tickNo-last <= 2)
	}
	last, seen := h.lastRepairTick[s.comp]
	return s.repairing || prev.repairing || (seen && h.tickNo-last <= 2)
}

// readNotServingClause reads the "Instance N on revision X not serving"
// clause of a budget or retry hold: it names the roll's target; it names
// an Instance the roll's own reader counted as not serving at one end of
// the tick; and an Instance it names whose row is Ready and whose pods
// serve, up again inside the proving window, is the known picture. An
// Instance the hold lists as down beside the roll ("no routed pod in
// rotation") has no pod serving at one end of the tick.
func (h *holdWorld) readNotServingClause(s, prev *holdSide, unchanged bool) {
	for _, m := range holdDownRe.FindAllStringSubmatch(s.hold.Reason, -1) {
		n, _ := strconv.Atoi(m[1])
		idx := int32(n)
		if unchanged && prev.anyServing[idx] && s.anyServing[idx] {
			h.recordStale(s, prev, idx, "hold-names-a-down-instance-that-serves", "%s hold lists Instance %d as down with no routed pod in rotation while a pod of it served at both ends of the tick: %s", s.comp, idx, s.hold.Reason)
		}
	}
	rev := holdClauseRevision(s.hold.Reason)
	if rev == "" {
		return
	}
	if rev != s.target {
		if unchanged && prev.target == s.target {
			// The clause's Instances are the ones a repair may be rebuilding.
			idx := int32(-1)
			if named := holdClauseInstances(s.hold.Reason); len(named) > 0 {
				idx = named[0]
			}
			h.recordStale(s, prev, idx, "hold-names-a-previous-target", "%s hold still names revision %s one pass after the roll's target moved to %s: %s", s.comp, shortRev(rev), shortRev(s.target), s.hold.Reason)
		}
		return
	}
	for _, idx := range holdClauseInstances(s.hold.Reason) {
		if !s.rows[idx] {
			continue
		}
		if s.proving[idx] {
			h.record("hold-names-a-proving-instance-as-not-serving", "%s hold names Instance %d as not serving while its row is Ready and its pods serve the target; its pods came back and have not held Ready for the proving window: %s", s.comp, idx, s.hold.Reason)
		}
		if unchanged && !prev.notServing[idx] && !s.notServing[idx] {
			h.recordStale(s, prev, idx, "hold-outlives-its-not-serving-instance", "%s hold names Instance %d as not serving one pass after it served again: %s", s.comp, idx, s.hold.Reason)
			return
		}
	}
}

// readBatchWait reads a batch-step hold whose text stood through the tick
// against the peer it names: the peer's batch had not served at either
// end, the count is the peer's promoted rows at one end, and the count is
// not short of the peer's Instances whose pods have served the new
// revision at both ends.
func (h *holdWorld) readBatchWait(s *holdSide, wait batchWait, unchanged bool, sides map[v1beta1.ComponentType]*holdSide) {
	peerPrev, peerNow := h.lastSides[wait.peer], sides[wait.peer]
	if !unchanged || peerPrev == nil || peerNow == nil {
		return
	}
	prev := h.lastSides[s.comp]
	if peerPrev.promoted >= wait.mark && peerNow.promoted >= wait.mark {
		h.recordStale(s, prev, -1, "hold-outlives-its-peer-batch-serving", "%s still waits on %s batch %d (%d of %d serving) one pass after %s promoted %d: %s", s.comp, wait.peer, wait.on, wait.promoted, wait.mark, wait.peer, peerNow.promoted, s.hold.Reason)
	}
	if wait.promoted != peerPrev.promoted && wait.promoted != peerNow.promoted && peerPrev.target == peerNow.target {
		h.recordStale(s, prev, -1, "hold-counts-stale-against-the-peers-rows", "%s hold counts %d of %d while %s has %d promoted rows at both ends of the tick: %s", s.comp, wait.promoted, wait.mark, wait.peer, peerNow.promoted, s.hold.Reason)
	}
	if wait.promoted < peerPrev.servingNew && wait.promoted < peerNow.servingNew && wait.promoted < wait.mark {
		h.record("hold-counts-lag-the-pods", "%s hold counts %d of %d serving while %d %s Instances have had every pod serving the new revision for two ticks: %s", s.comp, wait.promoted, wait.mark, peerNow.servingNew, wait.peer, s.hold.Reason)
	}
}

// readFlap: no hold goes from one text to another and back while no pod,
// row or spec of either side changed.
func (h *holdWorld) readFlap(c v1beta1.ComponentType, text, pods string) {
	steps := append(h.history[c], holdStep{text: text, pods: pods})
	if len(steps) > 3 {
		steps = steps[len(steps)-3:]
	}
	h.history[c] = steps
	if len(steps) < 3 {
		return
	}
	a, b, back := steps[0], steps[1], steps[2]
	if a.text == back.text && a.text != b.text && a.pods == b.pods && b.pods == back.pods {
		h.record("hold-flaps-without-a-pod-change", "%s hold went from %q to %q and back with no pod change on either side", c, a.text, b.text)
	}
}

// The probes.

type holdProbeKind uint8

const (
	holdProbePlain holdProbeKind = iota
	holdProbeCrashFixAtHeld
	holdProbeCrashFixEarly
	holdProbeUnpullableFixAtHeld
	holdProbeNoRoom
	holdProbeNodeLost
	holdProbeNodeStays
	holdProbeKindCount
)

func (k holdProbeKind) String() string {
	return [...]string{"plain push", "decoder crashes after serving, corrected once held", "decoder crashes after serving, corrected at the first crash",
		"decoder image cannot be pulled, corrected once held", "no room for new pods", "a node's kubelet stops and comes back", "a node's kubelet stops until its pods are rebuilt"}[k]
}

// holdProbe is one probe: its kind and, for the windows, the moment of
// the roll it opens at and the ticks it stands.
type holdProbe struct {
	kind   holdProbeKind
	moment refusedMoment
	window int
}

func (p holdProbe) String() string {
	switch p.kind {
	case holdProbeNoRoom, holdProbeNodeLost, holdProbeNodeStays:
		return fmt.Sprintf("%s at %s for %d ticks", p.kind, p.moment, p.window)
	}
	return p.kind.String()
}

func (p holdProbe) fromIdle() bool {
	return p.kind != holdProbeNoRoom && p.kind != holdProbeNodeLost && p.kind != holdProbeNodeStays
}

// decoderHoldGate is the gate of the decoder's hold, "" when none.
func (h *holdWorld) decoderHoldGate() v1beta1.RolloutHoldGate {
	ir := &v1beta1.InferenceReplica{}
	if err := h.cli.Get(h.ctx, h.replicaKey(v1beta1.DecoderComponent), ir); err != nil || ir.Status.RolloutHold == nil {
		return ""
	}
	return ir.Status.RolloutHold.Gate
}

// decoderCrashed reports whether a decoder pod has exited after serving.
func (h *holdWorld) decoderCrashed() bool {
	return len(h.crashedAt) > 0
}

// playUntil ticks until stop answers true or the bound passes, and reports
// whether it stopped on the condition.
func (h *holdWorld) playUntil(maxTicks int, stop func() bool) bool {
	for i := 0; i < maxTicks; i++ {
		h.tick()
		if stop() {
			return true
		}
	}
	return false
}

// run plays one probe from the snapshot of its moment and classifies the
// end.
func (h *holdWorld) run(p holdProbe, snap *refusedSnapshot, maxTicks int) {
	h.restore(snap)
	h.path = p.String()
	h.probes++
	switch p.kind {
	case holdProbePlain:
		h.push(refusedImageV2)
	case holdProbeCrashFixAtHeld, holdProbeCrashFixEarly, holdProbeUnpullableFixAtHeld:
		if p.kind == holdProbeUnpullableFixAtHeld {
			h.unpullable = refusedImageV2
		} else {
			h.crashImage = refusedImageV2
		}
		h.push(refusedImageV2)
		stop := func() bool { return h.decoderHoldGate() == v1beta1.RolloutHoldGateHeld }
		if p.kind == holdProbeCrashFixEarly {
			stop = h.decoderCrashed
		}
		if !h.playUntil(h.crashWaitTicks(), stop) {
			h.record("broken-push-never-reached-the-wait-for-the-correction", "the decoder's revision never reached the state the probe waits for before the corrected push: %s", h.holdText())
		}
		if p.kind == holdProbeCrashFixEarly {
			h.tick()
			h.tick()
		}
		h.push(refusedImageV3)
	case holdProbeNoRoom:
		h.refuseRoom(true)
		h.playUntil(p.window, func() bool { return false })
		h.refuseRoom(false)
	case holdProbeNodeLost:
		h.loseNode()
		h.playUntil(p.window, func() bool { return false })
		h.nodeBack()
	case holdProbeNodeStays:
		h.loseNode()
		h.playUntil(p.window+2*ratioHoldNodeOutageTicks, func() bool { return false })
		h.nodeBack()
	}
	h.classifyEnd(h.playOut(maxTicks))
}

// crashWaitTicks bounds the wait for a broken push's retry schedule to
// hold: twice the base on gangs and under the slow leave, whose attempts
// take longer to end.
func (h *holdWorld) crashWaitTicks() int {
	if h.cfg.settings.gangTiming || h.cfg.slowLeave {
		return 2 * ratioHoldCrashWaitTicks
	}
	return ratioHoldCrashWaitTicks
}

// holdProbes lists the probes of a run.
func holdProbes(full bool) []holdProbe {
	out := []holdProbe{{kind: holdProbePlain}, {kind: holdProbeCrashFixAtHeld}, {kind: holdProbeCrashFixEarly}, {kind: holdProbeUnpullableFixAtHeld}}
	moments := []refusedMoment{atSettling, atLast}
	windows := []int{ratioHoldWindowTicks}
	if full {
		moments = []refusedMoment{atSettling, atRotating, atDraining, atLast}
		windows = []int{2, ratioHoldWindowTicks, 2 * ratioHoldWindowTicks}
	}
	for _, m := range moments {
		for _, win := range windows {
			out = append(out, holdProbe{kind: holdProbeNoRoom, moment: m, window: win}, holdProbe{kind: holdProbeNodeLost, moment: m, window: win})
		}
		out = append(out, holdProbe{kind: holdProbeNodeStays, moment: m, window: ratioHoldWindowTicks})
	}
	return out
}

func holdTickBound(full bool) int {
	if full {
		return 360
	}
	return 140
}

// explore runs the probes from the moments of one plain roll, then random
// runs that mix a broken push with the windows, under a fixed seed.
func (h *holdWorld) explore(full bool, seed int64, runs int) {
	h.t.Helper()
	maxTicks := holdTickBound(full)
	h.reachIdle()
	snaps := h.captureMoments(maxTicks)
	for _, p := range holdProbes(full) {
		snap := snaps[atIdle]
		if !p.fromIdle() {
			snap = snaps[p.moment]
		}
		if snap == nil {
			continue
		}
		h.run(p, snap, maxTicks)
	}
	h.randomRuns(snaps[atIdle], seed, runs, maxTicks)
}

// randomWindowKind is what an open window of a random run is.
type randomWindowKind uint8

const (
	windowNone randomWindowKind = iota
	windowNoRoom
	windowNodeLost
)

// randomWindow is an open window of a random run and the tick it closes.
type randomWindow struct {
	kind randomWindowKind
	ends int
}

// closeWindow ends an open window.
func (h *holdWorld) closeWindow(w *randomWindow) {
	switch w.kind {
	case windowNoRoom:
		h.refuseRoom(false)
	case windowNodeLost:
		h.nodeBack()
	}
	*w = randomWindow{}
}

// randomRuns plays a push, broken or plain, with up to three windows at
// random ticks and the corrected push when the push was broken.
func (h *holdWorld) randomRuns(idle *refusedSnapshot, seed int64, runs, maxTicks int) {
	h.t.Helper()
	rng := rand.New(rand.NewSource(seed))
	for run := 0; run < runs; run++ {
		h.restore(idle)
		h.path = fmt.Sprintf("random run %d", run+1)
		h.probes++
		h.randomRun(rng, maxTicks)
	}
}

// randomRun plays one random run from the restored idle world.
func (h *holdWorld) randomRun(rng *rand.Rand, maxTicks int) {
	broken := rng.Intn(3)
	switch broken {
	case 1:
		h.crashImage = refusedImageV2
	case 2:
		h.unpullable = refusedImageV2
	}
	h.push(refusedImageV2)
	left, corrected := 3, broken == 0
	var window randomWindow
	settledTicks := 0
	for i := 0; i < maxTicks; i++ {
		if window.kind != windowNone && h.tickNo >= window.ends {
			h.closeWindow(&window)
		}
		if left > 0 && window.kind == windowNone && rng.Intn(6) == 0 {
			left--
			corrected = h.randomMove(rng, &window, corrected)
		}
		if !corrected && i == maxTicks/2 {
			h.push(refusedImageV3)
			corrected = true
		}
		h.tick()
		if window.kind == windowNone && h.previous.settled {
			settledTicks++
			if settledTicks >= 2 {
				break
			}
		} else {
			settledTicks = 0
		}
	}
	if window.kind != windowNone {
		h.closeWindow(&window)
		h.classifyEnd(h.playOut(maxTicks / 2))
		return
	}
	h.classifyEnd(settledTicks >= 2)
}

// randomMove opens a window, or pushes the correction when the push was
// broken and none has been pushed; it reports whether the correction has
// been pushed.
func (h *holdWorld) randomMove(rng *rand.Rand, window *randomWindow, corrected bool) bool {
	switch rng.Intn(3) {
	case 0:
		h.refuseRoom(true)
		*window = randomWindow{kind: windowNoRoom, ends: h.tickNo + 2 + rng.Intn(ratioHoldWindowTicks)}
	case 1:
		h.loseNode()
		*window = randomWindow{kind: windowNodeLost, ends: h.tickNo + 2 + rng.Intn(2*ratioHoldWindowTicks)}
	case 2:
		if !corrected {
			h.push(refusedImageV3)
			return true
		}
	}
	return corrected
}

// Configurations: the reference prefill/decode shapes and their gang
// twins, under RecreatePod and SurgeThenDrain and the keep-ratio setting.

func holdBoundedConfigs() []holdConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	group := func(prefill, decode int32, pods, routers int32, p, d v1beta1.UpdateStrategyType, keepRatio, gang bool) holdConfig {
		return holdConfig{refusedConfig: refusedConfig{shape: refusedShape{prefill, decode, pods, routers}, settings: refusedSettings{prefill: p, decode: d, group: true, maintainRatio: true, gangTiming: gang}}, keepRatio: keepRatio}
	}
	return []holdConfig{
		group(12, 8, 1, 0, recreate, recreate, false, false),
		group(12, 8, 1, 0, surge, surge, true, false),
		group(16, 8, 1, 0, surge, surge, true, false),
		group(6, 2, 1, 0, recreate, recreate, false, false),
		{refusedConfig: group(2, 2, 2, 2, surge, surge, true, true).refusedConfig, keepRatio: true, slowLeave: true},
		group(6, 2, 3, 0, recreate, recreate, false, true),
	}
}

func holdConfigs(full bool) []holdConfig {
	configs := holdBoundedConfigs()
	if !full {
		return configs
	}
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	shapes := []refusedShape{{12, 8, 1, 0}, {16, 8, 1, 0}, {15, 15, 1, 0}, {20, 10, 1, 0}, {12, 4, 1, 0}, {5, 5, 1, 0}, {6, 2, 1, 0}, {2, 2, 2, 2}, {4, 4, 2, 0}, {6, 2, 3, 0}, {3, 3, 3, 0}}
	pairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate}}
	seen := map[string]bool{}
	for _, c := range configs {
		seen[c.String()] = true
	}
	for _, shape := range shapes {
		for _, pair := range pairs {
			for _, keepRatio := range []bool{false, true} {
				for _, slow := range []bool{false, true} {
					cfg := holdConfig{refusedConfig: refusedConfig{shape: shape, settings: refusedSettings{prefill: pair[0], decode: pair[1], group: true, maintainRatio: true, gangTiming: shape.pods > 1}}, keepRatio: keepRatio, slowLeave: slow}
					if slow && shape.pods == 1 {
						continue
					}
					if !seen[cfg.String()] {
						seen[cfg.String()] = true
						configs = append(configs, cfg)
					}
				}
			}
		}
	}
	return configs
}

// knownHoldClasses are the pictures the model shows, each with the
// rule behind it. A finding in a known class is reported and does not
// fail the run.
var knownHoldClasses = map[string]string{
	"hold-counts-lag-the-pods":                                "a surge replacement counts as serving its batch only once the old pods it replaces are gone, so while they leave the text counts it as not serving although every new pod serves; the hold clears on the pass after the old pods are gone",
	"hold-names-a-peer-instance-absent-from-the-holders-rows": "a hold names an Instance of the other side by that side's own index; the index is not an Instance of the holder, and a reader that looks it up among the holder's rows will not find it",
	"hold-names-a-proving-instance-as-not-serving":            "a budget or retry hold says an Instance is not serving while its row is Ready and its pods serve, because its pods came back from a crash or a rebuild and have not held Ready for the proving window; an operator reads the Instance as up and the text as wrong; this model leaves open whether the text should say so",
	"hold-stands-while-a-repair-consumes-the-pass":            "a pass the restart pass takes for a repair, and the passes that settle a rebuilt set, end before the update position and leave the previous hold text standing: while a repair runs on the holder the text can lag the peer's rows by several passes, and while the named Instance is being rebuilt, and for two passes after, the text can name it as it was and, after a corrected push, name the previous target; this model leaves open whether the status writer should clear or refresh a hold whose target has left",
	"unavailable-budget-exceeded/none":                        "the world's own loss (a crashing runner, a lost node, no room) takes Instances down beside the roll; the reference roll with nothing broken is held to the budgets when the moments are captured",
	"surge-budget-exceeded/none":                              "the world's own loss (a crashing runner, a lost node, no room) takes Instances down beside the roll; the reference roll with nothing broken is held to the budgets when the moments are captured",
}

var holdExpectedEnds = map[string]bool{"complete": true}

type holdResult struct {
	cfg      holdConfig
	states   int
	ticks    int
	probes   int
	holds    int
	classes  map[string]int
	findings map[string]refusedFinding
	took     time.Duration
}

type holdExample struct {
	cfg     holdConfig
	finding refusedFinding
}

// ratioHoldExplore runs the model over the configurations keep admits, one
// subtest per configuration in parallel, and reports every class, failing
// on the classes that are neither expected ends nor known.
func ratioHoldExplore(t *testing.T, keep func(holdConfig) bool) {
	t.Helper()
	full := os.Getenv(ratioHoldFullEnv) == "full"
	only := os.Getenv(ratioHoldOnlyEnv)
	start := time.Now()
	var configs []holdConfig
	for _, cfg := range holdConfigs(full) {
		if keep(cfg) && (only == "" || strings.Contains(cfg.String(), only)) {
			configs = append(configs, cfg)
		}
	}
	results := make([]holdResult, len(configs))
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				began := time.Now()
				runs := 2
				if full {
					runs = 6
				}
				h := newHoldWorld(t, cfg)
				h.explore(full, 1, runs)
				results[i] = holdResult{cfg: cfg, states: len(h.seen), ticks: h.ticks, probes: h.probes, holds: h.holdTicks, classes: h.classes, findings: h.findings, took: time.Since(began)}
			})
		}
	})
	totals := map[string]int{}
	configurations := map[string]int{}
	examples := map[string][]holdExample{}
	var states, ticks, probes, holds int
	for n, res := range results {
		states += res.states
		ticks += res.ticks
		probes += res.probes
		holds += res.holds
		var parts []string
		for class, c := range res.classes {
			totals[class] += c
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: %d states, %d ticks, %d probes, %d held ticks, %s: %s", n+1, len(configs), res.cfg, res.states, res.ticks, res.probes, res.holds, res.took.Round(time.Second), strings.Join(parts, " "))
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 4 {
				examples[class] = append(examples[class], holdExample{cfg: res.cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d ticks, %d probes, %d held ticks, %s", len(configs), states, ticks, probes, holds, time.Since(start).Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d ticks in %d configurations", class, totals[class], configurations[class])
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		switch {
		case holdExpectedEnds[class]:
		case knownHoldClasses[class] != "":
			t.Logf("  known: %s", knownHoldClasses[class])
		default:
			t.Errorf("%s: %d ticks (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
		}
	}
}

// TestRatioHoldPublication_EveryHoldReadsTrueAtEveryTick explores every
// configuration: through a plain push, a push whose decoder revision
// crashes or cannot be pulled and is then corrected, a lost node and a
// cluster with no room, every published hold names Instances that exist,
// clears within one pass of its condition, carries the peer's own counts,
// does not flap without a pod change, and names the side it waits on.
func TestRatioHoldPublication_EveryHoldReadsTrueAtEveryTick(t *testing.T) {
	ratioHoldExplore(t, func(holdConfig) bool { return true })
}

// Locks: each states one invariant in plain words on one reference
// shape and fails when the engine stops keeping it.

// holdLockWorld builds the configuration and rolls it to idle at version 1.
func holdLockWorld(t *testing.T, cfg holdConfig) (*holdWorld, map[refusedMoment]*refusedSnapshot) {
	t.Helper()
	h := newHoldWorld(t, cfg)
	h.reachIdle()
	snaps := h.captureMoments(holdTickBound(false))
	return h, snaps
}

func (h *holdWorld) failOnClass(t *testing.T, class, why string) {
	t.Helper()
	if n := h.classes[class]; n > 0 {
		t.Fatalf("%s on %d ticks: %s\n  %s\n  path: %s", class, n, why, h.findings[class].message, h.findings[class].path)
	}
}

func holdLockConfig(prefill, decode, pods, routers int32, p, d v1beta1.UpdateStrategyType, keepRatio, gang, slow bool) holdConfig {
	return holdConfig{refusedConfig: refusedConfig{shape: refusedShape{prefill, decode, pods, routers}, settings: refusedSettings{prefill: p, decode: d, group: true, maintainRatio: true, gangTiming: gang}}, keepRatio: keepRatio, slowLeave: slow}
}

// TestRatioHoldPublication_ABatchHoldClearsOnThePassAfterThePeerBatchServes:
// 12 prefill and 8 decode under RecreatePod in one group with the ratio
// rule. While the roll runs, a side waits for the other side's batch; one
// pass after that batch's Instances run the new revision, the hold has
// changed its text or gone. The roll shows the wait, so the picture stands.
func TestRatioHoldPublication_ABatchHoldClearsOnThePassAfterThePeerBatchServes(t *testing.T) {
	h, _ := holdLockWorld(t, holdLockConfig(12, 8, 1, 0, v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategyRecreatePod, false, false, false))
	if h.holdTicks == 0 {
		t.Fatalf("the picture moved: no side carried a hold during the roll\n%s", strings.Join(h.trace, "\n"))
	}
	h.failOnClass(t, "hold-outlives-its-peer-batch-serving", "a hold stood one full pass after the peer batch it waits on had served")
	h.failOnClass(t, "hold-stands-after-the-roll-converged", "a hold stood one pass after its roll converged")
}

// TestRatioHoldPublication_TheHoldCountsMatchThePeersPromotedRows: on the
// same roll, and on the 16:8 surging pair under the keep-ratio setting,
// the "P of M serving" a hold carries is the count of the peer's Instances
// running the new revision as the holder's pass read them.
func TestRatioHoldPublication_TheHoldCountsMatchThePeersPromotedRows(t *testing.T) {
	for _, cfg := range []holdConfig{
		holdLockConfig(12, 8, 1, 0, v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategyRecreatePod, false, false, false),
		holdLockConfig(16, 8, 1, 0, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategySurgeThenDrain, true, false, false),
	} {
		t.Run(cfg.String(), func(t *testing.T) {
			h, _ := holdLockWorld(t, cfg)
			if h.holdTicks == 0 {
				t.Fatalf("the picture moved: no side carried a hold during the roll\n%s", strings.Join(h.trace, "\n"))
			}
			h.failOnClass(t, "hold-counts-stale-against-the-peers-rows", "a hold carried counts the peer's rows did not show at either end of the tick")
		})
	}
}

// TestRatioHoldPublication_ANoRoomHoldNamesInstancesThatExist: 16 prefill
// and 8 decode surging under the keep-ratio setting. Once the decoder's
// fifth batch has settled and the prefill side's fifth batch, Instances 8
// and 9, is about to start, the cluster refuses room to every new pod.
// The decoder's next batch waits on those prefill Instances, and its hold
// names them by the prefill side's own index and by the wait their rows
// carry. Every named Instance exists on the side named, and every named
// wait is that row's own; the named indices are no Instances of the
// decoder, which is what a reader must know.
func TestRatioHoldPublication_ANoRoomHoldNamesInstancesThatExist(t *testing.T) {
	h, snaps := holdLockWorld(t, holdLockConfig(16, 8, 1, 0, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategySurgeThenDrain, true, false, false))
	h.restore(snaps[atIdle])
	h.path = "no room as the prefill side's fifth batch starts"
	h.push(refusedImageV2)
	fifthBatchNext := func() bool {
		pods := h.listPods()
		return h.readSide(v1beta1.DecoderComponent, pods).promoted >= 5 && h.readSide(v1beta1.EngineComponent, pods).promoted >= 8
	}
	if !h.playUntil(holdTickBound(false), fifthBatchNext) {
		t.Fatalf("the roll never reached its fifth batch: %s\n%s", h.holdText(), strings.Join(h.trace, "\n"))
	}
	h.refuseRoom(true)
	const named = "hold-names-a-peer-instance-absent-from-the-holders-rows"
	h.playUntil(5*ratioHoldWindowTicks, func() bool { return h.classes[named] > 0 })
	h.playUntil(2, func() bool { return false })
	h.refuseRoom(false)
	h.classifyEnd(h.playOut(holdTickBound(false)))
	if h.classes[named] == 0 {
		t.Fatalf("the picture moved: no hold named an Instance of the other side by an index the holder lacks\n%s", strings.Join(h.trace, "\n"))
	}
	h.failOnClass(t, "hold-names-a-gone-instance", "a hold named an Instance with no row and no pod on the side it names")
	h.failOnClass(t, "hold-names-a-wait-the-row-does-not-carry", "a hold named a wait the Instance's own row does not carry")
	h.failOnClass(t, "holds-forever/none", "the roll did not finish once the room came back")
}

// TestRatioHoldPublication_AReplacementKeepsTheHoldCountUntilTheOldGangLeaves:
// 2 prefill gangs and 2 decode gangs of a leader and a worker behind 2
// routers, surging under the keep-ratio setting, the old gang taking
// thirty seconds to leave. The decoder's replacement gang serves while its
// old gang is still leaving; the engine's hold counts that decoder
// Instance as not serving until the old gang is gone, and clears on the
// pass after it is. This is the rule: a surge replacement settles
// only once its old pods are gone.
func TestRatioHoldPublication_AReplacementKeepsTheHoldCountUntilTheOldGangLeaves(t *testing.T) {
	h, _ := holdLockWorld(t, holdLockConfig(2, 2, 2, 2, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategySurgeThenDrain, true, true, true))
	if h.classes["hold-counts-lag-the-pods"] == 0 {
		t.Fatalf("the picture moved: no hold counted a serving replacement as not serving while its old gang left\n%s", strings.Join(h.trace, "\n"))
	}
	h.failOnClass(t, "hold-outlives-its-peer-batch-serving", "a hold stood one full pass after the peer batch it waits on had served")
	h.failOnClass(t, "hold-counts-stale-against-the-peers-rows", "a hold carried counts the peer's rows did not show")
}

// TestRatioHoldPublication_NoHoldStandsAfterACorrectedPushConverges: the
// same gang shape; the pushed decoder revision exits twenty seconds after
// it serves, every time, until its retry schedule holds it; the engine
// waits on the decoder with a hold that says so. The operator pushes a
// corrected revision; once every Instance serves it, no side carries a
// hold one pass later.
func TestRatioHoldPublication_NoHoldStandsAfterACorrectedPushConverges(t *testing.T) {
	h, snaps := holdLockWorld(t, holdLockConfig(2, 2, 2, 2, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategySurgeThenDrain, true, true, true))
	h.run(holdProbe{kind: holdProbeCrashFixAtHeld}, snaps[atIdle], holdTickBound(false))
	h.failOnClass(t, "broken-push-never-reached-the-wait-for-the-correction", "the crashing revision never reached Held, so the probe does not exercise the corrected push from a held roll")
	h.failOnClass(t, "holds-forever/none", "the corrected push did not land on every Instance")
	h.failOnClass(t, "hold-stands-after-the-roll-converged", "a hold stood one pass after the corrected push converged")
	for _, c := range h.components() {
		ir := &v1beta1.InferenceReplica{}
		if err := h.cli.Get(h.ctx, h.replicaKey(c), ir); err != nil {
			t.Fatalf("read %s: %v", c, err)
		}
		if ir.Status.RolloutHold != nil {
			t.Fatalf("%s still publishes a hold after the corrected push converged: %s", c, holdText(ir.Status.RolloutHold))
		}
	}
}

// TestRatioHoldPublication_APushBackClearsTheHeldHold: the gang shape
// under the keep-ratio setting, the decoder first. No kubelet can pull
// the pushed decoder image, so the decoder's first gang never serves, the
// retry schedule holds the revision after two attempts, and the engine,
// which never started, says it waits on the held decoder. The operator
// pushes version 1 back instead of a correction. Neither side has an
// Instance to move; one pass later no side carries a hold.
func TestRatioHoldPublication_APushBackClearsTheHeldHold(t *testing.T) {
	h, snaps := holdLockWorld(t, holdLockConfig(2, 2, 2, 2, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategySurgeThenDrain, true, true, true))
	h.restore(snaps[atIdle])
	h.path = "decoder image cannot be pulled, version 1 pushed back once held"
	h.unpullable = refusedImageV2
	h.push(refusedImageV2)
	if !h.playUntil(h.crashWaitTicks(), func() bool { return h.decoderHoldGate() == v1beta1.RolloutHoldGateHeld }) {
		t.Fatalf("the crashing revision never reached Held: %s\n%s", h.holdText(), strings.Join(h.trace, "\n"))
	}
	h.push(refusedImageV1)
	h.classifyEnd(h.playOut(holdTickBound(false)))
	h.failOnClass(t, "holds-forever/none", "the push back did not land on every Instance")
	h.failOnClass(t, "hold-stands-after-the-roll-converged", "a hold stood one pass after the push back converged")
	for _, c := range h.components() {
		ir := &v1beta1.InferenceReplica{}
		if err := h.cli.Get(h.ctx, h.replicaKey(c), ir); err != nil {
			t.Fatalf("read %s: %v", c, err)
		}
		if ir.Status.RolloutHold != nil {
			t.Fatalf("%s still publishes a hold after the push back converged: %s", c, holdText(ir.Status.RolloutHold))
		}
	}
}

// TestRatioHoldPublication_AHoldDoesNotFlapWithoutAPodChange: 12 prefill
// and 8 decode under RecreatePod; through the plain roll and a lost node
// at its first batch, no hold goes from one text to another and back
// while no pod, row or spec of either side changed, and every hold that
// waits on the other side names it.
func TestRatioHoldPublication_AHoldDoesNotFlapWithoutAPodChange(t *testing.T) {
	h, snaps := holdLockWorld(t, holdLockConfig(12, 8, 1, 0, v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategyRecreatePod, false, false, false))
	h.failOnClass(t, "hold-flaps-without-a-pod-change", "a hold flapped during the plain roll")
	h.run(holdProbe{kind: holdProbeNodeLost, moment: atSettling, window: ratioHoldWindowTicks}, snaps[atSettling], holdTickBound(false))
	h.failOnClass(t, "hold-flaps-without-a-pod-change", "a hold flapped while a node was lost")
	h.failOnClass(t, "hold-names-no-side", "a hold that waits on the other side did not name it")
}

// decoderRow is the decoder's row at idx as the replica publishes it, nil
// when there is none.
func (h *holdWorld) decoderRow(idx int32) *v1beta1.OMENativeInstanceStatus {
	ir := &v1beta1.InferenceReplica{}
	if err := h.cli.Get(h.ctx, h.replicaKey(v1beta1.DecoderComponent), ir); err != nil {
		return nil
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == idx {
			return &ir.Status.InstanceStatuses[i]
		}
	}
	return nil
}

// podNamed is the pod of that name, nil when there is none.
func (h *holdWorld) podNamed(name string) *corev1.Pod {
	pod := &corev1.Pod{}
	if err := h.cli.Get(h.ctx, client.ObjectKey{Namespace: refusedNamespace, Name: name}, pod); err != nil {
		return nil
	}
	return pod
}

// rowPrint is one row in the words of the trace.
func rowPrint(row *v1beta1.OMENativeInstanceStatus) string {
	if row == nil {
		return "no row"
	}
	s := fmt.Sprintf("%s run=%s tgt=%s", row.Phase, shortRev(row.RunningRevision), shortRev(row.TargetRevision))
	if op := row.Operation; op != nil {
		s += fmt.Sprintf(" %s/%s waiting=%q toward=%s", op.Type, op.Step, op.Waiting, shortRev(op.TargetRevision))
	}
	return s
}

// TestRatioHoldPublication_ARebuiltSurgeSourceBesideItsFailedReplacement:
// 15 prefill and 15 decode surging in one group with the ratio rule. The
// pushed decoder image cannot be pulled. Decoder Instance 1's surge attempt
// is open when its node stops reporting; the force-delete policy removes
// its version 1 pod while the version 2 replacement waits in
// ImagePullBackOff. The attempt fails twice, version 2 is held, and the
// engine rebuilds Instance 1 on version 1 beside the replacement the last
// attempt left at the other ordinal. Once the rebuilt pod runs with its
// containers Ready, the next passes put it into service and the row reads
// Ready on version 1. The corrected push then lands on every Instance and
// leaves no hold.
func TestRatioHoldPublication_ARebuiltSurgeSourceBesideItsFailedReplacement(t *testing.T) {
	h := newHoldWorld(t, holdLockConfig(15, 15, 1, 0, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategySurgeThenDrain, false, false, false))
	h.reachIdle()
	h.path = "decoder image cannot be pulled; the node under decoder Instance 1 stops while its surge attempt is open"
	h.unpullable = refusedImageV2
	h.push(refusedImageV2)
	row := func() *v1beta1.OMENativeInstanceStatus { return h.decoderRow(1) }
	surging := func() bool {
		r := row()
		return r != nil && r.Operation != nil && r.Operation.Type == v1beta1.InstanceOperationUpdate
	}
	if !h.playUntil(4*ratioHoldWindowTicks, surging) {
		t.Fatalf("decoder Instance 1 never opened its surge attempt: %s\n%s", h.holdText(), strings.Join(h.trace, "\n"))
	}
	h.loseNode()
	h.playUntil(ratioHoldWindowTicks, func() bool { return false })
	h.nodeBack()
	rebuilding := func() bool {
		r := row()
		return h.decoderHoldGate() == v1beta1.RolloutHoldGateHeld && r != nil && r.Phase == v1beta1.OMENativeInstanceCreating &&
			r.Operation != nil && r.Operation.Type == v1beta1.InstanceOperationCreate
	}
	if !h.playUntil(h.crashWaitTicks(), rebuilding) {
		t.Fatalf("the picture moved: version 2 was never held with decoder Instance 1 under a Create attempt; the row reads %s\n%s", rowPrint(row()), strings.Join(h.trace, "\n"))
	}
	name := query.PodName(refusedService, workloadtypes.ComponentDecoder, 1, workloadtypes.RunnerDefault, 0)
	running := func() bool {
		pod := h.podNamed(name)
		return pod != nil && podCondition(pod, corev1.ContainersReady) == corev1.ConditionTrue
	}
	if !h.playUntil(2*ratioHoldWindowTicks, running) {
		t.Fatalf("the rebuilt pod %s never ran with its containers Ready; the row reads %s\n%s", name, rowPrint(row()), strings.Join(h.trace, "\n"))
	}
	serving := func() bool {
		pod, r := h.podNamed(name), row()
		return pod != nil && podCondition(pod, corev1.PodReady) == corev1.ConditionTrue && r != nil && r.Phase == v1beta1.OMENativeInstanceReady
	}
	if !h.playUntil(ratioHoldWindowTicks, serving) {
		pod := h.podNamed(name)
		t.Fatalf("decoder Instance 1's rebuilt pod %s ran with its containers Ready for %d passes and was never put into service: PodReady %s, serving gate %v; the row reads %s; hold: %s\n%s",
			name, ratioHoldWindowTicks, podCondition(pod, corev1.PodReady), podreadiness.IsServing(pod), rowPrint(row()), h.holdText(), strings.Join(h.trace[len(h.trace)-6:], "\n"))
	}
	h.push(refusedImageV3)
	h.classifyEnd(h.playOut(holdTickBound(false)))
	h.failOnClass(t, "holds-forever/none", "the corrected push did not land on every Instance")
	h.failOnClass(t, "hold-stands-after-the-roll-converged", "a hold stood one pass after the corrected push converged")
}
