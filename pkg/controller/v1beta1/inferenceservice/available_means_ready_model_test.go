package inferenceservice

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	testingutils "sigs.k8s.io/ome/pkg/utils/testing"
)

// This file reads what a replica counts as Available, Ready and Serving
// while a pod is listed by its Component's headless Service and is not
// Ready. The real service pass and the real replica controller
// drive a prefill/decode roll on one fake apiserver (the world of
// pass_under_refused_writes_model_test.go), so every count and every gate
// is the engine's own code: the publication (status.AvailablePodSet,
// status.CountAvailablePods and status.CountersForInstance through
// workload.ComponentObservation.TakeInlineV1Publication), the roll's own
// budget (updateAdmission.admit over escalation.BudgetDenies with
// ops.InstanceNotServingOutsideRoll and ops.RolledInstanceNotServing), the
// group's budget and the prefill/decode pairing
// (coordination.EvaluateUpdateGate), and the Ready conditions of the
// replica (computeReadyCondition) and of the service
// (status.TopLevelComponentReadyFromLifecycle).
//
// The cluster of this model lists pods as the EndpointSlice controller
// does. A pod is listed from the moment it has an address, which the
// kubelet gives it when it is placed, until it is gone. A Service that
// publishes not-ready addresses, as the Component's headless Service does
// for peer discovery, lists every such pod as ready; every other Service
// lists a pod as ready only while the pod is Ready and is not being
// deleted. So a pod that starts, a pod whose readiness probe fails and a
// pod whose node stopped reporting are all listed by the headless Service
// and not Ready.
//
// The moves. Every runner that starts takes 15 s to pass its readiness
// probe; every runner that comes up fails its probe once, 5 s after it
// first passed; one pod's probe fails for a minute, or for good, at a
// moment of the roll; a node's kubelet stops for a minute, and the node
// lifecycle controller takes Ready away from its pods and changes nothing
// else on them; the manager restarts as a probe fails and again as it
// recovers. Each on single pods and on gangs, under RecreatePod,
// SurgeThenDrain and InPlaceIfPossible, with a minReadySeconds window of
// 0 and of 30 s.
//
// The readings, at every tick. A replica never publishes more Available
// Instances than it had Instances whose every pod was Ready, for the
// window, when its pass ran, and never fewer than the Instances that stood
// so for two ticks. A drain-first roll takes a serving Instance out only
// while the Instances that serve stay within the budget, whoever took the
// others down; a surge takes one out only once its replacement serves. A
// gang that serves through its leader beside a worker that is not Ready
// is read as short of a member, apart by whether the roll has moved it. A
// replica does not read every Instance Ready while one of them has served
// nothing for two ticks, and no Ready condition goes from one reading to
// another and back while no pod changed. Every roll ends once the pods
// are Ready again; a roll beside a pod that never is again ends with every
// other Instance moved, or says what it waits on.
//
// OME_AVAILABLE_MEANS_READY_HUNT=full widens the shapes, the moments and
// the random runs; OME_AVAILABLE_MEANS_READY_HUNT_ONLY=<text> keeps the
// configurations whose name carries the text.

const (
	availableHuntEnv = "OME_AVAILABLE_MEANS_READY_HUNT"
	availableOnlyEnv = "OME_AVAILABLE_MEANS_READY_HUNT_ONLY"
	// availablePodIP is the address a pod's sandbox gets when the pod is
	// placed, before any container runs.
	availablePodIP = "10.0.0.1"
	// availableMinuteTicks is a minute of five-second ticks: how long a
	// failing readiness probe or a stopped kubelet stands in the probes.
	availableMinuteTicks = 12
	// availableSlowStartTicks is how long a started runner takes to pass
	// its readiness probe under the slow start: 15 s.
	availableSlowStartTicks = 3
	// availableFlapTicks is how long a runner that just came up fails its
	// probe again under the comeback flap: 10 s.
	availableFlapTicks = 2
	// availableWindowSeconds is the minReadySeconds window the chart names
	// for an operator who opts in.
	availableWindowSeconds = 30
	// availableForGood is a probe failure that outlasts every run.
	availableForGood = 1 << 20
	// availableChartCoordination is the chart's coordination block.
	availableChartCoordination = "ome.controller.coordination"
)

// availableConfig is one configuration: the service-pass world's shape and
// settings, and the minReadySeconds window of both sides.
type availableConfig struct {
	refusedConfig
	window int32
}

func (c availableConfig) String() string {
	s := c.refusedConfig.String()
	if c.window > 0 {
		s += fmt.Sprintf("+window%ds", c.window)
	}
	return s
}

// availableConfigData renders the operator ConfigMap from the chart as it
// ships: its lifecycle block (a pod grace of a minute, the retry schedule,
// no force-delete policy, the prefill/decode rules) and its coordination
// block.
func availableConfigData(t *testing.T) map[string]string {
	t.Helper()
	render := func(path string) string {
		block := map[string]any{}
		if err := testingutils.ShippedChartValue(path, &block); err != nil {
			t.Fatalf("read the chart's %s block: %v", path, err)
		}
		b, err := json.Marshal(block)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	return map[string]string{"lifecycle": render(ratioHoldChartLifecycle), "coordination": render(availableChartCoordination)}
}

// availableWorld is the service-pass world with this model's cluster rules
// and readings.
type availableWorld struct {
	*refusedWorld
	cfg availableConfig
	// podGrace is the pod grace of the operator config.
	podGrace time.Duration

	// slowStart makes every runner that starts take the slow start to pass
	// its probe; flapTicks, when positive, makes every runner that comes up
	// fail its probe once for that long; lostNode is the second node's
	// kubelet stopped.
	slowStart bool
	flapTicks int
	lostNode  bool
	// passesFrom is the tick from which a runner's readiness probe passes,
	// and failFrom and failUntil the ticks between which it fails again,
	// by pod UID.
	passesFrom, failFrom, failUntil map[types.UID]int
	// flapped names the runners that already failed once after coming up.
	flapped map[types.UID]bool
	// stoppedFrom and stoppedUntil bound the ticks the kubelet under one
	// pod is stopped, by pod UID: a node of its own that stopped reporting.
	stoppedFrom, stoppedUntil map[types.UID]int
	// nodeBackAt and restartAt are the ticks a stopped kubelet comes back
	// and the manager restarts again; zero when neither is due.
	nodeBackAt, restartAt int
	// broken names the pod a probe keeps unready, for the end reading.
	broken types.UID

	// before is each side at the end of the previous tick and beforeThat
	// at the end of the tick before it; history is, per Ready condition,
	// its two readings before the current one.
	before, beforeThat map[v1beta1.ComponentType]*availableSide
	history            map[string]availableHistory
	// listedNotReady counts the ticks that ended with a pod listed as
	// ready by a headless Service while the pod was not Ready.
	listedNotReady int
	// listedShort counts the passes that read an Instance with every pod
	// listed and one of them not Ready; insideWindow the passes that read
	// a pod Ready for less than the window; waitedBeside the passes that
	// took no serving Instance out of a side still rolling while the
	// cluster kept one of its Instances from serving.
	listedShort, insideWindow, waitedBeside int
}

// availableState is the model's bookkeeping a snapshot carries.
type availableState struct {
	slowStart                       bool
	flapTicks                       int
	lostNode                        bool
	passesFrom, failFrom, failUntil map[types.UID]int
	stoppedFrom, stoppedUntil       map[types.UID]int
	flapped                         map[types.UID]bool
}

func copyUIDTicks(in map[types.UID]int) map[types.UID]int {
	out := make(map[types.UID]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func newAvailableWorld(t *testing.T, cfg availableConfig) *availableWorld {
	t.Helper()
	a := &availableWorld{cfg: cfg, passesFrom: map[types.UID]int{}, failFrom: map[types.UID]int{}, failUntil: map[types.UID]int{},
		stoppedFrom: map[types.UID]int{}, stoppedUntil: map[types.UID]int{}, flapped: map[types.UID]bool{}}
	a.resetReadings()
	configData := availableConfigData(t)
	a.podGrace = ratioHoldPodGrace(t, configData)
	ext := &worldExtension{
		configData: configData,
		objects:    []client.Object{ratioHoldSecondNodeObject()},
		lifecycle: func(_ v1beta1.ComponentType, lc *v1beta1.LifecycleSpec) {
			lc.RestartPolicy = ptr.To(v1beta1.InstanceRestartPolicyRecreateInstance)
			if cfg.window > 0 {
				lc.MinReadySeconds = ptr.To(cfg.window)
			}
		},
		advance:     func(_ *refusedWorld, pod *corev1.Pod) bool { return a.advance(pod) },
		letGoAfter:  a.letGoAfter,
		endpoint:    publishedEndpoint,
		afterPasses: func(*refusedWorld) { a.readPasses() },
		observe:     func(*refusedWorld) { a.readTick() },
		snapshot: func() any {
			flapped := make(map[types.UID]bool, len(a.flapped))
			for k, v := range a.flapped {
				flapped[k] = v
			}
			return availableState{slowStart: a.slowStart, flapTicks: a.flapTicks, lostNode: a.lostNode,
				passesFrom: copyUIDTicks(a.passesFrom), failFrom: copyUIDTicks(a.failFrom), failUntil: copyUIDTicks(a.failUntil),
				stoppedFrom: copyUIDTicks(a.stoppedFrom), stoppedUntil: copyUIDTicks(a.stoppedUntil), flapped: flapped}
		},
		restore: func(state any) {
			s, _ := state.(availableState)
			a.slowStart, a.flapTicks, a.lostNode = s.slowStart, s.flapTicks, s.lostNode
			a.passesFrom, a.failFrom, a.failUntil = copyUIDTicks(s.passesFrom), copyUIDTicks(s.failFrom), copyUIDTicks(s.failUntil)
			a.stoppedFrom, a.stoppedUntil = copyUIDTicks(s.stoppedFrom), copyUIDTicks(s.stoppedUntil)
			a.flapped = make(map[types.UID]bool, len(s.flapped))
			for k, v := range s.flapped {
				a.flapped[k] = v
			}
			a.nodeBackAt, a.restartAt, a.broken = 0, 0, ""
			a.resetReadings()
		},
	}
	a.refusedWorld = newRefusedWorldWithExtension(t, cfg.refusedConfig, ext)
	return a
}

func (a *availableWorld) resetReadings() {
	a.before, a.beforeThat = nil, nil
	a.history = map[string]availableHistory{}
}

// publishedEndpoint is the EndpointSlice controller's rule for one pod a
// Service selects. A pod with an address that has not run to an end is
// listed, with what the pod says of itself: serving while the pod is
// Ready, terminating once its deletion began. It is listed as ready while
// it serves and is not terminating, and whatever it says when the Service
// publishes not-ready addresses.
func publishedEndpoint(svc *corev1.Service, pod *corev1.Pod) (discoveryv1.Endpoint, bool) {
	if pod.Status.PodIP == "" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return discoveryv1.Endpoint{}, false
	}
	serving := podCondition(pod, corev1.PodReady) == corev1.ConditionTrue
	terminating := pod.DeletionTimestamp != nil
	ready := svc.Spec.PublishNotReadyAddresses || (serving && !terminating)
	return discoveryv1.Endpoint{
		Addresses:  []string{pod.Status.PodIP},
		Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(ready), Serving: ptr.To(serving), Terminating: ptr.To(terminating)},
		TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
	}, true
}

// The kubelet, the scheduler and the node lifecycle controller of this
// model, as one rule per pod.

// nodeFor places a pod: the odd-numbered Instances of the engine and the
// decoder on the second node, everything else on the first.
func (a *availableWorld) nodeFor(pod *corev1.Pod) string {
	if pod.Labels[constants.OMEComponentLabel] == string(v1beta1.RouterComponent) {
		return "node-1"
	}
	if idx, ok := query.InstanceIdxFromLabels(pod); ok && idx%2 == 1 {
		return ratioHoldSecondNode
	}
	return "node-1"
}

// advance is the cluster's rule for one pod: the placement, which gives
// the pod its address; a node that stopped reporting, whose pods lose
// Ready and keep everything else; the runner starting; the runner
// restarting on a patched image; the readiness probe; and the readiness
// gates folded into PodReady. One status write carries what changed.
func (a *availableWorld) advance(pod *corev1.Pod) bool {
	w := a.refusedWorld
	now := w.clock.Now()
	changed := false
	if pod.Spec.NodeName == "" {
		pod.Spec.NodeName = a.nodeFor(pod)
		if err := w.cli.Update(w.ctx, pod); err != nil {
			w.t.Fatalf("schedule pod %s: %v", pod.Name, err)
		}
		w.scheduledAt[pod.Name] = w.tickNo
		setPodCondition(pod, corev1.PodScheduled, corev1.ConditionTrue, now)
		pod.Status.Phase = corev1.PodPending
		pod.Status.PodIP = availablePodIP
		changed = true
	}
	if a.kubeletStopped(pod) {
		// The node lifecycle controller sets Ready to False on a pod whose
		// kubelet stopped reporting. It writes no reason and moves nothing
		// else: the containers' readiness and the serving gate keep their
		// last values.
		if podCondition(pod, corev1.PodReady) == corev1.ConditionTrue {
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now)
			changed = true
		}
		if changed {
			a.writePodStatus(pod)
		}
		return true
	}
	changed = a.startRunner(pod, now) || changed
	changed = a.restartOnPatchedImage(pod, now) || changed
	changed = a.probeRunner(pod, now) || changed
	ready := corev1.ConditionFalse
	if podCondition(pod, corev1.ContainersReady) == corev1.ConditionTrue && gatesSatisfied(pod) {
		ready = corev1.ConditionTrue
	}
	if podCondition(pod, corev1.PodReady) != ready {
		setPodCondition(pod, corev1.PodReady, ready, now)
		changed = true
	}
	if changed {
		a.writePodStatus(pod)
	}
	return true
}

// kubeletStopped reports whether the kubelet under the pod is stopped: the
// second node's while it is lost, or the pod's own for the ticks a probe
// stopped it.
func (a *availableWorld) kubeletStopped(pod *corev1.Pod) bool {
	if a.lostNode && pod.Spec.NodeName == ratioHoldSecondNode {
		return true
	}
	until, set := a.stoppedUntil[pod.UID]
	return set && a.tickNo >= a.stoppedFrom[pod.UID] && a.tickNo < until
}

// startRunner starts a placed pod's runner on the configured timing. The
// runner passes its readiness probe at once, or after the slow start.
func (a *availableWorld) startRunner(pod *corev1.Pod, now time.Time) bool {
	w := a.refusedWorld
	startAfter := 1
	if w.cfg.settings.gangTiming {
		startAfter = 2
	}
	if pod.Status.Phase != corev1.PodPending || w.tickNo-w.scheduledAt[pod.Name] < startAfter {
		return false
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, Image: podImage(pod), Started: ptr.To(true),
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)}},
	}}
	setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now)
	a.passesFrom[pod.UID] = w.tickNo
	if a.slowStart {
		a.passesFrom[pod.UID] = w.tickNo + availableSlowStartTicks
	}
	return true
}

// restartOnPatchedImage restarts a runner whose pod names another image
// than the one it runs: the kubelet's answer to an in-place patch. The new
// runner passes its probe from the next tick, or after the slow start.
func (a *availableWorld) restartOnPatchedImage(pod *corev1.Pod, now time.Time) bool {
	w := a.refusedWorld
	if pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	cs := &pod.Status.ContainerStatuses[0]
	if cs.Image == podImage(pod) {
		return false
	}
	started := metav1.NewTime(now)
	if cs.State.Running != nil {
		started = cs.State.Running.StartedAt
	}
	cs.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed", StartedAt: started, FinishedAt: metav1.NewTime(now)}}
	cs.RestartCount++
	cs.Ready = false
	cs.Image = podImage(pod)
	cs.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)}}
	setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now)
	a.passesFrom[pod.UID] = w.tickNo + 1
	if a.slowStart {
		a.passesFrom[pod.UID] = w.tickNo + availableSlowStartTicks
	}
	delete(a.flapped, pod.UID)
	return true
}

// failing reports whether the model keeps the pod's readiness probe
// failing at this tick.
func (a *availableWorld) failing(uid types.UID) bool {
	until, set := a.failUntil[uid]
	return set && a.tickNo >= a.failFrom[uid] && a.tickNo < until
}

// probeRunner is the readiness probe: it passes once the runner is up and
// while the model does not fail it. Under the comeback flap a runner that
// just passed fails again from the next tick, once.
func (a *availableWorld) probeRunner(pod *corev1.Pod, now time.Time) bool {
	if pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	cs := &pod.Status.ContainerStatuses[0]
	passes := a.tickNo >= a.passesFrom[pod.UID] && !a.failing(pod.UID)
	want := corev1.ConditionFalse
	if passes {
		want = corev1.ConditionTrue
	}
	if podCondition(pod, corev1.ContainersReady) == want && cs.Ready == passes {
		return false
	}
	cs.Ready = passes
	setPodCondition(pod, corev1.ContainersReady, want, now)
	if passes && a.flapTicks > 0 && !a.flapped[pod.UID] {
		a.flapped[pod.UID] = true
		a.failFrom[pod.UID], a.failUntil[pod.UID] = a.tickNo+1, a.tickNo+1+a.flapTicks
	}
	return true
}

func (a *availableWorld) writePodStatus(pod *corev1.Pod) {
	if err := a.cli.Status().Update(a.ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		a.t.Fatalf("kubelet status of %s: %v", pod.Name, err)
	}
}

// letGoAfter is how long a deleted pod stays: a force-deleted pod goes at
// once, a pod on the stopped node stays while its kubelet is stopped, and
// the rest leave after one tick.
func (a *availableWorld) letGoAfter(pod *corev1.Pod) int {
	switch {
	case a.forceDeleted[pod.Name]:
		return 0
	case a.kubeletStopped(pod):
		return availableForGood
	}
	return 1
}

// The cluster's moves beside the operator's pushes.

func (a *availableWorld) listPods() []*corev1.Pod {
	list := &corev1.PodList{}
	if err := a.cli.List(a.ctx, list, client.InNamespace(refusedNamespace)); err != nil {
		a.t.Fatalf("list pods: %v", err)
	}
	out := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, &list.Items[i])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// loseNode stops the second node's kubelet: the node reads Ready Unknown
// from now and its pods stop reporting.
func (a *availableWorld) loseNode() {
	a.lostNode = true
	a.setSecondNodeReady(corev1.ConditionUnknown)
	a.note("cluster: the kubelet of %s stopped", ratioHoldSecondNode)
}

// nodeBack starts the second node's kubelet again.
func (a *availableWorld) nodeBack() {
	a.lostNode = false
	a.nodeBackAt = 0
	a.setSecondNodeReady(corev1.ConditionTrue)
	a.note("cluster: the kubelet of %s is back", ratioHoldSecondNode)
}

func (a *availableWorld) setSecondNodeReady(status corev1.ConditionStatus) {
	node := &corev1.Node{}
	a.mustNode("read", a.cli.Get(a.ctx, client.ObjectKey{Name: ratioHoldSecondNode}, node))
	for i := range node.Status.Conditions {
		if cond := &node.Status.Conditions[i]; cond.Type == corev1.NodeReady {
			cond.Status, cond.LastTransitionTime = status, metav1.NewTime(a.clock.Now())
		}
	}
	a.mustNode("write", a.cli.Status().Update(a.ctx, node))
}

// mustNode fails the run when a read or a write of the second node fails.
func (a *availableWorld) mustNode(what string, err error) {
	a.t.Helper()
	if err != nil {
		a.t.Fatalf("%s %s: %v", what, ratioHoldSecondNode, err)
	}
}

// slowEveryStart makes every runner that starts from now take the slow
// start to pass its probe.
func (a *availableWorld) slowEveryStart() {
	a.slowStart = true
	a.note("cluster: every runner that starts takes %d ticks to pass its readiness probe", availableSlowStartTicks)
}

// failEveryComebackOnce makes every runner that comes up from now fail its probe once;
// the runners already up are left alone.
func (a *availableWorld) failEveryComebackOnce(ticks int) {
	a.flapTicks = ticks
	for _, pod := range a.listPods() {
		a.flapped[pod.UID] = true
	}
	a.note("cluster: every runner that comes up fails its readiness probe once, for %d ticks", ticks)
}

// availableTarget names the pod a probe breaks.
type availableTarget uint8

const (
	// availableDecodeOld is the routed pod of a decode Instance the roll
	// has not moved, availableDecodeNew of one it has; availablePrefillOld
	// is a prefill routed pod the roll has not moved; availableDecodeWorker
	// and availableDecodeWorkerNew are a worker of a decode gang the roll
	// has not moved and of one it has; availableDecodeWorkerLast is a worker
	// of the last decode gang the roll has not moved, the gang the roll
	// starts on last.
	availableDecodeOld availableTarget = iota
	availableDecodeNew
	availablePrefillOld
	availableDecodeWorker
	availableDecodeWorkerNew
	availableDecodeWorkerLast
	availableTargetCount
)

func (t availableTarget) String() string {
	return [...]string{"a decode pod the roll has not moved", "a decode pod the roll has moved", "a prefill pod the roll has not moved",
		"a worker of a decode gang the roll has not moved", "a worker of a decode gang the roll has moved",
		"a worker of the last decode gang the roll has not moved"}[t]
}

// targetPod is the live pod the target names: of its side, the routed pod
// or a worker of the lowest Instance whose pod runs the image the target
// asks for and is Ready, or of the highest one for the last gang. Before
// the push every pod is one the roll has not moved.
func (a *availableWorld) targetPod(target availableTarget) *corev1.Pod {
	comp := v1beta1.DecoderComponent
	routed := target != availableDecodeWorker && target != availableDecodeWorkerNew && target != availableDecodeWorkerLast
	last := target == availableDecodeWorkerLast
	moved := target == availableDecodeNew || target == availableDecodeWorkerNew
	if target == availablePrefillOld {
		comp = v1beta1.EngineComponent
	}
	var found *corev1.Pod
	at := int32(0)
	for _, pod := range a.listPods() {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || pod.DeletionTimestamp != nil || pod.Labels[constants.OMEComponentLabel] != string(comp) || query.RoutedRunner(pod) != routed {
			continue
		}
		if podCondition(pod, corev1.PodReady) != corev1.ConditionTrue || (podImage(pod) != refusedImageV1) != moved {
			continue
		}
		if found == nil || (idx < at) != last {
			found, at = pod, idx
		}
	}
	return found
}

// failProbe makes the target's readiness probe fail from this tick for
// ticks ticks and reports whether the target exists.
func (a *availableWorld) failProbe(target availableTarget, ticks int) bool {
	pod := a.targetPod(target)
	if pod == nil {
		a.note("cluster: no live pod is %s", target)
		return false
	}
	a.failFrom[pod.UID], a.failUntil[pod.UID] = a.tickNo+1, a.tickNo+1+ticks
	if ticks >= availableForGood {
		a.broken = pod.UID
		a.note("cluster: the readiness probe of %s fails from now on", pod.Name)
		return true
	}
	a.note("cluster: the readiness probe of %s fails for %d ticks", pod.Name, ticks)
	return true
}

// stopKubeletUnder stops the kubelet under the target for ticks ticks, as
// when the pod sits on a node of its own that stops reporting, and reports
// whether the target exists.
func (a *availableWorld) stopKubeletUnder(target availableTarget, ticks int) bool {
	pod := a.targetPod(target)
	if pod == nil {
		a.note("cluster: no live pod is %s", target)
		return false
	}
	a.stoppedFrom[pod.UID], a.stoppedUntil[pod.UID] = a.tickNo+1, a.tickNo+1+ticks
	a.note("cluster: the kubelet under %s stops for %d ticks", pod.Name, ticks)
	return true
}

// restartManager starts both controllers again over what the apiserver
// holds: nothing a controller kept in memory survives.
func (a *availableWorld) restartManager() {
	w := a.refusedWorld
	snap := w.snapshot()
	objs := make([]client.Object, 0, len(snap.objects))
	for _, obj := range snap.objects {
		objs = append(objs, obj.DeepCopyObject().(client.Object))
	}
	w.build(objs)
	a.restartAt = 0
	a.note("cluster: the manager restarted")
}

// faultOpen reports whether a move of the cluster still stands: a probe
// the model fails, a stopped kubelet, or a restart still due.
func (a *availableWorld) faultOpen() bool {
	if a.lostNode || a.restartAt > 0 {
		return true
	}
	for uid := range a.failUntil {
		if a.failing(uid) || a.tickNo < a.failFrom[uid] {
			return true
		}
	}
	for _, until := range a.stoppedUntil {
		if a.tickNo < until {
			return true
		}
	}
	return false
}

// The readings.

// availablePod is one pod as the readings see it.
type availablePod struct {
	name            string
	uid             types.UID
	routed          bool
	ready           bool
	gate            bool
	deleting        bool
	running         bool
	listed          bool
	containersReady bool
	// moved is the pod running the image the spec names: during a roll, a
	// pod the roll has moved.
	moved bool
	// stopped is the kubelet under the pod stopped by the model.
	stopped bool
	// since is when the pod's Ready condition last moved.
	since time.Time
}

// serves reports whether the pod takes traffic: Ready, with the serving
// gate, and not on its way out.
func (p availablePod) serves() bool { return p.ready && p.gate && !p.deleting }

// availableSide is one side at an instant: its pods by Instance, the
// Instances whose routed pod serves, and its pods in one line.
type availableSide struct {
	byIdx   map[int32][]availablePod
	serving map[int32]bool
	print   string
}

var availableSides = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}

func (a *availableWorld) readSides() map[v1beta1.ComponentType]*availableSide {
	out := map[v1beta1.ComponentType]*availableSide{}
	prints := map[v1beta1.ComponentType][]string{}
	for _, c := range availableSides {
		out[c] = &availableSide{byIdx: map[int32][]availablePod{}, serving: map[int32]bool{}}
	}
	for _, pod := range a.listPods() {
		c := v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel])
		side := out[c]
		idx, ok := query.InstanceIdxFromLabels(pod)
		if side == nil || !ok {
			continue
		}
		ready := findPodCondition(pod, corev1.PodReady)
		p := availablePod{
			name: pod.Name, uid: pod.UID, routed: query.RoutedRunner(pod),
			ready: ready.Status == corev1.ConditionTrue, since: ready.LastTransitionTime.Time,
			gate: podreadiness.IsServing(pod), deleting: pod.DeletionTimestamp != nil,
			running:         pod.Status.Phase == corev1.PodRunning,
			listed:          pod.Status.PodIP != "" && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed,
			containersReady: podCondition(pod, corev1.ContainersReady) == corev1.ConditionTrue,
			moved:           podImage(pod) == a.images[c],
			stopped:         a.kubeletStopped(pod),
		}
		side.byIdx[idx] = append(side.byIdx[idx], p)
		if p.routed && p.serves() {
			side.serving[idx] = true
		}
		prints[c] = append(prints[c], fmt.Sprintf("%s[%s ready=%t containers=%t gate=%t leaving=%t]", pod.Name, podImage(pod), p.ready, p.containersReady, p.gate, p.deleting))
	}
	for _, c := range availableSides {
		out[c].print = strings.Join(prints[c], " ")
	}
	return out
}

// readTick keeps the sides as the tick leaves them, and counts the tick
// when a headless Service lists as ready a pod that is not Ready.
func (a *availableWorld) readTick() {
	a.beforeThat, a.before = a.before, a.readSides()
	for _, side := range a.before {
		for _, pods := range side.byIdx {
			for _, p := range pods {
				if p.listed && !p.ready && !p.deleting {
					a.listedNotReady++
					return
				}
			}
		}
	}
}

// readPasses reads what the controllers of this tick published and did,
// against the pods as the previous tick left them: no pod changed between
// the two but by the controllers' own hand.
func (a *availableWorld) readPasses() {
	if a.before == nil {
		return
	}
	now := a.readSides()
	pods := ""
	for _, c := range availableSides {
		pods += a.before[c].print + " | "
		ir := &v1beta1.InferenceReplica{}
		if err := a.cli.Get(a.ctx, a.replicaKey(c), ir); err != nil {
			continue
		}
		a.readCounts(c, ir)
		a.readTaken(c, now[c])
		a.readReadyCondition(c, ir)
	}
	isvc := &v1beta1.InferenceService{}
	if err := a.cli.Get(a.ctx, a.serviceKey(), isvc); err != nil {
		return
	}
	var conds []string
	for _, cond := range isvc.Status.Conditions {
		conds = append(conds, fmt.Sprintf("%s=%s/%s", cond.Type, cond.Status, cond.Reason))
	}
	sort.Strings(conds)
	a.readFlap("the service's conditions", strings.Join(conds, " "), pods)
}

// readyInstances counts the Instances of a side with at least desired pods
// Ready, not leaving, and Ready for the window at the instant at.
func readyInstances(side *availableSide, desired int32, window time.Duration, at time.Time) map[int32]bool {
	out := map[int32]bool{}
	for idx, pods := range side.byIdx {
		var n int32
		for _, p := range pods {
			if p.ready && !p.deleting && !at.Before(p.since.Add(window)) {
				n++
			}
		}
		if n >= desired {
			out[idx] = true
		}
	}
	return out
}

// readCounts holds the published Available count to the pods the pass
// read: never more Instances than had every pod Ready for the window, and
// never fewer than the Instances that stood so at the end of two ticks.
func (a *availableWorld) readCounts(c v1beta1.ComponentType, ir *v1beta1.InferenceReplica) {
	desired := a.podsPerInstance(c)
	window := time.Duration(a.cfg.window) * time.Second
	at := a.clock.Now()
	ready := readyInstances(a.before[c], desired, window, at)
	a.readPictures(c, desired, window, at)
	st := ir.Status
	if int(st.AvailableReplicas) > len(ready) {
		a.record("available-counts-an-instance-that-is-not-ready", "%s publishes %d Available Instances; %d had every pod Ready for the %s window when the pass ran (published: %d Ready, %d serving, %d Instances)",
			c, st.AvailableReplicas, len(ready), window, st.ReadyReplicas, st.ServingReplicas, st.Replicas)
	}
	if a.beforeThat == nil {
		return
	}
	stable := 0
	for idx := range readyInstances(a.beforeThat[c], desired, window, at.Add(-refusedTickLength)) {
		if ready[idx] {
			stable++
		}
	}
	if int(st.AvailableReplicas) < stable {
		a.record("available-misses-an-instance-whose-pods-are-ready", "%s publishes %d Available Instances; %d had every pod Ready for the %s window at the end of the last two ticks (published: %d Ready, %d serving, %d Instances)",
			c, st.AvailableReplicas, stable, window, st.ReadyReplicas, st.ServingReplicas, st.Replicas)
	}
}

// readPictures counts the passes that read the pictures the count rule is
// about: an Instance with every pod listed and one of them not Ready, and
// a pod Ready for less than the window.
func (a *availableWorld) readPictures(c v1beta1.ComponentType, desired int32, window time.Duration, at time.Time) {
	short, inside := false, false
	for _, pods := range a.before[c].byIdx {
		var listed, ready int32
		for _, p := range pods {
			if p.deleting {
				continue
			}
			if p.listed {
				listed++
			}
			if p.ready {
				ready++
				inside = inside || at.Before(p.since.Add(window))
			}
		}
		short = short || (listed >= desired && ready < desired)
	}
	if short {
		a.listedShort++
	}
	if inside {
		a.insideWindow++
	}
}

// brokenByTheCluster counts the Instances with no serving routed pod whose
// live routed pod the model keeps unready: a failing probe or a stopped
// kubelet.
func (a *availableWorld) brokenByTheCluster(side *availableSide) int32 {
	var n int32
	for idx, pods := range side.byIdx {
		if side.serving[idx] {
			continue
		}
		for _, p := range pods {
			if p.routed && !p.deleting && (a.failing(p.uid) || p.stopped) {
				n++
				break
			}
		}
	}
	return n
}

// failedItsProbeForTheGrace reports whether the pod runs and its containers
// have been unready for the pod grace at the instant: the pod the update
// selection reads as none of the roll's work. A pod whose Ready alone is
// False, on a kubelet that stopped, is not that pod.
func (a *availableWorld) failedItsProbeForTheGrace(p availablePod, at time.Time) bool {
	return !p.deleting && p.running && !p.ready && !p.containersReady && at.Sub(p.since) >= a.podGrace
}

// shortOfAMember reads the serving Instances with a live pod that is not
// Ready, the gangs that serve through their leader beside a worker that
// does not, in three kinds: the gangs the roll has not moved, the moved
// gangs whose unready pods have all failed their probe for the pod grace,
// and the other moved gangs.
func (a *availableWorld) shortOfAMember(side *availableSide) (unmoved, pastGrace, other int32) {
	at := a.clock.Now()
	for idx, pods := range side.byIdx {
		if !side.serving[idx] {
			continue
		}
		unready, failed, moved := 0, 0, true
		for _, p := range pods {
			if p.deleting {
				continue
			}
			moved = moved && p.moved
			if p.ready {
				continue
			}
			unready++
			if a.failedItsProbeForTheGrace(p, at) {
				failed++
			}
		}
		switch {
		case unready == 0:
		case !moved:
			unmoved++
		case failed == unready:
			pastGrace++
		default:
			other++
		}
	}
	return unmoved, pastGrace, other
}

// unreadyPastGrace counts the Instances with no serving routed pod whose
// live routed pod the roll has moved and has failed its probe for the pod
// grace: the Instances the roll reads as none of its work.
func (a *availableWorld) unreadyPastGrace(side *availableSide) int32 {
	var n int32
	at := a.clock.Now()
	for idx, pods := range side.byIdx {
		if side.serving[idx] {
			continue
		}
		for _, p := range pods {
			if p.routed && p.moved && a.failedItsProbeForTheGrace(p, at) {
				n++
				break
			}
		}
	}
	return n
}

// readTaken holds the controllers to the budget when they take a serving
// Instance out. Under a drain-first strategy the Instances that serve
// stay within the budget of the count, whoever took the others down, with
// two pictures read on their own: an Instance the roll has moved whose
// pod has failed its probe for the pod grace, which the roll reads as none
// of its work, and a gang that serves through its leader beside a worker
// that is not Ready, read apart by whether the roll has moved the gang
// and whether the worker has failed its probe for the grace. Under a
// surge the Instances the cluster broke aside, the count stays served.
func (a *availableWorld) readTaken(c v1beta1.ComponentType, now *availableSide) {
	before := a.before[c]
	count := a.counts[c]
	served, serve := int32(len(before.serving)), int32(len(now.serving))
	if serve >= served {
		if !a.rolling(c) {
			return
		}
		if a.brokenByTheCluster(now) > 0 {
			a.waitedBeside++
		}
		return
	}
	budget := a.budgetOf(count)
	if a.strategyOf(c) == v1beta1.UpdateStrategySurgeThenDrain {
		if broken := a.brokenByTheCluster(now); serve+broken < count {
			a.record("surge-takes-a-serving-instance-out-below-the-count", "%s: the controllers took %d serving Instances out and %d of %d serve, with %d broken by the cluster", c, served-serve, serve, count, broken)
		}
		return
	}
	if serve >= count-budget {
		unmoved, pastGrace, other := a.shortOfAMember(now)
		if serve-unmoved-pastGrace-other >= count-budget {
			return
		}
		text := fmt.Sprintf("%s: the controllers took %d serving Instances out; %d of %d serve through their routed pod against a budget of %d, and of those %d the roll has not moved, %d it has moved whose worker failed its probe for the %s pod grace and %d other it has moved have a pod that is not Ready: %s",
			c, served-serve, serve, count, budget, unmoved, pastGrace, a.podGrace, other, before.print)
		switch {
		case other > 0:
			a.record("roll-takes-a-gang-out-beside-a-moved-gang-short-of-a-worker", "%s", text)
		case unmoved > 0:
			a.record("roll-takes-a-gang-out-beside-a-gang-it-has-not-moved-short-of-a-worker", "%s", text)
		default:
			a.record("roll-goes-on-beside-a-gang-whose-worker-is-unready-past-the-grace", "%s", text)
		}
		return
	}
	if pastGrace := a.unreadyPastGrace(before); serve+pastGrace >= count-budget {
		a.record("roll-goes-on-beside-an-instance-unready-past-the-grace", "%s: the controllers took %d serving Instances out; %d of %d serve against a budget of %d, and %d have been unready for the %s pod grace", c, served-serve, serve, count, budget, pastGrace, a.podGrace)
		return
	}
	a.record("roll-takes-a-serving-instance-out-past-the-budget", "%s: the controllers took %d serving Instances out and %d of %d serve, against a budget of %d: %s", c, served-serve, serve, count, budget, before.print)
}

// rolling reports whether a live pod of the side still runs another image
// than the one the spec names.
func (a *availableWorld) rolling(c v1beta1.ComponentType) bool {
	return a.offThePushedImage(c) > 0
}

// readReadyCondition reads the replica's Ready condition against the pods
// the pass read: it does not say every Instance is Ready while an Instance
// with pods has served nothing for two ticks, and it does not flap while
// no pod changed.
func (a *availableWorld) readReadyCondition(c v1beta1.ComponentType, ir *v1beta1.InferenceReplica) {
	cond := apimeta.FindStatusCondition(ir.Status.Conditions, inferencereplica.InferenceReplicaConditionReady)
	text := "none"
	if cond != nil {
		text = fmt.Sprintf("%s/%s", cond.Status, cond.Reason)
	}
	a.readFlap(fmt.Sprintf("the %s replica's Ready condition", c), text, a.before[c].print)
	if cond == nil || cond.Status != metav1.ConditionTrue || a.beforeThat == nil {
		return
	}
	var down []int32
	for idx := range a.before[c].byIdx {
		if idx < a.counts[c] && !a.before[c].serving[idx] && !a.beforeThat[c].serving[idx] && len(a.beforeThat[c].byIdx[idx]) > 0 {
			down = append(down, idx)
		}
	}
	if len(down) == 0 {
		return
	}
	sort.Slice(down, func(i, j int) bool { return down[i] < down[j] })
	st := ir.Status
	if cond.Reason == inferencereplica.ReasonAllInstancesReady {
		a.record("replica-reads-every-instance-ready-while-one-serves-nothing", "%s reads Ready=True, %s (%s) while Instances %v have had no pod serving for two ticks (published: %d Ready, %d serving, %d Available of %d)",
			c, cond.Reason, cond.Message, down, st.ReadyReplicas, st.ServingReplicas, st.AvailableReplicas, st.Replicas)
	}
	minimum := a.counts[c]
	if a.strategyOf(c) != v1beta1.UpdateStrategySurgeThenDrain {
		minimum -= a.budgetOf(a.counts[c])
	}
	if serve := a.counts[c] - int32(len(down)); serve < minimum {
		a.record("replica-reads-ready-below-its-serving-minimum", "%s reads Ready=True, %s (%s) while %d of %d Instances serve, under its serving minimum of %d, for two ticks (published: %d Ready, %d serving, %d Available)",
			c, cond.Reason, cond.Message, serve, a.counts[c], minimum, st.ReadyReplicas, st.ServingReplicas, st.AvailableReplicas)
	}
}

// availableReading is one reading of a Ready condition with the pods of
// its side at the time. availableHistory is the two readings before the
// current one, and how many readings came before it.
type availableReading struct{ text, pods string }

type availableHistory struct {
	older, last availableReading
	readings    int
}

// readFlap names a reading that went from one text to another and back
// while the pods it reads did not change.
func (a *availableWorld) readFlap(reader, text, pods string) {
	h := a.history[reader]
	a.history[reader] = availableHistory{older: h.last, last: availableReading{text: text, pods: pods}, readings: h.readings + 1}
	if h.readings < 2 {
		return
	}
	if h.older.text == text && h.last.text != text && h.older.pods == h.last.pods && h.last.pods == pods {
		a.record("ready-condition-flaps-without-a-pod-change", "%s went from %q to %q and back while no pod changed", reader, h.older.text, h.last.text)
	}
}

// The probes.

type availableProbeKind uint8

const (
	availablePlain availableProbeKind = iota
	availableSlowStart
	availableComebackFlap
	availableProbeFailsAMinute
	availableProbeFailsForGood
	availableNodeStopsAMinute
	availableKubeletStopsUnderOnePod
	availableProbeFailsAcrossRestarts
	availableProbeKindCount
)

func (k availableProbeKind) String() string {
	return [...]string{"plain push", "every runner that starts is listed 15 s before its probe passes", "every runner that comes up fails its probe once",
		"a readiness probe fails for a minute", "a readiness probe fails for good", "a node's kubelet stops for a minute",
		"the kubelet under one pod stops for a minute",
		"a readiness probe fails for a minute and the manager restarts as it fails and as it recovers"}[k]
}

// availableProbe is one probe: its kind, the moment of the roll it opens
// at, the pod it breaks when it breaks one, and whether the service is
// left at rest, with no push.
type availableProbe struct {
	kind   availableProbeKind
	moment refusedMoment
	target availableTarget
	rest   bool
}

func (p availableProbe) String() string {
	at := "at " + p.moment.String()
	if p.rest {
		at = "at rest, with no push"
	}
	switch p.kind {
	case availableProbeFailsAMinute, availableProbeFailsForGood, availableProbeFailsAcrossRestarts, availableKubeletStopsUnderOnePod:
		return fmt.Sprintf("%s %s on %s", p.kind, at, p.target)
	case availableNodeStopsAMinute:
		return fmt.Sprintf("%s %s", p.kind, at)
	}
	return p.kind.String()
}

// due makes the moves a probe scheduled for this tick.
func (a *availableWorld) due() {
	if a.nodeBackAt > 0 && a.tickNo >= a.nodeBackAt {
		a.nodeBack()
	}
	if a.restartAt > 0 && a.tickNo >= a.restartAt {
		a.restartManager()
	}
}

// play ticks until the world settles with no move of the cluster standing,
// or the bound passes, and reports whether it settled.
func (a *availableWorld) play(maxTicks int) bool {
	settledTicks := 0
	for i := 0; i < maxTicks; i++ {
		a.due()
		a.tick()
		if a.previous.settled && !a.faultOpen() {
			settledTicks++
			if settledTicks >= 2 {
				return true
			}
		} else {
			settledTicks = 0
		}
	}
	return false
}

// run plays one probe from the snapshot of its moment and classifies the
// end.
func (a *availableWorld) run(p availableProbe, snap *refusedSnapshot, maxTicks int) {
	a.restore(snap)
	a.path = p.String()
	a.probes++
	switch p.kind {
	case availableSlowStart:
		a.slowEveryStart()
	case availableComebackFlap:
		a.failEveryComebackOnce(availableFlapTicks)
	case availableProbeFailsAMinute:
		a.failProbe(p.target, availableMinuteTicks)
	case availableProbeFailsForGood:
		a.failProbe(p.target, availableForGood)
	case availableNodeStopsAMinute:
		a.loseNode()
		a.nodeBackAt = a.tickNo + availableMinuteTicks
	case availableKubeletStopsUnderOnePod:
		a.stopKubeletUnder(p.target, availableMinuteTicks)
	case availableProbeFailsAcrossRestarts:
		if a.failProbe(p.target, availableMinuteTicks) {
			a.restartManager()
			a.restartAt = a.tickNo + availableMinuteTicks + 1
		}
	}
	if p.moment == atIdle && !p.rest {
		a.push(refusedImageV2)
	}
	settled := a.play(maxTicks)
	if a.broken != "" {
		a.classifyBesideTheBrokenPod()
		return
	}
	a.classifyEnd(settled)
}

// classifyBesideTheBrokenPod reads the end of a run whose broken pod never
// passes its probe again. The roll is over when every Instance runs the
// pushed image and every Instance but the broken pod's serves; a roll that
// stopped short of that says on a replica what it waits on.
func (a *availableWorld) classifyBesideTheBrokenPod() {
	var short []string
	unexplained := false
	for _, c := range availableSides {
		text, named := a.shortBesideTheBrokenPod(c)
		if text == "" {
			continue
		}
		short = append(short, text)
		unexplained = unexplained || !named
	}
	switch {
	case len(short) == 0:
		a.classes["complete-beside-the-pod-that-stays-unready"]++
	case unexplained:
		a.record("roll-stops-beside-an-unready-pod-and-says-nothing", "the roll did not finish within the tick bound and a side that is short carries no hold: %s", strings.Join(short, "; "))
	default:
		a.record("roll-waits-beside-an-unready-pod", "the roll did not finish within the tick bound: %s", strings.Join(short, "; "))
	}
}

// shortBesideTheBrokenPod reads one side at the end of such a run. The
// text is empty when every pod of the side runs the pushed image and every
// Instance but the broken pod's serves; else it says what the side is
// short of and what its replica says it waits on. named is false when the
// replica names nothing.
func (a *availableWorld) shortBesideTheBrokenPod(c v1beta1.ComponentType) (text string, named bool) {
	offImage, down := a.offThePushedImage(c), 0
	side := a.before[c]
	for idx, pods := range side.byIdx {
		broken := false
		for _, p := range pods {
			broken = broken || p.uid == a.broken
		}
		if !side.serving[idx] && !broken {
			down++
		}
	}
	if offImage == 0 && down == 0 {
		return "", true
	}
	wait := a.waitNamedBy(c)
	named = wait != ""
	if !named {
		wait = "no hold and no open attempt"
	}
	return fmt.Sprintf("%s has %d pods off the pushed image and %d Instances down beside the broken pod (%s)", c, offImage, down, wait), named
}

// offThePushedImage counts the live pods of a side that run another image
// than the one the spec names.
func (a *availableWorld) offThePushedImage(c v1beta1.ComponentType) int {
	n := 0
	for _, pod := range a.listPods() {
		if pod.Labels[constants.OMEComponentLabel] == string(c) && pod.DeletionTimestamp == nil && podImage(pod) != a.images[c] {
			n++
		}
	}
	return n
}

// waitNamedBy is what the replica of a side says the roll waits on: its
// hold, or an attempt still open on a row, which names its step and which
// its deadline ends. It is empty when the replica names nothing.
func (a *availableWorld) waitNamedBy(c v1beta1.ComponentType) string {
	ir := &v1beta1.InferenceReplica{}
	if err := a.cli.Get(a.ctx, a.replicaKey(c), ir); err != nil {
		a.t.Fatalf("read the %s replica: %v", c, err)
	}
	if hold := ir.Status.RolloutHold; hold != nil {
		return fmt.Sprintf("%s: %s", hold.Gate, hold.Reason)
	}
	for i := range ir.Status.InstanceStatuses {
		if row := &ir.Status.InstanceStatuses[i]; row.Operation != nil {
			return fmt.Sprintf("an attempt open on Instance %d: %s", row.Index, rowPrint(row))
		}
	}
	return ""
}

// availableProbes lists the probes of a run on a configuration: the three
// pushes that slow every start, the three losses on a service at rest, and
// at each moment of the roll a loss on each pod the moment has.
func availableProbes(cfg availableConfig, full bool) []availableProbe {
	out := []availableProbe{
		{kind: availablePlain}, {kind: availableSlowStart}, {kind: availableComebackFlap},
		{kind: availableProbeFailsAMinute, target: availableDecodeOld, rest: true},
		{kind: availableKubeletStopsUnderOnePod, target: availableDecodeOld, rest: true},
		{kind: availableNodeStopsAMinute, rest: true},
	}
	targets := []availableTarget{availableDecodeOld, availableDecodeNew, availablePrefillOld}
	if cfg.shape.pods > 1 {
		targets = append(targets, availableDecodeWorker, availableDecodeWorkerNew, availableDecodeWorkerLast)
	}
	moments := []refusedMoment{atIdle, atRotating, atLast}
	if full {
		moments = []refusedMoment{atIdle, atSettling, atRotating, atDraining, atLast}
	}
	for _, m := range moments {
		for _, target := range targets {
			if moved := target == availableDecodeNew || target == availableDecodeWorkerNew; (m == atIdle) == moved && m != atRotating && !full {
				// Before the push no pod has moved; at the last Instance
				// the bounded run breaks only a pod that has.
				continue
			}
			out = append(out, availableProbe{kind: availableProbeFailsAMinute, moment: m, target: target})
			if full || target != availablePrefillOld {
				out = append(out, availableProbe{kind: availableKubeletStopsUnderOnePod, moment: m, target: target})
			}
			if full || (m != atLast && target != availablePrefillOld) {
				out = append(out, availableProbe{kind: availableProbeFailsForGood, moment: m, target: target})
			}
		}
		if full || m == atRotating {
			out = append(out, availableProbe{kind: availableNodeStopsAMinute, moment: m},
				availableProbe{kind: availableProbeFailsAcrossRestarts, moment: m, target: availableDecodeOld})
		}
	}
	return out
}

func availableTickBound(full bool) int {
	if full {
		return 360
	}
	return 160
}

// explore runs the probes from the moments of one plain roll, then random
// runs that mix the cluster's moves, under a fixed seed.
func (a *availableWorld) explore(full bool, seed int64, runs int) {
	a.t.Helper()
	maxTicks := availableTickBound(full)
	a.reachIdle()
	snaps := a.captureMoments(maxTicks)
	for _, p := range availableProbes(a.cfg, full) {
		if snap := snaps[p.moment]; snap != nil {
			a.run(p, snap, maxTicks)
		}
	}
	rng := rand.New(rand.NewSource(seed))
	for run := 0; run < runs; run++ {
		a.restore(snaps[atIdle])
		a.path = fmt.Sprintf("random run %d", run+1)
		a.probes++
		a.randomRun(rng, maxTicks)
	}
}

// randomRun plays a push with up to four moves of the cluster at random
// ticks: a probe that fails for a while on a random target, a stopped
// kubelet, a manager restart, the slow start or the comeback flap.
func (a *availableWorld) randomRun(rng *rand.Rand, maxTicks int) {
	a.push(refusedImageV2)
	left := 4
	settledTicks := 0
	for i := 0; i < maxTicks; i++ {
		a.due()
		if left > 0 && rng.Intn(5) == 0 {
			left--
			switch rng.Intn(5) {
			case 0:
				target := availableTarget(rng.Intn(int(availableTargetCount)))
				if rng.Intn(2) == 0 {
					a.failProbe(target, 1+rng.Intn(availableMinuteTicks))
				} else {
					a.stopKubeletUnder(target, 1+rng.Intn(availableMinuteTicks))
				}
			case 1:
				if !a.lostNode {
					a.loseNode()
					a.nodeBackAt = a.tickNo + 2 + rng.Intn(availableMinuteTicks)
				}
			case 2:
				a.restartManager()
			case 3:
				a.slowEveryStart()
			case 4:
				a.failEveryComebackOnce(1 + rng.Intn(availableFlapTicks))
			}
		}
		a.tick()
		if a.previous.settled && !a.faultOpen() {
			settledTicks++
			if settledTicks >= 2 {
				break
			}
		} else {
			settledTicks = 0
		}
	}
	a.classifyEnd(settledTicks >= 2)
}

// Configurations: the reference prefill/decode shape under the three
// strategies with the group and the ratio rule, the window of 0 and of
// 30 s, and gangs under RecreatePod and SurgeThenDrain.

func availableConfigOf(prefill, decode, pods int32, p, d v1beta1.UpdateStrategyType, group bool, window int32) availableConfig {
	return availableConfig{refusedConfig: refusedConfig{shape: refusedShape{prefill, decode, pods, 0},
		settings: refusedSettings{prefill: p, decode: d, group: group, maintainRatio: group, gangTiming: pods > 1}}, window: window}
}

func availableBoundedConfigs() []availableConfig {
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	return []availableConfig{
		availableConfigOf(12, 8, 1, recreate, recreate, true, 0),
		availableConfigOf(6, 4, 1, recreate, recreate, true, availableWindowSeconds),
		availableConfigOf(12, 8, 1, surge, surge, true, 0),
		availableConfigOf(12, 8, 1, inPlace, inPlace, true, 0),
		availableConfigOf(12, 8, 1, recreate, recreate, false, 0),
		availableConfigOf(4, 4, 2, recreate, recreate, false, 0),
		availableConfigOf(2, 2, 2, surge, surge, true, 0),
	}
}

func availableConfigs(full bool) []availableConfig {
	configs := availableBoundedConfigs()
	if !full {
		return configs
	}
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	seen := map[string]bool{}
	for _, c := range configs {
		seen[c.String()] = true
	}
	shapes := []refusedShape{{12, 8, 1, 0}, {30, 10, 1, 0}, {21, 14, 1, 0}, {4, 4, 2, 0}, {2, 2, 8, 0}}
	for _, shape := range shapes {
		for _, strategy := range []v1beta1.UpdateStrategyType{recreate, surge, inPlace} {
			for _, group := range []bool{true, false} {
				for _, window := range []int32{0, availableWindowSeconds} {
					cfg := availableConfigOf(shape.prefill, shape.decode, shape.pods, strategy, strategy, group, window)
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

// knownAvailableClasses are the pictures the model shows, each with
// the rule behind it. A finding in a known class is reported and does not
// fail the run.
var knownAvailableClasses = map[string]string{
	"unavailable-budget-exceeded/none":                                       "the cluster's own loss (a failing probe, a stopped kubelet) takes Instances down beside the roll; this model's own reading holds the controllers to the budget for what they take out",
	"surge-budget-exceeded/none":                                             "the cluster's own loss (a failing probe, a stopped kubelet) takes Instances down beside the roll; this model's own reading holds the controllers to the budget for what they take out",
	"roll-goes-on-beside-an-instance-unready-past-the-grace":                 availableKnownUnreadyPastTheGrace,
	"roll-goes-on-beside-a-gang-whose-worker-is-unready-past-the-grace":      availableKnownUnreadyPastTheGrace + "; a gang with such a worker still takes traffic through its leader, and the roll takes the next gang out beside it",
	"roll-waits-beside-an-unready-pod":                                       "a roll in a group counts every Instance that does not serve against the group's budget, whatever took it down, and an in-place attempt on a pod that never passes its probe again stays open until its deadline; the roll waits, and the hold or the open attempt names the Instance",
	"roll-takes-a-serving-instance-out-past-the-budget":                      availableKnownMovedPodOnAStoppedNode,
	"roll-takes-a-gang-out-beside-a-moved-gang-short-of-a-worker":            availableKnownMovedPodOnAStoppedNode,
	"roll-takes-a-gang-out-beside-a-gang-it-has-not-moved-short-of-a-worker": availableKnownUnmovedGangShortOfAWorker,
	"replica-reads-every-instance-ready-while-one-serves-nothing":            availableKnownReadyCountsStaleContainers,
	"replica-reads-ready-below-its-serving-minimum":                          availableKnownReadyCountsStaleContainers,
}

const availableKnownMovedPodOnAStoppedNode = "the roll's own budget reads an Instance it already moved by its containers' readiness and the serving gate (ops.RolledInstanceNotServing over query.PodSetFullyServing), which a stopped kubelet leaves at their last values, so the Instance holds no slot while Ready is False on its pod, and the roll takes the next serving Instance out; an Instance the roll has not moved is read by its pod's Ready condition and holds its slot (held by TestAvailableMeansReady_ANodeStopsUnderAPodTheRollAlreadyMoved)"

const availableKnownUnmovedGangShortOfAWorker = "the roll's own budget reads an Instance it has not moved by its routed pod alone (ops.InstanceNotServingOutsideRoll), so a gang that serves through its leader holds no slot however its workers stand, and the roll takes the next gang out beside it until, after the pod grace, it starts on that gang itself; a rollout group does not stop it, since with no side named first both sides start each batch together; this model leaves open whether such a gang should hold a slot (held by TestAvailableMeansReady_AGangShortOfAWorkerHoldsNoSlotBeforeTheRollMovesIt)"

const availableKnownUnreadyPastTheGrace = "an Instance already on the pushed revision whose pods run and have failed readiness for the pod grace holds no slot of the roll's own budget, by the update selection's rule for such an Instance: nothing those pods do ends the shortfall and no policy rebuilds a running pod, so the roll goes on beside it"

const availableKnownReadyCountsStaleContainers = "the Ready count reads the containers' readiness alone (status.CountReadyPods), which a stopped kubelet leaves True, and the node lifecycle controller writes no reason on the Ready condition it sets False, so the reader that leaves such a pod out (podreadiness.ReadyRevokedByNode) does not see it: the replica reads every Instance Ready while the serving and Available counts are short; this model leaves open how a pod on a node that stopped reporting should be recognised or whether the Ready count should leave it out (held by TestAvailableMeansReady_AStoppedNodeLeavesTheReplicaReadingEveryInstanceReady)"

var availableExpectedEnds = map[string]bool{"complete": true, "complete-beside-the-pod-that-stays-unready": true}

// failOnClass fails the test when the world recorded the class.
func (a *availableWorld) failOnClass(t *testing.T, class, why string) {
	t.Helper()
	if n := a.classes[class]; n > 0 {
		t.Fatalf("%s on %d ticks: %s\n  %s\n  path: %s", class, n, why, a.findings[class].message, a.findings[class].path)
	}
}

// availableLockWorld builds the configuration, rolls it to idle at version
// 1 and takes the snapshots of one plain roll.
func availableLockWorld(t *testing.T, cfg availableConfig) (*availableWorld, map[refusedMoment]*refusedSnapshot) {
	t.Helper()
	a := newAvailableWorld(t, cfg)
	a.reachIdle()
	return a, a.captureMoments(availableTickBound(false))
}

// runAt plays the probe from the snapshot of its moment, failing the test
// when the plain roll never reached the moment.
func (a *availableWorld) runAt(t *testing.T, snaps map[refusedMoment]*refusedSnapshot, p availableProbe) {
	t.Helper()
	if snaps[p.moment] == nil {
		t.Fatalf("the plain roll never reached %s", p.moment)
	}
	a.run(p, snaps[p.moment], availableTickBound(false))
}

// availableResult is what one configuration's run read.
type availableResult struct {
	cfg            availableConfig
	states         int
	ticks          int
	probes         int
	listedNotReady int
	classes        map[string]int
	findings       map[string]refusedFinding
	took           time.Duration
}

// String is the configuration's line: its sizes and its classes by name.
func (r availableResult) String() string {
	parts := make([]string, 0, len(r.classes))
	for class, n := range r.classes {
		parts = append(parts, fmt.Sprintf("%s=%d", class, n))
	}
	sort.Strings(parts)
	return fmt.Sprintf("[%s]: %d states, %d ticks, %d probes, %d ticks with a pod listed and not Ready, %s: %s",
		r.cfg, r.states, r.ticks, r.probes, r.listedNotReady, r.took.Round(time.Second), strings.Join(parts, " "))
}

// availableClass is one class over every configuration: how often it was
// read, in how many configurations, and its first three pictures.
type availableClass struct {
	total, configurations int
	examples              []availableResult
}

// availableRunConfig explores one configuration.
func availableRunConfig(t *testing.T, cfg availableConfig, full bool) availableResult {
	t.Helper()
	began := time.Now()
	runs := 2
	if full {
		runs = 6
	}
	a := newAvailableWorld(t, cfg)
	a.explore(full, 1, runs)
	return availableResult{cfg: cfg, states: len(a.seen), ticks: a.ticks, probes: a.probes, listedNotReady: a.listedNotReady, classes: a.classes, findings: a.findings, took: time.Since(began)}
}

// availableExplore runs the model over the configurations keep admits, one
// subtest per configuration in parallel, and reports every class, failing
// on the classes that are neither expected ends nor known.
func availableExplore(t *testing.T, keep func(availableConfig) bool) {
	t.Helper()
	full := os.Getenv(availableHuntEnv) == "full"
	only := os.Getenv(availableOnlyEnv)
	start := time.Now()
	var results []*availableResult
	t.Run("configurations", func(t *testing.T) {
		for _, cfg := range availableConfigs(full) {
			if !keep(cfg) || !strings.Contains(cfg.String(), only) {
				continue
			}
			res := &availableResult{}
			results = append(results, res)
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				*res = availableRunConfig(t, cfg, full)
			})
		}
	})
	classes := map[string]*availableClass{}
	sizes := availableResult{}
	for n, res := range results {
		t.Logf("config %d/%d %s", n+1, len(results), res)
		sizes.states, sizes.ticks, sizes.probes, sizes.listedNotReady = sizes.states+res.states, sizes.ticks+res.ticks, sizes.probes+res.probes, sizes.listedNotReady+res.listedNotReady
		for name, count := range res.classes {
			class := classes[name]
			if class == nil {
				class = &availableClass{}
				classes[name] = class
			}
			class.total += count
			if _, found := res.findings[name]; found {
				class.configurations++
				if len(class.examples) < 3 {
					class.examples = append(class.examples, *res)
				}
			}
		}
	}
	t.Logf("%d configurations, %d states, %d ticks, %d probes, %d ticks with a pod listed and not Ready, %s", len(results), sizes.states, sizes.ticks, sizes.probes, sizes.listedNotReady, time.Since(start).Round(time.Second))
	availableReportClasses(t, classes)
}

// availableReportClasses prints every class with its pictures and fails on a class
// that is neither an expected end nor known.
func availableReportClasses(t *testing.T, classes map[string]*availableClass) {
	t.Helper()
	names := make([]string, 0, len(classes))
	for name := range classes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		class := classes[name]
		t.Logf("%s: %d in %d configurations", name, class.total, class.configurations)
		for _, ex := range class.examples {
			f := ex.findings[name]
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, f.message, f.path, f.state)
		}
		known := knownAvailableClasses[name]
		switch {
		case availableExpectedEnds[name]:
		case known != "":
			t.Logf("  known: %s", known)
		case len(class.examples) == 0:
			t.Errorf("%s: %d", name, class.total)
		default:
			t.Errorf("%s: %d (first: %s: %s)", name, class.total, class.examples[0].cfg, class.examples[0].findings[name].message)
		}
	}
}

// TestAvailableMeansReady_EveryCountAndGateReadsThePodsThatAreReady
// explores every configuration.
func TestAvailableMeansReady_EveryCountAndGateReadsThePodsThatAreReady(t *testing.T) {
	availableExplore(t, func(availableConfig) bool { return true })
}

// Locks: each states one invariant in plain words on a reference shape
// and fails when the engine stops keeping it.

// TestAvailableMeansReady_APodListedAndNotReadyIsNotCountedAvailable: 12
// prefill and 8 decode pods under RecreatePod in their group, and 4 gangs
// of two on each side. Version 2 is pushed while every runner that starts
// takes 15 s to pass its probe; a decode pod the roll has moved fails its
// probe for a minute; the kubelet under a decode pod stops for a minute
// with nothing rolling. The headless Service lists each of those pods as
// ready all along. No replica publishes more Available Instances than had
// every pod Ready when its pass ran, and none fewer than stood so for two
// ticks.
func TestAvailableMeansReady_APodListedAndNotReadyIsNotCountedAvailable(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	for _, cfg := range []availableConfig{availableConfigOf(12, 8, 1, recreate, recreate, true, 0), availableConfigOf(4, 4, 2, recreate, recreate, false, 0)} {
		t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
			t.Parallel()
			a, snaps := availableLockWorld(t, cfg)
			a.runAt(t, snaps, availableProbe{kind: availableSlowStart})
			a.runAt(t, snaps, availableProbe{kind: availableProbeFailsAMinute, moment: atRotating, target: availableDecodeNew})
			a.runAt(t, snaps, availableProbe{kind: availableKubeletStopsUnderOnePod, target: availableDecodeOld, rest: true})
			if cfg.shape.pods > 1 {
				a.runAt(t, snaps, availableProbe{kind: availableKubeletStopsUnderOnePod, moment: atRotating, target: availableDecodeWorkerNew})
			}
			if a.listedShort == 0 {
				t.Fatalf("the picture moved: no pass read an Instance with every pod listed and one of them not Ready")
			}
			a.failOnClass(t, "available-counts-an-instance-that-is-not-ready", "a replica counted as Available an Instance with a pod that was not Ready")
			a.failOnClass(t, "available-misses-an-instance-whose-pods-are-ready", "a replica left out of its Available count an Instance whose pods had been Ready for two ticks")
		})
	}
}

// TestAvailableMeansReady_UnderAWindowAPodCountsOnceItHeldReadyThatLong: 6
// prefill and 4 decode pods under RecreatePod with minReadySeconds 30.
// Version 2 is pushed while every runner that comes up fails its probe
// once, and a decode pod the roll has moved fails its probe for a minute.
// A pod Ready for less than 30 s is in no Available count, a pod Ready for
// 30 s is in it on the next pass, and both rolls end.
func TestAvailableMeansReady_UnderAWindowAPodCountsOnceItHeldReadyThatLong(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	a, snaps := availableLockWorld(t, availableConfigOf(6, 4, 1, recreate, recreate, true, availableWindowSeconds))
	a.runAt(t, snaps, availableProbe{kind: availableComebackFlap})
	a.runAt(t, snaps, availableProbe{kind: availableProbeFailsAMinute, moment: atRotating, target: availableDecodeNew})
	if a.insideWindow == 0 {
		t.Fatalf("the picture moved: no pass read a pod Ready for less than the window")
	}
	a.failOnClass(t, "available-counts-an-instance-that-is-not-ready", "a replica counted as Available an Instance with a pod Ready for less than the window")
	a.failOnClass(t, "available-misses-an-instance-whose-pods-are-ready", "a replica left out of its Available count an Instance whose pods had held Ready for the window for two ticks")
	if got := a.classes["complete"]; got != 2 {
		t.Fatalf("%d of 2 rolls ended complete: %v", got, a.classes)
	}
}

// TestAvailableMeansReady_ARollTakesNoServingPodOutBesideOneWhoseProbeFails:
// 12 prefill and 8 decode pods under RecreatePod, one decode pod allowed
// down, with the rollout group and without it. A decode pod fails its
// readiness probe for a minute: before the push, on a pod the roll has not
// moved, and on one it has. While the pod is not Ready and inside the pod
// grace, the roll takes no other serving decode pod out; it waits. Then
// the prefill side with two pods allowed down and a partition of 2: a pod
// the partition holds fails its probe, the roll cannot start on it, and it
// takes one serving prefill pod out beside it, not two.
func TestAvailableMeansReady_ARollTakesNoServingPodOutBesideOneWhoseProbeFails(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	for _, group := range []bool{false, true} {
		cfg := availableConfigOf(12, 8, 1, recreate, recreate, group, 0)
		t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
			t.Parallel()
			a, snaps := availableLockWorld(t, cfg)
			a.runAt(t, snaps, availableProbe{kind: availableProbeFailsAMinute, moment: atIdle, target: availableDecodeOld})
			a.runAt(t, snaps, availableProbe{kind: availableProbeFailsAMinute, moment: atRotating, target: availableDecodeOld})
			a.runAt(t, snaps, availableProbe{kind: availableProbeFailsAMinute, moment: atRotating, target: availableDecodeNew})
			if a.waitedBeside == 0 {
				t.Fatalf("the picture moved: no pass left a rolling side alone while a pod of it failed its probe")
			}
			a.failOnClass(t, "roll-takes-a-serving-instance-out-past-the-budget", "the roll took a serving pod out beside a pod that failed its readiness probe")
			if got := a.classes["complete"]; got != 3 {
				t.Fatalf("%d of 3 rolls ended complete: %v", got, a.classes)
			}
		})
	}
	t.Run("a_pod_the_partition_holds", func(t *testing.T) {
		t.Parallel()
		cfg := availableConfigOf(12, 8, 1, recreate, recreate, false, 0)
		cfg.settings.partition = 2
		a, snaps := availableLockWorld(t, cfg)
		a.runAt(t, snaps, availableProbe{kind: availableProbeFailsAMinute, moment: atRotating, target: availablePrefillOld})
		if a.waitedBeside == 0 {
			t.Fatalf("the picture moved: no pass left the rolling prefill side alone while a pod the partition holds failed its probe")
		}
		a.failOnClass(t, "roll-takes-a-serving-instance-out-past-the-budget", "the roll took two serving prefill pods out beside a held pod that failed its readiness probe")
		if got := a.classes["complete"]; got != 1 {
			t.Fatalf("%d of 1 roll ended complete: %v", got, a.classes)
		}
	})
}

// TestAvailableMeansReady_ANodeStopsUnderAPodTheRollAlreadyMoved: 12 prefill
// and 8 decode pods under RecreatePod, one decode pod allowed down. The
// roll has moved one decode pod onto version 2 when the kubelet under it
// stops. The node lifecycle controller sets Ready to False on the pod and
// leaves its containers' readiness and the serving gate as they were. The
// roll reads the moved Instance by those two, so it holds no slot, and the
// roll takes the next serving pod out: 6 of 8 decode pods serve against a
// budget of one. The same stop under a pod the roll has not moved holds
// the slot, and the roll waits. On 4 decode gangs of one leader and one
// worker, one gang allowed down, the kubelet stops under the worker of a
// gang the roll has moved, and the roll takes the next gang out beside
// it: 2 of 4 gangs are whole.
func TestAvailableMeansReady_ANodeStopsUnderAPodTheRollAlreadyMoved(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	t.Run("single_pods", func(t *testing.T) {
		t.Parallel()
		a, snaps := availableLockWorld(t, availableConfigOf(12, 8, 1, recreate, recreate, false, 0))
		a.runAt(t, snaps, availableProbe{kind: availableKubeletStopsUnderOnePod, moment: atRotating, target: availableDecodeOld})
		a.failOnClass(t, "roll-takes-a-serving-instance-out-past-the-budget", "the roll took a serving pod out beside a pod it has not moved whose kubelet stopped")
		a.runAt(t, snaps, availableProbe{kind: availableKubeletStopsUnderOnePod, moment: atRotating, target: availableDecodeNew})
		if a.classes["roll-takes-a-serving-instance-out-past-the-budget"] == 0 {
			t.Fatalf("the picture moved: the roll took no serving pod out beside the moved pod whose kubelet stopped\n%s", strings.Join(a.trace, "\n"))
		}
		if got := a.classes["complete"]; got != 2 {
			t.Fatalf("%d of 2 rolls ended complete: %v", got, a.classes)
		}
	})
	t.Run("gangs_of_two", func(t *testing.T) {
		t.Parallel()
		a, snaps := availableLockWorld(t, availableConfigOf(4, 4, 2, recreate, recreate, false, 0))
		a.runAt(t, snaps, availableProbe{kind: availableKubeletStopsUnderOnePod, moment: atRotating, target: availableDecodeWorkerNew})
		if a.classes["roll-takes-a-gang-out-beside-a-moved-gang-short-of-a-worker"] == 0 {
			t.Fatalf("the picture moved: the roll took no gang out beside the moved gang whose worker's kubelet stopped\n%s", strings.Join(a.trace, "\n"))
		}
		if got := a.classes["complete"]; got != 1 {
			t.Fatalf("%d of 1 roll ended complete: %v", got, a.classes)
		}
	})
}

// TestAvailableMeansReady_AGangShortOfAWorkerHoldsNoSlotBeforeTheRollMovesIt:
// 4 prefill and 4 decode gangs of one leader and one worker under
// RecreatePod, one gang allowed down. While the roll moves the first
// decode gangs, a worker of the last decode gang starts failing its
// readiness probe and never passes it again. The gang still takes traffic
// through its leader, and the roll's own budget reads a gang it has not
// moved by its leader alone, so the roll takes the next gang out beside
// it, and 2 of 4 gangs are whole against a budget of one. A rollout group
// does not prevent it: with no side named first, both sides start each
// batch together and the decode side moves on without waiting out the pod
// grace. Either way the roll rebuilds that gang in its turn and ends.
func TestAvailableMeansReady_AGangShortOfAWorkerHoldsNoSlotBeforeTheRollMovesIt(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	const class = "roll-takes-a-gang-out-beside-a-gang-it-has-not-moved-short-of-a-worker"
	probe := availableProbe{kind: availableProbeFailsForGood, moment: atRotating, target: availableDecodeWorkerLast}
	for name, grouped := range map[string]bool{"without_a_group": false, "in_a_group": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a, snaps := availableLockWorld(t, availableConfigOf(4, 4, 2, recreate, recreate, grouped, 0))
			a.runAt(t, snaps, probe)
			if a.classes[class] == 0 {
				t.Fatalf("the picture moved: the roll took no gang out beside the gang whose worker failed its probe\n%s", strings.Join(a.trace, "\n"))
			}
			if got := a.classes["complete-beside-the-pod-that-stays-unready"]; got != 1 {
				t.Fatalf("%d of 1 roll ended with every gang on the pushed image and serving: %v", got, a.classes)
			}
		})
	}
}

// TestAvailableMeansReady_EveryRollEndsOnceThePodsAreReadyAgain: the
// prefill/decode pair under RecreatePod, SurgeThenDrain and
// InPlaceIfPossible in its group, and gangs of two under RecreatePod.
// Runners that start slowly, runners that fail their probe once as they
// come up, a probe that fails for a minute before the push and on the last
// Instance, a kubelet that stops for a minute, and a manager that restarts
// as a probe fails and as it recovers: every roll ends with every Instance
// serving version 2.
func TestAvailableMeansReady_EveryRollEndsOnceThePodsAreReadyAgain(t *testing.T) {
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	for _, cfg := range []availableConfig{
		availableConfigOf(12, 8, 1, recreate, recreate, true, 0),
		availableConfigOf(12, 8, 1, surge, surge, true, 0),
		availableConfigOf(12, 8, 1, inPlace, inPlace, true, 0),
		availableConfigOf(4, 4, 2, recreate, recreate, false, 0),
	} {
		t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
			t.Parallel()
			a, snaps := availableLockWorld(t, cfg)
			probes := []availableProbe{
				{kind: availableSlowStart}, {kind: availableComebackFlap},
				{kind: availableProbeFailsAMinute, moment: atIdle, target: availableDecodeOld},
				{kind: availableProbeFailsAMinute, moment: atLast, target: availableDecodeNew},
				{kind: availableNodeStopsAMinute, moment: atRotating},
				{kind: availableProbeFailsAcrossRestarts, moment: atRotating, target: availableDecodeOld},
			}
			for _, p := range probes {
				a.runAt(t, snaps, p)
			}
			if got := a.classes["complete"]; got != len(probes) {
				t.Fatalf("%d of %d rolls ended complete: %v\n  %s", got, len(probes), a.classes, a.findings["holds-forever/none"].message)
			}
		})
	}
}

// TestAvailableMeansReady_AStoppedNodeLeavesTheReplicaReadingEveryInstanceReady:
// 12 prefill and 8 decode pods at rest. The kubelet of the node under half
// of each side stops for a minute. The node lifecycle controller sets
// Ready to False on its pods, with no reason, and their containers'
// readiness stays True. Each replica publishes half its Instances as
// serving and as Available, and still reads Ready=True with every Instance
// Ready: the Ready count reads the containers alone.
func TestAvailableMeansReady_AStoppedNodeLeavesTheReplicaReadingEveryInstanceReady(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	a, snaps := availableLockWorld(t, availableConfigOf(12, 8, 1, recreate, recreate, true, 0))
	a.runAt(t, snaps, availableProbe{kind: availableNodeStopsAMinute, rest: true})
	a.failOnClass(t, "available-counts-an-instance-that-is-not-ready", "a replica counted as Available an Instance whose pod's kubelet stopped")
	below := a.findings["replica-reads-ready-below-its-serving-minimum"]
	if a.classes["replica-reads-ready-below-its-serving-minimum"] == 0 || !strings.Contains(below.message, inferencereplica.ReasonAllInstancesReady) {
		t.Fatalf("the picture moved: no replica read every Instance Ready while half of them served nothing: %v %s", a.classes, below.message)
	}
}

// TestAvailableMeansReady_NoReadyConditionFlapsWhileAPodStarts: 12 prefill
// and 8 decode pods under RecreatePod and under SurgeThenDrain. Version 2
// is pushed while every runner that starts is listed for 15 s before it is
// Ready, and again while every runner fails its probe once as it comes up.
// No Ready condition of a replica or of the service goes from one reading
// to another and back while no pod changed.
func TestAvailableMeansReady_NoReadyConditionFlapsWhileAPodStarts(t *testing.T) {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	for _, cfg := range []availableConfig{availableConfigOf(12, 8, 1, recreate, recreate, true, 0), availableConfigOf(12, 8, 1, surge, surge, true, 0)} {
		t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
			t.Parallel()
			a, snaps := availableLockWorld(t, cfg)
			a.runAt(t, snaps, availableProbe{kind: availableSlowStart})
			a.runAt(t, snaps, availableProbe{kind: availableComebackFlap})
			if a.listedNotReady == 0 {
				t.Fatalf("the picture moved: no tick ended with a pod listed and not Ready")
			}
			a.failOnClass(t, "ready-condition-flaps-without-a-pod-change", "a Ready condition flapped while no pod changed")
		})
	}
}
