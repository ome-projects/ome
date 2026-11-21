package inferenceservice

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// This file stages a node that stops reporting under a decode pod the roll
// has already moved, and stays away: for good, for good under the
// operator's force-delete policy, and for five minutes. It reads how long
// the roll stands beside that pod and what ends the stand, in the world of
// available_means_ready_model_test.go.
//
// The cluster of these stagings. The pod sits on a node of its own. From
// the stop the node reads Ready Unknown and carries the unreachable taint,
// as the node lifecycle controller leaves it, and Ready is False on the pod
// while its containers' readiness and its serving gate keep their last
// values. The model has no taint manager: nothing deletes the pod when its
// toleration of the taint ends, so the pod stands for as long as its node
// is away, as a pod that tolerates the taint without a limit does.

const (
	// availableOwnNode is the node of its own the stopped pod sits on.
	availableOwnNode = "node-3"
	// availableOutage is how long the node of the five-minute staging stays
	// away: the time Kubernetes lets a pod tolerate an unreachable node
	// unless the pod says otherwise.
	availableOutage = 5 * time.Minute
	// availablePolicySlack and availablePolicyThreshold are the force-delete
	// policy the chart's commented example names: the engine removes a pod
	// once its node has been unreachable for the threshold.
	availablePolicySlack, availablePolicyThreshold = "2m", "5m"
	// availablePastTheDeadline is how far past the chart's attempt deadline
	// the staging of a node that never returns runs, so that no deadline of
	// the engine is still ahead when it ends.
	availablePastTheDeadline = 5 * time.Minute
)

// lifecycleConfig is the lifecycle block of the operator config as the
// controller's own parser reads it.
func (a *availableWorld) lifecycleConfig() *controllerconfig.LifecycleConfig {
	a.t.Helper()
	lc := &controllerconfig.LifecycleConfig{}
	if err := json.Unmarshal([]byte(a.ext.configData["lifecycle"]), lc); err != nil {
		a.t.Fatalf("parse the lifecycle block: %v", err)
	}
	return lc
}

// ticksOf is a duration in ticks.
func ticksOf(d time.Duration) int { return int(d / refusedTickLength) }

// attemptDeadlineTicks is the chart's deadline for one attempt, in ticks.
func (a *availableWorld) attemptDeadlineTicks() int {
	a.t.Helper()
	deadline, err := a.lifecycleConfig().ToInstanceReadyTimeout()
	if err != nil || deadline <= 0 {
		a.t.Fatalf("the lifecycle block carries no attempt deadline: %v", err)
	}
	return ticksOf(deadline)
}

// useForceDeletePolicy writes a force-delete policy into the operator
// config. The controllers a restore builds read it.
func (a *availableWorld) useForceDeletePolicy(slack, threshold string) {
	a.t.Helper()
	lifecycle := map[string]any{}
	if err := json.Unmarshal([]byte(a.ext.configData["lifecycle"]), &lifecycle); err != nil {
		a.t.Fatalf("parse the lifecycle block: %v", err)
	}
	lifecycle["forceDelete"] = map[string]any{"overdueSlack": slack, "nodeUnreachableThreshold": threshold}
	b, err := json.Marshal(lifecycle)
	if err != nil {
		a.t.Fatal(err)
	}
	a.ext.configData["lifecycle"] = string(b)
}

// stopOwnNodeUnder gives the target a node of its own that stops reporting
// now, for ticks ticks, and returns the pod. The node reads Ready Unknown
// and carries the unreachable taint from this instant.
func (a *availableWorld) stopOwnNodeUnder(target availableTarget, ticks int) *corev1.Pod {
	a.t.Helper()
	pod := a.targetPod(target)
	if pod == nil {
		a.t.Fatalf("no live pod is %s", target)
	}
	now := metav1.NewTime(a.clock.Now())
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: availableOwnNode, UID: availableOwnNode + "-uid"},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoExecute, TimeAdded: &now}}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown,
			Reason: "NodeStatusUnknown", Message: "Kubelet stopped posting node status.", LastTransitionTime: now}}},
	}
	a.refuse.own(func() {
		if err := a.cli.Create(a.ctx, node); err != nil {
			a.t.Fatalf("create %s: %v", availableOwnNode, err)
		}
		pod.Spec.NodeName = availableOwnNode
		if err := a.cli.Update(a.ctx, pod); err != nil {
			a.t.Fatalf("place %s on %s: %v", pod.Name, availableOwnNode, err)
		}
	})
	a.stoppedFrom[pod.UID], a.stoppedUntil[pod.UID] = a.tickNo+1, a.tickNo+1+ticks
	a.note("cluster: %s sits on %s, whose kubelet stops for %d ticks", pod.Name, availableOwnNode, ticks)
	return pod
}

// ownNodeReports is the stopped node reporting again: it reads Ready and
// the node lifecycle controller takes the unreachable taint off.
func (a *availableWorld) ownNodeReports() {
	a.t.Helper()
	a.refuse.own(func() {
		node := &corev1.Node{}
		if err := a.cli.Get(a.ctx, client.ObjectKey{Name: availableOwnNode}, node); err != nil {
			a.t.Fatalf("read %s: %v", availableOwnNode, err)
		}
		node.Spec.Taints = nil
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady", LastTransitionTime: metav1.NewTime(a.clock.Now())}}
		if err := a.cli.Update(a.ctx, node); err != nil {
			a.t.Fatalf("write %s: %v", availableOwnNode, err)
		}
	})
	a.note("cluster: the kubelet of %s is back", availableOwnNode)
}

// nodeAwayReading is what a staging read of the decode side, in ticks
// counted from the stop.
type nodeAwayReading struct {
	// pod is the stopped pod's name, instance its Instance and revision
	// the revision the roll moves the side onto.
	pod      string
	instance int32
	revision string
	// fewest is the fewest decode Instances that served at the end of a
	// tick.
	fewest int
	// waitedFrom and waitedLast are the first and the last tick whose
	// passes left on the decoder replica a rollout hold that counts the
	// stopped pod's Instance as not serving, and waited is how many ticks
	// did; all zero when none did.
	waitedFrom, waitedLast, waited int
	// holds counts those ticks by the hold's gate and reason, and readies
	// by the replica's Ready condition beside the hold.
	holds, readies map[string]int
	// doneAt is the first tick every live decode pod ran the pushed image,
	// removedAt the first tick the stopped pod was gone, and servedAgainAt
	// the first tick its Instance served again; zero when the staging
	// ended first.
	doneAt, removedAt, servedAgainAt int
	// leftOff is the live decode pods off the pushed image when the
	// staging ended, and servingAtEnd the decode Instances that served.
	leftOff, servingAtEnd int
	// events are the controllers' events that name the stopped pod.
	events []string
	// pastBudget counts the passes that took a serving decode Instance out
	// while fewer served than the budget allows.
	pastBudget int
}

func (r *nodeAwayReading) String() string {
	return fmt.Sprintf("pod %s of Instance %d: fewest serving %d, a hold counted the Instance on %d ticks from tick %d to tick %d (%v beside Ready %v), done at tick %d, pod gone at tick %d, Instance serving again at tick %d, %d pods off the pushed image and %d Instances serving at the end, %d passes took a serving Instance out past the budget, events %q",
		r.pod, r.instance, r.fewest, r.waited, r.waitedFrom, r.waitedLast, r.holds, r.readies, r.doneAt, r.removedAt, r.servedAgainAt, r.leftOff, r.servingAtEnd, r.pastBudget, r.events)
}

// readNodeAwayTick reads the decode side as one tick left it.
func (a *availableWorld) readNodeAwayTick(r *nodeAwayReading, uid string, tick int) {
	a.t.Helper()
	dec := v1beta1.DecoderComponent
	ir := &v1beta1.InferenceReplica{}
	if err := a.cli.Get(a.ctx, a.replicaKey(dec), ir); err != nil {
		a.t.Fatalf("read the decoder replica: %v", err)
	}
	side := a.before[dec]
	if serving := len(side.serving); serving < r.fewest {
		r.fewest = serving
	}
	r.servingAtEnd = len(side.serving)
	r.leftOff = a.offThePushedImage(dec)
	if r.doneAt == 0 && r.leftOff == 0 {
		r.doneAt = tick
	}
	gone := true
	for _, p := range side.byIdx[r.instance] {
		gone = gone && string(p.uid) != uid
	}
	if r.removedAt == 0 && gone {
		r.removedAt = tick
	}
	if r.servedAgainAt == 0 && side.serving[r.instance] {
		r.servedAgainAt = tick
	}
	hold := ir.Status.RolloutHold
	if hold == nil || !strings.Contains(hold.Reason, fmt.Sprintf("Instance %d on revision %s not serving", r.instance, r.revision)) {
		return
	}
	if r.waitedFrom == 0 {
		r.waitedFrom = tick
	}
	r.waitedLast = tick
	r.waited++
	r.holds[string(hold.Gate)+": "+hold.Reason]++
	ready := "none"
	if cond := apimeta.FindStatusCondition(ir.Status.Conditions, inferencereplica.InferenceReplicaConditionReady); cond != nil {
		ready = fmt.Sprintf("%s/%s", cond.Status, cond.Reason)
	}
	r.readies[ready]++
}

// playNodeAway restores the first rotation of the plain roll, stops the
// node under a decode pod the roll has moved for away ticks, and reads the
// decode side for ticks ticks.
func (a *availableWorld) playNodeAway(t *testing.T, snaps map[refusedMoment]*refusedSnapshot, away, ticks int) *nodeAwayReading {
	t.Helper()
	if snaps[atRotating] == nil {
		t.Fatalf("the plain roll never reached %s", atRotating)
	}
	a.restore(snaps[atRotating])
	a.path = "a node stops under a decode pod the roll has moved"
	pod := a.stopOwnNodeUnder(availableDecodeNew, away)
	idx, _ := query.InstanceIdxFromLabels(pod)
	ir := &v1beta1.InferenceReplica{}
	if err := a.cli.Get(a.ctx, a.replicaKey(v1beta1.DecoderComponent), ir); err != nil {
		t.Fatalf("read the decoder replica: %v", err)
	}
	r := &nodeAwayReading{pod: pod.Name, instance: idx, revision: ir.Status.UpdateRevision, fewest: int(a.counts[v1beta1.DecoderComponent]),
		holds: map[string]int{}, readies: map[string]int{}}
	for tick := 1; tick <= ticks; tick++ {
		if tick == away+1 {
			a.ownNodeReports()
		}
		a.tick()
		a.readNodeAwayTick(r, string(pod.UID), tick)
	}
	r.pastBudget = a.classes["roll-takes-a-serving-instance-out-past-the-budget"]
	a.events.mu.Lock()
	defer a.events.mu.Unlock()
	for _, line := range a.events.lines {
		if strings.Contains(line, pod.Name) {
			r.events = append(r.events, line)
		}
	}
	return r
}

// nodeAwayStaging is one staging of this file: how long the node stays
// away, how long the staging runs, and what the decode side read.
type nodeAwayStaging struct {
	away, ticks int
	read        *nodeAwayReading
}

// stageNodeAway builds 12 prefill and 8 decode pods under the strategy,
// one decode pod allowed down, with the force-delete policy when asked for,
// and plays a node that stays away under a decode pod the roll has moved.
// The node is away for away, or for good when away is zero, and the staging
// runs until the tick ticksFor names from the world.
func stageNodeAway(t *testing.T, strategy v1beta1.UpdateStrategyType, policy bool, away time.Duration, ticksFor func(a *availableWorld) int) nodeAwayStaging {
	t.Helper()
	a, snaps := availableLockWorld(t, availableConfigOf(12, 8, 1, strategy, strategy, false, 0))
	if policy {
		a.useForceDeletePolicy(availablePolicySlack, availablePolicyThreshold)
	}
	s := nodeAwayStaging{away: availableForGood, ticks: ticksFor(a)}
	if away > 0 {
		s.away = ticksOf(away)
	}
	s.read = a.playNodeAway(t, snaps, s.away, s.ticks)
	t.Logf("%d ticks of 5 s: %s", s.ticks, s.read)
	return s
}

// pastTheAttemptDeadline runs a staging until five minutes past the
// chart's deadline for one attempt.
func pastTheAttemptDeadline(a *availableWorld) int {
	return a.attemptDeadlineTicks() + ticksOf(availablePastTheDeadline)
}

// threeOutages runs a staging for three times the five-minute outage: long
// enough for the roll to end after the node is back or the pod is replaced.
func threeOutages(*availableWorld) int { return 3 * ticksOf(availableOutage) }

// TestAvailableMeansReady_ANodeUnderAMovedPodStaysAway: 12 prefill and 8
// decode pods, one decode pod allowed down. The roll has moved one decode
// pod onto version 2 when the node under it stops reporting, and the node
// stays away. Ready is False on the pod from then on. The roll's budget
// reads the pod's Instance by its containers' readiness and its serving
// gate, which the stopped kubelet left true, so the Instance holds no slot
// and the roll does not wait for it. This is the known picture of
// TestAvailableMeansReady_ANodeStopsUnderAPodTheRollAlreadyMoved, two pods
// down against one allowed, read here for as long as the node stays away.
//
// Under RecreatePod the roll takes the next serving pod out beside it: 6
// of 8 decode pods serve against a budget of one, and the roll has moved
// every other pod before five minutes have passed. With the node away for
// good the side then stays at 7 of 8 for the rest of 35 minutes. Under the
// force-delete policy the engine removes the pod once the node has been
// unreachable for five minutes and the rebuilt pod serves. After an outage
// of five minutes the pod is Ready again. Under SurgeThenDrain the roll
// takes no serving pod out and goes on all the same: 7 of 8 serve
// throughout.
func TestAvailableMeansReady_ANodeUnderAMovedPodStaysAway(t *testing.T) {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	// wentOn fails the staging unless the roll went on beside the pod: no
	// hold ever counted the pod's Instance, every other pod moved, and the
	// fewest decode Instances that served are the number given.
	wentOn := func(t *testing.T, s nodeAwayStaging, fewest int) {
		t.Helper()
		r := s.read
		if r.fewest != fewest || r.waited != 0 || r.doneAt == 0 {
			t.Fatalf("the picture moved: the roll did not go on beside the pod with %d of 8 serving at the fewest: %s", fewest, r)
		}
		if past := fewest < 7; (r.pastBudget > 0) != past {
			t.Fatalf("the picture moved: %d passes took a serving pod out past the budget of one: %s", r.pastBudget, r)
		}
	}
	t.Run("for_good", func(t *testing.T) {
		t.Parallel()
		s := stageNodeAway(t, recreate, false, 0, pastTheAttemptDeadline)
		wentOn(t, s, 6)
		if r := s.read; r.doneAt > ticksOf(availableOutage) || r.removedAt != 0 || r.servingAtEnd != 7 {
			t.Fatalf("the picture moved: the roll did not end inside five minutes with the pod still there and 7 of 8 serving at the end: %s", r)
		}
	})
	t.Run("for_good_under_the_force_delete_policy", func(t *testing.T) {
		t.Parallel()
		s := stageNodeAway(t, recreate, true, 0, threeOutages)
		wentOn(t, s, 6)
		r := s.read
		if r.removedAt <= ticksOf(availableOutage) || r.doneAt >= r.removedAt || len(r.events) != 1 || !strings.Contains(r.events[0], "PodForceDeleted") {
			t.Fatalf("the picture moved: the policy did not remove the pod once its node had been unreachable for five minutes, after the roll had ended: %s", r)
		}
		if r.servedAgainAt == 0 || r.servingAtEnd != 8 {
			t.Fatalf("the rebuilt pod never served: %s", r)
		}
	})
	t.Run("for_five_minutes", func(t *testing.T) {
		t.Parallel()
		s := stageNodeAway(t, recreate, false, availableOutage, threeOutages)
		wentOn(t, s, 6)
		if r := s.read; r.doneAt > s.away || r.servedAgainAt != s.away+1 || r.servingAtEnd != 8 {
			t.Fatalf("the picture moved: the roll did not end before the node was back, or the pod did not serve again on the node's first tick back: %s", r)
		}
	})
	t.Run("for_good_under_a_surge", func(t *testing.T) {
		t.Parallel()
		s := stageNodeAway(t, surge, false, 0, pastTheAttemptDeadline)
		wentOn(t, s, 7)
		if r := s.read; r.removedAt != 0 || r.servingAtEnd != 7 {
			t.Fatalf("the picture moved: the surge roll did not end with the pod still there and 7 of 8 serving: %s", r)
		}
	})
}
