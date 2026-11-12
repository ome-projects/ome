package workload_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Three single-pod Instances serve version 1 and roll by recreate, one at
// a time, to a version 2 whose pod never passes readiness. The first
// Instance's attempt ends at the ready timeout and the roll waits on
// version 2's retry schedule. Then another Instance's only pod is lost.
// The roll cannot take that Instance while the schedule holds it, so
// leaving it to the roll would keep it down until the schedule or an
// operator moves the roll: it is rebuilt on version 1 instead and serves
// again while the roll stays held. Under RecreateInstanceOnPodRestart the
// restart pass rebuilds it; under None the Create pass refills it. No pod
// of the lost Instance is created on version 2 while the schedule holds,
// version 2's record keeps its one failed attempt, and the failed Instance
// keeps its attempt. The contrast: with a healthy version 2 the same loss
// is left to the roll, which rebuilds the Instance on version 2 at once and
// records no repair.
func TestReconcile_InstanceLostWhileTheRollIsHeld_RebuiltOnItsOwnRevision(t *testing.T) {
	const (
		replicas     = 3
		readyTimeout = 3 * time.Minute
		retryDelay   = 30 * time.Minute
	)
	for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyRecreateInstance, workloadtypes.RestartPolicyNone} {
		t.Run(string(policy), func(t *testing.T) {
			h := offTargetCrashHarness(t, false, workloadtypes.UpdateStrategyRecreatePod, 0, 1, policy, replicas)
			h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: readyTimeout}
			h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: retryDelay, MaxDelay: 2 * retryDelay, Multiplier: 2}
			h.stepCap = 30 * time.Second

			// Version 2 is version 1's image behind a readiness probe its pod
			// never passes.
			unreadySpec := h.podSpec(goodImage)
			unreadySpec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: neverReadyPath, Port: intstr.FromInt32(8080)},
			}}
			revUnready := h.ensureRevision(unreadySpec)
			unreadyHash := query.RevisionOf(revUnready).Hash()
			h.setTarget(revUnready, goodImage)
			h.desired.PodSpec = unreadySpec

			// The roll is held once one attempt failed at the ready timeout and
			// version 2's record waits in backoff; the other two Instances
			// still serve version 1.
			held := func() bool {
				b := retryBlockOf(h, revUnready.Name)
				return b != nil && b.State == workloadtypes.RetryBlockBackoff && len(h.landedOn(h.revV1)) == replicas-1
			}
			if !h.run(60, held) {
				h.dumpState("held roll")
				t.Fatalf("the roll never failed an attempt at %s and waited on its retry schedule", revUnready.Name)
			}
			var failed, lost int32 = -1, -1
			for _, s := range h.irStatuses() {
				switch {
				case s.Phase == workloadtypes.InstancePhaseFailed:
					failed = s.Index
				case lost < 0 && h.untouchedOn(s.Index, h.revV1):
					lost = s.Index
				}
			}
			if failed < 0 || lost < 0 {
				h.dumpState("held roll")
				t.Fatalf("the story needs one Failed Instance and one still serving version 1")
			}

			breakInstance(t, h, lost, false, podSetLost)
			h.events = nil
			lostAt := h.clk.Now()
			rebuilt := func() bool {
				if !h.untouchedOn(lost, h.revV1) || !h.instanceServes(lost) {
					return false
				}
				for _, pod := range h.podsOf(lost) {
					if !pod.CreationTimestamp.Time.After(lostAt) {
						return false
					}
				}
				return true
			}
			stillHeld := func() {
				for _, pod := range h.podsOf(lost) {
					if pod.CreationTimestamp.Time.After(lostAt) && pod.Labels[query.LabelRevisionHash] == unreadyHash {
						h.dumpState("rebuilt on the held version")
						t.Fatalf("pod %s of the lost Instance was created on %s, the version the roll is held on", pod.Name, revUnready.Name)
					}
				}
				if b := retryBlockOf(h, revUnready.Name); b == nil || b.State != workloadtypes.RetryBlockBackoff || b.AttemptsStarted != 1 {
					h.dumpState("record moved")
					t.Fatalf("version 2's record must keep its one failed attempt in backoff, got %+v", b)
				}
				if s := h.instance(failed); s == nil || s.Phase != workloadtypes.InstancePhaseFailed {
					h.dumpState("failed Instance moved")
					t.Fatalf("the failed Instance must keep its attempt while the schedule holds, got %+v", s)
				}
			}
			if !h.runWithInvariant(60, rebuilt, stillHeld) {
				h.dumpState("after the loss")
				t.Fatalf("Instance %d was not rebuilt on version 1 while the roll was held; last hold %+v", lost, h.lastHold())
			}
			if repaired := h.sawEvent(workloadtypes.EventReasonRestartTriggered); repaired != (policy == workloadtypes.RestartPolicyRecreateInstance) {
				t.Fatalf("RestartTriggered recorded = %v under %s: the restart pass rebuilds under RecreateInstanceOnPodRestart and the Create pass refills under None; events %v", repaired, policy, h.events)
			}
		})
	}

	t.Run("a roll that is not held takes the lost Instance", func(t *testing.T) {
		h := offTargetCrashHarness(t, false, workloadtypes.UpdateStrategyRecreatePod, 0, 1, workloadtypes.RestartPolicyRecreateInstance, replicas)
		h.setTarget(h.revFixed, fixedImage)
		const lost = int32(replicas - 1)
		breakInstance(t, h, lost, false, podSetLost)
		h.events = nil
		lostAt := h.clk.Now()
		fixedHash := query.RevisionOf(h.revFixed).Hash()
		leftToTheRoll := func() {
			if h.repairInFlight() {
				h.dumpState("repair opened")
				t.Fatalf("a repair opened on an Instance a roll that can start takes")
			}
			for _, pod := range h.podsOf(lost) {
				if pod.CreationTimestamp.Time.After(lostAt) && pod.Labels[query.LabelRevisionHash] != fixedHash {
					t.Fatalf("pod %s was created off the roll target after the loss; the roll owns the rebuild", pod.Name)
				}
			}
		}
		if !h.runWithInvariant(100, func() bool { return h.settledOn(h.revFixed, replicas) }, leftToTheRoll) {
			h.dumpState("after the loss")
			t.Fatalf("the roll never rebuilt the lost Instance on %s; last hold %+v", h.revFixed.Name, h.lastHold())
		}
		if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
			t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
		}
	})
}

// retryBlockOf is the harness's record for rev, nil when none is kept.
func retryBlockOf(h *recoveryHarness, rev string) *workloadtypes.RetryBlock {
	for i := range h.blocks {
		if h.blocks[i].TargetRevision == rev {
			return &h.blocks[i]
		}
	}
	return nil
}
