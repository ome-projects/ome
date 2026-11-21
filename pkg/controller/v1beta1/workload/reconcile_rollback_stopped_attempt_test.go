package workload_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// newStoppedReplacementHarness is three single-pod Instances serving
// version 1 under RecreatePod, one Instance offline at a time, with a
// 10-minute ready timeout. The operator pushes version 2. The first
// Instance the roll replaces serves version 2. The second Instance's
// version 2 pod runs but never passes readiness, so the ready timeout stops
// that update: the row reads Failed and keeps its stopped Update toward
// version 2, and its version 2 pod stands. The operator then holds every
// Instance with a partition of 3, as a stopped canary does. It returns the
// harness, the Instance that serves version 2 and the stopped Instance.
func newStoppedReplacementHarness(t *testing.T) (*recoveryHarness, int32, int32) {
	t.Helper()
	const replicas = 3
	h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(1024)
	h.replicas = replicas
	policy := workloadtypes.RestartPolicyRecreateInstance
	maxSurge, maxUnavailable := intstr.FromInt(0), intstr.FromInt(1)
	h.lifecycle = workloadtypes.Lifecycle{
		RestartPolicy:        &policy,
		InstanceReadyTimeout: &metav1.Duration{Duration: 10 * time.Minute},
		UpdateStrategy: &workloadtypes.UpdateStrategy{
			Type:          workloadtypes.UpdateStrategyRecreatePod,
			RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
		},
	}
	// The first retry comes due long after the story ends, so the stopped
	// update stays stopped until the operator acts.
	h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 6 * time.Hour, MaxDelay: 12 * time.Hour, Multiplier: 2}
	h.stepCap = 30 * time.Second
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("the initial create never settled three Instances on version 1")
	}

	h.setTarget(h.revFixed, fixedImage)
	fault := &secondReplacementFault{h: h, replicas: replicas, served: -1, stopped: -1}
	if !h.runWithInvariant(120, fault.updateStopped, fault.apply) {
		h.dumpState("second replacement")
		t.Fatalf("the ready timeout never stopped the second Instance's update toward version 2: served=%d stopped=%d row=%+v",
			fault.served, fault.stopped, h.instance(fault.stopped))
	}
	served, stopped := fault.served, fault.stopped
	if !servingReady(h, served, 1) || h.instance(served).RunningRevision != h.revFixed.Name {
		h.dumpState("second replacement")
		t.Fatalf("Instance %d must serve version 2 when the second update stops, got %+v", served, h.instance(served))
	}

	all := int32(replicas)
	h.lifecycle.UpdateStrategy.RollingUpdate.Partition = &all
	h.setTarget(h.target, h.desired.PodSpec.Containers[0].Image)
	h.settle(3)
	if !fault.updateStopped() || !anyPodOnImage(h, stopped, fixedImage) {
		h.dumpState("held")
		t.Fatalf("under the partition the stopped update must keep its row and its version 2 pod, got row=%+v pods=%d",
			h.instance(stopped), len(h.podsOf(stopped)))
	}
	h.events = nil
	return h, served, stopped
}

// secondReplacementFault makes the version 2 pod of the second Instance the
// roll replaces fail readiness, once the first replaced Instance serves
// version 2. served and stopped name those two Instances, -1 until known.
type secondReplacementFault struct {
	h               *recoveryHarness
	replicas        int32
	served, stopped int32
}

// apply finds the first Instance that serves version 2, then marks the
// version 2 pod of the next Instance the roll replaces as never ready.
func (f *secondReplacementFault) apply() {
	if f.stopped >= 0 {
		return
	}
	if f.served < 0 {
		f.served = f.firstServingVersion2()
	}
	if f.served >= 0 {
		f.stopped = f.markNextReplacementUnready()
	}
}

// firstServingVersion2 is the first Instance that serves version 2, or -1.
func (f *secondReplacementFault) firstServingVersion2() int32 {
	for idx := int32(0); idx < f.replicas; idx++ {
		if servingReady(f.h, idx, 1) && f.h.instance(idx).RunningRevision == f.h.revFixed.Name {
			return idx
		}
	}
	return -1
}

// markNextReplacementUnready marks the not-yet-ready version 2 pod of an
// Instance other than served as failing readiness and returns its Instance,
// or -1 when no such pod exists yet.
func (f *secondReplacementFault) markNextReplacementUnready() int32 {
	for idx := int32(0); idx < f.replicas; idx++ {
		if idx == f.served {
			continue
		}
		for _, pod := range f.h.podsOf(idx) {
			if pod.Spec.Containers[0].Image == fixedImage && !podConditionTrue(pod, corev1.ContainersReady) {
				f.h.readinessFails[pod.UID] = true
				return idx
			}
		}
	}
	return -1
}

// updateStopped reports that the ready timeout stopped the marked
// Instance's update: the row reads Failed and keeps its stopped Update
// toward version 2.
func (f *secondReplacementFault) updateStopped() bool {
	if f.stopped < 0 {
		return false
	}
	row := f.h.instance(f.stopped)
	return row != nil && row.Phase == workloadtypes.InstancePhaseFailed && row.Operation != nil &&
		row.Operation.Type == workloadtypes.InstanceOperationUpdate && row.Operation.Step == workloadtypes.UpdateStepParked &&
		row.Operation.TargetRevision == f.h.revFixed.Name
}

// anyPodOnImage reports whether Instance idx has a live pod running image.
func anyPodOnImage(h *recoveryHarness, idx int32, image string) bool {
	for _, pod := range h.podsOf(idx) {
		if pod.Spec.Containers[0].Image == image {
			return true
		}
	}
	return false
}

// A rollback to version 1 removes the version 2 pod of the stopped update,
// which no push asks for any more. The Instance it leaves has no pod, so
// the rollback must also rebuild it on version 1. Until that Instance
// serves, it holds the one Instance the rollback may take offline, so the
// Instance that serves version 2 can roll back only after it. The rollback
// ends with all three Instances serving version 1.
func TestRollBack_RebuildsTheInstanceWhoseStoppedReplacementItRemoved(t *testing.T) {
	h, served, stopped := newStoppedReplacementHarness(t)
	stoppedV2 := map[string]bool{}
	for _, pod := range h.podsOf(stopped) {
		stoppedV2[string(pod.UID)] = true
	}

	h.lifecycle.UpdateStrategy.RollingUpdate.Partition = nil
	h.setTarget(h.revV1, goodImage)
	withinBudget := func() {
		if got := h.instancesServingOn(goodImage) + h.instancesServingOn(fixedImage); got < 2 {
			h.dumpState("rollback")
			t.Fatalf("the rollback took a second Instance offline under a budget of one: %d of 3 serve", got)
		}
		for _, pod := range h.podsOf(stopped) {
			if pod.Spec.Containers[0].Image == fixedImage && !stoppedV2[string(pod.UID)] {
				h.dumpState("rollback")
				t.Fatalf("the rollback rebuilt Instance %d on version 2, which no push asks for: pod %s", stopped, pod.Name)
			}
		}
	}
	if !h.runWithInvariant(120, func() bool { return h.settledOn(h.revV1, 3) }, withinBudget) {
		hold := ""
		if last := h.lastHold(); last != nil {
			hold = string(last.Gate) + ": " + last.Reason
		}
		h.dumpState("rollback")
		t.Fatalf("the rollback never settled every Instance on version 1: stopped Instance %d reads %+v with %d pod(s), Instance %d reads %+v, last hold %q",
			stopped, h.instance(stopped), len(h.podsOf(stopped)), served, h.instance(served), hold)
	}
	if !h.sawEvent(workloadtypes.EventReasonSupersededWreckageCleaned) {
		t.Fatalf("the rollback deletes the stopped update's version 2 pod and records the delete as an event, got events %v", h.events)
	}
	for idx := int32(0); idx < 3; idx++ {
		for _, pod := range h.podsOf(idx) {
			if !podreadiness.IsServing(pod) || pod.Spec.Containers[0].Image != goodImage {
				t.Fatalf("Instance %d must serve version 1 after the rollback, pod %s runs %s", idx, pod.Name, pod.Spec.Containers[0].Image)
			}
		}
	}
}
