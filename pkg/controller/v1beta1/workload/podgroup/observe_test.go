package podgroup_test

// Gang verdict classification: what one Instance's PodGroup says about
// the gang, reduced to the states the escalation pass acts on.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const observeOwnerUID = types.UID("owner-uid")

// ownedGroup is a PodGroup this owner controls, sized for a two-member
// gang with the schedule timeout OME itself stamped.
func ownedGroup(timeoutSeconds int32) *schedulingv1alpha1.PodGroup {
	return &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-engine-0",
			Namespace: "prod",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "ome.io/v1beta1",
				Kind:       "InferenceReplica",
				Name:       "svc-engine",
				UID:        observeOwnerUID,
				Controller: ptrBool(true),
			}},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{MinMember: 2, ScheduleTimeoutSeconds: &timeoutSeconds},
	}
}

func ptrBool(b bool) *bool { return &b }

// TestObserveGang_AbsentOrHealthy: a name nothing occupies, and a gang
// the scheduler placed, both report nothing for the row to wait on.
func TestObserveGang_AbsentOrHealthy(t *testing.T) {
	if got := podgroup.ObserveGang("svc-engine-0", nil, false, observeOwnerUID); got.State != workload.GangStateNone {
		t.Errorf("absent PodGroup: got state %q want none", got.State)
	}
	running := ownedGroup(600)
	running.Status.Phase = schedulingv1alpha1.PodGroupRunning
	running.Status.Running = 2
	if got := podgroup.ObserveGang("svc-engine-0", running, true, observeOwnerUID); got.State != workload.GangStateNone {
		t.Errorf("running PodGroup: got state %q want none", got.State)
	}
}

// TestObserveGang_OwnershipConflict: a same-named object controlled by
// somebody else is a collision, and the evidence names the controller
// that holds it so an operator can find the other owner.
func TestObserveGang_OwnershipConflict(t *testing.T) {
	foreign := ownedGroup(600)
	foreign.OwnerReferences[0].UID = types.UID("other-uid")
	foreign.OwnerReferences[0].Kind = "StatefulSet"
	foreign.OwnerReferences[0].Name = "legacy"

	got := podgroup.ObserveGang("svc-engine-0", foreign, true, observeOwnerUID)
	if got.State != workload.GangStateOwnershipConflict {
		t.Fatalf("state: got %q want %q", got.State, workload.GangStateOwnershipConflict)
	}
	if !containsAll(got.Message, "svc-engine-0", "StatefulSet", "legacy") {
		t.Errorf("message %q must name the PodGroup and the foreign controller", got.Message)
	}
}

// TestObserveGang_Terminating: an owned object whose deletion has begun
// still occupies the deterministic name, so the row waits for it to be
// collected rather than reusing the name.
func TestObserveGang_Terminating(t *testing.T) {
	terminating := ownedGroup(600)
	deleted := metav1.Now()
	terminating.DeletionTimestamp = &deleted
	terminating.Status.Phase = schedulingv1alpha1.PodGroupRunning

	got := podgroup.ObserveGang("svc-engine-0", terminating, true, observeOwnerUID)
	if got.State != workload.GangStateTerminating {
		t.Fatalf("state: got %q want %q", got.State, workload.GangStateTerminating)
	}
	if !containsAll(got.Message, "svc-engine-0") {
		t.Errorf("message %q must name the PodGroup", got.Message)
	}
}

// TestObserveGang_PhaseFailed: the gang scheduler's own terminal verdict
// outranks every other reading of the object.
func TestObserveGang_PhaseFailed(t *testing.T) {
	failed := ownedGroup(600)
	failed.Status.Phase = schedulingv1alpha1.PodGroupFailed
	failed.Status.Failed = 2

	got := podgroup.ObserveGang("svc-engine-0", failed, true, observeOwnerUID)
	if got.State != workload.GangStateFailed {
		t.Fatalf("state: got %q want %q", got.State, workload.GangStateFailed)
	}
	if !containsAll(got.Message, "svc-engine-0", "Failed") {
		t.Errorf("message %q must name the PodGroup and its phase", got.Message)
	}
}

// TestObserveGang_SchedulingPhasesReportNothing: Pending and Scheduling
// are the ordinary shape of a gang coming up — neither says a placement
// is impossible. Reading either as a wait would park the deadline of
// every gang create. The unplaceable verdict travels on the member pods
// instead, so no row of any state ever sees an Unschedulable or
// ScheduleTimeout reading: the classifier is where that is settled.
func TestObserveGang_SchedulingPhasesReportNothing(t *testing.T) {
	for _, phase := range []schedulingv1alpha1.PodGroupPhase{
		schedulingv1alpha1.PodGroupPending,
		schedulingv1alpha1.PodGroupScheduling,
		schedulingv1alpha1.PodGroupUnknown,
		schedulingv1alpha1.PodGroupFinished,
	} {
		pg := ownedGroup(600)
		pg.Status.Phase = phase
		pg.Status.ScheduleStartTime = metav1.NewTime(time.Now().Add(-10 * time.Minute))
		if got := podgroup.ObserveGang("svc-engine-0", pg, true, observeOwnerUID); got.State != workload.GangStateNone {
			t.Errorf("phase %s: got state %q want none", phase, got.State)
		}
	}
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}
