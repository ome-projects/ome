package podgroup

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ObserveGang classifies one Instance's PodGroup into the reading the
// escalation pass acts on. Pure compute over an inventory entry; the
// caller supplies whether the deterministic name is occupied at all.
//
// Order matters. A name held by another controller is reported before
// anything is read off the object, because none of its status belongs to
// this owner. A terminating object comes next: its phase describes a
// gang on its way out, not one this Instance is waiting for. Only then
// does the scheduler's own verdict count.
//
// SCHEDULABILITY IS DELIBERATELY NOT READ HERE. PodGroupStatus
// distinguishes only "fewer members than minMember exist yet" from "they
// exist and are being placed" — neither says whether a placement is
// possible, and both are the ordinary shape of a healthy gang coming up.
// Whether a gang can be placed reaches this controller on its MEMBER
// PODS, whose PodScheduled=False/Unschedulable conditions the gang
// scheduler writes with its own explanation; the pod-level scheduler hold
// reads those and holds the Instance on them.
func ObserveGang(name string, pg *schedulingv1alpha1.PodGroup, found bool, ownerUID types.UID) workload.GangObservation {
	obs := workload.GangObservation{Name: name}
	if !found || pg == nil {
		return obs
	}
	if !ControlledByUID(pg, ownerUID) {
		obs.State = workload.GangStateOwnershipConflict
		obs.Message = fmt.Sprintf("PodGroup %s is controlled by %s, not by this owner", name, controllerDescription(pg))
		return obs
	}
	if pg.DeletionTimestamp != nil {
		obs.State = workload.GangStateTerminating
		obs.Message = fmt.Sprintf("PodGroup %s is terminating; the gang is announced again once it is collected", name)
		return obs
	}
	if pg.Status.Phase == schedulingv1alpha1.PodGroupFailed {
		obs.State = workload.GangStateFailed
		obs.Message = fmt.Sprintf("PodGroup %s reported phase %s: %d of %d members failed",
			name, pg.Status.Phase, pg.Status.Failed, pg.Spec.MinMember)
	}
	return obs
}

// controllerDescription names the controller that holds a colliding
// object, so an operator reading the Instance's failure can go straight
// to the other owner instead of guessing which controller wrote it.
func controllerDescription(pg *schedulingv1alpha1.PodGroup) string {
	ref := metav1.GetControllerOfNoCopy(pg)
	if ref == nil {
		return "no controller"
	}
	return fmt.Sprintf("%s/%s", ref.Kind, ref.Name)
}
