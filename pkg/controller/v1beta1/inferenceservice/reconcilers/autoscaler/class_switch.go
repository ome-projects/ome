package autoscaler

import (
	"context"
	"errors"
	"fmt"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/keda"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// A Component switches between two scalers that cannot run side by side:
// OME's own HPA, named after the Component, and a KEDA ScaledObject, which
// scales through an HPA that KEDA creates and controls. KEDA's admission
// webhook rejects a ScaledObject while another HPA scales the same target,
// and two HPAs on one target fight over its replica count.
//
// Moving to KEDA, OME hands its HPA over instead of deleting it: the new
// ScaledObject names that HPA and carries KEDA's transfer annotation, and
// KEDA takes the HPA over in place, keeping the same object. The Component
// keeps a working scaler throughout, and a ScaledObject that cannot be
// created leaves the HPA untouched.
//
// Moving back, OME checks that its HPA would be valid, deletes the
// ScaledObject and every HPA KEDA created for it, and creates its own HPA
// only after it has seen them gone.

// TransitionPendingError reports that a switch between OME's HPA and a KEDA
// ScaledObject waits for KEDA, or for a deletion, to finish. Nothing is wrong
// with the configuration: callers requeue and try again.
type TransitionPendingError struct {
	// Reason is one of the AutoscalerSwitched reasons in the v1beta1 API.
	Reason  string
	Message string
}

func (e *TransitionPendingError) Error() string { return e.Message }

// IsTransitionPending reports whether err only asks the caller to retry.
func IsTransitionPending(err error) bool {
	var pending *TransitionPendingError
	return errors.As(err, &pending)
}

func transitionPending(reason, format string, args ...any) error {
	return &TransitionPendingError{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// hpaControl says who controls the HPA named after the Component.
type hpaControl int

const (
	// hpaAbsent: no HPA carries the Component's name.
	hpaAbsent hpaControl = iota
	// hpaControlledByWorkload: the Component's owner controls the HPA, so it
	// is the HPA OME created for class hpa.
	hpaControlledByWorkload
	// hpaControlledByScaledObject: the Component's live ScaledObject controls
	// the HPA, so KEDA manages it.
	hpaControlledByScaledObject
	// hpaLeftByScaledObject: an earlier ScaledObject with the Component's
	// ScaledObject name controls the HPA; that ScaledObject no longer exists.
	hpaLeftByScaledObject
)

// componentHPA is the HPA named after the Component and who controls it.
type componentHPA struct {
	obj     *autoscalingv2.HorizontalPodAutoscaler
	control hpaControl
	// targetMatches reports whether the HPA scales the Component's target.
	targetMatches bool
}

// inspectComponentHPA reads the HPA named after the Component and works out
// who controls it, matching controllers by UID. An HPA controlled by anything
// else is an ownership error: OME never takes over or deletes a scaler it did
// not create.
func inspectComponentHPA(ctx context.Context, p DispatchParams, liveSO *kedav1.ScaledObject) (componentHPA, error) {
	obj, err := getHPAIfExists(ctx, p.Client, p.Namespace, p.Name)
	if err != nil || obj == nil {
		return componentHPA{}, err
	}
	state := componentHPA{obj: obj, targetMatches: sameScaleTarget(obj.Spec.ScaleTargetRef, p.ScaleTargetRef)}
	managed, err := controlledByExpectedOrVerifiedModeBridge(ctx, p.Client, obj, p.Owner, p.Labels)
	if err != nil {
		return componentHPA{}, err
	}
	if managed {
		state.control = hpaControlledByWorkload
		return state, nil
	}
	if controller := componentScaledObjectController(obj, p.Name); controller != nil {
		state.control = hpaLeftByScaledObject
		if liveSO != nil && controller.UID == liveSO.UID {
			state.control = hpaControlledByScaledObject
		}
		return state, nil
	}
	return componentHPA{}, requireExpectedController(ctx, p.Client, "HorizontalPodAutoscaler", obj, p.Owner, p.Labels)
}

// componentScaledObjectController returns the object's controller reference
// when it names the Component's ScaledObject, and nil otherwise.
func componentScaledObjectController(obj metav1.Object, componentName string) *metav1.OwnerReference {
	controller := metav1.GetControllerOf(obj)
	if controller == nil ||
		controller.APIVersion != kedav1.GroupVersion.String() ||
		controller.Kind != "ScaledObject" ||
		controller.Name != utils.GetScaledObjectName(componentName) {
		return nil
	}
	return controller
}

// sameScaleTarget compares two scale targets by API group, kind and name. The
// version is ignored: KEDA and the HPA controller resolve the target through
// its group and kind.
func sameScaleTarget(got, want autoscalingv2.CrossVersionObjectReference) bool {
	gotGV, gotErr := schema.ParseGroupVersion(got.APIVersion)
	wantGV, wantErr := schema.ParseGroupVersion(want.APIVersion)
	if gotErr != nil || wantErr != nil {
		return got == want
	}
	return gotGV.Group == wantGV.Group && got.Kind == want.Kind && got.Name == want.Name
}

// scaledObjectHPAPlan says which HPA the ScaledObject names, and what OME does
// with the HPA named after the Component around the ScaledObject write.
type scaledObjectHPAPlan struct {
	// hpaName is advanced.horizontalPodAutoscalerConfig.name on the
	// ScaledObject; empty lets KEDA pick its own name, which happens only
	// when that name fits KEDA's length limit.
	hpaName string
	// takeOver asks KEDA to take over the HPA named after the Component.
	takeOver bool
	// deleteBeforeWrite deletes that HPA before the ScaledObject write. The
	// live ScaledObject already scales the target through its own HPA, and
	// KEDA's webhook rejects ScaledObject writes while another HPA scales
	// the same target.
	deleteBeforeWrite bool
	// deleteAfterWrite deletes that HPA after the write. It scales another
	// target, so it never blocks the ScaledObject.
	deleteAfterWrite bool
}

// planScaledObjectHPA decides how the ScaledObject relates to the HPA named
// after the Component. Only a new ScaledObject starts a takeover. A live
// ScaledObject keeps the HPA name it already uses unless the operator asks for
// another one, because KEDA replaces its HPA whenever that name changes.
func planScaledObjectHPA(p DispatchParams, liveSO *kedav1.ScaledObject, hpa componentHPA) scaledObjectHPAPlan {
	var requested string
	if p.Autoscaler != nil && p.Autoscaler.Keda != nil {
		requested = keda.ConfiguredHPAName(p.Autoscaler.Keda.Advanced)
	}

	var plan scaledObjectHPAPlan
	switch {
	case liveSO == nil:
		// Hand the HPA OME runs for this target to KEDA so the target keeps
		// a scaler. A requested custom name is applied after the takeover.
		if hpa.control == hpaControlledByWorkload && (hpa.targetMatches || requested == p.Name) {
			plan.hpaName = p.Name
		} else {
			plan.hpaName = requested
			plan.deleteAfterWrite = hpa.control == hpaControlledByWorkload
		}
		plan.deleteBeforeWrite = hpa.control == hpaLeftByScaledObject
	case keda.ConfiguredHPAName(liveSO.Spec.Advanced) == p.Name:
		// Keep naming the HPA that was handed over. Once KEDA controls it,
		// a different requested name is an ordinary KEDA rename.
		plan.hpaName = p.Name
		if requested != "" && requested != p.Name &&
			(hpa.control == hpaControlledByScaledObject || hpa.control == hpaAbsent) {
			plan.hpaName = requested
		}
	default:
		plan.hpaName = requested
		plan.deleteBeforeWrite = hpa.control == hpaControlledByWorkload || hpa.control == hpaLeftByScaledObject
	}
	if plan.hpaName == "" {
		// KEDA's own HPA name for a long Component name fails its webhook.
		plan.hpaName = keda.ShortenedHPAName(utils.GetScaledObjectName(p.Name))
	}
	plan.takeOver = plan.hpaName == p.Name
	return plan
}

// verifyKEDATakeover re-reads the ScaledObject and the HPA it names after a
// takeover write. It returns nil once the ScaledObject controls the HPA, the
// HPA scales the Component's target, and KEDA reports the ScaledObject Ready;
// until then it returns a TransitionPendingError.
func verifyKEDATakeover(ctx context.Context, p DispatchParams) error {
	soName := utils.GetScaledObjectName(p.Name)
	so, err := getScaledObjectIfExists(ctx, p.Client, p.Namespace, p.Name)
	if err != nil {
		return err
	}
	if so == nil {
		return transitionPending(v1beta1.AutoscalerSwitchedReasonWaitingForKEDA,
			"waiting to observe ScaledObject %s/%s before KEDA takes over HPA %s", p.Namespace, soName, p.Name)
	}
	hpa, err := inspectComponentHPA(ctx, p, so)
	if err != nil {
		return fmt.Errorf("verify HPA ownership after ScaledObject write (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
	}
	switch hpa.control {
	case hpaControlledByScaledObject:
		if !hpa.targetMatches {
			return transitionPending(v1beta1.AutoscalerSwitchedReasonWaitingForKEDA,
				"KEDA controls HPA %s/%s but has not pointed it at %s %s yet", p.Namespace, p.Name, p.ScaleTargetRef.Kind, p.ScaleTargetRef.Name)
		}
		if ready := so.Status.Conditions.GetReadyCondition(); !ready.IsTrue() {
			return transitionPending(v1beta1.AutoscalerSwitchedReasonScaledObjectNotReady,
				"KEDA controls HPA %s/%s but does not report ScaledObject %s Ready", p.Namespace, p.Name, soName)
		}
		return nil
	case hpaControlledByWorkload:
		controller := metav1.GetControllerOf(hpa.obj)
		return transitionPending(v1beta1.AutoscalerSwitchedReasonWaitingForKEDA,
			"waiting for KEDA to take over HPA %s/%s from %s %s through ScaledObject %s", p.Namespace, p.Name, controller.Kind, controller.Name, soName)
	default:
		return transitionPending(v1beta1.AutoscalerSwitchedReasonWaitingForKEDA,
			"waiting for KEDA to take over HPA %s/%s through ScaledObject %s", p.Namespace, p.Name, soName)
	}
}

// removeKEDAScaler deletes the Component's ScaledObject and the HPAs KEDA
// created for it, so OME's HPA never shares the target with a KEDA HPA and its
// name is free. Before the ScaledObject goes it checks that OME's replacement
// HPA would be accepted, so a bad HPA configuration leaves KEDA scaling. It
// returns nil once nothing KEDA created remains, and a TransitionPendingError
// while deletions are still in progress.
func removeKEDAScaler(ctx context.Context, p DispatchParams, liveSO *kedav1.ScaledObject, hpa componentHPA) error {
	if liveSO == nil && hpa.control == hpaControlledByWorkload {
		return nil
	}
	soName := utils.GetScaledObjectName(p.Name)
	kedaHPAs, err := listScaledObjectHPAs(ctx, p.Client, p.Namespace, soName)
	if err != nil {
		return err
	}
	if liveSO == nil && len(kedaHPAs) == 0 {
		return nil
	}

	if liveSO != nil && liveSO.DeletionTimestamp == nil {
		if hpa.control != hpaControlledByWorkload {
			if err := validateReplacementHPA(ctx, p); err != nil {
				return fmt.Errorf("validate the replacement HPA before deleting ScaledObject %s/%s: %w", p.Namespace, soName, err)
			}
		}
		if err := deleteObserved(ctx, p.Client, liveSO, "ScaledObject", v1beta1.AutoscalerSwitchedReasonWaitingForScaledObjectDeletion); err != nil {
			return err
		}
	}
	for i := range kedaHPAs {
		if kedaHPAs[i].DeletionTimestamp != nil {
			continue
		}
		if err := deleteObserved(ctx, p.Client, &kedaHPAs[i], "HorizontalPodAutoscaler", v1beta1.AutoscalerSwitchedReasonWaitingForHPADeletion); err != nil {
			return err
		}
	}

	remainingSO, err := getScaledObjectIfExists(ctx, p.Client, p.Namespace, p.Name)
	if err != nil {
		return err
	}
	if remainingSO != nil {
		return transitionPending(v1beta1.AutoscalerSwitchedReasonWaitingForScaledObjectDeletion,
			"waiting for KEDA to finish deleting ScaledObject %s/%s before OME creates HPA %s", p.Namespace, soName, p.Name)
	}
	remaining, err := listScaledObjectHPAs(ctx, p.Client, p.Namespace, soName)
	if err != nil {
		return err
	}
	if len(remaining) > 0 {
		return transitionPending(v1beta1.AutoscalerSwitchedReasonWaitingForHPADeletion,
			"waiting for HPA %s/%s, which KEDA created for ScaledObject %s, to be deleted before OME creates HPA %s", p.Namespace, remaining[0].Name, soName, p.Name)
	}
	return nil
}

// listScaledObjectHPAs returns the HPAs in the namespace whose controller is a
// ScaledObject named soName: every HPA KEDA created for the Component's
// current or earlier ScaledObject, whatever name the ScaledObject gave it.
func listScaledObjectHPAs(ctx context.Context, c client.Client, namespace, soName string) ([]autoscalingv2.HorizontalPodAutoscaler, error) {
	list := &autoscalingv2.HorizontalPodAutoscalerList{}
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list HPAs in %s: %w", namespace, err)
	}
	var owned []autoscalingv2.HorizontalPodAutoscaler
	for i := range list.Items {
		controller := metav1.GetControllerOf(&list.Items[i])
		if controller != nil && controller.APIVersion == kedav1.GroupVersion.String() &&
			controller.Kind == "ScaledObject" && controller.Name == soName {
			owned = append(owned, list.Items[i])
		}
	}
	return owned, nil
}

// validateReplacementHPA asks the API server to validate the HPA OME is about
// to create, without storing it. The probe uses a generated name, because a
// KEDA HPA may still hold the Component's name.
func validateReplacementHPA(ctx context.Context, p DispatchParams) error {
	probe := desiredHPA(p).DeepCopy()
	probe.GenerateName = p.Name + "-"
	probe.Name = ""
	probe.ResourceVersion = ""
	return p.Client.Create(ctx, probe, client.DryRunAll)
}

// deleteObserved deletes exactly the object OME inspected. The UID and
// resourceVersion preconditions make the delete fail rather than remove an
// object that was replaced or changed in the meantime; OME reads it again on
// the next pass, so a conflict only means "retry".
func deleteObserved(ctx context.Context, c client.Client, obj client.Object, kind, pendingReason string) error {
	uid := obj.GetUID()
	resourceVersion := obj.GetResourceVersion()
	err := c.Delete(ctx, obj, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion})
	switch {
	case err == nil || apierrors.IsNotFound(err):
		return nil
	case apierrors.IsConflict(err):
		return transitionPending(pendingReason,
			"%s %s/%s changed while OME was deleting it; OME reads it again and retries", kind, obj.GetNamespace(), obj.GetName())
	default:
		return fmt.Errorf("delete %s %s/%s: %w", kind, obj.GetNamespace(), obj.GetName(), err)
	}
}
