package rolloutrun

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Event reasons the run layer emits. Operators grep these to reconstruct
// which plan version each phase of a long roll executed under.
const (
	EventRunOpened     = "RolloutRunOpened"
	EventRunClosed     = "RolloutRunClosed"
	EventPlanParked    = "RolloutPlanParked"
	EventPlanRepinned  = "RolloutPlanRepinned"
	EventRepinRejected = "RolloutRepinRejected"
	EventGroupRemoved  = "RolloutGroupRemovedMidRun"
)

// setPlanCondition writes one rollout-plan condition on the service's duck
// conditions. The transition time moves only when the status, reason or
// message changes, so the condition's age says how long the plan has been
// in that state.
func setPlanCondition(isvc *v1beta1.InferenceService, condType apis.ConditionType, status corev1.ConditionStatus, reason, message string, now metav1.Time) bool {
	if prev := isvc.Status.GetCondition(condType); prev != nil &&
		prev.Status == status && prev.Reason == reason && prev.Message == message {
		return false
	}
	isvc.Status.SetCondition(condType, &apis.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: apis.VolatileTime{Inner: now},
	})
	return true
}

// setPlanReady writes the RolloutPlanReady condition.
func setPlanReady(isvc *v1beta1.InferenceService, status corev1.ConditionStatus, reason, message string, now metav1.Time) bool {
	return setPlanCondition(isvc, apis.ConditionType(v1beta1.RolloutPlanReadyCondition), status, reason, message, now)
}

// setPlanDrift writes the RolloutPlanDrift condition; True means an edit
// landed mid-run and is inert until the next run or an explicit repin.
func setPlanDrift(isvc *v1beta1.InferenceService, drifted bool, reason, message string, now metav1.Time) bool {
	status := corev1.ConditionFalse
	if drifted {
		status = corev1.ConditionTrue
	}
	return setPlanCondition(isvc, apis.ConditionType(v1beta1.RolloutPlanDriftCondition), status, reason, message, now)
}

// emit is best-effort observability; a nil recorder (tests) is fine.
func emit(recorder record.EventRecorder, isvc *v1beta1.InferenceService, eventType, reason, messageFmt string, args ...interface{}) {
	if recorder == nil {
		return
	}
	recorder.Eventf(isvc, eventType, reason, messageFmt, args...)
}

// consumeAnnotation removes a one-shot verb annotation after it is applied.
// Two properties are load-bearing:
//
//   - It PATCHes (merge, RV-free) instead of Updating: during an active roll
//     the IR rollup bumps the ISVC's ResourceVersion every few hundred
//     milliseconds, and a 409 here would fail the pass and discard the
//     status mutation the verb produced.
//   - It writes through a STUB object, never the live isvc: the client
//     refreshes the passed object from the server's response — INCLUDING
//     STATUS — which would silently wipe this pass's un-flushed in-memory
//     status mutations (the repinned plan) and make the subsequent flush
//     persist the pre-verb state while the verb itself disappears.
func consumeAnnotation(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, key string) error {
	if _, ok := isvc.Annotations[key]; !ok {
		return nil
	}
	delete(isvc.Annotations, key)
	stub := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name, Namespace: isvc.Namespace}}
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, key))
	return c.Patch(ctx, stub, client.RawPatch(types.MergePatchType, patch))
}
