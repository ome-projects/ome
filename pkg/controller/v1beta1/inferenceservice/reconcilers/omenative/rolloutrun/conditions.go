package rolloutrun

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
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
	// EventTrafficIntentUnsupported is recorded when the configured route
	// provider declares it cannot express the spec's per-revision traffic
	// intent; EventTrafficIntentUnverified when it declares no capabilities
	// at all and the intent is applied unverified.
	EventTrafficIntentUnsupported = "TrafficIntentUnsupported"
	EventTrafficIntentUnverified  = "TrafficIntentUnverified"
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

// reportTrafficIntent writes the TrafficIntentReady condition from the live
// spec's per-revision traffic intent and the declared capabilities of the
// route provider it resolves to, and records an event when the standing
// worsens. The live spec is the intent admission saw and the next run
// records; an open run whose provider is reconfigured mid-flight keeps
// running (the run layer holds only at open) and is reported here rather
// than aborted. A spec without such intent leaves the condition alone unless
// it was written before, in which case it reads True/NoIntent. A pass handed
// no route provider at all (no ingress config resolved) has nothing to hold
// the intent against and reports nothing.
func reportTrafficIntent(in Inputs, now metav1.Time) {
	if in.RouteCapabilities.Provider == "" {
		return
	}
	isvc := in.ISVC
	required := validation.RequiredRouteCapabilities(&isvc.Spec)
	if len(required) == 0 {
		if isvc.Status.GetCondition(v1beta1.TrafficIntentReady) != nil {
			setTrafficIntent(isvc, corev1.ConditionTrue, v1beta1.TrafficIntentReasonNoIntent, "the spec carries no per-revision traffic intent", now)
		}
		return
	}
	caps := in.RouteCapabilities
	if !caps.Known {
		if setTrafficIntent(isvc, corev1.ConditionUnknown, v1beta1.TrafficIntentReasonCapabilitiesUnknown,
			fmt.Sprintf("route provider %q declares no capabilities, so %s is applied unverified; declare ingress.routeProviderCapabilities[%q] to have it checked",
				caps.Provider, required[0].Field, caps.Provider), now) {
			emit(in.Recorder, isvc, corev1.EventTypeNormal, EventTrafficIntentUnverified,
				"route provider %q declares no capabilities; %s is applied unverified", caps.Provider, required[0].Field)
		}
		return
	}
	if unsupported := validation.UnsupportedRouteCapabilities(required, caps); len(unsupported) > 0 {
		msg := validation.RouteCapabilityUnsupportedError(unsupported[0], caps).Error()
		if setTrafficIntent(isvc, corev1.ConditionFalse, v1beta1.TrafficIntentReasonUnsupported, msg, now) {
			emit(in.Recorder, isvc, corev1.EventTypeWarning, EventTrafficIntentUnsupported, "%s", msg)
		}
		return
	}
	setTrafficIntent(isvc, corev1.ConditionTrue, v1beta1.TrafficIntentReasonSupported,
		fmt.Sprintf("route provider %q declares %s", caps.Provider, required[0].Capability), now)
}

// setTrafficIntent writes TrafficIntentReady, moving the transition time
// only on a real change, and reports whether anything changed. The condition
// is advisory, so it is written into the condition list directly rather than
// through the shared condition set, whose Unknown marking folds into the
// aggregate Ready condition whenever a dependent of Ready is False.
func setTrafficIntent(isvc *v1beta1.InferenceService, status corev1.ConditionStatus, reason, message string, now metav1.Time) bool {
	cond := apis.Condition{
		Type:               v1beta1.TrafficIntentReady,
		Status:             status,
		Severity:           apis.ConditionSeverityInfo,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: apis.VolatileTime{Inner: now},
	}
	var kept apis.Conditions
	for _, c := range isvc.Status.GetConditions() {
		if c.Type != v1beta1.TrafficIntentReady {
			kept = append(kept, c)
			continue
		}
		if c.Status == status && c.Reason == reason && c.Message == message {
			return false
		}
	}
	kept = append(kept, cond)
	sort.Slice(kept, func(i, j int) bool { return kept[i].Type < kept[j].Type })
	isvc.Status.SetConditions(kept)
	return true
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
