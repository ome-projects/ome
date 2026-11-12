// Package status writes an InferenceService's component ready conditions,
// its model-loading state and its top-level component readiness from the
// workload objects and pods the component reconcilers observe.
package status

import (
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	lwsspec "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// StatusReconciler writes component ready conditions and the model-loading
// state onto an InferenceService status. It holds no state of its own.
type StatusReconciler struct{}

// NewStatusReconciler returns a StatusReconciler.
func NewStatusReconciler() *StatusReconciler {
	return &StatusReconciler{}
}

// PropagateRawStatus writes the component's ready condition from the
// Deployment's Available condition, records the component URL once that
// condition is True, and takes the Deployment's observed generation.
func (sr *StatusReconciler) PropagateRawStatus(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	deployment *appsv1.Deployment,
	url *apis.URL) {
	sr.propagate(status, component, deploymentReport(deployment), url)
}

// PropagateMultiNodeStatus writes the component's ready condition from the
// LeaderWorkerSet's Available condition, records the component URL once that
// condition is True, and takes the set's generation.
func (sr *StatusReconciler) PropagateMultiNodeStatus(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	lws *lwsspec.LeaderWorkerSet,
	url *apis.URL) {
	sr.propagate(status, component, leaderWorkerSetReport(lws), url)
}

// workloadReport is what a component's workload says about itself: its
// Available condition in the status shape, the zero condition when it
// reports none, and the generation the report describes.
type workloadReport struct {
	available  apis.Condition
	generation int64
}

// propagate stores the report under the component: the ready condition
// takes the Available condition's status, reason and message, the URL is
// recorded once that condition is True, and the status's observed generation
// becomes the report's. A zero Available condition leaves the ready
// condition as it is.
func (sr *StatusReconciler) propagate(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	report workloadReport,
	url *apis.URL) {
	statusSpec := sr.initializeComponentStatus(status, component)
	if report.available.Status == v1.ConditionTrue {
		statusSpec.URL = url
	}
	readyCondition, _ := v1beta1.ReadyConditionOf(component)
	status.SetCondition(readyCondition, &report.available)
	status.Components[component] = statusSpec
	status.ObservedGeneration = report.generation
}

// PropagateModelStatus writes the model-loading state from the pods: Pending
// with no pod, Loaded once the service is Ready, otherwise what the first
// pod's containers report.
func (sr *StatusReconciler) PropagateModelStatus(
	status *v1beta1.InferenceServiceStatus,
	podList *v1.PodList) {

	totalCopies := len(podList.Items)
	if totalCopies == 0 {
		sr.UpdateModelRevisionStates(status, v1beta1.Pending, totalCopies, nil)
		return
	}

	firstPod, err := sr.getFirstPod(podList)
	if err != nil {
		sr.UpdateModelRevisionStates(status, v1beta1.Pending, totalCopies, nil)
		return
	}

	if status.IsReady() {
		sr.UpdateModelRevisionStates(status, v1beta1.Loaded, totalCopies, nil)
		return
	}

	sr.checkContainerStatuses(status, firstPod, totalCopies)
}

// UpdateModelRevisionStates sets the target model state and the transition
// status it implies; Loaded also records the copy count and the active state.
func (sr *StatusReconciler) UpdateModelRevisionStates(
	status *v1beta1.InferenceServiceStatus,
	modelState v1beta1.ModelState,
	totalCopies int,
	info *v1beta1.FailureInfo) {

	if status.ModelStatus.ModelRevisionStates == nil {
		status.ModelStatus.ModelRevisionStates = &v1beta1.ModelRevisionStates{TargetModelState: modelState}
	} else {
		status.ModelStatus.ModelRevisionStates.TargetModelState = modelState
	}

	switch modelState {
	case v1beta1.Pending, v1beta1.Loading:
		status.ModelStatus.TransitionStatus = v1beta1.InProgress
	case v1beta1.Loaded:
		status.ModelStatus.TransitionStatus = v1beta1.UpToDate
		status.ModelStatus.ModelCopies = &v1beta1.ModelCopies{TotalCopies: totalCopies}
		status.ModelStatus.ModelRevisionStates.ActiveModelState = v1beta1.Loaded
	case v1beta1.FailedToLoad:
		status.ModelStatus.TransitionStatus = v1beta1.BlockedByFailedLoad
	}

	if info != nil {
		sr.SetModelFailureInfo(status, info)
	}
}

// SetModelFailureInfo records the last failure and reports whether it
// differs from the one already recorded.
func (sr *StatusReconciler) SetModelFailureInfo(status *v1beta1.InferenceServiceStatus, info *v1beta1.FailureInfo) bool {
	if reflect.DeepEqual(info, status.ModelStatus.LastFailureInfo) {
		return false
	}
	status.ModelStatus.LastFailureInfo = info
	return true
}
