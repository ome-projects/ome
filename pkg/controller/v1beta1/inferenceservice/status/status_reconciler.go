package status

import (
	"reflect"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/apis"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	lwsspec "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
)

// Constants for magic numbers and string literals
const (
	FullTrafficPercent           = 100
	RoutesReadyCondition         = "RoutesReady"
	ConfigurationsReadyCondition = "ConfigurationsReady"
)

// StatusReconciler handles all status-related operations for InferenceService
type StatusReconciler struct {
	Clientset     kubernetes.Interface
	Log           logr.Logger
	podLogFetcher func(namespace, podName, containerName string, previous bool) (string, error)
}

// NewStatusReconciler creates a new StatusReconciler instance.
// Pass a nil clientset when Kubernetes log fetching is not needed.
func NewStatusReconciler(clientset kubernetes.Interface) *StatusReconciler {
	reconciler := &StatusReconciler{
		Clientset: clientset,
		Log:       logf.Log.WithName("InferenceServiceStatus"),
	}
	reconciler.podLogFetcher = reconciler.fetchPodLogs
	return reconciler
}

// WithLogger overrides the default logger and returns the reconciler for chaining.
func (sr *StatusReconciler) WithLogger(log logr.Logger) *StatusReconciler {
	sr.Log = log
	return sr
}

// PropagateRawStatus propagates status from raw Kubernetes deployment
func (sr *StatusReconciler) PropagateRawStatus(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	deployment *appsv1.Deployment,
	url *apis.URL) {

	statusSpec := sr.initializeComponentStatus(status, component)

	statusSpec.LatestCreatedRevision = deployment.GetObjectMeta().GetAnnotations()["deployment.kubernetes.io/revision"]
	condition := sr.getDeploymentCondition(deployment, appsv1.DeploymentAvailable)
	if condition != nil && condition.Status == v1.ConditionTrue {
		statusSpec.URL = url
	}
	readyCondition := sr.getReadyConditionsMap()[component]
	sr.setCondition(status, readyCondition, condition)
	status.Components[component] = statusSpec
	status.ObservedGeneration = deployment.Status.ObservedGeneration
}

// PropagateMultiNodeStatus propagates status from LeaderWorkerSet
func (sr *StatusReconciler) PropagateMultiNodeStatus(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	lws *lwsspec.LeaderWorkerSet,
	url *apis.URL) {

	statusSpec := sr.initializeComponentStatus(status, component)

	statusSpec.LatestCreatedRevision = lws.GetObjectMeta().GetAnnotations()["resourceVersion"]
	lwsCondition := sr.getLWSConditions(lws, lwsspec.LeaderWorkerSetAvailable)
	if lwsCondition != nil && lwsCondition.Status == v1.ConditionTrue {
		statusSpec.URL = url
	}

	readyCondition := sr.getReadyConditionsMap()[component]

	// Create a new condition with the correct component ready condition type
	// instead of using the LWS condition type directly
	if lwsCondition != nil {
		componentCondition := &apis.Condition{
			Type:               readyCondition,
			Status:             lwsCondition.Status,
			Message:            lwsCondition.Message,
			Reason:             lwsCondition.Reason,
			LastTransitionTime: lwsCondition.LastTransitionTime,
		}
		sr.setCondition(status, readyCondition, componentCondition)
	}

	status.Components[component] = statusSpec
	status.ObservedGeneration = lws.Generation
}

// PropagateMultiNodeRayVLLMStatus propagates status from multiple deployments
func (sr *StatusReconciler) PropagateMultiNodeRayVLLMStatus(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	deployments []*appsv1.Deployment,
	url *apis.URL) {

	statusSpec := sr.initializeComponentStatus(status, component)

	firstDeployment, err := sr.getFirstDeployment(deployments)
	if err != nil {
		// Handle error case gracefully - set a default state
		sr.setCondition(status, sr.getReadyConditionsMap()[component], &apis.Condition{
			Type:    sr.getReadyConditionsMap()[component],
			Status:  v1.ConditionFalse,
			Reason:  "NoDeployments",
			Message: "No deployments available",
		})
		return
	}

	statusSpec.LatestCreatedRevision = firstDeployment.GetObjectMeta().GetAnnotations()["deployment.kubernetes.io/revision"]

	condition := sr.getMultiDeploymentCondition(deployments, appsv1.DeploymentAvailable)
	if condition != nil && condition.Status == v1.ConditionTrue {
		statusSpec.URL = url
	}
	readyCondition := sr.getReadyConditionsMap()[component]
	sr.setCondition(status, readyCondition, condition)
	status.Components[component] = statusSpec
	status.ObservedGeneration = firstDeployment.Status.ObservedGeneration
}

// PropagateStatus propagates status from Knative Service
func (sr *StatusReconciler) PropagateStatus(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	serviceStatus *knservingv1.ServiceStatus) {

	statusSpec := sr.initializeComponentStatus(status, component)

	statusSpec.LatestCreatedRevision = serviceStatus.LatestCreatedRevisionName
	revisionTraffic := map[string]int64{}
	for _, traffic := range serviceStatus.Traffic {
		if traffic.Percent != nil {
			revisionTraffic[traffic.RevisionName] += *traffic.Percent
		}
	}

	// Handle traffic routing logic
	sr.handleTrafficRouting(&statusSpec, serviceStatus, revisionTraffic)

	if serviceStatus.LatestReadyRevisionName != statusSpec.LatestReadyRevision {
		statusSpec.LatestReadyRevision = serviceStatus.LatestReadyRevisionName
	}

	// Propagate conditions
	sr.propagateServiceConditions(status, component, serviceStatus, &statusSpec)

	status.Components[component] = statusSpec
	status.ObservedGeneration = serviceStatus.ObservedGeneration
}

// PropagateModelStatus propagates model status from pod information
func (sr *StatusReconciler) PropagateModelStatus(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	statusSpec v1beta1.ComponentStatusSpec,
	podList *v1.PodList,
	rawDeployment bool) {
	// Check at least one pod is running for the latest revision of inferenceservice
	totalCopies := len(podList.Items)
	if totalCopies == 0 {
		sr.UpdateModelRevisionStates(status, v1beta1.Pending, totalCopies, nil)
		return
	}

	// Use helper function to safely get the first pod
	firstPod, err := sr.getFirstPod(podList)
	if err != nil {
		sr.UpdateModelRevisionStates(status, v1beta1.Pending, totalCopies, nil)
		return
	}

	// Update model state to 'Loaded' if inferenceservice status is ready.
	if status.IsReady() {
		if rawDeployment {
			sr.UpdateModelRevisionStates(status, v1beta1.Loaded, totalCopies, nil)
			return
		} else if statusSpec.LatestCreatedRevision == statusSpec.LatestReadyRevision {
			sr.UpdateModelRevisionStates(status, v1beta1.Loaded, totalCopies, nil)
			return
		}
	}

	// Check container statuses
	sr.checkContainerStatuses(status, component, firstPod, totalCopies)
}

// UpdateModelRevisionStates updates the model revision states
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

	// Update transition status, failure info based on new model state
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

// UpdateModelTransitionStatus updates the model transition status
func (sr *StatusReconciler) UpdateModelTransitionStatus(
	status *v1beta1.InferenceServiceStatus,
	transitionStatus v1beta1.TransitionStatus,
	info *v1beta1.FailureInfo) {

	status.ModelStatus.TransitionStatus = transitionStatus

	// Update model state to 'FailedToLoad' in case of invalid spec provided
	if status.ModelStatus.TransitionStatus == v1beta1.InvalidSpec {
		if status.ModelStatus.ModelRevisionStates == nil {
			status.ModelStatus.ModelRevisionStates = &v1beta1.ModelRevisionStates{TargetModelState: v1beta1.FailedToLoad}
		} else {
			status.ModelStatus.ModelRevisionStates.TargetModelState = v1beta1.FailedToLoad
		}
	}

	if info != nil {
		sr.SetModelFailureInfo(status, info)
	}
}

// SetModelFailureInfo sets the model failure information
func (sr *StatusReconciler) SetModelFailureInfo(status *v1beta1.InferenceServiceStatus, info *v1beta1.FailureInfo) bool {
	if reflect.DeepEqual(info, status.ModelStatus.LastFailureInfo) {
		return false
	}
	status.ModelStatus.LastFailureInfo = info
	return true
}

// PropagateCrossComponentStatus aggregates conditions across components
func (sr *StatusReconciler) PropagateCrossComponentStatus(
	status *v1beta1.InferenceServiceStatus,
	componentList []v1beta1.ComponentType,
	conditionType apis.ConditionType) {

	conditionsMap, ok := sr.getConditionsMapIndex()[conditionType]
	if !ok {
		return
	}

	crossComponentCondition := &apis.Condition{
		Type:   conditionType,
		Status: v1.ConditionTrue,
	}

	for _, component := range componentList {
		if !status.IsConditionReady(conditionsMap[component]) {
			crossComponentCondition.Status = v1.ConditionFalse
			if status.IsConditionUnknown(conditionsMap[component]) {
				crossComponentCondition.Status = v1.ConditionUnknown
			}
			crossComponentCondition.Reason = string(conditionsMap[component]) + " not ready"
		}
	}

	sr.setCondition(status, conditionType, crossComponentCondition)
}
