package status

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	lwsspec "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
)

// Helper functions for StatusReconciler

// initializeComponentStatus ensures component status is properly initialized
func (sr *StatusReconciler) initializeComponentStatus(status *v1beta1.InferenceServiceStatus, component v1beta1.ComponentType) v1beta1.ComponentStatusSpec {
	if len(status.Components) == 0 {
		status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
	}
	statusSpec, ok := status.Components[component]
	if !ok {
		statusSpec = v1beta1.ComponentStatusSpec{}
	}
	return statusSpec
}

// getFirstPod safely returns the first pod from a pod list
func (sr *StatusReconciler) getFirstPod(podList *v1.PodList) (*v1.Pod, error) {
	if podList == nil || len(podList.Items) == 0 {
		return nil, fmt.Errorf("pod list is empty")
	}
	return &podList.Items[0], nil
}

// getFirstDeployment safely returns the first deployment from a deployment slice
func (sr *StatusReconciler) getFirstDeployment(deployments []*appsv1.Deployment) (*appsv1.Deployment, error) {
	if len(deployments) == 0 {
		return nil, fmt.Errorf("deployment list is empty")
	}
	return deployments[0], nil
}

// getDeploymentCondition extracts condition from deployment.
func (sr *StatusReconciler) getDeploymentCondition(deployment *appsv1.Deployment, conditionType appsv1.DeploymentConditionType) *apis.Condition {
	for _, con := range deployment.Status.Conditions {
		if con.Type == conditionType {
			return &apis.Condition{
				Type:    apis.ConditionType(conditionType),
				Status:  con.Status,
				Message: con.Message,
				LastTransitionTime: apis.VolatileTime{
					Inner: con.LastTransitionTime,
				},
				Reason: con.Reason,
			}
		}
	}
	return &apis.Condition{
		Type:    apis.ConditionType(conditionType),
		Status:  v1.ConditionUnknown,
		Reason:  "DeploymentConditionMissing",
		Message: fmt.Sprintf("%s condition is not yet available on the deployment", conditionType),
	}
}

// getLWSConditions extracts condition from LeaderWorkerSet.
// When the requested condition type is not yet present, the function returns
// an explicit Unknown condition so that callers never receive a zero-value
// struct that silently falls through SetCondition.
func (sr *StatusReconciler) getLWSConditions(lws *lwsspec.LeaderWorkerSet, conditionType lwsspec.LeaderWorkerSetConditionType) *apis.Condition {
	for _, con := range lws.Status.Conditions {
		if lwsspec.LeaderWorkerSetConditionType(con.Type) == conditionType {
			return &apis.Condition{
				Type:    apis.ConditionType(conditionType),
				Status:  v1.ConditionStatus(con.Status),
				Message: con.Message,
				LastTransitionTime: apis.VolatileTime{
					Inner: con.LastTransitionTime,
				},
				Reason: con.Reason,
			}
		}
	}
	return &apis.Condition{
		Type:    apis.ConditionType(conditionType),
		Status:  v1.ConditionUnknown,
		Reason:  "LWSConditionMissing",
		Message: fmt.Sprintf("%s condition is not yet available on the LeaderWorkerSet", conditionType),
	}
}

// getMultiDeploymentCondition checks conditions across multiple deployments
func (sr *StatusReconciler) getMultiDeploymentCondition(deployments []*appsv1.Deployment, conditionType appsv1.DeploymentConditionType) *apis.Condition {
	condition := apis.Condition{}
	allDeploymentsAvailable := true

	if len(deployments) == 0 {
		return &apis.Condition{
			Type:    apis.ConditionType(conditionType),
			Status:  v1.ConditionFalse,
			Reason:  "NoDeployments",
			Message: "No deployments available",
		}
	}

	for _, d := range deployments {
		if d.Status.Conditions == nil {
			allDeploymentsAvailable = false
			break
		}
		for _, con := range d.Status.Conditions {
			if con.Type == conditionType && con.Status == v1.ConditionFalse {
				allDeploymentsAvailable = false
				break
			}
		}
	}

	if allDeploymentsAvailable {
		// Safely access the first deployment's conditions
		firstDeployment := deployments[0]
		if len(firstDeployment.Status.Conditions) > 0 {
			condition.Type = apis.ConditionType(conditionType)
			condition.Status = v1.ConditionTrue
			condition.Message = firstDeployment.Status.Conditions[0].Message
			condition.LastTransitionTime = apis.VolatileTime{
				Inner: firstDeployment.Status.Conditions[0].LastTransitionTime,
			}
			condition.Reason = firstDeployment.Status.Conditions[0].Reason
		} else {
			// Fallback if no conditions exist
			condition.Type = apis.ConditionType(conditionType)
			condition.Status = v1.ConditionTrue
			condition.Message = "All deployments available"
			condition.Reason = "Available"
		}
	}

	return &condition
}

// setCondition sets a condition on the status
func (sr *StatusReconciler) setCondition(status *v1beta1.InferenceServiceStatus, conditionType apis.ConditionType, condition *apis.Condition) {
	if condition != nil {
		status.SetCondition(conditionType, condition)
	}
}

// InitializeComponentCondition initializes a component ready condition if it doesn't exist
// This is used for MultiNode deployments to ensure conditions are visible from the start
func (sr *StatusReconciler) InitializeComponentCondition(status *v1beta1.InferenceServiceStatus, component v1beta1.ComponentType) {
	readyCondition := sr.getReadyConditionsMap()[component]

	// Only initialize if the condition doesn't exist yet.
	// A nil condition means the status has never been set for this component;
	if status.GetCondition(readyCondition) == nil {
		condition := &apis.Condition{
			Type:    readyCondition,
			Status:  v1.ConditionUnknown,
			Reason:  "Initializing",
			Message: fmt.Sprintf("%s component initializing", component),
		}
		sr.setCondition(status, readyCondition, condition)
	}
}

// getReadyConditionsMap returns the mapping of component types to ready conditions
func (sr *StatusReconciler) getReadyConditionsMap() map[v1beta1.ComponentType]apis.ConditionType {
	return map[v1beta1.ComponentType]apis.ConditionType{
		v1beta1.PredictorComponent: v1beta1.PredictorReady,
		v1beta1.RouterComponent:    v1beta1.RouterReady,
		v1beta1.EngineComponent:    v1beta1.EngineReady,
		v1beta1.DecoderComponent:   v1beta1.DecoderReady,
	}
}

// getRouteConditionsMap returns the mapping of component types to route conditions
func (sr *StatusReconciler) getRouteConditionsMap() map[v1beta1.ComponentType]apis.ConditionType {
	return map[v1beta1.ComponentType]apis.ConditionType{
		v1beta1.PredictorComponent: v1beta1.PredictorRouteReady,
		v1beta1.RouterComponent:    v1beta1.RouterRouteReady,
		v1beta1.EngineComponent:    v1beta1.EngineRouteReady,
		v1beta1.DecoderComponent:   v1beta1.DecoderRouteReady,
	}
}

// getConfigurationConditionsMap returns the mapping of component types to configuration conditions
func (sr *StatusReconciler) getConfigurationConditionsMap() map[v1beta1.ComponentType]apis.ConditionType {
	return map[v1beta1.ComponentType]apis.ConditionType{
		v1beta1.PredictorComponent: v1beta1.PredictorConfigurationReady,
		v1beta1.RouterComponent:    v1beta1.RouterConfigurationReady,
		v1beta1.EngineComponent:    v1beta1.EngineConfigurationReady,
		v1beta1.DecoderComponent:   v1beta1.DecoderConfigurationReady,
	}
}

// getConditionsMapIndex returns the mapping of condition types to component condition maps
func (sr *StatusReconciler) getConditionsMapIndex() map[apis.ConditionType]map[v1beta1.ComponentType]apis.ConditionType {
	return map[apis.ConditionType]map[v1beta1.ComponentType]apis.ConditionType{
		v1beta1.RoutesReady:           sr.getRouteConditionsMap(),
		v1beta1.LatestDeploymentReady: sr.getConfigurationConditionsMap(),
	}
}

// handleTrafficRouting handles the complex traffic routing logic
func (sr *StatusReconciler) handleTrafficRouting(
	statusSpec *v1beta1.ComponentStatusSpec,
	serviceStatus *knservingv1.ServiceStatus,
	revisionTraffic map[string]int64) {

	for _, traffic := range serviceStatus.Traffic {
		if traffic.RevisionName == serviceStatus.LatestReadyRevisionName && traffic.LatestRevision != nil &&
			*traffic.LatestRevision {
			if statusSpec.LatestRolledoutRevision != serviceStatus.LatestReadyRevisionName {
				if traffic.Percent != nil && *traffic.Percent == FullTrafficPercent {
					// track the last revision that's fully rolled out
					statusSpec.PreviousRolledoutRevision = statusSpec.LatestRolledoutRevision
					statusSpec.LatestRolledoutRevision = serviceStatus.LatestReadyRevisionName
				}
			} else {
				// This is to handle case when the latest ready revision is rolled out with 100% and then rolled back
				// so here we need to rollback the LatestRolledoutRevision to PreviousRolledoutRevision
				if serviceStatus.LatestReadyRevisionName == serviceStatus.LatestCreatedRevisionName {
					if traffic.Percent != nil && *traffic.Percent < FullTrafficPercent {
						// check the possibility that the traffic is split over the same revision
						if val, ok := revisionTraffic[traffic.RevisionName]; ok {
							if val == FullTrafficPercent && statusSpec.PreviousRolledoutRevision != "" {
								statusSpec.LatestRolledoutRevision = statusSpec.PreviousRolledoutRevision
							}
						}
					}
				}
			}
		}
	}
}

// propagateServiceConditions propagates conditions from Knative service
func (sr *StatusReconciler) propagateServiceConditions(
	status *v1beta1.InferenceServiceStatus,
	component v1beta1.ComponentType,
	serviceStatus *knservingv1.ServiceStatus,
	statusSpec *v1beta1.ComponentStatusSpec) {

	// propagate overall ready condition for each component
	readyCondition := serviceStatus.GetCondition(knservingv1.ServiceConditionReady)
	if readyCondition != nil && readyCondition.Status == v1.ConditionTrue {
		if serviceStatus.Address != nil {
			statusSpec.Address = serviceStatus.Address
		}
		if serviceStatus.URL != nil {
			statusSpec.URL = serviceStatus.URL
		}
	}
	readyConditionType := sr.getReadyConditionsMap()[component]
	sr.setCondition(status, readyConditionType, readyCondition)

	// propagate route condition for each component
	routeCondition := serviceStatus.GetCondition(RoutesReadyCondition)
	routeConditionType := sr.getRouteConditionsMap()[component]
	sr.setCondition(status, routeConditionType, routeCondition)

	// propagate configuration condition for each component
	configurationCondition := serviceStatus.GetCondition(ConfigurationsReadyCondition)
	configurationConditionType := sr.getConfigurationConditionsMap()[component]
	sr.setCondition(status, configurationConditionType, configurationCondition)

	// propagate traffic status for each component
	statusSpec.Traffic = serviceStatus.Traffic
}

// checkContainerStatuses checks the status of containers in a pod
func (sr *StatusReconciler) checkContainerStatuses(status *v1beta1.InferenceServiceStatus, firstPod *v1.Pod, totalCopies int) {
	// Update model state to 'Loading' if storage initializer is running.
	// If the storage initializer is terminated due to error or crashloopbackoff, update model
	// state to 'ModelLoadFailed' with failure info.
	for _, cs := range firstPod.Status.InitContainerStatuses {
		if cs.Name == constants.StorageInitializerContainerName {
			switch {
			case cs.State.Running != nil:
				sr.UpdateModelRevisionStates(status, v1beta1.Loading, totalCopies, nil)
				return
			case cs.State.Terminated != nil && cs.State.Terminated.Reason == constants.StateReasonError:
				message, exitCode, _ := sr.safeGetTerminationMessage(cs)
				sr.UpdateModelRevisionStates(status, v1beta1.FailedToLoad, totalCopies, &v1beta1.FailureInfo{
					Reason:   v1beta1.ModelLoadFailed,
					Message:  message,
					ExitCode: exitCode,
				})
				return
			case cs.State.Waiting != nil && cs.State.Waiting.Reason == constants.StateReasonCrashLoopBackOff:
				message, exitCode, hasTermination := sr.safeGetTerminationMessage(cs)
				if hasTermination {
					sr.UpdateModelRevisionStates(status, v1beta1.FailedToLoad, totalCopies, &v1beta1.FailureInfo{
						Reason:   v1beta1.ModelLoadFailed,
						Message:  message,
						ExitCode: exitCode,
					})
				}
				return
			}
		}
	}

	// If the ome container is terminated due to error or crashloopbackoff, update model
	// state to 'ModelLoadFailed' with failure info.
	for _, cs := range firstPod.Status.ContainerStatuses {
		if cs.Name == constants.MainContainerName {
			switch {
			case cs.State.Terminated != nil && cs.State.Terminated.Reason == constants.StateReasonError:
				message, exitCode, _ := sr.safeGetTerminationMessage(cs)
				sr.UpdateModelRevisionStates(status, v1beta1.FailedToLoad, totalCopies, &v1beta1.FailureInfo{
					Reason:   v1beta1.ModelLoadFailed,
					Message:  message,
					ExitCode: exitCode,
				})
			case cs.State.Waiting != nil && cs.State.Waiting.Reason == constants.StateReasonCrashLoopBackOff:
				message, exitCode, hasTermination := sr.safeGetTerminationMessage(cs)
				if hasTermination {
					sr.UpdateModelRevisionStates(status, v1beta1.FailedToLoad, totalCopies, &v1beta1.FailureInfo{
						Reason:   v1beta1.ModelLoadFailed,
						Message:  message,
						ExitCode: exitCode,
					})
				} else {
					sr.UpdateModelRevisionStates(status, v1beta1.Pending, totalCopies, nil)
				}
			default:
				sr.UpdateModelRevisionStates(status, v1beta1.Pending, totalCopies, nil)
			}
		}
	}
}

// safeGetTerminationMessage safely extracts termination message from container status
func (sr *StatusReconciler) safeGetTerminationMessage(cs v1.ContainerStatus) (message string, exitCode int32, hasTermination bool) {
	if cs.State.Terminated != nil {
		return cs.State.Terminated.Message, cs.State.Terminated.ExitCode, true
	}
	if cs.State.Waiting != nil && cs.State.Waiting.Reason == constants.StateReasonCrashLoopBackOff {
		if cs.LastTerminationState.Terminated != nil {
			return cs.LastTerminationState.Terminated.Message, cs.LastTerminationState.Terminated.ExitCode, true
		}
	}
	return "", 0, false
}
