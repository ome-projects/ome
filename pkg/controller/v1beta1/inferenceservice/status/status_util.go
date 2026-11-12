package status

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	lwsspec "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// initializeComponentStatus returns the component's status entry, empty when
// the component has none yet, and makes sure the map it is written back to
// exists.
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

// getFirstPod returns the first pod of the list; a nil or empty list is an
// error.
func (sr *StatusReconciler) getFirstPod(podList *v1.PodList) (*v1.Pod, error) {
	if podList == nil || len(podList.Items) == 0 {
		return nil, fmt.Errorf("pod list is empty")
	}
	return &podList.Items[0], nil
}

// deploymentReport reads a Deployment's Available condition and its observed
// generation.
func deploymentReport(deployment *appsv1.Deployment) workloadReport {
	report := workloadReport{generation: deployment.Status.ObservedGeneration}
	for _, c := range deployment.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			report.available = statusCondition(string(c.Type), c.Status, c.Reason, c.Message, c.LastTransitionTime)
			break
		}
	}
	return report
}

// leaderWorkerSetReport reads a LeaderWorkerSet's Available condition and
// its generation.
func leaderWorkerSetReport(lws *lwsspec.LeaderWorkerSet) workloadReport {
	report := workloadReport{generation: lws.Generation}
	for _, c := range lws.Status.Conditions {
		if lwsspec.LeaderWorkerSetConditionType(c.Type) == lwsspec.LeaderWorkerSetAvailable {
			report.available = statusCondition(c.Type, v1.ConditionStatus(c.Status), c.Reason, c.Message, c.LastTransitionTime)
			break
		}
	}
	return report
}

// statusCondition is a workload's condition in the InferenceService status
// shape.
func statusCondition(conditionType string, status v1.ConditionStatus, reason, message string, at metav1.Time) apis.Condition {
	return apis.Condition{
		Type:               apis.ConditionType(conditionType),
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: apis.VolatileTime{Inner: at},
	}
}

// InitializeComponentCondition rewrites the component's ready condition to
// False with reason Initializing when the status holds one that is neither
// True nor Unknown. A missing, Unknown or True condition is left as it is.
func (sr *StatusReconciler) InitializeComponentCondition(status *v1beta1.InferenceServiceStatus, component v1beta1.ComponentType) {
	readyCondition, _ := v1beta1.ReadyConditionOf(component)

	if !status.IsConditionReady(readyCondition) && !status.IsConditionUnknown(readyCondition) {
		status.SetCondition(readyCondition, &apis.Condition{
			Type:    readyCondition,
			Status:  v1.ConditionFalse,
			Reason:  "Initializing",
			Message: fmt.Sprintf("%s component initializing", component),
		})
	}
}

// checkContainerStatuses reads the model state off firstPod. The storage
// initializer decides while it runs (Loading) or once it is failing
// (FailedToLoad, when a termination is recorded). Otherwise the main
// container decides: FailedToLoad when it is failing, Pending in every other
// state. A pod with neither container leaves the state as it is.
func (sr *StatusReconciler) checkContainerStatuses(status *v1beta1.InferenceServiceStatus, firstPod *v1.Pod, totalCopies int) {
	if initializer, ok := containerStatusNamed(firstPod.Status.InitContainerStatuses, constants.StorageInitializerContainerName); ok {
		switch {
		case initializer.State.Running != nil:
			sr.UpdateModelRevisionStates(status, v1beta1.Loading, totalCopies, nil)
			return
		case terminatedWithError(initializer), waitingInCrashLoop(initializer):
			if failure := sr.modelLoadFailure(initializer); failure != nil {
				sr.UpdateModelRevisionStates(status, v1beta1.FailedToLoad, totalCopies, failure)
			}
			return
		}
	}

	mainContainer, ok := containerStatusNamed(firstPod.Status.ContainerStatuses, constants.MainContainerName)
	if !ok {
		return
	}
	if failure := sr.modelLoadFailure(mainContainer); failure != nil {
		sr.UpdateModelRevisionStates(status, v1beta1.FailedToLoad, totalCopies, failure)
		return
	}
	sr.UpdateModelRevisionStates(status, v1beta1.Pending, totalCopies, nil)
}

// modelLoadFailure is the failure a container reports: the termination that
// ended it with reason Error, or the last termination behind a
// CrashLoopBackOff wait. Nil when the container is not failing, and nil while
// its crash loop has no termination recorded yet.
func (sr *StatusReconciler) modelLoadFailure(cs v1.ContainerStatus) *v1beta1.FailureInfo {
	if !terminatedWithError(cs) && !waitingInCrashLoop(cs) {
		return nil
	}
	message, exitCode, hasTermination := sr.safeGetTerminationMessage(cs)
	if !hasTermination {
		return nil
	}
	return &v1beta1.FailureInfo{
		Reason:   v1beta1.ModelLoadFailed,
		Message:  message,
		ExitCode: exitCode,
	}
}

// containerStatusNamed returns the status of the container called name.
func containerStatusNamed(statuses []v1.ContainerStatus, name string) (v1.ContainerStatus, bool) {
	for i := range statuses {
		if statuses[i].Name == name {
			return statuses[i], true
		}
	}
	return v1.ContainerStatus{}, false
}

// terminatedWithError reports a container that exited with the kubelet's
// Error reason.
func terminatedWithError(cs v1.ContainerStatus) bool {
	return cs.State.Terminated != nil && cs.State.Terminated.Reason == constants.StateReasonError
}

// waitingInCrashLoop reports a container the kubelet holds in CrashLoopBackOff.
func waitingInCrashLoop(cs v1.ContainerStatus) bool {
	return cs.State.Waiting != nil && cs.State.Waiting.Reason == constants.StateReasonCrashLoopBackOff
}

// safeGetTerminationMessage returns the message and exit code of the
// termination that describes the container: its own when it is terminated,
// the last one when it waits in CrashLoopBackOff. hasTermination is false
// when neither exists.
func (sr *StatusReconciler) safeGetTerminationMessage(cs v1.ContainerStatus) (message string, exitCode int32, hasTermination bool) {
	if cs.State.Terminated != nil {
		return cs.State.Terminated.Message, cs.State.Terminated.ExitCode, true
	}
	if waitingInCrashLoop(cs) && cs.LastTerminationState.Terminated != nil {
		return cs.LastTerminationState.Terminated.Message, cs.LastTerminationState.Terminated.ExitCode, true
	}
	return "", 0, false
}
