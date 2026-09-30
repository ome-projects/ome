package render

import (
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// PodSpecReconciler is the shared merge of the runtime's runner container into
// a component's pod spec: the runner is strategic-merged into the container of
// the same name (or appended) and its placeholders are replaced.
type PodSpecReconciler struct {
	Log logr.Logger
}

// PodSpecInput contains the input for pod spec reconciliation
type PodSpecInput struct {
	BasePodSpec   *v1beta1.PodSpec
	RunnerSpec    *v1beta1.RunnerSpec
	LeaderPodSpec *v1beta1.PodSpec
	LeaderRunner  *v1beta1.RunnerSpec
	WorkerPodSpec *v1beta1.PodSpec
	WorkerSize    *int
}

// ReconcilePodSpec creates a pod spec from the provided inputs
func (r *PodSpecReconciler) ReconcilePodSpec(
	isvc *v1beta1.InferenceService,
	objectMeta *metav1.ObjectMeta,
	basePodSpec *v1beta1.PodSpec,
	runnerSpec *v1beta1.RunnerSpec,
) (*v1.PodSpec, error) {
	// Convert to core v1.PodSpec
	podSpec, err := isvcutils.ConvertPodSpec(basePodSpec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert pod spec")
	}

	// Determine the container to update
	container, containerIdx, err := r.resolveContainer(podSpec, runnerSpec)
	if err != nil {
		return nil, err
	}

	// Replace placeholders
	if err := isvcutils.ReplacePlaceholders(container, isvc.ObjectMeta); err != nil {
		return nil, errors.Wrap(err, "failed to replace placeholders in container")
	}

	// Update the container in the pod spec
	podSpec.Containers[containerIdx] = *container

	return podSpec, nil
}

// ReconcileWorkerPodSpec creates a worker pod spec for multi-node deployments
func (r *PodSpecReconciler) ReconcileWorkerPodSpec(
	isvc *v1beta1.InferenceService,
	objectMeta *metav1.ObjectMeta,
	workerPodSpec *v1beta1.PodSpec,
	leaderRunnerSpec *v1beta1.RunnerSpec,
) (*v1.PodSpec, error) {
	if workerPodSpec == nil {
		return nil, nil
	}

	// Convert worker pod spec to core v1.PodSpec
	podSpec, err := isvcutils.ConvertPodSpec(workerPodSpec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert worker pod spec")
	}

	// Determine the container to update
	var container *v1.Container
	var containerIdx int

	if leaderRunnerSpec != nil {
		// Find container with the same name as leader's runner container
		containerName := leaderRunnerSpec.Container.Name
		for i := range podSpec.Containers {
			if podSpec.Containers[i].Name == containerName {
				containerIdx = i
				container = &podSpec.Containers[i]
				break
			}
		}

		if container == nil {
			// If no matching container found, use the runner container as is
			podSpec.Containers = append(podSpec.Containers, v1.Container{})
			containerIdx = len(podSpec.Containers) - 1
			container = &podSpec.Containers[containerIdx]
		}

		// Merge with leader's runner container. Argument order: runtime
		// (CSR runner) is the base, user (ISVC worker) is the strategic-
		// merge override — predictor wins on field-level conflicts so a
		// user-pinned image overrides the runtime default. With
		// omitempty on Container.Image, an unset ISVC image cleanly
		// falls back to the runtime image.
		mergedContainer, err := isvcutils.MergeRuntimeContainers(&leaderRunnerSpec.Container, container)
		if err != nil {
			return nil, errors.Wrap(err, "failed to merge worker container with leader runner")
		}
		container = mergedContainer
	} else {
		// No leader runner spec, use the first container
		if len(podSpec.Containers) > 0 {
			containerIdx = 0
			container = &podSpec.Containers[0]
		} else {
			return nil, errors.New("no containers found in worker pod spec")
		}
	}

	// Replace placeholders
	if err := isvcutils.ReplacePlaceholders(container, isvc.ObjectMeta); err != nil {
		return nil, errors.Wrap(err, "failed to replace placeholders in worker container")
	}

	// Update the container in the pod spec
	podSpec.Containers[containerIdx] = *container

	return podSpec, nil
}

// resolveContainer determines which container to use and returns it with its index
func (r *PodSpecReconciler) resolveContainer(podSpec *v1.PodSpec, runnerSpec *v1beta1.RunnerSpec) (*v1.Container, int, error) {
	var container *v1.Container
	var containerIdx int

	if runnerSpec != nil {
		// Find container with the same name as runner's container
		containerName := runnerSpec.Container.Name
		for i := range podSpec.Containers {
			if podSpec.Containers[i].Name == containerName {
				containerIdx = i
				container = &podSpec.Containers[i]
				break
			}
		}

		if container == nil {
			// If no matching container found, use the runner container as is
			podSpec.Containers = append(podSpec.Containers, v1.Container{})
			containerIdx = len(podSpec.Containers) - 1
			container = &podSpec.Containers[containerIdx]
		}

		// Merge with runner container. Argument order: runtime (CSR
		// runner) is the base, user (ISVC container) is the strategic-
		// merge override — predictor wins on field-level conflicts so a
		// user-pinned image overrides the runtime default. With
		// omitempty on Container.Image, an unset ISVC image cleanly
		// falls back to the runtime image.
		mergedContainer, err := isvcutils.MergeRuntimeContainers(&runnerSpec.Container, container)
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to merge runner container")
		}
		container = mergedContainer
	} else {
		// No runner spec, find the first container
		if len(podSpec.Containers) > 0 {
			containerIdx = 0
			container = &podSpec.Containers[0]
		} else {
			return nil, 0, errors.New("no containers found in pod spec and no runner spec provided")
		}
	}

	return container, containerIdx, nil
}
