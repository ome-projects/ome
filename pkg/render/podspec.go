package render

import (
	"slices"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// PodSpecReconciler is the shared merge of the runtime's runner container into
// a component's pod spec: the runner is strategic-merged into the container of
// the same name (or appended) and its placeholders are replaced.
type PodSpecReconciler struct{}

// ReconcilePodSpec creates a pod spec from the provided inputs
func (r *PodSpecReconciler) ReconcilePodSpec(
	isvc *v1beta1.InferenceService,
	objectMeta *metav1.ObjectMeta,
	basePodSpec *v1beta1.PodSpec,
	runnerSpec *v1beta1.RunnerSpec,
) (*v1.PodSpec, error) {
	podSpec, err := isvcutils.ConvertPodSpec(basePodSpec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert pod spec")
	}
	return r.placeRunner(isvc, podSpec, runnerSpec, componentRunnerErrors)
}

// ReconcileWorkerPodSpec creates a worker pod spec for multi-node deployments.
// The leader's runner container is the one merged into the worker's container
// of the same name.
func (r *PodSpecReconciler) ReconcileWorkerPodSpec(
	isvc *v1beta1.InferenceService,
	objectMeta *metav1.ObjectMeta,
	workerPodSpec *v1beta1.PodSpec,
	leaderRunnerSpec *v1beta1.RunnerSpec,
) (*v1.PodSpec, error) {
	if workerPodSpec == nil {
		return nil, nil
	}
	podSpec, err := isvcutils.ConvertPodSpec(workerPodSpec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert worker pod spec")
	}
	return r.placeRunner(isvc, podSpec, leaderRunnerSpec, workerRunnerErrors)
}

// runnerPlacementErrors are the texts one placement reports: a runner
// container that does not merge, a pod spec with no container to place the
// runner in, and a placeholder that does not render.
type runnerPlacementErrors struct {
	merge, noContainer, placeholders string
}

var (
	componentRunnerErrors = runnerPlacementErrors{
		merge:        "failed to merge runner container",
		noContainer:  "no containers found in pod spec and no runner spec provided",
		placeholders: "failed to replace placeholders in container",
	}
	workerRunnerErrors = runnerPlacementErrors{
		merge:        "failed to merge worker container with leader runner",
		noContainer:  "no containers found in worker pod spec",
		placeholders: "failed to replace placeholders in worker container",
	}
)

// placeRunner merges the runner container into the pod spec and replaces the
// placeholders of the container it lands in.
func (r *PodSpecReconciler) placeRunner(isvc *v1beta1.InferenceService, podSpec *v1.PodSpec, runnerSpec *v1beta1.RunnerSpec, texts runnerPlacementErrors) (*v1.PodSpec, error) {
	container, containerIdx, err := r.resolveContainer(podSpec, runnerSpec, texts)
	if err != nil {
		return nil, err
	}
	if err := isvcutils.ReplacePlaceholders(container, isvc.ObjectMeta); err != nil {
		return nil, errors.Wrap(err, texts.placeholders)
	}
	podSpec.Containers[containerIdx] = *container
	return podSpec, nil
}

// resolveContainer is the container the runner lands in and its index: the
// pod spec's container of the runner's name, or one appended for it, merged
// with the runner; without a runner, the pod spec's first container as it is.
//
// Merge argument order: the runtime's runner is the base and the service's
// container the strategic-merge override, so a user-set image wins a field
// conflict and an unset one falls back to the runtime image.
func (r *PodSpecReconciler) resolveContainer(podSpec *v1.PodSpec, runnerSpec *v1beta1.RunnerSpec, texts runnerPlacementErrors) (*v1.Container, int, error) {
	if runnerSpec == nil {
		if len(podSpec.Containers) == 0 {
			return nil, 0, errors.New(texts.noContainer)
		}
		return &podSpec.Containers[0], 0, nil
	}
	containerIdx := slices.IndexFunc(podSpec.Containers, func(c v1.Container) bool { return c.Name == runnerSpec.Container.Name })
	if containerIdx < 0 {
		podSpec.Containers = append(podSpec.Containers, v1.Container{})
		containerIdx = len(podSpec.Containers) - 1
	}
	merged, err := isvcutils.MergeRuntimeContainers(&runnerSpec.Container, &podSpec.Containers[containerIdx])
	if err != nil {
		return nil, 0, errors.Wrap(err, texts.merge)
	}
	return merged, containerIdx, nil
}
