package render

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// UpdatePodSpecNodeSelector updates pod spec node selectors for scheduling.
func UpdatePodSpecNodeSelector(p *Piece, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, componentType v1beta1.ComponentType) {
	if p.BaseModel == nil || p.BaseModelMeta == nil {
		applyMergedNodeSelector(p.Runtime, p.AcceleratorClass, isvc, podSpec, componentType)
		return
	}

	// Skip node selector for fine-tuned serving with merged weights
	// as they don't need the base model on the node
	if p.FineTunedServingWithMergedWeights {
		p.Log.V(2).Info("Skipping node selector for fine-tuned serving with merged weights",
			"inferenceService", isvc.Name, "namespace", isvc.Namespace)
		return
	}

	// Skip node selector for PVC-backed models. The model agent does not
	// label nodes for PVC storage (PVCs aren't tied to specific nodes), so
	// the K8s scheduler handles placement based on PVC accessibility.
	if isPVCBaseModel(p) {
		p.Log.V(2).Info("Skipping model node selector for PVC-backed BaseModel; runtime/AcceleratorClass selectors still apply",
			"inferenceService", isvc.Name, "namespace", isvc.Namespace)
		applyMergedNodeSelector(p.Runtime, p.AcceleratorClass, isvc, podSpec, componentType)
		return
	}

	// Add preferred node affinity for model readiness using the shared utility function
	if isShardedModel(p.BaseModel) {
		p.Log.V(2).Info("Skipping per-node model readiness selector for sharded model",
			"modelName", p.BaseModelMeta.Name,
			"namespace", p.BaseModelMeta.Namespace,
			"inferenceService", isvc.Name)
	} else {
		isvcutils.AddNodeSelectorForModelReadyNode(podSpec, p.BaseModelMeta)
	}

	applyMergedNodeSelector(p.Runtime, p.AcceleratorClass, isvc, podSpec, componentType)

	if !isShardedModel(p.BaseModel) {
		p.Log.V(1).Info("Added preferred node affinity for model scheduling",
			"modelName", p.BaseModelMeta.Name,
			"namespace", p.BaseModelMeta.Namespace,
			"inferenceService", isvc.Name)
	}
}

func applyMergedNodeSelector(runtime *v1beta1.ServingRuntimeSpec, acceleratorClass *v1beta1.AcceleratorClassSpec, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, componentType v1beta1.ComponentType) {
	mergedNodeSelector := isvcutils.MergeNodeSelector(runtime, acceleratorClass, isvc, componentType)
	if len(mergedNodeSelector) > 0 {
		if podSpec.NodeSelector == nil {
			podSpec.NodeSelector = make(map[string]string)
		}
		for k, v := range mergedNodeSelector {
			podSpec.NodeSelector[k] = v
		}
	}
}

// MergeRuntimeArgumentsOverride merges runtime argument overrides according AcceleratorClass into the container args
func MergeRuntimeArgumentsOverride(p *Piece, container *corev1.Container) {
	// append arg var from runtime spec if it is specified
	if p.SupportedModelFormat != nil && p.SupportedModelFormat.AcceleratorConfig != nil && p.AcceleratorClassName != "" {
		acceleratorModelConfig := p.SupportedModelFormat.GetAcceleratorConfig(p.AcceleratorClassName)
		if acceleratorModelConfig != nil {
			argsOverride := acceleratorModelConfig.RuntimeArgsOverride
			container.Args = isvcutils.MergeArgs(container.Args, argsOverride)

			// if runtime argument override has TensorParallelism, update the args accordingly
			// it will be in container.command or container.args
			// check these two places
			if acceleratorModelConfig.TensorParallelismOverride != nil {
				tensorParallelismConfig := acceleratorModelConfig.TensorParallelismOverride

				// Override tensor parallel size if specified
				// --tp-size and --tp are parameters used in sglang
				// --tensor-parallel-size is the parameter used in vllm
				if tensorParallelismConfig.TensorParallelSize != nil && *tensorParallelismConfig.TensorParallelSize > 0 {
					overrideParam(container, []string{"--tp-size", "--tp", "--tensor-parallel-size"}, *tensorParallelismConfig.TensorParallelSize)
				}
				// Override pipeline parallel size if specified
				// --pp-size and --pp are parameters used in sglang
				// --pipeline-parallel-size is parameter used in vllm
				if tensorParallelismConfig.PipelineParallelSize != nil && *tensorParallelismConfig.PipelineParallelSize > 0 {
					overrideParam(container, []string{"--pp-size", "--pp", "--pipeline-parallel-size"}, *tensorParallelismConfig.PipelineParallelSize)
				}
			}
		}
	}
}

func overrideParam(container *corev1.Container, aliases []string, value int64) {
	var updated bool
	// First, try to override in container.Args
	for _, alias := range aliases {
		container.Args, updated = isvcutils.OverrideArgParam(container.Args, alias, value)
		if updated {
			return // Found and updated in Args
		}
	}

	// If not found in Args, try to override in container.Command
	for _, alias := range aliases {
		container.Command, updated = isvcutils.OverrideCommandParam(container.Command, alias, value)
		if updated {
			return // Found and updated in Command
		}
	}
}

// isResourcesUnspecified checks if the resource requirements are unspecified
func isResourcesUnspecified(resources corev1.ResourceRequirements) bool {
	return resources.Limits == nil && resources.Requests == nil && len(resources.Claims) == 0
}

// MergeResources merges resource requests and limits from the runtime and accelerator class into the container
func MergeResources(p *Piece, container *corev1.Container) {
	isvcutils.MergeResource(container, p.AcceleratorClass, p.Runtime)
}

// MergeEngineResources merges resource requests and limits for the engine container.
// It only merges resources from the runtime and accelerator class when the user has not
// explicitly specified resources in the InferenceService spec. This ensures user-specified
// resources are respected and not overridden, while providing sensible defaults from the
// runtime and accelerator class when resources are not specified.
func MergeEngineResources(p *Piece, isvc *v1beta1.InferenceService, container *corev1.Container) {
	if isvc.Spec.Engine != nil &&
		(isvc.Spec.Engine.Runner == nil ||
			isResourcesUnspecified(isvc.Spec.Engine.Runner.Container.Resources)) {
		p.Log.V(1).Info("Merging resources for engine container as user did not specify resources in InferenceService")
		MergeResources(p, container)
	}
}

// MergeDecoderResources merges resource requests and limits for the decoder container.
// It only merges resources from the runtime and accelerator class when the user has not
// explicitly specified resources in the InferenceService spec. This ensures user-specified
// resources are respected and not overridden, while providing sensible defaults from the
// runtime and accelerator class when resources are not specified.
func MergeDecoderResources(p *Piece, isvc *v1beta1.InferenceService, container *corev1.Container) {
	if isvc.Spec.Decoder != nil &&
		(isvc.Spec.Decoder.Runner == nil ||
			isResourcesUnspecified(isvc.Spec.Decoder.Runner.Container.Resources)) {
		p.Log.V(1).Info("Merging resources for decoder container as user did not specify resources in InferenceService")
		MergeResources(p, container)
	}
}

// UpdateEngineAffinity applies the accelerator class's discovery affinity to the
// engine pod spec when the user did not specify affinity in the InferenceService.
func UpdateEngineAffinity(p *Piece, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec) {
	if isvc.Spec.Engine != nil && isvc.Spec.Engine.PodSpec.Affinity == nil {
		mergeAcceleratorAffinity(p, podSpec)
	}
}

// UpdateDecoderAffinity applies the accelerator class's discovery affinity to the
// decoder pod spec when the user did not specify affinity in the InferenceService.
func UpdateDecoderAffinity(p *Piece, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec) {
	if isvc.Spec.Decoder != nil && isvc.Spec.Decoder.PodSpec.Affinity == nil {
		mergeAcceleratorAffinity(p, podSpec)
	}
}

// AcceleratorProvidesParallelismOverride reports whether the selected
// accelerator class supplies a TensorParallelismOverride for the matched
// model format. Only then does the accelerator config own the parallelism
// flags; otherwise the automatic PARALLELISM_SIZE computation still applies.
func AcceleratorProvidesParallelismOverride(p *Piece) bool {
	if p.AcceleratorClassName == "" || p.SupportedModelFormat == nil {
		return false
	}
	acceleratorConfig := p.SupportedModelFormat.GetAcceleratorConfig(p.AcceleratorClassName)
	return acceleratorConfig != nil && acceleratorConfig.TensorParallelismOverride != nil
}

// mergeAcceleratorAffinity fills the pod spec's NodeAffinity from the
// accelerator class's discovery affinity when the pod spec has none.
// Only NodeAffinity is taken: Discovery describes which NODES carry the
// hardware, and copying class-level pod (anti-)affinity terms could
// suppress the gang co-location terms OMENative injects per Instance
// (a pre-existing required podAffinity on the gang topology key skips
// the worker-follows-leader injection).
func mergeAcceleratorAffinity(p *Piece, podSpec *corev1.PodSpec) {
	if p.AcceleratorClass == nil || p.AcceleratorClass.Discovery.Affinity == nil {
		return
	}
	acAffinity := p.AcceleratorClass.Discovery.Affinity
	if acAffinity.NodeAffinity == nil {
		return
	}
	if podSpec.Affinity == nil {
		podSpec.Affinity = &corev1.Affinity{}
	}
	if podSpec.Affinity.NodeAffinity == nil {
		podSpec.Affinity.NodeAffinity = acAffinity.NodeAffinity.DeepCopy()
	}
}

// SetParallelismEnvVar calculates and sets the PARALLELISM_SIZE environment
// variable for the container: the accelerators of one pod times the pods of
// one replica, numLeaders leader pods plus workerReplicas workers. Each
// component supplies its own leader count: an engine replica always has one,
// a decoder counts one only when its spec carries a Leader or a Runner.
func SetParallelismEnvVar(p *Piece, container *corev1.Container, numLeaders, workerReplicas int) {
	if container == nil {
		p.Log.V(2).Info("Cannot set parallelism: container is nil")
		return
	}

	numGPUsPerPod := int64(isvcutils.GetGpuCountFromContainer(container, p.InferenceServiceConfig.AcceleratorResourceNames()))
	leaders := int64(numLeaders)
	numWorkers := int64(workerReplicas)

	// Only proceed if there are GPUs and some form of parallelism (leaders or workers)
	if numGPUsPerPod > 0 && (leaders > 0 || numWorkers > 0) {
		parallelismSize := numGPUsPerPod * (leaders + numWorkers)
		if parallelismSize > 0 {
			envVar := corev1.EnvVar{Name: constants.ParallelismSizeEnvVarKey, Value: strconv.FormatInt(parallelismSize, 10)}
			isvcutils.UpdateEnvVars(container, &envVar)
			p.Log.V(2).Info("Added parallelism env variable to container", "value", parallelismSize, "containerName", container.Name)
		} else {
			p.Log.V(2).Info("Calculated parallelism is zero, not adding env var", "containerName", container.Name)
		}
	} else {
		p.Log.V(2).Info("Conditions not met for parallelism (no GPUs or no leaders/workers)", "containerName", container.Name, "gpus", numGPUsPerPod, "leaders", leaders, "workers", numWorkers)
	}
}
