package render

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// RenderEngine renders the engine: fine-tuned weights, template metadata, the
// primary pod (leader template when a Leader block is declared, else the
// component template) and the worker pod. The spec must carry the runtime
// merge and deployment defaults. Inputs are copied; the caller's spec is not
// mutated.
func RenderEngine(ctx context.Context, p *Piece, service *v1beta1.InferenceService, spec *v1beta1.EngineSpec) (Rendered, error) {
	if p == nil || p.Client == nil || service == nil || spec == nil {
		return Rendered{}, fmt.Errorf("engine rendering requires render inputs with a client, a service, and a merged spec")
	}
	service, spec = service.DeepCopy(), spec.DeepCopy()
	if err := ReconcileFineTunedWeights(p, service); err != nil {
		return Rendered{}, err
	}
	meta, err := ReconcileComponentObjectMeta(p, service, v1beta1.EngineComponent, ComponentName(service, v1beta1.EngineComponent), spec.Annotations, spec.Labels)
	if err != nil {
		return Rendered{}, err
	}
	primary, err := EnginePodSpec(p, service, spec, &meta)
	if err != nil {
		return Rendered{}, err
	}
	worker, err := EngineWorkerPodSpec(p, service, spec, &meta)
	if err != nil {
		return Rendered{}, err
	}
	t := Templates{ObjectMeta: meta, Primary: primary, Worker: worker, WorkerSize: EngineWorkerSize(spec), MultiPod: spec.Leader != nil && spec.Worker != nil}
	return Rendered{Templates: t, ComponentExt: &spec.ComponentExtensionSpec,
		TopologyKey: spec.TopologyKey, TopologySpread: spec.TopologySpread, TopologySpreadKey: spec.TopologySpreadKey}, nil
}

// EngineUsesLeaderTemplate reports whether the engine sources its primary pod
// template from the Leader block (multi-pod shape — MultiNode or multi-pod
// OMENative) rather than the top-level engine spec (single-pod shape). It is
// a pure structural check on the spec; it deliberately does NOT consult the
// deployment mode, so dispatch-mode classification and template selection
// stay decoupled.
func EngineUsesLeaderTemplate(spec *v1beta1.EngineSpec) bool {
	return spec != nil && spec.Leader != nil
}

// EnginePodSpec renders the engine's primary pod. The selected runner
// container is completed in place on spec (env, volume mounts, resources,
// accelerator argument overrides, parallelism), then the template is
// converted and the model volumes, node selector and accelerator affinity
// applied.
func EnginePodSpec(p *Piece, isvc *v1beta1.InferenceService, spec *v1beta1.EngineSpec, objectMeta *metav1.ObjectMeta) (*corev1.PodSpec, error) {
	// Template selection is keyed on the presence of a Leader block, NOT on
	// the deployment mode. p.DeploymentMode is the mode the dispatch resolved;
	// re-inferring it from the spec here would disagree with the dispatch for
	// OMENative-mode engines that also set Leader/Worker (the inference
	// returns MultiNode; the dispatch is OMENative).
	var basePodSpec v1beta1.PodSpec
	var runnerSpec *v1beta1.RunnerSpec

	if EngineUsesLeaderTemplate(spec) {
		basePodSpec = spec.Leader.PodSpec
		runnerSpec = spec.Leader.Runner
	} else {
		// Fallback to the top-level engine spec — covers single-pod
		// OMENative, RawDeployment, and the malformed-but-tolerated
		// MultiNode-without-Leader shape.
		basePodSpec = spec.PodSpec
		runnerSpec = spec.Runner
	}
	if runnerSpec != nil {
		UpdateEnvVariables(p, isvc, &runnerSpec.Container, objectMeta)
		UpdateVolumeMounts(p, isvc, &runnerSpec.Container, objectMeta)
		MergeEngineResources(p, isvc, &runnerSpec.Container)
		MergeRuntimeArgumentsOverride(p, &runnerSpec.Container)
		if spec != nil && !AcceleratorProvidesParallelismOverride(p) {
			SetParallelismEnvVar(p, &runnerSpec.Container, engineParallelismLeaders(), EngineWorkerSize(spec))
		}
	}

	// Merge the runner container into the pod spec.
	podSpec, err := (&PodSpecReconciler{Log: p.Log}).ReconcilePodSpec(isvc, objectMeta, &basePodSpec, runnerSpec)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(p, isvc, podSpec, objectMeta)
	UpdatePodSpecNodeSelector(p, isvc, podSpec, v1beta1.EngineComponent)
	UpdateEngineAffinity(p, isvc, podSpec)

	p.Log.V(1).Info("Engine PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// EngineWorkerPodSpec renders the engine's worker pod, nil when the spec
// declares no worker. The worker runner container is completed in place on
// spec the same way as the primary's.
func EngineWorkerPodSpec(p *Piece, isvc *v1beta1.InferenceService, spec *v1beta1.EngineSpec, objectMeta *metav1.ObjectMeta) (*corev1.PodSpec, error) {
	// Return nil if no worker spec is defined
	if spec.Worker == nil {
		return nil, nil
	}

	// Get worker runner spec if available
	var workerRunner *v1beta1.RunnerSpec
	if spec.Worker != nil {
		workerRunner = spec.Worker.Runner
		if workerRunner != nil {
			UpdateVolumeMounts(p, isvc, &workerRunner.Container, objectMeta)
			UpdateEnvVariables(p, isvc, &workerRunner.Container, objectMeta)
			MergeEngineResources(p, isvc, &workerRunner.Container)
			MergeRuntimeArgumentsOverride(p, &workerRunner.Container)
			if spec != nil && !AcceleratorProvidesParallelismOverride(p) {
				SetParallelismEnvVar(p, &workerRunner.Container, engineParallelismLeaders(), EngineWorkerSize(spec))
			}
		}
	}

	// Merge the worker runner container into the worker pod spec.
	workerPodSpec, err := (&PodSpecReconciler{Log: p.Log}).ReconcileWorkerPodSpec(isvc, objectMeta, &spec.Worker.PodSpec, workerRunner)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(p, isvc, workerPodSpec, objectMeta)
	UpdatePodSpecNodeSelector(p, isvc, workerPodSpec, v1beta1.EngineComponent)
	UpdateEngineAffinity(p, isvc, workerPodSpec)
	p.Log.V(1).Info("Engine Worker PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return workerPodSpec, nil
}

// EngineWorkerSize is the worker count of one engine replica: Worker.Size
// when the spec declares a sized worker, else zero.
func EngineWorkerSize(spec *v1beta1.EngineSpec) int {
	var size int

	// Prioritize sizes in order: Engine.Worker -> default
	switch {
	case spec.Worker != nil && spec.Worker.Size != nil:
		size = *spec.Worker.Size
	default:
		size = 0 // Default value
	}

	return size
}

// engineParallelismLeaders is the leader count PARALLELISM_SIZE counts for the
// engine: an engine replica always has one leader (or single) pod.
func engineParallelismLeaders() int {
	return 1 // at least one leader/pod
}
