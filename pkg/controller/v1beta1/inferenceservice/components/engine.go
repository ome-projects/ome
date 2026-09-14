package components

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

var _ Component = &Engine{}
var _ ComponentConfig = &Engine{}

// Engine reconciles resources for the engine component
type Engine struct {
	BaseComponentFields
	engineSpec           *v1beta1.EngineSpec
	deploymentReconciler *common.DeploymentReconciler
	podSpecReconciler    *common.PodSpecReconciler
}

// NewEngine creates a new Engine component instance. deps carries the
// process-lifetime wiring (client, live APIReader, Expectations cache,
// recorder — any may be nil; the OMENative backend falls back to the
// cached client, the DefaultExpectations singleton, and a no-op event
// recorder respectively); in carries the per-reconcile pipeline output.
func NewEngine(deps *ComponentDeps, in ComponentInputs, engineSpec *v1beta1.EngineSpec) Component {
	base := newBaseComponentFields(deps, in, "EngineReconciler")

	return &Engine{
		BaseComponentFields: base,
		engineSpec:          engineSpec,
		deploymentReconciler: &common.DeploymentReconciler{
			Client:        deps.Client,
			APIReader:     deps.APIReader,
			Clientset:     deps.Clientset,
			Scheme:        deps.Scheme,
			StatusManager: base.StatusManager,
			Log:           base.Log,
		},
		podSpecReconciler: &common.PodSpecReconciler{
			Log: base.Log,
		},
	}
}

// Reconcile implements the Component interface for Engine
func (e *Engine) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	e.Log.V(1).Info("Reconciling engine component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate engine spec
	if e.engineSpec == nil {
		return ctrl.Result{}, errors.New("engine spec is nil")
	}

	// Reconcile fine-tuned weights if specified
	if isvc.Spec.Model != nil && len(isvc.Spec.Model.FineTunedWeights) > 0 {
		if err := ReconcileFineTunedWeights(&e.BaseComponentFields, isvc); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile fine-tuned weights")
		}
	}

	// Reconcile object metadata
	objectMeta, err := e.reconcileObjectMeta(ctx, isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile object metadata")
	}
	pdbRequest, err := resolveComponentPDBRequest(
		&e.BaseComponentFields,
		isvc,
		e.DeploymentMode,
		v1beta1.EngineComponent,
		objectMeta,
		&e.engineSpec.ComponentExtensionSpec,
	)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to resolve engine PodDisruptionBudget")
	}
	if err := preflightComponentPDB(ctx, &e.BaseComponentFields, pdbRequest); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to preflight engine PodDisruptionBudget")
	}

	// Reconcile pod spec
	podSpec, err := e.reconcilePodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile pod spec")
	}

	// Reconcile worker pod spec if needed
	workerPodSpec, err := e.reconcileWorkerPodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile worker pod spec")
	}

	// Get worker size
	size := e.getWorkerSize()

	// Reconcile deployment based on deployment mode. The deployment
	// reconciler's RequeueAfter MUST be preserved through the rest of
	// this function — OMENative's per-Instance dispatcher uses it to
	// re-run after a Sequential / Ratio gate denial OR to poll an
	// in-flight Update / Create. Discarding it strands the rollout
	// until an unrelated watch event happens to fire another reconcile
	// (the post-decoder Sequential stall).
	deploymentResult, err := e.reconcileDeployment(ctx, isvc, objectMeta, podSpec, size, workerPodSpec, pdbRequest)
	if err != nil {
		return deploymentResult, err
	}

	// Per-Component stable Service + PodMonitor for OMENative. Inlined
	// here (rather than driven from the IR controller) because every
	// top-level per-Component sub-resource is owned by the ISVC
	// controller; the IR controller manages only pods + the per-Component
	// headless Service (whose naming is tied to the per-Component pod
	// template).
	//
	// Raw / MultiNode keep their dispatcher-side service+podmonitor calls
	// because their selectors are mode-specific (Raw: nil → pod template
	// labels; MultiNode: worker-index=0 leader-only routing).
	if e.DeploymentMode == constants.OMENative {
		if err := e.reconcileOMENativeSubresources(ctx, isvc, objectMeta, podSpec); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile engine OMENative sub-resources")
		}
	}

	// Update engine status
	if err := e.updateEngineStatus(isvc, objectMeta); err != nil {
		return ctrl.Result{}, err
	}

	return deploymentResult, nil
}

// reconcileOMENativeSubresources delegates to the shared
// ReconcileOMENativeSubresources helper in base.go — engine, decoder,
// and router emit byte-identical Service + PodMonitor pairs; only the
// component enum and ComponentExtensionSpec pointer differ.
func (e *Engine) reconcileOMENativeSubresources(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec) error {
	return ReconcileOMENativeSubresources(ctx, &e.BaseComponentFields, isvc, v1beta1.EngineComponent, &e.engineSpec.ComponentExtensionSpec, objectMeta, podSpec)
}

// getWorkerSize returns the worker size for multi-node deployments
func (e *Engine) getWorkerSize() int {
	var size int

	// Prioritize sizes in order: Engine.Worker -> default
	switch {
	case e.engineSpec.Worker != nil && e.engineSpec.Worker.Size != nil:
		size = *e.engineSpec.Worker.Size
	default:
		size = 0 // Default value
	}

	return size
}

// reconcileDeployment manages the deployment logic for different deployment modes
func (e *Engine) reconcileDeployment(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec, workerSize int, workerPodSpec *v1.PodSpec, pdbRequest pdb.Request) (ctrl.Result, error) {
	switch e.DeploymentMode {
	case constants.RawDeployment:
		rawResolved, err := resolveRawComponentAutoscaling(ctx, &e.BaseComponentFields, isvc, v1beta1.EngineComponent, &e.engineSpec.ComponentExtensionSpec, objectMeta.Annotations)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to resolve autoscaler for raw engine")
		}
		result, err := e.deploymentReconciler.ReconcileRawDeployment(ctx, isvc, objectMeta, podSpec, &e.engineSpec.ComponentExtensionSpec, v1beta1.EngineComponent, rawResolved, pdbRequest)
		if err != nil {
			return result, err
		}
		// A policy hold recovers via config edits that emit no event, so the
		// hold carries its own periodic requeue. Smaller-nonzero merge: never
		// delay a dispatcher poll already scheduled on this result.
		return mergeRequeueAfter(result, rawResolved.RequeueAfter), nil
	case constants.MultiNode:
		return e.deploymentReconciler.ReconcileMultiNodeDeployment(isvc, objectMeta, podSpec, workerSize, workerPodSpec, &e.engineSpec.ComponentExtensionSpec, v1beta1.EngineComponent)
	case constants.OMENative:
		// Admission rejects orphan Leader/Worker and Worker.Size <= 0, so
		// the only valid multi-pod shape has both fields set.
		multiPod := e.engineSpec.Leader != nil && e.engineSpec.Worker != nil
		// OMENative-mode Components dispatch through the InferenceReplica
		// path: the ISVC controller projects the desired per-Component
		// spec onto an InferenceReplica object; the IR controller drives
		// per-Instance lifecycle from there.
		//
		// Resolve the authoritative ComponentAutoscaler from the
		// ISVC → policy → runtime → default chain. The resolved block is
		// projected onto ir.Spec.Autoscaler by the projector and
		// dispatched as HPA / KEDA / external / none against the
		// committed IR (autoscaler dispatch is always-on per Component).
		// A policy hold preserves the IR's stored block as last-known-good.
		res, err := resolveComponentAutoscaling(ctx, &e.BaseComponentFields, isvc, v1beta1.EngineComponent, &e.engineSpec.ComponentExtensionSpec)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to resolve autoscaler for engine")
		}
		ir, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
			ISVC:                      isvc,
			Component:                 v1beta1.EngineComponent,
			ComponentExt:              &e.engineSpec.ComponentExtensionSpec,
			ObjectMeta:                objectMeta,
			PodSpec:                   podSpec,
			WorkerPodSpec:             workerPodSpec,
			WorkerSize:                workerSize,
			MultiPod:                  multiPod,
			TopologyKey:               e.engineSpec.TopologyKey,
			TopologySpread:            e.engineSpec.TopologySpread,
			TopologySpreadKey:         e.engineSpec.TopologySpreadKey,
			ResolvedAutoscaler:        res.Resolved,
			PreserveAutoscaler:        res.Hold,
			QuotaAcceleratorResources: e.QuotaAcceleratorResources,
			Client:                    e.Client,
			Reader:                    e.APIReader,
		})
		if err != nil {
			if apierrors.IsConflict(err) {
				// Benign: a concurrent IR status write bumped the object.
				// Re-read and reproject next pass instead of surfacing a hard
				// error, which would fast-loop the ISVC reconcile.
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, errors.Wrap(err, "failed to project InferenceReplica for engine")
		}
		if err := ReconcileOMENativePDB(
			ctx,
			&e.BaseComponentFields,
			v1beta1.EngineComponent,
			ir,
			pdbRequest,
		); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile engine OMENative PodDisruptionBudget")
		}
		// Autoscaler dispatch is always-on per InferenceReplica.
		// Owner-ref the HPA / SO to the IR so GC cascades
		// when the IR is deleted; ScaleTargetRef points at the IR's
		// /scale subresource. external + none are status-field twins —
		// both fall through to the dispatch's "delete both" branch
		// with no separate code path.
		if err := dispatchIRAutoscaler(ctx, &e.BaseComponentFields, isvc, ir, &e.engineSpec.ComponentExtensionSpec, res); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to dispatch autoscaler for engine")
		}
		// A policy hold recovers via config edits that emit no event; carry
		// its periodic requeue on the result (smaller-nonzero merge).
		return mergeRequeueAfter(ctrl.Result{}, res.RequeueAfter), nil
	default:
		return ctrl.Result{}, errors.New("invalid deployment mode for engine")
	}
}

// updateEngineStatus updates the status of the engine
func (e *Engine) updateEngineStatus(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) error {
	return UpdateComponentStatus(&e.BaseComponentFields, isvc, v1beta1.EngineComponent, objectMeta, &e.engineSpec.ComponentExtensionSpec)
}

// reconcileObjectMeta creates the object metadata for the engine
// component. Delegates the annotation / label merge to the shared
// ReconcileComponentObjectMeta helper in base.go; the per-component
// name resolution stays here because the fallback rules still differ
// across components (Decoder gates the Service-existence lookup on
// non-MultiNode mode; engine / router don't — see section 4 of the
// components-dispatch review).
func (e *Engine) reconcileObjectMeta(ctx context.Context, isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	engineName, err := e.determineEngineName(ctx, isvc)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}

	var engineAnnotations, engineLabels map[string]string
	if e.engineSpec != nil {
		engineAnnotations = e.engineSpec.Annotations
		engineLabels = e.engineSpec.Labels
	}

	return ReconcileComponentObjectMeta(&e.BaseComponentFields, isvc, v1beta1.EngineComponent, engineName, engineAnnotations, engineLabels)
}

// determineEngineName determines the name of the engine service.
// The suffix is sourced from GetServiceSuffix so the engine / decoder /
// router suffix lives in exactly one place (ComponentConfig).
func (e *Engine) determineEngineName(ctx context.Context, isvc *v1beta1.InferenceService) (string, error) {
	defaultEngineName := isvc.Name + e.GetServiceSuffix()

	existing := &v1.Service{}
	if err := e.Client.Get(ctx, types.NamespacedName{Name: defaultEngineName, Namespace: isvc.Namespace}, existing); err == nil {
		return defaultEngineName, nil
	}

	return defaultEngineName, nil
}

// engineUsesLeaderTemplate reports whether the engine should source its
// primary pod template from the Leader block (multi-pod shape — MultiNode
// or multi-pod OMENative) rather than the top-level engine spec
// (single-pod shape). It is a pure structural check on the spec; it
// deliberately does NOT consult the deployment mode, so dispatch-mode
// classification and template selection stay decoupled.
func engineUsesLeaderTemplate(spec *v1beta1.EngineSpec) bool {
	return spec != nil && spec.Leader != nil
}

// reconcilePodSpec creates the pod spec for the engine component
func (e *Engine) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Template selection is keyed on the presence of a Leader block, NOT on
	// the deployment mode. e.DeploymentMode is set authoritatively at
	// construction time; calling isvcutils.DetermineEngineDeploymentMode
	// here would re-infer it from the spec and disagree with the dispatch
	// for OMENative-mode engines that also set Leader/Worker (the helper
	// returns MultiNode; the dispatch is OMENative).
	var basePodSpec v1beta1.PodSpec
	var runnerSpec *v1beta1.RunnerSpec

	if engineUsesLeaderTemplate(e.engineSpec) {
		basePodSpec = e.engineSpec.Leader.PodSpec
		runnerSpec = e.engineSpec.Leader.Runner
	} else {
		// Fallback to the top-level engine spec — covers single-pod
		// OMENative, RawDeployment, and the malformed-but-tolerated
		// MultiNode-without-Leader shape.
		basePodSpec = e.engineSpec.PodSpec
		runnerSpec = e.engineSpec.Runner
	}
	if runnerSpec != nil {
		UpdateEnvVariables(&e.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		UpdateVolumeMounts(&e.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		MergeEngineResources(&e.BaseComponentFields, isvc, &runnerSpec.Container)
		MergeRuntimeArgumentsOverride(&e.BaseComponentFields, &runnerSpec.Container)
		if !acceleratorProvidesParallelismOverride(&e.BaseComponentFields) {
			e.setParallelismEnvVarForEngine(&runnerSpec.Container, e.getWorkerSize())
		}
	}

	// Use common pod spec reconciler for base logic
	podSpec, err := e.podSpecReconciler.ReconcilePodSpec(isvc, objectMeta, &basePodSpec, runnerSpec)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(&e.BaseComponentFields, isvc, podSpec, objectMeta)
	UpdatePodSpecNodeSelector(&e.BaseComponentFields, isvc, podSpec, v1beta1.EngineComponent)
	UpdateEngineAffinity(&e.BaseComponentFields, isvc, podSpec)

	e.Log.V(1).Info("Engine PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// reconcileWorkerPodSpec reconciles the worker pod spec for multi-node deployments
func (e *Engine) reconcileWorkerPodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Return nil if no worker spec is defined
	if e.engineSpec.Worker == nil {
		return nil, nil
	}

	// Get worker runner spec if available
	var workerRunner *v1beta1.RunnerSpec
	if e.engineSpec.Worker != nil {
		workerRunner = e.engineSpec.Worker.Runner
		if workerRunner != nil {
			UpdateVolumeMounts(&e.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			UpdateEnvVariables(&e.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			MergeEngineResources(&e.BaseComponentFields, isvc, &workerRunner.Container)
			MergeRuntimeArgumentsOverride(&e.BaseComponentFields, &workerRunner.Container)
			if !acceleratorProvidesParallelismOverride(&e.BaseComponentFields) {
				e.setParallelismEnvVarForEngine(&workerRunner.Container, e.getWorkerSize())
			}
		}
	}

	// Use common reconciler for worker pod spec
	workerPodSpec, err := e.podSpecReconciler.ReconcileWorkerPodSpec(isvc, objectMeta, &e.engineSpec.Worker.PodSpec, workerRunner)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(&e.BaseComponentFields, isvc, workerPodSpec, objectMeta)
	UpdatePodSpecNodeSelector(&e.BaseComponentFields, isvc, workerPodSpec, v1beta1.EngineComponent)
	UpdateEngineAffinity(&e.BaseComponentFields, isvc, workerPodSpec)
	e.Log.V(1).Info("Engine Worker PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return workerPodSpec, nil
}

// setParallelismEnvVarForEngine calculates and sets the PARALLELISM_SIZE environment variable for the engine's container.
func (e *Engine) setParallelismEnvVarForEngine(container *v1.Container, workerReplicas int) {
	if container == nil || e.engineSpec == nil {
		e.Log.V(2).Info("Cannot set parallelism: container or engineSpec is nil")
		return
	}

	numGPUsPerPod := int64(isvcutils.GetGpuCountFromContainer(container, e.InferenceServiceConfig.AcceleratorResourceNames()))
	numLeaders := int64(1) // at least one leader/pod
	numWorkers := int64(workerReplicas)

	// Only proceed if there are GPUs
	if numGPUsPerPod > 0 {
		parallelismSize := numGPUsPerPod * (numLeaders + numWorkers)
		if parallelismSize > 0 {
			envVar := v1.EnvVar{Name: constants.ParallelismSizeEnvVarKey, Value: strconv.FormatInt(parallelismSize, 10)}
			isvcutils.UpdateEnvVars(container, &envVar)
			e.Log.V(2).Info("Added parallelism env variable to engine container", "value", parallelismSize, "containerName", container.Name)
		} else {
			e.Log.V(2).Info("Calculated parallelism is zero, not adding env var", "containerName", container.Name)
		}
	} else {
		e.Log.V(2).Info("Conditions not met for parallelism (no GPUs or no leaders/workers)", "containerName", container.Name, "gpus", numGPUsPerPod, "leaders", numLeaders, "workers", numWorkers)
	}
}

// GetComponentType implements ComponentConfig interface
func (e *Engine) GetComponentType() v1beta1.ComponentType {
	return v1beta1.EngineComponent
}

// GetComponentSpec implements ComponentConfig interface
func (e *Engine) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if e.engineSpec == nil {
		return nil
	}
	return &e.engineSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (e *Engine) GetServiceSuffix() string {
	return "-engine"
}

// ValidateSpec implements ComponentConfig interface
func (e *Engine) ValidateSpec() error {
	if e.engineSpec == nil {
		return errors.New("engine spec is nil")
	}
	// Add more validation logic as needed
	return nil
}
