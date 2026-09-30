package components

import (
	"context"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	"sigs.k8s.io/ome/pkg/render"
)

var _ Component = &Engine{}
var _ ComponentConfig = &Engine{}

// Engine reconciles resources for the engine component
type Engine struct {
	BaseComponentFields
	engineSpec           *v1beta1.EngineSpec
	deploymentReconciler *common.DeploymentReconciler
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
	}
}

// Reconcile implements the Component interface for Engine
func (e *Engine) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	e.Log.V(1).Info("Reconciling engine component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate engine spec
	if e.engineSpec == nil {
		return ctrl.Result{}, errors.New("engine spec is nil")
	}

	// Render the pod templates: fine-tuned weights, object metadata, the
	// primary pod and the worker pod. The engine spec is not mutated.
	rendered, err := render.RenderEngine(ctx, &e.Piece, isvc, e.engineSpec)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to render engine pods")
	}
	objectMeta, podSpec, workerPodSpec, size := rendered.ObjectMeta, rendered.Primary, rendered.Worker, rendered.WorkerSize

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

	if err := checkPlacementDemand(ctx, &e.BaseComponentFields, isvc, v1beta1.EngineComponent, e.engineSpec.Leader != nil, e.engineSpec.Worker != nil,
		ReplicaTemplates{Primary: podSpec, Worker: workerPodSpec, WorkerSize: size}); err != nil {
		return ctrl.Result{}, err
	}

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
			PacingPartition:           e.PacingPartition,
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

// The render steps below expose the library one step at a time for the tests
// that exercise them in isolation; Reconcile renders through
// render.RenderEngine. Unlike RenderEngine, they complete the runner
// containers in place on the engine spec.

// reconcileObjectMeta builds the engine's object metadata.
func (e *Engine) reconcileObjectMeta(_ context.Context, isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	var engineAnnotations, engineLabels map[string]string
	if e.engineSpec != nil {
		engineAnnotations = e.engineSpec.Annotations
		engineLabels = e.engineSpec.Labels
	}

	return ReconcileComponentObjectMeta(&e.BaseComponentFields, isvc, v1beta1.EngineComponent, render.ComponentName(isvc, v1beta1.EngineComponent), engineAnnotations, engineLabels)
}

// engineUsesLeaderTemplate reports whether the engine sources its primary
// pod template from the Leader block.
func engineUsesLeaderTemplate(spec *v1beta1.EngineSpec) bool {
	return render.EngineUsesLeaderTemplate(spec)
}

// reconcilePodSpec renders the engine's primary pod.
func (e *Engine) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	return render.EnginePodSpec(&e.Piece, isvc, e.engineSpec, objectMeta)
}

// reconcileWorkerPodSpec renders the engine's worker pod, nil without a worker.
func (e *Engine) reconcileWorkerPodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	return render.EngineWorkerPodSpec(&e.Piece, isvc, e.engineSpec, objectMeta)
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
