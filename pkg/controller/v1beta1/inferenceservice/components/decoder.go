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

var _ Component = &Decoder{}
var _ ComponentConfig = &Decoder{}

// Decoder reconciles resources for the decoder component
type Decoder struct {
	BaseComponentFields
	decoderSpec          *v1beta1.DecoderSpec
	deploymentReconciler *common.DeploymentReconciler
}

// NewDecoder creates a new Decoder component instance. deps carries
// the process-lifetime wiring; in carries the per-reconcile pipeline
// output.
func NewDecoder(deps *ComponentDeps, in ComponentInputs, decoderSpec *v1beta1.DecoderSpec) Component {
	base := newBaseComponentFields(deps, in, "DecoderReconciler")

	return &Decoder{
		BaseComponentFields: base,
		decoderSpec:         decoderSpec,
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

// Reconcile implements the Component interface for Decoder
func (d *Decoder) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	d.Log.V(1).Info("Reconciling decoder component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate decoder spec
	if d.decoderSpec == nil {
		return ctrl.Result{}, errors.New("decoder spec is nil")
	}

	// Render the pod templates: fine-tuned weights, object metadata, the
	// primary pod and the worker pod. The decoder spec is not mutated.
	rendered, err := render.RenderDecoder(ctx, &d.Piece, isvc, d.decoderSpec)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to render decoder pods")
	}
	objectMeta, podSpec, workerPodSpec, size := rendered.ObjectMeta, rendered.Primary, rendered.Worker, rendered.WorkerSize

	pdbRequest, err := resolveComponentPDBRequest(
		&d.BaseComponentFields,
		isvc,
		d.DeploymentMode,
		v1beta1.DecoderComponent,
		objectMeta,
		&d.decoderSpec.ComponentExtensionSpec,
	)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to resolve decoder PodDisruptionBudget")
	}
	if err := preflightComponentPDB(ctx, &d.BaseComponentFields, pdbRequest); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to preflight decoder PodDisruptionBudget")
	}

	if err := checkPlacementDemand(ctx, &d.BaseComponentFields, isvc, v1beta1.DecoderComponent, d.decoderSpec.Leader != nil, d.decoderSpec.Worker != nil,
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
	deploymentResult, err := d.reconcileDeployment(ctx, isvc, objectMeta, podSpec, size, workerPodSpec, pdbRequest)
	if err != nil {
		return deploymentResult, err
	}

	// Per-Component stable Service + PodMonitor for OMENative. See the
	// rationale on Engine.reconcileOMENativeSubresources; this method is
	// the decoder equivalent.
	if d.DeploymentMode == constants.OMENative {
		if err := d.reconcileOMENativeSubresources(ctx, isvc, objectMeta, podSpec); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile decoder OMENative sub-resources")
		}
	}

	// Update decoder status
	if err := d.updateDecoderStatus(isvc, objectMeta); err != nil {
		return ctrl.Result{}, err
	}

	return deploymentResult, nil
}

// reconcileOMENativeSubresources delegates to the shared
// ReconcileOMENativeSubresources helper in base.go — engine, decoder,
// and router emit byte-identical Service + PodMonitor pairs; only the
// component enum and ComponentExtensionSpec pointer differ.
func (d *Decoder) reconcileOMENativeSubresources(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec) error {
	return ReconcileOMENativeSubresources(ctx, &d.BaseComponentFields, isvc, v1beta1.DecoderComponent, &d.decoderSpec.ComponentExtensionSpec, objectMeta, podSpec)
}

// reconcileDeployment manages the deployment logic for different deployment modes
func (d *Decoder) reconcileDeployment(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec, workerSize int, workerPodSpec *v1.PodSpec, pdbRequest pdb.Request) (ctrl.Result, error) {
	switch d.DeploymentMode {
	case constants.RawDeployment:
		rawResolved, err := resolveRawComponentAutoscaling(ctx, &d.BaseComponentFields, isvc, v1beta1.DecoderComponent, &d.decoderSpec.ComponentExtensionSpec, objectMeta.Annotations)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to resolve autoscaler for raw decoder")
		}
		result, err := d.deploymentReconciler.ReconcileRawDeployment(ctx, isvc, objectMeta, podSpec, &d.decoderSpec.ComponentExtensionSpec, v1beta1.DecoderComponent, rawResolved, pdbRequest)
		if err != nil {
			return result, err
		}
		// A policy hold recovers via config edits that emit no event, so the
		// hold carries its own periodic requeue. Smaller-nonzero merge: never
		// delay a dispatcher poll already scheduled on this result.
		return mergeRequeueAfter(result, rawResolved.RequeueAfter), nil
	case constants.MultiNode:
		return d.deploymentReconciler.ReconcileMultiNodeDeployment(isvc, objectMeta, podSpec, workerSize, workerPodSpec, &d.decoderSpec.ComponentExtensionSpec, v1beta1.DecoderComponent)
	case constants.OMENative:
		// Admission rejects orphan Leader/Worker and Worker.Size <= 0, so
		// the only valid multi-pod shape has both fields set.
		multiPod := d.decoderSpec.Leader != nil && d.decoderSpec.Worker != nil
		// OMENative-mode Components dispatch through the InferenceReplica
		// path. See the comment on the engine dispatch for the full design.
		//
		// Resolve the authoritative ComponentAutoscaler from the
		// ISVC → policy → runtime → default chain. See engine dispatch for
		// the full design — dispatches HPA / KEDA / external / none against
		// the committed IR (autoscaler dispatch is always-on per Component).
		res, err := resolveComponentAutoscaling(ctx, &d.BaseComponentFields, isvc, v1beta1.DecoderComponent, &d.decoderSpec.ComponentExtensionSpec)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to resolve autoscaler for decoder")
		}
		ir, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
			ISVC:                      isvc,
			Component:                 v1beta1.DecoderComponent,
			ComponentExt:              &d.decoderSpec.ComponentExtensionSpec,
			ObjectMeta:                objectMeta,
			PodSpec:                   podSpec,
			WorkerPodSpec:             workerPodSpec,
			WorkerSize:                workerSize,
			MultiPod:                  multiPod,
			TopologyKey:               d.decoderSpec.TopologyKey,
			TopologySpread:            d.decoderSpec.TopologySpread,
			TopologySpreadKey:         d.decoderSpec.TopologySpreadKey,
			PacingPartition:           d.PacingPartition,
			ResolvedAutoscaler:        res.Resolved,
			PreserveAutoscaler:        res.Hold,
			QuotaAcceleratorResources: d.QuotaAcceleratorResources,
			Client:                    d.Client,
			Reader:                    d.APIReader,
		})
		if err != nil {
			if apierrors.IsConflict(err) {
				// Benign: a concurrent IR status write bumped the object.
				// Re-read and reproject next pass instead of surfacing a hard
				// error, which would fast-loop the ISVC reconcile.
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, errors.Wrap(err, "failed to project InferenceReplica for decoder")
		}
		if err := ReconcileOMENativePDB(
			ctx,
			&d.BaseComponentFields,
			v1beta1.DecoderComponent,
			ir,
			pdbRequest,
		); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile decoder OMENative PodDisruptionBudget")
		}
		// Autoscaler dispatch is always-on per InferenceReplica.
		// See engine dispatch for the full owner-ref + scaleTargetRef
		// rationale.
		if err := dispatchIRAutoscaler(ctx, &d.BaseComponentFields, isvc, ir, &d.decoderSpec.ComponentExtensionSpec, res); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to dispatch autoscaler for decoder")
		}
		// A policy hold recovers via config edits that emit no event; carry
		// its periodic requeue on the result (smaller-nonzero merge).
		return mergeRequeueAfter(ctrl.Result{}, res.RequeueAfter), nil
	default:
		return ctrl.Result{}, errors.New("invalid deployment mode for decoder")
	}
}

// updateDecoderStatus updates the status of the decoder
func (d *Decoder) updateDecoderStatus(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) error {
	return UpdateComponentStatus(&d.BaseComponentFields, isvc, v1beta1.DecoderComponent, objectMeta, &d.decoderSpec.ComponentExtensionSpec)
}

// The render steps below expose the library one step at a time for the tests
// that exercise them in isolation; Reconcile renders through
// render.RenderDecoder. Unlike RenderDecoder, they complete the runner
// containers in place on the decoder spec.

// reconcileObjectMeta builds the decoder's object metadata.
func (d *Decoder) reconcileObjectMeta(_ context.Context, isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	var decoderAnnotations, decoderLabels map[string]string
	if d.decoderSpec != nil {
		decoderAnnotations = d.decoderSpec.Annotations
		decoderLabels = d.decoderSpec.Labels
	}

	return ReconcileComponentObjectMeta(&d.BaseComponentFields, isvc, v1beta1.DecoderComponent, render.ComponentName(isvc, v1beta1.DecoderComponent), decoderAnnotations, decoderLabels)
}

// decoderUsesLeaderTemplate reports whether the decoder sources its primary
// pod template from the Leader block.
func decoderUsesLeaderTemplate(spec *v1beta1.DecoderSpec) bool {
	return render.DecoderUsesLeaderTemplate(spec)
}

// reconcilePodSpec renders the decoder's primary pod.
func (d *Decoder) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	return render.DecoderPodSpec(&d.Piece, isvc, d.decoderSpec, objectMeta)
}

// reconcileWorkerPodSpec renders the decoder's worker pod, nil without a worker.
func (d *Decoder) reconcileWorkerPodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	return render.DecoderWorkerPodSpec(&d.Piece, isvc, d.decoderSpec, objectMeta)
}

// GetComponentType implements ComponentConfig interface
func (d *Decoder) GetComponentType() v1beta1.ComponentType {
	return v1beta1.DecoderComponent
}

// GetComponentSpec implements ComponentConfig interface
func (d *Decoder) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if d.decoderSpec == nil {
		return nil
	}
	return &d.decoderSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (d *Decoder) GetServiceSuffix() string {
	return "-decoder"
}

// ValidateSpec implements ComponentConfig interface
func (d *Decoder) ValidateSpec() error {
	if d.decoderSpec == nil {
		return errors.New("decoder spec is nil")
	}
	// Add more validation logic as needed
	return nil
}
