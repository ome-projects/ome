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

var _ Component = &Decoder{}
var _ ComponentConfig = &Decoder{}

// Decoder reconciles resources for the decoder component
type Decoder struct {
	BaseComponentFields
	decoderSpec          *v1beta1.DecoderSpec
	deploymentReconciler *common.DeploymentReconciler
	podSpecReconciler    *common.PodSpecReconciler
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
		podSpecReconciler: &common.PodSpecReconciler{
			Log: base.Log,
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

	// Reconcile fine-tuned weights if specified
	if isvc.Spec.Model != nil && len(isvc.Spec.Model.FineTunedWeights) > 0 {
		if err := ReconcileFineTunedWeights(&d.BaseComponentFields, isvc); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile fine-tuned weights")
		}
	}

	// Reconcile object metadata
	objectMeta, err := d.reconcileObjectMeta(ctx, isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile object metadata")
	}
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

	// Reconcile pod spec
	podSpec, err := d.reconcilePodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile pod spec")
	}

	// Reconcile worker pod spec if needed
	workerPodSpec, err := d.reconcileWorkerPodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile worker pod spec")
	}

	// Get worker size
	size := d.getWorkerSize()

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

// getWorkerSize returns the worker size for multi-node deployments
func (d *Decoder) getWorkerSize() int {
	var size int

	// Prioritize sizes in order: Decoder.Worker -> default
	switch {
	case d.decoderSpec.Worker != nil && d.decoderSpec.Worker.Size != nil:
		size = *d.decoderSpec.Worker.Size
	default:
		size = 0 // Default value
	}

	return size
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

// reconcileObjectMeta creates the object metadata for the decoder
// component. Delegates the annotation / label merge to the shared
// ReconcileComponentObjectMeta helper in base.go; the per-component
// name resolution stays here because decoder gates the Service-
// existence lookup on non-MultiNode mode (engine / router don't).
func (d *Decoder) reconcileObjectMeta(ctx context.Context, isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	decoderName, err := d.determineDecoderName(ctx, isvc)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}

	var decoderAnnotations, decoderLabels map[string]string
	if d.decoderSpec != nil {
		decoderAnnotations = d.decoderSpec.Annotations
		decoderLabels = d.decoderSpec.Labels
	}

	return ReconcileComponentObjectMeta(&d.BaseComponentFields, isvc, v1beta1.DecoderComponent, decoderName, decoderAnnotations, decoderLabels)
}

// determineDecoderName determines the name of the decoder service.
// The suffix is sourced from GetServiceSuffix so the engine / decoder /
// router suffix lives in exactly one place (ComponentConfig).
func (d *Decoder) determineDecoderName(ctx context.Context, isvc *v1beta1.InferenceService) (string, error) {
	defaultDecoderName := isvc.Name + d.GetServiceSuffix()

	if d.DeploymentMode != constants.MultiNode {
		existing := &v1.Service{}
		if err := d.Client.Get(ctx, types.NamespacedName{Name: defaultDecoderName, Namespace: isvc.Namespace}, existing); err == nil {
			return defaultDecoderName, nil
		}
	}

	// If the default name doesn't exist, use it
	return defaultDecoderName, nil
}

// decoderUsesLeaderTemplate reports whether the decoder should source its
// primary pod template from the Leader block (multi-pod shape — MultiNode
// or multi-pod OMENative) rather than the top-level decoder spec
// (single-pod shape). Pure structural check on the spec; it deliberately
// does NOT consult the deployment mode, so dispatch-mode classification and
// template selection stay decoupled. Mirrors engineUsesLeaderTemplate.
func decoderUsesLeaderTemplate(spec *v1beta1.DecoderSpec) bool {
	return spec != nil && spec.Leader != nil
}

// reconcilePodSpec creates the pod spec for the decoder component
func (d *Decoder) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Template selection is keyed on the presence of a Leader block, NOT on
	// the deployment mode. d.DeploymentMode is set authoritatively at
	// construction time; switching on it here mis-selects the top-level
	// (empty) template for OMENative-mode decoders that also set
	// Leader/Worker — the dispatch classifies multi-pod OMENative as
	// OMENative, not MultiNode, so a `case constants.MultiNode` never
	// matches and the spec collapses to the empty top-level runner ("no
	// containers found in pod spec and no runner spec provided"). Mirrors
	// the engine path (engineUsesLeaderTemplate).
	var basePodSpec v1beta1.PodSpec
	var runnerSpec *v1beta1.RunnerSpec

	if decoderUsesLeaderTemplate(d.decoderSpec) {
		basePodSpec = d.decoderSpec.Leader.PodSpec
		runnerSpec = d.decoderSpec.Leader.Runner
	} else {
		// Fallback to the top-level decoder spec — covers single-pod
		// OMENative, RawDeployment, and the malformed-but-tolerated
		// MultiNode-without-Leader shape.
		basePodSpec = d.decoderSpec.PodSpec
		runnerSpec = d.decoderSpec.Runner
	}

	if runnerSpec != nil {
		UpdateEnvVariables(&d.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		UpdateVolumeMounts(&d.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		MergeDecoderResources(&d.BaseComponentFields, isvc, &runnerSpec.Container)
		MergeRuntimeArgumentsOverride(&d.BaseComponentFields, &runnerSpec.Container)
		if !acceleratorProvidesParallelismOverride(&d.BaseComponentFields) {
			d.setParallelismEnvVarForDecoder(&runnerSpec.Container, d.getWorkerSize())
		}
	}

	// Use common pod spec reconciler for base logic
	podSpec, err := d.podSpecReconciler.ReconcilePodSpec(isvc, objectMeta, &basePodSpec, runnerSpec)
	if err != nil {
		return nil, err
	}

	UpdatePodSpecVolumes(&d.BaseComponentFields, isvc, podSpec, objectMeta)
	UpdatePodSpecNodeSelector(&d.BaseComponentFields, isvc, podSpec, v1beta1.DecoderComponent)
	UpdateDecoderAffinity(&d.BaseComponentFields, isvc, podSpec)

	d.Log.V(1).Info("Decoder PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// reconcileWorkerPodSpec reconciles the worker pod spec for multi-node deployments
func (d *Decoder) reconcileWorkerPodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Return nil if no worker spec is defined
	if d.decoderSpec.Worker == nil {
		return nil, nil
	}

	// Get leader runner spec if available
	var workerRunner *v1beta1.RunnerSpec
	if d.decoderSpec.Worker != nil {
		workerRunner = d.decoderSpec.Worker.Runner
		if workerRunner != nil {
			UpdateVolumeMounts(&d.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			UpdateEnvVariables(&d.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			MergeDecoderResources(&d.BaseComponentFields, isvc, &workerRunner.Container)
			MergeRuntimeArgumentsOverride(&d.BaseComponentFields, &workerRunner.Container)
			if !acceleratorProvidesParallelismOverride(&d.BaseComponentFields) {
				d.setParallelismEnvVarForDecoder(&workerRunner.Container, d.getWorkerSize())
			}
		}
	}

	// Use common reconciler for worker pod spec
	workerPodSpec, err := d.podSpecReconciler.ReconcileWorkerPodSpec(isvc, objectMeta, &d.decoderSpec.Worker.PodSpec, workerRunner)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(&d.BaseComponentFields, isvc, workerPodSpec, objectMeta)
	UpdatePodSpecNodeSelector(&d.BaseComponentFields, isvc, workerPodSpec, v1beta1.DecoderComponent)
	UpdateDecoderAffinity(&d.BaseComponentFields, isvc, workerPodSpec)

	d.Log.V(1).Info("Decoder Worker PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return workerPodSpec, nil
}

// setParallelismEnvVarForDecoder calculates and sets the PARALLELISM_SIZE environment variable for the decoder's container.
func (d *Decoder) setParallelismEnvVarForDecoder(container *v1.Container, workerReplicas int) {
	if container == nil || d.decoderSpec == nil {
		d.Log.V(2).Info("Cannot set parallelism: container or decoderSpec is nil")
		return
	}

	numGPUsPerPod := int64(isvcutils.GetGpuCountFromContainer(container, d.InferenceServiceConfig.AcceleratorResourceNames()))
	numLeaders := int64(0)
	numWorkers := int64(workerReplicas)

	// Determine leader presence
	if d.decoderSpec.Leader != nil {
		numLeaders = 1
	} else if d.decoderSpec.Runner != nil { // Raw deployment or single pod considered as leader
		numLeaders = 1
	}

	// Only proceed if there are GPUs and some form of parallelism (leaders or workers)
	if numGPUsPerPod > 0 && (numLeaders > 0 || numWorkers > 0) {
		parallelismSize := numGPUsPerPod * (numLeaders + numWorkers)
		if parallelismSize > 0 {
			envVar := v1.EnvVar{Name: constants.ParallelismSizeEnvVarKey, Value: strconv.FormatInt(parallelismSize, 10)}
			isvcutils.UpdateEnvVars(container, &envVar)
			d.Log.V(2).Info("Added parallelism env variable to decoder container", "value", parallelismSize, "containerName", container.Name)
		} else {
			d.Log.V(2).Info("Calculated parallelism is zero, not adding env var", "containerName", container.Name)
		}
	} else {
		d.Log.V(2).Info("Conditions not met for parallelism (no GPUs or no leaders/workers)", "containerName", container.Name, "gpus", numGPUsPerPod, "leaders", numLeaders, "workers", numWorkers)
	}
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
