package components

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/podmonitor"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils"
)

// BaseComponentFields contains common fields for all components
type BaseComponentFields struct {
	Client    client.Client
	Clientset kubernetes.Interface
	// APIReader is the live (uncached) reader, typically mgr.GetAPIReader().
	// Intended for reads where cache lag would be a correctness problem.
	// Currently unread on the ISVC side — the InferenceReplica controller
	// owns revision bookkeeping, audit ledger dedup, and EndpointSlice
	// drain checks.
	APIReader client.Reader
	// Expectations is the OMENative per-controller create/delete
	// bookkeeping cache. Currently unread on the ISVC side — the
	// InferenceReplica controller owns its own instance. nil for
	// non-OMENative modes.
	Expectations *omenative.Expectations
	// Recorder is the parent controller's event recorder.
	Recorder record.EventRecorder
	// GangSchedulingAvailable is the cluster-discovery boolean threaded
	// from the InferenceServiceReconciler — true when the cluster has
	// the scheduler-plugins `scheduling.x-k8s.io/v1alpha1/PodGroup` CRD
	// installed. Currently unread on the ISVC side — the projected IR
	// carries the flag and the IR controller consults it. Other
	// deployment modes (Raw / MultiNode / PD) ignore this field.
	GangSchedulingAvailable           bool
	Scheme                            *runtime.Scheme
	InferenceServiceConfig            *controllerconfig.InferenceServicesConfig
	DeploymentMode                    constants.DeploymentModeType
	BaseModel                         *v1beta1.BaseModelSpec
	BaseModelMeta                     *metav1.ObjectMeta
	Runtime                           *v1beta1.ServingRuntimeSpec
	RuntimeName                       string
	AcceleratorClass                  *v1beta1.AcceleratorClassSpec
	AcceleratorClassName              string
	FineTunedServing                  bool
	FineTunedServingWithMergedWeights bool
	FineTunedWeights                  []*v1beta1.FineTunedWeight
	StatusManager                     *status.StatusReconciler
	Log                               logr.Logger
	SupportedModelFormat              *v1beta1.SupportedModelFormat
	// Overlays is the resolver output for spec.model.overlays; nil when
	// none declared. Consumed by UpdateVolumeMounts / UpdateEnvVariables
	// / UpdatePodSpecVolumes.
	Overlays []isvcutils.ResolvedOverlay

	// PolicyResolver renders per-component autoscalerPolicyRef attachments;
	// threaded from ComponentInputs (per reconcile). May be nil only in
	// tests; the shared helpers treat nil as feature-disabled and fail refs
	// closed.
	PolicyResolver *autoscaler.PolicyResolver

	// QuotaAcceleratorResources are the resource names that put a Component
	// under the quota backend, threaded from the InferenceServiceReconciler to
	// the InferenceReplica projector. Empty governs everything.
	QuotaAcceleratorResources []string
}

// newBaseComponentFields is the ONE place BaseComponentFields is
// assembled from deps + inputs. Constructors pass their component's
// logger name; nothing else differs per component. The fine-tuned
// serving fields (FineTunedServing / FineTunedServingWithMergedWeights /
// FineTunedWeights) are populated at reconcile time by
// ReconcileFineTunedWeights, never at construction.
func newBaseComponentFields(deps *ComponentDeps, in ComponentInputs, loggerName string) BaseComponentFields {
	return BaseComponentFields{
		Client:                    deps.Client,
		Clientset:                 deps.Clientset,
		APIReader:                 deps.APIReader,
		Expectations:              deps.Expectations,
		Recorder:                  deps.Recorder,
		GangSchedulingAvailable:   deps.GangSchedulingAvailable,
		QuotaAcceleratorResources: deps.QuotaAcceleratorResources,
		Scheme:                    deps.Scheme,
		InferenceServiceConfig:    deps.Config,
		DeploymentMode:            in.DeploymentMode,
		BaseModel:                 in.BaseModel,
		BaseModelMeta:             in.BaseModelMeta,
		Runtime:                   in.Runtime,
		RuntimeName:               in.RuntimeName,
		StatusManager:             status.NewStatusReconciler(),
		Log:                       ctrl.Log.WithName(loggerName),
		SupportedModelFormat:      in.ModelFormat,
		AcceleratorClass:          in.AcceleratorClass,
		AcceleratorClassName:      in.AcceleratorClassName,
		Overlays:                  in.Overlays,
		PolicyResolver:            in.PolicyResolver,
	}
}

// Common methods as functions that operate on BaseComponentFields

// ComponentAutoscaling is one component's autoscaler resolution for a single
// reconcile pass: the policy outcome (nil without a ref), the chain result,
// and the fail-closed hold flag.
type ComponentAutoscaling struct {
	Outcome  *autoscaler.PolicyOutcome
	Resolved *v1beta1.ComponentAutoscaler
	Source   autoscaler.SpecSource
	Hold     bool

	// RequeueAfter is the config-driven periodic retry for a hold: some hold
	// causes (an unbound provider binding) heal via operator-config edits
	// that emit no watch event toward the ISVC, so the component result asks
	// its caller to requeue. Zero when resolution succeeded or the periodic
	// requeue is disabled.
	RequeueAfter time.Duration
}

// resolveComponentAutoscaling runs the policy layer + the shared resolution
// chain for an IR-managed component. The error return is transient only; a
// policy hold comes back as Hold=true with a Warning event already emitted —
// the reconcile must keep succeeding (degraded, not an error), or one missing
// shared policy object would stall every other concern on every consumer.
func resolveComponentAutoscaling(ctx context.Context, b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, componentExt *v1beta1.ComponentExtensionSpec) (ComponentAutoscaling, error) {
	bounds := autoscaler.EffectiveComponentBounds(componentExt)
	outcome, err := b.PolicyResolver.Resolve(ctx, isvc, componentType, bounds)
	if err != nil {
		return ComponentAutoscaling{}, err
	}
	resolved, source, hold := autoscaler.ResolveComponentAutoscalerWithPolicy(b.Runtime, isvc, componentType, outcome)
	res := ComponentAutoscaling{Outcome: outcome, Resolved: resolved, Source: source, Hold: hold}
	if hold {
		emitHoldEvent(b, isvc, componentType, outcome)
		res.RequeueAfter = policyHoldRequeueAfter(b)
	}
	return res, nil
}

// emitHoldEvent raises the AutoscalerPolicyHold warning only on transition:
// when the component's status already reports AutoscalerResolved=False with
// the same reason, the operator has been told and re-firing every reconcile
// would drown the event stream.
func emitHoldEvent(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, outcome *autoscaler.PolicyOutcome) {
	if b.Recorder == nil || holdAlreadyReported(isvc, componentType, outcome.HoldReason) {
		return
	}
	b.Recorder.Eventf(isvc, corev1.EventTypeWarning, "AutoscalerPolicyHold",
		"%s autoscaler is holding last-known-good (%s): %s", componentType, outcome.HoldReason, outcome.HoldDetail)
}

// holdAlreadyReported reports whether the component's status already carries
// AutoscalerResolved=False with the given reason — i.e. an earlier pass
// observed this same hold and emitted its event.
func holdAlreadyReported(isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, reason string) bool {
	if isvc == nil {
		return false
	}
	comp, ok := isvc.Status.Components[componentType]
	if !ok || comp.Autoscaler == nil {
		return false
	}
	cond := apimeta.FindStatusCondition(comp.Autoscaler.Conditions, v1beta1.AutoscalerResolvedCondition)
	return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == reason
}

// policyHoldRequeueAfter dereferences the resolver's hold requeue interval;
// zero (no periodic requeue) when no resolver is wired.
func policyHoldRequeueAfter(b *BaseComponentFields) time.Duration {
	if b.PolicyResolver == nil {
		return 0
	}
	return b.PolicyResolver.HoldRequeueAfter
}

// mergeRequeueAfter folds a hold-driven periodic requeue into a deployment
// result, keeping the sooner of two nonzero RequeueAfters so a rollout poll
// is never delayed by the (typically longer) config-TTL requeue.
func mergeRequeueAfter(result ctrl.Result, after time.Duration) ctrl.Result {
	if after <= 0 {
		return result
	}
	if result.RequeueAfter <= 0 || after < result.RequeueAfter {
		result.RequeueAfter = after
	}
	return result
}

// dispatchIRAutoscaler is the post-projection dispatch step shared by
// engine / decoder / router. On a hold it re-dispatches the IR's stored
// last-known-good block — bounds keep flowing to the live scaler while the
// trigger content stays frozen — or dispatches nothing at all when no record
// exists: a policy failure never tears a scaler down and never substitutes a
// default HPA.
func dispatchIRAutoscaler(ctx context.Context, b *BaseComponentFields, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, componentExt *v1beta1.ComponentExtensionSpec, res ComponentAutoscaling) error {
	dispatchBlock := res.Resolved
	if res.Hold {
		if ir.Spec.Autoscaler == nil {
			return nil
		}
		dispatchBlock = ir.Spec.Autoscaler.DeepCopy()
	} else if res.Source == autoscaler.SpecSourcePolicy {
		if err := b.PolicyResolver.EnsureTriggerAuthentications(ctx, isvc.Namespace, res.Resolved); err != nil {
			return err
		}
	}
	return autoscaler.DispatchForIRComponent(ctx, autoscaler.IRDispatchInput{
		Client:             b.Client,
		Scheme:             b.Scheme,
		IR:                 ir,
		ResolvedAutoscaler: dispatchBlock,
		ComponentExt:       componentExt,
	})
}

// resolveRawComponentAutoscaling is the RawDeployment counterpart: policy
// layer + shared chain + the legacy annotation branch, with the hold event
// and TriggerAuthentication materialization handled in place. The returned
// RawResolved feeds ReconcileRawDeployment, whose dispatch owns the
// last-known-good annotation on the Deployment.
func resolveRawComponentAutoscaling(ctx context.Context, b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, componentExt *v1beta1.ComponentExtensionSpec, annotations map[string]string) (autoscaler.RawResolved, error) {
	// Raw-specific bounds: a declared min of 0 stays 0 (legal with typed
	// KEDA on Raw dispatch), so rendered {{ .MinReplicas }} literals match
	// the bounds rawMinMaxReplicas will stamp on the scaler.
	bounds := autoscaler.RawEffectiveComponentBounds(componentExt)
	outcome, err := b.PolicyResolver.Resolve(ctx, isvc, componentType, bounds)
	if err != nil {
		return autoscaler.RawResolved{}, err
	}
	resolved, source, hold, err := autoscaler.ResolveRawComponentAutoscalerWithPolicy(b.Runtime, isvc, componentType, annotations, outcome)
	if err != nil {
		return autoscaler.RawResolved{}, err
	}
	res := autoscaler.RawResolved{
		Autoscaler: resolved,
		FromPolicy: source == autoscaler.SpecSourcePolicy && !hold,
		Hold:       hold,
	}
	if hold {
		emitHoldEvent(b, isvc, componentType, outcome)
		res.RequeueAfter = policyHoldRequeueAfter(b)
	} else if source == autoscaler.SpecSourcePolicy {
		if err := b.PolicyResolver.EnsureTriggerAuthentications(ctx, isvc.Namespace, resolved); err != nil {
			return autoscaler.RawResolved{}, err
		}
	}
	return res, nil
}

// ReconcileFineTunedWeights reconciles fine-tuned weights for any component
func ReconcileFineTunedWeights(b *BaseComponentFields, isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Model == nil {
		return nil
	}
	numOfFineTunedWeights := len(isvc.Spec.Model.FineTunedWeights)
	if numOfFineTunedWeights == 0 {
		return nil
	}

	b.Log.Info("FT serving mode", "Number of fine-tuned weights", numOfFineTunedWeights)
	b.FineTunedServing = true

	// TODO: lift here when start supporting stacked FT serving
	if numOfFineTunedWeights > 1 {
		return fmt.Errorf("stacked fine-tuned serving is not supported yet")
	}

	allFineTunedWeights := make([]*v1beta1.FineTunedWeight, 0)

	for _, fineTunedWeightName := range isvc.Spec.Model.FineTunedWeights {
		fineTunedWeight, err := isvcutils.GetFineTunedWeight(b.Client, fineTunedWeightName)
		if err != nil {
			return err
		}
		allFineTunedWeights = append(allFineTunedWeights, fineTunedWeight)
	}

	// Determine if loading merged fine-tuned weights
	loadingMergedFineTunedWeights, err := isvcutils.LoadingMergedFineTunedWeight(allFineTunedWeights)
	if err != nil {
		b.Log.Error(err, "Failed to determine if loading merged fine-tuned weights")
		return err
	}
	b.FineTunedServingWithMergedWeights = loadingMergedFineTunedWeights
	b.FineTunedWeights = allFineTunedWeights

	return nil
}

// UpdateVolumeMounts updates volume mounts for the container
func UpdateVolumeMounts(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container, objectMeta *metav1.ObjectMeta) {
	if container == nil {
		b.Log.Error(errors.New("container is nil"), "UpdateVolumeMounts: container is nil")
		return
	}

	// Add model volume mount if base model is specified and it's necessary
	if b.BaseModel != nil && !isShardedModel(b.BaseModel) && b.BaseModel.Storage != nil && b.BaseModelMeta != nil {
		if isvcutils.IsOriginalModelVolumeMountNecessary(objectMeta.Annotations) {
			if pvc := parsePVCComponents(b); pvc != nil {
				vm := corev1.VolumeMount{
					Name:      b.BaseModelMeta.Name,
					MountPath: constants.ModelDefaultMountPath,
					SubPath:   pvc.SubPath,
					ReadOnly:  true,
				}
				isvcutils.AppendVolumeMount(container, &vm)
			} else if b.BaseModel.Storage.Path != nil {
				vm := corev1.VolumeMount{
					Name:      b.BaseModelMeta.Name,
					MountPath: *b.BaseModel.Storage.Path,
					ReadOnly:  true,
				}
				isvcutils.AppendVolumeMount(container, &vm)
			}
		}
	}

	AppendOverlayVolumeMounts(b, b.Overlays, container)

	// Add fine-tuned serving volume mounts
	if b.FineTunedServing {
		defaultModelVolumeMount := corev1.VolumeMount{
			Name:      constants.ModelEmptyDirVolumeName,
			MountPath: constants.ModelDefaultMountPath,
		}
		isvcutils.AppendVolumeMountIfNotExist(container, &defaultModelVolumeMount)

		if isvcutils.IsCohereCommand1TFewFTServing(objectMeta) {
			// Update to have `base` sub-path in model volume mount for cohere tfew stacked serving case
			defaultModelVolumeMountWithSubPath := corev1.VolumeMount{
				Name:      constants.ModelEmptyDirVolumeName,
				MountPath: filepath.Join(constants.ModelDefaultMountPath, objectMeta.Annotations[constants.BaseModelFormat]),
				SubPath:   constants.BaseModelVolumeMountSubPath,
			}
			isvcutils.UpdateVolumeMount(container, &defaultModelVolumeMountWithSubPath)

			tfewFineTunedWeightVolumeMount := corev1.VolumeMount{
				Name:      constants.ModelEmptyDirVolumeName,
				MountPath: filepath.Join(constants.CohereTFewFineTunedWeightVolumeMountPath, objectMeta.Annotations[constants.BaseModelFormat]),
				ReadOnly:  true,
				SubPath:   constants.FineTunedWeightVolumeMountSubPath,
			}
			isvcutils.AppendVolumeMount(container, &tfewFineTunedWeightVolumeMount)
		}
	}
}

// UpdateEnvVariables updates environment variables for the container
func UpdateEnvVariables(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container, objectMeta *metav1.ObjectMeta) {
	if container == nil {
		b.Log.Error(errors.New("container is nil"), "UpdateEnvVariables: container is nil")
		return
	}

	if !b.FineTunedServing {
		// Base model serving - add MODEL_PATH env variable if necessary
		if modelPath, ok := modelPathEnvValue(b, objectMeta); ok {
			b.Log.V(1).Info("Base model serving - adding MODEL_PATH env variable if not provided", "inference service", isvc.Name, "namespace", isvc.Namespace)
			isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
				{Name: constants.ModelPathEnvVarKey, Value: modelPath},
			})
		}
		AppendOverlayEnvVars(b, b.Overlays, container)
	} else {
		// Fine-tuned serving - add vendor-specific environment variables
		if b.BaseModel != nil && b.BaseModel.Vendor != nil {
			if *b.BaseModel.Vendor == string(constants.Meta) {
				// Llama/Meta vendor specific env vars
				isvcutils.UpdateEnvVars(container, &corev1.EnvVar{
					Name: constants.ServedModelNameEnvVarKey,
					Value: filepath.Join(
						constants.LLamaVllmFTServingServedModelNamePrefix,
						objectMeta.Annotations[constants.FineTunedAdapterInjectionKey],
					),
				})
				isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
					{Name: constants.ModelPathEnvVarKey, Value: constants.ModelDefaultMountPath},
				})
			} else if *b.BaseModel.Vendor == string(constants.Cohere) {
				// Cohere vendor specific env vars
				if isvcutils.IsCohereCommand1TFewFTServing(objectMeta) {
					isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
						{Name: constants.TFewWeightPathEnvVarKey, Value: constants.CohereTFewFineTunedWeightDefaultPath},
					})
				}
			}
		} else {
			b.Log.Info("Warning: no vendor given in base model spec - no env var added/updated")
		}
	}

	// append env var from runtime spec if it is specified.
	// runner container is user values, it takes precedence over runtime values.
	// if the env exists, update its value.
	// if the env does not exist, append it to the list.
	if b.SupportedModelFormat != nil && b.SupportedModelFormat.AcceleratorConfig != nil && b.AcceleratorClassName != "" {
		acceleratorConfig := b.SupportedModelFormat.GetAcceleratorConfig(b.AcceleratorClassName)
		if acceleratorConfig != nil {
			envOverride := acceleratorConfig.EnvironmentOverride
			for envName, envVar := range envOverride {
				isvcutils.UpdateEnvVars(container, &corev1.EnvVar{
					Name: envName, Value: envVar})
			}
		}
	}
}

// UpdatePodSpecNodeSelector updates pod spec node selectors for scheduling.
func UpdatePodSpecNodeSelector(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, componentType v1beta1.ComponentType) {
	if b.BaseModel == nil || b.BaseModelMeta == nil {
		applyMergedNodeSelector(b.Runtime, b.AcceleratorClass, isvc, podSpec, componentType)
		return
	}

	// Skip node selector for fine-tuned serving with merged weights
	// as they don't need the base model on the node
	if b.FineTunedServingWithMergedWeights {
		b.Log.V(2).Info("Skipping node selector for fine-tuned serving with merged weights",
			"inferenceService", isvc.Name, "namespace", isvc.Namespace)
		return
	}

	// Skip node selector for PVC-backed models. The model agent does not
	// label nodes for PVC storage (PVCs aren't tied to specific nodes), so
	// the K8s scheduler handles placement based on PVC accessibility.
	if isPVCBaseModel(b) {
		b.Log.V(2).Info("Skipping model node selector for PVC-backed BaseModel; runtime/AcceleratorClass selectors still apply",
			"inferenceService", isvc.Name, "namespace", isvc.Namespace)
		applyMergedNodeSelector(b.Runtime, b.AcceleratorClass, isvc, podSpec, componentType)
		return
	}

	// Add preferred node affinity for model readiness using the shared utility function
	if isShardedModel(b.BaseModel) {
		b.Log.V(2).Info("Skipping per-node model readiness selector for sharded model",
			"modelName", b.BaseModelMeta.Name,
			"namespace", b.BaseModelMeta.Namespace,
			"inferenceService", isvc.Name)
	} else {
		isvcutils.AddNodeSelectorForModelReadyNode(podSpec, b.BaseModelMeta)
	}

	applyMergedNodeSelector(b.Runtime, b.AcceleratorClass, isvc, podSpec, componentType)

	if !isShardedModel(b.BaseModel) {
		b.Log.V(1).Info("Added preferred node affinity for model scheduling",
			"modelName", b.BaseModelMeta.Name,
			"namespace", b.BaseModelMeta.Namespace,
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

// UpdatePodSpecVolumes updates pod spec with common volumes
func UpdatePodSpecVolumes(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, objectMeta *metav1.ObjectMeta) {
	// Add model volume if base model is specified.
	if b.BaseModel != nil && !isShardedModel(b.BaseModel) && b.BaseModel.Storage != nil && b.BaseModelMeta != nil {
		if pvc := parsePVCComponents(b); pvc != nil {
			modelVolume := corev1.Volume{
				Name: b.BaseModelMeta.Name,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc.PVCName,
						ReadOnly:  true,
					},
				},
			}
			podSpec.Volumes = append(podSpec.Volumes, modelVolume)
		} else if b.BaseModel.Storage.Path != nil {
			modelVolume := corev1.Volume{
				Name: b.BaseModelMeta.Name,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: *b.BaseModel.Storage.Path,
					},
				},
			}
			podSpec.Volumes = append(podSpec.Volumes, modelVolume)
		}
	}

	AppendOverlayVolumes(b, b.Overlays, podSpec)

	// Add empty model directory volume if required for fine-tuned serving
	if isvcutils.IsEmptyModelDirVolumeRequired(objectMeta.Annotations) {
		emptyModelDirVolume := corev1.Volume{
			Name: constants.ModelEmptyDirVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				},
			},
		}
		podSpec.Volumes = utils.AppendVolumeIfNotExists(podSpec.Volumes, emptyModelDirVolume)
	}
}

func isShardedModel(model *v1beta1.BaseModelSpec) bool {
	return model != nil && model.Distribution != nil && *model.Distribution == v1beta1.DistributionSharded
}

func modelPathEnvValue(b *BaseComponentFields, objectMeta *metav1.ObjectMeta) (string, bool) {
	if b == nil || b.BaseModel == nil || b.BaseModel.Storage == nil {
		return "", false
	}
	if isShardedModel(b.BaseModel) {
		if b.BaseModel.Storage.StorageUri == nil || *b.BaseModel.Storage.StorageUri == "" {
			return "", false
		}
		return *b.BaseModel.Storage.StorageUri, true
	}
	if objectMeta == nil || !isvcutils.IsOriginalModelVolumeMountNecessary(objectMeta.Annotations) {
		return "", false
	}
	if isPVCBaseModel(b) {
		return constants.ModelDefaultMountPath, true
	}
	if b.BaseModel.Storage.Path == nil || *b.BaseModel.Storage.Path == "" {
		return "", false
	}
	return *b.BaseModel.Storage.Path, true
}

func MergeRuntimeArgumentsOverride(b *BaseComponentFields, container *corev1.Container) {
	// append arg var from runtime spec if it is specified
	if b.SupportedModelFormat != nil && b.SupportedModelFormat.AcceleratorConfig != nil && b.AcceleratorClassName != "" {
		acceleratorModelConfig := b.SupportedModelFormat.GetAcceleratorConfig(b.AcceleratorClassName)
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
func MergeResources(b *BaseComponentFields, container *corev1.Container) {
	isvcutils.MergeResource(container, b.AcceleratorClass, b.Runtime)
}

// MergeEngineResources merges resource requests and limits for the engine container.
// It only merges resources from the runtime and accelerator class when the user has not
// explicitly specified resources in the InferenceService spec. This ensures user-specified
// resources are respected and not overridden, while providing sensible defaults from the
// runtime and accelerator class when resources are not specified.
func MergeEngineResources(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container) {
	if isvc.Spec.Engine != nil &&
		(isvc.Spec.Engine.Runner == nil ||
			isResourcesUnspecified(isvc.Spec.Engine.Runner.Container.Resources)) {
		b.Log.V(1).Info("Merging resources for engine container as user did not specify resources in InferenceService")
		MergeResources(b, container)
	}
}

// MergeDecoderResources merges resource requests and limits for the decoder container.
// It only merges resources from the runtime and accelerator class when the user has not
// explicitly specified resources in the InferenceService spec. This ensures user-specified
// resources are respected and not overridden, while providing sensible defaults from the
// runtime and accelerator class when resources are not specified.
func MergeDecoderResources(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container) {
	if isvc.Spec.Decoder != nil &&
		(isvc.Spec.Decoder.Runner == nil ||
			isResourcesUnspecified(isvc.Spec.Decoder.Runner.Container.Resources)) {
		b.Log.V(1).Info("Merging resources for decoder container as user did not specify resources in InferenceService")
		MergeResources(b, container)
	}
}

// UpdateEngineAffinity applies the accelerator class's discovery affinity to the
// engine pod spec when the user did not specify affinity in the InferenceService.
func UpdateEngineAffinity(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec) {
	if isvc.Spec.Engine != nil && isvc.Spec.Engine.PodSpec.Affinity == nil {
		mergeAcceleratorAffinity(b, podSpec)
	}
}

// UpdateDecoderAffinity applies the accelerator class's discovery affinity to the
// decoder pod spec when the user did not specify affinity in the InferenceService.
func UpdateDecoderAffinity(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec) {
	if isvc.Spec.Decoder != nil && isvc.Spec.Decoder.PodSpec.Affinity == nil {
		mergeAcceleratorAffinity(b, podSpec)
	}
}

// acceleratorProvidesParallelismOverride reports whether the selected
// accelerator class supplies a TensorParallelismOverride for the matched
// model format. Only then does the accelerator config own the parallelism
// flags; otherwise the automatic PARALLELISM_SIZE computation still applies.
func acceleratorProvidesParallelismOverride(b *BaseComponentFields) bool {
	if b.AcceleratorClassName == "" || b.SupportedModelFormat == nil {
		return false
	}
	acceleratorConfig := b.SupportedModelFormat.GetAcceleratorConfig(b.AcceleratorClassName)
	return acceleratorConfig != nil && acceleratorConfig.TensorParallelismOverride != nil
}

// mergeAcceleratorAffinity fills the pod spec's NodeAffinity from the
// accelerator class's discovery affinity when the pod spec has none.
// Only NodeAffinity is taken: Discovery describes which NODES carry the
// hardware, and copying class-level pod (anti-)affinity terms could
// suppress the gang co-location terms OMENative injects per Instance
// (a pre-existing required podAffinity on the gang topology key skips
// the worker-follows-leader injection).
func mergeAcceleratorAffinity(b *BaseComponentFields, podSpec *corev1.PodSpec) {
	if b.AcceleratorClass == nil || b.AcceleratorClass.Discovery.Affinity == nil {
		return
	}
	acAffinity := b.AcceleratorClass.Discovery.Affinity
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

// ProcessBaseAnnotations processes common annotations
func ProcessBaseAnnotations(b *BaseComponentFields, isvc *v1beta1.InferenceService, annotations map[string]string) (map[string]string, error) {
	// Add fine-tuned weight annotations if applicable
	if b.FineTunedServing && len(b.FineTunedWeights) > 0 {
		// Inject ft adapter for single/non-stacked fine-tuned weight downloading
		annotations[constants.FineTunedAdapterInjectionKey] = b.FineTunedWeights[0].Name

		// Add fine-tuned weight ft strategy
		fineTunedWeightFTStrategy, err := isvcutils.GetValueFromRawExtension(b.FineTunedWeights[0].Spec.HyperParameters, constants.StrategyConfigKey)
		if err != nil {
			b.Log.Error(err, "Error getting hyper-parameter strategy from FineTunedWeight", "FineTunedWeight", b.FineTunedWeights[0].Name, "namespace", isvc.Namespace)
			return nil, err
		}
		if fineTunedWeightFTStrategy == nil {
			return nil, fmt.Errorf("hyper-parameter %q not set on FineTunedWeight %s", constants.StrategyConfigKey, b.FineTunedWeights[0].Name)
		}
		strategy, ok := fineTunedWeightFTStrategy.(string)
		if !ok {
			return nil, fmt.Errorf("hyper-parameter %q on FineTunedWeight %s must be a string, got %T", constants.StrategyConfigKey, b.FineTunedWeights[0].Name, fineTunedWeightFTStrategy)
		}
		annotations[constants.FineTunedWeightFTStrategyKey] = strategy
	}

	if b.FineTunedServingWithMergedWeights {
		b.Log.V(1).Info("Fine-tuned serving with merged weights", "namespace", isvc.Namespace)
		annotations[constants.FTServingWithMergedWeightsAnnotationKey] = "true"
	}

	// Add base model specific annotations
	if b.BaseModel != nil && b.BaseModelMeta != nil {
		annotations[constants.BaseModelName] = b.BaseModelMeta.Name
		if b.BaseModel.Vendor != nil {
			annotations[constants.BaseModelVendorAnnotationKey] = *b.BaseModel.Vendor
		}
		annotations[constants.BaseModelFormat] = b.BaseModel.ModelFormat.Name
		if b.BaseModel.ModelFormat.Version != nil {
			annotations[constants.BaseModelFormatVersion] = *b.BaseModel.ModelFormat.Version
		}
	}

	if b.RuntimeName != "" {
		annotations[constants.ServingRuntimeKeyName] = b.RuntimeName
	}

	return annotations, nil
}

// ProcessBaseLabels processes common labels
func ProcessBaseLabels(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, labels map[string]string) (map[string]string, error) {
	baseModelCategory := "SMALL"
	if b.BaseModelMeta != nil {
		if category, ok := b.BaseModelMeta.Annotations[constants.ModelCategoryAnnotation]; ok {
			baseModelCategory = category
		}
	}

	baseLabels := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(componentType),
		constants.ServingRuntimeLabelKey:      b.RuntimeName,
		constants.FTServingLabelKey:           strconv.FormatBool(b.FineTunedServing),
	}

	// Merge with provided labels
	if labels == nil {
		labels = make(map[string]string)
	}
	for k, v := range baseLabels {
		labels[k] = v
	}

	if b.BaseModelMeta != nil {
		labels[constants.InferenceServiceBaseModelNameLabelKey] = b.BaseModelMeta.Name
		labels[constants.InferenceServiceBaseModelSizeLabelKey] = baseModelCategory
		labels[constants.BaseModelTypeLabelKey] = string(constants.ServingBaseModel)
	}

	if b.BaseModel != nil && b.BaseModel.Vendor != nil {
		labels[constants.BaseModelVendorLabelKey] = *b.BaseModel.Vendor
	}

	// Add fine-tuned serving related labels
	if b.FineTunedServing && len(b.FineTunedWeights) > 0 {
		ftStrategyParameter, err := isvcutils.GetValueFromRawExtension(b.FineTunedWeights[0].Spec.HyperParameters, constants.StrategyConfigKey)
		if err != nil {
			b.Log.Error(err, "Error getting hyper-parameter strategy from FineTunedWeight", "FineTunedWeight", b.FineTunedWeights[0].Name, "namespace", isvc.Namespace)
			return nil, err
		}

		fineTunedWeightFTStrategy := ""
		if ftStrategyParameter != nil {
			s, ok := ftStrategyParameter.(string)
			if !ok {
				return nil, fmt.Errorf("hyper-parameter %q on FineTunedWeight %s must be a string, got %T", constants.StrategyConfigKey, b.FineTunedWeights[0].Name, ftStrategyParameter)
			}
			fineTunedWeightFTStrategy = s
		}
		labels[constants.FineTunedWeightFTStrategyLabelKey] = fineTunedWeightFTStrategy

		labels[constants.FTServingWithMergedWeightsLabelKey] = strconv.FormatBool(b.FineTunedServingWithMergedWeights)
	}

	return labels, nil
}

// UpdateComponentStatus updates component status based on deployment mode.
// Every deployment mode (RawDeployment, MultiNode, OMENative) emits pods
// carrying the raw-deployment app label, so one constant label pair serves
// the engine, decoder and router alike.
func UpdateComponentStatus(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, objectMeta metav1.ObjectMeta, componentExt *v1beta1.ComponentExtensionSpec) error {
	// Always initialize the component ready condition to ensure it's visible from the start
	// The deployment reconciler will update the condition based on the actual deployment status:
	// - MultiNode: Updates when LWS becomes available
	// - RawDeployment: Updates when Deployment becomes available
	b.StatusManager.InitializeComponentCondition(&isvc.Status, componentType)

	// Mirror the resolved per-Component autoscaler block + canonical scale
	// target onto status.components.<c>.{autoscaler, scaleTargetRef}.
	// Runs before the lean-path return so operators always see the
	// resolved class / managed-by / scale target — even on model-less
	// ISVCs that exit early below.
	//
	// The live-mirror branches in writeComponentAutoscalerStatus degrade
	// gracefully on NotFound (Component without a scaler yet), keeping
	// ManagedBy correct for default class=hpa ISVCs and "none" for the
	// AutoscalerNone / unknown branches.
	if err := writeComponentAutoscalerStatus(b, isvc, componentType, objectMeta, componentExt); err != nil {
		return errors.Wrapf(err, "failed to write %s autoscaler status", componentType)
	}

	// Lean path: with no spec.model there is no model-loading lifecycle to
	// surface — skip the modelStatus writer so we do not stamp a
	// transitionStatus/modelRevisionStates block that will never reach
	// "UpToDate"/"Loaded". The webhook permits omitting spec.model when
	// spec.runtime is set explicitly; we honor that here by leaving
	// status.modelStatus untouched.
	if isvc.Spec.Model == nil {
		return nil
	}

	// Update model status for all deployment modes based on actual pod information
	rawDeployment := b.DeploymentMode == constants.RawDeployment
	statusSpec := isvc.Status.Components[componentType]
	podLabelKey := constants.RawDeploymentAppLabel
	podLabelValue := constants.GetRawServiceLabel(objectMeta.Name)

	pods, err := isvcutils.ListPodsByLabel(b.Client, isvc.ObjectMeta.Namespace, podLabelKey, podLabelValue)
	if err != nil {
		return errors.Wrapf(err, "failed to list %s pods by label", componentType)
	}
	b.StatusManager.PropagateModelStatus(&isvc.Status, statusSpec, pods, rawDeployment)

	return nil
}

// writeComponentAutoscalerStatus resolves the per-Component autoscaler and
// stamps the resulting ComponentAutoscalerStatus + ScaleTargetRef onto
// status.components.<c>.
// Existing fields on the ComponentStatusSpec entry are preserved — only the
// .autoscaler and .scaleTargetRef sub-fields are overwritten.
//
// See pkg/.../reconcilers/autoscaler/status.go for the underlying mapping
// + live-mirror semantics.
func writeComponentAutoscalerStatus(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, objectMeta metav1.ObjectMeta, componentExt *v1beta1.ComponentExtensionSpec) error {
	var (
		resolved *v1beta1.ComponentAutoscaler
		source   autoscaler.SpecSource
		hold     bool
		outcome  *autoscaler.PolicyOutcome
		err      error
	)

	// The status writer re-resolves the full chain — policy layer included —
	// independently of dispatch, so status and dispatch cannot disagree
	// about which layer won. MultiNode has no autoscaler dispatch at all, so
	// a ref there is inert and only surfaces as a condition below.
	ref := autoscaler.ComponentPolicyRef(isvc, componentType)
	policySupported := b.DeploymentMode != constants.MultiNode
	if ref != nil && policySupported {
		// Same bounds arithmetic as the dispatch path per mode, so the
		// status-side resolved digest always matches the dispatched render.
		bounds := autoscaler.EffectiveComponentBounds(componentExt)
		if b.DeploymentMode == constants.RawDeployment {
			bounds = autoscaler.RawEffectiveComponentBounds(componentExt)
		}
		outcome, err = b.PolicyResolver.Resolve(context.Background(), isvc, componentType, bounds)
		if err != nil {
			return err
		}
	}

	if b.DeploymentMode == constants.RawDeployment {
		resolved, source, hold, err = autoscaler.ResolveRawComponentAutoscalerWithPolicy(b.Runtime, isvc, componentType, objectMeta.Annotations, outcome)
		if err != nil {
			return err
		}
	} else {
		resolved, source, hold = autoscaler.ResolveComponentAutoscalerWithPolicy(b.Runtime, isvc, componentType, outcome)
	}

	scaleTargetRef := canonicalScaleTargetRef(b.DeploymentMode, isvc.Name, objectMeta.Name, componentType)

	// For OMENative-managed Components the dispatch names the HPA /
	// ScaledObject after the InferenceReplica; for RawDeployment it uses
	// the legacy component metadata Name. The writer matches that lookup
	// pattern so the live mirror finds the right object.
	objectName := objectMeta.Name
	if irprojector.IsIRManagedComponent(b.DeploymentMode) {
		objectName = irprojector.InferenceReplicaName(isvc.Name, componentType)
	}

	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	existing := isvc.Status.Components[componentType]
	prev := existing.Autoscaler

	// On a hold there is no freshly resolved block, but the live scaler (the
	// last-known-good) is still running — mirror it through the previously
	// reported class so counters and scaler conditions stay truthful during
	// the freeze instead of collapsing to ManagedBy=none.
	statusResolved := resolved
	statusSource := source
	if hold {
		lastClass := v1beta1.AutoscalerNone
		if prev != nil && prev.Class != "" {
			lastClass = prev.Class
		}
		statusResolved = &v1beta1.ComponentAutoscaler{Class: lastClass}
		statusSource = autoscaler.SpecSourcePolicy
	}

	asStatus, stRef, err := autoscaler.WriteAutoscalerStatus(
		context.Background(),
		b.Client,
		isvc.Namespace,
		objectName,
		statusResolved,
		statusSource,
		scaleTargetRef,
		prev,
	)
	if err != nil {
		return err
	}

	applyPolicyProvenance(asStatus, prev, isvc, b.DeploymentMode, ref, outcome, source, hold)

	existing.Autoscaler = asStatus
	existing.ScaleTargetRef = stRef
	isvc.Status.Components[componentType] = existing
	return nil
}

// applyPolicyProvenance stamps the policy provenance fields and the
// AutoscalerResolved condition onto a freshly mirrored autoscaler status.
// Written only for components that carry a ref: policy-less components keep
// their status surface unchanged.
func applyPolicyProvenance(asStatus *v1beta1.ComponentAutoscalerStatus, prev *v1beta1.ComponentAutoscalerStatus, isvc *v1beta1.InferenceService, mode constants.DeploymentModeType, ref *v1beta1.AutoscalerPolicyRef, outcome *autoscaler.PolicyOutcome, source autoscaler.SpecSource, hold bool) {
	if ref == nil {
		return
	}

	condition := metav1.Condition{
		Type:               v1beta1.AutoscalerResolvedCondition,
		ObservedGeneration: isvc.Generation,
	}
	switch {
	case mode == constants.MultiNode:
		condition.Status = metav1.ConditionFalse
		condition.Reason = v1beta1.AutoscalerResolvedReasonUnsupportedMode
		condition.Message = "MultiNode components have no autoscaler dispatch; the policy ref is inert"
	case hold:
		condition.Status = metav1.ConditionFalse
		condition.Reason = outcome.HoldReason
		condition.Message = "holding last-known-good scaler: " + outcome.HoldDetail
		// Trigger content is frozen; the last successful provenance stays
		// visible so operators can see WHICH render is standing.
		if prev != nil {
			asStatus.Policy = prev.Policy.DeepCopy()
		}
	case source == autoscaler.SpecSourcePolicy:
		condition.Status = metav1.ConditionTrue
		condition.Reason = v1beta1.AutoscalerResolvedReasonRenderedFromPolicy
		asStatus.Policy = outcome.Provenance.DeepCopy()
	case source == autoscaler.SpecSourceISVC:
		// The inline block outranks the ref — deterministic resolution, so
		// the condition stays True; the shadow fields are the preview
		// surface (what the policy WOULD render if the inline block were
		// removed). A broken shadowed policy carries no digests.
		condition.Status = metav1.ConditionTrue
		condition.Reason = v1beta1.AutoscalerResolvedReasonInlinePrecedence
		condition.Message = "inline autoscaler block outranks the policy ref"
		shadow := &v1beta1.ShadowedAutoscalerPolicy{Name: ref.Name}
		if outcome != nil && outcome.Provenance != nil {
			shadow.PortableDigest = outcome.Provenance.PortableDigest
			shadow.WouldRenderDigest = outcome.Provenance.ResolvedDigest
		}
		asStatus.ShadowedPolicyRef = shadow
	default:
		// The policy layer sits directly below inline; with a ref present the
		// chain cannot land on runtime/legacy/default. Defensive only.
		condition.Status = metav1.ConditionUnknown
		condition.Reason = v1beta1.AutoscalerResolvedReasonPolicyInvalid
		condition.Message = "unexpected resolution source " + string(source)
	}

	// Seed the previous condition first so an unchanged status keeps its
	// LastTransitionTime — a fresh timestamp every pass would make the
	// mirrored status differ every reconcile and storm status updates.
	if prev != nil {
		if previous := apimeta.FindStatusCondition(prev.Conditions, v1beta1.AutoscalerResolvedCondition); previous != nil {
			apimeta.SetStatusCondition(&asStatus.Conditions, *previous)
		}
	}
	apimeta.SetStatusCondition(&asStatus.Conditions, condition)
}

// canonicalScaleTargetRef returns the scale target an external scaler should
// point at for the given Component. OMENative-managed (default + IR-projected)
// → InferenceReplica's /scale subresource; everything else → the underlying
// Deployment via the legacy component metadata Name.
//
// Empty values are returned when the deployment mode isn't recognized so the
// status writer can surface "no published target" cleanly (the writer drops
// an all-empty ScaleTargetRef rather than emitting an obviously-broken
// `{apiVersion:"",kind:"",name:""}` block).
func canonicalScaleTargetRef(mode constants.DeploymentModeType, isvcName, componentMetaName string, componentType v1beta1.ComponentType) v1beta1.ScaleTargetRef {
	if irprojector.IsIRManagedComponent(mode) {
		return v1beta1.ScaleTargetRef{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       "InferenceReplica",
			Name:       irprojector.InferenceReplicaName(isvcName, componentType),
		}
	}
	switch mode {
	case constants.RawDeployment:
		return v1beta1.ScaleTargetRef{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       componentMetaName,
		}
	default:
		return v1beta1.ScaleTargetRef{}
	}
}

// ReconcileOMENativeSubresources ensures the per-Component stable
// Service (`<isvc>-<comp>`) and PodMonitor for an OMENative-managed
// Component (engine / decoder / router). Engine, Decoder, and Router
// share this implementation byte-for-byte — only the component enum,
// the per-Component ComponentExtensionSpec, and (implicitly via
// objectMeta.Name) the resource name differ.
//
// The base selector is the OMENative-specific three-key tuple
// (InferenceServicePodLabel + OMEComponentLabel + ManagedBy=OMENative)
// so the stable Service + PodMonitor scope to OMENative-stamped pods
// only — a same-Component mode switch (engine OMENative -> engine
// RawDeployment) doesn't strand traffic on the wrong pod set during
// the transition. PodMonitor scrape port matches the Raw fallback
// rules: prefer a port named "metrics", else the first declared port,
// else "http".
//
// The stable Service adds leader filters only when the ISVC declares
// Worker.Size. Runtime-only shape is not used here because this
// Service spans revisions that may have different runner shapes.
//
// The multi-pod filter is runner=leader AND pod-ordinal=0: pod-ordinal
// is numbered per runner, so the rank-0 worker also carries ordinal 0
// and pod-ordinal alone would still admit a worker. runner=leader pins
// the one serving pod.
//
// Single-pod Components keep the broader selector: SurgeThenDrain
// alternates the pod-naming ordinal between 0 and 1 across surges
// (see query.LabelPodOrdinal docstring), so pinning ordinal=0
// would zero-endpoint the Service during the surge phase.
//
// PodMonitor intentionally uses the base selector (no pod-ordinal
// filter) so Prometheus scrapes EVERY pod of the gang — workers
// emit per-rank metrics too, and per-pod scraping is the standard
// Prometheus model.
//
// When podSpec is nil (MinReplicas=0 cold-start, no rendered template)
// both reconcilers no-op: the Service reconciler would emit a portless
// ClusterIP and the PodMonitor would have no scrape target. Both are
// restored on the next reconcile pass after scale-up.
//
// Inlined PodMonitor build (rather than via the shared podmonitor
// reconciler) because the shared reconciler hardcodes the
// `app=<name>` selector that matches Raw / MultiNode pods but NOT
// OMENative pods. CreateOrUpdate is idempotent + drift-correcting on
// the three labelSelector / NamespaceSelector / PodMetricsEndpoints
// shape fields.
func ReconcileOMENativeSubresources(
	ctx context.Context,
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentExt *v1beta1.ComponentExtensionSpec,
	objectMeta metav1.ObjectMeta,
	podSpec *corev1.PodSpec,
) error {
	if podSpec == nil {
		return nil
	}
	// Base selector — narrows to OMENative-managed pods of this (ISVC,
	// Component) pair. Used as-is for PodMonitor; augmented with a
	// `runner=leader` + `pod-ordinal=0` filter when the ISVC declares a
	// multi-pod shape (see function-level docstring for rationale).
	baseSelector := omeNativeComponentSelector(isvc, componentType)
	stableSelector := baseSelector
	if isvcutils.IsMultiPodComponent(isvc, componentType) {
		stableSelector = make(map[string]string, len(baseSelector)+2)
		for k, v := range baseSelector {
			stableSelector[k] = v
		}
		// Pod ordinals are numbered per runner, so worker ordinal 0 also carries
		// this value. Combining the runner and ordinal selects only the leader.
		stableSelector[query.LabelRunner] = string(v1beta1.RunnerNameLeader)
		stableSelector[query.LabelPodOrdinal] = "0"
	}
	componentMeta := objectMeta.DeepCopy()
	componentMeta.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
	}

	// Stable Service via the SAME top-level reconciler RawDeployment and
	// MultiNode use; only the selector + owner refs are OMENative-shaped.
	sr := service.NewServiceReconciler(b.Client, b.Scheme, *componentMeta, componentExt, podSpec, stableSelector)
	if _, err := sr.Reconcile(); err != nil {
		return errors.Wrap(err, "stable service")
	}

	// PodMonitor is optional: its scheme is registered only when the Prometheus
	// operator CRD is present (manager startup). On a cluster without it, skip
	// creation rather than failing the whole reconcile.
	if !b.Scheme.Recognizes(monitoringv1.SchemeGroupVersion.WithKind(constants.PodMonitorKind)) {
		return nil
	}

	// PodMonitor scrape port follows the Raw fallback rules: prefer a
	// port named "metrics", else the first declared port, else "http".
	portName := "http"
	if len(podSpec.Containers) > 0 {
		ports := podSpec.Containers[0].Ports
		for _, p := range ports {
			if p.Name == "metrics" {
				portName = "metrics"
				break
			}
		}
		if portName == "http" && len(ports) > 0 && ports[0].Name != "" {
			portName = ports[0].Name
		}
	}
	target := &monitoringv1.PodMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectMeta.Name,
			Namespace: objectMeta.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, b.Client, target, func() error {
		target.Spec.NamespaceSelector = monitoringv1.NamespaceSelector{
			MatchNames: []string{objectMeta.Namespace},
		}
		target.Spec.Selector = metav1.LabelSelector{MatchLabels: baseSelector}
		endpoints := []monitoringv1.PodMetricsEndpoint{{
			Port:     &portName,
			Path:     "/metrics",
			Interval: "10s",
		}}
		endpoints = append(endpoints, podmonitor.ParseExtraEndpoints(objectMeta.Annotations)...)
		target.Spec.PodMetricsEndpoints = endpoints
		// Own copy of baseSelector for metadata.labels: ApplyManagedScrapeConfig
		// merges cfg.Labels into it, and spec.selector.MatchLabels must NOT gain
		// those labels (pods don't carry them).
		target.Labels = maps.Clone(baseSelector)
		// Cluster-scope PodMonitor defaults (metadata labels + endpoint
		// relabelings) from the inferenceservice-config ConfigMap. Applied
		// AFTER target.Labels/endpoints are set: labels merge into
		// metadata.labels only (spec.selector keeps selecting pods by
		// baseSelector), relabelings append to every endpoint. Without this the
		// PodMonitor carries only OME's own labels and a label-selecting
		// collector (e.g. an external target allocator) never scrapes it.
		if b.InferenceServiceConfig != nil {
			podmonitor.ApplyManagedScrapeConfig(target, b.InferenceServiceConfig.PodMonitor)
		}
		target.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "pod monitor %s/%s", target.Namespace, target.Name)
	}
	return nil
}

// ReconcileComponentObjectMeta builds the common ObjectMeta block
// (Name, Namespace, Annotations, Labels) shared by engine / decoder /
// router. The per-Component name is resolved upstream because the
// fallback logic (Service-existence lookup, MultiNode gating) still
// differs across components (see section 4 of the components-dispatch
// review). The annotation / label maps are the per-Component merge
// (ISVC + componentExt.Annotations / componentExt.Labels) already
// performed by the caller.
//
// On annotation-build failure the returned ObjectMeta carries Name +
// Namespace only; on label-build failure it carries Name + Namespace +
// Annotations, so callers see exactly the metadata built before the
// failure.
func ReconcileComponentObjectMeta(
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentName string,
	componentAnnotations map[string]string,
	componentLabels map[string]string,
) (metav1.ObjectMeta, error) {
	annotations, err := ProcessComponentAnnotations(b, isvc, componentAnnotations)
	if err != nil {
		return metav1.ObjectMeta{
			Name:      componentName,
			Namespace: isvc.Namespace,
		}, err
	}

	labels, err := ProcessComponentLabels(b, isvc, componentType, componentLabels)
	if err != nil {
		return metav1.ObjectMeta{
			Name:        componentName,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
		}, err
	}

	return metav1.ObjectMeta{
		Name:        componentName,
		Namespace:   isvc.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

// ProcessComponentAnnotations performs the per-Component annotation
// build: filter the ISVC-level annotations against the disallowed
// list, union them with the Component-level annotations, then hand
// off to ProcessBaseAnnotations for the FT / BaseModel / runtime
// annotations the base layer adds.
func ProcessComponentAnnotations(
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentAnnotations map[string]string,
) (map[string]string, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	mergedAnnotations := annotations
	if componentAnnotations != nil {
		mergedAnnotations = utils.Union(annotations, componentAnnotations)
	}
	delete(mergedAnnotations, constants.InferenceServiceInPlaceImageTransitionAnnotationKey)

	return ProcessBaseAnnotations(b, isvc, mergedAnnotations)
}

// ProcessComponentLabels performs the per-Component label build:
// union the ISVC-level labels with the Component-level labels, then
// hand off to ProcessBaseLabels for the FT / BaseModel / runtime
// labels the base layer adds.
func ProcessComponentLabels(
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentLabels map[string]string,
) (map[string]string, error) {
	// Union always copies: ProcessBaseLabels mutates the map it is
	// handed, and aliasing isvc.Labels would leak one component's
	// stamps into the next component's build.
	mergedLabels := utils.Union(isvc.Labels, componentLabels)

	return ProcessBaseLabels(b, isvc, componentType, mergedLabels)
}
