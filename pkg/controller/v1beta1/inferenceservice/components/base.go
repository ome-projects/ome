package components

import (
	"context"
	"maps"
	"time"

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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/podmonitor"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/render"
)

// BaseComponentFields contains common fields for all components. The
// embedded render.Piece holds the rendering inputs (model, runtime,
// accelerator class, config, fine-tuned weights); the rest is the
// reconciliation wiring the pod renderers never read.
type BaseComponentFields struct {
	render.Piece

	// Client serves the reconcilers; Piece.Client is the reader the render
	// helpers use, and both are set from the same deps client.
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
	GangSchedulingAvailable bool
	Scheme                  *runtime.Scheme
	StatusManager           *status.StatusReconciler

	// PolicyResolver renders per-component autoscalerPolicyRef attachments;
	// threaded from ComponentInputs (per reconcile). May be nil only in
	// tests; the shared helpers treat nil as feature-disabled and fail refs
	// closed.
	PolicyResolver *autoscaler.PolicyResolver

	// QuotaAcceleratorResources are the resource names that put a Component
	// under the quota backend, threaded from the InferenceServiceReconciler to
	// the InferenceReplica projector. Empty governs everything.
	QuotaAcceleratorResources []string

	// PacingPartition is the canary machine's rollout-control partition for
	// this Component, threaded from ComponentInputs to the InferenceReplica
	// projector. nil when no canary governs the Component.
	PacingPartition *int32
}

// newBaseComponentFields is the ONE place BaseComponentFields is
// assembled from deps + inputs. Constructors pass their component's
// logger name; nothing else differs per component. The fine-tuned
// serving fields (FineTunedServing / FineTunedServingWithMergedWeights /
// FineTunedWeights) are populated at reconcile time by
// ReconcileFineTunedWeights, never at construction.
func newBaseComponentFields(deps *ComponentDeps, in ComponentInputs, loggerName string) BaseComponentFields {
	return BaseComponentFields{
		Piece: render.Piece{
			Client:                 deps.Client,
			Log:                    ctrl.Log.WithName(loggerName),
			InferenceServiceConfig: deps.Config,
			DeploymentMode:         in.DeploymentMode,
			BaseModel:              in.BaseModel,
			BaseModelMeta:          in.BaseModelMeta,
			Runtime:                in.Runtime,
			RuntimeName:            in.RuntimeName,
			SupportedModelFormat:   in.ModelFormat,
			AcceleratorClass:       in.AcceleratorClass,
			AcceleratorClassName:   in.AcceleratorClassName,
			Overlays:               in.Overlays,
		},
		Client:                    deps.Client,
		Clientset:                 deps.Clientset,
		APIReader:                 deps.APIReader,
		Expectations:              deps.Expectations,
		Recorder:                  deps.Recorder,
		GangSchedulingAvailable:   deps.GangSchedulingAvailable,
		QuotaAcceleratorResources: deps.QuotaAcceleratorResources,
		Scheme:                    deps.Scheme,
		StatusManager:             status.NewStatusReconciler(),
		PolicyResolver:            in.PolicyResolver,
		PacingPartition:           in.PacingPartition,
	}
}

// pieceOf returns b's rendering inputs, or nil when b itself is nil, so the
// overlay forwarders accept the nil bag the overlay helpers tolerate.
func pieceOf(b *BaseComponentFields) *render.Piece {
	if b == nil {
		return nil
	}
	return &b.Piece
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

// The rendering helpers operate on the embedded render.Piece. These
// forwarders let callers holding a *BaseComponentFields use them without
// naming the piece.

// ReconcileFineTunedWeights reconciles fine-tuned weights for any component.
func ReconcileFineTunedWeights(b *BaseComponentFields, isvc *v1beta1.InferenceService) error {
	return render.ReconcileFineTunedWeights(&b.Piece, isvc)
}

// UpdateVolumeMounts updates volume mounts for the container.
func UpdateVolumeMounts(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container, objectMeta *metav1.ObjectMeta) {
	render.UpdateVolumeMounts(&b.Piece, isvc, container, objectMeta)
}

// UpdateEnvVariables updates environment variables for the container.
func UpdateEnvVariables(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container, objectMeta *metav1.ObjectMeta) {
	render.UpdateEnvVariables(&b.Piece, isvc, container, objectMeta)
}

// UpdatePodSpecNodeSelector updates pod spec node selectors for scheduling.
func UpdatePodSpecNodeSelector(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, componentType v1beta1.ComponentType) {
	render.UpdatePodSpecNodeSelector(&b.Piece, isvc, podSpec, componentType)
}

// UpdatePodSpecVolumes updates pod spec with common volumes.
func UpdatePodSpecVolumes(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, objectMeta *metav1.ObjectMeta) {
	render.UpdatePodSpecVolumes(&b.Piece, isvc, podSpec, objectMeta)
}

// MergeRuntimeArgumentsOverride merges the accelerator class's runtime
// argument overrides into the container args.
func MergeRuntimeArgumentsOverride(b *BaseComponentFields, container *corev1.Container) {
	render.MergeRuntimeArgumentsOverride(&b.Piece, container)
}

// MergeResources merges resource requests and limits from the runtime and
// accelerator class into the container.
func MergeResources(b *BaseComponentFields, container *corev1.Container) {
	render.MergeResources(&b.Piece, container)
}

// MergeEngineResources merges runtime and accelerator class resources into
// the engine container when the service specifies none.
func MergeEngineResources(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container) {
	render.MergeEngineResources(&b.Piece, isvc, container)
}

// MergeDecoderResources merges runtime and accelerator class resources into
// the decoder container when the service specifies none.
func MergeDecoderResources(b *BaseComponentFields, isvc *v1beta1.InferenceService, container *corev1.Container) {
	render.MergeDecoderResources(&b.Piece, isvc, container)
}

// UpdateEngineAffinity applies the accelerator class's discovery affinity to
// the engine pod spec when the service specifies no affinity.
func UpdateEngineAffinity(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec) {
	render.UpdateEngineAffinity(&b.Piece, isvc, podSpec)
}

// UpdateDecoderAffinity applies the accelerator class's discovery affinity to
// the decoder pod spec when the service specifies no affinity.
func UpdateDecoderAffinity(b *BaseComponentFields, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec) {
	render.UpdateDecoderAffinity(&b.Piece, isvc, podSpec)
}

// ProcessBaseAnnotations processes common annotations.
func ProcessBaseAnnotations(b *BaseComponentFields, isvc *v1beta1.InferenceService, annotations map[string]string) (map[string]string, error) {
	return render.ProcessBaseAnnotations(&b.Piece, isvc, annotations)
}

// ProcessBaseLabels processes common labels.
func ProcessBaseLabels(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, labels map[string]string) (map[string]string, error) {
	return render.ProcessBaseLabels(&b.Piece, isvc, componentType, labels)
}

// ReconcileComponentObjectMeta builds the common ObjectMeta block (Name,
// Namespace, Annotations, Labels) shared by engine / decoder / router.
func ReconcileComponentObjectMeta(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, componentName string, componentAnnotations map[string]string, componentLabels map[string]string) (metav1.ObjectMeta, error) {
	return render.ReconcileComponentObjectMeta(&b.Piece, isvc, componentType, componentName, componentAnnotations, componentLabels)
}

// ProcessComponentAnnotations performs the per-Component annotation build.
func ProcessComponentAnnotations(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentAnnotations map[string]string) (map[string]string, error) {
	return render.ProcessComponentAnnotations(&b.Piece, isvc, componentAnnotations)
}

// ProcessComponentLabels performs the per-Component label build.
func ProcessComponentLabels(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, componentLabels map[string]string) (map[string]string, error) {
	return render.ProcessComponentLabels(&b.Piece, isvc, componentType, componentLabels)
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

	scaleTargetRef := canonicalScaleTargetRef(b.DeploymentMode, isvc, objectMeta.Name, componentType)

	// For OMENative-managed Components the dispatch names the HPA /
	// ScaledObject after the role's InferenceReplica; for RawDeployment it
	// uses the legacy component metadata Name. The writer matches that
	// lookup pattern so the live mirror finds the right object.
	objectName := objectMeta.Name
	if irprojector.IsIRManagedComponent(b.DeploymentMode) {
		objectName = irprojector.RoleReplicaName(isvc, componentType)
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
// point at for the given Component. OMENative-managed (default + IR-managed)
// → the /scale subresource of the InferenceReplica serving the role;
// everything else → the underlying Deployment via the legacy component
// metadata Name.
//
// Empty values are returned when the deployment mode isn't recognized so the
// status writer can surface "no published target" cleanly (the writer drops
// an all-empty ScaleTargetRef rather than emitting an obviously-broken
// `{apiVersion:"",kind:"",name:""}` block).
func canonicalScaleTargetRef(mode constants.DeploymentModeType, isvc *v1beta1.InferenceService, componentMetaName string, componentType v1beta1.ComponentType) v1beta1.ScaleTargetRef {
	if irprojector.IsIRManagedComponent(mode) {
		return v1beta1.ScaleTargetRef{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       "InferenceReplica",
			Name:       irprojector.RoleReplicaName(isvc, componentType),
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
	return ReconcileStableSubresources(ctx, b, isvc, componentType, isvc.Name, isvcutils.IsMultiPodComponent(isvc, componentType), componentExt, objectMeta, podSpec)
}

// ReconcileStableSubresources is ReconcileOMENativeSubresources with the pod
// set spelled out, for a role whose pods a replica of another name runs:
// podPrefix is the ome.io/inferenceservice label value of the role's pods
// (the service's name for a projected replica, the replica's own name for a
// referenced one) and leaderOnly says whether each Instance has a leader the
// stable Service selects.
func ReconcileStableSubresources(
	ctx context.Context,
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	podPrefix string,
	leaderOnly bool,
	componentExt *v1beta1.ComponentExtensionSpec,
	objectMeta metav1.ObjectMeta,
	podSpec *corev1.PodSpec,
) error {
	// Base selector — narrows to OMENative-managed pods of this (ISVC,
	// Component) pair. Used as-is for PodMonitor; augmented with a
	// `runner=leader` + `pod-ordinal=0` filter when the ISVC declares a
	// multi-pod shape (see function-level docstring for rationale).
	baseSelector := omeNativePodSelector(podPrefix, componentType)
	stableSelector := baseSelector
	if leaderOnly {
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
