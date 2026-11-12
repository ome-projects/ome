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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
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
	Client client.Client
	// APIReader is the live (uncached) reader, typically mgr.GetAPIReader(),
	// for reads where cache lag would be a correctness problem: the
	// placement demand fingerprint, the disruption budget and the paired
	// role's rows.
	APIReader client.Reader
	// Recorder is the parent controller's event recorder.
	Recorder      record.EventRecorder
	Scheme        *runtime.Scheme
	StatusManager *status.StatusReconciler

	// PolicyResolver renders per-component autoscalerPolicyRef attachments;
	// threaded from ComponentInputs (per reconcile). May be nil only in
	// tests; the shared helpers treat nil as feature-disabled and fail refs
	// closed.
	PolicyResolver *autoscaler.PolicyResolver

	// QuotaAcceleratorResources are the resource names that put a Component
	// under the quota backend, threaded from the InferenceServiceReconciler to
	// the InferenceReplica projector. Empty governs everything.
	QuotaAcceleratorResources []string

	// PacingPartition sizes the canary machine's rollout-control partition
	// for this Component, threaded from ComponentInputs to the
	// InferenceReplica projector. nil when no canary governs the Component.
	PacingPartition irprojector.PartitionResolver
	// RollbackToRevision is the stable revision this Component's canary unit
	// keeps a side on, threaded from ComponentInputs to the projector for the
	// replica it creates. Empty when the unit keeps no stable side.
	RollbackToRevision  string
	CoordinatorFollower bool
	CoordinatorTarget   *int32
	FollowerReleased    bool
}

// newBaseComponentFields is the ONE place BaseComponentFields is
// assembled from deps + inputs. Constructors pass their component's
// logger name; nothing else differs per component. The fine-tuned
// serving fields (FineTunedServing / FineTunedServingWithMergedWeights /
// FineTunedWeights) are populated at reconcile time by
// render.ReconcileFineTunedWeights, never at construction.
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
		APIReader:                 deps.APIReader,
		Recorder:                  deps.Recorder,
		QuotaAcceleratorResources: deps.QuotaAcceleratorResources,
		Scheme:                    deps.Scheme,
		StatusManager:             status.NewStatusReconciler(),
		PolicyResolver:            in.PolicyResolver,
		PacingPartition:           in.PacingPartition,
		RollbackToRevision:        in.RollbackToRevision,
		CoordinatorFollower:       in.CoordinatorFollower,
		CoordinatorTarget:         in.CoordinatorTarget,
		FollowerReleased:          in.FollowerReleased,
	}
}

// Common methods as functions that operate on BaseComponentFields

// ComponentAutoscaling is one component's autoscaler resolution for a single
// reconcile pass: the resolved autoscaler, where it came from, and the
// fail-closed hold flag.
type ComponentAutoscaling struct {
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
	resolved = coordinatorFollowerAutoscaler(b, resolved, source, hold)
	res := ComponentAutoscaling{Resolved: resolved, Source: source, Hold: hold}
	if hold {
		emitHoldEvent(b, isvc, componentType, outcome)
		res.RequeueAfter = policyHoldRequeueAfter(b)
	}
	return res, nil
}

// coordinatorFollowerAutoscaler gives a scaling-policy follower that declares
// no autoscaler of its own class None: the coordinator writes its desired
// count, so the default scaler would fight it. Dispatch and the status writer
// both apply it, so status reports the class that actually runs.
func coordinatorFollowerAutoscaler(b *BaseComponentFields, resolved *v1beta1.ComponentAutoscaler, source autoscaler.SpecSource, hold bool) *v1beta1.ComponentAutoscaler {
	if b.CoordinatorFollower && source == autoscaler.SpecSourceDefault && !hold {
		return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
	}
	return resolved
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
//
// A switch between an HPA and a KEDA ScaledObject that waits for KEDA, or
// for a deletion, is not a failure: the pass requeues through the rate
// limiter so the component status still gets written, and the next pass
// checks again.
func dispatchIRAutoscaler(ctx context.Context, b *BaseComponentFields, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, componentExt *v1beta1.ComponentExtensionSpec, res ComponentAutoscaling) (ctrl.Result, error) {
	dispatchBlock := res.Resolved
	if res.Hold {
		if ir.Spec.Autoscaler == nil {
			return ctrl.Result{}, nil
		}
		dispatchBlock = ir.Spec.Autoscaler.DeepCopy()
	} else if res.Source == autoscaler.SpecSourcePolicy {
		if err := b.PolicyResolver.EnsureTriggerAuthentications(ctx, isvc.Namespace, res.Resolved); err != nil {
			return ctrl.Result{}, err
		}
	}
	err := autoscaler.DispatchForIRComponent(ctx, autoscaler.IRDispatchInput{
		Client:             b.Client,
		Scheme:             b.Scheme,
		IR:                 ir,
		ResolvedAutoscaler: dispatchBlock,
		ComponentExt:       componentExt,
	})
	if autoscaler.IsTransitionPending(err) {
		b.Log.Info("Autoscaler switch in progress", "inferenceService", isvc.Name, "namespace", isvc.Namespace, "inferenceReplica", ir.Name, "reason", transitionPendingReason(err), "detail", err.Error())
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, err
}

// transitionPendingReason returns the AutoscalerSwitched reason carried by a
// pending autoscaler switch.
func transitionPendingReason(err error) string {
	var pending *autoscaler.TransitionPendingError
	if errors.As(err, &pending) {
		return pending.Reason
	}
	return ""
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

// ReconcileComponentObjectMeta builds the common ObjectMeta block (Name,
// Namespace, Annotations, Labels) shared by engine / decoder / router.
func ReconcileComponentObjectMeta(b *BaseComponentFields, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, componentName string, componentAnnotations map[string]string, componentLabels map[string]string) (metav1.ObjectMeta, error) {
	return render.ReconcileComponentObjectMeta(&b.Piece, isvc, componentType, componentName, componentAnnotations, componentLabels)
}

// UpdateComponentStatus writes a Component's status: its ready condition,
// the autoscaler mirror and, for a service with a model, the model-loading
// state read from the Component's pods.
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

	pods, err := modelStatusPods(b, isvc, objectMeta)
	if err != nil {
		return errors.Wrapf(err, "failed to list %s pods by label", componentType)
	}
	b.StatusManager.PropagateModelStatus(&isvc.Status, pods)
	return nil
}

// modelStatusPods lists the pods the model-loading state is read from: the
// Component's pods that carry the raw app label. Every RawDeployment pod and
// the MultiNode leader carry it. An OMENative pod carries the workload's
// ome.io labels and never the app label, so for that mode the list is not
// made: it would walk every pod of the namespace and return none, and the
// state is written as for no pod either way.
func modelStatusPods(b *BaseComponentFields, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) (*corev1.PodList, error) {
	if b.DeploymentMode == constants.OMENative {
		return &corev1.PodList{}, nil
	}
	return isvcutils.ListPodsByLabel(b.Client, isvc.Namespace, constants.RawDeploymentAppLabel, constants.GetRawServiceLabel(objectMeta.Name))
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
		resolved = coordinatorFollowerAutoscaler(b, resolved, source, hold)
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
// the transition. The PodMonitor's own scrape endpoint (path, interval
// and port names) comes from the podMonitor block of
// inferenceservice-config; a block without them writes only the
// endpoints the extra-endpoints annotation declares.
//
// multiPod is the effective shape of the Component: the runtime's
// leader/worker blocks with the ISVC's merged over them, the same
// decision that renders leader and worker runners. A shape the runtime
// alone supplies therefore narrows the stable Service exactly like one
// declared on the ISVC. Workers are distributed-compute peers that do
// not serve the customer endpoint, so a multi-pod Service selects only
// the leader at ordinal 0 (query.LeaderRoutingSelector): pod-ordinal is
// numbered per runner, so the rank-0 worker also carries ordinal 0 and
// the runner key is what keeps it out.
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
	multiPod bool,
) error {
	if podSpec == nil {
		return nil
	}
	return ReconcileStableSubresources(ctx, b, isvc, componentType, isvc.Name, multiPod, componentExt, objectMeta, podSpec)
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
	// The PodMonitor scrapes every pod of the role; the stable Service
	// narrows to each Instance's leader when the role has one.
	baseSelector := omeNativePodSelector(podPrefix, componentType)
	stableSelector := baseSelector
	if leaderOnly {
		stableSelector = query.LeaderRoutingSelector(baseSelector, true)
	}
	if err := reconcileStableService(b, isvc, componentExt, objectMeta, podSpec, stableSelector); err != nil {
		return err
	}
	return reconcileScrapeMonitor(ctx, b, isvc, objectMeta, podSpec, baseSelector)
}

// reconcileStableService writes the role's stable Service through the
// reconciler every deployment mode shares; only the selector and the owner
// reference are OMENative-shaped.
func reconcileStableService(
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentExt *v1beta1.ComponentExtensionSpec,
	objectMeta metav1.ObjectMeta,
	podSpec *corev1.PodSpec,
	selector map[string]string,
) error {
	componentMeta := objectMeta.DeepCopy()
	componentMeta.OwnerReferences = []metav1.OwnerReference{isvcControllerRef(isvc)}
	sr := service.NewServiceReconciler(b.Client, b.Scheme, *componentMeta, componentExt, podSpec, selector)
	if _, err := sr.Reconcile(); err != nil {
		return errors.Wrap(err, "stable service")
	}
	return nil
}

// reconcileScrapeMonitor writes the role's PodMonitor. The monitoring API is
// registered only when the Prometheus operator CRD is installed; without it
// the role runs unscraped instead of failing the reconcile.
func reconcileScrapeMonitor(
	ctx context.Context,
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	objectMeta metav1.ObjectMeta,
	podSpec *corev1.PodSpec,
	selector map[string]string,
) error {
	if !b.Scheme.Recognizes(monitoringv1.SchemeGroupVersion.WithKind(constants.PodMonitorKind)) {
		return nil
	}
	scrape := scrapeConfig(b)
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
		target.Spec.Selector = metav1.LabelSelector{MatchLabels: selector}
		target.Spec.PodMetricsEndpoints = podmonitor.ScrapeEndpoints(scrape, podSpec, objectMeta.Annotations)
		// metadata.labels gets its own copy: the cluster-scope scrape config
		// merges its labels into it, while spec.selector keeps selecting pods
		// by the bare selector (pods do not carry those labels).
		target.Labels = maps.Clone(selector)
		// The cluster-scope defaults (metadata labels and endpoint
		// relabelings) from the inferenceservice-config ConfigMap apply after
		// the labels and endpoints are set; without them a label-selecting
		// collector never scrapes the PodMonitor.
		podmonitor.ApplyManagedScrapeConfig(target, scrape)
		target.OwnerReferences = []metav1.OwnerReference{isvcControllerRef(isvc)}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "pod monitor %s/%s", target.Namespace, target.Name)
	}
	return nil
}

// scrapeConfig is the podMonitor block of the operator configuration, or
// an empty block when the reconciler runs without one.
func scrapeConfig(b *BaseComponentFields) controllerconfig.PodMonitorConfig {
	if b.InferenceServiceConfig == nil {
		return controllerconfig.PodMonitorConfig{}
	}
	return b.InferenceServiceConfig.PodMonitor
}

// isvcControllerRef is the owner reference that makes the InferenceService
// the controller of one of its subresources.
func isvcControllerRef(isvc *v1beta1.InferenceService) metav1.OwnerReference {
	return *metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
}
