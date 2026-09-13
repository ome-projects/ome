package isvc

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils/storage"
	"sigs.k8s.io/ome/pkg/validation"
)

// InferenceServiceValidator wires the webhook framework into the
// per-rule validators in pkg/validation. The kubebuilder markers below
// suppress DeepCopy + OpenAPI codegen since this is a stateless
// wired-once helper, not a persisted CR.
// +kubebuilder:object:generate=false
// +k8s:openapi-gen=false
type InferenceServiceValidator struct {
	Client          client.Client
	RuntimeSelector runtimeselector.Selector

	// KnownPassthroughPrefixes lists the ome.io/* pass-through
	// annotation prefixes the active Gateway-implementation translator
	// supports (e.g. ["ome.io/btp."] when Envoy Gateway is loaded).
	// When non-empty, the validator rejects pass-through annotations
	// whose prefix is not in the set with `UnsupportedPassthrough`,
	// surfacing the misconfiguration at admission instead of letting
	// the controller catch it at runtime via the
	// BackendPolicyUnsupportedFields condition.
	//
	// Empty (default) keeps the prior permissive behavior — all
	// documented pass-through prefixes are accepted. Production
	// (cmd/manager/main.go) populates this from the active translator's
	// SupportedPassthroughPrefixes(); test wiring may leave it empty
	// unless explicitly exercising the rejection path.
	KnownPassthroughPrefixes []string

	// SupportedTrafficFields lists the typed spec.traffic capability
	// tokens (pkg/constants TrafficCapability*) the active
	// Gateway-implementation translator implements. Feeds
	// validation.ValidateTrafficCapabilities: the reserved
	// endpointOverride.type=Metadata is rejected while no translator
	// declares it, and multi-header consistent hashing is rejected
	// when the set is non-empty and lacks the capability. Empty
	// disables the provider-specific gate (the reserved-value
	// rejection still applies); production populates this from the
	// active translator's SupportedTrafficFields().
	SupportedTrafficFields []string

	// RolloutPolicyEnabled reports whether the RolloutPolicy CRD is
	// installed; with it false, any groups[].policyRef is rejected outright —
	// a ref on a non-feature cluster would otherwise park every rollout with
	// nothing installed to ever resolve it.
	RolloutPolicyEnabled bool

	// RolloutMaxPlanBytes caps the serialized size of each inline rollout
	// progression body (0 = uncapped; the operator config supplies the
	// value). Inline plans get pinned into status at run open, so both
	// authoring paths — policy bodies at policy admission, inline bodies
	// here — are bounded by the same knob.
	RolloutMaxPlanBytes int

	// AutoscalerPolicyEnabled reports whether the AutoscalerPolicy CRD is
	// installed and the feature is on. When false, any per-Component
	// autoscalerPolicyRef is rejected outright — the resolver would hold
	// the component fail-closed forever with no actor able to render it.
	// Production wiring sets this from CRD discovery.
	AutoscalerPolicyEnabled bool
}

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-inferenceservice,mutating=false,failurePolicy=fail,groups=ome.io,resources=inferenceservices,versions=v1beta1,name=inferenceservice.ome-webhook-server.validator
var _ admission.Validator[*v1beta1.InferenceService] = &InferenceServiceValidator{}

func (v *InferenceServiceValidator) ValidateCreate(ctx context.Context, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	if err := validateLegacyAutoscalerFieldsFromCtx(ctx); err != nil {
		return nil, err
	}
	// CREATE validates the policy outright; the update-time ratchet lives
	// in ValidateUpdate, which has the prior object.
	if err := validation.ValidateScalingPolicy(isvc.Spec.ScalingPolicy, "spec.scalingPolicy"); err != nil {
		return nil, err
	}
	// old=nil: every migration-request annotation present at CREATE is
	// an add and gets validated (parity with the update path).
	if err := validateAddedMigrationRequests(nil, isvc); err != nil {
		return nil, err
	}
	// old=nil: CREATE rejects every multi-pod OMENative Component that
	// sets lifecycle.readyPolicy=None.
	if err := validateMultiPodReadyPolicyNone(nil, isvc); err != nil {
		return nil, err
	}
	warnings, err := v.validateInferenceService(ctx, isvc)
	if err != nil {
		return warnings, err
	}
	// Rollout-ordering enforceability runs after the structural rules so
	// the more specific rejections (duplicate order entry, foreign order
	// Component, ...) keep precedence over the blanket ordering rejection.
	if err := validation.ValidateRolloutOrderingEnforced(&isvc.Spec); err != nil {
		return warnings, err
	}
	return warnings, nil
}

func (v *InferenceServiceValidator) ValidateUpdate(ctx context.Context, oldIsvc, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	// An ISVC already under deletion is admitted unconditionally.
	// Removing the controller's finalizer is an UPDATE, so it lands here
	// and never in the no-op ValidateDelete, and this webhook is
	// fail-closed (failurePolicy=fail): a denied finalizer removal wedges
	// the object in Terminating forever. Nothing is given up by skipping —
	// the controller stops reconciling a terminating ISVC (it returns
	// right after dropping the finalizer), so the spec the rules below
	// protect is never acted on again.
	if !oldIsvc.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	if err := validateLegacyAutoscalerFieldsFromCtx(ctx); err != nil {
		return nil, err
	}
	if err := validation.ValidateCoordinationUpdate(&oldIsvc.Spec, &isvc.Spec); err != nil {
		return nil, err
	}
	if err := validation.ValidatePairingProtocolUpdate(&oldIsvc.Spec, &isvc.Spec); err != nil {
		return nil, err
	}
	// Ratchet: only a newly-set or changed scaling policy is validated,
	// so a stored object carrying a rejected mode keeps accepting
	// unrelated spec updates.
	if err := validation.ValidateScalingPolicyUpdate(oldIsvc.Spec.ScalingPolicy, isvc.Spec.ScalingPolicy, "spec.scalingPolicy"); err != nil {
		return nil, err
	}
	if err := validateAddedMigrationRequests(oldIsvc, isvc); err != nil {
		return nil, err
	}
	if err := validateMultiPodReadyPolicyNone(oldIsvc, isvc); err != nil {
		return nil, err
	}
	warnings, err := v.validateInferenceService(ctx, isvc)
	if err != nil {
		return warnings, err
	}
	// Ratcheted rollout-ordering enforceability: applied only when this
	// update changes spec.rollout, so stored objects keep reconciling.
	// Runs after the structural rules so the more specific rejections keep
	// precedence (parity with the CREATE path).
	if err := validation.ValidateRolloutOrderingEnforcedUpdate(&oldIsvc.Spec, &isvc.Spec); err != nil {
		return warnings, err
	}
	return warnings, nil
}

// validateAddedMigrationRequests validates every migration-request
// annotation (ome.io/migration-request-v1-<uuid>) this write ADDS —
// present in the new object with a value absent or different in the
// old. A nil oldIsvc (CREATE) treats every present annotation as an
// add. Pre-existing unchanged annotations are never re-validated, and
// deletions are always admitted: the controller consumes the mailbox
// by deleting keys, so a delete must never be blocked.
func validateAddedMigrationRequests(oldIsvc, isvc *v1beta1.InferenceService) error {
	var oldAnnotations map[string]string
	if oldIsvc != nil {
		oldAnnotations = oldIsvc.Annotations
	}
	for key, raw := range isvc.Annotations {
		if !strings.HasPrefix(key, audit.MigrationRequestAnnotationPrefix) {
			continue
		}
		if oldRaw, existed := oldAnnotations[key]; existed && oldRaw == raw {
			continue
		}
		uuid := audit.ExtractRequestUUID(key)
		if uuid == "" {
			return fmt.Errorf("migration request %s: annotation key carries no request UUID", key)
		}
		req, err := audit.ParseMigrationRequest(raw)
		if err != nil {
			return fmt.Errorf("migration request %s: %v", uuid, err)
		}
		if err := validateMigrationRequestComponent(isvc, req.Component); err != nil {
			return fmt.Errorf("migration request %s: %v", uuid, err)
		}
		if req.Instance < 0 {
			return fmt.Errorf("migration request %s: instance must be >= 0, got %d", uuid, req.Instance)
		}
	}
	return nil
}

// validateMigrationRequestComponent checks a migration request's
// component against the components this ISVC declares: engine always;
// decoder / router only when their spec blocks are present (same
// enumeration rule as isvcAutoscalerChecks).
func validateMigrationRequestComponent(isvc *v1beta1.InferenceService, component string) error {
	switch component {
	case string(v1beta1.EngineComponent):
		return nil
	case string(v1beta1.DecoderComponent):
		if isvc.Spec.Decoder != nil {
			return nil
		}
	case string(v1beta1.RouterComponent):
		if isvc.Spec.Router != nil {
			return nil
		}
	}
	return fmt.Errorf("component %q does not exist on this InferenceService", component)
}

// validateLegacyAutoscalerFieldsFromCtx pulls the raw admission request
// out of ctx and runs the defense-in-depth check for the deleted
// scaleTarget / scaleMetric fields. Silently no-ops when the request
// cannot be retrieved (e.g., when the validator is invoked from a unit
// test that doesn't stamp the ctx), so it never blocks tests that only
// feed a typed object.
func validateLegacyAutoscalerFieldsFromCtx(ctx context.Context) error {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil
	}
	return validation.ValidateLegacyAutoscalerFieldsRaw(req.AdmissionRequest.Object.Raw)
}

func (v *InferenceServiceValidator) ValidateDelete(_ context.Context, _ *v1beta1.InferenceService) (admission.Warnings, error) {
	return nil, nil
}

// GetIntReference returns the pointer for the integer input
func GetIntReference(number int) *int {
	num := number
	return &num
}

// deploymentStrategyWarnings flags components that set deploymentStrategy while
// resolving to a non-Raw deployment mode. deploymentStrategy is the k8s-native
// appsv1.DeploymentStrategy and is honored only by RawDeployment (createRawDeployment
// copies it onto the Deployment); OMENative and PD-disaggregated manage pods
// directly and ignore it, so it is a silent no-op there. Point operators at
// lifecycle.updateStrategy, which OMENative reads for rollout pacing.
func deploymentStrategyWarnings(isvc *v1beta1.InferenceService) admission.Warnings {
	mode := isvc.Annotations[constants.DeploymentMode]
	if mode == "" && isvc.Spec.DeploymentMode != nil {
		mode = string(*isvc.Spec.DeploymentMode)
	}
	// Unknown mode (defaulter hasn't run, e.g. a unit test) or Raw: nothing to warn.
	if mode == "" || mode == string(constants.RawDeployment) {
		return nil
	}
	var warnings admission.Warnings
	check := func(name string, ext *v1beta1.ComponentExtensionSpec) {
		if ext != nil && ext.DeploymentStrategy != nil {
			warnings = append(warnings, fmt.Sprintf(
				"%s.deploymentStrategy is set but deploymentMode is %q; deploymentStrategy applies only to RawDeployment and is ignored here — use %s.lifecycle.updateStrategy.rollingUpdate for rollout pacing",
				name, mode, name))
		}
	}
	if isvc.Spec.Engine != nil {
		check("engine", &isvc.Spec.Engine.ComponentExtensionSpec)
	}
	if isvc.Spec.Decoder != nil {
		check("decoder", &isvc.Spec.Decoder.ComponentExtensionSpec)
	}
	if isvc.Spec.Router != nil {
		check("router", &isvc.Spec.Router.ComponentExtensionSpec)
	}
	return warnings
}

func (v *InferenceServiceValidator) validateInferenceService(ctx context.Context, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	var allWarnings admission.Warnings

	if err := validation.ValidateInferenceServiceName(isvc.Name); err != nil {
		return allWarnings, err
	}

	autoscalerWarnings, err := validation.ValidateAutoscalerConfig(isvc)
	if err != nil {
		return allWarnings, err
	}
	allWarnings = append(allWarnings, autoscalerWarnings...)

	// Per-Component Autoscaler block shape validation: class enum,
	// required fields, IdleReplicaCount vs MinReplicas, plus
	// annotation/block conflict. The per-Component dispatch is shared
	// with the ServingRuntime and InferenceReplica webhooks via
	// validation.ValidateComponentsAutoscalers.
	if err := validation.ValidateComponentsAutoscalers(isvcAutoscalerChecks(isvc)); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateAutoscalerAnnotationConflict(isvc); err != nil {
		return allWarnings, err
	}

	// Per-Component AutoscalerPolicy refs: the reserved
	// ClusterAutoscalerPolicy kind is rejected, and any ref is rejected
	// while the feature is disabled on this cluster.
	if err := validation.ValidateAutoscalerPolicyRefs(isvc, v.AutoscalerPolicyEnabled); err != nil {
		return allWarnings, err
	}
	// Split placement with a policy ref and no per-cluster replica cap
	// renders each home against an unbounded ceiling — warn, don't reject.
	if warning := validation.AutoscalerPolicySplitCeilingWarning(isvc); warning != "" {
		allWarnings = append(allWarnings, warning)
	}

	if err := validation.ValidateAutoscalerTargetUtilizationPercentage(isvc); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateEngineDecoderConfig(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateLeaderWorkerPairing(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	// Forward-looking guard that the leader Runner stays at size=1
	// (today enforced structurally by LeaderSpec having no Size field;
	// see ValidateLeaderSize).
	if err := validation.ValidateLeaderSize(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	// Lean (no-model, no-runtime) multi-pod specs must declare both
	// leader.runner.image and worker.runner.image — the controller has
	// no way to fill them in without a runtime or model.
	if err := validation.ValidateMultiPodLeanRunner(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateEngineDecoderDeploymentMode(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateScaleToZero(isvc); err != nil {
		return allWarnings, err
	}
	// Per-Component replica bounds: negative minReplicas, explicit
	// maxReplicas < 1, and minReplicas > maxReplicas are rejected with
	// the exact spec field path.
	if err := validateComponentReplicaBounds(isvc); err != nil {
		return allWarnings, err
	}
	// Per-Component disruption budget shape: minAvailable and
	// maxUnavailable are mutually exclusive on a Kubernetes
	// PodDisruptionBudget, so the pair is rejected before the controller
	// ever builds an invalid child PDB.
	if err := validateComponentPodDisruptionBudgets(isvc); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidatePlacement(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// Traffic block: typed core load-balancing config + ome.io/* annotations.
	if err := validation.ValidateTrafficSpec(isvc.Spec.Traffic); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateTrafficCapabilities(isvc.Spec.Traffic, v.SupportedTrafficFields); err != nil {
		return allWarnings, err
	}
	trafficWarnings, err := validation.ValidateTrafficAnnotations(isvc.Annotations, isvc.Spec.Traffic, v.KnownPassthroughPrefixes...)
	if err != nil {
		return allWarnings, err
	}
	allWarnings = append(allWarnings, trafficWarnings...)

	// spec.rollout.canary: validate the progressive-traffic rollout plan.
	if err := validation.ValidateRolloutPolicyRefs(&isvc.Spec, v.RolloutPolicyEnabled); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateInlineRolloutPlanSize(&isvc.Spec, v.RolloutMaxPlanBytes); err != nil {
		return allWarnings, err
	}
	if err := validation.ValidateCanary(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// Cross-Component rollout coordination: structural + RatioBalanced
	// tolerance warning.
	if err := validation.ValidateCoordination(&isvc.Spec); err != nil {
		return allWarnings, err
	}
	if warning := validation.CoordinationRatioToleranceWarning(&isvc.Spec); warning != "" {
		allWarnings = append(allWarnings, warning)
	}

	// Per-Component LifecycleSpec.UpdateStrategy.RollingUpdate semantic
	// validation: percent strings must be "<n>%" with 0 <= n <= 100;
	// integer values must be >= 0. The CRD XIntOrString marker handles
	// the structural "must be int or string" check; this validator adds
	// the range / format check.
	if err := validation.ValidateLifecycle(&isvc.Spec); err != nil {
		return allWarnings, err
	}

	// deploymentStrategy (k8s appsv1) is honored only by RawDeployment; on
	// OMENative / PD it is silently ignored. Warn so operators move rollout
	// pacing to lifecycle.updateStrategy, which OMENative actually reads.
	allWarnings = append(allWarnings, deploymentStrategyWarnings(isvc)...)

	// At least one of spec.model or spec.runtime must be set. When
	// spec.model is omitted (lean path), spec.runtime must name the
	// runtime to use — the controller skips model-fetch and runtime
	// auto-selection in that mode.
	modelSet := isvc.Spec.Model != nil && isvc.Spec.Model.Name != ""
	runtimeSet := isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != ""
	if !modelSet && !runtimeSet {
		return allWarnings, fmt.Errorf("at least one of spec.model or spec.runtime must be set")
	}

	if err := validateRuntimePin(isvc); err != nil {
		return allWarnings, err
	}
	if err := v.validateModelExists(ctx, isvc); err != nil {
		return allWarnings, err
	}
	if err := v.validateOverlays(isvc); err != nil {
		return allWarnings, err
	}

	if isvc.Spec.Engine != nil {
		resWarnings, err := v.validateRuntimeAndModelResolution(ctx, isvc)
		if err != nil {
			return allWarnings, err
		}
		allWarnings = append(allWarnings, resWarnings...)
	}
	return allWarnings, nil
}

// validateModelExists ensures the referenced BaseModel/ClusterBaseModel
// (and each overlay) is reachable in the ISVC's namespace, and that
// cross-namespace PVC rules pass per-overlay.
func (v *InferenceServiceValidator) validateModelExists(ctx context.Context, isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Model == nil || isvc.Spec.Model.Name == "" {
		return nil
	}
	spec, meta, err := isvcutils.GetBaseModel(v.Client, isvc.Spec.Model.Name, isvc.Namespace)
	if err != nil {
		return fmt.Errorf("referenced model %q not found in namespace %q: ensure a BaseModel exists in this namespace or a ClusterBaseModel exists cluster-wide with this name",
			isvc.Spec.Model.Name, isvc.Namespace)
	}
	if err := validateClusterBaseModelPVCNamespace(spec, meta, isvc); err != nil {
		return err
	}
	for _, ov := range isvc.Spec.Model.Overlays {
		ovSpec, ovMeta, err := isvcutils.GetBaseModel(v.Client, ov.Name, isvc.Namespace)
		if err != nil {
			return fmt.Errorf("overlay %q not found in namespace %q: %w", ov.Name, isvc.Namespace, err)
		}
		if err := validateClusterBaseModelPVCNamespace(ovSpec, ovMeta, isvc); err != nil {
			return fmt.Errorf("overlay %q: %w", ov.Name, err)
		}
	}
	return nil
}

// validateClusterBaseModelPVCNamespace rejects InferenceServices that
// reference a ClusterBaseModel whose PVC URI points to a namespace other
// than the InferenceService's own namespace. Kubernetes does not allow a
// pod to mount a PVC from a different namespace, so without this check the
// generated ISVC pods would fail at scheduling with a confusing
// "persistentvolumeclaim ... not found" error and no upstream signal.
//
// Skipped for namespaced BaseModel (already same-namespace by construction)
// and for non-PVC URIs.
func validateClusterBaseModelPVCNamespace(spec *v1beta1.BaseModelSpec, meta *metav1.ObjectMeta, isvc *v1beta1.InferenceService) error {
	// namespaced BaseModel ⇒ same-namespace by construction; skip
	if meta == nil || meta.Namespace != "" {
		return nil
	}
	if spec == nil || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return nil
	}
	uri := *spec.Storage.StorageUri
	storageType, err := storage.GetStorageType(uri)
	if err != nil || storageType != storage.StorageTypePVC {
		return nil
	}
	components, err := storage.ParsePVCStorageURI(uri)
	if err != nil || components.Namespace == "" || components.Namespace == isvc.Namespace {
		return nil
	}
	return fmt.Errorf(
		"ClusterBaseModel %q references PVC in namespace %q, but InferenceService is in namespace %q; "+
			"Kubernetes does not allow pods to mount PVCs from another namespace. "+
			"Either move the InferenceService to namespace %q or replicate the PVC into namespace %q.",
		isvc.Spec.Model.Name, components.Namespace, isvc.Namespace, components.Namespace, isvc.Namespace)
}

// validateOverlays rejects overlay sets that would produce a broken
// pod spec: duplicate Name, env-var name collision after sanitization
// (foo-bar and foo_bar both → FOO_BAR), or self-reference.
// Per-overlay existence + runtime compatibility live in
// validateRuntimeAndModelResolution.
func (v *InferenceServiceValidator) validateOverlays(isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Model == nil || len(isvc.Spec.Model.Overlays) == 0 {
		return nil
	}
	primary := isvc.Spec.Model.Name
	seenName := map[string]bool{}
	seenEnv := map[string]string{} // sanitized → original name
	for _, ov := range isvc.Spec.Model.Overlays {
		if ov.Name == "" {
			return fmt.Errorf("overlay name cannot be empty")
		}
		if ov.Name == primary {
			return fmt.Errorf("overlay %q must not reference the primary model of the same kind/apiGroup", ov.Name)
		}
		if seenName[ov.Name] {
			return fmt.Errorf("overlay %q is declared more than once", ov.Name)
		}
		seenName[ov.Name] = true

		envName := isvcutils.OverlayEnvVarName(ov.Name)
		if prev, ok := seenEnv[envName]; ok {
			return fmt.Errorf("overlays %q and %q both sanitize to env var %s; rename one (hyphens and underscores collapse to the same form)",
				prev, ov.Name, envName)
		}
		seenEnv[envName] = ov.Name
	}
	return nil
}

// validateRuntimeAndModelResolution gates runtime/model resolution on
// the Engine-architecture path. Runtime-only specs check live-synced
// source availability without running model compatibility validation.
func (v *InferenceServiceValidator) validateRuntimeAndModelResolution(ctx context.Context, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	var warnings admission.Warnings

	if isvc.Spec.Engine == nil {
		return warnings, nil
	}
	if isvc.Spec.Model == nil {
		if isvc.Spec.Runtime == nil && !hasFullRunnerConfig(isvc.Spec.Engine) {
			return warnings, fmt.Errorf("model reference is required when runtime is not specified and engine does not have complete runner configuration")
		}
		return warnings, v.validateRuntimeOnlyAvailability(ctx, isvc)
	}
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		return v.resolveModelAndRuntime(ctx, isvc, warnings)
	}
	if !hasFullRunnerConfig(isvc.Spec.Engine) {
		return v.resolveModelAndRuntime(ctx, isvc, warnings)
	}
	return warnings, nil
}

func (v *InferenceServiceValidator) validateRuntimeOnlyAvailability(ctx context.Context, isvc *v1beta1.InferenceService) error {
	runtimeRef := isvc.Spec.Runtime
	if runtimeRef == nil || runtimeRef.Name == "" {
		return nil
	}
	if runtimeRef.AutoSync != nil && !*runtimeRef.AutoSync {
		return nil
	}

	runtimeSpec, isCluster, err := v.RuntimeSelector.GetRuntime(ctx, runtimeRef.Name, isvc.Namespace, runtimeselector.RefKind(runtimeRef))
	if err != nil {
		if runtimeselector.IsRuntimeNotFoundError(err) {
			return nil
		}
		return err
	}
	if runtimeSpec.IsDisabled() {
		return &runtimeselector.RuntimeDisabledError{
			RuntimeName: runtimeRef.Name,
			IsCluster:   isCluster,
		}
	}
	return nil
}

func (v *InferenceServiceValidator) resolveModelAndRuntime(ctx context.Context, isvc *v1beta1.InferenceService, warnings admission.Warnings) (admission.Warnings, error) {
	baseModel, _, err := isvcutils.GetBaseModel(v.Client, isvc.Spec.Model.Name, isvc.Namespace)
	if err != nil {
		return warnings, fmt.Errorf("failed to resolve model %s: %w", isvc.Spec.Model.Name, err)
	}
	if baseModel.Disabled != nil && *baseModel.Disabled {
		return warnings, fmt.Errorf("model %s is disabled", isvc.Spec.Model.Name)
	}

	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		if err := v.RuntimeSelector.ValidateRuntime(ctx, isvc.Spec.Runtime.Name, baseModel, isvc); err != nil {
			// The operator named this runtime explicitly, so OME should
			// not block on the runtime's *declared* supportedModelFormats:
			// a generic runtime (e.g. sglang) can serve many architectures
			// it never enumerates. Downgrade a pure compatibility mismatch
			// (format / architecture / framework) to an advisory warning
			// and admit — the deliberate choice wins over the declaration.
			//
			// Everything else stays a hard error:
			//   - runtime not found / disabled / malformed model: the
			//     runtime genuinely cannot run, so admitting would only
			//     produce a broken ISVC.
			//   - a sharded (Distribution=Sharded) model with no configured
			//     cache provider: this surfaces as a RuntimeCompatibilityError
			//     too, but it is a real cluster-config blocker — a sharded
			//     model physically cannot load without a cache provider — not
			//     an over-conservative declared-format check. Keep it gating.
			if runtimeselector.IsRuntimeCompatibilityError(err) && !modelRequiresCacheProvider(baseModel) {
				warnings = append(warnings, fmt.Sprintf(
					"runtime %q does not declare support for model %q (%v); proceeding because the runtime was named explicitly",
					isvc.Spec.Runtime.Name, isvc.Spec.Model.Name, err))
				return warnings, nil
			}
			return warnings, fmt.Errorf("runtime %s does not support model %s: %w",
				isvc.Spec.Runtime.Name, isvc.Spec.Model.Name, err)
		}
		// Success is the common case and emits nothing — a bare "is valid"
		// admission Warning showed up as `Warning:` in kubectl and read as
		// if something were wrong.
		return warnings, nil
	}
	selection, err := v.RuntimeSelector.SelectRuntime(ctx, baseModel, isvc)
	if err != nil {
		return warnings, fmt.Errorf("no supporting runtime found for model %s and engine does not have complete runner configuration: %w", isvc.Spec.Model.Name, err)
	}
	warnings = append(warnings, fmt.Sprintf("Runtime %s will be auto-selected for model %s",
		selection.Name, isvc.Spec.Model.Name))
	return warnings, nil
}

// validateRuntimePin rejects malformed explicit ControllerRevision pins
// at admission. The full existence check is left to reconcile (the
// webhook can't reliably reach across cluster scope), but the cheap
// shape checks happen here:
//
//   - spec.runtime.revision is meaningful only when autoSync is false;
//     setting it with autoSync=true (default) is a confused-user
//     signal worth surfacing immediately.
//   - The revision name must conform to the convention for the named
//     runtime's kind + scope; an obviously-wrong name (e.g., pinning
//     to runtime A but naming a revision of runtime B) is rejected
//     before the controller ever sees it.
func validateRuntimePin(isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Revision == nil || *isvc.Spec.Runtime.Revision == "" {
		return nil
	}
	if isvc.Spec.Runtime.AutoSync == nil || *isvc.Spec.Runtime.AutoSync {
		return fmt.Errorf(
			"spec.runtime.revision requires spec.runtime.autoSync=false; AutoSync=true would silently ignore the pin")
	}
	kind := runtimerevision.KindClusterServingRuntime
	sourceNS := ""
	if isvc.Spec.Runtime.Kind != nil && *isvc.Spec.Runtime.Kind == string(runtimerevision.KindServingRuntime) {
		kind = runtimerevision.KindServingRuntime
		sourceNS = isvc.Namespace
	}
	revName := *isvc.Spec.Runtime.Revision
	if !runtimerevision.MatchesRuntime(revName, kind, sourceNS, isvc.Spec.Runtime.Name) {
		return fmt.Errorf(
			"spec.runtime.revision %q does not match the expected naming convention for runtime %q (%s); "+
				"expected the form %s",
			revName, isvc.Spec.Runtime.Name, kind, expectedPinPattern(kind, sourceNS, isvc.Spec.Runtime.Name))
	}
	return nil
}

func expectedPinPattern(kind runtimerevision.SourceKind, sourceNS, runtimeName string) string {
	switch kind {
	case runtimerevision.KindClusterServingRuntime:
		return fmt.Sprintf("cr-%s-<8 lowercase hex chars>", runtimeName)
	case runtimerevision.KindServingRuntime:
		return fmt.Sprintf("r-%s-%s-<8 lowercase hex chars>", sourceNS, runtimeName)
	default:
		return "<unknown kind>"
	}
}

// isvcAutoscalerChecks projects each declared Component (Engine,
// Decoder, Router) into the per-Component slice consumed by
// validation.ValidateComponentsAutoscalers. Nil Components are skipped
// so an absent Component never produces a spurious "<name>:" error.
func isvcAutoscalerChecks(isvc *v1beta1.InferenceService) []validation.ComponentAutoscalerCheck {
	var checks []validation.ComponentAutoscalerCheck
	if isvc.Spec.Engine != nil {
		checks = append(checks, validation.ComponentAutoscalerCheck{
			Name:        "engine",
			Autoscaler:  isvc.Spec.Engine.Autoscaler,
			MinReplicas: isvc.Spec.Engine.MinReplicas,
		})
	}
	if isvc.Spec.Decoder != nil {
		checks = append(checks, validation.ComponentAutoscalerCheck{
			Name:        "decoder",
			Autoscaler:  isvc.Spec.Decoder.Autoscaler,
			MinReplicas: isvc.Spec.Decoder.MinReplicas,
		})
	}
	if isvc.Spec.Router != nil {
		checks = append(checks, validation.ComponentAutoscalerCheck{
			Name:        "router",
			Autoscaler:  isvc.Spec.Router.Autoscaler,
			MinReplicas: isvc.Spec.Router.MinReplicas,
		})
	}
	return checks
}

// componentExtensionCheck names one declared Component's
// ComponentExtensionSpec so per-Component field checks can report the
// exact spec path (e.g. "spec.engine").
type componentExtensionCheck struct {
	name string
	ext  *v1beta1.ComponentExtensionSpec
}

// isvcComponentExtensions projects each declared Component (Engine,
// Decoder, Router) into the per-Component slice consumed by the
// replica-bounds and disruption-budget checks. Nil Components are
// skipped so an absent Component never produces a spurious error.
func isvcComponentExtensions(isvc *v1beta1.InferenceService) []componentExtensionCheck {
	var checks []componentExtensionCheck
	if isvc.Spec.Engine != nil {
		checks = append(checks, componentExtensionCheck{"engine", &isvc.Spec.Engine.ComponentExtensionSpec})
	}
	if isvc.Spec.Decoder != nil {
		checks = append(checks, componentExtensionCheck{"decoder", &isvc.Spec.Decoder.ComponentExtensionSpec})
	}
	if isvc.Spec.Router != nil {
		checks = append(checks, componentExtensionCheck{"router", &isvc.Spec.Router.ComponentExtensionSpec})
	}
	return checks
}

// validateComponentReplicaBounds checks MinReplicas / MaxReplicas
// sanity for every declared Component. MaxReplicas is a non-pointer
// int with no explicit "unset" marker — the zero value means the
// operator omitted it — so 0 skips the max-side checks instead of
// being rejected as < 1. MinReplicas=0 stays legal here; the
// scale-to-zero gate is a separate validator.
func validateComponentReplicaBounds(isvc *v1beta1.InferenceService) error {
	for _, c := range isvcComponentExtensions(isvc) {
		var max *int
		if c.ext.MaxReplicas != 0 {
			max = &c.ext.MaxReplicas
		}
		if err := validation.ValidateReplicaBounds(c.ext.MinReplicas, max); err != nil {
			// ValidateReplicaBounds messages begin with the offending
			// field name, so prefixing the Component path yields the
			// exact field path (e.g. "spec.engine.minReplicas must be >= 0").
			return fmt.Errorf("spec.%s.%w", c.name, err)
		}
	}
	return nil
}

// validateComponentPodDisruptionBudgets validates each declared
// Component's disruption budget: minAvailable and maxUnavailable are
// mutually exclusive (a Kubernetes PodDisruptionBudget accepts at most
// one) and each value must be a non-negative integer or a 0-100%
// percentage. Errors name the exact spec field path.
func validateComponentPodDisruptionBudgets(isvc *v1beta1.InferenceService) error {
	for _, c := range isvcComponentExtensions(isvc) {
		if err := validation.ValidatePodDisruptionBudget("spec."+c.name, c.ext.MinAvailable, c.ext.MaxUnavailable); err != nil {
			return err
		}
	}
	return nil
}

// modelRequiresCacheProvider reports whether the model is sharded
// (Distribution=Sharded) and therefore needs a configured cluster cache
// provider to load. A sharded model that names a runtime explicitly must
// still hard-fail when no provider is configured — it physically cannot
// load — so resolveModelAndRuntime keeps gating that case instead of
// downgrading it to an advisory warning. Mirrors the (unexported)
// runtimeselector helper of the same name.
func modelRequiresCacheProvider(model *v1beta1.BaseModelSpec) bool {
	return model != nil &&
		model.Distribution != nil &&
		*model.Distribution == v1beta1.DistributionSharded
}

// hasFullRunnerConfig reports whether the engine has enough runner
// configuration to skip runtime selection: either a top-level Runner
// with image, or matching Leader.Runner + Worker.Runner images (multi-node).
func hasFullRunnerConfig(engine *v1beta1.EngineSpec) bool {
	if engine == nil {
		return false
	}
	if engine.Runner != nil && engine.Runner.Image != "" {
		return true
	}
	if engine.Leader != nil && engine.Worker != nil {
		leaderHasRunner := engine.Leader.Runner != nil && engine.Leader.Runner.Image != ""
		workerHasRunner := engine.Worker.Runner != nil && engine.Worker.Runner.Image != ""
		return leaderHasRunner && workerHasRunner
	}
	return false
}
