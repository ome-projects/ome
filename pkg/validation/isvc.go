package validation

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/affinity"
)

// Multi-node validation reason constants. Operators reference these
// by name, so they're exported and stable.
const (
	// ReasonInvalidLeaderWorkerPairing is the legacy umbrella reason
	// for any leader/worker shape mismatch. Retained for back-compat
	// with operators scraping the error string; new errors also embed
	// the more-specific reason below.
	ReasonInvalidLeaderWorkerPairing = "InvalidLeaderWorkerPairing"

	// ReasonWorkerSizeMustBePositive rejects worker.size=0 (or nil)
	// when a leader is declared. The "default 0 means single-pod"
	// fallback is ambiguous — force the operator to either omit
	// Worker entirely (single-pod with default Runner) or set Size >= 1.
	ReasonWorkerSizeMustBePositive = "WorkerSizeMustBePositive"

	// ReasonLeaderRequiresWorker rejects leader-only configs. A leader
	// without a worker has no gang to lead and is meaningless for
	// tensor parallelism.
	ReasonLeaderRequiresWorker = "LeaderRequiresWorker"

	// ReasonWorkerRequiresLeader rejects worker-only configs.
	// Symmetric to LeaderRequiresWorker; covered by the same
	// validator function.
	ReasonWorkerRequiresLeader = "WorkerRequiresLeader"

	// ReasonLeaderSizeMustBeOne rejects user attempts to scale the
	// leader Runner above 1. The leader is fixed at size=1: the
	// OME_LEADER_ADDRESS contract resolves to a single
	// <isvc>-<comp>-<inst>-leader-0 FQDN, so multiple leaders would
	// produce ambiguous DNS. LeaderSpec has no explicit Size field
	// today (enforced by type); this validator is a forward-looking
	// guard in case the API surface evolves.
	ReasonLeaderSizeMustBeOne = "LeaderSizeMustBeOne"

	// ReasonMultiPodLeanModelNeedsRunner rejects lean (no spec.model,
	// no spec.runtime) ISVCs that declare a multi-pod Component
	// without populating both leader.runner.image and
	// worker.runner.image. The lean path is supported, but
	// multi-pod lean specs cannot be filled in by runtime selection
	// (no runtime) or model parsing (no model), so the operator must
	// supply images explicitly.
	ReasonMultiPodLeanModelNeedsRunner = "MultiPodLeanModelNeedsRunner"

	// ReasonZeroBudgetPacingUnstartable rejects PerComponent pacing
	// blocks that set BOTH maxSurge=0 AND maxUnavailable=0. With both
	// budgets at literal zero there is no surge headroom (no pod can be
	// added) AND no drain headroom (no pod can be taken offline), so
	// the rollout has no way to make progress and the Component will
	// never finish reconciling. Unset values are not zero — the
	// coordination resolver fills 25% for both — so this reason only
	// fires when the operator has explicitly set both to 0. Mirrored as a runtime
	// safety net in the coordination groups resolver.
	ReasonZeroBudgetPacingUnstartable = "ZeroBudgetPacingUnstartable"
)

func ValidateInferenceServiceName(name string) error {
	if !IsvcNameRegex.MatchString(name) {
		return fmt.Errorf("invalid InferenceService name %q, must match %q", name, IsvcNameFmt)
	}
	return nil
}

func ValidateEngineDecoderConfig(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Decoder != nil && spec.Engine == nil {
		return fmt.Errorf("decoder cannot be specified without engine")
	}
	return nil
}

// ValidateLeaderWorkerPairing enforces that the Leader / Worker spec pair of
// each declared Component is consistent: either both are absent (single-pod)
// or both are present (multi-pod) with Worker.Size positive when set. An
// unset size is legal: it resolves at reconcile time to the runtime's value,
// or to one worker. Only the engine and the decoder can declare the pair.
//
// Each refusal carries the shared "InvalidLeaderWorkerPairing" reason and the
// specific reason from the constants above.
func ValidateLeaderWorkerPairing(spec *v1beta1.InferenceServiceSpec) error {
	for _, r := range declaredRoles(spec) {
		if err := validateLeaderWorkerPairing(string(r.kind), r.leader, r.worker); err != nil {
			return err
		}
	}
	return nil
}

func validateLeaderWorkerPairing(component string, leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) error {
	if leader == nil && worker == nil {
		return nil
	}
	if leader != nil && worker == nil {
		return fmt.Errorf(
			"%s: %s.leader is set but %s.worker is not; multi-pod configurations require both (%s)",
			ReasonInvalidLeaderWorkerPairing, component, component, ReasonLeaderRequiresWorker,
		)
	}
	if leader == nil && worker != nil {
		return fmt.Errorf(
			"%s: %s.worker is set but %s.leader is not; multi-pod configurations require both (%s)",
			ReasonInvalidLeaderWorkerPairing, component, component, ReasonWorkerRequiresLeader,
		)
	}
	if worker.Size != nil && *worker.Size <= 0 {
		return fmt.Errorf(
			"%s: %s.worker.size must be a positive integer when set; leave it unset to use the runtime's size or one worker (%s)",
			ReasonInvalidLeaderWorkerPairing, component, ReasonWorkerSizeMustBePositive,
		)
	}
	return nil
}

// ValidateLeaderSize enforces that no Component sets a leader Runner
// size greater than 1. The leader is fixed at size=1: the
// OME_LEADER_ADDRESS env var that workers (and the leader itself)
// consult resolves to <isvc>-<comp>-<inst>-leader-0; a second leader
// pod would shadow the DNS contract and break tensor-parallel
// initialization.
//
// LeaderSpec has no explicit Size field today, so this validator is
// structurally a no-op against the current API. It exists as a
// forward-looking guard so that any future API evolution adding such
// a field automatically inherits the rejection, with the stable
// reason string (%s) already in place for operators and tests.
//
// Implementation note: when LeaderSpec gains a Size field, replace
// the per-component no-op with a check that returns the formatted
// error using ReasonLeaderSizeMustBeOne.
func ValidateLeaderSize(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil {
		return nil
	}
	// Engine leader: structurally fixed at size=1 — no field to inspect.
	if err := validateLeaderSize("engine", spec.Engine); err != nil {
		return err
	}
	// Decoder leader: same.
	if err := validateLeaderSize("decoder", spec.Decoder); err != nil {
		return err
	}
	return nil
}

// validateLeaderSize is the per-Component shape check. LeaderSpec has
// no Size field today; the function is a no-op placeholder so the
// public ValidateLeaderSize entry point exists and tests can pin the
// reason constant. See ValidateLeaderSize doc for the upgrade plan.
func validateLeaderSize(component string, spec interface{}) error {
	// No-op against the current API surface. See the doc comment on
	// ValidateLeaderSize.
	_ = component
	_ = spec
	return nil
}

// ValidateMultiPodLeanRunner enforces that a lean (no spec.model AND
// no spec.runtime) InferenceService declaring a multi-pod Component
// (Engine or Decoder) populates both leader.runner.image and
// worker.runner.image. Lean ISVCs are supported for the
// single-pod path because the operator supplies a single Runner
// image. For multi-pod the controller renders distinct PodSpecs for
// leader and worker — without a runtime or model to fill in defaults,
// both Runner images must be explicit.
//
// Single-pod lean ISVCs continue to use the existing
// hasFullRunnerConfig path (runner.image satisfies the requirement).
// This validator only fires when MultiPod is true.
func ValidateMultiPodLeanRunner(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil {
		return nil
	}
	modelSet := spec.Model != nil && spec.Model.Name != ""
	runtimeSet := spec.Runtime != nil && spec.Runtime.Name != ""
	if modelSet || runtimeSet {
		return nil
	}
	// A lean spec carries the runner configuration itself, so a role that
	// declares a leader and workers must name both images. Only the engine
	// and the decoder can declare the pair.
	for _, r := range declaredRoles(spec) {
		if err := validateMultiPodLeanRunnerImages(string(r.kind), r.leader, r.worker); err != nil {
			return err
		}
	}
	return nil
}

func validateMultiPodLeanRunnerImages(component string, leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) error {
	if leader == nil || worker == nil {
		// Single-pod or invalid pair (covered by
		// ValidateLeaderWorkerPairing).
		return nil
	}
	leaderImg := ""
	if leader.Runner != nil {
		leaderImg = leader.Runner.Image
	}
	workerImg := ""
	if worker.Runner != nil {
		workerImg = worker.Runner.Image
	}
	if leaderImg != "" && workerImg != "" {
		return nil
	}
	return fmt.Errorf(
		"%s: %s is multi-pod with no spec.model and no spec.runtime, but %s.leader.runner.image (%q) or %s.worker.runner.image (%q) is empty; multi-pod lean specs must declare both runner images",
		ReasonMultiPodLeanModelNeedsRunner,
		component, component, leaderImg, component, workerImg,
	)
}

// ValidateEngineDecoderDeploymentMode enforces that when both Engine and
// Decoder are present and either resolves to OMENative, both components
// resolve to the same dispatch mode. The per-Component
// ome.io/deploymentMode annotation wins over spec.deploymentMode, but
// spec.deploymentMode acts as a uniform default — so a spec that sets
// spec.deploymentMode=OMENative without per-Component overrides
// satisfies this check (both Engine and Decoder resolve to OMENative).
func ValidateEngineDecoderDeploymentMode(spec *v1beta1.InferenceServiceSpec) error {
	if spec.Engine == nil || spec.Decoder == nil {
		return nil
	}
	engineMode := resolveComponentDeploymentMode(spec.Engine.Annotations, spec.DeploymentMode)
	decoderMode := resolveComponentDeploymentMode(spec.Decoder.Annotations, spec.DeploymentMode)
	if engineMode != string(constants.OMENative) && decoderMode != string(constants.OMENative) {
		return nil
	}
	if engineMode != decoderMode {
		return fmt.Errorf(
			"InvalidDeploymentModeCombination: engine deploymentMode=%q does not match decoder deploymentMode=%q; when either is OMENative, both must use the same value",
			engineMode, decoderMode,
		)
	}
	return nil
}

// resolveComponentDeploymentMode returns the effective dispatch
// deployment mode for a single Component as a string, applying the
// resolution chain shared by all admission and reconcile call sites:
//
//  1. Per-Component ome.io/deploymentMode annotation (escape hatch).
//  2. spec.deploymentMode (the typed top-level default).
//  3. Empty string when neither is set.
//
// Shape-derived modes (MultiNode from Leader/Worker; PDDisaggregated
// from Engine+Decoder pairing) are intentionally NOT considered here:
// this helper is consumed by admission rules that only care about the
// DISPATCH backend (OMENative vs RawDeployment), not the shape.
func resolveComponentDeploymentMode(annotations map[string]string, specMode *constants.DeploymentModeType) string {
	if mode, ok := annotations[constants.DeploymentMode]; ok && mode != "" {
		return mode
	}
	if specMode != nil {
		return string(*specMode)
	}
	return ""
}

func ValidateReplicaBounds(min, max *int) error {
	if min != nil && *min < 0 {
		return fmt.Errorf("minReplicas must be >= 0")
	}
	if max != nil && *max < 1 {
		return fmt.Errorf("maxReplicas must be >= 1")
	}
	if min != nil && max != nil && *min > *max {
		return fmt.Errorf("minReplicas (%d) must be <= maxReplicas (%d)", *min, *max)
	}
	return nil
}

// ValidateScaleToZero rejects InferenceServices that explicitly set a
// Component's MinReplicas=0 unless that Component is KEDA-autoscaled.
//
// Before Serverless mode was removed, MinReplicas=0 auto-promoted the
// engine to Knative-Serving (which natively supports scale-to-zero).
// After removal, MinReplicas=0 falls through to RawDeployment and the
// Deployment sits at zero replicas (broken). Operators needing
// scale-to-zero must opt into KEDA's idleReplicaCount.
//
// KEDA opt-in is accepted via either route:
//
//   - Typed per-Component field: spec.<component>.autoscaler.class
//     == KEDA (v1beta1.AutoscalerKEDA, "KEDA"). This is the preferred
//     surface and is evaluated PER Component — the specific Component that
//     sets MinReplicas=0 must itself be KEDA-autoscaled.
//   - Legacy top-level annotation: ome.io/autoscalerClass == "keda"
//     (constants.AutoscalerClassKEDA). The annotation applies to the
//     whole ISVC, so it satisfies the gate for every Component. Note the
//     case mismatch is intentional: the typed enum is "KEDA" while the
//     legacy annotation value is the lowercase "keda".
//
// A Component with MinReplicas=0 that is neither covered by the legacy
// annotation nor carries a typed KEDA autoscaler block is rejected.
func ValidateScaleToZero(isvc *v1beta1.InferenceService) error {
	annotations := isvc.ObjectMeta.Annotations
	legacyKEDA := constants.AutoscalerClassType(annotations[constants.AutoscalerClass]) == constants.AutoscalerClassKEDA

	// Every role embeds ComponentExtensionSpec, which carries MinReplicas
	// and Autoscaler.
	for _, r := range declaredRoles(&isvc.Spec) {
		minReplicas, autoscaler := r.extension.MinReplicas, r.extension.Autoscaler
		if minReplicas == nil || *minReplicas != 0 {
			continue
		}
		// MinReplicas=0 on this Component. Accept when the whole-ISVC legacy
		// annotation opts into KEDA, or when this Component carries a typed
		// KEDA autoscaler block.
		if legacyKEDA {
			continue
		}
		if autoscaler != nil && autoscaler.Class == v1beta1.AutoscalerKEDA {
			continue
		}
		return fmt.Errorf(
			"InvalidScaleToZero: %s.minReplicas=0 requires KEDA autoscaling "+
				"(set spec.%s.autoscaler.class=%s or the %s=%s annotation); "+
				"Serverless mode no longer auto-promotes scale-to-zero",
			r.kind, r.kind, v1beta1.AutoscalerKEDA,
			constants.AutoscalerClass, constants.AutoscalerClassKEDA,
		)
	}
	return nil
}

// ValidateAutoscalerConfig validates autoscaler-related annotations and spec
// fields. It returns warnings (non-fatal messages) and an error if validation
// fails.
func ValidateAutoscalerConfig(isvc *v1beta1.InferenceService) ([]string, error) {
	annotations := isvc.ObjectMeta.Annotations
	value, ok := annotations[constants.AutoscalerClass]
	if !ok {
		return nil, nil
	}
	class := constants.AutoscalerClassType(value)
	if !slices.Contains(constants.AutoscalerAllowedClassList, class) {
		return nil, fmt.Errorf("[%s] is not a supported autoscaler class type", value)
	}
	switch class {
	case constants.AutoscalerClassHPA:
		if metric, ok := annotations[constants.AutoscalerMetrics]; ok {
			return nil, ValidateHPAMetrics(metric)
		}
		return nil, nil
	case constants.AutoscalerClassKEDA, constants.AutoscalerClassExternal:
		// The per-Component Autoscaler.Keda shape is validated by
		// ValidateAutoscaler; the annotation path accepts class=keda as is.
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown autoscaler class [%s]", class)
	}
}

func ValidateAutoscalerTargetUtilizationPercentage(isvc *v1beta1.InferenceService) error {
	annotations := isvc.ObjectMeta.Annotations
	if value, ok := annotations[constants.TargetUtilizationPercentage]; ok {
		t, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("the target utilization percentage should be a [1-100] integer")
		}
		if t < 1 || t > 100 {
			return fmt.Errorf("the target utilization percentage should be a [1-100] integer")
		}
	}
	return nil
}

// ValidateHPAMetrics validates the legacy ome.io/metrics annotation value
// against the AutoscalerAllowedMetricsList enum. It takes the raw
// annotation string: the annotation is its only caller and carries no
// typed enum.
func ValidateHPAMetrics(metric string) error {
	for _, item := range constants.AutoscalerAllowedMetricsList {
		if string(item) == metric {
			return nil
		}
	}
	return fmt.Errorf("[%s] is not a supported metric", metric)
}

// Autoscaler webhook validation reason constants. Operators and the
// envtest suite (.../webhook/inferenceservice) reference these by
// name, so they're exported and stable.
const (
	// ReasonAutoscalerClassUnknown rejects a ComponentAutoscaler.Class
	// value that is not in the enum {hpa, keda, external, none}. The
	// CRD's +kubebuilder:validation:Enum marker enforces this at the
	// schema level; the webhook is defense-in-depth for clients that
	// bypass schema validation (e.g., legacy clients with a stale CRD).
	ReasonAutoscalerClassUnknown = "AutoscalerClassUnknown"

	// ReasonKedaTriggersRequired rejects class=keda blocks with empty
	// or nil Keda.Triggers. The CRD enforces MinItems=1 at the schema
	// level; webhook is defense-in-depth and also covers the
	// nil-Keda-pointer case the marker can't express.
	ReasonKedaTriggersRequired = "KedaTriggersRequired"

	// ReasonHPAMetricMalformed rejects HPA.Metrics entries whose
	// declared Type field disagrees with which pointer-field is
	// populated (e.g., Type=Resource with Resource=nil). Such entries
	// would silently produce a no-op HPA at apply-time; the webhook
	// surfaces the bug at admission.
	ReasonHPAMetricMalformed = "HPAMetricMalformed"

	// ReasonKedaIdleBelowMin rejects KEDA blocks whose IdleReplicaCount
	// is not strictly less than the effective MinReplicaCount. KEDA's
	// own admission rejects the same shape; this is a friendlier
	// up-front error so operators don't get a confusing KEDA-side
	// rejection after the OME write succeeds.
	ReasonKedaIdleBelowMin = "KedaIdleBelowMin"

	// ReasonKedaHPANameInvalid rejects a KEDA block whose
	// advanced.horizontalPodAutoscalerConfig.name is not a DNS label, for
	// example because it is longer than 63 characters. KEDA's own admission
	// rejects the ScaledObject at reconcile time; this rejects the spec when
	// it is applied instead.
	ReasonKedaHPANameInvalid = "KedaHPANameInvalid"

	// ReasonAutoscalerAnnotationConflict rejects an ISVC that carries
	// both the legacy ome.io/autoscalerClass annotation AND a new
	// Autoscaler block on any Component. The
	// per-Component Autoscaler block wins; this validation preempts
	// confusion by forcing operators to remove the annotation.
	ReasonAutoscalerAnnotationConflict = "AutoscalerAnnotationConflict"

	// ReasonLegacyAutoscalerFieldsRejected is the friendly migration
	// error returned by ValidateLegacyAutoscalerFieldsRaw when a client
	// submits an ISVC carrying the deleted scaleTarget / scaleMetric
	// fields. The Go-struct fields and CRD schema entries are gone; a
	// modern kubectl against an up-to-date CRD will already prune them
	// via structural-schema enforcement. This reason is defense-in-depth
	// for clients with a stale local CRD that bypassed pruning.
	ReasonLegacyAutoscalerFieldsRejected = "LegacyAutoscalerFieldsRejected"
)

// validAutoscalerClasses mirrors the CRD enum
// (+kubebuilder:validation:Enum=hpa;keda;external;none on
// AutoscalerClass). Used by the webhook defense-in-depth check.
var validAutoscalerClasses = map[v1beta1.AutoscalerClass]struct{}{
	v1beta1.AutoscalerHPA:      {},
	v1beta1.AutoscalerKEDA:     {},
	v1beta1.AutoscalerExternal: {},
	v1beta1.AutoscalerNone:     {},
}

// ComponentAutoscalerCheck names one Component's Autoscaler block for
// the slice-driven ValidateComponentsAutoscalers helper. Name is the
// field label used to wrap a per-Component error (e.g., "engine",
// "engineConfig"). Autoscaler may be nil — skipped silently.
// MinReplicas is the effective per-Component MinReplicas floor used to
// gate KEDA.IdleReplicaCount; nil disables that check.
type ComponentAutoscalerCheck struct {
	Name        string
	Autoscaler  *v1beta1.ComponentAutoscaler
	MinReplicas *int
}

// ValidateComponentsAutoscalers loops the per-Component checks and
// returns the first failure wrapped with "<Name>: ". Used by every
// webhook that dispatches the per-Component Autoscaler shape check
// across engine/decoder/router (or their ServingRuntime field-path
// twins) so the engine/decoder/router copy-paste lives in exactly one
// place. See ValidateAutoscaler for the rule set.
func ValidateComponentsAutoscalers(checks []ComponentAutoscalerCheck) error {
	for _, c := range checks {
		if err := ValidateAutoscaler(c.Autoscaler, c.MinReplicas); err != nil {
			if c.Name == "" {
				return err
			}
			return fmt.Errorf("%s: %w", c.Name, err)
		}
	}
	return nil
}

// ValidateAutoscaler checks one Component's Autoscaler block: the class is
// one of HPA, KEDA, External or None; a KEDA block has a trigger, an idle
// replica count below minReplicas and an HPA name that is a DNS label; an
// HPA block's metrics each carry the source their type reads. A nil block
// is valid, and a nil minReplicas skips the idle check.
func ValidateAutoscaler(autoscaler *v1beta1.ComponentAutoscaler, minReplicas *int) error {
	if autoscaler == nil {
		return nil
	}
	// The class enum, kept beside the CRD marker for clients that bypass it.
	if _, ok := validAutoscalerClasses[autoscaler.Class]; !ok {
		return fmt.Errorf(
			"%s: autoscaler.class %q is not one of HPA|KEDA|External|None",
			ReasonAutoscalerClassUnknown, autoscaler.Class,
		)
	}
	switch autoscaler.Class {
	case v1beta1.AutoscalerKEDA:
		return validateKedaAutoscaler(autoscaler.Keda, minReplicas)
	case v1beta1.AutoscalerHPA:
		return validateHPAAutoscaler(autoscaler.HPA)
	}
	return nil
}

// validateKedaAutoscaler checks a class=keda block: at least one trigger, an
// idle replica count below the Component's minReplicas when both are set,
// and a requested HPA name that is a DNS label.
func validateKedaAutoscaler(keda *v1beta1.KedaAutoscaler, minReplicas *int) error {
	if keda == nil || len(keda.Triggers) == 0 {
		return fmt.Errorf(
			"%s: class=keda requires at least 1 trigger",
			ReasonKedaTriggersRequired,
		)
	}
	if keda.IdleReplicaCount != nil && minReplicas != nil {
		idle, minimum := int(*keda.IdleReplicaCount), *minReplicas
		if idle >= minimum {
			return fmt.Errorf(
				"%s: keda.idleReplicaCount must be < minReplicas (got idleReplicaCount=%d, minReplicas=%d)",
				ReasonKedaIdleBelowMin, idle, minimum,
			)
		}
	}
	return ValidateKedaHPAName(keda.Advanced)
}

// validateHPAAutoscaler checks a class=hpa block: every metrics entry names
// an accepted source type and carries the source that type reads. An absent
// block is valid; the controller then renders its default HPA.
func validateHPAAutoscaler(hpa *v1beta1.HPAAutoscaler) error {
	if hpa == nil {
		return nil
	}
	for i, m := range hpa.Metrics {
		if err := validateHPAMetricSpec(i, m); err != nil {
			return err
		}
	}
	return nil
}

// ValidateKedaHPAName checks advanced.horizontalPodAutoscalerConfig.name, the
// HPA name a KEDA block asks for. KEDA's admission rejects a ScaledObject
// whose HPA name is not a DNS label, so OME rejects the spec up front. An
// unset name is valid: OME and KEDA choose one.
func ValidateKedaHPAName(advanced *kedav1.AdvancedConfig) error {
	if advanced == nil || advanced.HorizontalPodAutoscalerConfig == nil || advanced.HorizontalPodAutoscalerConfig.Name == "" {
		return nil
	}
	name := advanced.HorizontalPodAutoscalerConfig.Name
	if errs := k8svalidation.IsDNS1123Label(name); len(errs) > 0 {
		return fmt.Errorf(
			"%s: keda.advanced.horizontalPodAutoscalerConfig.name %q is not a valid HPA name: %s",
			ReasonKedaHPANameInvalid, name, strings.Join(errs, "; "),
		)
	}
	return nil
}

// hpaMetricSource is one metric source type the HPA accepts: the field of the
// MetricSpec that carries the source and the name a refusal reports.
type hpaMetricSource struct {
	kind  autoscalingv2.MetricSourceType
	field string
	set   func(*autoscalingv2.MetricSpec) bool
}

// hpaMetricSources lists the accepted source types in the order the refusals
// name them.
var hpaMetricSources = []hpaMetricSource{
	{kind: autoscalingv2.ResourceMetricSourceType, field: "resource", set: func(m *autoscalingv2.MetricSpec) bool { return m.Resource != nil }},
	{kind: autoscalingv2.PodsMetricSourceType, field: "pods", set: func(m *autoscalingv2.MetricSpec) bool { return m.Pods != nil }},
	{kind: autoscalingv2.ObjectMetricSourceType, field: "object", set: func(m *autoscalingv2.MetricSpec) bool { return m.Object != nil }},
	{kind: autoscalingv2.ExternalMetricSourceType, field: "external", set: func(m *autoscalingv2.MetricSpec) bool { return m.External != nil }},
	{kind: autoscalingv2.ContainerResourceMetricSourceType, field: "containerResource", set: func(m *autoscalingv2.MetricSpec) bool { return m.ContainerResource != nil }},
}

// hpaMetricTypeNames is the accepted source types as a refusal lists them.
var hpaMetricTypeNames = func() string {
	names := make([]string, 0, len(hpaMetricSources))
	for _, source := range hpaMetricSources {
		names = append(names, string(source.kind))
	}
	return strings.Join(names, "|")
}()

// validateHPAMetricSpec checks that one metrics entry names an accepted source
// type and carries the source that type reads. The refusal names the entry's
// index.
func validateHPAMetricSpec(idx int, m autoscalingv2.MetricSpec) error {
	if m.Type == "" {
		return fmt.Errorf("%s: hpa.metrics[%d].type is empty; must be one of %s", ReasonHPAMetricMalformed, idx, hpaMetricTypeNames)
	}
	for _, source := range hpaMetricSources {
		if source.kind != m.Type {
			continue
		}
		if !source.set(&m) {
			return fmt.Errorf("%s: hpa.metrics[%d].type=%s but hpa.metrics[%d].%s is nil", ReasonHPAMetricMalformed, idx, source.kind, idx, source.field)
		}
		return nil
	}
	return fmt.Errorf("%s: hpa.metrics[%d].type %q is not one of %s", ReasonHPAMetricMalformed, idx, m.Type, hpaMetricTypeNames)
}

// ValidateAutoscalerAnnotationConflict rejects an InferenceService that
// declares the legacy ome.io/autoscalerClass annotation AND a new
// Autoscaler block on any Component. The
// per-Component Autoscaler block wins; this validator preempts confusion
// by forcing the operator to pick one.
//
// Returns nil when the annotation is absent OR when no Component
// carries an Autoscaler block.
func ValidateAutoscalerAnnotationConflict(isvc *v1beta1.InferenceService) error {
	if isvc == nil {
		return nil
	}
	if _, has := isvc.Annotations[constants.AutoscalerClass]; !has {
		return nil
	}
	conflicting := []v1beta1.ComponentType{}
	for _, r := range declaredRoles(&isvc.Spec) {
		if r.extension.Autoscaler != nil {
			conflicting = append(conflicting, r.kind)
		}
	}
	if len(conflicting) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%s: %s annotation is present together with spec.%s.autoscaler; "+
			"remove the annotation — per-Component autoscaler block wins",
		ReasonAutoscalerAnnotationConflict,
		constants.AutoscalerClass, conflicting[0],
	)
}

// ValidateLegacyAutoscalerFieldsRaw inspects the raw admission-request
// JSON for the legacy scaleTarget / scaleMetric fields and returns a
// friendly migration error if either is set on any Component
// (spec.engine, spec.decoder, spec.router, spec.engineConfig.*, etc).
//
// ComponentExtensionSpec has no such fields and the generated CRD's
// OpenAPI schema does not carry them, so a kubectl with --validate=true
// against an up-to-date CRD rejects them server-side before they reach
// the webhook, and the API server's structural-schema pruning silently
// drops them when --validate=false. This helper exists as
// defense-in-depth for the narrow window where:
//
//   - An operator's local CRD is stale (cluster updated, kubeconfig
//     points at older CRD), AND
//   - The client bypasses --validate, AND
//   - Field pruning fails for any reason (custom admission, intermediate
//     proxy, etc).
//
// In all three cases the field would silently no-op rather than reject;
// this helper surfaces the migration message at admission time so the
// operator gets a clear pointer at the Autoscaler block.
//
// rawObj is typically req.AdmissionRequest.Object.Raw from the
// admission.Request — the raw JSON bytes BEFORE the Go decoder strips
// unknown fields. Returns nil when rawObj is empty / unparsable (the
// caller's primary validation will surface the parse failure) and when
// no legacy fields are populated on any Component.
func ValidateLegacyAutoscalerFieldsRaw(rawObj []byte) error {
	if len(rawObj) == 0 {
		return nil
	}
	// Every role's block, on a service (keyed by the role) or a runtime
	// (keyed by its piece), is a member of spec.
	var envelope struct {
		Spec map[string]json.RawMessage `json:"spec,omitempty"`
	}
	if err := json.Unmarshal(rawObj, &envelope); err != nil {
		// The primary decoder will surface a JSON parse failure; this
		// helper is best-effort defense-in-depth and must not mask the
		// real error.
		return nil
	}
	names := make([]string, 0, 2*len(v1beta1.Roles))
	for _, r := range v1beta1.Roles {
		names = append(names, string(r.Type))
	}
	for _, r := range v1beta1.Roles {
		names = append(names, r.RuntimeField)
	}
	for _, name := range names {
		body := envelope.Spec[name]
		if len(body) == 0 {
			continue
		}
		if field, ok := legacyAutoscalerFieldOn(body); ok {
			return fmt.Errorf(
				"%s: spec.%s.%s is no longer supported. "+
					"Use spec.%s.autoscaler.%s instead.",
				ReasonLegacyAutoscalerFieldsRejected,
				name, field,
				name, legacyAutoscalerReplacement(field),
			)
		}
	}
	return nil
}

// legacyAutoscalerFieldOn checks a Component's raw JSON for the deleted
// legacy autoscaling fields. Returns the offending field name + true on
// the first match, else "" + false. Both fields are pointer types
// (*int / *ScaleMetric) in the old API surface, so a null value counts
// as "not set" — we only reject explicit non-null values.
func legacyAutoscalerFieldOn(componentBody json.RawMessage) (string, bool) {
	var probe struct {
		ScaleTarget *json.RawMessage `json:"scaleTarget,omitempty"`
		ScaleMetric *json.RawMessage `json:"scaleMetric,omitempty"`
	}
	if err := json.Unmarshal(componentBody, &probe); err != nil {
		return "", false
	}
	if isNonNullRaw(probe.ScaleTarget) {
		return "scaleTarget", true
	}
	if isNonNullRaw(probe.ScaleMetric) {
		return "scaleMetric", true
	}
	return "", false
}

// isNonNullRaw returns true when the raw message is set AND not the
// literal `null`. JSON `null` for a pointer field is the operator's
// explicit "unset" marker and must not trigger the rejection.
func isNonNullRaw(raw *json.RawMessage) bool {
	if raw == nil {
		return false
	}
	trimmed := string(*raw)
	return trimmed != "" && trimmed != "null"
}

// legacyAutoscalerReplacement maps a deleted field name to its
// Autoscaler-block equivalent so the migration error points the
// operator at the right replacement.
func legacyAutoscalerReplacement(field string) string {
	switch field {
	case "scaleTarget":
		return "hpa.metrics[0].resource.target.averageUtilization"
	case "scaleMetric":
		return "hpa.metrics[0].resource.name"
	default:
		return "<see Autoscaler block>"
	}
}

// AutoscalerPolicy ref validation reason constants. Operators and the
// envtest suite reference these by name, so they're exported and stable.
const (
	// ReasonAutoscalerPolicyRefKindReserved rejects a per-Component
	// autoscalerPolicyRef whose Kind is anything other than
	// "AutoscalerPolicy". "ClusterAutoscalerPolicy" is a reserved shape:
	// rejecting it outright means no stored ISVC can carry a kind an
	// older controller would misread as a namespaced policy.
	ReasonAutoscalerPolicyRefKindReserved = "AutoscalerPolicyRefKindReserved"

	// ReasonAutoscalerPolicyFeatureDisabled rejects any per-Component
	// autoscalerPolicyRef on a cluster where the AutoscalerPolicy feature
	// is not enabled. Admitting the ref would leave the component
	// permanently held fail-closed with no actor able to render it, so
	// the honest answer is a rejection at write time.
	ReasonAutoscalerPolicyFeatureDisabled = "AutoscalerPolicyFeatureDisabled"
)

// autoscalerPolicyRefCheck names one declared Component's policy ref so
// the ref validators can report the exact spec field path.
type autoscalerPolicyRefCheck struct {
	name string
	ref  *v1beta1.AutoscalerPolicyRef
}

// isvcAutoscalerPolicyRefChecks projects each declared Component (Engine,
// Decoder, Router) into the per-Component ref slice. Nil Components and
// nil refs are skipped so an absent ref never produces a spurious error.
func isvcAutoscalerPolicyRefChecks(isvc *v1beta1.InferenceService) []autoscalerPolicyRefCheck {
	if isvc == nil {
		return nil
	}
	var checks []autoscalerPolicyRefCheck
	for _, r := range declaredRoles(&isvc.Spec) {
		if r.extension.AutoscalerPolicyRef != nil {
			checks = append(checks, autoscalerPolicyRefCheck{string(r.kind), r.extension.AutoscalerPolicyRef})
		}
	}
	return checks
}

// ValidateAutoscalerPolicyRefs validates every per-Component
// autoscalerPolicyRef on the InferenceService:
//
//  1. Any ref is rejected while the AutoscalerPolicy feature is disabled
//     (featureEnabled=false) — the resolver would hold the component
//     fail-closed forever, so admission refuses the write honestly.
//  2. Kind must be "AutoscalerPolicy" (or empty, which defaults to it).
//     "ClusterAutoscalerPolicy" is a reserved shape with no consumer.
//
// Returns nil when no Component carries a ref.
func ValidateAutoscalerPolicyRefs(isvc *v1beta1.InferenceService, featureEnabled bool) error {
	for _, c := range isvcAutoscalerPolicyRefChecks(isvc) {
		if !featureEnabled {
			return fmt.Errorf(
				"%s: spec.%s.autoscalerPolicyRef references %q but the AutoscalerPolicy feature is not enabled on this cluster; remove the ref or enable the feature",
				ReasonAutoscalerPolicyFeatureDisabled, c.name, c.ref.Name,
			)
		}
		if c.ref.Kind != "" && c.ref.Kind != constants.AutoscalerPolicyKind {
			return fmt.Errorf(
				"%s: spec.%s.autoscalerPolicyRef.kind %q is not supported (ClusterAutoscalerPolicy is a reserved shape); use %s",
				ReasonAutoscalerPolicyRefKindReserved, c.name, c.ref.Kind, constants.AutoscalerPolicyKind,
			)
		}
	}
	return nil
}

// AutoscalerPolicySplitCeilingWarning returns a webhook warning (never a
// rejection) when placement splits replicas and a Component that references
// an AutoscalerPolicy has no per-cluster cap. A component is capped by its
// split.maxPerCluster key or default, or by the primary component's cap, which
// scales to the others. Policy templates render against each cluster's derived
// replica bounds; without a per-cluster cap that ceiling is unbounded, so a
// fail-to-max fallback on one cluster can fabricate the whole fleet's count.
// Empty string when the combination is absent.
func AutoscalerPolicySplitCeilingWarning(isvc *v1beta1.InferenceService) string {
	if isvc == nil || isvc.Spec.Placement == nil || (isvc.Spec.Placement.Mode != v1beta1.PlacementModeSplit && isvc.Spec.Placement.Mode != v1beta1.PlacementModeSplitByCapacity) {
		return ""
	}
	if affinity.UsesRatios(isvc.Spec.Placement.ClusterAffinity) {
		return ratioFormCeilingWarning(isvc)
	}
	split := isvc.Spec.Placement.Split
	primary := v1beta1.EngineComponent
	if isvc.Spec.Engine == nil && isvc.Spec.Decoder != nil {
		primary = v1beta1.DecoderComponent
	}
	if split.Ceiling(primary) > 0 {
		return ""
	}
	referencing := policyReferrersLacking(isvc, func(component string) bool { return split.Ceiling(v1beta1.ComponentType(component)) == 0 })
	if len(referencing) == 0 {
		return ""
	}
	return fmt.Sprintf(
		"spec.placement.mode=%s with autoscalerPolicyRef on %s but spec.placement.split.maxPerCluster caps none of them; "+
			"policy templates render against each home's derived bounds, and without a per-cluster cap that ceiling is unbounded (%s)",
		isvc.Spec.Placement.Mode, strings.Join(referencing, ", "), v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling,
	)
}

// policyReferrersLacking lists the Components that reference an
// AutoscalerPolicy and lack what the test asks for, in declaration order.
func policyReferrersLacking(isvc *v1beta1.InferenceService, lacks func(component string) bool) []string {
	var referencing []string
	for _, c := range isvcAutoscalerPolicyRefChecks(isvc) {
		if lacks(c.name) {
			referencing = append(referencing, c.name)
		}
	}
	return referencing
}

// ratioFormCeilingWarning covers a ratio-form source, whose per-cluster
// ceiling is a share of the component's maxReplicas: a component that
// references an AutoscalerPolicy without maxReplicas has no ceiling to share.
func ratioFormCeilingWarning(isvc *v1beta1.InferenceService) string {
	maxReplicas := map[string]int{}
	for _, r := range declaredRoles(&isvc.Spec) {
		maxReplicas[string(r.kind)] = r.extension.MaxReplicas
	}
	referencing := policyReferrersLacking(isvc, func(component string) bool { return maxReplicas[component] == 0 })
	if len(referencing) == 0 {
		return ""
	}
	return fmt.Sprintf(
		"spec.placement.mode=%s with replica ratios and autoscalerPolicyRef on %s but no maxReplicas on them; "+
			"each cluster's ceiling is its share of maxReplicas, and without maxReplicas that ceiling is unbounded (%s)",
		isvc.Spec.Placement.Mode, strings.Join(referencing, ", "), v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling,
	)
}

// ValidateComponentAutoscaler is a back-compat thin wrapper around
// ValidateAutoscaler that pulls the Autoscaler block + MinReplicas out
// of a ComponentExtensionSpec. New callers should prefer
// ValidateAutoscaler (or ValidateComponentsAutoscalers for the
// per-Component dispatch) so they don't have to synthesize a parent
// ComponentExtensionSpec (cf. the IR webhook, which carries Replicas
// on the IR spec rather than a nested ComponentExtensionSpec).
func ValidateComponentAutoscaler(componentExt *v1beta1.ComponentExtensionSpec) error {
	if componentExt == nil {
		return nil
	}
	return ValidateAutoscaler(componentExt.Autoscaler, componentExt.MinReplicas)
}
