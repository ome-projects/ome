package runtimeselector

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/validation"
)

// DefaultRuntimeMatcher implements RuntimeMatcher with comprehensive compatibility checking.
type DefaultRuntimeMatcher struct {
	config *Config
}

func NewDefaultRuntimeMatcher(config *Config) RuntimeMatcher {
	return &DefaultRuntimeMatcher{
		config: config,
	}
}

func (m *DefaultRuntimeMatcher) IsCompatible(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, runtimeName string) (bool, error) {
	report := m.evaluate(runtime, model, isvc)
	return report.IsCompatible, report.serviceRefusal(runtimeName, model)
}

func (m *DefaultRuntimeMatcher) GetCompatibilityDetails(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, _ string) (*CompatibilityReport, error) {
	return m.evaluate(runtime, model, isvc), nil
}

// evaluate is the one compatibility rule. The checks run in order and the
// first failure ends the report: the runtime is enabled, it declares every
// accelerator class the service asks for, its engine and decoder deployment
// modes agree with the service's, one of its supported formats fits the
// model, and the model's size lies in its size range.
func (m *DefaultRuntimeMatcher) evaluate(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) *CompatibilityReport {
	report := &CompatibilityReport{
		IncompatibilityReasons: []string{},
		Warnings:               []string{},
	}

	if runtime.IsDisabled() {
		return report.incompatible(runtimeDisabled, "runtime is disabled")
	}
	if !m.compareAcceleratorClass(runtime, isvc) {
		return report.incompatible(acceleratorClassNotServed, "runtime does not support the required accelerator class")
	}
	if ok, reason := m.compareDeploymentMode(runtime, isvc); !ok {
		return report.incompatible(deploymentModeMismatch, reason)
	}

	format, mismatches := m.firstSupportedFormat(runtime, model)
	if format == nil {
		return report.incompatible(modelFormatNotSupported, formatNotSupportedReason(model, mismatches))
	}
	report.MatchDetails = m.evaluateFormatMatch(model, *format)

	if model.ModelParameterSize != nil && runtime.ModelSizeRange != nil &&
		!modelSizeInRange(*model.ModelParameterSize, runtime.ModelSizeRange) {
		report.MatchDetails.SizeMatch = false
		return report.incompatible(modelSizeOutOfRange,
			fmt.Sprintf("model size %s is outside supported range %s",
				*model.ModelParameterSize, modelSizeRangeLabel(runtime.ModelSizeRange)))
	}

	report.IsCompatible = true

	// AutoSelect=false makes the runtime ineligible for auto-selection but
	// the runtime can still be picked explicitly via spec.runtime.name —
	// hence "warning", not "incompatible".
	if !runtimeHasAutoSelectFormat(runtime) {
		report.Warnings = append(report.Warnings,
			"runtime does not have auto-select enabled for any supported format")
	}

	if model.ModelParameterSize == nil && runtime.ModelSizeRange != nil {
		report.Warnings = append(report.Warnings,
			"model does not specify size, but runtime has size constraints")
	}

	return report
}

// incompatible records the check the runtime failed and its reason, and
// returns the report.
func (r *CompatibilityReport) incompatible(check compatibilityCheck, reason string) *CompatibilityReport {
	r.failedCheck = check
	r.IncompatibilityReasons = append(r.IncompatibilityReasons, reason)
	return r
}

// serviceRefusal is the typed error IsCompatible and ValidateRuntime return
// when the runtime will not serve the service at all: a RuntimeDisabledError
// for a disabled runtime, a RuntimeCompatibilityError for one that does not
// declare the service's accelerator class or deployment mode. It is nil for
// a compatible report and for a model the runtime's formats or size range
// do not fit.
func (r *CompatibilityReport) serviceRefusal(runtimeName string, model *v1beta1.BaseModelSpec) error {
	switch r.failedCheck {
	case runtimeDisabled:
		return &RuntimeDisabledError{RuntimeName: runtimeName}
	case acceleratorClassNotServed, deploymentModeMismatch:
		return &RuntimeCompatibilityError{
			RuntimeName: runtimeName,
			ModelFormat: model.ModelFormat.Name,
			Reason:      r.IncompatibilityReasons[0],
		}
	}
	return nil
}

// firstSupportedFormat returns the first supported format the model fits,
// or nil with each scanned format's reason for not fitting it.
func (m *DefaultRuntimeMatcher) firstSupportedFormat(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec) (*v1beta1.SupportedModelFormat, []string) {
	var mismatches []string
	for i := range runtime.SupportedModelFormats {
		format := &runtime.SupportedModelFormats[i]
		if m.compareSupportedModelFormats(model, *format) {
			return format, nil
		}
		mismatches = append(mismatches, m.getFormatMismatchReason(model, *format))
	}
	return nil, mismatches
}

// formatNotSupportedReason is the report's reason when no supported format
// fits the model: the model's format label with each format's own reason,
// or, when the runtime declares no formats, that it declares none.
func formatNotSupportedReason(model *v1beta1.BaseModelSpec, mismatches []string) string {
	if len(mismatches) == 0 {
		return fmt.Sprintf("model format '%s' not in supported formats: no supported formats defined",
			getModelFormatLabel(model))
	}
	return fmt.Sprintf("model format '%s' not in supported formats: %s",
		getModelFormatLabel(model), strings.Join(mismatches, "; "))
}

// evaluateFormatMatch runs only after compareSupportedModelFormats has
// already confirmed compatibility, so the cross-cutting matches (cache
// provider, diffusion pipeline, architecture, quantization) start true
// and the reason-collection branches act as defense in depth in case a
// future caller invokes this directly.
func (m *DefaultRuntimeMatcher) evaluateFormatMatch(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat) MatchDetails {
	match := MatchDetails{
		ArchitectureMatch:       true,
		DiffusionPipelineMatch:  true,
		QuantizationMatch:       true,
		ModelCacheProviderMatch: true,
		SizeMatch:               true,
		Priority:                m.config.DefaultPriority,
		Reasons:                 []string{},
	}

	if format.Priority != nil {
		match.Priority = *format.Priority
	}
	if format.AutoSelect != nil {
		match.AutoSelectEnabled = *format.AutoSelect
	}

	if validation.ModelRequiresCacheProvider(model) {
		match.ModelCacheProviderMatch = validation.FormatSupportsModelCacheProvider(format, m.config.ModelCacheProvider)
		if !match.ModelCacheProviderMatch {
			match.Reasons = append(match.Reasons, validation.ModelCacheProviderMismatchReason(m.config.ModelCacheProvider, format))
		}
	}

	pipelineMatch, pipelineReason := validation.CompareDiffusionPipeline(model.DiffusionPipeline, format.DiffusionPipeline)
	match.DiffusionPipelineMatch = pipelineMatch
	if !pipelineMatch && pipelineReason != "" {
		match.Reasons = append(match.Reasons, pipelineReason)
	}

	match.ArchitectureMatch = optionalEqual(model.ModelArchitecture, format.ModelArchitecture)
	if !match.ArchitectureMatch {
		if model.ModelArchitecture != nil && format.ModelArchitecture != nil {
			match.Reasons = append(match.Reasons,
				fmt.Sprintf("architecture mismatch: model=%s, runtime=%s",
					*model.ModelArchitecture, *format.ModelArchitecture))
		} else {
			match.Reasons = append(match.Reasons, "architecture requirement mismatch")
		}
	}

	match.QuantizationMatch = optionalEqual(model.Quantization, format.Quantization)
	if !match.QuantizationMatch {
		if model.Quantization != nil && format.Quantization != nil {
			match.Reasons = append(match.Reasons,
				fmt.Sprintf("quantization mismatch: model=%s, runtime=%s",
					*model.Quantization, *format.Quantization))
		} else {
			match.Reasons = append(match.Reasons, "quantization requirement mismatch")
		}
	}

	if format.ModelFormat != nil && format.ModelFormat.Name == model.ModelFormat.Name {
		match.FormatMatch = matchOptionalVersions(model.ModelFormat.Version, format.ModelFormat.Version, func() bool {
			return m.compareModelFormatVersions(format.ModelFormat, &model.ModelFormat)
		})
		if match.FormatMatch && format.ModelFormat.Weight > 0 {
			match.Weight += format.ModelFormat.Weight * int64(match.Priority)
		}
	}

	if format.ModelFramework != nil && model.ModelFramework != nil {
		if format.ModelFramework.Name == model.ModelFramework.Name {
			match.FrameworkMatch = matchOptionalVersions(model.ModelFramework.Version, format.ModelFramework.Version, func() bool {
				return m.compareModelFrameworkVersions(format.ModelFramework, model.ModelFramework)
			})
			if match.FrameworkMatch && format.ModelFramework.Weight > 0 {
				match.Weight += format.ModelFramework.Weight * int64(match.Priority)
			}
		}
	} else if format.ModelFramework == nil && model.ModelFramework == nil {
		match.FrameworkMatch = true
	}

	return match
}

// compareAcceleratorClass checks if the runtime supports the required accelerator class.
func (m *DefaultRuntimeMatcher) compareAcceleratorClass(runtime *v1beta1.ServingRuntimeSpec, isvc *v1beta1.InferenceService) bool {
	// if inferenceService is nil, we assume no accelerator requirement
	if isvc == nil {
		return true
	}

	// Collect all unique accelerator requirements from the InferenceService
	requiredClasses := make(map[string]struct{})
	if class, ok := isvc.Annotations[constants.AcceleratorClassAnnotationKey]; ok {
		requiredClasses[class] = struct{}{}
	}
	if isvc.Spec.AcceleratorSelector != nil && isvc.Spec.AcceleratorSelector.AcceleratorClass != nil {
		requiredClasses[*isvc.Spec.AcceleratorSelector.AcceleratorClass] = struct{}{}
	}
	for _, role := range v1beta1.ServiceRoles(&isvc.Spec).Declared() {
		if role.AcceleratorOverride != nil && role.AcceleratorOverride.AcceleratorClass != nil {
			requiredClasses[*role.AcceleratorOverride.AcceleratorClass] = struct{}{}
		}
	}

	// If ISVC has no accelerator requirements, it's compatible from this perspective.
	if len(requiredClasses) == 0 {
		return true
	}

	// If ISVC has requirements, the runtime must support them.
	if runtime.AcceleratorRequirements == nil || len(runtime.AcceleratorRequirements.AcceleratorClasses) == 0 {
		return false // Runtime supports no accelerators, but ISVC requires one.
	}

	supportedClasses := runtime.AcceleratorRequirements.AcceleratorClasses
	for reqClass := range requiredClasses {
		if !slices.Contains(supportedClasses, reqClass) {
			return false
		}
	}

	return true
}

// compareSupportedModelFormats asks the shared compatibility rule whether the model fits the
// format, with the cluster's model cache provider applied: a model that loads through an
// external model cache fits only a format that lists the provider, and an empty provider name
// fits no such model.
func (m *DefaultRuntimeMatcher) compareSupportedModelFormats(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat) bool {
	return validation.CompareModelToFormat(model, format, validation.WithModelCacheProvider(m.config.ModelCacheProvider))
}

// getFormatMismatchReason is the shared rule's reason why the model does not fit the format,
// such as "architecture mismatch (model=X, runtime=Y)"; several reasons are joined by ", ".
func (m *DefaultRuntimeMatcher) getFormatMismatchReason(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat) string {
	return validation.GetFormatMismatchReason(model, format, validation.WithModelCacheProvider(m.config.ModelCacheProvider))
}

func (m *DefaultRuntimeMatcher) compareModelFormatVersions(supportedFormat *v1beta1.ModelFormat, modelFormat *v1beta1.ModelFormat) bool {
	return validation.VersionsMatch(*modelFormat.Version, *supportedFormat.Version, supportedFormat.Operator)
}

func (m *DefaultRuntimeMatcher) compareModelFrameworkVersions(supportedFramework *v1beta1.ModelFrameworkSpec, modelFramework *v1beta1.ModelFrameworkSpec) bool {
	return validation.VersionsMatch(*modelFramework.Version, *supportedFramework.Version, supportedFramework.Operator)
}

// modelSizeInRange reports whether the parsed model size falls within the
// runtime's ModelSizeRange. Min and Max are each +optional in the CRD: a nil
// Min means "no lower bound" and a nil Max means "no upper bound", so a range
// with only one bound set constrains only that side rather than panicking on a
// nil dereference. Callers must guard against a nil sizeRange themselves.
func modelSizeInRange(modelSizeStr string, sizeRange *v1beta1.ModelSizeRangeSpec) bool {
	modelSize := parseModelSize(modelSizeStr)
	if sizeRange.Min != nil && modelSize < parseModelSize(*sizeRange.Min) {
		return false
	}
	if sizeRange.Max != nil && modelSize > parseModelSize(*sizeRange.Max) {
		return false
	}
	return true
}

// modelSizeRangeLabel renders a ModelSizeRange for error messages, using an
// open-interval bracket for whichever bound is unset, e.g. "[1B, 13B]",
// "[1B, inf)", or "(-inf, 13B]". Safe to call with nil Min/Max.
func modelSizeRangeLabel(sizeRange *v1beta1.ModelSizeRangeSpec) string {
	lower := "(-inf"
	if sizeRange.Min != nil {
		lower = "[" + *sizeRange.Min
	}
	upper := "inf)"
	if sizeRange.Max != nil {
		upper = *sizeRange.Max + "]"
	}
	return fmt.Sprintf("%s, %s", lower, upper)
}

func runtimeHasAutoSelectFormat(runtime *v1beta1.ServingRuntimeSpec) bool {
	return slices.ContainsFunc(runtime.SupportedModelFormats, autoSelectEnabled)
}

// optionalEqual treats "both nil" and "both equal" as matches; a mixed
// presence (one side requires the field, the other doesn't specify it) is
// considered a mismatch — opposite of standard pointer equality.
func optionalEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// matchOptionalVersions: both versions set -> compare via cmp; both nil ->
// match; exactly one set -> no match. Shared between ModelFormat.Version and
// ModelFrameworkSpec.Version.
func matchOptionalVersions(modelVersion, supportedVersion *string, cmp func() bool) bool {
	if modelVersion != nil && supportedVersion != nil {
		return cmp()
	}
	return modelVersion == nil && supportedVersion == nil
}

// parseModelSize converts the HuggingFace-style parameter count suffix
// ("7B", "350M", "1.5T") to a raw count. Returns 0 on parse error so callers
// can treat unknown sizes as out-of-range without panicking.
func parseModelSize(sizeStr string) float64 {
	var multiplier float64 = 1

	switch {
	case strings.HasSuffix(sizeStr, "T"):
		multiplier = 1_000_000_000_000
		sizeStr = strings.TrimSuffix(sizeStr, "T")
	case strings.HasSuffix(sizeStr, "B"):
		multiplier = 1_000_000_000
		sizeStr = strings.TrimSuffix(sizeStr, "B")
	case strings.HasSuffix(sizeStr, "M"):
		multiplier = 1_000_000
		sizeStr = strings.TrimSuffix(sizeStr, "M")
	}

	size, err := strconv.ParseFloat(sizeStr, 64)
	if err != nil {
		return 0
	}

	return size * multiplier
}

// getModelFormatLabel composes the "mt:" label used in user-facing error
// messages to identify a model's format/architecture/framework signature.
func getModelFormatLabel(model *v1beta1.BaseModelSpec) string {
	label := "mt:" + model.ModelFormat.Name
	if model.ModelFormat.Version != nil {
		label += ":" + *model.ModelFormat.Version
	}
	if model.ModelArchitecture != nil {
		label += ":" + *model.ModelArchitecture
	}
	if model.Quantization != nil {
		label += ":" + string(*model.Quantization)
	}
	if model.ModelFramework != nil {
		label += ":" + model.ModelFramework.Name
		if model.ModelFramework.Version != nil {
			label += ":" + *model.ModelFramework.Version
		}
	}
	return label
}

// compareDeploymentMode rejects a runtime only when both the InferenceService
// component and the matching runtime component explicitly declare different
// deployment modes. Only the roles that run the model are compared.
func (m *DefaultRuntimeMatcher) compareDeploymentMode(runtime *v1beta1.ServingRuntimeSpec, isvc *v1beta1.InferenceService) (bool, string) {
	for _, role := range v1beta1.Roles {
		if !role.RunsModel {
			continue
		}
		isvcMode, hasIsvcMode := inferenceServiceRoleDeploymentMode(isvc, role.Type)
		runtimeMode, hasRuntimeMode := getRuntimeComponentDeploymentMode(runtime, role.Type)
		if ok, reason := compareComponentDeploymentMode(string(role.Type), isvcMode, hasIsvcMode, runtimeMode, hasRuntimeMode); !ok {
			return false, reason
		}
	}
	return true, ""
}

func compareComponentDeploymentMode(
	componentName string,
	isvcDeploymentMode constants.DeploymentModeType,
	hasIsvcDeploymentMode bool,
	runtimeDeploymentMode constants.DeploymentModeType,
	hasRuntimeDeploymentMode bool) (bool, string) {
	// Keep the runtime unless both sides explicitly declare deployment modes and those values differ.
	if !hasIsvcDeploymentMode || !hasRuntimeDeploymentMode || isvcDeploymentMode == runtimeDeploymentMode {
		return true, ""
	}

	return false, fmt.Sprintf("runtime %s deployment mode %s does not match requested %s deployment mode %s",
		componentName, runtimeDeploymentMode, componentName, isvcDeploymentMode)
}

// inferenceServiceRoleDeploymentMode returns the deployment mode the
// InferenceService explicitly declares for role c: the per-component
// ome.io/deploymentMode annotation when present, else the typed
// spec.deploymentMode (which applies to every component). The engine always
// renders, so for it the typed field counts even when spec.engine is absent.
// Any other role reports nothing when the InferenceService does not declare
// it — a runtime piece's mode must not filter services that will never
// deploy that role.
func inferenceServiceRoleDeploymentMode(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (constants.DeploymentModeType, bool) {
	if isvc == nil {
		return "", false
	}
	view := v1beta1.SpecOf(&isvc.Spec, c)
	if view == nil && c != v1beta1.EngineComponent {
		return "", false
	}
	if view != nil {
		if mode, found := deploymentModeFromAnnotations(view.Extension.Annotations); found {
			return mode, true
		}
	}
	return deploymentModeFromSpecField(isvc.Spec.DeploymentMode)
}

func getRuntimeComponentDeploymentMode(runtime *v1beta1.ServingRuntimeSpec, componentType v1beta1.ComponentType) (constants.DeploymentModeType, bool) {
	piece := v1beta1.PieceOf(runtime, componentType)
	if piece == nil {
		return "", false
	}
	return deploymentModeFromAnnotations(piece.Extension.Annotations)
}

func deploymentModeFromAnnotations(annotations map[string]string) (constants.DeploymentModeType, bool) {
	if annotations == nil {
		return "", false
	}
	if mode, exists := annotations[constants.DeploymentMode]; exists {
		deploymentMode := constants.DeploymentModeType(mode)
		if deploymentMode.IsValid() {
			return deploymentMode, true
		}
	}
	return "", false
}

func deploymentModeFromSpecField(mode *constants.DeploymentModeType) (constants.DeploymentModeType, bool) {
	if mode != nil && mode.IsValid() {
		return *mode, true
	}
	return "", false
}
