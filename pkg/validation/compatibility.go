package validation

import (
	"fmt"
	"strings"

	modelVer "sigs.k8s.io/ome/pkg/modelver"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// CompatibilityOptions configures model-to-format compatibility checks.
type CompatibilityOptions struct {
	// ModelCacheProvider names the model cache provider the cluster runs. nil skips the model
	// cache check; an empty name means the cluster runs none.
	ModelCacheProvider *string
}

// CompatibilityOption is a functional option for CompareModelToFormat and
// GetFormatMismatchReason.
type CompatibilityOption func(*CompatibilityOptions)

// WithModelCacheProvider names the model cache provider the cluster runs. With it, a model
// that loads through an external model cache fits only a format that lists that provider, and
// an empty name fits no such model. Without it the model cache check is skipped, which suits
// offline validation that has no cluster config.
func WithModelCacheProvider(provider string) CompatibilityOption {
	return func(o *CompatibilityOptions) { o.ModelCacheProvider = &provider }
}

func buildOptions(opts []CompatibilityOption) CompatibilityOptions {
	var o CompatibilityOptions
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// CompareModelToFormat reports whether a model fits one supported format of a runtime. The
// runtime selector and admission both read this one rule.
func CompareModelToFormat(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat, opts ...CompatibilityOption) bool {
	ok, _ := formatMismatches(model, format, buildOptions(opts))
	return ok
}

// GetFormatMismatchReason names every rule the model fails, joined by ", ". A model that fails
// for no named reason reads "unknown mismatch".
func GetFormatMismatchReason(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat, opts ...CompatibilityOption) string {
	_, reasons := formatMismatches(model, format, buildOptions(opts))
	if len(reasons) == 0 {
		return "unknown mismatch"
	}
	return strings.Join(reasons, ", ")
}

// formatMismatches runs every rule a model must pass to fit a supported format and collects
// the reasons of the failed ones in reporting order: the model cache provider when the caller
// names one, the diffusion pipeline, then the attributes of formatAttributes. A failed rule may
// add no reason, so ok can be false with no reasons.
func formatMismatches(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat, options CompatibilityOptions) (ok bool, reasons []string) {
	ok = true
	fail := func(reason string) {
		ok = false
		if reason != "" {
			reasons = append(reasons, reason)
		}
	}
	if provider := options.ModelCacheProvider; provider != nil &&
		ModelRequiresCacheProvider(model) && !FormatSupportsModelCacheProvider(format, *provider) {
		fail(ModelCacheProviderMismatchReason(*provider, format))
	}
	if pipelineOK, reason := CompareDiffusionPipeline(model.DiffusionPipeline, format.DiffusionPipeline); !pipelineOK {
		fail(reason)
	}
	for _, attribute := range formatAttributes {
		if attributeOK, reason := attribute.compare(model, format); !attributeOK {
			fail(reason)
		}
	}
	return ok, reasons
}

// optionalAttribute is one attribute of a model that a supported format may constrain and that
// either side may leave unset. Both unset, or both set and equal, is a match. Each of the three
// other outcomes has its own reason text, in which {model} and {runtime} stand for the two
// values; a text may leave either out, and an empty text adds nothing to the reason.
type optionalAttribute struct {
	// model and runtime read the attribute from each side; set is false when that side leaves
	// it unset.
	model   func(*v1beta1.BaseModelSpec) (value string, set bool)
	runtime func(v1beta1.SupportedModelFormat) (value string, set bool)
	// equal compares two set values; nil means they must be the same string.
	equal func(modelValue, runtimeValue string, format v1beta1.SupportedModelFormat) bool
	// version is the attribute's version, compared only when both sides set the attribute and
	// agree on it; nil when the attribute carries no version.
	version *optionalAttribute
	// The reason texts: both set and different; only the runtime sets it; only the model sets it.
	differ, modelMissing, runtimeMissing string
}

// formatAttributes lists the attributes in the order their reasons are reported: the
// architecture, the quantization, the format's name and under it its version, the framework's
// name and under it its version.
var formatAttributes = []optionalAttribute{
	{
		model:          func(m *v1beta1.BaseModelSpec) (string, bool) { return optional(m.ModelArchitecture) },
		runtime:        func(f v1beta1.SupportedModelFormat) (string, bool) { return optional(f.ModelArchitecture) },
		differ:         "architecture mismatch (model={model}, runtime={runtime})",
		modelMissing:   "model has no architecture but runtime requires {runtime}",
		runtimeMissing: "model has architecture {model} but runtime has no architecture requirement",
	},
	{
		model:          func(m *v1beta1.BaseModelSpec) (string, bool) { return optional(m.Quantization) },
		runtime:        func(f v1beta1.SupportedModelFormat) (string, bool) { return optional(f.Quantization) },
		differ:         "quantization mismatch (model={model}, runtime={runtime})",
		modelMissing:   "model has no quantization but runtime requires {runtime}",
		runtimeMissing: "model has quantization {model} but runtime has no quantization requirement",
	},
	{
		// A model always names its format. A format entry that names none fits no model and
		// adds nothing to the reason.
		model: func(m *v1beta1.BaseModelSpec) (string, bool) { return m.ModelFormat.Name, true },
		runtime: func(f v1beta1.SupportedModelFormat) (string, bool) {
			if f.ModelFormat == nil {
				return "", false
			}
			return f.ModelFormat.Name, true
		},
		differ: "format name mismatch (model={model}, runtime={runtime})",
		version: &optionalAttribute{
			model: func(m *v1beta1.BaseModelSpec) (string, bool) { return optional(m.ModelFormat.Version) },
			runtime: func(f v1beta1.SupportedModelFormat) (string, bool) {
				if f.ModelFormat == nil {
					return "", false
				}
				return optional(f.ModelFormat.Version)
			},
			equal: func(modelVersion, runtimeVersion string, f v1beta1.SupportedModelFormat) bool {
				return VersionsMatch(modelVersion, runtimeVersion, f.ModelFormat.Operator)
			},
			differ:         "format version mismatch (model={model}, runtime={runtime})",
			modelMissing:   "model has no format version but runtime requires {runtime}",
			runtimeMissing: "model has format version but runtime has no version requirement",
		},
	},
	{
		model: func(m *v1beta1.BaseModelSpec) (string, bool) {
			if m.ModelFramework == nil {
				return "", false
			}
			return m.ModelFramework.Name, true
		},
		runtime: func(f v1beta1.SupportedModelFormat) (string, bool) {
			if f.ModelFramework == nil {
				return "", false
			}
			return f.ModelFramework.Name, true
		},
		differ:         "framework name mismatch (model={model}, runtime={runtime})",
		modelMissing:   "model has no framework but runtime requires {runtime}",
		runtimeMissing: "model has framework {model} but runtime has no framework requirement",
		version: &optionalAttribute{
			model: func(m *v1beta1.BaseModelSpec) (string, bool) {
				if m.ModelFramework == nil {
					return "", false
				}
				return optional(m.ModelFramework.Version)
			},
			runtime: func(f v1beta1.SupportedModelFormat) (string, bool) {
				if f.ModelFramework == nil {
					return "", false
				}
				return optional(f.ModelFramework.Version)
			},
			equal: func(modelVersion, runtimeVersion string, f v1beta1.SupportedModelFormat) bool {
				return VersionsMatch(modelVersion, runtimeVersion, f.ModelFramework.Operator)
			},
			differ:         "framework version mismatch (model={model}, runtime={runtime})",
			modelMissing:   "model has no framework version but runtime requires {runtime}",
			runtimeMissing: "model has framework version but runtime has no version requirement",
		},
	},
}

// compare reports whether the attribute matches on both sides and, when it does not, the
// reason an operator reads.
func (a optionalAttribute) compare(model *v1beta1.BaseModelSpec, format v1beta1.SupportedModelFormat) (bool, string) {
	modelValue, modelSet := a.model(model)
	runtimeValue, runtimeSet := a.runtime(format)
	switch {
	case !modelSet && !runtimeSet:
		return true, ""
	case !runtimeSet:
		return false, reasonText(a.runtimeMissing, modelValue, runtimeValue)
	case !modelSet:
		return false, reasonText(a.modelMissing, modelValue, runtimeValue)
	case !a.same(modelValue, runtimeValue, format):
		return false, reasonText(a.differ, modelValue, runtimeValue)
	case a.version != nil:
		return a.version.compare(model, format)
	default:
		return true, ""
	}
}

func (a optionalAttribute) same(modelValue, runtimeValue string, format v1beta1.SupportedModelFormat) bool {
	if a.equal == nil {
		return modelValue == runtimeValue
	}
	return a.equal(modelValue, runtimeValue, format)
}

// reasonText fills a reason text, in which {model} and {runtime} stand for the two values.
func reasonText(text, modelValue, runtimeValue string) string {
	return strings.NewReplacer("{model}", modelValue, "{runtime}", runtimeValue).Replace(text)
}

// optional reads an attribute a side may leave unset.
func optional[T ~string](value *T) (string, bool) {
	if value == nil {
		return "", false
	}
	return string(*value), true
}

// VersionsMatch reports whether a model's version meets a supported format's version under the
// format's operator, Equal when nil. A version that does not parse never matches. An unofficial
// version, such as a pre-release, on either side must be equal whatever the operator. The
// ordering operators also need both versions at the same precision and with the same major
// prefix.
func VersionsMatch(modelVersion, supportedVersion string, operator *v1beta1.RuntimeSelectorOperator) bool {
	model, err := modelVer.Parse(modelVersion)
	if err != nil {
		return false
	}
	supported, err := modelVer.Parse(supportedVersion)
	if err != nil {
		return false
	}

	comparison := v1beta1.RuntimeSelectorOpEqual
	if operator != nil {
		comparison = *operator
	}
	if comparison == v1beta1.RuntimeSelectorOpEqual ||
		modelVer.ContainsUnofficialVersion(model) ||
		modelVer.ContainsUnofficialVersion(supported) {
		return modelVer.Equal(supported, model)
	}
	if model.Precision != supported.Precision || model.MajorPrefix != supported.MajorPrefix {
		return false
	}
	switch comparison {
	case v1beta1.RuntimeSelectorOpGreaterThan:
		return modelVer.GreaterThan(supported, model)
	case v1beta1.RuntimeSelectorOpGreaterThanOrEqual:
		return modelVer.GreaterThanOrEqual(supported, model)
	default:
		return modelVer.Equal(supported, model)
	}
}

// --- diffusion pipeline helpers ---

// CompareDiffusionPipeline reports whether a model's diffusion pipeline fits the one a
// supported format requires, and the reason when it does not. A format that requires no
// pipeline fits every model; a failed check always names its reason.
func CompareDiffusionPipeline(model *v1beta1.DiffusionPipelineSpec, format *v1beta1.DiffusionPipelineSpec) (bool, string) {
	if format == nil {
		return true, ""
	}
	if model == nil {
		return false, "diffusion pipeline required by runtime but not specified in model"
	}
	if ok, reason := comparePipelineClass(model.ClassName, format.ClassName); !ok {
		return false, reason
	}
	if ok, reason := compareNamedPipelineComponents(model, format); !ok {
		return false, reason
	}
	return compareAdditionalPipelineComponents(model.AdditionalComponents, format.AdditionalComponents)
}

// comparePipelineClass requires the model to name the pipeline class a format
// names; a format that names none fits every model.
func comparePipelineClass(model, format *string) (bool, string) {
	if format == nil || (model != nil && *format == *model) {
		return true, ""
	}
	modelClassName := "<nil>"
	if model != nil {
		modelClassName = *model
	}
	return false, fmt.Sprintf("pipeline class mismatch (model=%s, runtime=%s)", modelClassName, *format)
}

// compareNamedPipelineComponents compares the five named components in the
// order their mismatches are reported.
func compareNamedPipelineComponents(model, format *v1beta1.DiffusionPipelineSpec) (bool, string) {
	componentChecks := []struct {
		name          string
		model, format *v1beta1.DiffusionComponentSpec
	}{
		{name: "scheduler", model: model.Scheduler, format: format.Scheduler},
		{name: "textEncoder", model: model.TextEncoder, format: format.TextEncoder},
		{name: "tokenizer", model: model.Tokenizer, format: format.Tokenizer},
		{name: "transformer", model: model.Transformer, format: format.Transformer},
		{name: "vae", model: model.VAE, format: format.VAE},
	}
	for _, check := range componentChecks {
		if ok, reason := compareDiffusionComponent(check.name, check.model, check.format); !ok {
			return false, reason
		}
	}
	return true, ""
}

// compareAdditionalPipelineComponents requires every additional component a
// format names to be in the model and to match it.
func compareAdditionalPipelineComponents(model, format map[string]v1beta1.DiffusionComponentSpec) (bool, string) {
	if len(format) == 0 {
		return true, ""
	}
	if len(model) == 0 {
		return false, "diffusion pipeline missing required additional components"
	}
	for key, formatComponent := range format {
		modelComponent, exists := model[key]
		if !exists {
			return false, fmt.Sprintf("diffusion component %s missing in model", key)
		}
		runtimeComponent := formatComponent
		if ok, reason := compareDiffusionComponent(key, &modelComponent, &runtimeComponent); !ok {
			return false, reason
		}
	}
	return true, ""
}

func compareDiffusionComponent(name string, model *v1beta1.DiffusionComponentSpec, runtime *v1beta1.DiffusionComponentSpec) (bool, string) {
	if runtime == nil {
		return true, ""
	}
	if model == nil {
		return false, fmt.Sprintf("component %s required by runtime but not specified in model", name)
	}
	if runtime.Library != "" && runtime.Library != model.Library {
		return false, fmt.Sprintf("%s library mismatch (model=%s, runtime=%s)",
			name, model.Library, runtime.Library)
	}
	if runtime.Type != "" && runtime.Type != model.Type {
		return false, fmt.Sprintf("%s type mismatch (model=%s, runtime=%s)",
			name, model.Type, runtime.Type)
	}
	return true, ""
}

// --- model cache provider helpers ---

// ModelRequiresCacheProvider reports whether a model's distribution loads it through an
// external model cache, so that serving it needs a model cache provider.
func ModelRequiresCacheProvider(model *v1beta1.BaseModelSpec) bool {
	return model != nil &&
		model.Distribution != nil &&
		*model.Distribution == v1beta1.DistributionSharded
}

// FormatSupportsModelCacheProvider reports whether a supported format lists the named provider.
// An empty name, or a format that lists none, never matches.
func FormatSupportsModelCacheProvider(format v1beta1.SupportedModelFormat, provider string) bool {
	provider = strings.TrimSpace(provider)
	if provider == "" || len(format.ModelCacheProviders) == 0 {
		return false
	}
	for _, supported := range format.ModelCacheProviders {
		if string(supported) == provider {
			return true
		}
	}
	return false
}

// ModelCacheProviderMismatchReason is the reason a model that needs a model cache provider
// does not fit a format under the named provider.
func ModelCacheProviderMismatchReason(provider string, format v1beta1.SupportedModelFormat) string {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return "no model cache provider is configured for sharded model loading"
	}
	if len(format.ModelCacheProviders) == 0 {
		return fmt.Sprintf("runtime format does not support model cache provider %q", provider)
	}

	supported := make([]string, 0, len(format.ModelCacheProviders))
	for _, cacheProvider := range format.ModelCacheProviders {
		supported = append(supported, string(cacheProvider))
	}
	return fmt.Sprintf("runtime format supports model cache providers [%s], not %q", strings.Join(supported, ", "), provider)
}
