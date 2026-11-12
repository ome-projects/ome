package runtimeselector

import (
	"math"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

// DefaultRuntimeScorer implements RuntimeScorer with configurable scoring weights.
type DefaultRuntimeScorer struct {
	config *Config
}

func NewDefaultRuntimeScorer(config *Config) RuntimeScorer {
	return &DefaultRuntimeScorer{
		config: config,
	}
}

// CalculateScore is the runtime's score in automatic selection: the best
// score among the formats marked autoSelect: true, each at its own priority
// (the configured default when it sets none), or 0 when none fits the model.
func (s *DefaultRuntimeScorer) CalculateScore(runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec) (int64, error) {
	_, score := bestFormat(s, runtime, model, false, s.config.DefaultPriority)
	return score, nil
}

// bestFormat is the one format match behind the ranking and the rendered
// format, so a pod renders with the format that gave its runtime the score.
// In automatic selection only a format marked autoSelect: true counts, each
// at its own priority (defaultPriority when it sets none). For a runtime the
// service named (userSpecifiedRuntime) every format counts at
// defaultPriority: naming the runtime overrides the priorities. It returns
// the highest-scoring format with its score. The first of equal scores wins.
// When no format scores above zero the score is 0 and the format is the zero
// value.
func bestFormat(scorer RuntimeScorer, runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, userSpecifiedRuntime bool, defaultPriority int32) (v1beta1.SupportedModelFormat, int64) {
	var best v1beta1.SupportedModelFormat
	var bestScore int64
	for _, format := range runtime.SupportedModelFormats {
		priority := int64(defaultPriority)
		if !userSpecifiedRuntime {
			if !autoSelectEnabled(format) {
				continue
			}
			priority = formatPriority(format, defaultPriority)
		}
		if score := scorer.CalculateFormatScore(model, format, priority); score > bestScore {
			best, bestScore = format, score
		}
	}
	return best, bestScore
}

// autoSelectEnabled reports whether the format is offered for automatic
// selection: autoSelect must be set to true, unset counts as false.
func autoSelectEnabled(format v1beta1.SupportedModelFormat) bool {
	return format.AutoSelect != nil && *format.AutoSelect
}

// formatPriority is the multiplier a format's score carries in automatic
// selection: its own priority when set, the configured default otherwise.
func formatPriority(format v1beta1.SupportedModelFormat, defaultPriority int32) int64 {
	if format.Priority != nil {
		return int64(*format.Priority)
	}
	return int64(defaultPriority)
}

// CompareRuntimes orders by (score desc, size-range distance asc,
// namespace-scoped before cluster, then name asc) to keep selection
// deterministic when several runtimes score identically.
func (s *DefaultRuntimeScorer) CompareRuntimes(r1, r2 RuntimeMatch, model *v1beta1.BaseModelSpec) int {
	if r1.Score != r2.Score {
		return int(r1.Score - r2.Score)
	}

	// Closer ModelSizeRange wins next — protects against picking a
	// "7B-70B" runtime over a "5B-10B" one for a 7B model when both
	// score the same on format/framework match.
	if model.ModelParameterSize != nil {
		r1SizeScore := s.calculateSizeScore(r1, model)
		r2SizeScore := s.calculateSizeScore(r2, model)

		if r1SizeScore != r2SizeScore {
			return int(r2SizeScore - r1SizeScore)
		}
	}

	// Namespace runtimes are tenant overrides and should win against
	// equally-scoring cluster runtimes.
	if r1.IsCluster != r2.IsCluster {
		if r1.IsCluster {
			return -1
		}
		return 1
	}

	if r1.Name < r2.Name {
		return 1
	} else if r1.Name > r2.Name {
		return -1
	}

	return 0
}

// CalculateFormatScore is one supported format's score for the model at the
// given priority: the format weight plus the framework weight (the configured
// defaults for a zero weight), times the priority, or 0 when the format does
// not fit the model.
func (s *DefaultRuntimeScorer) CalculateFormatScore(model *v1beta1.BaseModelSpec, supportedFormat v1beta1.SupportedModelFormat, priority int64) int64 {
	if validation.ModelRequiresCacheProvider(model) && !validation.FormatSupportsModelCacheProvider(supportedFormat, s.config.ModelCacheProvider) {
		return 0
	}
	var score int64
	if format := supportedFormat.ModelFormat; format != nil {
		want := versionedName{name: format.Name, version: format.Version, operator: format.Operator, weight: format.Weight}
		if !want.fits(model.ModelFormat.Name, model.ModelFormat.Version) {
			return 0
		}
		score += want.score(s.config.ModelFormatWeight, priority)
	}
	framework := supportedFormat.ModelFramework
	switch {
	case framework == nil && model.ModelFramework == nil:
		// Neither side names a framework: the format alone decides.
	case framework == nil || model.ModelFramework == nil:
		return 0
	default:
		want := versionedName{name: framework.Name, version: framework.Version, operator: framework.Operator, weight: framework.Weight}
		if !want.fits(model.ModelFramework.Name, model.ModelFramework.Version) {
			return 0
		}
		score += want.score(s.config.ModelFrameworkWeight, priority)
	}
	return score
}

// versionedName is what a supported format's modelFormat or modelFramework
// entry asks of the model: a name, a version under the entry's operator, and
// the weight a match earns.
type versionedName struct {
	name     string
	version  *string
	operator *v1beta1.RuntimeSelectorOperator
	weight   int64
}

// fits reports whether the model's name and version satisfy the entry.
func (v versionedName) fits(name string, version *string) bool {
	return v.name == name && versionsMatch(version, v.version, v.operator)
}

// score is the entry's weight, or the default when it sets none, times the
// priority.
func (v versionedName) score(defaultWeight, priority int64) int64 {
	weight := v.weight
	if weight == 0 {
		weight = defaultWeight
	}
	return weight * priority
}

// calculateSizeScore is the sum-of-distances of the model size from each
// boundary of the runtime's supported range — lower means a tighter fit.
// Min and Max are each +optional in the CRD; an unset bound is treated as an
// open (infinitely far) edge that contributes no distance, so a partial range
// scores only on the bound it actually sets rather than dereferencing nil.
func (s *DefaultRuntimeScorer) calculateSizeScore(runtime RuntimeMatch, model *v1beta1.BaseModelSpec) float64 {
	if runtime.Spec == nil || runtime.Spec.ModelSizeRange == nil || model.ModelParameterSize == nil {
		return 0
	}

	sizeRange := runtime.Spec.ModelSizeRange
	modelSize := parseModelSize(*model.ModelParameterSize)

	var distance float64
	if sizeRange.Min != nil {
		distance += math.Abs(parseModelSize(*sizeRange.Min) - modelSize)
	}
	if sizeRange.Max != nil {
		distance += math.Abs(parseModelSize(*sizeRange.Max) - modelSize)
	}

	return distance
}

// versionsMatch is the version rule of a supported format's modelFormat and
// modelFramework entries: a version either side leaves unset matches, and two
// set versions match under the runtime's operator (Equal when it names none).
func versionsMatch(modelVersion, supportedVersion *string, operator *v1beta1.RuntimeSelectorOperator) bool {
	if modelVersion == nil || supportedVersion == nil {
		return true
	}
	return validation.VersionsMatch(*modelVersion, *supportedVersion, operator)
}
