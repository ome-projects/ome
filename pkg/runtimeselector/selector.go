package runtimeselector

import (
	"context"
	"fmt"
	"sort"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

type defaultSelector struct {
	config  *Config
	fetcher RuntimeFetcher
	matcher RuntimeMatcher
	scorer  RuntimeScorer
}

func New(client client.Client) Selector {
	return NewWithConfig(NewConfig(client))
}

func NewWithConfig(config *Config) Selector {
	return &defaultSelector{
		config:  config,
		fetcher: NewDefaultRuntimeFetcher(config.Client),
		matcher: NewDefaultRuntimeMatcher(config),
		scorer:  NewDefaultRuntimeScorer(config),
	}
}

func (s *defaultSelector) SelectRuntime(ctx context.Context, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) (*RuntimeSelection, error) {
	namespace := isvc.Namespace
	logger := log.FromContext(ctx)

	if err := s.validateModel(model); err != nil {
		return nil, err
	}

	evaluated, err := s.evaluateRuntimes(ctx, model, isvc, namespace)
	if err != nil {
		return nil, err
	}

	if len(evaluated.matches) == 0 {
		return nil, &NoRuntimeFoundError{
			ModelName:          getModelName(model),
			ModelFormat:        model.ModelFormat.Name,
			Namespace:          namespace,
			ExcludedRuntimes:   evaluated.excluded,
			TotalRuntimes:      evaluated.namespaced + evaluated.cluster,
			NamespacedRuntimes: evaluated.namespaced,
			ClusterRuntimes:    evaluated.cluster,
		}
	}

	best := evaluated.matches[0]
	logger.Info("Selected runtime",
		"runtime", best.Name,
		"score", best.Score,
		"isCluster", best.IsCluster,
		"model", model.ModelFormat.Name,
		"namespace", namespace,
		"candidates", len(evaluated.matches))

	return &best.RuntimeSelection, nil
}

func (s *defaultSelector) GetCompatibleRuntimes(ctx context.Context, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, namespace string) ([]RuntimeMatch, error) {
	if err := s.validateModel(model); err != nil {
		return nil, err
	}

	evaluated, err := s.evaluateRuntimes(ctx, model, isvc, namespace)
	if err != nil {
		return nil, err
	}
	return evaluated.matches, nil
}

// evaluatedRuntimes is one pass over the runtimes visible from a namespace:
// the ranked matches, the first reason of every runtime the matcher found
// incompatible, and how many runtimes of each scope were considered.
type evaluatedRuntimes struct {
	matches             []RuntimeMatch
	excluded            map[string]error
	namespaced, cluster int
}

// evaluateRuntimes fetches the namespace's and the cluster's runtimes once
// and evaluates each. Namespace-scoped matches rank before cluster-scoped
// ones, each group sorted best first.
func (s *defaultSelector) evaluateRuntimes(ctx context.Context, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, namespace string) (*evaluatedRuntimes, error) {
	logger := log.FromContext(ctx)

	collection, err := s.fetcher.FetchRuntimes(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch runtimes: %w", err)
	}

	result := &evaluatedRuntimes{
		excluded:   make(map[string]error),
		namespaced: len(collection.NamespaceRuntimes),
		cluster:    len(collection.ClusterRuntimes),
	}
	var namespaceMatches, clusterMatches []RuntimeMatch
	collection.forEach(func(name string, spec *v1beta1.ServingRuntimeSpec, isCluster bool) {
		match, report := s.evaluateRuntime(ctx, spec, model, isvc, name, isCluster)
		switch {
		case match != nil && isCluster:
			clusterMatches = append(clusterMatches, *match)
		case match != nil:
			namespaceMatches = append(namespaceMatches, *match)
		case report != nil && !report.IsCompatible:
			result.excluded[name] = fmt.Errorf("%s", report.IncompatibilityReasons[0])
		}
	})

	s.sortMatches(namespaceMatches, model)
	s.sortMatches(clusterMatches, model)
	result.matches = append(namespaceMatches, clusterMatches...)

	logger.V(1).Info("Compatible runtimes evaluated",
		"model", model.ModelFormat.Name,
		"namespace", namespace,
		"matches", len(result.matches),
		"considered", result.namespaced+result.cluster)

	return result, nil
}

func (s *defaultSelector) ValidateRuntime(ctx context.Context, runtimeName string, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) error {
	namespace := isvc.Namespace

	if err := s.validateModel(model); err != nil {
		return err
	}

	// When the validated name is the ISVC's own runtime reference, honor
	// its declared kind so validation inspects the same runtime the live
	// fetch resolves on a namespaced/cluster name collision.
	kind := ""
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name == runtimeName {
		kind = RefKind(isvc.Spec.Runtime)
	}
	runtimeSpec, isCluster, err := s.fetcher.GetRuntime(ctx, runtimeName, namespace, kind)
	if err != nil {
		return err
	}

	if runtimeSpec.IsDisabled() {
		return &RuntimeDisabledError{
			RuntimeName: runtimeName,
			IsCluster:   isCluster,
		}
	}

	report, err := s.matcher.GetCompatibilityDetails(runtimeSpec, model, isvc, runtimeName)
	if err != nil {
		return err
	}
	if report.IsCompatible {
		return nil
	}
	if refusal := report.serviceRefusal(runtimeName, model); refusal != nil {
		return refusal
	}
	return &RuntimeCompatibilityError{
		RuntimeName: runtimeName,
		ModelName:   getModelName(model),
		ModelFormat: model.ModelFormat.Name,
		Reason:      report.IncompatibilityReasons[0],
	}
}

// evaluateRuntime returns the runtime as a ranked match when it is
// compatible, has an auto-selectable format and scores above zero, or nil.
// The matcher's report comes back either way.
func (s *defaultSelector) evaluateRuntime(ctx context.Context, spec *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService, name string, isCluster bool) (*RuntimeMatch, *CompatibilityReport) {
	logger := log.FromContext(ctx)

	report, err := s.matcher.GetCompatibilityDetails(spec, model, isvc, name)
	if err != nil {
		logger.Error(err, "Failed to get compatibility details", "runtime", name)
		return nil, nil
	}

	if !report.IsCompatible {
		if report.failedCheck == runtimeDisabled {
			logger.V(2).Info("Skipping disabled runtime", "runtime", name)
		} else {
			logger.V(2).Info("Runtime not compatible",
				"runtime", name,
				"reasons", report.IncompatibilityReasons)
		}
		return nil, report
	}

	if !runtimeHasAutoSelectFormat(spec) {
		logger.V(2).Info("Runtime does not have auto-select enabled", "runtime", name)
		return nil, report
	}

	score, err := s.scorer.CalculateScore(spec, model)
	if err != nil {
		logger.Error(err, "Failed to calculate score", "runtime", name)
		return nil, report
	}

	// Zero score means none of the supported formats produced a match
	// (format/framework name or version mismatch). Exclude these so they
	// don't pollute the ranked candidate list.
	if score <= 0 {
		logger.V(2).Info("Runtime has non-positive score", "runtime", name, "score", score)
		return nil, report
	}

	return &RuntimeMatch{
		RuntimeSelection: RuntimeSelection{
			Name:      name,
			Spec:      spec,
			Score:     score,
			IsCluster: isCluster,
		},
		MatchDetails: report.MatchDetails,
	}, report
}

func (s *defaultSelector) sortMatches(matches []RuntimeMatch, model *v1beta1.BaseModelSpec) {
	sort.Slice(matches, func(i, j int) bool {
		return s.scorer.CompareRuntimes(matches[i], matches[j], model) > 0
	})
}

func (s *defaultSelector) GetRuntime(ctx context.Context, name string, namespace string, kind string) (*v1beta1.ServingRuntimeSpec, bool, error) {
	return s.fetcher.GetRuntime(ctx, name, namespace, kind)
}

func (s *defaultSelector) validateModel(model *v1beta1.BaseModelSpec) error {
	if model == nil {
		return &ModelValidationError{
			Field:   "model",
			Message: "model specification is nil",
		}
	}

	if model.ModelFormat.Name == "" {
		return &ModelValidationError{
			Field:   "modelFormat.name",
			Message: "model format name is required",
		}
	}

	return nil
}

func getModelName(model *v1beta1.BaseModelSpec) string {
	if model.ModelFormat.Name != "" {
		return model.ModelFormat.Name
	}
	return "unknown"
}

// GetSupportedModelFormat picks the format the pod renders with. For an
// auto-selected runtime it reads the formats as the ranking does, only those
// marked autoSelect: true and each at its own priority, so the rendered
// format is the one that gave the runtime its score. For a named runtime
// every format counts at the default priority: naming the runtime overrides
// the priorities.
func (s *defaultSelector) GetSupportedModelFormat(ctx context.Context, runtime *v1beta1.ServingRuntimeSpec, model *v1beta1.BaseModelSpec, userSpecifiedRuntime bool) *v1beta1.SupportedModelFormat {
	// Lean path (no model): downstream components treat a nil
	// SupportedModelFormat as "no model-format metadata available", which is
	// the right shape for runtime-only ISVCs that don't use model parsing.
	if model == nil || runtime == nil || runtime.SupportedModelFormats == nil {
		return nil
	}
	maxScore := int64(0)
	bestSupportedFormat := v1beta1.SupportedModelFormat{}
	for _, supportedFormat := range runtime.SupportedModelFormats {
		priority := int64(s.config.DefaultPriority)
		if !userSpecifiedRuntime {
			if !autoSelectEnabled(supportedFormat) {
				continue
			}
			priority = formatPriority(supportedFormat, s.config.DefaultPriority)
		}
		score := s.scorer.CalculateFormatScore(model, supportedFormat, priority)
		if score > maxScore {
			maxScore = score
			bestSupportedFormat = supportedFormat
		}
	}
	if maxScore > 0 {
		return &bestSupportedFormat
	}
	return nil
}
