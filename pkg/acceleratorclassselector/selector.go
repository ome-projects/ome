package acceleratorclassselector

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// defaultSelector is the default implementation of the Selector interface.
type defaultSelector struct {
	fetcher AcceleratorFetcher
}

// New creates a new Selector with default implementations.
func New(client client.Client) Selector {
	config := NewConfig(client)
	return NewWithConfig(config)
}

// NewWithConfig creates a new Selector with the provided configuration.
func NewWithConfig(config *Config) Selector {
	return &defaultSelector{
		fetcher: NewDefaultAcceleratorFetcher(config.Client),
	}
}

// GetAcceleratorClass selects the accelerator class for one component of an
// InferenceService on a runtime: nil when the runtime states no accelerator
// requirements or nothing names or chooses a class, else the requested class
// read by name; a name no class carries is an AcceleratorNotFoundError.
func (s *defaultSelector) GetAcceleratorClass(ctx context.Context, isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec, component v1beta1.ComponentType) (*v1beta1.AcceleratorClass, string, error) {
	if runtime == nil || runtime.AcceleratorRequirements == nil || runtime.AcceleratorRequirements.AcceleratorClasses == nil {
		return nil, "", nil
	}
	log.FromContext(ctx).V(1).Info("Selecting accelerator class", "isvc", isvc.Name, "component", component)
	name, err := s.requestedClassName(ctx, isvc, runtime, component)
	if err != nil || name == "" {
		return nil, "", err
	}
	return s.fetchNamed(ctx, name)
}

// requestedClassName is the class the spec asks for the component: the name
// its override or the service's selector states, else the class the stated
// policy chooses among the runtime's candidates under the stated constraints.
// "" when the runtime lists no classes or nothing names or chooses one.
func (s *defaultSelector) requestedClassName(ctx context.Context, isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec, component v1beta1.ComponentType) (string, error) {
	if len(runtime.AcceleratorRequirements.AcceleratorClasses) == 0 {
		return "", nil
	}
	selectors := selectorsOf(isvc, component)
	if name := statedClassName(selectors); name != nil && *name != "" {
		return *name, nil
	}
	policy := statedPolicy(selectors)
	if policy == "" {
		log.FromContext(ctx).V(1).Info("No acceleratorClass name or policy set for component", "component", component, "inferenceService", isvc.Name)
		return "", nil
	}
	chosen, err := s.getAcceleratorClassByPolicy(ctx, runtime, policy, statedConstraints(selectors))
	if err != nil || chosen == nil {
		return "", err
	}
	return *chosen, nil
}

// fetchNamed reads the class the spec asked for by name.
func (s *defaultSelector) fetchNamed(ctx context.Context, name string) (*v1beta1.AcceleratorClass, string, error) {
	ac, found, err := s.fetcher.GetAcceleratorClass(ctx, name)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to fetch accelerator class", "name", name)
		return nil, name, err
	}
	if !found {
		return nil, name, &AcceleratorNotFoundError{AcceleratorClassName: name}
	}
	return ac, name, nil
}

// selectorsOf lists the selectors that speak for the component, the most
// specific first: the component's own override, then the service's selector.
func selectorsOf(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) []*v1beta1.AcceleratorSelector {
	if isvc == nil {
		return nil
	}
	var selectors []*v1beta1.AcceleratorSelector
	if view := v1beta1.SpecOf(&isvc.Spec, component); view != nil && view.AcceleratorOverride != nil {
		selectors = append(selectors, view.AcceleratorOverride)
	}
	if isvc.Spec.AcceleratorSelector != nil {
		selectors = append(selectors, isvc.Spec.AcceleratorSelector)
	}
	return selectors
}

// statedClassName is the class name of the first selector that states one,
// nil when none does.
func statedClassName(selectors []*v1beta1.AcceleratorSelector) *string {
	for _, selector := range selectors {
		if selector.AcceleratorClass != nil {
			return selector.AcceleratorClass
		}
	}
	return nil
}

// statedPolicy is the selection policy of the first selector that states one,
// "" when none does.
func statedPolicy(selectors []*v1beta1.AcceleratorSelector) v1beta1.AcceleratorSelectionPolicy {
	for _, selector := range selectors {
		if selector.Policy != "" {
			return selector.Policy
		}
	}
	return ""
}

// statedConstraints is the constraints block of the first selector that
// states one, nil when none does. The block is taken whole: an override's
// block replaces the service's, it is not merged with it.
func statedConstraints(selectors []*v1beta1.AcceleratorSelector) *v1beta1.AcceleratorConstraints {
	for _, selector := range selectors {
		if selector.Constraints != nil {
			return selector.Constraints
		}
	}
	return nil
}

// getAcceleratorClassByPolicy chooses among the runtime's classes under the
// policy: the candidates are read, those the constraints exclude are dropped,
// and the policy's chooser picks one against the same constraints. Nil when
// none remains or the policy is unknown. A read failure fails the selection,
// and with it the reconcile, rather than rendering the component without its
// accelerator.
func (s *defaultSelector) getAcceleratorClassByPolicy(ctx context.Context, runtime *v1beta1.ServingRuntimeSpec, acceleratorPolicy v1beta1.AcceleratorSelectionPolicy, constraints *v1beta1.AcceleratorConstraints) (*string, error) {
	logger := log.FromContext(ctx)
	candidates, err := candidatesFromNames(ctx, s.fetcher, runtime.AcceleratorRequirements.AcceleratorClasses)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		logger.V(1).Info("No accelerator candidates available")
		return nil, nil
	}

	validCandidates := filterCandidates(ctx, candidates, constraints)
	if len(validCandidates) == 0 {
		logger.V(1).Info("No candidates passed filtering", "policy", acceleratorPolicy)
		return nil, nil
	}
	logger.V(1).Info("Candidates after filtering", "count", len(validCandidates), "policy", acceleratorPolicy)

	choose, known := policies[acceleratorPolicy]
	if !known {
		return nil, nil
	}
	return choose(ctx, validCandidates, constraints), nil
}

// chooser applies one selection policy to the candidates that passed the constraints and
// returns the chosen class name, nil when none can be chosen.
type chooser func(ctx context.Context, candidates []v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) *string

// policies maps each selection policy to its chooser; a policy absent here chooses nothing.
var policies = map[v1beta1.AcceleratorSelectionPolicy]chooser{
	v1beta1.BestFitPolicy: selectBestFit,
	v1beta1.CheapestPolicy: func(ctx context.Context, candidates []v1beta1.AcceleratorClass, _ *v1beta1.AcceleratorConstraints) *string {
		return selectCheapest(ctx, candidates)
	},
	v1beta1.MostCapablePolicy: func(ctx context.Context, candidates []v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) *string {
		return selectMostCapable(ctx, candidates, precisionPreference(constraints))
	},
	v1beta1.FirstAvailablePolicy: selectFirstAvailable,
}

// precisionPreference reads the constraints' preferred precisions, nil when they state none.
func precisionPreference(constraints *v1beta1.AcceleratorConstraints) []string {
	if constraints == nil {
		return nil
	}
	return constraints.PreferredPrecisions
}

// trivialChoice settles a selection that has nothing to compare: nil when no candidate
// remains, the only candidate when one does. settled is false when two or more remain.
func trivialChoice(ctx context.Context, policy string, candidates []v1beta1.AcceleratorClass) (choice *string, settled bool) {
	logger := log.FromContext(ctx)
	switch len(candidates) {
	case 0:
		logger.V(1).Info("No valid candidates for " + policy + " selection")
		return nil, true
	case 1:
		logger.V(1).Info("Single candidate after filtering", "name", candidates[0].Name)
		return &candidates[0].Name, true
	}
	return nil, false
}

// highestScoring picks the candidate with the highest score; the first listed wins a tie.
func highestScoring(ctx context.Context, policy string, candidates []v1beta1.AcceleratorClass, score func(v1beta1.AcceleratorClass) float64) *string {
	if choice, settled := trivialChoice(ctx, policy, candidates); settled {
		return choice
	}
	logger := log.FromContext(ctx)
	best, bestScore := "", 0.0
	for i, candidate := range candidates {
		candidateScore := score(candidate)
		logger.V(1).Info("Scored candidate", "name", candidate.Name, "score", candidateScore)
		if i == 0 || candidateScore > bestScore {
			best, bestScore = candidate.Name, candidateScore
		}
	}
	logger.V(1).Info(policy+" selected", "name", best, "score", bestScore)
	return &best
}

// selectBestFit picks the candidate whose memory and compute match the constraints most
// closely (calculateBestFitScore).
func selectBestFit(ctx context.Context, candidates []v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) *string {
	return highestScoring(ctx, "BestFit", candidates, func(candidate v1beta1.AcceleratorClass) float64 {
		return calculateBestFitScore(candidate, constraints)
	})
}

// selectMostCapable picks the candidate with the most memory, bandwidth and compute
// relative to the other candidates (calculateCapabilityScore).
func selectMostCapable(ctx context.Context, candidates []v1beta1.AcceleratorClass, preferredPrecisions []string) *string {
	maxVals := computeMaxValues(candidates, preferredPrecisions)
	return highestScoring(ctx, "MostCapable", candidates, func(candidate v1beta1.AcceleratorClass) float64 {
		return calculateCapabilityScore(candidate, preferredPrecisions, maxVals)
	})
}

// selectCheapest picks the lowest price in the first cost group any candidate states
// (costGroups); candidates without cost data do not take part.
func selectCheapest(ctx context.Context, candidates []v1beta1.AcceleratorClass) *string {
	if choice, settled := trivialChoice(ctx, "Cheapest", candidates); settled {
		return choice
	}
	logger := log.FromContext(ctx)
	priced := make([]v1beta1.AcceleratorClass, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Spec.Cost == nil {
			logger.V(1).Info("Skipping candidate without cost data", "name", candidate.Name)
			continue
		}
		priced = append(priced, candidate)
	}
	for _, group := range costGroups {
		if cheapest := cheapestIn(group, priced); cheapest != nil {
			logger.V(1).Info("Cheapest selected", "name", cheapest.Name, "costType", group.label)
			return &cheapest.Name
		}
	}
	logger.Error(nil, "No usable cost fields found for any candidate")
	return nil
}

// selectFirstAvailable picks the first candidate in the runtime's order.
func selectFirstAvailable(ctx context.Context, candidates []v1beta1.AcceleratorClass, _ *v1beta1.AcceleratorConstraints) *string {
	if len(candidates) == 0 {
		return nil
	}
	selected := candidates[0].Name
	log.FromContext(ctx).V(1).Info("FirstAvailable selected", "name", selected)
	return &selected
}
