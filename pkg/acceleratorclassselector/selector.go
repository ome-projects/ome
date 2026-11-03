package acceleratorclassselector

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// defaultSelector is the default implementation of the Selector interface.
type defaultSelector struct {
	config  *Config
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
		config:  config,
		fetcher: NewDefaultAcceleratorFetcher(config.Client),
	}
}

// FetchAcceleratorClass fetches a specific accelerator class by name.
func (s *defaultSelector) FetchAcceleratorClass(ctx context.Context, name string) (*v1beta1.AcceleratorClass, bool, error) {
	return s.fetcher.GetAcceleratorClass(ctx, name)
}

// GetAcceleratorClass selects the best accelerator class for a given inference service, runtime, and component.
// This is a convenience function that implements the original simple selection logic.
// The selection follows this priority order:
//  1. If runtime doesn't contain AcceleratorRequirements, return nil
//  2. If runtime contains AcceleratorRequirements:
//     a. For engine component: check if engine has AcceleratorOverride with AcceleratorClass
//     b. For decoder component: check if decoder has AcceleratorOverride with AcceleratorClass
//     c. If component doesn't have AcceleratorOverride, check if InferenceService has AcceleratorSelector with AcceleratorClass
//     d. If InferenceService doesn't have AcceleratorClass in AcceleratorSelector, check if InferenceService has AcceleratorSelector with Policy
//     e. Otherwise, won't provide AcceleratorClass
func (s *defaultSelector) GetAcceleratorClass(ctx context.Context, isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec, component v1beta1.ComponentType) (*v1beta1.AcceleratorClass, string, error) {
	logger := log.FromContext(ctx)
	acName := ""
	// 1. If runtime doesn't contain AcceleratorRequirements, return nil
	if runtime == nil || runtime.AcceleratorRequirements == nil || runtime.AcceleratorRequirements.AcceleratorClasses == nil {
		return nil, "", nil
	}
	logger.V(1).Info("Selecting accelerator class", "isvc", isvc.Name, "component", component)

	if len(runtime.AcceleratorRequirements.AcceleratorClasses) > 0 {
		// get accelerator class by name if specified.
		if acceleratorClass := s.getAcceleratorClassByName(isvc, component); acceleratorClass != nil {
			acName = *acceleratorClass
		}
		// if accelerator class didn't have name specified, try to get accelerator class by policy
		// if policy is not specified, will skip getting acceleratorClass by policy
		if acName == "" {
			acceleratorPolicy := s.getComponentAcceleratorPolicy(isvc, component)
			if acceleratorPolicy == "" {
				logger.V(1).Info("No acceleratorClass name or policy set for component", "component", component, "inferenceService", isvc.Name)
				return nil, "", nil
			}
			acceleratorClass, err := s.getAcceleratorClassByPolicy(ctx, isvc, runtime, acceleratorPolicy)
			if err != nil {
				return nil, "", err
			}
			if acceleratorClass != nil {
				acName = *acceleratorClass
			}
		}
	}

	// fetch and return the selected accelerator class
	if acName != "" {
		ac, found, err := s.fetcher.GetAcceleratorClass(ctx, acName)
		if err != nil {
			logger.Error(err, "Failed to fetch accelerator class", "name", acName)
			return nil, acName, err
		}
		if !found {
			return nil, acName, &AcceleratorNotFoundError{AcceleratorClassName: acName}
		}
		return ac, acName, nil
	}
	return nil, "", nil
}

// getAcceleratorClassByName checks for component-specific AcceleratorOverride or InferenceService-level AcceleratorSelector
func (s *defaultSelector) getAcceleratorClassByName(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *string {
	// Use the component-specific AcceleratorOverride as the default.
	if acceleratorClass := s.getComponentAcceleratorOverride(isvc, component); acceleratorClass != nil {
		return acceleratorClass
	}

	// Check InferenceService-level AcceleratorSelector if specified
	if acceleratorClass := s.getInferenceServiceAcceleratorClass(isvc); acceleratorClass != nil {
		return acceleratorClass
	}

	return nil
}

// getAcceleratorClassByPolicy selects an accelerator class based on the specified policy
// It fetches candidates from runtime.AcceleratorClasses, filters by InferenceService constraints,
// and applies the policy's chooser.
func (s *defaultSelector) getAcceleratorClassByPolicy(ctx context.Context, isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec, acceleratorPolicy v1beta1.AcceleratorSelectionPolicy) (*string, error) {
	logger := log.FromContext(ctx)

	// Return nil if policy is empty
	if acceleratorPolicy == "" {
		return nil, nil
	}

	// Fetch candidates from runtime.AcceleratorClasses (candidate pool).
	// A read failure must fail the selection (and the reconcile) rather than
	// silently render the component without its accelerator configuration.
	candidates, err := s.getCandidateAccelerators(ctx, runtime)
	if err != nil {
		return nil, err
	}

	if len(candidates) == 0 {
		logger.V(1).Info("No accelerator candidates available")
		return nil, nil
	}

	// Get constraints from InferenceService
	var constraints *v1beta1.AcceleratorConstraints
	if isvc.Spec.AcceleratorSelector != nil {
		constraints = isvc.Spec.AcceleratorSelector.Constraints
	}

	// Filter candidates by InferenceService constraints
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

// getComponentAcceleratorOverride checks if the specified component has an AcceleratorOverride with AcceleratorClass
func (s *defaultSelector) getComponentAcceleratorOverride(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *string {
	if override := componentAcceleratorOverride(isvc, component); override != nil {
		return override.AcceleratorClass
	}
	return nil
}

// componentAcceleratorOverride is the AcceleratorOverride the InferenceService
// sets on the component's own block, nil when it sets none or the component
// has no such field.
func componentAcceleratorOverride(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *v1beta1.AcceleratorSelector {
	if isvc == nil {
		return nil
	}
	if view := v1beta1.SpecOf(&isvc.Spec, component); view != nil {
		return view.AcceleratorOverride
	}
	return nil
}

// getInferenceServiceAcceleratorClass checks if the InferenceService has an AcceleratorSelector with AcceleratorClass
func (s *defaultSelector) getInferenceServiceAcceleratorClass(isvc *v1beta1.InferenceService) *string {
	if isvc == nil ||
		isvc.Spec.AcceleratorSelector == nil ||
		isvc.Spec.AcceleratorSelector.AcceleratorClass == nil {
		return nil
	}

	return isvc.Spec.AcceleratorSelector.AcceleratorClass
}

// getComponentAcceleratorPolicy is the effective AcceleratorSelectionPolicy for the component:
// its AcceleratorOverride's policy first, then the InferenceService-level AcceleratorSelector's,
// and "" when neither states one.
func (s *defaultSelector) getComponentAcceleratorPolicy(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) v1beta1.AcceleratorSelectionPolicy {
	if isvc == nil {
		return ""
	}

	// Check component-specific AcceleratorOverride by first
	if override := componentAcceleratorOverride(isvc, component); override != nil && override.Policy != "" {
		return override.Policy
	}

	// if component-specific AcceleratorOverride not found, check InferenceService-level AcceleratorSelector
	if isvc.Spec.AcceleratorSelector != nil &&
		isvc.Spec.AcceleratorSelector.Policy != "" {
		return isvc.Spec.AcceleratorSelector.Policy
	}

	return ""
}

// getCandidateAccelerators fetches AcceleratorClass candidates from runtime.AcceleratorClasses
func (s *defaultSelector) getCandidateAccelerators(ctx context.Context, runtime *v1beta1.ServingRuntimeSpec) ([]v1beta1.AcceleratorClass, error) {
	logger := log.FromContext(ctx)

	if runtime == nil || runtime.AcceleratorRequirements == nil || len(runtime.AcceleratorRequirements.AcceleratorClasses) == 0 {
		logger.V(1).Info("No accelerator classes in runtime requirements")
		return nil, nil
	}

	// Fetch all candidates from the runtime's accelerator class list
	candidates, err := candidatesFromNames(ctx, s.fetcher, runtime.AcceleratorRequirements.AcceleratorClasses)
	if err != nil {
		return nil, err
	}

	logger.V(1).Info("Fetched candidate accelerators", "count", len(candidates))
	return candidates, nil
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

// precisionPreference reads the service's preferred precisions, nil when it states none.
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
