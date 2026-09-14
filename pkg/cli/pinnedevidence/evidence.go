// Package pinnedevidence validates controller-produced rollout run evidence
// without performing API reads or importing a controller package.
package pinnedevidence

import (
	"reflect"
	"regexp"
	"strings"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/validation"
)

const (
	maxGroups  = 3
	maxSteps   = 20
	maxMetrics = 10
)

var (
	revisionHashPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)
	runHashPattern      = regexp.MustCompile(`^[0-9a-f]{12}$`)
)

// ValidActiveRun reports whether the complete pinned run could have been
// produced by the controller. It validates the frozen plan, provenance,
// topology, target binding, and the run's initial monotonic clock.
func ValidActiveRun(isvc *omev1beta1.InferenceService) bool {
	_, valid := validateActiveRun(isvc)
	return valid
}

// ValidCanaryRepin reports whether a fully valid active run proves that the
// supplied canary status was clamped against the supplied steps by a repin.
// A repin advances PinnedAt strictly beyond the run open. The pinned canary
// group and primary target must match exactly. StepEnteredTime is deliberately
// not part of this proof: the canary capacity reconciler may re-stamp it after
// the repin while a raising PreStepHold remains armed.
func ValidCanaryRepin(
	isvc *omev1beta1.InferenceService,
	primary omev1beta1.ComponentType,
	steps []omev1beta1.RolloutGroupStep,
	targetRevision string,
) bool {
	validated, valid := validateActiveRun(isvc)
	if !valid || !validated.run.PinnedAt.Time.After(validated.run.OpenedAt.Time) ||
		!revisionHashPattern.MatchString(targetRevision) ||
		validated.targets[primary] != targetRevision {
		return false
	}

	matchingCanary := 0
	for i := range validated.run.Plan.Groups {
		group := &validated.run.Plan.Groups[i].Group
		if group.Canary == nil {
			continue
		}
		groupPrimary, primaryValid := primaryComponent(group.Components)
		if primaryValid && groupPrimary == primary &&
			reflect.DeepEqual(group.Canary.Steps, steps) {
			matchingCanary++
		}
	}
	return matchingCanary == 1
}

type activeRunEvidence struct {
	run     *omev1beta1.RolloutRun
	targets map[omev1beta1.ComponentType]string
}

func validateActiveRun(isvc *omev1beta1.InferenceService) (activeRunEvidence, bool) {
	if isvc == nil || isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return activeRunEvidence{}, false
	}
	run := isvc.Status.Rollout.ActiveRun
	prefix := isvc.Name + "-"
	if isvc.Name == "" || !strings.HasPrefix(run.RunID, prefix) ||
		!runHashPattern.MatchString(strings.TrimPrefix(run.RunID, prefix)) ||
		run.OpenedAt.IsZero() || run.PinnedAt.IsZero() ||
		run.PinnedAt.Time.Before(run.OpenedAt.Time) || len(run.Plan.Groups) == 0 {
		return activeRunEvidence{}, false
	}

	copy := isvc.DeepCopy()
	copy.Spec.Rollout = run.Plan.AsRolloutSpec(copy.Spec.Rollout)
	if !validStoredPlan(&copy.Spec) ||
		validation.ValidateRolloutOrderingEnforced(&copy.Spec) != nil {
		return activeRunEvidence{}, false
	}

	expectedComponents := make(map[omev1beta1.ComponentType]struct{}, 3)
	for i := range run.Plan.Groups {
		pinned := &run.Plan.Groups[i]
		progression, progressionValid := progressionKind(&pinned.Group)
		digest, digestErr := rolloutpolicy.ProgressionDigest(&pinned.Group)
		if !progressionValid || pinned.Group.PolicyRef != nil || digestErr != nil ||
			digest == "" || digest != pinned.PortableDigest ||
			!validSource(pinned, progression) {
			return activeRunEvidence{}, false
		}
		for _, component := range pinned.Group.Components {
			if _, duplicate := expectedComponents[component]; duplicate {
				return activeRunEvidence{}, false
			}
			expectedComponents[component] = struct{}{}
		}
	}
	if len(run.TargetRevisions) != len(expectedComponents) {
		return activeRunEvidence{}, false
	}

	targets := make(map[omev1beta1.ComponentType]string, len(run.TargetRevisions))
	for _, target := range run.TargetRevisions {
		if _, expected := expectedComponents[target.Component]; !expected ||
			!revisionHashPattern.MatchString(target.Revision) {
			return activeRunEvidence{}, false
		}
		if _, duplicate := targets[target.Component]; duplicate {
			return activeRunEvidence{}, false
		}
		targets[target.Component] = target.Revision
	}
	return activeRunEvidence{run: run, targets: targets}, true
}

func validStoredPlan(spec *omev1beta1.InferenceServiceSpec) bool {
	groups := spec.GetRolloutGroups()
	if len(groups) > maxGroups {
		return false
	}
	for i := range groups {
		group := &groups[i]
		if len(group.Components) == 0 || len(group.Components) > 3 || len(group.Order) > 3 {
			return false
		}
		if _, valid := progressionKind(group); !valid {
			return false
		}
		if group.Canary == nil {
			continue
		}
		if len(group.Canary.Steps) > maxSteps {
			return false
		}
		for stepIndex := range group.Canary.Steps {
			analysis := group.Canary.Steps[stepIndex].Analysis
			if analysis == nil {
				continue
			}
			if len(analysis.Metrics) > maxMetrics {
				return false
			}
			if analysis.OnInconclusive != nil &&
				*analysis.OnInconclusive != omev1beta1.OnInconclusiveHold &&
				*analysis.OnInconclusive != omev1beta1.OnInconclusiveRollback {
				return false
			}
		}
	}
	return validation.ValidateCanary(spec) == nil &&
		validation.ValidateCoordination(spec) == nil &&
		validation.ValidateLifecycle(spec) == nil
}

func validSource(
	pinned *omev1beta1.RolloutRunGroup,
	progression omev1beta1.RolloutProgressionKind,
) bool {
	switch pinned.Source {
	case omev1beta1.RolloutPlanSourceInline:
		return pinned.PolicyRef == nil && pinned.PolicyGeneration == 0
	case omev1beta1.RolloutPlanSourcePolicy:
		if pinned.PolicyRef == nil || pinned.PolicyGeneration < 0 ||
			!validPolicyRef(pinned.PolicyRef, progression, pinned.PolicyGeneration) {
			return false
		}
		policy := &omev1beta1.RolloutPolicySpec{
			Canary:        pinned.Group.Canary,
			BlueGreen:     pinned.Group.BlueGreen,
			RollingUpdate: pinned.Group.RollingUpdate,
		}
		return validation.ValidateRolloutPolicySpec(policy) == nil
	default:
		return false
	}
}

func validPolicyRef(
	ref *omev1beta1.RolloutPolicyRef,
	progression omev1beta1.RolloutProgressionKind,
	generation int64,
) bool {
	if ref.Name == "" || len(utilvalidation.IsDNS1123Subdomain(ref.Name)) != 0 {
		return false
	}
	// A derived ISVC recovers only a policy name from its derive-time
	// annotation and has no local object generation. A locally resolved ref
	// retains the admission-validated kind and declared progression, and the
	// Kubernetes object has a positive generation.
	if generation == 0 {
		return ref.Kind == "" && ref.Progression == ""
	}
	return generation > 0 &&
		(ref.Kind == "" || ref.Kind == validation.RolloutPolicyKind) &&
		ref.Progression == progression
}

func progressionKind(
	group *omev1beta1.RolloutGroup,
) (omev1beta1.RolloutProgressionKind, bool) {
	if group == nil {
		return "", false
	}
	progressions := 0
	kind := omev1beta1.RolloutProgressionKind("")
	if group.Canary != nil {
		progressions++
		kind = omev1beta1.RolloutProgressionCanary
	}
	if group.BlueGreen != nil {
		progressions++
		kind = omev1beta1.RolloutProgressionBlueGreen
	}
	if group.RollingUpdate != nil {
		progressions++
		kind = omev1beta1.RolloutProgressionRollingUpdate
	}
	return kind, progressions == 1
}

func primaryComponent(
	components []omev1beta1.ComponentType,
) (omev1beta1.ComponentType, bool) {
	seen := make(map[omev1beta1.ComponentType]struct{}, len(components))
	valid := len(components) > 0
	for _, component := range components {
		if _, duplicate := seen[component]; duplicate {
			valid = false
		}
		switch component {
		case omev1beta1.RouterComponent,
			omev1beta1.EngineComponent,
			omev1beta1.DecoderComponent:
		default:
			valid = false
		}
		seen[component] = struct{}{}
	}
	for _, candidate := range []omev1beta1.ComponentType{
		omev1beta1.RouterComponent,
		omev1beta1.EngineComponent,
		omev1beta1.DecoderComponent,
	} {
		if _, found := seen[candidate]; found {
			return candidate, valid
		}
	}
	return "", false
}
