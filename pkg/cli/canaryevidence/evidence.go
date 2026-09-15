// Package canaryevidence contains the pure invariants that bind canary status
// to its rollout phase, configured step, and primary-component traffic epoch.
package canaryevidence

import (
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/pinnedevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	omevalidation "sigs.k8s.io/ome/pkg/validation"
)

var revisionHashPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)

// Primary returns the component whose traffic is authoritative for a canary
// group and whether every member is unique and supported. The selected value
// still follows controller priority when the surrounding shape is invalid, so
// diagnostic projections can preserve the shipped phase source.
func Primary(components []omev1beta1.ComponentType) (omev1beta1.ComponentType, bool) {
	valid := len(components) > 0
	seen := make(map[omev1beta1.ComponentType]bool, len(components))
	for _, component := range components {
		if seen[component] || !supportedComponent(component) {
			valid = false
		}
		seen[component] = true
	}
	for _, candidate := range []omev1beta1.ComponentType{
		omev1beta1.RouterComponent,
		omev1beta1.EngineComponent,
		omev1beta1.DecoderComponent,
	} {
		if slices.Contains(components, candidate) {
			return candidate, valid
		}
	}
	return "", false
}

// ProjectPhase converts the API phase into the CLI's closed report contract.
func ProjectPhase(phase omev1beta1.RolloutPhase) reportv1alpha1.RolloutPhase {
	switch phase {
	case omev1beta1.RolloutPhaseStable:
		return reportv1alpha1.RolloutPhaseStable
	case omev1beta1.RolloutPhaseCanarying:
		return reportv1alpha1.RolloutPhaseCanarying
	case omev1beta1.RolloutPhaseBlueGreenStandby:
		return reportv1alpha1.RolloutPhaseBlueGreenStandby
	case omev1beta1.RolloutPhasePending:
		return reportv1alpha1.RolloutPhasePending
	case omev1beta1.RolloutPhasePaused:
		return reportv1alpha1.RolloutPhasePaused
	case omev1beta1.RolloutPhasePromoting:
		return reportv1alpha1.RolloutPhasePromoting
	case omev1beta1.RolloutPhaseRollingBack:
		return reportv1alpha1.RolloutPhaseRollingBack
	case omev1beta1.RolloutPhaseRolledBack:
		return reportv1alpha1.RolloutPhaseRolledBack
	case omev1beta1.RolloutPhaseFailed:
		return reportv1alpha1.RolloutPhaseFailed
	default:
		return reportv1alpha1.RolloutPhaseUnknown
	}
}

// SafeRevisionHash reports whether a controller revision hash is canonical.
func SafeRevisionHash(hash string) bool {
	return revisionHashPattern.MatchString(hash)
}

// ValidCanaryPlan applies the same pure plan-body validation used by the
// controller before it pins a canary plan. Projection must not trust a
// digest-correct status shape that the controller could never produce.
func ValidCanaryPlan(plan *omev1beta1.GroupCanary) bool {
	return plan != nil && omevalidation.ValidateCanaryPlan("canary", plan) == nil
}

// RevisionHash extracts a canonical hash from a per-revision Service name.
func RevisionHash(isvcName string, component omev1beta1.ComponentType, serviceName string) string {
	if len(serviceName) < 8 {
		return ""
	}
	hash := serviceName[len(serviceName)-8:]
	if !SafeRevisionHash(hash) {
		return ""
	}
	rawName := isvcName + "-" + string(component) + "-rev-" + hash
	expected := constants.TruncateNameWithMaxLength(rawName, validation.DNS1035LabelMaxLength)
	if serviceName != expected {
		return ""
	}
	return hash
}

// PhaseNeedsStatus reports whether a canary phase requires CanaryStatus.
func PhaseNeedsStatus(phase reportv1alpha1.RolloutPhase) bool {
	switch phase {
	case reportv1alpha1.RolloutPhasePending,
		reportv1alpha1.RolloutPhaseCanarying,
		reportv1alpha1.RolloutPhasePaused,
		reportv1alpha1.RolloutPhasePromoting,
		reportv1alpha1.RolloutPhaseRollingBack,
		reportv1alpha1.RolloutPhaseRolledBack,
		reportv1alpha1.RolloutPhaseFailed:
		return true
	default:
		return false
	}
}

// PhaseBindsTraffic reports whether the phase promises that primary Traffic
// and CanaryStatus describe the same applied epoch.
func PhaseBindsTraffic(phase reportv1alpha1.RolloutPhase) bool {
	switch phase {
	case reportv1alpha1.RolloutPhaseCanarying,
		reportv1alpha1.RolloutPhasePaused,
		reportv1alpha1.RolloutPhasePromoting,
		reportv1alpha1.RolloutPhaseRollingBack,
		reportv1alpha1.RolloutPhaseRolledBack:
		return true
	default:
		return false
	}
}

// StatusBindsTraffic reports whether primary Traffic must describe the same
// applied epoch as CanaryStatus. A repin pre-step hold keeps the previously
// programmed split while capacity converges or parks after its timeout, so
// Pending and Failed become traffic-bound for that status even though they are
// not traffic-bound during ordinary canary initialization and capacity waits.
func StatusBindsTraffic(
	phase reportv1alpha1.RolloutPhase,
	status *omev1beta1.CanaryStatus,
) bool {
	if PhaseBindsTraffic(phase) {
		return true
	}
	return status != nil && status.PreStepHold &&
		(phase == reportv1alpha1.RolloutPhasePending ||
			phase == reportv1alpha1.RolloutPhaseFailed)
}

// PhaseBindsStepTraffic reports whether observed traffic must match a
// configured canary step in this phase.
func PhaseBindsStepTraffic(phase reportv1alpha1.RolloutPhase) bool {
	switch phase {
	case reportv1alpha1.RolloutPhaseCanarying,
		reportv1alpha1.RolloutPhasePaused,
		reportv1alpha1.RolloutPhasePromoting:
		return true
	default:
		return false
	}
}

// ObservedTrafficMatchesStep accepts the current step, the controller's
// documented one-write canary advance residue where the new index is visible
// before its traffic is applied. A PreStepHold needs active-run evidence and
// is therefore accepted only by ValidRepinBoundary.
func ObservedTrafficMatchesStep(
	phase reportv1alpha1.RolloutPhase,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
) bool {
	if status == nil {
		return false
	}
	if !promotingTrafficComplete(phase, status) {
		return false
	}
	current := int(status.CurrentStep)
	if current < 0 || current >= len(steps) {
		return false
	}
	if status.PreStepHold {
		return false
	}
	if status.ObservedTrafficWeight == steps[current].Traffic {
		return true
	}
	return phase == reportv1alpha1.RolloutPhaseCanarying &&
		current > 0 &&
		status.ObservedTrafficWeight == steps[current-1].Traffic
}

// ValidPhaseStepResidue validates phase-specific step and durable promotion /
// rollback residue without requiring applied traffic during Pending or Failed.
func ValidPhaseStepResidue(
	phase reportv1alpha1.RolloutPhase,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
) bool {
	return validPhaseStepResidue(phase, steps, status, false)
}

func validPhaseStepResidue(
	phase reportv1alpha1.RolloutPhase,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
	allowPreStepHold bool,
) bool {
	if status == nil {
		return false
	}
	if !promotingTrafficComplete(phase, status) {
		return false
	}
	current := int(status.CurrentStep)
	if current < 0 || current >= len(steps) {
		return false
	}
	if status.PreStepHold {
		if !allowPreStepHold || !validPreStepHold(phase, steps, status) {
			return false
		}
	}
	last := len(steps) - 1
	switch phase {
	case reportv1alpha1.RolloutPhasePromoting:
		if current != last {
			return false
		}
	case reportv1alpha1.RolloutPhasePaused:
		if current >= last && !status.PreStepHold {
			return false
		}
	case reportv1alpha1.RolloutPhaseCanarying:
		if !status.PreStepHold && current == last &&
			(current == 0 || status.ObservedTrafficWeight != steps[current-1].Traffic) {
			return false
		}
	}

	rollbackPhase := phase == reportv1alpha1.RolloutPhaseRollingBack ||
		phase == reportv1alpha1.RolloutPhaseRolledBack
	if status.RolledBackRevisionHash != "" && !rollbackPhase {
		return false
	}
	if status.PromotedThrough == "" {
		return true
	}
	if current < 1 {
		// A valid repin hold can move an already-advanced index back to zero
		// without clearing the durable promotion record from the old ladder.
		return status.PreStepHold
	}
	previousStep := steps[current-1]
	if previousStep.Analysis == nil && previousStep.Pause != nil && previousStep.Pause.Duration == nil {
		return status.PromotedThrough == status.CanaryRevisionHash
	}
	return true
}

// ValidRepinBoundary recognizes the exact boundary the controller can persist
// after replacing an active run's pinned plan. Raising repins carry
// PreStepHold; a global pause can durably preserve a non-raising boundary.
//
// A formatted PinnedAt alone is insufficient. The complete pinned plan,
// provenance, topology, target map, canary step body, typed traffic, and
// controller-producible clocks must all bind to one active epoch.
func ValidRepinBoundary(
	isvc *omev1beta1.InferenceService,
	primary omev1beta1.ComponentType,
	phase reportv1alpha1.RolloutPhase,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
	traffic []omev1beta1.ComponentTrafficTarget,
) bool {
	if isvc == nil || status == nil || len(steps) == 0 ||
		status.StepEnteredTime == nil || status.StepEnteredTime.IsZero() ||
		!promotingTrafficComplete(phase, status) ||
		!pinnedevidence.ValidCanaryRepin(
			isvc, primary, steps, status.CanaryRevisionHash,
		) ||
		!ActivePinnedTrafficMatches(isvc, primary, phase, status, traffic) {
		return false
	}
	if status.PreStepHold {
		return validPhaseStepResidue(phase, steps, status, true)
	}
	return validPausedNonRaisingBoundary(isvc, phase, steps, status)
}

func validPausedNonRaisingBoundary(
	isvc *omev1beta1.InferenceService,
	phase reportv1alpha1.RolloutPhase,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
) bool {
	paused, _ := constants.RolloutPauseState(isvc.Annotations)
	if !paused {
		return false
	}
	switch phase {
	case reportv1alpha1.RolloutPhaseCanarying,
		reportv1alpha1.RolloutPhasePaused,
		reportv1alpha1.RolloutPhasePromoting:
	default:
		return false
	}

	current := int(status.CurrentStep)
	if current < 0 || current >= len(steps) ||
		status.ObservedTrafficWeight < 0 || status.ObservedTrafficWeight > 100 ||
		steps[current].Traffic < 0 || steps[current].Traffic > status.ObservedTrafficWeight ||
		!SafeRevisionHash(status.CanaryRevisionHash) ||
		!SafeRevisionHash(status.StableRevisionHash) ||
		status.StableRevisionHash == status.CanaryRevisionHash ||
		status.RolledBackRevisionHash != "" ||
		status.StepEnteredTime == nil || status.StepEnteredTime.IsZero() ||
		isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil ||
		!isvc.Status.Rollout.ActiveRun.PinnedAt.Time.After(status.StepEnteredTime.Time) {
		return false
	}
	return true
}

// promotingTrafficComplete rejects an impossible promoting phase before any
// step or repin residue can qualify it. The controller enters Promoting only
// after applying the terminal 100% traffic split.
func promotingTrafficComplete(
	phase reportv1alpha1.RolloutPhase,
	status *omev1beta1.CanaryStatus,
) bool {
	return phase != reportv1alpha1.RolloutPhasePromoting ||
		status != nil && status.ObservedTrafficWeight == 100
}

// validPreStepHold recognizes only states the controller can persist after
// clampCanary arms a hold. A run boundary is flushed before the canary executor
// replaces the prior phase, and a global pause can keep that boundary durable.
// Repin also runs before rolled-back run closure, so rollback phases can carry
// the newly armed hold. The held traffic must remain a bounded, strictly lower
// exposure than the clamped step; equality or a reduction cannot have armed
// PreStepHold.
func validPreStepHold(
	phase reportv1alpha1.RolloutPhase,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
) bool {
	if status == nil || !status.PreStepHold ||
		status.ObservedTrafficWeight < 0 || status.ObservedTrafficWeight > 100 {
		return false
	}
	switch phase {
	case reportv1alpha1.RolloutPhasePending,
		reportv1alpha1.RolloutPhaseCanarying,
		reportv1alpha1.RolloutPhasePaused,
		reportv1alpha1.RolloutPhaseRollingBack,
		reportv1alpha1.RolloutPhaseRolledBack,
		reportv1alpha1.RolloutPhaseFailed:
	default:
		return false
	}
	current := int(status.CurrentStep)
	if current < 0 || current >= len(steps) {
		return false
	}
	target := steps[current].Traffic
	return target >= 0 && target <= 100 && status.ObservedTrafficWeight < target
}

// ActiveTrafficMatches validates that primary Traffic is the exact epoch
// described by active CanaryStatus.
func ActiveTrafficMatches(
	isvcName string,
	primary omev1beta1.ComponentType,
	phase reportv1alpha1.RolloutPhase,
	status *omev1beta1.CanaryStatus,
	traffic []omev1beta1.ComponentTrafficTarget,
) bool {
	if status == nil || status.ObservedTrafficWeight < 0 || status.ObservedTrafficWeight > 100 ||
		!SafeRevisionHash(status.CanaryRevisionHash) ||
		(status.StableRevisionHash != "" && !SafeRevisionHash(status.StableRevisionHash)) ||
		(status.StableRevisionHash != "" && status.StableRevisionHash == status.CanaryRevisionHash) {
		return false
	}

	expected := make(map[string]int32, 2)
	if phase == reportv1alpha1.RolloutPhaseRollingBack || phase == reportv1alpha1.RolloutPhaseRolledBack {
		if status.RolledBackRevisionHash == "" ||
			status.RolledBackRevisionHash != status.CanaryRevisionHash ||
			status.StableRevisionHash == "" || status.ObservedTrafficWeight != 0 {
			return false
		}
		expected[status.StableRevisionHash] = 100
	} else {
		if status.RolledBackRevisionHash != "" {
			return false
		}
		if status.ObservedTrafficWeight > 0 {
			expected[status.CanaryRevisionHash] = status.ObservedTrafficWeight
		}
		stableWeight := int32(100) - status.ObservedTrafficWeight
		if stableWeight > 0 {
			if status.StableRevisionHash == "" {
				return false
			}
			expected[status.StableRevisionHash] = stableWeight
		}
	}

	if len(traffic) != len(expected) {
		return false
	}
	seen := make(map[string]struct{}, len(traffic))
	for _, target := range traffic {
		hash := RevisionHash(isvcName, primary, target.RevisionName)
		if hash == "" {
			return false
		}
		if _, duplicate := seen[hash]; duplicate {
			return false
		}
		seen[hash] = struct{}{}
		expectedPercent, found := expected[hash]
		if !found || expectedPercent != target.Percent {
			return false
		}
	}
	return true
}

// ActivePinnedTrafficMatches additionally recognizes an unchanged primary
// when a different canary member changed. Equality is never sufficient alone:
// the complete frozen group and its target ID must prove that actor shape.
// This pure helper cannot prove owned IR freshness; mutation callers must also
// collect and bind every current IR target before submitting a mailbox.
func ActivePinnedTrafficMatches(
	isvc *omev1beta1.InferenceService,
	primary omev1beta1.ComponentType,
	phase reportv1alpha1.RolloutPhase,
	status *omev1beta1.CanaryStatus,
	traffic []omev1beta1.ComponentTrafficTarget,
) bool {
	if isvc == nil || status == nil {
		return false
	}
	if status.StableRevisionHash != status.CanaryRevisionHash {
		return ActiveTrafficMatches(isvc.Name, primary, phase, status, traffic)
	}
	if !SafeRevisionHash(status.CanaryRevisionHash) || status.ObservedTrafficWeight < 0 || status.ObservedTrafficWeight > 100 || !pinnedevidence.ValidActiveRun(isvc) {
		return false
	}
	if phase == reportv1alpha1.RolloutPhaseRollingBack || phase == reportv1alpha1.RolloutPhaseRolledBack {
		if status.RolledBackRevisionHash != status.CanaryRevisionHash || status.ObservedTrafficWeight != 0 {
			return false
		}
	} else if status.RolledBackRevisionHash != "" {
		return false
	}
	run := isvc.Status.Rollout.ActiveRun
	var canary *omev1beta1.RolloutGroup
	for i := range run.Plan.Groups {
		if run.Plan.Groups[i].Group.Canary != nil {
			if canary != nil {
				return false
			}
			canary = &run.Plan.Groups[i].Group
		}
	}
	if canary == nil {
		return false
	}
	groupPrimary, valid := Primary(canary.Components)
	if !valid || groupPrimary != primary {
		return false
	}
	targets := make(map[omev1beta1.ComponentType]omev1beta1.RolloutRunTarget, len(run.TargetRevisions))
	for _, target := range run.TargetRevisions {
		targets[target.Component] = target
	}
	parts := make([]string, 0, len(canary.Components))
	changedSecondary := false
	for _, component := range canary.Components {
		target := targets[component]
		if !SafeRevisionHash(target.StableRevision) {
			return false
		}
		parts = append(parts, string(component)+"="+target.Revision)
		if component == primary {
			if target.Revision != status.CanaryRevisionHash || target.StableRevision != target.Revision {
				return false
			}
		} else if target.StableRevision != target.Revision {
			changedSecondary = true
		}
	}
	sort.Strings(parts)
	return changedSecondary && status.TargetID == "ct1:"+rolloutpolicy.ShortHash([]byte(strings.Join(parts, ";"))) &&
		CompletedTrafficMatches(isvc.Name, primary, status.CanaryRevisionHash, traffic)
}

// CompletedTrafficMatches validates the single 100% target promised by a
// completed canary epoch.
func CompletedTrafficMatches(
	isvcName string,
	primary omev1beta1.ComponentType,
	targetHash string,
	traffic []omev1beta1.ComponentTrafficTarget,
) bool {
	if len(traffic) != 1 {
		return false
	}
	target := traffic[0]
	return target.Percent == 100 && RevisionHash(isvcName, primary, target.RevisionName) == targetHash
}

// CompletedStatusMatches validates the completion sentinel and the primary
// traffic epoch together, so an active CanaryStatus cannot be interpreted as
// completed merely because the component phase changed first.
func CompletedStatusMatches(
	isvcName string,
	primary omev1beta1.ComponentType,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
	traffic []omev1beta1.ComponentTrafficTarget,
) bool {
	return status != nil &&
		!status.PreStepHold &&
		len(steps) > 0 &&
		ValidCompletedStep(steps[len(steps)-1]) &&
		int(status.CurrentStep) == len(steps) &&
		status.ObservedTrafficWeight == 100 &&
		status.StableRevisionHash == "" &&
		status.RolledBackRevisionHash == "" &&
		status.PromotedThrough == "" &&
		SafeRevisionHash(status.CanaryRevisionHash) &&
		CompletedTrafficMatches(isvcName, primary, status.CanaryRevisionHash, traffic)
}

// ValidCompletedStep reports whether a canary plan may truthfully publish its
// completion sentinel after the final step.
func ValidCompletedStep(step omev1beta1.RolloutGroupStep) bool {
	if step.Traffic != 100 || !safeCapacity(step.Capacity) {
		return false
	}
	if step.Capacity.Type == intstr.Int {
		return step.Capacity.IntVal > 0
	}
	percentage, err := strconv.Atoi(strings.TrimSuffix(step.Capacity.StrVal, "%"))
	return err == nil && percentage == 100
}

func supportedComponent(component omev1beta1.ComponentType) bool {
	return component == omev1beta1.EngineComponent ||
		component == omev1beta1.DecoderComponent ||
		component == omev1beta1.RouterComponent
}

func safeCapacity(value intstr.IntOrString) bool {
	switch value.Type {
	case intstr.Int:
		return value.IntVal >= 0
	case intstr.String:
		raw := value.StrVal
		if !strings.HasSuffix(raw, "%") {
			return false
		}
		parsed, err := strconv.Atoi(strings.TrimSuffix(raw, "%"))
		return err == nil && parsed >= 0 && parsed <= 100
	default:
		return false
	}
}
