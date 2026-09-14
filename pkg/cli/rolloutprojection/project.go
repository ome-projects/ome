// Package rolloutprojection projects the rollout fields of one already-read
// InferenceService into the bounded kubectl-ome report contract.
package rolloutprojection

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/apis"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	omevalidation "sigs.k8s.io/ome/pkg/validation"
)

var (
	// ErrNilInferenceService indicates that projection received no subject.
	ErrNilInferenceService = errors.New("inference service is required")
	// ErrSubjectNameRequired indicates that the observed subject has no name.
	ErrSubjectNameRequired = errors.New("inference service name is required")
	// ErrNamespaceRequired indicates that the observed subject has no namespace.
	ErrNamespaceRequired = errors.New("inference service namespace is required")
	// ErrSubjectUIDRequired indicates that observed evidence cannot be bound to
	// an immutable InferenceService identity.
	ErrSubjectUIDRequired = errors.New("inference service UID is required")
)

var revisionHashPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)

// Project builds a safe, deterministic rollout report from one observed
// InferenceService. It performs no cluster reads.
func Project(
	isvc *omev1beta1.InferenceService,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.RolloutStatusReport, error) {
	if isvc == nil {
		return reportv1alpha1.RolloutStatusReport{}, ErrNilInferenceService
	}
	if isvc.Name == "" {
		return reportv1alpha1.RolloutStatusReport{}, ErrSubjectNameRequired
	}
	if isvc.Namespace == "" {
		return reportv1alpha1.RolloutStatusReport{}, ErrNamespaceRequired
	}
	if isvc.UID == "" {
		return reportv1alpha1.RolloutStatusReport{}, ErrSubjectUIDRequired
	}

	// Validate and project one rollout view: the active run's pinned plan when
	// present, including an intentionally empty plan, and the live spec otherwise.
	effectiveSpec := isvc.Spec
	effectiveSpec.Rollout = omev1beta1.EffectiveRollout(isvc)
	b := projector{
		isvc:          isvc,
		effectiveSpec: &effectiveSpec,
		groupFor:      make(map[omev1beta1.ComponentType]int),
		declared:      make(map[omev1beta1.ComponentType]struct{}),
		issueKeys:     make(map[string]struct{}),
		warningCodes:  make(map[reportv1alpha1.WarningCode]struct{}),
	}
	if !validStoredRolloutSpec(b.effectiveSpec) {
		b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, nil, "")
	}
	b.projectGroups()
	b.projectComponents()
	b.projectCoordinationReady()
	b.projectSummary()

	reportValue := reportv1alpha1.NewRolloutStatusReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name},
		b.content,
		clock,
	)
	reportValue.Sources = []reportv1alpha1.RolloutSourceReference{{
		Kind:       reportv1alpha1.RolloutSourceInferenceService,
		Namespace:  isvc.Namespace,
		Name:       isvc.Name,
		UID:        string(isvc.UID),
		Generation: isvc.Generation,
		Evidence:   reportv1alpha1.EvidenceObserved,
	}}
	for code := range b.warningCodes {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.RolloutWarning{Code: code})
	}
	return reportValue.Canonical(), nil
}

// validStoredRolloutSpec applies the same pure, static checks as admission and
// mirrors the small set of CRD-only shape constraints needed to consume an
// already-stored object safely. Validator error text is intentionally dropped:
// report diagnostics are code-only and must never copy arbitrary input.
func validStoredRolloutSpec(spec *omev1beta1.InferenceServiceSpec) bool {
	return validStoredRolloutPlan(spec) && omevalidation.ValidateRolloutOrderingEnforced(spec) == nil
}

// validStoredRolloutPlan mirrors CRD-only bounds and every pure rollout-body
// validator except ordering. Keeping ordering separate lets validation reports
// distinguish an unsupported sequence from a malformed progression body.
func validStoredRolloutPlan(spec *omev1beta1.InferenceServiceSpec) bool {
	groups := spec.GetRolloutGroups()
	if len(groups) > 3 {
		return false
	}
	for i := range groups {
		group := &groups[i]
		if len(group.Components) == 0 || len(group.Components) > 3 || len(group.Order) > 3 {
			return false
		}
		progressions := 0
		if group.Canary != nil {
			progressions++
		}
		if group.BlueGreen != nil {
			progressions++
		}
		if group.RollingUpdate != nil {
			progressions++
		}
		if progressions > 1 {
			return false
		}
		if group.Canary == nil {
			continue
		}
		if len(group.Canary.Steps) > 20 {
			return false
		}
		for stepIndex := range group.Canary.Steps {
			analysis := group.Canary.Steps[stepIndex].Analysis
			if analysis == nil {
				continue
			}
			if len(analysis.Metrics) > 10 {
				return false
			}
			if analysis.OnInconclusive != nil &&
				*analysis.OnInconclusive != omev1beta1.OnInconclusiveHold &&
				*analysis.OnInconclusive != omev1beta1.OnInconclusiveRollback {
				return false
			}
		}
	}
	if err := omevalidation.ValidateCanary(spec); err != nil {
		return false
	}
	if err := omevalidation.ValidateCoordination(spec); err != nil {
		return false
	}
	if err := omevalidation.ValidateLifecycle(spec); err != nil {
		return false
	}
	return true
}

type projector struct {
	isvc          *omev1beta1.InferenceService
	effectiveSpec *omev1beta1.InferenceServiceSpec
	content       reportv1alpha1.RolloutStatusContent
	groupFor      map[omev1beta1.ComponentType]int
	declared      map[omev1beta1.ComponentType]struct{}
	issueKeys     map[string]struct{}
	warningCodes  map[reportv1alpha1.WarningCode]struct{}
	malformed     bool
}

func (b *projector) projectGroups() {
	groups := b.effectiveSpec.GetRolloutGroups()
	if len(groups) == 0 {
		b.rejectUnexpectedComponentPhaseResidue()
		if b.isvc.Status.Canary != nil {
			b.markMalformed(reportv1alpha1.RolloutIssueCanaryStatusUnexpected, nil, "")
		}
		b.flagUnexpectedCoordination(nil)
		return
	}
	canaryGroups := 0
	for i := range groups {
		if groups[i].Canary != nil {
			canaryGroups++
		}
	}
	if canaryGroups > 1 {
		b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, nil, "")
	}

	if b.canCollapseSequential(groups) {
		b.projectSequential(groups)
	} else {
		if len(groups) > 1 {
			b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, nil, "")
		}
		for index := range groups {
			b.projectGroup(index, &groups[index])
		}
	}
	b.flagUnexpectedCoordination(groups)
}

func (b *projector) rejectUnexpectedComponentPhaseResidue() {
	for component, status := range b.isvc.Status.Components {
		if !supportedComponent(component) || status.RolloutPhase == "" || status.RolloutPhase == omev1beta1.RolloutPhaseStable {
			continue
		}
		b.markMalformed(
			reportv1alpha1.RolloutIssueStatusMalformed,
			nil,
			projectComponent(component),
		)
	}
}

func (b *projector) canCollapseSequential(groups []omev1beta1.RolloutGroup) bool {
	if len(groups) < 2 {
		return false
	}
	seen := make(map[omev1beta1.ComponentType]struct{}, len(groups))
	for i := range groups {
		group := &groups[i]
		if len(group.Components) != 1 || group.Canary != nil || group.RollingUpdate != nil {
			return false
		}
		if _, duplicate := seen[group.Components[0]]; duplicate {
			return false
		}
		if !supportedComponent(group.Components[0]) {
			return false
		}
		seen[group.Components[0]] = struct{}{}
	}
	return true
}

func (b *projector) projectSequential(groups []omev1beta1.RolloutGroup) {
	projected := reportv1alpha1.RolloutGroupStatus{
		Index:      0,
		Strategy:   reportv1alpha1.RolloutStrategySequential,
		Phase:      reportv1alpha1.RolloutPhaseUnknown,
		Components: make([]reportv1alpha1.RuntimeComponentType, 0, len(groups)),
	}
	expectedComponents := make([]omev1beta1.ComponentType, 0, len(groups))
	for i := range groups {
		component := groups[i].Components[0]
		expectedComponents = append(expectedComponents, component)
		b.declared[component] = struct{}{}
		b.groupFor[component] = 0
		projected.Components = append(projected.Components, projectComponent(component))
	}
	if observed := b.coordinationGroup("0"); observed != nil {
		b.applyCoordinationStatus(
			&projected,
			observed,
			omev1beta1.CoordinationPolicySequential,
			expectedComponents,
		)
	} else {
		b.addIssue(reportv1alpha1.RolloutIssueGroupStatusMissing, ptrInt(0), "")
	}
	b.content.Groups = append(b.content.Groups, projected)
}

func (b *projector) projectGroup(index int, group *omev1beta1.RolloutGroup) {
	projected := reportv1alpha1.RolloutGroupStatus{
		Index: index, Phase: reportv1alpha1.RolloutPhaseUnknown,
	}
	progressions := 0
	if group.Canary != nil {
		progressions++
		projected.Strategy = reportv1alpha1.RolloutStrategyCanary
	}
	if group.BlueGreen != nil {
		progressions++
		projected.Strategy = reportv1alpha1.RolloutStrategyBlueGreen
	}
	if group.RollingUpdate != nil {
		progressions++
		projected.Strategy = reportv1alpha1.RolloutStrategyRollingUpdate
	}
	if progressions == 0 {
		projected.Strategy = reportv1alpha1.RolloutStrategyBlueGreen
	}
	if progressions > 1 {
		projected.Strategy = reportv1alpha1.RolloutStrategyUnknown
		b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, ptrInt(index), "")
	}

	seen := make(map[omev1beta1.ComponentType]struct{}, len(group.Components))
	for _, component := range group.Components {
		if !supportedComponent(component) {
			b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, ptrInt(index), "")
			continue
		}
		if _, duplicate := seen[component]; duplicate {
			b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, ptrInt(index), projectComponent(component))
			continue
		}
		seen[component] = struct{}{}
		if _, alreadyGrouped := b.groupFor[component]; alreadyGrouped {
			b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, ptrInt(index), projectComponent(component))
			continue
		}
		b.declared[component] = struct{}{}
		b.groupFor[component] = index
		projected.Components = append(projected.Components, projectComponent(component))
	}
	if len(projected.Components) == 0 {
		b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, ptrInt(index), "")
	}

	if group.Canary != nil && progressions == 1 {
		b.applyCanaryStatus(&projected, group)
	} else if progressions <= 1 {
		expectedPolicy := omev1beta1.CoordinationPolicyBlueGreen
		if group.RollingUpdate != nil {
			expectedPolicy = omev1beta1.CoordinationPolicyRollingUpdate
		}
		if observed := b.coordinationGroup(strconv.Itoa(index)); observed != nil {
			b.applyCoordinationStatus(&projected, observed, expectedPolicy, group.Components)
		} else {
			b.addIssue(reportv1alpha1.RolloutIssueGroupStatusMissing, ptrInt(index), "")
		}
	}
	b.content.Groups = append(b.content.Groups, projected)
}

func (b *projector) applyCanaryStatus(
	projected *reportv1alpha1.RolloutGroupStatus,
	group *omev1beta1.RolloutGroup,
) {
	primary := canaryPrimary(group.Components)
	projected.Phase = b.canaryGroupPhase(projected.Index, group.Components, primary)
	if primary == "" {
		b.markMalformed(reportv1alpha1.RolloutIssueSpecMalformed, ptrInt(projected.Index), "")
		return
	}
	if projected.Phase == reportv1alpha1.RolloutPhaseBlueGreenStandby {
		b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
	}
	status := b.isvc.Status.Canary
	if status == nil {
		if canaryPhaseNeedsStatus(projected.Phase) {
			b.addIssue(reportv1alpha1.RolloutIssueCanaryStatusMissing, ptrInt(projected.Index), "")
		}
		return
	}
	projected.StableRevisionHash = b.safeDirectHash(status.StableRevisionHash, projected.Index)
	projected.TargetRevisionHash = b.safeDirectHash(status.CanaryRevisionHash, projected.Index)
	projected.RejectedRevisionHash = b.safeDirectHash(status.RolledBackRevisionHash, projected.Index)
	if status.CanaryRevisionHash == "" {
		b.markMalformed(reportv1alpha1.RolloutIssueRevisionNameInvalid, ptrInt(projected.Index), "")
	}
	if projected.Phase == reportv1alpha1.RolloutPhaseStable {
		b.applyCompletedCanaryStatus(projected, group, primary, status)
		return
	}
	if !canaryPhaseNeedsStatus(projected.Phase) {
		b.markMalformed(reportv1alpha1.RolloutIssueCanaryStatusUnexpected, ptrInt(projected.Index), "")
		return
	}
	if status.CurrentStep < 0 || int(status.CurrentStep) >= len(group.Canary.Steps) {
		b.markMalformed(reportv1alpha1.RolloutIssueCanaryStepInvalid, ptrInt(projected.Index), "")
		return
	}
	if !validCanaryPhaseStepResidue(projected.Phase, group.Canary.Steps, status) {
		b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
	}
	if canaryPhaseBindsTraffic(projected.Phase) &&
		!b.activeCanaryTrafficMatches(primary, projected.Phase, status) {
		b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
	}
	stepSpec := &group.Canary.Steps[status.CurrentStep]
	if canaryPhaseBindsStepTraffic(projected.Phase) &&
		!canaryObservedTrafficMatchesStep(projected.Phase, group.Canary.Steps, status) {
		b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
	}
	step := reportv1alpha1.RolloutStepStatus{
		Index:           status.CurrentStep,
		Total:           int32(len(group.Canary.Steps)),
		TargetTraffic:   stepSpec.Traffic,
		ObservedTraffic: status.ObservedTrafficWeight,
		Gate:            gateFor(stepSpec),
	}
	if safeCapacity(stepSpec.Capacity) {
		step.Capacity = stepSpec.Capacity.String()
	} else {
		b.markMalformed(reportv1alpha1.RolloutIssueCanaryStepInvalid, ptrInt(projected.Index), "")
	}
	if step.TargetTraffic < 0 || step.TargetTraffic > 100 || step.ObservedTraffic < 0 || step.ObservedTraffic > 100 {
		step.TargetTraffic = 0
		step.ObservedTraffic = 0
		b.markMalformed(reportv1alpha1.RolloutIssueTrafficInvalid, ptrInt(projected.Index), "")
	}
	if stepSpec.Analysis != nil {
		var valid bool
		step.Analysis, valid = analysisState(stepSpec.Analysis, status)
		if !valid {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
		} else if step.Analysis == reportv1alpha1.RolloutAnalysisInconclusive {
			b.addIssue(reportv1alpha1.RolloutIssueAnalysisInconclusive, ptrInt(projected.Index), "")
		}
	}
	if status.StepEnteredTime != nil {
		if status.StepEnteredTime.IsZero() {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
		} else {
			enteredAt := status.StepEnteredTime.Time.UTC()
			step.EnteredAt = &enteredAt
		}
	}
	projected.Step = &step
}

func (b *projector) applyCompletedCanaryStatus(
	projected *reportv1alpha1.RolloutGroupStatus,
	group *omev1beta1.RolloutGroup,
	primary omev1beta1.ComponentType,
	status *omev1beta1.CanaryStatus,
) {
	validFinalStep := false
	if len(group.Canary.Steps) > 0 {
		finalStep := group.Canary.Steps[len(group.Canary.Steps)-1]
		validFinalStep = finalStep.Traffic == 100 && validCompletedCanaryCapacity(finalStep.Capacity)
	}
	valid := validFinalStep &&
		int(status.CurrentStep) == len(group.Canary.Steps) &&
		status.ObservedTrafficWeight == 100 &&
		status.StableRevisionHash == "" &&
		status.RolledBackRevisionHash == "" &&
		status.PromotedThrough == "" &&
		projected.TargetRevisionHash != "" &&
		b.completedCanaryTrafficMatches(primary, projected.TargetRevisionHash)
	if !valid {
		b.markMalformed(reportv1alpha1.RolloutIssueCanaryStepInvalid, ptrInt(projected.Index), "")
	}
}

func (b *projector) activeCanaryTrafficMatches(
	primary omev1beta1.ComponentType,
	phase reportv1alpha1.RolloutPhase,
	status *omev1beta1.CanaryStatus,
) bool {
	if status.ObservedTrafficWeight < 0 || status.ObservedTrafficWeight > 100 ||
		!safeRevisionHash(status.CanaryRevisionHash) ||
		(status.StableRevisionHash != "" && !safeRevisionHash(status.StableRevisionHash)) ||
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

	component, found := b.isvc.Status.Components[primary]
	if !found || len(component.Traffic) != len(expected) {
		return false
	}
	seen := make(map[string]struct{}, len(component.Traffic))
	for _, target := range component.Traffic {
		hash := extractRevisionHash(b.isvc.Name, primary, target.RevisionName)
		if hash == "" {
			return false
		}
		if _, duplicate := seen[hash]; duplicate {
			return false
		}
		seen[hash] = struct{}{}
		expectedPercent, expectedHash := expected[hash]
		if !expectedHash || expectedPercent != target.Percent {
			return false
		}
	}
	return true
}

func (b *projector) completedCanaryTrafficMatches(
	primary omev1beta1.ComponentType,
	targetHash string,
) bool {
	component, found := b.isvc.Status.Components[primary]
	if !found || len(component.Traffic) != 1 {
		return false
	}
	target := component.Traffic[0]
	return target.Percent == 100 &&
		extractRevisionHash(b.isvc.Name, primary, target.RevisionName) == targetHash
}

func (b *projector) applyCoordinationStatus(
	projected *reportv1alpha1.RolloutGroupStatus,
	observed *omev1beta1.RolloutCoordinationGroupStatus,
	expected omev1beta1.CoordinationPolicy,
	expectedComponents []omev1beta1.ComponentType,
) {
	projected.Phase = projectCoordinationPhase(observed.Phase)
	if projected.Phase == reportv1alpha1.RolloutPhaseUnknown {
		if observed.Phase == "" {
			b.addIssue(reportv1alpha1.RolloutIssueGroupStatusMissing, ptrInt(projected.Index), "")
		} else {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
		}
	}
	if observed.Policy != expected {
		b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
	}
	if !slices.Equal(observed.Components, expectedComponents) {
		b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
	}
	if expected == omev1beta1.CoordinationPolicySequential {
		if !slices.Equal(observed.Order, expectedComponents) ||
			!validSequentialCursor(
				expectedComponents,
				observed.Phase,
				observed.CurrentComponent,
				observed.PreviousComponent,
			) ||
			!validSequentialComposite(
				observed.Phase,
				observed.CompositePhase,
				observed.CurrentComponent,
				observed.PreviousComponent,
			) {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
		} else if observed.Phase == omev1beta1.CoordinationPhaseIdle &&
			observed.CompositePhase == omev1beta1.CompositePhaseSequentialAwaiting {
			projected.Phase = reportv1alpha1.RolloutPhaseAwaitingNextComponent
		}
	} else if len(observed.Order) > 0 || observed.CurrentComponent != "" || observed.PreviousComponent != "" {
		b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
	}
	if observed.CurrentComponent != "" {
		if b.groupContains(projected, observed.CurrentComponent) {
			projected.CurrentComponent = projectComponent(observed.CurrentComponent)
		} else {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
		}
	}
	if observed.PreviousComponent != "" {
		if b.groupContains(projected, observed.PreviousComponent) {
			projected.PreviousComponent = projectComponent(observed.PreviousComponent)
		} else {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
		}
	}
	if observed.ObservedSurge != nil {
		if safeCapacity(*observed.ObservedSurge) {
			projected.ObservedSurge = observed.ObservedSurge.String()
		} else {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
		}
	}
	if observed.LastTransitionTime != nil {
		if observed.LastTransitionTime.IsZero() {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, ptrInt(projected.Index), "")
		} else {
			transitionedAt := observed.LastTransitionTime.Time.UTC()
			projected.TransitionedAt = &transitionedAt
		}
	}
}

func (b *projector) projectComponents() {
	if b.isvc.Spec.Engine != nil {
		b.declared[omev1beta1.EngineComponent] = struct{}{}
	}
	if b.isvc.Spec.Decoder != nil {
		b.declared[omev1beta1.DecoderComponent] = struct{}{}
	}
	if b.isvc.Spec.Router != nil {
		b.declared[omev1beta1.RouterComponent] = struct{}{}
	}
	for component := range b.isvc.Status.Components {
		if supportedComponent(component) {
			b.declared[component] = struct{}{}
		} else {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, nil, "")
		}
	}
	components := make([]omev1beta1.ComponentType, 0, len(b.declared))
	for component := range b.declared {
		components = append(components, component)
	}
	sort.Slice(components, func(i, j int) bool { return componentOrder(components[i]) < componentOrder(components[j]) })
	for _, component := range components {
		projected := reportv1alpha1.RolloutComponentStatus{
			Type:     projectComponent(component),
			Strategy: reportv1alpha1.RolloutStrategyUnknown,
			Phase:    reportv1alpha1.RolloutPhaseUnknown,
			Traffic:  []reportv1alpha1.RolloutTrafficTarget{},
		}
		if group, found := b.groupFor[component]; found {
			projected.Group = ptrInt(group)
			projected.Strategy = b.groupStrategy(group)
		} else if b.componentExplicitlyOMENative(component) {
			projected.Strategy = reportv1alpha1.RolloutStrategyIndependent
		}
		observed, found := b.isvc.Status.Components[component]
		if !found {
			if projected.Group != nil || !b.componentExplicitlyNonOMENative(component) {
				b.addIssue(reportv1alpha1.RolloutIssueComponentStatusMissing, projected.Group, projected.Type)
			}
			b.content.Components = append(b.content.Components, projected)
			continue
		}
		if projected.Group == nil && observed.Lifecycle != nil {
			projected.Strategy = reportv1alpha1.RolloutStrategyIndependent
			projected.Phase = b.independentLifecyclePhase(component, observed.Lifecycle)
			if observed.RolloutPhase != "" &&
				projectComponentPhase(observed.RolloutPhase) != projected.Phase {
				b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, nil, projected.Type)
				projected.Phase = reportv1alpha1.RolloutPhaseUnknown
			}
		} else {
			if projected.Group == nil && !b.componentExplicitlyNonOMENative(component) {
				b.addIssue(reportv1alpha1.RolloutIssueComponentStatusMissing, nil, projected.Type)
			}
			projected.Phase = projectComponentPhase(observed.RolloutPhase)
			coordinated := b.nonCanaryCoordinationGroup(projected.Group)
			if projected.Group != nil && observed.RolloutPhase == "" &&
				!coordinated &&
				!b.normalEmptyCanarySecondary(component, projected.Group) {
				b.addIssue(reportv1alpha1.RolloutIssueComponentStatusMissing, projected.Group, projected.Type)
			}
			if projected.Phase == reportv1alpha1.RolloutPhaseUnknown && observed.RolloutPhase != "" {
				b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, projected.Group, projected.Type)
			}
			if coordinated &&
				projected.Phase != reportv1alpha1.RolloutPhaseUnknown &&
				b.nonCanaryComponentPhaseContradicts(projected.Group, projected.Phase) {
				b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, projected.Group, projected.Type)
			}
		}
		projected.RolledOutRevisionHash = b.serviceRevisionHash(observed.LatestRolledoutRevision, component, projected.Group)
		projected.ReadyRevisionHash = b.serviceRevisionHash(observed.LatestReadyRevision, component, projected.Group)
		projected.PreviousRevisionHash = b.serviceRevisionHash(observed.PreviousRolledoutRevision, component, projected.Group)
		projected.Traffic = b.projectTraffic(observed.Traffic, component, &projected)
		b.content.Components = append(b.content.Components, projected)
	}
}

func (b *projector) groupStrategy(index int) reportv1alpha1.RolloutStrategy {
	for _, group := range b.content.Groups {
		if group.Index == index {
			return group.Strategy
		}
	}
	return reportv1alpha1.RolloutStrategyUnknown
}

func (b *projector) independentLifecyclePhase(
	component omev1beta1.ComponentType,
	lifecycle *omev1beta1.LifecycleStatus,
) reportv1alpha1.RolloutPhase {
	projectedComponent := projectComponent(component)
	if lifecycle.CurrentRevision == "" || lifecycle.UpdateRevision == "" {
		b.addIssue(reportv1alpha1.RolloutIssueComponentStatusMissing, nil, projectedComponent)
		return reportv1alpha1.RolloutPhaseUnknown
	}
	currentHash := extractControllerRevisionHash(b.isvc.Name, component, lifecycle.CurrentRevision)
	updateHash := extractControllerRevisionHash(b.isvc.Name, component, lifecycle.UpdateRevision)
	if currentHash == "" || updateHash == "" {
		b.markMalformed(reportv1alpha1.RolloutIssueRevisionNameInvalid, nil, projectedComponent)
		return reportv1alpha1.RolloutPhaseUnknown
	}
	if lifecycle.CurrentRevision == lifecycle.UpdateRevision {
		return reportv1alpha1.RolloutPhaseStable
	}
	return reportv1alpha1.RolloutPhaseUpdating
}

func (b *projector) componentExplicitlyNonOMENative(component omev1beta1.ComponentType) bool {
	mode, found := b.componentDeploymentModeAnnotation(component)
	return found && mode.IsValid() && mode != constants.OMENative
}

func (b *projector) componentExplicitlyOMENative(component omev1beta1.ComponentType) bool {
	mode, found := b.componentDeploymentModeAnnotation(component)
	return found && mode == constants.OMENative
}

func (b *projector) componentDeploymentModeAnnotation(
	component omev1beta1.ComponentType,
) (constants.DeploymentModeType, bool) {
	var annotations map[string]string
	switch component {
	case omev1beta1.EngineComponent:
		if b.isvc.Spec.Engine == nil {
			return "", false
		}
		annotations = b.isvc.Spec.Engine.Annotations
	case omev1beta1.DecoderComponent:
		if b.isvc.Spec.Decoder == nil {
			return "", false
		}
		annotations = b.isvc.Spec.Decoder.Annotations
	case omev1beta1.RouterComponent:
		if b.isvc.Spec.Router == nil {
			return "", false
		}
		annotations = b.isvc.Spec.Router.Annotations
	default:
		return "", false
	}
	raw, found := annotations[constants.DeploymentMode]
	if !found {
		return "", false
	}
	return constants.DeploymentModeType(raw), true
}

func (b *projector) provesNotConfigured() bool {
	if b.hasRolloutStatusResidue() {
		return false
	}
	for _, component := range []omev1beta1.ComponentType{
		omev1beta1.EngineComponent,
		omev1beta1.DecoderComponent,
		omev1beta1.RouterComponent,
	} {
		if b.componentDeclared(component) && !b.componentExplicitlyNonOMENative(component) {
			return false
		}
	}
	return true
}

func (b *projector) componentDeclared(component omev1beta1.ComponentType) bool {
	switch component {
	case omev1beta1.EngineComponent:
		return b.isvc.Spec.Engine != nil
	case omev1beta1.DecoderComponent:
		return b.isvc.Spec.Decoder != nil
	case omev1beta1.RouterComponent:
		return b.isvc.Spec.Router != nil
	default:
		return false
	}
}

func (b *projector) hasRolloutStatusResidue() bool {
	if b.isvc.Status.Canary != nil || b.isvc.Status.RolloutCoordination != nil {
		return true
	}
	if rollout := b.isvc.Status.Rollout; rollout != nil &&
		(rollout.ActiveRun != nil || rollout.LastRun != nil || len(rollout.Groups) > 0) {
		return true
	}
	if b.hasComponentStatusResidue() {
		return true
	}
	for _, condition := range b.isvc.Status.Conditions {
		switch condition.Type {
		case apis.ConditionType(omev1beta1.RolloutCoordinationReady),
			apis.ConditionType(omev1beta1.RolloutPlanReadyCondition),
			apis.ConditionType(omev1beta1.RolloutPlanDriftCondition):
			return true
		}
	}
	return false
}

func (b *projector) hasComponentStatusResidue() bool {
	for _, component := range b.isvc.Status.Components {
		if component.Lifecycle != nil || component.RolloutPhase != "" ||
			component.LatestReadyRevision != "" || component.LatestRolledoutRevision != "" ||
			component.PreviousRolledoutRevision != "" || len(component.Traffic) > 0 {
			return true
		}
	}
	return false
}

func (b *projector) projectTraffic(
	observed []omev1beta1.ComponentTrafficTarget,
	component omev1beta1.ComponentType,
	projected *reportv1alpha1.RolloutComponentStatus,
) []reportv1alpha1.RolloutTrafficTarget {
	if len(observed) == 0 {
		return []reportv1alpha1.RolloutTrafficTarget{}
	}
	result := make([]reportv1alpha1.RolloutTrafficTarget, 0, len(observed))
	valid := true
	total := int64(0)
	seen := make(map[string]struct{}, len(observed))
	for _, target := range observed {
		hash := extractRevisionHash(b.isvc.Name, component, target.RevisionName)
		if hash == "" || target.Percent < 0 || target.Percent > 100 {
			valid = false
			continue
		}
		if _, duplicate := seen[hash]; duplicate {
			valid = false
			continue
		}
		seen[hash] = struct{}{}
		total += int64(target.Percent)
		result = append(result, reportv1alpha1.RolloutTrafficTarget{
			RevisionHash: hash,
			Percent:      target.Percent,
			Role:         revisionRole(hash, projected),
		})
	}
	if total != 100 {
		valid = false
	}
	if !valid {
		b.markMalformed(reportv1alpha1.RolloutIssueTrafficInvalid, projected.Group, projected.Type)
		return []reportv1alpha1.RolloutTrafficTarget{}
	}
	return result
}

func (b *projector) projectCoordinationReady() {
	hasCoordination := false
	for _, group := range b.content.Groups {
		if group.Strategy != reportv1alpha1.RolloutStrategyCanary && group.Strategy != reportv1alpha1.RolloutStrategyUnknown {
			hasCoordination = true
			break
		}
	}
	if !hasCoordination {
		b.content.Summary.CoordinationReady = reportv1alpha1.RolloutConditionNotApplicable
		return
	}
	expected := b.coordinationReadyFromGroups()
	var matches []apis.Condition
	for _, condition := range b.isvc.Status.Conditions {
		if condition.Type == apis.ConditionType(omev1beta1.RolloutCoordinationReady) {
			matches = append(matches, condition)
		}
	}
	switch len(matches) {
	case 0:
		b.content.Summary.CoordinationReady = reportv1alpha1.RolloutConditionUnobserved
	case 1:
		switch matches[0].Status {
		case corev1.ConditionTrue:
			b.content.Summary.CoordinationReady = reportv1alpha1.RolloutConditionTrue
		case corev1.ConditionFalse:
			b.content.Summary.CoordinationReady = reportv1alpha1.RolloutConditionFalse
		case corev1.ConditionUnknown:
			b.content.Summary.CoordinationReady = reportv1alpha1.RolloutConditionUnknown
		default:
			b.content.Summary.CoordinationReady = reportv1alpha1.RolloutConditionInvalid
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, nil, "")
		}
		if b.content.Summary.CoordinationReady != reportv1alpha1.RolloutConditionInvalid &&
			b.content.Summary.CoordinationReady != expected {
			b.content.Summary.CoordinationReady = reportv1alpha1.RolloutConditionInvalid
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, nil, "")
		}
	default:
		b.content.Summary.CoordinationReady = reportv1alpha1.RolloutConditionInvalid
		b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, nil, "")
	}
}

func (b *projector) projectSummary() {
	reportedState := b.reportedState()
	b.content.Summary.ReportedState = reportedState
	if b.summaryDependsOnParentStatus() {
		b.content.Summary.State = reportv1alpha1.RolloutStateUnknown
		b.content.Summary.Evidence = reportv1alpha1.EvidenceReported
		b.content.Summary.Epoch = reportv1alpha1.RolloutEpochUnverifiable
		b.addIssue(reportv1alpha1.RolloutIssueEpochUnverifiable, nil, "")
		return
	}
	b.content.Summary.State = reportedState
	b.content.Summary.Evidence = reportv1alpha1.EvidenceDeclared
	b.content.Summary.Epoch = reportv1alpha1.RolloutEpochNotApplicable
}

func (b *projector) reportedState() reportv1alpha1.RolloutState {
	if b.malformed {
		return reportv1alpha1.RolloutStateUnknown
	}
	if len(b.effectiveSpec.GetRolloutGroups()) == 0 && !hasIndependentComponent(b.content.Components) {
		if b.provesNotConfigured() {
			return reportv1alpha1.RolloutStateNotConfigured
		}
		return reportv1alpha1.RolloutStateUnknown
	}
	phases := make([]reportv1alpha1.RolloutPhase, 0, len(b.content.Groups)+len(b.content.Components))
	for _, group := range b.content.Groups {
		phases = append(phases, group.Phase)
	}
	for _, component := range b.content.Components {
		if b.componentPhaseContributes(component) {
			phases = append(phases, component.Phase)
		}
	}
	switch {
	case hasPhase(phases, reportv1alpha1.RolloutPhaseFailed):
		return reportv1alpha1.RolloutStateFailed
	case hasPhase(phases, reportv1alpha1.RolloutPhaseRollingBack):
		return reportv1alpha1.RolloutStateRollingBack
	case hasPhase(phases, reportv1alpha1.RolloutPhaseRolledBack):
		return reportv1alpha1.RolloutStateRolledBack
	case hasPhase(phases, reportv1alpha1.RolloutPhasePaused):
		return reportv1alpha1.RolloutStatePaused
	case hasPhase(phases, reportv1alpha1.RolloutPhaseUnknown):
		return reportv1alpha1.RolloutStateUnknown
	case anyActivePhase(phases):
		return reportv1alpha1.RolloutStateInProgress
	case hasPhase(phases, reportv1alpha1.RolloutPhaseStaged):
		return reportv1alpha1.RolloutStateStaged
	default:
		return reportv1alpha1.RolloutStateSucceeded
	}
}

func (b *projector) summaryDependsOnParentStatus() bool {
	if len(b.effectiveSpec.GetRolloutGroups()) == 0 &&
		!hasIndependentComponent(b.content.Components) &&
		!b.provesNotConfigured() {
		return true
	}
	if len(b.effectiveSpec.GetRolloutGroups()) > 0 {
		return true
	}
	return b.hasRolloutStatusResidue()
}

func (b *projector) componentPhaseContributes(component reportv1alpha1.RolloutComponentStatus) bool {
	if component.Group == nil {
		return component.Strategy == reportv1alpha1.RolloutStrategyIndependent
	}
	apiComponent := apiComponent(component.Type)
	if b.nonCanaryCoordinationGroup(component.Group) {
		_, found := b.isvc.Status.Components[apiComponent]
		return !found
	}
	return !b.normalEmptyCanarySecondary(apiComponent, component.Group)
}

func hasIndependentComponent(components []reportv1alpha1.RolloutComponentStatus) bool {
	for _, component := range components {
		if component.Strategy == reportv1alpha1.RolloutStrategyIndependent {
			return true
		}
	}
	return false
}

func (b *projector) nonCanaryCoordinationGroup(groupIndex *int) bool {
	if groupIndex == nil || *groupIndex < 0 || *groupIndex >= len(b.content.Groups) {
		return false
	}
	switch b.content.Groups[*groupIndex].Strategy {
	case reportv1alpha1.RolloutStrategyBlueGreen,
		reportv1alpha1.RolloutStrategyRollingUpdate,
		reportv1alpha1.RolloutStrategySequential:
		return true
	default:
		return false
	}
}

func (b *projector) nonCanaryComponentPhaseContradicts(
	groupIndex *int,
	componentPhase reportv1alpha1.RolloutPhase,
) bool {
	if !b.nonCanaryCoordinationGroup(groupIndex) {
		return false
	}
	groupPhase := b.content.Groups[*groupIndex].Phase
	switch groupPhase {
	case reportv1alpha1.RolloutPhaseIdle,
		reportv1alpha1.RolloutPhaseAwaitingNextComponent:
		return componentPhase != reportv1alpha1.RolloutPhaseStable
	case reportv1alpha1.RolloutPhaseStaged:
		return componentPhase != reportv1alpha1.RolloutPhaseStable &&
			componentPhase != reportv1alpha1.RolloutPhaseBlueGreenStandby
	case reportv1alpha1.RolloutPhaseFailed:
		return componentPhase != reportv1alpha1.RolloutPhaseStable &&
			componentPhase != reportv1alpha1.RolloutPhaseFailed
	case reportv1alpha1.RolloutPhaseRollingBack:
		return componentPhase != reportv1alpha1.RolloutPhaseStable &&
			componentPhase != reportv1alpha1.RolloutPhaseRollingBack &&
			componentPhase != reportv1alpha1.RolloutPhaseRolledBack
	case reportv1alpha1.RolloutPhasePaused:
		return componentPhase == reportv1alpha1.RolloutPhaseFailed ||
			componentPhase == reportv1alpha1.RolloutPhaseRollingBack ||
			componentPhase == reportv1alpha1.RolloutPhaseRolledBack
	case reportv1alpha1.RolloutPhaseSurging,
		reportv1alpha1.RolloutPhaseWaiting,
		reportv1alpha1.RolloutPhaseShifting,
		reportv1alpha1.RolloutPhaseDraining,
		reportv1alpha1.RolloutPhaseScalingDown:
		return componentPhase == reportv1alpha1.RolloutPhaseFailed ||
			componentPhase == reportv1alpha1.RolloutPhaseRollingBack ||
			componentPhase == reportv1alpha1.RolloutPhaseRolledBack ||
			componentPhase == reportv1alpha1.RolloutPhasePaused
	default:
		return false
	}
}

func (b *projector) normalEmptyCanarySecondary(component omev1beta1.ComponentType, groupIndex *int) bool {
	if groupIndex == nil || component == "" {
		return false
	}
	groups := b.effectiveSpec.GetRolloutGroups()
	if *groupIndex < 0 || *groupIndex >= len(groups) || groups[*groupIndex].Canary == nil {
		return false
	}
	if component == canaryPrimary(groups[*groupIndex].Components) {
		return false
	}
	observed, found := b.isvc.Status.Components[component]
	return found && observed.RolloutPhase == ""
}

func (b *projector) coordinationReadyFromGroups() reportv1alpha1.RolloutConditionState {
	anyFailed := false
	anyActive := false
	anyUnknown := false
	for _, group := range b.content.Groups {
		if group.Strategy == reportv1alpha1.RolloutStrategyCanary ||
			group.Strategy == reportv1alpha1.RolloutStrategyUnknown {
			continue
		}
		switch group.Phase {
		case reportv1alpha1.RolloutPhaseFailed:
			anyFailed = true
		case reportv1alpha1.RolloutPhaseIdle,
			reportv1alpha1.RolloutPhaseStaged,
			reportv1alpha1.RolloutPhaseAwaitingNextComponent:
		case reportv1alpha1.RolloutPhaseUnknown:
			anyUnknown = true
		default:
			anyActive = true
		}
	}
	switch {
	case anyFailed, anyActive:
		return reportv1alpha1.RolloutConditionFalse
	case anyUnknown:
		return reportv1alpha1.RolloutConditionUnknown
	default:
		return reportv1alpha1.RolloutConditionTrue
	}
}

func (b *projector) flagUnexpectedCoordination(groups []omev1beta1.RolloutGroup) {
	if b.isvc.Status.RolloutCoordination == nil {
		return
	}
	expected := make(map[string]struct{})
	if b.canCollapseSequential(groups) {
		expected["0"] = struct{}{}
	} else {
		for i := range groups {
			if groups[i].Canary == nil {
				expected[strconv.Itoa(i)] = struct{}{}
			}
		}
	}
	for _, observed := range b.isvc.Status.RolloutCoordination.Groups {
		if _, found := expected[observed.Name]; !found {
			b.markMalformed(reportv1alpha1.RolloutIssueGroupStatusUnexpected, nil, "")
		}
	}
}

func (b *projector) coordinationGroup(name string) *omev1beta1.RolloutCoordinationGroupStatus {
	if b.isvc.Status.RolloutCoordination == nil {
		return nil
	}
	var found *omev1beta1.RolloutCoordinationGroupStatus
	for i := range b.isvc.Status.RolloutCoordination.Groups {
		candidate := &b.isvc.Status.RolloutCoordination.Groups[i]
		if candidate.Name != name {
			continue
		}
		if found != nil {
			b.markMalformed(reportv1alpha1.RolloutIssueStatusMalformed, nil, "")
			return found
		}
		found = candidate
	}
	return found
}

func (b *projector) canaryGroupPhase(
	group int,
	components []omev1beta1.ComponentType,
	primary omev1beta1.ComponentType,
) reportv1alpha1.RolloutPhase {
	if primary == "" {
		return reportv1alpha1.RolloutPhaseUnknown
	}
	primaryStatus, found := b.isvc.Status.Components[primary]
	if !found {
		return reportv1alpha1.RolloutPhaseUnknown
	}
	primaryPhase := projectComponentPhase(primaryStatus.RolloutPhase)
	if primaryStatus.RolloutPhase != "" && primaryPhase == reportv1alpha1.RolloutPhaseUnknown {
		b.markMalformed(
			reportv1alpha1.RolloutIssueStatusMalformed,
			ptrInt(group),
			projectComponent(primary),
		)
	}
	for _, component := range components {
		if component == primary {
			continue
		}
		secondary, found := b.isvc.Status.Components[component]
		if !found || secondary.RolloutPhase == "" {
			continue
		}
		secondaryPhase := projectComponentPhase(secondary.RolloutPhase)
		if secondaryPhase == reportv1alpha1.RolloutPhaseUnknown ||
			(primaryPhase != reportv1alpha1.RolloutPhaseUnknown && secondaryPhase != primaryPhase) {
			b.markMalformed(
				reportv1alpha1.RolloutIssueStatusMalformed,
				ptrInt(group),
				projectComponent(component),
			)
		}
	}
	return primaryPhase
}

func canaryPrimary(components []omev1beta1.ComponentType) omev1beta1.ComponentType {
	for _, candidate := range []omev1beta1.ComponentType{
		omev1beta1.RouterComponent,
		omev1beta1.EngineComponent,
		omev1beta1.DecoderComponent,
	} {
		if slices.Contains(components, candidate) {
			return candidate
		}
	}
	return ""
}

func (b *projector) safeDirectHash(value string, group int) string {
	if value == "" {
		return ""
	}
	if !safeRevisionHash(value) {
		b.markMalformed(reportv1alpha1.RolloutIssueRevisionNameInvalid, ptrInt(group), "")
		return ""
	}
	return value
}

func (b *projector) serviceRevisionHash(
	name string,
	component omev1beta1.ComponentType,
	group *int,
) string {
	if name == "" {
		return ""
	}
	hash := extractRevisionHash(b.isvc.Name, component, name)
	if hash == "" {
		b.markMalformed(reportv1alpha1.RolloutIssueRevisionNameInvalid, group, projectComponent(component))
	}
	return hash
}

func (b *projector) groupContains(group *reportv1alpha1.RolloutGroupStatus, component omev1beta1.ComponentType) bool {
	projected := projectComponent(component)
	for _, member := range group.Components {
		if member == projected {
			return projected != ""
		}
	}
	return false
}

func (b *projector) markMalformed(code reportv1alpha1.RolloutIssueCode, group *int, component reportv1alpha1.RuntimeComponentType) {
	b.malformed = true
	b.addIssue(code, group, component)
	b.addWarning(reportv1alpha1.WarningPartialData)
}

func (b *projector) addIssue(code reportv1alpha1.RolloutIssueCode, group *int, component reportv1alpha1.RuntimeComponentType) {
	key := string(code) + "/" + string(component) + "/"
	if group != nil {
		key += strconv.Itoa(*group)
	}
	if _, found := b.issueKeys[key]; found {
		return
	}
	b.issueKeys[key] = struct{}{}
	var groupCopy *int
	if group != nil {
		groupCopy = ptrInt(*group)
	}
	b.content.Issues = append(b.content.Issues, reportv1alpha1.RolloutIssue{
		Code: code, Group: groupCopy, Component: component,
	})
	b.addWarning(reportv1alpha1.WarningPartialData)
}

func (b *projector) addWarning(code reportv1alpha1.WarningCode) {
	b.warningCodes[code] = struct{}{}
}

func projectComponent(component omev1beta1.ComponentType) reportv1alpha1.RuntimeComponentType {
	switch component {
	case omev1beta1.EngineComponent:
		return reportv1alpha1.RuntimeComponentEngine
	case omev1beta1.DecoderComponent:
		return reportv1alpha1.RuntimeComponentDecoder
	case omev1beta1.RouterComponent:
		return reportv1alpha1.RuntimeComponentRouter
	default:
		return ""
	}
}

func apiComponent(component reportv1alpha1.RuntimeComponentType) omev1beta1.ComponentType {
	switch component {
	case reportv1alpha1.RuntimeComponentEngine:
		return omev1beta1.EngineComponent
	case reportv1alpha1.RuntimeComponentDecoder:
		return omev1beta1.DecoderComponent
	case reportv1alpha1.RuntimeComponentRouter:
		return omev1beta1.RouterComponent
	default:
		return ""
	}
}

func validSequentialCursor(
	order []omev1beta1.ComponentType,
	phase omev1beta1.CoordinationPhase,
	current omev1beta1.ComponentType,
	previous omev1beta1.ComponentType,
) bool {
	if phase == omev1beta1.CoordinationPhasePaused {
		return current == "" && previous == ""
	}
	if current == "" {
		return phase == omev1beta1.CoordinationPhaseIdle && previous == ""
	}
	index := slices.Index(order, current)
	if index < 0 {
		return false
	}
	if index == 0 {
		return phase != omev1beta1.CoordinationPhaseIdle && previous == ""
	}
	return previous == order[index-1]
}

func validSequentialComposite(
	phase omev1beta1.CoordinationPhase,
	composite string,
	current omev1beta1.ComponentType,
	previous omev1beta1.ComponentType,
) bool {
	if phase == omev1beta1.CoordinationPhaseIdle {
		if current == "" && previous == "" {
			return composite == "" || composite == string(omev1beta1.CoordinationPhaseIdle)
		}
		return composite == omev1beta1.CompositePhaseSequentialAwaiting
	}
	if composite == "" {
		return true
	}
	if phase == omev1beta1.CoordinationPhaseFailed {
		return composite == omev1beta1.CompositePhaseSequentialFailed
	}
	if current == "" {
		return composite == string(phase)
	}
	return composite == string(current)+"."+string(phase)
}

func supportedComponent(component omev1beta1.ComponentType) bool {
	return projectComponent(component) != ""
}

func componentOrder(component omev1beta1.ComponentType) int {
	switch component {
	case omev1beta1.EngineComponent:
		return 0
	case omev1beta1.DecoderComponent:
		return 1
	case omev1beta1.RouterComponent:
		return 2
	default:
		return 3
	}
}

func projectComponentPhase(phase omev1beta1.RolloutPhase) reportv1alpha1.RolloutPhase {
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

func projectCoordinationPhase(phase omev1beta1.CoordinationPhase) reportv1alpha1.RolloutPhase {
	switch phase {
	case omev1beta1.CoordinationPhaseIdle:
		return reportv1alpha1.RolloutPhaseIdle
	case omev1beta1.CoordinationPhaseSurging:
		return reportv1alpha1.RolloutPhaseSurging
	case omev1beta1.CoordinationPhaseWaiting:
		return reportv1alpha1.RolloutPhaseWaiting
	case omev1beta1.CoordinationPhaseShifting:
		return reportv1alpha1.RolloutPhaseShifting
	case omev1beta1.CoordinationPhaseDraining:
		return reportv1alpha1.RolloutPhaseDraining
	case omev1beta1.CoordinationPhaseScalingDown:
		return reportv1alpha1.RolloutPhaseScalingDown
	case omev1beta1.CoordinationPhaseStaged:
		return reportv1alpha1.RolloutPhaseStaged
	case omev1beta1.CoordinationPhaseFailed:
		return reportv1alpha1.RolloutPhaseFailed
	case omev1beta1.CoordinationPhaseRollingBack:
		return reportv1alpha1.RolloutPhaseRollingBack
	case omev1beta1.CoordinationPhasePaused:
		return reportv1alpha1.RolloutPhasePaused
	default:
		return reportv1alpha1.RolloutPhaseUnknown
	}
}

func gateFor(step *omev1beta1.RolloutGroupStep) reportv1alpha1.RolloutGate {
	switch {
	case step.Analysis != nil:
		return reportv1alpha1.RolloutGateAnalysis
	case step.Pause != nil && step.Pause.Duration != nil:
		return reportv1alpha1.RolloutGateTimed
	case step.Pause != nil:
		return reportv1alpha1.RolloutGateManual
	default:
		return reportv1alpha1.RolloutGateImmediate
	}
}

func analysisState(
	analysis *omev1beta1.RolloutAnalysis,
	status *omev1beta1.CanaryStatus,
) (reportv1alpha1.RolloutAnalysisState, bool) {
	if analysis == nil {
		return reportv1alpha1.RolloutAnalysisUnobserved, false
	}
	if len(status.MetricResults) == 0 &&
		status.LastEvaluationTime == nil &&
		status.LastConclusiveEvaluationTime == nil &&
		status.AnalysisFailedChecks == 0 {
		return reportv1alpha1.RolloutAnalysisUnobserved, true
	}
	if status.AnalysisFailedChecks < 0 ||
		status.LastEvaluationTime == nil ||
		status.LastEvaluationTime.IsZero() ||
		len(status.MetricResults) == 0 {
		return reportv1alpha1.RolloutAnalysisInconclusive, false
	}
	if status.LastConclusiveEvaluationTime != nil {
		if status.LastConclusiveEvaluationTime.IsZero() ||
			status.LastConclusiveEvaluationTime.Time.After(status.LastEvaluationTime.Time) {
			return reportv1alpha1.RolloutAnalysisInconclusive, false
		}
	}
	if status.AnalysisFailedChecks > 0 && status.LastConclusiveEvaluationTime == nil {
		return reportv1alpha1.RolloutAnalysisInconclusive, false
	}
	expected := make(map[string]omev1beta1.AnalysisMetric, len(analysis.Metrics))
	for _, metric := range analysis.Metrics {
		expected[metric.Name] = metric
	}
	seen := make(map[string]struct{}, len(status.MetricResults))
	anyFail := false
	anyInconclusive := false
	for _, result := range status.MetricResults {
		if result.Time == nil || result.Time.IsZero() ||
			!result.Time.Time.Equal(status.LastEvaluationTime.Time) ||
			(result.Value == "" && result.Passed) {
			return reportv1alpha1.RolloutAnalysisInconclusive, false
		}
		metric, found := expected[result.Name]
		if !found {
			anyInconclusive = true
			continue
		}
		if _, duplicate := seen[result.Name]; duplicate {
			anyInconclusive = true
			continue
		}
		seen[result.Name] = struct{}{}
		if result.Threshold != metric.Threshold || result.Operator != metric.Operator {
			anyInconclusive = true
			continue
		}
		if result.Value == "" {
			anyInconclusive = true
			continue
		}
		value, valueErr := strconv.ParseFloat(result.Value, 64)
		threshold, thresholdErr := strconv.ParseFloat(metric.Threshold, 64)
		if valueErr != nil || thresholdErr != nil ||
			math.IsNaN(value) || math.IsInf(value, 0) ||
			math.IsNaN(threshold) || math.IsInf(threshold, 0) {
			return reportv1alpha1.RolloutAnalysisInconclusive, false
		}
		passed, comparable := analysisComparison(value, metric.Operator, threshold)
		if !comparable || passed != result.Passed {
			return reportv1alpha1.RolloutAnalysisInconclusive, false
		}
		if !passed {
			anyFail = true
		}
	}
	if len(seen) != len(expected) {
		anyInconclusive = true
	}
	state := reportv1alpha1.RolloutAnalysisPassing
	switch {
	case anyFail:
		state = reportv1alpha1.RolloutAnalysisFailing
	case anyInconclusive:
		state = reportv1alpha1.RolloutAnalysisInconclusive
	}
	if state == reportv1alpha1.RolloutAnalysisFailing && status.AnalysisFailedChecks == 0 {
		return reportv1alpha1.RolloutAnalysisInconclusive, false
	}
	if (state == reportv1alpha1.RolloutAnalysisPassing ||
		state == reportv1alpha1.RolloutAnalysisFailing) &&
		(status.LastConclusiveEvaluationTime == nil ||
			!status.LastConclusiveEvaluationTime.Time.Equal(status.LastEvaluationTime.Time)) {
		return reportv1alpha1.RolloutAnalysisInconclusive, false
	}
	return state, true
}

func analysisComparison(
	value float64,
	operator omev1beta1.ComparisonOperator,
	threshold float64,
) (bool, bool) {
	switch operator {
	case omev1beta1.ComparisonLT:
		return value < threshold, true
	case omev1beta1.ComparisonLTE:
		return value <= threshold, true
	case omev1beta1.ComparisonGT:
		return value > threshold, true
	case omev1beta1.ComparisonGTE:
		return value >= threshold, true
	default:
		return false, false
	}
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
		raw = strings.TrimSuffix(raw, "%")
		parsed, err := strconv.Atoi(raw)
		return err == nil && parsed >= 0 && parsed <= 100
	default:
		return false
	}
}

func validCompletedCanaryCapacity(value intstr.IntOrString) bool {
	if !safeCapacity(value) {
		return false
	}
	if value.Type == intstr.Int {
		return value.IntVal > 0
	}
	percentage, err := strconv.Atoi(strings.TrimSuffix(value.StrVal, "%"))
	return err == nil && percentage == 100
}

func extractRevisionHash(isvcName string, component omev1beta1.ComponentType, serviceName string) string {
	if len(serviceName) < 8 {
		return ""
	}
	hash := serviceName[len(serviceName)-8:]
	if !safeRevisionHash(hash) {
		return ""
	}
	rawName := fmt.Sprintf("%s-%s-rev-%s", isvcName, component, hash)
	expected := constants.TruncateNameWithMaxLength(rawName, validation.DNS1035LabelMaxLength)
	if serviceName != expected {
		return ""
	}
	return hash
}

func extractControllerRevisionHash(
	isvcName string,
	component omev1beta1.ComponentType,
	revisionName string,
) string {
	if len(revisionName) < 8 {
		return ""
	}
	hash := revisionName[len(revisionName)-8:]
	if !safeRevisionHash(hash) {
		return ""
	}
	if revisionName != fmt.Sprintf("%s-%s-%s", isvcName, component, hash) {
		return ""
	}
	return hash
}

func safeRevisionHash(hash string) bool {
	return revisionHashPattern.MatchString(hash)
}

func revisionRole(hash string, component *reportv1alpha1.RolloutComponentStatus) reportv1alpha1.RolloutRevisionRole {
	switch hash {
	case component.RolledOutRevisionHash:
		return reportv1alpha1.RolloutRevisionCurrent
	case component.ReadyRevisionHash:
		return reportv1alpha1.RolloutRevisionTarget
	case component.PreviousRevisionHash:
		return reportv1alpha1.RolloutRevisionPrevious
	default:
		return reportv1alpha1.RolloutRevisionOther
	}
}

func canaryPhaseNeedsStatus(phase reportv1alpha1.RolloutPhase) bool {
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

func canaryPhaseBindsTraffic(phase reportv1alpha1.RolloutPhase) bool {
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

func canaryPhaseBindsStepTraffic(phase reportv1alpha1.RolloutPhase) bool {
	switch phase {
	case reportv1alpha1.RolloutPhaseCanarying,
		reportv1alpha1.RolloutPhasePaused,
		reportv1alpha1.RolloutPhasePromoting:
		return true
	default:
		return false
	}
}

func canaryObservedTrafficMatchesStep(
	phase reportv1alpha1.RolloutPhase,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
) bool {
	current := int(status.CurrentStep)
	if current < 0 || current >= len(steps) {
		return false
	}
	if status.ObservedTrafficWeight == steps[current].Traffic {
		return true
	}
	return phase == reportv1alpha1.RolloutPhaseCanarying &&
		current > 0 &&
		status.ObservedTrafficWeight == steps[current-1].Traffic
}

func validCanaryPhaseStepResidue(
	phase reportv1alpha1.RolloutPhase,
	steps []omev1beta1.RolloutGroupStep,
	status *omev1beta1.CanaryStatus,
) bool {
	current := int(status.CurrentStep)
	if current < 0 || current >= len(steps) {
		return false
	}
	last := len(steps) - 1
	switch phase {
	case reportv1alpha1.RolloutPhasePromoting:
		if current != last {
			return false
		}
	case reportv1alpha1.RolloutPhasePaused:
		if current >= last {
			return false
		}
	case reportv1alpha1.RolloutPhaseCanarying:
		if current == last &&
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
		return false
	}
	previousStep := &steps[current-1]
	if gateFor(previousStep) == reportv1alpha1.RolloutGateManual {
		return status.PromotedThrough == status.CanaryRevisionHash
	}
	return true
}

func hasPhase(phases []reportv1alpha1.RolloutPhase, want reportv1alpha1.RolloutPhase) bool {
	for _, phase := range phases {
		if phase == want {
			return true
		}
	}
	return false
}

func anyActivePhase(phases []reportv1alpha1.RolloutPhase) bool {
	for _, phase := range phases {
		switch phase {
		case reportv1alpha1.RolloutPhaseCanarying,
			reportv1alpha1.RolloutPhaseBlueGreenStandby,
			reportv1alpha1.RolloutPhasePending,
			reportv1alpha1.RolloutPhasePromoting,
			reportv1alpha1.RolloutPhaseSurging,
			reportv1alpha1.RolloutPhaseWaiting,
			reportv1alpha1.RolloutPhaseShifting,
			reportv1alpha1.RolloutPhaseUpdating,
			reportv1alpha1.RolloutPhaseDraining,
			reportv1alpha1.RolloutPhaseScalingDown,
			reportv1alpha1.RolloutPhaseAwaitingNextComponent:
			return true
		}
	}
	return false
}

func ptrInt(value int) *int { return &value }
