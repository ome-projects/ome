package rolloutprojection

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/apis"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/validation"
)

const (
	maxExplainGroups = 3
	maxExplainSteps  = 20
	maxExplainIssues = 32
)

var (
	portableDigestPattern  = regexp.MustCompile(`^rp1:[0-9a-f]{12}$`)
	validationGroupPattern = regexp.MustCompile(`groups\[(\d+)\]`)
)

// ProjectExplain builds a bounded, redacted explanation from one already-read
// InferenceService. It deliberately performs no policy or child-resource reads.
func ProjectExplain(
	isvc *omev1beta1.InferenceService,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.RolloutExplainReport, error) {
	observed, err := Project(isvc, clock)
	if err != nil {
		return reportv1alpha1.RolloutExplainReport{}, err
	}

	b := explainProjector{isvc: isvc}
	b.content.Observed = observed.Content
	b.capObserved()
	b.projectSummary()
	b.content.DeclaredGroups = b.projectSpecGroups(reportv1alpha1.RolloutPlanViewDeclared, false)
	b.content.LiveGroups = []reportv1alpha1.RolloutPlanGroup{}
	if omev1beta1.RolloutRunActive(b.isvc) {
		b.content.LiveGroups = b.projectSpecGroups(reportv1alpha1.RolloutPlanViewLive, true)
	}
	b.projectEffectiveGroups()
	b.projectHolds()

	result := reportv1alpha1.NewRolloutExplainReport(observed.Metadata, b.content, clock)
	result.Sources = append([]reportv1alpha1.RolloutSourceReference{}, observed.Sources...)
	result.Warnings = append([]reportv1alpha1.RolloutWarning{}, observed.Warnings...)
	if len(b.content.Issues) > 0 {
		result.Warnings = append(result.Warnings, reportv1alpha1.RolloutWarning{
			Code: reportv1alpha1.WarningPartialData,
		})
	}
	if b.truncated {
		result.Warnings = append(result.Warnings, reportv1alpha1.RolloutWarning{
			Code: reportv1alpha1.WarningTruncated,
		})
	}
	return result.Canonical(), nil
}

type explainProjector struct {
	isvc                 *omev1beta1.InferenceService
	content              reportv1alpha1.RolloutExplainContent
	resolutions          map[int]*omev1beta1.RolloutGroupResolution
	malformedResolutions map[int]bool
	issueKeys            map[string]struct{}
	resolutionsPrepared  bool
	truncated            bool
}

func (b *explainProjector) capObserved() {
	if len(b.content.Observed.Groups) > maxExplainGroups {
		b.content.Observed.Groups = b.content.Observed.Groups[:maxExplainGroups]
		b.truncated = true
	}
	if len(b.content.Observed.Components) > 3 {
		b.content.Observed.Components = b.content.Observed.Components[:3]
		b.truncated = true
	}
	if len(b.content.Observed.Issues) > maxExplainIssues {
		b.content.Observed.Issues = b.content.Observed.Issues[:maxExplainIssues]
		b.truncated = true
	}
}

func (b *explainProjector) projectSummary() {
	b.content.Summary.EffectivePlan = reportv1alpha1.RolloutPlanSelection{
		Mode: reportv1alpha1.RolloutPlanModeLive, Evidence: reportv1alpha1.EvidenceDeclared,
	}
	if omev1beta1.RolloutRunActive(b.isvc) {
		b.content.Summary.EffectivePlan = reportv1alpha1.RolloutPlanSelection{
			Mode: reportv1alpha1.RolloutPlanModePinned, Evidence: reportv1alpha1.EvidenceReported,
		}
	}
	b.content.Summary.PlanReady = b.projectPlanCondition(
		omev1beta1.RolloutPlanReadyCondition,
		map[string]reportv1alpha1.RolloutPlanReason{
			omev1beta1.RolloutPlanReasonPinned:              reportv1alpha1.RolloutPlanReasonPinned,
			omev1beta1.RolloutPlanReasonNoRun:               reportv1alpha1.RolloutPlanReasonNoActiveRun,
			omev1beta1.RolloutPlanReasonPolicyNotFound:      reportv1alpha1.RolloutPlanReasonPolicyNotFound,
			omev1beta1.RolloutPlanReasonPolicyNotReady:      reportv1alpha1.RolloutPlanReasonPolicyNotReady,
			omev1beta1.RolloutPlanReasonProgressionMismatch: reportv1alpha1.RolloutPlanReasonProgressionMismatch,
			omev1beta1.RolloutPlanReasonPlanInvalid:         reportv1alpha1.RolloutPlanReasonPlanInvalid,
			omev1beta1.RolloutPlanReasonProviderUnbound:     reportv1alpha1.RolloutPlanReasonProviderUnbound,
		},
	)
	b.content.Summary.PlanDrift = b.projectPlanCondition(
		omev1beta1.RolloutPlanDriftCondition,
		map[string]reportv1alpha1.RolloutPlanReason{
			omev1beta1.RolloutPlanDriftReasonInSync:             reportv1alpha1.RolloutPlanReasonInSync,
			omev1beta1.RolloutPlanDriftReasonPolicyNewerThanRun: reportv1alpha1.RolloutPlanReasonPolicyNewer,
			omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun:   reportv1alpha1.RolloutPlanReasonSpecNewer,
		},
	)
	b.validatePlanConditionMode()
}

func (b *explainProjector) validatePlanConditionMode() {
	active := omev1beta1.RolloutRunActive(b.isvc)
	ready := &b.content.Summary.PlanReady
	readyMismatch := ready.State != reportv1alpha1.RolloutConditionInvalid &&
		ready.State != reportv1alpha1.RolloutConditionUnobserved &&
		((active && (ready.State != reportv1alpha1.RolloutConditionTrue || ready.Reason != reportv1alpha1.RolloutPlanReasonPinned)) ||
			(!active && ready.Reason == reportv1alpha1.RolloutPlanReasonPinned))
	if readyMismatch {
		b.invalidatePlanCondition(ready)
	}
	drift := &b.content.Summary.PlanDrift
	driftMismatch := !active && drift.State != reportv1alpha1.RolloutConditionInvalid &&
		drift.State != reportv1alpha1.RolloutConditionUnobserved &&
		(drift.State != reportv1alpha1.RolloutConditionFalse || drift.Reason != reportv1alpha1.RolloutPlanReasonInSync)
	if driftMismatch {
		b.invalidatePlanCondition(drift)
	}
}

func (b *explainProjector) invalidatePlanCondition(condition *reportv1alpha1.RolloutPlanCondition) {
	condition.State = reportv1alpha1.RolloutConditionInvalid
	condition.Reason = reportv1alpha1.RolloutPlanReasonUnknown
	b.addIssue(reportv1alpha1.RolloutExplainIssuePlanConditionMalformed, "", nil)
}

func (b *explainProjector) projectPlanCondition(
	conditionType string,
	reasons map[string]reportv1alpha1.RolloutPlanReason,
) reportv1alpha1.RolloutPlanCondition {
	var condition *apis.Condition
	for index := range b.isvc.Status.Conditions {
		candidate := &b.isvc.Status.Conditions[index]
		if candidate.Type != apis.ConditionType(conditionType) {
			continue
		}
		if condition != nil {
			b.addIssue(reportv1alpha1.RolloutExplainIssuePlanConditionMalformed, "", nil)
			return reportv1alpha1.RolloutPlanCondition{
				State: reportv1alpha1.RolloutConditionInvalid, Reason: reportv1alpha1.RolloutPlanReasonUnknown,
				Evidence: reportv1alpha1.EvidenceReported,
			}
		}
		condition = candidate
	}
	if condition == nil {
		return reportv1alpha1.RolloutPlanCondition{
			State: reportv1alpha1.RolloutConditionUnobserved, Evidence: reportv1alpha1.EvidenceUnavailable,
		}
	}
	result := reportv1alpha1.RolloutPlanCondition{
		State: projectConditionStatus(condition.Status), Evidence: reportv1alpha1.EvidenceReported,
	}
	if result.State == reportv1alpha1.RolloutConditionInvalid {
		b.addIssue(reportv1alpha1.RolloutExplainIssuePlanConditionMalformed, "", nil)
	}
	reason, valid := reasons[condition.Reason]
	if !valid || !conditionReasonMatchesStatus(conditionType, condition.Reason, condition.Status) {
		result.State = reportv1alpha1.RolloutConditionInvalid
		result.Reason = reportv1alpha1.RolloutPlanReasonUnknown
		b.addIssue(reportv1alpha1.RolloutExplainIssuePlanConditionMalformed, "", nil)
		return result
	}
	result.Reason = reason
	return result
}

func conditionReasonMatchesStatus(conditionType, reason string, status corev1.ConditionStatus) bool {
	if conditionType == omev1beta1.RolloutPlanReadyCondition {
		switch reason {
		case omev1beta1.RolloutPlanReasonPinned, omev1beta1.RolloutPlanReasonNoRun:
			return status == corev1.ConditionTrue
		case omev1beta1.RolloutPlanReasonPolicyNotFound,
			omev1beta1.RolloutPlanReasonPolicyNotReady,
			omev1beta1.RolloutPlanReasonProgressionMismatch,
			omev1beta1.RolloutPlanReasonPlanInvalid,
			omev1beta1.RolloutPlanReasonProviderUnbound:
			return status == corev1.ConditionFalse
		}
	}
	if conditionType == omev1beta1.RolloutPlanDriftCondition {
		switch reason {
		case omev1beta1.RolloutPlanDriftReasonInSync:
			return status == corev1.ConditionFalse
		case omev1beta1.RolloutPlanDriftReasonPolicyNewerThanRun,
			omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun:
			return status == corev1.ConditionTrue
		}
	}
	return false
}

func projectConditionStatus(status corev1.ConditionStatus) reportv1alpha1.RolloutConditionState {
	switch status {
	case corev1.ConditionTrue:
		return reportv1alpha1.RolloutConditionTrue
	case corev1.ConditionFalse:
		return reportv1alpha1.RolloutConditionFalse
	case corev1.ConditionUnknown:
		return reportv1alpha1.RolloutConditionUnknown
	default:
		return reportv1alpha1.RolloutConditionInvalid
	}
}

func (b *explainProjector) projectSpecGroups(
	view reportv1alpha1.RolloutPlanView,
	includeResolution bool,
) []reportv1alpha1.RolloutPlanGroup {
	if b.isvc.Spec.Rollout == nil {
		return []reportv1alpha1.RolloutPlanGroup{}
	}
	allGroups := b.isvc.Spec.Rollout.Groups
	groups := allGroups
	if includeResolution {
		b.prepareResolutions(len(groups), view)
	}
	b.validateCurrentPlan(view)
	if len(groups) > maxExplainGroups {
		b.truncated = true
		b.addIssue(reportv1alpha1.RolloutExplainIssuePlanTruncated, view, nil)
		groups = groups[:maxExplainGroups]
	}
	result := make([]reportv1alpha1.RolloutPlanGroup, 0, len(groups))
	bodyKnown := make(map[int]bool, len(groups))
	usable := make(map[int]bool, len(groups))
	for index := range groups {
		evidence := reportv1alpha1.EvidenceDeclared
		malformedCode := planMalformedCode(view)
		projected := b.projectGroup(index, &groups[index], evidence, view, malformedCode)
		if includeResolution {
			resolved := b.applyResolution(&projected, &groups[index], index, view)
			if groups[index].PolicyRef != nil && !resolved {
				b.addIssue(reportv1alpha1.RolloutExplainIssueResolutionMissing, view, ptrInt(index))
			}
		}
		bodyKnown[index] = hasInlineProgression(&groups[index]) || groups[index].PolicyRef == nil
		usable[index] = !b.hasIssue(malformedCode, view, ptrInt(index)) &&
			!b.hasIssue(malformedCode, view, nil)
		result = append(result, projected)
	}
	if b.hasAnyIssue(planMalformedCode(view), view) {
		for index := range usable {
			usable[index] = false
		}
	}
	annotateSettingEffects(result, len(allGroups), bodyKnown, usable)
	return result
}

func (b *explainProjector) projectEffectiveGroups() {
	if !omev1beta1.RolloutRunActive(b.isvc) {
		b.content.EffectiveGroups = b.projectSpecGroups(reportv1alpha1.RolloutPlanViewEffective, true)
		return
	}
	effectiveSpec := b.isvc.Spec
	effectiveSpec.Rollout = omev1beta1.EffectiveRollout(b.isvc)
	if !validStoredRolloutSpec(&effectiveSpec) {
		b.addIssue(
			reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
			reportv1alpha1.RolloutPlanViewEffective, nil,
		)
	}
	allGroups := b.isvc.Status.Rollout.ActiveRun.Plan.Groups
	invalidPlanGroups := invalidPinnedPlanGroups(allGroups)
	for index := range allGroups {
		if !invalidPlanGroups[index] {
			continue
		}
		if index < maxExplainGroups {
			b.addIssue(
				reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
				reportv1alpha1.RolloutPlanViewEffective, ptrInt(index),
			)
		} else {
			b.addIssue(
				reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
				reportv1alpha1.RolloutPlanViewEffective, nil,
			)
		}
	}
	groups := allGroups
	if len(groups) > maxExplainGroups {
		b.truncated = true
		b.addIssue(reportv1alpha1.RolloutExplainIssuePlanTruncated, reportv1alpha1.RolloutPlanViewEffective, nil)
		groups = groups[:maxExplainGroups]
	}
	b.content.EffectiveGroups = make([]reportv1alpha1.RolloutPlanGroup, 0, len(groups))
	bodyKnown := make(map[int]bool, len(groups))
	usable := make(map[int]bool, len(groups))
	for index := range groups {
		pinned := &groups[index]
		groupForProjection := pinned.Group.DeepCopy()
		groupForProjection.PolicyRef = nil
		projected := b.projectGroup(
			index, groupForProjection, reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutPlanViewEffective,
			reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		)
		projected.ShadowedPolicy = nil
		activeMalformed := b.hasIssue(
			reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
			reportv1alpha1.RolloutPlanViewEffective, ptrInt(index),
		)
		if strategy, valid := pinnedGroupStrategy(&pinned.Group); valid {
			projected.Strategy = strategy
		} else {
			projected.Strategy = reportv1alpha1.RolloutStrategyUnknown
			activeMalformed = true
		}
		if pinned.Group.PolicyRef != nil {
			activeMalformed = true
		}
		if portableDigestPattern.MatchString(pinned.PortableDigest) {
			projected.PortableDigest = pinned.PortableDigest
		} else {
			activeMalformed = true
		}
		switch pinned.Source {
		case omev1beta1.RolloutPlanSourceInline:
			projected.Source = reportv1alpha1.RolloutPlanSourceInline
			projected.Policy = nil
			projected.ProgressionOrigin = reportv1alpha1.RolloutProgressionOriginConfigured
			if projected.Strategy == reportv1alpha1.RolloutStrategyBlueGreen {
				projected.ProgressionOrigin = reportv1alpha1.RolloutProgressionOriginUnknown
			}
			if pinned.PolicyRef != nil || pinned.PolicyGeneration != 0 {
				activeMalformed = true
			}
		case omev1beta1.RolloutPlanSourcePolicy:
			projected.Source = reportv1alpha1.RolloutPlanSourcePolicy
			projected.ProgressionOrigin = reportv1alpha1.RolloutProgressionOriginPolicy
			if !validPinnedPolicyBody(&pinned.Group) {
				activeMalformed = true
			}
			projected.Policy = projectPinnedPolicyRef(pinned.PolicyRef, projected.Strategy)
			if projected.Policy != nil {
				projected.Policy.Generation = pinned.PolicyGeneration
				projected.Policy.Digest = pinned.PortableDigest
				projected.Policy.Evidence = reportv1alpha1.EvidenceReported
			} else {
				activeMalformed = true
			}
			if pinned.PolicyGeneration < 0 {
				activeMalformed = true
			}
		default:
			projected.Source = reportv1alpha1.RolloutPlanSourceUnknown
			projected.Policy = nil
			activeMalformed = true
		}
		if activeMalformed {
			b.addIssue(reportv1alpha1.RolloutExplainIssueActiveRunMalformed, reportv1alpha1.RolloutPlanViewEffective, ptrInt(index))
		}
		bodyKnown[index] = true
		usable[index] = !activeMalformed
		b.content.EffectiveGroups = append(b.content.EffectiveGroups, projected)
	}
	if b.hasAnyIssue(
		reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		reportv1alpha1.RolloutPlanViewEffective,
	) {
		for index := range usable {
			usable[index] = false
		}
	}
	annotateSettingEffects(b.content.EffectiveGroups, len(allGroups), bodyKnown, usable)
}

func pinnedGroupStrategy(group *omev1beta1.RolloutGroup) (reportv1alpha1.RolloutStrategy, bool) {
	strategy := reportv1alpha1.RolloutStrategyUnknown
	progressions := 0
	if group.Canary != nil {
		strategy = reportv1alpha1.RolloutStrategyCanary
		progressions++
	}
	if group.BlueGreen != nil {
		strategy = reportv1alpha1.RolloutStrategyBlueGreen
		progressions++
	}
	if group.RollingUpdate != nil {
		strategy = reportv1alpha1.RolloutStrategyRollingUpdate
		progressions++
	}
	return strategy, progressions == 1
}

func hasInlineProgression(group *omev1beta1.RolloutGroup) bool {
	return group != nil &&
		(group.Canary != nil || group.BlueGreen != nil || group.RollingUpdate != nil)
}

// validateCurrentPlan mirrors the controller/admission validators over the
// live spec without exposing their potentially sensitive error messages. A
// plan-level error is attributed to the indexed group when the validator
// supplies one; otherwise it is retained as a bounded view-level issue.
func (b *explainProjector) validateCurrentPlan(view reportv1alpha1.RolloutPlanView) {
	code := planMalformedCode(view)
	validators := []func(*omev1beta1.InferenceServiceSpec) error{
		validation.ValidateCanary,
		validation.ValidateCoordination,
		validation.ValidateRolloutOrderingEnforced,
	}
	for _, validate := range validators {
		err := validate(&b.isvc.Spec)
		if err == nil {
			continue
		}
		matches := validationGroupPattern.FindAllStringSubmatch(err.Error(), -1)
		if len(matches) == 0 {
			b.addIssue(code, view, nil)
			continue
		}
		for _, match := range matches {
			index, parseErr := strconv.Atoi(match[1])
			if parseErr == nil {
				b.addIssue(code, view, ptrInt(index))
			}
		}
	}
}

func validGroupBody(group *omev1beta1.RolloutGroup) bool {
	if group == nil || len(group.Components) < 1 || len(group.Components) > 3 || len(group.Order) > 0 {
		return false
	}
	seenComponents := map[omev1beta1.ComponentType]struct{}{}
	for _, component := range group.Components {
		if !supportedComponent(component) {
			return false
		}
		if _, exists := seenComponents[component]; exists {
			return false
		}
		seenComponents[component] = struct{}{}
	}
	progressions := 0
	if group.Canary != nil {
		progressions++
		if len(group.Canary.Steps) > maxExplainSteps ||
			validation.ValidateCanaryPlan("group.canary", group.Canary) != nil ||
			group.MaintainRatio != nil {
			return false
		}
		for _, step := range group.Canary.Steps {
			if step.Analysis != nil && len(step.Analysis.Metrics) > 10 {
				return false
			}
		}
	}
	if group.BlueGreen != nil {
		progressions++
	}
	if group.RollingUpdate != nil {
		progressions++
		if !validBudget(group.RollingUpdate.MaxSurge) ||
			!validBudget(group.RollingUpdate.MaxUnavailable) ||
			budgetResolvesToZero(group.RollingUpdate.MaxSurge) &&
				budgetResolvesToZero(group.RollingUpdate.MaxUnavailable) {
			return false
		}
	}
	if progressions > 1 {
		return false
	}
	if group.PolicyRef != nil && projectPolicyRef(group.PolicyRef) == nil {
		return false
	}
	if group.Soak != nil && group.Soak.Duration < 0 {
		return false
	}
	if group.MaintainRatio != nil && group.MaintainRatio.Tolerance != nil &&
		(*group.MaintainRatio.Tolerance < 0 || *group.MaintainRatio.Tolerance > 100) {
		return false
	}
	return true
}

func validPinnedPolicyBody(group *omev1beta1.RolloutGroup) bool {
	if group == nil {
		return false
	}
	policy := &omev1beta1.RolloutPolicySpec{
		Canary: group.Canary, BlueGreen: group.BlueGreen, RollingUpdate: group.RollingUpdate,
	}
	return validation.ValidateRolloutPolicySpec(policy) == nil
}

func invalidPinnedPlanGroups(groups []omev1beta1.RolloutRunGroup) map[int]bool {
	invalid := map[int]bool{}
	componentOwner := map[omev1beta1.ComponentType]int{}
	canaryGroups := []int{}
	coordinationGroups := 0
	collapsesToSequential := true
	for index := range groups {
		group := &groups[index].Group
		strategy, strategyValid := pinnedGroupStrategy(group)
		if strategy == reportv1alpha1.RolloutStrategyCanary {
			canaryGroups = append(canaryGroups, index)
		} else {
			coordinationGroups++
			if !strategyValid || strategy != reportv1alpha1.RolloutStrategyBlueGreen ||
				len(group.Components) != 1 {
				collapsesToSequential = false
			}
		}
		for _, component := range group.Components {
			if previous, exists := componentOwner[component]; exists {
				invalid[previous] = true
				invalid[index] = true
			} else {
				componentOwner[component] = index
			}
		}
	}
	if len(canaryGroups) > 1 {
		for _, index := range canaryGroups {
			invalid[index] = true
		}
	}
	if coordinationGroups < 2 {
		collapsesToSequential = false
	}
	for index := range groups {
		group := &groups[index].Group
		strategy, _ := pinnedGroupStrategy(group)
		if group.Soak != nil &&
			(strategy == reportv1alpha1.RolloutStrategyCanary || !collapsesToSequential) {
			invalid[index] = true
		}
		if len(groups) > 1 &&
			(strategy != reportv1alpha1.RolloutStrategyBlueGreen || len(group.Components) != 1) {
			invalid[index] = true
		}
	}
	return invalid
}

func validBudget(value *intstr.IntOrString) bool {
	return value == nil || safeCapacity(*value)
}

func budgetResolvesToZero(value *intstr.IntOrString) bool {
	if value == nil {
		return false
	}
	if value.Type == intstr.Int {
		return value.IntVal <= 0
	}
	parsed, err := strconv.Atoi(strings.TrimSuffix(value.StrVal, "%"))
	return err != nil || !strings.HasSuffix(value.StrVal, "%") || parsed <= 0
}

func (b *explainProjector) projectGroup(
	index int,
	group *omev1beta1.RolloutGroup,
	evidence reportv1alpha1.EvidenceLevel,
	view reportv1alpha1.RolloutPlanView,
	malformedCode reportv1alpha1.RolloutExplainIssueCode,
) reportv1alpha1.RolloutPlanGroup {
	result := reportv1alpha1.RolloutPlanGroup{
		Index: index, Evidence: evidence, Source: reportv1alpha1.RolloutPlanSourceInline,
		ProgressionOrigin: reportv1alpha1.RolloutProgressionOriginConfigured,
		Components:        projectComponents(group.Components), Order: projectComponents(group.Order),
		Steps: []reportv1alpha1.RolloutPlanStep{},
	}
	progressions := 0
	if group.Canary != nil {
		progressions++
		result.Strategy = reportv1alpha1.RolloutStrategyCanary
		result.Steps = b.projectSteps(group.Canary.Steps, view, index, malformedCode)
	}
	if group.BlueGreen != nil {
		progressions++
		result.Strategy = reportv1alpha1.RolloutStrategyBlueGreen
	}
	if group.RollingUpdate != nil {
		progressions++
		result.Strategy = reportv1alpha1.RolloutStrategyRollingUpdate
		result.RollingUpdate = projectRollingUpdate(group.RollingUpdate)
	}
	if progressions == 0 {
		if group.PolicyRef != nil {
			result.Source = reportv1alpha1.RolloutPlanSourcePolicy
			result.ProgressionOrigin = reportv1alpha1.RolloutProgressionOriginPolicy
			result.Policy = projectPolicyRef(group.PolicyRef)
			result.Strategy = projectProgression(group.PolicyRef.Progression)
			if view != reportv1alpha1.RolloutPlanViewDeclared {
				b.addIssue(reportv1alpha1.RolloutExplainIssuePolicyBodyUnavailable, view, ptrInt(index))
			}
		} else {
			result.Source = reportv1alpha1.RolloutPlanSourceDefaulted
			result.Strategy = reportv1alpha1.RolloutStrategyBlueGreen
			result.ProgressionOrigin = reportv1alpha1.RolloutProgressionOriginDefaulted
			result.Defaulted = true
		}
	} else if group.PolicyRef != nil {
		result.ShadowedPolicy = projectPolicyRef(group.PolicyRef)
	}
	if progressions > 1 || len(result.Components) != len(group.Components) ||
		(group.PolicyRef != nil && projectPolicyRef(group.PolicyRef) == nil) {
		result.Strategy = reportv1alpha1.RolloutStrategyUnknown
		result.ProgressionOrigin = reportv1alpha1.RolloutProgressionOriginUnknown
		b.addIssue(malformedCode, view, ptrInt(index))
	}
	if !validGroupBody(group) {
		b.addIssue(malformedCode, view, ptrInt(index))
	}
	if group.Soak != nil && group.Soak.Duration >= 0 {
		result.Soak = &reportv1alpha1.RolloutSetting{
			Value: group.Soak.Duration.String(), Source: reportv1alpha1.RolloutSettingConfigured,
			Effect: reportv1alpha1.RolloutSettingEffectUnknown,
		}
	}
	if group.MaintainRatio != nil {
		result.MaintainRatio = &reportv1alpha1.RolloutMaintainRatioSettings{
			Tolerance: reportv1alpha1.RolloutSetting{
				Source: reportv1alpha1.RolloutSettingUnresolved,
				Effect: reportv1alpha1.RolloutSettingEffectUnknown,
			},
		}
		if group.MaintainRatio.Tolerance != nil && *group.MaintainRatio.Tolerance >= 0 && *group.MaintainRatio.Tolerance <= 100 {
			result.MaintainRatio.Tolerance = reportv1alpha1.RolloutSetting{
				Value:  fmt.Sprintf("%d%%", *group.MaintainRatio.Tolerance),
				Source: reportv1alpha1.RolloutSettingConfigured,
				Effect: reportv1alpha1.RolloutSettingEffectUnknown,
			}
		}
	}
	return result
}

func (b *explainProjector) projectSteps(
	steps []omev1beta1.RolloutGroupStep,
	view reportv1alpha1.RolloutPlanView,
	group int,
	malformedCode reportv1alpha1.RolloutExplainIssueCode,
) []reportv1alpha1.RolloutPlanStep {
	if len(steps) > maxExplainSteps {
		b.truncated = true
		b.addIssue(reportv1alpha1.RolloutExplainIssuePlanTruncated, view, ptrInt(group))
		steps = steps[:maxExplainSteps]
	}
	result := make([]reportv1alpha1.RolloutPlanStep, 0, len(steps))
	for index := range steps {
		step := &steps[index]
		validCapacity := safeCapacity(step.Capacity)
		validTraffic := step.Traffic >= 0 && step.Traffic <= 100
		if !validCapacity || !validTraffic {
			b.addIssue(malformedCode, view, ptrInt(group))
		}
		projected := reportv1alpha1.RolloutPlanStep{
			Index: int32(index), Gate: gateFor(step),
		}
		if validCapacity {
			projected.Capacity = capacityString(step.Capacity)
		}
		if validTraffic {
			projected.Traffic = step.Traffic
		}
		if step.Pause != nil && step.Pause.Duration != nil && step.Pause.Duration.Duration >= 0 {
			projected.Pause = step.Pause.Duration.Duration.String()
		}
		if step.Analysis != nil {
			onInconclusive := omev1beta1.OnInconclusiveHold
			if step.Analysis.OnInconclusive != nil {
				switch *step.Analysis.OnInconclusive {
				case omev1beta1.OnInconclusiveHold, omev1beta1.OnInconclusiveRollback:
					onInconclusive = *step.Analysis.OnInconclusive
				default:
					b.addIssue(malformedCode, view, ptrInt(group))
				}
			}
			projected.Analysis = &reportv1alpha1.RolloutPlanAnalysis{
				MetricCount:    int32(min(len(step.Analysis.Metrics), 10)),
				Interval:       step.Analysis.Interval.Duration.String(),
				FailureLimit:   step.Analysis.FailureLimit,
				OnInconclusive: string(onInconclusive),
			}
			if step.Analysis.InitialDelay != nil && step.Analysis.InitialDelay.Duration >= 0 {
				projected.Analysis.InitialDelay = step.Analysis.InitialDelay.Duration.String()
			}
		}
		result = append(result, projected)
	}
	return result
}

func projectRollingUpdate(value *omev1beta1.GroupRollingUpdate) *reportv1alpha1.RolloutRollingUpdateSettings {
	return &reportv1alpha1.RolloutRollingUpdateSettings{
		MaxSurge: projectBudget(value.MaxSurge), MaxUnavailable: projectBudget(value.MaxUnavailable),
	}
}

func projectBudget(value *intstr.IntOrString) reportv1alpha1.RolloutSetting {
	if value == nil {
		return reportv1alpha1.RolloutSetting{
			Value: "25%", Source: reportv1alpha1.RolloutSettingDefaulted,
			Effect: reportv1alpha1.RolloutSettingEffectApplied,
		}
	}
	if !safeCapacity(*value) {
		return reportv1alpha1.RolloutSetting{
			Source: reportv1alpha1.RolloutSettingUnresolved,
			Effect: reportv1alpha1.RolloutSettingEffectApplied,
		}
	}
	return reportv1alpha1.RolloutSetting{
		Value: capacityString(*value), Source: reportv1alpha1.RolloutSettingConfigured,
		Effect: reportv1alpha1.RolloutSettingEffectApplied,
	}
}

func annotateSettingEffects(
	groups []reportv1alpha1.RolloutPlanGroup,
	groupCount int,
	bodyKnown map[int]bool,
	usable map[int]bool,
) {
	sequential, resolved, known, finalCoordinationGroup := sequentialPlanState(
		groups, groupCount, bodyKnown, usable,
	)
	for index := range groups {
		group := &groups[index]
		if group.Soak != nil {
			switch {
			case !usable[group.Index]:
				group.Soak.Effect = reportv1alpha1.RolloutSettingEffectUnknown
			case group.Strategy == reportv1alpha1.RolloutStrategyCanary:
				group.Soak.Effect = reportv1alpha1.RolloutSettingEffectIgnoredPlanShape
			case !known:
				group.Soak.Effect = reportv1alpha1.RolloutSettingEffectUnknown
			case sequential && group.Index == finalCoordinationGroup:
				group.Soak.Effect = reportv1alpha1.RolloutSettingEffectIgnoredFinalGroup
			case sequential && !resolved:
				group.Soak.Effect = reportv1alpha1.RolloutSettingEffectUnknown
			case sequential:
				group.Soak.Effect = reportv1alpha1.RolloutSettingEffectApplied
			default:
				group.Soak.Effect = reportv1alpha1.RolloutSettingEffectIgnoredPlanShape
			}
		}
		if group.MaintainRatio != nil {
			switch {
			case !usable[group.Index]:
				group.MaintainRatio.Tolerance.Effect = reportv1alpha1.RolloutSettingEffectUnknown
			case !bodyKnown[group.Index]:
				group.MaintainRatio.Tolerance.Effect = reportv1alpha1.RolloutSettingEffectUnknown
			case group.Strategy == reportv1alpha1.RolloutStrategyCanary ||
				group.Strategy == reportv1alpha1.RolloutStrategyUnknown:
				group.MaintainRatio.Tolerance.Effect = reportv1alpha1.RolloutSettingEffectIgnoredPlanShape
			case len(group.Components) < 2:
				group.MaintainRatio.Tolerance.Effect = reportv1alpha1.RolloutSettingEffectIgnoredSingleComponent
			default:
				group.MaintainRatio.Tolerance.Effect = reportv1alpha1.RolloutSettingEffectApplied
			}
		}
		if group.RollingUpdate != nil && !usable[group.Index] {
			group.RollingUpdate.MaxSurge.Effect = reportv1alpha1.RolloutSettingEffectUnknown
			group.RollingUpdate.MaxUnavailable.Effect = reportv1alpha1.RolloutSettingEffectUnknown
		}
	}
}

func sequentialPlanState(
	groups []reportv1alpha1.RolloutPlanGroup,
	groupCount int,
	bodyKnown map[int]bool,
	usable map[int]bool,
) (sequential bool, resolved bool, known bool, finalCoordinationGroup int) {
	if len(groups) != groupCount {
		return false, false, false, -1
	}
	resolved = true
	coordinationGroups := 0
	finalCoordinationGroup = -1
	for index := range groups {
		group := &groups[index]
		if !usable[group.Index] {
			return false, false, false, -1
		}
		if group.Strategy == reportv1alpha1.RolloutStrategyCanary {
			continue
		}
		coordinationGroups++
		finalCoordinationGroup = group.Index
		if len(group.Components) != 1 || group.Strategy != reportv1alpha1.RolloutStrategyBlueGreen {
			return false, true, true, finalCoordinationGroup
		}
		if !bodyKnown[group.Index] {
			resolved = false
		}
	}
	return coordinationGroups >= 2, resolved, true, finalCoordinationGroup
}

func capacityString(value intstr.IntOrString) string {
	if value.Type == intstr.Int {
		return strconv.FormatInt(int64(value.IntVal), 10)
	}
	return value.StrVal
}

func projectComponents(values []omev1beta1.ComponentType) []reportv1alpha1.RuntimeComponentType {
	result := make([]reportv1alpha1.RuntimeComponentType, 0, min(len(values), 3))
	for _, value := range values {
		if !supportedComponent(value) || slices.Contains(result, projectComponent(value)) {
			continue
		}
		result = append(result, projectComponent(value))
		if len(result) == 3 {
			break
		}
	}
	return result
}

func projectPolicyRef(value *omev1beta1.RolloutPolicyRef) *reportv1alpha1.RolloutPolicyReference {
	if value == nil {
		return nil
	}
	kind := value.Kind
	if kind == "" {
		kind = "RolloutPolicy"
	}
	if kind != "RolloutPolicy" || len(utilvalidation.IsDNS1123Subdomain(value.Name)) != 0 ||
		projectProgression(value.Progression) == reportv1alpha1.RolloutStrategyUnknown {
		return nil
	}
	return &reportv1alpha1.RolloutPolicyReference{
		Kind: kind, Name: value.Name, Progression: string(value.Progression), Evidence: reportv1alpha1.EvidenceDeclared,
	}
}

func projectPinnedPolicyRef(
	value *omev1beta1.RolloutPolicyRef,
	strategy reportv1alpha1.RolloutStrategy,
) *reportv1alpha1.RolloutPolicyReference {
	if value == nil {
		return nil
	}
	kind := value.Kind
	if kind == "" {
		kind = "RolloutPolicy"
	}
	if kind != "RolloutPolicy" || len(utilvalidation.IsDNS1123Subdomain(value.Name)) != 0 {
		return nil
	}
	progression := projectProgression(value.Progression)
	if value.Progression == "" {
		progression = strategy
	}
	if progression == reportv1alpha1.RolloutStrategyUnknown || progression != strategy {
		return nil
	}
	return &reportv1alpha1.RolloutPolicyReference{
		Kind: kind, Name: value.Name, Progression: progressionString(progression),
		Evidence: reportv1alpha1.EvidenceReported,
	}
}

func progressionString(value reportv1alpha1.RolloutStrategy) string {
	switch value {
	case reportv1alpha1.RolloutStrategyCanary:
		return string(omev1beta1.RolloutProgressionCanary)
	case reportv1alpha1.RolloutStrategyBlueGreen:
		return string(omev1beta1.RolloutProgressionBlueGreen)
	case reportv1alpha1.RolloutStrategyRollingUpdate:
		return string(omev1beta1.RolloutProgressionRollingUpdate)
	default:
		return ""
	}
}

func projectProgression(value omev1beta1.RolloutProgressionKind) reportv1alpha1.RolloutStrategy {
	switch value {
	case omev1beta1.RolloutProgressionCanary:
		return reportv1alpha1.RolloutStrategyCanary
	case omev1beta1.RolloutProgressionBlueGreen:
		return reportv1alpha1.RolloutStrategyBlueGreen
	case omev1beta1.RolloutProgressionRollingUpdate:
		return reportv1alpha1.RolloutStrategyRollingUpdate
	default:
		return reportv1alpha1.RolloutStrategyUnknown
	}
}

func (b *explainProjector) applyResolution(
	group *reportv1alpha1.RolloutPlanGroup,
	declared *omev1beta1.RolloutGroup,
	index int,
	view reportv1alpha1.RolloutPlanView,
) bool {
	if b.malformedResolutions[index] {
		return true
	}
	resolution, found := b.resolutions[index]
	if !found {
		return false
	}
	if !validResolutionForGroup(resolution, declared) {
		b.addIssue(reportv1alpha1.RolloutExplainIssueResolutionMalformed, view, ptrInt(index))
		return true
	}
	if resolution.Source == omev1beta1.RolloutPlanSourcePolicy {
		group.Source = reportv1alpha1.RolloutPlanSourcePolicy
		group.Policy = projectPolicyRef(resolution.PolicyRef)
		if group.Policy != nil {
			group.Policy.Digest = resolution.ObservedDigest
			group.Policy.Evidence = reportv1alpha1.EvidenceReported
		}
	}
	group.PortableDigest = resolution.ObservedDigest
	if resolution.ShadowedPolicyRef != nil {
		if group.ShadowedPolicy == nil {
			group.ShadowedPolicy = &reportv1alpha1.RolloutPolicyReference{Kind: "RolloutPolicy"}
		}
		group.ShadowedPolicy.Name = resolution.ShadowedPolicyRef.Name
		group.ShadowedPolicy.Digest = resolution.ShadowedPolicyRef.WouldPinDigest
		group.ShadowedPolicy.Evidence = reportv1alpha1.EvidenceReported
	}
	return true
}

func (b *explainProjector) prepareResolutions(groupCount int, view reportv1alpha1.RolloutPlanView) {
	if b.resolutionsPrepared {
		return
	}
	b.resolutionsPrepared = true
	b.resolutions = map[int]*omev1beta1.RolloutGroupResolution{}
	b.malformedResolutions = map[int]bool{}
	if b.isvc.Status.Rollout == nil {
		return
	}
	for index := range b.isvc.Status.Rollout.Groups {
		resolution := &b.isvc.Status.Rollout.Groups[index]
		groupIndex := int(resolution.Index)
		if groupIndex < 0 || groupIndex >= groupCount {
			b.addIssue(reportv1alpha1.RolloutExplainIssueResolutionMalformed, view, nil)
			continue
		}
		if _, exists := b.resolutions[groupIndex]; exists || b.malformedResolutions[groupIndex] {
			if !b.malformedResolutions[groupIndex] {
				b.addIssue(reportv1alpha1.RolloutExplainIssueResolutionMalformed, view, ptrInt(groupIndex))
			}
			delete(b.resolutions, groupIndex)
			b.malformedResolutions[groupIndex] = true
			continue
		}
		b.resolutions[groupIndex] = resolution
	}
}

func validResolutionForGroup(
	resolution *omev1beta1.RolloutGroupResolution,
	declared *omev1beta1.RolloutGroup,
) bool {
	inlineProgressions := 0
	if declared.Canary != nil {
		inlineProgressions++
	}
	if declared.BlueGreen != nil {
		inlineProgressions++
	}
	if declared.RollingUpdate != nil {
		inlineProgressions++
	}
	expectedSource := omev1beta1.RolloutPlanSourceInline
	if inlineProgressions == 0 && declared.PolicyRef != nil {
		expectedSource = omev1beta1.RolloutPlanSourcePolicy
	}
	if resolution.Source != expectedSource || !policyRefsMatch(declared.PolicyRef, resolution.PolicyRef) {
		return false
	}
	if expectedSource == omev1beta1.RolloutPlanSourceInline {
		if !portableDigestPattern.MatchString(resolution.ObservedDigest) {
			return false
		}
	} else if resolution.ObservedDigest != "" && !portableDigestPattern.MatchString(resolution.ObservedDigest) {
		return false
	}
	expectShadow := expectedSource == omev1beta1.RolloutPlanSourceInline && declared.PolicyRef != nil
	if !expectShadow {
		return resolution.ShadowedPolicyRef == nil
	}
	if resolution.ShadowedPolicyRef == nil || resolution.ShadowedPolicyRef.Name != declared.PolicyRef.Name {
		return false
	}
	return resolution.ShadowedPolicyRef.WouldPinDigest == "" ||
		portableDigestPattern.MatchString(resolution.ShadowedPolicyRef.WouldPinDigest)
}

func policyRefsMatch(expected, observed *omev1beta1.RolloutPolicyRef) bool {
	projectedExpected := projectPolicyRef(expected)
	projectedObserved := projectPolicyRef(observed)
	if projectedExpected == nil || projectedObserved == nil {
		return expected == nil && observed == nil
	}
	return projectedExpected.Kind == projectedObserved.Kind &&
		projectedExpected.Name == projectedObserved.Name &&
		projectedExpected.Progression == projectedObserved.Progression
}

func (b *explainProjector) projectHolds() {
	if paused, _ := constants.RolloutPauseState(b.isvc.Annotations); paused {
		b.content.Holds = append(b.content.Holds, reportv1alpha1.RolloutExplainHold{
			Kind: reportv1alpha1.RolloutHoldGlobalPause, Evidence: reportv1alpha1.EvidenceDeclared,
		})
	}
	if b.content.Summary.PlanReady.State == reportv1alpha1.RolloutConditionFalse {
		b.content.Holds = append(b.content.Holds, reportv1alpha1.RolloutExplainHold{
			Kind: reportv1alpha1.RolloutHoldPlanParked, Evidence: reportv1alpha1.EvidenceReported,
		})
	}
	activeCanaryGroupIndex := -1
	for _, group := range b.content.EffectiveGroups {
		if group.Strategy == reportv1alpha1.RolloutStrategyCanary {
			activeCanaryGroupIndex = group.Index
			break
		}
	}
	if activeCanaryGroupIndex >= 0 && b.isvc.Status.Canary != nil {
		if b.isvc.Status.Canary.PreStepHold {
			b.content.Holds = append(b.content.Holds, reportv1alpha1.RolloutExplainHold{
				Kind: reportv1alpha1.RolloutHoldCanaryPreStep, Evidence: reportv1alpha1.EvidenceReported,
				Group: ptrInt(activeCanaryGroupIndex),
			})
		}
	}
	for _, group := range b.content.Observed.Groups {
		if group.Phase == reportv1alpha1.RolloutPhasePaused {
			b.content.Holds = append(b.content.Holds, reportv1alpha1.RolloutExplainHold{
				Kind: reportv1alpha1.RolloutHoldObservedPaused, Evidence: reportv1alpha1.EvidenceReported,
				Group: ptrInt(group.Index),
			})
		}
	}
}

func planMalformedCode(view reportv1alpha1.RolloutPlanView) reportv1alpha1.RolloutExplainIssueCode {
	if view == reportv1alpha1.RolloutPlanViewDeclared {
		return reportv1alpha1.RolloutExplainIssueDeclaredPlanMalformed
	}
	if view == reportv1alpha1.RolloutPlanViewLive {
		return reportv1alpha1.RolloutExplainIssueLivePlanMalformed
	}
	return reportv1alpha1.RolloutExplainIssueEffectivePlanMalformed
}

func (b *explainProjector) hasIssue(
	code reportv1alpha1.RolloutExplainIssueCode,
	view reportv1alpha1.RolloutPlanView,
	group *int,
) bool {
	for _, issue := range b.content.Issues {
		if issue.Code != code || issue.View != view {
			continue
		}
		if issue.Group == nil || group == nil {
			if issue.Group == nil && group == nil {
				return true
			}
			continue
		}
		if *issue.Group == *group {
			return true
		}
	}
	return false
}

func (b *explainProjector) hasAnyIssue(
	code reportv1alpha1.RolloutExplainIssueCode,
	view reportv1alpha1.RolloutPlanView,
) bool {
	for _, issue := range b.content.Issues {
		if issue.Code == code && issue.View == view {
			return true
		}
	}
	return false
}

func (b *explainProjector) addIssue(
	code reportv1alpha1.RolloutExplainIssueCode,
	view reportv1alpha1.RolloutPlanView,
	group *int,
) {
	if b.issueKeys == nil {
		b.issueKeys = map[string]struct{}{}
	}
	key := string(code) + "\x00" + string(view) + "\x00"
	if group != nil {
		key += strconv.Itoa(*group)
	}
	if _, exists := b.issueKeys[key]; exists {
		return
	}
	if len(b.content.Issues) >= maxExplainIssues {
		b.truncated = true
		return
	}
	b.issueKeys[key] = struct{}{}
	b.content.Issues = append(b.content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: code, View: view, Group: group,
	})
}
