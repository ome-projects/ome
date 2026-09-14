package rolloutprojection

import (
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

var (
	// ErrSubjectIdentityInvalid rejects an object that cannot be bound to a
	// controller-producible Kubernetes identity.
	ErrSubjectIdentityInvalid = errors.New("inference service identity is invalid")
	historyRunHashPattern     = regexp.MustCompile(`^[0-9a-f]{12}$`)
)

// ProjectHistory projects the two rollout-run slots and current safe status
// retained on one already-read InferenceService. It performs no cluster reads
// and never serializes a pinned rollout body.
func ProjectHistory(
	isvc *omev1beta1.InferenceService,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.RolloutHistoryReport, error) {
	if isvc == nil {
		return reportv1alpha1.RolloutHistoryReport{}, ErrNilInferenceService
	}
	if isvc.Name == "" {
		return reportv1alpha1.RolloutHistoryReport{}, ErrSubjectNameRequired
	}
	if isvc.Namespace == "" {
		return reportv1alpha1.RolloutHistoryReport{}, ErrNamespaceRequired
	}
	if isvc.UID == "" {
		return reportv1alpha1.RolloutHistoryReport{}, ErrSubjectUIDRequired
	}
	if len(utilvalidation.IsDNS1123Subdomain(isvc.Name)) != 0 ||
		len(utilvalidation.IsDNS1123Label(isvc.Namespace)) != 0 ||
		len(utilvalidation.IsValidLabelValue(string(isvc.UID))) != 0 ||
		isvc.Generation <= 0 {
		return reportv1alpha1.RolloutHistoryReport{}, ErrSubjectIdentityInvalid
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	now := clock.Now().UTC()
	fixedClock := reportv1alpha1.ClockFunc(func() time.Time { return now })
	observed, err := Project(isvc, fixedClock)
	if err != nil {
		return reportv1alpha1.RolloutHistoryReport{}, err
	}

	result := reportv1alpha1.NewRolloutHistoryReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name},
		reportv1alpha1.RolloutHistoryContent{
			Summary: reportv1alpha1.RolloutHistorySummary{
				State:           reportv1alpha1.RolloutHistoryStateReported,
				Completeness:    reportv1alpha1.RolloutHistoryRetentionBounded,
				CurrentState:    observed.Content.Summary.State,
				CurrentEvidence: observed.Content.Summary.Evidence,
				CurrentEpoch:    observed.Content.Summary.Epoch,
			},
			Runs:         []reportv1alpha1.RolloutHistoryRun{},
			Provenance:   []reportv1alpha1.RolloutHistoryProvenance{},
			Revisions:    projectHistoryRevisions(observed.Content.Components),
			StatusIssues: append([]reportv1alpha1.RolloutIssue{}, observed.Content.Issues...),
			Issues:       []reportv1alpha1.RolloutHistoryIssue{},
		},
		fixedClock,
	)
	result.Sources = make([]reportv1alpha1.RolloutHistorySourceReference, 0, len(observed.Sources))
	for _, source := range observed.Sources {
		result.Sources = append(result.Sources, reportv1alpha1.RolloutHistorySourceReference{
			Kind: source.Kind, Namespace: source.Namespace, Name: source.Name,
			Generation: source.Generation, Evidence: source.Evidence, CollectedAt: source.CollectedAt,
		})
	}

	if isvc.Status.Rollout == nil {
		result.Content.Summary.State = reportv1alpha1.RolloutHistoryStateUnavailable
		if len(result.Content.Revisions) > 0 {
			result.Content.Summary.State = reportv1alpha1.RolloutHistoryStatePartial
			addHistoryWarning(&result, reportv1alpha1.WarningPartialData)
		}
		addHistoryIssue(&result, reportv1alpha1.RolloutHistoryIssue{
			Code: reportv1alpha1.RolloutHistoryIssueRunStatusUnavailable,
		})
		addHistoryWarning(&result, reportv1alpha1.WarningSourceUnavailable)
		if len(result.Content.StatusIssues) > 0 && result.Content.Summary.State == reportv1alpha1.RolloutHistoryStatePartial {
			addHistoryWarning(&result, reportv1alpha1.WarningPartialData)
		}
		return result.Canonical(), nil
	}

	status := isvc.Status.Rollout
	var activeRun *reportv1alpha1.RolloutHistoryRun
	var activeProvenance []reportv1alpha1.RolloutHistoryProvenance
	if status.ActiveRun != nil {
		run, provenance, targetIssues, ok := projectActiveHistory(isvc, status.ActiveRun)
		if ok {
			activeRun = &run
			activeProvenance = provenance
			for _, issue := range targetIssues {
				addHistoryIssue(&result, issue)
			}
		} else {
			addHistoryIssue(&result, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueActiveRunMalformed,
				View: reportv1alpha1.RolloutHistoryViewActive,
			})
		}
	}
	var lastRun *reportv1alpha1.RolloutHistoryRun
	var lastProvenance []reportv1alpha1.RolloutHistoryProvenance
	if status.LastRun != nil {
		run, provenance, ok := projectLastHistory(status.LastRun)
		if ok {
			lastRun = &run
			lastProvenance = provenance
		} else {
			addHistoryIssue(&result, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueLastRunMalformed,
				View: reportv1alpha1.RolloutHistoryViewLast,
			})
		}
	}
	if activeRun != nil && lastRun != nil && lastRun.ClosedAt != nil &&
		activeRun.OpenedAt != nil && lastRun.ClosedAt.After(*activeRun.OpenedAt) {
		lastRun = nil
		lastProvenance = nil
		addHistoryIssue(&result, reportv1alpha1.RolloutHistoryIssue{
			Code: reportv1alpha1.RolloutHistoryIssueRunChronologyMalformed,
			View: reportv1alpha1.RolloutHistoryViewLast,
		})
	}
	if activeRun != nil {
		result.Content.Runs = append(result.Content.Runs, *activeRun)
		result.Content.Provenance = append(result.Content.Provenance, activeProvenance...)
	}
	if lastRun != nil {
		result.Content.Runs = append(result.Content.Runs, *lastRun)
		result.Content.Provenance = append(result.Content.Provenance, lastProvenance...)
	}
	current, currentIssues := projectCurrentHistory(isvc)
	result.Content.Provenance = append(result.Content.Provenance, current...)
	for _, issue := range currentIssues {
		addHistoryIssue(&result, issue)
	}

	switch {
	case len(result.Content.Issues) > 0:
		result.Content.Summary.State = reportv1alpha1.RolloutHistoryStatePartial
		addHistoryWarning(&result, reportv1alpha1.WarningPartialData)
	case len(result.Content.Runs) == 0 && len(result.Content.Provenance) == 0 &&
		len(result.Content.Revisions) == 0:
		result.Content.Summary.State = reportv1alpha1.RolloutHistoryStateEmpty
	case len(result.Content.StatusIssues) > 0:
		result.Content.Summary.State = reportv1alpha1.RolloutHistoryStatePartial
		addHistoryWarning(&result, reportv1alpha1.WarningPartialData)
	default:
		result.Content.Summary.State = reportv1alpha1.RolloutHistoryStateReported
	}
	return result.Canonical(), nil
}

func projectActiveHistory(
	isvc *omev1beta1.InferenceService,
	active *omev1beta1.RolloutRun,
) (reportv1alpha1.RolloutHistoryRun, []reportv1alpha1.RolloutHistoryProvenance, []reportv1alpha1.RolloutHistoryIssue, bool) {
	if active == nil || !validHistoryRunID(isvc.Name, active.RunID) ||
		active.OpenedAt.IsZero() || active.PinnedAt.IsZero() ||
		active.PinnedAt.Before(&active.OpenedAt) ||
		len(active.Plan.Groups) < 1 || len(active.Plan.Groups) > maxExplainGroups {
		return reportv1alpha1.RolloutHistoryRun{}, nil, nil, false
	}
	effective := isvc.Spec
	effective.Rollout = active.Plan.AsRolloutSpec(isvc.Spec.Rollout)
	if !validStoredRolloutSpec(&effective) || len(invalidPinnedPlanGroups(active.Plan.Groups)) > 0 {
		return reportv1alpha1.RolloutHistoryRun{}, nil, nil, false
	}

	components := make(map[omev1beta1.ComponentType]struct{}, 3)
	provenance := make([]reportv1alpha1.RolloutHistoryProvenance, 0, len(active.Plan.Groups))
	observedAt := active.PinnedAt.Time.UTC()
	for index := range active.Plan.Groups {
		group := &active.Plan.Groups[index]
		projected, ok := projectPinnedHistoryProvenance(
			reportv1alpha1.RolloutHistoryViewActive, index, group.Source,
			group.PolicyRef, group.PolicyGeneration, group.PortableDigest,
			&group.Group, &observedAt,
		)
		if !ok {
			return reportv1alpha1.RolloutHistoryRun{}, nil, nil, false
		}
		for _, component := range group.Group.Components {
			if _, duplicate := components[component]; duplicate {
				return reportv1alpha1.RolloutHistoryRun{}, nil, nil, false
			}
			components[component] = struct{}{}
		}
		provenance = append(provenance, projected)
	}

	targets, targetIssues := projectActiveHistoryTargets(active.TargetRevisions)
	openedAt := active.OpenedAt.Time.UTC()
	pinnedAt := active.PinnedAt.Time.UTC()
	return reportv1alpha1.RolloutHistoryRun{
		Slot:    reportv1alpha1.RolloutHistoryRunActive,
		Outcome: reportv1alpha1.RolloutHistoryRunActiveState,
		RunID:   active.RunID, OpenedAt: &openedAt, PinnedAt: &pinnedAt,
		GroupCount: len(active.Plan.Groups), Targets: targets,
	}, provenance, targetIssues, true
}

func projectActiveHistoryTargets(
	values []omev1beta1.RolloutRunTarget,
) ([]reportv1alpha1.RolloutHistoryTarget, []reportv1alpha1.RolloutHistoryIssue) {
	if len(values) == 0 {
		return []reportv1alpha1.RolloutHistoryTarget{}, []reportv1alpha1.RolloutHistoryIssue{{
			Code: reportv1alpha1.RolloutHistoryIssueActiveTargetMalformed,
			View: reportv1alpha1.RolloutHistoryViewActive,
		}}
	}
	counts := make(map[omev1beta1.ComponentType]int, len(values))
	for _, value := range values {
		counts[value.Component]++
	}
	targets := make([]reportv1alpha1.RolloutHistoryTarget, 0, min(len(values), 3))
	issues := []reportv1alpha1.RolloutHistoryIssue{}
	malformedUnscoped := false
	for _, value := range values {
		component := projectComponent(value.Component)
		if component == "" || counts[value.Component] != 1 {
			malformedUnscoped = true
			continue
		}
		if value.Revision == "" {
			targets = append(targets, reportv1alpha1.RolloutHistoryTarget{
				Component: component, Evidence: reportv1alpha1.EvidenceUnavailable,
			})
			issues = append(issues, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueActiveTargetUnavailable,
				View: reportv1alpha1.RolloutHistoryViewActive, Component: component,
			})
			continue
		}
		if !safeRevisionHash(value.Revision) {
			issues = append(issues, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueActiveTargetMalformed,
				View: reportv1alpha1.RolloutHistoryViewActive, Component: component,
			})
			continue
		}
		targets = append(targets, reportv1alpha1.RolloutHistoryTarget{
			Component: component, RevisionHash: value.Revision,
			Evidence: reportv1alpha1.EvidenceReported,
		})
	}
	if malformedUnscoped || len(values) > 3 {
		issues = append(issues, reportv1alpha1.RolloutHistoryIssue{
			Code: reportv1alpha1.RolloutHistoryIssueActiveTargetMalformed,
			View: reportv1alpha1.RolloutHistoryViewActive,
		})
	}
	return targets, issues
}

func projectPinnedHistoryProvenance(
	view reportv1alpha1.RolloutHistoryView,
	groupIndex int,
	source omev1beta1.RolloutPlanSource,
	policyRef *omev1beta1.RolloutPolicyRef,
	policyGeneration int64,
	digest string,
	group *omev1beta1.RolloutGroup,
	observedAt *time.Time,
) (reportv1alpha1.RolloutHistoryProvenance, bool) {
	strategy, strategyValid := pinnedGroupStrategy(group)
	if !strategyValid || group == nil || group.PolicyRef != nil || !validGroupBody(group) ||
		!portableDigestPattern.MatchString(digest) {
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	computed, err := rolloutpolicy.ProgressionDigest(group)
	if err != nil || computed != digest {
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	result := reportv1alpha1.RolloutHistoryProvenance{
		View: view, Group: groupIndex, PortableDigest: digest,
		DigestEvidence: reportv1alpha1.EvidenceComputed, ObservedAt: observedAt,
	}
	switch source {
	case omev1beta1.RolloutPlanSourceInline:
		if policyRef != nil || policyGeneration != 0 {
			return reportv1alpha1.RolloutHistoryProvenance{}, false
		}
		result.Source = reportv1alpha1.RolloutPlanSourceInline
	case omev1beta1.RolloutPlanSourcePolicy:
		policy := projectPinnedPolicyRef(policyRef, strategy)
		if policy == nil || policyGeneration < 0 || !validPinnedPolicyBody(group) {
			return reportv1alpha1.RolloutHistoryProvenance{}, false
		}
		policy.Generation = policyGeneration
		policy.Digest = digest
		policy.Evidence = reportv1alpha1.EvidenceReported
		result.Source = reportv1alpha1.RolloutPlanSourcePolicy
		result.Policy = policy
	default:
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	return result, true
}

func projectLastHistory(
	last *omev1beta1.RolloutRunRecord,
) (reportv1alpha1.RolloutHistoryRun, []reportv1alpha1.RolloutHistoryProvenance, bool) {
	if last == nil || last.OpenedAt == nil || last.ClosedAt == nil ||
		last.OpenedAt.IsZero() || last.ClosedAt.IsZero() ||
		last.ClosedAt.Before(last.OpenedAt) || len(last.Groups) < 1 ||
		len(last.Groups) > maxExplainGroups {
		return reportv1alpha1.RolloutHistoryRun{}, nil, false
	}
	outcome := reportv1alpha1.RolloutHistoryRunOutcome("")
	switch last.Outcome {
	case omev1beta1.RolloutRunCompleted:
		outcome = reportv1alpha1.RolloutHistoryRunCompleted
	case omev1beta1.RolloutRunRolledBack:
		outcome = reportv1alpha1.RolloutHistoryRunRolledBack
	case omev1beta1.RolloutRunSuperseded:
		outcome = reportv1alpha1.RolloutHistoryRunSuperseded
	default:
		return reportv1alpha1.RolloutHistoryRun{}, nil, false
	}
	closedAt := last.ClosedAt.Time.UTC()
	provenance := make([]reportv1alpha1.RolloutHistoryProvenance, 0, len(last.Groups))
	for index := range last.Groups {
		projected, ok := projectRetainedHistoryProvenance(index, &last.Groups[index], &closedAt)
		if !ok {
			return reportv1alpha1.RolloutHistoryRun{}, nil, false
		}
		provenance = append(provenance, projected)
	}
	openedAt := last.OpenedAt.Time.UTC()
	return reportv1alpha1.RolloutHistoryRun{
		Slot: reportv1alpha1.RolloutHistoryRunLast, Outcome: outcome,
		OpenedAt: &openedAt, ClosedAt: &closedAt, GroupCount: len(last.Groups),
		Targets: []reportv1alpha1.RolloutHistoryTarget{},
	}, provenance, true
}

func projectRetainedHistoryProvenance(
	index int,
	value *omev1beta1.RolloutRunProvenance,
	observedAt *time.Time,
) (reportv1alpha1.RolloutHistoryProvenance, bool) {
	if value == nil || !portableDigestPattern.MatchString(value.PortableDigest) {
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	result := reportv1alpha1.RolloutHistoryProvenance{
		View: reportv1alpha1.RolloutHistoryViewLast, Group: index,
		PortableDigest: value.PortableDigest, DigestEvidence: reportv1alpha1.EvidenceReported,
		ObservedAt: observedAt,
	}
	switch value.Source {
	case omev1beta1.RolloutPlanSourceInline:
		if value.PolicyRef != nil {
			return reportv1alpha1.RolloutHistoryProvenance{}, false
		}
		result.Source = reportv1alpha1.RolloutPlanSourceInline
	case omev1beta1.RolloutPlanSourcePolicy:
		policy := projectRetainedPolicyRef(value.PolicyRef)
		if policy == nil {
			return reportv1alpha1.RolloutHistoryProvenance{}, false
		}
		policy.Digest = value.PortableDigest
		result.Source = reportv1alpha1.RolloutPlanSourcePolicy
		result.Policy = policy
	default:
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	return result, true
}

func projectRetainedPolicyRef(
	value *omev1beta1.RolloutPolicyRef,
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
	progression := ""
	if value.Progression != "" {
		strategy := projectProgression(value.Progression)
		if strategy == reportv1alpha1.RolloutStrategyUnknown {
			return nil
		}
		progression = progressionString(strategy)
	}
	return &reportv1alpha1.RolloutPolicyReference{
		Kind: kind, Name: value.Name, Progression: progression,
		Evidence: reportv1alpha1.EvidenceReported,
	}
}

func projectCurrentHistory(
	isvc *omev1beta1.InferenceService,
) ([]reportv1alpha1.RolloutHistoryProvenance, []reportv1alpha1.RolloutHistoryIssue) {
	groups := isvc.Spec.GetRolloutGroups()
	resolutions := isvc.Status.Rollout.Groups
	if len(groups) == 0 {
		if len(resolutions) == 0 {
			return []reportv1alpha1.RolloutHistoryProvenance{}, []reportv1alpha1.RolloutHistoryIssue{}
		}
		return nil, []reportv1alpha1.RolloutHistoryIssue{{
			Code: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			View: reportv1alpha1.RolloutHistoryViewCurrent,
		}}
	}
	if len(groups) > maxExplainGroups || !validStoredRolloutSpec(&isvc.Spec) {
		return nil, []reportv1alpha1.RolloutHistoryIssue{{
			Code: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			View: reportv1alpha1.RolloutHistoryViewCurrent,
		}}
	}
	if len(resolutions) > len(groups) || len(resolutions) > maxExplainGroups {
		return nil, []reportv1alpha1.RolloutHistoryIssue{{
			Code: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			View: reportv1alpha1.RolloutHistoryViewCurrent,
		}}
	}
	byIndex := make(map[int]*omev1beta1.RolloutGroupResolution, len(resolutions))
	issues := []reportv1alpha1.RolloutHistoryIssue{}
	for index := range resolutions {
		groupIndex := int(resolutions[index].Index)
		if groupIndex < 0 || groupIndex >= len(groups) || byIndex[groupIndex] != nil {
			issues = append(issues, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
				View: reportv1alpha1.RolloutHistoryViewCurrent,
			})
			return nil, issues
		}
		byIndex[groupIndex] = &resolutions[index]
	}
	result := make([]reportv1alpha1.RolloutHistoryProvenance, 0, len(groups))
	for index := range groups {
		resolution := byIndex[index]
		if resolution == nil {
			groupIndex := index
			issues = append(issues, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMissing,
				View: reportv1alpha1.RolloutHistoryViewCurrent, Group: &groupIndex,
			})
			continue
		}
		projected, ok := projectCurrentResolution(&groups[index], resolution, index)
		if !ok {
			groupIndex := index
			issues = append(issues, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
				View: reportv1alpha1.RolloutHistoryViewCurrent, Group: &groupIndex,
			})
			continue
		}
		result = append(result, projected)
	}
	return result, issues
}

func projectCurrentResolution(
	group *omev1beta1.RolloutGroup,
	resolution *omev1beta1.RolloutGroupResolution,
	index int,
) (reportv1alpha1.RolloutHistoryProvenance, bool) {
	if group == nil || resolution == nil || int(resolution.Index) != index {
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	expectedSource := omev1beta1.RolloutPlanSourceInline
	hasProgression := group.Canary != nil || group.BlueGreen != nil || group.RollingUpdate != nil
	if !hasProgression && group.PolicyRef != nil {
		expectedSource = omev1beta1.RolloutPlanSourcePolicy
	}
	if resolution.Source != expectedSource || !policyRefsMatch(group.PolicyRef, resolution.PolicyRef) {
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	if resolution.ObservedDigest != "" && !portableDigestPattern.MatchString(resolution.ObservedDigest) {
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	if expectedSource == omev1beta1.RolloutPlanSourceInline && hasProgression {
		digest, err := rolloutpolicy.ProgressionDigest(group)
		if err != nil || digest != resolution.ObservedDigest {
			return reportv1alpha1.RolloutHistoryProvenance{}, false
		}
	}
	if expectedSource == omev1beta1.RolloutPlanSourceInline && !hasProgression &&
		resolution.ObservedDigest != "" {
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	expectShadow := expectedSource == omev1beta1.RolloutPlanSourceInline && group.PolicyRef != nil
	if expectShadow != (resolution.ShadowedPolicyRef != nil) {
		return reportv1alpha1.RolloutHistoryProvenance{}, false
	}
	result := reportv1alpha1.RolloutHistoryProvenance{
		View: reportv1alpha1.RolloutHistoryViewCurrent, Group: index,
		PortableDigest: resolution.ObservedDigest,
	}
	if resolution.ObservedDigest == "" {
		result.DigestEvidence = reportv1alpha1.EvidenceUnavailable
	} else if expectedSource == omev1beta1.RolloutPlanSourceInline {
		result.DigestEvidence = reportv1alpha1.EvidenceComputed
	} else {
		result.DigestEvidence = reportv1alpha1.EvidenceReported
	}
	if expectedSource == omev1beta1.RolloutPlanSourceInline {
		result.Source = reportv1alpha1.RolloutPlanSourceInline
	} else {
		policy := projectPolicyRef(resolution.PolicyRef)
		if policy == nil {
			return reportv1alpha1.RolloutHistoryProvenance{}, false
		}
		policy.Evidence = reportv1alpha1.EvidenceReported
		policy.Digest = resolution.ObservedDigest
		result.Source = reportv1alpha1.RolloutPlanSourcePolicy
		result.Policy = policy
	}
	if resolution.ShadowedPolicyRef != nil {
		if resolution.ShadowedPolicyRef.Name != group.PolicyRef.Name ||
			(resolution.ShadowedPolicyRef.WouldPinDigest != "" &&
				!portableDigestPattern.MatchString(resolution.ShadowedPolicyRef.WouldPinDigest)) {
			return reportv1alpha1.RolloutHistoryProvenance{}, false
		}
		shadow := projectPolicyRef(group.PolicyRef)
		if shadow == nil {
			return reportv1alpha1.RolloutHistoryProvenance{}, false
		}
		shadow.Evidence = reportv1alpha1.EvidenceReported
		shadow.Digest = resolution.ShadowedPolicyRef.WouldPinDigest
		result.ShadowedPolicy = shadow
	}
	return result, true
}

func projectHistoryRevisions(
	components []reportv1alpha1.RolloutComponentStatus,
) []reportv1alpha1.RolloutHistoryRevision {
	result := make([]reportv1alpha1.RolloutHistoryRevision, 0, len(components)*3)
	for _, component := range components {
		for _, value := range []struct {
			role reportv1alpha1.RolloutRevisionRole
			hash string
		}{
			{role: reportv1alpha1.RolloutRevisionCurrent, hash: component.RolledOutRevisionHash},
			{role: reportv1alpha1.RolloutHistoryRevisionReady, hash: component.ReadyRevisionHash},
			{role: reportv1alpha1.RolloutRevisionPrevious, hash: component.PreviousRevisionHash},
		} {
			if value.hash == "" {
				continue
			}
			result = append(result, reportv1alpha1.RolloutHistoryRevision{
				Component: component.Type, Role: value.role, RevisionHash: value.hash,
				Phase: component.Phase,
			})
		}
	}
	return result
}

func validHistoryRunID(subjectName, value string) bool {
	prefix := subjectName + "-"
	return strings.HasPrefix(value, prefix) &&
		len(value) == len(prefix)+12 && historyRunHashPattern.MatchString(strings.TrimPrefix(value, prefix))
}

func addHistoryIssue(
	reportValue *reportv1alpha1.RolloutHistoryReport,
	issue reportv1alpha1.RolloutHistoryIssue,
) {
	for _, existing := range reportValue.Content.Issues {
		if existing.Code == issue.Code && existing.View == issue.View &&
			equalHistoryGroup(existing.Group, issue.Group) && existing.Component == issue.Component {
			return
		}
	}
	reportValue.Content.Issues = append(reportValue.Content.Issues, issue)
}

func equalHistoryGroup(left, right *int) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func addHistoryWarning(
	reportValue *reportv1alpha1.RolloutHistoryReport,
	code reportv1alpha1.WarningCode,
) {
	if slices.Contains(reportValue.Warnings, reportv1alpha1.RolloutWarning{Code: code}) {
		return
	}
	reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.RolloutWarning{Code: code})
}
