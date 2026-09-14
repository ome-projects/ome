// Package retryblockprojection turns current InferenceReplica RetryBlock
// status into a typed, read-only CLI report.
package retryblockprojection

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	ErrInferenceServiceRequired        = errors.New("retry-block projection requires an InferenceService")
	ErrInferenceServiceIdentityInvalid = errors.New("retry-block projection requires a safe InferenceService identity")
	ErrComponentInvalid                = errors.New("retry-block projection requires engine, decoder, or router")
	ErrCollectionEvidenceInvalid       = errors.New("retry-block projection collection evidence is invalid")
)

const (
	maxUIDLength          = 128
	maxReasonDisplayWidth = 256
)

type Input struct {
	InferenceService      *omev1beta1.InferenceService
	Collection            instancecollection.Result
	CollectionUnavailable reportv1alpha1.UnavailableReason
	Component             omev1beta1.ComponentType
}

func Project(input Input, clock reportv1alpha1.Clock) (reportv1alpha1.InstanceRetryBlocksReport, error) {
	if input.InferenceService == nil {
		return reportv1alpha1.InstanceRetryBlocksReport{}, ErrInferenceServiceRequired
	}
	isvc := input.InferenceService
	if !validSourceIdentity(isvc.Namespace, isvc.Name, isvc.UID) || isvc.Generation <= 0 {
		return reportv1alpha1.InstanceRetryBlocksReport{}, ErrInferenceServiceIdentityInvalid
	}
	if !validComponent(input.Component) {
		return reportv1alpha1.InstanceRetryBlocksReport{}, ErrComponentInvalid
	}
	if input.CollectionUnavailable != "" && !validUnavailableReason(input.CollectionUnavailable) {
		return reportv1alpha1.InstanceRetryBlocksReport{}, ErrCollectionEvidenceInvalid
	}

	reportValue := reportv1alpha1.NewInstanceRetryBlocksReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name},
		reportv1alpha1.InstanceRetryBlocksContent{
			Summary: reportv1alpha1.InstanceRetryBlocksSummary{State: reportv1alpha1.InstanceRetryBlocksEmpty},
			Blocks:  []reportv1alpha1.InstanceRetryBlock{},
			Issues:  []reportv1alpha1.InstanceRetryBlockIssue{},
		},
		clock,
	)
	reportValue.Sources = append(reportValue.Sources, reportv1alpha1.SourceReference{
		Kind: "InferenceService", Namespace: isvc.Namespace, Name: isvc.Name,
		UID: string(isvc.UID), Generation: isvc.Generation,
		Evidence: reportv1alpha1.EvidenceReported,
	})

	partial, stale, unavailable := false, false, false
	addIssue := func(issue reportv1alpha1.InstanceRetryBlockIssue) {
		reportValue.Content.Issues = append(reportValue.Content.Issues, issue)
		partial = true
	}
	if input.CollectionUnavailable != "" {
		unavailable = true
		addIssue(reportv1alpha1.InstanceRetryBlockIssue{
			Code:      reportv1alpha1.InstanceRetryBlockIssueCollectionUnavailable,
			Component: reportComponent(input.Component), UnavailableReason: input.CollectionUnavailable,
		})
		reportValue.Sources = append(reportValue.Sources, reportv1alpha1.SourceReference{
			Kind: "InferenceReplicaList", Namespace: isvc.Namespace, Name: isvc.Name,
			Evidence: reportv1alpha1.EvidenceUnavailable, UnavailableReason: input.CollectionUnavailable,
		})
	}
	if input.Collection.Truncated {
		reportValue.Content.Summary.Truncated = true
		addIssue(reportv1alpha1.InstanceRetryBlockIssue{
			Code:      reportv1alpha1.InstanceRetryBlockIssueCollectionTruncated,
			Component: reportComponent(input.Component),
		})
	}
	for _, rejection := range input.Collection.Rejected {
		if !validRejection(rejection) {
			return reportv1alpha1.InstanceRetryBlocksReport{}, ErrCollectionEvidenceInvalid
		}
		addIssue(reportv1alpha1.InstanceRetryBlockIssue{
			Code:      reportv1alpha1.InstanceRetryBlockIssueIdentityRejected,
			Component: reportComponent(input.Component), InferenceReplica: rejection.Name,
		})
	}

	items := append([]omev1beta1.InferenceReplica{}, input.Collection.Items...)
	for i := range items {
		if !validCollectedReplicaIdentity(&items[i], isvc) {
			return reportv1alpha1.InstanceRetryBlocksReport{}, ErrCollectionEvidenceInvalid
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Spec.Component != items[j].Spec.Component {
			return componentRank(items[i].Spec.Component) < componentRank(items[j].Spec.Component)
		}
		if items[i].Name != items[j].Name {
			return items[i].Name < items[j].Name
		}
		return items[i].UID < items[j].UID
	})
	selected := make([]*omev1beta1.InferenceReplica, 0, 1)
	itemKeys := make(map[string]int, len(items))
	for i := range items {
		key := replicaKey(items[i].Name, items[i].Spec.Component)
		itemKeys[key]++
		if items[i].Spec.Component == input.Component {
			selected = append(selected, &items[i])
		}
	}
	truncatedSelected := false
	seenTruncations := make(map[string]struct{}, len(input.Collection.RetryBlocksTruncated))
	for _, truncation := range input.Collection.RetryBlocksTruncated {
		key := replicaKey(truncation.Name, truncation.Component)
		if len(utilvalidation.IsDNS1123Subdomain(truncation.Name)) != 0 ||
			!validComponent(truncation.Component) || itemKeys[key] != 1 {
			return reportv1alpha1.InstanceRetryBlocksReport{}, ErrCollectionEvidenceInvalid
		}
		if _, duplicate := seenTruncations[key]; duplicate {
			return reportv1alpha1.InstanceRetryBlocksReport{}, ErrCollectionEvidenceInvalid
		}
		seenTruncations[key] = struct{}{}
		if truncation.Component == input.Component {
			truncatedSelected = true
			reportValue.Content.Summary.Truncated = true
			addIssue(reportv1alpha1.InstanceRetryBlockIssue{
				Code:      reportv1alpha1.InstanceRetryBlockIssueBlocksTruncated,
				Component: reportComponent(input.Component), InferenceReplica: truncation.Name,
			})
		}
	}

	if len(selected) == 0 {
		if !unavailable && !input.Collection.Truncated {
			addIssue(reportv1alpha1.InstanceRetryBlockIssue{
				Code:      reportv1alpha1.InstanceRetryBlockIssueComponentNotProjected,
				Component: reportComponent(input.Component),
			})
		}
		return finish(reportValue, partial, stale, unavailable), nil
	}
	if len(selected) > 1 {
		addIssue(reportv1alpha1.InstanceRetryBlockIssue{
			Code:      reportv1alpha1.InstanceRetryBlockIssueDuplicateComponent,
			Component: reportComponent(input.Component),
		})
		for _, ir := range selected {
			reportValue.Sources = append(reportValue.Sources, sourceReference(ir))
		}
		return finish(reportValue, partial, stale, unavailable), nil
	}

	ir := selected[0]
	reportValue.Sources = append(reportValue.Sources, sourceReference(ir))
	freshness, freshnessIssues := sourceFreshness(ir, isvc.Generation)
	for _, code := range freshnessIssues {
		addIssue(reportv1alpha1.InstanceRetryBlockIssue{
			Code: code, Component: reportComponent(input.Component), InferenceReplica: ir.Name,
		})
	}
	if freshness == reportv1alpha1.StatusFreshnessStale {
		stale = true
	}
	if truncatedSelected && len(ir.Status.RetryBlocks) != 0 {
		return reportv1alpha1.InstanceRetryBlocksReport{}, ErrCollectionEvidenceInvalid
	}

	releaseAuthorityComplete := input.CollectionUnavailable == "" &&
		!input.Collection.Truncated && len(input.Collection.Rejected) == 0 &&
		isvc.DeletionTimestamp == nil && ir.DeletionTimestamp == nil
	targetCounts := make(map[string]int, len(ir.Status.RetryBlocks))
	for i := range ir.Status.RetryBlocks {
		targetCounts[ir.Status.RetryBlocks[i].TargetRevision]++
	}
	for i := range ir.Status.RetryBlocks {
		source := &ir.Status.RetryBlocks[i]
		block, valid, issueCodes := projectBlock(source, ir, freshness)
		if targetCounts[source.TargetRevision] > 1 {
			valid = false
			issueCodes = append(issueCodes, reportv1alpha1.InstanceRetryBlockIssueDuplicateTarget)
		}
		block.ReleaseEligible = releaseAuthorityComplete && valid &&
			freshness == reportv1alpha1.StatusFreshnessCurrent &&
			block.State == reportv1alpha1.InstanceRetryBlockHeld
		reportValue.Content.Blocks = append(reportValue.Content.Blocks, block)
		for _, code := range issueCodes {
			addIssue(reportv1alpha1.InstanceRetryBlockIssue{
				Code: code, Component: reportComponent(input.Component),
				InferenceReplica: ir.Name, TargetRevision: block.TargetRevision,
			})
		}
	}

	reportValue.Content.Summary.Blocks = len(reportValue.Content.Blocks)
	for _, block := range reportValue.Content.Blocks {
		if block.State == reportv1alpha1.InstanceRetryBlockHeld {
			reportValue.Content.Summary.Held++
		}
		if block.ReleaseEligible {
			reportValue.Content.Summary.Eligible++
		}
	}
	return finish(reportValue, partial, stale, unavailable), nil
}

func finish(
	reportValue reportv1alpha1.InstanceRetryBlocksReport,
	partial, stale, unavailable bool,
) reportv1alpha1.InstanceRetryBlocksReport {
	switch {
	case unavailable && len(reportValue.Content.Blocks) == 0:
		reportValue.Content.Summary.State = reportv1alpha1.InstanceRetryBlocksUnavailable
	case partial:
		reportValue.Content.Summary.State = reportv1alpha1.InstanceRetryBlocksPartial
	case len(reportValue.Content.Blocks) > 0:
		reportValue.Content.Summary.State = reportv1alpha1.InstanceRetryBlocksReported
	default:
		reportValue.Content.Summary.State = reportv1alpha1.InstanceRetryBlocksEmpty
	}
	if reportValue.Content.Summary.Truncated {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.Warning{Code: reportv1alpha1.WarningTruncated})
	}
	if stale {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.Warning{Code: reportv1alpha1.WarningStaleEvidence})
	}
	if partial && !stale && !reportValue.Content.Summary.Truncated {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.Warning{Code: reportv1alpha1.WarningPartialData})
	}
	if unavailable {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.Warning{Code: reportv1alpha1.WarningSourceUnavailable})
	}
	return reportValue.Canonical()
}

func projectBlock(
	source *omev1beta1.RetryBlock,
	ir *omev1beta1.InferenceReplica,
	freshness reportv1alpha1.StatusFreshness,
) (reportv1alpha1.InstanceRetryBlock, bool, []reportv1alpha1.InstanceRetryBlockIssueCode) {
	issues := make([]reportv1alpha1.InstanceRetryBlockIssueCode, 0, 4)
	target, targetValid := safeTargetRevision(source.TargetRevision)
	if !targetValid {
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueTargetRevisionInvalid)
	}
	state := reportv1alpha1.InstanceRetryBlockState(source.State)
	stateValid := validState(source.State)
	if !stateValid {
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueStateInvalid)
	}
	attemptsValid := source.AttemptsStarted > 0
	if !attemptsValid {
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueAttemptsInvalid)
	}
	timestampsValid := validTimestamps(source)
	if !timestampsValid {
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueTimestampsInvalid)
	}
	reason := printers.BoundedCell(source.Reason, maxReasonDisplayWidth)
	block := reportv1alpha1.InstanceRetryBlock{
		Component: reportComponent(ir.Spec.Component), InferenceReplica: ir.Name,
		TargetRevision: target, State: state, AttemptsStarted: source.AttemptsStarted,
		NextRetryAt: timePointer(source.NextRetryAt), FirstFailureAt: timePointer(source.FirstFailureAt),
		LastFailureAt: timePointer(source.LastFailureAt), Reason: reason,
		ReasonTruncated: reason != source.Reason, Freshness: freshness,
	}
	return block, targetValid && stateValid && attemptsValid && timestampsValid, issues
}

func sourceFreshness(
	ir *omev1beta1.InferenceReplica,
	isvcGeneration int64,
) (reportv1alpha1.StatusFreshness, []reportv1alpha1.InstanceRetryBlockIssueCode) {
	issues := []reportv1alpha1.InstanceRetryBlockIssueCode{}
	result := reportv1alpha1.StatusFreshnessCurrent
	switch parentGeneration(ir, isvcGeneration) {
	case generationMissing:
		result = reportv1alpha1.StatusFreshnessUnobserved
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueParentGenerationMissing)
	case generationInvalid:
		result = reportv1alpha1.StatusFreshnessInvalid
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueParentGenerationInvalid)
	case generationStale:
		result = reportv1alpha1.StatusFreshnessStale
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueParentGenerationStale)
	case generationAhead:
		result = reportv1alpha1.StatusFreshnessInvalid
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueParentGenerationAhead)
	}
	switch {
	case ir.Status.ObservedGeneration == 0 && ir.Generation > 0:
		if result != reportv1alpha1.StatusFreshnessInvalid {
			result = reportv1alpha1.StatusFreshnessUnobserved
		}
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueStatusUnobserved)
	case ir.Generation <= 0 || ir.Status.ObservedGeneration < 0 || ir.Status.ObservedGeneration > ir.Generation:
		result = reportv1alpha1.StatusFreshnessInvalid
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueObservedGenerationInvalid)
	case ir.Status.ObservedGeneration < ir.Generation:
		if result == reportv1alpha1.StatusFreshnessCurrent {
			result = reportv1alpha1.StatusFreshnessStale
		}
		issues = append(issues, reportv1alpha1.InstanceRetryBlockIssueStatusStale)
	}
	return result, issues
}

type generationState int

const (
	generationCurrent generationState = iota
	generationMissing
	generationInvalid
	generationStale
	generationAhead
)

func parentGeneration(ir *omev1beta1.InferenceReplica, expected int64) generationState {
	raw, present := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	if !present {
		return generationMissing
	}
	if raw == "" || len(raw) > 19 {
		return generationInvalid
	}
	for i := range raw {
		if raw[i] < '0' || raw[i] > '9' {
			return generationInvalid
		}
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 || strconv.FormatInt(value, 10) != raw {
		return generationInvalid
	}
	if value < expected {
		return generationStale
	}
	if value > expected {
		return generationAhead
	}
	return generationCurrent
}

func validTimestamps(block *omev1beta1.RetryBlock) bool {
	for _, value := range []*metav1.Time{block.NextRetryAt, block.FirstFailureAt, block.LastFailureAt} {
		if value != nil && value.IsZero() {
			return false
		}
	}
	if block.FirstFailureAt != nil && block.LastFailureAt != nil && block.LastFailureAt.Before(block.FirstFailureAt) {
		return false
	}
	if block.NextRetryAt != nil && block.FirstFailureAt != nil && block.NextRetryAt.Before(block.FirstFailureAt) {
		return false
	}
	switch block.State {
	case omev1beta1.RetryBlockBackoff:
		return block.NextRetryAt != nil
	case omev1beta1.RetryBlockHeld:
		return block.NextRetryAt == nil
	case omev1beta1.RetryBlockRetryInProgress:
		return true
	default:
		return block.NextRetryAt == nil
	}
}

func validState(state omev1beta1.RetryBlockState) bool {
	switch state {
	case omev1beta1.RetryBlockBackoff, omev1beta1.RetryBlockHeld, omev1beta1.RetryBlockRetryInProgress:
		return true
	default:
		return false
	}
}

func safeTargetRevision(value string) (string, bool) {
	if len(utilvalidation.IsDNS1123Subdomain(value)) == 0 {
		return value, true
	}
	digest := sha256.Sum256([]byte(value))
	return "invalid-" + hex.EncodeToString(digest[:4]), false
}

func timePointer(value *metav1.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func sourceReference(ir *omev1beta1.InferenceReplica) reportv1alpha1.SourceReference {
	return reportv1alpha1.SourceReference{
		Kind: "InferenceReplica", Namespace: ir.Namespace, Name: ir.Name,
		UID: string(ir.UID), Generation: ir.Generation,
		Evidence: reportv1alpha1.EvidenceReported,
	}
}

func validSourceIdentity(namespace, name string, uid types.UID) bool {
	return len(utilvalidation.IsDNS1123Label(namespace)) == 0 &&
		len(utilvalidation.IsDNS1123Subdomain(name)) == 0 && validUID(uid)
}

func validUID(uid types.UID) bool {
	if len(uid) == 0 || len(uid) > maxUIDLength {
		return false
	}
	for _, value := range []byte(uid) {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '-' || value == '_' || value == '.' {
			continue
		}
		return false
	}
	return true
}

func validCollectedReplicaIdentity(ir *omev1beta1.InferenceReplica, isvc *omev1beta1.InferenceService) bool {
	requireRelationshipLabel := len(utilvalidation.IsValidLabelValue(isvc.Name)) == 0
	if ir == nil || !validSourceIdentity(ir.Namespace, ir.Name, ir.UID) || ir.Namespace != isvc.Namespace ||
		(requireRelationshipLabel && ir.Labels[constants.InferenceServiceLabel] != isvc.Name) ||
		ir.Labels[constants.OMEComponentLabel] != string(ir.Spec.Component) ||
		ir.Spec.ParentRef.Name != isvc.Name || !validComponent(ir.Spec.Component) {
		return false
	}
	return validOwner(ir.OwnerReferences, isvc)
}

func validOwner(owners []metav1.OwnerReference, isvc *omev1beta1.InferenceService) bool {
	var controller *metav1.OwnerReference
	for i := range owners {
		if owners[i].Controller == nil || !*owners[i].Controller {
			continue
		}
		if controller != nil {
			return false
		}
		controller = &owners[i]
	}
	return controller != nil && controller.APIVersion == omev1beta1.SchemeGroupVersion.String() &&
		controller.Kind == "InferenceService" && controller.Name == isvc.Name && controller.UID == isvc.UID
}

func validComponent(component omev1beta1.ComponentType) bool {
	switch component {
	case omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent:
		return true
	default:
		return false
	}
}

func reportComponent(component omev1beta1.ComponentType) reportv1alpha1.RuntimeComponentType {
	return reportv1alpha1.RuntimeComponentType(component)
}

func componentRank(component omev1beta1.ComponentType) int {
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

func replicaKey(name string, component omev1beta1.ComponentType) string {
	return string(component) + "\x00" + name
}

func validRejection(rejection instancecollection.Rejection) bool {
	if rejection.Name != "INVALID" && len(utilvalidation.IsDNS1123Subdomain(rejection.Name)) != 0 {
		return false
	}
	switch rejection.Reason {
	case instancecollection.RejectionMetadata, instancecollection.RejectionLabel,
		instancecollection.RejectionParentReference, instancecollection.RejectionOwnerReference,
		instancecollection.RejectionComponent:
		return true
	default:
		return false
	}
}

func validUnavailableReason(reason reportv1alpha1.UnavailableReason) bool {
	switch reason {
	case reportv1alpha1.UnavailableNotFound, reportv1alpha1.UnavailableForbidden,
		reportv1alpha1.UnavailableUnsupportedAPI, reportv1alpha1.UnavailableStaleGeneration,
		reportv1alpha1.UnavailableMalformedPayload, reportv1alpha1.UnavailableNotConfigured,
		reportv1alpha1.UnavailableUnreadable, reportv1alpha1.UnavailableCycle,
		reportv1alpha1.UnavailableMaxDepthExceeded, reportv1alpha1.UnavailableDisabled:
		return true
	default:
		return false
	}
}
