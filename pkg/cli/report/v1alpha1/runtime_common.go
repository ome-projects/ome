package v1alpha1

import (
	"sort"
	"time"

	"sigs.k8s.io/ome/pkg/cli/report"
)

// RuntimeKind identifies the supported runtime object scopes.
type RuntimeKind string

const (
	RuntimeKindUnknown               RuntimeKind = "Unknown"
	RuntimeKindServingRuntime        RuntimeKind = "ServingRuntime"
	RuntimeKindClusterServingRuntime RuntimeKind = "ClusterServingRuntime"
)

// RuntimeSelectionSource identifies how the runtime was chosen.
type RuntimeSelectionSource string

const (
	RuntimeSelectionSourceExplicit RuntimeSelectionSource = "Explicit"
	RuntimeSelectionSourceSelected RuntimeSelectionSource = "Selected"
)

// RuntimeComponentType identifies a runtime serving component.
type RuntimeComponentType string

const (
	RuntimeComponentEngine  RuntimeComponentType = "engine"
	RuntimeComponentDecoder RuntimeComponentType = "decoder"
	RuntimeComponentRouter  RuntimeComponentType = "router"
)

// DeploymentMode identifies the workload shape used by a component.
type DeploymentMode string

const (
	DeploymentModeRawDeployment     DeploymentMode = "RawDeployment"
	DeploymentModeMultiNode         DeploymentMode = "MultiNode"
	DeploymentModeVirtualDeployment DeploymentMode = "VirtualDeployment"
	DeploymentModeOMENative         DeploymentMode = "OMENative"
	DeploymentModePDDisaggregated   DeploymentMode = "PDDisaggregated"
)

// DeploymentModeSource identifies the evidence used to determine a mode.
type DeploymentModeSource string

const (
	DeploymentModeSourceComponentAnnotation DeploymentModeSource = "ComponentAnnotation"
	DeploymentModeSourceServiceAnnotation   DeploymentModeSource = "ServiceAnnotation"
	DeploymentModeSourceServiceSpec         DeploymentModeSource = "ServiceSpec"
	DeploymentModeSourceLeaderWorkerShape   DeploymentModeSource = "LeaderWorkerShape"
	DeploymentModeSourceDefault             DeploymentModeSource = "Default"
)

// InheritanceState identifies whether declared inheritance was observed,
// inherently absent from a flattened snapshot, or unavailable.
type InheritanceState string

const (
	InheritanceStateObserved    InheritanceState = "Observed"
	InheritanceStateNotRecorded InheritanceState = "NotRecorded"
	InheritanceStateUnavailable InheritanceState = "Unavailable"
)

// RuntimePinMode identifies the requested runtime synchronization policy.
type RuntimePinMode string

const (
	RuntimePinModeAutoSync    RuntimePinMode = "AutoSync"
	RuntimePinModeManagedPin  RuntimePinMode = "ManagedPin"
	RuntimePinModeExplicitPin RuntimePinMode = "ExplicitPin"
	RuntimePinModeInvalidPin  RuntimePinMode = "InvalidPin"
)

// RuntimePinState identifies the observed pin resolution outcome.
type RuntimePinState string

const (
	RuntimePinStateNotApplicable           RuntimePinState = "NotApplicable"
	RuntimePinStateAwaitingPin             RuntimePinState = "AwaitingPin"
	RuntimePinStateResolved                RuntimePinState = "Resolved"
	RuntimePinStateDesiredReportedMismatch RuntimePinState = "DesiredReportedMismatch"
	RuntimePinStateRevisionMissing         RuntimePinState = "RevisionMissing"
	RuntimePinStateRevisionInvalid         RuntimePinState = "RevisionInvalid"
	RuntimePinStateRevisionDisabled        RuntimePinState = "RevisionDisabled"
	RuntimePinStateUnavailable             RuntimePinState = "Unavailable"
	RuntimePinStateInvalidIntent           RuntimePinState = "InvalidIntent"
)

// StatusFreshness relates status observedGeneration to object generation.
type StatusFreshness string

const (
	StatusFreshnessCurrent    StatusFreshness = "Current"
	StatusFreshnessStale      StatusFreshness = "Stale"
	StatusFreshnessUnobserved StatusFreshness = "Unobserved"
	StatusFreshnessInvalid    StatusFreshness = "Invalid"
)

// DriftConditionState identifies the bounded state of reported drift.
type DriftConditionState string

const (
	DriftConditionStateNotReported     DriftConditionState = "NotReported"
	DriftConditionStateReportedTrue    DriftConditionState = "ReportedTrue"
	DriftConditionStateReportedFalse   DriftConditionState = "ReportedFalse"
	DriftConditionStateReportedUnknown DriftConditionState = "ReportedUnknown"
	DriftConditionStateMalformed       DriftConditionState = "Malformed"
)

// RuntimeDriftCause is a bounded classification of a reported drift cause.
type RuntimeDriftCause string

const (
	RuntimeDriftCauseRevisionMismatch     RuntimeDriftCause = "RevisionMismatch"
	RuntimeDriftCauseRevisionMissing      RuntimeDriftCause = "RevisionMissing"
	RuntimeDriftCauseSourceRuntimeMissing RuntimeDriftCause = "SourceRuntimeMissing"
	RuntimeDriftCauseRuntimeMismatch      RuntimeDriftCause = "RuntimeMismatch"
	RuntimeDriftCausePinAdvanced          RuntimeDriftCause = "PinAdvanced"
	RuntimeDriftCauseOther                RuntimeDriftCause = "Other"
)

// RuntimeSyncState identifies the relationship between synchronization values
// without exposing those values.
type RuntimeSyncState string

const (
	RuntimeSyncStateAbsent       RuntimeSyncState = "Absent"
	RuntimeSyncStateAcknowledged RuntimeSyncState = "Acknowledged"
	RuntimeSyncStatePending      RuntimeSyncState = "Pending"
	RuntimeSyncStateStatusOnly   RuntimeSyncState = "StatusOnly"
)

// ConfigurationState identifies whether a runtime configuration is available.
type ConfigurationState string

const (
	ConfigurationStateAvailable   ConfigurationState = "Available"
	ConfigurationStateUnavailable ConfigurationState = "Unavailable"
)

// ConfigurationOrigin identifies where a runtime configuration was observed.
type ConfigurationOrigin string

const (
	ConfigurationOriginLiveRuntime        ConfigurationOrigin = "LiveRuntime"
	ConfigurationOriginControllerRevision ConfigurationOrigin = "ControllerRevision"
)

// RuntimeHashRelation identifies the relation between two bounded hashes.
type RuntimeHashRelation string

const (
	RuntimeHashRelationUnknown   RuntimeHashRelation = "Unknown"
	RuntimeHashRelationEqual     RuntimeHashRelation = "Equal"
	RuntimeHashRelationDifferent RuntimeHashRelation = "Different"
	RuntimeHashRelationAmbiguous RuntimeHashRelation = "Ambiguous"
)

// RuntimeRevisionRole identifies why a revision appears in a report.
type RuntimeRevisionRole string

const (
	RuntimeRevisionRoleActive    RuntimeRevisionRole = "Active"
	RuntimeRevisionRoleRequested RuntimeRevisionRole = "Requested"
	RuntimeRevisionRoleReported  RuntimeRevisionRole = "Reported"
	RuntimeRevisionRoleHistory   RuntimeRevisionRole = "History"
)

// RevisionConsistency describes consistency with the controller writer
// contract. It does not make a cryptographic claim.
type RevisionConsistency string

const (
	RevisionConsistencyConsistent   RevisionConsistency = "Consistent"
	RevisionConsistencyInconsistent RevisionConsistency = "Inconsistent"
	RevisionConsistencyUnknown      RevisionConsistency = "Unknown"
)

// RevisionRelation identifies a revision's relation to the live runtime.
type RevisionRelation string

const (
	RevisionRelationMatchesLive     RevisionRelation = "MatchesLive"
	RevisionRelationDiffersFromLive RevisionRelation = "DiffersFromLive"
	RevisionRelationAmbiguous       RevisionRelation = "Ambiguous"
	RevisionRelationUnknown         RevisionRelation = "Unknown"
)

// HistoryObservationState identifies the outcome of history collection.
type HistoryObservationState string

const (
	HistoryObservationStateNotRequested HistoryObservationState = "NotRequested"
	HistoryObservationStateComplete     HistoryObservationState = "Complete"
	HistoryObservationStatePartial      HistoryObservationState = "Partial"
	HistoryObservationStateUnavailable  HistoryObservationState = "Unavailable"
)

// HistoryCompleteness describes the bounds on reported revision history.
type HistoryCompleteness string

const (
	HistoryCompletenessNotRequested     HistoryCompleteness = "NotRequested"
	HistoryCompletenessRetentionBounded HistoryCompleteness = "RetentionBounded"
	HistoryCompletenessIncomplete       HistoryCompleteness = "Incomplete"
)

// RuntimeIssueCode is an extensible, message-free runtime issue identifier.
type RuntimeIssueCode string

const (
	RuntimeIssueDeclaredCompatibilityMismatch RuntimeIssueCode = "DeclaredCompatibilityMismatch"
	RuntimeIssueInvalidDeclaredKind           RuntimeIssueCode = "InvalidDeclaredKind"
	RuntimeIssueInheritanceUnavailable        RuntimeIssueCode = "InheritanceUnavailable"
	RuntimeIssueStatusUnobserved              RuntimeIssueCode = "StatusUnobserved"
	RuntimeIssueStatusStale                   RuntimeIssueCode = "StatusStale"
	RuntimeIssueStatusInvalid                 RuntimeIssueCode = "StatusInvalid"
	RuntimeIssueLiveRuntimeUnavailable        RuntimeIssueCode = "LiveRuntimeUnavailable"
	RuntimeIssueActiveRevisionUnreported      RuntimeIssueCode = "ActiveRevisionUnreported"
	RuntimeIssueActiveRevisionUnavailable     RuntimeIssueCode = "ActiveRevisionUnavailable"
	RuntimeIssueRevisionNotOMEManaged         RuntimeIssueCode = "RevisionNotOMEManaged"
	RuntimeIssueRevisionSourceMismatch        RuntimeIssueCode = "RevisionSourceMismatch"
	RuntimeIssueRevisionHashInvalid           RuntimeIssueCode = "RevisionHashInvalid"
	RuntimeIssueRevisionPayloadMalformed      RuntimeIssueCode = "RevisionPayloadMalformed"
	RuntimeIssueRevisionHashMismatch          RuntimeIssueCode = "RevisionHashMismatch"
	RuntimeIssueRevisionNameMismatch          RuntimeIssueCode = "RevisionNameMismatch"
	RuntimeIssueRevisionOrdinalUnexpected     RuntimeIssueCode = "RevisionOrdinalUnexpected"
	RuntimeIssueRevisionPayloadNonCanonical   RuntimeIssueCode = "RevisionPayloadNonCanonical"
	RuntimeIssueRevisionDataObjectPresent     RuntimeIssueCode = "RevisionDataObjectPresent"
	RuntimeIssueRevisionIdentityMismatch      RuntimeIssueCode = "RevisionIdentityMismatch"
	RuntimeIssueRevisionDisabled              RuntimeIssueCode = "RevisionDisabled"
	RuntimeIssueDuplicateRevision             RuntimeIssueCode = "DuplicateRevision"
	RuntimeIssueConflictingRevision           RuntimeIssueCode = "ConflictingRevision"
	RuntimeIssueDuplicateRevisionContent      RuntimeIssueCode = "DuplicateRevisionContent"
	RuntimeIssueRevisionHashCollision         RuntimeIssueCode = "RevisionHashCollision"
	RuntimeIssueRevisionNotFound              RuntimeIssueCode = "RevisionNotFound"
	RuntimeIssueRevisionUnavailable           RuntimeIssueCode = "RevisionUnavailable"
	RuntimeIssueReportedDriftConflict         RuntimeIssueCode = "ReportedDriftConflict"
	RuntimeIssueHistoryUnavailable            RuntimeIssueCode = "HistoryUnavailable"
	RuntimeIssueHistoryTruncated              RuntimeIssueCode = "HistoryTruncated"
)

// RuntimeWarning is a message-free warning emitted by a runtime report.
// Runtime reports intentionally expose only stable, bounded warning codes.
type RuntimeWarning struct {
	Code WarningCode `json:"code"`
}

// RuntimeSourceReference identifies one allowlisted source used to build a
// runtime report. It deliberately omits Kubernetes resource versions.
type RuntimeSourceReference struct {
	Kind              string            `json:"kind"`
	Namespace         string            `json:"namespace,omitempty"`
	Name              string            `json:"name"`
	UID               string            `json:"uid,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	Evidence          EvidenceLevel     `json:"evidence"`
	CollectedAt       time.Time         `json:"collectedAt"`
	UnavailableReason UnavailableReason `json:"unavailableReason,omitempty"`
}

type runtimeContent[T any] interface {
	Content[T]
	runtimeReportKind() string
	RuntimeEffectiveContent | RuntimeHistoryContent | RuntimeTreeContent
}

// RuntimeEnvelope carries the fields shared by runtime reports. It is kept
// separate from the general diagnostic envelope so runtime output cannot
// carry arbitrary warning messages. Its sealed content contract also lets
// canonicalization restore the report kind.
type RuntimeEnvelope[T runtimeContent[T]] struct {
	APIVersion  string                   `json:"apiVersion"`
	Kind        string                   `json:"kind"`
	Metadata    Metadata                 `json:"metadata"`
	CollectedAt time.Time                `json:"collectedAt"`
	Sources     []RuntimeSourceReference `json:"sources"`
	Content     T                        `json:"content"`
	Warnings    []RuntimeWarning         `json:"warnings"`
}

func newRuntimeEnvelope[T runtimeContent[T]](metadata Metadata, content T, clock Clock) RuntimeEnvelope[T] {
	if clock == nil {
		clock = SystemClock{}
	}
	return (RuntimeEnvelope[T]{
		APIVersion:  APIVersion,
		Kind:        content.runtimeReportKind(),
		Metadata:    metadata,
		CollectedAt: clock.Now().UTC(),
		Sources:     []RuntimeSourceReference{},
		Content:     content,
		Warnings:    []RuntimeWarning{},
	}).Canonical()
}

// Canonical returns a deterministic deep-enough copy suitable for rendering.
// It never reorders caller-owned slices.
func (e RuntimeEnvelope[T]) Canonical() RuntimeEnvelope[T] {
	result := e
	result.APIVersion = APIVersion
	result.Kind = e.Content.runtimeReportKind()
	result.CollectedAt = e.CollectedAt.UTC()
	result.Sources = append([]RuntimeSourceReference{}, e.Sources...)
	for i := range result.Sources {
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.SliceStable(result.Sources, func(i, j int) bool {
		return runtimeSourceLess(result.Sources[i], result.Sources[j])
	})
	result.Warnings = append([]RuntimeWarning{}, e.Warnings...)
	sort.SliceStable(result.Warnings, func(i, j int) bool {
		return result.Warnings[i].Code < result.Warnings[j].Code
	})
	result.Content = e.Content.Canonical()
	return result
}

func runtimeSourceLess(a, b RuntimeSourceReference) bool {
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	if a.UID != b.UID {
		return a.UID < b.UID
	}
	if a.Generation != b.Generation {
		return a.Generation < b.Generation
	}
	if a.Evidence != b.Evidence {
		return a.Evidence < b.Evidence
	}
	if !a.CollectedAt.Equal(b.CollectedAt) {
		return a.CollectedAt.Before(b.CollectedAt)
	}
	return a.UnavailableReason < b.UnavailableReason
}

// Table returns the human-readable view of the canonical typed content.
func (e RuntimeEnvelope[T]) Table() report.Table {
	canonical := e.Canonical()
	if content, ok := any(canonical.Content).(runtimeWarningTable); ok {
		return content.tableWithWarnings(canonical.Warnings)
	}
	return canonical.Content.Table()
}

type runtimeWarningTable interface {
	tableWithWarnings([]RuntimeWarning) report.Table
}

// RuntimeObjectReference is an allowlisted runtime object identity.
type RuntimeObjectReference struct {
	APIVersion string      `json:"apiVersion"`
	Kind       RuntimeKind `json:"kind"`
	Namespace  string      `json:"namespace,omitempty"`
	Name       string      `json:"name"`
	UID        string      `json:"uid,omitempty"`
	Generation int64       `json:"generation,omitempty"`
}

// RuntimeRevisionReference is an allowlisted ControllerRevision identity.
type RuntimeRevisionReference struct {
	Namespace string     `json:"namespace"`
	Name      string     `json:"name"`
	UID       string     `json:"uid,omitempty"`
	CreatedAt *time.Time `json:"createdAt,omitempty"`
}

// RuntimeComponent summarizes one runtime component's deployment mode.
type RuntimeComponent struct {
	Type                 RuntimeComponentType `json:"type"`
	DeploymentMode       DeploymentMode       `json:"deploymentMode"`
	DeploymentModeSource DeploymentModeSource `json:"deploymentModeSource"`
}

// RuntimeIssue identifies a bounded issue and its optional revision.
type RuntimeIssue struct {
	Code     RuntimeIssueCode `json:"code"`
	Revision string           `json:"revision,omitempty"`
}

func orDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
