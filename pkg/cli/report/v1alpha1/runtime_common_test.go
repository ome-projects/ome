package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestRuntimeReportEnumValuesAreStable(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"runtime kind unknown", string(v1alpha1.RuntimeKindUnknown), "Unknown"},
		{"runtime kind namespaced", string(v1alpha1.RuntimeKindServingRuntime), "ServingRuntime"},
		{"runtime kind cluster", string(v1alpha1.RuntimeKindClusterServingRuntime), "ClusterServingRuntime"},
		{"selection explicit", string(v1alpha1.RuntimeSelectionSourceExplicit), "Explicit"},
		{"selection selected", string(v1alpha1.RuntimeSelectionSourceSelected), "Selected"},
		{"component engine", string(v1alpha1.RuntimeComponentEngine), "engine"},
		{"component decoder", string(v1alpha1.RuntimeComponentDecoder), "decoder"},
		{"component router", string(v1alpha1.RuntimeComponentRouter), "router"},
		{"mode raw", string(v1alpha1.DeploymentModeRawDeployment), "RawDeployment"},
		{"mode multi-node", string(v1alpha1.DeploymentModeMultiNode), "MultiNode"},
		{"mode virtual", string(v1alpha1.DeploymentModeVirtualDeployment), "VirtualDeployment"},
		{"mode native", string(v1alpha1.DeploymentModeOMENative), "OMENative"},
		{"mode source annotation", string(v1alpha1.DeploymentModeSourceComponentAnnotation), "ComponentAnnotation"},
		{"mode source service annotation", string(v1alpha1.DeploymentModeSourceServiceAnnotation), "ServiceAnnotation"},
		{"mode source service", string(v1alpha1.DeploymentModeSourceServiceSpec), "ServiceSpec"},
		{"mode source leader worker", string(v1alpha1.DeploymentModeSourceLeaderWorkerShape), "LeaderWorkerShape"},
		{"mode source default", string(v1alpha1.DeploymentModeSourceDefault), "Default"},
		{"inheritance observed", string(v1alpha1.InheritanceStateObserved), "Observed"},
		{"inheritance not recorded", string(v1alpha1.InheritanceStateNotRecorded), "NotRecorded"},
		{"inheritance unavailable", string(v1alpha1.InheritanceStateUnavailable), "Unavailable"},
		{"pin auto", string(v1alpha1.RuntimePinModeAutoSync), "AutoSync"},
		{"pin managed", string(v1alpha1.RuntimePinModeManagedPin), "ManagedPin"},
		{"pin explicit", string(v1alpha1.RuntimePinModeExplicitPin), "ExplicitPin"},
		{"pin invalid", string(v1alpha1.RuntimePinModeInvalidPin), "InvalidPin"},
		{"pin state not applicable", string(v1alpha1.RuntimePinStateNotApplicable), "NotApplicable"},
		{"pin state awaiting", string(v1alpha1.RuntimePinStateAwaitingPin), "AwaitingPin"},
		{"pin state resolved", string(v1alpha1.RuntimePinStateResolved), "Resolved"},
		{"pin state mismatch", string(v1alpha1.RuntimePinStateDesiredReportedMismatch), "DesiredReportedMismatch"},
		{"pin state missing", string(v1alpha1.RuntimePinStateRevisionMissing), "RevisionMissing"},
		{"pin state invalid revision", string(v1alpha1.RuntimePinStateRevisionInvalid), "RevisionInvalid"},
		{"pin state disabled", string(v1alpha1.RuntimePinStateRevisionDisabled), "RevisionDisabled"},
		{"pin state unavailable", string(v1alpha1.RuntimePinStateUnavailable), "Unavailable"},
		{"pin state invalid intent", string(v1alpha1.RuntimePinStateInvalidIntent), "InvalidIntent"},
		{"freshness current", string(v1alpha1.StatusFreshnessCurrent), "Current"},
		{"freshness stale", string(v1alpha1.StatusFreshnessStale), "Stale"},
		{"freshness unobserved", string(v1alpha1.StatusFreshnessUnobserved), "Unobserved"},
		{"freshness invalid", string(v1alpha1.StatusFreshnessInvalid), "Invalid"},
		{"drift absent", string(v1alpha1.DriftConditionStateNotReported), "NotReported"},
		{"drift true", string(v1alpha1.DriftConditionStateReportedTrue), "ReportedTrue"},
		{"drift false", string(v1alpha1.DriftConditionStateReportedFalse), "ReportedFalse"},
		{"drift unknown", string(v1alpha1.DriftConditionStateReportedUnknown), "ReportedUnknown"},
		{"drift malformed", string(v1alpha1.DriftConditionStateMalformed), "Malformed"},
		{"drift cause revision mismatch", string(v1alpha1.RuntimeDriftCauseRevisionMismatch), "RevisionMismatch"},
		{"drift cause revision missing", string(v1alpha1.RuntimeDriftCauseRevisionMissing), "RevisionMissing"},
		{"drift cause source missing", string(v1alpha1.RuntimeDriftCauseSourceRuntimeMissing), "SourceRuntimeMissing"},
		{"drift cause runtime mismatch", string(v1alpha1.RuntimeDriftCauseRuntimeMismatch), "RuntimeMismatch"},
		{"drift cause pin advanced", string(v1alpha1.RuntimeDriftCausePinAdvanced), "PinAdvanced"},
		{"drift cause other", string(v1alpha1.RuntimeDriftCauseOther), "Other"},
		{"sync absent", string(v1alpha1.RuntimeSyncStateAbsent), "Absent"},
		{"sync acknowledged", string(v1alpha1.RuntimeSyncStateAcknowledged), "Acknowledged"},
		{"sync pending", string(v1alpha1.RuntimeSyncStatePending), "Pending"},
		{"sync status only", string(v1alpha1.RuntimeSyncStateStatusOnly), "StatusOnly"},
		{"configuration available", string(v1alpha1.ConfigurationStateAvailable), "Available"},
		{"configuration unavailable", string(v1alpha1.ConfigurationStateUnavailable), "Unavailable"},
		{"origin live", string(v1alpha1.ConfigurationOriginLiveRuntime), "LiveRuntime"},
		{"origin revision", string(v1alpha1.ConfigurationOriginControllerRevision), "ControllerRevision"},
		{"hash unknown", string(v1alpha1.RuntimeHashRelationUnknown), "Unknown"},
		{"hash equal", string(v1alpha1.RuntimeHashRelationEqual), "Equal"},
		{"hash different", string(v1alpha1.RuntimeHashRelationDifferent), "Different"},
		{"hash ambiguous", string(v1alpha1.RuntimeHashRelationAmbiguous), "Ambiguous"},
		{"role active", string(v1alpha1.RuntimeRevisionRoleActive), "Active"},
		{"role requested", string(v1alpha1.RuntimeRevisionRoleRequested), "Requested"},
		{"role reported", string(v1alpha1.RuntimeRevisionRoleReported), "Reported"},
		{"role history", string(v1alpha1.RuntimeRevisionRoleHistory), "History"},
		{"consistency consistent", string(v1alpha1.RevisionConsistencyConsistent), "Consistent"},
		{"consistency inconsistent", string(v1alpha1.RevisionConsistencyInconsistent), "Inconsistent"},
		{"consistency unknown", string(v1alpha1.RevisionConsistencyUnknown), "Unknown"},
		{"relation matches", string(v1alpha1.RevisionRelationMatchesLive), "MatchesLive"},
		{"relation differs", string(v1alpha1.RevisionRelationDiffersFromLive), "DiffersFromLive"},
		{"relation ambiguous", string(v1alpha1.RevisionRelationAmbiguous), "Ambiguous"},
		{"relation unknown", string(v1alpha1.RevisionRelationUnknown), "Unknown"},
		{"observation not requested", string(v1alpha1.HistoryObservationStateNotRequested), "NotRequested"},
		{"observation complete", string(v1alpha1.HistoryObservationStateComplete), "Complete"},
		{"observation partial", string(v1alpha1.HistoryObservationStatePartial), "Partial"},
		{"observation unavailable", string(v1alpha1.HistoryObservationStateUnavailable), "Unavailable"},
		{"completeness not requested", string(v1alpha1.HistoryCompletenessNotRequested), "NotRequested"},
		{"completeness retention", string(v1alpha1.HistoryCompletenessRetentionBounded), "RetentionBounded"},
		{"completeness incomplete", string(v1alpha1.HistoryCompletenessIncomplete), "Incomplete"},
		{"unavailable cycle", string(v1alpha1.UnavailableCycle), "Cycle"},
		{"unavailable max depth", string(v1alpha1.UnavailableMaxDepthExceeded), "MaxDepthExceeded"},
		{"unavailable disabled", string(v1alpha1.UnavailableDisabled), "Disabled"},
		{"issue revision not found", string(v1alpha1.RuntimeIssueRevisionNotFound), "RevisionNotFound"},
		{"issue revision unavailable", string(v1alpha1.RuntimeIssueRevisionUnavailable), "RevisionUnavailable"},
		{"issue conflicting revision", string(v1alpha1.RuntimeIssueConflictingRevision), "ConflictingRevision"},
		{"issue duplicate revision content", string(v1alpha1.RuntimeIssueDuplicateRevisionContent), "DuplicateRevisionContent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestRuntimeReportCommonSchemaIsTypedAndAllowlisted(t *testing.T) {
	types := []reflect.Type{
		reflect.TypeOf(v1alpha1.RuntimeEnvelope[v1alpha1.RuntimeEffectiveContent]{}),
		reflect.TypeOf(v1alpha1.RuntimeEnvelope[v1alpha1.RuntimeHistoryContent]{}),
		reflect.TypeOf(v1alpha1.RuntimeSourceReference{}),
		reflect.TypeOf(v1alpha1.RuntimeObjectReference{}),
		reflect.TypeOf(v1alpha1.RuntimeRevisionReference{}),
		reflect.TypeOf(v1alpha1.RuntimeComponent{}),
		reflect.TypeOf(v1alpha1.RuntimeIssue{}),
	}
	for _, typ := range types {
		assertRuntimeReportSchema(t, typ, map[reflect.Type]bool{})
	}
}

func TestRuntimeEnvelopeRejectsEmbeddedExternalContent(t *testing.T) {
	command := exec.CommandContext(t.Context(), "go", "test", "./testdata/hostile_runtime_content")
	output, err := command.CombinedOutput()

	require.Error(t, err, "hostile embedded content unexpectedly instantiated RuntimeEnvelope:\n%s", output)
	assert.Contains(t, string(output), "hostileContent does not satisfy")
}

func TestRuntimeEnvelopeWarningsAreCodeOnlyAndCanonical(t *testing.T) {
	now := time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)
	sourceTime := time.Date(2026, time.August, 31, 11, 20, 0, 0, time.FixedZone("test", -7*60*60))
	reportValue := v1alpha1.NewRuntimeEffectiveReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RuntimeEffectiveContent{},
		fixedClock{now: now},
	)
	reportValue.Sources = []v1alpha1.RuntimeSourceReference{
		{Kind: "Pod", Namespace: "prod", Name: "z", CollectedAt: sourceTime},
		{Kind: "InferenceService", Namespace: "prod", Name: "chat"},
	}
	reportValue.Warnings = []v1alpha1.RuntimeWarning{
		{Code: v1alpha1.WarningTruncated},
		{Code: v1alpha1.WarningPartialData},
	}

	canonical := reportValue.Canonical()

	assert.Equal(t, []v1alpha1.RuntimeWarning{
		{Code: v1alpha1.WarningPartialData},
		{Code: v1alpha1.WarningTruncated},
	}, canonical.Warnings)
	assert.Equal(t, []v1alpha1.RuntimeWarning{
		{Code: v1alpha1.WarningTruncated},
		{Code: v1alpha1.WarningPartialData},
	}, reportValue.Warnings, "canonicalization must not reorder caller-owned warnings")
	require.Len(t, canonical.Sources, 2)
	assert.Equal(t, "InferenceService", canonical.Sources[0].Kind)
	assert.Equal(t, now, canonical.Sources[0].CollectedAt)
	assert.Equal(t, "Pod", canonical.Sources[1].Kind)
	assert.Equal(t, time.UTC, canonical.Sources[1].CollectedAt.Location())
	assert.Equal(t, "Pod", reportValue.Sources[0].Kind, "canonicalization must not reorder caller-owned sources")
	assert.Equal(t, "test", reportValue.Sources[0].CollectedAt.Location().String())

	warningType := reflect.TypeOf(v1alpha1.RuntimeWarning{})
	require.Equal(t, 1, warningType.NumField(), "runtime warnings must not gain arbitrary text fields")
	assert.Equal(t, "Code", warningType.Field(0).Name)

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, reportValue))
	assert.NotContains(t, output.String(), `"message"`)
}

func TestRuntimeEnvelopeCanonicalOrdersEveryRuntimeSourceField(t *testing.T) {
	collectedAt := time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC)
	base := v1alpha1.RuntimeSourceReference{
		Kind: "Pod", Namespace: "prod", Name: "runtime", UID: "uid", Generation: 2,
		Evidence: v1alpha1.EvidenceObserved, CollectedAt: collectedAt,
		UnavailableReason: v1alpha1.UnavailableNotFound,
	}
	tests := []struct {
		name  string
		early v1alpha1.RuntimeSourceReference
		late  v1alpha1.RuntimeSourceReference
	}{
		{
			name: "kind",
			early: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Kind = "A"
			}),
			late: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Kind = "Z"
			}),
		},
		{
			name: "namespace",
			early: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Namespace = "a"
			}),
			late: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Namespace = "z"
			}),
		},
		{
			name: "name",
			early: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Name = "a"
			}),
			late: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Name = "z"
			}),
		},
		{
			name: "uid",
			early: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.UID = "a"
			}),
			late: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.UID = "z"
			}),
		},
		{
			name: "generation",
			early: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Generation = 1
			}),
			late: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Generation = 3
			}),
		},
		{
			name: "evidence",
			early: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Evidence = v1alpha1.EvidenceDeclared
			}),
			late: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.Evidence = v1alpha1.EvidenceReported
			}),
		},
		{
			name: "collected at",
			early: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.CollectedAt = collectedAt.Add(-time.Minute)
			}),
			late: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.CollectedAt = collectedAt.Add(time.Minute)
			}),
		},
		{
			name: "unavailable reason",
			early: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.UnavailableReason = v1alpha1.UnavailableNotFound
			}),
			late: withRuntimeSource(base, func(source *v1alpha1.RuntimeSourceReference) {
				source.UnavailableReason = v1alpha1.UnavailableUnsupportedAPI
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forward := v1alpha1.NewRuntimeEffectiveReport(
				v1alpha1.Metadata{}, v1alpha1.RuntimeEffectiveContent{}, fixedClock{now: collectedAt},
			)
			forward.Sources = []v1alpha1.RuntimeSourceReference{tt.early, tt.late}
			reverse := forward
			reverse.Sources = []v1alpha1.RuntimeSourceReference{tt.late, tt.early}

			forward = forward.Canonical()
			reverse = reverse.Canonical()

			assert.Equal(t, forward.Sources, reverse.Sources, "input order must not affect canonical source ordering")
			require.Len(t, forward.Sources, 2)
			assert.Equal(t, tt.early, forward.Sources[0])
		})
	}
}

func TestRuntimeEnvelopeAcceptsDefaultSystemClock(t *testing.T) {
	before := time.Now().UTC()
	reportValue := v1alpha1.NewRuntimeHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RuntimeHistoryContent{},
		nil,
	)
	after := time.Now().UTC()

	assert.False(t, reportValue.CollectedAt.Before(before))
	assert.False(t, reportValue.CollectedAt.After(after))
}

func TestRuntimeEnvelopeCanonicalRestoresFixedKind(t *testing.T) {
	effective := v1alpha1.NewRuntimeEffectiveReport(
		v1alpha1.Metadata{Name: "chat"},
		v1alpha1.RuntimeEffectiveContent{},
		fixedClock{},
	)
	history := v1alpha1.NewRuntimeHistoryReport(
		v1alpha1.Metadata{Name: "chat"},
		v1alpha1.RuntimeHistoryContent{},
		fixedClock{},
	)
	effective.Kind = v1alpha1.RuntimeHistoryReportKind
	history.Kind = v1alpha1.RuntimeEffectiveReportKind

	assert.Equal(t, v1alpha1.RuntimeEffectiveReportKind, effective.Canonical().Kind)
	assert.Equal(t, v1alpha1.RuntimeHistoryReportKind, history.Canonical().Kind)
}

func TestRuntimeReportSchemaAuditRejectsEveryUnsafeFieldCategory(t *testing.T) {
	typ := reflect.TypeOf(struct {
		Image           string   `json:"image"`
		Command         string   `json:"command"`
		Args            []string `json:"args"`
		Env             []string `json:"env"`
		Environment     []string `json:"environment"`
		Secret          string   `json:"secret"`
		Label           string   `json:"label"`
		Annotation      string   `json:"annotation"`
		RawReason       string   `json:"rawReason"`
		Error           string   `json:"error"`
		Message         string   `json:"message"`
		Token           string   `json:"token"`
		SyncToken       string   `json:"syncToken"`
		ResourceVersion string   `json:"resourceVersion"`
	}{})

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		assert.True(t, isForbiddenRuntimeSchemaField(field), "unsafe category %s was not rejected", field.Name)
	}
}

func TestRuntimeIssueCodesUseBoundedIdentifiers(t *testing.T) {
	want := []string{
		"DeclaredCompatibilityMismatch", "InvalidDeclaredKind", "InheritanceUnavailable",
		"StatusUnobserved", "StatusStale", "StatusInvalid", "LiveRuntimeUnavailable",
		"ActiveRevisionUnreported", "ActiveRevisionUnavailable", "RevisionNotOMEManaged",
		"RevisionSourceMismatch", "RevisionHashInvalid", "RevisionPayloadMalformed",
		"RevisionHashMismatch", "RevisionNameMismatch", "RevisionOrdinalUnexpected",
		"RevisionPayloadNonCanonical", "RevisionDataObjectPresent", "RevisionIdentityMismatch",
		"RevisionDisabled", "DuplicateRevision", "ConflictingRevision", "DuplicateRevisionContent",
		"RevisionHashCollision", "RevisionNotFound", "RevisionUnavailable", "ReportedDriftConflict",
		"HistoryUnavailable", "HistoryTruncated",
	}
	got := []string{
		string(v1alpha1.RuntimeIssueDeclaredCompatibilityMismatch), string(v1alpha1.RuntimeIssueInvalidDeclaredKind),
		string(v1alpha1.RuntimeIssueInheritanceUnavailable), string(v1alpha1.RuntimeIssueStatusUnobserved),
		string(v1alpha1.RuntimeIssueStatusStale), string(v1alpha1.RuntimeIssueStatusInvalid),
		string(v1alpha1.RuntimeIssueLiveRuntimeUnavailable), string(v1alpha1.RuntimeIssueActiveRevisionUnreported),
		string(v1alpha1.RuntimeIssueActiveRevisionUnavailable), string(v1alpha1.RuntimeIssueRevisionNotOMEManaged),
		string(v1alpha1.RuntimeIssueRevisionSourceMismatch), string(v1alpha1.RuntimeIssueRevisionHashInvalid),
		string(v1alpha1.RuntimeIssueRevisionPayloadMalformed), string(v1alpha1.RuntimeIssueRevisionHashMismatch),
		string(v1alpha1.RuntimeIssueRevisionNameMismatch), string(v1alpha1.RuntimeIssueRevisionOrdinalUnexpected),
		string(v1alpha1.RuntimeIssueRevisionPayloadNonCanonical), string(v1alpha1.RuntimeIssueRevisionDataObjectPresent),
		string(v1alpha1.RuntimeIssueRevisionIdentityMismatch), string(v1alpha1.RuntimeIssueRevisionDisabled),
		string(v1alpha1.RuntimeIssueDuplicateRevision), string(v1alpha1.RuntimeIssueConflictingRevision),
		string(v1alpha1.RuntimeIssueDuplicateRevisionContent), string(v1alpha1.RuntimeIssueRevisionHashCollision),
		string(v1alpha1.RuntimeIssueRevisionNotFound), string(v1alpha1.RuntimeIssueRevisionUnavailable),
		string(v1alpha1.RuntimeIssueReportedDriftConflict), string(v1alpha1.RuntimeIssueHistoryUnavailable),
		string(v1alpha1.RuntimeIssueHistoryTruncated),
	}
	assert.Equal(t, want, got)
}

func TestRuntimeRevisionReferenceOmitsUnknownCreationTime(t *testing.T) {
	encoded, err := json.Marshal(v1alpha1.RuntimeRevisionReference{
		Namespace: "ome-system",
		Name:      "expected-revision",
	})
	require.NoError(t, err)

	assert.JSONEq(t, `{"namespace":"ome-system","name":"expected-revision"}`, string(encoded))
	typeOfReference := reflect.TypeOf(v1alpha1.RuntimeRevisionReference{})
	createdAt, found := typeOfReference.FieldByName("CreatedAt")
	require.True(t, found)
	assert.Equal(t, reflect.Pointer, createdAt.Type.Kind())
}

func runtimeReportTime(value time.Time) *time.Time {
	return &value
}

func assertRuntimeReportSchema(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	assertTypedSchema(t, typ, map[reflect.Type]bool{})
	assertRuntimeReportFieldNames(t, typ, seen)
}

func assertRuntimeReportFieldNames(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.PkgPath() == "time" || seen[typ] {
		return
	}
	seen[typ] = true
	if typ.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		require.False(t, isForbiddenRuntimeSchemaField(field), "runtime schema contains unsafe field %s.%s", typ, field.Name)
		assertRuntimeReportFieldNames(t, field.Type, seen)
	}
}

func isForbiddenRuntimeSchemaField(field reflect.StructField) bool {
	jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
	for _, name := range []string{strings.ToLower(field.Name), strings.ToLower(jsonName)} {
		if name == "env" || name == "reason" || name == "resourceversion" || strings.Contains(name, "rawreason") {
			return true
		}
		for _, fragment := range []string{
			"image", "command", "args", "environment", "secret", "label",
			"annotation", "error", "message", "token",
		} {
			if strings.Contains(name, fragment) {
				return true
			}
		}
	}
	return false
}

func withRuntimeSource(
	source v1alpha1.RuntimeSourceReference,
	change func(*v1alpha1.RuntimeSourceReference),
) v1alpha1.RuntimeSourceReference {
	change(&source)
	return source
}
