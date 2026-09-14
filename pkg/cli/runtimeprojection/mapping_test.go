package runtimeprojection

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type enumMappingCase struct {
	name  string
	got   string
	ok    bool
	want  string
	valid bool
}

func TestClosedRuntimeEnumMappings(t *testing.T) {
	tests := []enumMappingCase{
		selectionMapping("selection explicit", effective.RuntimeExplicit, reportv1alpha1.RuntimeSelectionSourceExplicit),
		selectionMapping("selection selected", effective.RuntimeSelected, reportv1alpha1.RuntimeSelectionSourceSelected),
		selectionMapping("selection unknown", effective.RuntimeSelectionSource("hostile"), ""),
		runtimeKindMapping("runtime kind namespaced", runtimeselector.KindServingRuntime, reportv1alpha1.RuntimeKindServingRuntime),
		runtimeKindMapping("runtime kind cluster", runtimeselector.KindClusterServingRuntime, reportv1alpha1.RuntimeKindClusterServingRuntime),
		runtimeKindMapping("runtime kind absent", "", reportv1alpha1.RuntimeKindUnknown),
		runtimeKindMapping("runtime kind hostile", "hostile-secret-kind", ""),
		pinModeMapping("pin auto", effective.RuntimePinModeAutoSync, reportv1alpha1.RuntimePinModeAutoSync),
		pinModeMapping("pin managed", effective.RuntimePinModeManagedPin, reportv1alpha1.RuntimePinModeManagedPin),
		pinModeMapping("pin explicit", effective.RuntimePinModeExplicitPin, reportv1alpha1.RuntimePinModeExplicitPin),
		pinModeMapping("pin invalid", effective.RuntimePinModeInvalidPin, reportv1alpha1.RuntimePinModeInvalidPin),
		pinModeMapping("pin unknown", effective.RuntimePinMode("hostile"), ""),
		pinStateMapping("pin state not applicable", effective.RuntimePinStateNotApplicable, reportv1alpha1.RuntimePinStateNotApplicable),
		pinStateMapping("pin state awaiting", effective.RuntimePinStateAwaitingPin, reportv1alpha1.RuntimePinStateAwaitingPin),
		pinStateMapping("pin state resolved", effective.RuntimePinStateResolved, reportv1alpha1.RuntimePinStateResolved),
		pinStateMapping("pin state mismatch", effective.RuntimePinStateDesiredReportedMismatch, reportv1alpha1.RuntimePinStateDesiredReportedMismatch),
		pinStateMapping("pin state missing", effective.RuntimePinStateRevisionMissing, reportv1alpha1.RuntimePinStateRevisionMissing),
		pinStateMapping("pin state invalid revision", effective.RuntimePinStateRevisionInvalid, reportv1alpha1.RuntimePinStateRevisionInvalid),
		pinStateMapping("pin state disabled", effective.RuntimePinStateRevisionDisabled, reportv1alpha1.RuntimePinStateRevisionDisabled),
		pinStateMapping("pin state unavailable", effective.RuntimePinStateUnavailable, reportv1alpha1.RuntimePinStateUnavailable),
		pinStateMapping("pin state invalid intent", effective.RuntimePinStateInvalidIntent, reportv1alpha1.RuntimePinStateInvalidIntent),
		pinStateMapping("pin state unknown", effective.RuntimePinState("hostile"), ""),
		freshnessMapping("freshness current", effective.StatusFreshnessCurrent, reportv1alpha1.StatusFreshnessCurrent),
		freshnessMapping("freshness stale", effective.StatusFreshnessStale, reportv1alpha1.StatusFreshnessStale),
		freshnessMapping("freshness unknown", effective.StatusFreshnessUnknown, reportv1alpha1.StatusFreshnessUnobserved),
		freshnessMapping("freshness inconsistent", effective.StatusFreshnessInconsistent, reportv1alpha1.StatusFreshnessInvalid),
		freshnessMapping("freshness hostile", effective.StatusFreshness("hostile"), ""),
		syncMapping("sync absent", effective.SyncTokenStateAbsent, reportv1alpha1.RuntimeSyncStateAbsent),
		syncMapping("sync acknowledged", effective.SyncTokenStateAcknowledged, reportv1alpha1.RuntimeSyncStateAcknowledged),
		syncMapping("sync pending", effective.SyncTokenStatePending, reportv1alpha1.RuntimeSyncStatePending),
		syncMapping("sync status only", effective.SyncTokenStateStatusOnly, reportv1alpha1.RuntimeSyncStateStatusOnly),
		syncMapping("sync hostile", effective.SyncTokenState("hostile"), ""),
		driftStateMapping("drift absent", effective.RuntimeDriftStateNotReported, reportv1alpha1.DriftConditionStateNotReported),
		driftStateMapping("drift true", effective.RuntimeDriftStateReportedTrue, reportv1alpha1.DriftConditionStateReportedTrue),
		driftStateMapping("drift false", effective.RuntimeDriftStateReportedFalse, reportv1alpha1.DriftConditionStateReportedFalse),
		driftStateMapping("drift unknown", effective.RuntimeDriftStateReportedUnknown, reportv1alpha1.DriftConditionStateReportedUnknown),
		driftStateMapping("drift malformed", effective.RuntimeDriftStateMalformed, reportv1alpha1.DriftConditionStateMalformed),
		driftStateMapping("drift hostile", effective.RuntimeDriftState("hostile"), ""),
		driftCauseMapping("drift cause omitted", "", ""),
		driftCauseMapping("drift cause revision mismatch", effective.RuntimeDriftReasonRevisionMismatch, reportv1alpha1.RuntimeDriftCauseRevisionMismatch),
		driftCauseMapping("drift cause revision missing", effective.RuntimeDriftReasonRevisionMissing, reportv1alpha1.RuntimeDriftCauseRevisionMissing),
		driftCauseMapping("drift cause source missing", effective.RuntimeDriftReasonSourceRuntimeMissing, reportv1alpha1.RuntimeDriftCauseSourceRuntimeMissing),
		driftCauseMapping("drift cause runtime mismatch", effective.RuntimeDriftReasonRuntimeMismatch, reportv1alpha1.RuntimeDriftCauseRuntimeMismatch),
		driftCauseMapping("drift cause pin advanced", effective.RuntimeDriftReasonPinAdvanced, reportv1alpha1.RuntimeDriftCausePinAdvanced),
		driftCauseMapping("drift cause other", effective.RuntimeDriftReasonOther, reportv1alpha1.RuntimeDriftCauseOther),
		driftCauseMapping("drift cause hostile", effective.RuntimeDriftReason("hostile-secret-reason"), ""),
		hashMapping("hash unknown", effective.RuntimeHashRelationUnknown, reportv1alpha1.RuntimeHashRelationUnknown),
		hashMapping("hash equal", effective.RuntimeHashRelationEqual, reportv1alpha1.RuntimeHashRelationEqual),
		hashMapping("hash different", effective.RuntimeHashRelationDifferent, reportv1alpha1.RuntimeHashRelationDifferent),
		hashMapping("hash ambiguous", effective.RuntimeHashRelationAmbiguous, reportv1alpha1.RuntimeHashRelationAmbiguous),
		hashMapping("hash hostile", effective.RuntimeHashRelation("hostile"), ""),
		originMapping("origin live", effective.ConfigurationOriginLiveRuntime, reportv1alpha1.ConfigurationOriginLiveRuntime),
		originMapping("origin revision", effective.ConfigurationOriginControllerRevision, reportv1alpha1.ConfigurationOriginControllerRevision),
		originMapping("origin hostile", effective.ConfigurationOrigin("hostile"), ""),
		componentMapping("component engine", v1beta1.EngineComponent, reportv1alpha1.RuntimeComponentEngine),
		componentMapping("component decoder", v1beta1.DecoderComponent, reportv1alpha1.RuntimeComponentDecoder),
		componentMapping("component router", v1beta1.RouterComponent, reportv1alpha1.RuntimeComponentRouter),
		componentMapping("component hostile", v1beta1.ComponentType("hostile"), ""),
		modeMapping("mode raw", constants.RawDeployment, reportv1alpha1.DeploymentModeRawDeployment),
		modeMapping("mode multi", constants.MultiNode, reportv1alpha1.DeploymentModeMultiNode),
		modeMapping("mode virtual", constants.VirtualDeployment, reportv1alpha1.DeploymentModeVirtualDeployment),
		modeMapping("mode native", constants.OMENative, reportv1alpha1.DeploymentModeOMENative),
		modeMapping("mode hostile", constants.DeploymentModeType("hostile"), ""),
		modeSourceMapping("mode source annotation", effective.DeploymentModeComponentAnnotation, reportv1alpha1.DeploymentModeSourceComponentAnnotation),
		modeSourceMapping("mode source service annotation", effective.DeploymentModeServiceAnnotation, reportv1alpha1.DeploymentModeSourceServiceAnnotation),
		modeSourceMapping("mode source service", effective.DeploymentModeServiceSpec, reportv1alpha1.DeploymentModeSourceServiceSpec),
		modeSourceMapping("mode source shape", effective.DeploymentModeLeaderWorkerShape, reportv1alpha1.DeploymentModeSourceLeaderWorkerShape),
		modeSourceMapping("mode source default", effective.DeploymentModeDefault, reportv1alpha1.DeploymentModeSourceDefault),
		modeSourceMapping("mode source hostile", effective.ComponentDeploymentModeSource("hostile"), ""),
		inheritanceReasonMapping("inheritance not found", effective.InheritanceNotFound, reportv1alpha1.UnavailableNotFound),
		inheritanceReasonMapping("inheritance forbidden", effective.InheritanceForbidden, reportv1alpha1.UnavailableForbidden),
		inheritanceReasonMapping("inheritance cycle", effective.InheritanceCycle, reportv1alpha1.UnavailableCycle),
		inheritanceReasonMapping("inheritance max depth", effective.InheritanceMaxDepthExceeded, reportv1alpha1.UnavailableMaxDepthExceeded),
		inheritanceReasonMapping("inheritance malformed", effective.InheritanceMalformed, reportv1alpha1.UnavailableMalformedPayload),
		inheritanceReasonMapping("inheritance unreadable", effective.InheritanceUnreadable, reportv1alpha1.UnavailableUnreadable),
		inheritanceReasonMapping("inheritance hostile", effective.InheritanceUnavailableReason("hostile"), ""),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.valid, test.ok)
			assert.Equal(t, test.want, test.got)
		})
	}
}

func TestRevisionEvidenceMappingsAreExhaustive(t *testing.T) {
	roleTests := []struct {
		input effective.RuntimeRevisionRole
		want  reportv1alpha1.RuntimeRevisionRole
		valid bool
	}{
		{effective.RuntimeRevisionRoleRequested, reportv1alpha1.RuntimeRevisionRoleRequested, true},
		{effective.RuntimeRevisionRoleReported, reportv1alpha1.RuntimeRevisionRoleReported, true},
		{effective.RuntimeRevisionRoleActive, reportv1alpha1.RuntimeRevisionRoleActive, true},
		{effective.RuntimeRevisionRoleHistory, reportv1alpha1.RuntimeRevisionRoleHistory, true},
		{effective.RuntimeRevisionRole("hostile"), "", false},
	}
	for _, test := range roleTests {
		got, ok := mapRevisionRole(test.input)
		assert.Equal(t, test.valid, ok)
		assert.Equal(t, test.want, got)
	}

	consistencyTests := []struct {
		input effective.RevisionConsistencyState
		want  reportv1alpha1.RevisionConsistency
		valid bool
	}{
		{effective.RevisionConsistencyConsistent, reportv1alpha1.RevisionConsistencyConsistent, true},
		{effective.RevisionConsistencyInconsistent, reportv1alpha1.RevisionConsistencyInconsistent, true},
		{effective.RevisionConsistencyUnknown, reportv1alpha1.RevisionConsistencyUnknown, true},
		{effective.RevisionConsistencyState("hostile"), "", false},
	}
	for _, test := range consistencyTests {
		got, ok := mapRevisionConsistency(test.input)
		assert.Equal(t, test.valid, ok)
		assert.Equal(t, test.want, got)
	}

	relationTests := []struct {
		input effective.RuntimeHashRelation
		want  reportv1alpha1.RevisionRelation
		valid bool
	}{
		{effective.RuntimeHashRelationUnknown, reportv1alpha1.RevisionRelationUnknown, true},
		{effective.RuntimeHashRelationEqual, reportv1alpha1.RevisionRelationMatchesLive, true},
		{effective.RuntimeHashRelationDifferent, reportv1alpha1.RevisionRelationDiffersFromLive, true},
		{effective.RuntimeHashRelationAmbiguous, reportv1alpha1.RevisionRelationAmbiguous, true},
		{effective.RuntimeHashRelation("hostile"), "", false},
	}
	for _, test := range relationTests {
		got, ok := mapRevisionRelation(test.input)
		assert.Equal(t, test.valid, ok)
		assert.Equal(t, test.want, got)
	}

	issueTests := []struct {
		input effective.RevisionConsistencyCode
		want  reportv1alpha1.RuntimeIssueCode
		valid bool
	}{
		{effective.RevisionConsistencyCreatedBy, reportv1alpha1.RuntimeIssueRevisionNotOMEManaged, true},
		{effective.RevisionConsistencySourceName, reportv1alpha1.RuntimeIssueRevisionSourceMismatch, true},
		{effective.RevisionConsistencySourceKind, reportv1alpha1.RuntimeIssueRevisionSourceMismatch, true},
		{effective.RevisionConsistencySourceNamespace, reportv1alpha1.RuntimeIssueRevisionSourceMismatch, true},
		{effective.RevisionConsistencyHashLabelInvalid, reportv1alpha1.RuntimeIssueRevisionHashInvalid, true},
		{effective.RevisionConsistencyHashLabelMismatch, reportv1alpha1.RuntimeIssueRevisionHashMismatch, true},
		{effective.RevisionConsistencyNameHash, reportv1alpha1.RuntimeIssueRevisionNameMismatch, true},
		{effective.RevisionConsistencyOrdinal, reportv1alpha1.RuntimeIssueRevisionOrdinalUnexpected, true},
		{effective.RevisionConsistencyPayloadCanonicality, reportv1alpha1.RuntimeIssueRevisionPayloadNonCanonical, true},
		{effective.RevisionConsistencyUnexpectedDataObject, reportv1alpha1.RuntimeIssueRevisionDataObjectPresent, true},
		{effective.RevisionConsistencyMalformedPayload, reportv1alpha1.RuntimeIssueRevisionPayloadMalformed, true},
		{effective.RevisionConsistencyReturnedIdentity, reportv1alpha1.RuntimeIssueRevisionIdentityMismatch, true},
		{effective.RevisionConsistencyDuplicateIdentity, reportv1alpha1.RuntimeIssueDuplicateRevision, true},
		{effective.RevisionConsistencyConflictingIdentity, reportv1alpha1.RuntimeIssueConflictingRevision, true},
		{effective.RevisionConsistencyDuplicateContentHash, reportv1alpha1.RuntimeIssueDuplicateRevisionContent, true},
		{effective.RevisionConsistencyShortHashCollision, reportv1alpha1.RuntimeIssueRevisionHashCollision, true},
		{effective.RevisionConsistencyCode("hostile"), "", false},
	}
	for _, test := range issueTests {
		got, ok := mapConsistencyIssue(test.input)
		assert.Equal(t, test.valid, ok)
		assert.Equal(t, test.want, got)
	}
}

func TestProjectConsistencyIssuesDeduplicatesCollapsedCodes(t *testing.T) {
	issues, ok := projectConsistencyIssues([]effective.RevisionConsistencyCode{
		effective.RevisionConsistencySourceNamespace,
		effective.RevisionConsistencySourceName,
		effective.RevisionConsistencySourceKind,
		effective.RevisionConsistencySourceName,
	}, true)

	assert.True(t, ok)
	assert.Equal(t, []reportv1alpha1.RuntimeIssueCode{
		reportv1alpha1.RuntimeIssueRevisionDisabled,
		reportv1alpha1.RuntimeIssueRevisionSourceMismatch,
	}, issues)
}

func TestLiveEvidenceShapeRequiresSnapshotAvailabilityAgreement(t *testing.T) {
	tests := []struct {
		name         string
		availability effective.LiveRuntimeAvailability
		livePresent  bool
		want         bool
	}{
		{name: "available snapshot", availability: effective.LiveRuntimeAvailable, livePresent: true, want: true},
		{name: "missing snapshot", availability: effective.LiveRuntimeNotFound, want: true},
		{name: "disabled snapshot", availability: effective.LiveRuntimeDisabled, want: true},
		{name: "unreadable snapshot", availability: effective.LiveRuntimeUnavailable, want: true},
		{name: "available without snapshot", availability: effective.LiveRuntimeAvailable},
		{name: "missing with snapshot", availability: effective.LiveRuntimeNotFound, livePresent: true},
		{name: "disabled with snapshot", availability: effective.LiveRuntimeDisabled, livePresent: true},
		{name: "unreadable with snapshot", availability: effective.LiveRuntimeUnavailable, livePresent: true},
		{name: "unknown availability", availability: effective.LiveRuntimeAvailability("hostile")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, validLiveEvidenceShape(test.availability, test.livePresent))
		})
	}
}

func TestOperationalErrorClassificationUsesOnlyBoundedReasons(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want reportv1alpha1.UnavailableReason
	}{
		{
			name: "not found",
			err:  apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "controllerrevisions"}, "secret-name"),
			want: reportv1alpha1.UnavailableNotFound,
		},
		{
			name: "forbidden",
			err: apierrors.NewForbidden(
				schema.GroupResource{Group: "apps", Resource: "controllerrevisions"},
				"secret-name", errors.New("secret-cause"),
			),
			want: reportv1alpha1.UnavailableForbidden,
		},
		{name: "unauthorized", err: apierrors.NewUnauthorized("secret-cause"), want: reportv1alpha1.UnavailableForbidden},
		{
			name: "unsupported api",
			err: &meta.NoKindMatchError{
				GroupKind: schema.GroupKind{Group: "secret-group", Kind: "secret-kind"},
			},
			want: reportv1alpha1.UnavailableUnsupportedAPI,
		},
		{name: "unreadable", err: errors.New("secret-cause"), want: reportv1alpha1.UnavailableUnreadable},
		{name: "empty error", want: reportv1alpha1.UnavailableUnreadable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, classifySourceError(test.err))
		})
	}
}

func TestLiveUnavailableReasonMappingIsBounded(t *testing.T) {
	tests := []struct {
		input effective.LiveRuntimeAvailability
		want  reportv1alpha1.UnavailableReason
	}{
		{effective.LiveRuntimeNotFound, reportv1alpha1.UnavailableNotFound},
		{effective.LiveRuntimeDisabled, reportv1alpha1.UnavailableDisabled},
		{effective.LiveRuntimeUnavailable, reportv1alpha1.UnavailableUnreadable},
		{effective.LiveRuntimeAvailable, reportv1alpha1.UnavailableUnreadable},
		{effective.LiveRuntimeAvailability("hostile"), reportv1alpha1.UnavailableUnreadable},
	}

	for _, test := range tests {
		assert.Equal(t, test.want, mapLiveUnavailableReason(test.input))
	}
	issue, reason, ok := projectSourceIssue(effective.RuntimeSourceIssue{
		Code: effective.RuntimeSourceIssueCode("hostile"), RevisionName: "secret-revision",
	})
	assert.False(t, ok)
	assert.Empty(t, issue)
	assert.Empty(t, reason)
}

func TestProjectionInvariantHelpersFailClosed(t *testing.T) {
	for _, value := range []effective.RuntimePinState{
		effective.RuntimePinStateRevisionMissing,
		effective.RuntimePinStateRevisionInvalid,
		effective.RuntimePinStateRevisionDisabled,
		effective.RuntimePinStateUnavailable,
	} {
		assert.True(t, failedRevisionPinState(value))
	}
	assert.False(t, failedRevisionPinState(effective.RuntimePinStateResolved))
	assert.False(t, failedRevisionPinState(effective.RuntimePinState("hostile")))

	for _, value := range []effective.RuntimeSourceIssueCode{
		effective.RuntimeSourceIssueLiveNotFound,
		effective.RuntimeSourceIssueLiveDisabled,
		effective.RuntimeSourceIssueLiveUnavailable,
	} {
		assert.True(t, liveSourceIssue(value))
	}
	assert.False(t, liveSourceIssue(effective.RuntimeSourceIssueRevisionNotFound))
	assert.False(t, liveSourceIssue(effective.RuntimeSourceIssueCode("hostile")))

	assert.True(t, validVerifiedShortHash(""))
	assert.True(t, validVerifiedShortHash("0123abcd"))
	assert.False(t, validVerifiedShortHash("0123abc"))
	assert.False(t, validVerifiedShortHash("0123abcD"))
	assert.False(t, validVerifiedShortHash("0123abcg"))

	validSource, ok := unavailableLiveSource(&effective.RuntimeState{
		RuntimeName: "runtime", DeclaredSourceKind: runtimeselector.KindServingRuntime,
		DeclaredSourceNamespace: "workloads",
	})
	assert.True(t, ok)
	assert.Equal(t, "runtime", validSource.Name)
	assert.Equal(t, "workloads", validSource.Namespace)
	assert.Equal(t, reportv1alpha1.UnavailableUnreadable, validSource.UnavailableReason)
	fallbackSource, ok := unavailableLiveSource(&effective.RuntimeState{
		RuntimeName: "runtime", RuntimeKind: runtimeselector.KindClusterServingRuntime,
	})
	assert.True(t, ok)
	assert.Equal(t, runtimeselector.KindClusterServingRuntime, fallbackSource.Kind)
	_, ok = unavailableLiveSource(&effective.RuntimeState{RuntimeName: "runtime", RuntimeKind: "hostile"})
	assert.False(t, ok)
	_, ok = unavailableLiveSource(&effective.RuntimeState{RuntimeKind: runtimeselector.KindClusterServingRuntime})
	assert.False(t, ok)

	left := &reportv1alpha1.RuntimeObjectReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: reportv1alpha1.RuntimeKindServingRuntime,
		Namespace: "workloads", Name: "runtime",
	}
	right := *left
	assert.True(t, sameRuntimeReference(nil, nil))
	assert.False(t, sameRuntimeReference(left, nil))
	assert.False(t, sameRuntimeReference(nil, left))
	assert.True(t, sameRuntimeReference(left, &right))
	right.Name = "other"
	assert.False(t, sameRuntimeReference(left, &right))
}

func TestProjectComponentsRejectsUnknownFieldsWithoutPartialOutput(t *testing.T) {
	valid := effective.EffectiveComponent{
		Type: v1beta1.EngineComponent, DeploymentMode: constants.OMENative,
		DeploymentModeSource: effective.DeploymentModeServiceSpec,
	}
	components, ok := projectComponents([]effective.EffectiveComponent{
		valid,
		{
			Type: v1beta1.DecoderComponent, DeploymentMode: constants.MultiNode,
			DeploymentModeSource: effective.DeploymentModeLeaderWorkerShape,
		},
		{
			Type: v1beta1.RouterComponent, DeploymentMode: constants.RawDeployment,
			DeploymentModeSource: effective.DeploymentModeComponentAnnotation,
		},
	})
	assert.True(t, ok)
	assert.Len(t, components, 3)

	tests := []effective.EffectiveComponent{
		{Type: v1beta1.ComponentType("hostile"), DeploymentMode: constants.OMENative, DeploymentModeSource: effective.DeploymentModeDefault},
		{Type: v1beta1.EngineComponent, DeploymentMode: constants.DeploymentModeType("hostile"), DeploymentModeSource: effective.DeploymentModeDefault},
		{Type: v1beta1.EngineComponent, DeploymentMode: constants.OMENative, DeploymentModeSource: effective.ComponentDeploymentModeSource("hostile")},
	}
	for _, test := range tests {
		components, ok := projectComponents([]effective.EffectiveComponent{valid, test})
		assert.False(t, ok)
		assert.Nil(t, components)
	}
}

func selectionMapping(
	name string,
	input effective.RuntimeSelectionSource,
	want reportv1alpha1.RuntimeSelectionSource,
) enumMappingCase {
	got, ok := mapSelectionSource(input)
	return enumMapping(name, string(got), ok, string(want))
}

func runtimeKindMapping(name, input string, want reportv1alpha1.RuntimeKind) enumMappingCase {
	got, ok := mapRuntimeKind(input)
	return enumMapping(name, string(got), ok, string(want))
}

func pinModeMapping(name string, input effective.RuntimePinMode, want reportv1alpha1.RuntimePinMode) enumMappingCase {
	got, ok := mapPinMode(input)
	return enumMapping(name, string(got), ok, string(want))
}

func pinStateMapping(name string, input effective.RuntimePinState, want reportv1alpha1.RuntimePinState) enumMappingCase {
	got, ok := mapPinState(input)
	return enumMapping(name, string(got), ok, string(want))
}

func freshnessMapping(name string, input effective.StatusFreshness, want reportv1alpha1.StatusFreshness) enumMappingCase {
	got, ok := mapFreshness(input)
	return enumMapping(name, string(got), ok, string(want))
}

func syncMapping(name string, input effective.SyncTokenState, want reportv1alpha1.RuntimeSyncState) enumMappingCase {
	got, ok := mapSyncState(input)
	return enumMapping(name, string(got), ok, string(want))
}

func driftStateMapping(name string, input effective.RuntimeDriftState, want reportv1alpha1.DriftConditionState) enumMappingCase {
	got, ok := mapDriftState(input)
	return enumMapping(name, string(got), ok, string(want))
}

func driftCauseMapping(name string, input effective.RuntimeDriftReason, want reportv1alpha1.RuntimeDriftCause) enumMappingCase {
	got, ok := mapDriftCause(input)
	if input == "" && want == "" {
		return enumMappingAllowEmpty(name, string(got), ok, string(want))
	}
	return enumMapping(name, string(got), ok, string(want))
}

func hashMapping(name string, input effective.RuntimeHashRelation, want reportv1alpha1.RuntimeHashRelation) enumMappingCase {
	got, ok := mapHashRelation(input)
	return enumMapping(name, string(got), ok, string(want))
}

func originMapping(name string, input effective.ConfigurationOrigin, want reportv1alpha1.ConfigurationOrigin) enumMappingCase {
	got, ok := mapConfigurationOrigin(input)
	return enumMapping(name, string(got), ok, string(want))
}

func componentMapping(name string, input v1beta1.ComponentType, want reportv1alpha1.RuntimeComponentType) enumMappingCase {
	got, ok := mapComponentType(input)
	return enumMapping(name, string(got), ok, string(want))
}

func modeMapping(name string, input constants.DeploymentModeType, want reportv1alpha1.DeploymentMode) enumMappingCase {
	got, ok := mapDeploymentMode(input)
	return enumMapping(name, string(got), ok, string(want))
}

func modeSourceMapping(name string, input effective.ComponentDeploymentModeSource, want reportv1alpha1.DeploymentModeSource) enumMappingCase {
	got, ok := mapDeploymentModeSource(input)
	return enumMapping(name, string(got), ok, string(want))
}

func inheritanceReasonMapping(name string, input effective.InheritanceUnavailableReason, want reportv1alpha1.UnavailableReason) enumMappingCase {
	got, ok := mapInheritanceUnavailableReason(input)
	return enumMapping(name, string(got), ok, string(want))
}

func enumMapping(name, got string, ok bool, want string) enumMappingCase {
	return enumMappingCase{name: name, got: got, ok: ok, want: want, valid: want != ""}
}

func enumMappingAllowEmpty(name, got string, ok bool, want string) enumMappingCase {
	result := enumMapping(name, got, ok, want)
	result.valid = true
	return result
}
