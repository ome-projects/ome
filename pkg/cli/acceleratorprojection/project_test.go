package acceleratorprojection

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestProjectReportsCurrentSelectionAndRedactedReason(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{Policy: omev1beta1.CheapestPolicy}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{
			AcceleratorClass: "gpu-a", Reason: "selected by policy",
			ResourceRequests: map[string]string{"memory": "8Gi", "example.com/gpu": "1"},
		}},
	}
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)
	class, err := ObserveAcceleratorClass(&omev1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{
		Name: "gpu-a", UID: types.UID("class-uid"), ResourceVersion: "9", Generation: 2,
	}})
	require.NoError(t, err)

	got, err := Project(isvc, base, map[string]AcceleratorClassEvidence{"gpu-a": class}, fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainReported, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.StatusFreshnessCurrent, got.Content.Summary.StatusFreshness)
	require.Len(t, got.Content.Components, 1)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorIntentPolicy, component.Intent.State)
	assert.Equal(t, reportv1alpha1.AcceleratorPolicyCheapest, component.Intent.Policy)
	assert.Equal(t, reportv1alpha1.AcceleratorSelectorSourceService, component.Intent.PolicySource)
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionReported, component.Selection.State)
	assert.Equal(t, "gpu-a", component.Selection.Class)
	assert.Equal(t, reportv1alpha1.AcceleratorReasonReported, component.Selection.Reason.State)
	assert.Equal(t, "rs1:09963eb2e26c", component.Selection.Reason.Digest)
	assert.Equal(t, reportv1alpha1.AcceleratorClassObserved, component.Class.State)
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsAvailable, component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsReported, component.Requests.EffectiveState)
	assert.Equal(t, []reportv1alpha1.AcceleratorResourceRequest{
		{Name: "example.com/gpu", Quantity: "1"}, {Name: "memory", Quantity: "8Gi"},
	}, component.Requests.Effective)
	assert.Empty(t, component.Issues)
	require.Len(t, got.Sources, 3)
	assert.Equal(t, "AcceleratorClass", got.Sources[0].Kind)
	assert.Equal(t, "InferenceService", got.Sources[1].Kind)
	assert.Equal(t, "ServingRuntime", got.Sources[2].Kind)
	assert.Equal(t, int64(2), got.Sources[2].Generation)
	data, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "selected by policy")
	assert.NotContains(t, string(data), "resourceVersion")
	assert.NotContains(t, string(data), "uid")
}

func TestProjectSelectionWithoutRequestsPreservesClassAndReportsAbsence(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{
		Policy: omev1beta1.CheapestPolicy,
	}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			SelectedAccelerator: &omev1beta1.AcceleratorSelection{
				AcceleratorClass: "gpu-a",
			},
		},
	}
	class, err := ObserveAcceleratorClass(&omev1beta1.AcceleratorClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-a", UID: "class-uid", ResourceVersion: "9", Generation: 2,
		},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"gpu-a"}, ReportedAcceleratorClassNames(isvc))
	got, err := Project(
		isvc,
		acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true),
		map[string]AcceleratorClassEvidence{"gpu-a": class},
		fixedClock(),
	)

	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionReported, component.Selection.State)
	assert.Equal(t, "gpu-a", component.Selection.Class)
	assert.Equal(t, reportv1alpha1.AcceleratorClassObserved, component.Class.State)
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsAvailable, component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsNotReported, component.Requests.EffectiveState)
	assert.Equal(t, []reportv1alpha1.AcceleratorResourceRequest{{Name: "cpu", Quantity: "2"}},
		component.Requests.Base)
	assert.Empty(t, component.Requests.Effective)
	assert.Contains(t, component.Issues, reportv1alpha1.AcceleratorIssueRequestsNotReported)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainPartial, got.Content.Summary.State)
}

func TestProjectExplicitEmptyRequestsRemainReported(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{
		Policy: omev1beta1.CheapestPolicy,
	}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			SelectedAccelerator: &omev1beta1.AcceleratorSelection{
				AcceleratorClass: "gpu-a", ResourceRequests: map[string]string{},
			},
		},
	}
	class, err := ObserveAcceleratorClass(&omev1beta1.AcceleratorClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-a", UID: "class-uid", ResourceVersion: "9", Generation: 2,
		},
	})
	require.NoError(t, err)

	got, err := Project(
		isvc,
		acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true),
		map[string]AcceleratorClassEvidence{"gpu-a": class},
		fixedClock(),
	)

	require.NoError(t, err)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsAvailable, component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsReported, component.Requests.EffectiveState)
	assert.Empty(t, component.Requests.Effective)
	assert.NotContains(t, component.Issues, reportv1alpha1.AcceleratorIssueRequestsNotReported)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainReported, got.Content.Summary.State)
}

func TestProjectStaleStatusNeverClaimsSelectionOrRequests(t *testing.T) {
	const secret = "ghp_stale-secret"
	isvc := acceleratorProjectionISVC()
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{Policy: omev1beta1.BestFitPolicy}
	isvc.Status.ObservedGeneration = isvc.Generation - 1
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{
			AcceleratorClass: secret, Reason: secret,
			ResourceRequests: map[string]string{"example.com/gpu": secret},
		}},
	}
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)
	base.StatusFreshness = effective.StatusFreshnessStale

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionUnavailable, component.Selection.State)
	assert.Empty(t, component.Selection.Class)
	assert.Empty(t, component.Selection.Reason.Digest)
	assert.Empty(t, component.Requests.Effective)
	assert.Contains(t, component.Issues, reportv1alpha1.AcceleratorIssueStatusStale)
	data, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(data), secret)
	assert.Empty(t, ReportedAcceleratorClassNames(isvc))
}

func TestProjectUnobservedOrFutureStatusNeverClaimsSelection(t *testing.T) {
	const secret = "ghp_untrusted-status-secret"
	for _, tt := range []struct {
		name       string
		observed   int64
		freshness  effective.StatusFreshness
		wantReport reportv1alpha1.StatusFreshness
		wantIssue  reportv1alpha1.AcceleratorExplainIssueCode
	}{
		{name: "unobserved", observed: 0, freshness: effective.StatusFreshnessUnknown,
			wantReport: reportv1alpha1.StatusFreshnessUnobserved,
			wantIssue:  reportv1alpha1.AcceleratorIssueStatusUnobserved},
		{name: "future generation", observed: 5, freshness: effective.StatusFreshnessInconsistent,
			wantReport: reportv1alpha1.StatusFreshnessInvalid,
			wantIssue:  reportv1alpha1.AcceleratorIssueStatusInvalid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isvc := acceleratorProjectionISVC()
			isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{Policy: omev1beta1.BestFitPolicy}
			isvc.Status.ObservedGeneration = tt.observed
			isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{
					AcceleratorClass: "gpu-a", Reason: secret,
					ResourceRequests: map[string]string{"example.com/gpu": "1"},
				}},
			}
			base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)
			base.StatusFreshness = tt.freshness

			got, err := Project(isvc, base, nil, fixedClock())

			require.NoError(t, err)
			component := got.Content.Components[0]
			assert.Equal(t, tt.wantReport, got.Content.Summary.StatusFreshness)
			assert.Empty(t, component.Selection.Class)
			assert.Empty(t, component.Selection.Reason.Digest)
			assert.Empty(t, component.Requests.Effective)
			assert.Contains(t, component.Issues, tt.wantIssue)
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), secret)
			assert.Empty(t, ReportedAcceleratorClassNames(isvc))
		})
	}
}

func TestProjectConfiguredButUnreportedSelectionRemainsUnknown(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	className := "gpu-a"
	isvc.Spec.Engine.AcceleratorOverride = &omev1beta1.AcceleratorSelector{AcceleratorClass: &className}
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainPartial, got.Content.Summary.State)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorIntentClass, component.Intent.State)
	assert.Equal(t, "gpu-a", component.Intent.DeclaredClass)
	assert.Equal(t, reportv1alpha1.AcceleratorSelectorSourceComponent, component.Intent.ClassSource)
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionNotReported, component.Selection.State)
	assert.Empty(t, component.Selection.Class)
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsAvailable, component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsUnavailable, component.Requests.EffectiveState)
	assert.Equal(t, []reportv1alpha1.AcceleratorResourceRequest{{Name: "cpu", Quantity: "2"}}, component.Requests.Base)
	assert.Contains(t, component.Issues, reportv1alpha1.AcceleratorIssueSelectionNotReported)
}

func TestProjectUnavailableClassPreservesReportedSelection(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{Policy: omev1beta1.FirstAvailablePolicy}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{AcceleratorClass: "gpu-a"}},
	}
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)
	missing, err := UnavailableAcceleratorClass("gpu-a", reportv1alpha1.AcceleratorClassNotFound)
	require.NoError(t, err)

	got, err := Project(isvc, base, map[string]AcceleratorClassEvidence{"gpu-a": missing}, fixedClock())

	require.NoError(t, err)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionReported, component.Selection.State)
	assert.Equal(t, "gpu-a", component.Selection.Class)
	assert.Equal(t, reportv1alpha1.AcceleratorClassNotFound, component.Class.State)
	assert.Contains(t, component.Issues, reportv1alpha1.AcceleratorIssueClassNotFound)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainPartial, got.Content.Summary.State)
}

func TestProjectMapsEveryUnavailableClassState(t *testing.T) {
	tests := []struct {
		state reportv1alpha1.AcceleratorClassState
		issue reportv1alpha1.AcceleratorExplainIssueCode
	}{
		{state: reportv1alpha1.AcceleratorClassNotFound,
			issue: reportv1alpha1.AcceleratorIssueClassNotFound},
		{state: reportv1alpha1.AcceleratorClassForbidden,
			issue: reportv1alpha1.AcceleratorIssueClassForbidden},
		{state: reportv1alpha1.AcceleratorClassUnsupportedAPI,
			issue: reportv1alpha1.AcceleratorIssueClassUnsupportedAPI},
		{state: reportv1alpha1.AcceleratorClassUnreadable,
			issue: reportv1alpha1.AcceleratorIssueClassUnreadable},
		{state: reportv1alpha1.AcceleratorClassInvalid,
			issue: reportv1alpha1.AcceleratorIssueClassInvalid},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			isvc := acceleratorProjectionISVC()
			isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{
				Policy: omev1beta1.FirstAvailablePolicy,
			}
			isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {
					SelectedAccelerator: &omev1beta1.AcceleratorSelection{
						AcceleratorClass: "gpu-a", ResourceRequests: map[string]string{},
					},
				},
			}
			evidence, err := UnavailableAcceleratorClass("gpu-a", tt.state)
			require.NoError(t, err)

			got, err := Project(
				isvc,
				acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true),
				map[string]AcceleratorClassEvidence{"gpu-a": evidence},
				fixedClock(),
			)

			require.NoError(t, err)
			assert.Equal(t, tt.state, got.Content.Components[0].Class.State)
			assert.Contains(t, got.Content.Components[0].Issues, tt.issue)
			require.Len(t, got.Sources, 3)
			assert.Equal(t, reportv1alpha1.EvidenceUnavailable, got.Sources[0].Evidence)
		})
	}
}

func TestProjectAcceptsBoundControllerRevisionBase(t *testing.T) {
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, false)
	base.ActiveOrigin = effective.ConfigurationOriginControllerRevision
	base.ActiveConsistency = effective.RevisionConsistencyConsistent
	base.ActiveRevisionName = "revision-a"
	base.ActiveSourceKind = "ControllerRevision"
	base.ActiveSourceName = "revision-a"
	base.ActiveSourceNamespace = "ome"
	base.ActiveSourceUID = "revision-uid"
	base.ActiveSourceResourceVersion = "revision-rv"
	base.ActiveSourceGeneration = 0

	got, err := Project(acceleratorProjectionISVC(), base, nil, fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainNotConfigured,
		got.Content.Summary.State)
	assert.Contains(t, got.Sources, reportv1alpha1.AcceleratorSourceReference{
		Kind: "ControllerRevision", Namespace: "ome", Name: "revision-a",
		Evidence:    reportv1alpha1.EvidenceObserved,
		CollectedAt: fixedClock().Now(),
	})
}

func TestProjectRejectsMalformedCurrentStatusWithoutEchoingIt(t *testing.T) {
	const secret = "ghp_status-secret"
	isvc := acceleratorProjectionISVC()
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{Policy: omev1beta1.MostCapablePolicy}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{
			AcceleratorClass: secret, Reason: secret,
			ResourceRequests: map[string]string{"example.com/gpu": "-1"},
		}},
	}
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionInvalid, component.Selection.State)
	assert.Empty(t, component.Selection.Class)
	assert.Empty(t, component.Requests.Effective)
	assert.Contains(t, component.Issues, reportv1alpha1.AcceleratorIssueRequestsInvalid)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainInvalid, got.Content.Summary.State)
	data, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(data), secret)
	assert.Empty(t, ReportedAcceleratorClassNames(isvc))
}

func TestProjectRejectsHostileNodeSelectorAndOversizedRequests(t *testing.T) {
	const secret = "ghp_hostile-selector-secret"
	tests := []struct {
		name      string
		mutate    func(*omev1beta1.AcceleratorSelection)
		wantIssue reportv1alpha1.AcceleratorExplainIssueCode
	}{
		{name: "invalid node selector", mutate: func(selection *omev1beta1.AcceleratorSelection) {
			selection.NodeSelector = map[string]string{"example.com/gpu": secret + "!"}
		}, wantIssue: reportv1alpha1.AcceleratorIssueNodeSelectorInvalid},
		{name: "too many request keys", mutate: func(selection *omev1beta1.AcceleratorSelection) {
			selection.ResourceRequests = map[string]string{}
			for i := 0; i < 65; i++ {
				selection.ResourceRequests[fmt.Sprintf("example.com/gpu-%02d", i)] = "1"
			}
		}, wantIssue: reportv1alpha1.AcceleratorIssueRequestsInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := acceleratorProjectionISVC()
			isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{Policy: omev1beta1.CheapestPolicy}
			selection := &omev1beta1.AcceleratorSelection{
				AcceleratorClass: "gpu-a", Reason: secret,
				ResourceRequests: map[string]string{"example.com/gpu": "1"},
			}
			tt.mutate(selection)
			isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {SelectedAccelerator: selection},
			}

			got, err := Project(isvc, acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true), nil, fixedClock())

			require.NoError(t, err)
			component := got.Content.Components[0]
			assert.Equal(t, reportv1alpha1.AcceleratorSelectionInvalid, component.Selection.State)
			assert.Empty(t, component.Selection.Class)
			assert.Empty(t, component.Requests.Effective)
			assert.Contains(t, component.Issues, tt.wantIssue)
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), secret)
			assert.Empty(t, ReportedAcceleratorClassNames(isvc))
		})
	}
}

func TestProjectDetectsDeclaredAndReportedClassMismatch(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	declared := "gpu-a"
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{AcceleratorClass: &declared}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{AcceleratorClass: "gpu-b"}},
	}
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)
	observed, err := ObserveAcceleratorClass(&omev1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{
		Name: "gpu-b", UID: "uid", ResourceVersion: "1",
	}})
	require.NoError(t, err)

	got, err := Project(isvc, base, map[string]AcceleratorClassEvidence{"gpu-b": observed}, fixedClock())

	require.NoError(t, err)
	assert.Contains(t, got.Content.Components[0].Issues, reportv1alpha1.AcceleratorIssueReportedClassMismatch)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainPartial, got.Content.Summary.State)
}

func TestProjectNoIntentAndNoStatusIsNotConfigured(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, false)

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainNotConfigured, got.Content.Summary.State)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorIntentNotConfigured, component.Intent.State)
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionNotConfigured, component.Selection.State)
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsAvailable, component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsNotConfigured, component.Requests.EffectiveState)
	assert.Equal(t, []reportv1alpha1.AcceleratorResourceRequest{{Name: "cpu", Quantity: "2"}},
		component.Requests.Base)
	assert.Empty(t, component.Issues)
}

func TestProjectNoIntentPreservesUnavailableRuntimeEvidence(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	base := effective.AcceleratorBaseResolution{
		StatusFreshness: effective.StatusFreshnessCurrent,
		ActiveState:     effective.AcceleratorActiveUnavailable,
		Components: []effective.AcceleratorBaseComponent{{
			Type: omev1beta1.EngineComponent, State: effective.AcceleratorBaseUnavailable,
			Requests: []effective.AcceleratorBaseRequest{},
		}},
	}

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainPartial, got.Content.Summary.State)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsUnavailable, component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsNotConfigured, component.Requests.EffectiveState)
	assert.Contains(t, component.Issues,
		reportv1alpha1.AcceleratorIssueActiveConfigurationUnavailable)
}

func TestProjectNoIntentPreservesUnavailableComponentEvidence(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, false)
	base.Components[0].State = effective.AcceleratorBaseUnavailable
	base.Components[0].Requests = []effective.AcceleratorBaseRequest{}

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainPartial, got.Content.Summary.State)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsUnavailable, component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsNotConfigured, component.Requests.EffectiveState)
	assert.Contains(t, component.Issues, reportv1alpha1.AcceleratorIssueBaseRequestsUnavailable)
}

func TestProjectNoIntentKeepsIndependentBaseDiagnostics(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	isvc.Status.ObservedGeneration--
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, false)
	base.StatusFreshness = effective.StatusFreshnessStale
	base.Components[0].State = effective.AcceleratorBaseInvalid
	base.Components[0].Requests = []effective.AcceleratorBaseRequest{}

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AcceleratorExplainInvalid, got.Content.Summary.State)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionNotConfigured, component.Selection.State)
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsInvalid, component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsNotConfigured, component.Requests.EffectiveState)
	assert.Equal(t, []reportv1alpha1.AcceleratorExplainIssueCode{
		reportv1alpha1.AcceleratorIssueRequestsInvalid,
	}, component.Issues)
}

func TestProjectCurrentSelectionPreservesBaseRequestEvidence(t *testing.T) {
	for _, test := range []struct {
		name        string
		baseState   effective.AcceleratorBaseState
		wantBase    reportv1alpha1.AcceleratorBaseRequestState
		wantSummary reportv1alpha1.AcceleratorExplainState
		wantIssue   reportv1alpha1.AcceleratorExplainIssueCode
	}{
		{
			name: "unavailable", baseState: effective.AcceleratorBaseUnavailable,
			wantBase:    reportv1alpha1.AcceleratorBaseRequestsUnavailable,
			wantSummary: reportv1alpha1.AcceleratorExplainPartial,
			wantIssue:   reportv1alpha1.AcceleratorIssueBaseRequestsUnavailable,
		},
		{
			name: "invalid", baseState: effective.AcceleratorBaseInvalid,
			wantBase:    reportv1alpha1.AcceleratorBaseRequestsInvalid,
			wantSummary: reportv1alpha1.AcceleratorExplainInvalid,
			wantIssue:   reportv1alpha1.AcceleratorIssueRequestsInvalid,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			isvc := acceleratorProjectionISVC()
			isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{
				Policy: omev1beta1.CheapestPolicy,
			}
			isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {
					SelectedAccelerator: &omev1beta1.AcceleratorSelection{
						AcceleratorClass: "gpu-a",
						ResourceRequests: map[string]string{"example.com/gpu": "1"},
					},
				},
			}
			base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)
			base.Components[0].State = test.baseState
			base.Components[0].Requests = []effective.AcceleratorBaseRequest{}
			class, classErr := ObserveAcceleratorClass(&omev1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "gpu-a", UID: "class-uid", ResourceVersion: "9", Generation: 2,
				},
			})
			require.NoError(t, classErr)

			got, err := Project(isvc, base,
				map[string]AcceleratorClassEvidence{"gpu-a": class}, fixedClock())

			require.NoError(t, err)
			component := got.Content.Components[0]
			assert.Equal(t, test.wantBase, component.Requests.BaseState)
			assert.Equal(t, reportv1alpha1.AcceleratorRequestsReported,
				component.Requests.EffectiveState)
			assert.Equal(t, []reportv1alpha1.AcceleratorResourceRequest{{
				Name: "example.com/gpu", Quantity: "1",
			}}, component.Requests.Effective)
			assert.Contains(t, component.Issues, test.wantIssue)
			assert.Equal(t, test.wantSummary, got.Content.Summary.State)
		})
	}
}

func TestProjectUnavailableRuntimeSuppressesApparentlyCurrentStatus(t *testing.T) {
	const secret = "ghp_unverifiable-selection-secret"
	isvc := acceleratorProjectionISVC()
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{
			AcceleratorClass: "gpu-a", Reason: secret,
		}},
	}
	base := effective.AcceleratorBaseResolution{
		StatusFreshness: effective.StatusFreshnessCurrent,
		ActiveState:     effective.AcceleratorActiveUnavailable,
		Components: []effective.AcceleratorBaseComponent{{
			Type: omev1beta1.EngineComponent, State: effective.AcceleratorBaseUnavailable,
			Requests: []effective.AcceleratorBaseRequest{},
		}},
	}

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionUnavailable, component.Selection.State)
	assert.Empty(t, component.Selection.Class)
	assert.Contains(t, component.Issues, reportv1alpha1.AcceleratorIssueActiveConfigurationUnavailable)
	assert.Contains(t, component.Issues, reportv1alpha1.AcceleratorIssueSelectionUnexpected)
	encoded, marshalErr := json.Marshal(got)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), secret)
}

func TestProjectRejectsIncompleteClassJoinAndContradictoryBaseState(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{Policy: omev1beta1.CheapestPolicy}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{
			AcceleratorClass: "gpu-a",
		}},
	}

	_, err := Project(isvc, acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true), nil, fixedClock())
	require.ErrorIs(t, err, ErrInvalidEvidence)

	base := effective.AcceleratorBaseResolution{
		StatusFreshness: effective.StatusFreshnessCurrent,
		ActiveState:     effective.AcceleratorActiveUnavailable,
		Components: []effective.AcceleratorBaseComponent{{
			Type: omev1beta1.EngineComponent, State: effective.AcceleratorBaseAvailable,
			Requests: []effective.AcceleratorBaseRequest{{Name: "cpu", Quantity: "1"}},
		}},
	}
	_, err = Project(acceleratorProjectionISVC(), base, nil, fixedClock())
	require.ErrorIs(t, err, ErrInvalidEvidence)
}

func TestProjectMarksUnknownPolicyAndUnexpectedRouterStatusInvalid(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	isvc.Spec.AcceleratorSelector = &omev1beta1.AcceleratorSelector{Policy: "SecretPolicy"}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.RouterComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{AcceleratorClass: "gpu-router"}},
	}
	base := acceleratorBaseFixture(effective.AcceleratorActiveAvailable, true)

	got, err := Project(isvc, base, nil, fixedClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AcceleratorIntentInvalid, got.Content.Components[0].Intent.State)
	assert.Contains(t, got.Content.Components[0].Issues, reportv1alpha1.AcceleratorIssuePolicyInvalid)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.AcceleratorExplainIssue{
		Code:      reportv1alpha1.AcceleratorIssueUnexpectedComponentEvidence,
		Component: reportv1alpha1.RuntimeComponentRouter,
	})
	assert.Empty(t, ReportedAcceleratorClassNames(isvc))
}

func TestClassEvidenceConstructorsRejectUnboundOrContradictoryInputs(t *testing.T) {
	objects := []*omev1beta1.AcceleratorClass{
		nil,
		{ObjectMeta: metav1.ObjectMeta{Name: "gpu-a", ResourceVersion: "1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "gpu-a", UID: "uid"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "ghp_SECRET", UID: "uid", ResourceVersion: "1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "gpu-a", Namespace: "wrong", UID: "uid", ResourceVersion: "1"}},
	}
	for _, object := range objects {
		_, err := ObserveAcceleratorClass(object)
		require.ErrorIs(t, err, ErrInvalidClassEvidence)
	}
	_, err := UnavailableAcceleratorClass("gpu-a", reportv1alpha1.AcceleratorClassObserved)
	require.ErrorIs(t, err, ErrInvalidClassEvidence)
}

func TestReportedAcceleratorClassNamesAreCurrentValidDeduplicatedAndBounded(t *testing.T) {
	isvc := acceleratorProjectionISVC()
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.DecoderComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{AcceleratorClass: "gpu-a"}},
		omev1beta1.EngineComponent:  {SelectedAccelerator: &omev1beta1.AcceleratorSelection{AcceleratorClass: "gpu-a"}},
		omev1beta1.RouterComponent:  {SelectedAccelerator: &omev1beta1.AcceleratorSelection{AcceleratorClass: "gpu-router"}},
	}
	assert.Equal(t, []string{"gpu-a"}, ReportedAcceleratorClassNames(isvc))

	isvc.Status.ObservedGeneration--
	assert.Empty(t, ReportedAcceleratorClassNames(isvc))
	assert.Empty(t, ReportedAcceleratorClassNames(nil))
}

func acceleratorProjectionISVC() *omev1beta1.InferenceService {
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: "isvc-uid", ResourceVersion: "17", Generation: 4,
	}, Spec: omev1beta1.InferenceServiceSpec{Engine: &omev1beta1.EngineSpec{}}}
	isvc.Status.ObservedGeneration = 4
	return isvc
}

func acceleratorBaseFixture(state effective.AcceleratorActiveState, configured bool) effective.AcceleratorBaseResolution {
	return effective.AcceleratorBaseResolution{
		StatusFreshness: effective.StatusFreshnessCurrent, ActiveState: state,
		ActiveOrigin:      effective.ConfigurationOriginLiveRuntime,
		ActiveConsistency: effective.RevisionConsistencyUnknown,
		RuntimeName:       "runtime", RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod",
		ActiveSourceKind: "ServingRuntime", ActiveSourceName: "runtime",
		ActiveSourceNamespace: "prod", ActiveSourceUID: "runtime-uid",
		ActiveSourceResourceVersion: "runtime-rv", ActiveSourceGeneration: 2,
		RuntimeAcceleratorsConfigured: configured,
		Components: []effective.AcceleratorBaseComponent{{
			Type: omev1beta1.EngineComponent, State: effective.AcceleratorBaseAvailable,
			Requests: []effective.AcceleratorBaseRequest{{Name: "cpu", Quantity: "2"}},
		}},
	}
}

func fixedClock() reportv1alpha1.Clock {
	return reportv1alpha1.ClockFunc(func() time.Time {
		return time.Date(2026, 9, 14, 20, 30, 0, 0, time.UTC)
	})
}
