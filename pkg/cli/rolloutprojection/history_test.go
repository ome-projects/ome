package rolloutprojection_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func TestProjectHistoryReportsOnlyRetainedRunAndRevisionEvidence(t *testing.T) {
	isvc := validHistoryInferenceService(t)

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.RolloutHistoryStatePartial, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.RolloutHistoryRetentionBounded, got.Content.Summary.Completeness)
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.CurrentState)
	assert.Equal(t, reportv1alpha1.EvidenceReported, got.Content.Summary.CurrentEvidence)
	assert.Equal(t, reportv1alpha1.RolloutEpochUnverifiable, got.Content.Summary.CurrentEpoch)
	assert.Equal(t, 1, got.Content.Summary.ActiveRuns)
	assert.Equal(t, 1, got.Content.Summary.RetainedRuns)
	assert.Equal(t, 3, got.Content.Summary.Revisions)

	require.Len(t, got.Content.Runs, 2)
	assert.Equal(t, reportv1alpha1.RolloutHistoryRun{
		Slot: reportv1alpha1.RolloutHistoryRunActive, Outcome: reportv1alpha1.RolloutHistoryRunActiveState,
		RunID: "chat-0123456789ab", OpenedAt: historyTime(18, 0), PinnedAt: historyTime(18, 30),
		GroupCount: 1,
		Targets: []reportv1alpha1.RolloutHistoryTarget{{
			Component: reportv1alpha1.RuntimeComponentEngine, RevisionHash: "cccccccc",
			Evidence: reportv1alpha1.EvidenceReported,
		}},
	}, got.Content.Runs[0])
	assert.Equal(t, reportv1alpha1.RolloutHistoryRun{
		Slot: reportv1alpha1.RolloutHistoryRunLast, Outcome: reportv1alpha1.RolloutHistoryRunCompleted,
		OpenedAt: historyTime(16, 0), ClosedAt: historyTime(17, 0), GroupCount: 1,
		Targets: []reportv1alpha1.RolloutHistoryTarget{},
	}, got.Content.Runs[1])

	require.Len(t, got.Content.Provenance, 3)
	assert.Equal(t, reportv1alpha1.RolloutHistoryViewActive, got.Content.Provenance[0].View)
	assert.Equal(t, reportv1alpha1.RolloutHistoryViewLast, got.Content.Provenance[1].View)
	assert.Equal(t, reportv1alpha1.RolloutHistoryViewCurrent, got.Content.Provenance[2].View)
	for index, provenance := range got.Content.Provenance {
		assert.Equal(t, reportv1alpha1.RolloutPlanSourceInline, provenance.Source)
		assert.Equal(t, "rp1:cca45ec1fb0f", provenance.PortableDigest)
		assert.Nil(t, provenance.Policy)
		assert.Nil(t, provenance.ShadowedPolicy)
		if index == 1 {
			assert.Equal(t, reportv1alpha1.EvidenceReported, provenance.DigestEvidence)
		} else {
			assert.Equal(t, reportv1alpha1.EvidenceComputed, provenance.DigestEvidence)
		}
	}

	assert.Equal(t, []reportv1alpha1.RolloutHistoryRevision{
		{Component: reportv1alpha1.RuntimeComponentEngine, Role: reportv1alpha1.RolloutRevisionCurrent,
			RevisionHash: "aaaaaaaa", Phase: reportv1alpha1.RolloutPhaseUnknown},
		{Component: reportv1alpha1.RuntimeComponentEngine, Role: reportv1alpha1.RolloutHistoryRevisionReady,
			RevisionHash: "bbbbbbbb", Phase: reportv1alpha1.RolloutPhaseUnknown},
		{Component: reportv1alpha1.RuntimeComponentEngine, Role: reportv1alpha1.RolloutRevisionPrevious,
			RevisionHash: "dddddddd", Phase: reportv1alpha1.RolloutPhaseUnknown},
	}, got.Content.Revisions)
	assert.Equal(t, []reportv1alpha1.RolloutIssue{{Code: reportv1alpha1.RolloutIssueEpochUnverifiable}}, got.Content.StatusIssues)
	assert.Empty(t, got.Content.Issues)
	assert.Equal(t, []reportv1alpha1.RolloutWarning{{Code: reportv1alpha1.WarningPartialData}}, got.Warnings)
	require.Len(t, got.Sources, 1)
	assert.Equal(t, reportv1alpha1.RolloutHistorySourceReference{
		Kind: reportv1alpha1.RolloutSourceInferenceService, Namespace: "prod", Name: "chat",
		Generation: 7, Evidence: reportv1alpha1.EvidenceObserved,
		CollectedAt: fixedClock().Now(),
	}, got.Sources[0])
}

func TestProjectHistoryDistinguishesUnavailableAndEmptyRunWindows(t *testing.T) {
	t.Run("run status absent", func(t *testing.T) {
		isvc := baseInferenceService()
		got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutHistoryStateUnavailable, got.Content.Summary.State)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
			Code: reportv1alpha1.RolloutHistoryIssueRunStatusUnavailable,
		})
		assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningSourceUnavailable})
	})

	t.Run("run status absent but revision evidence survives", func(t *testing.T) {
		isvc := validHistoryInferenceService(t)
		isvc.Status.Rollout = nil

		got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutHistoryStatePartial, got.Content.Summary.State)
		assert.Len(t, got.Content.Revisions, 3)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
			Code: reportv1alpha1.RolloutHistoryIssueRunStatusUnavailable,
		})
		assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningSourceUnavailable})
		assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningPartialData})
	})

	t.Run("run status present and empty", func(t *testing.T) {
		isvc := baseInferenceService()
		isvc.Status.Rollout = &omev1beta1.RolloutStatus{}
		got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutHistoryStateEmpty, got.Content.Summary.State)
		assert.Empty(t, got.Content.Runs)
		assert.Empty(t, got.Content.Provenance)
		assert.Empty(t, got.Content.Revisions)
		assert.Empty(t, got.Content.Issues)
		assert.Empty(t, got.Warnings)
	})
}

func TestProjectHistoryNeverSerializesPinnedPlansOrArbitraryFields(t *testing.T) {
	isvc := validHistoryInferenceService(t)
	isvc.UID = types.UID("SECRET_HOSTILE_UID")
	isvc.ResourceVersion = "SECRET_RESOURCE_VERSION"
	isvc.Annotations = map[string]string{"ome.io/rollout-promote": "SECRET_TOKEN"}
	isvc.Status.RolloutCoordination.Groups[0].Message = "SECRET_STATUS_MESSAGE"
	isvc.Spec.Rollout.Groups[0].Canary = secretCanaryPlan()
	isvc.Spec.Rollout.Groups[0].BlueGreen = nil
	pinned := isvc.Spec.Rollout.Groups[0].DeepCopy()
	digest, err := rolloutpolicy.ProgressionDigest(pinned)
	require.NoError(t, err)
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group = *pinned
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest = digest
	isvc.Status.Rollout.LastRun.Groups[0].PortableDigest = digest
	isvc.Status.Rollout.Groups[0].ObservedDigest = digest

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)
	var outputs []string
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, got))
		outputs = append(outputs, output.String())
	}
	var wide bytes.Buffer
	require.NoError(t, got.WideTable().Write(&wide))
	outputs = append(outputs, wide.String())

	for _, output := range outputs {
		for _, secret := range []string{
			"SECRET_RESOURCE_VERSION", "SECRET_TOKEN", "SECRET_STATUS_MESSAGE",
			"SECRET_SERVER", "SECRET_QUERY", "SECRET_HEADER", "SECRET_AUTH",
			"SECRET_METRIC", "SECRET_TOKEN_KEY", "SECRET_HOSTILE_UID",
		} {
			assert.NotContains(t, output, secret)
		}
		assert.NotContains(t, output, "serverAddress")
		assert.NotContains(t, output, "metrics")
		assert.NotContains(t, output, "headers")
	}
}

func TestProjectHistoryDoesNotMutateInferenceService(t *testing.T) {
	isvc := validHistoryInferenceService(t)
	original := isvc.DeepCopy()

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)
	assert.True(t, apiequality.Semantic.DeepEqual(original, isvc))

	got.Content.Runs[0].Targets[0].RevisionHash = "dddddddd"
	got.Content.Runs[0].OpenedAt = nil
	got.Content.Provenance[0].PortableDigest = "rp1:ffffffffffff"
	got.Content.StatusIssues[0].Code = reportv1alpha1.RolloutIssueStatusMalformed
	assert.True(t, apiequality.Semantic.DeepEqual(original, isvc))
}

func TestProjectHistoryRejectsMalformedActiveRunEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.RolloutRun)
	}{
		{name: "run id belongs to another service", mutate: func(run *omev1beta1.RolloutRun) { run.RunID = "other-0123456789ab" }},
		{name: "run id hash is not lowercase hex", mutate: func(run *omev1beta1.RolloutRun) { run.RunID = "chat-0123456789AB" }},
		{name: "opened time absent", mutate: func(run *omev1beta1.RolloutRun) { run.OpenedAt = metav1.Time{} }},
		{name: "pinned time absent", mutate: func(run *omev1beta1.RolloutRun) { run.PinnedAt = metav1.Time{} }},
		{name: "pinned before opened", mutate: func(run *omev1beta1.RolloutRun) {
			run.PinnedAt = metav1.NewTime(run.OpenedAt.Add(-time.Second))
		}},
		{name: "plan has no groups", mutate: func(run *omev1beta1.RolloutRun) { run.Plan.Groups = nil }},
		{name: "plan exceeds retained bound", mutate: func(run *omev1beta1.RolloutRun) {
			group := run.Plan.Groups[0]
			run.Plan.Groups = []omev1beta1.RolloutRunGroup{group, group, group, group}
		}},
		{name: "digest does not prove pinned body", mutate: func(run *omev1beta1.RolloutRun) {
			run.Plan.Groups[0].PortableDigest = "rp1:aaaaaaaaaaaa"
		}},
		{name: "pinned group retains a ref", mutate: func(run *omev1beta1.RolloutRun) {
			run.Plan.Groups[0].Group.PolicyRef = &omev1beta1.RolloutPolicyRef{
				Name: "guarded", Progression: omev1beta1.RolloutProgressionBlueGreen,
			}
		}},
		{name: "policy source lacks identity", mutate: func(run *omev1beta1.RolloutRun) {
			run.Plan.Groups[0].Source = omev1beta1.RolloutPlanSourcePolicy
		}},
		{name: "policy source has negative generation", mutate: func(run *omev1beta1.RolloutRun) {
			run.Plan.Groups[0].Source = omev1beta1.RolloutPlanSourcePolicy
			run.Plan.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{
				Name: "guarded", Progression: omev1beta1.RolloutProgressionBlueGreen,
			}
			run.Plan.Groups[0].PolicyGeneration = -1
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validHistoryInferenceService(t)
			tt.mutate(isvc.Status.Rollout.ActiveRun)

			got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
			require.NoError(t, err)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueActiveRunMalformed,
				View: reportv1alpha1.RolloutHistoryViewActive,
			})
			for _, run := range got.Content.Runs {
				assert.NotEqual(t, reportv1alpha1.RolloutHistoryRunActive, run.Slot)
			}
			for _, provenance := range got.Content.Provenance {
				assert.NotEqual(t, reportv1alpha1.RolloutHistoryViewActive, provenance.View)
			}
		})
	}
}

func TestProjectHistoryRetainsRunWhenOptionalTargetIsUnavailable(t *testing.T) {
	isvc := validHistoryInferenceService(t)
	isvc.Status.Rollout.ActiveRun.TargetRevisions[0].Revision = ""

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Runs, 2)
	assert.Equal(t, reportv1alpha1.RolloutHistoryRunActive, got.Content.Runs[0].Slot)
	require.Len(t, got.Content.Runs[0].Targets, 1)
	assert.Equal(t, reportv1alpha1.RolloutHistoryTarget{
		Component: reportv1alpha1.RuntimeComponentEngine,
		Evidence:  reportv1alpha1.EvidenceUnavailable,
	}, got.Content.Runs[0].Targets[0])
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
		Code: reportv1alpha1.RolloutHistoryIssueActiveTargetUnavailable,
		View: reportv1alpha1.RolloutHistoryViewActive, Component: reportv1alpha1.RuntimeComponentEngine,
	})
	assert.Equal(t, reportv1alpha1.RolloutHistoryStatePartial, got.Content.Summary.State)
	assert.True(t, slices.ContainsFunc(got.Content.Provenance, func(value reportv1alpha1.RolloutHistoryProvenance) bool {
		return value.View == reportv1alpha1.RolloutHistoryViewActive
	}))
}

func TestProjectHistoryRetainsRepinnedRunWithOriginalTargets(t *testing.T) {
	isvc := validHistoryInferenceService(t)
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	repinned := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}
	digest, err := rolloutpolicy.ProgressionDigest(&repinned)
	require.NoError(t, err)
	isvc.Status.Rollout.ActiveRun.Plan.Groups = []omev1beta1.RolloutRunGroup{{
		Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: repinned,
	}}
	isvc.Status.Rollout.ActiveRun.PinnedAt = metav1.NewTime(*historyTime(18, 45))

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Runs, 2)
	assert.Equal(t, reportv1alpha1.RolloutHistoryRunActive, got.Content.Runs[0].Slot)
	assert.Equal(t, []reportv1alpha1.RolloutHistoryTarget{{
		Component:    reportv1alpha1.RuntimeComponentEngine,
		RevisionHash: "cccccccc", Evidence: reportv1alpha1.EvidenceReported,
	}}, got.Content.Runs[0].Targets)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
		Code: reportv1alpha1.RolloutHistoryIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutHistoryViewActive,
	})
	require.True(t, slices.ContainsFunc(got.Content.Provenance, func(value reportv1alpha1.RolloutHistoryProvenance) bool {
		return value.View == reportv1alpha1.RolloutHistoryViewActive
	}))
}

func TestProjectHistoryValidatesTargetsIndependently(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.RolloutRun)
	}{
		{name: "targets absent", mutate: func(run *omev1beta1.RolloutRun) { run.TargetRevisions = nil }},
		{name: "target duplicated", mutate: func(run *omev1beta1.RolloutRun) {
			run.TargetRevisions = append(run.TargetRevisions, run.TargetRevisions[0])
		}},
		{name: "target component malformed", mutate: func(run *omev1beta1.RolloutRun) {
			run.TargetRevisions[0].Component = "SECRET"
		}},
		{name: "target revision malformed", mutate: func(run *omev1beta1.RolloutRun) {
			run.TargetRevisions[0].Revision = "BBBBBBBB"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validHistoryInferenceService(t)
			tt.mutate(isvc.Status.Rollout.ActiveRun)

			got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
			require.NoError(t, err)
			assert.True(t, slices.ContainsFunc(got.Content.Runs, func(run reportv1alpha1.RolloutHistoryRun) bool {
				return run.Slot == reportv1alpha1.RolloutHistoryRunActive
			}))
			assert.True(t, slices.ContainsFunc(got.Content.Provenance, func(value reportv1alpha1.RolloutHistoryProvenance) bool {
				return value.View == reportv1alpha1.RolloutHistoryViewActive
			}))
			assert.True(t, slices.ContainsFunc(got.Content.Issues, func(issue reportv1alpha1.RolloutHistoryIssue) bool {
				return issue.Code == reportv1alpha1.RolloutHistoryIssueActiveTargetMalformed &&
					issue.View == reportv1alpha1.RolloutHistoryViewActive
			}), got.Content.Issues)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueActiveRunMalformed,
				View: reportv1alpha1.RolloutHistoryViewActive,
			})
		})
	}
}

func TestProjectHistoryRejectsCrossSlotChronology(t *testing.T) {
	isvc := validHistoryInferenceService(t)
	reversed := metav1.NewTime(*historyTime(19, 0))
	isvc.Status.Rollout.LastRun.ClosedAt = &reversed

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutHistoryStatePartial, got.Content.Summary.State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
		Code: reportv1alpha1.RolloutHistoryIssueRunChronologyMalformed,
		View: reportv1alpha1.RolloutHistoryViewLast,
	})
	assert.True(t, slices.ContainsFunc(got.Content.Runs, func(run reportv1alpha1.RolloutHistoryRun) bool {
		return run.Slot == reportv1alpha1.RolloutHistoryRunActive
	}))
	assert.False(t, slices.ContainsFunc(got.Content.Runs, func(run reportv1alpha1.RolloutHistoryRun) bool {
		return run.Slot == reportv1alpha1.RolloutHistoryRunLast
	}))
	assert.False(t, slices.ContainsFunc(got.Content.Provenance, func(value reportv1alpha1.RolloutHistoryProvenance) bool {
		return value.View == reportv1alpha1.RolloutHistoryViewLast
	}))
}

func TestProjectHistoryRejectsMalformedLastRunEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.RolloutRunRecord)
	}{
		{name: "unknown outcome", mutate: func(run *omev1beta1.RolloutRunRecord) { run.Outcome = "SECRET" }},
		{name: "opened time absent", mutate: func(run *omev1beta1.RolloutRunRecord) { run.OpenedAt = nil }},
		{name: "closed time absent", mutate: func(run *omev1beta1.RolloutRunRecord) { run.ClosedAt = nil }},
		{name: "closed before opened", mutate: func(run *omev1beta1.RolloutRunRecord) {
			value := metav1.NewTime(run.OpenedAt.Add(-time.Second))
			run.ClosedAt = &value
		}},
		{name: "provenance absent", mutate: func(run *omev1beta1.RolloutRunRecord) { run.Groups = nil }},
		{name: "provenance exceeds bound", mutate: func(run *omev1beta1.RolloutRunRecord) {
			group := run.Groups[0]
			run.Groups = []omev1beta1.RolloutRunProvenance{group, group, group, group}
		}},
		{name: "unknown source", mutate: func(run *omev1beta1.RolloutRunRecord) { run.Groups[0].Source = "SECRET" }},
		{name: "digest malformed", mutate: func(run *omev1beta1.RolloutRunRecord) { run.Groups[0].PortableDigest = "SECRET" }},
		{name: "inline source has policy", mutate: func(run *omev1beta1.RolloutRunRecord) {
			run.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{
				Name: "guarded", Progression: omev1beta1.RolloutProgressionCanary,
			}
		}},
		{name: "policy source lacks policy", mutate: func(run *omev1beta1.RolloutRunRecord) {
			run.Groups[0].Source = omev1beta1.RolloutPlanSourcePolicy
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validHistoryInferenceService(t)
			tt.mutate(isvc.Status.Rollout.LastRun)

			got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
			require.NoError(t, err)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
				Code: reportv1alpha1.RolloutHistoryIssueLastRunMalformed,
				View: reportv1alpha1.RolloutHistoryViewLast,
			})
			for _, run := range got.Content.Runs {
				assert.NotEqual(t, reportv1alpha1.RolloutHistoryRunLast, run.Slot)
			}
			for _, provenance := range got.Content.Provenance {
				assert.NotEqual(t, reportv1alpha1.RolloutHistoryViewLast, provenance.View)
			}
		})
	}
}

func TestProjectHistoryRejectsMissingAndMalformedCurrentResolution(t *testing.T) {
	tests := []struct {
		name     string
		wantCode reportv1alpha1.RolloutHistoryIssueCode
		mutate   func(*omev1beta1.RolloutStatus)
	}{
		{name: "resolution missing", wantCode: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMissing,
			mutate: func(status *omev1beta1.RolloutStatus) { status.Groups = nil }},
		{name: "unexpected extra resolution", wantCode: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			mutate: func(status *omev1beta1.RolloutStatus) {
				status.Groups = append(status.Groups, status.Groups[0])
			}},
		{name: "index outside declared groups", wantCode: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			mutate: func(status *omev1beta1.RolloutStatus) { status.Groups[0].Index = 1 }},
		{name: "source mismatches declaration", wantCode: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			mutate: func(status *omev1beta1.RolloutStatus) { status.Groups[0].Source = omev1beta1.RolloutPlanSourcePolicy }},
		{name: "digest malformed", wantCode: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			mutate: func(status *omev1beta1.RolloutStatus) { status.Groups[0].ObservedDigest = "SECRET" }},
		{name: "digest does not prove current body", wantCode: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			mutate: func(status *omev1beta1.RolloutStatus) { status.Groups[0].ObservedDigest = "rp1:aaaaaaaaaaaa" }},
		{name: "unexpected policy identity", wantCode: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			mutate: func(status *omev1beta1.RolloutStatus) {
				status.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{
					Name: "guarded", Progression: omev1beta1.RolloutProgressionBlueGreen,
				}
			}},
		{name: "unexpected shadow identity", wantCode: reportv1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
			mutate: func(status *omev1beta1.RolloutStatus) {
				status.Groups[0].ShadowedPolicyRef = &omev1beta1.ShadowedRolloutPolicyRef{Name: "guarded"}
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validHistoryInferenceService(t)
			tt.mutate(isvc.Status.Rollout)

			got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
			require.NoError(t, err)
			assert.True(t, slices.ContainsFunc(got.Content.Issues, func(issue reportv1alpha1.RolloutHistoryIssue) bool {
				return issue.Code == tt.wantCode && issue.View == reportv1alpha1.RolloutHistoryViewCurrent
			}), got.Content.Issues)
			for _, provenance := range got.Content.Provenance {
				assert.NotEqual(t, reportv1alpha1.RolloutHistoryViewCurrent, provenance.View)
			}
		})
	}
}

func TestProjectHistoryRetainsValidDigestlessCurrentResolution(t *testing.T) {
	tests := []struct {
		name   string
		group  omev1beta1.RolloutGroup
		source omev1beta1.RolloutPlanSource
		policy *omev1beta1.RolloutPolicyRef
	}{
		{name: "default progression", group: omev1beta1.RolloutGroup{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		}, source: omev1beta1.RolloutPlanSourceInline},
		{name: "unresolved policy", group: omev1beta1.RolloutGroup{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "guarded", Progression: omev1beta1.RolloutProgressionCanary,
			},
		}, source: omev1beta1.RolloutPlanSourcePolicy, policy: &omev1beta1.RolloutPolicyRef{
			Name: "guarded", Progression: omev1beta1.RolloutProgressionCanary,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := baseInferenceService()
			isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{tt.group}}
			isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{{
				Index: 0, Source: tt.source, PolicyRef: tt.policy,
			}}}

			got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
			require.NoError(t, err)
			require.Len(t, got.Content.Provenance, 1)
			assert.Equal(t, reportv1alpha1.RolloutHistoryViewCurrent, got.Content.Provenance[0].View)
			assert.Empty(t, got.Content.Provenance[0].PortableDigest)
			assert.Equal(t, reportv1alpha1.EvidenceUnavailable, got.Content.Provenance[0].DigestEvidence)
			assert.Empty(t, got.Content.Issues)
		})
	}
}

func TestProjectHistoryRetainsValidCurrentRowsWhenAnotherResolutionIsMissing(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
		{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}, BlueGreen: &omev1beta1.GroupBlueGreen{}},
		{Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent}, BlueGreen: &omev1beta1.GroupBlueGreen{}},
	}}
	digest, err := rolloutpolicy.ProgressionDigest(&isvc.Spec.Rollout.Groups[0])
	require.NoError(t, err)
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{{
		Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: digest,
	}}}

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Provenance, 1)
	assert.Equal(t, 0, got.Content.Provenance[0].Group)
	assert.Equal(t, reportv1alpha1.RolloutHistoryStatePartial, got.Content.Summary.State)
	require.Len(t, got.Content.Issues, 1)
	assert.Equal(t, reportv1alpha1.RolloutHistoryIssueCurrentResolutionMissing, got.Content.Issues[0].Code)
	require.NotNil(t, got.Content.Issues[0].Group)
	assert.Equal(t, 1, *got.Content.Issues[0].Group)
}

func TestProjectHistoryProjectsOnlyAllowlistedPolicyProvenance(t *testing.T) {
	isvc := validHistoryInferenceService(t)
	policy := &omev1beta1.RolloutPolicyRef{
		Name: "guarded", Progression: omev1beta1.RolloutProgressionBlueGreen,
	}
	isvc.Spec.Rollout.Groups[0].PolicyRef = policy.DeepCopy()
	isvc.Status.Rollout.Groups[0].PolicyRef = policy.DeepCopy()
	isvc.Status.Rollout.Groups[0].ShadowedPolicyRef = &omev1beta1.ShadowedRolloutPolicyRef{
		Name: "guarded", WouldPinDigest: "rp1:dddddddddddd",
	}
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Source = omev1beta1.RolloutPlanSourcePolicy
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef = policy.DeepCopy()
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyGeneration = 4
	isvc.Status.Rollout.LastRun.Groups[0].Source = omev1beta1.RolloutPlanSourcePolicy
	isvc.Status.Rollout.LastRun.Groups[0].PolicyRef = policy.DeepCopy()

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Provenance, 3)
	active, last, current := got.Content.Provenance[0], got.Content.Provenance[1], got.Content.Provenance[2]
	require.NotNil(t, active.Policy)
	assert.Equal(t, int64(4), active.Policy.Generation)
	assert.Equal(t, "guarded", active.Policy.Name)
	require.NotNil(t, last.Policy)
	assert.Zero(t, last.Policy.Generation)
	assert.Equal(t, "guarded", last.Policy.Name)
	assert.Nil(t, current.Policy)
	require.NotNil(t, current.ShadowedPolicy)
	assert.Equal(t, "guarded", current.ShadowedPolicy.Name)
	assert.Equal(t, "rp1:dddddddddddd", current.ShadowedPolicy.Digest)
}

func TestProjectHistoryRejectsUnboundSubjectIdentities(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
		want   error
	}{
		{name: "nil", mutate: nil, want: rolloutprojection.ErrNilInferenceService},
		{name: "name absent", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Name = "" }, want: rolloutprojection.ErrSubjectNameRequired},
		{name: "namespace absent", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Namespace = "" }, want: rolloutprojection.ErrNamespaceRequired},
		{name: "uid absent", mutate: func(isvc *omev1beta1.InferenceService) { isvc.UID = "" }, want: rolloutprojection.ErrSubjectUIDRequired},
		{name: "name malformed", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Name = "SECRET/NAME" }, want: rolloutprojection.ErrSubjectIdentityInvalid},
		{name: "namespace malformed", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Namespace = "SECRET/NS" }, want: rolloutprojection.ErrSubjectIdentityInvalid},
		{name: "uid malformed", mutate: func(isvc *omev1beta1.InferenceService) { isvc.UID = types.UID("SECRET/UID") }, want: rolloutprojection.ErrSubjectIdentityInvalid},
		{name: "generation absent", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Generation = 0 }, want: rolloutprojection.ErrSubjectIdentityInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var isvc *omev1beta1.InferenceService
			if tt.mutate != nil {
				isvc = validHistoryInferenceService(t)
				tt.mutate(isvc)
			}
			_, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
			require.ErrorIs(t, err, tt.want)
			if isvc != nil {
				assert.NotContains(t, err.Error(), "SECRET")
			}
		})
	}
}

func TestProjectHistoryIsDeterministicAcrossTargetOrderAndSamplesClockOnce(t *testing.T) {
	left := validHistoryInferenceService(t)
	left.Spec.Decoder = &omev1beta1.DecoderSpec{}
	left.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Components = []omev1beta1.ComponentType{
		omev1beta1.EngineComponent, omev1beta1.DecoderComponent,
	}
	left.Status.Rollout.ActiveRun.TargetRevisions = append(
		left.Status.Rollout.ActiveRun.TargetRevisions,
		omev1beta1.RolloutRunTarget{Component: omev1beta1.DecoderComponent, Revision: "dddddddd"},
	)
	right := left.DeepCopy()
	right.Status.Rollout.ActiveRun.TargetRevisions[0], right.Status.Rollout.ActiveRun.TargetRevisions[1] =
		right.Status.Rollout.ActiveRun.TargetRevisions[1], right.Status.Rollout.ActiveRun.TargetRevisions[0]
	calls := 0
	clock := reportv1alpha1.ClockFunc(func() time.Time {
		calls++
		return fixedClock().Now().Add(time.Duration(calls) * time.Hour)
	})

	leftReport, err := rolloutprojection.ProjectHistory(left, clock)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	rightReport, err := rolloutprojection.ProjectHistory(right, fixedClock())
	require.NoError(t, err)

	leftReport.CollectedAt = rightReport.CollectedAt
	for i := range leftReport.Sources {
		leftReport.Sources[i].CollectedAt = rightReport.Sources[i].CollectedAt
	}
	assert.Equal(t, leftReport, rightReport)
}

func validHistoryInferenceService(t *testing.T) *omev1beta1.InferenceService {
	t.Helper()
	isvc := baseInferenceService()
	group := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{group}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			LatestRolledoutRevision:   "chat-engine-rev-aaaaaaaa",
			LatestReadyRevision:       "chat-engine-rev-bbbbbbbb",
			PreviousRolledoutRevision: "chat-engine-rev-dddddddd",
		},
	}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{
		Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
			Name: "0", Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			Policy: omev1beta1.CoordinationPolicyBlueGreen, Phase: omev1beta1.CoordinationPhaseWaiting,
		}},
	}
	digest, err := rolloutpolicy.ProgressionDigest(&group)
	require.NoError(t, err)
	require.Equal(t, "rp1:cca45ec1fb0f", digest)
	opened := metav1.NewTime(*historyTime(18, 0))
	pinned := metav1.NewTime(*historyTime(18, 30))
	lastOpened := metav1.NewTime(*historyTime(16, 0))
	lastClosed := metav1.NewTime(*historyTime(17, 0))
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{
		ActiveRun: &omev1beta1.RolloutRun{
			RunID: "chat-0123456789ab", OpenedAt: opened, PinnedAt: pinned,
			TargetRevisions: []omev1beta1.RolloutRunTarget{{
				Component: omev1beta1.EngineComponent, Revision: "cccccccc",
			}},
			Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
				Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: group,
			}}},
		},
		LastRun: &omev1beta1.RolloutRunRecord{
			Outcome: omev1beta1.RolloutRunCompleted, OpenedAt: &lastOpened, ClosedAt: &lastClosed,
			Groups: []omev1beta1.RolloutRunProvenance{{
				Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest,
			}},
		},
		Groups: []omev1beta1.RolloutGroupResolution{{
			Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: digest,
		}},
	}
	return isvc
}

func historyTime(hour, minute int) *time.Time {
	value := time.Date(2026, time.September, 14, hour, minute, 0, 0, time.UTC)
	return &value
}

func secretCanaryPlan() *omev1beta1.GroupCanary {
	return &omev1beta1.GroupCanary{
		Prometheus: &omev1beta1.AnalysisPrometheus{
			ServerAddress: "https://SECRET_SERVER",
			AuthRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "SECRET_AUTH"},
				Key:                  "SECRET_TOKEN_KEY",
			},
			Headers: map[string]string{"X-Secret": "SECRET_HEADER"},
		},
		Steps: []omev1beta1.RolloutGroupStep{{
			Capacity: intstrFromPercent(100), Traffic: 100,
			Analysis: &omev1beta1.RolloutAnalysis{
				Interval: metav1.Duration{Duration: time.Minute}, FailureLimit: 1,
				Metrics: []omev1beta1.AnalysisMetric{{
					Name: "SECRET_METRIC", Query: "SECRET_QUERY",
					Operator: omev1beta1.ComparisonLTE, Threshold: "1",
				}},
			},
		}},
	}
}

func intstrFromPercent(value int) intstr.IntOrString {
	return intstr.FromString(fmt.Sprintf("%d%%", value))
}
