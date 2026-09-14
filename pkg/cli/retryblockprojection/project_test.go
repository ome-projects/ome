package retryblockprojection_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/retryblockprojection"
	"sigs.k8s.io/ome/pkg/constants"
)

var projectionClock = reportv1alpha1.ClockFunc(func() time.Time {
	return time.Date(2026, 9, 14, 23, 0, 0, 0, time.UTC)
})

func TestProjectReportsCurrentRetryBlocksAndReleaseEligibility(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	next := metav1.NewTime(time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC))
	first := metav1.NewTime(time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC))
	last := metav1.NewTime(time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC))
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{
		{TargetRevision: "chat-engine-cccccccc", State: omev1beta1.RetryBlockRetryInProgress, AttemptsStarted: 3, FirstFailureAt: &first, LastFailureAt: &last},
		{TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld, AttemptsStarted: 3, FirstFailureAt: &first, LastFailureAt: &last, Reason: "image pull failed"},
		{TargetRevision: "chat-engine-bbbbbbbb", State: omev1beta1.RetryBlockBackoff, AttemptsStarted: 2, NextRetryAt: &next, FirstFailureAt: &first, LastFailureAt: &last},
	}

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}, Pages: 1},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceRetryBlocksSummary{
		State: reportv1alpha1.InstanceRetryBlocksReported, Blocks: 3, Held: 1, Eligible: 1,
	}, got.Content.Summary)
	require.Len(t, got.Content.Blocks, 3)
	assert.Equal(t, "chat-engine-aaaaaaaa", got.Content.Blocks[0].TargetRevision)
	assert.True(t, got.Content.Blocks[0].ReleaseEligible)
	assert.Equal(t, reportv1alpha1.StatusFreshnessCurrent, got.Content.Blocks[0].Freshness)
	assert.False(t, got.Content.Blocks[1].ReleaseEligible)
	assert.Equal(t, time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC), *got.Content.Blocks[1].NextRetryAt)
	assert.False(t, got.Content.Blocks[2].ReleaseEligible)
	assert.Empty(t, got.Content.Issues)
	require.Len(t, got.Sources, 2)
	assert.Equal(t, []string{"InferenceReplica", "InferenceService"}, []string{got.Sources[0].Kind, got.Sources[1].Kind})
}

func TestProjectRejectsStaleEvidenceForRelease(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	ir.Status.ObservedGeneration = ir.Generation - 1
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld, AttemptsStarted: 3,
	}}

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceRetryBlocksPartial, got.Content.Summary.State)
	require.Len(t, got.Content.Blocks, 1)
	assert.Equal(t, reportv1alpha1.StatusFreshnessStale, got.Content.Blocks[0].Freshness)
	assert.False(t, got.Content.Blocks[0].ReleaseEligible)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueStatusStale)
	assert.Contains(t, got.Warnings, reportv1alpha1.Warning{Code: reportv1alpha1.WarningStaleEvidence})
}

func TestProjectClassifiesFreshnessFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(*omev1beta1.InferenceReplica)
		freshness  reportv1alpha1.StatusFreshness
		wantIssues []reportv1alpha1.InstanceRetryBlockIssueCode
	}{
		{
			name: "parent generation missing",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				delete(ir.Annotations, constants.InferenceReplicaParentGenerationAnnotationKey)
			},
			freshness:  reportv1alpha1.StatusFreshnessUnobserved,
			wantIssues: []reportv1alpha1.InstanceRetryBlockIssueCode{reportv1alpha1.InstanceRetryBlockIssueParentGenerationMissing},
		},
		{
			name: "parent generation invalid",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "03"
			},
			freshness:  reportv1alpha1.StatusFreshnessInvalid,
			wantIssues: []reportv1alpha1.InstanceRetryBlockIssueCode{reportv1alpha1.InstanceRetryBlockIssueParentGenerationInvalid},
		},
		{
			name: "parent generation stale",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "2"
			},
			freshness:  reportv1alpha1.StatusFreshnessStale,
			wantIssues: []reportv1alpha1.InstanceRetryBlockIssueCode{reportv1alpha1.InstanceRetryBlockIssueParentGenerationStale},
		},
		{
			name: "parent generation ahead",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "4"
			},
			freshness:  reportv1alpha1.StatusFreshnessInvalid,
			wantIssues: []reportv1alpha1.InstanceRetryBlockIssueCode{reportv1alpha1.InstanceRetryBlockIssueParentGenerationAhead},
		},
		{
			name: "status unobserved",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.ObservedGeneration = 0
			},
			freshness:  reportv1alpha1.StatusFreshnessUnobserved,
			wantIssues: []reportv1alpha1.InstanceRetryBlockIssueCode{reportv1alpha1.InstanceRetryBlockIssueStatusUnobserved},
		},
		{
			name: "status generation negative",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.ObservedGeneration = -1
			},
			freshness:  reportv1alpha1.StatusFreshnessInvalid,
			wantIssues: []reportv1alpha1.InstanceRetryBlockIssueCode{reportv1alpha1.InstanceRetryBlockIssueObservedGenerationInvalid},
		},
		{
			name: "status generation ahead",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Status.ObservedGeneration = ir.Generation + 1
			},
			freshness:  reportv1alpha1.StatusFreshnessInvalid,
			wantIssues: []reportv1alpha1.InstanceRetryBlockIssueCode{reportv1alpha1.InstanceRetryBlockIssueObservedGenerationInvalid},
		},
		{
			name: "status generation stale",
			mutate: func(ir *omev1beta1.InferenceReplica) {
				ir.Generation++
			},
			freshness:  reportv1alpha1.StatusFreshnessStale,
			wantIssues: []reportv1alpha1.InstanceRetryBlockIssueCode{reportv1alpha1.InstanceRetryBlockIssueStatusStale},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			isvc := projectionISVC()
			ir := projectionIR(isvc, omev1beta1.EngineComponent)
			ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
				TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
				AttemptsStarted: 3,
			}}
			test.mutate(&ir)

			got, err := retryblockprojection.Project(retryblockprojection.Input{
				InferenceService: isvc,
				Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
				Component:        omev1beta1.EngineComponent,
			}, projectionClock)

			require.NoError(t, err)
			require.Len(t, got.Content.Blocks, 1)
			assert.Equal(t, test.freshness, got.Content.Blocks[0].Freshness)
			assert.False(t, got.Content.Blocks[0].ReleaseEligible)
			for _, issue := range test.wantIssues {
				assertRetryIssue(t, got.Content.Issues, issue)
			}
		})
	}
}

func TestProjectDoesNotOfferReleaseFromIncompleteCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		truncated   bool
		unavailable reportv1alpha1.UnavailableReason
		wantIssue   reportv1alpha1.InstanceRetryBlockIssueCode
	}{
		{
			name: "truncated", truncated: true,
			wantIssue: reportv1alpha1.InstanceRetryBlockIssueCollectionTruncated,
		},
		{
			name: "later page unavailable", unavailable: reportv1alpha1.UnavailableForbidden,
			wantIssue: reportv1alpha1.InstanceRetryBlockIssueCollectionUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			isvc := projectionISVC()
			ir := projectionIR(isvc, omev1beta1.EngineComponent)
			ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
				TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
				AttemptsStarted: 3,
			}}

			got, err := retryblockprojection.Project(retryblockprojection.Input{
				InferenceService: isvc,
				Collection: instancecollection.Result{
					Items: []omev1beta1.InferenceReplica{ir}, Truncated: test.truncated,
				},
				CollectionUnavailable: test.unavailable,
				Component:             omev1beta1.EngineComponent,
			}, projectionClock)

			require.NoError(t, err)
			require.Len(t, got.Content.Blocks, 1)
			assert.False(t, got.Content.Blocks[0].ReleaseEligible)
			assert.Equal(t, 0, got.Content.Summary.Eligible)
			assert.Equal(t, reportv1alpha1.InstanceRetryBlocksPartial, got.Content.Summary.State)
			assertRetryIssue(t, got.Content.Issues, test.wantIssue)
		})
	}
}

func TestProjectOmitsComponentAbsenceClaimForTruncatedCollection(t *testing.T) {
	t.Parallel()

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: projectionISVC(),
		Collection:       instancecollection.Result{Truncated: true},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueCollectionTruncated)
	assert.Equal(t, 0, countRetryIssues(
		got.Content.Issues,
		reportv1alpha1.InstanceRetryBlockIssueComponentNotProjected,
	))
}

func TestProjectAcceptsControllerReachableRetryTimestamps(t *testing.T) {
	t.Parallel()

	first := metav1.NewTime(time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC))
	priorNext := metav1.NewTime(time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC))
	refreshedLast := metav1.NewTime(time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC))
	tests := []struct {
		name  string
		block omev1beta1.RetryBlock
	}{
		{
			name: "retry in progress retains prior backoff deadline",
			block: omev1beta1.RetryBlock{
				TargetRevision: "chat-engine-running", State: omev1beta1.RetryBlockRetryInProgress,
				AttemptsStarted: 2, NextRetryAt: &priorNext,
				FirstFailureAt: &first, LastFailureAt: &refreshedLast,
			},
		},
		{
			name: "same wave backoff refresh leaves elapsed deadline",
			block: omev1beta1.RetryBlock{
				TargetRevision: "chat-engine-backoff", State: omev1beta1.RetryBlockBackoff,
				AttemptsStarted: 2, NextRetryAt: &priorNext,
				FirstFailureAt: &first, LastFailureAt: &refreshedLast,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			isvc := projectionISVC()
			ir := projectionIR(isvc, omev1beta1.EngineComponent)
			ir.Status.RetryBlocks = []omev1beta1.RetryBlock{test.block}

			got, err := retryblockprojection.Project(retryblockprojection.Input{
				InferenceService: isvc,
				Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
				Component:        omev1beta1.EngineComponent,
			}, projectionClock)

			require.NoError(t, err)
			assert.Equal(t, 0, countRetryIssues(
				got.Content.Issues,
				reportv1alpha1.InstanceRetryBlockIssueTimestampsInvalid,
			))
		})
	}
}

func TestProjectStillRejectsImpossibleRetryTimestamps(t *testing.T) {
	t.Parallel()

	first := metav1.NewTime(time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC))
	last := metav1.NewTime(time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC))
	next := metav1.NewTime(time.Date(2026, 9, 14, 23, 0, 0, 0, time.UTC))
	earlyNext := metav1.NewTime(time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC))
	tests := []omev1beta1.RetryBlock{
		{
			TargetRevision: "chat-engine-backoff", State: omev1beta1.RetryBlockBackoff,
			AttemptsStarted: 1,
		},
		{
			TargetRevision: "chat-engine-held", State: omev1beta1.RetryBlockHeld,
			AttemptsStarted: 1, NextRetryAt: &next,
		},
		{
			TargetRevision: "chat-engine-order", State: omev1beta1.RetryBlockRetryInProgress,
			AttemptsStarted: 1, FirstFailureAt: &first, LastFailureAt: &last,
		},
		{
			TargetRevision: "chat-engine-before-first", State: omev1beta1.RetryBlockBackoff,
			AttemptsStarted: 1, NextRetryAt: &earlyNext,
			FirstFailureAt: &first, LastFailureAt: &first,
		},
	}
	for _, block := range tests {
		isvc := projectionISVC()
		ir := projectionIR(isvc, omev1beta1.EngineComponent)
		ir.Status.RetryBlocks = []omev1beta1.RetryBlock{block}

		got, err := retryblockprojection.Project(retryblockprojection.Input{
			InferenceService: isvc,
			Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
			Component:        omev1beta1.EngineComponent,
		}, projectionClock)

		require.NoError(t, err)
		assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueTimestampsInvalid)
	}
}

func TestProjectMarksMalformedBlocksAndNeverEchoesUnsafeTargets(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	next := metav1.NewTime(time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC))
	first := metav1.NewTime(time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC))
	last := metav1.NewTime(time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC))
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{
		{TargetRevision: "bad\n\u202eSECRET", State: omev1beta1.RetryBlockHeld, AttemptsStarted: 1},
		{TargetRevision: "chat-engine-state", State: "Future", AttemptsStarted: 1},
		{TargetRevision: "chat-engine-attempt", State: omev1beta1.RetryBlockHeld, AttemptsStarted: -1},
		{TargetRevision: "chat-engine-backoff", State: omev1beta1.RetryBlockBackoff, AttemptsStarted: 1},
		{TargetRevision: "chat-engine-time", State: omev1beta1.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &next, FirstFailureAt: &first, LastFailureAt: &last},
	}

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceRetryBlocksPartial, got.Content.Summary.State)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueTargetRevisionInvalid)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueStateInvalid)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueAttemptsInvalid)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueTimestampsInvalid)
	for _, block := range got.Content.Blocks {
		assert.False(t, block.ReleaseEligible)
		assert.NotContains(t, block.TargetRevision, "SECRET")
	}
}

func TestProjectRejectsDuplicateTargetBlocksAndComponents(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	first := projectionIR(isvc, omev1beta1.EngineComponent)
	first.Status.RetryBlocks = []omev1beta1.RetryBlock{
		{TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld, AttemptsStarted: 2},
		{TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld, AttemptsStarted: 2},
	}

	duplicateTarget, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{first}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)
	require.NoError(t, err)
	assertRetryIssue(t, duplicateTarget.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueDuplicateTarget)
	assert.Equal(t, 0, duplicateTarget.Content.Summary.Eligible)

	second := projectionIR(isvc, omev1beta1.EngineComponent)
	second.Name = "chat-engine-shadow"
	second.UID = "chat-engine-shadow-uid"
	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{second, first}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)
	require.NoError(t, err)
	assert.Empty(t, got.Content.Blocks)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueDuplicateComponent)
}

func TestProjectSurfacesCollectionAndNestedTruncation(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{
			Items: []omev1beta1.InferenceReplica{ir}, Truncated: true,
			RetryBlocksTruncated: []instancecollection.RetryBlocksTruncation{{
				Name: ir.Name, Component: ir.Spec.Component,
			}},
		},
		Component: omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceRetryBlocksPartial, got.Content.Summary.State)
	assert.True(t, got.Content.Summary.Truncated)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueCollectionTruncated)
	assertRetryIssue(t, got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueBlocksTruncated)
	assert.Contains(t, got.Warnings, reportv1alpha1.Warning{Code: reportv1alpha1.WarningTruncated})
}

func TestProjectSurfacesIdentityRejectionsWithoutEchoingUnsafeNames(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 2,
	}}
	rejections := []instancecollection.Rejection{
		{Name: "bad-metadata", Reason: instancecollection.RejectionMetadata},
		{Name: "bad-label", Reason: instancecollection.RejectionLabel},
		{Name: "bad-parent", Reason: instancecollection.RejectionParentReference},
		{Name: "bad-owner", Reason: instancecollection.RejectionOwnerReference},
		{Name: "INVALID", Reason: instancecollection.RejectionComponent},
	}

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection: instancecollection.Result{
			Items: []omev1beta1.InferenceReplica{ir}, Rejected: rejections,
		},
		Component: omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.InstanceRetryBlocksPartial, got.Content.Summary.State)
	assert.Equal(t, 5, countRetryIssues(got.Content.Issues, reportv1alpha1.InstanceRetryBlockIssueIdentityRejected))
	require.Len(t, got.Content.Blocks, 1)
	assert.False(t, got.Content.Blocks[0].ReleaseEligible,
		"a rejected object claiming this parent makes release authority ambiguous")
}

func TestProjectWithholdsReleaseForTerminatingInferenceReplica(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	deletingAt := metav1.NewTime(time.Date(2026, 9, 14, 22, 30, 0, 0, time.UTC))
	ir.DeletionTimestamp = &deletingAt
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 3,
	}}

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	require.Len(t, got.Content.Blocks, 1)
	assert.False(t, got.Content.Blocks[0].ReleaseEligible)
	assert.Equal(t, 0, got.Content.Summary.Eligible)
}

func TestProjectWithholdsReleaseForTerminatingInferenceService(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	deletingAt := metav1.NewTime(time.Date(2026, 9, 14, 22, 30, 0, 0, time.UTC))
	isvc.DeletionTimestamp = &deletingAt
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 3,
	}}

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	require.Len(t, got.Content.Blocks, 1)
	assert.False(t, got.Content.Blocks[0].ReleaseEligible)
	assert.Equal(t, 0, got.Content.Summary.Eligible)
}

func TestProjectSanitizesAndBoundsReasonsWithoutMutatingInput(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 3, Reason: "pull\n\u202e" + strings.Repeat("界", 300),
	}}
	original := ir.Status.RetryBlocks[0].Reason

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	require.Len(t, got.Content.Blocks, 1)
	assert.NotContains(t, got.Content.Blocks[0].Reason, "\n")
	assert.NotContains(t, got.Content.Blocks[0].Reason, "\u202e")
	assert.True(t, got.Content.Blocks[0].ReasonTruncated)
	assert.Equal(t, original, ir.Status.RetryBlocks[0].Reason)
}

func TestProjectRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	tests := []struct {
		name  string
		input retryblockprojection.Input
		want  error
	}{
		{name: "nil parent", input: retryblockprojection.Input{Component: omev1beta1.EngineComponent}, want: retryblockprojection.ErrInferenceServiceRequired},
		{name: "invalid parent", input: retryblockprojection.Input{InferenceService: &omev1beta1.InferenceService{}, Component: omev1beta1.EngineComponent}, want: retryblockprojection.ErrInferenceServiceIdentityInvalid},
		{name: "invalid component", input: retryblockprojection.Input{InferenceService: isvc, Component: "future"}, want: retryblockprojection.ErrComponentInvalid},
		{name: "invalid unavailable reason", input: retryblockprojection.Input{InferenceService: isvc, Component: omev1beta1.EngineComponent, CollectionUnavailable: "Future"}, want: retryblockprojection.ErrCollectionEvidenceInvalid},
		{name: "invalid rejection reason", input: retryblockprojection.Input{InferenceService: isvc, Component: omev1beta1.EngineComponent, Collection: instancecollection.Result{Rejected: []instancecollection.Rejection{{Name: "bad", Reason: "Future"}}}}, want: retryblockprojection.ErrCollectionEvidenceInvalid},
		{name: "unsafe rejection name", input: retryblockprojection.Input{InferenceService: isvc, Component: omev1beta1.EngineComponent, Collection: instancecollection.Result{Rejected: []instancecollection.Rejection{{Name: "bad\nSECRET", Reason: instancecollection.RejectionMetadata}}}}, want: retryblockprojection.ErrCollectionEvidenceInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := retryblockprojection.Project(test.input, projectionClock)
			require.ErrorIs(t, err, test.want)
		})
	}
}

func TestProjectRejectsInvalidRetryBlockTruncationEvidence(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 2,
	}}
	tests := []struct {
		name        string
		truncations []instancecollection.RetryBlocksTruncation
	}{
		{
			name: "unknown source",
			truncations: []instancecollection.RetryBlocksTruncation{{
				Name: "missing-engine", Component: omev1beta1.EngineComponent,
			}},
		},
		{
			name: "duplicate evidence",
			truncations: []instancecollection.RetryBlocksTruncation{
				{Name: ir.Name, Component: ir.Spec.Component},
				{Name: ir.Name, Component: ir.Spec.Component},
			},
		},
		{
			name: "truncated source still has blocks",
			truncations: []instancecollection.RetryBlocksTruncation{{
				Name: ir.Name, Component: ir.Spec.Component,
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := retryblockprojection.Project(retryblockprojection.Input{
				InferenceService: isvc,
				Collection: instancecollection.Result{
					Items:                []omev1beta1.InferenceReplica{ir},
					RetryBlocksTruncated: test.truncations,
				},
				Component: omev1beta1.EngineComponent,
			}, projectionClock)
			require.ErrorIs(t, err, retryblockprojection.ErrCollectionEvidenceInvalid)
		})
	}
}

func TestProjectOmitsOpaqueResourceVersionsFromSources(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	isvc.ResourceVersion = "123"
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	ir.ResourceVersion = "456"

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	require.Len(t, got.Sources, 2)
	assert.Empty(t, got.Sources[0].ResourceVersion)
	assert.Empty(t, got.Sources[1].ResourceVersion)
}

func TestProjectAcceptsExactOwnerEvidenceForLongParentName(t *testing.T) {
	t.Parallel()

	isvc := projectionISVC()
	isvc.Name = strings.Repeat("a", 64)
	ir := projectionIR(isvc, omev1beta1.EngineComponent)
	ir.Name = "engine"
	ir.UID = "engine-uid"
	delete(ir.Labels, constants.InferenceServiceLabel)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld, AttemptsStarted: 2,
	}}

	got, err := retryblockprojection.Project(retryblockprojection.Input{
		InferenceService: isvc,
		Collection:       instancecollection.Result{Items: []omev1beta1.InferenceReplica{ir}},
		Component:        omev1beta1.EngineComponent,
	}, projectionClock)

	require.NoError(t, err)
	require.Len(t, got.Content.Blocks, 1)
	assert.True(t, got.Content.Blocks[0].ReleaseEligible)
}

func projectionISVC() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("isvc-uid"), Generation: 3,
	}}
}

func projectionIR(
	isvc *omev1beta1.InferenceService,
	component omev1beta1.ComponentType,
) omev1beta1.InferenceReplica {
	controller := true
	name := isvc.Name + "-" + string(component)
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: isvc.Namespace, UID: types.UID(name + "-uid"), Generation: 2,
			Labels: map[string]string{
				constants.InferenceServiceLabel: isvc.Name,
				constants.OMEComponentLabel:     string(component),
			},
			Annotations: map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: "3",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: isvc.Name, UID: isvc.UID, Controller: &controller,
			}},
		},
		Spec: omev1beta1.InferenceReplicaSpec{
			ParentRef: omev1beta1.ParentReference{Name: isvc.Name}, Component: component,
		},
		Status: omev1beta1.InferenceReplicaStatus{ObservedGeneration: 2},
	}
}

func assertRetryIssue(
	t *testing.T,
	issues []reportv1alpha1.InstanceRetryBlockIssue,
	want reportv1alpha1.InstanceRetryBlockIssueCode,
) {
	t.Helper()
	for _, issue := range issues {
		if issue.Code == want {
			return
		}
	}
	assert.Fail(t, "retry-block issue not found", "want %q in %#v", want, issues)
}

func countRetryIssues(
	issues []reportv1alpha1.InstanceRetryBlockIssue,
	want reportv1alpha1.InstanceRetryBlockIssueCode,
) int {
	count := 0
	for _, issue := range issues {
		if issue.Code == want {
			count++
		}
	}
	return count
}
