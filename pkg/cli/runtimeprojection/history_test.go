package runtimeprojection

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	appstyped "k8s.io/client-go/kubernetes/typed/apps/v1"
	k8stesting "k8s.io/client-go/testing"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

func TestProjectHistoryRejectsNilInputsWithFixedSentinel(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	state := &effective.RuntimeState{}

	_, err := ProjectHistory(nil, state, projectionTestClock)
	require.ErrorIs(t, err, ErrInvalidEvidence)
	assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())

	_, err = ProjectHistory(isvc, nil, projectionTestClock)
	require.ErrorIs(t, err, ErrInvalidEvidence)
	assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
}

func TestProjectHistoryMapsCompleteRetentionBoundedEvidence(t *testing.T) {
	isvc, state, activeRevision, olderRevision := resolveHistoryProjectionFixture(t)

	reportValue, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	activeCreatedAt := activeRevision.CreationTimestamp.Time.UTC()
	olderCreatedAt := olderRevision.CreationTimestamp.Time.UTC()
	assert.Equal(t, reportv1alpha1.RuntimeHistoryContent{
		Runtime: &reportv1alpha1.RuntimeObjectReference{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       reportv1alpha1.RuntimeKindClusterServingRuntime,
			Name:       "gpu-runtime",
			UID:        "runtime-uid",
			Generation: 5,
		},
		Observation:    reportv1alpha1.HistoryObservationStateComplete,
		Completeness:   reportv1alpha1.HistoryCompletenessRetentionBounded,
		RequestedPages: 1,
		ObservedPages:  1,
		Revisions: []reportv1alpha1.RuntimeRevisionEntry{
			{
				Revision: reportv1alpha1.RuntimeRevisionReference{
					Namespace: "ome-system", Name: activeRevision.Name, UID: "active-uid", CreatedAt: &activeCreatedAt,
				},
				Source: &reportv1alpha1.RuntimeObjectReference{
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Kind:       reportv1alpha1.RuntimeKindClusterServingRuntime,
					Name:       "gpu-runtime",
				},
				Hash: activeRevision.Labels[constants.RuntimeRevisionHashLabelKey],
				Roles: []reportv1alpha1.RuntimeRevisionRole{
					reportv1alpha1.RuntimeRevisionRoleActive,
					reportv1alpha1.RuntimeRevisionRoleReported,
					reportv1alpha1.RuntimeRevisionRoleHistory,
				},
				Consistency:    reportv1alpha1.RevisionConsistencyConsistent,
				RelationToLive: reportv1alpha1.RevisionRelationMatchesLive,
				Issues:         []reportv1alpha1.RuntimeIssueCode{},
			},
			{
				Revision: reportv1alpha1.RuntimeRevisionReference{
					Namespace: "ome-system", Name: olderRevision.Name, UID: "older-uid", CreatedAt: &olderCreatedAt,
				},
				Source: &reportv1alpha1.RuntimeObjectReference{
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Kind:       reportv1alpha1.RuntimeKindClusterServingRuntime,
					Name:       "gpu-runtime",
				},
				Hash:           olderRevision.Labels[constants.RuntimeRevisionHashLabelKey],
				Roles:          []reportv1alpha1.RuntimeRevisionRole{reportv1alpha1.RuntimeRevisionRoleHistory},
				Consistency:    reportv1alpha1.RevisionConsistencyConsistent,
				RelationToLive: reportv1alpha1.RevisionRelationDiffersFromLive,
				Issues:         []reportv1alpha1.RuntimeIssueCode{},
			},
		},
		Issues: []reportv1alpha1.RuntimeIssue{},
	}, reportValue.Content)
	assert.Equal(t, []reportv1alpha1.RuntimeSourceReference{
		{
			Kind: "ClusterServingRuntime", Name: "gpu-runtime", UID: "runtime-uid", Generation: 5,
			Evidence: reportv1alpha1.EvidenceObserved, CollectedAt: projectionTestClock.Now(),
		},
		{
			Kind: "ControllerRevision", Namespace: "ome-system", Name: activeRevision.Name, UID: "active-uid",
			Evidence: reportv1alpha1.EvidenceObserved, CollectedAt: projectionTestClock.Now(),
		},
		{
			Kind: "ControllerRevision", Namespace: "ome-system", Name: olderRevision.Name, UID: "older-uid",
			Evidence: reportv1alpha1.EvidenceObserved, CollectedAt: projectionTestClock.Now(),
		},
		{
			Kind: "InferenceService", Namespace: "workloads", Name: "chat", UID: "isvc-uid", Generation: 7,
			Evidence: reportv1alpha1.EvidenceObserved, CollectedAt: projectionTestClock.Now(),
		},
	}, reportValue.Sources)
	assert.Empty(t, reportValue.Warnings)
}

func TestProjectHistoryMapsCollectionStateWithoutInventingCompleteness(t *testing.T) {
	tests := []struct {
		name             string
		resolve          func(*testing.T) (*v1beta1.InferenceService, *effective.RuntimeState)
		wantObservation  reportv1alpha1.HistoryObservationState
		wantCompleteness reportv1alpha1.HistoryCompleteness
		wantRequested    int
		wantObserved     int
		wantIssues       []reportv1alpha1.RuntimeIssue
		wantWarnings     []reportv1alpha1.RuntimeWarning
	}{
		{
			name: "not requested", resolve: resolveLiveProjectionFixture,
			wantObservation:  reportv1alpha1.HistoryObservationStateNotRequested,
			wantCompleteness: reportv1alpha1.HistoryCompletenessNotRequested,
		},
		{
			name: "complete is retention bounded", resolve: resolveCompleteHistoryCollectionFixture,
			wantObservation:  reportv1alpha1.HistoryObservationStateComplete,
			wantCompleteness: reportv1alpha1.HistoryCompletenessRetentionBounded,
			wantRequested:    1, wantObserved: 1,
		},
		{
			name: "partial after one page", resolve: resolvePartialHistoryCollectionFixture,
			wantObservation:  reportv1alpha1.HistoryObservationStatePartial,
			wantCompleteness: reportv1alpha1.HistoryCompletenessIncomplete,
			wantRequested:    2, wantObserved: 1,
			wantIssues: []reportv1alpha1.RuntimeIssue{{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable}},
			wantWarnings: []reportv1alpha1.RuntimeWarning{
				{Code: reportv1alpha1.WarningPartialData},
				{Code: reportv1alpha1.WarningSourceUnavailable},
			},
		},
		{
			name: "partial after nonadvancing token", resolve: resolveNonadvancingHistoryCollectionFixture,
			wantObservation:  reportv1alpha1.HistoryObservationStatePartial,
			wantCompleteness: reportv1alpha1.HistoryCompletenessIncomplete,
			wantRequested:    2, wantObserved: 2,
			wantIssues: []reportv1alpha1.RuntimeIssue{{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable}},
			wantWarnings: []reportv1alpha1.RuntimeWarning{
				{Code: reportv1alpha1.WarningPartialData},
				{Code: reportv1alpha1.WarningSourceUnavailable},
			},
		},
		{
			name: "unavailable before first page", resolve: resolveUnavailableHistoryCollectionFixture,
			wantObservation:  reportv1alpha1.HistoryObservationStateUnavailable,
			wantCompleteness: reportv1alpha1.HistoryCompletenessIncomplete,
			wantRequested:    1,
			wantIssues:       []reportv1alpha1.RuntimeIssue{{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable}},
			wantWarnings: []reportv1alpha1.RuntimeWarning{
				{Code: reportv1alpha1.WarningPartialData},
				{Code: reportv1alpha1.WarningSourceUnavailable},
			},
		},
		{
			name: "truncated", resolve: resolveTruncatedHistoryCollectionFixture,
			wantObservation:  reportv1alpha1.HistoryObservationStatePartial,
			wantCompleteness: reportv1alpha1.HistoryCompletenessIncomplete,
			wantRequested:    2, wantObserved: 2,
			wantIssues: []reportv1alpha1.RuntimeIssue{{Code: reportv1alpha1.RuntimeIssueHistoryTruncated}},
			wantWarnings: []reportv1alpha1.RuntimeWarning{
				{Code: reportv1alpha1.WarningPartialData}, {Code: reportv1alpha1.WarningTruncated},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc, state := test.resolve(t)

			reportValue, err := ProjectHistory(isvc, state, projectionTestClock)
			require.NoError(t, err)
			if test.wantIssues == nil {
				test.wantIssues = []reportv1alpha1.RuntimeIssue{}
			}
			if test.wantWarnings == nil {
				test.wantWarnings = []reportv1alpha1.RuntimeWarning{}
			}
			assert.Equal(t, test.wantObservation, reportValue.Content.Observation)
			assert.Equal(t, test.wantCompleteness, reportValue.Content.Completeness)
			assert.Equal(t, test.wantRequested, reportValue.Content.RequestedPages)
			assert.Equal(t, test.wantObserved, reportValue.Content.ObservedPages)
			assert.Equal(t, test.wantIssues, reportValue.Content.Issues)
			assert.Equal(t, test.wantWarnings, reportValue.Warnings)
		})
	}
}

func resolveCompleteHistoryCollectionFixture(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState) {
	t.Helper()
	return resolveProjectionWithRevisionClient(
		t, k8sfake.NewSimpleClientset(), projectionRuntimeSpec("private.registry/live:secret"), "", nil,
	)
}

func resolvePartialHistoryCollectionFixture(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState) {
	t.Helper()
	clientset := k8sfake.NewSimpleClientset()
	calls := 0
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, &appsv1.ControllerRevisionList{ListMeta: metav1.ListMeta{Continue: "next"}}, nil
		}
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "apps", Resource: "controllerrevisions"},
			"gpu-runtime",
			errors.New("secret-list-denial"),
		)
	})
	return resolveProjectionWithRevisionClient(
		t, clientset, projectionRuntimeSpec("private.registry/live:secret"), "", nil,
	)
}

func resolveUnavailableHistoryCollectionFixture(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState) {
	t.Helper()
	clientset := k8sfake.NewSimpleClientset()
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "apps", Resource: "controllerrevisions"},
			"gpu-runtime",
			errors.New("secret-list-denial"),
		)
	})
	return resolveProjectionWithRevisionClient(
		t, clientset, projectionRuntimeSpec("private.registry/live:secret"), "", nil,
	)
}

func resolveNonadvancingHistoryCollectionFixture(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState) {
	t.Helper()
	clientset := k8sfake.NewSimpleClientset()
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &appsv1.ControllerRevisionList{ListMeta: metav1.ListMeta{Continue: "same"}}, nil
	})
	return resolveProjectionWithRevisionClient(
		t, clientset, projectionRuntimeSpec("private.registry/live:secret"), "", nil,
	)
}

func resolveTruncatedHistoryCollectionFixture(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState) {
	t.Helper()
	clientset := k8sfake.NewSimpleClientset()
	calls := 0
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		return true, &appsv1.ControllerRevisionList{
			ListMeta: metav1.ListMeta{Continue: fmt.Sprintf("page-%d", calls)},
		}, nil
	})
	return resolveProjectionWithRevisionClient(
		t, clientset, projectionRuntimeSpec("private.registry/live:secret"), "", nil,
	)
}

func TestProjectHistoryReportsRequestedHistoryWithoutRuntimeAsUnavailableNotFailed(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "workloads", UID: "isvc-uid", Generation: 7,
			ResourceVersion: "secret-isvc-resource-version",
		},
		Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
	isvc.Status.ObservedGeneration = 7
	resolver, err := effective.NewRuntimePinResolver(
		k8sfake.NewSimpleClientset().AppsV1(),
		effective.NewRuntimeResolver(ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()),
		"ome-system",
		paging.Limits{PageSize: 10, MaxItems: 20, MaxPages: 2, RequestTimeout: time.Second},
	)
	require.NoError(t, err)
	state, err := resolver.Resolve(t.Context(), isvc, effective.RuntimeResolveOptions{IncludeHistory: true})
	require.NoError(t, err)

	reportValue, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Nil(t, reportValue.Content.Runtime)
	assert.Equal(t, reportv1alpha1.HistoryObservationStateUnavailable, reportValue.Content.Observation)
	assert.Equal(t, reportv1alpha1.HistoryCompletenessIncomplete, reportValue.Content.Completeness)
	assert.Zero(t, reportValue.Content.RequestedPages)
	assert.Zero(t, reportValue.Content.ObservedPages)
	assert.Equal(t, []reportv1alpha1.RuntimeIssue{
		{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable},
		{Code: reportv1alpha1.RuntimeIssueInheritanceUnavailable},
	}, reportValue.Content.Issues)
	assert.Equal(t, []reportv1alpha1.RuntimeWarning{{Code: reportv1alpha1.WarningPartialData}}, reportValue.Warnings)
	assert.NotContains(t, reportValue.Warnings, reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningSourceUnavailable})
	assert.Equal(t, []reportv1alpha1.RuntimeSourceReference{{
		Kind: "InferenceService", Namespace: "workloads", Name: "chat", UID: "isvc-uid", Generation: 7,
		Evidence: reportv1alpha1.EvidenceObserved, CollectedAt: projectionTestClock.Now(),
	}}, reportValue.Sources)
}

func TestProjectionRejectsImpossibleCollectionCounters(t *testing.T) {
	isvc, baseState := resolveLiveProjectionFixture(t)
	tests := []struct {
		name   string
		mutate func(*effective.RuntimeState)
	}{
		{"negative requested", func(state *effective.RuntimeState) { state.HistoryRequestedPages = -1 }},
		{"negative observed", func(state *effective.RuntimeState) { state.HistoryObservedPages = -1 }},
		{"observed exceeds requested", func(state *effective.RuntimeState) {
			state.HistoryRequested = true
			state.HistoryRequestedPages = 1
			state.HistoryObservedPages = 2
		}},
		{"not requested with page", func(state *effective.RuntimeState) { state.HistoryRequestedPages = 1 }},
		{"not requested complete", func(state *effective.RuntimeState) { state.HistoryComplete = true }},
		{"not requested truncated", func(state *effective.RuntimeState) { state.HistoryTruncated = true }},
		{"complete and truncated", func(state *effective.RuntimeState) {
			state.HistoryRequested = true
			state.HistoryComplete = true
			state.HistoryTruncated = true
			state.HistoryRequestedPages = 1
			state.HistoryObservedPages = 1
		}},
		{"complete with no request", func(state *effective.RuntimeState) {
			state.HistoryRequested = true
			state.HistoryComplete = true
		}},
		{"complete page mismatch", func(state *effective.RuntimeState) {
			state.HistoryRequested = true
			state.HistoryComplete = true
			state.HistoryRequestedPages = 2
			state.HistoryObservedPages = 1
		}},
		{"incomplete failure without failed request", func(state *effective.RuntimeState) {
			state.HistoryRequested = true
			state.HistoryRequestedPages = 1
			state.HistoryObservedPages = 1
		}},
		{"truncated without page", func(state *effective.RuntimeState) {
			state.HistoryRequested = true
			state.HistoryTruncated = true
		}},
		{"truncated page mismatch", func(state *effective.RuntimeState) {
			state.HistoryRequested = true
			state.HistoryTruncated = true
			state.HistoryRequestedPages = 2
			state.HistoryObservedPages = 1
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := *baseState
			test.mutate(&state)

			_, err := ProjectEffective(isvc, &state, projectionTestClock)
			require.ErrorIs(t, err, ErrInvalidEvidence)
			assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
			_, err = ProjectHistory(isvc, &state, projectionTestClock)
			require.ErrorIs(t, err, ErrInvalidEvidence)
			assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
		})
	}
}

func TestProjectionAcceptsRecoveredExpiredHistorySnapshot(t *testing.T) {
	t.Parallel()

	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	stale := projectionRevision(
		t, "stale-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/stale:secret"),
	)
	freshA := projectionRevision(
		t, "fresh-a-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/fresh-a:secret"),
	)
	freshB := projectionRevision(
		t, "fresh-b-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/fresh-b:secret"),
	)
	clientset := k8sfake.NewSimpleClientset()
	calls := 0
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		switch calls {
		case 1:
			return true, &appsv1.ControllerRevisionList{
				Items:    []appsv1.ControllerRevision{*stale.DeepCopy()},
				ListMeta: metav1.ListMeta{Continue: "expired-token"},
			}, nil
		case 2:
			return true, nil, apierrors.NewResourceExpired("private expiration detail")
		case 3:
			return true, &appsv1.ControllerRevisionList{
				Items:    []appsv1.ControllerRevision{*freshA.DeepCopy()},
				ListMeta: metav1.ListMeta{Continue: "fresh-token"},
			}, nil
		case 4:
			return true, &appsv1.ControllerRevisionList{
				Items: []appsv1.ControllerRevision{*freshB.DeepCopy()},
			}, nil
		default:
			t.Fatalf("unexpected history list call %d", calls)
			return true, nil, nil
		}
	})
	isvc, state := resolveProjectionWithRevisionClientAndLimits(
		t, clientset, liveSpec, "", nil,
		paging.Limits{PageSize: 1, MaxItems: 8, MaxPages: 5, RequestTimeout: time.Second},
	)

	_, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)
	history, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.HistoryObservationStateComplete, history.Content.Observation)
	assert.Equal(t, reportv1alpha1.HistoryCompletenessRetentionBounded, history.Content.Completeness)
	assert.Equal(t, 4, history.Content.RequestedPages)
	assert.Equal(t, 2, history.Content.ObservedPages)
	assert.Empty(t, history.Content.Issues)
	assert.Empty(t, history.Warnings)
	require.Len(t, history.Content.Revisions, 2)
	for _, revision := range history.Content.Revisions {
		assert.NotEqual(t, stale.Name, revision.Revision.Name)
	}
}

func TestProjectionMapsSecondExpirationAfterRecoveredRestart(t *testing.T) {
	t.Parallel()

	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	stale := projectionRevision(
		t, "stale-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/stale:secret"),
	)
	fresh := projectionRevision(
		t, "fresh-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/fresh:secret"),
	)
	clientset := k8sfake.NewSimpleClientset()
	calls := 0
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		switch calls {
		case 1:
			return true, &appsv1.ControllerRevisionList{
				Items:    []appsv1.ControllerRevision{*stale.DeepCopy()},
				ListMeta: metav1.ListMeta{Continue: "first-private-token"},
			}, nil
		case 2:
			return true, nil, apierrors.NewResourceExpired("first private expiration detail")
		case 3:
			return true, &appsv1.ControllerRevisionList{
				Items:    []appsv1.ControllerRevision{*fresh.DeepCopy()},
				ListMeta: metav1.ListMeta{Continue: "second-private-token"},
			}, nil
		case 4:
			return true, nil, apierrors.NewResourceExpired("second private expiration detail")
		default:
			t.Fatalf("unexpected history list call %d", calls)
			return true, nil, nil
		}
	})
	isvc, state := resolveProjectionWithRevisionClientAndLimits(
		t, clientset, liveSpec, "", nil,
		paging.Limits{PageSize: 1, MaxItems: 8, MaxPages: 5, RequestTimeout: time.Second},
	)

	_, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)
	history, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.HistoryObservationStatePartial, history.Content.Observation)
	assert.Equal(t, reportv1alpha1.HistoryCompletenessIncomplete, history.Content.Completeness)
	assert.Equal(t, 4, history.Content.RequestedPages)
	assert.Equal(t, 1, history.Content.ObservedPages)
	assert.Equal(t, []reportv1alpha1.RuntimeIssue{
		{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable},
	}, history.Content.Issues)
	assert.Equal(t, []reportv1alpha1.RuntimeWarning{
		{Code: reportv1alpha1.WarningPartialData},
		{Code: reportv1alpha1.WarningSourceUnavailable},
	}, history.Warnings)
	require.Len(t, history.Content.Revisions, 1)
	assert.Equal(t, fresh.Name, history.Content.Revisions[0].Revision.Name)
	assert.NotEqual(t, stale.Name, history.Content.Revisions[0].Revision.Name)

	machine, err := json.Marshal(history)
	require.NoError(t, err)
	rendered := string(machine) + fmt.Sprint(history.Content.Table().Rows) +
		fmt.Sprint(history.Content.WideTable().Rows)
	for _, private := range []string{
		"first-private-token", "second-private-token",
		"first private expiration detail", "second private expiration detail",
	} {
		assert.NotContains(t, rendered, private)
	}
}

func TestProjectionMapsClearedExpirationWhenRecoveryBudgetIsTightened(t *testing.T) {
	t.Parallel()

	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	stale := projectionRevision(
		t, "stale-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/stale:secret"),
	)
	clientset := k8sfake.NewSimpleClientset()
	calls := 0
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, &appsv1.ControllerRevisionList{
				Items:    []appsv1.ControllerRevision{*stale.DeepCopy()},
				ListMeta: metav1.ListMeta{Continue: "private-expired-token"},
			}, nil
		}
		return true, nil, apierrors.NewResourceExpired("private expiration detail")
	})
	isvc, state := resolveProjectionWithRevisionClientAndLimits(
		t, clientset, liveSpec, "", nil,
		paging.Limits{
			PageSize: 1, MaxItems: 2, MaxPages: 2, MaxRequests: 2,
			RequestTimeout: time.Second,
		},
	)

	_, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)
	history, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Equal(t, 2, calls)
	assert.Equal(t, reportv1alpha1.HistoryObservationStateUnavailable, history.Content.Observation)
	assert.Equal(t, reportv1alpha1.HistoryCompletenessIncomplete, history.Content.Completeness)
	assert.Equal(t, 2, history.Content.RequestedPages)
	assert.Zero(t, history.Content.ObservedPages)
	assert.Empty(t, history.Content.Revisions, "the expired prefix must not reach projection")
	assert.Equal(t, []reportv1alpha1.RuntimeIssue{{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable}}, history.Content.Issues)
	assert.Equal(t, []reportv1alpha1.RuntimeWarning{
		{Code: reportv1alpha1.WarningPartialData},
		{Code: reportv1alpha1.WarningSourceUnavailable},
	}, history.Warnings)
	machine, marshalErr := json.Marshal(history)
	require.NoError(t, marshalErr)
	rendered := string(machine) + fmt.Sprint(history.Content.Table().Rows) +
		fmt.Sprint(history.Content.WideTable().Rows)
	assert.NotContains(t, rendered, stale.Name)
	assert.NotContains(t, rendered, "private-expired-token")
	assert.NotContains(t, rendered, "private expiration detail")
}

func TestProjectionRejectsCollectorCollectionContradictions(t *testing.T) {
	tests := []struct {
		name    string
		resolve func(*testing.T) (*v1beta1.InferenceService, *effective.RuntimeState)
		mutate  func(*effective.RuntimeState)
	}{
		{
			name: "history pages differ from requested pages", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) { state.HistoryPages++ },
		},
		{
			name: "successful recovered counters have no retained page", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) {
				state.HistoryPageLimit = 3
				state.HistoryPages = 3
				state.HistoryRequestedPages = 3
				state.HistoryObservedPages = 0
			},
		},
		{
			name: "history pages are negative", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) { state.HistoryPages = -1 },
		},
		{
			name: "observed pages exceed consistent snapshot limit", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) {
				state.HistoryPages = state.HistoryPageLimit + 1
				state.HistoryRequestedPages = state.HistoryPages
				state.HistoryObservedPages = state.HistoryPages
			},
		},
		{
			name: "requests exceed total request limit", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) {
				state.HistoryPages = state.HistoryRequestLimit + 1
				state.HistoryRequestedPages = state.HistoryPages
			},
		},
		{
			name: "request limit exceeds single restart bound", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) {
				state.HistoryRequestLimit = 2*state.HistoryPageLimit + 1
			},
		},
		{
			name: "collector page limit is absent", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) { state.HistoryPageLimit = 0 },
		},
		{
			name: "collector request limit is absent", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) { state.HistoryRequestLimit = 0 },
		},
		{
			name: "not requested carries a collector page", resolve: resolveLiveProjectionFixture,
			mutate: func(state *effective.RuntimeState) { state.HistoryPages = 1 },
		},
		{
			name: "failure counters have no list failure", resolve: resolveCompleteHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) {
				state.HistoryComplete = false
				state.HistoryPages = 2
				state.HistoryRequestedPages = 2
				state.HistoryObservedPages = 1
			},
		},
		{
			name: "list failure claims complete", resolve: resolveUnavailableHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) {
				state.HistoryComplete = true
				state.HistoryObservedPages = state.HistoryRequestedPages
			},
		},
		{
			name: "list failure claims truncation", resolve: resolveUnavailableHistoryCollectionFixture,
			mutate: func(state *effective.RuntimeState) {
				state.HistoryTruncated = true
				state.HistoryObservedPages = state.HistoryRequestedPages
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc, state := test.resolve(t)
			test.mutate(state)

			_, err := ProjectEffective(isvc, state, projectionTestClock)
			require.ErrorIs(t, err, ErrInvalidEvidence)
			assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
			_, err = ProjectHistory(isvc, state, projectionTestClock)
			require.ErrorIs(t, err, ErrInvalidEvidence)
			assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
		})
	}
}

func TestProjectHistoryCarriesGlobalBoundedDiagnostics(t *testing.T) {
	isvc, baseState := resolveLiveProjectionFixture(t)
	state := *baseState
	isvc = isvc.DeepCopy()
	isvc.Status.ObservedGeneration = 6
	state.ObservedGeneration = 6
	state.StatusFreshness = effective.StatusFreshnessStale
	state.DriftState = effective.RuntimeDriftStateMalformed
	setMalformedProjectionDrift(isvc)

	reportValue, err := ProjectHistory(isvc, &state, projectionTestClock)
	require.NoError(t, err)

	assert.Equal(t, []reportv1alpha1.RuntimeIssue{
		{Code: reportv1alpha1.RuntimeIssueReportedDriftConflict},
		{Code: reportv1alpha1.RuntimeIssueStatusStale},
	}, reportValue.Content.Issues)
	assert.Equal(t, []reportv1alpha1.RuntimeWarning{
		{Code: reportv1alpha1.WarningPartialData},
		{Code: reportv1alpha1.WarningStaleEvidence},
	}, reportValue.Warnings)
}

func TestProjectHistoryCarriesUnavailableLiveRuntimeSourceIdentity(t *testing.T) {
	isvc, state := resolveUnavailableLiveFixture(t, nil)

	reportValue, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Equal(t, &reportv1alpha1.RuntimeObjectReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: reportv1alpha1.RuntimeKindClusterServingRuntime,
		Name: "gpu-runtime",
	}, reportValue.Content.Runtime)
	assert.Contains(t, reportValue.Sources, reportv1alpha1.RuntimeSourceReference{
		Kind: "ClusterServingRuntime", Name: "gpu-runtime", Evidence: reportv1alpha1.EvidenceUnavailable,
		CollectedAt: projectionTestClock.Now(), UnavailableReason: reportv1alpha1.UnavailableNotFound,
	})
}

func TestProjectHistoryRejectsUnknownAggregateEnumsEvenWhenNotRendered(t *testing.T) {
	isvc, baseState := resolveLiveProjectionFixture(t)
	mutations := []func(*effective.RuntimeState){
		func(state *effective.RuntimeState) {
			state.SelectionSource = effective.RuntimeSelectionSource("secret")
		},
		func(state *effective.RuntimeState) { state.PinMode = effective.RuntimePinMode("secret") },
		func(state *effective.RuntimeState) { state.PinState = effective.RuntimePinState("secret") },
		func(state *effective.RuntimeState) { state.StatusFreshness = effective.StatusFreshness("secret") },
		func(state *effective.RuntimeState) { state.SyncTokenState = effective.SyncTokenState("secret") },
		func(state *effective.RuntimeState) { state.DriftState = effective.RuntimeDriftState("secret") },
		func(state *effective.RuntimeState) { state.DriftReason = effective.RuntimeDriftReason("secret") },
		func(state *effective.RuntimeState) { state.LiveToActive = effective.RuntimeHashRelation("secret") },
	}

	for index, mutate := range mutations {
		state := *baseState
		mutate(&state)

		_, err := ProjectHistory(isvc, &state, projectionTestClock)
		require.ErrorIs(t, err, ErrInvalidEvidence, "mutation %d", index)
		assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
		assert.NotContains(t, err.Error(), "secret")
	}
}

func TestProjectHistoryRejectsKnownButImpossiblePinConfiguration(t *testing.T) {
	isvc, state := resolveLiveProjectionFixture(t)
	state.PinState = effective.RuntimePinStateUnavailable

	_, err := ProjectHistory(isvc, state, projectionTestClock)
	require.ErrorIs(t, err, ErrInvalidEvidence)
	assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
}

func TestProjectHistoryRetainsExactEvidenceWhenHistoryListFails(t *testing.T) {
	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	activeRevision := projectionRevision(t, "active-uid", "gpu-runtime", liveSpec)
	activeRevision.CreationTimestamp = metav1.NewTime(time.Date(2026, time.August, 31, 19, 0, 0, 0, time.UTC))
	clientset := k8sfake.NewSimpleClientset(activeRevision.DeepCopy())
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "apps", Resource: "controllerrevisions"},
			"gpu-runtime",
			errors.New("secret-list-denial"),
		)
	})
	isvc, state := resolveProjectionWithRevisionClient(t, clientset, liveSpec, activeRevision.Name, nil)

	reportValue, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.HistoryObservationStateUnavailable, reportValue.Content.Observation)
	assert.Equal(t, 1, reportValue.Content.RequestedPages)
	assert.Zero(t, reportValue.Content.ObservedPages)
	require.Len(t, reportValue.Content.Revisions, 1, "the successful exact GET must survive LIST failure")
	assert.Equal(t, activeRevision.Name, reportValue.Content.Revisions[0].Revision.Name)
	assert.Equal(t, []reportv1alpha1.RuntimeIssue{{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable}}, reportValue.Content.Issues)
	assert.Equal(t, []reportv1alpha1.RuntimeWarning{
		{Code: reportv1alpha1.WarningPartialData},
		{Code: reportv1alpha1.WarningSourceUnavailable},
	}, reportValue.Warnings)
	assert.Contains(t, reportValue.Sources, reportv1alpha1.RuntimeSourceReference{
		Kind: "ControllerRevisionList", Namespace: "ome-system", Name: "gpu-runtime",
		Evidence: reportv1alpha1.EvidenceUnavailable, CollectedAt: projectionTestClock.Now(),
		UnavailableReason: reportv1alpha1.UnavailableForbidden,
	})
}

func TestProjectHistoryKeepsFailedExactKeySeparateFromSameNameListObject(t *testing.T) {
	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	historyRevision := projectionRevision(t, "history-uid", "gpu-runtime", liveSpec)
	historyRevision.Name = "expected-revision"
	clientset := k8sfake.NewSimpleClientset(historyRevision.DeepCopy())
	clientset.PrependReactor("get", "controllerrevisions", func(action k8stesting.Action) (bool, runtime.Object, error) {
		get := action.(k8stesting.GetAction)
		return true, nil, apierrors.NewNotFound(
			schema.GroupResource{Group: "apps", Resource: "controllerrevisions"}, get.GetName(),
		)
	})
	requested := historyRevision.Name
	isvc, state := resolveProjectionWithRevisionClient(t, clientset, liveSpec, "", &requested)

	reportValue, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	require.Len(t, reportValue.Content.Revisions, 1)
	assert.Equal(t, historyRevision.Name, reportValue.Content.Revisions[0].Revision.Name)
	assert.Equal(t, []reportv1alpha1.RuntimeRevisionRole{reportv1alpha1.RuntimeRevisionRoleHistory}, reportValue.Content.Revisions[0].Roles)
	assert.Contains(t, reportValue.Content.Issues, reportv1alpha1.RuntimeIssue{
		Code: reportv1alpha1.RuntimeIssueRevisionNotFound, Revision: requested,
	})
	var observed, unavailable int
	for _, source := range reportValue.Sources {
		if source.Kind != "ControllerRevision" || source.Name != requested {
			continue
		}
		switch source.Evidence {
		case reportv1alpha1.EvidenceObserved:
			observed++
		case reportv1alpha1.EvidenceUnavailable:
			unavailable++
			assert.Equal(t, reportv1alpha1.UnavailableNotFound, source.UnavailableReason)
		}
	}
	assert.Equal(t, 1, observed)
	assert.Equal(t, 1, unavailable)
}

func TestProjectionRejectsReturnedRevisionWithoutRequiredIdentity(t *testing.T) {
	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	tests := []struct {
		name   string
		mutate func(*appsv1.ControllerRevision)
	}{
		{name: "empty returned name", mutate: func(revision *appsv1.ControllerRevision) { revision.Name = "" }},
		{name: "empty returned namespace", mutate: func(revision *appsv1.ControllerRevision) { revision.Namespace = "" }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			returned := projectionRevision(t, "returned-uid", "gpu-runtime", liveSpec)
			test.mutate(returned)
			clientset := k8sfake.NewSimpleClientset()
			clientset.PrependReactor("get", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, returned.DeepCopy(), nil
			})
			clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, &appsv1.ControllerRevisionList{
					Items: []appsv1.ControllerRevision{*returned.DeepCopy()},
				}, nil
			})
			expected := "expected-revision"
			isvc, state := resolveProjectionWithRevisionClient(t, clientset, liveSpec, expected, nil)

			_, err := ProjectEffective(isvc, state, projectionTestClock)
			require.ErrorIs(t, err, ErrInvalidEvidence)
			assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
			_, err = ProjectHistory(isvc, state, projectionTestClock)
			require.ErrorIs(t, err, ErrInvalidEvidence)
			assert.Equal(t, ErrInvalidEvidence.Error(), err.Error())
		})
	}
}

func TestProjectionKeepsJoinedActiveHistoryAnomaliesInEffectiveReport(t *testing.T) {
	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	revision := projectionRevision(t, "revision-uid", "gpu-runtime", liveSpec)
	clientset := k8sfake.NewSimpleClientset(revision.DeepCopy())
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &appsv1.ControllerRevisionList{
			Items: []appsv1.ControllerRevision{*revision.DeepCopy(), *revision.DeepCopy()},
		}, nil
	})
	isvc, state := resolveProjectionWithRevisionClient(t, clientset, liveSpec, revision.Name, nil)

	effectiveReport, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)
	historyReport, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Equal(t, []reportv1alpha1.RuntimeIssue{
		{Code: reportv1alpha1.RuntimeIssueDuplicateRevision, Revision: revision.Name},
		{Code: reportv1alpha1.RuntimeIssueDuplicateRevisionContent, Revision: revision.Name},
	}, effectiveReport.Content.Issues, "anomalies joined to exact active evidence must remain visible")
	require.Len(t, historyReport.Content.Revisions, 2)
	for _, entry := range historyReport.Content.Revisions {
		assert.Equal(t, []reportv1alpha1.RuntimeIssueCode{
			reportv1alpha1.RuntimeIssueDuplicateRevision,
			reportv1alpha1.RuntimeIssueDuplicateRevisionContent,
		}, entry.Issues)
	}
	var revisionSources int
	for _, source := range historyReport.Sources {
		if source.Kind == "ControllerRevision" && source.Name == revision.Name {
			revisionSources++
		}
	}
	assert.Equal(t, 1, revisionSources, "identical source provenance must be deduplicated")
}

func TestProjectionKeepsJoinedActiveHistoryConflictsInEffectiveReport(t *testing.T) {
	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	exact := projectionRevision(t, "exact-revision-uid", "gpu-runtime", liveSpec)
	listed := exact.DeepCopy()
	listed.UID = "conflicting-list-uid"
	clientset := k8sfake.NewSimpleClientset(exact.DeepCopy())
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{*listed}}, nil
	})
	isvc, state := resolveProjectionWithRevisionClient(t, clientset, liveSpec, exact.Name, nil)

	effectiveReport, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Contains(t, effectiveReport.Content.Issues, reportv1alpha1.RuntimeIssue{
		Code: reportv1alpha1.RuntimeIssueConflictingRevision, Revision: exact.Name,
	})
	assert.Contains(t, effectiveReport.Content.Issues, reportv1alpha1.RuntimeIssue{
		Code: reportv1alpha1.RuntimeIssueDuplicateRevisionContent, Revision: exact.Name,
	})
	assert.NotContains(t, effectiveReport.Content.Issues, reportv1alpha1.RuntimeIssue{
		Code: reportv1alpha1.RuntimeIssueDuplicateRevision, Revision: exact.Name,
	})
}

func TestProjectionSuppressesExclusivelyHistoryAnomaliesFromEffectiveReport(t *testing.T) {
	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	active := projectionRevision(t, "active-revision-uid", "gpu-runtime", liveSpec)
	older := projectionRevision(
		t, "older-revision-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/older:secret"),
	)
	clientset := k8sfake.NewSimpleClientset(active.DeepCopy())
	clientset.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &appsv1.ControllerRevisionList{
			Items: []appsv1.ControllerRevision{*older.DeepCopy(), *older.DeepCopy()},
		}, nil
	})
	isvc, state := resolveProjectionWithRevisionClient(t, clientset, liveSpec, active.Name, nil)

	effectiveReport, err := ProjectEffective(isvc, state, projectionTestClock)
	require.NoError(t, err)
	historyReport, err := ProjectHistory(isvc, state, projectionTestClock)
	require.NoError(t, err)

	assert.Empty(t, effectiveReport.Content.Issues)
	var olderEntries []reportv1alpha1.RuntimeRevisionEntry
	for _, entry := range historyReport.Content.Revisions {
		if entry.Revision.Name != older.Name {
			continue
		}
		olderEntries = append(olderEntries, entry)
	}
	require.Len(t, olderEntries, 2)
	for _, entry := range olderEntries {
		assert.Equal(t, []reportv1alpha1.RuntimeIssueCode{
			reportv1alpha1.RuntimeIssueDuplicateRevision,
			reportv1alpha1.RuntimeIssueDuplicateRevisionContent,
		}, entry.Issues)
	}
}

func resolveProjectionWithRevisionClient(
	t *testing.T,
	clientset *k8sfake.Clientset,
	liveSpec *v1beta1.ServingRuntimeSpec,
	reportedRevision string,
	requestedRevision *string,
) (*v1beta1.InferenceService, *effective.RuntimeState) {
	return resolveProjectionWithRevisionClientAndLimits(
		t, clientset, liveSpec, reportedRevision, requestedRevision,
		paging.Limits{PageSize: 10, MaxItems: 20, MaxPages: 2, RequestTimeout: time.Second},
	)
}

func resolveProjectionWithRevisionClientAndLimits(
	t *testing.T,
	clientset *k8sfake.Clientset,
	liveSpec *v1beta1.ServingRuntimeSpec,
	reportedRevision string,
	requestedRevision *string,
	limits paging.Limits,
) (*v1beta1.InferenceService, *effective.RuntimeState) {
	return resolveProjectionWithRevisionGetterAndLimits(
		t, clientset.AppsV1(), liveSpec, reportedRevision, requestedRevision, limits,
	)
}

func resolveProjectionWithRevisionGetter(
	t *testing.T,
	revisions appstyped.ControllerRevisionsGetter,
	liveSpec *v1beta1.ServingRuntimeSpec,
	reportedRevision string,
	requestedRevision *string,
) (*v1beta1.InferenceService, *effective.RuntimeState) {
	return resolveProjectionWithRevisionGetterAndLimits(
		t, revisions, liveSpec, reportedRevision, requestedRevision,
		paging.Limits{PageSize: 10, MaxItems: 20, MaxPages: 2, RequestTimeout: time.Second},
	)
}

func resolveProjectionWithRevisionGetterAndLimits(
	t *testing.T,
	revisions appstyped.ControllerRevisionsGetter,
	liveSpec *v1beta1.ServingRuntimeSpec,
	reportedRevision string,
	requestedRevision *string,
	limits paging.Limits,
) (*v1beta1.InferenceService, *effective.RuntimeState) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-runtime", UID: "runtime-uid", Generation: 5},
		Spec:       *liveSpec.DeepCopy(),
	}
	liveClient := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject).Build()
	autoSync := false
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "workloads", UID: "isvc-uid", Generation: 7,
			ResourceVersion: "secret-isvc-resource-version",
		},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{
				Name: "gpu-runtime", AutoSync: &autoSync, Revision: requestedRevision,
			},
			Engine: &v1beta1.EngineSpec{},
		},
	}
	isvc.Status.ObservedGeneration = 7
	isvc.Status.PinnedRevisionName = reportedRevision
	resolver, err := effective.NewRuntimePinResolver(
		revisions, effective.NewRuntimeResolver(liveClient), "ome-system",
		limits,
	)
	require.NoError(t, err)
	state, err := resolver.Resolve(t.Context(), isvc, effective.RuntimeResolveOptions{IncludeHistory: true})
	require.NoError(t, err)
	return isvc, state
}

func resolveHistoryProjectionFixture(t *testing.T) (
	*v1beta1.InferenceService,
	*effective.RuntimeState,
	*appsv1.ControllerRevision,
	*appsv1.ControllerRevision,
) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	liveSpec := projectionRuntimeSpec("private.registry/live:secret")
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-runtime", UID: "runtime-uid", Generation: 5},
		Spec:       *liveSpec.DeepCopy(),
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject).Build()
	activeRevision := projectionRevision(t, "active-uid", "gpu-runtime", liveSpec)
	activeRevision.CreationTimestamp = metav1.NewTime(time.Date(2026, time.August, 31, 19, 0, 0, 0, time.UTC))
	olderRevision := projectionRevision(t, "older-uid", "gpu-runtime", projectionRuntimeSpec("private.registry/older:secret"))
	olderRevision.CreationTimestamp = metav1.NewTime(time.Date(2026, time.August, 30, 19, 0, 0, 0, time.UTC))
	autoSync := false
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "workloads", UID: "isvc-uid", Generation: 7,
			ResourceVersion: "secret-isvc-resource-version",
		},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: runtimeObject.Name, AutoSync: &autoSync},
			Engine:  &v1beta1.EngineSpec{},
		},
	}
	isvc.Status.ObservedGeneration = 7
	isvc.Status.PinnedRevisionName = activeRevision.Name
	resolver, err := effective.NewRuntimePinResolver(
		k8sfake.NewSimpleClientset(activeRevision.DeepCopy(), olderRevision.DeepCopy()).AppsV1(),
		effective.NewRuntimeResolver(client),
		"ome-system",
		paging.Limits{PageSize: 10, MaxItems: 20, MaxPages: 2, RequestTimeout: time.Second},
	)
	require.NoError(t, err)
	state, err := resolver.Resolve(t.Context(), isvc, effective.RuntimeResolveOptions{IncludeHistory: true})
	require.NoError(t, err)
	return isvc, state, activeRevision, olderRevision
}

func projectionRuntimeSpec(image string) *v1beta1.ServingRuntimeSpec {
	return &v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{
			Container: corev1.Container{
				Name: "runner", Image: image, Command: []string{"secret-command"},
				Env: []corev1.EnvVar{{Name: "SECRET_ENV", Value: "secret-value"}},
			},
		}},
	}
}

func projectionRevision(
	t *testing.T,
	uid, runtimeName string,
	spec *v1beta1.ServingRuntimeSpec,
) *appsv1.ControllerRevision {
	t.Helper()
	_, shortHash, err := runtimerevision.Hash(spec)
	require.NoError(t, err)
	raw, err := json.Marshal(spec)
	require.NoError(t, err)
	name := runtimerevision.Name(runtimerevision.KindClusterServingRuntime, "", runtimeName, shortHash)
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ome-system", UID: typesUID(uid),
			Labels: map[string]string{
				constants.RuntimeRevisionOfLabelKey:          runtimeName,
				constants.RuntimeRevisionOfKindLabelKey:      runtimeselector.KindClusterServingRuntime,
				constants.RuntimeRevisionOfNamespaceLabelKey: "",
				constants.RuntimeRevisionHashLabelKey:        shortHash,
			},
			Annotations: map[string]string{
				constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue,
			},
		},
		Data:     runtime.RawExtension{Raw: raw},
		Revision: 1,
	}
}

func typesUID(value string) types.UID { return types.UID(value) }
