package effective

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

func TestResolveHistoryUsesRuntimeNameOnlySelectorUnionsExactReadsAndSorts(t *testing.T) {
	newer := revisionFixture(t, runtimeselector.KindServingRuntime, "other-scope", "runtime", runtimeSpecFixture("newer"))
	newer.CreationTimestamp = metav1.NewTime(time.Unix(200, 0))
	older := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("older"))
	older.CreationTimestamp = metav1.NewTime(time.Unix(100, 0))
	autoSync := false
	var listOptions []metav1.ListOptions
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{
				get: func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
					return older.DeepCopy(), nil
				},
				list: func(_ context.Context, options metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
					listOptions = append(listOptions, options)
					switch options.Continue {
					case "":
						return &appsv1.ControllerRevisionList{
							Items:    []appsv1.ControllerRevision{*older.DeepCopy()},
							ListMeta: metav1.ListMeta{Continue: "next"},
						}, nil
					case "next":
						return &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{*newer.DeepCopy()}}, nil
					default:
						return nil, errors.New("unexpected continue token")
					}
				},
			}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			live := livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false)
			live.Runtime.spec = runtimeSpecFixture("live")
			return live, nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("runtime", &autoSync, "")
	isvc.Status.PinnedRevisionName = older.Name

	state, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{IncludeHistory: true})

	require.NoError(t, err)
	require.Len(t, listOptions, 2)
	for _, options := range listOptions {
		assert.Equal(t, constants.RuntimeRevisionOfLabelKey+"=runtime", options.LabelSelector)
		assert.Empty(t, options.FieldSelector)
	}
	assert.True(t, state.HistoryRequested)
	assert.True(t, state.HistoryComplete)
	assert.Equal(t, testPinLimits.MaxPages, state.HistoryPageLimit)
	assert.Equal(t, testPinLimits.MaxRequestAttempts(), state.HistoryRequestLimit)
	assert.Equal(t, 2, state.HistoryRequestedPages)
	assert.Equal(t, 2, state.HistoryObservedPages)
	observations := state.RevisionObservations()
	require.Len(t, observations, 2)
	assert.Equal(t, []string{newer.Name, older.Name}, []string{observations[0].Name, observations[1].Name})
	assert.Contains(t, observations[0].ConsistencyCodes(), RevisionConsistencySourceKind)
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleReported, RuntimeRevisionRoleActive, RuntimeRevisionRoleHistory}, observations[1].Roles())
	assert.Equal(t, RuntimeHashRelationDifferent, observations[0].RelationToLive)
}

func TestResolveHistoryRetainsSuccessfulPrefixAfterLaterFailure(t *testing.T) {
	revision := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("one"))
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "controllerrevisions"}, "", errors.New("denied"))
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{list: func(_ context.Context, options metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
				if options.Continue == "" {
					return &appsv1.ControllerRevisionList{
						Items:    []appsv1.ControllerRevision{*revision.DeepCopy()},
						ListMeta: metav1.ListMeta{Continue: "next"},
					}, nil
				}
				return nil, forbidden
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)

	state, err := resolver.Resolve(context.Background(), pinISVC("runtime", nil, ""), RuntimeResolveOptions{IncludeHistory: true})

	require.NoError(t, err)
	assert.False(t, state.HistoryComplete)
	assert.False(t, state.HistoryTruncated)
	assert.Equal(t, 2, state.HistoryRequestedPages)
	assert.Equal(t, 1, state.HistoryObservedPages)
	observations := state.RevisionObservations()
	require.Len(t, observations, 1)
	assert.Equal(t, revision.Name, observations[0].Name)
	issues := state.SourceIssues()
	require.Len(t, issues, 1)
	assert.Equal(t, "runtime revision history read failed", issues[0].Error())
	assert.ErrorIs(t, issues[0], forbidden)
}

func TestResolveHistoryRestartsExpiredContinuationWithoutStaleEvidence(t *testing.T) {
	stale := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("stale"))
	freshA := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("fresh-a"))
	freshB := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("fresh-b"))
	calls := 0
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{list: func(_ context.Context, options metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
				calls++
				switch calls {
				case 1:
					return &appsv1.ControllerRevisionList{
						Items:    []appsv1.ControllerRevision{*stale.DeepCopy()},
						ListMeta: metav1.ListMeta{Continue: "expired-token"},
					}, nil
				case 2:
					return nil, apierrors.NewResourceExpired("private detail")
				case 3:
					return &appsv1.ControllerRevisionList{
						Items:    []appsv1.ControllerRevision{*freshA.DeepCopy()},
						ListMeta: metav1.ListMeta{Continue: "fresh-token"},
					}, nil
				case 4:
					return &appsv1.ControllerRevisionList{
						Items: []appsv1.ControllerRevision{*freshB.DeepCopy()},
					}, nil
				default:
					t.Fatalf("unexpected request %d with token %q", calls, options.Continue)
					return nil, nil
				}
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)

	state, err := resolver.Resolve(
		context.Background(), pinISVC("runtime", nil, ""),
		RuntimeResolveOptions{IncludeHistory: true},
	)

	require.NoError(t, err)
	assert.True(t, state.HistoryComplete)
	assert.False(t, state.HistoryTruncated)
	assert.Equal(t, 4, state.HistoryPages)
	assert.Equal(t, 4, state.HistoryRequestedPages)
	assert.Equal(t, 2, state.HistoryObservedPages)
	assert.Empty(t, state.SourceIssues())
	observations := state.RevisionObservations()
	require.Len(t, observations, 2)
	for _, observation := range observations {
		assert.NotEqual(t, stale.Name, observation.Name)
	}
}

func TestResolveHistoryNilSuccessfulListResponseIsBounded(t *testing.T) {
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
				return nil, nil
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)

	state, err := resolver.Resolve(
		context.Background(), pinISVC("runtime", nil, ""), RuntimeResolveOptions{IncludeHistory: true},
	)

	require.NoError(t, err)
	assert.False(t, state.HistoryComplete)
	assert.Equal(t, 1, state.HistoryRequestedPages)
	assert.Zero(t, state.HistoryObservedPages)
	assert.Empty(t, state.RevisionObservations())
	assert.Equal(t, "ome", state.HistoryNamespace())
	issues := state.SourceIssues()
	require.Len(t, issues, 1)
	assert.Equal(t, RuntimeSourceIssueRevisionListFailed, issues[0].Code)
	assert.Equal(t, "runtime revision history read failed", issues[0].Error())
	assert.ErrorContains(t, errors.Unwrap(issues[0]), "empty response")
}

func TestResolveHistoryReportsTruncationAndNonadvancingTokens(t *testing.T) {
	revision := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("one"))
	t.Run("truncated", func(t *testing.T) {
		limits := testPinLimits
		limits.MaxItems = 1
		resolver, err := newRuntimePinResolver(
			func(string) revisionNamespace {
				return revisionNamespaceStub{list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
					return &appsv1.ControllerRevisionList{
						Items:    []appsv1.ControllerRevision{*revision.DeepCopy(), *revision.DeepCopy()},
						ListMeta: metav1.ListMeta{Continue: "more"},
					}, nil
				}}
			},
			liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
				return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
			}), "ome", limits,
		)
		require.NoError(t, err)
		state, err := resolver.Resolve(context.Background(), pinISVC("runtime", nil, ""), RuntimeResolveOptions{IncludeHistory: true})
		require.NoError(t, err)
		assert.True(t, state.HistoryTruncated)
		assert.False(t, state.HistoryComplete)
	})

	t.Run("nonadvancing", func(t *testing.T) {
		resolver, err := newRuntimePinResolver(
			func(string) revisionNamespace {
				return revisionNamespaceStub{list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
					return &appsv1.ControllerRevisionList{
						Items:    []appsv1.ControllerRevision{*revision.DeepCopy()},
						ListMeta: metav1.ListMeta{Continue: "same"},
					}, nil
				}}
			},
			liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
				return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
			}), "ome", testPinLimits,
		)
		require.NoError(t, err)
		state, err := resolver.Resolve(context.Background(), pinISVC("runtime", nil, ""), RuntimeResolveOptions{IncludeHistory: true})
		require.NoError(t, err)
		assert.False(t, state.HistoryComplete)
		assert.Equal(t, 2, state.HistoryRequestedPages)
		assert.Equal(t, 2, state.HistoryObservedPages)
		issues := state.SourceIssues()
		require.Len(t, issues, 1)
		assert.ErrorContains(t, errors.Unwrap(issues[0]), "continue token did not advance")
	})
}

func TestResolveHistoryKeepsExactEvidenceDistinctByRequestIdentity(t *testing.T) {
	autoSync := false
	requestedA := "requested-a"
	returnedB := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("b"))
	reportedB := returnedB.Name
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{
				get: func(_ context.Context, _ string, _ metav1.GetOptions) (*appsv1.ControllerRevision, error) {
					return returnedB.DeepCopy(), nil
				},
				list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
					return &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{*returnedB.DeepCopy()}}, nil
				},
			}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("runtime", &autoSync, requestedA)
	isvc.Status.PinnedRevisionName = reportedB

	state, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{IncludeHistory: true})

	require.NoError(t, err)
	observations := state.RevisionObservations()
	require.Len(t, observations, 2)
	byExpected := make(map[string]RuntimeRevisionObservation, len(observations))
	for _, observation := range observations {
		byExpected[observation.ExpectedName()] = observation
	}
	assert.Equal(t, returnedB.Name, byExpected[requestedA].ReturnedName())
	assert.Equal(t, "ome", byExpected[requestedA].ExpectedNamespace())
	assert.Equal(t, "ome", byExpected[requestedA].ReturnedNamespace())
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleRequested, RuntimeRevisionRoleActive}, byExpected[requestedA].Roles())
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleReported, RuntimeRevisionRoleHistory}, byExpected[reportedB].Roles())
	for _, observation := range observations {
		assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
		assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateContentHash)
	}
	active, err := state.RequireActive()
	require.NoError(t, err)
	assert.Equal(t, RevisionConsistencyInconsistent, active.Consistency)
	_, err = state.RequireConsistentActive()
	assert.ErrorIs(t, err, ErrActiveRuntimeInconsistent)
}

func TestResolveHistoryMergesOnlyFirstDuplicateIntoExactEvidence(t *testing.T) {
	autoSync := false
	revision := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("pin"))
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{
				get: func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
					return revision.DeepCopy(), nil
				},
				list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
					return &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{
						*revision.DeepCopy(), *revision.DeepCopy(),
					}}, nil
				},
			}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)

	state, err := resolver.Resolve(
		context.Background(), pinISVC("runtime", &autoSync, revision.Name),
		RuntimeResolveOptions{IncludeHistory: true},
	)

	require.NoError(t, err)
	observations := state.RevisionObservations()
	require.Len(t, observations, 2)
	var exact, duplicate *RuntimeRevisionObservation
	for i := range observations {
		if observations[i].ExpectedName() == revision.Name {
			exact = &observations[i]
		} else {
			duplicate = &observations[i]
		}
	}
	require.NotNil(t, exact)
	require.NotNil(t, duplicate)
	assert.Equal(t, []RuntimeRevisionRole{
		RuntimeRevisionRoleRequested, RuntimeRevisionRoleActive, RuntimeRevisionRoleHistory,
	}, exact.Roles())
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleHistory}, duplicate.Roles())
	for _, observation := range observations {
		assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
		assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateContentHash)
	}
	active, err := state.RequireActive()
	require.NoError(t, err)
	assert.Equal(t, RevisionConsistencyInconsistent, active.Consistency)
	_, err = state.RequireConsistentActive()
	assert.ErrorIs(t, err, ErrActiveRuntimeInconsistent)
}

func TestResolveHistoryDuplicateAbsorptionIsPermutationInvariant(t *testing.T) {
	autoSync := false
	exact := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("pin"))
	exact.UID = types.UID("revision-uid")
	exact.ResourceVersion = "1"
	createdAt := time.Unix(100, 123).UTC()
	exact.CreationTimestamp = metav1.NewTime(createdAt)
	rv2 := exact.DeepCopy()
	rv2.ResourceVersion = "2"
	rv2.Annotations[constants.RuntimeRevisionGCEligibleSinceKey] = "2026-01-01T00:00:00Z"
	rv2.OwnerReferences = []metav1.OwnerReference{{Name: "owner-two", UID: types.UID("owner-two")}}
	rv3 := exact.DeepCopy()
	rv3.ResourceVersion = "3"
	rv3.Annotations[constants.RuntimeRevisionGCEligibleSinceKey] = "2026-02-01T00:00:00Z"
	rv3.Finalizers = []string{"example.com/three"}

	resolve := func(t *testing.T, items ...*appsv1.ControllerRevision) *RuntimeState {
		t.Helper()
		listed := make([]appsv1.ControllerRevision, len(items))
		for i := range items {
			listed[i] = *items[i].DeepCopy()
		}
		resolver, err := newRuntimePinResolver(
			func(string) revisionNamespace {
				return revisionNamespaceStub{
					get: func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
						return exact.DeepCopy(), nil
					},
					list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
						return &appsv1.ControllerRevisionList{Items: listed}, nil
					},
				}
			},
			liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
				return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
			}),
			"ome", testPinLimits,
		)
		require.NoError(t, err)
		state, err := resolver.Resolve(
			context.Background(), pinISVC("runtime", &autoSync, exact.Name),
			RuntimeResolveOptions{IncludeHistory: true},
		)
		require.NoError(t, err)
		return state
	}

	type observationView struct {
		Name, Namespace, UID, ResourceVersion   string
		SourceName, SourceKind, SourceNamespace string
		ShortHash                               string
		CreationTimestamp                       metav1.Time
		Ordinal                                 int64
		Roles                                   []RuntimeRevisionRole
		Available                               bool
		Consistency                             RevisionConsistencyState
		Codes                                   []RevisionConsistencyCode
		RelationToLive                          RuntimeHashRelation
		Disabled                                bool
		ObjectReturned                          bool
		ExpectedName, ExpectedNamespace         string
		ReturnedName, ReturnedNamespace         string
	}
	type stateView struct {
		Generation, ObservedGeneration              int64
		RuntimeName, RuntimeKind, RuntimeNamespace  string
		DeclaredSourceKind, DeclaredSourceNamespace string
		SelectionSource                             RuntimeSelectionSource
		PinMode                                     RuntimePinMode
		PinState                                    RuntimePinState
		RequestedRevisionName                       string
		ReportedRevisionName                        string
		ActiveRevisionName                          string
		StatusFreshness                             StatusFreshness
		SyncTokenState                              SyncTokenState
		DriftState                                  RuntimeDriftState
		DriftReason                                 RuntimeDriftReason
		LiveToActive                                RuntimeHashRelation
		LiveShortHash                               string
		HistoryRequested                            bool
		HistoryPages                                int
		HistoryPageLimit                            int
		HistoryRequestLimit                         int
		HistoryRequestedPages                       int
		HistoryObservedPages                        int
		HistoryComplete                             bool
		HistoryTruncated                            bool
		LiveAvailability                            LiveRuntimeAvailability
		Live                                        *LiveConfiguration
		Active                                      *ActiveConfiguration
		ConsistentActiveError                       string
		SourceIssues                                []RuntimeSourceIssue
		Observations                                []observationView
	}
	view := func(t *testing.T, state *RuntimeState) stateView {
		t.Helper()
		active, err := state.RequireActive()
		require.NoError(t, err)
		_, consistentErr := state.RequireConsistentActive()
		require.Error(t, consistentErr)
		observations := state.RevisionObservations()
		result := stateView{
			Generation: state.Generation, ObservedGeneration: state.ObservedGeneration,
			RuntimeName: state.RuntimeName, RuntimeKind: state.RuntimeKind, RuntimeNamespace: state.RuntimeNamespace,
			DeclaredSourceKind: state.DeclaredSourceKind, DeclaredSourceNamespace: state.DeclaredSourceNamespace,
			SelectionSource: state.SelectionSource,
			PinMode:         state.PinMode, PinState: state.PinState,
			RequestedRevisionName: state.RequestedRevisionName,
			ReportedRevisionName:  state.ReportedRevisionName,
			ActiveRevisionName:    state.ActiveRevisionName,
			StatusFreshness:       state.StatusFreshness, SyncTokenState: state.SyncTokenState,
			DriftState: state.DriftState, DriftReason: state.DriftReason,
			LiveToActive: state.LiveToActive, LiveShortHash: state.LiveShortHash,
			HistoryRequested: state.HistoryRequested, HistoryPages: state.HistoryPages,
			HistoryPageLimit:      state.HistoryPageLimit,
			HistoryRequestLimit:   state.HistoryRequestLimit,
			HistoryRequestedPages: state.HistoryRequestedPages, HistoryObservedPages: state.HistoryObservedPages,
			HistoryComplete: state.HistoryComplete, HistoryTruncated: state.HistoryTruncated,
			LiveAvailability: state.LiveAvailability(), Live: state.LiveConfiguration(), Active: active,
			ConsistentActiveError: consistentErr.Error(), SourceIssues: state.SourceIssues(),
			Observations: make([]observationView, len(observations)),
		}
		for i := range observations {
			result.Observations[i] = observationView{
				Name: observations[i].Name, Namespace: observations[i].Namespace, UID: observations[i].UID,
				ResourceVersion: observations[i].ResourceVersion,
				SourceName:      observations[i].SourceName, SourceKind: observations[i].SourceKind,
				SourceNamespace: observations[i].SourceNamespace, ShortHash: observations[i].ShortHash,
				CreationTimestamp: observations[i].CreationTimestamp, Ordinal: observations[i].Ordinal,
				Roles: observations[i].Roles(), Available: observations[i].Available,
				Consistency: observations[i].Consistency, Codes: observations[i].ConsistencyCodes(),
				RelationToLive: observations[i].RelationToLive, Disabled: observations[i].Disabled,
				ObjectReturned: observations[i].ObjectReturned(),
				ExpectedName:   observations[i].ExpectedName(), ExpectedNamespace: observations[i].ExpectedNamespace(),
				ReturnedName: observations[i].ReturnedName(), ReturnedNamespace: observations[i].ReturnedNamespace(),
			}
		}
		return result
	}

	assertPermutationInvariant := func(t *testing.T, first, second *appsv1.ControllerRevision) []*RuntimeState {
		t.Helper()
		forward := resolve(t, first, second)
		reverse := resolve(t, second, first)

		assert.Equal(t, view(t, forward), view(t, reverse))
		for _, state := range []*RuntimeState{forward, reverse} {
			require.Len(t, state.RevisionObservations(), 2)
			_, err := state.RequireConsistentActive()
			assert.ErrorIs(t, err, ErrActiveRuntimeInconsistent)
			for _, observation := range state.RevisionObservations() {
				assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
				assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateContentHash)
			}
		}
		return []*RuntimeState{forward, reverse}
	}

	t.Run("resource version tie break", func(t *testing.T) {
		assertPermutationInvariant(t, rv2, rv3)
	})

	t.Run("same instant timestamp representations", func(t *testing.T) {
		utc := exact.DeepCopy()
		utc.ResourceVersion = "2"
		utc.CreationTimestamp = metav1.NewTime(createdAt)
		zoned := exact.DeepCopy()
		zoned.ResourceVersion = "2"
		zoned.CreationTimestamp = metav1.NewTime(createdAt.In(time.FixedZone("same-instant", 2*60*60)))
		wantUTC := utc.DeepCopy()
		wantZoned := zoned.DeepCopy()

		states := assertPermutationInvariant(t, utc, zoned)

		for _, state := range states {
			for _, observation := range state.RevisionObservations() {
				assert.Equal(t, time.UTC, observation.CreationTimestamp.Location())
				assert.Equal(t, createdAt, observation.CreationTimestamp.Time)
			}
		}
		assert.Equal(t, wantUTC, utc, "resolution must not mutate the UTC caller object")
		assert.Equal(t, wantZoned, zoned, "resolution must not mutate the fixed-zone caller object")
		assert.Equal(t, "same-instant", zoned.CreationTimestamp.Location().String())
	})
}

func TestResolveHistoryFingerprintUsesOnlyWriterContractEvidence(t *testing.T) {
	autoSync := false
	resolve := func(t *testing.T, exact, listed *appsv1.ControllerRevision) *RuntimeState {
		t.Helper()
		resolver, err := newRuntimePinResolver(
			func(string) revisionNamespace {
				return revisionNamespaceStub{
					get: func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
						return exact.DeepCopy(), nil
					},
					list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
						return &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{*listed.DeepCopy()}}, nil
					},
				}
			},
			liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
				return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
			}),
			"ome", testPinLimits,
		)
		require.NoError(t, err)
		state, err := resolver.Resolve(
			context.Background(), pinISVC("runtime", &autoSync, exact.Name),
			RuntimeResolveOptions{IncludeHistory: true},
		)
		require.NoError(t, err)
		return state
	}

	t.Run("mutable metadata does not split one revision", func(t *testing.T) {
		exact := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("pin"))
		exact.UID = types.UID("revision-uid")
		createdAt := time.Unix(100, 123).UTC()
		exact.CreationTimestamp = metav1.NewTime(createdAt)
		exact.ResourceVersion = "1"
		exact.Annotations[constants.RuntimeRevisionGCEligibleSinceKey] = "2026-01-01T00:00:00Z"
		exact.Annotations["example.com/mutable"] = "first"
		exact.Finalizers = []string{"example.com/first"}
		exact.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "first", UID: types.UID("owner-first"),
		}}
		exact.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "first"}}
		listed := exact.DeepCopy()
		listed.CreationTimestamp = metav1.NewTime(createdAt.In(time.FixedZone("same-instant", 2*60*60)))
		listed.ResourceVersion = "2"
		listed.Annotations[constants.RuntimeRevisionGCEligibleSinceKey] = "2026-02-01T00:00:00Z"
		listed.Annotations["example.com/mutable"] = "second"
		listed.Finalizers = []string{"example.com/second"}
		listed.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "second", UID: types.UID("owner-second"),
		}}
		listed.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "second"}}
		deletionTime := metav1.NewTime(time.Unix(200, 0))
		gracePeriod := int64(30)
		listed.DeletionTimestamp = &deletionTime
		listed.DeletionGracePeriodSeconds = &gracePeriod

		state := resolve(t, exact, listed)

		observations := state.RevisionObservations()
		require.Len(t, observations, 1)
		assert.Equal(t, []RuntimeRevisionRole{
			RuntimeRevisionRoleRequested, RuntimeRevisionRoleActive, RuntimeRevisionRoleHistory,
		}, observations[0].Roles())
		assert.NotContains(t, observations[0].ConsistencyCodes(), RevisionConsistencyConflictingIdentity)
	})

	t.Run("writer identity and content differences remain conflicts", func(t *testing.T) {
		mutations := []struct {
			name   string
			mutate func(*appsv1.ControllerRevision)
		}{
			{name: "uid", mutate: func(revision *appsv1.ControllerRevision) {
				revision.UID = types.UID("replacement-uid")
			}},
			{name: "creation timestamp", mutate: func(revision *appsv1.ControllerRevision) {
				revision.CreationTimestamp = metav1.NewTime(time.Unix(101, 123).UTC())
			}},
			{name: "missing creation timestamp", mutate: func(revision *appsv1.ControllerRevision) {
				revision.CreationTimestamp = metav1.Time{}
			}},
			{name: "writer label", mutate: func(revision *appsv1.ControllerRevision) {
				revision.Labels[constants.RuntimeRevisionHashLabelKey] = "deadbeef"
			}},
			{name: "arbitrary label", mutate: func(revision *appsv1.ControllerRevision) {
				revision.Labels["example.com/immutable"] = "added"
			}},
			{name: "data", mutate: func(revision *appsv1.ControllerRevision) {
				revision.Data.Raw = []byte(`{}`)
			}},
		}
		for _, mutation := range mutations {
			t.Run(mutation.name, func(t *testing.T) {
				exact := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("pin"))
				exact.UID = types.UID("revision-uid")
				exact.CreationTimestamp = metav1.NewTime(time.Unix(100, 123).UTC())
				listed := exact.DeepCopy()
				mutation.mutate(listed)

				state := resolve(t, exact, listed)

				observations := state.RevisionObservations()
				require.Len(t, observations, 2)
				for _, observation := range observations {
					assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyConflictingIdentity)
				}
				_, err := state.RequireConsistentActive()
				assert.ErrorIs(t, err, ErrActiveRuntimeInconsistent)
			})
		}
	})
}

func TestRuntimeRevisionWriterFingerprintMatchesWebhookLabelEquality(t *testing.T) {
	base := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("pin"))
	base.UID = types.UID("revision-uid")
	base.CreationTimestamp = metav1.NewTime(time.Unix(100, 123).UTC())
	tests := []struct {
		name      string
		prepare   func(*appsv1.ControllerRevision, *appsv1.ControllerRevision)
		wantEqual bool
	}{
		{
			name: "nil and empty maps differ",
			prepare: func(left, right *appsv1.ControllerRevision) {
				left.Labels = nil
				right.Labels = map[string]string{}
			},
			wantEqual: false,
		},
		{
			name: "equal maps ignore insertion order",
			prepare: func(left, right *appsv1.ControllerRevision) {
				entries := [][2]string{
					{constants.RuntimeRevisionOfLabelKey, "runtime"},
					{constants.RuntimeRevisionOfKindLabelKey, runtimeselector.KindClusterServingRuntime},
					{constants.RuntimeRevisionOfNamespaceLabelKey, ""},
					{constants.RuntimeRevisionHashLabelKey, base.Labels[constants.RuntimeRevisionHashLabelKey]},
					{"example.com/immutable-a", "one"},
					{"example.com/immutable-b", "two"},
				}
				left.Labels = make(map[string]string, len(entries))
				right.Labels = make(map[string]string, len(entries))
				for i := range entries {
					left.Labels[entries[i][0]] = entries[i][1]
					rightEntry := entries[len(entries)-1-i]
					right.Labels[rightEntry[0]] = rightEntry[1]
				}
			},
			wantEqual: true,
		},
		{
			name: "arbitrary label values differ",
			prepare: func(left, right *appsv1.ControllerRevision) {
				left.Labels["example.com/immutable"] = "one"
				right.Labels["example.com/immutable"] = "two"
			},
			wantEqual: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left := base.DeepCopy()
			right := base.DeepCopy()
			test.prepare(left, right)
			assert.Equal(t, test.wantEqual, reflect.DeepEqual(left.Labels, right.Labels))

			leftObservation := inspectRuntimeRevision(left, "ome", left.Name, "", "", "")
			rightObservation := inspectRuntimeRevision(right, "ome", right.Name, "", "", "")
			assert.Equal(t, test.wantEqual, leftObservation.objectFingerprint == rightObservation.objectFingerprint)

			observations := []RuntimeRevisionObservation{leftObservation, rightObservation}
			classifyRevisionCollectionAnomalies(observations)
			for _, observation := range observations {
				if test.wantEqual {
					assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
					assert.NotContains(t, observation.ConsistencyCodes(), RevisionConsistencyConflictingIdentity)
				} else {
					assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyConflictingIdentity)
					assert.NotContains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
				}
			}
		})
	}
}

func TestResolveHistoryPreservesExactFailureAssociation(t *testing.T) {
	autoSync := false
	requested := "requested-a"
	missing := apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "controllerrevisions"}, requested)
	history := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("history"))
	history.Name = requested
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{
				get: func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
					return nil, missing
				},
				list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
					return &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{*history.DeepCopy()}}, nil
				},
			}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("runtime", &autoSync, requested)
	isvc.Status.PinnedRevisionName = requested

	state, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{IncludeHistory: true})

	require.NoError(t, err)
	assert.Equal(t, RuntimePinStateRevisionMissing, state.PinState)
	observations := state.RevisionObservations()
	require.Len(t, observations, 2)
	var exact, listed *RuntimeRevisionObservation
	for i := range observations {
		if observations[i].ExpectedName() == requested {
			exact = &observations[i]
		} else if observations[i].ReturnedName() == requested {
			listed = &observations[i]
		}
	}
	require.NotNil(t, exact)
	require.NotNil(t, listed)
	assert.False(t, exact.ObjectReturned())
	assert.True(t, listed.ObjectReturned())
	assert.Empty(t, exact.ReturnedName())
	assert.Equal(t, "ome", exact.ExpectedNamespace())
	assert.Empty(t, exact.ReturnedNamespace())
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleRequested, RuntimeRevisionRoleReported}, exact.Roles())
	assert.Empty(t, listed.ExpectedName())
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleHistory}, listed.Roles())
	issues := state.SourceIssues()
	require.Len(t, issues, 1)
	assert.Equal(t, requested, issues[0].RevisionName)
	assert.ErrorIs(t, issues[0], missing)
}

func TestResolveHistoryPreservesDuplicateAndConflictingListEvidenceDeterministically(t *testing.T) {
	first := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("first"))
	second := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("second"))
	second.Name = first.Name

	resolve := func(t *testing.T, items []appsv1.ControllerRevision) []RuntimeRevisionObservation {
		t.Helper()
		resolver, err := newRuntimePinResolver(
			func(string) revisionNamespace {
				return revisionNamespaceStub{list: func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
					return &appsv1.ControllerRevisionList{Items: items}, nil
				}}
			},
			liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
				return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
			}),
			"ome", testPinLimits,
		)
		require.NoError(t, err)
		state, err := resolver.Resolve(context.Background(), pinISVC("runtime", nil, ""), RuntimeResolveOptions{IncludeHistory: true})
		require.NoError(t, err)
		return state.RevisionObservations()
	}

	t.Run("duplicate", func(t *testing.T) {
		observations := resolve(t, []appsv1.ControllerRevision{*first.DeepCopy(), *first.DeepCopy()})
		require.Len(t, observations, 2)
		for _, observation := range observations {
			assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
			assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyDuplicateContentHash)
		}
	})

	t.Run("conflicting", func(t *testing.T) {
		forward := resolve(t, []appsv1.ControllerRevision{*first.DeepCopy(), *second.DeepCopy()})
		reverse := resolve(t, []appsv1.ControllerRevision{*second.DeepCopy(), *first.DeepCopy()})
		require.Len(t, forward, 2)
		require.Len(t, reverse, 2)
		codes := func(observations []RuntimeRevisionObservation) [][]RevisionConsistencyCode {
			result := make([][]RevisionConsistencyCode, len(observations))
			for i := range observations {
				result[i] = observations[i].ConsistencyCodes()
				assert.Contains(t, result[i], RevisionConsistencyConflictingIdentity)
			}
			return result
		}
		assert.Equal(t, codes(forward), codes(reverse))
	})
}

func TestClassifyRevisionCollectionAnomaliesDetectsTrueShortHashCollision(t *testing.T) {
	observations := []RuntimeRevisionObservation{
		{Name: "a", Namespace: "ome", objectReturned: true, fullHash: "aaaa", computedShortHash: "deadbeef", Consistency: RevisionConsistencyConsistent},
		{Name: "b", Namespace: "ome", objectReturned: true, fullHash: "bbbb", computedShortHash: "deadbeef", Consistency: RevisionConsistencyConsistent},
	}

	classifyRevisionCollectionAnomalies(observations)

	for _, observation := range observations {
		assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyShortHashCollision)
		assert.Equal(t, RevisionConsistencyInconsistent, observation.Consistency)
	}
}

func TestClassifyRevisionCollectionAnomaliesPreservesDuplicateSubsetWithinConflict(t *testing.T) {
	observations := []RuntimeRevisionObservation{
		{Name: "same", Namespace: "ome", objectReturned: true, objectFingerprint: "duplicate", Consistency: RevisionConsistencyConsistent},
		{Name: "same", Namespace: "ome", objectReturned: true, objectFingerprint: "duplicate", Consistency: RevisionConsistencyConsistent},
		{Name: "same", Namespace: "ome", objectReturned: true, objectFingerprint: "conflict", Consistency: RevisionConsistencyConsistent},
	}

	classifyRevisionCollectionAnomalies(observations)

	assert.Contains(t, observations[0].ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
	assert.Contains(t, observations[1].ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
	assert.NotContains(t, observations[2].ConsistencyCodes(), RevisionConsistencyDuplicateIdentity)
	for _, observation := range observations {
		assert.Contains(t, observation.ConsistencyCodes(), RevisionConsistencyConflictingIdentity)
	}
}

func TestHistoryObservationOrderingUsesFullHashAsDeterministicTieBreak(t *testing.T) {
	left := RuntimeRevisionObservation{Name: "same", Namespace: "ome", fullHash: "bbbb"}
	right := RuntimeRevisionObservation{Name: "same", Namespace: "ome", fullHash: "aaaa"}
	observations := []RuntimeRevisionObservation{left, right}

	sortRuntimeRevisionObservations(observations)

	fullHashes := []string{observations[0].fullHash, observations[1].fullHash}
	expected := append([]string{}, fullHashes...)
	sort.Strings(expected)
	assert.Equal(t, expected, fullHashes)
}
