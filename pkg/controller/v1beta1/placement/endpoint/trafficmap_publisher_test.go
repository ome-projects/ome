package endpoint

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
)

type testTrafficMapPublishPlan struct {
	claims []string
}

func (p testTrafficMapPublishPlan) Claims() []string { return p.claims }

type recordingTrafficMapPublisher struct {
	name      string
	stateful  bool
	claims    []string
	events    []string
	resolve   func(map[string]string, map[string]string) (map[string]string, error)
	plan      func(*v1beta1.InferenceService, *v1beta1.TrafficMap, map[string]string) (TrafficMapPublishPlan, error)
	drain     func(context.Context, types.NamespacedName, []string) error
	apply     func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error)
	unpublish func(context.Context, types.NamespacedName, v1beta1.TrafficMapPublisherStatus) error
}

type preflightingTrafficMapPublisher struct {
	*recordingTrafficMapPublisher
	preflight func(context.Context, TrafficMapPublishPlan) error
}

type fallbackTrafficMapPublisher struct {
	*recordingTrafficMapPublisher
	capture func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error)
	replay  func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error)
}

func (p *fallbackTrafficMapPublisher) Capture(
	plan TrafficMapPublishPlan,
) (TrafficMapPublishPlanCapture, error) {
	if p.capture != nil {
		return p.capture(plan)
	}
	return TrafficMapPublishPlanCapture{}, errors.New("capture is not configured")
}

func (p *fallbackTrafficMapPublisher) Replay(
	lastPositive v1beta1.TrafficMapPublisherLastPositive,
) (TrafficMapPublishPlan, error) {
	p.events = append(p.events, "replay")
	if p.replay != nil {
		return p.replay(lastPositive)
	}
	return nil, errors.New("replay is not configured")
}

func (p *preflightingTrafficMapPublisher) Preflight(
	ctx context.Context,
	plan TrafficMapPublishPlan,
) error {
	p.events = append(p.events, "preflight")
	if p.preflight != nil {
		return p.preflight(ctx, plan)
	}
	return nil
}

func (p *recordingTrafficMapPublisher) Name() string { return p.name }
func (p *recordingTrafficMapPublisher) Stateful() bool {
	return p.stateful
}
func (p *recordingTrafficMapPublisher) ResolveOptions(global, inline map[string]string) (map[string]string, error) {
	if p.resolve != nil {
		return p.resolve(global, inline)
	}
	resolved := clonePublisherOptions(global)
	for key, value := range inline {
		resolved[key] = value
	}
	return resolved, nil
}
func (p *recordingTrafficMapPublisher) Plan(
	owner *v1beta1.InferenceService,
	trafficMap *v1beta1.TrafficMap,
	options map[string]string,
) (TrafficMapPublishPlan, error) {
	if p.plan != nil {
		return p.plan(owner, trafficMap, options)
	}
	return testTrafficMapPublishPlan{claims: p.claims}, nil
}
func (p *recordingTrafficMapPublisher) Drain(
	ctx context.Context,
	key types.NamespacedName,
	claims []string,
) error {
	p.events = append(p.events, "drain:"+strings.Join(claims, ","))
	if p.drain != nil {
		return p.drain(ctx, key, claims)
	}
	return nil
}
func (p *recordingTrafficMapPublisher) Apply(
	ctx context.Context,
	plan TrafficMapPublishPlan,
) (TrafficMapPublishResult, error) {
	p.events = append(p.events, "apply")
	if p.apply != nil {
		return p.apply(ctx, plan)
	}
	return TrafficMapPublishResult{}, nil
}
func (p *recordingTrafficMapPublisher) Unpublish(
	ctx context.Context,
	key types.NamespacedName,
	journal v1beta1.TrafficMapPublisherStatus,
) error {
	p.events = append(p.events, "unpublish:"+strings.Join(journal.ClaimedTargets, ","))
	if p.unpublish != nil {
		return p.unpublish(ctx, key, journal)
	}
	return nil
}

func TestTrafficMapPublisherAddsFinalizerBeforeEffects(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Empty(t, publisher.events, "no external effect may precede the persisted finalizer")

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Nil(t, current.Status.Publisher)
}

func TestTrafficMapPublisherModeOnlyPlacement(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy v1beta1.PlacementPolicy
		mode   v1beta1.PlacementMode
		want   bool
	}{
		{name: "single", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSingle, want: true},
		{name: "all", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeAll, want: true},
		{name: "split", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSplit, want: true},
		{name: "capacity", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSplitByCapacity, want: true},
		{name: "legacy", policy: v1beta1.PlacementPolicy("Legacy"), mode: v1beta1.PlacementModeAll},
		{name: "omitted policy", mode: v1beta1.PlacementModeSingle},
		{name: "local"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			owner.Spec.Placement = nil
			if tt.mode != "" {
				owner.Spec.Placement = &v1beta1.PlacementSpec{Policy: tt.policy, Mode: tt.mode}
			}
			trafficMap.Spec.Mode = tt.mode
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target-a"},
			}
			r, c := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			if _, err := reconcileTrafficMapPublisher(t, r, trafficMap); err != nil {
				t.Fatal(err)
			}
			var wantEvents []string
			if tt.want {
				wantEvents = []string{"apply"}
			}
			if diff := cmp.Diff(wantEvents, publisher.events); diff != "" {
				t.Fatalf("publication effects (-want +got):\n%s", diff)
			}
			current := getPublisherTrafficMap(t, c, trafficMap)
			if diff := cmp.Diff(tt.want, current.Status.Published); diff != "" {
				t.Fatalf("publication status (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(owner.UID, current.Status.SourceUID); diff != "" {
				t.Fatalf("source identity (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTrafficMapPublisherRequiresMatchingSourceUIDBeforePlanning(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sourceUID types.UID
	}{
		{name: "missing"},
		{name: "mismatched", sourceUID: "other-owner-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.SourceUID = tc.sourceUID
			planCalls := 0
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target-a"},
				plan: func(
					*v1beta1.InferenceService,
					*v1beta1.TrafficMap,
					map[string]string,
				) (TrafficMapPublishPlan, error) {
					planCalls++
					return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

			require.NoError(t, err)
			assert.Zero(t, planCalls)
			assert.Empty(t, publisher.events, "source provenance must gate every external mutation")
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, tc.sourceUID, current.Status.SourceUID,
				"publisher status ownership must not create or overwrite routing provenance")
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
		})
	}
}

func TestTrafficMapPublisherChecksSourceUIDThroughAPIReader(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.SourceUID = ""
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	stale := trafficMap.DeepCopy()
	stale.Status.SourceUID = owner.UID
	cacheClient := &staleTrafficMapClient{Client: baseClient, stale: stale}
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: cacheClient, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

	require.NoError(t, err)
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.Empty(t, current.Status.SourceUID)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
}

func TestTrafficMapPublisherRetriesInitialOwnerReadFailure(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	reader := &failingInferenceServiceGetReader{
		Reader: baseClient,
		err:    apierrors.NewInternalError(errors.New("owner read failed")),
	}
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

	require.ErrorContains(t, err, "owner read failed")
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
}

func TestTrafficMapPublisherRechecksSourceAndOwnerAtPreEffectFence(t *testing.T) {
	changes := []struct {
		name             string
		mutateTrafficMap func(*v1beta1.TrafficMap)
		mutateOwner      func(*v1beta1.InferenceService)
	}{
		{
			name: "source UID removed",
			mutateTrafficMap: func(trafficMap *v1beta1.TrafficMap) {
				trafficMap.Status.SourceUID = ""
			},
		},
		{
			name: "source UID changed",
			mutateTrafficMap: func(trafficMap *v1beta1.TrafficMap) {
				trafficMap.Status.SourceUID = "replacement-owner-uid"
			},
		},
		{
			name: "controller reference removed",
			mutateTrafficMap: func(trafficMap *v1beta1.TrafficMap) {
				trafficMap.OwnerReferences = nil
			},
		},
		{
			name: "owner began deleting",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				now := metav1.Now()
				owner.DeletionTimestamp = &now
				owner.Finalizers = []string{"example.com/hold"}
			},
		},
		{
			name: "owner became ineligible",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Spec.Placement = nil
			},
		},
		{
			name: "owner generation changed",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Generation++
			},
		},
		{
			name: "owner labels changed",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Labels = map[string]string{"example.com/publish": "disabled"}
			},
		},
		{
			name: "owner annotations changed",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Annotations = map[string]string{"example.com/publication": "changed"}
			},
		},
		{
			name: "owner finalizers changed",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Finalizers = []string{"example.com/publication"}
			},
		},
		{
			name: "owner opted out",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				disabled := false
				owner.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
			},
		},
	}
	for _, change := range changes {
		for _, stateful := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stateful=%t", change.name, stateful), func(t *testing.T) {
				owner, trafficMap := publisherTestObjects()
				if stateful {
					trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
				}
				baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
				reader := &preEffectChangingReader{
					Reader:           baseClient,
					mutateTrafficMap: change.mutateTrafficMap,
					mutateOwner:      change.mutateOwner,
				}
				planCalls := 0
				var claims []string
				if stateful {
					claims = []string{"target-a"}
				}
				publisher := &recordingTrafficMapPublisher{
					name: "example-publisher", stateful: stateful, claims: claims,
					plan: func(
						*v1beta1.InferenceService,
						*v1beta1.TrafficMap,
						map[string]string,
					) (TrafficMapPublishPlan, error) {
						planCalls++
						return testTrafficMapPublishPlan{claims: claims}, nil
					},
				}
				reconciler := &TrafficMapPublisherReconciler{
					Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
				}

				_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

				require.NoError(t, err)
				assert.Equal(t, 1, planCalls, "planning is pure and follows the initial authoritative provenance check")
				assert.Empty(t, publisher.events, "the last live provenance check must precede external effects")
				current := getPublisherTrafficMap(t, baseClient, trafficMap)
				assert.Equal(t, owner.UID, current.Status.SourceUID)
				condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
				require.NotNil(t, condition)
				assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
			})
		}
	}
}

func TestTrafficMapPublisherRetriesOwnerReadFailureAtPreEffectFence(t *testing.T) {
	for _, stateful := range []bool{false, true} {
		t.Run(fmt.Sprintf("stateful=%t", stateful), func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			if stateful {
				trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			}
			baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
			reader := &preEffectChangingReader{
				Reader:   baseClient,
				ownerErr: apierrors.NewInternalError(errors.New("owner read failed")),
			}
			planCalls := 0
			var claims []string
			if stateful {
				claims = []string{"target-a"}
			}
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: stateful, claims: claims,
				plan: func(
					*v1beta1.InferenceService,
					*v1beta1.TrafficMap,
					map[string]string,
				) (TrafficMapPublishPlan, error) {
					planCalls++
					return testTrafficMapPublishPlan{claims: claims}, nil
				},
			}
			reconciler := &TrafficMapPublisherReconciler{
				Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
			}

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

			require.ErrorContains(t, err, "owner read failed")
			assert.Equal(t, 1, planCalls)
			assert.Empty(t, publisher.events)
			current := getPublisherTrafficMap(t, baseClient, trafficMap)
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
		})
	}
}

func TestTrafficMapPublisherWaitsForLegacyEndpointCleanup(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(
		t, publisher, owner, trafficMap, legacyOwner,
	)
	reconciler.RequeueAfter = time.Minute

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, result.RequeueAfter)
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.NotContains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)

	require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(legacyOwner), legacyOwner))
	legacyOwner.Finalizers = nil
	require.NoError(t, kubeClient.Update(context.Background(), legacyOwner))
	result, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Equal(t, 1, planCalls)
	assert.Empty(t, publisher.events, "publication waits for the TrafficMap finalizer write")
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
}

func TestTrafficMapPublisherForeignJournalDoesNotBypassLegacyCleanup(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: "other-publisher"}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(
		t, publisher, owner, trafficMap, legacyOwner,
	)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)
}

func TestTrafficMapPublisherEstablishedJournalDoesNotUseMigrationFence(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
	}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, _ := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap, legacyOwner)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"apply"}, publisher.events)
}

func TestTrafficMapPublisherRetriesLegacyStateReadFailure(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	reader := &failInferenceServiceListReader{
		Reader: baseClient,
		err:    errors.New("injected InferenceService list failure"),
	}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "injected InferenceService list failure")
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)
}

func TestTrafficMapPublisherPreclaimsBeforeDrainAndApply(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName:  "example-publisher",
		ClaimedTargets: []string{"retired"},
	}
	trafficMap.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
		Reason: "Routable", ObservedGeneration: trafficMap.Generation,
	}}
	owner.Spec.Routing = &v1beta1.RoutingSpec{Publisher: &v1beta1.RoutingPublisherSpec{
		Options: map[string]string{"inline": "value"},
	}}

	global := map[string]string{"global": "value"}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		resolve: func(gotGlobal, gotInline map[string]string) (map[string]string, error) {
			assert.Equal(t, global, gotGlobal)
			assert.Equal(t, owner.Spec.Routing.Publisher.Options, gotInline)
			gotGlobal["mutated"] = "resolver"
			gotInline["mutated"] = "resolver"
			return map[string]string{"effective": "blue"}, nil
		},
		plan: func(_ *v1beta1.InferenceService, _ *v1beta1.TrafficMap, options map[string]string) (TrafficMapPublishPlan, error) {
			assert.Equal(t, map[string]string{"effective": "blue"}, options)
			options["effective"] = "plan-mutated"
			return testTrafficMapPublishPlan{claims: []string{"target-z", "target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	reconciler.GlobalOptions = global
	reconciler.RequeueAfter = time.Minute
	assertPreclaimed := func() {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		require.NotNil(t, current.Status.Publisher)
		assert.Equal(t, []string{"retired", "target-a", "target-z"}, current.Status.Publisher.ClaimedTargets)
	}
	publisher.drain = func(_ context.Context, key types.NamespacedName, claims []string) error {
		assertPreclaimed()
		assert.Equal(t, client.ObjectKeyFromObject(trafficMap), key)
		assert.Equal(t, []string{"retired"}, claims)
		return nil
	}
	publisher.apply = func(_ context.Context, plan TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		assertPreclaimed()
		assert.Equal(t, []string{"target-z", "target-a"}, plan.Claims())
		return TrafficMapPublishResult{GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "model",
		}}, nil
	}

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, result.RequeueAfter)
	assert.Equal(t, []string{"drain:retired", "apply"}, publisher.events)
	assert.Equal(t, map[string]string{"global": "value"}, global,
		"the resolver must receive a copy of global options")
	assert.Equal(t, map[string]string{"inline": "value"}, owner.Spec.Routing.Publisher.Options,
		"the resolver must receive a copy of inline options")

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, owner.UID, current.Status.SourceUID)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"retired", "target-a", "target-z"}, current.Status.Publisher.ClaimedTargets,
		"drained targets stay claimed until whole-map finalization")
	wantDigest, err := publisherOptionsDigest("example-publisher", map[string]string{"effective": "blue"})
	require.NoError(t, err)
	assert.Equal(t, wantDigest, current.Status.Publisher.ObservedOptionsDigest)
	assert.True(t, current.Status.Published)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.GatewayRef)
	assert.Equal(t, "GlobalRoute", current.Status.GatewayRef.Kind)
	assert.Equal(t, "model", current.Status.GatewayRef.Name)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionTrue, published.Status)
	assert.Equal(t, trafficMapPublisherReasonPublished, published.Reason)
	assert.Equal(t, "TrafficMap is published by publisher \"example-publisher\"", published.Message)
}

func TestTrafficMapPublisherCapturesCompletePositivePlanAfterApply(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName:  "example-publisher",
		ClaimedTargets: []string{"retired"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("b"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "retired", Weight: 100}},
		},
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-b", "target-a"},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets: []v1beta1.TrafficMapPublisherTarget{
					{Target: "target-b", Weight: 40},
					{Target: "target-a", Weight: 60},
				},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.False(t, current.Status.Published, "transition must be durable before Apply")
		fallback := apimeta.FindStatusCondition(
			current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
		)
		require.NotNil(t, fallback)
		assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
		assert.Equal(t, v1beta1.TrafficMapReasonPublicationTransitioning, fallback.Reason)
		require.NotNil(t, current.Status.Publisher)
		assert.Nil(t, current.Status.Publisher.LastPositive,
			"claim-set changes must clear the previous snapshot before external mutation")
		assert.Equal(t, []string{"retired", "target-a", "target-b"}, current.Status.Publisher.ClaimedTargets)
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:retired", "apply"}, publisher.events)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	require.NotNil(t, current.Status.Publisher.LastPositive)
	assert.Equal(t, trafficMap.Generation, current.Status.Publisher.LastPositive.TrafficMapGeneration)
	assert.Equal(t, owner.Generation, current.Status.Publisher.LastPositive.ObservedISVCGeneration)
	assert.Equal(t, testPublisherDigest("a"), current.Status.Publisher.LastPositive.PlanCompatibilityDigest)
	assert.Equal(t, []v1beta1.TrafficMapPublisherTarget{
		{Target: "retired", Weight: 0},
		{Target: "target-a", Weight: 60},
		{Target: "target-b", Weight: 40},
	}, current.Status.Publisher.LastPositive.Targets,
		"every retained claim must be represented, including retired targets at zero")
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonCurrentPlan, fallback.Reason)
}

func TestTrafficMapPublisherReplaysEligibleLastPositivePlan(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a", "target-b"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "target-a", Weight: 75},
			{Target: "target-b", Weight: 25},
		},
	}
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		ObservedOptionsDigest: testPublisherDigest("f"),
		LastPositive:          copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(got v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			assert.Equal(t, *lastPositive, got)
			return testTrafficMapPublishPlan{claims: claims}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(_ context.Context, plan TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		assert.Equal(t, claims, plan.Claims())
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.False(t, current.Status.Published)
		assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive,
			"fallback transition must retain the complete retry journal")
		fallback := apimeta.FindStatusCondition(
			current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
		)
		require.NotNil(t, fallback)
		assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"replay", "apply"}, publisher.events)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Published)
	assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.NotEqual(t, testPublisherDigest("f"), current.Status.Publisher.ObservedOptionsDigest,
		"the full current options digest advances after a successful replay")
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonPublicationFallback, published.Reason)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionTrue, fallback.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonLastPositiveRetained, fallback.Reason)
	assert.Equal(t, trafficMap.Generation, fallback.ObservedGeneration)
}

func TestTrafficMapPublisherReplaysAllHomesUnreadyForOptedInPublisher(t *testing.T) {
	owner, trafficMap := allHomesUnreadyPublisherObjects()
	currentClaims := []string{"target-a", "target-b", "target-c"}
	durableClaims := []string{"target-a", "target-b", "target-c", "target-retired"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation - 1,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "target-a", Weight: 75},
			{Target: "target-b", Weight: 25},
			{Target: "target-c", Weight: 0},
			{Target: "target-retired", Weight: 0},
		},
	}
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: durableClaims,
		ObservedOptionsDigest: testPublisherDigest("f"),
		LastPositive:          copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: currentClaims,
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets: []v1beta1.TrafficMapPublisherTarget{
					{Target: "target-a", Weight: 0},
					{Target: "target-b", Weight: 0},
					{Target: "target-c", Weight: 0},
				},
				PlanCompatibilityDigest:      testPublisherDigest("a"),
				NoPositivePlanPolicy:         TrafficMapNoPositivePlanPolicyRetainLastPositive,
				AllowAllHomesUnreadyFallback: true,
			}, nil
		},
		replay: func(got v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			assert.Equal(t, *lastPositive, got)
			return testTrafficMapPublishPlan{claims: durableClaims}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"replay", "apply"}, publisher.events)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, durableClaims, current.Status.Publisher.ClaimedTargets)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionTrue, fallback.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonLastPositiveRetained, fallback.Reason)
}

func TestTrafficMapPublisherAllHomesUnreadyFallbackRequiresPublisherOptIn(t *testing.T) {
	owner, trafficMap := allHomesUnreadyPublisherObjects()
	durableClaims := []string{"target-a", "target-b", "target-c", "target-retired"}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: durableClaims,
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets: []v1beta1.TrafficMapPublisherTarget{
				{Target: "target-a", Weight: 75},
				{Target: "target-b", Weight: 25},
				{Target: "target-c", Weight: 0},
				{Target: "target-retired", Weight: 0},
			},
		},
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		claims: []string{"target-a", "target-b", "target-c"},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets: []v1beta1.TrafficMapPublisherTarget{
					{Target: "target-a", Weight: 0},
					{Target: "target-b", Weight: 0},
					{Target: "target-c", Weight: 0},
				},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			t.Fatal("generic fallback must not replay AllHomesUnready")
			return nil, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:target-retired", "apply"}, publisher.events)
	assert.Nil(t, getPublisherTrafficMap(t, kubeClient, trafficMap).Status.Publisher.LastPositive)
}

func TestTrafficMapPublisherInvalidSnapshotClearsBeforeApplyingCurrentPlan(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a"}
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("b"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
		},
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			t.Fatal("invalid snapshot must not be replayed")
			return nil, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		require.NotNil(t, current.Status.Publisher)
		assert.Nil(t, current.Status.Publisher.LastPositive,
			"incompatible snapshot must be durably invalidated before Apply")
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:target-a", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher.LastPositive)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonApplyCurrent, fallback.Reason)
}

func TestTrafficMapPublisherStrictClaimSubsetInvalidatesFallback(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a", "target-b"}
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets: []v1beta1.TrafficMapPublisherTarget{
				{Target: "target-a", Weight: 50},
				{Target: "target-b", Weight: 50},
			},
		},
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 0}},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			t.Fatal("a nonempty strict claim subset must not replay the old plan")
			return nil, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.Nil(t, current.Status.Publisher.LastPositive)
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:target-b", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher.LastPositive)
}

func TestTrafficMapPublisherApplyCurrentClearsEmptyAndAllZeroPlans(t *testing.T) {
	tests := []struct {
		name       string
		claims     []string
		targets    []v1beta1.TrafficMapPublisherTarget
		wantEvents []string
	}{
		{
			name:       "empty",
			wantEvents: []string{"drain:target-a", "apply"},
		},
		{
			name:   "all zero",
			claims: []string{"target-a", "target-b"},
			targets: []v1beta1.TrafficMapPublisherTarget{
				{Target: "target-a", Weight: 0},
				{Target: "target-b", Weight: 0},
			},
			wantEvents: []string{"apply"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			durableClaims := []string{"target-a"}
			if len(tt.claims) != 0 {
				durableClaims = slices.Clone(tt.claims)
			}
			trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: durableClaims,
				LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
					TrafficMapGeneration:    trafficMap.Generation - 1,
					ObservedISVCGeneration:  owner.Generation,
					PlanCompatibilityDigest: testPublisherDigest("a"),
					Targets: []v1beta1.TrafficMapPublisherTarget{
						{Target: "target-a", Weight: 100},
					},
				},
			}
			if len(durableClaims) == 2 {
				trafficMap.Status.Publisher.LastPositive.Targets = append(
					trafficMap.Status.Publisher.LastPositive.Targets,
					v1beta1.TrafficMapPublisherTarget{Target: "target-b", Weight: 0},
				)
			}
			base := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: slices.Clone(tt.claims),
			}
			publisher := &fallbackTrafficMapPublisher{
				recordingTrafficMapPublisher: base,
				capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
					return TrafficMapPublishPlanCapture{
						Targets:                 slices.Clone(tt.targets),
						PlanCompatibilityDigest: testPublisherDigest("a"),
						NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyApplyCurrent,
					}, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			publisher.apply = func(_ context.Context, plan TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
				assert.Equal(t, tt.claims, plan.Claims())
				current := getPublisherTrafficMap(t, kubeClient, trafficMap)
				assert.Nil(t, current.Status.Publisher.LastPositive,
					"ApplyCurrent must durably clear retained state before applying no-positive plans")
				return TrafficMapPublishResult{}, nil
			}

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEvents, publisher.events)
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.True(t, current.Status.Published)
			assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
			assert.Nil(t, current.Status.Publisher.LastPositive)
			fallback := apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			)
			require.NotNil(t, fallback)
			assert.Equal(t, metav1.ConditionFalse, fallback.Status)
			assert.Equal(t, trafficMapPublisherReasonApplyCurrent, fallback.Reason)
		})
	}
}

func TestTrafficMapPublisherReplaysNoAddressableHome(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	owner.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "cluster-a", ReadyReplicas: 2,
		}},
	}
	apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
	).Reason = v1beta1.TrafficMapReasonNoAddressableHome
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			assert.Equal(t, *lastPositive, snapshot)
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"replay", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Published)
	assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
	assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisherPlacementUnknownPreservesSnapshotUntilRestartReplay(t *testing.T) {
	tests := []struct {
		name           string
		routableReason string
		makeUnknown    func(*v1beta1.InferenceService)
		makeEligible   func(*v1beta1.InferenceService)
	}{
		{
			name:           "single-home loss",
			routableReason: v1beta1.TrafficMapReasonNotPlaced,
			makeUnknown: func(owner *v1beta1.InferenceService) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
			},
			makeEligible: func(owner *v1beta1.InferenceService) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonLost
			},
		},
		{
			name:           "placed home temporarily unobservable",
			routableReason: v1beta1.TrafficMapReasonNoAddressableHome,
			makeUnknown: func(owner *v1beta1.InferenceService) {
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", ReadyReplicas: 0,
					}},
				}
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
			},
			makeEligible: func(owner *v1beta1.InferenceService) {
				owner.Status.Placement.Candidates[0].ReadyReplicas = 2
				owner.Status.Conditions[0].Status = corev1.ConditionFalse
				owner.Status.Conditions[0].Reason = "PlacementNotReady"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := eligiblePlacementLossPublisherObjects()
			tt.makeUnknown(owner)
			apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			).Reason = tt.routableReason
			lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
				TrafficMapGeneration:    trafficMap.Generation - 1,
				ObservedISVCGeneration:  owner.Generation,
				PlanCompatibilityDigest: testPublisherDigest("a"),
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
			}
			trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
				LastPositive: copyTrafficMapLastPositive(lastPositive),
			}
			apimeta.SetStatusCondition(&trafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapPublicationFallback, Status: metav1.ConditionTrue,
				Reason:             v1beta1.TrafficMapReasonLastPositiveRetained,
				ObservedGeneration: trafficMap.Generation - 1,
			})
			kubeClient := newTrafficMapPublisherClient(t, owner, trafficMap)
			var replayed []v1beta1.TrafficMapPublisherLastPositive
			newPublisher := func() *fallbackTrafficMapPublisher {
				base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
				return &fallbackTrafficMapPublisher{
					recordingTrafficMapPublisher: base,
					capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
						return TrafficMapPublishPlanCapture{
							PlanCompatibilityDigest: testPublisherDigest("a"),
							NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
						}, nil
					},
					replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
						replayed = append(replayed, *copyTrafficMapLastPositive(&snapshot))
						return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
					},
				}
			}
			firstPublisher := newPublisher()
			first := &TrafficMapPublisherReconciler{
				Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(),
				Publisher: firstPublisher, Active: true,
			}

			_, err := reconcileTrafficMapPublisher(t, first, trafficMap)
			require.NoError(t, err)
			assert.Empty(t, firstPublisher.events,
				"PlacementUnknown must neither replay nor clear the retained data plane")
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, []string{"target-a"}, current.Status.Publisher.ClaimedTargets)
			assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
			assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
			fallback := apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			)
			require.NotNil(t, fallback)
			assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
			assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, fallback.Reason)
			published := apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublished,
			)
			require.NotNil(t, published)
			assert.False(t, current.Status.Published)
			assert.Equal(t, metav1.ConditionFalse, published.Status)
			assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, published.Reason)

			liveOwner := &v1beta1.InferenceService{}
			require.NoError(t, kubeClient.Get(
				context.Background(), client.ObjectKeyFromObject(owner), liveOwner,
			))
			tt.makeEligible(liveOwner)
			require.NoError(t, kubeClient.Update(context.Background(), liveOwner))
			liveTrafficMap := getPublisherTrafficMap(t, kubeClient, trafficMap)
			apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionFalse,
				Reason: tt.routableReason, ObservedGeneration: liveTrafficMap.Generation,
			})
			apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
				Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: liveTrafficMap.Generation,
			})
			require.NoError(t, kubeClient.Status().Update(context.Background(), liveTrafficMap))
			current = getPublisherTrafficMap(t, kubeClient, trafficMap)

			restartedPublisher := newPublisher()
			restarted := &TrafficMapPublisherReconciler{
				Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(),
				Publisher: restartedPublisher, Active: true,
			}
			_, err = reconcileTrafficMapPublisher(t, restarted, current)
			require.NoError(t, err)
			assert.Equal(t, []string{"replay", "apply"}, restartedPublisher.events)
			require.Len(t, replayed, 1)
			assert.Equal(t, *lastPositive, replayed[0])
			current = getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
			assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			).Status)
		})
	}
}

func TestTrafficMapPublisherLaggingEmptyPlanReasonPreservesSnapshotUntilCatchUp(t *testing.T) {
	tests := []struct {
		name            string
		laggingReason   string
		caughtUpReason  string
		configureSource func(*v1beta1.InferenceService)
		catchUpSource   func(*v1beta1.InferenceService)
	}{
		{
			name:           "NotPlaced lags NoAddressableHome source",
			laggingReason:  v1beta1.TrafficMapReasonNotPlaced,
			caughtUpReason: v1beta1.TrafficMapReasonNoAddressableHome,
			configureSource: func(owner *v1beta1.InferenceService) {
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", ReadyReplicas: 2,
					}},
				}
				owner.Status.Conditions[0].Status = corev1.ConditionFalse
				owner.Status.Conditions[0].Reason = "PlacementNotReady"
			},
		},
		{
			name:           "NoAddressableHome lags PlacementLost source",
			laggingReason:  v1beta1.TrafficMapReasonNoAddressableHome,
			caughtUpReason: v1beta1.TrafficMapReasonNotPlaced,
			configureSource: func(*v1beta1.InferenceService) {
				// eligiblePlacementLossPublisherObjects already has the exact
				// Pending/last-winner/PlacementLost source shape.
			},
		},
		{
			name:           "NoAddressableHome lags pending PlacementUnknown source",
			laggingReason:  v1beta1.TrafficMapReasonNoAddressableHome,
			caughtUpReason: v1beta1.TrafficMapReasonNotPlaced,
			configureSource: func(owner *v1beta1.InferenceService) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
			},
			catchUpSource: func(owner *v1beta1.InferenceService) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonLost
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := eligiblePlacementLossPublisherObjects()
			tt.configureSource(owner)
			apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			).Reason = tt.laggingReason
			lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
				TrafficMapGeneration:    trafficMap.Generation - 1,
				ObservedISVCGeneration:  owner.Generation,
				PlanCompatibilityDigest: testPublisherDigest("a"),
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
			}
			trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
				LastPositive: copyTrafficMapLastPositive(lastPositive),
			}
			kubeClient := newTrafficMapPublisherClient(t, owner, trafficMap)
			var replayed []v1beta1.TrafficMapPublisherLastPositive
			newPublisher := func() *fallbackTrafficMapPublisher {
				base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
				return &fallbackTrafficMapPublisher{
					recordingTrafficMapPublisher: base,
					capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
						return TrafficMapPublishPlanCapture{
							PlanCompatibilityDigest: testPublisherDigest("a"),
							NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
						}, nil
					},
					replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
						replayed = append(replayed, *copyTrafficMapLastPositive(&snapshot))
						return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
					},
				}
			}
			firstPublisher := newPublisher()
			first := &TrafficMapPublisherReconciler{
				Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(),
				Publisher: firstPublisher, Active: true,
			}

			_, err := reconcileTrafficMapPublisher(t, first, trafficMap)
			require.NoError(t, err)
			assert.Empty(t, firstPublisher.events)
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, []string{"target-a"}, current.Status.Publisher.ClaimedTargets)
			assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
			assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
			fallback := apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			)
			require.NotNil(t, fallback)
			assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
			assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, fallback.Reason)

			if tt.catchUpSource != nil {
				liveOwner := &v1beta1.InferenceService{}
				require.NoError(t, kubeClient.Get(
					context.Background(), client.ObjectKeyFromObject(owner), liveOwner,
				))
				tt.catchUpSource(liveOwner)
				require.NoError(t, kubeClient.Update(context.Background(), liveOwner))
			}
			liveTrafficMap := getPublisherTrafficMap(t, kubeClient, trafficMap)
			apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionFalse,
				Reason: tt.caughtUpReason, ObservedGeneration: liveTrafficMap.Generation,
			})
			apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
				Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: liveTrafficMap.Generation,
			})
			require.NoError(t, kubeClient.Status().Update(context.Background(), liveTrafficMap))
			current = getPublisherTrafficMap(t, kubeClient, trafficMap)

			restartedPublisher := newPublisher()
			restarted := &TrafficMapPublisherReconciler{
				Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(),
				Publisher: restartedPublisher, Active: true,
			}
			_, err = reconcileTrafficMapPublisher(t, restarted, current)
			require.NoError(t, err)
			assert.Equal(t, []string{"replay", "apply"}, restartedPublisher.events)
			require.Len(t, replayed, 1)
			assert.Equal(t, *lastPositive, replayed[0])
			current = getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
			assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			).Status)
		})
	}
}

func TestTrafficMapPublisherLaggingNotPlacedPreservesUntilPositiveRecovery(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	endpoint, err := apis.ParseURL("https://model.example.com")
	require.NoError(t, err)
	owner.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitted,
			ReadyReplicas: 1, Endpoint: endpoint.DeepCopy(),
		}},
	}
	owner.Status.Conditions[0].Status = corev1.ConditionTrue
	owner.Status.Conditions[0].Reason = "PlacementReady"
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	kubeClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	positive := false
	newPublisher := func() *fallbackTrafficMapPublisher {
		base := &recordingTrafficMapPublisher{
			name: "example-publisher", stateful: true,
			plan: func(
				*v1beta1.InferenceService,
				*v1beta1.TrafficMap,
				map[string]string,
			) (TrafficMapPublishPlan, error) {
				if positive {
					return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
				}
				return testTrafficMapPublishPlan{}, nil
			},
		}
		return &fallbackTrafficMapPublisher{
			recordingTrafficMapPublisher: base,
			capture: func(plan TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
				capture := TrafficMapPublishPlanCapture{
					PlanCompatibilityDigest: testPublisherDigest("a"),
					NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
				}
				if len(plan.Claims()) != 0 {
					capture.Targets = []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 65}}
				}
				return capture, nil
			},
			replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
				t.Fatal("positive routing recovery must apply the current plan, not Replay")
				return nil, nil
			},
		}
	}
	firstPublisher := newPublisher()
	first := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: firstPublisher, Active: true,
	}

	_, err = reconcileTrafficMapPublisher(t, first, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, firstPublisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, fallback.Reason)

	positive = true
	liveTrafficMap := getPublisherTrafficMap(t, kubeClient, trafficMap)
	liveTrafficMap.Spec.Entries = []v1beta1.TrafficMapEntry{{
		Cluster: "cluster-a", Endpoint: endpoint.DeepCopy(), Weight: 100, Healthy: true,
	}}
	require.NoError(t, kubeClient.Update(context.Background(), liveTrafficMap))
	liveTrafficMap = getPublisherTrafficMap(t, kubeClient, trafficMap)
	apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
		Reason: v1beta1.TrafficMapReasonRoutable, ObservedGeneration: liveTrafficMap.Generation,
	})
	apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
		Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: liveTrafficMap.Generation,
	})
	require.NoError(t, kubeClient.Status().Update(context.Background(), liveTrafficMap))
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)

	restartedPublisher := newPublisher()
	restarted := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: restartedPublisher, Active: true,
	}
	_, err = reconcileTrafficMapPublisher(t, restarted, current)
	require.NoError(t, err)
	assert.Equal(t, []string{"apply"}, restartedPublisher.events)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Published)
	assert.Equal(t, current.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.Publisher.LastPositive)
	assert.Equal(t, []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 65}},
		current.Status.Publisher.LastPositive.Targets)
	assert.Equal(t, metav1.ConditionFalse, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisherPositiveRecoveryReplacesFallbackSnapshot(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	oldSnapshot := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = oldSnapshot.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: copyTrafficMapLastPositive(oldSnapshot),
	}
	trafficMap.Status.Conditions = []metav1.Condition{
		{
			Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionFalse,
			Reason: v1beta1.TrafficMapReasonPublicationFallback, ObservedGeneration: trafficMap.Generation - 1,
		},
		{
			Type: v1beta1.TrafficMapPublicationFallback, Status: metav1.ConditionTrue,
			Reason: v1beta1.TrafficMapReasonLastPositiveRetained, ObservedGeneration: trafficMap.Generation - 1,
		},
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 65}},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			t.Fatal("a positive recovery plan must not use Replay")
			return nil, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.Equal(t, oldSnapshot, current.Status.Publisher.LastPositive,
			"compatible prior snapshot remains the retry journal until positive Apply succeeds")
		assert.Equal(t, metav1.ConditionUnknown, apimeta.FindStatusCondition(
			current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
		).Status)
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Published)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.Publisher.LastPositive)
	assert.Equal(t, trafficMap.Generation, current.Status.Publisher.LastPositive.TrafficMapGeneration)
	assert.Equal(t, []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 65}},
		current.Status.Publisher.LastPositive.Targets)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonCurrentPlan, fallback.Reason)
}

func TestTrafficMapPublisherKeepsEarlierSnapshotUntilPositiveRecoveryApplies(t *testing.T) {
	owner, trafficMap := allHomesUnreadyPublisherObjects()
	owner.Status.Conditions[0].Status = corev1.ConditionTrue
	owner.Status.Conditions[0].Reason = "PlacementReady"
	for i := range owner.Status.Placement.Candidates {
		owner.Status.Placement.Candidates[i].ReadyReplicas = 1
	}
	for i := range trafficMap.Spec.Entries {
		trafficMap.Spec.Entries[i].Weight = 1
		trafficMap.Spec.Entries[i].Healthy = true
		trafficMap.Spec.Entries[i].Capacity.Ready = 1
	}
	routable := apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
	)
	routable.Status = metav1.ConditionTrue
	routable.Reason = v1beta1.TrafficMapReasonRoutable
	currentClaims := []string{"target-a", "target-b", "target-c"}
	durableClaims := []string{"target-a", "target-b", "target-c", "target-retired"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation - 1,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "target-a", Weight: 75},
			{Target: "target-b", Weight: 25},
			{Target: "target-c", Weight: 0},
			{Target: "target-retired", Weight: 0},
		},
	}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: durableClaims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: currentClaims,
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets: []v1beta1.TrafficMapPublisherTarget{
					{Target: "target-a", Weight: 50},
					{Target: "target-b", Weight: 30},
					{Target: "target-c", Weight: 20},
				},
				PlanCompatibilityDigest:      testPublisherDigest("a"),
				NoPositivePlanPolicy:         TrafficMapNoPositivePlanPolicyRetainLastPositive,
				AllowAllHomesUnreadyFallback: true,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive,
			"the retained plan must survive until positive recovery is fully applied")
		return TrafficMapPublishResult{}, errors.New("partial positive recovery")
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "partial positive recovery")
	assert.Equal(t, []string{"drain:target-retired", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
}

func TestTrafficMapPublisherUnknownEligibilityPreservesSnapshotWithoutEffects(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	owner.Status.ObservedGeneration--
	claims := []string{"target-a"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	assert.Equal(t, reconciler.RequeueAfter, result.RequeueAfter)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, fallback.Reason)
}

func TestTrafficMapPublisherFailedReplayPreservesSnapshotForFullRestartRetry(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a", "target-b"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "target-a", Weight: 80},
			{Target: "target-b", Weight: 20},
		},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	kubeClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	var replayed []v1beta1.TrafficMapPublisherLastPositive
	applyCalls := 0
	newPublisher := func() *fallbackTrafficMapPublisher {
		base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
		publisher := &fallbackTrafficMapPublisher{
			recordingTrafficMapPublisher: base,
			capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
				return TrafficMapPublishPlanCapture{
					PlanCompatibilityDigest: testPublisherDigest("a"),
					NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
				}, nil
			},
			replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
				replayed = append(replayed, *copyTrafficMapLastPositive(&snapshot))
				return testTrafficMapPublishPlan{claims: claims}, nil
			},
		}
		publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			applyCalls++
			if applyCalls == 1 {
				return TrafficMapPublishResult{}, errors.New("partial backend replay")
			}
			return TrafficMapPublishResult{}, nil
		}
		return publisher
	}
	firstPublisher := newPublisher()
	first := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: firstPublisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, first, trafficMap)
	require.ErrorContains(t, err, "partial backend replay")
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, claims, current.Status.Publisher.ClaimedTargets)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive,
		"a failed partial replay must retain the complete retry journal")
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
	restoreEligiblePublisherRoutingConditions(t, kubeClient, current)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)

	secondPublisher := newPublisher()
	restarted := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: secondPublisher, Active: true,
	}
	_, err = reconcileTrafficMapPublisher(t, restarted, current)
	require.NoError(t, err)
	require.Len(t, replayed, 2)
	assert.Equal(t, *lastPositive, replayed[0])
	assert.Equal(t, *lastPositive, replayed[1])
	assert.Equal(t, 2, applyCalls)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisherRestartReplaysAfterFallbackSuccessStatusFailure(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	failingClient := &failNthStatusApplyClient{
		Client: baseClient, failAt: 2, err: errors.New("lost fallback success status"),
	}
	replayCalls := 0
	applyCalls := 0
	newPublisher := func() *fallbackTrafficMapPublisher {
		base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
		publisher := &fallbackTrafficMapPublisher{
			recordingTrafficMapPublisher: base,
			capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
				return TrafficMapPublishPlanCapture{
					PlanCompatibilityDigest: testPublisherDigest("a"),
					NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
				}, nil
			},
			replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
				replayCalls++
				assert.Equal(t, *lastPositive, snapshot)
				return testTrafficMapPublishPlan{claims: claims}, nil
			},
		}
		publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			applyCalls++
			return TrafficMapPublishResult{}, nil
		}
		return publisher
	}
	first := &TrafficMapPublisherReconciler{
		Client: failingClient, APIReader: baseClient, Log: logr.Discard(), Publisher: newPublisher(), Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, first, trafficMap)
	require.ErrorContains(t, err, "lost fallback success status")
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.False(t, current.Status.Published)
	assert.Equal(t, metav1.ConditionUnknown, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
	restoreEligiblePublisherRoutingConditions(t, baseClient, current)
	current = getPublisherTrafficMap(t, baseClient, trafficMap)

	restarted := &TrafficMapPublisherReconciler{
		Client: baseClient, APIReader: baseClient, Log: logr.Discard(), Publisher: newPublisher(), Active: true,
	}
	_, err = reconcileTrafficMapPublisher(t, restarted, current)
	require.NoError(t, err)
	assert.Equal(t, 2, replayCalls, "restart must reconstruct and replay the full snapshot")
	assert.Equal(t, 2, applyCalls, "a lost success status must produce an idempotent full reapply")
	current = getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisherDoesNotConfirmDifferentSnapshotAfterReplay(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			return testTrafficMapPublishPlan{claims: claims}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		live := getPublisherTrafficMap(t, kubeClient, trafficMap)
		live.Status.Publisher.LastPositive.Targets[0].Weight = 99
		require.NoError(t, kubeClient.Status().Update(context.Background(), live))
		return TrafficMapPublishResult{}, nil
	}

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, int64(99), current.Status.Publisher.LastPositive.Targets[0].Weight)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.NotEqual(t, metav1.ConditionTrue, fallback.Status,
		"status must not confirm a snapshot other than the one actually replayed")
}

func TestTrafficMapPublisherRejectsPositiveCaptureReportedAsWithdrawn(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			return TrafficMapPublishResult{Withdrawn: true}, nil
		},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyApplyCurrent,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "does not match captured plan withdrawal")
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Nil(t, current.Status.Publisher.LastPositive,
		"a positive plan that was not applied as captured must never be journaled")
	assert.False(t, current.Status.Published)
}

func TestTrafficMapPublisherPreflightRunsAfterPreclaimBeforeEffects(t *testing.T) {
	tests := []struct {
		name      string
		preflight error
		wantError bool
	}{
		{
			name:      "terminal rejection",
			preflight: &terminalPublisherError{err: errors.New("hostname collision")},
		},
		{
			name:      "retryable read failure",
			preflight: errors.New("live route list failed"),
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"retired"},
			}
			base := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target-a"},
			}
			publisher := &preflightingTrafficMapPublisher{recordingTrafficMapPublisher: base}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			publisher.preflight = func(_ context.Context, plan TrafficMapPublishPlan) error {
				assert.Equal(t, []string{"target-a"}, plan.Claims())
				current := getPublisherTrafficMap(t, kubeClient, trafficMap)
				require.NotNil(t, current.Status.Publisher)
				assert.Equal(t, []string{"retired", "target-a"}, current.Status.Publisher.ClaimedTargets,
					"the durable claim must precede preflight")
				return tt.preflight
			}

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			if tt.wantError {
				require.ErrorContains(t, err, tt.preflight.Error())
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []string{"preflight"}, publisher.events,
				"a failed preflight must precede and prevent Drain and Apply")

			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, trafficMapPublisherReasonClaimRejected, condition.Reason)
		})
	}
}

func TestTrafficMapPublisherWithdrawnResultRecordsConvergence(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.GatewayRef = &v1beta1.TrafficMapGatewayRef{
		Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "old",
	}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration: 2, ObservedISVCGeneration: 3,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target", Weight: 37}},
		},
	}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			return TrafficMapPublishResult{
				GatewayRef: &v1beta1.TrafficMapGatewayRef{
					Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "ignored",
				},
				Withdrawn: true,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Published)
	assert.Nil(t, current.Status.GatewayRef)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets,
		"successful withdrawal retains the durable claim until finalization")
	assert.Nil(t, current.Status.Publisher.LastPositive)
	assert.NotEmpty(t, current.Status.Publisher.ObservedOptionsDigest)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, trafficMapPublisherReasonWithdrawn, published.Reason)
	assert.Equal(t, trafficMap.Generation, published.ObservedGeneration)
}

func TestTrafficMapPublisherCapturedWithdrawalClearsSnapshotBeforeEffects(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
		},
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
				Withdrawn:               true,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.Nil(t, current.Status.Publisher.LastPositive)
		assert.False(t, current.Status.Published)
		return TrafficMapPublishResult{Withdrawn: true}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:target-a", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher.LastPositive)
	assert.False(t, current.Status.Published)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonWithdrawn, fallback.Reason)
}

func TestTrafficMapPublisherRejectsUnsafeClaimsBeforeEffects(t *testing.T) {
	tests := []struct {
		name       string
		claims     []string
		journal    *v1beta1.TrafficMapPublisherStatus
		other      *v1beta1.TrafficMap
		wantReason string
	}{
		{
			name: "duplicate plan claims", claims: []string{"same", "same"},
			wantReason: trafficMapPublisherReasonInvalidPlan,
		},
		{
			name: "oversized target", claims: []string{strings.Repeat("x", v1beta1.MaxTrafficMapPublisherTargetLength+1)},
			wantReason: trafficMapPublisherReasonInvalidPlan,
		},
		{
			name: "publisher mismatch", claims: []string{"new"},
			journal:    &v1beta1.TrafficMapPublisherStatus{PublisherName: "old-publisher", ClaimedTargets: []string{"old"}},
			wantReason: trafficMapPublisherReasonPublisherChanged,
		},
		{
			name: "publisher mismatch with snapshot but no claims", claims: []string{"new"},
			journal: &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "old-publisher",
				LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
					TrafficMapGeneration: 1, ObservedISVCGeneration: 1,
					PlanCompatibilityDigest: testPublisherDigest("a"),
					Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "old", Weight: 1}},
				},
			},
			wantReason: trafficMapPublisherReasonPublisherChanged,
		},
		{
			name: "claim collision", claims: []string{"shared"},
			other:      publisherClaimedTrafficMap("other", "example-publisher", []string{"shared"}),
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
		{
			name:       "global publisher switch with no desired claim",
			other:      publisherClaimedTrafficMap("other", "different-publisher", []string{"theirs"}),
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
		{
			name: "claim union over bound", claims: []string{"new"},
			journal: &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: numberedClaims(v1beta1.MaxTrafficMapPublisherTargets),
			},
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = tt.journal
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: tt.claims,
			}
			objects := []client.Object{owner, trafficMap}
			if tt.other != nil {
				objects = append(objects, tt.other)
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, objects...)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err, "invalid desired state is reported in status without a retry hot-loop")
			assert.Empty(t, publisher.events, "rejected plans must have no external effect")

			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			if tt.journal == nil {
				assert.Nil(t, current.Status.Publisher)
			} else {
				assert.Equal(t, tt.journal, current.Status.Publisher)
			}
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, tt.wantReason, condition.Reason)
			assert.Equal(t, trafficMapPublisherFailureMessage(tt.wantReason), condition.Message)
		})
	}
}

func TestTrafficMapPublisherClaimArbitrationUsesUncachedState(t *testing.T) {
	ownerA, trafficMapA := publisherTestObjects()
	trafficMapA.Finalizers = []string{TrafficMapPublisherFinalizer}

	ownerB := ownerA.DeepCopy()
	ownerB.Name = "model-b"
	ownerB.UID = types.UID("owner-b-uid")
	trafficMapB := trafficMapA.DeepCopy()
	trafficMapB.Name = ownerB.Name
	trafficMapB.UID = types.UID("map-b-uid")
	trafficMapB.Spec.Service = ownerB.Name
	trafficMapB.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
		Name: ownerB.Name, UID: ownerB.UID, Controller: trafficMapA.OwnerReferences[0].Controller,
	}}
	trafficMapB.Status.SourceUID = ownerB.UID

	liveClient := newTrafficMapPublisherClient(t, ownerA, trafficMapA, ownerB, trafficMapB)
	cacheClient := &frozenTrafficMapCacheClient{
		Client: liveClient,
		trafficMaps: map[types.NamespacedName]*v1beta1.TrafficMap{
			client.ObjectKeyFromObject(trafficMapA): trafficMapA.DeepCopy(),
			client.ObjectKeyFromObject(trafficMapB): trafficMapB.DeepCopy(),
		},
	}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"shared-target"},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: cacheClient, APIReader: liveClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMapA)
	require.NoError(t, err)
	require.Equal(t, []string{"apply"}, publisher.events)
	claimed := getPublisherTrafficMap(t, liveClient, trafficMapA)
	require.NotNil(t, claimed.Status.Publisher)
	assert.Equal(t, []string{"shared-target"}, claimed.Status.Publisher.ClaimedTargets)

	publisher.events = nil
	_, err = reconcileTrafficMapPublisher(t, reconciler, trafficMapB)
	require.NoError(t, err, "the conflicting claim is a reported terminal rejection")
	assert.Empty(t, publisher.events, "the second map must not mutate the shared target")

	rejected := getPublisherTrafficMap(t, liveClient, trafficMapB)
	assert.Nil(t, rejected.Status.Publisher, "a rejected map must not acquire the live claim")
	published := apimeta.FindStatusCondition(rejected.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, trafficMapPublisherReasonClaimRejected, published.Reason)
	assert.Equal(t, 2, cacheClient.trafficMapGets,
		"both reconcile inputs came from the frozen informer view")
}

func TestTrafficMapPublisherRejectsInvalidOptionsWithoutEffects(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		resolve: func(map[string]string, map[string]string) (map[string]string, error) {
			return nil, errors.New("protected option is not allowed")
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.NotContains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Nil(t, current.Status.Publisher)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOptions, condition.Reason)
	assert.NotContains(t, condition.Message, "protected option")
}

func TestTrafficMapPublisherValidatesOwnerIdentityBeforeOptionsOrPlan(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.InferenceService, *v1beta1.TrafficMap)
	}{
		{
			name: "map name",
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Name = "different"
			},
		},
		{
			name: "service name",
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Service = "different"
			},
		},
		{
			name: "observed owner generation",
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Generation++
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			tt.mutate(owner, trafficMap)
			resolveCalls := 0
			planCalls := 0
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true,
				resolve: func(map[string]string, map[string]string) (map[string]string, error) {
					resolveCalls++
					return nil, nil
				},
				plan: func(*v1beta1.InferenceService, *v1beta1.TrafficMap, map[string]string) (TrafficMapPublishPlan, error) {
					planCalls++
					return testTrafficMapPublishPlan{}, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			assert.Zero(t, resolveCalls)
			assert.Zero(t, planCalls)
			assert.Empty(t, publisher.events)
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
		})
	}
}

func TestTrafficMapPublisherRepairsFinalizerBeforeRejectingExistingClaims(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	owner.Generation++
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
	}
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Empty(t, publisher.events)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
}

func TestStatefulTrafficMapPublisherRepairsFinalizerForZeroClaimState(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	owner.Generation++
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher",
	}
	trafficMap.Status.Published = true
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Empty(t, publisher.events)
}

func TestTrafficMapStatelessPublisherCannotReturnClaims(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: false, claims: []string{"unexpected"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidPlan, condition.Reason)
	assert.Equal(t, trafficMapPublisherFailureMessage(trafficMapPublisherReasonInvalidPlan), condition.Message)
}

func TestTrafficMapStatelessPublisherHonorsGlobalSwitchFence(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	other := publisherClaimedTrafficMap("other", "different-publisher", []string{"their-target"})
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: false}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap, other)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonClaimRejected, condition.Reason)
}

func TestTrafficMapPublisherSkipsWhenInactiveOrOwnerStops(t *testing.T) {
	falseValue := false
	tests := []struct {
		name            string
		active          bool
		ownerOptedOut   bool
		ownerDeleting   bool
		ownerIneligible bool
	}{
		{name: "installation disabled"},
		{name: "owner opted out", active: true, ownerOptedOut: true},
		{name: "owner deleting", active: true, ownerDeleting: true},
		{name: "owner placement ineligible", active: true, ownerIneligible: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			if tt.ownerOptedOut {
				owner.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &falseValue}
			}
			if tt.ownerDeleting {
				now := metav1.Now()
				owner.DeletionTimestamp = &now
				owner.Finalizers = []string{"example.com/hold"}
			}
			if tt.ownerIneligible {
				owner.Spec.Placement = nil
			}
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
			}
			if tt.ownerOptedOut {
				trafficMap.Status.Publisher.LastPositive = &v1beta1.TrafficMapPublisherLastPositive{
					TrafficMapGeneration:    1,
					ObservedISVCGeneration:  owner.Generation,
					PlanCompatibilityDigest: testPublisherDigest("a"),
					Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target", Weight: 1}},
				}
			}
			planCalls := 0
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target"},
				plan: func(
					*v1beta1.InferenceService,
					*v1beta1.TrafficMap,
					map[string]string,
				) (TrafficMapPublishPlan, error) {
					planCalls++
					return testTrafficMapPublishPlan{claims: []string{"target"}}, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			reconciler.Active = tt.active

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			assert.Zero(t, planCalls)
			assert.Empty(t, publisher.events, "deletion is requested before publisher effects")
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.True(t, current.DeletionTimestamp.IsZero(), "the routing controller owns map deletion")
			assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
			if tt.ownerOptedOut {
				assert.Nil(t, current.Status.Publisher.LastPositive,
					"the owning publisher clears its snapshot when the source opts out")
			}
		})
	}
}

func TestTrafficMapPublisherInactiveOwnerCannotEraseForeignSnapshot(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	falseValue := false
	owner.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &falseValue}
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	foreign := &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "other-publisher",
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "foreign-target", Weight: 1}},
		},
	}
	trafficMap.Status.Publisher = copyTrafficMapPublisherJournal(foreign)
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, foreign, current.Status.Publisher,
		"an inactive newly selected publisher must not clear or adopt another publisher's snapshot")
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, trafficMapPublisherReasonPublisherChanged, published.Reason)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
}

func TestTrafficMapPublisherApplyFailureKeepsPreclaimForRetry(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	attempts := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			attempts++
			if attempts == 1 {
				return TrafficMapPublishResult{}, errors.New("temporary backend failure")
			}
			return TrafficMapPublishResult{}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
	assert.Empty(t, current.Status.Publisher.ObservedOptionsDigest)
	assert.False(t, current.Status.Published)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonApplyFailed, condition.Reason)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Published)
	assert.NotEmpty(t, current.Status.Publisher.ObservedOptionsDigest)
	assert.Equal(t, 2, attempts)
}

func TestTrafficMapPublisherDrainFailureKeepsClaimsForRetry(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"retired"},
	}
	drainAttempts := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"current"},
		drain: func(_ context.Context, key types.NamespacedName, _ []string) error {
			assert.Equal(t, client.ObjectKeyFromObject(trafficMap), key)
			drainAttempts++
			if drainAttempts == 1 {
				return errors.New("temporary drain failure")
			}
			return nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"current", "retired"}, current.Status.Publisher.ClaimedTargets)
	assert.Equal(t, []string{"drain:retired"}, publisher.events)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonDrainFailed, condition.Reason)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:retired", "drain:retired", "apply"}, publisher.events)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Published)
}

func TestTrafficMapPublisherDoesNotRecreateLostJournalAfterApply(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		live := getPublisherTrafficMap(t, kubeClient, trafficMap)
		live.Status.Publisher = nil
		require.NoError(t, kubeClient.Status().Update(context.Background(), live))
		return TrafficMapPublishResult{GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Kind: "HTTPRoute", Name: "model",
		}}, nil
	}

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher)
	assert.False(t, current.Status.Published)
	assert.Nil(t, current.Status.GatewayRef)
}

func TestTrafficMapPublisherRetriesAfterSuccessStatusWriteFails(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	wrapped := &failNthStatusApplyClient{
		Client: baseClient, failAt: 2, err: errors.New("injected status write failure"),
	}
	applyCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			applyCalls++
			return TrafficMapPublishResult{}, nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: wrapped, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "injected status write failure")
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
	assert.False(t, current.Status.Published)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, 2, applyCalls, "retry must idempotently reapply after a lost success-status write")
	current = getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.True(t, current.Status.Published)
}

func TestTrafficMapPublisherFinalizesWhenInactiveWithoutOwner(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	trafficMap.Status.SourceUID = ""
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"a", "b"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    1,
			ObservedISVCGeneration:  1,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "a", Weight: 1}, {Target: "b"}},
		},
	}
	fail := true
	var gotKey types.NamespacedName
	var gotJournal v1beta1.TrafficMapPublisherStatus
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, key types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			gotKey = key
			gotJournal = journal
			assert.Nil(t, journal.LastPositive, "finalization must clear fallback state before cleanup")
			if fail {
				return errors.New("temporary cleanup failure")
			}
			return nil
		},
	}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap, legacyOwner)
	reconciler.Active = false

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	assert.Equal(t, client.ObjectKeyFromObject(trafficMap), gotKey)
	assert.Equal(t, []string{"a", "b"}, gotJournal.ClaimedTargets)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Equal(t, []string{"a", "b"}, current.Status.Publisher.ClaimedTargets)
	assert.Nil(t, current.Status.Publisher.LastPositive)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonPublicationFinalizing, fallback.Reason,
		"failed cleanup must remain in its durable finalizing state for retry")

	fail = false
	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	got := &v1beta1.TrafficMap{}
	err = kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), got)
	assert.True(t, apierrors.IsNotFound(err), "removing the last finalizer completes direct deletion")
}

func TestTrafficMapPublisherFinalizationIgnoresSourceUID(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sourceUID types.UID
	}{
		{name: "missing"},
		{name: "mismatched", sourceUID: "other-owner-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, trafficMap := publisherTestObjects()
			now := metav1.Now()
			trafficMap.DeletionTimestamp = &now
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.SourceUID = tc.sourceUID
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
			}
			publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

			require.NoError(t, err)
			assert.Equal(t, []string{"unpublish:target"}, publisher.events)
			err = kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), &v1beta1.TrafficMap{})
			assert.True(t, apierrors.IsNotFound(err), "journal cleanup must remain owner-independent")
		})
	}
}

func TestTrafficMapPublisherBlocksCleanupForMismatchedJournal(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "old-publisher", ClaimedTargets: []string{"target"},
	}
	publisher := &recordingTrafficMapPublisher{name: "new-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, "old-publisher", current.Status.Publisher.PublisherName)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonPublisherChanged, condition.Reason)
}

func TestTrafficMapPublisherFinalizationUsesUncachedJournal(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"live-claim"},
	}
	baseClient := newTrafficMapPublisherClient(t, trafficMap)
	stale := trafficMap.DeepCopy()
	stale.Status.Publisher = nil
	cacheClient := &staleTrafficMapClient{Client: baseClient, stale: stale}
	var gotClaims []string
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, _ types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			gotClaims = append([]string(nil), journal.ClaimedTargets...)
			return nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: cacheClient, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"live-claim"}, gotClaims)
}

func TestTrafficMapPublisherFinalizationRechecksJournalAfterUnpublish(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"a"},
	}

	var kubeClient client.Client
	var unpublished [][]string
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, _ types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			unpublished = append(unpublished, append([]string(nil), journal.ClaimedTargets...))
			journal.ClaimedTargets[0] = "backend-mutated"
			if len(unpublished) == 1 {
				live := getPublisherTrafficMap(t, kubeClient, trafficMap)
				live.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
					PublisherName: "example-publisher", ClaimedTargets: []string{"a", "b"},
				}
				require.NoError(t, kubeClient.Status().Update(context.Background(), live))
			}
			return nil
		},
	}
	reconciler, clientForTest := newTrafficMapPublisherReconciler(t, publisher, trafficMap)
	kubeClient = clientForTest

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"a", "b"}, current.Status.Publisher.ClaimedTargets)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"a"}, {"a", "b"}}, unpublished)
	got := &v1beta1.TrafficMap{}
	err = kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), got)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestClearPublisherJournalClearsPublisherOwnedStatus(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	desiredStatus := v1beta1.TrafficMapStatus{
		Published:                    true,
		ObservedTrafficMapGeneration: trafficMap.Generation,
		GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Group: "networking.example.com", Kind: "GlobalRoute", Name: "model",
		},
		Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
		},
		Conditions: []metav1.Condition{{
			Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue, Reason: "Published",
		}},
	}
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap)
	live := getPublisherTrafficMap(t, kubeClient, trafficMap)
	live.Status = desiredStatus
	require.NoError(t, reconciler.applyPublisherStatus(context.Background(), live))
	live = getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, live.Status.Publisher)

	require.NoError(t, reconciler.clearPublisherJournal(
		context.Background(), client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
		*live.Status.Publisher.DeepCopy(),
	))
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Published)
	assert.Zero(t, current.Status.ObservedTrafficMapGeneration)
	assert.Nil(t, current.Status.GatewayRef)
	assert.Nil(t, current.Status.Publisher)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, trafficMapPublisherReasonUnpublished, published.Reason)
}

func TestTrafficMapPublisherStatusApplyRetriesOnConflict(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	wrapped := &conflictOnceClient{Client: baseClient}
	wrapped.beforeConflict = func() {
		live := getPublisherTrafficMap(t, baseClient, trafficMap)
		apimeta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
			Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
			Reason: "ConcurrentRoutingWrite", ObservedGeneration: live.Generation,
		})
		require.NoError(t, baseClient.Status().Update(context.Background(), live))
	}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: wrapped, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.True(t, wrapped.conflicted)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionTrue, published.Status)
}

func TestTrafficMapPublisherStatusApplyIsFieldIsolated(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	trafficMap.ResourceVersion = "17"
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration: 2, ObservedISVCGeneration: 3,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target", Weight: 37}},
		},
	}
	trafficMap.Status.GatewayRef = &v1beta1.TrafficMapGatewayRef{Kind: "HTTPRoute", Name: "model"}
	trafficMap.Status.Conditions = []metav1.Condition{
		{Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue, Reason: "Routable"},
		{Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue, Reason: "Published"},
		{Type: v1beta1.TrafficMapPublicationFallback, Status: metav1.ConditionFalse, Reason: "CurrentPlanApplied"},
	}

	content := publisherApplyContent(t, trafficMapPublisherStatusApply(trafficMap))
	metadata := content["metadata"].(map[string]any)
	assert.Equal(t, "17", metadata["resourceVersion"])
	status := content["status"].(map[string]any)
	_, found := status["sourceUID"]
	assert.False(t, found, "publisher SSA must not own routing source provenance")
	publisher := status["publisher"].(map[string]any)
	lastPositive := publisher["lastPositive"].(map[string]any)
	assert.Equal(t, int64(2), lastPositive["trafficMapGeneration"])
	targets := lastPositive["targets"].([]any)
	require.Len(t, targets, 1)
	assert.Equal(t, int64(37), targets[0].(map[string]any)["weight"])
	conditions := status["conditions"].([]any)
	require.Len(t, conditions, 2)
	assert.Equal(t, v1beta1.TrafficMapPublished, conditions[0].(map[string]any)["type"])
	assert.Equal(t, v1beta1.TrafficMapPublicationFallback, conditions[1].(map[string]any)["type"])

	trafficMap.Status.Publisher = nil
	trafficMap.Status.GatewayRef = nil
	trafficMap.Status.Published = false
	trafficMap.Status.ObservedTrafficMapGeneration = 0
	content = publisherApplyContent(t, trafficMapPublisherStatusApply(trafficMap))
	status = content["status"].(map[string]any)
	_, found = status["publisher"]
	assert.False(t, found, "owned pointer fields are removed by SSA omission")
	_, found = status["gatewayRef"]
	assert.False(t, found, "owned pointer fields are removed by SSA omission")
	assert.Equal(t, false, status["published"])
	assert.Equal(t, int64(0), status["observedTrafficMapGeneration"])
}

func TestTrafficMapPublisherHelpers(t *testing.T) {
	first, err := publisherOptionsDigest("publisher-a", map[string]string{"b": "2", "a": "1"})
	require.NoError(t, err)
	second, err := publisherOptionsDigest("publisher-a", map[string]string{"a": "1", "b": "2"})
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, first)
	otherPublisher, err := publisherOptionsDigest("publisher-b", map[string]string{"a": "1", "b": "2"})
	require.NoError(t, err)
	assert.NotEqual(t, first, otherPublisher)

	options := trafficMapPublisherControllerOptions()
	assert.Equal(t, 1, options.MaxConcurrentReconciles)
	require.NotNil(t, options.NeedLeaderElection)
	assert.True(t, *options.NeedLeaderElection)
	requests := enqueueOwnedTrafficMap(context.Background(), &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"},
	})
	require.Len(t, requests, 1)
	assert.Equal(t, types.NamespacedName{Namespace: "team-a", Name: "model"}, requests[0].NamespacedName)
	assert.True(t, publisherClaimsReplayCompatible(nil, []string{"a", "b"}),
		"the factual empty plan is the eligible-loss exception")
	assert.True(t, publisherClaimsReplayCompatible([]string{"a", "b"}, []string{"a", "b"}))
	assert.False(t, publisherClaimsReplayCompatible([]string{"a"}, []string{"a", "b"}),
		"a nonempty strict subset is a claim-set change")
}

func TestTrafficMapFallbackEligibilityAllowlist(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*v1beta1.InferenceService, *v1beta1.TrafficMap)
		eligible bool
		known    bool
	}{
		{name: "single last winner loss", eligible: true, known: true},
		{
			name: "placed ready replica without address", eligible: true, known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", ReadyReplicas: 1,
					}},
				}
				routable := apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				)
				routable.Reason = v1beta1.TrafficMapReasonNoAddressableHome
			},
		},
		{
			name: "stale source observation", known: false,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Status.ObservedGeneration--
			},
		},
		{
			name: "stale routing verdict", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).ObservedGeneration--
			},
		},
		{
			name: "missing routable verdict", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.RemoveStatusCondition(&trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable)
			},
		},
		{
			name: "missing override verdict", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.RemoveStatusCondition(
					&trafficMap.Status.Conditions, v1beta1.TrafficMapOverrideActive,
				)
			},
		},
		{
			name: "stale override verdict", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapOverrideActive,
				).ObservedGeneration--
			},
		},
		{
			name: "manual drain annotation", known: true,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Annotations = map[string]string{constants.TrafficDrainAnnotation: ""}
			},
		},
		{
			name: "override pending with annotation intent", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Annotations = map[string]string{constants.TrafficDrainAnnotation: "malformed"}
				override := apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapOverrideActive,
				)
				override.Status = metav1.ConditionUnknown
				override.Reason = v1beta1.TrafficMapReasonOverridesPending
			},
		},
		{
			name: "annotation removal precedes override status", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				override := apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapOverrideActive,
				)
				override.Status = metav1.ConditionTrue
				override.Reason = v1beta1.TrafficMapReasonOverridesApplied
			},
		},
		{
			name: "manual drain intent overrides missing routing status", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Annotations = map[string]string{constants.TrafficDrainAnnotation: "{"}
				trafficMap.Status.Conditions = nil
			},
		},
		{
			name: "manual drain intent overrides stale placement observation", known: true,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Annotations = map[string]string{constants.TrafficDrainAnnotation: "{}"}
				owner.Status.ObservedGeneration--
			},
		},
		{
			name: "all homes unready", known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonAllHomesUnready
			},
		},
		{
			name: "no routable capacity", known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonNoRoutableCapacity
			},
		},
		{
			name: "all homes probe failed", known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonAllHomesProbeFailed
			},
		},
		{
			name: "traffic drain verdict", known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonTrafficDrain
			},
		},
		{
			name: "manual drain overrides placement unknown", known: true,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
				owner.Annotations = map[string]string{constants.TrafficDrainAnnotation: "{}"}
			},
		},
		{
			name: "no routable capacity overrides placement unknown", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonNoRoutableCapacity
			},
		},
		{
			name: "probe failure overrides placement unknown", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonAllHomesProbeFailed
			},
		},
		{
			name: "traffic drain overrides placement unknown", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonTrafficDrain
			},
		},
		{
			name: "initial placement", known: true,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Status.Placement = nil
			},
		},
		{
			name: "admitting placement", known: true,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
			},
		},
		{
			name: "failed placement", known: true,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Status.Placement.Phase = v1beta1.PlacementPhaseFailed
			},
		},
		{
			name: "mode mismatch", known: false,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Spec.Placement.Mode = v1beta1.PlacementModeAll
			},
		},
		{
			name: "non-single total loss", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Spec.Placement.Mode = v1beta1.PlacementModeAll
				trafficMap.Spec.Mode = v1beta1.PlacementModeAll
			},
		},
		{
			name: "placement unknown during single-home loss", known: false,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
			},
		},
		{
			name: "placement unknown during no-address observation", known: false,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", ReadyReplicas: 0,
					}},
				}
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonNoAddressableHome
			},
		},
		{
			name: "no-address reason lags pending placement unknown", known: false,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonNoAddressableHome
			},
		},
		{
			name: "placement unknown during all-homes-unready observation", known: false,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Placement = &v1beta1.PlacementStatus{Phase: v1beta1.PlacementPhasePlaced}
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonAllHomesUnready
			},
		},
		{
			name: "definitive all-homes-unready observation", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Placement = &v1beta1.PlacementStatus{Phase: v1beta1.PlacementPhasePlaced}
				owner.Status.Conditions[0].Status = corev1.ConditionFalse
				owner.Status.Conditions[0].Reason = "PlacementNotReady"
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonAllHomesUnready
			},
		},
		{
			name: "placed without ready replicas", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", ReadyReplicas: 0,
					}},
				}
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonNoAddressableHome
			},
		},
		{
			name: "owner recovered an address before routing status", known: false,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				endpoint, err := apis.ParseURL("https://model.example.com")
				require.NoError(t, err)
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitted,
						ReadyReplicas: 1, Endpoint: endpoint,
					}},
				}
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonNoAddressableHome
			},
		},
		{
			name: "not-placed reason lags recovered address", known: false,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				endpoint, err := apis.ParseURL("https://model.example.com")
				require.NoError(t, err)
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitted,
						ReadyReplicas: 1, Endpoint: endpoint,
					}},
				}
			},
		},
		{
			name: "inconsistent nonempty routing table", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries = []v1beta1.TrafficMapEntry{{Cluster: "cluster-a"}}
			},
		},
		{
			name: "contradictory routable true", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Status = metav1.ConditionTrue
			},
		},
		{
			name: "unclassifiable routable unknown", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Status = metav1.ConditionUnknown
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := eligiblePlacementLossPublisherObjects()
			if tt.mutate != nil {
				tt.mutate(owner, trafficMap)
			}
			eligible, known := trafficMapFallbackEligibility(trafficMap, owner)
			assert.Equal(t, tt.eligible, eligible)
			assert.Equal(t, tt.known, known)
		})
	}
}

func TestTrafficMapFallbackEligibilityAllHomesUnreadyExtension(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*v1beta1.InferenceService, *v1beta1.TrafficMap)
		allow    bool
		eligible bool
		known    bool
	}{
		{name: "definitive all-homes-unready", allow: true, eligible: true, known: true},
		{
			name: "publisher does not opt in", known: true,
		},
		{
			name: "placement observation unknown", allow: true, known: false,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Status.Conditions[0].Status = corev1.ConditionUnknown
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
			},
		},
		{
			name: "manual drain", allow: true, known: true,
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Annotations = map[string]string{constants.TrafficDrainAnnotation: "{}"}
			},
		},
		{
			name: "empty routing table", allow: true, known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries = nil
			},
		},
		{
			name: "manual drain provenance", allow: true, known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries[0].DrainRefs = []string{"maintenance"}
			},
		},
		{
			name: "probe gate", allow: true, known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries[0].Probe = &v1beta1.TrafficMapProbe{Gated: true}
			},
		},
		{
			name: "zero allocated capacity", allow: true, known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries[0].Capacity.Allocated = 0
			},
		},
		{
			name: "missing capacity provenance", allow: true, known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries[0].Capacity = nil
			},
		},
		{
			name: "contradictory ready capacity", allow: true, known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries[0].Capacity.Ready = 1
			},
		},
		{
			name: "positive route weight", allow: true, known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries[0].Weight = 1
			},
		},
		{
			name: "healthy route arm", allow: true, known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Entries[0].Healthy = true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := allHomesUnreadyPublisherObjects()
			if tt.mutate != nil {
				tt.mutate(owner, trafficMap)
			}
			eligible, known := trafficMapFallbackEligibilityWithAllHomesUnready(
				trafficMap, owner, tt.allow,
			)
			assert.Equal(t, tt.eligible, eligible)
			assert.Equal(t, tt.known, known)
		})
	}
}

func TestValidateTrafficMapLastPositive(t *testing.T) {
	valid := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    3,
		ObservedISVCGeneration:  7,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "a", Weight: 1},
			{Target: "b", Weight: 0},
		},
	}
	require.NoError(t, validateTrafficMapLastPositive(
		valid, []string{"a", "b"}, 7, testPublisherDigest("a"), 4,
	))

	tests := []struct {
		name   string
		mutate func(*v1beta1.TrafficMapPublisherLastPositive)
		claims []string
	}{
		{name: "missing", mutate: func(snapshot *v1beta1.TrafficMapPublisherLastPositive) {
			*snapshot = v1beta1.TrafficMapPublisherLastPositive{}
		}},
		{name: "future generation", mutate: func(snapshot *v1beta1.TrafficMapPublisherLastPositive) { snapshot.TrafficMapGeneration = 5 }},
		{name: "source generation", mutate: func(snapshot *v1beta1.TrafficMapPublisherLastPositive) { snapshot.ObservedISVCGeneration = 8 }},
		{name: "malformed digest", mutate: func(snapshot *v1beta1.TrafficMapPublisherLastPositive) { snapshot.PlanCompatibilityDigest = "bad" }},
		{name: "all zero", mutate: func(snapshot *v1beta1.TrafficMapPublisherLastPositive) { snapshot.Targets[0].Weight = 0 }},
		{name: "negative", mutate: func(snapshot *v1beta1.TrafficMapPublisherLastPositive) { snapshot.Targets[0].Weight = -1 }},
		{name: "duplicate", mutate: func(snapshot *v1beta1.TrafficMapPublisherLastPositive) { snapshot.Targets[1].Target = "a" }},
		{name: "claim mismatch", claims: []string{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := copyTrafficMapLastPositive(valid)
			if tt.mutate != nil {
				tt.mutate(snapshot)
			}
			claims := tt.claims
			if claims == nil {
				claims = []string{"a", "b"}
			}
			assert.Error(t, validateTrafficMapLastPositive(
				snapshot, claims, 7, testPublisherDigest("a"), 4,
			))
		})
	}
}

func TestValidateTrafficMapLastPositiveFromEarlierGeneration(t *testing.T) {
	valid := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    3,
		ObservedISVCGeneration:  6,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "a", Weight: 1}},
	}
	require.NoError(t, validateTrafficMapLastPositiveFromEarlierGeneration(
		valid, []string{"a"}, 7, testPublisherDigest("a"), 4,
	))
	assert.Error(t, validateTrafficMapLastPositive(
		valid, []string{"a"}, 7, testPublisherDigest("a"), 4,
	), "the generic fallback must continue to require the current source generation")

	future := copyTrafficMapLastPositive(valid)
	future.ObservedISVCGeneration = 8
	assert.Error(t, validateTrafficMapLastPositiveFromEarlierGeneration(
		future, []string{"a"}, 7, testPublisherDigest("a"), 4,
	))
}

func TestPublisherClaimsReplayCompatibleForAllHomesUnready(t *testing.T) {
	snapshot := &v1beta1.TrafficMapPublisherLastPositive{Targets: []v1beta1.TrafficMapPublisherTarget{
		{Target: "a", Weight: 75},
		{Target: "b", Weight: 25},
		{Target: "c", Weight: 0},
	}}
	tests := []struct {
		name    string
		current []string
		want    bool
	}{
		{name: "exact claims", current: []string{"a", "b", "c"}, want: true},
		{name: "omits zero target", current: []string{"a", "b"}, want: true},
		{name: "omits positive target", current: []string{"a", "c"}},
		{name: "adds target", current: []string{"a", "b", "c", "d"}},
		{name: "empty current claims"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, publisherClaimsReplayCompatibleForAllHomesUnready(
				tt.current, []string{"a", "b", "c"}, snapshot,
			))
		})
	}
}

func TestValidateTrafficMapPlanCapture(t *testing.T) {
	valid := TrafficMapPublishPlanCapture{
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "b", Weight: 0},
			{Target: "a", Weight: 1},
		},
		PlanCompatibilityDigest: testPublisherDigest("a"),
		NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyApplyCurrent,
	}
	require.NoError(t, validateTrafficMapPlanCapture(&valid, []string{"a", "b"}))
	assert.Equal(t, []v1beta1.TrafficMapPublisherTarget{
		{Target: "a", Weight: 1},
		{Target: "b", Weight: 0},
	}, valid.Targets)

	tests := []struct {
		name   string
		mutate func(*TrafficMapPublishPlanCapture)
	}{
		{
			name: "unknown policy",
			mutate: func(capture *TrafficMapPublishPlanCapture) {
				capture.NoPositivePlanPolicy = "Unknown"
			},
		},
		{
			name: "malformed digest",
			mutate: func(capture *TrafficMapPublishPlanCapture) {
				capture.PlanCompatibilityDigest = "sha256:abc"
			},
		},
		{
			name: "negative weight",
			mutate: func(capture *TrafficMapPublishPlanCapture) {
				capture.Targets[0].Weight = -1
			},
		},
		{
			name: "duplicate target",
			mutate: func(capture *TrafficMapPublishPlanCapture) {
				capture.Targets[1].Target = "a"
			},
		},
		{
			name: "withdrawn positive plan",
			mutate: func(capture *TrafficMapPublishPlanCapture) {
				capture.Withdrawn = true
			},
		},
		{
			name: "AllHomesUnready fallback with apply-current policy",
			mutate: func(capture *TrafficMapPublishPlanCapture) {
				capture.AllowAllHomesUnreadyFallback = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := valid
			capture.Targets = slices.Clone(valid.Targets)
			tt.mutate(&capture)
			assert.Error(t, validateTrafficMapPlanCapture(&capture, []string{"a", "b"}))
		})
	}

	mismatch := valid
	mismatch.Targets = slices.Clone(valid.Targets)
	assert.Error(t, validateTrafficMapPlanCapture(&mismatch, []string{"a"}))

	retained := valid
	retained.Targets = slices.Clone(valid.Targets)
	retained.NoPositivePlanPolicy = TrafficMapNoPositivePlanPolicyRetainLastPositive
	retained.AllowAllHomesUnreadyFallback = true
	require.NoError(t, validateTrafficMapPlanCapture(&retained, []string{"a", "b"}))
	retained.Withdrawn = true
	assert.Error(t, validateTrafficMapPlanCapture(&retained, []string{"a", "b"}))
}

func TestTrafficMapPublisherSetupValidation(t *testing.T) {
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	kubeClient := newTrafficMapPublisherClient(t)
	reconciler := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Publisher: publisher, Active: true,
	}

	err := reconciler.validateSetup()
	require.ErrorContains(t, err, "requires leader election")

	reconciler.LeaderElectionEnabled = true
	err = reconciler.validateSetup()
	require.ErrorContains(t, err, "requires a positive resync period")

	reconciler.Active = false
	reconciler.LeaderElectionEnabled = false
	err = reconciler.validateSetup()
	require.ErrorContains(t, err, "requires leader election")

	reconciler.LeaderElectionEnabled = true
	require.NoError(t, reconciler.validateSetup())
}

func TestTrafficMapPublisherOwnerPredicate(t *testing.T) {
	oldOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "model", Namespace: "team-a", Generation: 1,
	}}
	unchanged := oldOwner.DeepCopy()
	assert.False(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: unchanged,
	}))

	newGeneration := oldOwner.DeepCopy()
	newGeneration.Generation++
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newGeneration,
	}))

	newAnnotation := oldOwner.DeepCopy()
	newAnnotation.Annotations = map[string]string{"example.com/key": "value"}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newAnnotation,
	}))

	newLabel := oldOwner.DeepCopy()
	newLabel.Labels = map[string]string{"example.com/key": "value"}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newLabel,
	}))

	newFinalizer := oldOwner.DeepCopy()
	newFinalizer.Finalizers = []string{"example.com/finalizer"}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newFinalizer,
	}))

	newObservedGeneration := oldOwner.DeepCopy()
	newObservedGeneration.Status.ObservedGeneration = 1
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newObservedGeneration,
	}))

	newPlacement := oldOwner.DeepCopy()
	newPlacement.Status.Placement = &v1beta1.PlacementStatus{Phase: v1beta1.PlacementPhasePending}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newPlacement,
	}))

	newReady := oldOwner.DeepCopy()
	newReady.Status.Conditions = duckv1.Conditions{{
		Type: apis.ConditionReady, Status: corev1.ConditionUnknown, Reason: "PlacementLost",
	}}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newReady,
	}))
}

func publisherTestObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	controller := true
	owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "model", Namespace: "team-a", UID: types.UID("owner-uid"), Generation: 2,
	}, Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{
		Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle,
	}}}
	trafficMap := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "model", Namespace: "team-a", UID: types.UID("map-uid"), Generation: 3,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: owner.Name, UID: owner.UID, Controller: &controller,
			}},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service: "model", ObservedISVCGeneration: owner.Generation,
		},
		Status: v1beta1.TrafficMapStatus{SourceUID: owner.UID},
	}
	return owner, trafficMap
}

func eligiblePlacementLossPublisherObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	owner, trafficMap := publisherTestObjects()
	owner.Status.ObservedGeneration = owner.Generation
	owner.Status.Placement = &v1beta1.PlacementStatus{
		Phase:   v1beta1.PlacementPhasePending,
		Cluster: "cluster-a",
	}
	owner.Status.Conditions = duckv1.Conditions{{
		Type:   apis.ConditionReady,
		Status: corev1.ConditionUnknown,
		Reason: placementcontroller.PlacementReadyReasonLost,
	}}
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Spec.Mode = v1beta1.PlacementModeSingle
	trafficMap.Status.Conditions = []metav1.Condition{
		{
			Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionFalse,
			Reason: v1beta1.TrafficMapReasonNotPlaced, ObservedGeneration: trafficMap.Generation,
		},
		{
			Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
			Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: trafficMap.Generation,
		},
	}
	return owner, trafficMap
}

func allHomesUnreadyPublisherObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	owner.Generation++
	owner.Status.ObservedGeneration = owner.Generation
	owner.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	owner.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{
			{Cluster: "cluster-a", Endpoint: &apis.URL{Scheme: "https", Host: "a.example"}, AdmittedReplicas: 1},
			{Cluster: "cluster-b", Endpoint: &apis.URL{Scheme: "https", Host: "b.example"}, AdmittedReplicas: 1},
			{Cluster: "cluster-c", Endpoint: &apis.URL{Scheme: "https", Host: "c.example"}, AdmittedReplicas: 1},
		},
	}
	owner.Status.Conditions[0].Status = corev1.ConditionFalse
	owner.Status.Conditions[0].Reason = "PlacementNotReady"
	trafficMap.Spec.Mode = v1beta1.PlacementModeSplit
	trafficMap.Spec.ObservedISVCGeneration = owner.Generation
	trafficMap.Spec.Entries = []v1beta1.TrafficMapEntry{
		{Cluster: "cluster-a", Endpoint: &apis.URL{Scheme: "https", Host: "a.example"}, Capacity: &v1beta1.TrafficMapCapacity{Allocated: 1}},
		{Cluster: "cluster-b", Endpoint: &apis.URL{Scheme: "https", Host: "b.example"}, Capacity: &v1beta1.TrafficMapCapacity{Allocated: 1}},
		{Cluster: "cluster-c", Endpoint: &apis.URL{Scheme: "https", Host: "c.example"}, Capacity: &v1beta1.TrafficMapCapacity{Allocated: 1}},
	}
	apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
	).Reason = v1beta1.TrafficMapReasonAllHomesUnready
	return owner, trafficMap
}

func restoreEligiblePublisherRoutingConditions(
	t *testing.T,
	kubeClient client.Client,
	trafficMap *v1beta1.TrafficMap,
) {
	t.Helper()
	live := getPublisherTrafficMap(t, kubeClient, trafficMap)
	apimeta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionFalse,
		Reason: v1beta1.TrafficMapReasonNotPlaced, ObservedGeneration: live.Generation,
	})
	apimeta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
		Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: live.Generation,
	})
	require.NoError(t, kubeClient.Status().Update(context.Background(), live))
}

func testPublisherDigest(hexDigit string) string {
	return "sha256:" + strings.Repeat(hexDigit, sha256.Size*2)
}

func publisherClaimedTrafficMap(name, publisher string, claims []string) *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: types.UID("uid-" + name)},
		Status: v1beta1.TrafficMapStatus{Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName: publisher, ClaimedTargets: claims,
		}},
	}
}

func numberedClaims(count int) []string {
	claims := make([]string, count)
	for i := range claims {
		claims[i] = fmt.Sprintf("target-%03d", i)
	}
	return claims
}

func newTrafficMapPublisherReconciler(
	t *testing.T,
	publisher TrafficMapPublisher,
	objects ...client.Object,
) (*TrafficMapPublisherReconciler, client.Client) {
	t.Helper()
	kubeClient := newTrafficMapPublisherClient(t, objects...)
	return &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}, kubeClient
}

func newTrafficMapPublisherClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.TrafficMap{}).
		WithObjects(objects...).
		Build()
}

func reconcileTrafficMapPublisher(
	t *testing.T,
	reconciler *TrafficMapPublisherReconciler,
	trafficMap *v1beta1.TrafficMap,
) (ctrl.Result, error) {
	t.Helper()
	return reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(trafficMap),
	})
}

func getPublisherTrafficMap(t *testing.T, kubeClient client.Client, object client.Object) *v1beta1.TrafficMap {
	t.Helper()
	current := &v1beta1.TrafficMap{}
	require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(object), current))
	return current
}

func publisherApplyContent(t *testing.T, apply runtime.ApplyConfiguration) map[string]any {
	t.Helper()
	unstructuredApply, ok := apply.(interface{ UnstructuredContent() map[string]any })
	require.True(t, ok)
	return unstructuredApply.UnstructuredContent()
}

type conflictOnceClient struct {
	client.Client
	beforeConflict func()
	conflicted     bool
}

type failNthStatusApplyClient struct {
	client.Client
	failAt int
	calls  int
	err    error
}

// staleTrafficMapClient simulates a cache returning one stale TrafficMap read.
// The reconciler must still use APIReader for the deletion journal.
type staleTrafficMapClient struct {
	client.Client
	stale  *v1beta1.TrafficMap
	served bool
}

// frozenTrafficMapCacheClient models an informer cache that has not observed
// publisher status writes yet. Mutations still reach the live API client, as
// they do through controller-runtime's delegating client.
type frozenTrafficMapCacheClient struct {
	client.Client
	trafficMaps    map[types.NamespacedName]*v1beta1.TrafficMap
	trafficMapGets int
}

type preEffectChangingReader struct {
	client.Reader
	trafficMapGets   int
	ownerGets        int
	mutateTrafficMap func(*v1beta1.TrafficMap)
	mutateOwner      func(*v1beta1.InferenceService)
	ownerErr         error
}

func (r *preEffectChangingReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	switch object.(type) {
	case *v1beta1.TrafficMap:
		r.trafficMapGets++
	case *v1beta1.InferenceService:
		r.ownerGets++
		if r.ownerGets > 1 && r.ownerErr != nil {
			return r.ownerErr
		}
	}
	if err := r.Reader.Get(ctx, key, object, options...); err != nil {
		return err
	}
	switch typed := object.(type) {
	case *v1beta1.TrafficMap:
		if r.trafficMapGets > 1 && r.mutateTrafficMap != nil {
			r.mutateTrafficMap(typed)
		}
	case *v1beta1.InferenceService:
		if r.ownerGets > 1 && r.mutateOwner != nil {
			r.mutateOwner(typed)
		}
	}
	return nil
}

type failInferenceServiceListReader struct {
	client.Reader
	err error
}

type failingInferenceServiceGetReader struct {
	client.Reader
	err error
}

func (r *failingInferenceServiceGetReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if _, ok := object.(*v1beta1.InferenceService); ok {
		return r.err
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func (r *failInferenceServiceListReader) List(
	ctx context.Context,
	list client.ObjectList,
	options ...client.ListOption,
) error {
	if _, ok := list.(*v1beta1.InferenceServiceList); ok {
		return r.err
	}
	return r.Reader.List(ctx, list, options...)
}

func (c *staleTrafficMapClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	trafficMap, ok := object.(*v1beta1.TrafficMap)
	if !c.served && ok && key == client.ObjectKeyFromObject(c.stale) {
		c.served = true
		c.stale.DeepCopyInto(trafficMap)
		return nil
	}
	return c.Client.Get(ctx, key, object, options...)
}

func (c *frozenTrafficMapCacheClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	trafficMap, ok := object.(*v1beta1.TrafficMap)
	if !ok {
		return c.Client.Get(ctx, key, object, options...)
	}
	snapshot, found := c.trafficMaps[key]
	if !found {
		return apierrors.NewNotFound(v1beta1.Resource("trafficmaps"), key.Name)
	}
	c.trafficMapGets++
	snapshot.DeepCopyInto(trafficMap)
	return nil
}

func (c *frozenTrafficMapCacheClient) List(
	ctx context.Context,
	list client.ObjectList,
	options ...client.ListOption,
) error {
	trafficMaps, ok := list.(*v1beta1.TrafficMapList)
	if !ok {
		return c.Client.List(ctx, list, options...)
	}
	trafficMaps.Items = trafficMaps.Items[:0]
	for _, snapshot := range c.trafficMaps {
		trafficMaps.Items = append(trafficMaps.Items, *snapshot.DeepCopy())
	}
	return nil
}

func (c *conflictOnceClient) Status() client.SubResourceWriter {
	return &conflictOnceStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

func (c *failNthStatusApplyClient) Status() client.SubResourceWriter {
	return &failNthStatusApplyWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type failNthStatusApplyWriter struct {
	client.SubResourceWriter
	client *failNthStatusApplyClient
}

func (w *failNthStatusApplyWriter) Apply(
	ctx context.Context,
	obj runtime.ApplyConfiguration,
	opts ...client.SubResourceApplyOption,
) error {
	w.client.calls++
	if w.client.calls == w.client.failAt {
		return w.client.err
	}
	return w.SubResourceWriter.Apply(ctx, obj, opts...)
}

type conflictOnceStatusWriter struct {
	client.SubResourceWriter
	client *conflictOnceClient
}

func (w *conflictOnceStatusWriter) Apply(
	ctx context.Context,
	obj runtime.ApplyConfiguration,
	opts ...client.SubResourceApplyOption,
) error {
	if !w.client.conflicted {
		w.client.conflicted = true
		w.client.beforeConflict()
		return apierrors.NewConflict(
			schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps"},
			"model", errors.New("injected status conflict"),
		)
	}
	return w.SubResourceWriter.Apply(ctx, obj, opts...)
}

func (w *conflictOnceStatusWriter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.SubResourcePatchOption,
) error {
	if !w.client.conflicted {
		w.client.conflicted = true
		w.client.beforeConflict()
		return apierrors.NewConflict(
			schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps"},
			obj.GetName(), errors.New("injected status conflict"),
		)
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func TestTrafficMapPublicationWatchFiltersAcknowledgements(t *testing.T) {
	_, old := publisherTestObjects()
	current := old.DeepCopy()
	current.Status.Published = true
	current.Status.ObservedTrafficMapGeneration = current.Generation
	current.Status.Conditions = []metav1.Condition{{Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue}}
	require.False(t, trafficMapPublicationChange.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: current}))
	for _, kind := range []string{v1beta1.TrafficMapRoutable, v1beta1.TrafficMapCapacityFallback, v1beta1.TrafficMapOverrideActive} {
		current = old.DeepCopy()
		current.Status.Conditions = []metav1.Condition{{Type: kind, Status: metav1.ConditionFalse}}
		require.True(t, trafficMapPublicationChange.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: current}), kind)
	}
	require.True(t, trafficMapPublicationChange.Create(event.CreateEvent{Object: old}))
	require.True(t, trafficMapPublicationChange.Delete(event.DeleteEvent{Object: old}))
}
