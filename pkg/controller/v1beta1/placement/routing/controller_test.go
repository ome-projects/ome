package routing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	k8sptr "k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	placementendpoint "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/endpoint"
)

const (
	testName = "svc"
	testNS   = "prod"
)

func routingScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(s))
	require.NoError(t, clientgoscheme.AddToScheme(s))
	return s
}

// splitISVC builds a Placed, Split-mode ISVC with the given per-home admitted
// and ready replica counts. clusters, admitted, and ready are index-aligned.
func splitISVC(clusters []string, admitted, ready []int32) *v1beta1.InferenceService {
	cands := make([]v1beta1.CandidatePlacement, len(clusters))
	for i, c := range clusters {
		cands[i] = v1beta1.CandidatePlacement{
			Cluster:          c,
			Phase:            v1beta1.CandidatePhaseAdmitted,
			Endpoint:         apis.HTTPS(c + ".example"),
			AdmittedReplicas: admitted[i],
			ReadyReplicas:    ready[i],
		}
	}
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS, UID: "uid-1", Generation: 7},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{
				Mode:         v1beta1.PlacementModeSplit,
				Requirements: "gpu=tpu",
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Placement: &v1beta1.PlacementStatus{
				Phase:      v1beta1.PlacementPhasePlaced,
				Candidates: cands,
			},
		},
	}
}

func controllerTestConfig() Config {
	return Config{
		Enabled:  true,
		Observer: validObserverConfig(),
	}
}

func newReconciler(t *testing.T, cfg Config, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fakeclient.NewClientBuilder().WithScheme(routingScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.TrafficMap{}).
		WithObjects(objs...).Build()
	return &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: cfg}, c
}

// trafficMapBlindClient simulates a stale informer cache while the API reader
// still exposes the authoritative TrafficMap cleanup journal.
type trafficMapBlindClient struct {
	client.Client
}

func (c trafficMapBlindClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := object.(*v1beta1.TrafficMap); ok {
		return apierrors.NewNotFound(schema.GroupResource{
			Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps",
		}, key.Name)
	}
	return c.Client.Get(ctx, key, object, opts...)
}

func reconcile(t *testing.T, r *Reconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testName, Namespace: testNS},
	})
	require.NoError(t, err)
	return result
}

func testCapacityState(t *testing.T) (*CapacityPoller, ResolvedCapacityPolicy) {
	t.Helper()
	config := controllerTestCapacityConfig()
	digest, err := capacityPolicyDigest(config)
	require.NoError(t, err)
	return &CapacityPoller{
			Clock: clocktesting.NewFakeClock(capacityTestNow),
			state: map[capacityKey]*capacityState{},
		}, ResolvedCapacityPolicy{
			Enabled:      true,
			Capacity:     config,
			PolicyDigest: digest,
		}
}

func recordCapacity(
	t *testing.T,
	capacity *CapacityPoller,
	policy ResolvedCapacityPolicy,
	isvc *v1beta1.InferenceService,
	candidate int,
	reported *int32,
	reason string,
) {
	t.Helper()
	target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[candidate], "")
	require.True(t, capacity.record(target, policy, reported, reason))
}

func failingProbeState(
	t *testing.T,
	isvc *v1beta1.InferenceService,
	policy AllFailedPolicy,
	clusters ...string,
) (*Prober, ResolvedProbePolicy) {
	t.Helper()
	config := testProbeConfig()
	config.AllFailedPolicy = policy
	resolved := testProbePolicy(t, config)
	prober := &Prober{state: map[probeIdentity]*homeProbeState{}}
	wanted := make(map[string]struct{}, len(clusters))
	for _, cluster := range clusters {
		wanted[cluster] = struct{}{}
	}
	for _, candidate := range isvc.Status.Placement.Candidates {
		if _, ok := wanted[candidate.Cluster]; !ok {
			continue
		}
		target := targetForCandidate(isvc, &candidate, resolved.PolicyDigest)
		endpoint, err := CanonicalEndpoint(target.URL)
		require.NoError(t, err)
		identity := identityForTarget(target)
		identity.Endpoint = endpoint
		prober.state[identity] = &homeProbeState{
			identity:            identity,
			gated:               true,
			hasConclusiveResult: true,
			lastResult:          v1beta1.ProbeResultFailing,
			observed:            true,
		}
	}
	return prober, resolved
}

func controllerProbeSpec(config ProbeConfig) *v1beta1.RoutingProbeSpec {
	acceptStatuses := make([]int32, len(config.AcceptStatuses))
	for i, status := range config.AcceptStatuses {
		acceptStatuses[i] = int32(status)
	}
	gateStatuses := make([]int32, len(config.GateStatuses))
	for i, status := range config.GateStatuses {
		gateStatuses[i] = int32(status)
	}
	return &v1beta1.RoutingProbeSpec{
		Path:             config.Path,
		Method:           config.Method,
		AcceptStatuses:   acceptStatuses,
		GateStatuses:     gateStatuses,
		Period:           metav1.Duration{Duration: config.Period},
		Timeout:          metav1.Duration{Duration: config.Timeout},
		FailureThreshold: int32(config.FailureThreshold),
		SuccessThreshold: int32(config.SuccessThreshold),
		AllFailedPolicy:  v1beta1.RoutingAllFailedPolicy(config.AllFailedPolicy),
	}
}

func controllerCapacitySpec(config CapacityConfig) *v1beta1.RoutingCapacitySpec {
	return &v1beta1.RoutingCapacitySpec{
		Path:    config.Path,
		Method:  config.Method,
		Format:  string(config.Format),
		Options: config.Options,
		Period:  metav1.Duration{Duration: config.Period},
		Timeout: metav1.Duration{Duration: config.Timeout},
		Samples: int32(config.Samples),
		Quorum:  int32(config.Quorum),
		MaxAge:  metav1.Duration{Duration: config.MaxAge},
	}
}

func controllerTestCapacityConfig() CapacityConfig {
	return CapacityConfig{
		Path:    "/capacity",
		Method:  http.MethodGet,
		Format:  FormatReport,
		Period:  2 * time.Second,
		Timeout: time.Second,
		Samples: 1,
		Quorum:  1,
		MaxAge:  10 * time.Second,
	}
}

func newControllerCapacityPoller(
	t *testing.T,
	client *http.Client,
	clk *clocktesting.FakeClock,
) *CapacityPoller {
	t.Helper()
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	poller, err := NewCapacityPoller(executor, client, clk, 1<<10, log.Log)
	require.NoError(t, err)
	return poller
}

func getTrafficMap(t *testing.T, c client.Client) (*v1beta1.TrafficMap, bool) {
	t.Helper()
	tm := &v1beta1.TrafficMap{}
	err := c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, tm)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	require.NoError(t, err)
	return tm, true
}

func TestReconcile_SplitGeneratesWeightedTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"b", "a"}, []int32{2, 5}, []int32{2, 5})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok, "TrafficMap should be generated")
	assert.Equal(t, isvc.UID, tm.Status.SourceUID)
	assert.Equal(t, testName, tm.Spec.Service)
	assert.Equal(t, v1beta1.PlacementModeSplit, tm.Spec.Mode)
	assert.Equal(t, int64(7), tm.Spec.ObservedISVCGeneration)

	require.Len(t, tm.Spec.Entries, 2)
	// Entries are cluster-sorted: "a" (5 replicas) then "b" (2 replicas) -> 5:2.
	assert.Equal(t, "a", tm.Spec.Entries[0].Cluster)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Weight)
	assert.True(t, tm.Spec.Entries[0].Healthy)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Capacity.Ready)
	require.NotNil(t, tm.Spec.Entries[0].Endpoint)
	assert.Equal(t, "a.example", tm.Spec.Entries[0].Endpoint.Host)

	assert.Equal(t, "b", tm.Spec.Entries[1].Cluster)
	assert.Equal(t, int32(2), tm.Spec.Entries[1].Weight)

	// Owner-ref'd to the ISVC for garbage collection.
	require.True(t, metav1.IsControlledBy(tm, isvc), "TrafficMap should be controlled by its ISVC")
}

func TestReconcile_UnhealthyHomeGatedToZero(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{6, 4}, []int32{6, 0})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight, "healthy home keeps weight")
	assert.True(t, tm.Spec.Entries[0].Healthy)
	assert.Equal(t, int32(0), tm.Spec.Entries[1].Weight, "unhealthy home gated to 0")
	assert.False(t, tm.Spec.Entries[1].Healthy)
}

func TestReconcile_AllModeWeightsFollowReadyReplicas(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{9, 1}, []int32{9, 1})
	isvc.Spec.Placement.Mode = v1beta1.PlacementModeAll
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(9), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Capacity.Allocated)
}

func TestReconcile_SingleModeUsesReadyReplicasAndReadinessGate(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{9}, []int32{9})
	isvc.Spec.Placement.Mode = v1beta1.PlacementModeSingle
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 1)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
	assert.True(t, tm.Spec.Entries[0].Healthy)

	isvc.Status.Placement.Candidates[0].ReadyReplicas = 0
	require.NoError(t, c.Status().Update(context.Background(), isvc))
	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.False(t, tm.Spec.Entries[0].Healthy)
}

func TestReconcile_SplitWeightsFollowReadyReplicas(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{9, 9}, []int32{4, 2})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, int32(9), tm.Spec.Entries[1].Capacity.Allocated)
}

func TestReconcile_NonPlacementISVCDoesNotCreateTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Spec.Placement = nil
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	_, exists := getTrafficMap(t, c)
	assert.False(t, exists)
}

func TestReconcile_LegacyAnnotationPlacementCreatesSingleModeMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Spec.Placement = nil
	isvc.Annotations = map[string]string{"ome.io/accelerator-requirements": "gpu=tpu"}
	isvc.Status.Placement.Cluster = "legacy-home"
	isvc.Status.Placement.Endpoint = apis.HTTPS("legacy.example")
	isvc.Status.Placement.Candidates = nil
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Equal(t, v1beta1.PlacementModeSingle, tm.Spec.Mode)
	require.Len(t, tm.Spec.Entries, 1)
	assert.Equal(t, "legacy-home", tm.Spec.Entries[0].Cluster)
	assert.Equal(t, "legacy.example", tm.Spec.Entries[0].Endpoint.Host)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
}

func TestReconcile_DisabledReapsExistingTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	// Pre-existing owned TrafficMap that should be reaped when the feature is off.
	existing := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "traffic-map-uid",
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
	}
	r, c := newReconciler(t, Config{Enabled: false}, isvc, existing)

	reconcile(t, r)
	_, ok := getTrafficMap(t, c)
	assert.False(t, ok, "disabled controller should reap the TrafficMap it owns")
}

func TestReconcile_PlacementIneligibleReapsWithoutStamping(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Spec.Placement = nil
	existing := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "traffic-map-uid",
			ResourceVersion: "1",
			Finalizers:      []string{"example.com/hold"},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service:                isvc.Name,
			ObservedISVCGeneration: isvc.Generation,
			Entries: []v1beta1.TrafficMapEntry{{
				Cluster: "stale", Endpoint: apis.HTTPS("stale.example"), Weight: 1,
			}},
		},
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc, existing)

	reconcile(t, r)
	terminating, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.NotNil(t, terminating.DeletionTimestamp)
	assert.Empty(t, terminating.Status.SourceUID,
		"reap must not authorize a stale spec for publication before deletion")
}

func TestReconcile_DisabledDefersOrphanedReapUntilClaimFinalizerIsRepaired(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	existing := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "traffic-map-uid",
			ResourceVersion: "1",
		},
		Status: v1beta1.TrafficMapStatus{
			SourceUID: isvc.UID,
			Publisher: &v1beta1.TrafficMapPublisherStatus{
				PublisherName:  "gatewayapi",
				ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
			},
		},
	}
	r, apiClient := newReconciler(t, Config{Enabled: false}, isvc, existing)
	r.Client = trafficMapBlindClient{Client: apiClient}

	reconcile(t, r)

	deferred, exists := getTrafficMap(t, apiClient)
	require.True(t, exists)
	assert.Empty(t, deferred.OwnerReferences)
	assert.Equal(t, isvc.UID, deferred.Status.SourceUID)
	assert.True(t, deferred.DeletionTimestamp.IsZero(), "a claimed map cannot be deleted before finalizer repair")
	assert.Equal(t, existing.Status.Publisher.ClaimedTargets, deferred.Status.Publisher.ClaimedTargets)

	deferred.Finalizers = append(deferred.Finalizers, placementendpoint.TrafficMapPublisherFinalizer)
	require.NoError(t, apiClient.Update(context.Background(), deferred))
	requests := enqueueTrafficMapSource(context.Background(), deferred)
	require.Len(t, requests, 1, "the ownerless finalizer repair must enqueue its same-key source")
	_, err := r.Reconcile(context.Background(), requests[0])
	require.NoError(t, err)

	terminating, exists := getTrafficMap(t, apiClient)
	require.True(t, exists, "the repaired finalizer retains the map for publisher cleanup")
	assert.False(t, terminating.DeletionTimestamp.IsZero())
	require.NotNil(t, terminating.Status.Publisher)
	assert.Equal(t, existing.Status.Publisher.ClaimedTargets, terminating.Status.Publisher.ClaimedTargets)
}

func TestReconcile_DisabledReapsPublisherStatusWithoutClaims(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	existing := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "traffic-map-uid",
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
		Status: v1beta1.TrafficMapStatus{
			Publisher:                    &v1beta1.TrafficMapPublisherStatus{PublisherName: "stateless"},
			Published:                    true,
			ObservedTrafficMapGeneration: 7,
			GatewayRef: &v1beta1.TrafficMapGatewayRef{
				Group: "gateway.networking.k8s.io", Kind: "HTTPRoute", Name: "svc-global",
			},
		},
	}
	r, c := newReconciler(t, Config{Enabled: false}, isvc, existing)

	reconcile(t, r)
	_, exists := getTrafficMap(t, c)
	assert.False(t, exists, "status without target claims must not require finalizer repair")
}

func TestReconcile_RejectsMismatchedTrafficMapSourceUID(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testName,
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
		Spec:   v1beta1.TrafficMapSpec{Service: "existing-service"},
		Status: v1beta1.TrafficMapStatus{SourceUID: "other-source-uid"},
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc, tm)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(isvc),
	})

	require.ErrorContains(t, err, "source UID")
	current, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Equal(t, "existing-service", current.Spec.Service)
	assert.Equal(t, types.UID("other-source-uid"), current.Status.SourceUID)
}

func TestReconcile_DisabledRejectsMismatchedTrafficMapSourceUID(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testName,
			Namespace:  testNS,
			Finalizers: []string{"example.com/hold"},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
		Status: v1beta1.TrafficMapStatus{SourceUID: "other-source-uid"},
	}
	r, c := newReconciler(t, Config{Enabled: false}, isvc, tm)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(isvc),
	})

	require.ErrorContains(t, err, "source UID")
	current, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.True(t, current.DeletionTimestamp.IsZero())
	assert.Equal(t, types.UID("other-source-uid"), current.Status.SourceUID)
}

func TestReconcile_DeletingISVCReapsTrafficMapAndPreservesJournal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config Config
		optOut bool
	}{
		{name: "enabled", config: controllerTestConfig()},
		{name: "globally disabled", config: Config{Enabled: false}},
		{name: "ISVC opted out", config: controllerTestConfig(), optOut: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
			if tc.optOut {
				disabled := false
				isvc.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
			}
			now := metav1.Now()
			isvc.DeletionTimestamp = &now
			isvc.Finalizers = []string{"example.com/hold-source"}
			claims := []string{"v1:httproute:prod/svc-global"}
			tm := &v1beta1.TrafficMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:            testName,
					Namespace:       testNS,
					UID:             "traffic-map-uid",
					ResourceVersion: "1",
					Finalizers:      []string{placementendpoint.TrafficMapPublisherFinalizer},
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
					},
				},
				Status: v1beta1.TrafficMapStatus{Publisher: &v1beta1.TrafficMapPublisherStatus{
					PublisherName:  "gatewayapi",
					ClaimedTargets: claims,
				}},
			}
			r, c := newReconciler(t, tc.config, isvc, tm)

			reconcile(t, r)

			terminating, exists := getTrafficMap(t, c)
			require.True(t, exists, "publisher finalizer retains the cleanup journal")
			require.NotNil(t, terminating.DeletionTimestamp)
			require.NotNil(t, terminating.Status.Publisher)
			assert.Equal(t, claims, terminating.Status.Publisher.ClaimedTargets)
		})
	}
}

func TestReconcile_DeletingISVCReapsOrphanedProvenancedTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	now := metav1.Now()
	isvc.DeletionTimestamp = &now
	isvc.Finalizers = []string{"example.com/hold-source"}
	claims := []string{"v1:httproute:prod/svc-global"}
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "traffic-map-uid",
			ResourceVersion: "1",
			Finalizers:      []string{placementendpoint.TrafficMapPublisherFinalizer},
		},
		Status: v1beta1.TrafficMapStatus{
			SourceUID: isvc.UID,
			Publisher: &v1beta1.TrafficMapPublisherStatus{
				PublisherName:  "gatewayapi",
				ClaimedTargets: claims,
			},
		},
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc, tm)

	reconcile(t, r)

	terminating, exists := getTrafficMap(t, c)
	require.True(t, exists, "publisher finalizer retains the orphaned map's cleanup journal")
	require.NotNil(t, terminating.DeletionTimestamp)
	assert.Empty(t, terminating.OwnerReferences)
	assert.Equal(t, isvc.UID, terminating.Status.SourceUID)
	require.NotNil(t, terminating.Status.Publisher)
	assert.Equal(t, claims, terminating.Status.Publisher.ClaimedTargets)
}

func TestReconcile_DeletingISVCDoesNotReapConflictingController(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	now := metav1.Now()
	isvc.DeletionTimestamp = &now
	isvc.Finalizers = []string{"example.com/hold-source"}
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "traffic-map-uid",
			ResourceVersion: "1",
			Finalizers:      []string{"example.com/hold-map"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       "other-service",
				UID:        "other-source-uid",
				Controller: k8sptr.To(true),
			}},
		},
		Status: v1beta1.TrafficMapStatus{SourceUID: isvc.UID},
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc, tm)

	reconcile(t, r)

	current, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.True(t, current.DeletionTimestamp.IsZero())
	assert.Equal(t, isvc.UID, current.Status.SourceUID)
}

func TestReapTrafficMapFencesConcurrentReplacement(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
		Name:            testName,
		Namespace:       testNS,
		UID:             "owned-map-uid",
		ResourceVersion: "1",
	}, Status: v1beta1.TrafficMapStatus{SourceUID: isvc.UID}}
	key := client.ObjectKeyFromObject(tm)
	deleteCalls := 0
	c := fakeclient.NewClientBuilder().
		WithScheme(routingScheme(t)).
		WithObjects(tm).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
				deleteCalls++
				deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.NotNil(t, deleteOptions.Preconditions)
				require.NotNil(t, deleteOptions.Preconditions.UID)
				require.NotNil(t, deleteOptions.Preconditions.ResourceVersion)
				assert.Equal(t, object.GetUID(), *deleteOptions.Preconditions.UID)
				assert.Equal(t, object.GetResourceVersion(), *deleteOptions.Preconditions.ResourceVersion)

				replacement := &v1beta1.TrafficMap{}
				require.NoError(t, cl.Get(ctx, key, replacement))
				replacement.UID = "replacement-map-uid"
				replacement.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Kind:       "InferenceService",
					Name:       "other",
					UID:        "other-uid",
					Controller: k8sptr.To(true),
				}}
				require.NoError(t, cl.Update(ctx, replacement))
				return apierrors.NewConflict(
					schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps"},
					object.GetName(),
					apierrors.NewBadRequest("delete preconditions no longer match"),
				)
			},
		}).
		Build()
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log}

	err := r.reap(context.Background(), isvc)

	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(err))
	assert.Equal(t, 1, deleteCalls)
	replacement := &v1beta1.TrafficMap{}
	require.NoError(t, c.Get(context.Background(), key, replacement))
	assert.Equal(t, types.UID("replacement-map-uid"), replacement.UID)
	assert.Equal(t, "other", metav1.GetControllerOf(replacement).Name)
}

type trafficMapIdentityReader struct {
	client.Reader
	clearUID             bool
	clearResourceVersion bool
	err                  error
}

func (r trafficMapIdentityReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := object.(*v1beta1.TrafficMap); ok && r.err != nil {
		return r.err
	}
	if err := r.Reader.Get(ctx, key, object, opts...); err != nil {
		return err
	}
	if _, ok := object.(*v1beta1.TrafficMap); ok {
		if r.clearUID {
			object.SetUID("")
		}
		if r.clearResourceVersion {
			object.SetResourceVersion("")
		}
	}
	return nil
}

func TestReapTrafficMapReadFailureDoesNotDelete(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
		Name: testName, Namespace: testNS, UID: "traffic-map-uid", ResourceVersion: "1",
	}}
	tm.Status.SourceUID = isvc.UID
	deleteCalls := 0
	c := fakeclient.NewClientBuilder().
		WithScheme(routingScheme(t)).
		WithObjects(tm).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				deleteCalls++
				return nil
			},
		}).
		Build()
	r := &Reconciler{
		Client: c,
		APIReader: trafficMapIdentityReader{
			Reader: c,
			err:    errors.New("authoritative TrafficMap read failed"),
		},
		Log: log.Log,
	}

	err := r.reap(context.Background(), isvc)

	require.ErrorContains(t, err, "authoritative TrafficMap read failed")
	assert.Zero(t, deleteCalls)
}

func TestReapTrafficMapRequiresServerIdentity(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		clearUID             bool
		clearResourceVersion bool
		wantErr              string
	}{
		{name: "missing UID", clearUID: true, wantErr: "UID is empty"},
		{name: "missing resource version", clearResourceVersion: true, wantErr: "resourceVersion is empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
			tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
				Name:            testName,
				Namespace:       testNS,
				UID:             "traffic-map-uid",
				ResourceVersion: "1",
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
				},
			}}
			deleteCalls := 0
			c := fakeclient.NewClientBuilder().
				WithScheme(routingScheme(t)).
				WithObjects(tm).
				WithInterceptorFuncs(interceptor.Funcs{
					Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
						deleteCalls++
						return nil
					},
				}).
				Build()
			r := &Reconciler{
				Client: c,
				APIReader: trafficMapIdentityReader{
					Reader:               c,
					clearUID:             tc.clearUID,
					clearResourceVersion: tc.clearResourceVersion,
				},
				Log: log.Log,
			}

			err := r.reap(context.Background(), isvc)

			require.ErrorContains(t, err, tc.wantErr)
			assert.Zero(t, deleteCalls)
		})
	}
}

func TestReapTrafficMapRequiresSourceIdentity(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.UID = ""
	deleteCalls := 0
	c := fakeclient.NewClientBuilder().
		WithScheme(routingScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				deleteCalls++
				return nil
			},
		}).
		Build()
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log}

	err := r.reap(context.Background(), isvc)

	require.ErrorContains(t, err, "InferenceService UID is empty")
	assert.Zero(t, deleteCalls)
}

func TestReconcile_UnownedTrafficMapIsNeverMutated(t *testing.T) {
	foreign := func() *v1beta1.TrafficMap {
		return &v1beta1.TrafficMap{
			ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS},
			Spec:       v1beta1.TrafficMapSpec{Service: "foreign-service"},
		}
	}

	t.Run("enabled routing refuses to overwrite", func(t *testing.T) {
		isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
		r, c := newReconciler(t, controllerTestConfig(), isvc, foreign())

		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKeyFromObject(isvc),
		})
		require.ErrorContains(t, err, "is not controlled by InferenceService")

		tm, exists := getTrafficMap(t, c)
		require.True(t, exists)
		assert.Equal(t, "foreign-service", tm.Spec.Service)
		assert.Empty(t, tm.OwnerReferences)
	})

	t.Run("matching provenance alone does not authorize active mutation", func(t *testing.T) {
		isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
		tm := foreign()
		tm.Status.SourceUID = isvc.UID
		r, c := newReconciler(t, controllerTestConfig(), isvc, tm)

		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKeyFromObject(isvc),
		})

		require.ErrorContains(t, err, "is not controlled by InferenceService")
		current, exists := getTrafficMap(t, c)
		require.True(t, exists)
		assert.Equal(t, "foreign-service", current.Spec.Service)
		assert.Equal(t, isvc.UID, current.Status.SourceUID)
		assert.Empty(t, current.OwnerReferences)
	})

	t.Run("disabled routing refuses to delete", func(t *testing.T) {
		isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
		cfg := controllerTestConfig()
		cfg.Enabled = false
		r, c := newReconciler(t, cfg, isvc, foreign())

		reconcile(t, r)

		tm, exists := getTrafficMap(t, c)
		require.True(t, exists)
		assert.Equal(t, "foreign-service", tm.Spec.Service)
		assert.Empty(t, tm.OwnerReferences)
	})
}

func TestApply_RechecksUnownedTrafficMapBeforeMutation(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	foreign := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "foreign-map-uid",
			ResourceVersion: "42",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Name:       "foreign-owner",
				UID:        "foreign-owner-uid",
			}},
		},
		Spec: v1beta1.TrafficMapSpec{Service: "foreign-service"},
	}
	original := foreign.DeepCopy()
	r, c := newReconciler(t, controllerTestConfig(), isvc, foreign)

	err := r.apply(context.Background(), isvc, v1beta1.TrafficMapSpec{
		Service: testName,
		Mode:    v1beta1.PlacementModeSplit,
	})
	require.ErrorContains(t, err, "is not controlled by InferenceService")

	got, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Equal(t, original.Spec, got.Spec)
	assert.Equal(t, original.OwnerReferences, got.OwnerReferences)
}

func TestReconcile_ConfirmsInferenceServiceCacheMissWithAPIReader(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	scheme := routingScheme(t)
	cacheClient := fakeclient.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.TrafficMap{}).
		Build()
	apiReader := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(isvc).Build()
	r := &Reconciler{
		Client:    cacheClient,
		APIReader: apiReader,
		Log:       log.Log,
		Config:    controllerTestConfig(),
	}

	reconcile(t, r)

	tm, exists := getTrafficMap(t, cacheClient)
	require.True(t, exists, "the authoritative read found the live InferenceService")
	assert.Equal(t, testName, tm.Spec.Service)
	assert.True(t, metav1.IsControlledBy(tm, isvc))
}

func TestReconcile_SourceAbsentReapsOwnerlessProvenancedTrafficMap(t *testing.T) {
	claims := []string{"v1:httproute:prod/svc-global"}
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "traffic-map-uid",
			ResourceVersion: "1",
			Finalizers:      []string{placementendpoint.TrafficMapPublisherFinalizer},
		},
		Spec: v1beta1.TrafficMapSpec{Service: testName},
		Status: v1beta1.TrafficMapStatus{
			SourceUID: "deleted-source-uid",
			Publisher: &v1beta1.TrafficMapPublisherStatus{
				PublisherName:  "gatewayapi",
				ClaimedTargets: claims,
			},
		},
	}
	r, c := newReconciler(t, controllerTestConfig(), tm)

	reconcile(t, r)

	terminating, exists := getTrafficMap(t, c)
	require.True(t, exists, "the publisher finalizer must retain the cleanup journal")
	require.NotNil(t, terminating.DeletionTimestamp)
	assert.Empty(t, terminating.OwnerReferences)
	assert.Equal(t, types.UID("deleted-source-uid"), terminating.Status.SourceUID)
	require.NotNil(t, terminating.Status.Publisher)
	assert.Equal(t, claims, terminating.Status.Publisher.ClaimedTargets)
}

func TestReconcile_SourceAbsentLeavesUnattributedOrControlledTrafficMap(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*v1beta1.TrafficMap)
	}{
		{
			name: "ambiguous legacy map without source provenance",
		},
		{
			name: "map with an existing controller",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.SourceUID = "deleted-source-uid"
				tm.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Kind:       "InferenceService",
					Name:       testName,
					UID:        "deleted-source-uid",
					Controller: k8sptr.To(true),
				}}
			},
		},
		{
			name: "map identifying another service",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.SourceUID = "deleted-source-uid"
				tm.Spec.Service = "other-service"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := &v1beta1.TrafficMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:            testName,
					Namespace:       testNS,
					UID:             "traffic-map-uid",
					ResourceVersion: "1",
					Finalizers:      []string{"example.com/hold"},
				},
				Spec: v1beta1.TrafficMapSpec{Service: testName},
			}
			if tt.configure != nil {
				tt.configure(tm)
			}
			r, c := newReconciler(t, controllerTestConfig(), tm)

			reconcile(t, r)

			current, exists := getTrafficMap(t, c)
			require.True(t, exists)
			assert.True(t, current.DeletionTimestamp.IsZero())
		})
	}
}

func TestReconcile_SourceAbsentDefersClaimedMapUntilPublisherFinalizerRepair(t *testing.T) {
	claims := []string{"v1:httproute:prod/svc-global"}
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: testName, Namespace: testNS, UID: "traffic-map-uid", ResourceVersion: "1",
		},
		Spec: v1beta1.TrafficMapSpec{Service: testName},
		Status: v1beta1.TrafficMapStatus{
			SourceUID: "deleted-source-uid",
			Publisher: &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "gatewayapi", ClaimedTargets: claims,
			},
		},
	}
	r, c := newReconciler(t, controllerTestConfig(), tm)

	reconcile(t, r)
	deferred, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.True(t, deferred.DeletionTimestamp.IsZero(),
		"a claimed map cannot be deleted before publisher finalizer repair")

	deferred.Finalizers = append(deferred.Finalizers, placementendpoint.TrafficMapPublisherFinalizer)
	require.NoError(t, c.Update(context.Background(), deferred))
	reconcile(t, r)

	terminating, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.NotNil(t, terminating.DeletionTimestamp)
	require.NotNil(t, terminating.Status.Publisher)
	assert.Equal(t, claims, terminating.Status.Publisher.ClaimedTargets)
}

func TestReconcile_SourceAbsentTrafficMapReadFailureDoesNotDelete(t *testing.T) {
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: testName, Namespace: testNS, UID: "traffic-map-uid", ResourceVersion: "1",
		},
		Spec:   v1beta1.TrafficMapSpec{Service: testName},
		Status: v1beta1.TrafficMapStatus{SourceUID: "deleted-source-uid"},
	}
	c := fakeclient.NewClientBuilder().WithScheme(routingScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.TrafficMap{}).
		WithObjects(tm).Build()
	r := &Reconciler{
		Client: c,
		APIReader: trafficMapIdentityReader{
			Reader: c,
			err:    errors.New("authoritative TrafficMap read failed"),
		},
		Log:    log.Log,
		Config: controllerTestConfig(),
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testName, Namespace: testNS},
	})

	require.ErrorContains(t, err, "authoritative TrafficMap read failed")
	current, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.True(t, current.DeletionTimestamp.IsZero())
}

func TestReconcile_SourceAbsentTrafficMapDeleteFencesReplacement(t *testing.T) {
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: testName, Namespace: testNS, UID: "orphan-map-uid", ResourceVersion: "1",
		},
		Spec:   v1beta1.TrafficMapSpec{Service: testName},
		Status: v1beta1.TrafficMapStatus{SourceUID: "deleted-source-uid"},
	}
	key := client.ObjectKeyFromObject(tm)
	deleteCalls := 0
	c := fakeclient.NewClientBuilder().WithScheme(routingScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.TrafficMap{}).
		WithObjects(tm).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
				deleteCalls++
				deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.NotNil(t, deleteOptions.Preconditions)
				require.NotNil(t, deleteOptions.Preconditions.UID)
				require.NotNil(t, deleteOptions.Preconditions.ResourceVersion)
				assert.Equal(t, object.GetUID(), *deleteOptions.Preconditions.UID)
				assert.Equal(t, object.GetResourceVersion(), *deleteOptions.Preconditions.ResourceVersion)

				replacement := &v1beta1.TrafficMap{}
				require.NoError(t, cl.Get(ctx, key, replacement))
				replacement.UID = "replacement-map-uid"
				replacement.Spec.Service = "other-service"
				replacement.Status.SourceUID = "replacement-source-uid"
				require.NoError(t, cl.Update(ctx, replacement))
				return apierrors.NewConflict(
					schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps"},
					object.GetName(),
					apierrors.NewBadRequest("delete preconditions no longer match"),
				)
			},
		}).
		Build()
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: controllerTestConfig()}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})

	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(err))
	assert.Equal(t, 1, deleteCalls)
	replacement := &v1beta1.TrafficMap{}
	require.NoError(t, c.Get(context.Background(), key, replacement))
	assert.Equal(t, types.UID("replacement-map-uid"), replacement.UID)
	assert.Equal(t, "other-service", replacement.Spec.Service)
	assert.True(t, replacement.DeletionTimestamp.IsZero())
}

// routableCond returns the Routable condition, or nil when absent.
func routableCond(tm *v1beta1.TrafficMap) *metav1.Condition {
	return apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapRoutable)
}

func capacityFallbackCond(tm *v1beta1.TrafficMap) *metav1.Condition {
	return apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapCapacityFallback)
}

// An ISVC that is not Placed keeps its map, emptied, with the reason. Deleting
// it would make "not placed yet" indistinguishable from a wedged controller.
func TestReconcile_NotPlacedKeepsEmptyTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok, "an unroutable ISVC must still have a TrafficMap")
	assert.Empty(t, tm.Spec.Entries)
	assert.Equal(t, testName, tm.Spec.Service, "identity is populated even with no entries")
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, cond.Reason)
}

// Placed, but no candidate publishes an endpoint: empty table, distinct reason.
// The reason is what separates this from NotPlaced, since both tables are empty.
func TestReconcile_NoAddressableHomeKeepsEmptyTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Status.Placement.Candidates[0].Endpoint = nil
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Empty(t, tm.Spec.Entries)
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonNoAddressableHome, cond.Reason)
}

func TestReconcile_AllHomesUnreadyIsUnroutable(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{3, 3}, []int32{0, 0})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	for _, e := range tm.Spec.Entries {
		assert.False(t, e.Healthy)
		assert.Equal(t, int32(0), e.Weight, "an unready home must not receive traffic")
	}
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesUnready, cond.Reason)
}

func TestReconcile_InlineProbePolicyDrivesObservationAndRequeue(t *testing.T) {
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint

	inline := testProbeConfig()
	inline.Path = "/inline-ready"
	inline.Period = 3 * time.Second
	inline.Timeout = time.Second
	inline.FailureThreshold = 1
	inline.SuccessThreshold = 1
	inline.AllFailedPolicy = AllFailedPolicyDrain
	isvc.Spec.Routing = &v1beta1.RoutingSpec{Probe: controllerProbeSpec(inline)}

	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	r, c := newReconciler(t, cfg, isvc)
	r.Prober = prober

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Equal(t, inline.Period, result.RequeueAfter)
	select {
	case path := <-requests:
		assert.Equal(t, inline.Path, path)
	default:
		t.Fatal("inline probe request was not issued")
	}
	resolved, err := ResolveProbePolicy(cfg, &isvc.Spec)
	require.NoError(t, err)
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 1)
	entry := tm.Spec.Entries[0]
	assert.Equal(t, int32(0), entry.Weight)
	assert.False(t, entry.Healthy)
	require.NotNil(t, entry.Probe)
	assert.Equal(t, resolved.PolicyDigest, entry.Probe.PolicyDigest)
	assert.Equal(t, v1beta1.ProbeResultFailing, entry.Probe.Result)
	assert.True(t, entry.Probe.Gated)
	require.NotNil(t, entry.Probe.LastProbeTime)
	assert.True(t, entry.Probe.LastProbeTime.Time.Equal(testProbeNow))
}

func TestReconcile_InvalidProbeEndpointDoesNotBlockValidTarget(t *testing.T) {
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"invalid", "valid"}, []int32{3, 3}, []int32{3, 3})
	isvc.Status.Placement.Candidates[0].Endpoint = &apis.URL{Host: "invalid.example"}
	validEndpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[1].Endpoint = validEndpoint
	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	r, c := newReconciler(t, cfg, isvc)
	r.Prober = prober

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Equal(t, cfg.Probe.Period, result.RequeueAfter)
	select {
	case path := <-requests:
		assert.Equal(t, cfg.Probe.Path, path)
	default:
		t.Fatal("valid endpoint was not probed")
	}
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, "invalid", tm.Spec.Entries[0].Cluster)
	assert.Nil(t, tm.Spec.Entries[0].Probe)
	assert.True(t, tm.Spec.Entries[0].Healthy)
	assert.Positive(t, tm.Spec.Entries[0].Weight)
	assert.Equal(t, "valid", tm.Spec.Entries[1].Cluster)
	require.NotNil(t, tm.Spec.Entries[1].Probe)
	assert.Equal(t, v1beta1.ProbeResultPassing, tm.Spec.Entries[1].Probe.Result)
	assert.False(t, tm.Spec.Entries[1].Probe.Gated)
	assert.True(t, tm.Spec.Entries[1].Healthy)
	assert.Positive(t, tm.Spec.Entries[1].Weight)
	require.NotNil(t, routableCond(tm))
	assert.Equal(t, metav1.ConditionTrue, routableCond(tm).Status)
}

func TestReconcile_InlineProbeDisableSkipsGlobalProbe(t *testing.T) {
	requests := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	isvc.Spec.Routing = &v1beta1.RoutingSpec{
		Probe: &v1beta1.RoutingProbeSpec{Disabled: true},
	}
	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	r, c := newReconciler(t, cfg, isvc)
	r.Prober = prober

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Zero(t, result.RequeueAfter)
	assert.Empty(t, requests, "an explicit inline disable must suppress the inherited probe")
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 1)
	assert.Nil(t, tm.Spec.Entries[0].Probe)
	assert.True(t, tm.Spec.Entries[0].Healthy)
}

func TestReconcile_InlineCapacityPolicyDrivesObservationAndRequeue(t *testing.T) {
	requests := make(chan string, 1)
	now := testProbeNow
	body, err := json.Marshal(CapacityReport{Servable: 1, ObservedAt: now})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		_, _ = w.Write(body)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	inline := controllerTestCapacityConfig()
	inline.Path = "/inline-capacity"
	inline.Period = 3 * time.Second
	isvc.Spec.Routing = &v1beta1.RoutingSpec{Capacity: controllerCapacitySpec(inline)}

	cfg := controllerTestConfig()
	clk := clocktesting.NewFakeClock(now)
	r, c := newReconciler(t, cfg, isvc)
	r.Capacity = newControllerCapacityPoller(t, NewObserverClient(), clk)

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Equal(t, inline.Period, result.RequeueAfter)
	select {
	case path := <-requests:
		assert.Equal(t, inline.Path, path)
	default:
		t.Fatal("inline capacity request was not issued")
	}
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 1)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Capacity.Allocated)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.Reported)
	assert.Equal(t, int32(1), *tm.Spec.Entries[0].Capacity.Reported)
	assert.Equal(t, v1beta1.CapacitySourceEndpoint, tm.Spec.Entries[0].Capacity.Source)
	assert.Empty(t, tm.Spec.Entries[0].Capacity.FallbackReason)
	condition := capacityFallbackCond(tm)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonEndpointCapacityAvailable, condition.Reason)
}

func TestReconcile_CapacityExpiryReprojectsWithoutPollingEarly(t *testing.T) {
	now := testProbeNow
	body, err := json.Marshal(CapacityReport{
		Servable:   1,
		ObservedAt: now.Add(-25 * time.Second),
	})
	require.NoError(t, err)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	cfg := controllerTestConfig()
	cfg.Capacity = controllerTestCapacityConfig()
	cfg.Capacity.Period = 10 * time.Second
	cfg.Capacity.Timeout = time.Second
	cfg.Capacity.MaxAge = 30 * time.Second
	clk := clocktesting.NewFakeClock(now)
	r, c := newReconciler(t, cfg, isvc)
	r.Capacity = newControllerCapacityPoller(t, NewObserverClient(), clk)

	result := reconcile(t, r)
	assert.False(t, result.Requeue)
	assert.Equal(t, 5*time.Second, result.RequeueAfter)
	assert.Equal(t, int32(1), requests.Load())
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 1)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.Reported)
	assert.Equal(t, int32(1), *tm.Spec.Entries[0].Capacity.Reported)

	clk.Step(5 * time.Second)
	result = reconcile(t, r)
	assert.False(t, result.Requeue)
	assert.Equal(t, 5*time.Second, result.RequeueAfter)
	assert.Equal(t, int32(1), requests.Load(), "expiry must not move the regular poll deadline")
	tm, exists = getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 1)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	assert.Equal(t, int32(3), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Nil(t, tm.Spec.Entries[0].Capacity.Reported)
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, tm.Spec.Entries[0].Capacity.Source)
	assert.Contains(t, tm.Spec.Entries[0].Capacity.FallbackReason, "stale")
	condition := capacityFallbackCond(tm)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonEndpointCapacityFallback, condition.Reason)
}

func TestReconcile_InvalidCapacityEndpointDoesNotBlockValidTarget(t *testing.T) {
	now := testProbeNow
	body, err := json.Marshal(CapacityReport{Servable: 1, ObservedAt: now})
	require.NoError(t, err)
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		_, _ = w.Write(body)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"invalid", "valid"}, []int32{3, 3}, []int32{3, 3})
	isvc.Status.Placement.Candidates[0].Endpoint = &apis.URL{Host: "invalid.example"}
	validEndpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[1].Endpoint = validEndpoint
	cfg := controllerTestConfig()
	cfg.Capacity = controllerTestCapacityConfig()
	r, c := newReconciler(t, cfg, isvc)
	r.Capacity = newControllerCapacityPoller(t, NewObserverClient(), clocktesting.NewFakeClock(now))

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Equal(t, cfg.Capacity.Period, result.RequeueAfter)
	select {
	case path := <-requests:
		assert.Equal(t, cfg.Capacity.Path, path)
	default:
		t.Fatal("valid endpoint was not capacity-polled")
	}
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 2)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	assert.Nil(t, tm.Spec.Entries[0].Capacity.Reported)
	assert.NotEmpty(t, tm.Spec.Entries[0].Capacity.FallbackReason)
	require.NotNil(t, tm.Spec.Entries[1].Capacity)
	require.NotNil(t, tm.Spec.Entries[1].Capacity.Reported)
	assert.Equal(t, int32(1), *tm.Spec.Entries[1].Capacity.Reported)
}

func TestReconcile_InlineCapacityDisableSkipsGlobalCapacity(t *testing.T) {
	requests := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	isvc.Spec.Routing = &v1beta1.RoutingSpec{
		Capacity: &v1beta1.RoutingCapacitySpec{Disabled: true},
	}
	cfg := controllerTestConfig()
	cfg.Capacity = controllerTestCapacityConfig()
	r, c := newReconciler(t, cfg, isvc)
	r.Capacity = newControllerCapacityPoller(t, NewObserverClient(), clocktesting.NewFakeClock(testProbeNow))

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Zero(t, result.RequeueAfter)
	assert.Empty(t, requests, "an explicit inline disable must suppress inherited capacity polling")
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 1)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	assert.Nil(t, tm.Spec.Entries[0].Capacity.Reported)
	assert.Equal(t, int32(3), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, tm.Spec.Entries[0].Capacity.Source)
	assert.Empty(t, tm.Spec.Entries[0].Capacity.FallbackReason)
	condition := capacityFallbackCond(tm)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonCapacityPollingDisabled, condition.Reason)
}

func TestReconcile_UsesEarliestObservationDeadline(t *testing.T) {
	now := testProbeNow
	clk := clocktesting.NewFakeClock(now)
	capacityBody, err := json.Marshal(CapacityReport{Servable: 3, ObservedAt: now})
	require.NoError(t, err)
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		if request.URL.Path == "/capacity" {
			clk.Step(3 * time.Second)
			_, _ = w.Write(capacityBody)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	cfg.Probe.Period = 5 * time.Second
	cfg.Capacity = controllerTestCapacityConfig()
	cfg.Capacity.Period = 8 * time.Second
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	prober, err := NewProber(executor, NewObserverClient(), clk, 1<<10, log.Log)
	require.NoError(t, err)
	capacity, err := NewCapacityPoller(executor, NewObserverClient(), clk, 1<<10, log.Log)
	require.NoError(t, err)
	r, _ := newReconciler(t, cfg, isvc)
	r.Prober = prober
	r.Capacity = capacity

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Equal(t, 2*time.Second, result.RequeueAfter,
		"capacity work elapsed after the probe must be subtracted from the probe deadline")
	assert.Len(t, requests, 2)
}

func TestReconcile_EnabledCapacityRequiresPoller(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	cfg := controllerTestConfig()
	cfg.Capacity = controllerTestCapacityConfig()
	r, c := newReconciler(t, cfg, isvc)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(isvc),
	})

	require.ErrorContains(t, err, "capacity polling is enabled but no poller is configured")
	_, exists := getTrafficMap(t, c)
	assert.False(t, exists)
}

func TestReconcile_ValidatesCompleteRoutingPolicyBeforeObservation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*v1beta1.InferenceService)
		wantErr string
	}{
		{
			name: "duplicate capacity-factor sources",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Routing = &v1beta1.RoutingSpec{
					CapacityFactors: map[string]resource.Quantity{"a": resource.MustParse("1")},
				}
				//nolint:staticcheck // exercises rejection of the deprecated compatibility field
				isvc.Spec.Placement.CapacityFactors = map[string]resource.Quantity{"a": resource.MustParse("1")}
			},
			wantErr: "must not both be set",
		},
		{
			name: "non-positive routing capacity factor",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Routing = &v1beta1.RoutingSpec{
					CapacityFactors: map[string]resource.Quantity{"a": resource.MustParse("0")},
				}
			},
			wantErr: "must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
			tt.mutate(isvc)
			r, c := newReconciler(t, controllerTestConfig(), isvc)

			_, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(isvc),
			})

			require.ErrorContains(t, err, tt.wantErr)
			_, exists := getTrafficMap(t, c)
			assert.False(t, exists)
		})
	}
}

func TestReconcile_NotPlacedForgetsStaleCapacityTargets(t *testing.T) {
	now := testProbeNow
	body, err := json.Marshal(CapacityReport{Servable: 1, ObservedAt: now})
	require.NoError(t, err)
	requests := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		_, _ = w.Write(body)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	cfg := controllerTestConfig()
	cfg.Capacity = controllerTestCapacityConfig()
	policy, err := ResolveCapacityPolicy(cfg, &isvc.Spec)
	require.NoError(t, err)
	target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[0], "")
	clk := clocktesting.NewFakeClock(now)
	capacity := newControllerCapacityPoller(t, NewObserverClient(), clk)
	r, c := newReconciler(t, cfg, isvc)
	r.Capacity = capacity

	first := reconcile(t, r)
	assert.Equal(t, cfg.Capacity.Period, first.RequeueAfter)
	select {
	case <-requests:
	default:
		t.Fatal("initial capacity request was not issued")
	}
	require.NotNil(t, capacity.Reported(target, policy))
	other := splitISVC([]string{"other-home"}, []int32{4}, []int32{4})
	other.Name = "other-service"
	other.UID = "other-uid"
	otherTarget := targetForCandidate(other, &other.Status.Placement.Candidates[0], "")
	otherReported := int32(2)
	require.True(t, capacity.record(otherTarget, policy, &otherReported, ""))

	current := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), current))
	current.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
	require.NotEmpty(t, current.Status.Placement.Candidates)
	require.NoError(t, c.Status().Update(context.Background(), current))
	clk.Step(cfg.Capacity.Period)

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Zero(t, result.RequeueAfter)
	select {
	case <-requests:
		t.Fatal("a non-Placed InferenceService must not be capacity-polled")
	default:
	}
	assert.Nil(t, capacity.Reported(target, policy), "stale capacity state must be forgotten after placement demotion")
	require.NotNil(t, capacity.Reported(otherTarget, policy), "reconciling one service must not reset another service's state")
}

func TestReconcile_NotPlacedForgetsStaleProbeTargets(t *testing.T) {
	requests := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	resolved, err := ResolveProbePolicy(cfg, &isvc.Spec)
	require.NoError(t, err)
	target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[0], resolved.PolicyDigest)
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	r, c := newReconciler(t, cfg, isvc)
	r.Prober = prober

	first := reconcile(t, r)
	assert.Equal(t, cfg.Probe.Period, first.RequeueAfter)
	select {
	case <-requests:
	default:
		t.Fatal("initial probe request was not issued")
	}
	require.NotNil(t, prober.Provenance(target))

	current := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), current))
	current.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
	require.NotEmpty(t, current.Status.Placement.Candidates, "the demoted status retains a stale admitted candidate")
	require.NoError(t, c.Status().Update(context.Background(), current))
	clk.Step(cfg.Probe.Period)

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Zero(t, result.RequeueAfter)
	select {
	case <-requests:
		t.Fatal("a non-Placed InferenceService must not be probed")
	default:
	}
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Empty(t, tm.Spec.Entries)
	require.NotNil(t, routableCond(tm))
	assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, routableCond(tm).Reason)
	assert.Nil(t, prober.Provenance(target), "stale probe state must be forgotten after placement demotion")
}

func TestBuildSpec_NoRoutableCapacityIsReportedPrecisely(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	zero := int32(0)
	capacity, capacityPolicy := testCapacityState(t)
	recordCapacity(t, capacity, capacityPolicy, isvc, 0, &zero, "")
	recordCapacity(t, capacity, capacityPolicy, isvc, 1, &zero, "")

	spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, capacityPolicy, nil, capacity)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, int32(0), spec.Entries[0].Weight)
	assert.Equal(t, int32(0), spec.Entries[1].Weight)
	assert.True(t, spec.Entries[0].Healthy)
	assert.True(t, spec.Entries[1].Healthy)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, v1beta1.TrafficMapReasonNoRoutableCapacity, reason)
}

func TestBuildSpec_RecordsCapacityFallbackReason(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{7}, []int32{7})
	capacity, capacityPolicy := testCapacityState(t)
	recordCapacity(t, capacity, capacityPolicy, isvc, 0, nil, "capacity request timed out")

	spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, capacityPolicy, nil, capacity)

	require.Len(t, spec.Entries, 1)
	require.NotNil(t, spec.Entries[0].Capacity)
	assert.Equal(t, int32(7), spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, spec.Entries[0].Capacity.Source)
	assert.Equal(t, "capacity request timed out", spec.Entries[0].Capacity.FallbackReason)
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, reason)
}

func TestBuildSpec_ExpiresCapacityBeforeProjection(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{7}, []int32{7})
	capacity, capacityPolicy := testCapacityState(t)
	target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[0], "")
	reported := int32(2)
	observedAt := capacityTestNow.Add(-capacityPolicy.Capacity.MaxAge + time.Second)
	require.True(t, capacity.recordAt(target, capacityPolicy, &reported, observedAt, ""))

	spec, _, _ := buildSpec(isvc, ResolvedProbePolicy{}, capacityPolicy, nil, capacity)
	require.Len(t, spec.Entries, 1)
	require.NotNil(t, spec.Entries[0].Capacity)
	assert.Equal(t, int32(2), spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, v1beta1.CapacitySourceEndpoint, spec.Entries[0].Capacity.Source)
	assert.Empty(t, spec.Entries[0].Capacity.FallbackReason)

	capacity.Clock.(*clocktesting.FakeClock).Step(time.Second)
	spec, _, _ = buildSpec(isvc, ResolvedProbePolicy{}, capacityPolicy, nil, capacity)
	require.Len(t, spec.Entries, 1)
	require.NotNil(t, spec.Entries[0].Capacity)
	assert.Equal(t, int32(7), spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, spec.Entries[0].Capacity.Source)
	assert.Nil(t, spec.Entries[0].Capacity.Reported)
	assert.Contains(t, spec.Entries[0].Capacity.FallbackReason, "stale")
}

func TestBuildSpec_ProbeFailureCannotMaskZeroCapacity(t *testing.T) {
	for _, policy := range []AllFailedPolicy{AllFailedPolicyPreserveTraffic, AllFailedPolicyDrain} {
		t.Run(string(policy), func(t *testing.T) {
			isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
			zero := int32(0)
			capacity, capacityPolicy := testCapacityState(t)
			recordCapacity(t, capacity, capacityPolicy, isvc, 0, &zero, "")
			recordCapacity(t, capacity, capacityPolicy, isvc, 1, &zero, "")
			probe, resolved := failingProbeState(t, isvc, policy, "a", "b")

			spec, status, reason := buildSpec(isvc, resolved, capacityPolicy, probe, capacity)
			require.Len(t, spec.Entries, 2)
			assert.Equal(t, int32(0), spec.Entries[0].Weight)
			assert.Equal(t, int32(0), spec.Entries[1].Weight)
			assert.Equal(t, metav1.ConditionFalse, status)
			assert.Equal(t, v1beta1.TrafficMapReasonNoRoutableCapacity, reason)
		})
	}
}

func TestBuildSpec_PreserveTrafficKeepsReadyCapacityOnFleetWideProbeFailure(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	p, resolved := failingProbeState(t, isvc, AllFailedPolicyPreserveTraffic, "a", "b")

	spec, status, reason := buildSpec(isvc, resolved, ResolvedCapacityPolicy{}, p, nil)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, int32(7), spec.Entries[0].Weight)
	assert.Equal(t, int32(3), spec.Entries[1].Weight)
	assert.False(t, spec.Entries[0].Healthy)
	assert.False(t, spec.Entries[1].Healthy)
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesProbeFailed, reason)
}

func TestBuildSpec_AllProbeFailuresCanDrainEveryHome(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	p, resolved := failingProbeState(t, isvc, AllFailedPolicyDrain, "a", "b")

	spec, status, reason := buildSpec(isvc, resolved, ResolvedCapacityPolicy{}, p, nil)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, int32(0), spec.Entries[0].Weight)
	assert.Equal(t, int32(0), spec.Entries[1].Weight)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesProbeFailed, reason)
}

func TestBuildSpec_PreserveTrafficRequiresEveryProbeToFail(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 0})
	p, resolved := failingProbeState(t, isvc, AllFailedPolicyPreserveTraffic, "a")

	spec, status, reason := buildSpec(isvc, resolved, ResolvedCapacityPolicy{}, p, nil)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, int32(0), spec.Entries[0].Weight)
	assert.Equal(t, int32(0), spec.Entries[1].Weight)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, v1beta1.TrafficMapReasonNoRoutableCapacity, reason)
}

func TestReconcile_ServingHomeReportsRoutable(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, cond.Reason)
}

func TestRoutingStatusApplyIsFieldIsolated(t *testing.T) {
	tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
		Name: testName, Namespace: testNS, ResourceVersion: "17",
	}}
	tm.Status.SourceUID = "source-uid"
	tm.Status.Conditions = []metav1.Condition{
		{Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue, Reason: "Routable"},
		{Type: v1beta1.TrafficMapCapacityFallback, Status: metav1.ConditionTrue, Reason: "EndpointCapacityUnavailable"},
		{Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionTrue, Reason: "OverridesApplied"},
		{Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue, Reason: "Published"},
	}

	apply, ok := trafficMapRoutingStatusApply(tm).(interface{ UnstructuredContent() map[string]any })
	require.True(t, ok)
	content := apply.UnstructuredContent()
	metadata := content["metadata"].(map[string]any)
	assert.Equal(t, "17", metadata["resourceVersion"])
	status := content["status"].(map[string]any)
	assert.Equal(t, "source-uid", status["sourceUID"])
	_, found := status["publisher"]
	assert.False(t, found, "routing SSA must not own publisher status")
	conditions := status["conditions"].([]any)
	require.Len(t, conditions, 3)
	assert.Equal(t, v1beta1.TrafficMapRoutable, conditions[0].(map[string]any)["type"])
	assert.Equal(t, v1beta1.TrafficMapCapacityFallback, conditions[1].(map[string]any)["type"])
	assert.Equal(t, v1beta1.TrafficMapOverrideActive, conditions[2].(map[string]any)["type"])
}

func TestEnqueueTrafficMapSourceDoesNotRequireOwnerReference(t *testing.T) {
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS},
		Status:     v1beta1.TrafficMapStatus{SourceUID: "source-uid"},
	}

	requests := enqueueTrafficMapSource(context.Background(), tm)

	require.Len(t, requests, 1)
	assert.Equal(t, client.ObjectKeyFromObject(tm), requests[0].NamespacedName)
}

func TestSetupWithManagerRequiresAPIReader(t *testing.T) {
	err := (&Reconciler{}).SetupWithManager(nil)
	require.ErrorContains(t, err, "API reader is not configured")
}

func TestReconcile_UpdatesExistingTrafficMapOnReadyReplicaChange(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
	r, c := newReconciler(t, controllerTestConfig(), isvc)
	reconcile(t, r)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)

	// Home "a" becomes ready at 8 vs 2 -> 4:1.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, cur))
	cur.Status.Placement.Candidates[0].AdmittedReplicas = 8
	cur.Status.Placement.Candidates[0].ReadyReplicas = 8
	cur.Status.Placement.Candidates[1].AdmittedReplicas = 2
	cur.Status.Placement.Candidates[1].ReadyReplicas = 2
	require.NoError(t, c.Status().Update(context.Background(), cur))

	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(4), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
}

func TestReconcile_CapacityFactorWeightsHomes(t *testing.T) {
	// Two homes with equal admitted+ready replicas: without factors they would
	// weight 1:1. A per-cluster factor of 2 on "a" doubles its share -> 2:1.
	isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
	//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
	isvc.Spec.Placement.CapacityFactors = map[string]resource.Quantity{
		"a": resource.MustParse("2"),
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)

	assert.Equal(t, "a", tm.Spec.Entries[0].Cluster)
	assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight, "factor-2 home carries double the share")
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.Factor, "factor recorded as provenance")
	assert.Equal(t, "2", tm.Spec.Entries[0].Capacity.Factor.String())

	assert.Equal(t, "b", tm.Spec.Entries[1].Cluster)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	require.NotNil(t, tm.Spec.Entries[1].Capacity)
	assert.Nil(t, tm.Spec.Entries[1].Capacity.Factor, "home absent from map uses the identity factor")
}

func TestReconcile_RoutingCapacityFactorWeightsHomes(t *testing.T) {
	// The routing-level factor is the canonical field.
	isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
	isvc.Spec.Routing = &v1beta1.RoutingSpec{
		CapacityFactors: map[string]resource.Quantity{
			"a": resource.MustParse("2"),
		},
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.Factor)
	assert.Equal(t, "2", tm.Spec.Entries[0].Capacity.Factor.String())
}

func TestReconcile_FractionalCapacityFactor(t *testing.T) {
	// "500m" halves a home's per-replica capacity: a(4 replicas)*0.5 vs b(4)*1
	// -> 2:4 -> reduced 1:2.
	isvc := splitISVC([]string{"a", "b"}, []int32{4, 4}, []int32{4, 4})
	//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
	isvc.Spec.Placement.CapacityFactors = map[string]resource.Quantity{
		"a": resource.MustParse("500m"),
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(2), tm.Spec.Entries[1].Weight)
}

func TestReconcile_BackfillsSourceUIDAfterNormalizingExistingTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	existing := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testName,
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service: "stale-service",
			Entries: []v1beta1.TrafficMapEntry{{
				Cluster: "stale", Endpoint: apis.HTTPS("stale.example"), Weight: 1,
			}},
		},
		Status: v1beta1.TrafficMapStatus{Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName: "stateless",
		}},
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc, existing)

	reconcile(t, r)

	current, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Equal(t, isvc.Name, current.Spec.Service)
	assert.Equal(t, isvc.Generation, current.Spec.ObservedISVCGeneration)
	require.Len(t, current.Spec.Entries, 1)
	assert.Equal(t, "a", current.Spec.Entries[0].Cluster)
	assert.Equal(t, isvc.UID, current.Status.SourceUID)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, "stateless", current.Status.Publisher.PublisherName,
		"routing status ownership must preserve publisher state while backfilling")
}

func TestBuildSpec_CapacityStateIsIsolatedByPolicy(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{7}, []int32{7})
	capacity, originalPolicy := testCapacityState(t)
	zero := int32(0)
	recordCapacity(t, capacity, originalPolicy, isvc, 0, &zero, "")

	changedConfig := originalPolicy.Capacity
	changedConfig.Period += time.Second
	changedDigest, err := capacityPolicyDigest(changedConfig)
	require.NoError(t, err)
	changedPolicy := ResolvedCapacityPolicy{
		Enabled:      true,
		Capacity:     changedConfig,
		PolicyDigest: changedDigest,
	}

	spec, status, reason := buildSpec(
		isvc, ResolvedProbePolicy{}, changedPolicy, nil, capacity,
	)

	require.Len(t, spec.Entries, 1)
	require.NotNil(t, spec.Entries[0].Capacity)
	assert.Equal(t, int32(7), spec.Entries[0].Capacity.Allocated)
	assert.Nil(t, spec.Entries[0].Capacity.Reported)
	assert.Equal(t, "no capacity report yet", spec.Entries[0].Capacity.FallbackReason)
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, reason)
}

func TestRoutingTableChangePredicate(t *testing.T) {
	base := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})

	t.Run("no relevant change is dropped", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Annotations = map[string]string{"unrelated": "x"}
		assert.False(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("capacity change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Status.Placement.Candidates[0].ReadyReplicas = 1
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("routable-ness change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("deletion transition is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		now := metav1.Now()
		nw.DeletionTimestamp = &now
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("capacity factor change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
		nw.Spec.Placement.CapacityFactors = map[string]resource.Quantity{"a": resource.MustParse("2")}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("routing capacity factor change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Spec.Routing = &v1beta1.RoutingSpec{
			CapacityFactors: map[string]resource.Quantity{"a": resource.MustParse("2")},
		}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("routing opt-out is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		disabled := false
		nw.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("inline probe change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Spec.Routing = &v1beta1.RoutingSpec{Probe: controllerProbeSpec(testProbeConfig())}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("inline capacity change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Spec.Routing = &v1beta1.RoutingSpec{Capacity: controllerCapacitySpec(controllerTestCapacityConfig())}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("inline publisher change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Spec.Routing = &v1beta1.RoutingSpec{Publisher: &v1beta1.RoutingPublisherSpec{
			Options: map[string]string{"target": "edge"},
		}}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("placement eligibility removal is admitted", func(t *testing.T) {
		old := base.DeepCopy()
		old.Spec.Placement.Mode = v1beta1.PlacementModeSingle
		nw := old.DeepCopy()
		nw.Spec.Placement.Requirements = ""
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: nw}))
	})
}
