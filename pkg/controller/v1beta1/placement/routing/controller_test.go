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

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	k8sptr "k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	placementendpoint "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/endpoint"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// splitISVC builds a Placed, Split-mode ISVC with the given per-cluster admitted
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
			Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity,
				Mode: v1beta1.PlacementModeSplit,
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

// newRoutingClient is the fake client every routing test reconciles against:
// the ome.io and core schemes, the status subresources the controller writes,
// the row's objects, and the behaviours a row asks of the client.
func newRoutingClient(t *testing.T, objs []client.Object, opts ...fakeclient.Option) client.WithWatch {
	t.Helper()
	all := append([]fakeclient.Option{
		fakeclient.WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.TrafficMap{}),
		fakeclient.WithObjects(objs...),
	}, opts...)
	return fakeclient.New(t, all...)
}

func newReconciler(t *testing.T, cfg Config, objs ...client.Object) (*Reconciler, client.WithWatch) {
	t.Helper()
	c := newRoutingClient(t, objs)
	return &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: cfg}, c
}

// trafficMapReadsFail makes every TrafficMap read through a view fail with
// err while other kinds read from the store: a cache that has not seen the
// map yet, or an authoritative reader that cannot be reached.
func trafficMapReadsFail(err error) fakeclient.Option {
	return fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, err).OnKind(&v1beta1.TrafficMap{})
}

// trafficMapNotFound is the answer of a cache that has not seen the test's
// TrafficMap.
func trafficMapNotFound() error {
	return apierrors.NewNotFound(schema.GroupResource{
		Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps",
	}, testName)
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
	var value *float64
	if reported != nil {
		v := float64(*reported)
		value = &v
	}
	require.True(t, capacity.record(target, policy, value, reason))
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
	prober := &Prober{state: map[probeIdentity]*targetProbeState{}}
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
		prober.state[identity] = &targetProbeState{
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
		Mode:    config.Mode,
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
	poller, err := NewCapacityPoller(executor, client, clk, 1<<10)
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

// routableCond returns the Routable condition, or nil when absent.
func routableCond(tm *v1beta1.TrafficMap) *metav1.Condition {
	return apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapRoutable)
}

func capacityFallbackCond(tm *v1beta1.TrafficMap) *metav1.Condition {
	return apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapCapacityFallback)
}

// reconcileScenario is one row of TestReconcile: what the pass finds, how the
// reconciler is wired, what the pass must return, and the checks after it.
type reconcileScenario struct {
	config  Config
	objects []client.Object
	// clientOptions are the behaviours the fake client is built with, for
	// rows whose delete fails.
	clientOptions []fakeclient.Option
	// wire adjusts the built reconciler before the pass: it swaps a client
	// or reader, or attaches a prober or a capacity poller.
	wire func(r *Reconciler, c client.WithWatch)
	// wantErr is text the pass's error must contain; empty means the pass
	// must succeed.
	wantErr string
	check   func(t *testing.T, pass reconcilePass)
}

// reconcilePass is what a row checks after the pass: the reconciler for
// further passes, the client the TrafficMap is read from, and the result.
type reconcilePass struct {
	reconciler *Reconciler
	client     client.Client
	result     ctrl.Result
}

// reconcileVariant is one input of a row whose variants share their checks.
type reconcileVariant struct {
	name     string
	scenario func(t *testing.T) reconcileScenario
}

// reconcileRow is one situation and its outcome: a scenario, or the named
// variants of one scenario. Each is built fresh when it runs.
type reconcileRow struct {
	name     string
	scenario func(t *testing.T) reconcileScenario
	variants []reconcileVariant
}

// runReconcileScenario builds the client and the reconciler, runs one pass on
// the test key, checks the pass's error, and hands the rest to the row.
func runReconcileScenario(t *testing.T, s reconcileScenario) {
	t.Helper()
	c := newRoutingClient(t, s.objects, s.clientOptions...)
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: s.config}
	if s.wire != nil {
		s.wire(r, c)
	}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testName, Namespace: testNS},
	})
	if s.wantErr == "" {
		require.NoError(t, err)
	} else {
		require.ErrorContains(t, err, s.wantErr)
	}
	s.check(t, reconcilePass{reconciler: r, client: c, result: result})
}

// ownedTrafficMap is a TrafficMap controlled by the InferenceService, with the
// identity the fake client needs for a fenced delete.
func ownedTrafficMap(isvc *v1beta1.InferenceService) *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
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
}

// deletingISVC is a Placed InferenceService whose deletion waits on a
// finalizer, so the pass sees it on its way out.
func deletingISVC() *v1beta1.InferenceService {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	now := metav1.Now()
	isvc.DeletionTimestamp = &now
	isvc.Finalizers = []string{"example.com/hold-source"}
	return isvc
}

// conflictOnDelete is the error the fake client returns when a fenced delete
// finds the object changed.
func conflictOnDelete(object client.Object) error {
	return apierrors.NewConflict(
		schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps"},
		object.GetName(),
		apierrors.NewBadRequest("delete preconditions no longer match"),
	)
}

// endpointServer serves the handler for one row and closes with it.
func endpointServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func TestReconcile(t *testing.T) {
	rows := []reconcileRow{
		// Weights follow the placement mode, the ready replicas and the
		// capacity factors.
		{
			name: "SplitGeneratesWeightedTrafficMap",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"b", "a"}, []int32{2, 5}, []int32{2, 5})
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
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
					},
				}
			},
		},
		{
			name: "UnhealthyClusterGatedToZero",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a", "b"}, []int32{6, 4}, []int32{6, 0})
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						require.Len(t, tm.Spec.Entries, 2)
						assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight, "healthy cluster keeps weight")
						assert.True(t, tm.Spec.Entries[0].Healthy)
						assert.Equal(t, int32(0), tm.Spec.Entries[1].Weight, "unhealthy cluster gated to 0")
						assert.False(t, tm.Spec.Entries[1].Healthy)
					},
				}
			},
		},
		{
			name: "AllModeWeightsFollowReadyReplicas",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a", "b"}, []int32{9, 1}, []int32{9, 1})
				isvc.Spec.Placement.Mode = v1beta1.PlacementModeAll
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						require.Len(t, tm.Spec.Entries, 2)
						assert.Equal(t, int32(9), tm.Spec.Entries[0].Weight)
						assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
						assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
						assert.Equal(t, int32(1), tm.Spec.Entries[1].Capacity.Allocated)
					},
				}
			},
		},
		{
			name: "SingleModeUsesReadyReplicasAndReadinessGate",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{9}, []int32{9})
				isvc.Spec.Placement.Mode = v1beta1.PlacementModeSingle
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						require.Len(t, tm.Spec.Entries, 1)
						assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
						assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
						assert.True(t, tm.Spec.Entries[0].Healthy)

						isvc.Status.Placement.Candidates[0].ReadyReplicas = 0
						require.NoError(t, pass.client.Status().Update(context.Background(), isvc))
						reconcile(t, pass.reconciler)
						tm, ok = getTrafficMap(t, pass.client)
						require.True(t, ok)
						assert.False(t, tm.Spec.Entries[0].Healthy)
					},
				}
			},
		},
		{
			name: "SplitWeightsFollowReadyReplicas",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a", "b"}, []int32{9, 9}, []int32{4, 2})
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						require.Len(t, tm.Spec.Entries, 2)
						assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight)
						assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
						assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
						assert.Equal(t, int32(9), tm.Spec.Entries[1].Capacity.Allocated)
					},
				}
			},
		},
		{
			name: "UpdatesExistingTrafficMapOnReadyReplicaChange",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
						assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)

						// Cluster "a" becomes ready at 8 vs 2 -> 4:1.
						cur := &v1beta1.InferenceService{}
						require.NoError(t, pass.client.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, cur))
						cur.Status.Placement.Candidates[0].AdmittedReplicas = 8
						cur.Status.Placement.Candidates[0].ReadyReplicas = 8
						cur.Status.Placement.Candidates[1].AdmittedReplicas = 2
						cur.Status.Placement.Candidates[1].ReadyReplicas = 2
						require.NoError(t, pass.client.Status().Update(context.Background(), cur))

						reconcile(t, pass.reconciler)
						tm, ok = getTrafficMap(t, pass.client)
						require.True(t, ok)
						assert.Equal(t, int32(4), tm.Spec.Entries[0].Weight)
						assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
					},
				}
			},
		},
		{
			name: "RoutingCapacityFactorWeightsClusters",
			scenario: func(t *testing.T) reconcileScenario {
				// The routing-level factor is the canonical field.
				isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
				isvc.Spec.Routing = &v1beta1.RoutingSpec{
					CapacityFactors: map[string]resource.Quantity{
						"a": resource.MustParse("2"),
					},
				}
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						require.Len(t, tm.Spec.Entries, 2)
						assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight)
						assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
						require.NotNil(t, tm.Spec.Entries[0].Capacity)
						require.NotNil(t, tm.Spec.Entries[0].Capacity.Factor)
						assert.Equal(t, "2", tm.Spec.Entries[0].Capacity.Factor.String())
					},
				}
			},
		},
		{
			name: "FractionalCapacityFactor",
			scenario: func(t *testing.T) reconcileScenario {
				// "500m" halves a cluster's per-replica capacity: a(4 replicas)*0.5 vs b(4)*1
				// -> 2:4 -> reduced 1:2.
				isvc := splitISVC([]string{"a", "b"}, []int32{4, 4}, []int32{4, 4})
				isvc.Spec.Routing = &v1beta1.RoutingSpec{CapacityFactors: map[string]resource.Quantity{
					"a": resource.MustParse("500m"),
				}}
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						require.Len(t, tm.Spec.Entries, 2)
						assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
						assert.Equal(t, int32(2), tm.Spec.Entries[1].Weight)
					},
				}
			},
		},
		{
			name: "NonPlacementISVCDoesNotCreateTrafficMap",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Spec.Placement = nil
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						_, exists := getTrafficMap(t, pass.client)
						assert.False(t, exists)
					},
				}
			},
		},
		{
			name: "PolicyAndModeVariants_WritesMapOnlyForClusterAffinity",
			variants: []reconcileVariant{
				{name: "single", scenario: clusterAffinityWritesMap(v1beta1.PlacementPolicyClusterAffinity, v1beta1.PlacementModeSingle, true)},
				{name: "all", scenario: clusterAffinityWritesMap(v1beta1.PlacementPolicyClusterAffinity, v1beta1.PlacementModeAll, true)},
				{name: "split", scenario: clusterAffinityWritesMap(v1beta1.PlacementPolicyClusterAffinity, v1beta1.PlacementModeSplit, true)},
				{name: "capacity", scenario: clusterAffinityWritesMap(v1beta1.PlacementPolicyClusterAffinity, v1beta1.PlacementModeSplitByCapacity, true)},
				{name: "legacy", scenario: clusterAffinityWritesMap(v1beta1.PlacementPolicy("Legacy"), v1beta1.PlacementModeAll, false)},
				{name: "omitted policy", scenario: clusterAffinityWritesMap("", v1beta1.PlacementModeSingle, false)},
				{name: "local", scenario: clusterAffinityWritesMap("", "", false)},
			},
		},

		// Reaping, ownership and the source identity of an existing map.
		{
			name: "ObsoletePlacementIntentPreservesTrafficMap",
			variants: []reconcileVariant{
				{name: "legacy annotation", scenario: obsoleteIntentKeepsMap(func(source *v1beta1.InferenceService) {
					source.Annotations = map[string]string{"ome.io/accelerator-requirements": "accelerator=gpu"}
				})},
				{name: "empty legacy annotation", scenario: obsoleteIntentKeepsMap(func(source *v1beta1.InferenceService) {
					source.Annotations = map[string]string{"ome.io/cluster-selector": ""}
				})},
			},
		},
		{
			name: "DisabledReapsExistingTrafficMap",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				// Pre-existing owned TrafficMap that should be reaped when the feature is off.
				return reconcileScenario{
					config:  Config{Enabled: false},
					objects: []client.Object{isvc, ownedTrafficMap(isvc)},
					check: func(t *testing.T, pass reconcilePass) {
						_, ok := getTrafficMap(t, pass.client)
						assert.False(t, ok, "disabled controller should reap the TrafficMap it owns")
					},
				}
			},
		},
		{
			name: "PlacementRemovedFromSpec_DeletesMapWithoutSettingSourceUID",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Spec.Placement = nil
				existing := ownedTrafficMap(isvc)
				existing.Finalizers = []string{"example.com/hold"}
				existing.Spec = v1beta1.TrafficMapSpec{
					Service:                isvc.Name,
					ObservedISVCGeneration: isvc.Generation,
					Entries: []v1beta1.TrafficMapEntry{{
						Cluster: "stale", Endpoint: apis.HTTPS("stale.example"), Weight: 1,
					}},
				}
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc, existing},
					check: func(t *testing.T, pass reconcilePass) {
						terminating, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						require.NotNil(t, terminating.DeletionTimestamp)
						assert.Empty(t, terminating.Status.SourceUID,
							"reap must not authorize a stale spec for publication before deletion")
					},
				}
			},
		},
		{
			name: "DisabledWithClaimedOwnerlessMap_DeletesOnlyAfterFinalizerFix",
			scenario: func(t *testing.T) reconcileScenario {
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
				return reconcileScenario{
					config:  Config{Enabled: false},
					objects: []client.Object{isvc, existing},
					wire: func(r *Reconciler, apiClient client.WithWatch) {
						r.Client = fakeclient.Wrap(t, apiClient, trafficMapReadsFail(trafficMapNotFound()))
					},
					check: func(t *testing.T, pass reconcilePass) {
						deferred, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.Empty(t, deferred.OwnerReferences)
						assert.Equal(t, isvc.UID, deferred.Status.SourceUID)
						assert.True(t, deferred.DeletionTimestamp.IsZero(), "a claimed map cannot be deleted before finalizer repair")
						assert.Equal(t, existing.Status.Publisher.ClaimedTargets, deferred.Status.Publisher.ClaimedTargets)

						deferred.Finalizers = append(deferred.Finalizers, placementendpoint.TrafficMapPublisherFinalizer)
						require.NoError(t, pass.client.Update(context.Background(), deferred))
						requests := enqueueTrafficMapSource(context.Background(), deferred)
						require.Len(t, requests, 1, "the ownerless finalizer repair must enqueue its same-key source")
						_, err := pass.reconciler.Reconcile(context.Background(), requests[0])
						require.NoError(t, err)

						terminating, exists := getTrafficMap(t, pass.client)
						require.True(t, exists, "the repaired finalizer retains the map for publisher cleanup")
						assert.False(t, terminating.DeletionTimestamp.IsZero())
						require.NotNil(t, terminating.Status.Publisher)
						assert.Equal(t, existing.Status.Publisher.ClaimedTargets, terminating.Status.Publisher.ClaimedTargets)
					},
				}
			},
		},
		{
			name: "DisabledReapsPublisherStatusWithoutClaims",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				existing := ownedTrafficMap(isvc)
				existing.Status = v1beta1.TrafficMapStatus{
					Publisher:                    &v1beta1.TrafficMapPublisherStatus{PublisherName: "stateless"},
					Published:                    true,
					ObservedTrafficMapGeneration: 7,
					GatewayRef: &v1beta1.TrafficMapGatewayRef{
						Group: "gateway.networking.k8s.io", Kind: "HTTPRoute", Name: "svc-global",
					},
				}
				return reconcileScenario{
					config:  Config{Enabled: false},
					objects: []client.Object{isvc, existing},
					check: func(t *testing.T, pass reconcilePass) {
						_, exists := getTrafficMap(t, pass.client)
						assert.False(t, exists, "status without target claims must not require finalizer repair")
					},
				}
			},
		},
		{
			name: "RejectsMismatchedTrafficMapSourceUID",
			scenario: func(t *testing.T) reconcileScenario {
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
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc, tm},
					wantErr: "source UID",
					check: func(t *testing.T, pass reconcilePass) {
						current, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.Equal(t, "existing-service", current.Spec.Service)
						assert.Equal(t, types.UID("other-source-uid"), current.Status.SourceUID)
					},
				}
			},
		},
		{
			name: "DisabledRejectsMismatchedTrafficMapSourceUID",
			scenario: func(t *testing.T) reconcileScenario {
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
				return reconcileScenario{
					config:  Config{Enabled: false},
					objects: []client.Object{isvc, tm},
					wantErr: "source UID",
					check: func(t *testing.T, pass reconcilePass) {
						current, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.True(t, current.DeletionTimestamp.IsZero())
						assert.Equal(t, types.UID("other-source-uid"), current.Status.SourceUID)
					},
				}
			},
		},
		{
			name: "DeletingISVCReapsTrafficMapAndPreservesJournal",
			variants: []reconcileVariant{
				{name: "enabled", scenario: deletingISVCKeepsJournal(controllerTestConfig(), false)},
				{name: "globally disabled", scenario: deletingISVCKeepsJournal(Config{Enabled: false}, false)},
				{name: "ISVC opted out", scenario: deletingISVCKeepsJournal(controllerTestConfig(), true)},
			},
		},
		{
			name: "DeletingISVCReapConflictRequeuesWithoutError",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := deletingISVC()
				tm := ownedTrafficMap(isvc)
				tm.Status = v1beta1.TrafficMapStatus{SourceUID: isvc.UID}
				deletes := fakeclient.CountingVerb(fakeclient.Delete)
				return reconcileScenario{
					config:        controllerTestConfig(),
					objects:       []client.Object{isvc, tm},
					clientOptions: []fakeclient.Option{deletes, fakeclient.FailingVerb(fakeclient.Delete, fakeclient.EveryCall, conflictOnDelete(tm))},
					// A fenced delete that lost a race is retried, not reported.
					check: func(t *testing.T, pass reconcilePass) {
						assert.True(t, pass.result.Requeue)
						assert.Equal(t, 1, deletes.Count())
						current, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.True(t, current.DeletionTimestamp.IsZero())
					},
				}
			},
		},
		{
			name: "DeletingISVCWithClaimedMap_DeletesMapKeepingPublisherRecord",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := deletingISVC()
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
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc, tm},
					check: func(t *testing.T, pass reconcilePass) {
						terminating, exists := getTrafficMap(t, pass.client)
						require.True(t, exists, "publisher finalizer retains the orphaned map's cleanup journal")
						require.NotNil(t, terminating.DeletionTimestamp)
						assert.Empty(t, terminating.OwnerReferences)
						assert.Equal(t, isvc.UID, terminating.Status.SourceUID)
						require.NotNil(t, terminating.Status.Publisher)
						assert.Equal(t, claims, terminating.Status.Publisher.ClaimedTargets)
					},
				}
			},
		},
		{
			name: "DeletingISVCDoesNotReapConflictingController",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := deletingISVC()
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
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc, tm},
					check: func(t *testing.T, pass reconcilePass) {
						current, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.True(t, current.DeletionTimestamp.IsZero())
						assert.Equal(t, isvc.UID, current.Status.SourceUID)
					},
				}
			},
		},
		{
			name: "UnownedTrafficMapIsNeverMutated",
			variants: []reconcileVariant{
				{name: "enabled routing refuses to overwrite", scenario: func(t *testing.T) reconcileScenario {
					isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
					return reconcileScenario{
						config:  controllerTestConfig(),
						objects: []client.Object{isvc, unownedTrafficMap()},
						wantErr: "is not controlled by InferenceService",
						check: func(t *testing.T, pass reconcilePass) {
							tm, exists := getTrafficMap(t, pass.client)
							require.True(t, exists)
							assert.Equal(t, "foreign-service", tm.Spec.Service)
							assert.Empty(t, tm.OwnerReferences)
						},
					}
				}},
				{name: "matching provenance alone does not authorize active mutation", scenario: func(t *testing.T) reconcileScenario {
					isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
					tm := unownedTrafficMap()
					tm.Status.SourceUID = isvc.UID
					return reconcileScenario{
						config:  controllerTestConfig(),
						objects: []client.Object{isvc, tm},
						wantErr: "is not controlled by InferenceService",
						check: func(t *testing.T, pass reconcilePass) {
							current, exists := getTrafficMap(t, pass.client)
							require.True(t, exists)
							assert.Equal(t, "foreign-service", current.Spec.Service)
							assert.Equal(t, isvc.UID, current.Status.SourceUID)
							assert.Empty(t, current.OwnerReferences)
						},
					}
				}},
				{name: "disabled routing refuses to delete", scenario: func(t *testing.T) reconcileScenario {
					isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
					cfg := controllerTestConfig()
					cfg.Enabled = false
					return reconcileScenario{
						config:  cfg,
						objects: []client.Object{isvc, unownedTrafficMap()},
						check: func(t *testing.T, pass reconcilePass) {
							tm, exists := getTrafficMap(t, pass.client)
							require.True(t, exists)
							assert.Equal(t, "foreign-service", tm.Spec.Service)
							assert.Empty(t, tm.OwnerReferences)
						},
					}
				}},
			},
		},
		{
			name: "ConfirmsInferenceServiceCacheMissWithAPIReader",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				return reconcileScenario{
					config: controllerTestConfig(),
					// The cache holds nothing; only the authoritative reader has the InferenceService.
					wire: func(r *Reconciler, _ client.WithWatch) {
						r.APIReader = fakeclient.New(t, fakeclient.WithObjects(isvc))
					},
					check: func(t *testing.T, pass reconcilePass) {
						tm, exists := getTrafficMap(t, pass.client)
						require.True(t, exists, "the authoritative read found the live InferenceService")
						assert.Equal(t, testName, tm.Spec.Service)
						assert.True(t, metav1.IsControlledBy(tm, isvc))
					},
				}
			},
		},
		{
			name: "BackfillsSourceUIDAfterNormalizingExistingTrafficMap",
			scenario: func(t *testing.T) reconcileScenario {
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
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc, existing},
					check: func(t *testing.T, pass reconcilePass) {
						current, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.Equal(t, isvc.Name, current.Spec.Service)
						assert.Equal(t, isvc.Generation, current.Spec.ObservedISVCGeneration)
						require.Len(t, current.Spec.Entries, 1)
						assert.Equal(t, "a", current.Spec.Entries[0].Cluster)
						assert.Equal(t, isvc.UID, current.Status.SourceUID)
						require.NotNil(t, current.Status.Publisher)
						assert.Equal(t, "stateless", current.Status.Publisher.PublisherName,
							"routing status ownership must preserve publisher state while backfilling")
					},
				}
			},
		},

		// No InferenceService exists for the map.
		{
			name: "SourceAbsentReapsOwnerlessProvenancedTrafficMap",
			scenario: func(t *testing.T) reconcileScenario {
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
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{tm},
					check: func(t *testing.T, pass reconcilePass) {
						terminating, exists := getTrafficMap(t, pass.client)
						require.True(t, exists, "the publisher finalizer must retain the cleanup journal")
						require.NotNil(t, terminating.DeletionTimestamp)
						assert.Empty(t, terminating.OwnerReferences)
						assert.Equal(t, types.UID("deleted-source-uid"), terminating.Status.SourceUID)
						require.NotNil(t, terminating.Status.Publisher)
						assert.Equal(t, claims, terminating.Status.Publisher.ClaimedTargets)
					},
				}
			},
		},
		{
			name: "SourceAbsentLeavesUnattributedOrControlledTrafficMap",
			variants: []reconcileVariant{
				{name: "ambiguous legacy map without source provenance", scenario: sourceAbsentKeepsMap(nil)},
				{name: "map with an existing controller", scenario: sourceAbsentKeepsMap(func(tm *v1beta1.TrafficMap) {
					tm.Status.SourceUID = "deleted-source-uid"
					tm.OwnerReferences = []metav1.OwnerReference{{
						APIVersion: v1beta1.SchemeGroupVersion.String(),
						Kind:       "InferenceService",
						Name:       testName,
						UID:        "deleted-source-uid",
						Controller: k8sptr.To(true),
					}}
				})},
				{name: "map identifying another service", scenario: sourceAbsentKeepsMap(func(tm *v1beta1.TrafficMap) {
					tm.Status.SourceUID = "deleted-source-uid"
					tm.Spec.Service = "other-service"
				})},
			},
		},
		{
			name: "SourceAbsentDefersClaimedMapUntilPublisherFinalizerRepair",
			scenario: func(t *testing.T) reconcileScenario {
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
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{tm},
					check: func(t *testing.T, pass reconcilePass) {
						deferred, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.True(t, deferred.DeletionTimestamp.IsZero(),
							"a claimed map cannot be deleted before publisher finalizer repair")

						deferred.Finalizers = append(deferred.Finalizers, placementendpoint.TrafficMapPublisherFinalizer)
						require.NoError(t, pass.client.Update(context.Background(), deferred))
						reconcile(t, pass.reconciler)

						terminating, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						require.NotNil(t, terminating.DeletionTimestamp)
						require.NotNil(t, terminating.Status.Publisher)
						assert.Equal(t, claims, terminating.Status.Publisher.ClaimedTargets)
					},
				}
			},
		},
		{
			name: "SourceAbsentTrafficMapReadFailureDoesNotDelete",
			scenario: func(t *testing.T) reconcileScenario {
				tm := &v1beta1.TrafficMap{
					ObjectMeta: metav1.ObjectMeta{
						Name: testName, Namespace: testNS, UID: "traffic-map-uid", ResourceVersion: "1",
					},
					Spec:   v1beta1.TrafficMapSpec{Service: testName},
					Status: v1beta1.TrafficMapStatus{SourceUID: "deleted-source-uid"},
				}
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{tm},
					wire: func(r *Reconciler, c client.WithWatch) {
						r.APIReader = fakeclient.Wrap(t, c, trafficMapReadsFail(errors.New("authoritative TrafficMap read failed")))
					},
					wantErr: "authoritative TrafficMap read failed",
					check: func(t *testing.T, pass reconcilePass) {
						current, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.True(t, current.DeletionTimestamp.IsZero())
					},
				}
			},
		},
		{
			name: "SourceAbsentTrafficMapDeleteFencesReplacement",
			scenario: func(t *testing.T) reconcileScenario {
				tm := &v1beta1.TrafficMap{
					ObjectMeta: metav1.ObjectMeta{
						Name: testName, Namespace: testNS, UID: "orphan-map-uid", ResourceVersion: "1",
					},
					Spec:   v1beta1.TrafficMapSpec{Service: testName},
					Status: v1beta1.TrafficMapStatus{SourceUID: "deleted-source-uid"},
				}
				key := client.ObjectKeyFromObject(tm)
				deletes := fakeclient.CountingVerb(fakeclient.Delete)
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{tm},
					// The delete finds a replacement map under the same name: a
					// raw hook writes it into the store and answers a conflict.
					clientOptions: []fakeclient.Option{deletes, fakeclient.WithInterceptors(interceptor.Funcs{
						Delete: func(ctx context.Context, cl client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
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
							return conflictOnDelete(object)
						},
					})},
					// A fenced delete that lost a race is retried, not reported.
					check: func(t *testing.T, pass reconcilePass) {
						assert.True(t, pass.result.Requeue)
						assert.Equal(t, 1, deletes.Count())
						replacement := &v1beta1.TrafficMap{}
						require.NoError(t, pass.client.Get(context.Background(), key, replacement))
						assert.Equal(t, types.UID("replacement-map-uid"), replacement.UID)
						assert.Equal(t, "other-service", replacement.Spec.Service)
						assert.True(t, replacement.DeletionTimestamp.IsZero())
					},
				}
			},
		},

		// The Routable condition and its reasons.
		{
			// An ISVC that is not Placed keeps its map, emptied, with the reason. Deleting
			// it would make "not placed yet" indistinguishable from a controller that
			// stopped running.
			name: "NotPlacedKeepsEmptyTrafficMap",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok, "an unroutable ISVC must still have a TrafficMap")
						assert.Empty(t, tm.Spec.Entries)
						assert.Equal(t, testName, tm.Spec.Service, "identity is populated even with no entries")
						cond := routableCond(tm)
						require.NotNil(t, cond)
						assert.Equal(t, metav1.ConditionFalse, cond.Status)
						assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, cond.Reason)
					},
				}
			},
		},
		{
			// Placed, but no candidate publishes an endpoint: empty table, distinct reason.
			// The reason is what separates this from NotPlaced, since both tables are empty.
			name: "PlacedWithoutAnyEndpoint_EmptyMapReasonNoAddressableHome",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = nil
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						assert.Empty(t, tm.Spec.Entries)
						cond := routableCond(tm)
						require.NotNil(t, cond)
						assert.Equal(t, metav1.ConditionFalse, cond.Status)
						assert.Equal(t, v1beta1.TrafficMapReasonNoAddressableHome, cond.Reason)
					},
				}
			},
		},
		{
			name: "EveryClusterUnready_ZeroWeightsReasonAllHomesUnready",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a", "b"}, []int32{3, 3}, []int32{0, 0})
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						require.Len(t, tm.Spec.Entries, 2)
						for _, e := range tm.Spec.Entries {
							assert.False(t, e.Healthy)
							assert.Equal(t, int32(0), e.Weight, "an unready cluster must not receive traffic")
						}
						cond := routableCond(tm)
						require.NotNil(t, cond)
						assert.Equal(t, metav1.ConditionFalse, cond.Status)
						assert.Equal(t, v1beta1.TrafficMapReasonAllHomesUnready, cond.Reason)
					},
				}
			},
		},
		{
			name: "ServingClusterReportsRoutable",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				return reconcileScenario{
					config:  controllerTestConfig(),
					objects: []client.Object{isvc},
					check: func(t *testing.T, pass reconcilePass) {
						tm, ok := getTrafficMap(t, pass.client)
						require.True(t, ok)
						cond := routableCond(tm)
						require.NotNil(t, cond)
						assert.Equal(t, metav1.ConditionTrue, cond.Status)
						assert.Equal(t, v1beta1.TrafficMapReasonRoutable, cond.Reason)
					},
				}
			},
		},

		// Probes and capacity polls drive the entries and the requeue.
		{
			name: "InlineProbePolicyDrivesObservationAndRequeue",
			scenario: func(t *testing.T) reconcileScenario {
				requests := make(chan string, 1)
				server := endpointServer(t, func(w http.ResponseWriter, request *http.Request) {
					requests <- request.URL.Path
					w.WriteHeader(http.StatusServiceUnavailable)
				})

				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)

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
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Prober = prober },
					check: func(t *testing.T, pass reconcilePass) {
						assert.False(t, pass.result.Requeue)
						assert.Equal(t, inline.Period, pass.result.RequeueAfter)
						select {
						case path := <-requests:
							assert.Equal(t, inline.Path, path)
						default:
							t.Fatal("inline probe request was not issued")
						}
						resolved, err := ResolveProbePolicy(cfg, &isvc.Spec)
						require.NoError(t, err)
						tm, exists := getTrafficMap(t, pass.client)
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
					},
				}
			},
		},
		{
			name: "InvalidProbeEndpointDoesNotBlockValidTarget",
			scenario: func(t *testing.T) reconcileScenario {
				requests := make(chan string, 1)
				server := endpointServer(t, func(w http.ResponseWriter, request *http.Request) {
					requests <- request.URL.Path
					w.WriteHeader(http.StatusOK)
				})

				isvc := splitISVC([]string{"invalid", "valid"}, []int32{3, 3}, []int32{3, 3})
				isvc.Status.Placement.Candidates[0].Endpoint = &apis.URL{Host: "invalid.example"}
				isvc.Status.Placement.Candidates[1].Endpoint = mustAPISURL(t, server.URL)
				cfg := controllerTestConfig()
				cfg.Probe = testProbeConfig()
				clk := clocktesting.NewFakeClock(testProbeNow)
				prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Prober = prober },
					check: func(t *testing.T, pass reconcilePass) {
						assert.False(t, pass.result.Requeue)
						assert.Equal(t, cfg.Probe.Period, pass.result.RequeueAfter)
						select {
						case path := <-requests:
							assert.Equal(t, cfg.Probe.Path, path)
						default:
							t.Fatal("valid endpoint was not probed")
						}
						tm, exists := getTrafficMap(t, pass.client)
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
					},
				}
			},
		},
		{
			name: "InlineProbeDisableSkipsGlobalProbe",
			scenario: func(t *testing.T) reconcileScenario {
				requests := make(chan struct{}, 1)
				server := endpointServer(t, func(w http.ResponseWriter, _ *http.Request) {
					requests <- struct{}{}
					w.WriteHeader(http.StatusOK)
				})

				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)
				isvc.Spec.Routing = &v1beta1.RoutingSpec{
					Probe: &v1beta1.RoutingProbeSpec{Disabled: true},
				}
				cfg := controllerTestConfig()
				cfg.Probe = testProbeConfig()
				clk := clocktesting.NewFakeClock(testProbeNow)
				prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Prober = prober },
					check: func(t *testing.T, pass reconcilePass) {
						assert.False(t, pass.result.Requeue)
						assert.Zero(t, pass.result.RequeueAfter)
						assert.Empty(t, requests, "an explicit inline disable must suppress the inherited probe")
						tm, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						require.Len(t, tm.Spec.Entries, 1)
						assert.Nil(t, tm.Spec.Entries[0].Probe)
						assert.True(t, tm.Spec.Entries[0].Healthy)
					},
				}
			},
		},
		{
			name: "InlineCapacityPolicyDrivesObservationAndRequeue",
			scenario: func(t *testing.T) reconcileScenario {
				requests := make(chan string, 1)
				now := testProbeNow
				body, err := json.Marshal(CapacityReport{Servable: k8sptr.To(int32(1)), ObservedAt: now})
				require.NoError(t, err)
				server := endpointServer(t, func(w http.ResponseWriter, request *http.Request) {
					requests <- request.URL.Path
					_, _ = w.Write(body)
				})

				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)
				inline := controllerTestCapacityConfig()
				inline.Path = "/inline-capacity"
				inline.Period = 3 * time.Second
				isvc.Spec.Routing = &v1beta1.RoutingSpec{Capacity: controllerCapacitySpec(inline)}

				cfg := controllerTestConfig()
				clk := clocktesting.NewFakeClock(now)
				capacity := newControllerCapacityPoller(t, NewObserverClient(), clk)
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Capacity = capacity },
					check: func(t *testing.T, pass reconcilePass) {
						assert.False(t, pass.result.Requeue)
						assert.Equal(t, inline.Period, pass.result.RequeueAfter)
						select {
						case path := <-requests:
							assert.Equal(t, inline.Path, path)
						default:
							t.Fatal("inline capacity request was not issued")
						}
						tm, exists := getTrafficMap(t, pass.client)
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
					},
				}
			},
		},
		{
			name: "CapacityExpiryReprojectsWithoutPollingEarly",
			scenario: func(t *testing.T) reconcileScenario {
				now := testProbeNow
				body, err := json.Marshal(CapacityReport{
					Servable:   k8sptr.To(int32(1)),
					ObservedAt: now.Add(-25 * time.Second),
				})
				require.NoError(t, err)
				var requests atomic.Int32
				server := endpointServer(t, func(w http.ResponseWriter, _ *http.Request) {
					requests.Add(1)
					_, _ = w.Write(body)
				})

				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)
				cfg := controllerTestConfig()
				cfg.Capacity = controllerTestCapacityConfig()
				cfg.Capacity.Period = 10 * time.Second
				cfg.Capacity.Timeout = time.Second
				cfg.Capacity.MaxAge = 30 * time.Second
				clk := clocktesting.NewFakeClock(now)
				capacity := newControllerCapacityPoller(t, NewObserverClient(), clk)
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Capacity = capacity },
					check: func(t *testing.T, pass reconcilePass) {
						assert.False(t, pass.result.Requeue)
						assert.Equal(t, 5*time.Second, pass.result.RequeueAfter)
						assert.Equal(t, int32(1), requests.Load())
						tm, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						require.Len(t, tm.Spec.Entries, 1)
						require.NotNil(t, tm.Spec.Entries[0].Capacity.Reported)
						assert.Equal(t, int32(1), *tm.Spec.Entries[0].Capacity.Reported)

						clk.Step(5 * time.Second)
						result := reconcile(t, pass.reconciler)
						assert.False(t, result.Requeue)
						assert.Equal(t, 5*time.Second, result.RequeueAfter)
						assert.Equal(t, int32(1), requests.Load(), "expiry must not move the regular poll deadline")
						tm, exists = getTrafficMap(t, pass.client)
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
					},
				}
			},
		},
		{
			name: "InvalidCapacityEndpointDoesNotBlockValidTarget",
			scenario: func(t *testing.T) reconcileScenario {
				now := testProbeNow
				body, err := json.Marshal(CapacityReport{Servable: k8sptr.To(int32(1)), ObservedAt: now})
				require.NoError(t, err)
				requests := make(chan string, 1)
				server := endpointServer(t, func(w http.ResponseWriter, request *http.Request) {
					requests <- request.URL.Path
					_, _ = w.Write(body)
				})

				isvc := splitISVC([]string{"invalid", "valid"}, []int32{3, 3}, []int32{3, 3})
				isvc.Status.Placement.Candidates[0].Endpoint = &apis.URL{Host: "invalid.example"}
				isvc.Status.Placement.Candidates[1].Endpoint = mustAPISURL(t, server.URL)
				cfg := controllerTestConfig()
				cfg.Capacity = controllerTestCapacityConfig()
				capacity := newControllerCapacityPoller(t, NewObserverClient(), clocktesting.NewFakeClock(now))
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Capacity = capacity },
					check: func(t *testing.T, pass reconcilePass) {
						assert.False(t, pass.result.Requeue)
						assert.Equal(t, cfg.Capacity.Period, pass.result.RequeueAfter)
						select {
						case path := <-requests:
							assert.Equal(t, cfg.Capacity.Path, path)
						default:
							t.Fatal("valid endpoint was not capacity-polled")
						}
						tm, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						require.Len(t, tm.Spec.Entries, 2)
						require.NotNil(t, tm.Spec.Entries[0].Capacity)
						assert.Nil(t, tm.Spec.Entries[0].Capacity.Reported)
						assert.NotEmpty(t, tm.Spec.Entries[0].Capacity.FallbackReason)
						require.NotNil(t, tm.Spec.Entries[1].Capacity)
						require.NotNil(t, tm.Spec.Entries[1].Capacity.Reported)
						assert.Equal(t, int32(1), *tm.Spec.Entries[1].Capacity.Reported)
					},
				}
			},
		},
		{
			name: "InlineCapacityDisableSkipsGlobalCapacity",
			scenario: func(t *testing.T) reconcileScenario {
				requests := make(chan struct{}, 1)
				server := endpointServer(t, func(w http.ResponseWriter, _ *http.Request) {
					requests <- struct{}{}
					w.WriteHeader(http.StatusOK)
				})

				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)
				isvc.Spec.Routing = &v1beta1.RoutingSpec{
					Capacity: &v1beta1.RoutingCapacitySpec{Disabled: true},
				}
				cfg := controllerTestConfig()
				cfg.Capacity = controllerTestCapacityConfig()
				capacity := newControllerCapacityPoller(t, NewObserverClient(), clocktesting.NewFakeClock(testProbeNow))
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Capacity = capacity },
					check: func(t *testing.T, pass reconcilePass) {
						assert.False(t, pass.result.Requeue)
						assert.Zero(t, pass.result.RequeueAfter)
						assert.Empty(t, requests, "an explicit inline disable must suppress inherited capacity polling")
						tm, exists := getTrafficMap(t, pass.client)
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
					},
				}
			},
		},
		{
			name: "UsesEarliestObservationDeadline",
			scenario: func(t *testing.T) reconcileScenario {
				now := testProbeNow
				clk := clocktesting.NewFakeClock(now)
				capacityBody, err := json.Marshal(CapacityReport{Servable: k8sptr.To(int32(3)), ObservedAt: now})
				require.NoError(t, err)
				requests := make(chan string, 2)
				server := endpointServer(t, func(w http.ResponseWriter, request *http.Request) {
					requests <- request.URL.Path
					if request.URL.Path == "/capacity" {
						clk.Step(3 * time.Second)
						_, _ = w.Write(capacityBody)
						return
					}
					w.WriteHeader(http.StatusOK)
				})

				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)
				cfg := controllerTestConfig()
				cfg.Probe = testProbeConfig()
				cfg.Probe.Period = 5 * time.Second
				cfg.Capacity = controllerTestCapacityConfig()
				cfg.Capacity.Period = 8 * time.Second
				executor, _, _ := startTestObserverExecutor(t, 1, 1)
				prober, err := NewProber(executor, NewObserverClient(), clk, 1<<10)
				require.NoError(t, err)
				capacity, err := NewCapacityPoller(executor, NewObserverClient(), clk, 1<<10)
				require.NoError(t, err)
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire: func(r *Reconciler, _ client.WithWatch) {
						r.Prober = prober
						r.Capacity = capacity
					},
					check: func(t *testing.T, pass reconcilePass) {
						assert.False(t, pass.result.Requeue)
						assert.Equal(t, 2*time.Second, pass.result.RequeueAfter,
							"capacity work elapsed after the probe must be subtracted from the probe deadline")
						assert.Len(t, requests, 2)
					},
				}
			},
		},
		{
			name: "EnabledCapacityRequiresPoller",
			scenario: func(t *testing.T) reconcileScenario {
				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				cfg := controllerTestConfig()
				cfg.Capacity = controllerTestCapacityConfig()
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wantErr: "capacity polling is enabled but no poller is configured",
					check: func(t *testing.T, pass reconcilePass) {
						_, exists := getTrafficMap(t, pass.client)
						assert.False(t, exists)
					},
				}
			},
		},
		{
			name: "ValidatesCompleteRoutingPolicyBeforeObservation",
			variants: []reconcileVariant{
				{name: "non-positive routing capacity factor", scenario: func(t *testing.T) reconcileScenario {
					isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
					isvc.Spec.Routing = &v1beta1.RoutingSpec{
						CapacityFactors: map[string]resource.Quantity{"a": resource.MustParse("0")},
					}
					return reconcileScenario{
						config:  controllerTestConfig(),
						objects: []client.Object{isvc},
						wantErr: "must be positive",
						check: func(t *testing.T, pass reconcilePass) {
							_, exists := getTrafficMap(t, pass.client)
							assert.False(t, exists)
						},
					}
				}},
			},
		},
		{
			name: "NotPlacedForgetsStaleCapacityTargets",
			scenario: func(t *testing.T) reconcileScenario {
				now := testProbeNow
				body, err := json.Marshal(CapacityReport{Servable: k8sptr.To(int32(1)), ObservedAt: now})
				require.NoError(t, err)
				requests := make(chan struct{}, 2)
				server := endpointServer(t, func(w http.ResponseWriter, _ *http.Request) {
					requests <- struct{}{}
					_, _ = w.Write(body)
				})

				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)
				cfg := controllerTestConfig()
				cfg.Capacity = controllerTestCapacityConfig()
				policy, err := ResolveCapacityPolicy(cfg, &isvc.Spec)
				require.NoError(t, err)
				target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[0], "")
				clk := clocktesting.NewFakeClock(now)
				capacity := newControllerCapacityPoller(t, NewObserverClient(), clk)
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Capacity = capacity },
					check: func(t *testing.T, pass reconcilePass) {
						assert.Equal(t, cfg.Capacity.Period, pass.result.RequeueAfter)
						select {
						case <-requests:
						default:
							t.Fatal("initial capacity request was not issued")
						}
						require.NotNil(t, capacity.Reported(target, policy))
						other := splitISVC([]string{"other-cluster"}, []int32{4}, []int32{4})
						other.Name = "other-service"
						other.UID = "other-uid"
						otherTarget := targetForCandidate(other, &other.Status.Placement.Candidates[0], "")
						otherReported := float64(2)
						require.True(t, capacity.record(otherTarget, policy, &otherReported, ""))

						current := &v1beta1.InferenceService{}
						require.NoError(t, pass.client.Get(context.Background(), client.ObjectKeyFromObject(isvc), current))
						current.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
						require.NotEmpty(t, current.Status.Placement.Candidates)
						require.NoError(t, pass.client.Status().Update(context.Background(), current))
						clk.Step(cfg.Capacity.Period)

						result := reconcile(t, pass.reconciler)

						assert.False(t, result.Requeue)
						assert.Zero(t, result.RequeueAfter)
						select {
						case <-requests:
							t.Fatal("a non-Placed InferenceService must not be capacity-polled")
						default:
						}
						assert.Nil(t, capacity.Reported(target, policy), "stale capacity state must be forgotten after placement demotion")
						require.NotNil(t, capacity.Reported(otherTarget, policy), "reconciling one service must not reset another service's state")
					},
				}
			},
		},
		{
			name: "NotPlacedForgetsStaleProbeTargets",
			scenario: func(t *testing.T) reconcileScenario {
				requests := make(chan struct{}, 2)
				server := endpointServer(t, func(w http.ResponseWriter, _ *http.Request) {
					requests <- struct{}{}
					w.WriteHeader(http.StatusOK)
				})

				isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
				isvc.Status.Placement.Candidates[0].Endpoint = mustAPISURL(t, server.URL)
				cfg := controllerTestConfig()
				cfg.Probe = testProbeConfig()
				resolved, err := ResolveProbePolicy(cfg, &isvc.Spec)
				require.NoError(t, err)
				target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[0], resolved.PolicyDigest)
				clk := clocktesting.NewFakeClock(testProbeNow)
				prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
				return reconcileScenario{
					config:  cfg,
					objects: []client.Object{isvc},
					wire:    func(r *Reconciler, _ client.WithWatch) { r.Prober = prober },
					check: func(t *testing.T, pass reconcilePass) {
						assert.Equal(t, cfg.Probe.Period, pass.result.RequeueAfter)
						select {
						case <-requests:
						default:
							t.Fatal("initial probe request was not issued")
						}
						require.NotNil(t, prober.Provenance(target))

						current := &v1beta1.InferenceService{}
						require.NoError(t, pass.client.Get(context.Background(), client.ObjectKeyFromObject(isvc), current))
						current.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
						require.NotEmpty(t, current.Status.Placement.Candidates, "the demoted status retains a stale admitted candidate")
						require.NoError(t, pass.client.Status().Update(context.Background(), current))
						clk.Step(cfg.Probe.Period)

						result := reconcile(t, pass.reconciler)

						assert.False(t, result.Requeue)
						assert.Zero(t, result.RequeueAfter)
						select {
						case <-requests:
							t.Fatal("a non-Placed InferenceService must not be probed")
						default:
						}
						tm, exists := getTrafficMap(t, pass.client)
						require.True(t, exists)
						assert.Empty(t, tm.Spec.Entries)
						require.NotNil(t, routableCond(tm))
						assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, routableCond(tm).Reason)
						assert.Nil(t, prober.Provenance(target), "stale probe state must be forgotten after placement demotion")
					},
				}
			},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if row.scenario != nil {
				runReconcileScenario(t, row.scenario(t))
				return
			}
			if len(row.variants) == 0 {
				t.Fatal("the row has neither a scenario nor variants")
			}
			for _, variant := range row.variants {
				t.Run(variant.name, func(t *testing.T) {
					runReconcileScenario(t, variant.scenario(t))
				})
			}
		})
	}
}

// clusterAffinityWritesMap places one cluster under the policy and mode. A map
// exists only when want says so, and its entry follows the ready count through
// a ready and an unready pass.
func clusterAffinityWritesMap(policy v1beta1.PlacementPolicy, mode v1beta1.PlacementMode, want bool) func(*testing.T) reconcileScenario {
	return func(t *testing.T) reconcileScenario {
		source := splitISVC([]string{"member-a"}, []int32{3}, []int32{3})
		source.Spec.Placement = nil
		if mode != "" {
			source.Spec.Placement = &v1beta1.PlacementSpec{Policy: policy, Mode: mode}
		}
		var wantErr string
		if source.Spec.Placement != nil && !want {
			wantErr = "ClusterAffinity"
		}
		return reconcileScenario{
			config:  controllerTestConfig(),
			objects: []client.Object{source},
			wantErr: wantErr,
			check: func(t *testing.T, pass reconcilePass) {
				expect := func(stage string, ready, weight int32, healthy bool) {
					t.Helper()
					tm, exists := getTrafficMap(t, pass.client)
					if diff := cmp.Diff(want, exists); diff != "" {
						t.Fatalf("%s: TrafficMap existence (-want +got):\n%s", stage, diff)
					}
					if !exists {
						return
					}
					wantSpec := v1beta1.TrafficMapSpec{
						Service: source.Name, Mode: mode, ObservedISVCGeneration: source.Generation,
						Entries: []v1beta1.TrafficMapEntry{{
							Cluster: "member-a", Endpoint: apis.HTTPS("member-a.example"),
							Weight: weight, Healthy: healthy,
							Capacity: &v1beta1.TrafficMapCapacity{Allocated: 3, Ready: ready, Source: v1beta1.CapacitySourceControlPlane},
						}},
					}
					if diff := cmp.Diff(wantSpec, tm.Spec); diff != "" {
						t.Fatalf("%s: TrafficMap spec (-want +got):\n%s", stage, diff)
					}
					if diff := cmp.Diff(source.UID, tm.Status.SourceUID); diff != "" {
						t.Fatalf("%s: source identity (-want +got):\n%s", stage, diff)
					}
				}
				expect("ready", 3, 1, true)

				source.Status.Placement.Candidates[0].ReadyReplicas = 0
				if err := pass.client.Status().Update(t.Context(), source); err != nil {
					t.Fatal(err)
				}
				_, err := pass.reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(source)})
				if wantErr != "" {
					require.ErrorContains(t, err, wantErr)
				} else {
					require.NoError(t, err)
				}
				expect("unready", 0, 0, false)
			},
		}
	}
}

// obsoleteIntentKeepsMap writes a map, then applies an edit the controller
// refuses. The refusal leaves the map as it was.
func obsoleteIntentKeepsMap(edit func(*v1beta1.InferenceService)) func(*testing.T) reconcileScenario {
	return func(t *testing.T) reconcileScenario {
		source := splitISVC([]string{"a"}, []int32{3}, []int32{3})
		return reconcileScenario{
			config:  controllerTestConfig(),
			objects: []client.Object{source},
			check: func(t *testing.T, pass reconcilePass) {
				before, exists := getTrafficMap(t, pass.client)
				if !exists {
					t.Fatal("expected standing traffic map")
				}
				edit(source)
				if err := pass.client.Update(t.Context(), source); err != nil {
					t.Fatal(err)
				}
				if _, err := pass.reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(source)}); err == nil {
					t.Fatal("obsolete intent was accepted")
				}
				after, exists := getTrafficMap(t, pass.client)
				if !exists {
					t.Fatal("invalid intent removed standing traffic")
				}
				if diff := cmp.Diff(before, after); diff != "" {
					t.Errorf("traffic changed (-want +got):\n%s", diff)
				}
			},
		}
	}
}

// deletingISVCKeepsJournal reaps the map of a deleting InferenceService under
// the configuration; the publisher finalizer keeps the cleanup journal.
func deletingISVCKeepsJournal(config Config, optOut bool) func(*testing.T) reconcileScenario {
	return func(t *testing.T) reconcileScenario {
		isvc := deletingISVC()
		if optOut {
			disabled := false
			isvc.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
		}
		claims := []string{"v1:httproute:prod/svc-global"}
		tm := ownedTrafficMap(isvc)
		tm.Finalizers = []string{placementendpoint.TrafficMapPublisherFinalizer}
		tm.Status = v1beta1.TrafficMapStatus{Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName:  "gatewayapi",
			ClaimedTargets: claims,
		}}
		return reconcileScenario{
			config:  config,
			objects: []client.Object{isvc, tm},
			check: func(t *testing.T, pass reconcilePass) {
				terminating, exists := getTrafficMap(t, pass.client)
				require.True(t, exists, "publisher finalizer retains the cleanup journal")
				require.NotNil(t, terminating.DeletionTimestamp)
				require.NotNil(t, terminating.Status.Publisher)
				assert.Equal(t, claims, terminating.Status.Publisher.ClaimedTargets)
			},
		}
	}
}

// sourceAbsentKeepsMap finds a map under the test key with no InferenceService
// behind it; the map, shaped by configure, is not deleted.
func sourceAbsentKeepsMap(configure func(*v1beta1.TrafficMap)) func(*testing.T) reconcileScenario {
	return func(t *testing.T) reconcileScenario {
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
		if configure != nil {
			configure(tm)
		}
		return reconcileScenario{
			config:  controllerTestConfig(),
			objects: []client.Object{tm},
			check: func(t *testing.T, pass reconcilePass) {
				current, exists := getTrafficMap(t, pass.client)
				require.True(t, exists)
				assert.True(t, current.DeletionTimestamp.IsZero())
			},
		}
	}
}

// unownedTrafficMap is a map under the test key that routing did not create.
func unownedTrafficMap() *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS},
		Spec:       v1beta1.TrafficMapSpec{Service: "foreign-service"},
	}
}

const (
	testName = "svc"
	testNS   = "prod"
)

func TestReap_MapReplacedBeforeDelete_ReturnsConflictLeavingReplacement(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
		Name:            testName,
		Namespace:       testNS,
		UID:             "owned-map-uid",
		ResourceVersion: "1",
	}, Status: v1beta1.TrafficMapStatus{SourceUID: isvc.UID}}
	key := client.ObjectKeyFromObject(tm)
	deletes := fakeclient.CountingVerb(fakeclient.Delete)
	// The delete finds a replacement map under the same name: a raw hook
	// writes it into the store and answers a conflict.
	c := fakeclient.New(t, fakeclient.WithObjects(tm), deletes, fakeclient.WithInterceptors(interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
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
	}))
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log}

	err := r.reap(context.Background(), isvc)

	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(err))
	assert.Equal(t, 1, deletes.Count())
	replacement := &v1beta1.TrafficMap{}
	require.NoError(t, c.Get(context.Background(), key, replacement))
	assert.Equal(t, types.UID("replacement-map-uid"), replacement.UID)
	assert.Equal(t, "other", metav1.GetControllerOf(replacement).Name)
}

func TestReap_LiveReadFails_ReturnsErrorWithoutDelete(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
		Name: testName, Namespace: testNS, UID: "traffic-map-uid", ResourceVersion: "1",
	}}
	tm.Status.SourceUID = isvc.UID
	deletes := fakeclient.CountingVerb(fakeclient.Delete)
	c := fakeclient.New(t, fakeclient.WithObjects(tm), deletes)
	r := &Reconciler{
		Client:    c,
		APIReader: fakeclient.Wrap(t, c, trafficMapReadsFail(errors.New("authoritative TrafficMap read failed"))),
		Log:       log.Log,
	}

	err := r.reap(context.Background(), isvc)

	require.ErrorContains(t, err, "authoritative TrafficMap read failed")
	assert.Zero(t, deletes.Count())
}

func TestReap_MapUIDOrResourceVersionMissing_FailsWithoutDelete(t *testing.T) {
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
			deletes := fakeclient.CountingVerb(fakeclient.Delete)
			c := fakeclient.New(t, fakeclient.WithObjects(tm), deletes)
			// The authoritative reader answers the map without the identity
			// field the row clears.
			r := &Reconciler{
				Client: c,
				APIReader: fakeclient.Wrap(t, c, fakeclient.EditedRead[*v1beta1.TrafficMap](func(live *v1beta1.TrafficMap) {
					if tc.clearUID {
						live.SetUID("")
					}
					if tc.clearResourceVersion {
						live.SetResourceVersion("")
					}
				})),
				Log: log.Log,
			}

			err := r.reap(context.Background(), isvc)

			require.ErrorContains(t, err, tc.wantErr)
			assert.Zero(t, deletes.Count())
		})
	}
}

func TestReap_ISVCUIDEmpty_FailsWithoutDelete(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.UID = ""
	deletes := fakeclient.CountingVerb(fakeclient.Delete)
	c := fakeclient.New(t, deletes)
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log}

	err := r.reap(context.Background(), isvc)

	require.ErrorContains(t, err, "InferenceService UID is empty")
	assert.Zero(t, deletes.Count())
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
	require.True(t, capacity.recordAt(target, capacityPolicy, k8sptr.To(float64(reported)), observedAt, ""))

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

func TestBuildSpec_AllProbeFailuresCanDrainEveryCluster(t *testing.T) {
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

func TestTrafficMapRoutingStatusApply_Rendered_CarriesOnlyRoutingOwnedFields(t *testing.T) {
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

func TestEnqueueTrafficMapSource_MapWithoutOwnerReference_EnqueuesSameKey(t *testing.T) {
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS},
		Status:     v1beta1.TrafficMapStatus{SourceUID: "source-uid"},
	}

	requests := enqueueTrafficMapSource(context.Background(), tm)

	require.Len(t, requests, 1)
	assert.Equal(t, client.ObjectKeyFromObject(tm), requests[0].NamespacedName)
}

func TestSetupWithManager_NoAPIReader_ReturnsError(t *testing.T) {
	err := (&Reconciler{}).SetupWithManager(nil)
	require.ErrorContains(t, err, "API reader is not configured")
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

func TestRoutingTableChange_RelevantISVCChanges_PassUnrelatedDrop(t *testing.T) {
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
		nw.Spec.Placement = nil
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: nw}))
	})
}
