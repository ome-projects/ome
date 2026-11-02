package placement

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// resetPlacementMetrics clears every vector so whole-vector counts are
// deterministic. Required because other tests in this package reconcile ISVCs
// into these same globals, and CollectAndCount sizes the whole vector rather
// than one label set.
func resetPlacementMetrics() {
	placementPhase.Reset()
	placementWinner.Reset()
	placementCandidate.Reset()
	placementCandidateAdmitted.Reset()
	placementCandidateReady.Reset()
	placementCandidateObservationKnown.Reset()
	placementCandidateObservationFailingSince.Reset()
	placementPolicyInfo.Reset()
	placementSplitReplicas.Reset()
	placementSplitMaxPerCluster.Reset()
	placementTotalDesiredReplicas.Reset()
	placementCandidateDesiredReplicas.Reset()
	placementCapacityObservationKnown.Reset()
}

func splitISVC(ns, name string, replicas int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity,
				Mode: v1beta1.PlacementModeSplit,
				Split: &v1beta1.SplitSpec{
					Replicas:      &replicas,
					MaxPerCluster: engineCap(4),
				},
			},
		},
	}
}

// TestRecordPlacement_PublishesPhaseWinnerAndCandidates: one pass emits exactly
// one phase series, one winner series, and per-candidate state plus replica
// counts.
func TestRecordPlacement_PublishesPhaseWinnerAndCandidates(t *testing.T) {
	resetPlacementMetrics()

	isvc := splitISVC("ns", "svc", 8)
	res := placementResult{
		winner: "cluster-a",
		phase:  v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{
			{Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitted, AdmittedReplicas: 5, ReadyReplicas: 4},
			{Cluster: "cluster-b", Phase: v1beta1.CandidatePhaseAdmitting, AdmittedReplicas: 3, ReadyReplicas: 0},
		},
	}
	recordPlacement(isvc, res)

	if got := testutil.CollectAndCount(placementPhase); got != 1 {
		t.Fatalf("phase series = %d, want exactly 1", got)
	}
	if got := testutil.ToFloat64(placementPhase.WithLabelValues("ns", "svc", "Placed")); got != 1 {
		t.Errorf("phase{Placed} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(placementWinner.WithLabelValues("ns", "svc", "cluster-a")); got != 1 {
		t.Errorf("winner{cluster-a} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(placementCandidate); got != 2 {
		t.Errorf("candidate series = %d, want 2", got)
	}
	if got := testutil.ToFloat64(placementCandidateAdmitted.WithLabelValues("ns", "svc", "cluster-a")); got != 5 {
		t.Errorf("admitted{cluster-a} = %v, want 5", got)
	}
	if got := testutil.ToFloat64(placementCandidateReady.WithLabelValues("ns", "svc", "cluster-b")); got != 0 {
		t.Errorf("ready{cluster-b} = %v, want 0", got)
	}
}

// TestRecordPlacement_NoStaleSeriesOnTransition is the reason every vector is
// reset before it is written: an ISVC that moves Admitting -> Placed, changes
// winner, and drops a candidate must not keep reporting the old label values.
func TestRecordPlacement_NoStaleSeriesOnTransition(t *testing.T) {
	resetPlacementMetrics()

	isvc := splitISVC("ns", "svc", 8)
	recordPlacement(isvc, placementResult{
		winner: "cluster-a",
		phase:  v1beta1.PlacementPhaseAdmitting,
		candidates: []v1beta1.CandidatePlacement{
			{Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitting},
			{Cluster: "cluster-b", Phase: v1beta1.CandidatePhaseAdmitting},
		},
	})
	recordPlacement(isvc, placementResult{
		winner: "cluster-b",
		phase:  v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{
			{Cluster: "cluster-b", Phase: v1beta1.CandidatePhaseAdmitted},
		},
	})

	if got := testutil.CollectAndCount(placementPhase); got != 1 {
		t.Errorf("phase series = %d, want 1 (Admitting must not linger)", got)
	}
	if got := testutil.ToFloat64(placementPhase.WithLabelValues("ns", "svc", "Placed")); got != 1 {
		t.Errorf("phase{Placed} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(placementWinner); got != 1 {
		t.Errorf("winner series = %d, want 1 (cluster-a must not linger)", got)
	}
	if got := testutil.CollectAndCount(placementCandidate); got != 1 {
		t.Errorf("candidate series = %d, want 1 (swept loser must not linger)", got)
	}
	for name, metric := range map[string]*prometheus.GaugeVec{
		"observation_known":         placementCandidateObservationKnown,
		"observation_failing_since": placementCandidateObservationFailingSince,
	} {
		if got := testutil.CollectAndCount(metric); got != 1 {
			t.Errorf("%s series = %d, want 1 after removing a candidate", name, got)
		}
	}
}

// TestRecordPolicy_SplitFieldsAndModeChange: Split publishes the replica band;
// switching to a non-Split mode clears it rather than leaving a stale band.
func TestRecordPolicy_SplitFieldsAndModeChange(t *testing.T) {
	resetPlacementMetrics()

	isvc := splitISVC("ns", "svc", 8)
	recordPolicy(isvc)

	if got := testutil.ToFloat64(placementPolicyInfo.WithLabelValues(
		"ns", "svc", "ClusterAffinity", "Split")); got != 1 {
		t.Errorf("policy_info = %v, want 1", got)
	}
	if got := testutil.ToFloat64(placementSplitReplicas.WithLabelValues("ns", "svc")); got != 8 {
		t.Errorf("split_replicas = %v, want 8", got)
	}
	if got := testutil.ToFloat64(placementSplitMaxPerCluster.WithLabelValues("ns", "svc")); got != 4 {
		t.Errorf("max_per_cluster = %v, want 4", got)
	}

	isvc.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle}
	recordPolicy(isvc)

	if got := testutil.CollectAndCount(placementSplitReplicas); got != 0 {
		t.Errorf("split_replicas series = %d, want 0 after switching to Single", got)
	}
	if got := testutil.CollectAndCount(placementPolicyInfo); got != 1 {
		t.Errorf("policy_info series = %d, want 1 (Split labels must not linger)", got)
	}
}

// TestRecordPolicy_MaxPerClusterReportsThePrimaryCap: the gauge reports the
// primary component's cap, whichever field declares it.
func TestRecordPolicy_MaxPerClusterReportsThePrimaryCap(t *testing.T) {
	for _, tt := range []struct {
		name     string
		ceilings v1beta1.SplitCeilings
		want     float64
	}{
		{name: "default", ceilings: v1beta1.SplitCeilings{Default: ptr.To[int32](6), Router: ptr.To[int32](2)}, want: 6},
		{name: "engine key", ceilings: v1beta1.SplitCeilings{Engine: ptr.To[int32](5)}, want: 5},
		{name: "no engine cap", ceilings: v1beta1.SplitCeilings{Router: ptr.To[int32](2)}, want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resetPlacementMetrics()
			isvc := splitISVC("ns", "svc", 8)
			isvc.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](8), MaxPerCluster: &tt.ceilings}
			recordPolicy(isvc)
			if got := testutil.ToFloat64(placementSplitMaxPerCluster.WithLabelValues("ns", "svc")); got != tt.want {
				t.Errorf("max_per_cluster = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRecordPolicy_NoPlacementEmitsNothing: an ISVC with no placement block is
// not a multi-cluster ISVC and must not appear in the policy metrics.
func TestRecordPolicy_NoPlacementEmitsNothing(t *testing.T) {
	resetPlacementMetrics()

	recordPolicy(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "plain"},
	})

	if got := testutil.CollectAndCount(placementPolicyInfo); got != 0 {
		t.Errorf("policy_info series = %d, want 0", got)
	}
}

// TestDeleteForISVC_DropsEverything: teardown must leave no series behind, or a
// deleted ISVC keeps reporting a phase and a winning cluster forever.
func TestDeleteForISVC_DropsEverything(t *testing.T) {
	resetPlacementMetrics()

	recordPlacement(splitISVC("ns", "svc", 8), placementResult{
		winner:     "cluster-a",
		phase:      v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitted}},
	})
	DeleteForISVC("ns", "svc")

	for name, n := range map[string]int{
		"phase":                     testutil.CollectAndCount(placementPhase),
		"winner":                    testutil.CollectAndCount(placementWinner),
		"candidate":                 testutil.CollectAndCount(placementCandidate),
		"admitted":                  testutil.CollectAndCount(placementCandidateAdmitted),
		"ready":                     testutil.CollectAndCount(placementCandidateReady),
		"observation_known":         testutil.CollectAndCount(placementCandidateObservationKnown),
		"observation_failing_since": testutil.CollectAndCount(placementCandidateObservationFailingSince),
		"policy_info":               testutil.CollectAndCount(placementPolicyInfo),
		"split_replicas":            testutil.CollectAndCount(placementSplitReplicas),
		"max_per_cluster":           testutil.CollectAndCount(placementSplitMaxPerCluster),
		"desired":                   testutil.CollectAndCount(placementTotalDesiredReplicas),
		"candidate_desired":         testutil.CollectAndCount(placementCandidateDesiredReplicas),
		"capacity_known":            testutil.CollectAndCount(placementCapacityObservationKnown),
	} {
		if n != 0 {
			t.Errorf("%s series = %d, want 0 after DeleteForISVC", name, n)
		}
	}
}

func TestPlacementObservationMetricsAreRegistered(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	since := metav1.NewTime(time.Unix(100, 0))
	recordPlacement(splitISVC("team-a", "svc", 3), placementResult{
		candidates: []v1beta1.CandidatePlacement{{
			Cluster: "member-a", ReadyReplicas: 3, ObservationFailingSince: &since,
		}},
	})

	require.NoError(t, testutil.GatherAndCompare(ctrlmetrics.Registry, strings.NewReader(`
# HELP ome_isvc_placement_candidate_observation_known Whether the saved member observation is known (1) or unknown (0). A ready count can remain positive during observation grace.
# TYPE ome_isvc_placement_candidate_observation_known gauge
ome_isvc_placement_candidate_observation_known{cluster="member-a",isvc="svc",namespace="team-a"} 0
# HELP ome_isvc_placement_candidate_observation_failing_since_timestamp_seconds Unix timestamp of the observation failure recorded in candidate status, or 0 when no failure is recorded.
# TYPE ome_isvc_placement_candidate_observation_failing_since_timestamp_seconds gauge
ome_isvc_placement_candidate_observation_failing_since_timestamp_seconds{cluster="member-a",isvc="svc",namespace="team-a"} 100
`), "ome_isvc_placement_candidate_observation_known", "ome_isvc_placement_candidate_observation_failing_since_timestamp_seconds"))
}

func assertCandidateObservationMetrics(t *testing.T, source *v1beta1.InferenceService, cluster string, ready, known, failingSince float64) {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(placementCandidateReady, placementCandidateObservationKnown, placementCandidateObservationFailingSince)
	families, err := registry.Gather()
	require.NoError(t, err)
	want := map[string]float64{
		"ome_isvc_placement_candidate_ready_replicas":                              ready,
		"ome_isvc_placement_candidate_observation_known":                           known,
		"ome_isvc_placement_candidate_observation_failing_since_timestamp_seconds": failingSince,
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["namespace"] != source.Namespace || labels["isvc"] != source.Name || labels["cluster"] != cluster {
				continue
			}
			expected, ok := want[family.GetName()]
			require.True(t, ok, "unexpected or duplicate metric %s", family.GetName())
			require.Equal(t, expected, metric.GetGauge().GetValue(), "%s for %s/%s on %s", family.GetName(), source.Namespace, source.Name, cluster)
			delete(want, family.GetName())
		}
	}
	require.Empty(t, want, "candidate metrics must exist for %s/%s on %s", source.Namespace, source.Name, cluster)
}

func TestPlacementObservationMetricsStayWithinTheirService(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	first := splitISVC("team-a", "svc", 3)
	sameName := splitISVC("team-b", "svc", 3)
	sameNamespace := splitISVC("team-a", "other", 3)
	since := metav1.NewTime(time.Unix(100, 0))
	for _, source := range []*v1beta1.InferenceService{first, sameName, sameNamespace} {
		recordPlacement(source, placementResult{candidates: []v1beta1.CandidatePlacement{{
			Cluster: "member-a", ReadyReplicas: 3, ObservationFailingSince: &since,
		}}})
	}
	recordPlacement(first, placementResult{candidates: []v1beta1.CandidatePlacement{{
		Cluster: "member-a", ReadyReplicas: 2, ObservationKnown: true,
	}}})
	assertCandidateObservationMetrics(t, first, "member-a", 2, 1, 0)
	assertCandidateObservationMetrics(t, sameName, "member-a", 3, 0, 100)
	assertCandidateObservationMetrics(t, sameNamespace, "member-a", 3, 0, 100)

	DeleteForISVC(first.Namespace, first.Name)
	require.Equal(t, 2, testutil.CollectAndCount(placementCandidateObservationKnown))
	require.Equal(t, 2, testutil.CollectAndCount(placementCandidateObservationFailingSince))
	assertCandidateObservationMetrics(t, sameName, "member-a", 3, 0, 100)
	assertCandidateObservationMetrics(t, sameNamespace, "member-a", 3, 0, 100)
}

func TestWritePlacementMetricsUseSavedCandidates(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := splitISVC("team-a", "svc", 8)
	source.UID = "source-a"
	source.Status.Placement = &v1beta1.PlacementStatus{Candidates: []v1beta1.CandidatePlacement{
		{Cluster: "member-a", ReadyReplicas: 3, ObservationKnown: true, Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "registration-a"}},
		{Cluster: "member-b", ReadyReplicas: 4, ObservationKnown: true, Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "registration-b"}},
	}}
	r, hub := newPlacer(testScheme(t), nil, source)
	_, err := r.writePlacement(t.Context(), source, placementResult{
		phase:      v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", ReadyReplicas: 2, ObservationKnown: true}},
	})
	require.NoError(t, err)
	saved := &v1beta1.InferenceService{}
	require.NoError(t, hub.Get(t.Context(), client.ObjectKeyFromObject(source), saved))
	require.Len(t, saved.Status.Placement.Candidates, 2)
	require.False(t, candidateOf(t, saved, "member-a").ObservationKnown, "an observation without allocation identity cannot acknowledge the saved allocation")
	require.False(t, candidateOf(t, saved, "member-b").ObservationKnown, "a candidate omitted from the observation remains saved as unknown")
	assertCandidateObservationMetrics(t, saved, "member-a", 2, 0, 0)
	assertCandidateObservationMetrics(t, saved, "member-b", 4, 0, 0)
}

func TestWritePlacementMetricsKeepSavedStateWhenStatusWriteFails(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := splitISVC("team-a", "svc", 3)
	source.UID = "source-a"
	r, hub := newPlacer(testScheme(t), nil, source)
	_, err := r.writePlacement(t.Context(), source, placementResult{
		phase:      v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", ReadyReplicas: 3, ObservationKnown: true}},
	})
	require.NoError(t, err)
	assertCandidateObservationMetrics(t, source, "member-a", 3, 1, 0)

	writeErr := errors.New("status update rejected")
	r.Client = fakeclient.Wrap(t, hub.(client.WithWatch), fakeclient.FailingVerb(fakeclient.StatusUpdate, fakeclient.EveryCall, writeErr))
	since := metav1.NewTime(time.Unix(100, 0))
	_, err = r.writePlacement(t.Context(), source, placementResult{
		phase:      v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", ObservationFailingSince: &since}},
	})
	require.ErrorIs(t, err, writeErr)
	assert.False(t, r.failingSinceDropped.Load(), "a rejected write provides no evidence that the CRD dropped the timestamp")
	saved := &v1beta1.InferenceService{}
	require.NoError(t, hub.Get(t.Context(), client.ObjectKeyFromObject(source), saved))
	require.True(t, saved.Status.Placement.Candidates[0].ObservationKnown)
	require.Equal(t, int32(3), saved.Status.Placement.Candidates[0].ReadyReplicas)
	require.Nil(t, saved.Status.Placement.Candidates[0].ObservationFailingSince)
	assertCandidateObservationMetrics(t, source, "member-a", 3, 1, 0)
}

func TestWritePlacementMetricsUseAcceptedStatusResponse(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := splitISVC("team-a", "svc", 3)
	source.UID = "source-a"
	r, hub := newPlacer(testScheme(t), nil, source)
	r.ObservationGrace = 5 * time.Minute
	r.Client = fakeclient.Wrap(t, hub.(client.WithWatch), fakeclient.WithInterceptors(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if subresource != "status" {
				return cl.SubResource(subresource).Update(ctx, obj, opts...)
			}
			// A CRD that lacks the failure timestamp drops it from the saved
			// status and the response returned to the client.
			accepted := obj.(*v1beta1.InferenceService).DeepCopy()
			accepted.Status.Placement.Candidates[0].ObservationFailingSince = nil
			if err := cl.SubResource(subresource).Update(ctx, accepted, opts...); err != nil {
				return err
			}
			*obj.(*v1beta1.InferenceService) = *accepted
			return nil
		},
	}))
	since := metav1.NewTime(time.Unix(100, 0))
	_, err := r.writePlacement(t.Context(), source, placementResult{
		phase:      v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", ReadyReplicas: 3, ObservationFailingSince: &since}},
	})
	require.NoError(t, err)
	saved := &v1beta1.InferenceService{}
	require.NoError(t, hub.Get(t.Context(), client.ObjectKeyFromObject(source), saved))
	require.Nil(t, saved.Status.Placement.Candidates[0].ObservationFailingSince)
	assert.True(t, r.failingSinceDropped.Load(), "the sent timestamp must remain available for comparison with the saved response")
	assert.Zero(t, r.observationGrace())
	assertCandidateObservationMetrics(t, saved, "member-a", 3, 0, 0)

	r.Client = hub
	_, err = r.writePlacement(t.Context(), source, placementResult{
		phase:      v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", ReadyReplicas: 3, ObservationFailingSince: &since}},
	})
	require.NoError(t, err)
	assert.False(t, r.failingSinceDropped.Load())
	assert.Equal(t, r.ObservationGrace, r.observationGrace())
	assertCandidateObservationMetrics(t, source, "member-a", 3, 0, float64(since.Unix()))
}

func TestReconcileRemovesMetricsForAbsentOrLocalServices(t *testing.T) {
	source := splitISVC("team-a", "svc", 3)
	source.UID = "source-a"
	local := source.DeepCopy()
	local.UID = "source-b"
	local.Spec.Placement = nil
	for _, tt := range []struct {
		name         string
		cached, live *v1beta1.InferenceService
	}{
		{name: "source absent from cache"},
		{name: "source absent from live read", cached: source},
		{name: "cached service is local", cached: local, live: local},
		{name: "live service is local", cached: source, live: local},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resetPlacementMetrics()
			t.Cleanup(resetPlacementMetrics)
			other := splitISVC("team-b", "svc", 3)
			for _, service := range []*v1beta1.InferenceService{source, other} {
				recordPlacement(service, placementResult{
					phase:      v1beta1.PlacementPhasePlaced,
					candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", ReadyReplicas: 3, ObservationKnown: true}},
				})
			}
			var cachedObjects, liveObjects []client.Object
			if tt.cached != nil {
				cachedObjects = append(cachedObjects, tt.cached.DeepCopy())
			}
			if tt.live != nil {
				liveObjects = append(liveObjects, tt.live.DeepCopy())
			}
			scheme := testScheme(t)
			r, _ := newPlacer(scheme, nil, cachedObjects...)
			_, r.APIReader = newPlacer(scheme, nil, liveObjects...)
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(source)})
			require.NoError(t, err)
			require.Equal(t, 1, testutil.CollectAndCount(placementPhase))
			require.Equal(t, 1, testutil.CollectAndCount(placementCandidateReady))
			require.Equal(t, 1, testutil.CollectAndCount(placementCandidateObservationKnown))
			require.Equal(t, 1, testutil.CollectAndCount(placementCandidateObservationFailingSince))
			assertCandidateObservationMetrics(t, other, "member-a", 3, 1, 0)
		})
	}
}
