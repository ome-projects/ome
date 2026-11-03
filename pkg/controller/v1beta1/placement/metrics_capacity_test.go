package placement

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func capacitySource() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "service-a", UID: "source-a", Generation: 2},
		Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
			Plan: &v1beta1.PlacementPlanStatus{
				ID: "plan-a", Revision: 1, SourceUID: "source-a", ObservedGeneration: 2,
				RequestedReplicas: 100, AssignedReplicas: 100,
			},
		}},
	}
}

func capacityMember(name string, desired, ready int32) v1beta1.CandidatePlacement {
	return v1beta1.CandidatePlacement{
		Cluster: name, ObservationKnown: true, ReadyReplicas: ready,
		Allocation: &v1beta1.CandidateAllocationStatus{
			ClusterUID: "registered-member", CurrentReplicas: desired, DesiredReplicas: desired,
		},
	}
}

func TestCapacityMetricsFollowDesiredCount(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := capacitySource()
	for _, counts := range []struct{ desired, ready int32 }{{100, 100}, {70, 70}, {70, 40}, {70, 0}, {100, 70}, {0, 0}} {
		source.Status.Placement.Plan.RequestedReplicas = int64(counts.desired)
		source.Status.Placement.Plan.AssignedReplicas = int64(counts.desired)
		recordPlacement(source, placementResult{candidates: []v1beta1.CandidatePlacement{capacityMember("member-a", counts.desired, counts.ready)}})
		require.Equal(t, float64(counts.desired), testutil.ToFloat64(placementTotalDesiredReplicas.WithLabelValues(source.Namespace, source.Name)))
		require.Equal(t, float64(counts.desired), testutil.ToFloat64(placementCandidateDesiredReplicas.WithLabelValues(source.Namespace, source.Name, "member-a")))
		require.Equal(t, float64(counts.ready), testutil.ToFloat64(placementCandidateReady.WithLabelValues(source.Namespace, source.Name, "member-a")))
		require.Equal(t, float64(1), testutil.ToFloat64(placementCapacityObservationKnown.WithLabelValues(source.Namespace, source.Name)))
	}
}

func TestCapacityMetricsKeepUnassignedDemand(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := capacitySource()
	source.Status.Placement.Plan.AssignedReplicas = 60
	source.Status.Placement.Plan.UnassignedReplicas = 40
	recordCapacity(source, []v1beta1.CandidatePlacement{capacityMember("member-a", 60, 60)})
	require.Equal(t, float64(100), testutil.ToFloat64(placementTotalDesiredReplicas.WithLabelValues(source.Namespace, source.Name)))
	require.Equal(t, float64(60), testutil.ToFloat64(placementCandidateDesiredReplicas.WithLabelValues(source.Namespace, source.Name, "member-a")))
}

func TestCapacityMetricsUseServingPrimaryUnits(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := capacitySource()
	source.Status.Placement.Plan.RequestedReplicas = 30
	source.Status.Placement.Plan.AssignedReplicas = 30
	components := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	componentMinimums := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 30, v1beta1.DecoderComponent: 20}
	statuses := map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		v1beta1.EngineComponent:  {ReadyReplicas: 30},
		v1beta1.DecoderComponent: {ReadyReplicas: 10},
	}
	ready := placementReadyReplicas(components, statuses, componentMinimums)
	recordPlacement(source, placementResult{candidates: []v1beta1.CandidatePlacement{capacityMember("member-a", 30, ready)}})
	require.Equal(t, float64(30), testutil.ToFloat64(placementTotalDesiredReplicas.WithLabelValues(source.Namespace, source.Name)))
	require.Equal(t, float64(30), testutil.ToFloat64(placementCandidateDesiredReplicas.WithLabelValues(source.Namespace, source.Name, "member-a")))
	require.Equal(t, float64(15), testutil.ToFloat64(placementCandidateReady.WithLabelValues(source.Namespace, source.Name, "member-a")))
}

func TestCapacityMetricsEntirelyUnassignedTarget(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := capacitySource()
	source.Status.Placement.Plan.AssignedReplicas = 0
	source.Status.Placement.Plan.UnassignedReplicas = 100
	recordCapacity(source, nil)
	require.Equal(t, float64(100), testutil.ToFloat64(placementTotalDesiredReplicas.WithLabelValues(source.Namespace, source.Name)))
	require.Zero(t, testutil.CollectAndCount(placementCandidateDesiredReplicas))
	require.Equal(t, float64(1), testutil.ToFloat64(placementCapacityObservationKnown.WithLabelValues(source.Namespace, source.Name)))
}

func TestCapacityMetricsRejectUnknownAndIncompleteObservations(t *testing.T) {
	for _, mutate := range []struct {
		name         string
		apply        func(*v1beta1.InferenceService, *[]v1beta1.CandidatePlacement)
		desiredKnown bool
	}{
		{"observation grace", func(_ *v1beta1.InferenceService, members *[]v1beta1.CandidatePlacement) {
			(*members)[1].ObservationKnown = false
		}, true},
		{"missing member", func(_ *v1beta1.InferenceService, members *[]v1beta1.CandidatePlacement) { *members = (*members)[:1] }, true},
		{"duplicate member", func(_ *v1beta1.InferenceService, members *[]v1beta1.CandidatePlacement) {
			(*members)[1].Cluster = (*members)[0].Cluster
		}, true},
		{"missing allocation", func(_ *v1beta1.InferenceService, members *[]v1beta1.CandidatePlacement) {
			(*members)[1].Allocation = nil
		}, true},
		{"stale generation", func(source *v1beta1.InferenceService, _ *[]v1beta1.CandidatePlacement) { source.Generation++ }, false},
		{"wrong source", func(source *v1beta1.InferenceService, _ *[]v1beta1.CandidatePlacement) {
			source.Status.Placement.Plan.SourceUID = "other"
		}, false},
		{"no plan", func(source *v1beta1.InferenceService, _ *[]v1beta1.CandidatePlacement) {
			source.Status.Placement.Plan = nil
		}, false},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			resetPlacementMetrics()
			t.Cleanup(resetPlacementMetrics)
			source := capacitySource()
			members := []v1beta1.CandidatePlacement{capacityMember("member-a", 60, 60), capacityMember("member-b", 40, 40)}
			recordCapacity(source, members)
			mutate.apply(source, &members)
			recordCapacity(source, members)
			if mutate.name == "observation grace" {
				require.Equal(t, 2, testutil.CollectAndCount(placementCandidateDesiredReplicas), "unknown readiness must not erase desired allocations")
			} else {
				require.Zero(t, testutil.CollectAndCount(placementCandidateDesiredReplicas), "invalid allocations must not publish a partial target")
			}
			require.Zero(t, testutil.ToFloat64(placementCapacityObservationKnown.WithLabelValues(source.Namespace, source.Name)))
			if mutate.desiredKnown {
				require.Equal(t, float64(100), testutil.ToFloat64(placementTotalDesiredReplicas.WithLabelValues(source.Namespace, source.Name)))
			} else {
				require.Zero(t, testutil.CollectAndCount(placementTotalDesiredReplicas))
			}
		})
	}
}

func TestCapacityMetricsTeardownPreservesOtherServices(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := capacitySource()
	members := []v1beta1.CandidatePlacement{capacityMember("member-a", 100, 100)}
	recordCapacity(source, members)
	other := source.DeepCopy()
	other.Namespace = "team-b"
	recordCapacity(other, members)
	DeleteForISVC(source.Namespace, source.Name)
	for _, metric := range []*prometheus.GaugeVec{placementTotalDesiredReplicas, placementCandidateDesiredReplicas, placementCapacityObservationKnown} {
		require.Equal(t, 1, testutil.CollectAndCount(metric))
	}
	require.Equal(t, float64(100), testutil.ToFloat64(placementCandidateDesiredReplicas.WithLabelValues(other.Namespace, other.Name, "member-a")))
}

func TestCapacityMetricsKeepMemberTargetsAcrossObservationChanges(t *testing.T) {
	resetPlacementMetrics()
	t.Cleanup(resetPlacementMetrics)
	source := capacitySource()
	members := []v1beta1.CandidatePlacement{capacityMember("member-a", 60, 60), capacityMember("member-b", 40, 0)}
	recordPlacement(source, placementResult{candidates: members})
	require.Equal(t, float64(60), testutil.ToFloat64(placementCandidateDesiredReplicas.WithLabelValues(source.Namespace, source.Name, "member-a")))
	require.Equal(t, float64(40), testutil.ToFloat64(placementCandidateDesiredReplicas.WithLabelValues(source.Namespace, source.Name, "member-b")))
	require.Equal(t, float64(1), testutil.ToFloat64(placementCapacityObservationKnown.WithLabelValues(source.Namespace, source.Name)))
	members[1].ObservationKnown = false
	recordPlacement(source, placementResult{candidates: members})
	require.Equal(t, float64(40), testutil.ToFloat64(placementCandidateDesiredReplicas.WithLabelValues(source.Namespace, source.Name, "member-b")))
	require.Zero(t, testutil.ToFloat64(placementCapacityObservationKnown.WithLabelValues(source.Namespace, source.Name)))
	source.Status.Placement.Plan.RequestedReplicas = 60
	source.Status.Placement.Plan.AssignedReplicas = 60
	recordCapacity(source, members[:1])
	require.Equal(t, 1, testutil.CollectAndCount(placementCandidateDesiredReplicas), "removed members must not retain desired metrics")
}

func TestCapacityMetricsRetiredAndDrainingMembers(t *testing.T) {
	for _, tc := range []struct {
		name           string
		current, ready int32
		observed       bool
		want           float64
	}{
		{"unknown retired count", 0, 20, false, 1},
		{"confirmed retired zero", 0, 0, true, 1},
		{"contradictory retired readiness", 0, 20, true, 0},
		{"known draining capacity", 20, 20, true, 1},
		{"unknown draining capacity", 20, 20, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetPlacementMetrics()
			t.Cleanup(resetPlacementMetrics)
			source := capacitySource()
			retiring := capacityMember("member-b", 0, tc.ready)
			retiring.Allocation.CurrentReplicas = tc.current
			retiring.ObservationKnown = tc.observed
			recordCapacity(source, []v1beta1.CandidatePlacement{capacityMember("member-a", 100, 100), retiring})
			require.Equal(t, 2, testutil.CollectAndCount(placementCandidateDesiredReplicas))
			require.Zero(t, testutil.ToFloat64(placementCandidateDesiredReplicas.WithLabelValues(source.Namespace, source.Name, "member-b")))
			require.Equal(t, tc.want, testutil.ToFloat64(placementCapacityObservationKnown.WithLabelValues(source.Namespace, source.Name)))
		})
	}
}
