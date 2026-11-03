package endpoint

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
)

// The pure rules of the retained plan: when a lost placement may replay it, and
// what a valid snapshot and a valid capture are.

func TestTrafficMapFallbackEligibility_LossAndLagShapes_ClassifiesEligibleAndKnown(t *testing.T) {
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
			name: "override invalid with annotation intent", known: true,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Annotations = map[string]string{constants.TrafficDrainAnnotation: "malformed"}
				override := apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapOverrideActive,
				)
				override.Status = metav1.ConditionUnknown
				override.Reason = v1beta1.TrafficMapReasonOverridesInvalid
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
			name: "annotation removal precedes invalid-override status", known: false,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				override := apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapOverrideActive,
				)
				override.Status = metav1.ConditionUnknown
				override.Reason = v1beta1.TrafficMapReasonOverridesInvalid
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
			name: "all clusters unready", known: true,
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
			name: "all clusters probe failed", known: true,
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
			name: "placement unknown during single-cluster loss", known: false,
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
			name: "placement unknown during all-clusters-unready observation", known: false,
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				owner.Status.Placement = &v1beta1.PlacementStatus{Phase: v1beta1.PlacementPhasePlaced}
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
				apimeta.FindStatusCondition(
					trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
				).Reason = v1beta1.TrafficMapReasonAllHomesUnready
			},
		},
		{
			name: "definitive all-clusters-unready observation", known: true,
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

	// A Reported table without a report has fallen back to replica counts, so
	// it must classify exactly like a Ceiling table.
	for _, mode := range []v1beta1.RoutingCapacityMode{v1beta1.RoutingCapacityModeCeiling, v1beta1.RoutingCapacityModeReported} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					owner, trafficMap := eligiblePlacementLossPublisherObjects()
					trafficMap.Spec.CapacityMode = mode
					if tt.mutate != nil {
						tt.mutate(owner, trafficMap)
					}
					eligible, known := trafficMapFallbackEligibility(trafficMap, owner)
					assert.Equal(t, tt.eligible, eligible)
					assert.Equal(t, tt.known, known)
				})
			}
		})
	}
}

func TestTrafficMapFallbackEligibility_AllHomesUnreadyOptIn_EligibleOnlyWhenDefinite(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*v1beta1.InferenceService, *v1beta1.TrafficMap)
		allow    bool
		eligible bool
		known    bool
	}{
		{
			name: "reported table without a report replays like ceiling mode", allow: true, eligible: true, known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.CapacityMode = v1beta1.RoutingCapacityModeReported
			},
		},
		{
			name: "a reported zero is never replayed", allow: true, known: true,
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.CapacityMode = v1beta1.RoutingCapacityModeReported
				withZeroReports(trafficMap)
			},
		},
		{name: "definitive all-clusters-unready", allow: true, eligible: true, known: true},
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

func TestValidateTrafficMapLastPositive_ValidAndEachDefect_AcceptsOrRejects(t *testing.T) {
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

func TestValidateTrafficMapLastPositiveFromEarlierGeneration_OlderNotNewer_Accepts(t *testing.T) {
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

func TestPublisherClaimsReplayCompatibleForAllHomesUnready_ClaimsVary_ZeroesMayDrop(t *testing.T) {
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

func TestValidateTrafficMapPlanCapture_ValidAndEachDefect_SortsOrRejects(t *testing.T) {
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
