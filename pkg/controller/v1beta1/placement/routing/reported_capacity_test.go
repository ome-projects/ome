package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	kptr "k8s.io/utils/ptr"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func reportedCapacityPolicy(t *testing.T) ResolvedCapacityPolicy {
	t.Helper()
	cfg := singleSampleConfig()
	cfg.Mode = v1beta1.RoutingCapacityModeReported
	return testCapacityPolicy(t, cfg)
}

func TestBuildSpec_ReportedCapacityMode_WeightsByReportNotReplicas(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []float64
		want   []int32
	}{
		{"equal bottleneck", []float64{30, 30}, []int32{1, 1}},
		{"fractional capacities", []float64{0.25, 0.75}, []int32{1, 3}},
		{"zero capacity", []float64{0, 30}, []int32{0, 1}},
		{"all zero", []float64{0, 0}, []int32{0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// One ready replica on a lets its report apply without capping it.
			isvc := splitISVC([]string{"a", "b"}, []int32{60, 80}, []int32{1, 80})
			isvc.Spec.Routing = &v1beta1.RoutingSpec{CapacityFactors: map[string]resource.Quantity{
				"a": resource.MustParse("100"), "b": resource.MustParse("0.01"),
			}}
			poller := newTestPoller(t)
			policy := reportedCapacityPolicy(t)
			for i := range tc.values {
				target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[i], "")
				require.True(t, poller.record(target, policy, &tc.values[i], ""))
			}
			spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, policy, nil, poller)
			require.Len(t, spec.Entries, 2)
			for i, entry := range spec.Entries {
				assert.Equal(t, tc.want[i], entry.Weight)
				assert.Equal(t, v1beta1.RoutingCapacityModeReported, spec.CapacityMode)
				require.NotNil(t, entry.Capacity.ReportedWeight)
				assert.InDelta(t, tc.values[i], entry.Capacity.ReportedWeight.AsApproximateFloat64(), 1e-9)
				assert.Nil(t, entry.Capacity.Reported)
				assert.Nil(t, entry.Capacity.Factor, "the report already weights the hardware")
				assert.Equal(t, v1beta1.CapacitySourceEndpoint, entry.Capacity.Source)
			}
			if tc.want[0] == 0 && tc.want[1] == 0 {
				assert.Equal(t, metav1.ConditionFalse, status)
				assert.Equal(t, v1beta1.TrafficMapReasonNoRoutableCapacity, reason)
			} else {
				assert.Equal(t, metav1.ConditionTrue, status)
			}
		})
	}
}

func TestCapacityPollerObservation_PollFailsAfterAReport_KeepsItUntilItAges(t *testing.T) {
	poller := newTestPoller(t)
	policy := reportedCapacityPolicy(t)
	target := testCapacityTarget("a", "https://a.example")
	clk := poller.Clock.(*clocktesting.FakeClock)

	value, reason := poller.observation(target, policy)
	assert.Nil(t, value)
	assert.NotEmpty(t, reason)
	require.True(t, poller.record(target, policy, kptr.To(30.5), ""))
	clk.Step(policy.Capacity.MaxAge - time.Second)
	require.True(t, poller.record(target, policy, nil, "request timed out"))
	value, reason = poller.observation(target, policy)
	require.NotNil(t, value)
	assert.Equal(t, 30.5, *value)
	assert.Equal(t, "request timed out", reason)

	clk.Step(time.Second)
	value, reason = poller.observation(target, policy)
	assert.Nil(t, value, "failed polls cannot renew the last report")
	assert.Contains(t, reason, "stale")
	require.True(t, poller.record(target, policy, kptr.To(0.0), ""))
	value, reason = poller.observation(target, policy)
	require.NotNil(t, value)
	assert.Zero(t, *value, "zero is an accepted observation")
	assert.Empty(t, reason)
}

func TestPollOne_ReportBodies_AcceptsWeightsRejectsStaleOrMalformed(t *testing.T) {
	policy := reportedCapacityPolicy(t)
	for _, tc := range []struct {
		name string
		body map[string]any
		want *float64
	}{
		{"fraction", map[string]any{"weight": 0.375, "observedAt": capacityTestNow}, kptr.To(0.375)},
		{"zero", map[string]any{"weight": 0, "observedAt": capacityTestNow}, kptr.To(0.0)},
		{"replicas are not weights", map[string]any{"servable": 30, "observedAt": capacityTestNow}, nil},
		{"a servable of another type is ignored", map[string]any{"weight": 2.5, "servable": 2.5, "observedAt": capacityTestNow}, kptr.To(2.5)},
		{"negative", map[string]any{"weight": -1, "observedAt": capacityTestNow}, nil},
		{"missing timestamp", map[string]any{"weight": 30}, nil},
		{"stale", map[string]any{"weight": 30, "observedAt": capacityTestNow.Add(-policy.Capacity.MaxAge)}, nil},
		{"future", map[string]any{"weight": 30, "observedAt": capacityTestNow.Add(time.Second)}, nil},
		{"too large", map[string]any{"weight": 3e9, "observedAt": capacityTestNow}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(tc.body))
			}))
			defer srv.Close()
			poller := newTestPoller(t)
			value, _, reason := poller.pollOneDetailed(context.Background(), testCapacityTarget("a", srv.URL), policy.Capacity)
			assert.Equal(t, tc.want, value)
			assert.Equal(t, tc.want == nil, reason != "")
		})
	}
}

func TestCapacityPoller_SamplesAgeOut_QuorumHoldsOnlyWhileTwoAreFresh(t *testing.T) {
	poller := newTestPoller(t)
	config := singleSampleConfig()
	config.Mode = v1beta1.RoutingCapacityModeReported
	config.Samples, config.Quorum = 3, 2
	policy := testCapacityPolicy(t, config)
	target := testCapacityTarget("a", "https://a.example")
	clk := poller.Clock.(*clocktesting.FakeClock)
	start := clk.Now()
	poller.record(target, policy, kptr.To(0.75), "")
	assert.Nil(t, poller.Reported(target, policy), "one sample cannot supply weight")
	clk.Step(time.Second)
	poller.record(target, policy, kptr.To(0.75), "")
	clk.Step(time.Second)
	poller.record(target, policy, kptr.To(0.25), "")
	require.Equal(t, kptr.To(0.75), poller.Reported(target, policy), "one low reading cannot lower the weight")
	poller.record(target, policy, nil, "request timed out")
	clk.SetTime(start.Add(config.MaxAge))
	value, reason := poller.observation(target, policy)
	assert.Equal(t, kptr.To(0.25), value, "two fresh samples still form a quorum, and the smaller one applies")
	assert.Equal(t, "request timed out", reason, "expiry is not a successful poll")
	clk.Step(time.Second)
	assert.Nil(t, poller.Reported(target, policy), "one remaining sample cannot maintain weight")

	poller.record(target, policy, kptr.To(0.0), "")
	assert.Equal(t, kptr.To(0.0), poller.Reported(target, policy), "an accepted zero withdraws capacity")
}

func TestCapacityStateObserve_WindowOfReports_TakesTheLowerMedian(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   v1beta1.RoutingCapacityMode
		window []float64
		want   float64
		ok     bool
	}{
		{"below quorum", v1beta1.RoutingCapacityModeReported, []float64{4}, 0, false},
		{"one low reading cannot lower it", v1beta1.RoutingCapacityModeReported, []float64{4, 4, 1}, 4, true},
		{"one high reading cannot raise it", v1beta1.RoutingCapacityModeReported, []float64{2, 2, 9}, 2, true},
		{"an even window takes the smaller reading", v1beta1.RoutingCapacityModeReported, []float64{4, 2}, 2, true},
		{"a ceiling takes the quorum-th smallest", v1beta1.RoutingCapacityModeCeiling, []float64{4, 2}, 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := singleSampleConfig()
			config.Mode = tc.mode
			config.Samples, config.Quorum = 3, 2
			st := &capacityState{}
			var got float64
			var ok bool
			for _, v := range tc.window {
				got, ok = st.observe(v, capacityTestNow.Add(time.Hour), config)
			}
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBuildSpec_SomeClustersWithoutReports_FallBackPerCluster(t *testing.T) {
	// a reports more than its replicas; b and c have no report, and c has no
	// ready replicas either.
	isvc := splitISVC([]string{"a", "b", "c"}, []int32{60, 80, 40}, []int32{60, 50, 0})
	isvc.Spec.Routing = &v1beta1.RoutingSpec{CapacityFactors: map[string]resource.Quantity{
		"a": resource.MustParse("3"), "b": resource.MustParse("2"),
	}}
	poller := newTestPoller(t)
	policy := reportedCapacityPolicy(t)
	target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[0], "")
	require.True(t, poller.record(target, policy, kptr.To(90.0), ""))

	spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, policy, nil, poller)
	require.Len(t, spec.Entries, 3)
	// a weighs its report, neither capped by its replicas nor scaled by its
	// factor; b weighs min(80, 50) * 2 = 100; c weighs zero.
	assert.Equal(t, int32(9), spec.Entries[0].Weight)
	assert.Equal(t, int32(10), spec.Entries[1].Weight)
	assert.Zero(t, spec.Entries[2].Weight)
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, reason)

	reported := spec.Entries[0].Capacity
	assert.Equal(t, v1beta1.CapacitySourceEndpoint, reported.Source)
	require.NotNil(t, reported.ReportedWeight)
	assert.Nil(t, reported.Factor)
	assert.Empty(t, reported.FallbackReason)
	assert.True(t, spec.Entries[0].Healthy)
	for i, healthy := range []bool{true, false} {
		fallback := spec.Entries[i+1].Capacity
		assert.Equal(t, v1beta1.CapacitySourceControlPlane, fallback.Source)
		assert.Nil(t, fallback.ReportedWeight)
		assert.NotEmpty(t, fallback.FallbackReason)
		assert.Equal(t, healthy, spec.Entries[i+1].Healthy)
	}
	require.NotNil(t, spec.Entries[1].Capacity.Factor)
	assert.Equal(t, "2", spec.Entries[1].Capacity.Factor.String())
	assert.Equal(t, 2, capacityFallbackCount(spec))
}

func TestBuildSpec_ReportedModeNoReportsNoReadyReplicas_AllHomesUnready(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{60, 80}, []int32{0, 0})
	spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, reportedCapacityPolicy(t), nil, newTestPoller(t))
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesUnready, reason)
}

func TestBuildSpec_ReportedModeWithProbeAndDrainGates_ZerosGatedClusters(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{60, 60}, []int32{60, 60})
	poller := newTestPoller(t)
	policy := reportedCapacityPolicy(t)
	for i := range isvc.Status.Placement.Candidates {
		target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[i], "")
		require.True(t, poller.record(target, policy, kptr.To(30.0), ""))
	}
	prober, probePolicy := failingProbeState(t, isvc, AllFailedPolicyDrain, "a")
	spec, _, _ := buildSpec(isvc, probePolicy, policy, prober, poller)
	assert.Zero(t, spec.Entries[0].Weight)
	assert.Positive(t, spec.Entries[1].Weight)

	isvc.Status.Placement.Plan = &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeSplit}
	isvc.Status.Placement.Candidates[1].Allocation = &v1beta1.CandidateAllocationStatus{CurrentReplicas: 60, DrainRequested: true}
	spec, _, _ = buildSpec(isvc, probePolicy, policy, prober, poller)
	assert.Zero(t, spec.Entries[0].Weight)
	assert.Zero(t, spec.Entries[1].Weight)
}

func TestBuildSpec_ReportedModeSourceNotPlacedOrNoEndpoint_EmptyWithReason(t *testing.T) {
	for _, missingPlacement := range []bool{false, true} {
		isvc := splitISVC([]string{"a"}, []int32{60}, []int32{60})
		wantReason := v1beta1.TrafficMapReasonNoAddressableHome
		if missingPlacement {
			isvc.Status.Placement = nil
			wantReason = v1beta1.TrafficMapReasonNotPlaced
		} else {
			isvc.Status.Placement.Candidates[0].Endpoint = nil
		}
		spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, reportedCapacityPolicy(t), nil, nil)
		assert.Empty(t, spec.Entries)
		assert.Equal(t, v1beta1.RoutingCapacityModeReported, spec.CapacityMode)
		assert.Equal(t, metav1.ConditionFalse, status)
		assert.Equal(t, wantReason, reason)
	}
}

func TestBuildSpec_ReportedModeZeroReplicas_AdmittedPhaseDecidesNotCounts(t *testing.T) {
	// A ready replica lets the report apply, and a zero admitted count does not cap it.
	isvc := splitISVC([]string{"a"}, []int32{0}, []int32{1})
	poller := newTestPoller(t)
	policy := reportedCapacityPolicy(t)
	candidate := &isvc.Status.Placement.Candidates[0]
	target := targetForCandidate(isvc, candidate, "")
	require.True(t, poller.record(target, policy, kptr.To(30.0), ""))
	spec, _, _ := buildSpec(isvc, ResolvedProbePolicy{}, policy, nil, poller)
	require.Len(t, spec.Entries, 1)
	assert.Positive(t, spec.Entries[0].Weight, "replica counts do not override the router's capacity")

	candidate.Phase = v1beta1.CandidatePhaseAdmitting
	spec, _, _ = buildSpec(isvc, ResolvedProbePolicy{}, policy, nil, poller)
	assert.Empty(t, spec.Entries, "only admitted candidates can receive traffic")

	candidate.Phase = v1beta1.CandidatePhaseAdmitted
	isvc.Status.Placement.Plan = &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeSplit}
	candidate.Allocation = &v1beta1.CandidateAllocationStatus{}
	spec, _, _ = buildSpec(isvc, ResolvedProbePolicy{}, policy, nil, poller)
	assert.Empty(t, spec.Entries, "retired allocations cannot reuse an old report")
}

func TestWeights_ReportedZeroUnderPreserveTraffic_StaysZero(t *testing.T) {
	clusters := []ClusterCapacity{
		{Mode: v1beta1.RoutingCapacityModeReported, ReportedWeight: kptr.To(resource.MustParse("0.75")), Reachable: kptr.To(false)},
		{Mode: v1beta1.RoutingCapacityModeReported, Allocated: 60, Ready: 0, Reachable: kptr.To(false)},
	}
	assert.Equal(t, []int32{1, 0}, weights(clusters, AllFailedPolicyPreserveTraffic))
	assert.Equal(t, []int32{0, 0}, weights(clusters, AllFailedPolicyDrain))
}

func TestCapacityPollerObservation_ModeOrOwnerChanges_ReportNoLongerApplies(t *testing.T) {
	poller := newTestPoller(t)
	policy := reportedCapacityPolicy(t)
	target := testCapacityTarget("a", "https://a.example")
	require.True(t, poller.record(target, policy, kptr.To(30.5), ""))
	ceiling := policy.Capacity
	ceiling.Mode = v1beta1.RoutingCapacityModeCeiling
	value, _ := poller.observation(target, testCapacityPolicy(t, ceiling))
	assert.Nil(t, value)
	target.OwnerUID = "replacement"
	value, _ = poller.observation(target, policy)
	assert.Nil(t, value)
}

func TestResolveCapacityPolicy_ReportedGlobalMode_InlineBlocksDoNotInheritIt(t *testing.T) {
	cfg := controllerTestConfig()
	cfg.Capacity = reportedCapacityPolicy(t).Capacity
	resolved, err := ResolveCapacityPolicy(cfg, nil)
	require.NoError(t, err)
	assert.Equal(t, v1beta1.RoutingCapacityModeReported, resolved.Capacity.Mode)
	inline := &v1beta1.InferenceServiceSpec{Routing: &v1beta1.RoutingSpec{
		Capacity: controllerCapacitySpec(singleSampleConfig()),
	}}
	resolved, err = ResolveCapacityPolicy(cfg, inline)
	require.NoError(t, err)
	assert.Empty(t, resolved.Capacity.Mode, "a complete inline block does not inherit the global mode")
	inline.Routing.Capacity = &v1beta1.RoutingCapacitySpec{Disabled: true}
	resolved, err = ResolveCapacityPolicy(cfg, inline)
	require.NoError(t, err)
	assert.False(t, resolved.IsEnabled())
	cfg.Capacity.Path = ""
	_, err = ResolveCapacityPolicy(cfg, nil)
	assert.ErrorContains(t, err, "requires capacity polling")
}

func TestReconcile_ReportedCapacityOverHTTP_WeightsFollowReportsThenExpire(t *testing.T) {
	var unavailable [2]atomic.Bool
	var zeroFirst atomic.Bool
	clk := clocktesting.NewFakeClock(capacityTestNow)
	isvc := splitISVC([]string{"a", "b"}, []int32{60, 80}, []int32{0, 80})
	for i := range isvc.Status.Placement.Candidates {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if unavailable[i].Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			weight := 30.5
			if i == 1 {
				weight = 15.25
			} else if zeroFirst.Load() {
				weight = 0
			}
			_ = json.NewEncoder(w).Encode(CapacityReport{Weight: &weight, ObservedAt: clk.Now()})
		}))
		t.Cleanup(server.Close)
		endpoint, err := apis.ParseURL(server.URL)
		require.NoError(t, err)
		isvc.Status.Placement.Candidates[i].Endpoint = endpoint
	}
	config := controllerTestCapacityConfig()
	config.Mode = v1beta1.RoutingCapacityModeReported
	isvc.Spec.Routing = &v1beta1.RoutingSpec{Capacity: controllerCapacitySpec(config)}
	r, client := newReconciler(t, controllerTestConfig(), isvc)
	r.Capacity = newControllerCapacityPoller(t, NewObserverClient(), clk)
	setUnavailable := func(v bool) {
		for i := range unavailable {
			unavailable[i].Store(v)
		}
	}
	routableReason := func(tm *v1beta1.TrafficMap) string {
		for _, condition := range tm.Status.Conditions {
			if condition.Type == v1beta1.TrafficMapRoutable {
				return condition.Reason
			}
		}
		return ""
	}

	setUnavailable(true)
	reconcile(t, r)
	tm, exists := getTrafficMap(t, client)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Zero(t, tm.Spec.Entries[0].Weight, "no report and no ready replicas")
	assert.Positive(t, tm.Spec.Entries[1].Weight, "a cluster without a report keeps its replica count")
	assert.Nil(t, tm.Spec.Entries[1].Capacity.ReportedWeight)
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, tm.Spec.Entries[1].Capacity.Source)

	setUnavailable(false)
	clk.Step(config.Period)
	reconcile(t, r)
	tm, exists = getTrafficMap(t, client)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 2)
	unready := tm.Spec.Entries[0]
	assert.Zero(t, unready.Weight, "a report cannot route to a cluster without ready replicas")
	assert.False(t, unready.Healthy)
	assert.Nil(t, unready.Capacity.ReportedWeight)
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, unready.Capacity.Source)
	assert.Equal(t, reportedCapacityNotReadyReason, unready.Capacity.FallbackReason)
	assert.Positive(t, tm.Spec.Entries[1].Weight)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, routableReason(tm))

	// One ready replica applies a's accepted report at once, uncapped by that count.
	setCandidateReady(t, r, 0, 1)
	reconcile(t, r)
	tm, _ = getTrafficMap(t, client)
	assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	assert.True(t, tm.Spec.Entries[0].Healthy)
	assert.Equal(t, v1beta1.CapacitySourceEndpoint, tm.Spec.Entries[0].Capacity.Source)

	zeroFirst.Store(true)
	clk.Step(config.Period)
	reconcile(t, r)
	tm, _ = getTrafficMap(t, client)
	assert.Zero(t, tm.Spec.Entries[0].Weight)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.ReportedWeight, "a ready cluster's zero report applies")
	assert.True(t, tm.Spec.Entries[0].Capacity.ReportedWeight.IsZero())
	assert.False(t, tm.Spec.Entries[0].Healthy)
	assert.Positive(t, tm.Spec.Entries[1].Weight)

	setCandidateReady(t, r, 0, 0)
	setUnavailable(true)
	clk.Step(config.Period)
	reconcile(t, r)
	tm, _ = getTrafficMap(t, client)
	assert.Zero(t, tm.Spec.Entries[0].Weight)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.ReportedWeight, "a zero report applies without a ready replica")
	assert.True(t, tm.Spec.Entries[0].Capacity.ReportedWeight.IsZero())
	assert.Positive(t, tm.Spec.Entries[1].Weight)
	assert.NotNil(t, tm.Spec.Entries[1].Capacity.ReportedWeight, "a fresh report survives a failed poll")
	assert.NotEmpty(t, tm.Spec.Entries[1].Capacity.FallbackReason)

	clk.Step(config.MaxAge)
	reconcile(t, r)
	tm, _ = getTrafficMap(t, client)
	for _, entry := range tm.Spec.Entries {
		assert.Nil(t, entry.Capacity.ReportedWeight)
	}
	assert.Zero(t, tm.Spec.Entries[0].Weight)
	assert.Positive(t, tm.Spec.Entries[1].Weight, "expired reports fall back to the replica count")
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, routableReason(tm))

	// a reports again with one ready replica; b still cannot report.
	zeroFirst.Store(false)
	unavailable[0].Store(false)
	setCandidateReady(t, r, 0, 1)
	clk.Step(config.Period)
	reconcile(t, r)
	tm, _ = getTrafficMap(t, client)
	// 30.5 reported against 80 replicas.
	assert.Equal(t, int32(61), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(160), tm.Spec.Entries[1].Weight)
	assert.Equal(t, v1beta1.CapacitySourceEndpoint, tm.Spec.Entries[0].Capacity.Source)
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, tm.Spec.Entries[1].Capacity.Source)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, routableReason(tm))
}

// setCandidateReady stores one candidate's ready count in the source status.
func setCandidateReady(t *testing.T, r *Reconciler, index int, ready int32) {
	t.Helper()
	isvc := &v1beta1.InferenceService{}
	key := types.NamespacedName{Name: testName, Namespace: testNS}
	require.NoError(t, r.Get(context.Background(), key, isvc))
	isvc.Status.Placement.Candidates[index].ReadyReplicas = ready
	require.NoError(t, r.Status().Update(context.Background(), isvc))
}

// entryWeights lists the table's weights in entry order.
func entryWeights(spec v1beta1.TrafficMapSpec) []int32 {
	weights := make([]int32, len(spec.Entries))
	for i := range spec.Entries {
		weights[i] = spec.Entries[i].Weight
	}
	return weights
}

// A gateway can forward the capacity poll to routers that are not ready, so a
// report alone cannot make a cluster without ready replicas routable.
func TestBuildSpec_PositiveReportWithoutReadyReplica_DoesNotApply(t *testing.T) {
	isvc := splitISVC([]string{"a", "b", "c"}, []int32{9, 9, 6}, []int32{9, 9, 0})
	poller := newTestPoller(t)
	policy := reportedCapacityPolicy(t)
	for i, value := range []float64{9, 9, 6} {
		target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[i], "")
		require.True(t, poller.record(target, policy, kptr.To(value), ""))
	}
	spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, policy, nil, poller)
	require.Len(t, spec.Entries, 3)
	assert.Equal(t, []int32{1, 1, 0}, entryWeights(spec))
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, reason)
	unready := spec.Entries[2]
	assert.False(t, unready.Healthy)
	assert.Nil(t, unready.Capacity.ReportedWeight, "a report that does not apply is not published")
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, unready.Capacity.Source)
	assert.Equal(t, reportedCapacityNotReadyReason, unready.Capacity.FallbackReason)
	assert.Equal(t, int32(6), unready.Capacity.Allocated)
	assert.Zero(t, unready.Capacity.Ready)
	assert.Zero(t, capacityFallbackCount(spec), "a current report that waits for a ready replica is not a fallback")
	for _, entry := range spec.Entries[:2] {
		assert.True(t, entry.Healthy)
		assert.Equal(t, v1beta1.CapacitySourceEndpoint, entry.Capacity.Source)
	}

	// PreserveTraffic ignores only failed probes, so it cannot route to c either.
	prober, probePolicy := failingProbeState(t, isvc, AllFailedPolicyPreserveTraffic, "a", "b", "c")
	spec, status, reason = buildSpec(isvc, probePolicy, policy, prober, poller)
	assert.Equal(t, []int32{1, 1, 0}, entryWeights(spec))
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesProbeFailed, reason)

	// One ready replica applies the full report, not a weight capped at one replica.
	isvc.Status.Placement.Candidates[2].ReadyReplicas = 1
	spec, _, _ = buildSpec(isvc, ResolvedProbePolicy{}, policy, nil, poller)
	assert.Equal(t, []int32{3, 3, 2}, entryWeights(spec))
	assert.True(t, spec.Entries[2].Healthy)
	require.NotNil(t, spec.Entries[2].Capacity.ReportedWeight)
	assert.Equal(t, v1beta1.CapacitySourceEndpoint, spec.Entries[2].Capacity.Source)
	assert.Empty(t, spec.Entries[2].Capacity.FallbackReason)

	// After a failed poll c keeps its cached report and the poll's reason. Without
	// a ready replica the report waits, and the fallback count includes c.
	isvc.Status.Placement.Candidates[2].ReadyReplicas = 0
	target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[2], "")
	require.True(t, poller.record(target, policy, nil, "capacity report unavailable"))
	spec, _, _ = buildSpec(isvc, ResolvedProbePolicy{}, policy, nil, poller)
	assert.Equal(t, []int32{1, 1, 0}, entryWeights(spec))
	assert.Nil(t, spec.Entries[2].Capacity.ReportedWeight)
	assert.Equal(t, "capacity report unavailable", spec.Entries[2].Capacity.FallbackReason)
	assert.Equal(t, 1, capacityFallbackCount(spec))
}

// With no ready replica on any cluster every positive report waits, so the table
// carries no report. It gets the Ceiling table's weights and Routable reason, and
// both tables keep positive allocations, so a publisher may replay either one.
func TestBuildSpec_NoClusterReadyWithPositiveReports_MatchesCeiling(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{9, 6}, []int32{0, 0})
	poller := newTestPoller(t)
	reported := reportedCapacityPolicy(t)
	ceiling := testCapacityPolicy(t, singleSampleConfig())
	for i := range isvc.Status.Placement.Candidates {
		target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[i], "")
		require.True(t, poller.record(target, reported, kptr.To(6.0), ""))
		require.True(t, poller.record(target, ceiling, kptr.To(6.0), ""))
	}
	spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, reported, nil, poller)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesUnready, reason)
	for i, entry := range spec.Entries {
		assert.Zero(t, entry.Weight)
		assert.False(t, entry.Healthy)
		assert.Nil(t, entry.Capacity.ReportedWeight)
		assert.Equal(t, reportedCapacityNotReadyReason, entry.Capacity.FallbackReason)
		assert.Equal(t, isvc.Status.Placement.Candidates[i].AdmittedReplicas, entry.Capacity.Allocated)
	}

	want, wantStatus, wantReason := buildSpec(isvc, ResolvedProbePolicy{}, ceiling, nil, poller)
	assert.Equal(t, wantStatus, status)
	assert.Equal(t, wantReason, reason)
	assert.Equal(t, entryWeights(want), entryWeights(spec))
	for i := range want.Entries {
		assert.Equal(t, want.Entries[i].Healthy, spec.Entries[i].Healthy)
		assert.Positive(t, want.Entries[i].Capacity.Allocated)
	}
}

// A zero report adds no weight or health, so it applies without a ready replica.
// The table carries it and is published at once. The Ceiling table for the same
// reports allocates zero to that cluster, so no publisher replays it either.
func TestBuildSpec_ZeroReportWithoutReadyReplica_StillApplies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reports []float64
	}{
		{"every cluster reports zero", []float64{0, 0}},
		{"one cluster reports zero and one waits", []float64{0, 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := splitISVC([]string{"a", "b"}, []int32{9, 6}, []int32{0, 0})
			poller := newTestPoller(t)
			reported := reportedCapacityPolicy(t)
			ceiling := testCapacityPolicy(t, singleSampleConfig())
			for i, value := range tc.reports {
				target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[i], "")
				require.True(t, poller.record(target, reported, kptr.To(value), ""))
				require.True(t, poller.record(target, ceiling, kptr.To(value), ""))
			}
			spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, reported, nil, poller)
			require.Len(t, spec.Entries, 2)
			assert.Equal(t, metav1.ConditionFalse, status)
			assert.Equal(t, v1beta1.TrafficMapReasonNoRoutableCapacity, reason)
			assert.Equal(t, []int32{0, 0}, entryWeights(spec))
			for i, value := range tc.reports {
				capacity := spec.Entries[i].Capacity
				assert.False(t, spec.Entries[i].Healthy)
				if value > 0 {
					assert.Nil(t, capacity.ReportedWeight)
					assert.Equal(t, reportedCapacityNotReadyReason, capacity.FallbackReason)
					continue
				}
				require.NotNil(t, capacity.ReportedWeight)
				assert.True(t, capacity.ReportedWeight.IsZero())
				assert.Equal(t, v1beta1.CapacitySourceEndpoint, capacity.Source)
				assert.Empty(t, capacity.FallbackReason)
			}
			assert.Zero(t, capacityFallbackCount(spec))

			want, _, _ := buildSpec(isvc, ResolvedProbePolicy{}, ceiling, nil, poller)
			assert.Equal(t, entryWeights(want), entryWeights(spec))
			assert.Zero(t, want.Entries[0].Capacity.Allocated)
			for i := range want.Entries {
				assert.Equal(t, want.Entries[i].Healthy, spec.Entries[i].Healthy)
			}
		})
	}
}
