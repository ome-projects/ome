package coordination

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A placement surge pause holds a replica's workload at its placement
// replica limit while an autoscaler's request stays in spec.replicas
// (desiredFromIR), so no Instance exists above the limit. The batch step
// and the pairing gates count such a replica at the limit: its budgets
// scale with the limit, its batches end at the limit, and a protocol
// change keeps the limit less its maxUnavailable paired.

// withPlacementLimit has an autoscaler request requested replicas of ir
// while placement holds the workload at limit; with pause false the
// workload grows to the request.
func withPlacementLimit(ir *v1beta1.InferenceReplica, requested, limit int32, pause bool) *v1beta1.InferenceReplica {
	ir.Spec.Replicas = ptr.To(requested)
	ir.Spec.PlacementReplicaLimit = ptr.To(limit)
	ir.Spec.PlacementExecution = &v1beta1.PlacementExecutionPolicy{PlanID: "plan", Revision: 1, PauseSurge: pause}
	return ir
}

// A member's desired count and step, drain-first and surging alike, are
// read against the paused limit: 22 requested and held at 12, a 10% budget
// is 2. Growth that is not paused, or a limit above the request, leaves the
// request as the count.
func TestReadBatchMember_CountsThePausedPlacementLimit(t *testing.T) {
	dec := v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{MaxUnavailable: ptr.To(intstr.FromString("25%")), MaxSurge: ptr.To(intstr.FromString("25%"))}
	cases := []struct {
		name             string
		requested, limit int32
		pause            bool
		desired, step    int32
	}{
		{name: "paused under the request", requested: 22, limit: 12, pause: true, desired: 12, step: 2},
		{name: "growth not paused", requested: 22, limit: 12, pause: false, desired: 22, step: 3},
		{name: "paused over the request", requested: 10, limit: 12, pause: true, desired: 10, step: 1},
	}
	kinds := []struct {
		kind     stepKind
		strategy v1beta1.UpdateStrategyType
	}{
		{stepDrain, v1beta1.UpdateStrategyRecreatePod},
		{stepSurge, v1beta1.UpdateStrategySurgeThenDrain},
	}
	for _, tc := range cases {
		for _, k := range kinds {
			ir := withPlacementLimit(batchReplica(dec, batchMemberFixture{desired: tc.requested, step: "10%", surge: "10%", strategy: k.strategy}), tc.requested, tc.limit, tc.pause)
			if m := readBatchMember(ir, dec, "", pacing, k.kind); m.desired != tc.desired || m.step != tc.step {
				t.Errorf("%s, %s: desired %d step %d, want desired %d step %d", tc.name, k.strategy, m.desired, m.step, tc.desired, tc.step)
			}
		}
	}
}

// While paused at 12, a decoder requesting 22 replicas must not block its
// peer once those 12 serve: the prefill side's next start is admitted for a
// surging and a drain-first pair, under each prefill/decode rule set.
// Without the pause the decoder grows to 22, and the prefill side waits.
func TestEvaluateUpdateGate_BatchStepCountsThePausedPlacementLimit(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	strategies := []struct {
		strategy  v1beta1.UpdateStrategyType
		mechanism workloadtypes.UpdateStrategyType
	}{
		{v1beta1.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategySurgeThenDrain},
		{v1beta1.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategyRecreatePod},
	}
	rules := []struct {
		name     string
		defaults GroupDefaults
	}{
		{"none", GroupDefaults{}},
		{"keepRatio", keepRatio(5)},
		{"keepRatioDecoderFirst", keepRatioDecoderFirst(5)},
	}
	for _, s := range strategies {
		for _, r := range rules {
			for _, pause := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/pause=%t", s.strategy, r.name, pause), func(t *testing.T) {
					engine := batchMemberFixture{desired: 11, step: "10%", surge: "10%", strategy: s.strategy, promoted: 10}
					decoder := batchMemberFixture{desired: 12, step: "10%", surge: "10%", strategy: s.strategy, promoted: 12}
					isvc := batchService(batchGroupFixture{}, map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder})
					c := fakeclient.New(t, fakeclient.WithObjects(
						batchReplica(eng, engine),
						withPlacementLimit(batchReplica(dec, decoder), 22, 12, pause),
					))
					allowed, gate, reason := batchGateWith(t, isvc, c, eng, s.mechanism, 0, r.defaults)
					if pause && !allowed {
						t.Fatalf("the prefill side waits (gate %q, reason %q), but the paused decoder already serves every Instance it can run", gate, reason)
					}
					if !pause && (allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "waits for decoder batch")) {
						t.Fatalf("growth is not paused, so the decoder grows to 22 and the prefill side must wait for its batch: allowed=%v gate=%q reason=%q", allowed, gate, reason)
					}
				})
			}
		}
	}
}

// While paused at 12, the decoder keeps at least 10 of its 12 Instances
// paired through a protocol change: its 10% budget is 2 of the 12. The last
// v1 engine pod, its v2 replacement serving, may leave while 10 decoders
// keep a partner and must stay while fewer would. Without the pause the
// decoder grows to 22, and 19 must keep a partner.
func TestEvaluatePairingDrain_PausedDecoderKeepsTheLimitLessItsBudgetPaired(t *testing.T) {
	isvc := pairingISVC("v2")
	fleet := func(v1Decoders int, pause bool) []client.Object {
		decoder := replicaWith(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod)
		decoder.Spec.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{MaxUnavailable: ptr.To(intstr.FromString("10%"))}
		objs := []client.Object{
			sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, 6, 0),
			withPlacementLimit(decoder, 22, 12, pause),
		}
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 1, "v2", true)...)
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 5, 1, 0, "v1", true)...)
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 12-v1Decoders, 0, "v2", true)...)
		return append(objs, fleetPods(v1beta1.DecoderComponent, 12-v1Decoders, v1Decoders, 0, "v1", true)...)
	}
	source := []string{"llama-engine-5-default-0"}

	cases := []struct {
		name       string
		v1Decoders int
		pause      bool
		want       []string // in the hold reason; nil when the drain proceeds
	}{
		{name: "paused, 10 of 12 keep a partner", v1Decoders: 2, pause: true},
		{name: "paused, 9 of 12 keep a partner", v1Decoders: 3, pause: true,
			want: []string{"decoder with 9 pairable", "of 10 (desired 12 minus maxUnavailable 2)"}},
		{name: "growth not paused, 10 of 12 keep a partner", v1Decoders: 2, pause: false,
			want: []string{"decoder with 10 pairable", "of 19 (desired 22 minus maxUnavailable 3)"}},
	}
	for _, tc := range cases {
		allowed, gate, reason := drainGate(t, isvc, v1beta1.EngineComponent, source, fleet(tc.v1Decoders, tc.pause)...)
		if tc.want == nil {
			if !allowed {
				t.Errorf("%s: the drain must proceed: %s", tc.name, reason)
			}
			continue
		}
		if allowed || gate != v1beta1.RolloutHoldGatePairing {
			t.Errorf("%s: drain allowed=%v gate=%q, want a Pairing hold: %s", tc.name, allowed, gate, reason)
			continue
		}
		for _, want := range tc.want {
			if !strings.Contains(reason, want) {
				t.Errorf("%s: the hold reason must name %q: %s", tc.name, want, reason)
			}
		}
	}
}
