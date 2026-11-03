package coordination

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The pictures below are a pairing-protocol change, pair-v1 to pair-v2, on
// 8 prefill rebuilt one at a time (RecreatePod, maxUnavailable 1) beside 4
// decode surging one at a time with none allowed out of pairing
// (SurgeThenDrain, maxSurge 1, maxUnavailable 0), under the chart's
// settings: the ratio rule at a tolerance of 5 and the decoder first. The
// plan is 1:1 2:1 3:2 4:2 5:3 6:3 7:4 8:4, so the decoder's third surge
// waits for the engine's fourth rebuild and its fourth surge for the
// engine's sixth.
//
// A rebuilt Instance is promoted in one status write and its pod counted
// as serving in the next, so between the two the replica reads one serving
// Instance short with no attempt open. The pairing gate then holds every
// start of that side on its own pairable minimum, which nothing but its
// own pod's count releases, and must not report the side as held still
// by the other side's move.

// rebuiltSide is one Instance class of a side mid-roll: promoted and
// serving, promoted with its pod not yet counted as serving, an open
// rebuild with the old pod gone, or still on the old revision.
type rebuiltSide struct {
	promoted  int
	uncounted int
	open      int
	old       int
}

// comingUpIR builds a side's replica from its Instance classes, in the
// order listed, named and revisioned the way the pairing fixtures are.
func comingUpIR(comp v1beta1.ComponentType, strategy v1beta1.UpdateStrategyType, maxUnavailable int, maxSurge *int, side rebuiltSide) *v1beta1.InferenceReplica {
	old, target := "llama-"+string(comp)+"-v1", "llama-"+string(comp)+"-target"
	var rows []v1beta1.OMENativeInstanceStatus
	add := func(n int, row v1beta1.OMENativeInstanceStatus) {
		for i := 0; i < n; i++ {
			row.Index = int32(len(rows))
			rows = append(rows, row)
		}
	}
	add(side.promoted, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: target, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1})
	add(side.uncounted, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: target, PodCount: 1})
	add(side.open, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: old, TargetRevision: target,
		Operation: &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepDrain, TargetRevision: target}})
	add(side.old, v1beta1.OMENativeInstanceStatus{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: old, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1})
	ir := sizedPairingIR("prod", "llama", comp, rows, strategy, len(rows), maxUnavailable)
	if maxSurge != nil {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(*maxSurge))
	}
	ir.Status.CurrentRevision, ir.Status.UpdateRevision = old, target
	ir.Status.Replicas = int32(len(rows))
	ir.Status.ServingReplicas = int32(side.promoted + side.old)
	ir.Status.ReadyReplicas = ir.Status.ServingReplicas
	ir.Status.UpdatedReplicas = int32(side.promoted + side.uncounted)
	return ir
}

// comingUpObjects is the fleet of one picture: both replicas and the two
// cohorts' revisions.
func comingUpObjects(engine, decoder *v1beta1.InferenceReplica) []client.Object {
	return []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "v1", "pair-v1"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "target", "pair-v2"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "v1", "pair-v1"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "target", "pair-v2"),
		engine, decoder,
	}
}

// askComingUp asks the whole gate stack about the next start of component
// from pair-v1 under the chart's settings, and the pairing gate alone for
// its reading of the peer.
func askComingUp(t *testing.T, isvc *v1beta1.InferenceService, objs []client.Object, component v1beta1.ComponentType, strategy workloadtypes.UpdateStrategyType) (allowed bool, gate v1beta1.RolloutHoldGate, reason string, peerHeldStill bool) {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	defaults := keepRatioDecoderFirst(5)
	from := "llama-" + string(component) + "-v1"
	allowed, gate, reason = EvaluateUpdateGate(context.Background(), c, isvc, component, from, forwardTarget(isvc, component), nil, defaults, strategy, 0, 0)
	gateCtx := ResolveGateContextWithDefaults(context.Background(), c, isvc, component, defaults)
	gateCtx.TargetRevision = forwardTarget(isvc, component)
	answer := gateCtx.pairingVerdict(from, strategy, 0, 0)
	if !answer.allowed {
		t.Fatalf("the picture is wrong: the pairing gate must admit the %s start: %s", component, answer.reason)
	}
	return allowed, gate, reason, answer.peerHeldStill
}

// TestEvaluateUpdateGate_DecoderWaitsWhileTheRebuiltEngineIsCountedServing
// holds the decoder's next surge to the engine's previous batch across the
// moments of the 8:4 roll: while the engine's just-promoted Instance is not
// yet counted as serving, while its next rebuild is open, and once the
// batch serves. The decoder's third surge opens only once four engines
// serve pair-v2, and its fourth only once six do.
func TestEvaluateUpdateGate_DecoderWaitsWhileTheRebuiltEngineIsCountedServing(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	isvc := pairingISVC("pair-v2")
	one := 1
	cases := []struct {
		name    string
		engine  rebuiltSide
		decoder rebuiltSide
		allowed bool
		reason  string
	}{
		{
			name:    "the engine's third rebuild is promoted and its pod not yet counted: the decoder's third surge waits for the fourth rebuild",
			engine:  rebuiltSide{promoted: 2, uncounted: 1, old: 5},
			decoder: rebuiltSide{promoted: 2, old: 2},
			reason:  "decoder batch 5 waits for engine batch 4 to serve (3 of 4 serving)",
		},
		{
			name:    "the engine's third rebuild is counted and its fourth is open: the decoder's third surge still waits",
			engine:  rebuiltSide{promoted: 3, open: 1, old: 4},
			decoder: rebuiltSide{promoted: 2, old: 2},
			reason:  "decoder batch 5 waits for engine batch 4 to serve (3 of 4 serving)",
		},
		{
			name:    "four engines serve pair-v2: the decoder's third surge opens",
			engine:  rebuiltSide{promoted: 4, old: 4},
			decoder: rebuiltSide{promoted: 2, old: 2},
			allowed: true,
		},
		{
			name:    "the engine's fifth rebuild is promoted and its pod not yet counted: the decoder's fourth surge waits for the sixth rebuild",
			engine:  rebuiltSide{promoted: 4, uncounted: 1, old: 3},
			decoder: rebuiltSide{promoted: 3, old: 1},
			reason:  "decoder batch 7 waits for engine batch 6 to serve (5 of 6 serving)",
		},
		{
			name:    "six engines serve pair-v2: the decoder's fourth surge opens",
			engine:  rebuiltSide{promoted: 6, old: 2},
			decoder: rebuiltSide{promoted: 3, old: 1},
			allowed: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := comingUpObjects(
				comingUpIR(eng, recreate, 1, nil, tc.engine),
				comingUpIR(dec, surge, 0, &one, tc.decoder))
			allowed, gate, reason, peerHeldStill := askComingUp(t, isvc, objs, dec, workloadtypes.UpdateStrategySurgeThenDrain)
			if peerHeldStill {
				t.Errorf("the engine waits on its own pod's count, not on the decoder's move; the pairing gate must not read it as held still")
			}
			if tc.allowed {
				if !allowed {
					t.Fatalf("the decoder's surge must open: gate=%s reason=%q", gate, reason)
				}
				return
			}
			if allowed {
				t.Fatalf("the decoder's surge opened before the engine's previous batch served")
			}
			if gate != v1beta1.RolloutHoldGateRatio || reason != tc.reason {
				t.Fatalf("hold = %s %q, want Ratio %q", gate, reason, tc.reason)
			}
		})
	}
}

// TestEvaluateUpdateGate_EngineWaitsWhileTheRebuiltDecoderIsCountedServing
// is the mirror: 8 prefill surging one at a time beside 4 decode rebuilt
// one at a time, the decoder first. While the decoder's just-promoted
// Instance is not yet counted as serving, the engine's third surge still
// waits for the decoder's second rebuild to serve first.
func TestEvaluateUpdateGate_EngineWaitsWhileTheRebuiltDecoderIsCountedServing(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	isvc := pairingISVC("pair-v2")
	one := 1
	objs := comingUpObjects(
		comingUpIR(eng, surge, 0, &one, rebuiltSide{promoted: 2, old: 6}),
		comingUpIR(dec, recreate, 1, nil, rebuiltSide{uncounted: 1, old: 3}))
	allowed, gate, reason, peerHeldStill := askComingUp(t, isvc, objs, eng, workloadtypes.UpdateStrategySurgeThenDrain)
	if peerHeldStill {
		t.Errorf("the decoder waits on its own pod's count, not on the engine's move; the pairing gate must not read it as held still")
	}
	if allowed {
		t.Fatalf("the engine's third surge opened before the decoder's second rebuild served")
	}
	want := "engine batch 3 waits for decoder batch 3 to serve first (1 of 2 serving)"
	if gate != v1beta1.RolloutHoldGateRatio || !strings.HasPrefix(reason, want) {
		t.Fatalf("hold = %s %q, want Ratio %q", gate, reason, want)
	}
}
