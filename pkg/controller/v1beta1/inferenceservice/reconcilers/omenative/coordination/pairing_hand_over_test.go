package coordination

import (
	"context"
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

// The pictures below are a pairing-protocol change, pair-v1 to pair-v2, on
// a prefill/decode service in one blueGreen group where the side with fewer
// Instances surges. Its last replacement serves beside its old pod, and the
// drain-time pairing gate keeps that old pod in rotation: it is the last
// partner of the other side's Instances still on pair-v1. The Instance
// stays at step Surge with two serving pods until those Instances have
// moved, which takes the other side's next batch of starts.

// surgeReplacementServingRow is one Instance of a surging member whose replacement
// serves beside its old pod, the attempt still at step Surge: the drain
// gate has not let the old pod leave.
func surgeReplacementServingRow(comp v1beta1.ComponentType, index int32) v1beta1.OMENativeInstanceStatus {
	target := "llama-" + string(comp) + "-target"
	return v1beta1.OMENativeInstanceStatus{
		Index:           index,
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "llama-" + string(comp) + "-v1",
		TargetRevision:  target,
		PodCount:        2,
		ReadyPodCount:   2,
		ServingPodCount: 2,
		Operation:       &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurge, TargetRevision: target},
	}
}

// cohortPods builds the routable serving pods of one side: a pair-v2 pod
// for each promoted Instance, a pair-v1 pod for each old one, and both for
// the Instance handing over (none when handOver is negative).
func cohortPods(comp v1beta1.ComponentType, promoted, old []int, handOver int) []client.Object {
	name := func(index, ordinal int) string {
		return fmt.Sprintf("llama-%s-%d-default-%d", comp, index, ordinal)
	}
	var out []client.Object
	for _, i := range promoted {
		out = append(out, drainPod("llama", comp, name(i, 1), "default", "pair-v2", true))
	}
	for _, i := range old {
		out = append(out, drainPod("llama", comp, name(i, 0), "default", "pair-v1", true))
	}
	if handOver >= 0 {
		out = append(out,
			drainPod("llama", comp, name(handOver, 0), "default", "pair-v1", true),
			drainPod("llama", comp, name(handOver, 1), "default", "pair-v2", true))
	}
	return out
}

// indexes lists the Instances from through to.
func indexes(from, to int) []int {
	var out []int
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// revisionsOf names the revision each Instance runs: n on the target, then
// the rest on pair-v1.
func revisionsOf(promoted, total int) []string {
	out := make([]string, 0, total)
	for i := 0; i < total; i++ {
		if i < promoted {
			out = append(out, "target")
			continue
		}
		out = append(out, "v1")
	}
	return out
}

// askStart asks the whole gate stack about the next start of component on
// the given mechanism, from the pair-v1 revision.
func askStart(t *testing.T, isvc *v1beta1.InferenceService, objs []client.Object, component v1beta1.ComponentType, strategy workloadtypes.UpdateStrategyType) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	return EvaluateUpdateGate(context.Background(), c, isvc, component, "llama-"+string(component)+"-v1", forwardTarget(isvc, component), nil, GroupDefaults{}, strategy, 0, 0)
}

// handOverPicture is one shape of the change: the replicas, the side whose
// next start is asked about and how it moves, the side whose last old pod
// the drain gate keeps with that pod, what the drain gate says while the
// other side's old Instances remain, and the pods before and after they
// have moved.
type handOverPicture struct {
	name     string
	replicas []client.Object
	acting   v1beta1.ComponentType
	strategy workloadtypes.UpdateStrategyType
	kept     v1beta1.ComponentType
	source   string
	floor    string
	before   []client.Object
	after    []client.Object
}

// handOverPictures are the shapes the change takes: 6 prefill beside 2
// decode with both sides surging, its mirror, the prefill drained one at a
// time beside a surging decode, and 12 prefill beside 8 decode at a tenth
// of each count, where the last old prefill is kept for two old decode.
func handOverPictures() []handOverPicture {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge, recreate := v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod
	withHandOver := func(comp v1beta1.ComponentType, ir *v1beta1.InferenceReplica, index int32) *v1beta1.InferenceReplica {
		ir.Status.InstanceStatuses[index] = surgeReplacementServingRow(comp, index)
		return ir
	}
	tenPercent := func(ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromString("10%"))
		return ir
	}
	return []handOverPicture{
		{
			name: "6 prefill beside 2 decode, both surging: the prefill's third batch opens while the last old decode is kept",
			replicas: []client.Object{
				rolledPairingIR(eng, surge, 6, 1, revisionsOf(4, 6)...),
				withHandOver(dec, rolledPairingIR(dec, surge, 2, 1, "target", "v1"), 1),
			},
			acting: eng, strategy: workloadtypes.UpdateStrategySurgeThenDrain,
			kept: dec, source: "llama-decoder-1-default-0", floor: "engine with 4 pairable serving Instance(s), below its floor of 5",
			before: append(cohortPods(eng, indexes(0, 3), indexes(4, 5), -1), cohortPods(dec, []int{0}, nil, 1)...),
			after:  append(cohortPods(eng, indexes(0, 5), nil, -1), cohortPods(dec, []int{0}, nil, 1)...),
		},
		{
			name: "2 prefill beside 6 decode, both surging: the decode's third batch opens while the last old prefill is kept",
			replicas: []client.Object{
				withHandOver(eng, rolledPairingIR(eng, surge, 2, 1, "target", "v1"), 1),
				rolledPairingIR(dec, surge, 6, 1, revisionsOf(4, 6)...),
			},
			acting: dec, strategy: workloadtypes.UpdateStrategySurgeThenDrain,
			kept: eng, source: "llama-engine-1-default-0", floor: "decoder with 4 pairable serving Instance(s), below its floor of 5",
			before: append(cohortPods(dec, indexes(0, 3), indexes(4, 5), -1), cohortPods(eng, []int{0}, nil, 1)...),
			after:  append(cohortPods(dec, indexes(0, 5), nil, -1), cohortPods(eng, []int{0}, nil, 1)...),
		},
		{
			name: "6 prefill drained one at a time beside 2 surging decode: the prefill's third drain opens while the last old decode is kept",
			replicas: []client.Object{
				rolledPairingIR(eng, recreate, 6, 1, revisionsOf(2, 6)...),
				withHandOver(dec, rolledPairingIR(dec, surge, 2, 0, "target", "v1"), 1),
			},
			acting: eng, strategy: workloadtypes.UpdateStrategyRecreatePod,
			kept: dec, source: "llama-decoder-1-default-0", floor: "engine with 2 pairable serving Instance(s), below its floor of 5",
			before: append(cohortPods(eng, indexes(0, 1), indexes(2, 5), -1), cohortPods(dec, []int{0}, nil, 1)...),
			after:  append(cohortPods(eng, indexes(0, 5), nil, -1), cohortPods(dec, []int{0}, nil, 1)...),
		},
		{
			name: "12 prefill beside 8 decode at a tenth: the decode's seventh surge opens while the last old prefill is kept",
			replicas: []client.Object{
				withHandOver(eng, tenPercent(rolledPairingIR(eng, surge, 12, 1, revisionsOf(11, 12)...)), 11),
				tenPercent(rolledPairingIR(dec, surge, 8, 1, revisionsOf(6, 8)...)),
			},
			acting: dec, strategy: workloadtypes.UpdateStrategySurgeThenDrain,
			kept: eng, source: "llama-engine-11-default-0", floor: "decoder with 6 pairable serving Instance(s), below its floor of 7",
			before: append(cohortPods(eng, indexes(0, 10), nil, 11), cohortPods(dec, indexes(0, 5), indexes(6, 7), -1)...),
			after:  append(cohortPods(eng, indexes(0, 10), nil, 11), cohortPods(dec, indexes(0, 7), nil, -1)...),
		},
	}
}

// The batch step does not wait on a pairing peer whose only open attempt
// is a hand-over the pairing gate holds: that peer's old pod leaves only
// once this side's old Instances have moved, so the next batch of starts
// opens, and the drain gate releases the kept pod once they serve pair-v2.
func TestEvaluateUpdateGate_BatchStepWaivesAPeerWhoseHandOverThePairingGateHolds(t *testing.T) {
	for _, pic := range handOverPictures() {
		t.Run(pic.name, func(t *testing.T) {
			isvc := pairingISVC("pair-v2")
			objs := append(threeCohortCRs("pair-v2"), pic.replicas...)

			allowed, gate, reason := askStart(t, isvc, objs, pic.acting, pic.strategy)
			if !allowed {
				t.Fatalf("the %s's next start must open while the %s's last old pod is kept for the %s's old Instances; held under %s: %s", pic.acting, pic.kept, pic.acting, gate, reason)
			}
			if strings.Contains(reason, "batch") {
				t.Fatalf("the batch step must not answer the %s's start: %s", pic.acting, reason)
			}

			held, dgate, dreason := drainGate(t, isvc, pic.kept, []string{pic.source}, append(append([]client.Object{}, objs...), pic.before...)...)
			if held || dgate != v1beta1.RolloutHoldGatePairing || !strings.Contains(dreason, pic.floor) {
				t.Fatalf("the %s's hand-over must wait for the %s's old Instances under the floor (want %q): allowed=%v gate=%q reason=%q", pic.kept, pic.acting, pic.floor, held, dgate, dreason)
			}
			if ok, _, why := drainGate(t, isvc, pic.kept, []string{pic.source}, append(append([]client.Object{}, objs...), pic.after...)...); !ok {
				t.Fatalf("once every %s serves pair-v2 the %s's hand-over must proceed: %s", pic.acting, pic.kept, why)
			}
		})
	}
}

// The waiver is the pairing gate's reading of the peer: the pairing gate
// alone admits the start, and it reports the peer held still.
func TestCheckPairing_ReportsASurgingPeerHeldStillByItsOwnHandOver(t *testing.T) {
	isvc := pairingISVC("pair-v2")
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	decoders := rolledPairingIR(dec, surge, 2, 1, "target", "v1")
	decoders.Status.InstanceStatuses[1] = surgeReplacementServingRow(dec, 1)
	objs := append(threeCohortCRs("pair-v2"), rolledPairingIR(eng, surge, 6, 1, revisionsOf(4, 6)...), decoders)

	gate := pairingGate(t, isvc, eng, objs...)
	v := gate.pairingVerdict("llama-engine-v1", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if !v.allowed {
		t.Fatalf("a surge start takes nothing out of rotation and the pairing gate admits it: %s", v.reason)
	}
	if !v.peerHeldStill {
		t.Fatalf("the decoder's only open attempt is a hand-over this gate holds for the old prefill; it must read as held still")
	}
}

// A surging peer with an open attempt the pairing gate does not hold is
// moving on its own, and the batch step waits for its batch as before: a
// hand-over whose old pod is not the last partner of anything, a
// replacement that does not serve yet, and the same picture outside a
// protocol change.
func TestEvaluateUpdateGate_BatchStepWaitsOnAPeerWhoseHandOverIsNotHeld(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain

	t.Run("the kept old pod is not the last partner: the hand-over proceeds and the wait stands", func(t *testing.T) {
		isvc := pairingISVC("pair-v2")
		decoders := rolledPairingIR(dec, surge, 2, 1, "v1", "v1")
		decoders.Status.InstanceStatuses[0] = surgeReplacementServingRow(dec, 0)
		objs := append(threeCohortCRs("pair-v2"), rolledPairingIR(eng, surge, 6, 1, revisionsOf(2, 6)...), decoders)
		allowed, gate, reason := askStart(t, isvc, objs, eng, workloadtypes.UpdateStrategySurgeThenDrain)
		if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving)" {
			t.Fatalf("the prefill's second batch waits for the decode's first hand-over = (%v, %q, %q)", allowed, gate, reason)
		}
		pods := append(cohortPods(eng, indexes(0, 1), indexes(2, 5), -1), cohortPods(dec, nil, []int{1}, 0)...)
		if ok, _, why := drainGate(t, isvc, dec, []string{"llama-decoder-0-default-0"}, append(append([]client.Object{}, objs...), pods...)...); !ok {
			t.Fatalf("with another old decode serving, the first decode's old pod is not the last partner and leaves: %s", why)
		}
	})

	t.Run("the replacement does not serve yet: the wait stands", func(t *testing.T) {
		isvc := pairingISVC("pair-v2")
		decoders := rolledPairingIR(dec, surge, 2, 1, "target", "v1")
		row := surgeReplacementServingRow(dec, 1)
		row.ReadyPodCount, row.ServingPodCount = 1, 1
		decoders.Status.InstanceStatuses[1] = row
		objs := append(threeCohortCRs("pair-v2"), rolledPairingIR(eng, surge, 6, 1, revisionsOf(4, 6)...), decoders)
		allowed, gate, reason := askStart(t, isvc, objs, eng, workloadtypes.UpdateStrategySurgeThenDrain)
		if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != "engine batch 3 waits for decoder batch 2 to serve (1 of 2 serving)" {
			t.Fatalf("the prefill's third batch waits for the decode's replacement to serve = (%v, %q, %q)", allowed, gate, reason)
		}
	})

	t.Run("no protocol change: the wait stands", func(t *testing.T) {
		isvc := pairingISVC("pair-v2")
		var crs []client.Object
		for _, comp := range pairingComponents {
			for _, hash := range []string{"v1", "v2", "target"} {
				crs = append(crs, pairingCR("prod", "llama", comp, hash, "pair-v2"))
			}
		}
		decoders := rolledPairingIR(dec, surge, 2, 1, "target", "v1")
		decoders.Status.InstanceStatuses[1] = surgeReplacementServingRow(dec, 1)
		objs := append(crs, rolledPairingIR(eng, surge, 6, 1, revisionsOf(4, 6)...), decoders)
		allowed, gate, reason := askStart(t, isvc, objs, eng, workloadtypes.UpdateStrategySurgeThenDrain)
		if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != "engine batch 3 waits for decoder batch 2 to serve (1 of 2 serving)" {
			t.Fatalf("with every revision on one protocol the hand-over is not held and the wait stands = (%v, %q, %q)", allowed, gate, reason)
		}
	})
}
