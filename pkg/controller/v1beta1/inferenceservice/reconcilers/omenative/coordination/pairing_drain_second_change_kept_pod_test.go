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

// A pairing protocol changed again while the first change is half done,
// on two engines that surge one at a time with one allowed out of rotation
// and two decoders rebuilt one at a time whose pods start late.
//
// After the first change, v1 to v2, the first engine's v2 replacement
// serves while the first decoder's v2 pod is still starting. The engine's
// old v1 pod stays: the replacement has no partner and the old pod has
// one, and the engine's own budget of one has no say in that. The
// protocol then moves to v3. The v2 replacement pairs with nothing and
// the v1 pods keep the only pair, so the surge may remove it and start
// again toward v3, while the old pod stays either way.
//
// secondChangeKeptPodFleet is that picture: the engine replica with its
// first Instance mid-surge toward v2 and its second settled on v1, the
// decoder replica with its first Instance rebuilding toward v2 and its
// second settled on v1, the revisions of the three protocols, and the
// pods: engine 0 serving v1 and v2, engine 1 serving v1, decoder 0 with a
// v2 pod not Ready yet, decoder 1 serving v1.
func secondChangeKeptPodFleet(isvc *v1beta1.InferenceService) []client.Object {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	engines := sizedReplica(isvc, eng, v1beta1.UpdateStrategySurgeThenDrain, 2, 1)
	engines.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(1))
	objs := []client.Object{
		withRows(engines,
			surgingRow(isvc.Name, eng, 0, "v1", "v2", workloadtypes.UpdateStepSurge, 2),
			settledRow(isvc.Name, eng, 1, "v1")),
		withRows(sizedReplica(isvc, dec, v1beta1.UpdateStrategyRecreatePod, 2, 1),
			attemptRow(0, v1beta1.OMENativeInstanceUpdating, "llama-decoder-v1", workloadtypes.UpdateStepDrain, "llama-decoder-v2"),
			settledRow(isvc.Name, dec, 1, "v1")),
	}
	for _, comp := range pairingComponents {
		for _, proto := range []string{"v1", "v2", "v3"} {
			objs = append(objs, pairingCR(isvc.Namespace, isvc.Name, comp, proto, proto))
		}
	}
	objs = append(objs, fleetPods(eng, 0, 1, 0, "v1", true)...)
	objs = append(objs, fleetPods(eng, 0, 1, 1, "v2", true)...)
	objs = append(objs, fleetPods(eng, 1, 1, 0, "v1", true)...)
	objs = append(objs, fleetPods(dec, 0, 1, 0, "v2", false)...)
	return append(objs, fleetPods(dec, 1, 1, 0, "v1", true)...)
}

// The first change: the old engine pod stays while its v2 replacement has
// no decoder to pair with, under an engine budget that lets one engine out
// of rotation. The second change asks the other question of the same
// picture: the v2 replacement itself leaving, which the surge does before
// it starts again toward v3. It pairs with nothing and the v1 pods keep
// the only pair, so it may leave; the old pod still may not.
func TestEvaluatePairingDrain_SecondChange_AReplacementWithNoPartnerLeavesAndTheOldPodStays(t *testing.T) {
	eng := v1beta1.EngineComponent
	oldPod, replacement := []string{"llama-engine-0-default-0"}, []string{"llama-engine-0-default-1"}

	firstChange := pairingISVC("v2")
	allowed, gate, reason := drainGate(t, firstChange, eng, oldPod, secondChangeKeptPodFleet(firstChange)...)
	requireHeld(t, allowed, gate, reason, "first change: no decoder serves v2, so the old engine pod stays whatever the engine's budget allows")
	if !strings.Contains(reason, `no serving decoder runs cohort "v2" yet`) {
		t.Errorf("the hold must say the replacement waits for a decoder of the new protocol: %s", reason)
	}

	secondChange := pairingISVC("v3")
	if allowed, _, reason := drainGate(t, secondChange, eng, replacement, secondChangeKeptPodFleet(secondChange)...); !allowed {
		t.Fatalf("second change: the v2 replacement pairs with nothing and the v1 pods keep the only pair, so the surge may remove it: %s", reason)
	}
	allowed, gate, reason = drainGate(t, secondChange, eng, oldPod, secondChangeKeptPodFleet(secondChange)...)
	requireHeld(t, allowed, gate, reason, "second change: the old engine pod stays while its replacement on v2 has no partner")
	if !strings.Contains(reason, `"v2"`) {
		t.Errorf("the hold must name the replacement's own cohort, v2, as the one no decoder serves: %s", reason)
	}
}

// After the second change the first engine surges again: its v3
// replacement serves beside the kept v1 pod while the first decoder's v3
// pod is still starting. The old pod stays until that decoder serves v3,
// and the second decoder's rebuild, which would take the v1 pods' last
// partner out, waits for the same moment: the start-time gate admits it as
// the last hop onto the serving v3 engine, and the group's unavailability
// budget of one holds it behind the first decoder's rebuild. Once a
// decoder serves v3 the old pod leaves, the second decoder's rebuild
// opens, and the second engine's old pod leaves the moment its own v3
// replacement serves.
func TestEvaluatePairingDrain_SecondChange_TheOldEnginePodStaysUntilADecoderServesTheFinalProtocol(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	isvc := pairingISVC("v3")
	revisions := func() []client.Object {
		var out []client.Object
		for _, comp := range pairingComponents {
			for _, proto := range []string{"v1", "v3"} {
				out = append(out, pairingCR(isvc.Namespace, isvc.Name, comp, proto, proto))
			}
		}
		return out
	}
	engines := func(rows ...v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
		ir := sizedReplica(isvc, eng, v1beta1.UpdateStrategySurgeThenDrain, 2, 1)
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(1))
		return withRows(ir, rows...)
	}
	// decoders carries the counters the group budget reads: every row
	// with a serving pod counts as serving.
	decoders := func(rows ...v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
		ir := withRows(sizedReplica(isvc, dec, v1beta1.UpdateStrategyRecreatePod, 2, 1), rows...)
		ir.Status.Replicas = 2
		for _, row := range rows {
			if row.ServingPodCount > 0 {
				ir.Status.ServingReplicas++
				ir.Status.ReadyReplicas++
			}
		}
		return ir
	}
	decoderRebuild := func(ready bool) []client.Object {
		return fleetPods(dec, 0, 1, 0, "v3", ready)
	}
	firstEngineSurging := []client.Object{
		engines(surgingRow(isvc.Name, eng, 0, "v1", "v3", workloadtypes.UpdateStepSurge, 2), settledRow(isvc.Name, eng, 1, "v1")),
	}
	firstEngineSurging = append(firstEngineSurging, revisions()...)
	firstEngineSurging = append(firstEngineSurging, fleetPods(eng, 0, 1, 0, "v1", true)...)
	firstEngineSurging = append(firstEngineSurging, fleetPods(eng, 0, 1, 1, "v3", true)...)
	firstEngineSurging = append(firstEngineSurging, fleetPods(eng, 1, 1, 0, "v1", true)...)
	firstEngineSurging = append(firstEngineSurging, fleetPods(dec, 1, 1, 0, "v1", true)...)
	oldPod := []string{"llama-engine-0-default-0"}

	// The first decoder's v3 pod is still starting.
	decoderStarting := append(append([]client.Object{}, firstEngineSurging...),
		decoders(attemptRow(0, v1beta1.OMENativeInstanceUpdating, "llama-decoder-v1", workloadtypes.UpdateStepDrain, "llama-decoder-v3"), settledRow(isvc.Name, dec, 1, "v1")))
	decoderStarting = append(decoderStarting, decoderRebuild(false)...)
	allowed, gate, reason := drainGate(t, isvc, eng, oldPod, decoderStarting...)
	requireHeld(t, allowed, gate, reason, "no decoder serves v3 yet, so the old engine pod stays")
	if !strings.Contains(reason, `no serving decoder runs cohort "v3" yet`) {
		t.Errorf("the hold must say the replacement waits for a decoder of the final protocol: %s", reason)
	}
	if allowed, gate, reason := secondDecoderRebuild(t, isvc, decoderStarting...); allowed {
		t.Fatalf("the second decoder's rebuild would take the v1 engine pods' last partner out while no decoder serves v3; it must wait: allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}

	// The first decoder serves v3 and is promoted.
	decoderServes := append(append([]client.Object{}, firstEngineSurging...),
		decoders(settledRow(isvc.Name, dec, 0, "v3"), settledRow(isvc.Name, dec, 1, "v1")))
	decoderServes = append(decoderServes, decoderRebuild(true)...)
	if allowed, _, reason := drainGate(t, isvc, eng, oldPod, decoderServes...); !allowed {
		t.Fatalf("once a decoder serves v3 the replacement pairs and the old engine pod may leave: %s", reason)
	}
	if allowed, gate, reason := secondDecoderRebuild(t, isvc, decoderServes...); !allowed {
		t.Fatalf("once a decoder serves v3 the second decoder's rebuild opens: held under %s: %s", gate, reason)
	}

	// The first engine is promoted on v3; the second engine's v3 replacement
	// serves beside its v1 pod, which pairs with the second decoder still
	// on v1. That decoder is not the last partner of anything the gate
	// protects, so the drain proceeds at once.
	secondEngineSurging := []client.Object{
		engines(settledRow(isvc.Name, eng, 0, "v3"), surgingRow(isvc.Name, eng, 1, "v1", "v3", workloadtypes.UpdateStepSurge, 2)),
		decoders(settledRow(isvc.Name, dec, 0, "v3"), settledRow(isvc.Name, dec, 1, "v1")),
	}
	secondEngineSurging = append(secondEngineSurging, revisions()...)
	secondEngineSurging = append(secondEngineSurging, fleetPods(eng, 0, 1, 1, "v3", true)...)
	secondEngineSurging = append(secondEngineSurging, fleetPods(eng, 1, 1, 0, "v1", true)...)
	secondEngineSurging = append(secondEngineSurging, fleetPods(eng, 1, 1, 1, "v3", true)...)
	secondEngineSurging = append(secondEngineSurging, decoderRebuild(true)...)
	secondEngineSurging = append(secondEngineSurging, fleetPods(dec, 1, 1, 0, "v1", true)...)
	if allowed, _, reason := drainGate(t, isvc, eng, []string{"llama-engine-1-default-0"}, secondEngineSurging...); !allowed {
		t.Fatalf("the second engine's replacement pairs with the v3 decoder, so its old pod leaves at once: %s", reason)
	}
}

// secondDecoderRebuild asks the whole gate stack, under the operator's
// defaults, about the second decoder's rebuild from the v1 revision.
func secondDecoderRebuild(t *testing.T, isvc *v1beta1.InferenceService, objs ...client.Object) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	return EvaluateUpdateGate(context.Background(), c, isvc, v1beta1.DecoderComponent, "llama-decoder-v1", forwardTarget(isvc, v1beta1.DecoderComponent), nil, GroupDefaults{}, workloadtypes.UpdateStrategyRecreatePod, 0, 0)
}
