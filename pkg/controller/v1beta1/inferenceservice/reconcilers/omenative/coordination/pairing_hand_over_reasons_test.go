package coordination

import (
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The drain-time pairing gate keeps a surging Instance's old pod in
// rotation for one of three reasons: its replacement cannot pair because
// no pod of the other side serves the new cohort yet while the old pod
// does, the drain would push the other side below its pairable minimum, or
// the pod is the last keeping any pair alive. Whatever the reason, only
// the other side's own moves release it, so the start-time gate must read
// a peer whose only open attempts are such kept replacements as unable to move on its own; a
// batch step that waits on an Instance the pairing gate is itself keeping
// waits for ever. Three pictures of the first reason (the other side
// surging, rebuilding its Instances, and down to the one partner the old
// pod is) and one of the pairable minimum: the drain gate's hold is confirmed first,
// then the pairing verdict's reading of the peer from the other side's
// start. TestCheckPairing_ReadsAPeerHeldStillWhoseOnlyStartItHolds is the
// same reading for a peer that has not started.
func TestCheckPairing_ReadsAPeerHeldStillWhateverKeepsItsHandOver(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge, recreate := v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod
	withHandOver := func(comp v1beta1.ComponentType, ir *v1beta1.InferenceReplica, index int32) *v1beta1.InferenceReplica {
		ir.Status.InstanceStatuses[index] = surgeReplacementServingRow(comp, index)
		return ir
	}
	// withOpenDrain takes one Instance of a drain-first replica out of
	// rotation with its rebuild toward the target under way.
	withOpenDrain := func(comp v1beta1.ComponentType, ir *v1beta1.InferenceReplica, index int32) *v1beta1.InferenceReplica {
		target := "llama-" + string(comp) + "-target"
		ir.Status.InstanceStatuses[index] = v1beta1.OMENativeInstanceStatus{
			Index: index, Phase: v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "llama-" + string(comp) + "-v1", TargetRevision: target,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepDrain, TargetRevision: target},
		}
		ir.Status.ServingReplicas--
		ir.Status.ReadyReplicas--
		return ir
	}
	cases := []struct {
		name      string
		replicas  []client.Object
		pods      []client.Object
		acting    v1beta1.ComponentType
		mechanism workloadtypes.UpdateStrategyType
		kept      v1beta1.ComponentType
		source    string
		reason    string
	}{
		{
			name: "the replacement cannot pair yet and the other side surges: 12 prefill beside 8 decode, the first new decode serves before any new prefill",
			replicas: []client.Object{
				rolledPairingIR(eng, surge, 12, 1, revisionsOf(0, 12)...),
				withHandOver(dec, rolledPairingIR(dec, surge, 8, 1, revisionsOf(0, 8)...), 0),
			},
			pods:   append(cohortPods(eng, nil, indexes(0, 11), -1), cohortPods(dec, nil, indexes(1, 7), 0)...),
			acting: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, kept: dec, source: "llama-decoder-0-default-0",
			reason: `the replacement decoder cannot pair because no serving engine runs cohort "pair-v2" yet`,
		},
		{
			name: "the other side's pairable minimum: 6 prefill beside 2 decode, the last old decode is the last partner of the old prefill",
			replicas: []client.Object{
				rolledPairingIR(eng, surge, 6, 1, revisionsOf(4, 6)...),
				withHandOver(dec, rolledPairingIR(dec, surge, 2, 1, "target", "v1"), 1),
			},
			pods:   append(cohortPods(eng, indexes(0, 3), indexes(4, 5), -1), cohortPods(dec, []int{0}, nil, 1)...),
			acting: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, kept: dec, source: "llama-decoder-1-default-0",
			reason: "would leave the engine with 4 pairable serving Instance(s), below its floor of 5",
		},
		{
			name: "the replacement cannot pair yet and the other side rebuilds its Instances: 12 prefill rebuilt two at a time beside 8 decode with none allowed out of pairing, the first new decode serves while the first prefill rebuilds",
			replicas: []client.Object{
				withOpenDrain(eng, rolledPairingIR(eng, recreate, 12, 2, revisionsOf(0, 12)...), 0),
				withHandOver(dec, rolledPairingIR(dec, surge, 8, 0, revisionsOf(0, 8)...), 0),
			},
			pods:   append(cohortPods(eng, nil, indexes(1, 11), -1), cohortPods(dec, nil, indexes(1, 7), 0)...),
			acting: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, kept: dec, source: "llama-decoder-0-default-0",
			reason: `the replacement decoder cannot pair because no serving engine runs cohort "pair-v2" yet`,
		},
		{
			name: "the old pod is the only partner: 2 prefill rebuilt one at a time beside 1 surging decode, whose old pod is the only partner the prefill have",
			replicas: []client.Object{
				rolledPairingIR(eng, recreate, 2, 1, "v1", "v1"),
				withHandOver(dec, rolledPairingIR(dec, surge, 1, 1, "v1"), 0),
			},
			pods:   append(cohortPods(eng, nil, indexes(0, 1), -1), cohortPods(dec, nil, nil, 0)...),
			acting: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, kept: dec, source: "llama-decoder-0-default-0",
			reason: `the replacement decoder cannot pair because no serving engine runs cohort "pair-v2" yet`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := pairingISVC("pair-v2")
			objs := append(threeCohortCRs("pair-v2"), tc.replicas...)
			allowed, gate, reason := drainGate(t, isvc, tc.kept, []string{tc.source}, append(append([]client.Object{}, objs...), tc.pods...)...)
			if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, tc.reason) {
				t.Fatalf("the picture is wrong: the drain gate must keep the %s's old pod (want %q): allowed=%v gate=%q reason=%q", tc.kept, tc.reason, allowed, gate, reason)
			}
			v := pairingGate(t, isvc, tc.acting, objs...).pairingVerdict("llama-"+string(tc.acting)+"-v1", tc.mechanism, 0, 0)
			if !v.allowed {
				t.Fatalf("the picture is wrong: the pairing gate must admit the %s's start: %s", tc.acting, v.reason)
			}
			if !v.peerHeldStill {
				t.Fatalf("the %s's only open attempt is a replacement the drain gate keeps (%s); the pairing gate must read it as unable to move on its own, or the batch step waits on an Instance the pairing gate is itself keeping", tc.kept, tc.reason)
			}
		})
	}
}

// A surging side of one Instance has no replacement to keep yet: its only
// start is held by the start-time gate, because the picture after its
// replacement (its one Instance on the new cohort) leaves no pair until the
// other side serves the new cohort. That side moves only once the other
// side has, like a drain-first side whose every start is held, so the
// pairing gate reads it as unable to move on its own and the batch step does not wait
// on its batch. With 2 prefill beside 1 decode, both surging, the decode's
// start is held and the prefill's start reads the decode as unable to move on its own.
func TestCheckPairing_ReadsAPeerHeldStillWhoseOnlyStartItHolds(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	isvc := pairingISVC("pair-v2")
	objs := append(threeCohortCRs("pair-v2"),
		rolledPairingIR(eng, surge, 2, 1, "v1", "v1"),
		rolledPairingIR(dec, surge, 1, 1, "v1"))

	held := pairingGate(t, isvc, dec, objs...).pairingVerdict("llama-decoder-v1", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if held.allowed || !strings.Contains(held.reason, "would leave no pairable serving engine+decoder pair") {
		t.Fatalf("the picture is wrong: the decode's only start must be held by the one-pair rule: allowed=%v reason=%q", held.allowed, held.reason)
	}
	v := pairingGate(t, isvc, eng, objs...).pairingVerdict("llama-engine-v1", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if !v.allowed {
		t.Fatalf("the picture is wrong: the pairing gate must admit the prefill's start: %s", v.reason)
	}
	if !v.peerHeldStill {
		t.Fatalf("the decode has no attempt open and its only start is held until a prefill serves pair-v2; the pairing gate must read it as unable to move on its own, or the batch step waits on a batch the pairing gate itself keeps from opening")
	}
}

// gangRows are the rows of a gang member, one per revision hash, each
// serving all its pods.
func gangRows(comp v1beta1.ComponentType, pods int32, hashes ...string) []v1beta1.OMENativeInstanceStatus {
	rows := servingInstances("llama", comp, hashes...)
	for i := range rows {
		rows[i].PodCount, rows[i].ReadyPodCount, rows[i].ServingPodCount = pods, pods, pods
	}
	return rows
}

// gangIR is rolledPairingIR for a gang member: a leader and workers per
// Instance.
func gangIR(comp v1beta1.ComponentType, strategy v1beta1.UpdateStrategyType, pods int32, replicas, maxUnavailable int, hashes ...string) *v1beta1.InferenceReplica {
	ir := sizedPairingIR("prod", "llama", comp, gangRows(comp, pods, hashes...), strategy, replicas, maxUnavailable)
	ir.Spec.Runners = []v1beta1.Runner{{Name: workloadtypes.RunnerLeader, Size: 1}, {Name: workloadtypes.RunnerWorker, Size: pods - 1}}
	ir.Status.UpdateRevision = "llama-" + string(comp) + "-target"
	ir.Status.Replicas = int32(replicas)
	ir.Status.ServingReplicas = int32(len(hashes))
	ir.Status.ReadyReplicas = int32(len(hashes))
	return ir
}

// withGangHandOver makes one Instance of a gang member a surge whose
// replacement gang serves in its slot while the source gang still serves:
// the source row keeps its attempt at step Surge pointing at the slot, and
// the slot's row is the replacement at step GangSurgeTarget.
func withGangHandOver(ir *v1beta1.InferenceReplica, comp v1beta1.ComponentType, index, slot, pods int32) *v1beta1.InferenceReplica {
	target := "llama-" + string(comp) + "-target"
	src := &ir.Status.InstanceStatuses[index]
	src.Phase, src.TargetRevision = v1beta1.OMENativeInstanceUpdating, target
	src.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurge, TargetRevision: target, SurgeIndex: &slot}
	ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
		Index: slot, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: target, PodCount: pods, ReadyPodCount: pods, ServingPodCount: pods,
		Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepGangSurgeTarget, TargetRevision: target},
	})
	ir.Status.ServingReplicas++
	ir.Status.ReadyReplicas++
	return ir
}

// gangPodSet is the pods of one gang Instance as the drain gate lists them:
// a routable leader and workers that never carry traffic.
func gangPodSet(comp v1beta1.ComponentType, index int, proto string, ready bool, pods int32) []client.Object {
	out := []client.Object{drainPod("llama", comp, fmt.Sprintf("llama-%s-%d-%s-0", comp, index, workloadtypes.RunnerLeader), workloadtypes.RunnerLeader, proto, ready)}
	for k := int32(0); k < pods-1; k++ {
		out = append(out, drainPod("llama", comp, fmt.Sprintf("llama-%s-%d-%s-%d", comp, index, workloadtypes.RunnerWorker, k), workloadtypes.RunnerWorker, proto, ready))
	}
	return out
}

// gangPodNames are the pods of one gang Instance, the ones a kept replacement
// takes out of rotation.
func gangPodNames(comp v1beta1.ComponentType, index int, pods int32) []string {
	names := []string{fmt.Sprintf("llama-%s-%d-%s-0", comp, index, workloadtypes.RunnerLeader)}
	for k := int32(0); k < pods-1; k++ {
		names = append(names, fmt.Sprintf("llama-%s-%d-%s-%d", comp, index, workloadtypes.RunnerWorker, k))
	}
	return names
}

// A gang (an Instance of a leader and workers) surges through a slot of
// its own: the source row keeps its attempt at step Surge, pointing at the
// slot, while the replacement gang serves there beside the old one, and
// the drain gate is asked with every pod of the source gang. The gate
// keeps the old gang for the same three reasons it keeps a single old pod,
// and the pairing verdict must read a peer whose only open attempt is such
// a kept gang replacement as unable to move on its own, the slot's row included, whatever
// the reason. One picture per reason reached with gangs (the first reason
// twice, with the other side surging and with its old gang the only
// partner), and one with single-pod prefill beside decode gangs.
func TestCheckPairing_ReadsAGangPeerHeldStillWhateverKeepsItsHandOver(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge, recreate := v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod
	cases := []struct {
		name      string
		replicas  []client.Object
		pods      []client.Object
		acting    v1beta1.ComponentType
		mechanism workloadtypes.UpdateStrategyType
		kept      v1beta1.ComponentType
		sources   []string
		reason    string
	}{
		{
			name: "the replacement cannot pair yet and the other side surges: 12 prefill beside 8 decode, gangs of 2, the first new decode gang serves before any new prefill",
			replicas: []client.Object{
				gangIR(eng, surge, 2, 12, 1, revisionsOf(0, 12)...),
				withGangHandOver(gangIR(dec, surge, 2, 8, 1, revisionsOf(0, 8)...), dec, 0, 8, 2),
			},
			pods: func() []client.Object {
				var out []client.Object
				for i := 0; i < 12; i++ {
					out = append(out, gangPodSet(eng, i, "pair-v1", true, 2)...)
				}
				for i := 0; i < 8; i++ {
					out = append(out, gangPodSet(dec, i, "pair-v1", true, 2)...)
				}
				return append(out, gangPodSet(dec, 8, "pair-v2", true, 2)...)
			}(),
			acting: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, kept: dec, sources: gangPodNames(dec, 0, 2),
			reason: `the replacement decoder cannot pair because no serving engine runs cohort "pair-v2" yet`,
		},
		{
			name: "the other side's pairable minimum: 6 prefill beside 2 decode, gangs of 4, the last old decode gang is the last partner of the old prefill",
			replicas: []client.Object{
				gangIR(eng, surge, 4, 6, 1, revisionsOf(4, 6)...),
				withGangHandOver(gangIR(dec, surge, 4, 2, 1, "target", "v1"), dec, 1, 2, 4),
			},
			pods: func() []client.Object {
				var out []client.Object
				for i := 0; i < 4; i++ {
					out = append(out, gangPodSet(eng, i, "pair-v2", true, 4)...)
				}
				for i := 4; i < 6; i++ {
					out = append(out, gangPodSet(eng, i, "pair-v1", true, 4)...)
				}
				out = append(out, gangPodSet(dec, 0, "pair-v2", true, 4)...)
				out = append(out, gangPodSet(dec, 1, "pair-v1", true, 4)...)
				return append(out, gangPodSet(dec, 2, "pair-v2", true, 4)...)
			}(),
			acting: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, kept: dec, sources: gangPodNames(dec, 1, 4),
			reason: "would leave the engine with 4 pairable serving Instance(s), below its floor of 5",
		},
		{
			name: "the old gang is the only partner: 2 prefill gangs rebuilt one at a time beside 1 surging decode gang, whose old pods are the only partners the prefill have",
			replicas: []client.Object{
				gangIR(eng, recreate, 2, 2, 1, "v1", "v1"),
				withGangHandOver(gangIR(dec, surge, 2, 1, 1, "v1"), dec, 0, 1, 2),
			},
			pods: func() []client.Object {
				var out []client.Object
				for i := 0; i < 2; i++ {
					out = append(out, gangPodSet(eng, i, "pair-v1", true, 2)...)
				}
				out = append(out, gangPodSet(dec, 0, "pair-v1", true, 2)...)
				return append(out, gangPodSet(dec, 1, "pair-v2", true, 2)...)
			}(),
			acting: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, kept: dec, sources: gangPodNames(dec, 0, 2),
			reason: `the replacement decoder cannot pair because no serving engine runs cohort "pair-v2" yet`,
		},
		{
			name: "single-pod prefill beside decode gangs of 4: 12 prefill beside 8 decode, the first new decode gang serves before any new prefill",
			replicas: []client.Object{
				rolledPairingIR(eng, surge, 12, 1, revisionsOf(0, 12)...),
				withGangHandOver(gangIR(dec, surge, 4, 8, 1, revisionsOf(0, 8)...), dec, 0, 8, 4),
			},
			pods: func() []client.Object {
				out := cohortPods(eng, nil, indexes(0, 11), -1)
				for i := 0; i < 8; i++ {
					out = append(out, gangPodSet(dec, i, "pair-v1", true, 4)...)
				}
				return append(out, gangPodSet(dec, 8, "pair-v2", true, 4)...)
			}(),
			acting: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, kept: dec, sources: gangPodNames(dec, 0, 4),
			reason: `the replacement decoder cannot pair because no serving engine runs cohort "pair-v2" yet`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := pairingISVC("pair-v2")
			objs := append(threeCohortCRs("pair-v2"), tc.replicas...)
			allowed, gate, reason := drainGate(t, isvc, tc.kept, tc.sources, append(append([]client.Object{}, objs...), tc.pods...)...)
			if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, tc.reason) {
				t.Fatalf("the picture is wrong: the drain gate must keep the %s's old gang (want %q): allowed=%v gate=%q reason=%q", tc.kept, tc.reason, allowed, gate, reason)
			}
			v := pairingGate(t, isvc, tc.acting, objs...).pairingVerdict("llama-"+string(tc.acting)+"-v1", tc.mechanism, 0, 0)
			if !v.allowed {
				t.Fatalf("the picture is wrong: the pairing gate must admit the %s's start: %s", tc.acting, v.reason)
			}
			if !v.peerHeldStill {
				t.Fatalf("the %s's only open attempt is a gang replacement the drain gate keeps (%s); the pairing gate must read it as unable to move on its own, or the batch step waits on an Instance the pairing gate is itself keeping", tc.kept, tc.reason)
			}
		})
	}
}
