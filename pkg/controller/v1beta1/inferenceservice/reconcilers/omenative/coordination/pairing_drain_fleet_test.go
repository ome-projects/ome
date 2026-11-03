package coordination

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// fleetPods builds n routable pods of one Component and cohort, one per
// Instance index starting at firstIdx, on surge ordinal `ordinal`.
func fleetPods(comp v1beta1.ComponentType, firstIdx, n int, ordinal int, proto string, ready bool) []client.Object {
	out := make([]client.Object, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("llama-%s-%d-default-%d", comp, firstIdx+i, ordinal)
		out = append(out, drainPod("llama", comp, name, "default", proto, ready))
	}
	return out
}

// replicaWith is a Component's InferenceReplica declaring an update
// strategy ("" leaves the lifecycle block unset, the SurgeThenDrain default).
func replicaWith(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, strategy v1beta1.UpdateStrategyType) *v1beta1.InferenceReplica {
	ir := pairingIR(isvc.Namespace, isvc.Name, comp, nil)
	if strategy != "" {
		ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: strategy}}
	}
	return ir
}

// surgingPair is both pairing replicas on SurgeThenDrain.
func surgingPair(isvc *v1beta1.InferenceService) []client.Object {
	return []client.Object{
		replicaWith(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain),
		replicaWith(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategySurgeThenDrain),
	}
}

func requireHeld(t *testing.T, allowed bool, gate v1beta1.RolloutHoldGate, reason, why string) {
	t.Helper()
	if allowed {
		t.Fatalf("%s: drain allowed (gate=%q reason=%q)", why, gate, reason)
	}
	if gate != v1beta1.RolloutHoldGatePairing {
		t.Errorf("gate = %q, want Pairing", gate)
	}
	if !strings.Contains(reason, "cannot pair") {
		t.Errorf("reason must say the replacement cannot pair yet: %s", reason)
	}
}

// 6 prefill + 2 decode, protocol v1 -> v2, rolled 3P+1D at a time. The new
// decoder is Ready while all three new engines still load: it can pair with
// no serving engine, so retiring a v1 decoder would trade serving capacity
// for none. The drain is held until a v2 engine serves.
func TestEvaluatePairingDrain_6P2D_DecoderReadyFirst_HoldsTheOldDecoder(t *testing.T) {
	isvc := pairingISVC("v2")
	objs := surgingPair(isvc)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", false)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)

	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-0-default-0"}, objs...)
	requireHeld(t, allowed, gate, reason, "old decoder with no v2 engine serving")
}

// The mirror: three new engines Ready first, the new decoder still loading.
func TestEvaluatePairingDrain_6P2D_EnginesReadyFirst_HoldsTheOldEngines(t *testing.T) {
	isvc := pairingISVC("v2")
	objs := surgingPair(isvc)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", false)...)

	allowed, gate, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{"llama-engine-0-default-0"}, objs...)
	requireHeld(t, allowed, gate, reason, "old engine with no v2 decoder serving")
}

// A larger decode side: two old decoders already replaced and a third
// replacement Ready, still with no v2 engine serving. The third old decoder
// stays too; the floor does not thin out to one old pair.
func TestEvaluatePairingDrain_6P4D_HoldsEveryOldDecoderUntilAnEngineServes(t *testing.T) {
	isvc := pairingISVC("v2")
	objs := surgingPair(isvc)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", false)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 2, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 2, 1, 1, "v2", true)...)

	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-2-default-0"}, objs...)
	requireHeld(t, allowed, gate, reason, "third old decoder with no v2 engine serving")
}

// Once one v2 engine serves, the v2 decoders are real capacity and the old
// decoder's drain proceeds; the old engines' drains proceed as well.
func TestEvaluatePairingDrain_6P2D_FirstPeerServing_ReleasesTheDrains(t *testing.T) {
	isvc := pairingISVC("v2")
	objs := surgingPair(isvc)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 1, 2, 1, "v2", false)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)

	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-0-default-0"}, objs...); !allowed {
		t.Fatalf("with a v2 engine serving the v2 decoder pairs; the drain must proceed: %s", reason)
	}
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{"llama-engine-0-default-0"}, objs...); !allowed {
		t.Fatalf("with a v2 decoder serving the v2 engine pairs; the drain must proceed: %s", reason)
	}
}

// A source that cannot pair either is not protected: a v2 decoder replaced
// by another v2 decoder while no v2 engine serves trades nothing away, and
// the old pair stays intact behind it.
func TestEvaluatePairingDrain_UnpairableSourceIsNotHeld(t *testing.T) {
	isvc := pairingISVC("v2")
	objs := surgingPair(isvc)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 2, 1, 0, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 2, 1, 1, "v2", true)...)

	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-2-default-0"}, objs...); !allowed {
		t.Fatalf("a v2 source has no peer to pair with; its drain must proceed: %s", reason)
	}
}

// Engines without a protocol label carry the empty protocol and pair with
// anything, so a v2 decoder replacement is capacity from the moment it
// serves and the old decoder's drain proceeds.
func TestEvaluatePairingDrain_EmptyProtocolPeerPairsWithTheReplacement(t *testing.T) {
	isvc := pairingISVC("v2")
	objs := surgingPair(isvc)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 2, 0, "", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)

	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-0-default-0"}, objs...); !allowed {
		t.Fatalf("an unlabelled engine pairs with the v2 decoder; the drain must proceed: %s", reason)
	}
}

// attemptRow is an Instance row carrying an Update attempt toward the named
// revision at the given step, in the given phase; a settled row (no step)
// carries no attempt and runs runningRev.
func attemptRow(index int, phase v1beta1.OMENativeInstancePhase, runningRev, step, targetRev string) v1beta1.OMENativeInstanceStatus {
	row := v1beta1.OMENativeInstanceStatus{Index: int32(index), Phase: phase, RunningRevision: runningRev, ServingPodCount: 1}
	if step != "" {
		row.ServingPodCount = 0
		row.TargetRevision = targetRev
		row.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: targetRev}
	}
	return row
}

// withRows gives a replica the Instance rows the drain gate reads attempts
// from.
func withRows(ir *v1beta1.InferenceReplica, rows ...v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	ir.Status.InstanceStatuses = rows
	return ir
}

// Four surging engines, none allowed out of pairing, and four decoders
// recreated one at a time, the decoder's pods slow to start. The first
// engine's v2 replacement is Ready while no decoder serves v2: the
// replacement has no partner and the old engine pod partners three v1
// decoders, so the old pod stays whatever the decoder side is doing: its
// first step in flight with the new pod not created or still starting, that
// step ended by the escalation or failed, no step opened yet, and under an
// engine budget of one. The decoder's steps need nothing from this drain (the start-time
// gate admits them beside the kept pod), and the first decoder pod that
// serves v2 releases it.
func TestEvaluatePairingDrain_SurgingSideWaitsForADrainFirstPeerPodOfTheNewCohort(t *testing.T) {
	isvc := pairingISVC("v2")
	const v1Rev, v2Rev = "llama-decoder-v1", "llama-decoder-v2"
	settled := []v1beta1.OMENativeInstanceStatus{
		attemptRow(1, v1beta1.OMENativeInstanceReady, v1Rev, "", ""),
		attemptRow(2, v1beta1.OMENativeInstanceReady, v1Rev, "", ""),
		attemptRow(3, v1beta1.OMENativeInstanceReady, v1Rev, "", ""),
	}
	fleet := func(engineBudget int, first v1beta1.OMENativeInstanceStatus, firstPod []client.Object) []client.Object {
		objs := []client.Object{
			sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, 4, engineBudget),
			withRows(sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 4, 1), append([]v1beta1.OMENativeInstanceStatus{first}, settled...)...),
			pairingCR("prod", "llama", v1beta1.DecoderComponent, "v1", "v1"),
			pairingCR("prod", "llama", v1beta1.DecoderComponent, "v2", "v2"),
		}
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 4, 0, "v1", true)...)
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 1, "v2", true)...)
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 1, 3, 0, "v1", true)...)
		return append(objs, firstPod...)
	}
	source := []string{"llama-engine-0-default-0"}
	recreating := attemptRow(0, v1beta1.OMENativeInstanceUpdating, v1Rev, workloadtypes.UpdateStepDrain, v2Rev)
	newPod := func(ready bool) []client.Object { return fleetPods(v1beta1.DecoderComponent, 0, 1, 0, "v2", ready) }
	parked := recreating
	parked.Phase = v1beta1.OMENativeInstanceFailed
	parked.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepParked, TargetRevision: v2Rev}
	cleared := attemptRow(0, v1beta1.OMENativeInstanceFailed, v1Rev, "", "")
	cleared.ServingPodCount = 0
	expired := recreating
	expired.Phase = v1beta1.OMENativeInstanceFailed
	idle := attemptRow(0, v1beta1.OMENativeInstanceReady, v1Rev, "", "")

	for _, tc := range []struct {
		name         string
		engineBudget int
		first        v1beta1.OMENativeInstanceStatus
		firstPod     []client.Object
	}{
		{name: "decoder step in flight with its new pod not created yet", first: recreating},
		{name: "decoder step in flight with its new pod still starting", first: recreating, firstPod: newPod(false)},
		{name: "decoder step ended by the escalation at step Parked", first: parked, firstPod: newPod(false)},
		{name: "decoder step failed with its attempt cleared", first: cleared},
		{name: "decoder step failed by its deadline", first: expired, firstPod: newPod(false)},
		{name: "no decoder step opened yet", first: idle, firstPod: fleetPods(v1beta1.DecoderComponent, 0, 1, 0, "v1", true)},
		{name: "one engine allowed out of pairing", engineBudget: 1, first: recreating},
	} {
		allowed, gate, reason := drainGate(t, isvc, v1beta1.EngineComponent, source, fleet(tc.engineBudget, tc.first, tc.firstPod)...)
		requireHeld(t, allowed, gate, reason, tc.name)
	}
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, source, fleet(0, recreating, newPod(true))...); !allowed {
		t.Fatalf("once the decoder's new pod serves v2 the replacement pairs and the drain proceeds: %s", reason)
	}
}

// Two engines relabelled in place and two surging decoders, none allowed out
// of pairing. The first decoder's v2 replacement is Ready while no engine
// serves v2: the old decoder pod partners both v1 engines, so it stays
// whether the first engine's relabel is in flight, has not started, or
// moves toward another cohort, and leaves once an engine serves v2.
func TestEvaluatePairingDrain_SurgingSideWaitsForAnInPlacePeerPodOfTheNewCohort(t *testing.T) {
	isvc := pairingISVC("v2")
	const v1Rev, v2Rev = "llama-engine-v1", "llama-engine-v2"
	fleet := func(first v1beta1.OMENativeInstanceStatus, enginePods ...client.Object) []client.Object {
		objs := []client.Object{
			withRows(sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategyInPlaceIfPossible, 2, 1),
				first, attemptRow(1, v1beta1.OMENativeInstanceReady, v1Rev, "", "")),
			sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
			pairingCR("prod", "llama", v1beta1.EngineComponent, "v1", "v1"),
			pairingCR("prod", "llama", v1beta1.EngineComponent, "v2", "v2"),
		}
		objs = append(objs, enginePods...)
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
		return append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)
	}
	source := []string{"llama-decoder-0-default-0"}
	bothOnV1 := fleetPods(v1beta1.EngineComponent, 0, 2, 0, "v1", true)

	relabelling := attemptRow(0, v1beta1.OMENativeInstanceUpdating, v1Rev, workloadtypes.UpdateStepInPlace, v2Rev)
	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, source, fleet(relabelling, bothOnV1...)...)
	requireHeld(t, allowed, gate, reason, "engine relabel in flight with no v2 engine serving yet")

	idle := attemptRow(0, v1beta1.OMENativeInstanceReady, v1Rev, "", "")
	allowed, gate, reason = drainGate(t, isvc, v1beta1.DecoderComponent, source, fleet(idle, bothOnV1...)...)
	requireHeld(t, allowed, gate, reason, "no engine relabel opened yet")

	elsewhere := attemptRow(0, v1beta1.OMENativeInstanceUpdating, v1Rev, workloadtypes.UpdateStepInPlace, v1Rev)
	allowed, gate, reason = drainGate(t, isvc, v1beta1.DecoderComponent, source, fleet(elsewhere, bothOnV1...)...)
	requireHeld(t, allowed, gate, reason, "engine relabel toward another cohort")

	relabelled := attemptRow(0, v1beta1.OMENativeInstanceReady, v2Rev, "", "")
	oneOnV2 := append(fleetPods(v1beta1.EngineComponent, 0, 1, 0, "v2", true), fleetPods(v1beta1.EngineComponent, 1, 1, 0, "v1", true)...)
	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, source, fleet(relabelled, oneOnV2...)...); !allowed {
		t.Fatalf("once an engine serves v2 the replacement pairs and the drain proceeds: %s", reason)
	}
}

// The revision the leaving pods' Instance moves to cannot be read: the gate
// fails closed with a retryable Pairing hold rather than guess which cohort
// the old pods are traded for.
func TestEvaluatePairingDrain_FailsClosedWhenThePinnedRevisionCannotBeRead(t *testing.T) {
	isvc := pairingISVC("v2")
	objs := []client.Object{
		withRows(sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
			attemptRow(0, v1beta1.OMENativeInstanceUpdating, "llama-engine-v1", workloadtypes.UpdateStepSurge, "llama-engine-v2"),
			attemptRow(1, v1beta1.OMENativeInstanceReady, "llama-engine-v1", "", "")),
		sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 2, 1),
	}
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	c := fakeclient.New(t, fakeclient.WithObjects(objs...),
		fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, errors.New("apiserver unavailable")).OnKind(&appsv1.ControllerRevision{}))
	allowed, gate, reason := EvaluatePairingDrain(context.Background(), c, isvc, v1beta1.EngineComponent, forwardTarget(isvc, v1beta1.EngineComponent), GroupDefaults{}, []string{"llama-engine-0-default-0"})
	if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "failing closed") {
		t.Fatalf("an unreadable revision must fail closed with a retryable Pairing hold: allowed=%v gate=%q reason=%s", allowed, gate, reason)
	}
}

// A peer with no lifecycle block updates by the SurgeThenDrain default and is
// waited on; a peer whose replica does not exist is read from its pods alone
// and waited on all the same.
func TestEvaluatePairingDrain_PeerStrategyDefaultsAndUnknownReplica(t *testing.T) {
	isvc := pairingISVC("v2")
	pods := []client.Object{}
	pods = append(pods, fleetPods(v1beta1.EngineComponent, 0, 2, 0, "v1", true)...)
	pods = append(pods, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	pods = append(pods, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)

	withDefault := append([]client.Object{replicaWith(isvc, v1beta1.EngineComponent, "")}, pods...)
	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-0-default-0"}, withDefault...)
	requireHeld(t, allowed, gate, reason, "engine replica with the default strategy")

	allowed, gate, reason = drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-0-default-0"}, pods...)
	requireHeld(t, allowed, gate, reason, "no engine replica to read")
}

func TestEvaluatePairingDrain_FailsClosedWhenThePeerReplicaCannotBeRead(t *testing.T) {
	isvc := pairingISVC("v2")
	var objs []client.Object
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)
	c := fakeclient.New(t, fakeclient.WithObjects(objs...),
		fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, errors.New("apiserver unavailable")).OnKind(&v1beta1.InferenceReplica{}))
	allowed, gate, reason := EvaluatePairingDrain(context.Background(), c, isvc, v1beta1.DecoderComponent, forwardTarget(isvc, v1beta1.DecoderComponent), GroupDefaults{}, []string{"llama-decoder-0-default-0"})
	if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "failing closed") {
		t.Fatalf("an unreadable peer replica must fail closed with a retryable Pairing hold: allowed=%v gate=%q reason=%s", allowed, gate, reason)
	}
}

// sizedReplica is a Component's InferenceReplica with its desired count and
// update strategy, and the maxUnavailable budget its rollingUpdate declares:
// the numbers the pairable floor is read from.
func sizedReplica(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, strategy v1beta1.UpdateStrategyType, replicas, maxUnavailable int) *v1beta1.InferenceReplica {
	ir := replicaWith(isvc, comp, strategy)
	ir.Spec.Replicas = ptr.To(int32(replicas))
	ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{MaxUnavailable: ptr.To(intstr.FromInt(maxUnavailable))}
	return ir
}

func requireFloorHeld(t *testing.T, allowed bool, gate v1beta1.RolloutHoldGate, reason, why string) {
	t.Helper()
	if allowed {
		t.Fatalf("%s: drain allowed (gate=%q reason=%q)", why, gate, reason)
	}
	if gate != v1beta1.RolloutHoldGatePairing {
		t.Errorf("gate = %q, want Pairing", gate)
	}
	if !strings.Contains(reason, "below its floor") {
		t.Errorf("reason must name the floor the drain would break: %s", reason)
	}
}

// Six engines and two decoders, both surging, none allowed out of pairing.
// The first three engines and the first decoder are already on v2; the
// other three engines still run v1 with no replacement serving yet, and the
// second decoder's v2 replacement is Ready. Retiring that decoder's v1 pod
// would leave the three v1 engines without a partner: 3 of 6 pairable, under
// a floor of 6. The drain waits until each of those engines has a v2
// replacement in rotation; a budget of one lets one of them wait.
func TestEvaluatePairingDrain_6P2D_LastOldDecoderWaitsUntilEveryOldEngineHasAPartner(t *testing.T) {
	isvc := pairingISVC("v2")
	fleet := func(engineBudget int) []client.Object {
		objs := []client.Object{
			sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, 6, engineBudget),
			sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
		}
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", true)...)
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 3, 3, 0, "v1", true)...)
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 1, 1, 0, "v1", true)...)
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 1, 1, 1, "v2", true)...)
		return objs
	}
	source := []string{"llama-decoder-1-default-0"}

	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, source, fleet(0)...)
	requireFloorHeld(t, allowed, gate, reason, "last v1 decoder while three engines still speak only v1")
	if !strings.Contains(reason, "engine") || !strings.Contains(reason, "3 pairable") {
		t.Errorf("the hold must say which side falls short and by how much: %s", reason)
	}

	// Every v1 engine has a v2 replacement in rotation: no engine loses its
	// partner and the decoder's drain proceeds.
	withReplacements := append(fleet(0), fleetPods(v1beta1.EngineComponent, 3, 3, 1, "v2", true)...)
	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, source, withReplacements...); !allowed {
		t.Fatalf("with a v2 replacement serving on every v1 engine the drain must proceed: %s", reason)
	}
	// Replacements that are not Ready are not partners yet.
	pending := append(fleet(0), fleetPods(v1beta1.EngineComponent, 3, 3, 1, "v2", false)...)
	allowed, gate, reason = drainGate(t, isvc, v1beta1.DecoderComponent, source, pending...)
	requireFloorHeld(t, allowed, gate, reason, "replacements still starting")

	// With one engine allowed out of pairing, two replacements out of three
	// are enough; none is not.
	twoOfThree := append(fleet(1), fleetPods(v1beta1.EngineComponent, 3, 2, 1, "v2", true)...)
	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, source, twoOfThree...); !allowed {
		t.Fatalf("a budget of one lets one engine wait for its partner: %s", reason)
	}
	allowed, gate, reason = drainGate(t, isvc, v1beta1.DecoderComponent, source, fleet(1)...)
	requireFloorHeld(t, allowed, gate, reason, "three engines short under a budget of one")

	// The old engines' own drains never fall under the floor: each keeps
	// its Instance paired through the replacement, and both decoders still
	// have a partner.
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{"llama-engine-3-default-0"}, withReplacements...); !allowed {
		t.Fatalf("an old engine whose replacement pairs may leave: %s", reason)
	}
}

// Two surging engines, none allowed out of pairing, and two decoders
// recreated one at a time. The first engine's v2 replacement is Ready while
// the first decoder's v2 pod is still starting: no decoder serves v2 and
// the old engine pod partners the second decoder, so the drain waits for a
// decoder pod of v2, under an engine budget of one as well; once the
// recreated decoder serves, it goes.
func TestEvaluatePairingDrain_SurgingEngineWaitsForTheDecoderAlreadyRecreating(t *testing.T) {
	isvc := pairingISVC("v2")
	fleet := func(engineBudget int, decoderReady bool) []client.Object {
		objs := []client.Object{
			sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, 2, engineBudget),
			withRows(sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 2, 1),
				attemptRow(0, v1beta1.OMENativeInstanceUpdating, "llama-decoder-v1", workloadtypes.UpdateStepDrain, "llama-decoder-v2"),
				attemptRow(1, v1beta1.OMENativeInstanceReady, "llama-decoder-v1", "", "")),
			pairingCR("prod", "llama", v1beta1.DecoderComponent, "v2", "v2"),
		}
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 2, 0, "v1", true)...)
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 1, "v2", true)...)
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 0, "v2", decoderReady)...)
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 1, 1, 0, "v1", true)...)
		return objs
	}
	source := []string{"llama-engine-0-default-0"}

	allowed, gate, reason := drainGate(t, isvc, v1beta1.EngineComponent, source, fleet(0, false)...)
	requireHeld(t, allowed, gate, reason, "engine replacement with no v2 decoder serving yet")
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, source, fleet(0, true)...); !allowed {
		t.Fatalf("once the recreated decoder serves v2 the engine's replacement pairs and the drain proceeds: %s", reason)
	}
	// The rule reads partners, not the floor: an engine budget of one
	// keeps the old pod as well.
	allowed, gate, reason = drainGate(t, isvc, v1beta1.EngineComponent, source, fleet(1, false)...)
	requireHeld(t, allowed, gate, reason, "engine replacement with no v2 decoder serving yet, one engine allowed out of pairing")

	// A single drain-first decoder: while its only pod still serves and its
	// step is in flight the engine waits, like above; once that pod is down
	// no pair is left to protect and the engine's drain proceeds, the
	// window that decoder's recreate costs without pairing.
	single := func(decoderServing bool) []client.Object {
		objs := []client.Object{
			sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
			withRows(sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 1, 1),
				attemptRow(0, v1beta1.OMENativeInstanceUpdating, "llama-decoder-v1", workloadtypes.UpdateStepDrain, "llama-decoder-v2")),
			pairingCR("prod", "llama", v1beta1.DecoderComponent, "v2", "v2"),
		}
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 2, 0, "v1", true)...)
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 1, "v2", true)...)
		return append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 0, "v1", decoderServing)...)
	}
	allowed, gate, reason = drainGate(t, isvc, v1beta1.EngineComponent, source, single(true)...)
	requireHeld(t, allowed, gate, reason, "lone decoder still serving with its step in flight")
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, source, single(false)...); !allowed {
		t.Fatalf("with the lone decoder's pod down there is no pair to protect; the drain must proceed: %s", reason)
	}
}

// The transition reversed: the protocol went v1 to v2 and back to v1 while
// three of six engines and one of two decoders had moved to v2. The roll
// target is the v1 revision again, so the decoder on v2 surges a v1
// replacement. Retiring its v2 pod would leave the three v2 engines with no
// partner, 3 of 6 under a floor of 6, so the drain is held on the pairing
// gate until those engines have v1 replacements in rotation.
func TestEvaluatePairingDrain_ThenBack_HoldsTheOldProtocolUntilTheOtherSideHasMovedBack(t *testing.T) {
	isvc := pairingISVC("v1")
	stable := pairingCR(isvc.Namespace, isvc.Name, v1beta1.DecoderComponent, "stable", "v1")
	objs := []client.Object{
		sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, 6, 0),
		sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
	}
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 3, 3, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 1, 1, 0, "v1", true)...)
	source := []string{"llama-decoder-0-default-1"}

	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	allowed, gate, reason := EvaluatePairingDrain(context.Background(), c, isvc, v1beta1.DecoderComponent, stable, GroupDefaults{}, source)
	requireFloorHeld(t, allowed, gate, reason, "the last v2 decoder while three engines still speak v2")
	if !strings.Contains(reason, `transition to "v1"`) {
		t.Errorf("the hold must name the way back as the transition: %s", reason)
	}

	back := append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 0, "v1", true)...)
	c = fakeclient.New(t, fakeclient.WithObjects(back...))
	if allowed, _, reason := EvaluatePairingDrain(context.Background(), c, isvc, v1beta1.DecoderComponent, stable, GroupDefaults{}, source); !allowed {
		t.Fatalf("with a v1 replacement serving on every v2 engine the way back proceeds: %s", reason)
	}
}
