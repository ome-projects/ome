package coordination

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The services below pair prefill with decode in one blueGreen group. Their
// pairing protocol goes from pair-v1 to pair-v2 and changes again while the
// first change is half done.

// setBackStart asks the whole gate stack, after the set-back to pair-v1,
// about the next start of a side whose row runs the pair-v2 revision.
func setBackStart(t *testing.T, isvc *v1beta1.InferenceService, defaults GroupDefaults, component v1beta1.ComponentType, strategy workloadtypes.UpdateStrategyType, objs ...client.Object) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	return EvaluateUpdateGate(context.Background(), c, isvc, component, "llama-"+string(component)+"-v2", forwardTarget(isvc, component), nil, defaults, strategy, 0, 0)
}

// One pod patched in place beside one pod that surges, no side named first.
// After the set-back the old pod of the surge may leave, so the patch must
// not count it as a partner. The patch waits until the surging side is back.
func TestPairingGates_ProtocolSetBackOnOneByOne_ThePatchDoesNotCountAnOldPodThatLeaves(t *testing.T) {
	t.Run("prefill patched in place beside a decoder that surges", func(t *testing.T) {
		setBackWalk(t, v1beta1.EngineComponent, v1beta1.DecoderComponent)
	})
	t.Run("decoder patched in place beside a prefill that surges", func(t *testing.T) {
		setBackWalk(t, v1beta1.DecoderComponent, v1beta1.EngineComponent)
	})
}

// setBackWalk asks both gates at each step of the set-back, with the side
// that is patched in place and the side that surges as given.
func setBackWalk(t *testing.T, patched, surging v1beta1.ComponentType) {
	t.Helper()
	setBack := pairingISVC("pair-v1")
	noSideFirst := GroupDefaults{}
	surgingSideFirst := GroupDefaults{RatioTolerancePercent: ptr.To(int32(5)), PrefillDecode: PrefillDecodeRules{KeepRatio: true, FirstSide: surging}}
	patch, surgeStart := workloadtypes.UpdateStrategyInPlaceIfPossible, workloadtypes.UpdateStrategySurgeThenDrain
	firstPod, secondPod := "llama-"+string(surging)+"-0-default-0", "llama-"+string(surging)+"-0-default-1"
	onPairV1 := drainPod("llama", surging, firstPod, "default", "pair-v1", true)
	onPairV2 := drainPod("llama", surging, secondPod, "default", "pair-v2", true)
	// picture is the patched side serving pair-v2 beside the surging side:
	// one Instance, maxSurge 1, none allowed out of pairing, its row as
	// given, and its serving pods.
	picture := func(row v1beta1.OMENativeInstanceStatus, pods ...client.Object) []client.Object {
		ir := rolledPairingIR(surging, v1beta1.UpdateStrategySurgeThenDrain, 1, 0, "v1")
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(1))
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{row}
		objs := append(threeCohortCRs("pair-v1"),
			rolledPairingIR(patched, v1beta1.UpdateStrategyInPlaceIfPossible, 1, 1, "v2"), ir,
			drainPod("llama", patched, "llama-"+string(patched)+"-0-default-0", "default", "pair-v2", true))
		return append(objs, pods...)
	}
	requirePatchWaits := func(objs []client.Object, why string) {
		t.Helper()
		allowed, gate, reason := setBackStart(t, setBack, noSideFirst, patched, patch, objs...)
		if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "would leave no pairable serving engine+decoder pair") || !strings.Contains(reason, "the "+string(surging)+" surges") {
			t.Errorf("%s: the patch must wait under the Pairing gate for the %s's surge: allowed=%v gate=%q reason=%q", why, surging, allowed, gate, reason)
		}
	}

	// The set-back lands: the replacement stays, the old pod may leave, and
	// the patch waits.
	tradingOldPod := surgingRow("llama", surging, 0, "v1", "v2", workloadtypes.UpdateStepSurge, 2)
	objs := picture(tradingOldPod, onPairV1, onPairV2)
	if allowed, gate, reason := drainGate(t, setBack, surging, []string{secondPod}, objs...); allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "would leave no pairable serving engine+decoder pair") {
		t.Errorf("the pair-v2 replacement is the only partner of the %s, so the surge may not remove it: allowed=%v gate=%q reason=%q", patched, allowed, gate, reason)
	}
	if allowed, _, reason := drainGate(t, setBack, surging, []string{firstPod}, objs...); !allowed {
		t.Errorf("the replacement pairs with the %s on pair-v2, so the old pair-v1 pod may leave: %s", patched, reason)
	}
	requirePatchWaits(objs, "set-back, the old pod free to leave")
	if allowed, gate, reason := setBackStart(t, setBack, surgingSideFirst, patched, patch, objs...); allowed || gate != v1beta1.RolloutHoldGateRatio {
		t.Errorf("with the %s named first the patch waits behind that side's batch under the Ratio gate: allowed=%v gate=%q reason=%q", surging, allowed, gate, reason)
	}

	// The old pod's gate has closed. The row is at the drain step and its
	// serving count has not followed yet.
	oldPodLeaving := surgingRow("llama", surging, 0, "v1", "v2", workloadtypes.UpdateStepSurgeDrain, 2)
	requirePatchWaits(picture(oldPodLeaving, onPairV2), "set-back, the old pod out of rotation and still counted on its row")

	// The surging side rests on pair-v2 beside the patched side: its surge
	// back to pair-v1 is admitted and the patch waits for it.
	objs = picture(settledRow("llama", surging, 0, "v2"), onPairV2)
	if allowed, gate, reason := setBackStart(t, setBack, noSideFirst, surging, surgeStart, objs...); !allowed {
		t.Errorf("the %s's surge back keeps its pair-v2 pod serving until its replacement pairs, so it may open; held under %s: %s", surging, gate, reason)
	}
	requirePatchWaits(objs, "both sides at rest on pair-v2")

	// The pair-v1 replacement serves beside the pair-v2 pod. The drain gate
	// keeps that pod until the patched side serves pair-v1, so it still
	// counts and the patch may run.
	surgedBack := surgingRow("llama", surging, 0, "v2", "target", workloadtypes.UpdateStepSurge, 2)
	objs = picture(surgedBack, onPairV1, onPairV2)
	allowed, gate, reason := drainGate(t, setBack, surging, []string{secondPod}, objs...)
	requireHeld(t, allowed, gate, reason, "no "+string(patched)+" serves pair-v1 yet, so the pair-v2 pod stays")
	if allowed, gate, reason := setBackStart(t, setBack, noSideFirst, patched, patch, objs...); !allowed {
		t.Errorf("a %s serves pair-v1 beside the pod the drain gate keeps, so the patch may run; held under %s: %s", surging, gate, reason)
	}
}

// servingRowNotYetCounted is the status row of a single-pod Instance one
// status write after its pod came back into rotation: it names the revision
// the pod runs and counts no serving pod yet.
func servingRowNotYetCounted(component v1beta1.ComponentType, index int, hash string) v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{
		Index:           int32(index),
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: "llama-" + string(component) + "-" + hash,
		PodCount:        1,
	}
}

// One prefill pod patched in place beside one decode pod that surges. The
// set-back lands while the prefill pod serves pair-v2 and its status row
// does not count it yet. The patch back must wait for the decoder all the same.
func TestPairingGates_ProtocolSetBackOnOneByOne_ThePatchWaitsBeforeItsRowCountsItsPod(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	setBack := pairingISVC("pair-v1")
	oldPod, replacement := "llama-decoder-0-default-0", "llama-decoder-0-default-1"
	onPairV1 := drainPod("llama", dec, oldPod, "default", "pair-v1", true)
	onPairV2 := drainPod("llama", dec, replacement, "default", "pair-v2", true)
	picture := func(row v1beta1.OMENativeInstanceStatus, pods ...client.Object) []client.Object {
		prefill := rolledPairingIR(eng, v1beta1.UpdateStrategyInPlaceIfPossible, 1, 1, "v2")
		prefill.Status.InstanceStatuses[0] = servingRowNotYetCounted(eng, 0, "v2")
		prefill.Status.ServingReplicas, prefill.Status.ReadyReplicas = 0, 0
		decoder := rolledPairingIR(dec, v1beta1.UpdateStrategySurgeThenDrain, 1, 0, "v1")
		decoder.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(1))
		decoder.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{row}
		objs := append(threeCohortCRs("pair-v1"), prefill, decoder,
			drainPod("llama", eng, "llama-engine-0-default-0", "default", "pair-v2", true))
		return append(objs, pods...)
	}
	bothServe := picture(surgingRow("llama", dec, 0, "v1", "v2", workloadtypes.UpdateStepSurge, 2), onPairV1, onPairV2)
	if allowed, _, reason := drainGate(t, setBack, dec, []string{oldPod}, bothServe...); !allowed {
		t.Fatalf("the replacement pairs with the prefill pod on pair-v2, so the old pair-v1 decode pod may leave: %s", reason)
	}
	for _, tc := range []struct {
		name string
		objs []client.Object
	}{
		{"both decode pods serve", bothServe},
		{"the old decode pod is out of rotation", picture(surgingRow("llama", dec, 0, "v1", "v2", workloadtypes.UpdateStepSurgeDrain, 1), onPairV2)},
	} {
		// The pairing gate is asked alone: the budget would refuse this start
		// too, because the row that counts no pod spends it.
		allowed, reason := pairingGate(t, setBack, eng, tc.objs...).CheckPairing("llama-engine-v2", workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 0)
		if allowed || !strings.Contains(reason, "the decoder surges") {
			t.Errorf("%s: the prefill pod is the only partner of the pair-v2 decode pod, so its patch waits for the decoder's surge back: allowed=%v reason=%q", tc.name, allowed, reason)
		}
	}

	// A pod list that fails holds the start: the gate cannot tell whether a
	// pair serves.
	gate := ResolveGateContextWithDefaults(context.Background(), podListsFail{fakeclient.New(t, fakeclient.WithObjects(bothServe...))}, setBack, eng, GroupDefaults{})
	gate.TargetRevision = forwardTarget(setBack, eng)
	if allowed, reason := gate.CheckPairing("llama-engine-v2", workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 0); allowed || !strings.Contains(reason, "cannot list engine pods, failing closed") {
		t.Errorf("a pod list that fails must hold the start: allowed=%v reason=%q", allowed, reason)
	}
}

// podListsFail is a reader whose pod lists fail.
type podListsFail struct{ client.Reader }

func (r podListsFail) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return errors.New("pods are not readable")
	}
	return r.Reader.List(ctx, list, opts...)
}

// Six prefill pods patched in place, one allowed without a partner, beside
// two decode pods that surge. One prefill pod is back from its patch and its
// status row does not count it yet. Both gates must count that pod.
func TestPairingGates_ProtocolSetBack_BothGatesCountAPodItsRowDoesNotCountYet(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	setBack := pairingISVC("pair-v1")
	prefill := rolledPairingIR(eng, v1beta1.UpdateStrategyInPlaceIfPossible, 6, 1, "target", "v2", "v1", "v1", "v1", "v1")
	prefill.Status.InstanceStatuses[0] = servingRowNotYetCounted(eng, 0, "target")
	prefill.Status.ServingReplicas, prefill.Status.ReadyReplicas = 5, 5
	decoder := rolledPairingIR(dec, v1beta1.UpdateStrategySurgeThenDrain, 2, 0, "v1", "v1")
	decoder.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(1))
	decoder.Status.InstanceStatuses[0] = surgingRow("llama", dec, 0, "v1", "v2", workloadtypes.UpdateStepSurge, 2)
	replacement := "llama-decoder-0-default-1"
	objs := append(threeCohortCRs("pair-v1"), prefill, decoder,
		drainPod("llama", eng, "llama-engine-0-default-0", "default", "pair-v1", true),
		drainPod("llama", eng, "llama-engine-1-default-0", "default", "pair-v2", true),
		drainPod("llama", dec, "llama-decoder-0-default-0", "default", "pair-v1", true),
		drainPod("llama", dec, replacement, "default", "pair-v2", true),
		drainPod("llama", dec, "llama-decoder-1-default-0", "default", "pair-v1", true))
	for i := 2; i < 6; i++ {
		objs = append(objs, drainPod("llama", eng, fmt.Sprintf("llama-engine-%d-default-0", i), "default", "pair-v1", true))
	}

	if allowed, _, reason := drainGate(t, setBack, dec, []string{replacement}, objs...); !allowed {
		t.Fatalf("five prefill pods serve pair-v1 and five is the minimum, so the surge may remove its pair-v2 replacement: %s", reason)
	}
	if allowed, gate, reason := setBackStart(t, setBack, GroupDefaults{}, eng, workloadtypes.UpdateStrategyInPlaceIfPossible, objs...); !allowed {
		t.Fatalf("the pair-v2 prefill pod loses its only partner as that replacement leaves, so its patch costs no pair and may run; held under %s: %s", gate, reason)
	}
}

// Two prefill pods patched in place, one allowed without a partner, beside
// one decoder that surges. The surge may remove its replacement, so its old
// pair-v1 pod stays, still counts, and the patch back runs at once.
func TestPairingGates_ProtocolSetBack_ASurgeThatRemovesItsReplacementKeepsItsOldPodInTheCount(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	setBack := pairingISVC("pair-v1")
	oldPod, replacement := "llama-decoder-0-default-0", "llama-decoder-0-default-1"
	decoder := rolledPairingIR(dec, v1beta1.UpdateStrategySurgeThenDrain, 1, 0, "v1")
	decoder.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(1))
	decoder.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		surgingRow("llama", dec, 0, "v1", "v2", workloadtypes.UpdateStepSurge, 2),
	}
	objs := append(threeCohortCRs("pair-v1"),
		rolledPairingIR(eng, v1beta1.UpdateStrategyInPlaceIfPossible, 2, 1, "v2", "v1"),
		decoder,
		drainPod("llama", eng, "llama-engine-0-default-0", "default", "pair-v2", true),
		drainPod("llama", eng, "llama-engine-1-default-0", "default", "pair-v1", true),
		drainPod("llama", dec, oldPod, "default", "pair-v1", true),
		drainPod("llama", dec, replacement, "default", "pair-v2", true))

	if allowed, _, reason := drainGate(t, setBack, dec, []string{replacement}, objs...); !allowed {
		t.Fatalf("the prefill may run one pod short of a partner and the pair-v1 pods keep a pair, so the surge may remove its replacement: %s", reason)
	}
	if allowed, gate, reason := setBackStart(t, setBack, GroupDefaults{}, eng, workloadtypes.UpdateStrategyInPlaceIfPossible, objs...); !allowed {
		t.Fatalf("the old pair-v1 decode pod stays and pairs with the returning prefill, so the patch may run; held under %s: %s", gate, reason)
	}
}

// Two pods that surge beside two that are rebuilt, and the protocol changes
// twice. The roll drops the first replacement, so the rebuild of the last
// pair-v1 pod must not count it. The rebuild of the pair-v2 pod may open.
func TestPairingGates_ProtocolChangedTwice_ARebuildDoesNotCountAReplacementTheRollLeavesBehind(t *testing.T) {
	t.Run("prefill surges beside a decoder that is rebuilt", func(t *testing.T) {
		changedTwiceWalk(t, v1beta1.EngineComponent, v1beta1.DecoderComponent)
	})
	t.Run("decoder surges beside a prefill that is rebuilt", func(t *testing.T) {
		changedTwiceWalk(t, v1beta1.DecoderComponent, v1beta1.EngineComponent)
	})
}

// changedTwiceWalk asks both gates on the state the second change lands on,
// with the side that surges and the side that is rebuilt as given.
func changedTwiceWalk(t *testing.T, surging, rebuilt v1beta1.ComponentType) {
	t.Helper()
	changedTwice := pairingISVC("pair-v3")
	oldPod, replacement := "llama-"+string(surging)+"-0-default-0", "llama-"+string(surging)+"-0-default-1"
	surge := rolledPairingIR(surging, v1beta1.UpdateStrategySurgeThenDrain, 2, 1, "v1", "v1")
	surge.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(1))
	surge.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		surgingRow("llama", surging, 0, "v1", "v2", workloadtypes.UpdateStepSurge, 2),
		settledRow("llama", surging, 1, "v1"),
	}
	objs := append(threeCohortCRs("pair-v3"),
		surge,
		rolledPairingIR(rebuilt, v1beta1.UpdateStrategyRecreatePod, 2, 1, "v2", "v1"),
		drainPod("llama", surging, oldPod, "default", "pair-v1", true),
		drainPod("llama", surging, replacement, "default", "pair-v2", true),
		drainPod("llama", surging, "llama-"+string(surging)+"-1-default-0", "default", "pair-v1", true),
		drainPod("llama", rebuilt, "llama-"+string(rebuilt)+"-0-default-0", "default", "pair-v2", true),
		drainPod("llama", rebuilt, "llama-"+string(rebuilt)+"-1-default-0", "default", "pair-v1", true))

	if allowed, _, reason := drainGate(t, changedTwice, surging, []string{replacement}, objs...); !allowed {
		t.Fatalf("the %s on pair-v1 keeps both old pods paired and the %s may run one pod short of a partner, so the roll may leave the pair-v2 replacement behind: %s", rebuilt, rebuilt, reason)
	}
	rebuild := func(from string) (bool, v1beta1.RolloutHoldGate, string) {
		c := fakeclient.New(t, fakeclient.WithObjects(objs...))
		return EvaluateUpdateGate(context.Background(), c, changedTwice, rebuilt, "llama-"+string(rebuilt)+"-"+from, forwardTarget(changedTwice, rebuilt), nil, GroupDefaults{}, workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	}
	if allowed, gate, reason := rebuild("v1"); allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "would leave no pairable serving engine+decoder pair") {
		t.Errorf("the last pair-v1 %s must not be rebuilt on the strength of a replacement that is leaving: allowed=%v gate=%q reason=%q", rebuilt, allowed, gate, reason)
	}
	if allowed, gate, reason := rebuild("v2"); !allowed && gate == v1beta1.RolloutHoldGatePairing {
		t.Errorf("the pair-v2 %s has no partner to lose, so its rebuild may open; held under the Pairing gate: %s", rebuilt, reason)
	}
}
