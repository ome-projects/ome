package coordination

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// ratioPairingISVC declares engine and decoder in one rollingUpdate group
// under maintainRatio at the given tolerance, with the reference the service
// pass recorded and the pairing protocol the spec names ("" leaves it unset).
func ratioPairingISVC(tolerance int32, original map[v1beta1.ComponentType]int32, protocol string) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "prod"},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(tolerance)},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:          "0",
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:        v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{Original: original},
				}},
			},
		},
	}
	if protocol != "" {
		isvc.Spec.Rollout.PairingProtocol = ptr.To(protocol)
	}
	return pinActiveRun(isvc)
}

// rolledRatioIR is rolledPairingIR with the promoted count the lockstep
// reads and, when kept names an Instance, that Instance's replacement
// serving beside its old pod at the Surge step.
func rolledRatioIR(comp v1beta1.ComponentType, strategy v1beta1.UpdateStrategyType, replicas, maxUnavailable, promoted, kept int) *v1beta1.InferenceReplica {
	ir := rolledPairingIR(comp, strategy, replicas, maxUnavailable, revisionsOf(promoted, replicas)...)
	ir.Status.UpdatedReplicas = int32(promoted)
	if kept >= 0 {
		ir.Status.InstanceStatuses[kept] = surgeReplacementServingRow(comp, int32(kept))
	}
	return ir
}

// allPairV2CRs are the three revisions of each side all on pair-v2: no
// pairing-protocol change is in flight.
func allPairV2CRs() []client.Object {
	var crs []client.Object
	for _, comp := range pairingComponents {
		for _, hash := range []string{"v1", "v2", "target"} {
			crs = append(crs, pairingCR("prod", "llama", comp, hash, "pair-v2"))
		}
	}
	return crs
}

// lockstepPicture is one picture of a pairing-protocol change under the
// ratio rule where the batch step is off for the pair (one side patches
// single pods in place and the batch-planning setting is off): the
// replicas, the side whose next start is asked about and how it moves.
type lockstepPicture struct {
	name      string
	tolerance int32
	original  map[v1beta1.ComponentType]int32
	crs       []client.Object
	engine    *v1beta1.InferenceReplica
	decoder   *v1beta1.InferenceReplica
	acting    v1beta1.ComponentType
	strategy  workloadtypes.UpdateStrategyType
}

func (p lockstepPicture) objects() []client.Object {
	return append(append([]client.Object{}, p.crs...), p.engine, p.decoder)
}

// The ratio rule's lockstep holds a side whose share on the new revision
// would stand more than its allowance ahead of a still-rolling peer. During
// a pairing-protocol change the pairing gates can hold that peer still: its
// last replacement serves beside an old pod the drain gate keeps for this
// side's old Instances, or every start it could open is held under this
// side's pairable minimum. Such a peer moves only once this side has, so a
// lockstep that waited on it would wait on this side's own move, and the
// two would hold each other for good. The lockstep does not pace a side by
// a peer the pairing gates hold still, as the batch step does not wait on
// one; the start is admitted, and the drain gate releases the kept pod once
// the old Instances have moved.
func TestEvaluateUpdateGate_TheLockstepWaivesAPeerThePairingGatesHoldStill(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	inPlace, surge := v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategySurgeThenDrain
	twelveFour := map[v1beta1.ComponentType]int32{eng: 12, dec: 4}
	pictures := []lockstepPicture{
		{
			name: "12 prefill patched in place at 10 of 12 beside 4 decode surging at 3 of 4, the last old pod kept beside its serving replacement", tolerance: 5, original: twelveFour, crs: threeCohortCRs("pair-v2"),
			engine: rolledRatioIR(eng, inPlace, 12, 1, 10, -1), decoder: rolledRatioIR(dec, surge, 4, 1, 3, 3),
			acting: eng, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible,
		},
		{
			name: "6 prefill patched in place at 4 of 6 beside 2 decode surging at 1 of 2, the last old pod kept beside its serving replacement", tolerance: 5, original: map[v1beta1.ComponentType]int32{eng: 6, dec: 2}, crs: threeCohortCRs("pair-v2"),
			engine: rolledRatioIR(eng, inPlace, 6, 1, 4, -1), decoder: rolledRatioIR(dec, surge, 2, 1, 1, 1),
			acting: eng, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible,
		},
		{
			name: "12 prefill surging at 10 of 12 beside 4 decode patched in place at 3 of 4 whose last patch the pairable minimum holds", tolerance: 5, original: twelveFour, crs: threeCohortCRs("pair-v2"),
			engine: rolledRatioIR(eng, surge, 12, 1, 10, -1), decoder: rolledRatioIR(dec, inPlace, 4, 1, 3, -1),
			acting: eng, strategy: workloadtypes.UpdateStrategySurgeThenDrain,
		},
	}
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			isvc := ratioPairingISVC(pic.tolerance, pic.original, "pair-v2")
			objs := pic.objects()
			gate := pairingGate(t, isvc, pic.acting, objs...)
			if v := gate.pairingVerdict("llama-"+string(pic.acting)+"-v1", pic.strategy, 0, 0); !v.allowed || !v.peerHeldStill {
				t.Fatalf("the picture must have the pairing gate admit the %s's start and read the %s held still: allowed=%v peerHeldStill=%v reason=%q", pic.acting, pairingPeer(pic.acting), v.allowed, v.peerHeldStill, v.reason)
			}
			if allowed, gate, reason := askStart(t, isvc, objs, pic.acting, pic.strategy); !allowed {
				t.Fatalf("the %s's next start must open while the pairing gates hold the %s still: held under %s: %s", pic.acting, pairingPeer(pic.acting), gate, reason)
			}
		})
	}

	t.Run("the band still reads the start: a drain no step keeps inside a tolerance of 2 is admitted as the one step outside it, and the service is told", func(t *testing.T) {
		// 12 prefill rebuilt one at a time at 10 of 12 beside 4 decode patched
		// in place at 3 of 4, the decoder's last patch held under the
		// prefill side's pairable minimum of 11. The lockstep leaves the
		// held-still decoder out; the band then reads the drain on its own:
		// 11 beside 4 is 8.3 points off the 3:1 reference, past twice the
		// tolerance of 2, so it is admitted as the one step no step keeps
		// inside the tolerance and RatioStepOutsideTolerance is raised.
		pic := lockstepPicture{
			tolerance: 2, original: twelveFour, crs: threeCohortCRs("pair-v2"),
			engine: rolledRatioIR(eng, v1beta1.UpdateStrategyRecreatePod, 12, 1, 10, -1), decoder: rolledRatioIR(dec, inPlace, 4, 1, 3, -1),
			acting: eng, strategy: workloadtypes.UpdateStrategyRecreatePod,
		}
		isvc := ratioPairingISVC(pic.tolerance, pic.original, "pair-v2")
		objs := pic.objects()
		if v := pairingGate(t, isvc, eng, objs...).pairingVerdict("llama-engine-v1", pic.strategy, 0, 0); !v.allowed || !v.peerHeldStill {
			t.Fatalf("the picture must have the pairing gate admit the drain and read the decoder held still: allowed=%v peerHeldStill=%v reason=%q", v.allowed, v.peerHeldStill, v.reason)
		}
		recorder := record.NewFakeRecorder(4)
		c := fakeclient.New(t, fakeclient.WithObjects(objs...))
		allowed, gate, reason := EvaluateUpdateGate(context.Background(), c, isvc, eng, "llama-engine-v1", forwardTarget(isvc, eng), recorder, GroupDefaults{}, pic.strategy, 0, 0)
		if !allowed {
			t.Fatalf("the drain is the one step the band admits at the reference: held under %s: %s", gate, reason)
		}
		select {
		case event := <-recorder.Events:
			if !strings.Contains(event, EventReasonRatioStepOutsideTolerance) {
				t.Fatalf("the band must say the step runs outside the tolerance, got %q", event)
			}
		default:
			t.Fatalf("the waiver reaches the lockstep alone; the serving band must still read the drain and raise %s", EventReasonRatioStepOutsideTolerance)
		}
	})
}

// A peer the pairing gates do not hold still is moving on its own, and the
// lockstep holds the leading side for it as before: a surging peer whose
// last Instance has no attempt open yet, a peer patched in place whose last
// patch the pairable minimum admits, the same replacement picture outside
// a protocol change, where the drain gate does not keep the old pod, and a
// peer already on the target protocol on every Instance that has its own
// revision roll to run and no start of the old cohort left to hold.
func TestEvaluateUpdateGate_TheLockstepHoldsASideWhosePeerCanStillMove(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	inPlace, surge := v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategySurgeThenDrain
	twelveFour := map[v1beta1.ComponentType]int32{eng: 12, dec: 4}
	// A second push on the protocol the first change brought in: every
	// decoder serves pair-v2 on revision v2 and none is promoted to the
	// target revision, so the decoder has no attempt open and no start of
	// the old cohort for the pairing gate to hold.
	decoderOnTheProtocolBehindOnTheRevision := rolledPairingIR(dec, surge, 4, 1, "v2", "v2", "v2", "v2")
	pictures := []struct {
		lockstepPicture
		reason string
	}{
		{lockstepPicture{
			name: "the decoder's last Instance is old with no attempt open", tolerance: 5, original: twelveFour, crs: threeCohortCRs("pair-v2"),
			engine: rolledRatioIR(eng, inPlace, 12, 1, 10, -1), decoder: rolledRatioIR(dec, surge, 4, 1, 3, -1),
			acting: eng, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible,
		}, "in-place update would outpace peer new-revision rollout past tolerance"},
		{lockstepPicture{
			name: "the decoder's last patch is admitted under a pairable minimum of 10", tolerance: 5, original: twelveFour, crs: threeCohortCRs("pair-v2"),
			engine: rolledRatioIR(eng, surge, 12, 2, 10, -1), decoder: rolledRatioIR(dec, inPlace, 4, 1, 3, -1),
			acting: eng, strategy: workloadtypes.UpdateStrategySurgeThenDrain,
		}, "surge would outpace peer new-revision rollout past tolerance"},
		{lockstepPicture{
			name: "no protocol change: the decoder's old pod leaves on its own", tolerance: 5, original: twelveFour, crs: allPairV2CRs(),
			engine: rolledRatioIR(eng, inPlace, 12, 1, 10, -1), decoder: rolledRatioIR(dec, surge, 4, 1, 3, 3),
			acting: eng, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible,
		}, "in-place update would outpace peer new-revision rollout past tolerance"},
		{lockstepPicture{
			name: "the decoder already speaks the target protocol on every Instance and rolls to a newer revision of it", tolerance: 5, original: twelveFour, crs: threeCohortCRs("pair-v2"),
			engine: rolledRatioIR(eng, inPlace, 12, 1, 10, -1), decoder: decoderOnTheProtocolBehindOnTheRevision,
			acting: eng, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible,
		}, "in-place update would outpace peer new-revision rollout past tolerance"},
	}
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			isvc := ratioPairingISVC(pic.tolerance, pic.original, "pair-v2")
			objs := pic.objects()
			if v := pairingGate(t, isvc, pic.acting, objs...).pairingVerdict("llama-"+string(pic.acting)+"-v1", pic.strategy, 0, 0); !v.allowed || v.peerHeldStill {
				t.Fatalf("the picture must have the pairing gate admit the start and not read the peer held still: allowed=%v peerHeldStill=%v reason=%q", v.allowed, v.peerHeldStill, v.reason)
			}
			allowed, gate, reason := askStart(t, isvc, objs, pic.acting, pic.strategy)
			if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != pic.reason {
				t.Fatalf("the %s stands past its allowance ahead of a peer that can move, so the lockstep must hold it = (%v, %q, %q)", pic.acting, allowed, gate, reason)
			}
		})
	}

	t.Run("a router in the group still paces the side while the decoder is held still", func(t *testing.T) {
		// The first picture of the waiver with 2 routers rebuilt one at a
		// time in the same group, none moved yet: the decoder is left out of
		// the lockstep's reading, the router is not, and 10 of 12 beside 0 of
		// 2 is past the allowance.
		rtr := v1beta1.RouterComponent
		isvc := ratioPairingISVC(5, map[v1beta1.ComponentType]int32{eng: 12, dec: 4, rtr: 2}, "pair-v2")
		isvc.Spec.Rollout.Groups[0].Components = append(isvc.Spec.Rollout.Groups[0].Components, rtr)
		isvc.Status.RolloutCoordination.Groups[0].Components = append(isvc.Status.RolloutCoordination.Groups[0].Components, rtr)
		isvc = pinActiveRun(isvc)
		objs := append(threeCohortCRs("pair-v2"),
			pairingCR("prod", "llama", rtr, "v1", ""), pairingCR("prod", "llama", rtr, "target", ""),
			rolledRatioIR(eng, inPlace, 12, 1, 10, -1), rolledRatioIR(dec, surge, 4, 1, 3, 3),
			rolledRatioIR(rtr, v1beta1.UpdateStrategyRecreatePod, 2, 1, 0, -1))
		if v := pairingGate(t, isvc, eng, objs...).pairingVerdict("llama-engine-v1", workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 0); !v.allowed || !v.peerHeldStill {
			t.Fatalf("the picture must have the decoder held still: allowed=%v peerHeldStill=%v reason=%q", v.allowed, v.peerHeldStill, v.reason)
		}
		allowed, gate, reason := askStart(t, isvc, objs, eng, workloadtypes.UpdateStrategyInPlaceIfPossible)
		if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != "in-place update would outpace peer new-revision rollout past tolerance" {
			t.Fatalf("the router at 0 of 2 still paces the engine at 10 of 12 = (%v, %q, %q)", allowed, gate, reason)
		}
	})
}
