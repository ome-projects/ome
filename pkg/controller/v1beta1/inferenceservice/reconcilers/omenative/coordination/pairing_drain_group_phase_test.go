package coordination

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The drain-time pairing gate read beside the group phase. The gate keeps a
// surging pairing member's old pod in rotation after its replacement
// serves, or the replacement itself once the roll target has moved past the
// revision its surge attempt moves to, while the pod it would retire is the
// partner the other side cannot do without. The group phase is computed from
// the same pods and the same replica rows, so each case names the phase the
// coordination pass writes at the moment the gate answers.

// drainInstance is one Instance of a pairing side at the moment the gate is
// asked: the pairing protocol its old pod (ordinal 0) and its new pod
// (ordinal 1) speak, whether each is Ready, and what its row says about the
// roll. A protocol of "" is no pod on that ordinal.
type drainInstance struct {
	old, new           string
	oldReady, newReady bool
	// promoted rests the row on the revision of its new pod, the old pod
	// gone; surgeTo keeps a surge attempt open toward the named revision
	// hash, the old pod still in rotation beside the replacement.
	promoted bool
	surgeTo  string
	// failed reads the row Failed, its attempt spent and no pod left.
	failed bool
}

// drainSide is one pairing side: its update strategy, its own budgets and
// its Instances in index order.
type drainSide struct {
	strategy       v1beta1.UpdateStrategyType
	maxUnavailable int
	maxSurge       int
	instances      []drainInstance
}

// drainPicture is a service mid-change with both pairing sides, under the
// protocol the spec now names.
type drainPicture struct {
	protocol string
	engine   drainSide
	decoder  drainSide
	// paused sets the pause annotation the coordination pass reads ahead
	// of every member fact.
	paused bool
}

// drainHash names the revision hash a pairing protocol was minted under:
// the spec's protocol is the roll target, the others keep their own name.
func drainHash(protocol, target string) string {
	if protocol == target {
		return "target"
	}
	return strings.TrimPrefix(protocol, "pair-")
}

// drainRevisions are the ControllerRevisions of both sides for every
// protocol the picture names, the roll target among them.
func drainRevisions(p drainPicture) []client.Object {
	var out []client.Object
	for _, comp := range pairingComponents {
		for _, proto := range []string{"pair-v1", "pair-v2", "pair-v3"} {
			if proto == p.protocol {
				continue
			}
			out = append(out, pairingCR("prod", "llama", comp, drainHash(proto, p.protocol), proto))
		}
		out = append(out, pairingCR("prod", "llama", comp, "target", p.protocol))
	}
	return out
}

// drainPods are the routable pods of one side, named the way Render names
// them, each carrying its protocol and its readiness.
func drainPods(comp v1beta1.ComponentType, side drainSide) []client.Object {
	var out []client.Object
	for i, inst := range side.instances {
		if inst.old != "" {
			out = append(out, drainPod("llama", comp, fmt.Sprintf("llama-%s-%d-default-0", comp, i), "default", inst.old, inst.oldReady))
		}
		if inst.new != "" {
			out = append(out, drainPod("llama", comp, fmt.Sprintf("llama-%s-%d-default-1", comp, i), "default", inst.new, inst.newReady))
		}
	}
	return out
}

// drainReplica is one side's InferenceReplica as the replica controller
// publishes it for the picture: the spec's count and budgets, the roll from
// the pair-v1 revision to the target, one row per Instance, and the
// counters the coordination pass reads. UpdatedReplicas counts the rows
// resting on the target revision and UpdatedReadyReplicas those whose pod
// is Ready, the reading of TakeInlineV1Publication; a row promoted onto a
// revision the target has since moved past counts for neither.
func drainReplica(comp v1beta1.ComponentType, p drainPicture, side drainSide) *v1beta1.InferenceReplica {
	rev := func(hash string) string { return "llama-" + string(comp) + "-" + hash }
	ir := pairingIR("prod", "llama", comp, nil)
	ir.Spec.Replicas = ptr.To(int32(len(side.instances)))
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
		Type: side.strategy,
		RollingUpdate: &v1beta1.RollingUpdate{
			MaxUnavailable: ptr.To(intstr.FromInt(side.maxUnavailable)),
			MaxSurge:       ptr.To(intstr.FromInt(side.maxSurge)),
		},
	}}
	ir.Status.CurrentRevision = rev("v1")
	ir.Status.UpdateRevision = rev("target")
	ir.Status.Replicas = int32(len(side.instances))
	for i, inst := range side.instances {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: v1beta1.OMENativeInstanceReady, RunningRevision: rev("v1")}
		switch {
		case inst.failed:
			row.Phase = v1beta1.OMENativeInstanceFailed
			row.TargetRevision = rev("target")
		case inst.promoted:
			row.RunningRevision = rev(drainHash(inst.new, p.protocol))
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 1, ready(inst.newReady), ready(inst.newReady)
		case inst.surgeTo != "":
			row.Phase = v1beta1.OMENativeInstanceUpdating
			row.TargetRevision = rev("target")
			row.PodCount = 2
			row.ReadyPodCount = ready(inst.oldReady) + ready(inst.newReady)
			row.ServingPodCount = row.ReadyPodCount
			row.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurge, TargetRevision: rev(inst.surgeTo)}
		default:
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 1, ready(inst.oldReady), ready(inst.oldReady)
		}
		if row.RunningRevision == rev("target") {
			ir.Status.UpdatedReplicas++
			if inst.newReady {
				ir.Status.UpdatedReadyReplicas++
			}
		}
		if row.ReadyPodCount > 0 {
			ir.Status.ReadyReplicas++
			ir.Status.ServingReplicas++
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, row)
	}
	return ir
}

func ready(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// drainObjects is everything the gate and the phase read for the picture:
// both replicas, every revision and every pod.
func drainObjects(p drainPicture) []client.Object {
	objs := drainRevisions(p)
	objs = append(objs, drainReplica(v1beta1.EngineComponent, p, p.engine), drainReplica(v1beta1.DecoderComponent, p, p.decoder))
	objs = append(objs, drainPods(v1beta1.EngineComponent, p.engine)...)
	return append(objs, drainPods(v1beta1.DecoderComponent, p.decoder)...)
}

// drainGroupPhase is the phase the coordination pass computes for the
// picture under a blueGreen group, the only group kind admission lets a
// pairing-protocol change roll in: each side's counters as its replica
// publishes them and its pods counted per revision, as the pass lists them.
func drainGroupPhase(p drainPicture) v1beta1.CoordinationPhase {
	obs := GroupObservation{
		Group:        ResolvedGroup{Name: "0", Policy: v1beta1.CoordinationPolicyBlueGreen, Components: pairingComponents},
		Components:   map[v1beta1.ComponentType]ComponentObservation{},
		PausedGlobal: p.paused,
	}
	for comp, side := range map[v1beta1.ComponentType]drainSide{v1beta1.EngineComponent: p.engine, v1beta1.DecoderComponent: p.decoder} {
		pods := map[string]int32{}
		for _, obj := range drainPods(comp, side) {
			pods[obj.GetLabels()[query.LabelRevisionHash]]++
		}
		obs.Components[comp] = buildComponentObservation(&drainReplica(comp, p, side).Status, comp, pods, 0)
	}
	return ComputeTransition(obs).Phase
}

// surgeSide is a side that surges, none of its Instances allowed out of
// pairing unless maxUnavailable says otherwise.
func surgeSide(maxUnavailable, maxSurge int, instances ...drainInstance) drainSide {
	return drainSide{strategy: v1beta1.UpdateStrategySurgeThenDrain, maxUnavailable: maxUnavailable, maxSurge: maxSurge, instances: instances}
}

// repeat is n copies of one Instance shape.
func repeat(n int, inst drainInstance) []drainInstance {
	out := make([]drainInstance, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, inst)
	}
	return out
}

// TestEvaluatePairingDrain_HoldsWhateverPhaseTheGroupReads asks the drain
// gate about a pod leaving rotation in the pictures a pairing-protocol
// change passes through, and reads the group phase off the same pictures.
// Three reasons keep a pod, in the gate's order:
//
//   - noPartner: the replacement cannot pair yet, because no serving pod of
//     the other side runs the new protocol, while the old pod still pairs.
//     12 prefill beside 8 decode, both surging: the first new decode serves
//     before any new prefill (the group reads Surging); the first new prefill
//     served and then lost its pod (Waiting).
//   - belowMinimum: the drain would leave a side with fewer pairable serving
//     Instances than its pairable minimum, its desired count less its
//     maxUnavailable. 6 prefill with none allowed out beside 2 decode: the
//     second decode's old pod is the last partner of the three old prefill,
//     while the first new prefill still waits to leave its own old pod behind
//     (Surging), while one promoted prefill is not Ready (Waiting), and while
//     every new pod is Ready (Shifting).
//   - lastPair: the pod is the last keeping any serving engine and decoder
//     pair serving. A second protocol change lands while the first is half
//     done: the decode's replacement on the middle protocol is the only
//     partner of the engines on it, so the roll's attempt to retire that
//     replacement is held (Surging; Waiting once an Instance of each side
//     rests on the newest protocol with the decode's pod not yet Ready).
//
// A Failed row of either side turns the group Failed and changes no answer.
//
// A pause turns the group Paused and reaches one question of the gate: a
// paused Component starts no step, so no old pod is asked about, but the
// retire of a replacement the roll target has moved past is not a step,
// and the pass asks the gate about that replacement before taking it out.
// The second change under a pause: the decode's replacement on the middle
// protocol is the only partner of the engines resting on it, so it stays
// for the pairable minimum of three engines with one allowed out
// (belowMinimum) or as the last pair of one engine (lastPair); its own
// cohort is the one it is traded for, so the no-partner reason cannot name
// it.
func TestEvaluatePairingDrain_HoldsWhateverPhaseTheGroupReads(t *testing.T) {
	dec := v1beta1.DecoderComponent
	oldServing := drainInstance{old: "pair-v1", oldReady: true}
	newServing := drainInstance{new: "pair-v2", newReady: true, promoted: true}
	newUnready := drainInstance{new: "pair-v2", promoted: true}
	replacementServing := drainInstance{old: "pair-v1", oldReady: true, new: "pair-v2", newReady: true, surgeTo: "target"}
	replacementStarting := drainInstance{old: "pair-v1", oldReady: true, new: "pair-v2", surgeTo: "target"}
	spent := drainInstance{failed: true}

	// The first protocol change on the reference shape: 12 prefill surging
	// two at a time beside 8 decode surging one at a time.
	twelveEight := func(engine, decoder []drainInstance) drainPicture {
		return drainPicture{protocol: "pair-v2", engine: surgeSide(1, 2, engine...), decoder: surgeSide(1, 1, decoder...)}
	}
	// The first protocol change on 6 prefill, none allowed out of pairing,
	// beside 2 decode.
	sixTwo := func(engine, decoder []drainInstance) drainPicture {
		return drainPicture{protocol: "pair-v2", engine: surgeSide(0, 2, engine...), decoder: surgeSide(1, 1, decoder...)}
	}
	// A second protocol change, pair-v2 to pair-v3, while the first is half
	// done: the decode's first surge still moves to pair-v2 with its
	// replacement serving beside its old pair-v1 pod.
	middle := drainInstance{old: "pair-v1", oldReady: true, new: "pair-v2", newReady: true, surgeTo: "v2"}
	onMiddle := drainInstance{new: "pair-v2", newReady: true, promoted: true}
	secondChange := func(engine, decoder []drainInstance) drainPicture {
		return drainPicture{protocol: "pair-v3", engine: surgeSide(1, 1, engine...), decoder: surgeSide(1, 1, decoder...)}
	}
	paused := func(p drainPicture) drainPicture {
		p.paused = true
		return p
	}

	cases := []struct {
		name    string
		picture drainPicture
		leaving string // the pod the gate is asked about
		phase   v1beta1.CoordinationPhase
		reason  string // the words of the gate's reason
	}{
		{
			name:    "Surging/noPartner",
			picture: twelveEight(append(repeat(2, replacementStarting), repeat(10, oldServing)...), append([]drainInstance{replacementServing}, repeat(7, oldServing)...)),
			leaving: "llama-decoder-0-default-0", phase: v1beta1.CoordinationPhaseSurging, reason: "cannot pair",
		},
		{
			name:    "Surging/belowMinimum",
			picture: sixTwo(append([]drainInstance{replacementServing}, repeat(5, oldServing)...), []drainInstance{newServing, replacementServing}),
			leaving: "llama-decoder-1-default-0", phase: v1beta1.CoordinationPhaseSurging, reason: "below its floor",
		},
		{
			name:    "Surging/lastPair",
			picture: secondChange([]drainInstance{onMiddle}, []drainInstance{middle, oldServing}),
			leaving: "llama-decoder-0-default-1", phase: v1beta1.CoordinationPhaseSurging, reason: "would leave no pairable serving engine+decoder pair",
		},
		{
			name:    "Waiting/noPartner",
			picture: twelveEight(append([]drainInstance{newUnready}, repeat(11, oldServing)...), append([]drainInstance{newServing, replacementServing}, repeat(6, oldServing)...)),
			leaving: "llama-decoder-1-default-0", phase: v1beta1.CoordinationPhaseWaiting, reason: "cannot pair",
		},
		{
			name:    "Waiting/belowMinimum",
			picture: sixTwo(append([]drainInstance{newServing, newServing, newUnready}, repeat(3, oldServing)...), []drainInstance{newServing, replacementServing}),
			leaving: "llama-decoder-1-default-0", phase: v1beta1.CoordinationPhaseWaiting, reason: "below its floor",
		},
		{
			name: "Waiting/lastPair",
			picture: secondChange(
				[]drainInstance{{new: "pair-v3", newReady: true, promoted: true}, onMiddle},
				[]drainInstance{middle, oldServing, {new: "pair-v3", promoted: true}}),
			leaving: "llama-decoder-0-default-1", phase: v1beta1.CoordinationPhaseWaiting, reason: "would leave no pairable serving engine+decoder pair",
		},
		{
			name:    "Shifting/belowMinimum",
			picture: sixTwo(append(repeat(3, newServing), repeat(3, oldServing)...), []drainInstance{newServing, replacementServing}),
			leaving: "llama-decoder-1-default-0", phase: v1beta1.CoordinationPhaseShifting, reason: "below its floor",
		},
		{
			name:    "Failed/noPartner",
			picture: twelveEight(append(repeat(2, replacementStarting), repeat(10, oldServing)...), append(append([]drainInstance{replacementServing}, repeat(6, oldServing)...), spent)),
			leaving: "llama-decoder-0-default-0", phase: v1beta1.CoordinationPhaseFailed, reason: "cannot pair",
		},
		{
			name:    "Failed/belowMinimum",
			picture: sixTwo(append(append([]drainInstance{replacementServing}, repeat(4, oldServing)...), spent), []drainInstance{newServing, replacementServing}),
			leaving: "llama-decoder-1-default-0", phase: v1beta1.CoordinationPhaseFailed, reason: "below its floor",
		},
		{
			name:    "Failed/lastPair",
			picture: secondChange([]drainInstance{onMiddle}, []drainInstance{middle, spent}),
			leaving: "llama-decoder-0-default-1", phase: v1beta1.CoordinationPhaseFailed, reason: "would leave no pairable serving engine+decoder pair",
		},
		{
			name:    "Paused/belowMinimum",
			picture: paused(drainPicture{protocol: "pair-v3", engine: surgeSide(1, 1, onMiddle, onMiddle, onMiddle), decoder: surgeSide(1, 1, middle, oldServing)}),
			leaving: "llama-decoder-0-default-1", phase: v1beta1.CoordinationPhasePaused, reason: "with 0 pairable serving Instance(s)",
		},
		{
			name:    "Paused/lastPair",
			picture: paused(secondChange([]drainInstance{onMiddle}, []drainInstance{middle, oldServing})),
			leaving: "llama-decoder-0-default-1", phase: v1beta1.CoordinationPhasePaused, reason: "would leave no pairable serving engine+decoder pair",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := drainGroupPhase(tc.picture); got != tc.phase {
				t.Fatalf("group phase %q, want %q", got, tc.phase)
			}
			isvc := pairingISVC(tc.picture.protocol)
			allowed, gate, reason := drainGate(t, isvc, dec, []string{tc.leaving}, drainObjects(tc.picture)...)
			if allowed || gate != v1beta1.RolloutHoldGatePairing {
				t.Fatalf("allowed=%v gate=%q reason=%q, want a Pairing hold", allowed, gate, reason)
			}
			if !strings.Contains(reason, tc.reason) {
				t.Errorf("reason %q, want it to say %q", reason, tc.reason)
			}
		})
	}
}
