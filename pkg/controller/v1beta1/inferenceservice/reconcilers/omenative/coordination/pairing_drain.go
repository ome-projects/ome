package coordination

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// EvaluatePairingDrain is the drain-time half of the pair floor. A surge
// update asks it right before its source leaves rotation, with the
// replacement already serving, and the source stays in rotation while any
// of these holds:
//   - the replacement cannot pair yet and the pods leaving can: no serving
//     pod of the peer runs the target cohort (or the empty protocol, which
//     pairs with anything), so retiring a source that pairs would trade
//     serving capacity for none. The first peer pod of the target cohort to
//     serve releases the hold, whatever mechanism brings it up: a surging
//     peer's replacements come up without this drain, and a drain-first or
//     in-place peer's start is admitted beside the kept pods, because the
//     start-time gate reads an Instance kept here as held still and the
//     batch step does not wait on it (pairingPicture.heldStill);
//   - the drain would push the peer below its pairable floor: fewer than
//     desired minus maxUnavailable of its Instances would keep a serving pod
//     with a partner, because the pods leaving are the last partners of the
//     peer's old-cohort Instances. Those Instances recover a partner on
//     their own, through a replacement of their own in rotation or their
//     own move, so the hold always ends;
//   - the source is the last pod keeping any pairable engine+decoder pair
//     alive.
//
// An Instance counts as pairable while any of its routable serving pods has
// a partner, the surge replacement included from the moment it serves, so
// an old pod whose replacement already pairs never costs its Instance.
//
// It reads pods, not InferenceReplica status: a single-pod surge keeps its
// replacement inside the source's Instance until the promote, so only the
// pods' cohort labels tell the two cohorts apart. A serving pod is PodReady
// (the serving gate is folded into it) and routable; workers never carry
// traffic. targetRevision is the ControllerRevision the Component's Instances
// move to in this pass, the same one the start-time gate orients by. Fails
// closed on a list error, like the start-time gate.
//
// The cohort the leaving pods are traded for is the one of the revision
// their Instance's in-flight update is pinned to, read from the Component's
// replica. That is the pass target's cohort on a first change. When a later
// edit moves the target while the hand-over is already under way, the
// replacement in rotation still runs the pinned revision, and judging it as
// a pod of the newest cohort would hold the hand-over for a partner nothing
// brings up, or admit it against partners it does not have. The same reading
// answers the abandon question a moved target raises: the replacement
// itself leaving rotation, with the source staying. It is then on the
// pinned cohort already, so the hold that applies is the floor or the
// one-pair rule, which is what its partners on the other side would lose.
func EvaluatePairingDrain(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, targetRevision *appsv1.ControllerRevision, defaults GroupDefaults, sourcePods []string) (allowed bool, gate v1beta1.RolloutHoldGate, reason string) {
	gateCtx := ResolveGateContextWithDefaults(ctx, reads, isvc, component, defaults)
	gateCtx.TargetRevision = targetRevision
	if gateCtx.ShortCircuit {
		return true, "", gateCtx.ShortReason
	}
	if !groupPairsEngineAndDecoder(gateCtx.Group) {
		return true, "", "group does not pair engine and decoder"
	}
	if component != v1beta1.EngineComponent && component != v1beta1.DecoderComponent {
		return true, "", "component does not participate in pairing"
	}
	target, ok, reason := gateCtx.targetPairingProtocol()
	if !ok {
		return false, v1beta1.RolloutHoldGatePairing, reason
	}
	if target == "" {
		return true, "", "target revision declares no pairing protocol"
	}

	// Each pairing Component's pods are listed under its own replica prefix:
	// a referenced peer's pods carry the peer replica's name, not the
	// service's.
	serving := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.EngineComponent:  {},
		v1beta1.DecoderComponent: {},
	}
	// instances lists, per Component and Instance, the cohorts of its
	// routable serving pods.
	instances := map[v1beta1.ComponentType]map[string][]string{
		v1beta1.EngineComponent:  {},
		v1beta1.DecoderComponent: {},
	}
	protoByPod := map[string]string{}
	instanceByPod := map[string]string{}
	for _, comp := range pairingComponents {
		prefix := prefixFor(isvc, comp)
		pods := &corev1.PodList{}
		if err := reads.List(ctx, pods, client.InNamespace(isvc.Namespace), client.MatchingLabels{
			constants.InferenceServicePodLabelKey: prefix,
			constants.OMEComponentLabel:           string(comp),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		}); err != nil {
			return false, v1beta1.RolloutHoldGatePairing, fmt.Sprintf("pairing drain gate: cannot list %s pods, failing closed: %v", comp, err)
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.DeletionTimestamp != nil || !query.RoutedRunner(pod) {
				continue
			}
			proto := pod.Labels[query.LabelPairingProtocol]
			if !podreadiness.IsPodReady(pod) || query.RoutedServiceForPod(prefix, workloadtypes.ComponentType(comp), pod) == "" {
				continue
			}
			serving[comp][proto]++
			key := instanceKeyForPod(pod)
			instances[comp][key] = append(instances[comp][key], proto)
			protoByPod[pod.Name] = proto
			instanceByPod[pod.Name] = key
		}
	}

	// Both pairing replicas, read once: the revision the leaving pods'
	// Instance moves to and both sides' desired counts and budgets come
	// from them. A missing replica leaves the one-pair rule; an unreadable
	// one fails closed.
	replicas := map[v1beta1.ComponentType]*v1beta1.InferenceReplica{}
	for _, comp := range pairingComponents {
		ir, _, err := irprojector.DecodedComponentIRFor(ctx, reads, isvc, comp)
		if err != nil {
			return false, v1beta1.RolloutHoldGatePairing, fmt.Sprintf("pairing drain gate: cannot read the %s replica, failing closed: %v", comp, err)
		}
		replicas[comp] = ir
	}
	cohort, reason := pinnedCohort(ctx, reads, isvc.Namespace, component, replicas[component], instanceByPod, sourcePods, gateCtx.TargetRevision.Name, target)
	if reason != "" {
		return false, v1beta1.RolloutHoldGatePairing, reason
	}
	if cohort == "" {
		return true, "", "the revision the Instance hands over to declares no pairing protocol"
	}
	transition := false
	for _, comp := range pairingComponents {
		for proto, n := range serving[comp] {
			if n > 0 && proto != "" && proto != cohort {
				transition = true
			}
		}
	}
	if !transition {
		return true, "", "no pairing protocol transition in flight"
	}
	if !pairableServingPairExists(serving) {
		return true, "", fmt.Sprintf("no serving engine+decoder pair exists to protect; allowing progress toward cohort %q", cohort)
	}

	after := copyServing(serving)
	afterInstances := copyInstances(instances)
	leaving := map[string]struct{}{}
	for _, name := range sourcePods {
		proto, ok := protoByPod[name]
		if !ok {
			continue // not serving, or a worker: nothing leaves rotation
		}
		after[component][proto]--
		leaving[proto] = struct{}{}
		key := instanceByPod[name]
		afterInstances[component][key] = withoutOneCohort(afterInstances[component][key], proto)
	}
	peer := pairingPeer(component)
	picture := pairingDrainPicture{
		component: component,
		cohort:    cohort,
		serving:   serving, after: after,
		instances: instances, afterInstances: afterInstances,
		leaving: leaving,
		floors: map[v1beta1.ComponentType]pairFloor{
			component: gateCtx.pairableMinimumOf(replicas[component]),
			peer:      gateCtx.pairableMinimumOf(replicas[peer]),
		},
	}
	if held, reason := picture.hold(); held {
		return false, v1beta1.RolloutHoldGatePairing, reason
	}
	return true, "", ""
}

// pairingDrainPicture is what the drain-time pair floor judges when the
// old pods of one pairing Component leave rotation: the serving pods of
// both sides by cohort before and after they leave, each side's Instances
// with the cohorts of their serving pods, the cohorts leaving, the cohort
// the leaving pods are traded for, and both sides' pairable floors. The
// drain gate builds it from pods; the start-time gate builds it from the
// rows to read a hand-over it keeps (pairingPicture.handOverHeld), so both
// read one rule.
type pairingDrainPicture struct {
	component      v1beta1.ComponentType
	cohort         string
	serving        map[v1beta1.ComponentType]map[string]int32
	after          map[v1beta1.ComponentType]map[string]int32
	instances      map[v1beta1.ComponentType]map[string][]string
	afterInstances map[v1beta1.ComponentType]map[string][]string
	leaving        map[string]struct{}
	floors         map[v1beta1.ComponentType]pairFloor
}

// hold is the drain-time pair floor's answer: the old pods stay while the
// replacement cannot pair yet and the pods leaving can, while the drain
// would push the peer below its pairable floor, or while the pods are the
// last keeping any pairable pair alive, as EvaluatePairingDrain describes.
func (p pairingDrainPicture) hold() (held bool, reason string) {
	component, peer := p.component, pairingPeer(p.component)
	if !pairsWithPeer(p.serving[peer], p.cohort) && anyPairsWithPeer(p.serving[peer], p.leaving) {
		return true, fmt.Sprintf(
			"pairing protocol transition to %q: the replacement %s cannot pair because no serving %s runs cohort %q yet; holding the drain of the serving %s of cohort %s until one does",
			p.cohort, component, peer, p.cohort, component, cohortList(p.leaving))
	}
	if floor := p.floors[peer]; floor.Applies() {
		before := pairableInstanceCount(p.instances[peer], p.serving[component])
		left := pairableInstanceCount(p.afterInstances[peer], p.after[component])
		if left < floor.floor && left < before {
			return true, fmt.Sprintf(
				"pairing protocol transition to %q: draining the serving %s pods of cohort %s would leave the %s with %d pairable serving Instance(s), below its %s; holding the drain until the %s Instances still on cohort %s have another partner or have moved",
				p.cohort, component, cohortList(p.leaving), peer, left, floor, peer, cohortList(p.leaving))
		}
	}
	if pairableServingPairExists(p.after) {
		return false, ""
	}
	return true, fmt.Sprintf(
		"pairing protocol transition to %q: draining the serving %s pods of cohort %s would leave no pairable serving engine+decoder pair; holding the drain until cohort %q serves on both Components",
		p.cohort, component, cohortList(p.leaving), p.cohort)
}

// pinnedCohort is the cohort the leaving pods are traded for: the protocol
// of the revision their Instance's in-flight update is pinned to, which is
// the pass target's protocol unless a later edit moved the target while the
// hand-over was under way. Pods of an Instance with no pin, or of none,
// leave the pass target's protocol in force. Fails closed (a non-empty
// reason) when the pinned revision cannot be read or is gone, or when the
// leaving pods are pinned to more than one revision, like the rest of the
// gate.
func pinnedCohort(ctx context.Context, reads client.Reader, namespace string, component v1beta1.ComponentType, ir *v1beta1.InferenceReplica, instanceByPod map[string]string, sourcePods []string, targetName, target string) (string, string) {
	if ir == nil {
		return target, ""
	}
	pins := map[string]struct{}{}
	for _, name := range sourcePods {
		idx, err := strconv.Atoi(instanceByPod[name])
		if err != nil {
			continue
		}
		for i := range ir.Status.InstanceStatuses {
			row := &ir.Status.InstanceStatuses[i]
			if int(row.Index) != idx || row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationUpdate || row.Operation.TargetRevision == "" {
				continue
			}
			pins[row.Operation.TargetRevision] = struct{}{}
		}
	}
	if len(pins) == 0 {
		return target, ""
	}
	if len(pins) > 1 {
		return "", fmt.Sprintf("pairing drain gate: the leaving %s pods are pinned to %d revisions at once, failing closed", component, len(pins))
	}
	var pin string
	for candidate := range pins {
		pin = candidate
	}
	if pin == targetName {
		return target, ""
	}
	proto, found, err := pairingProtocolForRevisionName(ctx, reads, namespace, pin)
	if err != nil {
		return "", fmt.Sprintf("pairing drain gate: cannot read revision %s the leaving %s pods are pinned to, failing closed: %v", pin, component, err)
	}
	if !found {
		return "", fmt.Sprintf("pairing drain gate: revision %s the leaving %s pods are pinned to not found, failing closed", pin, component)
	}
	return proto, ""
}

// instanceKeyForPod names the Instance a pod belongs to, by its index label;
// a pod without one stands alone.
func instanceKeyForPod(pod *corev1.Pod) string {
	if idx, ok := query.InstanceIdxFromLabels(pod); ok {
		return strconv.Itoa(int(idx))
	}
	return "pod/" + pod.Name
}

func copyInstances(in map[v1beta1.ComponentType]map[string][]string) map[v1beta1.ComponentType]map[string][]string {
	out := make(map[v1beta1.ComponentType]map[string][]string, len(in))
	for comp, byInstance := range in {
		m := make(map[string][]string, len(byInstance))
		for key, protos := range byInstance {
			m[key] = append([]string(nil), protos...)
		}
		out[comp] = m
	}
	return out
}

// withoutOneCohort drops one entry of proto from an Instance's cohort list.
func withoutOneCohort(protos []string, proto string) []string {
	for i, p := range protos {
		if p == proto {
			return append(append([]string(nil), protos[:i]...), protos[i+1:]...)
		}
	}
	return protos
}

// pairingPeer is the other pairing Component.
func pairingPeer(component v1beta1.ComponentType) v1beta1.ComponentType {
	if component == v1beta1.DecoderComponent {
		return v1beta1.EngineComponent
	}
	return v1beta1.DecoderComponent
}

// pairsWithPeer reports whether a pod of cohort proto can pair with some
// serving pod of the peer: equal protocols, or either side empty.
func pairsWithPeer(peerServing map[string]int32, proto string) bool {
	for p, n := range peerServing {
		if n > 0 && (p == "" || proto == "" || p == proto) {
			return true
		}
	}
	return false
}

// anyPairsWithPeer reports whether any of the cohorts pairs with the peer.
func anyPairsWithPeer(peerServing map[string]int32, cohorts map[string]struct{}) bool {
	for proto := range cohorts {
		if pairsWithPeer(peerServing, proto) {
			return true
		}
	}
	return false
}

// cohortList renders cohorts for a hold reason, sorted and quoted.
func cohortList(cohorts map[string]struct{}) string {
	out := make([]string, 0, len(cohorts))
	for proto := range cohorts {
		out = append(out, fmt.Sprintf("%q", proto))
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// groupPairsEngineAndDecoder reports whether the resolved group spans both
// pairing Components; the pair floor is inert otherwise.
func groupPairsEngineAndDecoder(group ResolvedGroup) bool {
	spansEngine, spansDecoder := false, false
	for _, c := range group.Components {
		switch c {
		case v1beta1.EngineComponent:
			spansEngine = true
		case v1beta1.DecoderComponent:
			spansDecoder = true
		}
	}
	return spansEngine && spansDecoder
}
