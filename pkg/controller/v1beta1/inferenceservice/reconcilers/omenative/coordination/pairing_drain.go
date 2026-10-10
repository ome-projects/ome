package coordination

import (
	"context"
	"fmt"
	"sort"
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
// replacement already serving, and the source stays in rotation in two
// cases:
//   - the replacement cannot pair yet and the peer Component updates by
//     SurgeThenDrain: no serving pod of the peer runs the target cohort (or
//     the empty protocol, which pairs with anything), so retiring a source
//     that does pair would trade serving capacity for none, and the peer's
//     replacements come up without waiting on this drain, so the first one
//     to serve releases the hold;
//   - the source is the last pod keeping any pairable engine+decoder pair
//     alive.
//
// A drain-first or in-place peer is never waited on: its new cohort appears
// only by leaving the old one, a step the start-time gate admits on this
// Component's promoted state, so a hold here would wait on a step that waits
// on this drain.
//
// It reads pods, not InferenceReplica status: a single-pod surge keeps its
// replacement inside the source's Instance until the promote, so only the
// pods' cohort labels tell the two cohorts apart. A serving pod is PodReady
// (the serving gate is folded into it) and routable; workers never carry
// traffic. targetRevision is the ControllerRevision the Component's Instances
// move to in this pass, the same one the start-time gate orients by. Fails
// closed on a list error, like the start-time gate.
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
	protoByPod := map[string]string{}
	transition := false
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
			if pod.DeletionTimestamp != nil || !podreadiness.IsPodReady(pod) {
				continue
			}
			if query.RoutedServiceForPod(prefix, workloadtypes.ComponentType(comp), pod) == "" {
				continue
			}
			proto := pod.Labels[query.LabelPairingProtocol]
			serving[comp][proto]++
			protoByPod[pod.Name] = proto
			if proto != "" && proto != target {
				transition = true
			}
		}
	}
	if !transition {
		return true, "", "no pairing protocol transition in flight"
	}
	if !pairableServingPairExists(serving) {
		return true, "", fmt.Sprintf("no serving engine+decoder pair exists to protect; allowing progress toward cohort %q", target)
	}

	after := copyServing(serving)
	leaving := map[string]struct{}{}
	for _, name := range sourcePods {
		proto, ok := protoByPod[name]
		if !ok {
			continue // not serving, or a worker: nothing leaves rotation
		}
		after[component][proto]--
		leaving[proto] = struct{}{}
	}
	peer := pairingPeer(component)
	if !pairsWithPeer(serving[peer], target) && anyPairsWithPeer(serving[peer], leaving) {
		surges, err := peerSurges(ctx, reads, isvc, peer)
		if err != nil {
			return false, v1beta1.RolloutHoldGatePairing, fmt.Sprintf("pairing drain gate: cannot read the %s replica, failing closed: %v", peer, err)
		}
		if surges {
			return false, v1beta1.RolloutHoldGatePairing, fmt.Sprintf(
				"pairing protocol transition to %q: the replacement %s cannot pair because no serving %s runs cohort %q yet; holding the drain of the serving %s of cohort %s until one does",
				target, component, peer, target, component, cohortList(leaving))
		}
	}
	if pairableServingPairExists(after) {
		return true, "", ""
	}
	return false, v1beta1.RolloutHoldGatePairing, fmt.Sprintf(
		"pairing protocol transition to %q: draining the serving %s pods of cohort %s would leave no pairable serving engine+decoder pair; holding the drain until cohort %q serves on both Components",
		target, component, cohortList(leaving), target)
}

// peerSurges reports whether the peer Component updates by SurgeThenDrain,
// the one strategy whose new-cohort pods come up before any old pod leaves.
// An unset strategy is that default; a missing replica reports false.
func peerSurges(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, peer v1beta1.ComponentType) (bool, error) {
	ir, err := irprojector.ComponentIRFor(ctx, reads, isvc, peer)
	if err != nil || ir == nil {
		return false, err
	}
	if ir.Spec.Lifecycle == nil || ir.Spec.Lifecycle.UpdateStrategy == nil {
		return true, nil
	}
	switch ir.Spec.Lifecycle.UpdateStrategy.Type {
	case v1beta1.UpdateStrategySurgeThenDrain, "":
		return true, nil
	}
	return false, nil
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
