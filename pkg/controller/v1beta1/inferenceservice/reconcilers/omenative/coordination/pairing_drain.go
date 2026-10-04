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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// EvaluatePairingDrain is the drain-time half of the pair floor.
//
// CheckPairing runs once, at operation start, over the simulated end state
// of the whole surge-and-drain op. At 1×1 with no target-cohort capacity it
// must admit both first movers or deadlock the transition, which leaves each
// source's drain to its own replacement's readiness; when one side's
// replacement is slower, the router is left with an engine and a decoder of
// different cohorts until it arrives. A surge update therefore asks again
// right before its source leaves rotation, with the replacement already
// serving: once the source's pods are gone, does some pairable serving
// engine+decoder pair remain? Holding here cannot deadlock, because the
// peer's replacement comes up whether or not this drain waits, so there is
// no mutual-wall escape at this point.
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
	if pairableServingPairExists(after) {
		return true, "", ""
	}
	cohorts := make([]string, 0, len(leaving))
	for proto := range leaving {
		cohorts = append(cohorts, fmt.Sprintf("%q", proto))
	}
	sort.Strings(cohorts)
	return false, v1beta1.RolloutHoldGatePairing, fmt.Sprintf(
		"pairing protocol transition to %q: draining the serving %s pods of cohort %s would leave no pairable serving engine+decoder pair; holding the drain until cohort %q serves on both Components",
		target, component, strings.Join(cohorts, ","), target)
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
