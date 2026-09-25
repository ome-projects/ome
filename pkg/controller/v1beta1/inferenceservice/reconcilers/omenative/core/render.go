package core

import (
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// RenderWithRevisionForISVC is the (ReconcileParams, plan, inst,
// runner, ordinal, revisionHash) shape of workload/ops.RenderWithRevision.
// Wires the coordination.InjectPeerEnv hook so per-Component peer DNS
// env vars land on each rendered pod.
//
// Unused — adapters pass the hook directly via Deps.RenderHook (see
// ISVCRenderHook); retained for compatibility.
func RenderWithRevisionForISVC(
	p ReconcileParams,
	template *corev1.PodSpec,
	plan ComponentPlan,
	inst InstancePlan,
	runner RunnerPlan,
	ordinal int32,
	revisionHash string,
) (*corev1.Pod, error) {
	return ops.RenderWithRevision(
		p.ISVC,
		isvcGVK,
		isvcRenderKey(p.ISVC, v1beta1convert.ComponentTypeFromWorkload(plan.Component)),
		template,
		&p.ObjectMeta,
		plan,
		inst,
		runner,
		ordinal,
		revisionHash,
		isvcRenderHook(p.ISVC, nil),
	)
}

// isvcRenderKey is the workload.Key projection for the legacy ISVC
// dispatch path. Key.Component is workload.ComponentType — the v1beta1
// component the caller already holds gets cast at the boundary (the
// two share the same string underlying so the conversion is free).
func isvcRenderKey(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) workloadtypes.Key {
	return workloadtypes.Key{
		Namespace: isvc.Namespace,
		Component: workloadtypes.ComponentType(component),
		OwnerName: isvc.Name,
		SelectorLabels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// ISVCRenderHook returns the workload.RenderHook the ISVC adapter
// wires onto every Render call: coordination.InjectPeerEnv so the
// OME_<PEER>_ENDPOINT serving-topology vars land on every container when
// the ISVC declares a rollout. Returns nil when the ISVC has no rollout
// groups (renderer skips the no-op call).
//
// peerRevision resolves, per rendered pod, the peer revision the pod is
// paired with so OME_<PEER>_REVISION_ENDPOINT names that revision's
// Service; nil emits only the revision-agnostic endpoints.
//
// Exposed for callers (e.g. the omenative ops dispatch shim) that
// construct workload.Deps directly rather than going through the
// RenderWithRevisionForISVC wrapper.
func ISVCRenderHook(isvc *v1beta1.InferenceService, peerRevision coordination.PeerRevisionFunc) workloadtypes.RenderHook {
	return isvcRenderHook(isvc, peerRevision)
}

// isvcRenderHook is the unexported implementation kept as the in-package
// entry point so the existing RenderWithRevisionForISVC wiring keeps
// using the unqualified name.
func isvcRenderHook(isvc *v1beta1.InferenceService, peerRevision coordination.PeerRevisionFunc) workloadtypes.RenderHook {
	// Peer-env applies whenever the ISVC declares a rollout (any groups); skip
	// the hook otherwise. Membership is serving topology (below), not grouping.
	if !coordination.PeerEnvDeclared(isvc) {
		return nil
	}
	return func(pod *corev1.Pod, _ string, _ int32, revisionHash string) {
		// component is recoverable from the pod's component label
		// (constants.OMEComponentLabel, whose value is "component" — NOT
		// "ome.io/component"; the latter never matches the stamped key and
		// silently yields no peers, suppressing injection entirely).
		c := v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel])
		// Peers reflect SERVING topology (the ISVC's declared Components), not
		// rollout grouping — a PD engine needs the decoder's address regardless of
		// whether the rollout groups them together, separately, or via canary.
		peers := coordination.ServingPeers(isvc, c)
		if len(peers) == 0 {
			return
		}
		// Each Component hashes its own template, so the rendered pod's hash
		// never names a peer's per-revision Service; the adapter's resolver
		// maps this pod's revision onto the peer revision it pairs with. With
		// no resolver only the revision-agnostic endpoints are injected
		// rather than dead per-revision DNS.
		var revisionHashFor func(v1beta1.ComponentType) string
		if peerRevision != nil {
			revisionHashFor = func(peer v1beta1.ComponentType) string {
				return peerRevision(peer, revisionHash)
			}
		}
		coordination.InjectPeerEnv(pod, isvc.Name, isvc.Namespace, peers, revisionHashFor)
	}
}

// WouldOverlayConflictForISVC is the (PodSpec, *MigrationOverlay)
// wrapper around workload/ops.WouldOverlayConflictWithNodeAffinity.
// Unused — adapters use the workload form directly; retained for
// compatibility.
func WouldOverlayConflictForISVC(spec *corev1.PodSpec, overlay *workloadtypes.MigrationOverlay) bool {
	return ops.WouldOverlayConflictWithNodeAffinity(spec, overlay)
}
