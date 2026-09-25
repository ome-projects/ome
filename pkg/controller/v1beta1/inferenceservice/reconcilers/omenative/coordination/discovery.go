package coordination

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// PeerEndpointEnv is the set of peer-discovery env vars OMENative
// stamps on a pod for one peer Component:
//
//   - OME_<PEER>_ENDPOINT carries the generic, revision-agnostic
//     Service DNS name. Always emitted; it spans every revision of the
//     peer, so a runtime that reads it may pair across revisions
//     mid-rollout.
//   - OME_<PEER>_REVISION_ENDPOINT carries the per-revision Service
//     DNS name of the peer revision this pod is paired with. Emitted
//     whenever the caller can resolve that revision (see
//     PeerRevisionFunc); a runtime that prefers it only ever reaches
//     peers of its own pairing.
//
// Env vars are read-once at pod startup which matches the LLM-runtime
// pattern.
type PeerEndpointEnv struct {
	// Peer is the peer Component these vars target.
	Peer v1beta1.ComponentType

	// GenericName is the OME_<PEER>_ENDPOINT env var name.
	GenericName string

	// GenericValue is the DNS name pointing at the revision-agnostic
	// per-Component Service (`<isvc>-<peer>`).
	GenericValue string

	// RevisionName is the OME_<PEER>_REVISION_ENDPOINT env var name.
	// Only meaningful when a peer revision hash was supplied.
	RevisionName string

	// RevisionValue is the DNS name pointing at the peer's
	// per-revision Service (`<isvc>-<peer>-rev-<revision-hash>`).
	// Only meaningful when a peer revision hash was supplied.
	RevisionValue string
}

// PeerRevisionFunc resolves the revision hash of the PEER that a pod
// rendered for podRevisionHash pairs with. Each Component hashes its own
// template, so the rendered pod's own hash never names a peer revision;
// the adapter resolves the pairing from the peer's authoritative state
// instead. Returning "" for a peer emits only that peer's generic form.
type PeerRevisionFunc func(peer v1beta1.ComponentType, podRevisionHash string) string

// PeerEnvDeclared reports whether the ISVC's pods carry peer-discovery
// env vars at all: only when the ISVC declares a rollout (any groups).
// Peer membership itself is serving topology (ServingPeers), not
// grouping; the rollout declaration is the opt-in to the whole contract.
func PeerEnvDeclared(isvc *v1beta1.InferenceService) bool {
	return isvc != nil && isvc.Spec.Rollout != nil && len(isvc.Spec.Rollout.Groups) > 0
}

// BuildPeerEndpointEnv computes the env vars OMENative injects into a
// pod for one peer Component. peerRevisionHash, when non-empty, must be
// a revision hash of the PEER — each Component hashes its own
// template, so the rendered pod's own hash never names a peer
// revision. Callers without a peer hash pass "" and get only the
// generic form (InjectPeerEnv skips the revision pair).
//
// isvc + namespace identify the InferenceService; the function does
// not do I/O.
func BuildPeerEndpointEnv(isvcName, namespace string, peer v1beta1.ComponentType, peerRevisionHash string) PeerEndpointEnv {
	upper := strings.ToUpper(string(peer))
	return PeerEndpointEnv{
		Peer:          peer,
		GenericName:   fmt.Sprintf("OME_%s_ENDPOINT", upper),
		GenericValue:  genericPeerDNS(isvcName, peer, namespace),
		RevisionName:  fmt.Sprintf("OME_%s_REVISION_ENDPOINT", upper),
		RevisionValue: revisionPeerDNS(isvcName, peer, peerRevisionHash, namespace),
	}
}

// genericPeerDNS returns the in-cluster DNS for the revision-agnostic
// per-Component Service. The OMENative reconciler emits one of these
// per Component (`<isvc>-<peer>`).
func genericPeerDNS(isvc string, peer v1beta1.ComponentType, namespace string) string {
	return fmt.Sprintf("%s-%s.%s.%s", isvc, peer, namespace, constants.ClusterLocalDomain)
}

// revisionPeerDNS returns the in-cluster DNS for the per-revision peer
// Service. The OMENative coordination layer creates one of these per
// (Component, revisionHash) pair.
func revisionPeerDNS(isvc string, peer v1beta1.ComponentType, revisionHash, namespace string) string {
	// Derive from PerRevisionServiceName so the peer DNS always matches
	// the (bounded) routing Service name coordination actually creates.
	return fmt.Sprintf("%s.%s.%s", PerRevisionServiceName(isvc, peer, revisionHash), namespace, constants.ClusterLocalDomain)
}

// InjectPeerEnv overlays the OME_<PEER>_ENDPOINT and
// OME_<PEER>_REVISION_ENDPOINT env vars onto every container in pod
// for each peer Component the pod is told about. Existing env vars
// with the same Name are replaced — coordination's values are
// authoritative.
//
// If revisionHashFor is nil only the generic env var is emitted. A
// non-nil revisionHashFor must return a revision hash of the PEER it is
// called with (or "" to skip that peer's revision form).
//
// The peers slice should be deduplicated and stable in order;
// callers typically pass coordination.ServingPeers(...).
func InjectPeerEnv(pod *corev1.Pod, isvc, namespace string, peers []v1beta1.ComponentType, revisionHashFor func(v1beta1.ComponentType) string) {
	if pod == nil || len(peers) == 0 {
		return
	}
	envs := make([]corev1.EnvVar, 0, 2*len(peers))
	owned := make(map[string]struct{}, 2*len(peers))
	for _, peer := range peers {
		hash := ""
		if revisionHashFor != nil {
			hash = revisionHashFor(peer)
		}
		peerEnv := BuildPeerEndpointEnv(isvc, namespace, peer, hash)
		envs = append(envs, corev1.EnvVar{Name: peerEnv.GenericName, Value: peerEnv.GenericValue})
		owned[peerEnv.GenericName] = struct{}{}
		if hash != "" {
			envs = append(envs, corev1.EnvVar{Name: peerEnv.RevisionName, Value: peerEnv.RevisionValue})
			owned[peerEnv.RevisionName] = struct{}{}
		}
	}
	for i := range pod.Spec.Containers {
		kept := pod.Spec.Containers[i].Env[:0]
		for _, e := range pod.Spec.Containers[i].Env {
			if _, replace := owned[e.Name]; replace {
				continue
			}
			kept = append(kept, e)
		}
		pod.Spec.Containers[i].Env = append(kept, envs...)
	}
}

// ServingPeers returns the other Components declared on the ISVC (the serving
// topology: router/engine/decoder), excluding c, deduplicated and sorted.
//
// Peer endpoints are a SERVING concern — a PD engine needs the decoder's address
// to serve regardless of how the rollout groups or sequences the Components — so
// peer membership is the ISVC's declared Components, NOT rollout grouping. This
// keeps a PD pair wired together whether they roll in one group, in separate
// ordered groups, or via canary. Returns nil when c has no declared peer.
func ServingPeers(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) []v1beta1.ComponentType {
	if isvc == nil {
		return nil
	}
	declared := make([]v1beta1.ComponentType, 0, 3)
	if isvc.Spec.Router != nil {
		declared = append(declared, v1beta1.RouterComponent)
	}
	if isvc.Spec.Engine != nil {
		declared = append(declared, v1beta1.EngineComponent)
	}
	if isvc.Spec.Decoder != nil {
		declared = append(declared, v1beta1.DecoderComponent)
	}
	peers := make([]v1beta1.ComponentType, 0, len(declared))
	for _, d := range declared {
		if d != c {
			peers = append(peers, d)
		}
	}
	return sortedComponents(peers)
}

// sortedComponents returns a deduplicated, lexicographically-sorted
// copy of in. Used to give env-var injection a stable order so a pod
// re-render doesn't produce noise.
func sortedComponents(in []v1beta1.ComponentType) []v1beta1.ComponentType {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[v1beta1.ComponentType]struct{}, len(in))
	out := make([]v1beta1.ComponentType, 0, len(in))
	for _, c := range in {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
