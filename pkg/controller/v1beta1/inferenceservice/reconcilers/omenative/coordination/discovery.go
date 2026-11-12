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
	// per-Component Service (`<isvc>-<peer>`), which the InferenceService
	// owns under its own name whichever replica serves the peer.
	GenericValue string

	// RevisionName is the OME_<PEER>_REVISION_ENDPOINT env var name.
	// Only meaningful when a peer revision hash was supplied.
	RevisionName string

	// RevisionValue is the DNS name pointing at the peer's per-revision
	// Service (`<peer prefix>-<peer>-rev-<revision-hash>`), named with the
	// peer replica's prefix like every per-revision object of the peer.
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
// pod for one peer Component. isvcName names the InferenceService, whose
// stable per-Component Service the generic form resolves; peerPrefix is
// the peer replica's name prefix (prefixFor), which its per-revision
// Services are named with. clusterDomain is the cluster's DNS domain from
// the operator configuration (ingress.clusterDomain); empty renders the
// namespaced short names. peerRevisionHash, when non-empty, must be a
// revision hash of the PEER — each Component hashes its own template, so
// the rendered pod's own hash never names a peer revision. Callers without
// a peer hash pass "" and get only the generic form (InjectPeerEnv skips
// the revision pair). The function does not do I/O.
func BuildPeerEndpointEnv(isvcName, peerPrefix, namespace, clusterDomain string, peer v1beta1.ComponentType, peerRevisionHash string) PeerEndpointEnv {
	upper := strings.ToUpper(string(peer))
	return PeerEndpointEnv{
		Peer:          peer,
		GenericName:   fmt.Sprintf("OME_%s_ENDPOINT", upper),
		GenericValue:  genericPeerDNS(isvcName, peer, namespace, clusterDomain),
		RevisionName:  fmt.Sprintf("OME_%s_REVISION_ENDPOINT", upper),
		RevisionValue: revisionPeerDNS(peerPrefix, peer, peerRevisionHash, namespace, clusterDomain),
	}
}

// genericPeerDNS returns the in-cluster DNS for the revision-agnostic
// per-Component Service. The OMENative reconciler emits one of these
// per Component (`<isvc>-<peer>`).
func genericPeerDNS(isvc string, peer v1beta1.ComponentType, namespace, clusterDomain string) string {
	return constants.ServiceHostname(fmt.Sprintf("%s-%s", isvc, peer), namespace, clusterDomain)
}

// revisionPeerDNS returns the in-cluster DNS for the per-revision peer
// Service. The OMENative coordination layer creates one of these per
// (Component, revisionHash) pair under the peer replica's prefix.
func revisionPeerDNS(peerPrefix string, peer v1beta1.ComponentType, revisionHash, namespace, clusterDomain string) string {
	// Derive from PerRevisionServiceName so the peer DNS always matches
	// the (bounded) routing Service name coordination actually creates.
	return constants.ServiceHostname(PerRevisionServiceName(peerPrefix, peer, revisionHash), namespace, clusterDomain)
}

// InjectPeerEnv overlays the OME_<PEER>_ENDPOINT and
// OME_<PEER>_REVISION_ENDPOINT env vars onto every container in pod
// for each peer Component of isvc the pod is told about. Existing env
// vars with the same Name are replaced — coordination's values are
// authoritative. Each peer's revision endpoint is named with that peer
// replica's own prefix, so a referenced peer resolves as well as a
// projected one. clusterDomain is the cluster's DNS domain from the
// operator configuration; empty renders the namespaced short names.
//
// If revisionHashFor is nil only the generic env var is emitted. A
// non-nil revisionHashFor must return a revision hash of the PEER it is
// called with (or "" to skip that peer's revision form).
//
// The peers slice should be deduplicated and stable in order;
// callers typically pass coordination.ServingPeers(...).
func InjectPeerEnv(pod *corev1.Pod, isvc *v1beta1.InferenceService, clusterDomain string, peers []v1beta1.ComponentType, revisionHashFor func(v1beta1.ComponentType) string) {
	if pod == nil || isvc == nil || len(peers) == 0 {
		return
	}
	envs := make([]corev1.EnvVar, 0, 2*len(peers))
	owned := make(map[string]struct{}, 2*len(peers))
	for _, peer := range peers {
		hash := ""
		if revisionHashFor != nil {
			hash = revisionHashFor(peer)
		}
		peerEnv := BuildPeerEndpointEnv(isvc.Name, prefixFor(isvc, peer), isvc.Namespace, clusterDomain, peer, hash)
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
// topology: every declared role), excluding c, deduplicated and sorted.
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
	declared := v1beta1.ServiceRoles(&isvc.Spec).Declared()
	peers := make([]v1beta1.ComponentType, 0, len(declared))
	for _, d := range declared {
		if d.Type != c {
			peers = append(peers, d.Type)
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
