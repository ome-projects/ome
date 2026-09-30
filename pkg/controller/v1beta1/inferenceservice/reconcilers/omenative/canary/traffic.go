package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// canaryWeights builds the two-revision external weight split for a step: the
// canary revision receives `weight` percent, the stable revision the remainder.
// The canary entry is marked LatestRevision. (coordination.BuildTrafficTargets
// drops any entry with Percent<=0, so a 0%/100% step naturally collapses to a
// single target.) Each entry carries its revision's pairing protocol so the
// routing consumer can pair engine/decoder targets by equal values.
func canaryWeights(canaryHash, stableHash, canaryProtocol, stableProtocol string, weight int32) []coordination.RevisionWeight {
	if canaryHash != "" && canaryHash == stableHash {
		return []coordination.RevisionWeight{{
			RevisionHash: canaryHash, Percent: 100, Tag: "canary", LatestRevision: true, PairingProtocol: canaryProtocol,
		}}
	}
	return []coordination.RevisionWeight{
		{RevisionHash: canaryHash, Percent: weight, Tag: "canary", LatestRevision: true, PairingProtocol: canaryProtocol},
		{RevisionHash: stableHash, Percent: 100 - weight, Tag: "stable", PairingProtocol: stableProtocol},
	}
}

// memberWeights is a secondary's split at a step: its target revision at
// `weight` percent and its stable revision at the remainder, each under the
// protocol it was minted with. A member whose target is its stable revision
// was not bumped, and a member with no stable revision has nothing to shift
// traffic from; both serve their target alone. No target yields no weights.
func memberWeights(m MemberRevisions, weight int32) []coordination.RevisionWeight {
	if m.CanaryRevisionHash == "" {
		return nil
	}
	if m.StableRevisionHash == "" || m.StableRevisionHash == m.CanaryRevisionHash {
		return canaryWeights(m.CanaryRevisionHash, m.CanaryRevisionHash, m.CanaryPairingProtocol, m.CanaryPairingProtocol, weight)
	}
	return canaryWeights(m.CanaryRevisionHash, m.StableRevisionHash, m.CanaryPairingProtocol, m.StablePairingProtocol, weight)
}

// memberStableWeights is a rolled-back secondary's traffic: its stable
// revision alone, as the primary's. A member with no stable revision is sent
// no revert, so it yields no weights and keeps the targets it has.
func memberStableWeights(m MemberRevisions) []coordination.RevisionWeight {
	if m.StableRevisionHash == "" {
		return nil
	}
	return []coordination.RevisionWeight{{
		RevisionHash: m.StableRevisionHash, Percent: 100, Tag: "stable", PairingProtocol: m.StablePairingProtocol,
	}}
}

// setTraffic writes one Component's targets onto Status.Components.<c>.Traffic,
// the entries the HTTPRoute weighted-backendRef consumer reads.
func setTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, targets []v1beta1.ComponentTrafficTarget) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	cs.Traffic = targets
	isvc.Status.Components[c] = cs
}

// applyTraffic programs the step's external traffic weight on the primary: it
// writes the per-revision weights onto its traffic targets and records the
// applied weight on the canary status.
func applyTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, canaryHash, stableHash, canaryProtocol, stableProtocol string, weight int32) {
	setTraffic(isvc, c, coordination.BuildTrafficTargets(isvc.Name, c, canaryWeights(canaryHash, stableHash, canaryProtocol, stableProtocol, weight)))
	if cs := rollout.CanaryStatusFor(&isvc.Status, c); cs != nil {
		cs.ObservedTrafficWeight = weight
	}
}

// applyMemberTraffic writes one secondary's targets from its weights. No
// weights leaves the member's traffic as it is.
func applyMemberTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, weights []coordination.RevisionWeight) {
	if len(weights) == 0 {
		return
	}
	setTraffic(isvc, c, coordination.BuildTrafficTargets(isvc.Name, c, weights))
}

// applyGroupTraffic programs one step weight onto every member of the canary
// unit: the primary's split on its own revision pair and each secondary's on
// its own. The weights are the same; only the revisions they name differ, so
// a routing consumer finds a target for every member at every step.
func applyGroupTraffic(in ReconcileInputs, weight int32) {
	applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, in.StableRevisionHash,
		in.CanaryPairingProtocol, in.StablePairingProtocol, weight)
	for c, m := range in.Secondaries {
		applyMemberTraffic(in.ISVC, c, memberWeights(m, weight))
	}
}

// applyGroupStableTraffic returns every secondary to its stable revision
// alongside the primary's rollback write.
func applyGroupStableTraffic(in ReconcileInputs) {
	for c, m := range in.Secondaries {
		applyMemberTraffic(in.ISVC, c, memberStableWeights(m))
	}
}
