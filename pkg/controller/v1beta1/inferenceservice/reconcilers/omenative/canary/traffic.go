package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
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

// applyTraffic programs the step's external traffic weight: it writes the
// per-revision weights onto Status.Components.<c>.Traffic (the entries the
// HTTPRoute weighted-backendRef consumer reads) and records the applied weight
// on the canary status.
func applyTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, canaryHash, stableHash, canaryProtocol, stableProtocol string, weight int32) {
	targets := coordination.BuildTrafficTargets(isvc.Name, c, canaryWeights(canaryHash, stableHash, canaryProtocol, stableProtocol, weight))
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	cs.Traffic = targets
	isvc.Status.Components[c] = cs
	if isvc.Status.Canary != nil {
		isvc.Status.Canary.ObservedTrafficWeight = weight
	}
}
