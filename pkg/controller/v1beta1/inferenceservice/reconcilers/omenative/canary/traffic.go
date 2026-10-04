package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
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

// servingStable resolves the revision a split's remainder is written on, and
// its pairing protocol. The stable revision carries the remainder while it
// has a serving pod. A stable revision with none, whose instances rolled onto
// another revision, hands the remainder to that revision: the one serving the
// most pods off the canary, under no protocol (pairs with anything), so the
// remainder never points at capacity that moved. With nothing serving in its
// place, or no pod view, the stable revision keeps its share: a pod loss is
// the workload's repair, not a traffic decision.
func servingStable(stableHash, stableProtocol, canaryHash string, readyPods map[string]int32) (string, string) {
	if stableHash == "" || stableHash == canaryHash || readyPods == nil || readyPods[stableHash] > 0 {
		return stableHash, stableProtocol
	}
	substitute, most := "", int32(0)
	for hash, count := range readyPods {
		if hash == "" || hash == canaryHash || count <= 0 {
			continue
		}
		if count > most || (count == most && hash < substitute) {
			substitute, most = hash, count
		}
	}
	if substitute == "" {
		return stableHash, stableProtocol
	}
	return substitute, ""
}

// canaryPathServes reports whether every bumped member of the unit has a
// serving pod on its target revision: one member without one breaks the
// canary path for every member. No pod view, or an unbumped member, serves.
func canaryPathServes(in ReconcileInputs) bool {
	if !targetServes(in.PerRevisionPods, in.CanaryRevisionHash, in.StableRevisionHash) {
		return false
	}
	for c, m := range in.Secondaries {
		if !targetServes(in.GroupReadyPerRevisionPods[c], m.CanaryRevisionHash, m.StableRevisionHash) {
			return false
		}
	}
	return true
}

// targetServes reports whether a member's target revision has a serving pod
// in readyPods. No pod view, no target, or a target that is the member's
// stable revision reads as serving.
func targetServes(readyPods map[string]int32, target, stable string) bool {
	return readyPods == nil || target == "" || target == stable || readyPods[target] > 0
}

// servedWeight is the share a member writes on its target: the step weight
// while the canary path serves, nothing while it does not and the stable
// side has a serving pod. A stable side with no pod leaves the weight.
func servedWeight(weight int32, pathServes bool, stable string, readyPods map[string]int32) int32 {
	if pathServes || stable == "" || readyPods[stable] <= 0 {
		return weight
	}
	return 0
}

// memberWeights is a secondary's split: its target at `weight`, its stable
// at the remainder, each under its protocol, on the pods readyPods names;
// an unbumped member or one with no stable serves its target alone.
func memberWeights(m MemberRevisions, weight int32, pathServes bool, readyPods map[string]int32) []coordination.RevisionWeight {
	if m.CanaryRevisionHash == "" {
		return nil
	}
	if m.StableRevisionHash == "" || m.StableRevisionHash == m.CanaryRevisionHash {
		return canaryWeights(m.CanaryRevisionHash, m.CanaryRevisionHash, m.CanaryPairingProtocol, m.CanaryPairingProtocol, weight)
	}
	stable, protocol := servingStable(m.StableRevisionHash, m.StablePairingProtocol, m.CanaryRevisionHash, readyPods)
	return canaryWeights(m.CanaryRevisionHash, stable, m.CanaryPairingProtocol, protocol, servedWeight(weight, pathServes, stable, readyPods))
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

// writeTraffic writes a two-revision split onto the primary's traffic
// targets, named under the Component's replica prefix.
func writeTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, canaryHash, stableHash, canaryProtocol, stableProtocol string, weight int32) {
	prefix := irprojector.RoleReplicaPrefix(isvc, c)
	setTraffic(isvc, c, coordination.BuildTrafficTargets(prefix, c, canaryWeights(canaryHash, stableHash, canaryProtocol, stableProtocol, weight)))
}

// recordTrafficWeight records the step's weight on the canary status: the
// share the step holds, whatever the split carries on the pods it has.
func recordTrafficWeight(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, weight int32) {
	if cs := rollout.CanaryStatusFor(&isvc.Status, c); cs != nil {
		cs.ObservedTrafficWeight = weight
	}
}

// applyTraffic programs the step's external traffic weight on the primary: it
// writes the per-revision weights onto its traffic targets and records the
// applied weight on the canary status.
func applyTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, canaryHash, stableHash, canaryProtocol, stableProtocol string, weight int32) {
	writeTraffic(isvc, c, canaryHash, stableHash, canaryProtocol, stableProtocol, weight)
	recordTrafficWeight(isvc, c, weight)
}

// applyMemberTraffic writes one secondary's targets from its weights. No
// weights leaves the member's traffic as it is.
func applyMemberTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, weights []coordination.RevisionWeight) {
	if len(weights) == 0 {
		return
	}
	setTraffic(isvc, c, coordination.BuildTrafficTargets(irprojector.RoleReplicaPrefix(isvc, c), c, weights))
}

// applyGroupTraffic programs one step weight onto every member of the canary
// unit: the primary's split on its own revision pair and each secondary's on
// its own. The weights are the same; only the revisions they name differ, so
// a routing consumer finds a target for every member at every step. Each
// member's remainder rests on the revision servingStable resolves from that
// member's serving pods, and its target carries the share only while the
// canary path serves; the recorded weight is the step's regardless.
func applyGroupTraffic(in ReconcileInputs, weight int32) {
	pathServes := canaryPathServes(in)
	stable, protocol := servingStable(in.StableRevisionHash, in.StablePairingProtocol, in.CanaryRevisionHash, in.PerRevisionPods)
	writeTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, stable, in.CanaryPairingProtocol, protocol,
		servedWeight(weight, pathServes, stable, in.PerRevisionPods))
	recordTrafficWeight(in.ISVC, in.Component, weight)
	for c, m := range in.Secondaries {
		applyMemberTraffic(in.ISVC, c, memberWeights(m, weight, pathServes, in.GroupReadyPerRevisionPods[c]))
	}
}

// heldTraffic rewrites the split a unit already holds at weight against the
// pass's pods and reports whether the primary's was written; a member with
// no split written, or no stable distinct from its target, keeps what it has.
func heldTraffic(in ReconcileInputs, weight int32) bool {
	pathServes := canaryPathServes(in)
	written := false
	if len(in.ISVC.Status.Components[in.Component].Traffic) > 0 &&
		in.StableRevisionHash != "" && in.StableRevisionHash != in.CanaryRevisionHash {
		stable, protocol := servingStable(in.StableRevisionHash, in.StablePairingProtocol, in.CanaryRevisionHash, in.PerRevisionPods)
		writeTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, stable, in.CanaryPairingProtocol, protocol,
			servedWeight(weight, pathServes, stable, in.PerRevisionPods))
		written = true
	}
	for c, m := range in.Secondaries {
		if len(in.ISVC.Status.Components[c].Traffic) == 0 ||
			m.StableRevisionHash == "" || m.StableRevisionHash == m.CanaryRevisionHash {
			continue
		}
		applyMemberTraffic(in.ISVC, c, memberWeights(m, weight, pathServes, in.GroupReadyPerRevisionPods[c]))
	}
	return written
}

// holdGroupTraffic rewrites the split a unit holds under its recorded weight
// against the pass's pods: the share rests on the stable side while no
// canary pod serves and returns on the pass one does. The record stands.
func holdGroupTraffic(in ReconcileInputs) {
	if cs := rollout.CanaryStatusFor(&in.ISVC.Status, in.Component); cs != nil {
		heldTraffic(in, cs.ObservedTrafficWeight)
	}
}

// returnTrafficToStable writes a split left behind back onto each member's
// stable revision, as servingStable resolves it, with the target at nothing,
// and records nothing; a member with no split written keeps what it has.
func returnTrafficToStable(in ReconcileInputs) {
	if heldTraffic(in, 0) {
		recordTrafficWeight(in.ISVC, in.Component, 0)
	}
}

// applyCutoverTraffic writes the cutover on every member: its target alone.
// The sentinel releases the held stable instance, so no capacity is left to
// rest a fallback on; a canary pod lost from here is the workload's repair.
func applyCutoverTraffic(in ReconcileInputs) {
	applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, in.StableRevisionHash, in.CanaryPairingProtocol, in.StablePairingProtocol, 100)
	for c, m := range in.Secondaries {
		applyMemberTraffic(in.ISVC, c, memberWeights(m, 100, true, nil))
	}
}

// applyGroupStableTraffic returns every secondary to its stable revision
// alongside the primary's rollback write. A member whose stable revision is
// observed with no ready pod keeps its programmed split until one serves.
func applyGroupStableTraffic(in ReconcileInputs) {
	for c, m := range in.Secondaries {
		if ready, observed := in.GroupReadyPerRevisionPods[c]; observed && ready[m.StableRevisionHash] == 0 {
			continue
		}
		applyMemberTraffic(in.ISVC, c, memberStableWeights(m))
	}
}
