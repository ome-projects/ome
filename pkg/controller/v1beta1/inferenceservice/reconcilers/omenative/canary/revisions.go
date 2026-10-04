package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// The rolled-out revision fields of a canary-owned Component are the
// executor's to publish: coordination's pod-proportional writer leaves every
// member of a canary group alone, so the canary decides when a member's
// target has reached Ready and when a revision has come to own the member's
// traffic, and writes the fields through the boundary coordination uses for
// the Components it owns. The revision pair is the one the traffic writer
// programs, so the fields and the traffic targets name the same revisions.

// publishReadyRevisions records, for every member whose target revision has
// Ready capacity, that revision as the member's LatestReadyRevision. It runs
// ahead of the capacity gate, so the field leads LatestRolledoutRevision
// while the ladder is in flight.
func publishReadyRevisions(in ReconcileInputs) {
	if readyCanaryCapacity(in) > 0 {
		setLatestReady(in.ISVC, in.Component, in.CanaryRevisionHash)
	}
	for c, m := range in.Secondaries {
		if m.ReadyCanaryCapacity > 0 {
			setLatestReady(in.ISVC, c, m.CanaryRevisionHash)
		}
	}
}

// recordPromotedRevisions publishes a completed promotion on every member:
// its target now fully owns its traffic, and the stable revision the ladder
// shifted traffic from is the one it superseded. A member whose target is
// its stable revision was not bumped and records that revision without
// demoting it.
func recordPromotedRevisions(in ReconcileInputs, stableHash string) {
	recordRolledOut(in.ISVC, in.Component, in.CanaryRevisionHash, stableHash)
	for c, m := range in.Secondaries {
		recordRolledOut(in.ISVC, c, m.CanaryRevisionHash, m.StableRevisionHash)
	}
}

// recordRolledBackRevisions publishes a completed revert on every member
// with a stable revision: that revision owns the member's traffic again. A
// member already recorded on it changes nothing, so the rejected revision,
// which never completed, never enters PreviousRolledoutRevision.
func recordRolledBackRevisions(in ReconcileInputs, stableHash string) {
	recordRolledOut(in.ISVC, in.Component, stableHash, "")
	for c, m := range in.Secondaries {
		recordRolledOut(in.ISVC, c, m.StableRevisionHash, "")
	}
}

// setLatestReady writes one member's LatestReadyRevision, named under the
// member's replica prefix; no hash writes nothing.
func setLatestReady(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, hash string) {
	if hash == "" {
		return
	}
	cs := componentStatus(isvc, c)
	coordination.SetLatestReadyRevision(&cs, irprojector.RoleReplicaPrefix(isvc, c), c, hash)
	isvc.Status.Components[c] = cs
}

// recordRolledOut writes one member's LatestRolledoutRevision, named under
// the member's replica prefix, demoting the superseded revision, and brings
// LatestReadyRevision level with it: a revision that owns the traffic is one
// whose pods reached Ready. No hash writes nothing.
func recordRolledOut(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, hash, supersededHash string) {
	if hash == "" {
		return
	}
	prefix := irprojector.RoleReplicaPrefix(isvc, c)
	cs := componentStatus(isvc, c)
	coordination.RecordRolledOutRevision(&cs, prefix, c, hash, supersededHash)
	coordination.SetLatestReadyRevision(&cs, prefix, c, hash)
	isvc.Status.Components[c] = cs
}

// componentStatus returns one Component's status entry, creating the map as
// needed. (Map values are structs, so writers round-trip through the copy.)
func componentStatus(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) v1beta1.ComponentStatusSpec {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	return isvc.Status.Components[c]
}
