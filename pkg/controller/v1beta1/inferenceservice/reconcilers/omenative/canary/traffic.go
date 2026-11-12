package canary

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
	substitute := mostServedRevision(readyPods, canaryHash)
	if substitute == "" {
		return stableHash, stableProtocol
	}
	return substitute, ""
}

// mostServedRevision is the revision with the most serving pods in
// readyPods, other than the one excluded; ties go to the smaller hash, and
// nothing serving reads as "".
func mostServedRevision(readyPods map[string]int32, exclude string) string {
	most, count := "", int32(0)
	for hash, n := range readyPods {
		if hash == "" || hash == exclude || n <= 0 {
			continue
		}
		if n > count || (n == count && hash < most) {
			most, count = hash, n
		}
	}
	return most
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

// sidesServing is how one write reads the unit's two sides, the same for
// every member. canaryPath is true when every retargeted member has a
// serving pod on its target; stableGone is true when some member's stable
// revision, or the revision standing in for it, has no serving pod. The
// unit has one canary path and one stable side, so a prefill/decode unit
// moves together on either and no request is split between a version 2
// engine and a version 1 decoder.
type sidesServing struct {
	canaryPath bool
	stableGone bool
}

// bothSidesServe is the reading of a unit whose canary path and stable side
// both have a serving pod on every member.
var bothSidesServe = sidesServing{canaryPath: true}

// sidesServingOf reads the unit's two sides from every member's pods.
func sidesServingOf(in ReconcileInputs) sidesServing {
	return sidesServing{canaryPath: canaryPathServes(in), stableGone: !stableSideServes(in)}
}

// stableSideServes reports whether every member of the unit has a serving
// pod on the revision its remainder rests on: its stable revision, or the
// revision servingStable puts in its place.
func stableSideServes(in ReconcileInputs) bool {
	if !stableServes(in.PerRevisionPods, in.StableRevisionHash, in.StablePairingProtocol, in.CanaryRevisionHash) {
		return false
	}
	for c, m := range in.Secondaries {
		if !stableServes(in.GroupReadyPerRevisionPods[c], m.StableRevisionHash, m.StablePairingProtocol, m.CanaryRevisionHash) {
			return false
		}
	}
	return true
}

// stableServes reports whether a member's remainder has a serving pod under
// it. No pod view, no stable revision, or a stable revision that is the
// target reads as serving: such a member has no remainder to move.
func stableServes(readyPods map[string]int32, stable, protocol, canary string) bool {
	if readyPods == nil || stable == "" || stable == canary {
		return true
	}
	resolved, _ := servingStable(stable, protocol, canary, readyPods)
	return readyPods[resolved] > 0
}

// servedWeight is the share a member writes on its target. A revision with
// no serving pod carries no weight and the serving side carries the rest,
// read for the unit first: everything while the canary path serves on every
// member and some member's stable side has no pod, so both members of a
// prefill/decode unit move together. Otherwise the member's own pods decide:
// the step weight while both of its sides serve; nothing while the canary
// path is broken and its stable side serves; everything while its stable
// side has no pod and its own target serves. With no pod view, no stable
// side, or nothing serving on either side the step weight stands: a loss
// with nothing to move onto is the workload's repair, not a traffic
// decision.
func servedWeight(weight int32, unit sidesServing, target, stable string, readyPods map[string]int32) int32 {
	if unit.canaryPath && unit.stableGone {
		return 100
	}
	if stable == "" || readyPods == nil {
		return weight
	}
	stableServes, targetServes := readyPods[stable] > 0, readyPods[target] > 0
	switch {
	case !unit.canaryPath && stableServes:
		return 0
	case !stableServes && targetServes:
		return 100
	}
	return weight
}

// memberWeights is a secondary's split: its target at `weight`, its stable
// at the remainder, each under its protocol, on the pods readyPods names,
// under the unit's reading of its two sides; an unbumped member or one with
// no stable serves its target alone.
func memberWeights(m MemberRevisions, weight int32, unit sidesServing, readyPods map[string]int32) []coordination.RevisionWeight {
	if m.CanaryRevisionHash == "" {
		return nil
	}
	if m.StableRevisionHash == "" || m.StableRevisionHash == m.CanaryRevisionHash {
		return canaryWeights(m.CanaryRevisionHash, m.CanaryRevisionHash, m.CanaryPairingProtocol, m.CanaryPairingProtocol, weight)
	}
	stable, protocol := servingStable(m.StableRevisionHash, m.StablePairingProtocol, m.CanaryRevisionHash, readyPods)
	return canaryWeights(m.CanaryRevisionHash, stable, m.CanaryPairingProtocol, protocol, servedWeight(weight, unit, m.CanaryRevisionHash, stable, readyPods))
}

// stableAlone is a member's traffic on its stable revision alone: a
// rolled-back member's, and a stopped member's once a stable pod serves
// again. A member with no stable revision is sent no revert, so it yields
// no weights and keeps the targets it has.
func stableAlone(side memberSide) []coordination.RevisionWeight {
	if side.stable == "" {
		return nil
	}
	return []coordination.RevisionWeight{{
		RevisionHash: side.stable, Percent: 100, Tag: "stable", PairingProtocol: side.stableProtocol,
	}}
}

// memberSide is one member of the unit as the traffic writers and their
// reporter read it: its target and stable revisions with their pairing
// protocols, and its serving pods per revision.
type memberSide struct {
	component      v1beta1.ComponentType
	target         string
	targetProtocol string
	stable         string
	stableProtocol string
	readyPods      map[string]int32
}

// unitSides lists the unit's members, the primary first with the target and
// stable revisions given (its canary pair at a step, the rejected revision
// and the resolved stable under a revert), then the secondaries by name.
func unitSides(in ReconcileInputs, primaryTarget, primaryStable string) []memberSide {
	primary := memberSide{component: in.Component, target: primaryTarget, stable: primaryStable, readyPods: in.PerRevisionPods}
	if primaryTarget == in.CanaryRevisionHash {
		primary.targetProtocol = in.CanaryPairingProtocol
	}
	if primaryStable == in.StableRevisionHash {
		primary.stableProtocol = in.StablePairingProtocol
	}
	sides := []memberSide{primary}
	members := make([]v1beta1.ComponentType, 0, len(in.Secondaries))
	for c := range in.Secondaries {
		members = append(members, c)
	}
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	for _, c := range members {
		m := in.Secondaries[c]
		sides = append(sides, memberSide{
			component: c, target: m.CanaryRevisionHash, targetProtocol: m.CanaryPairingProtocol,
			stable: m.StableRevisionHash, stableProtocol: m.StablePairingProtocol, readyPods: in.GroupReadyPerRevisionPods[c],
		})
	}
	return sides
}

// stableShare is one member's traffic as a write finds or leaves it: the
// percent on its stable revision, and whether the whole split rests on its
// target alone, which is how a share moved off a stable revision with no
// serving pod reads.
type stableShare struct {
	onStable      int32
	onTargetAlone bool
}

func stableShareOf(isvc *v1beta1.InferenceService, side memberSide) stableShare {
	prefix := irprojector.RoleReplicaPrefix(isvc, side.component)
	targets := isvc.Status.Components[side.component].Traffic
	share := stableShare{}
	for _, t := range targets {
		if t.RevisionName == coordination.PerRevisionServiceName(prefix, side.component, side.stable) {
			share.onStable = t.Percent
		}
	}
	share.onTargetAlone = side.target != "" && side.target != side.stable && len(targets) == 1 &&
		targets[0].Percent == 100 && targets[0].RevisionName == coordination.PerRevisionServiceName(prefix, side.component, side.target)
	return share
}

// stableShares reads every member's stableShare before a write.
func stableShares(isvc *v1beta1.InferenceService, sides []memberSide) map[v1beta1.ComponentType]stableShare {
	out := make(map[v1beta1.ComponentType]stableShare, len(sides))
	for _, side := range sides {
		out[side.component] = stableShareOf(isvc, side)
	}
	return out
}

// namesAlone reports whether a member's traffic is one target at 100
// percent on the revision.
func namesAlone(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, hash string) bool {
	targets := isvc.Status.Components[c].Traffic
	return len(targets) == 1 && targets[0].Percent == 100 &&
		targets[0].RevisionName == coordination.PerRevisionServiceName(irprojector.RoleReplicaPrefix(isvc, c), c, hash)
}

// carrier is the revision a member's traffic rests on besides its stable
// revision: the target with the largest share, and how many of the
// member's pods serve it.
func carrier(isvc *v1beta1.InferenceService, side memberSide) (string, int32) {
	hash, most := "", int32(0)
	for _, t := range isvc.Status.Components[side.component].Traffic {
		if h := query.RevisionFromName(t.RevisionName).Hash(); h != side.stable && t.Percent > most {
			hash, most = h, t.Percent
		}
	}
	return hash, side.readyPods[hash]
}

func podsServing(n int32) string {
	if n == 1 {
		return "1 pod serving"
	}
	return fmt.Sprintf("%d pods serving", n)
}

// restOnServingRevision writes one member's traffic onto the revision that
// serves the most of its pods, its target included, while its stable
// revision has no serving pod. A member with nothing serving at all keeps
// what it has.
func restOnServingRevision(isvc *v1beta1.InferenceService, side memberSide) {
	restOn(isvc, side, mostServedRevision(side.readyPods, side.stable))
}

// restOn writes one member's traffic onto revision alone. A member with no
// traffic written keeps none, and one whose traffic already names the
// revision alone keeps it.
func restOn(isvc *v1beta1.InferenceService, side memberSide, revision string) {
	if revision == "" || len(isvc.Status.Components[side.component].Traffic) == 0 || namesAlone(isvc, side.component, revision) {
		return
	}
	weight := coordination.RevisionWeight{RevisionHash: revision, Percent: 100, Tag: "stable"}
	if revision == side.target {
		weight.Tag, weight.LatestRevision, weight.PairingProtocol = "canary", true, side.targetProtocol
	}
	applyMemberTraffic(isvc, side.component, []coordination.RevisionWeight{weight})
}

// recordedTrafficWeight is the step weight the primary's record carries, 0
// with no record.
func recordedTrafficWeight(in ReconcileInputs) int32 {
	if cs := rollout.CanaryStatusFor(&in.ISVC.Status, in.Component); cs != nil {
		return cs.ObservedTrafficWeight
	}
	return 0
}

// stepContext names the step a traffic event speaks of.
func stepContext(in ReconcileInputs) string {
	if cs := rollout.CanaryStatusFor(&in.ISVC.Status, in.Component); cs != nil {
		return fmt.Sprintf("step %d", cs.CurrentStep)
	}
	return "step 0"
}

// trafficWrite is one write of the unit's traffic as its reporter reads it:
// the step the events speak of, the members with their pods, each member's
// share as the write found it, whether the write meant to leave a share on
// the stable side (a step below 100 percent, a held or stopped split, a
// revert; the final step's own 100 percent write raises nothing), the
// record's weight before the write (a split a cutover left on the target
// alone, under a record at 100, is not read as a moved share), and whether
// the write read the unit's stable side as one, so a member may have
// followed another's loss and a return is the unit's.
type trafficWrite struct {
	context       string
	sides         []memberSide
	before        map[v1beta1.ComponentType]stableShare
	remainder     bool
	recorded      int32
	oneStableSide bool
}

// shareMoves sorts the members of one write by what it did to their share
// on the stable revision, read from the traffic before and after it: lost,
// the share left a stable revision with no serving pod; followed, the share
// left a stable revision that serves, because the unit has one stable side
// and another member's has no serving pod; returned, the split rested on
// the target alone and a share is back on the stable revision.
type shareMoves struct {
	lost, followed, returned []memberSide
}

// takesPartInSplit reports whether a member's traffic is a split between a
// target and a stable revision read on its own pods.
func takesPartInSplit(side memberSide) bool {
	return side.target != "" && side.stable != "" && side.stable != side.target && side.readyPods != nil
}

// readShareMoves sorts the members of w by the move the write made, read
// from each member's share before the write and its traffic after it.
func readShareMoves(isvc *v1beta1.InferenceService, w trafficWrite) shareMoves {
	var moves shareMoves
	for _, side := range w.sides {
		if !takesPartInSplit(side) {
			continue
		}
		was, now := w.before[side.component], stableShareOf(isvc, side)
		switch {
		case was.onStable > 0 && now.onStable == 0 && side.readyPods[side.stable] == 0:
			moves.lost = append(moves.lost, side)
		case was.onStable > 0 && now.onStable == 0 && now.onTargetAlone:
			moves.followed = append(moves.followed, side)
		case was.onTargetAlone && now.onStable > 0 && w.recorded < 100:
			moves.returned = append(moves.returned, side)
		}
	}
	return moves
}

// unservedStables names each member whose stable revision has no serving
// pod, in the order of sides.
func unservedStables(sides []memberSide) string {
	var parts []string
	for _, side := range sides {
		if takesPartInSplit(side) && side.readyPods[side.stable] == 0 {
			parts = append(parts, fmt.Sprintf("%s stable revision %s", side.component, side.stable))
		}
	}
	return strings.Join(parts, " and ")
}

// offStableClauses says, for each member whose share left its stable
// revision, where the share went: a lost member's by its own loss, a
// follower's with the unit.
func offStableClauses(isvc *v1beta1.InferenceService, w trafficWrite, moves shareMoves) []string {
	var clauses []string
	for _, side := range moves.lost {
		revision, pods := carrier(isvc, side)
		clauses = append(clauses, fmt.Sprintf("no pod serves %s stable revision %s, so its %d%% share of the traffic goes to %s revision %s (%s)",
			side.component, side.stable, w.before[side.component].onStable, side.component, revision, podsServing(pods)))
	}
	if len(moves.followed) == 0 {
		return clauses
	}
	var followed []string
	for _, side := range moves.followed {
		followed = append(followed, fmt.Sprintf("the %s's %d%% share on stable revision %s goes to %s revision %s (%s)",
			side.component, w.before[side.component].onStable, side.stable, side.component, side.target, podsServing(side.readyPods[side.target])))
	}
	if len(moves.lost) > 0 {
		return append(clauses, "the unit has one stable side, so "+strings.Join(followed, " and ")+" with it")
	}
	return append(clauses, fmt.Sprintf("the unit has one stable side and no pod serves %s, so %s", unservedStables(w.sides), strings.Join(followed, " and ")))
}

// backOnStableMessage says whose share is back on its stable revision. A
// write that reads the unit's stable side as one cannot tell a member whose
// own pod came back from one that followed, so with more than one member
// in the split it speaks for the unit.
func backOnStableMessage(isvc *v1beta1.InferenceService, w trafficWrite, returned []memberSide) string {
	var parts []string
	if !w.oneStableSide || splitMembers(w.sides) < 2 {
		for _, side := range returned {
			parts = append(parts, fmt.Sprintf("a pod serves %s stable revision %s again and its %d%% share of the traffic is back on it",
				side.component, side.stable, stableShareOf(isvc, side).onStable))
		}
		return strings.Join(parts, "; ")
	}
	for _, side := range returned {
		parts = append(parts, fmt.Sprintf("the %d%% share of the traffic is back on %s stable revision %s",
			stableShareOf(isvc, side).onStable, side.component, side.stable))
	}
	return "a pod serves the stable revision on every member again: " + strings.Join(parts, "; ")
}

// splitMembers counts the members whose traffic is a split on its own pods.
func splitMembers(sides []memberSide) int {
	n := 0
	for _, side := range sides {
		if takesPartInSplit(side) {
			n++
		}
	}
	return n
}

// reportTrafficOffStable raises the two events of a stable side with no
// serving pod around one write. A member whose share this write took off
// its stable revision is named in one Warning: by its own loss, while no
// pod of the revision serves, or with the unit, when the write reads the
// unit's stable side as one and another member's has no serving pod. A
// member whose split rested on its target alone and now carries a share on
// a serving stable pod again is named in one Normal.
func reportTrafficOffStable(in ReconcileInputs, w trafficWrite) {
	if !w.remainder || in.Recorder == nil {
		return
	}
	moves := readShareMoves(in.ISVC, w)
	if clauses := offStableClauses(in.ISVC, w, moves); len(clauses) > 0 {
		back := "the traffic returns to a stable revision when a pod of it serves again"
		if len(moves.followed) > 0 {
			back = "the traffic returns to the stable side when a pod of it serves on every member again"
		}
		emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryTrafficOffStable,
			"%s canary %s: %s; %s", in.Component, w.context, strings.Join(clauses, "; "), back)
	}
	if len(moves.returned) > 0 {
		emit(in.Recorder, in.ISVC, corev1.EventTypeNormal, EventReasonCanaryTrafficBackOnStable,
			"%s canary %s: %s", in.Component, w.context, backOnStableMessage(in.ISVC, w, moves.returned))
	}
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
// member's serving pods, and the unit's two sides decide the share its
// target carries; the recorded weight is the step's regardless.
func applyGroupTraffic(in ReconcileInputs, weight int32) {
	sides := unitSides(in, in.CanaryRevisionHash, in.StableRevisionHash)
	write := trafficWrite{context: stepContext(in), sides: sides, before: stableShares(in.ISVC, sides),
		remainder: weight < 100, recorded: recordedTrafficWeight(in), oneStableSide: true}
	unit := sidesServingOf(in)
	stable, protocol := servingStable(in.StableRevisionHash, in.StablePairingProtocol, in.CanaryRevisionHash, in.PerRevisionPods)
	writeTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, stable, in.CanaryPairingProtocol, protocol,
		servedWeight(weight, unit, in.CanaryRevisionHash, stable, in.PerRevisionPods))
	recordTrafficWeight(in.ISVC, in.Component, weight)
	for c, m := range in.Secondaries {
		applyMemberTraffic(in.ISVC, c, memberWeights(m, weight, unit, in.GroupReadyPerRevisionPods[c]))
	}
	reportTrafficOffStable(in, write)
}

// heldTraffic rewrites the split a unit already holds at weight against the
// pass's pods, under the unit's two sides, and reports whether the primary's
// was written; a member with no split written, or no stable distinct from
// its target, keeps what it has.
func heldTraffic(in ReconcileInputs, weight int32) bool {
	sides := unitSides(in, in.CanaryRevisionHash, in.StableRevisionHash)
	write := trafficWrite{context: stepContext(in), sides: sides, before: stableShares(in.ISVC, sides),
		remainder: weight < 100, recorded: recordedTrafficWeight(in), oneStableSide: true}
	unit := sidesServingOf(in)
	written := false
	if len(in.ISVC.Status.Components[in.Component].Traffic) > 0 &&
		in.StableRevisionHash != "" && in.StableRevisionHash != in.CanaryRevisionHash {
		stable, protocol := servingStable(in.StableRevisionHash, in.StablePairingProtocol, in.CanaryRevisionHash, in.PerRevisionPods)
		writeTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, stable, in.CanaryPairingProtocol, protocol,
			servedWeight(weight, unit, in.CanaryRevisionHash, stable, in.PerRevisionPods))
		written = true
	}
	for c, m := range in.Secondaries {
		if len(in.ISVC.Status.Components[c].Traffic) == 0 ||
			m.StableRevisionHash == "" || m.StableRevisionHash == m.CanaryRevisionHash {
			continue
		}
		applyMemberTraffic(in.ISVC, c, memberWeights(m, weight, unit, in.GroupReadyPerRevisionPods[c]))
	}
	reportTrafficOffStable(in, write)
	return written
}

// holdGroupTraffic rewrites the split a unit holds under its recorded weight
// against the pass's pods: the share rests on the stable side while no
// canary pod serves, on the canary side while some member's stable side has
// none, and returns on the pass the missing side serves again. The record
// stands.
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

// settleTrafficOnTarget writes each member's split onto the revision the
// run names for it where a pod of that revision serves, and rests it on the
// revision that serves the most of its pods otherwise; a member with no
// split written keeps none, and one already naming the revision alone keeps
// it. It is the write of a unit whose roll the operator took back: the
// Instances roll back to the revision under the run, and the traffic
// follows the pods onto it.
func settleTrafficOnTarget(in ReconcileInputs) {
	for _, side := range unitSides(in, in.CanaryRevisionHash, in.StableRevisionHash) {
		if side.target == "" {
			continue
		}
		if side.readyPods == nil || side.readyPods[side.target] > 0 {
			restOn(in.ISVC, side, side.target)
			continue
		}
		restOnServingRevision(in.ISVC, side)
	}
}

// applyCutoverTraffic writes the cutover on every member: its target alone.
// The sentinel releases the held stable instance, so no capacity is left to
// rest a fallback on; a canary pod lost from here is the workload's repair.
func applyCutoverTraffic(in ReconcileInputs) {
	applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, in.StableRevisionHash, in.CanaryPairingProtocol, in.StablePairingProtocol, 100)
	for c, m := range in.Secondaries {
		applyMemberTraffic(in.ISVC, c, memberWeights(m, 100, bothSidesServe, nil))
	}
}

// applyGroupStableTraffic returns every secondary to its stable revision
// alongside the primary's rollback write. A member with no stable revision
// is sent no revert and keeps the targets it has; one whose stable revision
// is observed with no ready pod rests its traffic on the revision that
// serves until one does.
func applyGroupStableTraffic(in ReconcileInputs, secondaries []memberSide) {
	for _, side := range secondaries {
		if side.stable == "" {
			continue
		}
		if ready, observed := in.GroupReadyPerRevisionPods[side.component]; observed && ready[side.stable] == 0 {
			restOnServingRevision(in.ISVC, side)
			continue
		}
		applyMemberTraffic(in.ISVC, side.component, stableAlone(side))
	}
}

// holdStoppedTraffic keeps a stopped unit's traffic off a revision with no
// serving pod. The stop leaves the split where it stood, and a pod the
// stopped canary gets back earns it nothing. The one move is a stable
// revision whose pods are all lost: its member's traffic goes to the
// revision that serves and, the unit having one stable side, every other
// member's goes to its target while the canary revision serves on every
// member; once a stable pod serves on every member again each member's
// whole split returns to the stable revision, where a stop rests it. A
// split a cutover left on the target alone, under a record at 100, is not
// touched.
func holdStoppedTraffic(in ReconcileInputs, cs *v1beta1.CanaryStatus) {
	if cs.CanaryRevisionHash != in.CanaryRevisionHash {
		return
	}
	sides := unitSides(in, in.CanaryRevisionHash, in.StableRevisionHash)
	write := trafficWrite{context: fmt.Sprintf("stopped at step %d", cs.CurrentStep), sides: sides,
		before: stableShares(in.ISVC, sides), remainder: true, recorded: cs.ObservedTrafficWeight, oneStableSide: true}
	unit := sidesServingOf(in)
	for _, side := range sides {
		if !takesPartInSplit(side) || len(in.ISVC.Status.Components[side.component].Traffic) == 0 {
			continue
		}
		switch {
		case side.readyPods[side.stable] == 0:
			restOnServingRevision(in.ISVC, side)
		case unit.canaryPath && unit.stableGone:
			restOn(in.ISVC, side, side.target)
		case write.before[side.component].onTargetAlone && cs.ObservedTrafficWeight < 100:
			applyMemberTraffic(in.ISVC, side.component, stableAlone(side))
		}
	}
	reportTrafficOffStable(in, write)
}
