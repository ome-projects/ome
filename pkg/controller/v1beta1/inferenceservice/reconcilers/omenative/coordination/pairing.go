package coordination

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// PairingProtocolForRevision resolves the P/D pairing protocol one revision of
// one Component was minted under, by reading the annotation (or stored
// payload) of the ControllerRevision named `<prefix>-<component>-<hash>`,
// where prefix is the Component's replica prefix (prefixFor).
//
// found=false reports a NotFound CR. Callers choose the degrade: traffic /
// Service-label producers treat it as "" (a swept or pre-pairing revision
// pairs with anything), while the pair-floor gate fails closed — retention
// protects every live revision, so a missing CR behind a SERVING instance is
// an anomaly the gate must not guess about.
func PairingProtocolForRevision(ctx context.Context, reads client.Reader, namespace, prefix string, component v1beta1.ComponentType, hash string) (protocol string, found bool, err error) {
	if hash == "" {
		return "", false, nil
	}
	return pairingProtocolForRevisionName(ctx, reads, namespace, fmt.Sprintf("%s-%s-%s", prefix, component, hash))
}

// pairingProtocolForRevisionName is PairingProtocolForRevision keyed by the
// full ControllerRevision name (the form InstanceStatus.RunningRevision
// records).
func pairingProtocolForRevisionName(ctx context.Context, reads client.Reader, namespace, name string) (protocol string, found bool, err error) {
	if reads == nil || name == "" {
		return "", false, nil
	}
	cr := &appsv1.ControllerRevision{}
	if gerr := reads.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cr); gerr != nil {
		if apierrors.IsNotFound(gerr) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("get ControllerRevision %s: %w", name, gerr)
	}
	return revision.PairingProtocolFromRevision(cr), true, nil
}

// AttachPairingProtocols fills RevisionWeight.PairingProtocol on each weight
// from its revision's ControllerRevision under the Component's replica
// prefix, so BuildTrafficTargets publishes the cohort token routing
// consumers pair on. A missing CR yields "" (a swept or pre-pairing revision
// pairs with anything); read errors propagate.
func AttachPairingProtocols(ctx context.Context, reads client.Reader, namespace, prefix string, component v1beta1.ComponentType, weights []RevisionWeight) error {
	for i := range weights {
		proto, _, err := PairingProtocolForRevision(ctx, reads, namespace, prefix, component, weights[i].RevisionHash)
		if err != nil {
			return err
		}
		weights[i].PairingProtocol = proto
	}
	return nil
}

// pairingComponents are the Components that participate in P/D pairing. The
// router fronts both cohorts and never constrains a pair.
var pairingComponents = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}

// CheckPairing is the pair-floor gate for pairing-protocol transitions: while
// a rollout is changing spec.rollout.pairingProtocol, every per-Instance
// update step must leave at least one pairable serving (engine, decoder) pair
// — a serving engine and a serving decoder whose cohort protocols are equal,
// or either empty (empty pairs with anything). Without it, per-Instance
// progressions can drain the last old-cohort engine while the new cohort has
// no serving decoder yet; every serving engine then speaks a protocol no
// serving decoder does and routing has no valid pair to place a request on.
//
// A step that takes its pod out of rotation at once (a drain-first recreate,
// or an in-place patch that returns the pod on the new cohort) must also
// keep both Components at their pairable floor: at least desired minus
// maxUnavailable Instances with a serving pod that has a partner on the
// other side (resolvePairFloor). A surge start takes nothing out of rotation,
// so it answers the one-pair rule alone and the drain-time gate
// (EvaluatePairingDrain) holds the floor when its source would leave.
//
// The target cohort is the protocol of ctx.TargetRevision, the revision the
// Component's Instances move to in this pass — the rollback revision while a
// rollback is pinned, else the spec target. A canary revert is therefore
// simulated toward the stable cohort while the live spec field still names
// the rejected protocol; the live field alone would orient it the wrong way.
//
// Inactive (allows) unless ALL of: the resolved group spans engine AND
// decoder, ctx.Component is one of the pairing Components, the target
// revision carries a non-empty pairing protocol, and some serving instance
// still runs a different non-empty protocol (a transition in flight). A
// transition to or from the empty protocol never activates the gate — empty
// pairs with anything, so every intermediate mix is routable.
//
// When active it simulates this step's end state against authoritative IR
// status, instance-granular (a serving instance is one with
// ServingPodCount>0): the Instance being started leaves the cohort of
// fromRevision, the ControllerRevision its row runs now. An Instance
// stranded on an intermediate cohort that pairs with nothing may therefore
// move on while its sibling on the last pairable cohort is held. An empty
// fromRevision, or one whose ControllerRevision is gone, names no cohort, so
// every old cohort is simulated losing the instance, the pessimistic
// reading. A surge in flight serves two cohorts at once: its source keeps
// the running cohort and its replacement, once it serves, counts for the
// cohort of the revision the surge is pinned to (servingRevisions), so a
// peer's step sees that capacity before the promote and never waits on a
// drain that waits on it. Surge and in-place strategies credit the target
// cohort with the replacement — a SurgeThenDrain op only drains after its
// replacement is Ready, and an in-place op returns the same pod — while
// drain-first strategies do not. In-place is NOT bypassed the way CheckRatio
// bypasses it: its net capacity change is ~0, but the instance still crosses
// cohorts, which is exactly what this gate paces. In-flight starts from the
// same wake-up are charged pessimistically against each cohort under
// simulation.
//
// A start that would put the first new-cohort Instance on its side waits
// while no step of the other side to the new cohort keeps this side at its
// floor (strandsFirstMover): the new Instance would wait for a partner.
//
// Four liveness escapes keep the gate deadlock-free:
//   - When no pairable pair exists BEFORE the step, there is nothing left to
//     protect and progress toward the target cohort is the only way back to
//     one — allow.
//   - When both pairing Components are down to a single serving instance with
//     no target-cohort capacity anywhere (the 1×1 mutual wall), denying both
//     first movers would deadlock the rollout permanently, so the step is
//     allowed. For surge strategies the drain-time check
//     (EvaluatePairingDrain) then holds each source until the peer's target
//     cohort serves, so admitting the first movers costs no unpaired window;
//     a drain-first strategy has no such point and keeps the window.
//   - When the acting Component is down to its last serving instance and the
//     peer already serves the target cohort (the last-hop shape), the step is
//     the acting side's final unavoidable hop and no other step can create
//     the pairing a denial would wait for — allow. The transient unpaired
//     window equals what a single-replica update costs WITHOUT pairing (the
//     gate must not be stricter than non-pairing behavior for the last
//     mover), and the incoming instance pairs with the peer's target
//     capacity the moment it serves.
//   - When nothing is in flight and the floor holds every start of both
//     sides, one start is admitted (deadEndMover): the one that gives the
//     new cohort's unpartnered Instances a partner at the smallest cost.
//
// Fails closed (deny, retryable reason) when the target revision is unknown
// or IR status or a serving instance's ControllerRevision cannot be read —
// approving a cohort-draining step on unreadable protocol data is how the
// last pair dies silently.
func (ctx GateContext) CheckPairing(fromRevision string, strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (allowed bool, reason string) {
	v := ctx.pairingVerdict(fromRevision, strategy, inFlightSurge, inFlightUnavail)
	return v.allowed, v.reason
}

// pairingVerdict is CheckPairing's answer beside the reading the batch step
// needs: whether the pairing peer is held still.
type pairingVerdict struct {
	allowed bool
	reason  string
	// peerHeldStill reports a peer this gate holds still, which moves only
	// once this Component has: a peer whose open attempts are all
	// hand-overs the drain-time gate keeps, or a peer with no attempt open
	// and every Instance serving whose every possible start this gate
	// holds. A peer with an Instance that serves nothing is coming up on
	// its own and is not one.
	peerHeldStill bool
}

func (ctx GateContext) pairingVerdict(fromRevision string, strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) pairingVerdict {
	if ctx.ShortCircuit {
		return pairingVerdict{allowed: true, reason: ctx.ShortReason}
	}
	if !groupPairsEngineAndDecoder(ctx.Group) {
		return pairingVerdict{allowed: true, reason: "group does not pair engine and decoder"}
	}
	component := ctx.Component
	if component != v1beta1.EngineComponent && component != v1beta1.DecoderComponent {
		return pairingVerdict{allowed: true, reason: "component does not participate in pairing"}
	}
	target, ok, reason := ctx.targetPairingProtocol()
	if !ok {
		return pairingVerdict{reason: reason}
	}
	if target == "" {
		return pairingVerdict{allowed: true, reason: "target revision declares no pairing protocol"}
	}

	// Serving instances per (Component, protocol), from authoritative IR
	// status. An instance with no recorded RunningRevision predates revision
	// tracking entirely and therefore predates pairing protocols: "".
	serving := map[v1beta1.ComponentType]map[string]int32{}
	instances := map[v1beta1.ComponentType]map[string][]string{}
	replicas := map[v1beta1.ComponentType]*v1beta1.InferenceReplica{}
	protoByRevision := map[string]string{}
	// servingProtocol resolves the cohort of a revision a serving instance
	// runs, once per revision. A revision that cannot be read, or is gone,
	// fails the gate closed: retention protects every live revision, so a
	// missing one behind a serving instance is an anomaly the gate must not
	// guess about.
	servingProtocol := func(revision string, comp v1beta1.ComponentType, index int32) (proto, reason string) {
		if p, ok := protoByRevision[revision]; ok {
			return p, ""
		}
		p, found, gerr := pairingProtocolForRevisionName(ctx.Ctx, ctx.Reads, ctx.ISVC.Namespace, revision)
		if gerr != nil {
			return "", fmt.Sprintf("pairing gate: cannot read revision %s for serving %s instance %d, failing closed: %v",
				revision, comp, index, gerr)
		}
		if !found {
			return "", fmt.Sprintf("pairing gate: revision %s for serving %s instance %d not found, failing closed",
				revision, comp, index)
		}
		protoByRevision[revision] = p
		return p, ""
	}
	transition := false
	for _, comp := range pairingComponents {
		serving[comp] = map[string]int32{}
		instances[comp] = map[string][]string{}
		ir, _, err := irprojector.DecodedComponentIRFor(ctx.Ctx, ctx.Reads, ctx.ISVC, comp)
		if err != nil {
			return pairingVerdict{reason: fmt.Sprintf("pairing gate: cannot read %s IR status, failing closed: %v", comp, err)}
		}
		if ir == nil {
			continue
		}
		replicas[comp] = ir
		for i := range ir.Status.InstanceStatuses {
			inst := &ir.Status.InstanceStatuses[i]
			if inst.ServingPodCount <= 0 {
				continue
			}
			for _, revision := range servingRevisions(inst) {
				proto := ""
				if revision != "" {
					var reason string
					if proto, reason = servingProtocol(revision, comp, inst.Index); reason != "" {
						return pairingVerdict{reason: reason}
					}
				}
				serving[comp][proto]++
				key := strconv.Itoa(int(inst.Index))
				instances[comp][key] = append(instances[comp][key], proto)
				if proto != "" && proto != target {
					transition = true
				}
			}
		}
	}
	if !transition {
		return pairingVerdict{allowed: true, reason: "no pairing protocol transition in flight"}
	}
	if !pairableServingPairExists(serving) {
		return pairingVerdict{allowed: true, reason: fmt.Sprintf("no serving engine+decoder pair exists to protect; allowing progress toward cohort %q", target)}
	}

	// This start moves as the caller says; the peer's starts move as its
	// replica declares them.
	mine := mechanicsOf(strategy)
	credit := mine.credit
	peer := pairingPeer(component)
	picture := pairingPicture{
		target:    target,
		serving:   serving,
		instances: instances,
		floors: map[v1beta1.ComponentType]pairFloor{
			component: ctx.pairableMinimumOf(replicas[component]),
			peer:      ctx.pairableMinimumOf(replicas[peer]),
		},
		mechanics: map[v1beta1.ComponentType]stepMechanics{
			component: mine,
			peer:      mechanicsOf(replicaMechanism(replicas[peer])),
		},
		known: map[v1beta1.ComponentType]bool{
			component: replicas[component] != nil,
			peer:      replicas[peer] != nil,
		},
		protocols: protoByRevision,
		firstSide: ctx.namedFirstSide(),
	}
	// A dead end needs a settled picture: nothing in flight on either side,
	// this pass's starts included.
	settled := inFlightSurge+inFlightUnavail == 0 && !attemptOpen(replicas[component]) && !attemptOpen(replicas[peer])
	// The cohort the started instance leaves. A row that records no revision,
	// or one whose revision is gone, names no cohort, and every old cohort is
	// then simulated losing the instance.
	fromProto, fromKnown := "", false
	if fromRevision != "" {
		p, ok := protoByRevision[fromRevision]
		if !ok {
			var gerr error
			p, ok, gerr = pairingProtocolForRevisionName(ctx.Ctx, ctx.Reads, ctx.ISVC.Namespace, fromRevision)
			if gerr != nil {
				return pairingVerdict{reason: fmt.Sprintf("pairing gate: cannot read revision %s of the starting %s instance, failing closed: %v",
					fromRevision, component, gerr)}
			}
		}
		fromProto, fromKnown = p, ok
	}
	allow := func(reason string) pairingVerdict {
		return pairingVerdict{allowed: true, reason: reason, peerHeldStill: picture.heldStill(peer, replicas[peer])}
	}
	// In-flight fresh starts from this wake-up haven't reached IR status yet;
	// charge them against the cohort under simulation (worst case they all
	// came from it). Surge in-flights carry their own replacement credit.
	inFlightLoss := inFlightSurge + inFlightUnavail
	for proto, n := range serving[component] {
		if proto == target || n <= 0 {
			continue
		}
		loss := inFlightLoss
		if !fromKnown || proto == fromProto {
			loss++
		}
		if loss == 0 {
			continue
		}
		sim := copyServing(serving)
		sim[component][proto] -= loss
		if sim[component][proto] < 0 {
			sim[component][proto] = 0
		}
		sim[component][target] += credit + inFlightSurge
		start := candidateStart{side: component, from: proto, stepMechanics: mine}
		verdict, reason := picture.judge(start, sim)
		switch verdict {
		case startAllowed:
			if reason != "" {
				return allow(reason)
			}
			continue
		case heldByFloor, heldAsFirstMover:
			if settled && fromKnown {
				if why, ok := picture.deadEndMover(start); ok {
					return allow(why)
				}
			}
		}
		return pairingVerdict{reason: reason}
	}
	return allow("")
}

// namedFirstSide is the pairing side the operator names to move first
// inside every batch, which the batch step orders. A canary unit is not
// batched, so none is named for it and the pairing gate orders the two
// sides itself.
func (ctx GateContext) namedFirstSide() v1beta1.ComponentType {
	if ctx.CanaryUnit {
		return ""
	}
	return ctx.Defaults.PrefillDecode.namedFirstSide()
}

// stepMechanics is what a start does to the serving picture at once: the
// replacement it is credited with, and whether its pod leaves rotation now.
type stepMechanics struct {
	credit            int32
	leavesRotationNow bool
}

// mechanicsOf reads a strategy: a surge op drains only after its
// replacement is Ready and an in-place op returns the same pod, so both
// carry a credit; a drain-first start takes the pod out with nothing back.
func mechanicsOf(strategy workloadtypes.UpdateStrategyType) stepMechanics {
	switch strategy {
	case workloadtypes.UpdateStrategySurgeThenDrain, "":
		return stepMechanics{credit: 1}
	case workloadtypes.UpdateStrategyInPlaceIfPossible, workloadtypes.UpdateStrategyInPlaceOnly:
		return stepMechanics{credit: 1, leavesRotationNow: true}
	}
	return stepMechanics{leavesRotationNow: true}
}

// replicaMechanism is the strategy a replica's starts run on: RecreatePod
// for every drain-first shape (an in-place strategy on a gang rebuilds it),
// else the declared strategy, SurgeThenDrain when unset.
func replicaMechanism(ir *v1beta1.InferenceReplica) workloadtypes.UpdateStrategyType {
	if ir == nil || ir.Spec.Lifecycle == nil || ir.Spec.Lifecycle.UpdateStrategy == nil {
		return workloadtypes.UpdateStrategySurgeThenDrain
	}
	if drainsFirst(ir) {
		return workloadtypes.UpdateStrategyRecreatePod
	}
	return workloadtypes.UpdateStrategyType(ir.Spec.Lifecycle.UpdateStrategy.Type)
}

// attemptOpen reports an operation of the replica still in flight: a row
// carrying one that neither failed nor parked.
func attemptOpen(ir *v1beta1.InferenceReplica) bool {
	if ir == nil {
		return false
	}
	for i := range ir.Status.InstanceStatuses {
		if rowAttemptOpen(&ir.Status.InstanceStatuses[i]) {
			return true
		}
	}
	return false
}

// rowAttemptOpen reports a row carrying an operation that neither failed
// nor parked.
func rowAttemptOpen(row *v1beta1.OMENativeInstanceStatus) bool {
	return row.Operation != nil && row.Operation.Step != workloadtypes.UpdateStepParked && row.Phase != v1beta1.OMENativeInstanceFailed
}

// pairingPicture is the serving state a protocol transition is judged on:
// serving Instances per side and cohort, each side's Instances with the
// cohorts of their serving pods, each side's pairable floor, how each
// side's starts move, and the cohort of every serving revision.
type pairingPicture struct {
	target    string
	serving   map[v1beta1.ComponentType]map[string]int32
	instances map[v1beta1.ComponentType]map[string][]string
	floors    map[v1beta1.ComponentType]pairFloor
	mechanics map[v1beta1.ComponentType]stepMechanics
	known     map[v1beta1.ComponentType]bool
	protocols map[string]string
	// firstSide is the pairing side the operator names to move first
	// inside every batch, empty when none is named. The batch step orders
	// the two sides when one is named; this gate does when none is.
	firstSide v1beta1.ComponentType
}

// candidateStart is one start a side could open: the cohort its Instance
// leaves and what the start does at once.
type candidateStart struct {
	side v1beta1.ComponentType
	from string
	stepMechanics
}

// simulate is the serving picture right after the start, nothing else in
// flight.
func (c candidateStart) simulate(serving map[v1beta1.ComponentType]map[string]int32, target string) map[v1beta1.ComponentType]map[string]int32 {
	sim := copyServing(serving)
	if sim[c.side][c.from] > 0 {
		sim[c.side][c.from]--
	}
	sim[c.side][target] += c.credit
	return sim
}

type startVerdict int

const (
	startAllowed startVerdict = iota
	heldByFloor
	heldAsFirstMover
	heldNoPair
)

// judge answers one start whose end state is sim: the one-pair rule with
// its escapes, then the pairable floor and the first-mover rule for a start
// whose pod leaves rotation now.
func (p pairingPicture) judge(c candidateStart, sim map[v1beta1.ComponentType]map[string]int32) (startVerdict, string) {
	other := pairingPeer(c.side)
	if pairableServingPairExists(sim) {
		if !c.leavesRotationNow {
			return startAllowed, ""
		}
		if held, reason := startBreaksPairFloor(p.serving, sim, c.side, other, p.floors, p.target, c.from); held {
			return heldByFloor, reason
		}
		if held, reason := p.strandsFirstMover(c, sim); held {
			return heldAsFirstMover, reason
		}
		return startAllowed, ""
	}
	if mutualPairingWall(p.serving, c.side, other, p.target) {
		if p.surgeOfTheOtherSideKeepsThePair(c) {
			return heldNoPair, fmt.Sprintf("pairing protocol transition to %q: updating the serving %s of cohort %q would leave no pairable serving engine+decoder pair, and the %s surges, keeping its pod in rotation while its replacement comes up; holding until a serving %s runs cohort %q",
				p.target, c.side, c.from, other, other, p.target)
		}
		return startAllowed, fmt.Sprintf("both %s and %s are down to their last serving instance with no cohort %q capacity; allowing the step rather than deadlocking the transition",
			c.side, other, p.target)
	}
	if lastHopEscape(p.serving, c.side, other, p.target) {
		return startAllowed, fmt.Sprintf("%s is down to its last serving instance and %s already serves cohort %q; allowing the final hop (the unpaired window equals a single-replica update without pairing)",
			c.side, other, p.target)
	}
	return heldNoPair, fmt.Sprintf("pairing protocol transition to %q: updating a serving %s of cohort %q would leave no pairable serving engine+decoder pair; holding until cohort %q serves on both Components",
		p.target, c.side, c.from, p.target)
}

// surgeOfTheOtherSideKeepsThePair reports a start at the one-by-one wall
// that waits rather than take the escape: its pod leaves rotation at once
// while the other side's starts keep theirs, and no side is named first.
// The other side's surge brings the target cohort up beside its old pod,
// which the drain-time gate keeps until this side serves that cohort, so
// the pair on the old cohort serves throughout and this start moves with a
// partner once the replacement serves. Two sides that both take their pod
// out at once have no such order, and a side named first is ordered by the
// batch step.
func (p pairingPicture) surgeOfTheOtherSideKeepsThePair(c candidateStart) bool {
	other := pairingPeer(c.side)
	return p.firstSide == "" && c.leavesRotationNow && p.known[other] && !p.mechanics[other].leavesRotationNow
}

// strandsFirstMover holds a start that would put the first target-cohort
// Instance on its side while no start of the drain-first or in-place other
// side to the target keeps this side at its floor: no step could pair it.
func (p pairingPicture) strandsFirstMover(c candidateStart, sim map[v1beta1.ComponentType]map[string]int32) (bool, string) {
	side, other := c.side, pairingPeer(c.side)
	floor := p.floors[side]
	if !floor.Applies() || !p.known[other] || !p.mechanics[other].leavesRotationNow || pairsWithPeer(sim[other], p.target) {
		return false, ""
	}
	// The picture once this start's replacement serves.
	after := copyServing(sim)
	if c.credit == 0 {
		after[side][p.target]++
	}
	for cohort, n := range after[other] {
		if cohort == p.target || n <= 0 {
			continue
		}
		answer := candidateStart{side: other, from: cohort, stepMechanics: p.mechanics[other]}
		asim := answer.simulate(after, p.target)
		if pairableServingPairExists(asim) {
			if held, _ := startBreaksPairFloor(after, asim, other, side, p.floors, p.target, cohort); !held {
				return false, ""
			}
			continue
		}
		if mutualPairingWall(after, other, side, p.target) || lastHopEscape(after, other, side, p.target) {
			return false, ""
		}
	}
	return true, fmt.Sprintf("pairing protocol transition to %q: updating a serving %s of cohort %q would put a %s on cohort %q with no %s to pair with, and every %s step to cohort %q would then take the %s below its %s; holding until a serving %s runs cohort %q",
		p.target, side, c.from, side, p.target, other, other, p.target, side, floor, other, p.target)
}

// candidates lists every start either side could open, engine first and
// cohorts in name order, so both sides rank them alike.
func (p pairingPicture) candidates() []candidateStart {
	var out []candidateStart
	for _, side := range pairingComponents {
		cohorts := make([]string, 0, len(p.serving[side]))
		for cohort, n := range p.serving[side] {
			if cohort != p.target && n > 0 {
				cohorts = append(cohorts, cohort)
			}
		}
		sort.Strings(cohorts)
		for _, cohort := range cohorts {
			out = append(out, candidateStart{side: side, from: cohort, stepMechanics: p.mechanics[side]})
		}
	}
	return out
}

// heldStill reports a side that cannot move on its own. Its open attempts,
// if any, are all hand-overs the drain-time gate keeps, for whichever of
// its reasons: their old pods leave only once the other side has moved or
// serves the new cohort. A gang's replacement is a row of its own, the
// slot its source's attempt points at, and is read with its source. With
// none open, every Instance of it serves and every start it could open is
// held by this gate, whatever its mechanism: a surge start is judged on
// the picture after its hand-over, so a side of one Instance that surges
// is held still until the other side serves the new cohort as much as one
// that drains first. A side with an Instance that serves nothing and
// carries no attempt is coming up on its own: a rebuilt Instance is
// promoted in one status write and its pods counted as serving in the
// next, and a lost pod is rebuilt by a repair. Its holds clear with its own
// pods, not with the other side's move, so it is not held still.
func (p pairingPicture) heldStill(side v1beta1.ComponentType, ir *v1beta1.InferenceReplica) bool {
	if !p.known[side] {
		return false
	}
	handOvers := 0
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		if !rowAttemptOpen(row) || gangSurgeSlot(row) {
			continue
		}
		if !p.handOverHeld(side, row, ir) {
			return false
		}
		handOvers++
	}
	if handOvers > 0 {
		return true
	}
	if sideComingUp(ir) {
		return false
	}
	starts := 0
	for _, c := range p.candidates() {
		if c.side != side {
			continue
		}
		starts++
		if verdict, _ := p.judge(c, c.simulate(p.serving, p.target)); verdict == startAllowed {
			return false
		}
	}
	return starts > 0
}

// sideComingUp reports a side with an Instance coming up on its own
// (rowComingUp). Its holds clear with its own pods, not with the other
// side's move, so the side is never read as held still.
func sideComingUp(ir *v1beta1.InferenceReplica) bool {
	for i := range ir.Status.InstanceStatuses {
		if rowComingUp(&ir.Status.InstanceStatuses[i]) {
			return true
		}
	}
	return false
}

// rowComingUp reports an Instance that serves nothing while no attempt
// and no failure stands on it: its pods are on their way into rotation
// (the count that follows a promote, a repair's rebuild) and nothing the
// other side does brings them.
func rowComingUp(row *v1beta1.OMENativeInstanceStatus) bool {
	if row.Operation != nil || row.ServingPodCount > 0 {
		return false
	}
	return v1beta1convert.InstanceStatusToWorkload(*row).Phase != workloadtypes.InstancePhaseFailed
}

// handOverHeld reports a surge whose replacement serves beside its old
// pods and whose old pods the drain-time gate keeps in rotation, for
// whichever of its reasons: the replacement cannot pair yet, the other
// side's floor, or the last pair. A single pod's replacement is its second
// serving pod; a gang's is the row in the slot its attempt points at,
// serving whole. It asks the drain gate's own rule
// (pairingDrainPicture.hold) over this picture, with the old pods as the
// ones leaving; only the other side's own moves can change the answer.
func (p pairingPicture) handOverHeld(side v1beta1.ComponentType, row *v1beta1.OMENativeInstanceStatus, ir *v1beta1.InferenceReplica) bool {
	op := row.Operation
	if op == nil || op.Type != v1beta1.InstanceOperationUpdate || op.Step != workloadtypes.UpdateStepSurge || row.ServingPodCount < 1 {
		return false
	}
	if op.SurgeIndex == nil {
		if row.ServingPodCount < 2 {
			return false
		}
	} else if !gangReplacementServes(ir, *op.SurgeIndex, op.TargetRevision) {
		return false
	}
	from, known := p.protocolOf(row.RunningRevision)
	if !known {
		return false
	}
	// The cohort the old pods are traded for: the one the attempt is pinned
	// to, which is the target's unless a later edit moved the target while
	// the hand-over was under way.
	cohort := p.target
	if proto, ok := p.protocolOf(op.TargetRevision); ok && op.TargetRevision != "" {
		cohort = proto
	}
	if cohort == "" {
		return false
	}
	key := strconv.Itoa(int(row.Index))
	after := copyServing(p.serving)
	if after[side][from] > 0 {
		after[side][from]--
	}
	afterInstances := copyInstances(p.instances)
	afterInstances[side][key] = withoutOneCohort(afterInstances[side][key], from)
	drain := pairingDrainPicture{
		component: side,
		cohort:    cohort,
		serving:   p.serving, after: after,
		instances: p.instances, afterInstances: afterInstances,
		leaving: map[string]struct{}{from: {}},
		floors:  p.floors,
	}
	held, _ := drain.hold()
	return held
}

// gangSurgeSlot reports the row a gang surge builds its replacement in,
// or retires it from: part of its source's attempt, not one of its own.
func gangSurgeSlot(row *v1beta1.OMENativeInstanceStatus) bool {
	op := row.Operation
	return op != nil && op.Type == v1beta1.InstanceOperationUpdate &&
		(op.Step == workloadtypes.UpdateStepGangSurgeTarget || op.Step == workloadtypes.UpdateStepGangSurgeTargetCleanup)
}

// gangReplacementServes reports whether the replacement gang in the given
// slot, pinned to the given revision, serves whole: the state in which its
// source's old pods are asked to leave.
func gangReplacementServes(ir *v1beta1.InferenceReplica, slot int32, pinned string) bool {
	if ir == nil {
		return false
	}
	for i := range ir.Status.InstanceStatuses {
		r := &ir.Status.InstanceStatuses[i]
		if r.Index != slot {
			continue
		}
		op := r.Operation
		return op != nil && op.Type == v1beta1.InstanceOperationUpdate && op.Step == workloadtypes.UpdateStepGangSurgeTarget &&
			op.TargetRevision == pinned && r.PodCount > 0 && r.ServingPodCount >= r.PodCount
	}
	return false
}

// protocolOf is the cohort of a revision the picture resolved; a row with
// no revision is the empty cohort.
func (p pairingPicture) protocolOf(revision string) (string, bool) {
	if revision == "" {
		return "", true
	}
	proto, ok := p.protocols[revision]
	return proto, ok
}

// deadEndMover reports whether start is the one start admitted when the
// floor or the first-mover rule holds every start of both sides: the one
// that pairs the target cohort's unpartnered Instances, at the least cost.
func (p pairingPicture) deadEndMover(start candidateStart) (string, bool) {
	type ranked struct {
		candidateStart
		restores bool
		loss     int32
	}
	var best *ranked
	for _, c := range p.candidates() {
		sim := c.simulate(p.serving, p.target)
		switch verdict, _ := p.judge(c, sim); verdict {
		case startAllowed:
			return "", false
		case heldNoPair:
			continue
		}
		other := pairingPeer(c.side)
		r := ranked{
			candidateStart: c,
			restores:       p.serving[other][p.target] > 0 && !pairsWithPeer(p.serving[c.side], p.target),
		}
		for _, side := range pairingComponents {
			o := pairingPeer(side)
			r.loss += pairableCohortCount(p.serving[side], p.serving[o]) - pairableCohortCount(sim[side], sim[o])
		}
		if best == nil || (r.restores && !best.restores) || (r.restores == best.restores && r.loss < best.loss) {
			r := r
			best = &r
		}
	}
	if best == nil || best.side != start.side || best.from != start.from {
		return "", false
	}
	other := pairingPeer(best.side)
	cost := ""
	if side, left, floor, ok := p.floorShortfall(best.simulate(p.serving, p.target)); ok {
		cost = fmt.Sprintf(" (the %s keeps %d of its %s while the replacement starts)", side, left, floor)
	}
	if best.restores {
		return fmt.Sprintf("pairing protocol transition to %q: the pairable floor holds every step of the engine and the decoder; admitting the serving %s of cohort %q, whose replacement gives the serving %s of cohort %q a partner at the smallest cost in pairable Instances%s",
			p.target, best.side, best.from, other, p.target, cost), true
	}
	return fmt.Sprintf("pairing protocol transition to %q: the pairable floor holds every step of the engine and the decoder; admitting the serving %s of cohort %q as the step that costs the fewest pairable Instances%s",
		p.target, best.side, best.from, cost), true
}

// floorShortfall names the first side the simulated picture leaves under
// its floor, with what it keeps.
func (p pairingPicture) floorShortfall(sim map[v1beta1.ComponentType]map[string]int32) (v1beta1.ComponentType, int32, pairFloor, bool) {
	for _, side := range pairingComponents {
		floor := p.floors[side]
		if !floor.Applies() {
			continue
		}
		if left := pairableCohortCount(sim[side], sim[pairingPeer(side)]); left < floor.floor {
			return side, left, floor, true
		}
	}
	return "", 0, pairFloor{}, false
}

// servingRevisions names the revisions a serving Instance's pods run, one
// entry per cohort in rotation. A settled row runs its RunningRevision; an
// empty one is the empty cohort, which pairs with anything. A single-pod
// surge keeps its replacement inside the source's Instance until the
// promote, so once both pods are in rotation the row serves the running
// revision and the one the surge is pinned to, and past the hand-over, with
// the source out of rotation, the replacement is the row's serving pod. A
// gang surge's replacement is its own row: it runs nothing yet and serves
// the pinned revision.
func servingRevisions(inst *v1beta1.OMENativeInstanceStatus) []string {
	op := inst.Operation
	if op == nil || op.Type != v1beta1.InstanceOperationUpdate || op.TargetRevision == "" {
		return []string{inst.RunningRevision}
	}
	switch op.Step {
	case workloadtypes.UpdateStepGangSurgeTarget:
		return []string{op.TargetRevision}
	case workloadtypes.UpdateStepSurge:
		if op.SurgeIndex == nil && inst.ServingPodCount >= 2 {
			return []string{inst.RunningRevision, op.TargetRevision}
		}
	case workloadtypes.UpdateStepSurgeDrain, workloadtypes.UpdateStepSurgeDrainSettle:
		if op.SurgeIndex == nil {
			if inst.ServingPodCount >= 2 {
				return []string{inst.RunningRevision, op.TargetRevision}
			}
			return []string{op.TargetRevision}
		}
	}
	return []string{inst.RunningRevision}
}

// startBreaksPairFloor reports whether the simulated step pushes a pairing
// Component below its pairable floor: fewer of its serving Instances have a
// partner after the step than its floor allows, and fewer than before it. A
// Component already short for another reason is not held on that shortfall.
func startBreaksPairFloor(serving, sim map[v1beta1.ComponentType]map[string]int32, component, peer v1beta1.ComponentType, floors map[v1beta1.ComponentType]pairFloor, target, fromProto string) (bool, string) {
	for _, side := range []v1beta1.ComponentType{peer, component} {
		floor := floors[side]
		if !floor.Applies() {
			continue
		}
		other := pairingPeer(side)
		before := pairableCohortCount(serving[side], serving[other])
		after := pairableCohortCount(sim[side], sim[other])
		if after >= floor.floor || after >= before {
			continue
		}
		return true, fmt.Sprintf("pairing protocol transition to %q: updating a serving %s of cohort %q would leave the %s with %d pairable serving Instance(s), below its %s; holding until cohort %q serves on both Components",
			target, component, fromProto, side, after, floor, target)
	}
	return false, ""
}

// targetPairingProtocol is the cohort protocol a step of this Component
// moves toward: the protocol of the revision its Instances are rolling to.
// A nil TargetRevision fails closed — without the revision the unit is
// moving to, the gate cannot tell which way the cohorts move.
func (ctx GateContext) targetPairingProtocol() (protocol string, ok bool, reason string) {
	if ctx.TargetRevision == nil {
		return "", false, "pairing gate: no target revision for this step, failing closed"
	}
	return revision.PairingProtocolFromRevision(ctx.TargetRevision), true, ""
}

// pairableServingPairExists reports whether some serving engine and some
// serving decoder can pair: equal protocols, or either side empty (empty
// pairs with anything).
func pairableServingPairExists(serving map[v1beta1.ComponentType]map[string]int32) bool {
	for pe, engines := range serving[v1beta1.EngineComponent] {
		if engines <= 0 {
			continue
		}
		for pd, decoders := range serving[v1beta1.DecoderComponent] {
			if decoders <= 0 {
				continue
			}
			if pe == "" || pd == "" || pe == pd {
				return true
			}
		}
	}
	return false
}

// mutualPairingWall reports the 1×1 deadlock shape: both pairing Components
// have at most one serving instance and neither serves the target cohort, so
// denying either Component's step would deny both forever.
func mutualPairingWall(serving map[v1beta1.ComponentType]map[string]int32, component, peer v1beta1.ComponentType, target string) bool {
	if serving[component][target] > 0 || serving[peer][target] > 0 {
		return false
	}
	return totalServing(serving[component]) <= 1 && totalServing(serving[peer]) <= 1
}

// lastHopEscape reports the asymmetric deadlock shape: the acting Component
// is down to at most one serving instance while the peer already serves the
// target cohort. The step is the acting side's final unavoidable hop — every
// path to convergence moves that instance, and no other step can create the
// pairing a denial would wait for (the peer's remaining old instances are
// themselves held by the ordinary simulation until the acting side flips).
// A Component with two or more serving instances never satisfies this: its
// old cohort keeps being paced instance-by-instance. Disjoint from the
// mutual wall, which covers the peer-has-no-target-capacity shape.
func lastHopEscape(serving map[v1beta1.ComponentType]map[string]int32, component, peer v1beta1.ComponentType, target string) bool {
	return totalServing(serving[component]) <= 1 && serving[peer][target] > 0
}

func totalServing(byProto map[string]int32) int32 {
	var total int32
	for _, n := range byProto {
		total += n
	}
	return total
}

func copyServing(in map[v1beta1.ComponentType]map[string]int32) map[v1beta1.ComponentType]map[string]int32 {
	out := make(map[v1beta1.ComponentType]map[string]int32, len(in))
	for comp, byProto := range in {
		m := make(map[string]int32, len(byProto)+1)
		for p, n := range byProto {
			m[p] = n
		}
		out[comp] = m
	}
	return out
}
