package coordination

import (
	"fmt"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils"
)

// The batch step paces a rollout group that lists both the engine and the
// decoder: each member's starts are counted in batches of its step, and no
// member opens batch b+1 until every member's batch b serves.
//
// A drain-first member (RecreatePod, or in-place on a multi-pod gang) steps
// on drains by its maxUnavailable budget; a SurgeThenDrain member steps on
// surges by its maxSurge budget, both as the budget gates resolve them.
//
// With 12 prefill beside 8 decode, both surging under the default budget,
// batch 0 creates 3 prefill and 2 decode, and the prefill's fourth surge
// waits until the 2 new decode serve. A hand-over is never a start.
//
// A pairing member that patches single pods in place leaves the other gates
// in charge. A router joins when it steps the way a pairing member does.
// Governed members are not consulted against the serving band.
//
// One wait is waived: a wait on a pairing peer the pairing gate holds still
// (EvaluateUpdateGate), since that peer moves only once this member has: a
// peer whose open attempts are all hand-overs that gate holds, or one with
// no attempt open whose every start that gate holds and this member's
// start releases. A peer short of serving pods of its own is waited on.
//
// Two operator settings layer on this (GroupDefaults.PrefillDecode). Under
// keepRatio a pairing member that patches single pods in place steps on
// its maxUnavailable like a drain-first one, and the engine's and decoder's
// batches are planned together so the share of each side started toward
// the new version stays within the group's tolerance of the other side's
// (pairedBatchPlan). When firstSide names a side, the other side's starts
// of a batch wait until the named side's new Instances of that batch serve.
// While the named side's retry record for its target is Held it opens no
// attempt, so that wait would never end: it reads under the Held gate
// instead, naming the held side and its record, and the roll is held as a
// whole until a corrected revision or a release of the record on that side.
// While the record is Held only the named side's served Instances settle
// its batches for the other side: an ended attempt no retry will re-open
// settles nothing, so the other side's start of the same batch reads Held
// too instead of opening beside a held side.
//
// An attempt that ended without serving is settled for its peers only
// while a retry record for the target stands on the member: the schedule
// counted the failure and owns the next start, after a backoff, one
// attempt at a time, or never while Held. With no record, as when the
// placement grace ended a pod the scheduler could not place, the member's
// next pass starts the attempt again at once; the Instance reads as still
// to start, neither started nor settled, so a peer keeps waiting on its
// batch and the member's own next start, that attempt, asks as the batch
// it belongs to. An attempt the escalation stopped with its pods kept is
// one such ended attempt: it is settled while its record stands and still
// to start once a release or a prune removes it. A hold
// that waits on such a member names the Instance and why it waits, as it
// names an open attempt the scheduler or admission holds.
//
// A member's batches cover the Instances the roll may move: its count less
// the Instances its partition holds on their revision, read with the same
// rule the roll holds them by. A member whose movable Instances have all
// served is done, whatever its partition keeps back, and no peer waits on
// it; a partition lowered later starts the held Instances as a new batch.
//
// A member whose Instances still on their old revision have no partner of
// that revision left is not paced at all: its pairing peer, which this run
// moved to a new revision, runs the target on every Instance (its last old
// Instance rolled, or lost its pod and was rebuilt onto the target), so the
// batch step holds none of the member's starts and its own budget paces
// them. A configured ratio tolerance keeps the pacing, since the ratio rule
// governs then; only the setting's absence releases the member. A peer the
// run did not move keeps its one revision for both of the member's versions
// and is never a lost partner.
//
// A peer's roll target is read off its own status, which a push reaches
// one status write after the spec. A peer whose status was not written
// for the spec it holds, or whose spec was projected from another service
// generation than the acting member's, may still record the previous
// target, with the Instances of the previous push promoted on it. Such a
// peer is read as not yet observed toward the push: nothing started, its
// count and step from its spec, so a wait on it is a wait on its first
// batch until its status records the push (peerStatusPredatesPush).

// batchMember is one member of the group as the batch step reads its
// replica: its desired count, the Instances its partition holds, its step,
// its planned batch marks when the ratio rule planned them, and its
// movable Instances by what the roll has done to them.
type batchMember struct {
	component v1beta1.ComponentType
	desired   int32
	held      int32 // Instances the partition keeps on their revision; the roll never moves them
	step      int32
	marks     []int32 // the count started after each planned batch; nil means batches of step
	target    string  // the revision the member moves its Instances to
	promoted  int32   // Instances running the target revision
	offTarget int32   // Instances not running it: still to move, moving, ended, held, or awaiting retirement
	open      int32   // Instances with an update attempt toward the target in flight
	ended     int32   // Instances whose attempt toward the target ended without serving, with a retry record standing
	// heldReason is the member's retry record for the target while it is
	// Held, in the record's own words: no fresh start opens on the member
	// until a new revision or an operator release. Empty otherwise.
	heldReason string
	// waits names the member's Instances whose attempt waits on the
	// cluster rather than on the roll, in index order: an open attempt
	// with a waiting token and an ended attempt the member starts again
	// at once, each with its reason. A hold that waits on the member
	// carries them.
	waits []string
}

// movable is the number of Instances the roll may move: the desired count
// less the Instances the partition holds.
func (m batchMember) movable() int32 {
	if m.held >= m.desired {
		return 0
	}
	return m.desired - m.held
}

// started is the number of starts the member has opened, counted from zero:
// every Instance on the target, moving to it, or whose move ended with a
// retry record standing. The starts of the current pass are among them,
// since each is written on its row, where the gate reads, before the next
// start is consulted. An ended attempt the member starts again at once
// is not among them: its next start is that attempt.
func (m batchMember) started() int32 { return m.promoted + m.open + m.ended }

// settled is what a peer may stop waiting on: Instances serving on the
// target, plus attempts that ended with a retry record standing, which
// the schedule starts again later as a fresh start that counts as open
// again.
func (m batchMember) settled() int32 { return m.promoted + m.ended }

// settledForTheOtherSide is what the other pairing side may stop waiting
// on when this member is the named first side: settled, unless the
// member's retry record is Held, when no retry re-opens an ended attempt
// and only the Instances serving the target count.
func (m batchMember) settledForTheOtherSide() int32 {
	if m.heldReason != "" {
		return m.promoted
	}
	return m.settled()
}

// mark is the cumulative count batch b should have started: the planned
// mark when the ratio rule planned the batches, else b steps, either way
// capped at the movable count.
func (m batchMember) mark(b int32) int32 {
	if b <= 0 {
		return 0
	}
	movable := m.movable()
	if m.marks != nil {
		if int(b) > len(m.marks) {
			return movable
		}
		return min32(m.marks[b-1], movable)
	}
	if t := b * m.step; t < movable {
		return t
	}
	return movable
}

// nextBatch is the batch the member's next start belongs to, counted from
// one: the first batch whose mark its starts have not reached. A member
// with every batch started is past its last.
func (m batchMember) nextBatch() int32 {
	started, movable := m.started(), m.movable()
	for b := int32(1); ; b++ {
		mark := m.mark(b)
		if mark > started {
			return b
		}
		if mark >= movable {
			return b + 1
		}
	}
}

// batchStep is one group's reading for one start: the members in group
// order, the acting member's position among them, and the pairing side
// that moves first inside each batch, empty when both start together.
// governs is false when the ratio and budget gates decide on their own.
type batchStep struct {
	governs   bool
	acting    int
	members   []batchMember
	firstSide v1beta1.ComponentType
	// released is true when the acting member's Instances still on their
	// old revision have no partner of that revision left and no ratio
	// tolerance is configured: the step then holds none of its starts.
	released bool
}

// stepKind is what a member counts its starts in: drains for a drain-first
// member, surges for a SurgeThenDrain member, nothing for one that patches
// single pods in place or has no replica.
type stepKind int

const (
	stepNone stepKind = iota
	stepDrain
	stepSurge
)

// stepKindOf reads a replica's step kind off its declared update strategy;
// an unset strategy is the SurgeThenDrain default.
func stepKindOf(ir *v1beta1.InferenceReplica) stepKind {
	switch {
	case ir == nil:
		return stepNone
	case drainsFirst(ir):
		return stepDrain
	case surgeStrategy(ir):
		return stepSurge
	}
	return stepNone
}

// readBatchStep reads the group's replicas and decides whether the batch
// step governs this start: the group rolls its members together (not
// Sequential), lists both engine and decoder, both of them step, and the
// acting member is a batched member with a step to count in. A member of
// another kind is batched when it steps the way some pairing member does and
// is left out otherwise. Under keepRatio a pairing member that patches
// single pods in place steps on drains too, and the pairing members' marks
// are planned to the group's tolerance. A recorded group the spec no longer
// declares is read by the pairing gates alone and is not batched. A read
// error is returned for the caller to fail closed on.
func (ctx GateContext) readBatchStep() (batchStep, error) {
	if ctx.ShortCircuit || ctx.Scope == RecordedGroupPairing || ctx.Group.Policy == v1beta1.CoordinationPolicySequential || !groupPairsEngineAndDecoder(ctx.Group) {
		return batchStep{}, nil
	}
	rules := ctx.Defaults.PrefillDecode
	type read struct {
		comp v1beta1.ComponentType
		ir   *v1beta1.InferenceReplica
		kind stepKind
	}
	reads := make([]read, 0, len(ctx.Group.Components))
	pairingKinds := map[stepKind]bool{}
	for _, comp := range ctx.Group.Components {
		ir, _, err := irprojector.DecodedComponentIRFor(ctx.Ctx, ctx.Reads, ctx.ISVC, comp)
		if err != nil {
			return batchStep{}, fmt.Errorf("cannot read %s IR status, failing closed: %w", comp, err)
		}
		kind := stepKindOf(ir)
		if comp == v1beta1.EngineComponent || comp == v1beta1.DecoderComponent {
			if kind == stepNone && ir != nil && rules.KeepRatio {
				kind = stepDrain
			}
			if kind == stepNone {
				return batchStep{}, nil
			}
			pairingKinds[kind] = true
		}
		reads = append(reads, read{comp: comp, ir: ir, kind: kind})
	}
	step := batchStep{acting: -1, firstSide: rules.namedFirstSide()}
	var acting *v1beta1.InferenceReplica
	for _, r := range reads {
		if r.comp == ctx.Component {
			acting = r.ir
		}
	}
	pair := map[v1beta1.ComponentType]int{}
	for _, r := range reads {
		if !pairingKinds[r.kind] {
			continue
		}
		target := rollTarget(r.ir)
		if r.comp == ctx.Component {
			step.acting = len(step.members)
			if ctx.TargetRevision != nil {
				target = ctx.TargetRevision.Name
			}
		}
		if r.comp == v1beta1.EngineComponent || r.comp == v1beta1.DecoderComponent {
			pair[r.comp] = len(step.members)
		}
		member := readBatchMember(r.ir, r.comp, target, ctx.Group.Pacing, r.kind)
		if r.comp != ctx.Component && peerStatusPredatesPush(r.ir, acting) {
			member = member.notYetObserved()
		}
		step.members = append(step.members, member)
	}
	if step.acting < 0 || step.members[step.acting].step <= 0 {
		return batchStep{}, nil
	}
	if rules.KeepRatio {
		if tolerance := ctx.batchTolerance(); tolerance != nil {
			engine, decoder := &step.members[pair[v1beta1.EngineComponent]], &step.members[pair[v1beta1.DecoderComponent]]
			engine.marks, decoder.marks = pairedBatchPlan(engine.movable(), decoder.movable(), engine.step, decoder.step, *tolerance)
		}
	}
	step.released = ctx.partnerless(step, pair)
	step.governs = true
	return step, nil
}

// partnerless reports whether the acting member's Instances still on their
// old revision have no partner of that revision left: the member is a
// pairing side, no ratio tolerance is configured for the group, and its
// pairing peer, which this run moved to a new revision, runs the target on
// every Instance with none still to create. A peer with nothing on the
// target, one still growing to its count, or one whose status predates the
// push is never read this way.
func (ctx GateContext) partnerless(step batchStep, pair map[v1beta1.ComponentType]int) bool {
	me := step.members[step.acting].component
	if me != v1beta1.EngineComponent && me != v1beta1.DecoderComponent {
		return false
	}
	i, ok := pair[pairingPeer(me)]
	if !ok || ctx.ratioToleranceConfigured() {
		return false
	}
	peer := step.members[i]
	return peer.desired > 0 && peer.offTarget == 0 && peer.promoted >= peer.desired && ctx.runMoved(peer.component, peer.target)
}

// ratioToleranceConfigured reports whether the group has a ratio tolerance
// to keep: one on its maintainRatio, written or filled from the operator's
// default, or the operator's default under keepRatio, which plans the
// batches to it. A tolerance of 100, the band's opt-out, is configured;
// only the setting's absence is not.
func (ctx GateContext) ratioToleranceConfigured() bool {
	return ctx.Group.Pacing.RatioTolerancePercent != nil ||
		(ctx.Defaults.PrefillDecode.KeepRatio && ctx.Defaults.RatioTolerancePercent != nil)
}

// runMoved reports whether the active run moves comp to a new revision: the
// target it records for comp is the revision comp rolls to and differs from
// the stable revision comp had when the run opened. A run that records a
// member at the revision it already runs did not move it.
func (ctx GateContext) runMoved(comp v1beta1.ComponentType, target string) bool {
	if ctx.ISVC == nil || ctx.ISVC.Status.Rollout == nil || ctx.ISVC.Status.Rollout.ActiveRun == nil {
		return false
	}
	for _, recorded := range ctx.ISVC.Status.Rollout.ActiveRun.TargetRevisions {
		if recorded.Component == comp {
			return recorded.Revision != "" && recorded.Revision == query.RevisionHashFromControllerRevisionName(target) && recorded.Revision != recorded.StableRevision
		}
	}
	return false
}

// batchTolerance is the tolerance the ratio rule sizes batches to: the
// group's maintainRatio tolerance, else the operator's configured default.
// Nil leaves each side on its own step.
func (ctx GateContext) batchTolerance() *int32 {
	if t := ctx.Group.Pacing.RatioTolerancePercent; t != nil {
		return t
	}
	return ctx.Defaults.RatioTolerancePercent
}

// hold answers the acting member's next start, which belongs to batch b:
// every member's batch b-1 but waived's must have settled first, and the
// first short, peers before self, names the hold under the Ratio gate.
// When a first side is named, a start of the other pairing side also
// waits until the named side's batch b has settled, by its served
// Instances alone while that side's retry record is Held
// (settledForTheOtherSide). A wait on the named side while its record is
// Held reads under the Held gate instead (batchStep.wait). A released
// member is held by neither wait. An empty gate is no hold.
func (s batchStep) hold(waived v1beta1.ComponentType) (gate v1beta1.RolloutHoldGate, reason string) {
	if s.released {
		return "", ""
	}
	me := s.members[s.acting]
	b := me.nextBatch()
	if b > 1 {
		order := make([]int, 0, len(s.members))
		for i := range s.members {
			if i != s.acting {
				order = append(order, i)
			}
		}
		order = append(order, s.acting)
		for _, i := range order {
			x := s.members[i]
			if x.component == waived {
				continue
			}
			if mark := x.mark(b - 1); x.settled() < mark {
				return s.wait(me, b, x, b-1, mark)
			}
		}
	}
	if s.firstSide != "" && me.component == pairingPeer(s.firstSide) {
		for _, x := range s.members {
			if x.component != s.firstSide || x.component == waived {
				continue
			}
			if mark := x.mark(b); x.settledForTheOtherSide() < mark {
				return s.wait(me, b, x, b, mark)
			}
		}
	}
	return "", ""
}

// wait names the acting member's wait on batch `on` of member x, whose
// settled count is short of mark: under the Ratio gate with the serving
// count, as "engine batch 2 waits for decoder batch 1 to serve (0 of 1
// serving)", with "to serve first" when the batch is the member's own,
// the first-side wait. When x is the named first side and its retry
// record is Held, nothing of x will serve until the operator acts, so the
// wait reads under the Held gate and names the side and its record.
func (s batchStep) wait(me batchMember, b int32, x batchMember, on, mark int32) (v1beta1.RolloutHoldGate, string) {
	first := ""
	if on == b {
		first = " first"
	}
	text := fmt.Sprintf("%s batch %d waits for %s batch %d to serve%s (%d of %d serving%s%s%s)",
		me.component, b, x.component, on, first, x.promoted, mark, failedNote(x), movableNote(x), waitsNote(x))
	if x.heldReason != "" && x.component == s.firstSide && x.component != me.component {
		return v1beta1.RolloutHoldGateHeld, fmt.Sprintf("%s, and the %s is held: %s; a corrected revision, or a release of the held revision on the %s, resumes both sides",
			text, x.component, x.heldReason, x.component)
	}
	return v1beta1.RolloutHoldGateRatio, text
}

// failedNote names a member's ended attempts in a hold reason.
func failedNote(m batchMember) string {
	if m.ended > 0 {
		return fmt.Sprintf(", %d failed", m.ended)
	}
	return ""
}

// waitsNote names, in a hold reason, the member's Instances whose attempt
// waits on the cluster: "; decoder Instance 0 Unschedulable, starts again".
func waitsNote(m batchMember) string {
	if len(m.waits) == 0 {
		return ""
	}
	return "; " + strings.Join(m.waits, ", ")
}

// movableNote names the Instances a member's partition lets the roll move
// in a hold reason, when it holds any.
func movableNote(m batchMember) string {
	if m.held > 0 {
		return fmt.Sprintf(", %d of %d movable", m.movable(), m.desired)
	}
	return ""
}

// readBatchMember classifies a replica's Instances against its roll target.
// The step is the member's own budget of its kind capped by the group's, as
// the budget gates resolve it. An Instance the partition holds is counted
// as held and nowhere else. A member with no roll target has nothing to
// move and reads as settled. The member's Held retry record for the
// target, when one stands, travels with it. An ended attempt counts as
// ended only while a retry record for the target stands; otherwise the
// member starts it again at once and the Instance is still to start.
func readBatchMember(ir *v1beta1.InferenceReplica, comp v1beta1.ComponentType, target string, pacing v1beta1.CoordinationPacing, kind stepKind) batchMember {
	m := batchMember{component: comp, target: target}
	if kind == stepSurge {
		m.desired, m.step = resolveSurgeStep(ir, pacing)
	} else {
		floor := resolvePairFloor(ir, pacing)
		m.desired, m.step = floor.desired, floor.maxUnavailable
	}
	if target == "" {
		m.promoted = m.desired
		return m
	}
	m.heldReason = heldUpdateReason(ir, target)
	rows := ir.Status.InstanceStatuses
	held := partitionHeldRows(ir, rows, target)
	classes := make([]batchRowClass, len(rows))
	for i := range rows {
		if held[rows[i].Index] {
			m.held++
			m.offTarget++
			continue
		}
		classes[i] = classifyBatchRow(&rows[i], target)
		if classes[i] == batchRowPromoted {
			m.promoted++
			continue
		}
		if classes[i] == batchRowOpen {
			m.open++
		}
		m.offTarget++
	}
	recorded := retryRecordStands(ir, target)
	for i := range rows {
		if classes[i] == batchRowEnded && recorded {
			m.ended++
			continue
		}
		if note, ok := rowWaitNote(&rows[i], comp, classes[i]); ok {
			m.waits = append(m.waits, note)
		}
	}
	return m
}

// retryRecordStands reports whether a retry record for the target stands
// on the member, in any of its states: the schedule counted a failure
// there and owns the next start of an ended attempt. With no record the
// member's next pass starts the attempt again at once.
func retryRecordStands(ir *v1beta1.InferenceReplica, target string) bool {
	if ir == nil {
		return false
	}
	_, ok := irprojector.RetryBlockFor(&ir.Status, target)
	return ok
}

// attemptStoppedWithItsPods reports a row whose attempt the escalation
// stopped while its pod set still ran and kept on the row at the step it
// records for that: an attempt that ended without serving, read like a
// Failed row.
// Whether it settles its batch is the retry record's to say, as for every
// ended attempt; the operator's release of a Held record, or a promote on
// the side that pruned it, leaves the row with none and still to start.
func attemptStoppedWithItsPods(row *v1beta1.OMENativeInstanceStatus) bool {
	op := row.Operation
	return op != nil && op.Type == v1beta1.InstanceOperationUpdate && op.Step == workloadtypes.UpdateStepParked
}

// rowWaitNote names an Instance whose attempt waits on the cluster: an
// open attempt with a waiting token ("decoder Instance 0 Unschedulable"),
// or an ended attempt the member starts again at once, with the reason
// its last failure recorded ("decoder Instance 0 Unschedulable, starts
// again"). The token alone is named, never the cluster's message, which
// moves from pass to pass and would churn the hold.
func rowWaitNote(row *v1beta1.OMENativeInstanceStatus, comp v1beta1.ComponentType, class batchRowClass) (string, bool) {
	switch {
	case class == batchRowOpen && row.Operation != nil && row.Operation.Waiting != "":
		return fmt.Sprintf("%s Instance %d %s", comp, row.Index, row.Operation.Waiting), true
	case class == batchRowEnded:
		if row.LastFailure != nil && row.LastFailure.Reason != "" {
			return fmt.Sprintf("%s Instance %d %s, starts again", comp, row.Index, row.LastFailure.Reason), true
		}
		return fmt.Sprintf("%s Instance %d starts again", comp, row.Index), true
	}
	return "", false
}

// heldUpdateReason is the text of a replica's Held retry record for the
// revision its roll moves Instances to; empty when no such record stands.
func heldUpdateReason(ir *v1beta1.InferenceReplica, target string) string {
	if ir == nil {
		return ""
	}
	block, ok := irprojector.HeldRetryBlock(&ir.Status, target)
	if !ok {
		return ""
	}
	return irprojector.HeldUpdateReason(block)
}

// partitionHeldRows is the set of a replica's Instances its partition holds
// on their revision, read as the roll reads it: the effective partition off
// the replica's spec (irprojector.IRPartition) and the lowest-indexed
// Instances still off the target that no attempt is moving
// (query.PartitionHeldIndices), each row decoded as the engine holds it.
func partitionHeldRows(ir *v1beta1.InferenceReplica, rows []v1beta1.OMENativeInstanceStatus, target string) map[int32]bool {
	partition := irprojector.IRPartition(ir)
	if partition <= 0 {
		return nil
	}
	observed := make([]*workloadtypes.InstanceStatus, 0, len(rows))
	for i := range rows {
		s := v1beta1convert.InstanceStatusToWorkload(rows[i])
		observed = append(observed, &s)
	}
	return query.PartitionHeldIndices(&partition, observed, target)
}

// resolveSurgeStep is a surging member's step: its own rollingUpdate.maxSurge
// against its desired count, capped by the group's surge budget, the way the
// surge gate resolves it. Zero when the replica records no count.
func resolveSurgeStep(ir *v1beta1.InferenceReplica, pacing v1beta1.CoordinationPacing) (desired, step int32) {
	desired = effectiveDesiredReplicas(ir)
	if desired <= 0 {
		return 0, 0
	}
	step = MaxSurgeBudget(pacing, desired)
	if lc := ir.Spec.Lifecycle; lc != nil && lc.UpdateStrategy != nil && lc.UpdateStrategy.RollingUpdate != nil && lc.UpdateStrategy.RollingUpdate.MaxSurge != nil {
		if own := utils.ScaledCountFromIntOrString(lc.UpdateStrategy.RollingUpdate.MaxSurge, desired, true); own < step {
			step = own
		}
	}
	return desired, step
}

// effectiveDesiredReplicas is the count the replica's workload runs toward:
// spec.replicas, held at the placement replica limit while a placement surge
// pause defers growth, the cap desiredFromIR applies. An autoscaler can raise
// spec.replicas above the limit, and no Instance is created for the excess,
// so a batch counted against it could never settle. Zero when unresolved.
func effectiveDesiredReplicas(ir *v1beta1.InferenceReplica) int32 {
	if ir == nil || ir.Spec.Replicas == nil || *ir.Spec.Replicas <= 0 {
		return 0
	}
	desired := *ir.Spec.Replicas
	if policy := ir.Spec.PlacementExecution; policy != nil && policy.PauseSurge && ir.Spec.PlacementReplicaLimit != nil {
		desired = min(desired, *ir.Spec.PlacementReplicaLimit)
	}
	return max(desired, 0)
}

type batchRowClass int

const (
	batchRowUntouched batchRowClass = iota
	batchRowPromoted
	batchRowOpen
	batchRowEnded
)

// classifyBatchRow reads what the roll has done to one Instance. It is
// promoted when it runs the target with no attempt pinned elsewhere, the
// reading of the UpdatedReplicas counter. An attempt toward the target stays
// on the row from its first stamp to the promote: a Failed row or a parked
// attempt ended without serving, any other is open. The phase is read as
// the engine holds it (v1beta1convert.InstanceStatusToWorkload): the status
// publication writes Ready over a failed replacement while the old pod it
// was to replace still serves, and that attempt ended all the same. A
// Create or Migrate on the target is a new Instance, not a start of this
// roll, and the slot a gang surge builds its replacement in belongs to the
// source row's start.
func classifyBatchRow(row *v1beta1.OMENativeInstanceStatus, target string) batchRowClass {
	if target == "" {
		return batchRowUntouched
	}
	op := row.Operation
	update := op != nil && op.Type == v1beta1.InstanceOperationUpdate
	if update && (op.Step == workloadtypes.UpdateStepGangSurgeTarget || op.Step == workloadtypes.UpdateStepGangSurgeTargetCleanup) {
		return batchRowUntouched
	}
	pinnedElsewhere := update && op.TargetRevision != "" && op.TargetRevision != target
	if row.RunningRevision == target && !pinnedElsewhere {
		return batchRowPromoted
	}
	toward := row.TargetRevision == target || (update && op.TargetRevision == target)
	if !toward || (op != nil && !update) {
		return batchRowUntouched
	}
	phase := v1beta1convert.InstanceStatusToWorkload(*row).Phase
	if phase == workloadtypes.InstancePhaseFailed || attemptStoppedWithItsPods(row) {
		return batchRowEnded
	}
	return batchRowOpen
}

// drainsFirst reports whether a replica's update takes an Instance out of
// rotation before anything returns: RecreatePod, or an in-place strategy on
// a multi-pod Instance, which rebuilds the whole gang. An unset strategy is
// the SurgeThenDrain default; a shape with no stored runners reads as
// single-pod.
func drainsFirst(ir *v1beta1.InferenceReplica) bool {
	if ir.Spec.Lifecycle == nil || ir.Spec.Lifecycle.UpdateStrategy == nil {
		return false
	}
	switch ir.Spec.Lifecycle.UpdateStrategy.Type {
	case v1beta1.UpdateStrategyRecreatePod:
		return true
	case v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategyInPlaceOnly:
		var pods int32
		for _, r := range ir.Spec.Runners {
			pods += r.Size
		}
		return pods > 1
	}
	return false
}

// rollTarget is the revision a replica moves its Instances to: the rollback
// revision while one is pinned, else its update revision.
func rollTarget(ir *v1beta1.InferenceReplica) string {
	if p := ir.Spec.Pacing; p != nil && p.RollbackToRevision != nil && *p.RollbackToRevision != "" {
		return *p.RollbackToRevision
	}
	return ir.Status.UpdateRevision
}

// peerStatusPredatesPush reports a peer replica whose recorded roll target
// may still be the previous push's: its status was not written for the
// pod templates its spec holds (StatusObservedTemplates), or its spec was
// projected from another service generation than the acting replica's
// (the parent-generation record the projector writes on both). A spec
// write that changes no template leaves the peer current. Two replicas
// with no record are compared on their status alone. A missing peer
// predates nothing.
func peerStatusPredatesPush(peer, acting *v1beta1.InferenceReplica) bool {
	if peer == nil {
		return false
	}
	if !StatusObservedTemplates(peer) {
		return true
	}
	if acting == nil {
		return false
	}
	peerGeneration, peerRecorded := peer.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	actingGeneration, actingRecorded := acting.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	return peerRecorded && actingRecorded && peerGeneration != actingGeneration
}

// notYetObserved is the member as the batch step reads it while its status
// predates the push: its count and step stand, nothing is started toward
// the target, every Instance reads off it, and no Instance reads held, so
// a peer waits on its first batch until the status records the push.
func (m batchMember) notYetObserved() batchMember {
	return batchMember{component: m.component, desired: m.desired, step: m.step, target: m.target, offTarget: m.desired}
}
