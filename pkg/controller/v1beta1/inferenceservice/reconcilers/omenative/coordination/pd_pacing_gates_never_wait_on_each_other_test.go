package coordination

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils"
)

// This file drives the full gate stack (EvaluateUpdateGate and
// EvaluatePairingDrain) over a small model of one prefill/decode rollout
// group and checks, at every reachable state, that the two sides never
// wait on each other, that neither waits on itself, that the budgets hold,
// that the batches stay within one planned batch of each other, and that
// every run ends with the roll complete or the retry ladder spent.
//
// The model is the replica rows as the status publishes them. The world
// admits the starts the gates allow, the way the dispatcher does (one pass
// per Component, every admitted start stamped on its row before the next
// question, the per-Component budget and the retry ladder in front of the
// gate, the Instances a partition holds never offered), then makes one
// move: a started Instance serves, an attempt fails (a drain that leaves
// nothing serving, or a surge published Ready over its serving old pod),
// an attempt is held by the engine, a surge hand-over is kept by the pairing drain
// gate during a protocol change, the ladder's backoff ends or its record
// is pruned, the ladder is spent (the replica publishing the record, so
// the gate reads a Held named side), a gang loses a member while its
// replacement is building or standing beside it or while it stands old or
// promoted (the repair refills it), a side's partition is
// raised, lowered or cleared (on one side or both), a side is scaled
// (one side, both sides by the same step, or both to a new ratio), with
// every plan read again from the counts the spec asks for and the group's
// ratio reference rewritten from them by the production service pass, a
// move of its own (huntState.reference), or a canary
// unit's step advances, each side's replica carrying the step's partition
// (batch_step_model_test.go widens the configurations to the reference
// shapes and reads the two sides' shares against the tolerance). A roll may
// also start with one side's status, or both, predating the push: the
// replica still records the previous roll target with every Instance
// promoted on it, until that side's own pass writes its status, and that
// write wakes the other side's pass as the replica controller's peer
// watch does. Every path is
// explored to a bound with a hash of visited states, the operator's moves
// are also tried at each moment of a plain roll (before the first pass,
// settling, rotating, draining, the last Instance, a side Held) with the
// roll then played to its end, and seeded random runs go deeper; the
// reference shapes are always among the configurations and the rest are
// sampled by a fixed seed. OME_PD_PACING_HUNT=full widens the sample and
// the bounds.

// The operator's prefill/decode settings (keepRatio and firstSide) live on
// GroupDefaults.PrefillDecode; huntRules builds them beside the default
// tolerance. A surge whose replacement failed while its old pod serves is
// published Ready with its target kept and no operation; the batch step
// reads that row as an attempt that ended, so the retry ladder can re-open
// it, and the explorer stages that failure as a move.

// huntRules is the operator's prefill/decode settings beside the default
// tolerance.
func huntRules(keepRatio bool, tolerance *int32, firstSide v1beta1.ComponentType) GroupDefaults {
	return GroupDefaults{
		RatioTolerancePercent: tolerance,
		PrefillDecode:         PrefillDecodeRules{KeepRatio: keepRatio, FirstSide: firstSide},
	}
}

// huntRulesOf reads the two settings back off the defaults.
func huntRulesOf(d GroupDefaults) (keepRatio bool, firstSide v1beta1.ComponentType) {
	return d.PrefillDecode.KeepRatio, d.PrefillDecode.FirstSide
}

// huntRow is what the roll has done to one Instance, as its row reads.
type huntRow uint8

const (
	huntOld                huntRow = iota // Ready on the old revision, serving
	huntOpenDrain                         // a recreate toward the target, nothing serving
	huntOpenInPlace                       // an in-place patch toward the target, out of rotation
	huntOpenSurge                         // a surge replacement starting beside the serving source
	huntReplacementHeld                   // the replacement serves beside the source; the drain gate keeps the source
	huntPromoted                          // Ready on the target, serving
	huntFailedUnserving                   // a drain-first attempt ended Failed; nothing serves; no operation
	huntFailedSurge                       // a surge ended Failed; published Ready over the serving source, target kept
	huntRetriesStopped                    // the retries stopped; the old pod serves
	huntCreating                          // created on the target by a scale-up, not serving yet
	huntFailedGangKept                    // a gang surge ended Failed; the source keeps its attempt at Surge while the replacement gang is retired
	huntSurgeShort                        // a gang surge whose source lost a worker while the replacement builds; the leader still serves
	huntReplacementShort                  // a kept old gang whose replacement lost a worker; the source still serves
	huntPromotedUncounted                 // promoted onto the target in one status write, its rebuilt pods not yet counted as serving until the next
	huntOldShort                          // an old gang short of a worker while the repair refills it; the leader serves
	huntPromotedShort                     // a promoted gang short of a worker while the repair refills it; the leader serves
	huntOldLostMember                     // an old gang short of a worker the repair left to the roll; the leader serves; the roll's next start
	huntOldUnreported                     // an old gang with a worker on a node that stopped reporting; the pod stands with its Ready revoked; the leader serves
	huntOldLostNode                       // the same once the taint manager evicted the pod, which stays Terminating on the lost node
	huntDrainBlocked                      // a recreate toward the target waiting on a Terminating pod no kubelet removes; nothing serves
	huntSurgeLostNode                     // a gang surge whose source has a pod on a lost node; the leader serves while the replacement builds
	huntSourceDrainBlocked                // the replacement serves; the source's drain waits on a Terminating pod no kubelet removes
	huntPromotedUnreported                // a promoted gang with a worker on a node that stopped reporting; the leader serves
	huntPromotedLostNode                  // the same once evicted: the repair's rebuild waits on the Terminating pod; nothing serves
	huntDrainRefused                      // a recreate whose member the scheduler cannot place; nothing serves
	huntSurgeRefused                      // a surge whose replacement member the scheduler cannot place; the source serves
	huntCreatingRefused                   // a created Instance the scheduler cannot place
	huntDrainBack                         // a recreate a rollback turned around: its pods are rebuilt on the revision the Instance last ran, the roll's target again; nothing serves
)

var huntRowNames = [...]string{"old", "drain", "inplace", "surge", "handover", "new", "failedunserving", "failedsurge", "parked", "creating", "failedgangkept", "surgeshort", "replacementshort", "newuncounted", "oldshort", "newshort",
	"oldlost", "oldunreported", "oldlostnode", "drainblocked", "surgelostnode", "sourcedrainblocked", "newunreported", "newlostnode", "drainrefused", "surgerefused", "creatingrefused", "drainback"}

func (r huntRow) String() string { return huntRowNames[r] }

func (r huntRow) serving() bool {
	switch r {
	case huntOld, huntOpenSurge, huntReplacementHeld, huntPromoted, huntFailedSurge, huntRetriesStopped, huntFailedGangKept, huntSurgeShort, huntReplacementShort, huntOldShort, huntPromotedShort,
		huntOldLostMember, huntOldUnreported, huntOldLostNode, huntSurgeLostNode, huntPromotedUnreported, huntSurgeRefused:
		return true
	}
	return false
}

// shortOfAWorker reports a gang whose leader serves and routes, so the
// pairing gate reads the Instance serving, while the replica's serving
// count, which needs every pod of the gang, leaves it out: one the repair
// is refilling, one the repair left to the roll, one with a pod on a node
// that stopped reporting, or a surge source short of a pod on such a node.
func (r huntRow) shortOfAWorker() bool {
	switch r {
	case huntOldShort, huntPromotedShort, huntOldLostMember, huntOldUnreported, huntOldLostNode, huntSurgeLostNode, huntPromotedUnreported:
		return true
	}
	return false
}

// lostNode reports a row with a pod on a node that stopped reporting,
// evicted or not: the Instance is short until a force delete frees the pod.
func (r huntRow) lostNode() bool {
	switch r {
	case huntOldUnreported, huntOldLostNode, huntDrainBlocked, huntSurgeLostNode, huntSourceDrainBlocked, huntPromotedUnreported, huntPromotedLostNode:
		return true
	}
	return false
}

// shortOld reports an old gang short of a worker the repair left to the
// roll, with the lost pod gone or on a lost node: the roll's start on it
// rebuilds the gang onto the target.
func (r huntRow) shortOld() bool {
	return r == huntOldLostMember || r == huntOldUnreported || r == huntOldLostNode
}

// partitionCandidate reports a row a partition may hold: off the target
// with no update attempt the roll must finish, as the roll's own rule
// reads it (an old row, a failed or parked attempt, a created Instance
// with no running revision yet).
func (r huntRow) partitionCandidate() bool {
	switch r {
	case huntOld, huntFailedUnserving, huntFailedSurge, huntRetriesStopped, huntCreating, huntOldShort, huntOldLostMember, huntOldUnreported, huntOldLostNode, huntCreatingRefused:
		return true
	}
	return false
}

// claimsUpdate reports a row the retry ladder and the surge budgets read
// as an attempt in flight: an open attempt, or a gang source keeping its
// attempt while its failed replacement is retired.
func (r huntRow) claimsUpdate() bool { return r.open() || r == huntFailedGangKept }

// ended reports an update attempt that failed or stopped retrying: the
// row keeps its target with no open attempt, or a gang source keeps its
// attempt while its failed replacement is retired.
func (r huntRow) ended() bool {
	switch r {
	case huntFailedUnserving, huntFailedSurge, huntRetriesStopped, huntFailedGangKept:
		return true
	}
	return false
}

// surgeSlot reports a row that holds a surge slot: an open surge, a kept
// hand-over, a gang source keeping its attempt, and, for a single pod, a
// failed surge whose extra pod still stands.
func (r huntRow) surgeSlot(gang bool) bool {
	switch r {
	case huntOpenSurge, huntReplacementHeld, huntFailedGangKept, huntSurgeShort, huntReplacementShort, huntSurgeLostNode, huntSourceDrainBlocked, huntSurgeRefused:
		return true
	case huntFailedSurge:
		return !gang
	}
	return false
}

// surgeTarget reports a row whose gang surge has a replacement row of its
// own in the replica.
func (r huntRow) surgeTarget() bool {
	switch r {
	case huntOpenSurge, huntReplacementHeld, huntFailedGangKept, huntSurgeShort, huntReplacementShort, huntSurgeLostNode, huntSourceDrainBlocked, huntSurgeRefused:
		return true
	}
	return false
}

// open reports an update attempt in flight: the row carries an operation
// that neither failed nor parked.
func (r huntRow) open() bool {
	switch r {
	case huntOpenDrain, huntOpenInPlace, huntOpenSurge, huntReplacementHeld, huntSurgeShort, huntReplacementShort,
		huntDrainBlocked, huntSurgeLostNode, huntSourceDrainBlocked, huntDrainRefused, huntSurgeRefused, huntDrainBack:
		return true
	}
	return false
}

// openDrain reports a drain-first attempt in flight, the per-Component
// unavailability budget's count: a recreate or an in-place patch, one
// waiting on a lost node's pod or on the scheduler among them.
func (r huntRow) openDrain() bool {
	switch r {
	case huntOpenDrain, huntOpenInPlace, huntDrainBlocked, huntDrainRefused, huntDrainBack:
		return true
	}
	return false
}

// huntSchedule is the state of the Component's retry record for the target.
type huntSchedule uint8

const (
	scheduleNone     huntSchedule = iota
	scheduleBackoff               // a failure was recorded; no fresh start until the backoff ends
	scheduleDue                   // the backoff ended; the next pass admits a wave of starts
	scheduleRetrying              // RetryInProgress: one wave at a time while an attempt is in flight
	scheduleHeld                  // the ladder is spent; no fresh start until a new revision
)

var huntScheduleNames = [...]string{"", "backoff", "due", "retrying", "held"}

func (l huntSchedule) String() string { return huntScheduleNames[l] }

// huntMember is one Component of the group: its count, how it rolls, and
// its own budget (maxSurge for a surging member, maxUnavailable otherwise).
type huntMember struct {
	comp     v1beta1.ComponentType
	desired  int32
	strategy v1beta1.UpdateStrategyType
	budget   string
	// unavailable is a surging member's own maxUnavailable beside its surge
	// budget, the pairable minimum the pairing gates hold it to; empty leaves the
	// group's default.
	unavailable string
	// gang is the pods per Instance: a leader and gang-1 workers above one.
	gang int32
}

func (m huntMember) surges() bool { return m.strategy == v1beta1.UpdateStrategySurgeThenDrain }

// pods is the pods of one Instance, at least one.
func (m huntMember) pods() int32 {
	if m.gang > 1 {
		return m.gang
	}
	return 1
}

// isGang reports a multi-pod Instance: an in-place strategy rebuilds it
// whole, a surge builds the replacement gang in a slot of its own.
func (m huntMember) isGang() bool { return m.gang > 1 }

func (m huntMember) inPlace() bool {
	return m.strategy == v1beta1.UpdateStrategyInPlaceIfPossible || m.strategy == v1beta1.UpdateStrategyInPlaceOnly
}

func (m huntMember) short() string {
	kind := "drain"
	switch {
	case m.surges():
		kind = "surge"
	case m.inPlace():
		kind = "inplace"
	}
	if m.isGang() {
		kind += fmt.Sprintf("/gang%d", m.gang)
	}
	return kind
}

// huntSide is one member's rows, ladder and partition, with the
// adversary's remaining moves on it.
type huntSide struct {
	rows      []huntRow
	schedule  huntSchedule
	attempts  int32
	partition int32 // the Instances the operator keeps on their revision, zero for none
	// behind is a replica status that predates the push: it still records
	// the previous roll target, every Instance promoted on it, until the
	// side's own pass writes its status.
	behind bool
	fails  int8
	parks  int8
	scales int8
	parts  int8 // partition changes left
	losses int8 // gang members that may still be lost mid-surge
	// refills is how many gang members may still be lost on an Instance
	// that is not surging (an old or a promoted gang), each refilled by the
	// repair, or left to the roll when the roll may take the Instance.
	refills int8
	// refusals is how many attempts or created Instances the scheduler may
	// still refuse to place a member of.
	refusals int8
	// lostNodes is how many old or promoted gangs may still have a worker's
	// node stop reporting.
	lostNodes int8
	// added and removed count the Instances scale-ups created and
	// scale-downs retired, so a reading can tell the roll's own starts from
	// the created Instances the batch step reads as promoted.
	added, removed int32
	// rebuilt counts the Instances the roll rebuilt onto the target with no
	// gate asked: a gang short of a worker the repair left to it.
	rebuilt int32
	// expired counts the attempts the placement grace ended with the retry
	// record left alone.
	expired int32
	// followed counts the scales the scaling coordinator wrote on the side
	// as the follower of the other side's count.
	followed int32
	// exempt marks, by index, the drains open on Instances the group's
	// serving count already left out, opened with no gate asked: the
	// group's unavailability budget is not asked for them by design.
	exempt uint64
	// canaryPartition is the partition the canary's current step projects
	// on the side (spec.pacing.partition), -1 when no canary runs. It
	// outranks the operator's partition, as the replica's reader does.
	canaryPartition int32
	// reversed is true once a rollback turned the roll around: the rows
	// read against the revision the roll left, which is its target again.
	reversed bool
	// revert is true once the canary's rollback signal names that revision
	// on the replica's pacing while its update revision stays the pushed
	// one.
	revert bool
	// inherited counts the Instances a rollback found already on the
	// revision the roll returns to: the previous roll left them there, up
	// to its pairable minimum without a partner, so they are not the turned
	// roll's replacements.
	inherited int32
	// pushed is true once the operator pushed a further revision while the
	// roll was open (huntConfig.repush): the roll target is the final
	// revision and the revision the first push named is the middle one.
	pushed bool
	// sourceMiddle marks, by index, a row whose own pods run the middle
	// revision: an Instance the first push had promoted, which the further
	// push moves again. attemptMiddle marks a row whose open attempt stays
	// on the middle revision after the further push: a surge whose
	// serving replacement the drain gate keeps in rotation, or a stopped
	// attempt. Both are zero while no further revision was pushed.
	sourceMiddle, attemptMiddle uint64
}

// srcMiddle reports a row whose own pods run the middle revision.
func (s huntSide) srcMiddle(idx int) bool { return idx < 64 && s.sourceMiddle&(1<<idx) != 0 }

// attemptOnMiddle reports a row whose attempt stays on the middle revision.
func (s huntSide) attemptOnMiddle(idx int) bool { return idx < 64 && s.attemptMiddle&(1<<idx) != 0 }

func (s *huntSide) setSrcMiddle(idx int, on bool) { s.sourceMiddle = setBit(s.sourceMiddle, idx, on) }

func (s *huntSide) setAttemptOnMiddle(idx int, on bool) {
	s.attemptMiddle = setBit(s.attemptMiddle, idx, on)
}

func setBit(mask uint64, idx int, on bool) uint64 {
	if idx >= 64 {
		return mask
	}
	if on {
		return mask | 1<<idx
	}
	return mask &^ (1 << idx)
}

// keepMiddleBelow drops the revision marks of the rows a scale-down
// removed.
func (s *huntSide) keepMiddleBelow(n int) {
	if n >= 64 {
		return
	}
	mask := uint64(1)<<n - 1
	s.sourceMiddle &= mask
	s.attemptMiddle &= mask
}

// exemptOpen counts the open drains the group's unavailability gate was
// not asked for.
func (s huntSide) exemptOpen() int32 {
	var n int32
	for idx, r := range s.rows {
		if idx < 64 && s.exempt&(1<<idx) != 0 && r.openDrain() {
			n++
		}
	}
	return n
}

// exemptInFlight counts the attempts in flight no gate was asked for,
// whatever their mechanism: a short gang's rebuild as a drain, an in-place
// patch or a surge.
func (s huntSide) exemptInFlight() int32 {
	var n int32
	for idx, r := range s.rows {
		if idx < 64 && s.exempt&(1<<idx) != 0 && r.claimsUpdate() {
			n++
		}
	}
	return n
}

// inFlightMask is the set of indices whose row is an attempt in flight,
// whatever its mechanism.
func (s huntSide) inFlightMask() uint64 {
	var mask uint64
	for idx, r := range s.rows {
		if idx < 64 && r.claimsUpdate() {
			mask |= 1 << idx
		}
	}
	return mask
}

func (s huntSide) count(kinds ...huntRow) int32 {
	var n int32
	for _, r := range s.rows {
		for _, k := range kinds {
			if r == k {
				n++
			}
		}
	}
	return n
}

func (s huntSide) inFlight() bool {
	for _, r := range s.rows {
		if r.claimsUpdate() {
			return true
		}
	}
	return false
}

func (s huntSide) complete() bool {
	for _, r := range s.rows {
		if r != huntPromoted {
			return false
		}
	}
	return true
}

// held is the set of Instances the side's partition keeps on their
// revision, by the roll's own rule (query.PartitionHeldIndices): the
// lowest-indexed rows off the target that carry no attempt the roll must
// finish, as many as the partition names. After a further push an
// Instance promoted onto the middle revision is off the target too.
func (s huntSide) held() map[int]bool {
	out := map[int]bool{}
	partition := s.effectivePartition()
	if partition <= 0 {
		return out
	}
	var n int32
	for idx, r := range s.rows {
		if n >= partition {
			break
		}
		if r.partitionCandidate() || (r == huntPromotedUncounted && s.srcMiddle(idx)) {
			out[idx] = true
			n++
		}
	}
	return out
}

// effectivePartition is the partition the side rolls under: the canary
// step's while a canary runs, else the operator's, the order the replica's
// reader keeps (irprojector.EffectivePartition).
func (s huntSide) effectivePartition() int32 {
	if s.canaryPartition >= 0 {
		return s.canaryPartition
	}
	return s.partition
}

// done reports a side the roll has nothing left to do on: every Instance
// is promoted or held by the partition.
func (s huntSide) done() bool {
	held := s.held()
	for idx, r := range s.rows {
		if r != huntPromoted && !held[idx] {
			return false
		}
	}
	return true
}

func (s huntSide) String() string {
	parts := make([]string, 0, len(s.rows))
	for idx, r := range s.rows {
		// A row on the middle revision carries a prime ("old'"), an attempt
		// held to it a caret.
		text := r.String()
		if s.srcMiddle(idx) {
			text += "'"
		}
		if s.attemptOnMiddle(idx) {
			text += "^"
		}
		parts = append(parts, text)
	}
	out := strings.Join(parts, ",")
	if s.schedule != scheduleNone {
		out += fmt.Sprintf(" schedule=%s/%d", s.schedule, s.attempts)
	}
	if s.partition > 0 {
		out += fmt.Sprintf(" partition=%d", s.partition)
	}
	if s.canaryPartition >= 0 {
		out += fmt.Sprintf(" canaryPartition=%d", s.canaryPartition)
	}
	if s.behind {
		out += " statusBehind"
	}
	if s.revert {
		out += " canaryRolledBack"
	} else if s.reversed {
		out += " rolledBack"
	}
	return out
}

// huntHold is the first denial a side met in its last pass.
type huntHold struct {
	gate   v1beta1.RolloutHoldGate
	reason string
}

func (h *huntHold) String() string {
	if h == nil {
		return "none"
	}
	return fmt.Sprintf("%s: %s", h.gate, h.reason)
}

// huntEditKind is what the operator changed in the rollout block while the
// run is open; the run keeps the plan it recorded.
type huntEditKind uint8

const (
	huntEditNone          huntEditKind = iota
	huntEditTolerance                  // the group's maintainRatio tolerance set (the rule added when none stood)
	huntEditRuleRemoved                // the group's maintainRatio removed
	huntEditGroupBudget                // the group's maxUnavailable and maxSurge lowered to one
	huntEditGroupRemoved               // every rollout group removed
	huntEditMemberAdded                // the router added to the group's member list
	huntEditMemberRemoved              // the decoder removed from the group's member list
)

// huntEdit is the operator's edit of the rollout block in force, read by
// the gates from the spec while the run keeps its recorded plan.
type huntEdit struct {
	kind      huntEditKind
	tolerance int32
}

func (e huntEdit) String() string {
	switch e.kind {
	case huntEditTolerance:
		return fmt.Sprintf("tolerance to %d", e.tolerance)
	case huntEditRuleRemoved:
		return "rule removed"
	case huntEditGroupBudget:
		return "group budget to 1"
	case huntEditGroupRemoved:
		return "group removed"
	case huntEditMemberAdded:
		return "router added to the group"
	case huntEditMemberRemoved:
		return "decoder removed from the group"
	}
	return "none"
}

// huntState is the model at one moment: every side, the holds of the last
// admission, the operator's edit of the rollout block in force and the
// edits still to try, the moves that led here, and the first start the
// last admission let the other side open before the side named to move
// first had settled the batch, when it did.
type huntState struct {
	sides     []huntSide
	holds     []*huntHold
	edit      huntEdit
	edits     int8
	path      []huntMove
	outOfTurn string
	// canaryStep is the canary's current step, counted from zero, the step
	// count once the canary is done; -1 when no canary runs.
	canaryStep int32
	// reference is the group's recorded ratio reference
	// (status.rolloutCoordination.groups[].observedRatio.original) as the
	// service pass last wrote it; nil for a group with no ratio rule. The
	// service pass is a move of its own, so after a resize the gates read
	// the reference one pass behind the counts until it runs.
	reference map[v1beta1.ComponentType]int32
	// referenceOff reports a service pass that wrote a reference other than
	// the counts the specs ask for.
	referenceOff bool
	// events are the service events the gates raised during the last
	// admission.
	events []string
	// policy is the proportional scaling policy in force, policyEdits the
	// operator's edits of it still to try (ratio_policy_model_test.go).
	policy      huntPolicy
	policyEdits int8
	// paused is true while the operator's pause annotation stands: no
	// pass runs and no attempt advances until the resume.
	paused bool
	// rollbacks, pauses and canaryRollbacks are the operator's rollbacks,
	// pauses and canary rollbacks still to try.
	rollbacks, pauses, canaryRollbacks int8
	// rollbackGap is how many batches apart the two sides stood when a
	// rollback turned the roll around, as the batch step counts them: the
	// gap the previous roll left, which the batch promise after a rollback
	// excuses and no wider.
	rollbackGap int32
	// pushed is true once the operator pushed the further revision
	// (huntConfig.repush); every side carries the same flag for its rows.
	pushed bool
	// pairingBreach is the pairable-capacity reading of the last move, when
	// the gates let a step take a side below its pairable minimum or leave
	// the fleet with no pairable pair (pairing_gate_model_test.go); the
	// class names what admitted the step.
	pairingBreach *huntFinding
}

// reversed reports a roll a rollback turned around.
func (st *huntState) reversed() bool {
	return len(st.sides) > 0 && st.sides[0].reversed
}

func (st *huntState) clone() *huntState {
	out := &huntState{sides: make([]huntSide, len(st.sides)), holds: make([]*huntHold, len(st.holds)), edit: st.edit, edits: st.edits, path: append([]huntMove(nil), st.path...), canaryStep: st.canaryStep, reference: maps.Clone(st.reference), referenceOff: st.referenceOff,
		policy: st.policy, policyEdits: st.policyEdits, paused: st.paused, rollbacks: st.rollbacks, pauses: st.pauses, canaryRollbacks: st.canaryRollbacks, rollbackGap: st.rollbackGap, pushed: st.pushed}
	for i, s := range st.sides {
		s.rows = append([]huntRow(nil), s.rows...)
		out.sides[i] = s
	}
	return out
}

func (st *huntState) key() string {
	var b strings.Builder
	for _, s := range st.sides {
		for _, r := range s.rows {
			b.WriteByte(byte('A' + r))
		}
		fmt.Fprintf(&b, "|%d%d%d%d%d%d%d%d%t%d.%d.%d.%d;%d.%d.%d.%d.%d.%x%t%t%d", s.schedule, s.attempts, s.fails, s.parks, s.scales, s.partition, s.parts, s.losses, s.behind, s.refills, s.added, s.removed, s.canaryPartition,
			s.refusals, s.lostNodes, s.rebuilt, s.expired, s.followed, s.exempt, s.reversed, s.revert, s.inherited)
		if s.sourceMiddle != 0 || s.attemptMiddle != 0 {
			fmt.Fprintf(&b, "m%x.%x", s.sourceMiddle, s.attemptMiddle)
		}
	}
	fmt.Fprintf(&b, "c%d", st.canaryStep)
	fmt.Fprintf(&b, "p%t%d.%d.%d.%d", st.paused, st.rollbacks, st.pauses, st.canaryRollbacks, st.rollbackGap)
	if st.pushed {
		b.WriteString("P")
	}
	if st.reference != nil {
		b.WriteString(" r" + referenceRatioText(st.reference))
	}
	fmt.Fprintf(&b, "e%d:%d:%d", st.edit.kind, st.edit.tolerance, st.edits)
	fmt.Fprintf(&b, "p%s:%d:%d", st.policy.follower, st.policy.milli, st.policyEdits)
	return b.String()
}

// desiredCounts are the Instance counts the sides' specs ask for, keyed by
// Component, each read as at least one, the way the service pass reads
// them for the reference.
func (st *huntState) desiredCounts(members []huntMember) map[v1beta1.ComponentType]int32 {
	out := make(map[v1beta1.ComponentType]int32, len(members))
	for i, m := range members {
		out[m.comp] = max(int32(len(st.sides[i].rows)), 1)
	}
	return out
}

func (st *huntState) complete() bool {
	for _, s := range st.sides {
		if !s.complete() {
			return false
		}
	}
	return true
}

// done reports a roll with nothing left to do: every Instance promoted or
// held by its partition.
func (st *huntState) done() bool {
	for _, s := range st.sides {
		if !s.done() {
			return false
		}
	}
	return true
}

// partitioned reports an operator's partition on any side.
func (st *huntState) partitioned() bool {
	for _, s := range st.sides {
		if s.effectivePartition() > 0 {
			return true
		}
	}
	return false
}

// huntMove is one thing the world does between two admissions. A scale or
// a partition change may land on two sides at once (side2 and delta2); an
// edit of the rollout block lands on the whole group. A move may carry a
// second one that lands in the same pass, before the admission that
// follows (second).
type huntMove struct {
	kind   string
	side   int
	idx    int
	delta  int32
	both   bool // the move lands on side2 too
	side2  int
	delta2 int32
	edit   huntEdit
	policy huntPolicy
	second *huntMove
}

// twoSided reports a move that lands on both sides at once.
func (m huntMove) twoSided() bool { return m.both }

func (m huntMove) String() string {
	text := m.single()
	if m.second != nil {
		text += " and in the same pass " + m.second.single()
	}
	return text
}

// single is the text of the move without the one it carries.
func (m huntMove) single() string {
	switch m.kind {
	case "scale":
		if m.twoSided() {
			return fmt.Sprintf("scale side %d by %+d and side %d by %+d", m.side, m.delta, m.side2, m.delta2)
		}
		return fmt.Sprintf("scale side %d by %+d", m.side, m.delta)
	case "partition":
		if m.twoSided() {
			return fmt.Sprintf("partition side %d to %d and side %d to %d", m.side, m.delta, m.side2, m.delta2)
		}
		return fmt.Sprintf("partition side %d to %d", m.side, m.delta)
	case "backoff", "prune", "spent", "follower-pass":
		return fmt.Sprintf("%s side %d", m.kind, m.side)
	case "canary-step":
		return fmt.Sprintf("canary step %d", m.delta)
	case "service-pass":
		return "service pass"
	case "edit":
		return "edit: " + m.edit.String()
	case "policy":
		return "policy: " + m.policy.String()
	case "rollback":
		return "roll back to the previous revision"
	case "canary-rollback":
		return "the canary rolls back"
	case "pause", "resume":
		return m.kind
	case "repush":
		return "push a further revision"
	}
	return fmt.Sprintf("%s side %d row %d", m.kind, m.side, m.idx)
}

// event reports a move the operator or the cluster makes to a roll, as
// against one the roll makes on its own: a partition change, a scale, an
// edit of the rollout block, a further revision pushed, a gang member
// lost, a member's node that stops reporting, a member the scheduler
// refuses. These are the moves tried at every moment of a plain roll
// (probe).
func (m huntMove) event() bool {
	switch m.kind {
	case "partition", "scale", "edit", "policy", "repush", "lose", "lostnode", "refuse", "rollback", "canary-rollback", "pause":
		return m.second == nil || m.second.event()
	}
	return false
}

// huntRepush is the further revision the operator pushes while the roll
// is open, named by the pairing protocols of the three revisions in play:
// the one the roll started from, the one the first push named (the middle
// revision once the further push lands) and the final one.
type huntRepush uint8

const (
	repushNone         huntRepush = iota
	repushSecondChange            // pair-v1 to pair-v2, then pair-v3 while that change is open
	repushBack                    // pair-v1 to pair-v2, then back to pair-v1
	repushLate                    // a plain push on pair-v1, then pair-v2 while that roll is open
)

var huntRepushNames = [...]string{"", "second-change", "back", "late-change"}

func (r huntRepush) String() string { return huntRepushNames[r] }

// huntConfig is one configuration of the group under exploration.
type huntConfig struct {
	name           string
	members        []huntMember
	groupUnavail   string
	groupSurge     string
	tolerance      *int32 // the group's maintainRatio tolerance
	defaults       GroupDefaults
	protocolChange bool
	maxAttempts    int32
	fails          int8
	parks          int8
	scales         int8
	parts          int8 // partition changes per side
	losses         int8 // gang members lost mid-surge per side
	refills        int8 // gang members lost on an old or a promoted gang per side, each refilled by the repair
	edits          int8 // edits of the rollout block while the run is open
	policyEdits    int8 // edits of the scaling policy while the run is open
	scaleDeltas    []int32
	// canary rolls the pair as a canary unit through these step capacities
	// ("25%", "100%"); nil rolls it as a rollingUpdate group.
	canary []string
	// shareBound reads the two sides' new-version shares against the
	// group's tolerance at every state (batch_step_model_test.go).
	shareBound bool
	// probes runs the operator's moves at every moment of a plain roll
	// with the roll then played to its end, beside the breadth-first walk.
	probes bool
	// behindPush names the sides whose replica status predates the push
	// when the roll starts, one bit per member.
	behindPush uint8
	// ratioRule sets maintainRatio on the group with no tolerance of its
	// own, so the operator's default (defaults.RatioTolerancePercent) fills
	// it; tolerance sets the rule with the group's own tolerance.
	ratioRule bool
	// referenceBehind is the ratio reference the group's status records
	// when the roll starts, when it differs from the members' counts: a
	// resize made while the group was idle whose service pass has not run.
	referenceBehind map[v1beta1.ComponentType]int32
	// ratioReadings reads the serving ratio and the two sides' shares
	// against the group's tolerance at every state
	// (ratio_gate_model_test.go).
	ratioReadings bool
	// follower names the pairing side a proportional scaling policy sizes
	// from the other side's count (the anchor) when the roll starts: the
	// operator scales the anchor alone and the coordinator writes the
	// follower's count one service pass later
	// (batch_step_events_model_test.go). The policy in force at a state is
	// the state's (huntState.policy), which the operator may edit.
	follower v1beta1.ComponentType
	// refusals is how many attempts or created Instances per side the
	// scheduler may refuse to place a member of.
	refusals int8
	// lostNodes is how many old or promoted gangs per side may have a
	// worker's node stop reporting.
	lostNodes int8
	// forceDelete is the operator's force-delete policy: a pod on a node
	// that stopped reporting is removed once the node has been unreachable
	// for the policy's threshold. Off, the chart's default, leaves such a
	// pod until an operator removes it.
	forceDelete bool
	// noPlacementGrace leaves lifecycle.unschedulableGracePeriod unset: an
	// attempt the scheduler refuses waits until the scheduler places it.
	noPlacementGrace bool
	// pairs tries two events in one pass, before the admission that follows.
	pairs bool
	// eventReadings reads every hold against the engine's hold reasons,
	// the batches' fill and the two sides' waits on each other
	// (batch_step_events_model_test.go).
	eventReadings bool
	// rollbacks is how many times the operator rolls back mid-roll by
	// re-applying the previous revision, which turns the roll around on
	// both sides at once; pauses is how many times the operator pauses and
	// resumes; canaryRollbacks is how many times the canary rolls the pair
	// back to its stable revision (batch_step_rollback_model_test.go).
	rollbacks, pauses, canaryRollbacks int8
	// repush is the further revision the operator may push once while the
	// roll is open, tried at every moment (pairing_gate_model_test.go);
	// repushNone offers none.
	repush huntRepush
	// pairingReadings reads the pairable capacity at every state: no step
	// the gates admit takes a side below its pairable minimum or leaves the
	// fleet with no pairable pair, and a standstill names the gate the
	// pairing gates wait on (pairing_gate_model_test.go).
	pairingReadings bool
}

// hasRatioRule reports a group with maintainRatio set, with its own
// tolerance or the operator's default.
func (c huntConfig) hasRatioRule() bool { return c.tolerance != nil || c.ratioRule }

// protocols are the pairing protocols of the three revisions in play: the
// one the roll started from, the first push's and the further push's. A
// configuration with no protocol change and no further push names none,
// so the pairing gates stay inert.
func (c huntConfig) protocols() [3]string {
	switch c.repush {
	case repushSecondChange:
		return [3]string{"pair-v1", "pair-v2", "pair-v3"}
	case repushBack:
		return [3]string{"pair-v1", "pair-v2", "pair-v1"}
	case repushLate:
		return [3]string{"pair-v1", "pair-v1", "pair-v2"}
	}
	if c.protocolChange {
		return [3]string{"pair-v1", "pair-v2", ""}
	}
	return [3]string{}
}

// podsListed reports a configuration whose pods the drain-time pairing
// gate reads: one with a protocol change at the push or a further push.
func (c huntConfig) podsListed() bool { return c.protocolChange || c.repush != repushNone }

// followerIndex is the index of the following side and of its anchor, -1
// when no scaling policy follows.
func (c huntConfig) followerIndex() (follower, anchor int) {
	follower, anchor = -1, -1
	if c.follower == "" {
		return follower, anchor
	}
	for i, m := range c.members {
		switch {
		case m.comp == c.follower:
			follower = i
		case m.comp == pairingPeer(c.follower):
			anchor = i
		}
	}
	if follower < 0 || anchor < 0 {
		return -1, -1
	}
	return follower, anchor
}

// followerMilli is the policy's ratio in thousandths, the largest that
// gives the follower its configured count from the anchor's
// (scaling.Target rounds up): 12 prefill beside 8 decode write 0.666.
func (c huntConfig) followerMilli() int64 {
	follower, anchor := c.followerIndex()
	if follower < 0 {
		return 0
	}
	return int64(c.members[follower].desired) * 1000 / int64(c.members[anchor].desired)
}

func (c huntConfig) String() string {
	var parts []string
	for _, m := range c.members {
		part := fmt.Sprintf("%s=%d/%s/%s", m.comp, m.desired, m.short(), m.budget)
		if m.unavailable != "" {
			part += "/mu" + m.unavailable
		}
		parts = append(parts, part)
	}
	s := strings.Join(parts, " ")
	if c.groupUnavail != "" || c.groupSurge != "" {
		s += fmt.Sprintf(" group(mu=%s,ms=%s)", c.groupUnavail, c.groupSurge)
	}
	if c.tolerance != nil {
		s += fmt.Sprintf(" maintainRatio=%d", *c.tolerance)
	} else if c.ratioRule {
		s += " maintainRatio=default"
	}
	if c.referenceBehind != nil {
		s += " reference=" + referenceRatioText(c.referenceBehind)
	}
	if c.edits > 0 {
		s += fmt.Sprintf(" edits=%d", c.edits)
	}
	if keepRatio, firstSide := huntRulesOf(c.defaults); keepRatio || firstSide != "" {
		if keepRatio {
			s += " keepRatio"
			if c.defaults.RatioTolerancePercent != nil {
				s += fmt.Sprintf("(default %d)", *c.defaults.RatioTolerancePercent)
			}
		}
		if firstSide != "" {
			s += " firstSide=" + string(firstSide)
		}
	}
	if c.protocolChange {
		s += " protocolChange"
	}
	if c.repush != repushNone {
		s += " repush=" + c.repush.String()
	}
	if c.canary != nil {
		s += " canary=" + strings.Join(c.canary, ",")
	}
	s += c.eventText()
	s += c.rollbackText()
	s += fmt.Sprintf(" schedule=%d", c.maxAttempts)
	if c.behindPush != 0 {
		var behind []string
		for i, m := range c.members {
			if c.behindPush&(1<<i) != 0 {
				behind = append(behind, string(m.comp))
			}
		}
		s += " statusBehind=" + strings.Join(behind, ",")
	}
	return s
}

// huntWorld builds the objects the gates read from a state and runs the
// dispatcher's admission over them.
type huntWorld struct {
	t       *testing.T
	cfg     huntConfig
	ctx     context.Context
	scheme  *runtime.Scheme
	isvc    *v1beta1.InferenceService
	targets []*appsv1.ControllerRevision
	// finals are the revisions a further push names, one per member, with
	// the final protocol; nil when the configuration offers no further push.
	finals []*appsv1.ControllerRevision
	// recorder takes the service events the gates raise; the pass drains
	// it into the state after every question.
	recorder *record.FakeRecorder
	// gateCalls counts the questions put to the production gates.
	gateCalls int
}

func newHuntWorld(t *testing.T, cfg huntConfig) *huntWorld {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	components := make([]v1beta1.ComponentType, 0, len(cfg.members))
	for _, m := range cfg.members {
		components = append(components, m.comp)
	}
	group := v1beta1.RolloutGroup{Components: components}
	if cfg.canary != nil {
		// A canary unit: the group carries its steps and no pacing block.
		group.Canary = &v1beta1.GroupCanary{}
		for _, capacity := range cfg.canary {
			traffic := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(capacity)), 100, true)
			group.Canary.Steps = append(group.Canary.Steps, v1beta1.RolloutGroupStep{Capacity: intstr.Parse(capacity), Traffic: traffic})
		}
	} else {
		group.RollingUpdate = &v1beta1.GroupRollingUpdate{}
		if cfg.groupUnavail != "" {
			group.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(cfg.groupUnavail))
		}
		if cfg.groupSurge != "" {
			group.RollingUpdate.MaxSurge = ptr.To(intstr.Parse(cfg.groupSurge))
		}
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: batchServiceName, Namespace: batchNamespace},
		Spec:       v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}}},
	}
	if cfg.canary != nil {
		// The canary's run state, keyed by the unit's primary member, at its
		// first step. The gates read the recorded plan, not the step; the
		// step reaches them as each side's partition (canaryPartition).
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			rollout.PrimaryOf(&group): {RolloutPhase: v1beta1.RolloutPhaseCanarying, Canary: &v1beta1.CanaryStatus{CurrentStep: 0}},
		}
	}
	if cfg.hasRatioRule() {
		// The group's status carries the reference the service pass last
		// wrote; the states carry it from here (huntState.reference).
		isvc.Spec.Rollout.Groups[0].MaintainRatio = &v1beta1.MaintainRatio{Tolerance: cfg.tolerance}
		isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{{
			Name: "0", Components: components, Policy: v1beta1.CoordinationPolicyRollingUpdate,
			ObservedRatio: &v1beta1.RolloutCoordinationRatio{Original: cfg.initialReference()},
		}}}
	}
	protocols := cfg.protocols()
	if protocols[1] != "" {
		isvc.Spec.Rollout.PairingProtocol = ptr.To(protocols[1])
	}
	isvc = pinActiveRun(isvc)
	w := &huntWorld{t: t, cfg: cfg, ctx: context.Background(), scheme: scheme, isvc: isvc, recorder: record.NewFakeRecorder(256)}
	for _, m := range cfg.members {
		w.targets = append(w.targets, forwardTarget(isvc, m.comp))
		if cfg.repush != repushNone {
			w.finals = append(w.finals, pairingCR(batchNamespace, batchServiceName, m.comp, "final", protocols[2]))
		}
	}
	return w
}

// transition reports a state at which a pairing-protocol change may be in
// flight, so the drain-time gate is asked before an old pod leaves: a
// configuration with the change at the push, or one whose further push,
// which names another protocol, has landed.
func (w *huntWorld) transition(st *huntState) bool {
	return w.cfg.protocolChange || (st.pushed && w.cfg.repush != repushNone)
}

// middleRevision names the revision the first push named, once a further
// push has made it the middle one.
func (w *huntWorld) middleRevision(i int) string {
	return batchRevision(w.cfg.members[i].comp, "target")
}

// rowProtocols are the pairing protocols of a row's own pods and of the
// revision its attempt moves to.
func (w *huntWorld) rowProtocols(s huntSide, idx int) (source, attempted string) {
	protocols := w.cfg.protocols()
	source, attempted = protocols[0], protocols[1]
	if s.reversed {
		// The rows read against the revision the roll returns to, so the
		// pods of the rows it leaves carry the pushed revision's protocol.
		source, attempted = attempted, source
	}
	if s.pushed {
		attempted = protocols[2]
	}
	if s.srcMiddle(idx) {
		source = protocols[1]
	}
	if s.attemptOnMiddle(idx) {
		attempted = protocols[1]
	}
	return source, attempted
}

// initialReference is the ratio reference the group's status records when
// the roll starts: the one a resize left behind when the configuration
// names it, else the members' counts.
func (c huntConfig) initialReference() map[v1beta1.ComponentType]int32 {
	if c.referenceBehind != nil {
		return maps.Clone(c.referenceBehind)
	}
	out := make(map[v1beta1.ComponentType]int32, len(c.members))
	for _, m := range c.members {
		out[m.comp] = m.desired
	}
	return out
}

// service is the InferenceService the gates read at a state: the spec as
// the operator's edit in force left it, the run still recording the plan
// it opened with, and the group's ratio reference as the state records it.
func (w *huntWorld) service(st *huntState) *v1beta1.InferenceService {
	isvc := w.editedService(st.edit)
	if st.pushed {
		// The further push names the final protocol on the spec; the run
		// keeps the plan it recorded.
		isvc = isvc.DeepCopy()
		isvc.Spec.Rollout.PairingProtocol = ptr.To(w.cfg.protocols()[2])
	}
	if st.reference == nil || isvc.Status.RolloutCoordination == nil {
		return isvc
	}
	withReference := *isvc
	group := isvc.Status.RolloutCoordination.Groups[0]
	group.ObservedRatio = &v1beta1.RolloutCoordinationRatio{Original: maps.Clone(st.reference)}
	withReference.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{group}}
	return &withReference
}

// referenceAfterServicePass is the ratio reference the service pass writes
// at a state, through the production writer: the group resolved as the
// gates resolve it, every member's replica observed, the reference taken
// from the counts the specs ask for (buildRatioState) and written into the
// group status (BuildGroupStatus, MergeGroupStatus).
func (w *huntWorld) referenceAfterServicePass(st *huntState) map[v1beta1.ComponentType]int32 {
	isvc := w.service(st)
	gate := ResolveGateContextWithDefaults(w.ctx, w.client(st), isvc, w.cfg.members[0].comp, w.cfg.defaults)
	if gate.ShortCircuit || gate.Hold {
		return st.reference
	}
	obs, err := buildGroupObservation(w.ctx, gate.Reads, isvc, gate.Group, nil)
	if err != nil {
		w.t.Fatalf("service pass: %v", err)
	}
	ratio := buildRatioState(isvc, gate.Group, obs, nil)
	merged := MergeGroupStatus(isvc.Status.RolloutCoordination, BuildGroupStatus(gate.Group, GroupTransition{}, ratio, time.Now()))
	for _, g := range merged.Groups {
		if g.Name == gate.Group.Name && g.ObservedRatio != nil {
			return g.ObservedRatio.Original
		}
	}
	return nil
}

// servicePassPending reports a state whose next service pass would write
// a reference other than the recorded one: the counts differ from it and
// the writer, asked, writes something else.
func (w *huntWorld) servicePassPending(st *huntState) bool {
	if st.reference == nil || maps.Equal(st.reference, st.desiredCounts(w.cfg.members)) {
		return false
	}
	return !maps.Equal(w.referenceAfterServicePass(st), st.reference)
}

// drainEvents moves the events the gates raised into the state.
func (w *huntWorld) drainEvents(st *huntState) {
	for {
		select {
		case e := <-w.recorder.Events:
			st.events = append(st.events, e)
		default:
			return
		}
	}
}

func (w *huntWorld) initial() *huntState {
	st := &huntState{sides: make([]huntSide, len(w.cfg.members)), holds: make([]*huntHold, len(w.cfg.members)), edits: w.cfg.edits,
		policy: w.cfg.initialPolicy(), policyEdits: w.cfg.policyEdits, rollbacks: w.cfg.rollbacks, pauses: w.cfg.pauses, canaryRollbacks: w.cfg.canaryRollbacks}
	if w.cfg.hasRatioRule() {
		st.reference = w.cfg.initialReference()
	}
	st.canaryStep = -1
	if w.cfg.canary != nil {
		st.canaryStep = 0
	}
	for i, m := range w.cfg.members {
		st.sides[i] = huntSide{rows: make([]huntRow, m.desired), fails: w.cfg.fails, parks: w.cfg.parks, scales: w.cfg.scales, parts: w.cfg.parts, behind: w.cfg.behindPush&(1<<i) != 0, canaryPartition: -1, refusals: w.cfg.refusals}
		if m.isGang() {
			st.sides[i].losses = w.cfg.losses
			st.sides[i].refills = w.cfg.refills
			st.sides[i].lostNodes = w.cfg.lostNodes
		}
		st.sides[i].canaryPartition = w.canaryPartition(st, i)
	}
	return st
}

// canaryPartition is the partition the canary's current step projects on
// side i, with the arithmetic of the canary's projector
// (canary/capacity.go:stepPartition; the canary package imports this one,
// so the test cannot call it): the side's count less the step's capacity
// rounded up, never below one Instance on the stable revision while a
// step runs, zero once the canary is past its last step, -1 with no canary.
func (w *huntWorld) canaryPartition(st *huntState, i int) int32 {
	if st.canaryStep < 0 {
		return -1
	}
	if int(st.canaryStep) >= len(w.cfg.canary) {
		return 0
	}
	desired := int32(len(st.sides[i].rows))
	capacity := utils.ClampInt32(utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(w.cfg.canary[st.canaryStep])), desired, true), 0, desired)
	partition := desired - capacity
	if desired > 1 && partition < 1 {
		partition = 1
	}
	return partition
}

func (w *huntWorld) oldRevision(i int) string { return batchRevision(w.cfg.members[i].comp, "old") }

// rollTargetFor is the ControllerRevision side i moves its Instances to at
// a state: the pushed revision, the further push's once it landed, or the
// one the roll left once a rollback turned it around, with the protocol
// that revision carries.
func (w *huntWorld) rollTargetFor(st *huntState, i int) *appsv1.ControllerRevision {
	if st.pushed {
		return w.finals[i]
	}
	if !st.reversed() {
		return w.targets[i]
	}
	return pairingCR(batchNamespace, batchServiceName, w.cfg.members[i].comp, "old", w.cfg.protocols()[0])
}

// fromRevisionFor is the revision a start of row idx of side i leaves: the
// middle revision for an Instance the first push promoted and the further
// push moves again, the pushed revision once a rollback turned the roll
// around, else the one the roll started from.
func (w *huntWorld) fromRevisionFor(s huntSide, i, idx int) string {
	if s.srcMiddle(idx) {
		return w.middleRevision(i)
	}
	if s.reversed {
		return batchRevision(w.cfg.members[i].comp, "target")
	}
	return w.oldRevision(i)
}

// editedService is the world's InferenceService with the operator's edit
// of the rollout block applied to the spec; the world's own when none is
// in force.
func (w *huntWorld) editedService(edit huntEdit) *v1beta1.InferenceService {
	if edit.kind == huntEditNone {
		return w.isvc
	}
	isvc := w.isvc.DeepCopy()
	switch edit.kind {
	case huntEditTolerance:
		isvc.Spec.Rollout.Groups[0].MaintainRatio = &v1beta1.MaintainRatio{Tolerance: ptr.To(edit.tolerance)}
	case huntEditRuleRemoved:
		isvc.Spec.Rollout.Groups[0].MaintainRatio = nil
	case huntEditGroupBudget:
		one := intstr.FromInt32(1)
		isvc.Spec.Rollout.Groups[0].RollingUpdate = &v1beta1.GroupRollingUpdate{MaxUnavailable: &one, MaxSurge: &one}
	case huntEditGroupRemoved:
		isvc.Spec.Rollout.Groups = nil
	case huntEditMemberAdded:
		members := isvc.Spec.Rollout.Groups[0].Components
		isvc.Spec.Rollout.Groups[0].Components = append(append([]v1beta1.ComponentType{}, members...), v1beta1.RouterComponent)
	case huntEditMemberRemoved:
		isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
	}
	return isvc
}

// grouped reports whether a rollout group still stands over the members at
// a state.
func (w *huntWorld) grouped(st *huntState) bool {
	return st.edit.kind != huntEditGroupRemoved
}

// ruleStands reports whether a ratio rule stands on the group at a state:
// the configured one unless an edit removed it, or one an edit added.
func (w *huntWorld) ruleStands(st *huntState) bool {
	switch st.edit.kind {
	case huntEditTolerance:
		return true
	case huntEditRuleRemoved, huntEditGroupRemoved:
		return false
	}
	return w.cfg.tolerance != nil
}

// toleranceAt is the tolerance the group names at a state, nil for none.
func (w *huntWorld) toleranceAt(st *huntState) *int32 {
	if st.edit.kind == huntEditTolerance {
		return ptr.To(st.edit.tolerance)
	}
	if !w.ruleStands(st) {
		return nil
	}
	return w.cfg.tolerance
}

// replica is the InferenceReplica a side publishes.
func (w *huntWorld) replica(i int, s huntSide) *v1beta1.InferenceReplica {
	m := w.cfg.members[i]
	// target is the revision the roll moves Instances to and old the one it
	// leaves; a rollback swaps them and a further push moves target to the
	// final revision. Under the canary's rollback the update revision stays
	// the pushed one and the pacing names the target.
	pushed := batchRevision(m.comp, "target")
	target, old := pushed, w.oldRevision(i)
	if s.reversed {
		target, old = old, target
	}
	if s.pushed {
		target = batchRevision(m.comp, "final")
	}
	n := int32(len(s.rows))
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: batchNamespace, Name: batchServiceName + "-" + string(m.comp)},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: m.comp,
			Replicas:  ptr.To(n),
			Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: m.strategy}},
		},
		Status: v1beta1.InferenceReplicaStatus{CurrentRevision: old, UpdateRevision: target, Replicas: n},
	}
	if s.revert {
		ir.Status.UpdateRevision = pushed
		ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To(target)}
	}
	if s.behind {
		// The status the replica wrote before the push: the previous roll
		// target recorded, every Instance promoted on it, the spec a
		// generation ahead of what the status was written for.
		ir.Generation, ir.Status.ObservedGeneration = 2, 1
		ir.Status.UpdateRevision, ir.Status.UpdatedReplicas = old, n
	}
	if m.budget != "" {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{}
		if m.surges() {
			ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.Parse(m.budget))
			if m.unavailable != "" {
				ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(m.unavailable))
			}
		} else {
			ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(m.budget))
		}
	}
	if m.isGang() {
		ir.Spec.Runners = []v1beta1.Runner{{Name: workloadtypes.RunnerLeader, Size: 1}, {Name: workloadtypes.RunnerWorker, Size: m.gang - 1}}
	}
	if s.partition > 0 {
		// The operator's partition, projected onto the replica's update
		// strategy; the roll reads it unless a canary's outranks it
		// (irprojector.EffectivePartition).
		if ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate == nil {
			ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{}
		}
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.Partition = ptr.To(s.partition)
	}
	if s.canaryPartition >= 0 {
		// The canary step's partition, written to spec.pacing.partition,
		// which the roll reads first.
		if ir.Spec.Pacing == nil {
			ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
		}
		ir.Spec.Pacing.Partition = ptr.To(s.canaryPartition)
	}
	g := m.pods()
	var serving, promoted int32
	var replacements []v1beta1.OMENativeInstanceStatus
	for idx, kind := range s.rows {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(idx)}
		// src is the revision the row's own pods run, attemptRev the one its
		// attempt moves to: the roll's own revisions unless a further
		// push made the first push's the middle one.
		src, attemptRev := w.fromRevisionFor(s, i, idx), target
		if s.attemptOnMiddle(idx) {
			attemptRev = w.middleRevision(i)
		}
		attempt := func(step string) *v1beta1.InstanceOperation {
			return &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: attemptRev}
		}
		switch kind {
		case huntOld:
			row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, src, g, g, g
		case huntOpenDrain:
			row.Phase, row.RunningRevision, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceUpdating, src, attemptRev, attempt(workloadtypes.UpdateStepDrain)
		case huntDrainBack:
			// The recreate keeps the revision the Instance last ran as its
			// running revision, and that revision is the target again.
			row.Phase, row.RunningRevision, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceUpdating, target, target, attempt(workloadtypes.UpdateStepDrain)
		case huntOpenInPlace:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, src, attemptRev, 1, attempt(workloadtypes.UpdateStepInPlace)
		case huntOpenSurge:
			if m.isGang() {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, src, attemptRev, g, g, g, attempt(workloadtypes.UpdateStepSurge)
			} else {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, src, attemptRev, 2, 1, 1, attempt(workloadtypes.UpdateStepSurge)
			}
		case huntReplacementHeld:
			if m.isGang() {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, src, attemptRev, g, g, g, attempt(workloadtypes.UpdateStepSurge)
			} else {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, src, attemptRev, 2, 2, 2, attempt(workloadtypes.UpdateStepSurge)
			}
		case huntPromoted:
			row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, target, g, g, g
		case huntPromotedUncounted:
			// The promote write carries the counters of the publication
			// before it, when the rebuilt pods stood but were not Ready. On
			// the middle revision the row is one the further push moves on.
			row.Phase, row.RunningRevision, row.PodCount = v1beta1.OMENativeInstanceReady, target, g
			if s.srcMiddle(idx) {
				row.RunningRevision = src
			}
		case huntFailedUnserving:
			row.Phase, row.RunningRevision, row.TargetRevision = v1beta1.OMENativeInstanceFailed, src, attemptRev
		case huntFailedSurge:
			if m.isGang() {
				// The replacement gang was retired: the source stands as the
				// engine publishes a failed attempt, Ready over the old pods.
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, src, attemptRev, g, g, g
			} else {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, src, attemptRev, 2, 1, 1
			}
		case huntFailedGangKept:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceReady, src, attemptRev, g, g, g, attempt(workloadtypes.UpdateStepSurge)
		case huntSurgeShort:
			// The source gang lost a worker: the leader keeps serving, the
			// counters read one pod short, the attempt stays at Surge.
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, src, attemptRev, g, g-1, g-1, attempt(workloadtypes.UpdateStepSurge)
		case huntReplacementShort:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, src, attemptRev, g, g, g, attempt(workloadtypes.UpdateStepSurge)
		case huntRetriesStopped:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceReady, src, attemptRev, 1, 1, 1, attempt(workloadtypes.UpdateStepParked)
		case huntCreating:
			row.Phase, row.TargetRevision, row.PodCount = v1beta1.OMENativeInstanceCreating, attemptRev, g
			row.Operation = &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate, TargetRevision: attemptRev}
		case huntOldShort:
			// The repair refills the lost worker under a Restart operation;
			// the leader keeps serving.
			row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceRestarting, src, g, g-1, g-1
			row.Operation = &v1beta1.InstanceOperation{ID: "refill", Type: v1beta1.InstanceOperationRestart}
		case huntPromotedShort:
			row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceRestarting, target, g, g-1, g-1
			row.Operation = &v1beta1.InstanceOperation{ID: "refill", Type: v1beta1.InstanceOperationRestart}
		default:
			eventRow(&row, kind, m, src, attemptRev, attempt)
		}
		if m.isGang() && kind.surgeTarget() {
			// The replacement gang lives in a slot of its own above the
			// desired count, taken from the source's attempt.
			slot := n + int32(len(replacements))
			row.Operation.SurgeIndex = ptr.To(slot)
			replacement := v1beta1.OMENativeInstanceStatus{Index: slot, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: attemptRev, PodCount: g,
				Operation: &v1beta1.InstanceOperation{ID: "replacement", Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepGangSurgeTarget, TargetRevision: attemptRev}}
			switch kind {
			case huntReplacementHeld, huntSourceDrainBlocked:
				replacement.ReadyPodCount, replacement.ServingPodCount = g, g
				serving++
			case huntReplacementShort:
				// The replacement gang lost a worker after it served: its
				// leader still serves, the gang no longer serves whole.
				replacement.ReadyPodCount, replacement.ServingPodCount = g-1, g-1
			case huntFailedGangKept:
				replacement.Phase = v1beta1.OMENativeInstanceFailed
				replacement.Operation.Step = workloadtypes.UpdateStepGangSurgeTargetCleanup
			}
			replacements = append(replacements, replacement)
		}
		if kind.serving() && !kind.shortOfAWorker() {
			serving++
		}
		// The updated counter counts a row running the target whose update
		// attempt, if any, names it: a recreate a rollback turned around is
		// among them while nothing of it serves; a row on the middle revision
		// after a further push is not.
		if (kind == huntPromoted || kind == huntPromotedUncounted || kind == huntPromotedShort || kind == huntDrainBack) && !s.srcMiddle(idx) {
			promoted++
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, row)
	}
	ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, replacements...)
	ir.Status.ServingReplicas, ir.Status.ReadyReplicas, ir.Status.UpdatedReplicas = serving, serving, promoted
	if record, ok := s.retryRecord(target); ok {
		ir.Status.RetryBlocks = []v1beta1.RetryBlock{record}
	}
	return ir
}

// retryRecord is the side's retry record for the target as the replica
// controller publishes it: Backoff from the first failure until the
// retry opens, RetryInProgress while one retry runs at a time, Held once
// the schedule is spent. None before the first failure. The gate reads a
// Held record on the named first side.
func (s huntSide) retryRecord(target string) (v1beta1.RetryBlock, bool) {
	record := v1beta1.RetryBlock{TargetRevision: target, AttemptsStarted: s.attempts, Reason: "the attempt never served"}
	switch s.schedule {
	case scheduleBackoff, scheduleDue:
		record.State = v1beta1.RetryBlockBackoff
	case scheduleRetrying:
		record.State = v1beta1.RetryBlockRetryInProgress
	case scheduleHeld:
		record.State = v1beta1.RetryBlockHeld
	default:
		return v1beta1.RetryBlock{}, false
	}
	return record, true
}

// slotOf is the index of the replacement gang a surging row of a gang
// member builds, as the replica numbers them: the first free index above
// the desired count, one per surge in flight in row order.
func (w *huntWorld) slotOf(s huntSide, idx int) int32 {
	slot := int32(len(s.rows))
	for j := 0; j < idx; j++ {
		if s.rows[j].surgeTarget() {
			slot++
		}
	}
	return slot
}

func (w *huntWorld) podName(i, idx, ordinal int) string {
	return fmt.Sprintf("%s-%s-%d-default-%d", batchServiceName, w.cfg.members[i].comp, idx, ordinal)
}

// gangPodNames are the pods of one gang Instance: the leader and its
// workers.
func (w *huntWorld) gangPodNames(i, idx int) []string {
	m := w.cfg.members[i]
	names := []string{fmt.Sprintf("%s-%s-%d-%s-0", batchServiceName, m.comp, idx, workloadtypes.RunnerLeader)}
	for k := int32(0); k < m.pods()-1; k++ {
		names = append(names, fmt.Sprintf("%s-%s-%d-%s-%d", batchServiceName, m.comp, idx, workloadtypes.RunnerWorker, k))
	}
	return names
}

// sourcePodNames are the pods a hand-over takes out of rotation: the old
// pod of a single-pod Instance, every pod of a gang.
func (w *huntWorld) sourcePodNames(i, idx int) []string {
	if w.cfg.members[i].isGang() {
		return w.gangPodNames(i, idx)
	}
	return []string{w.podName(i, idx, 0)}
}

// pod is one routable pod of a side as the drain gate lists it.
func (w *huntWorld) pod(i, idx, ordinal int, proto string, ready bool) *corev1.Pod {
	return w.labelledPod(i, w.podName(i, idx, ordinal), "default", idx, ordinal, proto, ready)
}

// gangPods are the pods of one gang Instance as the drain gate lists them:
// a routable leader and workers that never carry traffic.
func (w *huntWorld) gangPods(i, idx int, proto string, ready bool) []client.Object {
	var out []client.Object
	for k, name := range w.gangPodNames(i, idx) {
		runner := workloadtypes.RunnerLeader
		if k > 0 {
			runner = workloadtypes.RunnerWorker
		}
		out = append(out, w.labelledPod(i, name, runner, idx, 0, proto, ready))
	}
	return out
}

func (w *huntWorld) labelledPod(i int, name, runner string, idx, ordinal int, proto string, ready bool) *corev1.Pod {
	m := w.cfg.members[i]
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: batchNamespace, Name: name,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: prefixFor(w.isvc, m.comp),
			constants.OMEComponentLabel:           string(m.comp),
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelRunner:                     runner,
			query.LabelRevisionHash:               "rev" + strings.ReplaceAll(proto, "-", ""),
			query.LabelInstanceIdx:                strconv.Itoa(idx),
			query.LabelPodOrdinal:                 strconv.Itoa(ordinal),
			query.LabelPairingProtocol:            proto,
		},
	}}
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	return pod
}

// objects are everything the gates read for a state: the replicas, the
// revisions of both cohorts and, during a protocol change, the pods.
func (w *huntWorld) objects(st *huntState) []client.Object {
	var objs []client.Object
	protocols := w.cfg.protocols()
	for i, m := range w.cfg.members {
		objs = append(objs, w.replica(i, st.sides[i]),
			pairingCR(batchNamespace, batchServiceName, m.comp, "old", protocols[0]),
			pairingCR(batchNamespace, batchServiceName, m.comp, "target", protocols[1]))
		if w.cfg.repush != repushNone {
			objs = append(objs, pairingCR(batchNamespace, batchServiceName, m.comp, "final", protocols[2]))
		}
		if !w.cfg.podsListed() {
			continue
		}
		for idx, kind := range st.sides[i].rows {
			// Each row's own pods carry the protocol of the revision they
			// run and its replacement that of the revision the attempt
			// moves to.
			oldProto, newProto := w.rowProtocols(st.sides[i], idx)
			if m.isGang() {
				objs = append(objs, w.gangRowPods(st, i, idx, kind, oldProto, newProto)...)
			} else {
				objs = append(objs, w.rowPods(st.sides[i], i, idx, kind, oldProto, newProto)...)
			}
		}
	}
	return objs
}

// gangRowPods lists the pods of one gang row as the drain gate lists them.
func (w *huntWorld) gangRowPods(st *huntState, i, idx int, kind huntRow, oldProto, newProto string) []client.Object {
	m := w.cfg.members[i]
	var objs []client.Object
	switch kind {
	case huntOld, huntRetriesStopped, huntFailedSurge:
		objs = append(objs, w.gangPods(i, idx, oldProto, true)...)
	case huntPromoted:
		objs = append(objs, w.gangPods(i, idx, newProto, true)...)
	case huntPromotedUncounted:
		// The rebuilt pods are Ready, as the promote read them; only the
		// counters lag one status write. On the middle revision they run
		// the row's own revision.
		proto := newProto
		if st.sides[i].srcMiddle(idx) {
			proto = oldProto
		}
		objs = append(objs, w.gangPods(i, idx, proto, true)...)
	case huntOpenSurge, huntFailedGangKept:
		objs = append(objs, w.gangPods(i, idx, oldProto, true)...)
		objs = append(objs, w.gangPods(i, int(w.slotOf(st.sides[i], idx)), newProto, false)...)
	case huntSurgeShort:
		// The lost worker is gone from the list; the leader routes.
		objs = append(objs, w.gangPods(i, idx, oldProto, true)[:m.pods()-1]...)
		objs = append(objs, w.gangPods(i, int(w.slotOf(st.sides[i], idx)), newProto, false)...)
	case huntReplacementHeld:
		objs = append(objs, w.gangPods(i, idx, oldProto, true)...)
		objs = append(objs, w.gangPods(i, int(w.slotOf(st.sides[i], idx)), newProto, true)...)
	case huntReplacementShort:
		objs = append(objs, w.gangPods(i, idx, oldProto, true)...)
		objs = append(objs, w.gangPods(i, int(w.slotOf(st.sides[i], idx)), newProto, true)[:m.pods()-1]...)
	case huntCreating:
		objs = append(objs, w.gangPods(i, idx, newProto, false)...)
	case huntOldShort:
		objs = append(objs, w.gangPods(i, idx, oldProto, true)[:m.pods()-1]...)
	case huntPromotedShort:
		objs = append(objs, w.gangPods(i, idx, newProto, true)[:m.pods()-1]...)
	default:
		objs = append(objs, w.eventGangPods(i, idx, kind, st.sides[i], oldProto, newProto)...)
	}
	return objs
}

// rowPods lists the pods of one single-pod row as the drain gate lists
// them; a row with nothing routable lists none.
func (w *huntWorld) rowPods(s huntSide, i, idx int, kind huntRow, oldProto, newProto string) []client.Object {
	switch kind {
	case huntOld, huntRetriesStopped:
		return []client.Object{w.pod(i, idx, 0, oldProto, true)}
	case huntPromoted:
		return []client.Object{w.pod(i, idx, 1, newProto, true)}
	case huntPromotedUncounted:
		// The rebuilt pod is Ready, as the promote read it; only the
		// counters lag one status write. On the middle revision it runs
		// the row's own revision.
		proto := newProto
		if s.srcMiddle(idx) {
			proto = oldProto
		}
		return []client.Object{w.pod(i, idx, 1, proto, true)}
	case huntOpenSurge, huntFailedSurge, huntSurgeRefused:
		return []client.Object{w.pod(i, idx, 0, oldProto, true), w.pod(i, idx, 1, newProto, false)}
	case huntReplacementHeld:
		return []client.Object{w.pod(i, idx, 0, oldProto, true), w.pod(i, idx, 1, newProto, true)}
	case huntCreating, huntCreatingRefused:
		return []client.Object{w.pod(i, idx, 0, newProto, false)}
	}
	return nil
}

func (w *huntWorld) client(st *huntState) client.Client {
	return fake.NewClientBuilder().WithScheme(w.scheme).WithObjects(w.objects(st)...).Build()
}

// drainGateAdmits asks the drain gate whether the old pod of a surging
// Instance whose replacement serves may leave rotation.
func (w *huntWorld) drainGateAdmits(st *huntState, i, idx int) bool {
	if !w.transition(st) {
		return true
	}
	probe := st.clone()
	probe.sides[i].rows[idx] = huntReplacementHeld
	w.gateCalls++
	allowed, _, _ := EvaluatePairingDrain(w.ctx, w.client(probe), w.service(probe), w.cfg.members[i].comp, w.rollTargetFor(probe, i), w.cfg.defaults, w.sourcePodNames(i, idx))
	return allowed
}

// replacementRemovalAdmitted asks the drain gate whether the serving replacement of
// a kept replacement held to the middle revision may leave rotation, the
// question the roll asks before it ends a surge the further push moved
// past (ops.replacementKeptForPairing): a Pairing hold keeps the
// replacement, any other answer removes it.
func (w *huntWorld) replacementRemovalAdmitted(st *huntState, i, idx int) bool {
	probe := st.clone()
	probe.sides[i].rows[idx] = huntReplacementHeld
	probe.sides[i].setAttemptOnMiddle(idx, true)
	w.gateCalls++
	allowed, gate, _ := EvaluatePairingDrain(w.ctx, w.client(probe), w.service(probe), w.cfg.members[i].comp, w.rollTargetFor(probe, i), w.cfg.defaults, w.replacementPodNames(probe.sides[i], i, idx))
	return allowed || gate != v1beta1.RolloutHoldGatePairing
}

// replacementPodNames are the pods a surge brought into rotation: the
// replacement pod of a single-pod Instance, every pod of a replacement gang.
func (w *huntWorld) replacementPodNames(s huntSide, i, idx int) []string {
	if w.cfg.members[i].isGang() {
		return w.gangPodNames(i, int(w.slotOf(s, idx)))
	}
	return []string{w.podName(i, idx, 1)}
}

// huntRowVariant tells rows of one kind apart by the revisions they stand
// on after a further push; rows of one variant are interchangeable.
type huntRowVariant struct {
	kind         huntRow
	src, attempt bool
}

func (s huntSide) variant(idx int) huntRowVariant {
	return huntRowVariant{kind: s.rows[idx], src: s.srcMiddle(idx), attempt: s.attemptOnMiddle(idx)}
}

// rowVariants lists the variants of one kind in a fixed order.
func rowVariants(kind huntRow) [4]huntRowVariant {
	return [4]huntRowVariant{{kind, false, false}, {kind, true, false}, {kind, false, true}, {kind, true, true}}
}

// pass is one update pass of one Component: the ladder, then every fresh
// start candidate in selection order (Instances out of rotation first, then
// index order), each against the per-Component budget and the gate, every
// admitted start stamped before the next question. The first denial is
// the side's hold.
func (w *huntWorld) pass(st *huntState, i int) (admitted int, hold *huntHold) {
	m := w.cfg.members[i]
	side := &st.sides[i]
	// The pass ends with the side's status written for the push, a start
	// admitted or not: a replica held or backing off reports its
	// generation as observed too.
	defer func() { side.behind = false }()
	switch {
	case side.schedule == scheduleHeld:
		return 0, &huntHold{gate: v1beta1.RolloutHoldGateHeld, reason: fmt.Sprintf("update held after %d failed attempt(s)", side.attempts)}
	case side.schedule == scheduleBackoff:
		return 0, &huntHold{gate: v1beta1.RolloutHoldGateRetryBlock, reason: fmt.Sprintf("update in backoff after %d failed attempt(s)", side.attempts)}
	case side.schedule == scheduleRetrying && side.inFlight():
		return 0, &huntHold{gate: v1beta1.RolloutHoldGateRetryBlock, reason: "retrying one attempt at a time"}
	}
	// The Instances the partition holds are read once per pass, before any
	// start of the pass is stamped, and are never offered to the gates.
	held := side.held()
	order := make([]int, 0, len(side.rows))
	for idx, r := range side.rows {
		if r == huntFailedUnserving && !held[idx] {
			order = append(order, idx)
		}
	}
	// A gang short of a worker serves through its leader, so the start
	// order reads it like any old Instance (PodSetOutOfRotation reads the
	// routed pod), in index order.
	for idx, r := range side.rows {
		if (r == huntOld || r == huntFailedSurge || r == huntRetriesStopped || r.shortOld()) && !held[idx] {
			order = append(order, idx)
		}
	}
	// Rows of one variant are interchangeable: once one is denied the rest
	// of its variant are not asked.
	denied := map[huntRowVariant]bool{}
	var inFlightSurge, inFlightUnavail, gateUnavail int32
	var c client.Client
	for _, idx := range order {
		kind := side.rows[idx]
		if denied[side.variant(idx)] {
			continue
		}
		mechanism := workloadtypes.UpdateStrategyType(m.strategy)
		if m.strategy == "" {
			mechanism = workloadtypes.UpdateStrategySurgeThenDrain
		}
		if m.isGang() && m.inPlace() {
			// An in-place strategy rebuilds a gang whole.
			mechanism = workloadtypes.UpdateStrategyRecreatePod
		}
		exempt := kind == huntFailedUnserving
		if exempt {
			mechanism = workloadtypes.UpdateStrategyRecreatePod
		}
		// A start on a gang the serving count already leaves out, its lost
		// pod gone or Terminating, skips the gate under a surge as under a
		// recreate: the consult would count the outage twice
		// (skipsCoordinationGate). A pod still standing on a node that
		// stopped reporting is live, so that start keeps the consult.
		if kind.shortOld() && kind != huntOldUnreported {
			exempt = true
		}
		var h *huntHold
		if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
			budget := int32(-1)
			if m.budget != "" {
				budget = utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(m.budget)), int32(len(side.rows)), true)
			}
			// Attempts at a surge step hold slots, and so does the failed
			// replacement pod a single-pod surge left standing. A start on
			// the Instance whose own failed pod stands evicts it first and
			// answers for no other Instance's: those pods free only through
			// a start of their own, so charging them would deny every start
			// that can free one once the budget is below their number
			// (updateAdmission.unreplacedExtraSlots).
			var open, live int32
			for _, r := range side.rows {
				switch {
				case r == huntFailedSurge && !m.isGang():
					live++
				case r.surgeSlot(m.isGang()):
					open++
				}
			}
			prior := open + live
			if kind == huntFailedSurge && !m.isGang() {
				prior = open
			}
			if budget >= 0 && prior+inFlightSurge+1 > budget {
				h = &huntHold{gate: v1beta1.RolloutHoldGateBudget, reason: fmt.Sprintf("per-Component surge budget %d exhausted (would become %d)", budget, prior+inFlightSurge+1)}
			}
		} else {
			budget := int32(-1)
			if m.budget != "" {
				budget = utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(m.budget)), int32(len(side.rows)), true)
			}
			var prior int32
			for _, r := range side.rows {
				if r.openDrain() {
					prior++
				}
			}
			if budget >= 0 && prior+inFlightUnavail+1 > budget {
				h = &huntHold{gate: v1beta1.RolloutHoldGateBudget, reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)", budget, prior+inFlightUnavail+1)}
			}
		}
		if h == nil && !exempt {
			if c == nil {
				c = w.client(st)
			}
			w.gateCalls++
			allowed, gate, reason := EvaluateUpdateGate(w.ctx, c, w.service(st), m.comp, w.fromRevisionFor(*side, i, idx), w.rollTargetFor(st, i), w.recorder, w.cfg.defaults, mechanism, inFlightSurge, gateUnavail)
			w.drainEvents(st)
			if !allowed {
				h = &huntHold{gate: gate, reason: reason}
			} else if message, out := w.outOfTurn(st, i); out && st.outOfTurn == "" {
				st.outOfTurn = message
			}
		}
		if h != nil {
			denied[side.variant(idx)] = true
			if hold == nil {
				hold = h
			}
			continue
		}
		if exempt && kind.shortOld() {
			// The roll takes a gang short of a worker with no gate asked and
			// rebuilds it onto the target: a start the batch plan never made.
			side.rebuilt++
		}
		if exempt && idx < 64 {
			side.exempt |= 1 << idx
		}
		// A fresh attempt opens toward the revision the roll moves to now.
		side.setAttemptOnMiddle(idx, false)
		switch {
		case mechanism == workloadtypes.UpdateStrategySurgeThenDrain:
			side.rows[idx] = startedSurgeRow(kind)
			inFlightSurge++
		case mechanism == workloadtypes.UpdateStrategyRecreatePod:
			side.rows[idx] = startedDrainRow(kind)
			inFlightUnavail++
			if !exempt {
				gateUnavail++
			}
		default:
			side.rows[idx] = huntOpenInPlace
			inFlightUnavail++
			gateUnavail++
		}
		if side.schedule == scheduleDue {
			side.schedule = scheduleRetrying
		}
		admitted++
		c = nil
	}
	return admitted, hold
}

// outOfTurn reports a start of side i admitted while the side named to move
// first has not settled the batch the start belongs to, read on the rows as
// they are, every status written for the push: the first-side rule's own
// reading, which the gate misses only when a status it read predates the
// push or the pairing waiver skipped the named side.
func (w *huntWorld) outOfTurn(st *huntState, i int) (string, bool) {
	_, firstSide := huntRulesOf(w.cfg.defaults)
	if firstSide == "" || w.cfg.members[i].comp != pairingPeer(firstSide) {
		return "", false
	}
	written := st.clone()
	for j := range written.sides {
		written.sides[j].behind = false
	}
	gate := ResolveGateContextWithDefaults(w.ctx, w.client(written), w.service(written), w.cfg.members[i].comp, w.cfg.defaults)
	gate.TargetRevision = w.rollTargetFor(written, i)
	batch, err := gate.readBatchStep()
	if err != nil || !batch.governs {
		return "", false
	}
	me := batch.members[batch.acting]
	b := me.nextBatch()
	for _, x := range batch.members {
		if x.component != firstSide {
			continue
		}
		if mark := x.mark(b); x.settled() < mark {
			return fmt.Sprintf("the %s opened a start of batch %d while %s batch %d had %d of %d settled", me.component, b, x.component, b, x.settled(), mark), true
		}
	}
	return "", false
}

// settle runs passes over the Components in the given order until a whole
// round admits nothing.
func (w *huntWorld) settle(st *huntState, order []int) {
	st.outOfTurn = ""
	// A side whose status predates the push has not had its pass yet, so
	// the other side's pass comes first and reads that status.
	order = append([]int(nil), order...)
	sort.SliceStable(order, func(a, b int) bool { return !st.sides[order[a]].behind && st.sides[order[b]].behind })
	for round := 0; round < 16; round++ {
		changed := false
		for _, i := range order {
			written := st.sides[i].behind
			a, h := w.pass(st, i)
			st.holds[i] = h
			// A status written for the push wakes the peers' passes (the
			// replica controller's peer watch), so the round after it asks
			// the gates again with the status as written.
			changed = changed || a > 0 || (written && !st.sides[i].behind)
		}
		if !changed {
			return
		}
	}
	w.t.Fatalf("admission did not settle in 16 rounds: %s", st)
}

func (w *huntWorld) order(depth int) []int {
	order := make([]int, len(w.cfg.members))
	for i := range order {
		order[i] = i
	}
	if depth%2 == 1 {
		// the decoder first on odd depths
		for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
			order[i], order[j] = order[j], order[i]
		}
	}
	return order
}

// progressMoves are the moves that advance the roll by themselves: a
// started Instance serves, a kept hand-over the drain gate now releases,
// a lost gang member comes back, a failed replacement gang is retired, a
// backoff that ends, a retry record that is pruned.
func (w *huntWorld) progressMoves(st *huntState) []huntMove {
	var out []huntMove
	if st.paused {
		// A pause withholds the next operation and the next step: nothing
		// is promoted or released; repairs go on; the resume ends it.
		for i, s := range st.sides {
			seen := map[huntRow]bool{}
			for idx, r := range s.rows {
				if !seen[r] && (r == huntSurgeShort || r == huntReplacementShort || r == huntOldShort || r == huntPromotedShort) {
					seen[r] = true
					out = append(out, huntMove{kind: "mend", side: i, idx: idx})
				}
			}
		}
		return append(out, huntMove{kind: "resume"})
	}
	for i, s := range st.sides {
		// The first and the last row of each variant stand for every row of
		// it.
		seen := map[huntRowVariant]int{}
		last := map[huntRowVariant]int{}
		for idx := range s.rows {
			v := s.variant(idx)
			if _, ok := seen[v]; !ok {
				seen[v] = idx
			}
			last[v] = idx
		}
		rowsOf := func(kind huntRow) []int {
			var rows []int
			for _, v := range rowVariants(kind) {
				first, ok := seen[v]
				if !ok {
					continue
				}
				rows = append(rows, first)
				if last[v] != first {
					rows = append(rows, last[v])
				}
			}
			return rows
		}
		for _, kind := range []huntRow{huntOpenDrain, huntOpenInPlace, huntOpenSurge, huntCreating, huntSurgeShort, huntPromotedUncounted, huntSurgeLostNode, huntDrainBack} {
			for _, idx := range rowsOf(kind) {
				out = append(out, huntMove{kind: "serve", side: i, idx: idx})
			}
		}
		for _, idx := range rowsOf(huntReplacementHeld) {
			out = append(out, w.keptReplacementMoves(st, i, idx)...)
		}
		for _, kind := range []huntRow{huntSurgeShort, huntReplacementShort, huntOldShort, huntPromotedShort} {
			for _, idx := range rowsOf(kind) {
				out = append(out, huntMove{kind: "mend", side: i, idx: idx})
			}
		}
		for _, idx := range rowsOf(huntFailedGangKept) {
			out = append(out, huntMove{kind: "retire", side: i, idx: idx})
		}
		out = append(out, w.eventProgress(st, i, rowsOf)...)
		switch {
		case s.schedule == scheduleBackoff:
			out = append(out, huntMove{kind: "backoff", side: i})
		case s.schedule == scheduleRetrying && !s.inFlight():
			out = append(out, huntMove{kind: "prune", side: i})
		}
	}
	if st.canaryStep >= 0 && int(st.canaryStep) < len(w.cfg.canary) && st.done() {
		// The step's capacity serves on every side: the canary advances to
		// its next step, or past its last and releases the held Instances.
		out = append(out, huntMove{kind: "canary-step", delta: st.canaryStep + 1})
	}
	if w.servicePassPending(st) {
		// The service pass rewrites the ratio reference from the counts the
		// specs ask for.
		out = append(out, huntMove{kind: "service-pass"})
	}
	return out
}

// keptReplacementMoves are the moves a kept replacement may make: its old
// pods leave once the drain gate admits them (release). One held to the
// middle revision after a further push is first asked the roll's other
// question, whether its serving replacement may leave rotation (remove):
// the roll ends such a surge on the pass that finds the replacement
// unkept, before it asks about the old pods (ops.redirectWhenTargetMoved).
func (w *huntWorld) keptReplacementMoves(st *huntState, i, idx int) []huntMove {
	if st.sides[i].attemptOnMiddle(idx) && w.replacementRemovalAdmitted(st, i, idx) {
		return []huntMove{{kind: "remove", side: i, idx: idx}}
	}
	if w.drainGateAdmits(st, i, idx) {
		return []huntMove{{kind: "release", side: i, idx: idx}}
	}
	return nil
}

// repushApplies reports a state at which the configured further push may
// land: none has, no canary runs, no rollback or pause stands, no status
// predates the first push, and no gang stands short of a member, on a lost
// node, refused by the scheduler or retiring a failed replacement, the rows
// whose answer to a moved target the world does not model.
func (w *huntWorld) repushApplies(st *huntState) bool {
	if w.cfg.repush == repushNone || st.pushed || st.paused || st.reversed() || w.cfg.canary != nil {
		return false
	}
	for _, s := range st.sides {
		if s.behind {
			return false
		}
		for _, r := range s.rows {
			switch {
			case r.shortOfAWorker(), r.lostNode(), r == huntFailedGangKept, r == huntSurgeShort, r == huntReplacementShort,
				r == huntDrainRefused, r == huntSurgeRefused, r == huntCreatingRefused:
				return false
			}
		}
	}
	return true
}

// applyRepush lands the further revision: the roll target moves to the
// final revision and every row answers as the engine does to a target
// that moved. An Instance the first push promoted runs the middle revision
// and moves again; a recreate, an in-place patch out of rotation and a
// created Instance follow the new target; a surge whose replacement does
// not serve yet ends and its source stays (ops.redirectWhenTargetMoved); a
// surge whose serving replacement the drain gate keeps in rotation stays
// held to the middle revision and finishes onto it, else its replacement
// is removed and its source stays (ops.replacementKeptForPairing); a
// failed surge's extra pod goes with the revision that failed; a failed
// drain and a stopped attempt keep the revision they failed toward. The
// retry record is the pushed revision's (irprojector.RetryBlockFor), so no
// side is held or backing off toward the final one.
func (w *huntWorld) applyRepush(next *huntState) {
	next.pushed = true
	for i := range next.sides {
		side := &next.sides[i]
		side.pushed = true
		side.schedule, side.attempts = scheduleNone, 0
		for idx, r := range side.rows {
			switch r {
			case huntPromoted:
				side.rows[idx] = huntOld
				side.setSrcMiddle(idx, true)
			case huntPromotedUncounted:
				side.setSrcMiddle(idx, true)
			case huntOpenSurge, huntFailedSurge:
				side.rows[idx] = huntOld
			case huntRetriesStopped, huntFailedUnserving, huntReplacementHeld:
				side.setAttemptOnMiddle(idx, true)
			}
		}
	}
	// The kept replacements are asked one by one, in the order the passes
	// visit them, each on the picture the ones before it left.
	for i := range next.sides {
		side := &next.sides[i]
		for idx, r := range side.rows {
			if r == huntReplacementHeld && w.replacementRemovalAdmitted(next, i, idx) {
				side.rows[idx] = huntOld
				side.setAttemptOnMiddle(idx, false)
			}
		}
	}
}

// promoteKept ends a kept replacement whose old pods the drain gate let
// leave: onto the roll target, or onto the middle revision when the
// attempt was held to it, where the Instance is one the further push
// moves again.
func (s *huntSide) promoteKept(idx int) {
	if s.attemptOnMiddle(idx) {
		s.rows[idx] = huntOld
		s.setSrcMiddle(idx, true)
		s.setAttemptOnMiddle(idx, false)
		return
	}
	s.rows[idx] = huntPromoted
	s.setSrcMiddle(idx, false)
}

// scaleDelta turns a configured delta into Instances: 2 and -2 stand for
// half the side.
func scaleDelta(d, n int32) int32 {
	if d == 2 || d == -2 {
		half := n / 2
		if d < 0 {
			return -half
		}
		return half
	}
	return d
}

// partitionLevels are the levels a side's partition may be moved to from
// its current one: raised to half the side or the whole side, lowered to
// half of itself, cleared.
func partitionLevels(n, current int32) []int32 {
	var cands []int32
	if half := (n + 1) / 2; half > current {
		cands = append(cands, half)
	}
	if n > current {
		cands = append(cands, n)
	}
	if current > 0 {
		cands = append(cands, current/2, 0)
	}
	var out []int32
	seen := map[int32]bool{current: true}
	for _, level := range cands {
		if !seen[level] {
			seen[level] = true
			out = append(out, level)
		}
	}
	return out
}

// adverseMoves are the moves that cost the roll something: an attempt
// fails, is parked, the ladder is spent, a gang loses a member mid-surge,
// a partition is raised, lowered or cleared on one side or both, a side is
// scaled, both sides are scaled by the same step, or both to a new ratio,
// or the operator edits the rollout block: the group's tolerance is moved
// (or the rule added), the rule is removed, or the group is removed.
func (w *huntWorld) adverseMoves(st *huntState) []huntMove {
	out := w.turnMoves(st)
	if st.paused {
		// Under a pause the operator still edits partitions and the rollout
		// block and a gang may still lose a member; nothing else lands: no
		// attempt opens or is escalated, and no Instance is created.
		for i, s := range st.sides {
			if s.parts > 0 {
				for _, level := range partitionLevels(int32(len(s.rows)), s.partition) {
					out = append(out, huntMove{kind: "partition", side: i, delta: level})
				}
			}
			seen := map[huntRow]bool{}
			for idx, r := range s.rows {
				if !seen[r] && (r == huntOld || r == huntPromoted) && s.refills > 0 && w.cfg.members[i].isGang() {
					seen[r] = true
					out = append(out, huntMove{kind: "lose", side: i, idx: idx})
				}
			}
		}
		return out
	}
	out = append(out, w.policyMoves(st)...)
	if w.repushApplies(st) {
		out = append(out, huntMove{kind: "repush"})
	}
	if st.edits > 0 && w.grouped(st) {
		to := int32(20)
		if t := w.toleranceAt(st); t != nil && *t == 20 {
			to = 5
		}
		out = append(out, huntMove{kind: "edit", edit: huntEdit{kind: huntEditTolerance, tolerance: to}})
		if w.ruleStands(st) {
			out = append(out, huntMove{kind: "edit", edit: huntEdit{kind: huntEditRuleRemoved}})
		}
		out = append(out, huntMove{kind: "edit", edit: huntEdit{kind: huntEditGroupBudget}},
			huntMove{kind: "edit", edit: huntEdit{kind: huntEditGroupRemoved}},
			huntMove{kind: "edit", edit: huntEdit{kind: huntEditMemberAdded}},
			huntMove{kind: "edit", edit: huntEdit{kind: huntEditMemberRemoved}})
	}
	for i, s := range st.sides {
		seen := map[huntRowVariant]bool{}
		for idx, r := range s.rows {
			v := s.variant(idx)
			if seen[v] {
				continue
			}
			seen[v] = true
			switch r {
			case huntOpenDrain:
				if s.fails > 0 {
					out = append(out, huntMove{kind: "fail", side: i, idx: idx})
				}
			case huntOpenInPlace:
				if s.fails > 0 {
					out = append(out, huntMove{kind: "fail", side: i, idx: idx})
				}
				if s.parks > 0 {
					out = append(out, huntMove{kind: "park", side: i, idx: idx})
				}
			case huntOpenSurge:
				if s.fails > 0 {
					out = append(out, huntMove{kind: "fail", side: i, idx: idx})
				}
				if s.losses > 0 && w.cfg.members[i].isGang() {
					out = append(out, huntMove{kind: "lose", side: i, idx: idx})
				}
			case huntReplacementHeld:
				if s.losses > 0 && w.cfg.members[i].isGang() {
					out = append(out, huntMove{kind: "lose", side: i, idx: idx})
				}
			case huntOld, huntPromoted:
				if s.refills > 0 && w.cfg.members[i].isGang() {
					out = append(out, huntMove{kind: "lose", side: i, idx: idx})
				}
			}
		}
		if s.schedule != scheduleHeld && s.schedule != scheduleNone && s.count(huntFailedUnserving, huntFailedSurge) > 0 {
			out = append(out, huntMove{kind: "spent", side: i})
		}
		// Under a scaling policy the operator scales the anchor alone; the
		// coordinator writes the follower's count.
		if follower, _ := w.policySides(st); s.scales > 0 && i != follower {
			for _, d := range w.cfg.scaleDeltas {
				if delta := scaleDelta(d, int32(len(s.rows))); delta != 0 && int32(len(s.rows))+delta >= 1 {
					out = append(out, huntMove{kind: "scale", side: i, delta: delta})
				}
			}
		}
		if s.parts > 0 {
			for _, level := range partitionLevels(int32(len(s.rows)), s.partition) {
				out = append(out, huntMove{kind: "partition", side: i, delta: level})
			}
		}
		out = append(out, w.eventMoves(i, s)...)
	}
	if len(st.sides) >= 2 {
		a, b := st.sides[0], st.sides[1]
		na, nb := int32(len(a.rows)), int32(len(b.rows))
		if a.scales > 0 && b.scales > 0 && st.policy.follower == "" {
			// Both sides by the same step, and both to a new ratio: one
			// side up by half while the other goes down by half.
			for _, d := range w.cfg.scaleDeltas {
				da, db := scaleDelta(d, na), scaleDelta(d, nb)
				if da != 0 && db != 0 && na+da >= 1 && nb+db >= 1 {
					out = append(out, huntMove{kind: "scale", side: 0, delta: da, both: true, side2: 1, delta2: db})
				}
			}
			if da, db := na/2, nb/2; da > 0 && db > 0 {
				out = append(out, huntMove{kind: "scale", side: 0, delta: da, both: true, side2: 1, delta2: -db},
					huntMove{kind: "scale", side: 0, delta: -da, both: true, side2: 1, delta2: db})
			}
		}
		if a.parts > 0 && b.parts > 0 {
			if a.partition == 0 && b.partition == 0 {
				out = append(out, huntMove{kind: "partition", side: 0, delta: (na + 1) / 2, both: true, side2: 1, delta2: (nb + 1) / 2})
			} else if a.partition > 0 && b.partition > 0 {
				out = append(out, huntMove{kind: "partition", side: 0, delta: 0, both: true, side2: 1, delta2: 0})
			}
		}
	}
	if w.cfg.pairs {
		out = append(out, w.pairMoves(st, out)...)
	}
	return out
}

// apply makes one move, the one it carries for the same pass after it,
// and then lets the dispatcher admit what the gates allow.
func (w *huntWorld) apply(st *huntState, mv huntMove) *huntState {
	next := st.clone()
	next.path = append(next.path, mv)
	next.pairingBreach = nil
	w.applyRaw(next, mv)
	if mv.second != nil && w.applies(next, *mv.second) {
		w.applyRaw(next, *mv.second)
	}
	if next.reversed() && !st.reversed() {
		next.rollbackGap = w.rollbackGap(next)
	}
	var moved *huntState
	if w.cfg.pairingReadings {
		// The picture the gates saw when the admission that follows began.
		moved = next.clone()
	}
	if next.paused {
		// A pause withholds the passes' starts, not their status writes:
		// every side's status is written for the spec it holds.
		for i := range next.sides {
			next.sides[i].behind = false
		}
	} else {
		w.settle(next, w.order(len(next.path)))
	}
	// A start no gate was asked for is remembered while it is in flight,
	// as a drain, an in-place patch or a surge; once it serves or fails it
	// is a row like any other.
	for i := range next.sides {
		next.sides[i].exempt &= next.sides[i].inFlightMask()
	}
	if moved != nil {
		next.pairingBreach = w.readPairingBreach(st, moved, next, mv)
	}
	return next
}

// applyRaw makes one move on the state without the admission that follows.
func (w *huntWorld) applyRaw(next *huntState, mv huntMove) {
	side := &next.sides[mv.side]
	switch mv.kind {
	case "service-pass":
		next.reference = w.referenceAfterServicePass(next)
		next.referenceOff = !maps.Equal(next.reference, next.desiredCounts(w.cfg.members))
	case "serve":
		w.serve(next, mv.side, mv.idx)
	case "release":
		side.promoteKept(mv.idx)
	case "remove":
		// The replacement the further push moved past leaves rotation and
		// the source stays; the next pass starts toward the new target
		// through the gates again.
		side.rows[mv.idx] = huntOld
		side.setAttemptOnMiddle(mv.idx, false)
	case "retire":
		side.rows[mv.idx] = huntFailedSurge
	case "lose":
		switch side.rows[mv.idx] {
		case huntOld:
			// The repair yields a gang short of a worker to a roll that may
			// take it (RepairYieldsToTarget): the target open, the Instance
			// not held by the partition, the retry schedule admitting a
			// fresh start. Otherwise the repair refills it on its revision.
			side.refills--
			side.rows[mv.idx] = huntOldShort
			if w.rollTakes(*side, mv.idx) {
				side.rows[mv.idx] = huntOldLostMember
			}
		case huntPromoted:
			side.refills--
			side.rows[mv.idx] = huntPromotedShort
		case huntReplacementHeld:
			side.losses--
			side.rows[mv.idx] = huntReplacementShort
			if side.attemptOnMiddle(mv.idx) {
				// A replacement gang short of a member while the roll target
				// has moved off its revision is not rebuilt: the pair ends
				// and the source stays (update_gang.go, gangSurgeEnd, a pair ended after
				// its replacement took over serving).
				side.rows[mv.idx] = huntOld
				side.setAttemptOnMiddle(mv.idx, false)
			}
		default:
			side.losses--
			side.rows[mv.idx] = huntSurgeShort
		}
	case "mend":
		switch side.rows[mv.idx] {
		case huntOldShort:
			side.rows[mv.idx] = huntOld
		case huntPromotedShort:
			side.rows[mv.idx] = huntPromoted
		case huntSurgeShort:
			side.rows[mv.idx] = huntOpenSurge
		case huntReplacementShort:
			// The replacement serves whole again; the drain gate is asked
			// once more before the old gang leaves.
			if w.drainGateAdmits(next, mv.side, mv.idx) {
				side.promoteKept(mv.idx)
			} else {
				side.rows[mv.idx] = huntReplacementHeld
			}
		}
	case "repush":
		w.applyRepush(next)
	case "partition":
		side.parts--
		side.partition = mv.delta
		if mv.both {
			other := &next.sides[mv.side2]
			other.parts--
			other.partition = mv.delta2
		}
	case "fail":
		switch {
		case side.rows[mv.idx] == huntOpenSurge && w.cfg.members[mv.side].isGang():
			side.rows[mv.idx] = huntFailedGangKept
		case side.rows[mv.idx] == huntOpenSurge:
			side.rows[mv.idx] = huntFailedSurge
		default:
			side.rows[mv.idx] = huntFailedUnserving
		}
		side.fails--
		// A failure counts once per wave: a record in Backoff or Held is
		// not moved by a second failure of the same wave.
		if side.schedule != scheduleBackoff && side.schedule != scheduleHeld {
			side.attempts++
			if side.attempts >= w.cfg.maxAttempts {
				side.schedule = scheduleHeld
			} else {
				side.schedule = scheduleBackoff
			}
		}
	case "park":
		side.rows[mv.idx] = huntRetriesStopped
		side.parks--
	case "backoff":
		side.schedule = scheduleDue
	case "prune":
		side.schedule = scheduleNone
	case "spent":
		side.schedule = scheduleHeld
	case "scale":
		scaleSide(side, mv.delta)
		if mv.both {
			scaleSide(&next.sides[mv.side2], mv.delta2)
		}
		// A canary step's partition is sized against the new count.
		for i := range next.sides {
			next.sides[i].canaryPartition = w.canaryPartition(next, i)
		}
	case "canary-step":
		next.canaryStep = mv.delta
		for i := range next.sides {
			next.sides[i].canaryPartition = w.canaryPartition(next, i)
		}
	case "edit":
		next.edit = mv.edit
		next.edits--
	case "policy":
		next.policy = mv.policy
		next.policyEdits--
	case "pause":
		next.paused = true
		next.pauses--
	case "resume":
		next.paused = false
	case "rollback":
		next.rollbacks--
		w.turnAround(next, false)
	case "canary-rollback":
		next.canaryRollbacks--
		w.turnAround(next, true)
	default:
		w.applyEvent(next, mv)
	}
}

// serve lands a started Instance's pods in rotation. A surge replacement
// that serves is promoted when the drain gate lets the old pods leave and
// kept beside them otherwise; a rebuilt or patched Instance is promoted in
// one status write and its pods counted as serving in the next, which
// during a protocol change is a moment of its own (the pairing gate reads
// the publication in between); the pods of a promoted Instance on the
// middle revision count it among the Instances the further push moves.
func (w *huntWorld) serve(next *huntState, i, idx int) {
	side := &next.sides[i]
	switch side.rows[idx] {
	case huntOpenSurge, huntSurgeShort:
		if w.drainGateAdmits(next, i, idx) {
			side.promoteKept(idx)
		} else {
			side.rows[idx] = huntReplacementHeld
		}
	case huntSurgeLostNode:
		// The replacement serves; the source's drain then waits on the
		// pod its lost node never removes. A source the drain gate keeps
		// is read as kept from here on.
		if w.drainGateAdmits(next, i, idx) {
			side.rows[idx] = huntSourceDrainBlocked
		} else {
			side.rows[idx] = huntReplacementHeld
		}
	case huntOpenDrain, huntOpenInPlace, huntDrainBack:
		side.setSrcMiddle(idx, false)
		if w.transition(next) {
			side.rows[idx] = huntPromotedUncounted
		} else {
			side.rows[idx] = huntPromoted
		}
	case huntPromotedUncounted:
		if side.srcMiddle(idx) {
			side.rows[idx] = huntOld
		} else {
			side.rows[idx] = huntPromoted
		}
	default:
		side.rows[idx] = huntPromoted
	}
}

// scaleSide resizes one side on the operator's move: a scale-down removes
// the highest-indexed Instances, whatever they are doing; a scale-up
// creates new Instances on the target. The plans read the new count from
// the spec at once.
func scaleSide(side *huntSide, delta int32) {
	side.scales--
	resizeSide(side, delta)
}

// resizeSide resizes one side by delta Instances without spending the
// operator's scale moves: the operator's scale and the coordinator's
// follower write resize a side the same way.
func resizeSide(side *huntSide, delta int32) {
	if delta < 0 {
		side.rows = side.rows[:int32(len(side.rows))+delta]
		side.keepMiddleBelow(len(side.rows))
		// The highest-indexed rows go first, the created ones among them.
		gone := -delta
		if fromAdded := min32(gone, side.added); fromAdded > 0 {
			side.added -= fromAdded
			gone -= fromAdded
		}
		side.removed += gone
		return
	}
	side.added += delta
	for k := int32(0); k < delta; k++ {
		side.rows = append(side.rows, huntCreating)
	}
}

// huntFinding is one invariant broken at one state.
type huntFinding struct {
	class   string
	message string
	state   *huntState
}

// huntRecord records one finding of a class with its message.
type huntRecord func(class, format string, args ...any)

// huntBatchPosition is the span of batches a side sits inside: from the
// first batch whose mark its started count has reached to the batch after
// the last such mark.
type huntBatchPosition struct{ lo, hi int32 }

// batchGap is how many batches one side stands beyond the other's span;
// zero when the spans touch or overlap.
func batchGap(a, b huntBatchPosition) int32 {
	if a.lo > b.hi {
		return a.lo - b.hi
	}
	if b.lo > a.hi {
		return b.lo - a.hi
	}
	return 0
}

// batchPositions reads the batch step's members by Component and each
// side's batch position, and records every side whose rows the batch step
// reads otherwise than the model.
func (w *huntWorld) batchPositions(st *huntState, batch batchStep, record huntRecord) (map[v1beta1.ComponentType]batchMember, map[v1beta1.ComponentType]huntBatchPosition) {
	members := map[v1beta1.ComponentType]batchMember{}
	for _, bm := range batch.members {
		members[bm.component] = bm
	}
	positions := map[v1beta1.ComponentType]huntBatchPosition{}
	eng, dec := w.pairIndex()
	for _, i := range []int{eng, dec} {
		m := w.cfg.members[i]
		if bm, ok := members[m.comp]; ok {
			positions[m.comp] = sidePosition(m.comp, st.sides[i], bm, record)
		}
	}
	return members, positions
}

// sidePosition reads one side's batch position off the batch step's marks
// by the model's count of its rows, and records the rows the batch step
// reads otherwise than the model. A side sits inside every batch its
// started count has reached but not passed: from the first batch whose
// mark is its count up to the batch after the last such mark, so an empty
// planned batch does not move it; a side that started everything it may
// move is past every batch.
func sidePosition(comp v1beta1.ComponentType, s huntSide, bm batchMember, record huntRecord) huntBatchPosition {
	// The model's reading of the rows, by the tree's rule: a batch step
	// that counts a partition's held Instances leaves them out of every
	// other count.
	heldRows := s.held()
	var promoted, open, ended, heldCount int32
	for idx := range s.rows {
		if heldRows[idx] {
			heldCount++
			continue
		}
		switch s.batchRowReading(idx) {
		case batchRowPromoted:
			promoted++
		case batchRowOpen:
			open++
		case batchRowEnded:
			ended++
		}
	}
	if bm.promoted != promoted || bm.open != open || bm.ended != ended || bm.held != heldCount {
		record("classifier", "%s rows read as promoted %d open %d ended %d held %d by the batch step, the model holds %d %d %d %d (%s)", comp, bm.promoted, bm.open, bm.ended, bm.held, promoted, open, ended, heldCount, s)
	}
	started := promoted + open + ended
	bound := 2*int32(len(s.rows)) + 4
	var lo int32
	for lo = 0; bm.mark(lo) < started && lo < bound; lo++ {
	}
	hi := lo
	if bm.mark(lo) == started {
		for hi = lo; hi < bound && bm.mark(hi+1) == started; hi++ {
		}
		hi++
	}
	if started >= bm.desired-bm.held {
		hi = 1 << 20
	}
	return huntBatchPosition{lo, hi}
}

// rollbackGap is how many batches apart the two sides stand right after a
// rollback, as the batch step counts them once every status is written:
// the gap the previous roll left. Zero where the batch step does not
// govern the pair, a canary unit among them.
func (w *huntWorld) rollbackGap(st *huntState) int32 {
	eng, dec := w.pairIndex()
	if eng < 0 || dec < 0 || w.cfg.canary != nil {
		return 0
	}
	probe := st.clone()
	for i := range probe.sides {
		probe.sides[i].behind = false
	}
	gate := ResolveGateContextWithDefaults(w.ctx, w.client(probe), w.service(probe), v1beta1.EngineComponent, w.cfg.defaults)
	gate.TargetRevision = w.rollTargetFor(probe, eng)
	batch, err := gate.readBatchStep()
	if err != nil || !batch.governs {
		return 0
	}
	_, positions := w.batchPositions(probe, batch, func(string, string, ...any) {})
	return batchGap(positions[v1beta1.EngineComponent], positions[v1beta1.DecoderComponent])
}

// pairIndex finds the engine and the decoder among the members.
func (w *huntWorld) pairIndex() (engine, decoder int) {
	engine, decoder = -1, -1
	for i, m := range w.cfg.members {
		switch m.comp {
		case v1beta1.EngineComponent:
			engine = i
		case v1beta1.DecoderComponent:
			decoder = i
		}
	}
	return engine, decoder
}

// check reads every invariant at one settled state.
func (w *huntWorld) check(st *huntState) []huntFinding {
	var out []huntFinding
	add := func(class, format string, args ...any) {
		out = append(out, huntFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	// A scale moved the marks and the budgets under the roll; what the
	// gates do right after one is reported in classes of its own. The
	// coordinator's follower write is a scale the operator did not make.
	scaled := false
	for _, s := range st.sides {
		if s.scales < w.cfg.scales || s.followed > 0 {
			scaled = true
		}
	}
	budgetClass, promiseSuffix := "budget", ""
	if scaled {
		budgetClass, promiseSuffix = "budget-after-scale", "-after-scale"
	}
	// A gang the roll rebuilt with no gate asked is a start the batch plan
	// never made; the sides' positions are read in a class of its own.
	for _, s := range st.sides {
		if s.rebuilt > 0 {
			promiseSuffix = "-after-a-rebuild"
		}
	}
	// An edit of the rollout block re-plans the batches under the roll:
	// the picture inherited from the old plan may sit outside the new one
	// until the side behind catches up, and the attempts open at a budget
	// edit finish under the old budget, which is reported in classes of
	// their own.
	if st.edit.kind != huntEditNone {
		promiseSuffix = "-after-edit"
	}
	if st.edit.kind == huntEditGroupBudget {
		budgetClass = "budget-after-edit"
	}
	// A rollback turns the roll around: each side's batches are planned
	// again from the Instances still on the revision it returns to, so the
	// two sides' positions right after it are the previous roll's. With
	// nothing else re-planning the batches since, the gap between the two
	// sides never widens past the one the rollback found.
	rollbackBound := false
	if st.reversed() {
		rollbackBound = promiseSuffix == ""
		promiseSuffix = "-after-a-rollback"
	}
	svc := w.service(st)
	// Under a partition a side stops at its movable count while the other
	// runs to its end, and a partition raised or cleared moves the plan
	// under the counts already started; where the batch step counts held
	// Instances among the batches the promise reads off its marks anyway.
	partitionMoved := false
	for _, s := range st.sides {
		if s.parts < w.cfg.parts {
			partitionMoved = true
		}
	}
	if st.partitioned() || partitionMoved {
		promiseSuffix = "-under-partition"
	}
	// During a protocol change the pairing waiver lets a side run ahead
	// of a peer the pairing gate holds still, by design.
	if w.transition(st) {
		promiseSuffix = "-in-protocol-change"
	}
	// (c) the budgets: never more of a side's attempts in flight than its
	// own budget on the arm it rolls on, nor than the group's.
	for i, m := range w.cfg.members {
		s := st.sides[i]
		n := int32(len(s.rows))
		own := int32(-1)
		if m.budget != "" {
			own = utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(m.budget)), n, true)
		}
		// A canary unit carries no group budget and the gates ask none; the
		// group's budgets apply while the group stands, and with the group
		// removed each side has its own budget alone.
		groupBudgets := w.cfg.canary == nil && w.grouped(st)
		var pacing v1beta1.CoordinationPacing
		if groupBudgets {
			var groupPacing *v1beta1.CoordinationPacing
			if ru := svc.Spec.Rollout.Groups[0].RollingUpdate; ru != nil {
				groupPacing = &v1beta1.CoordinationPacing{MaxUnavailable: ru.MaxUnavailable, MaxSurge: ru.MaxSurge}
			}
			pacing = pacingWithDefaults(groupPacing, w.cfg.defaults)
		}
		if m.surges() {
			open := s.count(huntOpenSurge, huntReplacementHeld)
			var slots int32
			for _, r := range s.rows {
				if r.surgeSlot(m.isGang()) {
					slots++
				}
			}
			if own >= 0 && slots > own {
				add(budgetClass, "%s holds %d surge slots (%d in flight) against its own budget of %d", m.comp, slots, open, own)
			}
			if group := MaxSurgeBudget(pacing, n); groupBudgets && open > group {
				add(budgetClass, "%s has %d surges in flight against the group budget of %d", m.comp, open, group)
			}
		} else {
			var open int32
			for _, r := range s.rows {
				if r.openDrain() {
					open++
				}
			}
			if own >= 0 && open > own {
				add(budgetClass, "%s has %d Instances out of rotation against its own budget of %d", m.comp, open, own)
			}
			// The group arm counts every Instance not serving, the failed
			// ones included; a scale-up's new Instances are not drains and
			// are left out.
			// A drain opened with no gate asked, on an Instance the serving
			// count already left out, is not the group's admission.
			if group := MaxUnavailableBudget(pacing, n); groupBudgets && open-s.exemptOpen()+s.count(huntFailedUnserving) > group {
				add(budgetClass, "%s has %d Instances out of rotation (%d opened with no gate asked) beside %d failed against the group unavailability budget of %d", m.comp, open, s.exemptOpen(), s.count(huntFailedUnserving), group)
			}
		}
	}
	// (b) the batch promise: the two sides' started counts sit inside one
	// planned batch of each other; the engine's reading of the rows agrees
	// with the model's.
	// A canary unit answers to the pairing gate alone (EvaluateUpdateGate
	// returns before the batch step), so its batches are not read.
	eng, dec := w.pairIndex()
	if eng >= 0 && dec >= 0 && w.cfg.canary == nil {
		gate := ResolveGateContextWithDefaults(w.ctx, w.client(st), svc, v1beta1.EngineComponent, w.cfg.defaults)
		gate.TargetRevision = w.rollTargetFor(st, eng)
		batch, err := gate.readBatchStep()
		if err != nil {
			add("read", "batch step read failed: %v", err)
		}
		if batch.governs && w.cfg.shareBound {
			// (f) the two sides' new-version shares against the tolerance.
			out = append(out, w.shareReadings(st, batch)...)
		}
		if batch.governs {
			// (i) the Instances a rollback is rebuilding, read as serving.
			out = append(out, w.rollbackReadings(st, batch)...)
		}
		if w.cfg.ratioReadings {
			// (g) the ratio rule's promises: the shares in the leading
			// side's units, the serving ratio, the reference.
			out = append(out, w.ratioReadings(st, batch)...)
		}
		if w.cfg.eventReadings {
			// (h) every hold named by a hold reason, the batches' fill,
			// the two sides' waits on each other.
			out = append(out, w.eventReadings(st, batch)...)
		}
		if batch.governs {
			members, positions := w.batchPositions(st, batch, add)
			if pe, pd := positions[v1beta1.EngineComponent], positions[v1beta1.DecoderComponent]; len(positions) == 2 && batchGap(pe, pd) > 0 {
				class := "batch-promise" + promiseSuffix
				if keepRatio, _ := huntRulesOf(w.cfg.defaults); keepRatio {
					class = "ratio-promise" + promiseSuffix
				}
				// After a rollback the two sides stand where the previous
				// roll left them: a gap no wider than the one the rollback
				// found is that roll's, a wider one is the step's own.
				if rollbackBound && promiseSuffix == "-after-a-rollback" && batchGap(pe, pd) > st.rollbackGap {
					class = strings.TrimSuffix(class, "-after-a-rollback")
				}
				// A side whose promoted Instance is not yet counted as serving
				// is coming up on its own; nothing the other side does moves
				// it, so a start the other side opens meanwhile is never the
				// pairing waiver's: the gap is this class's while that start
				// is in flight. A gap that predates the promote keeps the
				// class it was read in. A scale re-plans the marks and a
				// partition ends one side early, so the gap is theirs there,
				// as the suffix says.
				behind, ahead := eng, dec
				if pe.lo > pd.hi {
					behind, ahead = dec, eng
				}
				// A peer the pairing gate holds still waits on the other
				// side's pods, not its own: that gap is the waiver's. A start
				// the gate stack was not asked for is a short gang's rebuild,
				// as a drain or as a surge, and the rebuild suffix names that
				// gap.
				behindHeldByPairing := st.holds[behind] != nil && st.holds[behind].gate == v1beta1.RolloutHoldGatePairing
				if !scaled && !st.partitioned() && !behindHeldByPairing && st.sides[behind].count(huntPromotedUncounted) > 0 && st.sides[ahead].inFlight() && st.sides[ahead].exemptInFlight() == 0 {
					class = "runs-ahead-of-a-peer-coming-up"
				}
				se, sd := members[v1beta1.EngineComponent], members[v1beta1.DecoderComponent]
				add(class, "the engine has started %d (batch %d..%d of marks %v) while the decoder has started %d (batch %d..%d of marks %v); more than one batch apart", se.started(), pe.lo, pe.hi, huntMarks(se), sd.started(), pd.lo, pd.hi, huntMarks(sd))
			}
		}
	}
	// (d) the partner promise during a protocol change
	// (strandedReplacementReadings).
	out = append(out, w.strandedReplacementReadings(st, eng, dec, scaled)...)
	// (e) the first-side rule: a start of the other side opened only once
	// the named side's batch had settled, on the rows as written. During a
	// protocol change the pairing waiver lets the other side go first by
	// design, so the reading is reported in a class of its own there.
	if st.outOfTurn != "" {
		class := "starts-before-the-named-side"
		if w.transition(st) {
			class += "-in-protocol-change"
		}
		add(class, "%s", st.outOfTurn)
	}
	// (i) the pairable capacity: a step the gates admitted took a side below
	// its pairable minimum or left the fleet with no pairable pair
	// (pairing_gate_model_test.go).
	if w.cfg.pairingReadings && st.pairingBreach != nil {
		out = append(out, *st.pairingBreach)
	}
	// (a) progress: the roll is not done, nothing can move on its own,
	// and no side was admitted a start.
	if !st.done() && len(w.progressMoves(st)) == 0 {
		class, message := w.classifyStandstill(st)
		if w.cfg.pairingReadings {
			class, message = w.pairingStandstill(st, class, message)
		}
		add(class, "%s", message)
	}
	return out
}

// strandedReplacementReadings is the partner promise during a protocol
// change: a surging side's old pod leaves rotation only once its
// replacement has a partner, so no Instance of that side serves the new
// cohort alone while the other side still serves the old cohort and none
// of the new (a rebuilt Instance promoted but not yet counted as serving
// is on the new cohort: its pod is what the drain gate read). A side scaled
// up builds its new Instances on the new cohort with no old pod to keep,
// so after a scale the reading is reported in a class of its own. After a
// further push three cohorts are in play and an old pod may leave once its
// own partners are gone, so the reading stops there.
func (w *huntWorld) strandedReplacementReadings(st *huntState, eng, dec int, scaled bool) []huntFinding {
	if !w.transition(st) || st.pushed || eng < 0 || dec < 0 {
		return nil
	}
	var out []huntFinding
	for _, pair := range [][2]int{{eng, dec}, {dec, eng}} {
		i, o := pair[0], pair[1]
		if !w.cfg.members[i].surges() {
			continue
		}
		onNew := st.sides[i].count(huntPromoted, huntPromotedShort, huntPromotedUnreported, huntPromotedLostNode)
		// The Instances a rollback found on the revision it returns to
		// are the previous roll's, partnerless within its pairable
		// minimum; only the turned roll's own replacements are read.
		onNew -= min32(st.sides[i].inherited, onNew)
		otherNew := st.sides[o].count(huntPromoted, huntPromotedUncounted, huntPromotedShort, huntReplacementHeld, huntReplacementShort, huntPromotedUnreported, huntPromotedLostNode, huntSourceDrainBlocked)
		otherOld := st.sides[o].count(huntOld, huntOldShort, huntRetriesStopped, huntFailedSurge, huntOpenSurge, huntReplacementHeld, huntSurgeShort, huntFailedGangKept,
			huntOldLostMember, huntOldUnreported, huntOldLostNode, huntSurgeLostNode, huntSurgeRefused)
		if onNew == 0 || otherNew > 0 || otherOld == 0 {
			continue
		}
		class := "stranded-replacement"
		if scaled {
			class += "-after-scale"
		}
		out = append(out, huntFinding{class: class, state: st, message: fmt.Sprintf("%d %s Instance(s) serve the new cohort with no %s partner while %d %s Instance(s) still serve the old cohort (%s)",
			onNew, w.cfg.members[i].comp, w.cfg.members[o].comp, otherOld, w.cfg.members[o].comp, st.sides[i])})
	}
	return out
}

// batchRowReading is the model's reading of one row the way the batch step
// classifies it: promoted on the target, open toward it, ended toward it
// with a retry record standing, else untouched. A row on the middle
// revision after a further push is off the target, and an attempt still
// on the middle revision does not move toward the target. An
// ended attempt, a stopped one among them, counts as ended only while a
// retry record stands; with none the side starts it again at once and the
// batch step reads it as still to start.
func (s huntSide) batchRowReading(idx int) batchRowClass {
	r := s.rows[idx]
	switch {
	case s.attemptOnMiddle(idx):
		return batchRowUntouched
	case r.promotedRow():
		if s.srcMiddle(idx) {
			return batchRowUntouched
		}
		return batchRowPromoted
	case r.open():
		return batchRowOpen
	case r.ended() && s.schedule != scheduleNone:
		return batchRowEnded
	}
	return batchRowUntouched
}

// huntMarks lists a member's batch marks as mark reads them, planned or
// in steps, up to its desired count.
func huntMarks(m batchMember) []int32 {
	var out []int32
	for b := int32(1); ; b++ {
		out = append(out, m.mark(b))
		if m.mark(b) >= m.desired || b > 64 {
			break
		}
	}
	return out
}

// batchWaitPeer names the member a batch-step hold waits on, read off the
// hold's text ("<me> batch B waits for <peer> batch B-1 to serve ...").
func (w *huntWorld) batchWaitPeer(reason string) (int, bool) {
	m := huntBatchWaitPattern.FindStringSubmatch(reason)
	if m == nil {
		return 0, false
	}
	for i, member := range w.cfg.members {
		if string(member.comp) == m[1] {
			return i, true
		}
	}
	return 0, false
}

var huntBatchWaitPattern = regexp.MustCompile(`waits for (\w+) batch`)

// playOut plays the roll from a state with the world's progress moves
// alone until it is complete or nothing can move, within a bound.
func (w *huntWorld) playOut(st *huntState) *huntState {
	var total int
	for _, s := range st.sides {
		total += len(s.rows)
	}
	for step := 0; step < 4*total+16 && !st.complete(); step++ {
		mv, ok := w.nextProgress(st, step)
		if !ok {
			break
		}
		st = w.apply(st, mv)
	}
	return st
}

// withoutPartitions is the state once every partition is cleared and the
// roll played on from there: the test that tells a hold the operator's
// partition explains from one nothing explains.
func (w *huntWorld) withoutPartitions(st *huntState) *huntState {
	probe := st.clone()
	for i := range probe.sides {
		probe.sides[i].partition = 0
		probe.sides[i].canaryPartition = -1
	}
	probe.canaryStep = -1
	w.settle(probe, w.order(len(probe.path)))
	return w.playOut(probe)
}

// classifyStandstill names why a state that cannot move is what it is. Every
// side with Instances to move is Held ("held"), a side whose hold reads
// Held because the named first side's retry record is Held counting as
// held with it, the roll held as a whole; one side is Held and the
// other waits on something only the Held side could change
// ("waits-on-held-peer"; where a spent ladder leaves the other side is
// left open); a side waits on a peer's batch whose only
// unserved Instances its partition holds ("waits-on-held-instances"); the
// ratio rule holds a side where the batch step does not govern
// ("ratio-rule"); a hold the
// operator's partition explains, because clearing it lets the roll finish
// ("partition-paced"); or no side is Held and the sides wait on each other
// or on themselves ("no-way-out").
func (w *huntWorld) classifyStandstill(st *huntState) (class, message string) {
	var lines []string
	held, waiting := 0, 0
	ratioRule, onHeldInstances, pairingHeld, lostNode := false, false, false, false
	for i, m := range w.cfg.members {
		s := st.sides[i]
		if s.schedule == scheduleHeld && !s.complete() {
			// A Held side with Instances left is Held whatever its
			// partition keeps back.
			held++
			lines = append(lines, fmt.Sprintf("%s is Held with %s", m.comp, s))
			continue
		}
		if s.done() {
			continue
		}
		h := st.holds[i]
		if h != nil && h.gate == v1beta1.RolloutHoldGateHeld {
			// The gate named the first side's Held record: this side is
			// held with it, under the operator's two ways out.
			held++
			lines = append(lines, fmt.Sprintf("%s (%s) is held with the named side: %s", m.comp, s, h))
			continue
		}
		waiting++
		if s.lostNodeStands() {
			lostNode = true
		}
		if h != nil && h.gate == v1beta1.RolloutHoldGateRatio && (strings.Contains(h.reason, "would outpace") || strings.Contains(h.reason, "would skew")) {
			ratioRule = true
		}
		// A side the pairing gates hold still: its start held under Pairing,
		// or its last old pod kept by the drain gate for the other side's
		// old Instances.
		if (h != nil && h.gate == v1beta1.RolloutHoldGatePairing) || s.count(huntReplacementHeld) > 0 {
			pairingHeld = true
		}
		if h != nil && h.gate == v1beta1.RolloutHoldGateRatio {
			if p, ok := w.batchWaitPeer(h.reason); ok && p != i {
				if peer := st.sides[p]; peer.partition > 0 && peer.done() {
					onHeldInstances = true
				}
			}
		}
		lines = append(lines, fmt.Sprintf("%s (%s) holds on %s", m.comp, s, h))
	}
	// A hold beside the operator's partition is the partition's when
	// clearing it lets the roll finish, whatever gate names the hold. A
	// canary's partition, a step's or the 0 its rollback and its end write,
	// is the engine's own and outranks the operator's, so a stop under one
	// is never excused by a partition.
	var cleared *huntState
	if st.partitioned() && !w.canaryStepStands(st) {
		cleared = w.withoutPartitions(st)
	}
	switch {
	case waiting == 0:
		class = "held"
	case held > 0:
		class = "waits-on-held-peer"
	case lostNode && !w.cfg.forceDelete:
		// A pod on a node that stopped reporting stands until an operator
		// removes it when no force-delete policy is configured: the attempt
		// or the repair behind it waits, and so does whatever waits on that
		// Instance's batch or budget.
		class = "waits-on-a-lost-node-without-force-delete"
	case w.sidesWaitOnEachOther(st):
		class = "sides-wait-on-each-other"
	case cleared != nil && cleared.complete():
		class = "partition-paced"
	case ratioRule && pairingHeld:
		// The lockstep holds this side for running ahead of a peer the
		// pairing gates hold still, and that peer can only move after this
		// side has: the two wait on each other. The batch step and the
		// lockstep both leave such a peer out (the one-mover waiver), so the
		// picture is a defect.
		class = "ratio-rule-waits-on-the-pairing-gate"
	case onHeldInstances:
		// The peer's batches were counted over every Instance, the ones
		// its partition holds included, so its last batches never serve
		// and this side waits on them for good; a batch step that counts
		// the held Instances apart ends the peer at its movable count.
		class = "waits-on-held-instances"
	case ratioRule:
		// The serving band or the fraction lockstep of the ratio rule
		// holds a waiting side: the rule the batch step replaces where it
		// governs, which runs only where the step is off for the pair,
		// with its own known wedges (a side of one or two Instances the
		// band never admits; an anchor a scale left stale). The other
		// side's wait, if any, stands on that one.
		class = "ratio-rule"
	case cleared != nil:
		// When the roll stops again without any partition, that stop
		// names the class.
		class, message = w.classifyStandstill(cleared)
		if class == "partition-paced" {
			class = "no-way-out"
		}
		return class, strings.Join(lines, "; ") + "; with every partition cleared the roll stops as " + class + ": " + message
	default:
		class = "no-way-out"
	}
	return class, strings.Join(lines, "; ")
}

// canaryStepStands reports a canary's partition written on a side: a
// step's, or the 0 its rollback and its end write, which outranks the
// operator's partition as the replica's reader does.
func (w *huntWorld) canaryStepStands(st *huntState) bool {
	for _, s := range st.sides {
		if s.canaryPartition >= 0 {
			return true
		}
	}
	return false
}

// huntReport is what one configuration's exploration found.
type huntReport struct {
	states, depth, runs, probes int
	findings                    map[string]huntFinding
	counts                      map[string]int
}

// note records the findings at one state.
func (rep *huntReport) note(findings []huntFinding) {
	for _, f := range findings {
		rep.counts[f.class]++
		if _, ok := rep.findings[f.class]; !ok {
			rep.findings[f.class] = f
		}
	}
}

// explore walks the state space breadth-first from the first admission to
// a bound, then tries the operator's moves at every moment of a plain roll
// with the roll played to its end.
func (w *huntWorld) explore(maxStates, maxDepth int) huntReport {
	rep := huntReport{findings: map[string]huntFinding{}, counts: map[string]int{}}
	start := w.initial()
	w.settle(start, w.order(0))
	visited := map[string]bool{start.key(): true}
	queue := []*huntState{start}
	for len(queue) > 0 && rep.states < maxStates {
		st := queue[0]
		queue = queue[1:]
		rep.states++
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
		rep.note(w.check(st))
		if st.complete() || len(st.path) >= maxDepth {
			if len(st.path) >= maxDepth && !st.done() {
				rep.counts["depth"]++
				if _, ok := rep.findings["depth"]; !ok {
					rep.findings["depth"] = huntFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
				}
			}
			continue
		}
		for _, mv := range append(w.progressMoves(st), w.adverseMoves(st)...) {
			child := w.apply(st, mv)
			k := child.key()
			if visited[k] {
				continue
			}
			visited[k] = true
			queue = append(queue, child)
		}
	}
	if w.cfg.probes {
		w.probe(&rep, func(st *huntState) {
			if k := st.key(); !visited[k] {
				visited[k] = true
				rep.states++
				rep.note(w.check(st))
			}
		})
	}
	return rep
}

// huntSeed is one moment of a plain roll an operator move is tried at.
type huntSeed struct {
	name  string
	state *huntState
}

// nextProgress picks the progress move a plain roll takes next: the first
// on the side whose turn it is (the sides alternate), else the first at
// all.
func (w *huntWorld) nextProgress(st *huntState, step int) (huntMove, bool) {
	moves := w.progressMoves(st)
	if len(moves) == 0 {
		return huntMove{}, false
	}
	side := step % len(st.sides)
	for _, mv := range moves {
		if mv.side == side {
			return mv, true
		}
	}
	return moves[0], true
}

// seeds plays the roll with no adversary and keeps the moments an
// operator move can land at: idle (before the first admission; the move
// lands before the first pass), settling (the first batch admitted,
// nothing served yet), rotating (half the Instances promoted), draining
// (one side has nothing left to start), last (one Instance left to
// promote), and held (the rotating moment with one open attempt failed
// and the retries spent).
func (w *huntWorld) seeds() []huntSeed {
	out := []huntSeed{{name: "idle", state: w.initial()}}
	start := w.initial()
	w.settle(start, w.order(0))
	out = append(out, huntSeed{name: "settling", state: start})
	var total int32
	for _, m := range w.cfg.members {
		total += m.desired
	}
	st := start
	marked := map[string]bool{}
	mark := func(name string, when bool) {
		if when && !marked[name] {
			marked[name] = true
			out = append(out, huntSeed{name: name, state: st})
		}
	}
	for step := 0; step < int(4*total)+16 && !st.complete(); step++ {
		mv, ok := w.nextProgress(st, step)
		if !ok {
			break
		}
		st = w.apply(st, mv)
		var promoted int32
		oneDrained := false
		for _, s := range st.sides {
			promoted += s.count(huntPromoted)
			if s.count(huntOld) == 0 && !s.complete() {
				oneDrained = true
			}
		}
		mark("rotating", 2*promoted >= total && !st.complete())
		mark("draining", oneDrained)
		mark("last", total-promoted == 1)
		mark("canary-step", st.canaryStep > 0 && !st.complete())
	}
	if w.cfg.rollbacks > 0 || w.cfg.canaryRollbacks > 0 || w.cfg.pauses > 0 {
		// Each moment again once the operator rolled back or paused there,
		// so the other moves are tried after a rollback and under a pause.
		var turned []huntSeed
		for _, seed := range out {
			for _, mv := range w.turnMoves(seed.state) {
				turned = append(turned, huntSeed{name: seed.name + "+" + mv.kind, state: w.apply(seed.state, mv)})
			}
		}
		out = append(out, turned...)
	}
	if w.cfg.fails > 0 {
		from := start
		for _, seed := range out {
			if seed.name == "rotating" {
				from = seed.state
			}
		}
	failed:
		for i, s := range from.sides {
			for idx, r := range s.rows {
				if r == huntOpenDrain || r == huntOpenInPlace || r == huntOpenSurge {
					held := w.apply(from, huntMove{kind: "fail", side: i, idx: idx})
					if held.sides[i].schedule == scheduleBackoff {
						held = w.apply(held, huntMove{kind: "spent", side: i})
					}
					out = append(out, huntSeed{name: "held", state: held})
					break failed
				}
			}
		}
	}
	return out
}

// probe tries every operator move at every seed moment and plays the roll
// to its end after it, noting each state reached.
func (w *huntWorld) probe(rep *huntReport, note func(*huntState)) {
	var total int
	for _, m := range w.cfg.members {
		total += int(m.desired)
	}
	for _, seed := range w.seeds() {
		for _, mv := range w.adverseMoves(seed.state) {
			if !mv.event() {
				continue
			}
			rep.probes++
			st := w.apply(seed.state, mv)
			for step := 0; ; step++ {
				note(st)
				if st.complete() || step > 4*total+16 {
					break
				}
				next, ok := w.nextProgress(st, step)
				if !ok {
					break
				}
				st = w.apply(st, next)
			}
		}
	}
}

// randomRuns plays whole runs with a fixed seed, favouring the moves that
// finish a roll, to reach the depths the breadth-first walk does not.
func (w *huntWorld) randomRuns(rep *huntReport, seed int64, runs, maxDepth int) {
	rng := rand.New(rand.NewSource(seed))
	for r := 0; r < runs; r++ {
		st := w.initial()
		w.settle(st, w.order(0))
		rep.runs++
		for depth := 0; ; depth++ {
			rep.note(w.check(st))
			if st.complete() {
				break
			}
			progress, adverse := w.progressMoves(st), w.adverseMoves(st)
			if len(progress) == 0 && len(adverse) == 0 {
				break
			}
			if depth >= maxDepth {
				if !st.done() {
					rep.counts["depth"]++
					if _, ok := rep.findings["depth"]; !ok {
						rep.findings["depth"] = huntFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
					}
				}
				break
			}
			var mv huntMove
			if len(progress) == 0 || (len(adverse) > 0 && rng.Intn(4) == 0) {
				mv = adverse[rng.Intn(len(adverse))]
			} else {
				mv = progress[rng.Intn(len(progress))]
			}
			st = w.apply(st, mv)
			if len(st.path) > rep.depth {
				rep.depth = len(st.path)
			}
		}
	}
}

func (st *huntState) String() string {
	var parts []string
	for i, s := range st.sides {
		parts = append(parts, fmt.Sprintf("side %d [%s] hold=%s", i, s, st.holds[i]))
	}
	out := strings.Join(parts, " | ")
	if st.reference != nil {
		out += " reference=" + referenceRatioText(st.reference)
	}
	if st.policy.follower != "" {
		out += " policy=" + st.policy.String()
	}
	if st.paused {
		out += " paused"
	}
	if st.reversed() {
		out += fmt.Sprintf(" rollbackGap=%d", st.rollbackGap)
	}
	return out
}

func huntPath(st *huntState) string {
	parts := make([]string, 0, len(st.path))
	for _, mv := range st.path {
		parts = append(parts, mv.String())
	}
	return strings.Join(parts, " -> ")
}

// huntShapes are the prefill:decode counts explored, the reference shapes
// among them.
var huntShapes = [][2]int32{{1, 1}, {2, 1}, {3, 3}, {6, 2}, {2, 6}, {4, 8}, {8, 4}, {12, 8}, {16, 8}, {24, 8}, {12, 4}}

var huntStrategies = []v1beta1.UpdateStrategyType{v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible}

var huntBudgets = []string{"1", "2", "10%", "25%", "50%"}

type huntSetting struct {
	name     string
	defaults GroupDefaults
}

func huntSettings() []huntSetting {
	out := []huntSetting{{name: "off"}}
	for _, side := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		out = append(out, huntSetting{name: "firstSide=" + string(side), defaults: huntRules(false, nil, side)})
	}
	for _, tol := range []int32{0, 5, 10, 20, 100} {
		for _, side := range []v1beta1.ComponentType{"", v1beta1.EngineComponent, v1beta1.DecoderComponent} {
			name := fmt.Sprintf("keepRatio=%d", tol)
			if side != "" {
				name += "/firstSide=" + string(side)
			}
			out = append(out, huntSetting{name: name, defaults: huntRules(true, ptr.To(tol), side)})
		}
	}
	return out
}

// huntConfigs lists the configurations to explore: every reference shape
// under the three strategies and the settings the chart ships, the
// reference shapes with the operator's partitions and scales tried at
// every moment, gangs of two and four pods, then a seeded sample over the
// whole space.
func huntConfigs(full bool) []huntConfig {
	var out []huntConfig
	mk := func(shape [2]int32, es, ds v1beta1.UpdateStrategyType, budget string, setting huntSetting, protocol bool, router v1beta1.UpdateStrategyType, attempts int32, tolerance *int32, gang int32) huntConfig {
		cfg := huntConfig{
			members: []huntMember{
				{comp: v1beta1.EngineComponent, desired: shape[0], strategy: es, budget: budget, gang: gang},
				{comp: v1beta1.DecoderComponent, desired: shape[1], strategy: ds, budget: budget, gang: gang},
			},
			defaults: setting.defaults, protocolChange: protocol, maxAttempts: attempts, tolerance: tolerance,
			fails: 2, parks: 1, scales: 1, parts: 1, losses: 1, scaleDeltas: []int32{1, -1, 2, -2},
		}
		if router != "" {
			cfg.members = append(cfg.members, huntMember{comp: v1beta1.RouterComponent, desired: 2, strategy: router, budget: budget})
		}
		cfg.name = cfg.String() + " " + setting.name
		return cfg
	}
	// probed marks a configuration whose operator moves are tried at every
	// moment of a plain roll, with the roll played to its end after each.
	probed := func(cfg huntConfig, parts int8) huntConfig {
		cfg.probes = true
		cfg.parts = parts
		cfg.name += " probed"
		return cfg
	}
	settings := huntSettings()
	chart := huntSetting{name: "chart", defaults: huntRules(true, ptr.To(int32(5)), v1beta1.DecoderComponent)}
	fixed := []huntSetting{{name: "off"}, chart,
		{name: "keepRatio=5", defaults: huntRules(true, ptr.To(int32(5)), "")},
		{name: "firstSide=decoder", defaults: huntRules(false, nil, v1beta1.DecoderComponent)},
	}
	inPlaceSettings := []huntSetting{{name: "off"}, chart}
	if full {
		fixed = append(fixed,
			huntSetting{name: "keepRatio=5/firstSide=engine", defaults: huntRules(true, ptr.To(int32(5)), v1beta1.EngineComponent)},
			huntSetting{name: "keepRatio=100/firstSide=decoder", defaults: huntRules(true, ptr.To(int32(100)), v1beta1.DecoderComponent)},
			huntSetting{name: "keepRatio=0", defaults: huntRules(true, ptr.To(int32(0)), "")},
		)
	}
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	pairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {inPlace, inPlace}, {recreate, surge}, {surge, recreate}}
	shapes := [][2]int32{{12, 8}, {6, 2}, {2, 6}, {3, 3}}
	if full {
		shapes = append(shapes, [2]int32{16, 8}, [2]int32{12, 4}, [2]int32{2, 1})
	}
	for _, shape := range shapes {
		for _, pair := range pairs {
			for _, setting := range fixed {
				for _, protocol := range []bool{false, true} {
					out = append(out, mk(shape, pair[0], pair[1], "10%", setting, protocol, "", 3, nil, 0))
				}
			}
		}
	}
	// 8 prefill beside 4 decode, one side rebuilt one at a time beside the
	// other surging one at a time, under the chart's settings during a
	// protocol change: the shape where a rebuilt Instance promoted but not
	// yet counted as serving leaves its side exactly at its pairable
	// minimum, with the moves tried at every moment of the roll.
	for _, pair := range [][2]v1beta1.UpdateStrategyType{{recreate, surge}, {surge, recreate}} {
		out = append(out, probed(mk([2]int32{8, 4}, pair[0], pair[1], "10%", chart, true, "", 3, nil, 0), 1))
	}
	for _, shape := range [][2]int32{{3, 3}, {12, 8}, {6, 2}} {
		for _, tol := range []int32{0, 5, 50} {
			for _, setting := range inPlaceSettings {
				out = append(out, mk(shape, inPlace, inPlace, "10%", setting, false, "", 3, ptr.To(tol), 0))
				out = append(out, mk(shape, inPlace, recreate, "10%", setting, false, "", 3, ptr.To(tol), 0))
			}
		}
	}
	// The reference shapes with the operator's partitions (raised,
	// lowered, cleared, on one side or both) and scales (one side, both,
	// the ratio) tried at every moment of the roll, under RecreatePod and
	// SurgeThenDrain at 10 percent, with and without the group's ratio
	// tolerance, with and without the chart's settings.
	partitionShapes := [][2]int32{{12, 8}, {16, 8}, {24, 8}, {6, 2}}
	partitionSettings := []huntSetting{{name: "off"}, chart}
	partitionTolerances := []*int32{nil}
	if full {
		partitionTolerances = append(partitionTolerances, ptr.To(int32(20)))
	}
	// edited marks a configuration whose rollout block is also edited at
	// every moment: the group's tolerance moved or the rule added, the
	// rule removed, the group removed.
	edited := func(cfg huntConfig) huntConfig {
		cfg.edits = 1
		cfg.name += " edits=1"
		return cfg
	}
	editedShapes := map[[2]int32]bool{{12, 8}: true, {6, 2}: true}
	for _, shape := range partitionShapes {
		for _, pair := range [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}} {
			for _, setting := range partitionSettings {
				for _, tolerance := range partitionTolerances {
					cfg := probed(mk(shape, pair[0], pair[1], "10%", setting, false, "", 3, tolerance, 0), 3)
					if full || editedShapes[shape] {
						cfg = edited(cfg)
					}
					out = append(out, cfg)
					if full {
						out = append(out, edited(probed(mk(shape, pair[0], pair[1], "10%", setting, true, "", 3, tolerance, 0), 3)))
					}
				}
			}
		}
	}
	// Gangs: Instances of two and four pods, surging through a slot of
	// their own or rebuilt whole, under a protocol change and without one,
	// a member lost mid-surge among the moves.
	gangShapes := [][2]int32{{6, 2}, {3, 3}}
	gangSizes := []int32{2}
	gangSettings := []huntSetting{{name: "off"}, chart}
	if full {
		gangShapes = [][2]int32{{12, 8}, {6, 2}, {2, 6}, {3, 3}, {2, 1}, {16, 8}}
		gangSizes = []int32{2, 4}
		gangSettings = append(gangSettings, huntSetting{name: "keepRatio=5", defaults: huntRules(true, ptr.To(int32(5)), "")})
	}
	for _, shape := range gangShapes {
		for _, pair := range [][2]v1beta1.UpdateStrategyType{{surge, surge}, {recreate, surge}, {surge, recreate}, {inPlace, surge}} {
			for _, gang := range gangSizes {
				for _, setting := range gangSettings {
					out = append(out, probed(mk(shape, pair[0], pair[1], "10%", setting, true, "", 3, nil, gang), 1))
					if full && setting.name == "off" {
						out = append(out, probed(mk(shape, pair[0], pair[1], "10%", setting, false, "", 3, nil, gang), 1))
					}
				}
			}
		}
	}
	if full {
		// Gangs of two and four on both sides at the reference shapes
		// without a protocol change, with the partitions and scales
		// tried at every moment.
		for _, shape := range [][2]int32{{12, 8}, {16, 8}, {6, 2}} {
			for _, gang := range []int32{2, 4} {
				out = append(out, probed(mk(shape, surge, surge, "10%", huntSetting{name: "off"}, false, "", 3, nil, gang), 2))
				out = append(out, probed(mk(shape, recreate, recreate, "10%", huntSetting{name: "off"}, false, "", 3, nil, gang), 2))
			}
		}
	}
	// The push reaching one side's status one write after the other's: the
	// reference shapes under the settings that name a first side, with the
	// engine's, the decoder's or both statuses predating the push at the
	// first pass, and nothing else in the way of the roll.
	behindShapes := [][2]int32{{12, 8}, {6, 2}}
	behindPairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}}
	behindSettings := []huntSetting{chart, {name: "firstSide=engine", defaults: huntRules(false, nil, v1beta1.EngineComponent)}}
	if full {
		behindShapes = append(behindShapes, [2]int32{2, 6}, [2]int32{3, 3})
		behindPairs = append(behindPairs, [2]v1beta1.UpdateStrategyType{recreate, surge}, [2]v1beta1.UpdateStrategyType{surge, recreate})
		behindSettings = append(behindSettings, huntSetting{name: "keepRatio=5", defaults: huntRules(true, ptr.To(int32(5)), "")},
			huntSetting{name: "firstSide=decoder", defaults: huntRules(false, nil, v1beta1.DecoderComponent)})
	}
	for _, shape := range behindShapes {
		for _, pairing := range behindPairs {
			for _, setting := range behindSettings {
				for behind := uint8(1); behind <= 3; behind++ {
					cfg := mk(shape, pairing[0], pairing[1], "10%", setting, false, "", 3, nil, 0)
					cfg.fails, cfg.parks, cfg.scales, cfg.parts = 0, 0, 0, 0
					cfg.behindPush = behind
					cfg.name = cfg.String() + " " + setting.name
					out = append(out, cfg)
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(20261019))
	// behindRng draws the statuses predating the push for the sampled
	// configurations apart, so the sample itself is the one it always was.
	behindRng := rand.New(rand.NewSource(20261020))
	sample := 60
	if full {
		sample = 300
	}
	for n := 0; n < sample; n++ {
		shape := huntShapes[rng.Intn(len(huntShapes))]
		es, ds := huntStrategies[rng.Intn(len(huntStrategies))], huntStrategies[rng.Intn(len(huntStrategies))]
		budget := huntBudgets[rng.Intn(len(huntBudgets))]
		setting := settings[rng.Intn(len(settings))]
		protocol := rng.Intn(3) == 0
		var router v1beta1.UpdateStrategyType
		if rng.Intn(4) == 0 {
			router = huntStrategies[rng.Intn(2)]
		}
		attempts := int32(3)
		if rng.Intn(4) == 0 {
			attempts = 1
		}
		var tolerance *int32
		if rng.Intn(3) == 0 {
			tolerance = ptr.To([]int32{0, 5, 20, 50, 100}[rng.Intn(5)])
		}
		gang := []int32{0, 0, 2, 4}[rng.Intn(4)]
		cfg := mk(shape, es, ds, budget, setting, protocol, router, attempts, tolerance, gang)
		cfg.parts = int8(rng.Intn(3))
		cfg.losses = int8(rng.Intn(2))
		if full && rng.Intn(3) == 0 {
			cfg = probed(cfg, cfg.parts)
		}
		if cfg.behindPush = uint8(behindRng.Intn(4)); cfg.behindPush != 0 {
			cfg.name = cfg.String() + " " + setting.name
		}
		out = append(out, cfg)
	}
	return out
}

// huntExplore explores every configuration keep admits and reports each
// broken invariant with the shortest path to it. A finding of class
// no-way-out, budget, classifier, batch-promise, ratio-promise or
// runs-ahead-of-a-peer-coming-up (a side more than one batch ahead of a
// peer whose promoted Instance is not yet counted as serving: that peer
// waits on its own pod, not on the other side) fails the test.
// waits-on-held-peer records where a spent ladder leaves the other side
// when the Held side is not the named first side (a side whose hold names
// a Held named side is held with it, in the held class), which is not
// decided yet; the after-scale, under-partition
// and in-protocol-change classes record where a scale re-plans the
// batches, where a partition ends one side early and where the pairing
// waiver lets a side run ahead by design (starts-before-the-named-side
// among them: outside a protocol change a start of the other side admitted
// before the named side's batch settled fails the test); the after-edit
// classes record where an edit of the rollout block re-plans the batches
// under a picture the old plan left outside the new one, or lowers the
// group's budget under attempts already open, which finish; partition-paced
// records a hold the operator's partition explains; all are reported only.
// waits-on-held-instances fails the test: the batch step counts a
// partition's held Instances apart from the batches, so no side waits on
// them.
func huntExplore(t *testing.T, keep func(huntConfig) bool) {
	t.Helper()
	full := os.Getenv("OME_PD_PACING_HUNT") == "full"
	maxStates, maxDepth, runs := 30, 40, 2
	if full {
		maxStates, maxDepth, runs = 80, 80, 3
	}
	var configs []huntConfig
	for _, cfg := range huntConfigs(full) {
		if keep(cfg) {
			configs = append(configs, cfg)
		}
	}
	totals := map[string]int{}
	var states, gateCalls, allRuns, probes int
	type example struct {
		cfg     huntConfig
		finding huntFinding
	}
	examples := map[string][]example{}
	for n, cfg := range configs {
		w := newHuntWorld(t, cfg)
		rep := w.explore(maxStates, maxDepth)
		w.randomRuns(&rep, 1, runs, 4*maxDepth)
		states += rep.states
		gateCalls += w.gateCalls
		allRuns += rep.runs
		probes += rep.probes
		if full {
			// Each configuration's own line, so a run cut short still reports.
			var parts []string
			for class, c := range rep.counts {
				parts = append(parts, fmt.Sprintf("%s=%d", class, c))
			}
			sort.Strings(parts)
			t.Logf("config %d/%d [%s]: %d states, depth %d, %d probes: %s", n+1, len(configs), cfg.name, rep.states, rep.depth, rep.probes, strings.Join(parts, " "))
		}
		for class, n := range rep.counts {
			totals[class] += n
		}
		for class, f := range rep.findings {
			if len(examples[class]) < 12 {
				examples[class] = append(examples[class], example{cfg: cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d probes, %d random runs, %d gate questions", len(configs), states, probes, allRuns, gateCalls)
	for _, class := range classes {
		t.Logf("%s: %d states across %d configurations", class, totals[class], len(examples[class]))
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg.name, ex.finding.message, huntPath(ex.finding.state), ex.finding.state)
		}
		fails := false
		switch class {
		case "no-way-out", "budget", "classifier", "batch-promise", "ratio-promise", "runs-ahead-of-a-peer-coming-up", "read", "depth", "starts-before-the-named-side", "stranded-replacement", "waits-on-held-instances", "ratio-rule-waits-on-the-pairing-gate":
			fails = true
		}
		if fails {
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg.name, examples[class][0].finding.message)
		}
	}
}

// TestPDPacing_PlainRollsNeverWedge explores every configuration without a
// pairing-protocol change: under every setting, strategy pair, budget and
// shape, with failures, parked attempts, spent ladders and scales thrown
// in, the two sides never wait on each other, the budgets hold, and the
// started counts stay within one planned batch of each other.
func TestPDPacing_PlainRollsNeverWedge(t *testing.T) {
	huntExplore(t, func(c huntConfig) bool { return !c.protocolChange })
}

// TestPDPacing_ProtocolChangesNeverWedge explores the same space during a
// pairing-protocol change, where the pairing gates join the batch step.
func TestPDPacing_ProtocolChangesNeverWedge(t *testing.T) {
	huntExplore(t, func(c huntConfig) bool { return c.protocolChange })
}

// picture builds a settled state from the rows and ladders given per side;
// a side not named keeps every Instance on the old revision.
func (w *huntWorld) picture(rows map[v1beta1.ComponentType][]huntRow, ladders map[v1beta1.ComponentType]huntSchedule) *huntState {
	st := w.initial()
	for i, m := range w.cfg.members {
		if r, ok := rows[m.comp]; ok {
			st.sides[i].rows = append([]huntRow(nil), r...)
		}
		if l, ok := ladders[m.comp]; ok {
			st.sides[i].schedule = l
			if l != scheduleNone {
				st.sides[i].attempts = 1
			}
		}
	}
	w.settle(st, w.order(0))
	return st
}

// rows repeats a kind n times.
func rows(kind huntRow, n int) []huntRow {
	out := make([]huntRow, n)
	for i := range out {
		out[i] = kind
	}
	return out
}

// gangs gives both pairing members Instances of the given pod counts.
func gangs(cfg huntConfig, engine, decoder int32) huntConfig {
	cfg.members[0].gang, cfg.members[1].gang = engine, decoder
	cfg.name = cfg.String()
	return cfg
}

// outOfPairingOf gives a surging member its own maxUnavailable beside its surge
// budget, the pairable minimum the pairing gates hold it to.
func outOfPairingOf(cfg huntConfig, comp v1beta1.ComponentType, maxUnavailable string) huntConfig {
	for i := range cfg.members {
		if cfg.members[i].comp == comp {
			cfg.members[i].unavailable = maxUnavailable
		}
	}
	cfg.name = cfg.String()
	return cfg
}

func pair(shape [2]int32, es, ds v1beta1.UpdateStrategyType, budget string, defaults GroupDefaults, protocol bool) huntConfig {
	cfg := huntConfig{
		members: []huntMember{
			{comp: v1beta1.EngineComponent, desired: shape[0], strategy: es, budget: budget},
			{comp: v1beta1.DecoderComponent, desired: shape[1], strategy: ds, budget: budget},
		},
		defaults: defaults, protocolChange: protocol, maxAttempts: 3,
		fails: 2, parks: 1, scales: 1, scaleDeltas: []int32{1, -1, 2, -2},
	}
	cfg.name = cfg.String()
	return cfg
}

// TestPDPacing_AHandOverKeptForTheOtherCohortMustNotHoldTheOtherSidesStart
// is the picture the exploration found during a pairing-protocol change on
// a surging side: the side's first replacement serves, and the drain gate
// keeps its old pod because no pod of the new cohort serves on the other
// side yet, whether that side surges, rebuilds its Instances or patches
// them in place. The other side's start is what would bring that pod up,
// and the batch step holds it on the kept Instance. Each picture stands after
// one move: with the chart's decoder-first setting the engine's first
// batch waits for the decoder's first Instance "to serve first"; under
// keepRatio alone on 6 prefill beside 2 decode the decoder's first start
// belongs to its planned batch 2, because the plan gives it an empty first
// batch, and waits for the engine's batch 1; with no setting at all, when
// the decoder's first surge failed, its retry counts as a batch-2 start
// and waits for the engine's batch 1. A side of one Instance that surges
// has the same wedge one move earlier: it has no hand-over yet, its only
// start is held by the start-time gate until the other side serves the
// new cohort, and the other side waits on its batch. In every picture the
// pairing gate holds the one side and the batch step holds the other, and
// nothing in the world can move either.
func TestPDPacing_AHandOverKeptForTheOtherCohortMustNotHoldTheOtherSidesStart(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge, recreate := v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod
	five := ptr.To(int32(5))
	cases := []struct {
		name    string
		cfg     huntConfig
		rows    map[v1beta1.ComponentType][]huntRow
		ladders map[v1beta1.ComponentType]huntSchedule
		kept    v1beta1.ComponentType // the side the pairing gate holds: its hand-over kept, or its only start held
		waiting v1beta1.ComponentType // the side whose start must open
		// startHeld names a picture where the kept side has not started and
		// the start-time gate holds its only start, instead of a hand-over.
		startHeld bool
	}{
		{
			name: "2 prefill beside 1 decode, both surging, the chart's decoder-first setting: the decode's only start is held until a prefill serves the new cohort, so the engine's first batch must open",
			cfg:  pair([2]int32{2, 1}, surge, surge, "10%", huntRules(true, five, dec), true),
			kept: dec, waiting: eng, startHeld: true,
		},
		{
			name:    "2 prefill beside 1 decode, both surging, no setting: the engine's first surge failed and its retry counts as a batch-2 start",
			cfg:     pair([2]int32{2, 1}, surge, surge, "10%", GroupDefaults{}, true),
			rows:    map[v1beta1.ComponentType][]huntRow{eng: {huntFailedSurge, huntOld}},
			ladders: map[v1beta1.ComponentType]huntSchedule{eng: scheduleDue},
			kept:    dec, waiting: eng, startHeld: true,
		},
		{
			name: "12 prefill beside 8 decode, both surging at 10 percent, the chart's decoder-first setting: the engine's first batch",
			cfg:  pair([2]int32{12, 8}, surge, surge, "10%", huntRules(true, five, dec), true),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append([]huntRow{huntReplacementHeld}, rows(huntOld, 7)...)},
			kept: dec, waiting: eng,
		},
		{
			name: "12 prefill beside 8 decode, both surging, firstSide decoder with no ratio rule: the engine's first batch",
			cfg:  pair([2]int32{12, 8}, surge, surge, "10%", huntRules(false, nil, dec), true),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append([]huntRow{huntReplacementHeld}, rows(huntOld, 7)...)},
			kept: dec, waiting: eng,
		},
		{
			name: "6 prefill beside 2 decode, both surging, keepRatio at 5: the decoder's first start is a planned batch-2 start",
			cfg:  pair([2]int32{6, 2}, surge, surge, "10%", huntRules(true, five, ""), true),
			rows: map[v1beta1.ComponentType][]huntRow{eng: append([]huntRow{huntReplacementHeld}, rows(huntOld, 5)...)},
			kept: eng, waiting: dec,
		},
		{
			name: "2 prefill beside 6 decode, both surging, keepRatio at 5: the engine's first start is a planned batch-2 start",
			cfg:  pair([2]int32{2, 6}, surge, surge, "10%", huntRules(true, five, ""), true),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append([]huntRow{huntReplacementHeld}, rows(huntOld, 5)...)},
			kept: dec, waiting: eng,
		},
		{
			name: "8 prefill beside 4 decode at a surge budget of 2, keepRatio at 5, engine first: the decoder's first batch",
			cfg:  pair([2]int32{8, 4}, surge, surge, "2", huntRules(true, five, eng), true),
			rows: map[v1beta1.ComponentType][]huntRow{eng: append([]huntRow{huntReplacementHeld, huntReplacementHeld}, rows(huntOld, 6)...)},
			kept: eng, waiting: dec,
		},
		{
			name: "3 prefill beside 3 decode, both surging, no setting: the decoder's retry of its failed first surge",
			cfg:  pair([2]int32{3, 3}, surge, surge, "10%", GroupDefaults{}, true),
			rows: map[v1beta1.ComponentType][]huntRow{
				eng: {huntReplacementHeld, huntOld, huntOld},
				dec: {huntFailedSurge, huntOld, huntOld},
			},
			ladders: map[v1beta1.ComponentType]huntSchedule{dec: scheduleDue},
			kept:    eng, waiting: dec,
		},
		{
			name: "3 prefill beside 3 decode, gangs of 2 both surging through a slot of their own, no setting: the decoder's retry of its failed first surge",
			cfg:  gangs(pair([2]int32{3, 3}, surge, surge, "10%", GroupDefaults{}, true), 2, 2),
			rows: map[v1beta1.ComponentType][]huntRow{
				eng: {huntReplacementHeld, huntOld, huntOld},
				dec: {huntFailedSurge, huntOld, huntOld},
			},
			ladders: map[v1beta1.ComponentType]huntSchedule{dec: scheduleDue},
			kept:    eng, waiting: dec,
		},
		{
			name: "12 prefill beside 8 decode, gangs of 2 both surging at 10 percent, the chart's decoder-first setting: the engine's first batch",
			cfg:  gangs(pair([2]int32{12, 8}, surge, surge, "10%", huntRules(true, five, dec), true), 2, 2),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append([]huntRow{huntReplacementHeld}, rows(huntOld, 7)...)},
			kept: dec, waiting: eng,
		},
		{
			name: "6 prefill beside 2 decode, gangs of 4 both surging, keepRatio at 5: the decoder's first start is a planned batch-2 start",
			cfg:  gangs(pair([2]int32{6, 2}, surge, surge, "10%", huntRules(true, five, ""), true), 4, 4),
			rows: map[v1beta1.ComponentType][]huntRow{eng: append([]huntRow{huntReplacementHeld}, rows(huntOld, 5)...)},
			kept: eng, waiting: dec,
		},
		{
			name: "6 prefill rebuilt whole as gangs of 2 beside 2 decode gangs of 2 surging, no setting: the last old decode gang is kept for the old prefill and the prefill's third rebuild must open",
			cfg:  gangs(pair([2]int32{6, 2}, v1beta1.UpdateStrategyRecreatePod, surge, "10%", GroupDefaults{}, true), 2, 2),
			rows: map[v1beta1.ComponentType][]huntRow{
				eng: append(rows(huntPromoted, 2), rows(huntOld, 4)...),
				dec: {huntPromoted, huntReplacementHeld},
			},
			kept: dec, waiting: eng,
		},
		{
			name: "2 prefill beside 1 decode, gangs of 2 both surging, the chart's decoder-first setting: the decode's only start is held until a prefill serves the new cohort",
			cfg:  gangs(pair([2]int32{2, 1}, surge, surge, "10%", huntRules(true, five, dec), true), 2, 2),
			kept: dec, waiting: eng, startHeld: true,
		},
		{
			name: "2 prefill rebuilt one at a time beside 2 decode surging with none allowed out of pairing, the chart's decoder-first setting: the first new decode serves before any prefill of the new cohort, its old pod is kept and the engine's first rebuild must open",
			cfg:  outOfPairingOf(pair([2]int32{2, 2}, recreate, surge, "1", huntRules(true, five, dec), true), dec, "0"),
			rows: map[v1beta1.ComponentType][]huntRow{dec: {huntReplacementHeld, huntOld}},
			kept: dec, waiting: eng,
		},
		{
			name: "2 prefill rebuilt one at a time beside 6 decode surging three at a time with one allowed out, the chart's decoder-first setting: three new decode serve before any prefill of the new cohort",
			cfg:  outOfPairingOf(pair([2]int32{2, 6}, recreate, surge, "50%", huntRules(true, five, dec), true), dec, "1"),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append(rows(huntReplacementHeld, 3), rows(huntOld, 3)...)},
			kept: dec, waiting: eng,
		},
		{
			name: "12 prefill rebuilt three at a time beside 8 decode surging two at a time with none allowed out, the chart's decoder-first setting: two new decode serve before any prefill of the new cohort",
			cfg:  outOfPairingOf(pair([2]int32{12, 8}, recreate, surge, "25%", huntRules(true, five, dec), true), dec, "0"),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append(rows(huntReplacementHeld, 2), rows(huntOld, 6)...)},
			kept: dec, waiting: eng,
		},
		{
			name: "12 prefill rebuilt beside 8 decode surging at a tenth, firstSide decoder with no ratio rule: the first new decode serves before any prefill of the new cohort",
			cfg:  pair([2]int32{12, 8}, recreate, surge, "10%", huntRules(false, nil, dec), true),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append([]huntRow{huntReplacementHeld}, rows(huntOld, 7)...)},
			kept: dec, waiting: eng,
		},
		{
			name: "12 prefill patched in place beside 8 decode surging at a tenth, the chart's decoder-first setting: the first new decode serves before any prefill of the new cohort",
			cfg:  pair([2]int32{12, 8}, v1beta1.UpdateStrategyInPlaceIfPossible, surge, "10%", huntRules(true, five, dec), true),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append([]huntRow{huntReplacementHeld}, rows(huntOld, 7)...)},
			kept: dec, waiting: eng,
		},
		{
			name: "12 prefill gangs of 2 rebuilt beside 8 decode gangs of 2 surging at a tenth, the chart's decoder-first setting: the first new decode gang serves before any prefill of the new cohort",
			cfg:  gangs(pair([2]int32{12, 8}, recreate, surge, "10%", huntRules(true, five, dec), true), 2, 2),
			rows: map[v1beta1.ComponentType][]huntRow{dec: append([]huntRow{huntReplacementHeld}, rows(huntOld, 7)...)},
			kept: dec, waiting: eng,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newHuntWorld(t, tc.cfg)
			st := w.picture(tc.rows, tc.ladders)
			ki, wi := 0, 0
			for i, m := range w.cfg.members {
				if m.comp == tc.kept {
					ki = i
				}
				if m.comp == tc.waiting {
					wi = i
				}
			}
			if tc.startHeld {
				if st.sides[ki].inFlight() || st.holds[ki] == nil || st.holds[ki].gate != v1beta1.RolloutHoldGatePairing {
					t.Fatalf("the picture is wrong: the pairing gate must hold the %s's only start: %s hold=%s", tc.kept, st.sides[ki], st.holds[ki])
				}
			} else {
				handOver := -1
				for idx, r := range st.sides[ki].rows {
					if r == huntReplacementHeld {
						handOver = idx
						break
					}
				}
				if handOver < 0 || w.drainGateAdmits(st, ki, handOver) {
					t.Fatalf("the picture is wrong: the drain gate must keep the %s's old pods: %s", tc.kept, st.sides[ki])
				}
			}
			if st.sides[wi].inFlight() {
				return
			}
			t.Errorf("the %s's start must open while the pairing gate holds the %s for want of a %s on the new cohort; instead the two wait on each other:\n  %s holds on %s\n  %s holds on %s",
				tc.waiting, tc.kept, tc.waiting, tc.waiting, st.holds[wi], tc.kept, st.holds[ki])
		})
	}
}

// TestPDPacing_ASpentScheduleLeavesThePeerWaitingAtItsBatch holds what the
// batch step does today when one side's retry ladder is spent with
// Instances left on the old revision. With 12 prefill beside 8 decode at
// 10 percent, three prefill promoted and a fourth failed when the ladder
// is spent, the failed Instance counts as settled, so the decoder still
// takes the batch that waits on the prefill's batch 2 (four Instances)
// and stops at the next one, under a Ratio hold that names the Held
// side's batch and its failed count. Where a spent ladder leaves the other
// side (whether it should finish its own roll) is left open; this
// test records the current behaviour.
func TestPDPacing_ASpentScheduleLeavesThePeerWaitingAtItsBatch(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	for _, tc := range []struct {
		name     string
		defaults GroupDefaults
		want     string
	}{
		{name: "no setting", want: "decoder batch 4 waits for engine batch 3 to serve (3 of 6 serving, 1 failed)"},
		{name: "the chart's setting", defaults: huntRules(true, ptr.To(int32(5)), dec), want: "decoder batch 4 waits for engine batch 3 to serve (3 of 5 serving, 1 failed)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newHuntWorld(t, pair([2]int32{12, 8}, recreate, recreate, "10%", tc.defaults, false))
			st := w.picture(map[v1beta1.ComponentType][]huntRow{
				eng: append(append(rows(huntPromoted, 3), huntFailedUnserving), rows(huntOld, 8)...),
				dec: append(rows(huntPromoted, 3), rows(huntOld, 5)...),
			}, map[v1beta1.ComponentType]huntSchedule{eng: scheduleHeld})
			if st.sides[1].inFlight() {
				t.Fatalf("the decoder opened a start beyond the batch the Held engine never settles: %s", st)
			}
			if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateRatio || h.reason != tc.want {
				t.Fatalf("the decoder's hold = %s, want Ratio %q", h, tc.want)
			}
			t.Logf("with the engine Held at %s, the decoder stops: %s", st.sides[0], st.holds[1])
		})
	}
}

// TestPDPacing_AHeldNamedSideHoldsTheRollAsAWhole is the push onto an
// image no pod can pull on 3 prefill beside 3 decode, both SurgeThenDrain
// at a maxSurge of 1, with the decoder named to move first (the chart's
// setting). The decoder's first surge failed until its retry schedule
// was spent; the engine's first attempt failed once and its retry is due.
// That retry belongs to the engine's batch 2 and would wait for the
// decoder's batch 2 to serve first, which a Held decoder never opens: the
// hold reads Held, names the decoder and its record, and the explorer
// reads the state as held, the roll held as a whole, not as a side waiting
// on a Held peer. With the engine named instead the mirror holds. A Held
// side that is not the named one leaves the other side waiting at its
// batch under the Ratio gate, as
// TestPDPacing_ASpentScheduleLeavesThePeerWaitingAtItsBatch holds.
func TestPDPacing_AHeldNamedSideHoldsTheRollAsAWhole(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	for _, tc := range []struct {
		name         string
		named, other v1beta1.ComponentType
	}{
		{name: "the decoder named and held", named: dec, other: eng},
		{name: "the engine named and held", named: eng, other: dec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newHuntWorld(t, pair([2]int32{3, 3}, surge, surge, "1", huntRules(true, ptr.To(int32(5)), tc.named), false))
			st := w.picture(map[v1beta1.ComponentType][]huntRow{
				tc.named: {huntFailedSurge, huntOld, huntOld},
				tc.other: {huntFailedSurge, huntOld, huntOld},
			}, map[v1beta1.ComponentType]huntSchedule{tc.named: scheduleHeld, tc.other: scheduleDue})
			engine, decoder := w.pairIndex()
			other := engine
			if tc.other == dec {
				other = decoder
			}
			if st.sides[other].inFlight() {
				t.Fatalf("the %s opened a start past the Held %s: %s", tc.other, tc.named, st)
			}
			h := st.holds[other]
			if h == nil || h.gate != v1beta1.RolloutHoldGateHeld ||
				!strings.Contains(h.reason, "the "+string(tc.named)+" is held: update to ") ||
				!strings.Contains(h.reason, "held after 1 failed attempt(s)") ||
				!strings.Contains(h.reason, "resumes both sides") {
				t.Fatalf("the %s's hold = %s, want the Held gate naming the %s and its record", tc.other, h, tc.named)
			}
			if class, message := w.classifyStandstill(st); class != "held" {
				t.Fatalf("the state reads %s (%s), want held: the roll is held as a whole", class, message)
			}
			t.Logf("with the %s Held, the %s reads: %s", tc.named, tc.other, h)
		})
	}
}

// TestPDPacing_AnInPlaceMemberWithNoUnavailabilityBudgetHoldsOnlyItself
// reads the in-place single-pod member under keepRatio with maxUnavailable
// 0: its step is zero, so the batch step leaves its own starts to the
// budget gate, which denies them, and the decoder beside it rolls to the
// end on its own steps without waiting on an engine batch that never
// opens.
func TestPDPacing_AnInPlaceMemberWithNoUnavailabilityBudgetHoldsOnlyItself(t *testing.T) {
	cfg := pair([2]int32{12, 8}, v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategyRecreatePod, "10%", huntRules(true, ptr.To(int32(5)), ""), false)
	cfg.members[0].budget = "0"
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	for round := 0; round < 64 && !st.sides[1].complete(); round++ {
		moved := false
		for idx, r := range st.sides[1].rows {
			if r.open() {
				st = w.apply(st, huntMove{kind: "serve", side: 1, idx: idx})
				moved = true
				break
			}
		}
		if !moved {
			t.Fatalf("the decoder stopped at %s with hold %s while the engine (%s) holds on %s", st.sides[1], st.holds[1], st.sides[0], st.holds[0])
		}
	}
	if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateBudget {
		t.Fatalf("the engine with no unavailability budget must hold on its own budget, got %s", h)
	}
	if st.sides[0].inFlight() || st.sides[0].count(huntPromoted) > 0 {
		t.Fatalf("the engine moved with no budget: %s", st.sides[0])
	}
}

// TestPDPacing_ARouterLeftOutOfTheBatchesIsPacedByTheBand holds where a
// drain-first router stands beside a surging prefill/decode pair under
// the group's maintainRatio: it steps in a kind no pairing member has, so
// the batch step leaves it out, and the ratio rule's serving band paces
// it instead. With two routers at a tolerance of 5 no single router drain
// fits the tolerance (one of two serving is 100 points off the anchor), so
// the band admits one router drain at a time from a fleet serving its
// reference, and a second while one is out is refused under the Ratio
// gate: by the lockstep while the pair has landed less than the router's
// half (the drain under way counts as the router's progress, "drain would
// outpace peer new-revision rollout past tolerance"), by the band once the
// pair has caught up ("drain would skew cross-Component serving ratio past
// tolerance"); the pair finishes and the router finishes behind it, one
// Instance at a time. The router's own budget is wide enough for both, so
// the ratio rule is what paces it. Recorded here so a change in how the
// rule paces the router is noticed.
func TestPDPacing_ARouterLeftOutOfTheBatchesIsPacedByTheBand(t *testing.T) {
	surge, recreate := v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod
	cfg := pair([2]int32{12, 4}, surge, surge, "10%", huntRules(true, ptr.To(int32(20)), ""), false)
	cfg.members = append(cfg.members, huntMember{comp: v1beta1.RouterComponent, desired: 2, strategy: recreate, budget: "100%"})
	cfg.tolerance = ptr.To(int32(5))
	cfg.fails, cfg.parks, cfg.scales = 0, 0, 0
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	routerSteps := 0
	for round := 0; round < 96 && !(st.sides[0].complete() && st.sides[1].complete() && st.sides[2].complete()); round++ {
		open := 0
		for _, r := range st.sides[2].rows {
			if r.open() {
				open++
			}
		}
		if open > 1 {
			t.Fatalf("the router has %d Instances out at once; the band admits one at a time: %s", open, st)
		}
		if open == 1 {
			routerSteps++
			if h := st.holds[2]; !st.sides[2].complete() && st.sides[2].count(huntOld) > 0 && (h == nil || h.gate != v1beta1.RolloutHoldGateRatio || !(strings.Contains(h.reason, "skew") || strings.Contains(h.reason, "outpace"))) {
				t.Fatalf("with one router Instance out, the ratio rule must hold the other: hold = %s, state %s", h, st)
			}
		}
		moved := false
		for i := 0; i < 3 && !moved; i++ {
			for idx, r := range st.sides[i].rows {
				if r.open() {
					st = w.apply(st, huntMove{kind: "serve", side: i, idx: idx})
					moved = true
					break
				}
			}
		}
		if !moved {
			t.Fatalf("the roll stopped: %s", st)
		}
	}
	if !st.sides[2].complete() {
		t.Fatalf("the router must finish behind the pair, one Instance at a time: %s (hold %s)", st.sides[2], st.holds[2])
	}
	if routerSteps == 0 {
		t.Fatalf("the router never had a drain open; the picture no longer drives the band's one-step rule")
	}
	t.Logf("the pair and the router (%s) are on version 2; the router rolled one Instance at a time", st.sides[2])
}

// TestPDPacing_APartitionedSideIsDoneOnceItsMovableInstancesServe plays a
// roll with a partition on one side to its end: 8 prefill beside 8 decode,
// both recreated one at a time, the operator keeping 2 prefill on the old
// version. The prefill side ends at 6 new and 2 old, the decoder rolls all
// 8, and no hold waits on the two held Instances; lowering the partition
// afterwards moves them as the next batches and the roll completes. The
// batch step counts a partition's held Instances apart from the batches; a
// step that counted them among the batches would stop the decoder at 7 of
// 8 on "decoder batch 8 waits for engine batch 7 to serve (6 of 7
// serving)", the exploration's waits-on-held-instances class.
func TestPDPacing_APartitionedSideIsDoneOnceItsMovableInstancesServe(t *testing.T) {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	for _, tc := range []struct {
		name string
		cfg  huntConfig
		held int32
	}{
		{name: "8 prefill beside 8 decode, both recreated one at a time, 2 prefill held", cfg: pair([2]int32{8, 8}, recreate, recreate, "1", GroupDefaults{}, false), held: 2},
		{name: "12 prefill beside 8 decode, both surging at 10 percent, 4 prefill held", cfg: pair([2]int32{12, 8}, surge, surge, "10%", GroupDefaults{}, false), held: 4},
		{name: "12 prefill beside 8 decode, both recreated at 10 percent, the chart's setting, 2 prefill held", cfg: pair([2]int32{12, 8}, recreate, recreate, "10%", huntRules(true, ptr.To(int32(5)), v1beta1.DecoderComponent), false), held: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.fails, tc.cfg.parks, tc.cfg.scales = 0, 0, 0
			w := newHuntWorld(t, tc.cfg)
			st := w.apply(w.initial(), huntMove{kind: "partition", side: 0, delta: tc.held})
			st = w.playOut(st)
			engine, decoder := st.sides[0], st.sides[1]
			if !decoder.complete() {
				t.Fatalf("the decoder stopped at %d of %d with hold %s while the engine (%s) holds on %s", decoder.count(huntPromoted), len(decoder.rows), st.holds[1], engine, st.holds[0])
			}
			if engine.count(huntPromoted) != int32(len(engine.rows))-tc.held || engine.count(huntOld) != tc.held {
				t.Fatalf("the engine must end with %d held Instances on the old version: %s", tc.held, engine)
			}
			if h := st.holds[0]; h != nil {
				t.Fatalf("the engine is done under its partition and must hold on nothing, got %s", h)
			}
			cleared := w.playOut(w.apply(st, huntMove{kind: "partition", side: 0, delta: 0}))
			if !cleared.complete() {
				t.Fatalf("with the partition cleared the held Instances must roll as the next batches; stopped at %s", cleared)
			}
		})
	}
}

// TestPDPacing_AScaleMidRollReplansTheBatchesFromTheNewCounts scales one
// side while the roll is rotating and reads the batch step's plan from the
// counts the spec asks for after the scale: 12 prefill beside 8 decode,
// both surging at 10 percent, the decoder scaled from 8 to 16 after the
// first batch serves reads a step of 2 over 16; scaled from 8 to 4 it
// reads a step of 1 over 4; both sides scaled to a new ratio at once (18
// prefill beside 4 decode) read 2 over 18 and 1 over 4. Every roll then
// plays to its end with no side waiting on the other.
func TestPDPacing_AScaleMidRollReplansTheBatchesFromTheNewCounts(t *testing.T) {
	surge := v1beta1.UpdateStrategySurgeThenDrain
	for _, tc := range []struct {
		name         string
		move         huntMove
		wantEngine   [2]int32 // desired, step
		wantDecoder  [2]int32
		wantDecoders int32
		wantPrefills int32
	}{
		{name: "the decoder doubled", move: huntMove{kind: "scale", side: 1, delta: 8}, wantEngine: [2]int32{12, 2}, wantDecoder: [2]int32{16, 2}, wantPrefills: 12, wantDecoders: 16},
		{name: "the decoder halved", move: huntMove{kind: "scale", side: 1, delta: -4}, wantEngine: [2]int32{12, 2}, wantDecoder: [2]int32{4, 1}, wantPrefills: 12, wantDecoders: 4},
		{name: "both sides to a new ratio", move: huntMove{kind: "scale", side: 0, delta: 6, both: true, side2: 1, delta2: -4}, wantEngine: [2]int32{18, 2}, wantDecoder: [2]int32{4, 1}, wantPrefills: 18, wantDecoders: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := pair([2]int32{12, 8}, surge, surge, "10%", GroupDefaults{}, false)
			cfg.fails, cfg.parks, cfg.scales = 0, 0, 1
			w := newHuntWorld(t, cfg)
			st := w.initial()
			w.settle(st, w.order(0))
			// The first batch serves: 2 prefill and 1 decode promoted.
			for step := 0; st.sides[0].count(huntPromoted) < 2 || st.sides[1].count(huntPromoted) < 1; step++ {
				mv, ok := w.nextProgress(st, step)
				if !ok {
					t.Fatalf("the first batch did not serve: %s", st)
				}
				st = w.apply(st, mv)
			}
			st = w.apply(st, tc.move)
			gate := ResolveGateContextWithDefaults(w.ctx, w.client(st), w.isvc, v1beta1.EngineComponent, w.cfg.defaults)
			gate.TargetRevision = w.targets[0]
			batch, err := gate.readBatchStep()
			if err != nil || !batch.governs {
				t.Fatalf("the batch step must govern after the scale: governs=%v err=%v", batch.governs, err)
			}
			for _, bm := range batch.members {
				want := tc.wantEngine
				if bm.component == v1beta1.DecoderComponent {
					want = tc.wantDecoder
				}
				if bm.desired != want[0] || bm.step != want[1] {
					t.Fatalf("after the scale the %s reads desired %d step %d, want %d and %d from the spec's new count", bm.component, bm.desired, bm.step, want[0], want[1])
				}
			}
			st = w.playOut(st)
			if !st.complete() {
				t.Fatalf("the roll stopped after the scale: %s", st)
			}
			if got := int32(len(st.sides[0].rows)); got != tc.wantPrefills {
				t.Fatalf("the engine ends with %d Instances, want %d", got, tc.wantPrefills)
			}
			if got := int32(len(st.sides[1].rows)); got != tc.wantDecoders {
				t.Fatalf("the decoder ends with %d Instances, want %d", got, tc.wantDecoders)
			}
		})
	}
}

// TestPDPacing_ASideWhosePromotedPodIsNotYetCountedIsWaitedOn is the
// picture the exploration found on 8 prefill rebuilt one at a time beside 4
// decode surging one at a time, the decoder first, during a protocol
// change: a rebuilt prefill is promoted in one status write and its pod
// counted as serving in the next, so for that publication the prefill
// side reads one serving Instance short with no attempt open, and the
// pairing gate holds every prefill start on the prefill's own pairable
// minimum. That hold is the prefill's own, released by its pod's count
// and by nothing the decoder does, so the decoder's next surge, which
// belongs to the batch after the prefill's next rebuild, waits for that
// rebuild to serve. The mirror holds the engine's surge the same way.
func TestPDPacing_ASideWhosePromotedPodIsNotYetCountedIsWaitedOn(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	chart := huntRules(true, ptr.To(int32(5)), dec)
	cases := []struct {
		name    string
		cfg     huntConfig
		rows    map[v1beta1.ComponentType][]huntRow
		waiting v1beta1.ComponentType
		hold    string
	}{
		{
			name: "the prefill's third rebuild promoted, its pod not yet counted: the decoder's third surge waits for the fourth rebuild",
			cfg:  pair([2]int32{8, 4}, recreate, surge, "10%", chart, true),
			rows: map[v1beta1.ComponentType][]huntRow{
				eng: append([]huntRow{huntPromoted, huntPromoted, huntPromotedUncounted}, rows(huntOld, 5)...),
				dec: {huntPromoted, huntPromoted, huntOld, huntOld},
			},
			waiting: dec, hold: "decoder batch 5 waits for engine batch 4 to serve (3 of 4 serving)",
		},
		{
			name: "the prefill's fifth rebuild promoted, its pod not yet counted: the decoder's fourth surge waits for the sixth rebuild",
			cfg:  pair([2]int32{8, 4}, recreate, surge, "10%", chart, true),
			rows: map[v1beta1.ComponentType][]huntRow{
				eng: append(append(rows(huntPromoted, 4), huntPromotedUncounted), rows(huntOld, 3)...),
				dec: {huntPromoted, huntPromoted, huntPromoted, huntOld},
			},
			waiting: dec, hold: "decoder batch 7 waits for engine batch 6 to serve (5 of 6 serving)",
		},
		{
			name: "the decoder's first rebuild promoted, its pod not yet counted: the engine's third surge waits for the second rebuild to serve first",
			cfg:  pair([2]int32{8, 4}, surge, recreate, "10%", chart, true),
			rows: map[v1beta1.ComponentType][]huntRow{
				eng: append([]huntRow{huntPromoted, huntPromoted}, rows(huntOld, 6)...),
				dec: {huntPromotedUncounted, huntOld, huntOld, huntOld},
			},
			waiting: eng, hold: "engine batch 3 waits for decoder batch 3 to serve first (1 of 2 serving)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newHuntWorld(t, tc.cfg)
			st := w.picture(tc.rows, nil)
			side := -1
			for i, m := range w.cfg.members {
				if m.comp == tc.waiting {
					side = i
				}
			}
			if st.sides[side].inFlight() {
				t.Fatalf("the %s opened a start while the other side's promoted Instance is not yet counted as serving: %s", tc.waiting, st)
			}
			h := st.holds[side]
			if h == nil || h.gate != v1beta1.RolloutHoldGateRatio || !strings.HasPrefix(h.reason, tc.hold) {
				t.Fatalf("the %s must wait under the Ratio gate with %q, got %s", tc.waiting, tc.hold, h)
			}
			if findings := w.check(st); len(findings) > 0 {
				t.Fatalf("the picture breaks an invariant: %s: %s", findings[0].class, findings[0].message)
			}
		})
	}
}

// TestPDPacing_ARuleEditMidRollPlansTheNextBatchFromTheSpec edits the
// rollout block while the run is open and reads the next start from the
// spec as edited, the run keeping the plan it recorded: 12 prefill beside 8
// decode, both RecreatePod at 10 percent, the group's tolerance at 5 and the
// ratio rule on. After the first batch serves (2 prefill and 1 decode) the
// prefill side has one more start open and its next one waits as batch 3
// for the decoder's batch 2. The tolerance raised to 20 plans 2, 2, 2, 2,
// 1, 2, 1, so that start belongs to batch 2 and is admitted; the rule
// removed leaves the prefill side on its own step of 2 with the same
// result; the group's budget lowered to 1 reads steps of 1 and 1 and holds
// the next start until the open attempt serves; the group removed leaves
// each side on its own budget alone, held by nothing but it; the router
// added to the group or the decoder removed from it changes nothing, since
// the gates keep the members the run recorded. Every roll then plays to
// its end.
func TestPDPacing_ARuleEditMidRollPlansTheNextBatchFromTheSpec(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	budgetMarks, _ := pairedBatchPlan(12, 8, 1, 1, 5)
	recordedMarks, _ := pairedBatchPlan(12, 8, 2, 1, 5)
	for _, tc := range []ruleEditCase{
		{name: "the tolerance raised to 20", edit: huntEdit{kind: huntEditTolerance, tolerance: 20}, wantMarks: []int32{2, 4, 6, 8, 9, 11, 12, 12}, wantSteps: [2]int32{2, 1}, wantOpen: 2, governs: true},
		{name: "the rule removed", edit: huntEdit{kind: huntEditRuleRemoved}, wantSteps: [2]int32{2, 1}, wantOpen: 2, governs: true},
		{name: "the group budget lowered to 1", edit: huntEdit{kind: huntEditGroupBudget}, wantMarks: budgetMarks, wantSteps: [2]int32{1, 1}, wantOpen: 1, governs: true},
		{name: "the group removed", edit: huntEdit{kind: huntEditGroupRemoved}, wantOpen: 2},
		{name: "the router added to the group", edit: huntEdit{kind: huntEditMemberAdded}, wantMarks: recordedMarks, wantSteps: [2]int32{2, 1}, wantOpen: 1, governs: true},
		{name: "the decoder removed from the group", edit: huntEdit{kind: huntEditMemberRemoved}, wantMarks: recordedMarks, wantSteps: [2]int32{2, 1}, wantOpen: 1, governs: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := pair([2]int32{12, 8}, recreate, recreate, "10%", huntRules(true, nil, ""), false)
			cfg.tolerance = ptr.To(int32(5))
			cfg.fails, cfg.scales, cfg.edits = 0, 0, 1
			w := newHuntWorld(t, cfg)
			st := w.initial()
			w.settle(st, w.order(0))
			st = serveTheFirstBatch(t, w, st)

			st = w.apply(st, huntMove{kind: "edit", edit: tc.edit})
			if open := st.sides[0].count(huntOpenDrain); open != tc.wantOpen {
				t.Fatalf("after %s the prefill side must have %d start(s) open, not %d: %s (hold %s)", tc.edit, tc.wantOpen, open, st, st.holds[0])
			}
			gate := ResolveGateContextWithDefaults(w.ctx, w.client(st), w.service(st), v1beta1.EngineComponent, w.cfg.defaults)
			gate.TargetRevision = w.targets[0]
			batch, err := gate.readBatchStep()
			if err != nil || batch.governs != tc.governs {
				t.Fatalf("after %s the batch step governs=%v (want %v) err=%v", tc.edit, batch.governs, tc.governs, err)
			}
			assertBatchStepAfterEdit(t, tc, st, batch)
			st = w.playOut(st)
			if !st.complete() {
				t.Fatalf("the roll stopped after %s: %s", tc.edit, st)
			}
		})
	}
}

// ruleEditCase is one edit of the rollout block made after the first batch
// serves, with what the batch step and the sides read afterwards.
type ruleEditCase struct {
	name      string
	edit      huntEdit
	wantMarks []int32 // the prefill marks after the edit; nil for the side's own step
	wantSteps [2]int32
	wantOpen  int32 // the prefill attempts open once the edit has settled
	governs   bool
}

// serveTheFirstBatch plays the roll until 2 prefill and 1 decode serve and
// holds the picture the edits start from: one prefill start open and the
// next waiting as batch 3 for the decoder's batch 2.
func serveTheFirstBatch(t *testing.T, w *huntWorld, st *huntState) *huntState {
	t.Helper()
	for step := 0; st.sides[0].count(huntPromoted) < 2 || st.sides[1].count(huntPromoted) < 1; step++ {
		mv, ok := w.nextProgress(st, step)
		if !ok {
			t.Fatalf("the first batch did not serve: %s", st)
		}
		st = w.apply(st, mv)
	}
	if open := st.sides[0].count(huntOpenDrain); open != 1 {
		t.Fatalf("under the recorded tolerance of 5 the prefill side has one start open after the first batch, not %d: %s", open, st)
	}
	if h := st.holds[0]; h == nil || !strings.Contains(h.reason, "engine batch 3 waits for decoder batch 2") {
		t.Fatalf("the prefill side's next start waits as batch 3 for the decoder's batch 2; got %s", h)
	}
	return st
}

// assertBatchStepAfterEdit holds the batch step to the plan the edit leaves:
// the prefill marks and the steps while it governs, nothing but each side's
// own budget once it does not.
func assertBatchStepAfterEdit(t *testing.T, tc ruleEditCase, st *huntState, batch batchStep) {
	t.Helper()
	if !tc.governs {
		for i, h := range st.holds {
			if h != nil && h.gate != v1beta1.RolloutHoldGateBudget {
				t.Fatalf("with the group removed side %d is held by nothing but its own budget; got %s", i, h)
			}
		}
		return
	}
	if got := batch.members[batch.acting].marks; fmt.Sprint(got) != fmt.Sprint(tc.wantMarks) {
		t.Fatalf("after %s the prefill marks read %v, want %v", tc.edit, got, tc.wantMarks)
	}
	if steps := [2]int32{batch.members[0].step, batch.members[1].step}; steps != tc.wantSteps {
		t.Fatalf("after %s the steps read %v, want %v", tc.edit, steps, tc.wantSteps)
	}
}

// TestPDPacing_ANamedSideHeldWholeBehindThePushHoldsTheOtherSideForOnePassOnly
// drives the picture the exploration reported as waits-on-held-instances.
// Before the side named to move first has written its status for the
// push, the operator holds every Instance of that side with a partition.
// The other side's first pass reads the named side's stale status, which
// records nothing started and nothing held, and waits "for decoder batch 1
// to serve first (0 of 1 serving)". The named side's own pass then writes
// its status with every Instance held, a transition the replica
// controller's peer watch turns into the waiting side's next pass, and
// that pass reads the named side with no movable Instance and opens its
// first batch. The roll ends with the other side on the new version and
// the named side whole on the old one; the partition cleared rolls the
// named side as the next batches.
func TestPDPacing_ANamedSideHeldWholeBehindThePushHoldsTheOtherSideForOnePassOnly(t *testing.T) {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	for _, tc := range []struct {
		name  string
		cfg   huntConfig
		named int // the index of the side named to move first
	}{
		{name: "6 prefill gangs rebuilt beside 2 decode gangs surging, both at 50 percent, the decoder first and held whole", cfg: gangs(pair([2]int32{6, 2}, recreate, surge, "50%", huntRules(false, nil, v1beta1.DecoderComponent), false), 2, 2), named: 1},
		{name: "12 prefill beside 8 decode, both recreated at 10 percent, the chart's settings, the decoder held whole", cfg: pair([2]int32{12, 8}, recreate, recreate, "10%", huntRules(true, ptr.To(int32(5)), v1beta1.DecoderComponent), false), named: 1},
		{name: "3 prefill rebuilt beside 3 decode surging, one at a time, the engine first and held whole", cfg: pair([2]int32{3, 3}, recreate, surge, "1", huntRules(false, nil, v1beta1.EngineComponent), false), named: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.fails, tc.cfg.parks, tc.cfg.scales = 0, 0, 0
			tc.cfg.behindPush = 1 << tc.named
			w := newHuntWorld(t, tc.cfg)
			other, whole := 1-tc.named, tc.cfg.members[tc.named].desired
			named, waiting := tc.cfg.members[tc.named].comp, tc.cfg.members[other].comp
			st := w.apply(w.initial(), huntMove{kind: "partition", side: tc.named, delta: whole})
			if h := st.holds[other]; h != nil {
				if p, ok := w.batchWaitPeer(h.reason); ok && p == tc.named {
					t.Fatalf("once the %s has written its status with every Instance held, the %s must wait on nothing of it; it holds on %s (%s)", named, waiting, h, st)
				}
			}
			if st.sides[other].count(huntOld) == tc.cfg.members[other].desired {
				t.Fatalf("the %s opened no start: %s", waiting, st)
			}
			st = w.playOut(st)
			if !st.sides[other].complete() || st.sides[tc.named].count(huntOld) != whole {
				t.Fatalf("the roll must end with the %s on the new version and the %s whole on the old one: %s", waiting, named, st)
			}
			cleared := w.playOut(w.apply(st, huntMove{kind: "partition", side: tc.named, delta: 0}))
			if !cleared.complete() {
				t.Fatalf("with the partition cleared the %s must roll as the next batches; stopped at %s", named, cleared)
			}
		})
	}
}
