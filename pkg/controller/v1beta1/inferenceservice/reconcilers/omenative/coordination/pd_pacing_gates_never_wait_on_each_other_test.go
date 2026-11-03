package coordination

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils"
)

// This file drives the production gate stack (EvaluateUpdateGate and
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
// replacement is building or standing beside it, a side's partition is
// raised, lowered or cleared (on one side or both), or a side is scaled
// (one side, both sides by the same step, or both to a new ratio), with
// every plan read again from the counts the spec asks for. A roll may
// also start with one side's status, or both, predating the push: the
// replica still records the previous roll target with every Instance
// promoted on it, until that side's own pass writes its status. Every path is
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
	huntOld               huntRow = iota // Ready on the old revision, serving
	huntOpenDrain                        // a recreate toward the target, nothing serving
	huntOpenInPlace                      // an in-place patch toward the target, out of rotation
	huntOpenSurge                        // a surge replacement starting beside the serving source
	huntReplacementHeld                  // the replacement serves beside the source; the drain gate keeps the source
	huntPromoted                         // Ready on the target, serving
	huntFailedDark                       // a drain-first attempt ended Failed; nothing serves; no operation
	huntFailedSurge                      // a surge ended Failed; published Ready over the serving source, target kept
	huntRetriesStopped                   // the retries stopped; the old pod serves
	huntCreating                         // created on the target by a scale-up, not serving yet
	huntFailedGangKept                   // a gang surge ended Failed; the source keeps its attempt at Surge while the replacement gang is retired
	huntSurgeShort                       // a gang surge whose source lost a worker while the replacement builds; the leader still serves
	huntReplacementShort                 // a kept old gang whose replacement lost a worker; the source still serves
	huntPromotedUncounted                // promoted onto the target in one status write, its rebuilt pods not yet counted as serving until the next
)

var huntRowNames = [...]string{"old", "drain", "inplace", "surge", "handover", "new", "faileddark", "failedsurge", "parked", "creating", "failedgangkept", "surgeshort", "replacementshort", "newuncounted"}

func (r huntRow) String() string { return huntRowNames[r] }

func (r huntRow) serving() bool {
	switch r {
	case huntOld, huntOpenSurge, huntReplacementHeld, huntPromoted, huntFailedSurge, huntRetriesStopped, huntFailedGangKept, huntSurgeShort, huntReplacementShort:
		return true
	}
	return false
}

// partitionCandidate reports a row a partition may hold: off the target
// with no update attempt the roll must finish, as the roll's own rule
// reads it (an old row, a failed or parked attempt, a created Instance
// with no running revision yet).
func (r huntRow) partitionCandidate() bool {
	switch r {
	case huntOld, huntFailedDark, huntFailedSurge, huntRetriesStopped, huntCreating:
		return true
	}
	return false
}

// claimsUpdate reports a row the retry ladder and the surge budgets read
// as an attempt in flight: an open attempt, or a gang source keeping its
// attempt while its failed replacement is retired.
func (r huntRow) claimsUpdate() bool { return r.open() || r == huntFailedGangKept }

// surgeSlot reports a row that holds a surge slot: an open surge, a kept
// hand-over, a gang source keeping its attempt, and, for a single pod, a
// failed surge whose extra pod still stands.
func (r huntRow) surgeSlot(gang bool) bool {
	switch r {
	case huntOpenSurge, huntReplacementHeld, huntFailedGangKept, huntSurgeShort, huntReplacementShort:
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
	case huntOpenSurge, huntReplacementHeld, huntFailedGangKept, huntSurgeShort, huntReplacementShort:
		return true
	}
	return false
}

// open reports an update attempt in flight: the row carries an operation
// that neither failed nor parked.
func (r huntRow) open() bool {
	switch r {
	case huntOpenDrain, huntOpenInPlace, huntOpenSurge, huntReplacementHeld, huntSurgeShort, huntReplacementShort:
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
// finish, as many as the partition names.
func (s huntSide) held() map[int]bool {
	out := map[int]bool{}
	if s.partition <= 0 {
		return out
	}
	var n int32
	for idx, r := range s.rows {
		if n >= s.partition {
			break
		}
		if r.partitionCandidate() {
			out[idx] = true
			n++
		}
	}
	return out
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
	for _, r := range s.rows {
		parts = append(parts, r.String())
	}
	out := strings.Join(parts, ",")
	if s.schedule != scheduleNone {
		out += fmt.Sprintf(" schedule=%s/%d", s.schedule, s.attempts)
	}
	if s.partition > 0 {
		out += fmt.Sprintf(" partition=%d", s.partition)
	}
	if s.behind {
		out += " statusBehind"
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

// huntState is the model at one moment: every side, the holds of the last
// admission, the moves that led here, and the first start the last
// admission let the other side open before the side named to move first
// had settled the batch, when it did.
type huntState struct {
	sides     []huntSide
	holds     []*huntHold
	path      []huntMove
	outOfTurn string
}

func (st *huntState) clone() *huntState {
	out := &huntState{sides: make([]huntSide, len(st.sides)), holds: make([]*huntHold, len(st.holds)), path: append([]huntMove(nil), st.path...)}
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
			b.WriteByte(byte('a' + r))
		}
		fmt.Fprintf(&b, "|%d%d%d%d%d%d%d%d%t;", s.schedule, s.attempts, s.fails, s.parks, s.scales, s.partition, s.parts, s.losses, s.behind)
	}
	return b.String()
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
		if s.partition > 0 {
			return true
		}
	}
	return false
}

// huntMove is one thing the world does between two admissions. A scale or
// a partition change may land on two sides at once (side2 and delta2).
type huntMove struct {
	kind   string
	side   int
	idx    int
	delta  int32
	both   bool // the move lands on side2 too
	side2  int
	delta2 int32
}

// twoSided reports a move that lands on both sides at once.
func (m huntMove) twoSided() bool { return m.both }

func (m huntMove) String() string {
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
	case "backoff", "prune", "spent":
		return fmt.Sprintf("%s side %d", m.kind, m.side)
	}
	return fmt.Sprintf("%s side %d row %d", m.kind, m.side, m.idx)
}

// operator reports a move the operator makes, as against one the world
// makes: a partition change, a scale, a gang member lost.
func (m huntMove) operator() bool {
	return m.kind == "partition" || m.kind == "scale" || m.kind == "lose"
}

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
	scaleDeltas    []int32
	// probes runs the operator's moves at every moment of a plain roll
	// with the roll then played to its end, beside the breadth-first walk.
	probes bool
	// behindPush names the sides whose replica status predates the push
	// when the roll starts, one bit per member.
	behindPush uint8
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
	group := v1beta1.RolloutGroup{Components: components, RollingUpdate: &v1beta1.GroupRollingUpdate{}}
	if cfg.groupUnavail != "" {
		group.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(cfg.groupUnavail))
	}
	if cfg.groupSurge != "" {
		group.RollingUpdate.MaxSurge = ptr.To(intstr.Parse(cfg.groupSurge))
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: batchServiceName, Namespace: batchNamespace},
		Spec:       v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}}},
	}
	if cfg.tolerance != nil {
		isvc.Spec.Rollout.Groups[0].MaintainRatio = &v1beta1.MaintainRatio{Tolerance: cfg.tolerance}
		original := map[v1beta1.ComponentType]int32{}
		for _, m := range cfg.members {
			original[m.comp] = m.desired
		}
		isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{{
			Name: "0", Components: components, Policy: v1beta1.CoordinationPolicyRollingUpdate,
			ObservedRatio: &v1beta1.RolloutCoordinationRatio{Original: original},
		}}}
	}
	if cfg.protocolChange {
		isvc.Spec.Rollout.PairingProtocol = ptr.To("pair-v2")
	}
	isvc = pinActiveRun(isvc)
	w := &huntWorld{t: t, cfg: cfg, ctx: context.Background(), scheme: scheme, isvc: isvc}
	for _, m := range cfg.members {
		w.targets = append(w.targets, forwardTarget(isvc, m.comp))
	}
	return w
}

func (w *huntWorld) initial() *huntState {
	st := &huntState{sides: make([]huntSide, len(w.cfg.members)), holds: make([]*huntHold, len(w.cfg.members))}
	for i, m := range w.cfg.members {
		st.sides[i] = huntSide{rows: make([]huntRow, m.desired), fails: w.cfg.fails, parks: w.cfg.parks, scales: w.cfg.scales, parts: w.cfg.parts, behind: w.cfg.behindPush&(1<<i) != 0}
		if m.isGang() {
			st.sides[i].losses = w.cfg.losses
		}
	}
	return st
}

func (w *huntWorld) oldRevision(i int) string { return batchRevision(w.cfg.members[i].comp, "old") }

// replica is the InferenceReplica a side publishes.
func (w *huntWorld) replica(i int, s huntSide) *v1beta1.InferenceReplica {
	m := w.cfg.members[i]
	target, old := batchRevision(m.comp, "target"), w.oldRevision(i)
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
		// The partition the projector writes onto the replica, which the
		// roll reads first (irprojector.IRPartition).
		ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: ptr.To(s.partition)}
	}
	attempt := func(step string) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: target}
	}
	g := m.pods()
	var serving, promoted int32
	var replacements []v1beta1.OMENativeInstanceStatus
	for idx, kind := range s.rows {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(idx)}
		switch kind {
		case huntOld:
			row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, old, g, g, g
		case huntOpenDrain:
			row.Phase, row.RunningRevision, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, attempt(workloadtypes.UpdateStepDrain)
		case huntOpenInPlace:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, 1, attempt(workloadtypes.UpdateStepInPlace)
		case huntOpenSurge:
			if m.isGang() {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, g, g, g, attempt(workloadtypes.UpdateStepSurge)
			} else {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, 2, 1, 1, attempt(workloadtypes.UpdateStepSurge)
			}
		case huntReplacementHeld:
			if m.isGang() {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, g, g, g, attempt(workloadtypes.UpdateStepSurge)
			} else {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, 2, 2, 2, attempt(workloadtypes.UpdateStepSurge)
			}
		case huntPromoted:
			row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, target, g, g, g
		case huntPromotedUncounted:
			// The promote write carries the counters of the publication
			// before it, when the rebuilt pods stood but were not Ready.
			row.Phase, row.RunningRevision, row.PodCount = v1beta1.OMENativeInstanceReady, target, g
		case huntFailedDark:
			row.Phase, row.RunningRevision, row.TargetRevision = v1beta1.OMENativeInstanceFailed, old, target
		case huntFailedSurge:
			if m.isGang() {
				// The replacement gang was retired: the source stands as the
				// engine publishes a failed attempt, Ready over the old pods.
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, old, target, g, g, g
			} else {
				row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, old, target, 2, 1, 1
			}
		case huntFailedGangKept:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceReady, old, target, g, g, g, attempt(workloadtypes.UpdateStepSurge)
		case huntSurgeShort:
			// The source gang lost a worker: the leader keeps serving, the
			// counters read one pod short, the attempt stays at Surge.
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, g, g-1, g-1, attempt(workloadtypes.UpdateStepSurge)
		case huntReplacementShort:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, g, g, g, attempt(workloadtypes.UpdateStepSurge)
		case huntRetriesStopped:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceReady, old, target, 1, 1, 1, attempt(workloadtypes.UpdateStepParked)
		case huntCreating:
			row.Phase, row.TargetRevision, row.PodCount = v1beta1.OMENativeInstanceCreating, target, g
			row.Operation = &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate, TargetRevision: target}
		}
		if m.isGang() && kind.surgeTarget() {
			// The replacement gang lives in a slot of its own above the
			// desired count, taken from the source's attempt.
			slot := n + int32(len(replacements))
			row.Operation.SurgeIndex = ptr.To(slot)
			replacement := v1beta1.OMENativeInstanceStatus{Index: slot, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: target, PodCount: g,
				Operation: &v1beta1.InstanceOperation{ID: "replacement", Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepGangSurgeTarget, TargetRevision: target}}
			switch kind {
			case huntReplacementHeld:
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
		if kind.serving() {
			serving++
		}
		if kind == huntPromoted || kind == huntPromotedUncounted {
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
	oldProto, newProto := "", ""
	if w.cfg.protocolChange {
		oldProto, newProto = "pair-v1", "pair-v2"
	}
	for i, m := range w.cfg.members {
		objs = append(objs, w.replica(i, st.sides[i]),
			pairingCR(batchNamespace, batchServiceName, m.comp, "old", oldProto),
			pairingCR(batchNamespace, batchServiceName, m.comp, "target", newProto))
		if !w.cfg.protocolChange {
			continue
		}
		if m.isGang() {
			for idx, kind := range st.sides[i].rows {
				switch kind {
				case huntOld, huntRetriesStopped, huntFailedSurge:
					objs = append(objs, w.gangPods(i, idx, oldProto, true)...)
				case huntPromoted:
					objs = append(objs, w.gangPods(i, idx, newProto, true)...)
				case huntPromotedUncounted:
					objs = append(objs, w.gangPods(i, idx, newProto, false)...)
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
				}
			}
			continue
		}
		for idx, kind := range st.sides[i].rows {
			switch kind {
			case huntOld, huntRetriesStopped:
				objs = append(objs, w.pod(i, idx, 0, oldProto, true))
			case huntPromoted:
				objs = append(objs, w.pod(i, idx, 1, newProto, true))
			case huntPromotedUncounted:
				objs = append(objs, w.pod(i, idx, 1, newProto, false))
			case huntOpenSurge, huntFailedSurge:
				objs = append(objs, w.pod(i, idx, 0, oldProto, true), w.pod(i, idx, 1, newProto, false))
			case huntReplacementHeld:
				objs = append(objs, w.pod(i, idx, 0, oldProto, true), w.pod(i, idx, 1, newProto, true))
			case huntCreating:
				objs = append(objs, w.pod(i, idx, 0, newProto, false))
			}
		}
	}
	return objs
}

func (w *huntWorld) client(st *huntState) client.Client {
	return fake.NewClientBuilder().WithScheme(w.scheme).WithObjects(w.objects(st)...).Build()
}

// handOverAllowed asks the drain gate whether the old pod of a surging
// Instance whose replacement serves may leave rotation.
func (w *huntWorld) handOverAllowed(st *huntState, i, idx int) bool {
	if !w.cfg.protocolChange {
		return true
	}
	probe := st.clone()
	probe.sides[i].rows[idx] = huntReplacementHeld
	w.gateCalls++
	allowed, _, _ := EvaluatePairingDrain(w.ctx, w.client(probe), w.isvc, w.cfg.members[i].comp, w.targets[i], w.cfg.defaults, w.sourcePodNames(i, idx))
	return allowed
}

// pass is one update pass of one Component: the ladder, then every fresh
// start candidate in selection order (Instances out of rotation first, then
// index order), each against the per-Component budget and the gate, every
// admitted start stamped before the next question. The first denial is
// the side's hold.
func (w *huntWorld) pass(st *huntState, i int) (admitted int, hold *huntHold) {
	m := w.cfg.members[i]
	side := &st.sides[i]
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
		if r == huntFailedDark && !held[idx] {
			order = append(order, idx)
		}
	}
	for idx, r := range side.rows {
		if (r == huntOld || r == huntFailedSurge || r == huntRetriesStopped) && !held[idx] {
			order = append(order, idx)
		}
	}
	denied := map[huntRow]bool{}
	var inFlightSurge, inFlightUnavail, gateUnavail int32
	var c client.Client
	for _, idx := range order {
		kind := side.rows[idx]
		if denied[kind] {
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
		exempt := kind == huntFailedDark
		if exempt {
			mechanism = workloadtypes.UpdateStrategyRecreatePod
		}
		var h *huntHold
		if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
			budget := int32(-1)
			if m.budget != "" {
				budget = utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(m.budget)), int32(len(side.rows)), true)
			}
			var prior int32
			for _, r := range side.rows {
				if r.surgeSlot(m.isGang()) {
					prior++
				}
			}
			if kind == huntFailedSurge && !m.isGang() {
				prior-- // the retry takes its own failed pod's slot
			}
			if budget >= 0 && prior+inFlightSurge+1 > budget {
				h = &huntHold{gate: v1beta1.RolloutHoldGateBudget, reason: fmt.Sprintf("per-Component surge budget %d exhausted (would become %d)", budget, prior+inFlightSurge+1)}
			}
		} else {
			budget := int32(-1)
			if m.budget != "" {
				budget = utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(m.budget)), int32(len(side.rows)), true)
			}
			prior := side.count(huntOpenDrain, huntOpenInPlace)
			if budget >= 0 && prior+inFlightUnavail+1 > budget {
				h = &huntHold{gate: v1beta1.RolloutHoldGateBudget, reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)", budget, prior+inFlightUnavail+1)}
			}
		}
		if h == nil && !exempt {
			if c == nil {
				c = w.client(st)
			}
			w.gateCalls++
			allowed, gate, reason := EvaluateUpdateGate(w.ctx, c, w.isvc, m.comp, w.oldRevision(i), w.targets[i], nil, w.cfg.defaults, mechanism, inFlightSurge, gateUnavail)
			if !allowed {
				h = &huntHold{gate: gate, reason: reason}
			} else if message, out := w.outOfTurn(st, i); out && st.outOfTurn == "" {
				st.outOfTurn = message
			}
		}
		if h != nil {
			denied[kind] = true
			if hold == nil {
				hold = h
			}
			continue
		}
		switch {
		case mechanism == workloadtypes.UpdateStrategySurgeThenDrain:
			side.rows[idx] = huntOpenSurge
			inFlightSurge++
		case mechanism == workloadtypes.UpdateStrategyRecreatePod:
			side.rows[idx] = huntOpenDrain
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
	// The pass ends with the side's status written for the push.
	side.behind = false
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
	gate := ResolveGateContextWithDefaults(w.ctx, w.client(written), w.isvc, w.cfg.members[i].comp, w.cfg.defaults)
	gate.TargetRevision = w.targets[i]
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
		total := 0
		for _, i := range order {
			a, h := w.pass(st, i)
			st.holds[i] = h
			total += a
		}
		if total == 0 {
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
	for i, s := range st.sides {
		seen := map[huntRow]int{}
		last := map[huntRow]int{}
		for idx, r := range s.rows {
			if _, ok := seen[r]; !ok {
				seen[r] = idx
			}
			last[r] = idx
		}
		rowsOf := func(kind huntRow) []int {
			first, ok := seen[kind]
			if !ok {
				return nil
			}
			if last[kind] != first {
				return []int{first, last[kind]}
			}
			return []int{first}
		}
		for _, kind := range []huntRow{huntOpenDrain, huntOpenInPlace, huntOpenSurge, huntCreating, huntSurgeShort, huntPromotedUncounted} {
			for _, idx := range rowsOf(kind) {
				out = append(out, huntMove{kind: "serve", side: i, idx: idx})
			}
		}
		for _, idx := range rowsOf(huntReplacementHeld) {
			if w.handOverAllowed(st, i, idx) {
				out = append(out, huntMove{kind: "release", side: i, idx: idx})
			}
		}
		for _, kind := range []huntRow{huntSurgeShort, huntReplacementShort} {
			for _, idx := range rowsOf(kind) {
				out = append(out, huntMove{kind: "mend", side: i, idx: idx})
			}
		}
		for _, idx := range rowsOf(huntFailedGangKept) {
			out = append(out, huntMove{kind: "retire", side: i, idx: idx})
		}
		switch {
		case s.schedule == scheduleBackoff:
			out = append(out, huntMove{kind: "backoff", side: i})
		case s.schedule == scheduleRetrying && !s.inFlight():
			out = append(out, huntMove{kind: "prune", side: i})
		}
	}
	return out
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
// scaled, both sides are scaled by the same step, or both to a new ratio.
func (w *huntWorld) adverseMoves(st *huntState) []huntMove {
	var out []huntMove
	for i, s := range st.sides {
		seen := map[huntRow]bool{}
		for idx, r := range s.rows {
			if seen[r] {
				continue
			}
			seen[r] = true
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
			}
		}
		if s.schedule != scheduleHeld && s.schedule != scheduleNone && s.count(huntFailedDark, huntFailedSurge) > 0 {
			out = append(out, huntMove{kind: "spent", side: i})
		}
		if s.scales > 0 {
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
	}
	if len(st.sides) >= 2 {
		a, b := st.sides[0], st.sides[1]
		na, nb := int32(len(a.rows)), int32(len(b.rows))
		if a.scales > 0 && b.scales > 0 {
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
	return out
}

// apply makes one move and then lets the dispatcher admit what the gates
// allow.
func (w *huntWorld) apply(st *huntState, mv huntMove) *huntState {
	next := st.clone()
	next.path = append(next.path, mv)
	w.applyRaw(next, mv)
	w.settle(next, w.order(len(next.path)))
	return next
}

// applyRaw makes one move on the state without the admission that follows.
func (w *huntWorld) applyRaw(next *huntState, mv huntMove) {
	side := &next.sides[mv.side]
	switch mv.kind {
	case "serve":
		switch side.rows[mv.idx] {
		case huntOpenSurge, huntSurgeShort:
			if w.handOverAllowed(next, mv.side, mv.idx) {
				side.rows[mv.idx] = huntPromoted
			} else {
				side.rows[mv.idx] = huntReplacementHeld
			}
		case huntOpenDrain, huntOpenInPlace:
			// A rebuilt or patched Instance is promoted in one status write
			// and its pods counted as serving in the next; the pairing gate
			// reads the publication in between, so during a protocol change
			// it is a moment of its own.
			if w.cfg.protocolChange {
				side.rows[mv.idx] = huntPromotedUncounted
			} else {
				side.rows[mv.idx] = huntPromoted
			}
		default:
			side.rows[mv.idx] = huntPromoted
		}
	case "release":
		side.rows[mv.idx] = huntPromoted
	case "retire":
		side.rows[mv.idx] = huntFailedSurge
	case "lose":
		side.losses--
		if side.rows[mv.idx] == huntReplacementHeld {
			side.rows[mv.idx] = huntReplacementShort
		} else {
			side.rows[mv.idx] = huntSurgeShort
		}
	case "mend":
		switch side.rows[mv.idx] {
		case huntSurgeShort:
			side.rows[mv.idx] = huntOpenSurge
		case huntReplacementShort:
			// The replacement serves whole again; the drain gate is asked
			// once more before the old gang leaves.
			if w.handOverAllowed(next, mv.side, mv.idx) {
				side.rows[mv.idx] = huntPromoted
			} else {
				side.rows[mv.idx] = huntReplacementHeld
			}
		}
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
			side.rows[mv.idx] = huntFailedDark
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
	}
}

// scaleSide resizes one side: a scale-down removes the highest-indexed
// Instances, whatever they are doing; a scale-up creates new Instances on
// the target. The plans read the new count from the spec at once.
func scaleSide(side *huntSide, delta int32) {
	side.scales--
	if delta < 0 {
		side.rows = side.rows[:int32(len(side.rows))+delta]
		return
	}
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
	// gates do right after one is reported in classes of its own.
	scaled := false
	for _, s := range st.sides {
		if s.scales < w.cfg.scales {
			scaled = true
		}
	}
	budgetClass, promiseSuffix := "budget", ""
	if scaled {
		budgetClass, promiseSuffix = "budget-after-scale", "-after-scale"
	}
	// Under a partition a side stops at its movable count while the other
	// runs to its end; where the batch step counts held Instances among
	// the batches the promise reads off its marks anyway.
	if st.partitioned() {
		promiseSuffix = "-under-partition"
	}
	// During a protocol change the pairing waiver lets a side run ahead
	// of a peer the pairing gate holds still, by design.
	if w.cfg.protocolChange {
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
		pacing := pacingWithDefaults(&v1beta1.CoordinationPacing{MaxUnavailable: w.isvc.Spec.Rollout.Groups[0].RollingUpdate.MaxUnavailable, MaxSurge: w.isvc.Spec.Rollout.Groups[0].RollingUpdate.MaxSurge}, w.cfg.defaults)
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
			if group := MaxSurgeBudget(pacing, n); open > group {
				add(budgetClass, "%s has %d surges in flight against the group budget of %d", m.comp, open, group)
			}
		} else {
			open := s.count(huntOpenDrain, huntOpenInPlace)
			if own >= 0 && open > own {
				add(budgetClass, "%s has %d Instances out of rotation against its own budget of %d", m.comp, open, own)
			}
			// The group arm counts every Instance not serving, the failed
			// ones included; a scale-up's new Instances are not drains and
			// are left out.
			if group := MaxUnavailableBudget(pacing, n); open+s.count(huntFailedDark) > group {
				add(budgetClass, "%s has %d Instances out of rotation beside %d failed against the group unavailability budget of %d", m.comp, open, s.count(huntFailedDark), group)
			}
		}
	}
	// (b) the batch promise: the two sides' started counts sit inside one
	// planned batch of each other; the engine's reading of the rows agrees
	// with the model's.
	eng, dec := w.pairIndex()
	if eng >= 0 && dec >= 0 {
		gate := ResolveGateContextWithDefaults(w.ctx, w.client(st), w.isvc, v1beta1.EngineComponent, w.cfg.defaults)
		gate.TargetRevision = w.targets[eng]
		batch, err := gate.readBatchStep()
		if err != nil {
			add("read", "batch step read failed: %v", err)
		}
		if batch.governs {
			members := map[v1beta1.ComponentType]batchMember{}
			for _, bm := range batch.members {
				members[bm.component] = bm
			}
			type pos struct{ lo, hi int32 }
			positions := map[v1beta1.ComponentType]pos{}
			for _, i := range []int{eng, dec} {
				m := w.cfg.members[i]
				s := st.sides[i]
				bm, ok := members[m.comp]
				if !ok {
					continue
				}
				// The model's reading of the rows, by the tree's rule: a
				// batch step that counts a partition's held Instances
				// leaves them out of every other count.
				heldRows := s.held()
				var promoted, open, ended, heldCount int32
				for idx, r := range s.rows {
					if heldRows[idx] {
						heldCount++
						continue
					}
					switch r {
					case huntPromoted, huntPromotedUncounted:
						promoted++
					case huntOpenDrain, huntOpenInPlace, huntOpenSurge, huntReplacementHeld, huntSurgeShort, huntReplacementShort:
						open++
					case huntFailedDark, huntFailedSurge, huntRetriesStopped, huntFailedGangKept:
						ended++
					}
				}
				if bm.promoted != promoted || bm.open != open || bm.ended != ended || bm.held != heldCount {
					add("classifier", "%s rows read as promoted %d open %d ended %d held %d by the batch step, the model holds %d %d %d %d (%s)", m.comp, bm.promoted, bm.open, bm.ended, bm.held, promoted, open, ended, heldCount, s)
				}
				// A side sits inside every batch its started count has
				// reached but not passed: from the first batch whose mark
				// is its count up to the batch after the last such mark,
				// so an empty planned batch does not move it.
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
				positions[m.comp] = pos{lo, hi}
			}
			if pe, pd := positions[v1beta1.EngineComponent], positions[v1beta1.DecoderComponent]; len(positions) == 2 && (pe.lo > pd.hi || pd.lo > pe.hi) {
				class := "batch-promise" + promiseSuffix
				if keepRatio, _ := huntRulesOf(w.cfg.defaults); keepRatio {
					class = "ratio-promise" + promiseSuffix
				}
				// A side whose promoted Instance is not yet counted as serving
				// is coming up on its own; nothing the other side does moves
				// it, so running ahead of it is never the pairing waiver's.
				// A scale re-plans the marks and a partition ends one side
				// early, so the gap is theirs there, as the suffix says.
				behind := eng
				if pe.lo > pd.hi {
					behind = dec
				}
				if !scaled && !st.partitioned() && st.sides[behind].count(huntPromotedUncounted) > 0 {
					class = "runs-ahead-of-a-peer-coming-up"
				}
				se, sd := members[v1beta1.EngineComponent], members[v1beta1.DecoderComponent]
				add(class, "the engine has started %d (batch %d..%d of marks %v) while the decoder has started %d (batch %d..%d of marks %v); more than one batch apart", se.started(), pe.lo, pe.hi, huntMarks(se), sd.started(), pd.lo, pd.hi, huntMarks(sd))
			}
		}
	}
	// (d) the partner promise during a protocol change: a surging side's old
	// pod leaves rotation only once its replacement has a partner, so no
	// Instance of that side serves the new cohort alone while the other
	// side still serves the old cohort and none of the new (a rebuilt
	// Instance promoted but not yet counted as serving is on the new
	// cohort: its pod is what the drain gate read). A side scaled
	// up builds its new Instances on the new cohort with no old pod to
	// keep, so after a scale the reading is reported in a class of its own.
	if w.cfg.protocolChange && eng >= 0 && dec >= 0 {
		for _, pair := range [][2]int{{eng, dec}, {dec, eng}} {
			i, o := pair[0], pair[1]
			if !w.cfg.members[i].surges() {
				continue
			}
			onNew := st.sides[i].count(huntPromoted)
			otherNew := st.sides[o].count(huntPromoted, huntPromotedUncounted, huntReplacementHeld, huntReplacementShort)
			otherOld := st.sides[o].count(huntOld, huntRetriesStopped, huntFailedSurge, huntOpenSurge, huntReplacementHeld, huntSurgeShort, huntFailedGangKept)
			if onNew > 0 && otherNew == 0 && otherOld > 0 {
				class := "stranded-replacement"
				if scaled {
					class += "-after-scale"
				}
				add(class, "%d %s Instance(s) serve the new cohort with no %s partner while %d %s Instance(s) still serve the old cohort (%s)",
					onNew, w.cfg.members[i].comp, w.cfg.members[o].comp, otherOld, w.cfg.members[o].comp, st.sides[i])
			}
		}
	}
	// (e) the first-side rule: a start of the other side opened only once
	// the named side's batch had settled, on the rows as written. During a
	// protocol change the pairing waiver lets the other side go first by
	// design, so the reading is reported in a class of its own there.
	if st.outOfTurn != "" {
		class := "starts-before-the-named-side"
		if w.cfg.protocolChange {
			class += "-in-protocol-change"
		}
		add(class, "%s", st.outOfTurn)
	}
	// (a) progress: the roll is not done, nothing can move on its own,
	// and no side was admitted a start.
	if !st.done() && len(w.progressMoves(st)) == 0 {
		class, message := w.classifyStandstill(st)
		add(class, "%s", message)
	}
	return out
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
	}
	w.settle(probe, w.order(len(probe.path)))
	return w.playOut(probe)
}

// classifyStandstill names why a state that cannot move is what it is. Every
// side with Instances to move is Held ("held"), a side whose hold reads
// Held because the named first side's retry record is Held counting as
// held with it, the roll held as a whole; one side is Held and the
// other waits on something only the Held side could change
// ("waits-on-held-peer"; where a spent ladder leaves the other side is
// not decided yet); a side waits on a peer's batch whose only
// unserved Instances its partition holds ("waits-on-held-instances"); the
// ratio rule holds a side where the batch step does not govern
// ("ratio-rule"); a hold the
// operator's partition explains, because clearing it lets the roll finish
// ("partition-paced"); or no side is Held and the sides wait on each other
// or on themselves ("deadlock").
func (w *huntWorld) classifyStandstill(st *huntState) (class, message string) {
	var lines []string
	held, waiting := 0, 0
	ratioRule, onHeldInstances := false, false
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
		if h != nil && h.gate == v1beta1.RolloutHoldGateRatio && (strings.Contains(h.reason, "would outpace") || strings.Contains(h.reason, "would skew")) {
			ratioRule = true
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
	switch {
	case waiting == 0:
		class = "held"
	case held > 0:
		class = "waits-on-held-peer"
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
	case st.partitioned():
		// A hold beside a partition is the partition's when clearing it
		// lets the roll finish; when the roll stops again without any
		// partition, that stop names the class.
		cleared := w.withoutPartitions(st)
		if cleared.complete() {
			class = "partition-paced"
			break
		}
		class, message = w.classifyStandstill(cleared)
		if class == "partition-paced" {
			class = "deadlock"
		}
		return class, strings.Join(lines, "; ") + "; with every partition cleared the roll stops as " + class + ": " + message
	default:
		class = "deadlock"
	}
	return class, strings.Join(lines, "; ")
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
			if !mv.operator() {
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
	return strings.Join(parts, " | ")
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
	for _, shape := range partitionShapes {
		for _, pair := range [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}} {
			for _, setting := range partitionSettings {
				for _, tolerance := range partitionTolerances {
					out = append(out, probed(mk(shape, pair[0], pair[1], "10%", setting, false, "", 3, tolerance, 0), 3))
					if full {
						out = append(out, probed(mk(shape, pair[0], pair[1], "10%", setting, true, "", 3, tolerance, 0), 3))
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
// deadlock, budget, classifier, batch-promise, ratio-promise or
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
// before the named side's batch settled fails the test); partition-paced
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
		case "deadlock", "budget", "classifier", "batch-promise", "ratio-promise", "runs-ahead-of-a-peer-coming-up", "read", "depth", "starts-before-the-named-side", "stranded-replacement", "waits-on-held-instances":
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
				if handOver < 0 || w.handOverAllowed(st, ki, handOver) {
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
// side (whether it should finish its own roll) is not decided yet; this
// test records the current behaviour and is updated when it changes.
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
				eng: append(append(rows(huntPromoted, 3), huntFailedDark), rows(huntOld, 8)...),
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
// reference and refuses a second while one is out, under "drain would skew
// cross-Component serving ratio past tolerance"; the pair finishes and the
// router finishes behind it, one Instance at a time. The router's own
// budget is wide enough for both, so the band is what paces it. Recorded
// here so a change in how the band paces the router is noticed.
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
			if h := st.holds[2]; !st.sides[2].complete() && st.sides[2].count(huntOld) > 0 && (h == nil || h.gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(h.reason, "skew")) {
				t.Fatalf("with one router Instance out, the band must hold the other: hold = %s, state %s", h, st)
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
