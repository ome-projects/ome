package coordination_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils"
)

// This file drives the full gate stack (EvaluateUpdateGate, with the
// Sequential gate last in it), the run layer (rolloutrun.Reconcile, which
// opens, keeps, closes and replaces the run the plan gate reads) and the
// group phase machine (ComputeTransition, whose finish record the
// Sequential wait is measured from) over a model of one Sequential chain:
// two or three Components declared as single-Component blueGreen groups in
// list order, which ResolveGroups folds into one Sequential group. The
// model is each member's InferenceReplica as the replica controller
// publishes it: the rows, the current and update revisions, the generation
// the status was written for, the generation the projector recorded, the
// partition the replica holds Instances at, the retry record and the Ready
// condition; and the service's status as the service controller writes it:
// the run, the plan conditions and the group entries.
//
// Between passes the world makes one move: the projector reaches a member,
// a started Instance serves, a pod, a whole gang or one gang member is
// lost, an attempt fails, the retry record moves, the wait after readiness
// passes, or the operator acts (a partition set, raised, lowered or cleared
// on a member; a canary step's partition written on a member; a scale, on
// one member or through a scaling policy that moves the follower with its
// anchor; a pause and a resume; a second push; a push back to version 1).
// After every move the service controller's pass runs (the run layer, then
// the group machine), then the dispatcher's admission runs for every member
// the way the replica controller runs it: the retry record, the
// per-Component budget and the gate, every admitted start written to its
// row before the next question, the Create pass rebuilding rows that lost
// their pods, and the status write that ends a pass. Every path is explored
// breadth-first to a bound with a hash of visited states, and seeded runs go
// deeper. At every state the model checks that the chain can still reach
// its end with its run closed, that no later Component started while an
// earlier one was unfinished, that nothing started under a pause, and that
// the budgets hold. OME_SEQUENTIAL_HUNT=full widens the shapes, the sample
// and the bounds.

const (
	// seqServiceName and seqNamespace name the service every chain is built
	// under; the replicas and revisions take their names from them.
	seqServiceName = "svc"
	seqNamespace   = "ns"
	// seqReadyCondition is the replica's Ready condition type: True once the
	// member's roll is done, and the clock the wait after it starts from.
	seqReadyCondition = "Ready"
	// seqLongAgo is how far back a clock is moved once the wait after it has
	// passed; every configured wait is shorter.
	seqLongAgo = 2 * time.Hour
)

// seqLastRevision is the highest version the operator pushes: version 1 is
// what the fleet stands on, version 2 the push, version 3 a later push.
const seqLastRevision int8 = 3

// seqRevisionName names a Component's revision the way the replica records
// it; the trailing segment is the hash the gates compare.
func seqRevisionName(comp v1beta1.ComponentType, rev int8) string {
	if rev <= 0 {
		return ""
	}
	return seqServiceName + "-" + string(comp) + "-v" + strconv.Itoa(int(rev))
}

// seqRevisionOf reads the version back off a revision name; 0 for none.
func seqRevisionOf(name string) int8 {
	i := strings.LastIndex(name, "-v")
	if i < 0 {
		return 0
	}
	n, err := strconv.Atoi(name[i+2:])
	if err != nil {
		return 0
	}
	return int8(n)
}

// seqRevisionOfHash reads the version off a revision hash, the trailing
// segment of the run layer's target ("v2"); 0 for none.
func seqRevisionOfHash(hash string) int8 {
	n, err := strconv.Atoi(strings.TrimPrefix(hash, "v"))
	if err != nil {
		return 0
	}
	return int8(n)
}

// seqRevisionObject is the ControllerRevision of one version of a member,
// named the way the controller names it, with no pairing protocol.
func seqRevisionObject(comp v1beta1.ComponentType, rev int8) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Namespace: seqNamespace, Name: seqRevisionName(comp, rev)}}
}

// seqParentGeneration is the projector's record of the service generation a
// replica was last projected from.
func seqParentGeneration(gen int64) map[string]string {
	return map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: strconv.FormatInt(gen, 10)}
}

// seqRowKind is what the roll has done to one Instance, as its row reads.
type seqRowKind uint8

const (
	seqReady           seqRowKind = iota // every pod Ready and serving on rev
	seqLost                              // every pod is gone; the row reads Pending on rev
	seqShort                             // a gang short of one member; the row keeps Ready on rev with the counts short, out of rotation
	seqRebuild                           // the Create pass rebuilds the pods toward to; the row records rev
	seqDrain                             // a recreate toward to is open; nothing serves
	seqSurge                             // a surge toward to is open; the source serves on rev
	seqFailedUnserving                   // a recreate toward to ended Failed; nothing serves
	seqFailedSurge                       // a surge toward to failed; published Ready over the serving source
	seqCreating                          // a new Instance builds toward to
)

var seqRowKindNames = [...]string{"ready", "lost", "short", "rebuild", "drain", "surge", "failed", "failedsurge", "creating"}

// seqRow is one Instance: its kind, the revision it runs (0 before it has
// served) and the revision its attempt, its rebuild or its creation moves
// toward. scaled marks an Instance a scale-up created during the roll.
type seqRow struct {
	kind   seqRowKind
	rev    int8
	to     int8
	scaled bool
}

func (r seqRow) serving() bool {
	switch r.kind {
	case seqReady, seqSurge, seqFailedSurge:
		return true
	}
	return false
}

// open reports an update attempt in flight.
func (r seqRow) open() bool { return r.kind == seqDrain || r.kind == seqSurge }

func (r seqRow) String() string {
	s := seqRowKindNames[r.kind]
	if r.rev > 0 {
		s += "@" + strconv.Itoa(int(r.rev))
	}
	if r.to > 0 && r.kind != seqReady && r.kind != seqShort && r.kind != seqLost && r.to != r.rev {
		s += ">" + strconv.Itoa(int(r.to))
	}
	if r.scaled {
		s += "+"
	}
	return s
}

// seqRetry is the state of a member's retry record for its target.
type seqRetry uint8

const (
	seqRetryNone     seqRetry = iota
	seqRetryBackoff           // a failure was recorded; no fresh start until the backoff ends
	seqRetryDue               // the backoff ended; the next pass admits a wave of starts
	seqRetryRetrying          // one wave at a time while an attempt is in flight
	seqRetryHeld              // the retries are spent; no fresh start until a new revision
)

var seqRetryNames = [...]string{"", "backoff", "due", "retrying", "held"}

// seqMemberConfig is one Component of the chain: its count, the pods of each
// Instance, how it rolls, its own budget and the user partition in place
// before the push.
type seqMemberConfig struct {
	comp      v1beta1.ComponentType
	desired   int32
	pods      int32 // pods per Instance: 1 for a single pod, 2 to 8 for a gang
	strategy  v1beta1.UpdateStrategyType
	budget    string // the member's own maxSurge when it surges, else its own maxUnavailable
	partition int32  // the rollingUpdate.partition set before the push; 0 for none
}

func (m seqMemberConfig) surges() bool { return m.strategy == v1beta1.UpdateStrategySurgeThenDrain }

func (m seqMemberConfig) short() string {
	if m.surges() {
		return "surge"
	}
	return "drain"
}

// seqBudgets is how many times the world may make each costly move in one
// run.
type seqBudgets struct {
	fails, losses, scales, partitionEdits, steps, pushes, rollbacks, pauses int8
}

// seqConfig is one configuration of the chain under exploration.
type seqConfig struct {
	name        string
	members     []seqMemberConfig // in chain order
	afterReady  time.Duration     // the wait after a Component is Ready before the next starts (the group's wait setting)
	maxAttempts int32
	// scalingPolicy makes the decoder follow the engine's count at the ratio
	// the shape declares: a resize of the engine resizes the decoder in the
	// same spec write, the way the scaling coordinator applies a policy.
	scalingPolicy bool
	budgets       seqBudgets
}

func (c seqConfig) String() string {
	parts := make([]string, 0, len(c.members))
	for _, m := range c.members {
		p := fmt.Sprintf("%s=%d", m.comp, m.desired)
		if m.pods > 1 {
			p += fmt.Sprintf("x%d", m.pods)
		}
		p += fmt.Sprintf("/%s/%s", m.short(), m.budget)
		if m.partition > 0 {
			p += fmt.Sprintf("/partition=%d", m.partition)
		}
		parts = append(parts, p)
	}
	s := strings.Join(parts, " -> ")
	if c.afterReady > 0 {
		s += " wait=" + c.afterReady.String()
	}
	if c.scalingPolicy {
		s += " scaling-policy"
	}
	return s + fmt.Sprintf(" retries=%d", c.maxAttempts)
}

// anchor is the member whose count a scaling policy drives: the engine when
// the chain has one, else the first member.
func (c seqConfig) anchor() int {
	for i, m := range c.members {
		if m.comp == v1beta1.EngineComponent {
			return i
		}
	}
	return 0
}

// follower is the member a scaling policy resizes with the anchor: the
// decoder; -1 when the chain has none.
func (c seqConfig) follower() int {
	for i, m := range c.members {
		if m.comp == v1beta1.DecoderComponent && i != c.anchor() {
			return i
		}
	}
	return -1
}

// seqMember is one member's replica as published, with the world's
// remaining moves on it.
type seqMember struct {
	rows []seqRow
	// published is the rows as the member's last status write published
	// them: what the gates, the run layer and the group machine read, one
	// pass behind the rows the member's own pass acts on.
	published []seqRow
	retry     seqRetry
	attempts  int32
	retryRev  int8 // the revision the retry record names

	target       int8 // the revision the replica's spec asks for, as projected
	statusTarget int8 // UpdateRevision as published
	current      int8 // CurrentRevision as published; 0 while the engine keeps it empty
	clearedFrom  int8 // the revision an empty current revision was cleared from while an Instance still ran another

	irGen      int64 // the replica's metadata.generation
	statusGen  int64 // status.observedGeneration
	annotation int64 // the parent generation the projector recorded

	userPartition   int32 // the projected rollingUpdate.partition; 0 for none
	pacingPartition int32 // the projected spec.pacing.partition; -1 for none
	replicas        int32 // the projected spec.replicas

	ready     metav1.ConditionStatus // the Ready condition: True, Unknown while a roll is recorded, False with no roll in flight and the serving minimum not met
	readyTrue bool                   // the Ready condition reads True
	waited    bool                   // the wait after readiness has passed since Ready turned True
	flips     int8                   // how often Ready left True again after it had been waited out

	fails, losses, scales, partitionEdits, steps int8
}

func (m seqMember) count(kinds ...seqRowKind) int32 {
	var n int32
	for _, r := range m.rows {
		for _, k := range kinds {
			if r.kind == k {
				n++
			}
		}
	}
	return n
}

func (m seqMember) inFlight() bool {
	for _, r := range m.rows {
		if r.open() {
			return true
		}
	}
	return false
}

// partition is the partition the replica holds Instances at: the projected
// pacing value when set (an explicit 0 releases every Instance), else the
// user's rollingUpdate.partition.
func (m seqMember) partition() int32 {
	if m.pacingPartition >= 0 {
		return m.pacingPartition
	}
	return m.userPartition
}

func (m seqMember) String() string {
	parts := make([]string, 0, len(m.rows))
	for _, r := range m.rows {
		parts = append(parts, r.String())
	}
	out := strings.Join(parts, ",")
	out += fmt.Sprintf(" target=%d current=%d", m.target, m.current)
	if m.retry != seqRetryNone {
		out += fmt.Sprintf(" retry=%s/%d", seqRetryNames[m.retry], m.attempts)
	}
	if p := m.partition(); p > 0 || m.pacingPartition == 0 {
		out += fmt.Sprintf(" partition=%d", p)
	}
	switch m.ready {
	case metav1.ConditionTrue:
		out += " ready"
		if m.waited {
			out += "+waited"
		}
	case metav1.ConditionFalse:
		out += " not-ready"
	}
	return out
}

// seqHold is the first denial a member met in its last pass.
type seqHold struct {
	gate   v1beta1.RolloutHoldGate
	reason string
}

func (h *seqHold) String() string {
	if h == nil {
		return "none"
	}
	return fmt.Sprintf("%s: %s", h.gate, h.reason)
}

// seqMove is one thing the world does between two admissions.
type seqMove struct {
	kind   string
	member int
	idx    int
	value  int32
}

func (m seqMove) String() string {
	switch m.kind {
	case "push", "rollback", "pause", "resume":
		return m.kind
	case "project", "project-between":
		return m.kind
	case "backoff", "prune", "spent", "wait":
		return fmt.Sprintf("%s member %d", m.kind, m.member)
	case "scale", "partition", "step":
		return fmt.Sprintf("%s member %d to %d", m.kind, m.member, m.value)
	}
	return fmt.Sprintf("%s member %d row %d", m.kind, m.member, m.idx)
}

// seqState is the model at one moment: the service spec, the service's
// status as the service controller last wrote it, every member's replica,
// the holds of the last admission, the order breaks the passes recorded, and
// the moves that led here.
type seqState struct {
	members    []seqMember
	gen        int64   // the service's generation
	rev        int8    // the revision the service spec names
	desired    []int32 // the count the spec asks of each member
	partitions []int32 // the user partition the spec sets on each member
	paused     bool    // the operator's pause annotation stands
	pushes     int8
	rollbacks  int8
	pauses     int8
	holds      []*seqHold
	breaks     []seqBreak
	path       []seqMove
	// service carries what the service controller writes between passes:
	// the run and its plan conditions, and the group entries with the
	// finish record the gate reads the wait from.
	service *v1beta1.InferenceService
}

// seqBreak is an order break a pass recorded: the class it falls in and
// what started ahead of what.
type seqBreak struct {
	class, message string
}

func (st *seqState) clone() *seqState {
	out := &seqState{
		members: make([]seqMember, len(st.members)), gen: st.gen, rev: st.rev,
		desired: append([]int32(nil), st.desired...), partitions: append([]int32(nil), st.partitions...),
		paused: st.paused, pushes: st.pushes, rollbacks: st.rollbacks, pauses: st.pauses,
		holds: make([]*seqHold, len(st.holds)), breaks: append([]seqBreak(nil), st.breaks...), path: append([]seqMove(nil), st.path...),
		service: st.service.DeepCopy(),
	}
	for i, m := range st.members {
		m.rows = append([]seqRow(nil), m.rows...)
		m.published = append([]seqRow(nil), m.published...)
		out.members[i] = m
	}
	return out
}

// activeRun is the run the service's status holds open, nil for none.
func (st *seqState) activeRun() *v1beta1.RolloutRun {
	if st.service.Status.Rollout == nil {
		return nil
	}
	return st.service.Status.Rollout.ActiveRun
}

// lastRunOutcome is how the last closed run ended, "" for none.
func (st *seqState) lastRunOutcome() v1beta1.RolloutRunOutcome {
	if st.service.Status.Rollout == nil || st.service.Status.Rollout.LastRun == nil {
		return ""
	}
	return st.service.Status.Rollout.LastRun.Outcome
}

// groupEntries are the coordination group entries the service's status
// carries.
func (st *seqState) groupEntries() []v1beta1.RolloutCoordinationGroupStatus {
	if st.service.Status.RolloutCoordination == nil {
		return nil
	}
	return st.service.Status.RolloutCoordination.Groups
}

// finishRecorded reports the group entry's finish record for a member and
// whether the wait after it has passed by the record's clock.
func (st *seqState) finishRecorded(comp v1beta1.ComponentType) (recorded, elapsed bool) {
	for _, g := range st.groupEntries() {
		if g.MemberFinished != nil && g.MemberFinished.Component == comp {
			return true, time.Since(g.MemberFinished.At.Time) > time.Hour
		}
	}
	return false, false
}

// key hashes what decides the future: the rows, the published revisions,
// whether the projection trails the spec and the status the rows, the partitions, the
// counts, the retry record, the readiness clock, the pause, the run and the
// group entries, and the budgets left.
func (st *seqState) key() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d|%d%d%d|%t|", st.rev, st.pushes, st.rollbacks, st.pauses, st.paused)
	for i, m := range st.members {
		for _, r := range m.rows {
			b.WriteByte(byte('a' + r.kind))
			b.WriteByte(byte('0' + r.rev))
			b.WriteByte(byte('0' + r.to))
			if r.scaled {
				b.WriteByte('+')
			}
		}
		projected, written := m.annotation == st.gen, m.statusGen == m.irGen && slices.Equal(m.published, m.rows)
		fmt.Fprintf(&b, "|%d%d%d|%d%d%d%d|%t%t|%d/%d/%d|%d/%d|%s%t|%d%d%d%d%d;",
			m.retry, m.attempts, m.retryRev, m.target, m.statusTarget, m.current, m.clearedFrom, projected, written,
			m.userPartition, m.pacingPartition, m.replicas, st.desired[i], st.partitions[i], m.ready, m.waited,
			m.fails, m.losses, m.scales, m.partitionEdits, m.steps)
	}
	if run := st.activeRun(); run != nil {
		b.WriteString("run:")
		for _, t := range run.TargetRevisions {
			fmt.Fprintf(&b, "%s=%d,", t.Component, seqRevisionOfHash(t.Revision))
		}
	} else {
		fmt.Fprintf(&b, "norun:%s", st.lastRunOutcome())
	}
	for _, g := range st.groupEntries() {
		fmt.Fprintf(&b, "|%s:%s/%s/%s/%s", g.Name, g.Phase, g.CompositePhase, g.CurrentComponent, g.PreviousComponent)
		if g.MemberFinished != nil {
			_, elapsed := st.finishRecorded(g.MemberFinished.Component)
			fmt.Fprintf(&b, "/%s%t", g.MemberFinished.Component, elapsed)
		}
	}
	return b.String()
}

func (st *seqState) String() string {
	var parts []string
	for i, m := range st.members {
		parts = append(parts, fmt.Sprintf("member %d [%s] hold=%s", i, m, st.holds[i]))
	}
	run := "no run"
	if r := st.activeRun(); r != nil {
		targets := make([]string, 0, len(r.TargetRevisions))
		for _, t := range r.TargetRevisions {
			targets = append(targets, fmt.Sprintf("%s=%d", t.Component, seqRevisionOfHash(t.Revision)))
		}
		run = "run open (" + strings.Join(targets, ",") + ")"
	} else if o := st.lastRunOutcome(); o != "" {
		run = "last run " + string(o)
	}
	var groups []string
	for _, g := range st.groupEntries() {
		groups = append(groups, g.CompositePhase)
	}
	paused := ""
	if st.paused {
		paused = ", paused"
	}
	return fmt.Sprintf("spec version %d (%s; group %s%s): ", st.rev, run, strings.Join(groups, ","), paused) + strings.Join(parts, " | ")
}

func seqPath(st *seqState) string {
	parts := make([]string, 0, len(st.path))
	for _, mv := range st.path {
		parts = append(parts, mv.String())
	}
	return strings.Join(parts, " -> ")
}

// seqWorld builds the objects the gates read from a state and runs the
// service controller's pass and the dispatcher's admission over them.
type seqWorld struct {
	t   *testing.T
	cfg seqConfig
	ctx context.Context
	// gateCalls counts the questions put to the full gate stack.
	gateCalls int
	// runPasses counts the run layer's passes.
	runPasses int
	// settleOrder, when set, is the order the members' passes run in after
	// every move; the explorer alternates the order by depth instead.
	settleOrder []int
	// passing is the member whose own pass is running, whose replica then
	// carries the rows that pass has written so far; -1 between passes.
	passing int
	// answers, when set, is told every answer the full gate stack
	// gives a start, with the state and the member that asked.
	answers func(st *seqState, member int, allowed bool, gate v1beta1.RolloutHoldGate, reason string)
	// visit, when set, is told every settled state the explorer records.
	visit func(st *seqState)
}

func newSeqWorld(t *testing.T, cfg seqConfig) *seqWorld {
	t.Helper()
	return &seqWorld{t: t, cfg: cfg, ctx: context.Background(), passing: -1}
}

// service is the service as the operator wrote it: one single-Component
// blueGreen group per member in chain order, the wait on every group but
// the last, no status yet.
func (w *seqWorld) service() *v1beta1.InferenceService {
	groups := make([]v1beta1.RolloutGroup, 0, len(w.cfg.members))
	for i, m := range w.cfg.members {
		g := v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{m.comp}, BlueGreen: &v1beta1.GroupBlueGreen{}}
		// The wait is the one after a group, so the last group carries none.
		if w.cfg.afterReady > 0 && i < len(w.cfg.members)-1 {
			g.Soak = &metav1.Duration{Duration: w.cfg.afterReady}
		}
		groups = append(groups, g)
	}
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: seqServiceName, Namespace: seqNamespace, Generation: 1},
		Spec:       v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: groups}},
	}
}

// initial is the moment after the push of version 2: the service's
// generation moved, no replica has been projected yet, every Instance runs
// version 1 and serves, and no run is open.
func (w *seqWorld) initial() *seqState {
	st := &seqState{gen: 2, rev: 2, pushes: w.cfg.budgets.pushes, rollbacks: w.cfg.budgets.rollbacks, pauses: w.cfg.budgets.pauses, service: w.service()}
	st.service.Generation = 2
	for _, mc := range w.cfg.members {
		m := seqMember{
			rows: make([]seqRow, mc.desired), target: 1, statusTarget: 1, current: 1,
			irGen: 1, statusGen: 1, annotation: 1,
			userPartition: mc.partition, pacingPartition: -1, replicas: mc.desired,
			ready: metav1.ConditionTrue, readyTrue: true, waited: true,
			fails: w.cfg.budgets.fails, losses: w.cfg.budgets.losses, scales: w.cfg.budgets.scales,
			partitionEdits: w.cfg.budgets.partitionEdits, steps: w.cfg.budgets.steps,
		}
		for i := range m.rows {
			m.rows[i] = seqRow{kind: seqReady, rev: 1}
		}
		m.published = append([]seqRow(nil), m.rows...)
		st.members = append(st.members, m)
		st.desired = append(st.desired, mc.desired)
		st.partitions = append(st.partitions, mc.partition)
		st.holds = append(st.holds, nil)
	}
	return st
}

// v1beta1Rows is the member's rows as the status publishes them, with the
// pod counts of the member's Instance shape.
func (w *seqWorld) v1beta1Rows(i int, m *seqMember) []v1beta1.OMENativeInstanceStatus {
	rows := m.published
	if i == w.passing {
		rows = m.rows
	}
	return w.rowStatuses(i, rows)
}

// rowStatuses is the status rows the replica of member i publishes for rows.
func (w *seqWorld) rowStatuses(i int, rows []seqRow) []v1beta1.OMENativeInstanceStatus {
	mc := w.cfg.members[i]
	comp, pods := mc.comp, mc.pods
	attempt := func(step string, to int8) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: seqRevisionName(comp, to)}
	}
	create := func(to int8) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate, TargetRevision: seqRevisionName(comp, to)}
	}
	out := make([]v1beta1.OMENativeInstanceStatus, 0, len(rows))
	for idx, r := range rows {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(idx), RunningRevision: seqRevisionName(comp, r.rev)}
		switch r.kind {
		case seqReady:
			row.Phase, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, pods, pods, pods
		case seqShort:
			row.Phase, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, pods, pods-1, pods-1
		case seqLost:
			row.Phase = v1beta1.OMENativeInstancePending
		case seqRebuild:
			row.Phase, row.PodCount, row.Operation = v1beta1.OMENativeInstanceCreating, pods, create(r.to)
		case seqDrain:
			row.Phase, row.PodCount, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceUpdating, pods, seqRevisionName(comp, r.to), attempt(workloadtypes.UpdateStepDrain, r.to)
		case seqSurge:
			row.Phase, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceUpdating, seqRevisionName(comp, r.to), attempt(workloadtypes.UpdateStepSurge, r.to)
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 2*pods, pods, pods
		case seqFailedUnserving:
			row.Phase, row.TargetRevision = v1beta1.OMENativeInstanceFailed, seqRevisionName(comp, r.to)
		case seqFailedSurge:
			row.Phase, row.TargetRevision = v1beta1.OMENativeInstanceReady, seqRevisionName(comp, r.to)
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 2*pods, pods, pods
		case seqCreating:
			row.Phase, row.PodCount, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceCreating, pods, seqRevisionName(comp, r.to), create(r.to)
		}
		row.AvailablePodCount = row.ServingPodCount
		out = append(out, row)
	}
	return out
}

// retryBlocks is the member's retry record as the status publishes it.
func (w *seqWorld) retryBlocks(i int, m *seqMember) []v1beta1.RetryBlock {
	if m.retry == seqRetryNone {
		return nil
	}
	state := v1beta1.RetryBlockBackoff
	switch m.retry {
	case seqRetryRetrying:
		state = v1beta1.RetryBlockRetryInProgress
	case seqRetryHeld:
		state = v1beta1.RetryBlockHeld
	}
	return []v1beta1.RetryBlock{{TargetRevision: seqRevisionName(w.cfg.members[i].comp, m.retryRev), State: state, AttemptsStarted: m.attempts}}
}

// readyClock is the Ready condition's transition time for a member: long
// ago once the wait after it has passed, else now.
func (m *seqMember) readyClock() metav1.Time {
	if m.waited {
		return metav1.NewTime(time.Now().Add(-seqLongAgo))
	}
	return metav1.Now()
}

// publishedRows are the rows a member's replica carries now: the live rows
// while its own pass runs, else the rows its last status write published.
func (w *seqWorld) publishedRows(i int, m *seqMember) []seqRow {
	if i == w.passing {
		return m.rows
	}
	return m.published
}

// replica is the InferenceReplica a member publishes: the rows, the
// counters the publication derives from them (Instances serving, Instances
// on the update revision and Ready on it), the revisions and the Ready
// condition with its clock.
func (w *seqWorld) replica(i int, m *seqMember) *v1beta1.InferenceReplica {
	mc := w.cfg.members[i]
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: seqNamespace, Name: seqServiceName + "-" + string(mc.comp),
			Generation: m.irGen, Annotations: seqParentGeneration(m.annotation),
		},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: mc.comp,
			Replicas:  ptr.To(m.replicas),
			Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: mc.strategy, RollingUpdate: &v1beta1.RollingUpdate{}}},
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: m.statusGen,
			CurrentRevision:    seqRevisionName(mc.comp, m.current),
			UpdateRevision:     seqRevisionName(mc.comp, m.statusTarget),
			InstanceStatuses:   w.v1beta1Rows(i, m),
			RetryBlocks:        w.retryBlocks(i, m),
		},
	}
	if mc.surges() {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.Parse(mc.budget))
	} else {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(mc.budget))
	}
	if m.userPartition > 0 {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.Partition = ptr.To(m.userPartition)
	}
	if m.pacingPartition >= 0 {
		ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: ptr.To(m.pacingPartition)}
	}
	// The counters follow the publication's rules: an Instance serves when
	// its whole pod set is in rotation; it is on the update revision when its
	// row records that revision, whatever its phase.
	var serving, updated, updatedReady int32
	for _, r := range w.publishedRows(i, m) {
		if r.serving() {
			serving++
		}
		if r.rev > 0 && r.rev == m.statusTarget {
			updated++
			if r.kind == seqReady {
				updatedReady++
			}
		}
	}
	ir.Status.Replicas = int32(len(w.publishedRows(i, m)))
	ir.Status.ServingReplicas, ir.Status.ReadyReplicas, ir.Status.AvailableReplicas = serving, serving, serving
	ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = updated, updatedReady
	// The Ready condition: True once the member is converged or staged, and
	// its transition time is what the next member's wait is measured from.
	cond := metav1.Condition{Type: seqReadyCondition, Status: m.ready, Reason: "RolloutInProgress", LastTransitionTime: metav1.Now()}
	switch m.ready {
	case metav1.ConditionTrue:
		cond.Reason, cond.LastTransitionTime = "AllInstancesReady", m.readyClock()
	case metav1.ConditionFalse:
		cond.Reason = "ReplicaCountMismatch"
	}
	ir.Status.Conditions = []metav1.Condition{cond}
	return ir
}

// seqReader serves the replicas and the revisions of a state to the gates
// and the run layer straight from the model, the only read they make being
// a Get by name: the member whose pass runs sees the rows it has written so
// far, every other reader the rows its last status write published.
type seqReader struct {
	client.Reader
	w  *seqWorld
	st *seqState
}

func (r seqReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if key.Namespace == seqNamespace {
		switch out := obj.(type) {
		case *v1beta1.InferenceReplica:
			for i, mc := range r.w.cfg.members {
				if key.Name == seqServiceName+"-"+string(mc.comp) {
					r.w.replica(i, &r.st.members[i]).DeepCopyInto(out)
					return nil
				}
			}
		case *appsv1.ControllerRevision:
			for _, mc := range r.w.cfg.members {
				for rev := int8(1); rev <= seqLastRevision; rev++ {
					if key.Name == seqRevisionName(mc.comp, rev) {
						seqRevisionObject(mc.comp, rev).DeepCopyInto(out)
						return nil
					}
				}
			}
		}
	}
	return apierrors.NewNotFound(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: fmt.Sprintf("%T", obj)}, key.Name)
}

// List answers a pod list with no pods, as a cache with none would: the
// unavailability gate lists the Component's pods on every drain-first ask,
// and the sequential world keeps none. Any other list is not served.
func (r seqReader) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	if pods, ok := list.(*corev1.PodList); ok {
		pods.Items = nil
		return nil
	}
	return errors.New("the sequential world serves single objects and pod lists only")
}

// seqClient is the run layer's client: it reads through seqReader and
// writes nothing, since the run layer writes only to consume a verb
// annotation the model never sets.
type seqClient struct {
	client.Client
	reads seqReader
}

func (c seqClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reads.Get(ctx, key, obj, opts...)
}

func (w *seqWorld) reader(st *seqState) seqReader {
	return seqReader{w: w, st: st}
}

// observe reads one member's published replica into the observation the
// group machine computes the chain's phase from, the way the service
// controller's observation reads it: the counters, the revisions, the Ready
// clock, a Failed row, and whether the rows rest at the staged shape.
func (w *seqWorld) observe(st *seqState, i int) coordination.ComponentObservation {
	mc := w.cfg.members[i]
	m := &st.members[i]
	ir := w.replica(i, m)
	summary := &ir.Status
	out := coordination.ComponentObservation{Component: mc.comp, Partition: irprojector.IRPartition(ir)}
	out.DesiredReplicas = summary.Replicas
	out.SpecReplicas = m.replicas
	if out.SpecReplicas <= 0 {
		out.SpecReplicas = 1
	}
	for _, row := range summary.InstanceStatuses {
		out.TotalPods += row.PodCount
		if row.Phase == v1beta1.OMENativeInstanceFailed {
			out.Failed = true
			out.FailureMessage = fmt.Sprintf("%s instance %d in Phase=Failed", mc.comp, row.Index)
		}
	}
	out.ReadyPods, out.ServingPods = summary.ReadyReplicas, summary.ServingReplicas
	out.NewRevisionPods, out.NewRevisionReadyPods = summary.UpdatedReplicas, summary.UpdatedReadyReplicas
	cur, tgt := query.RevisionFromName(summary.CurrentRevision), query.RevisionFromName(summary.UpdateRevision)
	out.CurrentRevisionHash, out.TargetRevisionHash = cur.Hash(), tgt.Hash()
	out.RolloutInFlight = !tgt.IsZero() && !tgt.Same(cur)
	for _, cond := range summary.Conditions {
		if cond.Type != seqReadyCondition {
			continue
		}
		if cond.Status == metav1.ConditionTrue {
			out.ReadyAt = cond.LastTransitionTime.Time
		}
		out.NotReady = cond.Status == metav1.ConditionFalse
	}
	out.AtDesiredShape = workloadstatus.ReachedDesiredShape(
		v1beta1convert.InstanceStatusSliceToWorkload(summary.InstanceStatuses), summary.UpdateRevision, out.Partition, summary.Replicas)
	out.UpdateStrategy = workloadtypes.UpdateStrategyRecreatePod
	if mc.surges() {
		out.UpdateStrategy = workloadtypes.UpdateStrategySurgeThenDrain
	}
	return out
}

// servicePass is the service controller's pass over the chain as published:
// the run layer opens, keeps, closes or replaces the run from the replicas'
// revisions, then the group machine computes the chain's phase and the
// finish record, which the status keeps between passes.
func (w *seqWorld) servicePass(st *seqState) {
	svc := st.service
	svc.Generation = st.gen
	if st.paused {
		svc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}
	} else {
		svc.Annotations = nil
	}
	reads := w.reader(st)
	w.runPasses++
	if _, err := rolloutrun.Reconcile(w.ctx, rolloutrun.Inputs{Client: seqClient{reads: reads}, Reader: reads, ISVC: svc, Now: time.Now()}); err != nil {
		w.t.Fatalf("run layer: %v (%s)", err, st)
	}
	groups := coordination.ResolveGroups(rollout.Effective(svc, rollout.Policies{}), coordination.GroupDefaults{})
	now := time.Now()
	var fresh []v1beta1.RolloutCoordinationGroupStatus
	for _, g := range groups {
		obs := coordination.GroupObservation{Group: g, Components: map[v1beta1.ComponentType]coordination.ComponentObservation{}, PausedGlobal: st.paused, Now: now}
		for i, mc := range w.cfg.members {
			for _, comp := range g.Components {
				if comp == mc.comp {
					obs.Components[comp] = w.observe(st, i)
				}
			}
		}
		for _, prev := range st.groupEntries() {
			if prev.Name == g.Name && prev.MemberFinished != nil {
				obs.MemberFinished = coordination.MemberFinish{Component: prev.MemberFinished.Component, At: prev.MemberFinished.At.Time}
			}
		}
		fresh = append(fresh, coordination.BuildGroupStatus(g, coordination.ComputeTransition(obs), nil, now))
	}
	// The merge keeps a group's transition time while its phase stands and
	// drops the entries of groups the plan does not declare.
	merged := svc.Status.RolloutCoordination
	declared := map[string]bool{}
	for _, g := range fresh {
		declared[g.Name] = true
		merged = coordination.MergeGroupStatus(merged, g)
	}
	if merged != nil {
		kept := merged.Groups[:0]
		for _, g := range merged.Groups {
			if declared[g.Name] {
				kept = append(kept, g)
			}
		}
		merged.Groups = kept
	}
	svc.Status.RolloutCoordination = merged
}

// heldIndices is the set of Instances the partition holds, by the roll's own
// rule over the member's rows as the replica would publish them.
func (w *seqWorld) heldIndices(i int, m *seqMember) map[int]bool {
	held := map[int]bool{}
	p := m.partition()
	if p <= 0 {
		return held
	}
	insts := v1beta1convert.InstanceStatusSliceToWorkload(w.rowStatuses(i, m.rows))
	observed := make([]*workloadtypes.InstanceStatus, len(insts))
	for k := range insts {
		observed[k] = &insts[k]
	}
	for idx := range query.PartitionHeldIndices(ptr.To(p), observed, seqRevisionName(w.cfg.members[i].comp, m.target)) {
		held[int(idx)] = true
	}
	return held
}

// finishedFor reports whether a member has nothing left to roll toward rev:
// no attempt open and every Instance on rev, except those a partition holds.
// An Instance a scale-up is still creating is new capacity, not the roll's
// work, whatever revision it builds toward.
func (w *seqWorld) finishedFor(m *seqMember, rev int8) bool {
	var off int32
	for _, r := range m.rows {
		if r.open() {
			return false
		}
		if r.kind == seqCreating {
			continue
		}
		if r.rev != rev {
			off++
		}
	}
	return off == 0 || (m.partition() > 0 && off <= m.partition())
}

// membersComplete reports the chain at its end by the replicas' reading:
// every member projected, its status written, every Instance Ready on the
// spec's revision or held by the member's partition, between one and the
// partition of them.
func (w *seqWorld) membersComplete(st *seqState) bool {
	for i := range st.members {
		m := &st.members[i]
		if m.annotation != st.gen || m.statusGen != m.irGen || m.target != st.rev {
			return false
		}
		var off int32
		for _, r := range m.rows {
			if r.kind != seqReady {
				return false
			}
			if r.rev != st.rev {
				off++
			}
		}
		if off != 0 && !(m.partition() > 0 && off <= m.partition()) {
			return false
		}
	}
	return true
}

// complete reports the end of the roll: the chain at its end by the
// replicas' reading and the run closed, so the service rests with no run
// open and the next push opens a fresh one.
func (w *seqWorld) complete(st *seqState) bool {
	return w.membersComplete(st) && st.activeRun() == nil
}

// restsAtPartition reports a member whose partition keeps Instances on
// another revision: between one and the partition of them.
func (w *seqWorld) restsAtPartition(m *seqMember, rev int8) bool {
	if m.partition() <= 0 {
		return false
	}
	var off int32
	for _, r := range m.rows {
		if r.rev != rev {
			off++
		}
	}
	return off >= 1 && off <= m.partition()
}

// writeStatus is the status write that ends a member's pass: the rows as
// they stand, the generation observed, the update revision, the current
// revision by the engine's own rule, and the Ready condition with its clock.
func (w *seqWorld) writeStatus(i int, m *seqMember) {
	comp := w.cfg.members[i].comp
	m.published = append([]seqRow(nil), m.rows...)
	m.statusGen = m.irGen
	m.statusTarget = m.target
	insts := v1beta1convert.InstanceStatusSliceToWorkload(w.v1beta1Rows(i, m))
	update := seqRevisionName(comp, m.target)
	var blocks []workloadtypes.RetryBlock
	if m.retry != seqRetryNone {
		blocks = []workloadtypes.RetryBlock{{TargetRevision: seqRevisionName(comp, m.retryRev)}}
	}
	pods := workloadstatus.PodRevisions{}
	for idx, r := range m.rows {
		switch r.kind {
		case seqReady, seqShort:
			pods[int32(idx)] = []query.RevisionID{query.RevisionFromName(seqRevisionName(comp, r.rev))}
		case seqSurge, seqFailedSurge:
			pods[int32(idx)] = []query.RevisionID{query.RevisionFromName(seqRevisionName(comp, r.rev)), query.RevisionFromName(seqRevisionName(comp, r.to))}
		case seqRebuild, seqDrain, seqCreating:
			// The pods a rebuild, a drain or a creation has made carry the
			// revision they build toward, whatever the row records.
			if r.to > 0 {
				pods[int32(idx)] = []query.RevisionID{query.RevisionFromName(seqRevisionName(comp, r.to))}
			}
		}
	}
	before, oldCurrent := seqRevisionName(comp, m.clearedFrom), seqRevisionName(comp, m.current)
	current := workloadstatus.CurrentRevisionFor(insts, oldCurrent, update, blocks, before, pods)
	m.clearedFrom = seqRevisionOf(workloadstatus.WithdrawnRevisionAfter(before, oldCurrent, current, update))
	m.current = seqRevisionOf(current)

	m.ready = w.readyCondition(i, m, insts)
	ready := m.ready == metav1.ConditionTrue
	switch {
	case ready && !m.readyTrue:
		m.waited = false
	case !ready && m.readyTrue:
		if m.waited {
			m.flips++
		}
		m.waited = false
	}
	m.readyTrue = ready
}

// readyCondition is the replica's Ready condition as the status writer
// derives it: False on a Failed Instance, True when staged at a partition,
// Unknown while the revisions differ, False with no Instance, True when
// every Instance is Ready or the serving count meets the member's own
// minimum, else False.
func (w *seqWorld) readyCondition(i int, m *seqMember, insts []workloadtypes.InstanceStatus) metav1.ConditionStatus {
	mc := w.cfg.members[i]
	for _, r := range m.rows {
		if r.kind == seqFailedUnserving {
			return metav1.ConditionFalse
		}
	}
	replicas := int32(len(m.rows))
	if p := m.partition(); p > 0 && workloadstatus.ReachedDesiredShape(insts, seqRevisionName(mc.comp, m.target), p, replicas) {
		return metav1.ConditionTrue
	}
	if m.current != m.target {
		return metav1.ConditionUnknown
	}
	if replicas == 0 {
		return metav1.ConditionFalse
	}
	var serving int32
	for _, r := range m.rows {
		if r.serving() {
			serving++
		}
	}
	if serving == replicas {
		return metav1.ConditionTrue
	}
	var maxUnavailable *intstr.IntOrString
	if !mc.surges() {
		maxUnavailable = ptr.To(intstr.Parse(mc.budget))
	}
	if serving >= utils.AvailabilityFloor(replicas, nil, nil, maxUnavailable) {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// waitOver reports whether the wait after the member before i has passed
// by the clock the gate reads: the recorded finish when the group's status
// carries one, else the member's Ready transition; and the member does not
// read not Ready.
func (w *seqWorld) waitOver(st *seqState, i int) bool {
	prev := &st.members[i-1]
	recorded, elapsed := st.finishRecorded(w.cfg.members[i-1].comp)
	if recorded {
		return elapsed && prev.ready != metav1.ConditionFalse
	}
	return prev.readyTrue && prev.waited
}

// waitPicture is what the gate read of the member before a start: the rows
// its status published, its recorded finish and the group's phase.
func (w *seqWorld) waitPicture(st *seqState, i int) string {
	m := &st.members[i]
	parts := make([]string, 0, len(m.published))
	for _, r := range m.published {
		parts = append(parts, r.String())
	}
	recorded, elapsed := st.finishRecorded(w.cfg.members[i].comp)
	return fmt.Sprintf("published %s, finish recorded=%t elapsed=%t, group %s", strings.Join(parts, ","), recorded, elapsed, w.groupPhases(st))
}

// rebuildLost is the Create pass's rebuild of every row short of its pods
// that the roll did not start: a row with no live pod left is rebuilt at the
// target, unless the partition holds it; a gang short of one member is
// rebuilt on the revision it runs. The rebuild asks no coordination gate.
func (w *seqWorld) rebuildLost(i int, m *seqMember) {
	held := w.heldIndices(i, m)
	for idx, r := range m.rows {
		switch r.kind {
		case seqLost:
			to := m.target
			if held[idx] {
				to = r.rev
			}
			m.rows[idx] = seqRow{kind: seqRebuild, rev: r.rev, to: to, scaled: r.scaled}
		case seqShort:
			m.rows[idx] = seqRow{kind: seqRebuild, rev: r.rev, to: r.rev, scaled: r.scaled}
		}
	}
}

// pass is one update pass of one member: the rows follow the count the
// replica asks for, the retry record, then every fresh start candidate in
// selection order (Instances out of rotation first, then index order) not
// held by the partition, each against the per-Component budget and the
// gate, every admitted start written before the next question; a row that
// lost its pods is rebuilt by the Create pass; the status write ends the
// pass. Under the operator's pause no start and no creation is admitted,
// while rebuilds and a scale-down still run. The first denial is the
// member's hold.
func (w *seqWorld) pass(st *seqState, i int) (admitted int, hold *seqHold) {
	mc := w.cfg.members[i]
	m := &st.members[i]
	w.passing = i
	defer func() { w.passing = -1 }()
	defer w.writeStatus(i, m)
	defer w.rebuildLost(i, m)
	// A retry record of a revision the spec does not ask for is removed.
	if m.retry != seqRetryNone && m.retryRev != m.target {
		m.retry, m.attempts = seqRetryNone, 0
	}
	// The Create and scale-down passes: new Instances build toward the
	// target; a scale-down retires the highest indices.
	for int32(len(m.rows)) < m.replicas && !st.paused {
		m.rows = append(m.rows, seqRow{kind: seqCreating, to: m.target, scaled: true})
	}
	if int32(len(m.rows)) > m.replicas {
		m.rows = m.rows[:m.replicas]
	}
	if st.paused {
		return 0, &seqHold{reason: "paused by the operator"}
	}
	switch {
	case m.retry == seqRetryHeld:
		return 0, &seqHold{gate: v1beta1.RolloutHoldGateHeld, reason: fmt.Sprintf("update held after %d failed attempt(s)", m.attempts)}
	case m.retry == seqRetryBackoff:
		return 0, &seqHold{gate: v1beta1.RolloutHoldGateRetryBlock, reason: fmt.Sprintf("update in backoff after %d failed attempt(s)", m.attempts)}
	case m.retry == seqRetryRetrying && m.inFlight():
		return 0, &seqHold{gate: v1beta1.RolloutHoldGateRetryBlock, reason: "retrying one attempt at a time"}
	}
	// A failed attempt toward a revision the spec has left settles on the
	// revision the Instance runs when that is the target: a Failed Instance
	// with no pods is the Create pass's to rebuild on the target it already
	// records, and a failed replacement beside a serving source on the
	// target is a leftover of a replaced revision.
	for idx, r := range m.rows {
		if r.rev != m.target {
			continue
		}
		switch r.kind {
		case seqFailedUnserving:
			m.rows[idx] = seqRow{kind: seqLost, rev: r.rev}
		case seqFailedSurge:
			m.rows[idx] = seqRow{kind: seqReady, rev: r.rev}
		}
	}
	held := w.heldIndices(i, m)
	// A Failed Instance with no pods that the partition holds is not
	// retried; the Create pass rebuilds it on the revision it ran. The
	// partition keeps a held Instance on the revision it serves and
	// nothing else: a failed replacement toward a revision the spec has
	// left is removed from under it and the row returns to Ready on the
	// revision it serves, while one toward the target stays, since its
	// retry is a start the partition refuses; the member then holds on
	// it with the Instance and the exit named.
	heldFailedToward := -1
	for idx, r := range m.rows {
		if !held[idx] {
			continue
		}
		switch {
		case r.kind == seqFailedUnserving:
			m.rows[idx] = seqRow{kind: seqLost, rev: r.rev}
		case r.kind == seqFailedSurge && r.to != m.target:
			m.rows[idx] = seqRow{kind: seqReady, rev: r.rev}
		case r.kind == seqFailedSurge && heldFailedToward < 0:
			heldFailedToward = idx
		}
	}
	defer func() {
		if hold == nil && admitted == 0 && heldFailedToward >= 0 && !m.inFlight() {
			hold = &seqHold{gate: v1beta1.RolloutHoldGateRetryBlock, reason: fmt.Sprintf("update to version %d waits on Instance %d, which the partition of %d holds with a failed replacement toward it; lower the partition to retry it, or push another revision to remove the failed pod", m.target, heldFailedToward, m.partition())}
		}
	}()
	// The roll's candidates: Instances off the target the partition does
	// not hold, a failed attempt first. An Instance that lost every pod is
	// the Create pass's to rebuild, not the roll's.
	var order []int
	for idx, r := range m.rows {
		if r.kind == seqFailedUnserving && r.rev != m.target && !held[idx] {
			order = append(order, idx)
		}
	}
	for idx, r := range m.rows {
		if (((r.kind == seqReady || r.kind == seqShort) && r.rev != m.target) || r.kind == seqFailedSurge) && !held[idx] {
			order = append(order, idx)
		}
	}
	type answer struct {
		kind seqRowKind
		rev  int8
	}
	denied := map[answer]bool{}
	var inFlightSurge, inFlightUnavail, gateUnavail int32
	reads := w.reader(st)
	own := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(mc.budget)), int32(len(m.rows)), true)
	target := seqRevisionObject(mc.comp, m.target)
	for _, idx := range order {
		r := m.rows[idx]
		if denied[answer{r.kind, r.rev}] {
			continue
		}
		// A surge needs a whole serving source: a gang short of a member
		// is recreated whatever the strategy says.
		mechanism := workloadtypes.UpdateStrategyRecreatePod
		if mc.surges() && (r.kind == seqReady || r.kind == seqFailedSurge) {
			mechanism = workloadtypes.UpdateStrategySurgeThenDrain
		}
		// A recreate of a Failed Instance that serves nothing skips the
		// gate: its outage is already in the gate's serving count.
		exempt := r.kind == seqFailedUnserving
		var h *seqHold
		if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
			prior := m.count(seqSurge, seqFailedSurge)
			if r.kind == seqFailedSurge {
				prior-- // the retry takes its own failed pod's slot
			}
			if prior+inFlightSurge+1 > own {
				h = &seqHold{gate: v1beta1.RolloutHoldGateBudget, reason: fmt.Sprintf("per-Component surge budget %d exhausted (would become %d)", own, prior+inFlightSurge+1)}
			}
		} else if prior := m.count(seqDrain); prior+inFlightUnavail+1 > own {
			h = &seqHold{gate: v1beta1.RolloutHoldGateBudget, reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)", own, prior+inFlightUnavail+1)}
		}
		gateReason := "no gate asked"
		if h == nil && !exempt {
			w.gateCalls++
			allowed, gate, reason := coordination.EvaluateUpdateGate(w.ctx, reads, st.service, mc.comp, seqRevisionName(mc.comp, r.rev), target, nil, coordination.GroupDefaults{}, mechanism, inFlightSurge, gateUnavail)
			if w.answers != nil {
				w.answers(st, i, allowed, gate, reason)
			}
			if !allowed {
				h = &seqHold{gate: gate, reason: reason}
			}
			gateReason = reason
		}
		if h != nil {
			denied[answer{r.kind, r.rev}] = true
			if hold == nil {
				hold = h
			}
			continue
		}
		orderClass := ""
		if !exempt {
			for j := 0; j < i; j++ {
				earlier := &st.members[j]
				if w.finishedFor(earlier, m.target) {
					continue
				}
				class := "order"
				switch {
				case m.target != st.rev:
					// The member's own projection trails the spec: it starts a
					// revision the operator has since replaced.
					class = "known-stale-projection-starts-a-retracted-revision"
				case earlier.statusTarget != m.target && earlier.current != earlier.statusTarget && w.stagedOrDone(earlier, earlier.statusTarget):
					// A member whose status still records the revision before
					// the push is not done for the push: the later member waits
					// until the status records it.
					class = "member-at-its-old-shape-read-as-done-after-a-push"
				}
				if orderClass == "" {
					orderClass = class
				}
				st.breaks = append(st.breaks, seqBreak{class: class, message: fmt.Sprintf("%s started Instance %d toward version %d while %s was not finished with it (%s)",
					mc.comp, idx, m.target, w.cfg.members[j].comp, *earlier)})
			}
		}
		if !exempt && i > 0 && w.cfg.afterReady > 0 && !w.waitOver(st, i) && !strings.HasPrefix(orderClass, "known-") {
			// The wait after readiness: the member before this one must not
			// read not Ready, and the wait since its recorded finish must have
			// passed. A start admitted under a stale status is that class.
			prev := &st.members[i-1]
			recorded, _ := st.finishRecorded(w.cfg.members[i-1].comp)
			class := "wait-skipped"
			switch {
			case !recorded && prev.ready != metav1.ConditionTrue:
				class = "known-wait-skipped-until-the-finish-is-recorded"
			case m.statusTarget != m.target && m.current != m.statusTarget && w.stagedOrDone(m, m.statusTarget):
				// The member's own status still records the revision before
				// the push, at whose staged shape its Instances sit: the member
				// is not done for the push, so a start here skipped the wait.
				class = "member-at-its-old-shape-read-as-done-after-a-push"
			case !prev.readyTrue:
				class = "wait-skipped-while-the-earlier-member-is-not-ready"
			case prev.partition() > 0 && prev.current != prev.statusTarget:
				class = "wait-skipped-after-a-staged-member"
			}
			st.breaks = append(st.breaks, seqBreak{class: class, message: fmt.Sprintf("%s started Instance %d toward version %d without the wait after %s (%s; %s; the gate said %q)",
				mc.comp, idx, m.target, w.cfg.members[i-1].comp, *prev, w.waitPicture(st, i-1), gateReason)})
		}
		if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
			m.rows[idx] = seqRow{kind: seqSurge, rev: r.rev, to: m.target}
			inFlightSurge++
		} else {
			m.rows[idx] = seqRow{kind: seqDrain, rev: r.rev, to: m.target}
			inFlightUnavail++
			if !exempt {
				gateUnavail++
			}
		}
		if m.retry == seqRetryDue {
			m.retry = seqRetryRetrying
		}
		admitted++
	}
	return admitted, hold
}

// settle runs the service controller's pass and then the members' passes
// in the given order until a whole round admits nothing and writes nothing
// new.
func (w *seqWorld) settle(st *seqState, order []int) {
	for round := 0; round < 32; round++ {
		before := st.key()
		w.servicePass(st)
		total := 0
		for _, i := range order {
			a, h := w.pass(st, i)
			st.holds[i] = h
			total += a
		}
		if total == 0 && st.key() == before {
			return
		}
	}
	w.t.Fatalf("admission did not settle in 32 rounds: %s", st)
}

func (w *seqWorld) order(depth int) []int {
	if w.settleOrder != nil {
		return append([]int(nil), w.settleOrder...)
	}
	order := make([]int, len(w.cfg.members))
	for i := range order {
		order[i] = i
	}
	if depth%2 == 1 {
		// the later members first on odd depths
		for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
			order[i], order[j] = order[j], order[i]
		}
	}
	return order
}

// projectorOrder is the order the service controller projects the
// Components in, whatever order the chain declares: the engine, then the
// decoder, then the router.
func (w *seqWorld) projectorOrder() []int {
	rank := func(c v1beta1.ComponentType) int {
		switch c {
		case v1beta1.EngineComponent:
			return 0
		case v1beta1.DecoderComponent:
			return 1
		}
		return 2
	}
	order := make([]int, len(w.cfg.members))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return rank(w.cfg.members[order[a]].comp) < rank(w.cfg.members[order[b]].comp) })
	return order
}

// project is the projector reaching one member: the parent generation is
// recorded, and the spec's revision, count and user partition land on the
// replica, which moves its generation when any of them changed.
func (w *seqWorld) project(st *seqState, i int) {
	m := &st.members[i]
	m.annotation = st.gen
	changed := m.target != st.rev || m.replicas != st.desired[i] || m.userPartition != st.partitions[i]
	if m.target != st.rev {
		// Rows that run the new target but recorded a failed attempt toward
		// another revision settle on it: a failed replacement beside a
		// serving source is removed, a Failed Instance with no pods is
		// rebuilt on the revision it ran.
		for idx, r := range m.rows {
			if r.rev != st.rev {
				continue
			}
			switch r.kind {
			case seqFailedSurge:
				m.rows[idx] = seqRow{kind: seqReady, rev: r.rev}
			case seqFailedUnserving:
				m.rows[idx] = seqRow{kind: seqLost, rev: r.rev}
			}
		}
	}
	m.target, m.replicas, m.userPartition = st.rev, st.desired[i], st.partitions[i]
	if changed {
		m.irGen++
	}
}

// progressMoves are the moves that advance the roll by themselves: the
// projector reaches a member, a started or rebuilt Instance serves, a
// backoff ends, a retry record its rows outlived is removed, the wait after
// readiness passes.
// Under a pause only rebuilt Instances come up, and the operator's resume
// is the move that lets the rest continue.
func (w *seqWorld) progressMoves(st *seqState) []seqMove {
	var out []seqMove
	// The projector's pass writes every member in its order within one
	// tick; the replicas may reconcile between two of its writes or only
	// after all of them.
	for i := range st.members {
		if st.members[i].annotation != st.gen {
			out = append(out, seqMove{kind: "project"}, seqMove{kind: "project-between"})
			break
		}
	}
	if st.paused {
		out = append(out, seqMove{kind: "resume"})
	}
	for i := range st.members {
		m := &st.members[i]
		first, last := map[seqRowKind]int{}, map[seqRowKind]int{}
		for idx, r := range m.rows {
			if _, ok := first[r.kind]; !ok {
				first[r.kind] = idx
			}
			last[r.kind] = idx
		}
		kinds := []seqRowKind{seqDrain, seqSurge, seqCreating, seqRebuild}
		if st.paused {
			kinds = []seqRowKind{seqRebuild}
		}
		for _, kind := range kinds {
			idx, ok := first[kind]
			if !ok {
				continue
			}
			out = append(out, seqMove{kind: "serve", member: i, idx: idx})
			if last[kind] != idx {
				out = append(out, seqMove{kind: "serve", member: i, idx: last[kind]})
			}
		}
		switch {
		case m.retry == seqRetryBackoff:
			out = append(out, seqMove{kind: "backoff", member: i})
		case m.retry != seqRetryNone && m.retry != seqRetryHeld && !m.inFlight() && m.count(seqFailedUnserving, seqFailedSurge) == 0:
			// Every row outlived the failure the record counts: the record is
			// removed once the rows have stood Ready for the window.
			out = append(out, seqMove{kind: "prune", member: i})
		}
		if w.cfg.afterReady > 0 && w.waitRunning(st, i) {
			out = append(out, seqMove{kind: "wait", member: i})
		}
	}
	return out
}

// waitRunning reports a wait clock still running after a member: the
// group's recorded finish when it names the member, else the member's Ready
// transition.
func (w *seqWorld) waitRunning(st *seqState, i int) bool {
	m := &st.members[i]
	if recorded, elapsed := st.finishRecorded(w.cfg.members[i].comp); recorded {
		return !elapsed || (m.readyTrue && !m.waited)
	}
	return m.readyTrue && !m.waited
}

// adverseMoves are the moves that cost the roll something: an attempt
// fails, a pod, a gang or a gang member is lost, the retries are spent, or
// the operator acts.
func (w *seqWorld) adverseMoves(st *seqState) []seqMove {
	var out []seqMove
	for i := range st.members {
		m := &st.members[i]
		mc := w.cfg.members[i]
		seenFail, seenLoss := map[seqRowKind]bool{}, map[int8]bool{}
		for idx, r := range m.rows {
			switch r.kind {
			case seqDrain, seqSurge:
				if m.fails > 0 && !seenFail[r.kind] {
					seenFail[r.kind] = true
					out = append(out, seqMove{kind: "fail", member: i, idx: idx})
				}
			case seqReady:
				// One loss per revision the member runs: a pod of the old
				// version and a pod of the new one are different pictures. A
				// gang loses either one member or every pod.
				if m.losses > 0 && !seenLoss[r.rev] {
					seenLoss[r.rev] = true
					out = append(out, seqMove{kind: "lose", member: i, idx: idx})
					if mc.pods > 1 {
						out = append(out, seqMove{kind: "lose-member", member: i, idx: idx})
					}
				}
			}
		}
		if m.retry != seqRetryHeld && m.retry != seqRetryNone && m.count(seqFailedUnserving, seqFailedSurge) > 0 {
			out = append(out, seqMove{kind: "spent", member: i})
		}
		n := st.desired[i]
		// Under a scaling policy the follower's count is the policy's to
		// write, so only the other members are resized by hand.
		if m.scales > 0 && !(w.cfg.scalingPolicy && i == w.cfg.follower()) {
			for _, delta := range []int32{1, -1, n / 2, -(n / 2)} {
				if delta != 0 && n+delta >= 1 {
					out = append(out, seqMove{kind: "scale", member: i, value: n + delta})
				}
			}
		}
		if m.partitionEdits > 0 {
			values := []int32{n / 2, n - 1, n}
			if st.partitions[i] > 0 {
				values = append(values, 0, st.partitions[i]+1)
			}
			seen := map[int32]bool{st.partitions[i]: true}
			for _, v := range values {
				if v < 0 || seen[v] {
					continue
				}
				seen[v] = true
				out = append(out, seqMove{kind: "partition", member: i, value: v})
			}
		}
		if m.steps > 0 {
			// A canary step's partition: a step that holds half, the final
			// step that releases every Instance, and the step removed.
			for _, v := range []int32{n / 2, 0, -1} {
				if v != m.pacingPartition && (v >= 0 || m.pacingPartition >= 0) {
					out = append(out, seqMove{kind: "step", member: i, value: v})
				}
			}
		}
	}
	if st.pushes > 0 && st.rev < seqLastRevision {
		out = append(out, seqMove{kind: "push"})
	}
	if st.rollbacks > 0 && st.rev >= 2 {
		out = append(out, seqMove{kind: "rollback"})
	}
	if st.pauses > 0 && !st.paused {
		out = append(out, seqMove{kind: "pause"})
	}
	return out
}

// followerCount is the count a scaling policy writes on the follower for an
// anchor count: the shape's ratio applied to it, rounded up, at least one.
func (w *seqWorld) followerCount(anchor int32) int32 {
	a, f := w.cfg.members[w.cfg.anchor()].desired, w.cfg.members[w.cfg.follower()].desired
	n := (anchor*f + a - 1) / a
	if n < 1 {
		n = 1
	}
	return n
}

// ageFinish moves the recorded finish of a member back by the wait, the
// way the passage of time does: the next member's wait after it is over.
func (w *seqWorld) ageFinish(st *seqState, comp v1beta1.ComponentType) {
	if st.service.Status.RolloutCoordination == nil {
		return
	}
	for gi := range st.service.Status.RolloutCoordination.Groups {
		g := &st.service.Status.RolloutCoordination.Groups[gi]
		if g.MemberFinished != nil && g.MemberFinished.Component == comp {
			g.MemberFinished.At = metav1.NewTime(time.Now().Add(-seqLongAgo))
		}
	}
}

// apply makes one move and then lets the service controller and the
// dispatcher do what the gates allow.
func (w *seqWorld) apply(st *seqState, mv seqMove) *seqState {
	next := st.clone()
	next.path = append(next.path, mv)
	next.breaks = nil
	var m *seqMember
	switch mv.kind {
	case "push", "rollback", "project", "project-between", "pause", "resume":
	default:
		m = &next.members[mv.member]
	}
	switch mv.kind {
	case "project":
		for _, i := range w.projectorOrder() {
			w.project(next, i)
		}
	case "project-between":
		for _, i := range w.projectorOrder() {
			w.project(next, i)
			w.settle(next, w.order(len(next.path)))
		}
	case "serve":
		r := m.rows[mv.idx]
		m.rows[mv.idx] = seqRow{kind: seqReady, rev: r.to, scaled: r.scaled}
		// An Instance converging on a revision removes that revision's
		// retry record; a Held record stays until a new revision or a release.
		if m.retry != seqRetryNone && m.retry != seqRetryHeld && m.retryRev == r.to {
			m.retry, m.attempts = seqRetryNone, 0
		}
	case "fail":
		r := m.rows[mv.idx]
		if r.kind == seqSurge {
			m.rows[mv.idx].kind = seqFailedSurge
		} else {
			m.rows[mv.idx].kind = seqFailedUnserving
		}
		m.fails--
		// A failure counts once per wave: a record in backoff or Held is not
		// moved by a second failure of the same wave.
		if m.retry != seqRetryBackoff && m.retry != seqRetryHeld {
			m.attempts++
			m.retryRev = r.to
			if m.attempts >= w.cfg.maxAttempts {
				m.retry = seqRetryHeld
			} else {
				m.retry = seqRetryBackoff
			}
		}
	case "lose":
		m.rows[mv.idx] = seqRow{kind: seqLost, rev: m.rows[mv.idx].rev, scaled: m.rows[mv.idx].scaled}
		m.losses--
	case "lose-member":
		m.rows[mv.idx] = seqRow{kind: seqShort, rev: m.rows[mv.idx].rev, scaled: m.rows[mv.idx].scaled}
		m.losses--
	case "backoff":
		m.retry = seqRetryDue
	case "prune":
		m.retry, m.attempts = seqRetryNone, 0
	case "spent":
		m.retry = seqRetryHeld
	case "wait":
		m.waited = true
		w.ageFinish(next, w.cfg.members[mv.member].comp)
	case "scale":
		next.desired[mv.member] = mv.value
		if w.cfg.scalingPolicy && mv.member == w.cfg.anchor() && w.cfg.follower() >= 0 {
			next.desired[w.cfg.follower()] = w.followerCount(mv.value)
		}
		next.gen++
		m.scales--
	case "partition":
		next.partitions[mv.member] = mv.value
		next.gen++
		m.partitionEdits--
	case "step":
		m.pacingPartition = mv.value
		m.irGen++
		m.steps--
	case "push":
		next.rev++
		next.gen++
		next.pushes--
	case "rollback":
		next.rev = 1
		next.gen++
		next.rollbacks--
	case "pause":
		next.paused = true
		next.pauses--
	case "resume":
		next.paused = false
	}
	w.settle(next, w.order(len(next.path)))
	return next
}

// seqFinding is one invariant broken, or one known reading met, at one state.
type seqFinding struct {
	class   string
	message string
	state   *seqState
}

// seqKnownClasses are the readings the explorer reports without failing,
// each in words: the engine's designed behaviour, the behaviour not decided
// yet, and the known gaps the tests below record as the current behaviour.
// A fix removes its line here; the class then fails the test if it comes
// back.
var seqKnownClasses = map[string]string{
	"known-run-stays-open-while-a-member-rests-at-its-partition":           "the chain reached its end with a member resting at its partition (between one and the partition of its Instances on another version), the group reads Idle, and the run stays open for good: the run layer closes a run on an unconverged member only through a Staged group phase, which a Sequential chain never computes, and the staged member diverges no more, so no new run opens either (a gap)",
	"known-failed-replacement-toward-the-target-waits-under-the-partition": "a surge replacement that failed toward the roll target stands under an Instance the partition holds, and its retry is a start on that Instance, which the partition refuses; the member holds with the Instance and the exits named, lowering the partition or pushing another revision, which removes the pod, until the operator acts (by design: the partition keeps the Instance on the revision it serves, and the failed attempt is the roll's own open work)",
	"known-wait-skipped-until-the-finish-is-recorded":                      "the next member's first start is admitted before the service controller records the previous member's finish, when that member's Ready condition is not True at the moment its roll reads done (Unknown while a retry record keeps its current revision behind, False while it is under its serving minimum): the gate reads no clock, applies neither the wait nor the readiness rule, and admits the start; the finish is recorded on the next service pass and every later start waits (a gap)",
	"known-held-record-outlives-the-retired-instance":                      "an Instance whose retries are spent is retired by a scale-down, and its Held retry record stays: the member's every Instance runs the new version, its current revision never moves, it reads \"update held after N failed attempt(s)\" for a failure no Instance carries, the chain moves on from its shape, and the run never closes until a new revision or a release of the held revision; under a partition no Instance is held, so the member never reads at its staged shape either, the chain never moves on from it, and the later members wait on it for good (a gap)",
	"known-spent-retries-hold-the-chain":                                   "an earlier Component whose retries are spent with Instances left on the old version keeps every later Component waiting under \"Sequential waiting on <c>\"; the hold names the Component, not its spent retries (the Sequential failure hold)",
	"known-retries-spent":                                                  "the only unfinished Component's retries are spent and nothing waits on it; a new revision or a release of the held revision ends it",
	"known-stale-projection-starts-a-retracted-revision":                   "a member still projected for a push the operator has since replaced starts that revision in the pass before the projector rewrites its spec, when the earlier member has nothing to roll for the new spec; the gate reads the stale projection as the member's own turn (left open)",
	"known-failed-replacement-under-a-held-instance-keeps-its-slot":        "a surge replacement that failed beside its serving source keeps its surge slot until a start on that Instance replaces it; once a partition holds that Instance no start comes, and under a surge budget of one the member's movable Instances never start (left open; the budget side, not the Sequential gate)",
	"known-budget-after-a-scale":                                           "an attempt that started before a scale-down counts against the smaller budget until it ends (by design)",
}

// check reads every invariant at one settled state.
func (w *seqWorld) check(st *seqState) []seqFinding {
	var out []seqFinding
	add := func(class, format string, args ...any) {
		out = append(out, seqFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	for _, b := range st.breaks {
		add(b.class, "%s", b.message)
	}
	pacing := w.groupPacing()
	for i, mc := range w.cfg.members {
		m := st.members[i]
		n := int32(len(m.rows))
		own := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(mc.budget)), n, true)
		// An attempt that started before a scale-down counts against the
		// smaller budget until it ends; that excess is reported on its own.
		budgetClass := "budget"
		if m.scales < w.cfg.budgets.scales || (w.cfg.scalingPolicy && st.members[w.cfg.anchor()].scales < w.cfg.budgets.scales) {
			budgetClass = "known-budget-after-a-scale"
		}
		if mc.surges() {
			open := m.count(seqSurge)
			if open+m.count(seqFailedSurge) > own {
				add(budgetClass, "%s has %d surges in flight beside %d failed surge pods against its own budget of %d", mc.comp, open, m.count(seqFailedSurge), own)
			}
			if group := coordination.MaxSurgeBudget(pacing, n); open > group {
				add(budgetClass, "%s has %d surges in flight against the group budget of %d", mc.comp, open, group)
			}
		} else {
			open := m.count(seqDrain)
			if open > own {
				add(budgetClass, "%s has %d Instances out of rotation against its own budget of %d", mc.comp, open, own)
			}
			if group := coordination.MaxUnavailableBudget(pacing, n); open+m.count(seqFailedUnserving) > group {
				add(budgetClass, "%s has %d Instances out of rotation beside %d failed against the group unavailability budget of %d", mc.comp, open, m.count(seqFailedUnserving), group)
			}
		}
	}
	if !w.complete(st) && len(w.progressMoves(st)) == 0 {
		class, message := w.classifyStopped(st)
		add(class, "%s", message)
	}
	return out
}

// groupPacing is the chain's pacing after the operator defaults, read off
// the resolved group the way the gates read it.
func (w *seqWorld) groupPacing() v1beta1.CoordinationPacing {
	groups := coordination.ResolveGroups(w.service().Spec.Rollout, coordination.GroupDefaults{})
	if len(groups) == 0 {
		w.t.Fatalf("the chain resolves to no group")
	}
	return groups[0].Pacing
}

// classifyStopped names why a state that cannot move is what it is. A chain
// at its end by the replicas' reading with its run still open is the Held
// record of a retired Instance, the known gap of a member resting at its
// partition, or a finding. Else the first unfinished member in chain order
// decides: its retries are spent ("known-spent-retries-hold-the-chain" when
// a later member waits on it, "known-retries-spent" when none does); a
// failed surge replacement under an Instance its partition holds keeps the
// surge slot its movable Instances need
// ("known-failed-replacement-under-a-held-instance-keeps-its-slot"), or
// stands toward the target under a held Instance the partition keeps the
// retry off, so the member holds with the exit named
// ("known-failed-replacement-toward-the-target-waits-under-the-partition");
// the plan gate holds it while no run is open ("plan-gate-holds-with-no-run");
// or nothing explains it ("no-way-out").
func (w *seqWorld) classifyStopped(st *seqState) (class, message string) {
	if w.membersComplete(st) {
		run := st.activeRun()
		if run == nil {
			return "no-way-out", "the chain is at its end with no run open and still reads incomplete: " + st.String()
		}
		for i := range st.members {
			m := &st.members[i]
			if m.retry == seqRetryHeld && m.current != m.target {
				return "known-held-record-outlives-the-retired-instance", fmt.Sprintf("%s runs version %d on every Instance under a Held record (%s) and the run %s stays open with the group reading %s", w.cfg.members[i].comp, st.rev, st.holds[i], run.RunID, w.groupPhases(st))
			}
		}
		for i := range st.members {
			if w.restsAtPartition(&st.members[i], st.rev) {
				return "known-run-stays-open-while-a-member-rests-at-its-partition", fmt.Sprintf("%s rests at its partition and the run %s stays open with the group reading %s", w.cfg.members[i].comp, run.RunID, w.groupPhases(st))
			}
		}
		return "run-open-after-the-chain-ended", fmt.Sprintf("every member rests on version %d and the run %s stays open with the group reading %s", st.rev, run.RunID, w.groupPhases(st))
	}
	var lines []string
	for i, mc := range w.cfg.members {
		m := &st.members[i]
		if m.retry == seqRetryHeld && m.current != m.target && m.partition() > 0 && w.everyInstanceReadyOn(m, st.rev) {
			// The Held record keeps the current revision behind while every
			// Instance runs the target; with none held the staged-shape
			// reading never says done, so the gate keeps the member active.
			lines = append(lines, fmt.Sprintf("%s runs version %d on every Instance under a Held record and a partition of %d; no Instance is held, so it never reads at its staged shape and the chain never moves on from it", mc.comp, st.rev, m.partition()))
			if class == "" {
				class = "known-held-record-outlives-the-retired-instance"
			}
			continue
		}
		if w.finishedFor(m, st.rev) && m.count(seqLost, seqShort, seqRebuild, seqCreating) == 0 && w.stagedOrDone(m, st.rev) {
			continue
		}
		var line string
		switch {
		case m.retry == seqRetryHeld:
			line = fmt.Sprintf("%s is Held with %s", mc.comp, m)
			if class == "" {
				class = "known-retries-spent"
			}
		case w.heldFailedReplacementStopsIt(st, i):
			line = fmt.Sprintf("%s (%s) holds on %s with a failed replacement under a held Instance", mc.comp, m, st.holds[i])
			if class == "" {
				class = "known-failed-replacement-under-a-held-instance-keeps-its-slot"
			}
		case w.heldFailedReplacementTowardTheTargetWaits(st, i):
			line = fmt.Sprintf("%s (%s) holds on %s: the partition keeps the retry off the Instance", mc.comp, m, st.holds[i])
			if class == "" {
				class = "known-failed-replacement-toward-the-target-waits-under-the-partition"
			}
		case st.holds[i] != nil && st.holds[i].gate == v1beta1.RolloutHoldGatePlan && st.activeRun() == nil:
			line = fmt.Sprintf("%s (%s) holds on %s while the run layer opens nothing", mc.comp, m, st.holds[i])
			if class == "" {
				class = "plan-gate-holds-with-no-run"
			}
		default:
			line = fmt.Sprintf("%s (%s) holds on %s", mc.comp, m, st.holds[i])
			switch class {
			case "":
				class = "no-way-out"
			case "known-retries-spent":
				class = "known-spent-retries-hold-the-chain"
			}
		}
		lines = append(lines, line)
	}
	if class == "" {
		class = "no-way-out"
	}
	return class, strings.Join(lines, "; ") + " (" + w.groupPhases(st) + ")"
}

// groupPhases is the chain's group entries in words.
func (w *seqWorld) groupPhases(st *seqState) string {
	var parts []string
	for _, g := range st.groupEntries() {
		parts = append(parts, g.CompositePhase)
	}
	if len(parts) == 0 {
		return "no group entry"
	}
	return strings.Join(parts, ",")
}

// heldFailedReplacementTowardTheTargetWaits reports a member that holds on
// a held Instance whose failed replacement toward the target stands: the
// retry is a start on that Instance, which the partition refuses, so the
// hold names the Instance and the exits and waits for the operator.
func (w *seqWorld) heldFailedReplacementTowardTheTargetWaits(st *seqState, i int) bool {
	m := &st.members[i]
	h := st.holds[i]
	if h == nil || h.gate != v1beta1.RolloutHoldGateRetryBlock || !strings.Contains(h.reason, "lower the partition") || m.inFlight() {
		return false
	}
	held := w.heldIndices(i, m)
	for idx, r := range m.rows {
		if r.kind == seqFailedSurge && r.to == m.target && held[idx] {
			return true
		}
	}
	return false
}

// heldFailedReplacementStopsIt reports a member whose next start the
// per-Component surge budget denies while a failed surge replacement sits
// under an Instance the partition holds, so no start ever frees its slot.
func (w *seqWorld) heldFailedReplacementStopsIt(st *seqState, i int) bool {
	m := &st.members[i]
	h := st.holds[i]
	if h == nil || h.gate != v1beta1.RolloutHoldGateBudget || !strings.Contains(h.reason, "surge budget") || m.inFlight() {
		return false
	}
	held := w.heldIndices(i, m)
	for idx, r := range m.rows {
		if r.kind == seqFailedSurge && held[idx] {
			return true
		}
	}
	return false
}

// everyInstanceReadyOn reports every Instance of a member Ready on rev.
func (w *seqWorld) everyInstanceReadyOn(m *seqMember, rev int8) bool {
	for _, r := range m.rows {
		if r.kind != seqReady || r.rev != rev {
			return false
		}
	}
	return true
}

// stagedOrDone reports a member at its end: every Instance Ready on rev, or
// between one and its partition held Ready on another revision, since a
// partition holds what still runs the old revision, at most what it asks
// for.
func (w *seqWorld) stagedOrDone(m *seqMember, rev int8) bool {
	var off int32
	for _, r := range m.rows {
		if r.kind != seqReady {
			return false
		}
		if r.rev != rev {
			off++
		}
	}
	return off == 0 || (m.partition() > 0 && off <= m.partition())
}

// seqReport is what one configuration's exploration found.
type seqReport struct {
	states, depth, runs int
	findings            map[string]seqFinding
	counts              map[string]int
}

func (w *seqWorld) record(rep *seqReport, st *seqState) {
	if w.visit != nil {
		w.visit(st)
	}
	for _, f := range w.check(st) {
		rep.counts[f.class]++
		if _, ok := rep.findings[f.class]; !ok {
			rep.findings[f.class] = f
		}
	}
}

func (w *seqWorld) explore(maxStates, maxDepth int) seqReport {
	rep := seqReport{findings: map[string]seqFinding{}, counts: map[string]int{}}
	start := w.initial()
	w.settle(start, w.order(0))
	visited := map[string]bool{start.key(): true}
	queue := []*seqState{start}
	for len(queue) > 0 && rep.states < maxStates {
		st := queue[0]
		queue = queue[1:]
		rep.states++
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
		w.record(&rep, st)
		if w.complete(st) || len(st.path) >= maxDepth {
			if len(st.path) >= maxDepth && !w.complete(st) {
				rep.counts["depth"]++
				if _, ok := rep.findings["depth"]; !ok {
					rep.findings["depth"] = seqFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
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
	return rep
}

// randomRuns plays whole runs with a fixed seed, favouring the moves that
// finish a roll, to reach the depths the breadth-first walk does not.
func (w *seqWorld) randomRuns(rep *seqReport, seed int64, runs, maxDepth int) {
	rng := rand.New(rand.NewSource(seed))
	for r := 0; r < runs; r++ {
		st := w.initial()
		w.settle(st, w.order(0))
		rep.runs++
		for depth := 0; ; depth++ {
			w.record(rep, st)
			if w.complete(st) {
				break
			}
			progress, adverse := w.progressMoves(st), w.adverseMoves(st)
			if len(progress) == 0 && len(adverse) == 0 {
				break
			}
			if depth >= maxDepth {
				rep.counts["depth"]++
				if _, ok := rep.findings["depth"]; !ok {
					rep.findings["depth"] = seqFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
				}
				break
			}
			var mv seqMove
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

// seqChain builds a configuration: the members in chain order, each with
// the given strategy and budget, single pods unless pods says otherwise.
func seqChain(shape []int32, comps []v1beta1.ComponentType, strategy v1beta1.UpdateStrategyType, budget string, partitions []int32, wait time.Duration, retries int32, budgets seqBudgets) seqConfig {
	cfg := seqConfig{afterReady: wait, maxAttempts: retries, budgets: budgets}
	for i, comp := range comps {
		var p int32
		if i < len(partitions) {
			p = partitions[i]
		}
		cfg.members = append(cfg.members, seqMemberConfig{comp: comp, desired: shape[i], pods: 1, strategy: strategy, budget: budget, partition: p})
	}
	cfg.name = cfg.String()
	return cfg
}

// withPods gives each member its gang width, in chain order.
func (c seqConfig) withPods(pods ...int32) seqConfig {
	for i := range c.members {
		if i < len(pods) && pods[i] > 0 {
			c.members[i].pods = pods[i]
		}
	}
	c.name = c.String()
	return c
}

// withScalingPolicy makes the decoder follow the engine's count.
func (c seqConfig) withScalingPolicy() seqConfig {
	c.scalingPolicy = true
	c.name = c.String()
	return c
}

func seqDefaultBudgets(full bool) seqBudgets {
	b := seqBudgets{fails: 1, losses: 1, scales: 1, partitionEdits: 1, steps: 1, pushes: 1, rollbacks: 1, pauses: 1}
	if full {
		b.fails, b.losses = 2, 2
	}
	return b
}

// seqShapes are the reference prefill:decode shapes beside two small ones
// that reach deeper within the bounds.
var seqShapes = [][2]int32{{12, 8}, {12, 4}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {6, 2}, {4, 2}}

// seqConfigs lists the configurations to explore: the whole matrix of
// strategy, partition placement and wait on two reference shapes and a
// small one, every other reference shape under both strategies with a
// partition and a wait and with a scaling policy, gangs of two, four and
// eight pods, a router of two behind the chain, an engine then a router;
// then a seeded sample over the whole space. The full mode runs the whole
// matrix on every shape and a larger sample.
func seqConfigs(full bool) []seqConfig {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	strategies := []v1beta1.UpdateStrategyType{recreate, surge}
	budgets := seqDefaultBudgets(full)
	pair := []v1beta1.ComponentType{eng, dec}
	var out []seqConfig
	matrix := map[[2]int32]bool{{12, 8}: true, {12, 4}: true, {6, 2}: true}
	partitionSets := [][]int32{nil, {2, 0}, {0, 2}, {2, 2}}
	for _, shape := range seqShapes {
		for _, strategy := range strategies {
			if full || matrix[shape] {
				for _, partitions := range partitionSets {
					for _, wait := range []time.Duration{0, 10 * time.Minute} {
						out = append(out, seqChain(shape[:], pair, strategy, "10%", partitions, wait, 3, budgets))
					}
				}
				out = append(out, seqChain([]int32{shape[0], shape[1], 2}, []v1beta1.ComponentType{eng, dec, rtr}, strategy, "10%", []int32{2, 0, 0}, 10*time.Minute, 3, budgets))
				out = append(out, seqChain(shape[:], pair, strategy, "10%", nil, 10*time.Minute, 3, budgets).withScalingPolicy())
				continue
			}
			out = append(out, seqChain(shape[:], pair, strategy, "10%", []int32{2, 0}, 10*time.Minute, 3, budgets))
			out = append(out, seqChain(shape[:], pair, strategy, "10%", nil, 0, 3, budgets).withScalingPolicy())
		}
	}
	// Gangs: the engine of four pods beside a decoder of two, both of eight,
	// and a small chain of pairs.
	for _, strategy := range strategies {
		out = append(out, seqChain([]int32{12, 8}, pair, strategy, "10%", []int32{2, 0}, 10*time.Minute, 3, budgets).withPods(4, 2))
		out = append(out, seqChain([]int32{16, 8}, pair, strategy, "10%", nil, 10*time.Minute, 3, budgets).withPods(8, 8))
		out = append(out, seqChain([]int32{4, 2}, pair, strategy, "1", []int32{1, 0}, 0, 3, budgets).withPods(2, 2))
		if full {
			out = append(out, seqChain([]int32{24, 8}, pair, strategy, "10%", []int32{0, 2}, 10*time.Minute, 3, budgets).withPods(8, 4))
			out = append(out, seqChain([]int32{12, 8, 2}, []v1beta1.ComponentType{eng, dec, rtr}, strategy, "10%", nil, 10*time.Minute, 3, budgets).withPods(4, 2, 1))
		}
	}
	for _, engines := range []int32{4, 8} {
		for _, strategy := range strategies {
			for _, partitions := range [][]int32{nil, {1, 0}} {
				out = append(out, seqChain([]int32{engines, 2}, []v1beta1.ComponentType{eng, rtr}, strategy, "1", partitions, 10*time.Minute, 3, budgets))
			}
			if full {
				out = append(out, seqChain([]int32{engines, 2}, []v1beta1.ComponentType{eng, rtr}, strategy, "10%", nil, 0, 1, budgets))
			}
		}
	}
	rng := rand.New(rand.NewSource(20261019))
	sample := 32
	if full {
		sample = 120
	}
	shapes := [][]int32{{12, 8}, {16, 8}, {12, 4}, {24, 8}, {30, 10}, {15, 15}, {20, 10}, {6, 2}, {4, 2}, {8, 2}, {2, 6}, {3, 3}, {1, 1}}
	for n := 0; n < sample; n++ {
		shape := append([]int32(nil), shapes[rng.Intn(len(shapes))]...)
		comps := []v1beta1.ComponentType{eng, dec}
		if rng.Intn(3) == 0 {
			comps = []v1beta1.ComponentType{dec, eng}
			shape[0], shape[1] = shape[1], shape[0]
		}
		if rng.Intn(3) == 0 {
			shape = append(shape, 2)
			comps = append(comps, rtr)
		}
		strategy := strategies[rng.Intn(2)]
		budget := []string{"1", "10%"}[rng.Intn(2)]
		partitions := make([]int32, len(comps))
		for i := range partitions {
			if rng.Intn(3) == 0 {
				partitions[i] = []int32{1, 2, shape[i] / 2}[rng.Intn(3)]
			}
		}
		var wait time.Duration
		if rng.Intn(2) == 0 {
			wait = 10 * time.Minute
		}
		retries := int32(3)
		if rng.Intn(4) == 0 {
			retries = 1
		}
		cfg := seqChain(shape, comps, strategy, budget, partitions, wait, retries, budgets)
		// A mixed chain: the second member rolls the other way.
		if rng.Intn(3) == 0 {
			other := surge
			if strategy == surge {
				other = recreate
			}
			cfg.members[1].strategy = other
		}
		if rng.Intn(3) == 0 {
			pods := []int32{2, 4, 8}[rng.Intn(3)]
			cfg = cfg.withPods(pods, pods)
		}
		if rng.Intn(4) == 0 && cfg.follower() >= 0 {
			cfg = cfg.withScalingPolicy()
		}
		cfg.name = cfg.String()
		out = append(out, cfg)
	}
	return out
}

// seqExplore explores every configuration keep admits (and, when
// OME_SEQUENTIAL_HUNT_ONLY is set, whose name contains it) and reports each
// broken invariant with the shortest path to it. A finding of class
// no-way-out (a state that cannot move with nothing known explaining it),
// order (a later Component started before an earlier one finished), a wait
// skipped, a run left open or a plan gate held with no run, budget, depth or
// unsettled fails the test; the known classes are reported with their
// reading, and a known class no configuration reached is named so a fix
// that removed it also removes its line.
func seqExplore(t *testing.T, keep func(seqConfig) bool) {
	t.Helper()
	full := os.Getenv("OME_SEQUENTIAL_HUNT") == "full"
	maxStates, maxDepth, runs := 200, 48, 8
	if full {
		maxStates, maxDepth, runs = 800, 80, 16
	}
	only := os.Getenv("OME_SEQUENTIAL_HUNT_ONLY")
	var configs []seqConfig
	for _, cfg := range seqConfigs(full) {
		if keep(cfg) && strings.Contains(cfg.name, only) {
			configs = append(configs, cfg)
		}
	}
	started := time.Now()
	totals := map[string]int{}
	configsWith := map[string]int{}
	var states, gateCalls, runPasses, allRuns int
	type example struct {
		cfg     seqConfig
		finding seqFinding
	}
	examples := map[string][]example{}
	for n, cfg := range configs {
		w := newSeqWorld(t, cfg)
		rep := w.explore(maxStates, maxDepth)
		w.randomRuns(&rep, 1, runs, 4*maxDepth)
		states += rep.states
		gateCalls += w.gateCalls
		runPasses += w.runPasses
		allRuns += rep.runs
		if full {
			var parts []string
			for class, c := range rep.counts {
				parts = append(parts, fmt.Sprintf("%s=%d", class, c))
			}
			sort.Strings(parts)
			t.Logf("config %d/%d [%s]: %d states, depth %d: %s", n+1, len(configs), cfg.name, rep.states, rep.depth, strings.Join(parts, " "))
		}
		for class, c := range rep.counts {
			totals[class] += c
		}
		for class, f := range rep.findings {
			configsWith[class]++
			if len(examples[class]) < 8 {
				examples[class] = append(examples[class], example{cfg: cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d random runs, %d gate questions, %d run passes, %s", len(configs), states, allRuns, gateCalls, runPasses, time.Since(started).Round(time.Second))
	for _, class := range classes {
		if known, ok := seqKnownClasses[class]; ok {
			t.Logf("%s: %d states across %d configurations: %s", class, totals[class], configsWith[class], known)
		} else {
			t.Logf("%s: %d states across %d configurations", class, totals[class], configsWith[class])
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg.name, ex.finding.message, seqPath(ex.finding.state), ex.finding.state)
		}
		if _, ok := seqKnownClasses[class]; !ok {
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg.name, examples[class][0].finding.message)
		}
	}
	for class := range seqKnownClasses {
		if totals[class] == 0 {
			t.Logf("known class %s: no configuration reached it in this mode", class)
		}
	}
}

// TestSequentialChains_TwoComponentChainsReachTheirEnd explores every chain
// of two Components: the engine then the decoder on the reference shapes,
// an engine then a router, single pods and gangs, under both strategies
// and budgets, with a partition on either member or both, with and without
// a wait after readiness and a scaling policy; failures, lost pods, gangs
// and gang members, spent retries, scales, partition edits, canary steps, a
// pause and a resume, a second push and a push back to version 1 thrown in.
func TestSequentialChains_TwoComponentChainsReachTheirEnd(t *testing.T) {
	seqExplore(t, func(c seqConfig) bool { return len(c.members) == 2 })
}

// TestSequentialChains_ThreeComponentChainsReachTheirEnd explores the same
// space with a router of two as the chain's last member.
func TestSequentialChains_ThreeComponentChainsReachTheirEnd(t *testing.T) {
	seqExplore(t, func(c seqConfig) bool { return len(c.members) == 3 })
}

// serveAll brings every started, created or rebuilt Instance up, the
// projector reaching every member first, until nothing is in flight.
func (w *seqWorld) serveAll(st *seqState) *seqState {
	for round := 0; round < 128; round++ {
		var mv *seqMove
		for _, candidate := range w.progressMoves(st) {
			if candidate.kind == "project" || candidate.kind == "serve" {
				c := candidate
				mv = &c
				break
			}
		}
		if mv == nil {
			return st
		}
		st = w.apply(st, *mv)
	}
	w.t.Fatalf("the fleet did not come up in 128 rounds: %s", st)
	return st
}

// serveOn brings n started Instances of one member up, lowest index first.
func (w *seqWorld) serveOn(st *seqState, member int, n int) *seqState {
	for k := 0; k < n; k++ {
		served := false
		for idx, r := range st.members[member].rows {
			if r.open() {
				st = w.apply(st, seqMove{kind: "serve", member: member, idx: idx})
				served = true
				break
			}
		}
		if !served {
			w.t.Fatalf("member %d has no started Instance to serve: %s", member, st)
		}
	}
	return st
}

// projectAll lets the projector's pass reach every member.
func (w *seqWorld) projectAll(st *seqState) *seqState {
	for i := range st.members {
		if st.members[i].annotation != st.gen {
			return w.apply(st, seqMove{kind: "project"})
		}
	}
	return st
}

// rollTo brings one member to the end of its roll toward the spec's
// revision, one Instance at a time.
func (w *seqWorld) rollTo(st *seqState, member int) *seqState {
	for round := 0; round < 128 && !w.finishedFor(&st.members[member], st.rev); round++ {
		st = w.serveOn(st, member, 1)
	}
	return st
}

// TestSequentialChains_TheRunStaysOpenWhenTheFirstMemberRestsAtItsPartition
// records the known gap the exploration reports as
// known-run-stays-open-while-a-member-rests-at-its-partition. 8 engines
// with a partition of 2, then 8 decoders, both SurgeThenDrain with a surge
// budget of one. The operator pushes version 2: six engines move and two
// stay on version 1 as asked, the engine rests at that shape, the decoders
// roll and finish, and the chain's group reads Idle. The run never closes:
// the run layer closes a run on a member that is not converged only through
// a Staged group phase, which a Sequential chain never computes, and the
// staged member diverges no more, so no new run opens either. The same
// chain without the partition closes its run Completed. The test records
// the current behaviour.
func TestSequentialChains_TheRunStaysOpenWhenTheFirstMemberRestsAtItsPartition(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{8, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategySurgeThenDrain, "1", []int32{2, 0}, 0, 3, seqBudgets{}))
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.serveAll(st)
	if !w.membersComplete(st) {
		t.Fatalf("the chain did not reach its end: %s", st)
	}
	var old int32
	for _, r := range st.members[0].rows {
		if r.rev == 1 {
			old++
		}
	}
	if old != 2 || st.holds[0] != nil || st.holds[1] != nil {
		t.Fatalf("want 2 engines on version 1 and no hold on either member, got %s", st)
	}
	if phases := w.groupPhases(st); phases != string(v1beta1.CoordinationPhaseIdle) {
		t.Fatalf("the chain's group must read Idle at its end, got %s", phases)
	}
	if st.activeRun() == nil {
		t.Fatalf("the run closed %s with the engine resting at its partition; the known gap is fixed, so this test and the known class are updated", st.lastRunOutcome())
	}
	if moves := w.progressMoves(st); len(moves) != 0 {
		t.Fatalf("nothing should be left to move, got %v", moves)
	}
	if class, _ := w.classifyStopped(st); class != "known-run-stays-open-while-a-member-rests-at-its-partition" {
		t.Fatalf("the explorer must read this state as the known gap, got %s", class)
	}
	t.Logf("known-run-stays-open-while-a-member-rests-at-its-partition: %s (%s)", seqKnownClasses["known-run-stays-open-while-a-member-rests-at-its-partition"], st)

	// Without the partition the same chain closes its run.
	w = newSeqWorld(t, seqChain([]int32{8, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategySurgeThenDrain, "1", nil, 0, 3, seqBudgets{}))
	st = w.initial()
	w.settle(st, w.order(0))
	st = w.serveAll(st)
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("without a partition the chain must end with its run closed Completed, got %s", st)
	}
}

// TestSequentialChains_APartitionRaisedAboveTheMovedCountRestsStagedAndReleasesTheChain
// records a partition that asks to hold more Instances than still run the old
// version. 12 engines then 8 decoders, both RecreatePod at 10 percent. The
// operator pushes version 2; six engines serve it and two more are moving.
// The operator raises the engine's partition to 8, asking to keep 8 on
// version 1. The two started engines finish, so 8 run version 2 and 4 run
// version 1: the partition holds the 4, the engine rests staged and Ready
// at that shape, and the decoders roll to the end. The second picture is
// the same shape reached by a scale: with a partition of 6 set before the
// push the engine rolls 6, rests staged and the decoders start; the engine
// is then scaled to 4, which retires the 6 moved Instances and leaves 4 on
// version 1 under a partition of 6; the engine rests staged at the 4 and the
// decoders finish. Lowering the partition to 4 afterwards changes nothing.
// The run stays open behind the staged engine in both pictures, the known
// gap recorded above.
func TestSequentialChains_APartitionRaisedAboveTheMovedCountRestsStagedAndReleasesTheChain(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	budgets := seqBudgets{partitionEdits: 2, scales: 1}
	for _, tc := range []struct {
		name          string
		before        []int32
		edit, release seqMove
	}{
		{
			name: "the partition raised to 8 after 6 engines moved",
			edit: seqMove{kind: "partition", member: 0, value: 8}, release: seqMove{kind: "partition", member: 0, value: 4},
		},
		{
			name: "the engine scaled from 12 to 4 under a partition of 6", before: []int32{6, 0},
			edit: seqMove{kind: "scale", member: 0, value: 4}, release: seqMove{kind: "partition", member: 0, value: 4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", tc.before, 0, 3, budgets))
			st := w.initial()
			w.settle(st, w.order(0))
			st = w.projectAll(st)
			st = w.serveOn(st, 0, 6)
			st = w.apply(st, tc.edit)
			st = w.projectAll(st)
			st = w.serveAll(st)
			engine := st.members[0]
			var held int32
			for _, r := range engine.rows {
				if r.rev != st.rev {
					held++
				}
			}
			if !w.membersComplete(st) || !engine.readyTrue || held != 4 || st.holds[0] != nil || st.holds[1] != nil {
				t.Fatalf("the engine must rest staged and Ready with the 4 Instances still on version 1 held and the decoders done, got %s", st)
			}
			if st.activeRun() == nil {
				t.Fatalf("the run closed behind the staged engine; the known gap is fixed, so this test is updated")
			}

			// Lowering the partition to the count still on version 1 changes
			// nothing: the shape is already the one it asks for.
			st = w.apply(st, tc.release)
			st = w.projectAll(st)
			if !w.membersComplete(st) || st.members[0].inFlight() || st.members[1].inFlight() {
				t.Fatalf("the chain must stay at its end once the partition matches what still runs version 1, got %s", st)
			}
		})
	}
}

// TestSequentialChains_ALaterMemberWaitsUntilTheStagedOneRecordsTheNextPush
// records the gate's reading of a staged member behind a push. 12 engines, then
// 8 decoders of which 2 stay on version 1, then 2 routers. Version 2 rolls
// in order and rests with the decoder staged: 6 decoders on version 2, 2
// held on version 1, its current revision still version 1. The operator
// pushes version 3 and the engines finish it. The projector writes the
// decoder's and the router's new spec; the router's replica reconciles
// before the decoder's status records version 3. The decoder's status still
// records update revision version 2 over current revision version 1 at its
// staged shape, which is done for version 2 and not for the push, so the
// router waits on the decoder; it still waits once the decoder's status
// records version 3.
func TestSequentialChains_ALaterMemberWaitsUntilTheStagedOneRecordsTheNextPush(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8, 2}, []v1beta1.ComponentType{eng, dec, rtr}, v1beta1.UpdateStrategyRecreatePod, "10%", []int32{0, 2, 0}, 0, 3, seqBudgets{pushes: 1}))
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.serveAll(st)
	if !w.membersComplete(st) || st.members[1].current != 1 || st.members[1].statusTarget != 2 {
		t.Fatalf("the picture is wrong: version 2 must have rolled with the decoder staged at 2 on version 1, got %s", st)
	}
	st = w.apply(st, seqMove{kind: "push"})
	// The projector reaches the engine; the engines finish version 3.
	next := st.clone()
	w.project(next, 0)
	w.settle(next, w.order(0))
	st = next
	st = w.rollTo(st, 0)
	// The projector reaches the decoder and the router; the router's
	// replica runs its pass before the decoder's status write.
	next = st.clone()
	w.project(next, 1)
	w.project(next, 2)
	admitted, hold := w.pass(next, 2)
	decoder := next.members[1]
	if decoder.statusTarget != 2 || decoder.current != 1 {
		t.Fatalf("the picture is wrong: the decoder's status must still record version 2 over version 1, got %s", decoder)
	}
	if admitted != 0 || hold == nil || hold.gate != v1beta1.RolloutHoldGateSequential || hold.reason != "Sequential waiting on decoder" {
		t.Fatalf("the router must wait on the decoder while the decoder's status still records version 2, got admitted=%d hold=%s", admitted, hold)
	}

	// Once the decoder's status records version 3, the router is held.
	later := st.clone()
	w.project(later, 1)
	w.project(later, 2)
	w.pass(later, 1)
	admitted, hold = w.pass(later, 2)
	if admitted != 0 || hold == nil || hold.gate != v1beta1.RolloutHoldGateSequential || hold.reason != "Sequential waiting on decoder" {
		t.Fatalf("after the decoder's status write the router must wait on the decoder, got admitted=%d hold=%s", admitted, hold)
	}
}

// TestSequentialChains_SpentRetriesOnTheFirstComponentHoldTheSecondUnderItsName
// records the engine's known failure hold: 12 engines then 8 decoders under
// RecreatePod at 10 percent; the engine's first start fails and its one
// retry is spent. The engine reports "update held after 1 failed
// attempt(s)" and the decoders wait under "Sequential waiting on engine",
// which names the Component and not its spent retries. The same hold appears on a
// real cluster.
func TestSequentialChains_SpentRetriesOnTheFirstComponentHoldTheSecondUnderItsName(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", nil, 0, 1, seqBudgets{fails: 1}))
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.apply(st, seqMove{kind: "fail", member: 0, idx: 0})
	st = w.serveAll(st)
	if st.members[0].retry != seqRetryHeld {
		t.Fatalf("the picture is wrong: the engine's retries must be spent, got %s", st)
	}
	if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateHeld {
		t.Fatalf("the engine's hold = %s, want Held", h)
	}
	if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential || h.reason != "Sequential waiting on engine" {
		t.Fatalf("the decoder's hold = %s, want Sequential \"Sequential waiting on engine\"", h)
	}
	if len(w.progressMoves(st)) != 0 || w.membersComplete(st) {
		t.Fatalf("the chain must stand still, got moves %v (%s)", w.progressMoves(st), st)
	}
	t.Logf("known-spent-retries-hold-the-chain: %s (engine: %s; decoder holds on %s)", seqKnownClasses["known-spent-retries-hold-the-chain"], st.members[0], st.holds[1])
}

// TestSequentialChains_AScaleUpOnTheWaitingComponentBuildsOnTheTargetVersionAtOnce
// records the decided behaviour: 12 engines then 8 decoders; the engine
// rolls, and the decoder is scaled from 8 to 9. The ninth decoder is created
// on version 2 and serves it while the engines are still moving; the gate
// holds starts, not creations, and the decoder's turn later covers the
// eight Instances that existed when the run opened.
func TestSequentialChains_AScaleUpOnTheWaitingComponentBuildsOnTheTargetVersionAtOnce(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", nil, 0, 3, seqBudgets{scales: 1}))
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.serveOn(st, 0, 2)
	st = w.apply(st, seqMove{kind: "scale", member: 1, value: 9})
	st = w.projectAll(st)
	decoder := st.members[1]
	if n := len(decoder.rows); n != 9 || decoder.rows[8].kind != seqCreating || decoder.rows[8].to != 2 {
		t.Fatalf("the picture is wrong: the ninth decoder must be building toward version 2, got %s", decoder)
	}
	st = w.apply(st, seqMove{kind: "serve", member: 1, idx: 8})
	if r := st.members[1].rows[8]; r.kind != seqReady || r.rev != 2 || w.finishedFor(&st.members[0], 2) {
		t.Fatalf("the ninth decoder must serve version 2 while the engine is unfinished, got %s", st)
	}
	if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential {
		t.Fatalf("the decoder's eight older Instances must still wait on the engine, got hold %s", h)
	}
	st = w.serveAll(st)
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("the chain must end with the run closed Completed, got %s", st)
	}
}

// TestSequentialChains_ALostInstanceOfTheWaitingMemberIsRebuiltOnTheNewVersionAtOnce
// records the decided behaviour: 12 engines then 8 decoders under
// RecreatePod at 10 percent. The engine rolls two Instances when decoder
// Instance 3 loses its pod. The Create pass rebuilds it on version 2 at
// once, without asking the Sequential gate, and it serves version 2 while
// the engine is unfinished; the decoder's other seven Instances wait for
// their turn and the chain ends with the run closed.
func TestSequentialChains_ALostInstanceOfTheWaitingMemberIsRebuiltOnTheNewVersionAtOnce(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", nil, 0, 3, seqBudgets{losses: 1}))
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.serveOn(st, 0, 2)
	st = w.apply(st, seqMove{kind: "lose", member: 1, idx: 3})
	if r := st.members[1].rows[3]; r.kind != seqRebuild || r.to != 2 {
		t.Fatalf("decoder Instance 3 must be rebuilt toward version 2 at once, got %s", st.members[1])
	}
	st = w.apply(st, seqMove{kind: "serve", member: 1, idx: 3})
	if r := st.members[1].rows[3]; r.kind != seqReady || r.rev != 2 || w.finishedFor(&st.members[0], 2) {
		t.Fatalf("decoder Instance 3 must serve version 2 while the engine is unfinished, got %s", st)
	}
	if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential || h.reason != "Sequential waiting on engine" {
		t.Fatalf("the decoder's other Instances must wait on the engine, got hold %s", h)
	}
	st = w.serveAll(st)
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("the chain must end with the run closed Completed, got %s", st)
	}
}

// TestSequentialChains_APodLostOnTheFinishedComponentKeepsOneWaitClock
// records the wait's clock: 12 engines under SurgeThenDrain then 8
// decoders, with a ten minute wait between them. The engines finish, the
// group records the engine's finish, the wait passes and the decoders
// start. One engine pod is lost: a surging member declares no
// maxUnavailable, so one missing pod takes the engine under its serving
// minimum and its Ready condition turns False. The decoder's next start is
// held while the engine is not Ready, and released as soon as the pod is
// rebuilt: the wait is measured from the recorded finish, so it does not
// start over.
func TestSequentialChains_APodLostOnTheFinishedComponentKeepsOneWaitClock(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategySurgeThenDrain, "10%", nil, 10*time.Minute, 3, seqBudgets{losses: 1}))
	w.settleOrder = []int{0, 1} // the engine's status write lands before the decoder asks
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.rollTo(st, 0)
	if h := st.holds[1]; h == nil || h.reason != "Sequential.Soak: decoder waiting out the 10m0s soak after engine" {
		t.Fatalf("the picture is wrong: the decoder must wait out the ten minutes, got hold %s (%s)", h, st)
	}
	if recorded, _ := st.finishRecorded(eng); !recorded {
		t.Fatalf("the group must record the engine's finish, got %s", st)
	}
	st = w.apply(st, seqMove{kind: "wait", member: 0})
	if !st.members[1].inFlight() {
		t.Fatalf("the picture is wrong: the decoders must start once the wait passed, got %s", st)
	}
	st = w.serveOn(st, 1, 1)
	st = w.apply(st, seqMove{kind: "lose", member: 0, idx: 0})
	if st.members[0].readyTrue {
		t.Fatalf("the picture is wrong: one lost pod must take a surging engine under its serving minimum, got %s", st.members[0])
	}
	// The decoder's one surge in flight serves, so its next start reaches
	// the gate.
	st = w.serveOn(st, 1, 1)
	if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential || !strings.Contains(h.reason, "held while engine is not Ready") {
		t.Fatalf("the decoder's next start must be held while the engine is not Ready, got hold %s", h)
	}
	st = w.apply(st, seqMove{kind: "serve", member: 0, idx: 0})
	if h := st.holds[1]; h != nil && h.gate == v1beta1.RolloutHoldGateSequential {
		t.Fatalf("the decoder's hold = %s; the wait is measured once from the recorded finish and must not start over", h)
	}
	st = w.serveAll(st)
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("the chain must end with the run closed Completed, got %s", st)
	}
}

// TestSequentialChains_TheWaitAfterReadinessIsMeasuredAfterAStagedComponent
// records the wait after readiness behind a staged Component. 12 engines of
// which 2 stay on version 1, then 8 decoders, with a ten minute wait between
// them. The ten movable engines serve version 2 and the engine rests staged,
// Ready, with its current revision at version 1 by design. The decoders wait
// out the ten minutes from the engine's Ready transition and start once the
// wait has passed. The same chain with no partition waits the same way.
func TestSequentialChains_TheWaitAfterReadinessIsMeasuredAfterAStagedComponent(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", []int32{2, 0}, 10*time.Minute, 3, seqBudgets{}))
	w.settleOrder = []int{0, 1}
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.rollTo(st, 0)
	engine := st.members[0]
	if !engine.readyTrue || engine.waited || engine.current != 1 || engine.statusTarget != 2 {
		t.Fatalf("the picture is wrong: the engine must rest staged and Ready with its current revision at version 1, got %s", engine)
	}
	if h := st.holds[1]; st.members[1].inFlight() || h == nil || h.reason != "Sequential.Soak: decoder waiting out the 10m0s soak after engine" {
		t.Fatalf("the decoder must wait out the ten minutes after the staged engine, got %s (hold %s)", st.members[1], h)
	}
	st = w.apply(st, seqMove{kind: "wait", member: 0})
	if !st.members[1].inFlight() {
		t.Fatalf("the decoders must start once the wait passed, got %s", st)
	}

	// Without the partition the same chain waits.
	w = newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", nil, 10*time.Minute, 3, seqBudgets{}))
	w.settleOrder = []int{0, 1}
	st = w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.rollTo(st, 0)
	if h := st.holds[1]; st.members[1].inFlight() || h == nil || h.reason != "Sequential.Soak: decoder waiting out the 10m0s soak after engine" {
		t.Fatalf("the picture is wrong: without a partition the decoder must wait, got %s (hold %s)", st.members[1], h)
	}
}

// TestSequentialChains_ThePartitionOnTheFirstComponentHandsOverOnceStaged
// records the designed reading beside the known gap: with a partition of 2 set
// before the push on 12 engines, the engine rolls 10, rests staged, and the
// decoders start; the chain ends with 2 engines on version 1 and the
// decoder's hold cleared.
func TestSequentialChains_ThePartitionOnTheFirstComponentHandsOverOnceStaged(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", []int32{2, 0}, 0, 3, seqBudgets{}))
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.serveAll(st)
	if !w.membersComplete(st) {
		t.Fatalf("the chain did not end: %s", st)
	}
	engine := st.members[0]
	var old int32
	for _, r := range engine.rows {
		if r.rev == 1 {
			old++
		}
	}
	if old != 2 || !engine.readyTrue || st.holds[1] != nil {
		t.Fatalf("want 2 engines on version 1, the engine Ready and the decoder's hold cleared, got %s", st)
	}
}

// TestSequentialChains_APauseHoldsTheChainAndAResumeContinuesIt records
// the operator's pause: 12 engines then 8 decoders, RecreatePod at 10
// percent, a ten minute wait between them. Two engines are moving when the
// operator pauses: nothing else starts, the moving Instances stand where
// they are and the group reads Paused. After the resume the engine
// finishes, the wait runs, and the decoders roll to the end. A pause set
// while the wait runs does not restart it: the engine's finish is read
// again after the resume and the decoders start as soon as the wait has
// passed.
func TestSequentialChains_APauseHoldsTheChainAndAResumeContinuesIt(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", nil, 10*time.Minute, 3, seqBudgets{pauses: 2}))
	w.settleOrder = []int{0, 1}
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	if open := st.members[0].count(seqDrain); open != 2 {
		t.Fatalf("the picture is wrong: two engines must be moving under 10 percent of 12, got %s", st.members[0])
	}
	st = w.apply(st, seqMove{kind: "pause"})
	if phases := w.groupPhases(st); phases != string(v1beta1.CoordinationPhasePaused) {
		t.Fatalf("the group must read Paused, got %s", phases)
	}
	if open := st.members[0].count(seqDrain); open != 2 || st.members[1].inFlight() {
		t.Fatalf("nothing may start or finish under the pause, got %s", st)
	}
	if moves := w.progressMoves(st); len(moves) != 1 || moves[0].kind != "resume" {
		t.Fatalf("under the pause only the resume moves the chain, got %v", moves)
	}
	st = w.apply(st, seqMove{kind: "resume"})
	st = w.rollTo(st, 0)
	if h := st.holds[1]; h == nil || h.reason != "Sequential.Soak: decoder waiting out the 10m0s soak after engine" {
		t.Fatalf("after the resume the decoder must wait out the ten minutes once the engine finished, got hold %s (%s)", h, st)
	}
	// A pause under the running wait: the wait passes meanwhile, and the
	// decoders start on the pass after the resume.
	st = w.apply(st, seqMove{kind: "pause"})
	st = w.apply(st, seqMove{kind: "wait", member: 0})
	if st.members[1].inFlight() {
		t.Fatalf("nothing may start under the pause, got %s", st.members[1])
	}
	st = w.apply(st, seqMove{kind: "resume"})
	if !st.members[1].inFlight() {
		t.Fatalf("the decoders must start on the pass after the resume, got %s (hold %s)", st.members[1], st.holds[1])
	}
	st = w.serveAll(st)
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("the chain must end with the run closed Completed, got %s", st)
	}
}

// TestSequentialChains_ARollbackMidChainReturnsTheFirstMemberAndStartsNoOther
// records a push back to version 1 while the chain's first member is half
// way: 12 engines then 8 decoders, RecreatePod at 10 percent. Four engines
// serve version 2 when the operator pushes version 1 back. The run toward
// version 2 is replaced by a run toward version 1, the four engines return
// to version 1 one batch at a time, the decoders never start, and the run
// closes Completed with every Instance on version 1.
func TestSequentialChains_ARollbackMidChainReturnsTheFirstMemberAndStartsNoOther(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", nil, 0, 3, seqBudgets{rollbacks: 1}))
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.serveOn(st, 0, 4)
	st = w.apply(st, seqMove{kind: "rollback"})
	st = w.projectAll(st)
	if run := st.activeRun(); run == nil || seqRevisionOfHash(run.TargetRevisions[0].Revision) != 1 || st.lastRunOutcome() != v1beta1.RolloutRunSuperseded {
		t.Fatalf("the run toward version 2 must be replaced by one toward version 1, got %s", st)
	}
	if !st.members[0].inFlight() {
		t.Fatalf("the engines on version 2 must start back toward version 1, got %s", st.members[0])
	}
	st = w.serveAll(st)
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("the chain must end on version 1 with the run closed Completed, got %s", st)
	}
	for i, m := range st.members {
		for _, r := range m.rows {
			if r.rev != 1 {
				t.Fatalf("member %d must be back on version 1, got %s", i, m)
			}
		}
	}
	if st.members[1].current != 1 || st.members[1].statusTarget != 1 {
		t.Fatalf("the decoders must rest on version 1, got %s", st.members[1])
	}
}

// TestSequentialChains_AFailedReplacementUnderARaisedPartitionHoldsWithTheExitNamed
// records what a raised partition does to a failed replacement the push
// still asks for. 12 engines with a partition of 2, then 8 decoders, both
// SurgeThenDrain at 10 percent. The operator pushes version 2: Instances 0
// and 1 stay on version 1, Instances 2 and 3 move, and the replacement of
// Instance 4 fails while its old pod serves. The operator raises the
// engine's partition to 3. The partition now holds Instances 0, 1 and 4,
// so the roll never retries Instance 4; the rest of the engines move. The
// failed replacement toward version 2 stays under the held Instance, since
// its retry is a start the partition refuses: the engine holds with
// Instance 4, the failed replacement and the exits named, the decoders
// wait under "Sequential waiting on engine", the group reads
// engine.Shifting and the run stays open until the operator acts. Clearing
// the partition lets the roll retry Instance 4 and the chain ends with its
// run closed. A push to another revision is the other exit
// (TestSequentialChains_AHeldInstanceKeepsItsFailedReplacementOnlyWhileThePushStillAsksForIt).
func TestSequentialChains_AFailedReplacementUnderARaisedPartitionHoldsWithTheExitNamed(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategySurgeThenDrain, "10%", []int32{2, 0}, 0, 3, seqBudgets{fails: 1, partitionEdits: 2}))
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.serveOn(st, 0, 2)
	if r := st.members[0].rows[4]; r.kind != seqSurge {
		t.Fatalf("the picture is wrong: engine Instance 4 must be surging, got %s", st.members[0])
	}
	st = w.apply(st, seqMove{kind: "fail", member: 0, idx: 4})
	st = w.apply(st, seqMove{kind: "partition", member: 0, value: 3})
	st = w.projectAll(st)
	st = w.serveAll(st)
	for len(w.progressMoves(st)) > 0 {
		st = w.apply(st, w.progressMoves(st)[0])
	}
	engine := st.members[0]
	if engine.rows[4].kind != seqFailedSurge || !w.heldIndices(0, &engine)[4] || engine.inFlight() {
		t.Fatalf("the picture is wrong: the partition must hold the failed replacement of Instance 4 with nothing in flight, got %s", engine)
	}
	if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateRetryBlock ||
		!strings.Contains(h.reason, "Instance 4") || !strings.Contains(h.reason, "partition of 3") || !strings.Contains(h.reason, "lower the partition") {
		t.Fatalf("the engine must hold with Instance 4, its failed replacement and the exit named, got hold %s (%s)", h, st)
	}
	if h := st.holds[1]; h == nil || h.reason != "Sequential waiting on engine" {
		t.Fatalf("the decoders must wait on the engine, got hold %s (%s)", h, st)
	}
	if class, _ := w.classifyStopped(st); class != "known-failed-replacement-toward-the-target-waits-under-the-partition" {
		t.Fatalf("the explorer must read this state as the failed replacement toward the target waiting under the partition, got %s (%s)", class, st)
	}
	if st.activeRun() == nil || w.groupPhases(st) != "engine.Shifting" {
		t.Fatalf("the run must stay open with the group reading engine.Shifting while the operator has not acted (%s)", st)
	}
	t.Logf("known-failed-replacement-toward-the-target-waits-under-the-partition: %s (%s)", seqKnownClasses["known-failed-replacement-toward-the-target-waits-under-the-partition"], st)

	// Clearing the partition releases the Instance: the roll retries it and
	// the chain ends.
	st = w.apply(st, seqMove{kind: "partition", member: 0, value: 0})
	st = w.projectAll(st)
	st = w.serveAll(st)
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("once the partition is cleared the chain must end with its run closed Completed, got %s", st)
	}
}

// seqHeldRecordOutlivesTheRetiredInstance builds the picture two tests
// read: 12 engines then 8 decoders, RecreatePod at 10 percent, a ten
// minute wait between them, one attempt before the retries stop. Two
// engines move, the third Instance's attempt fails and the retries stop.
// The operator scales the engine to 2, which retires the failed Instance.
// Every engine left runs version 2, but the Held record keeps the engine's
// current revision at version 1, so its Ready condition reads Unknown.
func seqHeldRecordOutlivesTheRetiredInstance(t *testing.T) (*seqWorld, *seqState) {
	t.Helper()
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", nil, 10*time.Minute, 1, seqBudgets{fails: 1, scales: 1, pushes: 1}))
	w.settleOrder = []int{0, 1}
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	st = w.serveOn(st, 0, 2)
	if r := st.members[0].rows[2]; r.kind != seqDrain {
		t.Fatalf("the picture is wrong: engine Instance 2 must be moving, got %s", st.members[0])
	}
	st = w.apply(st, seqMove{kind: "fail", member: 0, idx: 2})
	if st.members[0].retry != seqRetryHeld {
		t.Fatalf("the picture is wrong: the engine's retries must be spent, got %s", st.members[0])
	}
	st = w.apply(st, seqMove{kind: "scale", member: 0, value: 2})
	st = w.projectAll(st)
	engine := st.members[0]
	if len(engine.rows) != 2 || engine.rows[0].rev != 2 || engine.rows[1].rev != 2 || engine.current != 1 || engine.ready != metav1.ConditionUnknown {
		t.Fatalf("the picture is wrong: both engines left must run version 2 under a Held record with the current revision at version 1, got %s", engine)
	}
	return w, st
}

// TestSequentialChains_TheFirstStartOfTheNextMemberSlipsBeforeTheFinishIsRecorded
// records the finding the exploration reports as
// known-wait-skipped-until-the-finish-is-recorded, on the picture above.
// The engine's roll reads done the moment the failed Instance is retired,
// its Ready condition reads Unknown, and the decoder's replica asks before
// the service controller records the engine's finish: the gate reads no
// clock and admits the decoder's first start without the ten minute wait.
// The finish is recorded on the next service pass and the decoder's next
// start waits. The test records the current behaviour.
func TestSequentialChains_TheFirstStartOfTheNextMemberSlipsBeforeTheFinishIsRecorded(t *testing.T) {
	w, st := seqHeldRecordOutlivesTheRetiredInstance(t)
	decoder := st.members[1]
	if open := decoder.count(seqDrain); open != 1 {
		t.Fatalf("the decoder's first start must have been admitted without the wait; the finding is fixed, so this test and the known class are updated (%s)", st)
	}
	var found bool
	for _, b := range st.breaks {
		found = found || b.class == "known-wait-skipped-until-the-finish-is-recorded"
	}
	if !found {
		t.Fatalf("the explorer must read the start as the wait skipped until the finish is recorded, got %v", st.breaks)
	}
	if recorded, _ := st.finishRecorded(v1beta1.EngineComponent); !recorded {
		t.Fatalf("the service controller must have recorded the engine's finish after the start, got %s", st)
	}
	// The admitted Instance serves, so the decoder's next start reaches the
	// gate instead of its own budget.
	st = w.serveOn(st, 1, 1)
	if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential || !strings.Contains(h.reason, "Sequential.Soak") {
		t.Fatalf("the decoder's next start must wait once the finish is recorded, got hold %s", h)
	}
	t.Logf("known-wait-skipped-until-the-finish-is-recorded: %s (%s)", seqKnownClasses["known-wait-skipped-until-the-finish-is-recorded"], st)
}

// TestSequentialChains_AHeldRecordOfARetiredInstanceKeepsTheRunOpen records
// the finding the exploration reports as
// known-held-record-outlives-the-retired-instance, on the picture above.
// The decoders roll to the end and the chain's group reads Idle. The
// engine still reads "update held after 1 failed attempt(s)" for a failure
// no Instance carries, its current revision never moves to version 2, and
// the run never closes. A push of version 3 releases it: the chain rolls
// again and ends with its run closed. The test records the current
// behaviour.
func TestSequentialChains_AHeldRecordOfARetiredInstanceKeepsTheRunOpen(t *testing.T) {
	w, st := seqHeldRecordOutlivesTheRetiredInstance(t)
	st = w.apply(st, seqMove{kind: "wait", member: 0})
	st = w.serveAll(st)
	for len(w.progressMoves(st)) > 0 {
		st = w.apply(st, w.progressMoves(st)[0])
	}
	if !w.membersComplete(st) || w.groupPhases(st) != string(v1beta1.CoordinationPhaseIdle) {
		t.Fatalf("the chain must reach its end with the group reading Idle, got %s", st)
	}
	if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateHeld {
		t.Fatalf("the engine must still read its Held hold, got %s", h)
	}
	if st.activeRun() == nil {
		t.Fatalf("the run closed %s behind the Held record; the finding is fixed, so this test and the known class are updated", st.lastRunOutcome())
	}
	if class, _ := w.classifyStopped(st); class != "known-held-record-outlives-the-retired-instance" {
		t.Fatalf("the explorer must read this state as the Held record outliving the retired Instance, got %s", class)
	}
	t.Logf("known-held-record-outlives-the-retired-instance: %s (%s)", seqKnownClasses["known-held-record-outlives-the-retired-instance"], st)

	// A new revision releases the Held record.
	st = w.apply(st, seqMove{kind: "push"})
	st = w.serveAll(st)
	for len(w.progressMoves(st)) > 0 {
		st = w.apply(st, w.progressMoves(st)[0])
	}
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("after the push the chain must end with its run closed Completed, got %s", st)
	}
}

// TestSequentialChains_AHeldRecordOfARetiredInstanceUnderAPartitionKeepsTheChain
// records the same finding under a partition, on the picture above: the
// operator sets a partition of 1 on the engine. Both engines left run
// version 2, so no Instance is held and the staged-shape reading never says
// done, while the Held record keeps the current revision at version 1: the
// gate keeps the engine active and the decoder waits under "Sequential
// waiting on engine" for good. Clearing the partition lets the chain move
// on from the engine's shape, and it ends the way the test above records.
// The test records the current behaviour.
func TestSequentialChains_AHeldRecordOfARetiredInstanceUnderAPartitionKeepsTheChain(t *testing.T) {
	w, st := seqHeldRecordOutlivesTheRetiredInstance(t)
	st = w.serveAll(st)
	st = w.apply(st, seqMove{kind: "partition", member: 0, value: 1})
	st = w.projectAll(st)
	st = w.apply(st, seqMove{kind: "wait", member: 0})
	for len(w.progressMoves(st)) > 0 {
		st = w.apply(st, w.progressMoves(st)[0])
	}
	if w.membersComplete(st) {
		t.Fatalf("the decoder must not have rolled behind the held engine; the finding is fixed, so this test and the known class are updated (%s)", st)
	}
	if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential || !strings.Contains(h.reason, "Sequential waiting on engine") {
		t.Fatalf("the decoder must wait on the engine, got hold %s", h)
	}
	if class, _ := w.classifyStopped(st); class != "known-held-record-outlives-the-retired-instance" {
		t.Fatalf("the explorer must read this state as the Held record outliving the retired Instance, got %s", class)
	}
	t.Logf("known-held-record-outlives-the-retired-instance under a partition: %s", st)

	// Clearing the partition lets the chain move on from the engine's shape.
	st = w.apply(st, seqMove{kind: "partition", member: 0, value: 0})
	st = w.projectAll(st)
	st = w.serveAll(st)
	for len(w.progressMoves(st)) > 0 {
		st = w.apply(st, w.progressMoves(st)[0])
	}
	if !w.membersComplete(st) || st.activeRun() == nil {
		t.Fatalf("after the partition is cleared the chain must reach its end with the run still open behind the Held record, got %s", st)
	}
}

// seqParsePath reads a path the explorer printed back into moves.
func seqParsePath(path string) []seqMove {
	var out []seqMove
	for _, part := range strings.Split(path, " -> ") {
		f := strings.Fields(strings.TrimSpace(part))
		if len(f) == 0 {
			continue
		}
		mv := seqMove{kind: f[0]}
		if len(f) >= 3 && f[1] == "member" {
			mv.member, _ = strconv.Atoi(f[2])
		}
		if len(f) >= 5 && f[3] == "row" {
			mv.idx, _ = strconv.Atoi(f[4])
		}
		if len(f) >= 5 && f[3] == "to" {
			v, _ := strconv.Atoi(f[4])
			mv.value = int32(v)
		}
		out = append(out, mv)
	}
	return out
}

// TestSequentialChains_ReplayAPath replays the path OME_SEQUENTIAL_HUNT_PATH
// names (as the explorer prints it) on the configuration whose name is
// OME_SEQUENTIAL_HUNT_ONLY, and prints every surface at the end: the rows
// as published, the status fields, the observation the group machine
// reads, the Sequential gate's own answer and the group entries. Skipped
// without both variables.
func TestSequentialChains_ReplayAPath(t *testing.T) {
	name, path := os.Getenv("OME_SEQUENTIAL_HUNT_ONLY"), os.Getenv("OME_SEQUENTIAL_HUNT_PATH")
	if name == "" || path == "" {
		t.Skip("set OME_SEQUENTIAL_HUNT_ONLY to a configuration name and OME_SEQUENTIAL_HUNT_PATH to a path")
	}
	var cfg *seqConfig
	for _, full := range []bool{false, true} {
		for _, c := range seqConfigs(full) {
			if c.name == name {
				c := c
				cfg = &c
				break
			}
		}
	}
	if cfg == nil {
		t.Fatalf("no configuration named %q", name)
	}
	w := newSeqWorld(t, *cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	moves := seqParsePath(path)
	for n, mv := range moves {
		if n == len(moves)-1 {
			t.Logf("before the last move %s:\n%s", mv, w.surfaces(st))
		}
		st = w.apply(st, mv)
	}
	t.Logf("after the last move:\n%s", w.surfaces(st))
	for _, f := range w.check(st) {
		t.Logf("finding %s: %s", f.class, f.message)
	}
}

// surfaces is every surface of a state in words: the live and published
// rows, the status fields, the observation the group machine reads, the
// Sequential gate's own answer, the group entries and the run.
func (w *seqWorld) surfaces(st *seqState) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  %s\n", st)
	reads := w.reader(st)
	for i, mc := range w.cfg.members {
		m := &st.members[i]
		obs := w.observe(st, i)
		allowed, reason := coordination.CheckSequentialGate(w.ctx, reads, st.service, mc.comp)
		pub := make([]string, 0, len(m.published))
		for _, r := range m.published {
			pub = append(pub, r.String())
		}
		fmt.Fprintf(&b, "  %s: published %s; statusTarget=%d current=%d ready=%s waited=%t statusGen=%d irGen=%d annotation=%d gen=%d\n    observation inFlight=%t atShape=%t partition=%d readyAt=%t notReady=%t failed=%t\n    sequential gate: allowed=%t %s\n",
			mc.comp, strings.Join(pub, ","), m.statusTarget, m.current, m.ready, m.waited, m.statusGen, m.irGen, m.annotation, st.gen,
			obs.RolloutInFlight, obs.AtDesiredShape, obs.Partition, !obs.ReadyAt.IsZero(), obs.NotReady, obs.Failed, allowed, reason)
	}
	for _, g := range st.groupEntries() {
		record := "none"
		if g.MemberFinished != nil {
			record = string(g.MemberFinished.Component)
		}
		fmt.Fprintf(&b, "  group %s: %s current=%s previous=%s finish=%s message=%q\n", g.Name, g.CompositePhase, g.CurrentComponent, g.PreviousComponent, record, g.Message)
	}
	if run := st.activeRun(); run != nil {
		fmt.Fprintf(&b, "  run %s open\n", run.RunID)
	}
	return b.String()
}

// TestSequentialChains_AHeldInstanceKeepsItsFailedReplacementOnlyWhileThePushStillAsksForIt
// records what a partition does to a failed replacement under an Instance
// it holds. 4 engines under SurgeThenDrain with a surge budget of 1, then
// 2 decoders. The operator pushes version 2; the engine's first
// replacement pod fails beside its serving source and the retry record
// enters its backoff. The operator raises the engine's partition to 4,
// holding every engine Instance. While version 2 is what the push asks
// for, the failed replacement stays under held Instance 0, since its retry
// is a start the partition refuses: the engine holds with Instance 0 and
// the exits named, it does not read at rest, and the decoders wait under
// "Sequential waiting on engine". Lowering the partition lets Instance 0
// retry, the engine finishes, and the decoders roll to the end. A later
// push to version 3 removes the retry record and the failed replacement
// with it: Instance 0 returns to version 1, the engine rests at the shape
// the partition asks for, and the decoders roll version 3 to the end; the
// run stays open behind the engine resting at its partition until the
// partition is lowered and the engine rolls too.
func TestSequentialChains_AHeldInstanceKeepsItsFailedReplacementOnlyWhileThePushStillAsksForIt(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	setup := func(t *testing.T) (*seqWorld, *seqState) {
		w := newSeqWorld(t, seqChain([]int32{4, 2}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategySurgeThenDrain, "1", nil, 0, 3, seqBudgets{fails: 1, partitionEdits: 2, pushes: 1}))
		st := w.initial()
		w.settle(st, w.order(0))
		st = w.projectAll(st)
		if st.members[0].count(seqSurge) != 1 {
			t.Fatalf("the engine must have one surge open, got %s", st)
		}
		st = w.apply(st, seqMove{kind: "fail", member: 0, idx: 0})
		st = w.apply(st, seqMove{kind: "partition", member: 0, value: 4})
		return w, w.projectAll(st)
	}

	t.Run("the retry record still names version 2", func(t *testing.T) {
		w, st := setup(t)
		st = w.apply(st, seqMove{kind: "backoff", member: 0})
		st = w.projectAll(st)
		engine := st.members[0]
		if engine.rows[0].kind != seqFailedSurge || engine.inFlight() || engine.readyTrue {
			t.Fatalf("the engine must stand with the failed replacement under held Instance 0, nothing in flight and not Ready, got %s", st)
		}
		if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateRetryBlock ||
			!strings.Contains(h.reason, "Instance 0") || !strings.Contains(h.reason, "partition of 4") || !strings.Contains(h.reason, "lower the partition") {
			t.Fatalf("the engine must hold with Instance 0, its failed replacement and the exits named, got %s", st)
		}
		if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential || h.reason != "Sequential waiting on engine" {
			t.Fatalf("the decoders must wait on the engine, got %s", st)
		}
		if w.complete(st) || len(w.progressMoves(st)) != 0 {
			t.Fatalf("nothing moves until the operator acts, got %s", st)
		}
		if class, message := w.classifyStopped(st); class != "known-failed-replacement-toward-the-target-waits-under-the-partition" {
			t.Fatalf("the picture must read as known-failed-replacement-toward-the-target-waits-under-the-partition, got %s: %s", class, message)
		}

		// Lowering the partition lets Instance 0 retry: the engine
		// finishes and the decoders roll to the end.
		st = w.apply(st, seqMove{kind: "partition", member: 0, value: 0})
		st = w.projectAll(st)
		st = w.serveAll(st)
		if !w.complete(st) {
			t.Fatalf("the chain must reach its end once the partition no longer holds Instance 0, got %s", st)
		}
	})

	t.Run("a later push removed the retry record", func(t *testing.T) {
		w, st := setup(t)
		st = w.apply(st, seqMove{kind: "push"})
		st = w.projectAll(st)
		st = w.serveAll(st)
		engine := st.members[0]
		for idx, r := range engine.rows {
			if r.kind != seqReady || r.rev != 1 {
				t.Fatalf("engine %d must rest Ready on version 1 under the partition once the failed replacement is gone, got %s", idx, st)
			}
		}
		if !engine.readyTrue || st.holds[0] != nil || engine.retry != seqRetryNone {
			t.Fatalf("the engine must rest at the shape the partition asks for with no hold and no retry record, got %s", st)
		}
		if !w.membersComplete(st) || st.holds[1] != nil {
			t.Fatalf("the decoders must roll version 3 to the end behind the resting engine, got %s", st)
		}
		for idx, r := range st.members[1].rows {
			if r.kind != seqReady || r.rev != 3 {
				t.Fatalf("decoder %d must run version 3, got %s", idx, st)
			}
		}
		// The run stays open behind the engine resting at its partition:
		// the known gap TestSequentialChains_TheRunStaysOpenWhenTheFirstMemberRestsAtItsPartition
		// records, not this picture's.
		if class, message := w.classifyStopped(st); class != "known-run-stays-open-while-a-member-rests-at-its-partition" {
			t.Fatalf("the open run must read as the engine resting at its partition, got %s: %s", class, message)
		}

		// Lowering the partition rolls the engine too and the run closes.
		st = w.apply(st, seqMove{kind: "partition", member: 0, value: 0})
		st = w.projectAll(st)
		st = w.serveAll(st)
		if !w.complete(st) {
			t.Fatalf("the chain must reach its end with its run closed once the partition is lowered, got %s", st)
		}
	})
}

// TestSequentialChains_AFailedReplacementOfALeftRevisionUnderAHeldInstanceIsRemovedAndTheChainGoesOn
// records what a push does to a failed replacement under a held Instance.
// 16 engines under RecreatePod at 10 percent, then 9 decoders under
// SurgeThenDrain with a surge budget of 1, then 3 routers under RecreatePod.
// Version 2 rolls: the engines finish, decoders 0 to 4 move, the
// replacement for decoder 5 fails beside its serving source and version
// 2's retry record enters its backoff. The operator raises the decoder's
// partition to 9, which holds every decoder still on version 1, decoder 5
// among them. While version 2 is what the push asks for, the failed
// replacement stays: the decoder holds with Instance 5 and the exit
// named, the routers wait on the decoder, and nothing moves until the
// operator acts. A push to version 3 removes version 2's retry record and
// the failed replacement with it, since the partition keeps the serving
// source and not a replacement no push asks for: decoder 5 returns to
// version 1, the decoder rests at its staged shape, and the routers roll
// version 3 to the end. The run stays open behind the decoder resting at
// its partition, the known gap recorded above.
func TestSequentialChains_AFailedReplacementOfALeftRevisionUnderAHeldInstanceIsRemovedAndTheChainGoesOn(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	cfg := seqConfig{
		members: []seqMemberConfig{
			{comp: eng, desired: 16, strategy: v1beta1.UpdateStrategyRecreatePod, budget: "10%"},
			{comp: dec, desired: 9, strategy: v1beta1.UpdateStrategySurgeThenDrain, budget: "1"},
			{comp: rtr, desired: 3, strategy: v1beta1.UpdateStrategyRecreatePod, budget: "10%"},
		},
		maxAttempts: 3,
		budgets:     seqBudgets{fails: 1, partitionEdits: 1, pushes: 1},
	}
	cfg.name = cfg.String()
	w := newSeqWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.projectAll(st)
	for !w.stagedOrDone(&st.members[0], 2) {
		st = w.serveOn(st, 0, 1)
	}
	for i := 0; i < 5; i++ {
		st = w.serveOn(st, 1, 1)
	}
	if decoder := st.members[1]; decoder.count(seqSurge) != 1 || decoder.rows[5].kind != seqSurge {
		t.Fatalf("decoders 0 to 4 must run version 2 with decoder 5's surge open, got %s", decoder)
	}
	st = w.apply(st, seqMove{kind: "fail", member: 1, idx: 5})
	st = w.apply(st, seqMove{kind: "partition", member: 1, value: 9})
	st = w.projectAll(st)
	st = w.apply(st, seqMove{kind: "backoff", member: 1})
	st = w.projectAll(st)

	// Version 2 is still what the push asks for: the failed replacement
	// stays under the held Instance and the decoder names it.
	decoder := st.members[1]
	if decoder.rows[5].kind != seqFailedSurge || decoder.inFlight() || decoder.readyTrue {
		t.Fatalf("the failed replacement toward version 2 must stand under held decoder 5 with nothing in flight and the decoder not at rest, got %s", st)
	}
	if h := st.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateRetryBlock ||
		!strings.Contains(h.reason, "Instance 5") || !strings.Contains(h.reason, "partition of 9") || !strings.Contains(h.reason, "lower the partition") {
		t.Fatalf("the decoder must hold with Instance 5, its failed replacement and the exit named while the push still asks for version 2, got %s", st)
	}
	if h := st.holds[2]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential || h.reason != "Sequential waiting on decoder" {
		t.Fatalf("the routers must wait on the decoder, got %s", st)
	}
	if w.complete(st) || len(w.progressMoves(st)) != 0 {
		t.Fatalf("nothing moves until the operator acts, got %s", st)
	}
	if class, message := w.classifyStopped(st); class != "known-failed-replacement-toward-the-target-waits-under-the-partition" {
		t.Fatalf("the picture must read as known-failed-replacement-toward-the-target-waits-under-the-partition, got %s: %s", class, message)
	}

	// A push to version 3 removes the retry record and the failed
	// replacement, which no push asks for any more: the decoder rests
	// staged and the chain goes on.
	st = w.apply(st, seqMove{kind: "push"})
	st = w.projectAll(st)
	st = w.serveAll(st)
	decoder = st.members[1]
	for idx, r := range decoder.rows {
		want := int8(1)
		if idx < 5 {
			want = 2
		}
		if r.kind != seqReady || r.rev != want {
			t.Fatalf("decoder %d must rest Ready on version %d under the partition once the failed replacement is gone, got %s", idx, want, st)
		}
	}
	if !decoder.readyTrue || st.holds[1] != nil || decoder.retry != seqRetryNone {
		t.Fatalf("the decoder must rest at its staged shape with no hold and no retry record, got %s", st)
	}
	if !w.membersComplete(st) || st.holds[2] != nil {
		t.Fatalf("the chain must reach its end with the routers on version 3 and every decoder held, got %s", st)
	}
	for idx, r := range st.members[2].rows {
		if r.kind != seqReady || r.rev != 3 {
			t.Fatalf("router %d must run version 3, got %s", idx, st)
		}
	}
	// The decoder rests at its partition, so the run stays open behind it:
	// the known gap TestSequentialChains_TheRunStaysOpenWhenTheFirstMemberRestsAtItsPartition
	// records, not this picture's.
	if class, _ := w.classifyStopped(st); class != "known-run-stays-open-while-a-member-rests-at-its-partition" {
		t.Fatalf("with every member at rest the open run must read as a member resting at its partition, got %s (%s)", class, st)
	}
}
