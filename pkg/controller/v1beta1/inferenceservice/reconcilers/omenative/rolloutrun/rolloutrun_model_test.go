package rolloutrun_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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

// This file drives the run layer (rolloutrun.Reconcile: the run opens on a
// fresh divergence, is replaced on a new target, closes on convergence or
// on a group resting at its partition, and keeps the plan the gates read),
// the full gate stack (coordination.EvaluateUpdateGate, whose first
// gate is the plan gate the run layer feeds) and the group phase machine
// (ComputeTransition with BuildGroupStatus and MergeGroupStatus, whose
// Staged phase the run's close reads) over a model of one prefill/decode
// group: the engine and the decoder, with a router beside them in some
// configurations, in one rollingUpdate or blueGreen group. The model is
// each member's InferenceReplica as the replica controller publishes it
// (the rows, the current and update revisions, the generation the status
// was written for, the generation the projector recorded, the partition,
// the retry record and the Ready condition) and the service's status as
// the service controller writes it (the run, the plan conditions and the
// group entries).
//
// Between passes the world makes one move: the projector reaches a member,
// a started Instance serves, a pod, a whole gang or one gang member is
// lost, an attempt fails, the retry record moves, or the operator acts (a
// partition set, raised, lowered or cleared on a side; a scale of one
// side, of both, or of the anchor under a scaling policy; the policy
// added or removed; the group's ratio rule added or removed; the rollout
// group removed from the spec or put back; a pause and a resume; a second
// push; a push back to version 1; a release of a held revision). After
// every move the service controller's pass runs (the run layer, then the
// group machine), then the dispatcher's admission runs for every member
// the way the replica controller runs it: the retry record, the
// per-Component budget and the gate, every admitted start written to its
// row before the next question, the Create pass rebuilding rows that lost
// their pods, and the status write that ends a pass. Every path is
// explored breadth-first to a bound with a hash of visited states, and
// seeded runs go deeper. At every state the model checks that the roll can
// still reach its end with its run closed, that the run does not re-open
// over a picture it closed on, that a run does not close Completed before
// the members rest at the shape the spec asks for, and that the budgets
// hold. OME_RUN_LAYER_HUNT=full widens the shapes, the sample and the
// bounds.

const (
	// runServiceName and runNamespace name the service every group is built
	// under; the replicas and revisions take their names from them.
	runServiceName = "svc"
	runNamespace   = "ns"
	// runReadyCondition is the replica's Ready condition type.
	runReadyCondition = "Ready"
)

// runLastRevision is the highest version the operator pushes: version 1 is
// what the fleet stands on, version 2 the push, version 3 a later push.
const runLastRevision int8 = 3

// runRevisionName names a Component's revision the way the replica records
// it; the trailing segment is the hash the gates and the run compare.
func runRevisionName(comp v1beta1.ComponentType, rev int8) string {
	if rev <= 0 {
		return ""
	}
	return runServiceName + "-" + string(comp) + "-v" + strconv.Itoa(int(rev))
}

// runRevisionOf reads the version back off a revision name; 0 for none.
func runRevisionOf(name string) int8 {
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

// runRevisionOfHash reads the version off a revision hash, the trailing
// segment of the run layer's target ("v2"); 0 for none.
func runRevisionOfHash(hash string) int8 {
	n, err := strconv.Atoi(strings.TrimPrefix(hash, "v"))
	if err != nil {
		return 0
	}
	return int8(n)
}

// runRevisionObject is the ControllerRevision of one version of a member,
// named the way the controller names it, with no pairing protocol.
func runRevisionObject(comp v1beta1.ComponentType, rev int8) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Namespace: runNamespace, Name: runRevisionName(comp, rev)}}
}

// runParentGeneration is the projector's record of the service generation a
// replica was last projected from.
func runParentGeneration(gen int64) map[string]string {
	return map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: strconv.FormatInt(gen, 10)}
}

// runRowKind is what the roll has done to one Instance, as its row reads.
type runRowKind uint8

const (
	runReady           runRowKind = iota // every pod Ready and serving on rev
	runLost                              // every pod is gone; the row reads Pending on rev
	runShort                             // a gang short of one member; the row keeps Ready on rev with the counts short, out of rotation
	runRebuild                           // the Create pass rebuilds the pods toward to; the row records rev
	runDrain                             // a recreate toward to is open; nothing serves
	runSurge                             // a surge toward to is open; the source serves on rev
	runFailedUnserving                   // a recreate toward to ended Failed; nothing serves
	runFailedSurge                       // a surge toward to failed; published Ready over the serving source
	runCreating                          // a new Instance builds toward to
)

var runRowKindNames = [...]string{"ready", "lost", "short", "rebuild", "drain", "surge", "failed", "failedsurge", "creating"}

// runRow is one Instance: its kind, the revision it runs (0 before it has
// served) and the revision its attempt, its rebuild or its creation moves
// toward. scaled marks an Instance a scale-up created during the roll.
type runRow struct {
	kind   runRowKind
	rev    int8
	to     int8
	scaled bool
}

func (r runRow) serving() bool {
	switch r.kind {
	case runReady, runSurge, runFailedSurge:
		return true
	}
	return false
}

// open reports an update attempt in flight.
func (r runRow) open() bool { return r.kind == runDrain || r.kind == runSurge }

func (r runRow) String() string {
	s := runRowKindNames[r.kind]
	if r.rev > 0 {
		s += "@" + strconv.Itoa(int(r.rev))
	}
	if r.to > 0 && r.kind != runReady && r.kind != runShort && r.kind != runLost && r.to != r.rev {
		s += ">" + strconv.Itoa(int(r.to))
	}
	if r.scaled {
		s += "+"
	}
	return s
}

// runRetry is the state of a member's retry record for its target.
type runRetry uint8

const (
	runRetryNone     runRetry = iota
	runRetryBackoff           // a failure was recorded; no fresh start until the backoff ends
	runRetryDue               // the backoff ended; the next pass admits a wave of starts
	runRetryRetrying          // one wave at a time while an attempt is in flight
	runRetryHeld              // the retries are spent; no fresh start until a new revision or a release
)

var runRetryNames = [...]string{"", "backoff", "due", "retrying", "held"}

// runMemberConfig is one Component of the group: its count, the pods of
// each Instance, how it rolls, its own budget and the user partition in
// place before the push.
type runMemberConfig struct {
	comp      v1beta1.ComponentType
	desired   int32
	pods      int32 // pods per Instance: 1 for a single pod, 2 to 8 for a gang
	strategy  v1beta1.UpdateStrategyType
	budget    string // the member's own maxSurge when it surges, else its own maxUnavailable
	partition int32  // the rollingUpdate.partition set before the push; 0 for none
}

func (m runMemberConfig) surges() bool { return m.strategy == v1beta1.UpdateStrategySurgeThenDrain }

func (m runMemberConfig) short() string {
	if m.surges() {
		return "surge"
	}
	return "drain"
}

// runSetting is one of the operator's prefill/decode settings: the group's
// ratio rule, and the keepRatio and firstSide rules of the operator
// configuration.
type runSetting struct {
	name          string
	maintainRatio *int32 // the group's maintainRatio tolerance; nil for no rule
	keepRatio     bool
	tolerance     *int32
	firstSide     v1beta1.ComponentType
}

func (s runSetting) defaults() coordination.GroupDefaults {
	return coordination.GroupDefaults{
		RatioTolerancePercent: s.tolerance,
		PrefillDecode:         coordination.PrefillDecodeRules{KeepRatio: s.keepRatio, FirstSide: s.firstSide},
	}
}

// runBudgets is how many times the world may make each costly move in one
// run.
type runBudgets struct {
	fails, losses, scales, partitionEdits, pushes, rollbacks, pauses, policyEdits, groupEdits, ruleEdits, releases int8
}

// runConfig is one configuration of the group under exploration.
type runConfig struct {
	name    string
	members []runMemberConfig // the engine, the decoder, and a router when the group lists one
	// rollingUpdate declares the group's progression rollingUpdate with the
	// group budget; false declares blueGreen with no body.
	rollingUpdate bool
	groupBudget   string // the group's maxSurge and maxUnavailable; "" leaves the engine's default
	setting       runSetting
	maxAttempts   int32
	// scalingPolicy makes the decoder follow the engine's count at the ratio
	// the shape declares, the way the scaling coordinator applies a policy.
	scalingPolicy bool
	budgets       runBudgets
}

func (c runConfig) String() string {
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
	s := strings.Join(parts, " + ")
	if c.rollingUpdate {
		s += " rollingUpdate"
		if c.groupBudget != "" {
			s += "@" + c.groupBudget
		}
	} else {
		s += " blueGreen"
	}
	s += " " + c.setting.name
	if c.scalingPolicy {
		s += " scaling-policy"
	}
	return s + fmt.Sprintf(" retries=%d", c.maxAttempts)
}

// anchor is the member whose count a scaling policy drives: the engine.
func (c runConfig) anchor() int {
	for i, m := range c.members {
		if m.comp == v1beta1.EngineComponent {
			return i
		}
	}
	return 0
}

// follower is the member a scaling policy resizes with the anchor: the
// decoder; -1 when the group has none.
func (c runConfig) follower() int {
	for i, m := range c.members {
		if m.comp == v1beta1.DecoderComponent && i != c.anchor() {
			return i
		}
	}
	return -1
}

// runMember is one member's replica as published, with the world's
// remaining moves on it.
type runMember struct {
	rows []runRow
	// published is the rows as the member's last status write published
	// them: what the gates, the run layer and the group machine read, one
	// pass behind the rows the member's own pass acts on.
	published []runRow
	retry     runRetry
	attempts  int32
	retryRev  int8 // the revision the retry record names

	target       int8 // the revision the replica's spec asks for, as projected
	statusTarget int8 // UpdateRevision as published
	current      int8 // CurrentRevision as published; 0 while the engine keeps it empty
	clearedFrom  int8 // the revision an empty current revision was cleared from while an Instance still ran another

	irGen      int64 // the replica's metadata.generation
	statusGen  int64 // status.observedGeneration
	annotation int64 // the parent generation the projector recorded

	userPartition int32 // the projected rollingUpdate.partition; 0 for none
	replicas      int32 // the projected spec.replicas

	ready     metav1.ConditionStatus // the Ready condition
	readyTrue bool                   // the Ready condition reads True

	fails, losses, scales, partitionEdits int8
}

func (m runMember) count(kinds ...runRowKind) int32 {
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

func (m runMember) inFlight() bool {
	for _, r := range m.rows {
		if r.open() {
			return true
		}
	}
	return false
}

func (m runMember) String() string {
	parts := make([]string, 0, len(m.rows))
	for _, r := range m.rows {
		parts = append(parts, r.String())
	}
	out := strings.Join(parts, ",")
	out += fmt.Sprintf(" target=%d current=%d", m.target, m.current)
	if m.retry != runRetryNone {
		out += fmt.Sprintf(" retry=%s/%d", runRetryNames[m.retry], m.attempts)
	}
	if m.userPartition > 0 {
		out += fmt.Sprintf(" partition=%d", m.userPartition)
	}
	switch m.ready {
	case metav1.ConditionTrue:
		out += " ready"
	case metav1.ConditionFalse:
		out += " not-ready"
	}
	return out
}

// runHold is the first denial a member met in its last pass.
type runHold struct {
	gate   v1beta1.RolloutHoldGate
	reason string
}

func (h *runHold) String() string {
	if h == nil {
		return "none"
	}
	return fmt.Sprintf("%s: %s", h.gate, h.reason)
}

// runMove is one thing the world does between two admissions.
type runMove struct {
	kind   string
	member int
	idx    int
	value  int32
}

func (m runMove) String() string {
	switch m.kind {
	case "push", "rollback", "pause", "resume", "project", "project-between",
		"policy-on", "policy-off", "rule-on", "rule-off", "group-off", "group-on", "scale-both":
		if m.kind == "scale-both" {
			return fmt.Sprintf("scale both sides by %d", m.value)
		}
		return m.kind
	case "backoff", "prune", "spent", "release":
		return fmt.Sprintf("%s member %d", m.kind, m.member)
	case "scale", "partition":
		return fmt.Sprintf("%s member %d to %d", m.kind, m.member, m.value)
	}
	return fmt.Sprintf("%s member %d row %d", m.kind, m.member, m.idx)
}

// runEvent is a reading the service controller's pass produced about the
// run: the class it falls in and what happened.
type runEvent struct {
	class, message string
}

// runState is the model at one moment: the service spec, the service's
// status as the service controller last wrote it, every member's replica,
// the holds of the last admission, the run readings the passes recorded,
// and the moves that led here.
type runState struct {
	members                                                                 []runMember
	gen                                                                     int64   // the service's generation
	rev                                                                     int8    // the revision the service spec names
	desired                                                                 []int32 // the count the spec asks of each member
	partitions                                                              []int32 // the user partition the spec sets on each member
	paused                                                                  bool    // the operator's pause annotation stands
	policy                                                                  bool    // a scaling policy makes the decoder follow the engine
	ratioRule                                                               bool    // the group declares maintainRatio
	grouped                                                                 bool    // the spec declares the rollout group
	pushes, rollbacks, pauses, policyEdits, groupEdits, ruleEdits, releases int8
	holds                                                                   []*runHold
	events                                                                  []runEvent
	path                                                                    []runMove
	// service carries what the service controller writes between passes:
	// the run and its plan conditions, and the group entries.
	service *v1beta1.InferenceService
	// closedPicture is the published picture the last run closed on, so a
	// run that opens again over the same picture is read as such.
	closedPicture string
	closedOutcome v1beta1.RolloutRunOutcome
	// closedPhase is the group phase the last close read: the entry the
	// previous pass wrote, since the run layer runs before the group machine.
	closedPhase v1beta1.CoordinationPhase
}

func (st *runState) clone() *runState {
	out := &runState{
		members: make([]runMember, len(st.members)), gen: st.gen, rev: st.rev,
		desired: append([]int32(nil), st.desired...), partitions: append([]int32(nil), st.partitions...),
		paused: st.paused, policy: st.policy, ratioRule: st.ratioRule, grouped: st.grouped,
		pushes: st.pushes, rollbacks: st.rollbacks, pauses: st.pauses, policyEdits: st.policyEdits,
		groupEdits: st.groupEdits, ruleEdits: st.ruleEdits, releases: st.releases,
		holds: make([]*runHold, len(st.holds)), events: append([]runEvent(nil), st.events...), path: append([]runMove(nil), st.path...),
		service: st.service.DeepCopy(), closedPicture: st.closedPicture, closedOutcome: st.closedOutcome, closedPhase: st.closedPhase,
	}
	for i, m := range st.members {
		m.rows = append([]runRow(nil), m.rows...)
		m.published = append([]runRow(nil), m.published...)
		out.members[i] = m
	}
	return out
}

// activeRun is the run the service's status holds open, nil for none.
func (st *runState) activeRun() *v1beta1.RolloutRun {
	if st.service.Status.Rollout == nil {
		return nil
	}
	return st.service.Status.Rollout.ActiveRun
}

// lastRunOutcome is how the last closed run ended, "" for none.
func (st *runState) lastRunOutcome() v1beta1.RolloutRunOutcome {
	if st.service.Status.Rollout == nil || st.service.Status.Rollout.LastRun == nil {
		return ""
	}
	return st.service.Status.Rollout.LastRun.Outcome
}

// groupEntries are the coordination group entries the service's status
// carries.
func (st *runState) groupEntries() []v1beta1.RolloutCoordinationGroupStatus {
	if st.service.Status.RolloutCoordination == nil {
		return nil
	}
	return st.service.Status.RolloutCoordination.Groups
}

// groupPhase is the phase of the group's status entry, "" for none.
func (st *runState) groupPhase() v1beta1.CoordinationPhase {
	for _, g := range st.groupEntries() {
		return g.Phase
	}
	return ""
}

// planCondition is the RolloutPlanReady condition as the run layer last
// wrote it: its status and reason, "" for none.
func (st *runState) planCondition() string {
	for _, c := range st.service.Status.Conditions {
		if string(c.Type) == v1beta1.RolloutPlanReadyCondition {
			return string(c.Status) + "/" + c.Reason
		}
	}
	return ""
}

// picture is the published state of every member with the spec it was
// published against: what the run layer decides from.
func (st *runState) picture() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d|%t|%t|", st.rev, st.grouped, st.paused)
	for i, m := range st.members {
		for _, r := range m.published {
			b.WriteByte(byte('a' + r.kind))
			b.WriteByte(byte('0' + r.rev))
			b.WriteByte(byte('0' + r.to))
		}
		fmt.Fprintf(&b, "|%d/%d/%d/%d;", m.statusTarget, m.current, st.desired[i], st.partitions[i])
	}
	return b.String()
}

// key hashes what decides the future: the rows, the published revisions,
// whether the projection trails the spec and the status the rows, the
// partitions, the counts, the retry record, the pause and the spec flags,
// the run, the plan condition and the group entries, the budgets left and
// the run readings of the last pass.
func (st *runState) key() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d|%d%d%d%d%d%d%d|%t%t%t%t|", st.rev, st.pushes, st.rollbacks, st.pauses, st.policyEdits, st.groupEdits, st.ruleEdits, st.releases,
		st.paused, st.policy, st.ratioRule, st.grouped)
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
		fmt.Fprintf(&b, "|%d%d%d|%d%d%d%d|%t%t|%d/%d/%d/%d|%s|%d%d%d%d;",
			m.retry, m.attempts, m.retryRev, m.target, m.statusTarget, m.current, m.clearedFrom, projected, written,
			m.userPartition, m.replicas, st.desired[i], st.partitions[i], m.ready,
			m.fails, m.losses, m.scales, m.partitionEdits)
	}
	if run := st.activeRun(); run != nil {
		b.WriteString("run:")
		for _, t := range run.TargetRevisions {
			fmt.Fprintf(&b, "%s=%d/%d,", t.Component, runRevisionOfHash(t.Revision), runRevisionOfHash(t.StableRevision))
		}
		fmt.Fprintf(&b, "%d", len(run.Plan.Groups))
	} else {
		fmt.Fprintf(&b, "norun:%s", st.lastRunOutcome())
	}
	fmt.Fprintf(&b, "|%s", st.planCondition())
	for _, g := range st.groupEntries() {
		fmt.Fprintf(&b, "|%s:%s/%s", g.Name, g.Phase, g.CompositePhase)
	}
	for _, e := range st.events {
		b.WriteString("|" + e.class)
	}
	return b.String()
}

func (st *runState) String() string {
	var parts []string
	for i, m := range st.members {
		parts = append(parts, fmt.Sprintf("member %d [%s] hold=%s", i, m, st.holds[i]))
	}
	run := "no run"
	if r := st.activeRun(); r != nil {
		targets := make([]string, 0, len(r.TargetRevisions))
		for _, t := range r.TargetRevisions {
			targets = append(targets, fmt.Sprintf("%s=%d", t.Component, runRevisionOfHash(t.Revision)))
		}
		run = "run open (" + strings.Join(targets, ",") + ")"
	} else if o := st.lastRunOutcome(); o != "" {
		run = "last run " + string(o)
	}
	var groups []string
	for _, g := range st.groupEntries() {
		groups = append(groups, g.CompositePhase)
	}
	flags := ""
	if st.paused {
		flags += ", paused"
	}
	if !st.grouped {
		flags += ", no group"
	}
	if st.ratioRule {
		flags += ", ratio rule"
	}
	if st.policy {
		flags += ", scaling policy"
	}
	return fmt.Sprintf("spec version %d (%s; plan %s; group %s%s): ", st.rev, run, st.planCondition(), strings.Join(groups, ","), flags) + strings.Join(parts, " | ")
}

func runPath(st *runState) string {
	parts := make([]string, 0, len(st.path))
	for _, mv := range st.path {
		parts = append(parts, mv.String())
	}
	return strings.Join(parts, " -> ")
}

// runWorld builds the objects the gates read from a state and runs the
// passes over them.
type runWorld struct {
	t   *testing.T
	cfg runConfig
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
}

func newRunWorld(t *testing.T, cfg runConfig) *runWorld {
	t.Helper()
	return &runWorld{t: t, cfg: cfg, ctx: context.Background(), passing: -1}
}

// spec is the service spec as the operator has it at a state: one group
// listing every member, rollingUpdate with the group budget or blueGreen,
// the ratio rule when it stands; no rollout block while the group is
// removed.
func (w *runWorld) spec(st *runState) v1beta1.InferenceServiceSpec {
	if !st.grouped {
		return v1beta1.InferenceServiceSpec{}
	}
	comps := make([]v1beta1.ComponentType, 0, len(w.cfg.members))
	for _, m := range w.cfg.members {
		comps = append(comps, m.comp)
	}
	g := v1beta1.RolloutGroup{Components: comps}
	if w.cfg.rollingUpdate {
		g.RollingUpdate = &v1beta1.GroupRollingUpdate{}
		if w.cfg.groupBudget != "" {
			g.RollingUpdate.MaxSurge = ptr.To(intstr.Parse(w.cfg.groupBudget))
			g.RollingUpdate.MaxUnavailable = ptr.To(intstr.Parse(w.cfg.groupBudget))
		}
	} else {
		g.BlueGreen = &v1beta1.GroupBlueGreen{}
	}
	if st.ratioRule {
		g.MaintainRatio = &v1beta1.MaintainRatio{}
		if w.cfg.setting.maintainRatio != nil {
			g.MaintainRatio.Tolerance = ptr.To(*w.cfg.setting.maintainRatio)
		}
	}
	return v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{g}}}
}

// initial is the moment after the push of version 2: the service's
// generation moved, no replica has been projected yet, every Instance runs
// version 1 and serves, and no run is open.
func (w *runWorld) initial() *runState {
	b := w.cfg.budgets
	st := &runState{
		gen: 2, rev: 2, grouped: true, ratioRule: w.cfg.setting.maintainRatio != nil, policy: w.cfg.scalingPolicy,
		pushes: b.pushes, rollbacks: b.rollbacks, pauses: b.pauses, policyEdits: b.policyEdits, groupEdits: b.groupEdits,
		ruleEdits: b.ruleEdits, releases: b.releases,
	}
	st.service = &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: runServiceName, Namespace: runNamespace, Generation: 2}}
	st.service.Spec = w.spec(st)
	for _, mc := range w.cfg.members {
		m := runMember{
			rows: make([]runRow, mc.desired), target: 1, statusTarget: 1, current: 1,
			irGen: 1, statusGen: 1, annotation: 1,
			userPartition: mc.partition, replicas: mc.desired,
			ready: metav1.ConditionTrue, readyTrue: true,
			fails: b.fails, losses: b.losses, scales: b.scales, partitionEdits: b.partitionEdits,
		}
		for i := range m.rows {
			m.rows[i] = runRow{kind: runReady, rev: 1}
		}
		m.published = append([]runRow(nil), m.rows...)
		st.members = append(st.members, m)
		st.desired = append(st.desired, mc.desired)
		st.partitions = append(st.partitions, mc.partition)
		st.holds = append(st.holds, nil)
	}
	return st
}

// v1beta1Rows is the member's rows as the status publishes them, with the
// pod counts of the member's Instance shape.
func (w *runWorld) v1beta1Rows(i int, m *runMember) []v1beta1.OMENativeInstanceStatus {
	mc := w.cfg.members[i]
	comp, pods := mc.comp, mc.pods
	attempt := func(step string, to int8) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: runRevisionName(comp, to)}
	}
	create := func(to int8) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate, TargetRevision: runRevisionName(comp, to)}
	}
	rows := w.publishedRows(i, m)
	out := make([]v1beta1.OMENativeInstanceStatus, 0, len(rows))
	for idx, r := range rows {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(idx), RunningRevision: runRevisionName(comp, r.rev)}
		switch r.kind {
		case runReady:
			row.Phase, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, pods, pods, pods
		case runShort:
			row.Phase, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, pods, pods-1, pods-1
		case runLost:
			row.Phase = v1beta1.OMENativeInstancePending
		case runRebuild:
			row.Phase, row.PodCount, row.Operation = v1beta1.OMENativeInstanceCreating, pods, create(r.to)
		case runDrain:
			row.Phase, row.PodCount, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceUpdating, pods, runRevisionName(comp, r.to), attempt(workloadtypes.UpdateStepDrain, r.to)
		case runSurge:
			row.Phase, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceUpdating, runRevisionName(comp, r.to), attempt(workloadtypes.UpdateStepSurge, r.to)
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 2*pods, pods, pods
		case runFailedUnserving:
			row.Phase, row.TargetRevision = v1beta1.OMENativeInstanceFailed, runRevisionName(comp, r.to)
		case runFailedSurge:
			row.Phase, row.TargetRevision = v1beta1.OMENativeInstanceReady, runRevisionName(comp, r.to)
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = 2*pods, pods, pods
		case runCreating:
			row.Phase, row.PodCount, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceCreating, pods, runRevisionName(comp, r.to), create(r.to)
		}
		row.AvailablePodCount = row.ServingPodCount
		out = append(out, row)
	}
	return out
}

// retryBlocks is the member's retry record as the status publishes it.
func (w *runWorld) retryBlocks(i int, m *runMember) []v1beta1.RetryBlock {
	if m.retry == runRetryNone {
		return nil
	}
	state := v1beta1.RetryBlockBackoff
	switch m.retry {
	case runRetryRetrying:
		state = v1beta1.RetryBlockRetryInProgress
	case runRetryHeld:
		state = v1beta1.RetryBlockHeld
	}
	return []v1beta1.RetryBlock{{TargetRevision: runRevisionName(w.cfg.members[i].comp, m.retryRev), State: state, AttemptsStarted: m.attempts}}
}

// publishedRows are the rows a member's replica carries now: the live rows
// while its own pass runs, else the rows its last status write published.
func (w *runWorld) publishedRows(i int, m *runMember) []runRow {
	if i == w.passing {
		return m.rows
	}
	return m.published
}

// replica is the InferenceReplica a member publishes: the rows, the
// counters the publication derives from them (Instances serving, Instances
// on the update revision and Ready on it), the revisions and the Ready
// condition.
func (w *runWorld) replica(i int, m *runMember) *v1beta1.InferenceReplica {
	mc := w.cfg.members[i]
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: runNamespace, Name: runServiceName + "-" + string(mc.comp),
			Generation: m.irGen, Annotations: runParentGeneration(m.annotation),
		},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: mc.comp,
			Replicas:  ptr.To(m.replicas),
			Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: mc.strategy, RollingUpdate: &v1beta1.RollingUpdate{}}},
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: m.statusGen,
			CurrentRevision:    runRevisionName(mc.comp, m.current),
			UpdateRevision:     runRevisionName(mc.comp, m.statusTarget),
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
	if mc.pods > 1 {
		ir.Spec.Runners = []v1beta1.Runner{{Name: workloadtypes.RunnerLeader, Size: 1}, {Name: workloadtypes.RunnerWorker, Size: mc.pods - 1}}
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
			if r.kind == runReady {
				updatedReady++
			}
		}
	}
	ir.Status.Replicas = int32(len(w.publishedRows(i, m)))
	ir.Status.ServingReplicas, ir.Status.ReadyReplicas, ir.Status.AvailableReplicas = serving, serving, serving
	ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = updated, updatedReady
	cond := metav1.Condition{Type: runReadyCondition, Status: m.ready, Reason: "RolloutInProgress", LastTransitionTime: metav1.Now()}
	switch m.ready {
	case metav1.ConditionTrue:
		cond.Reason = "AllInstancesReady"
	case metav1.ConditionFalse:
		cond.Reason = "ReplicaCountMismatch"
	}
	ir.Status.Conditions = []metav1.Condition{cond}
	return ir
}

// runReader serves the replicas and the revisions of a state to the gates
// and the run layer straight from the model, the only read they make being
// a Get by name: the member whose pass runs sees the rows it has written so
// far, every other reader the rows its last status write published.
type runReader struct {
	client.Reader
	w  *runWorld
	st *runState
}

func (r runReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if key.Namespace == runNamespace {
		switch out := obj.(type) {
		case *v1beta1.InferenceReplica:
			for i, mc := range r.w.cfg.members {
				if key.Name == runServiceName+"-"+string(mc.comp) {
					r.w.replica(i, &r.st.members[i]).DeepCopyInto(out)
					return nil
				}
			}
		case *appsv1.ControllerRevision:
			for _, mc := range r.w.cfg.members {
				for rev := int8(1); rev <= runLastRevision; rev++ {
					if key.Name == runRevisionName(mc.comp, rev) {
						runRevisionObject(mc.comp, rev).DeepCopyInto(out)
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
// and the run world keeps none. Any other list is not served.
func (r runReader) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	if pods, ok := list.(*corev1.PodList); ok {
		pods.Items = nil
		return nil
	}
	return errors.New("the run world serves single objects and pod lists only")
}

// runClient is the run layer's client: it reads through runReader and
// writes nothing, since the run layer writes only to consume a verb
// annotation the model never sets.
type runClient struct {
	client.Client
	reads runReader
}

func (c runClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reads.Get(ctx, key, obj, opts...)
}

func (w *runWorld) reader(st *runState) runReader {
	return runReader{w: w, st: st}
}

// observe reads one member's published replica into the observation the
// group machine computes the group's phase from, the way the service
// controller's observation reads it: the counters, the revisions, the
// Ready condition, a Failed row, and whether the rows rest at the staged
// shape.
func (w *runWorld) observe(st *runState, i int) coordination.ComponentObservation {
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
		if cond.Type != runReadyCondition {
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

// servicePass is the service controller's pass over the group as
// published: the run layer opens, keeps, closes or replaces the run from
// the replicas' revisions, then the group machine computes the group's
// phase, which the status keeps between passes. A run boundary is read
// against the published picture: a close before the members rest at the
// shape the spec asks for, and an open over the very picture the last run
// closed on, are recorded as events of the state.
func (w *runWorld) servicePass(st *runState) (boundary bool) {
	svc := st.service
	svc.Generation = st.gen
	svc.Spec = w.spec(st)
	if st.paused {
		svc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}
	} else {
		svc.Annotations = nil
	}
	reads := w.reader(st)
	w.runPasses++
	var beforeID string
	if run := st.activeRun(); run != nil {
		beforeID = run.RunID
	}
	if _, err := rolloutrun.Reconcile(w.ctx, rolloutrun.Inputs{Client: runClient{reads: reads}, Reader: reads, ISVC: svc, Now: time.Now()}); err != nil {
		w.t.Fatalf("run layer: %v (%s)", err, st)
	}
	var afterID string
	if run := st.activeRun(); run != nil {
		afterID = run.RunID
	}
	picture := st.picture()
	boundary = beforeID != afterID
	switch {
	case beforeID != "" && afterID == "":
		st.closedPicture, st.closedOutcome, st.closedPhase = picture, st.lastRunOutcome(), st.groupPhase()
		if st.closedOutcome == v1beta1.RolloutRunCompleted && !w.publishedAtTheSpecShape(st) {
			st.events = append(st.events, runEvent{class: "run-closed-before-the-roll-ended",
				message: fmt.Sprintf("the run closed Completed while the published rows still have work: %s", w.publishedPicture(st))})
		}
	case beforeID == "" && afterID != "":
		if st.closedPicture != "" && st.closedPicture == picture {
			class, why := "run-reopens-over-the-picture-it-closed-on", ""
			if excuse, ok := w.stagedGroupExcusesAnotherMember(st); ok {
				class, why = "run-opens-and-closes-each-pass-on-a-staged-group", " ("+excuse+")"
			}
			st.events = append(st.events, runEvent{class: class,
				message: fmt.Sprintf("a run opened again over the picture the last run closed %s on%s: %s", st.closedOutcome, why, w.publishedPicture(st))})
		}
	}
	groups := coordination.ResolveGroups(rollout.Effective(svc, rollout.Policies{}), w.cfg.setting.defaults())
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
	return boundary
}

// stagedGroupExcusesAnotherMember reports the picture in which only the
// group's Staged phase could excuse a close: the group reads Staged because
// a member rests at its partition, and another member is neither converged
// on the spec's revision by its published rows and revisions nor resting at
// a partition of its own (an Instance a scale-up is still creating, or a
// retry record keeping its current revision behind). The close excuses only
// the member resting at its partition, so the run stays open until the
// other member converges; a run that opens and closes on alternate passes
// over this picture is named with the member the group would have excused.
func (w *runWorld) stagedGroupExcusesAnotherMember(st *runState) (string, bool) {
	if st.closedPhase != v1beta1.CoordinationPhaseStaged {
		return "", false
	}
	for i := range st.members {
		m := &st.members[i]
		if w.restsAtPartition(m, st.rev) {
			continue
		}
		converged := m.statusTarget == st.rev && m.current == st.rev
		for _, r := range m.published {
			if r.kind != runReady || r.rev != st.rev {
				converged = false
			}
		}
		if converged {
			continue
		}
		why := "its published rows are not all Ready on the spec's revision"
		for _, r := range m.published {
			if r.kind == runCreating {
				why = "a scale-up's Instance is still being created, with no running revision until it is Ready"
			}
		}
		if m.retry != runRetryNone && !m.inFlight() && m.current != st.rev {
			why = fmt.Sprintf("its retry record (%s) keeps its current revision at version %d while every Instance runs version %d", runRetryNames[m.retry], m.current, st.rev)
		}
		return fmt.Sprintf("%s: %s", w.cfg.members[i].comp, why), true
	}
	return "", false
}

// publishedPicture is every member's published rows in words.
func (w *runWorld) publishedPicture(st *runState) string {
	var parts []string
	for i, m := range st.members {
		rows := make([]string, 0, len(m.published))
		for _, r := range m.published {
			rows = append(rows, r.String())
		}
		parts = append(parts, fmt.Sprintf("%s published %s (update revision %d, current %d, partition %d)", w.cfg.members[i].comp, strings.Join(rows, ","), m.statusTarget, m.current, m.userPartition))
	}
	return strings.Join(parts, "; ")
}

// publishedAtTheSpecShape reports whether every member's published rows
// rest at the shape the spec asks for: projected for the spec, the update
// revision recorded, no attempt open or failed, and every Instance on the
// spec's revision but the ones its partition holds, between one and the
// partition of them. A row being rebuilt after a loss, or created by a
// scale-up, is a repair or new capacity, not the roll's work.
func (w *runWorld) publishedAtTheSpecShape(st *runState) bool {
	for i := range st.members {
		m := &st.members[i]
		if m.annotation != st.gen || m.statusTarget != st.rev {
			return false
		}
		var off int32
		for _, r := range m.published {
			switch r.kind {
			case runDrain, runSurge, runFailedUnserving, runFailedSurge:
				return false
			case runCreating:
				continue
			}
			if r.rev != st.rev {
				off++
			}
		}
		if off != 0 && !(m.userPartition > 0 && off <= m.userPartition) {
			return false
		}
	}
	return true
}

// heldIndices is the set of Instances the partition holds, by the roll's own
// rule: the lowest-indexed Instances off the target that no update attempt
// is moving.
func (w *runWorld) heldIndices(m *runMember) map[int]bool {
	held := map[int]bool{}
	p := m.userPartition
	if p <= 0 {
		return held
	}
	var n int32
	for idx, r := range m.rows {
		if r.open() || (r.rev == m.target && r.kind != runCreating) {
			continue
		}
		if n < p {
			held[idx] = true
			n++
		}
	}
	return held
}

// membersComplete reports the roll at its end by the replicas' reading:
// every member projected, its status written, every Instance Ready on the
// spec's revision or held by the member's partition, between one and the
// partition of them.
func (w *runWorld) membersComplete(st *runState) bool {
	for i := range st.members {
		m := &st.members[i]
		if m.annotation != st.gen || m.statusGen != m.irGen || m.target != st.rev {
			return false
		}
		var off int32
		for _, r := range m.rows {
			if r.kind != runReady {
				return false
			}
			if r.rev != st.rev {
				off++
			}
		}
		if off != 0 && !(m.userPartition > 0 && off <= m.userPartition) {
			return false
		}
	}
	return true
}

// complete reports the end of the roll: the members at their end by the
// replicas' reading and the run closed, so the service rests with no run
// open and the next push opens a fresh one.
func (w *runWorld) complete(st *runState) bool {
	return w.membersComplete(st) && st.activeRun() == nil
}

// restsAtPartition reports a member whose partition keeps Instances on
// another revision: between one and the partition of them.
func (w *runWorld) restsAtPartition(m *runMember, rev int8) bool {
	if m.userPartition <= 0 {
		return false
	}
	var off int32
	for _, r := range m.rows {
		if r.rev != rev {
			off++
		}
	}
	return off >= 1 && off <= m.userPartition
}

// writeStatus is the status write that ends a member's pass: the rows as
// they stand, the generation observed, the update revision, the current
// revision by the engine's own rule, and the Ready condition.
func (w *runWorld) writeStatus(i int, m *runMember) {
	comp := w.cfg.members[i].comp
	m.published = append([]runRow(nil), m.rows...)
	m.statusGen = m.irGen
	m.statusTarget = m.target
	insts := v1beta1convert.InstanceStatusSliceToWorkload(w.v1beta1Rows(i, m))
	update := runRevisionName(comp, m.target)
	var blocks []workloadtypes.RetryBlock
	if m.retry != runRetryNone {
		blocks = []workloadtypes.RetryBlock{{TargetRevision: runRevisionName(comp, m.retryRev)}}
	}
	pods := workloadstatus.PodRevisions{}
	for idx, r := range m.rows {
		switch r.kind {
		case runReady, runShort:
			pods[int32(idx)] = []query.RevisionID{query.RevisionFromName(runRevisionName(comp, r.rev))}
		case runSurge, runFailedSurge:
			pods[int32(idx)] = []query.RevisionID{query.RevisionFromName(runRevisionName(comp, r.rev)), query.RevisionFromName(runRevisionName(comp, r.to))}
		}
	}
	before, oldCurrent := runRevisionName(comp, m.clearedFrom), runRevisionName(comp, m.current)
	current := workloadstatus.CurrentRevisionFor(insts, oldCurrent, update, blocks, before, pods)
	m.clearedFrom = runRevisionOf(clearedRevisionAfter(before, oldCurrent, current, update))
	m.current = runRevisionOf(current)
	m.ready = w.readyCondition(i, m, insts)
	m.readyTrue = m.ready == metav1.ConditionTrue
}

// clearedRevisionAfter is the revision an empty current revision was
// cleared from, as the replica status writer records it beside the pair it
// persists: nothing once a current revision is set; otherwise the old
// current, or the earlier record, when it is the update revision.
func clearedRevisionAfter(before, oldCurrent, current, update string) string {
	switch {
	case current != "":
		return ""
	case oldCurrent != "" && oldCurrent == update:
		return update
	case before != "" && before == update:
		return before
	}
	return ""
}

// readyCondition is the replica's Ready condition as the status writer
// derives it: False on a Failed Instance, True when staged at a partition,
// Unknown while the revisions differ, False with no Instance, True when
// every Instance is Ready or the serving count meets the member's own
// minimum, else False.
func (w *runWorld) readyCondition(i int, m *runMember, insts []workloadtypes.InstanceStatus) metav1.ConditionStatus {
	mc := w.cfg.members[i]
	for _, r := range m.rows {
		if r.kind == runFailedUnserving {
			return metav1.ConditionFalse
		}
	}
	replicas := int32(len(m.rows))
	if p := m.userPartition; p > 0 && workloadstatus.ReachedDesiredShape(insts, runRevisionName(mc.comp, m.target), p, replicas) {
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
	if serving >= servingMinimum(replicas, maxUnavailable) {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// servingMinimum is the number of serving Instances the Ready condition
// asks for: the count less the strategy's maxUnavailable, never below zero.
func servingMinimum(replicas int32, maxUnavailable *intstr.IntOrString) int32 {
	if replicas <= 0 {
		return 0
	}
	if maxUnavailable == nil {
		return replicas
	}
	if minimum := replicas - utils.ScaledCountFromIntOrString(maxUnavailable, replicas, true); minimum > 0 {
		return minimum
	}
	return 0
}

// rebuildLost is the Create pass's rebuild of every row short of its pods
// that the roll did not start: a row with no live pod left is rebuilt at the
// target, unless the partition holds it; a gang short of one member is
// rebuilt on the revision it runs. The rebuild asks no coordination gate.
func (w *runWorld) rebuildLost(m *runMember) {
	held := w.heldIndices(m)
	for idx, r := range m.rows {
		switch r.kind {
		case runLost:
			to := m.target
			if held[idx] {
				to = r.rev
			}
			m.rows[idx] = runRow{kind: runRebuild, rev: r.rev, to: to, scaled: r.scaled}
		case runShort:
			m.rows[idx] = runRow{kind: runRebuild, rev: r.rev, to: r.rev, scaled: r.scaled}
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
func (w *runWorld) pass(st *runState, i int) (admitted int, hold *runHold) {
	mc := w.cfg.members[i]
	m := &st.members[i]
	w.passing = i
	defer func() { w.passing = -1 }()
	defer w.writeStatus(i, m)
	defer w.rebuildLost(m)
	// A retry record of a revision the spec does not ask for is removed.
	if m.retry != runRetryNone && m.retryRev != m.target {
		m.retry, m.attempts = runRetryNone, 0
	}
	// The Create and scale-down passes: new Instances build toward the
	// target; a scale-down retires the highest indices.
	for int32(len(m.rows)) < m.replicas && !st.paused {
		m.rows = append(m.rows, runRow{kind: runCreating, to: m.target, scaled: true})
	}
	if int32(len(m.rows)) > m.replicas {
		m.rows = m.rows[:m.replicas]
	}
	if st.paused {
		return 0, &runHold{reason: "paused by the operator"}
	}
	switch {
	case m.retry == runRetryHeld:
		return 0, &runHold{gate: v1beta1.RolloutHoldGateHeld, reason: fmt.Sprintf("update held after %d failed attempt(s)", m.attempts)}
	case m.retry == runRetryBackoff:
		return 0, &runHold{gate: v1beta1.RolloutHoldGateRetryBlock, reason: fmt.Sprintf("update in backoff after %d failed attempt(s)", m.attempts)}
	case m.retry == runRetryRetrying && m.inFlight():
		return 0, &runHold{gate: v1beta1.RolloutHoldGateRetryBlock, reason: "retrying one attempt at a time"}
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
		case runFailedUnserving:
			m.rows[idx] = runRow{kind: runLost, rev: r.rev}
		case runFailedSurge:
			m.rows[idx] = runRow{kind: runReady, rev: r.rev}
		}
	}
	held := w.heldIndices(m)
	// A Failed Instance with no pods that the partition holds is not
	// retried; the Create pass rebuilds it on the revision it ran.
	for idx, r := range m.rows {
		if r.kind == runFailedUnserving && held[idx] {
			m.rows[idx] = runRow{kind: runLost, rev: r.rev}
		}
	}
	// The roll's candidates: Instances off the target the partition does
	// not hold, a failed attempt first. An Instance that lost every pod is
	// the Create pass's to rebuild, not the roll's.
	var order []int
	for idx, r := range m.rows {
		if r.kind == runFailedUnserving && r.rev != m.target && !held[idx] {
			order = append(order, idx)
		}
	}
	for idx, r := range m.rows {
		if (((r.kind == runReady || r.kind == runShort) && r.rev != m.target) || r.kind == runFailedSurge) && !held[idx] {
			order = append(order, idx)
		}
	}
	type answer struct {
		kind runRowKind
		rev  int8
	}
	denied := map[answer]bool{}
	var inFlightSurge, inFlightUnavail, gateUnavail int32
	reads := w.reader(st)
	own := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(mc.budget)), int32(len(m.rows)), true)
	target := runRevisionObject(mc.comp, m.target)
	for _, idx := range order {
		r := m.rows[idx]
		if denied[answer{r.kind, r.rev}] {
			continue
		}
		// A surge needs a whole serving source: a gang short of a member
		// is recreated whatever the strategy says.
		mechanism := workloadtypes.UpdateStrategyRecreatePod
		if mc.surges() && (r.kind == runReady || r.kind == runFailedSurge) {
			mechanism = workloadtypes.UpdateStrategySurgeThenDrain
		}
		// A recreate of a Failed Instance that serves nothing skips the
		// gate: its outage is already in the gate's serving count.
		exempt := r.kind == runFailedUnserving
		var h *runHold
		if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
			prior := m.count(runSurge, runFailedSurge)
			if r.kind == runFailedSurge {
				prior-- // the retry takes its own failed pod's slot
			}
			if prior+inFlightSurge+1 > own {
				h = &runHold{gate: v1beta1.RolloutHoldGateBudget, reason: fmt.Sprintf("per-Component surge budget %d exhausted (would become %d)", own, prior+inFlightSurge+1)}
			}
		} else if prior := m.count(runDrain); prior+inFlightUnavail+1 > own {
			h = &runHold{gate: v1beta1.RolloutHoldGateBudget, reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)", own, prior+inFlightUnavail+1)}
		}
		if h == nil && !exempt {
			w.gateCalls++
			allowed, gate, reason := coordination.EvaluateUpdateGate(w.ctx, reads, st.service, mc.comp, runRevisionName(mc.comp, r.rev), target, nil, w.cfg.setting.defaults(), mechanism, inFlightSurge, gateUnavail)
			if !allowed {
				h = &runHold{gate: gate, reason: reason}
			}
		}
		if h != nil {
			denied[answer{r.kind, r.rev}] = true
			if hold == nil {
				hold = h
			}
			continue
		}
		if mechanism == workloadtypes.UpdateStrategySurgeThenDrain {
			m.rows[idx] = runRow{kind: runSurge, rev: r.rev, to: m.target}
			inFlightSurge++
		} else {
			m.rows[idx] = runRow{kind: runDrain, rev: r.rev, to: m.target}
			inFlightUnavail++
			if !exempt {
				gateUnavail++
			}
		}
		if m.retry == runRetryDue {
			m.retry = runRetryRetrying
		}
		admitted++
	}
	return admitted, hold
}

// settle runs the service controller's pass and then the members' passes
// in the given order until a whole round admits nothing and writes nothing
// new.
func (w *runWorld) settle(st *runState, order []int) {
	boundaries := 0
	for round := 0; round < 32; round++ {
		before := st.key()
		if w.servicePass(st) {
			boundaries++
		}
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
	// The passes never converged. A run that opens on one pass and closes
	// on the next, with a Staged group as the only excuse for a member that
	// is neither converged nor at a partition of its own, is recorded on the
	// state as a finding and the exploration goes on from the picture the
	// last pass left; the close excuses only the member at its partition,
	// so the finding fails the test.
	if excuse, ok := w.stagedGroupExcusesAnotherMember(st); ok && boundaries >= 8 {
		st.events = append(st.events, runEvent{class: "run-opens-and-closes-each-pass-on-a-staged-group",
			message: fmt.Sprintf("the run opened or closed on %d of 32 passes (%s): %s", boundaries, excuse, w.publishedPicture(st))})
		return
	}
	w.t.Errorf("admission did not settle in 32 rounds: %s", st)
}

func (w *runWorld) order(depth int) []int {
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
// Components in: the engine, then the decoder, then the router.
func (w *runWorld) projectorOrder() []int {
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
func (w *runWorld) project(st *runState, i int) {
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
			case runFailedSurge:
				m.rows[idx] = runRow{kind: runReady, rev: r.rev}
			case runFailedUnserving:
				m.rows[idx] = runRow{kind: runLost, rev: r.rev}
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
// backoff ends, a retry record its rows outlived is removed. Under a pause
// only rebuilt Instances come up, and the operator's resume is the move
// that lets the rest continue.
func (w *runWorld) progressMoves(st *runState) []runMove {
	var out []runMove
	// The projector's pass writes every member in its order within one
	// tick; the replicas may reconcile between two of its writes or only
	// after all of them.
	for i := range st.members {
		if st.members[i].annotation != st.gen {
			out = append(out, runMove{kind: "project"}, runMove{kind: "project-between"})
			break
		}
	}
	if st.paused {
		out = append(out, runMove{kind: "resume"})
	}
	for i := range st.members {
		m := &st.members[i]
		first, last := map[runRowKind]int{}, map[runRowKind]int{}
		for idx, r := range m.rows {
			if _, ok := first[r.kind]; !ok {
				first[r.kind] = idx
			}
			last[r.kind] = idx
		}
		kinds := []runRowKind{runDrain, runSurge, runCreating, runRebuild}
		if st.paused {
			kinds = []runRowKind{runRebuild}
		}
		for _, kind := range kinds {
			idx, ok := first[kind]
			if !ok {
				continue
			}
			out = append(out, runMove{kind: "serve", member: i, idx: idx})
			if last[kind] != idx {
				out = append(out, runMove{kind: "serve", member: i, idx: last[kind]})
			}
		}
		switch {
		case m.retry == runRetryBackoff:
			out = append(out, runMove{kind: "backoff", member: i})
		case m.retry != runRetryNone && m.retry != runRetryHeld && !m.inFlight() && m.count(runFailedUnserving, runFailedSurge) == 0:
			// Every row outlived the failure the record counts: the record is
			// removed once the rows have stood Ready for the window.
			out = append(out, runMove{kind: "prune", member: i})
		}
	}
	return out
}

// adverseMoves are the moves that cost the roll something: an attempt
// fails, a pod, a gang or a gang member is lost, the retries are spent, or
// the operator acts.
func (w *runWorld) adverseMoves(st *runState) []runMove {
	var out []runMove
	for i := range st.members {
		m := &st.members[i]
		mc := w.cfg.members[i]
		seenFail, seenLoss := map[runRowKind]bool{}, map[int8]bool{}
		for idx, r := range m.rows {
			switch r.kind {
			case runDrain, runSurge:
				if m.fails > 0 && !seenFail[r.kind] {
					seenFail[r.kind] = true
					out = append(out, runMove{kind: "fail", member: i, idx: idx})
				}
			case runReady:
				// One loss per revision the member runs: a pod of the old
				// version and a pod of the new one are different pictures. A
				// gang loses either one member or every pod.
				if m.losses > 0 && !seenLoss[r.rev] {
					seenLoss[r.rev] = true
					out = append(out, runMove{kind: "lose", member: i, idx: idx})
					if mc.pods > 1 {
						out = append(out, runMove{kind: "lose-member", member: i, idx: idx})
					}
				}
			}
		}
		if m.retry != runRetryHeld && m.retry != runRetryNone && m.count(runFailedUnserving, runFailedSurge) > 0 {
			out = append(out, runMove{kind: "spent", member: i})
		}
		if m.retry == runRetryHeld && st.releases > 0 {
			out = append(out, runMove{kind: "release", member: i})
		}
		n := st.desired[i]
		// Under a scaling policy the follower's count is the policy's to
		// write, so only the other members are resized by hand.
		if m.scales > 0 && !(st.policy && i == w.cfg.follower()) {
			for _, delta := range []int32{1, -1, n / 2, -(n / 2)} {
				if delta != 0 && n+delta >= 1 {
					out = append(out, runMove{kind: "scale", member: i, value: n + delta})
				}
			}
			if i == w.cfg.anchor() && w.cfg.follower() >= 0 && !st.policy {
				// Both sides at once, the ratio kept: each side grows or
				// shrinks by half of itself.
				out = append(out, runMove{kind: "scale-both", member: i, value: 1}, runMove{kind: "scale-both", member: i, value: -1})
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
				out = append(out, runMove{kind: "partition", member: i, value: v})
			}
		}
	}
	if st.pushes > 0 && st.rev < runLastRevision {
		out = append(out, runMove{kind: "push"})
	}
	if st.rollbacks > 0 && st.rev >= 2 {
		out = append(out, runMove{kind: "rollback"})
	}
	if st.pauses > 0 && !st.paused {
		out = append(out, runMove{kind: "pause"})
	}
	if st.policyEdits > 0 && w.cfg.follower() >= 0 {
		if st.policy {
			out = append(out, runMove{kind: "policy-off"})
		} else {
			out = append(out, runMove{kind: "policy-on"})
		}
	}
	if st.ruleEdits > 0 && st.grouped {
		if st.ratioRule {
			out = append(out, runMove{kind: "rule-off"})
		} else {
			out = append(out, runMove{kind: "rule-on"})
		}
	}
	if st.groupEdits > 0 {
		if st.grouped {
			out = append(out, runMove{kind: "group-off"})
		} else {
			out = append(out, runMove{kind: "group-on"})
		}
	}
	return out
}

// followerCount is the count a scaling policy writes on the follower for an
// anchor count: the shape's ratio applied to it, rounded up, at least one.
func (w *runWorld) followerCount(anchor int32) int32 {
	a, f := w.cfg.members[w.cfg.anchor()].desired, w.cfg.members[w.cfg.follower()].desired
	n := (anchor*f + a - 1) / a
	if n < 1 {
		n = 1
	}
	return n
}

// scaleBoth grows or shrinks every side by half of itself, the ratio kept,
// never below one Instance.
func (w *runWorld) scaleBoth(st *runState, direction int32) {
	for i := range st.desired {
		half := st.desired[i] / 2
		if half < 1 {
			half = 1
		}
		n := st.desired[i] + direction*half
		if n < 1 {
			n = 1
		}
		st.desired[i] = n
	}
}

// apply makes one move and then lets the service controller and the
// dispatcher do what the gates allow.
func (w *runWorld) apply(st *runState, mv runMove) *runState {
	next := st.clone()
	next.path = append(next.path, mv)
	next.events = nil
	var m *runMember
	switch mv.kind {
	case "push", "rollback", "project", "project-between", "pause", "resume",
		"policy-on", "policy-off", "rule-on", "rule-off", "group-off", "group-on":
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
		m.rows[mv.idx] = runRow{kind: runReady, rev: r.to, scaled: r.scaled}
		// An Instance converging on a revision removes that revision's
		// retry record; a Held record stays until a new revision or a release.
		if m.retry != runRetryNone && m.retry != runRetryHeld && m.retryRev == r.to {
			m.retry, m.attempts = runRetryNone, 0
		}
	case "fail":
		r := m.rows[mv.idx]
		if r.kind == runSurge {
			m.rows[mv.idx].kind = runFailedSurge
		} else {
			m.rows[mv.idx].kind = runFailedUnserving
		}
		m.fails--
		// A failure counts once per wave: a record in backoff or Held is not
		// moved by a second failure of the same wave.
		if m.retry != runRetryBackoff && m.retry != runRetryHeld {
			m.attempts++
			m.retryRev = r.to
			if m.attempts >= w.cfg.maxAttempts {
				m.retry = runRetryHeld
			} else {
				m.retry = runRetryBackoff
			}
		}
	case "lose":
		m.rows[mv.idx] = runRow{kind: runLost, rev: m.rows[mv.idx].rev, scaled: m.rows[mv.idx].scaled}
		m.losses--
	case "lose-member":
		m.rows[mv.idx] = runRow{kind: runShort, rev: m.rows[mv.idx].rev, scaled: m.rows[mv.idx].scaled}
		m.losses--
	case "backoff":
		m.retry = runRetryDue
	case "prune":
		m.retry, m.attempts = runRetryNone, 0
	case "spent":
		m.retry = runRetryHeld
	case "release":
		m.retry, m.attempts = runRetryNone, 0
		next.releases--
	case "scale":
		next.desired[mv.member] = mv.value
		if next.policy && mv.member == w.cfg.anchor() && w.cfg.follower() >= 0 {
			next.desired[w.cfg.follower()] = w.followerCount(mv.value)
		}
		next.gen++
		m.scales--
	case "scale-both":
		w.scaleBoth(next, mv.value)
		next.gen++
		m.scales--
	case "partition":
		next.partitions[mv.member] = mv.value
		next.gen++
		m.partitionEdits--
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
	case "policy-on":
		next.policy = true
		if n := w.followerCount(next.desired[w.cfg.anchor()]); n != next.desired[w.cfg.follower()] {
			next.desired[w.cfg.follower()] = n
			next.gen++
		}
		next.policyEdits--
	case "policy-off":
		next.policy = false
		next.policyEdits--
	case "rule-on", "rule-off":
		next.ratioRule = mv.kind == "rule-on"
		next.gen++
		next.ruleEdits--
	case "group-off", "group-on":
		next.grouped = mv.kind == "group-on"
		next.gen++
		next.groupEdits--
	}
	w.settle(next, w.order(len(next.path)))
	return next
}

// runFinding is one invariant broken, or one known reading met, at one
// state.
type runFinding struct {
	class   string
	message string
	state   *runState
}

// runKnownClasses are the readings the explorer reports without failing,
// each in words: the engine's designed behaviour, the behaviour not decided
// yet, and the known gaps the tests below record as the current behaviour.
// A fix removes its line here; the class then fails the test if it comes
// back.
var runKnownClasses = map[string]string{
	"known-run-open-while-a-partition-holds-a-whole-side":            "a member whose partition holds every one of its Instances (the partition at or above its count, no Instance on the new version) keeps the run open for good: the group machine reads a member with no new-revision pod as Surging and never Staged, and the run's close reads a member off the target only through a Staged group; the operator asked for exactly this shape, nothing is held by a gate, and only the next push replaces the run (a gap)",
	"known-group-reads-surging-while-a-partition-holds-a-whole-side": "the same reading once the run has closed another way (a push back or a new target replacing it with nothing left to open): the member whose partition holds every Instance keeps the group at Surging for good (a gap, the status half of the class above)",
	"known-failed-replacement-under-a-held-instance":                 "a surge replacement fails while its old pod serves and the partition then holds that Instance: the roll never retries a held Instance, the row keeps its target and reads Failed to the shape reading, the failed replacement keeps its surge slot against the member's own budget, and the member never reads done (the Sequential model's gap, seen here on a prefill/decode group)",
	"known-failed-replacement-under-a-held-instance-holds-the-group": "the same picture with the other members waiting on that member's batch under the batch step",
	"known-retries-spent-holds-the-group":                            "a member's retries are spent and the other members wait on its batch under the batch step; the hold names the batch, not the spent retries; a new revision or a release of the held revision ends it",
	"known-held-record-outlives-the-retired-instance":                "an Instance whose retries are spent is retired by a scale-down, and its Held retry record stays: the member's every Instance runs the new version, its current revision never moves, it reads \"update held after N failed attempt(s)\" for a failure no Instance carries, and the run never closes until a new revision or a release of the held revision (the Sequential model's gap, seen here on a prefill/decode group)",
	"known-retries-spent":                                            "a member's retries are spent with Instances left on the old version and nothing else can move; a new revision or a release of the held revision ends it",
	"known-budget-after-a-scale":                                     "an attempt that started before a scale-down counts against the smaller budget until it ends (by design)",
}

// check reads every invariant at one settled state.
func (w *runWorld) check(st *runState) []runFinding {
	var out []runFinding
	add := func(class, format string, args ...any) {
		out = append(out, runFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	for _, e := range st.events {
		add(e.class, "%s", e.message)
	}
	var pacing *v1beta1.CoordinationPacing
	if st.grouped {
		if groups := coordination.ResolveGroups(w.spec(st).Rollout, w.cfg.setting.defaults()); len(groups) > 0 {
			pacing = &groups[0].Pacing
		}
	}
	for i, mc := range w.cfg.members {
		m := st.members[i]
		n := int32(len(m.rows))
		own := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(mc.budget)), n, true)
		// An attempt that started before a scale-down counts against the
		// smaller budget until it ends; that excess is reported on its own.
		budgetClass := "budget"
		if m.scales < w.cfg.budgets.scales || st.members[w.cfg.anchor()].scales < w.cfg.budgets.scales || st.policyEdits < w.cfg.budgets.policyEdits {
			budgetClass = "known-budget-after-a-scale"
		}
		if mc.surges() {
			open := m.count(runSurge)
			if open+m.count(runFailedSurge) > own {
				add(budgetClass, "%s has %d surges in flight beside %d failed surge pods against its own budget of %d", mc.comp, open, m.count(runFailedSurge), own)
			}
			if pacing != nil {
				if group := coordination.MaxSurgeBudget(*pacing, n); open > group {
					add(budgetClass, "%s has %d surges in flight against the group budget of %d", mc.comp, open, group)
				}
			}
		} else {
			open := m.count(runDrain)
			if open > own {
				add(budgetClass, "%s has %d Instances out of rotation against its own budget of %d", mc.comp, open, own)
			}
			if pacing != nil {
				if group := coordination.MaxUnavailableBudget(*pacing, n); open+m.count(runFailedUnserving) > group {
					add(budgetClass, "%s has %d Instances out of rotation beside %d failed against the group unavailability budget of %d", mc.comp, open, m.count(runFailedUnserving), group)
				}
			}
		}
	}
	if !w.complete(st) && len(w.progressMoves(st)) == 0 {
		class, message := w.classifyStopped(st)
		add(class, "%s", message)
	}
	if w.complete(st) && st.grouped && !st.paused {
		switch st.groupPhase() {
		case v1beta1.CoordinationPhaseIdle, v1beta1.CoordinationPhaseStaged, "":
		default:
			class := "group-reads-rolling-after-the-roll-ended"
			for i := range st.members {
				if w.wholeSideHeld(&st.members[i], st.rev) {
					class = "known-group-reads-surging-while-a-partition-holds-a-whole-side"
				}
				if !w.cfg.rollingUpdate && w.convergedWithAPartition(&st.members[i], st.rev) && class == "group-reads-rolling-after-the-roll-ended" {
					class = "known-group-reads-shifting-while-a-converged-member-carries-a-partition-under-bluegreen"
				}
			}
			add(class, "the roll ended and the run closed %s, and the group reads %s: %s", st.lastRunOutcome(), w.groupPhases(st), w.publishedPicture(st))
		}
	}
	return out
}

// finishedFor reports whether a member has nothing left to roll toward rev:
// no attempt open and every Instance on rev, except those a partition holds.
// An Instance a scale-up is still creating is new capacity, not the roll's
// work, whatever revision it builds toward.
func (w *runWorld) finishedFor(m *runMember, rev int8) bool {
	var off int32
	for _, r := range m.rows {
		if r.open() {
			return false
		}
		if r.kind == runCreating {
			continue
		}
		if r.rev != rev {
			off++
		}
	}
	return off == 0 || (m.userPartition > 0 && off <= m.userPartition)
}

// classifyStopped names why a state that cannot move is what it is. The
// members at their end by the replicas' reading with the run still open
// is the Held record of a retired Instance, a member resting at its
// partition with the run open, or the run left open after the roll ended.
// Else each unfinished member decides: its retries are spent, the plan
// gate holds it with no run open or under an open run, two members hold
// on each other's batch, or nothing explains it.
func (w *runWorld) classifyStopped(st *runState) (class, message string) {
	if w.membersComplete(st) {
		run := st.activeRun()
		if run == nil {
			return "no-way-out", "the roll is at its end with no run open and still reads incomplete: " + st.String()
		}
		for i := range st.members {
			m := &st.members[i]
			if m.retry == runRetryHeld && m.current != m.target {
				return "known-held-record-outlives-the-retired-instance", fmt.Sprintf("%s runs version %d on every Instance under a Held record (%s) and the run %s stays open with the group reading %s", w.cfg.members[i].comp, st.rev, st.holds[i], run.RunID, w.groupPhases(st))
			}
		}
		for i := range st.members {
			if w.wholeSideHeld(&st.members[i], st.rev) {
				return "known-run-open-while-a-partition-holds-a-whole-side", fmt.Sprintf("%s has every Instance held by its partition of %d and the run %s stays open with the group reading %s; %s", w.cfg.members[i].comp, st.members[i].userPartition, run.RunID, w.groupPhases(st), w.publishedPicture(st))
			}
		}
		if !w.cfg.rollingUpdate {
			for i := range st.members {
				if w.convergedWithAPartition(&st.members[i], st.rev) {
					return "known-run-open-while-a-converged-member-carries-a-partition-under-bluegreen", fmt.Sprintf("%s runs version %d on every Instance under a partition of %d, so the blueGreen group never reads Staged, and the run %s stays open with the group reading %s; %s", w.cfg.members[i].comp, st.rev, st.members[i].userPartition, run.RunID, w.groupPhases(st), w.publishedPicture(st))
				}
			}
		}
		for i := range st.members {
			if w.restsAtPartition(&st.members[i], st.rev) {
				return "run-open-while-a-member-rests-at-its-partition", fmt.Sprintf("%s rests at its partition of %d and the run %s stays open with the group reading %s; %s", w.cfg.members[i].comp, st.members[i].userPartition, run.RunID, w.groupPhases(st), w.publishedPicture(st))
			}
		}
		return "run-open-after-the-roll-ended", fmt.Sprintf("every member rests on version %d and the run %s stays open with the group reading %s", st.rev, run.RunID, w.groupPhases(st))
	}
	// Each unfinished member gets a line and a reading; a known reading on
	// any member names the state, since the others wait on it under the
	// batch step's hold on its batch.
	var lines []string
	var root string
	ratioHolds, planHolds := 0, 0
	for i, mc := range w.cfg.members {
		m := &st.members[i]
		if w.finishedFor(m, st.rev) && m.count(runLost, runShort, runRebuild, runCreating, runFailedUnserving, runFailedSurge) == 0 {
			continue
		}
		switch {
		case m.retry == runRetryHeld:
			lines = append(lines, fmt.Sprintf("%s is Held with %s", mc.comp, m))
			if root == "" {
				root = "known-retries-spent"
			}
		case w.heldFailedReplacement(m):
			lines = append(lines, fmt.Sprintf("%s (%s) keeps a failed surge replacement under an Instance its partition holds; its hold is %s", mc.comp, m, st.holds[i]))
			if root == "" {
				root = "known-failed-replacement-under-a-held-instance"
			}
		case st.holds[i] != nil && st.holds[i].gate == v1beta1.RolloutHoldGatePlan:
			planHolds++
			lines = append(lines, fmt.Sprintf("%s (%s) holds on %s (plan %s)", mc.comp, m, st.holds[i], st.planCondition()))
		default:
			if st.holds[i] != nil && st.holds[i].gate == v1beta1.RolloutHoldGateRatio {
				ratioHolds++
			}
			lines = append(lines, fmt.Sprintf("%s (%s) holds on %s", mc.comp, m, st.holds[i]))
		}
	}
	switch {
	case root != "" && len(lines) > 1:
		class = root + "-holds-the-group"
	case root != "":
		class = root
	case planHolds > 0 && st.activeRun() == nil:
		class = "plan-gate-holds-with-no-run"
	case planHolds > 0:
		class = "plan-gate-holds-under-an-open-run"
	case ratioHolds >= 2:
		class = "two-gates-wait-on-each-other"
	default:
		class = "no-way-out"
	}
	return class, strings.Join(lines, "; ") + " (" + w.groupPhases(st) + "; " + st.String() + ")"
}

// heldFailedReplacement reports a member with no attempt in flight whose
// partition holds an Instance whose surge replacement failed beside its
// serving source: the roll never retries a held Instance, the row keeps
// its target, and the failed replacement keeps its surge slot.
func (w *runWorld) heldFailedReplacement(m *runMember) bool {
	if m.inFlight() {
		return false
	}
	held := w.heldIndices(m)
	for idx, r := range m.rows {
		if r.kind == runFailedSurge && held[idx] {
			return true
		}
	}
	return false
}

// wholeSideHeld reports a member whose partition holds every Instance it
// has: no Instance runs the spec's revision and the partition covers the
// count, so nothing of it moves by the operator's own request.
func (w *runWorld) wholeSideHeld(m *runMember, rev int8) bool {
	if m.userPartition <= 0 || m.userPartition < int32(len(m.rows)) {
		return false
	}
	for _, r := range m.rows {
		if r.rev == rev {
			return false
		}
	}
	return true
}

// convergedWithAPartition reports a member whose every Instance runs the
// spec's revision while its spec still carries a partition.
func (w *runWorld) convergedWithAPartition(m *runMember, rev int8) bool {
	if m.userPartition <= 0 {
		return false
	}
	for _, r := range m.rows {
		if r.rev != rev {
			return false
		}
	}
	return true
}

// groupPhases is the group's status entries in words.
func (w *runWorld) groupPhases(st *runState) string {
	var parts []string
	for _, g := range st.groupEntries() {
		parts = append(parts, g.CompositePhase)
	}
	if len(parts) == 0 {
		return "no group entry"
	}
	return strings.Join(parts, ",")
}

// runReport is what one configuration's exploration found.
type runReport struct {
	states, depth, runs int
	findings            map[string]runFinding
	counts              map[string]int
}

func (w *runWorld) record(rep *runReport, st *runState) {
	for _, f := range w.check(st) {
		rep.counts[f.class]++
		if _, ok := rep.findings[f.class]; !ok {
			rep.findings[f.class] = f
		}
	}
}

func (w *runWorld) explore(maxStates, maxDepth int) runReport {
	rep := runReport{findings: map[string]runFinding{}, counts: map[string]int{}}
	start := w.initial()
	w.settle(start, w.order(0))
	visited := map[string]bool{start.key(): true}
	queue := []*runState{start}
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
					rep.findings["depth"] = runFinding{class: "depth", message: "the roll did not end within the depth bound", state: st}
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
func (w *runWorld) randomRuns(rep *runReport, seed int64, runs, maxDepth int) {
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
					rep.findings["depth"] = runFinding{class: "depth", message: "the roll did not end within the depth bound", state: st}
				}
				break
			}
			var mv runMove
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

// runSettings are the operator's prefill/decode settings: none, the
// group's ratio rule at a tolerance of 20, the keepRatio rule, the
// firstSide rule, and their combinations.
func runSettings() []runSetting {
	tol := ptr.To(int32(20))
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	return []runSetting{
		{name: "off"},
		{name: "maintainRatio=20", maintainRatio: tol, tolerance: tol},
		{name: "keepRatio=20", keepRatio: true, tolerance: tol},
		{name: "firstSide=decoder", firstSide: dec},
		{name: "keepRatio=20/firstSide=engine", keepRatio: true, tolerance: tol, firstSide: eng},
		{name: "keepRatio=20/firstSide=decoder", keepRatio: true, tolerance: tol, firstSide: dec},
		{name: "maintainRatio=20/keepRatio=20", maintainRatio: tol, keepRatio: true, tolerance: tol},
	}
}

// runSettingNamed is the setting of that name.
func runSettingNamed(name string) runSetting {
	for _, s := range runSettings() {
		if s.name == name {
			return s
		}
	}
	panic("no setting named " + name)
}

// runPair builds a configuration: the engine and the decoder with their
// strategies and the same own budget, single pods, in one rollingUpdate
// group at the group budget or one blueGreen group.
func runPair(shape [2]int32, es, ds v1beta1.UpdateStrategyType, budget string, rollingUpdate bool, setting runSetting, partitions [2]int32, retries int32, budgets runBudgets) runConfig {
	cfg := runConfig{rollingUpdate: rollingUpdate, setting: setting, maxAttempts: retries, budgets: budgets}
	if rollingUpdate {
		cfg.groupBudget = budget
	}
	cfg.members = []runMemberConfig{
		{comp: v1beta1.EngineComponent, desired: shape[0], pods: 1, strategy: es, budget: budget, partition: partitions[0]},
		{comp: v1beta1.DecoderComponent, desired: shape[1], pods: 1, strategy: ds, budget: budget, partition: partitions[1]},
	}
	cfg.name = cfg.String()
	return cfg
}

// withPods gives each member its gang width, in member order.
func (c runConfig) withPods(pods ...int32) runConfig {
	c.members = append([]runMemberConfig(nil), c.members...)
	for i := range c.members {
		if i < len(pods) && pods[i] > 0 {
			c.members[i].pods = pods[i]
		}
	}
	c.name = c.String()
	return c
}

// withRouter adds a router of n single pods to the group, rolling the way
// the engine does.
func (c runConfig) withRouter(n int32) runConfig {
	c.members = append(append([]runMemberConfig(nil), c.members...),
		runMemberConfig{comp: v1beta1.RouterComponent, desired: n, pods: 1, strategy: c.members[0].strategy, budget: c.members[0].budget})
	c.name = c.String()
	return c
}

// withScalingPolicy makes the decoder follow the engine's count.
func (c runConfig) withScalingPolicy() runConfig {
	c.scalingPolicy = true
	c.name = c.String()
	return c
}

func runDefaultBudgets(full bool) runBudgets {
	b := runBudgets{fails: 1, losses: 1, scales: 1, partitionEdits: 1, pushes: 1, rollbacks: 1, pauses: 1, policyEdits: 1, groupEdits: 1, ruleEdits: 1, releases: 1}
	if full {
		b.fails, b.losses = 2, 2
	}
	return b
}

// runShapes are the reference prefill:decode shapes beside two small ones
// that reach deeper within the bounds.
var runShapes = [][2]int32{{12, 8}, {12, 4}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {6, 2}, {4, 2}}

// runConfigs lists the configurations to explore: the matrix of group
// kind, strategy pair, setting and partition placement on two shapes,
// every other reference shape under both kinds, the mixed strategy
// pairs, the firstSide settings, a scaling policy, gangs of two, four and
// eight pods, a router beside the pair, a partition that holds a whole
// side; then a seeded sample over the whole space. The full mode runs the
// matrix on every shape with more settings and a larger sample.
func runConfigs(full bool) []runConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	budgets := runDefaultBudgets(full)
	off, ratio, keep := runSettingNamed("off"), runSettingNamed("maintainRatio=20"), runSettingNamed("keepRatio=20")
	decoderFirst, keepEngineFirst, keepDecoderFirst := runSettingNamed("firstSide=decoder"), runSettingNamed("keepRatio=20/firstSide=engine"), runSettingNamed("keepRatio=20/firstSide=decoder")
	kinds := []bool{true, false}
	samePairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}}
	mixedPairs := [][2]v1beta1.UpdateStrategyType{{recreate, surge}, {surge, recreate}}
	none, enginePartition, decoderPartition := [2]int32{}, [2]int32{2, 0}, [2]int32{0, 2}
	var out []runConfig
	add := func(cfg runConfig) { out = append(out, cfg) }
	matrix := map[[2]int32]bool{{12, 8}: true, {6, 2}: true}
	settings := []runSetting{off, ratio, keep}
	partitions := [][2]int32{none, enginePartition}
	if full {
		settings = append(settings, keepDecoderFirst)
		partitions = append(partitions, decoderPartition)
	}
	for _, shape := range runShapes {
		for _, kind := range kinds {
			if full || matrix[shape] {
				for _, pair := range samePairs {
					for _, setting := range settings {
						for _, p := range partitions {
							add(runPair(shape, pair[0], pair[1], "10%", kind, setting, p, 3, budgets))
						}
					}
				}
				continue
			}
			if kind {
				add(runPair(shape, recreate, recreate, "10%", true, keep, none, 3, budgets))
			} else {
				add(runPair(shape, surge, surge, "10%", false, off, decoderPartition, 3, budgets))
			}
		}
	}
	for _, pair := range mixedPairs {
		add(runPair([2]int32{12, 8}, pair[0], pair[1], "10%", true, keep, none, 3, budgets))
		add(runPair([2]int32{12, 8}, pair[0], pair[1], "10%", false, off, decoderPartition, 3, budgets))
		if full {
			for _, shape := range [][2]int32{{16, 8}, {6, 2}} {
				add(runPair(shape, pair[0], pair[1], "10%", true, ratio, none, 3, budgets))
				add(runPair(shape, pair[0], pair[1], "10%", false, keepDecoderFirst, enginePartition, 3, budgets))
			}
		}
	}
	// The firstSide rule, alone and with keepRatio.
	add(runPair([2]int32{12, 8}, recreate, recreate, "10%", true, decoderFirst, none, 3, budgets))
	add(runPair([2]int32{12, 8}, surge, surge, "10%", true, keepEngineFirst, none, 3, budgets))
	if full {
		for _, shape := range [][2]int32{{12, 4}, {24, 8}, {6, 2}} {
			add(runPair(shape, recreate, recreate, "10%", true, keepEngineFirst, decoderPartition, 3, budgets))
			add(runPair(shape, surge, surge, "10%", false, decoderFirst, enginePartition, 3, budgets))
		}
	}
	// A scaling policy moving the decoder with the engine.
	add(runPair([2]int32{12, 8}, recreate, recreate, "10%", true, keep, none, 3, budgets).withScalingPolicy())
	add(runPair([2]int32{16, 8}, surge, surge, "10%", false, off, none, 3, budgets).withScalingPolicy())
	if full {
		add(runPair([2]int32{30, 10}, recreate, recreate, "10%", true, ratio, enginePartition, 3, budgets).withScalingPolicy())
		add(runPair([2]int32{12, 4}, surge, surge, "10%", true, keepDecoderFirst, none, 1, budgets).withScalingPolicy())
	}
	// Gangs: the engine of four pods beside a decoder of two, both of
	// eight, and a small group of pairs with a budget of one.
	add(runPair([2]int32{12, 8}, recreate, recreate, "10%", true, keep, none, 3, budgets).withPods(4, 2))
	add(runPair([2]int32{16, 8}, surge, surge, "10%", true, off, none, 3, budgets).withPods(8, 8))
	add(runPair([2]int32{4, 2}, recreate, recreate, "1", false, off, [2]int32{1, 0}, 3, budgets).withPods(2, 2))
	if full {
		add(runPair([2]int32{24, 8}, surge, surge, "10%", true, keepDecoderFirst, decoderPartition, 3, budgets).withPods(8, 4))
		add(runPair([2]int32{12, 8}, recreate, surge, "10%", false, ratio, none, 3, budgets).withPods(4, 2))
	}
	// A router beside the pair, in the same group.
	add(runPair([2]int32{12, 8}, recreate, recreate, "10%", true, keep, none, 3, budgets).withRouter(2))
	add(runPair([2]int32{12, 8}, surge, surge, "10%", false, off, none, 3, budgets).withRouter(2))
	if full {
		add(runPair([2]int32{16, 8}, surge, surge, "10%", true, ratio, enginePartition, 3, budgets).withRouter(2).withPods(4, 2, 1))
	}
	// A partition that holds the decoder whole.
	add(runPair([2]int32{12, 4}, recreate, recreate, "10%", true, keep, [2]int32{0, 4}, 3, budgets))
	add(runPair([2]int32{12, 4}, recreate, recreate, "10%", true, decoderFirst, [2]int32{0, 4}, 3, budgets))
	rng := rand.New(rand.NewSource(20261102))
	sample := 24
	if full {
		sample = 120
	}
	all := runSettings()
	shapes := [][2]int32{{12, 8}, {16, 8}, {12, 4}, {24, 8}, {30, 10}, {15, 15}, {20, 10}, {6, 2}, {4, 2}, {8, 2}, {3, 3}, {2, 2}}
	for n := 0; n < sample; n++ {
		shape := shapes[rng.Intn(len(shapes))]
		pair := [2]v1beta1.UpdateStrategyType{[]v1beta1.UpdateStrategyType{recreate, surge}[rng.Intn(2)], []v1beta1.UpdateStrategyType{recreate, surge}[rng.Intn(2)]}
		budget := []string{"1", "10%"}[rng.Intn(2)]
		var p [2]int32
		for i := range p {
			if rng.Intn(3) == 0 {
				p[i] = []int32{1, 2, shape[i] / 2, shape[i]}[rng.Intn(4)]
			}
		}
		retries := int32(3)
		if rng.Intn(4) == 0 {
			retries = 1
		}
		cfg := runPair(shape, pair[0], pair[1], budget, rng.Intn(2) == 0, all[rng.Intn(len(all))], p, retries, budgets)
		if rng.Intn(3) == 0 {
			pods := []int32{2, 4, 8}[rng.Intn(3)]
			cfg = cfg.withPods(pods, pods)
		}
		if rng.Intn(4) == 0 {
			cfg = cfg.withRouter(2)
		}
		if rng.Intn(4) == 0 {
			cfg = cfg.withScalingPolicy()
		}
		add(cfg)
	}
	return out
}

// runExample is the first picture of one class in one configuration.
type runExample struct {
	cfg     runConfig
	finding runFinding
}

// runSummary gathers the counts of every configuration.
type runSummary struct {
	mu                                  sync.Mutex
	states, gateCalls, runPasses, plays int
	totals, configsWith                 map[string]int
	examples                            map[string][]runExample
}

func (s *runSummary) add(cfg runConfig, w *runWorld, rep runReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states += rep.states
	s.gateCalls += w.gateCalls
	s.runPasses += w.runPasses
	s.plays += rep.runs
	for class, c := range rep.counts {
		s.totals[class] += c
	}
	for class, f := range rep.findings {
		s.configsWith[class]++
		if len(s.examples[class]) < 8 {
			s.examples[class] = append(s.examples[class], runExample{cfg: cfg, finding: f})
		}
	}
}

// runWorkers is how many configurations run at once: OME_RUN_LAYER_HUNT_WORKERS,
// else the processors up to sixteen.
func runWorkers() int {
	if v, err := strconv.Atoi(os.Getenv("OME_RUN_LAYER_HUNT_WORKERS")); err == nil && v > 0 {
		return v
	}
	n := runtime.GOMAXPROCS(0)
	if n > 16 {
		n = 16
	}
	if n < 1 {
		n = 1
	}
	return n
}

// runExplore explores every configuration keep admits (and, when
// OME_RUN_LAYER_HUNT_ONLY is set, whose name contains it) and reports each
// broken invariant with the shortest path to it. A finding of a class not
// listed in runKnownClasses fails the test: a state that cannot move with
// nothing known explaining it, two members holding on each other, the
// plan gate holding with no run or under an open run, a run left open after
// the roll ended or while a member rests at its partition, a run closed
// before the roll ended, a run opened again over the picture it closed
// on, a budget exceeded, the depth bound. The known classes are reported
// with their reading, and a known class no configuration reached is named
// so a fix that removed it also removes its line.
func runExplore(t *testing.T, keep func(runConfig) bool) {
	t.Helper()
	full := os.Getenv("OME_RUN_LAYER_HUNT") == "full"
	maxStates, maxDepth, plays := 1000, 64, 16
	if full {
		maxStates, maxDepth, plays = 4000, 128, 32
	}
	only := os.Getenv("OME_RUN_LAYER_HUNT_ONLY")
	var configs []runConfig
	for _, cfg := range runConfigs(full) {
		if keep(cfg) && strings.Contains(cfg.name, only) {
			configs = append(configs, cfg)
		}
	}
	started := time.Now()
	summary := &runSummary{totals: map[string]int{}, configsWith: map[string]int{}, examples: map[string][]runExample{}}
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < runWorkers(); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				cfg := configs[n]
				w := newRunWorld(t, cfg)
				rep := w.explore(maxStates, maxDepth)
				w.randomRuns(&rep, 1, plays, 4*maxDepth)
				summary.add(cfg, w, rep)
				if full {
					var parts []string
					for class, c := range rep.counts {
						parts = append(parts, fmt.Sprintf("%s=%d", class, c))
					}
					sort.Strings(parts)
					t.Logf("config %d/%d [%s]: %d states, depth %d: %s", n+1, len(configs), cfg.name, rep.states, rep.depth, strings.Join(parts, " "))
				}
			}
		}()
	}
	for n := range configs {
		jobs <- n
	}
	close(jobs)
	wg.Wait()
	classes := make([]string, 0, len(summary.totals))
	for class := range summary.totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d random runs, %d gate questions, %d run passes, %s", len(configs), summary.states, summary.plays, summary.gateCalls, summary.runPasses, time.Since(started).Round(time.Second))
	for _, class := range classes {
		if known, ok := runKnownClasses[class]; ok {
			t.Logf("%s: %d states across %d configurations: %s", class, summary.totals[class], summary.configsWith[class], known)
		} else {
			t.Logf("%s: %d states across %d configurations", class, summary.totals[class], summary.configsWith[class])
		}
		for _, ex := range summary.examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg.name, ex.finding.message, runPath(ex.finding.state), ex.finding.state)
		}
		if _, ok := runKnownClasses[class]; !ok {
			t.Errorf("%s: %d states (first: %s: %s)", class, summary.totals[class], summary.examples[class][0].cfg.name, summary.examples[class][0].finding.message)
		}
	}
	for class := range runKnownClasses {
		if summary.totals[class] == 0 {
			t.Logf("known class %s: no configuration reached it in this mode", class)
		}
	}
}

// TestRunLayer_PrefillDecodeGroupsReachTheirEndWithTheRunClosed explores
// every group of the engine and the decoder: the reference shapes and two
// small ones, single pods and gangs, rollingUpdate at the group budget and
// blueGreen, both strategies and the mixed pairs, every setting, with a
// partition on either side or none, with and without a scaling policy;
// failures, lost pods, gangs and gang members, spent retries and their
// release, scales of one side and of both, partition edits, a pause and a
// resume, a second push, a push back to version 1, the scaling policy, the
// ratio rule and the group itself added or removed thrown in.
func TestRunLayer_PrefillDecodeGroupsReachTheirEndWithTheRunClosed(t *testing.T) {
	runExplore(t, func(c runConfig) bool { return len(c.members) == 2 })
}

// TestRunLayer_GroupsWithARouterReachTheirEndWithTheRunClosed explores the
// same space with a router of two as the group's third member.
func TestRunLayer_GroupsWithARouterReachTheirEndWithTheRunClosed(t *testing.T) {
	runExplore(t, func(c runConfig) bool { return len(c.members) == 3 })
}

// runParseMove reads one move back from the words runMove.String prints.
func runParseMove(s string) (runMove, error) {
	s = strings.TrimSpace(s)
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return runMove{}, fmt.Errorf("empty move")
	}
	switch fields[0] {
	case "push", "rollback", "pause", "resume", "project", "project-between", "policy-on", "policy-off", "rule-on", "rule-off", "group-off", "group-on":
		if len(fields) == 1 {
			return runMove{kind: fields[0]}, nil
		}
	case "scale":
		if len(fields) == 5 && fields[1] == "both" {
			v, err := strconv.Atoi(fields[4])
			return runMove{kind: "scale-both", value: int32(v)}, err
		}
	}
	var mv runMove
	if n, err := fmt.Sscanf(s, "%s member %d to %d", &mv.kind, &mv.member, &mv.value); err == nil && n == 3 {
		return mv, nil
	}
	if n, err := fmt.Sscanf(s, "%s member %d row %d", &mv.kind, &mv.member, &mv.idx); err == nil && n == 3 {
		return mv, nil
	}
	if n, err := fmt.Sscanf(s, "%s member %d", &mv.kind, &mv.member); err == nil && n == 2 {
		return mv, nil
	}
	return runMove{}, fmt.Errorf("cannot read move %q", s)
}

// TestRunLayer_ReplayAPath replays a printed path on the configuration
// OME_RUN_LAYER_HUNT_ONLY names (OME_RUN_LAYER_HUNT_PATH holds the moves
// joined by " -> ") and prints every surface after each move: the state,
// the holds, the run, the plan condition and the group entry.
func TestRunLayer_ReplayAPath(t *testing.T) {
	only, path := os.Getenv("OME_RUN_LAYER_HUNT_ONLY"), os.Getenv("OME_RUN_LAYER_HUNT_PATH")
	if only == "" {
		t.Skip("set OME_RUN_LAYER_HUNT_ONLY to a configuration name and OME_RUN_LAYER_HUNT_PATH to a path")
	}
	full := os.Getenv("OME_RUN_LAYER_HUNT") == "full"
	var cfg *runConfig
	for _, c := range runConfigs(full) {
		if c.name == only {
			c := c
			cfg = &c
			break
		}
	}
	if cfg == nil {
		t.Fatalf("no configuration named %q", only)
	}
	w := newRunWorld(t, *cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	t.Logf("start: %s", st)
	if path != "" {
		for _, s := range strings.Split(path, "->") {
			mv, err := runParseMove(s)
			if err != nil {
				t.Fatal(err)
			}
			st = w.apply(st, mv)
			t.Logf("after %s: %s", mv, st)
			for _, f := range w.check(st) {
				t.Logf("  %s: %s", f.class, f.message)
			}
		}
	}
	t.Logf("complete=%t progress moves=%v", w.complete(st), w.progressMoves(st))
}

// projectAll lets the projector reach every member when it trails the spec.
func (w *runWorld) projectAll(st *runState) *runState {
	for _, mv := range w.progressMoves(st) {
		if mv.kind == "project" {
			return w.apply(st, mv)
		}
	}
	return st
}

// serveOne brings one member's first started, created or rebuilt Instance
// up, the projector reaching every member first.
func (w *runWorld) serveOne(st *runState, member int) *runState {
	st = w.projectAll(st)
	for _, mv := range w.progressMoves(st) {
		if mv.kind == "serve" && mv.member == member {
			return w.apply(st, mv)
		}
	}
	w.t.Fatalf("%s has nothing to serve: %s", w.cfg.members[member].comp, st)
	return st
}

// serveAll brings every started, created or rebuilt Instance up, the
// projector reaching every member first and a backoff ending when it is
// the only move left, until nothing is in flight; a pause is left standing.
func (w *runWorld) serveAll(st *runState) *runState {
	for round := 0; round < 1024; round++ {
		var next *runMove
		for _, mv := range w.progressMoves(st) {
			if mv.kind == "resume" || mv.kind == "project-between" {
				continue
			}
			mv := mv
			next = &mv
			break
		}
		if next == nil {
			return st
		}
		st = w.apply(st, *next)
	}
	w.t.Fatalf("the roll did not settle in 1024 moves: %s", st)
	return st
}

// readyOn counts a member's Instances Ready on rev.
func (m runMember) readyOn(rev int8) int32 {
	var n int32
	for _, r := range m.rows {
		if r.kind == runReady && r.rev == rev {
			n++
		}
	}
	return n
}

// TestRunLayer_APartitionHoldingAWholeSideKeepsTheRunOpen records the current
// behaviour. 12 prefill and 4 decode under RecreatePod at 10 percent with
// the keepRatio rule; the operator sets the decoder's partition to 4 and
// pushes version 2. The prefill side rolls, the decoder stays on version 1
// as asked, and the run never closes: the group machine reads a member
// with no new-revision pod as Surging and never Staged, and the run's
// close reads a member off the target only through a Staged group. A
// further push replaces the run with a newer one and the new run stays open
// the same way; clearing the partition rolls the decoder and closes the
// run Completed. The invariant a fix restores: a run closes once every member
// rests at the shape its spec asks for, partition included.
func TestRunLayer_APartitionHoldingAWholeSideKeepsTheRunOpen(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	cfg := runPair([2]int32{12, 4}, recreate, recreate, "10%", true, runSettingNamed("keepRatio=20"), [2]int32{0, 4}, 3, runDefaultBudgets(false))
	w := newRunWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.serveAll(st)
	if !w.membersComplete(st) {
		t.Fatalf("the members did not reach the shape the spec asks for: %s", st)
	}
	if st.activeRun() == nil || st.groupPhase() != v1beta1.CoordinationPhaseSurging {
		t.Fatalf("the run stays open with the group reading Surging; got %s", st)
	}
	if hold := st.holds[0]; hold != nil {
		t.Fatalf("the prefill side rests with no hold; got %s", hold)
	}
	// A further push replaces the run, and the new run stays open the same
	// way once the prefill side has rolled again.
	st = w.serveAll(w.apply(st, runMove{kind: "push"}))
	if st.lastRunOutcome() != v1beta1.RolloutRunSuperseded || st.activeRun() == nil || !w.membersComplete(st) {
		t.Fatalf("the push replaces the run with a newer one and the new run stays open; got %s", st)
	}
	// Clearing the partition rolls the decoder and closes the run.
	st = w.serveAll(w.apply(st, runMove{kind: "partition", member: 1, value: 0}))
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("clearing the partition closes the run Completed; got %s", st)
	}
}

// TestRunLayer_AFinishedMemberWithAPartitionLeavesTheGroupStagedAndTheRunClosed
// records the reading of a member done with its roll whose spec carries a
// partition. 12 prefill with a partition of 2 and 8 decode under RecreatePod
// at 10 percent, no ratio rule, in one blueGreen group and in one
// rollingUpdate group. The operator pushes version 2; the prefill side
// moves 10 Instances and rests at its partition; while the decoder's last
// Instance is still moving the operator sets the decoder's partition to 8,
// so that no later push moves a decoder. The decoder's last Instance
// serves and every decoder runs version 2. Under both strategies a member
// with no roll in flight is left out of the group's Staged reading,
// whatever partition its spec carries, so the group reads Staged on the
// prefill side's rest and the run closes Completed.
func TestRunLayer_AFinishedMemberWithAPartitionLeavesTheGroupStagedAndTheRunClosed(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	for _, rollingUpdate := range []bool{false, true} {
		cfg := runPair([2]int32{12, 8}, recreate, recreate, "10%", rollingUpdate, runSettingNamed("off"), [2]int32{2, 0}, 3, runDefaultBudgets(false))
		w := newRunWorld(t, cfg)
		st := w.initial()
		w.settle(st, w.order(0))
		for round := 0; ; round++ {
			if round > 200 {
				t.Fatalf("[%s] the roll did not reach the picture: %s", cfg.name, st)
			}
			st = w.projectAll(st)
			engine, decoder := &st.members[0], &st.members[1]
			if w.finishedFor(engine, 2) && !engine.inFlight() && decoder.readyOn(2) == 7 && decoder.inFlight() {
				break
			}
			switch {
			case engine.inFlight():
				st = w.serveOne(st, 0)
			case decoder.inFlight() && decoder.readyOn(2) < 7:
				st = w.serveOne(st, 1)
			default:
				t.Fatalf("[%s] nothing to serve toward the picture: %s", cfg.name, st)
			}
		}
		st = w.serveAll(w.apply(st, runMove{kind: "partition", member: 1, value: 8}))
		if !w.membersComplete(st) || st.members[1].readyOn(2) != 8 {
			t.Fatalf("[%s] the members did not reach the shape the spec asks for: %s", cfg.name, st)
		}
		if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted || st.groupPhase() != v1beta1.CoordinationPhaseStaged {
			t.Fatalf("[%s] the group reads Staged and the run closes Completed once every decoder runs version 2 under a partition of 8; got %s", cfg.name, st)
		}
	}
}

// TestRunLayer_AScaleUpOfAGroupAtRestOpensARun records a reading not decided
// yet: whether a scale-up opens a run at all. 12 prefill and 8 decode under
// RecreatePod at 10 percent in one rollingUpdate group roll to version 2
// and the run closes Completed. The operator then scales the decoder to 9:
// one run opens for the new Instance, because the run layer reads an
// Instance with no running revision as one trailing the target, stays open
// until the Instance serves, and closes Completed again. On a group resting
// at its partition (2 prefill kept on version 1) the same scale-up opens
// the same one run: the Staged group excuses only the member resting at
// its partition, never the decoder, so the run neither closes early nor
// opens again while the Instance is created.
func TestRunLayer_AScaleUpOfAGroupAtRestOpensARun(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	for _, partition := range []int32{0, 2} {
		cfg := runPair([2]int32{12, 8}, recreate, recreate, "10%", true, runSettingNamed("off"), [2]int32{partition, 0}, 3, runDefaultBudgets(false))
		w := newRunWorld(t, cfg)
		st := w.initial()
		w.settle(st, w.order(0))
		st = w.serveAll(st)
		if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
			t.Fatalf("[%s] the roll did not end with the run closed: %s", cfg.name, st)
		}
		first := st.service.Status.Rollout.LastRun.OpenedAt
		st = w.projectAll(w.apply(st, runMove{kind: "scale", member: 1, value: 9}))
		if st.members[1].count(runCreating) != 1 {
			t.Fatalf("[%s] the scale-up's Instance is being created; got %s", cfg.name, st)
		}
		opensAndCloses := false
		for _, e := range st.events {
			opensAndCloses = opensAndCloses || e.class == "run-opens-and-closes-each-pass-on-a-staged-group"
		}
		if st.activeRun() == nil || opensAndCloses {
			t.Fatalf("[%s] one run opens for the scale-up's Instance and stays open until it serves; got %s", cfg.name, st)
		}
		st = w.serveAll(st)
		if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted || st.service.Status.Rollout.LastRun.OpenedAt.Equal(first) {
			t.Fatalf("[%s] the run closes Completed once the new Instance serves; got %s", cfg.name, st)
		}
	}
}

// TestRunLayer_ARetryRecordOfARetiredAttemptKeepsTheRunOpenUntilItIsRemoved
// records the close on the path the explorer found. 15 prefill with a
// partition of 2 and 15 decode under RecreatePod at 10 percent in one
// rollingUpdate group; the operator raises the prefill partition to 3 and
// pushes version 2. A decoder attempt fails and its retry record goes into
// backoff; the operator scales both sides down to 8, which retires the
// failed decoder. Every decoder left runs version 2, the record keeps the
// decoder's current revision at version 1 until it outlives its window,
// the prefill side rests at its partition and the group reads Staged. The
// Staged group excuses only the prefill side, so the run stays open on
// every pass until the record is removed; then it closes Completed and
// stays closed.
func TestRunLayer_ARetryRecordOfARetiredAttemptKeepsTheRunOpenUntilItIsRemoved(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	cfg := runPair([2]int32{15, 15}, recreate, recreate, "10%", true, runSettingNamed("off"), [2]int32{2, 0}, 3, runDefaultBudgets(false))
	w := newRunWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	path := "partition member 0 to 3 -> project-between -> fail member 1 row 0 -> serve member 1 row 1 -> serve member 1 row 0 -> serve member 0 row 3 -> serve member 0 row 4 -> serve member 0 row 5 -> serve member 1 row 3 -> serve member 0 row 6 -> serve member 1 row 2 -> serve member 0 row 8 -> serve member 1 row 4 -> serve member 0 row 7 -> serve member 1 row 5 -> serve member 1 row 7 -> serve member 0 row 10 -> serve member 1 row 6 -> serve member 0 row 9 -> fail member 1 row 8 -> backoff member 1 -> scale both sides by -1 -> project-between"
	for _, s := range strings.Split(path, "->") {
		mv, err := runParseMove(s)
		if err != nil {
			t.Fatal(err)
		}
		st = w.apply(st, mv)
	}
	decoder := &st.members[1]
	if decoder.retry == runRetryNone || decoder.inFlight() || decoder.readyOn(2) != int32(len(decoder.rows)) || decoder.current != 1 {
		t.Fatalf("the picture: every decoder runs version 2 under a retry record that keeps its current revision at version 1; got %s", st)
	}
	if st.groupPhase() != v1beta1.CoordinationPhaseStaged {
		t.Fatalf("the group reads Staged on the prefill side's partition; got %s", st)
	}
	opensAndCloses := false
	for _, e := range st.events {
		opensAndCloses = opensAndCloses || e.class == "run-opens-and-closes-each-pass-on-a-staged-group"
	}
	if st.activeRun() == nil || opensAndCloses {
		t.Fatalf("the run stays open while the record keeps the decoder's current revision at version 1; got %s", st)
	}
	st = w.serveAll(w.apply(st, runMove{kind: "prune", member: 1}))
	if !w.complete(st) || st.lastRunOutcome() != v1beta1.RolloutRunCompleted {
		t.Fatalf("once the record is removed the run closes Completed and stays closed; got %s", st)
	}
}
