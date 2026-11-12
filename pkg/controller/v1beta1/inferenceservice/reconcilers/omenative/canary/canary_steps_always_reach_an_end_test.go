package canary

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils"
)

// This file drives a canary run through the production code the controller
// runs in one pass, in the controller's order: the run layer
// (rolloutrun.Reconcile and BindRun), the partition the projector writes for
// each member (PartitionResolver), and the canary dispatch (Dispatch) over a
// fake API that holds the replicas, their pods and their revisions. The
// workload engine is a small model: it holds the Instances the partition
// names, starts the others toward the target under the member's strategy and
// budget, follows a rollback target, and does nothing new while the rollout
// is paused. Between passes the world makes one move: a building Instance
// serves, a pod is lost and rebuilt, the clock passes a deadline, a status
// write is lost, or the operator acts (promote, pause, resume, roll back,
// scale, push a new revision, edit or remove the step list). Every path is
// explored breadth-first to a bound with a hash of visited states, and seeded
// random runs go deeper. At every state the model checks that the run can
// still reach an end, that traffic names only revisions that serve, that the
// held share of the fleet is what the step asks for, and that a run closes
// once. OME_CANARY_HUNT=full widens the sample and the bounds.

const (
	gateNamespace = "ns"
	gateService   = "svc"
)

// gateRevisions are the revision hashes the model knows: the one the fleet
// stands on, the one the operator pushes, and a later push.
var gateRevisions = []string{"r1", "r2", "r3"}

func gateRevisionName(comp v1beta1.ComponentType, hash string) string {
	return gateService + "-" + string(comp) + "-" + hash
}

// gateRowKind is what the roll has done to one Instance.
type gateRowKind uint8

const (
	gateRowServing    gateRowKind = iota // every pod Ready on rev
	gateRowLost                          // the pods are gone; the workload rebuilds them on rev
	gateRowRecreating                    // the old pods are gone; new pods build toward target
	gateRowSurging                       // the old pods serve while a replacement builds toward target
	gateRowCreating                      // a new Instance builds toward target
	gateRowShort                         // a gang short of one pod: the rest serve while the workload rebuilds it on rev
)

var gateRowKindNames = [...]string{"", "lost", "recreate", "surge", "create", "short"}

type gateRow struct {
	kind   gateRowKind
	rev    string // the revision the Instance stands on; empty for a new Instance
	target string // the revision an attempt builds toward
}

func (r gateRow) inFlight() bool {
	return r.kind == gateRowRecreating || r.kind == gateRowSurging || r.kind == gateRowCreating
}

// serving reports pods of the Instance in rotation: a serving row, or the
// old pods of a surging one.
func (r gateRow) serving() bool { return r.kind == gateRowServing || r.kind == gateRowSurging }

func (r gateRow) String() string {
	s := r.rev
	if s == "" {
		s = "-"
	}
	if r.kind != gateRowServing {
		s += ":" + gateRowKindNames[r.kind]
	}
	if r.target != "" {
		s += ">" + r.target
	}
	return s
}

// gateMember is one Component of the unit: its size, how it rolls, its own
// budget, its pods per Instance, and an operator partition beside the canary.
type gateMember struct {
	comp      v1beta1.ComponentType
	desired   int32
	strategy  v1beta1.UpdateStrategyType
	budget    string
	gang      int32
	partition *int32
}

func (m gateMember) surges() bool { return m.strategy == v1beta1.UpdateStrategySurgeThenDrain }

func (m gateMember) String() string {
	short := "recreate"
	if m.surges() {
		short = "surge"
	}
	s := fmt.Sprintf("%s=%d/%s/%s", m.comp, m.desired, short, m.budget)
	if m.gang > 1 {
		s += fmt.Sprintf("/gang%d", m.gang)
	}
	if m.partition != nil {
		s += fmt.Sprintf("/partition%d", *m.partition)
	}
	return s
}

// gateMemberState is one member's replica as the world holds it.
type gateMemberState struct {
	rows       []gateRow
	desired    int32
	updateRev  string // the spec's revision
	current    string // the revision the rollup publishes as current
	rollbackTo string // spec.pacing.rollbackToRevision as last written by the dispatch
	partition  *int32 // spec.pacing.partition as last projected
	generation int64
}

func (m gateMemberState) String() string {
	parts := make([]string, 0, len(m.rows))
	for _, r := range m.rows {
		parts = append(parts, r.String())
	}
	s := strings.Join(parts, " ")
	if m.partition != nil {
		s += fmt.Sprintf(" partition=%d", *m.partition)
	}
	if m.rollbackTo != "" {
		s += " rollbackTo=" + query.RevisionFromName(m.rollbackTo).Hash()
	}
	return s
}

// gateBudgets is how many times the world may still do each costly thing in
// one run.
type gateBudgets struct {
	lostPods, lostFlushes, scales, pushes, edits, removals, rollbacks, pauses, forces, resumes int8
	// nodeLosses is nodes lost, each taking one Instance of every member;
	// memberLosses is gang members lost; resizes is resizes of one side,
	// both sides or the ratio; analysisFails and analysisGaps are the
	// answers of the metrics source that breach or go unreadable.
	nodeLosses, memberLosses, resizes, analysisFails, analysisGaps int8
}

// gateConfig is one configuration under exploration.
type gateConfig struct {
	name           string
	stepsName      string
	members        []gateMember
	steps          []v1beta1.RolloutGroupStep
	scaleDownDelay int32
	budgets        gateBudgets
	// follower makes the decoder's count follow the engine's under a
	// proportional scaling policy, written beside the engine's resize as
	// the scaling coordinator writes it.
	follower bool
	// rolling puts the members under one rollingUpdate group with no
	// canary, so coordination owns their revision records.
	rolling bool
}

func (c gateConfig) String() string {
	parts := make([]string, 0, len(c.members))
	for _, m := range c.members {
		parts = append(parts, m.String())
	}
	s := fmt.Sprintf("%s steps=%s drain=%ds", strings.Join(parts, " "), c.stepsName, c.scaleDownDelay)
	if c.follower {
		s += " follower=decoder"
	}
	return s
}

// gateMove is one thing the world or the operator does between two passes.
type gateMove struct {
	kind   string
	member int
	idx    int
	delta  int32
}

func (m gateMove) String() string {
	switch m.kind {
	case "serve", "podloss", "memberloss":
		return fmt.Sprintf("%s member %d row %d", m.kind, m.member, m.idx)
	case "scale", "resize":
		return fmt.Sprintf("%s member %d by %+d", m.kind, m.member, m.delta)
	case "nodeloss":
		return gateNodeName(m.idx) + " lost"
	}
	return m.kind
}

// gateState is the model at one moment: the service as the controller holds
// it in memory, every member's replica, the clock, the budgets left, and the
// moves that led here.
type gateState struct {
	isvc             *v1beta1.InferenceService
	members          []gateMemberState
	now              time.Time
	budgets          gateBudgets
	lostFlushPending bool
	closes           map[string]int
	scaled           bool
	resumed          bool
	pushed           bool
	removed          bool
	unsettled        bool
	reopened         string
	// reusedRun is a run id opened again after a run with that id closed;
	// it is read at the state where it happened and not carried on.
	reusedRun string
	path      []gateMove
	// sample is the standing answer of the metrics source.
	sample gateSample
	// passEvents are the events the last pass recorded, the hold text an
	// operator reads beside the status.
	passEvents []string
	// history is the revision model's part of the state: each member's
	// ControllerRevisions and replica identity; nil for the canary model.
	history *gateHistory
}

func (st *gateState) clone() *gateState {
	out := &gateState{
		isvc:             st.isvc.DeepCopy(),
		members:          make([]gateMemberState, len(st.members)),
		now:              st.now,
		budgets:          st.budgets,
		lostFlushPending: st.lostFlushPending,
		closes:           make(map[string]int, len(st.closes)),
		scaled:           st.scaled,
		resumed:          st.resumed,
		pushed:           st.pushed,
		removed:          st.removed,
		reopened:         st.reopened,
		path:             append([]gateMove(nil), st.path...),
		sample:           st.sample,
		passEvents:       append([]string(nil), st.passEvents...),
		history:          st.history.clone(),
	}
	for i, m := range st.members {
		m.rows = append([]gateRow(nil), m.rows...)
		if m.partition != nil {
			m.partition = ptr.To(*m.partition)
		}
		out.members[i] = m
	}
	for k, v := range st.closes {
		out.closes[k] = v
	}
	return out
}

func (st *gateState) String() string {
	var parts []string
	for i, m := range st.members {
		parts = append(parts, fmt.Sprintf("member %d [%s]", i, m))
	}
	return strings.Join(parts, " | ")
}

// gateWorld builds the objects the production code reads from a state and
// runs the controller's pass over them.
type gateWorld struct {
	t      *testing.T
	cfg    gateConfig
	ctx    context.Context
	scheme *runtime.Scheme
	// passes counts controller passes; dispatches the canary dispatches in them.
	passes, dispatches int
	// ext is what another model adds to this world; nil for the canary model.
	ext *gateExtension
}

func newGateWorld(t *testing.T, cfg gateConfig, scheme *runtime.Scheme) *gateWorld {
	return &gateWorld{t: t, cfg: cfg, ctx: context.Background(), scheme: scheme}
}

// gateFailure is an error the production code returned inside a pass; the
// exploration records it as a finding of its own.
type gateFailure string

func (w *gateWorld) fail(format string, args ...any) {
	panic(gateFailure(fmt.Sprintf(format, args...)))
}

// gateISVC is the service: every member declared with its count, and one
// canary group over the members.
func gateISVC(cfg gateConfig) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: gateService, Namespace: gateNamespace, Generation: 1}}
	comps := make([]v1beta1.ComponentType, 0, len(cfg.members))
	for _, m := range cfg.members {
		comps = append(comps, m.comp)
		ext := v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(m.desired))}
		switch m.comp {
		case v1beta1.EngineComponent:
			isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: ext}
		case v1beta1.DecoderComponent:
			isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: ext}
		case v1beta1.RouterComponent:
			isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: ext}
		}
	}
	group := v1beta1.RolloutGroup{Components: comps}
	if cfg.rolling {
		group.RollingUpdate = &v1beta1.GroupRollingUpdate{}
	} else {
		canary := &v1beta1.GroupCanary{Steps: append([]v1beta1.RolloutGroupStep(nil), cfg.steps...)}
		if cfg.scaleDownDelay > 0 {
			canary.ScaleDownDelaySeconds = ptr.To(cfg.scaleDownDelay)
		}
		group.Canary = canary
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}}
	if cfg.follower {
		gateFollowerPolicy(isvc, cfg)
	}
	return isvc
}

// initial is the moment after the operator pushed the second revision: every
// Instance serves the first one, and every replica's target is the second.
func (w *gateWorld) initial() *gateState {
	st := &gateState{
		isvc:    gateISVC(w.cfg),
		members: make([]gateMemberState, len(w.cfg.members)),
		now:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		budgets: w.cfg.budgets,
		closes:  map[string]int{},
	}
	for i, m := range w.cfg.members {
		rows := make([]gateRow, m.desired)
		for idx := range rows {
			rows[idx] = gateRow{kind: gateRowServing, rev: gateRevisions[0]}
		}
		st.members[i] = gateMemberState{rows: rows, desired: m.desired, updateRev: gateRevisions[1], current: gateRevisions[0], generation: 1}
	}
	if w.ext != nil && w.ext.initial != nil {
		w.ext.initial(w, st)
	}
	return st
}

func gateRunners(gang int32) []v1beta1.Runner {
	if gang <= 1 {
		return []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	}
	return []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: gang - 1}}
}

// rollup publishes each member's current revision the way the replica's
// status does: the revision every Instance serves, once they all serve one.
func (w *gateWorld) rollup(st *gateState) {
	if w.ext != nil && w.ext.rollup != nil {
		w.ext.rollup(w, st)
		return
	}
	for i := range st.members {
		m := &st.members[i]
		shared := ""
		for _, r := range m.rows {
			if r.kind != gateRowServing || (shared != "" && r.rev != shared) {
				shared = ""
				break
			}
			shared = r.rev
		}
		if shared != "" {
			m.current = shared
		}
	}
}

// replica is the InferenceReplica a member publishes.
func (w *gateWorld) replica(st *gateState, i int) *v1beta1.InferenceReplica {
	cfg := w.cfg.members[i]
	m := st.members[i]
	r := ir(gateNamespace, gateService, cfg.comp, m.updateRev)
	r.Generation = m.generation
	r.Spec.Replicas = ptr.To(m.desired)
	r.Spec.Runners = gateRunners(cfg.gang)
	rolling := &v1beta1.RollingUpdate{Partition: cfg.partition}
	budget := intstr.Parse(cfg.budget)
	if cfg.surges() {
		rolling.MaxSurge = &budget
	} else {
		rolling.MaxUnavailable = &budget
	}
	r.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: cfg.strategy, RollingUpdate: rolling}}
	if m.partition != nil || m.rollbackTo != "" {
		r.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: m.partition}
		if m.rollbackTo != "" {
			r.Spec.Pacing.RollbackToRevision = ptr.To(m.rollbackTo)
		}
	}
	r.Status.ObservedGeneration = m.generation
	if m.current != "" {
		r.Status.CurrentRevision = gateRevisionName(cfg.comp, m.current)
	}
	r.Status.UpdateRevision = gateRevisionName(cfg.comp, m.updateRev)
	r.Status.Replicas = int32(len(m.rows))
	attempt := func(step string) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: step}
	}
	var ready, updated int32
	for idx, row := range m.rows {
		rs := v1beta1.OMENativeInstanceStatus{Index: int32(idx)}
		switch row.kind {
		case gateRowServing:
			rs.Phase, rs.RunningRevision = v1beta1.OMENativeInstanceReady, gateRevisionName(cfg.comp, row.rev)
			rs.PodCount, rs.ReadyPodCount, rs.ServingPodCount, rs.AvailablePodCount = cfg.gang, cfg.gang, cfg.gang, cfg.gang
		case gateRowLost:
			rs.Phase, rs.RunningRevision = v1beta1.OMENativeInstanceRestarting, gateRevisionName(cfg.comp, row.rev)
			rs.Operation = &v1beta1.InstanceOperation{ID: "repair", Type: v1beta1.InstanceOperationRestart}
		case gateRowRecreating:
			rs.Phase, rs.RunningRevision, rs.TargetRevision = v1beta1.OMENativeInstanceUpdating, gateRevisionName(cfg.comp, row.rev), gateRevisionName(cfg.comp, row.target)
			rs.PodCount, rs.Operation = cfg.gang, attempt(workloadtypes.UpdateStepDrain)
		case gateRowSurging:
			rs.Phase, rs.RunningRevision, rs.TargetRevision = v1beta1.OMENativeInstanceUpdating, gateRevisionName(cfg.comp, row.rev), gateRevisionName(cfg.comp, row.target)
			rs.PodCount, rs.ReadyPodCount, rs.ServingPodCount, rs.AvailablePodCount = 2*cfg.gang, cfg.gang, cfg.gang, cfg.gang
			rs.Operation = attempt(workloadtypes.UpdateStepSurge)
		case gateRowCreating:
			rs.Phase, rs.TargetRevision, rs.PodCount = v1beta1.OMENativeInstanceCreating, gateRevisionName(cfg.comp, row.target), cfg.gang
			rs.Operation = &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate}
		case gateRowShort:
			rs.Phase, rs.RunningRevision = v1beta1.OMENativeInstanceRestarting, gateRevisionName(cfg.comp, row.rev)
			rs.PodCount, rs.ReadyPodCount, rs.ServingPodCount, rs.AvailablePodCount = cfg.gang, cfg.gang-1, cfg.gang-1, cfg.gang-1
			rs.Operation = &v1beta1.InstanceOperation{ID: "repair", Type: v1beta1.InstanceOperationRestart}
		}
		if row.serving() {
			ready++
		}
		if row.kind == gateRowServing && row.rev == m.updateRev {
			updated++
		}
		r.Status.InstanceStatuses = append(r.Status.InstanceStatuses, rs)
	}
	r.Status.ReadyReplicas, r.Status.ServingReplicas, r.Status.AvailableReplicas, r.Status.UpdatedReplicas = ready, ready, ready, updated
	return r
}

// gateInstancePods are the pods of one Instance as the kubelet reports
// them: count pods of a gang-sized Instance on one revision, named after
// the service, the Component, the Instance index and the runner, Ready or
// not; the suffix tells a replacement's pods from the old ones.
func gateInstancePods(ns, service string, comp v1beta1.ComponentType, idx int, gang, count int32, rev, suffix string, ready bool) []runtime.Object {
	var out []runtime.Object
	for ordinal := int32(0); ordinal < count; ordinal++ {
		runner := v1beta1.RunnerNameDefault
		if gang > 1 {
			runner = v1beta1.RunnerNameWorker
			if ordinal == 0 {
				runner = v1beta1.RunnerNameLeader
			}
		}
		name := fmt.Sprintf("%s-%s-%d-%s-%d%s", service, comp, idx, runner, ordinal, suffix)
		pod := canaryRunnerPod(ns, service, string(comp), rev, name, runner, strconv.Itoa(int(ordinal)))
		pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(idx)
		if !ready {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
		}
		out = append(out, pod)
	}
	return out
}

// pods are one Instance's pods as the kubelet reports them.
func (w *gateWorld) pods(i, idx int, row gateRow) []runtime.Object {
	cfg := w.cfg.members[i]
	var out []runtime.Object
	add := func(rev, suffix string, ready bool, count int32) {
		out = append(out, gateInstancePods(gateNamespace, gateService, cfg.comp, idx, cfg.gang, count, rev, suffix, ready)...)
	}
	switch row.kind {
	case gateRowServing:
		add(row.rev, "", true, cfg.gang)
	case gateRowRecreating, gateRowCreating:
		add(row.target, "", false, cfg.gang)
	case gateRowSurging:
		add(row.rev, "", true, cfg.gang)
		add(row.target, "-s", false, cfg.gang)
	case gateRowShort:
		add(row.rev, "", true, cfg.gang-1)
	}
	return out
}

// objects are everything the pass reads: the service, each member's replica
// and pods, and every revision of every member.
func (w *gateWorld) objects(st *gateState) []runtime.Object {
	if w.ext != nil && w.ext.objects != nil {
		return w.ext.objects(w, st)
	}
	objs := []runtime.Object{st.isvc.DeepCopy()}
	for i, cfg := range w.cfg.members {
		objs = append(objs, w.replica(st, i))
		for idx, row := range st.members[i].rows {
			objs = append(objs, w.pods(i, idx, row)...)
		}
		for n, hash := range gateRevisions {
			objs = append(objs, canaryControllerRevision(gateNamespace, gateService, string(cfg.comp), hash, int64(n+1)))
		}
	}
	return objs
}

func (w *gateWorld) client(st *gateState) client.Client {
	return fake.NewClientBuilder().WithScheme(w.scheme).WithRuntimeObjects(w.objects(st)...).Build()
}

// workloadStep is the workload engine's reaction to the partition and the
// rollback target its replica carries: the lowest-indexed Instances off the
// target are held, the partition's worth of them, and the rest start toward
// the target under the member's strategy and budget. A paused rollout starts
// nothing new. An attempt toward a revision that is no longer the target
// turns toward the target.
func (w *gateWorld) workloadStep(st *gateState, i int) {
	cfg := w.cfg.members[i]
	m := &st.members[i]
	target := w.buildTarget(st, i)
	var partition int32
	switch {
	case m.partition != nil:
		partition = *m.partition
	case cfg.partition != nil:
		partition = *cfg.partition
	}
	held := map[int]bool{}
	var holding int32
	for idx := range m.rows {
		r := &m.rows[idx]
		if r.inFlight() && r.target != target {
			r.target = target
		}
		if r.inFlight() || r.rev == target {
			continue
		}
		if holding < partition {
			held[idx] = true
			holding++
		}
	}
	if paused, _ := constants.RolloutPauseState(st.isvc.Annotations); paused {
		return
	}
	budget := utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(cfg.budget)), int32(len(m.rows)), true)
	var inFlight int32
	for _, r := range m.rows {
		switch {
		case cfg.surges() && r.kind == gateRowSurging:
			inFlight++
		case !cfg.surges() && (r.kind == gateRowRecreating || r.kind == gateRowLost || r.kind == gateRowShort):
			inFlight++
		}
	}
	for idx := range m.rows {
		r := &m.rows[idx]
		if held[idx] || r.kind != gateRowServing || r.rev == target {
			continue
		}
		if inFlight >= budget {
			break
		}
		r.kind = gateRowRecreating
		if cfg.surges() {
			r.kind = gateRowSurging
		}
		r.target = target
		inFlight++
	}
}

// buildTarget is the revision a member's workload builds toward: the
// rollback target its replica carries, resolved against the replica's
// history when the extension holds one (a target outside the history
// leaves the workload on the spec), else the spec.
func (w *gateWorld) buildTarget(st *gateState, i int) string {
	m := st.members[i]
	if m.rollbackTo == "" {
		return m.updateRev
	}
	if w.ext != nil && w.ext.rollbackTarget != nil {
		if hash := w.ext.rollbackTarget(w, st, i); hash != "" {
			return hash
		}
		return m.updateRev
	}
	return query.RevisionFromName(m.rollbackTo).Hash()
}

// extraMoves are the extension's moves, none for the canary model.
func (w *gateWorld) extraMoves(st *gateState) []gateMove {
	if w.ext == nil || w.ext.moves == nil {
		return nil
	}
	return w.ext.moves(w, st)
}

// activeRunID is the open run's id, empty when none is open.
func activeRunID(isvc *v1beta1.InferenceService) string {
	if isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return ""
	}
	return isvc.Status.Rollout.ActiveRun.RunID
}

// targetsOf is a run's targets as one string, for the reopen check.
func targetsOf(targets []v1beta1.RolloutRunTarget) string {
	parts := make([]string, 0, len(targets))
	for _, t := range targets {
		parts = append(parts, string(t.Component)+"="+t.Revision)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// pass is one controller pass: the run layer, the binding of a freshly
// opened run, the partition each member is projected with and the workload's
// reaction to it, then the canary dispatch of every canary group, and the
// read-back of the rollback target the dispatch wrote on each replica. A
// pass whose status write is lost keeps only the run boundary, which the
// controller writes before anything else, and the replica writes.
func (w *gateWorld) pass(st *gateState) {
	w.rollup(st)
	isvc := st.isvc
	c := w.client(st)
	before := activeRunID(isvc)
	events := &gateEvents{}
	sampler := &gateSampler{st: st}
	out, err := rolloutrun.Reconcile(w.ctx, rolloutrun.Inputs{Client: c, Reader: c, Recorder: events, ISVC: isvc, Now: st.now})
	if err != nil {
		w.fail("run layer: %v (state %s)", err, st)
	}
	w.passes++
	policies := out.Policies
	if out.Opened {
		for _, g := range rollout.CanaryGroups(isvc, policies) {
			if err := BindRun(w.ctx, c, isvc, g, RunOpen{Adopted: out.Adopted, Closed: out.Closed}); err != nil {
				w.fail("bind run: %v", err)
			}
		}
	}
	var kept *v1beta1.InferenceService
	if st.lostFlushPending {
		kept = isvc.DeepCopy()
		st.lostFlushPending = false
	}
	for i, cfg := range w.cfg.members {
		var p *int32
		if resolver := PartitionResolver(isvc, policies, cfg.comp); resolver != nil {
			p = resolver(st.members[i].desired)
			if p != nil && *p > st.members[i].desired {
				p = ptr.To(st.members[i].desired)
			}
		} else {
			// The projector keeps the rollback target only while a canary
			// governs the member, as it keeps the partition.
			st.members[i].rollbackTo = ""
		}
		st.members[i].partition = p
		w.workloadStep(st, i)
	}
	w.rollup(st)
	c = w.client(st)
	for _, g := range rollout.CanaryGroups(isvc, policies) {
		if _, err := Dispatch(w.ctx, DispatchDeps{
			Client: c, Reader: c, Recorder: events, ISVC: isvc, Policies: policies, Now: st.now,
			DefaultReadyTimeout: cellReadyTimeout, Requeue: reconcileRequeue, ParkedRequeue: failedRequeue,
			ComponentRunnerPorts: canaryRunnerPorts(), Group: g,
			StepSampler: sampler, BundledPrometheusAddress: gatePrometheusAddress,
		}); err != nil {
			w.fail("dispatch: %v (state %s)", err, st)
		}
		w.dispatches++
	}
	st.passEvents = events.messages
	if sampler.answered && (st.sample.failing || st.sample.gap) {
		st.sample.reads++
	}
	// The controller drops the record and the phase of a unit no group names
	// any more, as it does after the dispatch.
	ReleaseUndeclaredUnits(isvc, policies)
	if w.ext != nil && w.ext.afterPass != nil {
		w.ext.afterPass(w, st, c, isvc, policies)
	}
	for i, cfg := range w.cfg.members {
		r := &v1beta1.InferenceReplica{}
		if err := c.Get(w.ctx, irprojector.RoleReplicaKey(isvc, cfg.comp), r); err != nil {
			w.fail("read replica: %v", err)
		}
		st.members[i].rollbackTo = ""
		if r.Spec.Pacing != nil && r.Spec.Pacing.RollbackToRevision != nil {
			st.members[i].rollbackTo = *r.Spec.Pacing.RollbackToRevision
		}
	}
	if kept != nil {
		isvc.Status = kept.Status
		isvc.Annotations = kept.Annotations
	}
	after := activeRunID(isvc)
	if before != "" && before != after {
		st.closes[before]++
	}
	if after != "" && after != before && st.closes[after] > 0 {
		// The id is the hash of the targets, the plan and the second the
		// run opened in, so a run opened in the same second as the close
		// of one toward the same targets carries the same id.
		st.reusedRun = after
		delete(st.closes, after)
	}
	if after != "" && after != before && isvc.Status.Rollout.LastRun != nil {
		last := isvc.Status.Rollout.LastRun
		if last.Outcome == v1beta1.RolloutRunCompleted && targetsOf(last.TargetRevisions) == targetsOf(isvc.Status.Rollout.ActiveRun.TargetRevisions) {
			st.reopened = targetsOf(last.TargetRevisions)
		}
	}
}

// settle runs passes until a pass changes nothing the key reads.
func (w *gateWorld) settle(st *gateState) {
	for round := 0; round < 8; round++ {
		before := st.key()
		w.pass(st)
		if st.key() == before {
			return
		}
	}
	st.unsettled = true
}

// unit is one canary group's record as the pass left it.
type gateUnit struct {
	group   *v1beta1.RolloutGroup
	primary v1beta1.ComponentType
	record  *v1beta1.CanaryStatus
	phase   v1beta1.RolloutPhase
	state   rollout.CanaryState
}

func (w *gateWorld) units(st *gateState) []gateUnit {
	var out []gateUnit
	for _, g := range rollout.CanaryGroups(st.isvc, rollout.Policies{}) {
		primary := rollout.PrimaryOf(g)
		cs := rollout.CanaryStatusFor(&st.isvc.Status, primary)
		phase := st.isvc.Status.Components[primary].RolloutPhase
		out = append(out, gateUnit{group: g, primary: primary, record: cs, phase: phase, state: rollout.StateOf(cs, phase, len(g.Canary.Steps))})
	}
	return out
}

// deadlines are the moments the record waits for: the end of a timed pause,
// the ready timeout of a capacity wait, and the end of the drain window.
func (w *gateWorld) deadlines(st *gateState) []time.Time {
	var out []time.Time
	for _, u := range w.units(st) {
		cs := u.record
		if cs == nil {
			continue
		}
		if cs.CapacityWaitSince != nil {
			out = append(out, cs.CapacityWaitSince.Add(cellReadyTimeout))
		}
		steps := u.group.Canary.Steps
		if cs.CurrentStep < 0 || int(cs.CurrentStep) >= len(steps) || cs.StepEnteredTime == nil {
			continue
		}
		step := steps[cs.CurrentStep]
		if step.Pause != nil && step.Pause.Duration != nil {
			out = append(out, cs.StepEnteredTime.Add(step.Pause.Duration.Duration))
		}
		out = append(out, gateAnalysisDeadlines(cs, step)...)
		if delay := u.group.Canary.ScaleDownDelaySeconds; int(cs.CurrentStep) == len(steps)-1 && delay != nil && *delay > 0 {
			out = append(out, cs.StepEnteredTime.Add(time.Duration(*delay)*time.Second))
		}
	}
	return out
}

// nextDeadline is the soonest deadline still ahead of the clock.
func (w *gateWorld) nextDeadline(st *gateState) (time.Time, bool) {
	var next time.Time
	for _, d := range w.deadlines(st) {
		if !d.After(st.now) {
			continue
		}
		if next.IsZero() || d.Before(next) {
			next = d
		}
	}
	return next, !next.IsZero()
}

// key is the state as the model distinguishes it: the rows and the spec of
// every member, the service's annotations, step list and status, which
// deadlines have passed, and the budgets left. The clock itself is not in
// it: only which deadlines it has passed matters to the code.
func (st *gateState) key() string {
	var b strings.Builder
	for _, m := range st.members {
		for _, r := range m.rows {
			fmt.Fprintf(&b, "%d%s>%s,", r.kind, r.rev, r.target)
		}
		p := "-"
		if m.partition != nil {
			p = strconv.Itoa(int(*m.partition))
		}
		fmt.Fprintf(&b, "|d%d u%s c%s rb%s p%s g%d;", m.desired, m.updateRev, m.current, m.rollbackTo, p, m.generation)
	}
	keys := make([]string, 0, len(st.isvc.Annotations))
	for k, v := range st.isvc.Annotations {
		keys = append(keys, k+"="+v)
	}
	sort.Strings(keys)
	b.WriteString("a:" + strings.Join(keys, ",") + ";")
	if st.isvc.Spec.Rollout == nil {
		b.WriteString("spec:none;")
	} else {
		for _, g := range st.isvc.Spec.Rollout.Groups {
			if g.Canary == nil {
				continue
			}
			for _, s := range g.Canary.Steps {
				fmt.Fprintf(&b, "%s/%d/%s,", s.Capacity.String(), s.Traffic, gatePauseKind(s))
			}
		}
		b.WriteString(";")
	}
	if r := st.isvc.Status.Rollout; r != nil {
		if r.ActiveRun != nil {
			b.WriteString("run:" + targetsOf(r.ActiveRun.TargetRevisions) + ":")
			for _, g := range r.ActiveRun.Plan.Groups {
				if g.Group.Canary != nil {
					for _, s := range g.Group.Canary.Steps {
						fmt.Fprintf(&b, "%s/%d/%s,", s.Capacity.String(), s.Traffic, gatePauseKind(s))
					}
				}
			}
		}
		if r.LastRun != nil {
			b.WriteString("last:" + string(r.LastRun.Outcome))
		}
		b.WriteString(";")
	}
	for _, name := range []string{v1beta1.RolloutPlanReadyCondition, v1beta1.RolloutPlanDriftCondition} {
		if c := st.isvc.Status.GetCondition(apis.ConditionType(name)); c != nil {
			fmt.Fprintf(&b, "%s=%s/%s;", name, c.Status, c.Reason)
		}
	}
	comps := make([]string, 0, len(st.isvc.Status.Components))
	for c := range st.isvc.Status.Components {
		comps = append(comps, string(c))
	}
	sort.Strings(comps)
	for _, c := range comps {
		cs := st.isvc.Status.Components[v1beta1.ComponentType(c)]
		fmt.Fprintf(&b, "%s:%s:", c, cs.RolloutPhase)
		for _, t := range cs.Traffic {
			fmt.Fprintf(&b, "%s=%d,", t.RevisionName, t.Percent)
		}
		if rec := cs.Canary; rec != nil {
			fmt.Fprintf(&b, "rec(%s,%s,%s,%d,%d,%s,%t,%s,", rec.TargetID, rec.CanaryRevisionHash, rec.StableRevisionHash, rec.CurrentStep, rec.ObservedTrafficWeight, rec.PromotedThrough, rec.PreStepHold, rec.RolledBackRevisionHash)
			if rec.Failed != nil {
				b.WriteString(string(rec.Failed.Reason))
			}
			fmt.Fprintf(&b, ",%t,%t)", rec.CapacityWaitSince != nil, rec.StepEnteredTime != nil)
		}
		b.WriteString(";")
	}
	fmt.Fprintf(&b, "budgets:%+v;flush:%t;sample:%s;", st.budgets, st.lostFlushPending, st.sample)
	if st.history != nil {
		b.WriteString(st.history.key(st))
	}
	return b.String()
}

func gatePauseKind(s v1beta1.RolloutGroupStep) string {
	switch {
	case s.Pause == nil:
		return "none"
	case s.Pause.Duration == nil:
		return "manual"
	}
	return s.Pause.Duration.Duration.String()
}

// clockKey adds which deadlines the clock has passed; it is read beside key
// so two states that differ only in a passed deadline are explored apart.
func (w *gateWorld) clockKey(st *gateState) string {
	var b strings.Builder
	for _, d := range w.deadlines(st) {
		if d.After(st.now) {
			b.WriteByte('0')
		} else {
			b.WriteByte('1')
		}
	}
	return st.key() + "clock:" + b.String()
}

// finished reports a run at its end: no run open, every Instance serving,
// and every unit's record at the done mark and Stable, rolled back, or absent.
func (w *gateWorld) finished(st *gateState) bool {
	if v1beta1.RolloutRunActive(st.isvc) {
		return false
	}
	for _, m := range st.members {
		for _, r := range m.rows {
			if r.kind != gateRowServing {
				return false
			}
		}
	}
	for _, u := range w.units(st) {
		switch u.state {
		case rollout.CanaryStateIdle, rollout.CanaryStateRolledBack:
		case rollout.CanaryStateDone:
			if u.phase != v1beta1.RolloutPhaseStable {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// progressMoves are the moves the world makes on its own: a building or
// lost Instance comes up (the lowest and the highest of each kind per
// member), and the clock reaches the next deadline or passes them all.
func (w *gateWorld) progressMoves(st *gateState) []gateMove {
	var out []gateMove
	for i, m := range st.members {
		first := map[gateRowKind]int{}
		last := map[gateRowKind]int{}
		for idx, r := range m.rows {
			if r.kind == gateRowServing {
				continue
			}
			if _, ok := first[r.kind]; !ok {
				first[r.kind] = idx
			}
			last[r.kind] = idx
		}
		for _, kind := range []gateRowKind{gateRowRecreating, gateRowSurging, gateRowCreating, gateRowLost, gateRowShort} {
			if idx, ok := first[kind]; ok {
				out = append(out, gateMove{kind: "serve", member: i, idx: idx})
				if last[kind] != idx {
					out = append(out, gateMove{kind: "serve", member: i, idx: last[kind]})
				}
			}
		}
	}
	if _, ok := w.nextDeadline(st); ok {
		out = append(out, gateMove{kind: "clock-next"}, gateMove{kind: "clock-all"})
	}
	// The world mends a breaching or unreadable metrics source once the gate
	// has read it: one unreadable sample, or breaches up to the step's limit.
	if (st.sample.gap && st.sample.reads >= 1) || (st.sample.failing && st.sample.reads >= 2) {
		out = append(out, gateMove{kind: "analysis-recover"})
	}
	return out
}

// releaseMoves are the operator verbs a hold names as its way out: a promote
// at a gate, the end of a pause, a resume of a Failed or rolled-back unit, a
// rollback of a Failed unit, and the plan reread the drift condition points at.
// The exploration spends budgets on the resume and the rollback; the
// classification of a quiet state asks for them regardless, since a budget
// is the model's bound and not a hold.
func (w *gateWorld) releaseMoves(st *gateState, full, ignoreBudget bool) []gateMove {
	var out []gateMove
	ann := st.isvc.Annotations
	budgets := st.budgets
	if ignoreBudget {
		budgets = gateBudgets{resumes: 1, rollbacks: 1}
	}
	if paused, _ := constants.RolloutPauseState(ann); paused {
		out = append(out, gateMove{kind: "unpause"})
	}
	for _, u := range w.units(st) {
		if u.record == nil {
			continue
		}
		_, promoting := ann[constants.RolloutPromoteAnnotation]
		switch u.state {
		case rollout.CanaryStateServing, rollout.CanaryStatePreHold, rollout.CanaryStateDraining, rollout.CanaryStateStaging:
			if !promoting && (u.state != rollout.CanaryStateStaging || full || ignoreBudget) {
				out = append(out, gateMove{kind: "promote"})
			}
			// The classification asks every verb a live step answers to:
			// the forced promote and the rollback beside the promote.
			if ignoreBudget {
				if _, forcing := ann[constants.RolloutPromoteForceAnnotation]; !forcing {
					out = append(out, gateMove{kind: "force"})
				}
				if u.record.RolledBackRevisionHash == "" {
					out = append(out, gateMove{kind: "rollback"})
				}
			}
		case rollout.CanaryStateFailed, rollout.CanaryStateRolledBack:
			if _, resuming := ann[constants.RolloutResumeAnnotation]; !resuming && budgets.resumes > 0 {
				out = append(out, gateMove{kind: "resume"})
			}
			if u.state == rollout.CanaryStateFailed && budgets.rollbacks > 0 && u.record.RolledBackRevisionHash == "" {
				out = append(out, gateMove{kind: "rollback"})
			}
		}
	}
	if c := st.isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanDriftCondition)); c != nil && c.Status == corev1.ConditionTrue {
		if _, repinning := ann[constants.RolloutRepinAnnotation]; !repinning {
			out = append(out, gateMove{kind: "repin"})
		}
	}
	return out
}

// adverseMoves are the moves that cost the run something, each limited by
// its budget: a pause, a rollback, a forced promote, a pod lost on the canary
// side or the stable side, a lost status write, a scale by one, a new
// revision pushed, the step list edited, the canary group removed.
func (w *gateWorld) adverseMoves(st *gateState) []gateMove {
	var out []gateMove
	ann := st.isvc.Annotations
	b := st.budgets
	if paused, _ := constants.RolloutPauseState(ann); !paused && b.pauses > 0 {
		out = append(out, gateMove{kind: "pause"})
	}
	for _, u := range w.units(st) {
		if u.record == nil || u.state == rollout.CanaryStateDone || u.state == rollout.CanaryStateRolledBack || u.state == rollout.CanaryStateRollingBack {
			continue
		}
		if b.rollbacks > 0 && u.record.RolledBackRevisionHash == "" && u.state != rollout.CanaryStateFailed {
			out = append(out, gateMove{kind: "rollback"})
		}
		if _, forcing := ann[constants.RolloutPromoteForceAnnotation]; b.forces > 0 && !forcing && u.state != rollout.CanaryStateFailed {
			out = append(out, gateMove{kind: "force"})
		}
		if b.lostPods > 0 {
			for i, m := range st.members {
				canaryRow, stableRow := -1, -1
				for idx, r := range m.rows {
					if r.kind != gateRowServing {
						continue
					}
					if r.rev == u.record.CanaryRevisionHash && canaryRow < 0 {
						canaryRow = idx
					}
					if r.rev == u.record.StableRevisionHash && stableRow < 0 {
						stableRow = idx
					}
				}
				for _, idx := range []int{canaryRow, stableRow} {
					if idx >= 0 {
						out = append(out, gateMove{kind: "podloss", member: i, idx: idx})
					}
				}
			}
		}
	}
	if b.lostFlushes > 0 && !st.lostFlushPending {
		out = append(out, gateMove{kind: "lostflush"})
	}
	if b.scales > 0 {
		for i, m := range st.members {
			if w.cfg.members[i].comp == v1beta1.RouterComponent && len(st.members) > 1 {
				continue
			}
			out = append(out, gateMove{kind: "scale", member: i, delta: 1})
			if m.desired > 1 {
				out = append(out, gateMove{kind: "scale", member: i, delta: -1})
			}
		}
	}
	if b.pushes > 0 && st.members[0].updateRev != gateRevisions[2] {
		out = append(out, gateMove{kind: "push"})
	}
	if st.isvc.Spec.Rollout != nil && len(st.isvc.Spec.Rollout.Groups) > 0 {
		if b.edits > 0 {
			out = append(out, gateMove{kind: "edit"})
		}
		if b.removals > 0 {
			out = append(out, gateMove{kind: "remove"})
		}
	}
	return append(out, w.faultMoves(st)...)
}

// apply makes one move and lets the controller settle on it.
func (w *gateWorld) apply(st *gateState, mv gateMove) *gateState {
	next := st.clone()
	next.path = append(next.path, mv)
	isvc := next.isvc
	switch mv.kind {
	case "serve":
		r := &next.members[mv.member].rows[mv.idx]
		switch r.kind {
		case gateRowRecreating, gateRowSurging, gateRowCreating:
			r.kind, r.rev, r.target = gateRowServing, r.target, ""
		case gateRowLost, gateRowShort:
			r.kind = gateRowServing
		}
	case "clock-next":
		if d, ok := w.nextDeadline(next); ok {
			next.now = d.Add(time.Second)
		}
	case "clock-all":
		next.now = next.now.Add(2 * time.Hour)
	case "promote":
		for _, u := range w.units(next) {
			if u.record != nil {
				annotate(isvc, constants.RolloutPromoteAnnotation, u.record.CanaryRevisionHash)
			}
		}
	case "force":
		next.budgets.forces--
		for _, u := range w.units(next) {
			if u.record != nil {
				annotate(isvc, constants.RolloutPromoteForceAnnotation, u.record.CanaryRevisionHash)
			}
		}
	case "pause":
		next.budgets.pauses--
		annotate(isvc, constants.PausedRolloutAnnotation, "true")
	case "unpause":
		delete(isvc.Annotations, constants.PausedRolloutAnnotation)
	case "rollback":
		next.budgets.rollbacks--
		annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	case "resume":
		next.budgets.resumes--
		next.resumed = true
		for _, u := range w.units(next) {
			if u.record != nil {
				annotate(isvc, constants.RolloutResumeAnnotation, u.record.CanaryRevisionHash)
			}
		}
	case "repin":
		annotate(isvc, constants.RolloutRepinAnnotation, "now")
	case "podloss":
		next.budgets.lostPods--
		r := &next.members[mv.member].rows[mv.idx]
		r.kind, r.target = gateRowLost, ""
	case "lostflush":
		next.budgets.lostFlushes--
		next.lostFlushPending = true
	case "scale":
		next.budgets.scales--
		next.scaled = true
		m := &next.members[mv.member]
		m.desired += mv.delta
		m.generation++
		isvc.Generation++
		if mv.delta < 0 {
			m.rows = m.rows[:len(m.rows)+int(mv.delta)]
		} else {
			target := w.buildTarget(next, mv.member)
			for k := int32(0); k < mv.delta; k++ {
				m.rows = append(m.rows, gateRow{kind: gateRowCreating, target: target})
			}
		}
	case "push":
		next.budgets.pushes--
		next.pushed = true
		for i := range next.members {
			next.members[i].updateRev = gateRevisions[2]
			next.members[i].generation++
		}
		isvc.Generation++
	case "edit":
		next.budgets.edits--
		g := &isvc.Spec.Rollout.Groups[0]
		if len(g.Canary.Steps) > 1 {
			g.Canary.Steps = g.Canary.Steps[len(g.Canary.Steps)-1:]
		} else {
			g.Canary.Steps[0].Pause = &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 10 * time.Minute}}
		}
		isvc.Generation++
	case "remove":
		next.budgets.removals--
		next.removed = true
		isvc.Spec.Rollout = nil
		isvc.Generation++
	default:
		if w.ext == nil || w.ext.apply == nil || !w.ext.apply(w, next, mv) {
			w.applyFault(next, mv)
		}
	}
	w.settle(next)
	return next
}

// gateFinding is one invariant broken at one state.
type gateFinding struct {
	class   string
	message string
	state   *gateState
}

// servingOn counts a member's pods in rotation on a revision hash.
func (w *gateWorld) servingOn(st *gateState, i int, hash string) int32 {
	var n int32
	for _, r := range st.members[i].rows {
		if r.rev != hash {
			continue
		}
		switch {
		case r.serving():
			n += w.cfg.members[i].gang
		case r.kind == gateRowShort:
			n += w.cfg.members[i].gang - 1
		}
	}
	return n
}

// check reads every invariant at one settled state.
func (w *gateWorld) check(st *gateState, full bool) []gateFinding {
	var out []gateFinding
	add := func(class, format string, args ...any) {
		out = append(out, gateFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	if w.ext != nil && w.ext.check != nil {
		out = append(out, w.ext.check(w, st)...)
	}
	if st.unsettled {
		add("unsettled", "the controller kept changing its status for eight passes with no move in between")
	}
	for id, n := range st.closes {
		if n > 1 {
			add("run-closed-twice", "run %s closed %d times", id, n)
		}
	}
	if st.reopened != "" {
		add("reopen-after-complete", "a run opened toward %s right after the run that closed Completed on the same targets", st.reopened)
	}
	if st.reusedRun != "" {
		add("run-id-reused", "run %s opened again in the second a run with that id closed, toward the same targets under the same plan", st.reusedRun)
	}
	// (a) traffic names only revisions that serve. A member with nothing in
	// rotation keeps the step's weight, and a unit under the operator's pause
	// has its pass write nothing; both are a split that names an unserving
	// revision and are reported apart. A rollback or a Failed hold moves a
	// stable share with no pod behind it onto the revision that serves, so a
	// split left on an empty stable revision under a hold fails, as does one
	// on a member that keeps no stable Instance; a stopped canary whose
	// canary pod is lost keeps the step's share on the canary revision until
	// the workload rebuilds the pod, which is reported apart.
	// After the done mark the cutover names the target alone until it lands,
	// which is reported apart.
	paused, _ := constants.RolloutPauseState(st.isvc.Annotations)
	held := false
	heldStable := map[string]bool{}
	for _, u := range w.units(st) {
		switch u.state {
		case rollout.CanaryStateFailed, rollout.CanaryStateRollingBack, rollout.CanaryStateRolledBack:
			held = true
			if u.record != nil {
				heldStable[u.record.StableRevisionHash] = true
			}
		}
	}
	for i, cfg := range w.cfg.members {
		if !Governs(st.isvc, rollout.Policies{}, cfg.comp) {
			// The split of a member no canary drives is the coordination
			// pass's, written from the pods on every pass; the model does
			// not run that pass.
			continue
		}
		cs := st.isvc.Status.Components[cfg.comp]
		var total int32
		for _, hash := range gateRevisions {
			total += w.servingOn(st, i, hash)
		}
		for _, t := range cs.Traffic {
			if t.Percent <= 0 {
				continue
			}
			for _, hash := range gateRevisions {
				if coordination.PerRevisionServiceName(gateService, cfg.comp, hash) != t.RevisionName || w.servingOn(st, i, hash) > 0 {
					continue
				}
				class := "traffic-empty-revision"
				switch {
				case total == 0:
					class = "traffic-empty-revision-nothing-serves"
				case paused:
					class = "traffic-empty-revision-under-a-pause"
				case held && heldStable[hash]:
					class = "traffic-empty-stable-revision-under-a-hold"
				case held:
					class = "traffic-empty-canary-revision-under-a-hold"
				case w.unitDone(st, cfg.comp):
					class = "traffic-empty-revision-after-cutover"
				case heldFloor(st.members[i].desired) == 0 && len(w.cfg.members) > 1:
					class = "known-single-router-traffic"
				}
				add(class, "%s traffic sends %d percent to revision %s, which has no pod in rotation (%d pods of the member serve)", cfg.comp, t.Percent, hash, total)
			}
		}
	}
	// (b) the held share of the fleet is what the step asks for. After a
	// scale the created Instances run the canary revision; after a resume
	// the unit continues at a step whose staged count covers the fleet.
	// Both pictures are named apart.
	suffix := ""
	switch {
	case st.scaled:
		suffix = "-after-scale"
	case st.resumed:
		suffix = "-after-resume"
	}
	for _, u := range w.units(st) {
		cs := u.record
		if cs == nil || cs.RolledBackRevisionHash != "" || cs.CurrentStep < 0 || int(cs.CurrentStep) >= len(u.group.Canary.Steps) {
			continue
		}
		step := u.group.Canary.Steps[cs.CurrentStep]
		for i, cfg := range w.cfg.members {
			if !gateGroupHas(u.group, cfg.comp) {
				continue
			}
			m := st.members[i]
			minimum := stepPartition(step, m.desired)
			var kept, onCanary int32
			for _, r := range m.rows {
				switch {
				case r.rev == cs.CanaryRevisionHash || (r.inFlight() && r.target == cs.CanaryRevisionHash):
					onCanary++
				case r.kind == gateRowServing || r.kind == gateRowLost || r.kind == gateRowSurging || r.kind == gateRowShort:
					kept++
				case r.kind == gateRowCreating && r.target != "":
					// A first build toward a revision other than the canary's
					// is kept off the canary revision by the attempt itself.
					kept++
				}
			}
			if kept < minimum {
				add("held-short"+suffix, "%s keeps %d Instances off the canary revision at step %d, the step holds %d of %d", cfg.comp, kept, cs.CurrentStep, minimum, m.desired)
			}
			if onCanary > m.desired-minimum {
				add("share-exceeded"+suffix, "%s runs or builds %d Instances on the canary revision at step %d, the step stages %d of %d", cfg.comp, onCanary, cs.CurrentStep, m.desired-minimum, m.desired)
			}
		}
	}
	// (c) a replica keeps a rollback target only while a canary drives it.
	if len(rollout.CanaryGroups(st.isvc, rollout.Policies{})) == 0 && !v1beta1.RolloutRunActive(st.isvc) {
		for i, m := range st.members {
			if m.rollbackTo != "" {
				add("rollback-target-left-behind", "%s still carries rollbackToRevision=%s with no canary group and no run left to clear it; its spec target is %s", w.cfg.members[i].comp, query.RevisionFromName(m.rollbackTo).Hash(), m.updateRev)
			}
		}
	}
	// (d) a capacity wait with nothing coming up never ends on its own: no
	// Instance is building, lost or short on any member, the rollout is not
	// paused, and a live unit still records the wait.
	if paused, _ := constants.RolloutPauseState(st.isvc.Annotations); !paused && !w.anythingComingUp(st) {
		for _, u := range w.units(st) {
			if u.record == nil || u.record.CapacityWaitSince == nil {
				continue
			}
			switch u.state {
			case rollout.CanaryStateStaging, rollout.CanaryStateServing, rollout.CanaryStatePreHold, rollout.CanaryStateDraining:
				add("capacity-never-arrives", "%s records a capacity wait at step %d with no Instance building, lost or short on any member: %s", u.primary, u.record.CurrentStep, st)
			}
		}
	}
	// (e) the run reaches an end: not finished, nothing moves on its own, and
	// no hold names a way out that works. An end is counted by how the last
	// run closed.
	if w.finished(st) {
		outcome := "no-run"
		if r := st.isvc.Status.Rollout; r != nil && r.LastRun != nil {
			outcome = strings.ToLower(string(r.LastRun.Outcome))
		}
		add("end-"+outcome, "the run reached its end: %s", w.describe(st))
	} else if len(w.progressMoves(st)) == 0 {
		class, message := w.classifyQuiet(st, full)
		add(class, "%s", message)
	}
	return out
}

func gateGroupHas(g *v1beta1.RolloutGroup, c v1beta1.ComponentType) bool {
	for _, gc := range g.Components {
		if gc == c {
			return true
		}
	}
	return false
}

// describe says where a quiet state stands, for a person reading the report.
func (w *gateWorld) describe(st *gateState) string {
	var parts []string
	if r := st.isvc.Status.Rollout; r != nil {
		switch {
		case r.ActiveRun != nil:
			parts = append(parts, "run open toward "+targetsOf(r.ActiveRun.TargetRevisions))
		case r.LastRun != nil:
			parts = append(parts, "last run "+string(r.LastRun.Outcome))
		}
	}
	for _, u := range w.units(st) {
		if u.record == nil {
			parts = append(parts, fmt.Sprintf("%s: no record, phase %s", u.primary, u.phase))
			continue
		}
		cs := u.record
		s := fmt.Sprintf("%s: %s at step %d of %d, phase %s, weight %d", u.primary, u.state, cs.CurrentStep, len(u.group.Canary.Steps), u.phase, cs.ObservedTrafficWeight)
		if cs.PreStepHold {
			s += ", pre-step hold"
		}
		if cs.Failed != nil {
			s += ", failed " + string(cs.Failed.Reason)
		}
		if cs.RolledBackRevisionHash != "" {
			s += ", rejected " + cs.RolledBackRevisionHash
		}
		if cs.CapacityWaitSince != nil {
			s += ", capacity wait running"
		}
		parts = append(parts, s)
	}
	if c := st.isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanDriftCondition)); c != nil && c.Status == corev1.ConditionTrue {
		parts = append(parts, "drift: "+c.Message)
	}
	keys := make([]string, 0, len(st.isvc.Annotations))
	for k, v := range st.isvc.Annotations {
		keys = append(keys, strings.TrimPrefix(k, constants.OMEAPIGroupName+"/")+"="+v)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		parts = append(parts, "annotations "+strings.Join(keys, ","))
	}
	return strings.Join(parts, "; ")
}

// classifyQuiet names a state that cannot move on its own. A hold whose
// named way out changes the state is reported under that hold's name; one
// with no way out is the class that fails the test.
func (w *gateWorld) classifyQuiet(st *gateState, full bool) (string, string) {
	// A run whose spec has no rollout group left is judged by the members it
	// recorded and closes like any other, so a quiet state with such a run
	// classifies by the verbs that release it, like every other hold.
	key := w.clockKey(st)
	var ways []string
	for _, mv := range w.releaseMoves(st, true, true) {
		child := w.apply(st, mv)
		if w.clockKey(child) != key {
			ways = append(ways, mv.kind)
		}
	}
	description := w.describe(st)
	if len(ways) == 0 {
		return "no-way-out", description + " | rows: " + st.String()
	}
	sort.Strings(ways)
	if named, reading := w.holdNamed(st); !named {
		return "hold-unnamed", fmt.Sprintf("the wait is not named in the status (%s); ways out: %s | %s | rows: %s", reading, strings.Join(ways, "+"), description, st)
	}
	class := "hold-" + strings.Join(ways, "+")
	for _, u := range w.units(st) {
		if u.state == rollout.CanaryStateFailed {
			class = "hold-failed"
		}
	}
	if paused, _ := constants.RolloutPauseState(st.isvc.Annotations); paused {
		class = "hold-paused"
	}
	return class, description
}

// gateReport is what one configuration's exploration found.
type gateReport struct {
	states, depth, runs int
	findings            map[string]gateFinding
	counts              map[string]int
}

func (w *gateWorld) record(rep *gateReport, st *gateState, full bool) {
	for _, f := range w.check(st, full) {
		if w.ext != nil && w.ext.fold != nil {
			if known := w.ext.fold(w, f); known != "" {
				f.class = known
			}
		}
		rep.counts[f.class]++
		if _, ok := rep.findings[f.class]; !ok {
			rep.findings[f.class] = f
		}
	}
}

// explore walks every path breadth-first to the bounds with a hash of
// visited states.
func (w *gateWorld) explore(maxStates, maxDepth int, full bool) gateReport {
	rep := gateReport{findings: map[string]gateFinding{}, counts: map[string]int{}}
	start := w.initial()
	w.settle(start)
	visited := map[string]bool{w.clockKey(start): true}
	queue := []*gateState{start}
	for len(queue) > 0 && rep.states < maxStates {
		st := queue[0]
		queue = queue[1:]
		rep.states++
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
		w.record(&rep, st, full)
		if w.finished(st) {
			continue
		}
		if len(st.path) >= maxDepth {
			rep.counts["depth"]++
			if _, ok := rep.findings["depth"]; !ok {
				rep.findings["depth"] = gateFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
			}
			continue
		}
		moves := append(w.progressMoves(st), w.releaseMoves(st, full, false)...)
		moves = append(moves, w.adverseMoves(st)...)
		moves = append(moves, w.extraMoves(st)...)
		for _, mv := range moves {
			child := w.apply(st, mv)
			k := w.clockKey(child)
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
// bring a run to its end, to reach the depths the breadth-first walk does not.
func (w *gateWorld) randomRuns(rep *gateReport, seed int64, runs, maxDepth int, full bool) {
	rng := rand.New(rand.NewSource(seed))
	for r := 0; r < runs; r++ {
		st := w.initial()
		w.settle(st)
		rep.runs++
		for depth := 0; ; depth++ {
			w.record(rep, st, full)
			if w.finished(st) {
				break
			}
			forward := append(w.progressMoves(st), w.releaseMoves(st, full, false)...)
			adverse := append(w.adverseMoves(st), w.extraMoves(st)...)
			if len(forward) == 0 && len(adverse) == 0 {
				break
			}
			if depth >= maxDepth {
				rep.counts["depth"]++
				if _, ok := rep.findings["depth"]; !ok {
					rep.findings["depth"] = gateFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
				}
				break
			}
			var mv gateMove
			if len(forward) == 0 || (len(adverse) > 0 && rng.Intn(4) == 0) {
				mv = adverse[rng.Intn(len(adverse))]
			} else {
				mv = forward[rng.Intn(len(forward))]
			}
			st = w.apply(st, mv)
			if len(st.path) > rep.depth {
				rep.depth = len(st.path)
			}
		}
	}
}

// straightRun plays one run with no adverse move: the world brings every
// Instance up, the clock reaches each deadline, and the operator answers
// every hold with the verb it names. Every configuration must reach a
// Completed end this way; a run that does not is reported under depth. It
// returns the end state.
func (w *gateWorld) straightRun(rep *gateReport, maxMoves int, full bool) *gateState {
	st := w.initial()
	w.settle(st)
	rep.runs++
	for depth := 0; ; depth++ {
		w.record(rep, st, full)
		if w.finished(st) {
			return st
		}
		moves := append(w.progressMoves(st), w.releaseMoves(st, full, false)...)
		if len(moves) == 0 || depth >= maxMoves {
			rep.counts["depth"]++
			if _, ok := rep.findings["depth"]; !ok {
				rep.findings["depth"] = gateFinding{class: "depth", message: fmt.Sprintf("the straight run did not reach an end in %d moves: %s", depth, w.describe(st)), state: st}
			}
			return st
		}
		st = w.apply(st, moves[0])
	}
}

// TestCanarySteps_EveryConfigurationCompletesWhenNothingGoesWrong plays the
// straight run of every configuration and expects a Completed end: the
// world brings each staged Instance up, the clock reaches each deadline,
// and the operator promotes each manual gate.
func TestCanarySteps_EveryConfigurationCompletesWhenNothingGoesWrong(t *testing.T) {
	full := os.Getenv("OME_CANARY_HUNT") == "full"
	scheme := canaryScheme(t)
	for _, cfg := range gateConfigs(full) {
		w := newGateWorld(t, cfg, scheme)
		rep := gateReport{findings: map[string]gateFinding{}, counts: map[string]int{}}
		end := w.straightRun(&rep, 400, full)
		outcome := v1beta1.RolloutRunOutcome("")
		if r := end.isvc.Status.Rollout; r != nil && r.LastRun != nil {
			outcome = r.LastRun.Outcome
		}
		if !w.finished(end) || outcome != v1beta1.RolloutRunCompleted {
			t.Errorf("[%s] the straight run did not end Completed after %d moves: %s\n  path: %s", cfg.name, len(end.path), w.describe(end), gatePath(end))
			continue
		}
		t.Logf("[%s] Completed in %d moves, %d passes", cfg.name, len(end.path), w.passes)
	}
}

func gatePath(st *gateState) string {
	parts := make([]string, 0, len(st.path))
	for _, mv := range st.path {
		parts = append(parts, mv.String())
	}
	return strings.Join(parts, " -> ")
}

// The step lists under exploration. Every list ends at 100 percent capacity
// and traffic; the three-step list opens with a step that stages capacity
// and moves no traffic.
func gateStepList(name string) []v1beta1.RolloutGroupStep {
	manual := &v1beta1.RolloutPause{}
	timed := func() *v1beta1.RolloutPause {
		return &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 10 * time.Minute}}
	}
	switch name {
	case "one-manual":
		return []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: manual}}
	case "one-timed":
		return []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: timed()}}
	case "two-manual":
		return []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: manual},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}
	case "two-timed":
		return []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: timed()},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}
	case "three-manual":
		return []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 0, Pause: manual},
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: manual},
			{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: manual},
		}
	case "three-timed":
		return []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 0, Pause: timed()},
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: timed()},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}
	case "two-analysis":
		return []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: gateBake(), Analysis: gateAnalysis(nil)},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}
	case "three-analysis":
		return []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 0, Pause: manual},
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: gateBake(), Analysis: gateAnalysis(ptr.To(v1beta1.OnInconclusiveRollbackOnStall))},
			{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: gateBake(), Analysis: gateAnalysis(ptr.To(v1beta1.OnInconclusiveRollback))},
		}
	}
	panic("unknown step list " + name)
}

var gateStepNames = []string{"one-manual", "one-timed", "two-manual", "two-timed", "three-manual", "three-timed"}

// gateShape is one fleet shape: the members with their counts and pods per
// Instance; the strategy and the operator partition are applied per
// configuration.
type gateShape struct {
	name    string
	members []gateMember
}

var gateShapes = []gateShape{
	{name: "engine4", members: []gateMember{{comp: v1beta1.EngineComponent, desired: 4, gang: 1}}},
	{name: "engine8", members: []gateMember{{comp: v1beta1.EngineComponent, desired: 8, gang: 1}}},
	{name: "engine16", members: []gateMember{{comp: v1beta1.EngineComponent, desired: 16, gang: 1}}},
	{name: "pd12:8+router2", members: []gateMember{
		{comp: v1beta1.RouterComponent, desired: 2, gang: 1},
		{comp: v1beta1.EngineComponent, desired: 12, gang: 1},
		{comp: v1beta1.DecoderComponent, desired: 8, gang: 1},
	}},
	{name: "pd6:2+router1", members: []gateMember{
		{comp: v1beta1.RouterComponent, desired: 1, gang: 1},
		{comp: v1beta1.EngineComponent, desired: 6, gang: 1},
		{comp: v1beta1.DecoderComponent, desired: 2, gang: 1},
	}},
	{name: "gang4x2", members: []gateMember{{comp: v1beta1.EngineComponent, desired: 4, gang: 2}}},
}

// gateWorkers is how many configurations explore at once; the machine is
// shared, so the default stays small and OME_CANARY_HUNT_WORKERS raises it.
func gateWorkers() int {
	if n, err := strconv.Atoi(os.Getenv("OME_CANARY_HUNT_WORKERS")); err == nil && n > 0 {
		return n
	}
	return 4
}

func gateDefaultBudgets(full bool) gateBudgets {
	b := gateBudgets{lostPods: 1, lostFlushes: 1, scales: 1, pushes: 1, edits: 1, removals: 1, rollbacks: 1, pauses: 1, resumes: 1}
	if full {
		b.forces, b.lostPods = 1, 2
	}
	return b
}

// gateConfigs lists the configurations to explore: every shape under both
// strategies at a 10 percent budget with the reference step
// lists, then a seeded sample over the whole space.
// gateConfigFor is one configuration: a shape under one strategy at a
// 10 percent budget, one step list, one drain window, and an
// operator partition beside the canary when given.
func gateConfigFor(shape gateShape, strategy v1beta1.UpdateStrategyType, stepsName string, drain int32, partition *int32, full bool) gateConfig {
	cfg := gateConfig{stepsName: stepsName, steps: gateStepList(stepsName), scaleDownDelay: drain, budgets: gateDefaultBudgets(full)}
	for _, m := range shape.members {
		m.strategy, m.budget, m.partition = strategy, "10%", partition
		cfg.members = append(cfg.members, m)
	}
	cfg.name = shape.name + " " + cfg.String()
	return cfg
}

func gateShapeNamed(name string) gateShape {
	for _, s := range gateShapes {
		if s.name == name {
			return s
		}
	}
	panic("unknown shape " + name)
}

func gateConfigs(full bool) []gateConfig {
	mk := func(shape gateShape, strategy v1beta1.UpdateStrategyType, stepsName string, drain int32, partition *int32) gateConfig {
		return gateConfigFor(shape, strategy, stepsName, drain, partition, full)
	}
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	var out []gateConfig
	fixed := []struct {
		shape    string
		strategy v1beta1.UpdateStrategyType
		steps    string
		drain    int32
	}{
		{"engine4", recreate, "two-manual", 0},
		{"engine4", surge, "three-manual", 600},
		{"engine8", recreate, "three-manual", 0},
		{"engine8", surge, "two-timed", 600},
		{"engine16", recreate, "one-manual", 600},
		{"engine16", surge, "three-timed", 0},
		{"pd12:8+router2", recreate, "three-manual", 600},
		{"pd12:8+router2", surge, "two-manual", 0},
		{"pd6:2+router1", recreate, "two-timed", 0},
		{"pd6:2+router1", surge, "one-timed", 600},
		{"gang4x2", recreate, "two-manual", 600},
		{"gang4x2", surge, "three-manual", 0},
	}
	shapes := map[string]gateShape{}
	for _, s := range gateShapes {
		shapes[s.name] = s
	}
	for _, f := range fixed {
		out = append(out, mk(shapes[f.shape], f.strategy, f.steps, f.drain, nil))
	}
	// An operator partition beside the canary, on the single-engine shape.
	out = append(out, mk(shapes["engine8"], recreate, "two-manual", 0, ptr.To(int32(2))))
	if full {
		for si, s := range gateShapes {
			for _, strategy := range []v1beta1.UpdateStrategyType{recreate, surge} {
				for ti, steps := range gateStepNames {
					var drain int32
					if (si+ti)%2 == 1 {
						drain = 600
					}
					out = append(out, mk(s, strategy, steps, drain, nil))
				}
			}
		}
		out = append(out, mk(shapes["engine4"], surge, "three-timed", 600, ptr.To(int32(1))), mk(shapes["pd6:2+router1"], recreate, "two-manual", 0, ptr.To(int32(1))))
	}
	rng := rand.New(rand.NewSource(20261019))
	sample := 6
	if full {
		sample = 20
	}
	for n := 0; n < sample; n++ {
		s := gateShapes[rng.Intn(len(gateShapes))]
		strategy := recreate
		if rng.Intn(2) == 0 {
			strategy = surge
		}
		var drain int32
		if rng.Intn(2) == 0 {
			drain = 600
		}
		var partition *int32
		if rng.Intn(4) == 0 {
			partition = ptr.To(int32(1))
		}
		out = append(out, mk(s, strategy, gateStepNames[rng.Intn(len(gateStepNames))], drain, partition))
	}
	return out
}

// gateKnownClasses are the readings the explorer reports without failing,
// each in words: the splits that name an unserving revision by rule, and the
// run id reused within one second.
var gateKnownClasses = map[string]string{
	"traffic-empty-revision-under-a-pause":       "the operator's pause holds the pass, which writes no traffic, so a member whose one Instance rolled to the pushed revision keeps the split the pause found on the revision the pod left (a paused step's split is left open)",
	"traffic-empty-revision-nothing-serves":      "a member with nothing in rotation keeps the step's weight: a loss with nothing to move onto is the workload's repair, not a traffic decision",
	"traffic-empty-canary-revision-under-a-hold": "a stopped canary whose canary pod is lost keeps the step's share on the canary revision until the workload rebuilds the pod: the stop leaves the split where it stood, and only an empty stable revision moves it (left open)",
	"traffic-empty-revision-after-cutover":       "after the done mark the cutover names the target alone and is not rewritten; a member whose target pods are lost while the old pod of its last surging Instance still serves sends its traffic to a revision with no pod (a split naming an unserving revision; left open)",
	"run-id-reused":                              "a run opened in the same second as the close of a run toward the same targets under the same plan carries the same id, so events name two runs alike (the id is a hash of the targets, the plan and the opening second; left open)",
}

// gateExplore explores every configuration keep admits and reports each
// broken invariant with the shortest path to it. The classes that fail the
// test are the ones read as bugs: a quiet state with no way
// out, a replica keeping a rollback target with no canary left to clear it,
// traffic on a revision that has no pod while the member's own canary pod
// serves, a held share short of the step's, a run closed twice or reopened
// after it completed, a straight run that reaches no end, and a controller
// that does not settle. Known holds, the known class above, a split under
// a hold or with nothing serving, and a scale or a resume under a step are
// reported only; those readings are left open.
func gateExplore(t *testing.T, keep func(gateConfig) bool) {
	t.Helper()
	var configs []gateConfig
	for _, cfg := range gateConfigs(os.Getenv("OME_CANARY_HUNT") == "full") {
		if keep(cfg) {
			configs = append(configs, cfg)
		}
	}
	gateExploreConfigs(t, configs)
}

// gateExploreConfigs explores the given configurations; see gateExplore.
// OME_CANARY_HUNT_ONLY keeps the configurations whose name contains one of
// its comma-separated parts, to replay a finding.
func gateExploreConfigs(t *testing.T, configs []gateConfig) {
	t.Helper()
	// Every pass builds a fake API from the whole fleet; the garbage of those
	// builds dominates the run, so the collector is given more room.
	defer debug.SetGCPercent(debug.SetGCPercent(400))
	full := os.Getenv("OME_CANARY_HUNT") == "full"
	if only := os.Getenv("OME_CANARY_HUNT_ONLY"); only != "" {
		var kept []gateConfig
		for _, cfg := range configs {
			for _, part := range strings.Split(only, ",") {
				if part != "" && strings.Contains(cfg.name, part) {
					kept = append(kept, cfg)
					break
				}
			}
		}
		configs = kept
	}
	maxStates, maxDepth, runs := 36, 60, 2
	if full {
		maxStates, maxDepth, runs = 80, 100, 3
	}
	totals := map[string]int{}
	var states, passes, dispatches, allRuns int
	type example struct {
		cfg     gateConfig
		finding gateFinding
	}
	examples := map[string][]example{}
	configsWith := map[string]int{}
	started := time.Now()
	scheme := canaryScheme(t)
	results := gateRunConfigs(configs, func(n int) *gateWorld { return newGateWorld(t, configs[n], scheme) }, gateRunBounds{states: maxStates, depth: maxDepth, runs: runs, probe: true, full: full}, gateWorkers())
	for n, cfg := range configs {
		res := results[n]
		rep := res.rep
		if res.failure != "" {
			if rep.counts == nil {
				rep.counts, rep.findings = map[string]int{}, map[string]gateFinding{}
			}
			rep.counts["model-error"]++
			rep.findings["model-error"] = gateFinding{class: "model-error", message: res.failure, state: &gateState{isvc: gateISVC(cfg)}}
		}
		states += rep.states
		passes += res.passes
		dispatches += res.dispatch
		allRuns += rep.runs
		if full {
			var parts []string
			for class, c := range rep.counts {
				parts = append(parts, fmt.Sprintf("%s=%d", class, c))
			}
			sort.Strings(parts)
			t.Logf("config %d/%d [%s]: %d states, depth %d, %d passes: %s", n+1, len(configs), cfg.name, rep.states, rep.depth, res.passes, strings.Join(parts, " "))
		}
		for class, c := range rep.counts {
			totals[class] += c
		}
		for class, f := range rep.findings {
			if known := gateKnownFindingFor(cfg, f); known != "" {
				totals[known] += rep.counts[class]
				totals[class] -= rep.counts[class]
				class = known
			}
			configsWith[class]++
			if len(examples[class]) < 8 {
				examples[class] = append(examples[class], example{cfg: cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class, n := range totals {
		if n > 0 {
			classes = append(classes, class)
		}
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d random runs, %d controller passes, %d canary dispatches, %s", len(configs), states, allRuns, passes, dispatches, time.Since(started).Round(time.Second))
	for _, class := range classes {
		if known, ok := gateKnownClasses[class]; ok {
			t.Logf("%s: %d states across %d configurations: %s", class, totals[class], configsWith[class], known)
		} else if why := gateKnownFindingWhy(class); why != "" {
			t.Logf("%s: %d states across %d configurations: %s", class, totals[class], configsWith[class], why)
		} else {
			t.Logf("%s: %d states across %d configurations", class, totals[class], configsWith[class])
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg.name, ex.finding.message, gatePath(ex.finding.state), ex.finding.state)
		}
		switch class {
		case "no-way-out", "hold-unnamed", "capacity-never-arrives", "rollback-target-left-behind", "traffic-empty-revision", "traffic-empty-stable-revision-under-a-hold",
			"known-single-router-traffic", "held-short", "share-exceeded", "run-closed-twice", "reopen-after-complete", "unsettled", "depth", "model-error":
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg.name, examples[class][0].finding.message)
		}
	}
}

// gateRunBounds are the bounds of one configuration's exploration: the
// breadth-first walk's states and depth, the random runs, and whether the
// operator's moves are probed at every moment of the straight run.
type gateRunBounds struct {
	states, depth, runs int
	probe, full         bool
}

// gateRunResult is one configuration's exploration as its worker left it; a
// panic inside the world is its failure.
type gateRunResult struct {
	rep              gateReport
	passes, dispatch int
	failure          string
}

// gateRunConfigs explores every configuration side by side, workers at a
// time, each in its own world built by newWorld, and returns the reports in
// configuration order.
func gateRunConfigs(configs []gateConfig, newWorld func(n int) *gateWorld, bounds gateRunBounds, workers int) []gateRunResult {
	results := make([]gateRunResult, len(configs))
	lanes := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for n := range configs {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			lanes <- struct{}{}
			defer func() { <-lanes }()
			w := newWorld(n)
			res := gateRunResult{}
			func() {
				defer func() {
					if r := recover(); r != nil {
						res.failure = fmt.Sprint(r)
					}
				}()
				res.rep = w.explore(bounds.states, bounds.depth, bounds.full)
				w.straightRun(&res.rep, 4*bounds.depth, bounds.full)
				if bounds.probe {
					w.probeMoments(&res.rep, 4*bounds.depth, bounds.full)
				}
				w.randomRuns(&res.rep, 1, bounds.runs, 4*bounds.depth, bounds.full)
			}()
			res.passes, res.dispatch = w.passes, w.dispatches
			results[n] = res
		}(n)
	}
	wg.Wait()
	return results
}

// TestCanarySteps_SingleEngineRunsReachAnEnd explores a canary over one
// engine of 4, 8 and 16 Instances, single pods and gangs, under both
// strategies, every step list, both drain windows, with an operator
// partition beside the canary in some configurations.
func TestCanarySteps_SingleEngineRunsReachAnEnd(t *testing.T) {
	gateExplore(t, func(c gateConfig) bool { return len(c.members) == 1 })
}

// TestCanarySteps_PrefillDecodeRunsReachAnEnd explores a canary over a
// router beside a prefill/decode pair, 12:8 and 6:2.
func TestCanarySteps_PrefillDecodeRunsReachAnEnd(t *testing.T) {
	gateExplore(t, func(c gateConfig) bool { return len(c.members) > 1 })
}

// serveAll brings every building or lost Instance up, lowest index first,
// and lets the controller settle after each, until nothing is in flight.
func (w *gateWorld) serveAll(st *gateState) *gateState {
	for round := 0; round < 128; round++ {
		moved := false
		for i, m := range st.members {
			for idx, r := range m.rows {
				if r.kind != gateRowServing {
					st = w.apply(st, gateMove{kind: "serve", member: i, idx: idx})
					moved = true
					break
				}
			}
			if moved {
				break
			}
		}
		if !moved {
			return st
		}
	}
	w.t.Fatalf("the fleet did not come up in 128 rounds: %s", st)
	return st
}

// runPasses runs n controller passes with no move in between.
func (w *gateWorld) runPasses(st *gateState, n int) *gateState {
	next := st.clone()
	for i := 0; i < n; i++ {
		w.pass(next)
	}
	return next
}

// TestCanarySteps_RemovingTheRolloutBlockMidRunStillLetsTheRunClose is the
// first quiet state with no way out the exploration guards against.
// Four engines run a two-step canary; the first split serves. The operator
// deletes spec.rollout, then promotes; the canary runs on the plan copy the
// run keeps, as designed, and reads Stable. The run layer judges a run whose
// groups left the spec by the members it recorded, so the run closes
// Completed on its own and the unit's record and phase go with the group.
// A plan reread issued while such a run is still open closes it at once, as the
// drift condition says.
func TestCanarySteps_RemovingTheRolloutBlockMidRunStillLetsTheRunClose(t *testing.T) {
	w := newGateWorld(t, gateConfigFor(gateShapeNamed("engine4"), v1beta1.UpdateStrategyRecreatePod, "two-manual", 0, nil, false), canaryScheme(t))
	st := w.initial()
	w.settle(st)
	st = w.serveAll(st)
	if u := w.units(st); len(u) != 1 || u[0].state != rollout.CanaryStateServing {
		t.Fatalf("the picture is wrong: the first split must be serving, got %s", w.describe(st))
	}
	st = w.apply(st, gateMove{kind: "remove"})
	st = w.apply(st, gateMove{kind: "promote"})
	st = w.serveAll(st)
	st = w.apply(st, gateMove{kind: "clock-all"})
	st = w.serveAll(st)
	if v1beta1.RolloutRunActive(st.isvc) {
		t.Errorf("the canary completed on the plan copy the run keeps and the run is still open (%s)", w.describe(st))
	}
	if r := st.isvc.Status.Rollout; r == nil || r.LastRun == nil || r.LastRun.Outcome != v1beta1.RolloutRunCompleted {
		t.Errorf("the run did not close Completed: %s", w.describe(st))
	}
	if rec := rollout.CanaryStatusFor(&st.isvc.Status, v1beta1.EngineComponent); rec != nil || st.isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase != "" {
		t.Errorf("the group is gone and the run closed, and the unit still carries a canary record or phase (%s)", w.describe(st))
	}
	for idx, r := range st.members[0].rows {
		if r.kind != gateRowServing || r.rev != gateRevisions[1] {
			t.Errorf("engine row %d stands at %s, want version 2 serving", idx, r)
		}
	}
	// The plan reread the drift condition names closes a run that has not finished.
	st = w.initial()
	w.settle(st)
	st = w.serveAll(st)
	st = w.apply(st, gateMove{kind: "remove"})
	drift := st.isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanDriftCondition))
	if drift == nil || drift.Status != corev1.ConditionTrue || !strings.Contains(drift.Message, constants.RolloutRepinAnnotation) {
		t.Fatalf("the picture is wrong: the drift condition must point at the plan reread, got %+v", drift)
	}
	st = w.apply(st, gateMove{kind: "repin"})
	st = w.runPasses(st, 3)
	if v1beta1.RolloutRunActive(st.isvc) {
		t.Errorf("the operator set %s=%q as the drift condition says, and the run is still open after three more passes (%s)",
			constants.RolloutRepinAnnotation, st.isvc.Annotations[constants.RolloutRepinAnnotation], w.describe(st))
	}
	if _, ok := st.isvc.Annotations[constants.RolloutRepinAnnotation]; ok {
		t.Errorf("the plan-reread annotation was not removed with the close (%s)", w.describe(st))
	}
}

// TestCanarySteps_RemovingTheCanaryAfterARollbackReleasesTheRollbackTarget
// is the second such state. Four engines, a two-step canary
// rolled back at its first step: every Instance is back on version 1, the
// run closed RolledBack, and the replica carries the rollback target the
// dispatch wrote. The operator then removes the canary group. The projector
// keeps the target only while a canary governs the member, so the next pass
// drops it with the partition, the controller drops the unit's record and
// phase, and the spec's revision rolls out: version 2 first, then version 3
// once it is pushed.
func TestCanarySteps_RemovingTheCanaryAfterARollbackReleasesTheRollbackTarget(t *testing.T) {
	w := newGateWorld(t, gateConfigFor(gateShapeNamed("engine4"), v1beta1.UpdateStrategyRecreatePod, "two-manual", 0, nil, false), canaryScheme(t))
	st := w.initial()
	w.settle(st)
	st = w.serveAll(st)
	st = w.apply(st, gateMove{kind: "rollback"})
	st = w.serveAll(st)
	if r := st.isvc.Status.Rollout; v1beta1.RolloutRunActive(st.isvc) || r == nil || r.LastRun == nil || r.LastRun.Outcome != v1beta1.RolloutRunRolledBack {
		t.Fatalf("the picture is wrong: the rollback must have closed the run RolledBack, got %s", w.describe(st))
	}
	if st.members[0].rollbackTo == "" {
		t.Fatalf("the picture is wrong: the replica must carry the rollback target while the unit holds rolled back")
	}
	st = w.apply(st, gateMove{kind: "remove"})
	st = w.runPasses(st, 3)
	if st.members[0].rollbackTo != "" {
		t.Errorf("the canary group is gone and the engine replica still carries rollbackToRevision=%s: its Instances stand on %s", st.members[0].rollbackTo, st.members[0])
	}
	if rec := rollout.CanaryStatusFor(&st.isvc.Status, v1beta1.EngineComponent); rec != nil || st.isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase != "" {
		t.Errorf("the group is gone and the unit still carries a canary record or phase (%s)", w.describe(st))
	}
	st = w.apply(st, gateMove{kind: "push"})
	st = w.serveAll(st)
	if st.members[0].rollbackTo != "" {
		t.Errorf("version 3 is pushed and the engine replica still carries rollbackToRevision=%s: its Instances stand on %s", st.members[0].rollbackTo, st.members[0])
	}
	for idx, r := range st.members[0].rows {
		if r.kind != gateRowServing || r.rev != gateRevisions[2] {
			t.Errorf("engine row %d stands at %s, want version 3 serving", idx, r)
		}
	}
}

// TestCanarySteps_ASingleRouterSendsItsTrafficToItsOwnPodWhileTheSecondariesComeUp
// holds that traffic follows the pods that serve. A
// router of one Instance fronts 6 prefill and 2 decode in one canary group;
// the first canary completes, so the router's traffic names version 2
// alone. The operator pushes version 3. The router has no Instance to keep
// on version 2, so it rolls at once, and the engine and decoder canary
// Instances are still coming up. The router's one pod serves version 3 and
// its traffic names that pod, not version 2, which has no router pod left,
// for as long as the other members' canary Instances take to come up.
func TestCanarySteps_ASingleRouterSendsItsTrafficToItsOwnPodWhileTheSecondariesComeUp(t *testing.T) {
	w := newGateWorld(t, gateConfigFor(gateShapeNamed("pd6:2+router1"), v1beta1.UpdateStrategyRecreatePod, "two-manual", 0, nil, false), canaryScheme(t))
	st := w.initial()
	w.settle(st)
	st = w.serveAll(st)
	st = w.apply(st, gateMove{kind: "promote"})
	st = w.serveAll(st)
	st = w.apply(st, gateMove{kind: "clock-all"})
	st = w.serveAll(st)
	if !w.finished(st) {
		t.Fatalf("the picture is wrong: the first canary must have completed, got %s", w.describe(st))
	}
	st = w.apply(st, gateMove{kind: "push"})
	st = w.apply(st, gateMove{kind: "serve", member: 0, idx: 0})
	router := st.members[0].rows[0]
	if router.kind != gateRowServing || router.rev != gateRevisions[2] {
		t.Fatalf("the picture is wrong: the router must serve version 3 by now, got %s", st)
	}
	if u := w.units(st); len(u) != 1 || u[0].state != rollout.CanaryStateStaging {
		t.Fatalf("the picture is wrong: the unit must be staging its secondaries, got %s", w.describe(st))
	}
	previous := coordination.PerRevisionServiceName(gateService, v1beta1.RouterComponent, gateRevisions[1])
	own := coordination.PerRevisionServiceName(gateService, v1beta1.RouterComponent, gateRevisions[2])
	var onPrevious, onOwn int32
	for _, target := range st.isvc.Status.Components[v1beta1.RouterComponent].Traffic {
		switch target.RevisionName {
		case previous:
			onPrevious = target.Percent
		case own:
			onOwn = target.Percent
		}
	}
	if onPrevious != 0 || onOwn != 100 {
		t.Fatalf("the router's traffic must follow its one serving pod: version 3 carries %d percent and version 2, with no router pod, %d percent", onOwn, onPrevious)
	}
}
