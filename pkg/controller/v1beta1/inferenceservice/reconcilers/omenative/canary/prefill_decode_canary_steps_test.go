package canary

import (
	"context"
	"fmt"
	"math/rand"
	"os"
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

// pairRules are the operator's prefill/decode settings as the chart renders
// them, at the chart's default tolerance of 5: the ratio rule on and the
// decoder named as the side that moves first inside every batch.
var pairRules = coordination.GroupDefaults{
	RatioTolerancePercent: ptr.To(int32(5)),
	PrefillDecode:         coordination.PrefillDecodeRules{KeepRatio: true, FirstSide: v1beta1.DecoderComponent},
}

// This file drives a prefill/decode canary, the engine and the decoder in one
// canary group, through the production code the controller runs in one pass,
// in the controller's order: the run layer (rolloutrun.Reconcile and BindRun),
// the partition the projector writes for each member (PartitionResolver), and
// the canary dispatch (Dispatch) over a fake API that holds the replicas,
// their pods and their revisions. The workload stand-in does what the
// replica controller does with the partition and the rollback target its
// replica carries: it holds the Instances the partition names, asks the
// coordination update gate (coordination.EvaluateUpdateGate, under the
// chart's prefill/decode settings) before each start, starts the admitted
// ones toward the target under the member's strategy and budget, follows a
// rollback target, and starts nothing new while the rollout is paused. The
// gate paces the two sides of the unit with the batch step: inside a step,
// and inside a revert, the decoder's Instances of a batch move first and the
// engine's wait until they serve.
//
// The configurations are the reference prefill/decode shapes (12:8, 15:15,
// 16:8, 20:10, 24:8, 30:10 and 12:4) under RecreatePod and SurgeThenDrain at
// 10 percent, with the step lists 10/30/60/100 and 25/50/100, manual and
// timed. Between passes the world makes one move: a building Instance serves,
// the clock passes a deadline, or the operator acts (promote, pause, resume,
// roll back, lose a pod on either side, scale either side, push a new
// revision, edit both sides' budgets). Every path is explored breadth-first to
// a bound with a hash of visited states; one straight run plays each
// configuration with no event; and each event is tried once at every moment
// of the straight run (each step serving, each step staging, the drain, the
// release of the Instance each side kept on version 1) and played to the end.
//
// At every settled state the model reads: a quiet state no verb moves
// (no-way-out); a version with Instances serving on one side only, beyond
// one batch of that side, and whether traffic names it; a split that sends
// weight to a revision with no serving pod; a step left before both sides
// served its staged capacity; the share each version has on each side against
// the fleet's ratio at tolerances 5, 10 and 20; and the load a thin version
// carries. OME_CANARY_HUNT=full widens the configurations and the bounds.

const (
	pairNamespace  = "ns"
	pairService    = "svc"
	pairHuntEnv    = "OME_CANARY_HUNT"
	pairWorkersEnv = "OME_CANARY_HUNT_WORKERS"
)

// pairRevisions are the revision hashes the model knows: the one the fleet
// stands on, the one the operator pushes, and a later push.
var pairRevisions = []string{"r1", "r2", "r3"}

// pairTolerances are the ratio tolerances the readings are taken at.
var pairTolerances = []int32{5, 10, 20}

func pairRevisionName(comp v1beta1.ComponentType, hash string) string {
	return pairService + "-" + string(comp) + "-" + hash
}

// pairRowKind is what the roll has done to one Instance.
type pairRowKind uint8

const (
	pairRowServing    pairRowKind = iota // the pod is Ready on rev
	pairRowLost                          // the pod is gone; the workload rebuilds it on rev
	pairRowRecreating                    // the old pod is gone; a new pod builds toward target
	pairRowSurging                       // the old pod serves while a replacement builds toward target
	pairRowCreating                      // a new Instance builds toward target
)

var pairRowKindNames = [...]string{"", "lost", "recreate", "surge", "create"}

type pairRow struct {
	kind   pairRowKind
	rev    string // the revision the Instance stands on; empty for a new Instance
	target string // the revision an attempt builds toward
}

func (r pairRow) inFlight() bool {
	return r.kind == pairRowRecreating || r.kind == pairRowSurging || r.kind == pairRowCreating
}

// serving reports a pod of the Instance in rotation: a serving row, or the
// old pod of a surging one.
func (r pairRow) serving() bool { return r.kind == pairRowServing || r.kind == pairRowSurging }

func (r pairRow) String() string {
	s := r.rev
	if s == "" {
		s = "-"
	}
	if r.kind != pairRowServing {
		s += ":" + pairRowKindNames[r.kind]
	}
	if r.target != "" {
		s += ">" + r.target
	}
	return s
}

// pairMemberState is one side's replica as the world holds it.
type pairMemberState struct {
	comp       v1beta1.ComponentType
	rows       []pairRow
	desired    int32
	budget     string // the side's own maxSurge or maxUnavailable
	updateRev  string // the spec's revision
	current    string // the revision the rollup publishes as current
	rollbackTo string // spec.pacing.rollbackToRevision as last written by the dispatch
	partition  *int32 // spec.pacing.partition as last projected
	generation int64
}

func (m pairMemberState) String() string {
	parts := make([]string, 0, len(m.rows))
	for _, r := range m.rows {
		parts = append(parts, r.String())
	}
	s := fmt.Sprintf("%s(%d/%s) %s", m.comp, m.desired, m.budget, strings.Join(parts, " "))
	if m.partition != nil {
		s += fmt.Sprintf(" partition=%d", *m.partition)
	}
	if m.rollbackTo != "" {
		s += " rollbackTo=" + query.RevisionFromName(m.rollbackTo).Hash()
	}
	return s
}

// servingOn counts the side's Instances in rotation on a revision hash.
func (m pairMemberState) servingOn(hash string) int32 {
	var n int32
	for _, r := range m.rows {
		if r.serving() && r.rev == hash {
			n++
		}
	}
	return n
}

// readyOn counts the side's Instances whose pods are Ready on a revision
// hash and nothing else: the rows the canary's capacity gate counts.
func (m pairMemberState) readyOn(hash string) int32 {
	var n int32
	for _, r := range m.rows {
		if r.kind == pairRowServing && r.rev == hash {
			n++
		}
	}
	return n
}

// step is the side's own batch: its budget over its current count, rounded
// up, as the workload engine sizes it.
func (m pairMemberState) step() int32 {
	return utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(m.budget)), int32(len(m.rows)), true)
}

// pairBudgets is how many times the world may still do each costly thing in
// one run.
type pairBudgets struct {
	lostPods, scales, pushes, rollbacks, pauses, budgetEdits, resumes, forces int8
}

// pairConfig is one configuration under exploration.
type pairConfig struct {
	name      string
	shape     [2]int32
	strategy  v1beta1.UpdateStrategyType
	stepsName string
	steps     []v1beta1.RolloutGroupStep
	drain     int32
	budgets   pairBudgets
}

func (c pairConfig) surges() bool { return c.strategy == v1beta1.UpdateStrategySurgeThenDrain }

func (c pairConfig) String() string {
	short := "recreate"
	if c.surges() {
		short = "surge"
	}
	return fmt.Sprintf("%d:%d %s/10%% steps=%s drain=%ds", c.shape[0], c.shape[1], short, c.stepsName, c.drain)
}

// pairMove is one thing the world or the operator does between two passes.
type pairMove struct {
	kind   string
	member int
	idx    int
	delta  int32
}

func (m pairMove) String() string {
	switch m.kind {
	case "serve", "podloss":
		return fmt.Sprintf("%s member %d row %d", m.kind, m.member, m.idx)
	case "scale":
		return fmt.Sprintf("scale member %d by %+d", m.member, m.delta)
	case "budget":
		if m.delta > 0 {
			return "budget raised to 25%"
		}
		return "budget lowered to 1"
	}
	return m.kind
}

// pairState is the model at one moment: the service as the controller holds
// it in memory, both replicas, the clock, the budgets left, what the passes
// since the last move observed, and the moves that led here.
type pairState struct {
	isvc      *v1beta1.InferenceService
	members   []pairMemberState
	now       time.Time
	budgets   pairBudgets
	closes    map[string]int
	scaled    bool
	resumed   bool
	pushed    bool
	unsettled bool
	reopened  string
	// notes are what the passes since the last move observed about a step
	// that was left: the class and the message of each.
	notes []pairFinding
	path  []pairMove
}

func (st *pairState) clone() *pairState {
	out := &pairState{
		isvc:     st.isvc.DeepCopy(),
		members:  make([]pairMemberState, len(st.members)),
		now:      st.now,
		budgets:  st.budgets,
		closes:   make(map[string]int, len(st.closes)),
		scaled:   st.scaled,
		resumed:  st.resumed,
		pushed:   st.pushed,
		reopened: st.reopened,
		path:     append([]pairMove(nil), st.path...),
	}
	for i, m := range st.members {
		m.rows = append([]pairRow(nil), m.rows...)
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

func (st *pairState) String() string {
	parts := make([]string, 0, len(st.members))
	for _, m := range st.members {
		parts = append(parts, m.String())
	}
	return strings.Join(parts, " | ")
}

// pairWorld builds the objects the production code reads from a state and
// runs the controller's pass over them.
type pairWorld struct {
	t      *testing.T
	cfg    pairConfig
	ctx    context.Context
	scheme *runtime.Scheme
	// passes counts controller passes; dispatches the canary dispatches in them.
	passes, dispatches int
}

func newPairWorld(t *testing.T, cfg pairConfig, scheme *runtime.Scheme) *pairWorld {
	return &pairWorld{t: t, cfg: cfg, ctx: context.Background(), scheme: scheme}
}

// pairFailure is an error the production code returned inside a pass; the
// exploration records it as a finding of its own.
type pairFailure string

func (w *pairWorld) fail(format string, args ...any) {
	panic(pairFailure(fmt.Sprintf(format, args...)))
}

// pairISVC is the service: the engine and the decoder declared with their
// counts, and one canary group over both.
func pairISVC(cfg pairConfig) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: pairService, Namespace: pairNamespace, Generation: 1}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(cfg.shape[0]))}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(cfg.shape[1]))}}
	canary := &v1beta1.GroupCanary{Steps: append([]v1beta1.RolloutGroupStep(nil), cfg.steps...)}
	if cfg.drain > 0 {
		canary.ScaleDownDelaySeconds = ptr.To(cfg.drain)
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     canary,
	}}}
	return isvc
}

// initial is the moment after the operator pushed the second revision: every
// Instance serves the first one, and both replicas' target is the second.
func (w *pairWorld) initial() *pairState {
	st := &pairState{
		isvc:    pairISVC(w.cfg),
		now:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		budgets: w.cfg.budgets,
		closes:  map[string]int{},
	}
	for i, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		rows := make([]pairRow, w.cfg.shape[i])
		for idx := range rows {
			rows[idx] = pairRow{kind: pairRowServing, rev: pairRevisions[0]}
		}
		st.members = append(st.members, pairMemberState{
			comp: comp, rows: rows, desired: w.cfg.shape[i], budget: "10%",
			updateRev: pairRevisions[1], current: pairRevisions[0], generation: 1,
		})
	}
	return st
}

// rollup publishes each side's current revision the way the replica's status
// does: the revision every Instance serves, once they all serve one.
func (w *pairWorld) rollup(st *pairState) {
	for i := range st.members {
		m := &st.members[i]
		shared := ""
		for _, r := range m.rows {
			if r.kind != pairRowServing || (shared != "" && r.rev != shared) {
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

// replica is the InferenceReplica a side publishes.
func (w *pairWorld) replica(st *pairState, i int) *v1beta1.InferenceReplica {
	m := st.members[i]
	r := ir(pairNamespace, pairService, m.comp, m.updateRev)
	r.Generation = m.generation
	r.Spec.Replicas = ptr.To(m.desired)
	rolling := &v1beta1.RollingUpdate{}
	budget := intstr.Parse(m.budget)
	if w.cfg.surges() {
		rolling.MaxSurge = &budget
	} else {
		rolling.MaxUnavailable = &budget
	}
	r.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: w.cfg.strategy, RollingUpdate: rolling}}
	if m.partition != nil || m.rollbackTo != "" {
		r.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: m.partition}
		if m.rollbackTo != "" {
			r.Spec.Pacing.RollbackToRevision = ptr.To(m.rollbackTo)
		}
	}
	r.Status.ObservedGeneration = m.generation
	r.Status.CurrentRevision = pairRevisionName(m.comp, m.current)
	r.Status.UpdateRevision = pairRevisionName(m.comp, m.updateRev)
	r.Status.Replicas = int32(len(m.rows))
	attempt := func(step string) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationUpdate, Step: step}
	}
	var ready, updated int32
	for idx, row := range m.rows {
		rs := v1beta1.OMENativeInstanceStatus{Index: int32(idx)}
		switch row.kind {
		case pairRowServing:
			rs.Phase, rs.RunningRevision = v1beta1.OMENativeInstanceReady, pairRevisionName(m.comp, row.rev)
			rs.PodCount, rs.ReadyPodCount, rs.ServingPodCount, rs.AvailablePodCount = 1, 1, 1, 1
		case pairRowLost:
			rs.Phase, rs.RunningRevision = v1beta1.OMENativeInstanceRestarting, pairRevisionName(m.comp, row.rev)
			rs.Operation = &v1beta1.InstanceOperation{ID: "repair", Type: v1beta1.InstanceOperationRestart}
		case pairRowRecreating:
			rs.Phase, rs.RunningRevision, rs.TargetRevision = v1beta1.OMENativeInstanceUpdating, pairRevisionName(m.comp, row.rev), pairRevisionName(m.comp, row.target)
			rs.PodCount, rs.Operation = 1, attempt(workloadtypes.UpdateStepDrain)
		case pairRowSurging:
			rs.Phase, rs.RunningRevision, rs.TargetRevision = v1beta1.OMENativeInstanceUpdating, pairRevisionName(m.comp, row.rev), pairRevisionName(m.comp, row.target)
			rs.PodCount, rs.ReadyPodCount, rs.ServingPodCount, rs.AvailablePodCount = 2, 1, 1, 1
			rs.Operation = attempt(workloadtypes.UpdateStepSurge)
		case pairRowCreating:
			rs.Phase, rs.TargetRevision, rs.PodCount = v1beta1.OMENativeInstanceCreating, pairRevisionName(m.comp, row.target), 1
			rs.Operation = &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate}
		}
		if row.serving() {
			ready++
		}
		if row.kind == pairRowServing && row.rev == m.updateRev {
			updated++
		}
		r.Status.InstanceStatuses = append(r.Status.InstanceStatuses, rs)
	}
	r.Status.ReadyReplicas, r.Status.ServingReplicas, r.Status.AvailableReplicas, r.Status.UpdatedReplicas = ready, ready, ready, updated
	return r
}

// pods are one Instance's pods as the kubelet reports them.
func (w *pairWorld) pods(m pairMemberState, idx int, row pairRow) []runtime.Object {
	var out []runtime.Object
	add := func(rev, suffix string, ready bool) {
		name := fmt.Sprintf("%s-%s-%d-%s-0%s", pairService, m.comp, idx, v1beta1.RunnerNameDefault, suffix)
		pod := canaryRunnerPod(pairNamespace, pairService, string(m.comp), rev, name, v1beta1.RunnerNameDefault, "0")
		pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(idx)
		if !ready {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
		}
		out = append(out, pod)
	}
	switch row.kind {
	case pairRowServing:
		add(row.rev, "", true)
	case pairRowRecreating, pairRowCreating:
		add(row.target, "", false)
	case pairRowSurging:
		add(row.rev, "", true)
		add(row.target, "-s", false)
	}
	return out
}

// objects are everything the pass reads: the service, both replicas and their
// pods, and every revision of both sides.
func (w *pairWorld) objects(st *pairState) []runtime.Object {
	objs := []runtime.Object{st.isvc.DeepCopy()}
	for i, m := range st.members {
		objs = append(objs, w.replica(st, i))
		for idx, row := range m.rows {
			objs = append(objs, w.pods(m, idx, row)...)
		}
		for n, hash := range pairRevisions {
			objs = append(objs, canaryControllerRevision(pairNamespace, pairService, string(m.comp), hash, int64(n+1)))
		}
	}
	return objs
}

func (w *pairWorld) client(st *pairState) client.Client {
	return fake.NewClientBuilder().WithScheme(w.scheme).WithRuntimeObjects(w.objects(st)...).Build()
}

// memberReplicaReader serves a model's member replicas to the coordination
// gate from the model's state, the way the replica controller's live
// reader does: every start of the pass so far is written on its row before
// the next start is asked about. The gate reads nothing else for a canary
// unit. replica builds member i's replica from the state as it stands.
type memberReplicaReader struct {
	namespace, service string
	comps              []v1beta1.ComponentType
	replica            func(i int) *v1beta1.InferenceReplica
}

func (r memberReplicaReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok || key.Namespace != r.namespace {
		return apierrors.NewNotFound(v1beta1.Resource("inferencereplicas"), key.Name)
	}
	for i, comp := range r.comps {
		if key.Name == irprojector.InferenceReplicaName(r.service, comp) {
			*ir = *r.replica(i)
			return nil
		}
	}
	return apierrors.NewNotFound(v1beta1.Resource("inferencereplicas"), key.Name)
}

func (r memberReplicaReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}

// replicaReader serves the pair's replicas from a state.
func (w *pairWorld) replicaReader(st *pairState) memberReplicaReader {
	comps := make([]v1beta1.ComponentType, len(st.members))
	for i, m := range st.members {
		comps[i] = m.comp
	}
	return memberReplicaReader{namespace: pairNamespace, service: pairService, comps: comps, replica: func(i int) *v1beta1.InferenceReplica { return w.replica(st, i) }}
}

// startAdmitted asks the coordination update gate about one more start of
// a side toward target, with admitted starts already taken in this pass,
// under the chart's prefill/decode settings; the plan gate, the batch step
// and whatever else the gate consults for a canary unit answer.
func (w *pairWorld) startAdmitted(st *pairState, i int, from, target string, admitted int32) bool {
	m := st.members[i]
	revision := canaryControllerRevision(pairNamespace, pairService, string(m.comp), target, int64(sort.SearchStrings(pairRevisions, target)+1))
	return coordinationAdmits(w.ctx, w.replicaReader(st), st.isvc, m.comp, pairRevisionName(m.comp, from), revision, w.cfg.strategy, admitted)
}

// coordinationAdmits asks the coordination update gate about one more start
// of a member from one revision toward a target, with admitted starts of
// the pass already taken under the member's strategy, under the chart's
// prefill/decode settings.
func coordinationAdmits(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, from string, target *appsv1.ControllerRevision, strategy v1beta1.UpdateStrategyType, admitted int32) bool {
	var inFlightSurge, inFlightUnavail int32
	if strategy == v1beta1.UpdateStrategySurgeThenDrain {
		inFlightSurge = admitted
	} else {
		inFlightUnavail = admitted
	}
	allowed, _, _ := coordination.EvaluateUpdateGate(ctx, reads, isvc, comp, from, target, nil, pairRules, workloadtypes.UpdateStrategyType(strategy), inFlightSurge, inFlightUnavail)
	return allowed
}

// workloadStep is the workload engine's reaction to the partition and the
// rollback target its replica carries: the lowest-indexed Instances off the
// target are held, the partition's worth of them, and the rest start toward
// the target under the side's strategy and budget, each start admitted by
// the coordination update gate first. A paused rollout starts nothing new.
// An attempt toward a revision that is no longer the target turns toward
// the target.
func (w *pairWorld) workloadStep(st *pairState, i int) {
	m := &st.members[i]
	target := m.updateRev
	if m.rollbackTo != "" {
		target = query.RevisionFromName(m.rollbackTo).Hash()
	}
	var partition int32
	if m.partition != nil {
		partition = *m.partition
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
	budget := m.step()
	var inFlight, admitted int32
	for _, r := range m.rows {
		switch {
		case w.cfg.surges() && r.kind == pairRowSurging:
			inFlight++
		case !w.cfg.surges() && (r.kind == pairRowRecreating || r.kind == pairRowLost):
			inFlight++
		}
	}
	for idx := range m.rows {
		r := &m.rows[idx]
		if held[idx] || r.kind != pairRowServing || r.rev == target {
			continue
		}
		if inFlight >= budget || !w.startAdmitted(st, i, r.rev, target, admitted) {
			break
		}
		r.kind = pairRowRecreating
		if w.cfg.surges() {
			r.kind = pairRowSurging
		}
		r.target = target
		inFlight++
		admitted++
	}
}

// pairActiveRunID is the open run's id, empty when none is open.
func pairActiveRunID(isvc *v1beta1.InferenceService) string {
	if isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return ""
	}
	return isvc.Status.Rollout.ActiveRun.RunID
}

// pairTargetsOf is a run's targets as one string, for the reopen check.
func pairTargetsOf(targets []v1beta1.RolloutRunTarget) string {
	parts := make([]string, 0, len(targets))
	for _, t := range targets {
		parts = append(parts, string(t.Component)+"="+t.Revision)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// pairUnit is the canary group's record as the pass left it.
type pairUnit struct {
	group  *v1beta1.RolloutGroup
	record *v1beta1.CanaryStatus
	phase  v1beta1.RolloutPhase
	state  rollout.CanaryState
}

// unit reads the one canary group's record; nil when the service has none.
func (w *pairWorld) unit(st *pairState) *pairUnit {
	return canaryUnitOf(st.isvc)
}

// canaryUnitOf reads a service's one canary group as the pass left it: the
// group, its primary's record and phase, and the state they make; nil when
// the service has no canary group.
func canaryUnitOf(isvc *v1beta1.InferenceService) *pairUnit {
	groups := rollout.CanaryGroups(isvc, rollout.Policies{})
	if len(groups) == 0 {
		return nil
	}
	g := groups[0]
	primary := rollout.PrimaryOf(g)
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	phase := isvc.Status.Components[primary].RolloutPhase
	return &pairUnit{group: g, record: cs, phase: phase, state: rollout.StateOf(cs, phase, len(g.Canary.Steps))}
}

// stepLeft reads which step the record stands on before a dispatch, so the
// pass can see a step advance and what capacity each side had when it moved.
type pairStepMark struct {
	hash string
	step int32
}

func (w *pairWorld) stepMark(st *pairState) (pairStepMark, bool) {
	u := w.unit(st)
	if u == nil || u.record == nil || u.record.RolledBackRevisionHash != "" {
		return pairStepMark{}, false
	}
	return pairStepMark{hash: u.record.CanaryRevisionHash, step: u.record.CurrentStep}, true
}

// pass is one controller pass: the run layer, the binding of a freshly
// opened run, the partition each side is projected with and the workload's
// reaction to it, then the canary dispatch, and the read-back of the rollback
// target the dispatch wrote on each replica. A step the dispatch leaves is
// checked against the capacity both sides had when it moved.
func (w *pairWorld) pass(st *pairState) {
	w.rollup(st)
	isvc := st.isvc
	c := w.client(st)
	before := pairActiveRunID(isvc)
	out, err := rolloutrun.Reconcile(w.ctx, rolloutrun.Inputs{Client: c, Reader: c, ISVC: isvc, Now: st.now})
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
	for i := range st.members {
		var p *int32
		if resolver := PartitionResolver(isvc, policies, st.members[i].comp); resolver != nil {
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
	mark, marked := w.stepMark(st)
	_, forced := isvc.Annotations[constants.RolloutPromoteForceAnnotation]
	for _, g := range rollout.CanaryGroups(isvc, policies) {
		if _, err := Dispatch(w.ctx, DispatchDeps{
			Client: c, Reader: c, ISVC: isvc, Policies: policies, Now: st.now,
			DefaultReadyTimeout: cellReadyTimeout, Requeue: reconcileRequeue, ParkedRequeue: failedRequeue,
			ComponentRunnerPorts: canaryRunnerPorts(), Group: g,
		}); err != nil {
			w.fail("dispatch: %v (state %s)", err, st)
		}
		w.dispatches++
	}
	if after, ok := w.stepMark(st); marked && ok && after.hash == mark.hash && after.step > mark.step {
		w.noteStepLeft(st, mark, forced)
	}
	for i := range st.members {
		r := &v1beta1.InferenceReplica{}
		if err := c.Get(w.ctx, irprojector.RoleReplicaKey(isvc, st.members[i].comp), r); err != nil {
			w.fail("read replica: %v", err)
		}
		st.members[i].rollbackTo = ""
		if r.Spec.Pacing != nil && r.Spec.Pacing.RollbackToRevision != nil {
			st.members[i].rollbackTo = *r.Spec.Pacing.RollbackToRevision
		}
	}
	after := pairActiveRunID(isvc)
	if before != "" && before != after {
		st.closes[before]++
	}
	if after != "" && after != before && isvc.Status.Rollout.LastRun != nil {
		last := isvc.Status.Rollout.LastRun
		if last.Outcome == v1beta1.RolloutRunCompleted && pairTargetsOf(last.TargetRevisions) == pairTargetsOf(isvc.Status.Rollout.ActiveRun.TargetRevisions) {
			st.reopened = pairTargetsOf(last.TargetRevisions)
		}
	}
}

// noteStepLeft records a step the dispatch left against the Ready canary
// capacity each side had at that moment. A step left with a side short of
// its staged count is a finding; a forced promote is the operator's own
// decision and is reported under its own name.
func (w *pairWorld) noteStepLeft(st *pairState, mark pairStepMark, forced bool) {
	steps := w.cfg.steps
	if int(mark.step) >= len(steps) {
		return
	}
	step := steps[mark.step]
	var short []string
	for _, m := range st.members {
		staged := stepStagedCount(step, m.desired)
		if ready := m.readyOn(mark.hash); ready < staged {
			short = append(short, fmt.Sprintf("%s %d of %d", m.comp, ready, staged))
		}
	}
	if len(short) == 0 {
		return
	}
	class := "promoted-short"
	if forced {
		class = "promoted-short-forced"
	}
	st.notes = append(st.notes, pairFinding{class: class, severity: float64(len(short)), message: fmt.Sprintf("step %d was left with a side short of its staged canary capacity: %s", mark.step, strings.Join(short, ", "))})
}

// settle runs passes until a pass changes nothing the key reads.
func (w *pairWorld) settle(st *pairState) {
	for round := 0; round < 8; round++ {
		before := st.key()
		w.pass(st)
		if st.key() == before {
			return
		}
	}
	st.unsettled = true
}

// deadlines are the moments the record waits for: the end of a timed pause,
// the ready timeout of a capacity wait, and the end of the drain window.
func (w *pairWorld) deadlines(st *pairState) []time.Time {
	var out []time.Time
	u := w.unit(st)
	if u == nil || u.record == nil {
		return nil
	}
	cs := u.record
	if cs.CapacityWaitSince != nil {
		out = append(out, cs.CapacityWaitSince.Add(cellReadyTimeout))
	}
	steps := u.group.Canary.Steps
	if cs.CurrentStep < 0 || int(cs.CurrentStep) >= len(steps) || cs.StepEnteredTime == nil {
		return out
	}
	step := steps[cs.CurrentStep]
	if step.Pause != nil && step.Pause.Duration != nil {
		out = append(out, cs.StepEnteredTime.Add(step.Pause.Duration.Duration))
	}
	if delay := u.group.Canary.ScaleDownDelaySeconds; int(cs.CurrentStep) == len(steps)-1 && delay != nil && *delay > 0 {
		out = append(out, cs.StepEnteredTime.Add(time.Duration(*delay)*time.Second))
	}
	return out
}

// nextDeadline is the soonest deadline still ahead of the clock.
func (w *pairWorld) nextDeadline(st *pairState) (time.Time, bool) {
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
// both sides, the service's annotations and status, and the budgets left.
// The clock itself is not in it: only which deadlines it has passed matters
// to the code, and clockKey adds that.
func (st *pairState) key() string {
	var b strings.Builder
	for _, m := range st.members {
		for _, r := range m.rows {
			fmt.Fprintf(&b, "%d%s>%s,", r.kind, r.rev, r.target)
		}
		p := "-"
		if m.partition != nil {
			p = strconv.Itoa(int(*m.partition))
		}
		fmt.Fprintf(&b, "|d%d b%s u%s c%s rb%s p%s g%d;", m.desired, m.budget, m.updateRev, m.current, m.rollbackTo, p, m.generation)
	}
	keys := make([]string, 0, len(st.isvc.Annotations))
	for k, v := range st.isvc.Annotations {
		keys = append(keys, k+"="+v)
	}
	sort.Strings(keys)
	b.WriteString("a:" + strings.Join(keys, ",") + ";")
	if r := st.isvc.Status.Rollout; r != nil {
		if r.ActiveRun != nil {
			b.WriteString("run:" + pairTargetsOf(r.ActiveRun.TargetRevisions))
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
	fmt.Fprintf(&b, "budgets:%+v;", st.budgets)
	return b.String()
}

// clockKey adds which deadlines the clock has passed; it is read beside key
// so two states that differ only in a passed deadline are explored apart.
func (w *pairWorld) clockKey(st *pairState) string {
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
// and the unit's record at the done mark and Stable, or rolled back.
func (w *pairWorld) finished(st *pairState) bool {
	if v1beta1.RolloutRunActive(st.isvc) {
		return false
	}
	for _, m := range st.members {
		for _, r := range m.rows {
			if r.kind != pairRowServing {
				return false
			}
		}
	}
	u := w.unit(st)
	if u == nil {
		return true
	}
	switch u.state {
	case rollout.CanaryStateIdle, rollout.CanaryStateRolledBack:
		return true
	case rollout.CanaryStateDone:
		return u.phase == v1beta1.RolloutPhaseStable
	}
	return false
}

// progressMoves are the moves the world makes on its own: a building or
// lost Instance comes up (the lowest and the highest of each kind per side),
// and the clock reaches the next deadline or passes them all.
func (w *pairWorld) progressMoves(st *pairState) []pairMove {
	var out []pairMove
	for i, m := range st.members {
		first := map[pairRowKind]int{}
		last := map[pairRowKind]int{}
		for idx, r := range m.rows {
			if r.kind == pairRowServing {
				continue
			}
			if _, ok := first[r.kind]; !ok {
				first[r.kind] = idx
			}
			last[r.kind] = idx
		}
		for _, kind := range []pairRowKind{pairRowRecreating, pairRowSurging, pairRowCreating, pairRowLost} {
			if idx, ok := first[kind]; ok {
				out = append(out, pairMove{kind: "serve", member: i, idx: idx})
				if last[kind] != idx {
					out = append(out, pairMove{kind: "serve", member: i, idx: last[kind]})
				}
			}
		}
	}
	if _, ok := w.nextDeadline(st); ok {
		out = append(out, pairMove{kind: "clock-next"}, pairMove{kind: "clock-all"})
	}
	return out
}

// releaseMoves are the operator verbs a hold names as its way out: a promote
// at a gate, the end of a pause, a resume of a Failed or rolled-back unit, a
// rollback of a Failed unit. The exploration spends budgets on the resume and
// the rollback; the classification of a quiet state asks for them
// regardless, since a budget is the model's bound and not a hold.
func (w *pairWorld) releaseMoves(st *pairState, ignoreBudget bool) []pairMove {
	var out []pairMove
	ann := st.isvc.Annotations
	budgets := st.budgets
	if ignoreBudget {
		budgets = pairBudgets{resumes: 1, rollbacks: 1}
	}
	if paused, _ := constants.RolloutPauseState(ann); paused {
		out = append(out, pairMove{kind: "unpause"})
	}
	u := w.unit(st)
	if u == nil || u.record == nil {
		return out
	}
	_, promoting := ann[constants.RolloutPromoteAnnotation]
	switch u.state {
	case rollout.CanaryStateServing, rollout.CanaryStatePreHold, rollout.CanaryStateDraining:
		if !promoting {
			out = append(out, pairMove{kind: "promote"})
		}
	case rollout.CanaryStateFailed, rollout.CanaryStateRolledBack:
		if _, resuming := ann[constants.RolloutResumeAnnotation]; !resuming && budgets.resumes > 0 {
			out = append(out, pairMove{kind: "resume"})
		}
		if u.state == rollout.CanaryStateFailed && budgets.rollbacks > 0 && u.record.RolledBackRevisionHash == "" {
			out = append(out, pairMove{kind: "rollback"})
		}
	}
	return out
}

// eventMoves are the operator's and the world's costly moves, each limited by
// its budget: a pause, a rollback, a forced promote, a pod lost on the canary
// side or the stable side of either member, a scale of either member by one,
// a new revision pushed, both sides' budgets raised or lowered.
func (w *pairWorld) eventMoves(st *pairState) []pairMove {
	var out []pairMove
	ann := st.isvc.Annotations
	b := st.budgets
	if paused, _ := constants.RolloutPauseState(ann); !paused && b.pauses > 0 {
		out = append(out, pairMove{kind: "pause"})
	}
	u := w.unit(st)
	live := u != nil && u.record != nil && u.state != rollout.CanaryStateDone && u.state != rollout.CanaryStateRolledBack && u.state != rollout.CanaryStateRollingBack
	if live {
		if b.rollbacks > 0 && u.record.RolledBackRevisionHash == "" && u.state != rollout.CanaryStateFailed {
			out = append(out, pairMove{kind: "rollback"})
		}
		if _, forcing := ann[constants.RolloutPromoteForceAnnotation]; b.forces > 0 && !forcing && u.state != rollout.CanaryStateFailed {
			out = append(out, pairMove{kind: "force"})
		}
		if b.lostPods > 0 {
			for i, m := range st.members {
				canaryRow, stableRow := -1, -1
				for idx, r := range m.rows {
					if r.kind != pairRowServing {
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
						out = append(out, pairMove{kind: "podloss", member: i, idx: idx})
					}
				}
			}
		}
	}
	if b.scales > 0 {
		for i, m := range st.members {
			out = append(out, pairMove{kind: "scale", member: i, delta: 1})
			if m.desired > 1 {
				out = append(out, pairMove{kind: "scale", member: i, delta: -1})
			}
		}
	}
	if b.pushes > 0 && st.members[0].updateRev != pairRevisions[2] {
		out = append(out, pairMove{kind: "push"})
	}
	if b.budgetEdits > 0 {
		out = append(out, pairMove{kind: "budget", delta: 1}, pairMove{kind: "budget", delta: -1})
	}
	return out
}

// apply makes one move and lets the controller settle on it. Every move
// but the clock's takes one second, so two runs opened one move apart do
// not share the second their ids are minted from (runID hashes the open
// time at one-second resolution).
func (w *pairWorld) apply(st *pairState, mv pairMove) *pairState {
	next := st.clone()
	next.path = append(next.path, mv)
	next.now = next.now.Add(time.Second)
	isvc := next.isvc
	switch mv.kind {
	case "serve":
		r := &next.members[mv.member].rows[mv.idx]
		switch r.kind {
		case pairRowRecreating, pairRowSurging, pairRowCreating:
			r.kind, r.rev, r.target = pairRowServing, r.target, ""
		case pairRowLost:
			r.kind = pairRowServing
		}
	case "clock-next":
		if d, ok := w.nextDeadline(next); ok {
			next.now = d.Add(time.Second)
		}
	case "clock-all":
		next.now = next.now.Add(2 * time.Hour)
	case "promote":
		if u := w.unit(next); u != nil && u.record != nil {
			annotate(isvc, constants.RolloutPromoteAnnotation, u.record.CanaryRevisionHash)
		}
	case "force":
		next.budgets.forces--
		if u := w.unit(next); u != nil && u.record != nil {
			annotate(isvc, constants.RolloutPromoteForceAnnotation, u.record.CanaryRevisionHash)
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
		if u := w.unit(next); u != nil && u.record != nil {
			annotate(isvc, constants.RolloutResumeAnnotation, u.record.CanaryRevisionHash)
		}
	case "podloss":
		next.budgets.lostPods--
		r := &next.members[mv.member].rows[mv.idx]
		r.kind, r.target = pairRowLost, ""
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
			target := m.updateRev
			if m.rollbackTo != "" {
				target = query.RevisionFromName(m.rollbackTo).Hash()
			}
			for k := int32(0); k < mv.delta; k++ {
				m.rows = append(m.rows, pairRow{kind: pairRowCreating, target: target})
			}
		}
	case "push":
		next.budgets.pushes--
		next.pushed = true
		for i := range next.members {
			next.members[i].updateRev = pairRevisions[2]
			next.members[i].generation++
		}
		isvc.Generation++
	case "budget":
		next.budgets.budgetEdits--
		budget := "25%"
		if mv.delta < 0 {
			budget = "1"
		}
		for i := range next.members {
			next.members[i].budget = budget
			next.members[i].generation++
		}
		isvc.Generation++
	}
	w.settle(next)
	return next
}

// pairFinding is one reading at one state. severity orders the states of
// one class, so the report shows the worst picture of each: a weight, a
// percentage of drift, a load factor, a count over a batch.
type pairFinding struct {
	class    string
	message  string
	severity float64
	state    *pairState
}

// ratioDriftPercent is how far the picture e:d (engine and decoder
// Instances of one version) drifts from the fleet's ratio E:D, as the
// percentage the ratio band and the API's tolerance read: |e/d - E/D| over
// E/D, which is |e*D - d*E| over d*E.
func ratioDriftPercent(e, d, fleetE, fleetD int32) float64 {
	diff := float64(e)*float64(fleetD) - float64(d)*float64(fleetE)
	if diff < 0 {
		diff = -diff
	}
	return diff * 100 / (float64(d) * float64(fleetE))
}

// shareSkewPercent is how far the two sides' shares of one version sit
// apart, as the paired batch plan reads a tolerance: |e/E - d/D| in percent.
func shareSkewPercent(e, d, fleetE, fleetD int32) float64 {
	diff := float64(e)*float64(fleetD) - float64(d)*float64(fleetE)
	if diff < 0 {
		diff = -diff
	}
	return diff * 100 / (float64(fleetE) * float64(fleetD))
}

// trafficOn is the weight a member's split sends to a revision hash.
func (w *pairWorld) trafficOn(st *pairState, m pairMemberState, hash string) int32 {
	name := coordination.PerRevisionServiceName(pairService, m.comp, hash)
	for _, t := range st.isvc.Status.Components[m.comp].Traffic {
		if t.RevisionName == name {
			return t.Percent
		}
	}
	return 0
}

// check reads every invariant at one settled state.
func (w *pairWorld) check(st *pairState) []pairFinding {
	var out []pairFinding
	add := func(class string, severity float64, format string, args ...any) {
		out = append(out, pairFinding{class: class, message: fmt.Sprintf(format, args...), severity: severity, state: st})
	}
	for _, n := range st.notes {
		out = append(out, pairFinding{class: n.class, message: n.message, severity: n.severity, state: st})
	}
	if st.unsettled {
		add("unsettled", 1, "the controller kept changing its status for eight passes with no move in between")
	}
	for id, n := range st.closes {
		if n > 1 {
			add("run-closed-twice", float64(n), "run %s closed %d times", id, n)
		}
	}
	if st.reopened != "" {
		add("reopen-after-complete", 1, "a run opened toward %s right after the run that closed Completed on the same targets", st.reopened)
	}
	u := w.unit(st)
	held, heldStable := false, ""
	if u != nil {
		switch u.state {
		case rollout.CanaryStateFailed, rollout.CanaryStateRollingBack, rollout.CanaryStateRolledBack:
			held = true
			if u.record != nil {
				heldStable = u.record.StableRevisionHash
			}
		}
	}
	engine, decoder := st.members[0], st.members[1]
	// (c) traffic names only revisions that serve. A member with nothing in
	// rotation keeps the step's weight, a split that names an unserving
	// revision by rule. A rollback or a Failed hold moves a stable share with
	// no pod behind it onto the revision that serves, so a split left on an
	// empty stable revision under a hold fails; a stopped canary whose canary
	// pod is lost keeps the step's share on the canary revision until the
	// workload rebuilds the pod, which is reported apart.
	for _, m := range st.members {
		var total int32
		for _, hash := range pairRevisions {
			total += m.servingOn(hash)
		}
		for _, hash := range pairRevisions {
			weight := w.trafficOn(st, m, hash)
			if weight <= 0 || m.servingOn(hash) > 0 {
				continue
			}
			class := "traffic-empty-revision"
			switch {
			case total == 0:
				class = "traffic-empty-revision-nothing-serves"
			case held && hash == heldStable:
				class = "traffic-empty-stable-revision-under-a-hold"
			case held:
				class = "traffic-empty-canary-revision-under-a-hold"
			}
			add(class, float64(weight), "%s traffic sends %d percent to revision %s, which has no pod in rotation (%d pods of the member serve)", m.comp, weight, hash, total)
		}
	}
	// (b) a version stands with one side only. With weight on it, the member
	// that still runs the version keeps sending it the step's share while the
	// other member has moved off it; without weight it is the two sides
	// staging at their own pace, counted once the populated side is past one
	// batch.
	for _, hash := range pairRevisions {
		e, d := engine.servingOn(hash), decoder.servingOn(hash)
		if (e == 0) == (d == 0) {
			continue
		}
		side, count, step := engine, e, engine.step()
		if e == 0 {
			side, count, step = decoder, d, decoder.step()
		}
		other := st.members[1-w.indexOf(side.comp)]
		weight, otherWeight := w.trafficOn(st, side, hash), w.trafficOn(st, other, hash)
		switch {
		case weight > 0 || otherWeight > 0:
			add("one-sided-version-with-traffic", float64(weight), "revision %s serves on %d %s Instances and no %s Instance; the %s split sends it %d percent, the %s split %d", hash, count, side.comp, other.comp, side.comp, weight, other.comp, otherWeight)
		case count > step:
			add("one-sided-version-beyond-a-batch", float64(count)/float64(step), "revision %s serves on %d %s Instances (one batch is %d) and on no %s Instance", hash, count, side.comp, step, other.comp)
		}
	}
	// The per-version ratio against the fleet's, at each tolerance, in the
	// two senses the engine uses: the drift of the version's engine:decoder
	// ratio from the fleet's (the ratio band and the API's tolerance) and the
	// gap between the two sides' shares of the version (the paired batch
	// plan). A picture with nothing in flight is one the operator looks at
	// for the length of a gate; one with Instances building is the staging
	// under way.
	inFlight := false
	for _, m := range st.members {
		for _, r := range m.rows {
			if r.kind != pairRowServing {
				inFlight = true
			}
		}
	}
	rest := ""
	if !inFlight {
		rest = "-at-rest"
	}
	for _, hash := range pairRevisions {
		e, d := engine.servingOn(hash), decoder.servingOn(hash)
		if e == 0 || d == 0 {
			continue
		}
		drift := ratioDriftPercent(e, d, engine.desired, decoder.desired)
		skew := shareSkewPercent(e, d, engine.desired, decoder.desired)
		for _, tolerance := range pairTolerances {
			if drift > float64(tolerance) {
				add(fmt.Sprintf("ratio-drift-%d%s", tolerance, rest), drift, "revision %s serves on %d engine and %d decoder Instances, a ratio %.0f percent off the fleet's %d:%d (tolerance %d)", hash, e, d, drift, engine.desired, decoder.desired, tolerance)
			}
			if skew > float64(tolerance) {
				add(fmt.Sprintf("share-skew-%d%s", tolerance, rest), skew, "revision %s serves on %d of %d engine and %d of %d decoder Instances, shares %.0f percent apart (tolerance %d)", hash, e, engine.desired, d, decoder.desired, skew, tolerance)
			}
		}
	}
	// The load a version carries: a member's split sends weight w to a
	// revision serving s of its D Instances; s Instances carry what w percent
	// of D would. A factor above two is reported.
	for _, m := range st.members {
		for _, hash := range pairRevisions {
			weight, serving := w.trafficOn(st, m, hash), m.servingOn(hash)
			if weight <= 0 || serving == 0 {
				continue
			}
			factor := float64(weight) * float64(m.desired) / (100 * float64(serving))
			if factor > 2 {
				add("traffic-on-a-thin-version", factor, "%s revision %s carries %d percent of the traffic on %d of %d Instances (%.1f times the even share)", m.comp, hash, weight, serving, m.desired, factor)
			}
		}
	}
	// The held share of the fleet is what the step asks for. After a scale
	// the created Instances run the canary revision, a known picture named
	// apart; after a resume the unit continues at a step whose staged count
	// covers the fleet, so a share off the step there is named apart and
	// not known.
	suffix := ""
	switch {
	case st.scaled:
		suffix = "-after-scale"
	case st.resumed:
		suffix = "-after-resume"
	}
	if u != nil && u.record != nil && u.record.RolledBackRevisionHash == "" && u.record.CurrentStep >= 0 && int(u.record.CurrentStep) < len(u.group.Canary.Steps) {
		cs := u.record
		step := u.group.Canary.Steps[cs.CurrentStep]
		for _, m := range st.members {
			minimum := stepPartition(step, m.desired)
			var kept, onCanary int32
			for _, r := range m.rows {
				switch {
				case r.rev == cs.CanaryRevisionHash || (r.inFlight() && r.target == cs.CanaryRevisionHash):
					onCanary++
				case r.kind == pairRowServing || r.kind == pairRowLost || r.kind == pairRowSurging:
					kept++
				}
			}
			if kept < minimum {
				add("held-short"+suffix, float64(minimum-kept), "%s keeps %d Instances off the canary revision at step %d, the step holds %d of %d", m.comp, kept, cs.CurrentStep, minimum, m.desired)
			}
			if onCanary > m.desired-minimum {
				add("share-exceeded"+suffix, float64(onCanary-(m.desired-minimum)), "%s runs or builds %d Instances on the canary revision at step %d, the step stages %d of %d", m.comp, onCanary, cs.CurrentStep, m.desired-minimum, m.desired)
			}
		}
	}
	// (a) the run reaches an end: not finished, nothing moves on its own, and
	// no hold names a way out that works. An end is counted by how the last
	// run closed.
	if w.finished(st) {
		outcome := "no-run"
		if r := st.isvc.Status.Rollout; r != nil && r.LastRun != nil {
			outcome = strings.ToLower(string(r.LastRun.Outcome))
		}
		add("end-"+outcome, 0, "the run reached its end: %s", w.describe(st))
	} else if len(w.progressMoves(st)) == 0 {
		class, message := w.classifyQuiet(st)
		add(class, 0, "%s", message)
	}
	return out
}

// indexOf is a side's position in the members: the engine first, the
// decoder second.
func (w *pairWorld) indexOf(c v1beta1.ComponentType) int {
	if c == v1beta1.EngineComponent {
		return 0
	}
	return 1
}

// describe says where a state stands, for a person reading the report.
func (w *pairWorld) describe(st *pairState) string {
	var parts []string
	if r := st.isvc.Status.Rollout; r != nil {
		switch {
		case r.ActiveRun != nil:
			parts = append(parts, "run open toward "+pairTargetsOf(r.ActiveRun.TargetRevisions))
		case r.LastRun != nil:
			parts = append(parts, "last run "+string(r.LastRun.Outcome))
		}
	}
	if u := w.unit(st); u != nil {
		if u.record == nil {
			parts = append(parts, fmt.Sprintf("no record, phase %s", u.phase))
		} else {
			cs := u.record
			s := fmt.Sprintf("%s at step %d of %d, phase %s, weight %d", u.state, cs.CurrentStep, len(u.group.Canary.Steps), u.phase, cs.ObservedTrafficWeight)
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
	}
	for _, m := range st.members {
		var split []string
		for _, hash := range pairRevisions {
			if weight := w.trafficOn(st, m, hash); weight > 0 {
				split = append(split, fmt.Sprintf("%s=%d", hash, weight))
			}
		}
		if len(split) > 0 {
			parts = append(parts, fmt.Sprintf("%s traffic %s", m.comp, strings.Join(split, " ")))
		}
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
func (w *pairWorld) classifyQuiet(st *pairState) (string, string) {
	key := w.clockKey(st)
	var ways []string
	for _, mv := range w.releaseMoves(st, true) {
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
	class := "hold-" + strings.Join(ways, "+")
	if u := w.unit(st); u != nil && u.state == rollout.CanaryStateFailed {
		class = "hold-failed"
	}
	if paused, _ := constants.RolloutPauseState(st.isvc.Annotations); paused {
		class = "hold-paused"
	}
	return class, description
}

// pairReport is what one configuration's exploration found.
type pairReport struct {
	states, depth, runs, probes int
	findings                    map[string]pairFinding
	counts                      map[string]int
}

func newPairReport() pairReport {
	return pairReport{findings: map[string]pairFinding{}, counts: map[string]int{}}
}

// record counts every reading at a state and keeps, per class, the worst
// picture seen (the first one among equals).
func (w *pairWorld) record(rep *pairReport, st *pairState) {
	for _, f := range w.check(st) {
		rep.counts[f.class]++
		if have, ok := rep.findings[f.class]; !ok || f.severity > have.severity {
			rep.findings[f.class] = f
		}
	}
}

// explore walks every path breadth-first to the bounds with a hash of
// visited states.
func (w *pairWorld) explore(rep *pairReport, maxStates, maxDepth int) {
	start := w.initial()
	w.settle(start)
	visited := map[string]bool{w.clockKey(start): true}
	queue := []*pairState{start}
	for len(queue) > 0 && rep.states < maxStates {
		st := queue[0]
		queue = queue[1:]
		rep.states++
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
		w.record(rep, st)
		if w.finished(st) {
			continue
		}
		if len(st.path) >= maxDepth {
			rep.counts["depth"]++
			if _, ok := rep.findings["depth"]; !ok {
				rep.findings["depth"] = pairFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
			}
			continue
		}
		moves := append(w.progressMoves(st), w.releaseMoves(st, false)...)
		moves = append(moves, w.eventMoves(st)...)
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
}

// nextStraightMove is the world's next move when nothing goes wrong: a
// building Instance of the side whose turn it is comes up, else the other
// side's, else the clock reaches the next deadline, else the operator answers
// the hold with the verb it names.
func (w *pairWorld) nextStraightMove(st *pairState, turn int) (pairMove, bool) {
	progress := w.progressMoves(st)
	for round := 0; round < len(st.members); round++ {
		side := (turn + round) % len(st.members)
		for _, mv := range progress {
			if mv.kind == "serve" && mv.member == side {
				return mv, true
			}
		}
	}
	for _, mv := range progress {
		if mv.kind == "clock-next" {
			return mv, true
		}
	}
	if release := w.releaseMoves(st, false); len(release) > 0 {
		return release[0], true
	}
	return pairMove{}, false
}

// playOut plays a state to its end with no further event: the sides come up
// in turns, the clock reaches each deadline, and the operator answers every
// hold with the verb it names. It records every state it visits and returns
// the end state; a play that reaches no end is reported under depth.
func (w *pairWorld) playOut(rep *pairReport, st *pairState, maxMoves int, visit func(*pairState)) *pairState {
	rep.runs++
	for depth := 0; ; depth++ {
		w.record(rep, st)
		if visit != nil {
			visit(st)
		}
		if w.finished(st) {
			return st
		}
		mv, ok := w.nextStraightMove(st, depth)
		if !ok || depth >= maxMoves {
			rep.counts["depth"]++
			if _, ok := rep.findings["depth"]; !ok {
				rep.findings["depth"] = pairFinding{class: "depth", message: fmt.Sprintf("the play did not reach an end in %d moves: %s", depth, w.describe(st)), state: st}
			}
			return st
		}
		st = w.apply(st, mv)
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
	}
}

// straightRun plays one run from the push with no event.
func (w *pairWorld) straightRun(rep *pairReport, maxMoves int) *pairState {
	st := w.initial()
	w.settle(st)
	return w.playOut(rep, st, maxMoves, nil)
}

// momentOf names the moment of a run a state stands at, for the probes: a
// step serving its split at a gate, a step staging its capacity, the final
// step draining, the kept Instance of each side rolling after the release. Empty for the
// moments the probes do not stop at.
func (w *pairWorld) momentOf(st *pairState) string {
	u := w.unit(st)
	if u == nil || u.record == nil {
		return ""
	}
	step := u.record.CurrentStep
	switch u.state {
	case rollout.CanaryStateServing:
		return fmt.Sprintf("serving-%d", step)
	case rollout.CanaryStateStaging:
		if step > 0 && u.record.CapacityWaitSince != nil {
			return fmt.Sprintf("staging-%d", step)
		}
	case rollout.CanaryStateDraining:
		if u.record.ObservedTrafficWeight == 100 {
			return "draining"
		}
	case rollout.CanaryStateDone:
		if u.phase == v1beta1.RolloutPhasePromoting {
			return "releasing"
		}
	}
	return ""
}

// pairEvents are the events a probe tries once at each moment, in the words
// the report uses; each resolves to a move when the state allows it.
var pairEvents = []string{
	"engine canary pod lost", "engine stable pod lost", "decoder canary pod lost", "decoder stable pod lost",
	"engine scaled up", "engine scaled down", "decoder scaled up", "decoder scaled down",
	"pause", "rollback", "second push", "budgets raised", "budgets lowered",
}

// eventMove resolves one named event to a move at a state; false when the
// state has nothing the event could act on.
func (w *pairWorld) eventMove(st *pairState, event string) (pairMove, bool) {
	u := w.unit(st)
	for _, mv := range w.eventMoves(st) {
		switch event {
		case "engine canary pod lost", "engine stable pod lost", "decoder canary pod lost", "decoder stable pod lost":
			if mv.kind != "podloss" || u == nil || u.record == nil {
				continue
			}
			m := st.members[mv.member]
			hash := u.record.CanaryRevisionHash
			if strings.Contains(event, "stable") {
				hash = u.record.StableRevisionHash
			}
			if strings.HasPrefix(event, string(m.comp)) && m.rows[mv.idx].rev == hash {
				return mv, true
			}
		case "engine scaled up", "engine scaled down", "decoder scaled up", "decoder scaled down":
			if mv.kind != "scale" || !strings.HasPrefix(event, string(st.members[mv.member].comp)) {
				continue
			}
			if (mv.delta > 0) == strings.HasSuffix(event, "up") {
				return mv, true
			}
		case "pause":
			if mv.kind == "pause" {
				return mv, true
			}
		case "rollback":
			if mv.kind == "rollback" {
				return mv, true
			}
		case "second push":
			if mv.kind == "push" {
				return mv, true
			}
		case "budgets raised":
			if mv.kind == "budget" && mv.delta > 0 {
				return mv, true
			}
		case "budgets lowered":
			if mv.kind == "budget" && mv.delta < 0 {
				return mv, true
			}
		}
	}
	return pairMove{}, false
}

// probe plays the straight run once to collect its moments, then tries every
// event once at every moment and plays each to the end. Each probe's path is
// the moment's path plus the event, so a finding carries the shortest story
// that reaches it.
func (w *pairWorld) probe(rep *pairReport, maxMoves int) {
	moments := map[string]*pairState{}
	var order []string
	start := w.initial()
	w.settle(start)
	w.playOut(rep, start, maxMoves, func(st *pairState) {
		if name := w.momentOf(st); name != "" {
			if _, seen := moments[name]; !seen {
				moments[name] = st
				order = append(order, name)
			}
		}
	})
	for _, name := range order {
		for _, event := range pairEvents {
			mv, ok := w.eventMove(moments[name], event)
			if !ok {
				continue
			}
			rep.probes++
			w.playOut(rep, w.apply(moments[name], mv), maxMoves, nil)
		}
	}
}

// randomRuns plays whole runs with a fixed seed, favouring the moves that
// bring a run to its end, to reach the depths the breadth-first walk does not.
func (w *pairWorld) randomRuns(rep *pairReport, seed int64, runs, maxDepth int) {
	rng := rand.New(rand.NewSource(seed))
	for r := 0; r < runs; r++ {
		st := w.initial()
		w.settle(st)
		rep.runs++
		for depth := 0; ; depth++ {
			w.record(rep, st)
			if w.finished(st) {
				break
			}
			forward := append(w.progressMoves(st), w.releaseMoves(st, false)...)
			events := w.eventMoves(st)
			if len(forward) == 0 && len(events) == 0 {
				break
			}
			if depth >= maxDepth {
				rep.counts["depth"]++
				if _, ok := rep.findings["depth"]; !ok {
					rep.findings["depth"] = pairFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
				}
				break
			}
			var mv pairMove
			if len(forward) == 0 || (len(events) > 0 && rng.Intn(4) == 0) {
				mv = events[rng.Intn(len(events))]
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

func pairPath(st *pairState) string {
	parts := make([]string, 0, len(st.path))
	for _, mv := range st.path {
		parts = append(parts, mv.String())
	}
	return strings.Join(parts, " -> ")
}

// The step lists under exploration: the reference lists 10/30/60/100 and
// 25/50/100, each with its intermediate steps gated by a manual pause or a
// ten-minute timed one, and traffic following capacity at every step.
func pairStepList(name string) []v1beta1.RolloutGroupStep {
	gate := func(manual bool) *v1beta1.RolloutPause {
		if manual {
			return &v1beta1.RolloutPause{}
		}
		return &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 10 * time.Minute}}
	}
	build := func(percents []int32, manual bool) []v1beta1.RolloutGroupStep {
		var out []v1beta1.RolloutGroupStep
		for i, p := range percents {
			step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString(fmt.Sprintf("%d%%", p)), Traffic: p}
			if i < len(percents)-1 {
				step.Pause = gate(manual)
			}
			out = append(out, step)
		}
		return out
	}
	switch name {
	case "four-manual":
		return build([]int32{10, 30, 60, 100}, true)
	case "four-timed":
		return build([]int32{10, 30, 60, 100}, false)
	case "three-manual":
		return build([]int32{25, 50, 100}, true)
	case "three-timed":
		return build([]int32{25, 50, 100}, false)
	}
	panic("unknown step list " + name)
}

var pairStepNames = []string{"four-manual", "four-timed", "three-manual", "three-timed"}

// pairShapes are the reference prefill/decode shapes, engine then decoder.
var pairShapes = [][2]int32{{12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}}

func pairFullMode() bool { return os.Getenv(pairHuntEnv) == "full" }

// pairWorkers is how many configurations explore at once; the machine is
// shared, so the default stays small and the environment raises it.
func pairWorkers() int {
	if n, err := strconv.Atoi(os.Getenv(pairWorkersEnv)); err == nil && n > 0 {
		return n
	}
	return 4
}

func pairDefaultBudgets(full bool) pairBudgets {
	b := pairBudgets{lostPods: 1, scales: 1, pushes: 1, rollbacks: 1, pauses: 1, budgetEdits: 1, resumes: 1}
	if full {
		b.forces, b.lostPods = 1, 2
	}
	return b
}

// pairConfigFor is one configuration: a shape under one strategy at 10
// percent, one step list and one drain window.
func pairConfigFor(shape [2]int32, strategy v1beta1.UpdateStrategyType, stepsName string, drain int32, full bool) pairConfig {
	cfg := pairConfig{shape: shape, strategy: strategy, stepsName: stepsName, steps: pairStepList(stepsName), drain: drain, budgets: pairDefaultBudgets(full)}
	cfg.name = cfg.String()
	return cfg
}

// pairConfigs lists the configurations to explore. The default run takes one
// configuration per shape, alternating the strategy, the step list and the
// drain window; the full run takes every shape under both strategies and all
// four step lists, the drain window alternating.
func pairConfigs(full bool) []pairConfig {
	strategies := []v1beta1.UpdateStrategyType{v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain}
	var out []pairConfig
	if !full {
		for si, shape := range pairShapes {
			out = append(out, pairConfigFor(shape, strategies[si%2], pairStepNames[si%len(pairStepNames)], int32(600*(si%2)), full))
		}
		// The 12:8 shape, a common shape, under both
		// strategies and both step families.
		out = append(out,
			pairConfigFor(pairShapes[0], strategies[1], "three-manual", 0, full),
			pairConfigFor(pairShapes[0], strategies[0], "three-timed", 600, full),
		)
		return out
	}
	for si, shape := range pairShapes {
		for _, strategy := range strategies {
			for ti, steps := range pairStepNames {
				var drain int32
				if (si+ti)%2 == 1 {
					drain = 600
				}
				out = append(out, pairConfigFor(shape, strategy, steps, drain, full))
			}
		}
	}
	return out
}

// pairKnownClasses are the readings the explorer reports without failing,
// each in words: behaviour left open, and pictures rather than holds.
var pairKnownClasses = map[string]string{
	"traffic-empty-revision-nothing-serves":      "a member with nothing in rotation keeps the step's weight: a loss with nothing to move onto is the workload's repair, not a traffic decision",
	"traffic-empty-canary-revision-under-a-hold": "a stopped canary whose canary pod is lost keeps the step's share on the canary revision until the workload rebuilds the pod: the stop leaves the split where it stood, and only an empty stable revision moves it (left open)",
	"held-short-after-scale":                     "a scale under a step changes the staged share: a created Instance runs the canary revision, a removed one was a staged one (left open)",
	"share-exceeded-after-scale":                 "a created Instance runs the canary revision, so the unit runs more on it than the step stages (left open)",
	"promoted-short-forced":                      "a forced promote opens the step without its capacity by the operator's own word (the CanaryStepForced event)",
	"one-sided-version-beyond-a-batch":           "one side's new Instances serve a version the other side has none on, past one batch of that side, with no traffic on it yet: the batch step moves the decoder first inside every batch, so this picture is left by an event (a decoder's new Instance lost or scaled away, a budget lowered under a batch already open, a push), not by the staging",
	"one-sided-version-with-traffic":             "a push before the cutover names version 1 stable: each member sends its whole traffic to its one version 1 pod, and a member with no version 1 pod left sends it to the revision serving most of its pods, so a version one side alone still runs carries weight (the open decision on the stable identity of a run opened over an unfinished canary); a stable pod lost on one member makes no such picture, since the unit has one stable side and both members move together",
	"traffic-on-a-thin-version":                  "a version carries the weight of the step it last served while its Instances on one side are rebuilt or retired, so the ones left carry more than twice their share",
	"ratio-drift-5":                              "a version's engine:decoder ratio drifts more than 5 percent from the fleet's while Instances build: inside a batch the decoder, the named side, moves first and the engines follow once it serves, so version 2 is one decoder ahead and version 1 one decoder behind until the batch closes; the batches themselves are planned to the tolerance in the share sense (the gap of the two sides' shares), which the ratio sense reads as a drift on the small counts",
	"ratio-drift-10":                             "the same at 10 percent",
	"ratio-drift-20":                             "the same at 20 percent",
	"ratio-drift-5-at-rest":                      "a version's engine:decoder ratio drifts more than 5 percent from the fleet's with nothing building: the step's own rounding per side (12 prefill and 4 decode at 10 percent stage 2:1, the first picture a plain roll of that shape takes too), or a side the plan has moved one batch ahead between two steps",
	"ratio-drift-10-at-rest":                     "the same at 10 percent",
	"ratio-drift-20-at-rest":                     "the same at 20 percent",
	"share-skew-5":                               "the two sides' shares of a version sit more than 5 percent apart while Instances build (the paired batch plan's reading of a tolerance): the decoder's Instance of a batch serves before the engines' start, so the picture inside a batch is one decoder, a quarter of a side of 4, ahead",
	"share-skew-10":                              "the same at 10 percent",
	"share-skew-20":                              "the same at 20 percent",
	"share-skew-5-at-rest":                       "the two sides' shares of a version sit more than 5 percent apart with nothing building: the step's rounding (12:4 at 10 percent: 17 and 25 percent, the closest batch to the line the planner can take), or an event (a pod lost, a scale, a push) that moved one side",
	"share-skew-10-at-rest":                      "the same at 10 percent",
	"share-skew-20-at-rest":                      "the same at 20 percent",
}

// pairFailingClasses are the readings that fail the test: a quiet state
// with no way out, traffic on a revision that has no pod while the unit is
// live or on an empty stable revision while it is held, a step left before
// both sides served it without a force, a held
// share short of the step's, a run closed twice or reopened after it
// completed, a play that reaches no end, a controller that does not settle,
// and an error inside the production code. A class in neither list is one
// nobody tracks yet and fails too.
var pairFailingClasses = map[string]bool{
	"no-way-out": true, "traffic-empty-revision": true, "traffic-empty-stable-revision-under-a-hold": true, "promoted-short": true,
	"held-short": true, "share-exceeded": true, "run-closed-twice": true, "reopen-after-complete": true,
	"unsettled": true, "depth": true, "model-error": true,
}

// pairReportedClasses are the readings that end a run or name a hold with a
// way out; they are counted and not failed.
func pairReportedClass(class string) bool {
	return strings.HasPrefix(class, "end-") || strings.HasPrefix(class, "hold-")
}

// pairExplore explores every configuration keep admits, probes every event
// at every moment, and reports each reading with the shortest path to it.
func pairExplore(t *testing.T, keep func(pairConfig) bool) {
	t.Helper()
	full := pairFullMode()
	maxStates, maxDepth, runs := 24, 60, 1
	if full {
		maxStates, maxDepth, runs = 60, 100, 3
	}
	var configs []pairConfig
	for _, cfg := range pairConfigs(full) {
		if keep(cfg) {
			configs = append(configs, cfg)
		}
	}
	type result struct {
		rep              pairReport
		passes, dispatch int
		failure          string
	}
	results := make([]result, len(configs))
	scheme := canaryScheme(t)
	workers := make(chan struct{}, pairWorkers())
	var wg sync.WaitGroup
	started := time.Now()
	for n := range configs {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			workers <- struct{}{}
			defer func() { <-workers }()
			w := newPairWorld(t, configs[n], scheme)
			res := result{rep: newPairReport()}
			func() {
				defer func() {
					if r := recover(); r != nil {
						res.failure = fmt.Sprint(r)
					}
				}()
				w.probe(&res.rep, 4*maxDepth)
				w.explore(&res.rep, maxStates, maxDepth)
				w.randomRuns(&res.rep, 1, runs, 4*maxDepth)
			}()
			res.passes, res.dispatch = w.passes, w.dispatches
			results[n] = res
		}(n)
	}
	wg.Wait()
	totals := map[string]int{}
	configsWith := map[string]int{}
	type example struct {
		cfg     pairConfig
		finding pairFinding
	}
	examples := map[string][]example{}
	var states, passes, dispatches, allRuns, probes int
	for n, cfg := range configs {
		res := results[n]
		rep := res.rep
		if res.failure != "" {
			rep.counts["model-error"]++
			rep.findings["model-error"] = pairFinding{class: "model-error", message: res.failure, state: &pairState{isvc: pairISVC(cfg)}}
		}
		states += rep.states
		passes += res.passes
		dispatches += res.dispatch
		allRuns += rep.runs
		probes += rep.probes
		var parts []string
		for class, c := range rep.counts {
			part := fmt.Sprintf("%s=%d", class, c)
			if f := rep.findings[class]; f.severity > 0 {
				part += fmt.Sprintf("(worst %.2g)", f.severity)
			}
			parts = append(parts, part)
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: %d states, %d probes, depth %d, %d passes: %s", n+1, len(configs), cfg.name, rep.states, rep.probes, rep.depth, res.passes, strings.Join(parts, " "))
		for class, c := range rep.counts {
			totals[class] += c
		}
		for class, f := range rep.findings {
			configsWith[class]++
			examples[class] = append(examples[class], example{cfg: cfg, finding: f})
		}
	}
	// The examples of a class are the worst pictures across the
	// configurations, six of them.
	for class := range examples {
		list := examples[class]
		sort.SliceStable(list, func(i, j int) bool { return list[i].finding.severity > list[j].finding.severity })
		if len(list) > 6 {
			list = list[:6]
		}
		examples[class] = list
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d probes, %d runs, %d controller passes, %d canary dispatches, %s", len(configs), states, probes, allRuns, passes, dispatches, time.Since(started).Round(time.Second))
	for _, class := range classes {
		if known, ok := pairKnownClasses[class]; ok && known != "" {
			t.Logf("%s: %d states across %d configurations: %s", class, totals[class], configsWith[class], known)
		} else {
			t.Logf("%s: %d states across %d configurations", class, totals[class], configsWith[class])
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg.name, ex.finding.message, pairPath(ex.finding.state), ex.finding.state)
		}
		_, known := pairKnownClasses[class]
		if pairFailingClasses[class] || (!known && !pairReportedClass(class)) {
			t.Errorf("%s: %d states (worst: %s: %s)", class, totals[class], examples[class][0].cfg.name, examples[class][0].finding.message)
		}
	}
}

// playTo plays the straight run until the unit first stands at the named
// moment (momentOf) and returns that state.
func (w *pairWorld) playTo(moment string) *pairState {
	st := w.initial()
	w.settle(st)
	for depth := 0; depth < 400; depth++ {
		if w.momentOf(st) == moment {
			return st
		}
		mv, ok := w.nextStraightMove(st, depth)
		if !ok {
			break
		}
		st = w.apply(st, mv)
	}
	w.t.Fatalf("the straight run never reached the moment %q: %s", moment, w.describe(st))
	return st
}

// serveSide brings one side's building Instances up, lowest index first,
// letting the controller settle after each, until the side has nothing in
// flight; the other side does not move.
func (w *pairWorld) serveSide(st *pairState, member int) *pairState {
	for round := 0; round < 128; round++ {
		moved := false
		for idx, r := range st.members[member].rows {
			if r.kind != pairRowServing {
				st = w.apply(st, pairMove{kind: "serve", member: member, idx: idx})
				moved = true
				break
			}
		}
		if !moved {
			return st
		}
	}
	w.t.Fatalf("side %d did not come up in 128 rounds: %s", member, st)
	return st
}

// playUntil plays the straight run from a state, the sides coming up in
// turns, until the state satisfies until, and returns that state.
func (w *pairWorld) playUntil(st *pairState, until func(*pairState) bool) *pairState {
	for depth := 0; depth < 400; depth++ {
		if until(st) {
			return st
		}
		mv, ok := w.nextStraightMove(st, depth)
		if !ok {
			break
		}
		st = w.apply(st, mv)
	}
	w.t.Fatalf("the straight run never reached the picture asked for: %s", w.describe(st))
	return st
}

// serveInTurns brings the two sides' building Instances up in turns, the
// decoder's first, leaving a lost pod lost, until the state satisfies until,
// and returns that state.
func (w *pairWorld) serveInTurns(st *pairState, until func(*pairState) bool) *pairState {
	for round := 0; round < 400; round++ {
		if until(st) {
			return st
		}
		moved := false
		for _, member := range []int{1, 0} {
			for idx, r := range st.members[member].rows {
				if r.inFlight() {
					st = w.apply(st, pairMove{kind: "serve", member: member, idx: idx})
					moved = true
					break
				}
			}
			if moved {
				break
			}
		}
		if !moved {
			break
		}
	}
	w.t.Fatalf("serving the sides in turns never reached the picture asked for: %s", st)
	return st
}

// TestPrefillDecodeCanary_TheFinalStepKeepsThePreviousWeightOnAThinningStableVersion
// records the current behaviour on the reference shape. 12 prefill and 8 decode
// under RecreatePod at 10 percent run a 10/30/60/100 canary. At the final
// step the version 1 Instances are rebuilt in batches of two engines beside
// one decoder, the decoder first, and the split stays at the step before (60
// percent to version 2, 40 to version 1) until both sides reach the final
// step's capacity. With the decoder side rebuilt down to its one kept
// Instance and the engine side on its last batch, version 1 is that decoder
// and at most three engines carrying 40 percent of the traffic: the one
// decoder carries 3.2 times its even share. Whether the weight should
// follow the capacity is left open; this test records the current
// behaviour.
func TestPrefillDecodeCanary_TheFinalStepKeepsThePreviousWeightOnAThinningStableVersion(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{12, 8}, v1beta1.UpdateStrategyRecreatePod, "four-manual", 0, false), canaryScheme(t))
	st := w.playTo("staging-3")
	st = w.playUntil(st, func(st *pairState) bool { return st.members[1].servingOn("r1") == 1 })
	engine, decoder := st.members[0], st.members[1]
	e, d := engine.servingOn("r1"), decoder.servingOn("r1")
	if d != 1 || e < 1 || e > 3 {
		t.Fatalf("the picture is wrong: version 1 must stand at its last decoder beside at most one engine batch, got %d:%d (%s)", e, d, st)
	}
	if w := w.trafficOn(st, decoder, "r1"); w != 40 {
		t.Fatalf("the decoder split sends %d percent to version 1; the current behaviour keeps the previous step's 40; update this test when it changes", w)
	}
	if w := w.trafficOn(st, engine, "r1"); w != 40 {
		t.Fatalf("the engine split sends %d percent to version 1; the current behaviour keeps the previous step's 40", w)
	}
	t.Logf("version 1 at %d engines and 1 decoder carries 40 percent of the traffic: the decoder at %.1f times its even share, the engines at %.1f", e, 40*8.0/(100*1), 40*12.0/(100*float64(e)))
}

// TestPrefillDecodeCanary_TheFinalStepKeepsThePreviousWeightOnTheLastStableDecoderUnderSurge
// records the current behaviour under SurgeThenDrain. 20 prefill and 10 decode at
// 10 percent run a 25/50/100 canary with timed gates. At the final step each
// old pod leaves as its replacement serves, in batches of two engines beside
// one decoder, the decoder first. With the decoder side down to its held
// Instance, version 1 is 1 decoder beside at most three engines (the held
// one and the last batch, whose replacements may still be building),
// carrying the 50 percent of the step before: the one decoder answers at 5
// times its even share. Whether the weight should follow the capacity is not
// decided yet; this test records the current behaviour.
func TestPrefillDecodeCanary_TheFinalStepKeepsThePreviousWeightOnTheLastStableDecoderUnderSurge(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{20, 10}, v1beta1.UpdateStrategySurgeThenDrain, "three-timed", 600, false), canaryScheme(t))
	st := w.playTo("staging-2")
	st = w.playUntil(st, func(st *pairState) bool { return st.members[1].servingOn("r1") == 1 })
	engine, decoder := st.members[0], st.members[1]
	e, d := engine.servingOn("r1"), decoder.servingOn("r1")
	if d != 1 || e < 1 || e > 3 {
		t.Fatalf("the picture is wrong: version 1 must stand at its last decoder beside at most one engine batch, got %d:%d (%s)", e, d, st)
	}
	if w := w.trafficOn(st, decoder, "r1"); w != 50 {
		t.Fatalf("the decoder split sends %d percent to version 1; the current behaviour keeps the previous step's 50; update this test when it changes", w)
	}
	t.Logf("version 1 at %d engines and 1 decoder carries 50 percent of the traffic: the decoder at %.1f times its even share", e, 50*10.0/(100*1))
}

// TestPrefillDecodeCanary_ALostStableEnginePodMovesTheUnitsSplitTogether
// reads the two splits when one member's stable side loses its last pod on
// the reference shape. 12 prefill and 8 decode under RecreatePod at 10
// percent run a 10/30/60/100 canary; the final step is staging and the step
// before left 60 percent on version 2 and 40 on version 1. The engine's
// held version 1 pod is lost, and the two sides go on rebuilding in turns
// until the engine's last batch is being rebuilt, so no engine serves
// version 1 while the decoder's kept Instance still does. The unit has one
// stable side: with no engine behind it, the 40 percent moves onto version
// 2 on both members, so no request is split between a version 2 engine and
// a version 1 decoder. A rule that reads the stable side per member sends
// the engine 100 and leaves the decoder's 40 on a version no engine runs.
func TestPrefillDecodeCanary_ALostStableEnginePodMovesTheUnitsSplitTogether(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{12, 8}, v1beta1.UpdateStrategyRecreatePod, "four-manual", 0, false), canaryScheme(t))
	st := w.playTo("staging-3")
	st = w.apply(st, pairMove{kind: "podloss", member: 0, idx: 0})
	st = w.serveInTurns(st, func(st *pairState) bool {
		engine := st.members[0]
		return engine.servingOn("r1") == 0 && engine.inFlightCount() > 0
	})
	engine, decoder := st.members[0], st.members[1]
	if e, d := engine.servingOn("r1"), decoder.servingOn("r1"); e != 0 || d == 0 {
		t.Fatalf("the picture is wrong: no engine and some decoder must serve version 1, got %d:%d (%s)", e, d, st)
	}
	if w := w.trafficOn(st, engine, "r2"); w != 100 {
		t.Fatalf("the engine split sends %d percent to version 2; with no engine on version 1 it must send 100", w)
	}
	if r1, r2 := w.trafficOn(st, decoder, "r1"), w.trafficOn(st, decoder, "r2"); r1 != 0 || r2 != 100 {
		t.Fatalf("the decoder split sends %d percent to version 1 and %d to version 2; the unit has one stable side, so with no engine on version 1 the decoder's 40 moves to version 2 with the engine's", r1, r2)
	}
	t.Logf("engine split: version 2 at 100; decoder split: version 2 at 100 while one decoder still serves version 1 (%s)", st)
}

// TestPrefillDecodeCanary_RunsReachAnEnd explores a canary over the engine
// and the decoder on the reference shapes under both strategies, every
// step list and both drain windows, with every event tried at every moment.
func TestPrefillDecodeCanary_RunsReachAnEnd(t *testing.T) {
	pairExplore(t, func(pairConfig) bool { return true })
}

// TestPrefillDecodeCanary_EveryConfigurationCompletesWhenNothingGoesWrong
// plays the straight run of every configuration and expects a Completed
// end: both sides bring each staged Instance up in turn, the clock reaches
// each deadline, and the operator promotes each manual gate.
func TestPrefillDecodeCanary_EveryConfigurationCompletesWhenNothingGoesWrong(t *testing.T) {
	scheme := canaryScheme(t)
	for _, cfg := range pairConfigs(pairFullMode()) {
		w := newPairWorld(t, cfg, scheme)
		rep := newPairReport()
		end := w.straightRun(&rep, 400)
		outcome := v1beta1.RolloutRunOutcome("")
		if r := end.isvc.Status.Rollout; r != nil && r.LastRun != nil {
			outcome = r.LastRun.Outcome
		}
		if !w.finished(end) || outcome != v1beta1.RolloutRunCompleted {
			t.Errorf("[%s] the straight run did not end Completed after %d moves: %s\n  path: %s", cfg.name, len(end.path), w.describe(end), pairPath(end))
			continue
		}
		t.Logf("[%s] Completed in %d moves, %d passes", cfg.name, len(end.path), w.passes)
	}
}

// TestPrefillDecodeCanary_ARollbackAtTheFinalStepSendsAllTrafficToTheKeptInstances
// records the current behaviour of a late rollback. 30 prefill and 10 decode under
// SurgeThenDrain at 10 percent run a 10/30/60/100 canary with a ten-minute
// drain window; the final step has shifted 100 percent to version 2 and
// holds one Instance per side on version 1. The operator rolls back. The
// rollback writes 100 percent onto version 1 on the pass it starts, since
// version 1 has a serving pod, while the 29 engines and 9 decoders on
// version 2 are rebuilt three and one at a time: the one version 1 engine
// carries 30 times its even share and the one decoder 10 times until the
// revert brings the others back. Whether the rollback should move the weight
// with the reverted capacity is left open; this test records the
// current behaviour.
func TestPrefillDecodeCanary_ARollbackAtTheFinalStepSendsAllTrafficToTheKeptInstances(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{30, 10}, v1beta1.UpdateStrategySurgeThenDrain, "four-timed", 600, false), canaryScheme(t))
	st := w.playTo("draining")
	st = w.apply(st, pairMove{kind: "rollback"})
	engine, decoder := st.members[0], st.members[1]
	if e, d := engine.servingOn("r1"), decoder.servingOn("r1"); e != 1 || d != 1 {
		t.Fatalf("the picture is wrong: version 1 must stand at the one kept Instance per side, got %d:%d (%s)", e, d, st)
	}
	if u := w.unit(st); u == nil || u.state != rollout.CanaryStateRollingBack {
		t.Fatalf("the picture is wrong: the unit must be rolling back, got %s", w.describe(st))
	}
	if e, d := w.trafficOn(st, engine, "r1"), w.trafficOn(st, decoder, "r1"); e != 100 || d != 100 {
		t.Fatalf("the splits send %d and %d percent to version 1; the current behaviour moves all of it there on the pass the rollback starts; update this test when it changes", e, d)
	}
	t.Logf("version 1 at 1 engine and 1 decoder carries 100 percent of the traffic: %.0f and %.0f times the even share while %d engines and %d decoders are reverted", 100*30.0/100, 100*10.0/100, engine.servingOn("r2"), decoder.servingOn("r2"))
}

// TestPrefillDecodeCanary_ASecondPushBeforeTheCutoverNamesVersionOneStable
// records the current behaviour of a push landing before the first canary
// completes. 15 prefill and 15 decode under SurgeThenDrain at 10 percent run
// a 10/30/60/100 timed canary with a ten-minute drain window; the final step
// has shifted 100 percent to version 2 and its window is open, one Instance
// per side still held on version 1. The operator pushes version 3. The run
// layer closes the first run and opens a second whose stable revision is
// each replica's current revision, which is still version 1 because the
// first canary never completed, so the new canary's split rests on version
// 1: each member sends 100 percent of its traffic to its one version 1 pod
// and nothing to its 14 version 2 Instances until a version 3 pod serves.
// Whether the stable identity should follow the Instances that serve is not
// decided yet; this test records the current behaviour.
func TestPrefillDecodeCanary_ASecondPushBeforeTheCutoverNamesVersionOneStable(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{15, 15}, v1beta1.UpdateStrategySurgeThenDrain, "four-timed", 600, false), canaryScheme(t))
	st := w.playTo("draining")
	st = w.apply(st, pairMove{kind: "push"})
	u := w.unit(st)
	if u == nil || u.record == nil || u.record.CanaryRevisionHash != "r3" || u.record.StableRevisionHash != "r1" {
		t.Fatalf("the picture is wrong: the new canary must name version 3 against a stable version 1, got %s", w.describe(st))
	}
	for _, m := range st.members {
		if on1, on2 := m.servingOn("r1"), m.servingOn("r2"); on1 != 1 || on2 != 14 {
			t.Fatalf("the picture is wrong: %s must serve one Instance on version 1 and fourteen on version 2, got %d and %d (%s)", m.comp, on1, on2, st)
		}
		if weight := w.trafficOn(st, m, "r1"); weight != 100 {
			t.Fatalf("the %s split sends %d percent to version 1; the current behaviour sends it all to the one version 1 pod; update this test when it changes", m.comp, weight)
		}
	}
	t.Logf("after the push both splits send 100 percent to version 1, one pod per side, and nothing to version 2 (%s)", st)
}

// TestPrefillDecodeCanary_APauseLongerThanTheReadyTimeoutStopsTheClockAndTheResumeContinuesAtTheStoppedStep
// drives the reference shape through a long pause. 24 prefill and 8 decode
// under RecreatePod at 10 percent run a 10/30/60/100 canary; the final step
// is staging with 15 engines and 5 decoders on version 2 and three engines
// mid-rebuild: the batch step plans the step over the Instances it may
// move (23 and 7), the decoder's share is ahead of the engine's, so the
// engines catch up first and the decoder's next Instance waits for them.
// The operator pauses the rollout: the rebuilds in flight finish (18 and 5
// on version 2) and nothing new starts. The operator comes back after the
// ready timeout. The pause stopped the capacity wait's clock, so the pass
// after the unpause keeps the unit staging the final step and clocks the
// wait again from that pass: no time spent paused counts toward the ready
// timeout, and the pods have the whole timeout from the unpause. That pass
// projects the step's partition, so one more start opens, the decoder's
// next Instance, with the engines' next batch waiting on it. A whole ready
// timeout after the unpause, with six engines still to come up, the unit
// stops Failed with CapacityTimeout; from then on the stop holds the roll
// where it stands, with each member's partition its whole count: 18 engines
// and 6 decoders on version 2, 6 and 2 on version 1, and the split still 60
// to version 2 and 40 to version 1. The operator resumes: the unit continues
// at the final step, the step the stop interrupted, so no split sends the
// first step's 90 percent back to the six engines and two decoders the stop
// kept on version 1; the split stands at 60/40 while the rest of the fleet
// is rebuilt, and once the final step's capacity is up its 100 percent lands
// and the cutover follows.
func TestPrefillDecodeCanary_APauseLongerThanTheReadyTimeoutStopsTheClockAndTheResumeContinuesAtTheStoppedStep(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{24, 8}, v1beta1.UpdateStrategyRecreatePod, "four-manual", 0, false), canaryScheme(t))
	st := w.playTo("staging-3")
	st = w.apply(st, pairMove{kind: "pause"})
	st = w.serveSide(st, 0)
	st = w.serveSide(st, 1)
	expectPairCounts(t, w, st, 18, 6, 5, 3, "under the pause, with the rebuilds in flight finished")
	st = w.apply(st, pairMove{kind: "clock-all"})
	st = w.apply(st, pairMove{kind: "unpause"})
	u := w.unit(st)
	if u == nil || u.record == nil || u.state != rollout.CanaryStateStaging || u.record.CurrentStep != 3 || u.record.Failed != nil {
		t.Fatalf("the unit must still stage the final step after a pause longer than the ready timeout; got %s", w.describe(st))
	}
	if since := u.record.CapacityWaitSince; since == nil || !since.Time.Equal(st.now) {
		t.Fatalf("the capacity wait must be clocked again from the first pass after the pause, got %v at %s", since, st.now)
	}
	// A whole ready timeout after the unpause, with the engines still short,
	// the unit stops: the pods had the whole timeout from the unpause.
	st = w.apply(st, pairMove{kind: "clock-all"})
	u = w.unit(st)
	if u == nil || u.state != rollout.CanaryStateFailed || u.record.Failed == nil || u.record.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout {
		t.Fatalf("the unit must read Failed with CapacityTimeout a whole ready timeout after the pause ended; got %s", w.describe(st))
	}
	stopped := u.record.CurrentStep
	if stopped != 3 {
		t.Fatalf("the picture is wrong: the stop must interrupt the final step, got step %d (%s)", stopped, w.describe(st))
	}
	// The stop holds the roll where it stands: the start the unpause pass
	// had already released finishes, and nothing moves after it.
	st = w.serveSide(st, 0)
	st = w.serveSide(st, 1)
	expectPairCounts(t, w, st, 18, 6, 6, 2, "after the stop")
	expectPairPartitions(t, st, 24, 8, "after the stop")
	expectPairSplit(t, w, st, 40, 60, "after the stop")
	st = w.apply(st, pairMove{kind: "resume"})
	u = w.unit(st)
	if u == nil || u.record == nil || u.record.CurrentStep != stopped {
		t.Fatalf("the resume must continue at step %d, the step the stop interrupted, got %s", stopped, w.describe(st))
	}
	expectPairSplit(t, w, st, 40, 60, "after the resume")
	// The rest of the fleet comes up in the batch step's turns, the
	// decoder's Instances of each batch first; the final step then shifts
	// everything to version 2 and the cutover completes.
	st = w.serveInTurns(st, func(st *pairState) bool {
		return st.members[0].servingOn("r2") == 24 && st.members[1].servingOn("r2") == 8
	})
	if !w.finished(st) {
		t.Fatalf("the resumed canary must reach its end once the final step's capacity is up, got %s", w.describe(st))
	}
	expectPairSplit(t, w, st, 0, 100, "after the cutover")
	t.Logf("after the resume the unit continued at step %d and completed (%s)", stopped, w.describe(st))
}

// TestPrefillDecodeCanary_AStopAtTheFinalStepHoldsTheRollWhereItStands drives
// the reference shape through a final step whose capacity never comes up.
// 24 prefill and 8 decode under RecreatePod at 10 percent run a 10/30/60/100
// canary; the final step is staging with 15 engines and 5 decoders on
// version 2, three engines mid-rebuild (the batch step plans the step over
// the 23 and 7 Instances it may move, the decoder's share is ahead, so the
// engines catch up first and the decoder's next Instance waits for them),
// and the rest on version 1 under the split of the step before (60 to
// version 2, 40 to version 1). The ready timeout passes and the unit stops
// Failed with CapacityTimeout. The stop holds the roll where it stands: the
// rebuilds in flight finish and no further Instance moves (18 engines and 5
// decoders on version 2, 6 and 3 on version 1), each member's partition is
// its whole count, and the split stays at 60/40 on the pods that serve. The
// operator's rollback ends the hold and returns every Instance to version 1.
func TestPrefillDecodeCanary_AStopAtTheFinalStepHoldsTheRollWhereItStands(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{24, 8}, v1beta1.UpdateStrategyRecreatePod, "four-manual", 0, false), canaryScheme(t))
	st := w.playTo("staging-3")
	expectPairCounts(t, w, st, 15, 6, 5, 3, "at the final step's staging")
	st = w.apply(st, pairMove{kind: "clock-all"})
	u := w.unit(st)
	if u == nil || u.state != rollout.CanaryStateFailed || u.record.Failed == nil || u.record.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout {
		t.Fatalf("the unit must read Failed with CapacityTimeout once the final step's wait outlives the ready timeout; got %s", w.describe(st))
	}
	st = w.serveSide(st, 0)
	st = w.serveSide(st, 1)
	expectPairCounts(t, w, st, 18, 6, 5, 3, "after the stop")
	expectPairPartitions(t, st, 24, 8, "after the stop")
	expectPairSplit(t, w, st, 40, 60, "after the stop")
	if u := w.unit(st); u == nil || u.state != rollout.CanaryStateFailed {
		t.Fatalf("the hold must stand until the operator acts, got %s", w.describe(st))
	}
	st = w.apply(st, pairMove{kind: "rollback"})
	// The revert comes back in the batch step's turns, the decoder's
	// Instances of each batch first.
	st = w.serveInTurns(st, func(st *pairState) bool {
		return st.members[0].servingOn("r1") == 24 && st.members[1].servingOn("r1") == 8
	})
	if u := w.unit(st); u == nil || u.state != rollout.CanaryStateRolledBack {
		t.Fatalf("the rollback must end the hold and settle, got %s", w.describe(st))
	}
	expectPairCounts(t, w, st, 0, 24, 0, 8, "after the rollback")
	expectPairPartitions(t, st, 0, 0, "after the rollback")
	expectPairSplit(t, w, st, 100, 0, "after the rollback")
}

// expectPairCounts reads each side's serving Instances per version.
func expectPairCounts(t *testing.T, w *pairWorld, st *pairState, engineNew, engineOld, decoderNew, decoderOld int32, when string) {
	t.Helper()
	engine, decoder := st.members[0], st.members[1]
	if on1, on2 := engine.servingOn("r1"), engine.servingOn("r2"); on1 != engineOld || on2 != engineNew {
		t.Fatalf("%s the engine must serve %d on version 1 and %d on version 2, got %d and %d (%s)", when, engineOld, engineNew, on1, on2, st)
	}
	if on1, on2 := decoder.servingOn("r1"), decoder.servingOn("r2"); on1 != decoderOld || on2 != decoderNew {
		t.Fatalf("%s the decoder must serve %d on version 1 and %d on version 2, got %d and %d (%s)", when, decoderOld, decoderNew, on1, on2, st)
	}
	_ = w
}

// expectPairPartitions reads the partition last projected on each side.
func expectPairPartitions(t *testing.T, st *pairState, engine, decoder int32, when string) {
	t.Helper()
	for i, want := range []int32{engine, decoder} {
		m := st.members[i]
		if m.partition == nil || *m.partition != want {
			t.Fatalf("%s %s must be projected partition %d, got %s (%s)", when, m.comp, want, fmtPartition(m.partition), st)
		}
	}
}

// expectPairSplit reads both members' splits on version 1 and version 2.
func expectPairSplit(t *testing.T, w *pairWorld, st *pairState, onOld, onNew int32, when string) {
	t.Helper()
	for _, m := range st.members {
		if w1, w2 := w.trafficOn(st, m, "r1"), w.trafficOn(st, m, "r2"); w1 != onOld || w2 != onNew {
			t.Fatalf("%s the %s split must send %d percent to version 1 and %d to version 2, got %d and %d (%s)", when, m.comp, onOld, onNew, w1, w2, w.describe(st))
		}
	}
}

// onVersion counts a side's Instances still standing on a revision: serving
// it, or taken down from it by an attempt that has not come back yet.
func (m pairMemberState) onVersion(hash string) int32 {
	var n int32
	for _, r := range m.rows {
		if r.rev == hash {
			n++
		}
	}
	return n
}

// inFlightCount counts a side's Instances with an attempt open.
func (m pairMemberState) inFlightCount() int32 {
	var n int32
	for _, r := range m.rows {
		if r.inFlight() {
			n++
		}
	}
	return n
}

// TestPrefillDecodeCanary_AStepStartsTheDecoderBeforeTheEngines holds a
// step's staging to the batch step. 12 prefill and 4 decode under
// RecreatePod at 10 percent run a 10/30/60/100 canary; the first step
// stages 2 engines and 1 decoder. The decoder's Instance moves first and the
// two engines start only once it serves, so version 2 never stands on
// engines alone. At rest the step reads 2:1 against the fleet's 3:1, the
// step's own rounding on each side (10 percent of 12 and of 4, rounded up),
// the same first picture a plain roll of this shape takes. Without the
// batch step both sides start in the same pass.
func TestPrefillDecodeCanary_AStepStartsTheDecoderBeforeTheEngines(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{12, 4}, v1beta1.UpdateStrategyRecreatePod, "four-manual", 0, false), canaryScheme(t))
	st := w.initial()
	w.settle(st)
	if e, d := st.members[0].inFlightCount(), st.members[1].inFlightCount(); e != 0 || d != 1 {
		t.Fatalf("after the push the decoder's Instance must move alone and the engines wait for it; got %d engines and %d decoders moving (%s)", e, d, st)
	}
	st = w.serveSide(st, 1)
	if e := st.members[0].inFlightCount(); e != 2 {
		t.Fatalf("once the decoder's Instance serves the two engines of the step must start; got %d moving (%s)", e, st)
	}
	st = w.serveSide(st, 0)
	engine, decoder := st.members[0], st.members[1]
	if e, d := engine.servingOn("r2"), decoder.servingOn("r2"); e != 2 || d != 1 {
		t.Fatalf("at rest the step must stand at 2 engines and 1 decoder on version 2, got %d:%d (%s)", e, d, st)
	}
	t.Logf("at rest version 2 stands at 2 engines beside 1 decoder, %.0f percent off the fleet's 3:1: the step's rounding on each side, the first picture a plain roll of this shape takes too", ratioDriftPercent(2, 1, 12, 4))
}

// TestPrefillDecodeCanary_ARevertMovesBothSidesTogether holds a late
// rollback to the batch step. 30 prefill and 10 decode under SurgeThenDrain
// at 10 percent run a 10/30/60/100 timed canary with a ten-minute drain
// window; the final step has shifted 100 percent to version 2 and keeps one
// Instance per side on version 1. The operator rolls back and the two sides
// come back one Instance at a time in turns. The revert is planned in
// batches of 3 engines beside 1 decoder, the decoder first, so at no moment
// is version 2 more than one engine batch off the fleet's 3:1: with e
// engines and d decoders still on version 2, |e - 3d| stays at 3 or below.
// With each side reverting at its own pace the same play reads version 2 at
// 21 engines beside 1 decoder.
func TestPrefillDecodeCanary_ARevertMovesBothSidesTogether(t *testing.T) {
	w := newPairWorld(t, pairConfigFor([2]int32{30, 10}, v1beta1.UpdateStrategySurgeThenDrain, "four-timed", 600, false), canaryScheme(t))
	st := w.playTo("draining")
	st = w.apply(st, pairMove{kind: "rollback"})
	var worst int32
	worstPicture := ""
	for depth := 0; !w.finished(st); depth++ {
		if depth > 400 {
			t.Fatalf("the revert did not end in 400 moves: %s", w.describe(st))
		}
		e, d := st.members[0].onVersion("r2"), st.members[1].onVersion("r2")
		if off := e - 3*d; off > worst || -off > worst {
			if off < 0 {
				off = -off
			}
			worst, worstPicture = off, fmt.Sprintf("%d engines beside %d decoders still on version 2 (path: %s)", e, d, pairPath(st))
		}
		mv, ok := w.nextStraightMove(st, depth)
		if !ok {
			t.Fatalf("the revert cannot move on: %s", w.describe(st))
		}
		st = w.apply(st, mv)
	}
	if worst > 3 {
		t.Fatalf("the revert left one side far ahead of the other: at worst %s, %d engines off the fleet's 3:1 where one engine batch is 3", worstPicture, worst)
	}
	t.Logf("through the revert version 2 stayed within one engine batch of 3:1 (at worst %s)", worstPicture)
}
