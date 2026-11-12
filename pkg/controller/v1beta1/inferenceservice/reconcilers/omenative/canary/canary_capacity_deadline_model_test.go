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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils"
)

// This file drives a prefill/decode canary, with routers beside it, on a
// clock. The production code of one controller pass runs in the
// controller's order: the run layer (rolloutrun.Reconcile and BindRun), the
// partition the projector writes for each member (PartitionResolver), the
// coordination update gate asked before each start
// (coordination.EvaluateUpdateGate under the chart's prefill/decode
// settings, which moves the decoder's Instances of a batch first and the
// engines once they serve), and the canary dispatch (Dispatch) with its
// capacity gate and the step's ready timeout. The workload stand-in starts
// the admitted Instances toward the target under each member's strategy and
// budget. A start takes time to come up: a pod's start, the members of a
// gang coming up one after another, the gang scheduler's placement wait, a
// slow node under one Instance of each side; and the pass that reads an
// Instance Ready runs a little after it. The world moves the clock to the
// next moment the controller acts on, an Instance coming up or a deadline
// the record waits for. Between those moments the operator or the world
// may act: a pod is lost, a side's starts toward the new revision can get
// no capacity (an image that cannot be pulled) and get it back, the
// operator pauses, resumes, rolls back, promotes, or pushes a corrected
// revision at a stopped step.
//
// At every settled state the model reads: a step stopped for capacity
// (CapacityTimeout) with no fault in the world while a start was coming up,
// the deadline run against starts the pacing itself puts one after
// another; a step stopped for capacity with nothing coming up; a side that
// can get no capacity whose step is not stopped one ready timeout after its
// last progress; a quiet state no verb moves; a member with more starts in
// flight than its budget; and the new version on one side only past one
// batch of that side. OME_CANARY_DEADLINE_HUNT=full widens the
// configurations and the bounds; OME_CANARY_DEADLINE_HUNT_ONLY keeps the
// configurations whose name contains one of its comma-separated parts.

const (
	deadlineNamespace  = "ns"
	deadlineService    = "svc"
	deadlineHuntEnv    = "OME_CANARY_DEADLINE_HUNT"
	deadlineOnlyEnv    = "OME_CANARY_DEADLINE_HUNT_ONLY"
	deadlineWorkersEnv = "OME_CANARY_DEADLINE_HUNT_WORKERS"
)

// deadlineRevisions are the revision hashes the model knows: the one the
// fleet stands on, the one the operator pushes, and the corrected push.
var deadlineRevisions = []string{"r1", "r2", "r3"}

func deadlineRevisionName(comp v1beta1.ComponentType, hash string) string {
	return deadlineService + "-" + string(comp) + "-" + hash
}

// deadlineRowKind is what the roll has done to one Instance.
type deadlineRowKind uint8

const (
	deadlineRowServing    deadlineRowKind = iota // every pod Ready on rev
	deadlineRowLost                              // the pods are gone; the workload rebuilds them on rev
	deadlineRowRecreating                        // the old pods are gone; new pods build toward target
	deadlineRowSurging                           // the old pods serve while a replacement builds toward target
	deadlineRowCreating                          // a new Instance builds toward target
)

var deadlineRowKindNames = [...]string{"", "lost", "recreate", "surge", "create"}

// deadlineRow is one Instance with the clock's view of it: when the attempt
// in flight opened, when the pods it or a rebuild created were created,
// when they come up (zero while the start can get no capacity), and when
// the Instance last came up (the row's readySince).
type deadlineRow struct {
	kind       deadlineRowKind
	rev        string
	target     string
	openedAt   time.Time
	createdAt  time.Time
	readyAt    time.Time
	readySince time.Time
	// rebuilt marks an attempt that rebuilds a lost Instance at the roll's
	// target: the world's doing, not a start the roll's budget admitted.
	rebuilt bool
}

func (r deadlineRow) inFlight() bool {
	return r.kind == deadlineRowRecreating || r.kind == deadlineRowSurging || r.kind == deadlineRowCreating
}

// coming reports an Instance whose pods will come up at readyAt.
func (r deadlineRow) coming() bool { return r.kind != deadlineRowServing && !r.readyAt.IsZero() }

// toward is the revision the pods in flight build toward: the attempt's
// target, or a lost Instance's own revision, which its rebuild restores.
func (r deadlineRow) toward() string {
	if r.kind == deadlineRowLost {
		return r.rev
	}
	return r.target
}

// serving reports pods of the Instance in rotation: a serving row, or the
// old pods of a surging one.
func (r deadlineRow) serving() bool {
	return r.kind == deadlineRowServing || r.kind == deadlineRowSurging
}

func (r deadlineRow) String() string {
	s := r.rev
	if s == "" {
		s = "-"
	}
	if r.kind != deadlineRowServing {
		s += ":" + deadlineRowKindNames[r.kind]
		if r.readyAt.IsZero() {
			s += "(no capacity)"
		}
	}
	if r.target != "" {
		s += ">" + r.target
	}
	return s
}

// deadlineMember is one Component's replica as the world holds it.
type deadlineMember struct {
	comp       v1beta1.ComponentType
	rows       []deadlineRow
	desired    int32
	gang       int32
	budget     string
	updateRev  string
	current    string
	rollbackTo string
	partition  *int32
	generation int64
	// noCapacity marks a member whose starts toward the new revision can
	// get no capacity: its image cannot be pulled, so an attempt opened
	// under it never comes up until the fault is removed or a corrected
	// revision is pushed.
	noCapacity bool
}

func (m deadlineMember) String() string {
	parts := make([]string, 0, len(m.rows))
	for _, r := range m.rows {
		parts = append(parts, r.String())
	}
	s := fmt.Sprintf("%s(%d/%s) %s", m.comp, m.desired, m.budget, strings.Join(parts, " "))
	if m.partition != nil {
		s += fmt.Sprintf(" partition=%d", *m.partition)
	}
	if m.noCapacity {
		s += " no-capacity"
	}
	return s
}

// step is the member's own batch: its budget over its current count,
// rounded up, as the workload engine sizes it.
func (m deadlineMember) step() int32 {
	return utils.ScaledCountFromIntOrString(ptr.To(intstr.Parse(m.budget)), int32(len(m.rows)), true)
}

// buildTarget is the revision the member's workload builds toward: the
// rollback target its replica carries, else the spec.
func (m deadlineMember) buildTarget() string {
	if m.rollbackTo != "" {
		return query.RevisionFromName(m.rollbackTo).Hash()
	}
	return m.updateRev
}

// servingOn counts the member's Instances in rotation on a revision hash.
func (m deadlineMember) servingOn(hash string) int32 {
	var n int32
	for _, r := range m.rows {
		if r.serving() && r.rev == hash {
			n++
		}
	}
	return n
}

// onVersion counts the member's Instances that serve a revision or build
// toward it.
func (m deadlineMember) onVersion(hash string) int32 {
	var n int32
	for _, r := range m.rows {
		if (r.kind == deadlineRowServing && r.rev == hash) || (r.inFlight() && r.target == hash) {
			n++
		}
	}
	return n
}

// inFlightForBudget counts the Instances the member's budget is spent on:
// the replacements in flight under SurgeThenDrain, the Instances out of
// rotation under RecreatePod, a lost one among them.
func (m deadlineMember) inFlightForBudget(surges bool) int32 {
	var n int32
	for _, r := range m.rows {
		switch {
		case surges && r.kind == deadlineRowSurging:
			n++
		case !surges && (r.kind == deadlineRowRecreating || r.kind == deadlineRowLost):
			n++
		}
	}
	return n
}

// attemptsOpen counts the roll's own starts in flight, the ones its budget
// admits: a lost Instance's rebuild is the world's doing and not among them.
func (m deadlineMember) attemptsOpen(surges bool) int32 {
	var n int32
	for _, r := range m.rows {
		if r.rebuilt {
			continue
		}
		if (surges && r.kind == deadlineRowSurging) || (!surges && r.kind == deadlineRowRecreating) {
			n++
		}
	}
	return n
}

// waitsForCapacity reports a start of the member that can get no capacity.
func (m deadlineMember) waitsForCapacity() bool {
	for _, r := range m.rows {
		if r.kind != deadlineRowServing && r.readyAt.IsZero() {
			return true
		}
	}
	return false
}

// newestReadySince is the newest moment one of the member's Instances
// serving hash came up; zero when none serves it.
func (m deadlineMember) newestReadySince(hash string) time.Time {
	var newest time.Time
	for _, r := range m.rows {
		if r.kind == deadlineRowServing && r.rev == hash && r.readySince.After(newest) {
			newest = r.readySince
		}
	}
	return newest
}

// newestAdmission is the newest moment pods of the member were created
// toward hash: an attempt opened toward it, or the rebuild of a lost
// Instance on it; zero when none is in flight.
func (m deadlineMember) newestAdmission(hash string) time.Time {
	var newest time.Time
	for _, r := range m.rows {
		if r.kind != deadlineRowServing && r.toward() == hash && r.createdAt.After(newest) {
			newest = r.createdAt
		}
	}
	return newest
}

// deadlineTiming is how long the world takes to bring a start up.
type deadlineTiming struct {
	name string
	// start is a pod's time from creation to Ready; lag the time from an
	// Instance's Ready to the pass that reads it (the status write and the
	// endpoint).
	start, lag time.Duration
	// stagger is the time between the members of a gang coming up, one
	// after another; placement the gang scheduler's wait before a gang's
	// pods start, zero under the default scheduler.
	stagger, placement time.Duration
	// slow is the start time on the slow node, under the last Instance of
	// each side, the first one a canary moves; zero for no slow node.
	slow time.Duration
}

func (t deadlineTiming) String() string { return t.name }

// deadlineTimings are the timings under exploration: the default
// scheduler, the gang scheduler's placement wait, and a slow node.
var deadlineTimings = []deadlineTiming{
	{name: "default", start: 20 * time.Second, lag: 5 * time.Second, stagger: 5 * time.Second},
	{name: "gang-scheduler", start: 20 * time.Second, lag: 5 * time.Second, stagger: 5 * time.Second, placement: 10 * time.Second},
	{name: "slow-node", start: 20 * time.Second, lag: 5 * time.Second, stagger: 5 * time.Second, slow: 60 * time.Second},
}

// deadlineBudgets is how many times the world may still do each costly
// thing in one run.
type deadlineBudgets struct {
	lostPods, noCapacity, pushes, rollbacks, pauses, resumes int8
}

// deadlineConfig is one configuration under exploration.
type deadlineConfig struct {
	name         string
	shape        [2]int32 // engine, decoder
	routers      int32
	gang         [2]int32
	strategy     v1beta1.UpdateStrategyType
	stepsName    string
	steps        []v1beta1.RolloutGroupStep
	readyTimeout time.Duration
	timing       deadlineTiming
	budgets      deadlineBudgets
}

func (c deadlineConfig) surges() bool { return c.strategy == v1beta1.UpdateStrategySurgeThenDrain }

func (c deadlineConfig) String() string {
	short := "recreate"
	if c.surges() {
		short = "surge"
	}
	s := fmt.Sprintf("%d:%d+router%d %s/10%% steps=%s timeout=%s timing=%s", c.shape[0], c.shape[1], c.routers, short, c.stepsName, c.readyTimeout, c.timing)
	if c.gang[0] > 1 || c.gang[1] > 1 {
		s += fmt.Sprintf(" gang%dx%d", c.gang[0], c.gang[1])
	}
	return s
}

// deadlineMove is one thing the world or the operator does between two
// passes.
type deadlineMove struct {
	kind   string
	member int
	idx    int
}

func (m deadlineMove) String() string {
	switch m.kind {
	case "podloss":
		return fmt.Sprintf("pod of member %d row %d lost", m.member, m.idx)
	case "nocapacity":
		return fmt.Sprintf("member %d can get no capacity", m.member)
	}
	return m.kind
}

// deadlineStop is one stop a pass recorded, with the picture at the stop:
// when the wait began, when an Instance of the unit last came up on the
// new revision, how many starts were coming up and the soonest of them,
// and whether a side could get no capacity since the later of those two
// moments (the engine cannot tell a start that waited on a fault from one
// that will never come up).
type deadlineStop struct {
	reason       v1beta1.CanaryFailureReason
	step         int32
	at           time.Time
	since        time.Time
	lastProgress time.Time
	admitted     time.Time
	coming       int
	youngest     time.Time
	soonest      time.Time
	fault        bool
	event        string
}

// anchor is the moment one ready timeout must have passed from before a
// stop for capacity is the deadline's job: the latest of the wait's start,
// the last Instance of the unit that came up on the new revision and the
// last start admitted toward it.
func (s deadlineStop) anchor() time.Time {
	return laterOf(laterOf(s.since, s.lastProgress), s.admitted)
}

// deadlineState is the model at one moment: the service as the controller
// holds it in memory, every member's replica, the clock, the budgets left,
// the stops the passes since the last move recorded, and the moves that led
// here.
type deadlineState struct {
	isvc    *v1beta1.InferenceService
	members []deadlineMember
	now     time.Time
	budgets deadlineBudgets
	stops   []deadlineStop
	// capacityBackAt is the last moment a member got its capacity back, by
	// the fault's removal or a corrected push; zero when none did.
	capacityBackAt time.Time
	unsettled      bool
	pushed         bool
	path           []deadlineMove
}

func (st *deadlineState) clone() *deadlineState {
	out := &deadlineState{
		isvc:           st.isvc.DeepCopy(),
		members:        make([]deadlineMember, len(st.members)),
		now:            st.now,
		budgets:        st.budgets,
		capacityBackAt: st.capacityBackAt,
		pushed:         st.pushed,
		path:           append([]deadlineMove(nil), st.path...),
	}
	for i, m := range st.members {
		m.rows = append([]deadlineRow(nil), m.rows...)
		if m.partition != nil {
			m.partition = ptr.To(*m.partition)
		}
		out.members[i] = m
	}
	return out
}

func (st *deadlineState) String() string {
	parts := make([]string, 0, len(st.members))
	for _, m := range st.members {
		parts = append(parts, m.String())
	}
	return strings.Join(parts, " | ")
}

// eventful reports a path with a move of the world or the operator beside
// the clock and the verbs a straight run answers holds with.
func (st *deadlineState) eventful() bool {
	for _, mv := range st.path {
		switch mv.kind {
		case "clock", "promote", "unpause":
		default:
			return true
		}
	}
	return false
}

// deadlineWorld builds the objects the production code reads from a state
// and runs the controller's pass over them.
type deadlineWorld struct {
	t      *testing.T
	cfg    deadlineConfig
	ctx    context.Context
	scheme *runtime.Scheme
	// passes counts controller passes; dispatches the canary dispatches in them.
	passes, dispatches int
}

func newDeadlineWorld(t *testing.T, cfg deadlineConfig, scheme *runtime.Scheme) *deadlineWorld {
	return &deadlineWorld{t: t, cfg: cfg, ctx: context.Background(), scheme: scheme}
}

// deadlineFailure is an error the production code returned inside a pass;
// the exploration records it as a finding of its own.
type deadlineFailure string

func (w *deadlineWorld) fail(format string, args ...any) {
	panic(deadlineFailure(fmt.Sprintf(format, args...)))
}

// deadlineISVC is the service: the engine, the decoder and the routers
// declared with their counts, and one canary group over them with the
// step list and the plan's ready timeout, the way step lists are usually written.
func deadlineISVC(cfg deadlineConfig) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: deadlineService, Namespace: deadlineNamespace, Generation: 1}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(cfg.shape[0]))}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(cfg.shape[1]))}}
	comps := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	if cfg.routers > 0 {
		isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(cfg.routers))}}
		comps = append(comps, v1beta1.RouterComponent)
	}
	canary := &v1beta1.GroupCanary{Steps: append([]v1beta1.RolloutGroupStep(nil), cfg.steps...)}
	if cfg.readyTimeout > 0 {
		canary.ReadyTimeout = &metav1.Duration{Duration: cfg.readyTimeout}
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{Components: comps, Canary: canary}}}
	return isvc
}

// initial is the moment after the operator pushed the second revision:
// every Instance serves the first one, and every replica's target is the
// second.
func (w *deadlineWorld) initial() *deadlineState {
	st := &deadlineState{isvc: deadlineISVC(w.cfg), now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), budgets: w.cfg.budgets}
	add := func(comp v1beta1.ComponentType, desired, gang int32) {
		rows := make([]deadlineRow, desired)
		for idx := range rows {
			rows[idx] = deadlineRow{kind: deadlineRowServing, rev: deadlineRevisions[0], readySince: st.now}
		}
		st.members = append(st.members, deadlineMember{
			comp: comp, rows: rows, desired: desired, gang: gang, budget: "10%",
			updateRev: deadlineRevisions[1], current: deadlineRevisions[0], generation: 1,
		})
	}
	add(v1beta1.EngineComponent, w.cfg.shape[0], w.cfg.gang[0])
	add(v1beta1.DecoderComponent, w.cfg.shape[1], w.cfg.gang[1])
	if w.cfg.routers > 0 {
		add(v1beta1.RouterComponent, w.cfg.routers, 1)
	}
	return st
}

// startTime is how long a start of member i's Instance idx takes to come
// up: a pod's start, or the slow node's under the last Instance of a
// pairing side; the gang scheduler's placement wait before a gang starts;
// and the members of a gang coming up one after another.
func (w *deadlineWorld) startTime(st *deadlineState, i, idx int) time.Duration {
	m := st.members[i]
	t := w.cfg.timing
	d := t.start
	if t.slow > 0 && m.comp != v1beta1.RouterComponent && idx == int(m.desired)-1 {
		d = t.slow
	}
	if m.gang > 1 {
		d += t.placement + time.Duration(m.gang-1)*t.stagger
	}
	return d
}

// rollup publishes each member's current revision the way the replica's
// status does: the revision every Instance serves, once they all serve
// one.
func (w *deadlineWorld) rollup(st *deadlineState) {
	for i := range st.members {
		m := &st.members[i]
		shared := ""
		for _, r := range m.rows {
			if r.kind != deadlineRowServing || (shared != "" && r.rev != shared) {
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

// rowStatus is one Instance row as the replica publishes it: the phase, the
// revisions, the pod counts at the gang's size, the operation, and the
// moment the Instance came up for the rows that serve.
func deadlineRowStatus(m deadlineMember, idx int, row deadlineRow) v1beta1.OMENativeInstanceStatus {
	rs := v1beta1.OMENativeInstanceStatus{Index: int32(idx)}
	running, target := deadlineRevisionName(m.comp, row.rev), deadlineRevisionName(m.comp, row.target)
	op := func(id string, kind v1beta1.InstanceOperationType, step string) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{ID: id, Type: kind, Step: step}
	}
	ready := func(count int32) {
		rs.ReadyPodCount, rs.ServingPodCount, rs.AvailablePodCount = count, count, count
		rs.ReadySince = &metav1.Time{Time: row.readySince}
	}
	switch row.kind {
	case deadlineRowServing:
		rs.Phase, rs.RunningRevision, rs.PodCount = v1beta1.OMENativeInstanceReady, running, m.gang
		ready(m.gang)
	case deadlineRowLost:
		rs.Phase, rs.RunningRevision, rs.PodCount = v1beta1.OMENativeInstanceRestarting, running, m.gang
		rs.Operation = op("repair", v1beta1.InstanceOperationRestart, "")
	case deadlineRowRecreating:
		rs.Phase, rs.RunningRevision, rs.TargetRevision, rs.PodCount = v1beta1.OMENativeInstanceUpdating, running, target, m.gang
		rs.Operation = op("attempt", v1beta1.InstanceOperationUpdate, workloadtypes.UpdateStepDrain)
	case deadlineRowSurging:
		rs.Phase, rs.RunningRevision, rs.TargetRevision, rs.PodCount = v1beta1.OMENativeInstanceUpdating, running, target, 2*m.gang
		ready(m.gang)
		rs.Operation = op("attempt", v1beta1.InstanceOperationUpdate, workloadtypes.UpdateStepSurge)
	case deadlineRowCreating:
		rs.Phase, rs.TargetRevision, rs.PodCount = v1beta1.OMENativeInstanceCreating, target, m.gang
		rs.Operation = op("create", v1beta1.InstanceOperationCreate, "")
	}
	return rs
}

// replica is the InferenceReplica a member publishes.
func (w *deadlineWorld) replica(st *deadlineState, i int) *v1beta1.InferenceReplica {
	m := st.members[i]
	r := ir(deadlineNamespace, deadlineService, m.comp, m.updateRev)
	r.Generation = m.generation
	r.Spec.Replicas = ptr.To(m.desired)
	r.Spec.Runners = gateRunners(m.gang)
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
	r.Status.CurrentRevision = deadlineRevisionName(m.comp, m.current)
	r.Status.UpdateRevision = deadlineRevisionName(m.comp, m.updateRev)
	r.Status.Replicas = int32(len(m.rows))
	var ready, updated int32
	for idx, row := range m.rows {
		if row.serving() {
			ready++
		}
		if row.kind == deadlineRowServing && row.rev == m.updateRev {
			updated++
		}
		r.Status.InstanceStatuses = append(r.Status.InstanceStatuses, deadlineRowStatus(m, idx, row))
	}
	r.Status.ReadyReplicas, r.Status.ServingReplicas, r.Status.AvailableReplicas, r.Status.UpdatedReplicas = ready, ready, ready, updated
	return r
}

// pods are one Instance's pods as the kubelet reports them, the gang's
// worth per revision in play.
func (w *deadlineWorld) pods(m deadlineMember, idx int, row deadlineRow) []runtime.Object {
	add := func(rev, suffix string, ready bool, created time.Time) []runtime.Object {
		out := gateInstancePods(deadlineNamespace, deadlineService, m.comp, idx, m.gang, m.gang, rev, suffix, ready)
		for _, o := range out {
			o.(*corev1.Pod).CreationTimestamp = metav1.NewTime(created)
		}
		return out
	}
	switch row.kind {
	case deadlineRowServing:
		return add(row.rev, "", true, time.Time{})
	case deadlineRowLost:
		return add(row.rev, "", false, row.createdAt)
	case deadlineRowRecreating, deadlineRowCreating:
		return add(row.target, "", false, row.createdAt)
	case deadlineRowSurging:
		return append(add(row.rev, "", true, time.Time{}), add(row.target, "-s", false, row.createdAt)...)
	}
	return nil
}

// objects are everything the pass reads: the service, each member's replica
// and pods, and every revision of every member.
func (w *deadlineWorld) objects(st *deadlineState) []runtime.Object {
	objs := []runtime.Object{st.isvc.DeepCopy()}
	for i, m := range st.members {
		objs = append(objs, w.replica(st, i))
		for idx, row := range m.rows {
			objs = append(objs, w.pods(m, idx, row)...)
		}
		for n, hash := range deadlineRevisions {
			objs = append(objs, canaryControllerRevision(deadlineNamespace, deadlineService, string(m.comp), hash, int64(n+1)))
		}
	}
	return objs
}

func (w *deadlineWorld) client(st *deadlineState) client.Client {
	return fake.NewClientBuilder().WithScheme(w.scheme).WithRuntimeObjects(w.objects(st)...).Build()
}

// replicaReader serves the members' replicas to the coordination gate from
// the model's state, the way the replica controller's live reader does:
// every start of the pass so far is on its row before the next start is
// asked about.
func (w *deadlineWorld) replicaReader(st *deadlineState) memberReplicaReader {
	comps := make([]v1beta1.ComponentType, len(st.members))
	for i, m := range st.members {
		comps[i] = m.comp
	}
	return memberReplicaReader{namespace: deadlineNamespace, service: deadlineService, comps: comps, replica: func(i int) *v1beta1.InferenceReplica { return w.replica(st, i) }}
}

// startAdmitted asks the coordination update gate about one more start of
// member i toward target, with admitted starts already taken in this pass,
// under the chart's prefill/decode settings.
func (w *deadlineWorld) startAdmitted(st *deadlineState, i int, from, target string, admitted int32) bool {
	m := st.members[i]
	revision := canaryControllerRevision(deadlineNamespace, deadlineService, string(m.comp), target, int64(sort.SearchStrings(deadlineRevisions, target)+1))
	return coordinationAdmits(w.ctx, w.replicaReader(st), st.isvc, m.comp, deadlineRevisionName(m.comp, from), revision, w.cfg.strategy, admitted)
}

// open starts member i's Instance idx toward target on the clock: the
// attempt opens now, its pods are created now and come up after the start
// time, or never while the member can get no capacity toward the new
// revision.
func (w *deadlineWorld) open(st *deadlineState, i, idx int, target string) {
	m := &st.members[i]
	r := &m.rows[idx]
	r.target, r.openedAt, r.createdAt, r.readyAt = target, st.now, st.now, st.now.Add(w.startTime(st, i, idx))
	if m.noCapacity && target == m.updateRev {
		r.readyAt = time.Time{}
	}
}

// heldRows are the lowest-indexed Instances off the target, the
// partition's worth of them. An attempt toward a revision that is no
// longer the target turns toward the target and is timed again, and a
// lost Instance the roll may move is rebuilt at the target, as the
// workload's repair yields to the roll.
func (w *deadlineWorld) heldRows(st *deadlineState, i int, target string) map[int]bool {
	m := &st.members[i]
	var partition int32
	if m.partition != nil {
		partition = *m.partition
	}
	held := map[int]bool{}
	var holding int32
	for idx := range m.rows {
		r := &m.rows[idx]
		if r.inFlight() && r.target != target {
			w.open(st, i, idx, target)
		}
		if r.inFlight() || r.rev == target {
			continue
		}
		if holding < partition {
			held[idx] = true
			holding++
		}
	}
	for idx := range m.rows {
		if r := &m.rows[idx]; r.kind == deadlineRowLost && r.rev != target && !held[idx] {
			r.kind, r.rebuilt = deadlineRowRecreating, true
			w.open(st, i, idx, target)
		}
	}
	return held
}

// workloadStep is the workload engine's reaction to the partition and the
// rollback target its replica carries: the held Instances stay, and the
// rest start toward the target under the member's strategy and budget,
// each start admitted by the coordination update gate first. A paused
// rollout starts nothing new.
func (w *deadlineWorld) workloadStep(st *deadlineState, i int) {
	m := &st.members[i]
	target := m.buildTarget()
	held := w.heldRows(st, i, target)
	if paused, _ := constants.RolloutPauseState(st.isvc.Annotations); paused {
		return
	}
	budget := m.step()
	inFlight := m.inFlightForBudget(w.cfg.surges())
	var admitted int32
	for idx := range m.rows {
		r := &m.rows[idx]
		if held[idx] || r.kind != deadlineRowServing || r.rev == target {
			continue
		}
		if inFlight >= budget || !w.startAdmitted(st, i, r.rev, target, admitted) {
			break
		}
		r.kind = deadlineRowRecreating
		if w.cfg.surges() {
			r.kind = deadlineRowSurging
		}
		w.open(st, i, idx, target)
		inFlight++
		admitted++
	}
}

// serveDue brings up every Instance whose pods the pass can read Ready: the
// pods came up a lag ago, the status write and the endpoint behind them.
// An attempt lands on its target, a lost Instance comes back on its own
// revision, and the row's readySince is the moment the pods came up.
func (w *deadlineWorld) serveDue(st *deadlineState) {
	for i := range st.members {
		for idx := range st.members[i].rows {
			r := &st.members[i].rows[idx]
			if !r.coming() || r.readyAt.Add(w.cfg.timing.lag).After(st.now) {
				continue
			}
			if r.inFlight() {
				r.rev = r.target
			}
			r.kind, r.target, r.readySince, r.rebuilt = deadlineRowServing, "", r.readyAt, false
			r.openedAt, r.createdAt, r.readyAt = time.Time{}, time.Time{}, time.Time{}
		}
	}
}

// unit reads the one canary group's record; nil when the service has none.
func (w *deadlineWorld) unit(st *deadlineState) *pairUnit {
	return canaryUnitOf(st.isvc)
}

// stopReason is the reason of the unit's recorded stop, empty when it
// stands no stop.
func (w *deadlineWorld) stopReason(st *deadlineState) v1beta1.CanaryFailureReason {
	u := w.unit(st)
	if u == nil || u.record == nil || u.record.Failed == nil {
		return ""
	}
	return u.record.Failed.Reason
}

// pass is one controller pass at the clock's moment: the Instances due
// come up, then the run layer, the binding of a freshly opened run, the
// partition each member is projected with and the workload's reaction to
// it, then the canary dispatch, and the read-back of the rollback target
// the dispatch wrote on each replica. A stop the dispatch records is noted
// with the picture at that moment.
func (w *deadlineWorld) pass(st *deadlineState) {
	w.serveDue(st)
	w.rollup(st)
	isvc := st.isvc
	c := w.client(st)
	events := &gateEvents{}
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
	// The projector writes every member's partition in the service's pass;
	// each member's workload then reads its peers' partitions as written.
	for i := range st.members {
		var p *int32
		if resolver := PartitionResolver(isvc, policies, st.members[i].comp); resolver != nil {
			p = resolver(st.members[i].desired)
			if p != nil && *p > st.members[i].desired {
				p = ptr.To(st.members[i].desired)
			}
		} else {
			st.members[i].rollbackTo = ""
		}
		st.members[i].partition = p
	}
	for i := range st.members {
		w.workloadStep(st, i)
	}
	w.rollup(st)
	c = w.client(st)
	stoppedBefore := w.stopReason(st) != ""
	for _, g := range rollout.CanaryGroups(isvc, policies) {
		if _, err := Dispatch(w.ctx, DispatchDeps{
			Client: c, Reader: c, Recorder: events, ISVC: isvc, Policies: policies, Now: st.now,
			DefaultReadyTimeout: cellReadyTimeout, Requeue: reconcileRequeue, ParkedRequeue: failedRequeue,
			ComponentRunnerPorts: canaryRunnerPorts(), Group: g,
		}); err != nil {
			w.fail("dispatch: %v (state %s)", err, st)
		}
		w.dispatches++
	}
	if reason := w.stopReason(st); reason != "" && !stoppedBefore {
		w.noteStop(st, reason, events)
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
}

// noteStop records a stop the dispatch just wrote with the picture at
// that moment: when the wait began, when an Instance of the unit last came
// up on the new revision, the starts coming up and the soonest of them,
// whether a side could get no capacity, and the event that named the stop.
func (w *deadlineWorld) noteStop(st *deadlineState, reason v1beta1.CanaryFailureReason, events *gateEvents) {
	u := w.unit(st)
	cs := u.record
	stop := deadlineStop{reason: reason, step: cs.CurrentStep, at: st.now}
	if cs.CapacityWaitSince != nil {
		stop.since = cs.CapacityWaitSince.Time
	}
	for _, m := range st.members {
		stop.lastProgress = laterOf(stop.lastProgress, m.newestReadySince(cs.CanaryRevisionHash))
		stop.admitted = laterOf(stop.admitted, m.newestAdmission(cs.CanaryRevisionHash))
		if m.noCapacity {
			stop.fault = true
		}
		for _, r := range m.rows {
			if !r.coming() || r.toward() != cs.CanaryRevisionHash {
				continue
			}
			stop.coming++
			stop.youngest = laterOf(stop.youngest, r.createdAt)
			if stop.soonest.IsZero() || r.readyAt.Before(stop.soonest) {
				stop.soonest = r.readyAt
			}
		}
	}
	if st.capacityBackAt.After(stop.anchor()) {
		stop.fault = true
	}
	for _, e := range events.messages {
		if strings.Contains(e, EventReasonCanaryRollHeld) {
			stop.event = e
		}
	}
	st.stops = append(st.stops, stop)
}

// settle runs passes until a pass changes nothing the key reads.
func (w *deadlineWorld) settle(st *deadlineState) {
	for round := 0; round < 8; round++ {
		before := st.key()
		w.pass(st)
		if st.key() == before {
			return
		}
	}
	st.unsettled = true
}

// deadlines are the moments the record waits for: the ready timeout of a
// capacity wait, measured from the wait's start, from the last Instance of
// the unit that came up on the new revision and from the last start
// admitted toward it (the clock visits each, so a stop at any is read),
// the end of a timed pause, and the end of the drain window.
func (w *deadlineWorld) deadlines(st *deadlineState) []time.Time {
	u := w.unit(st)
	if u == nil || u.record == nil {
		return nil
	}
	var out []time.Time
	cs := u.record
	if cs.CapacityWaitSince != nil {
		out = append(out, cs.CapacityWaitSince.Add(w.timeout()))
		for _, m := range st.members {
			for _, at := range []time.Time{m.newestReadySince(cs.CanaryRevisionHash), m.newestAdmission(cs.CanaryRevisionHash)} {
				if at.After(cs.CapacityWaitSince.Time) {
					out = append(out, at.Add(w.timeout()))
				}
			}
		}
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

// timeout is the ready timeout the plan sets, else the operator's default.
func (w *deadlineWorld) timeout() time.Duration {
	if w.cfg.readyTimeout > 0 {
		return w.cfg.readyTimeout
	}
	return cellReadyTimeout
}

// nextMoment is the next moment the controller acts on: the pass that
// reads the soonest Instance coming up, or the pass after the soonest
// deadline still ahead. None when nothing is coming and no deadline is
// ahead.
func (w *deadlineWorld) nextMoment(st *deadlineState) (time.Time, bool) {
	var next time.Time
	consider := func(t time.Time) {
		if t.After(st.now) && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}
	for _, m := range st.members {
		for _, r := range m.rows {
			if r.coming() {
				consider(r.readyAt.Add(w.cfg.timing.lag))
			}
		}
	}
	for _, d := range w.deadlines(st) {
		consider(d.Add(time.Second))
	}
	return next, !next.IsZero()
}

// key is the state as the model distinguishes it: the rows of every member
// with what is coming and when, the spec, the service's annotations and
// status, and the budgets left.
func (st *deadlineState) key() string {
	var b strings.Builder
	for _, m := range st.members {
		for _, r := range m.rows {
			fmt.Fprintf(&b, "%d%s>%s@%d/%d,", r.kind, r.rev, r.target, r.readyAt.Unix(), r.createdAt.Unix())
		}
		p := "-"
		if m.partition != nil {
			p = strconv.Itoa(int(*m.partition))
		}
		fmt.Fprintf(&b, "|d%d u%s c%s rb%s p%s g%d nc%t;", m.desired, m.updateRev, m.current, m.rollbackTo, p, m.generation, m.noCapacity)
	}
	keys := make([]string, 0, len(st.isvc.Annotations))
	for k, v := range st.isvc.Annotations {
		keys = append(keys, k+"="+v)
	}
	sort.Strings(keys)
	b.WriteString("a:" + strings.Join(keys, ",") + ";")
	if r := st.isvc.Status.Rollout; r != nil {
		if r.ActiveRun != nil {
			b.WriteString("run:" + r.ActiveRun.RunID)
		}
		if r.LastRun != nil {
			b.WriteString("last:" + string(r.LastRun.Outcome))
		}
		b.WriteString(";")
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
			since := int64(0)
			if rec.CapacityWaitSince != nil {
				since = rec.CapacityWaitSince.Unix()
			}
			fmt.Fprintf(&b, ",%d,%t)", since, rec.StepEnteredTime != nil)
		}
		b.WriteString(";")
	}
	fmt.Fprintf(&b, "budgets:%+v;", st.budgets)
	return b.String()
}

// finished reports a run at its end: no run open, every Instance serving,
// and the unit's record at the done mark and Stable, or rolled back.
func (w *deadlineWorld) finished(st *deadlineState) bool {
	if v1beta1.RolloutRunActive(st.isvc) {
		return false
	}
	for _, m := range st.members {
		for _, r := range m.rows {
			if r.kind != deadlineRowServing {
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

// progressMoves is the world's own move: the clock reaching the next
// moment the controller acts on.
func (w *deadlineWorld) progressMoves(st *deadlineState) []deadlineMove {
	if _, ok := w.nextMoment(st); ok {
		return []deadlineMove{{kind: "clock"}}
	}
	return nil
}

// releaseMoves are the verbs a hold names as its way out: the end of a
// pause, a promote at a gate, a resume or a rollback of a stopped unit, a
// corrected push at a stopped step, and the capacity a side could not get
// coming back. The exploration spends budgets on them; the classification
// of a quiet state asks for them regardless, since a budget is the model's
// bound and not a hold.
func (w *deadlineWorld) releaseMoves(st *deadlineState, ignoreBudget bool) []deadlineMove {
	var out []deadlineMove
	ann := st.isvc.Annotations
	budgets := st.budgets
	if ignoreBudget {
		budgets = deadlineBudgets{resumes: 1, rollbacks: 1, pushes: 1}
	}
	if paused, _ := constants.RolloutPauseState(ann); paused {
		out = append(out, deadlineMove{kind: "unpause"})
	}
	for i, m := range st.members {
		if m.noCapacity && m.waitsForCapacity() {
			out = append(out, deadlineMove{kind: "capacity-back", member: i})
		}
	}
	u := w.unit(st)
	if u == nil || u.record == nil {
		return out
	}
	_, promoting := ann[constants.RolloutPromoteAnnotation]
	switch u.state {
	case rollout.CanaryStateServing, rollout.CanaryStatePreHold, rollout.CanaryStateDraining:
		if !promoting {
			out = append(out, deadlineMove{kind: "promote"})
		}
	case rollout.CanaryStateFailed, rollout.CanaryStateRolledBack:
		if _, resuming := ann[constants.RolloutResumeAnnotation]; !resuming && budgets.resumes > 0 {
			out = append(out, deadlineMove{kind: "resume"})
		}
		if u.state == rollout.CanaryStateFailed && budgets.rollbacks > 0 && u.record.RolledBackRevisionHash == "" {
			out = append(out, deadlineMove{kind: "rollback"})
		}
		if u.state == rollout.CanaryStateFailed && budgets.pushes > 0 && st.members[0].updateRev != deadlineRevisions[2] {
			out = append(out, deadlineMove{kind: "push"})
		}
	}
	return out
}

// eventMoves are the operator's and the world's costly moves, each limited
// by its budget: a pause, a rollback of a live unit, a pod lost on the new
// revision of either pairing side, a side whose starts toward the new
// revision can get no capacity, and in the full mode a corrected push at
// any live moment.
func (w *deadlineWorld) eventMoves(st *deadlineState, full bool) []deadlineMove {
	var out []deadlineMove
	ann := st.isvc.Annotations
	b := st.budgets
	if paused, _ := constants.RolloutPauseState(ann); !paused && b.pauses > 0 {
		out = append(out, deadlineMove{kind: "pause"})
	}
	u := w.unit(st)
	live := u != nil && u.record != nil && u.state != rollout.CanaryStateDone && u.state != rollout.CanaryStateRolledBack && u.state != rollout.CanaryStateRollingBack && u.state != rollout.CanaryStateFailed
	if !live {
		return out
	}
	if b.rollbacks > 0 && u.record.RolledBackRevisionHash == "" {
		out = append(out, deadlineMove{kind: "rollback"})
	}
	if full && b.pushes > 0 && st.members[0].updateRev != deadlineRevisions[2] {
		out = append(out, deadlineMove{kind: "push"})
	}
	for i, m := range st.members {
		if m.comp == v1beta1.RouterComponent {
			continue
		}
		if b.noCapacity > 0 && !m.noCapacity && m.onVersion(m.updateRev) < m.desired {
			out = append(out, deadlineMove{kind: "nocapacity", member: i})
		}
		if b.lostPods == 0 {
			continue
		}
		for idx, r := range m.rows {
			if r.kind == deadlineRowServing && r.rev == u.record.CanaryRevisionHash {
				out = append(out, deadlineMove{kind: "podloss", member: i, idx: idx})
				break
			}
		}
	}
	return out
}

// apply makes one move and lets the controller settle on it. The clock's
// move lands on the next moment; every other move takes one second.
func (w *deadlineWorld) apply(st *deadlineState, mv deadlineMove) *deadlineState {
	next := st.clone()
	next.path = append(next.path, mv)
	next.now = next.now.Add(time.Second)
	switch mv.kind {
	case "clock":
		if moment, ok := w.nextMoment(st); ok {
			next.now = moment
		}
	case "promote", "resume":
		if u := w.unit(next); u != nil && u.record != nil {
			annotate(next.isvc, w.verbAnnotation(mv.kind), u.record.CanaryRevisionHash)
		}
		if mv.kind == "resume" {
			next.budgets.resumes--
		}
	case "pause":
		next.budgets.pauses--
		annotate(next.isvc, constants.PausedRolloutAnnotation, "true")
	case "unpause":
		delete(next.isvc.Annotations, constants.PausedRolloutAnnotation)
	case "rollback":
		next.budgets.rollbacks--
		annotate(next.isvc, constants.RolloutRollbackAnnotation, "true")
	case "podloss":
		next.budgets.lostPods--
		w.losePod(next, mv.member, mv.idx)
	case "nocapacity":
		next.budgets.noCapacity--
		w.takeCapacity(next, mv.member)
	case "capacity-back":
		w.giveCapacity(next, mv.member)
	case "push":
		next.budgets.pushes--
		next.pushed = true
		for i := range next.members {
			next.members[i].updateRev = deadlineRevisions[2]
			next.members[i].generation++
			w.giveCapacity(next, i)
		}
		next.isvc.Generation++
	default:
		w.fail("unknown move %q", mv.kind)
	}
	w.settle(next)
	return next
}

// verbAnnotation is the annotation a promote or a resume writes.
func (w *deadlineWorld) verbAnnotation(kind string) string {
	if kind == "resume" {
		return constants.RolloutResumeAnnotation
	}
	return constants.RolloutPromoteAnnotation
}

// losePod takes a serving Instance's pods away; the workload rebuilds them
// on the Instance's own revision, on the clock, or never while that
// revision is the new one and the member can get no capacity toward it.
func (w *deadlineWorld) losePod(st *deadlineState, i, idx int) {
	m := &st.members[i]
	r := &m.rows[idx]
	r.kind, r.target, r.openedAt, r.createdAt, r.readyAt = deadlineRowLost, "", st.now, st.now, st.now.Add(w.startTime(st, i, idx))
	if m.noCapacity && r.rev == m.updateRev {
		r.readyAt = time.Time{}
	}
}

// takeCapacity takes a member's capacity toward the new revision away: the
// starts in flight toward it stop coming up.
func (w *deadlineWorld) takeCapacity(st *deadlineState, i int) {
	m := &st.members[i]
	m.noCapacity = true
	for idx := range m.rows {
		r := &m.rows[idx]
		toward := r.target
		if r.kind == deadlineRowLost {
			toward = r.rev
		}
		if r.kind != deadlineRowServing && toward == m.updateRev {
			r.readyAt = time.Time{}
		}
	}
}

// giveCapacity gives a member its capacity back, by the fault's removal or
// a corrected push: every start that was waiting on it comes up a whole
// start time from now.
func (w *deadlineWorld) giveCapacity(st *deadlineState, i int) {
	m := &st.members[i]
	if m.noCapacity || m.waitsForCapacity() {
		st.capacityBackAt = st.now
	}
	m.noCapacity = false
	for idx := range m.rows {
		if r := &m.rows[idx]; r.kind != deadlineRowServing && r.readyAt.IsZero() {
			r.openedAt, r.readyAt = st.now, st.now.Add(w.startTime(st, i, idx))
		}
	}
}

// deadlineFinding is one reading at one state.
type deadlineFinding struct {
	class   string
	message string
	state   *deadlineState
}

// check reads every invariant at one settled state.
func (w *deadlineWorld) check(st *deadlineState, full bool) []deadlineFinding {
	var out []deadlineFinding
	add := func(class, format string, args ...any) {
		out = append(out, deadlineFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	for _, stop := range st.stops {
		class, message := w.readStop(stop)
		add(class, "%s", message)
	}
	if st.unsettled {
		add("unsettled", "the controller kept changing its status for eight passes with no move in between")
	}
	w.checkCapacityWait(st, add)
	for _, m := range st.members {
		if n, budget := m.attemptsOpen(w.cfg.surges()), m.step(); n > budget {
			add("budget-exceeded", "%s has %d starts of the roll in flight against a budget of %d: %s", m.comp, n, budget, st)
		}
	}
	w.checkSidesTogether(st, add)
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

// readStop classifies a recorded stop. A stop for capacity with no fault
// in the world while a start toward the new revision was coming up that
// had been admitted less than a ready timeout earlier is the deadline run
// against starts the pacing puts one after another, or against progress a
// pod loss took off the record; one while every start coming up had been
// admitted a whole timeout or more earlier is a start slower than the
// timeout, the deadline's job; one with nothing coming up is a hold the
// deadline punished; a stop under a side that can get no capacity is the
// deadline's job, late when it lands more than one ready timeout after the
// wait began, the unit last made progress or a start was last admitted.
func (w *deadlineWorld) readStop(stop deadlineStop) (string, string) {
	if stop.reason != v1beta1.CanaryFailureCapacityTimeout {
		return "stop-" + strings.ToLower(string(stop.reason)), fmt.Sprintf("the unit stopped at step %d with reason %s", stop.step, stop.reason)
	}
	picture := fmt.Sprintf("step %d stopped for capacity %s after the wait began; an Instance of the unit last came up on the new revision %s; a start was last admitted toward it %s (ready timeout %s)",
		stop.step, stop.at.Sub(stop.since), deadlineAgo(stop.at, stop.lastProgress), deadlineAgo(stop.at, stop.admitted), w.timeout())
	switch {
	case stop.fault:
		if stop.at.After(stop.anchor().Add(w.timeout() + w.cfg.timing.lag + 2*time.Second)) {
			return "no-capacity-stopped-late", picture + "; a side could get no capacity since the wait's anchor, and the stop came late"
		}
		return "no-capacity-stopped", picture + "; a side could get no capacity since the wait's anchor"
	case stop.coming > 0 && stop.at.Before(stop.youngest.Add(w.timeout())):
		return "healthy-step-stopped-while-a-start-was-coming-up", fmt.Sprintf("%s, while %d start(s) were coming up, the youngest admitted %s before the stop, the soonest Ready %s after the stop and read %s after that; %s", picture, stop.coming, stop.at.Sub(stop.youngest), stop.soonest.Sub(stop.at), w.cfg.timing.lag, stop.event)
	case stop.coming > 0:
		return "start-slower-than-the-ready-timeout-stopped", fmt.Sprintf("%s, while %d start(s) were coming up, each admitted a whole ready timeout or more before the stop, the soonest Ready %s after the stop; %s", picture, stop.coming, stop.soonest.Sub(stop.at), stop.event)
	}
	return "healthy-step-stopped-with-nothing-coming-up", picture + ", with nothing coming up and no fault in the world; " + stop.event
}

// deadlineAgo says how long before at the moment t came, or that it never
// came.
func deadlineAgo(at, t time.Time) string {
	if t.IsZero() {
		return "at no time"
	}
	return at.Sub(t).String() + " before the stop"
}

// checkCapacityWait reads a live unit's capacity wait under a side that can
// get no capacity: more than one ready timeout after the latest of the
// wait's start, the unit's last progress and its last start admitted, the
// step must stand stopped.
func (w *deadlineWorld) checkCapacityWait(st *deadlineState, add func(class, format string, args ...any)) {
	u := w.unit(st)
	if u == nil || u.record == nil || u.record.CapacityWaitSince == nil {
		return
	}
	switch u.state {
	case rollout.CanaryStateStaging, rollout.CanaryStateServing, rollout.CanaryStatePreHold, rollout.CanaryStateDraining, rollout.CanaryStateDone:
	default:
		return
	}
	fault := false
	anchor := u.record.CapacityWaitSince.Time
	for _, m := range st.members {
		fault = fault || m.noCapacity
		anchor = laterOf(anchor, m.newestReadySince(u.record.CanaryRevisionHash))
		anchor = laterOf(anchor, m.newestAdmission(u.record.CanaryRevisionHash))
	}
	if paused, _ := constants.RolloutPauseState(st.isvc.Annotations); paused || !fault {
		return
	}
	if st.now.After(anchor.Add(w.timeout() + w.cfg.timing.lag + 2*time.Second)) {
		add("no-capacity-not-stopped", "a side can get no capacity and the unit still waits at step %d, %s after the wait began, the unit last made progress or a start was last admitted (ready timeout %s): %s",
			u.record.CurrentStep, st.now.Sub(anchor), w.timeout(), w.describe(st))
	}
}

// checkSidesTogether reads the new version on one side only past one batch
// of that side: on a straight run the batch step moves the decoder first
// and the engines once it serves, so the picture is left only by an event.
func (w *deadlineWorld) checkSidesTogether(st *deadlineState, add func(class, format string, args ...any)) {
	engine, decoder := st.members[0], st.members[1]
	for _, hash := range deadlineRevisions[1:] {
		e, d := engine.servingOn(hash), decoder.servingOn(hash)
		if (e == 0) == (d == 0) {
			continue
		}
		side, other, count := engine, decoder, e
		if e == 0 {
			side, other, count = decoder, engine, d
		}
		if count <= side.step() {
			continue
		}
		class := "one-sided-version-beyond-a-batch"
		if st.eventful() {
			class += "-after-an-event"
		}
		add(class, "revision %s serves on %d %s Instances (one batch is %d) and on no %s Instance", hash, count, side.comp, side.step(), other.comp)
	}
}

// describe says where a state stands, for a person reading the report.
func (w *deadlineWorld) describe(st *deadlineState) string {
	var parts []string
	if r := st.isvc.Status.Rollout; r != nil {
		switch {
		case r.ActiveRun != nil:
			parts = append(parts, "run open")
		case r.LastRun != nil:
			parts = append(parts, "last run "+string(r.LastRun.Outcome))
		}
	}
	if u := w.unit(st); u != nil && u.record != nil {
		cs := u.record
		s := fmt.Sprintf("%s at step %d of %d, phase %s, weight %d", u.state, cs.CurrentStep, len(u.group.Canary.Steps), u.phase, cs.ObservedTrafficWeight)
		if cs.Failed != nil {
			s += ", failed " + string(cs.Failed.Reason)
		}
		if cs.CapacityWaitSince != nil {
			s += fmt.Sprintf(", capacity wait running for %s", st.now.Sub(cs.CapacityWaitSince.Time))
		}
		parts = append(parts, s)
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
func (w *deadlineWorld) classifyQuiet(st *deadlineState, full bool) (string, string) {
	key := st.key()
	var ways []string
	for _, mv := range w.releaseMoves(st, true) {
		child := w.apply(st, mv)
		if child.key() != key {
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
	_ = full
	return class, description
}

// deadlineReport is what one configuration's exploration found.
type deadlineReport struct {
	states, depth, runs, probes int
	findings                    map[string]deadlineFinding
	counts                      map[string]int
}

func newDeadlineReport() deadlineReport {
	return deadlineReport{findings: map[string]deadlineFinding{}, counts: map[string]int{}}
}

func (w *deadlineWorld) record(rep *deadlineReport, st *deadlineState, full bool) {
	for _, f := range w.check(st, full) {
		rep.counts[f.class]++
		if _, ok := rep.findings[f.class]; !ok {
			rep.findings[f.class] = f
		}
	}
}

// explore walks every path breadth-first to the bounds with a hash of
// visited states.
func (w *deadlineWorld) explore(rep *deadlineReport, maxStates, maxDepth int, full bool) {
	start := w.initial()
	w.settle(start)
	visited := map[string]bool{start.key(): true}
	queue := []*deadlineState{start}
	for len(queue) > 0 && rep.states < maxStates {
		st := queue[0]
		queue = queue[1:]
		rep.states++
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
		w.record(rep, st, full)
		if w.finished(st) {
			continue
		}
		if len(st.path) >= maxDepth {
			rep.noteDepth(st)
			continue
		}
		moves := append(w.progressMoves(st), w.releaseMoves(st, false)...)
		moves = append(moves, w.eventMoves(st, full)...)
		for _, mv := range moves {
			child := w.apply(st, mv)
			if k := child.key(); !visited[k] {
				visited[k] = true
				queue = append(queue, child)
			}
		}
	}
}

func (rep *deadlineReport) noteDepth(st *deadlineState) {
	rep.counts["depth"]++
	if _, ok := rep.findings["depth"]; !ok {
		rep.findings["depth"] = deadlineFinding{class: "depth", message: "the run did not end within the depth bound", state: st}
	}
}

// nextStraightMove is the world's next move when nothing goes wrong: the
// clock reaches the next moment, else the operator answers the hold with
// the verb it names.
func (w *deadlineWorld) nextStraightMove(st *deadlineState) (deadlineMove, bool) {
	if progress := w.progressMoves(st); len(progress) > 0 {
		return progress[0], true
	}
	if release := w.releaseMoves(st, false); len(release) > 0 {
		return release[0], true
	}
	return deadlineMove{}, false
}

// playOut plays a state to its end with no further event, recording every
// state it visits, and returns the end state; a play that reaches no end
// is reported under depth.
func (w *deadlineWorld) playOut(rep *deadlineReport, st *deadlineState, maxMoves int, full bool, visit func(*deadlineState)) *deadlineState {
	rep.runs++
	for depth := 0; ; depth++ {
		w.record(rep, st, full)
		if visit != nil {
			visit(st)
		}
		if w.finished(st) {
			return st
		}
		mv, ok := w.nextStraightMove(st)
		if !ok || depth >= maxMoves {
			rep.noteDepth(st)
			return st
		}
		st = w.apply(st, mv)
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
	}
}

// momentOf names the moment of a run a state stands at, for the probes: a
// step staging with its wait running, a step serving at a gate, the final
// step draining, the kept Instances rolling after the release, a stop.
// Empty for the moments the probes do not stop at.
func (w *deadlineWorld) momentOf(st *deadlineState) string {
	u := w.unit(st)
	if u == nil || u.record == nil {
		return ""
	}
	step := u.record.CurrentStep
	switch u.state {
	case rollout.CanaryStateStaging:
		if u.record.CapacityWaitSince != nil {
			return fmt.Sprintf("staging-%d", step)
		}
	case rollout.CanaryStateServing:
		return fmt.Sprintf("serving-%d", step)
	case rollout.CanaryStateDraining:
		if u.record.ObservedTrafficWeight == 100 {
			return "draining"
		}
	case rollout.CanaryStateDone:
		if u.phase == v1beta1.RolloutPhasePromoting {
			return "releasing"
		}
	case rollout.CanaryStateFailed:
		return fmt.Sprintf("failed-%d", step)
	}
	return ""
}

// probe plays the straight run once to collect its moments, then tries
// every event and release verb once at every moment and plays each to the
// end, so a finding carries the shortest story that reaches it.
func (w *deadlineWorld) probe(rep *deadlineReport, maxMoves int, full bool) {
	moments := map[string]*deadlineState{}
	var order []string
	start := w.initial()
	w.settle(start)
	w.playOut(rep, start, maxMoves, full, func(st *deadlineState) {
		if name := w.momentOf(st); name != "" {
			if _, seen := moments[name]; !seen {
				moments[name] = st
				order = append(order, name)
			}
		}
	})
	for _, name := range order {
		st := moments[name]
		for _, mv := range append(w.eventMoves(st, full), w.releaseMoves(st, false)...) {
			rep.probes++
			w.playOut(rep, w.apply(st, mv), maxMoves, full, nil)
		}
	}
}

// randomRuns plays whole runs with a fixed seed, favouring the moves that
// bring a run to its end, to reach the depths the breadth-first walk does
// not.
func (w *deadlineWorld) randomRuns(rep *deadlineReport, seed int64, runs, maxDepth int, full bool) {
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
			forward := append(w.progressMoves(st), w.releaseMoves(st, false)...)
			events := w.eventMoves(st, full)
			if len(forward) == 0 && len(events) == 0 {
				break
			}
			if depth >= maxDepth {
				rep.noteDepth(st)
				break
			}
			var mv deadlineMove
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

func deadlinePath(st *deadlineState) string {
	parts := make([]string, 0, len(st.path))
	for _, mv := range st.path {
		parts = append(parts, mv.String())
	}
	return strings.Join(parts, " -> ")
}

// The step lists under exploration: a four-step list of 10/30/60/100 with a
// short timed pause at each step but the last, the same with manual gates,
// and 25/50/100 and 50/100, so a step adds one to four Instances per side
// on the reference shapes.
func deadlineStepList(name string) []v1beta1.RolloutGroupStep {
	build := func(percents []int32, pause *v1beta1.RolloutPause) []v1beta1.RolloutGroupStep {
		var out []v1beta1.RolloutGroupStep
		for i, p := range percents {
			step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString(fmt.Sprintf("%d%%", p)), Traffic: p}
			if i < len(percents)-1 {
				step.Pause = pause
			}
			out = append(out, step)
		}
		return out
	}
	timed := &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 20 * time.Second}}
	manual := &v1beta1.RolloutPause{}
	switch name {
	case "four-timed":
		return build([]int32{10, 30, 60, 100}, timed)
	case "four-manual":
		return build([]int32{10, 30, 60, 100}, manual)
	case "three-timed":
		return build([]int32{25, 50, 100}, timed)
	case "two-manual":
		return build([]int32{50, 100}, manual)
	}
	panic("unknown step list " + name)
}

var deadlineStepNames = []string{"four-timed", "four-manual", "three-timed", "two-manual"}

// deadlineShapes are the reference prefill/decode shapes and the
// 8:8 shape, engine then decoder, each behind two routers.
var deadlineShapes = [][2]int32{{8, 8}, {12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}}

// deadlineGangs are the pods per Instance, engine and decoder: single pods
// and gangs of two to eight. The bounded mode runs the first three.
var deadlineGangs = [][2]int32{{1, 1}, {2, 2}, {4, 4}, {8, 8}, {8, 4}, {2, 8}}

// deadlineCaseTimeout is a short ready timeout.
const deadlineCaseTimeout = 90 * time.Second

func deadlineFullMode() bool { return os.Getenv(deadlineHuntEnv) == "full" }

// deadlineWorkers is how many configurations explore at once; the machine
// is shared, so the default stays small and the environment raises it.
func deadlineWorkers() int {
	if n, err := strconv.Atoi(os.Getenv(deadlineWorkersEnv)); err == nil && n > 0 {
		return n
	}
	return 4
}

func deadlineDefaultBudgets() deadlineBudgets {
	return deadlineBudgets{lostPods: 1, noCapacity: 1, pushes: 1, rollbacks: 1, pauses: 1, resumes: 1}
}

// deadlineConfigFor is one configuration: a shape behind two routers, the
// pods per Instance, one strategy at 10 percent, one step list, one ready
// timeout and one timing.
func deadlineConfigFor(shape, gang [2]int32, strategy v1beta1.UpdateStrategyType, stepsName string, timeout time.Duration, timing deadlineTiming) deadlineConfig {
	cfg := deadlineConfig{shape: shape, routers: 2, gang: gang, strategy: strategy, stepsName: stepsName, steps: deadlineStepList(stepsName),
		readyTimeout: timeout, timing: timing, budgets: deadlineDefaultBudgets()}
	cfg.name = cfg.String()
	return cfg
}

// deadlineConfigs lists the configurations to explore. The bounded run
// takes the 8:8 shape under both strategies and one configuration per
// reference shape, the strategies, step lists, gangs and timings cycling,
// with the chart's 15 minute timeout on one of them; the full run takes
// every shape under both strategies, every step list and every timing with
// the gangs cycling, the chart's timeout on the 8:8 shape, and a seeded
// sample over the whole space.
func deadlineConfigs(full bool) []deadlineConfig {
	strategies := []v1beta1.UpdateStrategyType{v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain}
	gangs := deadlineGangs[:3]
	if full {
		gangs = deadlineGangs
	}
	var out []deadlineConfig
	if !full {
		for _, strategy := range strategies {
			out = append(out, deadlineConfigFor(deadlineShapes[0], gangs[0], strategy, "four-timed", deadlineCaseTimeout, deadlineTimings[0]))
		}
		for si, shape := range deadlineShapes[1:] {
			out = append(out, deadlineConfigFor(shape, gangs[si%len(gangs)], strategies[si%2], deadlineStepNames[si%len(deadlineStepNames)], deadlineCaseTimeout, deadlineTimings[si%len(deadlineTimings)]))
		}
		out = append(out, deadlineConfigFor(deadlineShapes[0], gangs[0], strategies[0], "four-timed", cellReadyTimeout, deadlineTimings[0]))
		return out
	}
	n := 0
	for _, shape := range deadlineShapes {
		for _, strategy := range strategies {
			for _, steps := range deadlineStepNames {
				for _, timing := range deadlineTimings {
					out = append(out, deadlineConfigFor(shape, gangs[n%len(gangs)], strategy, steps, deadlineCaseTimeout, timing))
					n++
				}
			}
		}
	}
	for _, strategy := range strategies {
		out = append(out, deadlineConfigFor(deadlineShapes[0], gangs[0], strategy, "four-timed", cellReadyTimeout, deadlineTimings[0]))
	}
	rng := rand.New(rand.NewSource(20261110))
	for i := 0; i < 40; i++ {
		timeout := deadlineCaseTimeout
		if rng.Intn(4) == 0 {
			timeout = cellReadyTimeout
		}
		out = append(out, deadlineConfigFor(deadlineShapes[rng.Intn(len(deadlineShapes))], gangs[rng.Intn(len(gangs))], strategies[rng.Intn(2)],
			deadlineStepNames[rng.Intn(len(deadlineStepNames))], timeout, deadlineTimings[rng.Intn(len(deadlineTimings))]))
	}
	return out
}

// deadlineKnownClasses are the readings the explorer reports without
// failing, each in words: a finding kept visible until the behaviour changes,
// and pictures that are not holds.
var deadlineKnownClasses = map[string]string{
	"healthy-step-stopped-while-a-start-was-coming-up": "the step's ready timeout is measured from the pass that first read the step's capacity short, across the starts the batch step and each member's budget admit one after another (the decoder's Instance of a batch, then the engines once it serves); a step that needs more serial starts than fit in one ready timeout is stopped with CapacityTimeout while its last start is coming up, every pod healthy",
	"no-capacity-stopped":                              "a side whose starts toward the new revision could get no capacity since the wait began or the unit last made progress stops the step one ready timeout after that moment: the deadline's job, the capacity back late in the window included, since the engine cannot tell a start that waited on a fault from one that never comes",
	"one-sided-version-beyond-a-batch-after-an-event":  "the new version serves on one side only past one batch of that side after a pod loss, a push or a side that can get no capacity moved one side; the staging itself leaves no such picture",
	"start-slower-than-the-ready-timeout-stopped":      "a stop for capacity while every start coming up toward the new revision had been admitted a whole ready timeout or more before it: a start slower than the ready timeout (the slow node's gang of eight takes 95 seconds against 90), the deadline's job",
}

// deadlineFailingClasses are the readings that fail the test: a step
// stopped for capacity while a start admitted less than a ready timeout
// earlier was coming up (unless known) or with nothing coming up and no
// fault; a side that can get no capacity
// whose step is not stopped inside the deadline, or stopped late; a quiet
// state with no way out; a member past its budget; the new version on one
// side only past a batch on a straight run; a play that reaches no end; a
// controller that does not settle; an error inside the production code. A
// class in neither list is one nobody tracks yet and fails too.
var deadlineFailingClasses = map[string]bool{
	"healthy-step-stopped-while-a-start-was-coming-up": true, "healthy-step-stopped-with-nothing-coming-up": true,
	"no-capacity-not-stopped": true, "no-capacity-stopped-late": true, "no-way-out": true, "budget-exceeded": true,
	"one-sided-version-beyond-a-batch": true, "unsettled": true, "depth": true, "model-error": true,
}

// deadlineReportedClass reports a reading that ends a run or names a hold
// with a way out; those are counted and not failed.
func deadlineReportedClass(class string) bool {
	return strings.HasPrefix(class, "end-") || strings.HasPrefix(class, "hold-") || strings.HasPrefix(class, "stop-")
}

// deadlineResult is one configuration's exploration as its worker left it;
// a panic inside the world is its failure.
type deadlineResult struct {
	rep              deadlineReport
	passes, dispatch int
	failure          string
}

// deadlineRunConfigs explores every configuration side by side, workers at
// a time, and returns the results in configuration order.
func deadlineRunConfigs(t *testing.T, configs []deadlineConfig, full bool) []deadlineResult {
	maxStates, maxDepth, runs := 24, 80, 1
	if full {
		maxStates, maxDepth, runs = 60, 120, 3
	}
	results := make([]deadlineResult, len(configs))
	scheme := canaryScheme(t)
	lanes := make(chan struct{}, deadlineWorkers())
	var wg sync.WaitGroup
	for n := range configs {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			lanes <- struct{}{}
			defer func() { <-lanes }()
			w := newDeadlineWorld(t, configs[n], scheme)
			res := deadlineResult{rep: newDeadlineReport()}
			func() {
				defer func() {
					if r := recover(); r != nil {
						res.failure = fmt.Sprint(r)
					}
				}()
				w.probe(&res.rep, 4*maxDepth, full)
				w.explore(&res.rep, maxStates, maxDepth, full)
				w.randomRuns(&res.rep, 1, runs, 4*maxDepth, full)
			}()
			res.passes, res.dispatch = w.passes, w.dispatches
			results[n] = res
		}(n)
	}
	wg.Wait()
	return results
}

// deadlineOnly keeps the configurations OME_CANARY_DEADLINE_HUNT_ONLY names.
func deadlineOnly(configs []deadlineConfig) []deadlineConfig {
	only := os.Getenv(deadlineOnlyEnv)
	if only == "" {
		return configs
	}
	var kept []deadlineConfig
	for _, cfg := range configs {
		for _, part := range strings.Split(only, ",") {
			if part != "" && strings.Contains(cfg.name, part) {
				kept = append(kept, cfg)
				break
			}
		}
	}
	return kept
}

// deadlineExplore explores every configuration, probes every event at every
// moment, and reports each reading with the shortest path to it.
func deadlineExplore(t *testing.T, configs []deadlineConfig) {
	t.Helper()
	defer debug.SetGCPercent(debug.SetGCPercent(400))
	full := deadlineFullMode()
	configs = deadlineOnly(configs)
	started := time.Now()
	results := deadlineRunConfigs(t, configs, full)
	totals, configsWith := map[string]int{}, map[string]int{}
	type example struct {
		cfg     deadlineConfig
		finding deadlineFinding
	}
	examples := map[string][]example{}
	var states, passes, dispatches, runs, probes int
	for n, cfg := range configs {
		res := results[n]
		rep := res.rep
		if res.failure != "" {
			rep.counts["model-error"]++
			rep.findings["model-error"] = deadlineFinding{class: "model-error", message: res.failure, state: &deadlineState{isvc: deadlineISVC(cfg)}}
		}
		states, passes, dispatches, runs, probes = states+rep.states, passes+res.passes, dispatches+res.dispatch, runs+rep.runs, probes+rep.probes
		var parts []string
		for class, c := range rep.counts {
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: %d states, %d probes, depth %d, %d passes: %s", n+1, len(configs), cfg.name, rep.states, rep.probes, rep.depth, res.passes, strings.Join(parts, " "))
		for class, c := range rep.counts {
			totals[class] += c
		}
		for class, f := range rep.findings {
			configsWith[class]++
			if len(examples[class]) < 6 {
				examples[class] = append(examples[class], example{cfg: cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d probes, %d runs, %d controller passes, %d canary dispatches, %s", len(configs), states, probes, runs, passes, dispatches, time.Since(started).Round(time.Second))
	for _, class := range classes {
		known, isKnown := deadlineKnownClasses[class]
		if isKnown {
			t.Logf("%s: %d states across %d configurations: %s", class, totals[class], configsWith[class], known)
		} else {
			t.Logf("%s: %d states across %d configurations", class, totals[class], configsWith[class])
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg.name, ex.finding.message, deadlinePath(ex.finding.state), ex.finding.state)
		}
		if isKnown {
			continue
		}
		if deadlineFailingClasses[class] || !deadlineReportedClass(class) {
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg.name, examples[class][0].finding.message)
		}
	}
}

// TestCanaryCapacityDeadline_StepsOnAClockHoldOnlyForWantOfCapacity drives
// a prefill/decode canary behind two routers over the reference shapes and
// the 8:8 shape, single pods and gangs of two to eight, under both
// strategies at 10 percent, the step lists that add one to four Instances
// per side, a ready timeout of 90 seconds and the chart's 15
// minutes, under the default scheduler, the gang scheduler's placement
// wait and a slow node, with a pod lost mid-step, a side that can get no
// capacity, a corrected push at a stopped step, and the operator's pause,
// rollback, resume and promote. A step is stopped for capacity only when a
// side can get none or a start has had a whole ready timeout and not come
// up, and then inside one ready timeout of the wait's start, its last
// progress or its last start admitted.
func TestCanaryCapacityDeadline_StepsOnAClockHoldOnlyForWantOfCapacity(t *testing.T) {
	deadlineExplore(t, deadlineConfigs(deadlineFullMode()))
}

// deadlinePlayTo plays the straight run until the unit first stands at the
// named moment and returns that state.
func (w *deadlineWorld) playTo(moment string) *deadlineState {
	st := w.initial()
	w.settle(st)
	for depth := 0; depth < 400; depth++ {
		if w.momentOf(st) == moment {
			return st
		}
		mv, ok := w.nextStraightMove(st)
		if !ok {
			break
		}
		st = w.apply(st, mv)
	}
	w.t.Fatalf("the straight run never reached the moment %q: %s", moment, w.describe(st))
	return st
}

// playUntil plays the straight run from a state until it satisfies until,
// and returns that state.
func (w *deadlineWorld) playUntil(st *deadlineState, until func(*deadlineState) bool) *deadlineState {
	for depth := 0; depth < 400; depth++ {
		if until(st) {
			return st
		}
		mv, ok := w.nextStraightMove(st)
		if !ok {
			break
		}
		st = w.apply(st, mv)
	}
	w.t.Fatalf("the straight run never reached the picture asked for: %s", w.describe(st))
	return st
}

// TestCanaryCapacityDeadline_AStepOfFourSerialStartsIsStoppedWhileItsLastStartComesUp
// holds the current reading of the 8:8 shape. 8 prefill and 8 decode behind
// two routers under RecreatePod at 10 percent run a 10/30/60/100 canary
// with a ready timeout of 90 seconds; a pod takes 20 seconds to come up
// and the pass that reads it runs 5 seconds later. The second step adds
// two Instances per side, which the batch step admits one after another,
// the decoder's first: four starts, 95 seconds in all. The ready timeout
// runs from the pass that first read the step short, so the unit stops
// with CapacityTimeout at 91 seconds while the fourth start, opened 16
// seconds earlier, has 4 seconds to go, and every pod is healthy. The fix
// removes this test's reading and the known class with it.
func TestCanaryCapacityDeadline_AStepOfFourSerialStartsIsStoppedWhileItsLastStartComesUp(t *testing.T) {
	w := newDeadlineWorld(t, deadlineConfigFor(deadlineShapes[0], deadlineGangs[0], v1beta1.UpdateStrategyRecreatePod, "four-timed", deadlineCaseTimeout, deadlineTimings[0]), canaryScheme(t))
	st := w.playTo("staging-1")
	st = w.playUntil(st, func(st *deadlineState) bool {
		u := w.unit(st)
		return u.state == rollout.CanaryStateFailed || u.record.CurrentStep > 1
	})
	u := w.unit(st)
	if u.state != rollout.CanaryStateFailed || u.record.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout {
		t.Fatalf("current reading: the second step must stop with CapacityTimeout while its fourth start comes up; got %s", w.describe(st))
	}
	if len(st.stops) != 1 || st.stops[0].fault || st.stops[0].coming == 0 {
		t.Fatalf("the stop must be recorded with a start coming up and no fault in the world; got %+v", st.stops)
	}
	stop := st.stops[0]
	if since := stop.at.Sub(stop.since); since != deadlineCaseTimeout+time.Second {
		t.Fatalf("the stop must land one pass after the ready timeout from the first short pass, got %s", since)
	}
	t.Logf("%s; path: %s", w.readStopMessage(stop), deadlinePath(st))
}

func (w *deadlineWorld) readStopMessage(stop deadlineStop) string {
	_, message := w.readStop(stop)
	return message
}

// TestCanaryCapacityDeadline_AStepWhoseStartCanGetNoCapacityStopsInsideOneReadyTimeout
// holds the deadline's job. The case's shape under SurgeThenDrain, the
// second step staging; the decoder's starts toward the new revision can
// get no capacity from the moment the step opens. The engines wait for the
// decoder's Instance of the batch, nothing comes up, and the unit stops
// with CapacityTimeout one ready timeout after the wait began, within one
// pass.
func TestCanaryCapacityDeadline_AStepWhoseStartCanGetNoCapacityStopsInsideOneReadyTimeout(t *testing.T) {
	w := newDeadlineWorld(t, deadlineConfigFor(deadlineShapes[0], deadlineGangs[0], v1beta1.UpdateStrategySurgeThenDrain, "four-timed", deadlineCaseTimeout, deadlineTimings[0]), canaryScheme(t))
	st := w.playTo("staging-1")
	st = w.apply(st, deadlineMove{kind: "nocapacity", member: 1})
	st = w.playUntil(st, func(st *deadlineState) bool { return w.unit(st).state == rollout.CanaryStateFailed })
	if len(st.stops) != 1 || !st.stops[0].fault {
		t.Fatalf("the stop must be recorded under the fault; got %+v", st.stops)
	}
	stop := st.stops[0]
	if late := stop.at.Sub(stop.anchor()); late > deadlineCaseTimeout+w.cfg.timing.lag+2*time.Second {
		t.Fatalf("the stop must land inside one ready timeout and a pass of the wait's start, the unit's last progress or its last start admitted; got %s", late)
	}
	if stop.at.Sub(stop.since) < deadlineCaseTimeout {
		t.Fatalf("the stop must not land before one ready timeout has passed since the wait began; got %s", stop.at.Sub(stop.since))
	}
	t.Logf("%s; path: %s", w.readStopMessage(stop), deadlinePath(st))
}
