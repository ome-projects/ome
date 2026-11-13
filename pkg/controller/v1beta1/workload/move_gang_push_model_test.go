package workload_test

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the production engine over a move request on a gang
// Instance (one leader and its workers) while a push is in flight. It
// runs in the move model's world (move_model_test.go): one
// workload.Reconcile loop per Component on the recovery harness, the
// coordination gates wired between the Components, one pod per node, and
// the chart's lifecycle values. A gang rolls as a unit: under
// SurgeThenDrain the roll builds a whole new gang at a free index, takes
// the old gang out of rotation once the new one serves, removes it and
// frees its index; under RecreatePod the gang is rebuilt in place at its
// own index. A move binds to the Instance, not to an index: the record of
// a request still waiting when a gang surge rebuilds its Instance under
// another index follows it there, and a request whose Instance already
// left the node it names closes with nothing to move.
//
// The requests land on the gang the roll is replacing or rebuilding now,
// on one still waiting its turn, on one already moved, and on one the
// partition holds; naming the leader's node or a worker's; alone, with a
// member lost right after, and with a worker's node stopping right after;
// while the roll settles, rotates, drains, moves its last gangs, rests at
// a canary step, holds on a spent retry schedule, is paused, or absorbs a
// scale. The shapes are the reference gang shapes: two gangs of two
// under a surge of one, eight gangs of eight under RecreatePod and under
// a surge of two, three prefill and three decode gangs of eight under
// RecreatePod, one gang of four.
//
// What every probe must end with, beside the move model's readings: the
// record names the Instance the operator asked about at every pass,
// wherever the roll put it; a request closed with nothing to move leaves
// no pod of that Instance on the node it names; the Instance is built at
// most once between the request and the move's answer; no two attempts
// claim one index; the live gangs never exceed the count plus the
// larger of the surge budget and the one gang a move adds; a request that
// waited for the roll and then followed its Instance keeps its named
// wait, so the move's window is measured again from the roll's release.
//
// OME_MOVE_GANG_HUNT=full widens the configurations, the moments, the
// probes and the bounds; OME_MOVE_GANG_HUNT_ONLY=<text> keeps the
// configurations whose name carries the text.

const (
	moveGangFullEnv = "OME_MOVE_GANG_HUNT"
	moveGangOnlyEnv = "OME_MOVE_GANG_HUNT_ONLY"
	// gangTickBudgetBounded and gangTickBudgetFull bound the ticks one
	// configuration spends; the probes left when a budget is spent are
	// counted as skipped, so a run on a slower machine reads the same
	// first probes.
	gangTickBudgetBounded = 900
	gangTickBudgetFull    = 14000
	// gangWorkers bounds the configurations explored at once in full
	// mode, so a full run leaves the machine room for other work.
	gangWorkers = 16
	// gangWaitTicks is how long a probe lets a request stand before its
	// second verb lands, so the record has been read by at least one pass.
	gangWaitTicks = 2
	// gangFollowText is the text the engine writes on a record it rebinds
	// to the index a gang surge rebuilt its Instance under.
	gangFollowText = "following the Instance rebuilt as"
)

// gangVerb is what the operator does to the gang.
type gangVerb uint8

const (
	gangRequest           gangVerb = iota // the operator asks the gang off its leader's node
	gangRequestWorkerNode                 // the request names the node of one of the gang's workers
	gangRequestMemberLost                 // the request is filed, then one worker pod of the gang is lost
	gangRequestWorkerDown                 // the request is filed, then the kubelet under one worker stops for good
	gangSecondRequest                     // the request closes, then a second request names the same index
)

var gangVerbNames = [...]string{"request", "request-worker-node", "request-then-member-lost", "request-then-worker-node-lost", "request-then-second-request-on-the-index"}

func (v gangVerb) String() string { return gangVerbNames[v] }

// gangTarget picks the gang the request lands on.
type gangTarget uint8

const (
	gangOpen   gangTarget = iota // the gang the roll is replacing or rebuilding now
	gangBehind                   // the lowest gang still on the old version with no attempt open
	gangDone                     // the highest gang already on the new version
	gangHeld                     // the lowest gang the partition holds on the old version
)

var gangTargetNames = [...]string{"open", "behind", "done", "held"}

func (t gangTarget) String() string { return gangTargetNames[t] }

// gangProbe is one request on one gang at one moment.
type gangProbe struct {
	moment moveMoment
	verb   gangVerb
	side   int
	target gangTarget
}

func (p gangProbe) String() string {
	return fmt.Sprintf("%s at %s on side %d (%s)", p.verb, p.moment, p.side, p.target)
}

// gangConfig is the move model's configuration with the default
// scheduler's timing as an option: the workers of a new gang placed one
// tick after its leader.
type gangConfig struct {
	moveConfig
	staggered bool
}

func (c gangConfig) String() string {
	if c.staggered {
		return c.moveConfig.String() + "+staggered"
	}
	return c.moveConfig.String()
}

// gangRecordTrail is what this file remembers of one record across ticks.
type gangRecordTrail struct {
	// waited says the record has named the roll as what it waits for;
	// followed says the engine rebound it to a rebuilt Instance's index.
	waited, followed bool
	// windowRead says the window reading was taken at the allocation.
	windowRead bool
	// closedRead says the close was read.
	closedRead bool
}

// gangWorld is the move model's world with the gang verbs and the
// readings a request on a gang beside a push needs.
type gangWorld struct {
	*moveWorld
	gcfg gangConfig
	// gside, source, identity and fromNode are the probe's request: the
	// side, the index the request named, the Instance it stands for, and
	// the node it named.
	gside    *scaleSide
	source   int32
	identity int32
	fromNode string
	// second is the second request of the second-request verb, nil for
	// the other verbs.
	second *moveRequestFacts
	// trails is what this file remembers of every record, by request id.
	trails map[string]*gangRecordTrail
	// unnamedSince is the tick a record was first read with no text.
	unnamedSince map[string]int
	// passClock is the clock the last tick's passes ran at: the readings
	// run after the tick stepped the clock, and a record's times made
	// through the harness's Deps read the wall clock, not the world's.
	passClock time.Time
	// podsAtRequest names the pods the request's Instance had when the
	// request was filed; nodesSince names every node a member built
	// since then has stood on, leader or worker, by build number.
	podsAtRequest map[types.UID]bool
	nodesSince    map[string]int
	// lossVerb says the probe's verb loses a member or a worker's node
	// after the request, so the gang's repair rebuilds it once.
	lossVerb bool
	// unnamedScene is the scene last seen holding each record of this
	// probe while it stood with no text, by request id.
	unnamedScene map[string]string
	// servedBuilds names every build whose leader pod has been Ready: a
	// build the roll gave up before it served is an attempt, not a rebuild.
	servedBuilds map[types.UID]bool
}

func newGangWorld(t *testing.T, cfg gangConfig) *gangWorld {
	t.Helper()
	w := &gangWorld{moveWorld: newMoveWorld(t, cfg.moveConfig), gcfg: cfg}
	w.unnamedClass = w.unnamedWaitClass
	w.overdueClass = w.overdueWaitClass
	w.failedClass = w.failedRecordClass
	w.moreChecks = w.readTick
	w.resetGangTrails()
	return w
}

// resetGangTrails forgets what this file remembered of the last probe.
func (w *gangWorld) resetGangTrails() {
	w.gside, w.source, w.identity, w.fromNode, w.second = nil, 0, 0, "", nil
	w.trails = map[string]*gangRecordTrail{}
	w.unnamedSince = map[string]int{}
	w.podsAtRequest = map[types.UID]bool{}
	w.nodesSince = map[string]int{}
	w.lossVerb = false
	w.unnamedScene = map[string]string{}
	w.servedBuilds = map[types.UID]bool{}
}

// startGangProbe puts the world at a snapshot with every reading
// cleared, the move model's and this file's.
func (w *gangWorld) startGangProbe(snap *scaleSnapshot) {
	w.startProbe(snap)
	w.resetGangTrails()
	w.passClock = w.clk.Now()
}

// unnamedWaitClass names an unnamed wait by the scene holding the record
// now, or by the scene that held it while it stood with no text: a wait
// whose scene ends on the tick the reading counts it is still named by
// what held it. Any other unnamed wait is the move model's to name.
func (w *gangWorld) unnamedWaitClass(s *scaleSide, rec workloadtypes.MigrationRecord) string {
	if scene := w.liveUnnamedScene(s, rec); scene != "" {
		return scene
	}
	return w.unnamedScene[rec.RequestUUID]
}

// liveUnnamedScene names what holds a record with no text on this pass:
// the roll rebuilding its gang in place, a repair rebuilding its gang
// (the drive defers a source mid-update or mid-repair without a word,
// since the attempt in flight ends by itself, and a gang's rebuild takes
// as long as its members take to come up), or a move already in flight
// on the same Instance, behind which a second request stands with no
// text while no surge of the roll is in flight.
func (w *gangWorld) liveUnnamedScene(s *scaleSide, rec workloadtypes.MigrationRecord) string {
	if w.podsPerInstance(s) < 2 {
		return ""
	}
	if row := s.h.instance(rec.SourceInstance); row != nil && row.Operation != nil {
		switch row.Operation.Type {
		case workloadtypes.InstanceOperationUpdate:
			if row.Operation.Step == workloadtypes.UpdateStepDrain || row.Operation.Step == workloadtypes.UpdateStepInPlace {
				return "move-waits-unnamed-while-the-roll-rebuilds-its-gang-in-place"
			}
		case workloadtypes.InstanceOperationRestart:
			return "move-waits-unnamed-while-the-repair-rebuilds-its-gang"
		}
	}
	if f := w.factsOf(rec.RequestUUID); f != nil && f.surgeInFlight {
		return ""
	}
	for i := range s.h.migrations {
		other := &s.h.migrations[i]
		if other.RequestUUID != rec.RequestUUID && !other.Phase.Terminal() && other.SurgeAllocated() &&
			w.identityOf(s, other.SourceInstance) == w.identityOf(s, rec.SourceInstance) {
			return "move-waits-unnamed-behind-the-move-in-flight-on-its-instance"
		}
	}
	return ""
}

// rememberUnnamedScenes keeps, for every record of this probe standing
// open with no text, the scene holding it, and forgets it once the record
// carries a text, starts or closes.
func (w *gangWorld) rememberUnnamedScenes() {
	for _, req := range w.filed {
		s := req.side
		rec := s.h.migration(req.uuid)
		if rec == nil || rec.Phase.Terminal() || rec.SurgeAllocated() || rec.Message != "" {
			delete(w.unnamedScene, req.uuid)
			continue
		}
		if scene := w.liveUnnamedScene(s, *rec); scene != "" {
			w.unnamedScene[req.uuid] = scene
		}
	}
}

// overdueWaitClass names a record past the deadline it was accepted with
// while its text names the roll: that deadline stands still for the wait
// and is measured again once no surge of the roll is in flight.
func (w *gangWorld) overdueWaitClass(_ *scaleSide, rec workloadtypes.MigrationRecord) string {
	if !rec.SurgeAllocated() && moveWaitsForTheRoll(&rec) {
		return "move-past-its-accept-deadline-while-waiting-for-the-roll"
	}
	return ""
}

// failedRecordClass names a request on a gang refused as a single pod
// that spans nodes while the side has more Instances than it wants: a
// gang the scale-down is removing is out of the plan, so the move reads
// its shape as one pod.
func (w *gangWorld) failedRecordClass(s *scaleSide, rec workloadtypes.MigrationRecord) string {
	if w.podsPerInstance(s) > 1 && int32(len(w.rows(s))) > s.h.replicas && strings.Contains(rec.Message, "source pods span multiple nodes") {
		return "move-on-a-gang-the-scale-down-removes-refused-as-spanning-nodes"
	}
	return ""
}

// readServedBuilds remembers every build whose leader pod has been Ready.
func (w *gangWorld) readServedBuilds() {
	s := w.gside
	if s == nil {
		return
	}
	for _, pod := range w.pods(s) {
		if !query.RoutedRunner(pod) || w.servedBuilds[pod.UID] {
			continue
		}
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				w.servedBuilds[pod.UID] = true
			}
		}
	}
}

// memberPendingOnAStoppedNode reports a pod of the probe's side bound to
// a node whose kubelet stopped before the pod ran.
func (w *gangWorld) memberPendingOnAStoppedNode() bool {
	s := w.gside
	if s == nil {
		return false
	}
	for _, pod := range s.h.livePods() {
		if pod.Spec.NodeName != "" && s.h.deadNodes[pod.Spec.NodeName] && pod.Status.Phase != corev1.PodRunning {
			return true
		}
	}
	return false
}

// stagger applies the default scheduler's timing when the configuration
// asks for it: the workers of a new gang are placed one tick after its
// leader.
func (w *gangWorld) stagger() {
	if w.gcfg.staggered {
		staggerWorkerPlacement(w.scaleWorld)
	}
}

// readTick runs this file's readings after every pass of every side: the
// scheduler timing, the record's Instance, the index claims, the gangs
// with live pods against the ceiling, and the window of a record that followed
// its Instance.
func (w *gangWorld) readTick() {
	w.stagger()
	passClock := w.passClock
	w.passClock = w.clk.Now()
	if len(w.filed) == 0 {
		return
	}
	w.readRecordInstances()
	w.readIndexClaims()
	w.readCeiling()
	w.readWindows(passClock)
	w.readMemberNodes()
	w.readServedBuilds()
	w.rememberUnnamedScenes()
}

// readMemberNodes records the node of every member of the request's
// Instance built since the request, leader or worker, with the side's
// build number at the time: a request may name a worker's node, and a
// rebuilt gang may put a worker back on it while its leader lands off it.
func (w *gangWorld) readMemberNodes() {
	s := w.gside
	if s == nil {
		return
	}
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || w.identityOf(s, idx) != w.identity || w.podsAtRequest[pod.UID] || pod.Spec.NodeName == "" {
			continue
		}
		if _, seen := w.nodesSince[pod.Spec.NodeName]; !seen {
			w.nodesSince[pod.Spec.NodeName] = w.seq[s]
		}
	}
}

// readRecordInstances reads every record this probe filed against the
// Instance the operator asked about: the record names that Instance's
// index at every pass, wherever the roll put it, until it closes; a
// record that names an index another Instance holds would move the wrong
// gang.
func (w *gangWorld) readRecordInstances() {
	for _, req := range w.filed {
		s := req.side
		rec := s.h.migration(req.uuid)
		if rec == nil {
			continue
		}
		trail := w.trailOf(req.uuid)
		if rec.Phase.Terminal() {
			if trail.closedRead {
				continue
			}
			trail.closedRead = true
			w.readUnmovedCloseAgainstTheNode(req, rec)
		}
		if rec.SurgeAllocated() && rec.Phase.Terminal() {
			// The source row is gone once the move completed; the
			// replacement's index stands for the Instance.
			continue
		}
		if id := w.identityOf(s, rec.SourceInstance); id != req.identity {
			w.note("move-record-names-another-instance", "the move %s was filed for %s Instance %d; its record names index %d, which stands for another Instance (%d): %s", req.uuid, s.comp, req.source, rec.SourceInstance, id, w.picture())
		}
	}
}

// trailOf is this file's trail of a record, made on first sight.
func (w *gangWorld) trailOf(uuid string) *gangRecordTrail {
	trail := w.trails[uuid]
	if trail == nil {
		trail = &gangRecordTrail{}
		w.trails[uuid] = trail
	}
	return trail
}

// readUnmovedCloseAgainstTheNode reads a close with nothing to move on
// the pass it lands: a live pod of the Instance the request was filed
// for still on the node it named is a request closed unsatisfied, even
// when a later rebuild moves the pod off before the world settles.
func (w *gangWorld) readUnmovedCloseAgainstTheNode(req *moveRequestFacts, rec *workloadtypes.MigrationRecord) {
	if rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance != nil || req.fromNode == "" {
		return
	}
	s := req.side
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if ok && w.identityOf(s, idx) == req.identity && pod.DeletionTimestamp == nil && pod.Spec.NodeName == req.fromNode {
			w.note("already-left-with-a-pod-still-on-the-node", "the move %s closed Completed (%s) on this pass and %s Instance %d, the one it was filed for, still runs pod %s on %s, the node it was asked to leave: %s", rec.RequestUUID, rec.Message, s.comp, idx, pod.Name, req.fromNode, w.picture())
		}
	}
}

// readIndexClaims reads every side's rows for an index two rows share or
// a replacement index two attempts claim: a gang surge and a move both
// take the lowest free index, and a claim both make would build two gangs
// under one name.
func (w *gangWorld) readIndexClaims() {
	for _, s := range w.sides {
		rows := map[int32]bool{}
		claims := map[int32]int32{}
		for _, r := range w.rows(s) {
			if rows[r.Index] {
				w.note("two-rows-at-one-index", "%s has two rows at index %d: %s", s.comp, r.Index, w.picture())
			}
			rows[r.Index] = true
			op := r.Operation
			if op == nil || op.SurgeIndex == nil || *op.SurgeIndex == r.Index {
				continue
			}
			if op.Type == workloadtypes.InstanceOperationUpdate && (op.Step == workloadtypes.UpdateStepGangSurgeTarget || op.Step == workloadtypes.UpdateStepGangSurgeTargetCleanup) {
				continue
			}
			if op.Type == workloadtypes.InstanceOperationMigrate && r.Phase != workloadtypes.InstancePhaseMigrating {
				continue
			}
			if other, taken := claims[*op.SurgeIndex]; taken {
				w.note("surge-index-claimed-twice", "%s rows %d and %d both claim index %d as their replacement: %s", s.comp, other, r.Index, *op.SurgeIndex, w.picture())
			}
			claims[*op.SurgeIndex] = r.Index
		}
	}
}

// readCeiling reads the live gangs on a side against the count plus the
// larger of the surge budget and the one gang a move adds: the roll's
// surges wait for a move in flight and a move waits for the roll's
// surges, so they never stack. A side a scale moved is the scaling
// model's to read.
func (w *gangWorld) readCeiling() {
	if w.scaled {
		return
	}
	for _, s := range w.sides {
		live := map[int32]bool{}
		for _, pod := range w.pods(s) {
			if idx, ok := query.InstanceIdxFromLabels(pod); ok && pod.DeletionTimestamp == nil {
				live[idx] = true
			}
		}
		extra := int32(1)
		if s.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
			if budget := escalation.PerComponentMaxSurgeBudget(s.h.lifecycle.UpdateStrategy.RollingUpdate, s.h.replicas); budget != escalation.BudgetNoLimit && budget > extra {
				extra = budget
			}
		}
		if int32(len(live)) > s.h.replicas+extra {
			w.note("gangs-over-the-ceiling", "%s has %d Instances with live pods against %d wanted and %d allowed beside them: %s", s.comp, len(live), s.h.replicas, extra, w.picture())
		}
	}
}

// readWindows reads every record this probe filed for its wait and its
// window: a record that named the roll as what it waits for and was
// then rebound to its rebuilt Instance's index has its window measured
// again when the move starts, as the wait promised; a record closed
// Failed at its deadline after such a wait expired on a window the wait
// spent.
func (w *gangWorld) readWindows(passClock time.Time) {
	for _, req := range w.filed {
		s := req.side
		rec := s.h.migration(req.uuid)
		if rec == nil {
			continue
		}
		trail := w.trailOf(req.uuid)
		if moveWaitsForTheRoll(rec) {
			trail.waited = true
		}
		if strings.Contains(rec.Message, gangFollowText) {
			trail.followed = true
		}
		window := s.h.lifecycle.InstanceReadyTimeout.Duration
		switch {
		case !trail.windowRead && rec.SurgeAllocated() && !rec.Deadline.IsZero() && !passClock.IsZero():
			trail.windowRead = true
			if short := window - rec.Deadline.Sub(passClock); trail.waited && trail.followed && short > 0 {
				w.note("move-starts-with-the-window-the-rolls-wait-spent", "the move %s waited for the roll, followed its Instance to index %d, and started with %s of its %s window already spent: %s", req.uuid, rec.SourceInstance, short.Round(time.Second), window, w.picture())
			}
		case rec.Phase == workloadtypes.MigrationPhaseFailed && trail.waited && strings.Contains(rec.Message, "while "+gangFollowText):
			w.note("move-expired-after-a-named-wait-while-following-the-gang-surge", "the move %s named its wait for the roll, followed its Instance, and was closed Failed at a deadline the wait had spent: %s: %s", req.uuid, rec.Message, w.picture())
		}
	}
}

// pickGang is the gang a probe's target names on a side, or false when
// the side has none: the move model's picks, with the open gang read
// through the attempt the roll has on it.
func (w *gangWorld) pickGang(s *scaleSide, target gangTarget) (int32, bool) {
	switch target {
	case gangBehind:
		return w.pick(s, targetBehind)
	case gangDone:
		return w.pick(s, targetDone)
	case gangHeld:
		return w.pick(s, targetHeld)
	}
	return w.pick(s, targetOpen)
}

// workerPods lists the live worker pods of an Instance, by name.
func (w *gangWorld) workerPods(s *scaleSide, idx int32) []*corev1.Pod {
	var out []*corev1.Pod
	for _, pod := range s.h.podsOf(idx) {
		if pod.Labels[query.LabelRunner] == "worker" && pod.DeletionTimestamp == nil {
			out = append(out, pod)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// land plays the probe on the world: the request, the node it names, and
// the second verb after it. It reports false when no gang fits the
// probe's target.
func (w *gangWorld) land(p gangProbe) bool {
	s := w.sides[p.side]
	idx, ok := w.pickGang(s, p.target)
	if !ok {
		return false
	}
	w.path = append(w.path, fmt.Sprintf("%s on Instance %d of %s", p.verb, idx, s.comp))
	if w.landed == 0 {
		w.markBuilds()
	}
	w.landed++
	w.probe, w.probeTick = moveProbe{moment: p.moment, verb: verbRequest, side: p.side, target: targetBehind}, w.mticks
	w.backoffAtMove = w.targetInBackoff(s)
	w.gside, w.source, w.identity = s, idx, w.identityOf(s, idx)
	w.lossVerb = p.verb == gangRequestMemberLost || p.verb == gangRequestWorkerDown
	for _, pod := range s.h.podsOf(idx) {
		w.podsAtRequest[pod.UID] = true
	}
	workers := w.workerPods(s, idx)
	uuid := w.fileMoveRequest(s, idx)
	w.fromNode = w.request.fromNode
	if p.verb == gangRequestWorkerNode && len(workers) > 0 {
		w.nameNode(uuid, workers[len(workers)-1].Spec.NodeName)
	}
	switch p.verb {
	case gangRequestMemberLost:
		w.waitTicks(gangWaitTicks)
		w.loseMember(s, idx)
	case gangRequestWorkerDown:
		w.waitTicks(gangWaitTicks)
		w.loseWorkerNode(s, idx)
	}
	if p.moment == whenIdle {
		// With nothing under way the push is what lands beside the
		// request: the roll must go on around it.
		w.push(fixedImage)
		w.request.pushedAfter = true
	}
	return true
}

// nameNode names node on the request as the node the operator saw the
// gang on: a worker's node, when the operator means a worker.
func (w *gangWorld) nameNode(uuid, node string) {
	if rec := w.gside.h.migration(uuid); rec != nil {
		rec.FromNode = node
	}
	w.request.fromNode = node
	w.fromNode = node
	w.path = append(w.path, "the request names "+node)
}

// waitTicks ticks n times, so a request stands through at least one pass.
func (w *gangWorld) waitTicks(n int) {
	for i := 0; i < n; i++ {
		w.tick()
	}
}

// loseMember deletes one worker pod of the gang at idx, as a node
// reboot or an eviction would: the gang's restart policy rebuilds the
// whole gang. The Instance counts as one a verb took the pods of.
func (w *gangWorld) loseMember(s *scaleSide, idx int32) {
	cur := w.indexOf(s, w.identityOf(s, idx))
	workers := w.workerPods(s, cur)
	if len(workers) == 0 {
		return
	}
	w.takePodsOf(s, cur)
	if err := s.h.c.Delete(s.h.ctx, workers[0]); err != nil {
		w.t.Fatalf("delete worker pod %s: %v", workers[0].Name, err)
	}
	w.path = append(w.path, "worker pod "+workers[0].Name+" is lost")
}

// loseWorkerNode stops the kubelet under one worker of the gang at idx
// for good: the force-delete policy, when set, frees the pod at its
// threshold and the gang is rebuilt. A fresh node joins for the one lost.
func (w *gangWorld) loseWorkerNode(s *scaleSide, idx int32) {
	cur := w.indexOf(s, w.identityOf(s, idx))
	workers := w.workerPods(s, cur)
	if len(workers) == 0 || workers[0].Spec.NodeName == "" {
		return
	}
	node := workers[0].Spec.NodeName
	w.takePodsOf(s, cur)
	w.lostNodes[s][node] = true
	s.h.failNode(node)
	s.h.addNodes(fmt.Sprintf("%s-node-%d", s.comp, len(s.h.nodeNames)))
	w.path = append(w.path, "the kubelet under worker node "+node+" stops")
}

// takePodsOf records the Instance at idx as one a verb took the pods of,
// as the move model does for a loss: a replacement already standing
// elsewhere makes the loss a touch, not a loss.
func (w *gangWorld) takePodsOf(s *scaleSide, idx int32) {
	if w.replacementElsewhere(s, idx) {
		w.touched[s][w.identityOf(s, idx)] = true
		return
	}
	w.markMoved(s, idx)
}

// fileSecondRequest files a second request naming the index the first
// request named, as an operator who read the first request's index off
// the first record would: the Instance at that index now is whatever the
// roll put there.
func (w *gangWorld) fileSecondRequest() {
	s := w.gside
	row := s.h.instance(w.source)
	if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil {
		return
	}
	w.fileMoveRequest(s, w.source)
	w.second = w.request
	w.path = append(w.path, fmt.Sprintf("a second request names index %d", w.source))
}

// play ticks the probe to its settled end; the second-request verb files
// its second request once the first has closed and plays out again. It
// reports whether the world settled.
func (w *gangWorld) play(p gangProbe) bool {
	settled := w.playOut()
	if settled && w.gside != nil && !w.sideComplete(w.gside) && w.memberPendingOnAStoppedNode() {
		// The roll's surge waits its placement grace on a member bound
		// to a node whose kubelet stopped, then retires the attempt; the
		// probe waits that out once.
		w.path = append(w.path, "the surge's placement grace is waited out")
		settled = w.playOut()
	}
	if p.verb != gangSecondRequest || !settled {
		return settled
	}
	w.fileSecondRequest()
	if w.second == nil {
		return settled
	}
	return w.playOut()
}

// finishProbe reads a probe's end: the move model's readings, then this
// file's.
func (w *gangWorld) finishProbe(settled bool) {
	w.classifyProbe(settled)
	w.readGang(settled)
}

// readGang reads the settled end of a probe: the builds of the Instance
// since the request, where the Instance stands against the node the
// request names, who holds that node now, and the second request's end.
func (w *gangWorld) readGang(settled bool) {
	if !settled || w.gside == nil || len(w.filed) == 0 {
		return
	}
	first := w.filed[0]
	rec := first.side.h.migration(first.uuid)
	if rec == nil {
		return
	}
	w.readBuildsSinceTheRequest(first, rec)
	w.readWhereTheGangStands(first, rec)
	if w.second != nil {
		w.readSecondRequest()
	}
}

// readBuildsSinceTheRequest counts the builds of the Instance the request
// named between the request and the move's own replacement, or the close
// of a request answered without a move, by the node each landed on. One
// build the roll made off the node answers the request; one build on the
// node the request names, moved again by the move, is two builds for one
// request; more is a finding of its own.
func (w *gangWorld) readBuildsSinceTheRequest(req *moveRequestFacts, rec *workloadtypes.MigrationRecord) {
	if rec.Phase != workloadtypes.MigrationPhaseCompleted {
		return
	}
	s := req.side
	var since []moveBuild
	for _, b := range w.buildsBetween(s, req.identity, req.seqAt, 0) {
		if w.servedBuilds[b.uid] {
			since = append(since, b)
		}
	}
	if rec.SurgeInstance == nil {
		trail := w.records[rec.RequestUUID]
		var before []moveBuild
		for _, b := range since {
			if trail == nil || b.seq <= trail.completedSeq {
				before = append(before, b)
			}
		}
		before, repair := w.withoutTheRepairsRebuild(req, before)
		switch {
		case len(before) == 0 && repair != nil && w.memberLandedOn(w.fromNode, repair.seq):
			w.note("rebuilt-on-the-node-and-the-move-closed-satisfied", "the repair rebuilt %s Instance %d on %s, the node the move %s names, and the move still closed as already left: %s", s.comp, req.source, repair.node, rec.RequestUUID, w.picture())
		case len(before) == 0 && repair != nil:
			w.note("repaired-after-the-loss-off-the-node-and-the-move-closed-satisfied", "the repair rebuilt %s Instance %d once after the loss, at its index off %s, and the move %s closed as already left", s.comp, req.source, w.fromNode, rec.RequestUUID)
		case len(before) == 0:
			w.note("answered-without-a-build-since-the-request", "the move %s closed as already left with no build of %s Instance %d since the request: %s", rec.RequestUUID, s.comp, req.source, rec.Message)
		case len(before) == 1 && repair != nil && !w.memberLandedOn(w.fromNode, before[0].seq):
			w.note("repaired-after-the-loss-then-rebuilt-off-the-node-and-the-move-closed-satisfied", "the repair rebuilt %s Instance %d after the loss and the roll rebuilt it once more, at index %d off %s, and the move %s closed as already left", s.comp, req.source, before[0].index, w.fromNode, rec.RequestUUID)
		case len(before) == 1 && !w.memberLandedOn(w.fromNode, before[0].seq):
			w.note("rebuilt-off-the-node-and-the-move-closed-satisfied", "the roll rebuilt %s Instance %d once since the move %s was filed, at index %d off %s, and the move closed as already left", s.comp, req.source, rec.RequestUUID, before[0].index, w.fromNode)
		case len(before) == 1:
			w.note("rebuilt-on-the-node-and-the-move-closed-satisfied", "the roll rebuilt %s Instance %d on %s, the node the move %s names, and the move still closed as already left: %s", s.comp, req.source, before[0].node, rec.RequestUUID, w.picture())
		default:
			w.note("rebuilt-more-than-once-before-the-move-closed", "%s Instance %d was built %d times between the move %s and its close (%s): %s", s.comp, req.source, len(before), rec.RequestUUID, buildNodes(before), w.picture())
		}
		return
	}
	own := w.ownBuildSeq(req)
	if own == 0 {
		return
	}
	var before []moveBuild
	for _, b := range since {
		if b.seq < own {
			before = append(before, b)
		}
	}
	switch {
	case len(before) == 0:
		w.note("built-once-since-the-request", "%s Instance %d was built once since the move %s was filed, by the move, at index %d", s.comp, req.source, rec.RequestUUID, *rec.SurgeInstance)
	case len(before) == 1 && w.memberLandedOn(w.fromNode, own):
		w.note("rebuilt-on-the-requests-node-then-moved-again", "the roll or the repair rebuilt %s Instance %d on %s, the node the move %s names, and the move built it again at index %d: 2 builds for one request (%s)", s.comp, req.source, before[0].node, rec.RequestUUID, *rec.SurgeInstance, buildNodes(before))
	case len(before) == 1:
		w.note("moved-again-after-a-rebuild-off-the-requests-node", "%s Instance %d was rebuilt on %s, off %s, and the move %s still built it again at index %d: 2 builds for one request (%s): %s", s.comp, req.source, before[0].node, w.fromNode, rec.RequestUUID, *rec.SurgeInstance, buildNodes(before), w.picture())
	default:
		w.note("rebuilt-more-than-once-before-the-move", "%s Instance %d was built %d times between the move %s and its replacement (%s): %s", s.comp, req.source, len(before), rec.RequestUUID, buildNodes(before), w.picture())
	}
}

// withoutTheRepairsRebuild splits off the first build when the probe's
// verb lost a member and the gang's repair rebuilt it at its own index on
// the revision it ran: that build answers the loss, not the request.
func (w *gangWorld) withoutTheRepairsRebuild(req *moveRequestFacts, builds []moveBuild) ([]moveBuild, *moveBuild) {
	if !w.lossVerb || len(builds) == 0 || builds[0].index != req.source || builds[0].revision != req.sourceRev {
		return builds, nil
	}
	repair := builds[0]
	return builds[1:], &repair
}

// memberLandedOn reports a member of the request's Instance, built since
// the request and before build number until, standing on node.
func (w *gangWorld) memberLandedOn(node string, until int) bool {
	seq, seen := w.nodesSince[node]
	return seen && seq <= until
}

// buildNodes describes builds as index:revision@node in the order built.
func buildNodes(builds []moveBuild) string {
	var parts []string
	for _, b := range builds {
		parts = append(parts, fmt.Sprintf("%d:%s@%s", b.index, b.revision, b.node))
	}
	return strings.Join(parts, " ")
}

// readWhereTheGangStands reads a completed request against the node it
// names: a live pod of the Instance the operator asked about still there
// is a request closed unsatisfied; a pod of another Instance there is the
// roll's own placement, which a request binds nothing against.
func (w *gangWorld) readWhereTheGangStands(req *moveRequestFacts, rec *workloadtypes.MigrationRecord) {
	if rec.Phase != workloadtypes.MigrationPhaseCompleted || w.fromNode == "" {
		return
	}
	s := req.side
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || pod.DeletionTimestamp != nil || pod.Spec.NodeName != w.fromNode {
			continue
		}
		if w.identityOf(s, idx) == req.identity {
			// A moved Instance's pod on the node is the move model's
			// reading of the replacement; an unmoved one is this file's.
			if rec.SurgeInstance == nil {
				w.note("already-left-with-a-pod-still-on-the-node", "the move %s closed Completed (%s) and %s Instance %d, the one it was filed for, still runs pod %s on %s, the node it was asked to leave: %s", rec.RequestUUID, rec.Message, s.comp, idx, pod.Name, w.fromNode, w.picture())
			}
			continue
		}
		class := "another-instance-landed-on-the-moves-node"
		if idx == req.source {
			class = "the-next-gang-took-the-requests-index-and-its-node"
		}
		w.note(class, "the move %s closed Completed (%s); %s Instance %d, another gang, runs pod %s on %s, the node the request named: %s", rec.RequestUUID, rec.Message, s.comp, idx, pod.Name, w.fromNode, w.picture())
	}
}

// readSecondRequest reads the second request's end: it named an index,
// and the Instance at that index now is what it moved or found already
// off the node.
func (w *gangWorld) readSecondRequest() {
	req := w.second
	rec := req.side.h.migration(req.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted {
		return
	}
	same := req.identity == w.identity
	switch {
	case rec.SurgeInstance == nil && same:
		w.note("second-request-on-the-same-instance-closed-already-left", "the second request %s named index %d, still the first request's Instance, and closed as already left: %s", req.uuid, req.source, rec.Message)
	case rec.SurgeInstance == nil:
		w.note("second-request-on-the-reused-index-closed-already-left", "the second request %s named index %d, now another Instance's, and closed as already left: %s", req.uuid, req.source, rec.Message)
	case same:
		w.note("second-request-moved-the-same-instance-again", "the second request %s named index %d, still the first request's Instance, and moved it to index %d", req.uuid, req.source, *rec.SurgeInstance)
	default:
		w.note("second-request-moved-the-instance-now-at-the-index", "the second request %s named index %d, now another Instance's, and moved that Instance to index %d: a request names an index, and the Instance under it may have changed", req.uuid, req.source, *rec.SurgeInstance)
	}
}

// gangMoments lists the moments a probe may land at, in the order the
// explore plays them.
var gangMoments = []moveMoment{whenIdle, whenSettling, whenRotating, whenDraining, whenLast, whenCanaryStep, whenHeld, whenPaused, whenScalingUp, whenScalingDown}

// gangShape is one probe shape: the verb and the gang it lands on.
type gangShape struct {
	verb   gangVerb
	target gangTarget
}

// boundedGangShapes keeps, per moment, the probes operators meet most,
// so the bounded run stays inside its time: the request on
// the gang the roll is replacing now, the one it reaches next, the one
// already moved; a member lost and a worker named beside the roll.
var boundedGangShapes = map[moveMoment][]gangShape{
	whenIdle:     {{gangRequest, gangDone}, {gangRequestMemberLost, gangDone}},
	whenSettling: {{gangRequest, gangOpen}, {gangRequest, gangBehind}},
	whenRotating: {{gangRequest, gangOpen}, {gangRequest, gangDone}, {gangRequestWorkerNode, gangOpen}, {gangRequestMemberLost, gangBehind}, {gangRequestWorkerDown, gangBehind}, {gangSecondRequest, gangOpen}},
	whenDraining: {{gangRequest, gangOpen}},
	whenLast:     {{gangRequest, gangOpen}, {gangRequestWorkerNode, gangOpen}},
	whenPaused:   {{gangRequest, gangBehind}},
}

// probesAt lists the probes tried from a moment: the bounded table on the
// first side, the decode side's request on the gang the roll is replacing
// while it rotates, plus the held gang where a partition stands; in full
// mode every verb on every target on both sides.
func (w *gangWorld) probesAt(m moveMoment, full bool) []gangProbe {
	if !full {
		var out []gangProbe
		for _, shape := range boundedGangShapes[m] {
			out = append(out, gangProbe{moment: m, verb: shape.verb, side: 0, target: shape.target})
		}
		if d := w.decodeSideIndex(); d >= 0 && m == whenRotating {
			out = append(out, gangProbe{moment: m, verb: gangRequest, side: d, target: gangOpen})
		}
		if w.cfg.settings.partition > 0 && m == whenRotating {
			out = append(out, gangProbe{moment: m, verb: gangRequest, side: 0, target: gangHeld})
		}
		return out
	}
	sides := []int{0}
	if d := w.decodeSideIndex(); d >= 0 {
		sides = append(sides, d)
	}
	targets := []gangTarget{gangOpen, gangBehind, gangDone}
	if w.cfg.settings.partition > 0 {
		targets = append(targets, gangHeld)
	}
	var out []gangProbe
	for _, side := range sides {
		for verb := gangRequest; verb <= gangSecondRequest; verb++ {
			for _, target := range targets {
				out = append(out, gangProbe{moment: m, verb: verb, side: side, target: target})
			}
		}
	}
	return out
}

// decodeSideIndex is the index of the decode side among the sides, -1
// when the shape has none.
func (w *gangWorld) decodeSideIndex() int {
	for i, s := range w.sides {
		if s.comp == workloadtypes.ComponentDecoder {
			return i
		}
	}
	return -1
}

// explore runs one configuration: the moments kept, every probe from
// every moment played to its end, then the random runs.
func (w *gangWorld) explore(full bool, seed int64, runs int) {
	w.t.Helper()
	budget := gangTickBudgetBounded
	if full {
		budget = gangTickBudgetFull
	}
	w.reachIdle()
	snaps := w.momentSnapshots(full)
	for _, m := range gangMoments {
		snap := snaps[m]
		if snap == nil {
			continue
		}
		for _, p := range w.probesAt(m, full) {
			if w.mticks >= budget {
				w.skipped++
				continue
			}
			w.startGangProbe(snap)
			w.path = append(w.path, "at "+m.String())
			if !w.land(p) {
				continue
			}
			w.probes++
			settled := w.play(p)
			if m == whenPaused && settled {
				w.setPaused(false)
				w.path = append(w.path, "resume")
				settled = w.playOut()
			}
			w.finishProbe(settled)
		}
	}
	w.randomRuns(snaps[whenIdle], seed, runs, budget)
}

// randomRuns plays the roll from idle and lands one probe of a random
// shape at a random tick of it, under a fixed seed.
func (w *gangWorld) randomRuns(idle *scaleSnapshot, seed int64, runs, budget int) {
	w.t.Helper()
	rng := rand.New(rand.NewSource(seed))
	for run := 0; run < runs; run++ {
		if w.mticks >= budget || idle == nil {
			w.skipped++
			continue
		}
		w.startGangProbe(idle)
		w.path = append(w.path, fmt.Sprintf("random run %d", run+1))
		w.push(fixedImage)
		w.probes++
		p := gangProbe{moment: whenRotating, verb: gangVerb(rng.Intn(int(gangSecondRequest) + 1)), target: gangTarget(rng.Intn(3))}
		if d := w.decodeSideIndex(); d >= 0 && rng.Intn(2) == 0 {
			p.side = d
		}
		for i, wait := 0, rng.Intn(12); i < wait; i++ {
			w.tick()
		}
		if !w.land(p) {
			w.finishProbe(w.playOut())
			continue
		}
		w.finishProbe(w.play(p))
	}
}

// gangReferenceConfigs are the configurations every run explores: the
// reference gang shapes. Two gangs of two under a surge of one; eight
// gangs of eight under RecreatePod at the chart's budget and under a
// surge of a quarter; three prefill and three decode gangs of eight under
// RecreatePod; one gang of four under RecreatePod.
func gangReferenceConfigs() []gangConfig {
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	return []gangConfig{
		{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 2}, settings: scaleSettings{prefill: surge, budget: "1"}}}},
		{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 8}, settings: scaleSettings{prefill: recreate}}}},
		{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 8}, settings: scaleSettings{prefill: surge, budget: "25%"}}}},
		{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{3, 3, 8}, settings: scaleSettings{prefill: recreate, decode: recreate}}}},
		{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{1, 0, 4}, settings: scaleSettings{prefill: recreate}}}},
	}
}

// gangConfigs lists the configurations of a run: the reference ones and,
// in full mode, the default scheduler's timing beside each, the
// force-delete policy left out, a partition, the ratio rule on the pair,
// six prefill and two decode gangs of eight, and the pairs of gangs the
// move model drives.
func gangConfigs(full bool) []gangConfig {
	configs := gangReferenceConfigs()
	if !full {
		return configs
	}
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	for _, cfg := range gangReferenceConfigs() {
		cfg.staggered = true
		configs = append(configs, cfg)
	}
	for _, cfg := range gangReferenceConfigs()[:3] {
		cfg.forceDeleteOff = true
		configs = append(configs, cfg)
	}
	return append(configs,
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{6, 2, 8}, settings: scaleSettings{prefill: recreate, decode: recreate}}}},
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 8}, settings: scaleSettings{prefill: recreate, partition: 2}}}},
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 8}, settings: scaleSettings{prefill: surge, partition: 2, budget: "25%"}}}},
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{3, 3, 8}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}}},
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 4, 2}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}}},
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 4}, settings: scaleSettings{prefill: recreate}}}},
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{4, 0, 8}, settings: scaleSettings{prefill: surge, budget: "50%"}}}},
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 2, 8}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true}}}},
		gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{4, 4, 4}, settings: scaleSettings{prefill: recreate, decode: surge, canary: true}}}},
	)
}

// gangExpectedClasses are the readings of this file a probe may end
// with that are not findings, beside the move model's.
var gangExpectedClasses = map[string]bool{
	"built-once-since-the-request":                            true,
	"answered-without-a-build-since-the-request":              true,
	"rebuilt-off-the-node-and-the-move-closed-satisfied":      true,
	"second-request-on-the-same-instance-closed-already-left": true,
	"second-request-on-the-reused-index-closed-already-left":  true,
	"second-request-moved-the-same-instance-again":            true,
	"second-request-moved-the-instance-now-at-the-index":      true,
	// After a lost member the gang's repair rebuilds it once at its own
	// index; that build answers the loss, not the request.
	"repaired-after-the-loss-off-the-node-and-the-move-closed-satisfied":              true,
	"repaired-after-the-loss-then-rebuilt-off-the-node-and-the-move-closed-satisfied": true,
	// The scaling world notes a shape whose held moment it does not reach
	// within the bound; no probe lands at that moment for the shape.
	"held-not-reached": true,
}

// knownGangClasses are the readings of this file the model shows on
// the current engine, each with the picture behind it. A finding in a
// known class is reported and does not fail the run; a change of
// behaviour removes its entry and the lock test that holds the picture.
var knownGangClasses = map[string]string{
	"the-next-gang-took-the-requests-index-and-its-node":              "a gang surge frees the index of the gang it replaced, the next gang surge takes that index as the lowest free one, and its members land on the least-loaded nodes, among them the node the request named once the old gang left it; the request closed as already left since the Instance it was filed for runs elsewhere; a request binds the Instance, not the node, and no rule keeps the Component's later builds off a node a request named; whether one should is a choice for the maintainers (held by TestMoveGang_TheRequestsIndexIsReusedByTheNextGangOnTheNodeTheRequestNamed)",
	"another-instance-landed-on-the-moves-node":                       "the same placement under another index: a gang built after the request closed lands on the node the request named, as the least-loaded node then (the same choice)",
	"rebuilt-on-the-requests-node-then-moved-again":                   "the roll's recreate or the repair rebuilds a gang with an open move request on the node the request names, the least-loaded node once the old gang left it, and the move then builds the gang again elsewhere: two builds for one request; a rebuild that kept off the node an open request names would make it one",
	"move-waits-unnamed-while-the-roll-rebuilds-its-gang-in-place":    "a request on a gang the roll is rebuilding in place is deferred without a word until the rebuilt gang is Ready, since an attempt in flight is expected to end by itself; a gang of eight takes as long as its slowest member; it is the wait every source mid-update gets, and nothing names it",
	"move-waits-unnamed-while-the-repair-rebuilds-its-gang":           "a request on a gang its repair is rebuilding is deferred without a word until the gang is Ready again (the same unnamed wait)",
	"move-waits-unnamed-behind-the-move-in-flight-on-its-instance":    "a second request on an Instance whose first request is still moving it stands Accepted with no text until the first closes: the pass drives one record per Instance and says nothing on the one behind it: a queued request carries no text",
	"move-past-its-accept-deadline-while-waiting-for-the-roll":        "a request waiting for the roll's surge replacements keeps the deadline it was accepted with on its record while the clock passes it: the deadline stands still for that wait and is measured again from the first pass with no surge in flight, as the text says",
	"move-on-a-gang-the-scale-down-removes-refused-as-spanning-nodes": "a request on a gang the scale-down is removing is refused as a single pod whose pods span several nodes: the gang is out of the plan the scale-down built, so the move reads its shape as one pod; the refusal is right, its reason is not",
	"move-starts-with-the-window-the-rolls-wait-spent":                "a request that named the roll as its wait and was then rebound to the index a gang surge rebuilt its Instance under loses the wait's text to the rebinding note, so the first pass with no surge in flight does not measure the window again and the move starts with the window the wait spent",
	"move-expired-after-a-named-wait-while-following-the-gang-surge":  "the same record closed Failed at the deadline the wait had spent, since the expiry restarts the deadline only on a record whose text still names the roll",
}

// gangKnownClasses are every known class a run reads against: the
// scaling model's, the move model's and this file's.
func gangKnownClasses() map[string]string {
	out := scaleKnownClasses()
	for class, text := range knownGangClasses {
		out[class] = text
	}
	return out
}

// gangResult is what one configuration's exploration found.
type gangResult struct {
	cfg                                               gangConfig
	states, ticks, passes, gateCalls, probes, skipped int
	counts                                            map[string]int
	findings                                          map[string]scaleFinding
	took                                              time.Duration
}

// gangExample is one configuration's first picture of a class.
type gangExample struct {
	cfg     gangConfig
	finding scaleFinding
}

// gangTotals sums the results of a run and keeps the first pictures of
// every class.
type gangTotals struct {
	states, ticks, passes, gateCalls, probes, skipped int
	classes                                           map[string]int
	configurations                                    map[string]int
	examples                                          map[string][]gangExample
}

func (tot *gangTotals) add(res gangResult) {
	tot.states += res.states
	tot.ticks += res.ticks
	tot.passes += res.passes
	tot.gateCalls += res.gateCalls
	tot.probes += res.probes
	tot.skipped += res.skipped
	for class, c := range res.counts {
		tot.classes[class] += c
	}
	for class, f := range res.findings {
		tot.configurations[class]++
		if len(tot.examples[class]) < 6 {
			tot.examples[class] = append(tot.examples[class], gangExample{cfg: res.cfg, finding: f})
		}
	}
}

// report logs every class with its first pictures and fails on a class
// that is neither expected nor known.
func (tot *gangTotals) report(t *testing.T, configs int, took time.Duration) {
	t.Helper()
	known := gangKnownClasses()
	classes := make([]string, 0, len(tot.classes))
	for class := range tot.classes {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d ticks, %d passes, %d gate questions, %d probes (%d skipped past the tick budget), %s", configs, tot.states, tot.ticks, tot.passes, tot.gateCalls, tot.probes, tot.skipped, took.Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d states in %d configurations", class, tot.classes[class], tot.configurations[class])
		if moveExpectedClasses[class] || gangExpectedClasses[class] {
			continue
		}
		for _, ex := range tot.examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		if text := known[class]; text != "" {
			t.Logf("  known: %s", text)
			continue
		}
		t.Errorf("%s: %d states (first: %s: %s)", class, tot.classes[class], tot.examples[class][0].cfg, tot.examples[class][0].finding.message)
	}
}

// exploreGangConfig explores one configuration once a worker slot is
// free and reports what it found.
func exploreGangConfig(t *testing.T, cfg gangConfig, full bool, seed int64, runs int, admission chan struct{}) gangResult {
	t.Helper()
	admission <- struct{}{}
	defer func() { <-admission }()
	began := time.Now()
	w := newGangWorld(t, cfg)
	w.explore(full, seed, runs)
	return gangResult{cfg: cfg, states: len(w.seen), ticks: w.ticks, passes: w.passes, gateCalls: w.gateCalls, probes: w.probes, skipped: w.skipped,
		counts: w.counts, findings: w.findings, took: time.Since(began)}
}

// gangExplore runs the model over the configurations keep admits, a
// bounded number at a time in parallel, and reports every class.
func gangExplore(t *testing.T, keep func(gangConfig) bool) {
	t.Helper()
	full := os.Getenv(moveGangFullEnv) == "full"
	only := os.Getenv(moveGangOnlyEnv)
	runs, workers := 1, 8
	if full {
		runs, workers = 4, gangWorkers
	}
	start := time.Now()
	var configs []gangConfig
	for _, cfg := range gangConfigs(full) {
		if keep(cfg) && (only == "" || strings.Contains(cfg.String(), only)) {
			configs = append(configs, cfg)
		}
	}
	results := make([]gangResult, len(configs))
	admission := make(chan struct{}, workers)
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				results[i] = exploreGangConfig(t, cfg, full, int64(i+1), runs, admission)
			})
		}
	})
	tot := &gangTotals{classes: map[string]int{}, configurations: map[string]int{}, examples: map[string][]gangExample{}}
	for n, res := range results {
		tot.add(res)
		var parts []string
		for class, c := range res.counts {
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: %d states, %d ticks, %d probes (%d skipped past the tick budget), %s: %s", n+1, len(configs), res.cfg, res.states, res.ticks, res.probes, res.skipped, res.took.Round(time.Second), strings.Join(parts, " "))
	}
	tot.report(t, len(configs), time.Since(start))
}

// TestMoveGang_EveryRequestOnAGangBesideAPushEndsOnTheInstance explores
// every configuration: a move request on a gang while a push is in
// flight, at every moment of the roll, naming the leader's node or a
// worker's, alone, with a member lost after it, with a worker's node
// stopped after it, and with a second request on the same index, leaves
// the record naming the Instance the operator asked about, the Instance
// built at most once between the request and its answer and off the node
// a completed request names, no index claimed twice, no gang over the
// ceiling, the record closed or naming its wait, and the roll finished
// or holding for a reason it names.
func TestMoveGang_EveryRequestOnAGangBesideAPushEndsOnTheInstance(t *testing.T) {
	gangExplore(t, func(gangConfig) bool { return true })
}

// playOneGangProbe plays one probe from a moment on a fresh world to its
// settled end and returns the world for a test's own reading.
func playOneGangProbe(t *testing.T, cfg gangConfig, p gangProbe) *gangWorld {
	t.Helper()
	w := newGangWorld(t, cfg)
	w.reachIdle()
	snap := w.momentSnapshots(p.moment == whenHeld)[p.moment]
	if snap == nil {
		t.Fatalf("the roll never reached its %s moment", p.moment)
	}
	w.startGangProbe(snap)
	w.path = append(w.path, "at "+p.moment.String())
	if !w.land(p) {
		t.Fatalf("no gang fits the probe %s; picture: %s", p, w.picture())
	}
	settled := w.play(p)
	if p.moment == whenPaused && settled {
		w.setPaused(false)
		w.path = append(w.path, "resume")
		settled = w.playOut()
	}
	w.finishProbe(settled)
	if !settled {
		t.Fatalf("the world never settled; picture: %s", w.picture())
	}
	return w
}

// podsOfTheInstanceOffTheNode fails the test when a live pod of the
// Instance the request named still stands on the node it names.
func (w *gangWorld) podsOfTheInstanceOffTheNode(t *testing.T) {
	t.Helper()
	s := w.gside
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if ok && w.identityOf(s, idx) == w.identity && pod.DeletionTimestamp == nil && pod.Spec.NodeName == w.fromNode {
			t.Errorf("%s Instance %d, the one the request was filed for, still runs pod %s on %s, the node it was asked to leave; picture: %s", s.comp, idx, pod.Name, w.fromNode, w.picture())
		}
	}
}

// eventsNaming counts the side's events with the reason and the request
// id, from the request on.
func (w *gangWorld) eventsNaming(req *moveRequestFacts, reason workloadtypes.EventReason) int {
	n := 0
	s := req.side
	for _, e := range s.h.events[min(req.eventsAt, len(s.h.events)):] {
		if strings.Contains(e, " "+string(reason)+" ") && strings.Contains(e, "uuid="+req.uuid) {
			n++
		}
	}
	return n
}

// TestMoveGang_ARequestOnTheGangTheRollIsSurgingFollowsItAndClosesAlreadyLeft
// holds the reference picture of two gangs of a leader and a worker
// under SurgeThenDrain with a surge of one. Version 2 is pushed; while
// the roll's copy of gang 0 comes up at index 2, the operator asks gang 0
// off its leader's node. The record names the roll as what it waits for.
// The copy serves, the old gang 0 leaves, and the record follows the
// Instance to index 2, saying so once. On the first pass with no surge in
// flight the request closes Completed with nothing to move: the Instance
// runs at index 2 off the node, no replacement is built, one completion
// event says so. The roll finishes with both gangs on version 2.
func TestMoveGang_ARequestOnTheGangTheRollIsSurgingFollowsItAndClosesAlreadyLeft(t *testing.T) {
	cfg := gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 2}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategySurgeThenDrain, budget: "1"}}}}
	w := playOneGangProbe(t, cfg, gangProbe{moment: whenSettling, verb: gangRequest, side: 0, target: gangOpen})
	s := w.gside
	req := w.request
	rec := s.h.migration(req.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance != nil || !strings.Contains(rec.Message, "nothing to move") {
		t.Fatalf("the move should close Completed with nothing to move; got %v; picture: %s", rec, w.picture())
	}
	if !w.trailOf(req.uuid).waited {
		t.Errorf("the record should have named the roll as what it waits for while the copy of gang %d came up", req.source)
	}
	if rec.SourceInstance == req.source {
		t.Errorf("the record should follow the Instance to the copy's index; it still names %d; picture: %s", req.source, w.picture())
	}
	if idx := w.indexOf(s, req.identity); rec.SourceInstance != idx {
		t.Errorf("the record names index %d; the Instance the request was filed for stands at index %d; picture: %s", rec.SourceInstance, idx, w.picture())
	}
	if n := w.eventsNaming(req, workloadtypes.EventReasonMigrationSourceRebuilt); n != 1 {
		t.Errorf("one event should say the record follows the rebuilt Instance; got %d", n)
	}
	if n := w.eventsNaming(req, workloadtypes.EventReasonMigrationCompleted); n != 1 {
		t.Errorf("one event should say the move completed; got %d", n)
	}
	w.podsOfTheInstanceOffTheNode(t)
	if !w.complete() {
		t.Errorf("the roll should finish around the request; picture: %s", w.picture())
	}
	builds := w.buildsBetween(s, req.identity, req.seqAt, 0)
	if len(builds) > 1 {
		t.Errorf("the roll's copy should be the only build of gang %d since the request; builds %s", req.source, buildNodes(builds))
	}
	for _, b := range builds {
		if b.node == w.fromNode {
			t.Errorf("the roll's copy of gang %d landed on %s, the node the request names; builds %s", req.source, w.fromNode, buildNodes(builds))
		}
	}
}

// TestMoveGang_TheRequestsIndexIsReusedByTheNextGangOnTheNodeTheRequestNamed
// holds the current placement on the same shape and probe: once gang 0 has
// gone to index 2 and the request has closed, the roll's copy of gang 1
// takes index 0, the lowest free, and its members land on the
// least-loaded nodes, the node the request named among them. The Instance
// the operator asked about is off that node; the gang now at index 0 is
// another Instance, which the request never bound. A rule that keeps a
// request's node off the Component's later builds flips the last check.
func TestMoveGang_TheRequestsIndexIsReusedByTheNextGangOnTheNodeTheRequestNamed(t *testing.T) {
	cfg := gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 2}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategySurgeThenDrain, budget: "1"}}}}
	w := playOneGangProbe(t, cfg, gangProbe{moment: whenSettling, verb: gangRequest, side: 0, target: gangOpen})
	s := w.gside
	req := w.request
	if rec := s.h.migration(req.uuid); rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance != nil {
		t.Fatalf("the move should close Completed with nothing to move; got %v; picture: %s", rec, w.picture())
	}
	w.podsOfTheInstanceOffTheNode(t)
	row := s.h.instance(req.source)
	if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.RunningRevision != s.h.target.Name || w.identityOf(s, req.source) == req.identity {
		t.Fatalf("index %d should hold another gang, Ready on version 2; row %v, identity %d against the request's %d; picture: %s", req.source, row, w.identityOf(s, req.source), req.identity, w.picture())
	}
	onNode := false
	for _, pod := range s.h.podsOf(req.source) {
		if pod.Spec.NodeName == w.fromNode {
			onNode = true
		}
	}
	if !onNode {
		t.Errorf("current placement: the gang at index %d has a member on %s, the node the request named; its pods stand on %s; picture: %s", req.source, w.fromNode, w.nodesOfIndex(s, req.source), w.picture())
	}
	if n := w.counts["the-next-gang-took-the-requests-index-and-its-node"]; n != 1 {
		t.Errorf("the model should read the reused index on the request's node once; classes %v", sortedFindingClasses(w.findings))
	}
}

// nodesOfIndex lists the nodes the live pods at an index stand on.
func (w *gangWorld) nodesOfIndex(s *scaleSide, idx int32) string {
	var nodes []string
	for _, pod := range s.h.podsOf(idx) {
		nodes = append(nodes, pod.Spec.NodeName)
	}
	sort.Strings(nodes)
	return strings.Join(nodes, ",")
}

// TestMoveGang_AMoveOfAWholeGangLandsEveryMemberOffTheNode holds the
// move of a gang with no roll open: eight gangs of a leader and seven
// workers under RecreatePod. The operator asks gang 7 off its leader's
// node, and version 2 is pushed beside the request. The move builds one
// whole gang at the next free index with no member on that node, drains
// the old gang and removes its row; the request closes Completed once;
// the push finishes with eight whole gangs on version 2.
func TestMoveGang_AMoveOfAWholeGangLandsEveryMemberOffTheNode(t *testing.T) {
	cfg := gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 8}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod}}}}
	w := playOneGangProbe(t, cfg, gangProbe{moment: whenIdle, verb: gangRequest, side: 0, target: gangDone})
	s := w.gside
	req := w.request
	rec := s.h.migration(req.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance == nil {
		t.Fatalf("the move should close Completed with a replacement; got %v; picture: %s", rec, w.picture())
	}
	if !w.complete() {
		t.Fatalf("the push should finish around the move; picture: %s", w.picture())
	}
	if pods := s.h.podsOf(*rec.SurgeInstance); int32(len(pods)) != w.podsPerInstance(s) {
		t.Errorf("the replacement gang %d should be whole with %d pods; it has %d", *rec.SurgeInstance, w.podsPerInstance(s), len(pods))
	}
	w.podsOfTheInstanceOffTheNode(t)
	if row := s.h.instance(req.source); row != nil {
		t.Errorf("the source row %d should be gone after the move; it reads %v", req.source, row)
	}
	if n := w.eventsNaming(req, workloadtypes.EventReasonMigrationCompleted); n != 1 {
		t.Errorf("one event should say the move completed; got %d", n)
	}
	if n := w.counts["built-once-since-the-request"]; n != 1 {
		t.Errorf("the model should read one build since the request, the move's; classes %v", sortedFindingClasses(w.findings))
	}
}

// TestMoveGang_TheRollsSurgesAndTheMoveNeverClaimOneIndex holds the index
// rule on eight gangs of eight under a surge of two: a request on the
// gang the roll is replacing, one on a gang already moved and one on a
// gang still waiting, each while the roll rotates with two copies coming
// up. At no pass do two rows share an index or two attempts claim one
// replacement index, and the live gangs never exceed ten. Every request
// closes Completed and the roll finishes.
func TestMoveGang_TheRollsSurgesAndTheMoveNeverClaimOneIndex(t *testing.T) {
	cfg := gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 8}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategySurgeThenDrain, budget: "25%"}}}}
	w := newGangWorld(t, cfg)
	w.reachIdle()
	snap := w.momentSnapshots(false)[whenRotating]
	if snap == nil {
		t.Fatalf("the roll never reached its rotating moment")
	}
	for _, target := range []gangTarget{gangOpen, gangDone, gangBehind} {
		w.startGangProbe(snap)
		w.path = append(w.path, "at rotating")
		if !w.land(gangProbe{moment: whenRotating, verb: gangRequest, side: 0, target: target}) {
			t.Fatalf("no gang fits the target %s; picture: %s", target, w.picture())
		}
		settled := w.playOut()
		w.finishProbe(settled)
		if !settled || !w.complete() {
			t.Fatalf("the roll should finish around the request on the %s gang; picture: %s", target, w.picture())
		}
		if rec := w.gside.h.migration(w.request.uuid); rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted {
			t.Fatalf("the request on the %s gang should close Completed; got %v; picture: %s", target, rec, w.picture())
		}
	}
	for _, class := range []string{"surge-index-claimed-twice", "two-rows-at-one-index", "gangs-over-the-ceiling", "move-record-names-another-instance"} {
		if f, read := w.findings[class]; read {
			t.Errorf("%s: %s", class, f.message)
		}
	}
}

// TestMoveGang_ARequestOnAGangTheRollRebuildsInPlaceWaitsUnnamedUntilItIsReady
// holds the current picture on eight gangs of eight under RecreatePod:
// while the roll rotates the operator asks the gang the roll is rebuilding
// right now off its leader's node. The record stands Accepted with no
// text through the rebuild, since the drive names only the waits no
// attempt in flight ends, and closes Completed once the gang is Ready
// again: as already left when the rebuilt leader landed elsewhere, else
// with a replacement built off the node. A rule that names the wait on
// the record flips the first check.
func TestMoveGang_ARequestOnAGangTheRollRebuildsInPlaceWaitsUnnamedUntilItIsReady(t *testing.T) {
	cfg := gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 8}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod}}}}
	w := newGangWorld(t, cfg)
	w.reachIdle()
	snap := w.momentSnapshots(false)[whenRotating]
	if snap == nil {
		t.Fatalf("the roll never reached its rotating moment")
	}
	w.startGangProbe(snap)
	w.path = append(w.path, "at rotating")
	if !w.land(gangProbe{moment: whenRotating, verb: gangRequest, side: 0, target: gangOpen}) {
		t.Fatalf("no gang is being rebuilt; picture: %s", w.picture())
	}
	s := w.gside
	req := w.request
	unnamed := 0
	for i := 0; i < moveSettleTicks; i++ {
		rec := s.h.migration(req.uuid)
		if rec == nil || rec.Phase.Terminal() {
			break
		}
		row := s.h.instance(req.source)
		rebuilding := row != nil && row.Operation != nil && row.Operation.Type == workloadtypes.InstanceOperationUpdate
		if rebuilding && rec.Message == "" {
			unnamed++
		}
		if rebuilding && rec.Message != "" {
			t.Fatalf("the record named a wait while the gang was rebuilt in place: %q; a rule change, not the current picture; picture: %s", rec.Message, w.picture())
		}
		w.tick()
	}
	if unnamed == 0 {
		t.Fatalf("current picture: the record stands Accepted with no text through the rebuild; it was never read so; picture: %s", w.picture())
	}
	if !w.playOut() {
		t.Fatalf("the world never settled; picture: %s", w.picture())
	}
	w.finishProbe(true)
	rec := s.h.migration(req.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted {
		t.Fatalf("the request should close Completed once the gang is Ready again; got %v; picture: %s", rec, w.picture())
	}
	w.podsOfTheInstanceOffTheNode(t)
	if !w.complete() {
		t.Errorf("the roll should finish around the request; picture: %s", w.picture())
	}
	t.Logf("the record stood unnamed for %d tick(s) of the rebuild and closed %q; classes %v", unnamed, rec.Message, sortedFindingClasses(w.findings))
}

// cordonEveryNodeBut marks every live node of a side unschedulable except
// the ones named, so the next pods placed land among those.
func (w *gangWorld) cordonEveryNodeBut(s *scaleSide, keep ...string) {
	kept := map[string]bool{}
	for _, name := range keep {
		kept[name] = true
	}
	for _, name := range s.h.nodeNames {
		if !kept[name] && !s.h.deadNodes[name] && !s.h.cordonedNodes[name] {
			s.h.cordonNode(name)
		}
	}
	w.path = append(w.path, fmt.Sprintf("every node but %v is cordoned", keep))
}

// uncordonEveryNode removes every cordon of a side.
func (w *gangWorld) uncordonEveryNode(s *scaleSide) {
	for name := range s.h.cordonedNodes {
		node := &corev1.Node{}
		if err := s.h.c.Get(s.h.ctx, types.NamespacedName{Name: name}, node); err != nil {
			w.t.Fatalf("get node %s: %v", name, err)
		}
		node.Spec.Unschedulable = false
		if err := s.h.c.Update(s.h.ctx, node); err != nil {
			w.t.Fatalf("uncordon node %s: %v", name, err)
		}
	}
	s.h.cordonedNodes = map[string]bool{}
	w.path = append(w.path, "every cordon is removed")
}

// TestMoveGang_ARequestThatFollowedItsGangStartsWithTheWindowTheRollsWaitSpent
// holds the current deadline on two gangs of two under a surge of one, in
// a cluster with room only on the nodes the gangs hold. Version 2 is
// pushed; the roll's copy of gang 0 comes up on the nodes gang 0 holds,
// its leader on gang 0's leader's node, and the operator asks gang 0 off
// that node while the copy comes up. The record names the roll as what
// it waits for and says its window will run from the roll's release. The
// copy serves, the old gang leaves, and the record follows the Instance
// to the copy's index; the room is then opened again. On the first pass
// with no surge in flight the Instance still sits on the node, so the
// move starts: it starts with the window the wait spent, since the
// rebinding note replaced the wait's text and only that text restarts
// the window. A rule that starts the move with its whole window flips
// the last check.
func TestMoveGang_ARequestThatFollowedItsGangStartsWithTheWindowTheRollsWaitSpent(t *testing.T) {
	cfg := gangConfig{moveConfig: moveConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 2}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategySurgeThenDrain, budget: "1"}}}}
	w := newGangWorld(t, cfg)
	w.reachIdle()
	s := w.prefill()
	w.startGangProbe(w.snapshot())
	var held []string
	for _, pod := range s.h.livePods() {
		held = append(held, pod.Spec.NodeName)
	}
	w.cordonEveryNodeBut(s, held...)
	w.push(fixedImage)
	w.tick()
	if !w.land(gangProbe{moment: whenSettling, verb: gangRequest, side: 0, target: gangOpen}) {
		t.Fatalf("the roll should be surging a gang; picture: %s", w.picture())
	}
	req := w.request
	window := s.h.lifecycle.InstanceReadyTimeout.Duration
	rec, _ := w.tickUntilTheWaitIsNamed(s, req.uuid)
	if rec == nil || !moveWaitsForTheRoll(rec) {
		t.Fatalf("the record should name the roll as what it waits for; got %v; picture: %s", rec, w.picture())
	}
	waitedFrom := w.clk.Now()
	for i := 0; i < moveSettleTicks; i++ {
		rec = s.h.migration(req.uuid)
		if rec == nil || rec.Phase.Terminal() || rec.SourceInstance != req.source {
			break
		}
		w.tick()
	}
	if rec == nil || rec.Phase.Terminal() || rec.SourceInstance == req.source {
		t.Fatalf("the record should follow the Instance to the copy's index while open; got %v; picture: %s", rec, w.picture())
	}
	waited := w.clk.Now().Sub(waitedFrom)
	w.uncordonEveryNode(s)
	startedAt := w.clk.Now()
	for i := 0; i < moveSettleTicks; i++ {
		rec = s.h.migration(req.uuid)
		if rec == nil || rec.Phase.Terminal() || rec.SurgeAllocated() {
			break
		}
		startedAt = w.clk.Now()
		w.tick()
	}
	if rec == nil || !rec.SurgeAllocated() {
		t.Fatalf("the move should start once no surge is in flight, since the Instance sits on %s; got %v; picture: %s", w.fromNode, rec, w.picture())
	}
	// The pass that started the move ran at the clock before its tick.
	left := rec.Deadline.Sub(startedAt)
	if !w.playOut() {
		t.Fatalf("the world never settled after the move started; picture: %s", w.picture())
	}
	w.finishProbe(true)
	w.podsOfTheInstanceOffTheNode(t)
	if final := s.h.migration(req.uuid); final == nil || final.Phase != workloadtypes.MigrationPhaseCompleted {
		t.Errorf("the move should close Completed; got %v; picture: %s", final, w.picture())
	}
	if waited < scaleRollLeastStep {
		t.Fatalf("the wait for the roll should span at least one tick for the reading to hold; it spanned %s", waited)
	}
	if !rec.Deadline.Time.Equal(rec.StartedAt.Add(window)) || left >= window {
		t.Errorf("current deadline: the move starts with the window the wait spent, its deadline still the accept time plus %s; the deadline reads %s for a request accepted at %s, and the move started at %s with %s left", window, rec.Deadline.Time, rec.StartedAt.Time, startedAt, left)
	}
	if n := w.counts["move-starts-with-the-window-the-rolls-wait-spent"]; n != 1 {
		t.Errorf("the model should read the spent window once; classes %v", sortedFindingClasses(w.findings))
	}
	t.Logf("the request waited %s for the roll and started with %s of its %s window", waited, left, window)
}
