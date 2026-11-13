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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the production engine over a move of one Instance
// while something else is under way. Three things move an Instance off
// its node: the operator's move request (a record in status.migrations
// that builds a replacement beside the source, drains the source and
// removes it), a node that stops reporting (the force-delete policy
// frees the pods on it at its threshold and the Instance is rebuilt
// elsewhere), and a repair that rebuilds an Instance whose pods were
// lost. Each lands while the Component is idle, while a roll is
// settling, rotating, draining or at its last Instance, at a canary
// step, while the roll is held on a spent retry schedule, under a pause,
// under a partition that holds part of the fleet, beside a scale-up
// still coming up and beside a scale-down still leaving; under rolling
// groups at 10 and 25 percent and under a blueGreen group; on the
// production engine-only and prefill/decode shapes, single pods and
// gangs. A fourth move loses the node under a move's source once its
// replacement serves, so the move waits on a kubelet that is gone.
//
// The world is the scaling model's (scale_beside_a_roll_model_test.go):
// one workload.Reconcile loop per Component on the recovery harness, the
// coordination gates wired between the Components, the endpoint model on
// since a move reads its replacement's rotation off the routed Service,
// one pod per node as the reference shapes place engines, and the
// chart's lifecycle values read through the operator's config loaders
// (the ready timeout, the retry schedule, the force-delete policy and
// the migration caps).
//
// The model asks three settled questions: a move during a roll builds its
// replacement
// once, on the roll's target when the roll could take the source (a
// retry record in Backoff counts as a roll that could), else on the
// revision the source runs; a move waiting on a kubelet that is gone
// holds no other Instance's push or create. And what an operator expects
// of any move: the record's phases only go forward and end Completed, or
// Failed naming why; the replacement serves off the node the move left;
// every Instance the move or the roll takes is built once per reason; a
// hold, a wait token or the record's text names every wait; no hold and
// no record text opens and closes on alternate passes; no attempt stands
// past its deadline with nothing named.
//
// OME_MOVE_HUNT=full widens the configurations, the moments, the targets
// and the bounds; OME_MOVE_HUNT_ONLY=<text> keeps the configurations
// whose name carries the text.

const (
	moveHuntFullEnv = "OME_MOVE_HUNT"
	moveHuntOnlyEnv = "OME_MOVE_HUNT_ONLY"
	// moveSettleTicks is how many ticks a probe may take before it is
	// read as never settling.
	moveSettleTicks = 160
	// moveTickBudgetBounded and moveTickBudgetFull bound the ticks one
	// configuration spends; the probes left when a budget is spent are
	// counted as skipped, so a run on a slower machine reads the same
	// first probes.
	moveTickBudgetBounded = 3000
	moveTickBudgetFull    = 12000
	// moveWorkers is the most configurations explored at once in full
	// mode, to keep the suite's CPU use bounded.
	moveWorkers = 16
	// moveDrainingTicks is how long the fourth move waits for the
	// request's record to reach its source drain before it loses the
	// node under the source.
	moveDrainingTicks = 30
	// moveFlapWindow is how many ticks a hold or a record text must
	// alternate over, with the rows unchanged, to read as a loop.
	moveFlapWindow = 4
	// moveOverdueTicks is how many ticks an attempt or a record may stand
	// past its deadline with nothing named before it is a finding.
	moveOverdueTicks = 3
	// moveStallClock is how long, on the model's clock, a side with
	// Instances still to roll may stand with its rows and pods unchanged
	// beside a move waiting on its source teardown before the wait is read
	// as the roll's: longer than a batch's drain quiet under the chart's
	// values, shorter than the settling window.
	moveStallClock = 4 * time.Minute
	// moveUnnamedTicks is how many ticks a record may stand open with no
	// text, on a side that is not paused, before the wait is read as
	// unnamed.
	moveUnnamedTicks = 6
	// moveFreshIdentityBase numbers the Instances created again under an
	// index an earlier Instance of the probe had.
	moveFreshIdentityBase int32 = 1 << 20
)

// moveVerb is what moves the Instance.
type moveVerb uint8

const (
	verbRequest               moveVerb = iota // the operator asks the Instance off its node
	verbNodeLost                              // the node under the Instance stops reporting for good
	verbPodsLost                              // the Instance's pods are deleted; the repair rebuilds it
	verbRequestSourceNodeLost                 // a move request whose source's node stops reporting once the replacement serves
)

var moveVerbNames = [...]string{"move-request", "node-lost", "pods-lost", "move-request-then-source-node-lost"}

func (v moveVerb) String() string { return moveVerbNames[v] }

// moveMoment is what is under way when the move lands.
type moveMoment uint8

const (
	whenIdle        moveMoment = iota // no roll
	whenSettling                      // a roll with its first attempts open, nothing promoted
	whenRotating                      // a roll with Instances promoted, open and still to move
	whenDraining                      // a roll's attempt deleting its old pods
	whenLast                          // a roll's last Instances moving
	whenCanaryStep                    // the pair resting at a canary step
	whenHeld                          // the roll held on a spent retry schedule
	whenPaused                        // the roll paused while rotating
	whenScalingUp                     // a scale-up landed while rotating, its Instances still coming up
	whenScalingDown                   // a scale-down landed while rotating, its Instances still leaving
)

var moveMomentNames = [...]string{"idle", "settling", "rotating", "draining", "last", "canary-step", "held", "paused", "scaling-up", "scaling-down"}

func (m moveMoment) String() string { return moveMomentNames[m] }

// moveTarget picks the Instance the move lands on.
type moveTarget uint8

const (
	targetBehind moveTarget = iota // the lowest Instance still on the old version with no attempt open
	targetDone                     // the highest Instance already on the new version
	targetHeld                     // the lowest Instance the partition holds on the old version
	targetOpen                     // the lowest Instance with an attempt open
)

var moveTargetNames = [...]string{"behind", "done", "held", "open"}

func (t moveTarget) String() string { return moveTargetNames[t] }

// moveProbe is one move at one moment on one Instance.
type moveProbe struct {
	moment moveMoment
	verb   moveVerb
	side   int
	target moveTarget
}

func (p moveProbe) String() string {
	return fmt.Sprintf("%s at %s on side %d (%s)", p.verb, p.moment, p.side, p.target)
}

// moveConfig is one configuration the model explores: the scaling
// model's shape and settings, and whether the force-delete policy is
// left out, as the chart's default leaves it.
type moveConfig struct {
	scaleConfig
	forceDeleteOff bool
}

func (c moveConfig) String() string {
	s := c.scaleConfig.String()
	if c.forceDeleteOff {
		s += "+no-force-delete"
	}
	return s
}

// moveBuild is one routable pod seen for an Instance: a build of it,
// attributed to the Instance the pod's index stood for when it was seen.
type moveBuild struct {
	uid      types.UID
	identity int32
	index    int32
	revision string
	node     string
	tick     int
	seq      int
}

// moveRecordTrail is what the model remembers of one move record across
// ticks: the distinct phases in order, the last texts, and how long it
// has stood past its deadline.
type moveRecordTrail struct {
	side     *scaleSide
	phases   []workloadtypes.MigrationPhase
	messages []string
	overdue  int
	// unnamed counts the ticks the record has stood open with no text;
	// unnamedMost is the longest such run.
	unnamed, unnamedMost int
	// pairSeq is the build number the side had reached when the record
	// left Accepted, completedSeq the one it had reached when the record
	// closed; the move's own replacement is the first build between them.
	pairSeq, completedSeq int
	// completedAt is the tick the record first read terminal, -1 before.
	completedAt int
	// leftNamed says the record has been read waiting for the roll with
	// its Instance already off the node it names.
	leftNamed bool
}

// moveHoldTrail is a side's hold gates over the last ticks beside the
// rows they were read against.
type moveHoldTrail struct {
	gates []string
	rows  []string
}

// moveRequestFacts is what the model read when a move request was filed,
// for the readings at the end of the probe.
type moveRequestFacts struct {
	side      *scaleSide
	uuid      string
	source    int32
	identity  int32
	fromNode  string
	sourceRev string
	// rollCouldTake says the roll could take the source when the request
	// was filed; rollCouldTakeAtStart says the same at the pass the move
	// allocated its replacement in, after any wait under a pause. The
	// decided revision of the replacement follows the start.
	rollCouldTake        bool
	rollCouldTakeAtStart bool
	// sourceRevAtStart is the revision the source ran at the pass that
	// started the move: the roll may have taken the source while the
	// move waited for it to stand steady.
	sourceRevAtStart string
	started          bool
	takeBefore       bool
	srcBefore        string
	surgeInFlight    bool
	// holdAtRequest says the side held when the request was filed.
	holdAtRequest bool
	// seqAt is the side's build number when the request was filed; tick
	// is the tick it was filed at.
	seqAt int
	tick  int
	// paused and held say why the roll could not take the source: the
	// side was paused, or the partition held the source. heldBefore is
	// the set the partition held when the request was filed.
	paused     bool
	held       bool
	heldBefore map[int32]bool
	// pushedAfter says the probe pushed a new revision after the request,
	// so the roll that takes the replacement came later by design.
	pushedAfter bool
	// eventsAt is how many events the side had when the request was filed.
	eventsAt int
}

// moveWorld is the scaling model's world with the move verbs, the
// readings a move needs and the bookkeeping behind them.
type moveWorld struct {
	*scaleWorld
	mcfg moveConfig
	// probe is the probe under play; probeTick is the tick its verb landed.
	probe     moveProbe
	probeTick int
	// requests counts the move requests filed, to name each record.
	requests int
	// request is the last request this probe filed, nil for the other
	// verbs; filed is every request of the probe, in the order filed.
	request *moveRequestFacts
	filed   []*moveRequestFacts
	// lostNodes names the nodes this probe stopped, per side.
	lostNodes map[*scaleSide]map[string]bool
	// moved are the Instances a verb took the pods of, per side and by
	// identity, with whether a roll could take each at that moment and
	// the build sequence and tick it happened at; touched are the
	// Instances with any pod on a node this probe stopped. backoffAtMove
	// says the target's retry record was in Backoff when the verb landed.
	moved         map[*scaleSide]map[int32]bool
	touched       map[*scaleSide]map[int32]bool
	rollTakes     map[*scaleSide]map[int32]bool
	movedAt       map[*scaleSide]map[int32]int
	movedTick     map[*scaleSide]map[int32]int
	backoffAtMove bool
	// builds keeps, per side, every routable pod seen, with the Instance
	// it was built for and the revision it renders, numbered in the order
	// seen; buildMark is the number the probe's first verb landed at.
	builds    map[*scaleSide][]moveBuild
	seq       map[*scaleSide]int
	buildMark map[*scaleSide]int
	// identity maps an index to the Instance it stands for: itself, the
	// Instance a gang surge or a move replaces under it, or a fresh
	// Instance when a freed index is created again; fresh numbers those.
	// seqAtTick is the build number at the start of this tick's readings;
	// prevRows and everRows are the indices with a row at the last tick
	// and at any tick of the probe.
	identity  map[*scaleSide]map[int32]int32
	fresh     map[*scaleSide]int32
	seqAtTick map[*scaleSide]int
	prevRows  map[*scaleSide]map[int32]bool
	everRows  map[*scaleSide]map[int32]bool
	// records is every move record's trail, by request id.
	records map[string]*moveRecordTrail
	// holds is every side's hold trail.
	holds map[*scaleSide]*moveHoldTrail
	// overdue counts, per side and Instance, the ticks an attempt has
	// stood past its deadline with nothing named while the side stood
	// still; deadlineKey is the side's rows and pods as last read.
	overdue     map[*scaleSide]map[int32]int
	deadlineKey map[*scaleSide]string
	// stall counts, per side, the ticks the side stood unchanged with
	// Instances still to roll, no hold and a move waiting on its source
	// teardown.
	stall    map[*scaleSide]int
	stallKey map[*scaleSide]string
	// stallSince is the clock time a side's rows and pods last changed
	// while a move of it waited on its source teardown.
	stallSince map[*scaleSide]time.Time
	// landed counts the verbs landed in this probe; the first one marks
	// every Instance's builds.
	landed int
	// unnamedClass, when set, names an unnamed wait by a scene a model
	// built on this world knows; empty leaves the reading to this file.
	unnamedClass func(s *scaleSide, rec workloadtypes.MigrationRecord) string
	// overdueClass, when set, names a record past its deadline by a scene
	// a model built on this world knows; empty leaves the reading to this
	// file.
	overdueClass func(s *scaleSide, rec workloadtypes.MigrationRecord) string
	// failedClass, when set, names a record closed Failed by a scene a
	// model built on this world knows; empty leaves the reading to this
	// file.
	failedClass func(s *scaleSide, rec workloadtypes.MigrationRecord) string
	// probes, skipped and mticks count the configuration's work.
	probes, skipped, mticks int
}

// moveChartKnobs are the chart's lifecycle values, read through the
// operator's config loaders the way the controller loads them.
func moveChartKnobs(forceDelete bool) repairKnobs {
	return repairKnobs{deadlines: "chart", tries: 3, forceDelete: forceDelete}
}

func newMoveWorld(t *testing.T, cfg moveConfig) *moveWorld {
	t.Helper()
	w := &moveWorld{scaleWorld: newScaleWorld(t, cfg.scaleConfig), mcfg: cfg}
	settings := resolveRepairSettings(t, moveChartKnobs(!cfg.forceDeleteOff).lifecycleConfig())
	for _, s := range w.sides {
		h := s.h
		h.routed = true
		h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: settings.readyTimeout}
		h.retryPolicy = settings.retry
		h.forceDelete = settings.forceDelete
		h.repairBatchSize = settings.repairBatch
		h.requeue = settings.requeue
		h.migrationAudit = settings.audit
		h.unschedulableGrace = settings.unschedulableGrace
		if w.podsPerInstance(s) > 1 {
			// Gangs run the policy production sets on them: a member
			// lost rebuilds the gang.
			policy := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &policy
		}
		w.render(s)
		// One pod per node, as the reference shapes place engines, with
		// room for every replacement: a lost node takes one pod with it.
		var nodes []string
		for i := len(h.nodeNames); i < 2*int(h.replicas*w.podsPerInstance(s))+2; i++ {
			nodes = append(nodes, fmt.Sprintf("%s-node-%d", s.comp, i))
		}
		h.addNodes(nodes...)
	}
	w.resetTrails()
	return w
}

// resetTrails forgets what the model remembered of the previous probe.
func (w *moveWorld) resetTrails() {
	w.request = nil
	w.filed = nil
	w.lostNodes = map[*scaleSide]map[string]bool{}
	w.moved = map[*scaleSide]map[int32]bool{}
	w.touched = map[*scaleSide]map[int32]bool{}
	w.rollTakes = map[*scaleSide]map[int32]bool{}
	w.movedAt = map[*scaleSide]map[int32]int{}
	w.movedTick = map[*scaleSide]map[int32]int{}
	w.backoffAtMove = false
	w.builds = map[*scaleSide][]moveBuild{}
	w.seq = map[*scaleSide]int{}
	w.buildMark = map[*scaleSide]int{}
	w.identity = map[*scaleSide]map[int32]int32{}
	w.fresh = map[*scaleSide]int32{}
	w.seqAtTick = map[*scaleSide]int{}
	w.prevRows = map[*scaleSide]map[int32]bool{}
	w.everRows = map[*scaleSide]map[int32]bool{}
	w.records = map[string]*moveRecordTrail{}
	w.holds = map[*scaleSide]*moveHoldTrail{}
	w.overdue = map[*scaleSide]map[int32]int{}
	w.deadlineKey = map[*scaleSide]string{}
	w.stall = map[*scaleSide]int{}
	w.stallKey = map[*scaleSide]string{}
	w.stallSince = map[*scaleSide]time.Time{}
	w.landed = 0
	for _, s := range w.sides {
		w.lostNodes[s] = map[string]bool{}
		w.moved[s] = map[int32]bool{}
		w.touched[s] = map[int32]bool{}
		w.rollTakes[s] = map[int32]bool{}
		w.movedAt[s] = map[int32]int{}
		w.movedTick[s] = map[int32]int{}
		w.identity[s] = map[int32]int32{}
		w.everRows[s] = map[int32]bool{}
		w.holds[s] = &moveHoldTrail{}
		w.overdue[s] = map[int32]int{}
	}
}

// startProbe puts the world at a snapshot with the readings cleared and
// an event recorder on every side, since the restore drops it.
func (w *moveWorld) startProbe(snap *scaleSnapshot) {
	w.restore(snap)
	for _, s := range w.sides {
		s.h.recorder = record.NewFakeRecorder(8192)
	}
	w.resetTrails()
	w.observe()
}

// tick runs the scaling model's tick and the move readings after it. The
// key it returns carries the move records beside the rows, since a
// record's phase or text changes the state as much as a row does.
func (w *moveWorld) tick() string {
	w.readMoveStarts(false)
	key := w.scaleWorld.tick()
	w.mticks++
	w.readMoveStarts(true)
	w.observe()
	return key + w.recordsKey()
}

// readMoveStarts reads, before a pass, whether the roll could take each
// open move's source, and keeps that reading for a move the pass started:
// the pass that allocates the replacement is the one that picks its
// revision.
func (w *moveWorld) readMoveStarts(afterPass bool) {
	for _, req := range w.filed {
		if req.started {
			continue
		}
		rec := req.side.h.migration(req.uuid)
		if rec == nil || rec.Phase.Terminal() {
			continue
		}
		if !afterPass {
			req.takeBefore = w.rollCouldTake(req.side, req.source)
			if row := req.side.h.instance(req.source); row != nil {
				req.srcBefore = w.short(req.side, row.RunningRevision)
			}
			continue
		}
		if rec.SurgeInstance != nil {
			req.started, req.rollCouldTakeAtStart, req.sourceRevAtStart = true, req.takeBefore, req.srcBefore
		}
	}
}

// couldTakeAtStart is the reading of the roll against the move's source
// at the pass that started the move; a move the model never saw start
// keeps the reading from its filing.
func (req *moveRequestFacts) couldTakeAtStart() bool {
	if req.started {
		return req.rollCouldTakeAtStart
	}
	return req.rollCouldTake
}

// sourceRevisionAtStart is the revision the source ran at the pass that
// started the move, or at the filing for a move the model never saw start.
func (req *moveRequestFacts) sourceRevisionAtStart() string {
	if req.started && req.sourceRevAtStart != "" {
		return req.sourceRevAtStart
	}
	return req.sourceRev
}

// picture is the scaling model's picture with every open move record's
// phase and text after it.
func (w *moveWorld) picture() string {
	var b strings.Builder
	b.WriteString(w.scaleWorld.picture())
	for _, s := range w.sides {
		for _, rec := range s.h.migrations {
			if rec.Phase.Terminal() {
				continue
			}
			fmt.Fprintf(&b, "; move %s %s %q", rec.RequestUUID, rec.Phase, rec.Message)
		}
	}
	return b.String()
}

// recordsKey is every side's move records, by phase and text.
func (w *moveWorld) recordsKey() string {
	var b strings.Builder
	for _, s := range w.sides {
		for _, rec := range s.h.migrations {
			fmt.Fprintf(&b, "%s:%s:%s;", rec.RequestUUID, rec.Phase, rec.Message)
		}
		b.WriteString("|")
	}
	return b.String()
}

// moveSettling is the scaling model's settling with the move records in
// the state. A record still open with no text keeps the world unsettled:
// the next pass or the record's own deadline moves it.
type moveSettling struct{ inner *scaleSettling }

func (w *moveWorld) startSettling() *moveSettling {
	inner := w.scaleWorld.startSettling()
	inner.prev += w.recordsKey()
	return &moveSettling{inner: inner}
}

func (t *moveSettling) observe(w *moveWorld, key string) bool {
	return t.inner.observe(w.scaleWorld, key) && !w.openMoveUnnamed()
}

// openMoveUnnamed reports a move record open with no text on a side that
// is not paused; under a pause the pause is what the record waits on.
func (w *moveWorld) openMoveUnnamed() bool {
	for _, s := range w.sides {
		if s.paused {
			continue
		}
		for _, rec := range s.h.migrations {
			if !rec.Phase.Terminal() && rec.Message == "" {
				return true
			}
		}
	}
	return false
}

// playOut ticks until the world has settled or the bound is reached; it
// reports whether the world settled.
func (w *moveWorld) playOut() bool {
	settling := w.startSettling()
	for i := 0; i < moveSettleTicks; i++ {
		if settling.observe(w, w.tick()) {
			return true
		}
	}
	return false
}

// revisionNameFor is the revision a pod of side s renders, by the name
// the world knows it under.
func (w *moveWorld) revisionNameFor(s *scaleSide, pod *corev1.Pod) string {
	hash := pod.Labels[query.LabelRevisionHash]
	if hash == "" {
		return ""
	}
	return recoveryOwner + "-" + string(s.comp) + "-" + hash
}

// observe runs the readings that compare two ticks: the builds, the move
// records, the holds and the deadlines.
func (w *moveWorld) observe() {
	for _, s := range w.sides {
		w.seqAtTick[s] = w.seq[s]
		w.observeBuilds(s)
		w.observeRecords(s)
		w.observeHold(s)
		w.observeDeadlines(s)
		w.observeStall(s)
	}
}

// observeBuilds counts a new routable pod as one build of the Instance
// its index stands for, with the revision it renders. A surge or a move
// writes its replacement's index on the row it replaces; from then on
// the replacement's index stands for that Instance, and an index freed
// and used again stands for whatever it is next written for.
func (w *moveWorld) observeBuilds(s *scaleSide) {
	rows := w.rows(s)
	byIndex := map[int32]*workloadtypes.InstanceStatus{}
	for i := range rows {
		byIndex[rows[i].Index] = &rows[i]
	}
	for i := range rows {
		r := &rows[i]
		op := r.Operation
		if op == nil || op.SurgeIndex == nil || *op.SurgeIndex == r.Index {
			continue
		}
		source := (op.Type == workloadtypes.InstanceOperationUpdate && op.Step != workloadtypes.UpdateStepGangSurgeTarget && op.Step != workloadtypes.UpdateStepGangSurgeTargetCleanup) ||
			(op.Type == workloadtypes.InstanceOperationMigrate && r.Phase == workloadtypes.InstancePhaseMigrating)
		target := byIndex[*op.SurgeIndex]
		// The pair stands when the replacement's row points back, or
		// when it is a gang surge's target row, which never points back
		// and exists only as the target of the one source that names it.
		// A surge index named on the source alone, with no such row, is
		// a reservation the plan may still move.
		if source && target != nil && isSurgePartnerOf(target, r.Index) {
			w.identity[s][*op.SurgeIndex] = w.identityOf(s, r.Index)
		}
	}
	w.observeRows(s)
	seen := map[types.UID]int{}
	for i := range w.builds[s] {
		seen[w.builds[s][i].uid] = i
	}
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || !query.RoutedRunner(pod) {
			continue
		}
		if i, ok := seen[pod.UID]; ok {
			if w.builds[s][i].node == "" {
				w.builds[s][i].node = pod.Spec.NodeName
			}
			continue
		}
		w.seq[s]++
		w.builds[s] = append(w.builds[s], moveBuild{uid: pod.UID, identity: w.identityOf(s, idx), index: idx, revision: w.short(s, w.revisionNameFor(s, pod)), node: pod.Spec.NodeName, tick: w.mticks, seq: w.seq[s]})
	}
}

// isSurgePartnerOf reports a row written as the replacement of Instance
// idx: its operation points back at idx, or it is a gang surge's target
// row.
func isSurgePartnerOf(r *workloadtypes.InstanceStatus, idx int32) bool {
	op := r.Operation
	if op == nil {
		return false
	}
	if op.SurgeIndex != nil && *op.SurgeIndex == idx {
		return true
	}
	return op.Type == workloadtypes.InstanceOperationUpdate && op.Step == workloadtypes.UpdateStepGangSurgeTarget
}

// identityOf is the Instance an index stands for.
func (w *moveWorld) identityOf(s *scaleSide, idx int32) int32 {
	if id, ok := w.identity[s][idx]; ok {
		return id
	}
	return idx
}

// observeRows reads which indices have a row this tick: an index that
// had a row earlier in the probe, lost it, and has one again without a
// surge or a move writing it is a fresh Instance under an old number.
func (w *moveWorld) observeRows(s *scaleSide) {
	now := map[int32]bool{}
	for _, r := range w.rows(s) {
		now[r.Index] = true
	}
	if w.prevRows[s] != nil {
		for idx := range now {
			if w.prevRows[s][idx] || !w.everRows[s][idx] || w.mappedThisTick(s, idx) {
				continue
			}
			w.fresh[s]++
			w.identity[s][idx] = moveFreshIdentityBase + w.fresh[s]
		}
	}
	for idx := range now {
		w.everRows[s][idx] = true
	}
	w.prevRows[s] = now
}

// mappedThisTick reports an index whose row points at another row as
// the Instance it replaces, or stands as a gang surge's target: a surge
// or a move pair standing this tick.
func (w *moveWorld) mappedThisTick(s *scaleSide, idx int32) bool {
	rows := w.rows(s)
	for i := range rows {
		r := &rows[i]
		if r.Index != idx || r.Operation == nil {
			continue
		}
		if (r.Operation.SurgeIndex != nil && *r.Operation.SurgeIndex != idx) || (r.Operation.Type == workloadtypes.InstanceOperationUpdate && r.Operation.Step == workloadtypes.UpdateStepGangSurgeTarget) {
			return true
		}
	}
	return false
}

// indexOf is the index that stands for an Instance now, or the Instance's
// own number when no row does.
func (w *moveWorld) indexOf(s *scaleSide, id int32) int32 {
	for _, r := range w.rows(s) {
		if w.identityOf(s, r.Index) == id {
			return r.Index
		}
	}
	if id >= moveFreshIdentityBase {
		for idx, mapped := range w.identity[s] {
			if mapped == id {
				return idx
			}
		}
	}
	return id
}

// buildsBetween lists an Instance's builds numbered after from and, when
// until is set, up to it, in the order built.
func (w *moveWorld) buildsBetween(s *scaleSide, identity int32, from, until int) []moveBuild {
	var out []moveBuild
	for _, b := range w.builds[s] {
		if b.identity == identity && b.seq > from && (until == 0 || b.seq <= until) {
			out = append(out, b)
		}
	}
	return out
}

// buildsOf describes every build of an Instance this probe saw, as
// index:revision@build-number in the order built, for a reading's text.
func (w *moveWorld) buildsOf(s *scaleSide, id int32) string {
	var parts []string
	for _, b := range w.builds[s] {
		if b.identity == id {
			parts = append(parts, fmt.Sprintf("%d:%s@%d", b.index, b.revision, b.seq))
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

// buildsSince lists an Instance's builds after the verb that moved it,
// or after the probe's first verb for an Instance no verb moved.
func (w *moveWorld) buildsSince(s *scaleSide, identity int32) []moveBuild {
	from := w.buildMark[s]
	if at, ok := w.movedAt[s][identity]; ok && at > from {
		from = at
	}
	return w.buildsBetween(s, identity, from, 0)
}

// markBuilds remembers the build number the probe's first verb lands
// at, so the builds after it are the probe's.
func (w *moveWorld) markBuilds() {
	for _, s := range w.sides {
		w.buildMark[s] = w.seq[s]
	}
}

// observeRecords reads every move record of a side: a phase that steps
// back, a text that alternates, and a record past its deadline.
func (w *moveWorld) observeRecords(s *scaleSide) {
	now := w.clk.Now()
	for _, rec := range s.h.migrations {
		trail := w.records[rec.RequestUUID]
		if trail == nil {
			trail = &moveRecordTrail{side: s, completedAt: -1}
			w.records[rec.RequestUUID] = trail
		}
		w.observePhase(trail, rec)
		if trail.pairSeq == 0 && rec.Phase != workloadtypes.MigrationPhaseAccepted {
			trail.pairSeq = w.seqAtTick[s]
		}
		w.observeText(trail, rec)
		if rec.Phase.Terminal() {
			if trail.completedAt < 0 {
				trail.completedAt = w.mticks
				trail.completedSeq = w.seq[s]
				w.readCompletion(s, rec)
			}
			continue
		}
		w.readUnnamedWait(s, rec, trail)
		w.readWaitAfterTheInstanceLeft(s, rec, trail)
		w.readOverdue(s, rec, trail, now)
	}
}

// observePhase appends a record's phase to its trail when it changes and
// reads a phase that steps back.
func (w *moveWorld) observePhase(trail *moveRecordTrail, rec workloadtypes.MigrationRecord) {
	n := len(trail.phases)
	if n > 0 && trail.phases[n-1] == rec.Phase {
		return
	}
	if n > 0 && !workloadtypes.MigrationPhaseAtOrPast(rec.Phase, trail.phases[n-1]) {
		w.note("move-record-steps-back", "the move %s stepped back from %s to %s: %s", rec.RequestUUID, trail.phases[n-1], rec.Phase, w.picture())
	}
	trail.phases = append(trail.phases, rec.Phase)
}

// observeText appends a record's text to its trail when it changes and
// reads a text that alternates.
func (w *moveWorld) observeText(trail *moveRecordTrail, rec workloadtypes.MigrationRecord) {
	if n := len(trail.messages); n > 0 && trail.messages[n-1] == rec.Message {
		return
	}
	trail.messages = append(trail.messages, rec.Message)
	if alternates(trail.messages) {
		w.note("move-text-opens-and-closes-on-alternate-passes", "the move %s rewrites its text on alternate passes: %q then %q: %s", rec.RequestUUID, trail.messages[len(trail.messages)-2], rec.Message, w.picture())
	}
}

// readOverdue counts the ticks an open record stands past its deadline on
// a side that is not paused, and names it once the count is reached.
func (w *moveWorld) readOverdue(s *scaleSide, rec workloadtypes.MigrationRecord, trail *moveRecordTrail, now time.Time) {
	if rec.Deadline.IsZero() || !now.After(rec.Deadline.Time) || s.paused {
		trail.overdue = 0
		return
	}
	trail.overdue++
	if trail.overdue != moveOverdueTicks {
		return
	}
	class := "move-past-its-deadline-still-open"
	switch {
	case w.overdueClass != nil && w.overdueClass(s, rec) != "":
		class = w.overdueClass(s, rec)
	case strings.Contains(rec.Message, "source teardown"):
		class = "move-past-its-deadline-waiting-on-its-source-teardown"
	}
	w.note(class, "the move %s stands %s past its deadline in phase %s with the text %q: %s", rec.RequestUUID, now.Sub(rec.Deadline.Time).Round(time.Second), rec.Phase, rec.Message, w.picture())
}

// readUnnamedWait counts the ticks a record stands open with no text on
// a side that is not paused, and names the wait once it has lasted, by
// the scene the request was filed in: behind the roll's surges, or under
// a pause with its source's node lost before the resume; any other
// unnamed wait is named plainly.
func (w *moveWorld) readUnnamedWait(s *scaleSide, rec workloadtypes.MigrationRecord, trail *moveRecordTrail) {
	if rec.Message != "" || s.paused {
		trail.unnamed = 0
		return
	}
	trail.unnamed++
	if trail.unnamed > trail.unnamedMost {
		trail.unnamedMost = trail.unnamed
	}
	if trail.unnamed != moveUnnamedTicks {
		return
	}
	class := "move-waits-unnamed"
	f := w.factsOf(rec.RequestUUID)
	switch {
	case w.unnamedClass != nil && w.unnamedClass(s, rec) != "":
		class = w.unnamedClass(s, rec)
	case f != nil && f.surgeInFlight:
		class = "move-waits-unnamed-behind-the-rolls-surges"
	case f != nil && f.paused && len(w.lostNodes[s]) > 0:
		class = "move-waits-unnamed-after-the-resume-with-its-source-node-lost"
	}
	w.note(class, "the move %s has stood in phase %s with no text for %d ticks: %s", rec.RequestUUID, rec.Phase, moveUnnamedTicks, w.picture())
}

// readWaitAfterTheInstanceLeft names, once per record, a move that waits
// for the roll's surge replacements while the Instance it names already
// runs off the node it was asked to leave: the node was lost and the
// Instance rebuilt elsewhere, so answering the request allocates nothing
// and the wait buys nothing.
func (w *moveWorld) readWaitAfterTheInstanceLeft(s *scaleSide, rec workloadtypes.MigrationRecord, trail *moveRecordTrail) {
	if trail.leftNamed || !moveWaitsForTheRoll(&rec) || rec.FromNode == "" || !w.lostNodes[s][rec.FromNode] {
		return
	}
	for _, pod := range s.h.podsOf(rec.SourceInstance) {
		if query.RoutedRunner(pod) && pod.Spec.NodeName != rec.FromNode {
			trail.leftNamed = true
			w.note("move-waits-for-the-roll-after-its-instance-left-the-node", "the move %s waits for the roll while %s Instance %d already runs on %s, off %s: %s", rec.RequestUUID, s.comp, rec.SourceInstance, pod.Spec.NodeName, rec.FromNode, w.picture())
			return
		}
	}
}

// waitedBehindTheSurges reports a record filed while the roll's surges
// held the budget, or one that stood open with no text long enough to be
// read as waiting unnamed.
func (w *moveWorld) waitedBehindTheSurges(uuid string) bool {
	if f := w.factsOf(uuid); f != nil && f.surgeInFlight {
		return true
	}
	trail := w.records[uuid]
	return trail != nil && trail.unnamedMost >= moveUnnamedTicks
}

// alternates reports a list whose last four entries read A, B, A, B.
func alternates(texts []string) bool {
	n := len(texts)
	return n >= moveFlapWindow && texts[n-1] == texts[n-3] && texts[n-2] == texts[n-4] && texts[n-1] != texts[n-2]
}

// readCompletion reads a move record the pass just closed: a Completed
// move's replacement serves off the node the move left, and a Failed
// move names a reason this model knows.
func (w *moveWorld) readCompletion(s *scaleSide, rec workloadtypes.MigrationRecord) {
	switch rec.Phase {
	case workloadtypes.MigrationPhaseCompleted:
		if rec.SurgeInstance == nil {
			w.note("move-answered-without-moving", "the move %s closed Completed with no replacement built: %s", rec.RequestUUID, rec.Message)
			return
		}
		k := *rec.SurgeInstance
		row := s.h.instance(k)
		if row == nil || row.Phase != workloadtypes.InstancePhaseReady {
			w.note("replacement-not-ready-at-completion", "the move %s closed Completed while its replacement %d reads %v: %s", rec.RequestUUID, k, row, w.picture())
		}
		for _, pod := range s.h.podsOf(k) {
			if pod.Spec.NodeName == rec.FromNode {
				w.note("replacement-on-the-node-the-move-left", "the move %s closed Completed with its replacement pod %s on %s, the node it was to leave: %s", rec.RequestUUID, pod.Name, rec.FromNode, w.picture())
			}
		}
	case workloadtypes.MigrationPhaseFailed:
		switch {
		case w.failedClass != nil && w.failedClass(s, rec) != "":
			w.note(w.failedClass(s, rec), "the move %s closed Failed: %s: %s", rec.RequestUUID, rec.Message, w.picture())
		case strings.Contains(rec.Message, "which is held after its attempts"):
			w.note("move-refused-source-revision-held", "the move %s was refused: %s", rec.RequestUUID, rec.Message)
		case strings.Contains(rec.Message, "has not started"):
			w.note("move-refused-revision-not-started", "the move %s was refused: %s", rec.RequestUUID, rec.Message)
		case strings.Contains(rec.Message, "surge never allocated"):
			class := "move-expired-before-it-started"
			if f := w.factsOf(rec.RequestUUID); f != nil && f.surgeInFlight {
				class = "move-requested-during-a-surge-expires-with-no-wait-named"
			} else if w.lostNodes[s][rec.FromNode] {
				class = "move-expired-while-its-source-was-down"
			}
			w.note(class, "the move %s expired: %s: %s", rec.RequestUUID, rec.Message, w.picture())
		case strings.Contains(rec.Message, "while following the Instance rebuilt as"):
			w.note("move-expires-while-following-a-gang-surge", "the move %s expired: %s: %s", rec.RequestUUID, rec.Message, w.picture())
		case strings.Contains(rec.Message, "source drain incomplete") && w.lostNodes[s][rec.FromNode]:
			w.note("move-expired-draining-a-source-on-a-lost-kubelet", "the move %s expired: %s: %s", rec.RequestUUID, rec.Message, w.picture())
		case strings.Contains(rec.Message, "surge pods never became ready") && w.waitedBehindTheSurges(rec.RequestUUID):
			w.note("move-expired-after-waiting-behind-the-rolls-surges", "the move %s waited behind the roll's surges for most of its deadline, started late and expired: %s: %s", rec.RequestUUID, rec.Message, w.picture())
		case strings.Contains(rec.Message, "surge pods never became ready"):
			w.note("move-expired-waiting-for-its-replacement", "the move %s expired: %s: %s", rec.RequestUUID, rec.Message, w.picture())
		case strings.Contains(rec.Message, "source instance has no live pods"):
			w.note("move-refused-source-without-pods", "the move %s was refused: %s", rec.RequestUUID, rec.Message)
		case strings.Contains(rec.Message, "source InstanceStatus missing"):
			w.note("move-refused-source-gone", "the move %s was refused: %s", rec.RequestUUID, rec.Message)
		case (strings.Contains(rec.Message, "ImagePullBackOff") || strings.Contains(rec.Message, "pulling image")) && s.h.findBlock(s.h.target.Name) != nil:
			w.note("move-failed-on-a-target-no-pod-can-pull", "the move %s built its replacement on the roll's target, which no pod can pull while the target's retry record waits, and closed Failed: %s: %s", rec.RequestUUID, rec.Message, w.picture())
		default:
			w.note("move-failed", "the move %s closed Failed: %s: %s", rec.RequestUUID, rec.Message, w.picture())
		}
	}
}

// observeHold reads a side's hold against the rows it was read with: a
// hold that opens and closes on alternate ticks while the rows stand
// still is a loop.
func (w *moveWorld) observeHold(s *scaleSide) {
	trail := w.holds[s]
	gate := "none"
	if s.h.hold != nil {
		gate = string(s.h.hold.Gate)
	}
	trail.gates = append(trail.gates, gate)
	trail.rows = append(trail.rows, w.rowsKey(s))
	if len(trail.gates) > moveFlapWindow {
		trail.gates, trail.rows = trail.gates[1:], trail.rows[1:]
	}
	if len(trail.gates) < moveFlapWindow || !alternates(trail.gates) {
		return
	}
	for _, rows := range trail.rows {
		if rows != trail.rows[0] {
			return
		}
	}
	w.note("hold-opens-and-closes-on-alternate-passes", "%s writes a %s hold on one pass and none on the next while its rows stand still: %s", s.comp, trail.gates[len(trail.gates)-1], w.picture())
}

// rowsKey is a side's rows alone, as the hold readings compare them.
func (w *moveWorld) rowsKey(s *scaleSide) string {
	var b strings.Builder
	for _, r := range w.rows(s) {
		fmt.Fprintf(&b, "%d:%s:%s>%s", r.Index, r.Phase, w.short(s, r.RunningRevision), w.short(s, r.TargetRevision))
		if r.Operation != nil {
			fmt.Fprintf(&b, ":%s/%s/%s", r.Operation.Type, r.Operation.Step, r.Operation.Waiting)
		}
		b.WriteString(";")
	}
	return b.String()
}

// podsKey is a side's pods as the stall reading compares them: name,
// phase, readiness, serving gate and deletion.
func (w *moveWorld) podsKey(s *scaleSide) string {
	var b strings.Builder
	for _, pod := range w.pods(s) {
		fmt.Fprintf(&b, "%s:%s:%t:%t:%t;", pod.Name, pod.Status.Phase, podConditionTrue(pod, corev1.PodReady), podreadiness.IsServing(pod), pod.DeletionTimestamp != nil)
	}
	return b.String()
}

// observeDeadlines reads every attempt of the roll or a scale-up past its
// deadline with nothing named, on a side that stands still: no wait
// token, no pause, no hold, and neither the rows nor the pods changing.
// A move's own rows carry the record's deadline, not their own, and a
// repair keeps its operation past the deadline by design; both are left
// to the record and the repair readings.
func (w *moveWorld) observeDeadlines(s *scaleSide) {
	now := w.clk.Now()
	if key := w.rowsKey(s) + w.podsKey(s); key != w.deadlineKey[s] || s.paused || s.h.hold != nil || w.waitsOnSourceTeardown(s) {
		w.deadlineKey[s] = key
		w.overdue[s] = map[int32]int{}
		return
	}
	for _, r := range w.rows(s) {
		op := r.Operation
		if op == nil || op.Deadline.IsZero() || !now.After(op.Deadline.Time) || op.Waiting != "" ||
			r.Phase == workloadtypes.InstancePhaseFailed || (op.Type != workloadtypes.InstanceOperationUpdate && op.Type != workloadtypes.InstanceOperationCreate) ||
			(w.mcfg.forceDeleteOff && w.podOnALostNode(s, w.identityOf(s, r.Index))) {
			delete(w.overdue[s], r.Index)
			continue
		}
		w.overdue[s][r.Index]++
		if w.overdue[s][r.Index] == moveOverdueTicks {
			w.note("attempt-past-its-deadline-with-nothing-named", "%s Instance %d's %s stands %s past its deadline with no wait named and no hold on the side, and nothing on the side moves: %s", s.comp, r.Index, op.Type, now.Sub(op.Deadline.Time).Round(time.Second), w.picture())
		}
	}
}

// waitsOnSourceTeardown reports a move record of side s open on a source
// pod the kubelet never removed: the wait is named on the record, and
// what the roll does beside it is the stall reading's to read.
func (w *moveWorld) waitsOnSourceTeardown(s *scaleSide) bool {
	for _, rec := range s.h.migrations {
		if !rec.Phase.Terminal() && strings.Contains(rec.Message, "source teardown") {
			return true
		}
	}
	return false
}

// observeStall reads a side with Instances still to roll or to create
// beyond the waiting move's own pair, whose rows and pods stand unchanged
// with no hold while one of its moves waits on its source teardown: the
// wait is the move's unless an attempt of the roll itself has a pod on a
// node that stopped reporting.
func (w *moveWorld) observeStall(s *scaleSide) {
	waiting := w.waitsOnSourceTeardown(s)
	key := w.rowsKey(s) + w.podsKey(s)
	now := w.clk.Now()
	ownDone := waiting && w.sideDoneAroundItsMove(s)
	sibling := w.siblingOnTheGroupGate(s)
	if !waiting || w.doneAroundTheMove(s) || (ownDone && sibling == nil) || s.h.hold != nil || s.paused || key != w.stallKey[s] {
		w.stall[s], w.stallKey[s], w.stallSince[s] = 0, key, now
		return
	}
	if w.stall[s] > 0 || now.Sub(w.stallSince[s]) < moveStallClock {
		return
	}
	w.stall[s] = 1
	if ownDone {
		w.note("group-gate-waits-on-a-move-held-by-its-source-teardown", "%s is done around a move waiting on its source teardown, and %s has waited %s on the group gate for the batch that move's pair is in: %s", s.comp, sibling.comp, now.Sub(w.stallSince[s]).Round(time.Second), w.picture())
		return
	}
	class := "roll-waits-behind-a-move-on-a-lost-kubelet"
	for _, r := range w.rows(s) {
		if r.Operation == nil || r.Operation.Type != workloadtypes.InstanceOperationUpdate {
			continue
		}
		for _, pod := range w.pods(s) {
			if idx, ok := query.InstanceIdxFromLabels(pod); ok && idx == r.Index && w.lostNodes[s][pod.Spec.NodeName] {
				class = "roll-waits-behind-its-own-attempt-on-a-lost-kubelet"
			}
		}
	}
	w.note(class, "%s stands still for %s with Instances left to roll, no hold written, while a move waits on a pod the kubelet never removed: %s", s.comp, now.Sub(w.stallSince[s]).Round(time.Second), w.picture())
}

// pick is the Instance a probe's target names on a side, or false when
// the side has none.
func (w *moveWorld) pick(s *scaleSide, target moveTarget) (int32, bool) {
	held := w.heldIndices(s)
	byIndex := w.livePodsByIndex(s)
	placed := func(idx int32) bool {
		for _, pod := range byIndex[idx] {
			if query.RoutedRunner(pod) && pod.Spec.NodeName != "" && pod.DeletionTimestamp == nil {
				return true
			}
		}
		return false
	}
	chosen, found := int32(0), false
	for _, r := range w.rows(s) {
		steady := r.Phase == workloadtypes.InstancePhaseReady && r.Operation == nil && placed(r.Index)
		switch target {
		case targetBehind:
			if steady && r.RunningRevision != s.h.target.Name && !held[r.Index] && !found {
				chosen, found = r.Index, true
			}
		case targetDone:
			if steady && r.RunningRevision == s.h.target.Name {
				chosen, found = r.Index, true
			}
		case targetHeld:
			if steady && held[r.Index] && !found {
				chosen, found = r.Index, true
			}
		case targetOpen:
			if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationUpdate && placed(r.Index) && !found {
				chosen, found = r.Index, true
			}
		}
	}
	return chosen, found
}

// rollCouldTake reports whether the roll could take Instance idx of side
// s from where it stands: a target the Instance does not run, the side
// not paused, the target's retry record not Held, and neither the
// partition nor the canary step's count holding the Instance. The
// decided rule for a move's replacement.
func (w *moveWorld) rollCouldTake(s *scaleSide, idx int32) bool {
	row := s.h.instance(idx)
	if row == nil || row.RunningRevision == "" || row.RunningRevision == s.h.target.Name || s.paused || s.h.heldOn(s.h.target.Name) {
		return false
	}
	return !w.heldIndices(s)[idx]
}

// targetInBackoff reports the target's retry record waiting out a delay.
func (w *moveWorld) targetInBackoff(s *scaleSide) bool {
	b := s.h.findBlock(s.h.target.Name)
	return b != nil && b.State == workloadtypes.RetryBlockBackoff
}

// targetHeld reports the target's retry record held: its schedule is spent.
func (w *moveWorld) targetHeld(s *scaleSide) bool {
	b := s.h.findBlock(s.h.target.Name)
	return b != nil && b.State == workloadtypes.RetryBlockHeld
}

// surgeInFlight reports an attempt of side s with a surge replacement in
// flight: the steps that build or drain beside a replacement.
func (w *moveWorld) surgeInFlight(s *scaleSide) bool {
	for _, r := range w.rows(s) {
		if r.Operation == nil || r.Operation.Type != workloadtypes.InstanceOperationUpdate {
			continue
		}
		switch r.Operation.Step {
		case workloadtypes.UpdateStepSurge, workloadtypes.UpdateStepSurgeDrain, workloadtypes.UpdateStepSurgeDrainSettle, workloadtypes.UpdateStepGangSurgeTarget:
			return true
		}
	}
	return false
}

// fileMoveRequest files an operator's move of Instance idx of side s off
// the node its routable pod runs on, as the accept pass records it: a
// Manual record, Accepted, with the deadline the ready timeout sets.
func (w *moveWorld) fileMoveRequest(s *scaleSide, idx int32) string {
	fromNode := ""
	for _, pod := range s.h.podsOf(idx) {
		if query.RoutedRunner(pod) && pod.Spec.NodeName != "" {
			fromNode = pod.Spec.NodeName
		}
	}
	w.requests++
	uuid := fmt.Sprintf("move-%s-%d-%d", s.comp, idx, w.requests)
	now := metav1.NewTime(w.clk.Now())
	row := s.h.instance(idx)
	held := w.heldIndices(s)
	w.request = &moveRequestFacts{side: s, uuid: uuid, source: idx, identity: w.identityOf(s, idx), fromNode: fromNode, rollCouldTake: w.rollCouldTake(s, idx), surgeInFlight: w.surgeInFlight(s),
		paused: s.paused, held: held[idx], heldBefore: held, holdAtRequest: s.h.hold != nil, seqAt: w.seq[s], tick: w.mticks, eventsAt: len(s.h.events)}
	if row != nil {
		w.request.sourceRev = w.short(s, row.RunningRevision)
	}
	w.filed = append(w.filed, w.request)
	s.h.migrations = append(s.h.migrations, workloadtypes.MigrationRecord{
		RequestUUID:    uuid,
		Trigger:        workloadtypes.MigrationTriggerManual,
		Phase:          workloadtypes.MigrationPhaseAccepted,
		SourceInstance: idx,
		FromNode:       fromNode,
		Reason:         "NodeDrain",
		StartedAt:      now,
		Deadline:       metav1.NewTime(now.Add(s.h.lifecycle.InstanceReadyTimeout.Duration)),
	})
	return uuid
}

// loseNodeUnder stops the kubelet under Instance idx's routable pod and
// records as moved every Instance the loss leaves with no routable pod off
// that node, by identity, so a gang surge or a move replacement standing
// elsewhere counts as the Instance's; every Instance with any pod on the
// node is touched. A fresh node joins for the one lost, as a cluster
// replaces a node, so the readings are about the engine and never about
// room.
func (w *moveWorld) loseNodeUnder(s *scaleSide, idx int32) {
	byIndex := w.livePodsByIndex(s)
	node := placedNode(byIndex[idx])
	if node == "" {
		return
	}
	onNode, elsewhere := map[int32][]int32{}, map[int32]bool{}
	for index, pods := range byIndex {
		id := w.identityOf(s, index)
		for _, pod := range pods {
			switch {
			case pod.Spec.NodeName == node:
				onNode[id] = append(onNode[id], index)
			case query.RoutedRunner(pod):
				elsewhere[id] = true
			}
		}
	}
	for id, indices := range onNode {
		w.touched[s][id] = true
		if !elsewhere[id] {
			w.markMoved(s, indices[0])
		}
	}
	w.lostNodes[s][node] = true
	s.h.failNode(node)
	s.h.addNodes(fmt.Sprintf("%s-node-%d", s.comp, len(s.h.nodeNames)))
}

// replacementElsewhere reports a routable pod under another index that
// stands for the same Instance as idx: a surge or a move replacement
// already on its way, so the pods of idx were leaving anyway.
func (w *moveWorld) replacementElsewhere(s *scaleSide, idx int32) bool {
	id := w.identityOf(s, idx)
	for index, pods := range w.livePodsByIndex(s) {
		if index == idx || w.identityOf(s, index) != id {
			continue
		}
		for _, pod := range pods {
			if query.RoutedRunner(pod) {
				return true
			}
		}
	}
	return false
}

// markMoved records the Instance index stands for as one a verb took the
// pods of, with whether the roll could take it at that moment, and the
// build number and tick it happened at, so the builds after this verb are
// read as its rebuilds.
func (w *moveWorld) markMoved(s *scaleSide, idx int32) {
	id := w.identityOf(s, idx)
	w.moved[s][id] = true
	w.touched[s][id] = true
	w.rollTakes[s][id] = w.rollCouldTake(s, idx)
	w.movedAt[s][id] = w.seq[s]
	w.movedTick[s][id] = w.mticks
}

// land plays the probe's verb on the world: the move request, the node
// loss, the pod loss, or the request followed by the node loss under its
// source once the record drains it. It reports false when the target
// Instance does not exist at this moment.
func (w *moveWorld) land(p moveProbe) bool {
	s := w.sides[p.side]
	idx, ok := w.pick(s, p.target)
	if !ok {
		return false
	}
	w.path = append(w.path, fmt.Sprintf("%s Instance %d of %s", p.verb, idx, s.comp))
	if w.landed == 0 {
		w.markBuilds()
	}
	w.landed++
	w.probe, w.probeTick = p, w.mticks
	w.backoffAtMove = w.targetInBackoff(s)
	switch p.verb {
	case verbRequest:
		w.fileMoveRequest(s, idx)
	case verbNodeLost:
		w.loseNodeUnder(s, idx)
	case verbPodsLost:
		if w.replacementElsewhere(s, idx) {
			w.touched[s][w.identityOf(s, idx)] = true
		} else {
			w.markMoved(s, idx)
		}
		s.h.losePods(idx)
	case verbRequestSourceNodeLost:
		// Under a pause the record cannot reach its drain, so the node
		// goes at once, under a move still waiting to start.
		uuid := w.fileMoveRequest(s, idx)
		for i := 0; i < moveDrainingTicks && !s.paused; i++ {
			rec := s.h.migration(uuid)
			if rec == nil || rec.Phase.Terminal() || workloadtypes.MigrationPhaseAtOrPast(rec.Phase, workloadtypes.MigrationPhaseDraining) {
				break
			}
			w.tick()
		}
		if rec := s.h.migration(uuid); rec != nil && !rec.Phase.Terminal() {
			w.loseNodeUnder(s, idx)
		}
		if p.moment == whenIdle {
			// With nothing under way the push is what lands beside the
			// waiting move: the roll must go on around it.
			w.push(fixedImage)
			w.request.pushedAfter = true
		}
	}
	return true
}

// classifyProbe reads the settled end of a probe beside the scaling
// model's own reading of it. A settled end with a move still waiting on a
// source pod the kubelet never removed is read as the move's: the rest of
// the side done around it, or the rest of the side waiting behind it. A
// side a hold or a pause keeps still is read by the scaling model's end
// reading, since the hold, not the move, is what stops the roll there.
func (w *moveWorld) classifyProbe(settled bool) {
	switch {
	case !settled || w.complete():
		w.classifyEnd(settled)
	case w.waitingOnTeardown() != nil && w.waitingOnTeardown().h.hold == nil && !w.waitingOnTeardown().paused:
		s := w.waitingOnTeardown()
		switch {
		case w.doneAroundTheMove(s):
			w.note("move-waits-for-good-on-its-source-teardown", "the move waits for a pod the kubelet never removes while everything else is done: %s", w.picture())
		case w.sideDoneAroundItsMove(s) && w.siblingOnTheGroupGate(s) != nil:
			w.note("group-gate-waits-on-a-move-held-by-its-source-teardown", "%s is done around a move waiting on its source teardown, and %s waits on the group gate for the batch that move's pair is in: %s", s.comp, w.siblingOnTheGroupGate(s).comp, w.picture())
		case w.sideDoneAroundItsMove(s):
			w.classifyEnd(settled)
		default:
			w.note("roll-waits-behind-a-move-on-a-lost-kubelet", "%s stands with Instances left to roll or create while a move waits for a pod the kubelet never removes: %s", s.comp, w.picture())
		}
	case w.mcfg.forceDeleteOff && w.anyPodOnALostNode():
		w.note("roll-waits-on-instances-left-on-a-lost-kubelet-with-no-force-delete", "the world stands still with pods left on a node that stopped reporting and no force-delete policy to free them: %s", w.picture())
	default:
		w.classifyEnd(settled)
	}
	if !settled {
		return
	}
	w.readRequestEnd()
	w.readRebuilds()
	w.readEvents()
}

// waitingOnTeardown is the side with a move record waiting on its source
// teardown, nil when none.
func (w *moveWorld) waitingOnTeardown() *scaleSide {
	for _, s := range w.sides {
		for _, rec := range s.h.migrations {
			if !rec.Phase.Terminal() && strings.Contains(rec.Message, "source teardown") {
				return s
			}
		}
	}
	return nil
}

// doneAroundTheMove reports every side at rest but for the pair of a
// waiting move: every other row Ready on the target or held by the
// partition, the count met once the pair's replacement is counted.
func (w *moveWorld) doneAroundTheMove(side *scaleSide) bool {
	for _, s := range w.sides {
		if s == side {
			if !w.sideDoneAroundItsMove(s) {
				return false
			}
			continue
		}
		if !w.sideComplete(s) {
			return false
		}
	}
	return true
}

// sideDoneAroundItsMove reports side s done but for the pair of a waiting
// move: every other row Ready with no attempt open, on the target or held
// by the partition.
func (w *moveWorld) sideDoneAroundItsMove(s *scaleSide) bool {
	pair := map[int32]bool{}
	for _, rec := range s.h.migrations {
		if !rec.Phase.Terminal() && rec.SurgeInstance != nil {
			pair[rec.SourceInstance], pair[*rec.SurgeInstance] = true, true
		}
	}
	held := w.heldIndices(s)
	rows := w.rows(s)
	if int32(len(rows))-int32(len(pair))+1 != s.h.replicas {
		return false
	}
	for _, r := range rows {
		if pair[r.Index] {
			continue
		}
		if r.Phase != workloadtypes.InstancePhaseReady || r.Operation != nil || (r.RunningRevision != s.h.target.Name && !held[r.Index]) {
			return false
		}
	}
	return true
}

// siblingOnTheGroupGate is a side other than s that a coordination hold
// keeps waiting, nil when none.
func (w *moveWorld) siblingOnTheGroupGate(s *scaleSide) *scaleSide {
	for _, o := range w.sides {
		if o == s || o.h.hold == nil {
			continue
		}
		switch o.h.hold.Gate {
		case workloadtypes.RolloutHoldGateRatio, workloadtypes.RolloutHoldGateSequential, workloadtypes.RolloutHoldGatePairing:
			return o
		}
	}
	return nil
}

// factsOf is what the model read when the request with this id was
// filed, nil for a record the probe did not file.
func (w *moveWorld) factsOf(uuid string) *moveRequestFacts {
	for _, f := range w.filed {
		if f.uuid == uuid {
			return f
		}
	}
	return nil
}

// readRequestEnd reads every record this probe filed: terminal with the
// replacement built once on the decided revision, or a wait it names.
func (w *moveWorld) readRequestEnd() {
	for _, req := range w.filed {
		w.readOneRequestEnd(req)
	}
}

func (w *moveWorld) readOneRequestEnd(req *moveRequestFacts) {
	s := req.side
	rec := s.h.migration(req.uuid)
	if rec == nil {
		w.note("move-record-gone", "the move of %s Instance %d has no record left: %s", s.comp, req.source, w.picture())
		return
	}
	if !rec.Phase.Terminal() {
		switch {
		case s.paused:
			w.note("move-waits-under-the-pause", "the move %s waits in phase %s while the roll is paused: %s", rec.RequestUUID, rec.Phase, rec.Message)
		case strings.Contains(rec.Message, "source teardown"):
			w.note("move-waits-for-good-on-its-source-teardown", "the move %s waits for a pod the kubelet never removes: %s", rec.RequestUUID, rec.Message)
		case strings.Contains(rec.Message, "waiting"):
			w.note("move-waits-with-its-text-at-the-end", "the move %s waits in phase %s and says why: %s: %s", rec.RequestUUID, rec.Phase, rec.Message, w.picture())
		default:
			w.note("move-never-ends-unnamed", "the move %s stands in phase %s with nothing named: %q: %s", rec.RequestUUID, rec.Phase, rec.Message, w.picture())
		}
		return
	}
	if rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance == nil {
		return
	}
	w.readHandedHold(req, rec)
	builds, cut := w.requestBuilds(req)
	target := w.short(s, s.h.target.Name)
	hold := req.holdAtRequest || s.h.hold != nil
	couldTake := req.couldTakeAtStart()
	srcRev := req.sourceRevisionAtStart()
	thenRolled := len(builds) == 2 && builds[0].revision == srcRev && builds[1].revision == target
	switch {
	case len(builds) == 0:
		w.note("move-completed-without-a-build", "the move %s closed Completed and no pod of its replacement %d was ever built: %s; builds %s", rec.RequestUUID, *rec.SurgeInstance, w.picture(), w.buildsOf(s, req.identity))
	case len(builds) == 1 && req.started && req.sourceRevAtStart != req.sourceRev && builds[0].revision == req.sourceRevAtStart:
		w.note("move-built-once-after-the-roll-took-its-source", "the move %s waited for its source to stand steady while the roll took it from %s to %s, then built its replacement once, on %s", rec.RequestUUID, req.sourceRev, req.sourceRevAtStart, builds[0].revision)
	case len(builds) == 1 && couldTake && builds[0].revision == target && hold:
		w.note("move-built-on-the-roll-target-behind-a-hold", "the move %s built its replacement once, on %s, the roll's target, while a hold kept the roll off the side: %s", rec.RequestUUID, target, w.picture())
	case len(builds) == 1 && couldTake && builds[0].revision == target:
		w.note("move-built-once-on-the-roll-target", "the move %s built its replacement once, on %s, the roll's target, and the roll never rebuilt it", rec.RequestUUID, target)
	case len(builds) == 1 && !couldTake && builds[0].revision != srcRev:
		w.note("move-built-on-the-roll-target-while-the-roll-could-not-take-it", "the move %s built its replacement once, on %s, while the roll could not take the source, which ran %s: %s; builds %s", rec.RequestUUID, builds[0].revision, srcRev, w.picture(), w.buildsOf(s, req.identity))
	case len(builds) == 1 && !couldTake:
		w.note("move-built-once-on-its-revision", "the move %s built its replacement once, on %s, the revision the source ran, while no roll could take the source", rec.RequestUUID, builds[0].revision)
	case len(builds) == 1 && cut:
		w.note("move-built-once-then-a-later-verb-took-the-instance", "the move %s built its replacement once, on %s, and a later loss or move took the Instance before the roll reached the replacement", rec.RequestUUID, builds[0].revision)
	case len(builds) == 1 && w.heldIndices(s)[*rec.SurgeInstance]:
		w.note("move-built-once-on-its-revision-and-now-held", "the move %s built its replacement %d once, on %s; the replacement took a lower free index than the source and the partition's count now holds it, so the roll stands on it: %s", rec.RequestUUID, *rec.SurgeInstance, builds[0].revision, w.picture())
	case len(builds) == 1 && hold:
		w.note("move-built-once-on-its-revision-behind-a-hold", "the move %s built its replacement once, on %s, while a hold kept the roll off the side", rec.RequestUUID, builds[0].revision)
	case len(builds) == 1 && !w.complete():
		w.note("move-built-once-on-its-revision-while-the-roll-stands", "the move %s built its replacement once, on %s, and the roll, which has not finished, has not reached it: %s", rec.RequestUUID, builds[0].revision, w.picture())
	case len(builds) == 1:
		w.note("move-built-once-on-its-revision-under-an-open-roll", "the move %s built its replacement once on %s while the roll to %s was open and the roll never took it: %s; builds %s", rec.RequestUUID, builds[0].revision, target, w.picture(), w.buildsOf(s, req.identity))
	case thenRolled && couldTake:
		w.note("move-replacement-rebuilt-by-the-roll", "the move %s built its replacement on %s and the roll rebuilt it on %s: the Instance was built twice", rec.RequestUUID, builds[0].revision, target)
	case thenRolled && req.paused:
		w.note("move-built-on-its-revision-under-the-pause-then-rolled", "the move %s built its replacement on %s under the pause, and the roll took it after the resume", rec.RequestUUID, builds[0].revision)
	case thenRolled && req.held:
		w.note("move-of-a-held-instance-hands-its-hold-to-the-next-instance", "the move %s took Instance %d, which the partition held, built its replacement %d on %s, and the roll rebuilt it on %s once the hold had gone to another Instance: %s", rec.RequestUUID, req.source, *rec.SurgeInstance, builds[0].revision, target, w.picture())
	case thenRolled && req.pushedAfter:
		w.note("move-built-on-its-revision-then-rolled-by-a-later-push", "the move %s built its replacement on %s with no roll open, and the push that came after it rolled the replacement to %s", rec.RequestUUID, builds[0].revision, target)
	case thenRolled:
		w.note("move-built-on-its-revision-then-rolled", "the move %s built its replacement on %s while no roll could take the source, and the roll took it later: %s; builds %s", rec.RequestUUID, builds[0].revision, w.picture(), w.buildsOf(s, req.identity))
	default:
		var revs []string
		for _, b := range builds {
			revs = append(revs, b.revision)
		}
		w.note("move-replacement-built-again-and-again", "the move %s's replacement %d was built %d times (%s): %s; builds %s", rec.RequestUUID, *rec.SurgeInstance, len(builds), strings.Join(revs, ", "), w.picture(), w.buildsOf(s, req.identity))
	}
}

// requestBuilds lists the builds a move made for the Instance it moved
// and what the roll did with them: from the move's own replacement on,
// since a build of the source before it is the roll's or a repair's, up
// to a later loss or a later move's replacement on the same Instance,
// whose builds are that verb's. cut reports such a later verb.
func (w *moveWorld) requestBuilds(req *moveRequestFacts) (builds []moveBuild, cut bool) {
	s := req.side
	own := w.ownBuildSeq(req)
	if own == 0 {
		return nil, false
	}
	from := own - 1
	until := 0
	if at, ok := w.movedAt[s][req.identity]; ok && at > from {
		until = at
	}
	for _, f := range w.filed {
		if f.side != s || f.identity != req.identity || f == req {
			continue
		}
		if later := w.ownBuildSeq(f); later > own && (until == 0 || later-1 < until) {
			until = later - 1
		}
	}
	return w.buildsBetween(s, req.identity, from, until), until != 0
}

// ownBuildSeq is the build number of a move's own replacement: the first
// build of the moved Instance at the replacement's index after the pair
// was written; zero while no such pod has served.
func (w *moveWorld) ownBuildSeq(req *moveRequestFacts) int {
	rec := req.side.h.migration(req.uuid)
	trail := w.records[req.uuid]
	if rec == nil || rec.SurgeInstance == nil || trail == nil {
		return 0
	}
	for _, b := range w.builds[req.side] {
		if b.identity == req.identity && b.index == *rec.SurgeInstance && b.seq > trail.pairSeq {
			return b.seq
		}
	}
	return 0
}

// ownBuildSeqs is the set of build numbers that are the replacements of
// the moves this probe filed on one Instance.
func (w *moveWorld) ownBuildSeqs(s *scaleSide, id int32) map[int]bool {
	own := map[int]bool{}
	for _, f := range w.filed {
		if f.side != s || f.identity != id {
			continue
		}
		if seq := w.ownBuildSeq(f); seq != 0 {
			own[seq] = true
		}
	}
	return own
}

// readHandedHold reads where the partition's hold stands once a move of
// an Instance it held has completed: the hold is by count over the
// lowest-indexed Instances off the target, so it passes over the
// replacement, which takes the highest index, and lands on the next
// Instance the operator never picked.
func (w *moveWorld) readHandedHold(req *moveRequestFacts, rec *workloadtypes.MigrationRecord) {
	if !req.held || rec.SurgeInstance == nil {
		return
	}
	s := req.side
	now := w.heldIndices(s)
	if now[*rec.SurgeInstance] {
		return
	}
	var taken []int32
	for idx := range now {
		if !req.heldBefore[idx] {
			taken = append(taken, idx)
		}
	}
	if len(taken) == 0 {
		return
	}
	sort.Slice(taken, func(i, j int) bool { return taken[i] < taken[j] })
	w.note("move-of-a-held-instance-hands-its-hold-to-the-next-instance", "the move %s took Instance %d, which the partition held; the hold passed over its replacement %d and now holds Instance %v, which the operator never picked: %s", rec.RequestUUID, req.source, *rec.SurgeInstance, taken, w.picture())
}

// readRebuilds reads the Instances a node loss or a pod loss took the pods
// of: each is rebuilt once, on the roll's target when the roll could take
// it; and the Instances no verb touched are built at most once by the
// roll after the probe's first verb.
func (w *moveWorld) readRebuilds() {
	for _, s := range w.sides {
		live := map[int32]bool{}
		for _, r := range w.rows(s) {
			live[w.identityOf(s, r.Index)] = true
		}
		for id := range w.moved[s] {
			if live[id] {
				w.readLostInstance(s, id)
			}
		}
		if w.landed > 0 {
			w.readUntouchedBuilds(s)
		}
	}
}

// readLostInstance reads how an Instance whose pods a verb took came
// back: by a move's replacement, by one rebuild on the revision the roll
// decides, not at all for a reason the side names, or more than once.
func (w *moveWorld) readLostInstance(s *scaleSide, id int32) {
	target := w.short(s, s.h.target.Name)
	builds, byMove := w.rebuildsOf(s, id)
	if byMove {
		w.note("lost-instance-rebuilt-by-its-move", "%s Instance %d lost its pods and a move's replacement rebuilt it, on %s", s.comp, w.indexOf(s, id), builds[0].revision)
		return
	}
	switch {
	case len(builds) == 0 && w.mcfg.forceDeleteOff && w.podOnALostNode(s, id):
		w.note("lost-instance-waits-on-its-kubelet-with-no-force-delete", "%s Instance %d keeps a pod on a node that stopped reporting, and with no force-delete policy nothing rebuilds it: %s", s.comp, w.indexOf(s, id), w.picture())
	case len(builds) == 0 && w.failedUnderAHeldRevision(s, id):
		w.note("lost-instance-left-failed-under-a-held-revision", "%s Instance %d lost its pods while its row stood Failed under a held revision, and the hold names the way out: %s", s.comp, w.indexOf(s, id), w.picture())
	case len(builds) == 0 && w.leftToAHeldRoll(s, id):
		w.note("lost-instance-left-to-a-roll-the-group-gate-holds", "%s Instance %d, on the old version, lost a pod and was left to the roll, which the group gate holds: %s", s.comp, w.indexOf(s, id), w.picture())
	case len(builds) == 0:
		w.note("lost-instance-never-rebuilt", "%s Instance %d lost its pods and was never rebuilt: %s; builds %s", s.comp, w.indexOf(s, id), w.picture(), w.buildsOf(s, id))
	case len(builds) == 1 && builds[0].revision == target:
		w.note("rebuilt-once-on-the-roll-target", "%s Instance %d was rebuilt once, on %s", s.comp, w.indexOf(s, id), target)
	case len(builds) == 1:
		w.note("rebuilt-once-on-its-revision", "%s Instance %d was rebuilt once, on %s", s.comp, w.indexOf(s, id), builds[0].revision)
	case len(builds) == 2 && builds[1].revision == target && w.backoffAtMove && w.rollTakes[s][id]:
		w.note("rebuilt-on-its-revision-under-a-backoff-then-rolled", "%s Instance %d was rebuilt on %s while the target's retry record waited out its delay, and the roll rebuilt it on %s", s.comp, w.indexOf(s, id), builds[0].revision, target)
	case len(builds) == 2 && builds[1].revision == target && !w.rollTakes[s][id]:
		w.note("rebuilt-on-its-revision-then-rolled", "%s Instance %d was rebuilt on %s while no roll could take it, and the roll took it later", s.comp, w.indexOf(s, id), builds[0].revision)
	case len(builds) == 2 && builds[0].revision == target && builds[1].revision != target && w.targetHeld(s):
		w.note("rebuilt-on-the-roll-target-then-on-its-revision-once-held", "%s Instance %d was the roll's last start on %s, which failed and spent the retry schedule, and the repair rebuilt it on %s once the revision was held", s.comp, w.indexOf(s, id), target, builds[1].revision)
	default:
		w.note("lost-instance-rebuilt-twice", "%s Instance %d lost its pods and was built %d times (%s) where one build on the roll's target would do: %s; builds %s", s.comp, w.indexOf(s, id), len(builds), revisionList(builds), w.picture(), w.buildsOf(s, id))
	}
}

// readUntouchedBuilds reads the Instances the probe's verbs never
// touched: the roll builds each at most once after the probe's first verb.
func (w *moveWorld) readUntouchedBuilds(s *scaleSide) {
	read := map[int32]bool{}
	for _, b := range w.builds[s] {
		id := b.identity
		if read[id] || w.touched[s][id] || w.moveParty(s, id) {
			continue
		}
		read[id] = true
		if builds := w.buildsSince(s, id); len(builds) > 1 {
			w.note("instance-built-twice", "%s Instance %d, which the move did not touch, was built %d times after it (%s): %s", s.comp, w.indexOf(s, id), len(builds), revisionList(builds), w.picture())
		}
	}
}

// revisionList lists builds by revision, comma and space separated.
func revisionList(builds []moveBuild) string {
	var revs []string
	for _, b := range builds {
		revs = append(revs, b.revision)
	}
	return strings.Join(revs, ", ")
}

// rebuildsOf lists the builds of an Instance after the verb that took its
// pods, up to the first replacement a move of the same Instance built,
// whose story the move's own reading tells. byMove reports that the
// first build after the loss was such a replacement: the move, waiting
// when the pods went or filed before the repair reached them, is what
// rebuilt the Instance.
func (w *moveWorld) rebuildsOf(s *scaleSide, id int32) (builds []moveBuild, byMove bool) {
	from := w.buildMark[s]
	if at, ok := w.movedAt[s][id]; ok && at > from {
		from = at
	}
	own := w.ownBuildSeqs(s, id)
	for _, b := range w.buildsBetween(s, id, from, 0) {
		if own[b.seq] {
			if len(builds) == 0 {
				return []moveBuild{b}, true
			}
			break
		}
		builds = append(builds, b)
	}
	return builds, false
}

// leftToAHeldRoll reports an Instance off the roll's target whose repair
// yields to the update pass while a coordination or budget hold keeps
// that pass off the side: the row stands with no attempt open, nothing
// rebuilds it, and the hold names the other Component as the way out.
func (w *moveWorld) leftToAHeldRoll(s *scaleSide, id int32) bool {
	if s.h.hold == nil {
		return false
	}
	row := s.h.instance(w.indexOf(s, id))
	return row != nil && row.Operation == nil && row.RunningRevision != "" && row.RunningRevision != s.h.target.Name
}

// podOnALostNode reports an Instance with a pod, Terminating or not, on
// a node this probe stopped.
func (w *moveWorld) podOnALostNode(s *scaleSide, id int32) bool {
	for _, pod := range w.pods(s) {
		if idx, ok := query.InstanceIdxFromLabels(pod); ok && w.identityOf(s, idx) == id && w.lostNodes[s][pod.Spec.NodeName] {
			return true
		}
	}
	return false
}

// anyPodOnALostNode reports a pod left on a node this probe stopped, on
// any side.
func (w *moveWorld) anyPodOnALostNode() bool {
	for _, s := range w.sides {
		for _, pod := range w.pods(s) {
			if w.lostNodes[s][pod.Spec.NodeName] {
				return true
			}
		}
	}
	return false
}

// failedUnderAHeldRevision reports an Instance whose row stands Failed
// while its side holds on a revision whose retry schedule is spent: the
// hold names the way out, a corrected revision.
func (w *moveWorld) failedUnderAHeldRevision(s *scaleSide, id int32) bool {
	if s.h.hold == nil || s.h.hold.Gate != workloadtypes.RolloutHoldGateHeld {
		return false
	}
	for _, r := range w.rows(s) {
		if w.identityOf(s, r.Index) == id && r.Phase == workloadtypes.InstancePhaseFailed {
			return true
		}
	}
	return false
}

// moveParty reports whether an Instance is the source of a move this
// probe filed; its replacement stands for the same Instance.
func (w *moveWorld) moveParty(s *scaleSide, id int32) bool {
	for _, f := range w.filed {
		if f.side == s && f.identity == id {
			return true
		}
	}
	return false
}

// readEvents reads the events a move request raised: one acceptance and
// one completion for a move that completed.
func (w *moveWorld) readEvents() {
	for _, req := range w.filed {
		w.readOneRequestEvents(req)
	}
}

func (w *moveWorld) readOneRequestEvents(req *moveRequestFacts) {
	s := req.side
	rec := s.h.migration(req.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted {
		return
	}
	accepted, completed := 0, 0
	for _, e := range s.h.events[min(req.eventsAt, len(s.h.events)):] {
		if strings.Contains(e, " "+string(workloadtypes.EventReasonMigrationRequestAccepted)+" ") && strings.Contains(e, "uuid="+req.uuid) {
			accepted++
		}
		if strings.Contains(e, " "+string(workloadtypes.EventReasonMigrationCompleted)+" ") && strings.Contains(e, "uuid="+req.uuid) {
			completed++
		}
	}
	if completed != 1 {
		w.note("move-completion-event-count", "the move %s closed Completed with %d MigrationCompleted events, not one", rec.RequestUUID, completed)
	}
	if rec.SurgeInstance != nil && accepted != 1 {
		w.note("move-acceptance-event-count", "the move %s built a replacement with %d MigrationRequestAccepted events, not one", rec.RequestUUID, accepted)
	}
}

// momentSnapshots keeps a snapshot at every moment of this configuration:
// the scaling model's roll moments, then the pause, the scale-up and the
// scale-down taken from the rotating moment, and in full mode the roll
// held on a spent retry schedule.
func (w *moveWorld) momentSnapshots(full bool) map[moveMoment]*scaleSnapshot {
	w.t.Helper()
	snaps := w.captureRollMoments(moveSettleTicks)
	out := map[moveMoment]*scaleSnapshot{
		whenIdle: snaps[scaleAtIdle], whenSettling: snaps[scaleAtSettling], whenRotating: snaps[scaleAtRotating],
		whenDraining: snaps[scaleAtDraining], whenLast: snaps[scaleAtLast], whenCanaryStep: snaps[scaleAtCanaryStep],
	}
	if full {
		w.captureHeldMoment(snaps, moveSettleTicks)
		out[whenHeld] = snaps[scaleAtHeld]
	}
	rotating := snaps[scaleAtRotating]
	if rotating == nil {
		rotating = snaps[scaleAtSettling]
	}
	if rotating == nil {
		return out
	}
	prefill := w.prefill()
	w.restore(rotating)
	w.setPaused(true)
	w.path = append(w.path, "pause")
	w.scaleWorld.tick()
	out[whenPaused] = w.snapshot()
	w.restore(rotating)
	w.setReplicas(prefill, prefill.h.replicas+w.stepOf(prefill.h.replicas))
	w.path = append(w.path, fmt.Sprintf("scale %s up to %d", prefill.comp, prefill.h.replicas))
	w.scaleWorld.tick()
	out[whenScalingUp] = w.snapshot()
	w.restore(rotating)
	w.setReplicas(prefill, prefill.h.replicas-w.stepOf(prefill.h.replicas))
	w.path = append(w.path, fmt.Sprintf("scale %s down to %d", prefill.comp, prefill.h.replicas))
	w.scaleWorld.tick()
	out[whenScalingDown] = w.snapshot()
	return out
}

// probesAt lists the probes tried from a moment: the bounded list, or in
// full mode every verb on every target of every side.
func (w *moveWorld) probesAt(m moveMoment, full bool) []moveProbe {
	sides := []int{0}
	if full {
		for i := 1; i < len(w.sides); i++ {
			if w.sides[i].comp == workloadtypes.ComponentDecoder {
				sides = append(sides, i)
			}
		}
	}
	verbs := []moveVerb{verbRequest, verbNodeLost, verbPodsLost}
	if w.mcfg.forceDeleteOff {
		verbs = []moveVerb{verbRequest, verbRequestSourceNodeLost}
	} else if full || m == whenRotating || m == whenIdle {
		verbs = append(verbs, verbRequestSourceNodeLost)
	}
	targets := []moveTarget{targetBehind, targetDone}
	if w.cfg.settings.partition > 0 {
		targets = append(targets, targetHeld)
	}
	if full {
		targets = append(targets, targetOpen)
	}
	var out []moveProbe
	for _, side := range sides {
		for _, verb := range verbs {
			for _, target := range targets {
				if !full && !w.boundedProbe(m, verb, target) {
					continue
				}
				out = append(out, moveProbe{moment: m, verb: verb, side: side, target: target})
			}
		}
	}
	if !full && w.decode() != nil && m == whenRotating {
		out = append(out, moveProbe{moment: m, verb: verbRequest, side: 1, target: targetBehind})
	}
	return out
}

// boundedProbe keeps, per moment, the probes operators meet
// most, so the bounded run stays inside its time.
func (w *moveWorld) boundedProbe(m moveMoment, verb moveVerb, target moveTarget) bool {
	switch m {
	case whenIdle:
		return target == targetDone
	case whenCanaryStep:
		return target == targetDone && verb == verbRequest
	case whenLast:
		return target == targetDone && verb != verbPodsLost
	case whenSettling, whenDraining, whenScalingDown:
		return target == targetBehind && verb == verbRequest
	case whenRotating:
		return target == targetHeld || verb == verbRequest || (target == targetBehind && verb != verbRequestSourceNodeLost)
	case whenPaused:
		return target == targetBehind && verb != verbPodsLost
	case whenScalingUp:
		return target == targetBehind && verb != verbNodeLost
	}
	return target == targetBehind
}

// explore runs one configuration: the moments kept, every probe from
// every moment played to its end, then the random runs.
func (w *moveWorld) explore(full bool, seed int64, runs int) {
	w.t.Helper()
	budget := moveTickBudgetBounded
	if full {
		budget = moveTickBudgetFull
	}
	w.reachIdle()
	snaps := w.momentSnapshots(full)
	moments := []moveMoment{whenIdle, whenSettling, whenRotating, whenDraining, whenLast, whenCanaryStep, whenHeld, whenPaused, whenScalingUp, whenScalingDown}
	for _, m := range moments {
		snap := snaps[m]
		if snap == nil {
			continue
		}
		for _, p := range w.probesAt(m, full) {
			if w.mticks >= budget {
				w.skipped++
				continue
			}
			w.startProbe(snap)
			w.path = append(w.path, "at "+m.String())
			if !w.land(p) {
				continue
			}
			w.probes++
			settled := w.playOut()
			if m == whenPaused && settled {
				w.setPaused(false)
				w.path = append(w.path, "resume")
				settled = w.playOut()
			}
			w.classifyProbe(settled)
		}
	}
	w.randomRuns(snaps[whenIdle], seed, runs, budget)
}

// randomRuns plays the roll from idle with up to three moves at random
// ticks, under a fixed seed.
func (w *moveWorld) randomRuns(idle *scaleSnapshot, seed int64, runs, budget int) {
	w.t.Helper()
	rng := rand.New(rand.NewSource(seed))
	verbs := []moveVerb{verbRequest, verbNodeLost, verbPodsLost}
	for run := 0; run < runs; run++ {
		if w.mticks >= budget || idle == nil {
			w.skipped++
			continue
		}
		w.startProbe(idle)
		w.path = append(w.path, fmt.Sprintf("random run %d", run+1))
		w.push(fixedImage)
		w.probes++
		left := 3
		settling := w.startSettling()
		settled := false
		for i := 0; i < moveSettleTicks && !settled; i++ {
			if left > 0 && rng.Intn(5) == 0 {
				p := moveProbe{moment: whenRotating, verb: verbs[rng.Intn(len(verbs))], side: rng.Intn(len(w.sides)), target: moveTarget(rng.Intn(2))}
				if w.sides[p.side].comp == workloadtypes.ComponentRouter {
					p.side = 0
				}
				if w.land(p) {
					left--
					settling = w.startSettling()
				}
			}
			settled = settling.observe(w, w.tick())
		}
		w.classifyProbe(settled)
	}
}

// moveReferenceConfigs are the configurations every run explores: the
// reference shapes under their reference settings, the budgets the
// task names, one blueGreen group, one partition, one gang shape.
func moveReferenceConfigs() []moveConfig {
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	configs := []scaleConfig{
		{shape: scaleShape{16, 0, 1}, settings: scaleSettings{prefill: recreate}},
		{shape: scaleShape{12, 0, 1}, settings: scaleSettings{prefill: surge, budget: "25%"}},
		{shape: scaleShape{8, 0, 4}, settings: scaleSettings{prefill: recreate}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true, firstSide: v1beta1.DecoderComponent}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, blueGreen: true, budget: "25%"}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, partition: 2}},
	}
	var out []moveConfig
	for _, cfg := range configs {
		out = append(out, moveConfig{scaleConfig: cfg})
	}
	return out
}

// moveConfigs lists the configurations of a run: the reference ones, and
// in full mode the force-delete policy left out beside each, the canary
// pair, and a seeded sample of the whole product of shapes, gang sizes,
// strategies, budgets and settings.
func moveConfigs(full bool) []moveConfig {
	configs := moveReferenceConfigs()
	if !full {
		return configs
	}
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	for _, cfg := range moveReferenceConfigs() {
		configs = append(configs, moveConfig{scaleConfig: cfg.scaleConfig, forceDeleteOff: true})
	}
	configs = append(configs,
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: recreate, decode: surge, canary: true}}},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 0, 1}, settings: scaleSettings{prefill: recreate, partition: 4, budget: "25%"}}},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, blueGreen: true}}},
	)
	shapes := [][2]int32{{16, 0}, {12, 0}, {6, 0}, {12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}}
	pods := []int32{1, 1, 2, 4, 8}
	budgets := []string{"10%", "10%", "25%"}
	variants := []scaleSettings{{}, {}, {maintainRatio: true}, {keepRatio: true, firstSide: v1beta1.DecoderComponent}, {partition: 2}, {blueGreen: true}, {canary: true}, {router: true}, {policy: true}}
	rng := rand.New(rand.NewSource(20261103))
	seen := map[string]bool{}
	for _, cfg := range configs {
		seen[cfg.String()] = true
	}
	for len(configs) < 48 {
		shape := shapes[rng.Intn(len(shapes))]
		p := pods[rng.Intn(len(pods))]
		if (shape[0]+shape[1])*p > scaleFullPodCeiling {
			continue
		}
		settings := variants[rng.Intn(len(variants))]
		settings.prefill, settings.decode = recreate, recreate
		if rng.Intn(2) == 0 {
			settings.prefill = surge
		}
		if rng.Intn(2) == 0 {
			settings.decode = surge
		}
		settings.budget = budgets[rng.Intn(len(budgets))]
		if shape[1] == 0 {
			settings.decode, settings.maintainRatio, settings.keepRatio, settings.firstSide, settings.policy, settings.canary = "", false, false, "", false, false
		}
		if settings.canary && settings.partition > 0 {
			settings.partition = 0
		}
		cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{shape[0], shape[1], p}, settings: settings}, forceDeleteOff: rng.Intn(6) == 0}
		if seen[cfg.String()] {
			continue
		}
		seen[cfg.String()] = true
		configs = append(configs, cfg)
	}
	return configs
}

// moveExpectedClasses are the readings a probe may end with that are not
// findings: the decided behaviour, the roll's own ends, and the waits the
// operator asked for.
var moveExpectedClasses = map[string]bool{
	"held": true, "ends-paused": true,
	"move-built-once-on-the-roll-target": true, "move-built-once-on-its-revision": true, "move-built-once-after-the-roll-took-its-source": true, "move-built-on-its-revision-under-the-pause-then-rolled": true, "move-built-on-its-revision-then-rolled-by-a-later-push": true,
	"move-answered-without-moving": true, "move-refused-source-revision-held": true, "move-refused-revision-not-started": true,
	"move-waits-under-the-pause": true, "move-waits-with-its-text-at-the-end": true,
	"move-built-once-on-its-revision-behind-a-hold": true, "move-built-once-on-its-revision-and-now-held": true, "move-built-once-then-a-later-verb-took-the-instance": true, "move-expired-while-its-source-was-down": true,
	"lost-instance-left-failed-under-a-held-revision": true, "move-built-once-on-its-revision-while-the-roll-stands": true,
	"move-refused-source-without-pods": true, "move-refused-source-gone": true,
	"rebuilt-once-on-the-roll-target": true, "rebuilt-once-on-its-revision": true, "rebuilt-on-its-revision-then-rolled": true,
	"rebuilt-on-its-revision-under-a-backoff-then-rolled": true, "lost-instance-rebuilt-by-its-move": true,
}

// knownMoveClasses are the readings the model currently shows, each with
// the behaviour behind it. A finding in a
// known class is reported and does not fail the run; a change of behaviour removes its
// entry and the lock test that holds the picture.
var knownMoveClasses = map[string]string{
	"move-built-on-the-roll-target-behind-a-hold":                         "a move filed while a coordination hold paces the side builds its replacement on the roll's target at once, as the repair's rebuild rule does; the start counts outside the batch plan, which the ratio readings see",
	"move-requested-during-a-surge-expires-with-no-wait-named":            "a move request filed behind another fresh request while a surge attempt is in flight sits Accepted with nothing named and keeps its accept-time deadline, which closes it Failed with `surge never allocated` once no surge is in flight: the pass names the wait for the roll, holds the deadline and measures it again on the oldest fresh record alone",
	"move-waits-unnamed-behind-the-rolls-surges":                          "a move request filed behind another fresh request while the roll's surges hold the surge budget sits Accepted with no text until the request ahead of it starts: the pass names the wait for the roll on the oldest fresh record alone, and a record behind it says nothing",
	"move-waits-for-the-roll-after-its-instance-left-the-node":            "a move filed while the roll is paused, whose source's node stops reporting before the resume, finds its Instance rebuilt on the roll's target on another node at the resume (the lost-node sweep frees the pod under the pause, and the Create pass rebuilds the Instance in the pass the roll's surges start) and still waits, named, for the roll's surge replacements with its deadline standing still, then closes Completed `already left <node>; nothing to move` on the first pass with no surge replacement in flight: answering the request allocates nothing, so the wait buys nothing (held by TestMove_AMoveFiledUnderAPauseWithItsSourceNodeLostNamesItsWaitAndClosesAlreadyLeft)",
	"move-expired-after-waiting-behind-the-rolls-surges":                  "a move request filed behind another fresh request while the roll's surges hold the budget waits unnamed with its accept-time deadline running, starts once the request ahead of it is done with little of that deadline left, and is closed Failed as its replacement comes up, which is then torn down: the oldest fresh record alone has its deadline measured again when it starts",
	"roll-waits-behind-a-move-on-a-lost-kubelet":                          "a move waiting on a source pod the kubelet never removed ends every pass of its Component, so no other Instance is pushed or created while it waits (held by TestMove_AMoveWaitingOnALostKubeletHoldsOrFreesTheRestOfTheRoll)",
	"group-gate-waits-on-a-move-held-by-its-source-teardown":              "a move waiting on its source teardown keeps its pair out of the serving count the group gate reads: the replacement's row reads Creating with no revision until the record completes and the source is out of rotation, so the other Component's batch waits for good on a batch that never reads as served, and the hold names the batch, not the move",
	"roll-waits-behind-its-own-attempt-on-a-lost-kubelet":                 "an attempt of the roll holds its slot on a pod of a node that stopped reporting until the force-delete policy frees it, and no hold names the wait",
	"move-past-its-deadline-waiting-on-its-source-teardown":               "with no force-delete policy a source pod left Terminating by a kubelet that is gone counts as drained, so the record waits past its deadline for as long as the kubelet stays away",
	"move-waits-for-good-on-its-source-teardown":                          "with no force-delete policy a move whose source pod the kubelet never removes waits for good, and its text says so",
	"lost-instance-waits-on-its-kubelet-with-no-force-delete":             "with no force-delete policy, the chart's default, a pod left Terminating by a kubelet that stopped keeps its Instance's name, so the Instance is never rebuilt while the kubelet stays away; the DrainOverdue condition is the operator's signal",
	"roll-waits-on-instances-left-on-a-lost-kubelet-with-no-force-delete": "with no force-delete policy the pods a stopped kubelet holds stay where they are, the Instances behind them never serve again while the kubelet is away, and a roll or a scale stands on them with the budget or the ratio hold naming the Instances",
	"move-expires-while-following-a-gang-surge":                           "a move request that no pass has named, filed behind another fresh request, whose source a gang surge replaces under another index, follows the rebuilt Instance with its accept-time deadline running and, when the surge outlasts that deadline, closes Failed naming what it followed once no surge is in flight: the oldest fresh record alone waits with its deadline held",
	"move-expired-draining-a-source-on-a-lost-kubelet":                    "with the force-delete policy on, a move whose source's kubelet stops mid-drain is closed Failed at its deadline with `source drain incomplete` when the deadline comes before the policy frees the pod; the replacement is torn down and the source restored",
	"move-failed-on-a-target-no-pod-can-pull":                             "a move filed while the target's retry record waits out a delay builds its replacement on the target, as a record in Backoff counts as an open run; when no pod can pull that revision the replacement never comes up, the move closes Failed naming the pull, and the source stays where it was",
	"move-of-a-held-instance-hands-its-hold-to-the-next-instance":         "a move of an Instance the partition holds builds its replacement on the revision the source runs; the hold is by count over the lowest-indexed Instances off the target, so it passes over the replacement, which takes the highest index, lands on the next Instance the operator never picked, and the roll rebuilds the replacement on the target: two builds and a hold the operator did not place (held by TestMove_AMoveOfAHeldInstanceHandsItsHoldToTheNextInstance)",
	"rebuilt-on-the-roll-target-then-on-its-revision-once-held":           "a lost Instance left to a roll whose retry record has failed attempts is the roll's next start when the delay ends, since a start on a row the serving count already leaves out asks no group gate; when that attempt fails too the schedule is spent, the revision is held, and the repair rebuilds the Instance on its own revision: two builds, the second the repair's, and the Instance serves again (held by TestMove_ALostMemberOfAnOldGangIsTheRollsLastAttemptOnAnImageNoPodCanPull)",
}

// scaleKnownClasses are the scaling model's known classes, read the same
// way here since the world is its.
func scaleKnownClasses() map[string]string {
	out := map[string]string{}
	for class, text := range knownScaleFindingClasses {
		out[class] = text
	}
	for class, text := range knownMoveClasses {
		out[class] = text
	}
	return out
}

type moveExample struct {
	cfg     moveConfig
	finding scaleFinding
}

// moveResult is what one configuration's exploration found.
type moveResult struct {
	cfg                                               moveConfig
	states, ticks, passes, gateCalls, probes, skipped int
	counts                                            map[string]int
	findings                                          map[string]scaleFinding
	took                                              time.Duration
}

// moveExplore runs the model over the configurations keep admits, a
// bounded number at a time in parallel, and reports every class, failing
// on the classes that are neither expected nor known.
func moveExplore(t *testing.T, keep func(moveConfig) bool) {
	t.Helper()
	full := os.Getenv(moveHuntFullEnv) == "full"
	only := os.Getenv(moveHuntOnlyEnv)
	runs, workers := 1, 8
	if full {
		runs, workers = 4, moveWorkers
	}
	start := time.Now()
	var configs []moveConfig
	for _, cfg := range moveConfigs(full) {
		if keep(cfg) && (only == "" || strings.Contains(cfg.String(), only)) {
			configs = append(configs, cfg)
		}
	}
	results := make([]moveResult, len(configs))
	admission := make(chan struct{}, workers)
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				admission <- struct{}{}
				defer func() { <-admission }()
				began := time.Now()
				w := newMoveWorld(t, cfg)
				w.explore(full, int64(i+1), runs)
				results[i] = moveResult{cfg: cfg, states: len(w.seen), ticks: w.ticks, passes: w.passes, gateCalls: w.gateCalls, probes: w.probes, skipped: w.skipped,
					counts: w.counts, findings: w.findings, took: time.Since(began)}
			})
		}
	})
	known := scaleKnownClasses()
	totals := map[string]int{}
	examples := map[string][]moveExample{}
	configurations := map[string]int{}
	var states, ticks, passes, gateCalls, probes, skipped int
	for n, res := range results {
		states += res.states
		ticks += res.ticks
		passes += res.passes
		gateCalls += res.gateCalls
		probes += res.probes
		skipped += res.skipped
		var parts []string
		for class, c := range res.counts {
			totals[class] += c
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: %d states, %d ticks, %d probes (%d skipped past the tick budget), %s: %s", n+1, len(configs), res.cfg, res.states, res.ticks, res.probes, res.skipped, res.took.Round(time.Second), strings.Join(parts, " "))
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 6 {
				examples[class] = append(examples[class], moveExample{cfg: res.cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d ticks, %d passes, %d gate questions, %d probes (%d skipped past the tick budget), %s", len(configs), states, ticks, passes, gateCalls, probes, skipped, time.Since(start).Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d states in %d configurations", class, totals[class], configurations[class])
		if moveExpectedClasses[class] {
			continue
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		switch {
		case known[class] != "":
			t.Logf("  known: %s", known[class])
		default:
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
		}
	}
}

// TestMove_EveryMoveEndsBuiltOnceOrNamed explores every configuration: a
// move request, a node lost or an Instance's pods lost at every moment of
// a roll, under a pause, a partition, a canary step, a scale-up and a
// scale-down, leaves the Instance built once on the decided revision, the
// record closed or naming its wait, the roll finished or holding for a
// reason it names, with no hold or record text opening and closing on
// alternate passes and nothing past its deadline unnamed.
func TestMove_EveryMoveEndsBuiltOnceOrNamed(t *testing.T) {
	moveExplore(t, func(moveConfig) bool { return true })
}

// TestMove_AMoveDuringARollBuildsItsReplacementOnTheRollsTarget holds
// the move rules on the reference engine shape: 16 Instances
// under RecreatePod at ten percent, a move request on the lowest Instance
// still on version 1 while the roll rotates, nothing holding the roll off
// it. The move builds its replacement once, on version 2, and the roll
// finishes around it; any other build list fails.
func TestMove_AMoveDuringARollBuildsItsReplacementOnTheRollsTarget(t *testing.T) {
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{16, 0, 1}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod}}}
	w := newMoveWorld(t, cfg)
	w.reachIdle()
	snaps := w.captureRollMoments(moveSettleTicks)
	if snaps[scaleAtRotating] == nil {
		t.Fatalf("the roll never reached its rotating moment")
	}
	w.startProbe(snaps[scaleAtRotating])
	s := w.sides[0]
	idx, ok := w.pick(s, targetBehind)
	if !ok {
		t.Fatalf("no Instance still on version 1 with no attempt open; picture: %s", w.picture())
	}
	uuid := w.fileMoveRequest(s, idx)
	if !w.request.rollCouldTake {
		t.Fatalf("the roll should be free to take Instance %d; picture: %s", idx, w.picture())
	}
	if !w.playOut() {
		t.Fatalf("the world never settled after the move; picture: %s", w.picture())
	}
	rec := s.h.migration(uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance == nil {
		t.Fatalf("the move should close Completed with a replacement; got %v; picture: %s", rec, w.picture())
	}
	if !w.complete() {
		t.Fatalf("the roll should finish around the move; picture: %s", w.picture())
	}
	if revs := w.requestRevisions(uuid); revs != "v2" {
		t.Errorf("the replacement %d should be built once, on v2; it was built as %q; picture: %s", *rec.SurgeInstance, revs, w.picture())
	}
}

// buildRevisions lists builds by revision, comma-separated.
func buildRevisions(builds []moveBuild) string {
	var revs []string
	for _, b := range builds {
		revs = append(revs, b.revision)
	}
	return strings.Join(revs, ",")
}

// TestMove_AMoveWaitingOnALostKubeletHoldsOrFreesTheRestOfTheRoll holds the
// move rule on the reference engine shape with no force-delete policy: an
// Instance is asked off its node, its replacement serves, the source
// drains, and the kubelet under the source goes away with the pod left
// Terminating; then version 2 is pushed. Every Instance has a node of its
// own, so the lost kubelet takes nothing but the source with it. The move
// waits on its source teardown for good. The wait ends every pass
// of the Component, so no other Instance is pushed; the decided picture
// rolls every other Instance to version 2 around the waiting move. Either
// passes; a roll that started and stopped part way fails.
func TestMove_AMoveWaitingOnALostKubeletHoldsOrFreesTheRestOfTheRoll(t *testing.T) {
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{16, 0, 1}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod}}, forceDeleteOff: true}
	w := newMoveWorld(t, cfg)
	s := w.sides[0]
	w.reachIdle()
	w.startProbe(w.snapshot())
	if !w.land(moveProbe{moment: whenIdle, verb: verbRequestSourceNodeLost, side: 0, target: targetDone}) {
		t.Fatalf("no Instance to move; picture: %s", w.picture())
	}
	w.playOut()
	rec := s.h.migration(w.request.uuid)
	if rec == nil || rec.Phase.Terminal() || rec.SurgeInstance == nil || !strings.Contains(rec.Message, "source teardown") {
		t.Fatalf("with no force-delete policy the move should wait on its source teardown; got %v; picture: %s", rec, w.picture())
	}
	others, onTarget, open := 0, 0, 0
	for _, r := range w.rows(s) {
		if r.Index == rec.SourceInstance || r.Index == *rec.SurgeInstance {
			continue
		}
		others++
		if r.RunningRevision == s.h.target.Name {
			onTarget++
		}
		if r.Operation != nil {
			open++
		}
	}
	switch {
	case onTarget == 0 && open == 0:
		t.Logf("current picture: none of the other %d Instances was pushed while the move waits on its source teardown", others)
	case onTarget == others && open == 0:
		t.Logf("the decided picture: all other %d Instances rolled to version 2 around the waiting move", others)
	default:
		t.Errorf("the other %d Instances should all stand on version 1 (today) or all on version 2 (decided); %d are on version 2 and %d have an attempt open; picture: %s", others, onTarget, open, w.picture())
	}
}

// TestMove_AMoveOfAHeldInstanceHandsItsHoldToTheNextInstance holds
// the current reading of a move of an Instance the partition holds: 12
// Instances under RecreatePod at ten percent with a partition of 2, the
// lowest held Instance asked off its node while the roll rotates. The
// move builds its replacement on version 1 at the highest index; the
// partition holds the lowest-indexed Instances off version 2 by count, so
// the hold passes over the replacement and lands on the next Instance
// still on version 1, and the roll rebuilds the replacement on version 2.
// The operator ends with two Instances on version 1 neither of which they
// picked, and the moved Instance built twice. A fix flips the last two
// checks: the replacement inherits the hold and is built once.
func TestMove_AMoveOfAHeldInstanceHandsItsHoldToTheNextInstance(t *testing.T) {
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 0, 1}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod, partition: 2}}}
	w := newMoveWorld(t, cfg)
	w.reachIdle()
	snaps := w.captureRollMoments(moveSettleTicks)
	if snaps[scaleAtRotating] == nil {
		t.Fatalf("the roll never reached its rotating moment")
	}
	w.startProbe(snaps[scaleAtRotating])
	s := w.sides[0]
	idx, ok := w.pick(s, targetHeld)
	if !ok {
		t.Fatalf("no Instance the partition holds; picture: %s", w.picture())
	}
	before := w.heldIndices(s)
	uuid := w.fileMoveRequest(s, idx)
	if !w.playOut() {
		t.Fatalf("the world never settled after the move; picture: %s", w.picture())
	}
	rec := s.h.migration(uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance == nil {
		t.Fatalf("the move should close Completed with a replacement; got %v; picture: %s", rec, w.picture())
	}
	if !w.complete() {
		t.Fatalf("the roll should finish around the move with the partition's count held; picture: %s", w.picture())
	}
	after := w.heldIndices(s)
	if len(after) != len(before) {
		t.Errorf("the partition should hold %d Instances before and after the move; it holds %d after; picture: %s", len(before), len(after), w.picture())
	}
	var taken []int32
	for k := range after {
		if !before[k] {
			taken = append(taken, k)
		}
	}
	sort.Slice(taken, func(i, j int) bool { return taken[i] < taken[j] })
	if after[*rec.SurgeInstance] || len(taken) == 0 {
		t.Errorf("current reading: the hold passes over the replacement %d and lands on an Instance the operator never picked; held before %v, after %v; picture: %s", *rec.SurgeInstance, sortedKeys(before), sortedKeys(after), w.picture())
	}
	if revs := w.requestRevisions(uuid); revs != "v1,v2" {
		t.Errorf("current reading: the replacement %d is built on v1 by the move and rebuilt on v2 by the roll; it was built as %q; picture: %s", *rec.SurgeInstance, revs, w.picture())
	}
}

// TestMove_AMoveFiledUnderAPauseWithItsSourceNodeLostNamesItsWaitAndClosesAlreadyLeft
// holds the reference engine shape under SurgeThenDrain at 25 percent:
// the roll is paused while rotating, the operator asks the lowest
// Instance still on version 1 off its node, and the node under that
// Instance stops reporting while the move still waits to start. Under
// the pause the record stands Accepted with no text, as a paused side
// may; the sweep frees the pod on the lost node, and at the resume the
// Instance is rebuilt on version 2 on another node in the pass the
// roll's surges start. The move waits for the roll: within six ticks of
// the resume the record names the roll it waits for, written by a pass
// that began with a surge replacement of the roll in flight, and its
// deadline stands still for as long as the text holds. The record stays
// open through every pass that begins with a surge replacement in
// flight, and the first pass that begins with none closes it Completed
// as already left: the Instance runs on version 2 off the node it was
// asked to leave, no replacement was built, and no deadline closed the
// record Failed. The roll finishes around it.
func TestMove_AMoveFiledUnderAPauseWithItsSourceNodeLostNamesItsWaitAndClosesAlreadyLeft(t *testing.T) {
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 0, 1}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategySurgeThenDrain, budget: "25%"}}}
	w := newMoveWorld(t, cfg)
	w.reachIdle()
	snaps := w.momentSnapshots(false)
	if snaps[whenPaused] == nil {
		t.Fatalf("the roll never reached its paused moment")
	}
	w.startProbe(snaps[whenPaused])
	w.path = append(w.path, "at "+whenPaused.String())
	if !w.land(moveProbe{moment: whenPaused, verb: verbRequestSourceNodeLost, side: 0, target: targetBehind}) {
		t.Fatalf("no Instance still on version 1 to move; picture: %s", w.picture())
	}
	s := w.sides[0]
	uuid, source, fromNode := w.request.uuid, w.request.source, w.request.fromNode
	if !w.playOut() {
		t.Fatalf("the world never settled under the pause; picture: %s", w.picture())
	}
	if rec := s.h.migration(uuid); rec == nil || rec.Phase != workloadtypes.MigrationPhaseAccepted || rec.Message != "" {
		t.Fatalf("under the pause the move should stand Accepted with no text; got %v; picture: %s", rec, w.picture())
	}
	w.setPaused(false)
	w.path = append(w.path, "resume")
	rec, surgesBefore := w.tickUntilTheWaitIsNamed(s, uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseAccepted || !moveWaitsForTheRoll(rec) {
		t.Fatalf("the move should name the roll it waits for within %d ticks of the resume; the record reads %v; picture: %s", moveUnnamedTicks, rec, w.picture())
	}
	if surgesBefore == 0 {
		t.Errorf("the record says a surge replacement of the roll is in flight, and the pass that wrote it began with none; picture: %s", w.picture())
	}
	if !strings.Contains(rec.Message, s.h.target.Name) {
		t.Errorf("the wait should name the roll's target revision %s; it reads %q", s.h.target.Name, rec.Message)
	}
	rec = w.tickUntilTheMoveCloses(s, uuid, rec.Deadline)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted || !strings.Contains(rec.Message, "already left "+fromNode) || !strings.Contains(rec.Message, "nothing to move") {
		t.Fatalf("the move should close Completed as already left on the first pass with no surge replacement in flight; got %v; picture: %s", rec, w.picture())
	}
	if rec.SurgeInstance != nil {
		t.Errorf("a move closed as already left builds no replacement; the record names Instance %d; picture: %s", *rec.SurgeInstance, w.picture())
	}
	if row := s.h.instance(source); row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.RunningRevision != s.h.target.Name {
		t.Errorf("Instance %d should run Ready on v2, rebuilt off %s; it reads %v; picture: %s", source, fromNode, row, w.picture())
	}
	pods := s.h.podsOf(source)
	if len(pods) == 0 {
		t.Errorf("Instance %d should have a live pod off %s; it has none; picture: %s", source, fromNode, w.picture())
	}
	for _, pod := range pods {
		if pod.Spec.NodeName == fromNode {
			t.Errorf("Instance %d still has pod %s on %s, the node it was asked to leave; picture: %s", source, pod.Name, fromNode, w.picture())
		}
	}
	if !w.playOut() {
		t.Fatalf("the world never settled after the move closed; picture: %s", w.picture())
	}
	if !w.complete() {
		t.Errorf("the roll should finish around the move; picture: %s", w.picture())
	}
	if _, read := w.findings["move-waits-for-the-roll-after-its-instance-left-the-node"]; !read {
		t.Errorf("the model should read the move waiting for the roll while Instance %d already runs off %s; it read %v", source, fromNode, sortedFindingClasses(w.findings))
	}
}

// sortedFindingClasses lists the classes a world has noted, in order.
func sortedFindingClasses(findings map[string]scaleFinding) []string {
	var out []string
	for class := range findings {
		out = append(out, class)
	}
	sort.Strings(out)
	return out
}

// tickUntilTheWaitIsNamed ticks for at most moveUnnamedTicks ticks and
// returns the record once it names the roll as what it waits for, or is
// terminal, or as it stands after the last tick, with the surge
// replacements of the roll that were in flight when the pass that wrote
// the record began.
func (w *moveWorld) tickUntilTheWaitIsNamed(s *scaleSide, uuid string) (*workloadtypes.MigrationRecord, int) {
	surgesBefore := 0
	for i := 0; i < moveUnnamedTicks; i++ {
		surgesBefore = w.surgeReplacementsInFlight(s)
		w.tick()
		rec := s.h.migration(uuid)
		if rec == nil || rec.Phase.Terminal() || moveWaitsForTheRoll(rec) {
			return rec, surgesBefore
		}
	}
	return s.h.migration(uuid), surgesBefore
}

// tickUntilTheMoveCloses ticks until the record is terminal or the
// settling bound is spent, reading each pass of the wait against the
// roll: a pass that began with a surge replacement of the roll in flight
// leaves the record open, with its wait named, its deadline where it
// stood and its phase short of Failed; the first pass that began with
// none closes it.
func (w *moveWorld) tickUntilTheMoveCloses(s *scaleSide, uuid string, deadline metav1.Time) *workloadtypes.MigrationRecord {
	w.t.Helper()
	for i := 0; i < moveSettleTicks; i++ {
		surgesBefore := w.surgeReplacementsInFlight(s)
		w.tick()
		rec := s.h.migration(uuid)
		if rec == nil {
			return nil
		}
		switch {
		case rec.Phase.Terminal() && surgesBefore > 0:
			w.t.Errorf("the move closed in a pass that began with %d surge replacements of the roll in flight: %v; picture: %s", surgesBefore, rec, w.picture())
		case rec.Phase.Terminal():
		case surgesBefore == 0:
			w.t.Errorf("the move should close in the first pass that begins with no surge replacement in flight; after one it still reads %v; picture: %s", rec, w.picture())
		case rec.Phase == workloadtypes.MigrationPhaseFailed, !moveWaitsForTheRoll(rec), !rec.Deadline.Equal(&deadline):
			w.t.Errorf("while the roll's surge replacements are in flight the move should keep its wait and its deadline %s; it reads %v; picture: %s", deadline.Time, rec, w.picture())
		}
		if rec.Phase.Terminal() {
			return rec
		}
	}
	return s.h.migration(uuid)
}

// moveWaitsForTheRoll reports a record whose text names the roll's surge
// replacements as what it waits for.
func moveWaitsForTheRoll(rec *workloadtypes.MigrationRecord) bool {
	return rec != nil && strings.HasPrefix(rec.Message, "waiting for the roll")
}

// surgeReplacementsInFlight counts the side's rows whose roll keeps a
// surge replacement pod beside the source, the reading the migrate pass
// holds a fresh move on.
func (w *moveWorld) surgeReplacementsInFlight(s *scaleSide) int {
	n := 0
	rows := w.rows(s)
	for i := range rows {
		if workloadtypes.SurgeReplacementInFlight(&rows[i]) {
			n++
		}
	}
	return n
}

// requestRevisions is the revisions a move's replacement was built on, in
// the order built, joined by commas.
func (w *moveWorld) requestRevisions(uuid string) string {
	builds, _ := w.requestBuilds(w.factsOf(uuid))
	return buildRevisions(builds)
}

// sortedKeys lists a held set in index order.
func sortedKeys(held map[int32]bool) []int32 {
	var out []int32
	for k := range held {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// loseAMemberBehindTheHeldDecoder plays the opening the two walks below
// share: the decoder is pushed to an image no pod can pull and ends Held,
// so the group gate holds the engine side's starts for it; the node under
// one member of an engine gang still on version 1 goes away; the world
// plays out. It fails the test when the gang is left to the roll the gate
// holds, and returns the world and the engine side for the walk's own
// reading of the builds.
func loseAMemberBehindTheHeldDecoder(t *testing.T, cfg moveConfig) (*moveWorld, *scaleSide) {
	t.Helper()
	w := newMoveWorld(t, cfg)
	w.reachIdle()
	snaps := w.captureRollMoments(moveSettleTicks)
	w.captureHeldMoment(snaps, moveSettleTicks)
	if snaps[scaleAtHeld] == nil {
		t.Fatalf("the decoder's retry schedule was never spent; picture: %s", w.picture())
	}
	w.startProbe(snaps[scaleAtHeld])
	s := w.sides[0]
	if s.h.hold == nil {
		t.Fatalf("the engine side should stand behind the group gate for the held decoder; picture: %s", w.picture())
	}
	if !w.land(moveProbe{moment: whenHeld, verb: verbNodeLost, side: 0, target: targetBehind}) {
		t.Fatalf("no engine gang still on version 1 with no attempt open; picture: %s", w.picture())
	}
	w.classifyProbe(w.playOut())
	if n := w.counts["lost-instance-left-to-a-roll-the-group-gate-holds"]; n != 0 {
		t.Fatalf("the gang short of a member was left to the roll the group gate holds: %s", w.findings["lost-instance-left-to-a-roll-the-group-gate-holds"].message)
	}
	if len(w.moved[s]) == 0 {
		t.Fatalf("the node loss took no engine Instance's pod; picture: %s", w.picture())
	}
	return w, s
}

// TestMove_ALostMemberOfAnOldGangIsRebuiltWhileTheGroupGateHoldsTheSide
// holds the rule past the side's own retry schedule: 12 engine
// gangs of two beside 4 decode gangs of two, both SurgeThenDrain at a 10 percent
// budget in one group under maintainRatio. The decoder is pushed
// to an image no pod can pull and ends Held, so the group gate holds the
// engine side's starts for it. The node under one member of an engine
// gang still on version 1 goes away. The gang is off the roll target, so
// the repair leaves it to the roll, and the roll surges it onto version 2
// with no gate asked, since the gang serves nothing already: it is built
// once, on version 2, and no Instance is left to a roll the gate holds.
func TestMove_ALostMemberOfAnOldGangIsRebuiltWhileTheGroupGateHoldsTheSide(t *testing.T) {
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 4, 2}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}}
	w, s := loseAMemberBehindTheHeldDecoder(t, cfg)
	for id := range w.moved[s] {
		builds, byMove := w.rebuildsOf(s, id)
		if byMove || len(builds) != 1 || builds[0].revision != w.short(s, s.h.target.Name) {
			t.Fatalf("the gang should be built once, on version 2, by the roll; builds %s; picture: %s", w.buildsOf(s, id), w.picture())
		}
	}
}

// TestMove_ALostMemberOfAnOldGangIsTheRollsLastAttemptOnAnImageNoPodCanPull
// holds the twin where the engine's own target is broken too: 16 engine
// gangs of two under SurgeThenDrain beside 8 decode gangs of two under
// RecreatePod, the batches planned together with the decoder first, at ten
// percent. Both sides are pushed to an image no pod can pull; the decoder's
// record ends Held while the engine's waits out its delay after two failed
// attempts. The node under one member of an engine gang still on version 1
// goes away. When the delay ends the roll's third attempt is the short
// gang's surge, which asks no group gate; it fails like the two before it,
// the revision is held, and the repair rebuilds the gang on version 1. Two
// builds, and the gang serves again instead of standing short for the hold.
func TestMove_ALostMemberOfAnOldGangIsTheRollsLastAttemptOnAnImageNoPodCanPull(t *testing.T) {
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{16, 8, 2}, settings: scaleSettings{
		prefill: workloadtypes.UpdateStrategySurgeThenDrain, decode: workloadtypes.UpdateStrategyRecreatePod,
		keepRatio: true, firstSide: v1beta1.DecoderComponent, budget: "10%"}}}
	w, s := loseAMemberBehindTheHeldDecoder(t, cfg)
	target := w.short(s, s.h.target.Name)
	for id := range w.moved[s] {
		builds, byMove := w.rebuildsOf(s, id)
		if byMove || len(builds) != 2 || builds[0].revision != target || builds[1].revision != "v1" {
			t.Fatalf("the gang should be the roll's last attempt on the unpullable image and then rebuilt on version 1; builds %s; picture: %s", w.buildsOf(s, id), w.picture())
		}
	}
	if n := w.counts["rebuilt-on-the-roll-target-then-on-its-revision-once-held"]; n != 1 {
		t.Fatalf("the reading should name the two builds once, got %d; picture: %s", n, w.picture())
	}
	if !w.targetHeld(s) {
		t.Fatalf("the engine's retry schedule should be spent; picture: %s", w.picture())
	}
}
