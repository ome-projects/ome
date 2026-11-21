package workload_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the real engine over a move request on an
// Instance whose node's kubelet stops reporting and, in most probes,
// starts again later. It runs in the move model's world
// (move_model_test.go): one workload.Reconcile loop per Component on the
// recovery harness, the coordination gates wired between the Components,
// one pod per node, and the chart's lifecycle values. The kubelet stops
// before the request is filed, right after it is recorded, while the
// replacement is coming up, or while the source drains; it starts again
// within a minute, once the force-delete policy has had time to act, once
// everything else has settled, or never. Each lands at every moment of a
// roll on the reference shapes, single pods and gangs, with and without
// the force-delete policy, on the Instance the push reaches next, the one
// it reaches last and one it has already moved.
//
// What the operator asked for, and what every probe must end with: from
// the request on, the Instance is built once, off the node the request
// names, and the record closes Completed; or the record closes Failed
// naming why; or it waits with its text naming the wait. A second build
// for one request, an Instance left on the node it was asked to leave, a
// source row left behind a completed move, two rows for one Instance, or
// a record still open once the kubelet is back and the roll has finished
// is a finding. The move model's own readings (a hold with no way out, two
// gates waiting on each other, a budget exceeded, a record that steps back, a wait with
// nothing named) run beside these.
//
// OME_MOVE_KUBELET_OUTAGE_HUNT=full widens the configurations, the
// moments, the probes and the bounds; OME_MOVE_KUBELET_OUTAGE_HUNT_ONLY=<text>
// keeps the configurations whose name carries the text.

const (
	kubeletOutageFullEnv = "OME_MOVE_KUBELET_OUTAGE_HUNT"
	kubeletOutageOnlyEnv = "OME_MOVE_KUBELET_OUTAGE_HUNT_ONLY"
	// outageTickBudgetBounded and outageTickBudgetFull bound the ticks one
	// configuration spends; the probes left when a budget is spent are
	// counted as skipped, so a run on a slower machine reads the same
	// first probes.
	outageTickBudgetBounded = 1400
	outageTickBudgetFull    = 16000
	// outageWorkers is the most configurations explored at once in full
	// mode, to keep the suite's CPU use bounded.
	outageWorkers = 28
	// outageAllocateTicks is how long a probe waits for the record to
	// reach the phase it stops the kubelet at.
	outageAllocateTicks = 30
	// outageSoon and outageLater are how long the kubelet stays away:
	// under the force-delete policy's threshold, and past the threshold
	// and its slack, so the policy has had time to act.
	outageSoon  = time.Minute
	outageLater = scaleRollNodeUnreachable + scaleRollOverdueSlack + time.Minute
)

// outageStart is when the kubelet under the move's source stops.
type outageStart uint8

const (
	stopBeforeRequest outageStart = iota // the Instance leaves rotation on a stopped node, then the operator asks it off that node
	stopAtAccept                         // the request is recorded and the kubelet stops before the engine's next pass
	stopAtSurge                          // the replacement is allocated and coming up when the kubelet stops
	stopAtDrain                          // the replacement serves and the source drains when the kubelet stops
)

var outageStartNames = [...]string{"before-the-request", "at-accept", "at-surge", "at-drain"}

func (s outageStart) String() string { return outageStartNames[s] }

// outageEnd is when the kubelet starts again.
type outageEnd uint8

const (
	backNever        outageEnd = iota // the kubelet never reports again
	backSoon                          // back within a minute, before any policy acts
	backLater                         // back once the force-delete policy has had time to act
	backAfterTheRest                  // back once everything else has settled, done or holding
)

var outageEndNames = [...]string{"never-back", "back-soon", "back-later", "back-after-the-rest"}

func (e outageEnd) String() string { return outageEndNames[e] }

// outageTarget picks the Instance asked off its node.
type outageTarget uint8

const (
	outageBehind outageTarget = iota // the lowest Instance still on the old version with no attempt open
	outageLast                       // the highest such Instance: the one the push reaches last
	outageDone                       // the highest Instance already on the new version
	outageHeld                       // the lowest Instance the partition holds on the old version
)

var outageTargetNames = [...]string{"behind", "last", "done", "held"}

func (t outageTarget) String() string { return outageTargetNames[t] }

// outageProbe is one move request with one kubelet outage around it, at
// one moment, on one Instance.
type outageProbe struct {
	moment moveMoment
	start  outageStart
	end    outageEnd
	side   int
	target outageTarget
}

func (p outageProbe) String() string {
	return fmt.Sprintf("kubelet stops %s, %s, at %s on side %d (%s)", p.start, p.end, p.moment, p.side, p.target)
}

// outageWorld is the move model's world with the kubelet's return and
// the readings a request under an outage needs.
type outageWorld struct {
	*moveWorld
	// oside, source, identity and node are the probe's move: the side,
	// the Instance asked off its node, the Instance it stands for, and
	// the node whose kubelet stops.
	oside    *scaleSide
	source   int32
	identity int32
	node     string
	// podBefore is the routable pod of the source when the kubelet
	// stopped; stoppedAt is the clock then; back and backAt say the
	// kubelet started again and when.
	podBefore types.UID
	stoppedAt time.Time
	back      bool
	backAt    time.Time
	backTick  int
}

func newOutageWorld(t *testing.T, cfg moveConfig) *outageWorld {
	t.Helper()
	w := &outageWorld{moveWorld: newMoveWorld(t, cfg)}
	w.unnamedClass = w.unnamedWaitClass
	for _, s := range w.sides {
		// The status pass's deadline step runs after every pass, as it
		// does in the controller: a pause stops the clocks.
		s.h.statusDeadlineStep = true
	}
	return w
}

// unnamedWaitClass names an unnamed wait of a record whose source the
// roll's own attempt holds on a pod the stopped kubelet never removed:
// the drive defers a source mid-update without a word, and nothing ends
// the update while the kubelet stays away. Any other unnamed wait is the
// move model's to name.
func (w *outageWorld) unnamedWaitClass(s *scaleSide, rec workloadtypes.MigrationRecord) string {
	row := s.h.instance(rec.SourceInstance)
	if row == nil || row.Operation == nil || row.Operation.Type != workloadtypes.InstanceOperationUpdate || !w.podOnALostNode(s, w.identityOf(s, rec.SourceInstance)) {
		return ""
	}
	return "move-waits-unnamed-while-the-rolls-attempt-on-its-source-waits-on-a-lost-kubelet"
}

// startProbe puts the world at a snapshot with every reading cleared,
// the move model's and this file's.
func (w *outageWorld) startProbe(snap *scaleSnapshot) {
	w.moveWorld.startProbe(snap)
	w.oside, w.source, w.identity, w.node = nil, 0, 0, ""
	w.podBefore, w.stoppedAt, w.back, w.backAt, w.backTick = "", time.Time{}, false, time.Time{}, -1
}

// pickTarget is the Instance a probe's target names on a side, or false
// when the side has none.
func (w *outageWorld) pickTarget(s *scaleSide, target outageTarget) (int32, bool) {
	switch target {
	case outageBehind:
		return w.pick(s, targetBehind)
	case outageDone:
		return w.pick(s, targetDone)
	case outageHeld:
		return w.pick(s, targetHeld)
	}
	return w.pickLast(s)
}

// pickLast is the highest Instance still on the old version with no
// attempt open, a placed pod, and no partition holding it: the Instance
// the push reaches last.
func (w *outageWorld) pickLast(s *scaleSide) (int32, bool) {
	held := w.heldIndices(s)
	byIndex := w.livePodsByIndex(s)
	chosen, found := int32(0), false
	for _, r := range w.rows(s) {
		steady := r.Phase == workloadtypes.InstancePhaseReady && r.Operation == nil && routablePodPlaced(byIndex[r.Index])
		if !steady || r.RunningRevision == s.h.target.Name || held[r.Index] {
			continue
		}
		if !found || r.Index > chosen {
			chosen, found = r.Index, true
		}
	}
	return chosen, found
}

// routablePodPlaced reports a routable pod among pods that is bound to a
// node and not deleting.
func routablePodPlaced(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if query.RoutedRunner(pod) && pod.Spec.NodeName != "" && pod.DeletionTimestamp == nil {
			return true
		}
	}
	return false
}

// land plays the probe on the world: the request and the kubelet's stop
// in the order the probe names, and at idle the push after them, since
// with nothing under way the push is what lands beside the waiting move.
// It reports false when no Instance fits the probe's target.
func (w *outageWorld) land(p outageProbe) bool {
	s := w.sides[p.side]
	idx, ok := w.pickTarget(s, p.target)
	if !ok {
		return false
	}
	w.path = append(w.path, fmt.Sprintf("move of Instance %d of %s with the kubelet stopping %s and %s", idx, s.comp, p.start, p.end))
	if w.landed == 0 {
		w.markBuilds()
	}
	w.landed++
	w.probe, w.probeTick = moveProbe{moment: p.moment, verb: verbRequest, side: p.side, target: targetBehind}, w.mticks
	w.backoffAtMove = w.targetInBackoff(s)
	w.oside, w.source, w.identity = s, idx, w.identityOf(s, idx)
	switch p.start {
	case stopBeforeRequest:
		w.stopKubeletUnder(s, idx)
		w.tick()
		w.fileMoveRequestFrom(s, idx, w.node)
	case stopAtAccept:
		w.fileMoveRequest(s, idx)
		w.stopKubeletUnder(s, idx)
	default:
		uuid := w.fileMoveRequest(s, idx)
		w.tickUntilTheRecordReaches(s, uuid, p.start)
		if rec := s.h.migration(uuid); rec != nil && !rec.Phase.Terminal() {
			w.stopKubeletUnder(s, idx)
		}
	}
	if p.moment == whenIdle {
		w.push(fixedImage)
		w.request.pushedAfter = true
	}
	return true
}

// fileMoveRequestFrom files the operator's move of Instance idx off node,
// the node the operator sees the Instance on: a pod the roll has already
// deleted on a stopped kubelet is no longer live, and a request filed
// from the live pods would name no node.
func (w *outageWorld) fileMoveRequestFrom(s *scaleSide, idx int32, node string) string {
	uuid := w.fileMoveRequest(s, idx)
	if rec := s.h.migration(uuid); rec != nil && rec.FromNode == "" {
		rec.FromNode = node
		w.request.fromNode = node
	}
	return uuid
}

// tickUntilTheRecordReaches ticks until the record has allocated its
// replacement (at-surge) or reached its drain (at-drain), is terminal,
// or the wait is spent; under a pause the record cannot get there, so
// the kubelet goes at once.
func (w *outageWorld) tickUntilTheRecordReaches(s *scaleSide, uuid string, start outageStart) {
	for i := 0; i < outageAllocateTicks && !s.paused; i++ {
		rec := s.h.migration(uuid)
		if rec == nil || rec.Phase.Terminal() {
			return
		}
		if start == stopAtSurge && rec.SurgeInstance != nil {
			return
		}
		if start == stopAtDrain && workloadtypes.MigrationPhaseAtOrPast(rec.Phase, workloadtypes.MigrationPhaseDraining) {
			return
		}
		w.tick()
	}
}

// stopKubeletUnder stops the kubelet under Instance idx's routable pod,
// through the move model's node loss, and remembers the node, the pod
// and the clock for the readings.
func (w *outageWorld) stopKubeletUnder(s *scaleSide, idx int32) {
	w.node = placedNode(w.livePodsByIndex(s)[idx])
	w.stoppedAt, w.back = w.clk.Now(), false
	for _, pod := range s.h.podsOf(idx) {
		if query.RoutedRunner(pod) {
			w.podBefore = pod.UID
		}
	}
	w.loseNodeUnder(s, idx)
	w.path = append(w.path, "the kubelet under "+w.node+" stops")
}

// kubeletBack starts the kubelet under the probe's node again: the node
// posts Ready, the pods still on it report again, and a pod deleted on
// it finishes terminating. The node is no longer a lost one for the move
// model's readings.
func (w *outageWorld) kubeletBack() {
	if w.node == "" || w.back || !w.lostNodes[w.oside][w.node] {
		return
	}
	w.oside.h.recoverNode(w.node)
	delete(w.lostNodes[w.oside], w.node)
	w.back, w.backAt, w.backTick = true, w.clk.Now(), w.mticks
	w.path = append(w.path, "the kubelet under "+w.node+" starts again")
}

// play ticks the probe to its settled end under the probe's return plan:
// the kubelet comes back after its time away or once the world has
// settled without it, and the world plays out again after it. It reports
// whether the world settled.
func (w *outageWorld) play(p outageProbe) bool {
	switch p.end {
	case backNever:
		return w.playOut()
	case backSoon, backLater:
		away := outageSoon
		if p.end == backLater {
			away = outageLater
		}
		settling := w.startSettling()
		for i := 0; i < moveSettleTicks && w.clk.Now().Sub(w.stoppedAt) < away; i++ {
			if settling.observe(w.moveWorld, w.tick()) {
				break
			}
		}
	default:
		w.playOut()
	}
	w.kubeletBack()
	return w.playOut()
}

// excuseThePodThatCameBack takes off the move model's lost-Instance list
// an Instance whose routable pod is the one the outage stopped, live and
// Ready again after the kubelet's return: nothing rebuilt it because
// nothing had to. The move model would read it as never rebuilt.
func (w *outageWorld) excuseThePodThatCameBack() {
	if !w.back || w.oside == nil {
		return
	}
	s := w.oside
	for id := range w.moved[s] {
		if w.servesAgain(s, id) {
			delete(w.moved[s], id)
			w.note("pod-served-again-after-the-kubelet-returned", "%s Instance %d kept its pod through the outage and serves again on %s", s.comp, w.indexOf(s, id), w.node)
		}
	}
}

// servesAgain reports the pod the outage stopped standing live and Ready
// for the Instance id.
func (w *outageWorld) servesAgain(s *scaleSide, id int32) bool {
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || w.identityOf(s, idx) != id || pod.UID != w.podBefore {
			continue
		}
		return pod.DeletionTimestamp == nil && podConditionTrue(pod, corev1.PodReady)
	}
	return false
}

// readOutage reads the settled end of a probe that filed a request: the
// builds since the request, where the Instance stands, the rows left
// behind the move, and the record after the kubelet's return.
func (w *outageWorld) readOutage(settled bool) {
	if !settled || w.request == nil || w.node == "" {
		return
	}
	req := w.request
	rec := req.side.h.migration(req.uuid)
	if rec == nil {
		return
	}
	w.readBuildsSinceTheRequest(req, rec)
	w.readWhereTheInstanceStands(req, rec)
	w.readRowsAfterTheMove(req, rec)
	w.readRecordAfterTheReturn(req, rec)
}

// leaving is the node the request names, or the node the outage stopped
// when the record names none.
func (w *outageWorld) leaving(rec *workloadtypes.MigrationRecord) string {
	if rec.FromNode != "" {
		return rec.FromNode
	}
	return w.node
}

// readBuildsSinceTheRequest counts the builds of the moved Instance
// between the request and the move's own replacement, at any index: a
// rebuild by the roll or the repair that the move did not count as done.
// None is the promise; one is a second build for one request, named by
// where it landed. The builds after the replacement are the roll's, which
// the move model reads. A move closed as already left is satisfied by the
// rebuild, whose count the move model's lost-Instance reading holds.
func (w *outageWorld) readBuildsSinceTheRequest(req *moveRequestFacts, rec *workloadtypes.MigrationRecord) {
	if rec.Phase != workloadtypes.MigrationPhaseCompleted {
		return
	}
	s := req.side
	since := w.buildsBetween(s, req.identity, req.seqAt, 0)
	idx := w.indexOf(s, req.identity)
	if rec.SurgeInstance == nil {
		if len(since) == 0 {
			w.note("rebuilt-off-the-node-before-the-request-and-the-move-closed-satisfied", "the roll had rebuilt %s Instance %d off %s before the move %s was filed, and the move closed as already left: %s", s.comp, idx, w.leaving(rec), rec.RequestUUID, rec.Message)
			return
		}
		w.note("rebuilt-off-the-node-and-the-move-closed-satisfied", "%s Instance %d was rebuilt %d time(s) since the move %s was filed (%s), off %s, and the move closed as already left", s.comp, idx, len(since), rec.RequestUUID, buildList(since), w.leaving(rec))
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
	list := buildList(before)
	switch {
	case len(before) == 0:
		w.note("built-once-since-the-request", "%s Instance %d was built once since the move %s was filed, by the move, at index %d", s.comp, req.source, rec.RequestUUID, *rec.SurgeInstance)
	case len(before) == 1 && before[0].node == w.leaving(rec) && w.back && before[0].tick >= w.backTick:
		w.note("rebuilt-on-the-requests-node-after-the-kubelet-returned-then-moved-again", "the roll rebuilt %s Instance %d on %s, the node the move %s names, once its kubelet was back, and the move built it again at index %d: 2 builds for one request (%s)", s.comp, req.source, before[0].node, rec.RequestUUID, *rec.SurgeInstance, list)
	case len(before) == 1 && before[0].node == w.leaving(rec):
		w.note("rebuilt-on-the-requests-node-while-its-kubelet-was-away-then-moved-again", "%s Instance %d was rebuilt on %s while its kubelet was away, and the move %s built it again: 2 builds for one request (%s)", s.comp, req.source, before[0].node, rec.RequestUUID, list)
	case len(before) == 1:
		w.note("moved-again-after-a-rebuild-off-the-requests-node", "%s Instance %d was rebuilt on %s, off %s, and the move %s still built it again at index %d: 2 builds for one request (%s): %s", s.comp, req.source, before[0].node, w.leaving(rec), rec.RequestUUID, *rec.SurgeInstance, list, w.picture())
	default:
		w.note("rebuilt-more-than-once-before-the-move", "%s Instance %d was built %d times between the move %s and its replacement (%s): %s", s.comp, req.source, len(before), rec.RequestUUID, list, w.picture())
	}
}

// buildList describes builds as index:revision@node in the order built.
func buildList(builds []moveBuild) string {
	var parts []string
	for _, b := range builds {
		parts = append(parts, fmt.Sprintf("%d:%s@%s", b.index, b.revision, b.node))
	}
	return strings.Join(parts, " ")
}

// nodesOf lists the nodes the live routable pods of an Instance stand on.
func (w *outageWorld) nodesOf(s *scaleSide, id int32) string {
	var nodes []string
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if ok && w.identityOf(s, idx) == id && query.RoutedRunner(pod) && pod.DeletionTimestamp == nil {
			nodes = append(nodes, pod.Spec.NodeName)
		}
	}
	if len(nodes) == 0 {
		return "no node"
	}
	return strings.Join(nodes, ",")
}

// readWhereTheInstanceStands reads a completed move whose Instance has a
// live routable pod on the node the request names: a pod the move itself
// left there is a finding; a pod the roll built there after the move
// closed is the roll's placement, which a completed request does not
// bind.
func (w *outageWorld) readWhereTheInstanceStands(req *moveRequestFacts, rec *workloadtypes.MigrationRecord) {
	if rec.Phase != workloadtypes.MigrationPhaseCompleted {
		return
	}
	s := req.side
	closedAt := 0
	if trail := w.records[rec.RequestUUID]; trail != nil {
		closedAt = trail.completedSeq
	}
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || w.identityOf(s, idx) != req.identity || !query.RoutedRunner(pod) || pod.DeletionTimestamp != nil || pod.Spec.NodeName != w.leaving(rec) {
			continue
		}
		if w.buildSeqOf(s, pod.UID) > closedAt {
			w.note("rolled-back-onto-the-node-after-the-move-completed", "the move %s closed Completed (%s); the roll then rebuilt %s Instance %d and placed pod %s on %s, the node the move left: %s", rec.RequestUUID, rec.Message, s.comp, idx, pod.Name, pod.Spec.NodeName, w.picture())
			continue
		}
		w.note("instance-left-on-the-node-the-move-names", "the move %s closed Completed (%s) and %s Instance %d still runs pod %s on %s, the node it was asked to leave: %s", rec.RequestUUID, rec.Message, s.comp, idx, pod.Name, pod.Spec.NodeName, w.picture())
	}
}

// buildSeqOf is the build number of the pod with uid on side s, zero for
// a pod the model never saw as a build.
func (w *outageWorld) buildSeqOf(s *scaleSide, uid types.UID) int {
	for _, b := range w.builds[s] {
		if b.uid == uid {
			return b.seq
		}
	}
	return 0
}

// readRowsAfterTheMove reads the rows a completed move with a replacement
// leaves: one row for the Instance, and none at the source's index.
func (w *outageWorld) readRowsAfterTheMove(req *moveRequestFacts, rec *workloadtypes.MigrationRecord) {
	if rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance == nil {
		return
	}
	s := req.side
	var at []int32
	for _, r := range w.rows(s) {
		if w.identityOf(s, r.Index) == req.identity {
			at = append(at, r.Index)
		}
	}
	if len(at) > 1 {
		w.note("moved-instance-at-two-indexes", "%s Instance %d has rows at %v after the move %s completed: %s", s.comp, req.source, at, rec.RequestUUID, w.picture())
	}
	for _, idx := range at {
		if idx == rec.SourceInstance {
			w.note("source-row-left-after-the-move-completed", "%s row %d, the source of the move %s, is still there after the move completed: %s", s.comp, idx, rec.RequestUUID, w.picture())
		}
	}
}

// readRecordAfterTheReturn reads the record once the kubelet is back: a
// request that expired after its Instance could report again, or one
// still open after the roll finished around it.
func (w *outageWorld) readRecordAfterTheReturn(req *moveRequestFacts, rec *workloadtypes.MigrationRecord) {
	if !w.back {
		return
	}
	s := req.side
	trail := w.records[rec.RequestUUID]
	switch {
	case rec.Phase == workloadtypes.MigrationPhaseFailed && strings.Contains(rec.Message, "surge never allocated") && trail != nil && trail.completedAt >= w.backTick:
		w.note("move-expired-after-the-kubelet-returned", "the move %s expired after the kubelet under %s was back, with %s Instance %d on %s: %s: %s", rec.RequestUUID, w.node, s.comp, w.indexOf(s, req.identity), w.nodesOf(s, req.identity), rec.Message, w.picture())
	case !rec.Phase.Terminal() && !s.paused && w.complete():
		w.note("move-open-after-the-roll-finished-and-the-kubelet-returned", "the move %s stands in phase %s with the text %q after the kubelet under %s came back and the roll finished: %s", rec.RequestUUID, rec.Phase, rec.Message, w.node, w.picture())
	}
}

// outageMoments lists the moments a probe may land at, in the order the
// explore plays them.
var outageMoments = []moveMoment{whenIdle, whenSettling, whenRotating, whenDraining, whenLast, whenCanaryStep, whenHeld, whenPaused, whenScalingUp, whenScalingDown}

// outageShape is one probe shape: when the kubelet stops, when it
// returns, and which Instance is asked off its node.
type outageShape struct {
	start  outageStart
	end    outageEnd
	target outageTarget
}

// boundedOutageShapes keeps, per moment, the probes operators
// meet most, so the bounded run stays inside its time: the picture of a
// kubelet that stops before the request and returns once the push has
// gone around the Instance, and a probe of every other start and end.
var boundedOutageShapes = map[moveMoment][]outageShape{
	whenIdle:        {{stopBeforeRequest, backAfterTheRest, outageDone}, {stopAtDrain, backSoon, outageDone}, {stopAtAccept, backNever, outageDone}, {stopAtSurge, backNever, outageDone}},
	whenSettling:    {{stopBeforeRequest, backSoon, outageLast}},
	whenRotating:    {{stopBeforeRequest, backLater, outageBehind}, {stopAtAccept, backSoon, outageBehind}, {stopAtSurge, backLater, outageDone}, {stopAtDrain, backNever, outageDone}, {stopBeforeRequest, backAfterTheRest, outageLast}, {stopAtSurge, backSoon, outageBehind}, {stopAtDrain, backLater, outageBehind}, {stopBeforeRequest, backNever, outageLast}},
	whenDraining:    {{stopAtAccept, backLater, outageBehind}},
	whenLast:        {{stopBeforeRequest, backSoon, outageLast}, {stopBeforeRequest, backLater, outageLast}, {stopAtAccept, backAfterTheRest, outageLast}, {stopAtDrain, backSoon, outageDone}},
	whenCanaryStep:  {{stopAtAccept, backSoon, outageDone}},
	whenPaused:      {{stopAtAccept, backAfterTheRest, outageBehind}},
	whenScalingUp:   {{stopBeforeRequest, backSoon, outageBehind}},
	whenScalingDown: {{stopAtDrain, backLater, outageDone}},
}

// decodeOutageShapes are the decode side's probes of the bounded run at
// the rotating moment: the two gang pictures, a kubelet that never
// returns before the request and at the drain, and one that returns
// soon on the Instance the push reaches last.
var decodeOutageShapes = []outageShape{{stopBeforeRequest, backNever, outageBehind}, {stopAtDrain, backNever, outageDone}, {stopBeforeRequest, backSoon, outageLast}}

// probesAt lists the probes tried from a moment: the bounded table on the
// prefill side with the decode side's shapes at the rotating moment; in
// full mode every start, end and target on both sides at the rotating and
// last moments, where every known picture lands, and the bounded
// table elsewhere.
func (w *outageWorld) probesAt(m moveMoment, full bool) []outageProbe {
	if !full || (m != whenRotating && m != whenLast) {
		return w.boundedProbesAt(m)
	}
	sides := []int{0}
	if d := w.decodeSide(); d >= 0 {
		sides = append(sides, d)
	}
	targets := []outageTarget{outageBehind, outageLast, outageDone}
	if w.cfg.settings.partition > 0 {
		targets = append(targets, outageHeld)
	}
	var out []outageProbe
	for _, side := range sides {
		for start := stopBeforeRequest; start <= stopAtDrain; start++ {
			for end := backNever; end <= backAfterTheRest; end++ {
				for _, target := range targets {
					out = append(out, outageProbe{moment: m, start: start, end: end, side: side, target: target})
				}
			}
		}
	}
	return out
}

// boundedProbesAt is the bounded table's probes at a moment.
func (w *outageWorld) boundedProbesAt(m moveMoment) []outageProbe {
	var out []outageProbe
	for _, shape := range boundedOutageShapes[m] {
		out = append(out, outageProbe{moment: m, start: shape.start, end: shape.end, side: 0, target: shape.target})
	}
	if d := w.decodeSide(); d >= 0 && m == whenRotating {
		for _, shape := range decodeOutageShapes {
			out = append(out, outageProbe{moment: m, start: shape.start, end: shape.end, side: d, target: shape.target})
		}
	}
	return out
}

// decodeSide is the index of the decode side among the sides, -1 when
// the shape has none.
func (w *outageWorld) decodeSide() int {
	for i, s := range w.sides {
		if s.comp == workloadtypes.ComponentDecoder {
			return i
		}
	}
	return -1
}

// explore runs one configuration: the moments kept, every probe from
// every moment played to its end under its return plan, then the random
// runs.
func (w *outageWorld) explore(full bool, seed int64, runs int) {
	w.t.Helper()
	budget := outageTickBudgetBounded
	if full {
		budget = outageTickBudgetFull
	}
	w.reachIdle()
	snaps := w.momentSnapshots(full)
	probesAt := func(m moveMoment) []outageProbe { return w.probesAt(m, full) }
	playProbesFromTheMoments(w.moveWorld, w, snaps, outageMoments, probesAt, budget)
	w.randomRuns(snaps[whenIdle], seed, runs, budget)
}

// finishProbe reads a probe's end: the move model's readings, with the
// pod that came back excused, then this file's.
func (w *outageWorld) finishProbe(settled bool) {
	w.excuseThePodThatCameBack()
	w.classifyProbe(settled)
	w.readOutage(settled)
}

// randomRuns plays the roll from idle and lands one probe of a random
// shape at a random tick of it, under a fixed seed.
func (w *outageWorld) randomRuns(idle *scaleSnapshot, seed int64, runs, budget int) {
	w.t.Helper()
	rng := rand.New(rand.NewSource(seed))
	for run := 0; run < runs; run++ {
		if w.mticks >= budget || idle == nil {
			w.skipped++
			continue
		}
		w.startProbe(idle)
		w.path = append(w.path, fmt.Sprintf("random run %d", run+1))
		w.push(fixedImage)
		w.probes++
		p := outageProbe{moment: whenRotating, start: outageStart(rng.Intn(4)), end: outageEnd(rng.Intn(4)), target: outageTarget(rng.Intn(3))}
		if d := w.decodeSide(); d >= 0 && rng.Intn(2) == 0 {
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

// outageReferenceConfigs are the configurations every run explores: the
// single-pod 12:8 recreate shape and the 3:3 gangs of two under surge,
// the engine-only reference shape, and the prefill/decode surge pair, each
// under its reference force-delete setting.
func outageReferenceConfigs() []moveConfig {
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	return []moveConfig{
		{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}}, forceDeleteOff: true},
		{scaleConfig: scaleConfig{shape: scaleShape{3, 3, 2}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}},
		{scaleConfig: scaleConfig{shape: scaleShape{16, 0, 1}, settings: scaleSettings{prefill: recreate}}, forceDeleteOff: true},
		{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true, firstSide: v1beta1.DecoderComponent}}},
	}
}

// outageConfigs lists the configurations of a run: the reference ones,
// and in full mode the same with the policy flipped, every reference
// prefill/decode shape under the two reference settings, gangs, a
// partition, a canary, a blueGreen group, a 25 percent budget and a
// scaling policy.
func outageConfigs(full bool) []moveConfig {
	configs := outageReferenceConfigs()
	if !full {
		return configs
	}
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	for _, cfg := range outageReferenceConfigs() {
		cfg.forceDeleteOff = !cfg.forceDeleteOff
		configs = append(configs, cfg)
	}
	for _, shape := range [][2]int32{{15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}} {
		configs = append(configs,
			moveConfig{scaleConfig: scaleConfig{shape: scaleShape{shape[0], shape[1], 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}}, forceDeleteOff: true},
			moveConfig{scaleConfig: scaleConfig{shape: scaleShape{shape[0], shape[1], 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true, firstSide: v1beta1.DecoderComponent}}})
	}
	return append(configs,
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{4, 4, 4}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 2, 8}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}, forceDeleteOff: true},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{8, 0, 4}, settings: scaleSettings{prefill: recreate}}},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, partition: 2}}},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: recreate, decode: surge, canary: true}}},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, blueGreen: true, budget: "25%"}}},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 0, 1}, settings: scaleSettings{prefill: surge, budget: "25%"}}, forceDeleteOff: true},
		moveConfig{scaleConfig: scaleConfig{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: surge, decode: recreate, keepRatio: true, firstSide: v1beta1.DecoderComponent, policy: true}}},
	)
}

// outageExpectedClasses are the readings of this file a probe may end
// with that are not findings, beside the move model's.
var outageExpectedClasses = map[string]bool{
	"built-once-since-the-request":                                          true,
	"rebuilt-off-the-node-and-the-move-closed-satisfied":                    true,
	"rebuilt-off-the-node-before-the-request-and-the-move-closed-satisfied": true,
	"pod-served-again-after-the-kubelet-returned":                           true,
	// The scaling world notes a shape whose held moment it does not reach
	// within the bound; no probe lands at that moment for the shape.
	"held-not-reached": true,
}

// knownOutageClasses are the readings of this file the model shows on
// the current engine, each with the picture behind it. A finding in a known
// class is reported and does not fail the run; a change of behaviour removes its entry
// and the lock test that holds the picture.
var knownOutageClasses = map[string]string{
	"rebuilt-on-the-requests-node-after-the-kubelet-returned-then-moved-again":         "the roll's rebuild of an Instance with a pending move request lands on the node the request names once that node's kubelet is back, the least-loaded node then, and the move builds the Instance again elsewhere: two builds for one request; the rebuild of an Instance with a pending request does not keep off the request's node (held by TestMoveKubeletOutage_ARebuildAfterTheKubeletReturnsLandsOnTheNodeTheMoveNamesAndIsMovedAgain)",
	"rolled-back-onto-the-node-after-the-move-completed":                               "a completed move leaves no exclusion for the node it left, so a later rebuild by the roll may place the Instance there again once that node is the least loaded (held by TestMoveKubeletOutage_ACompletedMoveLeavesNoExclusionForTheNodeItLeft)",
	"move-waits-unnamed-while-the-rolls-attempt-on-its-source-waits-on-a-lost-kubelet": "a request on an Instance whose roll attempt waits on a pod a stopped kubelet never removes is deferred without a word, since the drive names only the waits no operation in flight ends, and stands Accepted with no text until its deadline closes it Failed (held by TestMoveKubeletOutage_ARequestOnASourceHeldOnALostKubeletWaitsUnnamedAndExpires)",
}

// outageKnownClasses are every known class a run reads against: the
// scaling model's, the move model's and this file's.
func outageKnownClasses() map[string]string {
	out := scaleKnownClasses()
	for class, text := range knownOutageClasses {
		out[class] = text
	}
	return out
}

// exploreOutageConfig explores one configuration and reports what it found.
func exploreOutageConfig(t *testing.T, cfg moveConfig, full bool, seed int64, runs int) probeResult[moveConfig] {
	t.Helper()
	began := time.Now()
	w := newOutageWorld(t, cfg)
	w.explore(full, seed, runs)
	return probeResultOf(w.moveWorld, cfg, began)
}

// outageExplore runs the model over the configurations keep admits, a
// bounded number at a time in parallel, and reports every class.
func outageExplore(t *testing.T, keep func(moveConfig) bool) {
	t.Helper()
	runModel(t, modelRun[moveConfig]{fullEnv: kubeletOutageFullEnv, onlyEnv: kubeletOutageOnlyEnv, runs: 2, fullRuns: 4, fullWorkers: outageWorkers, configs: outageConfigs, explore: exploreOutageConfig, known: outageKnownClasses(), expected: outageExpectedClasses}, keep)
}

// TestMoveKubeletOutage_EveryOutageEndsWithTheInstanceBuiltOnceOffTheNode
// explores every configuration: a move request on an Instance whose
// kubelet stops before, at or after the request and returns soon, later,
// after everything else has settled, or never, at every moment of a
// roll, leaves the Instance built once since the request, off the node
// the request names, with the record closed or naming its wait, the
// roll finished or holding for a reason it names, and no row left behind
// the move.
func TestMoveKubeletOutage_EveryOutageEndsWithTheInstanceBuiltOnceOffTheNode(t *testing.T) {
	outageExplore(t, func(moveConfig) bool { return true })
}

// playOneProbe plays one probe from a moment on a fresh world to its
// settled end and returns the world for a test's own reading.
func playOneProbe(t *testing.T, cfg moveConfig, p outageProbe) *outageWorld {
	t.Helper()
	w := newOutageWorld(t, cfg)
	playOneProbeFromTheMoment(t, w.moveWorld, w, p.moment, p, "Instance")
	return w
}

// podsOffTheNode fails the test when a live routable pod of the moved
// Instance still stands on the node the request names.
func (w *outageWorld) podsOffTheNode(t *testing.T, s *scaleSide, id int32) {
	t.Helper()
	for _, pod := range w.pods(s) {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if ok && w.identityOf(s, idx) == id && query.RoutedRunner(pod) && pod.DeletionTimestamp == nil && pod.Spec.NodeName == w.node {
			t.Errorf("%s Instance %d still runs pod %s on %s, the node it was asked to leave; picture: %s", s.comp, idx, pod.Name, w.node, w.picture())
		}
	}
}

// TestMoveKubeletOutage_ARebuildAfterTheKubeletReturnsLandsOnTheNodeTheMoveNamesAndIsMovedAgain
// holds the current picture of a rebuild under a kubelet that returns: 12
// prefill and 8 decode under RecreatePod at ten percent with the ratio
// rule and no force-delete policy. While the roll settles, the kubelet
// under the prefill Instance the push reaches last stops; the operator
// asks that Instance off its node; the kubelet starts again a minute
// later. The roll reaches the Instance, deletes its pod and rebuilds it
// at its index on version 2, on the node the request names, since that
// node is back and carries nothing. The move then builds it again at a
// new index on another node: two builds for one request. A rule that
// keeps the rebuild off the request's node flips the two checks: one
// build since the request, and the move closes as already left.
func TestMoveKubeletOutage_ARebuildAfterTheKubeletReturnsLandsOnTheNodeTheMoveNamesAndIsMovedAgain(t *testing.T) {
	recreate := workloadtypes.UpdateStrategyRecreatePod
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}}, forceDeleteOff: true}
	w := playOneProbe(t, cfg, outageProbe{moment: whenSettling, start: stopBeforeRequest, end: backSoon, side: 0, target: outageLast})
	s := w.sides[0]
	req := w.request
	rec := s.h.migration(req.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted {
		t.Fatalf("the move should close Completed; got %v; picture: %s", rec, w.picture())
	}
	if !w.complete() {
		t.Fatalf("the roll should finish around the move; picture: %s", w.picture())
	}
	w.podsOffTheNode(t, s, req.identity)
	builds := w.buildsBetween(s, req.identity, req.seqAt, 0)
	if len(builds) != 2 || builds[0].index != req.source || builds[0].node != w.node || rec.SurgeInstance == nil || builds[1].index != *rec.SurgeInstance {
		t.Fatalf("current picture: the roll rebuilds Instance %d on %s once the kubelet is back and the move builds it again; builds %s, record %v", req.source, w.node, buildList(builds), rec)
	}
	if n := w.counts["rebuilt-on-the-requests-node-after-the-kubelet-returned-then-moved-again"]; n != 1 {
		t.Fatalf("the model should read the two builds once; read %d; classes %v", n, sortedFindingClasses(w.findings))
	}
}

// TestMoveKubeletOutage_ARequestOnASourceHeldOnALostKubeletWaitsUnnamedAndExpires
// holds the current picture on the same shape: while the roll rotates, the
// kubelet under a decode Instance still on version 1 stops; the roll's
// recreate of that Instance deletes its pod, which stays Terminating
// with no force-delete policy; the operator asks the Instance off that
// node. The record stands Accepted with no text for the whole of its
// deadline, then closes Failed as never allocated. A rule that names the
// wait on the record removes the unnamed reading.
func TestMoveKubeletOutage_ARequestOnASourceHeldOnALostKubeletWaitsUnnamedAndExpires(t *testing.T) {
	recreate := workloadtypes.UpdateStrategyRecreatePod
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}}, forceDeleteOff: true}
	w := playOneProbe(t, cfg, outageProbe{moment: whenRotating, start: stopBeforeRequest, end: backNever, side: 1, target: outageBehind})
	s := w.sides[1]
	rec := s.h.migration(w.request.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseFailed || !strings.Contains(rec.Message, "surge never allocated") {
		t.Fatalf("current picture: the record expires as never allocated; got %v; picture: %s", rec, w.picture())
	}
	if n := w.counts["move-waits-unnamed-while-the-rolls-attempt-on-its-source-waits-on-a-lost-kubelet"]; n == 0 {
		t.Fatalf("the model should read the unnamed wait on the held source; classes %v", sortedFindingClasses(w.findings))
	}
	if trail := w.records[rec.RequestUUID]; trail == nil || trail.unnamedMost < moveUnnamedTicks {
		t.Fatalf("the record should stand unnamed for at least %d ticks; trail %+v", moveUnnamedTicks, trail)
	}
}

// TestMoveKubeletOutage_ASourcePodThatReportsAgainIsMovedOnce holds the
// engine-only reference shape: 16 Instances under RecreatePod at ten percent,
// no force-delete policy. While the roll rotates the operator asks an
// Instance already on version 2 off its node, and the kubelet under it
// stops before the engine's next pass and starts again a minute later.
// The pod reports again, the move builds its replacement once, off the
// node, and the roll finishes around it.
func TestMoveKubeletOutage_ASourcePodThatReportsAgainIsMovedOnce(t *testing.T) {
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{16, 0, 1}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod}}, forceDeleteOff: true}
	w := playOneProbe(t, cfg, outageProbe{moment: whenRotating, start: stopAtAccept, end: backSoon, side: 0, target: outageDone})
	s := w.sides[0]
	req := w.request
	rec := s.h.migration(req.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance == nil {
		t.Fatalf("the move should close Completed with a replacement; got %v; picture: %s", rec, w.picture())
	}
	if !w.complete() {
		t.Fatalf("the roll should finish around the move; picture: %s", w.picture())
	}
	w.podsOffTheNode(t, s, req.identity)
	if builds := w.buildsBetween(s, req.identity, req.seqAt, 0); len(builds) != 1 || builds[0].index != *rec.SurgeInstance {
		t.Fatalf("the Instance should be built once since the request, by the move; builds %s", buildList(builds))
	}
}

// TestMoveKubeletOutage_APairOpenUnderALongPauseIsBuiltOnceAfterTheResume
// holds the world's status step: three prefill and three decode gangs
// of two under SurgeThenDrain with the ratio rule; the roll is paused
// with a gang surge pair open and resumed forty minutes later, longer
// than the pair's thirty-minute window. The pause stops the pair's
// clock and the resume starts it again, so the new gang is built once
// and the roll finishes. Without the step the resume reads the pair as
// overdue, deletes the gang it just built and builds it again.
func TestMoveKubeletOutage_APairOpenUnderALongPauseIsBuiltOnceAfterTheResume(t *testing.T) {
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{3, 3, 2}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}}
	w := newOutageWorld(t, cfg)
	w.reachIdle()
	snap := w.momentSnapshots(false)[whenPaused]
	if snap == nil {
		t.Fatalf("the roll never reached its paused moment")
	}
	w.startProbe(snap)
	s := w.sides[0]
	pairs := 0
	for _, r := range w.rows(s) {
		if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationUpdate && r.Operation.Step == workloadtypes.UpdateStepSurge {
			pairs++
		}
	}
	if pairs == 0 {
		t.Fatalf("the pause should hold a gang surge pair open; picture: %s", w.picture())
	}
	w.markBuilds()
	for pausedAt := w.clk.Now(); w.clk.Now().Sub(pausedAt) < 40*time.Minute; {
		w.tick()
	}
	w.setPaused(false)
	w.path = append(w.path, "resume after forty minutes")
	if !w.playOut() || !w.complete() {
		t.Fatalf("the roll should finish after the resume; picture: %s", w.picture())
	}
	for _, e := range s.h.events {
		if strings.Contains(e, " GangSurgeAbandoned ") {
			t.Fatalf("the resume should not retire the pair's new gang: %s", e)
		}
	}
	for _, r := range w.rows(s) {
		if builds := w.buildsSince(s, w.identityOf(s, r.Index)); len(builds) > 1 {
			t.Fatalf("%s Instance %d should be built once after the pause; builds %s", s.comp, r.Index, buildList(builds))
		}
	}
}

// TestMoveKubeletOutage_ACompletedMoveLeavesNoExclusionForTheNodeItLeft
// holds the current rule on two gangs of two under RecreatePod with the
// force-delete policy: the operator asks a gang off its node with no
// roll open, the replacement serves, the kubelet under the source stops
// as the source drains and starts again a minute later, and a push lands
// beside the move. The move closes Completed; the plan then carries no
// exclusion for the node the move left, so the roll's recreate of the
// moved gang may place it there again. A rule that keeps a completed
// move's node off the Instance flips the plan check.
func TestMoveKubeletOutage_ACompletedMoveLeavesNoExclusionForTheNodeItLeft(t *testing.T) {
	cfg := moveConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 2}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod}}}
	w := playOneProbe(t, cfg, outageProbe{moment: whenIdle, start: stopAtDrain, end: backSoon, side: 0, target: outageDone})
	s := w.sides[0]
	req := w.request
	rec := s.h.migration(req.uuid)
	if rec == nil || rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance == nil {
		t.Fatalf("the move should close Completed with a replacement; got %v; picture: %s", rec, w.picture())
	}
	if !w.complete() {
		t.Fatalf("the push should finish around the move; picture: %s", w.picture())
	}
	in := s.h.buildInput()
	plan, err := workload.BuildPlan(s.h.component, s.h.desired, in.ObservedState)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	for _, inst := range plan.Instances {
		if w.identityOf(s, inst.Index) != req.identity {
			continue
		}
		for _, exclusion := range inst.ExcludedNodes {
			if exclusion.Node == w.node {
				t.Fatalf("current rule: a completed move leaves no exclusion; Instance %d carries %v", inst.Index, inst.ExcludedNodes)
			}
		}
	}
	t.Logf("the moved gang stands on %s after the push; the node it left was %s; classes %v", w.nodesOf(s, req.identity), w.node, sortedFindingClasses(w.findings))
}
