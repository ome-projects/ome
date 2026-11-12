package workload_test

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the production engine of a prefill/decode service
// through the loss of a node under an open roll and reads what the repair
// of the lost Instances does to the version mix the coordination gates
// keep. It runs on the world of scale_beside_a_roll_model_test.go: one
// workload.Reconcile loop per Component on the recovery harness, the
// coordination gates wired between the Components the way the replica
// adapter wires them, the follower count of a scaling policy and the
// partition a canary step projects, all from production code.
//
// The rule under test. An Instance that loses every pod while a roll is
// open is handed to the roll (ops/restart.go:RepairYieldsToTarget), its
// row is written Pending in the pass after the loss (plan_decision.go:
// planUnbackedDemotions) and the Create pass builds it at the target in
// the next pass with no coordination gate and no budget asked
// (ops/create.go:createRevision), under SurgeThenDrain and RecreatePod
// alike. An Instance whose pods are still listed but serve nothing (a node
// that stopped reporting, a gang short of a member) is the update pass's:
// its start skips the coordination gate (plan_decision.go:
// skipsCoordinationGate reads the row outside the gate's serving count;
// pass_update.go:updateAdmission.admit skips the consult on
// CoordGateExempt) and answers to the Component's own budget alone. Neither
// path reads the revision the rebuild targets or how many Instances were
// lost at once. A rebuild on the revision the Instance ran keeps the
// version mix; a rebuild on the new version is a roll step and moves it.
//
// The world's move is a lost node. The default scheduler spreads a
// service's pods, so one node holds about three or four pods of both
// sides; the gang scheduler packs them, so one node can end up holding most
// of one side (nine prefill pods of the 16:8 shape here and no decode pod). The model takes 1, 3, 9 and 16
// Instances of the prefill side, of the decode side, or of both sides in
// proportion; the node is deleted from the API with its kubelet stopped
// (every pod it held leaves the API at once), or it stops reporting (its
// pods stay out of rotation until the force-delete policy frees them), or,
// on gangs, it held one worker of each gang; the Instances it held are the
// lowest ones still on the old version, or in full mode the highest ones
// already promoted. Each loss lands at every moment of the roll (idle at
// the push, settling, rotating, draining, the last Instance, held on a
// spent retry schedule, a canary step) on every configuration of the scale
// model, and the roll is then played to its end. Seeded random runs mix a
// loss with the operator's moves.
//
// What fails the test. At every tick: a side above its own budget, read
// the way the engine charges it; a side with more Instances built than its
// surge budget allows above its count; and, under a ratio rule, the two
// sides' new-version shares further apart than the tolerance plus one
// step of the leading side, with the share the rebuilds added read apart
// from the share the gates admitted. At every settled state: the roll
// complete, or held where it stood held before the loss, with no two gates
// waiting on each other and no side standing still without a hold. A loss
// that leaves a retry schedule spent, a lost Instance never rebuilt, or a
// hold with no way out is a finding. The time from the loss until every
// lost Instance has a replacement pod, and until every one serves again,
// is read per loss and reported.
//
// OME_REPAIR_RATIO_HUNT=full widens the configurations, the moments, the
// losses and the random runs; OME_REPAIR_RATIO_HUNT_ONLY=<text> keeps the
// configurations whose name carries the text.

const (
	repairRatioFullEnv = "OME_REPAIR_RATIO_HUNT"
	repairRatioOnlyEnv = "OME_REPAIR_RATIO_HUNT_ONLY"
)

// lossScope is the side or sides a lost node held pods of.
type lossScope uint8

const (
	lossPrefill   lossScope = iota // the node held prefill pods alone
	lossDecode                     // the node held decode pods alone
	lossBothSides                  // the node held pods of both sides, in the fleet's proportion
)

var lossScopeNames = [...]string{"prefill", "decode", "both sides"}

func (s lossScope) String() string { return lossScopeNames[s] }

// lossKind is how the pods the node held leave.
type lossKind uint8

const (
	lossGone       lossKind = iota // the node is deleted from the API with its kubelet stopped: every pod it held leaves the API at once
	lossUnreported                 // the node stops reporting: its pods stay, out of rotation, until the force-delete policy frees them
	lossMember                     // the node held one worker of each gang: each gang is short a member at once
)

var lossKindNames = [...]string{"node deleted", "node stopped reporting", "a worker of each gang gone"}

func (k lossKind) String() string { return lossKindNames[k] }

// lossPick is which Instances the node held.
type lossPick uint8

const (
	pickOldFirst lossPick = iota // the lowest Instances still on the old version, then promoted ones
	pickNewFirst                 // the highest promoted Instances, then old ones
)

var lossPickNames = [...]string{"old first", "promoted first"}

func (p lossPick) String() string { return lossPickNames[p] }

// repairLoss is one lost node: how many Instances it held, of which side
// or sides, which ones, and how their pods leave.
type repairLoss struct {
	scope lossScope
	count int32
	kind  lossKind
	pick  lossPick
}

func (l repairLoss) String() string {
	return fmt.Sprintf("%s under %d %s Instances, %s", l.kind, l.count, l.scope, l.pick)
}

// lossPart is the share of one loss that lands on one side.
type lossPart struct {
	side  int
	count int32
}

// repairRestore is how long a loss took to repair: ticks and clock from
// the loss until every lost Instance had a replacement pod, and until
// every one served again.
type repairRestore struct {
	loss          string
	builtTicks    int
	restoredTicks int
	restoredAfter time.Duration
}

// repairLead is the two sides' new-version shares at one tick: the moved
// and movable Instances of each, the gap between the shares and the gap
// the ratio rule allows, and how many Instances the rebuilds added to the
// leading side's share beyond what the gates admitted.
type repairLead struct {
	prefillMoved, prefillMovable, decodeMoved, decodeMovable int32
	step                                                     int32
	gap, allowed                                             float64
	rebuilt                                                  int32
}

// repairRatioModel runs the losses over one configuration of the scale
// world.
type repairRatioModel struct {
	w    *scaleWorld
	full bool
	// bound is the most ticks one play may take; budget the most ticks one
	// configuration may spend, the losses left past it counted as skipped
	// so a slower machine reads the same first losses.
	bound, budget int
	// lost is, per side, the Instances the play's loss took pods from;
	// startedAtLoss is, per side, the Instances that had moved toward the
	// target when the loss landed, so a rebuild on the new version can be
	// told from a start the gates admitted.
	lost          map[*scaleSide]map[int32]bool
	startedAtLoss map[*scaleSide]map[int32]bool
	// heldBefore is whether a side stood held on a spent retry schedule
	// when the loss landed: such a roll owes a held end.
	heldBefore bool
	// lossTick and lossAt are when the play's loss landed; builtTick and
	// restoredTick are the ticks after it when every lost Instance had a
	// replacement pod and when every one served again, -1 until then.
	lossTick                int
	lossAt                  time.Time
	builtTick, restoredTick int
	// lossNodes counts the nodes this model added, so each has its own name.
	lossNodes int
	// restores keeps the longest repair per loss text; worstLead the widest
	// gap the rebuilds opened and the play that opened it.
	restores      map[string]repairRestore
	worstLead     repairLead
	worstLeadPath string
	// waits counts, per hold gate, the ticks a lost Instance stood with no
	// replacement pod while its side named that hold.
	waits                 map[string]int
	probes, skipped, runs int
}

func newRepairRatioModel(w *scaleWorld, full bool) *repairRatioModel {
	budget := 5000
	if full {
		budget = 40000
	}
	return &repairRatioModel{w: w, full: full, bound: scaleTickBound(full), budget: budget,
		restores: map[string]repairRestore{}, waits: map[string]int{}}
}

// reset clears the play's loss.
func (m *repairRatioModel) reset() {
	m.lost = map[*scaleSide]map[int32]bool{}
	m.startedAtLoss = map[*scaleSide]map[int32]bool{}
	m.lossTick, m.builtTick, m.restoredTick = 0, -1, -1
	m.heldBefore = false
}

// moments are the moments a loss lands at: in bounded mode the push, the
// first batch, the rotating roll, the last Instance and the held roll; in
// full mode the draining roll and the canary step too.
func (m *repairRatioModel) moments() []scaleMoment {
	if m.full {
		return []scaleMoment{scaleAtIdle, scaleAtSettling, scaleAtRotating, scaleAtDraining, scaleAtLast, scaleAtHeld, scaleAtCanaryStep}
	}
	return []scaleMoment{scaleAtIdle, scaleAtSettling, scaleAtRotating, scaleAtLast, scaleAtHeld}
}

// repairLossCounts are the Instance counts one node held, the spread of
// the default scheduler at the small end and the packing of the gang
// scheduler at the large one.
var repairLossCounts = []int32{1, 3, 9, 16}

// lossesAt lists the losses tried at a moment, each resolved against the
// configuration so two losses that take the same Instances are tried
// once. Bounded mode takes the node deleted under old Instances of the
// prefill side at every count, of both sides at three and nine, and nine
// decode Instances, and the node stopped reporting under nine prefill
// Instances; at the push only nine of the prefill side and nine of both.
// Full mode takes every scope, count, kind and pick.
func (m *repairRatioModel) lossesAt(moment scaleMoment) []repairLoss {
	var candidates []repairLoss
	switch {
	case m.full:
		kinds := []lossKind{lossGone, lossUnreported}
		if m.w.cfg.shape.pods > 1 {
			kinds = append(kinds, lossMember)
		}
		for _, scope := range []lossScope{lossPrefill, lossDecode, lossBothSides} {
			for _, count := range repairLossCounts {
				for _, kind := range kinds {
					for _, pick := range []lossPick{pickOldFirst, pickNewFirst} {
						candidates = append(candidates, repairLoss{scope: scope, count: count, kind: kind, pick: pick})
					}
				}
			}
		}
	case moment == scaleAtIdle:
		candidates = []repairLoss{{scope: lossPrefill, count: 9}, {scope: lossBothSides, count: 9}}
	default:
		for _, count := range repairLossCounts {
			candidates = append(candidates, repairLoss{scope: lossPrefill, count: count})
			if count == 3 || count == 9 {
				candidates = append(candidates, repairLoss{scope: lossBothSides, count: count})
			}
		}
		candidates = append(candidates, repairLoss{scope: lossDecode, count: 9}, repairLoss{scope: lossPrefill, count: 9, kind: lossUnreported})
	}
	var out []repairLoss
	seen := map[string]bool{}
	for _, l := range candidates {
		parts := m.split(l)
		if len(parts) == 0 {
			continue
		}
		key := fmt.Sprintf("%v/%s/%s", parts, l.kind, l.pick)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, l)
	}
	return out
}

// split resolves a loss into the Instances it takes from each side: the
// whole count on one side, capped at the side's count; across both sides
// in the fleet's proportion, at least one on each. A loss of one Instance
// across both sides, or on a side the service does not have, is nothing.
func (m *repairRatioModel) split(l repairLoss) []lossPart {
	w := m.w
	prefill, decode := w.prefill(), w.decode()
	instances := l.count
	if l.kind != lossMember && w.cfg.shape.pods > 1 {
		// A node holds whole gangs only when the gangs are small enough to
		// sit on it together; the count names pods and the loss takes the
		// gangs they belong to.
		instances = (l.count + w.cfg.shape.pods - 1) / w.cfg.shape.pods
	}
	switch l.scope {
	case lossPrefill:
		return []lossPart{{side: 0, count: min(instances, prefill.h.replicas)}}
	case lossDecode:
		if decode == nil {
			return nil
		}
		return []lossPart{{side: 1, count: min(instances, decode.h.replicas)}}
	}
	if decode == nil || instances < 2 {
		return nil
	}
	total := float64(prefill.h.replicas + decode.h.replicas)
	onPrefill := int32(math.Round(float64(instances) * float64(prefill.h.replicas) / total))
	onPrefill = max(1, min(onPrefill, instances-1))
	return []lossPart{
		{side: 0, count: min(onPrefill, prefill.h.replicas)},
		{side: 1, count: min(instances-onPrefill, decode.h.replicas)},
	}
}

// pickIndices chooses the Instances of a side the node held, up to count:
// old first takes the Instances the roll has not reached, lowest first,
// then the ones with an attempt open, then the promoted ones; promoted
// first takes them in the other order, highest first.
func (m *repairRatioModel) pickIndices(s *scaleSide, count int32, pick lossPick) []int32 {
	w := m.w
	byIndex := w.livePodsByIndex(s)
	started := w.startedIndices(s)
	var old, starting, promoted []int32
	for _, r := range w.rows(s) {
		if len(byIndex[r.Index]) == 0 || replacementOrLeaving(r) {
			continue
		}
		switch {
		case r.RunningRevision == s.h.target.Name:
			promoted = append(promoted, r.Index)
		case started[r.Index]:
			starting = append(starting, r.Index)
		default:
			old = append(old, r.Index)
		}
	}
	groups := [][]int32{old, starting, promoted}
	if pick == pickNewFirst {
		groups = [][]int32{promoted, starting, old}
		for _, g := range groups {
			sort.Slice(g, func(i, j int) bool { return g[i] > g[j] })
		}
	}
	var order []int32
	for _, g := range groups {
		order = append(order, g...)
	}
	if int(count) < len(order) {
		order = order[:count]
	}
	return order
}

// tickUntilRebuildStarts ticks, at most limit times, until a lost Instance
// of side stands toward the target, and reports how many ticks that took
// and the lost Instances toward the target at that tick.
func (m *repairRatioModel) tickUntilRebuildStarts(s *scaleSide, limit int) (int, []int32) {
	for i := 1; i <= limit; i++ {
		m.w.tick()
		m.check()
		if toward := m.towardTheTarget(s); len(toward) > 0 {
			return i, toward
		}
	}
	return limit, nil
}

// apply lands one loss on the world: the Instances the node held on each
// side in scope are marked, what each side had moved toward the target is
// kept, and the pods leave the way the kind names.
func (m *repairRatioModel) apply(l repairLoss) {
	w := m.w
	w.path = append(w.path, l.String())
	m.reset()
	for _, s := range w.sides {
		if s.h.heldOn(s.h.target.Name) {
			m.heldBefore = true
		}
	}
	for _, part := range m.split(l) {
		s := w.sides[part.side]
		indices := m.pickIndices(s, part.count, l.pick)
		m.lost[s] = map[int32]bool{}
		for _, idx := range indices {
			w.markFaulted(s, idx)
			m.lost[s][idx] = true
		}
		m.startedAtLoss[s] = w.startedIndices(s)
		switch l.kind {
		case lossGone:
			m.removePods(s, indices, "")
		case lossMember:
			m.removePods(s, indices, "worker")
		case lossUnreported:
			m.stopNodeUnder(s, indices)
		}
	}
	m.lossTick, m.lossAt = w.ticks, w.clk.Now()
}

// removePods takes every live pod of the Instances out of the API at once,
// every pod of each when runner is empty and the named runner's pod
// otherwise, as the garbage collection behind a deleted Node does.
func (m *repairRatioModel) removePods(s *scaleSide, indices []int32, runner string) {
	w := m.w
	byIndex := w.livePodsByIndex(s)
	for _, idx := range indices {
		for _, pod := range byIndex[idx] {
			if runner != "" && pod.Labels[query.LabelRunner] != runner {
				continue
			}
			if err := s.h.c.Delete(s.h.ctx, pod, client.GracePeriodSeconds(0)); err != nil {
				w.t.Fatalf("remove pod %s: %v", pod.Name, err)
			}
		}
	}
}

// stopNodeUnder binds every live pod of the Instances to one new node, as
// a scheduler that packs them would have, and stops that node's kubelet. A
// pod the kubelet model has not reported yet is left as the API server
// and the scheduler leave a pod bound to a node whose kubelet never picks
// it up: created now, Pending, scheduled, with no container status.
func (m *repairRatioModel) stopNodeUnder(s *scaleSide, indices []int32) {
	w := m.w
	m.lossNodes++
	node := fmt.Sprintf("%s-lost-node-%d", s.comp, m.lossNodes)
	s.h.addNodes(node)
	byIndex := w.livePodsByIndex(s)
	for _, idx := range indices {
		for _, pod := range byIndex[idx] {
			pod.Spec.NodeName = node
			unreported := pod.CreationTimestamp.IsZero()
			if unreported {
				pod.CreationTimestamp = metav1.NewTime(w.clk.Now())
			}
			if err := s.h.c.Update(s.h.ctx, pod); err != nil {
				w.t.Fatalf("bind pod %s to %s: %v", pod.Name, node, err)
			}
			if !unreported {
				continue
			}
			pod.Status.Phase = corev1.PodPending
			setPodScheduled(pod, corev1.ConditionTrue, "", "", w.clk.Now())
			if err := s.h.c.Status().Update(s.h.ctx, pod); err != nil {
				w.t.Fatalf("write pod %s pending on %s: %v", pod.Name, node, err)
			}
		}
	}
	s.h.failNode(node)
}

// playOut ticks until the world has settled or the bound is reached, with
// this model's readings after every tick; it reports whether the world
// settled.
func (m *repairRatioModel) playOut() bool {
	w := m.w
	settling := w.startSettling()
	for i := 0; i < m.bound; i++ {
		key := w.tick()
		m.check()
		if settling.observe(w, key) {
			return true
		}
	}
	return false
}

// check runs this model's per-tick readings: the repair's progress, the
// version-mix lead and the surge ceiling.
func (m *repairRatioModel) check() {
	if len(m.lost) == 0 {
		return
	}
	m.readRepair()
	m.readLead()
	m.readCeiling()
}

// readRepair reads how far the repair of the lost Instances has come:
// every one with a replacement pod, every one serving again; and counts
// the ticks a lost Instance stands with no replacement pod while its side
// names a hold, by the hold's gate.
func (m *repairRatioModel) readRepair() {
	w := m.w
	built, serving := true, true
	for s, lost := range m.lost {
		byIndex := w.livePodsByIndex(s)
		rows := map[int32]workloadtypes.InstanceStatus{}
		for _, r := range w.rows(s) {
			rows[r.Index] = r
		}
		unbuilt := false
		for idx := range lost {
			if len(byIndex[idx]) == 0 {
				built, unbuilt = false, true
			}
			r, ok := rows[idx]
			if !ok || r.Phase != workloadtypes.InstancePhaseReady || r.Operation != nil || !w.servingSet(s, byIndex[idx]) {
				serving = false
			}
		}
		if unbuilt && s.h.hold != nil {
			m.waits[string(s.h.hold.Gate)]++
		}
	}
	if built && m.builtTick < 0 {
		m.builtTick = w.ticks - m.lossTick
	}
	if serving && m.restoredTick < 0 {
		m.restoredTick = w.ticks - m.lossTick
		loss := w.path[len(w.path)-1]
		for i := len(w.path) - 1; i >= 0; i-- {
			if strings.HasPrefix(w.path[i], "node ") || strings.HasPrefix(w.path[i], "a worker") {
				loss = w.path[i]
				break
			}
		}
		if prior, ok := m.restores[loss]; !ok || m.restoredTick > prior.restoredTicks {
			m.restores[loss] = repairRestore{loss: loss, builtTicks: m.builtTick, restoredTicks: m.restoredTick, restoredAfter: w.clk.Now().Sub(m.lossAt)}
		}
	}
}

// repairTolerance is the tolerance a ratio rule holds the pair to now, and
// whether one governs: the group's rule at the wide tolerance, else
// keepRatio at the chart's default, else none.
func (m *repairRatioModel) repairTolerance() (float64, bool) {
	w := m.w
	if w.canaryStep >= 0 || w.cfg.settings.sequential || w.decode() == nil {
		return 0, false
	}
	switch {
	case w.recordedRatioRule():
		return float64(scaleRollTolerance) / 100, true
	case w.cfg.settings.keepRatio:
		return float64(scaleRollKeepRatioTolerance) / 100, true
	}
	return 0, false
}

// shares reads one side's moved and movable Instances the way the scale
// model's version-mix reading does, the Instances in exclude read as not
// moved.
func (m *repairRatioModel) shares(s *scaleSide, exclude map[int32]bool) (moved, movable int32) {
	w := m.w
	held := w.heldIndices(s)
	started := w.startedIndices(s)
	for _, r := range w.rows(s) {
		if held[r.Index] || replacementOrLeaving(r) {
			continue
		}
		movable++
		if started[r.Index] && !exclude[r.Index] {
			moved++
		}
	}
	if w.endedBatchUnseen(s) {
		moved = min(moved+w.stepOf(movable), movable)
	}
	return moved, movable
}

// rebuiltBeyondThePlan is, per side, the lost Instances that stand toward
// the target now and did not when the loss landed: the rebuilds on the new
// version the gates were not asked about.
func (m *repairRatioModel) rebuiltBeyondThePlan(s *scaleSide) map[int32]bool {
	out := map[int32]bool{}
	started := m.w.startedIndices(s)
	for idx := range m.lost[s] {
		if started[idx] && !m.startedAtLoss[s][idx] {
			out[idx] = true
		}
	}
	return out
}

// lead reads the two sides' new-version shares, the gap between them and
// the gap the rule allows (the tolerance plus one step of the leading
// side), with the rebuilds beyond the plan left in or read out.
func (m *repairRatioModel) lead(tolerance float64, withoutRebuilds bool) (repairLead, bool) {
	w := m.w
	prefill, decode := w.prefill(), w.decode()
	exclude := map[*scaleSide]map[int32]bool{}
	var rebuilt int32
	for _, s := range []*scaleSide{prefill, decode} {
		if withoutRebuilds {
			exclude[s] = m.rebuiltBeyondThePlan(s)
		}
		rebuilt += int32(len(m.rebuiltBeyondThePlan(s)))
	}
	out := repairLead{rebuilt: rebuilt}
	out.prefillMoved, out.prefillMovable = m.shares(prefill, exclude[prefill])
	out.decodeMoved, out.decodeMovable = m.shares(decode, exclude[decode])
	if out.prefillMovable <= 0 || out.decodeMovable <= 0 {
		return out, false
	}
	prefillShare := float64(out.prefillMoved) / float64(out.prefillMovable)
	decodeShare := float64(out.decodeMoved) / float64(out.decodeMovable)
	out.gap = prefillShare - decodeShare
	out.step = w.stepOf(out.prefillMovable)
	out.allowed = tolerance + float64(out.step)/float64(out.prefillMovable)
	if out.gap < 0 {
		out.gap = -out.gap
		out.step = w.stepOf(out.decodeMovable)
		out.allowed = tolerance + float64(out.step)/float64(out.decodeMovable)
	}
	return out, true
}

// readLead holds the two sides' new-version shares within the ratio rule's
// tolerance plus one step. While a lost Instance rebuilt on the new version
// stands in the shares, the excess is the rebuilds' own, whether they
// opened it themselves or the gates admitted the other side's starts on
// the strength of the share they added; the message carries the lead
// without them. A gap in a play with no such rebuild is the gates' own and
// carries the scale model's class for it.
func (m *repairRatioModel) readLead() {
	w := m.w
	tolerance, governed := m.repairTolerance()
	if !governed {
		return
	}
	actual, ok := m.lead(tolerance, false)
	if !ok || actual.gap <= actual.allowed+1e-9 {
		return
	}
	without, _ := m.lead(tolerance, true)
	class := "version-mix"
	switch {
	case actual.rebuilt > 0:
		class = knownRebuildsOutrunTheRatio
	case !w.cfg.settings.keepRatio:
		class = "version-mix-by-batch-number"
	}
	if class == knownRebuildsOutrunTheRatio && actual.gap > m.worstLead.gap {
		m.worstLead, m.worstLeadPath = actual, strings.Join(w.path, " -> ")
	}
	w.note(class, "%d lost Instances were rebuilt on the new version with no gate asked: the new version reads %d of %d prefill (%.0f points) beside %d of %d decode (%.0f points), a lead of %.0f points against %.0f allowed (the tolerance of %.0f plus one step of %d); without the rebuilds the lead is %.0f points",
		actual.rebuilt, actual.prefillMoved, actual.prefillMovable, 100*float64(actual.prefillMoved)/float64(actual.prefillMovable),
		actual.decodeMoved, actual.decodeMovable, 100*float64(actual.decodeMoved)/float64(actual.decodeMovable),
		100*actual.gap, 100*actual.allowed, 100*tolerance, actual.step, 100*without.gap)
}

// readCeiling holds every side to the pods its surge budget allows above
// its count, as an operator reads a surge budget: a side that drains
// first builds nothing above its count, a side that surges at most its
// surge budget's Instances. The Instances a scale-down is removing are
// not pods above the count: the engine finishes their drain.
func (m *repairRatioModel) readCeiling() {
	w := m.w
	for _, s := range w.sides {
		if s.comp == workloadtypes.ComponentRouter {
			continue
		}
		per := w.podsPerInstance(s)
		if per <= 0 {
			continue
		}
		leaving := map[int32]bool{}
		for _, r := range w.rows(s) {
			if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationDelete {
				leaving[r.Index] = true
			}
		}
		var live int32
		for idx, pods := range w.livePodsByIndex(s) {
			if !leaving[idx] {
				live += int32(len(pods))
			}
		}
		built := (live + per - 1) / per
		allowed := int32(0)
		if s.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
			budget := escalation.PerComponentMaxSurgeBudget(s.h.lifecycle.UpdateStrategy.RollingUpdate, s.h.replicas)
			if budget == escalation.BudgetNoLimit {
				continue
			}
			allowed = budget
		}
		if built > s.h.replicas+allowed {
			w.note("pods-above-the-surge-ceiling", "%s has pods for %d Instances against %d wanted and a surge budget of %d: %s", s.comp, built, s.h.replicas, allowed, w.picture())
		}
	}
}

// classify reads the end of a play: the roll complete; held where it stood
// held before the loss; or the scale model's reading of an unfinished roll
// at rest, with a roll the loss itself left held named as such.
func (m *repairRatioModel) classify(settled bool) {
	w := m.w
	if !settled {
		w.note("did-not-settle", "the world kept changing to the end of the bound: %s", w.picture())
		return
	}
	if w.complete() {
		w.note("complete", "the roll ended after the loss")
		return
	}
	for _, s := range w.sides {
		if s.h.heldOn(s.h.target.Name) && !m.heldBefore {
			w.note("held-by-the-loss", "the loss left a retry schedule spent: %s", w.picture())
			return
		}
	}
	if m.attemptWaitsOnANeverStartedPodOfALostNode() {
		w.note(knownNeverStartedPodOfALostNode, "an attempt waits on a pod that never started on a node that stopped reporting, and names no hold: %s", w.picture())
		return
	}
	w.classifyEnd(true)
}

// neverStartedOnALostNode reports a pod bound to a node whose kubelet
// stopped before it ever ran a container: Pending, not on its way out,
// with no container status, on a node the harness stopped.
func neverStartedOnALostNode(s *scaleSide, pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || len(pod.Status.ContainerStatuses) > 0 {
		return false
	}
	if pod.Status.Phase != corev1.PodPending && pod.Status.Phase != "" {
		return false
	}
	return s.h.deadNodes[pod.Spec.NodeName]
}

// attemptWaitsOnANeverStartedPodOfALostNode reads the picture of a roll
// at rest whose every unfinished side either has an open attempt (an
// update or a create) waiting on a pod that never started on a node that
// stopped reporting, or holds on a peer gate or a budget for such a side.
func (m *repairRatioModel) attemptWaitsOnANeverStartedPodOfALostNode() bool {
	w := m.w
	waiting, peers := 0, 0
	for _, s := range w.sides {
		if w.sideComplete(s) {
			continue
		}
		found := false
		for _, pod := range w.pods(s) {
			idx, ok := query.InstanceIdxFromLabels(pod)
			if !ok || !neverStartedOnALostNode(s, pod) {
				continue
			}
			for _, r := range w.rows(s) {
				if r.Index == idx && r.Operation != nil && (r.Operation.Type == workloadtypes.InstanceOperationUpdate || r.Operation.Type == workloadtypes.InstanceOperationCreate) {
					found = true
				}
			}
		}
		switch {
		case found:
			waiting++
		case s.h.hold != nil && (s.h.hold.Gate == workloadtypes.RolloutHoldGateRatio || s.h.hold.Gate == workloadtypes.RolloutHoldGatePairing || s.h.hold.Gate == workloadtypes.RolloutHoldGateSequential || s.h.hold.Gate == workloadtypes.RolloutHoldGateBudget):
			peers++
		default:
			return false
		}
	}
	return waiting > 0
}

// explore runs one configuration: the plain roll with its moments kept,
// every loss at every moment played to its end, then the random runs.
func (m *repairRatioModel) explore(seed int64) {
	w := m.w
	w.reachIdle()
	snaps := w.captureMoments(m.bound)
	for _, moment := range m.moments() {
		snap := snaps[moment]
		if snap == nil {
			continue
		}
		for _, l := range m.lossesAt(moment) {
			if w.ticks >= m.budget {
				m.skipped++
				continue
			}
			w.restore(snap)
			w.path = append(w.path, "at "+moment.String())
			if moment == scaleAtIdle {
				w.push(fixedImage)
			}
			m.apply(l)
			m.probes++
			m.classify(m.playOut())
		}
	}
	m.randomRuns(snaps[scaleAtIdle], seed)
}

// randomRuns plays the roll from idle with one or two losses and, in half
// of the runs, one of the operator's rotating-moment moves, each at a
// random tick, under a fixed seed.
func (m *repairRatioModel) randomRuns(idle *scaleSnapshot, seed int64) {
	w := m.w
	if w.decode() == nil {
		return
	}
	runs := 2
	if m.full {
		runs = 10
	}
	rng := rand.New(rand.NewSource(seed))
	for run := 0; run < runs; run++ {
		if w.ticks >= m.budget {
			m.skipped++
			continue
		}
		w.restore(idle)
		m.reset()
		w.path = append(w.path, fmt.Sprintf("random run %d", run+1))
		w.push(fixedImage)
		losses := m.lossesAt(scaleAtRotating)
		lossesLeft := 1 + rng.Intn(2)
		operatorMoves := rng.Intn(2)
		m.probes++
		m.runs++
		settling := w.startSettling()
		settled := false
		for i := 0; i < m.bound && !settled; i++ {
			switch {
			case lossesLeft > 0 && rng.Intn(5) == 0:
				m.apply(losses[rng.Intn(len(losses))])
				lossesLeft--
				settling = w.startSettling()
			case operatorMoves > 0 && rng.Intn(6) == 0:
				moves := w.boundedMoves(scaleAtRotating)
				w.apply(moves[rng.Intn(len(moves))])
				operatorMoves--
				settling = w.startSettling()
			}
			key := w.tick()
			m.check()
			settled = settling.observe(w, key)
		}
		m.classify(settled)
	}
}

// repairRatioReferenceConfigs are the configurations every run explores:
// the reference shapes under their reference settings, the ratio
// rule on most of them, both strategies and their mix, gangs of two and
// four, a partition, a canary, a scaling policy and a router.
func repairRatioReferenceConfigs() []scaleConfig {
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	return []scaleConfig{
		{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}},
		{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}},
		{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: recreate, decode: surge, maintainRatio: true}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true}},
		{shape: scaleShape{12, 4, 2}, settings: scaleSettings{prefill: recreate, decode: recreate, keepRatio: true, firstSide: v1beta1.DecoderComponent}},
		{shape: scaleShape{15, 15, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true, firstSide: v1beta1.EngineComponent}},
		{shape: scaleShape{24, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true, partition: 2}},
		{shape: scaleShape{30, 10, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true, policy: true}},
		{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: recreate, decode: surge, canary: true}},
		{shape: scaleShape{12, 4, 4}, settings: scaleSettings{prefill: surge, decode: surge, policy: true}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true, router: true}},
	}
}

// repairRatioConfigs lists the configurations of a run: the reference
// ones, and in full mode every configuration of the scale model beside
// them.
func repairRatioConfigs(full bool) []scaleConfig {
	configs := repairRatioReferenceConfigs()
	if !full {
		return configs
	}
	seen := map[string]bool{}
	for _, cfg := range configs {
		seen[cfg.String()] = true
	}
	for _, cfg := range scaleConfigs(true) {
		if !seen[cfg.String()] {
			seen[cfg.String()] = true
			configs = append(configs, cfg)
		}
	}
	return configs
}

// The known pictures of this model: reported, not failed, until a fix
// removes the entry and the lock test that holds it.
const (
	knownRebuildsOutrunTheRatio     = "known-rebuilds-on-the-new-version-outrun-the-ratio"
	knownNeverStartedPodOfALostNode = "known-attempt-waits-on-a-never-started-pod-of-a-lost-node"
)

// knownRepairRatioClasses are the finding classes this model shows on main
// today, each with the picture behind it.
var knownRepairRatioClasses = map[string]string{
	knownNeverStartedPodOfALostNode: "a node that stops reporting while it holds an attempt's new pod before that pod ever ran leaves the attempt waiting on it for good: the force-delete policy removes the pods the node had run after its threshold, but a pod that never ran a container is removed only inside a repair (ops/restart.go:freeRebuiltSetNames), not for a surge, a recreate or the Create pass's own build, and the attempt names no hold once the old pod is gone (held by TestRepairsUnderTheRatioGate_ALostNodeUnderTheLastSurgeLeavesItsReplacementWaitingWithNoHold)",
	knownRebuildsOutrunTheRatio:     "Instances that lose every pod under an open roll are handed to the roll, written Pending and rebuilt by the Create pass on the new version in one pass with no coordination gate and no budget asked, under both strategies; an Instance whose pods are listed but serve nothing is recreated by the update pass exempt from the gate; so a loss of many Instances on one side puts that side's new-version share past the ratio rule's tolerance plus one step at once, where the gates admit one step at a time (held by TestRepairsUnderTheRatioGate_NineLostPrefillInstancesAreRebuiltOnTheNewVersionAtOnce and TestRepairsUnderTheRatioGate_UnderRecreatePodTheLostInstancesAreRebuiltOnTheNewVersionAtOnceToo)",
}

// repairRatioExpected reports an end the engine owes, counted and not
// failed: the roll complete, or held where it stood held before the loss.
func repairRatioExpected(class string) bool {
	return class == "complete" || class == "held"
}

// repairRatioResult is what one configuration's exploration found.
type repairRatioResult struct {
	cfg                                                     scaleConfig
	states, ticks, passes, gateCalls, probes, skipped, runs int
	counts                                                  map[string]int
	findings                                                map[string]scaleFinding
	restores                                                map[string]repairRestore
	waits                                                   map[string]int
	worstLead                                               repairLead
	worstLeadPath                                           string
	took                                                    time.Duration
}

// repairRatioExplore runs the model over every configuration, one subtest
// per configuration in parallel, and reports every class, failing on the
// classes that are neither owed ends nor known.
func repairRatioExplore(t *testing.T) {
	t.Helper()
	full := os.Getenv(repairRatioFullEnv) == "full"
	only := os.Getenv(repairRatioOnlyEnv)
	start := time.Now()
	var configs []scaleConfig
	for _, cfg := range repairRatioConfigs(full) {
		if only == "" || strings.Contains(cfg.String(), only) {
			configs = append(configs, cfg)
		}
	}
	results := make([]repairRatioResult, len(configs))
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				began := time.Now()
				w := newScaleWorld(t, cfg)
				m := newRepairRatioModel(w, full)
				m.explore(int64(i + 1))
				results[i] = repairRatioResult{cfg: cfg, states: len(w.seen), ticks: w.ticks, passes: w.passes, gateCalls: w.gateCalls,
					probes: m.probes, skipped: m.skipped, runs: m.runs, counts: w.counts, findings: w.findings,
					restores: m.restores, waits: m.waits, worstLead: m.worstLead, worstLeadPath: m.worstLeadPath, took: time.Since(began)}
			})
		}
	})
	repairRatioReport(t, configs, results, time.Since(start))
}

// longestRestore is the slowest repair among a configuration's losses.
func longestRestore(restores map[string]repairRestore) (repairRestore, bool) {
	var out repairRestore
	found := false
	for _, r := range restores {
		if !found || r.restoredTicks > out.restoredTicks {
			out, found = r, true
		}
	}
	return out, found
}

// repairRatioReport prints every configuration's counts, its slowest repair
// and widest lead, every class with its examples, and fails the classes the
// engine does not owe.
func repairRatioReport(t *testing.T, configs []scaleConfig, results []repairRatioResult, took time.Duration) {
	t.Helper()
	totals, configurations := map[string]int{}, map[string]int{}
	examples := map[string][]scaleExample{}
	waits := map[string]int{}
	var states, ticks, passes, gateCalls, probes, skipped, runs int
	for n, res := range results {
		states += res.states
		ticks += res.ticks
		passes += res.passes
		gateCalls += res.gateCalls
		probes += res.probes
		skipped += res.skipped
		runs += res.runs
		var parts []string
		for class, c := range res.counts {
			totals[class] += c
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		for gate, c := range res.waits {
			waits[gate] += c
		}
		line := fmt.Sprintf("config %d/%d [%s]: %d states, %d ticks, %d losses (%d skipped past the tick budget), %d random runs, %s: %s",
			n+1, len(configs), res.cfg, res.states, res.ticks, res.probes, res.skipped, res.runs, res.took.Round(time.Second), strings.Join(parts, " "))
		if r, ok := longestRestore(res.restores); ok {
			line += fmt.Sprintf("\n    slowest repair: every lost Instance had a pod %d tick(s) after the loss and served again %d tick(s) (%s) after it, under %q", r.builtTicks, r.restoredTicks, r.restoredAfter.Round(time.Second), r.loss)
		}
		if res.worstLead.rebuilt > 0 {
			l := res.worstLead
			line += fmt.Sprintf("\n    widest lead the rebuilds opened: %.0f points against %.0f allowed (%d of %d prefill beside %d of %d decode, %d rebuilt) on %s", 100*l.gap, 100*l.allowed, l.prefillMoved, l.prefillMovable, l.decodeMoved, l.decodeMovable, l.rebuilt, res.worstLeadPath)
		}
		t.Log(line)
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 6 {
				examples[class] = append(examples[class], scaleExample{cfg: res.cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	var waitParts []string
	for gate, c := range waits {
		waitParts = append(waitParts, fmt.Sprintf("%s=%d", gate, c))
	}
	sort.Strings(waitParts)
	t.Logf("%d configurations, %d states, %d ticks, %d passes, %d gate questions, %d losses (%d skipped past the tick budget), %d random runs, %s; ticks a lost Instance stood unbuilt while its side held, by gate: %s",
		len(configs), states, ticks, passes, gateCalls, probes, skipped, runs, took.Round(time.Second), strings.Join(waitParts, " "))
	for _, class := range classes {
		t.Logf("%s: %d states in %d configurations", class, totals[class], configurations[class])
		if repairRatioExpected(class) {
			continue
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		if known := knownRepairRatioClasses[class]; known != "" {
			t.Logf("  known: %s", known)
			continue
		}
		if known := knownScaleFindingClasses[class]; known != "" {
			t.Logf("  known: %s", known)
			continue
		}
		t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
	}
}

// TestRepairsUnderTheRatioGate_EveryLossLeavesTheRollAWayOut explores every
// configuration: a node lost under 1, 3, 9 or 16 Instances of one side or
// of both, at every moment of a roll, leaves a roll that finishes, or
// holds where it stood held before, within its budgets, with every lost
// Instance rebuilt and serving, and with the version mix inside the ratio
// rule except where the known class names the rebuilds.
func TestRepairsUnderTheRatioGate_EveryLossLeavesTheRollAWayOut(t *testing.T) {
	repairRatioExplore(t)
}

// repairWorldAtSettling builds a configuration's world, pushes version 2
// and plays the roll to its first batch, before any Instance serves the
// new version.
func repairWorldAtSettling(t *testing.T, cfg scaleConfig) (*scaleWorld, *repairRatioModel) {
	t.Helper()
	w := newScaleWorld(t, cfg)
	m := newRepairRatioModel(w, false)
	w.reachIdle()
	w.push(fixedImage)
	for i := 0; i < 40 && !w.momentReached(scaleAtSettling); i++ {
		w.tick()
	}
	if !w.momentReached(scaleAtSettling) {
		t.Fatalf("the first batch did not open: %s", w.picture())
	}
	return w, m
}

// towardTheTarget lists, of a side's lost Instances, the ones whose row
// stands toward the target: promoted on it, or with an attempt toward it.
func (m *repairRatioModel) towardTheTarget(s *scaleSide) []int32 {
	started := m.w.startedIndices(s)
	var out []int32
	for idx := range m.lost[s] {
		if started[idx] {
			out = append(out, idx)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestRepairsUnderTheRatioGate_NineLostPrefillInstancesAreRebuiltOnTheNewVersionAtOnce
// holds the picture the model found on a 16:8 service under the gang
// scheduler. 16 prefill and 8 decode
// under SurgeThenDrain at ten percent with maintainRatio at a tolerance
// of 20; the first batch is open, nothing serves version 2 yet. A node
// deleted from the API with its kubelet stopped takes the pods of nine
// prefill Instances on version 1. The next pass rebuilds all nine on
// version 2 with no gate asked, so the new version reads nine or more of
// 16 prefill beside at most one of 8 decode, a lead past the tolerance
// plus one step; the prefill side then holds on the ratio gate while the
// decoder catches up, and the roll finishes. When the engine rebuilds the
// lost Instances on the version they ran, or paces their rebuild on the
// new version through the gate, this test fails and the known class goes
// with it.
func TestRepairsUnderTheRatioGate_NineLostPrefillInstancesAreRebuiltOnTheNewVersionAtOnce(t *testing.T) {
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	cfg := scaleConfig{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}
	w, m := repairWorldAtSettling(t, cfg)
	m.apply(repairLoss{scope: lossPrefill, count: 9})
	prefill := w.prefill()
	if len(m.lost[prefill]) != 9 {
		t.Fatalf("the loss took %d prefill Instances, not nine: %s", len(m.lost[prefill]), w.picture())
	}
	ticks, toward := m.tickUntilRebuildStarts(prefill, 4)
	lead, ok := m.lead(float64(scaleRollTolerance)/100, false)
	t.Logf("%d pass(es) after the loss: %d of the nine lost Instances stand toward version 2 (%v); the new version reads %d of %d prefill beside %d of %d decode, a lead of %.0f points against %.0f allowed: %s",
		ticks, len(toward), toward, lead.prefillMoved, lead.prefillMovable, lead.decodeMoved, lead.decodeMovable, 100*lead.gap, 100*lead.allowed, w.picture())
	if len(toward) != 9 {
		t.Fatalf("the picture moved: %d of the nine lost Instances were rebuilt on version 2 in one pass, not nine; if the engine now rebuilds them on version 1 or paces them through the gate, remove the known class and this lock: %s", len(toward), w.picture())
	}
	if !ok || lead.gap <= lead.allowed+1e-9 {
		t.Fatalf("the picture moved: the lead after the rebuilds is %.0f points against %.0f allowed; remove the known class and this lock: %s", 100*lead.gap, 100*lead.allowed, w.picture())
	}
	ratioHolds := 0
	settling := w.startSettling()
	settled := false
	for i := 0; i < m.bound && !settled; i++ {
		key := w.tick()
		m.check()
		if hold := prefill.h.hold; hold != nil && hold.Gate == workloadtypes.RolloutHoldGateRatio {
			ratioHolds++
		}
		settled = settling.observe(w, key)
	}
	if !settled || !w.complete() {
		t.Fatalf("the roll must finish after the loss: %s", w.picture())
	}
	if ratioHolds == 0 {
		t.Fatalf("the picture moved: the prefill side never held on the ratio gate while the decoder caught up; remove the known class and this lock: %s", w.picture())
	}
	if _, class := w.findings[knownRebuildsOutrunTheRatio]; !class {
		t.Fatalf("the picture moved: the model's reading did not name the rebuilds; remove the known class and this lock: %v", w.counts)
	}
	t.Logf("the prefill side held on the ratio gate for %d tick(s) while the decoder caught up, and the roll finished: %s", ratioHolds, w.picture())
}

// TestRepairsUnderTheRatioGate_UnderRecreatePodTheLostInstancesAreRebuiltOnTheNewVersionAtOnceToo
// holds the drain-first face of the same picture. 16 prefill and 8 decode
// under RecreatePod at ten percent with maintainRatio at a tolerance of
// 20; the first batch is open. A node deleted from the API takes the pods
// of nine prefill Instances on version 1. The pass after the loss writes
// the nine rows Pending and the next builds all nine on version 2 through
// the Create pass, with no coordination gate and no unavailability budget
// asked, so the new version's prefill share passes the tolerance plus one
// step at once. When the engine rebuilds the lost Instances on the
// version they ran, or paces their rebuild through the gate or the
// budget, this test fails and the known class goes with it.
func TestRepairsUnderTheRatioGate_UnderRecreatePodTheLostInstancesAreRebuiltOnTheNewVersionAtOnceToo(t *testing.T) {
	recreate := workloadtypes.UpdateStrategyRecreatePod
	cfg := scaleConfig{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}}
	w, m := repairWorldAtSettling(t, cfg)
	m.apply(repairLoss{scope: lossPrefill, count: 9})
	prefill := w.prefill()
	if len(m.lost[prefill]) != 9 {
		t.Fatalf("the loss took %d prefill Instances, not nine: %s", len(m.lost[prefill]), w.picture())
	}
	ticks, first := m.tickUntilRebuildStarts(prefill, 4)
	lead, ok := m.lead(float64(scaleRollTolerance)/100, false)
	t.Logf("%d pass(es) after the loss: %d of the nine lost Instances stand toward version 2 (%v); the new version reads %d of %d prefill beside %d of %d decode, a lead of %.0f points against %.0f allowed: %s",
		ticks, len(first), first, lead.prefillMoved, lead.prefillMovable, lead.decodeMoved, lead.decodeMovable, 100*lead.gap, 100*lead.allowed, w.picture())
	if len(first) != 9 {
		t.Fatalf("the picture moved: %d of the nine lost Instances were rebuilt on version 2 in the first pass that rebuilt one, not nine; if the engine now rebuilds them on version 1 or paces them through the gate or the budget, remove the known class and this lock: %s", len(first), w.picture())
	}
	if !ok || lead.gap <= lead.allowed+1e-9 {
		t.Fatalf("the picture moved: the lead after the rebuilds is %.0f points against %.0f allowed; remove the known class and this lock: %s", 100*lead.gap, 100*lead.allowed, w.picture())
	}
	settling := w.startSettling()
	settled := false
	for i := 0; i < m.bound && !settled; i++ {
		key := w.tick()
		m.check()
		settled = settling.observe(w, key)
	}
	if !settled || !w.complete() {
		t.Fatalf("the roll must finish after the loss: %s", w.picture())
	}
	if _, class := w.findings[knownRebuildsOutrunTheRatio]; !class {
		t.Fatalf("the picture moved: the lead never passed the tolerance through the rebuilds; remove the known class and this lock: %v", w.counts)
	}
	r, ok := longestRestore(m.restores)
	if !ok {
		t.Fatalf("the lost Instances never served again: %s", w.picture())
	}
	t.Logf("every lost Instance had a pod %d tick(s) after the loss and served again %d tick(s) (%s) after it; the widest lead the rebuilds opened was %.0f points against %.0f allowed; the roll finished: %s",
		r.builtTicks, r.restoredTicks, r.restoredAfter.Round(time.Second), 100*m.worstLead.gap, 100*m.worstLead.allowed, w.picture())
}

// TestRepairsUnderTheRatioGate_ALossInTheFleetsProportionKeepsTheVersionMix
// reads the other face of the rule: 16 prefill and 8 decode under
// SurgeThenDrain at ten percent with maintainRatio at a tolerance of 20;
// the first batch is open. A node deleted from the API takes the pods of
// six prefill and three decode Instances on version 1, the fleet's own
// proportion. Both sides' lost Instances are rebuilt on version 2 with no
// gate asked, and the two sides' new-version shares stay within the
// tolerance plus one step throughout: the count lost is not what moves
// the mix, the side it is lost from is.
func TestRepairsUnderTheRatioGate_ALossInTheFleetsProportionKeepsTheVersionMix(t *testing.T) {
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	cfg := scaleConfig{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}
	w, m := repairWorldAtSettling(t, cfg)
	m.apply(repairLoss{scope: lossBothSides, count: 9})
	if got, want := len(m.lost[w.prefill()]), 6; got != want {
		t.Fatalf("the loss took %d prefill Instances, not %d", got, want)
	}
	if got, want := len(m.lost[w.decode()]), 3; got != want {
		t.Fatalf("the loss took %d decode Instances, not %d", got, want)
	}
	if !m.playOut() || !w.complete() {
		t.Fatalf("the roll must finish after the loss: %s", w.picture())
	}
	if c := w.counts[knownRebuildsOutrunTheRatio]; c != 0 {
		t.Fatalf("a loss in the fleet's proportion moved the version mix past the tolerance on %d tick(s): %s", c, w.findings[knownRebuildsOutrunTheRatio].message)
	}
	r, _ := longestRestore(m.restores)
	t.Logf("nine Instances lost in the fleet's proportion were rebuilt on version 2 (every one with a pod %d tick(s) after the loss, serving %d tick(s) after it) and the version mix stayed within the tolerance: %s", r.builtTicks, r.restoredTicks, w.picture())
}

// repairWorldAtLast builds a configuration's world, pushes version 2 and
// plays the roll to its last Instances: every other Instance promoted, the
// last attempts open.
func repairWorldAtLast(t *testing.T, cfg scaleConfig) (*scaleWorld, *repairRatioModel) {
	t.Helper()
	w := newScaleWorld(t, cfg)
	m := newRepairRatioModel(w, false)
	w.reachIdle()
	w.push(fixedImage)
	for i := 0; i < 80 && !w.momentReached(scaleAtLast); i++ {
		w.tick()
	}
	if !w.momentReached(scaleAtLast) {
		t.Fatalf("the last Instance did not open: %s", w.picture())
	}
	return w, m
}

// TestRepairsUnderTheRatioGate_ALostNodeUnderTheLastSurgeLeavesItsReplacementWaitingWithNoHold
// holds the picture the model found. 16 prefill and 8 decode under
// SurgeThenDrain at ten percent with maintainRatio; the roll is at its
// last two prefill Instances, their replacements just created. The node
// that holds those two Instances' old pods and replacements, and seven
// promoted Instances' pods, stops reporting. After the force-delete
// threshold the engine removes every pod the node had run and rebuilds
// the seven promoted Instances elsewhere, but the two replacements that
// never ran a container stay, and the two attempts wait on them with no
// hold while the decoder finishes. When the engine removes a never-run
// pod of an update attempt on a node that stopped reporting and builds
// it again, this test fails and the known class goes with it.
func TestRepairsUnderTheRatioGate_ALostNodeUnderTheLastSurgeLeavesItsReplacementWaitingWithNoHold(t *testing.T) {
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	cfg := scaleConfig{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true}}
	w, m := repairWorldAtLast(t, cfg)
	m.apply(repairLoss{scope: lossPrefill, count: 9, kind: lossUnreported})
	settled := m.playOut()
	prefill := w.prefill()
	var waiting []string
	for _, pod := range w.pods(prefill) {
		if neverStartedOnALostNode(prefill, pod) {
			waiting = append(waiting, pod.Name)
		}
	}
	t.Logf("settled=%t after the loss: %s; pods that never started on the lost node: %v", settled, w.picture(), waiting)
	if !settled {
		t.Fatalf("the world did not settle: %s", w.picture())
	}
	if w.complete() {
		t.Fatalf("the picture moved: the roll finished after the loss; remove the known class and this lock: %s", w.picture())
	}
	if len(waiting) == 0 || prefill.h.hold != nil || !m.attemptWaitsOnANeverStartedPodOfALostNode() {
		t.Fatalf("the picture moved: the prefill side no longer waits on a never-run pod with no hold (hold %v, pods %v); remove the known class and this lock: %s", prefill.h.hold, waiting, w.picture())
	}
}
