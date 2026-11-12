package workload_test

import (
	"math/rand"
	"testing"

	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the production engine of a prefill/decode service while
// the operator sets, raises, lowers or clears a rollout partition at every
// moment of a roll. It runs in the shared world of
// scale_beside_a_roll_model_test.go: one workload.Reconcile loop per
// Component on the recovery harness, the coordination gates wired between
// the Components, the follower count of a scaling policy, and the
// partition a canary step projects over the operator's. The partition is
// the move under test: on the prefill side, the decode side or both, to
// zero, one, one step, half the count, all but one, the count and one
// above it, at every moment of the roll, once or twice in one roll; the
// world's losses, the scales, the canary's promotion, a roll back and a
// pause ride beside it. Seeded random runs mix up to three moves.
//
// The held set is read as the engine reads it (query.PartitionHeldIndices
// over workloadops.EffectivePartition): the lowest-indexed Instances still
// off the target, as many as the partition names. Beside the shared
// world's readings (the budgets, the version mix, a settled end that is
// complete, paused, held on a spent retry schedule or on the partition's
// own hold, no two gates waiting on each other) four are this model's:
//   - held-instance-moved: while the partition, the target, the count and
//     the rows stand, the held set never shrinks; the roll never takes a
//     held Instance.
//   - held-count-off-the-partition: while the partition stands above
//     zero, the held set has as many members as the partition names, or
//     every Instance off the target with no update attempt open when
//     those are fewer.
//   - rolled-back-under-the-partition: an Instance once Ready on the
//     target never runs an older revision again while the target stands.
//   - staged-shape-disagrees: at a complete end the status reader's rule
//     (status.ReachedDesiredShape) reads the member staged under a
//     partition that holds Instances and complete under one that holds
//     none.
//
// OME_PARTITION_HUNT=full widens the configurations, the moves and the
// bounds; OME_PARTITION_HUNT_ONLY=<text> keeps the configurations whose
// name carries the text.

const (
	partitionFullEnv = "OME_PARTITION_HUNT"
	partitionOnlyEnv = "OME_PARTITION_HUNT_ONLY"
	// partitionFullSample is how many sampled configurations the full run
	// adds to the reference ones; partitionFullTickBudget is the most ticks
	// one configuration may spend in full mode, three times the scaling
	// hunt's, so the moves beside the partition are reached on the large
	// shapes.
	partitionFullSample     = 36
	partitionFullTickBudget = 24000
)

// partitionHunt is the model's reading of one world: each side's
// partition standing and the Instances seen Ready on the target.
type partitionHunt struct {
	w *scaleWorld
	// clk is the world's clock the readings were taken on; a restore
	// replaces the clock, and the readings start over.
	clk *clocktesting.FakeClock
	// stand is, per side, the partition's standing as last read; held is
	// the held set under it and unset the Instances that had run no
	// revision, both as last read.
	stand map[*scaleSide]partitionStand
	held  map[*scaleSide]map[int32]bool
	unset map[*scaleSide]map[int32]bool
	// shape is, per side, the target, count and row set the onTarget
	// records were taken under; onTarget is the Instances seen Ready on
	// the target under it.
	shape    map[*scaleSide]partitionStand
	onTarget map[*scaleSide]map[int32]bool
}

// partitionStand is what the held set is read under: the effective
// partition, the target, the count and the row count, with the held count
// read under them. While the first four stand, the held count may not
// fall: the roll never takes a held Instance.
type partitionStand struct {
	partition int32
	target    string
	replicas  int32
	rows      int
	held      int
}

// sameStanding reports two readings taken under one partition, target,
// count and row set.
func (p partitionStand) sameStanding(q partitionStand) bool {
	return p.partition == q.partition && p.target == q.target && p.replicas == q.replicas && p.rows == q.rows
}

// installPartitionHunt puts the model's moves and checks on a world.
func installPartitionHunt(w *scaleWorld) {
	h := &partitionHunt{w: w}
	h.reset()
	w.moves = h.moves
	w.moreChecks = h.check
	w.endChecks = h.endCheck
}

// reset starts the readings over on the world's current clock.
func (h *partitionHunt) reset() {
	h.clk = h.w.clk
	h.stand = map[*scaleSide]partitionStand{}
	h.held = map[*scaleSide]map[int32]bool{}
	h.unset = map[*scaleSide]map[int32]bool{}
	h.shape = map[*scaleSide]partitionStand{}
	h.onTarget = map[*scaleSide]map[int32]bool{}
}

// moves lists the moves tried from a moment: the partition on each side
// to every value of the mode, the partition changed twice, then the moves
// that ride beside a partition change, each listed once.
func (h *partitionHunt) moves(m scaleMoment, full bool) []scaleMove {
	var out []scaleMove
	for side, s := range h.w.pair() {
		for _, v := range h.values(s, m, full) {
			out = append(out, scaleMove{kind: movePartition, side: side, delta: v})
		}
		for _, change := range h.twice(s, m, full) {
			out = append(out, scaleMove{kind: movePartitionTwice, side: side, delta: change[0], delta2: change[1]})
		}
	}
	seen := map[string]bool{}
	var kept []scaleMove
	for _, mv := range append(out, h.beside(m, full)...) {
		if text := mv.String(); !seen[text] {
			seen[text] = true
			kept = append(kept, mv)
		}
	}
	return kept
}

// values lists the partition values a move tries on a side at a moment:
// in full mode cleared, one, one step, half the count, all but one, the
// count, one above it and one off the standing value; in bounded mode the
// values an operator reaches for at that moment. The standing value is
// left out: setting it again moves nothing.
func (h *partitionHunt) values(s *scaleSide, m scaleMoment, full bool) []int32 {
	n, p := s.h.replicas, s.partition
	var candidates []int32
	switch {
	case full:
		candidates = []int32{0, 1, h.w.stepOf(n), n / 2, n - 1, n, n + 1, p - 1, p + 1}
	case s == h.w.prefill():
		candidates = partitionBoundedPrefillValues(m, n, p)
	default:
		candidates = partitionBoundedDecodeValues(m, n)
	}
	seen := map[int32]bool{p: true}
	var out []int32
	for _, v := range candidates {
		if v < 0 || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

// partitionBoundedPrefillValues are the bounded run's values on the
// prefill side per moment: the standing partition cleared, half the
// count, the whole count, one, and one above the standing value.
func partitionBoundedPrefillValues(m scaleMoment, n, p int32) []int32 {
	switch m {
	case scaleAtIdle:
		return []int32{n / 2, 1}
	case scaleAtSettling, scaleAtDraining:
		return []int32{0, n}
	case scaleAtRotating:
		return []int32{0, 1, n / 2, n, p + 1}
	case scaleAtLast:
		return []int32{n, 0}
	case scaleAtHeld:
		return []int32{n / 2}
	}
	return []int32{n / 2, 0}
}

// partitionBoundedDecodeValues are the bounded run's values on the decode
// side per moment: half the count while settling, half and cleared while
// rotating, one at the last Instance, and the whole count, one and
// cleared while the decoder is held.
func partitionBoundedDecodeValues(m scaleMoment, n int32) []int32 {
	switch m {
	case scaleAtSettling:
		return []int32{n / 2}
	case scaleAtRotating:
		return []int32{n / 2, 0}
	case scaleAtLast:
		return []int32{1}
	case scaleAtHeld:
		return []int32{n, 1, 0}
	}
	return nil
}

// twice lists the partition changed twice in one roll: set, then once the
// roll settles raised, lowered or cleared. The bounded run tries half the
// count then cleared and the whole count then one, on the prefill side
// while rotating.
func (h *partitionHunt) twice(s *scaleSide, m scaleMoment, full bool) [][2]int32 {
	n := s.h.replicas
	if !full {
		if s != h.w.prefill() || m != scaleAtRotating {
			return nil
		}
		return [][2]int32{{n / 2, 0}, {n, 1}}
	}
	var out [][2]int32
	for _, first := range []int32{1, n / 2, n} {
		for _, second := range []int32{0, 1, n} {
			if first != second {
				out = append(out, [2]int32{first, second})
			}
		}
	}
	return out
}

// beside lists the moves that ride beside a partition change at a moment:
// the scaling hunt's whole list in full mode; in bounded mode a pod or a
// gang member lost on each side's lowest old Instance (the lowest held
// one under a partition) and a pod lost on the prefill side's newest
// promoted one, a scale of the prefill side by a step and of both sides
// by one, a roll back, the canary promoted and the policy edited while
// rotating, a scale of the decoder at idle and while held, a pod lost on
// the prefill side while draining and at the last Instance, and the
// canary promoted at its step.
func (h *partitionHunt) beside(m scaleMoment, full bool) []scaleMove {
	w := h.w
	if full {
		return w.allMoves(m)
	}
	var out []scaleMove
	switch m {
	case scaleAtRotating:
		for side, s := range h.w.pair() {
			for i, idx := range w.lossIndices(s) {
				if i > 0 && side > 0 {
					break
				}
				out = append(out, scaleMove{kind: moveLosePod, side: side, index: idx})
				if i == 0 && w.cfg.shape.pods > 1 {
					out = append(out, scaleMove{kind: moveLoseMember, side: side, index: idx})
				}
			}
		}
		out = append(out, scaleMove{kind: moveScale, side: 0, delta: w.stepOf(w.prefill().h.replicas)}, scaleMove{kind: moveRollBack})
		if w.decode() != nil {
			out = append(out, scaleMove{kind: moveScaleBoth, delta: -1})
		}
		if w.cfg.settings.canary {
			out = append(out, scaleMove{kind: moveCanaryPromote})
		}
		if w.cfg.settings.policy {
			out = append(out, scaleMove{kind: movePolicyEdit})
		}
	case scaleAtIdle:
		if w.decode() != nil {
			out = append(out, scaleMove{kind: moveScale, side: 1, delta: -1})
		}
	case scaleAtHeld:
		if w.decode() != nil {
			out = append(out, scaleMove{kind: moveScale, side: 1, delta: 1})
		}
	case scaleAtDraining, scaleAtLast:
		if idx := w.lossIndices(w.prefill()); len(idx) > 0 {
			out = append(out, scaleMove{kind: moveLosePod, side: 0, index: idx[0]})
		}
	case scaleAtCanaryStep:
		out = append(out, scaleMove{kind: moveCanaryPromote})
	}
	return out
}

// partitionValue reads an effective partition as a count; nil holds nothing.
func partitionValue(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

// check runs the model's per-tick readings on every side.
func (h *partitionHunt) check() {
	w := h.w
	if h.clk != w.clk {
		h.reset()
	}
	for _, s := range w.sides {
		rows := w.rows(s)
		held := w.heldIndices(s)
		stand := partitionStand{partition: partitionValue(w.effectivePartition(s)), target: s.h.target.Name, replicas: s.h.replicas, rows: len(rows), held: len(held)}
		h.checkHeldSetKept(s, stand, held, rows)
		h.checkHeldCount(s, stand, held, rows)
		h.checkNothingRolledBack(s, rows, stand)
	}
}

// unsetRevisionIndices is the set of rows that have run no revision yet.
func unsetRevisionIndices(rows []workloadtypes.InstanceStatus) map[int32]bool {
	out := map[int32]bool{}
	for _, r := range rows {
		if r.RunningRevision == "" {
			out[r.Index] = true
		}
	}
	return out
}

// checkHeldSetKept holds the held set to its size while the partition,
// the target, the count and the rows stand: the roll never takes a held
// Instance, so the set only shrinks when one of those changes. An
// Instance that leaves the set having run no revision a tick ago is the
// create pass building a new Instance of a scale-up on the target while
// the held set counted it; one that ran a revision is a held Instance the
// roll took.
func (h *partitionHunt) checkHeldSetKept(s *scaleSide, stand partitionStand, held map[int32]bool, rows []workloadtypes.InstanceStatus) {
	prev, ok := h.stand[s]
	prevHeld, prevUnset := h.held[s], h.unset[s]
	h.stand[s], h.held[s], h.unset[s] = stand, held, unsetRevisionIndices(rows)
	if !ok || !prev.sameStanding(stand) || stand.held >= prev.held {
		return
	}
	for idx := range prevHeld {
		switch {
		case held[idx]:
		case prevUnset[idx]:
			h.w.note("new-instance-under-the-partition-built-on-the-new-version", "%s Instance %d had run no revision and counted among the %d Instances the partition of %d holds; the create pass built it on the target: %s",
				s.comp, idx, prev.held, stand.partition, h.w.picture())
		default:
			h.w.note("held-instance-moved", "%s Instance %d was among the %d Instances the partition of %d holds and left the set, with the partition, the target, the count and the rows unchanged: %s",
				s.comp, idx, prev.held, stand.partition, h.w.picture())
		}
	}
}

// checkHeldCount ties the held count to the partition's value with a count
// of its own: the Instances off the target (one that has run no revision
// included) with no update attempt open or continuing and no migration in
// flight are the ones a partition can hold, and the held set has as many
// of them as the partition names, or all of them when they are fewer. An
// engine that holds one fewer or one more than asked is read here.
func (h *partitionHunt) checkHeldCount(s *scaleSide, stand partitionStand, held map[int32]bool, rows []workloadtypes.InstanceStatus) {
	if stand.partition <= 0 {
		return
	}
	candidates := 0
	for i := range rows {
		r := &rows[i]
		if r.RunningRevision == stand.target || workloadtypes.Owner(r) == workloadtypes.OwnerUpdate || workloadtypes.UpdateContinuation(r) {
			continue
		}
		if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationMigrate && r.Phase != workloadtypes.InstancePhaseMigrating {
			continue
		}
		candidates++
	}
	if want := min(int(stand.partition), candidates); len(held) != want {
		h.w.note("held-count-off-the-partition", "%s holds %d Instances under a partition of %d with %d Instances off the target it could hold; the partition names %d: %s",
			s.comp, len(held), stand.partition, candidates, want, h.w.picture())
	}
}

// checkNothingRolledBack holds every Instance seen Ready on the target to
// the target while the target, the count and the rows stand: a partition
// set or raised over it, a repair of it or a move beside it never puts it
// back on an older revision.
func (h *partitionHunt) checkNothingRolledBack(s *scaleSide, rows []workloadtypes.InstanceStatus, stand partitionStand) {
	key := partitionStand{target: stand.target, replicas: stand.replicas, rows: stand.rows}
	if h.shape[s] != key {
		h.shape[s] = key
		h.onTarget[s] = map[int32]bool{}
	}
	seen := h.onTarget[s]
	for _, r := range rows {
		if replacementOrLeaving(r) {
			continue
		}
		switch {
		case r.RunningRevision == stand.target && r.Phase == workloadtypes.InstancePhaseReady && r.Operation == nil:
			seen[r.Index] = true
		case seen[r.Index] && r.RunningRevision != "" && r.RunningRevision != stand.target:
			delete(seen, r.Index)
			h.w.note("rolled-back-under-the-partition", "%s Instance %d served the target and runs %s again with the target unchanged: %s",
				s.comp, r.Index, h.w.short(s, r.RunningRevision), h.w.picture())
		}
	}
}

// endCheck reads a complete settled end with the status reader's rule
// (status.ReachedDesiredShape): staged under a partition that holds
// Instances, complete under one that holds none. The staged ends are
// recorded with their shape, and a partition with no Instance left to
// hold as its own end: the operator set it when every Instance had moved
// or was already moving.
func (h *partitionHunt) endCheck() {
	w := h.w
	for _, s := range w.sides {
		rows := w.rows(s)
		p := partitionValue(w.effectivePartition(s))
		old := status.HeldInstanceCount(rows, s.h.target.Name)
		switch {
		case p > 0 && old == 0:
			w.note("partition-holds-nothing", "%s rests complete under a partition of %d with no Instance left on the old version: %s", s.comp, p, w.picture())
		case !status.ReachedDesiredShape(rows, s.h.target.Name, p, s.h.replicas):
			w.note("staged-shape-disagrees", "%s rests at the shape its partition of %d allows (%d Instances on the old version) and the status reader does not read it so: %s", s.comp, p, old, w.picture())
		case p > 0:
			w.note("ends-staged", "%s rests with %d of %d Instances on the old version under a partition of %d: %s", s.comp, old, s.h.replicas, p, w.picture())
		}
	}
}

// partitionReferenceConfigs are the configurations every run explores: the
// reference shapes under their reference settings, each with a
// partition standing at the push on the prefill side, the decode side or
// both, and one with none, so every value is also set mid-roll.
func partitionReferenceConfigs() []scaleConfig {
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	return []scaleConfig{
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, partition: 2}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true, decodePartition: 4}},
		{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true, partition: 2, decodePartition: 1}},
		{shape: scaleShape{15, 15, 1}, settings: scaleSettings{prefill: surge, decode: recreate, keepRatio: true, firstSide: v1beta1.DecoderComponent, partition: 5}},
		{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: recreate, decode: surge, canary: true, partition: 2}},
		{shape: scaleShape{24, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, policy: true, partition: 2}},
		{shape: scaleShape{30, 10, 1}, settings: scaleSettings{prefill: surge, decode: surge, maintainRatio: true, decodePartition: 5}},
		{shape: scaleShape{12, 4, 2}, settings: scaleSettings{prefill: recreate, decode: recreate, keepRatio: true, firstSide: v1beta1.DecoderComponent, partition: 1}},
		{shape: scaleShape{12, 4, 4}, settings: scaleSettings{prefill: surge, decode: surge, partition: 2}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, router: true, partition: 2, decodePartition: 2}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate}},
	}
}

// partitionConfigs lists the configurations of a run: the reference ones,
// and in full mode a seeded sample of the product of shapes, gang sizes,
// strategy pairs, settings and partition placements (prefill at two,
// decode at half, both, none).
func partitionConfigs(full bool) []scaleConfig {
	configs := partitionReferenceConfigs()
	if !full {
		return configs
	}
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	shapes := [][2]int32{{12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}}
	pods := []int32{1, 2, 4, 8}
	strategies := [][2]workloadtypes.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate}}
	variants := []scaleSettings{
		{},
		{maintainRatio: true},
		{keepRatio: true},
		{keepRatio: true, firstSide: v1beta1.DecoderComponent},
		{keepRatio: true, firstSide: v1beta1.EngineComponent},
		{canary: true},
		{policy: true},
		{router: true},
		{maintainRatio: true, policy: true},
	}
	seen := map[string]bool{}
	for _, cfg := range configs {
		seen[cfg.String()] = true
	}
	var all []scaleConfig
	for _, shape := range shapes {
		for _, p := range pods {
			if (shape[0]+shape[1])*p > scaleFullPodCeiling {
				continue
			}
			for _, pair := range strategies {
				for _, variant := range variants {
					for _, placement := range [][2]int32{{2, 0}, {0, shape[1] / 2}, {2, 1}, {0, 0}} {
						settings := variant
						settings.prefill, settings.decode = pair[0], pair[1]
						settings.partition, settings.decodePartition = placement[0], placement[1]
						cfg := scaleConfig{shape: scaleShape{shape[0], shape[1], p}, settings: settings}
						if !seen[cfg.String()] {
							seen[cfg.String()] = true
							all = append(all, cfg)
						}
					}
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	if len(all) > partitionFullSample {
		all = all[:partitionFullSample]
	}
	return append(configs, all...)
}

// partitionExpectedEnds are the ends a run may reach that are not
// findings: a spent retry schedule, a pause, the partition's own hold on
// a failed attempt, a roll that rests at the shape its partition allows,
// and a partition that found nothing left to hold.
var partitionExpectedEnds = map[string]bool{"held": true, "ends-paused": true, "held-by-the-partition": true, "partition-holds-nothing": true, "ends-staged": true}

// knownPartitionFindingClasses are the finding classes the model shows on
// main today: the shared world's pictures of the version mix after a move
// that re-plans the batches, which a partition change is one of, and the
// new Instance of a scale-up under a partition built on the new version.
// A finding in a known class is reported and does not fail the run; a fix
// removes its entry and the lock test that holds the picture.
var knownPartitionFindingClasses = partitionKnownClasses()

func partitionKnownClasses() map[string]string {
	out := map[string]string{
		"new-instance-under-the-partition-built-on-the-new-version": "a scale-up or a follower write adds an Instance while a partition holds the Component; the held set counts it among the held until it runs a revision, and the create pass builds it on the target, so the Component rests with one more Instance on the new version than the partition allows (a new Instance of a roll in flight joins the revision the roll leaves)",
	}
	for class, picture := range knownScaleFindingClasses {
		out[class] = picture
	}
	return out
}

// TestPartition_EveryChangeLeavesTheRollAWayOut explores every
// configuration: a partition set, raised, lowered or cleared on either
// side at any moment of a roll, once or twice, beside the world's losses
// and the operator's other moves, leaves a roll that finishes at the shape
// the partition allows, holds on a spent retry schedule or on the
// partition's own hold, or waits paused; within its budgets, with the
// held set never shrinking, its count the partition's, and no Instance
// put back on an older revision.
func TestPartition_EveryChangeLeavesTheRollAWayOut(t *testing.T) {
	full, only := scaleHuntMode(partitionFullEnv, partitionOnlyEnv)
	runScaleHunt(t, scaleHunt{
		full:    full,
		runs:    scaleHuntRuns(full),
		configs: keepScaleConfigs(partitionConfigs(full), func(scaleConfig) bool { return true }, only),
		setup: func(w *scaleWorld) {
			installPartitionHunt(w)
			if full {
				w.tickBudget = partitionFullTickBudget
			}
		},
		expectedEnds: partitionExpectedEnds,
		known:        knownPartitionFindingClasses,
	})
}

// TestPartition_AScaleUpUnderAPartitionAboveTheCountComesUpOnTheNewVersion
// holds the picture the model found: 12 prefill and 8 decode under
// RecreatePod at ten percent, version 2 pushed with a partition of 9 on
// the decoder, above its count, so every decoder stays on version 1 while
// the prefill side rolls. The operator scales the decoder to 9. The held
// set counts the ninth decoder among the nine the partition holds while
// it is built, and the create pass builds it on version 2: the decoder
// rests with 8 on version 1 and 1 on version 2 under a partition that
// says all nine stay on version 1. When the new Instance joins version 1
// and stays held, this test fails, and the known class and this lock go
// with it.
func TestPartition_AScaleUpUnderAPartitionAboveTheCountComesUpOnTheNewVersion(t *testing.T) {
	recreate := workloadtypes.UpdateStrategyRecreatePod
	cfg := scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, decodePartition: 9}}
	w := newScaleWorld(t, cfg)
	w.reachIdle()
	w.push(fixedImage)
	if !w.playOut(scaleTickBound(false)) || !w.complete() {
		t.Fatalf("the roll did not rest with every decoder held: %s", w.picture())
	}
	decode := w.decode()
	if old := status.HeldInstanceCount(w.rows(decode), decode.h.target.Name); old != 8 {
		t.Fatalf("the partition of 9 left %d of 8 decoders on version 1 before the scale: %s", old, w.picture())
	}
	w.apply(scaleMove{kind: moveScale, side: 1, delta: 1})
	w.tick()
	if held := len(w.heldIndices(decode)); held != 9 {
		t.Fatalf("the held set reads %d decoders held a tick after the scale, not all nine: %s", held, w.picture())
	}
	if !w.playOut(scaleTickBound(false)) || !w.complete() {
		t.Fatalf("the world did not rest after the scale: %s", w.picture())
	}
	rows := w.rows(decode)
	old := status.HeldInstanceCount(rows, decode.h.target.Name)
	if len(rows) != 9 || old != 8 {
		t.Fatalf("the picture moved: %d of %d decoders run version 1 after the scale under a partition of 9; if the ninth now joins version 1 and stays held, remove the known class and this lock: %s", old, len(rows), w.picture())
	}
	t.Logf("the ninth decoder comes up on version 2 beside 8 held on version 1 under a partition of 9: %s", w.picture())
}

// TestPartition_ARollBackUnderACanaryReturnsEveryInstanceToVersionOne holds
// the engine's picture of a push the operator takes back while a canary
// step stands: 20 prefill and 10 decode, RecreatePod and SurgeThenDrain
// at ten percent, a prefill partition of 2, a canary whose first step
// moves a quarter. Version 2 pushed rests at the step with 5 prefill and
// 3 decoders on version 2 under the step's partitions of 15 and 7. The
// operator pushes version 1 back: the canary's record ends, the unit
// reads RollingBack, the canary projects partition 0 over the operator's
// 2, all 20 and 10 return to version 1, and the unit then reads Stable.
func TestPartition_ARollBackUnderACanaryReturnsEveryInstanceToVersionOne(t *testing.T) {
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	cfg := scaleConfig{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: recreate, decode: surge, canary: true, partition: 2}}
	w := newScaleWorld(t, cfg)
	w.reachIdle()
	w.push(fixedImage)
	if !w.playOut(scaleTickBound(false)) || !w.complete() {
		t.Fatalf("the canary's first step did not rest: %s", w.picture())
	}
	prefill, decode := w.prefill(), w.decode()
	for _, want := range []struct {
		s   *scaleSide
		old int32
	}{{prefill, 15}, {decode, 7}} {
		if old := status.HeldInstanceCount(w.rows(want.s), want.s.h.target.Name); old != want.old {
			t.Fatalf("%s rests with %d Instances on version 1 at the first step, not %d: %s", want.s.comp, old, want.old, w.picture())
		}
	}
	w.apply(scaleMove{kind: moveRollBack})
	unit := w.isvc.Status.Components[v1beta1.EngineComponent]
	if unit.Canary != nil || unit.RolloutPhase != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the push back left the canary's record standing: phase %s, record %+v", unit.RolloutPhase, unit.Canary)
	}
	for _, s := range []*scaleSide{prefill, decode} {
		if p := partitionValue(w.effectivePartition(s)); p != 0 {
			t.Fatalf("%s rolls back under a partition of %d; the canary projects 0 over the operator's %d while the record is ended", s.comp, p, s.partition)
		}
	}
	if !w.playOut(scaleTickBound(false)) || !w.complete() {
		t.Fatalf("the roll back did not rest: %s", w.picture())
	}
	for _, s := range []*scaleSide{prefill, decode} {
		if old := status.HeldInstanceCount(w.rows(s), s.h.target.Name); old != 0 {
			t.Fatalf("%s rests with %d Instances still on version 2 after the push back: %s", s.comp, old, w.picture())
		}
	}
	if phase := w.isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; phase != v1beta1.RolloutPhaseStable {
		t.Fatalf("the unit reads %s with every Instance back on version 1, not Stable", phase)
	}
	t.Logf("all 20 and 10 back on version 1, the operator's partition of %d not in force during the roll back: %s", prefill.partition, w.picture())
}
