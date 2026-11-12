package workload

import (
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// BuildPlan computes the desired ComponentPlan from the projected
// (WorkloadDesiredSpec, WorkloadObservedState) pair. Pure: no I/O,
// no controller-runtime calls.
//
// Runner layout:
//   - Single-pod (desired.MultiPod=false): one "default" Runner of
//     size 1. Used by Router and Engine/Decoder without Leader+Worker.
//   - Multi-pod (desired.MultiPod=true): one "leader" Runner of size 1
//     plus one "worker" Runner of the user-set Worker.Size.
//
// An unset Lifecycle field means its fixed fallback; BuildPlan resolves
// those here, so a spec that names none of them plans the same way as
// one that spells them out.
func BuildPlan(component types.ComponentType, desired types.WorkloadDesiredSpec, observed types.WorkloadObservedState) (types.ComponentPlan, error) {
	replicas := desired.Replicas
	if replicas < 0 || (replicas == 0 && !desired.AllowZeroReplicas) {
		replicas = 1
	}

	workerSize := types.WorkerSizeOf(desired.Runners)

	// Compute the plan's Instance index set. Existing InstanceStatus
	// entries — including any migration-surge index above the replica
	// count — are preserved so the reconcile loop keeps driving them.
	// The plan grows beyond replicas during the surge phase of a
	// migration; scale-down logic is responsible for picking the right
	// deletion target after that surge resolves.
	indices := instancePlanIndices(observed.InstanceStatuses, replicas, observed.RetryBlocks, observed.CordonedInstances, observed.RollTarget)
	// The rows are indexed once here, so each planned Instance finds its
	// row in one read whatever the fleet's size.
	observed.IndexRows()
	instances := make([]types.InstancePlan, len(indices))
	runners := runnersForInstance(desired.MultiPod, workerSize)
	layout := types.ComponentPlan{Runners: runners, WorkerSizeByRevision: observed.WorkerSizeByRevision}
	for i, idx := range indices {
		// An Instance renders the width its own revision recorded, so a gang
		// settled on an older revision keeps its layout until the roll takes it,
		// and a new Instance the width of the revision it is built on.
		instances[i] = types.InstancePlan{
			Index:       idx,
			Incarnation: incarnationForIndex(&observed, idx),
			Runners:     layout.RunnersFor(types.InstancePlan{Runners: runners}, renderedRevision(desired, &observed, idx)),
			// Relocation-directive memory: the adapter projects the
			// per-instance node-exclusion list from the audit ledger;
			// Render turns it into a required NotIn hostname term so
			// the rebuild lands off the recorded suspect node(s).
			ExcludedNodes: append([]types.NodeExclusion(nil), observed.ExcludedNodesByInstance[idx]...),
		}
	}

	lifecycle := desired.Lifecycle

	return types.ComponentPlan{
		Component:      component,
		Replicas:       replicas,
		Instances:      instances,
		RestartPolicy:  restartPolicyOrDefault(lifecycle.RestartPolicy, desired.MultiPod),
		UpdateStrategy: updateStrategyWithDefaults(lifecycle.UpdateStrategy),
		ReadyPolicy:    readyPolicyOrDefault(lifecycle.ReadyPolicy, desired.MultiPod),
		// Per-resource value only: the operator-configured fallback lives in
		// the ConfigMap the adapter reads, so the adapter overlays the
		// resolved window on the returned plan the same way it overlays
		// GangScheduleTimeout. Zero here means "no per-resource window".
		InstanceReadyTimeout: ResolveInstanceReadyTimeout(lifecycle.InstanceReadyTimeout, 0),
		MinReadySeconds:      max(desired.MinReadySeconds, 0),
		MigrationMode:        MigrationModeOrDefault(lifecycle.MigrationPolicy),
		Paused:               desired.Paused,
		PauseFreeze:          desired.PauseFreeze,
		TopologyKey:          desired.TopologyKey,
		TopologySpread:       desired.TopologySpread,
		TopologySpreadKey:    desired.TopologySpreadKey,
		PairingProtocol:      desired.PairingProtocol,
		Runners:              runners,
		WorkerSizeByRevision: observed.WorkerSizeByRevision,
	}, nil
}

// renderedRevision names the revision the pods of the row at idx render
// at: the revision it runs, else the one it converges to, else the one its
// attempt names; the row a migration's surge claims renders the revision
// its operation names (the roll's target, for a move admitted while the
// roll could take the source), else its source's. A row that records no
// revision, and an index with no row, render the revision a new Instance
// is built on, read from the same function the Create pass reads: the
// stable side of a canary step that holds the Component, else the roll
// target, which an empty name renders.
func renderedRevision(desired types.WorkloadDesiredSpec, observed *types.WorkloadObservedState, idx int32) string {
	row := observed.Instance(idx)
	if row != nil && migrationSurgeRow(row) {
		return migrationSurgeRevision(observed, row)
	}
	if rev := recordedRevision(row); rev != "" {
		return rev
	}
	return query.NewInstanceRevision(desired, observed, observed.RollTarget)
}

// recordedRevision names the revision a row records for its pods: the one
// it runs, else the one it converges to, else the one its attempt names.
// Empty for no row and for a row that records none.
func recordedRevision(row *types.InstanceStatus) string {
	if row == nil {
		return ""
	}
	if row.RunningRevision != "" {
		return row.RunningRevision
	}
	if row.TargetRevision != "" {
		return row.TargetRevision
	}
	if row.Operation != nil {
		return row.Operation.TargetRevision
	}
	return ""
}

// migrationSurgeRow reports the row a migration's surge claims: a Creating
// row whose Migrate operation names the source it moves.
func migrationSurgeRow(row *types.InstanceStatus) bool {
	return row.Phase == types.InstancePhaseCreating && row.Operation != nil &&
		row.Operation.Type == types.InstanceOperationMigrate && row.Operation.SurgeIndex != nil
}

// migrationSurgeRevision names the revision a migration's surge renders:
// the one its operation names, else the one its source runs. Empty when
// the source is gone, which renders the desired width.
func migrationSurgeRevision(observed *types.WorkloadObservedState, row *types.InstanceStatus) string {
	if row.Operation.TargetRevision != "" {
		return row.Operation.TargetRevision
	}
	if source := observed.Instance(*row.Operation.SurgeIndex); source != nil && source.Index != row.Index {
		return source.RunningRevision
	}
	return ""
}

// runnersForInstance returns the Runner layout for one Instance based
// on the Component's multi-pod flag and worker size. The "worker"
// runner is emitted even when workerSize=0 so the layout shape is
// consistent (downstream code keys on Runner.Name); a zero-size worker
// runner contributes no pod.
func runnersForInstance(multiPod bool, workerSize int32) []types.RunnerPlan {
	if !multiPod {
		return []types.RunnerPlan{{Name: types.RunnerDefault, Size: 1}}
	}
	return []types.RunnerPlan{
		{Name: types.RunnerLeader, Size: 1},
		{Name: types.RunnerWorker, Size: workerSize},
	}
}

func restartPolicyOrDefault(p *types.RestartPolicy, multiPod bool) types.RestartPolicy {
	if p != nil {
		return *p
	}
	if multiPod {
		return types.RestartPolicyRecreateInstance
	}
	return types.RestartPolicyNone
}

func readyPolicyOrDefault(p *types.InstanceReadyPolicy, multiPod bool) types.InstanceReadyPolicy {
	if p != nil {
		return *p
	}
	if multiPod {
		return types.InstanceReadyPolicyAllPodReady
	}
	return types.InstanceReadyPolicyNone
}

func updateStrategyWithDefaults(s *types.UpdateStrategy) types.UpdateStrategy {
	if s == nil {
		s = &types.UpdateStrategy{}
	}
	out := *s
	if out.Type == "" {
		// SurgeThenDrain default: recreates throttle to MaxUnavailable
		// at the dispatcher gate, whereas in-place at scale could mass-
		// drain the fleet on a single wake-up. Opt into in-place
		// explicitly via spec.<component>.omeNative.updateStrategy.type.
		out.Type = types.UpdateStrategySurgeThenDrain
	}
	if out.InPlaceUpdateStrategy == nil {
		out.InPlaceUpdateStrategy = &types.InPlaceUpdateStrategy{}
	}
	if out.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle == nil {
		mark := true
		out.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle = &mark
	}
	return out
}

// ResolveInstanceReadyTimeout resolves the effective per-Component
// InstanceReadyTimeout: the per-resource lifecycle value when set,
// otherwise the operator-configured window (lifecycle.instanceReadyTimeout).
// Zero means neither level supplies one and operations open with no
// deadline. Exported so adapters stamping deadlines outside BuildPlan
// (e.g. migration accept) resolve the identical value the per-op writers
// read from ComponentPlan.InstanceReadyTimeout.
func ResolveInstanceReadyTimeout(spec *metav1.Duration, configured time.Duration) time.Duration {
	if spec != nil && spec.Duration > 0 {
		return spec.Duration
	}
	return configured
}

// MigrationModeOrDefault resolves the effective per-Component
// migration mode: the configured MigrationPolicy.Mode, or Auto when
// unset. Exported so adapters gating outside BuildPlan (e.g. migration
// accept rejecting under Never) resolve the identical value the
// dispatcher reads from ComponentPlan.MigrationMode.
func MigrationModeOrDefault(p *types.MigrationPolicy) types.MigrationMode {
	if p == nil || p.Mode == "" {
		return types.MigrationModeAuto
	}
	return p.Mode
}

// instancePlanIndices computes the per-Instance index set the plan should
// drive, in three steps over one reading of the rows. Keep: a surge pair
// in flight stays whatever the count, the source charged to it and the
// replacement the transient extra over it. Rank: the steady rows fill
// the rest of the count in rank order (scaleDownRank), lowest index first
// within a rank, so a scale-down removes the highest ranks first; a
// source whose replacement is proven promoted is terminal cleanup and
// fills no slot. Add: new indexes round a scale-up out, lowest unused
// first. blocks lists the Component's retry blocks, which the rank reads a
// row's revision by; cordoned holds the indices with a pod on a cordoned
// node, which rank below every other row; rollTarget names the revision
// the roll converges to, which the rank keeps the rows already on ahead
// of the rest.
//
// A gang surge whose source has failed keeps nothing by itself: the pair
// competes for the count behind the Ready rows as one unit, the source
// charged like any steady row and its replacement row following it in or
// out, so a scale-down retires the two together instead of taking a
// healthy sibling. The pair counts as cordoned when either half is.
func instancePlanIndices(instances []types.InstanceStatus, replicas int32, blocks []types.RetryBlock, cordoned map[int32]struct{}, rollTarget string) []int32 {
	sel := readIndexSelection(instances, replicas, blocks, cordoned, rollTarget)
	sel.keepPairsInFlight()
	sel.fillCountByRank()
	sel.addNewIndexes()
	slices.Sort(sel.chosen)
	return sel.chosen
}

// indexSelection is one plan's choice of Instance indexes, read once from
// the rows and filled by the three steps of instancePlanIndices.
type indexSelection struct {
	replicas int32
	// existing lists every recorded index, ascending.
	existing []int32
	// position is where each recorded index's row sits in the rows, the
	// last row of an index when two record it; the rows themselves are
	// read in place and never copied.
	position map[int32]int
	rank     map[int32]int

	// pairMembers holds both halves of every surge pair in flight, kept
	// whatever the count; pairSources holds the halves charged to it.
	pairMembers map[int32]struct{}
	pairSources map[int32]struct{}
	// retiring holds the sources whose replacement is proven promoted.
	retiring map[int32]struct{}
	// A failed gang surge pair moves as one unit: failedTargetOf names the
	// replacement row that follows each failed source, failedSourceOf the
	// source each such row follows.
	failedTargetOf map[int32]int32
	failedSourceOf map[int32]int32

	// chosen and picked hold the indexes selected so far; charged counts
	// the ones charged to the count.
	chosen  []int32
	picked  map[int32]struct{}
	charged int32
}

// readIndexSelection reads the rows once: where each index's row sits, the
// surge pairs in flight and the rank of every row. The failed gang surge
// sources are read once and handed to every reading that excludes them.
func readIndexSelection(instances []types.InstanceStatus, replicas int32, blocks []types.RetryBlock, cordoned map[int32]struct{}, rollTarget string) *indexSelection {
	position := rowPositions(instances)
	sel := &indexSelection{
		replicas:    replicas,
		existing:    make([]int32, 0, len(position)),
		position:    position,
		rank:        make(map[int32]int, len(instances)),
		pairMembers: migrationInFlightIndices(instances),
		pairSources: migrationSourceIndices(instances),
		retiring:    map[int32]struct{}{},
		chosen:      make([]int32, 0, len(position)),
		picked:      make(map[int32]struct{}, len(position)),
	}
	for idx := range position {
		sel.existing = append(sel.existing, idx)
	}
	slices.Sort(sel.existing)
	unpinned := failedGangSurgeSources(instances, position)
	for idx := range updateSurgeInFlightIndices(instances, unpinned) {
		sel.pairMembers[idx] = struct{}{}
	}
	for idx := range updateSurgeSourceIndices(instances, unpinned) {
		sel.pairSources[idx] = struct{}{}
	}
	sel.failedTargetOf, sel.failedSourceOf = failedGangSurgeFollowers(instances, unpinned, position)
	claims := sel.claimReplacements(instances, unpinned)
	sel.rankRows(instances, blocks, cordoned, rollTarget)
	sel.releasePromotedSources(instances, claims)
	return sel
}

// rowPositions is where each recorded index's row sits in the rows: the
// last row of an index when two record it.
func rowPositions(instances []types.InstanceStatus) map[int32]int {
	position := make(map[int32]int, len(instances))
	for i := range instances {
		position[instances[i].Index] = i
	}
	return position
}

// claimsReplacement reports whether the row's attempt claims a replacement
// index: a migration or an update with a surge index.
func claimsReplacement(s *types.InstanceStatus) bool {
	if s.Operation == nil || s.Operation.SurgeIndex == nil {
		return false
	}
	return s.Operation.Type == types.InstanceOperationMigrate || s.Operation.Type == types.InstanceOperationUpdate
}

// claimReplacements counts the sources that claim each replacement index
// and keeps the replacement of every source that has not failed beside its
// source. Every claim counts, so an index two sources name promotes
// neither; a claim of a source in failed keeps nothing. Whether a source
// has proof enough to retire is decided after the rows are read
// (releasePromotedSources).
func (sel *indexSelection) claimReplacements(instances []types.InstanceStatus, failed map[int32]struct{}) map[int32]int {
	claims := map[int32]int{}
	for i := range instances {
		s := &instances[i]
		if !claimsReplacement(s) {
			continue
		}
		target := *s.Operation.SurgeIndex
		claims[target]++
		if _, sourceFailed := failed[s.Index]; !sourceFailed {
			sel.pairMembers[target] = struct{}{}
		}
	}
	return claims
}

// rankRows ranks every row for a scale-down. A failed gang surge pair
// counts as cordoned when either half has a pod on a cordoned node.
func (sel *indexSelection) rankRows(instances []types.InstanceStatus, blocks []types.RetryBlock, cordoned map[int32]struct{}, rollTarget string) {
	for i := range instances {
		s := &instances[i]
		_, onCordonedNode := cordoned[s.Index]
		if target, paired := sel.failedTargetOf[s.Index]; paired {
			if _, targetCordoned := cordoned[target]; targetCordoned {
				onCordonedNode = true
			}
		}
		sel.rank[s.Index] = scaleDownRank(s, blocks, onCordonedNode, rollTarget)
	}
}

// releasePromotedSources frees every pair whose replacement is proven
// promoted: the pair keeps neither half any more, and the source is
// terminal cleanup that fills no slot of the count.
func (sel *indexSelection) releasePromotedSources(instances []types.InstanceStatus, claims map[int32]int) {
	for i := range instances {
		s := &instances[i]
		if s.Operation == nil || s.Operation.SurgeIndex == nil {
			continue
		}
		target := *s.Operation.SurgeIndex
		at, found := sel.position[target]
		if !found || !replacementPromoted(s, &instances[at], claims[target]) {
			continue
		}
		delete(sel.pairMembers, s.Index)
		delete(sel.pairMembers, target)
		delete(sel.pairSources, s.Index)
		sel.retiring[s.Index] = struct{}{}
	}
}

// replacementPromoted reports whether a source's replacement carries the
// proof its verb demands: a gang update's drain step and the attempt's
// recorded revision, a migration's Ready target on the source's revision.
func replacementPromoted(source, target *types.InstanceStatus, claims int) bool {
	if source.Operation.Type == types.InstanceOperationUpdate {
		return updateHandoffPromoted(source, target, claims)
	}
	return migrationHandoffPromoted(source, target, claims)
}

// keepPairsInFlight keeps both halves of every surge pair in flight,
// whatever the count, and charges the sources to it: the source is the
// replica being replaced, the replacement the transient extra.
func (sel *indexSelection) keepPairsInFlight() {
	for _, idx := range sel.existing {
		if _, member := sel.pairMembers[idx]; !member {
			continue
		}
		sel.choose(idx)
		if _, source := sel.pairSources[idx]; source {
			sel.charged++
		}
	}
}

// fillCountByRank fills the rest of the count with the steady rows, rank
// 0 first and the lowest index first within a rank, so a scale-down
// removes the highest ranks first. A row kept this way brings a failed
// gang surge's replacement row along, uncharged.
func (sel *indexSelection) fillCountByRank() {
	for rank := 0; rank <= lowestScaleDownRank; rank++ {
		for _, idx := range sel.existing {
			if sel.charged >= sel.replicas {
				return
			}
			if !sel.fillsSlot(idx, rank) {
				continue
			}
			sel.choose(idx)
			sel.charged++
			if target, paired := sel.failedTargetOf[idx]; paired {
				sel.choose(target)
			}
		}
	}
}

// fillsSlot reports whether the row at idx may fill a slot of the count at
// this rank: not chosen yet, not a retiring source, not the replacement
// row of a failed gang surge (its source brings it), and ranked here.
func (sel *indexSelection) fillsSlot(idx int32, rank int) bool {
	if _, already := sel.picked[idx]; already {
		return false
	}
	if _, retiring := sel.retiring[idx]; retiring {
		return false
	}
	if _, follows := sel.failedSourceOf[idx]; follows {
		return false
	}
	return sel.rank[idx] == rank
}

// choose adds an index to the selection once.
func (sel *indexSelection) choose(idx int32) {
	if _, already := sel.picked[idx]; already {
		return
	}
	sel.chosen = append(sel.chosen, idx)
	sel.picked[idx] = struct{}{}
}

// addNewIndexes rounds a scale-up out with the lowest unused indexes,
// lowest first. An index a row records stays taken whether or not the
// plan keeps the row. Every index below a slot just taken is in use, so
// the search resumes at that slot and a scale-up of any size pays one
// pass over the indexes in all; a count already filled reads none.
func (sel *indexSelection) addNewIndexes() {
	if sel.charged >= sel.replicas {
		return
	}
	taken := make(map[int32]struct{}, len(sel.existing))
	for _, idx := range sel.existing {
		taken[idx] = struct{}{}
	}
	slot := int32(0)
	for sel.charged < sel.replicas {
		slot = lowestUnusedIndex(taken, slot)
		sel.chosen = append(sel.chosen, slot)
		taken[slot] = struct{}{}
		sel.charged++
	}
}

// scaleDownRank orders the steady rows a scale-down keeps, lowest kept
// first. A scale-down removes the Instances the roll has yet to move
// before the ones it has moved, and never throws a replacement or a
// rebuild in progress away for an Instance that still owes the same
// work. A source whose SurgeThenDrain replacement is coming up on a sound
// revision ranks above every other row: the steady Instance kept in its
// place still owes a surge and a drain. Then, on a sound revision: a
// serving row on the roll's target; a row whose rebuild is coming up (the
// scale-down wave keeps a serving Instance it retires in rotation until
// that rebuild serves, ops.DeleteBatch, so the Component never serves
// less through the scale than the roll alone would leave it); a Ready row
// on the roll's target whose published counts fall short of its pods (the
// counts trail the pods by a pass after a promotion, and a set broken
// after it is the repair's); a serving row on any other revision; then
// the rest. A row on a failing revision ranks below every row on a sound
// one: its Ready is the pause between crashes of a revision the retry
// schedule reads as failed, not capacity the Component can keep. A row
// with a pod on a cordoned node ranks below every row without one: its
// node is being emptied, so keeping it means moving it later, while
// dropping it frees the node now; a rebuild coming up keeps its place
// whatever old set it leaves there. A row left Creating with no operation
// ranks below every other row: nothing drives it and it has never served,
// so keeping it would retire an Instance whose pod still runs.
func scaleDownRank(s *types.InstanceStatus, blocks []types.RetryBlock, onCordonedNode bool, rollTarget string) int {
	if creatingWithNoOperation(s) {
		return lowestScaleDownRank
	}
	failing := types.RowOnFailingRevision(s, blocks)
	if replacementComingUp(s) && !failing {
		return 0
	}
	onTarget := rollTarget != "" && s.Phase == types.InstancePhaseReady && s.RunningRevision == rollTarget
	rank := 1
	switch {
	case onTarget && keepsPreference(s):
	case types.RebuildComingUp(s) && !failing:
		return 2
	case onTarget:
		rank = 3
	case keepsPreference(s):
		rank = 4
	default:
		rank = 5
	}
	if failing {
		rank += 5
	}
	if onCordonedNode {
		rank += 10
	}
	return rank
}

// lowestScaleDownRank is the last rank scaleDownRank assigns, the one a
// row nothing drives takes; the ranked rows end one below it.
const lowestScaleDownRank = 21

// replacementComingUp reports whether the row is the source of a
// SurgeThenDrain update whose replacement is being created, or is up
// with the old pod set draining: Phase Updating with an Update
// operation at the Surge or SurgeDrain step. A gang source at those
// steps is kept through the protected pair, so the rank never decides
// it; a Failed source, or an attempt its deadline ended, has no
// replacement coming up.
func replacementComingUp(s *types.InstanceStatus) bool {
	if s.Phase != types.InstancePhaseUpdating || s.Operation == nil || s.Operation.Type != types.InstanceOperationUpdate {
		return false
	}
	switch s.Operation.Step {
	case types.UpdateStepSurge, types.UpdateStepSurgeDrain, types.UpdateStepSurgeDrainSettle:
		return true
	}
	return false
}

// creatingWithNoOperation reports a row left in phase Creating with no
// operation: a move's replacement the record's expiry released, or a
// status write applied in part. No pass owns the shape and no deadline
// runs on it; it fills the count only when no other row can, and a
// scale-down retires it first.
func creatingWithNoOperation(s *types.InstanceStatus) bool {
	return s.Phase == types.InstancePhaseCreating && s.Operation == nil
}

// keepsPreference reports whether a row is a stable replica the plan keeps
// ahead of the rest: Ready, with every pod its published counts hold in
// rotation. A Ready row that serves fewer pods than it has serves nothing
// the Component can count on, whatever its phase says. A row with no
// published pods is read by its phase alone.
func keepsPreference(s *types.InstanceStatus) bool {
	if s.Phase != types.InstancePhaseReady {
		return false
	}
	return s.PodCount == 0 || s.ServingPodCount >= s.PodCount
}

func migrationHandoffPromoted(source, target *types.InstanceStatus, targetReferences int) bool {
	return targetReferences == 1 &&
		source.Phase == types.InstancePhaseMigrating && source.RunningRevision != "" && source.TargetRevision == "" &&
		source.Operation != nil && source.Operation.Type == types.InstanceOperationMigrate &&
		source.Operation.RequestUUID != "" && source.Operation.SurgeIndex != nil &&
		*source.Operation.SurgeIndex == target.Index &&
		target.Incarnation == 1 && target.ActiveOrdinal == 0 && target.Phase == types.InstancePhaseReady &&
		target.RunningRevision == source.RunningRevision && target.TargetRevision == "" && target.Operation == nil
}

func updateHandoffPromoted(source, target *types.InstanceStatus, targetReferences int) bool {
	return targetReferences == 1 &&
		source.Phase == types.InstancePhaseUpdating && source.RunningRevision != "" && source.TargetRevision != "" &&
		source.Operation != nil && source.Operation.Type == types.InstanceOperationUpdate &&
		source.Operation.Step == types.UpdateStepSurgeDrain && source.Operation.SurgeIndex != nil &&
		*source.Operation.SurgeIndex == target.Index && source.Operation.TargetRevision == source.TargetRevision &&
		target.Incarnation == 1 && target.ActiveOrdinal == 0 && target.Phase == types.InstancePhaseReady &&
		target.RunningRevision == source.TargetRevision && target.TargetRevision == "" && target.Operation == nil
}

// migrationInFlightIndices returns the indices of InstanceStatuses
// currently participating in a migration — either Phase=Migrating
// (the source) or Operation.Type=Migrate (the surge). These indices
// stay in the plan past scale-down boundaries because deleting them
// mid-migration would abandon a partially-rolled-out surge.
func migrationInFlightIndices(instances []types.InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	for i := range instances {
		if migrationPinned(&instances[i]) {
			out[instances[i].Index] = struct{}{}
		}
	}
	return out
}

// migrationPinned reports whether a migration record still references this
// row: the source by its phase, the surge by the pin on its operation.
//
// This is not the row's owner. A pin is a durable reference that outlives
// the attempt it was made for — a Failed row keeps it, and an index the
// record still names must stay in the plan so the retirement does not
// abandon half a pair — while ownership ends when the attempt does.
func migrationPinned(s *types.InstanceStatus) bool {
	if s == nil {
		return false
	}
	return s.Phase == types.InstancePhaseMigrating ||
		(s.Operation != nil && s.Operation.Type == types.InstanceOperationMigrate)
}

// migrationSourceIndices returns the indices of source-side migration
// participants — InstanceStatuses with Phase=Migrating. Surge-side
// participants (Phase=Creating + Operation.Migrate) are not included
// because they are the +1 over MinReplicas, not the user-facing
// replica being relocated.
func migrationSourceIndices(instances []types.InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	for i := range instances {
		if instances[i].Phase == types.InstancePhaseMigrating {
			out[instances[i].Index] = struct{}{}
		}
	}
	return out
}

// updateSurgeInFlightIndices returns the indices participating in a
// multi-pod (gang) SurgeThenDrain update — both the source (Op.Type=
// Update with a SurgeIndex set) and the status at the referenced target
// index. Like migration surge pairs, these stay in the plan past the replica
// cap so neither side is scale-down-deleted before the operation resolves.
//
// The gang source carries Op.Step=Surge so CurrentSurgeInFlight counts
// it against MaxSurge; the SurgeIndex pointer is what distinguishes a
// gang surge (new index) from a single-pod surge (ActiveOrdinal toggle,
// SurgeIndex nil).
//
// A target is kept only while a live source references it. This includes
// an occupied target discovered during recovery: the source and occupant
// remain intact until gangSurgeUpdate can reset the claim. An unreferenced
// surge-target row is left for the scale-down pipeline to reap. A source
// in unpinned (failedGangSurgeSources) keeps nothing, itself included: the
// failed pair competes for the steady budget as one unit
// (instancePlanIndices).
func updateSurgeInFlightIndices(instances []types.InstanceStatus, unpinned map[int32]struct{}) map[int32]struct{} {
	out := map[int32]struct{}{}
	referenced := map[int32]struct{}{}
	for i := range instances {
		s := &instances[i]
		if s.Operation == nil || s.Operation.Type != types.InstanceOperationUpdate {
			continue
		}
		if _, failed := unpinned[s.Index]; failed {
			continue
		}
		if s.Operation.SurgeIndex != nil { // live source
			out[s.Index] = struct{}{}
			referenced[*s.Operation.SurgeIndex] = struct{}{}
		}
	}
	for i := range instances {
		if _, live := referenced[instances[i].Index]; live {
			out[instances[i].Index] = struct{}{}
		}
	}
	return out
}

// updateSurgeSourceIndices returns the source-side indices of a live gang
// surge (Op.Type=Update with SurgeIndex set, not in unpinned, the failed
// sources). These count toward the steady replica budget — the source IS
// the user-facing replica being rolled; the surge target is the transient
// +1.
func updateSurgeSourceIndices(instances []types.InstanceStatus, unpinned map[int32]struct{}) map[int32]struct{} {
	out := map[int32]struct{}{}
	for i := range instances {
		s := &instances[i]
		if _, failed := unpinned[s.Index]; failed {
			continue
		}
		if s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate && s.Operation.SurgeIndex != nil {
			out[s.Index] = struct{}{}
		}
	}
	return out
}

// failedGangSurgeSources is the set of gang surge sources whose attempt
// has failed and whose claim keeps nothing: Phase=Failed with the surge
// operation preserved, the shape the escalation leaves for the gang
// abandon, naming an index that holds the replacement's surge-target row
// or nothing at all. A failed source whose index is occupied by any other
// row keeps its claim: source and occupant stay intact until
// gangSurgeUpdate resets the claim. position is where each index's row
// sits (rowPositions). Nil when no source has failed.
func failedGangSurgeSources(instances []types.InstanceStatus, position map[int32]int) map[int32]struct{} {
	var out map[int32]struct{}
	for i := range instances {
		s := &instances[i]
		if s.Phase != types.InstancePhaseFailed || s.Operation == nil ||
			s.Operation.Type != types.InstanceOperationUpdate || s.Operation.SurgeIndex == nil {
			continue
		}
		if at, found := position[*s.Operation.SurgeIndex]; found && !gangSurgeTargetMarker(&instances[at]) {
			continue
		}
		if out == nil {
			out = map[int32]struct{}{}
		}
		out[s.Index] = struct{}{}
	}
	return out
}

// gangSurgeTargetMarker reports whether the row is the marker a gang
// surge claims its replacement's index with, live or in cleanup.
func gangSurgeTargetMarker(s *types.InstanceStatus) bool {
	return s != nil && s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate &&
		(s.Operation.Step == types.UpdateStepGangSurgeTarget || s.Operation.Step == types.UpdateStepGangSurgeTargetCleanup)
}

// failedGangSurgeFollowers pairs each source in unpinned (the failed gang
// surge sources) with the surge-target row it still references: targetOf
// by source index, sourceOf by the replacement's index. A source whose
// replacement's index no row records (position, rowPositions) has no
// follower; a replacement two failed sources claim follows the first.
func failedGangSurgeFollowers(instances []types.InstanceStatus, unpinned map[int32]struct{}, position map[int32]int) (targetOf, sourceOf map[int32]int32) {
	targetOf, sourceOf = map[int32]int32{}, map[int32]int32{}
	for i := range instances {
		s := &instances[i]
		if _, failed := unpinned[s.Index]; !failed {
			continue
		}
		target := *s.Operation.SurgeIndex
		if _, found := position[target]; !found {
			continue
		}
		if _, taken := sourceOf[target]; taken {
			continue
		}
		targetOf[s.Index] = target
		sourceOf[target] = s.Index
	}
	return targetOf, sourceOf
}

// lowestUnusedIndex returns the smallest int32 at or above from that is
// not present in used. Pigeonhole-bounded: of the len(used)+1 indexes
// that start at from, at least one is unused.
func lowestUnusedIndex(used map[int32]struct{}, from int32) int32 {
	last := from + int32(len(used))
	for i := from; i <= last; i++ {
		if _, taken := used[i]; !taken {
			return i
		}
	}
	return last // unreachable; keep function total.
}

// incarnationForIndex returns the current Incarnation for idx, or 1
// when no matching entry exists yet (first reconcile or after delete).
func incarnationForIndex(observed *types.WorkloadObservedState, idx int32) int64 {
	if row := observed.Instance(idx); row != nil && row.Incarnation > 0 {
		return row.Incarnation
	}
	return 1
}
