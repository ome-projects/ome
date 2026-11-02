package workload

import (
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	indices := instancePlanIndices(observed.InstanceStatuses, replicas, observed.RetryBlocks, observed.CordonedInstances)
	// The rows are indexed once here, so each planned Instance finds its
	// row in one read whatever the fleet's size.
	observed.IndexRows()
	instances := make([]types.InstancePlan, len(indices))
	runners := runnersForInstance(desired.MultiPod, workerSize)
	layout := types.ComponentPlan{Runners: runners, WorkerSizeByRevision: observed.WorkerSizeByRevision}
	for i, idx := range indices {
		// An Instance renders the width its own revision recorded, so a gang
		// settled on an older revision keeps its layout until the roll takes it.
		instances[i] = types.InstancePlan{
			Index:       idx,
			Incarnation: incarnationForIndex(&observed, idx),
			Runners:     layout.RunnersFor(types.InstancePlan{Runners: runners}, renderedRevision(&observed, idx)),
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
// attempt pinned; a migration's surge marker renders its source's. Empty
// when no row records one, which renders the roll target.
func renderedRevision(observed *types.WorkloadObservedState, idx int32) string {
	row := observed.Instance(idx)
	if row == nil {
		return ""
	}
	if row.Phase == types.InstancePhaseCreating && row.Operation != nil &&
		row.Operation.Type == types.InstanceOperationMigrate && row.Operation.SurgeIndex != nil {
		if source := observed.Instance(*row.Operation.SurgeIndex); source != nil && source.Index != idx {
			return source.RunningRevision
		}
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
// node, which rank below every other row.
//
// A gang surge whose source has failed keeps nothing by itself: the pair
// competes for the count behind the Ready rows as one unit, the source
// charged like any steady row and its replacement row following it in or
// out, so a scale-down retires the two together instead of taking a
// healthy sibling. The pair counts as cordoned when either half is.
func instancePlanIndices(instances []types.InstanceStatus, replicas int32, blocks []types.RetryBlock, cordoned map[int32]struct{}) []int32 {
	sel := readIndexSelection(instances, replicas, blocks, cordoned)
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
	rows     map[int32]types.InstanceStatus
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

// readIndexSelection reads the rows once: the indexes they record, the
// surge pairs in flight and the rank of every row.
func readIndexSelection(instances []types.InstanceStatus, replicas int32, blocks []types.RetryBlock, cordoned map[int32]struct{}) *indexSelection {
	used := existingInstanceIndices(instances)
	sel := &indexSelection{
		replicas:    replicas,
		existing:    make([]int32, 0, len(used)),
		rows:        make(map[int32]types.InstanceStatus, len(instances)),
		rank:        make(map[int32]int, len(instances)),
		pairMembers: migrationInFlightIndices(instances),
		pairSources: migrationSourceIndices(instances),
		retiring:    map[int32]struct{}{},
		chosen:      make([]int32, 0, len(used)),
		picked:      map[int32]struct{}{},
	}
	for idx := range used {
		sel.existing = append(sel.existing, idx)
	}
	slices.Sort(sel.existing)
	for idx := range updateSurgeInFlightIndices(instances) {
		sel.pairMembers[idx] = struct{}{}
	}
	for idx := range updateSurgeSourceIndices(instances) {
		sel.pairSources[idx] = struct{}{}
	}
	sel.failedTargetOf, sel.failedSourceOf = failedGangSurgeFollowers(instances)
	claims := sel.claimReplacements(instances)
	sel.rankRows(instances, blocks, cordoned)
	sel.releasePromotedSources(instances, claims)
	return sel
}

// claimsReplacement reports whether the row's attempt claims a replacement
// index: a migration or an update with a surge index.
func claimsReplacement(s types.InstanceStatus) bool {
	if s.Operation == nil || s.Operation.SurgeIndex == nil {
		return false
	}
	return s.Operation.Type == types.InstanceOperationMigrate || s.Operation.Type == types.InstanceOperationUpdate
}

// claimReplacements counts the sources that claim each replacement index
// and keeps the replacement of every source that has not failed beside its
// source. Every claim counts, so an index two sources name promotes
// neither; a failed source's claim keeps nothing. Whether a source has
// proof enough to retire is decided after the rows are read
// (releasePromotedSources).
func (sel *indexSelection) claimReplacements(instances []types.InstanceStatus) map[int32]int {
	failed := failedGangSurgeSources(instances)
	claims := map[int32]int{}
	for _, s := range instances {
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

// rankRows records every row by its index and ranks it for a scale-down.
// A failed gang surge pair counts as cordoned when either half has a pod
// on a cordoned node.
func (sel *indexSelection) rankRows(instances []types.InstanceStatus, blocks []types.RetryBlock, cordoned map[int32]struct{}) {
	for _, s := range instances {
		sel.rows[s.Index] = s
		_, onCordonedNode := cordoned[s.Index]
		if target, paired := sel.failedTargetOf[s.Index]; paired {
			if _, targetCordoned := cordoned[target]; targetCordoned {
				onCordonedNode = true
			}
		}
		sel.rank[s.Index] = scaleDownRank(s, blocks, onCordonedNode)
	}
}

// releasePromotedSources frees every pair whose replacement is proven
// promoted: the pair keeps neither half any more, and the source is
// terminal cleanup that fills no slot of the count.
func (sel *indexSelection) releasePromotedSources(instances []types.InstanceStatus, claims map[int32]int) {
	for _, s := range instances {
		if s.Operation == nil || s.Operation.SurgeIndex == nil {
			continue
		}
		target := *s.Operation.SurgeIndex
		row, found := sel.rows[target]
		if !found || !replacementPromoted(s, row, claims[target]) {
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
func replacementPromoted(source, target types.InstanceStatus, claims int) bool {
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
// pass over the indexes in all.
func (sel *indexSelection) addNewIndexes() {
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
// first. A row left Creating with no operation ranks below every other
// row: nothing drives it and it has never served, so keeping it would
// retire an Instance whose pod still runs. Among the rest, a row with a
// pod on a cordoned node ranks below every row without one: its node is
// being emptied, so keeping it means moving it later, while dropping it
// frees the node now. Within each half: a serving row on a sound
// revision, any other row on a sound revision, a serving row on a
// failing revision, then the rest. A row on a failing revision ranks
// below every row on a sound one: its Ready is the pause between crashes
// of a revision the ladder reads as failed, not capacity the Component
// can keep.
func scaleDownRank(s types.InstanceStatus, blocks []types.RetryBlock, onCordonedNode bool) int {
	if creatingWithNoOperation(s) {
		return lowestScaleDownRank
	}
	rank := 0
	if !keepsPreference(s) {
		rank++
	}
	if rowOnFailingRevision(s, blocks) {
		rank += 2
	}
	if onCordonedNode {
		rank += 4
	}
	return rank
}

// lowestScaleDownRank is the last rank scaleDownRank assigns, the one a
// row nothing drives takes.
const lowestScaleDownRank = 8

// creatingWithNoOperation reports a row left in phase Creating with no
// operation: a move's replacement the record's expiry released, or a
// status write applied in part. No pass owns the shape and no deadline
// runs on it; it fills the count only when no other row can, and a
// scale-down retires it first.
func creatingWithNoOperation(s types.InstanceStatus) bool {
	return s.Phase == types.InstancePhaseCreating && s.Operation == nil
}

// keepsPreference reports whether a row is a stable replica the plan keeps
// ahead of the rest: Ready, with every pod its published counts hold in
// rotation. A Ready row that serves fewer pods than it has serves nothing
// the Component can count on, whatever its phase says. A row with no
// published pods is read by its phase alone.
func keepsPreference(s types.InstanceStatus) bool {
	if s.Phase != types.InstancePhaseReady {
		return false
	}
	return s.PodCount == 0 || s.ServingPodCount >= s.PodCount
}

// rowOnFailingRevision reports whether the row stands on a revision the
// retry ladder reads as failing: the revision its attempt is pinned to,
// else the one it runs, carries a Held or Backoff RetryBlock — a failed
// attempt no later attempt has answered — or an attempt in flight that
// answers a crash this row remembers of its promoted set
// (types.RemembersCrash). A revision with no block is sound whatever the
// row's failure record says: that record also names expired migrations
// and overdue drains, which say nothing about the revision.
func rowOnFailingRevision(s types.InstanceStatus, blocks []types.RetryBlock) bool {
	rev := s.TargetRevision
	if rev == "" {
		rev = s.RunningRevision
	}
	if rev == "" {
		return false
	}
	b := types.FindRetryBlock(blocks, rev)
	if b == nil {
		return false
	}
	switch b.State {
	case types.RetryBlockHeld, types.RetryBlockBackoff:
		return true
	case types.RetryBlockRetryInProgress:
		return types.RemembersCrash(&s)
	}
	return false
}

func migrationHandoffPromoted(source, target types.InstanceStatus, targetReferences int) bool {
	return targetReferences == 1 &&
		source.Phase == types.InstancePhaseMigrating && source.RunningRevision != "" && source.TargetRevision == "" &&
		source.Operation != nil && source.Operation.Type == types.InstanceOperationMigrate &&
		source.Operation.RequestUUID != "" && source.Operation.SurgeIndex != nil &&
		*source.Operation.SurgeIndex == target.Index &&
		target.Incarnation == 1 && target.ActiveOrdinal == 0 && target.Phase == types.InstancePhaseReady &&
		target.RunningRevision == source.RunningRevision && target.TargetRevision == "" && target.Operation == nil
}

func updateHandoffPromoted(source, target types.InstanceStatus, targetReferences int) bool {
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
	for _, s := range instances {
		if s.Phase == types.InstancePhaseMigrating {
			out[s.Index] = struct{}{}
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
// A target is pinned only while a live source references it. This includes
// an occupied target discovered during recovery: the source and occupant
// remain intact until gangSurgeUpdate can reset the claim. An unreferenced
// marker is left for the scale-down pipeline to reap. A source that has
// failed pins nothing, itself included: the failed pair competes for the
// steady budget as one unit (instancePlanIndices).
func updateSurgeInFlightIndices(instances []types.InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	referenced := map[int32]struct{}{}
	unpinned := failedGangSurgeSources(instances)
	for _, s := range instances {
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
	for _, s := range instances {
		if _, live := referenced[s.Index]; live {
			out[s.Index] = struct{}{}
		}
	}
	return out
}

// updateSurgeSourceIndices returns the source-side indices of a live gang
// surge (Op.Type=Update with SurgeIndex set, not Failed). These count
// toward the steady replica budget — the source IS the user-facing
// replica being rolled; the surge target is the transient +1.
func updateSurgeSourceIndices(instances []types.InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	unpinned := failedGangSurgeSources(instances)
	for _, s := range instances {
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
// has failed and whose claim pins nothing: Phase=Failed with the surge
// operation preserved, the shape the escalation leaves for the gang
// abandon, naming an index that holds the replacement's marker or
// nothing at all. A failed source whose index is occupied by any other
// row keeps its pin: source and occupant stay intact until
// gangSurgeUpdate resets the claim.
func failedGangSurgeSources(instances []types.InstanceStatus) map[int32]struct{} {
	byIndex := make(map[int32]*types.InstanceStatus, len(instances))
	for i := range instances {
		byIndex[instances[i].Index] = &instances[i]
	}
	out := map[int32]struct{}{}
	for i := range instances {
		s := &instances[i]
		if s.Phase != types.InstancePhaseFailed || s.Operation == nil ||
			s.Operation.Type != types.InstanceOperationUpdate || s.Operation.SurgeIndex == nil {
			continue
		}
		if occupant, found := byIndex[*s.Operation.SurgeIndex]; found && !gangSurgeTargetMarker(occupant) {
			continue
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

// failedGangSurgeFollowers pairs each unpinned failed gang surge source
// with the marker it still references: markerOf by source index,
// sourceOf by marker index. A source whose marker is already gone has no
// follower; a marker claimed by two failed sources follows the first.
func failedGangSurgeFollowers(instances []types.InstanceStatus) (markerOf, sourceOf map[int32]int32) {
	markerOf, sourceOf = map[int32]int32{}, map[int32]int32{}
	unpinned := failedGangSurgeSources(instances)
	present := existingInstanceIndices(instances)
	for i := range instances {
		s := &instances[i]
		if _, failed := unpinned[s.Index]; !failed {
			continue
		}
		marker := *s.Operation.SurgeIndex
		if _, found := present[marker]; !found {
			continue
		}
		if _, taken := sourceOf[marker]; taken {
			continue
		}
		markerOf[s.Index] = marker
		sourceOf[marker] = s.Index
	}
	return markerOf, sourceOf
}

// existingInstanceIndices returns the set of recorded indices.
func existingInstanceIndices(instances []types.InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	for _, s := range instances {
		out[s.Index] = struct{}{}
	}
	return out
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
