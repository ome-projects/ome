package workload

import (
	"sort"
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

	workerSize := workerSizeFromRunners(desired.Runners)

	// Compute the plan's Instance index set. Existing InstanceStatus
	// entries — including any migration-surge index above the replica
	// count — are preserved so the reconcile loop keeps driving them.
	// The plan grows beyond replicas during the surge phase of a
	// migration; scale-down logic is responsible for picking the right
	// deletion target after that surge resolves.
	indices := instancePlanIndices(observed.InstanceStatuses, replicas, observed.RetryBlocks)
	instances := make([]types.InstancePlan, len(indices))
	runners := runnersForInstance(desired.MultiPod, workerSize)
	for i, idx := range indices {
		instances[i] = types.InstancePlan{
			Index:       idx,
			Incarnation: incarnationForIndex(observed.InstanceStatuses, idx),
			Runners:     append([]types.RunnerPlan(nil), runners...),
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
	}, nil
}

// workerSizeFromRunners returns the "worker" Runner size or 0 when no
// worker Runner is present (single-pod Component).
func workerSizeFromRunners(runners []types.Runner) int32 {
	for _, r := range runners {
		if r.Name == types.RunnerWorker {
			return r.Size
		}
	}
	return 0
}

// runnersForInstance returns the Runner layout for one Instance based
// on the Component's multi-pod flag and worker size. The "worker"
// runner is emitted even when workerSize=0 so the layout shape is
// consistent (downstream code keys on Runner.Name). Admission rejects
// Worker.Size <= 0 at the webhook so workerSize=0 multi-pod requests
// never reach this function in production.
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
// drive: live surge pairs (unbounded by replicas), then the existing
// steady indices up to the replica cap — in rank order (scaleDownRank),
// oldest first within a rank — excluding sources whose replacements are
// proven promoted, then new indices to round out scale-up. blocks is the
// Component's retry ladder, which the rank reads a row's revision by.
//
// The replica cap counts only non-migration indices. Counting the
// surge against it would drop a healthy non-migrating sibling out of
// the plan and into scale-down.
//
// A gang surge whose source has failed holds no pin: the pair competes
// for the steady budget behind the Ready rows, the source charged like
// any steady row and its marker following the source in or out, so the
// two leave the plan together and the scale-down retires them as one
// unit instead of taking a healthy sibling.
func instancePlanIndices(instances []types.InstanceStatus, replicas int32, blocks []types.RetryBlock) []int32 {
	used := existingInstanceIndices(instances)
	// "Protected" / "source" sets cover BOTH migration surge pairs and
	// multi-pod (gang) update-surge pairs — the two cases that transiently
	// hold an extra index over the replica cap. Union the gang-surge sets
	// in so the Pass 1/2/3 algorithm below treats them identically (pin
	// the pair; count only the source toward the steady budget).
	migrationProtected := migrationInFlightIndices(instances)
	for idx := range updateSurgeInFlightIndices(instances) {
		migrationProtected[idx] = struct{}{}
	}
	migrationSource := migrationSourceIndices(instances)
	for idx := range updateSurgeSourceIndices(instances) {
		migrationSource[idx] = struct{}{}
	}
	unpinnedSources := failedGangSurgeSources(instances)
	markerOf, sourceOf := failedGangSurgeFollowers(instances)
	// Every claim reserves its target for uniqueness; the handoff predicates
	// separately decide whether a source has enough proof to retire. A
	// failed source's claim reserves without pinning.
	handoffTargetReferences := map[int32]int{}
	for _, s := range instances {
		if s.Operation == nil || s.Operation.SurgeIndex == nil {
			continue
		}
		targetIndex := *s.Operation.SurgeIndex
		switch s.Operation.Type {
		case types.InstanceOperationMigrate, types.InstanceOperationUpdate:
			handoffTargetReferences[targetIndex]++
			if _, unpinned := unpinnedSources[s.Index]; !unpinned {
				migrationProtected[targetIndex] = struct{}{}
			}
		}
	}

	// The steady rows are kept in rank order; a scale-down removes the
	// lowest-ranked rows first.
	rankByIndex := map[int32]int{}
	statusByIndex := map[int32]types.InstanceStatus{}
	retiringSources := map[int32]struct{}{}
	for _, s := range instances {
		statusByIndex[s.Index] = s
		rankByIndex[s.Index] = scaleDownRank(s, blocks)
	}

	// A handoff source retires only after its replacement is Ready. Gang updates
	// additionally require the drain step and the operation's pinned revision;
	// a fresh claim that collides with an occupied index stays protected.
	for _, s := range instances {
		if s.Operation == nil || s.Operation.SurgeIndex == nil {
			continue
		}
		targetIndex := *s.Operation.SurgeIndex
		if s.Operation.Type != types.InstanceOperationUpdate {
			target, found := statusByIndex[targetIndex]
			if found && migrationHandoffPromoted(s, target, handoffTargetReferences[targetIndex]) {
				delete(migrationProtected, s.Index)
				delete(migrationProtected, targetIndex)
				delete(migrationSource, s.Index)
				retiringSources[s.Index] = struct{}{}
			}
			continue
		}
		target, found := statusByIndex[targetIndex]
		if !found || !updateHandoffPromoted(s, target, handoffTargetReferences[targetIndex]) {
			continue
		}
		delete(migrationProtected, s.Index)
		delete(migrationProtected, targetIndex)
		delete(migrationSource, s.Index)
		retiringSources[s.Index] = struct{}{}
	}

	sorted := make([]int32, 0, len(used))
	for idx := range used {
		sorted = append(sorted, idx)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	indices := make([]int32, 0, len(sorted))
	picked := map[int32]struct{}{}

	// Pass 1: pin migration-in-flight indices (no cap on count; both
	// source AND surge get pinned). Only the source counts toward the
	// steady replica budget — it IS the user-facing replica being
	// relocated; the surge is the transient +1 and does not.
	steadyCount := int32(0)
	for _, idx := range sorted {
		if _, protected := migrationProtected[idx]; !protected {
			continue
		}
		indices = append(indices, idx)
		picked[idx] = struct{}{}
		if _, isSource := migrationSource[idx]; isSource {
			steadyCount++
		}
	}

	// Pass 2: fill the steady replica budget in rank order, keeping the
	// oldest eligible index within each rank.
	fill := func(rank int) {
		for _, idx := range sorted {
			if steadyCount >= replicas {
				return
			}
			if _, already := picked[idx]; already {
				continue
			}
			// A source with a proven promoted replacement is terminal cleanup,
			// not a fallback for an unrelated Instance awaiting status promotion.
			if _, retiring := retiringSources[idx]; retiring {
				continue
			}
			// A failed gang surge's marker is its source's to bring along.
			if _, follower := sourceOf[idx]; follower {
				continue
			}
			if rankByIndex[idx] != rank {
				continue
			}
			indices = append(indices, idx)
			picked[idx] = struct{}{}
			steadyCount++
			if marker, paired := markerOf[idx]; paired {
				if _, already := picked[marker]; !already {
					indices = append(indices, marker)
					picked[marker] = struct{}{}
				}
			}
		}
	}
	for rank := 0; rank <= lowestScaleDownRank; rank++ {
		fill(rank)
	}

	// Pass 3: allocate new slots for scale-up.
	taken := make(map[int32]struct{}, len(used)+len(indices))
	for k, v := range used {
		taken[k] = v
	}
	for _, idx := range indices {
		taken[idx] = struct{}{}
	}
	for steadyCount < replicas {
		next := lowestUnusedIndex(taken)
		indices = append(indices, next)
		taken[next] = struct{}{}
		steadyCount++
	}

	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	return indices
}

// scaleDownRank orders the steady rows a scale-down keeps, lowest kept
// first: a serving row on a sound revision, any other row on a sound
// revision, a serving row on a failing revision, then the rest. A row on
// a failing revision ranks below every row on a sound one: its Ready is
// the pause between crashes of a revision the ladder reads as failed,
// not capacity the Component can keep.
func scaleDownRank(s types.InstanceStatus, blocks []types.RetryBlock) int {
	rank := 0
	if !keepsPreference(s) {
		rank++
	}
	if rowOnFailingRevision(s, blocks) {
		rank += 2
	}
	return rank
}

// lowestScaleDownRank is the last rank scaleDownRank assigns.
const lowestScaleDownRank = 3

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

// lowestUnusedIndex returns the smallest non-negative int32 not
// present in used. Pigeonhole-bounded: for any set of size N at least
// one of {0,..,N} must be unused.
func lowestUnusedIndex(used map[int32]struct{}) int32 {
	for i := int32(0); i <= int32(len(used)); i++ {
		if _, taken := used[i]; !taken {
			return i
		}
	}
	return int32(len(used)) // unreachable; keep function total.
}

// incarnationForIndex returns the current Incarnation for idx, or 1
// when no matching entry exists yet (first reconcile or after delete).
func incarnationForIndex(instances []types.InstanceStatus, idx int32) int64 {
	for _, s := range instances {
		if s.Index == idx && s.Incarnation > 0 {
			return s.Incarnation
		}
	}
	return 1
}
