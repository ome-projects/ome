package defrag

import (
	"math"
	"sort"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// PolicyName is the metric/event label value for Policy #1.
const PolicyName = "defragmentation"

// costOMENativeSurge is the disruption cost of Alpha's only executable
// defragmentation shape: OMENative place-then-free surge.
const costOMENativeSurge = 0.15

// emergencyBoostFactor multiplies a positive Score when the move unblocks an
// over-age pending pod — what lets a starving workload jump the queue ahead
// of routine consolidation.
const emergencyBoostFactor = 2.0

// spotSourceBoostFactor multiplies a positive Score when the source node is
// preemptible and spot policy prefers evacuating such nodes before a
// preemption event does.
const spotSourceBoostFactor = 1.25

// costWeight realizes the aggressiveness scoring knob: conservative demands
// more benefit per unit of disruption, aggressive tolerates more. Never a
// safety bypass — cooldowns, caps, and the breaker live in the Arbiter.
func costWeight(aggressiveness string) float64 {
	switch aggressiveness {
	case config.AggressivenessConservative:
		return 1.5
	case config.AggressivenessAggressive:
		return 0.5
	default:
		return 1.0
	}
}

// Policy is Policy #1 (Defragmentation): pure candidate selection over the
// snapshot. It holds no client; the engine routes its output.
type Policy struct{}

var _ policy.Policy = &Policy{}

// Name implements policy.Policy.
func (*Policy) Name() string { return PolicyName }

// evalCtx carries the per-pool evaluation state shared by every candidate.
type evalCtx struct {
	snap       *snapshot.ClusterSnapshot
	cfg        *config.Config
	pool       string
	bins       []binState
	ladder     []int64
	weights    map[int64]float64
	totalFree  int64
	before     float64
	pendings   []snapshot.PendingPod
	costWeight float64
	// executionOpen is the exact reclaimable/pending-pressure gate. A pool
	// reached only through observed fragmentation may emit advisories but
	// must not rank targets, simulate, or create capacity claims.
	executionOpen bool
}

// Evaluate turns the snapshot into a ranked []Candidate (OEP-0008
// §Candidate selection). Gate → enumerate → classify → simulate → score →
// boost → rank → filter.
func (*Policy) Evaluate(snap *snapshot.ClusterSnapshot, cfg *config.Config) []policy.Candidate {
	d := &cfg.Policies.Defragmentation
	if !*d.Enabled {
		return nil
	}
	scores := ComputeScores(snap, cfg)
	threshold := *d.FragmentationThreshold

	ladder := int64Ladder(d.Scoring.SizeLadder)
	prior := parsePrior(d.Scoring.SizePrior)
	lambda := *d.Scoring.DemandBlendLambda
	demand := demandByPoolAndSize(snap, ladder)
	weight := costWeight(d.Aggressiveness)
	now := snap.Timestamp

	var out []policy.Candidate
	for _, pool := range snap.GPUPools() {
		cs := scores.PerPool[pool]
		if cs == nil {
			continue
		}
		executionOpen := cs.Score > threshold
		advisoryOpen := math.Max(0, cs.FObserved-cs.FReclaimable) > threshold
		if !executionOpen && !advisoryOpen {
			continue
		}
		ctx := &evalCtx{
			snap:          snap,
			cfg:           cfg,
			pool:          pool,
			bins:          schedulableBins(snap, cfg, pool),
			ladder:        ladder,
			weights:       demandWeights(ladder, demand[pool], prior, lambda),
			totalFree:     cs.TotalFree,
			pendings:      poolPendings(snap, pool),
			costWeight:    weight,
			executionOpen: executionOpen,
		}
		ctx.before = weightedFrag(ctx.bins, ladder, ctx.weights, ctx.totalFree)

		for _, w := range sortedWorkloads(snap) {
			for _, comp := range sortedComponents(w) {
				out = append(out, evaluateComponent(ctx, w, comp)...)
			}
		}
	}

	// Maintenance windows gate defragmentation dispatch: outside every
	// window, executable candidates are dropped; advisories carry no
	// dispatch and survive, and an emergency — a pod starved past
	// emergencyPendingAgeMinutes — overrides the window (a steady-state
	// optimization can wait; a starving workload cannot). Node-health
	// evacuation ignores windows entirely in its own policy.
	if !maintenanceOpen(cfg, now) {
		kept := out[:0]
		for _, c := range out {
			if !c.Executable || c.Emergency {
				kept = append(kept, c)
			}
		}
		out = kept
	}

	rankCandidates(out)
	return out
}

// evaluateComponent classifies one component by deployment mode (the
// classification sets Executable, it does not drop the candidate) and
// evaluates its instances.
func evaluateComponent(ctx *evalCtx, w *snapshot.Workload, comp *snapshot.Component) []policy.Candidate {
	switch comp.DeploymentMode {
	case constants.MultiNode:
		// LWS-backed groups are unsafe to migrate automatically
		// (RecreateGroupOnPodRestart tears the group down with no surge
		// protection) but the opportunity must still surface.
		if !w.Movable || len(w.ActiveMigrations) > 0 || inCooldown(w, ctx.cfg, ctx.snap.Timestamp) ||
			!*ctx.cfg.LWSRecommendationsEnabled {
			return nil
		}
		if componentPrimaryPool(ctx.snap, comp) != ctx.pool {
			return nil
		}
		return []policy.Candidate{advisory(w, comp, policy.ComponentWideInstance,
			policy.AdvisoryLWSMigrationUnsupported, componentPrimaryNode(comp), 0)}
	case constants.RawDeployment, constants.OMENative:
	default:
		return nil
	}
	if comp.DeploymentMode == constants.RawDeployment {
		if !w.Movable || len(w.ActiveMigrations) > 0 || inCooldown(w, ctx.cfg, ctx.snap.Timestamp) {
			return nil
		}
		return rawAdvisories(ctx, w, comp)
	}

	// Component-wide downgrades, most fundamental first; one advisory, not
	// a stack.
	from := componentPrimaryNode(comp)
	if reason := modelMovabilityReason(ctx.snap, w); reason != "" {
		if componentPrimaryPool(ctx.snap, comp) != ctx.pool {
			return nil
		}
		return []policy.Candidate{advisory(w, comp, policy.ComponentWideInstance,
			reason, from, 0)}
	}
	instances := append([]*snapshot.Instance(nil), comp.Instances...)
	sort.Slice(instances, func(i, j int) bool { return instances[i].Index < instances[j].Index })

	var out []policy.Candidate
	for _, inst := range instances {
		if inst.TotalGPUs == 0 || !instanceInPool(ctx.snap, inst, ctx.pool) {
			continue
		}
		if !*ctx.cfg.OMENativeMigrationEnabled {
			out = append(out, advisory(w, comp, inst.Index,
				policy.AdvisoryMigrationSurfaceDisabled, primaryNode(inst), 0))
			continue
		}
		if reason := omenativeExecutionEligibility(ctx.snap, ctx.cfg, w, comp, inst, ctx.snap.Timestamp); reason != "" {
			out = append(out, advisory(w, comp, inst.Index, reason, primaryNode(inst), 0))
			continue
		}
		if !ctx.executionOpen {
			out = append(out, advisory(w, comp, inst.Index,
				policy.AdvisoryNonExecutableObservedFragmentation, primaryNode(inst), 0))
			continue
		}
		if c, ok := evaluateInstance(ctx, w, comp, inst); ok {
			out = append(out, c)
		}
	}
	return out
}

func rawAdvisories(ctx *evalCtx, w *snapshot.Workload, comp *snapshot.Component) []policy.Candidate {
	instances := append([]*snapshot.Instance(nil), comp.Instances...)
	sort.Slice(instances, func(i, j int) bool { return instances[i].Index < instances[j].Index })
	out := make([]policy.Candidate, 0, len(instances))
	for _, inst := range instances {
		if inst.TotalGPUs == 0 || hasTerminating(inst) || !instanceInPool(ctx.snap, inst, ctx.pool) {
			continue
		}
		out = append(out, advisory(w, comp, inst.Index,
			policy.AdvisoryRawDeploymentMigrationUnsupported, primaryNode(inst), 0))
	}
	return out
}

// evaluateInstance simulates Alpha's sole executable defragmentation shape
// (OMENative place-then-free surge), scores it benefit-minus-cost, and
// applies the emergency and spot-source boosts.
func evaluateInstance(ctx *evalCtx, w *snapshot.Workload, comp *snapshot.Component,
	inst *snapshot.Instance) (policy.Candidate, bool) {

	prints := instanceFootprints(inst)
	if len(prints) == 0 {
		return policy.Candidate{}, false
	}
	from := primaryNode(inst)

	exclude := make(map[string]bool, len(inst.NodesSet))
	for node := range inst.NodesSet {
		exclude[node] = true
	}
	// Rank targets that fit the SMALLEST member pod: a multi-pod
	// instance's pods land on different targets, and excluding nodes that
	// only fit the smaller pods would fabricate NoSurgeHeadroom.
	// placeThenFree re-checks each footprint's size per target; for
	// single-pod instances min and max coincide.
	minPod := prints[len(prints)-1].gpus
	ranked := rankTargets(ctx.snap, ctx.cfg, ctx.bins, w, minPod, exclude)

	after, _, ok := placeThenFree(ctx.bins, prints, ranked)
	if !ok {
		// Not dispatchable: without surge headroom the migration would
		// stall in SurgePending until timeout.
		c := advisory(w, comp, inst.Index, policy.AdvisoryNoSurgeHeadroom, from, inst.TotalGPUs)
		c.SurgeShaped = true
		return c, true
	}

	benefit := ctx.before - weightedFrag(after, ctx.ladder, ctx.weights, ctx.totalFree)
	cost := costOMENativeSurge
	score := benefit - ctx.costWeight*cost
	if score <= 0 {
		c := advisory(w, comp, inst.Index,
			policy.AdvisoryNonExecutableObservedFragmentation, from, 0)
		c.SurgeShaped = true
		c.Benefit = benefit
		c.Cost = cost
		c.Score = score
		return c, true
	}

	emergency := unblocksOverAgePending(ctx, w, after)
	if emergency {
		score *= emergencyBoostFactor
	}
	if spotPrefersSource(w, ctx.cfg) {
		if node := ctx.snap.Nodes[from]; node != nil && node.Preemptible {
			score *= spotSourceBoostFactor
		}
	}

	// Reporting retains the historical bounded ranked hints, while arbitration
	// replays the complete ranked target set used by the successful simulation.
	// The latter includes every proof node plus feasible alternates if an earlier
	// same-cycle admission consumes one of the preferred targets.
	placementTargets := deduplicateTargets(ranked)
	hints := placementTargets
	if len(hints) > maxHintTargets {
		hints = hints[:maxHintTargets]
	}
	return policy.Candidate{
		Policy:               PolicyName,
		Workload:             w.NamespacedName,
		Component:            comp.Type,
		Instance:             inst.Index,
		Mode:                 comp.DeploymentMode,
		Reason:               policy.ReasonFragmentation,
		FromNode:             from,
		HintTargetNodes:      append([]string(nil), hints...),
		PlacementTargetNodes: append([]string(nil), placementTargets...),
		Executable:           true,
		SurgeShaped:          true,
		FootprintGPUs:        inst.TotalGPUs,
		Benefit:              benefit,
		Cost:                 cost,
		Score:                score,
		Emergency:            emergency,
	}, true
}

func deduplicateTargets(targets []string) []string {
	seen := make(map[string]struct{}, len(targets))
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	return out
}

// omenativeExecutionEligibility is the single fail-closed Alpha execution
// baseline shared by candidate classification and executable repacking. It
// returns the first stable advisory reason in deterministic check order; an
// empty reason is the only executable result.
func omenativeExecutionEligibility(snap *snapshot.ClusterSnapshot, cfg *config.Config,
	w *snapshot.Workload, comp *snapshot.Component, inst *snapshot.Instance, now time.Time) string {

	if !w.Movable || !w.MigrationStateValid || len(w.MalformedRequests) > 0 ||
		len(w.ActiveMigrations) > 0 || inCooldown(w, cfg, now) {
		return policy.AdvisoryOMENativeStateIneligible
	}
	if comp.IR == nil || !comp.StatusFresh || !comp.ObservationValid {
		return policy.AdvisoryOMENativeObservationInvalid
	}
	ir := comp.IR
	if ir.Spec.Paused {
		return policy.AdvisoryOMENativeStateIneligible
	}
	if ir.Spec.Lifecycle != nil && ir.Spec.Lifecycle.MigrationPolicy != nil &&
		ir.Spec.Lifecycle.MigrationPolicy.Mode == v1beta1.MigrationPolicyModeNever {
		return policy.AdvisoryOMENativeStateIneligible
	}
	desired := int32(1)
	if ir.Spec.Replicas != nil {
		desired = *ir.Spec.Replicas
	}
	status := &ir.Status
	if desired <= 0 || int32(len(comp.Instances)) != desired || status.Replicas != desired || status.ReadyReplicas != desired ||
		status.ServingReplicas != desired || status.AvailableReplicas != desired ||
		status.UpdatedReplicas != desired || status.UpdatedReadyReplicas != desired {
		return policy.AdvisoryOMENativeStateIneligible
	}
	if status.CurrentRevision == "" || status.UpdateRevision == "" ||
		status.CurrentRevision != status.UpdateRevision {
		return policy.AdvisoryOMENativeStateIneligible
	}
	// Structured capability is the sole authorization source. The temporary
	// OMENativeAvailable compatibility boolean is deliberately ignored.
	if !snap.OMENativeExecutor.Available {
		return policy.AdvisoryOMENativeUnavailable
	}
	if !inst.ObservationValid {
		return policy.AdvisoryOMENativeObservationInvalid
	}
	if !inst.Admitted || inst.Phase != v1beta1.OMENativeInstanceReady || inst.Operation != nil {
		return policy.AdvisoryOMENativeStateIneligible
	}
	if inst.DesiredPods <= 0 || inst.StatusPods != inst.DesiredPods ||
		inst.ObservedPods != inst.DesiredPods || int32(len(inst.Pods)) != inst.DesiredPods ||
		inst.ReadyPods != inst.DesiredPods || inst.ServingPods != inst.DesiredPods ||
		inst.AvailablePods != inst.DesiredPods {
		return policy.AdvisoryOMENativeStateIneligible
	}
	if inst.RunningRevision == "" || inst.TargetRevision == "" ||
		inst.RunningRevision != inst.TargetRevision || inst.RunningRevision != status.CurrentRevision {
		return policy.AdvisoryOMENativeStateIneligible
	}
	for i := range inst.Pods {
		if !inst.Pods[i].Ready || inst.Pods[i].Terminating {
			return policy.AdvisoryOMENativeStateIneligible
		}
	}
	return ""
}

// unblocksOverAgePending reports whether the simulated after-state seats a
// pending pod (real or virtual) that the observed state cannot, aged past
// emergencyPendingAgeMinutes, within the workload's tenant scope.
func unblocksOverAgePending(ctx *evalCtx, w *snapshot.Workload, after []binState) bool {
	minAge := ctx.cfg.EmergencyPendingAge()
	for i := range ctx.pendings {
		p := &ctx.pendings[i]
		if ctx.snap.Timestamp.Sub(p.PendingSince) <= minAge {
			continue
		}
		if !tenantCompatible(ctx.snap, ctx.cfg, w, p) {
			continue
		}
		if !canSeat(ctx.bins, p.GPUsNeeded) && canSeat(after, p.GPUsNeeded) {
			return true
		}
	}
	return false
}

// tenantCompatible enforces the tenant boundary on the emergency boost: a
// move is boosted only by pending demand it may legitimately serve — its own
// namespace, or (when the cluster admin has not vetoed cross-tenant moves) a
// workload sharing its explicit tenant group. Pure consolidation benefit is
// pool-wide and needs no tenant check.
func tenantCompatible(snap *snapshot.ClusterSnapshot, cfg *config.Config, w *snapshot.Workload, p *snapshot.PendingPod) bool {
	if p.Namespace == w.NamespacedName.Namespace {
		return true
	}
	if !*cfg.AllowCrossTenantOptimization || w.TenantGroup == "" {
		return false
	}
	if p.ISVC.Name == "" {
		return false
	}
	owner, ok := snap.Workloads[p.ISVC]
	return ok && owner.TenantGroup == w.TenantGroup
}

// spotPrefersSource resolves the per-workload spot-policy annotation against
// the cluster default for the source-side boost: "migrate" always boosts,
// "ignore" never does, anything else follows spotPolicy.preferAsSource.
func spotPrefersSource(w *snapshot.Workload, cfg *config.Config) bool {
	switch w.SpotPolicy {
	case "migrate":
		return true
	case "ignore":
		return false
	default:
		return *cfg.SpotPolicy.PreferAsSource
	}
}

// inCooldown applies the per-workload cooldown from the newest terminal
// migration, honoring the alfred.ome.io/cooldown-minutes override.
func inCooldown(w *snapshot.Workload, cfg *config.Config, now time.Time) bool {
	if w.LastMigration == nil {
		return false
	}
	cooldown := cfg.PerWorkloadCooldown()
	if w.CooldownOverride != nil {
		cooldown = *w.CooldownOverride
	}
	return now.Sub(*w.LastMigration) < cooldown
}

var weekdayNames = map[time.Weekday]string{
	time.Monday: "Mon", time.Tuesday: "Tue", time.Wednesday: "Wed",
	time.Thursday: "Thu", time.Friday: "Fri", time.Saturday: "Sat", time.Sunday: "Sun",
}

// maintenanceOpen reports whether defragmentation may dispatch now: no
// windows means always, otherwise inside at least one weekly UTC window.
// Window syntax is validated at config load.
func maintenanceOpen(cfg *config.Config, now time.Time) bool {
	if len(cfg.MaintenanceWindows) == 0 {
		return true
	}
	utc := now.UTC()
	day := weekdayNames[utc.Weekday()]
	minute := utc.Hour()*60 + utc.Minute()
	for _, w := range cfg.MaintenanceWindows {
		dayMatch := false
		for _, d := range w.Days {
			if d == day {
				dayMatch = true
				break
			}
		}
		if !dayMatch {
			continue
		}
		start, _ := time.Parse("15:04", w.Start)
		end, _ := time.Parse("15:04", w.End)
		if minute >= start.Hour()*60+start.Minute() && minute < end.Hour()*60+end.Minute() {
			return true
		}
	}
	return false
}

// rankCandidates orders by FinalScore descending, breaking ties toward the
// smaller footprint — smaller moves fit more often, disrupt less, and each
// completion frees GPUs that make larger moves feasible later. Remaining
// ties order deterministically by identity.
func rankCandidates(out []policy.Candidate) {
	sort.SliceStable(out, func(i, j int) bool {
		a, b := &out[i], &out[j]
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		if a.FootprintGPUs != b.FootprintGPUs {
			return a.FootprintGPUs < b.FootprintGPUs
		}
		if a.Workload.String() != b.Workload.String() {
			return a.Workload.String() < b.Workload.String()
		}
		if a.Component != b.Component {
			return a.Component < b.Component
		}
		return a.Instance < b.Instance
	})
}

func advisory(w *snapshot.Workload, comp *snapshot.Component, instance int32, reason, from string, footprint int64) policy.Candidate {
	return policy.Candidate{
		Policy:         PolicyName,
		Workload:       w.NamespacedName,
		Component:      comp.Type,
		Instance:       instance,
		Mode:           comp.DeploymentMode,
		Reason:         policy.ReasonFragmentation,
		FromNode:       from,
		Executable:     false,
		AdvisoryReason: reason,
		FootprintGPUs:  footprint,
	}
}

// --- snapshot traversal helpers (deterministic ordering throughout) ---

func sortedWorkloads(snap *snapshot.ClusterSnapshot) []*snapshot.Workload {
	out := make([]*snapshot.Workload, 0, len(snap.Workloads))
	for _, w := range snap.Workloads {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].NamespacedName.String() < out[j].NamespacedName.String()
	})
	return out
}

func sortedComponents(w *snapshot.Workload) []*snapshot.Component {
	out := make([]*snapshot.Component, 0, len(w.Components))
	for _, comp := range w.Components {
		out = append(out, comp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// modelMovabilityReason is the shared fail-closed model truth for candidate
// classification and executable repacking. An empty reason is the only model
// state that may move.
func modelMovabilityReason(snap *snapshot.ClusterSnapshot, w *snapshot.Workload) string {
	if w.ModelKey.Zero() {
		return ""
	}
	avail, ok := snap.Models[w.ModelKey]
	if !ok || avail.ResolveError != "" {
		return policy.AdvisoryModelUnresolved
	}
	if avail.VolumePinned {
		return policy.AdvisoryVolumePinned
	}
	return ""
}

func hasTerminating(inst *snapshot.Instance) bool {
	for _, pod := range inst.Pods {
		if pod.Terminating {
			return true
		}
	}
	return false
}

// instanceInPool reports whether every member pod sits on a node of the
// pool; instances straddling pools (or on unknown nodes) are skipped.
func instanceInPool(snap *snapshot.ClusterSnapshot, inst *snapshot.Instance, pool string) bool {
	for node := range inst.NodesSet {
		n, ok := snap.Nodes[node]
		if !ok || n.GPUPool != pool {
			return false
		}
	}
	return len(inst.NodesSet) > 0
}

// primaryNode is the node holding the largest share of the instance's GPUs
// (name tie-break) — the FromNode a migration request would carry.
func primaryNode(inst *snapshot.Instance) string {
	perNode := map[string]int64{}
	for _, pod := range inst.Pods {
		perNode[pod.Node] += pod.GPUs
	}
	best, bestGPUs := "", int64(-1)
	for node, gpus := range perNode {
		if gpus > bestGPUs || (gpus == bestGPUs && node < best) {
			best, bestGPUs = node, gpus
		}
	}
	return best
}

// componentPrimaryNode is primaryNode over all of a component's pods.
func componentPrimaryNode(comp *snapshot.Component) string {
	perNode := map[string]int64{}
	for _, inst := range comp.Instances {
		for _, pod := range inst.Pods {
			perNode[pod.Node] += pod.GPUs
		}
	}
	best, bestGPUs := "", int64(-1)
	for node, gpus := range perNode {
		if gpus > bestGPUs || (gpus == bestGPUs && node < best) {
			best, bestGPUs = node, gpus
		}
	}
	return best
}

// componentPrimaryPool attributes a component to the pool holding the
// largest share of its GPUs (lexicographic tie-break), so component-wide
// advisories are emitted exactly once.
func componentPrimaryPool(snap *snapshot.ClusterSnapshot, comp *snapshot.Component) string {
	perPool := map[string]int64{}
	for _, inst := range comp.Instances {
		for _, pod := range inst.Pods {
			node, ok := snap.Nodes[pod.Node]
			if !ok {
				continue
			}
			perPool[node.GPUPool] += pod.GPUs
		}
	}
	best, bestGPUs := "", int64(-1)
	for pool, gpus := range perPool {
		if gpus > bestGPUs || (gpus == bestGPUs && pool < best) {
			best, bestGPUs = pool, gpus
		}
	}
	return best
}

func poolPendings(snap *snapshot.ClusterSnapshot, pool string) []snapshot.PendingPod {
	var out []snapshot.PendingPod
	for _, p := range snap.PendingPods {
		if p.GPUPool == pool || p.GPUPool == "" {
			out = append(out, p)
		}
	}
	return out
}
