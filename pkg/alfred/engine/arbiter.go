package engine

import (
	"sort"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// Rejection reason codes. The Reporter emits them verbatim as the `reason`
// label of alfred_recommendations_rejected_total and in Events.
const (
	RejectCircuitBreakerOpen = "CircuitBreakerOpen"
	RejectInstanceGone       = "InstanceGone"
	RejectNotMovable         = "NotMovable"
	RejectMalformedRequest   = "MalformedRequestPending"
	RejectTerminating        = "InstanceTerminating"
	RejectWorkloadBusy       = "WorkloadBusy"
	RejectAutoscalerActive   = "AutoscalerActive"
	RejectCooldown           = "Cooldown"
	RejectPlacementCooldown  = "PlacementCooldown"
	RejectNodeCooldown       = "NodeCooldown"
	RejectTargetUnderEvac    = "TargetUnderEvacuation"
	RejectTargetNodeBusy     = "TargetNodeBusy"
	RejectNoCapacity         = "NoCapacity"
	RejectInFlightCap        = "InFlightCap"
	RejectHourlyCap          = "HourlyCap"
)

// AutoscalerIntent is the OEP-0013 read-only signal.
type AutoscalerIntent int

const (
	// IntentIdle: the autoscaler is not touching the workload.
	IntentIdle AutoscalerIntent = iota
	// IntentScaling: replicas are actively being added or removed.
	IntentScaling
	// IntentUnknown: the signal exists but could not be read — treated as
	// busy, because acting on a workload that might be mid-scale is the
	// costlier error.
	IntentUnknown
)

// Decision is the Arbiter's verdict on one executable candidate.
type Decision struct {
	Candidate policy.Candidate
	Admitted  bool
	// Reason is the rejection code; empty when admitted.
	Reason string
	// Target is the claimed placement for an admitted candidate: the
	// first feasible hint net of claims, or empty for an eviction
	// expected to reuse its source.
	Target string
	// CooldownOverridden marks a health evacuation admitted under the
	// health floor while the standard per-workload window was still
	// running (audited as CooldownOverriddenForEvacuation).
	CooldownOverridden bool
}

// Arbiter applies the global safety bounds to the merged candidate stream of
// every policy — one budget for Alfred as a whole, enforced once, here.
type Arbiter struct {
	Ledger *Ledger

	// AutoscalerIntent reports whether OEP-0013 is actively scaling a
	// workload. Nil means no autoscaler integration is wired into this
	// build (no CRDs are read yet), which is idle by construction — the
	// treat-unavailable-as-busy default applies to a wired signal that
	// fails, not to an integration that does not exist.
	AutoscalerIntent func(snap *snapshot.ClusterSnapshot, workload string) AutoscalerIntent
}

// Admit arbitrates the executable candidates and returns one Decision per
// candidate, in arbitration order (health before defrag, then score).
// Advisory candidates are not arbitrated — the engine routes them straight
// to the Reporter — and produce no Decision. Admit reconciles the Ledger
// against the snapshot (expired records are pruned, completed migrations
// stop claiming capacity) but records no new state: recording actual
// dispatches is the caller's act, after actuation.
func (a *Arbiter) Admit(snap *snapshot.ClusterSnapshot, cands []policy.Candidate, cfg *config.Config, now time.Time) []Decision {
	ordered := make([]policy.Candidate, 0, len(cands))
	for _, c := range cands {
		if c.Executable {
			ordered = append(ordered, c)
		}
	}
	// Priority: health preempts defrag across the merged stream; within a
	// class the policies' own ranking (score, then smaller footprint)
	// carries over deterministically.
	sort.SliceStable(ordered, func(i, j int) bool {
		hi, hj := ordered[i].Reason == policy.ReasonNodeUnhealthy, ordered[j].Reason == policy.ReasonNodeUnhealthy
		if hi != hj {
			return hi
		}
		if ordered[i].Score != ordered[j].Score {
			return ordered[i].Score > ordered[j].Score
		}
		return ordered[i].FootprintGPUs < ordered[j].FootprintGPUs
	})

	st := newAdmitState(a, snap, cfg, ordered, now)
	decisions := make([]Decision, 0, len(ordered))
	for _, c := range ordered {
		decisions = append(decisions, st.decide(c))
	}
	return decisions
}

// admitState is the per-pass arbitration bookkeeping.
type admitState struct {
	arbiter *Arbiter
	snap    *snapshot.ClusterSnapshot
	cfg     *config.Config
	now     time.Time

	breakerOpen bool
	// evacuating are the FromNodes of this cycle's health candidates —
	// nodes no admitted move may land on.
	evacuating map[string]bool
	// claims is per-node replacement GPUs: prior in-flight dispatches
	// (ledger) plus this cycle's admissions.
	claims map[string]int64
	// landed are nodes an admitted move already targets this cycle (at
	// most one landing per node per cycle).
	landed map[string]bool
	// admittedWorkloads enforces one action per workload per cycle.
	admittedWorkloads map[string]bool

	inFlight int
	hourly   int
	admitted int
}

func newAdmitState(a *Arbiter, snap *snapshot.ClusterSnapshot, cfg *config.Config, ordered []policy.Candidate, now time.Time) *admitState {
	st := &admitState{
		arbiter:           a,
		snap:              snap,
		cfg:               cfg,
		now:               now,
		evacuating:        map[string]bool{},
		claims:            map[string]int64{},
		landed:            map[string]bool{},
		admittedWorkloads: map[string]bool{},
	}
	if a.Ledger != nil {
		a.Ledger.AbsorbSnapshot(snap, now)
		st.breakerOpen = a.Ledger.BreakerOpen(now)
		st.claims = a.Ledger.ActiveClaims()
	}
	for _, c := range ordered {
		if c.Reason == policy.ReasonNodeUnhealthy && c.FromNode != "" {
			st.evacuating[c.FromNode] = true
		}
	}
	st.inFlight, st.hourly = migrationLoad(snap, a.Ledger, now)
	return st
}

// migrationLoad derives current in-flight and rolling-hour usage. In-flight
// is authoritative in the snapshot (one entry per live request UUID). The
// hourly figure takes the max of the ledger's own dispatch count and the
// snapshot-visible migration starts — max, not sum, so a dispatch is never
// double-counted once its annotation lands, while a fresh ledger after
// failover still sees the cluster's recent churn.
func migrationLoad(snap *snapshot.ClusterSnapshot, ledger *Ledger, now time.Time) (inFlight, hourly int) {
	snapshotHour := 0
	for _, w := range snap.Workloads {
		inFlight += len(w.ActiveMigrations)
		for _, m := range w.ActiveMigrations {
			if now.Sub(m.RequestedAt) <= time.Hour {
				snapshotHour++
			}
		}
		if w.LastMigration != nil && now.Sub(*w.LastMigration) <= time.Hour {
			snapshotHour++
		}
	}
	hourly = snapshotHour
	if ledger != nil {
		if n := ledger.DispatchesWithinHour(now); n > hourly {
			hourly = n
		}
	}
	return inFlight, hourly
}

// decide runs one candidate through the gates in order; the first failing
// gate names the rejection.
func (st *admitState) decide(c policy.Candidate) Decision {
	if st.breakerOpen {
		return Decision{Candidate: c, Reason: RejectCircuitBreakerOpen}
	}

	w := st.snap.Workloads[c.Workload]
	inst := lookupInstance(w, c)
	if w == nil || inst == nil {
		return Decision{Candidate: c, Reason: RejectInstanceGone}
	}

	// Opt-in/eligibility is the Arbiter's gate too: policies pre-filter,
	// but a pluggable policy must never be able to move an opted-out
	// workload. A workload carrying an unresolved (malformed) request
	// write is treated as busy until the executor ack-rejects it — the
	// garbage might be a corrupted real intent.
	if !w.Movable {
		return Decision{Candidate: c, Reason: RejectNotMovable}
	}
	if len(w.MalformedRequests) > 0 {
		return Decision{Candidate: c, Reason: RejectMalformedRequest}
	}

	// Terminating exclusion: action never while a pod is deleting.
	for _, pod := range inst.Pods {
		if pod.Terminating {
			return Decision{Candidate: c, Reason: RejectTerminating}
		}
	}

	// Mutual exclusion per workload: one in-flight action, whether it is
	// already running (snapshot) or admitted earlier this cycle.
	key := c.Workload.String()
	if len(w.ActiveMigrations) > 0 || st.admittedWorkloads[key] {
		return Decision{Candidate: c, Reason: RejectWorkloadBusy}
	}

	// OEP-0013 awareness: never race the autoscaler over the same pods.
	if st.arbiter.AutoscalerIntent != nil {
		if st.arbiter.AutoscalerIntent(st.snap, key) != IntentIdle {
			return Decision{Candidate: c, Reason: RejectAutoscalerActive}
		}
	}

	// Class-aware cooldowns: health evacuations wait only the floor.
	health := c.Reason == policy.ReasonNodeUnhealthy
	window := st.cfg.PerWorkloadCooldown()
	if w.CooldownOverride != nil {
		window = *w.CooldownOverride
	}
	floor := st.cfg.HealthCooldownFloor()
	overridden := false
	if w.LastMigration != nil {
		age := st.now.Sub(*w.LastMigration)
		if health {
			if age < floor {
				return Decision{Candidate: c, Reason: RejectCooldown}
			}
			overridden = age < window
		} else if age < window {
			return Decision{Candidate: c, Reason: RejectCooldown}
		}
	}
	// Placement cooldown, authorship-blind: a freshly scheduled pod means
	// someone just placed this instance — operator, scheduler, or Alfred.
	placementWindow := st.cfg.RecentPlacementCooldown()
	if health {
		placementWindow = floor
	}
	for _, pod := range inst.Pods {
		if pod.StartTime != nil && st.now.Sub(*pod.StartTime) < placementWindow {
			return Decision{Candidate: c, Reason: RejectPlacementCooldown}
		}
	}
	// Per-node cooldown, source side: a routine move may not pull from a
	// node still cooling; a health evacuation may (draining is the point).
	if st.arbiter.Ledger != nil && !health &&
		st.arbiter.Ledger.NodeCooling(c.FromNode, st.cfg.PerNodeCooldown(), st.now) {
		return Decision{Candidate: c, Reason: RejectNodeCooldown}
	}

	target, placement, reason := st.selectTarget(c, inst)
	if reason != "" {
		return Decision{Candidate: c, Reason: reason}
	}

	// Global caps, on the combined stream, after everything candidate-
	// specific: budget is a property of Alfred as a whole.
	if st.inFlight+st.admitted >= st.cfg.MaxInFlightMigrations {
		return Decision{Candidate: c, Reason: RejectInFlightCap}
	}
	if st.hourly+st.admitted >= st.cfg.MaxMigrationsPerHour {
		return Decision{Candidate: c, Reason: RejectHourlyCap}
	}

	st.admitted++
	st.admittedWorkloads[key] = true
	for node, gpus := range placement {
		st.landed[node] = true
		st.claims[node] += gpus
	}
	return Decision{Candidate: c, Admitted: true, Target: target, CooldownOverridden: overridden}
}

// selectTarget re-checks capacity mode-aware and net of claims, against the
// candidate's exhaustive placement targets when supplied, or its reporting
// hints for compatibility with other policies. Those targets are already
// schedulable- and model-filtered by the policy on this same snapshot. Surge
// shapes must fit while the source still holds its GPUs; eviction needs no
// headroom and may reuse its source.
// It returns the primary target, the per-node GPUs the admission will claim,
// and — when nothing fits a surge — a reason distinguishing *why*: a hint
// under evacuation, a hint already landed on this cycle, or plain capacity.
func (st *admitState) selectTarget(c policy.Candidate, inst *snapshot.Instance) (string, map[string]int64, string) {
	targets := c.PlacementTargetNodes
	if len(targets) == 0 {
		targets = c.HintTargetNodes
	}

	// The saw* flags feed the rejection reason (a metrics label and Event
	// text), so they are reset per placement attempt: an early pod probing
	// past a busy hint must not relabel a later pod's plain capacity
	// failure.
	var sawEvacuating, sawLanded, sawCooling bool
	resetSaw := func() { sawEvacuating, sawLanded, sawCooling = false, false, false }
	fits := func(node string, gpus int64) bool {
		n := st.snap.Nodes[node]
		if n == nil {
			return false
		}
		if st.evacuating[node] {
			sawEvacuating = true
			return false
		}
		if st.landed[node] {
			sawLanded = true
			return false
		}
		// Target-side per-node cooldown: nothing lands on a node still
		// cooling from a recent landing (or recent defrag outflow).
		if st.arbiter.Ledger != nil &&
			st.arbiter.Ledger.NodeCooling(node, st.cfg.PerNodeCooldown(), st.now) {
			sawCooling = true
			return false
		}
		return n.FreeGPUs-st.claims[node] >= gpus
	}

	if !c.SurgeShaped {
		// Free-then-place cannot deadlock; a target is a preference,
		// not a precondition. Claim the first target it will likely land on;
		// none feasible means the replacement reuses the source.
		for _, hint := range targets {
			if fits(hint, c.FootprintGPUs) {
				return hint, map[string]int64{hint: c.FootprintGPUs}, ""
			}
		}
		return "", nil, ""
	}

	// Surge: every member pod's replacement must fit simultaneously.
	// Greedy largest-first over the ranked hints mirrors the policy's own
	// simulation; claims accumulate per hint as pods are placed.
	pods := make([]int64, 0, len(inst.Pods))
	for _, pod := range inst.Pods {
		if pod.GPUs > 0 {
			pods = append(pods, pod.GPUs)
		}
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i] > pods[j] })
	if len(pods) == 0 && c.FootprintGPUs > 0 {
		// Defensive: a pluggable policy may declare a footprint without
		// per-pod detail; hold the declared footprint so the surge
		// still passes the capacity gate and claims its block.
		pods = append(pods, c.FootprintGPUs)
	}
	placement := map[string]int64{}
	first := ""
	for _, gpus := range pods {
		placed := false
		resetSaw()
		for _, hint := range targets {
			if !fits(hint, gpus+placement[hint]) {
				continue
			}
			placement[hint] += gpus
			if first == "" {
				first = hint
			}
			placed = true
			break
		}
		if !placed {
			switch {
			case sawEvacuating:
				return "", nil, RejectTargetUnderEvac
			case sawLanded:
				return "", nil, RejectTargetNodeBusy
			case sawCooling:
				return "", nil, RejectNodeCooldown
			default:
				return "", nil, RejectNoCapacity
			}
		}
	}
	return first, placement, ""
}

// lookupInstance resolves the candidate's addressed instance by Index.
func lookupInstance(w *snapshot.Workload, c policy.Candidate) *snapshot.Instance {
	if w == nil {
		return nil
	}
	comp := w.Components[c.Component]
	if comp == nil {
		return nil
	}
	for _, inst := range comp.Instances {
		if inst.Index == c.Instance {
			return inst
		}
	}
	return nil
}
