package placement

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/placement/plan"
)

// reconcileSplit persists each allocation before granting member authority.
// Readiness gates execution while the full affinity match determines shares.
func (r *Reconciler) reconcileSplit(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string, standing *placementObservations) (result ctrl.Result, resultErr error) {
	var samples map[string]capacity.Sample
	if placementMode(source) == v1beta1.PlacementModeSplitByCapacity {
		defer func() {
			if r.Capacity != nil && r.Capacity.RefreshInterval > 0 && (result.RequeueAfter <= 0 || result.RequeueAfter > r.Capacity.RefreshInterval) {
				result.RequeueAfter = r.Capacity.RefreshInterval
			}
		}()
		now := r.capacityNow()
		var err error
		samples, err = r.readSplitCapacity(ctx, source, clusters, now)
		if err != nil {
			r.forgetCapacity(client.ObjectKeyFromObject(source))
			return r.writeSplitHold(ctx, source, standing, "CapacityUnknown", err)
		}
		if err := r.stableCapacity(source, samples, now); err != nil {
			return r.writeSplitHold(ctx, source, standing, "CapacityStabilizing", err)
		}
	} else {
		r.forgetCapacity(client.ObjectKeyFromObject(source))
	}
	proposal, err := desiredSplitPlan(source, clusters, samples)
	if err != nil {
		return r.writeSplitHold(ctx, source, standing, "AllocationUnresolved", err)
	}
	if source.Status.Placement == nil || source.Status.Placement.Plan == nil {
		if err := r.adoptSplitMembers(ctx, source, clusters, &proposal); err != nil {
			return r.writeSplitHold(ctx, source, standing, "ObservationUnknown", err)
		}
	}
	return r.executePlannedAllocation(ctx, source, eligible, standing, proposal, nil)
}

// executePlannedAllocation persists the proposal, advances it against member
// observations and applies the step. holds explain unresolved member inputs in
// the status condition while the step waits for them.
func (r *Reconciler) executePlannedAllocation(ctx context.Context, source *v1beta1.InferenceService, eligible []string, standing *placementObservations, proposal plan.Proposal, holds []string) (ctrl.Result, error) {
	proposal.PauseSurge = splitPauseRequired(proposal)
	if proposal.AdoptionDigest != "" {
		proposal.PauseSurge = false
	}
	store := plan.Store{Client: r.Client, Reader: r.APIReader}
	accepted, err := store.Persist(ctx, source, proposal)
	if err != nil {
		return r.writeSplitHold(ctx, source, standing, "PlanNotPersisted", err)
	}
	r.recordSplitCeilingRaised(source, accepted)
	observations := r.observeSplitMembers(ctx, accepted, eligible)
	if proposal.Mode == v1beta1.PlacementModeAll {
		for name, observed := range observations {
			// The standing read still describes an unreadable planned member. It keeps
			// the plan it last acknowledged, and its ready count follows the grace
			// the failed planned read applied.
			if member, exists := standing.get(name); exists && !observed.Candidate.ObservationKnown {
				candidate := *member.candidate.DeepCopy()
				candidate.Allocation, candidate.AppliedPlanID = observed.Candidate.Allocation, observed.Candidate.AppliedPlanID
				candidate.ObservationKnown, candidate.ReadyReplicas = false, observed.Candidate.ReadyReplicas
				candidate.ObservationFailingSince = observed.Candidate.ObservationFailingSince.DeepCopy()
				observed.Candidate = candidate
				observations[name] = observed
			}
		}
	}
	var step allocation.Step
	if proposal.SingleMove != nil && proposal.SingleMove.Selected == "" {
		step, err = advanceSingleRace(accepted, observations, &proposal, time.Now())
	} else {
		step, err = advanceSplitPlan(accepted, observations)
	}
	if err != nil {
		return r.writeSplitObservations(ctx, accepted, observations, "ObservationUnknown", err.Error())
	}
	if step.Reason == "AwaitingHomeInputs" && len(holds) > 0 {
		step.Message = strings.Join(holds, "; ")
	}
	if proposal.SingleMove != nil && proposal.SingleMove.Selected != "" && proposal.SingleMove.Selected != proposal.Winner {
		selected := observations[proposal.SingleMove.Selected]
		if !selected.FullPolicyReady || !selected.MemberState.Routable {
			step.Complete = false
		}
	}
	for name, target := range step.Targets {
		assignment := proposal.Assignments[name]
		mayRefresh := proposal.SingleMove == nil || proposal.SingleMove.Selected != "" || assignment.CurrentReplicas == 0 || equality.Semantic.DeepEqual(assignment.CurrentHome, assignment.DesiredHome)
		assignment.CurrentReplicas = target
		if mayRefresh && assignment.DesiredHome != nil && target == assignment.DesiredReplicas {
			assignment.CurrentHome = assignment.DesiredHome.DeepCopy()
		}
		if slices.Contains(step.Drain, name) {
			assignment.DrainRequested = true
		}
		if slices.Contains(step.Resume, name) {
			assignment.DrainRequested = false
		}
		if target == 0 && assignment.DesiredHome == nil && observations[name].MemberState.Absent {
			assignment.CurrentHome, assignment.DrainRequested = nil, false
		}
		proposal.Assignments[name] = assignment
	}
	if step.Complete {
		proposal.PauseSurge = false
		if proposal.SingleMove != nil {
			proposal.Winner = proposal.SingleMove.Selected
			proposal.SingleMove = nil
		}
		proposal.OriginalUnassignedReplicas = proposal.UnassignedReplicas
		for name, assignment := range proposal.Assignments {
			assignment.OriginalReplicas = assignment.CurrentReplicas
			assignment.ReplacementStartedAt = nil
			if assignment.CurrentReplicas == 0 && assignment.DesiredHome == nil && observations[name].MemberState.Absent {
				assignment.DrainRequested = false
				assignment.CurrentHome = nil
				if proposal.Mode == v1beta1.PlacementModeSingle {
					assignment.RaceCandidate = true
				}
			}
			proposal.Assignments[name] = assignment
		}
	}
	if proposal.SingleMove != nil && proposal.SingleMove.Selected != "" && proposal.SingleMove.Selected != proposal.Winner {
		selected := observations[proposal.SingleMove.Selected]
		if selected.FullPolicyReady && selected.MemberState.Routable && selected.MemberState.Known && (slices.Contains(step.Drain, proposal.Winner) || observations[proposal.Winner].MemberState.Absent) {
			proposal.Winner = proposal.SingleMove.Selected
		}
	}
	next, err := store.Persist(ctx, accepted, proposal)
	if err != nil {
		return r.writeSplitObservations(ctx, accepted, observations, "PlanNotPersisted", err.Error())
	}
	// A drain acknowledgement belongs to the exact routing plan. A revision change
	// invalidates it, even when the next plan retains the same zero floor.
	changed := next.Status.Placement.Plan.ID != accepted.Status.Placement.Plan.ID
	// Completing a transition can release the pause in a new execution policy.
	// Observe that policy on members before declaring the accepted plan settled.
	if step.Complete && step.Reason == "" {
		for _, candidate := range next.Status.Placement.Candidates {
			observed := observations[candidate.Cluster].Candidate
			if (candidate.Allocation.DesiredReplicas > 0 || candidate.Allocation.DesiredHome != nil) &&
				(!observed.ObservationKnown || observed.AppliedPlanID != next.Status.Placement.Plan.ID) {
				step.Reason = "AwaitingMemberConvergence"
				break
			}
		}
	}
	// An apportionment error also fails each split member's placement below, while
	// drains and deletions still proceed.
	floorHolds, holdErr := splitSecondaryHolds(next, observations)
	if holdErr != nil {
		r.Log.V(1).Info("secondary floor holds unresolved", "error", holdErr.Error())
	}
	if len(floorHolds) > 0 && step.Reason == "" {
		step.Reason, step.Message = "ReplacementNotReady", heldFloorsMessage(floorHolds)
	}
	for _, candidate := range next.Status.Placement.Candidates {
		observed := observations[candidate.Cluster]
		assignment := candidate.Allocation
		if !observed.Candidate.ObservationKnown || (observed.Member != nil && !observed.Member.DeletionTimestamp.IsZero()) {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		switch {
		case next.Status.Placement.Plan.SingleMove != nil && next.Status.Placement.Plan.SingleMove.Selected == "" && assignment.RaceCandidate && assignment.CurrentReplicas == 0 && assignment.DesiredReplicas == 0 && assignment.DrainRequested:
			err = r.deletePlannedRaceLoser(cctx, next, candidate)
		case (assignment.CurrentReplicas > 0 || (assignment.CurrentHome != nil && !assignment.DrainRequested)) && observed.MemberState.Eligible:
			err = r.placePlannedMember(cctx, next, candidate, false, floorHolds[candidate.Cluster])
		case assignment.CurrentReplicas == 0 && assignment.DrainRequested && observed.MemberState.Drained && !changed:
			err = r.deletePlannedOn(cctx, next, candidate, true)
		case observed.Member != nil:
			err = r.syncPlannedPolicy(cctx, next, candidate)
		default:
			err = nil
		}
		cancel()
		if err != nil {
			if isMemberWriteRace(err) {
				if step.Reason != "MemberApplicationFailed" {
					step.Reason = "MemberApplicationPending"
				}
				r.Log.V(1).Info("planned member application conflicted; retrying", "cluster", candidate.Cluster)
			} else {
				step.Reason = "MemberApplicationFailed"
				r.Log.Error(err, "planned member application failed", "cluster", candidate.Cluster)
			}
		}
	}
	// The persisted plan no longer pauses, so the operator's release has taken
	// effect; status and an event say so before the request is removed.
	if step.Complete && step.CleanupReleased {
		note := cleanupReleasedNote(accepted, observations)
		if step.Message == "" {
			step.Message = note
		} else {
			step.Message = strings.TrimSuffix(step.Message, ".") + ". " + note
		}
		r.recordCleanupReleased(accepted, note)
	}
	result, err := r.writeSplitObservations(ctx, next, observations, step.Reason, step.Message)
	// A request for the revision whose pause just ended has nothing left to do.
	// A request for a move that has not finished stays until it does.
	if step.Complete && releasesCleanupWait(accepted) {
		r.answerEndedCleanupRelease(ctx, accepted, step.CleanupReleased)
	}
	return result, err
}

func splitPauseRequired(proposal plan.Proposal) bool {
	if proposal.SingleMove != nil {
		return true
	}
	grow, shrink, drain := false, false, false
	for _, assignment := range proposal.Assignments {
		grow = grow || assignment.DesiredReplicas > assignment.CurrentReplicas
		shrink = shrink || assignment.DesiredReplicas < assignment.CurrentReplicas
		drain = drain || assignment.DrainRequested
	}
	return proposal.PauseSurge || grow || shrink || drain
}

// adoptSplitMembers inventories standing copies before an allocation exists.
// Unassigned original floor protects serving replicas without treating pending
// fan-out requests as extra migration budget.
func (r *Reconciler) adoptSplitMembers(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, proposal *plan.Proposal) error {
	readSource := source.DeepCopy()
	readSource.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{SourceUID: source.UID}}
	registered := map[string]v1beta1.WorkloadCluster{}
	for _, cluster := range clusters {
		registered[cluster.Name] = cluster
	}
	for _, previous := range standingPlacementCandidates(source) {
		if _, exists := registered[previous.Cluster]; !exists {
			return fmt.Errorf("standing cluster %q registration cannot be verified", previous.Cluster)
		}
	}
	// Member writes can survive a failed first status write. Inventory every
	// registration before adopting authority, including currently unmatched members.
	inventory := maps.Clone(proposal.Assignments)
	for _, cluster := range clusters {
		if _, exists := inventory[cluster.Name]; !exists {
			inventory[cluster.Name] = v1beta1.CandidateAllocationStatus{ClusterUID: cluster.UID}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(inventory)) {
		assignment := inventory[name]
		candidate := v1beta1.CandidatePlacement{Cluster: name, Allocation: &assignment}
		observed, err := r.observePlannedMember(ctx, readSource, candidate, declaredComponents(source))
		if err != nil {
			return fmt.Errorf("cluster %q: %w", name, err)
		}
		if _, tracked := proposal.Assignments[name]; observed.MemberState.Absent && !tracked {
			continue
		}
		if !observed.MemberState.Absent {
			floor, err := adoptedMemberFloor(observed.Member)
			if err != nil {
				return fmt.Errorf("cluster %q: %w", name, err)
			}
			assignment.CurrentReplicas = floor
			proposal.PauseSurge = true
		}
		proposal.Assignments[name] = assignment
	}
	proposal.OriginalUnassignedReplicas = splitDesiredReplicas(source)
	return nil
}

// adoptedMemberFloor reads a standing member's floor in primary units: the
// engine minimum when declared, otherwise the decoder minimum. Every scalable
// component must carry a resolved count.
func adoptedMemberFloor(member *v1beta1.InferenceService) (int32, error) {
	if member == nil {
		return 0, fmt.Errorf("remaining member resources have no verifiable service floor")
	}
	var floors []*int
	if member.Spec.Engine != nil {
		floors = append(floors, member.Spec.Engine.MinReplicas)
	}
	if member.Spec.Decoder != nil {
		floors = append(floors, member.Spec.Decoder.MinReplicas)
	}
	if len(floors) == 0 {
		return 0, fmt.Errorf("standing service has no scalable component")
	}
	for _, value := range floors {
		if value == nil || *value < 0 || int64(*value) > math.MaxInt32 {
			return 0, fmt.Errorf("standing component floor is unresolved")
		}
	}
	return int32(*floors[0]), nil
}

func (r *Reconciler) observeSplitMembers(ctx context.Context, source *v1beta1.InferenceService, eligible []string) map[string]plannedMemberObservation {
	routed := r.routesTraffic(source)
	var trafficMap *v1beta1.TrafficMap
	if routed {
		trafficMap = &v1beta1.TrafficMap{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(source), trafficMap); err != nil {
			trafficMap = nil
		}
	}
	out := map[string]plannedMemberObservation{}
	settled := true
	for _, candidate := range source.Status.Placement.Candidates {
		settled = settled && candidate.Allocation.CurrentReplicas == candidate.Allocation.DesiredReplicas
	}
	for _, candidate := range source.Status.Placement.Candidates {
		observed, err := r.observePlannedMember(ctx, source, candidate, declaredComponents(source))
		if err != nil {
			r.Log.Error(err, "planned member observation failed", "cluster", candidate.Cluster)
		}
		observed.MemberState.Eligible = slices.Contains(eligible, candidate.Cluster)
		if !routed {
			// OME routes no traffic for this source, so it never gets a TrafficMap.
			// A cluster serves what its pods report ready, and a cluster asked to
			// drain has no OME-routed traffic to wait for.
			observed.MemberState.Drained = candidate.Allocation.DrainRequested
			observed.MemberState.Routable = !observed.MemberState.Drained && observed.MemberState.Ready > 0
			out[candidate.Cluster] = observed
			continue
		}
		known, routable, drained := plannedTrafficEvidence(source, trafficMap, observed.Candidate)
		observed.MemberState.Routable, observed.MemberState.Drained = routable, drained
		// Settled positive floors can release their pause without routing. Any
		// movement or zero-floor cleanup requires current routing intent so a
		// stale table cannot authorize retiring a serving member.
		activeZero := source.Status.Placement.Plan.Mode == v1beta1.PlacementModeAll && !source.Status.Placement.Plan.PauseSurge && candidate.Allocation.CurrentHome != nil && candidate.Allocation.DesiredHome != nil && !candidate.Allocation.DrainRequested
		if !known && !observed.MemberState.Absent && (!settled || (candidate.Allocation.CurrentReplicas == 0 && !activeZero)) {
			observed.MemberState.Known = false
		}
		out[candidate.Cluster] = observed
	}
	return out
}

// routesTraffic reports whether OME routes traffic for the source: routing is
// on for the installation and the source does not turn it off.
func (r *Reconciler) routesTraffic(source *v1beta1.InferenceService) bool {
	return !r.RoutingDisabled && !RoutingOptedOut(source)
}

func advanceSplitPlan(source *v1beta1.InferenceService, observations map[string]plannedMemberObservation) (allocation.Step, error) {
	accepted := source.Status.Placement.Plan
	input := allocation.Transition{
		From:    allocation.Plan{Targets: map[string]int32{}, Unassigned: accepted.OriginalUnassignedReplicas},
		Current: map[string]int32{}, Desired: allocation.Plan{Targets: map[string]int32{}, Unassigned: accepted.UnassignedReplicas},
		Members: map[string]allocation.MemberState{},
		// A move may hold one extra full copy of the desired allocation.
		Surge: int64(accepted.UnassignedReplicas),
	}
	// The transition spends the allowance view of each member, in which decoders
	// its copy keeps running are its own share. The cleanup wait below compares
	// the share view, in which they are excess, with the replicas the plan assigns.
	var reserved, allowanceReserved, occupied int64
	pauseAcknowledged := true
	for _, candidate := range source.Status.Placement.Candidates {
		assignment, observed := candidate.Allocation, observations[candidate.Cluster]
		input.From.Targets[candidate.Cluster] = assignment.OriginalReplicas
		input.Current[candidate.Cluster] = assignment.CurrentReplicas
		input.Desired.Targets[candidate.Cluster] = assignment.DesiredReplicas
		input.Surge += int64(assignment.DesiredReplicas)
		state, reservation := observed.allowanceState()
		input.Members[candidate.Cluster] = state
		reserved += int64(observed.RolloutReserved)
		allowanceReserved += int64(reservation)
		occupied += int64(observed.MemberState.Occupied)
		if !observed.MemberState.Absent && !observed.PauseAcknowledged {
			pauseAcknowledged = false
		}
	}
	if reserved > math.MaxInt32 {
		return allocation.Step{}, fmt.Errorf("rollout reservations exceed supported replica count")
	}
	input.RolloutReserved = int32(allowanceReserved)
	if accepted.PauseSurge && !pauseAcknowledged {
		return allocation.Step{Targets: input.Current, Reason: "AwaitingSurgePause"}, nil
	}
	if accepted.Mode == v1beta1.PlacementModeAll {
		zeroFloor := false
		for _, candidate := range source.Status.Placement.Candidates {
			a := candidate.Allocation
			zeroFloor = zeroFloor || hasZeroReplicaFloor(a.CurrentHome) || hasZeroReplicaFloor(a.DesiredHome)
			if a.InventoryPending || a.HomeInputsPending || (a.Matched && (a.DesiredHome == nil || a.DesiredHome.InputDigest != accepted.InputDigest)) {
				return allocation.Step{Targets: input.Current, Reason: "AwaitingHomeInputs"}, nil
			}
		}
		if zeroFloor {
			return advanceAllZeroFloors(source, observations), nil
		}
	}
	if accepted.SingleMove != nil {
		selected := accepted.SingleMove.Selected
		resolved := slices.ContainsFunc(source.Status.Placement.Candidates, func(c v1beta1.CandidatePlacement) bool {
			return c.Cluster == selected && !c.Allocation.HomeInputsPending
		})
		if selected == "" || !resolved {
			return allocation.Step{Targets: input.Current, Reason: "AwaitingHomeInputs"}, nil
		}
		if !observations[selected].FullPolicyReady {
			member := input.Members[selected]
			member.Ready = 0
			input.Members[selected] = member
		}
	}
	advance := allocation.Advance
	if accepted.Mode == v1beta1.PlacementModeAll || accepted.SingleMove != nil {
		advance = allocation.AdvanceWholeMembers
	}
	step, err := advance(input)
	if accepted.SingleMove != nil && accepted.SingleMove.Selected != accepted.Winner && observations[accepted.SingleMove.Selected].Terminal {
		step.Complete = false
		step.Reason = "ReplacementFailed"
	}
	// While growth is paused, a complete move waits until the members' replicas,
	// with rollout reservations, fit the plan's minimums. Two changes skip that
	// wait, because autoscalers need not shrink for them: an All change that keeps
	// every member and adds no engine replicas, and a Split change with a local
	// ceiling where no member leaves or drains, once each one serves its new share.
	// An operator request that names this plan revision also skips it, and only it.
	if step.Complete && accepted.PauseSurge && occupied+reserved > accepted.AssignedReplicas &&
		!allMembersStayWithoutEngineGrowth(source.Status.Placement) && !splitSharesChangedInPlace(source, observations) {
		if releasesCleanupWait(source) {
			step.CleanupReleased = true
		} else {
			step.Complete = false
			step.Reason = "AwaitingMemberCleanup"
		}
	}
	return step, err
}

// allMembersStayWithoutEngineGrowth reports whether an All plan keeps every member,
// drains none, gives each its new minimums, and raises the counted minimum (engine,
// or decoder without one) no higher than where the change began. Others are not compared.
func allMembersStayWithoutEngineGrowth(placement *v1beta1.PlacementStatus) bool {
	if placement.Plan.Mode != v1beta1.PlacementModeAll {
		return false
	}
	// Callers pass only a complete, paused step: adoption never pauses, and a single move needs Single mode.
	for _, candidate := range placement.Candidates {
		a := candidate.Allocation
		switch {
		case a.DrainRequested:
			return false
		case !a.Matched:
			// A cluster outside the plan must have had no replicas assigned when the change began.
			if a.OriginalReplicas > 0 {
				return false
			}
		case !equality.Semantic.DeepEqual(a.CurrentHome, a.DesiredHome) || a.CurrentReplicas > a.OriginalReplicas:
			return false
		}
	}
	return true
}

// splitSharesChangedInPlace reports whether a complete, paused Split step can
// end the pause while autoscalers keep replicas above the new shares. The
// primary component needs headroom (see primaryHeadroom), which lets an
// autoscaler run above its share. No member
// leaves or drains and no replicas are unassigned. A member may join. A complete
// step shows only that members applied their targets, so each must also serve
// at least its new engine and decoder shares, and its router share when routers
// are divided, as the plan's weights split them. No member may keep a decoder or
// router for a replacement elsewhere.
func splitSharesChangedInPlace(source *v1beta1.InferenceService, observations map[string]plannedMemberObservation) bool {
	placement := source.Status.Placement
	primary := primaryScaleComponent(declaredComponents(source))
	// A Split plan records no mode of its own.
	if (placement.Plan.Mode != "" && placement.Plan.Mode != v1beta1.PlacementModeSplit) || placementMode(source) != v1beta1.PlacementModeSplit ||
		primary == "" || !primaryHeadroom(source, primary) ||
		placement.Plan.UnassignedReplicas != 0 || placement.Plan.OriginalUnassignedReplicas != 0 {
		return false
	}
	if source.Spec.Decoder != nil || routerDivided(source) {
		if held, err := splitSecondaryHolds(source, observations); err != nil || len(held) > 0 {
			return false
		}
	}
	// Engines and decoders are always divided across the members. Routers are
	// divided unless the terms set weights that name only the engine or
	// decoder; then every member runs the full router policy.
	divided := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	if routerDivided(source) {
		divided = append(divided, v1beta1.RouterComponent)
	}
	shares := componentFloorsFor(source, observations)
	for _, candidate := range placement.Candidates {
		a := candidate.Allocation
		// A member that leaves still has replicas to shut down.
		if a == nil || a.DrainRequested || (a.OriginalReplicas > 0 && a.DesiredReplicas == 0) {
			return false
		}
		if a.DesiredReplicas == 0 {
			continue
		}
		// An ineligible member keeps its old bounds in its spec, so its minimums
		// there cannot show that it serves the new ones.
		observed, known := observations[candidate.Cluster]
		if !known || observed.Member == nil || !observed.MemberState.Eligible {
			return false
		}
		// A member that OME can read but not place gets the new plan without its
		// new minimums, so its spec can lag the plan's shares.
		minimums := memberMinimums(observed.Member)
		minimums[primary] = max(minimums[primary], a.CurrentReplicas)
		for _, share := range shares[candidate.Cluster] {
			minimums[share.Component] = max(minimums[share.Component], share.Desired)
		}
		for _, component := range divided {
			if observed.ComponentReady[component] < minimums[component] {
				return false
			}
		}
	}
	return true
}

// primaryHeadroom reports whether a member's autoscaler may run the primary
// component above its share: in the ratio form, when the source's maxReplicas
// for it exceeds its minReplicas; otherwise, under a cap in split.maxPerCluster.
func primaryHeadroom(source *v1beta1.InferenceService, primary v1beta1.ComponentType) bool {
	if usesRatioForm(source) {
		return componentMaxReplicas(source, primary) > componentMinReplicas(source, primary)
	}
	return source.Spec.Placement.Split.Ceiling(primary) > 0
}

// memberMinimums reads the minimum of each scalable component from a member's
// spec, where the planned allocation writes them.
func memberMinimums(member *v1beta1.InferenceService) map[v1beta1.ComponentType]int32 {
	out := map[v1beta1.ComponentType]int32{}
	if e := member.Spec.Engine; e != nil && e.MinReplicas != nil {
		out[v1beta1.EngineComponent] = int32(*e.MinReplicas)
	}
	if d := member.Spec.Decoder; d != nil && d.MinReplicas != nil {
		out[v1beta1.DecoderComponent] = int32(*d.MinReplicas)
	}
	if r := member.Spec.Router; r != nil && r.MinReplicas != nil {
		out[v1beta1.RouterComponent] = int32(*r.MinReplicas)
	}
	return out
}

func (r *Reconciler) writeSplitObservations(ctx context.Context, source *v1beta1.InferenceService, observations map[string]plannedMemberObservation, reason, message string) (ctrl.Result, error) {
	if source.Status.Placement.Plan.Mode == v1beta1.PlacementModeSingle {
		standing := &placementObservations{members: map[string]memberObservation{}}
		for name, observed := range observations {
			state := memberPresent
			if !observed.MemberState.Known {
				state = memberUnknown
			} else if observed.MemberState.Absent {
				state = memberAbsent
			}
			serving := observed.Member != nil && placementCandidateServing(observed.Member, observed.Candidate)
			standing.members[name] = memberObservation{state: state, candidate: observed.Candidate, serving: serving, terminal: observed.Terminal}
			standing.standing = append(standing.standing, name)
		}
		return r.writeSinglePlanStatus(ctx, source, standing, reason, message)
	}
	res := placementResult{phase: v1beta1.PlacementPhasePending, conditions: []policyCondition{splitProgressCondition(reason, message), componentSharesCondition(source)}}
	if placementMode(source) == v1beta1.PlacementModeSplitByCapacity {
		res.conditions = append(res.conditions, capacityFreshCondition(corev1.ConditionTrue, "CapacityVerified", "Matched members have usable hardware and resolved demand"))
	}
	componentFloors := componentFloorsFor(source, observations)
	for _, previous := range source.Status.Placement.Candidates {
		observed := observations[previous.Cluster]
		candidate := observed.Candidate
		if candidate.Cluster == "" {
			candidate = r.unknownCandidate(source, previous)
		}
		candidate.ComponentFloors = componentFloors[previous.Cluster]
		res.candidates = append(res.candidates, candidate)
		if (previous.Allocation.CurrentReplicas > 0 || previous.Allocation.CurrentHome != nil) && res.phase == v1beta1.PlacementPhasePending {
			res.phase = v1beta1.PlacementPhaseAdmitting
		}
		// An idle member proves Placed only through a readable observation. An
		// unreadable pass keeps the Placed verdict the last readable pass reached
		// for this same plan instead of retracting evidence it did not disprove.
		keptPlaced := !candidate.ObservationKnown && hasZeroReplicaFloor(previous.Allocation.CurrentHome) &&
			candidate.AppliedPlanID == source.Status.Placement.Plan.ID && source.Status.Placement.Phase == v1beta1.PlacementPhasePlaced
		if candidate.AdmittedReplicas > 0 || observed.IdleZeroFloor || keptPlaced {
			res.phase = v1beta1.PlacementPhasePlaced
		}
		res.ready = res.ready || (candidate.ObservationKnown && candidate.ReadyReplicas > 0 && candidate.Endpoint != nil)
		res.readinessUnknown = res.readinessUnknown || !candidate.ObservationKnown
	}
	res.readinessUnknown = res.readinessUnknown && !res.ready
	result, err := r.writePlacement(ctx, source, res)
	if reason != "" {
		result.RequeueAfter = r.requeue()
	}
	return result, err
}

func (r *Reconciler) writeSplitHold(ctx context.Context, source *v1beta1.InferenceService, standing *placementObservations, reason string, cause error) (ctrl.Result, error) {
	if source.Status.Placement != nil && source.Status.Placement.Plan != nil && source.Status.Placement.Plan.Mode == v1beta1.PlacementModeSingle {
		return r.writeSinglePlanStatus(ctx, source, standing, reason, cause.Error())
	}
	res := placementResult{phase: v1beta1.PlacementPhasePending, conditions: []policyCondition{splitProgressCondition(reason, cause.Error())}}
	if placementMode(source) == v1beta1.PlacementModeSplitByCapacity {
		status := corev1.ConditionTrue
		if reason == "CapacityUnknown" {
			status = corev1.ConditionFalse
		}
		res.conditions = append(res.conditions, capacityFreshCondition(status, reason, cause.Error()))
	}
	for _, name := range standing.projectedStanding() {
		member := standing.members[name]
		if member.state == memberAbsent {
			continue
		}
		res.candidates = append(res.candidates, member.candidate)
		res.ready = res.ready || member.serving
		res.readinessUnknown = res.readinessUnknown || member.state == memberUnknown
		// A pass that waits on a reason has not read the member inventory, so it
		// cannot re-prove that a member kept idle by a zero minimum replica count is
		// still placed. It keeps the Placed verdict the last readable pass reached.
		if member.candidate.AdmittedReplicas > 0 || heldPlaced(source, name, member) {
			res.phase = v1beta1.PlacementPhasePlaced
		} else if res.phase == v1beta1.PlacementPhasePending {
			res.phase = v1beta1.PlacementPhaseAdmitting
		}
	}
	res.readinessUnknown = res.readinessUnknown && !res.ready
	result, err := r.writePlacement(ctx, source, res)
	result.RequeueAfter = r.requeue()
	return result, err
}

// heldPlaced keeps the Placed phase for a member whose minimum replica count is
// zero while a pass waits on a reason: the last readable pass left the placement
// Placed and the member still acknowledges the plan that stands.
func heldPlaced(source *v1beta1.InferenceService, cluster string, member memberObservation) bool {
	placement := source.Status.Placement
	if member.terminal || placement == nil || placement.Plan == nil || placement.Phase != v1beta1.PlacementPhasePlaced {
		return false
	}
	for _, candidate := range placement.Candidates {
		if candidate.Cluster == cluster {
			return candidate.Allocation != nil && hasZeroReplicaFloor(candidate.Allocation.CurrentHome) && candidate.AppliedPlanID == placement.Plan.ID
		}
	}
	return false
}

func splitProgressCondition(reason, message string) policyCondition {
	condition := policyCondition{condType: apis.ConditionType(v1beta1.PlacementConverged), cond: apis.Condition{
		Status: corev1.ConditionFalse, Reason: reason, Message: message,
	}}
	if reason == "" {
		condition.cond.Status = corev1.ConditionTrue
		condition.cond.Reason = "AllocationConverged"
		condition.cond.Message = "Members have applied the accepted allocation"
	} else if reason == "ReplacementFailed" && message == "" {
		condition.cond.Message = "Selected replacement is terminally failed; the serving winner remains retained"
	} else if reason == "PositiveFloorRequired" && message == "" {
		condition.cond.Message = "Configure positive component minReplicas before moving the retained home"
	} else if message == "" {
		condition.cond.Message = "Waiting for member execution and routing to converge on the accepted allocation"
	}
	return condition
}
