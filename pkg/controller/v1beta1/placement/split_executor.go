package placement

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	return r.executePlannedAllocation(ctx, source, eligible, standing, proposal)
}

func (r *Reconciler) executePlannedAllocation(ctx context.Context, source *v1beta1.InferenceService, eligible []string, standing *placementObservations, proposal plan.Proposal) (ctrl.Result, error) {
	proposal.PauseSurge = splitPauseRequired(source, proposal)
	if proposal.AdoptionDigest != "" {
		proposal.PauseSurge = false
	}
	store := plan.Store{Client: r.Client, Reader: r.APIReader}
	accepted, err := store.Persist(ctx, source, proposal)
	if err != nil {
		return r.writeSplitHold(ctx, source, standing, "PlanNotPersisted", err)
	}
	observations := r.observeSplitMembers(ctx, accepted, eligible)
	if proposal.Mode == v1beta1.PlacementModeAll {
		for name, observed := range observations {
			// The standing read still describes an unreadable planned home. It keeps
			// the plan it last acknowledged; only unverified ready capacity is withheld.
			if home, exists := standing.get(name); exists && !observed.Candidate.ObservationKnown {
				candidate := *home.candidate.DeepCopy()
				candidate.Allocation, candidate.AppliedPlanID = observed.Candidate.Allocation, observed.Candidate.AppliedPlanID
				candidate.ObservationKnown, candidate.ReadyReplicas = false, 0
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
	if proposal.SingleMove != nil && proposal.SingleMove.Selected != "" && proposal.SingleMove.Selected != proposal.Winner {
		selected := observations[proposal.SingleMove.Selected]
		if !selected.FullHomeReady || !selected.Home.Routable {
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
		if target == 0 && assignment.DesiredHome == nil && observations[name].Home.Absent {
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
			if assignment.CurrentReplicas == 0 && assignment.DesiredHome == nil && observations[name].Home.Absent {
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
		if selected.FullHomeReady && selected.Home.Routable && selected.Home.Known && (slices.Contains(step.Drain, proposal.Winner) || observations[proposal.Winner].Home.Absent) {
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
		case (assignment.CurrentReplicas > 0 || (assignment.CurrentHome != nil && !assignment.DrainRequested)) && observed.Home.Eligible:
			err = r.placePlannedOn(cctx, next, candidate)
		case assignment.CurrentReplicas == 0 && assignment.DrainRequested && observed.Home.Drained && !changed:
			err = r.deletePlannedOn(cctx, next, candidate, true)
		case observed.Member != nil:
			err = r.syncPlannedPolicy(cctx, next, candidate)
		default:
			err = nil
		}
		cancel()
		if err != nil {
			if apierrors.IsConflict(err) {
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
	return r.writeSplitObservations(ctx, next, observations, step.Reason, "")
}

func splitPauseRequired(source *v1beta1.InferenceService, proposal plan.Proposal) bool {
	if proposal.SingleMove != nil {
		return true
	}
	grow, shrink, drain := false, false, false
	for _, assignment := range proposal.Assignments {
		grow = grow || assignment.DesiredReplicas > assignment.CurrentReplicas
		shrink = shrink || assignment.DesiredReplicas < assignment.CurrentReplicas
		drain = drain || assignment.DrainRequested
	}
	active := source.Status.Placement != nil && source.Status.Placement.Plan != nil && source.Status.Placement.Plan.PauseSurge
	// An unconfigured move cannot begin. Leave member rollouts and scaling
	// available unless an accepted transition already owns their allowance.
	if source.Spec.Placement.MaxSurge == nil && grow && shrink && !active {
		return false
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
	// registration before adopting authority, including currently unmatched homes.
	inventory := maps.Clone(proposal.Assignments)
	for _, cluster := range clusters {
		if _, exists := inventory[cluster.Name]; !exists {
			inventory[cluster.Name] = v1beta1.CandidateAllocationStatus{ClusterUID: cluster.UID}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(inventory)) {
		assignment := inventory[name]
		candidate := v1beta1.CandidatePlacement{Cluster: name, Allocation: &assignment}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		observed, err := r.observePlannedHome(cctx, readSource, candidate, declaredComponents(source))
		cancel()
		if err != nil {
			return fmt.Errorf("cluster %q: %w", name, err)
		}
		if _, tracked := proposal.Assignments[name]; observed.Home.Absent && !tracked {
			continue
		}
		if !observed.Home.Absent {
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
	var floor int32
	for i, value := range floors {
		if value == nil || *value < 0 || int64(*value) > math.MaxInt32 {
			return 0, fmt.Errorf("standing component floor is unresolved")
		}
		if i > 0 && floor != int32(*value) {
			return 0, fmt.Errorf("standing engine and decoder floors must use whole replica units")
		}
		floor = int32(*value)
	}
	if len(floors) == 0 {
		return 0, fmt.Errorf("standing service has no scalable component")
	}
	return floor, nil
}

func (r *Reconciler) observeSplitMembers(ctx context.Context, source *v1beta1.InferenceService, eligible []string) map[string]plannedHomeObservation {
	trafficMap := &v1beta1.TrafficMap{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(source), trafficMap); err != nil {
		trafficMap = nil
	}
	out := map[string]plannedHomeObservation{}
	settled := true
	for _, candidate := range source.Status.Placement.Candidates {
		settled = settled && candidate.Allocation.CurrentReplicas == candidate.Allocation.DesiredReplicas
	}
	for _, candidate := range source.Status.Placement.Candidates {
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		observed, err := r.observePlannedHome(cctx, source, candidate, declaredComponents(source))
		cancel()
		if err != nil {
			r.Log.Error(err, "planned member observation failed", "cluster", candidate.Cluster)
		}
		observed.Home.Eligible = slices.Contains(eligible, candidate.Cluster)
		known, routable, drained := plannedTrafficEvidence(source, trafficMap, observed.Candidate)
		observed.Home.Routable, observed.Home.Drained = routable, drained
		// Settled positive floors can release their pause without routing. Any
		// movement or zero-floor cleanup requires current routing intent so a
		// stale table cannot authorize retiring a serving home.
		activeZero := source.Status.Placement.Plan.Mode == v1beta1.PlacementModeAll && !source.Status.Placement.Plan.PauseSurge && candidate.Allocation.CurrentHome != nil && candidate.Allocation.DesiredHome != nil && !candidate.Allocation.DrainRequested
		if !known && !observed.Home.Absent && (!settled || (candidate.Allocation.CurrentReplicas == 0 && !activeZero)) {
			observed.Home.Known = false
		}
		out[candidate.Cluster] = observed
	}
	return out
}

func advanceSplitPlan(source *v1beta1.InferenceService, observations map[string]plannedHomeObservation) (allocation.Step, error) {
	accepted := source.Status.Placement.Plan
	input := allocation.Transition{
		From:    allocation.Plan{Targets: map[string]int32{}, Unassigned: accepted.OriginalUnassignedReplicas},
		Current: map[string]int32{}, Desired: allocation.Plan{Targets: map[string]int32{}, Unassigned: accepted.UnassignedReplicas},
		Homes: map[string]allocation.Home{}, MaxSurge: source.Spec.Placement.MaxSurge,
	}
	var reserved, occupied int64
	pauseAcknowledged := true
	for _, candidate := range source.Status.Placement.Candidates {
		assignment, observed := candidate.Allocation, observations[candidate.Cluster]
		input.From.Targets[candidate.Cluster] = assignment.OriginalReplicas
		input.Current[candidate.Cluster] = assignment.CurrentReplicas
		input.Desired.Targets[candidate.Cluster] = assignment.DesiredReplicas
		input.Homes[candidate.Cluster] = observed.Home
		reserved += int64(observed.RolloutReserved)
		occupied += int64(observed.Home.Occupied)
		if !observed.Home.Absent && !observed.PauseAcknowledged {
			pauseAcknowledged = false
		}
	}
	if reserved > math.MaxInt32 {
		return allocation.Step{}, fmt.Errorf("rollout reservations exceed supported replica count")
	}
	input.RolloutReserved = int32(reserved)
	if accepted.PauseSurge && !pauseAcknowledged {
		return allocation.Step{Targets: input.Current, Reason: "AwaitingSurgePause"}, nil
	}
	if accepted.Mode == v1beta1.PlacementModeAll {
		zeroFloor := false
		for _, candidate := range source.Status.Placement.Candidates {
			a := candidate.Allocation
			zeroFloor = zeroFloor || zeroHomeFloor(a.CurrentHome) || zeroHomeFloor(a.DesiredHome)
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
		if !observations[selected].FullHomeReady {
			home := input.Homes[selected]
			home.Ready = 0
			input.Homes[selected] = home
		}
	}
	advance := allocation.Advance
	if accepted.Mode == v1beta1.PlacementModeAll || accepted.SingleMove != nil {
		advance = allocation.AdvanceWholeHomes
	}
	step, err := advance(input)
	if accepted.SingleMove != nil && accepted.SingleMove.Selected != accepted.Winner && observations[accepted.SingleMove.Selected].Terminal {
		step.Complete = false
		step.Reason = "ReplacementFailed"
	}
	if step.Complete && accepted.PauseSurge && occupied+reserved > accepted.AssignedReplicas {
		step.Complete = false
		step.Reason = "AwaitingMemberCleanup"
	}
	return step, err
}

func (r *Reconciler) writeSplitObservations(ctx context.Context, source *v1beta1.InferenceService, observations map[string]plannedHomeObservation, reason, message string) (ctrl.Result, error) {
	if source.Status.Placement.Plan.Mode == v1beta1.PlacementModeSingle {
		standing := &placementObservations{homes: map[string]homeObservation{}}
		for name, observed := range observations {
			state := homePresent
			if !observed.Home.Known {
				state = homeUnknown
			} else if observed.Home.Absent {
				state = homeAbsent
			}
			serving := observed.Member != nil && placementCandidateServing(observed.Member, observed.Candidate)
			standing.homes[name] = homeObservation{state: state, candidate: observed.Candidate, serving: serving, terminal: observed.Terminal}
			standing.standing = append(standing.standing, name)
		}
		return r.writeSinglePlanStatus(ctx, source, standing, reason)
	}
	res := placementResult{phase: v1beta1.PlacementPhasePending, conditions: []policyCondition{splitProgressCondition(reason, message)}}
	if placementMode(source) == v1beta1.PlacementModeSplitByCapacity {
		res.conditions = append(res.conditions, capacityFreshCondition(corev1.ConditionTrue, "CapacityVerified", "Matched members have usable hardware and resolved demand"))
	}
	for _, previous := range source.Status.Placement.Candidates {
		observed := observations[previous.Cluster]
		candidate := observed.Candidate
		if candidate.Cluster == "" {
			candidate = retainedUnknownCandidate(source, previous)
		}
		res.candidates = append(res.candidates, candidate)
		if (previous.Allocation.CurrentReplicas > 0 || previous.Allocation.CurrentHome != nil) && res.phase == v1beta1.PlacementPhasePending {
			res.phase = v1beta1.PlacementPhaseAdmitting
		}
		if candidate.AdmittedReplicas > 0 || observed.IdleZeroFloor {
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
		return r.writeSinglePlanStatus(ctx, source, standing, reason)
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
		home := standing.homes[name]
		if home.state == homeAbsent {
			continue
		}
		res.candidates = append(res.candidates, home.candidate)
		res.ready = res.ready || home.serving
		res.readinessUnknown = res.readinessUnknown || home.state == homeUnknown
		if home.candidate.AdmittedReplicas > 0 {
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
