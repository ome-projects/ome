package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// reconcileSinglePlanned commits race nominations and the observed winner
// before member writes. Race cleanup is distinct from moving a retained home.
func (r *Reconciler) reconcileSinglePlanned(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string, observations *placementObservations) (ctrl.Result, error) {
	if source.Status.Placement != nil && source.Status.Placement.Plan != nil && source.Status.Placement.Plan.SingleMove != nil {
		return r.reconcileSingleMove(ctx, source, clusters, eligible, observations)
	}
	winner := winnerCluster(source)
	if winner != "" {
		home, ok := observations.get(winner)
		if !ok {
			home = observations.refresh(ctx, r, source, winner)
		}
		if home.state == homeUnknown || home.terminal {
			return r.writeSinglePlanStatus(ctx, source, observations, "", "")
		}
		retainZero, err := r.retainZeroFloorWinner(ctx, source, winner)
		if err != nil {
			r.Log.Error(err, "planned member observation failed", "cluster", winner)
			return r.writeSinglePlanStatus(ctx, source, observations, "AwaitingMemberConvergence", "")
		}
		if home.state == homeAbsent || (home.candidate.Phase != v1beta1.CandidatePhaseAdmitted && !retainZero) {
			if r.graceRemaining(source.UID, time.Now()) > 0 {
				return r.writeSinglePlanStatus(ctx, source, observations, "AwaitingWinnerRecovery", "")
			}
			if len(eligible) == 0 {
				return r.writeSinglePlanStatus(ctx, source, observations, "AwaitingWinnerRecovery", "")
			}
			r.clearGrace(source.UID)
			winner = ""
		} else {
			r.clearGrace(source.UID)
			if !observations.matches[winner] {
				return r.reconcileSingleMove(ctx, source, clusters, eligible, observations)
			}
			if !slices.Contains(eligible, winner) {
				return r.writeSinglePlanStatus(ctx, source, observations, "AwaitingHomeInputs", "")
			}
		}
	}
	nominated := []string{winner}
	var roundWait time.Duration
	if winner == "" {
		nominated, roundWait = r.nominate().Nominate(source.UID, eligible, time.Now())
		if source.Status.Placement != nil && source.Status.Placement.Plan != nil && source.Status.Placement.Plan.Mode == v1beta1.PlacementModeSingle && source.Status.Placement.Plan.Winner == "" {
			for _, candidate := range source.Status.Placement.Candidates {
				if candidate.Allocation != nil && candidate.Allocation.RaceCandidate && candidate.Allocation.DesiredHome != nil && slices.Contains(eligible, candidate.Cluster) && !slices.Contains(nominated, candidate.Cluster) {
					nominated = append(nominated, candidate.Cluster)
				}
			}
			slices.Sort(nominated)
		}
	}
	proposal, err := r.singleProposal(ctx, source, clusters, winner, nominated)
	if err != nil {
		r.Log.Error(err, "Single home policy unresolved")
		return r.writeSinglePlanStatus(ctx, source, observations, "HomePolicyUnresolved", err.Error())
	}
	store := plan.Store{Client: r.Client, Reader: r.APIReader}
	accepted, err := store.Persist(ctx, source, proposal)
	if err != nil {
		r.Log.Error(err, "Single plan persistence held")
		return r.writeSinglePlanStatus(ctx, source, observations, "PlanNotPersisted", "")
	}
	applied := map[string]bool{}
	for _, candidate := range accepted.Status.Placement.Candidates {
		a := candidate.Allocation
		if a.CurrentHome == nil || a.DrainRequested || a.HomeInputsPending || !slices.Contains(nominated, candidate.Cluster) {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		err := r.placePlannedMember(cctx, accepted, candidate, winner != "")
		cancel()
		if err != nil {
			r.Log.Error(err, "Single member application held", "cluster", candidate.Cluster)
			observations.refresh(ctx, r, accepted, candidate.Cluster)
			continue
		}
		applied[candidate.Cluster] = true
		observations.refresh(ctx, r, accepted, candidate.Cluster)
	}
	if winner == "" {
		for _, name := range slices.Sorted(maps.Keys(applied)) {
			home := observations.homes[name]
			if home.state != homePresent || home.terminal || home.candidate.Phase != v1beta1.CandidatePhaseAdmitted {
				continue
			}
			proposal.Winner = name
			for cluster, a := range proposal.Assignments {
				if cluster == name {
					a.RaceCandidate = false
					a.OriginalReplicas = a.DesiredReplicas
				} else {
					a.OriginalReplicas, a.CurrentReplicas, a.DesiredReplicas = 0, 0, 0
					a.DesiredHome = nil
					a.DrainRequested = true
				}
				proposal.Assignments[cluster] = a
			}
			selected, err := store.Persist(ctx, accepted, proposal)
			if err != nil {
				r.Log.Error(err, "Single winner persistence held")
				return r.writeSinglePlanStatus(ctx, accepted, observations, "WinnerNotPersisted", "")
			}
			accepted = selected
			winner = name
			break
		}
	}
	if winner != "" {
		r.sweepPlannedRace(ctx, accepted)
	}
	waiting := false
	if winner == "" && len(nominated) > 0 {
		waiting = true
		for _, name := range nominated {
			waiting = waiting && applied[name] && observations.homes[name].state == homePresent
		}
	}
	result, err := r.writePlacement(ctx, accepted, r.singlePlanStatus(ctx, accepted, observations, "", ""))
	// A materialized race waits for member events or the configured backstop.
	// Failed writes, unknown observations and stale snapshots retry promptly.
	if !waiting && result.RequeueAfter > 0 {
		result.RequeueAfter = r.requeue()
	}
	if winner == "" && roundWait > 0 {
		result.RequeueAfter = min(result.RequeueAfter, roundWait)
	}
	return result, err
}

func (r *Reconciler) singleProposal(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, winner string, nominated []string) (plan.Proposal, error) {
	out := plan.Proposal{Mode: v1beta1.PlacementModeSingle, Winner: winner, Assignments: map[string]v1beta1.CandidateAllocationStatus{}}
	registered := map[string]types.UID{}
	for _, cluster := range clusters {
		if cluster.Name == "" || cluster.UID == "" || registered[cluster.Name] != "" {
			return out, fmt.Errorf("Single placement requires distinct identified registrations")
		}
		registered[cluster.Name] = cluster.UID
	}
	if previous := source.Status.Placement; previous != nil && previous.Plan != nil {
		if previous.Plan.Mode != v1beta1.PlacementModeSingle || previous.Plan.SourceUID != source.UID || previous.Plan.PauseSurge {
			return out, fmt.Errorf("standing authority cannot enter an unpaused Single race")
		}
		for _, candidate := range previous.Candidates {
			if candidate.Allocation == nil {
				return out, fmt.Errorf("standing Single home lacks allocation authority")
			}
			out.Assignments[candidate.Cluster] = *candidate.Allocation.DeepCopy()
		}
	}
	// Every registered copy is accounted for before winner cleanup. An
	// unobserved registration grants no positive workload authority.
	for name, uid := range registered {
		if _, exists := out.Assignments[name]; !exists {
			out.Assignments[name] = v1beta1.CandidateAllocationStatus{ClusterUID: uid, RaceCandidate: name != winner}
		}
	}
	targets := map[string]types.UID{}
	for _, name := range nominated {
		if name == "" {
			continue
		}
		a := out.Assignments[name]
		if registered[name] == "" || a.ClusterUID != registered[name] {
			return out, fmt.Errorf("Single target %q incarnation is unverified", name)
		}
		targets[name] = registered[name]
	}
	encoded, err := json.Marshal(struct {
		Spec                v1beta1.InferenceServiceSpec
		Labels, Annotations map[string]string
		Targets             map[string]types.UID
		Winner              string
	}{source.Spec, source.Labels, source.Annotations, targets, winner})
	if err != nil {
		return out, err
	}
	digest := sha256.Sum256(encoded)
	out.InputDigest = hex.EncodeToString(digest[:])
	desired, err := r.derivedFor(source)
	if err != nil {
		return out, err
	}
	for name, a := range out.Assignments {
		a.Matched = targets[name] != ""
		a.DesiredHome = nil
		a.DesiredReplicas = 0
		a.HomeInputsPending = false
		if winner == "" {
			a.RaceCandidate = true
		}
		if targets[name] != "" {
			cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
			home, resolveErr := r.resolveFullHome(cctx, source, desired, name, a.ClusterUID, out.InputDigest)
			cancel()
			if resolveErr != nil {
				if name == winner {
					return out, fmt.Errorf("home policy is unresolved on %s: %w", name, resolveErr)
				}
				a.HomeInputsPending = true
			} else {
				a.CurrentHome, a.DesiredHome = home.DeepCopy(), home
				a.CurrentReplicas, _ = protocol.ValidateReplicaFloors(home.ReplicaFloors)
				a.DesiredReplicas = a.CurrentReplicas
				a.DrainRequested = false
				if name == winner {
					a.OriginalReplicas = a.CurrentReplicas
					a.RaceCandidate = false
				}
			}
		}
		if a.DesiredHome == nil {
			a.CurrentReplicas = 0
			a.DrainRequested = a.CurrentHome != nil || winner != ""
		}
		out.Assignments[name] = a
	}
	return out, nil
}

// sweepPlannedRace uses only persisted race-loser permission. A retained home
// cannot acquire this cleanup permission merely by changing affinity.
func (r *Reconciler) sweepPlannedRace(ctx context.Context, source *v1beta1.InferenceService) {
	for _, candidate := range source.Status.Placement.Candidates {
		if candidate.Cluster == winnerCluster(source) || !candidate.Allocation.RaceCandidate {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		err := r.deletePlannedRaceLoser(cctx, source, candidate)
		cancel()
		if err != nil {
			r.Log.Error(err, "Single race loser cleanup held", "cluster", candidate.Cluster)
		}
	}
}

func (r *Reconciler) deletePlannedRaceLoser(ctx context.Context, source *v1beta1.InferenceService, candidate v1beta1.CandidatePlacement) error {
	if source.Status.Placement == nil || source.Status.Placement.Plan == nil || source.Status.Placement.Plan.Mode != v1beta1.PlacementModeSingle || source.Status.Placement.Plan.Winner == "" || candidate.Cluster == source.Status.Placement.Plan.Winner || candidate.Allocation == nil || !candidate.Allocation.RaceCandidate || candidate.Allocation.DesiredReplicas != 0 || candidate.Allocation.DesiredHome != nil {
		return fmt.Errorf("race cleanup requires a committed winner and explicit loser authority")
	}
	return r.deletePlannedOn(ctx, source, candidate, true)
}

func (r *Reconciler) writeSinglePlanStatus(ctx context.Context, source *v1beta1.InferenceService, observations *placementObservations, reason, message string) (ctrl.Result, error) {
	result, err := r.writePlacement(ctx, source, r.singlePlanStatus(ctx, source, observations, reason, message))
	if result.RequeueAfter > 0 {
		result.RequeueAfter = r.requeue()
	}
	return result, err
}

func (r *Reconciler) singlePlanStatus(ctx context.Context, source *v1beta1.InferenceService, observations *placementObservations, reason, message string) placementResult {
	winner := winnerCluster(source)
	res := placementResult{winner: winner, phase: v1beta1.PlacementPhasePending}
	cleanupPending, winnerApplied, winnerIdle, winnerKeptPlaced := false, false, false, false
	names := observations.standing
	if source.Status.Placement != nil && source.Status.Placement.Plan != nil {
		names = nil
		for _, candidate := range source.Status.Placement.Candidates {
			names = append(names, candidate.Cluster)
		}
	}
	if winner != "" && !slices.Contains(names, winner) {
		names = append(slices.Clone(names), winner)
	}
	for _, name := range names {
		home, ok := observations.get(name)
		if !ok {
			home = observations.refresh(ctx, r, source, name)
		}
		candidate := home.candidate
		if candidate.Cluster == "" {
			candidate = identityCandidate(name)
		}
		var stored v1beta1.CandidatePlacement
		if source.Status.Placement != nil {
			for _, previous := range source.Status.Placement.Candidates {
				if previous.Cluster == name {
					stored = previous
					candidate.Allocation = previous.Allocation.DeepCopy()
					break
				}
			}
		}
		candidate.ObservationKnown = home.state != homeUnknown
		// A pass that cannot read the home keeps the plan it last acknowledged;
		// a conclusively absent home acknowledges nothing.
		candidate.AppliedPlanID = ""
		if candidate.Allocation != nil && home.state != homeAbsent {
			candidate.AppliedPlanID = stored.AppliedPlanID
		}
		if candidate.Allocation != nil {
			observed, err := r.observePlannedHome(ctx, source, candidate, declaredComponents(source))
			if err != nil {
				r.Log.Error(err, "planned member observation failed", "cluster", name)
			} else {
				candidate.AppliedPlanID = observed.Candidate.AppliedPlanID
			}
			if candidate.Allocation.RaceCandidate && (err != nil || !observed.Home.Absent) {
				cleanupPending = true
			}
			// The winner-applied decision reads this pass, never a kept value.
			if err == nil && name == winner && candidate.AppliedPlanID == source.Status.Placement.Plan.ID {
				winnerApplied = true
				winnerIdle = observed.IdleZeroFloor
			}
			// An idle home proves Placed only through a readable observation. An
			// unreadable pass keeps the Placed verdict the last readable pass reached
			// for this same plan instead of retracting evidence it did not disprove.
			if err != nil && name == winner && candidate.AppliedPlanID == source.Status.Placement.Plan.ID && zeroHomeFloor(candidate.Allocation.CurrentHome) {
				winnerKeptPlaced = source.Status.Placement.Phase == v1beta1.PlacementPhasePlaced
			}
		}
		if candidate.Allocation != nil || (home.state != homeAbsent && !sourceHasPendingWinnerIdentity(source)) {
			res.candidates = append(res.candidates, candidate)
		}
		if winner == "" {
			if home.state != homeAbsent {
				res.phase = v1beta1.PlacementPhaseAdmitting
			}
			res.readinessUnknown = res.readinessUnknown || home.state == homeUnknown
			continue
		}
		if name != winner {
			continue
		}
		switch {
		case home.terminal:
			res.phase = v1beta1.PlacementPhaseFailed
		case home.state == homeAbsent:
			res.placementLost = true
		case home.state == homeUnknown:
			res.phase = source.Status.Placement.Phase
			res.readinessUnknown = true
			res.url = candidate.Endpoint
		case candidate.Phase == v1beta1.CandidatePhaseAdmitted || winnerIdle || winnerKeptPlaced:
			res.phase = v1beta1.PlacementPhasePlaced
			res.ready = home.serving
			res.url = candidate.Endpoint
		default:
			res.phase = v1beta1.PlacementPhaseAdmitting
		}
	}
	if reason != "" || (source.Status.Placement != nil && source.Status.Placement.Plan != nil) {
		if reason == "" {
			switch {
			case winner == "":
				reason = "AwaitingSingleWinner"
			case source.Status.Placement.Plan.SingleMove != nil:
				reason = "AwaitingMemberConvergence"
			case cleanupPending:
				reason = "AwaitingRaceCleanup"
			case !winnerApplied:
				reason = "AwaitingMemberConvergence"
			}
		}
		res.conditions = []policyCondition{splitProgressCondition(reason, message)}
	}
	return res
}
