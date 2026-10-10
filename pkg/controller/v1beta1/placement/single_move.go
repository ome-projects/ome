package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func (r *Reconciler) reconcileSingleMove(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string, standing *placementObservations) (ctrl.Result, error) {
	active := source.Status.Placement != nil && source.Status.Placement.Plan != nil && source.Status.Placement.Plan.SingleMove != nil
	if !active && source.Status.Placement != nil && source.Status.Placement.Plan != nil {
		r.sweepPlannedRace(ctx, source)
	}
	if handled, result, err := r.refreshSingleMovementFloor(ctx, source, clusters, standing); handled {
		return result, err
	}
	if active {
		if r.singleMoveEmpty(ctx, source, clusters, standing) {
			if r.graceRemaining(source.UID, time.Now()) > 0 || len(eligible) == 0 {
				return r.writeSinglePlanStatus(ctx, source, standing, "AwaitingWinnerRecovery", "")
			}
			store := plan.Store{Client: r.Client, Reader: r.APIReader}
			reset, err := store.Persist(ctx, source, emptySingleMove(source))
			if err != nil {
				return r.writeSplitHold(ctx, source, standing, "PlanNotPersisted", err)
			}
			r.clearGrace(source.UID)
			return r.reconcileSinglePlanned(ctx, reset, clusters, eligible, standing)
		}
		r.clearGrace(source.UID)
	}
	proposal, err := r.singleMoveProposal(ctx, source, clusters, eligible, standing)
	if err != nil {
		if errors.Is(err, errPositiveMovementFloor) {
			return r.writeSplitHold(ctx, source, standing, "PositiveFloorRequired", err)
		}
		return r.writeSplitHold(ctx, source, standing, "AwaitingHomeInputs", err)
	}
	return r.executePlannedAllocation(ctx, source, eligible, standing, proposal, nil)
}

// singleMoveProposal keeps the original floor across probing and handoff.
// An admitted replacement is selected before its readiness authorizes removal.
func (r *Reconciler) singleMoveProposal(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string, standing *placementObservations) (plan.Proposal, error) {
	out := plan.Proposal{Mode: v1beta1.PlacementModeSingle, Winner: winnerCluster(source), PauseSurge: true, SingleMove: &v1beta1.PlacementSingleMoveStatus{}, Assignments: map[string]v1beta1.CandidateAllocationStatus{}}
	if out.Winner == "" {
		return out, fmt.Errorf("bounded replacement requires a retained winner")
	}
	registered, matched := map[string]types.UID{}, map[string]types.UID{}
	for _, cluster := range clusters {
		if cluster.UID == "" || cluster.Name == "" || registered[cluster.Name] != "" {
			return out, fmt.Errorf("replacement requires distinct identified registrations")
		}
		registered[cluster.Name] = cluster.UID
		if standing.matches[cluster.Name] && cluster.DeletionTimestamp.IsZero() {
			matched[cluster.Name] = cluster.UID
		}
	}
	if previous := source.Status.Placement; previous != nil && previous.Plan != nil {
		if previous.Plan.Mode != v1beta1.PlacementModeSingle || previous.Plan.SourceUID != source.UID {
			return out, fmt.Errorf("replacement requires accepted Single authority")
		}
		if previous.Plan.SingleMove != nil {
			out.SingleMove = previous.Plan.SingleMove.DeepCopy()
		}
		for _, c := range previous.Candidates {
			if c.Allocation == nil {
				return out, fmt.Errorf("standing home lacks allocation authority")
			}
			out.Assignments[c.Cluster] = *c.Allocation.DeepCopy()
		}
	}
	for name, uid := range registered {
		if a, exists := out.Assignments[name]; exists {
			if a.ClusterUID != uid {
				return out, fmt.Errorf("member %q incarnation changed", name)
			}
		} else {
			out.Assignments[name] = v1beta1.CandidateAllocationStatus{ClusterUID: uid, RaceCandidate: name != out.Winner}
		}
	}
	encoded, err := json.Marshal(struct {
		Spec                v1beta1.InferenceServiceSpec
		Labels, Annotations map[string]string
		Matched             map[string]types.UID
	}{source.Spec, source.Labels, source.Annotations, matched})
	if err != nil {
		return out, err
	}
	digest := sha256.Sum256(encoded)
	out.InputDigest = hex.EncodeToString(digest[:])
	if source.Status.Placement.Plan == nil {
		a := out.Assignments[out.Winner]
		err = r.adoptAllHome(ctx, source, out.Winner, &a, out.InputDigest, true)
		if err != nil {
			return out, err
		}
		if err := positiveMovementFloor(a.CurrentHome); err != nil {
			return out, err
		}
		if a.CurrentHome == nil || a.CurrentReplicas <= 0 {
			return out, fmt.Errorf("retained winner has no verified full home")
		}
		a.DesiredHome, a.DesiredReplicas = a.CurrentHome.DeepCopy(), a.CurrentReplicas
		a.RaceCandidate = false
		out.Assignments[out.Winner] = a
	}
	// Finish an accepted handoff before another move. Cancelling an uncommitted
	// replacement first restores the current winner and drains the probe.
	if matched[out.Winner] != "" || (out.SingleMove.Selected != "" && matched[out.SingleMove.Selected] == "") {
		out.SingleMove.Selected = out.Winner
	}
	desired, err := r.derivedFor(source)
	if err != nil {
		return out, err
	}
	for _, name := range slices.Sorted(maps.Keys(out.Assignments)) {
		a := out.Assignments[name]
		if a.RaceCandidate && a.CurrentReplicas == 0 && a.DrainRequested && zeroHomeFloor(a.CurrentHome) {
			observed, err := r.observePlannedHome(ctx, source, v1beta1.CandidatePlacement{Cluster: name, Allocation: &a}, declaredComponents(source))
			if err != nil {
				return out, err
			}
			if !observed.Home.Absent {
				return out, fmt.Errorf("zero-floor race loser %q still requires cleanup", name)
			}
			a.CurrentHome = nil
		}
		a.Matched, a.HomeInputsPending = matched[name] != "", false
		selected := out.SingleMove.Selected
		switch {
		case selected == out.Winner && name == out.Winner:
			a.DesiredHome, a.DesiredReplicas = a.CurrentHome.DeepCopy(), a.CurrentReplicas
		case name == out.Winner || !a.Matched || (selected != "" && name != selected):
			a.DesiredHome, a.DesiredReplicas = nil, 0
		case a.CurrentReplicas == 0 && a.DrainRequested:
			a.DesiredHome, a.DesiredReplicas = nil, 0
		default:
			a.HomeInputsPending = true
			if slices.Contains(eligible, name) {
				cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
				home, resolveErr := r.resolveFullHome(cctx, source, desired, name, a.ClusterUID, out.InputDigest)
				cancel()
				if resolveErr == nil {
					a.HomeInputsPending = false
					a.DesiredHome = home
					a.DesiredReplicas, _ = protocol.ValidateReplicaFloors(home.ReplicaFloors)
				}
			}
		}
		out.Assignments[name] = a
		for _, home := range []*v1beta1.PlacementHomePolicy{a.CurrentHome, a.DesiredHome} {
			if err := positiveMovementFloor(home); err != nil {
				return out, err
			}
		}
	}
	if out.SingleMove.Selected == out.Winner {
		if err := r.restoreSingleWinner(ctx, source, standing, eligible, desired, &out); err != nil {
			return out, err
		}
	}
	return out, nil
}

// restoreSingleWinner resolves fresh creation authority after cancellation.
// A missing service alone cannot release a floor still backed by IRs or Pods.
func (r *Reconciler) restoreSingleWinner(ctx context.Context, source *v1beta1.InferenceService, standing *placementObservations, eligible []string, desired *v1beta1.InferenceService, proposal *plan.Proposal) error {
	a := proposal.Assignments[proposal.Winner]
	if a.OriginalReplicas <= 0 {
		return nil
	}
	if a.CurrentReplicas > 0 {
		home, known := standing.get(proposal.Winner)
		if !known || home.state != homeAbsent {
			return nil
		}
		observed, err := r.observePlannedHome(ctx, source, v1beta1.CandidatePlacement{Cluster: proposal.Winner, Allocation: &a}, declaredComponents(source))
		if err != nil {
			return err
		}
		if !observed.Home.Absent {
			return nil
		}
		a.CurrentReplicas, a.CurrentHome, a.DrainRequested = 0, nil, false
	}
	if !slices.Contains(eligible, proposal.Winner) {
		return fmt.Errorf("absent original home is ineligible for restoration")
	}
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	home, err := r.resolveFullHome(cctx, source, desired, proposal.Winner, a.ClusterUID, proposal.InputDigest)
	cancel()
	if err != nil {
		return err
	}
	if err := positiveMovementFloor(home); err != nil {
		return err
	}
	a.DesiredHome = home
	a.DesiredReplicas, _ = protocol.ValidateReplicaFloors(home.ReplicaFloors)
	proposal.Assignments[proposal.Winner] = a
	return nil
}

// advanceSingleRace charges every occupied home and rollout reservation. The
// smallest active probe floor bounds the budget so any admitted probe can win.
func advanceSingleRace(source *v1beta1.InferenceService, observations map[string]plannedHomeObservation, proposal *plan.Proposal, now time.Time) (allocation.Step, error) {
	step := allocation.Step{Targets: map[string]int32{}, Reason: "AwaitingReplacementAdmission"}
	var original, used int64
	var activeFloor int32
	allKnown, paused := true, true
	selected := ""
	for _, name := range slices.Sorted(maps.Keys(proposal.Assignments)) {
		a, o := proposal.Assignments[name], observations[name]
		step.Targets[name] = a.CurrentReplicas
		original += int64(a.OriginalReplicas)
		used += int64(max(a.CurrentReplicas, o.Home.Occupied)) + int64(o.RolloutReserved)
		allKnown = allKnown && o.Home.Known
		paused = paused && (o.Home.Absent || o.PauseAcknowledged)
		if name == proposal.Winner || a.CurrentReplicas <= 0 || a.DesiredReplicas <= 0 || a.DrainRequested {
			continue
		}
		if activeFloor == 0 || a.CurrentReplicas < activeFloor {
			activeFloor = a.CurrentReplicas
		}
		if selected == "" && o.Home.Known && o.Home.Applied && o.Home.Eligible && !o.Terminal && o.Candidate.Phase == v1beta1.CandidatePhaseAdmitted && !a.HomeInputsPending && equality.Semantic.DeepEqual(a.CurrentHome, a.DesiredHome) {
			selected = name
		}
	}
	if selected != "" {
		proposal.SingleMove.Selected = selected
		for name, a := range proposal.Assignments {
			if name == selected {
				a.RaceCandidate = false
			} else {
				a.DesiredHome, a.DesiredReplicas = nil, 0
			}
			proposal.Assignments[name] = a
		}
		step.Reason = "ReplacementNotReady"
		return step, nil
	}
	if !allKnown {
		step.Reason = "ObservationUnknown"
		return step, nil
	}
	winner := proposal.Assignments[proposal.Winner]
	if winner.OriginalReplicas > 0 && winner.CurrentReplicas > 0 && observations[proposal.Winner].Home.Absent {
		step.Targets[proposal.Winner] = 0
		step.Reason = "AwaitingMemberConvergence"
		return step, nil
	}
	if !paused {
		step.Reason = "AwaitingSurgePause"
		return step, nil
	}
	for name, a := range proposal.Assignments {
		o := observations[name]
		if name == proposal.Winner {
			continue
		}
		if a.CurrentReplicas == 0 && o.Home.Absent {
			a.CurrentHome, a.DrainRequested = nil, false
			a.ReplacementStartedAt = nil
			proposal.Assignments[name] = a
			continue
		}
		expired := source.Spec.Placement.ReplacementTimeout != nil && a.ReplacementStartedAt != nil && !now.Before(a.ReplacementStartedAt.Add(source.Spec.Placement.ReplacementTimeout.Duration))
		if a.RaceCandidate && a.CurrentReplicas > 0 && (expired || a.DesiredReplicas == 0 || (!a.HomeInputsPending && !equality.Semantic.DeepEqual(a.CurrentHome, a.DesiredHome))) {
			step.Targets[name] = 0
			step.Drain = append(step.Drain, name)
			a.DesiredHome, a.DesiredReplicas = nil, 0
			proposal.Assignments[name] = a
		}
	}
	if len(step.Drain) > 0 {
		step.Reason = "AwaitingMemberCleanup"
		return step, nil
	}
	names := slices.Sorted(maps.Keys(proposal.Assignments))
	index := 0
	for i, name := range names {
		if name <= proposal.SingleMove.Cursor {
			index = i + 1
		}
	}
	names = append(slices.Clone(names[index:]), names[:index]...)
	for _, name := range names {
		a, o := proposal.Assignments[name], observations[name]
		if name == proposal.Winner || a.DesiredReplicas <= 0 || a.CurrentReplicas > 0 || a.HomeInputsPending || a.DrainRequested || !o.Home.Eligible || !o.Home.Absent || a.DesiredHome == nil {
			continue
		}
		floor := a.DesiredReplicas
		if activeFloor > 0 {
			floor = min(floor, activeFloor)
		}
		// The original floor plus one full probe, so a serving original races
		// one replacement at a time.
		limit := max(original, int64(floor)) + int64(floor)
		if used+int64(a.DesiredReplicas) > limit {
			step.Reason = "SurgeBudgetExhausted"
			continue
		}
		step.Targets[name] = a.DesiredReplicas
		started := metav1.NewMicroTime(now.UTC().Truncate(time.Microsecond))
		a.ReplacementStartedAt = &started
		proposal.Assignments[name] = a
		used += int64(a.DesiredReplicas)
		activeFloor = floor
		proposal.SingleMove.Cursor = name
		step.Reason = "AwaitingMemberConvergence"
	}
	return step, nil
}
