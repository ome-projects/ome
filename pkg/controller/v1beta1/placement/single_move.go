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
	registered, matched, err := clusterRegistrations(clusters, standing)
	if err != nil {
		return out, err
	}
	if err := carryStandingPlan(source, &out); err != nil {
		return out, err
	}
	if err := registerCandidates(registered, &out); err != nil {
		return out, err
	}
	if out.InputDigest, err = singleMoveInputDigest(source, matched); err != nil {
		return out, err
	}
	if source.Status.Placement.Plan == nil {
		if err := r.adoptRetainedMember(ctx, source, &out); err != nil {
			return out, err
		}
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
	planning := singleMovePlanning{source: source, desired: desired, eligible: eligible, matched: matched, out: &out}
	for _, name := range slices.Sorted(maps.Keys(out.Assignments)) {
		if err := r.planMemberTarget(ctx, planning, name); err != nil {
			return out, err
		}
	}
	if out.SingleMove.Selected == out.Winner {
		if err := r.restoreSingleWinner(ctx, source, standing, eligible, desired, &out); err != nil {
			return out, err
		}
	}
	return out, nil
}

// clusterRegistrations indexes the registered clusters by name with their UIDs,
// and the subset the selector matches that is not deleting. A registration
// without a UID or a name, or a name registered twice, is refused.
func clusterRegistrations(clusters []v1beta1.WorkloadCluster, standing *placementObservations) (registered, matched map[string]types.UID, err error) {
	registered, matched = map[string]types.UID{}, map[string]types.UID{}
	for _, cluster := range clusters {
		if cluster.UID == "" || cluster.Name == "" || registered[cluster.Name] != "" {
			return nil, nil, fmt.Errorf("replacement requires distinct identified registrations")
		}
		registered[cluster.Name] = cluster.UID
		if standing.matches[cluster.Name] && cluster.DeletionTimestamp.IsZero() {
			matched[cluster.Name] = cluster.UID
		}
	}
	return registered, matched, nil
}

// carryStandingPlan copies the standing plan's move record and each
// candidate's allocation into the proposal. A plan of another mode or
// source, or a candidate without an allocation, is refused.
func carryStandingPlan(source *v1beta1.InferenceService, out *plan.Proposal) error {
	previous := source.Status.Placement
	if previous == nil || previous.Plan == nil {
		return nil
	}
	if previous.Plan.Mode != v1beta1.PlacementModeSingle || previous.Plan.SourceUID != source.UID {
		return fmt.Errorf("replacement requires accepted Single authority")
	}
	if previous.Plan.SingleMove != nil {
		out.SingleMove = previous.Plan.SingleMove.DeepCopy()
	}
	for _, c := range previous.Candidates {
		if c.Allocation == nil {
			return fmt.Errorf("standing home lacks allocation authority")
		}
		out.Assignments[c.Cluster] = *c.Allocation.DeepCopy()
	}
	return nil
}

// registerCandidates checks each registered cluster's standing assignment
// against the cluster's UID and adds a race candidate for each cluster
// without one; the chosen cluster joins as the member it is.
func registerCandidates(registered map[string]types.UID, out *plan.Proposal) error {
	for name, uid := range registered {
		if a, exists := out.Assignments[name]; exists {
			if a.ClusterUID != uid {
				return fmt.Errorf("member %q incarnation changed", name)
			}
		} else {
			out.Assignments[name] = v1beta1.CandidateAllocationStatus{ClusterUID: uid, RaceCandidate: name != out.Winner}
		}
	}
	return nil
}

// singleMoveInputDigest names the inputs the plan is made from: the source
// spec, its labels, the annotations that carry placement intent, and the
// matched clusters with their UIDs.
func singleMoveInputDigest(source *v1beta1.InferenceService, matched map[string]types.UID) (string, error) {
	encoded, err := json.Marshal(struct {
		Spec                v1beta1.InferenceServiceSpec
		Labels, Annotations map[string]string
		Matched             map[string]types.UID
	}{source.Spec, source.Labels, planIntentAnnotations(source.Annotations), matched})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// adoptRetainedMember inventories the chosen cluster before a plan exists:
// its standing copy becomes its current placement and its original count,
// and the copy is kept where it is. A cluster without a verified copy, or
// a copy with a zero minimum, is refused.
func (r *Reconciler) adoptRetainedMember(ctx context.Context, source *v1beta1.InferenceService, out *plan.Proposal) error {
	a := out.Assignments[out.Winner]
	if err := r.adoptAllModeMember(ctx, source, out.Winner, &a, out.InputDigest, true); err != nil {
		return err
	}
	if err := positiveMovementFloor(a.CurrentHome); err != nil {
		return err
	}
	if a.CurrentHome == nil || a.CurrentReplicas <= 0 {
		return fmt.Errorf("retained winner has no verified full home")
	}
	a.DesiredHome, a.DesiredReplicas = a.CurrentHome.DeepCopy(), a.CurrentReplicas
	a.RaceCandidate = false
	out.Assignments[out.Winner] = a
	return nil
}

// singleMovePlanning is what the per-cluster target decision reads: the
// source and its derived copy, the eligible and the matched clusters, and
// the proposal under construction.
type singleMovePlanning struct {
	source, desired *v1beta1.InferenceService
	eligible        []string
	matched         map[string]types.UID
	out             *plan.Proposal
}

// planMemberTarget decides the named cluster's target. A drained replacement
// with nothing left drops its placement record first. The selected chosen
// cluster keeps its placement; the chosen cluster, an unmatched cluster, a
// cluster other than the selected one and a drained replacement get none;
// any other matching cluster resolves the source's policy when eligible,
// else stays pending. A zero minimum on the current or the target placement
// is refused.
func (r *Reconciler) planMemberTarget(ctx context.Context, p singleMovePlanning, name string) error {
	a := p.out.Assignments[name]
	if err := r.dropDrainedPlacement(ctx, p.source, name, &a); err != nil {
		return err
	}
	a.Matched, a.HomeInputsPending = p.matched[name] != "", false
	selected := p.out.SingleMove.Selected
	switch {
	case selected == p.out.Winner && name == p.out.Winner:
		a.DesiredHome, a.DesiredReplicas = a.CurrentHome.DeepCopy(), a.CurrentReplicas
	case name == p.out.Winner || !a.Matched || (selected != "" && name != selected):
		a.DesiredHome, a.DesiredReplicas = nil, 0
	case a.CurrentReplicas == 0 && a.DrainRequested:
		a.DesiredHome, a.DesiredReplicas = nil, 0
	default:
		r.resolveMemberTarget(ctx, p, name, &a)
	}
	p.out.Assignments[name] = a
	for _, policy := range []*v1beta1.PlacementHomePolicy{a.CurrentHome, a.DesiredHome} {
		if err := positiveMovementFloor(policy); err != nil {
			return err
		}
	}
	return nil
}

// dropDrainedPlacement clears the placement record of a race candidate that
// drained to zero under a zero minimum once its cluster holds nothing of it;
// while resources remain, the candidate is refused until they are cleaned up.
func (r *Reconciler) dropDrainedPlacement(ctx context.Context, source *v1beta1.InferenceService, name string, a *v1beta1.CandidateAllocationStatus) error {
	if !a.RaceCandidate || a.CurrentReplicas != 0 || !a.DrainRequested || !hasZeroReplicaFloor(a.CurrentHome) {
		return nil
	}
	observed, err := r.observePlannedMember(ctx, source, v1beta1.CandidatePlacement{Cluster: name, Allocation: a}, declaredComponents(source))
	if err != nil {
		return err
	}
	if !observed.MemberState.Absent {
		return fmt.Errorf("zero-floor race loser %q still requires cleanup", name)
	}
	a.CurrentHome = nil
	return nil
}

// resolveMemberTarget resolves the source's policy on an eligible matching
// cluster as its target; a cluster outside the eligible set, or one whose
// resolution fails, keeps its target and stays pending.
func (r *Reconciler) resolveMemberTarget(ctx context.Context, p singleMovePlanning, name string, a *v1beta1.CandidateAllocationStatus) {
	a.HomeInputsPending = true
	if !slices.Contains(p.eligible, name) {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	policy, resolveErr := r.resolveFullPolicy(cctx, p.source, p.desired, name, a.ClusterUID, p.out.InputDigest)
	cancel()
	if resolveErr == nil {
		a.HomeInputsPending = false
		a.DesiredHome = policy
		a.DesiredReplicas, _ = protocol.ValidateReplicaFloors(policy.ReplicaFloors)
	}
}

// restoreSingleWinner resolves fresh creation authority after cancellation.
// A missing service alone cannot release a floor still backed by IRs or Pods.
func (r *Reconciler) restoreSingleWinner(ctx context.Context, source *v1beta1.InferenceService, standing *placementObservations, eligible []string, desired *v1beta1.InferenceService, proposal *plan.Proposal) error {
	a := proposal.Assignments[proposal.Winner]
	if a.OriginalReplicas <= 0 {
		return nil
	}
	if a.CurrentReplicas > 0 {
		member, known := standing.get(proposal.Winner)
		if !known || member.state != memberAbsent {
			return nil
		}
		observed, err := r.observePlannedMember(ctx, source, v1beta1.CandidatePlacement{Cluster: proposal.Winner, Allocation: &a}, declaredComponents(source))
		if err != nil {
			return err
		}
		if !observed.MemberState.Absent {
			return nil
		}
		a.CurrentReplicas, a.CurrentHome, a.DrainRequested = 0, nil, false
	}
	if !slices.Contains(eligible, proposal.Winner) {
		return fmt.Errorf("absent original home is ineligible for restoration")
	}
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	policy, err := r.resolveFullPolicy(cctx, source, desired, proposal.Winner, a.ClusterUID, proposal.InputDigest)
	cancel()
	if err != nil {
		return err
	}
	if err := positiveMovementFloor(policy); err != nil {
		return err
	}
	a.DesiredHome = policy
	a.DesiredReplicas, _ = protocol.ValidateReplicaFloors(policy.ReplicaFloors)
	proposal.Assignments[proposal.Winner] = a
	return nil
}

// singleRace is one pass's reading of the bounded replacement race: the
// step being built, the budget charged so far and what the members say.
type singleRace struct {
	step allocation.Step
	// original is the chosen cluster's count before the move, the base of
	// the budget; used is what every member holds or reserves.
	original, used int64
	// activeMinimum is the smallest serving minimum among the replacements
	// that hold pods; it bounds the next grant so any of them can win.
	activeMinimum int32
	allKnown      bool
	paused        bool
	// selected is the first admitted replacement at its target, in name order.
	selected string
}

// decided is the step with the reason the pass ends on.
func (r *singleRace) decided(reason string) allocation.Step {
	r.step.Reason = reason
	return r.step
}

// advanceSingleRace runs one pass of the bounded replacement race. An
// admitted replacement at its target is selected and the rest lose their
// targets. Otherwise the pass stops on an unknown member, an absent chosen
// cluster that still counts its pods, or a pause not yet acknowledged;
// drains the replacements that expired or lost their target; and grants
// the budget to the next candidates after the cursor.
func advanceSingleRace(source *v1beta1.InferenceService, observations map[string]plannedMemberObservation, proposal *plan.Proposal, now time.Time) (allocation.Step, error) {
	race := readSingleRace(observations, proposal)
	switch {
	case race.selected != "":
		selectReplacement(proposal, race.selected)
		return race.decided("ReplacementNotReady"), nil
	case !race.allKnown:
		return race.decided("ObservationUnknown"), nil
	case chosenClusterServedAndAbsent(observations, proposal):
		race.step.Targets[proposal.Winner] = 0
		return race.decided("AwaitingMemberConvergence"), nil
	case !race.paused:
		return race.decided("AwaitingSurgePause"), nil
	}
	releaseAndDrainReplacements(source, observations, proposal, &race, now)
	if len(race.step.Drain) > 0 {
		return race.decided("AwaitingMemberCleanup"), nil
	}
	grantReplacements(observations, proposal, &race, now)
	return race.step, nil
}

// readSingleRace reads every member once, in name order: the step's targets
// are the current counts, the budget is charged with what each member holds
// or reserves, and the first admitted replacement at its target is selected.
func readSingleRace(observations map[string]plannedMemberObservation, proposal *plan.Proposal) singleRace {
	race := singleRace{step: allocation.Step{Targets: map[string]int32{}, Reason: "AwaitingReplacementAdmission"}, allKnown: true, paused: true}
	for _, name := range slices.Sorted(maps.Keys(proposal.Assignments)) {
		a, o := proposal.Assignments[name], observations[name]
		race.step.Targets[name] = a.CurrentReplicas
		race.original += int64(a.OriginalReplicas)
		race.used += int64(max(a.CurrentReplicas, o.MemberState.Occupied)) + int64(o.RolloutReserved)
		race.allKnown = race.allKnown && o.MemberState.Known
		race.paused = race.paused && (o.MemberState.Absent || o.PauseAcknowledged)
		if name == proposal.Winner || a.CurrentReplicas <= 0 || a.DesiredReplicas <= 0 || a.DrainRequested {
			continue
		}
		if race.activeMinimum == 0 || a.CurrentReplicas < race.activeMinimum {
			race.activeMinimum = a.CurrentReplicas
		}
		if race.selected == "" && admittedAtTarget(a, o) {
			race.selected = name
		}
	}
	return race
}

// admittedAtTarget reports a replacement that is admitted, applied and
// eligible, not terminal, with its inputs resolved and serving the
// placement its target names.
func admittedAtTarget(a v1beta1.CandidateAllocationStatus, o plannedMemberObservation) bool {
	return o.MemberState.Known && o.MemberState.Applied && o.MemberState.Eligible && !o.Terminal &&
		o.Candidate.Phase == v1beta1.CandidatePhaseAdmitted && !a.HomeInputsPending &&
		equality.Semantic.DeepEqual(a.CurrentHome, a.DesiredHome)
}

// selectReplacement records the selected replacement: it keeps its pods out
// of the race's deletions, and every other member loses its target.
func selectReplacement(proposal *plan.Proposal, selected string) {
	proposal.SingleMove.Selected = selected
	for name, a := range proposal.Assignments {
		if name == selected {
			a.RaceCandidate = false
		} else {
			a.DesiredHome, a.DesiredReplicas = nil, 0
		}
		proposal.Assignments[name] = a
	}
}

// chosenClusterServedAndAbsent reports a chosen cluster that served before
// the move, still counts its pods and is observed absent: its count is
// released before any replacement grows.
func chosenClusterServedAndAbsent(observations map[string]plannedMemberObservation, proposal *plan.Proposal) bool {
	chosen := proposal.Assignments[proposal.Winner]
	return chosen.OriginalReplicas > 0 && chosen.CurrentReplicas > 0 && observations[proposal.Winner].MemberState.Absent
}

// releaseAndDrainReplacements walks the replacements: one at zero and
// absent forgets its placement; one holding pods whose timeout passed,
// whose target is gone or whose resolved target changed is drained and
// loses its target.
func releaseAndDrainReplacements(source *v1beta1.InferenceService, observations map[string]plannedMemberObservation, proposal *plan.Proposal, race *singleRace, now time.Time) {
	for name, a := range proposal.Assignments {
		o := observations[name]
		if name == proposal.Winner {
			continue
		}
		if a.CurrentReplicas == 0 && o.MemberState.Absent {
			a.CurrentHome, a.DrainRequested = nil, false
			a.ReplacementStartedAt = nil
			proposal.Assignments[name] = a
			continue
		}
		if a.RaceCandidate && a.CurrentReplicas > 0 && (replacementExpired(source, a, now) || a.DesiredReplicas == 0 || (!a.HomeInputsPending && !equality.Semantic.DeepEqual(a.CurrentHome, a.DesiredHome))) {
			race.step.Targets[name] = 0
			race.step.Drain = append(race.step.Drain, name)
			a.DesiredHome, a.DesiredReplicas = nil, 0
			proposal.Assignments[name] = a
		}
	}
}

// replacementExpired reports a replacement that started longer ago than
// the service's replacement timeout.
func replacementExpired(source *v1beta1.InferenceService, a v1beta1.CandidateAllocationStatus, now time.Time) bool {
	timeout := source.Spec.Placement.ReplacementTimeout
	return timeout != nil && a.ReplacementStartedAt != nil && !now.Before(a.ReplacementStartedAt.Add(timeout.Duration))
}

// grantOrder is the members in name order starting after the cursor, so
// each pass offers the budget to the next candidate.
func grantOrder(proposal *plan.Proposal) []string {
	names := slices.Sorted(maps.Keys(proposal.Assignments))
	index := 0
	for i, name := range names {
		if name <= proposal.SingleMove.Cursor {
			index = i + 1
		}
	}
	return append(slices.Clone(names[index:]), names[:index]...)
}

// awaitsGrant reports a replacement with a resolved target and no pods, not
// draining, whose cluster is eligible and verified empty.
func awaitsGrant(a v1beta1.CandidateAllocationStatus, o plannedMemberObservation) bool {
	return a.DesiredReplicas > 0 && a.CurrentReplicas <= 0 && !a.HomeInputsPending && !a.DrainRequested &&
		o.MemberState.Eligible && o.MemberState.Absent && a.DesiredHome != nil
}

// grantReplacements offers the budget to each replacement awaiting a grant,
// after the cursor. One that fits the chosen cluster's count plus one
// replacement starts now and moves the cursor; one that does not leaves
// the step at SurgeBudgetExhausted.
func grantReplacements(observations map[string]plannedMemberObservation, proposal *plan.Proposal, race *singleRace, now time.Time) {
	for _, name := range grantOrder(proposal) {
		a, o := proposal.Assignments[name], observations[name]
		if name == proposal.Winner || !awaitsGrant(a, o) {
			continue
		}
		minimum := a.DesiredReplicas
		if race.activeMinimum > 0 {
			minimum = min(minimum, race.activeMinimum)
		}
		// The chosen cluster's count plus one full replacement, so a serving
		// original races one replacement at a time.
		limit := max(race.original, int64(minimum)) + int64(minimum)
		if race.used+int64(a.DesiredReplicas) > limit {
			race.step.Reason = "SurgeBudgetExhausted"
			continue
		}
		race.step.Targets[name] = a.DesiredReplicas
		started := metav1.NewMicroTime(now.UTC().Truncate(time.Microsecond))
		a.ReplacementStartedAt = &started
		proposal.Assignments[name] = a
		race.used += int64(a.DesiredReplicas)
		race.activeMinimum = minimum
		proposal.SingleMove.Cursor = name
		race.step.Reason = "AwaitingMemberConvergence"
	}
}
