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

// reconcileSinglePlanned commits race nominations and the observed chosen
// cluster before member writes. Race cleanup is distinct from moving a
// retained member.
func (r *Reconciler) reconcileSinglePlanned(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, eligible []string, observations *placementObservations) (ctrl.Result, error) {
	if source.Status.Placement != nil && source.Status.Placement.Plan != nil && source.Status.Placement.Plan.SingleMove != nil {
		return r.reconcileSingleMove(ctx, source, clusters, eligible, observations)
	}
	standing := r.readChosenCluster(ctx, source, eligible, observations)
	if standing.moves {
		return r.reconcileSingleMove(ctx, source, clusters, eligible, observations)
	}
	if standing.held {
		return r.writeSinglePlanStatus(ctx, source, observations, standing.reason, "")
	}
	chosen := standing.chosen
	nominated, roundWait := r.singleNominations(source, eligible, chosen)
	proposal, err := r.singleProposal(ctx, source, clusters, chosen, nominated)
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
	applied := r.applySingleNominees(ctx, accepted, nominated, chosen, observations)
	if chosen == "" {
		accepted, chosen, err = r.commitElectedMember(ctx, store, accepted, proposal, applied, observations)
		if err != nil {
			r.Log.Error(err, "Single winner persistence held")
			return r.writeSinglePlanStatus(ctx, accepted, observations, "WinnerNotPersisted", "")
		}
	}
	if chosen != "" {
		r.sweepPlannedRace(ctx, accepted)
	}
	waiting := singleRaceWaiting(chosen, nominated, applied, observations)
	result, err := r.writePlacement(ctx, accepted, r.singlePlanStatus(ctx, accepted, observations, "", ""))
	// A materialized race waits for member events or the configured backstop.
	// Failed writes, unknown observations and stale snapshots retry promptly.
	if !waiting && result.RequeueAfter > 0 {
		result.RequeueAfter = r.requeue()
	}
	if chosen == "" && roundWait > 0 {
		result.RequeueAfter = min(result.RequeueAfter, roundWait)
	}
	return result, err
}

// singleStanding is what the chosen cluster's observation decides for the
// pass: keep it, reopen the race without one (chosen empty), hold the pass
// with a reason, or move it because it stopped matching the affinity.
type singleStanding struct {
	chosen string
	held   bool
	reason string
	moves  bool
}

// readChosenCluster observes the plan's chosen cluster. One that cannot be
// read or is terminal holds the pass without a reason. One whose member is
// gone or no longer admitted holds until the loss grace ends and some cluster
// is eligible, then the race reopens. A retained one that stopped matching
// moves; one that is not eligible holds for its inputs.
func (r *Reconciler) readChosenCluster(ctx context.Context, source *v1beta1.InferenceService, eligible []string, observations *placementObservations) singleStanding {
	chosen := winnerCluster(source)
	if chosen == "" {
		return singleStanding{}
	}
	member, ok := observations.get(chosen)
	if !ok {
		member = observations.refresh(ctx, r, source, chosen)
	}
	if member.state == memberUnknown || member.terminal {
		return singleStanding{held: true}
	}
	retainZero, err := r.retainZeroFloorWinner(ctx, source, chosen)
	if err != nil {
		r.Log.Error(err, "planned member observation failed", "cluster", chosen)
		return singleStanding{held: true, reason: "AwaitingMemberConvergence"}
	}
	if member.state == memberAbsent || (member.candidate.Phase != v1beta1.CandidatePhaseAdmitted && !retainZero) {
		if r.graceRemaining(source.UID, time.Now()) > 0 || len(eligible) == 0 {
			return singleStanding{held: true, reason: "AwaitingWinnerRecovery"}
		}
		r.clearGrace(source.UID)
		return singleStanding{}
	}
	r.clearGrace(source.UID)
	if !observations.matches[chosen] {
		return singleStanding{chosen: chosen, moves: true}
	}
	if !slices.Contains(eligible, chosen) {
		return singleStanding{held: true, reason: "AwaitingHomeInputs"}
	}
	return singleStanding{chosen: chosen}
}

// singleNominations names the members this pass writes: the chosen cluster
// alone, or the dispatcher's nominees plus every race candidate the accepted
// race already holds, so a restart keeps its nominations.
func (r *Reconciler) singleNominations(source *v1beta1.InferenceService, eligible []string, chosen string) ([]string, time.Duration) {
	if chosen != "" {
		return []string{chosen}, 0
	}
	nominated, roundWait := r.nominate().Nominate(source.UID, eligible, time.Now())
	if source.Status.Placement != nil && source.Status.Placement.Plan != nil && source.Status.Placement.Plan.Mode == v1beta1.PlacementModeSingle && source.Status.Placement.Plan.Winner == "" {
		for _, candidate := range source.Status.Placement.Candidates {
			if candidate.Allocation != nil && candidate.Allocation.RaceCandidate && candidate.Allocation.DesiredHome != nil && slices.Contains(eligible, candidate.Cluster) && !slices.Contains(nominated, candidate.Cluster) {
				nominated = append(nominated, candidate.Cluster)
			}
		}
		slices.Sort(nominated)
	}
	return nominated, roundWait
}

// applySingleNominees writes the accepted allocation onto every nominated
// member that holds a policy and is neither draining nor waiting for its
// inputs, refreshes each member's observation after the write, and reports
// which writes landed.
func (r *Reconciler) applySingleNominees(ctx context.Context, accepted *v1beta1.InferenceService, nominated []string, chosen string, observations *placementObservations) map[string]bool {
	applied := map[string]bool{}
	for _, candidate := range accepted.Status.Placement.Candidates {
		a := candidate.Allocation
		if a.CurrentHome == nil || a.DrainRequested || a.HomeInputsPending || !slices.Contains(nominated, candidate.Cluster) {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		err := r.placePlannedMember(cctx, accepted, candidate, chosen != "", nil)
		cancel()
		if err != nil {
			if isMemberWriteRace(err) {
				r.Log.V(1).Info("Single member application conflicted; retrying", "cluster", candidate.Cluster)
			} else {
				r.Log.Error(err, "Single member application held", "cluster", candidate.Cluster)
			}
			observations.refresh(ctx, r, accepted, candidate.Cluster)
			continue
		}
		applied[candidate.Cluster] = true
		observations.refresh(ctx, r, accepted, candidate.Cluster)
	}
	return applied
}

// commitElectedMember persists the first applied member, by name, that is
// present, not terminal and admitted as the plan's chosen cluster: it keeps
// its replica minimums and every other member drains. The accepted service
// and the chosen name come back unchanged when no applied member qualifies.
func (r *Reconciler) commitElectedMember(ctx context.Context, store plan.Store, accepted *v1beta1.InferenceService, proposal plan.Proposal, applied map[string]bool, observations *placementObservations) (*v1beta1.InferenceService, string, error) {
	for _, name := range slices.Sorted(maps.Keys(applied)) {
		member := observations.members[name]
		if member.state != memberPresent || member.terminal || member.candidate.Phase != v1beta1.CandidatePhaseAdmitted {
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
			return accepted, "", err
		}
		return selected, name, nil
	}
	return accepted, "", nil
}

// singleRaceWaiting reports a race whose every nominee was written and is
// present: the pass then waits for member events or the configured backstop
// instead of retrying promptly.
func singleRaceWaiting(chosen string, nominated []string, applied map[string]bool, observations *placementObservations) bool {
	if chosen != "" || len(nominated) == 0 {
		return false
	}
	for _, name := range nominated {
		if !applied[name] || observations.members[name].state != memberPresent {
			return false
		}
	}
	return true
}

// singleProposal builds the Single plan for the nominated clusters: every
// identified registration gets an assignment, the standing assignments
// carry over, and each nominee's policy is resolved against its member.
func (r *Reconciler) singleProposal(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, chosen string, nominated []string) (plan.Proposal, error) {
	out := plan.Proposal{Mode: v1beta1.PlacementModeSingle, Winner: chosen, Assignments: map[string]v1beta1.CandidateAllocationStatus{}}
	registered, err := singleRegistrations(clusters)
	if err != nil {
		return out, err
	}
	if err := carryStandingSingleAssignments(source, out.Assignments); err != nil {
		return out, err
	}
	// Every registered copy is accounted for before the race cleanup. An
	// unobserved registration grants no positive workload authority.
	for name, uid := range registered {
		if _, exists := out.Assignments[name]; !exists {
			out.Assignments[name] = v1beta1.CandidateAllocationStatus{ClusterUID: uid, RaceCandidate: name != chosen}
		}
	}
	targets, err := singleTargets(nominated, registered, out.Assignments)
	if err != nil {
		return out, err
	}
	out.InputDigest, err = singleInputDigest(source, targets, chosen)
	if err != nil {
		return out, err
	}
	desired, err := r.derivedFor(source)
	if err != nil {
		return out, err
	}
	for name, a := range out.Assignments {
		a, err = r.resolveSingleAssignment(ctx, source, desired, name, chosen, a, targets, out.InputDigest)
		if err != nil {
			return out, err
		}
		out.Assignments[name] = a
	}
	return out, nil
}

// singleRegistrations maps each cluster to its UID; a nameless, unidentified
// or repeated registration refuses the whole plan.
func singleRegistrations(clusters []v1beta1.WorkloadCluster) (map[string]types.UID, error) {
	registered := map[string]types.UID{}
	for _, cluster := range clusters {
		if cluster.Name == "" || cluster.UID == "" || registered[cluster.Name] != "" {
			return nil, fmt.Errorf("Single placement requires distinct identified registrations")
		}
		registered[cluster.Name] = cluster.UID
	}
	return registered, nil
}

// carryStandingSingleAssignments copies the accepted plan's allocations into
// assignments. Only an unpaused Single plan of this source may enter a race.
func carryStandingSingleAssignments(source *v1beta1.InferenceService, assignments map[string]v1beta1.CandidateAllocationStatus) error {
	previous := source.Status.Placement
	if previous == nil || previous.Plan == nil {
		return nil
	}
	if previous.Plan.Mode != v1beta1.PlacementModeSingle || previous.Plan.SourceUID != source.UID || previous.Plan.PauseSurge {
		return fmt.Errorf("standing authority cannot enter an unpaused Single race")
	}
	for _, candidate := range previous.Candidates {
		if candidate.Allocation == nil {
			return fmt.Errorf("standing Single home lacks allocation authority")
		}
		assignments[candidate.Cluster] = *candidate.Allocation.DeepCopy()
	}
	return nil
}

// singleTargets maps each nominee to the UID its assignment was accepted
// with; a nominee whose registration changed identity refuses the plan.
func singleTargets(nominated []string, registered map[string]types.UID, assignments map[string]v1beta1.CandidateAllocationStatus) (map[string]types.UID, error) {
	targets := map[string]types.UID{}
	for _, name := range nominated {
		if name == "" {
			continue
		}
		if registered[name] == "" || assignments[name].ClusterUID != registered[name] {
			return nil, fmt.Errorf("Single target %q incarnation is unverified", name)
		}
		targets[name] = registered[name]
	}
	return targets, nil
}

// singleInputDigest names the plan's inputs: the spec, the labels, the
// intent annotations, the targets and the chosen cluster.
func singleInputDigest(source *v1beta1.InferenceService, targets map[string]types.UID, chosen string) (string, error) {
	encoded, err := json.Marshal(struct {
		Spec                v1beta1.InferenceServiceSpec
		Labels, Annotations map[string]string
		Targets             map[string]types.UID
		Winner              string
	}{source.Spec, source.Labels, planIntentAnnotations(source.Annotations), targets, chosen})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// resolveSingleAssignment settles one cluster's assignment. A target takes
// the policy resolved on its member, or waits for its inputs when the
// resolution fails; the chosen cluster's failure refuses the plan. A cluster
// left without a policy drains whatever it held.
func (r *Reconciler) resolveSingleAssignment(ctx context.Context, source, desired *v1beta1.InferenceService, name, chosen string, a v1beta1.CandidateAllocationStatus, targets map[string]types.UID, digest string) (v1beta1.CandidateAllocationStatus, error) {
	a.Matched = targets[name] != ""
	a.DesiredHome = nil
	a.DesiredReplicas = 0
	a.HomeInputsPending = false
	if chosen == "" {
		a.RaceCandidate = true
	}
	if targets[name] != "" {
		var err error
		if a, err = r.resolveSingleTarget(ctx, source, desired, name, chosen, a, digest); err != nil {
			return a, err
		}
	}
	if a.DesiredHome == nil {
		a.CurrentReplicas = 0
		a.DrainRequested = a.CurrentHome != nil || chosen != ""
	}
	return a, nil
}

// resolveSingleTarget resolves a nominee's policy on its member. A failure
// refuses the plan when the nominee is the chosen cluster and leaves any
// other nominee waiting for its inputs.
func (r *Reconciler) resolveSingleTarget(ctx context.Context, source, desired *v1beta1.InferenceService, name, chosen string, a v1beta1.CandidateAllocationStatus, digest string) (v1beta1.CandidateAllocationStatus, error) {
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	member, err := r.resolveFullPolicy(cctx, source, desired, name, a.ClusterUID, digest)
	cancel()
	if err != nil {
		if name == chosen {
			return a, fmt.Errorf("home policy is unresolved on %s: %w", name, err)
		}
		a.HomeInputsPending = true
		return a, nil
	}
	a.CurrentHome, a.DesiredHome = member.DeepCopy(), member
	a.CurrentReplicas, _ = protocol.ValidateReplicaFloors(member.ReplicaFloors)
	a.DesiredReplicas = a.CurrentReplicas
	a.DrainRequested = false
	if name == chosen {
		a.OriginalReplicas = a.CurrentReplicas
		a.RaceCandidate = false
	}
	return a, nil
}

// sweepPlannedRace uses only persisted race-loser permission. A retained member
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

// singleStatusFlags are the facts the Single status sums over its members
// and decides its reason from.
type singleStatusFlags struct {
	cleanupPending   bool // a race candidate still holds resources or could not be read
	chosenApplied    bool // the chosen cluster acknowledged the accepted plan in this pass
	chosenIdle       bool // the applied chosen cluster idles at a zero minimum
	chosenKeptPlaced bool // an unreadable chosen cluster keeps the last readable pass's Placed phase
}

// singlePlanStatus reads every member the plan names and sums them into the
// status: the rows to publish, the phase the chosen cluster earns, and the
// reason the plan has not converged.
func (r *Reconciler) singlePlanStatus(ctx context.Context, source *v1beta1.InferenceService, observations *placementObservations, reason, message string) placementResult {
	chosen := winnerCluster(source)
	res := placementResult{winner: chosen, phase: v1beta1.PlacementPhasePending}
	flags := &singleStatusFlags{}
	for _, name := range singleStatusNames(source, observations, chosen) {
		candidate, member := r.singleMemberCandidate(ctx, source, observations, name)
		if candidate.Allocation != nil {
			r.observeSingleAllocation(ctx, source, name, chosen, &candidate, flags)
		}
		recordSingleMember(&res, source, name, chosen, candidate, member, flags)
	}
	if reason != "" || (source.Status.Placement != nil && source.Status.Placement.Plan != nil) {
		res.conditions = []policyCondition{splitProgressCondition(singleStatusReason(source, chosen, reason, flags), message)}
	}
	return res
}

// singleStatusNames lists the members the status reports: the accepted
// plan's candidates, else the standing members, with the chosen cluster
// always among them.
func singleStatusNames(source *v1beta1.InferenceService, observations *placementObservations, chosen string) []string {
	names := observations.standing
	if source.Status.Placement != nil && source.Status.Placement.Plan != nil {
		names = nil
		for _, candidate := range source.Status.Placement.Candidates {
			names = append(names, candidate.Cluster)
		}
	}
	if chosen != "" && !slices.Contains(names, chosen) {
		names = append(slices.Clone(names), chosen)
	}
	return names
}

// singleMemberCandidate reads one member's row: the observation's row, or an
// identity row for an unobserved member, with the stored allocation copied
// in. A pass that cannot read the member keeps the plan it last acknowledged;
// a conclusively absent member acknowledges nothing.
func (r *Reconciler) singleMemberCandidate(ctx context.Context, source *v1beta1.InferenceService, observations *placementObservations, name string) (v1beta1.CandidatePlacement, memberObservation) {
	member, ok := observations.get(name)
	if !ok {
		member = observations.refresh(ctx, r, source, name)
	}
	candidate := member.candidate
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
	candidate.ObservationKnown = member.state != memberUnknown
	candidate.AppliedPlanID = ""
	if candidate.Allocation != nil && member.state != memberAbsent {
		candidate.AppliedPlanID = stored.AppliedPlanID
	}
	return candidate, member
}

// observeSingleAllocation reads an allocated member's acknowledgement of the
// plan and notes what it proves. A race candidate that still holds resources
// or could not be read keeps cleanup pending. The chosen cluster counts as
// applied only when this pass read its acknowledgement, never from a kept
// value. An unreadable chosen cluster at a zero minimum keeps the Placed
// phase the last readable pass reached for the same plan instead of
// retracting proof the pass did not disprove.
func (r *Reconciler) observeSingleAllocation(ctx context.Context, source *v1beta1.InferenceService, name, chosen string, candidate *v1beta1.CandidatePlacement, flags *singleStatusFlags) {
	observed, err := r.observePlannedMember(ctx, source, *candidate, declaredComponents(source))
	if err != nil {
		r.Log.Error(err, "planned member observation failed", "cluster", name)
	} else {
		candidate.AppliedPlanID = observed.Candidate.AppliedPlanID
	}
	if candidate.Allocation.RaceCandidate && (err != nil || !observed.MemberState.Absent) {
		flags.cleanupPending = true
	}
	if err == nil && name == chosen && candidate.AppliedPlanID == source.Status.Placement.Plan.ID {
		flags.chosenApplied = true
		flags.chosenIdle = observed.IdleZeroFloor
	}
	if err != nil && name == chosen && candidate.AppliedPlanID == source.Status.Placement.Plan.ID && hasZeroReplicaFloor(candidate.Allocation.CurrentHome) {
		flags.chosenKeptPlaced = source.Status.Placement.Phase == v1beta1.PlacementPhasePlaced
	}
}

// recordSingleMember adds the member's row to the result and lets the chosen
// cluster set the phase: terminal fails the placement, absent loses it,
// unreadable keeps the stored phase, and admitted or idle is Placed with its
// endpoint. Without a chosen cluster any present member keeps the race in
// Admitting.
func recordSingleMember(res *placementResult, source *v1beta1.InferenceService, name, chosen string, candidate v1beta1.CandidatePlacement, member memberObservation, flags *singleStatusFlags) {
	if candidate.Allocation != nil || (member.state != memberAbsent && !sourceHasPendingWinnerIdentity(source)) {
		res.candidates = append(res.candidates, candidate)
	}
	if chosen == "" {
		if member.state != memberAbsent {
			res.phase = v1beta1.PlacementPhaseAdmitting
		}
		res.readinessUnknown = res.readinessUnknown || member.state == memberUnknown
		return
	}
	if name != chosen {
		return
	}
	switch {
	case member.terminal:
		res.phase = v1beta1.PlacementPhaseFailed
	case member.state == memberAbsent:
		res.placementLost = true
	case member.state == memberUnknown:
		res.phase = source.Status.Placement.Phase
		res.readinessUnknown = true
		res.url = candidate.Endpoint
	case candidate.Phase == v1beta1.CandidatePhaseAdmitted || flags.chosenIdle || flags.chosenKeptPlaced:
		res.phase = v1beta1.PlacementPhasePlaced
		res.ready = member.serving
		res.url = candidate.Endpoint
	default:
		res.phase = v1beta1.PlacementPhaseAdmitting
	}
}

// singleStatusReason names why the plan has not converged: no chosen cluster
// yet, a move in flight, race cleanup still pending, or a chosen cluster that
// has not applied the plan; a converged plan has none. A reason the caller
// passed stands.
func singleStatusReason(source *v1beta1.InferenceService, chosen, reason string, flags *singleStatusFlags) string {
	if reason != "" {
		return reason
	}
	switch {
	case chosen == "":
		return "AwaitingSingleWinner"
	case source.Status.Placement.Plan.SingleMove != nil:
		return "AwaitingMemberConvergence"
	case flags.cleanupPending:
		return "AwaitingRaceCleanup"
	case !flags.chosenApplied:
		return "AwaitingMemberConvergence"
	}
	return ""
}
