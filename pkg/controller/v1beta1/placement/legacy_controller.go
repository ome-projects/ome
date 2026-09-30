package placement

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/validation"
)

// Legacy execution retains admission-driven packing, whole-block selector
// precedence, and sticky Single winners independently of planned execution.
func (r *Reconciler) reconcileLegacy(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if err := validation.ValidatePlacementIntent(isvc); err != nil {
		clusters := &v1beta1.WorkloadClusterList{}
		if err := r.List(ctx, clusters); err != nil {
			return ctrl.Result{}, err
		}
		observations := r.observeStandingHomes(ctx, isvc, clusters.Items)
		observations.projectAll = true
		return r.writeObservedPlacement(ctx, isvc, observations)
	}
	if controllerutil.AddFinalizer(isvc, PlacementFinalizer) {
		if err := r.Update(ctx, isvc); err != nil {
			return ctrl.Result{}, err
		}
	}

	clusters := &v1beta1.WorkloadClusterList{}
	if err := r.List(ctx, clusters); err != nil {
		return ctrl.Result{}, err
	}
	observations := r.observeStandingHomes(ctx, isvc, clusters.Items)
	candidates, reason, err := MatchCandidates(isvc, clusters.Items)
	if err != nil || len(candidates) == 0 {
		r.Log.Info("no placement candidates", "isvc", isvc.Namespace+"/"+isvc.Name,
			"reason", reason, "error", err)
		if isvc.Status.Placement != nil && len(observations.standing) > 0 {
			return r.writeObservedPlacement(ctx, isvc, observations)
		}
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}

	pf := r.preflightPolicies(ctx, isvc, candidates, clusters.Items)
	if pf != nil && pf.holdAsIs {
		return r.writeObservedPlacement(ctx, isvc, observations)
	}
	if pf != nil && !pf.hold {
		candidates = pf.eligible
	}

	var rf *policyPreflightOutcome
	if pf == nil || !pf.hold {
		rf = r.preflightRolloutPolicies(ctx, isvc, candidates, clusters.Items)
		if rf != nil && rf.holdAsIs {
			return r.writeObservedPlacement(ctx, isvc, observations)
		}
		if rf != nil && !rf.hold {
			candidates = rf.eligible
		}
	}

	var res ctrl.Result
	if (pf != nil && pf.hold) || (rf != nil && rf.hold) {
		res, err = r.writeObservedPlacement(ctx, isvc, observations)
	} else {
		switch mode := placementMode(isvc); mode {
		case v1beta1.PlacementModeSingle:
			res, err = r.legacyReconcileSingle(ctx, isvc, candidates, observations)
		case v1beta1.PlacementModeAll:
			res, err = r.legacyReconcileAll(ctx, isvc, candidates, observations)
		case v1beta1.PlacementModeSplit:
			res, err = r.legacyReconcileSplit(ctx, isvc, candidates, observations)
		default:
			r.Log.Info("placement mode not supported by this build; holding Pending",
				"mode", mode, "isvc", isvc.Namespace+"/"+isvc.Name)
			res, err = r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
		}
	}
	transient := (pf != nil && pf.transient) || (rf != nil && rf.transient)
	if err == nil && transient && res.RequeueAfter > r.requeue() {
		res.RequeueAfter = r.requeue()
	}
	return res, err
}

func (r *Reconciler) legacyReconcileSingle(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	candidates []string,
	observations *placementObservations,
) (ctrl.Result, error) {
	if winner := winnerCluster(isvc); winner != "" &&
		(observations.projects(winner) || !observations.known[winner]) {
		home, ok := observations.get(winner)
		if !ok {
			home = observations.refresh(ctx, r, isvc, winner)
		}
		if home.err != nil {
			r.Log.Error(home.err, "sticky winner: member read failed; holding placement",
				"cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name)
		}
		switch home.state {
		case homeUnknown:
			res, err := r.writePlacement(ctx, isvc, observedSingleResult(isvc, winner, home))
			if err == nil {
				res.RequeueAfter = r.requeue()
			}
			return res, err
		case homeAbsent:
			if remaining := r.graceRemaining(isvc.UID, time.Now()); remaining > 0 {
				return r.writeSingleAbsentGrace(ctx, isvc, winner)
			}
			r.clearGrace(isvc.UID)
		case homePresent:
			if home.terminal {
				r.clearGrace(isvc.UID)
				if slices.Contains(candidates, winner) {
					if cl, ok := r.Clusters.ClientFor(winner); ok {
						if err := r.legacyUpdateTerminalMember(ctx, winner, cl, isvc, home.terminalMember); err != nil {
							return r.legacyObservedStatusThenError(ctx, isvc, observations, err)
						}
					}
				}
				return r.writePlacement(ctx, isvc, placementResult{
					winner: winner, phase: v1beta1.PlacementPhaseFailed,
					candidates: []v1beta1.CandidatePlacement{home.candidate},
				})
			}
			if home.candidate.Phase != v1beta1.CandidatePhaseAdmitted {
				if remaining := r.graceRemaining(isvc.UID, time.Now()); remaining > 0 {
					return r.writeSingleAdmissionGrace(ctx, isvc, winner)
				}
				r.clearGrace(isvc.UID)
				break
			}
			r.clearGrace(isvc.UID)
			if !slices.Contains(candidates, winner) {
				res, err := r.writePlacement(ctx, isvc, observedCandidateResult(home))
				if err == nil {
					res.RequeueAfter = r.requeue()
				}
				return res, err
			}
			cl, ok := r.Clusters.ClientFor(winner)
			if !ok {
				unknown := retainedUnknownCandidate(isvc, home.candidate)
				res, err := r.writePlacement(ctx, isvc, placementResult{
					winner: winner, phase: v1beta1.PlacementPhasePlaced,
					candidates: []v1beta1.CandidatePlacement{unknown},
					url:        unknown.Endpoint, readinessUnknown: true,
				})
				if err == nil {
					res.RequeueAfter = r.requeue()
				}
				return res, err
			}
			if err := r.legacyPlaceOnBounded(ctx, winner, cl, isvc); err != nil {
				return r.legacyObservedStatusThenError(ctx, isvc, observations, err)
			}
			r.legacyDeleteLosers(ctx, isvc, candidates, winner)
			return r.writePlacement(ctx, isvc, observedCandidateResult(home))
		}
	}

	nominated, hold := r.nominate().Nominate(isvc.UID, candidates, time.Now())
	if len(nominated) == 0 {
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}

	placed, err := r.legacyFanOut(ctx, isvc, nominated)
	if err != nil {
		return r.legacyObservedStatusThenError(ctx, isvc, observations, err)
	}
	placedNow := make(map[string]bool, len(placed))
	for _, cluster := range placed {
		placedNow[cluster] = true
	}

	cands := make([]v1beta1.CandidatePlacement, 0, len(nominated))
	unknown := false
	for _, cluster := range nominated {
		if !placedNow[cluster] && !observations.legacyHad(cluster) {
			continue
		}
		home := observations.refresh(ctx, r, isvc, cluster)
		if home.err != nil {
			r.Log.Error(home.err, "race: member read failed; keeping unknown candidate",
				"cluster", cluster, "isvc", isvc.Namespace+"/"+isvc.Name)
		}
		if home.state == homeAbsent {
			if placedNow[cluster] {
				cands = append(cands, identityCandidate(cluster))
			}
			continue
		}
		unknown = unknown || home.state == homeUnknown
		cands = append(cands, home.candidate)
		if home.state != homePresent || home.terminal || home.candidate.Phase != v1beta1.CandidatePhaseAdmitted {
			continue
		}
		r.clearGrace(isvc.UID)
		r.legacyDeleteLosers(ctx, isvc, candidates, cluster)
		return r.writePlacement(ctx, isvc, observedCandidateResult(home))
	}
	if len(cands) == 0 {
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}
	{
		res, err := r.writePlacement(ctx, isvc, placementResult{
			phase: v1beta1.PlacementPhaseAdmitting, candidates: cands,
			readinessUnknown: unknown,
		})
		if err == nil {
			res.RequeueAfter = r.requeue()
			if hold > 0 {
				res.RequeueAfter = min(hold, res.RequeueAfter)
			}
		}
		return res, err
	}
}

func (r *Reconciler) legacyReconcileAll(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	candidates []string,
	observations *placementObservations,
) (ctrl.Result, error) {
	placed, err := r.legacyFanOut(ctx, isvc, candidates)
	if err != nil {
		return r.legacyObservedStatusThenError(ctx, isvc, observations, err)
	}
	placedNow := make(map[string]bool, len(placed))
	for _, c := range placed {
		placedNow[c] = true
	}

	projected := observations.legacyProjectedClusters(candidates)
	cands := make([]v1beta1.CandidatePlacement, 0, len(projected))
	admitted := 0
	serving := false
	unobserved := false
	for _, cluster := range projected {
		actuatable := slices.Contains(candidates, cluster)
		if actuatable && placedNow[cluster] {
			observations.refresh(ctx, r, isvc, cluster)
		}
		home, observed := observations.get(cluster)
		if !observed {
			home = observations.refresh(ctx, r, isvc, cluster)
		}
		if actuatable && !placedNow[cluster] && !observations.legacyHad(cluster) {
			continue
		}
		if home.err != nil {
			r.Log.Error(home.err, "all: member read failed; keeping unknown home",
				"cluster", cluster, "isvc", isvc.Namespace+"/"+isvc.Name)
		}
		if home.state == homeAbsent {
			if actuatable && placedNow[cluster] {
				cands = append(cands, identityCandidate(cluster))
			}
			continue
		}
		unobserved = unobserved || home.state == homeUnknown
		cands = append(cands, home.candidate)
		if home.candidate.Phase == v1beta1.CandidatePhaseAdmitted {
			admitted++
			serving = serving || home.serving
		}
	}
	if len(placed) == 0 && len(cands) == 0 {
		res, err := r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
		if err == nil {
			res.RequeueAfter = r.requeue()
		}
		return res, err
	}

	phase := v1beta1.PlacementPhaseAdmitting
	if admitted > 0 {
		phase = v1beta1.PlacementPhasePlaced
	}
	res, err := r.writePlacement(ctx, isvc, placementResult{
		phase: phase, candidates: cands, ready: serving,
		readinessUnknown: unobserved && !serving,
	})
	if err == nil && (admitted < len(cands) || unobserved) {
		res.RequeueAfter = r.requeue()
	}
	return res, err
}

func (r *Reconciler) legacyReconcileSplit(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	candidates []string,
	observations *placementObservations,
) (ctrl.Result, error) {
	desired := splitDesiredReplicas(isvc)
	if desired <= 0 {
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}
	var maxPer, minPer int32
	spread := false
	if sp := isvc.Spec.Placement.Split; sp != nil {
		maxPer, minPer, spread = sp.MaxReplicasPerCluster, sp.MinReplicasPerCluster, sp.Spread
	}

	projected := observations.legacyProjectedClusters(candidates)
	selected := make(map[string]bool, len(candidates))
	for _, cluster := range candidates {
		selected[cluster] = true
	}
	homes := make(map[string]homeObservation, len(projected))
	for _, cluster := range projected {
		home, ok := observations.get(cluster)
		if !ok {
			home = observations.refresh(ctx, r, isvc, cluster)
		}
		homes[cluster] = home
		if home.err != nil {
			r.Log.Error(home.err, "split: member read failed; keeping allocation share",
				"cluster", cluster, "isvc", isvc.Namespace+"/"+isvc.Name)
		}
	}
	actuatable := make(map[string]bool, len(candidates))
	actionCandidates := make([]string, 0, len(candidates))
	admitted := make(map[string]int32, len(candidates))
	var reserved int32
	for _, cluster := range projected {
		home := homes[cluster]
		count := home.candidate.AdmittedReplicas
		if home.state == homeAbsent || home.terminal ||
			(minPer > 0 && count > 0 && count < minPer) {
			count = 0
		}
		if selected[cluster] && !home.terminal {
			actuatable[cluster] = true
			actionCandidates = append(actionCandidates, cluster)
			admitted[cluster] = count
		} else if !selected[cluster] {
			reserved += count
		}
	}

	remainingDesired := desired - reserved
	if remainingDesired < 0 {
		remainingDesired = 0
	}
	targets := legacySplitApportion(actionCandidates, admitted, remainingDesired, maxPer, spread)

	var admittedTotal int32
	cands := make([]v1beta1.CandidatePlacement, 0, len(candidates))
	serving := false
	unobserved := false
	retrySoon := false
	for _, cluster := range projected {
		home := homes[cluster]
		sliver := home.candidate.AdmittedReplicas > 0 && minPer > 0 &&
			home.candidate.AdmittedReplicas < minPer
		if home.terminal {
			cands = append(cands, home.candidate)
			continue
		}
		if !actuatable[cluster] {
			if home.state == homeAbsent {
				continue
			}
			candidate := home.candidate
			if sliver {
				candidate = identityCandidate(cluster)
			}
			cands = append(cands, candidate)
			unobserved = unobserved || home.state == homeUnknown
			if candidate.Phase == v1beta1.CandidatePhaseAdmitted {
				admittedTotal += candidate.AdmittedReplicas
				serving = serving || home.serving
			}
			continue
		}
		if home.state == homeUnknown {
			unobserved = true
			if observations.legacyHad(cluster) {
				cands = append(cands, home.candidate)
				if home.candidate.Phase == v1beta1.CandidatePhaseAdmitted {
					admittedTotal += min(home.candidate.AdmittedReplicas, targets[cluster])
				}
			}
			continue
		}
		if sliver || targets[cluster] <= 0 {
			if home.state == homePresent {
				if err := r.legacyDeleteDerivedOnBounded(ctx, cluster, isvc); err != nil {
					r.Log.Error(err, "split: sweep cluster failed", "cluster", cluster, "isvc", isvc.Namespace+"/"+isvc.Name)
					cands = append(cands, home.candidate)
					if home.candidate.Phase == v1beta1.CandidatePhaseAdmitted {
						admittedTotal += home.candidate.AdmittedReplicas
						serving = serving || home.serving
					}
					retrySoon = true
				}
			}
			continue
		}
		cl, ok := r.Clusters.ClientFor(cluster)
		if !ok {
			if home.state == homePresent {
				cands = append(cands, home.candidate)
			}
			if home.state == homePresent && home.candidate.Phase == v1beta1.CandidatePhaseAdmitted {
				admittedTotal += min(home.candidate.AdmittedReplicas, targets[cluster])
				serving = serving || home.serving
			}
			retrySoon = true
			continue
		}
		if err := r.legacyPlaceOnReplicasBounded(ctx, cluster, cl, isvc, targets[cluster], maxPer); err != nil {
			if apierrors.IsConflict(err) {
				r.Log.V(1).Info("split: place conflicted on cluster; retrying", "cluster", cluster, "isvc", isvc.Namespace+"/"+isvc.Name)
			} else {
				r.Log.Error(err, "split: place failed on cluster", "cluster", cluster, "isvc", isvc.Namespace+"/"+isvc.Name)
			}
			if home.state == homePresent {
				cands = append(cands, home.candidate)
			}
			if home.state == homePresent && home.candidate.Phase == v1beta1.CandidatePhaseAdmitted {
				admittedTotal += min(home.candidate.AdmittedReplicas, targets[cluster])
				serving = serving || home.serving
			}
			retrySoon = true
			continue
		}
		if home.candidate.AdmittedReplicas == 0 {
			cands = append(cands, identityCandidate(cluster))
			continue
		}
		counted := home.candidate.AdmittedReplicas
		if counted > targets[cluster] {
			counted = targets[cluster]
		}
		admittedTotal += counted
		cands = append(cands, home.candidate)
		serving = serving || home.serving
	}

	phase := v1beta1.PlacementPhaseAdmitting
	if admittedTotal > 0 {
		phase = v1beta1.PlacementPhasePlaced
	}
	res, err := r.writePlacement(ctx, isvc, placementResult{
		phase: phase, candidates: cands, ready: serving,
		readinessUnknown: unobserved && !serving,
	})
	if err == nil && (admittedTotal < desired || unobserved || retrySoon) {
		res.RequeueAfter = r.requeue()
	}
	return res, err
}

// legacySplitApportion over-requests a deficit in candidate order and trims
// surplus admission. Spread requests a rounded-up share independently per home.
func legacySplitApportion(candidates []string, admitted map[string]int32, desired, maxPer int32, spread bool) map[string]int32 {
	targets := make(map[string]int32, len(candidates))
	if len(candidates) == 0 {
		return targets
	}
	capTo := func(v int32) int32 {
		if maxPer > 0 && v > maxPer {
			return maxPer
		}
		return v
	}
	if spread {
		share := capTo(legacyCeilDiv(desired, int32(len(candidates))))
		for _, c := range candidates {
			targets[c] = share
		}
		return targets
	}
	var total int32
	for _, c := range candidates {
		total += admitted[c]
	}
	remaining := desired
	if total >= desired {
		for _, c := range candidates { // TRIM
			t := capTo(min(admitted[c], remaining))
			targets[c] = t
			remaining -= t
		}
		return targets
	}
	for _, c := range candidates { // FILL
		if remaining <= 0 {
			targets[c] = 0
			continue
		}
		targets[c] = capTo(remaining)
		remaining -= admitted[c]
	}
	return targets
}

func (r *Reconciler) legacyFanOut(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string) ([]string, error) {
	connected := legacyConnectedSet(r.Clusters)

	var placed []string
	var errs []error
	for _, c := range candidates {
		if !connected[c] {
			continue // not connected this pass; the next poll re-attempts it
		}
		cl, ok := r.Clusters.ClientFor(c)
		if !ok {
			continue // disconnected between snapshot and lookup; skip
		}
		if err := r.legacyPlaceOnBounded(ctx, c, cl, isvc); err != nil {
			r.Log.Error(err, "fan out failed on cluster", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			errs = append(errs, fmt.Errorf("%q: %w", c, err))
			continue
		}
		placed = append(placed, c)
	}
	if len(placed) == 0 && len(errs) > 0 {
		return nil, fmt.Errorf("fan out %s/%s failed on all connected candidates: %w", isvc.Namespace, isvc.Name, errors.Join(errs...))
	}
	return placed, nil
}

func (r *Reconciler) legacyPlaceOnBounded(ctx context.Context, cluster string, cl client.Client, isvc *v1beta1.InferenceService) error {
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	defer cancel()
	return r.legacyPlaceOn(cctx, cluster, cl, isvc)
}

// A terminal placement can receive a corrected spec without being re-placed.
// The observed member's UID and resource version fence the update against
// replacement, deletion, and concurrent ownership changes.
func (r *Reconciler) legacyUpdateTerminalMember(ctx context.Context, cluster string, cl client.Client, src, member *v1beta1.InferenceService) error {
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	defer cancel()
	if member == nil || member.UID == "" || member.ResourceVersion == "" || !member.DeletionTimestamp.IsZero() {
		return fmt.Errorf("terminal placement update requires a live member identity")
	}
	if protocol.IsAffinityMember(member) || !isOurDerived(member, src) {
		return fmt.Errorf("terminal placement update requires an owned legacy member")
	}
	desired, err := r.derivedFor(src)
	if err != nil {
		return err
	}
	if protocol.IsAffinityMember(desired) {
		return fmt.Errorf("legacy placement cannot apply ClusterAffinity authority")
	}
	if err := r.checkLegacySource(cctx, src); err != nil {
		return err
	}
	r.observePolicyRefStamp(src, cluster, member, desired)
	target := member.DeepCopy()
	target.Labels = mergeOwnedKeys(target.Labels, desired.Labels)
	target.Annotations = mergeOwnedKeys(target.Annotations, desired.Annotations)
	target.Spec = desired.Spec
	if equality.Semantic.DeepEqual(member, target) {
		return nil
	}
	return cl.Update(cctx, target)
}

func (r *Reconciler) legacyPlaceOnReplicasBounded(ctx context.Context, cluster string, cl client.Client, isvc *v1beta1.InferenceService, replicas, maxPer int32) error {
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	defer cancel()
	return r.legacyPlaceOnReplicas(cctx, cluster, cl, isvc, replicas, maxPer)
}

func (r *Reconciler) legacyPlaceOnReplicas(ctx context.Context, cluster string, cl client.Client, src *v1beta1.InferenceService, replicas, maxPer int32) error {
	d, err := r.derivedFor(src)
	if err != nil {
		return err
	}
	legacySetDerivedReplicas(d, replicas, maxPer)
	return r.legacyApplyDerived(ctx, cluster, cl, src, d)
}

func (r *Reconciler) legacyPlaceOn(ctx context.Context, cluster string, cl client.Client, src *v1beta1.InferenceService) error {
	d, err := r.derivedFor(src)
	if err != nil {
		return err
	}
	return r.legacyApplyDerived(ctx, cluster, cl, src, d)
}

func legacyConnectedSet(clusters ClusterClients) map[string]bool {
	names := clusters.Connected()
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return set
}

func (r *Reconciler) legacyApplyDerived(ctx context.Context, cluster string, cl client.Client, src, desired *v1beta1.InferenceService) error {
	if protocol.IsAffinityMember(desired) {
		return fmt.Errorf("legacy placement cannot apply ClusterAffinity authority")
	}
	target := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, cl, target, func() error {
		if protocol.IsAffinityMember(target) {
			return fmt.Errorf("legacy placement cannot overwrite a ClusterAffinity member")
		}
		if target.ResourceVersion != "" && !isOurDerived(target, src) {
			return fmt.Errorf("refusing to overwrite non-derived InferenceService %s/%s on candidate cluster: not a placement derived of control plane %q",
				target.Namespace, target.Name, r.ControlPlaneID)
		}
		r.observePolicyRefStamp(src, cluster, target, desired)
		if err := r.checkLegacySource(ctx, src); err != nil {
			return err
		}
		target.Labels = mergeOwnedKeys(target.Labels, desired.Labels)
		target.Annotations = mergeOwnedKeys(target.Annotations, desired.Annotations)
		target.Spec = desired.Spec
		return nil
	})
	return err
}

func (r *Reconciler) legacyDeleteLosers(ctx context.Context, isvc *v1beta1.InferenceService, clusters []string, keep string) {
	for _, c := range clusters {
		if c == keep {
			continue
		}
		if err := r.legacyDeleteDerivedOnBounded(ctx, c, isvc); err != nil {
			r.Log.Error(err, "loser cleanup failed on cluster (will retry next poll)", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
		}
	}
}

func (r *Reconciler) legacyDeleteDerivedOnBounded(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) error {
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	defer cancel()
	return r.legacyDeleteDerivedOn(cctx, cluster, isvc)
}

func (r *Reconciler) legacyDeleteDerivedOn(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) error {
	cl, ok := r.Clusters.ClientFor(cluster)
	if !ok {
		return nil
	}
	derived := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}, derived); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !isOurDerived(derived, isvc) {
		return nil
	}

	if protocol.IsAffinityMember(derived) {
		return fmt.Errorf("legacy placement cannot delete a ClusterAffinity member")
	}
	if err := r.checkLegacySource(ctx, isvc); err != nil {
		return err
	}
	uid := derived.UID
	opts := []client.DeleteOption{}
	if uid != "" {
		opts = append(opts, client.Preconditions{UID: &uid, ResourceVersion: &derived.ResourceVersion})
	}
	if err := cl.Delete(ctx, derived, opts...); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// legacySetDerivedReplicas preserves local ceilings while assigning each home
// its requested floor; an explicit split ceiling overrides the component ceiling.
func legacySetDerivedReplicas(d *v1beta1.InferenceService, replicas, maxPer int32) {
	n := int(replicas)
	apply := func(c *v1beta1.ComponentExtensionSpec) {
		c.MinReplicas = ptr.To(n)
		switch {
		case maxPer > 0:
			c.MaxReplicas = int(maxPer)
		case c.MaxReplicas < n:
			c.MaxReplicas = n
		}
	}
	if d.Spec.Engine != nil {
		apply(&d.Spec.Engine.ComponentExtensionSpec)
	}
	if d.Spec.Decoder != nil {
		apply(&d.Spec.Decoder.ComponentExtensionSpec)
	}
}

func legacyCeilDiv(a, b int32) int32 {
	if b <= 0 {
		return a
	}
	return (a + b - 1) / b
}

func (o *placementObservations) legacyProjectedClusters(candidates []string) []string {
	seen := make(map[string]bool, len(candidates)+len(o.standing))
	clusters := make([]string, 0, len(candidates)+len(o.standing))
	for _, cluster := range candidates {
		if !seen[cluster] {
			seen[cluster] = true
			clusters = append(clusters, cluster)
		}
	}
	for _, cluster := range o.projectedStanding() {
		if !seen[cluster] {
			seen[cluster] = true
			clusters = append(clusters, cluster)
		}
	}
	sort.Strings(clusters)
	return clusters
}

func (o *placementObservations) legacyHad(cluster string) bool {
	_, ok := o.previous[cluster]
	return ok
}

func (r *Reconciler) legacyObservedStatusThenError(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	observations *placementObservations,
	cause error,
) (ctrl.Result, error) {
	if _, err := r.writeObservedPlacement(ctx, isvc, observations); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, cause
}

// checkLegacySource fences remote mutations against the live policy selection.
func (r *Reconciler) checkLegacySource(ctx context.Context, source *v1beta1.InferenceService) error {
	if r.APIReader == nil {
		return fmt.Errorf("legacy placement requires a direct source reader")
	}
	live := &v1beta1.InferenceService{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(source), live); err != nil {
		return err
	}
	if !plan.SameSnapshot(source, live) || live.Spec.Placement.UsesClusterAffinity() {
		return plan.ErrStaleSnapshot
	}
	return validation.ValidatePlacementIntent(live)
}
