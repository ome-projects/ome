package placement

import (
	"context"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

type homeObservationState uint8

const (
	homeUnknown homeObservationState = iota
	homeAbsent
	homePresent
)

// homeObservation is one status-recorded home's current member-side state.
// Unknown means the home could not be read, while Absent is reserved for
// conclusive evidence that the placement copy is gone.
type homeObservation struct {
	state          homeObservationState
	candidate      v1beta1.CandidatePlacement
	serving        bool
	terminal       bool
	err            error
	terminalMember *v1beta1.InferenceService
}

// placementObservations caches the observations made before placement gates.
// Reconciliation paths may refresh an entry after mutating that member, while
// sharing the same observation semantics.
type placementObservations struct {
	known      map[string]bool
	matches    map[string]bool
	projectAll bool
	previous   map[string]v1beta1.CandidatePlacement
	standing   []string
	homes      map[string]homeObservation
}

func newPlacementObservations(isvc *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) *placementObservations {
	o := &placementObservations{
		known:    make(map[string]bool, len(clusters)),
		matches:  make(map[string]bool, len(clusters)),
		previous: make(map[string]v1beta1.CandidatePlacement),
		homes:    make(map[string]homeObservation),
	}
	selector, selectorErr := placementSelector(isvc)
	o.projectAll = selectorErr != nil
	for i := range clusters {
		o.known[clusters[i].Name] = true
		_, matches := selector.Match(&clusters[i])
		if selectorErr == nil && matches {
			o.matches[clusters[i].Name] = true
		}
	}
	for _, candidate := range standingPlacementCandidates(isvc) {
		o.previous[candidate.Cluster] = candidate
		o.standing = append(o.standing, candidate.Cluster)
	}
	return o
}

func (o *placementObservations) projectedStanding() []string {
	clusters := make([]string, 0, len(o.standing))
	for _, cluster := range o.standing {
		if o.projects(cluster) {
			clusters = append(clusters, cluster)
		}
	}
	sort.Strings(clusters)
	return clusters
}

func (o *placementObservations) projects(cluster string) bool {
	return o.projectAll || o.matches[cluster]
}

func (r *Reconciler) observeStandingHomes(ctx context.Context, isvc *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) *placementObservations {
	observations := newPlacementObservations(isvc, clusters)
	for _, cluster := range observations.standing {
		observations.refresh(ctx, r, isvc, cluster)
	}
	return observations
}

func (o *placementObservations) refresh(ctx context.Context, r *Reconciler, isvc *v1beta1.InferenceService, cluster string) homeObservation {
	previous, ok := o.previous[cluster]
	if !ok {
		previous = v1beta1.CandidatePlacement{Cluster: cluster, Phase: v1beta1.CandidatePhaseAdmitting}
	}
	if current, observed := o.homes[cluster]; observed {
		if current.state == homeAbsent {
			previous = identityCandidate(cluster)
		} else {
			previous = current.candidate
		}
	}
	observationCtx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	home := r.observeHome(observationCtx, isvc, previous, o.known[cluster])
	cancel()
	o.homes[cluster] = home
	return home
}

func (o *placementObservations) get(cluster string) (homeObservation, bool) {
	home, ok := o.homes[cluster]
	return home, ok
}

// observeHome reads one member without changing it. A disconnected or
// unreadable member is Unknown and loses routable ready capacity for this
// observation. NotFound, a terminating copy, a foreign same-name object, or a
// removed WorkloadCluster are conclusive absence.
func (r *Reconciler) observeHome(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	previous v1beta1.CandidatePlacement,
	known bool,
) homeObservation {
	absent := func() homeObservation {
		if hasPolicyRefs(isvc) {
			r.policyState().forgetHome(isvc.UID, previous.Cluster)
		}
		if hasRolloutPolicyRefs(isvc) {
			r.rolloutState().forgetHome(isvc.UID, previous.Cluster)
		}
		return homeObservation{state: homeAbsent}
	}
	if !known {
		return absent()
	}
	unknown := func(err error) homeObservation {
		candidate := retainedUnknownCandidate(isvc, previous)
		return homeObservation{state: homeUnknown, candidate: candidate, err: err}
	}
	var cl client.Client
	if isvc.Status.Placement != nil && isvc.Status.Placement.Plan != nil {
		for _, candidate := range isvc.Status.Placement.Candidates {
			if candidate.Cluster == previous.Cluster && candidate.Allocation != nil {
				direct, err := r.plannedClient(ctx, previous.Cluster, candidate.Allocation.ClusterUID)
				if err != nil {
					return unknown(err)
				}
				cl = direct
				break
			}
		}
	}
	if cl == nil {
		remote, connected := r.Clusters.ClientFor(previous.Cluster)
		if !connected {
			return unknown(nil)
		}
		cl = remote
	}

	derived := &v1beta1.InferenceService{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}
	if err := cl.Get(ctx, key, derived); err != nil {
		if apierrors.IsNotFound(err) {
			return absent()
		}
		return unknown(err)
	}
	if !derived.DeletionTimestamp.IsZero() || !isOurDerived(derived, isvc) {
		return absent()
	}

	r.observeDerivedPolicyStatus(isvc, previous.Cluster, derived)
	r.observeDerivedRolloutStatus(isvc, previous.Cluster, derived)
	if IsTerminallyFailed(derived, nil) {
		return terminalHome(previous.Cluster, derived)
	}

	statuses, err := componentIRStatuses(ctx, r.instanceStatusReader(cl), derived)
	if err != nil {
		candidate := retainedUnknownCandidate(isvc, previous)
		candidate.Endpoint = nil
		if endpoint := endpointFor(derived); endpoint != nil {
			candidate.Endpoint = endpoint.DeepCopy()
		}
		candidate.Autoscaling = nil
		candidate.Rollout = nil
		return homeObservation{
			state: homeUnknown, candidate: candidate, err: err,
		}
	}
	if IsTerminallyFailed(derived, statuses) {
		return terminalHome(previous.Cluster, derived)
	}

	candidate := v1beta1.CandidatePlacement{
		Cluster: previous.Cluster,
		Phase:   v1beta1.CandidatePhaseAdmitting,
	}
	if AllComponentsAdmitted(derived, statuses) {
		components := placementScaleComponents(derived)
		candidate.Phase = v1beta1.CandidatePhaseAdmitted
		if endpoint := endpointFor(derived); endpoint != nil {
			candidate.Endpoint = endpoint.DeepCopy()
		}
		candidate.AdmittedReplicas = placementAdmittedReplicas(components, statuses)
		if derived.Status.IsConditionReady(v1beta1.IngressReady) {
			candidate.ReadyReplicas = placementReadyReplicas(components, statuses)
		}
	}
	return homeObservation{
		state: homePresent, candidate: candidate,
		serving: placementCandidateServing(derived, candidate),
	}
}

func terminalHome(cluster string, member *v1beta1.InferenceService) homeObservation {
	return homeObservation{
		state: homePresent,
		candidate: v1beta1.CandidatePlacement{
			Cluster: cluster,
			Phase:   v1beta1.CandidatePhaseAdmitting,
		},
		terminal:       true,
		terminalMember: member,
	}
}

func retainedUnknownCandidate(isvc *v1beta1.InferenceService, previous v1beta1.CandidatePlacement) v1beta1.CandidatePlacement {
	candidate := *previous.DeepCopy()
	candidate = normalizeCandidatePhase(candidate)
	candidate.ReadyReplicas = 0
	if !hasPolicyRefs(isvc) {
		candidate.Autoscaling = nil
	}
	if !hasRolloutPolicyRefs(isvc) {
		candidate.Rollout = nil
	}
	return candidate
}

func standingPlacementCandidates(isvc *v1beta1.InferenceService) []v1beta1.CandidatePlacement {
	placement := isvc.Status.Placement
	if placement == nil {
		return nil
	}
	candidates := make([]v1beta1.CandidatePlacement, 0, len(placement.Candidates)+1)
	seen := make(map[string]bool, len(placement.Candidates)+1)
	for i := range placement.Candidates {
		candidate := *placement.Candidates[i].DeepCopy()
		if candidate.Cluster == placement.Cluster && candidate.Endpoint == nil {
			endpoint := placement.Endpoint
			if endpoint == nil {
				endpoint = isvc.Status.URL
			}
			if endpoint != nil {
				candidate.Endpoint = endpoint.DeepCopy()
			}
		}
		candidates = append(candidates, candidate)
		seen[candidate.Cluster] = true
	}
	if placement.Cluster != "" && !seen[placement.Cluster] {
		phase := v1beta1.CandidatePhaseAdmitting
		if placement.Phase == v1beta1.PlacementPhasePlaced {
			phase = v1beta1.CandidatePhaseAdmitted
		}
		endpoint := placement.Endpoint
		if endpoint == nil {
			endpoint = isvc.Status.URL
		}
		candidate := v1beta1.CandidatePlacement{Cluster: placement.Cluster, Phase: phase}
		if endpoint != nil {
			candidate.Endpoint = endpoint.DeepCopy()
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func identityCandidate(cluster string) v1beta1.CandidatePlacement {
	return v1beta1.CandidatePlacement{Cluster: cluster, Phase: v1beta1.CandidatePhaseAdmitting}
}

// writeObservedPlacement publishes member observations without fan-out,
// re-application, deletion, or a Single-mode re-race.
func (r *Reconciler) writeObservedPlacement(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	observations *placementObservations,
) (ctrl.Result, error) {
	var res ctrl.Result
	var err error
	if placementMode(isvc) == v1beta1.PlacementModeSingle {
		res, err = r.writeObservedSingle(ctx, isvc, observations)
	} else {
		res, err = r.writeObservedMulti(ctx, isvc, observations)
	}
	if err == nil && res.RequeueAfter > r.requeue() {
		res.RequeueAfter = r.requeue()
	}
	return res, err
}

func (r *Reconciler) writeObservedSingle(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	observations *placementObservations,
) (ctrl.Result, error) {
	if isvc.Status.Placement != nil && isvc.Status.Placement.Plan != nil && isvc.Status.Placement.Plan.Mode == v1beta1.PlacementModeSingle {
		return r.writeSinglePlanStatus(ctx, isvc, observations, "")
	}
	winner := winnerCluster(isvc)
	if winner != "" && observations.known[winner] && !observations.projects(winner) {
		r.clearGrace(isvc.UID)
		winner = ""
	}
	if winner == "" {
		candidates := make([]v1beta1.CandidatePlacement, 0, len(observations.standing))
		unknown := false
		for _, cluster := range observations.projectedStanding() {
			home := observations.homes[cluster]
			if home.state == homeAbsent {
				continue
			}
			unknown = unknown || home.state == homeUnknown
			candidates = append(candidates, home.candidate)
			if home.state == homePresent && !home.terminal && home.candidate.Phase == v1beta1.CandidatePhaseAdmitted {
				r.clearGrace(isvc.UID)
				return r.writePlacement(ctx, isvc, placementResult{
					winner: cluster, phase: v1beta1.PlacementPhasePlaced,
					candidates: []v1beta1.CandidatePlacement{home.candidate},
					url:        home.candidate.Endpoint, ready: home.serving,
				})
			}
		}
		phase := v1beta1.PlacementPhasePending
		if len(candidates) > 0 {
			phase = v1beta1.PlacementPhaseAdmitting
		}
		return r.writePlacement(ctx, isvc, placementResult{
			phase: phase, candidates: candidates, readinessUnknown: unknown,
		})
	}

	home, ok := observations.get(winner)
	if !ok {
		home = homeObservation{state: homeAbsent}
	}
	switch home.state {
	case homeUnknown:
		return r.writePlacement(ctx, isvc, observedSingleResult(isvc, winner, home))
	case homeAbsent:
		return r.writeSingleAbsentGrace(ctx, isvc, winner)
	case homePresent:
		if home.terminal {
			r.clearGrace(isvc.UID)
			return r.writePlacement(ctx, isvc, placementResult{
				winner: winner, phase: v1beta1.PlacementPhaseFailed,
				candidates: []v1beta1.CandidatePlacement{home.candidate},
			})
		}
		if home.candidate.Phase != v1beta1.CandidatePhaseAdmitted {
			remaining := r.graceRemaining(isvc.UID, time.Now())
			if remaining > 0 {
				return r.writeSingleAdmissionGrace(ctx, isvc, winner)
			}
			r.clearGrace(isvc.UID)
			return r.writePlacement(ctx, isvc, placementResult{
				phase:      v1beta1.PlacementPhaseAdmitting,
				candidates: []v1beta1.CandidatePlacement{home.candidate},
			})
		}
		r.clearGrace(isvc.UID)
		return r.writePlacement(ctx, isvc, placementResult{
			winner: winner, phase: v1beta1.PlacementPhasePlaced,
			candidates: []v1beta1.CandidatePlacement{home.candidate},
			url:        home.candidate.Endpoint, ready: home.serving,
		})
	default:
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}
}

func sourceHasPendingWinnerIdentity(isvc *v1beta1.InferenceService) bool {
	placement := isvc.Status.Placement
	return placement != nil && placement.Phase == v1beta1.PlacementPhasePending &&
		placement.Cluster != "" && len(placement.Candidates) == 0
}

func observedSingleResult(
	isvc *v1beta1.InferenceService,
	winner string,
	home homeObservation,
) placementResult {
	if home.state == homeUnknown && sourceHasPendingWinnerIdentity(isvc) {
		return placementResult{
			winner: winner, phase: v1beta1.PlacementPhasePending,
			readinessUnknown: true,
		}
	}
	return observedCandidateResult(home)
}

func (r *Reconciler) writeSingleAbsentGrace(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	winner string,
) (ctrl.Result, error) {
	remaining := r.graceRemaining(isvc.UID, time.Now())
	if remaining <= 0 {
		remaining = r.requeue()
	}
	res, err := r.writePlacement(ctx, isvc, placementResult{
		winner: winner, phase: v1beta1.PlacementPhasePending, placementLost: true,
	})
	if err == nil {
		res.RequeueAfter = min(res.RequeueAfter, min(remaining, r.requeue()))
	}
	return res, err
}

func (r *Reconciler) writeSingleAdmissionGrace(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	winner string,
) (ctrl.Result, error) {
	remaining := r.graceRemaining(isvc.UID, time.Now())
	if remaining <= 0 {
		r.clearGrace(isvc.UID)
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}
	res, err := r.writePlacement(ctx, isvc, placementResult{
		winner: winner, phase: v1beta1.PlacementPhaseAdmitting,
		candidates: []v1beta1.CandidatePlacement{identityCandidate(winner)},
	})
	if err == nil {
		res.RequeueAfter = min(res.RequeueAfter, min(remaining, r.requeue()))
	}
	return res, err
}

func (r *Reconciler) writeObservedMulti(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	observations *placementObservations,
) (ctrl.Result, error) {
	standing := observations.projectedStanding()
	candidates := make([]v1beta1.CandidatePlacement, 0, len(standing))
	admitted := 0
	serving := false
	unknown := false
	considered := 0
	allAbsent := true
	for _, cluster := range observations.standing {
		if observations.known[cluster] && !observations.projects(cluster) {
			continue
		}
		considered++
		if observations.homes[cluster].state != homeAbsent {
			allAbsent = false
		}
	}
	for _, cluster := range standing {
		home := observations.homes[cluster]
		if home.state == homeAbsent {
			continue
		}
		candidate := home.candidate
		candidates = append(candidates, candidate)
		unknown = unknown || home.state == homeUnknown
		if candidate.Phase == v1beta1.CandidatePhaseAdmitted {
			admitted++
			serving = serving || home.serving
		}
	}
	phase := v1beta1.PlacementPhasePending
	if len(candidates) > 0 {
		phase = v1beta1.PlacementPhaseAdmitting
	}
	if admitted > 0 {
		phase = v1beta1.PlacementPhasePlaced
	}
	return r.writePlacement(ctx, isvc, placementResult{
		phase: phase, candidates: candidates, ready: serving,
		readinessUnknown: unknown && !serving,
		placementLost:    considered > 0 && allAbsent && len(candidates) == 0,
	})
}

func observedCandidateResult(home homeObservation) placementResult {
	phase := v1beta1.PlacementPhaseAdmitting
	if home.candidate.Phase == v1beta1.CandidatePhaseAdmitted {
		phase = v1beta1.PlacementPhasePlaced
	}
	return placementResult{
		winner: home.candidate.Cluster, phase: phase,
		candidates: []v1beta1.CandidatePlacement{home.candidate},
		url:        home.candidate.Endpoint, ready: home.serving,
		readinessUnknown: home.state == homeUnknown,
	}
}
