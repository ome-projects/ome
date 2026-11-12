package placement

import (
	"cmp"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Per-ISVC placement metrics. Every vector carries "namespace" and "isvc" so
// same-named ISVCs cannot collide and the series set can be dropped on teardown,
// bounding cardinality to the live ISVC count.
//
// Enum state is a label on an always-1 gauge, which makes phase="Failed"
// directly alertable but only holds if stale label values are removed — hence
// the reset-before-write in recordPlacement.
var (
	placementPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_phase",
		Help: "Always 1, labelled with the InferenceService's current multi-cluster placement phase (Pending, Admitting, Placed, Failed; legacy Racing may appear during upgrades). Exactly one series per ISVC.",
	}, []string{"namespace", "isvc", "phase"})

	placementWinner = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_winner",
		Help: "Always 1, labelled with the WorkloadCluster the InferenceService is currently placed on. Absent while no cluster has won the race.",
	}, []string{"namespace", "isvc", "cluster"})

	placementCandidate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_candidate",
		Help: "Always 1 per (ISVC, candidate cluster) the control plane has fanned out to, labelled with that candidate's phase (Admitting, Admitted; legacy Placed may appear during upgrades).",
	}, []string{"namespace", "isvc", "cluster", "phase"})

	placementCandidateAdmitted = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_candidate_admitted_replicas",
		Help: "Replicas this candidate cluster's Kueue has admitted. Meaningful in Split mode, where the control plane sums it across homes to decide whether the desired count is met.",
	}, []string{"namespace", "isvc", "cluster"})

	placementCandidateReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_candidate_ready_replicas",
		Help: "Ready replica count saved in candidate status. This is an input to traffic weighting and may retain the last successful count during observation grace.",
	}, []string{"namespace", "isvc", "cluster"})

	placementCandidateObservationKnown = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_candidate_observation_known",
		Help: "Whether the saved member observation is known (1) or unknown (0). A ready count can remain positive during observation grace.",
	}, []string{"namespace", "isvc", "cluster"})

	placementCandidateObservationFailingSince = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_candidate_observation_failing_since_timestamp_seconds",
		Help: "Unix timestamp of the observation failure recorded in candidate status, or 0 when no failure is recorded.",
	}, []string{"namespace", "isvc", "cluster"})

	// Policy is spec-derived, not status-derived: it answers "what was asked
	// for" so it can be compared against where the workload actually landed.
	placementPolicyInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_policy_info",
		Help: "Always 1, labelled with the InferenceService's declared placement policy: policy and mode. Exactly one series per ISVC that declares a placement.",
	}, []string{"namespace", "isvc", "policy", "mode"})

	placementSplitReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_split_replicas",
		Help: "Desired total replicas to distribute across homes, from spec.placement.split.replicas.",
	}, []string{"namespace", "isvc"})

	placementSplitMaxPerCluster = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_split_max_replicas_per_cluster",
		Help: "Per-cluster replica cap of the primary component (the engine, or else the decoder), from spec.placement.split.maxPerCluster (0 = uncapped).",
	}, []string{"namespace", "isvc"})

	placementTotalDesiredReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_total_desired_replicas",
		Help: "Requested primary replica units in the accepted placement plan for the current source generation, including unassigned replicas.",
	}, []string{"namespace", "isvc"})

	placementCandidateDesiredReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_candidate_desired_replicas",
		Help: "Desired primary replica units assigned to this member in the current accepted plan. Zero includes draining or retired members; unassigned demand is excluded.",
	}, []string{"namespace", "isvc", "cluster"})

	placementCapacityObservationKnown = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_capacity_observation_known",
		Help: "Whether the current placement plan and all required member capacity observations are known (1) or unknown (0).",
	}, []string{"namespace", "isvc"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		placementPhase, placementWinner, placementCandidate,
		placementCandidateAdmitted, placementCandidateReady,
		placementCandidateObservationKnown, placementCandidateObservationFailingSince,
		placementPolicyInfo, placementSplitReplicas,
		placementSplitMaxPerCluster,
		placementTotalDesiredReplicas, placementCandidateDesiredReplicas, placementCapacityObservationKnown,
	)
}

// recordPlacement publishes the placement metrics for one source ISVC from the
// status this pass just persisted, plus its declared policy. Called from
// writePlacement after a successful status update so the gauges never advertise
// a placement the API server rejected.
func recordPlacement(isvc *v1beta1.InferenceService, res placementResult) {
	if isvc == nil || isvc.Name == "" {
		return
	}
	ns, name := isvc.Namespace, isvc.Name
	match := prometheus.Labels{"namespace": ns, "isvc": name}

	// Phase: reset so only the current phase reports 1.
	placementPhase.DeletePartialMatch(match)
	if res.phase != "" {
		placementPhase.WithLabelValues(ns, name, string(res.phase)).Set(1)
	}

	// Winner: reset so a re-placement onto a different cluster does not leave
	// the previous member reporting 1 alongside the new one.
	placementWinner.DeletePartialMatch(match)
	if res.winner != "" {
		placementWinner.WithLabelValues(ns, name, res.winner).Set(1)
	}

	// Remove every candidate series before recording the saved candidates, so
	// clusters removed from placement stop reporting.
	placementCandidate.DeletePartialMatch(match)
	placementCandidateAdmitted.DeletePartialMatch(match)
	placementCandidateReady.DeletePartialMatch(match)
	placementCandidateObservationKnown.DeletePartialMatch(match)
	placementCandidateObservationFailingSince.DeletePartialMatch(match)
	for _, c := range res.candidates {
		if c.Cluster == "" {
			continue
		}
		placementCandidate.WithLabelValues(ns, name, c.Cluster, string(c.Phase)).Set(1)
		placementCandidateAdmitted.WithLabelValues(ns, name, c.Cluster).Set(float64(c.AdmittedReplicas))
		placementCandidateReady.WithLabelValues(ns, name, c.Cluster).Set(float64(c.ReadyReplicas))
		known := float64(0)
		if c.ObservationKnown {
			known = 1
		}
		placementCandidateObservationKnown.WithLabelValues(ns, name, c.Cluster).Set(known)
		failingSince := float64(0)
		if c.ObservationFailingSince != nil {
			failingSince = float64(c.ObservationFailingSince.Unix())
		}
		placementCandidateObservationFailingSince.WithLabelValues(ns, name, c.Cluster).Set(failingSince)
	}

	recordPolicy(isvc)
	recordCapacity(isvc, res.candidates)
}

// recordCapacity publishes the accepted plan's requested total, each member's
// desired count, and whether every member's capacity observation is known.
// The series are cleared first. The total is published for a current plan, the
// member counts only when the members account for the plan's totals, so a
// member that disappears never leaves its target behind. An unknown
// observation must not reuse the Ready count retained during grace.
func recordCapacity(source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement) {
	ns, name := source.Namespace, source.Name
	match := prometheus.Labels{"namespace": ns, "isvc": name}
	placementTotalDesiredReplicas.DeletePartialMatch(match)
	placementCandidateDesiredReplicas.DeletePartialMatch(match)
	placementCapacityObservationKnown.WithLabelValues(ns, name).Set(0)
	accepted, ok := publishableCapacityPlan(source)
	if !ok {
		return
	}
	placementTotalDesiredReplicas.WithLabelValues(ns, name).Set(float64(accepted.RequestedReplicas))
	targets, ok := readMemberCapacityTargets(accepted, candidates)
	if !ok {
		return
	}
	for cluster, replicas := range targets.desired {
		placementCandidateDesiredReplicas.WithLabelValues(ns, name, cluster).Set(float64(replicas))
	}
	if targets.known {
		placementCapacityObservationKnown.WithLabelValues(ns, name).Set(1)
	}
}

// publishableCapacityPlan is the accepted plan the capacity series describe:
// one accepted for the source as it is now, with nonnegative counts.
func publishableCapacityPlan(source *v1beta1.InferenceService) (*v1beta1.PlacementPlanStatus, bool) {
	if source.Status.Placement == nil || source.Status.Placement.Plan == nil {
		return nil, false
	}
	accepted := source.Status.Placement.Plan
	if source.UID == "" || accepted.SourceUID != source.UID || accepted.ID == "" || accepted.Revision <= 0 || accepted.ObservedGeneration != source.Generation || accepted.RequestedReplicas < 0 || accepted.AssignedReplicas < 0 || accepted.UnassignedReplicas < 0 {
		return nil, false
	}
	return accepted, true
}

// memberCapacityTargets is what the candidates say about the accepted plan's
// capacity: each member's desired count, their sum, and whether every
// member's observation is known.
type memberCapacityTargets struct {
	desired  map[string]int32
	assigned int64
	known    bool
}

// readMemberCapacityTargets reads the candidates' targets. It reports false
// when a candidate cannot be measured (unnamed, repeated, without an
// allocation or with a negative count, or placed without an identified
// cluster) or when the counts do not add up to the plan's.
func readMemberCapacityTargets(accepted *v1beta1.PlacementPlanStatus, candidates []v1beta1.CandidatePlacement) (memberCapacityTargets, bool) {
	targets := memberCapacityTargets{desired: make(map[string]int32, len(candidates)), known: true}
	for _, candidate := range candidates {
		if !targets.add(candidate) {
			return targets, false
		}
	}
	if targets.assigned != accepted.AssignedReplicas || accepted.RequestedReplicas != targets.assigned+int64(accepted.UnassignedReplicas) {
		return targets, false
	}
	return targets, true
}

// add reads one candidate's target; false when the candidate cannot be
// measured. A retired member contributes no capacity, and a trusted positive
// Ready count contradicts its retirement.
func (t *memberCapacityTargets) add(candidate v1beta1.CandidatePlacement) bool {
	allocation := candidate.Allocation
	if candidate.Cluster == "" || allocation == nil || allocation.DesiredReplicas < 0 || allocation.CurrentReplicas < 0 {
		return false
	}
	if _, repeated := t.desired[candidate.Cluster]; repeated {
		return false
	}
	t.desired[candidate.Cluster] = allocation.DesiredReplicas
	t.assigned += int64(allocation.DesiredReplicas)
	if allocationRetired(allocation) {
		t.known = t.known && (!candidate.ObservationKnown || candidate.ReadyReplicas == 0)
		return true
	}
	if allocation.ClusterUID == "" {
		return false
	}
	t.known = t.known && candidate.ObservationKnown && candidate.ReadyReplicas >= 0
	return true
}

// allocationRetired reports an allocation with nothing assigned, nothing
// running and no placement recorded either way.
func allocationRetired(allocation *v1beta1.CandidateAllocationStatus) bool {
	return allocation.DesiredReplicas == 0 && allocation.CurrentReplicas == 0 && allocation.DesiredHome == nil && allocation.CurrentHome == nil
}

// recordPolicy publishes the spec-side placement policy. Split-only numeric
// fields are cleared for a non-Split (or absent) policy so a mode change does
// not leave a stale replica band behind.
func recordPolicy(isvc *v1beta1.InferenceService) {
	ns, name := isvc.Namespace, isvc.Name
	match := prometheus.Labels{"namespace": ns, "isvc": name}

	placementPolicyInfo.DeletePartialMatch(match)
	placementSplitReplicas.DeletePartialMatch(match)
	placementSplitMaxPerCluster.DeletePartialMatch(match)

	p := isvc.Spec.Placement
	if p == nil {
		return
	}

	placementPolicyInfo.WithLabelValues(ns, name, string(p.Policy), string(p.Mode)).Set(1)

	if p.Split == nil {
		return
	}
	if p.Split.Replicas != nil {
		placementSplitReplicas.WithLabelValues(ns, name).Set(float64(*p.Split.Replicas))
	}
	// A source that declares no component reports the engine's cap.
	primary := cmp.Or(primaryScaleComponent(declaredComponents(isvc)), v1beta1.EngineComponent)
	placementSplitMaxPerCluster.WithLabelValues(ns, name).Set(float64(p.Split.Ceiling(primary)))
}

// DeleteForISVC drops every placement series for an ISVC. Called on teardown so
// a deleted ISVC does not keep reporting a phase and a winning cluster.
func DeleteForISVC(namespace, isvc string) {
	if isvc == "" {
		return
	}
	match := prometheus.Labels{"namespace": namespace, "isvc": isvc}
	placementPhase.DeletePartialMatch(match)
	placementWinner.DeletePartialMatch(match)
	placementCandidate.DeletePartialMatch(match)
	placementCandidateAdmitted.DeletePartialMatch(match)
	placementCandidateReady.DeletePartialMatch(match)
	placementCandidateObservationKnown.DeletePartialMatch(match)
	placementCandidateObservationFailingSince.DeletePartialMatch(match)
	placementPolicyInfo.DeletePartialMatch(match)
	placementSplitReplicas.DeletePartialMatch(match)
	placementSplitMaxPerCluster.DeletePartialMatch(match)
	placementTotalDesiredReplicas.DeletePartialMatch(match)
	placementCandidateDesiredReplicas.DeletePartialMatch(match)
	placementCapacityObservationKnown.DeletePartialMatch(match)
}
