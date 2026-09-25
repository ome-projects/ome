package placement

import (
	"strconv"

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
		Help: "Replicas serving traffic on this candidate cluster. In Split mode this is the weight an external LB uses to split traffic across homes.",
	}, []string{"namespace", "isvc", "cluster"})

	// Policy is spec-derived, not status-derived: it answers "what was asked
	// for" so it can be compared against where the workload actually landed.
	// The selector strings are free-form, but they vary per ISVC rather than per
	// scrape, so the series count still tracks the live ISVC count.
	placementPolicyInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_policy_info",
		Help: "Always 1, labelled with the InferenceService's declared placement policy: mode, accelerator requirements, cluster selector, and Split spread. Exactly one series per ISVC that declares a placement.",
	}, []string{"namespace", "isvc", "mode", "requirements", "cluster_selector", "spread"})

	placementSplitReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_split_replicas",
		Help: "Desired total replicas to distribute across homes, from spec.placement.split.replicas.",
	}, []string{"namespace", "isvc"})

	placementSplitMaxPerCluster = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_split_max_replicas_per_cluster",
		Help: "Per-cluster replica ceiling from spec.placement.split.maxReplicasPerCluster (0 = uncapped).",
	}, []string{"namespace", "isvc"})

	placementSplitMinPerCluster = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_isvc_placement_split_min_replicas_per_cluster",
		Help: "Per-cluster replica floor from spec.placement.split.minReplicasPerCluster (0 = unset).",
	}, []string{"namespace", "isvc"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		placementPhase, placementWinner, placementCandidate,
		placementCandidateAdmitted, placementCandidateReady,
		placementPolicyInfo, placementSplitReplicas,
		placementSplitMaxPerCluster, placementSplitMinPerCluster,
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
	// the previous home reporting 1 alongside the new one.
	placementWinner.DeletePartialMatch(match)
	if res.winner != "" {
		placementWinner.WithLabelValues(ns, name, res.winner).Set(1)
	}

	// Candidates: reset all three vectors so a cluster dropped from the race
	// (loser sweep) stops reporting.
	placementCandidate.DeletePartialMatch(match)
	placementCandidateAdmitted.DeletePartialMatch(match)
	placementCandidateReady.DeletePartialMatch(match)
	for _, c := range res.candidates {
		if c.Cluster == "" {
			continue
		}
		placementCandidate.WithLabelValues(ns, name, c.Cluster, string(c.Phase)).Set(1)
		placementCandidateAdmitted.WithLabelValues(ns, name, c.Cluster).Set(float64(c.AdmittedReplicas))
		placementCandidateReady.WithLabelValues(ns, name, c.Cluster).Set(float64(c.ReadyReplicas))
	}

	recordPolicy(isvc)
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
	placementSplitMinPerCluster.DeletePartialMatch(match)

	p := isvc.Spec.Placement
	if p == nil {
		return
	}

	spread := false
	if p.Split != nil {
		spread = p.Split.Spread
	}
	placementPolicyInfo.WithLabelValues(
		ns, name, string(p.Mode), p.Requirements, p.ClusterSelector, strconv.FormatBool(spread),
	).Set(1)

	if p.Split == nil {
		return
	}
	if p.Split.Replicas != nil {
		placementSplitReplicas.WithLabelValues(ns, name).Set(float64(*p.Split.Replicas))
	}
	placementSplitMaxPerCluster.WithLabelValues(ns, name).Set(float64(p.Split.MaxReplicasPerCluster))
	placementSplitMinPerCluster.WithLabelValues(ns, name).Set(float64(p.Split.MinReplicasPerCluster))
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
	placementPolicyInfo.DeletePartialMatch(match)
	placementSplitReplicas.DeletePartialMatch(match)
	placementSplitMaxPerCluster.DeletePartialMatch(match)
	placementSplitMinPerCluster.DeletePartialMatch(match)
}
