package coordination

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Coordination layer Prometheus metrics. Registered with the
// controller-runtime metrics registry at package init so the operator's
// /metrics endpoint surfaces them without additional wiring.
//
// Every per-ISVC vector carries both a "namespace" and an "isvc" label so
// same-named ISVCs across namespaces do not collide, and so the series can be
// dropped wholesale on ISVC teardown (see DeleteForISVC) to bound cardinality.
var (
	groupPhaseTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_rollout_group_phase_total",
			Help: "Count of reconciles per (namespace, isvc, group, phase) for OMENative coordination groups.",
		},
		[]string{"namespace", "isvc", "group", "phase"},
	)

	groupTransitionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_rollout_group_transition_total",
			Help: "Count of phase transitions per (namespace, isvc, group, from, to) for OMENative coordination groups.",
		},
		[]string{"namespace", "isvc", "group", "from", "to"},
	)

	groupFailureTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_rollout_group_failure_total",
			Help: "Count of group entries into the Failed phase per (namespace, isvc, group, reason).",
		},
		[]string{"namespace", "isvc", "group", "reason"},
	)

	ratioSkewTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_ratio_skew_total",
			Help: "Count of reconciles that rejected a surge to avoid cross-Component ratio skew.",
		},
		[]string{"namespace", "isvc", "group"},
	)

	perRevisionServiceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ome_omenative_per_revision_service_total",
			Help: "Number of live per-revision Services per (namespace, isvc, component).",
		},
		[]string{"namespace", "isvc", "component"},
	)

	mixedPairingTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_mixed_pairing_total",
			Help: "Count of distinct cross-revision pairings observed in a RollingUpdate group.",
		},
		[]string{"namespace", "isvc", "group"},
	)

	autoMigrationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_auto_migrations_total",
			Help: "Count of relocation directives the deadline disposition recorded to recover an Instance whose attempt expired (config-driven stuckPodGracePeriod / instanceReadyTimeout), per (namespace, isvc, component, reason).",
		},
		[]string{"namespace", "isvc", "component", "reason"},
	)

	ratioGateBypassedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_ratio_gate_bypassed_total",
			Help: "Number of times the RatioBalanced serving-capacity band was waived for an in-place strategy because the drain is paired 1:1 with a same-pod return; the new-revision lockstep still ran.",
		},
		[]string{"component", "strategy"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		groupPhaseTotal,
		groupTransitionTotal,
		groupFailureTotal,
		ratioSkewTotal,
		perRevisionServiceGauge,
		mixedPairingTotal,
		autoMigrationsTotal,
		ratioGateBypassedTotal,
	)
}

// RecordAutoMigrationTriggered increments the auto-migration counter
// when the deadline decision records a relocation directive
// (a terminal AutoRecover record) for an Instance whose attempt
// expired via the config-driven stuckPodGracePeriod fast path or the
// instanceReadyTimeout backstop.
func RecordAutoMigrationTriggered(namespace, isvc, component, reason string) {
	if isvc == "" || component == "" || reason == "" {
		return
	}
	autoMigrationsTotal.WithLabelValues(namespace, isvc, component, reason).Inc()
}

// RecordGroupPhase increments the phase counter for one reconcile.
// Callers invoke this once per (group, reconcile).
func RecordGroupPhase(namespace, isvc, group, phase string) {
	if isvc == "" || group == "" || phase == "" {
		return
	}
	groupPhaseTotal.WithLabelValues(namespace, isvc, group, phase).Inc()
}

// RecordGroupTransition increments the transition counter when a
// group's phase changes between reconciles.
func RecordGroupTransition(namespace, isvc, group, from, to string) {
	if isvc == "" || group == "" || from == to {
		return
	}
	groupTransitionTotal.WithLabelValues(namespace, isvc, group, from, to).Inc()
}

// RecordGroupFailure increments the failure counter when a group
// enters the Failed phase.
func RecordGroupFailure(namespace, isvc, group, reason string) {
	if isvc == "" || group == "" {
		return
	}
	groupFailureTotal.WithLabelValues(namespace, isvc, group, reason).Inc()
}

// RecordRatioSkew increments the ratio-skew counter when RatioBalanced
// pacing refuses a surge.
func RecordRatioSkew(namespace, isvc, group string) {
	if isvc == "" || group == "" {
		return
	}
	ratioSkewTotal.WithLabelValues(namespace, isvc, group).Inc()
}

// SetPerRevisionServiceCount updates the gauge tracking the number of
// live per-revision Services for one (isvc, component).
func SetPerRevisionServiceCount(namespace, isvc, component string, count float64) {
	if isvc == "" || component == "" {
		return
	}
	perRevisionServiceGauge.WithLabelValues(namespace, isvc, component).Set(count)
}

// RecordMixedPairing increments the mixed-pairing counter when a
// RollingUpdate group sees a distinct revision-pair combination.
func RecordMixedPairing(namespace, isvc, group string) {
	if isvc == "" || group == "" {
		return
	}
	mixedPairingTotal.WithLabelValues(namespace, isvc, group).Inc()
}

// RecordRatioGateBypassed increments the bypass counter when the
// OMENative ops dispatcher admits an in-place start (InPlaceIfPossible /
// InPlaceOnly) past the RatioBalanced serving-capacity tolerance; the
// version-progress check between the two sides still ran for it. See EventReasonRatioGateBypassed
// for the rationale.
func RecordRatioGateBypassed(component, strategy string) {
	if component == "" || strategy == "" {
		return
	}
	ratioGateBypassedTotal.WithLabelValues(component, strategy).Inc()
}

// DeleteForISVC drops every coordination series carrying the given
// (namespace, isvc) label pair. Called on terminal ISVC delete so the
// per-ISVC vectors do not leak unbounded series after teardown. The
// ratioGateBypassedTotal vector is not keyed by isvc, so it is unaffected.
func DeleteForISVC(namespace, isvc string) {
	if isvc == "" {
		return
	}
	match := prometheus.Labels{"namespace": namespace, "isvc": isvc}
	groupPhaseTotal.DeletePartialMatch(match)
	groupTransitionTotal.DeletePartialMatch(match)
	groupFailureTotal.DeletePartialMatch(match)
	ratioSkewTotal.DeletePartialMatch(match)
	perRevisionServiceGauge.DeletePartialMatch(match)
	mixedPairingTotal.DeletePartialMatch(match)
	autoMigrationsTotal.DeletePartialMatch(match)
}
