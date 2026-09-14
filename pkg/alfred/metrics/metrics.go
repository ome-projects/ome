// Package metrics defines every Prometheus series Alfred exports (OEP-0008
// §Observability), constructed against an injectable registerer so tests can
// use a private registry. All series share the alfred_ prefix.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metrics holds every Alfred series. Snapshot-derived gauges are published by
// the observation loop; decision-side series are emitted only by the engine's
// Reporter stage.
type Metrics struct {
	// Snapshot / fragmentation gauges.
	ClusterFragmentationScore prometheus.Gauge
	FragmentationObserved     *prometheus.GaugeVec // pool, size
	FragmentationReclaimable  *prometheus.GaugeVec // pool
	PendingPressure           *prometheus.GaugeVec // pool
	GPUCapacity               *prometheus.GaugeVec // node, status
	PendingPodCount           prometheus.Gauge
	PendingPodGPURequirements *prometheus.GaugeVec // size
	SurgeHeadroomGPUs         *prometheus.GaugeVec // pool

	// Recommendation / migration counters (Reporter-only).
	RecommendationsProduced *prometheus.CounterVec // policy, workload, component, reason, executable
	RecommendationsAccepted *prometheus.CounterVec // policy, workload, component
	RecommendationsRejected *prometheus.CounterVec // policy, workload, component, reason
	MigrationCalls          *prometheus.CounterVec // policy, workload, mode, surface
	MigrationOutcome        *prometheus.CounterVec // policy, workload, mode, outcome
	LWSRecommendations      *prometheus.CounterVec // isvc, action

	// Node-health counters (Reporter-only).
	NodeHealthEvacuations  *prometheus.CounterVec // node, workload, surface, outcome
	NodeHealthSignals      *prometheus.CounterVec // node, reason
	NodeMaintenanceSignals *prometheus.CounterVec // node, reason
	CooldownOverrides      *prometheus.CounterVec // policy

	// Loop / operational.
	ObservationLoopDuration prometheus.Histogram
	DecisionLoopDuration    prometheus.Histogram
	LeaderStatus            *prometheus.GaugeVec   // pod
	PolicyReload            *prometheus.CounterVec // outcome
	CircuitBreakerState     prometheus.Gauge
	OMENativeUnavailable    prometheus.Gauge
}

// New constructs and registers every series. A nil registerer registers into
// the controller-runtime registry served on the manager's metrics endpoint.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = ctrlmetrics.Registry
	}
	factory := promauto.With(reg)
	return &Metrics{
		ClusterFragmentationScore: factory.NewGauge(prometheus.GaugeOpts{
			Name: "alfred_cluster_fragmentation_score",
			Help: "Combined fragmentation gate value in [0,1]: max over hardware pools of the reclaimable-or-pending score.",
		}),
		FragmentationObserved: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "alfred_fragmentation_observed",
			Help: "Frag(c,s): fraction of the pool's free GPUs a demand of the labeled size cannot use.",
		}, []string{"pool", "size"}),
		FragmentationReclaimable: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "alfred_fragmentation_reclaimable",
			Help: "Share of observed fragmentation that migrating movable workloads could fix.",
		}, []string{"pool"}),
		PendingPressure: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "alfred_pending_pressure",
			Help: "P(c): age-weighted pressure from pending pods a repack could seat.",
		}, []string{"pool"}),
		GPUCapacity: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "alfred_gpu_capacity",
			Help: "Per-node GPU capacity; status is total, allocated, free, or contiguous_max.",
		}, []string{"node", "status"}),
		PendingPodCount: factory.NewGauge(prometheus.GaugeOpts{
			Name: "alfred_pending_pod_count",
			Help: "Unscheduled GPU-requesting pods, real and virtual.",
		}),
		PendingPodGPURequirements: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "alfred_pending_pod_gpu_requirements",
			Help: "Pending GPU demand bucketed by per-pod request size.",
		}, []string{"size"}),
		SurgeHeadroomGPUs: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "alfred_surge_headroom_gpus",
			Help: "Largest replacement footprint that could surge right now per pool; 0 means surge-shaped migration is infeasible.",
		}, []string{"pool"}),

		RecommendationsProduced: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_recommendations_produced_total",
			Help: "Candidates produced by policies, labeled by outcome-relevant attributes.",
		}, []string{"policy", "workload", "component", "reason", "executable"}),
		RecommendationsAccepted: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_recommendations_accepted_total",
			Help: "Candidates admitted by the arbiter.",
		}, []string{"policy", "workload", "component"}),
		RecommendationsRejected: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_recommendations_rejected_total",
			Help: "Candidates rejected or withheld by the arbiter, with the drop reason.",
		}, []string{"policy", "workload", "component", "reason"}),
		MigrationCalls: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_migration_calls_total",
			Help: "Migration requests dispatched; surface is omenative, rollingrestart, or eviction.",
		}, []string{"policy", "workload", "mode", "surface"}),
		MigrationOutcome: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_migration_outcome_total",
			Help: "Terminal outcomes of dispatched migrations: completed, failed, or timeout.",
		}, []string{"policy", "workload", "mode", "outcome"}),
		LWSRecommendations: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_lws_recommendations_total",
			Help: "Advisory recommendations for LWS-backed workloads Alfred never executes.",
		}, []string{"isvc", "action"}),

		NodeHealthEvacuations: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_nodehealth_evacuations_total",
			Help: "Evacuation actions Policy #2 dispatched.",
		}, []string{"node", "workload", "surface", "outcome"}),
		NodeHealthSignals: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_nodehealth_signals_total",
			Help: "Signal-only outcomes where the caretaker emitted a signal instead of acting.",
		}, []string{"node", "reason"}),
		NodeMaintenanceSignals: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_nodemaintenance_signals_total",
			Help: "Planned-maintenance lifecycle signals, separate from node health damage.",
		}, []string{"node", "reason"}),
		CooldownOverrides: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_cooldown_overrides_total",
			Help: "Health-evacuation admissions under the standard cooldown (each also emits CooldownOverriddenForEvacuation).",
		}, []string{"policy"}),

		ObservationLoopDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "alfred_observation_loop_duration_seconds",
			Help:    "Duration of one observation pass (snapshot build + gauge publish).",
			Buckets: prometheus.DefBuckets,
		}),
		DecisionLoopDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "alfred_decision_loop_duration_seconds",
			Help:    "Duration of one decision pass (policies + arbiter + reporter + dispatch).",
			Buckets: prometheus.DefBuckets,
		}),
		LeaderStatus: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "alfred_leader_status",
			Help: "1 on the replica currently holding leadership, 0 otherwise.",
		}, []string{"pod"}),
		PolicyReload: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "alfred_policy_reload_total",
			Help: "Config reload attempts by outcome (success or failure).",
		}, []string{"outcome"}),
		CircuitBreakerState: factory.NewGauge(prometheus.GaugeOpts{
			Name: "alfred_circuit_breaker_state",
			Help: "1 while the execution circuit breaker is open, 0 while closed.",
		}),
		OMENativeUnavailable: factory.NewGauge(prometheus.GaugeOpts{
			Name: "alfred_omenative_unavailable",
			Help: "1 while no OMENative executor is available (multi-pod candidates degrade to advisory).",
		}),
	}
}

// ObserveConfigReload implements the config package's ReloadObserver.
func (m *Metrics) ObserveConfigReload(outcome string) {
	m.PolicyReload.WithLabelValues(outcome).Inc()
}

// ResetSnapshotGauges clears the node- and pool-keyed snapshot gauges before
// a republish so series for departed nodes and classes do not linger.
func (m *Metrics) ResetSnapshotGauges() {
	m.FragmentationObserved.Reset()
	m.FragmentationReclaimable.Reset()
	m.PendingPressure.Reset()
	m.GPUCapacity.Reset()
	m.PendingPodGPURequirements.Reset()
	m.SurgeHeadroomGPUs.Reset()
}
