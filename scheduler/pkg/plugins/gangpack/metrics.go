package gangpack

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// Metrics are registered into kube-scheduler's global (legacy) registry, so they
// are served on the scheduler's existing /metrics endpoint alongside the built-in
// framework metrics — no extra serving to wire. ALPHA stability: these are
// OME-specific and may evolve.
const metricsSubsystem = "ome_scheduler"

var (
	// gangAssignmentTotal, exported as gang_pin_total, counts domain-placement
	// decisions by result: pinned (a domain chosen and assigned), no_fit (no
	// domain has the capacity), reserved (a domain has the capacity but a forming
	// gang holds it), adopted, stale_replan, topology_replan.
	gangAssignmentTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "gang_pin_total",
			Help:           "Gang domain-placement decisions by result (pinned, no_fit, reserved, adopted, stale_replan, topology_replan).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// gangGateTotal counts Permit gate evaluations by result: "wait" (a member
	// held because the gang is incomplete) or "admit" (the arriving member
	// completed the gang and released it).
	gangGateTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "gang_gate_total",
			Help:           "Permit gate evaluations by result (wait, admit).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// gangActivationTotal counts explicit activations by trigger: "permit" (a
	// member reached the gate), "templates_complete" (the live member set
	// reached minMember after a member had waited short of it),
	// "reservation_released" (a forming gang's domain reservation drained or was
	// released, requeuing the pods it had kept out of the domain),
	// "podgroup_change" (the plugin's PodGroup informer stored a new or edited
	// PodGroup, requeuing the gang's members) or "domain_failed" (an assigned
	// member was vetoed in a domain new to its gang while another is untried,
	// and retries to plan there).
	gangActivationTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "gang_activation_total",
			Help:           "Explicit pod activations by trigger (permit, templates_complete, reservation_released, podgroup_change, domain_failed).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"trigger"},
	)

	// gangUnwindTotal counts gangs torn down by Unreserve after a member failed to
	// schedule (gate timeout or bind failure).
	gangUnwindTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "gang_unwind_total",
			Help:           "Gangs unwound (Unreserve) after a member failed to schedule.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// gangPreemptionTotal counts preemption decisions by result: "evicted"
	// (lower-priority pods evicted and a domain held), "held" (a domain held while pods
	// already shutting down there leave), "in_flight_limit" (the configured
	// number of preemptions was already in flight), "no_domain" (no domain fits
	// the gang even with every lower-priority unit gone) or "hold_expired" (a
	// held domain still could not fit its gang when the hold's time was up).
	gangPreemptionTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "gang_preemption_total",
			Help:           "Gang preemption decisions by result (evicted, held, in_flight_limit, no_domain, hold_expired).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// gangPreemptionEvictedPods counts the pods gang preemption evicted.
	gangPreemptionEvictedPods = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "gang_preemption_evicted_pods_total",
			Help:           "Pods evicted by gang preemption.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// assignedGroups, exported as pinned_groups, is how many placement groups
	// currently hold a domain assignment (and its capacity reservation).
	assignedGroups = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      metricsSubsystem,
			Name:           "pinned_groups",
			Help:           "Placement groups currently pinned to a domain.",
			StabilityLevel: metrics.ALPHA,
		},
	)
)

var registerMetricsOnce sync.Once

// registerMetrics registers the plugin's metrics into the scheduler's global
// registry exactly once.
func registerMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(gangAssignmentTotal, gangGateTotal, gangActivationTotal, gangUnwindTotal, assignedGroups,
			gangPreemptionTotal, gangPreemptionEvictedPods)
	})
}

func init() { registerMetrics() }
