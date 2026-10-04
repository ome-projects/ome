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
	// gangPinTotal counts domain-placement decisions by result: pinned,
	// no_fit (no domain has the capacity), reserved (a domain has the capacity
	// but a forming gang holds it), adopted, stale_replan, topology_replan.
	gangPinTotal = metrics.NewCounterVec(
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
	// reached minMember after a member had parked short of it),
	// "reservation_released" (a forming gang's domain reservation drained or was
	// released, waking the pods it had kept out of the domain) or
	// "podgroup_change" (the plugin's PodGroup informer stored a new or edited
	// PodGroup, waking the gang's members).
	gangActivationTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "gang_activation_total",
			Help:           "Explicit pod activations by trigger (permit, templates_complete, reservation_released, podgroup_change).",
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

	// pinnedGroups tracks how many placement groups are currently pinned to a
	// domain (holding a capacity reservation).
	pinnedGroups = metrics.NewGauge(
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
		legacyregistry.MustRegister(gangPinTotal, gangGateTotal, gangActivationTotal, gangUnwindTotal, pinnedGroups)
	})
}

func init() { registerMetrics() }
