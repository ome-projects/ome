package placement

import (
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// DefaultStatusSafetyRequeue is the fallback steady-state re-read backstop. It
// is NOT the convergence latency (the funnel re-reconciles on events) — only the
// backstop for a missed event (dropped under a full channel, a watch gap).
// Config-driven; this is the graceful-degradation default.
const DefaultStatusSafetyRequeue = 10 * time.Minute

// DefaultStatusBatchPeriod is the fallback debounce folding a burst of funnel
// events for one ISVC into a single reconcile. Config-driven; graceful default.
const DefaultStatusBatchPeriod = 1 * time.Second

// convergeConfig is the resolved status-convergence configuration assembled from
// functional Options at SetupWithManager. Every field has a
// graceful-degradation default applied by resolveConvergeConfig so an unset
// option never injects a magic literal mid-package.
type convergeConfig struct {
	// statusEvents contains the remote watch-funnel channels for event-driven
	// convergence. Nil leaves the controller poll-only (it still re-reconciles on
	// the safety requeue), so the funnel is an optimization, not a dependency.
	statusEvents []<-chan event.GenericEvent
	// batchPeriod debounces funnel events before they enqueue a reconcile.
	batchPeriod time.Duration
	// safetyRequeue is the long steady-state re-read backstop for the
	// status-convergence path.
	safetyRequeue time.Duration
}

// ConvergeOption mutates the status-convergence configuration at
// SetupWithManager. Functional options keep the funnel channel and the timing
// knobs injectable so tests can wire a fake channel and shrink the batch/safety
// windows from minutes to milliseconds without touching package state.
type ConvergeOption func(*convergeConfig)

// WithStatusEvents adds a remote watch-funnel channel the controller consumes
// via source.Channel. Without it the controller is poll-only (safety requeue).
func WithStatusEvents(ch <-chan event.GenericEvent) ConvergeOption {
	return func(c *convergeConfig) {
		if ch != nil {
			c.statusEvents = append(c.statusEvents, ch)
		}
	}
}

// WithStatusBatchPeriod overrides the funnel-event debounce window. A
// non-positive value falls back to DefaultStatusBatchPeriod.
func WithStatusBatchPeriod(d time.Duration) ConvergeOption {
	return func(c *convergeConfig) { c.batchPeriod = d }
}

// WithStatusSafetyRequeue overrides the long steady-state re-read backstop. A
// non-positive value falls back to DefaultStatusSafetyRequeue.
func WithStatusSafetyRequeue(d time.Duration) ConvergeOption {
	return func(c *convergeConfig) { c.safetyRequeue = d }
}

// resolveConvergeConfig builds the convergeConfig from options, then fills any
// still-unset timing field with its graceful-degradation default.
func resolveConvergeConfig(opts ...ConvergeOption) convergeConfig {
	c := convergeConfig{}
	for _, o := range opts {
		o(&c)
	}
	if c.batchPeriod <= 0 {
		c.batchPeriod = DefaultStatusBatchPeriod
	}
	if c.safetyRequeue <= 0 {
		c.safetyRequeue = DefaultStatusSafetyRequeue
	}
	return c
}

// safetyRequeue returns the long steady-state re-read backstop for the
// status-convergence path. Falls back to the seeded Reconciler.Requeue (legacy
// poll cadence) when SetupWithManager has not resolved a convergeConfig (e.g.
// unit tests building the Reconciler directly), so the existing struct-field API
// keeps working; absent both, the package default applies.
func (r *Reconciler) safetyRequeue() time.Duration {
	if r.converge != nil && r.converge.safetyRequeue > 0 {
		return r.converge.safetyRequeue
	}
	if r.Requeue > 0 {
		return r.Requeue
	}
	return DefaultStatusSafetyRequeue
}

// localKeyForDerived resolves a remote derived InferenceService delivered by a
// workload-cluster informer to the LOCAL (control-plane) source ISVC key, and
// reports whether it is a derived this control plane owns. A derived shares its
// source's namespace/name (placeOn preserves identity) and carries the
// placement-origin marker; when a control-plane identity is configured the
// control-plane label must also match, so one control plane never re-reconciles
// off another's deriveds on a shared workload cluster. An object lacking the
// origin marker (a user's same-named ISVC) is rejected (ok=false).
//
// controlPlaneID is the operative control-plane identity (config-driven). The
// empty value disables the control-plane-label check (single-control-plane
// behavior) but still requires the origin marker — a blank marker never matches.
func localKeyForDerived(remote client.Object, controlPlaneID string) (types.NamespacedName, bool) {
	if remote == nil {
		return types.NamespacedName{}, false
	}
	lbls := remote.GetLabels()
	anns := remote.GetAnnotations()
	// Require the origin marker (label or annotation), non-empty: this is what
	// distinguishes a copy WE derived from a same-named user ISVC.
	if lbls[PlacementOriginLabel] == "" && anns[PlacementOriginUIDAnnotation] == "" {
		return types.NamespacedName{}, false
	}
	// Ownership scoping: when this control plane has an identity, only react to
	// deriveds it stamped. An unstamped derived under a configured identity is
	// another control plane's (or pre-dates the stamp) — not ours to converge.
	if controlPlaneID != "" && lbls[PlacementControlPlaneLabel] != controlPlaneID {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: remote.GetNamespace(), Name: remote.GetName()}, true
}

// originWatchSelector builds the label selector that scopes the funnel's
// establish-watch to this control plane's derived ISVCs: the origin label must
// exist, and (when an identity is configured) the control-plane label must equal
// it. It keeps the remote apiserver from streaming derived objects this control
// plane does not own. Returns labels.Everything() only if the requirements
// cannot be constructed (never expected) so the watch still functions.
func originWatchSelector(controlPlaneID string) labels.Selector {
	sel := labels.NewSelector()
	originExists, err := labels.NewRequirement(PlacementOriginLabel, selection.Exists, nil)
	if err != nil {
		return labels.Everything()
	}
	sel = sel.Add(*originExists)
	if controlPlaneID != "" {
		cp, err := labels.NewRequirement(PlacementControlPlaneLabel, selection.Equals, []string{controlPlaneID})
		if err != nil {
			return sel
		}
		sel = sel.Add(*cp)
	}
	return sel
}

// FunnelConfigFor builds the workloadcluster.FunnelConfig for the cross-cluster
// status watch funnel: the watched kind is the derived InferenceService, the
// watch is scoped to this control plane's deriveds, and each remote event is
// resolved to the local source key. cmd wiring passes the result to
// workloadcluster.NewStatusFunnel; the funnel never imports placement, so the
// origin/control-plane label semantics live here. batchPeriod/safetyRequeue
// timing stays on the controller side — this configures only the funnel.
func FunnelConfigFor(controlPlaneID string) workloadcluster.FunnelConfig {
	return workloadcluster.FunnelConfig{
		NewList:   func() client.ObjectList { return &v1beta1.InferenceServiceList{} },
		NewObject: func() client.Object { return &v1beta1.InferenceService{} },
		Resolve: func(remote client.Object) (types.NamespacedName, bool) {
			return localKeyForDerived(remote, controlPlaneID)
		},
		WatchSelector: originWatchSelector(controlPlaneID),
	}
}
