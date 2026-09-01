package inferencereplica

import (
	"context"
	"fmt"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/utils"
)

// SetupWithManager wires the Reconciler into the controller manager.
//
// Watches:
//   - InferenceReplica (primary): re-enqueue on spec / status changes.
//   - Pods: a custom handler that BOTH updates the Expectations cache
//     (ObservedCreate / ObservedDelete) AND re-enqueues the owning IR.
//     The expectations update is load-bearing: the workload dispatcher's
//     destructive steps (surge-pod create, old-pod delete during
//     SurgeThenDrain / RecreatePod / Migrate) gate on
//     ExpectationsCache().Satisfied(). Without an observer feeding the
//     SAME cache the dispatcher reads, those gates only release on the
//     2-minute TTL — every rollout stalls with the Instance pinned at
//     Phase=Updating and readyReplicas=0. Owns(Pod) alone would
//     re-enqueue but never feed the cache, so we use Watches with the
//     observe-then-enqueue handler instead.
//   - ControllerRevisions (owned): re-enqueue on revision GC so the
//     workload-side retention sweep doesn't fight a manual delete.
//   - HorizontalPodAutoscaler (owned): re-enqueue when the dispatcher-
//     emitted HPA changes — the status writer mirrors
//     HPA.Status.Conditions onto the IR / ISVC status surface.
//   - ScaledObject (owned): watched only when the KEDA CRD is present
//     (probed at setup); an unconditional watch would fail manager
//     startup when KEDA is absent (cache-sync timeout).
//
// The parent InferenceService is NOT watched — the IR's owner ref to
// its parent means the parent's controller already gets re-enqueued
// on IR status churn via ISVC's Owns(&InferenceReplica{}).
//
// Services / PodMonitor are NOT watched: the IR re-reconciles its
// headless Service unconditionally every pass (drift self-heals on the
// next reconcile), and the routing Services / PodMonitor belong to the
// ISVC controller — the IR controller can't observe-only on resources
// it doesn't manage without confusing the owner-ref shape.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Own an Expectations cache so this controller is self-consistent:
	// the same instance is read by the workload dispatcher (via
	// Deps.Expectations below — Reconcile threads r.Expectations) and
	// written by the pod event handler. Mirrors how the ISVC controller
	// seeds omenative.NewExpectations(). When nil the workload dispatcher
	// would fall back to the global singleton, which the pod handler does
	// not feed — the bug this fixes.
	if r.Expectations == nil {
		r.Expectations = workload.NewExpectations()
	}
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	// Cache dynamically resolved lifecycle settings with the shared controller
	// cache. The flag-driven TTL keeps those ConfigMap edits applying without a
	// restart.
	if r.ConfigCache == nil {
		r.ConfigCache = controllerconfig.NewConfigCache(r.ConfigCacheTTL)
	}
	// Discover the scheduler-plugins PodGroup CRD once at setup (mirrors the
	// ISVC controller). Gates per-Instance PodGroup creation for multi-pod
	// Instances; absence is a degradation surface, not a hard fail (the
	// reconciler stamps GangSchedulingUnavailable and skips PodGroup creation).
	podGroupFound, err := utils.IsCrdAvailable(mgr.GetConfig(), schedulingv1alpha1.SchemeGroupVersion.String(), constants.PodGroupKind)
	if err != nil {
		return err
	}
	r.GangSchedulingAvailable = podGroupFound
	if podGroupFound {
		if err := workloadgang.RegisterPodGroupControllerUIDIndex(context.Background(), mgr.GetFieldIndexer()); err != nil {
			return fmt.Errorf("inferencereplica: register PodGroup controller UID index: %w", err)
		}
	}
	if err := r.validateWiring(); err != nil {
		return err
	}

	// Optional CRD (mirrors the ISVC controller): only watch ScaledObject when
	// present, else manager startup fails on cache sync when KEDA is absent.
	kedaFound, err := utils.IsCrdAvailable(mgr.GetConfig(), kedav1.SchemeGroupVersion.String(), constants.KEDAScaledObjectKind)
	if err != nil {
		return err
	}

	b := ctrl.NewControllerManagedBy(mgr).
		// MaxConcurrentReconciles parallelizes reconciles for distinct IRs;
		// zero (unset) falls back to controller-runtime's single-worker default.
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		For(&v1beta1.InferenceReplica{}).
		Owns(&appsv1.ControllerRevision{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{})
	if podGroupFound {
		b = b.Owns(&schedulingv1alpha1.PodGroup{}, builder.WithPredicates(podGroupPredicate()))
	}
	if kedaFound {
		b = b.Owns(&kedav1.ScaledObject{})
	} else {
		r.Log.Info("The InferenceReplica controller won't watch keda.sh/v1alpha1/ScaledObject resources because the CRD is not available; InferenceReplicas requesting KEDA autoscaling will fail on reconcile until KEDA is installed.")
	}
	b = b.
		Watches(
			&corev1.Pod{},
			newPodEventHandler(r.Expectations),
			builder.WithPredicates(managedByOMENativePredicate()),
		).
		// EndpointSlice events for OMENative drain Services (per-Component
		// headless + per-revision routed) enqueue the owning IR so the
		// SurgeThenDrain / RecreatePod drain step reacts to kube-proxy
		// convergence immediately instead of waiting on the
		// workloadops.UpdateRequeueInterval poll tick. The mapper rejects
		// slices that don't target an OMENative drain Service.
		Watches(
			&discoveryv1.EndpointSlice{},
			handler.EnqueueRequestsFromMapFunc(EndpointSliceToIR),
		)
	if r.CapabilityReadiness != nil {
		// The buffered sentinel can enter the work queue while sources start,
		// but no worker can process it until every syncing source's WaitForSync
		// has completed. That worker handoff is the publisher's readiness proof.
		b = b.WatchesRawSource(capabilityReadinessSource())
	}
	return b.Complete(r.reconcilerWithCapabilityReadiness())
}

// validateWiring rejects a mis-wired reconciler at setup. The
// authoritative (live) reader is a correctness dependency — see
// workload/types AuthoritativeReader.
func (r *Reconciler) validateWiring() error {
	if r.APIReader == nil {
		return fmt.Errorf("inferencereplica: APIReader (AuthoritativeReader) must be wired")
	}
	return nil
}
