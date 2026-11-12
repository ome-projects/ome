package runtimerevision

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch

// gcRequestKey is the singleton request key. ControllerRevision and
// ISVC events all coalesce to one reconcile per loop because the plan
// is a whole-world computation.
var gcRequestKey = types.NamespacedName{Name: "runtime-revision-gc"}

// GCReconciler periodically prunes OME-owned ControllerRevisions in
// the OME namespace to the configured retention.
type GCReconciler struct {
	client.Client
	Log                 logr.Logger
	OMENamespace        string
	RetentionPerRuntime int
	GracePeriod         time.Duration
	// Resync caps the gap between event-driven runs so a Mark whose
	// grace expires without any other event still gets a Delete pass.
	Resync time.Duration
	// Clock is injectable for tests.
	Clock clock.Clock
}

func (r *GCReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}

	revs, err := r.listOMERevisions(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	referenced, err := r.referencedRevisionNames(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	decisions := Plan(revs, referenced, r.Clock.Now(), r.RetentionPerRuntime, r.GracePeriod, constants.RuntimeRevisionGCEligibleSinceKey)
	for i := range decisions {
		if err := r.apply(ctx, &decisions[i]); err != nil {
			// Log + keep going; one bad revision shouldn't stall GC for
			// the rest. Next resync will retry.
			r.Log.Error(err, "GC apply failed",
				"revision", decisions[i].Revision.Name,
				"action", decisions[i].Action)
		}
	}
	return ctrl.Result{RequeueAfter: r.Resync}, nil
}

// listOMERevisions returns ControllerRevisions in the OME namespace
// that the OME controller created (gated by created-by annotation).
// StatefulSet/DaemonSet revisions are filtered out — they have their
// own management.
func (r *GCReconciler) listOMERevisions(ctx context.Context) ([]appsv1.ControllerRevision, error) {
	var list appsv1.ControllerRevisionList
	if err := r.List(ctx, &list, client.InNamespace(r.OMENamespace)); err != nil {
		return nil, fmt.Errorf("list ControllerRevisions in %s: %w", r.OMENamespace, err)
	}
	out := list.Items[:0]
	for _, rev := range list.Items {
		if rev.Annotations[constants.RuntimeRevisionCreatedByKey] != constants.RuntimeRevisionCreatedByOMEValue {
			continue
		}
		out = append(out, rev)
	}
	return out, nil
}

// referencedRevisionNames collects every revision name currently
// referenced by an ISVC (explicit pin OR status pin). Cluster-wide
// list — ISVCs in any namespace can pin to any revision.
func (r *GCReconciler) referencedRevisionNames(ctx context.Context) (map[string]bool, error) {
	var list v1beta1.InferenceServiceList
	if err := r.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("list InferenceServices: %w", err)
	}
	out := make(map[string]bool, len(list.Items)*2)
	for i := range list.Items {
		isvc := &list.Items[i]
		if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Revision != nil && *isvc.Spec.Runtime.Revision != "" {
			out[*isvc.Spec.Runtime.Revision] = true
		}
		if isvc.Status.PinnedRevisionName != "" {
			out[isvc.Status.PinnedRevisionName] = true
		}
	}
	return out, nil
}

// apply executes one Decision. Idempotent: conflicts are non-fatal
// because the next reconcile will recompute the plan.
func (r *GCReconciler) apply(ctx context.Context, d *Decision) error {
	switch d.Action {
	case Keep:
		if !d.HadAnnotation {
			return nil
		}
		// Clear the gc-eligible-since annotation (revision moved back
		// into the retain set).
		delete(d.Revision.Annotations, constants.RuntimeRevisionGCEligibleSinceKey)
		return r.tolerateConflict(r.Update(ctx, d.Revision))
	case Mark:
		if d.Revision.Annotations == nil {
			d.Revision.Annotations = map[string]string{}
		}
		d.Revision.Annotations[constants.RuntimeRevisionGCEligibleSinceKey] = r.Clock.Now().UTC().Format(time.RFC3339)
		return r.tolerateConflict(r.Update(ctx, d.Revision))
	case Wait:
		return nil
	case Delete:
		return r.tolerateConflict(r.Delete(ctx, d.Revision))
	}
	return nil
}

func (r *GCReconciler) tolerateConflict(err error) error {
	if err == nil || apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// enqueueGC is the singleton enqueuer used by all watch sources.
func (r *GCReconciler) enqueueGC(_ context.Context, _ client.Object) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: gcRequestKey}}
}

func (r *GCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Resync == 0 {
		r.Resync = 5 * time.Minute
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("runtime-revision-gc").
		Watches(&appsv1.ControllerRevision{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueGC),
			builder.WithPredicates(predicate.NewPredicateFuncs(omeOwnedRevision))).
		Watches(&v1beta1.InferenceService{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueGC)).
		Complete(r)
}

// omeOwnedRevision is the watch predicate that filters
// ControllerRevision events to ones the OME controller wrote — keeps
// our event volume scoped to revisions we care about.
func omeOwnedRevision(obj client.Object) bool {
	return obj.GetAnnotations()[constants.RuntimeRevisionCreatedByKey] == constants.RuntimeRevisionCreatedByOMEValue
}
