package basemodel

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/backends/pernode"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/backends/pvc"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// pickBackend returns the first backend whose Matches(spec) is true.
// Panics if none match — perNodeBackend is the fallback and must be
// registered last.
func pickBackend(backends []shared.Backend, spec *v1beta1.BaseModelSpec) shared.Backend {
	for _, b := range backends {
		if b.Matches(spec) {
			return b
		}
	}
	panic("basemodel: no default backend registered (expected per-node fallback last)")
}

// perNodeBackend is the observational default: it reflects the per-node
// model-status ConfigMaps written by the model-agent DaemonSet into the
// model's status. It always matches and is the registry fallback.
type perNodeBackend struct{ nodeReader client.Reader }

func (perNodeBackend) Name() string                          { return "pernode" }
func (perNodeBackend) Matches(_ *v1beta1.BaseModelSpec) bool { return true }

func (b perNodeBackend) Reconcile(ctx context.Context, a shared.BackendArgs) (ctrl.Result, error) {
	// A model whose storage URI flipped away from pvc:// lands on the
	// per-node backend; scrub any leftover PVC Job / ConfigMap / conditions
	// before running the per-node flow. Idempotent no-op when there are no
	// PVC artifacts (the common case).
	if err := pvc.CleanupStaleArtifacts(ctx, a.Client, a.Log, a.Obj, a.IsClusterScoped); err != nil {
		a.Log.Error(err, "Failed to clean up stale PVC artifacts after URI swap")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	if err := pernode.ReconcileStatusFromConfigMaps(ctx, a.Client, b.nodeReader, a.Log, a.Obj, a.IsClusterScoped, a.Kind); err != nil {
		a.Log.Error(err, "Failed to update "+a.Kind+" status")
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	// Requeue while downloading to ensure status is updated regularly
	if a.Status.State == v1beta1.LifeCycleStateImporting || a.Status.State == v1beta1.LifeCycleStateInTransit {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (perNodeBackend) HandleDeletion(ctx context.Context, a shared.BackendArgs) (ctrl.Result, error) {
	return pernode.HandleModelDeletion(ctx, a.Client, a.Obj, a.Finalizer)
}
