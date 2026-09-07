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

// perNodeBackend wraps pernode.Reconcile with a pre-step that scrubs
// PVC artifacts left over when a model's URI flips away from pvc://.
// Lives here (not in pernode/) so that cross-backend cleanup doesn't
// drag a pernode→pvc import.
type perNodeBackend struct {
	nodeReader client.Reader
}

func (perNodeBackend) Name() string                          { return "pernode" }
func (perNodeBackend) Matches(_ *v1beta1.BaseModelSpec) bool { return true }

func (b perNodeBackend) Reconcile(ctx context.Context, a shared.BackendArgs) (ctrl.Result, error) {
	if err := pvc.CleanupStaleArtifacts(ctx, a.Client, a.Log, a.Obj, a.IsClusterScoped); err != nil {
		a.Log.Error(err, "Failed to clean up stale PVC artifacts after URI swap")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	if err := pernode.ReconcileStatusFromConfigMaps(ctx, a.Client, b.nodeReader, a.Log, a.Obj, a.IsClusterScoped, a.Kind); err != nil {
		a.Log.Error(err, "Failed to update "+a.Kind+" status")
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	if a.Status.State == v1beta1.LifeCycleStateImporting || a.Status.State == v1beta1.LifeCycleStateInTransit {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (b perNodeBackend) HandleDeletion(ctx context.Context, a shared.BackendArgs) (ctrl.Result, error) {
	return pernode.HandleModelDeletion(ctx, a.Client, b.nodeReader, a.Obj, a.Finalizer)
}
