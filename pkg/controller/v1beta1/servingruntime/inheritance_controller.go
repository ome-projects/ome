package servingruntime

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
)

// +kubebuilder:rbac:groups=ome.io,resources=clusterservingruntimes,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=clusterservingruntimes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=servingruntimes,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=servingruntimes/status,verbs=get;update;patch

// InheritanceReconciler resolves runtime inheritance for both
// cluster- and namespace-scoped runtimes. Reconcile branches on
// request namespace: empty → ClusterServingRuntime, set →
// ServingRuntime in that namespace.
type InheritanceReconciler struct {
	client.Client
	Log      logr.Logger
	Recorder record.EventRecorder
}

func (r *InheritanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.Namespace == "" {
		csr := &v1beta1.ClusterServingRuntime{}
		if err := r.Get(ctx, req.NamespacedName, csr); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		return r.reconcileRuntime(ctx, csr, &csr.Status,
			runtimeinheritance.ClusterRuntimeRef(csr), runtimeinheritance.ClusterRuntimeFetcher(r.Client),
			"clusterservingruntime", req.Name)
	}
	sr := &v1beta1.ServingRuntime{}
	if err := r.Get(ctx, req.NamespacedName, sr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return r.reconcileRuntime(ctx, sr, &sr.Status,
		runtimeinheritance.NamespacedRuntimeRef(sr), runtimeinheritance.NamespacedRuntimeFetcher(r.Client, req.Namespace),
		"servingruntime", req.Name, "namespace", req.Namespace)
}

// reconcileRuntime resolves the inheritance chain of one fetched runtime and
// records the outcome in its status. The kind decides only where the resolver
// starts, how it reads parents and how the runtime is named in the log.
func (r *InheritanceReconciler) reconcileRuntime(ctx context.Context, obj client.Object, status *v1beta1.ServingRuntimeStatus,
	start *runtimeinheritance.RuntimeRef, parents runtimeinheritance.Fetcher, logKeys ...any) (ctrl.Result, error) {
	_, chain, err := runtimeinheritance.Resolve(ctx, start, parents, constants.RuntimeInheritMaxDepth)
	if err != nil {
		r.Log.WithValues(logKeys...).Info("inheritance resolution failed",
			"reason", classifyResolveError(err), "error", err.Error())
		r.Recorder.Eventf(obj, "Warning", classifyResolveError(err), "%v", err)
	}

	newStatus := projectInheritanceResult(*status, obj.GetGeneration(), chain, err)
	if equality.Semantic.DeepEqual(*status, newStatus) {
		return ctrl.Result{}, nil
	}
	*status = newStatus
	if updateErr := r.Status().Update(ctx, obj); updateErr != nil {
		if apierrors.IsConflict(updateErr) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{}, nil
}

// dependentsOfCluster enqueues self + every CSR/SR below the changed CSR
// in the inheritance graph. Drives cluster→cluster and cluster→namespaced
// cascade on parent profile updates.
func (r *InheritanceReconciler) dependentsOfCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.selfAndDependents(ctx, types.NamespacedName{Name: obj.GetName()})
}

// dependentsOfNamespaced enqueues self + the same-namespace SRs below the
// changed SR. Drives namespaced→namespaced cascade. SRs cannot be
// inherited from outside their namespace.
func (r *InheritanceReconciler) dependentsOfNamespaced(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.selfAndDependents(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()})
}

// selfAndDependents enqueues root plus its whole subtree. The walk is
// transitive because a chain edit reaches past direct children: repointing
// a runtime at a different parent rewrites the resolved chain of everything
// below it, and only the runtime's own reconcile can record that.
func (r *InheritanceReconciler) selfAndDependents(ctx context.Context, root types.NamespacedName) []reconcile.Request {
	reqs := []reconcile.Request{{NamespacedName: root}}

	dependents, err := runtimeinheritance.Descendants(ctx, r.Client, root)
	if err != nil {
		r.Log.Error(err, "inheritance cascade: resolve dependents failed",
			"namespace", root.Namespace, "name", root.Name)
		return reqs
	}
	for _, dependent := range dependents {
		reqs = append(reqs, reconcile.Request{NamespacedName: dependent})
	}
	return reqs
}

// SetupWithManager registers a single controller watching both CSR and
// SR via Watches() with fan-out map functions. A cluster-scoped parent
// update cascades into all cluster- and namespace-scoped dependents;
// a namespace-scoped parent update cascades into same-namespace
// dependents only. Reconcile branches on req.Namespace.
func (r *InheritanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("servingruntime-inheritance").
		For(&v1beta1.ClusterServingRuntime{},
			builder.WithPredicates(inheritanceTriggerPredicate())).
		Watches(&v1beta1.ClusterServingRuntime{},
			handler.EnqueueRequestsFromMapFunc(r.dependentsOfCluster),
			builder.WithPredicates(inheritanceTriggerPredicate())).
		Watches(&v1beta1.ServingRuntime{},
			handler.EnqueueRequestsFromMapFunc(r.dependentsOfNamespaced),
			builder.WithPredicates(inheritanceTriggerPredicate())).
		Complete(r)
}
