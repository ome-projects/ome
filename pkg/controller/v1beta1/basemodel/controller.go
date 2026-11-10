package basemodel

import (
	"context"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/backends/pernode"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/backends/pvc"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// +kubebuilder:rbac:groups=ome.io,resources=basemodels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=basemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels/finalizers,verbs=update
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;update;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// BaseModelReconciler reconciles BaseModel objects
type BaseModelReconciler struct {
	client.Client
	Log            logr.Logger
	Scheme         *runtime.Scheme
	nodeReader     client.Reader
	OmeAgentConfig *controllerconfig.OmeAgentConfig
}

// ClusterBaseModelReconciler reconciles ClusterBaseModel objects
type ClusterBaseModelReconciler struct {
	client.Client
	Log            logr.Logger
	Scheme         *runtime.Scheme
	nodeReader     client.Reader
	OmeAgentConfig *controllerconfig.OmeAgentConfig
}

// backends builds the dispatch slice in match order: pvc → pernode.
// perNodeBackend always matches, so it goes last as the fallback.
func (r *BaseModelReconciler) backends() []shared.Backend {
	return []shared.Backend{
		pvc.New(r.OmeAgentConfig),
		perNodeBackend{nodeReader: r.nodeReader},
	}
}

func (r *ClusterBaseModelReconciler) backends() []shared.Backend {
	return []shared.Backend{
		pvc.New(r.OmeAgentConfig),
		perNodeBackend{nodeReader: r.nodeReader},
	}
}

// Reconcile handles BaseModel reconciliation
func (r *BaseModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("basemodel", req.NamespacedName)

	baseModel := &v1beta1.BaseModel{}
	if err := r.Get(ctx, req.NamespacedName, baseModel); err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return without error since it was likely deleted
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get BaseModel")
		return ctrl.Result{}, err
	}
	return reconcileModel(ctx, r.Client, r.Scheme, log, r.backends(), baseModel, constants.BaseModelFinalizer, false, "BaseModel")
}

// Reconcile handles ClusterBaseModel reconciliation
func (r *ClusterBaseModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("clusterbasemodel", req.NamespacedName)

	clusterBaseModel := &v1beta1.ClusterBaseModel{}
	if err := r.Get(ctx, req.NamespacedName, clusterBaseModel); err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return without error since it was likely deleted
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get ClusterBaseModel")
		return ctrl.Result{}, err
	}
	return reconcileModel(ctx, r.Client, r.Scheme, log, r.backends(), clusterBaseModel, constants.ClusterBaseModelFinalizer, true, "ClusterBaseModel")
}

// reconcileModel is the backend-agnostic reconcile core: resolve the
// spec/status, pick the backend, add the finalizer, and dispatch
// deletion/reconcile to that backend.
func reconcileModel(ctx context.Context, c client.Client, scheme *runtime.Scheme, log logr.Logger, backends []shared.Backend, obj client.Object, finalizer string, isClusterScoped bool, kind string) (ctrl.Result, error) {
	spec, status, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Reconciling " + kind)

	backend := pickBackend(backends, spec)
	args := shared.BackendArgs{
		Client:          c,
		Scheme:          scheme,
		Log:             log,
		Obj:             obj,
		Spec:            spec,
		Status:          status,
		Finalizer:       finalizer,
		IsClusterScoped: isClusterScoped,
		Kind:            kind,
	}

	// Handle deletion
	if !obj.GetDeletionTimestamp().IsZero() {
		log.Info("Handling " + kind + " deletion via " + backend.Name() + " backend")
		return backend.HandleDeletion(ctx, args)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(obj, finalizer) {
		log.Info("Adding finalizer to " + kind)
		controllerutil.AddFinalizer(obj, finalizer)
		if err := c.Update(ctx, obj); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	log.V(1).Info("Dispatching "+kind+" reconcile to backend", "backend", backend.Name())
	return backend.Reconcile(ctx, args)
}

// SetupWithManager sets up the BaseModel controller with the Manager
func (r *BaseModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.nodeReader = mgr.GetAPIReader()
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.BaseModel{}).
		Owns(&batchv1.Job{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return pernode.MapConfigMapToModelRequests(obj, r.Log, true) // true = namespaced
			}),
			// OR'd: per-node basemodel-status ConfigMaps (agent flow) +
			// per-PVC pvc-status ConfigMaps (extraction Job output). Both are
			// keyed by GetModelConfigMapKey, so one mapper fits both.
			builder.WithPredicates(predicate.Or(pernode.CreateModelStatusConfigMapPredicate(), createPVCStatusConfigMapPredicate())),
		).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return pernode.HandleNodeDeletion(ctx, r.Client, r.Log, obj)
			}),
			builder.WithPredicates(pernode.CreateNodeDeletionPredicate()),
		).
		Watches(
			&corev1.PersistentVolumeClaim{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return pvc.MapToBaseModels(ctx, r.Client, r.Log, obj)
			}),
			builder.WithPredicates(pvc.CreatePhasePredicate()),
		).
		Complete(r)
}

// SetupWithManager sets up the ClusterBaseModel controller with the Manager
func (r *ClusterBaseModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.nodeReader = mgr.GetAPIReader()
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.ClusterBaseModel{}).
		Owns(&batchv1.Job{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return pernode.MapConfigMapToModelRequests(obj, r.Log, false) // false = cluster-scoped
			}),
			builder.WithPredicates(predicate.Or(pernode.CreateModelStatusConfigMapPredicate(), createPVCStatusConfigMapPredicate())),
		).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return pernode.HandleNodeDeletion(ctx, r.Client, r.Log, obj)
			}),
			builder.WithPredicates(pernode.CreateNodeDeletionPredicate()),
		).
		Watches(
			&corev1.PersistentVolumeClaim{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return pvc.MapToClusterBaseModels(ctx, r.Client, r.Log, obj)
			}),
			builder.WithPredicates(pvc.CreatePhasePredicate()),
		).
		Complete(r)
}

// createPVCStatusConfigMapPredicate fires on ConfigMaps in the OME namespace
// carrying the PVC-status label. Lives here (not in pvc/) because it's OR'd
// with pernode's predicate in the single ConfigMap watch.
func createPVCStatusConfigMapPredicate() predicate.Predicate {
	isPVC := func(obj client.Object) bool {
		if obj.GetNamespace() != constants.OMENamespace {
			return false
		}
		return obj.GetLabels()[constants.PVCStorageConfigMapLabel] == "true"
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return isPVC(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool { return isPVC(e.ObjectNew) },
		DeleteFunc: func(e event.DeleteEvent) bool { return isPVC(e.Object) },
	}
}
