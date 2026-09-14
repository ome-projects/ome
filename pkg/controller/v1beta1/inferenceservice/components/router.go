package components

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/rbac"
)

var _ Component = &Router{}
var _ ComponentConfig = &Router{}

// Router reconciles resources for the router component
type Router struct {
	BaseComponentFields
	routerSpec           *v1beta1.RouterSpec
	deploymentReconciler *common.DeploymentReconciler
	podSpecReconciler    *common.PodSpecReconciler
	rbacReconciler       *rbac.RBACReconciler
}

// NewRouter creates a new Router component instance. deps carries the
// process-lifetime wiring; in carries the per-reconcile pipeline
// output. Router has no supported-model-format input — callers leave
// in.ModelFormat unset (nil).
func NewRouter(deps *ComponentDeps, in ComponentInputs, routerSpec *v1beta1.RouterSpec) Component {
	base := newBaseComponentFields(deps, in, "RouterReconciler")

	return &Router{
		BaseComponentFields: base,
		routerSpec:          routerSpec,
		deploymentReconciler: &common.DeploymentReconciler{
			Client:        deps.Client,
			APIReader:     deps.APIReader,
			Clientset:     deps.Clientset,
			Scheme:        deps.Scheme,
			StatusManager: base.StatusManager,
			Log:           base.Log,
		},
		podSpecReconciler: &common.PodSpecReconciler{
			Log: base.Log,
		},
	}
}

// Reconcile implements the Component interface for Router
func (r *Router) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	r.Log.V(1).Info("Reconciling router component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate router spec
	if r.routerSpec == nil {
		return ctrl.Result{}, errors.New("router spec is nil")
	}
	// Reconcile object metadata
	objectMeta, err := r.reconcileObjectMeta(ctx, isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile object metadata")
	}
	pdbRequest, err := resolveComponentPDBRequest(
		&r.BaseComponentFields,
		isvc,
		r.DeploymentMode,
		v1beta1.RouterComponent,
		objectMeta,
		&r.routerSpec.ComponentExtensionSpec,
	)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to resolve router PodDisruptionBudget")
	}
	if err := preflightComponentPDB(ctx, &r.BaseComponentFields, pdbRequest); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to preflight router PodDisruptionBudget")
	}

	// Reconcile RBAC resources (ServiceAccount, Role, RoleBinding)
	r.rbacReconciler = rbac.NewRBACReconciler(
		r.Client,
		r.Scheme,
		objectMeta,
		v1beta1.RouterComponent,
		isvc,
	)
	if err := r.rbacReconciler.Reconcile(); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile RBAC resources")
	}

	// Reconcile pod spec
	podSpec, err := r.reconcilePodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile pod spec")
	}

	// Set the service account name in the pod spec
	podSpec.ServiceAccountName = r.rbacReconciler.GetServiceAccountName()

	// Reconcile deployment based on deployment mode. The deployment
	// reconciler's RequeueAfter MUST be preserved through the rest of
	// this function — OMENative's per-Instance dispatcher uses it to
	// re-run after a Sequential / Ratio gate denial OR to poll an
	// in-flight Update / Create. Discarding it strands the rollout
	// until an unrelated watch event happens to fire another reconcile
	// (the post-decoder Sequential stall).
	deploymentResult, err := r.reconcileDeployment(ctx, isvc, objectMeta, podSpec, pdbRequest)
	if err != nil {
		return deploymentResult, err
	}

	// Per-Component stable Service + PodMonitor for OMENative. See the
	// rationale on Engine.reconcileOMENativeSubresources; this method is
	// the router equivalent.
	if r.DeploymentMode == constants.OMENative {
		if err := r.reconcileOMENativeSubresources(ctx, isvc, objectMeta, podSpec); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile router OMENative sub-resources")
		}
	}

	// Update router status
	if err := r.updateRouterStatus(isvc, objectMeta); err != nil {
		return ctrl.Result{}, err
	}

	return deploymentResult, nil
}

// reconcileOMENativeSubresources delegates to the shared
// ReconcileOMENativeSubresources helper in base.go — engine, decoder,
// and router emit byte-identical Service + PodMonitor pairs; only the
// component enum and ComponentExtensionSpec pointer differ.
func (r *Router) reconcileOMENativeSubresources(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec) error {
	return ReconcileOMENativeSubresources(ctx, &r.BaseComponentFields, isvc, v1beta1.RouterComponent, &r.routerSpec.ComponentExtensionSpec, objectMeta, podSpec)
}

// reconcileDeployment manages the deployment logic for different deployment modes
func (r *Router) reconcileDeployment(ctx context.Context, isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec, pdbRequest pdb.Request) (ctrl.Result, error) {
	switch r.DeploymentMode {
	case constants.RawDeployment:
		rawResolved, err := resolveRawComponentAutoscaling(ctx, &r.BaseComponentFields, isvc, v1beta1.RouterComponent, &r.routerSpec.ComponentExtensionSpec, objectMeta.Annotations)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to resolve autoscaler for raw router")
		}
		result, err := r.deploymentReconciler.ReconcileRawDeployment(ctx, isvc, objectMeta, podSpec, &r.routerSpec.ComponentExtensionSpec, v1beta1.RouterComponent, rawResolved, pdbRequest)
		if err != nil {
			return result, err
		}
		// A policy hold recovers via config edits that emit no event, so the
		// hold carries its own periodic requeue. Smaller-nonzero merge: never
		// delay a dispatcher poll already scheduled on this result.
		return mergeRequeueAfter(result, rawResolved.RequeueAfter), nil
	case constants.OMENative:
		// OMENative-mode Components dispatch through the InferenceReplica
		// path. Router is always single-pod (MultiPod=false; no Worker
		// block) so the projector receives a nil WorkerPodSpec.
		//
		// Resolve the authoritative ComponentAutoscaler from the
		// ISVC → policy → runtime → default chain. See engine dispatch for
		// the full design — dispatches HPA / KEDA / external / none against
		// the committed IR (autoscaler dispatch is always-on per Component).
		res, err := resolveComponentAutoscaling(ctx, &r.BaseComponentFields, isvc, v1beta1.RouterComponent, &r.routerSpec.ComponentExtensionSpec)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to resolve autoscaler for router")
		}
		ir, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
			ISVC:                      isvc,
			Component:                 v1beta1.RouterComponent,
			ComponentExt:              &r.routerSpec.ComponentExtensionSpec,
			ObjectMeta:                objectMeta,
			PodSpec:                   podSpec,
			MultiPod:                  false,
			ResolvedAutoscaler:        res.Resolved,
			PreserveAutoscaler:        res.Hold,
			QuotaAcceleratorResources: r.QuotaAcceleratorResources,
			Client:                    r.Client,
			Reader:                    r.APIReader,
		})
		if err != nil {
			if apierrors.IsConflict(err) {
				// Benign: a concurrent IR status write bumped the object.
				// Re-read and reproject next pass instead of surfacing a hard
				// error, which would fast-loop the ISVC reconcile.
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, errors.Wrap(err, "failed to project InferenceReplica for router")
		}
		if err := ReconcileOMENativePDB(
			ctx,
			&r.BaseComponentFields,
			v1beta1.RouterComponent,
			ir,
			pdbRequest,
		); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile router OMENative PodDisruptionBudget")
		}
		// Autoscaler dispatch is always-on per InferenceReplica.
		// See engine dispatch for the full owner-ref + scaleTargetRef
		// rationale.
		if err := dispatchIRAutoscaler(ctx, &r.BaseComponentFields, isvc, ir, &r.routerSpec.ComponentExtensionSpec, res); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to dispatch autoscaler for router")
		}
		// A policy hold recovers via config edits that emit no event; carry
		// its periodic requeue on the result (smaller-nonzero merge).
		return mergeRequeueAfter(ctrl.Result{}, res.RequeueAfter), nil
	default:
		return ctrl.Result{}, errors.New("invalid deployment mode for router")
	}
}

// updateRouterStatus updates the status of the router component
func (r *Router) updateRouterStatus(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) error {
	return UpdateComponentStatus(&r.BaseComponentFields, isvc, v1beta1.RouterComponent, objectMeta, &r.routerSpec.ComponentExtensionSpec)
}

// reconcileObjectMeta creates the object metadata for the router
// component. Delegates the annotation / label merge to the shared
// ReconcileComponentObjectMeta helper in base.go.
func (r *Router) reconcileObjectMeta(ctx context.Context, isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	routerName, err := r.determineRouterName(ctx, isvc)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}

	var routerAnnotations, routerLabels map[string]string
	if r.routerSpec != nil {
		routerAnnotations = r.routerSpec.Annotations
		routerLabels = r.routerSpec.Labels
	}

	return ReconcileComponentObjectMeta(&r.BaseComponentFields, isvc, v1beta1.RouterComponent, routerName, routerAnnotations, routerLabels)
}

// determineRouterName determines the name of the router service.
// The suffix is sourced from GetServiceSuffix so the engine / decoder /
// router suffix lives in exactly one place (ComponentConfig).
func (r *Router) determineRouterName(ctx context.Context, isvc *v1beta1.InferenceService) (string, error) {
	defaultRouterName := isvc.Name + r.GetServiceSuffix()

	existing := &v1.Service{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: defaultRouterName, Namespace: isvc.Namespace}, existing); err == nil {
		return defaultRouterName, nil
	}

	return defaultRouterName, nil
}

// reconcilePodSpec creates the pod spec for the router component
func (r *Router) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	if r.routerSpec.Runner != nil {
		if r.routerSpec.Config != nil {
			r.Log.V(2).Info("Adding config to router env", "inference service", isvc.Name, "namespace", isvc.Namespace)
			r.routerSpec.Runner.Env = append(r.routerSpec.Runner.Env, configEnvVars(r.routerSpec.Config)...)
		}
	}
	// Use common pod spec reconciler for base logic
	podSpec, err := r.podSpecReconciler.ReconcilePodSpec(isvc, objectMeta, &r.routerSpec.PodSpec, r.routerSpec.Runner)
	if err != nil {
		return nil, err
	}

	UpdatePodSpecVolumes(&r.BaseComponentFields, isvc, podSpec, objectMeta)

	r.Log.V(1).Info("Router PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// configEnvVars converts a component config map to env vars in sorted
// key order — map iteration order would churn the pod template hash
// across reconciles and trigger spurious rollouts.
func configEnvVars(config map[string]string) []v1.EnvVar {
	keys := make([]string, 0, len(config))
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	envs := make([]v1.EnvVar, 0, len(keys))
	for _, k := range keys {
		envs = append(envs, v1.EnvVar{Name: k, Value: config[k]})
	}
	return envs
}

// GetComponentType implements ComponentConfig interface
func (r *Router) GetComponentType() v1beta1.ComponentType {
	return v1beta1.RouterComponent
}

// GetComponentSpec implements ComponentConfig interface
func (r *Router) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if r.routerSpec == nil {
		return nil
	}
	return &r.routerSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (r *Router) GetServiceSuffix() string {
	return "-router"
}

// ValidateSpec implements ComponentConfig interface
func (r *Router) ValidateSpec() error {
	if r.routerSpec == nil {
		return errors.New("router spec is nil")
	}
	// Add more validation logic as needed
	return nil
}
