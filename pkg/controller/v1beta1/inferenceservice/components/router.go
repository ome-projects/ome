package components

import (
	"context"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/rbac"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/status"
	"github.com/sgl-project/ome/pkg/utils"
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

// NewRouter creates a new Router component instance
func NewRouter(
	client client.Client,
	clientset kubernetes.Interface,
	scheme *runtime.Scheme,
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig,
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	routerSpec *v1beta1.RouterSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
) Component {
	logger := ctrl.Log.WithName("RouterReconciler")
	statusManager := status.NewStatusReconciler(clientset).WithLogger(logger.WithName("InferenceServiceStatus"))
	base := BaseComponentFields{
		Client:                 client,
		Clientset:              clientset,
		Scheme:                 scheme,
		InferenceServiceConfig: inferenceServiceConfig,
		DeploymentMode:         deploymentMode,
		BaseModel:              baseModel,
		BaseModelMeta:          baseModelMeta,
		Runtime:                runtime,
		RuntimeName:            runtimeName,
		StatusManager:          statusManager,
		Log:                    logger,
	}

	return &Router{
		BaseComponentFields: base,
		routerSpec:          routerSpec,
		deploymentReconciler: &common.DeploymentReconciler{
			Client:        client,
			Clientset:     clientset,
			Scheme:        scheme,
			StatusManager: base.StatusManager,
			Log:           base.Log,
		},
		podSpecReconciler: &common.PodSpecReconciler{
			Log: base.Log,
		},
	}
}

// Reconcile implements the Component interface for Router
func (r *Router) Reconcile(isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	r.Log.Info("Reconciling router component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate router spec
	if r.routerSpec == nil {
		return ctrl.Result{}, errors.New("router spec is nil")
	}

	// Reconcile object metadata
	objectMeta, err := r.reconcileObjectMeta(isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile object metadata")
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

	// Reconcile deployment based on deployment mode
	if result, err := r.reconcileDeployment(isvc, objectMeta, podSpec); err != nil {
		return result, err
	}

	// Update router status
	if err := r.updateRouterStatus(isvc, objectMeta); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileDeployment manages the deployment logic for different deployment modes
func (r *Router) reconcileDeployment(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec) (ctrl.Result, error) {
	switch r.DeploymentMode {
	case constants.RawDeployment:
		return r.deploymentReconciler.ReconcileRawDeployment(isvc, objectMeta, podSpec, &r.routerSpec.ComponentExtensionSpec, v1beta1.RouterComponent)
	case constants.Serverless:
		return r.deploymentReconciler.ReconcileKnativeDeployment(isvc, objectMeta, podSpec, &r.routerSpec.ComponentExtensionSpec, v1beta1.RouterComponent)
	default:
		return ctrl.Result{}, errors.New("invalid deployment mode for router")
	}
}

// updateRouterStatus updates the status of the router component
func (r *Router) updateRouterStatus(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) error {
	return UpdateComponentStatus(&r.BaseComponentFields, isvc, v1beta1.RouterComponent, objectMeta, r.getPodLabelInfo)
}

// getPodLabelInfo returns the pod label key and value based on the deployment mode
func (r *Router) getPodLabelInfo(rawDeployment bool, objectMeta metav1.ObjectMeta, statusSpec v1beta1.ComponentStatusSpec) (string, string) {
	if rawDeployment {
		return constants.RawDeploymentAppLabel, constants.GetRawServiceLabel(objectMeta.Name)
	}
	return constants.RevisionLabel, statusSpec.LatestCreatedRevision
}

// reconcileObjectMeta creates the object metadata for the router component
func (r *Router) reconcileObjectMeta(isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	routerName, err := r.determineRouterName(isvc)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}

	annotations, err := r.processAnnotations(isvc)
	if err != nil {
		return metav1.ObjectMeta{
			Name:      routerName,
			Namespace: isvc.Namespace,
		}, err
	}

	labels, err := r.processLabels(isvc)

	if err != nil {
		return metav1.ObjectMeta{
			Name:        routerName,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
		}, err
	}

	return metav1.ObjectMeta{
		Name:        routerName,
		Namespace:   isvc.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

// processAnnotations processes the annotations for the router
func (r *Router) processAnnotations(isvc *v1beta1.InferenceService) (map[string]string, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	// Merge with router annotations
	mergedAnnotations := annotations
	if r.routerSpec != nil {
		routerAnnotations := r.routerSpec.Annotations
		mergedAnnotations = utils.Union(annotations, routerAnnotations)
	}

	// Use common function for base annotations processing
	processedAnnotations, err := ProcessBaseAnnotations(&r.BaseComponentFields, isvc, mergedAnnotations)
	if err != nil {
		return nil, err
	}

	return processedAnnotations, nil
}

// processLabels processes the labels for the router
func (r *Router) processLabels(isvc *v1beta1.InferenceService) (map[string]string, error) {
	mergedLabels := isvc.Labels
	if r.routerSpec != nil {
		routerLabels := r.routerSpec.Labels
		mergedLabels = utils.Union(isvc.Labels, routerLabels)
	}

	// Use common function for base labels processing
	return ProcessBaseLabels(&r.BaseComponentFields, isvc, v1beta1.RouterComponent, mergedLabels)
}

// determineRouterName determines the name of the router service
func (r *Router) determineRouterName(isvc *v1beta1.InferenceService) (string, error) {
	// For router, we'll use a pattern similar to predictor but with "-router" suffix
	defaultRouterName := isvc.Name + "-router"
	existingName := defaultRouterName

	if r.DeploymentMode == constants.RawDeployment {
		existing := &v1.Service{}
		if err := r.Client.Get(context.TODO(), types.NamespacedName{Name: defaultRouterName, Namespace: isvc.Namespace}, existing); err == nil {
			return existingName, nil
		}
	} else {
		existing := &knservingv1.Service{}
		if err := r.Client.Get(context.TODO(), types.NamespacedName{Name: defaultRouterName, Namespace: isvc.Namespace}, existing); err == nil {
			return existingName, nil
		}
	}

	// If the default name doesn't exist, use it
	return defaultRouterName, nil
}

// reconcilePodSpec creates the pod spec for the router component
func (r *Router) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	if r.routerSpec.Runner != nil {
		if r.routerSpec.Config != nil {
			r.Log.Info("Adding config to router env", "inference service", isvc.Name, "namespace", isvc.Namespace)
			for k, v := range r.routerSpec.Config {
				r.routerSpec.Runner.Env = append(r.routerSpec.Runner.Env, v1.EnvVar{Name: k, Value: v})
			}
		}
	}
	// Use common pod spec reconciler for base logic
	podSpec, err := r.podSpecReconciler.ReconcilePodSpec(isvc, objectMeta, &r.routerSpec.PodSpec, r.routerSpec.Runner)
	if err != nil {
		return nil, err
	}

	UpdatePodSpecVolumes(&r.BaseComponentFields, isvc, podSpec, objectMeta)

	r.Log.Info("Router PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
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
