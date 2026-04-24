package inferenceservice

import (
	"context"
	"fmt"

	"github.com/sgl-project/ome/pkg/acceleratorclassselector"

	policyv1 "k8s.io/api/policy/v1"

	"github.com/go-logr/logr"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/pkg/errors"
	ray "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	istioclientv1beta1 "istio.io/client-go/pkg/apis/networking/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	v1beta1 "github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/components"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/external_service"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress"
	multimodelconfig "github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/modelconfig"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/sgl-project/ome/pkg/runtimeselector"
	"github.com/sgl-project/ome/pkg/utils"
)

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices;inferenceservices/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=servingruntimes;servingruntimes/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=servingruntimes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=clusterservingruntimes;clusterservingruntimes/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=clusterservingruntimes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels;basemodels/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=finetunedweights/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=finetunedweights;finetunedweights/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels;basemodels/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networking.istio.io,resources=virtualservices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=virtualservices/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=virtualservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networking.istio.io,resources=sidecars,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/log,verbs=get
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// InferenceServiceState describes the Readiness of the InferenceService
type InferenceServiceState string

// Different InferenceServiceState an InferenceService may have.
const (
	InferenceServiceReadyState    InferenceServiceState = "InferenceServiceReady"
	InferenceServiceNotReadyState InferenceServiceState = "InferenceServiceNotReady"
)

// InferenceServiceReconciler reconciles an InferenceService object
type InferenceServiceReconciler struct {
	client.Client
	ClientConfig             *rest.Config
	Clientset                kubernetes.Interface
	Log                      logr.Logger
	Scheme                   *runtime.Scheme
	Recorder                 record.EventRecorder
	StatusManager            *status.StatusReconciler
	RuntimeSelector          runtimeselector.Selector
	AcceleratorClassSelector acceleratorclassselector.Selector
}

func (r *InferenceServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Fetch the InferenceService instance
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, req.NamespacedName, isvc); err != nil {
		if apierrors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	// get annotations from isvc
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	deployConfig, err := controllerconfig.NewDeployConfig(r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create DeployConfig")
	}

	// For backward compatibility with predictor-based architecture
	deploymentMode := isvcutils.GetDeploymentMode(annotations, deployConfig)
	r.Log.Info("Inference service deployment mode ", "namespace", isvc.Namespace, "inference service", isvc.Name, "deployment mode", deploymentMode)

	// name of our custom finalizer
	finalizerName := "inferenceservice.finalizers"

	// examine DeletionTimestamp to determine if object is under deletion
	if isvc.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then lets add the finalizer and update the object. This is equivalent
		// registering our finalizer.
		if !controllerutil.ContainsFinalizer(isvc, finalizerName) {
			controllerutil.AddFinalizer(isvc, finalizerName)
			if err := r.Update(context.Background(), isvc); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		// The object is being deleted
		if controllerutil.ContainsFinalizer(isvc, finalizerName) {
			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(isvc, finalizerName)
			if err := r.Update(context.Background(), isvc); err != nil {
				return ctrl.Result{}, err
			}
		}

		// Stop reconciliation as the item is being deleted
		return ctrl.Result{}, nil
	}

	// Handle VirtualDeployment without actual reconciliation
	if deploymentMode == constants.VirtualDeployment {
		return r.handleVirtualDeployment(isvc)
	}

	// Abort early if the resolved deployment mode is Serverless, but Knative Services are not available
	if deploymentMode == constants.Serverless {
		if result, err := r.handleServerlessPrerequisites(isvc); err != nil {
			return result, err
		}
	}

	// Initialize status if not already initialized
	if isvc.Status.Components == nil {
		isvc.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
	}

	// Setup reconcilers
	r.Log.Info("Reconciling inference service", "apiVersion", isvc.APIVersion, "namespace", isvc.Namespace, "isvc", isvc.Name)
	isvcConfig, err := controllerconfig.NewInferenceServicesConfig(r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create InferenceServicesConfig")
	}

	modelConfigReconciler := multimodelconfig.NewModelConfigReconciler(r.Client, r.Clientset, r.Scheme)
	result, err := modelConfigReconciler.Reconcile(ctx, isvc) // Added ctx
	if err != nil {
		return result, err
	}

	// Initialize ComponentBuilderFactory
	// Note: isvcConfig is created a few lines above inside the Reconcile function
	// for NewInferenceServicesConfig. We will use that existing isvcConfig.
	componentBuilderFactory := components.NewComponentBuilderFactory(r.Client, r.Clientset, r.Scheme, isvcConfig)

	// Determine which components to reconcile based on the spec
	var reconcilers []components.Component

	// Migrate predictor spec to new architecture if needed
	if err := r.migratePredictorToNewArchitecture(isvc); err != nil {
		r.Log.Error(err, "Failed to migrate predictor spec", "namespace", isvc.Namespace, "inferenceService", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "PredictorMigrationError", err.Error())
		return reconcile.Result{}, err
	}

	var ingressDeploymentMode constants.DeploymentModeType

	// Step 1: Reconcile model first
	baseModel, baseModelMeta, err := isvcutils.ReconcileBaseModel(r.Client, isvc)
	if err != nil {
		r.Log.Error(err, "Failed to reconcile base model", "Name", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "ModelReconcileError", err.Error())
		return reconcile.Result{}, err
	}

	// Step 2: Get runtime spec (either specified or auto-selected based on model)
	var rt *v1beta1.ServingRuntimeSpec
	var rtName string
	userSpecifiedRuntime := false

	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		// Validate specified runtime
		rtName = isvc.Spec.Runtime.Name
		userSpecifiedRuntime = true
		if err := r.RuntimeSelector.ValidateRuntime(ctx, rtName, baseModel, isvc); err != nil {
			r.Log.Error(err, "Runtime validation failed", "runtime", rtName, "model", isvc.Spec.Model.Name)
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeValidationError",
				"Runtime %s does not support model %s: %v", rtName, isvc.Spec.Model.Name, err)
			return reconcile.Result{}, err
		}

		// Get the runtime spec using selector
		rtSpec, _, err := r.RuntimeSelector.GetRuntime(ctx, rtName, isvc.Namespace)
		if err != nil {
			r.Log.Error(err, "Failed to get runtime spec", "runtime", rtName)
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeFetchError", err.Error())
			return reconcile.Result{}, err
		}
		rt = rtSpec
	} else {
		// Auto-select runtime
		selection, err := r.RuntimeSelector.SelectRuntime(ctx, baseModel, isvc)
		if err != nil {
			r.Log.Error(err, "Failed to auto-select runtime", "model", isvc.Spec.Model.Name)
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeSelectionError",
				"Failed to find runtime for model %s: %v", isvc.Spec.Model.Name, err)
			return reconcile.Result{}, err
		}
		rt = selection.Spec
		rtName = selection.Name
		r.Log.Info("Auto-selected runtime", "runtime", rtName, "model", isvc.Spec.Model.Name)
	}

	// Step 3: Merge rt and isvc specs to get final engine, decoder, and router specs
	mergedEngine, mergedDecoder, mergedRouter, err := isvcutils.MergeRuntimeSpecs(isvc, rt, r.Log)
	if err != nil {
		r.Log.Error(err, "Failed to merge specs", "Name", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "MergeSpecsError", err.Error())
		return reconcile.Result{}, err
	}

	// Step 4: Determine deployment modes based on merged specs
	engineDeploymentMode, decoderDeploymentMode, routerDeploymentMode, err := isvcutils.DetermineDeploymentModes(mergedEngine, mergedDecoder, mergedRouter, rt)
	if err != nil {
		r.Log.Error(err, "Failed to determine deployment modes", "Name", isvc.Name)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "DeploymentModeError", err.Error())
		return reconcile.Result{}, err
	}

	// If both engine and decoder exist, it's PD-disaggregated
	if mergedEngine != nil && mergedDecoder != nil {
		r.Log.Info("PD-disaggregated deployment detected", "namespace", isvc.Namespace, "inferenceService", isvc.Name)
	}

	// Step 5: Create reconcilers based on merged specs
	if mergedEngine != nil {
		engineACObj, engineAcName, err := r.AcceleratorClassSelector.GetAcceleratorClass(ctx, isvc, rt, v1beta1.EngineComponent)
		if err != nil {
			r.Log.Error(err, "Failed to get accelerator class for engine component", "Name", isvc.Name)
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "AcceleratorClassError", "Failed to get accelerator class for engine: %v", err)
			return reconcile.Result{}, err
		}
		var engineAC *v1beta1.AcceleratorClassSpec
		if engineACObj == nil {
			r.Log.Info("Accelerator class not specified for engine component", "inferenceService", isvc.Name)
		} else {
			engineAC = &engineACObj.Spec
		}
		engineSupportedModelFormats := r.RuntimeSelector.GetSupportedModelFormat(ctx, rt, baseModel, userSpecifiedRuntime)
		r.Log.Info("Creating engine reconciler",
			"deploymentMode", engineDeploymentMode,
			"namespace", isvc.Namespace,
			"inferenceService", isvc.Name,
			"acceleratorClass", engineAcName)

		engineReconciler := componentBuilderFactory.CreateEngineComponent(
			engineDeploymentMode,
			baseModel,
			baseModelMeta,
			mergedEngine,
			rt,
			rtName,
			engineSupportedModelFormats,
			engineAC,
			engineAcName,
		)
		reconcilers = append(reconcilers, engineReconciler)
	}

	if mergedDecoder != nil {
		decoderACObj, decoderAcName, err := r.AcceleratorClassSelector.GetAcceleratorClass(ctx, isvc, rt, v1beta1.DecoderComponent)
		if err != nil {
			r.Log.Error(err, "Failed to get accelerator class for decoder component", "Name", isvc.Name)
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "AcceleratorClassError", "Failed to get accelerator class for decoder: %v", err)
			return reconcile.Result{}, err
		}
		var decoderAC *v1beta1.AcceleratorClassSpec
		if decoderACObj == nil {
			r.Log.Info("Accelerator class not specified for decoder component", "inferenceService", isvc.Name)
		} else {
			decoderAC = &decoderACObj.Spec
		}
		decoderSupportedModelFormats := r.RuntimeSelector.GetSupportedModelFormat(ctx, rt, baseModel, userSpecifiedRuntime)
		r.Log.Info("Creating decoder reconciler",
			"deploymentMode", decoderDeploymentMode,
			"namespace", isvc.Namespace,
			"inferenceService", isvc.Name,
			"acceleratorClass", decoderAcName)

		decoderReconciler := componentBuilderFactory.CreateDecoderComponent(
			decoderDeploymentMode,
			baseModel,
			baseModelMeta,
			mergedDecoder,
			rt,
			rtName,
			decoderSupportedModelFormats,
			decoderAC,
			decoderAcName,
		)
		reconcilers = append(reconcilers, decoderReconciler)
	}

	// Add Router reconciler if merged router spec exists (using new v2 Router)
	if mergedRouter != nil {
		r.Log.Info("Creating router reconciler",
			"deploymentMode", routerDeploymentMode, // Using the determined router deployment mode
			"namespace", isvc.Namespace,
			"inferenceService", isvc.Name)

		routerReconciler := componentBuilderFactory.CreateRouterComponent(
			routerDeploymentMode, // Using the determined router deployment mode
			baseModel,
			baseModelMeta,
			mergedRouter, // Using the merged router spec instead of isvc.Spec.Router
			rt,
			rtName,
		)
		reconcilers = append(reconcilers, routerReconciler)
	}

	// Determine the correct ingress deployment mode using the same logic as ingress reconciler
	// but with the already-determined deployment modes to avoid inconsistency
	if mergedRouter != nil {
		ingressDeploymentMode = routerDeploymentMode
	} else if mergedDecoder != nil {
		ingressDeploymentMode = decoderDeploymentMode
	} else {
		ingressDeploymentMode = engineDeploymentMode
	}

	r.Log.Info("Determined ingress deployment mode",
		"ingressDeploymentMode", ingressDeploymentMode,
		"namespace", isvc.Namespace,
		"inferenceService", isvc.Name)

	// Step 6: Run all reconcilers
	for _, reconciler := range reconcilers {
		result, err := reconciler.Reconcile(isvc)
		if err != nil {
			r.Log.Error(err, "Failed to reconcile component",
				"component", fmt.Sprintf("%T", reconciler),
				"namespace", isvc.Namespace,
				"inferenceService", isvc.Name)
			return result, err
		}
		if result.Requeue || result.RequeueAfter > 0 {
			return result, nil
		}
	}

	// Now reconcile ingress and external service after components have created their services
	ingressConfig, err := controllerconfig.NewIngressConfig(r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create IngressConfig")
	}

	// Resolve ingress config with annotation overrides
	resolvedIngressConfig := isvcutils.ResolveIngressConfig(ingressConfig, isvc.Annotations)

	// New architecture: ingress uses the determined ingress deployment mode
	ingressReconciler := ingress.NewIngressReconciler(r.Client, r.Clientset, r.Scheme, resolvedIngressConfig, isvcConfig)
	r.Log.Info("Reconciling ingress for inference service", "isvc", isvc.Name)
	if err := ingressReconciler.(*ingress.IngressReconciler).ReconcileWithDeploymentMode(ctx, isvc, ingressDeploymentMode); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile ingress")
	}

	// Reconcile external service - creates a service with the inference service name
	// when ingress is disabled to provide a stable endpoint
	externalServiceReconciler := external_service.NewExternalServiceReconciler(r.Client, r.Clientset, r.Scheme, resolvedIngressConfig)
	r.Log.Info("Reconciling external service for inference service", "isvc", isvc.Name)
	if err := externalServiceReconciler.Reconcile(ctx, isvc); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile external service")
	}

	// Set Status.Address for external service and add ingress disable annotation when ingress is disabled
	if resolvedIngressConfig.DisableIngressCreation {
		// Add annotation to InferenceService so cleanup logic knows to keep the external service
		if err := r.ensureIngressDisableAnnotation(isvc); err != nil {
			return reconcile.Result{}, errors.Wrapf(err, "fails to add ingress disable annotation")
		}
		if err := r.setExternalServiceURL(ctx, isvc, resolvedIngressConfig); err != nil {
			r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
			return reconcile.Result{}, errors.Wrapf(err, "fails to set external service URL")
		}
	}

	// Propagate status for all components
	componentList := []v1beta1.ComponentType{v1beta1.EngineComponent}
	if deploymentMode != constants.Serverless {
		// For other modes (RawDeployment, etc.), we check all defined components.
		if mergedDecoder != nil {
			componentList = append(componentList, v1beta1.DecoderComponent)
		}
		if mergedRouter != nil {
			componentList = append(componentList, v1beta1.RouterComponent)
		}
	}

	// Clean up old predictor deployment after new components are ready (migration-specific)
	// This is a temporary migration-specific cleanup function.
	deploymentReady, err := r.cleanupOldPredictorDeployment(ctx, isvc)
	if err != nil {
		r.Log.Error(err, "Failed to cleanup old predictor deployment", "namespace", isvc.Namespace, "inferenceService", isvc.Name)
		return ctrl.Result{Requeue: true}, nil
	} else if !deploymentReady {
		// Requeue until old predictor deployment is fully cleaned up
		return ctrl.Result{Requeue: true}, nil
	} else {
		// Clean up resources for components that no longer exist
		// Move it under else condition to avoid old predictor deployment exist but hpa is cleaned up
		// After migration, it will refactor.
		if err := r.cleanupRemovedComponents(ctx, isvc, mergedEngine, mergedDecoder, mergedRouter); err != nil {
			r.Log.Error(err, "Failed to cleanup removed components", "namespace", isvc.Namespace, "inferenceService", isvc.Name)
			// Don't fail reconciliation on cleanup errors
		}
	}

	// Clean up status for components that no longer exist
	if isvc.Status.Components != nil {
		r.Log.Info("Cleaning up component status",
			"namespace", isvc.Namespace,
			"inferenceService", isvc.Name,
			"mergedEngine", mergedEngine != nil,
			"mergedDecoder", mergedDecoder != nil,
			"mergedRouter", mergedRouter != nil,
			"statusComponents", len(isvc.Status.Components))

		if mergedEngine == nil {
			delete(isvc.Status.Components, v1beta1.EngineComponent)
			r.Log.Info("Deleted engine from status", "namespace", isvc.Namespace, "inferenceService", isvc.Name)
		}
		if mergedDecoder == nil {
			delete(isvc.Status.Components, v1beta1.DecoderComponent)
			r.Log.Info("Deleted decoder from status", "namespace", isvc.Namespace, "inferenceService", isvc.Name)
		}
		if mergedRouter == nil {
			delete(isvc.Status.Components, v1beta1.RouterComponent)
			r.Log.Info("Deleted router from status", "namespace", isvc.Namespace, "inferenceService", isvc.Name)
		}
	}

	if deploymentMode == constants.Serverless {
		r.StatusManager.PropagateCrossComponentStatus(&isvc.Status, componentList, v1beta1.RoutesReady)
		r.StatusManager.PropagateCrossComponentStatus(&isvc.Status, componentList, v1beta1.LatestDeploymentReady)
	}

	if err = r.updateStatus(isvc, deploymentMode); err != nil {
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *InferenceServiceReconciler) handleVirtualDeployment(isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	// We directly set URL and inference service status to Ready in VirtualDeployment mode

	// Set URL across all Status components
	host := network.GetServiceHostname(isvc.Name, isvc.Namespace)
	openAIURL := knapis.HTTP(host)
	addressURL := &duckv1.Addressable{
		URL: &knapis.URL{
			Host:   host,
			Scheme: "http",
		},
	}
	isvc.Status.URL = openAIURL
	isvc.Status.Address = addressURL
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.PredictorComponent: {
			URL: openAIURL,
		},
	}

	isvc.Status.SetConditions(knapis.Conditions{{
		Type:               knapis.ConditionReady,
		Status:             v1.ConditionTrue,
		LastTransitionTime: knapis.VolatileTime{Inner: metav1.Now()},
		Reason:             "VirtualDeployment",
		Message:            "InferenceService is in VirtualDeployment mode",
	}})

	if err := r.updateStatus(isvc, constants.VirtualDeployment); err != nil {
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *InferenceServiceReconciler) handleServerlessPrerequisites(isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	// Abort early if the resolved deployment mode is Serverless, but Knative Services are not available
	ksvcAvailable, err := utils.IsCrdAvailable(r.ClientConfig, knservingv1.SchemeGroupVersion.String(), constants.KnativeServiceKind)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !ksvcAvailable {
		r.Recorder.Event(isvc, v1.EventTypeWarning, "ServerlessModeRejected",
			"It is not possible to use Serverless deployment mode when Knative Services are not available")
		return ctrl.Result{Requeue: false},
			reconcile.TerminalError(fmt.Errorf("the resolved deployment mode of InferenceService '%s' is Serverless, but Knative Serving is not available", isvc.Name))
	}

	return ctrl.Result{}, nil
}

func (r *InferenceServiceReconciler) updateStatus(desiredService *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType) error {
	existingService := &v1beta1.InferenceService{}
	namespacedName := types.NamespacedName{Name: desiredService.Name, Namespace: desiredService.Namespace}
	if err := r.Get(context.TODO(), namespacedName, existingService); err != nil {
		return err
	}
	wasReady := inferenceServiceReadiness(existingService.Status)
	if inferenceServiceStatusEqual(existingService.Status, desiredService.Status) {
		// If we didn't change anything then don't call updateStatus.
		// This is important because the copy we loaded from the informer's
		// cache may be stale, and we don't want to overwrite a prior update
		// to status with this stale state.
	} else if err := r.Status().Update(context.TODO(), desiredService); err != nil {
		r.Log.Error(err, "Failed to update InferenceService status", "InferenceService", desiredService.Name)
		r.Recorder.Eventf(desiredService, v1.EventTypeWarning, "UpdateFailed",
			"Failed to update status for InferenceService %q: %v", desiredService.Name, err)
		return errors.Wrapf(err, "fails to update InferenceService status")
	} else {
		// If there was a difference and there was no error.
		isReady := inferenceServiceReadiness(desiredService.Status)
		if wasReady && !isReady { // Moved to NotReady State
			r.Recorder.Eventf(desiredService, v1.EventTypeWarning, string(InferenceServiceNotReadyState),
				fmt.Sprintf("InferenceService [%v] is no longer Ready", desiredService.GetName()))
		} else if !wasReady && isReady { // Moved to Ready State
			r.Recorder.Eventf(desiredService, v1.EventTypeNormal, string(InferenceServiceReadyState),
				fmt.Sprintf("InferenceService [%v] is Ready", desiredService.GetName()))
		}
	}
	return nil
}

func inferenceServiceReadiness(status v1beta1.InferenceServiceStatus) bool {
	return status.Conditions != nil &&
		status.GetCondition(knapis.ConditionReady) != nil &&
		status.GetCondition(knapis.ConditionReady).Status == v1.ConditionTrue
}

func inferenceServiceStatusEqual(s1, s2 v1beta1.InferenceServiceStatus) bool {
	return equality.Semantic.DeepEqual(s1, s2)
}

// ensureIngressDisableAnnotation adds the ome.io/ingress-disable-creation annotation to the InferenceService
// This annotation is used by the cleanup logic to determine if external service should be kept
func (r *InferenceServiceReconciler) ensureIngressDisableAnnotation(isvc *v1beta1.InferenceService) error {
	const ingressDisableAnnotation = "ome.io/ingress-disable-creation"

	// Check if annotation already exists
	if val, ok := isvc.Annotations[ingressDisableAnnotation]; ok && val == "true" {
		return nil
	}

	// Add annotation
	if isvc.Annotations == nil {
		isvc.Annotations = make(map[string]string)
	}
	isvc.Annotations[ingressDisableAnnotation] = "true"

	return nil
}

func (r *InferenceServiceReconciler) SetupWithManager(mgr ctrl.Manager, deployConfig *controllerconfig.DeployConfig, ingressConfig *controllerconfig.IngressConfig) error {
	r.ClientConfig = mgr.GetConfig()

	// NEW: Initialize StatusReconciler
	r.StatusManager = status.NewStatusReconciler(r.Clientset)

	// Initialize RuntimeSelector
	r.RuntimeSelector = runtimeselector.New(mgr.GetClient())

	// Initialize AcceleratorClassSelector
	r.AcceleratorClassSelector = acceleratorclassselector.New(mgr.GetClient())

	ksvcFound, err := utils.IsCrdAvailable(r.ClientConfig, knservingv1.SchemeGroupVersion.String(), constants.KnativeServiceKind)
	if err != nil {
		return err
	}

	vsFound, err := utils.IsCrdAvailable(r.ClientConfig, istioclientv1beta1.SchemeGroupVersion.String(), constants.IstioVirtualServiceKind)
	if err != nil {
		return err
	}

	rayFound, err := utils.IsCrdAvailable(r.ClientConfig, ray.SchemeGroupVersion.String(), constants.RayClusterKind)
	if err != nil {
		return err
	}

	lwsFound, err := utils.IsCrdAvailable(r.ClientConfig, lws.SchemeGroupVersion.String(), constants.LWSKind)
	if err != nil {
		return err
	}

	kedaFound, err := utils.IsCrdAvailable(r.ClientConfig, kedav1.SchemeGroupVersion.String(), constants.KEDAScaledObjectKind)
	if err != nil {
		return err
	}

	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.InferenceService{}).
		Owns(&appsv1.Deployment{}).
		Owns(&v1.Service{}).
		Owns(&v1.ConfigMap{}).
		Owns(&v1.PersistentVolume{}).
		Owns(&v1.PersistentVolumeClaim{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&policyv1.PodDisruptionBudget{})

	if ksvcFound {
		ctrlBuilder = ctrlBuilder.Owns(&knservingv1.Service{})
	} else {
		r.Log.Info("The InferenceService controller won't watch serving.knative.dev/v1/Service resources because the CRD is not available.")
	}

	if rayFound {
		ctrlBuilder = ctrlBuilder.Owns(&ray.RayCluster{})
	} else {
		r.Log.Info("The InferenceService controller won't watch ray.io/v1/RayCluster resources because the CRD is not available.")
	}

	if kedaFound {
		ctrlBuilder = ctrlBuilder.Owns(&kedav1.ScaledObject{})
	} else {
		r.Log.Info("The InferenceService controller won't watch keda.sh/v1/ScaledObject resources because the CRD is not available.")
	}

	if lwsFound {
		ctrlBuilder = ctrlBuilder.Owns(&lws.LeaderWorkerSet{})
	} else {
		r.Log.Info("The InferenceService controller won't watch leaderworkerset.x-k8s.io/v1/LeaderWorkerSet resources because the CRD is not available.")
	}

	if vsFound && !ingressConfig.DisableIstioVirtualHost {
		ctrlBuilder = ctrlBuilder.Owns(&istioclientv1beta1.VirtualService{})
	} else {
		r.Log.Info("The InferenceService controller won't watch networking.istio.io/v1beta1/VirtualService resources because the CRD is not available.")
	}

	// Add watches for ServingRuntime and ClusterServingRuntime to populate cache
	ctrlBuilder = ctrlBuilder.
		Watches(&v1beta1.ServingRuntime{},
			handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
				return nil // Just populate cache
			})).
		Watches(&v1beta1.ClusterServingRuntime{},
			handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
				return nil // Just populate cache
			}))

	return ctrlBuilder.Complete(r)
}

func (r *InferenceServiceReconciler) setExternalServiceURL(ctx context.Context, isvc *v1beta1.InferenceService, ingressConfig *controllerconfig.IngressConfig) error {
	// Get the external service
	externalService := &v1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, externalService); err != nil {
		return err
	}

	// Get the port from the external service
	var port int32 = constants.CommonISVCPort // default port
	if len(externalService.Spec.Ports) > 0 {
		port = externalService.Spec.Ports[0].Port
	}

	// Set the URL and Address of the external service with port
	host := network.GetServiceHostname(externalService.Name, externalService.Namespace)
	hostWithPort := fmt.Sprintf("%s:%d", host, port)

	isvc.Status.URL = &knapis.URL{
		Host:   hostWithPort,
		Scheme: "http",
	}
	isvc.Status.Address = &duckv1.Addressable{
		URL: &knapis.URL{
			Host:   hostWithPort,
			Scheme: "http",
		},
	}

	return nil
}

// migratePredictorToNewArchitecture delegates to the migration utility
func (r *InferenceServiceReconciler) migratePredictorToNewArchitecture(isvc *v1beta1.InferenceService) error {
	return isvcutils.MigratePredictorToNewArchitecture(context.Background(), r.Client, r.Log, isvc)
}
