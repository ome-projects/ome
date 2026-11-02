package ingress

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/factory"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

var mainLog = logf.Log.WithName("IngressReconciler")

// IngressReconciler is the new main ingress reconciler using the refactored architecture
type IngressReconciler struct {
	client        client.Client
	clientset     kubernetes.Interface
	scheme        *runtime.Scheme
	ingressConfig *controllerconfig.IngressConfig
	isvcConfig    *controllerconfig.InferenceServicesConfig
	factory       interfaces.StrategyFactory
}

// NewIngressReconciler creates a new main ingress reconciler
func NewIngressReconciler(
	client client.Client,
	clientset kubernetes.Interface,
	scheme *runtime.Scheme,
	ingressConfig *controllerconfig.IngressConfig,
	isvcConfig *controllerconfig.InferenceServicesConfig,
) interfaces.Reconciler {
	// Create factory
	strategyFactory := factory.NewStrategyFactory(clientset)

	return &IngressReconciler{
		client:        client,
		clientset:     clientset,
		scheme:        scheme,
		ingressConfig: ingressConfig,
		isvcConfig:    isvcConfig,
		factory:       strategyFactory,
	}
}

// ReconcileWithDeploymentMode reconciles ingress resources for a specific deployment mode
func (r *IngressReconciler) ReconcileWithDeploymentMode(ctx context.Context, isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType) error {
	mainLog := logf.FromContext(ctx)

	// Check if ingress creation is disabled
	if r.ingressConfig.DisableIngressCreation {
		mainLog.Info("Ingress creation disabled, skipping ingress reconciliation", "isvc", isvc.Name)

		// Set IngressReady to True since we're intentionally not creating ingress
		// External service will be created as fallback for cluster access
		mainLog.Info("Setting IngressReady condition to True with reason IngressDisabled", "isvc", isvc.Name)
		isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:    v1beta1.IngressReady,
			Status:  corev1.ConditionTrue,
			Reason:  "IngressDisabled",
			Message: "Ingress creation is disabled, using external service for access",
		})

		mainLog.Info("Returning early from ingress reconciler due to disabled ingress creation", "isvc", isvc.Name)
		return nil
	}

	// Resolve ingress config with annotation overrides
	resolvedIngressConfig := isvcutils.ResolveIngressConfig(r.ingressConfig, isvc.Annotations)

	// Create reconciler options
	opts := interfaces.ReconcilerOptions{
		Client:        r.client,
		Scheme:        r.scheme,
		IngressConfig: resolvedIngressConfig,
		IsvcConfig:    r.isvcConfig,
	}

	// Get the appropriate strategy
	strategy, err := r.getStrategy(deploymentMode, opts)
	if err != nil {
		return fmt.Errorf("failed to get ingress strategy for deployment mode %s: %w", deploymentMode, err)
	}

	mainLog.V(1).Info("Using ingress strategy",
		"strategy", strategy.GetName(),
		"isvc", isvc.Name)

	// Execute the strategy
	return strategy.Reconcile(ctx, isvc)
}

// Reconcile orchestrates the ingress reconciliation using the appropriate strategy
func (r *IngressReconciler) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error {
	// Determine deployment mode for ingress strategy selection
	deploymentMode := r.getDeploymentMode(isvc)

	return r.ReconcileWithDeploymentMode(ctx, isvc, deploymentMode)
}

// getDeploymentMode returns the deployment mode of the service's entrypoint
// component, read from the service's own role blocks.
func (r *IngressReconciler) getDeploymentMode(isvc *v1beta1.InferenceService) constants.DeploymentModeType {
	modes, err := isvcutils.DetermineRoleDeploymentModes(v1beta1.ServiceRoles(&isvc.Spec), isvc.Spec.DeploymentMode)
	if err != nil {
		mainLog.Error(err, "Failed to determine deployment modes, falling back to RawDeployment", "isvc", isvc.Name)
		return constants.RawDeployment
	}
	if mode, ok := modes[isvcutils.DetermineEntrypointComponent(isvc)]; ok {
		return mode
	}
	return modes[v1beta1.EngineComponent]
}

// getStrategy returns the appropriate strategy for the deployment mode
func (r *IngressReconciler) getStrategy(deploymentMode constants.DeploymentModeType, opts interfaces.ReconcilerOptions) (interfaces.IngressStrategy, error) {
	if r.factory == nil {
		return nil, fmt.Errorf("strategy factory is not initialized")
	}
	return r.factory.CreateStrategyWithOptions(string(deploymentMode), opts)
}
