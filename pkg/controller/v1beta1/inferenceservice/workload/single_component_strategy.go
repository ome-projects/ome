package workload

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/components"
)

// SingleComponentStrategy implements the single component independent deployment strategy.
// This is the default strategy where each component is reconciled independently without using All-in-One workloads.
type SingleComponentStrategy struct {
	log logr.Logger
}

func NewSingleComponentStrategy(log logr.Logger) *SingleComponentStrategy {
	return &SingleComponentStrategy{
		log: log,
	}
}

func (s *SingleComponentStrategy) GetStrategyName() string {
	return "SingleComponent"
}

func (s *SingleComponentStrategy) IsApplicable(isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType) bool {

	// By default, SingleComponent strategy is always applicable.
	return true
}

// ValidateDeploymentModes validates component deployment modes.
func (s *SingleComponentStrategy) ValidateDeploymentModes(modes *ComponentDeploymentModes) error {
	// SingleComponent strategy supports all deployment modes, no validation needed.
	return nil
}

// ReconcileWorkload executes component workload reconciliation.
func (s *SingleComponentStrategy) ReconcileWorkload(ctx context.Context, request *WorkloadReconcileRequest) (ctrl.Result, error) {
	s.log.Info("Reconciling with SingleComponent strategy",
		"namespace", request.InferenceService.Namespace,
		"inferenceService", request.InferenceService.Name)

	var reconcilers []components.Component

	// Create Engine component
	if request.MergedEngine != nil {
		s.log.Info("Creating engine reconciler",
			"deploymentMode", request.DeploymentModes.Engine,
			"namespace", request.InferenceService.Namespace,
			"inferenceService", request.InferenceService.Name)

		engineReconciler := request.ComponentBuilderFactory.CreateEngineComponent(
			request.DeploymentModes.Engine,
			request.BaseModel,
			request.BaseModelMeta,
			request.MergedEngine,
			request.Runtime,
			request.RuntimeName,
			request.EngineSupportedModelFormat,
			request.EngineAcceleratorClass,
			request.EngineAcceleratorClassName,
		)
		reconcilers = append(reconcilers, engineReconciler)
	}

	// Create Decoder component
	if request.MergedDecoder != nil {
		s.log.Info("Creating decoder reconciler",
			"deploymentMode", request.DeploymentModes.Decoder,
			"namespace", request.InferenceService.Namespace,
			"inferenceService", request.InferenceService.Name)

		decoderReconciler := request.ComponentBuilderFactory.CreateDecoderComponent(
			request.DeploymentModes.Decoder,
			request.BaseModel,
			request.BaseModelMeta,
			request.MergedDecoder,
			request.Runtime,
			request.RuntimeName,
			request.DecoderSupportedModelFormat,
			request.DecoderAcceleratorClass,
			request.DecoderAcceleratorClassName,
		)
		reconcilers = append(reconcilers, decoderReconciler)
	}

	// Create Router component
	if request.MergedRouter != nil {
		s.log.Info("Creating router reconciler",
			"deploymentMode", request.DeploymentModes.Router,
			"namespace", request.InferenceService.Namespace,
			"inferenceService", request.InferenceService.Name)

		routerReconciler := request.ComponentBuilderFactory.CreateRouterComponent(
			request.DeploymentModes.Router,
			request.BaseModel,
			request.BaseModelMeta,
			request.MergedRouter,
			request.Runtime,
			request.RuntimeName,
		)
		reconcilers = append(reconcilers, routerReconciler)
	}

	// Run all reconcilers
	for _, reconciler := range reconcilers {
		result, err := reconciler.Reconcile(request.InferenceService)
		if err != nil {
			s.log.Error(err, "Failed to reconcile component",
				"component", fmt.Sprintf("%T", reconciler),
				"namespace", request.InferenceService.Namespace,
				"inferenceService", request.InferenceService.Name)
			return result, err
		}
		if result.Requeue || result.RequeueAfter > 0 {
			return result, nil
		}
	}

	s.log.Info("SingleComponent strategy reconciliation completed",
		"namespace", request.InferenceService.Namespace,
		"inferenceService", request.InferenceService.Name)

	return reconcile.Result{}, nil
}
