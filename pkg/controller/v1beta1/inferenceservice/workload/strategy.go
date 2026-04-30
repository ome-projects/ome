package workload

import (
	"context"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	ctrl "sigs.k8s.io/controller-runtime"
)

// WorkloadStrategy defines the unified interface for workload strategies.
// All workload strategies (SingleComponent, RBG, etc.) must implement this interface.
type WorkloadStrategy interface {
	// GetStrategyName returns the name of the strategy.
	GetStrategyName() string

	// IsApplicable determines whether this strategy is applicable to the current InferenceService.
	IsApplicable(isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType) bool

	// ValidateDeploymentModes validates whether component deployment modes are supported by this strategy.
	// Different strategies may support different deployment modes.
	ValidateDeploymentModes(modes *ComponentDeploymentModes) error

	// ReconcileWorkload executes workload reconciliation.
	// This is the core method of the strategy, responsible for creating and updating workload resources.
	ReconcileWorkload(ctx context.Context, request *WorkloadReconcileRequest) (ctrl.Result, error)
}
