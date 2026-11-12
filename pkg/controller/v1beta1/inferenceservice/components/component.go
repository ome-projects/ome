package components

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Component can be reconciled to create underlying resources for an InferenceService
type Component interface {
	Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error)
}
