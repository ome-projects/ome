package interfaces

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// IngressStrategy defines the interface for different ingress reconciliation strategies
type IngressStrategy interface {
	Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error
	GetName() string
}

// DomainService handles domain name generation logic
type DomainService interface {
	GenerateDomainName(name string, obj interface{}, ingressConfig *controllerconfig.IngressConfig) (string, error)
	GetAdditionalHosts(domainList *[]string, serviceHost string, config *controllerconfig.IngressConfig) *[]string
}

// PathService handles URL path generation
type PathService interface {
	GenerateUrlPath(name string, namespace string, ingressConfig *controllerconfig.IngressConfig) (string, error)
}

// Reconciler is the main interface for ingress reconciliation
type Reconciler interface {
	Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error
}

// StrategyFactory creates appropriate ingress strategies based on configuration
type StrategyFactory interface {
	CreateStrategyWithOptions(deploymentMode string, opts ReconcilerOptions) (IngressStrategy, error)
}

// ReconcilerOptions contains configuration for the ingress reconciler
type ReconcilerOptions struct {
	Client        client.Client
	Scheme        *runtime.Scheme
	IngressConfig *controllerconfig.IngressConfig
	IsvcConfig    *controllerconfig.InferenceServicesConfig
}
