package strategies

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// TargetServicePortNotAvailable is the IngressReady=False reason written
// while the target Service's port cannot be read.
const TargetServicePortNotAvailable = "TargetServicePortNotAvailable"

// KubernetesIngressStrategy handles Kubernetes Ingress (raw deployment mode)
type KubernetesIngressStrategy struct {
	client        client.Client
	scheme        *runtime.Scheme
	ingressConfig *controllerconfig.IngressConfig
	isvcConfig    *controllerconfig.InferenceServicesConfig
	domainService interfaces.DomainService
	pathService   interfaces.PathService
	builder       interfaces.IngressBuilder
}

// NewKubernetesIngressStrategy creates a new Kubernetes Ingress strategy
func NewKubernetesIngressStrategy(opts interfaces.ReconcilerOptions, domainService interfaces.DomainService, pathService interfaces.PathService) interfaces.IngressStrategy {
	builder := builders.NewIngressBuilder(opts.Client, opts.Scheme, opts.IngressConfig, opts.IsvcConfig, domainService, pathService)

	return &KubernetesIngressStrategy{
		client:        opts.Client,
		scheme:        opts.Scheme,
		ingressConfig: opts.IngressConfig,
		isvcConfig:    opts.IsvcConfig,
		domainService: domainService,
		pathService:   pathService,
		builder:       builder,
	}
}

func (k *KubernetesIngressStrategy) GetName() string {
	return "KubernetesIngress"
}

func (k *KubernetesIngressStrategy) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error {
	if !k.ingressConfig.DisableIngressCreation {
		built, err := k.reconcileIngress(ctx, isvc)
		if err != nil || !built {
			return err
		}
	}

	// status.url names the target Service's port. A port that cannot be
	// read yet, because the Service is absent or declares none, leaves the
	// URL unset and ends the pass with the error so the next pass reads it
	// again: a URL with a guessed port would name a port no Service declares.
	servicePort, err := isvcutils.TargetServicePort(ctx, k.client, isvc, irprojector.RoleDeclared(isvc, v1beta1.RouterComponent))
	if err != nil {
		return k.markPortNotAvailable(isvc, err)
	}
	return k.setStatusEndpoints(isvc, servicePort)
}

// markPortNotAvailable writes IngressReady=False with the reason the target
// Service's port could not be read and hands the error back to the pass.
func (k *KubernetesIngressStrategy) markPortNotAvailable(isvc *v1beta1.InferenceService, err error) error {
	isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
		Type:    v1beta1.IngressReady,
		Status:  corev1.ConditionFalse,
		Reason:  TargetServicePortNotAvailable,
		Message: fmt.Sprintf("target Service port not read: %v", err),
	})
	return fmt.Errorf("target service port: %w", err)
}

// reconcileIngress brings the service's Ingress to the desired spec and
// reports whether one stands. A Component that cannot back a rule yet writes
// IngressReady=False and ends the pass without a URL.
func (k *KubernetesIngressStrategy) reconcileIngress(ctx context.Context, isvc *v1beta1.InferenceService) (bool, error) {
	desired, err := k.builder.BuildIngress(ctx, isvc)
	var notReady *interfaces.ComponentNotReadyError
	if errors.As(err, &notReady) {
		isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:    v1beta1.IngressReady,
			Status:  corev1.ConditionFalse,
			Reason:  "ComponentNotReady",
			Message: "Target service not ready for ingress creation",
		})
		klog.V(1).InfoS("Component not ready; Ingress not built", "isvc", isvc.Name, "component", notReady.Component)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("builder error: %w", err)
	}

	// A builder that hands back no Ingress without saying why broke its
	// contract; refuse it instead of dereferencing a nil object below.
	ingress, ok := desired.(*netv1.Ingress)
	if !ok || ingress == nil {
		return false, fmt.Errorf("builder returned %T, expected a non-nil *netv1.Ingress", desired)
	}

	existing := &netv1.Ingress{}
	err = k.client.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}, existing)
	switch {
	case apierr.IsNotFound(err):
		return true, k.client.Create(ctx, ingress)
	case err != nil:
		return false, err
	}
	if k.semanticIngressEquals(ingress, existing) {
		return true, nil
	}
	// Set ResourceVersion which is required for update operation
	ingress.ResourceVersion = existing.ResourceVersion
	return true, k.client.Update(ctx, ingress)
}

// setStatusEndpoints writes status.url, the domain name on the target
// Service's port, and status.address, the in-cluster hostname on that port,
// then IngressReady=True.
func (k *KubernetesIngressStrategy) setStatusEndpoints(isvc *v1beta1.InferenceService, servicePort int32) error {
	serviceHost, err := k.createRawURL(isvc)
	if err != nil {
		return err
	}
	serviceHost.Host = fmt.Sprintf("%s:%d", serviceHost.Host, servicePort)
	isvc.Status.URL = serviceHost

	hostWithPort := fmt.Sprintf("%s:%d", k.getRawServiceHost(isvc), servicePort)
	isvc.Status.Address = &duckv1.Addressable{
		URL: &apis.URL{
			Host:   hostWithPort,
			Scheme: k.ingressConfig.UrlScheme,
			Path:   "",
		},
	}
	isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
		Type:   v1beta1.IngressReady,
		Status: corev1.ConditionTrue,
	})
	return nil
}

func (k *KubernetesIngressStrategy) createRawURL(isvc *v1beta1.InferenceService) (*apis.URL, error) {
	var err error
	url := &apis.URL{}
	url.Scheme = k.ingressConfig.UrlScheme
	url.Host, err = k.domainService.GenerateDomainName(isvc.Name, isvc.ObjectMeta, k.ingressConfig)
	if err != nil {
		return nil, err
	}
	return url, nil
}

// getRawServiceHost is the in-cluster hostname of the Service that fronts
// the InferenceService: the router's when one is declared, else the engine's
// ("<isvc>-engine", constants.EngineServiceName, the same name
// TargetServicePort resolves the port from). The domain suffix comes from
// ingress.clusterDomain.
func (k *KubernetesIngressStrategy) getRawServiceHost(isvc *v1beta1.InferenceService) string {
	if irprojector.RoleDeclared(isvc, v1beta1.RouterComponent) {
		return constants.ServiceHostname(constants.RouterServiceName(isvc.Name), isvc.Namespace, k.ingressConfig.ClusterDomain)
	}
	return constants.ServiceHostname(constants.EngineServiceName(isvc.Name), isvc.Namespace, k.ingressConfig.ClusterDomain)
}

func (k *KubernetesIngressStrategy) semanticIngressEquals(desired, existing *netv1.Ingress) bool {
	return equality.Semantic.DeepEqual(desired.Spec, existing.Spec)
}
