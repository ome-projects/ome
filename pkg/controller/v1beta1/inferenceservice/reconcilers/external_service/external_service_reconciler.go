package external_service

import (
	"context"
	"reflect"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// ExternalServiceReconciler reconciles the external service for an InferenceService
// This service provides external access when ingress is disabled
type ExternalServiceReconciler struct {
	client        client.Client
	clientset     kubernetes.Interface
	scheme        *runtime.Scheme
	ingressConfig *controllerconfig.IngressConfig
}

// NewExternalServiceReconciler creates a new external service reconciler
func NewExternalServiceReconciler(
	client client.Client,
	clientset kubernetes.Interface,
	scheme *runtime.Scheme,
	ingressConfig *controllerconfig.IngressConfig,
) *ExternalServiceReconciler {
	return &ExternalServiceReconciler{
		client:        client,
		clientset:     clientset,
		scheme:        scheme,
		ingressConfig: ingressConfig,
	}
}

// Reconcile reconciles the external service for the InferenceService
func (r *ExternalServiceReconciler) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error {
	// Determine if external service should be created
	shouldCreateExternalService := r.shouldCreateExternalService(isvc)

	// Get existing service
	existingService := &corev1.Service{}
	err := r.client.Get(ctx, client.ObjectKey{
		Namespace: isvc.Namespace,
		Name:      isvc.Name,
	}, existingService)

	serviceExists := err == nil
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to get external service for InferenceService %s", isvc.Name)
	}

	if shouldCreateExternalService {
		// Build the desired service
		desiredService, err := r.buildExternalService(ctx, isvc)
		if err != nil {
			return errors.Wrapf(err, "failed to build external service for InferenceService %s", isvc.Name)
		}

		if serviceExists {
			// Update existing service if spec has changed
			if !r.serviceSpecsEqual(&existingService.Spec, &desiredService.Spec) {
				existingService.Spec = desiredService.Spec
				existingService.Labels = desiredService.Labels
				existingService.Annotations = desiredService.Annotations
				if err := r.client.Update(ctx, existingService); err != nil {
					return errors.Wrapf(err, "failed to update external service for InferenceService %s", isvc.Name)
				}
			}
		} else {
			// Create new service
			if err := r.client.Create(ctx, desiredService); err != nil {
				return errors.Wrapf(err, "failed to create external service for InferenceService %s", isvc.Name)
			}
		}
	} else if serviceExists {
		// Delete existing service if it should no longer exist
		if err := r.client.Delete(ctx, existingService); err != nil {
			return errors.Wrapf(err, "failed to delete external service for InferenceService %s", isvc.Name)
		}
	}

	return nil
}

// shouldCreateExternalService determines whether the external service should be created
func (r *ExternalServiceReconciler) shouldCreateExternalService(isvc *v1beta1.InferenceService) bool {
	// Only create if ingress creation is disabled
	if !r.ingressConfig.DisableIngressCreation {
		return false
	}

	// Don't create for cluster-local services
	if val, ok := isvc.Labels[constants.VisibilityLabel]; ok && val == constants.ClusterLocalVisibility {
		return false
	}

	// Only create if there are components that can serve traffic
	return irprojector.RoleDeclared(isvc, v1beta1.RouterComponent) || irprojector.RoleDeclared(isvc, v1beta1.EngineComponent)
}

// determineTargetSelector determines which component should be the target
// for the external service. The pods are labeled with their replica's name
// prefix, which is the referenced replica's own name for a referenced role.
func (r *ExternalServiceReconciler) determineTargetSelector(isvc *v1beta1.InferenceService) map[string]string {
	baseSelector := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
	}

	// Priority: Router > Engine
	for _, c := range []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent} {
		if irprojector.RoleDeclared(isvc, c) {
			baseSelector[constants.InferenceServicePodLabelKey] = irprojector.RoleReplicaPrefix(isvc, c)
			baseSelector[constants.OMEComponentLabel] = string(c)
			return baseSelector
		}
	}
	return baseSelector
}

// buildExternalService builds the external service specification
func (r *ExternalServiceReconciler) buildExternalService(ctx context.Context, isvc *v1beta1.InferenceService) (*corev1.Service, error) {
	selector := r.determineTargetSelector(isvc)

	// Get the target port from the internal service
	targetPort, err := isvcutils.TargetServicePort(ctx, r.client, isvc, irprojector.RoleDeclared(isvc, v1beta1.RouterComponent))
	if err != nil {
		targetPort = constants.CommonISVCPort
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      isvc.Name,
			Namespace: isvc.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           "external-service",
			},
			Annotations: r.getServiceAnnotations(isvc),
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       targetPort,
					TargetPort: intstr.FromInt32(targetPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Type: r.getServiceType(isvc),
		},
	}

	// Set owner reference
	if err := controllerutil.SetControllerReference(isvc, service, r.scheme); err != nil {
		return nil, errors.Wrapf(err, "failed to set controller reference for external service")
	}

	return service, nil
}

// getServiceType determines the service type based on annotations
func (r *ExternalServiceReconciler) getServiceType(isvc *v1beta1.InferenceService) corev1.ServiceType {
	if serviceType, ok := isvc.Annotations[constants.ServiceType]; ok {
		switch serviceType {
		case "LoadBalancer":
			return corev1.ServiceTypeLoadBalancer
		case "NodePort":
			return corev1.ServiceTypeNodePort
		case "ClusterIP":
			return corev1.ServiceTypeClusterIP
		}
	}
	// Default to ClusterIP
	return corev1.ServiceTypeClusterIP
}

// getServiceAnnotations extracts service-related annotations from the InferenceService
func (r *ExternalServiceReconciler) getServiceAnnotations(isvc *v1beta1.InferenceService) map[string]string {
	annotations := make(map[string]string)

	// Copy service-related annotations
	serviceAnnotationPrefixes := []string{
		"service.beta.kubernetes.io/",
		"cloud.google.com/",
		"service.kubernetes.io/",
	}

	for key, value := range isvc.Annotations {
		for _, prefix := range serviceAnnotationPrefixes {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				annotations[key] = value
				break
			}
		}
	}

	return annotations
}

// serviceSpecsEqual compares two service specs for equality
func (r *ExternalServiceReconciler) serviceSpecsEqual(spec1, spec2 *corev1.ServiceSpec) bool {
	return reflect.DeepEqual(spec1.Selector, spec2.Selector) &&
		reflect.DeepEqual(spec1.Ports, spec2.Ports) &&
		spec1.Type == spec2.Type
}

// getDeploymentMode determines the deployment mode of the InferenceService
func (r *ExternalServiceReconciler) getDeploymentMode(isvc *v1beta1.InferenceService) constants.DeploymentModeType {
	// Check if this is a MultiNode deployment by looking for Leader/Worker specs
	if isvc.Spec.Engine != nil {
		if isvc.Spec.Engine.Leader != nil || isvc.Spec.Engine.Worker != nil {
			return constants.MultiNode
		}
	}

	// All other cases are RawDeployment
	return constants.RawDeployment
}
