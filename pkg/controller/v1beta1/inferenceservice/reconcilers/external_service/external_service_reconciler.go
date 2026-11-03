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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
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

// Reconcile makes the external Service match what the InferenceService asks for:
// present when ingress creation is disabled and a role can serve, absent otherwise.
func (r *ExternalServiceReconciler) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error {
	existing, err := r.existingExternalService(ctx, isvc)
	if err != nil {
		return err
	}
	desired, err := r.desiredExternalService(ctx, isvc)
	if err != nil {
		return err
	}
	return r.applyExternalService(ctx, isvc, existing, desired)
}

// existingExternalService reads the external Service; nil when there is none.
func (r *ExternalServiceReconciler) existingExternalService(ctx context.Context, isvc *v1beta1.InferenceService) (*corev1.Service, error) {
	existing := &corev1.Service{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: isvc.Namespace, Name: isvc.Name}, existing)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get external service for InferenceService %s", isvc.Name)
	}
	return existing, nil
}

// desiredExternalService builds the Service the InferenceService should have;
// nil when it should have none.
func (r *ExternalServiceReconciler) desiredExternalService(ctx context.Context, isvc *v1beta1.InferenceService) (*corev1.Service, error) {
	if !r.shouldCreateExternalService(isvc) {
		return nil, nil
	}
	desired, err := r.buildExternalService(ctx, isvc)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to build external service for InferenceService %s", isvc.Name)
	}
	return desired, nil
}

// applyExternalService creates, updates or deletes the external Service so the
// cluster holds desired; a nil desired means the Service goes.
func (r *ExternalServiceReconciler) applyExternalService(ctx context.Context, isvc *v1beta1.InferenceService, existing, desired *corev1.Service) error {
	switch {
	case desired == nil && existing == nil:
		return nil
	case desired == nil:
		return errors.Wrapf(r.client.Delete(ctx, existing), "failed to delete external service for InferenceService %s", isvc.Name)
	case existing == nil:
		return errors.Wrapf(r.client.Create(ctx, desired), "failed to create external service for InferenceService %s", isvc.Name)
	case r.serviceSpecsEqual(&existing.Spec, &desired.Spec):
		return nil
	}
	existing.Spec = desired.Spec
	existing.Labels = desired.Labels
	existing.Annotations = desired.Annotations
	return errors.Wrapf(r.client.Update(ctx, existing), "failed to update external service for InferenceService %s", isvc.Name)
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

	desired := &corev1.Service{
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
			Type: service.ServiceTypeFromAnnotations(isvc.Annotations),
		},
	}

	if err := controllerutil.SetControllerReference(isvc, desired, r.scheme); err != nil {
		return nil, errors.Wrapf(err, "failed to set controller reference for external service")
	}

	return desired, nil
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
