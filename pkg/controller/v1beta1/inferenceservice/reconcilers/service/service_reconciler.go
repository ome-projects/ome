package service

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils"
)

var log = logf.Log.WithName("ServiceReconciler")

// ServiceReconciler reconciles Service objects
type ServiceReconciler struct {
	client       client.Client
	scheme       *runtime.Scheme
	Service      *corev1.Service
	componentExt *v1beta1.ComponentExtensionSpec
}

// NewServiceReconciler creates a new ServiceReconciler instance
func NewServiceReconciler(client client.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
	podSpec *corev1.PodSpec,
	Selector map[string]string,
) *ServiceReconciler {
	return &ServiceReconciler{
		client:       client,
		scheme:       scheme,
		Service:      buildService(componentMeta, componentExt, podSpec, Selector),
		componentExt: componentExt,
	}
}

// ServiceTypeFromAnnotations reads the Service type the annotations ask for.
// An absent or unknown value means ClusterIP.
func ServiceTypeFromAnnotations(annotations map[string]string) corev1.ServiceType {
	switch annotations[constants.ServiceType] {
	case "LoadBalancer":
		return corev1.ServiceTypeLoadBalancer
	case "NodePort":
		return corev1.ServiceTypeNodePort
	default:
		return corev1.ServiceTypeClusterIP
	}
}

// buildServiceWithLoadBalancer constructs a Service object with LoadBalancer IP support
func buildServiceWithLoadBalancer(
	componentMeta metav1.ObjectMeta,
	serviceType corev1.ServiceType,
	servicePorts []corev1.ServicePort,
	selector map[string]string,
) *corev1.Service {

	var loadBalancerIP string
	if loadBalancerIPAnnotation, ok := componentMeta.Annotations[constants.LoadBalancerIP]; ok {
		loadBalancerIP = loadBalancerIPAnnotation
	}

	spec := corev1.ServiceSpec{
		Type:     serviceType,
		Selector: selector,
		Ports:    servicePorts,
	}

	if serviceType == corev1.ServiceTypeLoadBalancer && loadBalancerIP != "" {
		spec.LoadBalancerIP = loadBalancerIP
	}

	service := &corev1.Service{
		ObjectMeta: componentMeta,
		Spec:       spec,
	}
	// service metadata name has 63 limitation, update metadata name if it reaches the limitation
	service.Name = constants.TruncateNameWithMaxLength(service.Name, 63)
	return service
}

// buildService constructs a Service object from the given specifications.
// It filters out pod-only annotations (e.g., Grafana scraping, GKE networking, container injection)
// that should not be applied to Services. These annotations are defined in component specs
// (like engineConfig.annotations) for pod-level configurations but have no effect on Services.
func buildService(componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
	podSpec *corev1.PodSpec,
	selector map[string]string,
) *corev1.Service {
	// Create a copy to avoid mutating the original componentMeta, then filter pod-only annotations.
	// The original componentMeta with all annotations is still used for Deployment/Pod creation.
	serviceMeta := componentMeta.DeepCopy()
	serviceMeta.Annotations = utils.FilterPodOnlyAnnotations(componentMeta.Annotations)

	var appProtocols map[string]string
	if componentExt != nil {
		appProtocols = componentExt.ServicePortAppProtocols
	}
	servicePorts := buildServicePorts(podSpec, appProtocols)
	serviceType := ServiceTypeFromAnnotations(serviceMeta.Annotations)
	if selector == nil {
		selector = map[string]string{"app": constants.TruncateNameWithMaxLength(serviceMeta.Name, 63)}
	}

	return buildServiceWithLoadBalancer(*serviceMeta, serviceType, servicePorts, selector)
}

// buildServicePorts creates service ports configuration from pod spec
func buildServicePorts(podSpec *corev1.PodSpec, appProtocols map[string]string) []corev1.ServicePort {
	if len(podSpec.Containers) == 0 {
		return nil
	}

	container := podSpec.Containers[0]
	if len(container.Ports) == 0 {
		return []corev1.ServicePort{buildDefaultServicePort(container.Name, appProtocols[container.Name])}
	}

	servicePorts := make([]corev1.ServicePort, 0, len(container.Ports))
	for _, port := range container.Ports {
		servicePorts = append(servicePorts, buildServicePort(port, appProtocols[port.Name]))
	}
	return servicePorts
}

// buildServicePort creates a ServicePort from a ContainerPort
func buildServicePort(containerPort corev1.ContainerPort, appProtocol string) corev1.ServicePort {
	protocol := containerPort.Protocol
	if protocol == "" {
		protocol = corev1.ProtocolTCP
	}

	servicePort := corev1.ServicePort{
		Name: containerPort.Name,
		Port: containerPort.ContainerPort,
		TargetPort: intstr.IntOrString{
			Type:   intstr.Int,
			IntVal: containerPort.ContainerPort,
		},
		Protocol: protocol,
	}
	if appProtocol != "" {
		servicePort.AppProtocol = &appProtocol
	}
	return servicePort
}

// buildDefaultServicePort creates a default ServicePort
func buildDefaultServicePort(name, appProtocol string) corev1.ServicePort {
	port, _ := strconv.ParseInt(constants.InferenceServiceDefaultHttpPort, 10, 32)
	servicePort := corev1.ServicePort{
		Name: name,
		Port: constants.CommonISVCPort,
		TargetPort: intstr.IntOrString{
			Type:   intstr.Int,
			IntVal: int32(port),
		},
		Protocol: corev1.ProtocolTCP,
	}
	if appProtocol != "" {
		servicePort.AppProtocol = &appProtocol
	}
	return servicePort
}

// Reconcile ensures the Service matches the desired state
func (r *ServiceReconciler) Reconcile() (*corev1.Service, error) {
	checkResult, existingService, err := r.checkServiceState()
	log.V(1).Info("Reconcile service", "namespace", r.Service.Namespace, "name", r.Service.Name, "checkResult", checkResult)
	if err != nil {
		return nil, err
	}

	return r.handleReconcileAction(checkResult, existingService)
}

// checkServiceState checks the current state of the service
func (r *ServiceReconciler) checkServiceState() (constants.CheckResultType, *corev1.Service, error) {
	existingService := &corev1.Service{}
	err := r.client.Get(context.TODO(), types.NamespacedName{
		Namespace: r.Service.Namespace,
		Name:      r.Service.Name,
	}, existingService)

	if err != nil {
		if apierr.IsNotFound(err) {
			return constants.CheckResultCreate, nil, nil
		}
		return constants.CheckResultUnknown, nil, err
	}

	// existed, check equivalent
	if semanticServiceEquals(r.Service, existingService) {
		return constants.CheckResultExisted, existingService, nil
	}
	return constants.CheckResultUpdate, existingService, nil
}

// semanticServiceEquals compares the desired service spec with the existing one,
// focusing on fields that might require an update.
func semanticServiceEquals(desired, existing *corev1.Service) bool {
	return equality.Semantic.DeepEqual(desired.Spec.Ports, existing.Spec.Ports) &&
		equality.Semantic.DeepEqual(desired.Spec.Selector, existing.Spec.Selector)
}

// handleReconcileAction performs the appropriate action based on the reconcile check result
func (r *ServiceReconciler) handleReconcileAction(checkResult constants.CheckResultType, existingService *corev1.Service) (*corev1.Service, error) {
	ctx := context.TODO()

	switch checkResult {
	case constants.CheckResultCreate:
		log.Info("Creating Service", "namespace", r.Service.Namespace, "name", r.Service.Name)
		if err := r.client.Create(ctx, r.Service); err != nil {
			log.Error(err, "Failed to create Service", "namespace", r.Service.Namespace, "name", r.Service.Name)
			return nil, err
		}
		return r.Service, nil
	case constants.CheckResultUpdate:
		existingService.Spec.Ports = r.Service.Spec.Ports
		existingService.Spec.Selector = r.Service.Spec.Selector
		if err := r.client.Update(ctx, existingService); err != nil {
			log.Error(err, "Failed to update Service", "namespace", r.Service.Namespace, "name", r.Service.Name)
			return nil, err
		}
		return existingService, nil
	default:
		return existingService, nil
	}
}
