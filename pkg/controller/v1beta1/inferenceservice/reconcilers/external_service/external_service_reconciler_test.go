package external_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestExternalServiceReconciler_shouldCreateExternalService(t *testing.T) {
	tests := []struct {
		name          string
		isvc          *v1beta1.InferenceService
		ingressConfig *controllerconfig.IngressConfig
		expected      bool
		description   string
	}{
		{
			name: "should create when ingress disabled and has engine",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: true,
			},
			expected:    true,
			description: "should create external service when ingress is disabled and engine component exists",
		},
		{
			name: "should create when ingress disabled and has router",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Router: &v1beta1.RouterSpec{},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: true,
			},
			expected:    true,
			description: "should create external service when ingress is disabled and router component exists",
		},
		{
			name: "should not create when ingress enabled",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: false,
			},
			expected:    false,
			description: "should not create external service when ingress is enabled",
		},
		{
			name: "should not create for cluster-local service",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
					Labels: map[string]string{
						constants.VisibilityLabel: constants.ClusterLocalVisibility,
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: true,
			},
			expected:    false,
			description: "should not create external service for cluster-local services",
		},
		{
			name: "should create for multinode deployment when ingress disabled",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						Leader: &v1beta1.LeaderSpec{},
						Worker: &v1beta1.WorkerSpec{},
					},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: true,
			},
			expected:    true,
			description: "should create external service for multinode deployments when ingress disabled",
		},
		{
			name: "should not create when no components",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: true,
			},
			expected:    false,
			description: "should not create external service when no servable components exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fakeclient.New(t)
			clientset := fake.NewSimpleClientset()

			reconciler := NewExternalServiceReconciler(client, clientset, client.Scheme(), tt.ingressConfig)
			result := reconciler.shouldCreateExternalService(tt.isvc)

			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestExternalServiceReconciler_determineTargetSelector(t *testing.T) {
	tests := []struct {
		name             string
		isvc             *v1beta1.InferenceService
		expectedSelector map[string]string
		description      string
	}{
		{
			name: "router component takes priority",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Router: &v1beta1.RouterSpec{},
					Engine: &v1beta1.EngineSpec{},
				},
			},
			expectedSelector: map[string]string{
				constants.InferenceServicePodLabelKey: "test-service",
				constants.OMEComponentLabel:           string(v1beta1.RouterComponent),
			},
			description: "router component should be selected when multiple components exist",
		},
		{
			name: "engine component when no router",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			expectedSelector: map[string]string{
				constants.InferenceServicePodLabelKey: "test-service",
				constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			},
			description: "engine component should be selected when router doesn't exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fakeclient.New(t)
			clientset := fake.NewSimpleClientset()
			ingressConfig := &controllerconfig.IngressConfig{}

			reconciler := NewExternalServiceReconciler(client, clientset, client.Scheme(), ingressConfig)
			result := reconciler.determineTargetSelector(tt.isvc)

			assert.Equal(t, tt.expectedSelector, result, tt.description)
		})
	}
}

func TestExternalServiceReconciler_buildExternalService(t *testing.T) {
	tests := []struct {
		name               string
		isvc               *v1beta1.InferenceService
		internalService    *corev1.Service
		expectedTargetPort int32
		description        string
	}{
		{
			name: "basic service creation with engine",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			internalService: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service-engine",
					Namespace: "default",
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Port: 8081,
						},
					},
				},
			},
			expectedTargetPort: 8081,
			description:        "should create basic external service with port from internal engine service",
		},
		{
			name: "service with LoadBalancer type annotation and router",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
					Annotations: map[string]string{
						constants.ServiceType: "LoadBalancer",
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Router: &v1beta1.RouterSpec{},
				},
			},
			internalService: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service-router",
					Namespace: "default",
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Port: 8082,
						},
					},
				},
			},
			expectedTargetPort: 8082,
			description:        "should create LoadBalancer type external service with port from internal router service",
		},
		{
			name: "fallback to default port when internal service not found",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			internalService:    nil, // No internal service
			expectedTargetPort: constants.CommonISVCPort,
			description:        "should fallback to default port when internal service not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []client.Object
			if tt.internalService != nil {
				objects = append(objects, tt.internalService)
			}

			client := fakeclient.New(t, fakeclient.WithObjects(objects...))
			clientset := fake.NewSimpleClientset()
			ingressConfig := &controllerconfig.IngressConfig{}

			reconciler := NewExternalServiceReconciler(client, clientset, client.Scheme(), ingressConfig)
			service, err := reconciler.buildExternalService(context.TODO(), tt.isvc)

			assert.NoError(t, err, "should not return error when building external service")
			assert.NotNil(t, service, "external service should not be nil")
			assert.Equal(t, tt.isvc.Name, service.Name, "service name should match InferenceService name")
			assert.Equal(t, tt.isvc.Namespace, service.Namespace, "service namespace should match InferenceService namespace")

			// Check if service type is set correctly
			if serviceType, ok := tt.isvc.Annotations[constants.ServiceType]; ok && serviceType == "LoadBalancer" {
				assert.Equal(t, corev1.ServiceTypeLoadBalancer, service.Spec.Type, "service type should be LoadBalancer")
			} else {
				assert.Equal(t, corev1.ServiceTypeClusterIP, service.Spec.Type, "service type should default to ClusterIP")
			}

			// Check ports - both Port and TargetPort use the dynamic port from internal service
			assert.Len(t, service.Spec.Ports, 1, "should have one port")
			assert.Equal(t, "http", service.Spec.Ports[0].Name, "port name should be http")
			assert.Equal(t, tt.expectedTargetPort, service.Spec.Ports[0].Port, "port should match internal service port")
			assert.Equal(t, intstr.FromInt32(tt.expectedTargetPort), service.Spec.Ports[0].TargetPort, "target port should match internal service port")
		})
	}
}

func TestExternalServiceReconciler_Reconcile(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		ingressConfig   *controllerconfig.IngressConfig
		existingService *corev1.Service
		internalService *corev1.Service
		expectedAction  string
		expectError     bool
		description     string
	}{
		{
			name: "create external service when ingress disabled",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: true,
			},
			existingService: nil,
			internalService: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service-engine",
					Namespace: "default",
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Port: 8080,
						},
					},
				},
			},
			expectedAction: "create",
			expectError:    false,
			description:    "should create external service when none exists and ingress is disabled",
		},
		{
			name: "update external service when spec changes",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
					Annotations: map[string]string{
						constants.ServiceType: "LoadBalancer",
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Router: &v1beta1.RouterSpec{},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: true,
			},
			existingService: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeClusterIP,
					Selector: map[string]string{
						constants.InferenceServicePodLabelKey: "test-service",
						constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       80,
							TargetPort: intstr.FromInt(8080),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
			internalService: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service-router",
					Namespace: "default",
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Port: 8080,
						},
					},
				},
			},
			expectedAction: "update",
			expectError:    false,
			description:    "should update external service when spec changes",
		},
		{
			name: "delete external service when ingress enabled",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: false,
			},
			existingService: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeClusterIP,
					Selector: map[string]string{
						constants.InferenceServicePodLabelKey: "test-service",
						constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       80,
							TargetPort: intstr.FromInt(8080),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
			internalService: nil,
			expectedAction:  "delete",
			expectError:     false,
			description:     "should delete external service when ingress is enabled",
		},
		{
			name: "no action when ingress enabled and no service exists",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			ingressConfig: &controllerconfig.IngressConfig{
				DisableIngressCreation: false,
			},
			existingService: nil,
			internalService: nil,
			expectedAction:  "none",
			expectError:     false,
			description:     "should take no action when ingress is enabled and no service exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []client.Object
			if tt.existingService != nil {
				objects = append(objects, tt.existingService)
			}
			if tt.internalService != nil {
				objects = append(objects, tt.internalService)
			}

			client := fakeclient.New(t, fakeclient.WithObjects(objects...))
			clientset := fake.NewSimpleClientset()

			reconciler := NewExternalServiceReconciler(client, clientset, client.Scheme(), tt.ingressConfig)

			err := reconciler.Reconcile(context.TODO(), tt.isvc)

			if tt.expectError {
				assert.Error(t, err, "should return error")
			} else {
				assert.NoError(t, err, "should not return error")

				// Verify the expected action was performed
				service := &corev1.Service{}
				getErr := client.Get(context.TODO(), types.NamespacedName{
					Namespace: tt.isvc.Namespace,
					Name:      tt.isvc.Name,
				}, service)

				switch tt.expectedAction {
				case "create", "update":
					assert.NoError(t, getErr, "external service should exist after creation/update")
					assert.Equal(t, tt.isvc.Name, service.Name, "service name should match")
				case "delete", "none":
					assert.True(t, apierrors.IsNotFound(getErr), "external service should not exist after deletion or when not created")
				}
			}
		})
	}
}
