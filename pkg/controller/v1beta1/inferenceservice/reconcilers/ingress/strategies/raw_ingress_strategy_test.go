package strategies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
)

func TestKubernetesIngressStrategy_GetName(t *testing.T) {
	strategy := createKubernetesIngressStrategy(t)
	assert.Equal(t, "KubernetesIngress", strategy.GetName())
}

func TestKubernetesIngressStrategy_Reconcile(t *testing.T) {
	tests := []struct {
		name                  string
		isvc                  *v1beta1.InferenceService
		ingressConfig         *controllerconfig.IngressConfig
		existingIngress       *netv1.Ingress
		expectedError         bool
		expectedIngressReady  corev1.ConditionStatus
		expectIngressCreation bool
	}{
		{
			name: "successful reconcile with predictor only",
			isvc: createTestInferenceServiceRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:         false,
			expectedIngressReady:  corev1.ConditionTrue,
			expectIngressCreation: true,
		},
		{
			name: "successful reconcile with router",
			isvc: createTestInferenceServiceWithRouterRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:         false,
			expectedIngressReady:  corev1.ConditionTrue,
			expectIngressCreation: true,
		},
		{
			name: "successful reconcile with decoder",
			isvc: createTestInferenceServiceWithDecoderRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:         false,
			expectedIngressReady:  corev1.ConditionTrue,
			expectIngressCreation: true,
		},
		{
			name: "ingress creation disabled",
			isvc: createTestInferenceServiceRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: true,
			},
			expectedError:         false,
			expectedIngressReady:  corev1.ConditionTrue,
			expectIngressCreation: false,
		},
		{
			name: "cluster local visibility",
			isvc: createTestInferenceServiceWithClusterLocalRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:         false,
			expectedIngressReady:  corev1.ConditionTrue,
			expectIngressCreation: true,
		},
		{
			name: "cluster local domain",
			isvc: createTestInferenceServiceRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          constants.ClusterLocalDomain,
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:         false,
			expectedIngressReady:  corev1.ConditionTrue,
			expectIngressCreation: true,
		},
		{
			name: "update existing ingress",
			isvc: createTestInferenceServiceRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			existingIngress: &netv1.Ingress{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			expectedError:         false,
			expectedIngressReady:  corev1.ConditionTrue,
			expectIngressCreation: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, netv1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			// Set component statuses to ready
			setComponentStatusReadyRaw(tt.isvc)

			objs := []client.Object{tt.isvc}
			if tt.existingIngress != nil {
				objs = append(objs, tt.existingIngress)
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			// Use the proper constructor to create the strategy with builder
			opts := interfaces.ReconcilerOptions{
				Client:        fakeClient,
				Scheme:        scheme,
				IngressConfig: tt.ingressConfig,
				IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
			}
			strategy := NewKubernetesIngressStrategy(opts, services.NewDomainService(), services.NewPathService())

			err := strategy.Reconcile(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedIngressReady, tt.isvc.Status.GetCondition(v1beta1.IngressReady).Status)
				assert.NotNil(t, tt.isvc.Status.URL)
				assert.NotNil(t, tt.isvc.Status.Address)
			}
		})
	}
}

func TestKubernetesIngressStrategy_CreateRawURL(t *testing.T) {
	tests := []struct {
		name        string
		isvc        *v1beta1.InferenceService
		config      *controllerconfig.IngressConfig
		expectedURL string
	}{
		{
			name: "simple domain generation",
			isvc: createTestInferenceServiceRaw("test-isvc", "default"),
			config: &controllerconfig.IngressConfig{
				UrlScheme:      "https",
				IngressDomain:  "example.com",
				DomainTemplate: "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			},
			expectedURL: "https://test-isvc.default.example.com",
		},
		{
			name: "custom domain template",
			isvc: createTestInferenceServiceRaw("test-isvc", "default"),
			config: &controllerconfig.IngressConfig{
				UrlScheme:      "http",
				IngressDomain:  "local.dev",
				DomainTemplate: "{{.Name}}-service.{{.IngressDomain}}",
			},
			expectedURL: "http://test-isvc-service.local.dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &KubernetesIngressStrategy{
				ingressConfig: tt.config,
				domainService: services.NewDomainService(),
			}

			url, err := strategy.createRawURL(tt.isvc)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedURL, url.String())
		})
	}
}

func TestKubernetesIngressStrategy_GetRawServiceHost(t *testing.T) {
	tests := []struct {
		name         string
		isvc         *v1beta1.InferenceService
		expectedHost string
	}{
		{
			name:         "with router",
			isvc:         createTestInferenceServiceWithRouterRaw("test-isvc", "default"),
			expectedHost: "test-isvc-router.default.svc.cluster.local",
		},
		{
			// Engine Service is named "<isvc>-engine" (constants.EngineServiceName);
			// "<isvc>" resolves to no Service.
			name:         "without router",
			isvc:         createTestInferenceServiceRaw("test-isvc", "default"),
			expectedHost: "test-isvc-engine.default.svc.cluster.local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createKubernetesIngressStrategy(t)
			host := strategy.(*KubernetesIngressStrategy).getRawServiceHost(tt.isvc)
			assert.Equal(t, tt.expectedHost, host)
		})
	}
}

func TestKubernetesIngressStrategy_SemanticIngressEquals(t *testing.T) {
	tests := []struct {
		name     string
		desired  *netv1.Ingress
		existing *netv1.Ingress
		expected bool
	}{
		{
			name: "identical ingresses",
			desired: &netv1.Ingress{
				Spec: netv1.IngressSpec{
					Rules: []netv1.IngressRule{
						{
							Host: "test.example.com",
						},
					},
				},
			},
			existing: &netv1.Ingress{
				Spec: netv1.IngressSpec{
					Rules: []netv1.IngressRule{
						{
							Host: "test.example.com",
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "different ingresses",
			desired: &netv1.Ingress{
				Spec: netv1.IngressSpec{
					Rules: []netv1.IngressRule{
						{
							Host: "test.example.com",
						},
					},
				},
			},
			existing: &netv1.Ingress{
				Spec: netv1.IngressSpec{
					Rules: []netv1.IngressRule{
						{
							Host: "different.example.com",
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createKubernetesIngressStrategy(t)
			result := strategy.(*KubernetesIngressStrategy).semanticIngressEquals(tt.desired, tt.existing)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper functions
func createKubernetesIngressStrategy(t *testing.T) interfaces.IngressStrategy {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, netv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).Build()

	opts := interfaces.ReconcilerOptions{
		Client: fakeClient,
		Scheme: scheme,
		IngressConfig: &controllerconfig.IngressConfig{
			EnableGatewayAPI: false,
			IngressDomain:    "example.com",
			IngressClassName: stringPtr("nginx"),
			DomainTemplate:   "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			UrlScheme:        "https",
		},
		IsvcConfig: &controllerconfig.InferenceServicesConfig{},
	}

	return NewKubernetesIngressStrategy(opts, services.NewDomainService(), services.NewPathService())
}

func createTestInferenceServiceRaw(name, namespace string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
		},
		Status: v1beta1.InferenceServiceStatus{
			Status: duckv1.Status{
				Conditions: []apis.Condition{},
			},
		},
	}
}

func createTestInferenceServiceWithRouterRaw(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceRaw(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	return isvc
}

func createTestInferenceServiceWithDecoderRaw(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceRaw(name, namespace)
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func createTestInferenceServiceWithClusterLocalRaw(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceRaw(name, namespace)
	isvc.Labels = map[string]string{
		constants.VisibilityLabel: constants.ClusterLocalVisibility,
	}
	return isvc
}

func setComponentStatusReadyRaw(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.EngineReady, &apis.Condition{
		Type:   v1beta1.EngineReady,
		Status: corev1.ConditionTrue,
	})

	if isvc.Spec.Router != nil {
		isvc.Status.SetCondition(v1beta1.RouterReady, &apis.Condition{
			Type:   v1beta1.RouterReady,
			Status: corev1.ConditionTrue,
		})
	}

	if isvc.Spec.Decoder != nil {
		isvc.Status.SetCondition(v1beta1.DecoderReady, &apis.Condition{
			Type:   v1beta1.DecoderReady,
			Status: corev1.ConditionTrue,
		})
	}
}

func TestKubernetesIngressStrategy_URLWithPort(t *testing.T) {
	tests := []struct {
		name                string
		isvc                *v1beta1.InferenceService
		ingressConfig       *controllerconfig.IngressConfig
		services            []corev1.Service
		expectedURLHost     string
		expectedAddressHost string
	}{
		{
			name: "engine only with custom port",
			isvc: createTestInferenceServiceRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: true, // Skip ingress creation to focus on URL
			},
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-isvc-engine",
						Namespace: "default",
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 8081},
						},
					},
				},
			},
			expectedURLHost: "test-isvc.default.example.com:8081",
			// Cluster-local address targets the actual engine Service
			// ("<isvc>-engine") — the same one the port was read from.
			expectedAddressHost: "test-isvc-engine.default.svc.cluster.local:8081",
		},
		{
			name: "with router and custom port",
			isvc: createTestInferenceServiceWithRouterRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: true,
			},
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-isvc-router",
						Namespace: "default",
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Port: 8082},
						},
					},
				},
			},
			expectedURLHost:     "test-isvc.default.example.com:8082",
			expectedAddressHost: "test-isvc-router.default.svc.cluster.local:8082",
		},
		{
			name: "service not found uses default port",
			isvc: createTestInferenceServiceRaw("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       false,
				IngressDomain:          "example.com",
				IngressClassName:       stringPtr("nginx"),
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: true,
			},
			services:            []corev1.Service{}, // No services
			expectedURLHost:     "test-isvc.default.example.com:8080",
			expectedAddressHost: "test-isvc-engine.default.svc.cluster.local:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, netv1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			setComponentStatusReadyRaw(tt.isvc)

			objs := []client.Object{tt.isvc}
			for i := range tt.services {
				objs = append(objs, &tt.services[i])
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			opts := interfaces.ReconcilerOptions{
				Client:        fakeClient,
				Scheme:        scheme,
				IngressConfig: tt.ingressConfig,
				IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
			}
			strategy := NewKubernetesIngressStrategy(opts, services.NewDomainService(), services.NewPathService())

			err := strategy.Reconcile(context.Background(), tt.isvc)

			assert.NoError(t, err)
			assert.NotNil(t, tt.isvc.Status.URL)
			assert.Equal(t, tt.expectedURLHost, tt.isvc.Status.URL.Host)
			assert.NotNil(t, tt.isvc.Status.Address)
			assert.Equal(t, tt.expectedAddressHost, tt.isvc.Status.Address.URL.Host)
		})
	}
}

// stringPtr returns a pointer to the given string.
func stringPtr(s string) *string {
	return &s
}
