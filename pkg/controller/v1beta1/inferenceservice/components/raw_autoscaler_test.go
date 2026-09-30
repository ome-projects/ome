package components

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/render"
)

func TestRawComponentDeployment_UsesTypedKEDAForEveryComponent(t *testing.T) {
	for _, component := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		t.Run(string(component), func(t *testing.T) {
			scheme := rawAutoscalerTestScheme(t)
			cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
			clientset := rawAutoscalerTestClientset()
			minReplicas := 1
			ext := v1beta1.ComponentExtensionSpec{
				MinReplicas: &minReplicas,
				MaxReplicas: 4,
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerKEDA,
					Keda: &v1beta1.KedaAutoscaler{
						Triggers: []kedav1.ScaleTriggers{{
							Type: "prometheus",
							Metadata: map[string]string{
								"serverAddress": "https://metrics.example.com",
								"query":         "requests_in_flight",
								"threshold":     "5",
							},
						}},
					},
				},
			}
			isvc := rawAutoscalerTestISVC(component, ext)
			componentMeta := metav1.ObjectMeta{
				Name:        isvc.Name + "-" + string(component),
				Namespace:   isvc.Namespace,
				Annotations: map[string]string{},
			}
			podSpec := &corev1.PodSpec{
				Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/runtime:test"}},
			}
			deps := &ComponentDeps{
				Client:    cl,
				Clientset: clientset,
				Scheme:    scheme,
				Config:    &controllerconfig.InferenceServicesConfig{},
			}

			var err error
			switch component {
			case v1beta1.EngineComponent:
				engine := NewEngine(deps, ComponentInputs{DeploymentMode: constants.RawDeployment}, isvc.Spec.Engine).(*Engine)
				request := mustComponentPDBRequest(t, &engine.BaseComponentFields, isvc, constants.RawDeployment, component, componentMeta, &isvc.Spec.Engine.ComponentExtensionSpec)
				_, err = engine.reconcileDeployment(context.Background(), isvc, componentMeta, podSpec, 0, nil, request)
			case v1beta1.DecoderComponent:
				decoder := NewDecoder(deps, ComponentInputs{DeploymentMode: constants.RawDeployment}, isvc.Spec.Decoder).(*Decoder)
				request := mustComponentPDBRequest(t, &decoder.BaseComponentFields, isvc, constants.RawDeployment, component, componentMeta, &isvc.Spec.Decoder.ComponentExtensionSpec)
				_, err = decoder.reconcileDeployment(context.Background(), isvc, componentMeta, podSpec, 0, nil, request)
			case v1beta1.RouterComponent:
				router := NewRouter(deps, ComponentInputs{DeploymentMode: constants.RawDeployment}, isvc.Spec.Router).(*Router)
				request := mustComponentPDBRequest(t, &router.BaseComponentFields, isvc, constants.RawDeployment, component, componentMeta, &isvc.Spec.Router.ComponentExtensionSpec)
				_, err = router.reconcileDeployment(context.Background(), isvc, componentMeta, podSpec, request)
			}
			require.NoError(t, err)

			so := &kedav1.ScaledObject{}
			require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
				Namespace: componentMeta.Namespace,
				Name:      isvcutils.GetScaledObjectName(componentMeta.Name),
			}, so))
			assert.Equal(t, "apps/v1", so.Spec.ScaleTargetRef.APIVersion)
			assert.Equal(t, "Deployment", so.Spec.ScaleTargetRef.Kind)
			assert.Equal(t, componentMeta.Name, so.Spec.ScaleTargetRef.Name)

			hpa := &autoscalingv2.HorizontalPodAutoscaler{}
			err = cl.Get(context.Background(), types.NamespacedName{Namespace: componentMeta.Namespace, Name: componentMeta.Name}, hpa)
			assert.True(t, apierrors.IsNotFound(err), "typed KEDA must not leave an HPA")
		})
	}
}

func TestWriteComponentAutoscalerStatus_RawCompatibilityResolution(t *testing.T) {
	tests := []struct {
		name          string
		autoscaler    *v1beta1.ComponentAutoscaler
		annotations   map[string]string
		wantClass     v1beta1.AutoscalerClass
		wantManagedBy v1beta1.AutoscalerManagedBy
		wantSource    string
		wantErr       string
	}{
		{
			name:          "typed None overrides legacy HPA",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			annotations:   map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassHPA)},
			wantClass:     v1beta1.AutoscalerNone,
			wantManagedBy: v1beta1.AutoscalerManagedByNone,
			wantSource:    "isvc",
		},
		{
			name:          "legacy HPA reports legacy source",
			annotations:   map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassHPA)},
			wantClass:     v1beta1.AutoscalerHPA,
			wantManagedBy: v1beta1.AutoscalerManagedByOME,
			wantSource:    "legacy",
		},
		{
			name:        "invalid legacy class returns an error",
			annotations: map[string]string{constants.AutoscalerClass: "unsupported"},
			wantErr:     "unknown legacy autoscaler class",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := rawAutoscalerTestScheme(t)
			cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "status-raw", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: tt.autoscaler},
					},
				},
			}
			base := &BaseComponentFields{Piece: render.Piece{DeploymentMode: constants.RawDeployment}, Client: cl}
			objectMeta := metav1.ObjectMeta{Name: "status-raw-engine", Namespace: "default", Annotations: tt.annotations}

			err := writeComponentAutoscalerStatus(base, isvc, v1beta1.EngineComponent, objectMeta, &v1beta1.ComponentExtensionSpec{})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			status := isvc.Status.Components[v1beta1.EngineComponent]
			require.NotNil(t, status.Autoscaler)
			assert.Equal(t, tt.wantClass, status.Autoscaler.Class)
			assert.Equal(t, tt.wantManagedBy, status.Autoscaler.ManagedBy)
			assert.Equal(t, tt.wantSource, status.Autoscaler.SpecSource)
			require.NotNil(t, status.ScaleTargetRef)
			assert.Equal(t, "apps/v1", status.ScaleTargetRef.APIVersion)
			assert.Equal(t, "Deployment", status.ScaleTargetRef.Kind)
			assert.Equal(t, objectMeta.Name, status.ScaleTargetRef.Name)
		})
	}
}

func rawAutoscalerTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, addToScheme := range []func(*runtime.Scheme) error{
		v1beta1.AddToScheme,
		appsv1.AddToScheme,
		autoscalingv2.AddToScheme,
		corev1.AddToScheme,
		policyv1.AddToScheme,
		kedav1.AddToScheme,
		monitoringv1.AddToScheme,
	} {
		require.NoError(t, addToScheme(scheme))
	}
	return scheme
}

func rawAutoscalerTestClientset() kubernetes.Interface {
	return fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: map[string]string{
			controllerconfig.IngressConfigKeyName: `{"ingressGateway":"knative-serving/knative-ingress-gateway","ingressService":"istio-ingressgateway.istio-system.svc.cluster.local","ingressDomain":"example.com","domainTemplate":"{{.Name}}.{{.Namespace}}.{{.IngressDomain}}","urlScheme":"https"}`,
		},
	})
}

func rawAutoscalerTestISVC(component v1beta1.ComponentType, ext v1beta1.ComponentExtensionSpec) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "typed-" + string(component),
			Namespace: "default",
			UID:       types.UID("typed-" + string(component) + "-uid"),
		},
	}
	switch component {
	case v1beta1.EngineComponent:
		isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: ext}
	case v1beta1.DecoderComponent:
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: ext}
	case v1beta1.RouterComponent:
		isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: ext}
	}
	return isvc
}
