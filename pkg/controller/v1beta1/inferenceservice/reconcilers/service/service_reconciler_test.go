package service

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestBuildServiceFiltersAnnotations(t *testing.T) {
	scenarios := map[string]struct {
		componentMeta         metav1.ObjectMeta
		expectedAnnotations   map[string]string
		unexpectedAnnotations []string
	}{
		"FilterGrafanaAnnotations": {
			componentMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
				Annotations: map[string]string{
					"k8s.grafana.com/scrape": "true",
					"k8s.grafana.com/port":   "8080",
					"ome.io/base-model-name": "test-model",
					"ome.io/service-type":    "ClusterIP",
				},
			},
			expectedAnnotations: map[string]string{
				"ome.io/base-model-name": "test-model",
				"ome.io/service-type":    "ClusterIP",
			},
			unexpectedAnnotations: []string{
				"k8s.grafana.com/scrape",
				"k8s.grafana.com/port",
			},
		},
		"FilterLokiAnnotations": {
			componentMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
				Annotations: map[string]string{
					"loki.grafana.com/scrape":     "true",
					"loki.grafana.com/log-format": "json",
					"ome.io/serving-runtime":      "test-runtime",
				},
			},
			expectedAnnotations: map[string]string{
				"ome.io/serving-runtime": "test-runtime",
			},
			unexpectedAnnotations: []string{
				"loki.grafana.com/scrape",
				"loki.grafana.com/log-format",
			},
		},
		"FilterPrometheusAnnotations": {
			componentMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
				Annotations: map[string]string{
					"prometheus.io/scrape":   "true",
					"prometheus.io/port":     "8080",
					"prometheus.io/path":     "/metrics",
					"ome.io/serving-runtime": "test-runtime",
				},
			},
			expectedAnnotations: map[string]string{
				"ome.io/serving-runtime": "test-runtime",
			},
			unexpectedAnnotations: []string{
				"prometheus.io/scrape",
				"prometheus.io/port",
				"prometheus.io/path",
			},
		},
		"FilterNetworkingGKEAnnotations": {
			componentMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
				Annotations: map[string]string{
					"networking.gke.io/default-interface": "eth0",
					"networking.gke.io/interfaces":        "[{\"interfaceName\":\"eth0\"}]",
					"ome.io/deploymentMode":               "RawDeployment",
				},
			},
			expectedAnnotations: map[string]string{
				"ome.io/deploymentMode": "RawDeployment",
			},
			unexpectedAnnotations: []string{
				"networking.gke.io/default-interface",
				"networking.gke.io/interfaces",
			},
		},
		"FilterInjectionAnnotations": {
			componentMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
				Annotations: map[string]string{
					constants.FineTunedAdapterInjectionKey: "weight-name",
					constants.ServingSidecarInjectionKey:   "true",
					"ome.io/base-model-name":               "test-model",
				},
			},
			expectedAnnotations: map[string]string{
				"ome.io/base-model-name": "test-model",
			},
			unexpectedAnnotations: []string{
				constants.FineTunedAdapterInjectionKey,
				constants.ServingSidecarInjectionKey,
			},
		},
		"FilterMixedAnnotations": {
			componentMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
				Annotations: map[string]string{
					"k8s.grafana.com/scrape":               "true",
					"networking.gke.io/interfaces":         "[...]",
					constants.FineTunedAdapterInjectionKey: "weight-name",
					"rdma.ome.io/auto-inject":              "true",
					"ome.io/base-model-name":               "test-model",
					"ome.io/service-type":                  "ClusterIP",
					"meta.helm.sh/release-name":            "test",
				},
			},
			expectedAnnotations: map[string]string{
				"ome.io/base-model-name":    "test-model",
				"ome.io/service-type":       "ClusterIP",
				"meta.helm.sh/release-name": "test",
			},
			unexpectedAnnotations: []string{
				"k8s.grafana.com/scrape",
				"networking.gke.io/interfaces",
				constants.FineTunedAdapterInjectionKey,
				"rdma.ome.io/auto-inject",
			},
		},
		"PreserveAllNonPodOnlyAnnotations": {
			componentMeta: metav1.ObjectMeta{
				Name:      "test-service",
				Namespace: "default",
				Annotations: map[string]string{
					"ome.io/deploymentMode":     "RawDeployment",
					"ome.io/service-type":       "ClusterIP",
					"ome.io/load-balancer-ip":   "10.0.0.1",
					"custom.annotation/key":     "value",
					"meta.helm.sh/release-name": "test",
				},
			},
			expectedAnnotations: map[string]string{
				"ome.io/deploymentMode":     "RawDeployment",
				"ome.io/service-type":       "ClusterIP",
				"ome.io/load-balancer-ip":   "10.0.0.1",
				"custom.annotation/key":     "value",
				"meta.helm.sh/release-name": "test",
			},
			unexpectedAnnotations: []string{},
		},
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "test-container",
			Ports: []corev1.ContainerPort{{
				Name:          "http",
				ContainerPort: 8080,
			}},
		}},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			service := buildService(scenario.componentMeta, nil, podSpec, nil)

			// Check that expected annotations are present
			for key, expectedValue := range scenario.expectedAnnotations {
				if actualValue, ok := service.Annotations[key]; !ok {
					t.Errorf("Expected annotation %q to be present on Service", key)
				} else if actualValue != expectedValue {
					t.Errorf("Annotation %q: expected %q, got %q", key, expectedValue, actualValue)
				}
			}

			// Check that unexpected (pod-only) annotations are NOT present
			for _, key := range scenario.unexpectedAnnotations {
				if _, ok := service.Annotations[key]; ok {
					t.Errorf("Unexpected pod-only annotation %q found on Service", key)
				}
			}
		})
	}
}

func TestBuildServicePreservesOtherMetadata(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "test-service",
		Namespace: "test-namespace",
		Labels: map[string]string{
			"app":     "test-app",
			"version": "v1",
		},
		Annotations: map[string]string{
			"k8s.grafana.com/scrape": "true",
			"ome.io/service-type":    "ClusterIP",
		},
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "test-container",
			Ports: []corev1.ContainerPort{{
				Name:          "http",
				ContainerPort: 8080,
			}},
		}},
	}

	service := buildService(componentMeta, nil, podSpec, nil)

	// Name and Namespace should be preserved
	if service.Name != "test-service" {
		t.Errorf("Expected service name to be 'test-service', got %q", service.Name)
	}
	if service.Namespace != "test-namespace" {
		t.Errorf("Expected service namespace to be 'test-namespace', got %q", service.Namespace)
	}

	// Labels should be preserved
	expectedLabels := map[string]string{
		"app":     "test-app",
		"version": "v1",
	}
	if diff := cmp.Diff(expectedLabels, service.Labels); diff != "" {
		t.Errorf("Labels mismatch (-want +got):\n%s", diff)
	}

	// Pod-only annotation should be filtered
	if _, ok := service.Annotations["k8s.grafana.com/scrape"]; ok {
		t.Error("Pod-only annotation should have been filtered")
	}

	// Non-pod-only annotation should be preserved
	if service.Annotations["ome.io/service-type"] != "ClusterIP" {
		t.Error("Non-pod-only annotation should have been preserved")
	}
}

func TestBuildServiceWithNilAnnotations(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:        "test-service",
		Namespace:   "default",
		Annotations: nil,
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "test-container",
			Ports: []corev1.ContainerPort{{
				ContainerPort: 8080,
			}},
		}},
	}

	// Should not panic with nil annotations
	service := buildService(componentMeta, nil, podSpec, nil)

	if service == nil {
		t.Error("Expected service to be created, got nil")
	}
}

func TestBuildServiceWithEmptyAnnotations(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:        "test-service",
		Namespace:   "default",
		Annotations: map[string]string{},
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "test-container",
			Ports: []corev1.ContainerPort{{
				ContainerPort: 8080,
			}},
		}},
	}

	service := buildService(componentMeta, nil, podSpec, nil)

	if service == nil {
		t.Fatal("Expected service to be created, got nil")
	}
	if len(service.Annotations) != 0 {
		t.Errorf("Expected empty annotations, got %v", service.Annotations)
	}
}

func TestBuildServiceSetsConfiguredPortAppProtocols(t *testing.T) {
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "router",
			Ports: []corev1.ContainerPort{
				{Name: "http", ContainerPort: 8000},
				{Name: "metrics", ContainerPort: 29000},
			},
		}},
	}
	componentExt := &v1beta1.ComponentExtensionSpec{
		ServicePortAppProtocols: map[string]string{
			"http": "kubernetes.io/h2c",
		},
	}

	service := buildService(metav1.ObjectMeta{Name: "test-service"}, componentExt, podSpec, nil)

	if got := service.Spec.Ports[0].AppProtocol; got == nil || *got != "kubernetes.io/h2c" {
		t.Errorf("http appProtocol: got %v, want kubernetes.io/h2c", got)
	}
	if got := service.Spec.Ports[1].AppProtocol; got != nil {
		t.Errorf("metrics appProtocol: got %q, want nil", *got)
	}
}

func TestServiceReconcilerAddsAppProtocolToExistingService(t *testing.T) {
	existing := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test-service", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.1",
			Selector:  map[string]string{"app": "test-service"},
			Ports: []corev1.ServicePort{{
				Name: "http",
				Port: 8000,
			}},
		},
	}
	c := fakeclient.New(t, fakeclient.WithObjects(existing))
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{
		Name: "router",
		Ports: []corev1.ContainerPort{{
			Name:          "http",
			ContainerPort: 8000,
		}},
	}}}
	componentExt := &v1beta1.ComponentExtensionSpec{
		ServicePortAppProtocols: map[string]string{"http": "kubernetes.io/h2c"},
	}
	reconciler := NewServiceReconciler(
		c,
		c.Scheme(),
		metav1.ObjectMeta{Name: "test-service", Namespace: "default"},
		componentExt,
		podSpec,
		nil,
	)

	if _, err := reconciler.Reconcile(); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "test-service", Namespace: "default"}, got); err != nil {
		t.Fatalf("get Service after reconcile: %v", err)
	}
	if got.Spec.ClusterIP != existing.Spec.ClusterIP {
		t.Errorf("ClusterIP: got %q, want %q", got.Spec.ClusterIP, existing.Spec.ClusterIP)
	}
	if got.Spec.Ports[0].AppProtocol == nil || *got.Spec.Ports[0].AppProtocol != "kubernetes.io/h2c" {
		t.Errorf("appProtocol: got %v, want kubernetes.io/h2c", got.Spec.Ports[0].AppProtocol)
	}
}

// TestServiceReconciler_OMENativeShape verifies the top-level
// ServiceReconciler emits the per-Component stable Service the
// OMENative dispatch path expects. OMENative ISVCs route through this
// same reconciler rather than an omenative-side duplicate.
// This test pins the contract: given an OMENative-shape selector +
// Component meta name, the reconciler emits a Service with the
// expected Name, Namespace, Selector (including the
// LabelManagedBy=OMENative discriminator), Type, OwnerReferences, and
// derived Ports. Object-presence + spec only — no traffic-routing
// assertions.
func TestServiceReconciler_OMENativeShape(t *testing.T) {
	ns := "prod"
	isvcName := "llama-70b"
	component := v1beta1.EngineComponent
	serviceName := isvcName + "-" + string(component) // matches constants.EngineServiceName

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      isvcName,
			Namespace: ns,
			UID:       types.UID(isvcName + "-uid"),
		},
	}
	c := fakeclient.New(t, fakeclient.WithObjects(isvc))

	// componentMeta as the OMENative caller assembles it: name matches
	// the per-Component Service name (constants.EngineServiceName and its
	// decoder and router siblings), with the
	// per-Component OwnerReferences stamped on it (mirrors the
	// reconcileOMENativeSubresources wiring in
	// components/{engine,decoder,router}.go).
	componentMeta := metav1.ObjectMeta{
		Name:      serviceName,
		Namespace: ns,
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
		},
	}
	// OMENative-shape selector: three keys including the
	// LabelManagedBy=OMENative discriminator. This is what the
	// omenative.componentSelector helper emits.
	selector := map[string]string{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
		"ome.io/managed-by":                   "OMENative",
	}
	// Rendered PodSpec with a single container declaring port 8080 —
	// matches what the OMENative renderer emits today.
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "runner",
			Ports: []corev1.ContainerPort{{
				Name:          "http",
				ContainerPort: 8080,
				Protocol:      corev1.ProtocolTCP,
			}},
		}},
	}

	componentExt := &v1beta1.ComponentExtensionSpec{
		ServicePortAppProtocols: map[string]string{"http": "kubernetes.io/h2c"},
	}
	sr := NewServiceReconciler(c, c.Scheme(), componentMeta, componentExt, podSpec, selector)
	if _, err := sr.Reconcile(); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: serviceName}, got); err != nil {
		t.Fatalf("get Service after reconcile: %v", err)
	}

	if got.Name != serviceName {
		t.Errorf("Service.Name: got %q want %q", got.Name, serviceName)
	}
	if got.Namespace != ns {
		t.Errorf("Service.Namespace: got %q want %q", got.Namespace, ns)
	}
	if got.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Service.Spec.Type: got %q want ClusterIP", got.Spec.Type)
	}
	// LabelManagedBy is load-bearing for the OMENative path: scopes the
	// Service to OMENative-managed pods so a same-component mode-switch
	// (engine OMENative → engine RawDeployment) doesn't strand traffic
	// on the wrong pod set. Drop the assertion if/when the discriminator
	// becomes provably unnecessary.
	if diff := cmp.Diff(selector, got.Spec.Selector); diff != "" {
		t.Errorf("Service.Spec.Selector (-want +got):\n%s", diff)
	}
	if len(got.Spec.Ports) != 1 {
		t.Fatalf("Service.Spec.Ports: got %d ports want 1", len(got.Spec.Ports))
	}
	port := got.Spec.Ports[0]
	if port.Name != "http" {
		t.Errorf("Service.Spec.Ports[0].Name: got %q want http", port.Name)
	}
	if port.Port != 8080 {
		t.Errorf("Service.Spec.Ports[0].Port: got %d want 8080", port.Port)
	}
	if port.AppProtocol == nil || *port.AppProtocol != "kubernetes.io/h2c" {
		t.Errorf("Service.Spec.Ports[0].AppProtocol: got %v want kubernetes.io/h2c", port.AppProtocol)
	}
	if len(got.OwnerReferences) != 1 {
		t.Fatalf("Service.OwnerReferences: got %d want 1", len(got.OwnerReferences))
	}
	ref := got.OwnerReferences[0]
	if ref.Kind != "InferenceService" || ref.Name != isvcName {
		t.Errorf("Service.OwnerReferences[0]: got Kind=%q Name=%q want InferenceService/%s", ref.Kind, ref.Name, isvcName)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("Service.OwnerReferences[0].Controller: want true, got %v", ref.Controller)
	}
}
