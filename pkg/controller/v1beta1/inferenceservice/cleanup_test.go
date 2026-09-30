package inferenceservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
)

func TestCleanupRemovedComponents(t *testing.T) {
	testCases := []struct {
		name               string
		isvc               *v1beta1.InferenceService
		existingResources  []client.Object
		engineSpec         *v1beta1.EngineSpec
		decoderSpec        *v1beta1.DecoderSpec
		routerSpec         *v1beta1.RouterSpec
		expectedDeleted    []string
		expectedNotDeleted []string
	}{
		{
			name: "Delete orphaned decoder resources when decoder removed from spec",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
				},
			},
			existingResources: []client.Object{
				createDeployment("test-isvc-engine", "default", "test-isvc", "test-uid", v1beta1.EngineComponent),
				createDeployment("test-isvc-decoder", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent),
				createService("test-isvc-engine", "default", "test-isvc", "test-uid", v1beta1.EngineComponent),
				createService("test-isvc-decoder", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent),
			},
			engineSpec:         &v1beta1.EngineSpec{},
			decoderSpec:        nil, // Decoder removed
			routerSpec:         nil,
			expectedDeleted:    []string{"test-isvc-decoder"},
			expectedNotDeleted: []string{"test-isvc-engine"},
		},
		{
			name: "Delete orphaned router resources when router removed from spec",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
				},
			},
			existingResources: []client.Object{
				createDeployment("test-isvc-engine", "default", "test-isvc", "test-uid", v1beta1.EngineComponent),
				createDeployment("test-isvc-router", "default", "test-isvc", "test-uid", v1beta1.RouterComponent),
				createService("test-isvc-router", "default", "test-isvc", "test-uid", v1beta1.RouterComponent),
				createHPA("test-isvc-router-hpa", "default", "test-isvc", "test-uid", v1beta1.RouterComponent),
			},
			engineSpec:         &v1beta1.EngineSpec{},
			decoderSpec:        nil,
			routerSpec:         nil, // Router removed
			expectedDeleted:    []string{"test-isvc-router", "test-isvc-router-hpa"},
			expectedNotDeleted: []string{"test-isvc-engine"},
		},
		{
			name: "Keep all resources when all components are active",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
				},
			},
			existingResources: []client.Object{
				createDeployment("test-isvc-engine", "default", "test-isvc", "test-uid", v1beta1.EngineComponent),
				createDeployment("test-isvc-decoder", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent),
				createDeployment("test-isvc-router", "default", "test-isvc", "test-uid", v1beta1.RouterComponent),
			},
			engineSpec:         &v1beta1.EngineSpec{},
			decoderSpec:        &v1beta1.DecoderSpec{},
			routerSpec:         &v1beta1.RouterSpec{},
			expectedDeleted:    []string{},
			expectedNotDeleted: []string{"test-isvc-engine", "test-isvc-decoder", "test-isvc-router"},
		},
		{
			name: "Only delete resources with correct owner reference",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
				},
			},
			existingResources: []client.Object{
				createDeployment("test-isvc-engine", "default", "test-isvc", "test-uid", v1beta1.EngineComponent),
				createDeployment("test-isvc-decoder", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent),
				createDeploymentWithoutOwner("test-isvc-decoder-no-owner", "default", "test-isvc", v1beta1.DecoderComponent),
			},
			engineSpec:         &v1beta1.EngineSpec{},
			decoderSpec:        nil, // Decoder removed
			routerSpec:         nil,
			expectedDeleted:    []string{"test-isvc-decoder"},
			expectedNotDeleted: []string{"test-isvc-engine", "test-isvc-decoder-no-owner"},
		},
		{
			name: "Handle multiple resource types correctly",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
				},
			},
			existingResources: []client.Object{
				createDeployment("test-isvc-engine", "default", "test-isvc", "test-uid", v1beta1.EngineComponent),
				createService("test-isvc-engine", "default", "test-isvc", "test-uid", v1beta1.EngineComponent),
				createConfigMap("test-isvc-engine-config", "default", "test-isvc", "test-uid", v1beta1.EngineComponent),
				createDeployment("test-isvc-decoder", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent),
				createService("test-isvc-decoder", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent),
				createConfigMap("test-isvc-decoder-config", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent),
				createIngress("test-isvc-decoder-ingress", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent),
			},
			engineSpec:         &v1beta1.EngineSpec{},
			decoderSpec:        nil, // Decoder removed
			routerSpec:         nil,
			expectedDeleted:    []string{"test-isvc-decoder", "test-isvc-decoder-config", "test-isvc-decoder-ingress"},
			expectedNotDeleted: []string{"test-isvc-engine", "test-isvc-engine-config"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = log.IntoContext(ctx, log.Log)

			// Create scheme
			scheme := runtime.NewScheme()
			_ = v1beta1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)
			_ = autoscalingv2.AddToScheme(scheme)
			_ = networkingv1.AddToScheme(scheme)

			// Create fake client with existing resources
			allObjects := append([]client.Object{tc.isvc}, tc.existingResources...)
			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(allObjects...).
				Build()

			// Create reconciler
			r := &InferenceServiceReconciler{
				Client:    fakeClient,
				Clientset: fake.NewSimpleClientset(),
			}

			// Execute cleanup
			err := r.cleanupRemovedComponents(ctx, tc.isvc, tc.engineSpec, tc.decoderSpec, tc.routerSpec)
			require.NoError(t, err)

			// Verify expected deletions
			for _, name := range tc.expectedDeleted {
				// Check various resource types
				checkResourceDeleted(t, ctx, fakeClient, name, tc.isvc.Namespace)
			}

			// Verify expected resources still exist
			for _, name := range tc.expectedNotDeleted {
				checkResourceExists(t, ctx, fakeClient, name, tc.isvc.Namespace)
			}
		})
	}
}

// TestCleanupRemovedComponentsSweepsInferenceReplicas verifies the sweep
// requests deletion of a removed component's InferenceReplica (finalizer-
// drained teardown: DeletionTimestamp set, finalizer intact) while leaving
// active components' IRs untouched.
func TestCleanupRemovedComponentsSweepsInferenceReplicas(t *testing.T) {
	ctx := context.Background()
	ctx = log.IntoContext(ctx, log.Log)

	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			UID:       "test-uid",
		},
	}

	engineIR := createInferenceReplica("test-isvc-engine", "default", "test-isvc", "test-uid", v1beta1.EngineComponent)
	decoderIR := createInferenceReplica("test-isvc-decoder", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent)

	fakeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc, engineIR, decoderIR).
		Build()

	r := &InferenceServiceReconciler{
		Client:    fakeClient,
		Clientset: fake.NewSimpleClientset(),
	}

	// Decoder removed from the spec.
	err := r.cleanupRemovedComponents(ctx, isvc, &v1beta1.EngineSpec{}, nil, nil)
	require.NoError(t, err)

	// Decoder IR: deletion requested, finalizer still present (drained path).
	decoder := &v1beta1.InferenceReplica{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-isvc-decoder", Namespace: "default"}, decoder),
		"decoder IR should still be observable while the teardown finalizer drains it")
	assert.NotNil(t, decoder.DeletionTimestamp, "decoder IR should have deletion requested after component removal")
	assert.Contains(t, decoder.Finalizers, inferencereplica.TeardownFinalizer,
		"decoder IR should keep the teardown finalizer so pods drain before it vanishes")

	// Engine IR: untouched.
	engine := &v1beta1.InferenceReplica{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-isvc-engine", Namespace: "default"}, engine))
	assert.Nil(t, engine.DeletionTimestamp, "active engine IR must not be deleted")
}

// createInferenceReplica mirrors the projector's IR shape: name
// <isvc>-<component>, the sweep's selector labels, the ISVC controller
// owner-ref, and the IR controller's teardown finalizer.
func createInferenceReplica(name, namespace, isvcName, uid string, component v1beta1.ComponentType) *v1beta1.InferenceReplica {
	controller := true
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(component),
			},
			Finalizers: []string{inferencereplica.TeardownFinalizer},
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "InferenceService",
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Name:       isvcName,
					UID:        types.UID(uid),
					Controller: &controller,
				},
			},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: isvcName},
			Component: component,
		},
	}
}

func TestIsOwnedBy(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			UID:       "test-uid",
		},
	}

	testCases := []struct {
		name          string
		obj           *unstructured.Unstructured
		expectedOwned bool
	}{
		{
			name: "Object owned by InferenceService",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"ownerReferences": []interface{}{
							map[string]interface{}{
								"kind":       "InferenceService",
								"apiVersion": v1beta1.SchemeGroupVersion.String(),
								"name":       "test-isvc",
								"uid":        "test-uid",
							},
						},
					},
				},
			},
			expectedOwned: true,
		},
		{
			name: "Object with wrong UID",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"ownerReferences": []interface{}{
							map[string]interface{}{
								"kind":       "InferenceService",
								"apiVersion": v1beta1.SchemeGroupVersion.String(),
								"name":       "test-isvc",
								"uid":        "wrong-uid",
							},
						},
					},
				},
			},
			expectedOwned: false,
		},
		{
			name: "Object with wrong kind",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"ownerReferences": []interface{}{
							map[string]interface{}{
								"kind":       "Deployment",
								"apiVersion": "apps/v1",
								"name":       "test-isvc",
								"uid":        "test-uid",
							},
						},
					},
				},
			},
			expectedOwned: false,
		},
		{
			name: "Object with no owner references",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{},
				},
			},
			expectedOwned: false,
		},
	}

	r := &InferenceServiceReconciler{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			owned := r.isOwnedBy(tc.obj, isvc)
			assert.Equal(t, tc.expectedOwned, owned)
		})
	}
}

func TestGetAvailableResourceTypes(t *testing.T) {
	r := &InferenceServiceReconciler{
		ClientConfig: nil, // No config means only core resources
	}

	gvks, err := r.getAvailableResourceTypes()
	require.NoError(t, err)

	// Should contain at least the core resource types
	expectedCoreTypes := []schema.GroupVersionKind{
		{Group: "apps", Version: "v1", Kind: "Deployment"},
		{Group: "", Version: "v1", Kind: "Service"},
		{Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscaler"},
		{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"},
		{Group: "", Version: "v1", Kind: "ConfigMap"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "Role"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "RoleBinding"},
		{Group: "", Version: "v1", Kind: "ServiceAccount"},
		{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"},
		v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"),
	}

	for _, expected := range expectedCoreTypes {
		found := false
		for _, gvk := range gvks {
			if gvk == expected {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected GVK %v not found in available types", expected)
	}
}

func TestContains(t *testing.T) {
	testCases := []struct {
		name     string
		slice    []string
		item     string
		expected bool
	}{
		{
			name:     "Item exists in slice",
			slice:    []string{"list", "get", "delete"},
			item:     "delete",
			expected: true,
		},
		{
			name:     "Item does not exist in slice",
			slice:    []string{"list", "get"},
			item:     "delete",
			expected: false,
		},
		{
			name:     "Empty slice",
			slice:    []string{},
			item:     "delete",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := contains(tc.slice, tc.item)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Helper functions to create test resources

func createDeployment(name, namespace, isvcName, uid string, component v1beta1.ComponentType) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(component),
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "InferenceService",
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Name:       isvcName,
					UID:        types.UID(uid),
				},
			},
		},
	}
}

func createDeploymentWithoutOwner(name, namespace, isvcName string, component v1beta1.ComponentType) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(component),
			},
		},
	}
}

func createService(name, namespace, isvcName, uid string, component v1beta1.ComponentType) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(component),
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "InferenceService",
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Name:       isvcName,
					UID:        types.UID(uid),
				},
			},
		},
	}
}

func createConfigMap(name, namespace, isvcName, uid string, component v1beta1.ComponentType) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(component),
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "InferenceService",
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Name:       isvcName,
					UID:        types.UID(uid),
				},
			},
		},
	}
}

func createHPA(name, namespace, isvcName, uid string, component v1beta1.ComponentType) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(component),
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "InferenceService",
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Name:       isvcName,
					UID:        types.UID(uid),
				},
			},
		},
	}
}

func createIngress(name, namespace, isvcName, uid string, component v1beta1.ComponentType) *networkingv1.Ingress {
	return &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(component),
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "InferenceService",
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Name:       isvcName,
					UID:        types.UID(uid),
				},
			},
		},
	}
}

// Helper functions to check resource existence

func checkResourceDeleted(t *testing.T, ctx context.Context, client client.Client, name, namespace string) {
	// Try multiple resource types
	deployment := &appsv1.Deployment{}
	err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, deployment)
	if err == nil {
		t.Errorf("Expected deployment %s to be deleted, but it still exists", name)
		return
	}

	service := &corev1.Service{}
	err = client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, service)
	if err == nil {
		t.Errorf("Expected service %s to be deleted, but it still exists", name)
		return
	}

	configMap := &corev1.ConfigMap{}
	err = client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, configMap)
	if err == nil {
		t.Errorf("Expected configmap %s to be deleted, but it still exists", name)
		return
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	err = client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, hpa)
	if err == nil {
		t.Errorf("Expected HPA %s to be deleted, but it still exists", name)
		return
	}

	ingress := &networkingv1.Ingress{}
	err = client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, ingress)
	if err == nil {
		t.Errorf("Expected ingress %s to be deleted, but it still exists", name)
		return
	}
}

func checkResourceExists(t *testing.T, ctx context.Context, client client.Client, name, namespace string) {
	// Check if at least one resource type exists
	found := false

	deployment := &appsv1.Deployment{}
	if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, deployment); err == nil {
		found = true
	}

	service := &corev1.Service{}
	if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, service); err == nil {
		found = true
	}

	configMap := &corev1.ConfigMap{}
	if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, configMap); err == nil {
		found = true
	}

	if !found {
		t.Errorf("Expected resource %s to exist, but it was not found", name)
	}
}

func TestShouldKeepExternalService(t *testing.T) {
	testCases := []struct {
		name             string
		isvc             *v1beta1.InferenceService
		activeComponents map[v1beta1.ComponentType]bool
		expected         bool
		description      string
	}{
		{
			name: "Keep external service when annotation is set and engine is active",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
					Annotations: map[string]string{
						"ome.io/ingress-disable-creation": "true",
					},
				},
			},
			activeComponents: map[v1beta1.ComponentType]bool{
				v1beta1.EngineComponent: true,
			},
			expected:    true,
			description: "should keep external service when annotation is set and engine is active",
		},
		{
			name: "Keep external service when annotation is set and router is active",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
					Annotations: map[string]string{
						"ome.io/ingress-disable-creation": "true",
					},
				},
			},
			activeComponents: map[v1beta1.ComponentType]bool{
				v1beta1.RouterComponent: true,
			},
			expected:    true,
			description: "should keep external service when annotation is set and router is active",
		},
		{
			name: "Do not keep external service when annotation is not set",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
				},
			},
			activeComponents: map[v1beta1.ComponentType]bool{
				v1beta1.EngineComponent: true,
			},
			expected:    false,
			description: "should not keep external service when annotation is not set",
		},
		{
			name: "Do not keep external service when annotation is set to false",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
					Annotations: map[string]string{
						"ome.io/ingress-disable-creation": "false",
					},
				},
			},
			activeComponents: map[v1beta1.ComponentType]bool{
				v1beta1.EngineComponent: true,
			},
			expected:    false,
			description: "should not keep external service when annotation is set to false",
		},
		{
			name: "Do not keep external service when no active components even with annotation",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					UID:       "test-uid",
					Annotations: map[string]string{
						"ome.io/ingress-disable-creation": "true",
					},
				},
			},
			activeComponents: map[v1beta1.ComponentType]bool{},
			expected:         false,
			description:      "should not keep external service when no active components exist",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := &InferenceServiceReconciler{}
			result := r.shouldKeepExternalService(tc.isvc, tc.activeComponents)
			assert.Equal(t, tc.expected, result, tc.description)
		})
	}
}

func TestCleanupExternalServiceWithAnnotation(t *testing.T) {
	ctx := context.Background()
	ctx = log.IntoContext(ctx, log.Log)

	// Create scheme
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = autoscalingv2.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)

	// Test case: External service should NOT be deleted when annotation is set
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			UID:       "test-uid",
			Annotations: map[string]string{
				"ome.io/ingress-disable-creation": "true",
			},
		},
	}

	// Create external service with "external-service" component label
	externalService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "test-isvc",
				constants.OMEComponentLabel:           "external-service",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "InferenceService",
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Name:       "test-isvc",
					UID:        types.UID("test-uid"),
				},
			},
		},
	}

	// Create fake client with existing resources
	allObjects := []client.Object{isvc, externalService}
	fakeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(allObjects...).
		Build()

	// Create reconciler
	r := &InferenceServiceReconciler{
		Client:    fakeClient,
		Clientset: fake.NewSimpleClientset(),
	}

	// Execute cleanup with engine active
	engineSpec := &v1beta1.EngineSpec{}
	err := r.cleanupRemovedComponents(ctx, isvc, engineSpec, nil, nil)
	require.NoError(t, err)

	// Verify external service still exists
	service := &corev1.Service{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-isvc", Namespace: "default"}, service)
	assert.NoError(t, err, "External service should still exist after cleanup when annotation is set")
}

func TestCleanupExternalServiceWithoutAnnotation(t *testing.T) {
	ctx := context.Background()
	ctx = log.IntoContext(ctx, log.Log)

	// Create scheme
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = autoscalingv2.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)

	// Test case: External service SHOULD be deleted when annotation is NOT set
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			UID:       "test-uid",
			// No annotation set
		},
	}

	// Create external service with "external-service" component label
	externalService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "test-isvc",
				constants.OMEComponentLabel:           "external-service",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "InferenceService",
					APIVersion: v1beta1.SchemeGroupVersion.String(),
					Name:       "test-isvc",
					UID:        types.UID("test-uid"),
				},
			},
		},
	}

	// Create fake client with existing resources
	allObjects := []client.Object{isvc, externalService}
	fakeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(allObjects...).
		Build()

	// Create reconciler
	r := &InferenceServiceReconciler{
		Client:    fakeClient,
		Clientset: fake.NewSimpleClientset(),
	}

	// Execute cleanup with engine active
	engineSpec := &v1beta1.EngineSpec{}
	err := r.cleanupRemovedComponents(ctx, isvc, engineSpec, nil, nil)
	require.NoError(t, err)

	// Verify external service was deleted
	service := &corev1.Service{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-isvc", Namespace: "default"}, service)
	assert.Error(t, err, "External service should be deleted when annotation is not set")
}
