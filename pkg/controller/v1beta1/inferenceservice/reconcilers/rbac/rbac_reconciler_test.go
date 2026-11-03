package rbac

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

const (
	testNamespace        = "test-namespace"
	testInferenceService = "test-inference-service"
	testServiceName      = "test-service"
)

func TestNewRBACReconciler(t *testing.T) {
	fakeClient := fakeclient.New(t)
	scheme := fakeClient.Scheme()
	objectMeta := metav1.ObjectMeta{
		Name:      testServiceName,
		Namespace: testNamespace,
		Labels: map[string]string{
			"app": "test-app",
		},
	}

	inferenceService := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testInferenceService,
			Namespace: testNamespace,
			UID:       "test-uid",
		},
	}

	tests := []struct {
		name             string
		client           client.Client
		scheme           *runtime.Scheme
		objectMeta       metav1.ObjectMeta
		componentType    v1beta1.ComponentType
		inferenceService *v1beta1.InferenceService
		expectError      bool
	}{
		{
			name:             "valid inputs",
			client:           fakeClient,
			scheme:           scheme,
			objectMeta:       objectMeta,
			componentType:    v1beta1.RouterComponent,
			inferenceService: inferenceService,
			expectError:      false,
		},
		{
			name:             "valid with different component type",
			client:           fakeClient,
			scheme:           scheme,
			objectMeta:       objectMeta,
			componentType:    v1beta1.EngineComponent,
			inferenceService: inferenceService,
			expectError:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := NewRBACReconciler(
				tt.client,
				tt.scheme,
				tt.objectMeta,
				tt.componentType,
				tt.inferenceService,
			)

			assert.NotNil(t, reconciler)
			assert.Equal(t, tt.client, reconciler.client)
			assert.Equal(t, tt.scheme, reconciler.scheme)
			assert.Equal(t, tt.objectMeta, reconciler.objectMeta)
			assert.Equal(t, tt.componentType, reconciler.componentType)
			assert.Equal(t, tt.inferenceService, reconciler.inferenceService)
			assert.NotNil(t, reconciler.Log)
		})
	}
}

func TestRBACReconciler_GetServiceAccountName(t *testing.T) {
	tests := []struct {
		name             string
		inferenceService *v1beta1.InferenceService
		componentType    v1beta1.ComponentType
		expected         string
	}{
		{
			name: "router component",
			inferenceService: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "my-service"},
			},
			componentType: v1beta1.RouterComponent,
			expected:      "my-service-router",
		},
		{
			name: "decoder component",
			inferenceService: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test-svc"},
			},
			componentType: v1beta1.DecoderComponent,
			expected:      "test-svc-decoder",
		},
		{
			name: "engine component",
			inferenceService: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "engine-svc"},
			},
			componentType: v1beta1.EngineComponent,
			expected:      "engine-svc-engine",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := &RBACReconciler{
				inferenceService: tt.inferenceService,
				componentType:    tt.componentType,
			}

			result := reconciler.GetServiceAccountName()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRBACReconciler_Reconcile_RouterComponent(t *testing.T) {
	ownerRef := metav1.OwnerReference{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceService",
		Name:       testInferenceService,
		UID:        "test-uid",
	}

	inferenceService := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testInferenceService,
			Namespace: testNamespace,
			UID:       "test-uid",
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ome.io/v1beta1",
			Kind:       "InferenceService",
		},
	}

	tests := []struct {
		name            string
		ownerReferences []metav1.OwnerReference
	}{
		{
			name:            "with existing owner references",
			ownerReferences: []metav1.OwnerReference{ownerRef},
		},
		{
			name:            "without owner references - use inferenceService as owner",
			ownerReferences: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objectMeta := metav1.ObjectMeta{
				Name:            testServiceName,
				Namespace:       testNamespace,
				Labels:          map[string]string{"app": "test-app"},
				OwnerReferences: tt.ownerReferences,
			}

			fakeClient := fakeclient.New(t)
			reconciler := NewRBACReconciler(
				fakeClient,
				fakeClient.Scheme(),
				objectMeta,
				v1beta1.RouterComponent,
				inferenceService,
			)

			// Test successful reconciliation
			err := reconciler.Reconcile(context.Background())
			require.NoError(t, err)

			expectedServiceAccountName := "test-inference-service-router"

			// Verify ServiceAccount was created
			sa := &corev1.ServiceAccount{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      expectedServiceAccountName,
				Namespace: testNamespace,
			}, sa)
			require.NoError(t, err)
			assert.Equal(t, expectedServiceAccountName, sa.Name)
			assert.Equal(t, testNamespace, sa.Namespace)
			assert.Equal(t, objectMeta.Labels, sa.Labels)

			// Verify OwnerReference was set correctly (either from objectMeta or from inferenceService)
			require.Len(t, sa.OwnerReferences, 1)
			assert.Equal(t, "ome.io/v1beta1", sa.OwnerReferences[0].APIVersion)
			assert.Equal(t, "InferenceService", sa.OwnerReferences[0].Kind)
			assert.Equal(t, testInferenceService, sa.OwnerReferences[0].Name)
			assert.Equal(t, types.UID("test-uid"), sa.OwnerReferences[0].UID)

			// Verify Role was created
			role := &rbacv1.Role{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      expectedServiceAccountName,
				Namespace: testNamespace,
			}, role)
			require.NoError(t, err)
			assert.Equal(t, expectedServiceAccountName, role.Name)
			assert.Equal(t, testNamespace, role.Namespace)
			assert.Equal(t, objectMeta.Labels, role.Labels)

			// Verify Role OwnerReference
			require.Len(t, role.OwnerReferences, 1)
			assert.Equal(t, "ome.io/v1beta1", role.OwnerReferences[0].APIVersion)
			assert.Equal(t, "InferenceService", role.OwnerReferences[0].Kind)
			assert.Equal(t, testInferenceService, role.OwnerReferences[0].Name)
			assert.Equal(t, types.UID("test-uid"), role.OwnerReferences[0].UID)

			// Verify Role rules
			expectedRules := []rbacv1.PolicyRule{
				{
					APIGroups: []string{""},
					Resources: []string{"pods"},
					Verbs:     []string{"get", "list", "watch"},
				},
			}
			assert.Equal(t, expectedRules, role.Rules)

			// Verify RoleBinding was created
			rb := &rbacv1.RoleBinding{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      expectedServiceAccountName,
				Namespace: testNamespace,
			}, rb)
			require.NoError(t, err)
			assert.Equal(t, expectedServiceAccountName, rb.Name)
			assert.Equal(t, testNamespace, rb.Namespace)
			assert.Equal(t, objectMeta.Labels, rb.Labels)

			// Verify RoleBinding OwnerReference
			require.Len(t, rb.OwnerReferences, 1)
			assert.Equal(t, "ome.io/v1beta1", rb.OwnerReferences[0].APIVersion)
			assert.Equal(t, "InferenceService", rb.OwnerReferences[0].Kind)
			assert.Equal(t, testInferenceService, rb.OwnerReferences[0].Name)
			assert.Equal(t, types.UID("test-uid"), rb.OwnerReferences[0].UID)

			// Verify RoleBinding subjects and roleRef
			expectedSubjects := []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      expectedServiceAccountName,
					Namespace: testNamespace,
				},
			}
			assert.Equal(t, expectedSubjects, rb.Subjects)

			expectedRoleRef := rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     expectedServiceAccountName,
			}
			assert.Equal(t, expectedRoleRef, rb.RoleRef)
		})
	}
}

func TestRBACReconciler_Reconcile_NonRouterComponent(t *testing.T) {
	objectMeta := metav1.ObjectMeta{
		Name:      testServiceName,
		Namespace: testNamespace,
		Labels: map[string]string{
			"app": "test-app",
		},
	}

	inferenceService := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testInferenceService,
			Namespace: testNamespace,
			UID:       "test-uid",
		},
	}

	fakeClient := fakeclient.New(t)
	reconciler := NewRBACReconciler(
		fakeClient,
		fakeClient.Scheme(),
		objectMeta,
		v1beta1.EngineComponent, // Non-router component
		inferenceService,
	)

	// Test successful reconciliation
	err := reconciler.Reconcile(context.Background())
	require.NoError(t, err)

	expectedServiceAccountName := "test-inference-service-engine"

	// Verify ServiceAccount was created
	sa := &corev1.ServiceAccount{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      expectedServiceAccountName,
		Namespace: testNamespace,
	}, sa)
	require.NoError(t, err)

	// Verify Role was NOT created for non-router component
	role := &rbacv1.Role{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      expectedServiceAccountName,
		Namespace: testNamespace,
	}, role)
	assert.True(t, apierrors.IsNotFound(err), "Role should not exist for non-router component")

	// Verify RoleBinding was NOT created for non-router component
	rb := &rbacv1.RoleBinding{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      expectedServiceAccountName,
		Namespace: testNamespace,
	}, rb)
	assert.True(t, apierrors.IsNotFound(err), "RoleBinding should not exist for non-router component")
}

func TestRBACReconciler_Reconcile_Update(t *testing.T) {
	objectMeta := metav1.ObjectMeta{
		Name:      testServiceName,
		Namespace: testNamespace,
		Labels: map[string]string{
			"app": "test-app",
		},
	}

	inferenceService := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testInferenceService,
			Namespace: testNamespace,
			UID:       "test-uid",
		},
	}

	expectedServiceAccountName := "test-inference-service-router"

	// Pre-create resources with different labels
	existingSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      expectedServiceAccountName,
			Namespace: testNamespace,
			Labels: map[string]string{
				"old-label": "old-value",
			},
		},
	}

	fakeClient := fakeclient.New(t, fakeclient.WithObjects(existingSA))

	reconciler := NewRBACReconciler(
		fakeClient,
		fakeClient.Scheme(),
		objectMeta,
		v1beta1.RouterComponent,
		inferenceService,
	)

	// Test reconciliation updates existing resources
	err := reconciler.Reconcile(context.Background())
	require.NoError(t, err)

	// Verify ServiceAccount was updated with new labels
	sa := &corev1.ServiceAccount{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      expectedServiceAccountName,
		Namespace: testNamespace,
	}, sa)
	require.NoError(t, err)
	assert.Equal(t, objectMeta.Labels, sa.Labels)
}

func TestRBACReconciler_createOrUpdate(t *testing.T) {
	fakeClient := fakeclient.New(t)
	reconciler := &RBACReconciler{
		client: fakeClient,
		Log:    logr.Discard(),
	}

	// Test creating new resource
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sa",
			Namespace: testNamespace,
		},
	}

	err := reconciler.createOrUpdate(context.Background(), sa)
	require.NoError(t, err)

	// Verify resource was created
	createdSA := &corev1.ServiceAccount{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "test-sa",
		Namespace: testNamespace,
	}, createdSA)
	require.NoError(t, err)
	assert.Equal(t, "test-sa", createdSA.Name)

	// Test updating existing resource
	sa.Labels = map[string]string{"updated": "true"}
	err = reconciler.createOrUpdate(context.Background(), sa)
	require.NoError(t, err)

	// Verify resource was updated
	updatedSA := &corev1.ServiceAccount{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "test-sa",
		Namespace: testNamespace,
	}, updatedSA)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"updated": "true"}, updatedSA.Labels)
}

func TestRBACReconciler_createOrUpdate_Error(t *testing.T) {
	// Create a failing client that returns errors
	fakeClient := fakeclient.New(t)
	reconciler := &RBACReconciler{
		client: fakeClient,
		Log:    logr.Discard(),
	}

	// Test with invalid resource (missing required fields)
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: "", // Invalid empty name
		},
	}

	err := reconciler.createOrUpdate(context.Background(), sa)
	assert.Error(t, err)
}

// TestRBACReconciler_createOrUpdate_NoOpWhenUnchanged verifies that a steady
// object (live == desired) does not trigger an Update PUT.
func TestRBACReconciler_createOrUpdate_NoOpWhenUnchanged(t *testing.T) {
	labels := map[string]string{"app": "test-app"}
	owner := []metav1.OwnerReference{{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceService",
		Name:       testInferenceService,
		UID:        "test-uid",
	}}

	cases := []struct {
		name    string
		desired client.Object
		live    client.Object
	}{
		{
			name: "ServiceAccount unchanged",
			desired: &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNamespace, Labels: labels, OwnerReferences: owner},
			},
			live: &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNamespace, Labels: labels, OwnerReferences: owner},
			},
		},
		{
			name: "Role unchanged",
			desired: &rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{Name: "role", Namespace: testNamespace, Labels: labels, OwnerReferences: owner},
				Rules: []rbacv1.PolicyRule{{
					APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"},
				}},
			},
			live: &rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{Name: "role", Namespace: testNamespace, Labels: labels, OwnerReferences: owner},
				Rules: []rbacv1.PolicyRule{{
					APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"},
				}},
			},
		},
		{
			name: "RoleBinding unchanged",
			desired: &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "rb", Namespace: testNamespace, Labels: labels, OwnerReferences: owner},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "role"},
				Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sa", Namespace: testNamespace}},
			},
			live: &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "rb", Namespace: testNamespace, Labels: labels, OwnerReferences: owner},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "role"},
				Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sa", Namespace: testNamespace}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updates := fakeclient.CountingVerb(fakeclient.Update)
			reconciler := &RBACReconciler{client: fakeclient.New(t, fakeclient.WithObjects(tc.live), updates), Log: logr.Discard()}

			require.NoError(t, reconciler.createOrUpdate(context.Background(), tc.desired))
			assert.Equal(t, 0, updates.Count(), "steady %s must not issue an Update PUT", tc.name)
		})
	}
}

// TestRBACReconciler_createOrUpdate_UpdatesOnDrift verifies that a real
// difference in a managed field does trigger exactly one Update.
func TestRBACReconciler_createOrUpdate_UpdatesOnDrift(t *testing.T) {
	cases := []struct {
		name    string
		desired client.Object
		live    client.Object
	}{
		{
			name: "ServiceAccount label drift",
			desired: &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNamespace, Labels: map[string]string{"app": "new"}},
			},
			live: &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNamespace, Labels: map[string]string{"app": "old"}},
			},
		},
		{
			name: "Role rules drift",
			desired: &rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{Name: "role", Namespace: testNamespace},
				Rules: []rbacv1.PolicyRule{{
					APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"},
				}},
			},
			live: &rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{Name: "role", Namespace: testNamespace},
				Rules: []rbacv1.PolicyRule{{
					APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get"},
				}},
			},
		},
		{
			name: "RoleBinding subjects drift",
			desired: &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "rb", Namespace: testNamespace},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "role"},
				Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "new-sa", Namespace: testNamespace}},
			},
			live: &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "rb", Namespace: testNamespace},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "role"},
				Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "old-sa", Namespace: testNamespace}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updates := fakeclient.CountingVerb(fakeclient.Update)
			reconciler := &RBACReconciler{client: fakeclient.New(t, fakeclient.WithObjects(tc.live), updates), Log: logr.Discard()}

			require.NoError(t, reconciler.createOrUpdate(context.Background(), tc.desired))
			assert.Equal(t, 1, updates.Count(), "drifted %s must issue exactly one Update PUT", tc.name)
		})
	}
}

// Benchmark tests
func BenchmarkRBACReconciler_Reconcile(b *testing.B) {
	objectMeta := metav1.ObjectMeta{
		Name:      testServiceName,
		Namespace: testNamespace,
		Labels: map[string]string{
			"app": "test-app",
		},
	}

	inferenceService := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testInferenceService,
			Namespace: testNamespace,
			UID:       "test-uid",
		},
	}

	fakeClient := fakeclient.New(b)
	reconciler := NewRBACReconciler(
		fakeClient,
		fakeClient.Scheme(),
		objectMeta,
		v1beta1.RouterComponent,
		inferenceService,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := reconciler.Reconcile(context.Background())
		require.NoError(b, err)
	}
}
