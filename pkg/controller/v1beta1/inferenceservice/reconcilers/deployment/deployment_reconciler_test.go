package deployment

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// mockClient implements client.Client interface for testing error conditions
type mockClient struct {
	client.Client
	shouldErrorOnGet    bool
	shouldErrorOnCreate bool
	shouldErrorOnUpdate bool
}

func (m *mockClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if m.shouldErrorOnGet {
		return fmt.Errorf("mock get error")
	}
	return m.Client.Get(ctx, key, obj, opts...)
}

func (m *mockClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if m.shouldErrorOnCreate {
		return fmt.Errorf("mock create error")
	}
	return m.Client.Create(ctx, obj, opts...)
}

func (m *mockClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if m.shouldErrorOnUpdate {
		return fmt.Errorf("mock update error")
	}
	return m.Client.Update(ctx, obj, opts...)
}

func TestSetDefaultPodSpec(t *testing.T) {
	tests := []struct {
		name       string
		podSpec    *corev1.PodSpec
		expected   *corev1.PodSpec
		containers int
	}{
		{
			name: "empty pod spec",
			podSpec: &corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "test"},
				},
			},
			expected: &corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:                     "test",
						TerminationMessagePath:   "/dev/termination-log",
						TerminationMessagePolicy: corev1.TerminationMessageReadFile,
						ImagePullPolicy:          corev1.PullIfNotPresent,
					},
				},
				DNSPolicy:                     corev1.DNSClusterFirst,
				RestartPolicy:                 corev1.RestartPolicyAlways,
				TerminationGracePeriodSeconds: ptr.Int64(30),
				SecurityContext:               &corev1.PodSecurityContext{},
				SchedulerName:                 corev1.DefaultSchedulerName,
			},
			containers: 1,
		},
		{
			name: "pod spec with some fields set",
			podSpec: &corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:                   "test",
						TerminationMessagePath: "/custom/path",
						Args:                   []string{"arg1", "arg2"},
					},
					{
						Name:            "test2",
						ImagePullPolicy: corev1.PullAlways,
					},
				},
				DNSPolicy:     corev1.DNSClusterFirstWithHostNet,
				SchedulerName: "custom-scheduler",
			},
			expected: &corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:                     "test",
						TerminationMessagePath:   "/custom/path",
						TerminationMessagePolicy: corev1.TerminationMessageReadFile,
						ImagePullPolicy:          corev1.PullIfNotPresent,
						Args:                     []string{"arg1", "arg2"},
					},
					{
						Name:                     "test2",
						TerminationMessagePath:   "/dev/termination-log",
						TerminationMessagePolicy: corev1.TerminationMessageReadFile,
						ImagePullPolicy:          corev1.PullAlways,
					},
				},
				DNSPolicy:                     corev1.DNSClusterFirstWithHostNet,
				RestartPolicy:                 corev1.RestartPolicyAlways,
				TerminationGracePeriodSeconds: ptr.Int64(30),
				SecurityContext:               &corev1.PodSecurityContext{},
				SchedulerName:                 "custom-scheduler",
			},
			containers: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setDefaultPodSpec(tt.podSpec)
			assert.Equal(t, tt.expected.DNSPolicy, tt.podSpec.DNSPolicy)
			assert.Equal(t, tt.expected.RestartPolicy, tt.podSpec.RestartPolicy)
			assert.Equal(t, tt.expected.TerminationGracePeriodSeconds, tt.podSpec.TerminationGracePeriodSeconds)
			assert.Equal(t, tt.expected.SecurityContext, tt.podSpec.SecurityContext)
			assert.Equal(t, tt.expected.SchedulerName, tt.podSpec.SchedulerName)
			assert.Equal(t, tt.containers, len(tt.podSpec.Containers))
			for i := 0; i < tt.containers; i++ {
				assert.Equal(t, tt.expected.Containers[i].TerminationMessagePath, tt.podSpec.Containers[i].TerminationMessagePath)
				assert.Equal(t, tt.expected.Containers[i].TerminationMessagePolicy, tt.podSpec.Containers[i].TerminationMessagePolicy)
				assert.Equal(t, tt.expected.Containers[i].ImagePullPolicy, tt.podSpec.Containers[i].ImagePullPolicy)
			}
		})
	}
}

func TestSetDefaultReadinessProbe(t *testing.T) {
	tests := []struct {
		name          string
		container     *corev1.Container
		expectProbe   bool
		expectedPort  int32
		existingProbe bool
	}{
		{
			name: "main container without ports",
			container: &corev1.Container{
				Name: constants.MainContainerName,
			},
			expectProbe:  true,
			expectedPort: 8080, // Default port
		},
		{
			name: "main container with custom port",
			container: &corev1.Container{
				Name: constants.MainContainerName,
				Ports: []corev1.ContainerPort{
					{ContainerPort: 9000},
				},
			},
			expectProbe:  true,
			expectedPort: 9000,
		},
		{
			name: "non-special container",
			container: &corev1.Container{
				Name: "some-other-container",
			},
			expectProbe: false,
		},
		{
			name: "container with existing probe",
			container: &corev1.Container{
				Name: constants.MainContainerName,
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/custom-health",
							Port: intstr.FromInt(9090),
						},
					},
				},
			},
			expectProbe:   true,
			existingProbe: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalProbe := tt.container.ReadinessProbe
			setDefaultReadinessProbe(tt.container)

			if tt.expectProbe {
				assert.NotNil(t, tt.container.ReadinessProbe)
				if tt.existingProbe {
					// Should not modify existing probe
					assert.Equal(t, originalProbe, tt.container.ReadinessProbe)
				} else {
					// Should create a new probe with default settings
					assert.NotNil(t, tt.container.ReadinessProbe.TCPSocket)
					assert.Equal(t, tt.expectedPort, tt.container.ReadinessProbe.TCPSocket.Port.IntVal)
					assert.Equal(t, int32(1), tt.container.ReadinessProbe.TimeoutSeconds)
					assert.Equal(t, int32(10), tt.container.ReadinessProbe.PeriodSeconds)
					assert.Equal(t, int32(1), tt.container.ReadinessProbe.SuccessThreshold)
					assert.Equal(t, int32(3), tt.container.ReadinessProbe.FailureThreshold)
				}
			} else {
				assert.Nil(t, tt.container.ReadinessProbe)
			}
		})
	}
}

func TestSetDefaultDeploymentSpec(t *testing.T) {
	tests := []struct {
		name          string
		spec          *appsv1.DeploymentSpec
		expectedType  appsv1.DeploymentStrategyType
		customRolling bool
	}{
		{
			name:         "empty spec",
			spec:         &appsv1.DeploymentSpec{},
			expectedType: appsv1.RollingUpdateDeploymentStrategyType,
		},
		{
			name: "recreate strategy",
			spec: &appsv1.DeploymentSpec{
				Strategy: appsv1.DeploymentStrategy{
					Type: appsv1.RecreateDeploymentStrategyType,
				},
			},
			expectedType: appsv1.RecreateDeploymentStrategyType,
		},
		{
			name: "rolling update with custom values",
			spec: &appsv1.DeploymentSpec{
				Strategy: appsv1.DeploymentStrategy{
					Type: appsv1.RollingUpdateDeploymentStrategyType,
					RollingUpdate: &appsv1.RollingUpdateDeployment{
						MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 2},
						MaxSurge:       &intstr.IntOrString{Type: intstr.Int, IntVal: 3},
					},
				},
			},
			expectedType:  appsv1.RollingUpdateDeploymentStrategyType,
			customRolling: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setDefaultDeploymentSpec(tt.spec)

			assert.Equal(t, tt.expectedType, tt.spec.Strategy.Type)

			if tt.spec.Strategy.Type == appsv1.RollingUpdateDeploymentStrategyType {
				assert.NotNil(t, tt.spec.Strategy.RollingUpdate)

				if !tt.customRolling {
					// Check default rolling update values
					assert.Equal(t, int32(0), tt.spec.Strategy.RollingUpdate.MaxUnavailable.IntVal)
					assert.Equal(t, int32(1), tt.spec.Strategy.RollingUpdate.MaxSurge.IntVal)
				}
			}

			// Check other default values
			assert.NotNil(t, tt.spec.RevisionHistoryLimit)
			assert.Equal(t, int32(10), *tt.spec.RevisionHistoryLimit)

			assert.NotNil(t, tt.spec.ProgressDeadlineSeconds)
			assert.Equal(t, int32(600), *tt.spec.ProgressDeadlineSeconds)
		})
	}
}

func TestCreateRawDeployment(t *testing.T) {
	testContainer := corev1.Container{
		Name:  "test-container",
		Image: "test-image:latest",
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}

	tests := []struct {
		name          string
		componentMeta metav1.ObjectMeta
		componentExt  *v1beta1.ComponentExtensionSpec
		hasStrategy   bool
	}{
		{
			name: "default deployment",
			componentMeta: metav1.ObjectMeta{
				Name:      "test-isvc",
				Namespace: "default",
				Labels: map[string]string{
					"app": "test-isvc",
				},
				Annotations: map[string]string{
					"annotation": "value",
				},
			},
			componentExt: &v1beta1.ComponentExtensionSpec{},
			hasStrategy:  false,
		},
		{
			name: "deployment with custom strategy",
			componentMeta: metav1.ObjectMeta{
				Name:      "test-isvc-custom",
				Namespace: "custom-namespace",
				Labels: map[string]string{
					"app": "test-isvc-custom",
				},
			},
			componentExt: &v1beta1.ComponentExtensionSpec{
				DeploymentStrategy: &appsv1.DeploymentStrategy{
					Type: appsv1.RecreateDeploymentStrategyType,
				},
			},
			hasStrategy: true,
		},
		{
			name: "DAC deployment",
			componentMeta: metav1.ObjectMeta{
				Name:      "test-isvc-dac",
				Namespace: "dac-namespace",
				Labels: map[string]string{
					"app": "test-isvc-dac",
				},
				Annotations: map[string]string{
					constants.DedicatedAICluster: "true",
				},
			},
			componentExt: &v1beta1.ComponentExtensionSpec{},
			hasStrategy:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := createRawDeployment(tt.componentMeta, tt.componentExt, podSpec.DeepCopy())

			// Check selector matches labels
			assert.Equal(t, constants.GetRawServiceLabel(tt.componentMeta.Name), deployment.Spec.Selector.MatchLabels["app"])
			assert.Equal(t, constants.GetRawServiceLabel(tt.componentMeta.Name), deployment.Spec.Template.Labels["app"])

			// Check strategy
			if tt.hasStrategy {
				assert.Equal(t, tt.componentExt.DeploymentStrategy.Type, deployment.Spec.Strategy.Type)
			} else {
				assert.Equal(t, appsv1.RollingUpdateDeploymentStrategyType, deployment.Spec.Strategy.Type)
			}

			assert.Equal(t, tt.componentMeta.Name, deployment.Name)

			// Check defaults set
			assert.NotNil(t, deployment.Spec.RevisionHistoryLimit)
			assert.NotNil(t, deployment.Spec.ProgressDeadlineSeconds)
		})
	}
}

func TestCreateRawDeploymentDoesNotShareLabels(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
		Labels: map[string]string{
			"existing": "label",
		},
		Annotations: map[string]string{
			constants.DedicatedAICluster: "dac-queue",
		},
	}
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "test-container"}},
	}

	deployment := createRawDeployment(componentMeta, &v1beta1.ComponentExtensionSpec{}, podSpec)

	// Pod template carries the stamped labels.
	assert.Equal(t, constants.GetRawServiceLabel(componentMeta.Name), deployment.Spec.Template.Labels["app"])
	assert.Equal(t, "dac-queue", deployment.Spec.Template.Labels[constants.VolcanoQueueName])

	// Pod-only labels must not leak into the caller's metadata or the
	// Deployment's object-level metadata.
	assert.NotContains(t, componentMeta.Labels, "app")
	assert.NotContains(t, componentMeta.Labels, constants.VolcanoQueueName)
	assert.NotContains(t, deployment.Labels, "app")
	assert.NotContains(t, deployment.Labels, constants.VolcanoQueueName)

	// Mutating the deployment's labels must not touch the caller's map.
	deployment.Labels["mutated"] = "true"
	assert.NotContains(t, componentMeta.Labels, "mutated")
}

func TestCreateRawDeploymentNilLabels(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
	}
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "test-container"}},
	}

	deployment := createRawDeployment(componentMeta, &v1beta1.ComponentExtensionSpec{}, podSpec)

	assert.Equal(t, constants.GetRawServiceLabel(componentMeta.Name), deployment.Spec.Template.Labels["app"])
	assert.Nil(t, componentMeta.Labels)
	assert.Nil(t, deployment.Labels)
}

func TestNewDeploymentReconciler(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	err := appsv1.AddToScheme(scheme)
	assert.NoError(t, err)

	// Create fake client
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	testContainer := corev1.Container{
		Name:  "test-container",
		Image: "test-image:latest",
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}

	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
		Labels: map[string]string{
			"app": "test-isvc",
		},
	}

	componentExt := &v1beta1.ComponentExtensionSpec{}

	reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

	// Verify the reconciler is properly initialized
	assert.NotNil(t, reconciler)
	assert.Equal(t, client, reconciler.client)
	assert.Equal(t, scheme, reconciler.scheme)
	assert.NotNil(t, reconciler.Deployment)
	assert.Equal(t, componentExt, reconciler.componentExt)

	// Verify the deployment is properly created
	assert.Equal(t, componentMeta.Name, reconciler.Deployment.Name)
	assert.Equal(t, componentMeta.Namespace, reconciler.Deployment.Namespace)
	assert.Equal(t, constants.GetRawServiceLabel(componentMeta.Name), reconciler.Deployment.Spec.Template.Labels["app"])
}

func TestCheckDeploymentExist(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	err := appsv1.AddToScheme(scheme)
	assert.NoError(t, err)

	testContainer := corev1.Container{
		Name:  "test-container",
		Image: "test-image:latest",
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}

	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
		Labels: map[string]string{
			"app": "test-isvc",
		},
	}

	componentExt := &v1beta1.ComponentExtensionSpec{}

	// 1. Test case: Deployment doesn't exist (should return CheckResultCreate)
	t.Run("Deployment doesn't exist", func(t *testing.T) {
		// Create a fake client without any existing deployment
		client := fake.NewClientBuilder().WithScheme(scheme).Build()

		// Create a reconciler
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Call checkDeploymentExist
		result, deployObj, err := reconciler.checkDeploymentExist()
		assert.NoError(t, err)
		assert.Equal(t, constants.CheckResultCreate, result)
		assert.Nil(t, deployObj)
	})

	// 2. Test case: Deployment exists but needs updates (should return CheckResultUpdate)
	t.Run("Deployment exists but needs updates", func(t *testing.T) {
		// Build the existing deployment from a copy so mutating it below does
		// not also mutate the reconciler's desired spec.
		existingDeployment := createRawDeployment(componentMeta, componentExt, podSpec.DeepCopy())
		// Modify it to be different
		existingDeployment.Spec.Template.Spec.Containers[0].Image = "different-image:latest"

		// Create a fake client with the existing deployment
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingDeployment).Build()

		// Create a reconciler with different specs
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Call checkDeploymentExist
		result, deployObj, err := reconciler.checkDeploymentExist()
		assert.NoError(t, err)
		assert.Equal(t, constants.CheckResultUpdate, result)
		assert.NotNil(t, deployObj)
		assert.Equal(t, existingDeployment.Name, deployObj.Name)
	})

	// 3. Test case: Deployment exists and matches desired state (should return CheckResultExisted)
	t.Run("Deployment exists and matches", func(t *testing.T) {
		// Create a fake client with the deployment
		client := fake.NewClientBuilder().WithScheme(scheme).Build()

		// Create a reconciler
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Create the deployment first so it exists
		err := client.Create(context.TODO(), reconciler.Deployment)
		assert.NoError(t, err)

		// Call checkDeploymentExist
		result, deployObj, err := reconciler.checkDeploymentExist()
		assert.NoError(t, err)
		assert.Equal(t, constants.CheckResultExisted, result)
		assert.NotNil(t, deployObj)
		assert.Equal(t, reconciler.Deployment.Name, deployObj.Name)
	})

	// 4. Test case: Error handling for client.Get failure
	t.Run("Get error", func(t *testing.T) {
		// Create a fake client with a custom client that will return error on Get
		client := &mockClient{
			Client:           fake.NewClientBuilder().WithScheme(scheme).Build(),
			shouldErrorOnGet: true,
		}

		// Create a reconciler
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Call checkDeploymentExist
		result, deployObj, err := reconciler.checkDeploymentExist()
		assert.Error(t, err)
		assert.Equal(t, constants.CheckResultUnknown, result)
		assert.Nil(t, deployObj)
		assert.Contains(t, err.Error(), "mock get error")
	})
}

func TestReconcile(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	err := appsv1.AddToScheme(scheme)
	assert.NoError(t, err)

	// Create test data
	testContainer := corev1.Container{
		Name:  "test-container",
		Image: "test-image:latest",
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}

	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
		Labels: map[string]string{
			"app": "test-isvc",
		},
	}

	componentExt := &v1beta1.ComponentExtensionSpec{}

	// 1. Test case: Create a new Deployment when it doesn't exist
	t.Run("Create new Deployment", func(t *testing.T) {
		// Create a fake client without any existing deployment
		client := fake.NewClientBuilder().WithScheme(scheme).Build()

		// Create a reconciler
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Verify Deployment was created
		verifyDeployment := &appsv1.Deployment{}
		err = client.Get(context.TODO(), types.NamespacedName{Name: componentMeta.Name, Namespace: componentMeta.Namespace}, verifyDeployment)
		assert.NoError(t, err)
		assert.Equal(t, componentMeta.Name, verifyDeployment.Name)
	})

	// 2. Test case: Update an existing Deployment when changes are detected
	t.Run("Update existing Deployment", func(t *testing.T) {
		// Create an existing deployment with different specs
		existingDeployment := createRawDeployment(componentMeta, componentExt, podSpec)
		// Modify it to be different
		existingDeployment.Spec.Template.Spec.Containers[0].Image = "different-image:latest"

		// Create a fake client with the existing deployment
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingDeployment).Build()

		// Create a reconciler with different specs - ensure the reconciler's deployment has the expected image
		newPodSpec := podSpec.DeepCopy()
		newPodSpec.Containers[0].Image = "test-image:latest"
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, newPodSpec, nil)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Verify Deployment was updated
		verifyDeployment := &appsv1.Deployment{}
		err = client.Get(context.TODO(), types.NamespacedName{Name: componentMeta.Name, Namespace: componentMeta.Namespace}, verifyDeployment)
		assert.NoError(t, err)
		assert.Equal(t, "test-image:latest", verifyDeployment.Spec.Template.Spec.Containers[0].Image)
	})

	// 3. Test case: No changes needed when Deployment already exists and matches
	t.Run("No changes needed", func(t *testing.T) {
		// Create a fake client
		client := fake.NewClientBuilder().WithScheme(scheme).Build()

		// Create a reconciler
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Create the deployment first
		err := client.Create(context.TODO(), reconciler.Deployment)
		assert.NoError(t, err)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	// 4. Test case: Error handling for client.Get failure
	t.Run("Get error", func(t *testing.T) {
		// Create a fake client with a custom client that will return error on Get
		client := &mockClient{
			Client:           fake.NewClientBuilder().WithScheme(scheme).Build(),
			shouldErrorOnGet: true,
		}

		// Create a reconciler
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "mock get error")
	})

	// 5. Test case: Error handling for client.Create failure
	t.Run("Create error", func(t *testing.T) {
		// Create a fake client with a custom client that will return error on Create
		client := &mockClient{
			Client:              fake.NewClientBuilder().WithScheme(scheme).Build(),
			shouldErrorOnCreate: true,
		}

		// Create a reconciler
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "mock create error")
	})

	// 6. Test case: Error handling for client.Update failure
	t.Run("Update error", func(t *testing.T) {
		// Create an existing deployment
		existingDeployment := createRawDeployment(componentMeta, componentExt, podSpec)
		existingDeployment.Spec.Template.Spec.Containers[0].Image = "different-image:latest"

		// Create a fake client with the existing deployment and that will return error on Update
		baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingDeployment).Build()
		client := &mockClient{
			Client:              baseClient,
			shouldErrorOnUpdate: true,
		}

		// Create a reconciler
		reconciler := NewDeploymentReconciler(client, scheme, componentMeta, componentExt, podSpec, nil)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "mock update error")
	})
}

func TestControllerOwnedReplicas(t *testing.T) {
	three := 3
	zero := 0
	tests := []struct {
		name       string
		autoscaler *v1beta1.ComponentAutoscaler
		ext        *v1beta1.ComponentExtensionSpec
		want       *int32
	}{
		{
			name:       "None class with explicit floor is controller-owned",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			ext:        &v1beta1.ComponentExtensionSpec{MinReplicas: &three},
			want:       ptr.Int32(3),
		},
		{
			name: "nil autoscaler resolves to None",
			ext:  &v1beta1.ComponentExtensionSpec{MinReplicas: &three},
			want: ptr.Int32(3),
		},
		{
			name:       "None class without a declared floor preserves the live count",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			ext:        &v1beta1.ComponentExtensionSpec{},
		},
		{
			name:       "None class with a non-positive floor preserves the live count",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			ext:        &v1beta1.ComponentExtensionSpec{MinReplicas: &zero},
		},
		{
			name:       "HPA class is scaler-owned",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			ext:        &v1beta1.ComponentExtensionSpec{MinReplicas: &three},
		},
		{
			name:       "KEDA class is scaler-owned",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA},
			ext:        &v1beta1.ComponentExtensionSpec{MinReplicas: &three},
		},
		{
			name:       "External class is scaler-owned",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			ext:        &v1beta1.ComponentExtensionSpec{MinReplicas: &three},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := controllerOwnedReplicas(tt.autoscaler, tt.ext)
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}
			assert.NotNil(t, got)
			assert.Equal(t, *tt.want, *got)
		})
	}
}

func TestReconcileReplicaOwnership(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, appsv1.AddToScheme(scheme))

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "test-container", Image: "test-image:latest"}},
	}
	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
	}
	minReplicas := 3

	t.Run("None class stamps minReplicas on create", func(t *testing.T) {
		componentExt := &v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas}
		cl := fake.NewClientBuilder().WithScheme(scheme).Build()
		reconciler := NewDeploymentReconciler(cl, scheme, componentMeta, componentExt, podSpec.DeepCopy(),
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone})

		_, err := reconciler.Reconcile()
		assert.NoError(t, err)

		stored := &appsv1.Deployment{}
		assert.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: componentMeta.Name, Namespace: componentMeta.Namespace}, stored))
		assert.NotNil(t, stored.Spec.Replicas)
		assert.Equal(t, int32(3), *stored.Spec.Replicas)
	})

	t.Run("None class re-stamps a drifted replica count", func(t *testing.T) {
		componentExt := &v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas}
		existing := createRawDeployment(componentMeta, componentExt, podSpec.DeepCopy())
		existing.Spec.Replicas = ptr.Int32(5)
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
		reconciler := NewDeploymentReconciler(cl, scheme, componentMeta, componentExt, podSpec.DeepCopy(),
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone})

		_, err := reconciler.Reconcile()
		assert.NoError(t, err)

		stored := &appsv1.Deployment{}
		assert.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: componentMeta.Name, Namespace: componentMeta.Namespace}, stored))
		assert.NotNil(t, stored.Spec.Replicas)
		assert.Equal(t, int32(3), *stored.Spec.Replicas)
	})

	t.Run("HPA class preserves a scaler-written replica count", func(t *testing.T) {
		componentExt := &v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas, MaxReplicas: 5}
		existing := createRawDeployment(componentMeta, componentExt, podSpec.DeepCopy())
		existing.Spec.Replicas = ptr.Int32(5)
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
		reconciler := NewDeploymentReconciler(cl, scheme, componentMeta, componentExt, podSpec.DeepCopy(),
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA})

		_, err := reconciler.Reconcile()
		assert.NoError(t, err)

		stored := &appsv1.Deployment{}
		assert.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: componentMeta.Name, Namespace: componentMeta.Namespace}, stored))
		assert.NotNil(t, stored.Spec.Replicas)
		assert.Equal(t, int32(5), *stored.Spec.Replicas)
	})
}

// A new Deployment starts at minReplicas whatever the class; no scaler has
// written a count before the Deployment exists.
func TestReconcile_CreateStartsAtMinReplicasForEveryClass(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, appsv1.AddToScheme(scheme))
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "test-container", Image: "test-image:latest"}}}
	componentMeta := metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"}
	minReplicas := 4

	for _, as := range []*v1beta1.ComponentAutoscaler{
		nil,
		{Class: v1beta1.AutoscalerNone},
		{Class: v1beta1.AutoscalerHPA},
		{Class: v1beta1.AutoscalerKEDA},
		{Class: v1beta1.AutoscalerExternal},
	} {
		name := "nil"
		if as != nil {
			name = string(as.Class)
		}
		t.Run(name, func(t *testing.T) {
			componentExt := &v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas, MaxReplicas: 8}
			cl := fake.NewClientBuilder().WithScheme(scheme).Build()
			reconciler := NewDeploymentReconciler(cl, scheme, componentMeta, componentExt, podSpec.DeepCopy(), as)

			_, err := reconciler.Reconcile()
			assert.NoError(t, err)

			stored := &appsv1.Deployment{}
			assert.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: componentMeta.Name, Namespace: componentMeta.Namespace}, stored))
			if assert.NotNil(t, stored.Spec.Replicas) {
				assert.Equal(t, int32(4), *stored.Spec.Replicas)
			}
		})
	}
}

// Without a positive minReplicas a new Deployment leaves spec.replicas unset
// and takes the apiserver default.
func TestReconcile_CreateWithoutPositiveMinReplicasLeavesReplicasUnset(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, appsv1.AddToScheme(scheme))
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "test-container", Image: "test-image:latest"}}}
	componentMeta := metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"}
	zero := 0

	for _, ext := range []*v1beta1.ComponentExtensionSpec{{}, {MinReplicas: &zero, MaxReplicas: 3}} {
		cl := fake.NewClientBuilder().WithScheme(scheme).Build()
		reconciler := NewDeploymentReconciler(cl, scheme, componentMeta, ext, podSpec.DeepCopy(),
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA})
		_, err := reconciler.Reconcile()
		assert.NoError(t, err)
		stored := &appsv1.Deployment{}
		assert.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: componentMeta.Name, Namespace: componentMeta.Namespace}, stored))
		assert.Nil(t, stored.Spec.Replicas)
	}
}

// After create, a scaler-owned class keeps the live count even when it sits
// below minReplicas; the scaler, not the controller, moves it.
func TestReconcile_ExternalClassUpdateKeepsLiveCountBelowMinReplicas(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, appsv1.AddToScheme(scheme))
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "test-container", Image: "test-image:latest"}}}
	componentMeta := metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"}
	minReplicas := 4
	componentExt := &v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas, MaxReplicas: 8}
	existing := createRawDeployment(componentMeta, componentExt, podSpec.DeepCopy())
	existing.Spec.Replicas = ptr.Int32(2)
	existing.Spec.Template.Spec.Containers[0].Image = "old-image:latest"
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	reconciler := NewDeploymentReconciler(cl, scheme, componentMeta, componentExt, podSpec.DeepCopy(),
		&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal})
	_, err := reconciler.Reconcile()
	assert.NoError(t, err)

	stored := &appsv1.Deployment{}
	assert.NoError(t, cl.Get(context.TODO(), types.NamespacedName{Name: componentMeta.Name, Namespace: componentMeta.Namespace}, stored))
	assert.Equal(t, "test-image:latest", stored.Spec.Template.Spec.Containers[0].Image)
	if assert.NotNil(t, stored.Spec.Replicas) {
		assert.Equal(t, int32(2), *stored.Spec.Replicas)
	}
}

// TestCreateRawDeploymentTemplateCarriesDeclaredAndOMEAnnotationsOnly shows
// which annotations of the component metadata reach the Deployment's pod
// template: a key the Component declares, OME's own keys and the pod-level
// prefixes do; a service annotation the Component does not declare stays on
// the Deployment's own metadata only.
func TestCreateRawDeploymentTemplateCarriesDeclaredAndOMEAnnotationsOnly(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "service-engine",
		Namespace: "team-a",
		Labels:    map[string]string{"app": "service-engine"},
		Annotations: map[string]string{
			"editor.example.com/write-counter": "1",
			"example.com/owner":                "team-a",
			"ome.io/local-queue":               "queue-a",
			"prometheus.io/scrape":             "true",
			"example.com/declared":             "component",
		},
	}
	componentExt := &v1beta1.ComponentExtensionSpec{Annotations: map[string]string{"example.com/declared": "component"}}
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "example.com/serving:v1"}}}

	deployment := createRawDeployment(componentMeta, componentExt, podSpec)

	got := deployment.Spec.Template.Annotations
	for key, want := range map[string]string{
		"ome.io/local-queue":   "queue-a",
		"prometheus.io/scrape": "true",
		"example.com/declared": "component",
	} {
		assert.Equal(t, want, got[key], "template annotation %s", key)
	}
	for _, key := range []string{"editor.example.com/write-counter", "example.com/owner"} {
		value, ok := got[key]
		assert.False(t, ok, "a service annotation the Component does not declare reached the pod template: %s=%q", key, value)
		assert.Equal(t, componentMeta.Annotations[key], deployment.Annotations[key], "the Deployment's own metadata keeps %s", key)
	}
	assert.Equal(t, "1", componentMeta.Annotations["editor.example.com/write-counter"], "the caller's metadata is left alone")
}

// TestReconcileServiceAnnotationValueWritesNoNewTemplate shows what a writer
// that rewrites a service annotation does to a RawDeployment: the second
// value builds the same pod template, the stored Deployment is not written
// (its resourceVersion stands) and no ReplicaSet rolls. A declared Component
// annotation still moves the template.
func TestReconcileServiceAnnotationValueWritesNoNewTemplate(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, appsv1.AddToScheme(scheme))
	const key = "editor.example.com/write-counter"
	componentMeta := func(value string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Name:        "service-engine",
			Namespace:   "team-a",
			Labels:      map[string]string{"app": "service-engine"},
			Annotations: map[string]string{key: value, "ome.io/local-queue": "queue-a"},
		}
	}
	componentExt := &v1beta1.ComponentExtensionSpec{}
	podSpec := func() *corev1.PodSpec {
		return &corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "example.com/serving:v1"}}}
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	deploymentKey := types.NamespacedName{Name: "service-engine", Namespace: "team-a"}

	_, err := NewDeploymentReconciler(cl, scheme, componentMeta("1"), componentExt, podSpec(), nil).Reconcile()
	assert.NoError(t, err)
	stored := &appsv1.Deployment{}
	assert.NoError(t, cl.Get(context.TODO(), deploymentKey, stored))
	firstVersion := stored.ResourceVersion
	_, carried := stored.Spec.Template.Annotations[key]
	assert.False(t, carried, "the service annotation must stay off the pod template")

	second := NewDeploymentReconciler(cl, scheme, componentMeta("2"), componentExt, podSpec(), nil)
	result, _, err := second.checkDeploymentExist()
	assert.NoError(t, err)
	assert.Equal(t, constants.CheckResultExisted, result, "the second value must build the same Deployment spec")
	_, err = second.Reconcile()
	assert.NoError(t, err)
	assert.NoError(t, cl.Get(context.TODO(), deploymentKey, stored))
	assert.Equal(t, firstVersion, stored.ResourceVersion, "the second value must not write the Deployment")
	_, carried = stored.Spec.Template.Annotations[key]
	assert.False(t, carried, "the second value must stay off the pod template")

	declared := &v1beta1.ComponentExtensionSpec{Annotations: map[string]string{"example.com/declared": "1"}}
	declaredMeta := componentMeta("2")
	declaredMeta.Annotations["example.com/declared"] = "1"
	_, err = NewDeploymentReconciler(cl, scheme, declaredMeta, declared, podSpec(), nil).Reconcile()
	assert.NoError(t, err)
	assert.NoError(t, cl.Get(context.TODO(), deploymentKey, stored))
	assert.NotEqual(t, firstVersion, stored.ResourceVersion, "a declared Component annotation writes the Deployment")
	assert.Equal(t, "1", stored.Spec.Template.Annotations["example.com/declared"], "a declared Component annotation reaches the pod template")
	_, carried = stored.Spec.Template.Annotations[key]
	assert.False(t, carried, "the service annotation stays off the template through a declared change")
}
