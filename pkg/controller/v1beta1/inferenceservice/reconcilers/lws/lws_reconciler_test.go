package lws

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

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

func TestNewLWSReconciler(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	err := lws.AddToScheme(scheme)
	assert.NoError(t, err)

	// Create fake client
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	testContainer := corev1.Container{
		Name:  "test-container",
		Image: "test-image:latest",
	}

	headPod := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}
	workerPod := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}

	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
		Labels: map[string]string{
			"app": "test-isvc",
		},
	}

	minReplicas := 2
	componentExt := &v1beta1.ComponentExtensionSpec{
		MinReplicas: &minReplicas,
	}

	reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

	// Verify the reconciler is properly initialized
	assert.NotNil(t, reconciler)
	assert.Equal(t, client, reconciler.client)
	assert.Equal(t, scheme, reconciler.scheme)
	assert.NotNil(t, reconciler.LWS)

	// Verify the LWS is properly created
	assert.Equal(t, constants.LWSName(componentMeta.Name), reconciler.LWS.Name)
	assert.Equal(t, componentMeta.Namespace, reconciler.LWS.Namespace)
	assert.Equal(t, int32(4), *reconciler.LWS.Spec.LeaderWorkerTemplate.Size) // workerSize(3) + 1
	assert.Equal(t, int32(2), *reconciler.LWS.Spec.Replicas)                  // From componentExt.MinReplicas
}

func TestReconcile(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	err := lws.AddToScheme(scheme)
	assert.NoError(t, err)

	// Create test data
	testContainer := corev1.Container{
		Name:  "test-container",
		Image: "test-image:latest",
	}

	headPod := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}
	workerPod := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}

	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
		Labels: map[string]string{
			"app": "test-isvc",
		},
	}

	minReplicas := 2
	componentExt := &v1beta1.ComponentExtensionSpec{
		MinReplicas: &minReplicas,
	}

	// 1. Test case: Create a new LWS when it doesn't exist
	t.Run("Create new LWS", func(t *testing.T) {
		// Create a fake client without any existing LWS
		client := fake.NewClientBuilder().WithScheme(scheme).Build()

		// Create a reconciler
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Verify LWS was created
		verifyLWS := &lws.LeaderWorkerSet{}
		err = client.Get(context.TODO(), types.NamespacedName{Name: constants.LWSName(componentMeta.Name), Namespace: componentMeta.Namespace}, verifyLWS)
		assert.NoError(t, err)
		assert.Equal(t, constants.LWSName(componentMeta.Name), verifyLWS.Name)
	})

	// 2. Test case: Update an existing LWS when changes are detected
	t.Run("Update existing LWS", func(t *testing.T) {
		// Create an existing LWS with different specs
		existingLWS := createLWS(headPod, workerPod, 2, componentExt, componentMeta)
		// Modify it to be different from what we'll create
		existingLWS.Spec.LeaderWorkerTemplate.Size = ptr.Int32(3) // Different from what we'll create

		// Create a fake client with the existing LWS
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingLWS).Build()

		// Create a reconciler with different specs
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Verify LWS was updated
		verifyLWS := &lws.LeaderWorkerSet{}
		err = client.Get(context.TODO(), types.NamespacedName{Name: constants.LWSName(componentMeta.Name), Namespace: componentMeta.Namespace}, verifyLWS)
		assert.NoError(t, err)
		assert.Equal(t, int32(4), *verifyLWS.Spec.LeaderWorkerTemplate.Size) // Should be updated to 4 (3+1)
	})

	// 3. Test case: No changes needed when LWS already exists and matches
	t.Run("No changes needed", func(t *testing.T) {
		// Create an existing LWS with identical specs
		existingLWS := createLWS(headPod, workerPod, 3, componentExt, componentMeta)

		// Create a fake client with the existing LWS
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingLWS).Build()

		// Create a reconciler with the same specs
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, existingLWS, result)
	})

	// 4. Test case: Error handling for client.Get failure
	t.Run("Get error", func(t *testing.T) {
		// Create a fake client with a custom client that will return error on Get
		client := &mockClient{
			Client:           fake.NewClientBuilder().WithScheme(scheme).Build(),
			shouldErrorOnGet: true,
		}

		// Create a reconciler
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

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
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

		// Call Reconcile
		result, err := reconciler.Reconcile()
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "mock create error")
	})
}

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

func TestCheckLeaderWorkerSetExist(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	err := lws.AddToScheme(scheme)
	assert.NoError(t, err)

	// Create test data
	testContainer := corev1.Container{
		Name:  "test-container",
		Image: "test-image:latest",
	}

	headPod := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}
	workerPod := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}

	componentMeta := metav1.ObjectMeta{
		Name:      "test-isvc",
		Namespace: "default",
		Labels: map[string]string{
			"app": "test-isvc",
		},
	}

	minReplicas := 2
	componentExt := &v1beta1.ComponentExtensionSpec{
		MinReplicas: &minReplicas,
	}

	// 1. Test case: LWS doesn't exist (should return CheckResultCreate)
	t.Run("LWS doesn't exist", func(t *testing.T) {
		// Create a fake client without any existing LWS
		client := fake.NewClientBuilder().WithScheme(scheme).Build()

		// Create a reconciler
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

		// Call checkLeaderWorkerSetExist
		result, lwsObj, err := reconciler.checkLeaderWorkerSetExist()
		assert.NoError(t, err)
		assert.Equal(t, constants.CheckResultCreate, result)
		assert.Nil(t, lwsObj)
	})

	// 2. Test case: LWS exists but needs updates (should return CheckResultUpdate)
	t.Run("LWS exists but needs updates", func(t *testing.T) {
		// Create an existing LWS with different specs
		existingLWS := createLWS(headPod, workerPod, 2, componentExt, componentMeta)
		// Modify it to be different from what we'll create
		existingLWS.Spec.LeaderWorkerTemplate.Size = ptr.Int32(3) // Different from what we'll create

		// Create a fake client with the existing LWS
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingLWS).Build()

		// Create a reconciler with different specs
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

		// Call checkLeaderWorkerSetExist
		result, lwsObj, err := reconciler.checkLeaderWorkerSetExist()
		assert.NoError(t, err)
		assert.Equal(t, constants.CheckResultUpdate, result)
		assert.NotNil(t, lwsObj)
		assert.Equal(t, existingLWS.Name, lwsObj.Name)
	})

	// 3. Test case: LWS exists and matches desired state (should return CheckResultExisted)
	t.Run("LWS exists and matches", func(t *testing.T) {
		// Create an existing LWS with identical specs
		existingLWS := createLWS(headPod, workerPod, 3, componentExt, componentMeta)

		// Create a fake client with the existing LWS
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingLWS).Build()

		// Create a reconciler with the same specs
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

		// Call checkLeaderWorkerSetExist
		result, lwsObj, err := reconciler.checkLeaderWorkerSetExist()
		assert.NoError(t, err)
		assert.Equal(t, constants.CheckResultExisted, result)
		assert.NotNil(t, lwsObj)
		assert.Equal(t, existingLWS.Name, lwsObj.Name)
	})

	// 4. Test case: Error handling for client.Get failure
	t.Run("Get error", func(t *testing.T) {
		// Create a fake client with a custom client that will return error on Get
		client := &mockClient{
			Client:           fake.NewClientBuilder().WithScheme(scheme).Build(),
			shouldErrorOnGet: true,
		}

		// Create a reconciler
		reconciler := NewLWSReconciler(client, scheme, headPod, workerPod, 3, componentExt, componentMeta)

		// Call checkLeaderWorkerSetExist
		result, lwsObj, err := reconciler.checkLeaderWorkerSetExist()
		assert.Error(t, err)
		assert.Equal(t, constants.CheckResultUnknown, result)
		assert.Nil(t, lwsObj)
		assert.Contains(t, err.Error(), "mock get error")
	})
}

func TestCreateLWS(t *testing.T) {
	type args struct {
		headPod       *corev1.PodSpec
		workerPod     *corev1.PodSpec
		workerSize    int32
		componentExt  *v1beta1.ComponentExtensionSpec
		componentMeta metav1.ObjectMeta
	}

	testContainer := corev1.Container{
		Name:  "test-container",
		Image: "test-image:latest",
	}

	defaultPodSpec := &corev1.PodSpec{
		Containers: []corev1.Container{testContainer},
	}

	minReplicas2 := 2
	minReplicas5 := 5

	testInput := map[string]args{
		"defaultLWS": {
			headPod:    defaultPodSpec.DeepCopy(),
			workerPod:  defaultPodSpec.DeepCopy(),
			workerSize: 2,
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: nil,
			},
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
		},
		"customMinReplicasLWS": {
			headPod:    defaultPodSpec.DeepCopy(),
			workerPod:  defaultPodSpec.DeepCopy(),
			workerSize: 3,
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: &minReplicas2,
			},
			componentMeta: metav1.ObjectMeta{
				Name:      "test-isvc-custom",
				Namespace: "custom-namespace",
				Labels: map[string]string{
					"app": "test-isvc-custom",
				},
			},
		},
		"withPrometheusAnnotations": {
			headPod:    defaultPodSpec.DeepCopy(),
			workerPod:  defaultPodSpec.DeepCopy(),
			workerSize: 4,
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: &minReplicas5,
			},
			componentMeta: metav1.ObjectMeta{
				Name:      "test-isvc-prometheus",
				Namespace: "monitoring",
				Labels: map[string]string{
					"app": "test-isvc-prometheus",
				},
				Annotations: map[string]string{
					constants.PrometheusPathAnnotationKey:   "/metrics",
					constants.PrometheusPortAnnotationKey:   "8080",
					constants.PrometheusScrapeAnnotationKey: "true",
				},
			},
		},
	}

	// Create expected outputs for each test case
	SubdomainShared := lws.SubdomainShared
	expectedLWSSpecs := map[string]*lws.LeaderWorkerSet{
		"defaultLWS": {
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.LWSName(testInput["defaultLWS"].componentMeta.Name),
				Namespace: testInput["defaultLWS"].componentMeta.Namespace,
				Labels:    testInput["defaultLWS"].componentMeta.Labels,
				Annotations: map[string]string{
					"annotation": "value",
				},
			},
			Spec: lws.LeaderWorkerSetSpec{
				Replicas:      ptr.Int32(1),
				StartupPolicy: lws.LeaderCreatedStartupPolicy,
				NetworkConfig: &lws.NetworkConfig{
					SubdomainPolicy: &SubdomainShared,
				},
				RolloutStrategy: lws.RolloutStrategy{
					Type: lws.RollingUpdateStrategyType,
					RollingUpdateConfiguration: &lws.RollingUpdateConfiguration{
						MaxUnavailable: intstr.IntOrString{Type: intstr.Int, IntVal: 1},
						MaxSurge:       intstr.IntOrString{Type: intstr.Int, IntVal: 1},
					},
				},
				LeaderWorkerTemplate: lws.LeaderWorkerTemplate{
					Size:          ptr.Int32(3), // workerSize + 1
					RestartPolicy: lws.RecreateGroupOnPodRestart,
					LeaderTemplate: &corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      testInput["defaultLWS"].componentMeta.Name,
							Namespace: testInput["defaultLWS"].componentMeta.Namespace,
							Labels: map[string]string{
								"app": "test-isvc",
							},
							// The templates take no annotation the Component does not
							// declare; the object metadata above keeps it.
							Annotations: map[string]string{},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:                     "test-container",
									Image:                    "test-image:latest",
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
					},
					WorkerTemplate: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      testInput["defaultLWS"].componentMeta.Name,
							Namespace: testInput["defaultLWS"].componentMeta.Namespace,
							Labels: map[string]string{
								"app": "test-isvc",
							},
							// The templates take no annotation the Component does not
							// declare; the object metadata above keeps it.
							Annotations: map[string]string{},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:                     "test-container",
									Image:                    "test-image:latest",
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
					},
				},
			},
		},
		"customMinReplicasLWS": {
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.LWSName(testInput["customMinReplicasLWS"].componentMeta.Name),
				Namespace: testInput["customMinReplicasLWS"].componentMeta.Namespace,
				Labels:    testInput["customMinReplicasLWS"].componentMeta.Labels,
				// Not initializing Annotations since createLWS will set nil for empty annotations
			},
			Spec: lws.LeaderWorkerSetSpec{
				Replicas:      ptr.Int32(2), // From componentExt.MinReplicas
				StartupPolicy: lws.LeaderCreatedStartupPolicy,
				NetworkConfig: &lws.NetworkConfig{
					SubdomainPolicy: &SubdomainShared,
				},
				RolloutStrategy: lws.RolloutStrategy{
					Type: lws.RollingUpdateStrategyType,
					RollingUpdateConfiguration: &lws.RollingUpdateConfiguration{
						MaxUnavailable: intstr.IntOrString{Type: intstr.Int, IntVal: 1},
						MaxSurge:       intstr.IntOrString{Type: intstr.Int, IntVal: 1},
					},
				},
				LeaderWorkerTemplate: lws.LeaderWorkerTemplate{
					Size:          ptr.Int32(4), // workerSize + 1
					RestartPolicy: lws.RecreateGroupOnPodRestart,
					LeaderTemplate: &corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      testInput["customMinReplicasLWS"].componentMeta.Name,
							Namespace: testInput["customMinReplicasLWS"].componentMeta.Namespace,
							Labels: map[string]string{
								"app": "test-isvc-custom",
							},
							// Not initializing Annotations since createLWS will set nil for empty annotations
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:                     "test-container",
									Image:                    "test-image:latest",
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
					},
					WorkerTemplate: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      testInput["customMinReplicasLWS"].componentMeta.Name,
							Namespace: testInput["customMinReplicasLWS"].componentMeta.Namespace,
							Labels: map[string]string{
								"app": "test-isvc-custom",
							},
							// Not initializing Annotations since createLWS will set nil for empty annotations
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:                     "test-container",
									Image:                    "test-image:latest",
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
					},
				},
			},
		},
		"withPrometheusAnnotations": {
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.LWSName(testInput["withPrometheusAnnotations"].componentMeta.Name),
				Namespace: testInput["withPrometheusAnnotations"].componentMeta.Namespace,
				Labels:    testInput["withPrometheusAnnotations"].componentMeta.Labels,
				Annotations: map[string]string{
					constants.PrometheusPathAnnotationKey:   "/metrics",
					constants.PrometheusPortAnnotationKey:   "8080",
					constants.PrometheusScrapeAnnotationKey: "true",
				},
			},
			Spec: lws.LeaderWorkerSetSpec{
				Replicas:      ptr.Int32(5), // From componentExt.MinReplicas
				StartupPolicy: lws.LeaderCreatedStartupPolicy,
				NetworkConfig: &lws.NetworkConfig{
					SubdomainPolicy: &SubdomainShared,
				},
				RolloutStrategy: lws.RolloutStrategy{
					Type: lws.RollingUpdateStrategyType,
					RollingUpdateConfiguration: &lws.RollingUpdateConfiguration{
						MaxUnavailable: intstr.IntOrString{Type: intstr.Int, IntVal: 1},
						MaxSurge:       intstr.IntOrString{Type: intstr.Int, IntVal: 1},
					},
				},
				LeaderWorkerTemplate: lws.LeaderWorkerTemplate{
					Size:          ptr.Int32(5), // workerSize + 1
					RestartPolicy: lws.RecreateGroupOnPodRestart,
					LeaderTemplate: &corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      testInput["withPrometheusAnnotations"].componentMeta.Name,
							Namespace: testInput["withPrometheusAnnotations"].componentMeta.Namespace,
							Labels: map[string]string{
								"app": "test-isvc-prometheus",
							},
							Annotations: map[string]string{
								constants.PrometheusPathAnnotationKey:   "/metrics",
								constants.PrometheusPortAnnotationKey:   "8080",
								constants.PrometheusScrapeAnnotationKey: "true",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:                     "test-container",
									Image:                    "test-image:latest",
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
					},
					WorkerTemplate: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:      testInput["withPrometheusAnnotations"].componentMeta.Name,
							Namespace: testInput["withPrometheusAnnotations"].componentMeta.Namespace,
							Labels: map[string]string{
								"app": "test-isvc-prometheus",
							},
							Annotations: map[string]string{},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:                     "test-container",
									Image:                    "test-image:latest",
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
					},
				},
			},
		},
	}

	tests := []struct {
		name     string
		args     args
		expected *lws.LeaderWorkerSet
	}{
		{
			name:     "default LWS configuration",
			args:     testInput["defaultLWS"],
			expected: expectedLWSSpecs["defaultLWS"],
		},
		{
			name:     "custom min replicas LWS configuration",
			args:     testInput["customMinReplicasLWS"],
			expected: expectedLWSSpecs["customMinReplicasLWS"],
		},
		{
			name:     "LWS with Prometheus annotations",
			args:     testInput["withPrometheusAnnotations"],
			expected: expectedLWSSpecs["withPrometheusAnnotations"],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createLWS(tt.args.headPod, tt.args.workerPod, tt.args.workerSize, tt.args.componentExt, tt.args.componentMeta)
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("Test %q unexpected LWS (-want +got): %v", tt.name, diff)
			}
		})
	}
}

// TestCreateLWSTemplatesCarryDeclaredAndOMEAnnotationsOnly shows which
// annotations of the component metadata reach the leader and worker pod
// templates: a key the Component declares, OME's own keys and the pod-level
// prefixes do; a service annotation the Component does not declare stays on
// the LeaderWorkerSet's own metadata only.
func TestCreateLWSTemplatesCarryDeclaredAndOMEAnnotationsOnly(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "service-engine",
		Namespace: "team-a",
		Labels:    map[string]string{"app": "service-engine"},
		Annotations: map[string]string{
			"editor.example.com/write-counter": "1",
			"ome.io/local-queue":               "queue-a",
			"k8s.grafana.com/scrape":           "true",
			"example.com/declared":             "component",
		},
	}
	componentExt := &v1beta1.ComponentExtensionSpec{Annotations: map[string]string{"example.com/declared": "component"}}
	podSpec := func() *corev1.PodSpec {
		return &corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "example.com/serving:v1"}}}
	}

	set := createLWS(podSpec(), podSpec(), 2, componentExt, componentMeta)

	for name, got := range map[string]map[string]string{
		"leader": set.Spec.LeaderWorkerTemplate.LeaderTemplate.Annotations,
		"worker": set.Spec.LeaderWorkerTemplate.WorkerTemplate.Annotations,
	} {
		for key, want := range map[string]string{
			"ome.io/local-queue":     "queue-a",
			"k8s.grafana.com/scrape": "true",
			"example.com/declared":   "component",
		} {
			assert.Equal(t, want, got[key], "%s template annotation %s", name, key)
		}
		value, ok := got["editor.example.com/write-counter"]
		assert.False(t, ok, "a service annotation the Component does not declare reached the %s template: %q", name, value)
	}
	assert.Equal(t, "1", set.Annotations["editor.example.com/write-counter"], "the LeaderWorkerSet's own metadata keeps the service annotation")
}
