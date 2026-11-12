package status

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	lwsspec "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestNewStatusReconciler(t *testing.T) {
	manager := NewStatusReconciler()

	assert.NotNil(t, manager)
}

func TestPropagateRawStatus(t *testing.T) {
	tests := []struct {
		name           string
		status         *v1beta1.InferenceServiceStatus
		component      v1beta1.ComponentType
		deployment     *appsv1.Deployment
		url            *apis.URL
		expectedStatus v1beta1.ComponentStatusSpec
	}{
		{
			name:      "successful deployment with available condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			deployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "default",
					Annotations: map[string]string{
						"deployment.kubernetes.io/revision": "1",
					},
				},
				Status: appsv1.DeploymentStatus{
					Replicas:           3,
					ReadyReplicas:      3,
					AvailableReplicas:  3,
					UpdatedReplicas:    3,
					ObservedGeneration: 1,
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:   appsv1.DeploymentAvailable,
							Status: corev1.ConditionTrue,
							Reason: "MinimumReplicasAvailable",
						},
						{
							Type:   appsv1.DeploymentProgressing,
							Status: corev1.ConditionTrue,
							Reason: "NewReplicaSetAvailable",
						},
					},
				},
			},
			url: &apis.URL{Scheme: "http", Host: "test-service.default.svc.cluster.local"},
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: &apis.URL{Scheme: "http", Host: "test-service.default.svc.cluster.local"},
			},
		},
		{
			name:      "engine deployment with available condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			deployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-engine-deployment",
					Namespace: "default",
					Annotations: map[string]string{
						"deployment.kubernetes.io/revision": "2",
					},
				},
				Status: appsv1.DeploymentStatus{
					Replicas:           2,
					ReadyReplicas:      2,
					AvailableReplicas:  2,
					UpdatedReplicas:    2,
					ObservedGeneration: 1,
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:   appsv1.DeploymentAvailable,
							Status: corev1.ConditionTrue,
							Reason: "MinimumReplicasAvailable",
						},
						{
							Type:   appsv1.DeploymentProgressing,
							Status: corev1.ConditionTrue,
							Reason: "NewReplicaSetAvailable",
						},
					},
				},
			},
			url: &apis.URL{Scheme: "http", Host: "test-engine-service.default.svc.cluster.local"},
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: &apis.URL{Scheme: "http", Host: "test-engine-service.default.svc.cluster.local"},
			},
		},
		{
			name:      "decoder deployment with progressing condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.DecoderComponent,
			deployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-decoder-deployment",
					Namespace: "default",
					Annotations: map[string]string{
						"deployment.kubernetes.io/revision": "3",
					},
				},
				Status: appsv1.DeploymentStatus{
					Replicas:           4,
					ReadyReplicas:      2,
					AvailableReplicas:  2,
					UpdatedReplicas:    3,
					ObservedGeneration: 2,
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:   appsv1.DeploymentProgressing,
							Status: corev1.ConditionTrue,
							Reason: "ReplicaSetUpdated",
						},
						{
							Type:   appsv1.DeploymentAvailable,
							Status: corev1.ConditionFalse,
							Reason: "MinimumReplicasUnavailable",
						},
					},
				},
			},
			url: nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: nil,
			},
		},
		{
			name:      "deployment with progressing condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			deployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "default",
					Annotations: map[string]string{
						"deployment.kubernetes.io/revision": "2",
					},
				},
				Status: appsv1.DeploymentStatus{
					Replicas:           3,
					ReadyReplicas:      1,
					AvailableReplicas:  1,
					UpdatedReplicas:    2,
					ObservedGeneration: 2,
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:   appsv1.DeploymentProgressing,
							Status: corev1.ConditionTrue,
							Reason: "ReplicaSetUpdated",
						},
						{
							Type:   appsv1.DeploymentAvailable,
							Status: corev1.ConditionFalse,
							Reason: "MinimumReplicasUnavailable",
						},
					},
				},
			},
			url: nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: nil,
			},
		},
		{
			name:      "deployment with replica failure",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			deployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-deployment",
					Namespace: "default",
					Annotations: map[string]string{
						"deployment.kubernetes.io/revision": "3",
					},
				},
				Status: appsv1.DeploymentStatus{
					Replicas:           3,
					ReadyReplicas:      0,
					AvailableReplicas:  0,
					UpdatedReplicas:    0,
					ObservedGeneration: 3,
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:    appsv1.DeploymentReplicaFailure,
							Status:  corev1.ConditionTrue,
							Reason:  "FailedCreate",
							Message: "pods \"test-pod-\" is forbidden: exceeded quota",
						},
					},
				},
			},
			url: nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.PropagateRawStatus(tt.status, tt.component, tt.deployment, tt.url)

			actualStatus := tt.status.Components[tt.component]
			assert.Equal(t, tt.expectedStatus.URL, actualStatus.URL)
			assert.Equal(t, tt.deployment.Status.ObservedGeneration, tt.status.ObservedGeneration)

			// Verify the correct condition was set based on component type
			var expectedCondition apis.ConditionType
			switch tt.component {
			case v1beta1.EngineComponent:
				expectedCondition = v1beta1.EngineReady
			case v1beta1.DecoderComponent:
				expectedCondition = v1beta1.DecoderReady
			}
			condition := tt.status.GetCondition(expectedCondition)

			// For deployments with Available condition, we expect a condition to be set
			// For deployments with only failure conditions, the condition might not be set
			hasAvailableCondition := false
			for _, deploymentCondition := range tt.deployment.Status.Conditions {
				if deploymentCondition.Type == appsv1.DeploymentAvailable {
					hasAvailableCondition = true
					break
				}
			}
			if hasAvailableCondition {
				assert.NotNil(t, condition)
			}
		})
	}
}

func TestPropagateMultiNodeStatus(t *testing.T) {
	tests := []struct {
		name           string
		status         *v1beta1.InferenceServiceStatus
		component      v1beta1.ComponentType
		lws            *lwsspec.LeaderWorkerSet
		url            *apis.URL
		expectedStatus v1beta1.ComponentStatusSpec
	}{
		{
			name:      "successful LeaderWorkerSet with ready condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			lws: &lwsspec.LeaderWorkerSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-lws",
					Namespace:  "default",
					Generation: 1,
					Annotations: map[string]string{
						"resourceVersion": "12345",
					},
				},
				Status: lwsspec.LeaderWorkerSetStatus{
					Replicas:        3,
					ReadyReplicas:   3,
					UpdatedReplicas: 3,
					Conditions: []metav1.Condition{
						{
							Type:   string(lwsspec.LeaderWorkerSetAvailable),
							Status: metav1.ConditionTrue,
							Reason: "AllReplicasReady",
						},
						{
							Type:   "Progressing",
							Status: metav1.ConditionTrue,
							Reason: "NewReplicaSetAvailable",
						},
					},
				},
			},
			url: &apis.URL{Scheme: "http", Host: "test-lws-service.default.svc.cluster.local"},
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: &apis.URL{Scheme: "http", Host: "test-lws-service.default.svc.cluster.local"},
			},
		},
		{
			name:      "engine LeaderWorkerSet with ready condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			lws: &lwsspec.LeaderWorkerSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-engine-lws",
					Namespace:  "default",
					Generation: 1,
					Annotations: map[string]string{
						"resourceVersion": "67890",
					},
				},
				Status: lwsspec.LeaderWorkerSetStatus{
					Replicas:        2,
					ReadyReplicas:   2,
					UpdatedReplicas: 2,
					Conditions: []metav1.Condition{
						{
							Type:   string(lwsspec.LeaderWorkerSetAvailable),
							Status: metav1.ConditionTrue,
							Reason: "AllReplicasReady",
						},
						{
							Type:   "Progressing",
							Status: metav1.ConditionTrue,
							Reason: "NewReplicaSetAvailable",
						},
					},
				},
			},
			url: &apis.URL{Scheme: "http", Host: "test-engine-lws-service.default.svc.cluster.local"},
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: &apis.URL{Scheme: "http", Host: "test-engine-lws-service.default.svc.cluster.local"},
			},
		},
		{
			name:      "decoder LeaderWorkerSet with progressing condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.DecoderComponent,
			lws: &lwsspec.LeaderWorkerSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-decoder-lws",
					Namespace:  "default",
					Generation: 2,
					Annotations: map[string]string{
						"resourceVersion": "54321",
					},
				},
				Status: lwsspec.LeaderWorkerSetStatus{
					Replicas:        4,
					ReadyReplicas:   2,
					UpdatedReplicas: 3,
					Conditions: []metav1.Condition{
						{
							Type:   "Progressing",
							Status: metav1.ConditionTrue,
							Reason: "ReplicaSetUpdated",
						},
						{
							Type:   string(lwsspec.LeaderWorkerSetAvailable),
							Status: metav1.ConditionFalse,
							Reason: "MinimumReplicasUnavailable",
						},
					},
				},
			},
			url: nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: nil,
			},
		},
		{
			name:      "LeaderWorkerSet with progressing condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			lws: &lwsspec.LeaderWorkerSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-lws",
					Namespace:  "default",
					Generation: 2,
					Annotations: map[string]string{
						"resourceVersion": "12346",
					},
				},
				Status: lwsspec.LeaderWorkerSetStatus{
					Replicas:        3,
					ReadyReplicas:   1,
					UpdatedReplicas: 2,
					Conditions: []metav1.Condition{
						{
							Type:   "Progressing",
							Status: metav1.ConditionTrue,
							Reason: "ReplicaSetUpdated",
						},
						{
							Type:   string(lwsspec.LeaderWorkerSetAvailable),
							Status: metav1.ConditionFalse,
							Reason: "MinimumReplicasUnavailable",
						},
					},
				},
			},
			url: nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				URL: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.PropagateMultiNodeStatus(tt.status, tt.component, tt.lws, tt.url)

			actualStatus := tt.status.Components[tt.component]
			assert.Equal(t, tt.expectedStatus.URL, actualStatus.URL)
			assert.Equal(t, tt.lws.Generation, tt.status.ObservedGeneration)

			// Verify the correct condition was set based on component type
			var expectedCondition apis.ConditionType
			switch tt.component {
			case v1beta1.EngineComponent:
				expectedCondition = v1beta1.EngineReady
			case v1beta1.DecoderComponent:
				expectedCondition = v1beta1.DecoderReady
			}
			condition := tt.status.GetCondition(expectedCondition)

			// For deployments with Available condition, we expect a condition to be set
			// For deployments with only failure conditions, the condition might not be set
			hasAvailableCondition := false
			for _, deploymentCondition := range tt.lws.Status.Conditions {
				if deploymentCondition.Type == string(lwsspec.LeaderWorkerSetAvailable) {
					hasAvailableCondition = true
					break
				}
			}
			if hasAvailableCondition {
				assert.NotNil(t, condition)
			}
		})
	}
}

func TestPropagateModelStatus(t *testing.T) {
	tests := []struct {
		name          string
		status        *v1beta1.InferenceServiceStatus
		podList       *corev1.PodList
		expectedState v1beta1.ModelState
	}{
		{
			name:          "no pods available",
			status:        &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			podList:       &corev1.PodList{Items: []corev1.Pod{}},
			expectedState: v1beta1.Pending,
		},
		{
			name: "pods available and service ready",
			status: &v1beta1.InferenceServiceStatus{
				Status: duckv1.Status{
					Conditions: duckv1.Conditions{
						{Type: v1beta1.EngineReady, Status: corev1.ConditionTrue},
					},
				},
				ModelStatus: v1beta1.ModelStatus{},
			},
			podList: &corev1.PodList{
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "test-pod-1"},
						Status: corev1.PodStatus{
							Phase: corev1.PodRunning,
							ContainerStatuses: []corev1.ContainerStatus{
								{
									Name:  "container-1",
									Ready: true,
									State: corev1.ContainerState{
										Running: &corev1.ContainerStateRunning{},
									},
								},
							},
						},
					},
				},
			},
			expectedState: v1beta1.Loaded,
		},
		{
			name: "service ready with a pod reporting no containers",
			status: &v1beta1.InferenceServiceStatus{
				Status: duckv1.Status{
					Conditions: duckv1.Conditions{
						{Type: v1beta1.EngineReady, Status: corev1.ConditionTrue},
					},
				},
				ModelStatus: v1beta1.ModelStatus{},
			},
			podList: &corev1.PodList{
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "test-pod-1"},
						Status: corev1.PodStatus{
							Phase: corev1.PodRunning,
						},
					},
				},
			},
			expectedState: v1beta1.Loaded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.PropagateModelStatus(tt.status, tt.podList)

			if tt.status.ModelStatus.ModelRevisionStates != nil {
				assert.Equal(t, tt.expectedState, tt.status.ModelStatus.ModelRevisionStates.TargetModelState)
			}
		})
	}
}

func TestUpdateModelRevisionStates(t *testing.T) {
	tests := []struct {
		name                     string
		status                   *v1beta1.InferenceServiceStatus
		modelState               v1beta1.ModelState
		totalCopies              int
		info                     *v1beta1.FailureInfo
		expectedTransitionStatus v1beta1.TransitionStatus
	}{
		{
			name:                     "update to loaded state",
			status:                   &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			modelState:               v1beta1.Loaded,
			totalCopies:              3,
			info:                     nil,
			expectedTransitionStatus: v1beta1.UpToDate,
		},
		{
			name:                     "update to pending state",
			status:                   &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			modelState:               v1beta1.Pending,
			totalCopies:              0,
			info:                     nil,
			expectedTransitionStatus: v1beta1.InProgress,
		},
		{
			name:                     "update to failed state",
			status:                   &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			modelState:               v1beta1.FailedToLoad,
			totalCopies:              0,
			info:                     &v1beta1.FailureInfo{Reason: v1beta1.ModelLoadFailed, Message: "Failed to load model"},
			expectedTransitionStatus: v1beta1.BlockedByFailedLoad,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.UpdateModelRevisionStates(tt.status, tt.modelState, tt.totalCopies, tt.info)

			assert.Equal(t, tt.expectedTransitionStatus, tt.status.ModelStatus.TransitionStatus)
			assert.Equal(t, tt.modelState, tt.status.ModelStatus.ModelRevisionStates.TargetModelState)
			if tt.info != nil {
				assert.Equal(t, tt.info, tt.status.ModelStatus.LastFailureInfo)
			}
		})
	}
}

func TestSetModelFailureInfo(t *testing.T) {
	tests := []struct {
		name            string
		status          *v1beta1.InferenceServiceStatus
		info            *v1beta1.FailureInfo
		expectedInfo    *v1beta1.FailureInfo
		expectedChanged bool
	}{
		{
			name:   "set new failure info",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			info: &v1beta1.FailureInfo{
				Reason:  v1beta1.ModelLoadFailed,
				Message: "Model failed to load",
			},
			expectedInfo: &v1beta1.FailureInfo{
				Reason:  v1beta1.ModelLoadFailed,
				Message: "Model failed to load",
			},
			expectedChanged: true,
		},
		{
			name: "set same failure info",
			status: &v1beta1.InferenceServiceStatus{
				ModelStatus: v1beta1.ModelStatus{
					LastFailureInfo: &v1beta1.FailureInfo{
						Reason:  v1beta1.ModelLoadFailed,
						Message: "Model failed to load",
					},
				},
			},
			info: &v1beta1.FailureInfo{
				Reason:  v1beta1.ModelLoadFailed,
				Message: "Model failed to load",
			},
			expectedInfo: &v1beta1.FailureInfo{
				Reason:  v1beta1.ModelLoadFailed,
				Message: "Model failed to load",
			},
			expectedChanged: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			changed := manager.SetModelFailureInfo(tt.status, tt.info)

			assert.Equal(t, tt.expectedChanged, changed)
			assert.Equal(t, tt.expectedInfo, tt.status.ModelStatus.LastFailureInfo)
		})
	}
}

// The component URL is recorded only while the workload is Available; a
// workload that is not keeps the URL it had.
func TestPropagate_URLRecordedOnlyWhenAvailable(t *testing.T) {
	url := &apis.URL{Scheme: "http", Host: "engine.default.svc.cluster.local"}
	deploymentAvailable := func(status corev1.ConditionStatus) *appsv1.Deployment {
		return &appsv1.Deployment{Status: appsv1.DeploymentStatus{
			Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: status}},
		}}
	}
	lwsAvailable := func(status metav1.ConditionStatus) *lwsspec.LeaderWorkerSet {
		return &lwsspec.LeaderWorkerSet{Status: lwsspec.LeaderWorkerSetStatus{
			Conditions: []metav1.Condition{{Type: string(lwsspec.LeaderWorkerSetAvailable), Status: status}},
		}}
	}

	t.Run("deployment", func(t *testing.T) {
		status := &v1beta1.InferenceServiceStatus{}
		NewStatusReconciler().PropagateRawStatus(status, v1beta1.EngineComponent, deploymentAvailable(corev1.ConditionFalse), url)
		assert.Nil(t, status.Components[v1beta1.EngineComponent].URL, "no URL while the deployment is not available")

		NewStatusReconciler().PropagateRawStatus(status, v1beta1.EngineComponent, deploymentAvailable(corev1.ConditionTrue), url)
		assert.Equal(t, url, status.Components[v1beta1.EngineComponent].URL)
	})

	t.Run("leaderworkerset", func(t *testing.T) {
		status := &v1beta1.InferenceServiceStatus{}
		NewStatusReconciler().PropagateMultiNodeStatus(status, v1beta1.EngineComponent, lwsAvailable(metav1.ConditionFalse), url)
		assert.Nil(t, status.Components[v1beta1.EngineComponent].URL, "no URL while the set is not available")

		NewStatusReconciler().PropagateMultiNodeStatus(status, v1beta1.EngineComponent, lwsAvailable(metav1.ConditionTrue), url)
		assert.Equal(t, url, status.Components[v1beta1.EngineComponent].URL)
	})
}

// A component's ready condition is rewritten to Initializing only when the
// status holds one that is False; a True, Unknown or missing condition stays
// as it is.
func TestInitializeComponentCondition(t *testing.T) {
	tests := []struct {
		name           string
		existing       *apis.Condition
		expectedStatus corev1.ConditionStatus
		expectedReason string
	}{
		{name: "false", existing: &apis.Condition{Status: corev1.ConditionFalse, Reason: "Failed"}, expectedStatus: corev1.ConditionFalse, expectedReason: "Initializing"},
		{name: "unknown", existing: &apis.Condition{Status: corev1.ConditionUnknown, Reason: "Pending"}, expectedStatus: corev1.ConditionUnknown, expectedReason: "Pending"},
		{name: "true", existing: &apis.Condition{Status: corev1.ConditionTrue, Reason: "Ready"}, expectedStatus: corev1.ConditionTrue, expectedReason: "Ready"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := &v1beta1.InferenceServiceStatus{}
			status.SetCondition(v1beta1.EngineReady, tt.existing)

			NewStatusReconciler().InitializeComponentCondition(status, v1beta1.EngineComponent)

			condition := status.GetCondition(v1beta1.EngineReady)
			assert.NotNil(t, condition)
			assert.Equal(t, tt.expectedStatus, condition.Status)
			assert.Equal(t, tt.expectedReason, condition.Reason)
		})
	}

	t.Run("missing", func(t *testing.T) {
		status := &v1beta1.InferenceServiceStatus{}
		NewStatusReconciler().InitializeComponentCondition(status, v1beta1.EngineComponent)
		assert.Nil(t, status.GetCondition(v1beta1.EngineReady), "a missing condition is not written")
	})
}
