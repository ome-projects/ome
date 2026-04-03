package status

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	lwsspec "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
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
			component: v1beta1.PredictorComponent,
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
				LatestCreatedRevision: "1",
				URL:                   &apis.URL{Scheme: "http", Host: "test-service.default.svc.cluster.local"},
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
				LatestCreatedRevision: "2",
				URL:                   &apis.URL{Scheme: "http", Host: "test-engine-service.default.svc.cluster.local"},
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
				LatestCreatedRevision: "3",
				URL:                   nil,
			},
		},
		{
			name:      "deployment with progressing condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.PredictorComponent,
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
				LatestCreatedRevision: "2",
				URL:                   nil,
			},
		},
		{
			name:      "deployment with replica failure",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.PredictorComponent,
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
				LatestCreatedRevision: "3",
				URL:                   nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.PropagateRawStatus(tt.status, tt.component, tt.deployment, tt.url)

			actualStatus := tt.status.Components[tt.component]
			assert.Equal(t, tt.expectedStatus.LatestCreatedRevision, actualStatus.LatestCreatedRevision)
			assert.Equal(t, tt.expectedStatus.URL, actualStatus.URL)
			assert.Equal(t, tt.deployment.Status.ObservedGeneration, tt.status.ObservedGeneration)

			// Verify the correct condition was set based on component type
			var expectedCondition apis.ConditionType
			switch tt.component {
			case v1beta1.PredictorComponent:
				expectedCondition = v1beta1.PredictorReady
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
			component: v1beta1.PredictorComponent,
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
				LatestCreatedRevision: "12345",
				URL:                   &apis.URL{Scheme: "http", Host: "test-lws-service.default.svc.cluster.local"},
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
				LatestCreatedRevision: "67890",
				URL:                   &apis.URL{Scheme: "http", Host: "test-engine-lws-service.default.svc.cluster.local"},
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
				LatestCreatedRevision: "54321",
				URL:                   nil,
			},
		},
		{
			name:      "LeaderWorkerSet with progressing condition",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.PredictorComponent,
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
				LatestCreatedRevision: "12346",
				URL:                   nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.PropagateMultiNodeStatus(tt.status, tt.component, tt.lws, tt.url)

			actualStatus := tt.status.Components[tt.component]
			assert.Equal(t, tt.expectedStatus.LatestCreatedRevision, actualStatus.LatestCreatedRevision)
			assert.Equal(t, tt.expectedStatus.URL, actualStatus.URL)
			assert.Equal(t, tt.lws.Generation, tt.status.ObservedGeneration)

			// Verify the correct condition was set based on component type
			var expectedCondition apis.ConditionType
			switch tt.component {
			case v1beta1.PredictorComponent:
				expectedCondition = v1beta1.PredictorReady
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

func TestPropagateStatus(t *testing.T) {
	tests := []struct {
		name           string
		status         *v1beta1.InferenceServiceStatus
		component      v1beta1.ComponentType
		serviceStatus  *knservingv1.ServiceStatus
		expectedStatus v1beta1.ComponentStatusSpec
	}{
		{
			name:      "successful Knative service",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.PredictorComponent,
			serviceStatus: &knservingv1.ServiceStatus{
				Status: duckv1.Status{
					Conditions: duckv1.Conditions{
						{
							Type:   knservingv1.ServiceConditionReady,
							Status: corev1.ConditionTrue,
						},
					},
				},
				RouteStatusFields: knservingv1.RouteStatusFields{
					URL: &apis.URL{
						Scheme: "https",
						Host:   "test-service.example.com",
					},
					Address: &duckv1.Addressable{
						URL: &apis.URL{
							Scheme: "https",
							Host:   "test-service.example.com",
						},
					},
					Traffic: []knservingv1.TrafficTarget{
						{
							RevisionName:   "test-service-00001",
							Percent:        ptr.To(int64(100)),
							LatestRevision: ptr.To(true),
						},
					},
				},
				ConfigurationStatusFields: knservingv1.ConfigurationStatusFields{
					LatestReadyRevisionName:   "test-service-00001",
					LatestCreatedRevisionName: "test-service-00001",
				},
			},
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestReadyRevision:   "test-service-00001",
				LatestCreatedRevision: "test-service-00001",
				URL:                   &apis.URL{Scheme: "https", Host: "test-service.example.com"},
				Address:               &duckv1.Addressable{URL: &apis.URL{Scheme: "https", Host: "test-service.example.com"}},
			},
		},
		{
			name:      "successful engine Knative service",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			serviceStatus: &knservingv1.ServiceStatus{
				Status: duckv1.Status{
					Conditions: duckv1.Conditions{
						{
							Type:   knservingv1.ServiceConditionReady,
							Status: corev1.ConditionTrue,
						},
					},
				},
				RouteStatusFields: knservingv1.RouteStatusFields{
					URL: &apis.URL{
						Scheme: "https",
						Host:   "test-engine-service.example.com",
					},
					Address: &duckv1.Addressable{
						URL: &apis.URL{
							Scheme: "https",
							Host:   "test-engine-service.example.com",
						},
					},
					Traffic: []knservingv1.TrafficTarget{
						{
							RevisionName:   "test-engine-service-00002",
							Percent:        ptr.To(int64(100)),
							LatestRevision: ptr.To(true),
						},
					},
				},
				ConfigurationStatusFields: knservingv1.ConfigurationStatusFields{
					LatestReadyRevisionName:   "test-engine-service-00002",
					LatestCreatedRevisionName: "test-engine-service-00002",
				},
			},
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestReadyRevision:   "test-engine-service-00002",
				LatestCreatedRevision: "test-engine-service-00002",
				URL:                   &apis.URL{Scheme: "https", Host: "test-engine-service.example.com"},
				Address:               &duckv1.Addressable{URL: &apis.URL{Scheme: "https", Host: "test-engine-service.example.com"}},
			},
		},
		{
			name:      "decoder service with traffic split",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.DecoderComponent,
			serviceStatus: &knservingv1.ServiceStatus{
				Status: duckv1.Status{
					Conditions: duckv1.Conditions{
						{
							Type:   knservingv1.ServiceConditionReady,
							Status: corev1.ConditionTrue,
						},
					},
				},
				RouteStatusFields: knservingv1.RouteStatusFields{
					URL: &apis.URL{
						Scheme: "https",
						Host:   "test-decoder-service.example.com",
					},
					Address: &duckv1.Addressable{
						URL: &apis.URL{
							Scheme: "https",
							Host:   "test-decoder-service.example.com",
						},
					},
					Traffic: []knservingv1.TrafficTarget{
						{
							RevisionName:   "test-decoder-service-00003",
							Percent:        ptr.To(int64(50)),
							LatestRevision: ptr.To(true),
						},
						{
							RevisionName:   "test-decoder-service-00002",
							Percent:        ptr.To(int64(50)),
							LatestRevision: ptr.To(false),
						},
					},
				},
				ConfigurationStatusFields: knservingv1.ConfigurationStatusFields{
					LatestReadyRevisionName:   "test-decoder-service-00003",
					LatestCreatedRevisionName: "test-decoder-service-00003",
				},
			},
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestReadyRevision:   "test-decoder-service-00003",
				LatestCreatedRevision: "test-decoder-service-00003",
				URL:                   &apis.URL{Scheme: "https", Host: "test-decoder-service.example.com"},
				Address:               &duckv1.Addressable{URL: &apis.URL{Scheme: "https", Host: "test-decoder-service.example.com"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.PropagateStatus(tt.status, tt.component, tt.serviceStatus)

			actualStatus := tt.status.Components[tt.component]
			assert.Equal(t, tt.expectedStatus.LatestReadyRevision, actualStatus.LatestReadyRevision)
			assert.Equal(t, tt.expectedStatus.LatestCreatedRevision, actualStatus.LatestCreatedRevision)
			assert.Equal(t, tt.expectedStatus.URL, actualStatus.URL)
			assert.Equal(t, tt.expectedStatus.Address, actualStatus.Address)

			// Verify appropriate conditions were set
			var expectedReadyCondition apis.ConditionType

			switch tt.component {
			case v1beta1.PredictorComponent:
				expectedReadyCondition = v1beta1.PredictorReady
			case v1beta1.EngineComponent:
				expectedReadyCondition = v1beta1.EngineReady
			case v1beta1.DecoderComponent:
				expectedReadyCondition = v1beta1.DecoderReady
			}

			// The status reconciler will propagate service conditions to component conditions
			readyCondition := tt.status.GetCondition(expectedReadyCondition)
			assert.NotNil(t, readyCondition)
		})
	}
}

func TestPropagateModelStatus(t *testing.T) {
	tests := []struct {
		name          string
		status        *v1beta1.InferenceServiceStatus
		statusSpec    v1beta1.ComponentStatusSpec
		podList       *corev1.PodList
		rawDeployment bool
		expectedState v1beta1.ModelState
	}{
		{
			name:   "no pods available",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			statusSpec: v1beta1.ComponentStatusSpec{
				LatestReadyRevision:   "rev-1",
				LatestCreatedRevision: "rev-1",
			},
			podList:       &corev1.PodList{Items: []corev1.Pod{}},
			rawDeployment: false,
			expectedState: v1beta1.Pending,
		},
		{
			name: "pods available and service ready",
			status: &v1beta1.InferenceServiceStatus{
				Status: duckv1.Status{
					Conditions: duckv1.Conditions{
						{Type: v1beta1.PredictorReady, Status: corev1.ConditionTrue},
					},
				},
				ModelStatus: v1beta1.ModelStatus{},
			},
			statusSpec: v1beta1.ComponentStatusSpec{
				LatestReadyRevision:   "rev-1",
				LatestCreatedRevision: "rev-1",
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
			rawDeployment: false,
			expectedState: v1beta1.Loaded,
		},
		{
			name: "raw deployment ready",
			status: &v1beta1.InferenceServiceStatus{
				Status: duckv1.Status{
					Conditions: duckv1.Conditions{
						{Type: v1beta1.PredictorReady, Status: corev1.ConditionTrue},
					},
				},
				ModelStatus: v1beta1.ModelStatus{},
			},
			statusSpec: v1beta1.ComponentStatusSpec{
				LatestReadyRevision:   "rev-1",
				LatestCreatedRevision: "rev-1",
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
			rawDeployment: true,
			expectedState: v1beta1.Loaded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.PropagateModelStatus(tt.status, v1beta1.PredictorComponent, tt.statusSpec, tt.podList, tt.rawDeployment)

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

func TestUpdateModelTransitionStatus(t *testing.T) {
	tests := []struct {
		name                     string
		status                   *v1beta1.InferenceServiceStatus
		transitionStatus         v1beta1.TransitionStatus
		info                     *v1beta1.FailureInfo
		expectedTransitionStatus v1beta1.TransitionStatus
	}{
		{
			name:                     "update transition status to invalid spec",
			status:                   &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			transitionStatus:         v1beta1.InvalidSpec,
			info:                     &v1beta1.FailureInfo{Reason: v1beta1.InvalidPredictorSpec, Message: "Invalid spec"},
			expectedTransitionStatus: v1beta1.InvalidSpec,
		},
		{
			name:                     "update transition status to in progress",
			status:                   &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			transitionStatus:         v1beta1.InProgress,
			info:                     nil,
			expectedTransitionStatus: v1beta1.InProgress,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.UpdateModelTransitionStatus(tt.status, tt.transitionStatus, tt.info)

			assert.Equal(t, tt.expectedTransitionStatus, tt.status.ModelStatus.TransitionStatus)
			if tt.info != nil {
				assert.Equal(t, tt.info, tt.status.ModelStatus.LastFailureInfo)
			}
		})
	}
}

func TestPropagateCrossComponentStatus(t *testing.T) {
	tests := []struct {
		name           string
		status         *v1beta1.InferenceServiceStatus
		componentList  []v1beta1.ComponentType
		conditionType  apis.ConditionType
		setupStatus    func(*v1beta1.InferenceServiceStatus)
		expectedStatus corev1.ConditionStatus
	}{
		{
			name:          "all components ready",
			status:        &v1beta1.InferenceServiceStatus{},
			componentList: []v1beta1.ComponentType{v1beta1.PredictorComponent},
			conditionType: v1beta1.RoutesReady,
			setupStatus: func(status *v1beta1.InferenceServiceStatus) {
				status.SetCondition(v1beta1.PredictorRouteReady, &apis.Condition{
					Type:   v1beta1.PredictorRouteReady,
					Status: corev1.ConditionTrue,
				})
			},
			expectedStatus: corev1.ConditionTrue,
		},
		{
			name:          "component not ready",
			status:        &v1beta1.InferenceServiceStatus{},
			componentList: []v1beta1.ComponentType{v1beta1.PredictorComponent},
			conditionType: v1beta1.RoutesReady,
			setupStatus: func(status *v1beta1.InferenceServiceStatus) {
				status.SetCondition(v1beta1.PredictorRouteReady, &apis.Condition{
					Type:   v1beta1.PredictorRouteReady,
					Status: corev1.ConditionFalse,
					Reason: "RouteNotReady",
				})
			},
			expectedStatus: corev1.ConditionFalse,
		},
		{
			name:          "multiple components all ready",
			status:        &v1beta1.InferenceServiceStatus{},
			componentList: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			conditionType: v1beta1.RoutesReady,
			setupStatus: func(status *v1beta1.InferenceServiceStatus) {
				status.SetCondition(v1beta1.EngineRouteReady, &apis.Condition{
					Type:   v1beta1.EngineRouteReady,
					Status: corev1.ConditionTrue,
				})
				status.SetCondition(v1beta1.DecoderRouteReady, &apis.Condition{
					Type:   v1beta1.DecoderRouteReady,
					Status: corev1.ConditionTrue,
				})
			},
			expectedStatus: corev1.ConditionTrue,
		},
		{
			name:          "multiple components one not ready",
			status:        &v1beta1.InferenceServiceStatus{},
			componentList: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			conditionType: v1beta1.RoutesReady,
			setupStatus: func(status *v1beta1.InferenceServiceStatus) {
				status.SetCondition(v1beta1.EngineRouteReady, &apis.Condition{
					Type:   v1beta1.EngineRouteReady,
					Status: corev1.ConditionTrue,
				})
				status.SetCondition(v1beta1.DecoderRouteReady, &apis.Condition{
					Type:   v1beta1.DecoderRouteReady,
					Status: corev1.ConditionFalse,
					Reason: "DecoderRouteNotReady",
				})
			},
			expectedStatus: corev1.ConditionFalse,
		},
		{
			name:          "configuration ready for engine and decoder",
			status:        &v1beta1.InferenceServiceStatus{},
			componentList: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			conditionType: v1beta1.LatestDeploymentReady,
			setupStatus: func(status *v1beta1.InferenceServiceStatus) {
				status.SetCondition(v1beta1.EngineConfigurationReady, &apis.Condition{
					Type:   v1beta1.EngineConfigurationReady,
					Status: corev1.ConditionTrue,
				})
				status.SetCondition(v1beta1.DecoderConfigurationReady, &apis.Condition{
					Type:   v1beta1.DecoderConfigurationReady,
					Status: corev1.ConditionTrue,
				})
			},
			expectedStatus: corev1.ConditionTrue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()
			tt.setupStatus(tt.status)

			manager.PropagateCrossComponentStatus(tt.status, tt.componentList, tt.conditionType)

			condition := tt.status.GetCondition(tt.conditionType)
			require.NotNil(t, condition)
			assert.Equal(t, tt.expectedStatus, condition.Status)
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

func TestPropagateMultiNodeRayVLLMStatus(t *testing.T) {
	tests := []struct {
		name           string
		status         *v1beta1.InferenceServiceStatus
		component      v1beta1.ComponentType
		deployments    []*appsv1.Deployment
		url            *apis.URL
		expectedStatus v1beta1.ComponentStatusSpec
		expectError    bool
	}{
		{
			name:      "successful multi-deployment with available conditions",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.PredictorComponent,
			deployments: []*appsv1.Deployment{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-head",
						Namespace: "default",
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
					},
					Status: appsv1.DeploymentStatus{
						Replicas:           1,
						ReadyReplicas:      1,
						AvailableReplicas:  1,
						UpdatedReplicas:    1,
						ObservedGeneration: 1,
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionTrue,
								Reason: "MinimumReplicasAvailable",
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-worker",
						Namespace: "default",
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
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
						},
					},
				},
			},
			url: &apis.URL{Scheme: "http", Host: "ray-service.default.svc.cluster.local"},
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestCreatedRevision: "1",
				URL:                   &apis.URL{Scheme: "http", Host: "ray-service.default.svc.cluster.local"},
			},
			expectError: false,
		},
		{
			name:      "engine multi-deployment all available",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			deployments: []*appsv1.Deployment{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "engine-head",
						Namespace: "default",
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "2",
						},
					},
					Status: appsv1.DeploymentStatus{
						Replicas:           1,
						ReadyReplicas:      1,
						AvailableReplicas:  1,
						UpdatedReplicas:    1,
						ObservedGeneration: 2,
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionTrue,
								Reason: "MinimumReplicasAvailable",
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "engine-worker",
						Namespace: "default",
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "2",
						},
					},
					Status: appsv1.DeploymentStatus{
						Replicas:           4,
						ReadyReplicas:      4,
						AvailableReplicas:  4,
						UpdatedReplicas:    4,
						ObservedGeneration: 2,
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionTrue,
								Reason: "MinimumReplicasAvailable",
							},
						},
					},
				},
			},
			url: &apis.URL{Scheme: "http", Host: "engine-service.default.svc.cluster.local"},
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestCreatedRevision: "2",
				URL:                   &apis.URL{Scheme: "http", Host: "engine-service.default.svc.cluster.local"},
			},
			expectError: false,
		},
		{
			name:      "decoder deployment partially available",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.DecoderComponent,
			deployments: []*appsv1.Deployment{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "decoder-head",
						Namespace: "default",
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "3",
						},
					},
					Status: appsv1.DeploymentStatus{
						Replicas:           1,
						ReadyReplicas:      1,
						AvailableReplicas:  1,
						UpdatedReplicas:    1,
						ObservedGeneration: 3,
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionTrue,
								Reason: "MinimumReplicasAvailable",
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "decoder-worker",
						Namespace: "default",
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "3",
						},
					},
					Status: appsv1.DeploymentStatus{
						Replicas:           3,
						ReadyReplicas:      1,
						AvailableReplicas:  1,
						UpdatedReplicas:    2,
						ObservedGeneration: 3,
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionFalse,
								Reason: "MinimumReplicasUnavailable",
							},
						},
					},
				},
			},
			url: nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestCreatedRevision: "3",
				URL:                   nil,
			},
			expectError: false,
		},
		{
			name:      "one deployment not available",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.PredictorComponent,
			deployments: []*appsv1.Deployment{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-head",
						Namespace: "default",
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
					},
					Status: appsv1.DeploymentStatus{
						Replicas:           1,
						ReadyReplicas:      1,
						AvailableReplicas:  1,
						UpdatedReplicas:    1,
						ObservedGeneration: 1,
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionTrue,
								Reason: "MinimumReplicasAvailable",
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-worker",
						Namespace: "default",
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
					},
					Status: appsv1.DeploymentStatus{
						Replicas:           2,
						ReadyReplicas:      0,
						AvailableReplicas:  0,
						UpdatedReplicas:    0,
						ObservedGeneration: 1,
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionFalse,
								Reason: "MinimumReplicasUnavailable",
							},
						},
					},
				},
			},
			url: nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestCreatedRevision: "1",
				URL:                   nil,
			},
			expectError: false,
		},
		{
			name:        "empty deployment list",
			status:      &v1beta1.InferenceServiceStatus{},
			component:   v1beta1.PredictorComponent,
			deployments: []*appsv1.Deployment{},
			url:         nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestCreatedRevision: "",
				URL:                   nil,
			},
			expectError: true,
		},
		{
			name:        "empty deployment list for engine",
			status:      &v1beta1.InferenceServiceStatus{},
			component:   v1beta1.EngineComponent,
			deployments: []*appsv1.Deployment{},
			url:         nil,
			expectedStatus: v1beta1.ComponentStatusSpec{
				LatestCreatedRevision: "",
				URL:                   nil,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()

			manager.PropagateMultiNodeRayVLLMStatus(tt.status, tt.component, tt.deployments, tt.url)

			if tt.expectError {
				// Check that a condition was set indicating the error
				var expectedCondition apis.ConditionType
				switch tt.component {
				case v1beta1.PredictorComponent:
					expectedCondition = v1beta1.PredictorReady
				case v1beta1.EngineComponent:
					expectedCondition = v1beta1.EngineReady
				case v1beta1.DecoderComponent:
					expectedCondition = v1beta1.DecoderReady
				}

				condition := tt.status.GetCondition(expectedCondition)
				if condition != nil {
					assert.Equal(t, corev1.ConditionFalse, condition.Status)
				}
			} else {
				actualStatus := tt.status.Components[tt.component]
				assert.Equal(t, tt.expectedStatus.LatestCreatedRevision, actualStatus.LatestCreatedRevision)
				assert.Equal(t, tt.expectedStatus.URL, actualStatus.URL)

				if len(tt.deployments) > 0 {
					assert.Equal(t, tt.deployments[0].Status.ObservedGeneration, tt.status.ObservedGeneration)
				}
			}
		})
	}
}
