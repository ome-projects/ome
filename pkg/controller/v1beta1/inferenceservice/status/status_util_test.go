package status

import (
	"fmt"
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
	"github.com/sgl-project/ome/pkg/constants"
)

func TestInitializeComponentStatus(t *testing.T) {
	tests := []struct {
		name      string
		status    *v1beta1.InferenceServiceStatus
		component v1beta1.ComponentType
		expected  v1beta1.ComponentStatusSpec
	}{
		{
			name:      "initialize empty status",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.PredictorComponent,
			expected:  v1beta1.ComponentStatusSpec{},
		},
		{
			name: "initialize with existing components",
			status: &v1beta1.InferenceServiceStatus{
				Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.PredictorComponent: {
						LatestReadyRevision: "existing-rev",
					},
				},
			},
			component: v1beta1.PredictorComponent,
			expected: v1beta1.ComponentStatusSpec{
				LatestReadyRevision: "existing-rev",
			},
		},
		{
			name:      "initialize engine component",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.EngineComponent,
			expected:  v1beta1.ComponentStatusSpec{},
		},
		{
			name:      "initialize decoder component",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.DecoderComponent,
			expected:  v1beta1.ComponentStatusSpec{},
		},
		{
			name: "initialize engine with existing data",
			status: &v1beta1.InferenceServiceStatus{
				Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.EngineComponent: {
						LatestReadyRevision:   "engine-rev-1",
						LatestCreatedRevision: "engine-rev-2",
						URL:                   &apis.URL{Scheme: "http", Host: "engine.example.com"},
					},
				},
			},
			component: v1beta1.EngineComponent,
			expected: v1beta1.ComponentStatusSpec{
				LatestReadyRevision:   "engine-rev-1",
				LatestCreatedRevision: "engine-rev-2",
				URL:                   &apis.URL{Scheme: "http", Host: "engine.example.com"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			result := manager.initializeComponentStatus(tt.status, tt.component)
			assert.Equal(t, tt.expected, result)
			assert.NotNil(t, tt.status.Components)
		})
	}
}

func TestInitializeComponentCondition(t *testing.T) {
	tests := []struct {
		name              string
		initialCondition  *apis.Condition
		expectedReason    string
		expectedMessage   string
		expectedCondition corev1.ConditionStatus
	}{
		{
			name:              "initialize missing condition",
			initialCondition:  nil,
			expectedReason:    "Initializing",
			expectedMessage:   "engine component initializing",
			expectedCondition: corev1.ConditionFalse,
		},
		{
			name: "preserve existing failure condition",
			initialCondition: &apis.Condition{
				Type:    v1beta1.EngineReady,
				Status:  corev1.ConditionFalse,
				Reason:  constants.InsufficientGPUMemoryReason,
				Message: constants.InsufficientGPUMemoryMessage,
			},
			expectedReason:    constants.InsufficientGPUMemoryReason,
			expectedMessage:   constants.InsufficientGPUMemoryMessage,
			expectedCondition: corev1.ConditionFalse,
		},
		{
			name: "preserve existing ready condition",
			initialCondition: &apis.Condition{
				Type:    v1beta1.EngineReady,
				Status:  corev1.ConditionTrue,
				Reason:  "MinimumReplicasAvailable",
				Message: "Deployment has minimum availability.",
			},
			expectedReason:    "MinimumReplicasAvailable",
			expectedMessage:   "Deployment has minimum availability.",
			expectedCondition: corev1.ConditionTrue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			status := &v1beta1.InferenceServiceStatus{}
			if tt.initialCondition != nil {
				status.SetCondition(v1beta1.EngineReady, tt.initialCondition)
			}

			manager.InitializeComponentCondition(status, v1beta1.EngineComponent)

			engineReady := status.GetCondition(v1beta1.EngineReady)
			require.NotNil(t, engineReady)
			assert.Equal(t, tt.expectedCondition, engineReady.Status)
			assert.Equal(t, tt.expectedReason, engineReady.Reason)
			assert.Equal(t, tt.expectedMessage, engineReady.Message)
		})
	}
}

func TestGetFirstPod(t *testing.T) {
	tests := []struct {
		name        string
		podList     *corev1.PodList
		expectError bool
		expectedPod string
	}{
		{
			name: "successful get first pod",
			podList: &corev1.PodList{
				Items: []corev1.Pod{
					{ObjectMeta: metav1.ObjectMeta{Name: "pod-1"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "pod-2"}},
				},
			},
			expectError: false,
			expectedPod: "pod-1",
		},
		{
			name:        "empty pod list",
			podList:     &corev1.PodList{Items: []corev1.Pod{}},
			expectError: true,
		},
		{
			name:        "nil pod list",
			podList:     nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			pod, err := manager.getFirstPod(tt.podList)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, pod)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, pod)
				assert.Equal(t, tt.expectedPod, pod.Name)
			}
		})
	}
}

func TestGetFirstDeployment(t *testing.T) {
	tests := []struct {
		name               string
		deployments        []*appsv1.Deployment
		expectError        bool
		expectedDeployment string
	}{
		{
			name: "successful get first deployment",
			deployments: []*appsv1.Deployment{
				{ObjectMeta: metav1.ObjectMeta{Name: "deployment-1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "deployment-2"}},
			},
			expectError:        false,
			expectedDeployment: "deployment-1",
		},
		{
			name:        "empty deployment list",
			deployments: []*appsv1.Deployment{},
			expectError: true,
		},
		{
			name:        "nil deployment list",
			deployments: nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			deployment, err := manager.getFirstDeployment(tt.deployments)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, deployment)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, deployment)
				assert.Equal(t, tt.expectedDeployment, deployment.Name)
			}
		})
	}
}

func TestGetDeploymentCondition(t *testing.T) {
	tests := []struct {
		name          string
		deployment    *appsv1.Deployment
		conditionType appsv1.DeploymentConditionType
		expected      *apis.Condition
	}{
		{
			name: "deployment with available condition",
			deployment: &appsv1.Deployment{
				Status: appsv1.DeploymentStatus{
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:    appsv1.DeploymentAvailable,
							Status:  corev1.ConditionTrue,
							Reason:  "MinimumReplicasAvailable",
							Message: "Deployment has minimum availability.",
						},
					},
				},
			},
			conditionType: appsv1.DeploymentAvailable,
			expected: &apis.Condition{
				Type:    apis.ConditionType(appsv1.DeploymentAvailable),
				Status:  corev1.ConditionTrue,
				Reason:  "MinimumReplicasAvailable",
				Message: "Deployment has minimum availability.",
			},
		},
		{
			name: "deployment with progressing condition",
			deployment: &appsv1.Deployment{
				Status: appsv1.DeploymentStatus{
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:    appsv1.DeploymentProgressing,
							Status:  corev1.ConditionTrue,
							Reason:  "NewReplicaSetAvailable",
							Message: "ReplicaSet has successfully progressed.",
						},
					},
				},
			},
			conditionType: appsv1.DeploymentProgressing,
			expected: &apis.Condition{
				Type:    apis.ConditionType(appsv1.DeploymentProgressing),
				Status:  corev1.ConditionTrue,
				Reason:  "NewReplicaSetAvailable",
				Message: "ReplicaSet has successfully progressed.",
			},
		},
		{
			name: "deployment with replica failure condition",
			deployment: &appsv1.Deployment{
				Status: appsv1.DeploymentStatus{
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
			conditionType: appsv1.DeploymentReplicaFailure,
			expected: &apis.Condition{
				Type:    apis.ConditionType(appsv1.DeploymentReplicaFailure),
				Status:  corev1.ConditionTrue,
				Reason:  "FailedCreate",
				Message: "pods \"test-pod-\" is forbidden: exceeded quota",
			},
		},
		{
			name: "deployment without requested condition",
			deployment: &appsv1.Deployment{
				Status: appsv1.DeploymentStatus{
					Conditions: []appsv1.DeploymentCondition{
						{
							Type:   appsv1.DeploymentProgressing,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			conditionType: appsv1.DeploymentAvailable,
			expected: &apis.Condition{
				Type: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			result := manager.getDeploymentCondition(tt.deployment, tt.conditionType)

			assert.Equal(t, tt.expected.Type, result.Type)
			assert.Equal(t, tt.expected.Status, result.Status)
			assert.Equal(t, tt.expected.Reason, result.Reason)
			assert.Equal(t, tt.expected.Message, result.Message)
		})
	}
}

func TestGetLWSConditions(t *testing.T) {
	tests := []struct {
		name          string
		lws           *lwsspec.LeaderWorkerSet
		conditionType lwsspec.LeaderWorkerSetConditionType
		expected      *apis.Condition
	}{
		{
			name: "LWS with available condition",
			lws: &lwsspec.LeaderWorkerSet{
				Status: lwsspec.LeaderWorkerSetStatus{
					Conditions: []metav1.Condition{
						{
							Type:    string(lwsspec.LeaderWorkerSetAvailable),
							Status:  metav1.ConditionTrue,
							Reason:  "AllReplicasReady",
							Message: "All replicas are ready",
						},
					},
				},
			},
			conditionType: lwsspec.LeaderWorkerSetAvailable,
			expected: &apis.Condition{
				Type:    apis.ConditionType(lwsspec.LeaderWorkerSetAvailable),
				Status:  corev1.ConditionTrue,
				Reason:  "AllReplicasReady",
				Message: "All replicas are ready",
			},
		},
		{
			name: "LWS with progressing condition",
			lws: &lwsspec.LeaderWorkerSet{
				Status: lwsspec.LeaderWorkerSetStatus{
					Conditions: []metav1.Condition{
						{
							Type:    "Progressing",
							Status:  metav1.ConditionTrue,
							Reason:  "ReplicaSetUpdated",
							Message: "ReplicaSet is being updated",
						},
					},
				},
			},
			conditionType: "Progressing",
			expected: &apis.Condition{
				Type:    apis.ConditionType("Progressing"),
				Status:  corev1.ConditionTrue,
				Reason:  "ReplicaSetUpdated",
				Message: "ReplicaSet is being updated",
			},
		},
		{
			name: "LWS without requested condition",
			lws: &lwsspec.LeaderWorkerSet{
				Status: lwsspec.LeaderWorkerSetStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "Progressing",
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			conditionType: lwsspec.LeaderWorkerSetAvailable,
			expected: &apis.Condition{
				Type: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			result := manager.getLWSConditions(tt.lws, tt.conditionType)

			assert.Equal(t, tt.expected.Type, result.Type)
			assert.Equal(t, tt.expected.Status, result.Status)
			assert.Equal(t, tt.expected.Reason, result.Reason)
			assert.Equal(t, tt.expected.Message, result.Message)
		})
	}
}

func TestGetMultiDeploymentCondition(t *testing.T) {
	tests := []struct {
		name          string
		deployments   []*appsv1.Deployment
		conditionType appsv1.DeploymentConditionType
		expected      *apis.Condition
	}{
		{
			name: "all deployments available",
			deployments: []*appsv1.Deployment{
				{
					Status: appsv1.DeploymentStatus{
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:    appsv1.DeploymentAvailable,
								Status:  corev1.ConditionTrue,
								Reason:  "MinimumReplicasAvailable",
								Message: "Deployment has minimum availability.",
							},
						},
					},
				},
				{
					Status: appsv1.DeploymentStatus{
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionTrue,
							},
						},
					},
				},
			},
			conditionType: appsv1.DeploymentAvailable,
			expected: &apis.Condition{
				Type:    apis.ConditionType(appsv1.DeploymentAvailable),
				Status:  corev1.ConditionTrue,
				Reason:  "MinimumReplicasAvailable",
				Message: "Deployment has minimum availability.",
			},
		},
		{
			name: "one deployment not available",
			deployments: []*appsv1.Deployment{
				{
					Status: appsv1.DeploymentStatus{
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionTrue,
							},
						},
					},
				},
				{
					Status: appsv1.DeploymentStatus{
						Conditions: []appsv1.DeploymentCondition{
							{
								Type:   appsv1.DeploymentAvailable,
								Status: corev1.ConditionFalse,
							},
						},
					},
				},
			},
			conditionType: appsv1.DeploymentAvailable,
			expected: &apis.Condition{
				Type: "",
			},
		},
		{
			name:          "empty deployment list",
			deployments:   []*appsv1.Deployment{},
			conditionType: appsv1.DeploymentAvailable,
			expected: &apis.Condition{
				Type:    apis.ConditionType(appsv1.DeploymentAvailable),
				Status:  corev1.ConditionFalse,
				Reason:  "NoDeployments",
				Message: "No deployments available",
			},
		},
		{
			name: "deployment with no conditions",
			deployments: []*appsv1.Deployment{
				{
					Status: appsv1.DeploymentStatus{
						Conditions: nil,
					},
				},
			},
			conditionType: appsv1.DeploymentAvailable,
			expected: &apis.Condition{
				Type: "",
			},
		},
		{
			name: "all deployments available but no conditions on first",
			deployments: []*appsv1.Deployment{
				{
					Status: appsv1.DeploymentStatus{
						Conditions: []appsv1.DeploymentCondition{},
					},
				},
			},
			conditionType: appsv1.DeploymentAvailable,
			expected: &apis.Condition{
				Type:    apis.ConditionType(appsv1.DeploymentAvailable),
				Status:  corev1.ConditionTrue,
				Reason:  "Available",
				Message: "All deployments available",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			result := manager.getMultiDeploymentCondition(tt.deployments, tt.conditionType)

			assert.Equal(t, tt.expected.Type, result.Type)
			assert.Equal(t, tt.expected.Status, result.Status)
			assert.Equal(t, tt.expected.Reason, result.Reason)
			assert.Equal(t, tt.expected.Message, result.Message)
		})
	}
}

func TestSetCondition(t *testing.T) {
	tests := []struct {
		name          string
		status        *v1beta1.InferenceServiceStatus
		conditionType apis.ConditionType
		condition     *apis.Condition
		shouldSet     bool
	}{
		{
			name:          "set condition true",
			status:        &v1beta1.InferenceServiceStatus{},
			conditionType: v1beta1.PredictorReady,
			condition: &apis.Condition{
				Type:   v1beta1.PredictorReady,
				Status: corev1.ConditionTrue,
				Reason: "Ready",
			},
			shouldSet: true,
		},
		{
			name:          "set condition false",
			status:        &v1beta1.InferenceServiceStatus{},
			conditionType: v1beta1.PredictorReady,
			condition: &apis.Condition{
				Type:   v1beta1.PredictorReady,
				Status: corev1.ConditionFalse,
				Reason: "NotReady",
			},
			shouldSet: true,
		},
		{
			name:          "set condition unknown",
			status:        &v1beta1.InferenceServiceStatus{},
			conditionType: v1beta1.PredictorReady,
			condition: &apis.Condition{
				Type:   v1beta1.PredictorReady,
				Status: corev1.ConditionUnknown,
				Reason: "Unknown",
			},
			shouldSet: true,
		},
		{
			name:          "set condition true with reason and message",
			status:        &v1beta1.InferenceServiceStatus{},
			conditionType: v1beta1.PredictorReady,
			condition: &apis.Condition{
				Type:    v1beta1.PredictorReady,
				Status:  corev1.ConditionTrue,
				Reason:  "Ready",
				Message: "Predictor is ready",
			},
			shouldSet: true,
		},
		{
			name:          "nil condition",
			status:        &v1beta1.InferenceServiceStatus{},
			conditionType: v1beta1.PredictorReady,
			condition:     nil,
			shouldSet:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			manager.setCondition(tt.status, tt.conditionType, tt.condition)

			if tt.shouldSet {
				condition := tt.status.GetCondition(tt.conditionType)
				assert.NotNil(t, condition)
				assert.Equal(t, tt.condition.Status, condition.Status)
				assert.Equal(t, tt.condition.Reason, condition.Reason)
				assert.Equal(t, tt.condition.Message, condition.Message)
			}
		})
	}
}

func TestGetReadyConditionsMap(t *testing.T) {
	manager := NewStatusReconciler(nil)
	conditionsMap := manager.getReadyConditionsMap()

	assert.NotNil(t, conditionsMap)
	assert.Equal(t, v1beta1.PredictorReady, conditionsMap[v1beta1.PredictorComponent])
	assert.Equal(t, v1beta1.EngineReady, conditionsMap[v1beta1.EngineComponent])
	assert.Equal(t, v1beta1.DecoderReady, conditionsMap[v1beta1.DecoderComponent])
}

func TestGetRouteConditionsMap(t *testing.T) {
	manager := NewStatusReconciler(nil)
	conditionsMap := manager.getRouteConditionsMap()

	assert.NotNil(t, conditionsMap)
	assert.Equal(t, v1beta1.PredictorRouteReady, conditionsMap[v1beta1.PredictorComponent])
	assert.Equal(t, v1beta1.EngineRouteReady, conditionsMap[v1beta1.EngineComponent])
	assert.Equal(t, v1beta1.DecoderRouteReady, conditionsMap[v1beta1.DecoderComponent])
}

func TestGetConfigurationConditionsMap(t *testing.T) {
	manager := NewStatusReconciler(nil)
	conditionsMap := manager.getConfigurationConditionsMap()

	assert.NotNil(t, conditionsMap)
	assert.Equal(t, v1beta1.PredictorConfigurationReady, conditionsMap[v1beta1.PredictorComponent])
	assert.Equal(t, v1beta1.EngineConfigurationReady, conditionsMap[v1beta1.EngineComponent])
	assert.Equal(t, v1beta1.DecoderConfigurationReady, conditionsMap[v1beta1.DecoderComponent])
}

func TestGetConditionsMapIndex(t *testing.T) {
	manager := NewStatusReconciler(nil)
	conditionsMapIndex := manager.getConditionsMapIndex()

	assert.NotNil(t, conditionsMapIndex)
	assert.Contains(t, conditionsMapIndex, v1beta1.RoutesReady)
	assert.Contains(t, conditionsMapIndex, v1beta1.LatestDeploymentReady)
}

func TestHandleTrafficRouting(t *testing.T) {
	tests := []struct {
		name             string
		statusSpec       *v1beta1.ComponentStatusSpec
		serviceStatus    *knservingv1.ServiceStatus
		revisionTraffic  map[string]int64
		expectedLatest   string
		expectedPrevious string
	}{
		{
			name: "full traffic to latest revision",
			statusSpec: &v1beta1.ComponentStatusSpec{
				LatestRolledoutRevision: "",
			},
			serviceStatus: &knservingv1.ServiceStatus{
				ConfigurationStatusFields: knservingv1.ConfigurationStatusFields{
					LatestReadyRevisionName:   "rev-2",
					LatestCreatedRevisionName: "rev-2",
				},
				RouteStatusFields: knservingv1.RouteStatusFields{
					Traffic: []knservingv1.TrafficTarget{
						{
							RevisionName:   "rev-2",
							Percent:        ptr.To(int64(100)),
							LatestRevision: ptr.To(true),
						},
					},
				},
			},
			revisionTraffic:  map[string]int64{"rev-2": 100},
			expectedLatest:   "rev-2",
			expectedPrevious: "",
		},
		{
			name: "traffic split between revisions",
			statusSpec: &v1beta1.ComponentStatusSpec{
				LatestRolledoutRevision: "rev-1",
			},
			serviceStatus: &knservingv1.ServiceStatus{
				ConfigurationStatusFields: knservingv1.ConfigurationStatusFields{
					LatestReadyRevisionName:   "rev-2",
					LatestCreatedRevisionName: "rev-2",
				},
				RouteStatusFields: knservingv1.RouteStatusFields{
					Traffic: []knservingv1.TrafficTarget{
						{
							RevisionName:   "rev-2",
							Percent:        ptr.To(int64(50)),
							LatestRevision: ptr.To(true),
						},
					},
				},
			},
			revisionTraffic:  map[string]int64{"rev-2": 50},
			expectedLatest:   "rev-1", // Should remain unchanged
			expectedPrevious: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			manager.handleTrafficRouting(tt.statusSpec, tt.serviceStatus, tt.revisionTraffic)

			assert.Equal(t, tt.expectedLatest, tt.statusSpec.LatestRolledoutRevision)
			assert.Equal(t, tt.expectedPrevious, tt.statusSpec.PreviousRolledoutRevision)
		})
	}
}

func TestPropagateServiceConditions(t *testing.T) {
	tests := []struct {
		name          string
		status        *v1beta1.InferenceServiceStatus
		component     v1beta1.ComponentType
		serviceStatus *knservingv1.ServiceStatus
		expectedURL   *apis.URL
		expectedAddr  *duckv1.Addressable
	}{
		{
			name:      "service ready with URL and address",
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
					URL: &apis.URL{Scheme: "https", Host: "test.example.com"},
					Address: &duckv1.Addressable{
						URL: &apis.URL{Scheme: "https", Host: "test.example.com"},
					},
				},
			},
			expectedURL:  &apis.URL{Scheme: "https", Host: "test.example.com"},
			expectedAddr: &duckv1.Addressable{URL: &apis.URL{Scheme: "https", Host: "test.example.com"}},
		},
		{
			name:      "service not ready",
			status:    &v1beta1.InferenceServiceStatus{},
			component: v1beta1.PredictorComponent,
			serviceStatus: &knservingv1.ServiceStatus{
				Status: duckv1.Status{
					Conditions: duckv1.Conditions{
						{
							Type:   knservingv1.ServiceConditionReady,
							Status: corev1.ConditionFalse,
						},
					},
				},
			},
			expectedURL:  nil,
			expectedAddr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			statusSpec := v1beta1.ComponentStatusSpec{}

			manager.propagateServiceConditions(tt.status, tt.component, tt.serviceStatus, &statusSpec)

			assert.Equal(t, tt.expectedURL, statusSpec.URL)
			assert.Equal(t, tt.expectedAddr, statusSpec.Address)

			// Check that conditions were set
			readyCondition := tt.status.GetCondition(v1beta1.PredictorReady)
			assert.NotNil(t, readyCondition)
		})
	}
}

func TestCheckContainerStatuses(t *testing.T) {
	tests := []struct {
		name          string
		status        *v1beta1.InferenceServiceStatus
		pod           *corev1.Pod
		totalCopies   int
		expectedState v1beta1.ModelState
		expectedError string
	}{
		{
			name:   "storage initializer running",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							Name: constants.StorageInitializerContainerName,
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			totalCopies:   1,
			expectedState: v1beta1.Loading,
		},
		{
			name:   "storage initializer terminated with gpu oom error",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							Name: constants.StorageInitializerContainerName,
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Reason:   constants.StateReasonError,
									Message:  "CUDA error: not enough GPU memory to load model",
									ExitCode: 1,
								},
							},
						},
					},
				},
			},
			totalCopies:   1,
			expectedState: v1beta1.FailedToLoad,
			expectedError: "Model failed to load: not enough GPU memory",
		},
		{
			name:   "storage initializer crash loop back off",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							Name: constants.StorageInitializerContainerName,
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason: constants.StateReasonCrashLoopBackOff,
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Message:  "Failed to download model",
									ExitCode: 1,
								},
							},
						},
					},
				},
			},
			totalCopies:   1,
			expectedState: v1beta1.FailedToLoad,
		},
		{
			name:   "main container terminated with error",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: constants.MainContainerName,
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Reason:   constants.StateReasonError,
									Message:  "Model loading failed",
									ExitCode: 1,
								},
							},
						},
					},
				},
			},
			totalCopies:   1,
			expectedState: v1beta1.FailedToLoad,
		},
		{
			name:   "main container crash loop back off with termination",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: constants.MainContainerName,
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason: constants.StateReasonCrashLoopBackOff,
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Message:  "Model loading failed",
									ExitCode: 1,
								},
							},
						},
					},
				},
			},
			totalCopies:   1,
			expectedState: v1beta1.FailedToLoad,
		},
		{
			name:   "main container crash loop back off without termination",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: constants.MainContainerName,
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason: constants.StateReasonCrashLoopBackOff,
								},
							},
						},
					},
				},
			},
			totalCopies:   1,
			expectedState: v1beta1.Pending,
		},
		{
			name:   "main container running",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: constants.MainContainerName,
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			totalCopies:   1,
			expectedState: v1beta1.Pending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			manager.checkContainerStatuses(tt.status, v1beta1.EngineComponent, tt.pod, tt.totalCopies)

			if tt.status.ModelStatus.ModelRevisionStates != nil {
				assert.Equal(t, tt.expectedState, tt.status.ModelStatus.ModelRevisionStates.TargetModelState)
			}
			if tt.expectedError != "" {
				require.NotNil(t, tt.status.ModelStatus.LastFailureInfo)
				assert.Equal(t, tt.expectedError, tt.status.ModelStatus.LastFailureInfo.Message)
				engineReady := tt.status.GetCondition(v1beta1.EngineReady)
				require.NotNil(t, engineReady)
				assert.Equal(t, corev1.ConditionFalse, engineReady.Status)
				assert.Equal(t, constants.InsufficientGPUMemoryReason, engineReady.Reason)
				assert.Equal(t, constants.InsufficientGPUMemoryMessage, engineReady.Message)
				ready := tt.status.GetCondition(apis.ConditionReady)
				require.NotNil(t, ready)
				assert.Equal(t, corev1.ConditionFalse, ready.Status)
			}
		})
	}
}

func TestCheckContainerStatusesUsesPodLogsForGPUOOM(t *testing.T) {
	manager := NewStatusReconciler(nil)
	manager.podLogFetcher = func(namespace, podName, containerName string, previous bool) (string, error) {
		assert.Equal(t, "default", namespace)
		assert.Equal(t, "test-pod", podName)
		assert.Equal(t, constants.MainContainerName, containerName)
		assert.True(t, previous)
		return "\u001b[0;36m(APIServer pid=1)\u001b[0;0m RuntimeError: Engine core initialization failed. See root cause above. Failed core proc(s): {}\n" +
			"\u001b[0;36m(Worker_TP0_EP0 pid=544)\u001b[0;0m ERROR Failed to load model - not enough GPU memory.", nil
	}

	status := &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: constants.MainContainerName,
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason: constants.StateReasonCrashLoopBackOff,
						},
					},
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Message:  "RuntimeError: Engine core initialization failed. See root cause above. Failed core proc(s): {}",
							ExitCode: 1,
						},
					},
				},
			},
		},
	}

	manager.checkContainerStatuses(status, v1beta1.EngineComponent, pod, 1)

	require.NotNil(t, status.ModelStatus.ModelRevisionStates)
	assert.Equal(t, v1beta1.FailedToLoad, status.ModelStatus.ModelRevisionStates.TargetModelState)
	require.NotNil(t, status.ModelStatus.LastFailureInfo)
	assert.Equal(t, constants.ModelLoadGPUOOMFailureMessage, status.ModelStatus.LastFailureInfo.Message)
	assert.Equal(t, int32(1), status.ModelStatus.LastFailureInfo.ExitCode)
	engineReady := status.GetCondition(v1beta1.EngineReady)
	require.NotNil(t, engineReady)
	assert.Equal(t, corev1.ConditionFalse, engineReady.Status)
	assert.Equal(t, constants.InsufficientGPUMemoryReason, engineReady.Reason)
	assert.Equal(t, constants.InsufficientGPUMemoryMessage, engineReady.Message)
	ready := status.GetCondition(apis.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, corev1.ConditionFalse, ready.Status)
}

func TestCheckContainerStatusesDoesNotUsePodLogsForStorageInitializer(t *testing.T) {
	manager := NewStatusReconciler(nil)
	fetcherCalls := 0
	manager.podLogFetcher = func(namespace, podName, containerName string, previous bool) (string, error) {
		fetcherCalls++
		return constants.ModelLoadGPUOOMFailureMessage, nil
	}

	status := &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: constants.StorageInitializerContainerName,
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   constants.StateReasonError,
							Message:  "failed to download model",
							ExitCode: 2,
						},
					},
				},
			},
		},
	}

	manager.checkContainerStatuses(status, v1beta1.EngineComponent, pod, 1)

	assert.Equal(t, 0, fetcherCalls)
	require.NotNil(t, status.ModelStatus.ModelRevisionStates)
	assert.Equal(t, v1beta1.FailedToLoad, status.ModelStatus.ModelRevisionStates.TargetModelState)
	require.NotNil(t, status.ModelStatus.LastFailureInfo)
	assert.Equal(t, "failed to download model", status.ModelStatus.LastFailureInfo.Message)
	engineReady := status.GetCondition(v1beta1.EngineReady)
	if engineReady != nil {
		assert.NotEqual(t, constants.InsufficientGPUMemoryReason, engineReady.Reason)
	}
}

func TestCheckContainerStatusesDoesNotSetComponentFailureConditionForNonGPUOOM(t *testing.T) {
	manager := NewStatusReconciler(nil)
	status := &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}}
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: constants.MainContainerName,
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   constants.StateReasonError,
							Message:  "generic startup failure",
							ExitCode: 2,
						},
					},
				},
			},
		},
	}

	manager.checkContainerStatuses(status, v1beta1.EngineComponent, pod, 1)

	require.NotNil(t, status.ModelStatus.ModelRevisionStates)
	assert.Equal(t, v1beta1.FailedToLoad, status.ModelStatus.ModelRevisionStates.TargetModelState)
	require.NotNil(t, status.ModelStatus.LastFailureInfo)
	assert.Equal(t, "generic startup failure", status.ModelStatus.LastFailureInfo.Message)
	engineReady := status.GetCondition(v1beta1.EngineReady)
	if engineReady != nil {
		assert.NotEqual(t, constants.InsufficientGPUMemoryReason, engineReady.Reason)
	}
}

func TestGetContainerFailureMessage(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	tests := []struct {
		name                string
		containerStatus     corev1.ContainerStatus
		logMessage          string
		logErr              error
		expectFetcherCalled bool
		expectedPrevious    bool
		expectedMessage     string
		expectedExitCode    int32
		expectedFound       bool
	}{
		{
			name: "terminated container uses current pod logs to upgrade generic failure",
			containerStatus: corev1.ContainerStatus{
				Name: constants.MainContainerName,
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "RuntimeError: Engine core initialization failed. See root cause above. Failed core proc(s): {}",
						ExitCode: 7,
					},
				},
			},
			logMessage:          "torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 1.31 GiB.",
			expectFetcherCalled: true,
			expectedPrevious:    false,
			expectedMessage:     constants.ModelLoadGPUOOMFailureMessage,
			expectedExitCode:    7,
			expectedFound:       true,
		},
		{
			name: "crash loop back off uses previous pod logs to upgrade generic failure",
			containerStatus: corev1.ContainerStatus{
				Name: constants.MainContainerName,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: constants.StateReasonCrashLoopBackOff,
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "WorkerProc failed to start.",
						ExitCode: 8,
					},
				},
			},
			logMessage:          "ERROR Failed to load model - not enough GPU memory.",
			expectFetcherCalled: true,
			expectedPrevious:    true,
			expectedMessage:     constants.ModelLoadGPUOOMFailureMessage,
			expectedExitCode:    8,
			expectedFound:       true,
		},
		{
			name: "gpu oom from termination message skips log fetch",
			containerStatus: corev1.ContainerStatus{
				Name: constants.MainContainerName,
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "torch.OutOfMemoryError: CUDA out of memory.",
						ExitCode: 9,
					},
				},
			},
			expectFetcherCalled: false,
			expectedMessage:     constants.ModelLoadGPUOOMFailureMessage,
			expectedExitCode:    9,
			expectedFound:       true,
		},
		{
			name: "non oom logs preserve original termination message",
			containerStatus: corev1.ContainerStatus{
				Name: constants.MainContainerName,
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "generic startup failure",
						ExitCode: 10,
					},
				},
			},
			logMessage:          "some unrelated log line",
			expectFetcherCalled: true,
			expectedPrevious:    false,
			expectedMessage:     "generic startup failure",
			expectedExitCode:    10,
			expectedFound:       true,
		},
		{
			name: "log fetch error preserves original termination message",
			containerStatus: corev1.ContainerStatus{
				Name: constants.MainContainerName,
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "generic startup failure",
						ExitCode: 11,
					},
				},
			},
			logErr:              fmt.Errorf("log fetch failed"),
			expectFetcherCalled: true,
			expectedPrevious:    false,
			expectedMessage:     "generic startup failure",
			expectedExitCode:    11,
			expectedFound:       true,
		},
		{
			name: "no termination info returns no message",
			containerStatus: corev1.ContainerStatus{
				Name: constants.MainContainerName,
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			},
			logMessage:          "torch.OutOfMemoryError: CUDA out of memory.",
			expectFetcherCalled: true,
			expectedPrevious:    false,
			expectedMessage:     constants.ModelLoadGPUOOMFailureMessage,
			expectedExitCode:    0,
			expectedFound:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)

			fetcherCalls := 0
			manager.podLogFetcher = func(namespace, podName, containerName string, previous bool) (string, error) {
				fetcherCalls++
				assert.Equal(t, "default", namespace)
				assert.Equal(t, "test-pod", podName)
				assert.Equal(t, constants.MainContainerName, containerName)
				assert.Equal(t, tt.expectedPrevious, previous)
				return tt.logMessage, tt.logErr
			}

			message, exitCode, found := manager.getContainerFailureMessage(pod, tt.containerStatus)

			assert.Equal(t, tt.expectedMessage, message)
			assert.Equal(t, tt.expectedExitCode, exitCode)
			assert.Equal(t, tt.expectedFound, found)
			if tt.expectFetcherCalled {
				assert.Equal(t, 1, fetcherCalls)
			} else {
				assert.Equal(t, 0, fetcherCalls)
			}
		})
	}
}

func TestGetContainerFailureMessageWithoutPodOrFetcher(t *testing.T) {
	containerStatus := corev1.ContainerStatus{
		Name: constants.MainContainerName,
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				Message:  "generic startup failure",
				ExitCode: 12,
			},
		},
	}

	manager := NewStatusReconciler(nil)
	manager.podLogFetcher = nil

	message, exitCode, found := manager.getContainerFailureMessage(nil, containerStatus)
	assert.Equal(t, "generic startup failure", message)
	assert.Equal(t, int32(12), exitCode)
	assert.True(t, found)

	message, exitCode, found = NewStatusReconciler(nil).getContainerFailureMessage(nil, containerStatus)
	assert.Equal(t, "generic startup failure", message)
	assert.Equal(t, int32(12), exitCode)
	assert.True(t, found)
}

func TestSafeGetTerminationMessage(t *testing.T) {
	tests := []struct {
		name                   string
		containerStatus        corev1.ContainerStatus
		expectedMessage        string
		expectedExitCode       int32
		expectedHasTermination bool
	}{
		{
			name: "terminated container",
			containerStatus: corev1.ContainerStatus{
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "Container terminated",
						ExitCode: 1,
					},
				},
			},
			expectedMessage:        "Container terminated",
			expectedExitCode:       1,
			expectedHasTermination: true,
		},
		{
			name: "terminated container with ansi gpu oom log",
			containerStatus: corev1.ContainerStatus{
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "\u001b[0;36m(Worker_TP0_EP0 pid=544)\u001b[0;0m ERROR Failed to load model - not enough GPU memory. Tried to allocate 1.31 GiB.",
						ExitCode: 1,
					},
				},
			},
			expectedMessage:        "Model failed to load: not enough GPU memory",
			expectedExitCode:       1,
			expectedHasTermination: true,
		},
		{
			name: "terminated container with multiline torch out of memory log",
			containerStatus: corev1.ContainerStatus{
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "\u001b[0;36m(Worker_TP1_EP1 pid=545)\u001b[0;0m ERROR 03-25 22:10:36 [multiproc_executor.py:772] torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 1.31 GiB. GPU 1 has a total capacity of 79.32 GiB of which 724.44 MiB is free.",
						ExitCode: 1,
					},
				},
			},
			expectedMessage:        "Model failed to load: not enough GPU memory",
			expectedExitCode:       1,
			expectedHasTermination: true,
		},
		{
			name: "crash loop back off with last termination",
			containerStatus: corev1.ContainerStatus{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: constants.StateReasonCrashLoopBackOff,
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "Last termination message",
						ExitCode: 2,
					},
				},
			},
			expectedMessage:        "Last termination message",
			expectedExitCode:       2,
			expectedHasTermination: true,
		},
		{
			name: "crash loop back off with ansi last termination message",
			containerStatus: corev1.ContainerStatus{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: constants.StateReasonCrashLoopBackOff,
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message:  "\u001b[0;36m(worker)\u001b[0;0m regular failure message",
						ExitCode: 2,
					},
				},
			},
			expectedMessage:        "(worker) regular failure message",
			expectedExitCode:       2,
			expectedHasTermination: true,
		},
		{
			name: "crash loop back off with multiline gpu oom worker startup log",
			containerStatus: corev1.ContainerStatus{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: constants.StateReasonCrashLoopBackOff,
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message: "\u001b[0;36m(Worker_TP1_EP1 pid=545)\u001b[0;0m ERROR 03-25 22:10:36 [multiproc_executor.py:772] torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 1.31 GiB. GPU 1 has a total capacity of 79.32 GiB of which 724.44 MiB is free.\n" +
							"\u001b[0;36m(Worker_TP1_EP1 pid=545)\u001b[0;0m INFO 03-25 22:10:36 [multiproc_executor.py:730] Parent process exited, terminating worker\n" +
							"\u001b[0;36m(Worker_TP0_EP0 pid=544)\u001b[0;0m INFO 03-25 22:10:36 [multiproc_executor.py:730] Parent process exited, terminating worker\n" +
							"\u001b[0;36m(Worker_TP0_EP0 pid=544)\u001b[0;0m ERROR 03-25 22:10:36 [gpu_model_runner.py:4128] Failed to load model - not enough GPU memory. Try lowering --gpu-memory-utilization to free memory for weights, increasing --tensor-parallel-size, or using --quantization.\n" +
							"\u001b[0;36m(Worker_TP0_EP0 pid=544)\u001b[0;0m ERROR 03-25 22:10:36 [multiproc_executor.py:772] WorkerProc failed to start.",
						ExitCode: 2,
					},
				},
			},
			expectedMessage:        "Model failed to load: not enough GPU memory",
			expectedExitCode:       2,
			expectedHasTermination: true,
		},
		{
			name: "crash loop back off without last termination",
			containerStatus: corev1.ContainerStatus{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: constants.StateReasonCrashLoopBackOff,
					},
				},
			},
			expectedMessage:        "",
			expectedExitCode:       0,
			expectedHasTermination: false,
		},
		{
			name: "running container",
			containerStatus: corev1.ContainerStatus{
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			},
			expectedMessage:        "",
			expectedExitCode:       0,
			expectedHasTermination: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler(nil)
			message, exitCode, hasTermination := manager.safeGetTerminationMessage(tt.containerStatus)

			assert.Equal(t, tt.expectedMessage, message)
			assert.Equal(t, tt.expectedExitCode, exitCode)
			assert.Equal(t, tt.expectedHasTermination, hasTermination)
		})
	}
}
