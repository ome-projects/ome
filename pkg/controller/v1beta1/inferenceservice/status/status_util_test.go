package status

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	lwsspec "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
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
			component: v1beta1.EngineComponent,
			expected:  v1beta1.ComponentStatusSpec{},
		},
		{
			name: "initialize with existing components",
			status: &v1beta1.InferenceServiceStatus{
				Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.EngineComponent: {},
				},
			},
			component: v1beta1.EngineComponent,
			expected:  v1beta1.ComponentStatusSpec{},
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
						URL: &apis.URL{Scheme: "http", Host: "engine.example.com"},
					},
				},
			},
			component: v1beta1.EngineComponent,
			expected: v1beta1.ComponentStatusSpec{
				URL: &apis.URL{Scheme: "http", Host: "engine.example.com"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewStatusReconciler()
			result := manager.initializeComponentStatus(tt.status, tt.component)
			assert.Equal(t, tt.expected, result)
			assert.NotNil(t, tt.status.Components)
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
			manager := NewStatusReconciler()
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

// A Deployment's report is its Available condition and its observed
// generation; a Deployment without that condition reports the zero condition.
func TestDeploymentReport(t *testing.T) {
	available := appsv1.DeploymentCondition{
		Type:    appsv1.DeploymentAvailable,
		Status:  corev1.ConditionTrue,
		Reason:  "MinimumReplicasAvailable",
		Message: "Deployment has minimum availability.",
	}
	progressing := appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetAvailable"}
	tests := []struct {
		name       string
		conditions []appsv1.DeploymentCondition
		expected   apis.Condition
	}{
		{
			name:       "available among other conditions",
			conditions: []appsv1.DeploymentCondition{progressing, available},
			expected:   apis.Condition{Type: "Available", Status: corev1.ConditionTrue, Reason: "MinimumReplicasAvailable", Message: "Deployment has minimum availability."},
		},
		{
			name:       "no available condition",
			conditions: []appsv1.DeploymentCondition{progressing},
		},
		{
			name: "no conditions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := &appsv1.Deployment{Status: appsv1.DeploymentStatus{ObservedGeneration: 7, Conditions: tt.conditions}}
			report := deploymentReport(deployment)

			assert.Equal(t, int64(7), report.generation)
			assert.Equal(t, tt.expected, report.available)
		})
	}
}

// A LeaderWorkerSet's report is its Available condition and its generation;
// a set without that condition reports the zero condition.
func TestLeaderWorkerSetReport(t *testing.T) {
	available := metav1.Condition{
		Type:    string(lwsspec.LeaderWorkerSetAvailable),
		Status:  metav1.ConditionTrue,
		Reason:  "AllReplicasReady",
		Message: "All replicas are ready",
	}
	progressing := metav1.Condition{Type: "Progressing", Status: metav1.ConditionTrue, Reason: "ReplicaSetUpdated"}
	tests := []struct {
		name       string
		conditions []metav1.Condition
		expected   apis.Condition
	}{
		{
			name:       "available among other conditions",
			conditions: []metav1.Condition{progressing, available},
			expected:   apis.Condition{Type: "Available", Status: corev1.ConditionTrue, Reason: "AllReplicasReady", Message: "All replicas are ready"},
		},
		{
			name:       "no available condition",
			conditions: []metav1.Condition{progressing},
		},
		{
			name: "no conditions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lws := &lwsspec.LeaderWorkerSet{Status: lwsspec.LeaderWorkerSetStatus{Conditions: tt.conditions}}
			lws.Generation = 7
			report := leaderWorkerSetReport(lws)

			assert.Equal(t, int64(7), report.generation)
			assert.Equal(t, tt.expected, report.available)
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
			name:   "storage initializer terminated with error",
			status: &v1beta1.InferenceServiceStatus{ModelStatus: v1beta1.ModelStatus{}},
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							Name: constants.StorageInitializerContainerName,
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Reason:   constants.StateReasonError,
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
			manager := NewStatusReconciler()
			manager.checkContainerStatuses(tt.status, tt.pod, tt.totalCopies)

			if tt.status.ModelStatus.ModelRevisionStates != nil {
				assert.Equal(t, tt.expectedState, tt.status.ModelStatus.ModelRevisionStates.TargetModelState)
			}
		})
	}
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
			manager := NewStatusReconciler()
			message, exitCode, hasTermination := manager.safeGetTerminationMessage(tt.containerStatus)

			assert.Equal(t, tt.expectedMessage, message)
			assert.Equal(t, tt.expectedExitCode, exitCode)
			assert.Equal(t, tt.expectedHasTermination, hasTermination)
		})
	}
}
