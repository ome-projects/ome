package effective

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestResolveAcceleratorBaseUsesActiveMergedRunnerAndRuntimeFallback(t *testing.T) {
	isvc := acceleratorISVCFixture()
	isvc.Spec.Engine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner"}}
	activeEngine := isvc.Spec.Engine.DeepCopy()
	activeEngine.Runner.Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2"),
	}
	runtimeSpec := &v1beta1.ServingRuntimeSpec{
		AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
			AcceleratorClasses: []string{"gpu-a"},
		},
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{Containers: []corev1.Container{{
			Name: "runner", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			}},
		}}},
	}
	state := acceleratorRuntimeState(isvc, &ActiveConfiguration{
		Origin: ConfigurationOriginLiveRuntime, RuntimeName: "runtime",
		RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod",
		Consistency: RevisionConsistencyUnknown, spec: runtimeSpec,
		components: []EffectiveComponent{{Type: v1beta1.EngineComponent, engine: activeEngine}},
	})

	got, err := ResolveAcceleratorBase(isvc, state)

	require.NoError(t, err)
	assert.Equal(t, AcceleratorActiveAvailable, got.ActiveState)
	assert.True(t, got.RuntimeAcceleratorsConfigured)
	assert.Equal(t, "ServingRuntime", got.ActiveSourceKind)
	assert.Equal(t, "runtime", got.ActiveSourceName)
	assert.Equal(t, "prod", got.ActiveSourceNamespace)
	assert.Equal(t, "runtime-uid", got.ActiveSourceUID)
	assert.Equal(t, "runtime-rv", got.ActiveSourceResourceVersion)
	assert.EqualValues(t, 2, got.ActiveSourceGeneration)
	require.Len(t, got.Components, 1)
	assert.Equal(t, AcceleratorBaseAvailable, got.Components[0].State)
	assert.Equal(t, []AcceleratorBaseRequest{
		{Name: "cpu", Quantity: "2"},
		{Name: "memory", Quantity: "4Gi"},
	}, got.Components[0].Requests)
	assert.Equal(t, StatusFreshnessCurrent, got.StatusFreshness)
}

func TestResolveAcceleratorBasePreservesExplicitISVCResourceOwnership(t *testing.T) {
	isvc := acceleratorISVCFixture()
	isvc.Spec.Engine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{
		Name: "runner", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1"),
		}},
	}}
	activeEngine := isvc.Spec.Engine.DeepCopy()
	runtimeSpec := &v1beta1.ServingRuntimeSpec{
		AcceleratorRequirements: &v1beta1.AcceleratorRequirements{AcceleratorClasses: []string{"gpu-a"}},
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{Containers: []corev1.Container{{
			Name: "runner", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("64Gi"),
			}},
		}}},
	}
	state := acceleratorRuntimeState(isvc, &ActiveConfiguration{
		Origin: ConfigurationOriginLiveRuntime, RuntimeName: "runtime",
		RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod",
		Consistency: RevisionConsistencyUnknown, spec: runtimeSpec,
		components: []EffectiveComponent{{Type: v1beta1.EngineComponent, engine: activeEngine}},
	})

	got, err := ResolveAcceleratorBase(isvc, state)

	require.NoError(t, err)
	require.Len(t, got.Components, 1)
	assert.Equal(t, []AcceleratorBaseRequest{{Name: "cpu", Quantity: "1"}}, got.Components[0].Requests)
}

func TestResolveAcceleratorBaseUsesLeaderServingTemplate(t *testing.T) {
	isvc := acceleratorISVCFixture()
	activeEngine := &v1beta1.EngineSpec{
		Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("99")},
		}}},
		Leader: &v1beta1.LeaderSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
			Name: "leader", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("3"),
			}},
		}}},
	}
	state := acceleratorRuntimeState(isvc, &ActiveConfiguration{
		Origin: ConfigurationOriginLiveRuntime, RuntimeName: "runtime",
		RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod",
		Consistency: RevisionConsistencyUnknown, spec: &v1beta1.ServingRuntimeSpec{},
		components: []EffectiveComponent{{Type: v1beta1.EngineComponent, engine: activeEngine}},
	})

	got, err := ResolveAcceleratorBase(isvc, state)

	require.NoError(t, err)
	require.Len(t, got.Components, 1)
	assert.Equal(t, []AcceleratorBaseRequest{{Name: "cpu", Quantity: "3"}}, got.Components[0].Requests)
}

func TestResolveAcceleratorBaseSupportsDecoderAndCanonicalizesRequests(t *testing.T) {
	isvc := acceleratorISVCFixture()
	isvc.Spec.Engine = nil
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{Runner: &v1beta1.RunnerSpec{}}
	activeDecoder := isvc.Spec.Decoder.DeepCopy()
	activeDecoder.Runner.Resources.Requests = corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("8192Mi"),
	}
	state := acceleratorRuntimeState(isvc, &ActiveConfiguration{
		Origin: ConfigurationOriginLiveRuntime, RuntimeName: "runtime",
		RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod",
		Consistency: RevisionConsistencyUnknown, spec: &v1beta1.ServingRuntimeSpec{},
		components: []EffectiveComponent{{Type: v1beta1.DecoderComponent, decoder: activeDecoder}},
	})

	got, err := ResolveAcceleratorBase(isvc, state)

	require.NoError(t, err)
	require.Len(t, got.Components, 1)
	assert.Equal(t, v1beta1.DecoderComponent, got.Components[0].Type)
	assert.Equal(t, []AcceleratorBaseRequest{{Name: "memory", Quantity: "8Gi"}},
		got.Components[0].Requests)
}

func TestResolveAcceleratorBaseMarksUnsafeRequestsInvalid(t *testing.T) {
	for _, tt := range []struct {
		name     string
		requests corev1.ResourceList
	}{
		{name: "negative", requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("-1"),
		}},
		{name: "too many", requests: func() corev1.ResourceList {
			result := corev1.ResourceList{}
			for i := 0; i < 65; i++ {
				result[corev1.ResourceName(fmt.Sprintf("example.com/gpu-%02d", i))] = resource.MustParse("1")
			}
			return result
		}()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isvc := acceleratorISVCFixture()
			activeEngine := isvc.Spec.Engine.DeepCopy()
			activeEngine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{
				Resources: corev1.ResourceRequirements{Requests: tt.requests},
			}}
			state := acceleratorRuntimeState(isvc, &ActiveConfiguration{
				Origin: ConfigurationOriginLiveRuntime, RuntimeName: "runtime",
				RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod",
				Consistency: RevisionConsistencyUnknown, spec: &v1beta1.ServingRuntimeSpec{},
				components: []EffectiveComponent{{Type: v1beta1.EngineComponent, engine: activeEngine}},
			})

			got, err := ResolveAcceleratorBase(isvc, state)

			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			assert.Equal(t, AcceleratorBaseInvalid, got.Components[0].State)
			assert.Empty(t, got.Components[0].Requests)
		})
	}
}

func TestResolveAcceleratorBaseKeepsMissingActiveConfigurationUnavailable(t *testing.T) {
	isvc := acceleratorISVCFixture()
	state := acceleratorRuntimeState(isvc, nil)

	got, err := ResolveAcceleratorBase(isvc, state)

	require.NoError(t, err)
	assert.Equal(t, AcceleratorActiveUnavailable, got.ActiveState)
	require.Len(t, got.Components, 1)
	assert.Equal(t, AcceleratorBaseUnavailable, got.Components[0].State)
}

func TestAcceleratorBaseResolutionCannotSerializePrivateEvidence(t *testing.T) {
	value := AcceleratorBaseResolution{}

	_, err := json.Marshal(value)
	require.ErrorIs(t, err, ErrUnsafeRuntimeSerialization)
	_, err = value.MarshalYAML()
	require.ErrorIs(t, err, ErrUnsafeRuntimeSerialization)
	assert.Equal(t, "<effective.AcceleratorBaseResolution redacted>", value.String())
	assert.Equal(t, "<effective.AcceleratorBaseResolution redacted>", value.GoString())
}

func TestResolveAcceleratorBaseFailsClosedForInconsistentRevision(t *testing.T) {
	isvc := acceleratorISVCFixture()
	state := acceleratorRuntimeState(isvc, &ActiveConfiguration{
		Origin: ConfigurationOriginControllerRevision, RuntimeName: "runtime",
		RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod", RevisionName: "revision",
		Consistency: RevisionConsistencyInconsistent, spec: &v1beta1.ServingRuntimeSpec{},
		components: []EffectiveComponent{{Type: v1beta1.EngineComponent, engine: &v1beta1.EngineSpec{}}},
	})
	state.revisions = []RuntimeRevisionObservation{{
		Name: "revision", Namespace: "ome", UID: "revision-uid",
		ResourceVersion: "revision-rv", expectedName: "revision",
		expectedNamespace: "ome", objectReturned: true,
		roles: []RuntimeRevisionRole{RuntimeRevisionRoleActive},
	}}

	got, err := ResolveAcceleratorBase(isvc, state)

	require.NoError(t, err)
	assert.Equal(t, AcceleratorActiveInconsistent, got.ActiveState)
	require.Len(t, got.Components, 1)
	assert.Equal(t, AcceleratorBaseUnavailable, got.Components[0].State)
	assert.Empty(t, got.Components[0].Requests)
	assert.Equal(t, "ControllerRevision", got.ActiveSourceKind)
	assert.Equal(t, "revision", got.ActiveSourceName)
	assert.Equal(t, "revision", got.ActiveRevisionName)
	assert.Equal(t, "revision-uid", got.ActiveSourceUID)
}

func TestResolveAcceleratorBaseRejectsUnboundActiveSource(t *testing.T) {
	isvc := acceleratorISVCFixture()
	state := acceleratorRuntimeState(isvc, &ActiveConfiguration{
		Origin: ConfigurationOriginLiveRuntime, RuntimeName: "runtime",
		RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod",
		Consistency: RevisionConsistencyUnknown, spec: &v1beta1.ServingRuntimeSpec{},
		components: []EffectiveComponent{{Type: v1beta1.EngineComponent, engine: &v1beta1.EngineSpec{}}},
	})
	state.live.Runtime.UID = ""

	_, err := ResolveAcceleratorBase(isvc, state)

	require.ErrorIs(t, err, ErrAcceleratorEvidenceInvalid)
}

func TestResolveAcceleratorBaseRejectsUnboundOrMismatchedEvidence(t *testing.T) {
	isvc := acceleratorISVCFixture()
	valid := acceleratorRuntimeState(isvc, &ActiveConfiguration{
		Origin: ConfigurationOriginLiveRuntime, RuntimeName: "runtime",
		RuntimeKind: "ServingRuntime", RuntimeNamespace: "prod",
		Consistency: RevisionConsistencyUnknown, spec: &v1beta1.ServingRuntimeSpec{},
		components: []EffectiveComponent{{Type: v1beta1.EngineComponent, engine: &v1beta1.EngineSpec{}}},
	})

	tests := []struct {
		name  string
		isvc  *v1beta1.InferenceService
		state *RuntimeState
	}{
		{name: "nil service", state: valid},
		{name: "nil state", isvc: isvc},
		{name: "snapshot mismatch", isvc: func() *v1beta1.InferenceService {
			copy := isvc.DeepCopy()
			copy.ResourceVersion = "different"
			return copy
		}(), state: valid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ResolveAcceleratorBase(tt.isvc, tt.state)
			require.ErrorIs(t, err, ErrAcceleratorEvidenceInvalid)
		})
	}
}

func TestResolveVirtualAcceleratorBaseDoesNotRequireRuntimeEvidence(t *testing.T) {
	isvc := acceleratorISVCFixture()
	isvc.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.VirtualDeployment),
	}
	isvc.Status.ObservedGeneration = 0

	got, err := ResolveVirtualAcceleratorBase(isvc)

	require.NoError(t, err)
	assert.Equal(t, StatusFreshnessUnknown, got.StatusFreshness)
	assert.Equal(t, AcceleratorActiveUnavailable, got.ActiveState)
	assert.Empty(t, got.RuntimeName)
	require.Len(t, got.Components, 1)
	assert.Equal(t, AcceleratorBaseUnavailable, got.Components[0].State)
	assert.Empty(t, got.Components[0].Requests)
}

func TestResolveVirtualAcceleratorBaseRejectsNonVirtualOrUnboundService(t *testing.T) {
	isvc := acceleratorISVCFixture()

	_, err := ResolveVirtualAcceleratorBase(isvc)
	require.ErrorIs(t, err, ErrAcceleratorEvidenceInvalid)

	isvc.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.VirtualDeployment),
	}
	isvc.UID = ""
	_, err = ResolveVirtualAcceleratorBase(isvc)
	require.ErrorIs(t, err, ErrAcceleratorEvidenceInvalid)
}

func acceleratorISVCFixture() *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "prod", UID: types.UID("isvc-uid"),
			ResourceVersion: "17", Generation: 4,
		},
		Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
	isvc.Status.ObservedGeneration = 4
	return isvc
}

func acceleratorRuntimeState(isvc *v1beta1.InferenceService, active *ActiveConfiguration) *RuntimeState {
	state := &RuntimeState{
		Generation: isvc.Generation, ObservedGeneration: isvc.Status.ObservedGeneration,
		StatusFreshness: StatusFreshnessCurrent, active: active,
		inferenceService: inferenceServiceBinding{
			identity: InferenceServiceIdentity{
				Name: isvc.Name, Namespace: isvc.Namespace, UID: string(isvc.UID),
			},
			resourceVersion: isvc.ResourceVersion,
		},
	}
	if active != nil && active.Origin == ConfigurationOriginLiveRuntime {
		state.live = &LiveConfiguration{Runtime: RuntimeResolution{
			Name: active.RuntimeName, Kind: active.RuntimeKind,
			Namespace: active.RuntimeNamespace, UID: "runtime-uid",
			Generation: 2, IdentityObserved: true,
			resourceVersion: "runtime-rv", spec: active.spec.DeepCopy(),
		}, Components: cloneEffectiveComponents(active.components)}
	}
	return state
}
