package effective

import (
	"context"
	"errors"
	"strings"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestResolveAutoscalingPrecedenceAndLegacyScope(t *testing.T) {
	tests := []struct {
		name           string
		mode           constants.DeploymentModeType
		isvcAutoscaler *v1beta1.ComponentAutoscaler
		policyRef      *v1beta1.AutoscalerPolicyRef
		runtimeScaler  *v1beta1.ComponentAutoscaler
		isvcAnn        map[string]string
		componentAnn   map[string]string
		wantState      AutoscalingComponentState
		wantClass      v1beta1.AutoscalerClass
		wantSource     AutoscalingSpecSource
		wantMetrics    int
		wantTriggers   int
	}{
		{
			name: "inline wins over policy and runtime", mode: constants.RawDeployment,
			isvcAutoscaler: hpaAutoscaler(), policyRef: &v1beta1.AutoscalerPolicyRef{Name: "fleet"},
			runtimeScaler: kedaAutoscaler("prometheus"),
			wantState:     AutoscalingComponentAvailable, wantClass: v1beta1.AutoscalerHPA,
			wantSource: AutoscalingSpecSourceISVC, wantMetrics: 1,
		},
		{
			name: "policy is explicitly unavailable without controller configuration", mode: constants.OMENative,
			policyRef: &v1beta1.AutoscalerPolicyRef{Name: "fleet"}, runtimeScaler: hpaAutoscaler(),
			wantState: AutoscalingComponentUnavailable, wantSource: AutoscalingSpecSourcePolicy,
		},
		{
			name: "runtime wins over raw legacy", mode: constants.RawDeployment,
			runtimeScaler: hpaAutoscaler(),
			isvcAnn:       map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassKEDA)},
			wantState:     AutoscalingComponentAvailable, wantClass: v1beta1.AutoscalerHPA,
			wantSource: AutoscalingSpecSourceRuntime, wantMetrics: 1,
		},
		{
			name: "component legacy class overrides top level", mode: constants.RawDeployment,
			isvcAnn:      map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassExternal), "secret.example/token": "do-not-copy"},
			componentAnn: map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassHPA), constants.TargetUtilizationPercentage: "42", "secret.example/component": "do-not-copy"},
			wantState:    AutoscalingComponentAvailable, wantClass: v1beta1.AutoscalerHPA,
			wantSource: AutoscalingSpecSourceLegacy, wantMetrics: 1,
		},
		{
			name: "default is HPA", mode: constants.RawDeployment,
			wantState: AutoscalingComponentAvailable, wantClass: v1beta1.AutoscalerHPA,
			wantSource: AutoscalingSpecSourceDefault, wantMetrics: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(tt.mode)
			isvc.Annotations = tt.isvcAnn
			isvc.Spec.Engine.Autoscaler = tt.isvcAutoscaler
			isvc.Spec.Engine.AutoscalerPolicyRef = tt.policyRef
			isvc.Spec.Engine.Annotations = tt.componentAnn
			runtimeSpec := &v1beta1.ServingRuntimeSpec{
				EngineConfig: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: tt.runtimeScaler}},
			}
			state := autoscaleRuntimeState(t, isvc, runtimeSpec)

			got, err := ResolveAutoscaling(isvc, state)
			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			component := got.Components[0]
			assert.Equal(t, tt.wantState, component.State)
			assert.Equal(t, tt.wantClass, component.Class)
			assert.Equal(t, tt.wantSource, component.SpecSource)
			assert.Equal(t, tt.wantMetrics, component.MetricCount)
			assert.Equal(t, tt.wantTriggers, component.TriggerCount)
			if tt.wantSource == AutoscalingSpecSourcePolicy {
				assert.Equal(t, ScaleToZeroUnavailable, component.ScaleToZero)
			}
		})
	}
}

func TestResolveAutoscalingFailsClosedForInvalidLegacyAdmissionState(t *testing.T) {
	const secret = "legacy-value-with-user-secret"
	tests := []struct {
		name        string
		mode        constants.DeploymentModeType
		annotations map[string]string
		autoscaler  *v1beta1.ComponentAutoscaler
		runtime     *v1beta1.ComponentAutoscaler
	}{
		{
			name: "top-level legacy class conflicts with inline block", mode: constants.RawDeployment,
			annotations: map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassKEDA)},
			autoscaler:  hpaAutoscaler(),
		},
		{
			name: "malformed target utilization is invalid even when runtime wins", mode: constants.RawDeployment,
			annotations: map[string]string{constants.TargetUtilizationPercentage: secret},
			runtime:     hpaAutoscaler(),
		},
		{
			name: "unknown legacy class is invalid even outside Raw dispatch", mode: constants.OMENative,
			annotations: map[string]string{constants.AutoscalerClass: secret},
			autoscaler:  hpaAutoscaler(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := autoscaleISVC(test.mode)
			isvc.Annotations = test.annotations
			isvc.Spec.Engine.Autoscaler = test.autoscaler
			runtimeSpec := baseAutoscaleRuntime()
			runtimeSpec.EngineConfig.Autoscaler = test.runtime
			state := autoscaleRuntimeState(t, isvc, runtimeSpec)

			got, err := ResolveAutoscaling(isvc, state)

			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			component := got.Components[0]
			assert.Equal(t, AutoscalingComponentInvalid, component.State)
			assert.Empty(t, component.Class)
			assert.Empty(t, component.ManagedBy)
			assert.Empty(t, component.SpecSource)
			assert.Nil(t, component.Target)
			assert.Equal(t, AutoscalingBoundsInvalid, component.Bounds.State)
			assert.Equal(t, ScaleToZeroInvalid, component.ScaleToZero)
			assert.Equal(t, []AutoscalingIssueCode{AutoscalingIssueLegacyAutoscalerInvalid}, component.Issues)
			assert.NotContains(t, got.String(), secret)
		})
	}
}

func TestResolveAutoscalingDispatchValidationAndOppositeBlocks(t *testing.T) {
	tests := []struct {
		name       string
		mode       constants.DeploymentModeType
		autoscaler *v1beta1.ComponentAutoscaler
		policyRef  *v1beta1.AutoscalerPolicyRef
		wantState  AutoscalingComponentState
		wantIssue  AutoscalingIssueCode
	}{
		{
			name: "HPA ignores unused KEDA payload", mode: constants.RawDeployment,
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA, Keda: &v1beta1.KedaAutoscaler{}},
			wantState:  AutoscalingComponentAvailable,
		},
		{
			name: "HPA accepts defaultable stabilization-only behavior", mode: constants.RawDeployment,
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerHPA,
				HPA: &v1beta1.HPAAutoscaler{Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
					ScaleDown: &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: ptr.To[int32](60)},
				}},
			},
			wantState: AutoscalingComponentAvailable,
		},
		{
			name: "KEDA ignores unused malformed HPA payload", mode: constants.OMENative,
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda:  &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{Type: "prometheus"}}},
				HPA:   &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType}}},
			},
			wantState: AutoscalingComponentAvailable,
		},
		{
			name: "KEDA accepts defaultable stabilization-only HPA behavior", mode: constants.RawDeployment,
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers: []kedav1.ScaleTriggers{{Type: "prometheus"}},
					Advanced: &kedav1.AdvancedConfig{HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{
						Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
							ScaleDown: &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: ptr.To[int32](60)},
						},
					}},
				},
			},
			wantState: AutoscalingComponentAvailable,
		},
		{
			name: "KEDA requires triggers", mode: constants.RawDeployment,
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: &v1beta1.KedaAutoscaler{}},
			wantState:  AutoscalingComponentInvalid, wantIssue: AutoscalingIssueKEDATriggersRequired,
		},
		{
			name: "HPA metrics are class selectively validated", mode: constants.RawDeployment,
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerHPA,
				HPA:   &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType}}},
			},
			wantState: AutoscalingComponentInvalid, wantIssue: AutoscalingIssueHPAMetricMalformed,
		},
		{
			name: "KEDA derived HPA name cannot collide with reserved scaler name", mode: constants.RawDeployment,
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers: []kedav1.ScaleTriggers{{Type: "prometheus"}},
					Advanced: &kedav1.AdvancedConfig{HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{Name: "svc-engine"}},
				},
			},
			wantState: AutoscalingComponentInvalid, wantIssue: AutoscalingIssueReservedHPANameCollision,
		},
		{
			name: "MultiNode has no autoscaler dispatch", mode: constants.MultiNode,
			autoscaler: hpaAutoscaler(), wantState: AutoscalingComponentUnsupported,
			wantIssue: AutoscalingIssueDeploymentModeUnsupported,
		},
		{
			name: "VirtualDeployment ignores an unused policy block", mode: constants.VirtualDeployment,
			policyRef: &v1beta1.AutoscalerPolicyRef{Name: "secret-policy"},
			wantState: AutoscalingComponentUnsupported, wantIssue: AutoscalingIssueDeploymentModeUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(tt.mode)
			isvc.Spec.Engine.Autoscaler = tt.autoscaler
			isvc.Spec.Engine.AutoscalerPolicyRef = tt.policyRef
			state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())
			got, err := ResolveAutoscaling(isvc, state)
			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			assert.Equal(t, tt.wantState, got.Components[0].State)
			if tt.wantIssue != "" {
				assert.Contains(t, got.Components[0].Issues, tt.wantIssue)
			}
		})
	}
}

func TestResolveAutoscalingHonorsServiceVirtualDeploymentEarlyExit(t *testing.T) {
	isvc := autoscaleISVC(constants.RawDeployment)
	isvc.Spec.DeploymentMode = nil
	isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)}
	isvc.Spec.Engine.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
	state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())

	got, err := ResolveAutoscaling(isvc, state)

	require.NoError(t, err)
	require.Len(t, got.Components, 1)
	component := got.Components[0]
	assert.Equal(t, constants.VirtualDeployment, component.DeploymentMode)
	assert.Equal(t, DeploymentModeServiceAnnotation, component.DeploymentModeSource)
	assert.Equal(t, AutoscalingComponentUnsupported, component.State)
	assert.Nil(t, component.Target)
	assert.Equal(t, AutoscalingBoundsUnavailable, component.Bounds.State)
	assert.Contains(t, component.Issues, AutoscalingIssueDeploymentModeUnsupported)
}

func TestResolveVirtualAutoscalingPreservesAdmissionFailures(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*v1beta1.InferenceService)
		wantIssue AutoscalingIssueCode
	}{
		{
			name: "invalid KEDA shape",
			configure: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA}
			},
			wantIssue: AutoscalingIssueKEDATriggersRequired,
		},
		{
			name: "reserved policy ref even when inline shadows it",
			configure: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Autoscaler = hpaAutoscaler()
				isvc.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{
					Name: "fleet", Kind: "ClusterAutoscalerPolicy",
				}
			},
			wantIssue: AutoscalingIssuePolicyReferenceInvalid,
		},
		{
			name: "invalid legacy configuration",
			configure: func(isvc *v1beta1.InferenceService) {
				isvc.Annotations[constants.TargetUtilizationPercentage] = "secret-malformed-value"
			},
			wantIssue: AutoscalingIssueLegacyAutoscalerInvalid,
		},
		{
			name: "invalid replica bounds",
			configure: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinReplicas = ptr.To(3)
				isvc.Spec.Engine.MaxReplicas = 2
			},
			wantIssue: AutoscalingIssueReplicaBoundsInvalid,
		},
		{
			name: "invalid scale to zero gate",
			configure: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinReplicas = ptr.To(0)
				isvc.Spec.Engine.Autoscaler = hpaAutoscaler()
			},
			wantIssue: AutoscalingIssueScaleToZeroInvalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(constants.RawDeployment)
			isvc.Spec.DeploymentMode = nil
			isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)}
			tt.configure(isvc)

			got, err := ResolveVirtualAutoscaling(isvc)

			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			component := got.Components[0]
			assert.Equal(t, AutoscalingComponentInvalid, component.State)
			assert.Equal(t, AutoscalingBoundsInvalid, component.Bounds.State)
			assert.Equal(t, ScaleToZeroInvalid, component.ScaleToZero)
			assert.Contains(t, component.Issues, tt.wantIssue)
		})
	}
}

func TestResolveAutoscalingRejectsShadowedInvalidRuntimeAutoscalerConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*v1beta1.EngineSpec)
		wantIssue AutoscalingIssueCode
	}{
		{
			name: "invalid KEDA block",
			configure: func(engine *v1beta1.EngineSpec) {
				engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA}
			},
			wantIssue: AutoscalingIssueKEDATriggersRequired,
		},
		{
			name: "forbidden runtime policy ref",
			configure: func(engine *v1beta1.EngineSpec) {
				engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{Name: "runtime-policy"}
			},
			wantIssue: AutoscalingIssuePolicyReferenceInvalid,
		},
		{
			name: "runtime-local KEDA idle is not below its minimum",
			configure: func(engine *v1beta1.EngineSpec) {
				engine.MinReplicas = ptr.To(1)
				engine.Autoscaler = kedaAutoscalerWithIdle(1)
			},
			wantIssue: AutoscalingIssueKEDAIdleNotBelowMinimum,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(constants.RawDeployment)
			isvc.Spec.Engine.Autoscaler = hpaAutoscaler()
			runtimeSpec := baseAutoscaleRuntime()
			tt.configure(runtimeSpec.EngineConfig)
			state := autoscaleRuntimeState(t, isvc, runtimeSpec)

			got, err := ResolveAutoscaling(isvc, state)

			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			component := got.Components[0]
			assert.Equal(t, AutoscalingComponentInvalid, component.State)
			assert.Equal(t, AutoscalingSpecSourceRuntime, component.SpecSource)
			assert.Contains(t, component.Issues, tt.wantIssue)
		})
	}
}

func TestResolveAutoscalingIgnoresShadowedRuntimeChildPayload(t *testing.T) {
	isvc := autoscaleISVC(constants.RawDeployment)
	isvc.Spec.Engine.Autoscaler = hpaAutoscaler()
	runtimeSpec := baseAutoscaleRuntime()
	runtimeSpec.EngineConfig.Autoscaler = &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{
			{Type: "prometheus", Name: "duplicate"},
			{Type: "cron", Name: "duplicate"},
		}},
	}
	state := autoscaleRuntimeState(t, isvc, runtimeSpec)

	got, err := ResolveAutoscaling(isvc, state)

	require.NoError(t, err)
	require.Len(t, got.Components, 1)
	component := got.Components[0]
	assert.Equal(t, AutoscalingComponentAvailable, component.State)
	assert.Equal(t, AutoscalingSpecSourceISVC, component.SpecSource)
	assert.Empty(t, component.Issues)
}

func TestResolveAutoscalingRejectsChildAdmissionInvalidPayloads(t *testing.T) {
	zero := int32(0)
	negative := int32(-1)
	fifty := int32(50)
	tests := []struct {
		name        string
		serviceName string
		minReplicas *int
		autoscaler  *v1beta1.ComponentAutoscaler
		wantIssue   AutoscalingIssueCode
	}{
		{
			name: "HPA utilization target must be positive",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerHPA,
				HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "cpu",
						Target: autoscalingv2.MetricTarget{
							Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &zero,
						},
					},
				}}},
			},
			wantIssue: AutoscalingIssueHPAMetricMalformed,
		},
		{
			name: "HPA explicitly empty scaling policies remain invalid",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerHPA,
				HPA: &v1beta1.HPAAutoscaler{Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
					ScaleDown: &autoscalingv2.HPAScalingRules{Policies: []autoscalingv2.HPAScalingPolicy{}},
				}},
			},
			wantIssue: AutoscalingIssueHPAMetricMalformed,
		},
		{
			name: "KEDA polling interval must be positive",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers:        []kedav1.ScaleTriggers{{Type: "prometheus"}},
					PollingInterval: &zero,
				},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name:        "KEDA CPU-only scaler cannot activate from zero",
			minReplicas: ptr.To(0),
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda:  &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{Type: "cpu"}}},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "KEDA cooldown period must not be negative",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers:       []kedav1.ScaleTriggers{{Type: "prometheus"}},
					CooldownPeriod: &negative,
				},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "KEDA idle replica count must not be negative",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers:         []kedav1.ScaleTriggers{{Type: "prometheus"}},
					IdleReplicaCount: &negative,
				},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "KEDA fallback rejects CPU-only triggers",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers: []kedav1.ScaleTriggers{{Type: "cpu"}},
					Fallback: &kedav1.Fallback{FailureThreshold: 3, Replicas: 2},
				},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "KEDA scaling modifier formula must compile",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers: []kedav1.ScaleTriggers{{Type: "prometheus", Name: "load"}},
					Advanced: &kedav1.AdvancedConfig{ScalingModifiers: kedav1.ScalingModifiers{
						Formula: "load +", Target: "1",
					}},
				},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "KEDA scaling modifier activation target must be finite numeric data",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers: []kedav1.ScaleTriggers{{Type: "prometheus", Name: "load"}},
					Advanced: &kedav1.AdvancedConfig{ScalingModifiers: kedav1.ScalingModifiers{
						Formula: "load", Target: "1", ActivationTarget: "not-a-number",
					}},
				},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name:        "KEDA generated HPA name must fit its label contract",
			serviceName: strings.Repeat("a", 42),
			autoscaler:  kedaAutoscaler("prometheus"),
			wantIssue:   AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "KEDA custom HPA name must be a valid Kubernetes name",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers: []kedav1.ScaleTriggers{{Type: "prometheus"}},
					Advanced: &kedav1.AdvancedConfig{HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{
						Name: "Not_A_Name",
					}},
				},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "KEDA advanced HPA behavior must pass HPA admission",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers: []kedav1.ScaleTriggers{{Type: "prometheus"}},
					Advanced: &kedav1.AdvancedConfig{HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{
						Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
							ScaleUp: &autoscalingv2.HPAScalingRules{Policies: []autoscalingv2.HPAScalingPolicy{{
								Type: "Bogus", Value: 0, PeriodSeconds: 0,
							}}},
						},
					}},
				},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "KEDA trigger names must be unique",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{
					{Type: "prometheus", Name: "duplicate"},
					{Type: "cron", Name: "duplicate"},
				}},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
		{
			name: "HPA container resource name must be standard or extended",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerHPA,
				HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
					Type: autoscalingv2.ContainerResourceMetricSourceType,
					ContainerResource: &autoscalingv2.ContainerResourceMetricSource{
						Name: corev1.ResourceName("bogus"), Container: "main",
						Target: autoscalingv2.MetricTarget{
							Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &fifty,
						},
					},
				}}},
			},
			wantIssue: AutoscalingIssueHPAMetricMalformed,
		},
		{
			name: "KEDA cached metrics reject CPU triggers",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{
					Type: "cpu", UseCachedMetrics: true,
				}}},
			},
			wantIssue: AutoscalingIssueCode("KEDAConfigurationInvalid"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(constants.RawDeployment)
			if tt.serviceName != "" {
				isvc.Name = tt.serviceName
			}
			if tt.minReplicas != nil {
				isvc.Spec.Engine.MinReplicas = tt.minReplicas
			}
			isvc.Spec.Engine.Autoscaler = tt.autoscaler
			state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())

			got, err := ResolveAutoscaling(isvc, state)

			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			component := got.Components[0]
			assert.Equal(t, AutoscalingComponentInvalid, component.State)
			assert.Nil(t, component.Target)
			assert.Contains(t, component.Issues, tt.wantIssue)
		})
	}
}

func TestResolveAutoscalingReplicaBoundsAndScaleToZero(t *testing.T) {
	tests := []struct {
		name          string
		mode          constants.DeploymentModeType
		min           *int
		max           int
		autoscaler    *v1beta1.ComponentAutoscaler
		runtimeScaler *v1beta1.ComponentAutoscaler
		legacyClass   constants.AutoscalerClassType
		wantState     AutoscalingComponentState
		wantBounds    AutoscalingBoundsState
		wantMin       *int32
		wantMax       *int32
		wantScaleZero ScaleToZeroState
		wantIssue     AutoscalingIssueCode
	}{
		{
			name: "Raw typed KEDA may scale from zero", mode: constants.RawDeployment,
			min: ptr.To(0), autoscaler: kedaAutoscaler("prometheus"),
			wantState: AutoscalingComponentAvailable, wantBounds: AutoscalingBoundsAvailable,
			wantMin: ptr.To[int32](0), wantMax: ptr.To[int32](1), wantScaleZero: ScaleToZeroEligible,
		},
		{
			name: "Raw legacy KEDA zero is dispatch-ineligible", mode: constants.RawDeployment,
			min: ptr.To(0), legacyClass: constants.AutoscalerClassKEDA,
			wantState: AutoscalingComponentInvalid, wantBounds: AutoscalingBoundsInvalid,
			wantScaleZero: ScaleToZeroInvalid,
		},
		{
			name: "OMENative authored zero is floored and unsupported", mode: constants.OMENative,
			min: ptr.To(0), autoscaler: kedaAutoscaler("prometheus"),
			wantState: AutoscalingComponentAvailable, wantBounds: AutoscalingBoundsAvailable,
			wantMin: ptr.To[int32](1), wantMax: ptr.To[int32](1), wantScaleZero: ScaleToZeroUnsupported,
		},
		{
			name: "OMENative KEDA idle zero with positive minimum is eligible", mode: constants.OMENative,
			min: ptr.To(1), autoscaler: kedaAutoscalerWithIdle(0),
			wantState: AutoscalingComponentAvailable, wantBounds: AutoscalingBoundsAvailable,
			wantMin: ptr.To[int32](1), wantMax: ptr.To[int32](1), wantScaleZero: ScaleToZeroEligible,
		},
		{
			name: "OMENative validates KEDA idle against dispatched default minimum", mode: constants.OMENative,
			autoscaler: kedaAutoscalerWithIdle(1),
			wantState:  AutoscalingComponentInvalid, wantBounds: AutoscalingBoundsInvalid,
			wantScaleZero: ScaleToZeroInvalid, wantIssue: AutoscalingIssueKEDAIdleNotBelowMinimum,
		},
		{
			name: "OMENative cross-layer runtime idle zero remains eligible after minimum floor", mode: constants.OMENative,
			min: ptr.To(0), runtimeScaler: kedaAutoscalerWithIdle(0), legacyClass: constants.AutoscalerClassKEDA,
			wantState: AutoscalingComponentAvailable, wantBounds: AutoscalingBoundsAvailable,
			wantMin: ptr.To[int32](1), wantMax: ptr.To[int32](1), wantScaleZero: ScaleToZeroEligible,
		},
		{
			name: "OMENative inline idle zero with declared zero remains admission-invalid", mode: constants.OMENative,
			min: ptr.To(0), autoscaler: kedaAutoscalerWithIdle(0),
			wantState: AutoscalingComponentInvalid, wantBounds: AutoscalingBoundsInvalid,
			wantScaleZero: ScaleToZeroInvalid, wantIssue: AutoscalingIssueKEDAIdleNotBelowMinimum,
		},
		{
			name: "HPA authored zero fails admission gate", mode: constants.RawDeployment,
			min: ptr.To(0), autoscaler: hpaAutoscaler(),
			wantState: AutoscalingComponentInvalid, wantBounds: AutoscalingBoundsInvalid,
			wantScaleZero: ScaleToZeroInvalid,
		},
		{
			name: "negative minimum is invalid", mode: constants.OMENative,
			min: ptr.To(-1), autoscaler: hpaAutoscaler(),
			wantState: AutoscalingComponentInvalid, wantBounds: AutoscalingBoundsInvalid,
			wantScaleZero: ScaleToZeroInvalid,
		},
		{
			name: "inverted range is invalid", mode: constants.RawDeployment,
			min: ptr.To(3), max: 2, autoscaler: hpaAutoscaler(),
			wantState: AutoscalingComponentInvalid, wantBounds: AutoscalingBoundsInvalid,
			wantScaleZero: ScaleToZeroInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(tt.mode)
			isvc.Spec.Engine.MinReplicas = tt.min
			isvc.Spec.Engine.MaxReplicas = tt.max
			isvc.Spec.Engine.Autoscaler = tt.autoscaler
			if tt.legacyClass != "" {
				isvc.Annotations = map[string]string{constants.AutoscalerClass: string(tt.legacyClass)}
			}
			runtimeSpec := baseAutoscaleRuntime()
			runtimeSpec.EngineConfig.Autoscaler = tt.runtimeScaler
			state := autoscaleRuntimeState(t, isvc, runtimeSpec)
			got, err := ResolveAutoscaling(isvc, state)
			require.NoError(t, err)
			component := got.Components[0]
			assert.Equal(t, tt.wantState, component.State)
			assert.Equal(t, tt.wantBounds, component.Bounds.State)
			assert.Equal(t, tt.wantMin, component.Bounds.MinReplicas)
			assert.Equal(t, tt.wantMax, component.Bounds.MaxReplicas)
			assert.Equal(t, tt.wantScaleZero, component.ScaleToZero)
			if tt.wantIssue != "" {
				assert.Contains(t, component.Issues, tt.wantIssue)
			}
		})
	}
}

func TestResolveAutoscalingOMENativeRuntimeZeroIsFlooredWithoutServiceRequest(t *testing.T) {
	isvc := autoscaleISVC(constants.OMENative)
	runtimeSpec := baseAutoscaleRuntime()
	runtimeSpec.EngineConfig.MinReplicas = ptr.To(0)
	runtimeSpec.EngineConfig.Autoscaler = hpaAutoscaler()
	state := autoscaleRuntimeState(t, isvc, runtimeSpec)

	got, err := ResolveAutoscaling(isvc, state)

	require.NoError(t, err)
	require.Len(t, got.Components, 1)
	component := got.Components[0]
	assert.Equal(t, AutoscalingComponentAvailable, component.State)
	assert.Equal(t, AutoscalingBoundsAvailable, component.Bounds.State)
	assert.Equal(t, ptr.To[int32](1), component.Bounds.MinReplicas)
	assert.Equal(t, ptr.To[int32](1), component.Bounds.MaxReplicas)
	assert.Equal(t, ScaleToZeroNotRequested, component.ScaleToZero)
}

func TestResolveAutoscalingReplicaBoundsFollowAdmissionAndDispatchOrigins(t *testing.T) {
	tests := []struct {
		name       string
		mode       constants.DeploymentModeType
		isvcMin    *int
		isvcMax    int
		runtimeMin *int
		runtimeMax int
		wantState  AutoscalingComponentState
		wantMin    *int32
		wantMax    *int32
	}{
		{
			name: "OMENative floors negative runtime bounds", mode: constants.OMENative,
			runtimeMin: ptr.To(-1), runtimeMax: -1,
			wantState: AutoscalingComponentAvailable, wantMin: ptr.To[int32](1), wantMax: ptr.To[int32](1),
		},
		{
			name: "OMENative clamps inverted runtime bounds", mode: constants.OMENative,
			runtimeMin: ptr.To(3), runtimeMax: 2,
			wantState: AutoscalingComponentAvailable, wantMin: ptr.To[int32](3), wantMax: ptr.To[int32](3),
		},
		{
			name: "Raw rejects negative runtime bounds at dispatch", mode: constants.RawDeployment,
			runtimeMin: ptr.To(-1), runtimeMax: -1, wantState: AutoscalingComponentInvalid,
		},
		{
			name: "Raw rejects inverted runtime bounds at dispatch", mode: constants.RawDeployment,
			runtimeMin: ptr.To(3), runtimeMax: 2, wantState: AutoscalingComponentInvalid,
		},
		{
			name: "admission rejects negative ISVC bounds before OMENative dispatch", mode: constants.OMENative,
			isvcMin: ptr.To(-1), isvcMax: -1, wantState: AutoscalingComponentInvalid,
		},
		{
			name: "admission rejects inverted ISVC bounds before OMENative dispatch", mode: constants.OMENative,
			isvcMin: ptr.To(3), isvcMax: 2, wantState: AutoscalingComponentInvalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(tt.mode)
			isvc.Spec.Engine.MinReplicas = tt.isvcMin
			isvc.Spec.Engine.MaxReplicas = tt.isvcMax
			runtimeSpec := baseAutoscaleRuntime()
			runtimeSpec.EngineConfig.MinReplicas = tt.runtimeMin
			runtimeSpec.EngineConfig.MaxReplicas = tt.runtimeMax
			state := autoscaleRuntimeState(t, isvc, runtimeSpec)

			got, err := ResolveAutoscaling(isvc, state)

			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			component := got.Components[0]
			assert.Equal(t, tt.wantState, component.State)
			if tt.wantState == AutoscalingComponentAvailable {
				assert.Equal(t, AutoscalingBoundsAvailable, component.Bounds.State)
				assert.Equal(t, tt.wantMin, component.Bounds.MinReplicas)
				assert.Equal(t, tt.wantMax, component.Bounds.MaxReplicas)
			} else {
				assert.Equal(t, AutoscalingBoundsInvalid, component.Bounds.State)
				assert.Contains(t, component.Issues, AutoscalingIssueReplicaBoundsInvalid)
			}
		})
	}
}

func TestResolveAutoscalingValidatesInheritedRuntimeAgainstDispatchedBounds(t *testing.T) {
	parent := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "base", UID: "base-uid", Generation: 2, ResourceVersion: "20"},
		Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: ptr.To(1), Autoscaler: kedaAutoscalerWithIdle(0),
			},
		}},
	}
	child := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "child", Namespace: "workloads", UID: "child-uid", Generation: 3, ResourceVersion: "30",
			Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "base"},
		},
		Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0)},
		}},
	}
	isvc := autoscaleISVC(constants.OMENative)
	isvc.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: child.Name}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(parent, child).Build()
	live, err := NewRuntimeResolver(client).ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	state := &RuntimeState{
		Generation: isvc.Generation, ObservedGeneration: isvc.Status.ObservedGeneration,
		StatusFreshness: StatusFreshnessCurrent,
		inferenceService: inferenceServiceBinding{
			identity:        InferenceServiceIdentity{Name: isvc.Name, Namespace: isvc.Namespace, UID: string(isvc.UID)},
			resourceVersion: isvc.ResourceVersion,
		},
		live: live,
		active: &ActiveConfiguration{
			Origin: ConfigurationOriginLiveRuntime, RuntimeName: live.Runtime.Name,
			RuntimeKind: live.Runtime.Kind, RuntimeNamespace: live.Runtime.Namespace,
			Consistency: RevisionConsistencyConsistent,
			components:  live.Components, spec: live.Runtime.spec.DeepCopy(),
		},
	}

	got, err := ResolveAutoscaling(isvc, state)

	require.NoError(t, err)
	require.Len(t, got.Components, 1)
	component := got.Components[0]
	assert.Equal(t, AutoscalingComponentAvailable, component.State)
	assert.Equal(t, AutoscalingSpecSourceRuntime, component.SpecSource)
	assert.Equal(t, ptr.To[int32](1), component.Bounds.MinReplicas)
	assert.Equal(t, ScaleToZeroEligible, component.ScaleToZero)
}

func TestResolveAutoscalingValidatesDeclaredBoundsBeforePolicyAvailability(t *testing.T) {
	tests := []struct {
		name string
		min  *int
		max  int
	}{
		{name: "scale-to-zero admission gate", min: ptr.To(0)},
		{name: "replica bounds", min: ptr.To(3), max: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(constants.RawDeployment)
			isvc.Spec.Engine.MinReplicas = tt.min
			isvc.Spec.Engine.MaxReplicas = tt.max
			isvc.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{Name: "fleet"}
			state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())

			got, err := ResolveAutoscaling(isvc, state)
			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			assert.Equal(t, AutoscalingComponentInvalid, got.Components[0].State)
			assert.Equal(t, ScaleToZeroInvalid, got.Components[0].ScaleToZero)
		})
	}
}

func TestResolveAutoscalingRejectsReservedPolicyReferenceWithoutFetchingPayload(t *testing.T) {
	tests := []struct {
		name       string
		mode       constants.DeploymentModeType
		autoscaler *v1beta1.ComponentAutoscaler
	}{
		{name: "winning policy", mode: constants.RawDeployment},
		{name: "inline autoscaler cannot hide invalid ref", mode: constants.RawDeployment, autoscaler: hpaAutoscaler()},
		{name: "unsupported dispatch cannot hide invalid ref", mode: constants.VirtualDeployment},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(tt.mode)
			isvc.Spec.Engine.Autoscaler = tt.autoscaler
			isvc.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{
				Name: "fleet", Kind: "ClusterAutoscalerPolicy",
			}
			state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())

			got, err := ResolveAutoscaling(isvc, state)

			require.NoError(t, err)
			require.Len(t, got.Components, 1)
			component := got.Components[0]
			assert.Equal(t, AutoscalingComponentInvalid, component.State)
			assert.Equal(t, AutoscalingSpecSourcePolicy, component.SpecSource)
			assert.Contains(t, component.Issues, AutoscalingIssueCode("PolicyReferenceInvalid"))
			assert.Equal(t, ScaleToZeroUnavailable, component.ScaleToZero)
			assert.Nil(t, component.Target)
		})
	}
}

func TestResolveAutoscalingScalingPolicyPrecedenceAndSupport(t *testing.T) {
	tests := []struct {
		name          string
		isvcPolicy    *v1beta1.ScalingPolicy
		runtimePolicy *v1beta1.ScalingPolicy
		wantMode      v1beta1.ScalingMode
		wantSource    ScalingPolicySource
		wantState     ScalingPolicyState
	}{
		{name: "default independent", wantMode: v1beta1.ScalingIndependent, wantSource: ScalingPolicySourceDefault, wantState: ScalingPolicyAvailable},
		{name: "runtime independent", runtimePolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, wantMode: v1beta1.ScalingIndependent, wantSource: ScalingPolicySourceRuntime, wantState: ScalingPolicyAvailable},
		{name: "ISVC replaces runtime", isvcPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, runtimePolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, wantMode: v1beta1.ScalingIndependent, wantSource: ScalingPolicySourceISVC, wantState: ScalingPolicyAvailable},
		{name: "stored proportional is unsupported", isvcPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional}, wantMode: v1beta1.ScalingProportional, wantSource: ScalingPolicySourceISVC, wantState: ScalingPolicyUnsupported},
		{name: "unknown stored mode is invalid", isvcPolicy: &v1beta1.ScalingPolicy{Mode: "Elastic"}, wantMode: "Elastic", wantSource: ScalingPolicySourceISVC, wantState: ScalingPolicyInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(constants.RawDeployment)
			isvc.Spec.ScalingPolicy = tt.isvcPolicy
			runtimeSpec := baseAutoscaleRuntime()
			runtimeSpec.ScalingPolicy = tt.runtimePolicy
			state := autoscaleRuntimeState(t, isvc, runtimeSpec)

			got, err := ResolveAutoscaling(isvc, state)
			require.NoError(t, err)
			assert.Equal(t, tt.wantMode, got.ScalingPolicy.Mode)
			assert.Equal(t, tt.wantSource, got.ScalingPolicy.Source)
			assert.Equal(t, tt.wantState, got.ScalingPolicy.State)
		})
	}
}

func TestResolveAutoscalingRequiresBoundActiveState(t *testing.T) {
	isvc := autoscaleISVC(constants.RawDeployment)
	runtimeSpec := baseAutoscaleRuntime()
	state := autoscaleRuntimeState(t, isvc, runtimeSpec)

	mutated := isvc.DeepCopy()
	mutated.ResourceVersion = "other"
	_, err := ResolveAutoscaling(mutated, state)
	assert.ErrorIs(t, err, ErrAutoscalingEvidenceInvalid)

	state.active = nil
	_, err = ResolveAutoscaling(isvc, state)
	assert.ErrorIs(t, err, ErrActiveRuntimeUnavailable)

	_, err = ResolveAutoscaling(nil, state)
	assert.ErrorIs(t, err, ErrAutoscalingEvidenceInvalid)
}

func TestResolveAutoscalingActiveRevisionUsesExpectedBoundIdentity(t *testing.T) {
	isvc := autoscaleISVC(constants.RawDeployment)
	state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())
	state.active.Origin = ConfigurationOriginControllerRevision
	state.active.RevisionName = "expected-revision"
	state.active.Consistency = RevisionConsistencyInconsistent
	state.revisions = []RuntimeRevisionObservation{{
		Name: "hostile-returned-name", Namespace: "hostile-returned-namespace", UID: "hostile-returned-uid",
		expectedName: "expected-revision", expectedNamespace: "ome-system",
		roles: []RuntimeRevisionRole{RuntimeRevisionRoleRequested, RuntimeRevisionRoleActive},
	}}

	got, err := ResolveAutoscaling(isvc, state)

	require.NoError(t, err)
	require.NotNil(t, got.Active.Revision)
	assert.Equal(t, "expected-revision", got.Active.Revision.Name)
	assert.Equal(t, "ome-system", got.Active.Revision.Namespace)
	assert.Empty(t, got.Active.Revision.UID, "UID from a mismatched returned identity must not be rebound")
	assert.False(t, got.Active.Revision.IdentityObserved)
	assert.Equal(t, RuntimeRevisionRoleActive, got.Active.Revision.Role)
}

func TestResolveAutoscalingPinnedActiveDoesNotAttributeLiveInheritance(t *testing.T) {
	isvc := autoscaleISVC(constants.RawDeployment)
	state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())
	state.active.Origin = ConfigurationOriginControllerRevision
	state.active.RevisionName = "active-revision"
	state.active.Consistency = RevisionConsistencyConsistent
	state.revisions = []RuntimeRevisionObservation{{
		Name: "active-revision", Namespace: "ome", UID: "revision-uid",
		expectedName: "active-revision", expectedNamespace: "ome",
		roles: []RuntimeRevisionRole{RuntimeRevisionRoleActive},
	}}

	got, err := ResolveAutoscaling(isvc, state)

	require.NoError(t, err)
	assert.Equal(t, InheritanceNotRecorded, got.Active.Inheritance.State)
	assert.Empty(t, got.Active.Inheritance.UnavailableReason)
	assert.Empty(t, got.Active.Inheritance.Sources)
	assert.NotContains(t, got.Issues, AutoscalingIssueInheritanceUnavailable)
}

func TestResolveAutoscalingDoesNotClaimUnboundInheritanceSourcesObserved(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*RuntimeSourceReference)
	}{
		{name: "missing UID", mutate: func(source *RuntimeSourceReference) { source.UID = "" }},
		{name: "missing resourceVersion", mutate: func(source *RuntimeSourceReference) { source.ResourceVersion = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isvc := autoscaleISVC(constants.RawDeployment)
			state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())
			source := RuntimeSourceReference{
				Kind: "ServingRuntime", Namespace: isvc.Namespace, Name: "runtime",
				UID: "runtime-uid", Generation: 2, ResourceVersion: "29",
			}
			tt.mutate(&source)
			state.live.Runtime.DeclaredInheritance = observedInheritance([]RuntimeSourceReference{source})

			got, err := ResolveAutoscaling(isvc, state)

			require.NoError(t, err)
			assert.Equal(t, InheritanceUnavailable, got.Active.Inheritance.State)
			assert.Equal(t, InheritanceMalformed, got.Active.Inheritance.UnavailableReason)
			assert.Empty(t, got.Active.Inheritance.Sources)
			assert.Contains(t, got.Issues, AutoscalingIssueInheritanceUnavailable)
		})
	}
}

func TestResolveAutoscalingBindsLiveRuntimeIdentityOnlyToObservedSnapshot(t *testing.T) {
	isvc := autoscaleISVC(constants.RawDeployment)
	state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())
	state.live.Runtime.UID = "runtime-uid"
	state.live.Runtime.Generation = 7
	state.live.Runtime.IdentityObserved = true
	state.live.Runtime.resourceVersion = "19"

	got, err := ResolveAutoscaling(isvc, state)

	require.NoError(t, err)
	assert.True(t, got.Active.Runtime.IdentityObserved)
	assert.Equal(t, "runtime-uid", got.Active.Runtime.UID)
	assert.Equal(t, int64(7), got.Active.Runtime.Generation)

	state.active.Origin = ConfigurationOriginControllerRevision
	state.active.RevisionName = "active-revision"
	state.active.Consistency = RevisionConsistencyConsistent
	state.revisions = []RuntimeRevisionObservation{{
		Name: "active-revision", Namespace: "ome", expectedName: "active-revision", expectedNamespace: "ome",
		roles: []RuntimeRevisionRole{RuntimeRevisionRoleActive},
	}}
	got, err = ResolveAutoscaling(isvc, state)
	require.NoError(t, err)
	assert.False(t, got.Active.Runtime.IdentityObserved)
	assert.Empty(t, got.Active.Runtime.UID)
	assert.Zero(t, got.Active.Runtime.Generation)
}

func TestAutoscalingResolutionSerializationIsRejected(t *testing.T) {
	value := AutoscalingResolution{}
	_, err := value.MarshalJSON()
	assert.True(t, errors.Is(err, ErrUnsafeRuntimeSerialization))
	_, err = value.MarshalYAML()
	assert.True(t, errors.Is(err, ErrUnsafeRuntimeSerialization))
	assert.Equal(t, "<effective.AutoscalingResolution redacted>", value.String())
	assert.Equal(t, "<effective.AutoscalingResolution redacted>", value.GoString())
}

func TestResolveAutoscalingIncludesOnlyCausalAutoSelectionModel(t *testing.T) {
	isvc := autoscaleISVC(constants.RawDeployment)
	state := autoscaleRuntimeState(t, isvc, baseAutoscaleRuntime())
	state.live.Runtime.SelectionSource = RuntimeSelected
	state.live.Model = &ModelResolution{
		Name: "model", Kind: "BaseModel", Namespace: isvc.Namespace,
		UID: types.UID("model-uid"), Generation: 6,
	}

	selected, err := ResolveAutoscaling(isvc, state)
	require.NoError(t, err)
	require.NotNil(t, selected.Model)
	assert.Equal(t, AutoscalingModelReference{
		Kind: "BaseModel", Namespace: isvc.Namespace, Name: "model",
		UID: "model-uid", Generation: 6,
	}, *selected.Model)

	state.live.Runtime.SelectionSource = RuntimeExplicit
	explicit, err := ResolveAutoscaling(isvc, state)
	require.NoError(t, err)
	assert.Nil(t, explicit.Model)

	state.live.Runtime.SelectionSource = RuntimeSelected
	state.active.Origin = ConfigurationOriginControllerRevision
	pinned, err := ResolveAutoscaling(isvc, state)
	require.NoError(t, err)
	assert.Nil(t, pinned.Model, "a live model must not be attributed to pinned desired configuration")
}

func autoscaleISVC(mode constants.DeploymentModeType) *v1beta1.InferenceService {
	result := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "workloads", UID: types.UID("isvc-uid"),
			ResourceVersion: "17", Generation: 4,
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &v1beta1.EngineSpec{},
		},
	}
	result.Status.ObservedGeneration = 4
	return result
}

func baseAutoscaleRuntime() *v1beta1.ServingRuntimeSpec {
	return &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}
}

func autoscaleRuntimeState(t *testing.T, isvc *v1beta1.InferenceService, runtimeSpec *v1beta1.ServingRuntimeSpec) *RuntimeState {
	t.Helper()
	components, err := MergeEffectiveComponents(isvc, runtimeSpec)
	require.NoError(t, err)
	return &RuntimeState{
		Generation:         isvc.Generation,
		ObservedGeneration: isvc.Status.ObservedGeneration,
		StatusFreshness:    StatusFreshnessCurrent,
		inferenceService: inferenceServiceBinding{
			identity:        InferenceServiceIdentity{Name: isvc.Name, Namespace: isvc.Namespace, UID: string(isvc.UID)},
			resourceVersion: isvc.ResourceVersion,
		},
		live: &LiveConfiguration{Runtime: RuntimeResolution{
			Name: "runtime", Kind: "ServingRuntime", Namespace: isvc.Namespace,
			DeclaredInheritance: observedInheritance([]RuntimeSourceReference{{
				Kind: "ServingRuntime", Namespace: isvc.Namespace, Name: "runtime", UID: types.UID("runtime-uid"),
				Generation: 2, ResourceVersion: "29",
			}}),
		}},
		active: &ActiveConfiguration{
			Origin: ConfigurationOriginLiveRuntime, RuntimeName: "runtime", RuntimeKind: "ServingRuntime",
			RuntimeNamespace: isvc.Namespace, Consistency: RevisionConsistencyConsistent,
			components: components, spec: runtimeSpec.DeepCopy(),
		},
	}
}

func hpaAutoscaler() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
}

func kedaAutoscaler(triggerType string) *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda:  &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{Type: triggerType}}},
	}
}

func kedaAutoscalerWithIdle(idle int32) *v1beta1.ComponentAutoscaler {
	result := kedaAutoscaler("prometheus")
	result.Keda.IdleReplicaCount = &idle
	return result
}
