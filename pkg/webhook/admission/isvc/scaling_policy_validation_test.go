package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
	"sigs.k8s.io/ome/pkg/validation"
)

// scalingPolicyISVC declares both roles for a proportional policy.
func scalingPolicyISVC(policy *v1beta1.ScalingPolicy) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime:       &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
			ScalingPolicy: policy,
		},
	}
	if policy != nil && policy.Mode == v1beta1.ScalingProportional {
		native := constants.OMENative
		isvc.Spec.DeploymentMode = &native
		isvc.Spec.Engine = &v1beta1.EngineSpec{}
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	}
	return isvc
}

func scalingPolicyValidator(t *testing.T) *InferenceServiceValidator {
	t.Helper()
	c := fakeclient.New(t)
	return &InferenceServiceValidator{Client: c, Reader: c, RuntimeSelector: runtimeselector.New(c)}
}

func isvcProportionalPolicy(decoderRatio string) *v1beta1.ScalingPolicy {
	return &v1beta1.ScalingPolicy{
		Mode: v1beta1.ScalingProportional,
		Proportional: &v1beta1.ProportionalPolicy{
			Anchor: v1beta1.EngineComponent,
			Ratios: map[v1beta1.ComponentType]resource.Quantity{
				v1beta1.DecoderComponent: resource.MustParse(decoderRatio),
			},
		},
	}
}

func TestInferenceService_ScalingPolicyCreate(t *testing.T) {
	tests := []struct {
		name    string
		policy  *v1beta1.ScalingPolicy
		wantErr bool
	}{
		{name: "nil policy allowed", policy: nil},
		{name: "Independent allowed", policy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}},
		{name: "Proportional accepted", policy: isvcProportionalPolicy("1")},
		{name: "Pinned rejected", policy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := scalingPolicyValidator(t)
			_, err := v.ValidateCreate(context.Background(), scalingPolicyISVC(tt.policy))
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), validation.ReasonScalingModeNotImplemented)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestInferenceService_ScalingPolicyUpdate(t *testing.T) {
	tests := []struct {
		name      string
		oldPolicy *v1beta1.ScalingPolicy
		newPolicy *v1beta1.ScalingPolicy
		wantErr   bool
	}{
		{name: "nil to nil allowed", oldPolicy: nil, newPolicy: nil},
		{name: "nil to Independent allowed", oldPolicy: nil, newPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}},
		{name: "newly set Proportional accepted", oldPolicy: nil, newPolicy: isvcProportionalPolicy("1")},
		{name: "newly set Pinned rejected", oldPolicy: nil, newPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, wantErr: true},
		{name: "Independent to Proportional accepted", oldPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, newPolicy: isvcProportionalPolicy("1")},
		{name: "unchanged stored Proportional ratchets through", oldPolicy: isvcProportionalPolicy("1"), newPolicy: isvcProportionalPolicy("1")},
		{name: "unchanged stored Pinned ratchets through", oldPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, newPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}},
		{name: "changed Proportional ratio accepted", oldPolicy: isvcProportionalPolicy("1"), newPolicy: isvcProportionalPolicy("2")},
		{name: "Proportional to Independent allowed", oldPolicy: isvcProportionalPolicy("1"), newPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}},
		{name: "Proportional removed allowed", oldPolicy: isvcProportionalPolicy("1"), newPolicy: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := scalingPolicyValidator(t)
			_, err := v.ValidateUpdate(context.Background(), scalingPolicyISVC(tt.oldPolicy), scalingPolicyISVC(tt.newPolicy))
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), validation.ReasonScalingModeNotImplemented)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestInferenceService_ScalingPolicyRatchetAllowsUnrelatedUpdate pins the
// ratchet's purpose: a stored object carrying a rejected mode still accepts
// writes that do not touch the policy.
func TestInferenceService_ScalingPolicyRatchetAllowsUnrelatedUpdate(t *testing.T) {
	oldIsvc := scalingPolicyISVC(isvcProportionalPolicy("1"))
	newIsvc := scalingPolicyISVC(isvcProportionalPolicy("1"))
	newIsvc.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "another-runtime"}

	v := scalingPolicyValidator(t)
	_, err := v.ValidateUpdate(context.Background(), oldIsvc, newIsvc)
	require.NoError(t, err)
}

func TestInferenceService_ScalingPolicyRejectsFollowerAutoscalerPolicyRef(t *testing.T) {
	v := scalingPolicyValidator(t)
	v.AutoscalerPolicyEnabled = true
	old := scalingPolicyISVC(isvcProportionalPolicy("1"))
	withRef := old.DeepCopy()
	withRef.Spec.Decoder.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{Name: "decoder-keda"}
	_, err := v.ValidateCreate(context.Background(), withRef)
	require.ErrorContains(t, err, "decoder follower cannot reference AutoscalerPolicy \"decoder-keda\"")
	_, err = v.ValidateUpdate(context.Background(), old, withRef)
	require.ErrorContains(t, err, "decoder follower cannot reference AutoscalerPolicy \"decoder-keda\"")
}
