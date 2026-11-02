package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func proportionalPolicy(decoderRatio string) *v1beta1.ScalingPolicy {
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

func TestValidateScalingPolicy_Modes_AcceptsIndependentOrProportionalWithRatios(t *testing.T) {
	t.Run("nil policy is ok", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicy(nil, "spec.scalingPolicy"))
	})

	t.Run("empty mode is ok", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicy(&v1beta1.ScalingPolicy{}, "spec.scalingPolicy"))
	})

	t.Run("Independent is ok", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicy(
			&v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, "spec.scalingPolicy"))
	})

	t.Run("valid Proportional is accepted", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicy(proportionalPolicy("1"), "spec.scalingPolicy"))
	})

	t.Run("Proportional without ratios is rejected", func(t *testing.T) {
		err := ValidateScalingPolicy(
			&v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional}, "spec.scalingPolicy")
		require.ErrorContains(t, err, "proportional.ratios must contain a follower")
	})
	t.Run("Independent cannot carry inert proportional settings", func(t *testing.T) {
		policy := proportionalPolicy("1")
		policy.Mode = v1beta1.ScalingIndependent
		require.ErrorContains(t, ValidateScalingPolicy(policy, "spec.scalingPolicy"), "proportional requires mode")
	})
	for _, ratio := range []string{"0", "-1", "0.0001", "1001"} {
		t.Run("invalid ratio "+ratio, func(t *testing.T) {
			require.Error(t, ValidateScalingPolicy(proportionalPolicy(ratio), "spec.scalingPolicy"))
		})
	}
	t.Run("valid fractional ratio", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicy(proportionalPolicy("0.25"), "spec.scalingPolicy"))
	})

	t.Run("Pinned is rejected as not implemented", func(t *testing.T) {
		err := ValidateScalingPolicy(
			&v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, "spec.scalingPolicy")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Pinned")
		assert.Contains(t, err.Error(), "not implemented")
		assert.Contains(t, err.Error(), ReasonScalingModeNotImplemented)
	})

	t.Run("unknown mode is rejected", func(t *testing.T) {
		err := ValidateScalingPolicy(
			&v1beta1.ScalingPolicy{Mode: v1beta1.ScalingMode("Elastic")}, "spec.scalingPolicy")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Elastic")
		assert.Contains(t, err.Error(), "is not one of")
	})

	t.Run("fieldPath is reflected in the error", func(t *testing.T) {
		err := ValidateScalingPolicy(
			&v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, "spec.other.path")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spec.other.path.mode")
	})
}

func TestValidateScalingPolicyUpdate_ModeChanges_KeepsStoredRejectsNewUnsupported(t *testing.T) {
	t.Run("nil to nil is ok", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(nil, nil, "spec.scalingPolicy"))
	})

	t.Run("nil to Independent is ok", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(
			nil, &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, "spec.scalingPolicy"))
	})

	t.Run("newly set Proportional is accepted", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(nil, proportionalPolicy("1"), "spec.scalingPolicy"))
	})

	t.Run("newly set Pinned is rejected", func(t *testing.T) {
		err := ValidateScalingPolicyUpdate(
			nil, &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, "spec.scalingPolicy")
		require.Error(t, err)
		assert.Contains(t, err.Error(), ReasonScalingModeNotImplemented)
	})

	t.Run("Independent changed to Proportional is accepted", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(
			&v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent},
			proportionalPolicy("1"), "spec.scalingPolicy"))
	})

	t.Run("unchanged stored Proportional is ratcheted through", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(
			proportionalPolicy("1"), proportionalPolicy("1"), "spec.scalingPolicy"))
	})

	t.Run("unchanged stored Pinned is ratcheted through", func(t *testing.T) {
		pinned := &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}
		require.NoError(t, ValidateScalingPolicyUpdate(
			pinned, pinned.DeepCopy(), "spec.scalingPolicy"))
	})

	t.Run("semantically equal ratio quantities count as unchanged", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(
			proportionalPolicy("1"), proportionalPolicy("1000m"), "spec.scalingPolicy"))
	})

	t.Run("changed Proportional ratio is accepted", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(
			proportionalPolicy("1"), proportionalPolicy("2"), "spec.scalingPolicy"))
	})

	t.Run("Proportional changed to Pinned is rejected", func(t *testing.T) {
		err := ValidateScalingPolicyUpdate(
			proportionalPolicy("1"),
			&v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, "spec.scalingPolicy")
		require.Error(t, err)
		assert.Contains(t, err.Error(), ReasonScalingModeNotImplemented)
	})

	t.Run("Proportional downgraded to Independent is ok", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(
			proportionalPolicy("1"),
			&v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, "spec.scalingPolicy"))
	})

	t.Run("Proportional removed entirely is ok", func(t *testing.T) {
		require.NoError(t, ValidateScalingPolicyUpdate(
			proportionalPolicy("1"), nil, "spec.scalingPolicy"))
	})
}

func TestValidateScalingPolicyComponents_FollowerAutoscaler_RejectedUnlessNone(t *testing.T) {
	policy := proportionalPolicy("1")
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.DecoderComponent: {Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}},
	}
	require.ErrorContains(t, validateScalingPolicyComponents(policy, components), "follower autoscaler")
	components[v1beta1.DecoderComponent].Autoscaler.Class = v1beta1.AutoscalerNone
	require.NoError(t, validateScalingPolicyComponents(policy, components))
}

func TestValidateScalingPolicyService_FollowerPolicyRef_RejectedOnCreateAndUpdate(t *testing.T) {
	native := constants.OMENative
	service := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &native,
			Engine:         &v1beta1.EngineSpec{},
			Decoder:        &v1beta1.DecoderSpec{},
			ScalingPolicy:  proportionalPolicy("1"),
		},
	}
	ref := &v1beta1.AutoscalerPolicyRef{Name: "decoder-keda"}
	withRef := service.DeepCopy()
	withRef.Spec.Decoder.AutoscalerPolicyRef = ref
	for _, tc := range []struct {
		name    string
		service *v1beta1.InferenceService
	}{
		{"create with a follower ref", withRef},
		{"inline None does not make a follower ref safe", func() *v1beta1.InferenceService {
			copy := withRef.DeepCopy()
			copy.Spec.Decoder.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
			return copy
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateScalingPolicyService(tc.service)
			require.ErrorContains(t, err, "decoder follower cannot reference AutoscalerPolicy \"decoder-keda\"")
			require.ErrorContains(t, err, "remove the reference or remove decoder from ratios")
		})
	}
	t.Run("adding the ref revalidates ownership", func(t *testing.T) {
		require.ErrorContains(t, ValidateScalingPolicyServiceUpdate(service, withRef), "decoder follower cannot reference AutoscalerPolicy")
	})
	t.Run("changing a stored ref revalidates ownership", func(t *testing.T) {
		updated := withRef.DeepCopy()
		updated.Spec.Decoder.AutoscalerPolicyRef.Name = "other-policy"
		require.ErrorContains(t, ValidateScalingPolicyServiceUpdate(withRef, updated), "decoder follower cannot reference AutoscalerPolicy")
	})
	t.Run("an anchor ref remains valid", func(t *testing.T) {
		valid := service.DeepCopy()
		valid.Spec.Engine.AutoscalerPolicyRef = ref
		require.NoError(t, ValidateScalingPolicyService(valid))
	})
}

func TestValidateScalingPolicyService_Proportional_NeedsAnchorFollowerAndOMENative(t *testing.T) {
	native := constants.OMENative
	service := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &native,
			Engine:         &v1beta1.EngineSpec{},
			Decoder:        &v1beta1.DecoderSpec{},
			ScalingPolicy:  proportionalPolicy("0.25"),
		},
	}
	require.NoError(t, ValidateScalingPolicyService(service))
	runtime := &v1beta1.ServingRuntimeSpec{
		EngineConfig:  &v1beta1.EngineSpec{},
		DecoderConfig: &v1beta1.DecoderSpec{},
		ScalingPolicy: proportionalPolicy("1"),
	}
	require.NoError(t, ValidateScalingPolicyRuntime(runtime))

	t.Run("missing anchor", func(t *testing.T) {
		invalid := service.DeepCopy()
		invalid.Spec.Engine = nil
		require.ErrorContains(t, ValidateScalingPolicyService(invalid), "anchor")
	})
	t.Run("missing follower", func(t *testing.T) {
		invalid := service.DeepCopy()
		invalid.Spec.Decoder = nil
		require.ErrorContains(t, ValidateScalingPolicyService(invalid), "requires a declared component")
	})
	t.Run("referenced replica cannot be scaled", func(t *testing.T) {
		invalid := service.DeepCopy()
		invalid.Spec.ReplicaRefs = &v1beta1.ReplicaRefs{}
		require.ErrorContains(t, ValidateScalingPolicyService(invalid), "owned replicas")
	})
	t.Run("explicit RawDeployment is incompatible", func(t *testing.T) {
		invalid := service.DeepCopy()
		raw := constants.RawDeployment
		invalid.Spec.DeploymentMode = &raw
		require.ErrorContains(t, ValidateScalingPolicyService(invalid), "requires OMENative")
	})
	t.Run("component override cannot use another backend", func(t *testing.T) {
		invalid := service.DeepCopy()
		invalid.Spec.Decoder.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
		require.ErrorContains(t, ValidateScalingPolicyService(invalid), "requires OMENative decoder")
	})
	for _, class := range []v1beta1.AutoscalerClass{
		v1beta1.AutoscalerHPA, v1beta1.AutoscalerKEDA, v1beta1.AutoscalerExternal,
	} {
		t.Run("follower scaler "+string(class), func(t *testing.T) {
			invalid := service.DeepCopy()
			invalid.Spec.Decoder.Autoscaler = &v1beta1.ComponentAutoscaler{Class: class}
			require.ErrorContains(t, ValidateScalingPolicyService(invalid), "follower autoscaler")
		})
	}
	t.Run("explicit None is allowed", func(t *testing.T) {
		valid := service.DeepCopy()
		valid.Spec.Decoder.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
		require.NoError(t, ValidateScalingPolicyService(valid))
	})
	t.Run("runtime requires the follower", func(t *testing.T) {
		invalid := runtime.DeepCopy()
		invalid.DecoderConfig = nil
		require.ErrorContains(t, ValidateScalingPolicyRuntime(invalid), "requires a declared component")
	})
	t.Run("runtime forbids a competing follower", func(t *testing.T) {
		invalid := runtime.DeepCopy()
		invalid.DecoderConfig.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
		require.ErrorContains(t, ValidateScalingPolicyRuntime(invalid), "follower autoscaler")
	})
}

func TestValidateScalingPolicyServiceUpdate_Edits_RecheckOnlyWhatTheEditTouches(t *testing.T) {
	old := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{ScalingPolicy: proportionalPolicy("1")},
	}
	updated := old.DeepCopy()
	updated.Labels = map[string]string{"unrelated": "value"}
	require.NoError(t, ValidateScalingPolicyServiceUpdate(old, updated),
		"unrelated updates must not reject a stored policy with missing components")

	old.Spec.Engine = &v1beta1.EngineSpec{}
	old.Spec.Decoder = &v1beta1.DecoderSpec{}
	updated = old.DeepCopy()
	updated.Spec.Decoder.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
	require.ErrorContains(t, ValidateScalingPolicyServiceUpdate(old, updated), "follower autoscaler")

	oldRuntime := &v1beta1.ServingRuntimeSpec{ScalingPolicy: proportionalPolicy("1")}
	newRuntime := oldRuntime.DeepCopy()
	newRuntime.SupportedModelFormats = []v1beta1.SupportedModelFormat{{Name: "example"}}
	require.NoError(t, ValidateScalingPolicyRuntimeUpdate(oldRuntime, newRuntime))
	oldRuntime.EngineConfig = &v1beta1.EngineSpec{}
	oldRuntime.DecoderConfig = &v1beta1.DecoderSpec{}
	newRuntime = oldRuntime.DeepCopy()
	newRuntime.DecoderConfig.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
	require.ErrorContains(t, ValidateScalingPolicyRuntimeUpdate(oldRuntime, newRuntime), "follower autoscaler")
}
