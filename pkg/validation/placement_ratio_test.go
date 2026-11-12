package validation

import (
	"strings"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func ratioTerm(cluster string, w *v1beta1.ClusterAffinityWeights) v1beta1.ClusterAffinityTerm {
	return v1beta1.ClusterAffinityTerm{
		Weights: w,
		MatchFields: []v1beta1.ClusterSelectorRequirement{
			{Key: "metadata.name", Operator: "In", Values: []string{cluster}},
		},
	}
}

func TestValidateRatioForm(t *testing.T) {
	ratio := &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}
	plainWeights := &v1beta1.ClusterAffinityWeights{Engine: ratio}
	emptyWeights := &v1beta1.ClusterAffinityWeights{Engine: &v1beta1.ReplicaRatio{}}
	const splitMsg = "clusterAffinity weights are allowed only in Split and replace spec.placement.split; remove split"

	for _, tt := range []struct {
		name    string
		p       *v1beta1.PlacementSpec
		wantErr string
	}{
		{name: "nil placement", p: nil},
		{name: "plain keys only", p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", plainWeights), ratioTerm("cluster-b", plainWeights)},
		}},
		{name: "empty ratio", p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", emptyWeights), ratioTerm("cluster-b", plainWeights)},
		}},
		{name: "unweighted with split block", p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Split:           &v1beta1.SplitSpec{Replicas: ptr.To[int32](4), MaxPerCluster: &v1beta1.SplitCeilings{Engine: ptr.To[int32](3)}},
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", nil)},
		}},
		{name: "empty ratio with split replicas", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Split:           &v1beta1.SplitSpec{Replicas: ptr.To[int32](4)},
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", emptyWeights)},
		}},
		{name: "plain key with split replicas", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Split:           &v1beta1.SplitSpec{Replicas: ptr.To[int32](4)},
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", plainWeights)},
		}},
		{name: "plain key with split maxPerCluster", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Split:           &v1beta1.SplitSpec{MaxPerCluster: &v1beta1.SplitCeilings{Engine: ptr.To[int32](3)}},
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", plainWeights)},
		}},
		{name: "plain key in SplitByCapacity", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplitByCapacity,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", plainWeights)},
		}},
		{name: "plain key in All", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeAll,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", plainWeights)},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRatioForm(tt.p)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidatePlacementRatioUndeclaredComponent(t *testing.T) {
	spec := func(w *v1beta1.ClusterAffinityWeights, router *v1beta1.RouterSpec) *v1beta1.InferenceServiceSpec {
		return &v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
			Router: router,
			Placement: &v1beta1.PlacementSpec{
				Policy:          v1beta1.PlacementPolicyClusterAffinity,
				Mode:            v1beta1.PlacementModeSplit,
				ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", w)},
			},
		}
	}
	err := ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		Decoder: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "weights.decoder names") || !strings.Contains(err.Error(), "does not declare") {
		t.Fatalf("decoder without a decoder: error = %v", err)
	}
	err = ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		Decoder: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "weights.decoder names") {
		t.Fatalf("decoder without a decoder: error = %v", err)
	}
	err = ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		Router: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "weights.router names") {
		t.Fatalf("router without a router: error = %v", err)
	}
	err = ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		Router: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "weights.router names") {
		t.Fatalf("router without a router: error = %v", err)
	}
	err = ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		Router: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2)}}))
	if err != nil {
		t.Fatalf("router with a router: unexpected error %v", err)
	}
	withDecoder := spec(&v1beta1.ClusterAffinityWeights{
		Decoder: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil)
	withDecoder.Decoder = &v1beta1.DecoderSpec{}
	if err := ValidatePlacement(withDecoder); err != nil {
		t.Fatalf("decoder with a decoder: unexpected error %v", err)
	}
	noEngine := spec(&v1beta1.ClusterAffinityWeights{
		Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil)
	noEngine.Engine = nil
	noEngine.Decoder = &v1beta1.DecoderSpec{}
	err = ValidatePlacement(noEngine)
	if err == nil || !strings.Contains(err.Error(), "weights.engine") || !strings.Contains(err.Error(), "does not declare") {
		t.Fatalf("engine without an engine: error = %v", err)
	}
	if err := ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil)); err != nil {
		t.Fatalf("engine with an engine: unexpected error %v", err)
	}
}

func TestValidatePlacementRatioRouterMinimumNamesTheKeys(t *testing.T) {
	for _, w := range []*v1beta1.ClusterAffinityWeights{
		{Default: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)}},
		{Router: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)}},
	} {
		spec := &v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
			Router: &v1beta1.RouterSpec{},
			Placement: &v1beta1.PlacementSpec{
				Policy:          v1beta1.PlacementPolicyClusterAffinity,
				Mode:            v1beta1.PlacementModeSplit,
				ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", w)},
			},
		}
		err := ValidatePlacement(spec)
		if err == nil || !strings.Contains(err.Error(), "weights.default or weights.router; set a positive router.minReplicas") {
			t.Fatalf("error = %v, want the router minimum message naming the keys", err)
		}
	}
}

func TestAutoscalerPolicySplitCeilingWarningRatioForm(t *testing.T) {
	ref := &v1beta1.AutoscalerPolicyRef{Name: "example-policy"}
	for name, weights := range map[string]*v1beta1.ClusterAffinityWeights{
		"plain key": {Engine: &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}},
	} {
		build := func(maxReplicas int) *v1beta1.InferenceService {
			isvc := isvcWithPolicyRefs(ref, nil, nil)
			isvc.Spec.Engine.MaxReplicas = maxReplicas
			isvc.Spec.Placement = &v1beta1.PlacementSpec{
				Mode:            v1beta1.PlacementModeSplit,
				ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", weights)},
			}
			return isvc
		}
		got := AutoscalerPolicySplitCeilingWarning(build(0))
		for _, want := range []string{"maxReplicas", "engine", v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling} {
			if !strings.Contains(got, want) {
				t.Errorf("%s: warning %q does not mention %s", name, got, want)
			}
		}
		if got := AutoscalerPolicySplitCeilingWarning(build(40)); got != "" {
			t.Errorf("%s: unexpected warning with maxReplicas set: %q", name, got)
		}
	}
}
