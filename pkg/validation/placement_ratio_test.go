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
	ratioWeights := &v1beta1.ClusterAffinityWeights{EngineReplicas: ratio}
	intWeights := &v1beta1.ClusterAffinityWeights{Engine: ptr.To[int32](2)}
	emptyWeights := &v1beta1.ClusterAffinityWeights{EngineReplicas: &v1beta1.ReplicaRatio{}}
	const bothMsg = "clusterAffinity weights use either default/engine/decoder/router or the *Replicas ratios, not both"
	const splitMsg = "*Replicas ratios are allowed only in Split and replace spec.placement.split; remove split"

	for _, tt := range []struct {
		name    string
		p       *v1beta1.PlacementSpec
		wantErr string
	}{
		{name: "nil placement", p: nil},
		{name: "ratio only", p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", ratioWeights), ratioTerm("cluster-b", ratioWeights)},
		}},
		{name: "empty ratio", p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", emptyWeights), ratioTerm("cluster-b", ratioWeights)},
		}},
		{name: "empty ratio and integer on two terms", wantErr: bothMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", emptyWeights), ratioTerm("cluster-b", intWeights)},
		}},
		{name: "empty ratio with split replicas", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Split:           &v1beta1.SplitSpec{Replicas: ptr.To[int32](4)},
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", emptyWeights)},
		}},
		{name: "integer only", p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Split:           &v1beta1.SplitSpec{Replicas: ptr.To[int32](4)},
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", intWeights)},
		}},
		{name: "ratio and integer on two terms", wantErr: bothMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", ratioWeights), ratioTerm("cluster-b", intWeights)},
		}},
		{name: "ratio and integer on one term", wantErr: bothMsg, p: &v1beta1.PlacementSpec{
			Mode: v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", &v1beta1.ClusterAffinityWeights{
				EngineReplicas: ratio, Decoder: ptr.To[int32](1),
			})},
		}},
		{name: "ratio with split replicas", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Split:           &v1beta1.SplitSpec{Replicas: ptr.To[int32](4)},
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", ratioWeights)},
		}},
		{name: "ratio with split maxPerCluster", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Split:           &v1beta1.SplitSpec{MaxPerCluster: &v1beta1.SplitCeilings{Engine: ptr.To[int32](3)}},
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", ratioWeights)},
		}},
		{name: "ratio in SplitByCapacity", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplitByCapacity,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", ratioWeights)},
		}},
		{name: "ratio in All", wantErr: splitMsg, p: &v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeAll,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", ratioWeights)},
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
		DecoderReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "decoderReplicas") || !strings.Contains(err.Error(), "does not declare") {
		t.Fatalf("decoderReplicas without a decoder: error = %v", err)
	}
	err = ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		RouterReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil))
	if err == nil || !strings.Contains(err.Error(), "routerReplicas") {
		t.Fatalf("routerReplicas without a router: error = %v", err)
	}
	err = ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		RouterReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2)}}))
	if err != nil {
		t.Fatalf("routerReplicas with a router: unexpected error %v", err)
	}
	withDecoder := spec(&v1beta1.ClusterAffinityWeights{
		DecoderReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil)
	withDecoder.Decoder = &v1beta1.DecoderSpec{}
	if err := ValidatePlacement(withDecoder); err != nil {
		t.Fatalf("decoderReplicas with a decoder: unexpected error %v", err)
	}
	noEngine := spec(&v1beta1.ClusterAffinityWeights{
		EngineReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil)
	noEngine.Engine = nil
	noEngine.Decoder = &v1beta1.DecoderSpec{}
	err = ValidatePlacement(noEngine)
	if err == nil || !strings.Contains(err.Error(), "engineReplicas") || !strings.Contains(err.Error(), "does not declare") {
		t.Fatalf("engineReplicas without an engine: error = %v", err)
	}
	if err := ValidatePlacement(spec(&v1beta1.ClusterAffinityWeights{
		EngineReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
	}, nil)); err != nil {
		t.Fatalf("engineReplicas with an engine: unexpected error %v", err)
	}
}

func TestValidatePlacementReportsRatioFormFirst(t *testing.T) {
	// The ratio-form error comes before the selector compile and component
	// checks, so a mixed term that also names an undeclared decoder reports
	// the mixing.
	spec := &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{},
		Placement: &v1beta1.PlacementSpec{
			Policy: v1beta1.PlacementPolicyClusterAffinity,
			Mode:   v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", &v1beta1.ClusterAffinityWeights{
				EngineReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)}, Decoder: ptr.To[int32](1),
			})},
		},
	}
	err := ValidatePlacement(spec)
	if err == nil || err.Error() != ratioMixedFormMessage {
		t.Fatalf("error = %v, want %q", err, ratioMixedFormMessage)
	}
}

func TestValidatePlacementRatioRouterMinimumNamesRatioKeys(t *testing.T) {
	spec := &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{},
		Router: &v1beta1.RouterSpec{},
		Placement: &v1beta1.PlacementSpec{
			Policy: v1beta1.PlacementPolicyClusterAffinity,
			Mode:   v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{ratioTerm("cluster-a", &v1beta1.ClusterAffinityWeights{
				DefaultReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)},
			})},
		},
	}
	err := ValidatePlacement(spec)
	if err == nil || !strings.Contains(err.Error(), "weights.defaultReplicas or weights.routerReplicas") || strings.Contains(err.Error(), "weights.default or") {
		t.Fatalf("error = %v, want the router minimum message naming the ratio keys", err)
	}
}

func TestAutoscalerPolicySplitCeilingWarningRatioForm(t *testing.T) {
	ref := &v1beta1.AutoscalerPolicyRef{Name: "example-policy"}
	build := func(maxReplicas int) *v1beta1.InferenceService {
		isvc := isvcWithPolicyRefs(ref, nil, nil)
		isvc.Spec.Engine.MaxReplicas = maxReplicas
		isvc.Spec.Placement = &v1beta1.PlacementSpec{
			Mode: v1beta1.PlacementModeSplit,
			ClusterAffinity: []v1beta1.ClusterAffinityTerm{
				ratioTerm("cluster-a", &v1beta1.ClusterAffinityWeights{EngineReplicas: &v1beta1.ReplicaRatio{Min: ptr.To[int32](2)}}),
			},
		}
		return isvc
	}
	got := AutoscalerPolicySplitCeilingWarning(build(0))
	for _, want := range []string{"maxReplicas", "engine", v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling} {
		if !strings.Contains(got, want) {
			t.Errorf("warning %q does not mention %s", got, want)
		}
	}
	if got := AutoscalerPolicySplitCeilingWarning(build(40)); got != "" {
		t.Errorf("unexpected warning with maxReplicas set: %q", got)
	}
}
