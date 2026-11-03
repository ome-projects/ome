package scaling

import (
	"math"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestResolve(t *testing.T) {
	runtime := &v1beta1.ServingRuntimeSpec{ScalingPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional}}
	isvc := &v1beta1.InferenceService{}
	policy, source := Resolve(isvc, runtime)
	if source != "runtime" || policy.Mode != v1beta1.ScalingProportional {
		t.Fatalf("runtime policy: %v, %q", policy, source)
	}
	isvc.Spec.ScalingPolicy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}
	policy, source = Resolve(isvc, runtime)
	if source != "isvc" || policy.Mode != v1beta1.ScalingIndependent {
		t.Fatalf("service override: %v, %q", policy, source)
	}
	policy.Mode = v1beta1.ScalingProportional
	if isvc.Spec.ScalingPolicy.Mode != v1beta1.ScalingIndependent {
		t.Fatal("resolution must not mutate the service")
	}
	policy, source = Resolve(isvc, nil)
	if source != "isvc" || policy.Mode != v1beta1.ScalingIndependent {
		t.Fatalf("runtime absent: %v, %q", policy, source)
	}
}

func TestTargetScaleUpAndDown(t *testing.T) {
	for _, tc := range []struct {
		name   string
		anchor int32
		milli  int64
		min    int32
		max    *int32
		want   int32
		bound  string
	}{
		{"equal-up", 5, 1000, 1, ptr.To[int32](12), 5, ""},
		{"equal-down", 2, 1000, 1, ptr.To[int32](12), 2, ""},
		{"fractional-up", 5, 250, 1, ptr.To[int32](12), 2, ""},
		{"fractional-down", 4, 250, 1, ptr.To[int32](12), 1, ""},
		{"minimum", 1, 250, 3, ptr.To[int32](12), 3, boundMinReplicas},
		{"maximum", 5, 1000, 1, ptr.To[int32](3), 3, boundMaxReplicas},
		{"zero-floor", 0, 1000, 1, ptr.To[int32](12), 1, boundMinReplicas},
		{"over-one", 5, 1500, 1, ptr.To[int32](12), 8, ""},
		{"no-maximum-follows", 8, 1000, 2, nil, 8, ""},
		{"no-maximum-rounds-up", 36, 250, 2, nil, 9, ""},
		{"no-maximum-held-at-minimum", 1, 250, 2, nil, 2, boundMinReplicas},
		{"maximum-below-the-ratio", 36, 250, 1, ptr.To[int32](8), 8, boundMaxReplicas},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count, err := Target(tc.anchor, tc.milli, Bounds{Min: tc.min, Max: tc.max})
			if err != nil || count.Target != tc.want || count.Bound != tc.bound || (tc.bound == "" && count.Wanted != int64(tc.want)) {
				t.Fatalf("Target(%d, %d, %d, %v) = %+v, %v; want %d bounded by %q", tc.anchor, tc.milli, tc.min, tc.max, count, err, tc.want, tc.bound)
			}
		})
	}
	open := Bounds{Min: 1}
	if _, err := Target(-1, 1000, open); err == nil {
		t.Fatal("negative anchor desired count must be rejected")
	}
	if _, err := Target(5, 0, open); err == nil {
		t.Fatal("zero ratio must be rejected")
	}
	if _, err := Target(5, 1000, Bounds{Min: 4, Max: ptr.To[int32](3)}); err == nil {
		t.Fatal("a maximum below the minimum must be rejected")
	}
	if _, err := Target(math.MaxInt32, 1_000_000, open); err == nil {
		t.Fatal("a count past the replica range must be rejected, not wrapped")
	}
}

func TestFollowerBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *v1beta1.ComponentExtensionSpec
		min  int32
		max  *int32
	}{
		{"undeclared", nil, 1, nil},
		{"only-a-minimum", &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2)}, 2, nil},
		{"both", &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2), MaxReplicas: 8}, 2, ptr.To[int32](8)},
		{"maximum-raised-to-minimum", &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(5), MaxReplicas: 3}, 5, ptr.To[int32](5)},
		{"zero-minimum-raised-to-one", &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0)}, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := FollowerBounds(tc.spec)
			if got.Min != tc.min || (got.Max == nil) != (tc.max == nil) || (got.Max != nil && *got.Max != *tc.max) {
				t.Fatalf("FollowerBounds(%+v) = %+v; want min %d max %v", tc.spec, got, tc.min, tc.max)
			}
		})
	}
}

func TestFollows(t *testing.T) {
	ratio := resource.MustParse("0.25")
	policy := &v1beta1.ScalingPolicy{
		Mode: v1beta1.ScalingProportional,
		Proportional: &v1beta1.ProportionalPolicy{
			Anchor: v1beta1.EngineComponent,
			Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: ratio},
		},
	}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent: {}, v1beta1.DecoderComponent: {},
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative,
	}
	if !Follows(policy, v1beta1.DecoderComponent, components, modes) || Follows(policy, v1beta1.EngineComponent, components, modes) {
		t.Fatal("only the declared follower belongs to the coordinator")
	}
	policy.Proportional.Ratios[v1beta1.RouterComponent] = resource.MustParse("0.5")
	if !Follows(policy, v1beta1.DecoderComponent, components, modes) || Follows(policy, v1beta1.RouterComponent, components, modes) {
		t.Fatal("undeclared router must not suppress its scaler or stop the decoder following")
	}
	components[v1beta1.RouterComponent] = &v1beta1.ComponentExtensionSpec{}
	modes[v1beta1.RouterComponent] = constants.RawDeployment
	if !Follows(policy, v1beta1.DecoderComponent, components, modes) || Follows(policy, v1beta1.RouterComponent, components, modes) {
		t.Fatal("RawDeployment router must not become a coordinator-owned follower")
	}
	modes[v1beta1.EngineComponent] = constants.RawDeployment
	if Follows(policy, v1beta1.DecoderComponent, components, modes) {
		t.Fatal("no follower loses its autoscaler when the anchor is not OMENative")
	}
	modes[v1beta1.EngineComponent] = constants.OMENative
	delete(components, v1beta1.EngineComponent)
	if Follows(policy, v1beta1.DecoderComponent, components, modes) {
		t.Fatal("no follower loses its autoscaler when the anchor is undeclared")
	}
}
