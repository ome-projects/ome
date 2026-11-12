package validation

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func canaryGroup(components ...v1beta1.ComponentType) v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components: components,
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 10},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}
}

func rolloutSpec(router bool, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceServiceSpec {
	mode := constants.OMENative
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: &mode,
		Engine:         &v1beta1.EngineSpec{},
		Rollout:        &v1beta1.RolloutSpec{Groups: groups},
	}
	if router {
		spec.Router = &v1beta1.RouterSpec{}
	}
	return spec
}

var (
	weightedProvider = RouteProviderCapabilities{Provider: "GatewayAPI", Capabilities: []string{constants.RouteCapabilityWeightedRevisionRouting}, Known: true}
	bareProvider     = RouteProviderCapabilities{Provider: "KubernetesIngress", Known: true}
	unknownProvider  = RouteProviderCapabilities{Provider: "None"}
)

func TestRequiredRouteCapabilities(t *testing.T) {
	tests := []struct {
		name string
		spec *v1beta1.InferenceServiceSpec
		want []RouteCapabilityRequirement
	}{
		{name: "nil spec", spec: nil},
		{name: "no rollout", spec: &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}},
		{
			name: "blueGreen asks nothing",
			spec: rolloutSpec(false, v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}}),
		},
		{
			name: "engine canary without a router programs the external route",
			spec: rolloutSpec(false, canaryGroup(v1beta1.EngineComponent)),
			want: []RouteCapabilityRequirement{{Field: "spec.rollout.groups[0].canary.steps[].traffic", Capability: constants.RouteCapabilityWeightedRevisionRouting}},
		},
		{
			name: "engine+decoder canary without a router programs the external route",
			spec: rolloutSpec(false, canaryGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent)),
			want: []RouteCapabilityRequirement{{Field: "spec.rollout.groups[0].canary.steps[].traffic", Capability: constants.RouteCapabilityWeightedRevisionRouting}},
		},
		{
			name: "engine canary behind a router asks nothing of the provider",
			spec: rolloutSpec(true, canaryGroup(v1beta1.EngineComponent)),
		},
		{
			name: "router canary programs the external route",
			spec: rolloutSpec(true, v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}}, canaryGroup(v1beta1.RouterComponent)),
			want: []RouteCapabilityRequirement{{Field: "spec.rollout.groups[1].canary.steps[].traffic", Capability: constants.RouteCapabilityWeightedRevisionRouting}},
		},
		{
			name: "referenced canary plan is recognised by its declared progression",
			spec: rolloutSpec(false, v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "std", Progression: v1beta1.RolloutProgressionCanary},
			}),
			want: []RouteCapabilityRequirement{{Field: "spec.rollout.groups[0].policyRef (progression canary)", Capability: constants.RouteCapabilityWeightedRevisionRouting}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := RequiredRouteCapabilities(tc.spec)
			if len(got) != len(tc.want) {
				t.Fatalf("RequiredRouteCapabilities() = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("RequiredRouteCapabilities()[%d] = %v, want %v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestValidateRouteCapabilities(t *testing.T) {
	canary := rolloutSpec(false, canaryGroup(v1beta1.EngineComponent))

	t.Run("rejected when the provider declares no weighted routing", func(t *testing.T) {
		err := ValidateRouteCapabilities(canary, bareProvider)
		if err == nil {
			t.Fatal("expected rejection, got nil")
		}
		for _, want := range []string{ReasonRouteProviderUnsupported, `"KubernetesIngress"`, constants.RouteCapabilityWeightedRevisionRouting, "spec.rollout.groups[0].canary.steps[].traffic"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q should name %q", err, want)
			}
		}
	})
	t.Run("admitted when the provider declares weighted routing", func(t *testing.T) {
		if err := ValidateRouteCapabilities(canary, weightedProvider); err != nil {
			t.Fatalf("expected acceptance, got %v", err)
		}
	})
	t.Run("admitted when the provider's capabilities are unknown", func(t *testing.T) {
		if err := ValidateRouteCapabilities(canary, unknownProvider); err != nil {
			t.Fatalf("an undeclared provider must not gate, got %v", err)
		}
	})
	t.Run("a canary behind a router passes an incapable provider", func(t *testing.T) {
		if err := ValidateRouteCapabilities(rolloutSpec(true, canaryGroup(v1beta1.EngineComponent)), bareProvider); err != nil {
			t.Fatalf("expected acceptance, got %v", err)
		}
	})
	t.Run("no intent passes any provider", func(t *testing.T) {
		if err := ValidateRouteCapabilities(rolloutSpec(false), bareProvider); err != nil {
			t.Fatalf("expected acceptance, got %v", err)
		}
	})
}

func TestUnsupportedRouteCapabilities(t *testing.T) {
	required := RequiredRouteCapabilities(rolloutSpec(false, canaryGroup(v1beta1.EngineComponent)))
	if got := UnsupportedRouteCapabilities(required, unknownProvider); got != nil {
		t.Fatalf("unknown provider must report nothing unsupported, got %v", got)
	}
	if got := UnsupportedRouteCapabilities(required, weightedProvider); got != nil {
		t.Fatalf("capable provider must report nothing unsupported, got %v", got)
	}
	if got := UnsupportedRouteCapabilities(required, bareProvider); len(got) != 1 || got[0] != required[0] {
		t.Fatalf("bare provider must report the requirement, got %v", got)
	}
}

func TestValidateRouteCapabilitiesUpdate(t *testing.T) {
	canary := rolloutSpec(false, canaryGroup(v1beta1.EngineComponent))
	plain := rolloutSpec(false)

	t.Run("unchanged intent and provider is admitted even when unsupported", func(t *testing.T) {
		scaled := canary.DeepCopy()
		scaled.Engine.MinReplicas = new(int)
		if err := ValidateRouteCapabilitiesUpdate(canary, scaled, bareProvider, bareProvider); err != nil {
			t.Fatalf("expected the ratchet to admit, got %v", err)
		}
	})
	t.Run("editing the rollout is gated", func(t *testing.T) {
		edited := canary.DeepCopy()
		edited.Rollout.Groups[0].Canary.Steps[0].Traffic = 20
		if err := ValidateRouteCapabilitiesUpdate(canary, edited, bareProvider, bareProvider); err == nil {
			t.Fatal("expected rejection, got nil")
		}
	})
	t.Run("moving the route by adding a router is re-evaluated", func(t *testing.T) {
		fronted := canary.DeepCopy()
		fronted.Router = &v1beta1.RouterSpec{}
		if err := ValidateRouteCapabilitiesUpdate(canary, fronted, bareProvider, bareProvider); err != nil {
			t.Fatalf("an engine canary behind a router asks nothing of the provider, got %v", err)
		}
	})
	t.Run("adding the intent is gated", func(t *testing.T) {
		if err := ValidateRouteCapabilitiesUpdate(plain, canary, bareProvider, bareProvider); err == nil {
			t.Fatal("expected rejection, got nil")
		}
	})
	t.Run("switching to an incapable provider is gated", func(t *testing.T) {
		if err := ValidateRouteCapabilitiesUpdate(canary, canary, weightedProvider, bareProvider); err == nil {
			t.Fatal("expected rejection, got nil")
		}
	})
	t.Run("removing the intent is admitted", func(t *testing.T) {
		if err := ValidateRouteCapabilitiesUpdate(canary, plain, bareProvider, bareProvider); err != nil {
			t.Fatalf("expected acceptance, got %v", err)
		}
	})
}
