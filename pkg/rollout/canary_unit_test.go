package rollout

import (
	"reflect"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Every role that pairs rolls in the engine's unit; the router is its own.
func TestCanaryUnitFoldsThePairingRolesIntoTheEngine(t *testing.T) {
	cases := map[v1beta1.ComponentType]v1beta1.ComponentType{
		v1beta1.EngineComponent:  v1beta1.EngineComponent,
		v1beta1.DecoderComponent: v1beta1.EngineComponent,
		v1beta1.EncoderComponent: v1beta1.EngineComponent,
		v1beta1.RouterComponent:  v1beta1.RouterComponent,
		"sidecar":                "sidecar",
	}
	for c, want := range cases {
		if got := CanaryUnit(c); got != want {
			t.Errorf("CanaryUnit(%q) = %q, want %q", c, got, want)
		}
	}
	group := &v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{
		v1beta1.EncoderComponent, v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent,
	}}
	want := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent}
	if got := CanaryUnitsOf(group); !reflect.DeepEqual(got, want) {
		t.Errorf("CanaryUnitsOf(%v) = %v, want %v", group.Components, got, want)
	}
}
