package irprojector

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestLifecycleFromComponentExt_EachSideCarriesItsOwnLeaderGoneGrace: a
// service with an engine and a decoder sets lifecycle.leaderGoneGracePeriod
// to 30 s on the engine and to 45 s on the decoder. The replica of each
// side gets the value of its own side, as a copy the service does not
// share. A side with no lifecycle block gets none.
func TestLifecycleFromComponentExt_EachSideCarriesItsOwnLeaderGoneGrace(t *testing.T) {
	with := func(d time.Duration) v1beta1.ComponentExtensionSpec {
		return v1beta1.ComponentExtensionSpec{Lifecycle: &v1beta1.LifecycleSpec{LeaderGoneGracePeriod: &metav1.Duration{Duration: d}}}
	}
	engine := &v1beta1.EngineSpec{ComponentExtensionSpec: with(30 * time.Second)}
	decoder := &v1beta1.DecoderSpec{ComponentExtensionSpec: with(45 * time.Second)}

	check := func(side string, source *v1beta1.ComponentExtensionSpec, want time.Duration) {
		t.Helper()
		got := lifecycleFromComponentExt(source)
		if got == nil || got.LeaderGoneGracePeriod == nil || got.LeaderGoneGracePeriod.Duration != want {
			t.Fatalf("the %s replica's lifecycle reads %+v, want a leaderGoneGracePeriod of %s", side, got, want)
		}
		if got == source.Lifecycle || got.LeaderGoneGracePeriod == source.Lifecycle.LeaderGoneGracePeriod {
			t.Fatalf("the %s replica shares its lifecycle with the service", side)
		}
	}
	check("engine", &engine.ComponentExtensionSpec, 30*time.Second)
	check("decoder", &decoder.ComponentExtensionSpec, 45*time.Second)
	if got := lifecycleFromComponentExt(&v1beta1.ComponentExtensionSpec{}); got != nil {
		t.Fatalf("a side with no lifecycle block gives its replica %+v, want none", got)
	}
}
