package v1beta1convert

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestLifecycleSpecToWorkload_LeaderGoneGracePeriod: the engine's lifecycle
// carries a copy of the replica's leaderGoneGracePeriod. A replica that sets
// none, or a value that is not above zero, hands the engine no grace.
func TestLifecycleSpecToWorkload_LeaderGoneGracePeriod(t *testing.T) {
	in := v1beta1.LifecycleSpec{LeaderGoneGracePeriod: &metav1.Duration{Duration: 30 * time.Second}}
	out := LifecycleSpecToWorkload(in)
	if grace, set := out.LeaderGoneGrace(); !set || grace != 30*time.Second {
		t.Fatalf("the engine reads %s (set=%v), want 30s", grace, set)
	}
	in.LeaderGoneGracePeriod.Duration = time.Hour
	if grace, _ := out.LeaderGoneGrace(); grace != 30*time.Second {
		t.Fatalf("the engine's value follows a later change of the replica's: %s, want a copy at 30s", grace)
	}

	none := map[string]v1beta1.LifecycleSpec{
		"unset":    {},
		"zero":     {LeaderGoneGracePeriod: &metav1.Duration{}},
		"negative": {LeaderGoneGracePeriod: &metav1.Duration{Duration: -time.Second}},
	}
	for name, spec := range none {
		if grace, set := LifecycleSpecToWorkload(spec).LeaderGoneGrace(); set {
			t.Errorf("%s: the engine reads a grace of %s, want none", name, grace)
		}
	}
}
