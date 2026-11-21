package validation

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestValidateLifecycle_LeaderGoneGracePeriod holds the pure-validator half
// of the lifecycle.leaderGoneGracePeriod rule on an InferenceService: an
// unset field and a value above zero are accepted, and zero or a negative
// value is rejected with the component's field path and the
// InvalidLeaderGoneGracePeriod reason.
func TestValidateLifecycle_LeaderGoneGracePeriod(t *testing.T) {
	with := func(d time.Duration) *v1beta1.LifecycleSpec {
		return &v1beta1.LifecycleSpec{LeaderGoneGracePeriod: &metav1.Duration{Duration: d}}
	}
	onEngine := func(lc *v1beta1.LifecycleSpec) *v1beta1.InferenceServiceSpec {
		return &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Lifecycle: lc}}}
	}
	onRouter := func(lc *v1beta1.LifecycleSpec) *v1beta1.InferenceServiceSpec {
		return &v1beta1.InferenceServiceSpec{Router: &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Lifecycle: lc}}}
	}
	cases := []struct {
		name string
		spec *v1beta1.InferenceServiceSpec
		// refusedAt is the field path the refusal names; empty when the value is accepted.
		refusedAt string
	}{
		{name: "an engine with the field unset", spec: onEngine(&v1beta1.LifecycleSpec{})},
		{name: "an engine with 30 s", spec: onEngine(with(30 * time.Second))},
		{name: "an engine with 1 ms", spec: onEngine(with(time.Millisecond))},
		{name: "an engine with zero", spec: onEngine(with(0)), refusedAt: "spec.engine.lifecycle.leaderGoneGracePeriod"},
		{name: "an engine with a negative value", spec: onEngine(with(-5 * time.Second)), refusedAt: "spec.engine.lifecycle.leaderGoneGracePeriod"},
		{name: "a router with 30 s", spec: onRouter(with(30 * time.Second))},
		{name: "a router with zero", spec: onRouter(with(0)), refusedAt: "spec.router.lifecycle.leaderGoneGracePeriod"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLifecycle(tc.spec)
			if tc.refusedAt == "" {
				if err != nil {
					t.Fatalf("want the value accepted, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want a refusal at %s, got none", tc.refusedAt)
			}
			if !strings.Contains(err.Error(), tc.refusedAt) || !strings.Contains(err.Error(), ReasonInvalidLeaderGoneGracePeriod) {
				t.Fatalf("the refusal must name %s and %s, got %v", tc.refusedAt, ReasonInvalidLeaderGoneGracePeriod, err)
			}
		})
	}
}

// TestValidateLeaderGoneGracePeriod_NamesThePathItIsGiven: the replica
// webhook hands the same rule the replica's own field path.
func TestValidateLeaderGoneGracePeriod_NamesThePathItIsGiven(t *testing.T) {
	if err := ValidateLeaderGoneGracePeriod("spec.lifecycle", nil); err != nil {
		t.Fatalf("no lifecycle block, no refusal: %v", err)
	}
	if err := ValidateLeaderGoneGracePeriod("spec.lifecycle", &v1beta1.LifecycleSpec{}); err != nil {
		t.Fatalf("an unset field, no refusal: %v", err)
	}
	err := ValidateLeaderGoneGracePeriod("spec.lifecycle", &v1beta1.LifecycleSpec{LeaderGoneGracePeriod: &metav1.Duration{}})
	if err == nil || !strings.Contains(err.Error(), "spec.lifecycle.leaderGoneGracePeriod is 0s and must be above 0s") {
		t.Fatalf("want a refusal that names the path and the value, got %v", err)
	}
}
