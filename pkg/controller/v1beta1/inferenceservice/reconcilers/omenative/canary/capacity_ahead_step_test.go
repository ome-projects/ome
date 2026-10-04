package canary

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// A step at 0% traffic whose canary capacity is Ready is the capacity-ahead
// warm-up: the canary is validated in-cluster before it takes traffic. It is
// an ordinary step of the ladder, Paused while its gate holds, with the
// stable revision still the only routed target; it has no phase of its own.
func TestReconcile_CapacityAheadStepIsAPausedStep(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("100%"), Traffic: 0, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	isvc := canaryISVC(steps, nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		StableRevisionHash: "old",
		StepEnteredTime:    &metav1.Time{Time: time.Unix(1000, 0)},
	}
	res, err := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 4, "old": 4}))
	if err != nil {
		t.Fatal(err)
	}
	// The step stages its capacity on every instance but the held stable one:
	// the stable revision keeps its last instance (the held floor) while it
	// still carries traffic, so 100% capacity on 4 instances is partition 1.
	if !res.Active || res.Partition != 1 {
		t.Fatalf("a Ready capacity-ahead step is an active step staged on all but the held stable instance, got %+v", res)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhasePaused {
		t.Fatalf("phase = %q, want Paused: the 0%% step holds at its manual gate like any other step", got)
	}
	cs := isvc.Status.Canary
	if cs.CurrentStep != 0 || cs.ObservedTrafficWeight != 0 {
		t.Fatalf("status = %+v, want step 0 programmed at 0%%", cs)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
}
