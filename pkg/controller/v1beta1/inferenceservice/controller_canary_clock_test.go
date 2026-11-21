package inferenceservice

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// canaryClockWait is how long the canary's first step waits.
const canaryClockWait = 20 * time.Second

// A canary's timed step and the rollout around it take their time from the
// reconciler's clock. Two prefill and two decode pods roll in two steps: half
// the pods with half the traffic, a 20 second wait, then the rest. The
// world's clock moves 5 seconds per tick, far faster than the machine's. The
// rollout is dated by the pass that opened it, the wait is dated by the pass
// that first sent traffic to version 2, and the step ends on the first pass
// that runs 20 seconds of the reconciler's clock later.
func TestTimedCanaryStepFollowsTheReconcilersClock(t *testing.T) {
	t.Parallel()
	w := newRefusedWorld(t, refusedConfig{
		shape:    refusedShape{prefill: 2, decode: 2, pods: 1},
		settings: refusedSettings{prefill: v1beta1.UpdateStrategySurgeThenDrain, decode: v1beta1.UpdateStrategySurgeThenDrain},
	})
	w.service.Clock = w.clock
	writeCanarySpec(w)

	builtRun, built := "", false
	for i := 0; i < 60 && !built; i++ {
		_, isvc := canaryClockTick(w)
		if isvc.Status.Rollout != nil && isvc.Status.Rollout.ActiveRun != nil {
			builtRun = isvc.Status.Rollout.ActiveRun.RunID
			continue
		}
		built = canaryClockServes(w, refusedImageV1)
	}
	if !built {
		t.Fatalf("the fleet never served version 1 with no rollout open:\n%s", canaryClockTrace(w))
	}

	w.images[v1beta1.EngineComponent], w.images[v1beta1.DecoderComponent] = refusedImageV2, refusedImageV2
	writeCanarySpec(w)

	var entered time.Time
	opened := false
	for i := 0; i < 60 && entered.IsZero(); i++ {
		at, isvc := canaryClockTick(w)
		if run := isvc.Status.Rollout; !opened && run != nil && run.ActiveRun != nil && run.ActiveRun.RunID != builtRun {
			opened = true
			if !run.ActiveRun.OpenedAt.Time.Equal(at) {
				t.Fatalf("the pass at %s opened the rollout and dated it %s: the rollout does not read the reconciler's clock",
					at.Format(time.RFC3339), run.ActiveRun.OpenedAt.Time.UTC().Format(time.RFC3339))
			}
		}
		unit := isvc.Status.Components[v1beta1.EngineComponent]
		if unit.RolloutPhase != v1beta1.RolloutPhasePaused || unit.Canary == nil || unit.Canary.CurrentStep != 0 || unit.Canary.StepEnteredTime == nil {
			continue
		}
		entered = unit.Canary.StepEnteredTime.Time
		if !entered.Equal(at) {
			t.Fatalf("the pass at %s started the first step's wait and dated it %s: the canary does not read the reconciler's clock",
				at.Format(time.RFC3339), entered.UTC().Format(time.RFC3339))
		}
	}
	if !opened {
		t.Fatalf("the push of version 2 opened no rollout:\n%s", canaryClockTrace(w))
	}
	if entered.IsZero() {
		t.Fatalf("the canary never reached the wait of its first step:\n%s", canaryClockTrace(w))
	}

	due := entered.Add(canaryClockWait)
	for i := 0; i < 8; i++ {
		at, isvc := canaryClockTick(w)
		unit := isvc.Status.Components[v1beta1.EngineComponent]
		moved := unit.Canary != nil && unit.Canary.CurrentStep > 0
		switch {
		case moved && at.Before(due):
			t.Fatalf("the pass at %s ended the first step, and its %s wait lasts until %s", at.Format(time.RFC3339), canaryClockWait, due.Format(time.RFC3339))
		case moved:
			return
		case !at.Before(due):
			t.Fatalf("the pass at %s ran after the %s wait was over at %s and left the canary on its first step", at.Format(time.RFC3339), canaryClockWait, due.Format(time.RFC3339))
		}
	}
	t.Fatalf("the first step never ended:\n%s", canaryClockTrace(w))
}

// writeCanarySpec writes the spec the world renders with a two-step canary
// plan over the two sides. The world's own writer renders no canary plan, so
// every spec write of this test goes through here.
func writeCanarySpec(w *refusedWorld) {
	w.t.Helper()
	w.refuse.own(func() {
		isvc := &v1beta1.InferenceService{}
		if err := w.cli.Get(w.ctx, w.serviceKey(), isvc); err != nil {
			w.t.Fatalf("read service: %v", err)
		}
		isvc.Spec = w.serviceSpec()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: canaryClockWait}}},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			}},
		}}}
		if err := w.cli.Update(w.ctx, isvc); err != nil {
			w.t.Fatalf("write service spec: %v", err)
		}
	})
}

// canaryClockServes reports that each side holds its two pods, all serving
// the image and none leaving. The world's own settled reading waits for a
// traffic entry, and a canary group at rest writes none.
func canaryClockServes(w *refusedWorld, image string) bool {
	serving := map[v1beta1.ComponentType]int{}
	for _, pod := range w.pods() {
		if pod.leaving || !pod.serving || pod.image != image {
			return false
		}
		serving[pod.component]++
	}
	return serving[v1beta1.EngineComponent] == 2 && serving[v1beta1.DecoderComponent] == 2
}

// canaryClockTick runs one tick of the world. It returns the time the tick's
// service pass ran at and the service as the tick left it.
func canaryClockTick(w *refusedWorld) (time.Time, *v1beta1.InferenceService) {
	w.t.Helper()
	at := w.clock.Now()
	w.tick()
	isvc := &v1beta1.InferenceService{}
	if err := w.cli.Get(w.ctx, w.serviceKey(), isvc); err != nil {
		w.t.Fatalf("read service: %v", err)
	}
	return at, isvc
}

// canaryClockTrace is the end of the world's per-tick log, for a failure.
func canaryClockTrace(w *refusedWorld) string {
	return strings.Join(w.trace[max(0, len(w.trace)-20):], "\n")
}
