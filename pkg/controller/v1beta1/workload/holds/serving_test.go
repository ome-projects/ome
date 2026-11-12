package holds

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"

	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestUnlessServing_AServingRowReleasesTheHold: a hold that a fully serving
// pod set retires is released on a serving row without building its wait,
// keeps the wait it builds on a row that is not serving, and reports a pod
// read that fails instead of deciding on it.
func TestUnlessServing_AServingRowReleasesTheHold(t *testing.T) {
	const name = "svc-a-engine-0-default-0"
	_, in := newStore(creatingRow(""))
	row := rowsFor(in, 1)[0]
	input := func(pods map[int32][]*corev1.Pod, err error) PassInput {
		return PassInput{Input: in, Rows: []Row{row}, Pods: func(context.Context) (map[int32][]*corev1.Pod, error) { return pods, err }}
	}
	builds := 0
	wait := func() reading {
		builds++
		return reading{waiting: true, evidence: &types.InstanceTermination{Reason: types.WaitingReasonUnschedulable, PodName: name}}
	}

	t.Run("a serving row releases the hold and builds no wait", func(t *testing.T) {
		builds = 0
		got, err := unlessServing(context.Background(), input(map[int32][]*corev1.Pod{0: {fixture.ServingPod(name)}}, nil), row, wait)
		if err != nil {
			t.Fatal(err)
		}
		if !got.release || got.waiting || got.evidence != nil {
			t.Errorf("reading = %+v, want a release with no wait", got)
		}
		if builds != 0 {
			t.Errorf("the wait was built %d time(s) for a serving row, want 0", builds)
		}
	})

	t.Run("a row that is not serving keeps the wait it builds", func(t *testing.T) {
		builds = 0
		pod := fixture.UnschedulablePod(name, "0/3 nodes are available", fixture.Epoch)
		got, err := unlessServing(context.Background(), input(map[int32][]*corev1.Pod{0: {pod}}, nil), row, wait)
		if err != nil {
			t.Fatal(err)
		}
		if !got.waiting || got.release || got.evidence == nil || got.evidence.PodName != name {
			t.Errorf("reading = %+v, want the hold's own wait", got)
		}
		if builds != 1 {
			t.Errorf("the wait was built %d time(s), want 1", builds)
		}
	})

	t.Run("a row with no pods keeps the wait", func(t *testing.T) {
		got, err := unlessServing(context.Background(), input(nil, nil), row, wait)
		if err != nil {
			t.Fatal(err)
		}
		if !got.waiting {
			t.Errorf("reading = %+v, want the wait: no pod serves", got)
		}
	})

	t.Run("a failed pod read is reported, not decided on", func(t *testing.T) {
		builds = 0
		readErr := errors.New("pods unavailable")
		got, err := unlessServing(context.Background(), input(nil, readErr), row, wait)
		if !errors.Is(err, readErr) {
			t.Fatalf("err = %v, want %v", err, readErr)
		}
		if got.waiting || got.release || got.evidence != nil || builds != 0 {
			t.Errorf("reading = %+v after %d build(s), want an empty reading and no wait built", got, builds)
		}
	})
}
