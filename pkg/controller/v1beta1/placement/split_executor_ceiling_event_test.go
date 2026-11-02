package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ratioEventFixture is an engine-only ratio-form Split service over member-a
// and member-b with the given engine bounds and ratios, and a recorder.
func ratioEventFixture(t *testing.T, minimum, maximum int, ratios map[string]*v1beta1.ReplicaRatio) (*backendFixture, *record.FakeRecorder) {
	t.Helper()
	f := singleFixture(t, false)
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	f.source.Spec.Placement.Split = nil
	f.source.Spec.Placement.ClusterAffinity = engineRatios(ratios)
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(minimum), maximum
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	recorder := record.NewFakeRecorder(64)
	f.reconciler.Recorder = recorder
	return f, recorder
}

// TestSplitCeilingRaisedEventOncePerPlan divides 8 engines by min ratios 3 and
// 1, giving member-a 6 and member-b 2, and the maximum of 8 by max ratios 1 and
// 3, giving 2 and 6. member-a's maximum is raised to its 6, so the members'
// maximums add up to 12. The warning appears once for that plan, and once more
// when a new maximum of 9 makes a new plan (6 and 7, adding up to 13).
func TestSplitCeilingRaisedEventOncePerPlan(t *testing.T) {
	f, recorder := ratioEventFixture(t, 8, 8, map[string]*v1beta1.ReplicaRatio{"member-a": ratio(3, 1), "member-b": ratio(1, 3)})
	f.reconcile(t)
	f.reconcile(t)
	want := []string{"Warning SplitCeilingRaised engine maxReplicas on cluster member-a raised from 2 to its minReplicas 6; the members' maxReplicas add up to 12, 4 more than the service's maxReplicas 8"}
	if diff := cmp.Diff(want, recordedEvents(recorder)); diff != "" {
		t.Fatalf("events (-want +got):\n%s", diff)
	}

	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) { s.Spec.Engine.MaxReplicas = 9 })
	f.reconcile(t)
	f.reconcile(t)
	want = []string{"Warning SplitCeilingRaised engine maxReplicas on cluster member-a raised from 2 to its minReplicas 6; the members' maxReplicas add up to 13, 4 more than the service's maxReplicas 9"}
	if diff := cmp.Diff(want, recordedEvents(recorder)); diff != "" {
		t.Fatalf("events after the new maximum (-want +got):\n%s", diff)
	}
}

// TestSplitCeilingRaisedEventNotRaised divides 12 to 40 engines by ratios
// {2, 3} and {1, 1}: members get 8 to 30 and 4 to 10, and no maximum is raised.
func TestSplitCeilingRaisedEventNotRaised(t *testing.T) {
	f, recorder := ratioEventFixture(t, 12, 40, map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 3), "member-b": ratio(1, 1)})
	got := f.reconcile(t)
	f.reconcile(t)
	if got.Status.Placement == nil || got.Status.Placement.Plan == nil {
		t.Fatalf("no plan was accepted: %+v", got.Status.Conditions)
	}
	if events := recordedEvents(recorder); len(events) > 0 {
		t.Fatalf("unexpected events: %q", events)
	}
}

// TestSplitCeilingRaisedEventOmittedMax divides 8 engines by ratios {min 3}
// and {}: an omitted min or max counts as 1, so the min ratios are 3 and 1
// (6 and 2 engines) and the max ratios are 1 and 1 (4 and 4). member-a's
// maximum of 4 is raised to its minimum of 6.
func TestSplitCeilingRaisedEventOmittedMax(t *testing.T) {
	f, recorder := ratioEventFixture(t, 8, 8, map[string]*v1beta1.ReplicaRatio{"member-a": {Min: ptr.To[int32](3)}, "member-b": {}})
	f.reconcile(t)
	f.reconcile(t)
	want := []string{"Warning SplitCeilingRaised engine maxReplicas on cluster member-a raised from 4 to its minReplicas 6; the members' maxReplicas add up to 10, 2 more than the service's maxReplicas 8"}
	if diff := cmp.Diff(want, recordedEvents(recorder)); diff != "" {
		t.Fatalf("events (-want +got):\n%s", diff)
	}
}
