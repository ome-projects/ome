package placement

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// TestSplitRaisedOrMovedSharesEndThePause changes an autoscaled Split service
// whose members all stay while each autoscaler keeps more engines than the new
// share. Each change raises or moves the shares, and the pause ends once every
// member serves its new share. At 16 engines per member for the raise, and 12
// for the weights, the replicas in use fill the move's surge allowance.
func TestSplitRaisedOrMovedSharesEndThePause(t *testing.T) {
	raise := func(s *v1beta1.InferenceService) { s.Spec.Engine.MinReplicas = ptr.To(16) }
	move := func(s *v1beta1.InferenceService) {
		s.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
	}
	running := func(engines int32) map[v1beta1.ComponentType]int32 {
		return map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: engines, v1beta1.DecoderComponent: 6, v1beta1.RouterComponent: 2}
	}
	engineCap := &v1beta1.SplitSpec{MaxPerCluster: &v1beta1.SplitCeilings{Engine: ptr.To[int32](18)}}
	for _, tt := range []struct {
		name   string
		split  *v1beta1.SplitSpec
		change func(*v1beta1.InferenceService)
		busy   map[v1beta1.ComponentType]int32
		shares map[string]int32
	}{
		{name: "raised engine minimum", change: raise, busy: autoscaledAllBusy, shares: map[string]int32{"member-a": 8, "member-b": 8}},
		{name: "raised engine minimum under an engine cap in split.maxPerCluster", split: engineCap, change: raise, busy: autoscaledAllBusy, shares: map[string]int32{"member-a": 8, "member-b": 8}},
		{name: "raised engine minimum with 16 engines per member", change: raise, busy: running(16), shares: map[string]int32{"member-a": 8, "member-b": 8}},
		{name: "raised split replicas", change: func(s *v1beta1.InferenceService) { s.Spec.Placement.Split.Replicas = ptr.To[int32](16) }, busy: autoscaledAllBusy, shares: map[string]int32{"member-a": 8, "member-b": 8}},
		{name: "moved weights", change: move, busy: autoscaledAllBusy, shares: map[string]int32{"member-a": 8, "member-b": 4}},
		{name: "moved weights with 12 engines per member", change: move, busy: running(12), shares: map[string]int32{"member-a": 8, "member-b": 4}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := autoscaledSplitFixture(t)
			if tt.split != nil {
				f = autoscaledSplitFixtureWith(t, tt.split)
			}
			changeAutoscaledSplit(t, f, tt.change)
			paused := false
			got := serveAutoscaledSplit(t, f, tt.busy, tt.busy, func(got *v1beta1.InferenceService) bool {
				paused = paused || got.Status.Placement.Plan.PauseSurge
				return autoscaledAllConverged(got)
			})
			if !paused {
				t.Fatalf("the change did not pause member growth: %s", describeAutoscaledAll(t, f, got))
			}
			if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
				t.Fatalf("member growth stayed paused: %s", describeAutoscaledAll(t, f, got))
			}
			for _, candidate := range got.Status.Placement.Candidates {
				want := tt.shares[candidate.Cluster]
				if a := candidate.Allocation; a.OriginalReplicas != want || a.CurrentReplicas != want || a.DesiredReplicas != want {
					t.Fatalf("%s did not keep its share of %d: %+v", candidate.Cluster, want, a)
				}
				member, _ := allMemberOn(t, f, candidate.Cluster)
				if policy, err := protocol.FromDerived(member); err != nil || policy == nil || policy.PauseSurge {
					t.Fatalf("%s still defers autoscaler growth: %+v (%v)", candidate.Cluster, policy, err)
				}
			}
		})
	}
}

// TestSplitUndoneRaiseEndsThePause raises the engine minimum of the autoscaled
// Split service and sets it back before the members apply it. Each share ends
// where it began while the autoscalers keep more replicas, and the pause ends.
func TestSplitUndoneRaiseEndsThePause(t *testing.T) {
	f := autoscaledSplitFixture(t)
	setAutoscaledAll(t, f, 16, 8, "")
	if raised := f.reconcile(t); !raised.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the raise did not pause member growth: %s", describeAutoscaledAll(t, f, raised))
	}
	setAutoscaledAll(t, f, 12, 8, "")
	got := serveAutoscaledSplit(t, f, autoscaledAllBusy, autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member growth stayed paused: %s", describeAutoscaledAll(t, f, got))
	}
	for _, candidate := range got.Status.Placement.Candidates {
		if a := candidate.Allocation; a.OriginalReplicas != 6 || a.CurrentReplicas != 6 || a.DesiredReplicas != 6 {
			t.Fatalf("%s did not return to its share of 6: %+v", candidate.Cluster, a)
		}
	}
}

// TestSplitRaisedShareWaitsForNewReplicas raises the engine minimum from 12 to
// 16, so each share grows from 6 to 8. member-a's autoscaler already keeps 9
// engines, while member-b runs 6 until its two new engines serve. The pause
// holds until then and ends after it, with member-a still above its share.
func TestSplitRaisedShareWaitsForNewReplicas(t *testing.T) {
	f := autoscaledSplitFixture(t)
	setAutoscaledAll(t, f, 16, 8, "")
	serve := func(passes int, memberB map[v1beta1.ComponentType]int32) *v1beta1.InferenceService {
		var got *v1beta1.InferenceService
		for range passes {
			runAutoscaledSplitMember(t, f, "member-a", autoscaledAllBusy, autoscaledAllBusy)
			member, _ := allMemberOn(t, f, "member-b")
			runAutoscaledSplitMember(t, f, "member-b", memberB, memberMinimums(member))
			routeAutoscaledAll(t, f)
			got = f.reconcile(t)
		}
		return got
	}
	held := serve(4, autoscaledAllRunning)
	if condition := held.Status.GetCondition(v1beta1.PlacementConverged); !held.Status.Placement.Plan.PauseSurge || condition == nil || condition.Reason != "AwaitingMemberCleanup" {
		t.Fatalf("the move did not finish and wait for member-b's new engines: %s", describeAutoscaledAll(t, f, held))
	}
	got := serve(6, map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 8, v1beta1.DecoderComponent: 4, v1beta1.RouterComponent: 2})
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("the served engines did not end the pause: %s", describeAutoscaledAll(t, f, got))
	}
}

// TestSplitPauseWaitsForAStaleDecoderMinimum starts at weights 2:1 with 6
// engines and 30 decoders. While member-a is not eligible, the owner sets 8
// engines, 48 decoders and equal weights. member-a keeps 4 engines, but its
// decoder share rises from 20 to 24, and OME cannot place it, so its spec keeps
// 20. Once it is eligible again with 20 ready decoders, the pause holds until it
// serves 24.
func TestSplitPauseWaitsForAStaleDecoderMinimum(t *testing.T) {
	f := singleFixture(t, true)
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	f.source.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: engineCap(18)}
	f.source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(6), 36
	f.source.Spec.Decoder.MinReplicas, f.source.Spec.Decoder.MaxReplicas = ptr.To(30), 180
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	var got *v1beta1.InferenceService
	for range 8 {
		for _, name := range []string{"member-a", "member-b"} {
			member, present := allMemberOn(t, f, name)
			if !present {
				t.Fatalf("%s lost its member", name)
			}
			counts := memberMinimums(member)
			runAutoscaledSplitMember(t, f, name, counts, counts)
		}
		routeAutoscaledAll(t, f)
		got = f.reconcile(t)
	}
	if !autoscaledAllConverged(got) || got.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the service did not settle on its first shares: %s", describeAutoscaledAll(t, f, got))
	}
	setClusterReady(t, f, "member-a", false)
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(8), ptr.To(48)
		s.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 1, "member-b": 1})
	})
	serve := func(passes int, decodersA int32) {
		for range passes {
			// member-a asks for 6 engines and 30 decoders; member-b runs its new share.
			runAutoscaledSplitMember(t, f, "member-a",
				map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: decodersA},
				map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 6, v1beta1.DecoderComponent: 30})
			runAutoscaledSplitMember(t, f, "member-b",
				map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: 24},
				map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: 24})
			routeAutoscaledAll(t, f)
			got = f.reconcile(t)
		}
	}
	serve(10, 20)
	if !got.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the pause ended while member-a was not eligible: %s", describeAutoscaledAll(t, f, got))
	}
	setClusterReady(t, f, "member-a", true)
	member, _ := allMemberOn(t, f, "member-a")
	if memberMinimums(member)[v1beta1.DecoderComponent] != 20 {
		t.Fatalf("member-a's spec does not keep its old decoder minimum: %v", memberMinimums(member))
	}
	serve(1, 20)
	if !got.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the pause ended while member-a served 20 of its 24 decoders: %s", describeAutoscaledAll(t, f, got))
	}
	serve(6, 24)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("the pause did not end once member-a served its decoder share: %s", describeAutoscaledAll(t, f, got))
	}
}

// setClusterReady marks a member cluster ready, or clears its conditions so
// placement treats it as not eligible.
func setClusterReady(t *testing.T, f *backendFixture, name string, ready bool) {
	t.Helper()
	cluster := &v1beta1.WorkloadCluster{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKey{Name: name}, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Status.Conditions = nil
	if ready {
		cluster.Status.Conditions = []metav1.Condition{{Type: v1beta1.WorkloadClusterReady, Status: metav1.ConditionTrue, Reason: "Reachable", LastTransitionTime: metav1.Now()}}
	}
	if err := f.reconciler.Update(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
}

// changeAutoscaledSplit applies change to the stored source.
func changeAutoscaledSplit(t *testing.T, f *backendFixture, change func(*v1beta1.InferenceService)) {
	t.Helper()
	source := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	change(source)
	if err := f.reconciler.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
}

// TestSplitSharesInPlaceNeedAPrimaryCap: a share change stays in place only
// under a cap on the primary component. A source that declares neither an
// engine nor a decoder has no primary, so it has no such cap.
func TestSplitSharesInPlaceNeedAPrimaryCap(t *testing.T) {
	source := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{
			Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSplit,
			Split: &v1beta1.SplitSpec{MaxPerCluster: &v1beta1.SplitCeilings{Default: ptr.To[int32](18)}},
		}},
		Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{}}},
	}
	if splitSharesChangedInPlace(source, nil) {
		t.Fatal("a source without a primary component changed its shares in place")
	}
}
