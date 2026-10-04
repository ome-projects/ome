package placement

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func allZeroFixture(t *testing.T, pd bool) *backendFixture {
	t.Helper()
	f := singleFixture(t, pd)
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeAll
	f.source.Spec.Engine.MinReplicas = ptr.To(0)
	if pd {
		f.source.Spec.Decoder.MinReplicas = ptr.To(0)
	}
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAllZeroFloorPolicyChanges(t *testing.T) {
	for _, tt := range []struct {
		name             string
		current, desired int32
		pending, pause   bool
		want             int32
		wantErr          bool
	}{
		{name: "authored scale to zero retains the home", current: 2, want: 0},
		{name: "authored positive floor refreshes the existing home", desired: 2, want: 2},
		{name: "unresolved input retains the accepted floor", current: 2, pending: true, want: 2},
		{name: "active movement cannot accept a zero contract", current: 2, pause: true, want: 2, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := func(n int32) *v1beta1.PlacementHomePolicy {
				return &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: n}}}
			}
			proposal := plan.Proposal{Mode: v1beta1.PlacementModeAll, InputDigest: "intent", PauseSurge: tt.pause, Assignments: map[string]v1beta1.CandidateAllocationStatus{
				"member-a": {Matched: true, ClusterUID: "member-a-uid", CurrentHome: home(tt.current), DesiredHome: home(tt.desired), OriginalReplicas: tt.current, CurrentReplicas: tt.current, DesiredReplicas: tt.desired, HomeInputsPending: tt.pending},
			}}
			source := srcISVCMode(v1beta1.PlacementModeAll, "")
			source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeAll}}
			refresh, err := (&Reconciler{}).prepareAllZeroFloors(t.Context(), source, &proposal)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error (-want +got): %s; err=%v", diff, err)
			}
			if diff := cmp.Diff(false, refresh); diff != "" {
				t.Fatal(diff)
			}
			a := proposal.Assignments["member-a"]
			if diff := cmp.Diff(home(tt.want), a.CurrentHome); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.want, a.CurrentReplicas); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.want, a.OriginalReplicas); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestAllZeroFloorRetention(t *testing.T) {
	for _, tt := range []struct {
		name        string
		pd, restart bool
		replicas    int32
		edit        func(*testing.T, *backendFixture)
		wantReason  string
	}{
		{name: "idle engine homes retain their policies"},
		{name: "idle disaggregated homes retain every component", pd: true},
		{name: "restart retains idle homes", restart: true},
		{name: "autoscaler requests are not orphaned allocations", replicas: 3},
		{name: "unknown inventory retains accepted authority", wantReason: "AwaitingMemberConvergence", edit: func(_ *testing.T, f *backendFixture) {
			worker := interceptor.NewClient(f.workers["member-a"], interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok {
					return fmt.Errorf("inventory unavailable")
				}
				return cl.List(ctx, list, opts...)
			}})
			f.connections.m["member-a"] = workloadcluster.NewNeverCachingClient(worker)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := allZeroFixture(t, tt.pd)
			f.reconcile(t)
			for name := range f.workers {
				seedSingleAdmission(t, f, name, declaredComponents(f.source)...)
				acknowledgeSingleFloor(t, f, name, tt.replicas)
			}
			stored := f.reconcile(t)
			if tt.restart {
				f.reconciler, _ = newPlacer(testScheme(t), f.connections, stored, &f.clusters[0], &f.clusters[1])
			}
			if tt.edit != nil {
				tt.edit(t, f)
			}
			for range 3 {
				got := f.reconcile(t)
				if diff := cmp.Diff(stored.Status.Placement.Plan, got.Status.Placement.Plan); diff != "" {
					t.Fatalf("retained authority changed (-want +got):\n%s", diff)
				}
				wantReason := tt.wantReason
				if wantReason == "" {
					wantReason = "AllocationConverged"
				}
				if diff := cmp.Diff(wantReason, got.Status.GetCondition(v1beta1.PlacementConverged).Reason); diff != "" {
					t.Fatal(diff)
				}
				for _, candidate := range got.Status.Placement.Candidates {
					if candidate.Allocation.CurrentHome == nil || candidate.Allocation.DesiredHome == nil || candidate.Allocation.DrainRequested {
						t.Fatalf("zero floor lost retained authority: %+v", candidate.Allocation)
					}
					member := &v1beta1.InferenceService{}
					if err := f.workers[candidate.Cluster].Get(t.Context(), client.ObjectKeyFromObject(got), member); err != nil {
						t.Fatal(err)
					}
				}
				if tt.replicas == 0 {
					if diff := cmp.Diff(v1beta1.PlacementPhasePlaced, got.Status.Placement.Phase); diff != "" {
						t.Fatal(diff)
					}
				}
			}
		})
	}
}

func TestAllZeroFloorMovement(t *testing.T) {
	for _, pd := range []bool{false, true} {
		t.Run(fmt.Sprintf("disaggregated=%t", pd), func(t *testing.T) {
			f := allZeroFixture(t, pd)
			selectHome := func(s *v1beta1.InferenceService, name string) {
				s.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}}}}
			}
			selectHome(f.source, "member-a")
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", declaredComponents(f.source)...)
			acknowledgeSingleFloor(t, f, "member-a", 3)
			source := f.reconcile(t)
			accepted := source.Status.Placement.Plan.DeepCopy()
			selectHome(source, "member-b")
			source.Spec.Placement.MaxSurge = ptr.To[int32](3)
			source.Generation++
			if err := f.reconciler.Update(t.Context(), source); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				source = f.reconcile(t)
				if diff := cmp.Diff("PositiveFloorRequired", source.Status.GetCondition(v1beta1.PlacementConverged).Reason); diff != "" {
					t.Fatal(diff)
				}
				if diff := cmp.Diff(accepted, source.Status.Placement.Plan); diff != "" {
					t.Fatal(diff)
				}
				err := f.workers["member-b"].Get(t.Context(), client.ObjectKeyFromObject(source), &v1beta1.InferenceService{})
				if !apierrors.IsNotFound(err) {
					t.Fatalf("held move wrote the destination: %v", err)
				}
			}
			source.Spec.Engine.MinReplicas = ptr.To(2)
			if pd {
				source.Spec.Decoder.MinReplicas = ptr.To(2)
			}
			source.Generation++
			if err := f.reconciler.Update(t.Context(), source); err != nil {
				t.Fatal(err)
			}
			source = f.reconcile(t)
			if diff := cmp.Diff(false, source.Status.Placement.Plan.PauseSurge); diff != "" {
				t.Fatal(diff)
			}
			member := &v1beta1.InferenceService{}
			if err := f.workers["member-a"].Get(t.Context(), client.ObjectKeyFromObject(source), member); err != nil {
				t.Fatal(err)
			}
			policy, err := protocol.FromDerived(member)
			if err != nil {
				t.Fatal(err)
			}
			for _, floor := range policy.ReplicaFloors {
				if diff := cmp.Diff(int32(2), floor.Replicas); diff != "" {
					t.Fatal(diff)
				}
			}
			if diff := cmp.Diff(ptr.To(2), member.Spec.Engine.MinReplicas); diff != "" {
				t.Fatal(diff)
			}
			acknowledgeSingleFloor(t, f, "member-a", 3)
			source = f.reconcile(t)
			if diff := cmp.Diff(true, source.Status.Placement.Plan.PauseSurge); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(int32(2), source.Status.Placement.Candidates[0].Allocation.OriginalReplicas); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestAllZeroFloorConvergence(t *testing.T) {
	for _, tt := range []struct {
		name   string
		home   allocation.Home
		remove bool
		want   allocation.Step
	}{
		{name: "idle policy is acknowledged", home: allocation.Home{Known: true, Applied: true}, want: allocation.Step{Targets: map[string]int32{"a": 0}, Complete: true}},
		{name: "autoscaled occupancy remains active", home: allocation.Home{Known: true, Applied: true, Ready: 3, Occupied: 3, Routable: true}, want: allocation.Step{Targets: map[string]int32{"a": 0}, Complete: true}},
		{name: "missing component cannot acknowledge zero", home: allocation.Home{Known: true}, want: allocation.Step{Targets: map[string]int32{"a": 0}, Reason: "AwaitingMemberConvergence"}},
		{name: "unreachable home is not empty", want: allocation.Step{Targets: map[string]int32{"a": 0}, Reason: "AwaitingMemberConvergence"}},
		{name: "zero floor cannot authorize retirement", home: allocation.Home{Known: true, Applied: true}, remove: true, want: allocation.Step{Targets: map[string]int32{"a": 0}, Reason: "PositiveFloorRequired"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}}}
			source := &v1beta1.InferenceService{Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{Candidates: []v1beta1.CandidatePlacement{{Cluster: "a", Allocation: &v1beta1.CandidateAllocationStatus{CurrentHome: home, DesiredHome: home.DeepCopy()}}}}}}
			if tt.remove {
				source.Status.Placement.Candidates[0].Allocation.DesiredHome = nil
			}
			got := advanceAllZeroFloors(source, map[string]plannedHomeObservation{"a": {Home: tt.home}})
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// TestAllZeroFloorUnreadableObservationKeepsAppliedPlan retains idle zero-floor
// homes while one home's inventory read fails on alternating passes. The idle
// plan never changes and the home's acknowledged plan survives every pass; an
// unreadable pass reports only an unknown observation.
func TestAllZeroFloorUnreadableObservationKeepsAppliedPlan(t *testing.T) {
	f := allZeroFixture(t, false)
	f.reconcile(t)
	for name := range f.workers {
		seedSingleAdmission(t, f, name, declaredComponents(f.source)...)
		acknowledgeSingleFloor(t, f, name, 0)
	}
	stored := f.reconcile(t)
	if candidate := candidateOf(t, stored, "member-a"); !candidate.ObservationKnown || candidate.AppliedPlanID != stored.Status.Placement.Plan.ID {
		t.Fatalf("idle home has not acknowledged the accepted plan: %+v", candidate)
	}
	readable := workloadcluster.NewNeverCachingClient(f.workers["member-a"])
	unreadable := unreadableAllMember(f, "member-a")
	for pass, known := range []bool{true, false, true, false, false, true} {
		f.connections.m["member-a"] = readable
		if !known {
			f.connections.m["member-a"] = unreadable
		}
		got := f.reconcile(t)
		if diff := cmp.Diff(stored.Status.Placement.Plan, got.Status.Placement.Plan); diff != "" {
			t.Fatalf("pass %d changed the idle plan (-want +got):\n%s", pass, diff)
		}
		candidate := candidateOf(t, got, "member-a")
		if diff := cmp.Diff(got.Status.Placement.Plan.ID, candidate.AppliedPlanID); diff != "" {
			t.Fatalf("pass %d (readable=%t) lost the acknowledged plan (-want +got):\n%s", pass, known, diff)
		}
		if diff := cmp.Diff(known, candidate.ObservationKnown); diff != "" {
			t.Fatalf("pass %d observation flag (-want +got):\n%s", pass, diff)
		}
		if candidate.Allocation.CurrentHome == nil || candidate.Allocation.DesiredHome == nil || candidate.Allocation.DrainRequested || candidate.ReadyReplicas != 0 {
			t.Fatalf("pass %d changed the retained zero floor: %+v", pass, candidate)
		}
		wantReason := "AllocationConverged"
		if !known {
			wantReason = "AwaitingMemberConvergence"
		}
		if diff := cmp.Diff(wantReason, got.Status.GetCondition(v1beta1.PlacementConverged).Reason); diff != "" {
			t.Fatalf("pass %d convergence (-want +got):\n%s", pass, diff)
		}
		if diff := cmp.Diff(v1beta1.PlacementPhasePlaced, got.Status.Placement.Phase); diff != "" {
			t.Fatalf("pass %d phase (-want +got):\n%s", pass, diff)
		}
	}
}
