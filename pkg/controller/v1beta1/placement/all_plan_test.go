package placement

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func TestAllPlanSurvivesMemberStatusUpdates(t *testing.T) {
	for _, tt := range []struct {
		name  string
		floor *int
	}{
		{name: "inherited runtime floor"},
		{name: "explicit service floor", floor: ptr.To(3)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newBackendFixture(t, v1beta1.PlacementModeAll)
			if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
				t.Fatal(err)
			}
			f.source.Spec.Engine.MinReplicas = tt.floor
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			source := f.reconcile(t)
			want := source.Status.Placement.Plan.DeepCopy()
			for name, cl := range f.workers {
				reads := 0
				watched := interceptor.NewClient(cl, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := c.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if member, ok := obj.(*v1beta1.InferenceService); ok {
						reads++
						member.ResourceVersion = strconv.Itoa(reads)
						member.Status.ObservedGeneration = int64(reads)
					}
					return nil
				}})
				f.connections.m[name] = workloadcluster.NewNeverCachingClient(watched)
			}
			for range 3 {
				proposal, err := f.reconciler.allProposal(t.Context(), source, f.clusters, []string{"member-a", "member-b"})
				if err != nil {
					t.Fatal(err)
				}
				for name, assignment := range proposal.Assignments {
					if diff := cmp.Diff(false, assignment.HomeInputsPending); diff != "" {
						t.Fatalf("%s home resolution (-want +got):\n%s", name, diff)
					}
				}
				source, err = (plan.Store{Client: f.reconciler.Client, Reader: f.reconciler.APIReader}).Persist(t.Context(), source, proposal)
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(want, source.Status.Placement.Plan); diff != "" {
					t.Fatalf("accepted plan changed (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestAllPlanInitialApplication(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*testing.T, *backendFixture)
		want map[string]int32
	}{
		{name: "full source policy on every home", want: map[string]int32{"member-a": 3, "member-b": 3}},
		{name: "unreachable matched peer permits independent provisioning", edit: func(_ *testing.T, f *backendFixture) { delete(f.connections.m, "member-b") }, want: map[string]int32{"member-a": 3}},
		{name: "runtime floor remains inherited", edit: func(t *testing.T, f *backendFixture) {
			f.source.Spec.Engine.MinReplicas = nil
			for _, cl := range f.workers {
				rt := backendTestRuntime()
				if err := cl.Get(t.Context(), client.ObjectKeyFromObject(rt), rt); err != nil {
					t.Fatal(err)
				}
				rt.Spec.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(3)}}
				if err := cl.Update(t.Context(), rt); err != nil {
					t.Fatal(err)
				}
			}
		}, want: map[string]int32{"member-a": 3, "member-b": 3}},
		{name: "unresolved home floor holds only that home", edit: func(t *testing.T, f *backendFixture) {
			b := f.workers["member-b"]
			unresolved := backendTestRuntime()
			if err := b.Get(t.Context(), client.ObjectKeyFromObject(unresolved), unresolved); err != nil {
				t.Fatal(err)
			}
			unresolved.Spec.EngineConfig = nil
			if err := b.Update(t.Context(), unresolved); err != nil {
				t.Fatal(err)
			}

			f.source.Spec.Engine.MinReplicas = nil
			cl := f.workers["member-a"]
			rt := backendTestRuntime()
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(rt), rt); err != nil {
				t.Fatal(err)
			}
			rt.Spec.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(3)}}
			if err := cl.Update(t.Context(), rt); err != nil {
				t.Fatal(err)
			}
		}, want: map[string]int32{"member-a": 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newBackendFixture(t, v1beta1.PlacementModeAll)
			if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
				t.Fatal(err)
			}
			f.source.Spec.Engine.MinReplicas = ptr.To(3)
			f.source.Spec.Engine.MaxReplicas = 9
			if tt.edit != nil {
				tt.edit(t, f)
			}
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			// A member Create must observe the complete accepted allocation first.
			for name, cl := range f.workers {
				if _, ok := f.connections.m[name]; !ok {
					continue
				}
				watched := interceptor.NewClient(cl, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*v1beta1.InferenceService); ok {
						live := &v1beta1.InferenceService{}
						if err := f.reconciler.Get(ctx, client.ObjectKeyFromObject(f.source), live); err != nil {
							return err
						}
						if live.Status.Placement == nil || live.Status.Placement.Plan == nil {
							t.Fatal("member write preceded persisted authority")
						}
					}
					return c.Create(ctx, obj, opts...)
				}})
				f.connections.m[name] = workloadcluster.NewNeverCachingClient(watched)
			}
			if _, err := f.reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.source)}); err != nil {
				t.Fatal(err)
			}
			live := &v1beta1.InferenceService{}
			if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), live); err != nil {
				t.Fatal(err)
			}
			got := map[string]int32{}
			for name, cl := range f.workers {
				member := &v1beta1.InferenceService{}
				err := cl.Get(t.Context(), client.ObjectKeyFromObject(f.source), member)
				if apierrors.IsNotFound(err) {
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				p, err := protocol.FromDerived(member)
				if err != nil || p == nil {
					t.Fatalf("missing full-home authority: %v", err)
				}
				count, err := protocol.ValidateReplicaFloors(p.ReplicaFloors)
				if err != nil {
					t.Fatal(err)
				}
				got[name] = count
				if diff := cmp.Diff(f.source.Spec.Engine, member.Spec.Engine); diff != "" {
					t.Fatalf("full policy changed: %s", diff)
				}
				if diff := cmp.Diff(live.Status.Placement.Plan.ID, p.PlanID); diff != "" {
					t.Fatal(diff)
				}
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestAllPlanWholeHomeBudget(t *testing.T) {
	for _, tt := range []struct {
		name   string
		edit   func(*v1beta1.InferenceService, map[string]plannedHomeObservation)
		wantB  int32
		reason string
	}{
		{name: "complete home fits", wantB: 3, reason: "AwaitingMemberConvergence"},
		{name: "partial home is forbidden", edit: func(s *v1beta1.InferenceService, _ map[string]plannedHomeObservation) {
			s.Spec.Placement.MaxSurge = ptr.To[int32](2)
		}, reason: "SurgeBudgetExhausted"},
		{name: "existing rollout shares allowance", edit: func(_ *v1beta1.InferenceService, o map[string]plannedHomeObservation) {
			h := o["a"]
			h.RolloutReserved = 1
			o["a"] = h
		}, reason: "SurgeBudgetExhausted"},
		{name: "unset allowance blocks move", edit: func(s *v1beta1.InferenceService, _ map[string]plannedHomeObservation) {
			s.Spec.Placement.MaxSurge = nil
			s.Status.Placement.Plan.PauseSurge = false
		}, reason: "MigrationBlocked"},
		{name: "pause required on standing home", edit: func(_ *v1beta1.InferenceService, o map[string]plannedHomeObservation) {
			h := o["a"]
			h.PauseAcknowledged = false
			o["a"] = h
		}, reason: "AwaitingSurgePause"},
		{name: "unknown inventory cannot fund movement", edit: func(s *v1beta1.InferenceService, _ map[string]plannedHomeObservation) {
			s.Status.Placement.Candidates[0].Allocation.InventoryPending = true
		}, reason: "AwaitingHomeInputs"},
		{name: "unreadable runtime retains authority but holds movement", edit: func(s *v1beta1.InferenceService, _ map[string]plannedHomeObservation) {
			s.Status.Placement.Candidates[1].Allocation.HomeInputsPending = true
		}, reason: "AwaitingHomeInputs"},
		{name: "unresolved target holds movement", edit: func(s *v1beta1.InferenceService, _ map[string]plannedHomeObservation) {
			s.Status.Placement.Candidates[1].Allocation.DesiredHome.InputDigest = "old-intent"
		}, reason: "AwaitingHomeInputs"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 3}}}
			s := srcISVCMode(v1beta1.PlacementModeAll, "")
			s.Spec.Placement.MaxSurge = ptr.To[int32](3)
			s.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeAll, InputDigest: "intent", AssignedReplicas: 3, PauseSurge: true}, Candidates: []v1beta1.CandidatePlacement{
				{Cluster: "a", Allocation: &v1beta1.CandidateAllocationStatus{CurrentHome: home.DeepCopy(), OriginalReplicas: 3, CurrentReplicas: 3}},
				{Cluster: "b", Allocation: &v1beta1.CandidateAllocationStatus{DesiredHome: home.DeepCopy(), DesiredReplicas: 3, Matched: true}},
			}}
			o := map[string]plannedHomeObservation{
				"a": {Home: allocation.Home{Known: true, Applied: true, Routable: true, Ready: 3, Occupied: 3}, PauseAcknowledged: true},
				"b": {Home: allocation.Home{Known: true, Absent: true, Eligible: true}},
			}
			if tt.edit != nil {
				tt.edit(s, o)
			}
			got, err := advanceSplitPlan(s, o)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[string]int32{"a": 3, "b": tt.wantB}, got.Targets); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.reason, got.Reason); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestAllInventoryRecoveryAcceptsRetarget(t *testing.T) {
	for _, failure := range []string{"disconnected member", "unreadable member inventory"} {
		t.Run(failure, func(t *testing.T) {
			f := newBackendFixture(t, v1beta1.PlacementModeAll)
			connected := f.connections.m["member-b"]
			if failure == "disconnected member" {
				delete(f.connections.m, "member-b")
			} else {
				unreadable := interceptor.NewClient(f.workers["member-b"], interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*v1beta1.InferenceReplicaList); ok {
						return errors.New("member inventory unavailable")
					}
					return cl.List(ctx, list, opts...)
				}})
				f.connections.m["member-b"] = workloadcluster.NewNeverCachingClient(unreadable)
			}
			if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
				t.Fatal(err)
			}
			f.source.Spec.Engine.MinReplicas = ptr.To(3)
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			initial := f.reconcile(t)
			original := initial.Status.Placement.Plan.AdoptionDigest
			if original == "" {
				t.Fatal("unknown home inventory was treated as complete")
			}
			if err := f.workers["member-a"].Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); err != nil {
				t.Fatalf("independent home was not provisioned: %v", err)
			}
			wc := readyWC("member-c", nil)
			if err := f.reconciler.Create(t.Context(), wc); err != nil {
				t.Fatal(err)
			}
			worker := emptyWorker(f.reconciler.Scheme)
			f.connections.m[wc.Name] = workloadcluster.NewNeverCachingClient(worker)
			held := f.reconcile(t)
			if diff := cmp.Diff(original, held.Status.Placement.Plan.AdoptionDigest); diff != "" {
				t.Fatal(diff)
			}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); !apierrors.IsNotFound(err) {
				t.Fatalf("retarget created a destination before inventory completed: %v", err)
			}

			f.connections.m["member-b"] = connected
			recovered := f.reconcile(t)
			if diff := cmp.Diff("", recovered.Status.Placement.Plan.AdoptionDigest); diff != "" {
				t.Fatalf("inventory hold survived recovery (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(int64(9), recovered.Status.Placement.Plan.AssignedReplicas); diff != "" {
				t.Fatal(diff)
			}
			got := map[string]v1beta1.CandidateAllocationStatus{}
			for _, candidate := range recovered.Status.Placement.Candidates {
				allocation := candidate.Allocation
				if allocation == nil || allocation.DesiredHome == nil {
					t.Fatalf("%s has no desired home policy", candidate.Cluster)
				}
				if diff := cmp.Diff(recovered.Status.Placement.Plan.InputDigest, allocation.DesiredHome.InputDigest); diff != "" {
					t.Fatal(diff)
				}
				got[candidate.Cluster] = v1beta1.CandidateAllocationStatus{
					OriginalReplicas: allocation.OriginalReplicas, CurrentReplicas: allocation.CurrentReplicas,
					DesiredReplicas: allocation.DesiredReplicas, InventoryPending: allocation.InventoryPending,
					HomeInputsPending: allocation.HomeInputsPending,
				}
			}
			want := map[string]v1beta1.CandidateAllocationStatus{
				"member-a": {CurrentReplicas: 3, DesiredReplicas: 3},
				"member-b": {DesiredReplicas: 3},
				"member-c": {DesiredReplicas: 3},
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("recovered plan changed its original budget (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFullHomePausePreservesAcceptedFloor(t *testing.T) {
	for _, tt := range []struct {
		name    string
		pause   bool
		initial *int
		want    *int
	}{
		{name: "inherited floor frozen during pause", pause: true, want: ptr.To(2)},
		{name: "changed member floor frozen during pause", pause: true, initial: ptr.To(8), want: ptr.To(2)},
		{name: "release preserves inheritance", pause: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedTestSource()
			source.Status.Placement.Plan.PauseSurge = tt.pause
			a := source.Status.Placement.Candidates[0].Allocation
			a.CurrentHome = &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}}}
			member := DeriveISVC(source, "", "")
			member.UID = "member-uid"
			member.Spec.Engine.MinReplicas = tt.initial
			member.Spec.Engine.MaxReplicas = 9
			worker := emptyWorker(testScheme(t))
			if err := worker.Create(t.Context(), member); err != nil {
				t.Fatal(err)
			}
			connections := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, _ := newPlacer(testScheme(t), connections, source, plannedTestRegistration())
			if err := r.syncPlannedPolicy(t.Context(), source, source.Status.Placement.Candidates[0]); err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(member), got); err != nil {
				t.Fatal(err)
			}
			want := member.Spec.DeepCopy()
			want.Engine.MinReplicas = tt.want
			if diff := cmp.Diff(want, &got.Spec); diff != "" {
				t.Fatal(diff)
			}
			policy, err := protocol.FromDerived(got)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(a.CurrentHome.ReplicaFloors, policy.ReplicaFloors); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestAllUnknownRuntimeRetainsDesiredAuthority(t *testing.T) {
	f := newBackendFixture(t, v1beta1.PlacementModeAll)
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
		t.Fatal(err)
	}
	f.source.Spec.Engine.MinReplicas = ptr.To(3)
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.source)}
	if _, err := f.reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := f.reconciler.Get(t.Context(), request.NamespacedName, f.source); err != nil {
		t.Fatal(err)
	}
	before := f.source.Status.Placement.Candidates[0].Allocation.DeepCopy()
	if err := f.workers["member-a"].Delete(t.Context(), backendTestRuntime()); err != nil {
		t.Fatal(err)
	}
	proposal, err := f.reconciler.allProposal(t.Context(), f.source, f.clusters, []string{"member-a", "member-b"})
	if err != nil {
		t.Fatal(err)
	}
	got := proposal.Assignments["member-a"]
	if !got.HomeInputsPending {
		t.Fatal("failed runtime read retained movement authority")
	}
	if diff := cmp.Diff(before.DesiredHome, got.DesiredHome); diff != "" {
		t.Fatalf("last desired authority lost: %s", diff)
	}
	if diff := cmp.Diff(before.OriginalReplicas, got.OriginalReplicas); diff != "" {
		t.Fatalf("migration baseline changed: %s", diff)
	}
}
