package placement

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
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
				watched := fakeclient.Wrap(t, cl, fakeclient.EditedRead(func(member *v1beta1.InferenceService) {
					reads++
					member.ResourceVersion = strconv.Itoa(reads)
					member.Status.ObservedGeneration = int64(reads)
				}))
				f.connections.m[name] = workloadcluster.NewNeverCachingClient(watched)
			}
			for range 3 {
				proposal, _, err := f.reconciler.allProposal(t.Context(), source, f.clusters, []string{"member-a", "member-b"})
				if err != nil {
					t.Fatal(err)
				}
				for name, assignment := range proposal.Assignments {
					if diff := cmp.Diff(false, assignment.HomeInputsPending); diff != "" {
						t.Fatalf("%s member resolution (-want +got):\n%s", name, diff)
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
		{name: "full source policy on every member", want: map[string]int32{"member-a": 3, "member-b": 3}},
		{name: "zero floor retains every full-policy member", edit: func(_ *testing.T, f *backendFixture) {
			f.source.Spec.Engine.MinReplicas = ptr.To(0)
		}, want: map[string]int32{"member-a": 0, "member-b": 0}},
		{name: "zero floor progresses independently of an unreachable peer", edit: func(_ *testing.T, f *backendFixture) {
			f.source.Spec.Engine.MinReplicas = ptr.To(0)
			delete(f.connections.m, "member-b")
		}, want: map[string]int32{"member-a": 0}},
		{name: "mixed inherited floors retain the full per-member policy", edit: func(t *testing.T, f *backendFixture) {
			f.source.Spec.Engine.MinReplicas = nil
			for name, cl := range f.workers {
				rt := backendTestRuntime()
				if err := cl.Get(t.Context(), client.ObjectKeyFromObject(rt), rt); err != nil {
					t.Fatal(err)
				}
				floor := 0
				if name == "member-b" {
					floor = 3
				}
				rt.Spec.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(floor)}}
				if err := cl.Update(t.Context(), rt); err != nil {
					t.Fatal(err)
				}
			}
		}, want: map[string]int32{"member-a": 0, "member-b": 3}},
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
		{name: "unresolved member floor holds only that member", edit: func(t *testing.T, f *backendFixture) {
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
				watched := fakeclient.Wrap(t, cl, fakeclient.WithInterceptors(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
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
				}}))
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
					t.Fatalf("missing full-policy authority: %v", err)
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

func TestAllPlanWholeMemberBudget(t *testing.T) {
	for _, tt := range []struct {
		name   string
		edit   func(*v1beta1.InferenceService, map[string]plannedMemberObservation)
		wantB  int32
		reason string
	}{
		{name: "complete member fits", wantB: 3, reason: "AwaitingMemberConvergence"},
		{name: "existing rollout shares allowance", edit: func(_ *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			h := o["a"]
			h.RolloutReserved = 1
			o["a"] = h
		}, reason: "SurgeBudgetExhausted"},
		{name: "pause required on standing member", edit: func(_ *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			h := o["a"]
			h.PauseAcknowledged = false
			o["a"] = h
		}, reason: "AwaitingSurgePause"},
		{name: "unknown inventory cannot fund movement", edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Status.Placement.Candidates[0].Allocation.InventoryPending = true
		}, reason: "AwaitingHomeInputs"},
		{name: "unreadable runtime retains authority but holds movement", edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Status.Placement.Candidates[1].Allocation.HomeInputsPending = true
		}, reason: "AwaitingHomeInputs"},
		{name: "unresolved target holds movement", edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Status.Placement.Candidates[1].Allocation.DesiredHome.InputDigest = "old-intent"
		}, reason: "AwaitingHomeInputs"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 3}}}
			s := srcISVCMode(v1beta1.PlacementModeAll, "")
			s.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeAll, InputDigest: "intent", AssignedReplicas: 3, PauseSurge: true}, Candidates: []v1beta1.CandidatePlacement{
				{Cluster: "a", Allocation: &v1beta1.CandidateAllocationStatus{CurrentHome: policy.DeepCopy(), OriginalReplicas: 3, CurrentReplicas: 3}},
				{Cluster: "b", Allocation: &v1beta1.CandidateAllocationStatus{DesiredHome: policy.DeepCopy(), DesiredReplicas: 3, Matched: true}},
			}}
			o := map[string]plannedMemberObservation{
				"a": {MemberState: allocation.MemberState{Known: true, Applied: true, Routable: true, Ready: 3, Occupied: 3}, PauseAcknowledged: true},
				"b": {MemberState: allocation.MemberState{Known: true, Absent: true, Eligible: true}},
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
				unreadable := fakeclient.Wrap(t, f.workers["member-b"], fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall, errors.New("member inventory unavailable")).OnKind(&v1beta1.InferenceReplicaList{}))
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
				t.Fatal("unknown member inventory was treated as complete")
			}
			if err := f.workers["member-a"].Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); err != nil {
				t.Fatalf("independent member was not provisioned: %v", err)
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
					t.Fatalf("%s has no desired member policy", candidate.Cluster)
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

func TestFullPolicyPausePreservesAcceptedFloor(t *testing.T) {
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
	proposal, _, err := f.reconciler.allProposal(t.Context(), f.source, f.clusters, []string{"member-a", "member-b"})
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

// settleAllServingMembers converges two serving members on an All source and
// returns the fixture with its settled status.
func settleAllServingMembers(t *testing.T, replicas int32) (*backendFixture, *v1beta1.InferenceService) {
	t.Helper()
	names := []string{"member-a", "member-b"}
	f := newBackendFixture(t, v1beta1.PlacementModeAll)
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
		t.Fatal(err)
	}
	f.source.Spec.Engine.MinReplicas = ptr.To(int(replicas))
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	for _, name := range names {
		projectAllModeMember(t, f, name, replicas)
		serveAllModeMember(t, f, name, replicas)
	}
	weights := map[string]int32{"member-a": replicas, "member-b": replicas}
	var settled *v1beta1.InferenceService
	for range 8 {
		for _, name := range names {
			projectAllModeMember(t, f, name, replicas)
		}
		publishAllRouting(t, f, weights)
		settled = f.reconcile(t)
		if condition := settled.Status.GetCondition(v1beta1.PlacementConverged); condition != nil && condition.Status == corev1.ConditionTrue {
			break
		}
	}
	if condition := settled.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Status != corev1.ConditionTrue {
		t.Fatalf("serving members did not settle: %+v", condition)
	}
	return f, settled
}

// TestAllUnreadableObservationKeepsAcknowledgedCandidate settles two serving
// members, then fails one member's inventory read for a single pass. That pass
// reports the gap through the observation flag alone: the member keeps its
// acknowledged plan, endpoint and admission, every decision holds, and the
// next readable pass restores the known observation.
func TestAllUnreadableObservationKeepsAcknowledgedCandidate(t *testing.T) {
	const replicas = 3
	f, settled := settleAllServingMembers(t, replicas)
	before := candidateOf(t, settled, "member-a")
	if !before.ObservationKnown || before.AppliedPlanID != settled.Status.Placement.Plan.ID || before.ReadyReplicas != replicas || before.Endpoint == nil {
		t.Fatalf("settled member is not serving its acknowledged plan: %+v", before)
	}

	f.connections.m["member-a"] = unreadableAllMember(f, "member-a")
	unknown := f.reconcile(t)
	f.connections.m["member-a"] = workloadcluster.NewNeverCachingClient(f.workers["member-a"])
	if diff := cmp.Diff(settled.Status.Placement.Plan, unknown.Status.Placement.Plan); diff != "" {
		t.Fatalf("unreadable observation changed the accepted plan (-want +got):\n%s", diff)
	}
	type carried struct {
		Known         bool
		AppliedPlanID string
		Endpoint      string
		Phase         v1beta1.CandidatePlacementPhase
		Admitted      int32
		Ready         int32
	}
	report := func(candidate v1beta1.CandidatePlacement) carried {
		return carried{Known: candidate.ObservationKnown, AppliedPlanID: candidate.AppliedPlanID, Endpoint: candidate.Endpoint.String(), Phase: candidate.Phase, Admitted: candidate.AdmittedReplicas, Ready: candidate.ReadyReplicas}
	}
	// Unverified ready capacity is the routing weight and is withheld; every
	// other field the pass could not read stays as last known.
	want := report(before)
	want.Known, want.Ready = false, 0
	got := candidateOf(t, unknown, "member-a")
	if diff := cmp.Diff(want, report(got)); diff != "" {
		t.Fatalf("unreadable observation rewrote the candidate (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(before.Allocation, got.Allocation); diff != "" {
		t.Fatalf("unreadable observation changed the allocation (-want +got):\n%s", diff)
	}
	if condition := unknown.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Status != corev1.ConditionFalse || condition.Reason != "ObservationUnknown" {
		t.Fatalf("kept acknowledgement did not hold the allocation: %+v", condition)
	}
	if condition := unknown.Status.GetCondition(v1beta1.PlacementSatisfied); condition == nil || condition.Status == corev1.ConditionTrue {
		t.Fatalf("kept acknowledgement satisfied the plan: %+v", condition)
	}
	if _, present := allMemberOn(t, f, "member-a"); !present {
		t.Fatal("unreadable observation removed the serving member")
	}

	recovered := f.reconcile(t)
	if diff := cmp.Diff(report(before), report(candidateOf(t, recovered, "member-a"))); diff != "" {
		t.Fatalf("readable observation did not restore the candidate (-want +got):\n%s", diff)
	}
	if condition := recovered.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Status != corev1.ConditionTrue {
		t.Fatalf("readable observation did not reconverge: %+v", condition)
	}
}

// TestAllAbsentMemberWithUnreadableInventoryIsUnknown settles two serving
// members, then removes one member's placed copy while its component inventory
// cannot be listed. The standing read is conclusively absent and the planned
// read is unknown: the pass keeps the member's acknowledged plan behind an
// unknown observation instead of carrying the flag of the last readable pass.
func TestAllAbsentMemberWithUnreadableInventoryIsUnknown(t *testing.T) {
	f, settled := settleAllServingMembers(t, 3)
	before := candidateOf(t, settled, "member-a")
	if !before.ObservationKnown || before.AppliedPlanID != settled.Status.Placement.Plan.ID {
		t.Fatalf("settled member has not acknowledged the accepted plan: %+v", before)
	}
	member, present := allMemberOn(t, f, "member-a")
	if !present {
		t.Fatal("settled member has no placed copy")
	}
	if err := f.workers["member-a"].Delete(t.Context(), member); err != nil {
		t.Fatal(err)
	}
	f.connections.m["member-a"] = unreadableAllMember(f, "member-a")
	unknown := f.reconcile(t)
	if diff := cmp.Diff(settled.Status.Placement.Plan, unknown.Status.Placement.Plan); diff != "" {
		t.Fatalf("unknown pass changed the accepted plan (-want +got):\n%s", diff)
	}
	got := candidateOf(t, unknown, "member-a")
	if diff := cmp.Diff(before.AppliedPlanID, got.AppliedPlanID); diff != "" {
		t.Fatalf("unknown pass blanked the acknowledged plan (-want +got):\n%s", diff)
	}
	if got.ObservationKnown {
		t.Fatalf("absent member with unreadable inventory reported a current observation: %+v", got)
	}
	if condition := unknown.Status.GetCondition(v1beta1.PlacementSatisfied); condition == nil || condition.Status == corev1.ConditionTrue {
		t.Fatalf("retained evidence satisfied the plan: %+v", condition)
	}
}

// ratioAllSource gives the fixture source a decoder and a router whose
// minimums differ from the engine's.
func ratioAllSource(t *testing.T, f *backendFixture) {
	t.Helper()
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
		t.Fatal(err)
	}
	runner := &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}}
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(60), 60
	f.source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(40), MaxReplicas: 40}, Runner: runner.DeepCopy()}
	f.source.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(8), MaxReplicas: 8}, Runner: runner.DeepCopy()}
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
}

func TestAllPlanPlacesRatioFloorsOnEveryMember(t *testing.T) {
	f := newBackendFixture(t, v1beta1.PlacementModeAll)
	ratioAllSource(t, f)
	live := f.reconcile(t)
	want := []v1beta1.PlacementComponentFloor{{Component: v1beta1.DecoderComponent, Replicas: 40}, {Component: v1beta1.EngineComponent, Replicas: 60}, {Component: v1beta1.RouterComponent, Replicas: 8}}
	if live.Status.Placement == nil || live.Status.Placement.Plan == nil {
		t.Fatalf("no accepted plan: %+v", live.Status)
	}
	// Every full-policy member requests its own 60 primary units.
	if diff := cmp.Diff(int64(60*len(live.Status.Placement.Candidates)), live.Status.Placement.Plan.RequestedReplicas); diff != "" {
		t.Errorf("requested primary units (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(2, len(live.Status.Placement.Candidates)); diff != "" {
		t.Errorf("planned members (-want +got):\n%s", diff)
	}
	if condition := live.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Reason == "AwaitingHomeInputs" {
		t.Errorf("ratio floors held as unresolved: %+v", condition)
	}
	for _, candidate := range live.Status.Placement.Candidates {
		a := candidate.Allocation
		if a == nil || a.HomeInputsPending || a.DesiredHome == nil {
			t.Fatalf("%s member is unresolved: %+v", candidate.Cluster, a)
		}
		if diff := cmp.Diff(want, a.DesiredHome.ReplicaFloors); diff != "" {
			t.Errorf("%s desired floors (-want +got):\n%s", candidate.Cluster, diff)
		}
		if diff := cmp.Diff(int32(60), a.DesiredReplicas); diff != "" {
			t.Errorf("%s desired primary units (-want +got):\n%s", candidate.Cluster, diff)
		}
		member, ok := allMemberOn(t, f, candidate.Cluster)
		if !ok {
			t.Fatalf("%s has no member", candidate.Cluster)
		}
		policy, err := protocol.FromDerived(member)
		if err != nil || policy == nil {
			t.Fatalf("%s full-policy authority: %v", candidate.Cluster, err)
		}
		if diff := cmp.Diff(want, policy.ReplicaFloors); diff != "" {
			t.Errorf("%s member floors (-want +got):\n%s", candidate.Cluster, diff)
		}
		if diff := cmp.Diff(f.source.Spec.Decoder.MinReplicas, member.Spec.Decoder.MinReplicas); diff != "" {
			t.Errorf("%s decoder policy changed (-want +got):\n%s", candidate.Cluster, diff)
		}
	}
}

func TestAllPlanReportsUnresolvedMemberPolicy(t *testing.T) {
	f := newBackendFixture(t, v1beta1.PlacementModeAll)
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
		t.Fatal(err)
	}
	f.source.Spec.Engine.MinReplicas = nil
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	unresolved := backendTestRuntime()
	if err := f.workers["member-b"].Get(t.Context(), client.ObjectKeyFromObject(unresolved), unresolved); err != nil {
		t.Fatal(err)
	}
	unresolved.Spec.EngineConfig = nil
	if err := f.workers["member-b"].Update(t.Context(), unresolved); err != nil {
		t.Fatal(err)
	}
	live := f.reconcile(t)
	condition := live.Status.GetCondition(v1beta1.PlacementConverged)
	if condition == nil || condition.Reason != "AwaitingHomeInputs" {
		t.Fatalf("unresolved member did not hold inputs: %+v", condition)
	}
	if !strings.Contains(condition.Message, "home policy is unresolved on member-b: ") {
		t.Fatalf("condition message does not name the unresolved member: %q", condition.Message)
	}
	for _, candidate := range live.Status.Placement.Candidates {
		if candidate.Cluster == "member-b" && !candidate.Allocation.HomeInputsPending {
			t.Fatalf("member-b inputs were not held: %+v", candidate.Allocation)
		}
	}
}

// TestAllLowerMinimumOnRetainedMembersIsLocalScaling authors lower minimums
// on an All-mode service whose members all stay: no cluster is added,
// released or drained, so no capacity moves between clusters. Each member
// takes the new minimums at once and the plan does not pause member growth;
// the members' autoscalers stay authoritative above them. A raised minimum
// on any component grows each member, so it keeps the surge pause, also when
// another component's minimum falls.
func TestAllLowerMinimumOnRetainedMembersIsLocalScaling(t *testing.T) {
	for _, tt := range []struct {
		name            string
		pd              bool
		engine, decoder int
		wantPause       bool
	}{
		{name: "lower engine minimum", engine: 1},
		{name: "lower prefill and decode minimums", pd: true, engine: 1, decoder: 1},
		{name: "raised engine minimum keeps the pause", engine: 5, wantPause: true},
		{name: "raised decode minimum keeps the pause beside a lower prefill minimum", pd: true, engine: 1, decoder: 5, wantPause: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, tt.pd)
			f.source.Spec.Placement.Mode = v1beta1.PlacementModeAll
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			f.reconcile(t)
			for name := range f.workers {
				seedSingleAdmission(t, f, name, declaredComponents(f.source)...)
				acknowledgeMemberAt(t, f, name, 3, false)
			}
			stored := f.reconcile(t)
			for pass := 0; stored.Status.Placement.Plan.PauseSurge && pass < 5; pass++ {
				for name := range f.workers {
					acknowledgeMemberAt(t, f, name, 3, true)
				}
				stored = f.reconcile(t)
			}
			if stored.Status.Placement.Plan.PauseSurge {
				t.Fatalf("the initial application never released its pause: %+v (%s)", stored.Status.Placement.Plan, stored.Status.GetCondition(v1beta1.PlacementConverged).Reason)
			}
			before := map[string]int32{}
			for _, candidate := range stored.Status.Placement.Candidates {
				before[candidate.Cluster] = candidate.Allocation.CurrentReplicas
			}

			if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
				t.Fatal(err)
			}
			f.source.Spec.Engine.MinReplicas = ptr.To(tt.engine)
			if f.source.Spec.Decoder != nil {
				f.source.Spec.Decoder.MinReplicas = ptr.To(tt.decoder)
			}
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			got := f.reconcile(t)
			if tt.wantPause {
				if !got.Status.Placement.Plan.PauseSurge {
					t.Fatalf("a raised minimum must keep the surge pause: %+v", got.Status.Placement.Plan)
				}
				return
			}
			if got.Status.Placement.Plan.PauseSurge {
				t.Fatalf("a lower minimum on retained members paused member growth: %+v", got.Status.Placement.Plan)
			}
			for _, candidate := range got.Status.Placement.Candidates {
				a := candidate.Allocation
				if a.CurrentHome == nil || a.DesiredHome == nil || a.DrainRequested {
					t.Fatalf("member %s lost its retained policy: %+v", candidate.Cluster, a)
				}
				if a.CurrentReplicas != a.DesiredReplicas || a.OriginalReplicas != a.DesiredReplicas || a.CurrentReplicas == before[candidate.Cluster] {
					t.Fatalf("member %s did not take the new minimum at once: original %d, current %d, desired %d (before %d)", candidate.Cluster, a.OriginalReplicas, a.CurrentReplicas, a.DesiredReplicas, before[candidate.Cluster])
				}
			}
		})
	}
}

// acknowledgeMemberAt acknowledges the member's current policy at the given
// count the way its replicas do. With limit set, each paused component also
// persists a placement replica limit at its count, as a member does while
// placement pauses growth.
func acknowledgeMemberAt(t *testing.T, f *backendFixture, name string, replicas int32, limit bool) {
	t.Helper()
	worker := f.workers[name]
	member := &v1beta1.InferenceService{}
	if err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.source), member); err != nil {
		t.Fatal(err)
	}
	p, err := protocol.FromDerived(member)
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range declaredComponents(f.source) {
		ir := &v1beta1.InferenceReplica{}
		if err := worker.Get(t.Context(), client.ObjectKey{Namespace: member.Namespace, Name: member.Name + "-" + string(component)}, ir); err != nil {
			t.Fatal(err)
		}
		ir.Generation++
		ir.Spec.Replicas, ir.Spec.PlacementExecution = ptr.To(replicas), p.DeepCopy()
		if limit && p != nil && p.PauseSurge {
			ir.Spec.PlacementReplicaLimit = ptr.To(replicas)
		}
		ir.Annotations = map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: strconv.FormatInt(member.Generation, 10)}
		ir.Status.ObservedGeneration, ir.Status.PlacementObservedGeneration = ir.Generation, ir.Generation
		if err := worker.Update(t.Context(), ir); err != nil {
			t.Fatal(err)
		}
	}
}
