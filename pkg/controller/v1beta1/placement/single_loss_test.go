package placement

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestSingleMoveLossRecovery(t *testing.T) {
	for _, tt := range []struct {
		name                                        string
		expired, selected, handedOff                bool
		disconnected, orphan, noEligible, failReset bool
		wantReset, wantTimer                        bool
	}{
		{name: "empty probe race waits for loss grace", wantTimer: true},
		{name: "empty probe race restarts after grace", expired: true, wantReset: true},
		{name: "lost selection restarts after grace", expired: true, selected: true, wantReset: true},
		{name: "lost destination after handoff can follow new affinity", expired: true, selected: true, handedOff: true, wantReset: true},
		{name: "unreachable retained member interrupts recovery grace", expired: true, disconnected: true},
		{name: "remaining resources interrupt recovery grace", expired: true, orphan: true},
		{name: "empty eligible set retains movement authority", expired: true, noEligible: true, wantTimer: true},
		{name: "failed reset cannot authorize member creation", expired: true, failReset: true, wantTimer: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source, proposal, observations := replacementRaceFixture()
			source.Spec.Engine.MinReplicas = ptr.To(3)
			source.Spec.Placement.ClusterAffinity = testAffinity("target=true")
			activeReplacement(&proposal, observations, "member-b", true)
			if tt.selected {
				proposal.SingleMove.Selected = "member-b"
				a := proposal.Assignments["member-b"]
				a.RaceCandidate = false
				proposal.Assignments["member-b"] = a
			}
			target := "member-b"
			if tt.handedOff {
				proposal.Winner = "member-b"
				target = "member-c"
			}
			if tt.noEligible {
				target = ""
			}
			source.Status.Placement = &v1beta1.PlacementStatus{Cluster: proposal.Winner, Plan: &v1beta1.PlacementPlanStatus{
				ID: "accepted-plan", Revision: 1, SourceUID: source.UID, ObservedGeneration: source.Generation,
				Mode: proposal.Mode, Winner: proposal.Winner, PauseSurge: true, InputDigest: proposal.InputDigest, SingleMove: proposal.SingleMove.DeepCopy(),
			}}
			scheme := testScheme(t)
			connections := splitTestClusters{fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}}
			workers := map[string]client.WithWatch{}
			objects := []client.Object{source}
			for _, name := range []string{"member-a", "member-b", "member-c"} {
				a := proposal.Assignments[name]
				if tt.selected && name != "member-b" {
					a.DesiredReplicas, a.DesiredHome = 0, nil
				}
				source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: a.DeepCopy()})
				labels := map[string]string{}
				if name == target {
					labels["target"] = "true"
				}
				objects = append(objects, readyWC(name, labels))
				workers[name] = emptyWorker(scheme)
				if !tt.disconnected || name != "member-c" {
					connections.m[name] = workloadcluster.NewNeverCachingClient(workers[name])
				}
			}
			if tt.orphan {
				fixture := observationFixture(t)
				fixture.resources.ir.Namespace = source.Namespace
				fixture.resources.pods[0].Namespace = source.Namespace
				for _, obj := range []client.Object{fixture.resources.ir, &fixture.resources.pods[0]} {
					if err := workers["member-a"].Create(t.Context(), obj); err != nil {
						t.Fatal(err)
					}
				}
			}
			r, root := newPlacer(scheme, connections, objects...)
			r.WinnerLostGracePeriod = time.Minute
			if tt.expired {
				r.winnerLostSince.Store(source.UID, time.Now().Add(-2*r.WinnerLostGracePeriod))
			}
			if tt.failReset {
				r.Client = interceptor.NewClient(root.(client.WithWatch), interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if s, ok := obj.(*v1beta1.InferenceService); ok && s.Status.Placement.Plan.SingleMove == nil {
						return fmt.Errorf("plan update unavailable")
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				}})
			}
			if _, err := r.Reconcile(t.Context(), req()); err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.InferenceService{}
			if err := root.Get(t.Context(), client.ObjectKeyFromObject(source), got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wantReset, got.Status.Placement.Plan.SingleMove == nil); diff != "" {
				t.Fatalf("movement reset: %s", diff)
			}
			_, timer := r.winnerLostSince.Load(source.UID)
			if diff := cmp.Diff(tt.wantTimer, timer); diff != "" {
				t.Fatalf("loss grace: %s", diff)
			}
			for name, worker := range workers {
				member := &v1beta1.InferenceService{}
				err := worker.Get(t.Context(), client.ObjectKeyFromObject(source), member)
				if diff := cmp.Diff(tt.wantReset && name == target, err == nil); diff != "" {
					t.Fatalf("member %s created: %s", name, diff)
				}
			}
			if tt.wantReset {
				if diff := cmp.Diff("", got.Status.Placement.Plan.Winner); diff != "" {
					t.Fatalf("new race reused serving winner: %s", diff)
				}
				if got.Status.Placement.Plan.PauseSurge {
					t.Fatal("new race retains movement pause")
				}
				for _, candidate := range got.Status.Placement.Candidates {
					if diff := cmp.Diff(int32(0), candidate.Allocation.OriginalReplicas); diff != "" {
						t.Fatalf("new race reused original floor: %s", diff)
					}
				}
			}
		})
	}
}

func TestSingleMoveEmptyInventory(t *testing.T) {
	for _, tt := range []struct {
		name        string
		standing    homeObservationState
		newMember   bool
		orphan      bool
		unavailable bool
		want        bool
	}{
		{name: "all registrations empty", standing: homeAbsent, want: true},
		{name: "unknown winner cannot restart", standing: homeUnknown},
		{name: "present winner cannot restart", standing: homePresent},
		{name: "new registration must also be empty", standing: homeAbsent, newMember: true},
		{name: "new registration orphan cannot be ignored", standing: homeAbsent, orphan: true},
		{name: "unreadable new registration blocks restart", standing: homeAbsent, unavailable: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := observationFixture(t)
			source := fixture.source
			source.Status.Placement.Plan.Mode = v1beta1.PlacementModeSingle
			source.Status.Placement.Plan.Winner = "member-a"
			standing := &placementObservations{homes: map[string]homeObservation{"member-a": {state: tt.standing}}}
			scheme := testScheme(t)
			workers := map[string]client.WithWatch{"member-a": emptyWorker(scheme), "member-b": emptyWorker(scheme)}
			if tt.newMember {
				if err := workers["member-b"].Create(t.Context(), fixture.member); err != nil {
					t.Fatal(err)
				}
			}
			if tt.orphan {
				if err := workers["member-b"].Create(t.Context(), &fixture.resources.pods[0]); err != nil {
					t.Fatal(err)
				}
			}
			if tt.unavailable {
				workers["member-b"] = interceptor.NewClient(workers["member-b"], interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, obj client.ObjectList, opts ...client.ListOption) error {
					if _, ok := obj.(*corev1.PodList); ok {
						return fmt.Errorf("pod inventory unavailable")
					}
					return cl.List(ctx, obj, opts...)
				}})
			}
			connections := splitTestClusters{fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}}
			clusters := []v1beta1.WorkloadCluster{*readyWC("member-a", nil), *readyWC("member-b", nil)}
			for name, worker := range workers {
				connections.m[name] = workloadcluster.NewNeverCachingClient(worker)
			}
			r, _ := newPlacer(scheme, connections, &clusters[0], &clusters[1])
			got := r.singleMoveEmpty(t.Context(), source, clusters, standing)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestSelectedReplacementTerminalFailure(t *testing.T) {
	for _, tt := range []struct {
		name       string
		absent     bool
		wantReason string
	}{
		{name: "serving original is retained", wantReason: "ReplacementFailed"},
		{name: "absent original cannot make failed selection converged", absent: true, wantReason: "ReplacementFailed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source, proposal, observations := replacementRaceFixture()
			activeReplacement(&proposal, observations, "member-b", true)
			b := observations["member-b"]
			b.Terminal = true
			observations["member-b"] = b
			source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{
				Mode: v1beta1.PlacementModeSingle, Winner: "member-a", PauseSurge: true, AssignedReplicas: 3,
				SingleMove: &v1beta1.PlacementSingleMoveStatus{Selected: "member-b"},
			}}
			for _, name := range []string{"member-a", "member-b", "member-c"} {
				a := proposal.Assignments[name]
				if name != "member-b" {
					a.DesiredReplicas, a.DesiredHome = 0, nil
				}
				if name == "member-a" && tt.absent {
					a.CurrentReplicas, a.CurrentHome = 0, nil
					o := observations[name]
					o.Home.Absent, o.Home.Ready, o.Home.Occupied = true, 0, 0
					observations[name] = o
				}
				source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: a.DeepCopy()})
			}
			step, err := advanceSplitPlan(source, observations)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wantReason, step.Reason); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(false, step.Complete); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff([]string(nil), step.Drain); diff != "" {
				t.Fatalf("failed replacement authorized drain: %s", diff)
			}
			condition := splitProgressCondition(step.Reason, "")
			if diff := cmp.Diff(corev1.ConditionFalse, condition.cond.Status); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
