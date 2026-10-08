package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func singleFixture(t *testing.T, pd bool) *backendFixture {
	t.Helper()
	f := newBackendFixture(t, v1beta1.PlacementModeSingle)
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
		t.Fatal(err)
	}
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(3), 9
	if pd {
		f.source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(3), MaxReplicas: 9}, Runner: f.source.Spec.Engine.Runner.DeepCopy()}
	}
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	return f
}

func seedSingleAdmission(t *testing.T, f *backendFixture, name string, components ...v1beta1.ComponentType) {
	t.Helper()
	member := &v1beta1.InferenceService{}
	if err := f.workers[name].Get(t.Context(), client.ObjectKeyFromObject(f.source), member); err != nil {
		t.Fatal(err)
	}
	for _, component := range components {
		ir := irWithInstances(component, true, false, false)
		ir.Name, ir.Namespace = member.Name+"-"+string(component), member.Namespace
		objects := observedWorkerObjects(member.DeepCopy(), ir)
		for _, obj := range objects[2:] {
			if err := f.workers[name].Create(t.Context(), obj); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSingleRacePersistsBeforeMemberWrites(t *testing.T) {
	for _, tt := range []struct {
		name   string
		pd     bool
		floor  *int
		a, b   []v1beta1.ComponentType
		winner string
	}{
		{name: "one admitted instance wins below the floor", a: []v1beta1.ComponentType{v1beta1.EngineComponent}, winner: "member-a"},
		{name: "lexical tie", a: []v1beta1.ComponentType{v1beta1.EngineComponent}, b: []v1beta1.ComponentType{v1beta1.EngineComponent}, winner: "member-a"},
		{name: "every declared component is required", pd: true, a: []v1beta1.ComponentType{v1beta1.EngineComponent}, b: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, winner: "member-b"},
		{name: "partial component admission keeps race open", pd: true, a: []v1beta1.ComponentType{v1beta1.EngineComponent}},
		{name: "zero nominees await external admission", floor: ptr.To(0)},
		{name: "zero floor winner retains its home", floor: ptr.To(0), a: []v1beta1.ComponentType{v1beta1.EngineComponent}, winner: "member-a"},
		{name: "zero floor disaggregated race", floor: ptr.To(0), pd: true, a: []v1beta1.ComponentType{v1beta1.EngineComponent}, b: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, winner: "member-b"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, tt.pd)
			floor := 3
			if tt.floor != nil {
				floor = *tt.floor
				f.source.Spec.Engine.MinReplicas = ptr.To(floor)
				if f.source.Spec.Decoder != nil {
					f.source.Spec.Decoder.MinReplicas = ptr.To(floor)
				}
				if err := f.reconciler.Update(t.Context(), f.source); err != nil {
					t.Fatal(err)
				}
			}
			created, deleted := []string{}, []string{}
			for name, worker := range f.workers {
				watched := interceptor.NewClient(worker, interceptor.Funcs{
					Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						if member, ok := obj.(*v1beta1.InferenceService); ok {
							live := &v1beta1.InferenceService{}
							if err := f.reconciler.Get(ctx, client.ObjectKeyFromObject(f.source), live); err != nil {
								return err
							}
							p, err := protocol.FromDerived(member)
							if err != nil || p == nil || live.Status.Placement == nil || live.Status.Placement.Plan == nil {
								t.Fatalf("creation lacks persisted authority: %v", err)
							}
							if diff := cmp.Diff(live.Status.Placement.Plan.ID, p.PlanID); diff != "" {
								t.Fatal(diff)
							}
							if diff := cmp.Diff("", live.Status.Placement.Plan.Winner); diff != "" {
								t.Fatal(diff)
							}
							created = append(created, name)
						}
						return cl.Create(ctx, obj, opts...)
					},
					Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
						live := &v1beta1.InferenceService{}
						if err := f.reconciler.Get(ctx, client.ObjectKeyFromObject(f.source), live); err != nil {
							return err
						}
						if live.Status.Placement.Plan.Winner == "" {
							t.Fatal("cleanup preceded committed winner")
						}
						if diff := cmp.Diff(tt.winner, live.Status.Placement.Plan.Winner); diff != "" {
							t.Fatal(diff)
						}
						options := (&client.DeleteOptions{}).ApplyOptions(opts)
						if options.Preconditions == nil || options.Preconditions.UID == nil || options.Preconditions.ResourceVersion == nil {
							t.Fatal("cleanup lacks conditional identity")
						}
						deleted = append(deleted, name)
						return cl.Delete(ctx, obj, opts...)
					},
				})
				f.connections.m[name] = workloadcluster.NewNeverCachingClient(watched)
			}
			race := f.reconcile(t)
			if diff := cmp.Diff([]string{"member-a", "member-b"}, created); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff("AwaitingSingleWinner", race.Status.GetCondition(v1beta1.PlacementSatisfied).Reason); diff != "" {
				t.Fatal(diff)
			}
			seedSingleAdmission(t, f, "member-a", tt.a...)
			seedSingleAdmission(t, f, "member-b", tt.b...)
			got := f.reconcile(t)
			if diff := cmp.Diff(tt.winner, got.Status.Placement.Plan.Winner); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.winner, got.Status.Placement.Cluster); diff != "" {
				t.Fatal(diff)
			}
			wantDeleted := []string{}
			if tt.winner != "" {
				loser := "member-b"
				if tt.winner == loser {
					loser = "member-a"
				}
				wantDeleted = append(wantDeleted, loser)
				if diff := cmp.Diff(int64(floor), got.Status.Placement.Plan.AssignedReplicas); diff != "" {
					t.Fatal(diff)
				}
				if got.Status.Placement.Plan.ID == race.Status.Placement.Plan.ID {
					t.Fatal("winner reused race authority")
				}
			}
			if diff := cmp.Diff(wantDeleted, deleted); diff != "" {
				t.Fatal(diff)
			}
			for _, c := range got.Status.Placement.Candidates {
				if c.Cluster == tt.winner {
					if diff := cmp.Diff(int32(1), c.AdmittedReplicas); diff != "" {
						t.Fatal(diff)
					}
				}
			}
		})
	}
}

func TestSingleWinnerFailureCannotDeleteLosers(t *testing.T) {
	for _, tt := range []struct {
		name  string
		prune bool
	}{
		{name: "status update fails"}, {name: "winner field pruned", prune: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", v1beta1.EngineComponent)
			base := f.reconciler.Client
			f.reconciler.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				s := obj.(*v1beta1.InferenceService)
				if s.Status.Placement != nil && s.Status.Placement.Plan != nil && s.Status.Placement.Plan.Winner != "" {
					if !tt.prune {
						return errors.New("status unavailable")
					}
					s = s.DeepCopy()
					s.Status.Placement.Plan.Winner = ""
				}
				return cl.SubResource(sub).Update(ctx, s, opts...)
			}})
			_, err := f.reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.source)})
			if err != nil && !errors.Is(err, plan.ErrStaleSnapshot) {
				t.Fatal(err)
			}
			for _, worker := range f.workers {
				if err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); err != nil {
					t.Fatalf("uncommitted winner removed a member: %v", err)
				}
			}
			f.reconciler.Client = base
			got := f.reconcile(t)
			if diff := cmp.Diff("member-a", got.Status.Placement.Plan.Winner); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestSingleRestartRetainsNominations(t *testing.T) {
	f := singleFixture(t, false)
	f.reconcile(t)
	seedSingleAdmission(t, f, "member-b", v1beta1.EngineComponent)
	f.reconciler.dispatcher = newIncrementalDispatcher(1, time.Hour)
	got := f.reconcile(t)
	if diff := cmp.Diff("member-b", got.Status.Placement.Plan.Winner); diff != "" {
		t.Fatal(diff)
	}
}

func TestSingleNominationsMustPersistBeforeCreation(t *testing.T) {
	for _, tt := range []struct {
		name  string
		prune bool
	}{
		{name: "status unavailable"}, {name: "plan pruned", prune: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			base := f.reconciler.Client
			f.reconciler.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				s := obj.(*v1beta1.InferenceService).DeepCopy()
				if s.Status.Placement != nil && s.Status.Placement.Plan != nil {
					if !tt.prune {
						return errors.New("status unavailable")
					}
					s.Status.Placement.Plan = nil
				}
				return cl.SubResource(sub).Update(ctx, s, opts...)
			}})
			f.reconcile(t)
			for _, worker := range f.workers {
				if err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); !apierrors.IsNotFound(err) {
					t.Fatalf("unpersisted nominations created a member: %v", err)
				}
			}
			f.reconciler.Client = base
			got := f.reconcile(t)
			if got.Status.Placement.Plan == nil {
				t.Fatal("recovered API did not accept race")
			}
		})
	}
}

func TestSingleProposalRejectsUnverifiedAuthority(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*backendFixture, *v1beta1.InferenceService)
	}{
		{name: "unidentified registration", edit: func(f *backendFixture, _ *v1beta1.InferenceService) { f.clusters[0].UID = "" }},
		{name: "duplicate registration", edit: func(f *backendFixture, _ *v1beta1.InferenceService) { f.clusters = append(f.clusters, f.clusters[0]) }},
		{name: "replaced registration", edit: func(f *backendFixture, _ *v1beta1.InferenceService) { f.clusters[0].UID = "replacement" }},
		{name: "foreign source plan", edit: func(_ *backendFixture, s *v1beta1.InferenceService) {
			s.Status.Placement.Plan.SourceUID = "other-source"
		}},
		{name: "paused authority cannot start race", edit: func(_ *backendFixture, s *v1beta1.InferenceService) { s.Status.Placement.Plan.PauseSurge = true }},
		{name: "other mode cannot start race", edit: func(_ *backendFixture, s *v1beta1.InferenceService) {
			s.Status.Placement.Plan.Mode = v1beta1.PlacementModeAll
		}},
		{name: "incomplete accepted assignments", edit: func(_ *backendFixture, s *v1beta1.InferenceService) {
			s.Status.Placement.Candidates[0].Allocation = nil
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			s := f.reconcile(t)
			tt.edit(f, s)
			before := s.DeepCopy()
			_, err := f.reconciler.singleProposal(t.Context(), s, f.clusters, "", []string{"member-a", "member-b"})
			if err == nil {
				t.Fatal("unverified authority accepted")
			}
			if diff := cmp.Diff(before, s); diff != "" {
				t.Fatalf("proposal mutated accepted state: %s", diff)
			}
		})
	}
}

func TestSingleRetainedWinnerHolds(t *testing.T) {
	for _, tt := range []struct {
		name   string
		edit   func(*testing.T, *backendFixture, *v1beta1.InferenceService)
		reason string
	}{
		{name: "stale top level winner cannot reopen race", edit: func(t *testing.T, f *backendFixture, s *v1beta1.InferenceService) {
			s.Status.Placement.Cluster = "member-b"
			if err := f.reconciler.Status().Update(t.Context(), s); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "affinity change keeps the winner until routing is known", edit: func(t *testing.T, f *backendFixture, s *v1beta1.InferenceService) {
			s.Spec.Placement.ClusterAffinity = testAffinity("metadata.name=member-b")
			s.Generation++
			if err := f.reconciler.Update(t.Context(), s); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "no matching peers retains healthy winner", edit: func(t *testing.T, f *backendFixture, s *v1beta1.InferenceService) {
			s.Spec.Placement.ClusterAffinity = testAffinity("metadata.name=member-c")
			s.Generation++
			if err := f.reconciler.Update(t.Context(), s); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unreachable winner is not failed", edit: func(_ *testing.T, f *backendFixture, _ *v1beta1.InferenceService) {
			delete(f.connections.m, "member-a")
		}},
		{name: "replacement registration cannot assume winner identity", edit: func(t *testing.T, f *backendFixture, _ *v1beta1.InferenceService) {
			c := readyWC("member-a", nil)
			if err := f.reconciler.Delete(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			c.UID = "replacement"
			if err := f.reconciler.Create(t.Context(), c); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", v1beta1.EngineComponent)
			f.reconcile(t)
			accepted := f.reconcile(t)
			tt.edit(t, f, accepted)
			got := f.reconcile(t)
			if diff := cmp.Diff("member-a", got.Status.Placement.Plan.Winner); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff("member-a", got.Status.Placement.Cluster); diff != "" {
				t.Fatal(diff)
			}
			if tt.reason != "" {
				condition := got.Status.GetCondition(v1beta1.PlacementConverged)
				if condition == nil || condition.Status != corev1.ConditionFalse {
					t.Fatalf("missing hold: %v", condition)
				}
				if diff := cmp.Diff(tt.reason, condition.Reason); diff != "" {
					t.Fatal(diff)
				}
			}
			if err := f.workers["member-b"].Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); !apierrors.IsNotFound(err) {
				t.Fatalf("retained winner started new race: %v", err)
			}
		})
	}
}

func TestSingleModeChangeReportsOnlyWinnerHealth(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state homeObservationState
		want  corev1.ConditionStatus
	}{
		{name: "unready winner cannot borrow loser readiness", state: homePresent, want: corev1.ConditionFalse},
		{name: "unknown winner cannot borrow loser readiness", state: homeUnknown, want: corev1.ConditionUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", v1beta1.EngineComponent)
			s := f.reconcile(t)
			s.Spec.Placement.Mode = v1beta1.PlacementModeAll
			s.Generation++
			if err := f.reconciler.Update(t.Context(), s); err != nil {
				t.Fatal(err)
			}
			observations := newPlacementObservations(s, f.clusters)
			observations.homes["member-a"] = homeObservation{state: tt.state, candidate: v1beta1.CandidatePlacement{Cluster: "member-a", Phase: v1beta1.CandidatePhaseAdmitted}}
			observations.homes["member-b"] = homeObservation{state: homePresent, serving: true, candidate: v1beta1.CandidatePlacement{Cluster: "member-b", Phase: v1beta1.CandidatePhaseAdmitted, AdmittedReplicas: 3, ReadyReplicas: 3, Endpoint: apis.HTTPS("member-b.example.com")}}
			if _, err := f.reconciler.writeSplitHold(t.Context(), s, observations, "PlacementModeChangeBlocked", errors.New("accepted mode held")); err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.InferenceService{}
			if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(s), got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff("member-a", got.Status.Placement.Cluster); diff != "" {
				t.Fatal(diff)
			}
			if got.Status.URL != nil {
				t.Fatal("loser endpoint published")
			}
			if diff := cmp.Diff(tt.want, got.Status.GetCondition(apis.ConditionReady).Status); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// TestSingleUnreadableWinnerKeepsAppliedPlan converges a Single winner, then
// makes one pass unable to read it: once through a lost connection, once with
// only its component inventory unreadable. The pass keeps the plan the winner
// last acknowledged and holds convergence instead of deciding on the kept
// value; the next readable pass reconverges.
func TestSingleUnreadableWinnerKeepsAppliedPlan(t *testing.T) {
	for _, tt := range []struct {
		name   string
		unread func(*backendFixture)
		known  bool
	}{
		{name: "lost connection", unread: func(f *backendFixture) { delete(f.connections.m, "member-a") }},
		{name: "unreadable component inventory", known: true, unread: func(f *backendFixture) { f.connections.m["member-a"] = unreadableAllMember(f, "member-a") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", v1beta1.EngineComponent)
			f.reconcile(t)
			f.reconcile(t)
			acknowledgeSingleFloor(t, f, "member-a", 3)
			settled := f.reconcile(t)
			if condition := settled.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Status != corev1.ConditionTrue {
				t.Fatalf("winner did not converge: %+v", condition)
			}
			before := candidateOf(t, settled, "member-a")
			if !before.ObservationKnown || before.AppliedPlanID != settled.Status.Placement.Plan.ID {
				t.Fatalf("winner has not acknowledged the accepted plan: %+v", before)
			}

			readable := f.connections.m["member-a"]
			tt.unread(f)
			unknown := f.reconcile(t)
			f.connections.m["member-a"] = readable
			if diff := cmp.Diff(settled.Status.Placement.Plan, unknown.Status.Placement.Plan); diff != "" {
				t.Fatalf("unreadable pass changed the accepted plan (-want +got):\n%s", diff)
			}
			got := candidateOf(t, unknown, "member-a")
			if diff := cmp.Diff(before.AppliedPlanID, got.AppliedPlanID); diff != "" {
				t.Fatalf("unreadable pass blanked the winner's acknowledged plan (-want +got):\n%s", diff)
			}
			// The observation flag follows the standing read; the kept
			// acknowledgement never satisfies the winner-applied decision.
			if diff := cmp.Diff(tt.known, got.ObservationKnown); diff != "" {
				t.Fatalf("observation flag (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff("AwaitingMemberConvergence", unknown.Status.GetCondition(v1beta1.PlacementConverged).Reason); diff != "" {
				t.Fatalf("unreadable pass decided on the kept acknowledgement (-want +got):\n%s", diff)
			}
			if err := f.workers["member-b"].Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unreadable pass reopened the race: %v", err)
			}

			recovered := f.reconcile(t)
			if condition := recovered.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Status != corev1.ConditionTrue {
				t.Fatalf("readable pass did not reconverge: %+v", condition)
			}
			if diff := cmp.Diff(before, candidateOf(t, recovered, "member-a")); diff != "" {
				t.Fatalf("readable pass did not restore the candidate (-want +got):\n%s", diff)
			}
		})
	}
}
