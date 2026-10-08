package placement

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func zeroFloorObservationFixture(t *testing.T) plannedObservationFixture {
	t.Helper()
	f := observationFixture(t)
	f.source.Spec.Placement.Mode, f.source.Spec.Placement.Split = v1beta1.PlacementModeSingle, nil
	f.source.Spec.Engine.MinReplicas = ptr.To(0)
	f.source.Status.Placement.Plan.Mode, f.source.Status.Placement.Plan.Winner = v1beta1.PlacementModeSingle, "member-a"
	f.source.Status.Placement.Plan.PauseSurge = false
	a := f.source.Status.Placement.Candidates[0].Allocation
	a.CurrentReplicas, a.DesiredReplicas = 0, 0
	a.CurrentHome = &v1beta1.PlacementHomePolicy{InputDigest: "source-intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}}}
	a.DesiredHome = a.CurrentHome.DeepCopy()
	p := executionPolicy(f.source, a)
	raw, err := protocol.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	f.member.Annotations[constants.PlacementExecution] = raw
	f.resources.ir.Spec.PlacementExecution = p.DeepCopy()
	f.resources.ir.Spec.PlacementReplicaLimit = nil
	f.resources.ir.Spec.Replicas = ptr.To[int32](0)
	f.resources.ir.Status.InstanceStatuses, f.resources.ir.Status.ReadyReplicas = nil, 0
	f.resources.pods = nil
	return f
}

func TestZeroFloorHomeObservation(t *testing.T) {
	type result struct {
		Idle, ScalingDown, Applied bool
		Occupied, Ready, Admitted  int32
	}
	for _, tt := range []struct {
		name string
		edit func(*plannedObservationFixture)
		want result
	}{
		{name: "idle home is applied without admission", want: result{Idle: true, Applied: true}},
		{name: "pending autoscaler demand needs admission", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.Replicas = ptr.To[int32](2) }, want: result{Applied: true, Occupied: 2}},
		{name: "physical cleanup retains the winner", edit: func(f *plannedObservationFixture) {
			f.resources.pods = observationFixture(t).resources.pods
		}, want: result{ScalingDown: true, Applied: true, Occupied: 1}},
		{name: "unmaterialized rollout reservation prevents idle credit", edit: func(f *plannedObservationFixture) {
			f.resources.ir.Status.InstanceStatuses = resourceFixture().ir.Status.InstanceStatuses
			f.resources.addSurge()
		}, want: result{ScalingDown: true, Applied: true}},
		{name: "missing component cannot acknowledge zero", edit: func(f *plannedObservationFixture) { f.resources.ir = nil }},
		{name: "missing decoder cannot acknowledge zero", edit: func(f *plannedObservationFixture) { f.components = append(f.components, v1beta1.DecoderComponent) }},
		{name: "unacknowledged execution policy cannot establish idle home", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.PlacementExecution = nil }},
		{name: "stale guard acknowledgement cannot establish idle home", edit: func(f *plannedObservationFixture) { f.resources.ir.Status.PlacementObservedGeneration-- }},
		{name: "stale parent projection cannot establish idle home", edit: func(f *plannedObservationFixture) {
			f.resources.ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "2"
		}},
		{name: "terminating service cannot establish idle home", edit: func(f *plannedObservationFixture) {
			f.member.Finalizers = []string{"example.com/cleanup"}
			f.member.DeletionTimestamp = ptr.To(metav1.Now())
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := zeroFloorObservationFixture(t)
			if tt.edit != nil {
				tt.edit(&f)
			}
			objects := []client.Object{f.member}
			if f.resources.ir != nil {
				objects = append(objects, f.resources.ir)
			}
			for i := range f.resources.pods {
				objects = append(objects, &f.resources.pods[i])
			}
			scheme := testScheme(t)
			worker := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, _ := newPlacer(scheme, clusters, f.source, plannedTestRegistration())
			observed, err := r.observePlannedHome(t.Context(), f.source, f.source.Status.Placement.Candidates[0], f.components)
			if err != nil {
				t.Fatal(err)
			}
			got := result{observed.IdleZeroFloor, observed.ScalingToZero, observed.Home.Applied, observed.Home.Occupied, observed.Candidate.ReadyReplicas, observed.Candidate.AdmittedReplicas}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func acknowledgeSingleFloor(t *testing.T, f *backendFixture, name string, replicas int32) {
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
		ir.Annotations = map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: strconv.FormatInt(member.Generation, 10)}
		ir.Status.ObservedGeneration, ir.Status.PlacementObservedGeneration = ir.Generation, ir.Generation
		if replicas == 0 {
			ir.Status.InstanceStatuses, ir.Status.ReadyReplicas = nil, 0
		}
		if err := worker.Update(t.Context(), ir); err != nil {
			t.Fatal(err)
		}
	}
	if replicas == 0 {
		pods := &corev1.PodList{}
		if err := worker.List(t.Context(), pods, client.InNamespace(member.Namespace)); err != nil {
			t.Fatal(err)
		}
		for i := range pods.Items {
			if err := worker.Delete(t.Context(), &pods.Items[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// unreadablePodInventory returns a connection to the named home whose Pod
// inventory cannot be listed while its component inventory stays readable, so
// only the planned observation of that home fails.
func unreadablePodInventory(f *backendFixture, name string) workloadcluster.SelectivelyCachingClient {
	unreadable := interceptor.NewClient(f.workers[name], interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*corev1.PodList); ok {
			return fmt.Errorf("inventory unavailable")
		}
		return cl.List(ctx, list, opts...)
	}})
	return workloadcluster.NewNeverCachingClient(unreadable)
}

func TestSingleZeroFloorWinnerRetention(t *testing.T) {
	for _, tt := range []struct {
		name       string
		pd         bool
		restart    bool
		verified   bool
		edit       func(*testing.T, *backendFixture)
		wantReason string
		wantPhase  v1beta1.PlacementPhase
	}{
		{name: "idle engine retains the committed winner"},
		{name: "idle disaggregated service retains the committed winner", pd: true},
		{name: "restart retains an idle winner", restart: true},
		{name: "unknown inventory after an idle proof keeps the winner Placed", verified: true, wantReason: "AwaitingMemberConvergence", edit: func(_ *testing.T, f *backendFixture) {
			f.connections.m["member-a"] = unreadablePodInventory(f, "member-a")
		}},
		{name: "unknown inventory before any idle proof holds the winner", wantReason: "AwaitingMemberConvergence", wantPhase: v1beta1.PlacementPhaseAdmitting, edit: func(_ *testing.T, f *backendFixture) {
			f.connections.m["member-a"] = unreadablePodInventory(f, "member-a")
		}},
		{name: "stale placement acknowledgement holds the winner", wantReason: "AwaitingMemberConvergence", wantPhase: v1beta1.PlacementPhaseAdmitting, edit: func(t *testing.T, f *backendFixture) {
			ir := &v1beta1.InferenceReplica{}
			key := client.ObjectKey{Namespace: f.source.Namespace, Name: f.source.Name + "-engine"}
			if err := f.workers["member-a"].Get(t.Context(), key, ir); err != nil {
				t.Fatal(err)
			}
			ir.Status.PlacementObservedGeneration--
			if err := f.workers["member-a"].Update(t.Context(), ir); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, tt.pd)
			f.source.Spec.Engine.MinReplicas = ptr.To(0)
			if tt.pd {
				f.source.Spec.Decoder.MinReplicas = ptr.To(0)
			}
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", declaredComponents(f.source)...)
			winner := f.reconcile(t)
			if diff := cmp.Diff("member-a", winner.Status.Placement.Plan.Winner); diff != "" {
				t.Fatal(diff)
			}
			f.reconcile(t)
			acknowledgeSingleFloor(t, f, "member-a", 0)
			if tt.verified {
				if diff := cmp.Diff(v1beta1.PlacementPhasePlaced, f.reconcile(t).Status.Placement.Phase); diff != "" {
					t.Fatalf("idle winner was not proven before the edit (-want +got):\n%s", diff)
				}
			}
			if tt.restart {
				stored := f.reconcile(t)
				f.reconciler, _ = newPlacer(testScheme(t), f.connections, stored, &f.clusters[0], &f.clusters[1])
			}
			if tt.edit != nil {
				tt.edit(t, f)
			}
			f.reconciler.WinnerLostGracePeriod = time.Nanosecond
			for range 3 {
				got := f.reconcile(t)
				if diff := cmp.Diff("member-a", got.Status.Placement.Plan.Winner); diff != "" {
					t.Fatal(diff)
				}
				if tt.wantReason != "" {
					if diff := cmp.Diff(tt.wantReason, got.Status.GetCondition(v1beta1.PlacementConverged).Reason); diff != "" {
						t.Fatal(diff)
					}
				}
				wantPhase := tt.wantPhase
				if wantPhase == "" {
					wantPhase = v1beta1.PlacementPhasePlaced
				}
				if diff := cmp.Diff(wantPhase, got.Status.Placement.Phase); diff != "" {
					t.Fatalf("retained winner phase (-want +got):\n%s", diff)
				}
				loser := &v1beta1.InferenceServiceList{}
				if err := f.workers["member-b"].List(t.Context(), loser); err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(0, len(loser.Items)); diff != "" {
					t.Fatalf("idle winner reopened the race: %s", diff)
				}
			}
		})
	}
}

// TestSingleZeroFloorUnreadableWinnerKeepsPlaced converges an idle Single
// winner, then makes one pass unable to verify its inventory while its
// standing read still succeeds. The pass keeps the Placed phase, the endpoint,
// the readiness verdict and the acknowledged plan, holds convergence on the
// reason, and the next readable pass reconverges. A lost connection is the
// control: its standing read is unknown and keeps the phase on its own.
func TestSingleZeroFloorUnreadableWinnerKeepsPlaced(t *testing.T) {
	for _, tt := range []struct {
		name   string
		unread func(*backendFixture)
		known  bool
	}{
		{name: "unreadable pod inventory", known: true, unread: func(f *backendFixture) { f.connections.m["member-a"] = unreadablePodInventory(f, "member-a") }},
		{name: "unreadable component inventory", known: true, unread: func(f *backendFixture) { f.connections.m["member-a"] = unreadableAllMember(f, "member-a") }},
		{name: "lost connection", unread: func(f *backendFixture) { delete(f.connections.m, "member-a") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			f.source.Spec.Engine.MinReplicas = ptr.To(0)
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", v1beta1.EngineComponent)
			f.reconcile(t)
			f.reconcile(t)
			acknowledgeSingleFloor(t, f, "member-a", 0)
			settled := f.reconcile(t)
			if diff := cmp.Diff(v1beta1.PlacementPhasePlaced, settled.Status.Placement.Phase); diff != "" {
				t.Fatalf("idle winner did not settle as Placed (-want +got):\n%s", diff)
			}
			before := candidateOf(t, settled, "member-a")
			if !before.ObservationKnown || before.AppliedPlanID != settled.Status.Placement.Plan.ID {
				t.Fatalf("idle winner has not acknowledged the accepted plan: %+v", before)
			}
			readyBefore := settled.Status.GetCondition(apis.ConditionReady)

			readable := f.connections.m["member-a"]
			tt.unread(f)
			unknown := f.reconcile(t)
			f.connections.m["member-a"] = readable

			if diff := cmp.Diff(settled.Status.Placement.Plan, unknown.Status.Placement.Plan); diff != "" {
				t.Fatalf("unreadable pass changed the accepted plan (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(v1beta1.PlacementPhasePlaced, unknown.Status.Placement.Phase); diff != "" {
				t.Fatalf("unreadable pass demoted the idle winner (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(settled.Status.URL, unknown.Status.URL); diff != "" {
				t.Fatalf("unreadable pass changed the endpoint (-want +got):\n%s", diff)
			}
			got := candidateOf(t, unknown, "member-a")
			if diff := cmp.Diff(before.AppliedPlanID, got.AppliedPlanID); diff != "" {
				t.Fatalf("unreadable pass blanked the acknowledged plan (-want +got):\n%s", diff)
			}
			// The observation flag follows the standing read; the kept phase never
			// satisfies the winner-applied decision.
			if diff := cmp.Diff(tt.known, got.ObservationKnown); diff != "" {
				t.Fatalf("observation flag (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff("AwaitingMemberConvergence", unknown.Status.GetCondition(v1beta1.PlacementConverged).Reason); diff != "" {
				t.Fatalf("unreadable pass decided on the kept phase (-want +got):\n%s", diff)
			}
			if tt.known {
				ready := unknown.Status.GetCondition(apis.ConditionReady)
				if diff := cmp.Diff([]string{string(readyBefore.Status), readyBefore.Reason}, []string{string(ready.Status), ready.Reason}); diff != "" {
					t.Fatalf("unreadable pass changed the readiness verdict (-want +got):\n%s", diff)
				}
			}
			if err := f.workers["member-b"].Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unreadable pass reopened the race: %v", err)
			}

			recovered := f.reconcile(t)
			if diff := cmp.Diff(v1beta1.PlacementPhasePlaced, recovered.Status.Placement.Phase); diff != "" {
				t.Fatalf("readable pass did not keep the idle winner Placed (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, candidateOf(t, recovered, "member-a")); diff != "" {
				t.Fatalf("readable pass did not restore the candidate (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSingleZeroFloorMovementRequiresPositiveFloor(t *testing.T) {
	for _, pd := range []bool{false, true} {
		t.Run(fmt.Sprintf("disaggregated=%t", pd), func(t *testing.T) {
			f := singleFixture(t, pd)
			f.source.Spec.Engine.MinReplicas = ptr.To(0)
			if pd {
				f.source.Spec.Decoder.MinReplicas = ptr.To(0)
			}
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", declaredComponents(f.source)...)
			f.reconcile(t)
			source := f.reconcile(t)
			acknowledgeSingleFloor(t, f, "member-a", 3)
			accepted := source.Status.Placement.Plan.DeepCopy()
			source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"member-b"}}}}}
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
					t.Fatalf("blocked move changed authority: %s", diff)
				}
				members := &v1beta1.InferenceServiceList{}
				if err := f.workers["member-b"].List(t.Context(), members); err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(0, len(members.Items)); diff != "" {
					t.Fatal(diff)
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
			if diff := cmp.Diff("member-a", source.Status.Placement.Plan.Winner); diff != "" {
				t.Fatal(diff)
			}
			if source.Status.Placement.Plan.SingleMove != nil || source.Status.Placement.Plan.PauseSurge {
				t.Fatal("move began before the retained home received its positive floor")
			}
			member := &v1beta1.InferenceService{}
			if err := f.workers["member-a"].Get(t.Context(), client.ObjectKeyFromObject(source), member); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(ptr.To(2), member.Spec.Engine.MinReplicas); diff != "" {
				t.Fatal(diff)
			}
			acknowledgeSingleFloor(t, f, "member-a", 3)
			source = f.reconcile(t)
			if source.Status.Placement.Plan.SingleMove == nil || !source.Status.Placement.Plan.PauseSurge {
				t.Fatal("positive floor did not permit bounded movement")
			}
			if diff := cmp.Diff(int32(2), source.Status.Placement.Candidates[0].Allocation.OriginalReplicas); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
