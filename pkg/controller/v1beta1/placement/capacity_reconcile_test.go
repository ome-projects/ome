package placement

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func TestReconcileCapacityInitialPlacement(t *testing.T) {
	for _, tt := range []struct {
		name                   string
		unknown, local, static bool
	}{
		{name: "hardware proportional initial placement"},
		{name: "unknown capacity holds", unknown: true},
		{name: "local service ignores capacity configuration", local: true, unknown: true},
		{name: "static split ignores capacity configuration", static: true, unknown: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCapacityProviderFixture(t)
			if tt.unknown {
				f.r.Capacity = nil
			}
			if tt.local {
				f.source.Spec.Placement = nil
			}
			if tt.static {
				f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
			}
			if err := f.r.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			before := f.source.DeepCopy()
			result, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.source)})
			if err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.InferenceService{}
			if err := f.r.Get(t.Context(), client.ObjectKeyFromObject(f.source), got); err != nil {
				t.Fatal(err)
			}
			if tt.local {
				if diff := cmp.Diff(before, got); diff != "" {
					t.Fatalf("local changed: %s", diff)
				}
				return
			}
			if tt.unknown && !tt.static {
				if got.Status.Placement.Plan != nil {
					t.Fatal("unknown capacity committed a plan")
				}
			} else {
				if got.Status.Placement.Plan == nil {
					t.Fatalf("no plan: %+v", got.Status.Conditions)
				}
				targets := map[string]int32{}
				for _, candidate := range got.Status.Placement.Candidates {
					targets[candidate.Cluster] = candidate.Allocation.DesiredReplicas
				}
				want := map[string]int32{"member-a": 8, "member-b": 4, "member-c": 0}
				if tt.static {
					want = map[string]int32{"member-a": 4, "member-b": 4, "member-c": 4}
				}
				if diff := cmp.Diff(want, targets); diff != "" {
					t.Fatal(diff)
				}
				if !tt.static && result.RequeueAfter > providerConfig().RefreshInterval {
					t.Fatal("capacity demand refresh is unbounded")
				}
			}
			for name, worker := range f.workers {
				member := &v1beta1.InferenceService{}
				err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.source), member)
				if (tt.unknown && !tt.static) || (name == "member-c" && !tt.static) {
					if !apierrors.IsNotFound(err) {
						t.Fatalf("unexpected member on %s: %v", name, err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(*member.Spec.Engine.MinReplicas, member.Spec.Engine.MaxReplicas); diff != "" {
					t.Errorf("member %s bounds differ: %s", name, diff)
				}
				policy, err := protocol.FromDerived(member)
				if err != nil {
					t.Fatal(err)
				}
				if policy == nil || (!tt.static && policy.Demand == nil) {
					t.Fatalf("missing applied authority on %s", name)
				}
			}
		})
	}
}

func TestCapacityHoldPreservesAcceptedAllocation(t *testing.T) {
	for _, tt := range []struct {
		name       string
		hardware   bool
		wantReason string
	}{
		{name: "stale report", wantReason: "CapacityUnknown"},
		{name: "hardware waiting for stability", hardware: true, wantReason: "CapacityStabilizing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCapacityProviderFixture(t)
			key := client.ObjectKeyFromObject(f.source)
			if _, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			before := &v1beta1.InferenceService{}
			if err := f.r.Get(t.Context(), key, before); err != nil {
				t.Fatal(err)
			}
			members := map[string]*v1beta1.InferenceService{}
			for _, name := range []string{"member-a", "member-b"} {
				member := &v1beta1.InferenceService{}
				if err := f.workers[name].Get(t.Context(), key, member); err != nil {
					t.Fatal(err)
				}
				members[name] = member
			}
			if tt.hardware {
				root := &v1beta1.AcceleratorQuota{}
				if err := f.workers["member-a"].Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
					t.Fatal(err)
				}
				root.Status.Capacity[0].Allocatable = resource.MustParse("20")
				if err := f.workers["member-a"].Update(t.Context(), root); err != nil {
					t.Fatal(err)
				}
				f.syncFleet(t)
			} else {
				f.clock.Step(providerConfig().MaxAge)
			}
			if _, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.InferenceService{}
			if err := f.r.Get(t.Context(), key, got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before.Status.Placement.Plan, got.Status.Placement.Plan); diff != "" {
				t.Fatalf("plan changed while held: %s", diff)
			}
			allocations := func(source *v1beta1.InferenceService) map[string]*v1beta1.CandidateAllocationStatus {
				out := map[string]*v1beta1.CandidateAllocationStatus{}
				for _, candidate := range source.Status.Placement.Candidates {
					out[candidate.Cluster] = candidate.Allocation
				}
				return out
			}
			if diff := cmp.Diff(allocations(before), allocations(got)); diff != "" {
				t.Fatalf("allocation changed while held: %s", diff)
			}
			condition := got.Status.GetCondition(apis.ConditionType(v1beta1.PlacementCapacityFresh))
			if condition == nil {
				t.Fatal("capacity freshness condition missing")
			}
			if diff := cmp.Diff(tt.wantReason, condition.Reason); diff != "" {
				t.Fatal(diff)
			}
			for name, want := range members {
				member := &v1beta1.InferenceService{}
				if err := f.workers[name].Get(t.Context(), key, member); err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(want, member); diff != "" {
					t.Fatalf("member %s changed while held: %s", name, diff)
				}
			}
		})
	}
}

// TestCapacityHoldKeepsAcknowledgedPlan settles a capacity split whose members
// acknowledged the accepted plan, then holds that plan on passes that read no
// member: unknown capacity, then a plan write that fails. Every hold pass keeps
// each member's acknowledged plan behind an unknown observation, and the next
// readable pass reports the acknowledgement it observes.
func TestCapacityHoldKeepsAcknowledgedPlan(t *testing.T) {
	f := newCapacityProviderFixture(t)
	bf := &backendFixture{source: f.source, workers: f.workers, reconciler: f.r}
	floors := map[string]int32{"member-a": 8, "member-b": 4}
	settled := bf.reconcile(t)
	acknowledged := func(source *v1beta1.InferenceService) bool {
		for name := range floors {
			candidate := candidateOf(t, source, name)
			if !candidate.ObservationKnown || candidate.AppliedPlanID != source.Status.Placement.Plan.ID {
				return false
			}
		}
		return true
	}
	for pass := 0; pass < 8 && !acknowledged(settled); pass++ {
		for name, floor := range floors {
			projectAllHome(t, bf, name, floor)
		}
		settled = bf.reconcile(t)
	}
	if !acknowledged(settled) {
		t.Fatalf("members did not acknowledge the accepted plan: %+v", settled.Status.Placement.Candidates)
	}
	accepted := settled.Status.Placement.Plan.DeepCopy()

	assertHeld := func(t *testing.T, got *v1beta1.InferenceService, reason string) {
		t.Helper()
		if diff := cmp.Diff(reason, got.Status.GetCondition(v1beta1.PlacementConverged).Reason); diff != "" {
			t.Fatalf("hold reason (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(accepted, got.Status.Placement.Plan); diff != "" {
			t.Fatalf("%s changed the accepted plan (-want +got):\n%s", reason, diff)
		}
		for name := range floors {
			candidate := candidateOf(t, got, name)
			if diff := cmp.Diff(accepted.ID, candidate.AppliedPlanID); diff != "" {
				t.Fatalf("%s blanked the acknowledged plan on %s (-want +got):\n%s", reason, name, diff)
			}
			if candidate.ObservationKnown {
				t.Fatalf("%s reported a current observation of %s without reading it", reason, name)
			}
		}
	}

	f.clock.Step(providerConfig().MaxAge)
	assertHeld(t, bf.reconcile(t), "CapacityUnknown")

	// Fresh reports and a new demand produce a plan the store cannot write.
	for _, cluster := range f.clusters {
		root := &v1beta1.AcceleratorQuota{}
		if err := f.workers[cluster.Name].Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
			t.Fatal(err)
		}
		root.Status.Capacity[0].ObservedAt = &metav1.Time{Time: f.clock.Now()}
		if err := f.workers[cluster.Name].Update(t.Context(), root); err != nil {
			t.Fatal(err)
		}
	}
	f.syncFleet(t)
	source := &v1beta1.InferenceService{}
	if err := f.r.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	source.Spec.Placement.Split.Replicas = ptr.To[int32](14)
	source.Generation++
	if err := f.r.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	writer := f.r.Client
	f.r.Client = interceptor.NewClient(writer.(client.WithWatch), interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		if written, ok := obj.(*v1beta1.InferenceService); ok && written.Status.Placement != nil && written.Status.Placement.Plan != nil && written.Status.Placement.Plan.ID != accepted.ID {
			return errors.New("plan write rejected")
		}
		return c.SubResource(name).Update(ctx, obj, opts...)
	}})
	assertHeld(t, bf.reconcile(t), "PlanNotPersisted")

	f.r.Client = writer
	readable := bf.reconcile(t)
	if readable.Status.Placement.Plan.ID == accepted.ID {
		t.Fatal("readable pass did not persist the new plan")
	}
	for name := range floors {
		candidate := candidateOf(t, readable, name)
		if !candidate.ObservationKnown || candidate.AppliedPlanID != "" {
			t.Fatalf("readable pass kept a stale acknowledgement on %s: %+v", name, candidate)
		}
	}
}
