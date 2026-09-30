package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
