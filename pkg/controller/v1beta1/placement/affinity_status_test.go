package placement

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func affinityDiagnosticSource() *v1beta1.InferenceService {
	source := srcISVC("")
	source.Namespace = "team-a"
	source.Spec.Engine.MinReplicas = ptr.To(1)
	source.Spec.Placement.ClusterAffinity = append(testAffinity("metadata.name=member-a"), testAffinity("region=east")...)
	return source
}

func TestAffinityMatchDiagnostics(t *testing.T) {
	for _, tt := range []struct {
		name      string
		edit      func(*v1beta1.InferenceService)
		clusters  []v1beta1.WorkloadCluster
		unmatched string
	}{
		{name: "empty registry", unmatched: "[0 1]"},
		{name: "partially matched terms", clusters: []v1beta1.WorkloadCluster{*readyWC("member-a", nil)}, unmatched: "[1]"},
		{name: "overlapping terms", clusters: []v1beta1.WorkloadCluster{*readyWC("member-a", map[string]string{"region": "east"})}},
		{name: "unready member still matches", clusters: []v1beta1.WorkloadCluster{{ObjectMeta: metav1.ObjectMeta{Name: "member-a", Labels: map[string]string{"region": "east"}}}}},
		{name: "deleting registration is excluded", clusters: []v1beta1.WorkloadCluster{{ObjectMeta: metav1.ObjectMeta{Name: "member-a", Labels: map[string]string{"region": "east"}, DeletionTimestamp: ptr.To(metav1.Now())}}}, unmatched: "[0 1]"},
		{name: "name label cannot impersonate identity", clusters: []v1beta1.WorkloadCluster{*readyWC("member-b", map[string]string{"metadata.name": "member-a"})}, unmatched: "[0 1]"},
		{name: "omitted affinity is unconstrained", edit: func(s *v1beta1.InferenceService) { s.Spec.Placement.ClusterAffinity = nil }},
		{name: "empty affinity remains invalid", edit: func(s *v1beta1.InferenceService) { s.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{} }},
		{name: "empty term remains invalid", edit: func(s *v1beta1.InferenceService) {
			s.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{}}
		}},
		{name: "unselected policy has no affinity diagnostics", edit: func(s *v1beta1.InferenceService) {
			s.Spec.Placement = &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeAll}
		}},
		{name: "weighted terms retain original indexes", edit: func(s *v1beta1.InferenceService) {
			s.Spec.Placement.Mode = v1beta1.PlacementModeSplit
			s.Spec.Placement.ClusterAffinity[0].Weight = ptr.To[int32](3)
		}, clusters: []v1beta1.WorkloadCluster{*readyWC("member-b", map[string]string{"region": "east"})}, unmatched: "[0]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := affinityDiagnosticSource()
			if tt.edit != nil {
				tt.edit(source)
			}
			before := source.DeepCopy()
			want := placementInputCondition(source)
			if tt.unmatched != "" {
				want.cond.Message += "; clusterAffinity terms " + tt.unmatched + " match no registered WorkloadCluster (zero-based indexes)"
			}
			ctx := withPlacementInputCondition(t.Context(), source, tt.clusters)
			got := placementInputForWrite(ctx, source)
			if diff := cmp.Diff(want.cond, got.cond); diff != "" {
				t.Fatalf("condition (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatalf("source changed (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAffinityDiagnosticsFollowSourceSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.InferenceService)
	}{
		{name: "source identity", edit: func(s *v1beta1.InferenceService) { s.UID = "other-source" }},
		{name: "source generation", edit: func(s *v1beta1.InferenceService) { s.Generation++ }},
		{name: "source name", edit: func(s *v1beta1.InferenceService) { s.Name = "other-service" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := affinityDiagnosticSource()
			ctx := withPlacementInputCondition(t.Context(), source, nil)
			tt.edit(source)
			want := placementInputCondition(source)
			got := placementInputForWrite(ctx, source)
			if diff := cmp.Diff(want.cond, got.cond); diff != "" {
				t.Fatalf("stale diagnostic (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReconcilePublishesAffinityDiagnostics(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity} {
		t.Run(string(mode), func(t *testing.T) {
			source := affinityDiagnosticSource()
			source.Spec.Placement.Mode = mode
			r, cl := newPlacer(testScheme(t), fakeClusters{}, source, readyWC("member-a", nil))
			key := client.ObjectKeyFromObject(source)
			reconcile := func(want string) {
				t.Helper()
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
				got := &v1beta1.InferenceService{}
				if err := cl.Get(t.Context(), key, got); err != nil {
					t.Fatal(err)
				}
				condition := got.Status.GetCondition(v1beta1.PlacementInputValid)
				if condition == nil {
					t.Fatal("input condition was not published")
				}
				if diff := cmp.Diff(corev1.ConditionTrue, condition.Status); diff != "" {
					t.Fatalf("valid intent (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(want, condition.Message); diff != "" {
					t.Fatalf("diagnostic (-want +got):\n%s", diff)
				}
			}
			reconcile("Placement intent is valid; clusterAffinity terms [1] match no registered WorkloadCluster (zero-based indexes)")
			unready := &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: "member-b", UID: "member-b-uid", Labels: map[string]string{"region": "east"}}}
			if err := cl.Create(t.Context(), unready); err != nil {
				t.Fatal(err)
			}
			reconcile("Placement intent is valid")
			if err := cl.Delete(t.Context(), unready); err != nil {
				t.Fatal(err)
			}
			reconcile("Placement intent is valid; clusterAffinity terms [1] match no registered WorkloadCluster (zero-based indexes)")
		})
	}
}
